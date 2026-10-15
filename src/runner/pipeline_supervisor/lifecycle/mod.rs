/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Owns one ready Tenon Document's restartable Pipeline lifecycle.

mod cleanup;
mod failure;

use super::{PipelineExecutionAdmission, PipelineLifecycleControl};
use crate::config::RunnerConfig;
use crate::identifiers::TenonDocumentId;
use crate::runner::document_store;
use crate::runner::execution::ExecutionExpiry;
use crate::runner::management::{PipelineLifecycleState, PipelineStateUpdate};
use crate::runner::pipeline::{
    PipelineLifecycleTarget, PipelineTargetPublication, RunnerPipelineControlSessionError,
    RunnerPipelineLaunchError, RunnerPipelineLauncher, RunnerPipelineProcess,
    RunnerPipelineProcessError, RunnerPipelineProcessEvent,
};
use crate::runner::process_tree::PipelineShutdownOutcome;
use cleanup::CleanupTarget;
use document_store::TenonDocumentEtag;
use failure::PipelineRestartCause;
pub(in crate::runner) use failure::RunnerPipelineLifecycleError;
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time;

#[derive(Clone)]
pub(super) struct PipelineLifecycleIdentity(Arc<()>);

impl PipelineLifecycleIdentity {
    pub(super) fn new() -> Self {
        Self(Arc::new(()))
    }

    pub(super) fn same_as(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

pub(super) struct PipelineLifecycleUpdate {
    pub(super) identity: PipelineLifecycleIdentity,
    pub(super) update: PipelineStateUpdate,
}

pub(super) fn run_pipeline_lifecycle(
    document_id: TenonDocumentId,
    identity: PipelineLifecycleIdentity,
    context: PipelineLifecycleContext,
) -> (
    PipelineLifecycleControl,
    impl Future<Output = Result<PipelineLifecycleCompletion, RunnerPipelineLifecycleError>>,
) {
    let (control, mut input) = PipelineLifecycleInput::new();
    (control, async move {
        let mut backoff = RetryBackoffSequence::new(
            context.config.retry_initial_delay(),
            context.config.retry_maximum_delay(),
        );
        let mut next_attempt = NextAttempt::Initial;

        loop {
            let delay = match next_attempt {
                NextAttempt::RetryAfter(delay) => Some(delay),
                _ => None,
            };
            let Some(target) = wait_for_ready_target(&mut input, delay).await else {
                return Ok(PipelineLifecycleCompletion::Stopped);
            };
            let working_directory = context
                .runtime_directory
                .join(target.document_etag().directory_name());
            let attempted_etag = target.document_etag();
            publish_state(
                &context.state_updates,
                &document_id,
                &identity,
                PipelineLifecycleState::Starting,
            )
            .await?;
            let outcome = if input.is_shutdown_requested()
                || input.execution_admission_is_closed()
                || context.execution_expiry.is_expired()
            {
                PipelineAttemptOutcome::Stopped {
                    shutdown_timed_out: false,
                }
            } else {
                match context.launcher.start_pipeline(
                    &context.pipeline_executable,
                    target,
                    &context.config,
                    &working_directory,
                ) {
                    Ok(mut process) => {
                        if !matches!(next_attempt, NextAttempt::Initial) {
                            context.launcher.record_restart(&document_id);
                        }
                        let result = context
                            .run_launched_attempt(&mut process, &document_id, &identity, &mut input)
                            .await;
                        let outcome = match result {
                            Ok(outcome) => outcome,
                            Err(primary) => {
                                return Err(match process.cleanup().await {
                                    Ok(()) => primary,
                                    Err(cleanup) => {
                                        RunnerPipelineLifecycleError::CleanupAfterFailure {
                                            primary: Box::new(primary),
                                            cleanup,
                                        }
                                    }
                                });
                            }
                        };
                        outcome.with_cleanup(
                            context
                                .cleanup_with_retry(
                                    &document_id,
                                    &identity,
                                    CleanupTarget::Process(&mut process),
                                )
                                .await,
                        )?
                    }
                    Err(error @ RunnerPipelineLaunchError::ProcessTreeOwnershipFailed(_)) => {
                        return Err(RunnerPipelineLifecycleError::Launch(error));
                    }
                    Err(mut error) => {
                        if let RunnerPipelineLaunchError::LaunchCleanupPending {
                            resources, ..
                        } = &mut error
                        {
                            context
                                .cleanup_with_retry(
                                    &document_id,
                                    &identity,
                                    CleanupTarget::Launch(resources),
                                )
                                .await?;
                        }
                        context
                            .publish_restart(
                                &document_id,
                                &identity,
                                PipelineRestartCause::Launch(error),
                                attempted_etag,
                            )
                            .await?
                    }
                }
            };

            match outcome.with_cleanup(
                context
                    .cleanup_with_retry(
                        &document_id,
                        &identity,
                        CleanupTarget::Directory(&working_directory),
                    )
                    .await,
            )? {
                PipelineAttemptOutcome::Stopped {
                    shutdown_timed_out: false,
                } => {
                    return Ok(PipelineLifecycleCompletion::Stopped);
                }
                PipelineAttemptOutcome::Stopped {
                    shutdown_timed_out: true,
                } => {
                    return Ok(PipelineLifecycleCompletion::ShutdownTimedOut);
                }
                PipelineAttemptOutcome::Restart(cause) => {
                    cause.log_creation_failure(&document_id);
                    next_attempt = NextAttempt::RetryAfter(backoff.take_next());
                }
                PipelineAttemptOutcome::Replace { .. } => {
                    // The old process is gone even when the newest target is unready.
                    publish_state(
                        &context.state_updates,
                        &document_id,
                        &identity,
                        PipelineLifecycleState::Starting,
                    )
                    .await?;
                    next_attempt = NextAttempt::Replacement;
                }
            }
        }
    })
}

pub(super) struct PipelineLifecycleContext {
    config: Arc<RunnerConfig>,
    pipeline_executable: PathBuf,
    runtime_directory: PathBuf,
    launcher: RunnerPipelineLauncher,
    state_updates: mpsc::Sender<PipelineLifecycleUpdate>,
    execution_expiry: ExecutionExpiry,
    runner_shutdown: watch::Sender<bool>,
}

impl PipelineLifecycleContext {
    pub(super) fn new(
        config: Arc<RunnerConfig>,
        pipeline_executable: PathBuf,
        runtime_directory: PathBuf,
        launcher: RunnerPipelineLauncher,
        state_updates: mpsc::Sender<PipelineLifecycleUpdate>,
        execution_expiry: ExecutionExpiry,
    ) -> Self {
        Self {
            config,
            pipeline_executable,
            runtime_directory,
            launcher,
            state_updates,
            execution_expiry,
            runner_shutdown: watch::channel(false).0,
        }
    }

    pub(super) fn stop_cleanup_retries(&self) {
        self.runner_shutdown.send_replace(true);
    }

    async fn run_launched_attempt(
        &self,
        process: &mut RunnerPipelineProcess,
        document_id: &TenonDocumentId,
        identity: &PipelineLifecycleIdentity,
        input: &mut PipelineLifecycleInput,
    ) -> Result<PipelineAttemptOutcome, RunnerPipelineLifecycleError> {
        tokio::select! {
            biased;
            _ = &mut input.shutdown_requested => {
                return self
                    .stop_pipeline(process, document_id)
                    .await;
            }
            result = process.wait_for_attachment() => match result {
                Ok(()) => {},
                Err(error) => {
                    return self
                        .publish_restart(
                            document_id,
                            identity,
                            PipelineRestartCause::Launch(error),
                            process.latest_document_etag(),
                        )
                        .await;
                }
            }
        };
        let first_status = tokio::select! {
            biased;
            _ = &mut input.shutdown_requested => {
                return self
                    .stop_pipeline(process, document_id)
                    .await;
            }
            result = process.wait_for_startup_status() => match result {
                Ok(status) => status,
                Err(error) => {
                    return self
                        .publish_restart(
                            document_id,
                            identity,
                            PipelineRestartCause::Startup(error),
                            process.latest_document_etag(),
                        )
                        .await;
                }
            }
        };
        if publish_state(
            &self.state_updates,
            document_id,
            identity,
            PipelineLifecycleState::Running(first_status),
        )
        .await
        .is_err()
        {
            return Err(RunnerPipelineLifecycleError::StateReceiverClosed);
        }
        if input.execution_admission_is_closed() || self.execution_expiry.is_expired() {
            return self.stop_pipeline(process, document_id).await;
        }
        match input.publish_latest_target(process) {
            Ok(PipelineTargetPublication::Published) => {}
            Ok(PipelineTargetPublication::ReplaceProcess) => {
                return self.replace_pipeline(process, document_id).await;
            }
            Err(error) => {
                let primary =
                    PipelineRestartCause::Runtime(RunnerPipelineProcessError::Control(error));
                return self
                    .invalidate_and_finish_attempt(process, document_id, identity, primary)
                    .await;
            }
        }

        loop {
            let event = tokio::select! {
                biased;
                _ = &mut input.shutdown_requested => {
                    return self
                        .stop_pipeline(process, document_id)
                        .await;
                }
                changed = input.target_updates.changed() => {
                    if changed.is_err() {
                        return self
                            .stop_pipeline(process, document_id)
                            .await;
                    }
                    if input.execution_admission_is_closed() || self.execution_expiry.is_expired() {
                        return self.stop_pipeline(process, document_id).await;
                    }
                    match input.publish_latest_target(process) {
                        Ok(PipelineTargetPublication::Published) => {},
                        Ok(PipelineTargetPublication::ReplaceProcess) => return self.replace_pipeline(process, document_id).await,
                        Err(error) => {
                            let primary = PipelineRestartCause::Runtime(RunnerPipelineProcessError::Control(error));
                            return self.invalidate_and_finish_attempt(process, document_id, identity, primary).await;
                        }
                    }
                    continue;
                }
                event = process.next_event() => event,
            };
            match event {
                Ok(RunnerPipelineProcessEvent::Status(status)) => {
                    if publish_state(
                        &self.state_updates,
                        document_id,
                        identity,
                        PipelineLifecycleState::Running(status),
                    )
                    .await
                    .is_err()
                    {
                        return Err(RunnerPipelineLifecycleError::StateReceiverClosed);
                    }
                }
                Ok(RunnerPipelineProcessEvent::Exited(status)) => {
                    return self
                        .publish_restart(
                            document_id,
                            identity,
                            PipelineRestartCause::Exited(status),
                            process.latest_document_etag(),
                        )
                        .await;
                }
                Err(error) => {
                    let primary = PipelineRestartCause::Runtime(error);
                    return self
                        .invalidate_and_finish_attempt(process, document_id, identity, primary)
                        .await;
                }
            }
        }
    }

    async fn replace_pipeline(
        &self,
        process: &mut RunnerPipelineProcess,
        document_id: &TenonDocumentId,
    ) -> Result<PipelineAttemptOutcome, RunnerPipelineLifecycleError> {
        let PipelineAttemptOutcome::Stopped { shutdown_timed_out } =
            self.stop_pipeline(process, document_id).await?
        else {
            unreachable!("stopping a process returns only its shutdown outcome");
        };
        Ok(PipelineAttemptOutcome::Replace { shutdown_timed_out })
    }

    async fn stop_pipeline(
        &self,
        owner: &mut RunnerPipelineProcess,
        document_id: &TenonDocumentId,
    ) -> Result<PipelineAttemptOutcome, RunnerPipelineLifecycleError> {
        let shutdown_timed_out = match owner
            .terminate_and_reap(self.config.pipeline_shutdown_timeout())
            .await
        {
            Ok(outcome) => matches!(outcome, PipelineShutdownOutcome::ForcedAfterDeadline),
            Err(error) => {
                eprintln!(
                    "runner.pipeline_cleanup_failed: Pipeline {document_id}: {}",
                    crate::error::ErrorChain(&error)
                );
                error.deadline_elapsed()
            }
        };
        // The attempt must still complete cleanup before the lifecycle returns.
        Ok(PipelineAttemptOutcome::Stopped { shutdown_timed_out })
    }

    async fn invalidate_and_finish_attempt(
        &self,
        process: &mut RunnerPipelineProcess,
        document_id: &TenonDocumentId,
        identity: &PipelineLifecycleIdentity,
        primary: PipelineRestartCause,
    ) -> Result<PipelineAttemptOutcome, RunnerPipelineLifecycleError> {
        let state =
            PipelineLifecycleState::RestartBackoff(primary.view(process.latest_document_etag()));
        if publish_state(&self.state_updates, document_id, identity, state)
            .await
            .is_err()
        {
            return Err(RunnerPipelineLifecycleError::StateReceiverClosed);
        }
        Ok(PipelineAttemptOutcome::Restart(primary))
    }

    async fn publish_restart(
        &self,
        document_id: &TenonDocumentId,
        identity: &PipelineLifecycleIdentity,
        cause: PipelineRestartCause,
        attempted_etag: TenonDocumentEtag,
    ) -> Result<PipelineAttemptOutcome, RunnerPipelineLifecycleError> {
        publish_state(
            &self.state_updates,
            document_id,
            identity,
            PipelineLifecycleState::RestartBackoff(cause.view(attempted_etag)),
        )
        .await?;
        Ok(PipelineAttemptOutcome::Restart(cause))
    }
}

impl Clone for PipelineLifecycleContext {
    fn clone(&self) -> Self {
        Self {
            config: Arc::clone(&self.config),
            pipeline_executable: self.pipeline_executable.clone(),
            runtime_directory: self.runtime_directory.clone(),
            launcher: self.launcher.clone(),
            state_updates: self.state_updates.clone(),
            execution_expiry: self.execution_expiry.clone(),
            runner_shutdown: self.runner_shutdown.clone(),
        }
    }
}

impl fmt::Debug for PipelineLifecycleContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PipelineLifecycleContext")
            .field("pipeline_executable", &self.pipeline_executable)
            .field("runtime_directory", &self.runtime_directory)
            .field("launcher", &self.launcher)
            .finish_non_exhaustive()
    }
}

async fn publish_state(
    updates: &mpsc::Sender<PipelineLifecycleUpdate>,
    document_id: &TenonDocumentId,
    identity: &PipelineLifecycleIdentity,
    state: PipelineLifecycleState,
) -> Result<(), RunnerPipelineLifecycleError> {
    updates
        .send(PipelineLifecycleUpdate {
            identity: identity.clone(),
            update: PipelineStateUpdate {
                document_id: document_id.clone(),
                state,
            },
        })
        .await
        .map_err(|_| RunnerPipelineLifecycleError::StateReceiverClosed)
}

struct PipelineLifecycleInput {
    target_updates: watch::Receiver<Option<Arc<PipelineLifecycleTarget>>>,
    execution_admission_updates: watch::Receiver<PipelineExecutionAdmission>,
    shutdown_requested: oneshot::Receiver<()>,
}

impl PipelineLifecycleInput {
    #[must_use]
    fn new() -> (PipelineLifecycleControl, Self) {
        let (targets, target_updates) = watch::channel(None);
        let (execution_admission, execution_admission_updates) =
            watch::channel(PipelineExecutionAdmission::Open);
        let (shutdown, shutdown_requested) = oneshot::channel();
        (
            PipelineLifecycleControl {
                targets,
                execution_admission,
                shutdown,
            },
            Self {
                target_updates,
                execution_admission_updates,
                shutdown_requested,
            },
        )
    }

    fn publish_latest_target(
        &mut self,
        process: &mut RunnerPipelineProcess,
    ) -> Result<PipelineTargetPublication, RunnerPipelineControlSessionError> {
        let Some(target) = self.target_updates.borrow_and_update().clone() else {
            return Ok(PipelineTargetPublication::Published);
        };
        process.publish_target(target)
    }

    fn is_shutdown_requested(&mut self) -> bool {
        match self.shutdown_requested.try_recv() {
            Ok(()) | Err(TryRecvError::Closed) => true,
            Err(TryRecvError::Empty) => false,
        }
    }

    fn execution_admission_is_closed(&self) -> bool {
        *self.execution_admission_updates.borrow() == PipelineExecutionAdmission::Closed
    }
}

async fn wait_for_ready_target(
    input: &mut PipelineLifecycleInput,
    minimum_delay: Option<Duration>,
) -> Option<Arc<PipelineLifecycleTarget>> {
    if input.is_shutdown_requested() || input.execution_admission_is_closed() {
        return None;
    }
    if let Some(delay) = minimum_delay {
        let delay = time::sleep(delay);
        tokio::pin!(delay);
        loop {
            tokio::select! {
                biased;
                _ = &mut input.shutdown_requested => return None,
                changed = input.execution_admission_updates.changed() => {
                    if changed.is_err() || input.execution_admission_is_closed() {
                        return None;
                    }
                }
                changed = input.target_updates.changed() => {
                    if changed.is_err() {
                        return None;
                    }
                    drop(input.target_updates.borrow_and_update());
                }
                () = &mut delay => break,
            }
        }
    }

    loop {
        if input.execution_admission_is_closed() {
            return None;
        }
        if let Some(target) = input.target_updates.borrow_and_update().clone() {
            return Some(target);
        }
        tokio::select! {
            biased;
            _ = &mut input.shutdown_requested => return None,
            changed = input.execution_admission_updates.changed() => {
                if changed.is_err() || input.execution_admission_is_closed() {
                    return None;
                }
            }
            changed = input.target_updates.changed() => {
                if changed.is_err() {
                    return None;
                }
            }
        }
    }
}

struct RetryBackoffSequence {
    next: Duration,
    maximum: Duration,
}

impl RetryBackoffSequence {
    const fn new(initial: Duration, maximum: Duration) -> Self {
        Self {
            next: initial,
            maximum,
        }
    }

    fn take_next(&mut self) -> Duration {
        let current = self.next;
        self.next = self.next.saturating_mul(2).min(self.maximum);
        current
    }
}

enum PipelineAttemptOutcome {
    Stopped { shutdown_timed_out: bool },
    Replace { shutdown_timed_out: bool },
    Restart(PipelineRestartCause),
}

enum NextAttempt {
    Initial,
    Replacement,
    RetryAfter(Duration),
}

impl PipelineAttemptOutcome {
    fn with_cleanup(
        self,
        cleanup: Result<(), RunnerPipelineLifecycleError>,
    ) -> Result<Self, RunnerPipelineLifecycleError> {
        let Err(cleanup) = cleanup else {
            return Ok(self);
        };
        let cleanup = match cleanup {
            RunnerPipelineLifecycleError::Cleanup(cleanup) => cleanup,
            error => return Err(error),
        };
        Err(match self {
            Self::Stopped {
                shutdown_timed_out: false,
            }
            | Self::Replace {
                shutdown_timed_out: false,
            } => RunnerPipelineLifecycleError::Cleanup(cleanup),
            Self::Stopped {
                shutdown_timed_out: true,
            }
            | Self::Replace {
                shutdown_timed_out: true,
            } => RunnerPipelineLifecycleError::CleanupAfterShutdownTimeout(cleanup),
            Self::Restart(primary) => RunnerPipelineLifecycleError::CleanupAfterRestart {
                primary: Box::new(primary),
                cleanup,
            },
        })
    }
}

pub(super) enum PipelineLifecycleCompletion {
    Stopped,
    ShutdownTimedOut,
}

#[cfg(test)]
pub(super) mod test_support;

#[cfg(test)]
mod instance_tests;
#[cfg(test)]
mod tests;
