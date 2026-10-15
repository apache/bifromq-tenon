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

//! Coordinates every Pipeline lifecycle task managed by one Runner.
//!
//! Admission closes before accepted HTTP work drains; its lifecycle context
//! keeps state delivery open until full shutdown. Task ownership and collected
//! shutdown results stay here across cancelled waits. Individual lifecycle
//! tasks own their process shutdown deadlines and mandatory cleanup.

mod lifecycle;

use crate::config::RunnerConfig;
use crate::identifiers::TenonDocumentId;
use crate::runner::execution::ExecutionExpiry;
use crate::runner::management::{PipelineDirective, PipelineLifecycleRole, PipelineStateUpdate};
use crate::runner::pipeline::{PipelineLifecycleTarget, RunnerPipelineLauncher};
use lifecycle::{
    PipelineLifecycleCompletion, PipelineLifecycleContext, PipelineLifecycleIdentity,
    PipelineLifecycleUpdate, RunnerPipelineLifecycleError, run_pipeline_lifecycle,
};
use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fmt, mem};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::{Id as TaskId, JoinError, JoinSet};

pub(super) struct PipelineSupervisor {
    lifecycles: HashMap<TenonDocumentId, PipelineLifecycleSlot>,
    tasks: JoinSet<PipelineLifecycleTaskOutput>,
    task_documents: HashMap<TaskId, PipelineTaskIdentity>,
    state_updates: mpsc::Receiver<PipelineLifecycleUpdate>,
    phase: PipelineSupervisorPhase,
    drain_result: PipelineDrainResult,
}

impl PipelineSupervisor {
    pub(super) fn new(
        config: Arc<RunnerConfig>,
        pipeline_executable: PathBuf,
        runtime_directory: PathBuf,
        launcher: RunnerPipelineLauncher,
        initial_directives: Box<[PipelineDirective]>,
        execution_expiry: ExecutionExpiry,
    ) -> Self {
        let initial_capacity = initial_directives.len();
        let (state_update_sender, state_updates) = mpsc::channel(initial_capacity.max(1));
        let context = PipelineLifecycleContext::new(
            config,
            pipeline_executable,
            runtime_directory,
            launcher,
            state_update_sender,
            execution_expiry,
        );
        let mut supervisor = Self {
            lifecycles: HashMap::new(),
            tasks: JoinSet::new(),
            task_documents: HashMap::new(),
            state_updates,
            phase: PipelineSupervisorPhase::Running(context),
            drain_result: PipelineDrainResult::default(),
        };
        supervisor.apply_active(initial_directives);
        supervisor
    }

    pub(super) fn apply(
        &mut self,
        directives: Box<[PipelineDirective]>,
    ) -> Result<(), PipelineSupervisorError> {
        if matches!(self.phase, PipelineSupervisorPhase::Draining) {
            return Err(PipelineSupervisorError::Stopped);
        }
        self.apply_active(directives);
        Ok(())
    }

    pub(super) async fn next_event(&mut self) -> PipelineEvent {
        loop {
            let event = if self.execution_admission_is_closed() {
                tokio::select! {
                    biased;
                    update = self.state_updates.recv() => Some(self.state_event(update)),
                    task = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                        self.joined_task_event(task)
                    }
                }
            } else {
                tokio::select! {
                    biased;
                    task = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => {
                        self.joined_task_event(task)
                    }
                    update = self.state_updates.recv() => Some(self.state_event(update)),
                }
            };
            if let Some(event) = event {
                return event;
            }
        }
    }

    /// Closes the process-spawn boundary without stopping a process that is
    /// still serving requests accepted before HTTP admission closed.
    pub(super) fn close_execution_admission(&mut self) {
        let phase = mem::replace(&mut self.phase, PipelineSupervisorPhase::Draining);
        self.phase = match phase {
            PipelineSupervisorPhase::Running(context) => {
                PipelineSupervisorPhase::AdmissionClosed(context)
            }
            phase => phase,
        };
        for slot in self.lifecycles.values_mut() {
            match &mut slot.phase {
                PipelineLifecyclePhase::Active(control) => control.close_execution_admission(),
                PipelineLifecyclePhase::Retiring { pending_target } => *pending_target = None,
            }
        }
    }

    pub(super) async fn shutdown(&mut self) -> PipelineDrainResult {
        self.request_shutdown();
        while !self.tasks.is_empty() {
            tokio::select! {
                biased;
                task = self.tasks.join_next_with_id() => {
                    self.drain_result.record(task, &mut self.task_documents);
                }
                update = self.state_updates.recv() => {
                    let _ = update;
                }
            }
        }

        while self.state_updates.recv().await.is_some() {}
        debug_assert!(self.task_documents.is_empty());
        let cleanup = self.drain_result.cleanup;
        mem::replace(
            &mut self.drain_result,
            PipelineDrainResult {
                cleanup,
                ..PipelineDrainResult::default()
            },
        )
    }

    fn apply_active(&mut self, directives: Box<[PipelineDirective]>) {
        for directive in directives {
            match directive {
                PipelineDirective::SetTarget {
                    document_id,
                    target,
                } => {
                    if let Some(slot) = self.lifecycles.get_mut(&document_id) {
                        if !matches!(self.phase, PipelineSupervisorPhase::Running(_)) {
                            continue;
                        }
                        match &mut slot.phase {
                            PipelineLifecyclePhase::Active(control) => {
                                control.set_target(target);
                            }
                            PipelineLifecyclePhase::Retiring { pending_target } => {
                                *pending_target = target;
                            }
                        }
                    } else if let Some(target) = target {
                        self.start_lifecycle(document_id, target);
                    }
                }
                PipelineDirective::Stop { document_id } => {
                    if let Some(slot) = self.lifecycles.get_mut(&document_id) {
                        let phase = mem::replace(
                            &mut slot.phase,
                            PipelineLifecyclePhase::Retiring {
                                pending_target: None,
                            },
                        );
                        match phase {
                            PipelineLifecyclePhase::Active(control) => control.stop(),
                            PipelineLifecyclePhase::Retiring { .. } => {}
                        }
                    }
                }
            }
        }
    }

    fn start_lifecycle(
        &mut self,
        document_id: TenonDocumentId,
        target: Arc<PipelineLifecycleTarget>,
    ) {
        let PipelineSupervisorPhase::Running(context) = &self.phase else {
            return;
        };
        let identity = PipelineLifecycleIdentity::new();
        let (control, lifecycle) =
            run_pipeline_lifecycle(document_id.clone(), identity.clone(), context.clone());
        control.set_target(Some(target));
        let task = self.tasks.spawn(lifecycle);
        let previous_task = self.task_documents.insert(
            task.id(),
            PipelineTaskIdentity {
                document_id: document_id.clone(),
                lifecycle: identity.clone(),
            },
        );
        let previous_slot = self.lifecycles.insert(
            document_id,
            PipelineLifecycleSlot {
                identity,
                phase: PipelineLifecyclePhase::Active(control),
            },
        );
        debug_assert!(previous_task.is_none());
        debug_assert!(previous_slot.is_none());
    }

    fn execution_admission_is_closed(&self) -> bool {
        !matches!(self.phase, PipelineSupervisorPhase::Running(_))
    }

    fn state_event(&self, update: Option<PipelineLifecycleUpdate>) -> PipelineEvent {
        match update {
            Some(update) => {
                let role = self.lifecycle_role(&update);
                PipelineEvent::State {
                    update: update.update,
                    role,
                }
            }
            None => PipelineEvent::Failure(PipelineSupervisorError::StateChannelClosed),
        }
    }

    fn request_shutdown(&mut self) {
        match mem::replace(&mut self.phase, PipelineSupervisorPhase::Draining) {
            PipelineSupervisorPhase::Running(context)
            | PipelineSupervisorPhase::AdmissionClosed(context) => {
                context.stop_cleanup_retries();
            }
            PipelineSupervisorPhase::Draining => return,
        }
        for (_, slot) in self.lifecycles.drain() {
            if let PipelineLifecyclePhase::Active(control) = slot.phase {
                control.stop();
            }
        }
    }

    fn lifecycle_role(&self, update: &PipelineLifecycleUpdate) -> PipelineLifecycleRole {
        match self.lifecycles.get(&update.update.document_id) {
            Some(slot) if slot.identity.same_as(&update.identity) => match slot.phase {
                PipelineLifecyclePhase::Active(_) => PipelineLifecycleRole::Current,
                PipelineLifecyclePhase::Retiring { .. } => PipelineLifecycleRole::Retiring,
            },
            Some(_) | None => PipelineLifecycleRole::Retiring,
        }
    }

    fn joined_task_event(&mut self, task: JoinedPipelineLifecycleTask) -> Option<PipelineEvent> {
        let (task, task_identity) = match take_task_identity(task, &mut self.task_documents) {
            Ok(result) => result,
            Err(source) => return Some(self.failure_event(source)),
        };
        let Some(slot) = self.lifecycles.remove(&task_identity.document_id) else {
            return Some(
                self.failure_event(PipelineSupervisorError::LifecycleIdentityMissing(
                    task_identity.document_id,
                )),
            );
        };
        if !slot.identity.same_as(&task_identity.lifecycle) {
            return Some(
                self.failure_event(PipelineSupervisorError::LifecycleIdentityMismatch(
                    task_identity.document_id,
                )),
            );
        }
        match slot.phase {
            PipelineLifecyclePhase::Retiring { pending_target } => {
                if let Err(source) = expected_pipeline_stop(task, &task_identity.document_id) {
                    return Some(self.failure_event(source));
                }
                if let Some(target) = pending_target {
                    self.start_lifecycle(task_identity.document_id, target);
                }
                None
            }
            PipelineLifecyclePhase::Active(_) if self.execution_admission_is_closed() => {
                if let Err(source) = expected_pipeline_stop(task, &task_identity.document_id) {
                    return Some(self.failure_event(source));
                }
                None
            }
            PipelineLifecyclePhase::Active(_) => Some(self.failure_event(
                unexpected_pipeline_task_failure(task, task_identity.document_id),
            )),
        }
    }

    fn failure_event(&mut self, source: PipelineSupervisorError) -> PipelineEvent {
        self.drain_result.cleanup.observe(&source);
        PipelineEvent::Failure(source)
    }
}

pub(super) enum PipelineEvent {
    State {
        update: PipelineStateUpdate,
        role: PipelineLifecycleRole,
    },
    Failure(PipelineSupervisorError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PipelineCleanup {
    Complete,
    Incomplete,
}

impl PipelineCleanup {
    fn observe(&mut self, failure: &PipelineSupervisorError) {
        if !failure.process_owners_recovered() {
            *self = Self::Incomplete;
        }
    }
}

pub(super) struct PipelineDrainResult {
    pub(super) cleanup: PipelineCleanup,
    pub(super) failures: Vec<PipelineSupervisorError>,
    pub(super) timed_out_documents: Vec<TenonDocumentId>,
}

impl Default for PipelineDrainResult {
    fn default() -> Self {
        Self {
            cleanup: PipelineCleanup::Complete,
            failures: Vec::new(),
            timed_out_documents: Vec::new(),
        }
    }
}

impl PipelineDrainResult {
    fn record_failure(&mut self, failure: PipelineSupervisorError) {
        self.cleanup.observe(&failure);
        self.failures.push(failure);
    }

    fn record(
        &mut self,
        task: JoinedPipelineLifecycleTask,
        task_documents: &mut HashMap<TaskId, PipelineTaskIdentity>,
    ) {
        let (task, task_identity) = match take_task_identity(task, task_documents) {
            Ok(result) => result,
            Err(source) => {
                self.record_failure(source);
                return;
            }
        };
        let document_id = task_identity.document_id;
        match task {
            Ok((_, Ok(PipelineLifecycleCompletion::Stopped))) => {}
            Ok((_, Ok(PipelineLifecycleCompletion::ShutdownTimedOut))) => {
                self.timed_out_documents.push(document_id);
            }
            Ok((_, Err(source))) => {
                if source.includes_shutdown_timeout() {
                    self.timed_out_documents.push(document_id.clone());
                }
                self.record_failure(PipelineSupervisorError::Lifecycle {
                    document_id,
                    source: Box::new(source),
                });
            }
            Err(source) => self.record_failure(PipelineSupervisorError::Task {
                document_id,
                source,
            }),
        }
    }
}

fn take_task_identity(
    result: JoinedPipelineLifecycleTask,
    task_documents: &mut HashMap<TaskId, PipelineTaskIdentity>,
) -> Result<(JoinedPipelineTask, PipelineTaskIdentity), PipelineSupervisorError> {
    let Some(result) = result else {
        return Err(PipelineSupervisorError::TaskSetClosed);
    };
    let task_id = match &result {
        Ok((task_id, _)) => *task_id,
        Err(source) => source.id(),
    };
    let Some(identity) = task_documents.remove(&task_id) else {
        return Err(PipelineSupervisorError::TaskIdentityMissing(task_id));
    };
    Ok((result, identity))
}

fn expected_pipeline_stop(
    result: Result<(TaskId, PipelineLifecycleTaskOutput), JoinError>,
    document_id: &TenonDocumentId,
) -> Result<(), PipelineSupervisorError> {
    match result {
        Ok((_, Ok(PipelineLifecycleCompletion::Stopped))) => Ok(()),
        Ok((_, Ok(PipelineLifecycleCompletion::ShutdownTimedOut))) => {
            super::write_pipeline_shutdown_timeout(document_id);
            Ok(())
        }
        Ok((_, Err(source))) => Err(PipelineSupervisorError::Lifecycle {
            document_id: document_id.clone(),
            source: Box::new(source),
        }),
        Err(source) => Err(PipelineSupervisorError::Task {
            document_id: document_id.clone(),
            source,
        }),
    }
}

fn unexpected_pipeline_task_failure(
    result: Result<(TaskId, PipelineLifecycleTaskOutput), JoinError>,
    document_id: TenonDocumentId,
) -> PipelineSupervisorError {
    match result {
        Ok((_, Ok(_))) => PipelineSupervisorError::StoppedUnexpectedly(document_id),
        Ok((_, Err(source))) => PipelineSupervisorError::Lifecycle {
            document_id,
            source: Box::new(source),
        },
        Err(source) => PipelineSupervisorError::Task {
            document_id,
            source,
        },
    }
}

#[derive(Debug)]
pub(super) enum PipelineSupervisorError {
    Lifecycle {
        document_id: TenonDocumentId,
        source: Box<RunnerPipelineLifecycleError>,
    },
    Task {
        document_id: TenonDocumentId,
        source: JoinError,
    },
    TaskIdentityMissing(TaskId),
    LifecycleIdentityMissing(TenonDocumentId),
    LifecycleIdentityMismatch(TenonDocumentId),
    TaskSetClosed,
    StateChannelClosed,
    Stopped,
    StoppedUnexpectedly(TenonDocumentId),
}

impl PipelineSupervisorError {
    pub(super) fn process_owners_recovered(&self) -> bool {
        match self {
            Self::Lifecycle { source, .. } => source.process_owner_recovered(),
            Self::Task { .. }
            | Self::TaskIdentityMissing(_)
            | Self::LifecycleIdentityMissing(_)
            | Self::LifecycleIdentityMismatch(_)
            | Self::TaskSetClosed => false,
            Self::StateChannelClosed | Self::Stopped | Self::StoppedUnexpectedly(_) => true,
        }
    }
}

impl fmt::Display for PipelineSupervisorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lifecycle { document_id, .. } => {
                write!(formatter, "Runner Pipeline lifecycle failed: {document_id}")
            }
            Self::Task { document_id, .. } => {
                write!(formatter, "Runner Pipeline task failed: {document_id}")
            }
            Self::TaskIdentityMissing(task_id) => write!(
                formatter,
                "Runner Pipeline task identity is missing: {task_id}"
            ),
            Self::LifecycleIdentityMissing(document_id) => write!(
                formatter,
                "Runner Pipeline lifecycle identity is missing: {document_id}"
            ),
            Self::LifecycleIdentityMismatch(document_id) => write!(
                formatter,
                "Runner Pipeline lifecycle identity changed before task completion: {document_id}"
            ),
            Self::TaskSetClosed => {
                formatter.write_str("Runner Pipeline task set closed unexpectedly")
            }
            Self::StateChannelClosed => {
                formatter.write_str("Runner Pipeline state channel closed unexpectedly")
            }
            Self::Stopped => formatter.write_str("Runner Pipeline supervisor was already stopped"),
            Self::StoppedUnexpectedly(document_id) => write!(
                formatter,
                "Runner Pipeline lifecycle stopped unexpectedly: {document_id}"
            ),
        }
    }
}

impl Error for PipelineSupervisorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Lifecycle { source, .. } => Some(source.as_ref()),
            Self::Task { source, .. } => Some(source),
            Self::TaskIdentityMissing(_)
            | Self::LifecycleIdentityMissing(_)
            | Self::LifecycleIdentityMismatch(_)
            | Self::TaskSetClosed
            | Self::StateChannelClosed
            | Self::Stopped
            | Self::StoppedUnexpectedly(_) => None,
        }
    }
}

enum PipelineSupervisorPhase {
    Running(PipelineLifecycleContext),
    AdmissionClosed(PipelineLifecycleContext),
    Draining,
}

/// Runner-side handle for changing or stopping one Pipeline lifecycle.
struct PipelineLifecycleControl {
    targets: watch::Sender<Option<Arc<PipelineLifecycleTarget>>>,
    execution_admission: watch::Sender<PipelineExecutionAdmission>,
    shutdown: oneshot::Sender<()>,
}

impl PipelineLifecycleControl {
    /// Replaces the desired runnable target without queuing obsolete revisions.
    fn set_target(&self, target: Option<Arc<PipelineLifecycleTarget>>) {
        drop(self.targets.send_replace(target));
    }

    /// Prevents future process spawns and target publication while a live
    /// process continues its accepted work until full shutdown.
    fn close_execution_admission(&self) {
        let _previous = self
            .execution_admission
            .send_replace(PipelineExecutionAdmission::Closed);
    }

    /// Requests planned termination and consumes this lifecycle handle.
    fn stop(self) {
        let _ = self.shutdown.send(());
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PipelineExecutionAdmission {
    Open,
    Closed,
}

struct PipelineLifecycleSlot {
    identity: PipelineLifecycleIdentity,
    phase: PipelineLifecyclePhase,
}

enum PipelineLifecyclePhase {
    Active(PipelineLifecycleControl),
    Retiring {
        pending_target: Option<Arc<PipelineLifecycleTarget>>,
    },
}

struct PipelineTaskIdentity {
    document_id: TenonDocumentId,
    lifecycle: PipelineLifecycleIdentity,
}

type PipelineLifecycleTaskOutput =
    Result<PipelineLifecycleCompletion, RunnerPipelineLifecycleError>;
type JoinedPipelineTask = Result<(TaskId, PipelineLifecycleTaskOutput), JoinError>;
type JoinedPipelineLifecycleTask = Option<JoinedPipelineTask>;

#[cfg(test)]
pub(super) mod test_support {
    use super::*;
    use std::future::Future;

    pub(super) fn supervisor(
        tasks: JoinSet<PipelineLifecycleTaskOutput>,
        task_documents: HashMap<TaskId, TenonDocumentId>,
        state_updates: mpsc::Receiver<PipelineLifecycleUpdate>,
    ) -> PipelineSupervisor {
        PipelineSupervisor {
            lifecycles: HashMap::new(),
            tasks,
            task_documents: task_documents
                .into_iter()
                .map(|(task_id, document_id)| {
                    (
                        task_id,
                        PipelineTaskIdentity {
                            document_id,
                            lifecycle: PipelineLifecycleIdentity::new(),
                        },
                    )
                })
                .collect(),
            state_updates,
            phase: PipelineSupervisorPhase::Draining,
            drain_result: PipelineDrainResult::default(),
        }
    }

    pub(in crate::runner) fn supervisor_with_final_state(
        config: Arc<RunnerConfig>,
        pipeline_executable: PathBuf,
        runtime_directory: PathBuf,
        launcher: RunnerPipelineLauncher,
        update: PipelineStateUpdate,
        publish_after: impl Future<Output = ()> + Send + 'static,
    ) -> (PipelineSupervisor, oneshot::Receiver<()>) {
        let document_id = update.document_id.clone();
        let identity = PipelineLifecycleIdentity::new();
        let state_identity = identity.clone();
        let (state_sender, state_updates) = mpsc::channel(1);
        let context = PipelineLifecycleContext::new(
            config,
            pipeline_executable,
            runtime_directory,
            launcher,
            state_sender.clone(),
            ExecutionExpiry::default(),
        );
        let (published, publication) = oneshot::channel();
        let mut tasks = JoinSet::new();
        let task = tasks.spawn(async move {
            publish_after.await;
            state_sender
                .send(PipelineLifecycleUpdate {
                    identity: state_identity,
                    update,
                })
                .await
                .map_err(|_| RunnerPipelineLifecycleError::StateReceiverClosed)?;
            let _ = published.send(());
            Ok(PipelineLifecycleCompletion::Stopped)
        });
        let control = lifecycle::test_support::lifecycle_control();
        let supervisor = PipelineSupervisor {
            lifecycles: HashMap::from([(
                document_id.clone(),
                PipelineLifecycleSlot {
                    identity: identity.clone(),
                    phase: PipelineLifecyclePhase::Active(control),
                },
            )]),
            tasks,
            task_documents: HashMap::from([(
                task.id(),
                PipelineTaskIdentity {
                    document_id,
                    lifecycle: identity,
                },
            )]),
            state_updates,
            phase: PipelineSupervisorPhase::Running(context),
            drain_result: PipelineDrainResult::default(),
        };
        (supervisor, publication)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload_contract::PluginInterface;
    use crate::runner::control_server::test_support::pipeline_launcher;
    use crate::runner::management::PipelineLifecycleState;
    use crate::runner::pipeline::PipelineCleanupError;
    use crate::runner::pipeline_supervisor::lifecycle::test_support::target;
    use crate::runner::test_support::{install_plugin, load_config};
    use std::future::{poll_fn, ready};
    use std::task::Poll;
    use std::time::Duration;
    use std::{io, panic};
    use tokio::time;

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_drain_records_a_final_state_before_reaping_its_task() -> io::Result<()> {
        let document_id = TenonDocumentId::try_from("shutdown-state").map_err(io::Error::other)?;
        let directory = tempfile::tempdir()?;
        let (mut supervisor, _publication) = test_support::supervisor_with_final_state(
            Arc::new(load_config(directory.path())?),
            PathBuf::from("/tmp/unused-tenon-executable"),
            directory.path().to_path_buf(),
            pipeline_launcher(PathBuf::from("/tmp/unused-tenon-control.sock"))?,
            PipelineStateUpdate {
                document_id,
                state: PipelineLifecycleState::Starting,
            },
            ready(()),
        );
        supervisor.close_execution_admission();

        assert!(matches!(
            supervisor.next_event().await,
            PipelineEvent::State {
                role: PipelineLifecycleRole::Current,
                ..
            }
        ));
        let cleanup = supervisor.shutdown().await;
        assert!(cleanup.failures.is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replacement_waits_for_the_retiring_lifecycle() -> io::Result<()> {
        let state_directory = tempfile::tempdir()?;
        install_plugin(state_directory.path(), PluginInterface::Source)?;
        install_plugin(state_directory.path(), PluginInterface::Sink)?;
        let config = Arc::new(load_config(state_directory.path())?);
        let first = Arc::new(target(
            state_directory.path(),
            &config,
            "same-document",
            "function main(event) emit() end",
        )?);
        let replacement = Arc::new(target(
            state_directory.path(),
            &config,
            "same-document",
            "function main(event) emit(event.payload) end",
        )?);
        let launcher = pipeline_launcher(PathBuf::from("/tmp/unused-tenon-control.sock"))?;
        let mut supervisor = PipelineSupervisor::new(
            Arc::clone(&config),
            PathBuf::from("/tmp/unused-tenon-executable"),
            state_directory.path().to_path_buf(),
            launcher,
            Box::new([]),
            ExecutionExpiry::default(),
        );
        let document_id = TenonDocumentId::try_from("same-document").map_err(io::Error::other)?;

        supervisor
            .apply(Box::new([PipelineDirective::SetTarget {
                document_id: document_id.clone(),
                target: Some(first),
            }]))
            .map_err(io::Error::other)?;
        supervisor
            .apply(Box::new([PipelineDirective::Stop {
                document_id: document_id.clone(),
            }]))
            .map_err(io::Error::other)?;
        supervisor
            .apply(Box::new([PipelineDirective::SetTarget {
                document_id: document_id.clone(),
                target: Some(Arc::clone(&replacement)),
            }]))
            .map_err(io::Error::other)?;

        assert_eq!(supervisor.tasks.len(), 1);
        assert!(matches!(
            supervisor.lifecycles.get(&document_id).map(|slot| &slot.phase),
            Some(PipelineLifecyclePhase::Retiring {
                pending_target: Some(target),
            }) if Arc::ptr_eq(target, &replacement)
        ));
        let retiring_identity = supervisor
            .lifecycles
            .get(&document_id)
            .map(|slot| slot.identity.clone())
            .ok_or_else(|| io::Error::other("Retiring lifecycle identity is missing"))?;
        let joined = time::timeout(Duration::from_secs(1), supervisor.tasks.join_next_with_id())
            .await
            .map_err(|_| io::Error::other("Retiring lifecycle did not stop"))?;

        assert!(supervisor.joined_task_event(joined).is_none());
        assert_eq!(supervisor.tasks.len(), 1);
        assert!(matches!(
            supervisor
                .lifecycles
                .get(&document_id)
                .map(|slot| &slot.phase),
            Some(PipelineLifecyclePhase::Active(_))
        ));
        let stale_update = PipelineLifecycleUpdate {
            identity: retiring_identity,
            update: PipelineStateUpdate {
                document_id,
                state: PipelineLifecycleState::Starting,
            },
        };
        assert!(matches!(
            supervisor.lifecycle_role(&stale_update),
            PipelineLifecycleRole::Retiring
        ));
        let result = supervisor.shutdown().await;
        assert!(result.failures.is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn closed_spawn_admission_blocks_new_and_retiring_lifecycles() -> io::Result<()> {
        let state_directory = tempfile::tempdir()?;
        install_plugin(state_directory.path(), PluginInterface::Source)?;
        install_plugin(state_directory.path(), PluginInterface::Sink)?;
        let config = Arc::new(load_config(state_directory.path())?);
        let first = Arc::new(target(
            state_directory.path(),
            &config,
            "closed-supervisor-admission",
            "function main(event) emit() end",
        )?);
        let replacement = Arc::new(target(
            state_directory.path(),
            &config,
            "closed-supervisor-admission",
            "function main(event) emit(event.payload) end",
        )?);
        let launcher = pipeline_launcher(PathBuf::from("/tmp/unused-tenon-control.sock"))?;
        let mut supervisor = PipelineSupervisor::new(
            Arc::clone(&config),
            PathBuf::from("/tmp/unused-tenon-executable"),
            state_directory.path().to_path_buf(),
            launcher,
            Box::new([]),
            ExecutionExpiry::default(),
        );
        let document_id =
            TenonDocumentId::try_from("closed-supervisor-admission").map_err(io::Error::other)?;

        supervisor
            .apply(Box::new([PipelineDirective::SetTarget {
                document_id: document_id.clone(),
                target: Some(first),
            }]))
            .map_err(io::Error::other)?;
        supervisor
            .apply(Box::new([PipelineDirective::Stop {
                document_id: document_id.clone(),
            }]))
            .map_err(io::Error::other)?;
        supervisor.close_execution_admission();
        supervisor
            .apply(Box::new([PipelineDirective::SetTarget {
                document_id: document_id.clone(),
                target: Some(Arc::clone(&replacement)),
            }]))
            .map_err(io::Error::other)?;

        assert!(matches!(
            supervisor
                .lifecycles
                .get(&document_id)
                .map(|slot| &slot.phase),
            Some(PipelineLifecyclePhase::Retiring {
                pending_target: None,
            })
        ));
        let joined = time::timeout(Duration::from_secs(1), supervisor.tasks.join_next_with_id())
            .await
            .map_err(|_| io::Error::other("Retiring lifecycle did not stop"))?;
        assert!(supervisor.joined_task_event(joined).is_none());
        assert!(supervisor.tasks.is_empty());

        let another_id =
            TenonDocumentId::try_from("closed-new-lifecycle").map_err(io::Error::other)?;
        supervisor
            .apply(Box::new([PipelineDirective::SetTarget {
                document_id: another_id.clone(),
                target: Some(replacement),
            }]))
            .map_err(io::Error::other)?;
        assert!(!supervisor.lifecycles.contains_key(&another_id));
        assert!(supervisor.tasks.is_empty());
        supervisor.close_execution_admission();
        {
            let next = supervisor.next_event();
            tokio::pin!(next);
            poll_fn(|context| {
                assert!(next.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        assert!(supervisor.shutdown().await.failures.is_empty());
        assert!(matches!(
            supervisor.apply(Box::new([])),
            Err(PipelineSupervisorError::Stopped)
        ));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn task_drain_discards_a_final_state_published_immediately_before_exit() -> io::Result<()>
    {
        let document_id = TenonDocumentId::try_from("final-state").map_err(io::Error::other)?;
        let (sender, receiver) = mpsc::channel(1);
        let mut tasks = JoinSet::new();
        let task_document_id = document_id.clone();
        let state_document_id = task_document_id.clone();
        let lifecycle_identity = PipelineLifecycleIdentity::new();
        let state_identity = lifecycle_identity.clone();
        let task = tasks.spawn(async move {
            sender
                .send(PipelineLifecycleUpdate {
                    identity: state_identity,
                    update: PipelineStateUpdate {
                        document_id: state_document_id,
                        state: PipelineLifecycleState::Starting,
                    },
                })
                .await
                .map_err(|_| RunnerPipelineLifecycleError::StateReceiverClosed)?;
            Ok(PipelineLifecycleCompletion::Stopped)
        });
        let task_documents = HashMap::from([(task.id(), task_document_id)]);
        let mut pipelines = test_support::supervisor(tasks, task_documents, receiver);
        let result = pipelines.shutdown().await;

        assert!(result.failures.is_empty());
        assert!(result.timed_out_documents.is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn panicked_pipeline_task_keeps_its_document_identity() -> io::Result<()> {
        let document_id = TenonDocumentId::try_from("panicked-task").map_err(io::Error::other)?;
        let mut tasks = JoinSet::new();
        let task = tasks.spawn(panic_pipeline_task());
        let task_documents = HashMap::from([(task.id(), document_id.clone())]);
        let (sender, receiver) = mpsc::channel(1);
        drop(sender);
        let mut pipelines = test_support::supervisor(tasks, task_documents, receiver);
        let result = pipelines.shutdown().await;

        assert!(matches!(
            result.failures.as_slice(),
            [PipelineSupervisorError::Task {
                document_id: failed_id,
                ..
            }] if failed_id == &document_id
        ));
        assert_eq!(result.cleanup, PipelineCleanup::Incomplete);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_shutdown_retains_consumed_failures_and_timeouts() -> io::Result<()> {
        let failed = TenonDocumentId::try_from("failed-task").map_err(io::Error::other)?;
        let timed_out = TenonDocumentId::try_from("timed-out-task").map_err(io::Error::other)?;
        let panicked = TenonDocumentId::try_from("panicked-task").map_err(io::Error::other)?;
        let waiting = TenonDocumentId::try_from("waiting-task").map_err(io::Error::other)?;
        let mut tasks = JoinSet::new();
        let mut task_documents = HashMap::new();
        let (finished, mut completions) = mpsc::channel(3);
        for (document_id, outcome) in [
            (
                failed.clone(),
                Err(RunnerPipelineLifecycleError::Cleanup(
                    PipelineCleanupError::Process(io::Error::other("injected wait failure")),
                )),
            ),
            (
                timed_out.clone(),
                Ok(PipelineLifecycleCompletion::ShutdownTimedOut),
            ),
        ] {
            let finished = finished.clone();
            let task = tasks.spawn(async move {
                let _ = finished.send(()).await;
                outcome
            });
            task_documents.insert(task.id(), document_id);
        }
        let task = tasks.spawn(async move {
            let _ = finished.send(()).await;
            panic_pipeline_task().await
        });
        task_documents.insert(task.id(), panicked.clone());
        let (release, released) = oneshot::channel();
        let (state_sender, state_updates) = mpsc::channel(1);
        let task = tasks.spawn(async move {
            let _ = released.await;
            drop(state_sender);
            Ok(PipelineLifecycleCompletion::Stopped)
        });
        task_documents.insert(task.id(), waiting);
        for _ in 0..3 {
            completions
                .recv()
                .await
                .ok_or_else(|| io::Error::other("Task did not finish"))?;
        }
        let mut supervisor = test_support::supervisor(tasks, task_documents, state_updates);
        {
            let stopping = supervisor.shutdown();
            tokio::pin!(stopping);
            poll_fn(|context| {
                assert!(stopping.as_mut().poll(context).is_pending());
                Poll::Ready(())
            })
            .await;
        }
        assert_eq!(supervisor.tasks.len(), 1);
        assert_eq!(supervisor.task_documents.len(), 1);
        release
            .send(())
            .map_err(|_| io::Error::other("Waiting task disappeared"))?;
        let result = supervisor.shutdown().await;
        assert_eq!(result.cleanup, PipelineCleanup::Incomplete);
        assert_eq!(result.timed_out_documents, [timed_out]);
        assert_eq!(result.failures.len(), 2);
        assert!(result.failures.iter().any(|failure| matches!(failure,
            PipelineSupervisorError::Lifecycle { document_id, .. } if document_id == &failed
        )));
        assert!(result.failures.iter().any(|failure| matches!(failure,
            PipelineSupervisorError::Task { document_id, .. } if document_id == &panicked
        )));
        assert!(supervisor.tasks.is_empty());
        assert!(supervisor.task_documents.is_empty());
        Ok(())
    }

    async fn panic_pipeline_task()
    -> Result<PipelineLifecycleCompletion, RunnerPipelineLifecycleError> {
        panic::resume_unwind(Box::new("injected Pipeline task panic"))
    }
}
