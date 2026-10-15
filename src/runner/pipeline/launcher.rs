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

//! Runner-side Pipeline process creation and exclusive process ownership.
//!
//! [`RunnerPipelineLauncher`] registers one launch before spawning its child.
//! The returned process owner retains the process tree, stdin lifetime channel,
//! attachment receiver, retained revisions, and the single startup deadline. The
//! launch registration stays inside that exact owner until the child is reaped.

use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Meter;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use tokio::process::{Child, ChildStdin, Command};

use super::control::session::{RunnerPipelineControlSession, RunnerPipelineControlSessionError};
use super::directory::{PipelineCleanupError, cleanup_pipeline_directory};
use super::launch_registry::{
    PipelineAttachmentError, PipelineLaunchRegistration, RunnerPipelineLaunchRegistry,
};
use super::published_revisions::PublishedPipelineRevisions;
use super::target::{PipelineLifecycleTarget, PipelineRunningState};
use crate::config::RunnerConfig;
use crate::error::ErrorChain;
use crate::identifiers::TenonDocumentId;
use crate::runner::process_resources::{PipelineResourceGroup, RunnerResources};
use crate::runner::process_tree::{
    PipelineProcessTree, PipelineShutdownError, PipelineShutdownOutcome,
};
use crate::time::Deadline;
use tokio::sync::oneshot;

/// Cloneable process-creation capability for one Runner control socket.
pub(in crate::runner) struct RunnerPipelineLauncher {
    launches: RunnerPipelineLaunchRegistry,
    control_socket: PathBuf,
    restarts: Option<Counter<u64>>,
    resources: Arc<RunnerResources>,
}

impl RunnerPipelineLauncher {
    #[must_use]
    pub(in crate::runner) fn new(
        launches: RunnerPipelineLaunchRegistry,
        control_socket: PathBuf,
        resources: Arc<RunnerResources>,
    ) -> Self {
        assert!(
            control_socket.is_absolute(),
            "Runner-owned control socket paths must remain absolute"
        );
        Self {
            launches,
            control_socket,
            restarts: None,
            resources,
        }
    }

    pub(in crate::runner) fn with_metrics(mut self, meter: Option<&Meter>) -> Self {
        self.restarts = meter.map(|meter| {
            meter
                .u64_counter("tenon.pipeline.restarts")
                .with_unit("{restart}")
                .build()
        });
        self
    }

    /// Registers and starts one Pipeline child process.
    ///
    /// Registration is complete before `spawn` can execute. The returned owner
    /// retains the child, stdin lifetime channel, attachment receiver, and the
    /// one startup deadline that begins immediately after `spawn` succeeds.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerPipelineLaunchError`] when the process cannot start.
    #[must_use = "a successful start owns a live child process"]
    pub(in crate::runner) fn start_pipeline(
        &self,
        executable: &Path,
        target: Arc<PipelineLifecycleTarget>,
        config: &RunnerConfig,
        working_directory: &Path,
    ) -> Result<RunnerPipelineProcess, RunnerPipelineLaunchError> {
        self.start_pipeline_with_spawn(
            executable,
            target,
            config,
            working_directory,
            |command, _| command.spawn(),
        )
    }

    pub(in crate::runner) fn record_restart(&self, document_id: &TenonDocumentId) {
        if let Some(restarts) = &self.restarts {
            restarts.add(
                1,
                &[KeyValue::new(
                    "tenon.pipeline.id",
                    document_id.as_str().to_owned(),
                )],
            );
        }
    }

    #[allow(
        clippy::expect_used,
        reason = "Runner config paths originate as UTF-8 strings and runtime children are ASCII"
    )]
    fn start_pipeline_with_spawn(
        &self,
        executable: &Path,
        target: Arc<PipelineLifecycleTarget>,
        config: &RunnerConfig,
        working_directory: &Path,
        spawn: impl FnOnce(&mut Command, &[u8]) -> io::Result<Child>,
    ) -> Result<RunnerPipelineProcess, RunnerPipelineLaunchError> {
        let control_socket = self
            .control_socket
            .to_str()
            .expect("Runner-owned control socket paths must remain UTF-8");
        let pending = self.launches.register(
            target.document_id().clone(),
            target.bootstrap(config, working_directory),
        );
        let plugin_directory =
            crate::plugin_control_directory(&self.control_socket, pending.launch_id());
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&plugin_directory)
            .map_err(RunnerPipelineLaunchError::ControlDirectoryCreate)?;
        let mut resources = PipelineLaunchResources {
            control_directory: plugin_directory,
            resource_group: None,
        };
        let launch_id = STANDARD.encode(pending.launch_id());
        let mut command = Command::new(executable);
        command
            .arg(crate::SUBCOMMAND)
            .arg("--control-socket")
            .arg(control_socket)
            .arg("--launch-id")
            .arg(launch_id)
            .stdin(Stdio::piped())
            .process_group(0)
            .kill_on_drop(true);
        resources.resource_group = match PipelineResourceGroup::prepare(
            &self.resources,
            &mut command,
            target.document(),
            pending.launch_id(),
        ) {
            Ok(group) => group,
            Err(error) => {
                resources.resource_group = error.group;
                return Err(resources.finish_failed_start(
                    RunnerPipelineLaunchError::ResourceLimits(error.failure),
                ));
            }
        };
        let mut child = match spawn(&mut command, pending.launch_id()) {
            Ok(child) => child,
            Err(failure) => {
                return Err(
                    resources.finish_failed_start(RunnerPipelineLaunchError::SpawnFailed(failure))
                );
            }
        };
        let startup_deadline = Deadline::start(config.pipeline_startup_timeout());
        #[allow(
            clippy::expect_used,
            reason = "Tokio guarantees Child::stdin is Some after Stdio::piped spawn succeeds"
        )]
        let lifetime_channel = child
            .stdin
            .take()
            .expect("piped Pipeline stdin must exist after successful spawn");
        let process_tree = PipelineProcessTree::from_spawned_child(child)
            .map_err(RunnerPipelineLaunchError::ProcessTreeOwnershipFailed)?;

        let (registration, attached) = pending.into_parts();
        Ok(RunnerPipelineProcess {
            process_tree,
            resources,
            lifetime_channel,
            registration,
            connection: PipelineConnection::Pending(attached),
            startup_deadline,
            published_revisions: PublishedPipelineRevisions::new(target),
        })
    }
}

impl Clone for RunnerPipelineLauncher {
    fn clone(&self) -> Self {
        Self {
            launches: self.launches.clone(),
            control_socket: self.control_socket.clone(),
            restarts: self.restarts.clone(),
            resources: Arc::clone(&self.resources),
        }
    }
}

impl fmt::Debug for RunnerPipelineLauncher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunnerPipelineLauncher")
            .field("control_socket", &self.control_socket)
            .finish_non_exhaustive()
    }
}

/// Exclusive ownership of one Pipeline process, connection, and retained revisions.
///
/// Cancelling a wait leaves the process, registration, stdin lifetime channel,
/// revisions, and original deadline in this owner for the next explicit action.
#[must_use = "dropping this owner force-terminates its Pipeline process group"]
pub(in crate::runner) struct RunnerPipelineProcess {
    process_tree: PipelineProcessTree,
    resources: PipelineLaunchResources,
    #[allow(
        dead_code,
        reason = "ownership keeps the Pipeline parent-lifetime channel open"
    )]
    lifetime_channel: ChildStdin,
    registration: PipelineLaunchRegistration,
    connection: PipelineConnection,
    startup_deadline: Deadline,
    published_revisions: PublishedPipelineRevisions,
}

impl RunnerPipelineProcess {
    pub(in crate::runner) fn latest_document_etag(
        &self,
    ) -> crate::runner::document_store::TenonDocumentEtag {
        self.published_revisions.latest_etag()
    }

    /// Waits for Attach while also enforcing child exit and the one startup
    /// deadline that began after process creation.
    ///
    /// Cancelling this method leaves this owner unchanged. A failure removes
    /// the pending registration and explicitly attempts to kill and reap the
    /// child before returning. If cleanup itself fails, this owner still holds
    /// the child so the caller can retry or terminate the Runner.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerPipelineLaunchError`] when the child exits or cannot be
    /// observed, attachment fails, the deadline is reached, or forced cleanup
    /// fails.
    #[allow(
        clippy::panic,
        reason = "the lifecycle advances after its single successful attachment wait"
    )]
    pub(in crate::runner) async fn wait_for_attachment(
        &mut self,
    ) -> Result<(), RunnerPipelineLaunchError> {
        let PipelineConnection::Pending(attached) = &mut self.connection else {
            panic!("Pipeline attachment is awaited only before it completes");
        };
        let deadline = self.startup_deadline.wait();
        tokio::pin!(deadline);
        let attachment_result = tokio::select! {
            biased;
            () = &mut deadline => {
                return Err(self
                    .cleanup_failure(RunnerPipelineLaunchError::AttachmentTimedOut)
                    .await);
            },
            result = attached => result.map_err(|_| PipelineAttachmentError),
            result = self.process_tree.wait() => {
                self.registration.remove();
                return match result {
                    Ok(status) => Err(
                        RunnerPipelineLaunchError::ExitedBeforeAttachmentCompleted(status),
                    ),
                    Err(source) => Err(self
                        .cleanup_failure(RunnerPipelineLaunchError::ProcessTreeWaitFailed(source))
                        .await),
                };
            },
        };

        match attachment_result {
            Ok(session) => match self.process_tree.try_wait() {
                Ok(None) => {
                    self.connection = PipelineConnection::Attached {
                        session,
                        startup_failure: None,
                    };
                    Ok(())
                }
                Ok(Some(status)) => Err(
                    RunnerPipelineLaunchError::ExitedBeforeAttachmentCompleted(status),
                ),
                Err(source) => Err(self
                    .cleanup_failure(RunnerPipelineLaunchError::ProcessTreeWaitFailed(source))
                    .await),
            },
            Err(source) => Err(self
                .cleanup_failure(RunnerPipelineLaunchError::AttachmentFailed(source))
                .await),
        }
    }

    /// Waits for the first complete Pipeline status under the
    /// launch's original startup deadline.
    ///
    /// Attachment does not restart the deadline. Cancelling this wait retains
    /// both the inbound status and the same absolute deadline. Every terminal
    /// failure explicitly terminates and reaps a still-running child before it
    /// is returned to the Runner control loop.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerPipelineStartupError`] when the deadline expires, the
    /// child exits, the control stream fails, or mandatory cleanup fails.
    pub(in crate::runner) async fn wait_for_startup_status(
        &mut self,
    ) -> Result<PipelineRunningState, RunnerPipelineStartupError> {
        let (session, startup_failure) = self.connection.attached_parts();
        if let Some(failure) = startup_failure.clone() {
            return Err(self.finish_startup_failure(failure).await);
        }

        let deadline = self.startup_deadline.wait();
        tokio::pin!(deadline);
        let message = tokio::select! {
            biased;
            () = &mut deadline => {
                return Err(self.fail_startup(RunnerPipelineStartupFailure::TimedOut).await);
            },
            status = self.process_tree.wait() => {
                let failure = match status {
                    Ok(status) => RunnerPipelineStartupFailure::ExitedBeforeCompletion(status),
                    Err(source) => RunnerPipelineStartupFailure::ProcessTreeWait(Arc::new(source)),
                };
                return Err(self.fail_startup(failure).await);
            },
            message = session.receive_pipeline_message() => match message {
                Ok(message) => message,
                Err(source) => {
                    return Err(self
                        .fail_startup(RunnerPipelineStartupFailure::Control(Arc::new(source)))
                        .await);
                }
            },
        };

        Ok(self.published_revisions.observe(message))
    }

    /// Publishes the latest complete Runtime-Integrity-passed revision on this
    /// child's bound control stream.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerPipelineControlSessionError::Disconnected`] after the response
    /// stream has closed.
    pub(in crate::runner) fn publish_target(
        &mut self,
        target: Arc<PipelineLifecycleTarget>,
    ) -> Result<PipelineTargetPublication, RunnerPipelineControlSessionError> {
        if self
            .published_revisions
            .requires_resource_replacement(&target)
        {
            return Ok(PipelineTargetPublication::ReplaceProcess);
        }
        let (session, _) = self.connection.attached_parts();
        self.published_revisions
            .publish(target, |target| session.publish_revision(target.revision()))?;
        Ok(PipelineTargetPublication::Published)
    }

    /// Waits for either the next inbound Pipeline message or child exit.
    ///
    /// Both waits are cancellation safe. Cancelling this method leaves the
    /// message and process status available to the next call, so the Runner
    /// control loop can select another event without losing either fact.
    ///
    /// # Errors
    ///
    /// Returns [`RunnerPipelineProcessError`] when the bound control stream or
    /// operating-system child wait fails.
    pub(in crate::runner) async fn next_event(
        &mut self,
    ) -> Result<RunnerPipelineProcessEvent, RunnerPipelineProcessError> {
        let (session, _) = self.connection.attached_parts();
        tokio::select! {
            biased;
            status = self.process_tree.wait() => status
                .map(RunnerPipelineProcessEvent::Exited)
                .map_err(RunnerPipelineProcessError::ProcessTreeWait),
            message = session.receive_pipeline_message() => {
                let message = message.map_err(RunnerPipelineProcessError::Control)?;
                Ok(RunnerPipelineProcessEvent::Status(
                    self.published_revisions.observe(message),
                ))
            },
        }
    }

    /// Force-terminates the process group and reaps the direct child.
    ///
    /// # Errors
    ///
    /// Returns the operating-system error if the group cannot be terminated or
    /// the direct child cannot be reaped. This owner remains available after an
    /// error so cleanup can be retried without losing either responsibility.
    pub(in crate::runner) async fn force_kill_and_reap(&mut self) -> io::Result<()> {
        self.cancel_pending_attachment();
        self.process_tree.force_kill_and_reap().await.map(|_| ())
    }

    /// Requests ordered Pipeline shutdown, then force-cleans the complete
    /// process group if the Runner-wide deadline expires.
    ///
    /// # Errors
    ///
    /// Returns the operating-system error if termination, process-group cleanup,
    /// or direct-child reaping cannot be completed.
    pub(in crate::runner) async fn terminate_and_reap(
        &mut self,
        shutdown_timeout: Duration,
    ) -> Result<PipelineShutdownOutcome, PipelineShutdownError> {
        self.cancel_pending_attachment();
        self.process_tree.terminate_and_reap(shutdown_timeout).await
    }

    /// Releases resources after the process exits. The caller keeps this owner after an error.
    ///
    /// The lifecycle calls this only after it has reaped the Pipeline process.
    /// The owner keeps its resource group until group cleanup is completed.
    ///
    /// # Errors
    ///
    /// Returns a resource-group, task, or filesystem error if cleanup is not completed.
    ///
    /// # Panics
    ///
    /// Panics if the lifecycle calls this before it has reaped the Pipeline process.
    pub(in crate::runner) async fn cleanup_control_directory(
        &mut self,
    ) -> Result<(), PipelineCleanupError> {
        assert!(
            self.process_tree.process_id().is_none(),
            "Pipeline control directory cleanup follows process-group reaping"
        );
        self.resources.cleanup().await
    }

    pub(in crate::runner) async fn cleanup(&mut self) -> Result<(), PipelineCleanupError> {
        if self.process_tree.process_id().is_some() {
            self.force_kill_and_reap()
                .await
                .map_err(PipelineCleanupError::Process)?;
        }
        self.cleanup_control_directory().await
    }

    async fn cleanup_failure(
        &mut self,
        failure: RunnerPipelineLaunchError,
    ) -> RunnerPipelineLaunchError {
        self.registration.remove();
        match self.process_tree.force_kill_and_reap().await {
            Ok(_) => failure,
            Err(cleanup) => RunnerPipelineLaunchError::CleanupFailed {
                failure: Box::new(failure),
                cleanup,
            },
        }
    }

    fn cancel_pending_attachment(&self) {
        if matches!(self.connection, PipelineConnection::Pending(_)) {
            self.registration.remove();
        }
    }

    async fn fail_startup(
        &mut self,
        failure: RunnerPipelineStartupFailure,
    ) -> RunnerPipelineStartupError {
        let (_, startup_failure) = self.connection.attached_parts();
        let failure = startup_failure.get_or_insert(failure).clone();
        self.finish_startup_failure(failure).await
    }

    async fn finish_startup_failure(
        &mut self,
        failure: RunnerPipelineStartupFailure,
    ) -> RunnerPipelineStartupError {
        match self.process_tree.force_kill_and_reap().await {
            Ok(_) => RunnerPipelineStartupError::Failed(failure),
            Err(cleanup) => RunnerPipelineStartupError::CleanupFailed { failure, cleanup },
        }
    }
}

/// Owns launch resources until cleanup is completed.
/// This includes resources from launch attempts that return an error.
pub(in crate::runner) struct PipelineLaunchResources {
    control_directory: PathBuf,
    resource_group: Option<PipelineResourceGroup>,
}

impl PipelineLaunchResources {
    pub(in crate::runner) async fn cleanup(&mut self) -> Result<(), PipelineCleanupError> {
        if let Some(group) = &self.resource_group {
            group
                .cleanup()
                .await
                .map_err(PipelineCleanupError::ResourceGroup)?;
            self.resource_group.take();
        }
        cleanup_pipeline_directory(self.control_directory.clone()).await
    }

    fn finish_failed_start(
        mut self,
        failure: RunnerPipelineLaunchError,
    ) -> RunnerPipelineLaunchError {
        let cleanup = (|| {
            if let Some(group) = &self.resource_group {
                group.discard_empty()?;
                self.resource_group.take();
            }
            fs::remove_dir(&self.control_directory)
        })();
        match cleanup {
            Ok(()) => failure,
            Err(cleanup) => RunnerPipelineLaunchError::LaunchCleanupPending {
                failure: Box::new(failure),
                cleanup,
                resources: Box::new(self),
            },
        }
    }
}

impl fmt::Debug for PipelineLaunchResources {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PipelineLaunchResources")
            .field("control_directory", &self.control_directory)
            .finish_non_exhaustive()
    }
}

/// Whether a complete target can remain in this process's resource envelope.
pub(in crate::runner) enum PipelineTargetPublication {
    Published,
    ReplaceProcess,
}

impl fmt::Debug for RunnerPipelineProcess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunnerPipelineProcess")
            .field("process_id", &self.process_tree.process_id())
            .field("connection", &self.connection)
            .finish_non_exhaustive()
    }
}

/// The next externally observable fact from one attached Pipeline process.
#[derive(Debug)]
pub(in crate::runner) enum RunnerPipelineProcessEvent {
    /// One complete status matched its retained revision.
    Status(PipelineRunningState),
    /// The operating system reaped the Pipeline child.
    Exited(ExitStatus),
}

/// An attached Pipeline process can no longer produce control events normally.
#[derive(Debug)]
pub(in crate::runner) enum RunnerPipelineProcessError {
    /// The bound gRPC stream failed or disconnected.
    Control(RunnerPipelineControlSessionError),
    /// The direct child wait or remaining process-group cleanup failed.
    ProcessTreeWait(io::Error),
}

impl fmt::Display for RunnerPipelineProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(_) => formatter.write_str("Pipeline control session failed"),
            Self::ProcessTreeWait(_) => formatter.write_str("Pipeline process tree wait failed"),
        }
    }
}

impl Error for RunnerPipelineProcessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::ProcessTreeWait(source) => Some(source),
        }
    }
}

/// The terminal fact that prevented one Pipeline from completing startup.
#[derive(Clone, Debug)]
pub(in crate::runner) enum RunnerPipelineStartupFailure {
    /// The bound gRPC stream failed or disconnected.
    Control(Arc<RunnerPipelineControlSessionError>),
    /// The direct child wait or remaining process-group cleanup failed.
    ProcessTreeWait(Arc<io::Error>),
    /// The child exited after Attach but before its first complete status.
    ExitedBeforeCompletion(ExitStatus),
    /// The original launch deadline elapsed before the first complete status.
    TimedOut,
}

impl fmt::Display for RunnerPipelineStartupFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(_) => formatter.write_str("Pipeline control session failed"),
            Self::ProcessTreeWait(_) => formatter.write_str("Pipeline process tree wait failed"),
            Self::ExitedBeforeCompletion(status) => write!(
                formatter,
                "Pipeline process exited before startup completed with {status}"
            ),
            Self::TimedOut => formatter.write_str("Pipeline process startup timed out"),
        }
    }
}

impl Error for RunnerPipelineStartupFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(source) => Some(source.as_ref()),
            Self::ProcessTreeWait(source) => Some(source.as_ref()),
            Self::ExitedBeforeCompletion(_) | Self::TimedOut => None,
        }
    }
}

/// Startup failure plus the result of mandatory child cleanup.
#[derive(Debug)]
pub(in crate::runner) enum RunnerPipelineStartupError {
    /// The original failure was retained and the child was reaped.
    Failed(RunnerPipelineStartupFailure),
    /// The original failure was retained but forced cleanup also failed.
    CleanupFailed {
        /// Original startup failure.
        failure: RunnerPipelineStartupFailure,
        /// Child termination or reaping failure.
        cleanup: io::Error,
    },
}

impl fmt::Display for RunnerPipelineStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed(_) => formatter.write_str("Pipeline startup attempt ended in failure"),
            Self::CleanupFailed { failure, cleanup } => write!(
                formatter,
                "Pipeline startup failed: {}; Pipeline process cleanup also failed: {}",
                ErrorChain(failure),
                ErrorChain(cleanup)
            ),
        }
    }
}

impl Error for RunnerPipelineStartupError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Failed(failure) => Some(failure),
            Self::CleanupFailed { .. } => None,
        }
    }
}

/// One Pipeline child could not reach an attached Runner control session.
#[derive(Debug)]
pub(in crate::runner) enum RunnerPipelineLaunchError {
    /// A launch error keeps resources for the lifecycle to release.
    LaunchCleanupPending {
        failure: Box<Self>,
        cleanup: io::Error,
        resources: Box<PipelineLaunchResources>,
    },
    /// Requested operating-system limits could not be established before exec.
    ResourceLimits(io::Error),
    /// The exact launch's private Plugin endpoint directory could not be created.
    ControlDirectoryCreate(io::Error),
    /// The operating system could not create the child process.
    SpawnFailed(io::Error),
    /// The spawned child could not be adopted as an owned process group.
    ProcessTreeOwnershipFailed(io::Error),
    /// The control service claimed the launch but could not hand over its session.
    AttachmentFailed(PipelineAttachmentError),
    /// The child exited before a live attached process could be returned.
    ExitedBeforeAttachmentCompleted(ExitStatus),
    /// The direct child wait or remaining process-group cleanup failed.
    ProcessTreeWaitFailed(io::Error),
    /// No attached session was available before the configured startup deadline.
    AttachmentTimedOut,
    /// Mandatory forced termination or reaping failed after another launch failure.
    CleanupFailed {
        failure: Box<Self>,
        cleanup: io::Error,
    },
}

impl fmt::Display for RunnerPipelineLaunchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceLimits(_) => {
                formatter.write_str("Pipeline resource limits could not be applied")
            }
            Self::ControlDirectoryCreate(_) => {
                formatter.write_str("Pipeline Plugin control directory could not be created")
            }
            Self::SpawnFailed(_) => formatter.write_str("Pipeline process could not be started"),
            Self::ProcessTreeOwnershipFailed(_) => {
                formatter.write_str("Pipeline process group ownership could not be established")
            }
            Self::AttachmentFailed(_) => {
                formatter.write_str("Pipeline control session attachment failed")
            }
            Self::ExitedBeforeAttachmentCompleted(status) => write!(
                formatter,
                "Pipeline process exited before attachment completed with {status}"
            ),
            Self::ProcessTreeWaitFailed(_) => {
                formatter.write_str("Pipeline process tree exit could not be completed")
            }
            Self::AttachmentTimedOut => {
                formatter.write_str("Pipeline process attachment timed out")
            }
            Self::CleanupFailed { failure, cleanup }
            | Self::LaunchCleanupPending {
                failure, cleanup, ..
            } => write!(
                formatter,
                "Pipeline launch failed: {}; Pipeline process cleanup also failed: {}",
                ErrorChain(failure.as_ref()),
                ErrorChain(cleanup)
            ),
        }
    }
}

impl Error for RunnerPipelineLaunchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ResourceLimits(source)
            | Self::ControlDirectoryCreate(source)
            | Self::SpawnFailed(source)
            | Self::ProcessTreeOwnershipFailed(source)
            | Self::ProcessTreeWaitFailed(source) => Some(source),
            Self::AttachmentFailed(source) => Some(source),
            Self::CleanupFailed { .. } | Self::LaunchCleanupPending { .. } => None,
            Self::ExitedBeforeAttachmentCompleted(_) | Self::AttachmentTimedOut => None,
        }
    }
}

#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "each process owns one inline session; boxing adds an allocation without reducing retained data"
)]
enum PipelineConnection {
    Pending(oneshot::Receiver<RunnerPipelineControlSession>),
    Attached {
        session: RunnerPipelineControlSession,
        startup_failure: Option<RunnerPipelineStartupFailure>,
    },
}

impl PipelineConnection {
    #[allow(
        clippy::panic,
        reason = "the lifecycle uses control only after attachment succeeds"
    )]
    fn attached_parts(
        &mut self,
    ) -> (
        &mut RunnerPipelineControlSession,
        &mut Option<RunnerPipelineStartupFailure>,
    ) {
        let Self::Attached {
            session,
            startup_failure,
        } = self
        else {
            panic!("Pipeline control operations follow successful attachment");
        };
        (session, startup_failure)
    }
}

#[cfg(test)]
pub(super) mod test_support {
    use super::*;

    pub(in crate::runner::pipeline) fn start_pipeline_with_spawn(
        launcher: &RunnerPipelineLauncher,
        executable: &Path,
        target: Arc<PipelineLifecycleTarget>,
        config: &RunnerConfig,
        working_directory: &Path,
        spawn: impl FnOnce(&mut Command, &[u8]) -> io::Result<Child>,
    ) -> Result<RunnerPipelineProcess, RunnerPipelineLaunchError> {
        launcher.start_pipeline_with_spawn(executable, target, config, working_directory, spawn)
    }

    pub(in crate::runner::pipeline) fn expire_startup_deadline(
        process: &mut RunnerPipelineProcess,
    ) {
        process.startup_deadline = Deadline::start(Duration::ZERO);
    }
}
