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

//! Pipeline process-group and Runner direct-child lifecycle.
//!
//! Every Pipeline is the leader of a private POSIX process group. Source and
//! Sink processes inherit that group, so the Runner can terminate every live
//! descendant even after the direct Pipeline child has exited. Normal
//! shutdown still signals only the Pipeline and lets it close Plugins in order;
//! the process-group signal is the final cleanup boundary.

use crate::error::ErrorChain;
use crate::time::Deadline;
use rustix::io::Errno;
use rustix::process::{
    Pid, Signal, WaitId, WaitIdOptions, kill_process, kill_process_group, waitid,
};
use std::fmt;
use std::io;
use std::process::ExitStatus;
use std::time::Duration;
use tokio::process::Child;
use tokio::signal::unix::{SignalKind, signal};

/// Exclusive ownership of one Pipeline child and every process in its group.
#[must_use = "dropping this owner force-terminates the Pipeline process group"]
pub(crate) struct PipelineProcessTree {
    child: Child,
    process_group: Option<PipelineProcessGroup>,
    shutdown_deadline: Option<Deadline>,
}

impl PipelineProcessTree {
    /// Adopts a child spawned as its own process-group leader with kill-on-drop enabled.
    ///
    /// # Errors
    ///
    /// Returns an error if the operating system did not expose a valid POSIX
    /// process id for the newly spawned child.
    pub(crate) fn from_spawned_child(child: Child) -> io::Result<Self> {
        let process_group = PipelineProcessGroup::from_child(&child)?;
        Ok(Self {
            child,
            process_group: Some(process_group),
            shutdown_deadline: None,
        })
    }

    /// Returns the direct Pipeline child id while it has not been reaped.
    #[must_use]
    pub(crate) fn process_id(&self) -> Option<u32> {
        self.child.id()
    }

    /// Observes the direct Pipeline child exit without reaping it, force-cleans
    /// any descendants, and only then reaps the direct child.
    ///
    /// Cancelling the wait preserves the child, process group, and exit status
    /// inside this owner. A later call resumes the same cleanup responsibility.
    pub(crate) async fn wait(&mut self) -> io::Result<ExitStatus> {
        let process_group = self.owned_process_group()?;
        process_group.wait_for_leader_exit().await?;
        process_group.force_kill()?;
        let status = self.child.wait().await?;
        self.process_group = None;
        self.shutdown_deadline = None;
        Ok(status)
    }

    /// Observes an already-exited child without blocking and cleans remaining
    /// descendants before returning its status.
    pub(crate) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        let process_group = self.owned_process_group()?;
        if !process_group.leader_has_exited()? {
            return Ok(None);
        }
        process_group.force_kill()?;
        let status = self.child.try_wait()?.ok_or_else(|| {
            io::Error::other("Exited Pipeline child did not retain a waitable status")
        })?;
        self.process_group = None;
        self.shutdown_deadline = None;
        Ok(Some(status))
    }

    /// Requests normal Pipeline shutdown and enforces one cancellation-safe
    /// total deadline before force-cleaning the complete process group.
    pub(crate) async fn terminate_and_reap(
        &mut self,
        timeout: Duration,
    ) -> Result<PipelineShutdownOutcome, PipelineShutdownError> {
        let deadline = match self.shutdown_deadline {
            Some(deadline) => deadline,
            None => {
                let process_group = match self.owned_process_group() {
                    Ok(process_group) => process_group,
                    Err(primary) => {
                        return Err(PipelineShutdownError::BeforeDeadline {
                            operation: "Pipeline process group is unavailable during shutdown",
                            primary,
                            cleanup: None,
                        });
                    }
                };
                if let Err(termination) = process_group.request_termination() {
                    return Err(self
                        .failure_before_deadline("Pipeline termination request failed", termination)
                        .await);
                }
                let deadline = Deadline::start(timeout);
                self.shutdown_deadline = Some(deadline);
                deadline
            }
        };
        let progress = tokio::select! {
            biased;
            result = self.wait() => ShutdownProgress::Exited(result),
            () = deadline.wait() => ShutdownProgress::DeadlineElapsed,
        };

        match progress {
            ShutdownProgress::Exited(Ok(_)) => Ok(PipelineShutdownOutcome::ExitedBeforeDeadline),
            ShutdownProgress::Exited(Err(failure)) => Err(self
                .failure_before_deadline("Pipeline shutdown wait failed", failure)
                .await),
            ShutdownProgress::DeadlineElapsed => self
                .force_kill_and_reap()
                .await
                .map(|_| PipelineShutdownOutcome::ForcedAfterDeadline)
                .map_err(PipelineShutdownError::CleanupAfterDeadline),
        }
    }

    /// Force-terminates the complete process group and reaps the direct child.
    ///
    /// The group is signalled before the direct child is reaped. Keeping the
    /// group leader waitable until after that signal prevents its numeric group
    /// id from being reused at the cleanup boundary. The group can disappear
    /// while its killed leader is becoming waitable; the owned child wait is
    /// therefore the only completion fact after the signal attempt.
    pub(crate) async fn force_kill_and_reap(&mut self) -> io::Result<ExitStatus> {
        self.force_kill_group()?;
        let status = self.child.wait().await?;
        self.process_group = None;
        self.shutdown_deadline = None;
        Ok(status)
    }

    async fn failure_before_deadline(
        &mut self,
        operation: &'static str,
        primary: io::Error,
    ) -> PipelineShutdownError {
        let cleanup = self.force_kill_and_reap().await.err();
        PipelineShutdownError::BeforeDeadline {
            operation,
            primary,
            cleanup,
        }
    }

    fn force_kill_group(&self) -> io::Result<()> {
        self.process_group
            .map_or(Ok(()), PipelineProcessGroup::force_kill)
    }

    fn owned_process_group(&self) -> io::Result<PipelineProcessGroup> {
        self.process_group
            .ok_or_else(|| io::Error::other("Pipeline process group is no longer owned"))
    }
}

impl fmt::Debug for PipelineProcessTree {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PipelineProcessTree")
            .field("process_id", &self.process_id())
            .field("process_group", &self.process_group)
            .field("shutdown_deadline", &self.shutdown_deadline)
            .finish_non_exhaustive()
    }
}

impl Drop for PipelineProcessTree {
    fn drop(&mut self) {
        let _ = self.force_kill_group();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PipelineProcessGroup(Pid);

impl PipelineProcessGroup {
    fn from_child(child: &Child) -> io::Result<Self> {
        let process_id = child
            .id()
            .ok_or_else(|| io::Error::other("Pipeline child process id is unavailable"))?;
        let process_id = i32::try_from(process_id)
            .ok()
            .and_then(Pid::from_raw)
            .ok_or_else(|| io::Error::other("Pipeline child process id is outside POSIX range"))?;
        Ok(Self(process_id))
    }

    fn force_kill(self) -> io::Result<()> {
        match kill_process_group(self.0, Signal::KILL) {
            Ok(()) => Ok(()),
            // macOS reports EPERM when only the waitable group leader remains
            // as a zombie. Every live Tenon descendant keeps the Runner's OS
            // identity and would make the same group signal deliverable.
            Err(Errno::SRCH | Errno::PERM) => Ok(()),
            Err(source) => Err(io::Error::from(source)),
        }
    }

    fn request_termination(self) -> io::Result<()> {
        match kill_process(self.0, Signal::TERM) {
            Ok(()) | Err(Errno::SRCH) => Ok(()),
            Err(source) => Err(io::Error::from(source)),
        }
    }

    async fn wait_for_leader_exit(self) -> io::Result<()> {
        let mut child_exits = signal(SignalKind::child())?;
        loop {
            if self.leader_has_exited()? {
                return Ok(());
            }
            child_exits
                .recv()
                .await
                .ok_or_else(|| io::Error::other("Pipeline child-exit signal stream closed"))?;
        }
    }

    fn leader_has_exited(self) -> io::Result<bool> {
        let status = waitid(
            WaitId::Pid(self.0),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        )?;
        Ok(status.is_some())
    }
}

enum ShutdownProgress {
    Exited(io::Result<ExitStatus>),
    DeadlineElapsed,
}

/// How one planned Pipeline shutdown reached its terminal process boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PipelineShutdownOutcome {
    /// The Pipeline completed its own ordered shutdown before the deadline.
    ExitedBeforeDeadline,
    /// The Runner force-terminated the process group at the deadline.
    ForcedAfterDeadline,
}

/// A planned Pipeline shutdown failure that preserves whether its deadline had
/// already elapsed before mandatory process-group cleanup failed.
#[derive(Debug)]
pub(crate) enum PipelineShutdownError {
    /// Termination, waiting, or cleanup failed before the deadline won.
    BeforeDeadline {
        /// The operation that first failed.
        operation: &'static str,
        /// The original operation failure.
        primary: io::Error,
        /// A mandatory process-group cleanup failure, when cleanup also failed.
        cleanup: Option<io::Error>,
    },
    /// The deadline elapsed and the subsequent force-kill or reap failed.
    CleanupAfterDeadline(io::Error),
}

impl PipelineShutdownError {
    /// Returns whether the configured shutdown deadline had already elapsed.
    #[must_use]
    pub(crate) const fn deadline_elapsed(&self) -> bool {
        matches!(self, Self::CleanupAfterDeadline(_))
    }
}

impl fmt::Display for PipelineShutdownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeDeadline {
                operation,
                primary,
                cleanup: Some(cleanup),
            } => write!(
                formatter,
                "{operation}: {}; process-group cleanup also failed: {}",
                ErrorChain(primary),
                ErrorChain(cleanup)
            ),
            Self::BeforeDeadline {
                operation,
                cleanup: None,
                ..
            } => formatter.write_str(operation),
            Self::CleanupAfterDeadline(_) => formatter
                .write_str("Pipeline shutdown deadline elapsed and process-group cleanup failed"),
        }
    }
}

impl std::error::Error for PipelineShutdownError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BeforeDeadline {
                primary,
                cleanup: None,
                ..
            }
            | Self::CleanupAfterDeadline(primary) => Some(primary),
            Self::BeforeDeadline {
                cleanup: Some(_), ..
            } => None,
        }
    }
}

impl From<PipelineShutdownError> for io::Error {
    fn from(error: PipelineShutdownError) -> Self {
        let kind = match &error {
            PipelineShutdownError::BeforeDeadline { primary, .. }
            | PipelineShutdownError::CleanupAfterDeadline(primary) => primary.kind(),
        };
        io::Error::new(kind, error)
    }
}

#[cfg(test)]
mod tests;

#[cfg(all(test, not(feature = "loom-model")))]
pub(super) mod test_support;
