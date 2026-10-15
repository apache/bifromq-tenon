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

//! Failures and restart diagnostics for one Pipeline lifecycle owner.

use crate::error::ErrorChain;
use crate::identifiers::TenonDocumentId;
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::management::PipelineAttemptError;
use crate::runner::pipeline::{
    PipelineCleanupError, RunnerPipelineLaunchError, RunnerPipelineProcessError,
    RunnerPipelineStartupError, RunnerPipelineStartupFailure,
};
use std::error::Error;
use std::fmt;
use std::process::ExitStatus;

#[derive(Debug)]
pub(in crate::runner) enum PipelineRestartCause {
    Launch(RunnerPipelineLaunchError),
    Startup(RunnerPipelineStartupError),
    Exited(ExitStatus),
    Runtime(RunnerPipelineProcessError),
}

impl PipelineRestartCause {
    pub(super) fn log_creation_failure(&self, document_id: &TenonDocumentId) {
        if matches!(self, Self::Launch(_)) {
            eprintln!(
                "{}: Pipeline {}: {}",
                self.code(),
                document_id.as_str(),
                ErrorChain(self)
            );
        }
    }

    pub(super) fn view(&self, document_etag: TenonDocumentEtag) -> PipelineAttemptError {
        let message = match self {
            Self::Launch(RunnerPipelineLaunchError::ResourceLimits(_)) => {
                "Pipeline resource limits could not be applied; check the Runner deployment and requested limits"
            }
            Self::Launch(_) | Self::Startup(_) => "Pipeline startup failed",
            Self::Exited(_) => "Pipeline process exited unexpectedly",
            Self::Runtime(_) => "Pipeline control failed",
        };
        PipelineAttemptError {
            document_etag: match self {
                Self::Launch(_) | Self::Startup(_) => Some(document_etag),
                // Published pending targets do not identify which active apply failed.
                Self::Exited(_) | Self::Runtime(_) => None,
            },
            code: self.code(),
            message,
        }
    }

    const fn code(&self) -> &'static str {
        match self {
            Self::Launch(RunnerPipelineLaunchError::ResourceLimits(_)) => {
                "resource_limits_apply_failed"
            }
            Self::Launch(RunnerPipelineLaunchError::SpawnFailed(_)) => {
                "runner.pipeline_spawn_failed"
            }
            Self::Launch(RunnerPipelineLaunchError::AttachmentTimedOut)
            | Self::Startup(RunnerPipelineStartupError::Failed(
                RunnerPipelineStartupFailure::TimedOut,
            )) => "runner.pipeline_startup_timed_out",
            Self::Launch(_) | Self::Startup(_) => "runner.pipeline_startup_failed",
            Self::Exited(_) => "runner.pipeline_exited",
            Self::Runtime(_) => "runner.pipeline_control_failed",
        }
    }
}

impl fmt::Display for PipelineRestartCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Launch(_) => formatter.write_str("Pipeline restart is required after launch"),
            Self::Startup(_) => formatter.write_str("Pipeline restart is required after startup"),
            Self::Exited(status) => write!(formatter, "Pipeline process exited with {status}"),
            Self::Runtime(_) => formatter.write_str("Pipeline control failed"),
        }
    }
}

impl Error for PipelineRestartCause {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Launch(source) => Some(source),
            Self::Startup(source) => Some(source),
            Self::Runtime(source) => Some(source),
            Self::Exited(_) => None,
        }
    }
}

/// An internal failure, or a cleanup error during Runner shutdown.
#[derive(Debug)]
pub(in crate::runner) enum RunnerPipelineLifecycleError {
    CleanupAfterFailure {
        primary: Box<Self>,
        cleanup: PipelineCleanupError,
    },
    Launch(RunnerPipelineLaunchError),
    Cleanup(PipelineCleanupError),
    CleanupAfterShutdownTimeout(PipelineCleanupError),
    CleanupAfterRestart {
        primary: Box<PipelineRestartCause>,
        cleanup: PipelineCleanupError,
    },
    StateReceiverClosed,
}

impl RunnerPipelineLifecycleError {
    pub(in crate::runner::pipeline_supervisor) const fn includes_shutdown_timeout(&self) -> bool {
        match self {
            Self::CleanupAfterFailure { primary, .. } => primary.includes_shutdown_timeout(),
            Self::CleanupAfterShutdownTimeout(_) => true,
            Self::Launch(_)
            | Self::Cleanup(_)
            | Self::CleanupAfterRestart { .. }
            | Self::StateReceiverClosed => false,
        }
    }

    pub(in crate::runner::pipeline_supervisor) const fn process_owner_recovered(&self) -> bool {
        match self {
            Self::CleanupAfterFailure { primary, cleanup } => {
                primary.process_owner_recovered() && cleanup.process_owner_recovered()
            }
            Self::Launch(_) => false,
            Self::Cleanup(cleanup)
            | Self::CleanupAfterShutdownTimeout(cleanup)
            | Self::CleanupAfterRestart { cleanup, .. } => cleanup.process_owner_recovered(),
            Self::StateReceiverClosed => true,
        }
    }
}

impl fmt::Display for RunnerPipelineLifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CleanupAfterFailure { primary, cleanup } => write!(
                formatter,
                "{}; cleanup error: {}",
                ErrorChain(primary.as_ref()),
                ErrorChain(cleanup),
            ),
            Self::Launch(_) => formatter.write_str("Pipeline process ownership error"),
            Self::Cleanup(_) => formatter.write_str("Pipeline cleanup did not complete"),
            Self::CleanupAfterShutdownTimeout(error) => write!(
                formatter,
                "Pipeline shutdown exceeded its deadline; {}",
                ErrorChain(error),
            ),
            Self::CleanupAfterRestart { primary, cleanup } => write!(
                formatter,
                "{}: {}; {}",
                primary.code(),
                ErrorChain(primary.as_ref()),
                ErrorChain(cleanup),
            ),
            Self::StateReceiverClosed => {
                formatter.write_str("Runner Pipeline state receiver closed")
            }
        }
    }
}

impl Error for RunnerPipelineLifecycleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Launch(source) => Some(source),
            Self::Cleanup(source) => Some(source),
            Self::CleanupAfterFailure { .. }
            | Self::CleanupAfterShutdownTimeout(_)
            | Self::CleanupAfterRestart { .. }
            | Self::StateReceiverClosed => None,
        }
    }
}

#[cfg(test)]
mod tests;
