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

//! Stable failures and shutdown observations owned by the Runner main loop.

use crate::error::ErrorChain;
use crate::identifiers::TenonDocumentId;
use crate::runner::control_server::RunnerControlServerError;
use crate::runner::extensions::ExecutionDenied;
use crate::runner::http::RunnerHttpServerError;
use crate::runner::management::RunnerManagementSupervisorError;
use crate::runner::pipeline_supervisor::PipelineSupervisorError;
use crate::runner::recovery::RunnerRecoveryError;
use crate::runner::runtime_resources::RunnerRuntimeResourcesError;
use crate::tenon_document::TenonDocumentVerifierInitializationError;
use std::error::Error;
use std::{fmt, io};

pub(super) struct RunnerFailureAccumulator {
    failures: Vec<RunnerMainLoopError>,
}

impl RunnerFailureAccumulator {
    pub(super) fn new(primary: Option<RunnerMainLoopError>) -> Self {
        Self {
            failures: primary.into_iter().collect(),
        }
    }

    pub(super) fn push(&mut self, failure: Option<RunnerMainLoopError>) {
        self.failures.extend(failure);
    }

    pub(super) fn extend(&mut self, failures: impl IntoIterator<Item = RunnerMainLoopError>) {
        self.failures.extend(failures);
    }

    pub(super) fn is_empty(&self) -> bool {
        self.failures.is_empty()
    }

    pub(super) fn finish(self) -> Option<RunnerMainLoopError> {
        let mut failures = self.failures.into_iter();
        let primary = failures.next()?;
        Some(combine_failures(primary, failures.collect()))
    }
}

pub(super) fn combine_failures(
    primary: RunnerMainLoopError,
    additional: Vec<RunnerMainLoopError>,
) -> RunnerMainLoopError {
    if additional.is_empty() {
        primary
    } else {
        RunnerMainLoopError::AdditionalFailures {
            primary: Box::new(primary),
            additional: additional.into_boxed_slice(),
        }
    }
}

/// The non-fatal observations produced by one planned Runner shutdown.
#[derive(Debug, Default)]
pub(crate) struct RunnerShutdownReport {
    timed_out_documents: Box<[TenonDocumentId]>,
}

impl RunnerShutdownReport {
    pub(super) fn new(timed_out_documents: Box<[TenonDocumentId]>) -> Self {
        Self {
            timed_out_documents,
        }
    }

    /// Returns every Pipeline that required a forced kill after its deadline.
    #[must_use]
    pub(crate) const fn timed_out_documents(&self) -> &[TenonDocumentId] {
        &self.timed_out_documents
    }
}

/// A fatal Runner main-loop failure together with non-fatal shutdown facts.
#[derive(Debug)]
pub(crate) struct RunnerMainLoopFailure {
    report: RunnerShutdownReport,
    source: Box<RunnerMainLoopError>,
}

impl RunnerMainLoopFailure {
    pub(super) fn new(report: RunnerShutdownReport, source: RunnerMainLoopError) -> Self {
        Self {
            report,
            source: Box::new(source),
        }
    }

    /// Returns the stable category of the primary failure.
    #[must_use]
    pub(crate) const fn code(&self) -> &'static str {
        self.source.code()
    }

    /// Returns shutdown observations that remain valid despite the failure.
    #[must_use]
    pub(crate) const fn shutdown_report(&self) -> &RunnerShutdownReport {
        &self.report
    }
}

impl From<RunnerMainLoopError> for RunnerMainLoopFailure {
    fn from(source: RunnerMainLoopError) -> Self {
        Self {
            report: RunnerShutdownReport::default(),
            source: Box::new(source),
        }
    }
}

impl fmt::Display for RunnerMainLoopFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Runner main loop failed")
    }
}

impl Error for RunnerMainLoopFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// A stable terminal failure of the Runner control loop or its cleanup chain.
#[derive(Debug)]
pub(super) enum RunnerMainLoopError {
    CpuObservation(io::Error),
    ProcessResources(io::Error),
    ExecutionExpired,
    Recovery(RunnerRecoveryError),
    ExecutionDenied(ExecutionDenied),
    RuntimeResources(RunnerRuntimeResourcesError),
    ShutdownSignal(io::Error),
    ControlServer(RunnerControlServerError),
    HttpServer(RunnerHttpServerError),
    ManagementInitialization(TenonDocumentVerifierInitializationError),
    Management(RunnerManagementSupervisorError),
    Pipeline(PipelineSupervisorError),
    AdditionalFailures {
        primary: Box<Self>,
        additional: Box<[Self]>,
    },
}

impl RunnerMainLoopError {
    #[must_use]
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::CpuObservation(_) => "runner.cpu_observation_failed",
            Self::ExecutionExpired => "runner.execution_permit_expired",
            Self::Recovery(source) => source.code(),
            Self::ExecutionDenied(source) => source.code(),
            Self::ProcessResources(_) => "runner.process_resource_recovery_failed",
            Self::RuntimeResources(source) => source.code(),
            Self::ShutdownSignal(_) => "runner.shutdown_signal_failed",
            Self::ControlServer(_) => "runner.pipeline_control_failed",
            Self::HttpServer(_) => "runner.http_server_failed",
            Self::Management(RunnerManagementSupervisorError::ExecutionDenied(source)) => {
                source.code()
            }
            Self::ManagementInitialization(_) | Self::Management(_) => "runner.management_failed",
            Self::Pipeline(_) => "runner.pipeline_lifecycle_failed",
            Self::AdditionalFailures { primary, .. } => primary.code(),
        }
    }
}

impl fmt::Display for RunnerMainLoopError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CpuObservation(_) => {
                formatter.write_str("Runner could not determine allowed logical CPUs")
            }
            Self::ProcessResources(_) => {
                formatter.write_str("Runner could not recover stale process resource groups")
            }
            Self::ExecutionExpired => {
                formatter.write_str("Runner execution entitlement has expired")
            }
            Self::Recovery(_) => formatter.write_str("Runner startup recovery failed"),
            Self::ExecutionDenied(_) => formatter.write_str("Runner execution is not permitted"),
            Self::RuntimeResources(source) => write!(formatter, "{source}"),
            Self::ShutdownSignal(_) => formatter.write_str("Runner shutdown signal stream failed"),
            Self::ControlServer(_) => {
                formatter.write_str("Runner Pipeline control service stopped unexpectedly")
            }
            Self::HttpServer(_) => formatter.write_str("Runner HTTP service stopped unexpectedly"),
            Self::ManagementInitialization(_) => {
                formatter.write_str("Runner management interface initialization failed")
            }
            Self::Management(source) => write!(formatter, "{source}"),
            Self::Pipeline(source) => write!(formatter, "{source}"),
            Self::AdditionalFailures {
                primary,
                additional,
            } => {
                write!(formatter, "{}", ErrorChain(primary.as_ref()))?;
                for cleanup in additional {
                    write!(formatter, "; cleanup also failed: {}", ErrorChain(cleanup))?;
                }
                Ok(())
            }
        }
    }
}

impl Error for RunnerMainLoopError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CpuObservation(source) => Some(source),
            Self::ProcessResources(source) => Some(source),
            Self::ExecutionExpired => None,
            Self::Recovery(source) => Some(source),
            Self::ExecutionDenied(source) => Some(source),
            Self::RuntimeResources(source) => Some(source),
            Self::ShutdownSignal(source) => Some(source),
            Self::ControlServer(source) => Some(source),
            Self::HttpServer(source) => Some(source),
            Self::ManagementInitialization(source) => Some(source),
            Self::Management(source) => Some(source),
            Self::Pipeline(source) => Some(source),
            Self::AdditionalFailures { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn cleanup_failures_are_flat_and_preserve_the_primary_failure_code() -> Result<(), &'static str>
    {
        let mut failures =
            RunnerFailureAccumulator::new(Some(RunnerMainLoopError::RuntimeResources(
                RunnerRuntimeResourcesError::Invalid(PathBuf::from("invalid-runtime")),
            )));
        failures.extend(
            (0..10_000).map(|_| RunnerMainLoopError::Pipeline(PipelineSupervisorError::Stopped)),
        );

        let Some(error) = failures.finish() else {
            return Err("failure disappeared during cleanup");
        };
        assert_eq!(error.code(), "runner.runtime_directory_invalid");
        let RunnerMainLoopError::AdditionalFailures { additional, .. } = error else {
            return Err("cleanup failures were not represented by one flat owner");
        };
        assert_eq!(additional.len(), 10_000);
        Ok(())
    }
}
