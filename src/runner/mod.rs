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

//! Process startup boundary for the long-lived `tenon` Runner.

mod control_server;
mod diagnostics;
mod document_store;
mod executable;
mod execution;
pub(crate) mod extensions;
mod http;
mod main_loop;
mod management;
mod metrics;
mod pipeline;
mod pipeline_supervisor;
mod plugin;
mod private_filesystem;
mod process_resources;
mod process_tree;
mod recovery;
mod runtime_resources;
mod state_directory;

#[cfg(test)]
pub(crate) mod test_support;

use crate::config::{RunnerConfig, RunnerConfigError};
use crate::error::ErrorChain;
use crate::metrics::{CoreProcess, MetricsRuntime};
use executable::CapturedRunnerExecutable;
use extensions::RunnerHooks;
use main_loop::{RunnerMainLoop, RunnerMainLoopFailure, RunnerShutdownReport};
use state_directory::{RunnerStateDirectoryError, prepare_runner_state_directory};
use std::error::Error;
use std::ffi::OsString;
use std::path::PathBuf;
use std::process::ExitCode;
use std::{fmt, io, sync};
use sync::Arc;
use tokio::runtime;
use tokio::signal::unix::{Signal, SignalKind, signal};

/// Parses startup, restores private state, and runs until an operating-system
/// shutdown request or a fatal Runner invariant failure.
#[must_use]
pub(crate) fn run_from_arguments(
    arguments: impl IntoIterator<Item = OsString>,
    initialize: impl FnOnce(&RunnerConfig) -> Result<RunnerHooks, Box<dyn Error>>,
) -> ExitCode {
    match run(arguments, initialize) {
        Ok(report) => {
            write_shutdown_diagnostics(&report);
            ExitCode::SUCCESS
        }
        Err(error) => {
            if let Some(report) = error.shutdown_report() {
                write_shutdown_diagnostics(report);
            }
            eprintln!("{}: {}", error.code(), ErrorChain(&error));
            error.exit_code()
        }
    }
}

fn write_shutdown_diagnostics(report: &RunnerShutdownReport) {
    for document_id in report.timed_out_documents() {
        eprintln!(
            "runner.pipeline_shutdown_timed_out: Pipeline shutdown exceeded its deadline: {document_id}"
        );
    }
}

fn run(
    arguments: impl IntoIterator<Item = OsString>,
    initialize: impl FnOnce(&RunnerConfig) -> Result<RunnerHooks, Box<dyn Error>>,
) -> Result<RunnerShutdownReport, RunnerStartupError> {
    let config = load_from_arguments(arguments)?;
    let resources = Arc::new(
        process_resources::RunnerResources::initialize()
            .map_err(RunnerStartupError::ProcessResources)?,
    );
    let hooks = initialize(&config).map_err(RunnerStartupError::ExtensionInitialization)?;
    let tls = config
        .http_tls()
        .map(http::tls::load)
        .transpose()
        .map_err(RunnerStartupError::Tls)?;
    let executable_image =
        CapturedRunnerExecutable::capture_current().map_err(RunnerStartupError::ExecutableImage)?;
    let state_layout = prepare_runner_state_directory(config.state_directory())
        .map_err(RunnerStartupError::StateDirectory)?;
    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(RunnerStartupError::RuntimeInitialization)?;
    runtime.block_on(async move {
        let shutdown = RunnerShutdownSignals::open()
            .map_err(RunnerStartupError::ShutdownSignalRegistration)?;
        let metrics = Arc::new(
            MetricsRuntime::start(config.metrics().node_id(), CoreProcess::Runner)
                .map_err(RunnerStartupError::MetricsIdentity)?,
        );
        let result = match RunnerMainLoop::start(
            config,
            resources,
            state_layout,
            executable_image,
            tls,
            hooks,
            Arc::clone(&metrics),
        )
        .await
        {
            Ok(runner) => runner.run_until_shutdown(shutdown.wait()).await,
            Err(source) => Err(source),
        };
        metrics.shutdown();
        result.map_err(|source| RunnerStartupError::MainLoop(Box::new(source)))
    })
}

fn load_from_arguments(
    arguments: impl IntoIterator<Item = OsString>,
) -> Result<RunnerConfig, RunnerStartupError> {
    let mut arguments = arguments.into_iter();
    let Some(flag) = arguments.next() else {
        return Err(RunnerStartupError::ArgumentsInvalid);
    };
    if flag != "--config" {
        return Err(RunnerStartupError::ArgumentsInvalid);
    }
    let Some(path) = arguments.next() else {
        return Err(RunnerStartupError::ArgumentsInvalid);
    };
    if arguments.next().is_some() {
        return Err(RunnerStartupError::ArgumentsInvalid);
    }

    RunnerConfig::load(&PathBuf::from(path)).map_err(RunnerStartupError::Config)
}

#[derive(Debug)]
enum RunnerStartupError {
    ArgumentsInvalid,
    Config(RunnerConfigError),
    ExtensionInitialization(Box<dyn Error>),
    Tls(http::tls::TlsIdentityError),
    ProcessResources(io::Error),
    MetricsIdentity(getrandom::Error),
    StateDirectory(RunnerStateDirectoryError),
    ExecutableImage(io::Error),
    RuntimeInitialization(io::Error),
    ShutdownSignalRegistration(io::Error),
    MainLoop(Box<RunnerMainLoopFailure>),
}

impl RunnerStartupError {
    const fn code(&self) -> &'static str {
        match self {
            Self::ArgumentsInvalid => "runner.arguments_invalid",
            Self::Config(error) => error.code(),
            Self::ExtensionInitialization(_) => "runner.extension_initialization_failed",
            Self::Tls(_) => "runner.tls_identity_invalid",
            Self::ProcessResources(_) => "runner.process_resources_initialization_failed",
            Self::MetricsIdentity(_) => "runner.metrics_identity_unavailable",
            Self::StateDirectory(error) => error.code(),
            Self::ExecutableImage(_) => "runner.executable_image_unavailable",
            Self::RuntimeInitialization(_) => "runner.runtime_initialization_failed",
            Self::ShutdownSignalRegistration(_) => "runner.shutdown_signal_failed",
            Self::MainLoop(error) => error.code(),
        }
    }

    fn exit_code(&self) -> ExitCode {
        match self {
            Self::ArgumentsInvalid => ExitCode::from(2),
            Self::Config(_)
            | Self::ExtensionInitialization(_)
            | Self::MetricsIdentity(_)
            | Self::ProcessResources(_)
            | Self::Tls(_)
            | Self::StateDirectory(_)
            | Self::ExecutableImage(_)
            | Self::RuntimeInitialization(_)
            | Self::ShutdownSignalRegistration(_)
            | Self::MainLoop(_) => ExitCode::FAILURE,
        }
    }

    const fn shutdown_report(&self) -> Option<&RunnerShutdownReport> {
        match self {
            Self::MainLoop(error) => Some(error.shutdown_report()),
            Self::ArgumentsInvalid
            | Self::Config(_)
            | Self::ExtensionInitialization(_)
            | Self::MetricsIdentity(_)
            | Self::ProcessResources(_)
            | Self::Tls(_)
            | Self::StateDirectory(_)
            | Self::ExecutableImage(_)
            | Self::RuntimeInitialization(_)
            | Self::ShutdownSignalRegistration(_) => None,
        }
    }
}

impl fmt::Display for RunnerStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArgumentsInvalid => formatter.write_str("usage: tenon --config <absolute-path>"),
            Self::Config(_) => formatter.write_str("Runner configuration is invalid"),
            Self::ExtensionInitialization(_) => {
                formatter.write_str("Runner extensions could not be initialized")
            }
            Self::MetricsIdentity(_) => {
                formatter.write_str("Runner metrics identity could not be created")
            }
            Self::ProcessResources(_) => {
                formatter.write_str("Runner could not initialize process resources")
            }
            Self::Tls(_) => formatter.write_str("Runner HTTPS identity could not be initialized"),
            Self::StateDirectory(_) => formatter.write_str("Runner state directory setup failed"),
            Self::ExecutableImage(_) => {
                formatter.write_str("Runner executable image could not be captured")
            }
            Self::RuntimeInitialization(_) => {
                formatter.write_str("Runner asynchronous runtime could not be initialized")
            }
            Self::ShutdownSignalRegistration(_) => {
                formatter.write_str("Runner shutdown signals could not be registered")
            }
            Self::MainLoop(_) => formatter.write_str("Runner execution failed"),
        }
    }
}

impl Error for RunnerStartupError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ArgumentsInvalid => None,
            Self::Config(error) => Some(error),
            Self::ExtensionInitialization(error) => Some(error.as_ref()),
            Self::ProcessResources(error) => Some(error),
            Self::Tls(error) => Some(error),
            Self::MetricsIdentity(error) => Some(error),
            Self::StateDirectory(error) => Some(error),
            Self::ExecutableImage(error)
            | Self::RuntimeInitialization(error)
            | Self::ShutdownSignalRegistration(error) => Some(error),
            Self::MainLoop(error) => Some(error),
        }
    }
}

struct RunnerShutdownSignals {
    interrupt: Signal,
    terminate: Signal,
}

impl RunnerShutdownSignals {
    fn open() -> io::Result<Self> {
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    async fn wait(mut self) -> io::Result<()> {
        let received = tokio::select! {
            biased;
            received = self.terminate.recv() => received,
            received = self.interrupt.recv() => received,
        };
        received
            .map(|_| ())
            .ok_or_else(|| io::Error::other("Runner shutdown signal stream closed"))
    }
}

#[cfg(test)]
mod tests {
    use super::{RunnerStartupError, load_from_arguments};
    use std::ffi::OsString;

    #[test]
    fn startup_requires_exactly_one_config_argument() {
        for arguments in [
            vec![],
            vec!["--config"],
            vec!["--unknown", "/tmp/config.jsonc"],
            vec!["--config", "/tmp/one.jsonc", "--config", "/tmp/two.jsonc"],
        ] {
            let result = load_from_arguments(arguments.into_iter().map(OsString::from));
            assert!(matches!(result, Err(RunnerStartupError::ArgumentsInvalid)));
        }
    }
}
