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

//! Owns the Pipeline process boundary and its complete private implementation.
//!
//! The root parses the private command, creates the one current-thread Tokio
//! runtime, delegates the process lifetime to [`main_loop`], and renders terminal
//! diagnostics. Pipeline-only control, runtime, Plugin, Queue, and self-termination
//! details stay below this module. Shared Runner-Pipeline protocol types remain
//! at the crate root.

mod channel;
mod controller;
mod diagnostics;
mod ingress_queue;
mod main_loop;
mod metrics;
mod metrics_stream;
mod plugin;
mod reconfigure;
mod runtime;

#[cfg(test)]
pub(crate) mod test_support;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use rustix::process::{Signal, getpgrp, getpid, kill_process_group};
use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use crate::error::ErrorChain;

use self::main_loop::{PipelineControlLoopError, PipelineLaunch};

const CONTROL_SOCKET_FLAG: &str = "--control-socket";
const LAUNCH_ID_FLAG: &str = "--launch-id";

pub(crate) fn run_from_arguments(arguments: impl IntoIterator<Item = OsString>) -> ExitCode {
    match run(arguments) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}: {}", error.code(), ErrorChain(&error));
            error.exit_code()
        }
    }
}

fn run(arguments: impl IntoIterator<Item = OsString>) -> Result<(), PipelineProcessError> {
    let launch = parse_arguments(arguments)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(PipelineProcessError::RuntimeBuild)?;
    // A panic ends this private process; no borrowed state is resumed afterward.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime
            .block_on(main_loop::run(launch))
            .map_err(PipelineProcessError::ControlLoop)
    }));
    // Terminal failure or panic can interrupt preparation inside Lua startup.
    // Exiting must not wait for that obsolete blocking job to finish.
    runtime.shutdown_timeout(Duration::ZERO);
    match result {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

fn parse_arguments(
    arguments: impl IntoIterator<Item = OsString>,
) -> Result<PipelineLaunch, PipelineProcessError> {
    let mut arguments = arguments.into_iter();
    require_flag(arguments.next(), CONTROL_SOCKET_FLAG)?;
    let control_socket = arguments
        .next()
        .and_then(|path| path.into_string().ok())
        .filter(|path| Path::new(path).is_absolute())
        .map(String::into_boxed_str)
        .ok_or(PipelineProcessError::ArgumentsInvalid)?;
    require_flag(arguments.next(), LAUNCH_ID_FLAG)?;
    let encoded_launch_id = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or(PipelineProcessError::ArgumentsInvalid)?;
    if arguments.next().is_some() {
        return Err(PipelineProcessError::ArgumentsInvalid);
    }
    let launch_id = STANDARD
        .decode(&encoded_launch_id)
        .ok()
        .filter(|launch_id| {
            !launch_id.is_empty() && STANDARD.encode(launch_id) == encoded_launch_id
        })
        .ok_or(PipelineProcessError::ArgumentsInvalid)?;

    Ok(PipelineLaunch {
        control_socket,
        launch_id,
    })
}

fn require_flag(argument: Option<OsString>, expected: &str) -> Result<(), PipelineProcessError> {
    if argument.as_deref() == Some(expected.as_ref()) {
        Ok(())
    } else {
        Err(PipelineProcessError::ArgumentsInvalid)
    }
}

#[derive(Debug)]
enum PipelineProcessError {
    ArgumentsInvalid,
    RuntimeBuild(std::io::Error),
    ControlLoop(PipelineControlLoopError),
}

impl PipelineProcessError {
    fn code(&self) -> &'static str {
        match self {
            Self::ArgumentsInvalid => "pipeline.arguments_invalid",
            Self::RuntimeBuild(_) => "pipeline.runtime_build_failed",
            Self::ControlLoop(error) => error.code(),
        }
    }

    fn exit_code(&self) -> ExitCode {
        match self {
            Self::ArgumentsInvalid => ExitCode::from(2),
            _ => ExitCode::FAILURE,
        }
    }
}

impl fmt::Display for PipelineProcessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArgumentsInvalid => formatter.write_str(
                "usage: tenon pipeline --control-socket <absolute-path> \
                 --launch-id <canonical-padded-base64>",
            ),
            Self::RuntimeBuild(_) => formatter.write_str("Pipeline runtime could not be created"),
            Self::ControlLoop(_) => formatter.write_str("Pipeline control loop failed"),
        }
    }
}

impl Error for PipelineProcessError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RuntimeBuild(error) => Some(error),
            Self::ControlLoop(error) => Some(error),
            Self::ArgumentsInvalid => None,
        }
    }
}

/// Terminates this Pipeline group without dropping any live resource owner.
fn terminate_pipeline() -> ! {
    let process_id = getpid();
    if getpgrp() != process_id || kill_process_group(process_id, Signal::KILL).is_err() {
        std::process::abort();
    }
    // macOS can return before delivering SIGKILL. Keep all owners alive until then.
    loop {
        std::thread::park();
    }
}

/// Narrow source for the repository's explicit public test facade.
#[cfg(any(test, feature = "repository-test-support"))]
pub(crate) mod contract_test_support {
    /// Uses production path derivation without making that implementation public.
    pub fn egress_queue_path(
        instance_directory: &std::path::Path,
        flow_id: &str,
        channel_id: u32,
    ) -> std::path::PathBuf {
        super::reconfigure::contract_test_support::egress_queue_path(
            instance_directory,
            flow_id,
            channel_id,
        )
    }

    /// Returns the Bell Region that holds one Flow's Channel doorbells.
    pub fn flow_channel_bell_path(
        pipeline_root: &std::path::Path,
        flow_id: &str,
    ) -> std::path::PathBuf {
        super::reconfigure::contract_test_support::flow_channel_bell_path(pipeline_root, flow_id)
    }

    /// Returns the Bell Region that holds one plugin side's own loop doorbells.
    pub fn loops_bell_path(side_directory: &std::path::Path) -> std::path::PathBuf {
        super::reconfigure::contract_test_support::loops_bell_path(side_directory)
    }

    /// Returns the directory name holding one plugin Instance's Source side.
    pub const fn source_directory_name() -> &'static str {
        super::reconfigure::contract_test_support::SOURCE_DIRECTORY_NAME
    }

    /// Returns the directory name holding one plugin Instance's Sink side.
    pub const fn sink_directory_name() -> &'static str {
        super::reconfigure::contract_test_support::SINK_DIRECTORY_NAME
    }
    pub(crate) mod ingress_queue {
        pub use super::super::ingress_queue::contract_test_support::{
            max_pending_records, validate_pair,
        };
        pub use super::super::ingress_queue::{
            COMPLETION_MAX_PAYLOAD_SIZE, completion_capacity, submission_capacity,
        };
    }

    #[cfg(feature = "repository-test-support")]
    pub(crate) mod plugin_program {
        pub use super::super::plugin::contract_test_support::{
            PluginProgramLaunch, force_stop_sink_program, run_sink_program,
            run_sink_program_with_control_stream_loss, run_sink_program_with_owner_loss,
            run_source_and_sink_program, run_source_program,
        };
    }
}

#[cfg(all(test, not(feature = "loom-model")))]
#[path = "../../sdk/rust/ipc/tests/support/wait.rs"]
mod queue_test_support;

#[cfg(test)]
mod tests {
    use super::{PipelineProcessError, parse_arguments};
    use crate::error::ErrorChain;
    use std::ffi::OsString;
    use std::io;

    #[test]
    fn pipeline_arguments_require_exact_named_values() {
        let valid = [
            "--control-socket",
            "/tmp/control.sock",
            "--launch-id",
            "bGF1bmNoLWE=",
        ];
        assert!(parse_arguments(valid.map(OsString::from)).is_ok());

        for invalid in [
            valid[..3].to_vec(),
            vec![
                "--control-socket",
                "relative.sock",
                "--launch-id",
                "bGF1bmNoLWE=",
            ],
            vec![
                "--control-socket",
                "/tmp/control.sock",
                "--launch-id",
                "bGF1bmNoLWE",
            ],
            vec!["--control-socket", "/tmp/control.sock", "--launch-id", ""],
            vec![
                "--control-socket",
                "/tmp/control.sock",
                "--launch-id",
                "bGF1bmNoLWE=",
                "unexpected",
            ],
        ] {
            assert!(matches!(
                parse_arguments(invalid.into_iter().map(OsString::from)),
                Err(PipelineProcessError::ArgumentsInvalid)
            ));
        }
    }

    #[test]
    fn pipeline_process_diagnostic_includes_the_complete_error_chain() {
        let error = PipelineProcessError::RuntimeBuild(io::Error::other("runtime cause"));

        assert_eq!(
            format!("{}: {}", error.code(), ErrorChain(&error)),
            "pipeline.runtime_build_failed: Pipeline runtime could not be created: runtime cause"
        );
    }
}
