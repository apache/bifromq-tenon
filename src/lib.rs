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

//! Tenon's executable library boundary.
//!
//! The default binary enters through [`run_main`]. Trusted distributions use
//! [`run_main_with`] to install HTTP authorization, execution, and artifact protection hooks. Runtime
//! owners stay private; hooks receive immutable configuration and verified facts.

use std::env;
use std::error::Error;
use std::process::ExitCode;

pub use config::{HttpTlsConfig, RunnerConfig, ScriptVmLimits, ScriptVmLimitsError};
pub use identifiers::{
    ExactVersion, FlowId, PluginInstanceId, PluginProgramIdentity, ProgramName, TenonDocumentId,
};
pub use runner::extensions::{
    AllowAll, ArtifactProtection, ByPass, ExecutionDenied, ExecutionPermit, ExecutionPolicy,
    ExecutionScope, HttpApiAuthorization, HttpAuthRejection, NoHttpAuth, PluginPackageOutput,
    PolicyChanges, RunnerHooks,
};
pub use tenon_document::verified::{Flow, PluginInstance};
pub use tenon_document::{ResourceLimits, VerifiedTenonDocument};

mod config;

mod contracts;

mod identifiers;

mod lua;

mod metrics;

mod payload_contract;

mod pipeline;

mod time;

mod error;

mod tenon_document;

mod runner;

mod strict_jsonc;

const SUBCOMMAND: &str = "pipeline";
const PLUGIN_CONTROL_SOCKET_NAME: &str = "control.sock";

/// Selects the Runner or Pipeline process subcommand from the exact command line.
///
/// Startup failures are rendered to stderr and represented by a non-zero process
/// exit code. Each process boundary retains its structured error internally.
#[must_use]
pub fn run_main() -> ExitCode {
    run_main_with(|_| Ok(RunnerHooks::default()))
}

/// Runs this distribution with exactly one Runner-only initialization callback.
///
/// The callback receives the fully validated, frozen configuration before Store
/// recovery and returns the three hooks. It is never called in a Pipeline subprocess.
/// Initialization failures produce a non-zero exit code without opening the API.
#[must_use]
pub fn run_main_with(
    initialize: impl FnOnce(&RunnerConfig) -> Result<RunnerHooks, Box<dyn Error>>,
) -> ExitCode {
    let mut arguments = env::args_os().skip(1).peekable();
    if arguments
        .peek()
        .is_some_and(|argument| argument == SUBCOMMAND)
    {
        let _ = arguments.next();
        pipeline::run_from_arguments(arguments)
    } else {
        runner::run_from_arguments(arguments, initialize)
    }
}

/// Repository integration-test access to Tenon's internal contract adapters.
///
/// This module is test infrastructure, not a supported Rust SDK.
#[cfg(any(test, feature = "repository-test-support"))]
pub mod runner_test_support;

// Lets the shared black-box Queue helper compile when an in-crate test reuses it.
#[cfg(test)]
extern crate self as tenon;

/// Derives one launch's short Plugin endpoint directory from its existing identity.
#[allow(
    clippy::expect_used,
    reason = "Runner control sockets are absolute file paths"
)]
pub(crate) fn plugin_control_directory(
    runner_control_socket: &std::path::Path,
    launch_id: &[u8],
) -> std::path::PathBuf {
    use base64::Engine as _;
    runner_control_socket
        .parent()
        .expect("a Runner control socket must have a parent directory")
        .join(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(launch_id))
}
