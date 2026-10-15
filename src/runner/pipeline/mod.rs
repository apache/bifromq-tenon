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

//! Runner-side ownership and transport interface for one Pipeline process.
//!
//! Management resolves runnable targets and consumes applied states through this
//! module. The supervisor starts one process owner for each attempt and retains
//! its own latest target, working directory, and restart policy. The process
//! owner keeps its connection, published revisions, and exact control directory
//! together until explicit cleanup. Control and diagnostics adapters share the
//! launch registry; the control server only assembles their transports.

mod control;
mod diagnostics;
mod directory;
mod environment;
mod launch_registry;
mod launcher;
mod programs;
mod published_revisions;
mod runtime_resolution;
mod target;

pub(super) use control::{RunnerPipelineControl, RunnerPipelineControlSessionError};
pub(super) use diagnostics::RunnerPipelineDiagnostics;
pub(super) use directory::{PipelineCleanupError, cleanup_pipeline_directory};
pub(super) use launch_registry::{
    RunnerPipelineLaunchRegistry, RunnerPipelineLaunchRegistryCreateError,
};
pub(super) use launcher::{
    PipelineLaunchResources, PipelineTargetPublication, RunnerPipelineLaunchError,
    RunnerPipelineLauncher, RunnerPipelineProcess, RunnerPipelineProcessError,
    RunnerPipelineProcessEvent, RunnerPipelineStartupError, RunnerPipelineStartupFailure,
};
pub(super) use runtime_resolution::{RuntimeResolution, RuntimeResolutionIssue, RuntimeResolver};
pub(super) use target::{PipelineLifecycleTarget, PipelineRunningState};

#[cfg(test)]
pub(super) mod test_support;
#[cfg(test)]
mod tests;
