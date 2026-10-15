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

use super::*;
use std::io;

#[test]
fn incomplete_process_cleanup_preserves_runtime_files() {
    for cleanup in [
        PipelineCleanupError::Process(io::Error::other("process wait")),
        PipelineCleanupError::ResourceGroup(io::Error::other("resource group")),
    ] {
        let error = RunnerPipelineLifecycleError::CleanupAfterShutdownTimeout(cleanup);
        assert!(!error.process_owner_recovered());
        assert!(error.includes_shutdown_timeout());
    }
}

#[test]
fn directory_failure_keeps_the_restart_reason_without_claiming_a_live_process() {
    let error = RunnerPipelineLifecycleError::CleanupAfterRestart {
        primary: Box::new(PipelineRestartCause::Runtime(
            RunnerPipelineProcessError::ProcessTreeWait(io::Error::other("runtime wait")),
        )),
        cleanup: PipelineCleanupError::Filesystem {
            path: "/runtime/pipeline".into(),
            source: io::Error::other("directory cleanup"),
        },
    };
    assert!(error.process_owner_recovered());
    assert!(!error.includes_shutdown_timeout());
    assert_eq!(
        ErrorChain(&error).to_string(),
        "runner.pipeline_control_failed: Pipeline control failed: Pipeline process tree wait failed: runtime wait; Pipeline directory cleanup failed: /runtime/pipeline: directory cleanup"
    );
}

#[test]
fn internal_failure_retains_a_failed_cleanup_and_the_timeout() {
    let error = RunnerPipelineLifecycleError::CleanupAfterFailure {
        primary: Box::new(RunnerPipelineLifecycleError::CleanupAfterShutdownTimeout(
            PipelineCleanupError::Filesystem {
                path: "/runtime/pipeline".into(),
                source: io::Error::other("directory cleanup"),
            },
        )),
        cleanup: PipelineCleanupError::Process(io::Error::other("process wait")),
    };
    assert!(!error.process_owner_recovered());
    assert!(error.includes_shutdown_timeout());
    assert!(
        ErrorChain(&error)
            .to_string()
            .ends_with("cleanup error: Pipeline process cleanup error: process wait")
    );
}
