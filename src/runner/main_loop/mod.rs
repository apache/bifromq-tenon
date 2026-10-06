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

//! Orchestrates the long-lived Runner services and Pipeline lifecycles.
//!
//! One Tokio `current_thread` runtime owns the private UDS service, operating
//! system shutdown signals and one structured lifecycle task per ready Tenon
//! Document. Dedicated owners contain the HTTP server, control server, and
//! Pipeline task implementations. The Runner management state separately owns
//! the complete in-memory management facts and rules. This loop only orders
//! their events and shutdown dependencies.
//!
//! Each lifecycle task exclusively owns its current Pipeline process group.
//! The biased main selection makes a simultaneously-ready shutdown request the
//! linearization point before server or worker failure. Planned shutdown then
//! notifies every task before awaiting any one of them and keeps draining state
//! updates. The normal path removes the UDS and private runtime root only after
//! every process owner is recovered. If ownership remains uncertain, Runner
//! stops new control admission without waiting for open sessions, preserves the
//! root under an unmistakable blocked name, and exits with the failure chain.

mod failure;
mod shutdown;
mod startup;

use crate::runner::control_server::RunnerControlServer;
use crate::runner::diagnostics::RunnerDiagnostics;
use crate::runner::execution::RunnerExecution;
use crate::runner::http::RunnerHttpServer;
use crate::runner::management::{RunnerManagementEvent, RunnerManagementSupervisor};
use crate::runner::pipeline_supervisor::{PipelineEvent, PipelineSupervisor};
use crate::runner::runtime_resources::RunnerRuntimeResources;
use failure::RunnerMainLoopError;
pub(crate) use failure::{RunnerMainLoopFailure, RunnerShutdownReport};
use std::future::Future;
use std::io;

/// A fully started Runner whose remaining responsibility is event ordering.
pub(crate) struct RunnerMainLoop {
    execution: RunnerExecution,
    management: RunnerManagementSupervisor,
    pipelines: PipelineSupervisor,
    http_server: RunnerHttpServer,
    control_server: RunnerControlServer,
    diagnostics: RunnerDiagnostics,
    runtime_resources: RunnerRuntimeResources,
}

impl RunnerMainLoop {
    /// Runs the already-started owners until shutdown or a terminal event.
    pub(crate) async fn run_until_shutdown(
        mut self,
        shutdown: impl Future<Output = io::Result<()>>,
    ) -> Result<RunnerShutdownReport, RunnerMainLoopFailure> {
        tokio::pin!(shutdown);
        let primary_failure = loop {
            let (check, policy_changed, expiry) = self.execution.split();
            tokio::select! {
            biased;
            result = &mut shutdown => {
                break result.err().map(RunnerMainLoopError::ShutdownSignal);
            }
            () = expiry.wait_for_expiry() => {
                break Some(RunnerMainLoopError::ExecutionExpired);
            }
            changed = policy_changed => {
                if changed && let Err(source) = self.management.check_execution(&check) {
                    break Some(RunnerMainLoopError::ExecutionDenied(source));
                }
            }
            source = self.control_server.wait() => {
                break Some(RunnerMainLoopError::ControlServer(source));
            }
            source = self.http_server.wait() => {
                break Some(RunnerMainLoopError::HttpServer(source));
            }
            event = self.management.next_event(&check) => {
                match event {
                    RunnerManagementEvent::CommitReady(commit) => {
                        if let Err(source) =
                            commit.publish_after(|directives| self.pipelines.apply(directives))
                        {
                            break Some(RunnerMainLoopError::Pipeline(source));
                        }
                    }
                    RunnerManagementEvent::Failure(source) => {
                        break Some(RunnerMainLoopError::Management(source));
                    }
                }
            }
            event = self.pipelines.next_event() => {
                match event {
                    PipelineEvent::State { update, role } => {
                        self.management.record_pipeline_state(update, role);
                    }
                    PipelineEvent::Failure(source) => {
                        break Some(RunnerMainLoopError::Pipeline(source));
                    }
                }
            }
            }
        };
        self.finish_shutdown(primary_failure).await
    }
}

#[cfg(test)]
mod tests;
