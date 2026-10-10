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

//! Ordered cleanup for every long-lived Runner owner.

use super::failure::{RunnerFailureAccumulator, RunnerMainLoopError};
use super::{
    PipelineSupervisor, RunnerControlServer, RunnerMainLoop, RunnerMainLoopFailure,
    RunnerRuntimeResources, RunnerShutdownReport,
};
use crate::runner::control_server::{ControlServerShutdown, ControlSocketCleanup};
use crate::runner::diagnostics::RunnerDiagnostics;
use crate::runner::http::RunnerHttpServer;
use crate::runner::management::{RunnerManagementEvent, RunnerManagementSupervisor};
use crate::runner::pipeline_supervisor::{PipelineCleanup, PipelineEvent};
use crate::runner::runtime_resources::RunnerRuntimeCleanup;

impl RunnerMainLoop {
    pub(super) async fn finish_shutdown(
        self,
        primary_failure: Option<RunnerMainLoopError>,
    ) -> Result<RunnerShutdownReport, RunnerMainLoopFailure> {
        let Self {
            execution,
            mut management,
            mut pipelines,
            mut http_server,
            control_server,
            diagnostics,
            runtime_resources,
        } = self;

        let mut failures = RunnerFailureAccumulator::new(primary_failure);
        pipelines.close_execution_admission();
        management.close_pipeline_publication();
        if failures.is_empty() {
            http_server.begin_shutdown();
            loop {
                tokio::select! {
                    biased;
                    () = execution.wait_for_expiry() => {
                        failures.push(Some(RunnerMainLoopError::ExecutionExpired));
                        failures.push(http_server.abort().await.map(RunnerMainLoopError::HttpServer));
                        break;
                    }
                    () = http_server.wait_for_handlers() => {
                        break;
                    }
                    event = management.next_event(|scope| execution.check(scope)) => {
                        match event {
                            RunnerManagementEvent::CommitReady(_) => {
                                unreachable!("closed management publication cannot emit a commit")
                            }
                            RunnerManagementEvent::Failure(source) => {
                                failures.push(Some(RunnerMainLoopError::Management(source)));
                                failures.push(
                                    http_server.abort().await.map(RunnerMainLoopError::HttpServer),
                                );
                                break;
                            }
                        }
                    }
                    event = pipelines.next_event() => {
                        match event {
                            PipelineEvent::State { update, role } => {
                                management.record_pipeline_state(update, role);
                            }
                            PipelineEvent::Failure(source) => {
                                failures.push(Some(RunnerMainLoopError::Pipeline(source)));
                                failures.push(
                                    http_server.abort().await.map(RunnerMainLoopError::HttpServer),
                                );
                                break;
                            }
                        }
                    }
                }
            }
        } else {
            failures.push(
                http_server
                    .abort()
                    .await
                    .map(RunnerMainLoopError::HttpServer),
            );
        }

        let shutdown = shutdown_runner_owners(
            management,
            pipelines,
            Some(http_server),
            control_server,
            diagnostics,
            runtime_resources,
        )
        .await;
        failures.extend(shutdown.failures);
        match failures.finish() {
            Some(source) => Err(RunnerMainLoopFailure::new(shutdown.report, source)),
            None => Ok(shutdown.report),
        }
    }
}

pub(super) struct RunnerOwnerShutdownResult {
    pub(super) report: RunnerShutdownReport,
    pub(super) failures: Vec<RunnerMainLoopError>,
}

pub(super) async fn shutdown_runner_owners(
    mut management: RunnerManagementSupervisor,
    mut pipelines: PipelineSupervisor,
    mut http_server: Option<RunnerHttpServer>,
    control_server: RunnerControlServer,
    diagnostics: RunnerDiagnostics,
    runtime_resources: RunnerRuntimeResources,
) -> RunnerOwnerShutdownResult {
    let management_cleanup = management
        .shutdown()
        .await
        .err()
        .map(RunnerMainLoopError::Management);
    drop(management);

    let mut pipeline_cleanup = pipelines.shutdown().await;
    diagnostics.close();
    let http_cleanup = match &mut http_server {
        Some(server) => server
            .wait_for_shutdown()
            .await
            .map(RunnerMainLoopError::HttpServer),
        None => None,
    };
    let control_mode = match pipeline_cleanup.cleanup {
        PipelineCleanup::Complete => ControlServerShutdown::Graceful,
        PipelineCleanup::Incomplete => ControlServerShutdown::Abort,
    };
    let control_shutdown = control_server.shutdown(control_mode).await;
    let runtime_disposition = if matches!(pipeline_cleanup.cleanup, PipelineCleanup::Incomplete) {
        RunnerRuntimeCleanup::MarkOwnerIncomplete
    } else if matches!(
        control_shutdown.socket_cleanup,
        ControlSocketCleanup::Incomplete
    ) {
        RunnerRuntimeCleanup::RecoverLater
    } else {
        RunnerRuntimeCleanup::Remove
    };

    let mut failures = management_cleanup.into_iter().collect::<Vec<_>>();
    failures.extend(http_cleanup);
    failures.extend(
        pipeline_cleanup
            .failures
            .into_iter()
            .map(RunnerMainLoopError::Pipeline),
    );
    failures.extend(
        control_shutdown
            .failure
            .map(RunnerMainLoopError::ControlServer),
    );
    failures.extend(
        runtime_resources
            .cleanup(runtime_disposition)
            .err()
            .map(RunnerMainLoopError::RuntimeResources),
    );

    pipeline_cleanup
        .timed_out_documents
        .sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
    RunnerOwnerShutdownResult {
        report: RunnerShutdownReport::new(pipeline_cleanup.timed_out_documents.into_boxed_slice()),
        failures,
    }
}
