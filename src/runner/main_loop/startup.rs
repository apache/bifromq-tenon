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

//! Assembles every long-lived Runner owner before event dispatch begins.

use super::failure::combine_failures;
use super::shutdown::shutdown_runner_owners;
use super::{
    PipelineSupervisor, RunnerControlServer, RunnerMainLoop, RunnerMainLoopError,
    RunnerMainLoopFailure, RunnerRuntimeResources,
};
use crate::config::RunnerConfig;
use crate::metrics::MetricsRuntime;
use crate::runner::diagnostics::RunnerDiagnostics;
use crate::runner::executable::CapturedRunnerExecutable;
use crate::runner::execution::RunnerExecution;
use crate::runner::extensions::{ExecutionScope, RunnerHooks};
use crate::runner::http::tls::TlsServerConfig;
use crate::runner::http::{RunnerHttpServer, RunnerHttpServerError};
use crate::runner::management::RunnerManagementSupervisor;
use crate::runner::metrics::RunnerMetrics;
use crate::runner::process_resources;
use crate::runner::recovery::recover;
use crate::runner::runtime_resources::{
    RunnerRuntimeCleanup, remove_stale_runtime_directory, stale_runner_directories,
};
use crate::runner::state_directory::RunnerStateLayout;
use process_resources::RunnerResources;
use std::path::Path;
use std::sync::Arc;

impl RunnerMainLoop {
    /// Prepares every required owner before the event loop can be entered.
    #[allow(
        clippy::too_many_arguments,
        reason = "startup transfers the independently prepared Runner owners"
    )]
    pub(crate) async fn start(
        config: RunnerConfig,
        resources: Arc<RunnerResources>,
        state_layout: RunnerStateLayout,
        executable_image: CapturedRunnerExecutable,
        tls: Option<TlsServerConfig>,
        hooks: RunnerHooks,
        metrics: Arc<MetricsRuntime>,
    ) -> Result<Self, RunnerMainLoopFailure> {
        let meter = metrics.meter();
        let metrics = RunnerMetrics::new(metrics, config.metrics().timeout());
        let mut execution = RunnerExecution::new(hooks.execution_policy);
        execution
            .check(ExecutionScope { documents: &[] })
            .map_err(RunnerMainLoopError::ExecutionDenied)?;
        let document_verifier = config.tenon_document_verifier().map_err(|source| {
            RunnerMainLoopFailure::from(RunnerMainLoopError::ManagementInitialization(source))
        })?;

        let pipeline_runtime_directory = state_layout.pipeline_runtime_directory();
        resources.recover_stale_groups().await.map_err(|source| {
            RunnerMainLoopFailure::from(RunnerMainLoopError::ProcessResources(source))
        })?;
        recover_stale_runtime_resources(&pipeline_runtime_directory)?;
        let runtime_resources =
            RunnerRuntimeResources::prepare(&pipeline_runtime_directory, executable_image)
                .map_err(|source| {
                    RunnerMainLoopFailure::from(RunnerMainLoopError::RuntimeResources(source))
                })?;
        let prepared: Result<_, RunnerMainLoopError> = async {
            let recovered = recover(
                &config,
                &state_layout,
                &hooks.artifact_protection,
                runtime_resources.directory(),
            )
            .map_err(RunnerMainLoopError::Recovery)?;
            let recovered_management = RunnerManagementSupervisor::recover(
                state_layout.state_directory(),
                config.script_vm_limits(),
                recovered,
                hooks.artifact_protection,
                Some(&meter),
                |scope| execution.check(scope),
            )
            .map_err(RunnerMainLoopError::CpuObservation)?;
            // Process notifications received during recovery before any Pipeline starts.
            let (check, policy_changed, _) = execution.split();
            let policy_result = tokio::select! {
                biased;
                changed = policy_changed => {
                    if changed {
                        recovered_management.check_execution(check)
                    } else {
                        Ok(())
                    }
                }
                () = std::future::ready(()) => Ok(()),
            };
            policy_result.map_err(RunnerMainLoopError::ExecutionDenied)?;
            Ok(recovered_management)
        }
        .await;
        let recovered_management = match prepared {
            Ok(management) => management,
            Err(source) => {
                let cleanup = runtime_resources
                    .cleanup(RunnerRuntimeCleanup::Remove)
                    .err()
                    .map(RunnerMainLoopError::RuntimeResources)
                    .into_iter()
                    .collect();
                return Err(combine_failures(source, cleanup).into());
            }
        };
        let diagnostics = RunnerDiagnostics::new();
        let control_server = match RunnerControlServer::start(
            runtime_resources.directory(),
            resources,
            diagnostics.clone(),
            metrics.clone(),
        ) {
            Ok(server) => server,
            Err(source) => {
                let disposition = if source.requires_runtime_recovery() {
                    RunnerRuntimeCleanup::RecoverLater
                } else {
                    RunnerRuntimeCleanup::Remove
                };
                let cleanup = runtime_resources
                    .cleanup(disposition)
                    .err()
                    .map(RunnerMainLoopError::RuntimeResources)
                    .into_iter()
                    .collect();
                return Err(RunnerMainLoopFailure::from(combine_failures(
                    RunnerMainLoopError::ControlServer(source),
                    cleanup,
                )));
            }
        };
        let (management, management_interface, initial_pipeline_directives) =
            RunnerManagementSupervisor::start(recovered_management, document_verifier);
        let config = Arc::new(config);
        let pipelines = PipelineSupervisor::new(
            Arc::clone(&config),
            runtime_resources.executable(),
            runtime_resources.directory().to_path_buf(),
            control_server
                .pipeline_launcher()
                .with_metrics(Some(&meter)),
            initial_pipeline_directives,
            execution.expiry(),
        );
        let http_server = match RunnerHttpServer::start(
            config.http_listen_address(),
            tls,
            management_interface,
            diagnostics.clone(),
            metrics,
            hooks.http_authorization,
        )
        .await
        {
            Ok(server) => server,
            Err(source) => {
                return Err(cleanup_after_http_start_failure(
                    management,
                    pipelines,
                    control_server,
                    diagnostics,
                    runtime_resources,
                    source,
                )
                .await);
            }
        };
        Ok(Self {
            execution,
            management,
            pipelines,
            http_server,
            control_server,
            diagnostics,
            runtime_resources,
        })
    }
}

async fn cleanup_after_http_start_failure(
    management: RunnerManagementSupervisor,
    pipelines: PipelineSupervisor,
    control_server: RunnerControlServer,
    diagnostics: RunnerDiagnostics,
    runtime_resources: RunnerRuntimeResources,
    source: RunnerHttpServerError,
) -> RunnerMainLoopFailure {
    let shutdown = shutdown_runner_owners(
        management,
        pipelines,
        None,
        control_server,
        diagnostics,
        runtime_resources,
    )
    .await;
    RunnerMainLoopFailure::new(
        shutdown.report,
        combine_failures(RunnerMainLoopError::HttpServer(source), shutdown.failures),
    )
}

pub(super) fn recover_stale_runtime_resources(
    pipeline_runtime_directory: &Path,
) -> Result<(), RunnerMainLoopFailure> {
    let stale_directories = stale_runner_directories(pipeline_runtime_directory)
        .map_err(RunnerMainLoopError::RuntimeResources)?;
    for directory in stale_directories {
        RunnerControlServer::remove_stale_socket(&directory)
            .map_err(RunnerMainLoopError::ControlServer)?;
        remove_stale_runtime_directory(directory).map_err(RunnerMainLoopError::RuntimeResources)?;
    }
    Ok(())
}
