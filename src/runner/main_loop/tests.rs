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

use super::startup::recover_stale_runtime_resources;
use super::{RunnerControlServer, RunnerMainLoop};
use crate::config::RunnerConfig;
use crate::contracts::core::{PipelineStatusSnapshot, PluginInstanceState, PluginInstanceStatus};
use crate::identifiers::TenonDocumentId;
use crate::payload_contract::PluginInterface;
use crate::runner::control_server::test_support as control_server_test_support;
use crate::runner::diagnostics::RunnerDiagnostics;
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::executable::CapturedRunnerExecutable;
use crate::runner::execution::RunnerExecution;
use crate::runner::extensions::{AllowAll, RunnerHooks};
use crate::runner::http::test_support::server_with_admitted_handler;
use crate::runner::management::test_support::{
    RunnerManagementState, empty_state, insert_document, pipeline as management_pipeline,
    supervisor as management_supervisor,
};
use crate::runner::management::{
    PipelineConvergence, PipelineLifecycleRole, PipelineLifecycleState, PipelineStateUpdate,
};
use crate::runner::pipeline::test_support::applied_state;
use crate::runner::pipeline_supervisor::test_support as pipeline_supervisor_test_support;
use crate::runner::recovery::recover;
use crate::runner::runtime_resources::{RunnerRuntimeCleanup, RunnerRuntimeResources};
use crate::runner::state_directory::prepare_runner_state_directory;
use crate::runner::test_support::{
    captured_runner_executable, document, install_plugin, load_config, target, write_document,
};
use crate::tenon_document::{UnverifiedTenonDocument, VerifiedTenonDocument};
use std::fs::{self, File};
use std::os::unix::fs::PermissionsExt as _;
use std::sync::Arc;
use std::time::Duration;
use std::{io, mem};
use tokio::sync::{Notify, oneshot};
use tokio::task;
use tokio::time;

#[test]
fn delayed_pipeline_status_keeps_the_exact_applied_document_when_desired_is_unready()
-> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let config = load_config(directory.path())?;
    let document_id = TenonDocumentId::try_from("crossed-status").map_err(io::Error::other)?;
    install_plugin(directory.path(), PluginInterface::Source)?;
    install_plugin(directory.path(), PluginInterface::Sink)?;
    let applied_target = target(
        directory.path(),
        &config,
        "crossed-status",
        "function main(event) emit() end",
    )?;
    let (desired_source, desired_document) =
        verified_document(&config, "crossed-status", "com.example.serial")?;
    let applied_etag = applied_target.document_etag();
    let desired_etag = TenonDocumentEtag::for_source(&desired_source);
    let mut management = empty_state(config.state_directory(), config.script_vm_limits());
    insert_document(
        &mut management,
        desired_source,
        desired_document,
        PipelineLifecycleState::Starting,
    );
    let applied_state = applied_state(
        &applied_target,
        PipelineStatusSnapshot {
            document_etag: applied_etag.strong_value(),
            plugin_instances: ["source", "primary"]
                .into_iter()
                .map(|id| PluginInstanceStatus {
                    id: id.to_owned(),
                    state: PluginInstanceState::Running as i32,
                    last_error: None,
                })
                .collect(),
        },
    );
    management.record_pipeline_state(
        PipelineStateUpdate {
            document_id: document_id.clone(),
            state: PipelineLifecycleState::Running(applied_state),
        },
        PipelineLifecycleRole::Current,
    );

    let view = management_pipeline(&management, &document_id)
        .ok_or_else(|| io::Error::other("Pipeline view disappeared"))?;
    assert_eq!(view.status.document_etag, desired_etag);
    let PipelineConvergence::Unready {
        applied: Some(applied),
        ..
    } = view.status.convergence
    else {
        return Err(io::Error::other(
            "Unready desired state did not retain the delayed applied status",
        ));
    };
    assert_eq!(applied, applied_etag);
    assert!(
        view.plugin_instances
            .iter()
            .any(|instance| instance.id.as_str() == "source"
                && instance.program_name.as_str() == "com.example.source")
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn admitted_http_handler_drain_keeps_advancing_pipeline_state() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;
    let config = load_config(state_directory.path())?;
    let document_id = TenonDocumentId::try_from("drain-state").map_err(io::Error::other)?;
    install_plugin(state_directory.path(), PluginInterface::Source)?;
    install_plugin(state_directory.path(), PluginInterface::Sink)?;
    write_document(
        state_directory.path(),
        document_id.as_str(),
        document("drain-state"),
    )?;
    let verifier = config.tenon_document_verifier().map_err(io::Error::other)?;
    let recovered = recover(
        &config,
        &state_layout,
        &RunnerHooks::default().artifact_protection,
        &state_layout.pipeline_runtime_directory(),
    )
    .map_err(io::Error::other)?;
    let (state, initial_directives) = RunnerManagementState::recover(
        config.state_directory(),
        config.script_vm_limits(),
        recovered,
        RunnerHooks::default().artifact_protection,
        None,
        |_| Ok(()),
    )?;
    assert_eq!(initial_directives.len(), 1);
    let (management, management_client) = management_supervisor(state, verifier);

    let executable = state_directory.path().join("runner-image");
    fs::write(&executable, b"runner")?;
    let runtime_resources = RunnerRuntimeResources::prepare(
        &state_layout.pipeline_runtime_directory(),
        CapturedRunnerExecutable::from_file(File::open(&executable)?)?,
    )
    .map_err(io::Error::other)?;
    let diagnostics = RunnerDiagnostics::new();
    let control_server = RunnerControlServer::start(
        runtime_resources.directory(),
        crate::runner::process_resources::test_support::unavailable(),
        diagnostics.clone(),
        crate::runner::metrics::test_support::empty()?,
    )
    .map_err(io::Error::other)?;

    let drain_started = Arc::new(Notify::new());
    let (http_shutdown, http_shutdown_requested) = oneshot::channel();
    let (release_http, http_released) = oneshot::channel();
    let http_drain_started = Arc::clone(&drain_started);
    let http_task = tokio::spawn(async move {
        let _ = http_shutdown_requested.await;
        http_drain_started.notify_one();
        let _ = http_released.await;
        Ok(())
    });
    let (http_server, release_handler) = server_with_admitted_handler(http_shutdown, http_task);

    let pipeline_drain_started = Arc::clone(&drain_started);
    let (pipelines, state_was_sent) = pipeline_supervisor_test_support::supervisor_with_final_state(
        Arc::new(config),
        executable,
        runtime_resources.directory().to_path_buf(),
        control_server.pipeline_launcher(),
        PipelineStateUpdate {
            document_id: document_id.clone(),
            state: PipelineLifecycleState::RestartBackoff(
                crate::runner::management::PipelineAttemptError {
                    document_etag: Some(TenonDocumentEtag::for_source(
                        document("drain-state").as_bytes(),
                    )),
                    code: "runner.pipeline_exited",
                    message: "Pipeline process exited unexpectedly",
                },
            ),
        },
        async move { pipeline_drain_started.notified().await },
    );
    let runner = RunnerMainLoop {
        execution: RunnerExecution::new(Box::new(AllowAll)),
        management,
        pipelines,
        http_server,
        control_server,
        diagnostics,
        runtime_resources,
    };

    let driver = async move {
        state_was_sent
            .await
            .map_err(|_| io::Error::other("Pipeline state was not sent during handler drain"))?;
        let deadline = time::Instant::now() + Duration::from_secs(1);
        loop {
            task::yield_now().await;
            let view = management_client
                .pipeline(document_id.clone())
                .await
                .map_err(|_| io::Error::other("Management stopped before handler drain finished"))?
                .ok_or_else(|| io::Error::other("Pipeline view disappeared"))?;
            if matches!(view.status.convergence, PipelineConvergence::RestartBackoff) {
                break;
            }
            if time::Instant::now() >= deadline {
                return Err(io::Error::other(
                    "Pipeline state did not advance while a handler was draining",
                ));
            }
        }
        release_handler
            .send(())
            .map_err(|_| io::Error::other("HTTP handler tracker disappeared"))?;
        release_http
            .send(())
            .map_err(|_| io::Error::other("HTTP drain owner disappeared"))?;
        Ok::<_, io::Error>(())
    };
    let (runner_result, driver_result) =
        tokio::join!(runner.run_until_shutdown(async { Ok(()) }), driver,);
    driver_result?;
    runner_result.map_err(io::Error::other)?;
    Ok(())
}

fn verified_document(
    config: &RunnerConfig,
    id: &str,
    source_program: &str,
) -> io::Result<(Box<[u8]>, Arc<VerifiedTenonDocument>)> {
    let source = document(id)
        .replace("com.example.source", source_program)
        .into_bytes()
        .into_boxed_slice();
    let parsed = UnverifiedTenonDocument::parse(&source).map_err(io::Error::other)?;
    let verified = config
        .tenon_document_verifier()
        .map_err(io::Error::other)?
        .verify(parsed)
        .map_err(io::Error::other)?;
    Ok((source, Arc::new(verified)))
}

#[test]
fn startup_removes_only_stale_runner_runtime_directories() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;
    let parent = state_layout.pipeline_runtime_directory().to_path_buf();
    let stale = parent.join(".tenon-runner-stale");
    let unrelated = parent.join("operator-owned");
    fs::create_dir_all(stale.join("nested"))?;
    fs::write(stale.join("nested/queue"), "stale")?;
    fs::create_dir_all(&unrelated)?;
    fs::create_dir(unrelated.join(".tenon-cleanup-incomplete-unrelated"))?;

    recover_stale_runtime_resources(&parent).map_err(io::Error::other)?;
    let runtime = RunnerRuntimeResources::prepare(&parent, captured_runner_executable()?)
        .map_err(io::Error::other)?;

    assert!(!stale.exists());
    assert!(unrelated.is_dir());
    assert!(runtime.directory().is_dir());
    runtime
        .cleanup(RunnerRuntimeCleanup::Remove)
        .map_err(io::Error::other)
}

#[tokio::test(flavor = "current_thread")]
async fn startup_removes_the_stale_control_socket_derived_from_a_crashed_runtime() -> io::Result<()>
{
    let state_directory = tempfile::tempdir()?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;
    let pipeline_runtime_directory = state_layout.pipeline_runtime_directory();
    let stale =
        RunnerRuntimeResources::prepare(&pipeline_runtime_directory, captured_runner_executable()?)
            .map_err(io::Error::other)?;
    let server = RunnerControlServer::start(
        stale.directory(),
        crate::runner::process_resources::test_support::unavailable(),
        RunnerDiagnostics::new(),
        crate::runner::metrics::test_support::empty()?,
    )
    .map_err(io::Error::other)?;
    let socket_directory = control_server_test_support::socket_path(&server)
        .parent()
        .ok_or_else(|| io::Error::other("Control socket parent is missing"))?
        .to_path_buf();
    assert_eq!(
        socket_directory.metadata()?.permissions().mode() & 0o777,
        0o700
    );
    let stale_path = stale.directory().to_path_buf();
    mem::forget(stale);
    mem::forget(server);

    recover_stale_runtime_resources(&pipeline_runtime_directory).map_err(io::Error::other)?;
    let current =
        RunnerRuntimeResources::prepare(&pipeline_runtime_directory, captured_runner_executable()?)
            .map_err(io::Error::other)?;

    assert!(!stale_path.exists());
    assert!(!socket_directory.exists());
    current
        .cleanup(RunnerRuntimeCleanup::Remove)
        .map_err(io::Error::other)
}

#[test]
fn socket_cleanup_failure_keeps_a_recoverable_runtime_identity() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;
    let pipeline_runtime_directory = state_layout.pipeline_runtime_directory();
    let runtime =
        RunnerRuntimeResources::prepare(&pipeline_runtime_directory, captured_runner_executable()?)
            .map_err(io::Error::other)?;
    let runtime_path = runtime.directory().to_path_buf();

    runtime
        .cleanup(RunnerRuntimeCleanup::RecoverLater)
        .map_err(io::Error::other)?;
    assert!(runtime_path.is_dir());

    recover_stale_runtime_resources(&pipeline_runtime_directory).map_err(io::Error::other)?;
    assert!(!runtime_path.exists());
    Ok(())
}
