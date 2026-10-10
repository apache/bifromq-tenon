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

use super::recover;
use crate::config::RunnerConfig;
use crate::payload_contract::PluginInterface;
use crate::pipeline::test_support::PipelineRevision;
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::executable::CapturedRunnerExecutable;
use crate::runner::extensions::RunnerHooks;
use crate::runner::main_loop::RunnerMainLoop;
use crate::runner::pipeline::PipelineLifecycleTarget;
use crate::runner::pipeline::{RuntimeResolution, RuntimeResolutionIssue, RuntimeResolver};
use crate::runner::plugin::package::tests::{valid_program_package, valid_source_program_package};
use crate::runner::plugin::store::PluginProgramStore;
use crate::runner::plugin::store::test_support::program_count;
use crate::runner::process_resources;
use crate::runner::state_directory::prepare_runner_state_directory;
use crate::runner::test_support::{document, install_plugin, load_config, write_document};
use opentelemetry_proto::tonic::metrics::v1::{metric, number_data_point};
use rustix::io::Errno;
use rustix::process::{Pid, test_kill_process};
use std::fs::{self, File};
use std::io::{self, Cursor};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt as _;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::{task, time};

#[path = "../../../tests/support/file_tree.rs"]
mod file_tree;

#[test]
fn startup_resolution_reflects_the_current_plugin_registry() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    install_plugin(state_directory.path(), PluginInterface::Source)?;
    install_plugin(state_directory.path(), PluginInterface::Sink)?;
    write_document(state_directory.path(), "ready", document("ready"))?;
    let config = load_config(state_directory.path())?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;

    let (documents, programs) = recover(
        &config,
        &state_layout,
        &RunnerHooks::default().artifact_protection,
        &state_layout.pipeline_runtime_directory(),
    )
    .map_err(io::Error::other)?
    .into_parts();
    let mut documents = documents.into_vec();
    if documents.len() != 1 {
        return Err(io::Error::other(
            "startup did not recover exactly one Tenon Document",
        ));
    }
    let recovered = documents
        .pop()
        .ok_or_else(|| io::Error::other("recovered Tenon Document is unavailable"))?;
    let (source, document) = recovered.into_parts();
    let document_etag = TenonDocumentEtag::for_source(&source);
    let RuntimeResolution::Ready(plan) = RuntimeResolver::new(
        config.script_vm_limits(),
        process_resources::available_cpu_count()?,
    )
    .resolve(&document, &programs) else {
        return Err(io::Error::other("installed runtime material was not ready"));
    };
    assert_eq!(plan.document().id().as_str(), "ready");
    assert_eq!(document_etag.directory_name().len(), 64);
    let working_directory = state_directory.path().join("pipeline-working");
    let target = PipelineLifecycleTarget::new(plan, document_etag);
    let bootstrap = target.bootstrap(&config, &working_directory);
    let revision = PipelineRevision::from_runner(
        bootstrap
            .revision_plan
            .ok_or_else(|| io::Error::other("Fixture revision is missing"))?,
    );
    let environment = bootstrap
        .environment
        .ok_or_else(|| io::Error::other("Fixture environment is missing"))?;
    assert_eq!(revision.document_etag(), document_etag.strong_value());
    assert_eq!(environment.pipeline_working_directory(), working_directory);

    fs::remove_dir_all(
        state_directory
            .path()
            .join("plugins/programs/com.example.sink"),
    )?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;
    let (documents, programs) = recover(
        &config,
        &state_layout,
        &RunnerHooks::default().artifact_protection,
        &state_layout.pipeline_runtime_directory(),
    )
    .map_err(io::Error::other)?
    .into_parts();
    let mut documents = documents.into_vec();
    if documents.len() != 1 {
        return Err(io::Error::other(
            "startup did not recover exactly one Tenon Document",
        ));
    }
    let recovered = documents
        .pop()
        .ok_or_else(|| io::Error::other("recovered Tenon Document is unavailable"))?;
    let (_source, document) = recovered.into_parts();
    let RuntimeResolution::Unready(issues) = RuntimeResolver::new(
        config.script_vm_limits(),
        process_resources::available_cpu_count()?,
    )
    .resolve(&document, &programs) else {
        return Err(io::Error::other(
            "missing Sink Program did not make the document unready",
        ));
    };
    assert_eq!(document.id().as_str(), "ready");
    assert_eq!(issues.len(), 1);
    assert!(
        matches!(&issues[0], RuntimeResolutionIssue::PluginProgramMissing { program_name, .. } if program_name.as_str() == "com.example.sink")
    );
    Ok(())
}

#[test]
fn startup_recovers_all_program_interfaces_in_one_store() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    install_plugin(state_directory.path(), PluginInterface::Source)?;
    install_plugin(state_directory.path(), PluginInterface::Sink)?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;
    let mut store = PluginProgramStore::recover(
        state_layout.plugin_program_store_directory(),
        state_layout.plugin_program_store_directory(),
        std::sync::Arc::new(crate::runner::extensions::ByPass),
    )
    .map_err(io::Error::other)?;
    store
        .install(Cursor::new(valid_program_package(
            PluginInterface::SourceAndSink,
        )?))
        .map_err(io::Error::other)?;
    let config = load_config(state_directory.path())?;

    let (_documents, programs) = recover(
        &config,
        &state_layout,
        &RunnerHooks::default().artifact_protection,
        &state_layout.pipeline_runtime_directory(),
    )
    .map_err(io::Error::other)?
    .into_parts();

    assert_eq!(program_count(&programs), 3);
    Ok(())
}

#[test]
fn startup_preserves_a_corrupt_original_package_and_documents() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;
    let mut store = PluginProgramStore::recover(
        state_layout.plugin_program_store_directory(),
        state_layout.pipeline_runtime_directory(),
        std::sync::Arc::new(crate::runner::extensions::ByPass),
    )
    .map_err(io::Error::other)?;
    store
        .install(Cursor::new(valid_source_program_package()?))
        .map_err(io::Error::other)?;
    drop(store);
    write_document(
        state_directory.path(),
        "keep-document",
        document("keep-document"),
    )?;
    let package = state_directory
        .path()
        .join("plugins/programs/com.example.source/.tenon-artifact-92521fc3cbd964bdc9f584a991b89fddaa5754ed1cc96d6d42445338669c1305");
    fs::write(&package, b"invalid archive")?;
    let config = load_config(state_directory.path())?;
    let error = recover(
        &config,
        &state_layout,
        &RunnerHooks::default().artifact_protection,
        &state_layout.pipeline_runtime_directory(),
    )
    .err()
    .ok_or_else(|| io::Error::other("Corrupt package did not stop recovery"))?;
    assert_eq!(error.code(), "plugin_store_integrity_invalid");
    assert_eq!(fs::read(&package)?, b"invalid archive");
    assert_eq!(
        fs::read(
            fs::read_dir(state_layout.tenon_document_store_directory())?
                .next()
                .transpose()?
                .ok_or_else(|| io::Error::other("Saved Document is missing"))?
                .path()
        )?,
        document("keep-document").as_bytes()
    );
    Ok(())
}

#[test]
fn startup_rejects_a_committed_file_whose_name_does_not_match_its_document_id() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    write_document(
        state_directory.path(),
        "different-id",
        document("actual-id"),
    )?;
    let config = load_config(state_directory.path())?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;

    let error = recover(
        &config,
        &state_layout,
        &RunnerHooks::default().artifact_protection,
        &state_layout.pipeline_runtime_directory(),
    )
    .err()
    .ok_or_else(|| io::Error::other("mismatched committed identity was accepted"))?;

    assert_eq!(error.code(), "runner.tenon_document_identity_mismatch");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn empty_runner_starts_and_stops_its_private_control_service() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    let config = load_config(state_directory.path())?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;

    RunnerMainLoop::start(
        config,
        crate::runner::process_resources::test_support::unavailable(),
        state_layout,
        CapturedRunnerExecutable::capture_current()?,
        None,
        RunnerHooks::default(),
        crate::metrics::test_support::runner()?,
    )
    .await
    .map_err(io::Error::other)?
    .run_until_shutdown(async { Ok(()) })
    .await
    .map_err(io::Error::other)?;

    let pipelines = state_directory.path().join("pipelines");
    assert!(pipelines.is_dir());
    assert!(pipelines.read_dir()?.next().is_none());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn failed_http_start_cleans_already_prepared_runtime_owners() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    install_plugin(state_directory.path(), PluginInterface::Source)?;
    install_plugin(state_directory.path(), PluginInterface::Sink)?;
    write_document(
        state_directory.path(),
        "http-start-failure",
        document("http-start-failure"),
    )?;
    let config = load_config(state_directory.path())?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;
    let _occupied = TcpListener::bind(config.http_listen_address())?;

    let error = RunnerMainLoop::start(
        config,
        crate::runner::process_resources::test_support::unavailable(),
        state_layout,
        CapturedRunnerExecutable::capture_current()?,
        None,
        RunnerHooks::default(),
        crate::metrics::test_support::runner()?,
    )
    .await
    .err()
    .ok_or_else(|| io::Error::other("Occupied HTTP address was accepted"))?;

    assert_eq!(error.code(), "runner.http_server_failed");
    let pipelines = state_directory.path().join("pipelines");
    assert!(pipelines.is_dir());
    assert!(pipelines.read_dir()?.next().is_none());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_ready_before_lifecycle_poll_prevents_pipeline_spawn() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    install_plugin(state_directory.path(), PluginInterface::Source)?;
    install_plugin(state_directory.path(), PluginInterface::Sink)?;
    write_document(
        state_directory.path(),
        "shutdown-before-spawn",
        document("shutdown-before-spawn"),
    )?;
    let config = load_config(state_directory.path())?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;
    let marker = state_directory.path().join("pipeline-spawned");
    let executable = state_directory.path().join("pipeline.sh");
    fs::write(
        &executable,
        format!("#!/bin/sh\n: > {}\nexit 1\n", marker.display()),
    )?;
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))?;

    RunnerMainLoop::start(
        config,
        crate::runner::process_resources::test_support::unavailable(),
        state_layout,
        CapturedRunnerExecutable::from_file(File::open(&executable)?)?,
        None,
        RunnerHooks::default(),
        crate::metrics::test_support::runner()?,
    )
    .await
    .map_err(io::Error::other)?
    .run_until_shutdown(async { Ok(()) })
    .await
    .map_err(io::Error::other)?;

    assert!(!marker.exists());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_period_plugin_commit_does_not_start_a_new_pipeline() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    install_plugin(state_directory.path(), PluginInterface::Sink)?;
    write_document(
        state_directory.path(),
        "shutdown-commit",
        document("shutdown-commit"),
    )?;
    let config = load_config(state_directory.path())?;
    let http_address = config.http_listen_address();
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;
    let marker = state_directory
        .path()
        .join("pipeline-spawned-after-shutdown");
    let executable = state_directory.path().join("pipeline.sh");
    fs::write(
        &executable,
        format!("#!/bin/sh\n: > {}\nexit 1\n", marker.display()),
    )?;
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))?;
    let main_loop = RunnerMainLoop::start(
        config,
        crate::runner::process_resources::test_support::unavailable(),
        state_layout,
        CapturedRunnerExecutable::from_file(File::open(&executable)?)?,
        None,
        RunnerHooks::default(),
        crate::metrics::test_support::runner()?,
    )
    .await
    .map_err(io::Error::other)?;
    let package = valid_program_package(PluginInterface::Source)?;
    let (shutdown, shutdown_requested) = oneshot::channel();

    let request = async move {
        let mut stream = TcpStream::connect(http_address).await?;
        stream
            .write_all(
                format!(
                    "POST /plugins HTTP/1.1\r\nHost: {http_address}\r\nConnection: close\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nExpect: 100-continue\r\n\r\n",
                    package.len(),
                )
                .as_bytes(),
            )
            .await?;
        let mut interim = Vec::new();
        let mut byte = [0_u8; 1];
        while !interim.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await?;
            interim.push(byte[0]);
            if interim.len() > 1024 {
                return Err(io::Error::other("HTTP interim response is too large"));
            }
        }
        if interim != b"HTTP/1.1 100 Continue\r\n\r\n" {
            return Err(io::Error::other(format!(
                "Plugin upload was not admitted: {}",
                String::from_utf8_lossy(&interim),
            )));
        }
        let _ = shutdown.send(());
        let admission_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if TcpStream::connect(http_address).await.is_err() {
                break;
            }
            if Instant::now() >= admission_deadline {
                return Err(io::Error::other(
                    "HTTP admission did not close after shutdown",
                ));
            }
            task::yield_now().await;
        }
        stream.write_all(&package).await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        if !response.starts_with(b"HTTP/1.1 201") {
            return Err(io::Error::other(format!(
                "Shutdown-period Plugin upload failed: {}",
                String::from_utf8_lossy(&response),
            )));
        }
        Ok::<_, io::Error>(())
    };
    let shutdown_signal = async move {
        shutdown_requested
            .await
            .map_err(|_| io::Error::other("Shutdown trigger was dropped"))
    };
    let (runner_result, request_result) =
        tokio::join!(main_loop.run_until_shutdown(shutdown_signal), request,);
    request_result?;
    runner_result.map_err(io::Error::other)?;

    assert!(!marker.exists());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_while_pipeline_waits_to_attach_reaps_the_process_tree() -> io::Result<()> {
    observe_unattached_pipeline(UnattachedShutdown::WhileStarting).await
}

#[tokio::test(flavor = "current_thread")]
async fn starting_and_failed_restart_metrics_preserve_process_cleanup() -> io::Result<()> {
    observe_unattached_pipeline(UnattachedShutdown::AfterFailedRestart).await
}

enum UnattachedShutdown {
    WhileStarting,
    AfterFailedRestart,
}

async fn observe_unattached_pipeline(shutdown: UnattachedShutdown) -> io::Result<()> {
    let metrics = crate::metrics::test_support::runner()?;
    let state_directory = tempfile::tempdir()?;
    install_plugin(state_directory.path(), PluginInterface::Source)?;
    install_plugin(state_directory.path(), PluginInterface::Sink)?;
    write_document(
        state_directory.path(),
        "attach-shutdown",
        document("attach-shutdown"),
    )?;
    let _ = load_config(state_directory.path())?;
    let config_path = state_directory.path().join("runner.jsonc");
    let mut settings: serde_json::Value = serde_json::from_slice(&fs::read(&config_path)?)?;
    settings["pipeline"]["retryBackoff"] =
        serde_json::json!({"initialDelayMs":500,"maximumDelayMs":500});
    fs::write(&config_path, serde_json::to_vec(&settings)?)?;
    let config = RunnerConfig::load(&config_path).map_err(io::Error::other)?;
    let state_layout =
        prepare_runner_state_directory(state_directory.path()).map_err(io::Error::other)?;
    let process_marker = state_directory.path().join("pipeline.pid");
    let executable = state_directory.path().join("pipeline.sh");
    fs::write(
        &executable,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$$\" > {}\ntrap 'exit 0' TERM\nwhile :; do sleep 1; done\n",
            process_marker.display()
        ),
    )?;
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))?;
    let shutdown_marker = process_marker.clone();

    RunnerMainLoop::start(
        config,
        crate::runner::process_resources::test_support::unavailable(),
        state_layout,
        CapturedRunnerExecutable::from_file(File::open(&executable)?)?,
        None,
        RunnerHooks::default(),
        std::sync::Arc::clone(&metrics),
    )
    .await
    .map_err(io::Error::other)?
    .run_until_shutdown(async {
        time::timeout(Duration::from_secs(5), async {
            while !fs::read_to_string(&shutdown_marker)
                .is_ok_and(|value| value.trim().parse::<i32>().is_ok())
            {
                time::sleep(Duration::from_millis(5)).await;
            }
            let mut observed_starting = false;
            loop {
                {
                    let export = metrics.collect(&[]);
                    let points: Vec<_> = export
                        .resource_metrics
                        .iter()
                        .flat_map(|resource| &resource.scope_metrics)
                        .flat_map(|scope| &scope.metrics)
                        .filter_map(|metric| match &metric.data {
                            Some(metric::Data::Gauge(gauge)) => Some((metric.name.as_str(), gauge)),
                            _ => None,
                        })
                        .flat_map(|(name, gauge)| {
                            gauge.data_points.iter().map(move |point| (name, point))
                        })
                        .collect();
                    let value = |name| {
                        points
                            .iter()
                            .find_map(|(metric, point)| (*metric == name).then_some(point.value))
                    };
                    let state = value("tenon.pipeline.state");
                    if state == Some(Some(number_data_point::Value::AsInt(1))) {
                        assert_eq!(
                            value("tenon.pipeline.configuration.applied"),
                            Some(Some(number_data_point::Value::AsInt(0)))
                        );
                        if matches!(shutdown, UnattachedShutdown::WhileStarting) {
                            break;
                        }
                        // The live first process remains valid; every later exec must fail.
                        let images = file_tree::named_files(state_directory.path(), "tenon")?;
                        assert_eq!(images.len(), 1);
                        fs::set_permissions(&images[0], fs::Permissions::from_mode(0o600))?;
                        observed_starting = true;
                    }
                    if observed_starting && state == Some(Some(number_data_point::Value::AsInt(4)))
                    {
                        time::sleep(Duration::from_millis(1100)).await;
                        {
                            let export = metrics.collect(&[]);
                            assert!(
                                !export
                                    .resource_metrics
                                    .iter()
                                    .flat_map(|resource| &resource.scope_metrics)
                                    .flat_map(|scope| &scope.metrics)
                                    .any(|metric| metric.name == "tenon.pipeline.restarts")
                            );
                        }
                        break;
                    }
                }
                time::sleep(Duration::from_millis(5)).await;
            }
            Ok::<_, io::Error>(())
        })
        .await
        .map_err(|_| io::Error::other("Pipeline starting/backoff metrics were not observed"))?
    })
    .await
    .map_err(io::Error::other)?;

    let process_id = fs::read_to_string(&process_marker)?
        .trim()
        .parse::<i32>()
        .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))?;
    let process_id = Pid::from_raw(process_id)
        .ok_or_else(|| io::Error::other("Pipeline process id is invalid"))?;
    assert_eq!(test_kill_process(process_id), Err(Errno::SRCH));
    metrics.shutdown();
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn policy_change_during_recovery_is_applied_before_pipeline_start() -> io::Result<()> {
    use crate::runner::extensions::{
        ArtifactProtection, ExecutionDenied, ExecutionPermit, ExecutionPolicy, ExecutionScope,
        NoHttpAuth, PolicyChanges,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct Policy {
        denied: Arc<AtomicBool>,
        changes: PolicyChanges,
    }
    impl ExecutionPolicy for Policy {
        fn authorize(
            &self,
            _scope: ExecutionScope<'_>,
        ) -> Result<ExecutionPermit, ExecutionDenied> {
            if self.denied.load(Ordering::Acquire) {
                Err(ExecutionDenied::new(
                    "test.recovery_denied",
                    "Execution is not permitted",
                    None,
                ))
            } else {
                Ok(ExecutionPermit::default())
            }
        }
        fn changes(&self) -> Option<PolicyChanges> {
            Some(self.changes.clone())
        }
    }
    struct Protection {
        denied: Arc<AtomicBool>,
        changes: tokio::sync::watch::Sender<()>,
    }
    impl ArtifactProtection for Protection {
        fn protect(&self, source: &[u8], output: &mut dyn io::Write) -> io::Result<()> {
            output.write_all(source)
        }
        fn unprotect(&self, stored: &[u8], output: &mut dyn io::Write) -> io::Result<()> {
            output.write_all(stored)?;
            self.denied.store(true, Ordering::Release);
            self.changes.send_replace(());
            Ok(())
        }
    }
    let root = tempfile::tempdir()?;
    let config = load_config(root.path())?;
    let layout = prepare_runner_state_directory(root.path()).map_err(io::Error::other)?;
    write_document(root.path(), "notify-recovery", document("notify-recovery"))?;
    let denied = Arc::new(AtomicBool::new(false));
    let (changes, receiver) = tokio::sync::watch::channel(());
    let hooks = RunnerHooks::new(
        Policy {
            denied: Arc::clone(&denied),
            changes: receiver,
        },
        Protection { denied, changes },
        NoHttpAuth,
    );
    let error = RunnerMainLoop::start(
        config,
        crate::runner::process_resources::test_support::unavailable(),
        layout,
        CapturedRunnerExecutable::capture_current()?,
        None,
        hooks,
        crate::metrics::test_support::runner()?,
    )
    .await
    .err()
    .ok_or_else(|| io::Error::other("Recovery notification was lost"))?;
    assert_eq!(error.code(), "test.recovery_denied");
    assert_eq!(
        fs::read_dir(root.path().join("tenon-documents"))?.count(),
        1
    );
    assert_eq!(fs::read_dir(root.path().join("pipelines"))?.count(), 0);
    Ok(())
}
