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

//! Real Runner launch, target control, process recovery, and Queue isolation.

use super::RunnerPipelineLifecycleError;
use crate::config::RunnerConfig;
use crate::contracts::core::{PipelineStatusSnapshot, PluginInstanceState};
use crate::contracts::sink::EgressRecord;
use crate::contracts::source::IngressRecord;
use crate::identifiers::{ExactVersion, ProgramName, TenonDocumentId};
use crate::payload_contract::PluginInterface;
use crate::pipeline::contract_test_support::{
    egress_queue_path, flow_channel_bell_path, loops_bell_path, sink_directory_name,
    source_directory_name,
};
use crate::pipeline::test_support::instance_directory;
use crate::runner;
use crate::runner::control_server::{
    ControlServerShutdown, ControlSocketCleanup, RunnerControlServer,
};
use crate::runner::diagnostics::RunnerDiagnostics;
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::execution::ExecutionExpiry;
use crate::runner::management::{PipelineDirective, PipelineLifecycleState};
use crate::runner::pipeline::PipelineDirectoryCleanupError;
use crate::runner::pipeline::PipelineLifecycleTarget;
use crate::runner::pipeline::test_support::{TargetFixture, shell_quote, snapshot};
use crate::runner::pipeline_supervisor::{
    PipelineCleanup, PipelineEvent, PipelineSupervisor, PipelineSupervisorError,
};
use crate::runner::plugin::store::{PluginStoreError, PluginUninstallOutcome};
use crate::runner::process_resources;
use crate::runner::test_support::load_config;
use metrics::test_support;
use prost::Message as _;
use runner::metrics;
use rustix::io::Errno;
use rustix::process;
use serde_json::{Value, json};
use std::error::Error;
use std::future::{Future, poll_fn};
use std::os::unix::fs::PermissionsExt as _;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use std::{env, fs, io};
use tenon_ipc::queue::contract_test_support::{open_reader, open_writer};
use tenon_ipc::queue::{ReadOutcome, WriteOutcome};
use tokio::time;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const TEST_LIMIT: Duration = Duration::from_secs(20);

#[cfg(not(feature = "loom-model"))]
mod shutdown_owner_loss;

#[tokio::test(flavor = "current_thread")]
async fn planned_shutdown_quiesces_sources_and_waits_for_final_child_exit() -> TestResult {
    with_fixture(async |fixture| {
        let target = fixture
            .set(&document("shutdown", "A", "delay-shutdown"))?
            .ok_or("target is unready")?;
        fixture.running(target.document_etag()).await?;
        let root = fixture.working_directory(&target);
        let source = instance_directory(&root, "input")?;
        let sink = instance_directory(&root, "archive")?;
        let pids = instance_pids(&root)?;
        let pipeline_pid = process_group(pids[0])?;
        let socket = fixture.socket_directory(pipeline_pid)?;
        drop(target);
        let program_name = ProgramName::try_from("com.example.archive")?;
        let exact_version = ExactVersion::try_from("1.0.0")?;
        let mut stopping = Box::pin(fixture.supervisor.shutdown());
        tokio::select! {
            _ = &mut stopping => {
                return Err("Pipeline stopped before final child exit".into());
            }
            result = wait_for_file(sink.join("shutdown.received")) => result?,
        }
        assert!(source.join("source-quiesced.received").exists());
        assert!(root.exists());
        assert!(socket.exists());
        assert!(process::test_kill_process(pids[1]).is_ok());
        assert!(matches!(
            fixture
                .targets
                .store
                .uninstall(&program_name, &exact_version),
            Err(PluginStoreError::ProgramInUse)
        ));
        fs::write(sink.join("allow-shutdown"), [])?;
        let result = stopping.await;
        assert_eq!(result.cleanup, PipelineCleanup::Complete);
        assert!(result.failures.is_empty());
        assert!(!root.exists());
        assert!(!socket.exists());
        wait_for_processes_gone(&pids).await?;
        assert_reaped(pipeline_pid)?;
        assert_eq!(
            fixture
                .targets
                .store
                .uninstall(&program_name, &exact_version)?,
            PluginUninstallOutcome::Uninstalled
        );
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn applying_a_new_program_releases_the_old_installation_without_stopping_the_pipeline()
-> TestResult {
    with_fixture(async |fixture| {
        fixture.targets.install("com.example.archive", "2.0.0", PluginInterface::Sink)?;
        let first = fixture.set(&document("replace-program", "A", "normal"))?.ok_or("A is unready")?;
        fixture.running(first.document_etag()).await?;
        let root = fixture.working_directory(&first);
        let pipeline_pid = process_group(instance_pids(&root)?[0])?;
        fixture.observe(traffic(&root, 1)).await?;
        drop(first);
        let program_name = ProgramName::try_from("com.example.archive")?;
        let old_version = ExactVersion::try_from("1.0.0")?;
        assert!(matches!(fixture.targets.store.uninstall(&program_name, &old_version), Err(PluginStoreError::ProgramInUse)));
        let mut replacement = document("replace-program", "B", "normal");
        replacement["pluginInstances"]["archive"]["exactVersion"] = json!("2.0.0");
        replacement["flows"]["forward"]["process"]["script"] = json!("local output = registry:getBuilder('com.example.archive@2.0.0'); function main(event) emit(output:build()) end");
        let second = fixture.set(&replacement)?.ok_or("B is unready")?;
        fixture.running(second.document_etag()).await?;
        drop(second);
        assert_eq!(fixture.targets.store.uninstall(&program_name, &old_version)?, PluginUninstallOutcome::Uninstalled);
        assert_eq!(process_group(instance_pids(&root)?[0])?, pipeline_pid);
        fixture.observe(traffic(&root, 2)).await?;
        assert_eq!(fixture.launch_count()?, 1);
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn planned_shutdown_deadline_reaps_blocked_pipeline_without_restarting_it() -> TestResult {
    with_fixture(async |fixture| {
        let target = fixture.set(&document("shutdown-timeout", "A", "delay-shutdown"))?.ok_or("target is unready")?;
        fixture.running(target.document_etag()).await?;
        let root = fixture.working_directory(&target);
        let sink = instance_directory(&root, "archive")?;
        let pids = instance_pids(&root)?;
        let pipeline_pid = process_group(pids[0])?;
        let socket = fixture.socket_directory(pipeline_pid)?;
        let mut stopping = Box::pin(fixture.supervisor.shutdown());
        tokio::select! {
            _ = &mut stopping => return Err("Pipeline exited before the held Shutdown was observed".into()),
            result = wait_for_file(sink.join("shutdown.received")) => result?,
        }
        drop(stopping);
        assert!(root.exists());
        assert!(!sink.join("allow-shutdown").exists());
        let timeout = RunnerConfig::load(&fixture.root.path().join("runner.jsonc"))?.pipeline_shutdown_timeout();
        time::pause();
        time::advance(timeout + Duration::from_millis(1)).await;
        // Resume real process I/O after crossing only the Runner-owned deadline.
        time::resume();
        wait_for_processes_gone(&pids).await?;
        let result = fixture.supervisor.shutdown().await;
        assert_eq!(result.cleanup, PipelineCleanup::Complete);
        assert!(!root.exists());
        assert!(result.failures.is_empty());
        assert_eq!(result.timed_out_documents, [TenonDocumentId::try_from("shutdown-timeout")?]);
        assert!(!socket.exists());
        assert_reaped(pipeline_pid)?;
        assert_eq!(fixture.launch_count()?, 1);
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn deadline_recovery_cleans_before_latest_bootstrap_and_preserves_sibling_traffic()
-> TestResult {
    for blocked_phase in [
        BlockedHandoff::SinkShutdown,
        BlockedHandoff::SourceCompletion,
    ] {
        with_fixture(async |fixture| {
            let handoff_document = |endpoint: &str| {
                let mut value = document(
                    "recover",
                    endpoint,
                    if endpoint == "A" && matches!(blocked_phase, BlockedHandoff::SinkShutdown) {
                        "delay-shutdown"
                    } else {
                        "normal"
                    },
                );
                if matches!(blocked_phase, BlockedHandoff::SourceCompletion) {
                    value["pluginInstances"]["input"]["config"]["endpoint"] = json!(endpoint);
                }
                value
            };
            let first = fixture.set(&handoff_document("A"))?.ok_or("A is unready")?;
            let sibling = fixture
                .set(&document("sibling", "steady", "normal"))?
                .ok_or("sibling is unready")?;
            fixture.running(first.document_etag()).await?;
            fixture.running(sibling.document_etag()).await?;
            let root = fixture.working_directory(&first);
            let sibling_root = fixture.working_directory(&sibling);
            let old_pids = instance_pids(&root)?;
            let pipeline_pid = process_group(old_pids[0])?;
            let sibling_pid = process_group(instance_pids(&sibling_root)?[0])?;
            let old_socket = fixture.socket_directory(pipeline_pid)?;
            fixture.observe(traffic(&root, 1)).await?;
            fixture.observe(traffic(&sibling_root, 1)).await?;
            let blocked = fixture.set(&handoff_document("B"))?.ok_or("B is unready")?;
            fixture
                .observe(wait_for_file(match blocked_phase {
                    BlockedHandoff::SinkShutdown => {
                        instance_directory(&root, "archive")?.join("shutdown.received")
                    }
                    BlockedHandoff::SourceCompletion => {
                        instance_directory(&root, "input")?.join("source-quiesced.received")
                    }
                }))
                .await?;
            fixture.observe(traffic(&sibling_root, 2)).await?;
            fixture.require_removed_before_spawn(&[root.clone(), old_socket.clone()])?;
            let latest = fixture.set(&handoff_document("C"))?.ok_or("C is unready")?;
            fixture.running(latest.document_etag()).await?;
            assert!(
                !fixture.applied(&blocked),
                "blocked B must never be reported applied"
            );
            assert!(!root.exists());
            assert!(!old_socket.exists());
            fixture.observe(wait_for_processes_gone(&old_pids)).await?;
            assert_reaped(pipeline_pid)?;
            let latest_root = fixture.working_directory(&latest);
            let config: Value = serde_json::from_str(&fs::read_to_string(
                instance_directory(&latest_root, "archive")?.join("configs.received"),
            )?)?;
            assert_eq!(config["endpoint"], "C");
            fixture.observe(traffic(&latest_root, 1)).await?;
            fixture.observe(traffic(&sibling_root, 3)).await?;
            assert_eq!(
                process_group(instance_pids(&sibling_root)?[0])?,
                sibling_pid
            );
            assert_eq!(
                fixture.launch_count()?,
                3,
                "two initial Pipelines and only latest C may start"
            );
            Ok(())
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn starting_instances_keep_installed_programs_after_latest_becomes_unready() -> TestResult {
    with_fixture(async |fixture| {
        let first = fixture
            .set(&document("starting-reference", "A", "delay-ready"))?
            .ok_or("A is unready")?;
        let etag = first.document_etag();
        let root = fixture.working_directory(&first);
        let sink = instance_directory(&root, "archive")?;
        fixture
            .observe(wait_for_file(sink.join("lifecycle.received")))
            .await?;
        while !fixture.statuses.iter().any(|status| {
            status.document_etag == etag.strong_value()
                && status.plugin_instances.iter().any(|instance| {
                    instance.id == "archive" && instance.state() == PluginInstanceState::Starting
                })
        }) {
            fixture.next_event().await?;
        }
        let mut unready = document("starting-reference", "missing", "normal");
        unready["pluginInstances"]["archive"]["programName"] = json!("com.example.missing");
        assert!(fixture.set(&unready)?.is_none());
        drop(first);
        let name = ProgramName::try_from("com.example.archive")?;
        let version = ExactVersion::try_from("1.0.0")?;
        assert!(matches!(
            fixture.targets.store.uninstall(&name, &version),
            Err(PluginStoreError::ProgramInUse)
        ));
        fs::write(sink.join("allow-ready"), [])?;
        fixture.running(etag).await?;
        fixture.observe(traffic(&root, 1)).await?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn instance_retry_keeps_its_program_without_a_runnable_latest_target() -> TestResult {
    with_fixture(async |fixture| {
        let first = fixture
            .set(&document("retry-reference", "A", "normal"))?
            .ok_or("A is unready")?;
        let etag = first.document_etag();
        fixture.running(etag).await?;
        let root = fixture.working_directory(&first);
        let old_pids = instance_pids(&root)?;
        let pipeline_pid = process_group(old_pids[0])?;
        let mut unready = document("retry-reference", "missing", "normal");
        unready["pluginInstances"]["archive"]["programName"] = json!("com.example.missing");
        assert!(fixture.set(&unready)?.is_none());
        drop(first);
        let previous_statuses = fixture.statuses.len();
        process::kill_process(old_pids[1], process::Signal::KILL)?;
        while !fixture.statuses[previous_statuses..].iter().any(|status| {
            status.document_etag == etag.strong_value()
                && status.plugin_instances.iter().any(|instance| {
                    instance.id == "archive"
                        && instance.state() == PluginInstanceState::RestartBackoff
                })
        }) {
            fixture.next_event().await?;
        }
        let name = ProgramName::try_from("com.example.archive")?;
        let version = ExactVersion::try_from("1.0.0")?;
        assert!(matches!(
            fixture.targets.store.uninstall(&name, &version),
            Err(PluginStoreError::ProgramInUse)
        ));
        // Observe a new actual child, not an older Running snapshot.
        fixture
            .observe(async {
                loop {
                    let pids = instance_pids(&root)?;
                    if pids[1] != old_pids[1] {
                        return Ok(());
                    }
                    time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await?;
        while !fixture.statuses[previous_statuses..].iter().any(|status| {
            status.document_etag == etag.strong_value()
                && status
                    .plugin_instances
                    .iter()
                    .all(|instance| instance.state() == PluginInstanceState::Running)
        }) {
            fixture.next_event().await?;
        }
        fixture.observe(traffic(&root, 1)).await?;
        assert_eq!(process_group(instance_pids(&root)?[0])?, pipeline_pid);
        assert_eq!(fixture.launch_count()?, 1);
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn the_real_runner_publishes_c_then_d_while_b_is_blocked_and_converges_to_d() -> TestResult {
    with_fixture(async |fixture| {
        let first = fixture
            .set(&document("latest-pending", "A", "delay-shutdown"))?
            .ok_or("A is unready")?;
        fixture.running(first.document_etag()).await?;
        let root = fixture.working_directory(&first);
        let pipeline_pid = process_group(instance_pids(&root)?[0])?;
        let blocked = fixture
            .set(&document("latest-pending", "B", "normal"))?
            .ok_or("B is unready")?;
        let sink = instance_directory(&root, "archive")?;
        fixture
            .observe(wait_for_file(sink.join("shutdown.received")))
            .await?;
        let third = fixture
            .set(&document("latest-pending", "C", "normal"))?
            .ok_or("C is unready")?;
        fixture.published(&third).await?;
        let fourth = fixture
            .set(&document("latest-pending", "D", "normal"))?
            .ok_or("D is unready")?;
        fixture.published(&fourth).await?;
        assert!(!fixture.applied(&blocked));
        assert!(!fixture.applied(&third));
        fs::write(sink.join("allow-shutdown"), [])?;
        fixture.running(fourth.document_etag()).await?;
        assert!(!fixture.applied(&third));
        let configs = fs::read_to_string(sink.join("configs.received"))?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            configs.last().ok_or("no config was received")?["endpoint"],
            "D"
        );
        assert!(configs.iter().all(|config| config["endpoint"] != "C"));
        fixture.observe(traffic(&root, 1)).await?;
        assert_eq!(process_group(instance_pids(&root)?[0])?, pipeline_pid);
        assert_eq!(fixture.launch_count()?, 1);
        Ok(())
    })
    .await
}

#[derive(Clone, Copy)]
enum BlockedHandoff {
    SinkShutdown,
    SourceCompletion,
}

#[tokio::test(flavor = "current_thread")]
async fn an_unready_latest_document_does_not_cancel_an_already_received_target() -> TestResult {
    with_fixture(async |fixture| {
        let first = fixture
            .set(&document("pending-unready", "A", "delay-shutdown"))?
            .ok_or("A is unready")?;
        fixture.running(first.document_etag()).await?;
        let root = fixture.working_directory(&first);
        let pid = process_group(instance_pids(&root)?[0])?;
        let blocked = fixture
            .set(&document("pending-unready", "B", "normal"))?
            .ok_or("B is unready")?;
        let sink = instance_directory(&root, "archive")?;
        fixture
            .observe(wait_for_file(sink.join("shutdown.received")))
            .await?;
        let mut unready = document("pending-unready", "C", "normal");
        unready["pluginInstances"]["archive"]["programName"] = json!("com.example.missing");
        assert!(fixture.set(&unready)?.is_none());
        let blocked_etag = blocked.document_etag();
        drop(first);
        drop(blocked);
        assert!(matches!(
            fixture.targets.store.uninstall(
                &ProgramName::try_from("com.example.archive")?,
                &ExactVersion::try_from("1.0.0")?,
            ),
            Err(PluginStoreError::ProgramInUse)
        ));
        fs::write(sink.join("allow-shutdown"), [])?;
        fixture.running(blocked_etag).await?;
        fixture.observe(traffic(&root, 1)).await?;
        assert_eq!(process_group(instance_pids(&root)?[0])?, pid);
        assert_eq!(fixture.launch_count()?, 1);
        let corrected = fixture
            .set(&document("pending-unready", "D", "normal"))?
            .ok_or("D is unready")?;
        fixture.running(corrected.document_etag()).await?;
        fixture.observe(traffic(&root, 2)).await?;
        assert_eq!(fixture.launch_count()?, 1);
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn reverting_to_the_applied_document_recovers_after_the_blocked_target_deadline() -> TestResult
{
    with_fixture(async |fixture| {
        let original = document("revert", "A", "delay-shutdown");
        let first = fixture.set(&original)?.ok_or("A is unready")?;
        fixture.running(first.document_etag()).await?;
        let root = fixture.working_directory(&first);
        let old_pids = instance_pids(&root)?;
        let old_pid = process_group(old_pids[0])?;
        let old_socket = fixture.socket_directory(old_pid)?;
        let blocked = fixture
            .set(&document("revert", "B", "normal"))?
            .ok_or("B is unready")?;
        fixture
            .observe(wait_for_file(
                instance_directory(&root, "archive")?.join("shutdown.received"),
            ))
            .await?;
        fixture.require_removed_before_spawn(&[root.clone(), old_socket.clone()])?;
        let reverted = fixture.set(&original)?.ok_or("reverted A is unready")?;
        assert_eq!(reverted.document_etag(), first.document_etag());
        let before_recovery = fixture.statuses.len();
        fixture.observe(wait_for_processes_gone(&old_pids)).await?;
        // The original A has the same ETag; only a new Running report proves recovery.
        while !fixture.statuses[before_recovery..]
            .iter()
            .any(|status| is_running(status, reverted.document_etag()))
        {
            fixture.next_event().await?;
        }
        assert!(!fixture.applied(&blocked));
        assert_reaped(old_pid)?;
        assert!(!old_socket.exists());
        let new_pid = process_group(instance_pids(&root)?[0])?;
        assert_ne!(new_pid, old_pid);
        let sink = instance_directory(&root, "archive")?;
        let config: Value = serde_json::from_slice(&fs::read(sink.join("configs.received"))?)?;
        assert_eq!(config["endpoint"], "A");
        fixture.observe(traffic(&root, 1)).await?;
        assert_eq!(fixture.launch_count()?, 2);
        // The reverted fixture deliberately retains A's delayed shutdown behavior.
        fs::write(sink.join("allow-shutdown"), [])?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn an_unready_latest_document_blocks_restart_until_corrected() -> TestResult {
    with_fixture(async |fixture| {
        let first = fixture
            .set(&document("unready", "A", "delay-shutdown"))?
            .ok_or("A is unready")?;
        fixture.running(first.document_etag()).await?;
        let root = fixture.working_directory(&first);
        let pid = process_group(instance_pids(&root)?[0])?;
        let old_socket = fixture.socket_directory(pid)?;
        let blocked = fixture
            .set(&document("unready", "B", "normal"))?
            .ok_or("B is unready")?;
        fixture
            .observe(wait_for_file(
                instance_directory(&root, "archive")?.join("shutdown.received"),
            ))
            .await?;
        let mut unready = document("unready", "C", "normal");
        unready["pluginInstances"]["archive"]["programName"] = json!("com.example.missing");
        assert!(
            fixture.set(&unready)?.is_none(),
            "real Runtime Integrity must reject the missing Program"
        );
        fixture
            .observe(async {
                while root.exists() || old_socket.exists() {
                    time::sleep(Duration::from_millis(5)).await;
                }
                Ok(())
            })
            .await?;
        assert_reaped(pid)?;
        let retry_delay =
            RunnerConfig::load(&fixture.root.path().join("runner.jsonc"))?.retry_maximum_delay();
        // Observe the actual cleanup first, then let the parent clock cross
        // the retry window without guessing how quickly child cleanup runs.
        time::pause();
        let restarted = time::timeout(retry_delay * 2, async {
            loop {
                match fixture.supervisor.next_event().await {
                    PipelineEvent::State { update, .. }
                        if matches!(update.state, PipelineLifecycleState::RestartBackoff(_)) => {}
                    event => return event,
                }
            }
        })
        .await;
        time::resume();
        assert!(
            restarted.is_err(),
            "unready input must remain idle after cleanup"
        );
        assert_eq!(
            fixture.launch_count()?,
            1,
            "unready latest input must not restart any older ready target"
        );
        assert!(!fixture.applied(&blocked));
        fixture.require_removed_before_spawn(&[root, old_socket])?;
        let corrected = fixture
            .set(&document("unready", "D", "normal"))?
            .ok_or("D is unready")?;
        fixture.running(corrected.document_etag()).await?;
        fixture
            .observe(traffic(&fixture.working_directory(&corrected), 1))
            .await?;
        assert_eq!(fixture.launch_count()?, 2);
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn directory_cleanup_failure_never_launches_a_second_pipeline() -> TestResult {
    with_fixture(async |fixture| {
        let first = fixture
            .set(&document("cleanup", "A", "delay-shutdown"))?
            .ok_or("A is unready")?;
        fixture.running(first.document_etag()).await?;
        let root = fixture.working_directory(&first);
        let pid = process_group(instance_pids(&root)?[0])?;
        let old_socket = fixture.socket_directory(pid)?;
        fixture.set(&document("cleanup", "B", "normal"))?;
        fixture
            .observe(wait_for_file(
                instance_directory(&root, "archive")?.join("shutdown.received"),
            ))
            .await?;
        // A real filesystem replacement makes remove_dir_all fail. The original
        // socket remains owned by this test until all child owners are reaped.
        fs::rename(&old_socket, fixture.root.path().join("displaced-control"))?;
        fs::write(&old_socket, b"not a directory")?;
        fixture.set(&document("cleanup", "C", "normal"))?;
        let failure = loop {
            match fixture.supervisor.next_event().await {
                PipelineEvent::State { .. } => {}
                PipelineEvent::Failure(failure) => break failure,
            }
        };
        let PipelineSupervisorError::Lifecycle { source, .. } = failure else {
            return Err(format!("unexpected supervisor failure: {failure}").into());
        };
        assert!(
            matches!(*source, RunnerPipelineLifecycleError::CleanupAfterRestart {
            cleanup: PipelineDirectoryCleanupError::Filesystem { ref path, ref source }, ..
        } if path == &old_socket && source.kind() == io::ErrorKind::NotADirectory)
        );
        assert_reaped(pid)?;
        assert!(old_socket.is_file());
        assert!(
            root.is_dir(),
            "Control cleanup failure must preserve the working directory"
        );
        assert_eq!(fixture.launch_count()?, 1);
        assert_eq!(
            fixture.supervisor.shutdown().await.cleanup,
            PipelineCleanup::Complete
        );
        Ok(())
    })
    .await
}

struct Fixture {
    supervisor: PipelineSupervisor,
    targets: TargetFixture,
    statuses: Vec<PipelineStatusSnapshot>,
    records: PathBuf,
    runtime: tempfile::TempDir,
    root: tempfile::TempDir,
}

impl Fixture {
    fn set(&mut self, document: &Value) -> TestResult<Option<Arc<PipelineLifecycleTarget>>> {
        let target = self.targets.resolve(document)?;
        let document_id = TenonDocumentId::try_from(
            document["id"]
                .as_str()
                .ok_or("document id is missing")?
                .to_owned(),
        )?;
        self.supervisor
            .apply(Box::new([PipelineDirective::SetTarget {
                document_id,
                target: target.clone(),
            }]))?;
        Ok(target)
    }

    fn working_directory(&self, target: &PipelineLifecycleTarget) -> PathBuf {
        self.runtime
            .path()
            .join(target.document_etag().directory_name())
    }

    fn applied(&self, target: &PipelineLifecycleTarget) -> bool {
        self.statuses
            .iter()
            .any(|status| status.document_etag == target.document_etag().strong_value())
    }

    async fn running(&mut self, etag: TenonDocumentEtag) -> TestResult {
        while !self.statuses.iter().any(|status| is_running(status, etag)) {
            self.next_event().await?;
        }
        Ok(())
    }

    async fn published(&mut self, target: &Arc<PipelineLifecycleTarget>) -> TestResult {
        self.observe(async {
            // The test and latest slot hold two references. The only other
            // owner after a lifecycle poll is the transport-accepted chain.
            while Arc::strong_count(target) < 3 {
                time::sleep(Duration::from_millis(5)).await;
            }
            Ok(())
        })
        .await
    }

    async fn next_event(&mut self) -> TestResult {
        match self.supervisor.next_event().await {
            PipelineEvent::State { update, .. } => {
                if let PipelineLifecycleState::Running(state) = update.state {
                    self.statuses.push(snapshot(&state).clone());
                }
                Ok(())
            }
            PipelineEvent::Failure(failure) => Err(failure.into()),
        }
    }

    async fn observe<T>(
        &mut self,
        operation: impl Future<Output = TestResult<T>>,
    ) -> TestResult<T> {
        tokio::pin!(operation);
        loop {
            tokio::select! {
                result = &mut operation => return result,
                result = self.next_event() => result?,
            }
        }
    }

    fn socket_directory(&self, pid: process::Pid) -> TestResult<PathBuf> {
        Ok(serde_json::from_slice(&fs::read(
            self.records.join(format!("{pid}.json")),
        )?)?)
    }

    fn launch_count(&self) -> TestResult<usize> {
        Ok(fs::read_dir(&self.records)?
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "json")
                    && entry
                        .path()
                        .file_stem()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.parse::<u32>().is_ok())
            })
            .count())
    }

    fn require_removed_before_spawn(&self, paths: &[PathBuf]) -> TestResult {
        fs::write(
            self.records.join("must-be-removed.json"),
            serde_json::to_vec(paths)?,
        )?;
        Ok(())
    }
}

fn is_running(status: &PipelineStatusSnapshot, etag: TenonDocumentEtag) -> bool {
    status.document_etag == etag.strong_value()
        && status
            .plugin_instances
            .iter()
            .all(|instance| instance.state() == PluginInstanceState::Running)
}

async fn with_fixture(check: impl AsyncFnOnce(&mut Fixture) -> TestResult) -> TestResult {
    let root = tempfile::tempdir()?;
    let runtime = tempfile::Builder::new()
        .prefix(".tenon-runner-")
        .tempdir_in(root.path())?;
    let _initial = load_config(root.path())?;
    let config_path = root.path().join("runner.jsonc");
    let mut config: Value = serde_json::from_slice(&fs::read(&config_path)?)?;
    config["pipeline"]["reconfigureTimeoutMs"] = json!(1000);
    config["pipeline"]["startupTimeoutMs"] = json!(5000);
    config["pipeline"]["retryBackoff"] = json!({"initialDelayMs":1000,"maximumDelayMs":1000});
    fs::write(&config_path, serde_json::to_vec(&config)?)?;
    let config = Arc::new(RunnerConfig::load(&config_path)?);
    let programs = root.path().join("programs");
    fs::create_dir(&programs)?;
    let targets = TargetFixture::new(&programs, &config)?;
    let records = root.path().join("launches");
    fs::create_dir(&records)?;
    let executable = root.path().join("pipeline-child");
    let current_exe = env::current_exe()?;
    fs::write(
        &executable,
        format!(
            r#"#!/bin/sh
[ "$#" = "5" ] || exit 70
[ "$1" = "pipeline" ] || exit 71
[ "$2" = "--control-socket" ] || exit 72
[ "$4" = "--launch-id" ] || exit 73
TENON_TEST_PIPELINE_RECORDS={}
TENON_TEST_PIPELINE_CONTROL_SOCKET="$3"
TENON_TEST_PIPELINE_LAUNCH_ID="$5"
export TENON_TEST_PIPELINE_RECORDS TENON_TEST_PIPELINE_CONTROL_SOCKET TENON_TEST_PIPELINE_LAUNCH_ID
exec {} --exact pipeline::main_loop::tests::instance_flow_pipeline_child --ignored --nocapture
"#,
            shell_quote(records.to_str().ok_or("records path is not UTF-8")?),
            shell_quote(
                current_exe
                    .to_str()
                    .ok_or("test executable path is not UTF-8")?
            )
        ),
    )?;
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))?;
    let server = RunnerControlServer::start(
        runtime.path(),
        process_resources::test_support::unavailable(),
        RunnerDiagnostics::new(),
        test_support::empty()?,
    )?;
    let supervisor = PipelineSupervisor::new(
        config,
        executable,
        runtime.path().to_owned(),
        server.pipeline_launcher(),
        Box::new([]),
        ExecutionExpiry::default(),
    );
    let mut fixture = Fixture {
        supervisor,
        targets,
        statuses: Vec::new(),
        records,
        runtime,
        root,
    };
    let outcome = {
        let mut checking = Box::pin(check(&mut fixture));
        time::timeout(
            TEST_LIMIT,
            poll_fn(|context| {
                match catch_unwind(AssertUnwindSafe(|| checking.as_mut().poll(context))) {
                    Ok(result) => result.map(Ok),
                    Err(panic) => Poll::Ready(Err(panic)),
                }
            }),
        )
        .await
    };
    let cleanup = time::timeout(TEST_LIMIT, fixture.supervisor.shutdown()).await?;
    assert_eq!(cleanup.cleanup, PipelineCleanup::Complete);
    assert!(cleanup.failures.is_empty(), "fixture shutdown failed");
    let cleanup = server.shutdown(ControlServerShutdown::Graceful).await;
    assert!(matches!(
        cleanup.socket_cleanup,
        ControlSocketCleanup::Complete
    ));
    assert!(cleanup.failure.is_none());
    match outcome? {
        Ok(result) => result,
        Err(panic) => resume_unwind(panic),
    }
}

fn document(id: &str, endpoint: &str, behavior: &str) -> Value {
    json!({
        "specVersion": "1", "id": id,
        "pluginInstances": {
            "input": {"programName": "com.example.input", "exactVersion": "1.0.0", "config": {}},
            "archive": {"programName": "com.example.archive", "exactVersion": "1.0.0", "config": {"endpoint": endpoint, "behavior": behavior}}
        },
        "flows": {"forward": {"parallelism": 1, "source": "input", "process": {"script": "local output = registry:getBuilder('com.example.archive@1.0.0'); function main(event) emit(output:build()) end"}, "sinks": ["archive"]}}
    })
}

fn instance_pids(root: &Path) -> TestResult<Vec<process::Pid>> {
    ["input", "archive"]
        .into_iter()
        .map(|id| {
            let raw = fs::read_to_string(instance_directory(root, id)?.join("process.pid"))?;
            process::Pid::from_raw(raw.trim().parse()?)
                .ok_or_else(|| "instance pid is invalid".into())
        })
        .collect()
}

fn process_group(pid: process::Pid) -> TestResult<process::Pid> {
    Ok(process::getpgid(Some(pid))?)
}

fn assert_reaped(pid: process::Pid) -> TestResult {
    assert_eq!(
        process::waitpid(Some(pid), process::WaitOptions::NOHANG).err(),
        Some(Errno::CHILD)
    );
    assert!(process::test_kill_process(pid).is_err());
    Ok(())
}

async fn wait_for_processes_gone(pids: &[process::Pid]) -> TestResult {
    while pids
        .iter()
        .any(|pid| process::test_kill_process(*pid).is_ok())
    {
        time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

async fn wait_for_file(path: PathBuf) -> TestResult {
    while !path.exists() {
        time::sleep(Duration::from_millis(5)).await;
    }
    Ok(())
}

async fn traffic(root: &Path, id: u64) -> TestResult {
    let input = instance_directory(root, "input")?;
    let archive = instance_directory(root, "archive")?;
    let channel_bells = flow_channel_bell_path(root, "forward");
    let mut source = open_writer(
        &input.join("source/submission-0.queue"),
        &loops_bell_path(&input.join(source_directory_name())),
        0,
        &channel_bells,
    )?;
    let mut sink = open_reader(
        &egress_queue_path(&archive, "forward", 0),
        &loops_bell_path(&archive.join(sink_directory_name())),
        0,
        &channel_bells,
    )?;
    assert!(matches!(
        {
            let record = IngressRecord {
                record_id: id,
                payload: Vec::new().into(),
            };
            source.try_write_with(
                record.encoded_len(),
                |destination| record.encode(destination).map_err(std::io::Error::other),
                || {},
            )
        }?,
        WriteOutcome::Committed(_)
    ));
    loop {
        match sink.try_read()? {
            ReadOutcome::Record(record) => {
                let record = EgressRecord::decode(record.payload())?;
                assert!(record.payload.is_empty());
                sink.release(1)?;
                return Ok(());
            }
            ReadOutcome::Empty => time::sleep(Duration::from_millis(5)).await,
        }
    }
}
