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

//! A real Controller must escape an unreleased Sink without inventing Completion.

use super::*;
use crate::contracts::core::{LuaLimits, PipelineBootstrap, PipelineEnvironment, RetryBackoff};
use crate::pipeline::diagnostics::test_support::interested_flow_channel;
use crate::pipeline::plugin::test_support::control_socket_path;
use crate::pipeline::reconfigure::start_controller;
use crate::pipeline::reconfigure::test_support::AVAILABLE_CPU_COUNT;
use crate::runner::test_support::run_pipeline_child_until_exit;
use crate::time::Deadline;
use std::os::unix::process::ExitStatusExt as _;
use std::time::Duration;

const CHILD_ROOT: &str = "TENON_TEST_RECONFIGURATION_DEADLINE_ROOT";
const MAX_PENDING_RECORDS: u64 = 8;

#[tokio::test(flavor = "current_thread")]
async fn unreleased_sink_expires_without_fabricating_release_or_completion() -> TestResult {
    for scenario in ["unreleased-sink", "configuration-handoff", "record-limits"] {
        let (parent, status) = run_child(scenario).await?;
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        let root = parent.path().join("pipeline");
        let mut sink = sink_reader(&root, "archive", "dual-to-archive", 0)?;
        let old = EgressRecord::decode(read_record(&mut sink).await?.as_slice())?;
        assert_eq!(
            TestPayload::decode(old.payload.as_slice())?.value,
            "stable:1"
        );
        let mut completion = source_completion(&root, "dual-b", "dual-to-archive", 0)?;
        assert!(matches!(completion.try_read()?, ReadOutcome::Empty));
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn candidate_initialization_expires_despite_repeated_latest_applied_revision() -> TestResult {
    let (_, status) = run_child("candidate-initialization").await?;
    assert_eq!(status.signal(), Some(libc::SIGKILL));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_published_reconfiguration_disarms_its_deadline() -> TestResult {
    let (parent, status) = run_child("healthy").await?;
    assert!(status.success());
    assert!(!parent.path().join("pipeline").exists());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn bootstrap_is_not_subject_to_the_reconfiguration_budget() -> TestResult {
    let (parent, status) = run_child("bootstrap").await?;
    assert!(status.success());
    assert!(!parent.path().join("pipeline").exists());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn publication_at_the_deadline_cannot_install_the_target() -> TestResult {
    let (_, status) = run_child("publish-expired").await?;
    assert_eq!(status.signal(), Some(libc::SIGKILL));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_full_completion_queue_cannot_hold_reconfiguration_forever() -> TestResult {
    let (parent, status) = run_child("full-completion").await?;
    assert_eq!(status.signal(), Some(libc::SIGKILL));
    let mut completion =
        source_completion(&parent.path().join("pipeline"), "input", "input-to-dual", 0)?;
    for offset in 0..=MAX_PENDING_RECORDS {
        let record = IngressCompletion::decode(read_record(&mut completion).await?.as_slice())?;
        assert_eq!(record.record_id, u64::MAX - offset);
    }
    assert!(matches!(completion.try_read()?, ReadOutcome::Empty));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn blocked_business_shutdown_expires_while_retained_plugin_retry_progresses() -> TestResult {
    let (_, status) = run_child("business-shutdown").await?;
    assert_eq!(status.signal(), Some(libc::SIGKILL));
    Ok(())
}

async fn run_child(scenario: &str) -> TestResult<(tempfile::TempDir, std::process::ExitStatus)> {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command.args(["--exact", "pipeline::reconfigure::resource_stage::activation::tests::additive_apply::deadline::deadline_child", "--ignored", "--nocapture"])
        .env(CHILD_ROOT, parent.path())
        .env("TENON_TEST_RECONFIGURATION_SCENARIO", scenario);
    let outcome =
        run_pipeline_child_until_exit(command, &parent.path().join("operation-observed")).await;
    // SIGKILL cannot run the server's TempDir destructor. This test owns the
    // exact socket recorded by its server, never a global prefix cleanup.
    let socket_record = parent.path().join("control-socket");
    if socket_record.exists() {
        let socket = PathBuf::from(std::fs::read_to_string(socket_record)?);
        let directory = socket.parent().ok_or("Socket parent is missing")?;
        if directory.exists() {
            std::fs::remove_dir_all(directory)?;
        }
    }
    let status = outcome?;
    for id in ["input", "dual-a", "dual-b", "archive", "backup"] {
        let directory = instance_directory(&root, id)?;
        if !directory.join("process.pid").exists() {
            continue;
        }
        let pid = recorded_pid(&directory)?;
        timeout(TEST_DEADLINE, async {
            while rustix::process::test_kill_process(pid).is_ok() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
    }
    assert!(!parent.path().join("published-blocked-target").exists());
    Ok((parent, status))
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "child process entry point invoked by the parent deadline test"]
async fn deadline_child() -> TestResult {
    let Some(parent) = std::env::var_os(CHILD_ROOT) else {
        return Ok(());
    };
    let parent = Path::new(&parent);
    let root = parent.join("pipeline");
    let scenario = std::env::var("TENON_TEST_RECONFIGURATION_SCENARIO")?;
    let server = PluginControlServer::start()?;
    std::fs::write(
        parent.join("control-socket"),
        control_socket_path(&server).as_os_str().as_encoded_bytes(),
    )?;
    let mut first = revision(parent, "normal")?;
    let mut document: Value = serde_json::from_str(&first.tenon_document_json)?;
    document["flows"]["dual-to-archive"]["process"]["script"] = json!(COUNTING_LUA);
    if scenario == "full-completion" {
        document["flows"]["input-to-dual"]["process"]["script"] = json!(COMPLETING_LUA);
    }
    if scenario == "business-shutdown" {
        document["pluginInstances"]["backup"] = instance("com.example.archive", "retained");
        document["flows"]["input-to-dual"]["sinks"] = json!(["dual-a", "backup"]);
        document["pluginInstances"]["archive"]["config"]["behavior"] = json!("delay-shutdown");
    }
    if scenario == "configuration-handoff" {
        document["pluginInstances"]["archive"]["config"]["behavior"] = json!("exit-before-ready");
    }
    for flow in document["flows"]
        .as_object_mut()
        .ok_or("Fixture Flows are missing")?
        .values_mut()
    {
        flow["maxPendingRecords"] = json!(MAX_PENDING_RECORDS);
        flow["maxRecordBytes"] = json!(1024);
    }
    first.tenon_document_json = serde_json::to_string(&document)?;
    let bootstrap = PipelineBootstrap {
        revision_plan: Some(first.clone()),
        environment: Some(PipelineEnvironment {
            metrics_node_id: None,
            pipeline_working_directory: root.to_str().ok_or("Fixture root is not UTF-8")?.into(),
            lua_limits: Some(LuaLimits {
                cpu_time_limit_ms: 10_000,
                memory_limit_bytes: 16_777_216,
            }),
            retry_backoff: Some(RetryBackoff {
                initial_delay_ms: 100,
                maximum_delay_ms: 1000,
            }),
            reconfigure_timeout_ms: if scenario == "bootstrap" { 1 } else { 500 },
            available_cpu_count: AVAILABLE_CPU_COUNT.get() as u32,
        }),
    };
    let (diagnostics, mut records) =
        interested_flow_channel(&FlowId::try_from(String::from("dual-to-archive"))?, 0);
    let target = PipelineRevision::from_runner(
        bootstrap
            .revision_plan
            .ok_or("Fixture revision is missing")?,
    );
    let environment = bootstrap
        .environment
        .ok_or("Fixture environment is missing")?;
    if scenario == "publish-expired" {
        let mut reconfigurer = Reconfigurer::new(environment, diagnostics, server.launcher());
        reconfigurer.apply(target, None).await?;
        wait_for_states(current(&mut reconfigurer)?, &[]).await?;
        let deadline = Deadline::start(Duration::from_millis(500));
        tokio::time::pause();
        tokio::time::advance(Duration::from_millis(500)).await;
        std::fs::write(parent.join("operation-observed"), [])?;
        first.document_etag = "next-target".into();
        // No outer timer participates: the actual Publish boundary must
        // itself reject expiry, including a synchronous completion race.
        reconfigurer.apply(model(first)?, Some(deadline)).await?;
        std::fs::write(parent.join("published-blocked-target"), [])?;
        return Err("Publish accepted an expired target".into());
    }
    let mut controller =
        start_controller(target, environment, diagnostics, server.launcher(), None);
    loop {
        let status = controller.next_status().await?;
        if status.plugin_instances.iter().all(|instance| {
            instance.state()
                == if scenario == "configuration-handoff" && instance.id == "archive" {
                    PluginInstanceState::StartFailed
                } else {
                    PluginInstanceState::Running
                }
        }) {
            break;
        }
    }
    let mut source = source_writer(&root, "dual-b", "dual-to-archive", 0)?;
    let mut sink = sink_reader(&root, "archive", "dual-to-archive", 0)?;
    submit(&mut source, 1)?;
    read_record(&mut sink).await?;
    if !matches!(
        scenario.as_str(),
        "unreleased-sink" | "configuration-handoff" | "record-limits"
    ) {
        sink.release(1)?;
    }
    if scenario == "bootstrap" {
        std::fs::write(parent.join("operation-observed"), [])?;
        controller.force_shutdown_and_wait().await?;
        server.shutdown().await?;
        return Ok(());
    }
    let full_completion = if scenario == "full-completion" {
        Some(fill_completion(&root, MAX_PENDING_RECORDS).await?)
    } else {
        None
    };
    let mut next = first.clone();
    match scenario.as_str() {
        "record-limits" => {
            document["flows"]["dual-to-archive"]["maxPendingRecords"] = json!(1);
            document["flows"]["dual-to-archive"]["maxRecordBytes"] = json!(2048);
        }
        "configuration-handoff" => {
            document["pluginInstances"]["archive"]["config"]["behavior"] = json!("normal");
            document["pluginInstances"]["dual-b"]["config"]["endpoint"] = json!("after");
        }
        "candidate-initialization" => {
            document["flows"]["dual-to-archive"]["process"]["script"] =
                json!("print('candidate-initialized'); while true do end")
        }
        "business-shutdown" => {
            document["pluginInstances"]["archive"]["config"]["endpoint"] = json!("after")
        }
        "full-completion" => {
            document["flows"]["input-to-dual"]["process"]["script"] =
                json!("function main(event) emit() end")
        }
        _ => document["flows"]["dual-to-archive"]["process"]["script"] = json!(ADDED_LUA),
    }
    next.tenon_document_json = serde_json::to_string(&document)?;
    next.document_etag = "next-target".into();
    controller.submit_latest(next);
    if scenario == "configuration-handoff" {
        wait_for_file(&instance_directory(&root, "archive")?.join("ready.received")).await?;
        wait_for_file(&instance_directory(&root, "dual-b")?.join("source-quiesced.received"))
            .await?;
    }
    if scenario == "candidate-initialization" {
        records
            .recv()
            .await
            .ok_or("Candidate diagnostic is missing")?;
    }
    if full_completion.is_some() {
        wait_for_channel_park(&root, "input", "input-to-dual", 0).await?;
    }
    if scenario == "business-shutdown" {
        wait_for_file(&instance_directory(&root, "archive")?.join("shutdown.received")).await?;
        let input = instance_directory(&root, "backup")?;
        rustix::process::kill_process(recorded_pid(&input)?, rustix::process::Signal::KILL)?;
        super::retained_lifecycle::wait_for_ready_count(&input, 2).await?;
    }
    std::fs::write(parent.join("operation-observed"), [])?;
    if scenario == "healthy" {
        while controller.next_status().await?.document_etag != "next-target" {}
        // Deliberately exceed the completed operation's budget, then prove
        // that the same new Lua still handles real Queue traffic.
        tokio::time::sleep(Duration::from_millis(600)).await;
        submit(&mut source, 2)?;
        assert_eq!(read_value(&mut sink).await?, "added");
        controller.force_shutdown_and_wait().await?;
        server.shutdown().await?;
        return Ok(());
    }
    loop {
        tokio::select! {
            status = controller.next_status() => {
                if status?.document_etag == "next-target" {
                    std::fs::write(parent.join("published-blocked-target"), [])?;
                    return Err("Blocked target was published".into());
                }
            }
            () = tokio::time::sleep(Duration::from_millis(10)), if matches!(scenario.as_str(), "candidate-initialization" | "configuration-handoff") => controller.submit_latest(first.clone()),
        }
    }
}
