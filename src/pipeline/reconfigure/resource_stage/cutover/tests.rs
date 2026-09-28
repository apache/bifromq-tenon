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
use crate::contracts::core::{
    PipelineRevisionPlan, PluginInstanceState as PluginStatusState, PluginInterface,
    pipeline_diagnostic_record,
};
use crate::contracts::source::IngressRecord;
use crate::identifiers::PluginInstanceId;
use crate::payload_contract::PluginInterface as ContractInterface;
use crate::pipeline::diagnostics::test_support::interested_instances;
use crate::pipeline::plugin::test_support::{
    PluginControlServer, TEST_DEADLINE, assert_reaped, controlled_program_command,
    instance_statuses, lifecycle_events, recorded_pid, retry_backoff, wait_for_file,
};
use crate::pipeline::reconfigure::plan::ReconfigurePlan;
use crate::pipeline::reconfigure::plan::tests::{flow, instance, model, program};
use crate::pipeline::reconfigure::runtime_files::{
    egress_queue_path, flow_channel_bell_path, instance_working_directory, loops_bell_path,
};
use crate::pipeline::reconfigure::test_support::environment as fixture_environment;
use crate::pipeline::runtime::StartupControl;
use crate::pipeline::runtime::test_support as runtime_test_support;
use prost::Message as _;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::future::{Future, poll_fn};
use std::io;
use std::os::unix::process::ExitStatusExt as _;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::task::Poll;
use tenon_ipc::queue::contract_test_support::{open_reader, open_writer};
use tenon_ipc::queue::{QueueReader, QueueWriter, ReadOutcome, WriteOutcome};
use tokio::time::timeout;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[tokio::test(flavor = "current_thread")]
async fn cutover_starts_all_interfaces_with_exact_material_and_keeps_data_paused() -> TestResult {
    with_candidate("normal", async |candidate, root, records| {
        assert_eq!(states(candidate).len(), 4);
        wait_for_states(candidate, &BTreeMap::new()).await?;
        assert_eq!(
            runtime_test_support::data_plane_counts(&candidate.staged.runtime),
            (3, 4)
        );
        assert!(runtime_test_support::is_pending(&candidate.staged.runtime));

        let mut pids = BTreeSet::new();
        for (id, endpoint) in [
            ("input", "device"),
            ("dual-a", "first"),
            ("dual-b", "second"),
            ("archive", "disk"),
        ] {
            let directory = instance_directory(root, id)?;
            assert!(pids.insert(recorded_pid(&directory)?.as_raw_nonzero()));
            assert_eq!(
                std::fs::read_to_string(directory.join("starts.received"))?,
                "started\n"
            );
            assert_eq!(lifecycle_events(&directory)?, "attach\nready\n");
            let config: Value = serde_json::from_str(&std::fs::read_to_string(
                directory.join("configs.received"),
            )?)?;
            assert_eq!(config, json!({"endpoint": endpoint, "behavior": "normal"}));
            let expected_program = if id.starts_with("dual-") { "dual" } else { id };
            assert_eq!(
                std::fs::read_to_string(directory.join("program.received"))?,
                root.parent()
                    .ok_or("Pipeline parent is missing")?
                    .join(expected_program)
                    .canonicalize()?
                    .display()
                    .to_string()
            );
            let launch_ids = std::fs::read_to_string(directory.join("launch-ids.received"))?;
            assert_eq!(launch_ids.lines().count(), 1);
        }
        assert_eq!(pids.len(), 4);

        let mut output = BTreeMap::<String, Vec<_>>::new();
        timeout(TEST_DEADLINE, async {
            while output.values().map(Vec::len).sum::<usize>() < 16 {
                let record = records.recv().await.ok_or("Diagnostic stream closed")?;
                if let Some(pipeline_diagnostic_record::Record::Plugin(record)) = record.record
                    && record.text.starts_with("diagnostic-")
                {
                    output
                        .entry(record.plugin_instance_id.clone())
                        .or_default()
                        .push(record);
                }
            }
            Ok::<_, Box<dyn Error>>(())
        })
        .await??;
        assert_eq!(
            output.keys().map(String::as_str).collect::<Vec<_>>(),
            ["archive", "dual-a", "dual-b", "input"]
        );
        let mut incarnations = BTreeSet::new();
        for lines in output.values() {
            assert_eq!(lines.len(), 4);
            let process = lines[0].plugin_process_instance_id;
            assert!(incarnations.insert(process));
            assert!(
                lines
                    .iter()
                    .all(|line| line.plugin_process_instance_id == process)
            );
            assert_eq!(
                lines
                    .iter()
                    .map(|line| line.sequence)
                    .collect::<BTreeSet<_>>()
                    .len(),
                4
            );
            assert_eq!(
                lines
                    .iter()
                    .map(|line| line.stream)
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([0, 1])
            );
        }

        let mut submission = source_writer(root, "input", "input-to-dual", 0)?;
        let receipt = match submission.try_write(
            &IngressRecord {
                record_id: 7,
                payload: Vec::new().into(),
            }
            .encode_to_vec(),
        )? {
            WriteOutcome::Committed(receipt) => receipt,
            WriteOutcome::Full => return Err("Fresh Submission Queue is full".into()),
        };
        assert!(!submission.is_released(&receipt)?);
        for (id, flow) in [
            ("dual-a", "input-to-dual"),
            ("dual-b", "dual-to-dual"),
            ("archive", "dual-to-archive"),
        ] {
            assert!(matches!(
                sink_reader(root, id, flow, 0)?.try_read()?,
                ReadOutcome::Empty
            ));
        }
        assert!(matches!(
            source_completion(root, "input", "input-to-dual", 0)?.try_read()?,
            ReadOutcome::Empty
        ));
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn delayed_ready_does_not_block_other_instances_and_a_canceled_wait_can_resume() -> TestResult
{
    with_candidate("delay-ready", async |candidate, root, _| {
        wait_for_states(
            candidate,
            &BTreeMap::from([("dual-b", PluginStatusState::Starting)]),
        )
        .await?;
        let directory = instance_directory(root, "dual-b")?;
        wait_for_file(&directory.join("lifecycle.received")).await?;
        assert_eq!(lifecycle_events(&directory)?, "attach\n");
        // The next event has no producer until this test releases the real child.
        let mut waiting = Box::pin(candidate.plugins.instances.next_event());
        poll_fn(|context| {
            assert!(waiting.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(waiting);
        assert!(root.exists());
        std::fs::write(directory.join("allow-ready"), [])?;
        wait_for_states(candidate, &BTreeMap::new()).await?;
        assert_eq!(
            std::fs::read_to_string(directory.join("starts.received"))?,
            "started\n"
        );
        assert_eq!(lifecycle_events(&directory)?, "attach\nready\n");
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn exit_before_ready_fails_only_that_instance_without_discarding_the_candidate() -> TestResult
{
    with_candidate("exit-before-ready", async |candidate, root, _| {
        wait_for_states(
            candidate,
            &BTreeMap::from([("dual-b", PluginStatusState::StartFailed)]),
        )
        .await?;
        let directory = instance_directory(root, "dual-b")?;
        assert_reaped(recorded_pid(&directory)?)?;
        assert_eq!(lifecycle_events(&directory)?, "attach\n");
        assert!(runtime_test_support::is_pending(&candidate.staged.runtime));
        assert!(root.exists());
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn a_missing_program_fails_its_instances_but_other_programs_still_reach_ready() -> TestResult
{
    with_candidate("missing-dual-program", async |candidate, _, _| {
        wait_for_states(
            candidate,
            &BTreeMap::from([
                ("dual-a", PluginStatusState::StartFailed),
                ("dual-b", PluginStatusState::StartFailed),
            ]),
        )
        .await?;
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn terminal_cleanup_racing_with_cancellation_retains_stage_until_explicit_drop() -> TestResult
{
    with_candidate("delay-ready", async |candidate, root, _| {
        wait_for_states(
            candidate,
            &BTreeMap::from([("dual-b", PluginStatusState::Starting)]),
        )
        .await?;
        // Starting is a parent-side state, not proof that the child wrote its PID.
        wait_for_file(&instance_directory(root, "dual-b")?.join("attach.received")).await?;
        let pids = ["input", "dual-a", "dual-b", "archive"]
            .map(|id| recorded_pid(&instance_directory(root, id)?))
            .into_iter()
            .collect::<Result<Vec<_>, io::Error>>()?;
        let mut cleanup = Box::pin(candidate.force_stop());
        let first_poll = poll_fn(|context| Poll::Ready(cleanup.as_mut().poll(context))).await;
        drop(cleanup);
        // Reaping can already have completed on a fast OS. Both cancellation
        // before completion and completion before cancellation retain Stage.
        if let Poll::Ready(result) = first_poll {
            result?;
        }
        assert!(root.exists());
        candidate.force_stop().await?;
        assert!(candidate.plugins.instances.is_empty());
        for pid in pids {
            assert_reaped(pid)?;
        }
        assert!(root.exists());
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn all_failed_starts_release_collection_ownership_without_waiting_for_children() -> TestResult
{
    with_candidate("missing-all-programs", async |candidate, root, _| {
        assert_eq!(
            states(candidate).values().copied().collect::<Vec<_>>(),
            vec![PluginStatusState::StartFailed; 4]
        );
        let mut cleanup = Box::pin(candidate.force_stop());
        let result = poll_fn(|context| Poll::Ready(cleanup.as_mut().poll(context))).await;
        assert!(
            matches!(result, Poll::Ready(Ok(()))),
            "Failed spawns have no child to await: {result:?}"
        );
        drop(cleanup);
        assert!(candidate.plugins.instances.is_empty());
        assert!(root.exists());
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn abandoning_terminal_ownership_aborts_before_deleting_queue_files() -> TestResult {
    let parent = tempfile::tempdir()?;
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "pipeline::reconfigure::resource_stage::cutover::tests::abandoned_candidate_child",
            "--ignored",
            "--nocapture",
        ])
        .env("TENON_TEST_ABANDONED_CANDIDATE_ROOT", parent.path())
        .kill_on_drop(true);
    let output = timeout(TEST_DEADLINE, command.output()).await??;
    assert_eq!(
        output.status.signal(),
        Some(libc::SIGABRT),
        "Child must abort on an uncompleted owner handoff"
    );
    assert!(
        instance_directory(&parent.path().join("pipeline"), "input")?
            .join("source/submission-0.queue")
            .exists()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "child process entry point invoked by the parent cutover test"]
async fn abandoned_candidate_child() -> TestResult {
    let Some(parent) = std::env::var_os("TENON_TEST_ABANDONED_CANDIDATE_ROOT") else {
        return Ok(());
    };
    let parent = PathBuf::from(parent);
    let root = parent.join("pipeline");
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let target = model(revision(&parent, "missing-all-programs")?)?;
    let (diagnostics, _records) =
        interested_instances(target.document().plugin_instances().keys().cloned(), 1);
    let environment = fixture_environment(&root)?;
    let staged = ReconfigurePlan::derive(None, target, environment.available_cpu_count())?
        .compile()?
        .stage(
            &environment,
            diagnostics.clone(),
            Arc::new(StartupControl::new()),
            None,
            None,
        )?;
    let mut candidate = staged.begin_cutover(retry_backoff());
    candidate.launch_instances(|_| true, &launcher, retry_backoff(), &diagnostics);
    // No Plugin child can exist here; the isolated abort cannot orphan children.
    server.shutdown().await?;
    drop(candidate);
    Err("Dropping an unterminated candidate unexpectedly returned".into())
}

async fn with_candidate(
    behavior: &str,
    check: impl AsyncFnOnce(
        &mut StartedResourceChanges,
        &Path,
        &mut tokio::sync::mpsc::Receiver<crate::contracts::core::PipelineDiagnosticRecord>,
    ) -> TestResult,
) -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let target = model(revision(parent.path(), behavior)?)?;
    let (diagnostics, mut records) =
        interested_instances(target.document().plugin_instances().keys().cloned(), 128);
    let environment = fixture_environment(&root)?;
    let staged = ReconfigurePlan::derive(None, target, environment.available_cpu_count())?
        .compile()?
        .stage(
            &environment,
            diagnostics.clone(),
            Arc::new(StartupControl::new()),
            None,
            None,
        )?;
    let mut candidate = staged.begin_cutover(retry_backoff());
    candidate.launch_instances(|_| true, &launcher, retry_backoff(), &diagnostics);
    // Preserve the async owner through assertion panics as well as returned errors.
    let mut checking = Box::pin(timeout(
        TEST_DEADLINE,
        check(&mut candidate, &root, &mut records),
    ));
    let outcome = poll_fn(|context| {
        match catch_unwind(AssertUnwindSafe(|| checking.as_mut().poll(context))) {
            Ok(result) => result.map(Ok),
            Err(panic) => Poll::Ready(Err(panic)),
        }
    })
    .await;
    drop(checking);
    let pids = (|| -> io::Result<Vec<_>> {
        let mut pids = Vec::new();
        for id in ["input", "dual-a", "dual-b", "archive"] {
            let directory = instance_directory(&root, id)?;
            if directory.join("process.pid").try_exists()? {
                pids.push(recorded_pid(&directory)?);
            }
        }
        Ok(pids)
    })();
    timeout(TEST_DEADLINE, candidate.force_stop()).await??;
    for pid in pids? {
        assert_reaped(pid)?;
    }
    assert!(root.exists());
    drop(candidate);
    assert!(!root.exists());
    // A new candidate uses the same externally owned server after cleanup.
    let target = model(revision(parent.path(), "normal")?)?;
    let staged = ReconfigurePlan::derive(None, target, environment.available_cpu_count())?
        .compile()?
        .stage(
            &environment,
            diagnostics.clone(),
            Arc::new(StartupControl::new()),
            None,
            None,
        )?;
    let mut replacement = staged.begin_cutover(retry_backoff());
    replacement.launch_instances(|_| true, &launcher, retry_backoff(), &diagnostics);
    let ready = wait_for_states(&mut replacement, &BTreeMap::new()).await;
    timeout(TEST_DEADLINE, replacement.force_stop()).await??;
    drop(replacement);
    server.shutdown().await?;
    ready?;
    match outcome {
        Ok(result) => result?,
        Err(panic) => resume_unwind(panic),
    }
}

async fn wait_for_states(
    candidate: &mut StartedResourceChanges,
    exceptions: &BTreeMap<&str, PluginStatusState>,
) -> TestResult {
    timeout(TEST_DEADLINE, async {
        loop {
            let states = states(candidate);
            if states.len() == 4
                && states.iter().all(|(id, state)| {
                    *state
                        == exceptions
                            .get(id.as_str())
                            .copied()
                            .unwrap_or(PluginStatusState::Running)
                })
            {
                return Ok(());
            }
            candidate.plugins.instances.next_event().await?;
        }
    })
    .await?
}

fn states(candidate: &StartedResourceChanges) -> BTreeMap<String, PluginStatusState> {
    instance_statuses(&candidate.plugins.instances)
}

pub(in crate::pipeline::reconfigure::resource_stage) fn instance_directory(
    root: &Path,
    id: &str,
) -> io::Result<PathBuf> {
    let id = PluginInstanceId::try_from(id.to_owned()).map_err(io::Error::other)?;
    Ok(instance_working_directory(&root.join("instances"), &id))
}

/// The own-loop doorbell every Submission Queue of a Source publishes: the
/// Source's single Submission loop parks on this ordinal in `source/loops.bells`.
const SUBMISSION_LOOP_SLOT: u32 = 0;
/// The own-loop doorbell every Completion Queue of a Source publishes: the
/// Source's single Completion loop parks on this ordinal in `source/loops.bells`.
const COMPLETION_LOOP_SLOT: u32 = 1;

/// Opens the Source side's writer of one Submission Queue.
///
/// A test that plays a Source process opens the same two files the real
/// Instance does: the `source/loops.bells` region its loop parks on, and the
/// Channel region of the Flow its commit rings. The Source owns one own-loop
/// doorbell per loop, not per Channel, so every Channel's Submission publishes
/// the one Submission slot.
pub(in crate::pipeline::reconfigure::resource_stage) fn source_writer(
    root: &Path,
    id: &str,
    flow: &str,
    channel: u32,
) -> TestResult<QueueWriter> {
    let source = instance_directory(root, id)?.join("source");
    Ok(open_writer(
        &source.join(format!("submission-{channel}.queue")),
        &loops_bell_path(&source),
        SUBMISSION_LOOP_SLOT,
        &flow_channel_bell_path(root, flow),
    )?)
}

/// Opens the Source side's reader of one Completion Queue, the Completion loop's doorbell.
pub(in crate::pipeline::reconfigure::resource_stage) fn source_completion(
    root: &Path,
    id: &str,
    flow: &str,
    channel: u32,
) -> TestResult<QueueReader> {
    let source = instance_directory(root, id)?.join("source");
    Ok(open_reader(
        &source.join(format!("completion-{channel}.queue")),
        &loops_bell_path(&source),
        COMPLETION_LOOP_SLOT,
        &flow_channel_bell_path(root, flow),
    )?)
}

/// Opens the Sink side's reader of one Egress Queue.
///
/// The Sink reads every one of its Egress Queues from a single loop, so all of
/// them publish the same own-loop doorbell: slot 0 of the `sink/loops.bells`
/// region the Instance was launched with.
pub(in crate::pipeline::reconfigure::resource_stage) fn sink_reader(
    root: &Path,
    id: &str,
    flow: &str,
    channel: u32,
) -> TestResult<QueueReader> {
    let directory = instance_directory(root, id)?;
    Ok(open_reader(
        &egress_queue_path(&directory, flow, channel),
        &loops_bell_path(&directory.join("sink")),
        0,
        &flow_channel_bell_path(root, flow),
    )?)
}

pub(in crate::pipeline) fn revision(
    parent: &Path,
    behavior: &str,
) -> TestResult<PipelineRevisionPlan> {
    let mut document = json!({
        "specVersion": "1", "id": "initial-cutover", "name": "Initial cutover",
        "pluginInstances": {
            "input": instance("com.example.input", "device"),
            "dual-a": instance("com.example.dual", "first"),
            "dual-b": instance("com.example.dual", "second"),
            "archive": instance("com.example.archive", "disk")
        },
        "flows": {
            "input-to-dual": flow("input", 2, &["dual-a"]),
            "dual-to-dual": flow("dual-a", 1, &["dual-b"]),
            "dual-to-archive": flow("dual-b", 1, &["archive"])
        }
    });
    for id in ["input", "dual-a", "dual-b", "archive"] {
        document["pluginInstances"][id]["config"]["behavior"] =
            json!(if id == "dual-b" && behavior != "missing-dual-program" {
                behavior
            } else {
                "normal"
            });
    }
    document["flows"]["dual-to-archive"]["process"]["script"] = json!(
        "local builder = registry:getBuilder(\"com.example.archive@1.0.0\"); setTimeout(0); function main(event) emit(builder:build()) end"
    );
    let mut programs = Vec::new();
    for (name, wire, contract) in [
        ("input", PluginInterface::Source, ContractInterface::Source),
        (
            "dual",
            PluginInterface::SourceAndSink,
            ContractInterface::SourceAndSink,
        ),
        ("archive", PluginInterface::Sink, ContractInterface::Sink),
    ] {
        let directory = parent.join(name);
        std::fs::create_dir_all(&directory)?;
        let mut material = program(&format!("com.example.{name}"), "1.0.0", wire)?;
        material.program_directory = directory.display().to_string();
        material.command = if (name == "dual" && behavior == "missing-dual-program")
            || behavior == "missing-all-programs"
        {
            vec![directory.join("missing-command").display().to_string()]
        } else {
            controlled_program_command(contract)?
        };
        programs.push(material);
    }
    Ok(PipelineRevisionPlan {
        document_etag: "initial-cutover".into(),
        tenon_document_json: serde_json::to_string(&document)?,
        plugin_programs: programs,
    })
}
