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

use super::super::cutover::tests::{instance_directory, revision};
use super::*;
use crate::contracts::core::PluginInstanceState;
use crate::contracts::sink::EgressRecord;
use crate::contracts::source::{IngressCompletion, IngressCompletionStatus, IngressRecord};
use crate::identifiers::FlowId;
use crate::metrics::capture_test_support::Capture;
use crate::pipeline::channel::metrics::FlowMetrics;
use crate::pipeline::diagnostics::test_support::publisher;
use crate::pipeline::plugin::test_support::{
    PluginControlServer, TEST_DEADLINE, assert_reaped, lifecycle_events, recorded_pid,
    retry_backoff, wait_for_file,
};
use crate::pipeline::reconfigure::ReconfigureShutdown;
use crate::pipeline::reconfigure::plan::ReconfigurePlan;
use crate::pipeline::reconfigure::plan::tests::model;
use crate::pipeline::reconfigure::runtime_files::{
    egress_queue_path, flow_channel_bell_path, loops_bell_path,
};
use crate::pipeline::reconfigure::test_support::environment as fixture_environment;
use crate::pipeline::runtime::StartupControl;
use prost::Message as _;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::error::Error;
use std::future::{Future as _, poll_fn};
use std::os::unix::fs::{FileExt as _, MetadataExt as _};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::task::Poll;
use tenon_ipc::queue::contract_test_support::{open_reader, open_writer};
use tenon_ipc::queue::{
    QueueReader, QueueWaiter, QueueWriter, ReadOutcome, TEST_RELEASE_OFFSET, WriteOutcome,
    queue_waiter_is_armed,
};
use tokio::time::timeout;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type Reconfigurer = PipelineReconfigurer;

const DUAL_EMIT_LUA: &str = "local b = registry:getBuilder('com.example.dual@1.0.0'); function main(event) emit(b:build()) end";

fn corrupt_egress_commit(queue: &std::fs::File) -> TestResult {
    // Queue v1 stores the writer position at byte 64. The real reader's next
    // release wakes the writer, which must reject this unaligned position.
    queue.write_all_at(&1_u64.to_le_bytes(), 64)?;
    Ok(())
}

async fn hold_dual_output(root: &Path) -> TestResult<QueueReader> {
    let mut source = source_writer(root, "dual-a", "dual-to-dual", 0)?;
    let mut sink = sink_reader(root, "dual-b", "dual-to-dual", 0)?;
    submit(&mut source, 900)?;
    read_record(&mut sink).await?;
    Ok(sink)
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
/// Instance does: the `source/loops.bells` region the loop parks on, and the
/// Channel region of the Flow its commit rings. The Source owns one own-loop
/// doorbell per loop, not per Channel, so every Channel's Submission publishes
/// the one Submission slot.
fn source_writer(root: &Path, id: &str, flow: &str, channel: u32) -> TestResult<QueueWriter> {
    let source = instance_directory(root, id)?.join("source");
    Ok(open_writer(
        &source.join(format!("submission-{channel}.queue")),
        &loops_bell_path(&source),
        SUBMISSION_LOOP_SLOT,
        &flow_channel_bell_path(root, flow),
    )?)
}

/// Opens the Source side's reader of one Completion Queue, the Completion loop's doorbell.
fn source_completion(root: &Path, id: &str, flow: &str, channel: u32) -> TestResult<QueueReader> {
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
fn sink_reader(root: &Path, id: &str, flow: &str, channel: u32) -> TestResult<QueueReader> {
    let directory = instance_directory(root, id)?;
    Ok(open_reader(
        &egress_queue_path(&directory, flow, channel),
        &loops_bell_path(&directory.join("sink")),
        0,
        &flow_channel_bell_path(root, flow),
    )?)
}

/// Gives one fixture run the Channel metric publication the Runner supplies.
///
/// Without it a fixture Channel has no observation context and publishes no
/// `tenon.flow.waiting` sample, so a test could not name the wait it reached.
fn publish_channel_metrics(reconfigurer: &mut Reconfigurer, captured: &Capture) {
    reconfigurer.metrics = Some(FlowMetrics::new(&captured.meter()));
}

/// Waits until one Flow Channel is blocked on the Completion Queue's capacity.
///
/// `tenon.flow.waiting` publishes the shortage a Channel is inside; the value
/// `3` is `WaitKind::CompletionCapacity`. A parked doorbell slot cannot say
/// this: one Channel owns exactly one slot for every wait it runs, so the slot
/// only reports that the loop is parked, never why.
async fn wait_for_completion_capacity(captured: &Capture, flow: &str, channel: u32) -> TestResult {
    const COMPLETION_CAPACITY: f64 = 3.0;
    let channel = channel.to_string();
    let labels = [
        ("tenon.flow.id", flow),
        ("tenon.channel.index", channel.as_str()),
    ];
    while captured.collect()?.number("tenon.flow.waiting", &labels) != Some(COMPLETION_CAPACITY) {
        tokio::task::yield_now().await;
    }
    Ok(())
}

/// Waits until the Channel that owns the other end of one Source's Completion
/// Queue is parked on its own doorbell.
///
/// A doorbell records no reason for a wait, so this proves only that the Channel
/// loop is not running. A test that must name the wait it reached reads
/// `tenon.flow.waiting` through [`wait_for_completion_capacity`] instead, and a
/// test that must name a handoff phase reads the event file that phase writes.
async fn wait_for_channel_park(root: &Path, id: &str, flow: &str, channel: u32) -> TestResult {
    let source = instance_directory(root, id)?;
    let path = source.join(format!("source/completion-{channel}.queue"));
    let region = flow_channel_bell_path(root, flow);
    while !queue_waiter_is_armed(&path, &region, QueueWaiter::Writer)? {
        tokio::task::yield_now().await;
    }
    Ok(())
}

mod additive_apply;
mod initial_apply;
mod runtime_status;

#[tokio::test(flavor = "current_thread")]
async fn publication_vectors_accept_starting_and_failed_instances_without_polling_ready()
-> TestResult {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Vector {
        name: String,
        behavior: String,
        expected_instance_states: BTreeMap<String, String>,
    }
    let vectors: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/contracts/plugin/test-fixtures/process_protocol_test_vectors.json"
    )))?;
    let cases: Vec<Vector> = serde_json::from_value(vectors["initialActivation"].clone())?;
    assert_eq!(cases.len(), 3);
    for case in cases {
        with_activation(
            &case.behavior,
            &[],
            async |reconfigurer, candidate, root| {
                assert!(reconfigurer.current.is_none(), "Activate is not Publish");
                let status = publish(reconfigurer, candidate)?;
                assert_eq!(status.document_etag, "initial-cutover");
                assert_eq!(
                    states(&status),
                    case.expected_instance_states,
                    "{}",
                    case.name
                );
                for instance in &status.plugin_instances {
                    assert_eq!(
                        instance.last_error.is_some(),
                        instance.state() == PluginInstanceState::StartFailed
                    );
                }
                assert_eq!(
                    PipelineStatusSnapshot::decode(status.encode_to_vec().as_slice())?,
                    status
                );
                assert_eq!(current(reconfigurer)?.status_snapshot(), status);
                assert!(root.exists());
                Ok(())
            },
        )
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_timer_outputs_before_ready_and_later_ready_preserves_the_applied_etag() -> TestResult {
    with_activation("delay-ready", &[], async |reconfigurer, candidate, root| {
        // No Instance event has been polled, so config delivery and Ready are still pending.
        let mut archive = sink_reader(root, "archive", "dual-to-archive", 0)?;
        assert!(
            EgressRecord::decode(read_record(&mut archive).await?.as_slice())?
                .payload
                .is_empty()
        );
        assert!(reconfigurer.current.is_none());
        let published = publish(reconfigurer, candidate)?;
        assert!(
            published
                .plugin_instances
                .iter()
                .all(|instance| instance.state() == PluginInstanceState::Starting)
        );

        wait_for_states(
            current(reconfigurer)?,
            &[("dual-b", PluginInstanceState::Starting)],
        )
        .await?;
        let delayed = instance_directory(root, "dual-b")?;
        wait_for_file(&delayed.join("lifecycle.received")).await?;
        assert_eq!(lifecycle_events(&delayed)?, "attach\n");
        let pid = recorded_pid(&delayed)?;
        let mut waiting = Box::pin(current(reconfigurer)?.plugins.instances.next_event());
        poll_fn(|context| {
            assert!(waiting.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(waiting);
        assert!(root.exists());
        std::fs::write(delayed.join("allow-ready"), [])?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        assert_eq!(
            current(reconfigurer)?.status_snapshot().document_etag,
            published.document_etag
        );
        assert_eq!(recorded_pid(&delayed)?, pid);
        assert_eq!(
            std::fs::read_to_string(delayed.join("starts.received"))?,
            "started\n"
        );
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn exit_before_ready_changes_only_that_published_instance() -> TestResult {
    with_activation(
        "exit-before-ready",
        &[],
        async |reconfigurer, candidate, _| {
            let published = publish(reconfigurer, candidate)?;
            wait_for_states(
                current(reconfigurer)?,
                &[("dual-b", PluginInstanceState::StartFailed)],
            )
            .await?;
            assert_eq!(
                current(reconfigurer)?.status_snapshot().document_etag,
                published.document_etag
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn exit_after_ready_reports_backoff_without_changing_the_applied_etag() -> TestResult {
    with_activation("normal", &[], async |reconfigurer, candidate, root| {
        let published = publish(reconfigurer, candidate)?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let pid = recorded_pid(&instance_directory(root, "dual-b")?)?;
        rustix::process::kill_process(pid, rustix::process::Signal::KILL)?;
        wait_for_states(
            current(reconfigurer)?,
            &[("dual-b", PluginInstanceState::RestartBackoff)],
        )
        .await?;
        let status = current(reconfigurer)?.status_snapshot();
        assert_eq!(status.document_etag, published.document_etag);
        assert!(
            status
                .plugin_instances
                .iter()
                .find(|instance| instance.id == "dual-b")
                .ok_or("Failed Instance is missing")?
                .last_error
                .is_some()
        );
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn a_full_not_ready_sink_blocks_only_its_dependent_flow_until_release() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        let pending = std::num::NonZeroU64::new(1).ok_or("Missing pending limit")?;
        let bytes = std::num::NonZeroU64::new(1024).ok_or("Missing record limit")?;
        let capacity = tenon_ipc::queue::capacity_for_record_limit(
            pending, tenon_ipc::queue::maximum_frame_len(bytes)?,
        )?;
        let frame = tenon_ipc::queue::record_frame_len(capacity,
            std::num::NonZeroU64::new(capacity.get() - tenon_ipc::queue::FRAME_HEADER_LEN as u64).ok_or("Queue limit is missing")?,
            crate::contracts::sink::EncodedEgressRecord::from(&EgressRecord { payload: Vec::new() }).len())?;
        let count = usize::try_from(capacity.get())? / frame;
        assert_eq!(count * frame, usize::try_from(capacity.get())?);
        let target = additive_apply::next_revision(root, |document| {
            document["flows"]["dual-to-dual"]["maxPendingRecords"] = serde_json::json!(pending.get());
            document["flows"]["dual-to-dual"]["maxRecordBytes"] = serde_json::json!(bytes.get());
            document["pluginInstances"]["dual-b"]["config"]["behavior"] = serde_json::json!("delay-ready");
            document["flows"]["dual-to-dual"]["process"]["script"] = serde_json::json!(format!(
                "local b = registry:getBuilder('com.example.dual@1.0.0'); local first = true; function main(event) if first then first = false; emit(); for i = 1, {count} do emit(b:build()) end else emit(b:build()) end end"));
        })?;
        reconfigurer.apply(target, None).await?;
        wait_for_states(current(reconfigurer)?, &[("dual-b", PluginInstanceState::Starting)]).await?;
        let mut source = source_writer(root, "dual-a", "dual-to-dual", 0)?;
        let mut completion = source_completion(root, "dual-a", "dual-to-dual", 0)?;
        let mut sink = sink_reader(root, "dual-b", "dual-to-dual", 0)?;
        submit(&mut source, 40)?;
        for _ in 0..count { read_record(&mut sink).await?; }
        assert_eq!(IngressCompletion::decode(read_record(&mut completion).await?.as_slice())?.record_id, 40);
        completion.release(1)?;
        submit(&mut source, 41)?;
        let mut independent = source_writer(root, "input", "input-to-dual", 0)?;
        let mut independent_completion = source_completion(root, "input", "input-to-dual", 0)?;
        submit(&mut independent, 42)?;
        assert_eq!(IngressCompletion::decode(read_record(&mut independent_completion).await?.as_slice())?.record_id, 42);
        assert!(matches!(completion.try_read()?, ReadOutcome::Empty));
        assert!(matches!(sink.try_read()?, ReadOutcome::Empty));
        sink.release(count)?;
        assert!(EgressRecord::decode(read_record(&mut sink).await?.as_slice())?.payload.is_empty());
        sink.release(1)?;
        let completed = IngressCompletion::decode(read_record(&mut completion).await?.as_slice())?;
        assert_eq!(completed.record_id, 41);
        assert_eq!(completed.status(), IngressCompletionStatus::Ok);
        assert_eq!(states(&current(reconfigurer)?.status_snapshot())["dual-b"], "starting");
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_channel_rejects_publication_and_preserves_its_cause_after_cleanup() -> TestResult
{
    with_activation("normal", &[("dual-to-dual", DUAL_EMIT_LUA)], async |reconfigurer, candidate, root| {
        let active = candidate.as_mut().ok_or("Candidate is missing")?;
        wait_for_states(active, &[]).await?;
        let pids = ["input", "dual-a", "dual-b", "archive"]
            .map(|id| recorded_pid(&instance_directory(root, id)?))
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        let mut held = hold_dual_output(root).await?;
        let queue = std::fs::OpenOptions::new().write(true)
            .open(egress_queue_path(&instance_directory(root, "dual-b")?, "dual-to-dual", 0))?;
        corrupt_egress_commit(&queue)?;
        held.release(1)?;
        timeout(TEST_DEADLINE, active.runtime.wait_for_worker_exit()).await?;
        let result = reconfigurer.publish(candidate.take().ok_or("Candidate is missing")?, None);
        match result {
            Ok(_) => return Err("An exited worker was published".into()),
            Err(rejected) => *candidate = Some(*rejected),
        }
        assert!(reconfigurer.current.is_none());
        assert!(root.exists());
        let error = candidate
            .take()
            .ok_or("Rejected owner is missing")?
            .reject_after_worker_exit()
            .await;
        let PipelineReconfigureError::DataPlaneFailure(source) = &error else {
            return Err("A corrupted Egress Queue lost its failure".into());
        };
        assert!(matches!(source.as_ref(),
            PipelineRuntimeError::FlowChannelFailed { flow_id, .. } if flow_id.as_str() == "dual-to-dual"
        ));
        assert!(source.source().and_then(Error::source).is_some());
        for pid in pids {
            assert_reaped(pid)?;
        }
        assert!(!root.exists());
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn forced_cleanup_racing_with_cancellation_keeps_current_and_queue_files() -> TestResult {
    with_activation("delay-ready", &[], async |reconfigurer, candidate, root| {
        publish(reconfigurer, candidate)?;
        wait_for_states(
            current(reconfigurer)?,
            &[("dual-b", PluginInstanceState::Starting)],
        )
        .await?;
        let mut cleanup = Box::pin(current(reconfigurer)?.force_stop());
        let outcome = poll_fn(|context| Poll::Ready(cleanup.as_mut().poll(context))).await;
        drop(cleanup);
        if let Poll::Ready(result) = outcome {
            result?;
        }
        assert!(reconfigurer.current.is_some());
        assert!(root.exists());
        current(reconfigurer)?.force_stop().await?;
        assert!(root.exists());
        Ok(())
    })
    .await
}

async fn with_activation(
    behavior: &str,
    scripts: &[(&str, &str)],
    check: impl AsyncFnOnce(&mut Reconfigurer, &mut Option<ActivePipeline>, &Path) -> TestResult,
) -> TestResult {
    with_environment(
        behavior,
        scripts,
        async |reconfigurer, candidate, root, target| {
            let staged = ReconfigurePlan::derive(
                None,
                target,
                reconfigurer.environment.available_cpu_count(),
            )?
            .compile()?
            .stage(
                &reconfigurer.environment,
                reconfigurer.diagnostics.clone(),
                Arc::new(StartupControl::new()),
                None,
                None,
            )?;
            let mut started = staged.begin_cutover(retry_backoff());
            started.staged.runtime.bind().await?;
            started.launch_instances(
                |_| true,
                &reconfigurer.control,
                retry_backoff(),
                &reconfigurer.diagnostics,
            );
            *candidate = Some(started.activate());
            check(reconfigurer, candidate, root).await
        },
    )
    .await
}

pub(in crate::pipeline::reconfigure) async fn with_environment(
    behavior: &str,
    scripts: &[(&str, &str)],
    check: impl AsyncFnOnce(
        &mut Reconfigurer,
        &mut Option<ActivePipeline>,
        &Path,
        PipelineRevision,
    ) -> TestResult,
) -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let server = PluginControlServer::start()?;
    let mut target = revision(parent.path(), behavior)?;
    let mut document: serde_json::Value = serde_json::from_str(&target.tenon_document_json)?;
    for (flow, script) in scripts {
        document["flows"][flow]["process"]["script"] = (*script).into();
    }
    target.tenon_document_json = serde_json::to_string(&document)?;
    let environment = fixture_environment(&root)?;
    let diagnostics = publisher();
    let mut reconfigurer = Reconfigurer::new(environment, diagnostics, server.launcher());
    let mut candidate = None;
    let shutdown = reconfigurer.shutdown_handle();
    let mut checking = Box::pin(check(
        &mut reconfigurer,
        &mut candidate,
        &root,
        model(target)?,
    ));
    let outcome = {
        let observing = poll_fn(|context| {
            match catch_unwind(AssertUnwindSafe(|| checking.as_mut().poll(context))) {
                Ok(result) => result.map(Ok),
                Err(panic) => Poll::Ready(Err(panic)),
            }
        });
        tokio::pin!(observing);
        match timeout(TEST_DEADLINE, &mut observing).await {
            Ok(outcome) => outcome,
            Err(elapsed) => {
                // A deadline requests cleanup; it must not drop an in-progress apply.
                shutdown.request(crate::pipeline::reconfigure::ReconfigureShutdown::Force);
                match observing.await {
                    Ok(_) => Ok(Err(Box::new(elapsed) as Box<dyn Error>)),
                    Err(panic) => Err(panic),
                }
            }
        }
    };
    drop(checking);
    let pids = (|| -> TestResult<Vec<_>> {
        let mut pids = Vec::new();
        for owner in candidate.iter().chain(reconfigurer.current.iter()) {
            for id in owner.target.document().plugin_instances().keys() {
                let directory = instance_directory(&root, id.as_str())?;
                if directory.join("process.pid").try_exists()? {
                    pids.push(recorded_pid(&directory)?);
                }
            }
        }
        Ok(pids)
    })();
    for owner in candidate.iter_mut().chain(reconfigurer.current.iter_mut()) {
        timeout(TEST_DEADLINE, owner.force_stop()).await??;
    }
    for pid in pids? {
        assert_reaped(pid)?;
    }
    assert_eq!(
        root.exists(),
        candidate.is_some() || reconfigurer.current.is_some()
    );
    for owner in candidate
        .take()
        .into_iter()
        .chain(reconfigurer.current.take())
    {
        owner.finish_cleanup().await?;
    }
    drop(candidate);
    drop(reconfigurer);
    assert!(!root.exists());
    server.shutdown().await?;
    match outcome {
        Ok(result) => result,
        Err(panic) => resume_unwind(panic),
    }
}

fn publish(
    reconfigurer: &mut Reconfigurer,
    candidate: &mut Option<ActivePipeline>,
) -> TestResult<PipelineStatusSnapshot> {
    match reconfigurer.publish(candidate.take().ok_or("Candidate is missing")?, None) {
        Ok(status) => Ok(status),
        Err(rejected) => {
            *candidate = Some(*rejected);
            Err("Healthy candidate was rejected".into())
        }
    }
}

fn current(reconfigurer: &mut Reconfigurer) -> TestResult<&mut ActivePipeline> {
    reconfigurer
        .current
        .as_mut()
        .ok_or_else(|| "Current runtime is missing".into())
}

fn instance_pids(root: &Path) -> TestResult<BTreeMap<&'static str, rustix::process::Pid>> {
    ["input", "dual-a", "dual-b", "archive"]
        .into_iter()
        .map(|id| Ok((id, recorded_pid(&instance_directory(root, id)?)?)))
        .collect()
}

fn queue_files(root: &Path) -> TestResult<BTreeMap<PathBuf, (u64, u64)>> {
    let mut files = BTreeMap::new();
    for (id, channels) in [("input", 2), ("dual-a", 1), ("dual-b", 1), ("archive", 0)] {
        let directory = instance_directory(root, id)?;
        let mut paths = Vec::new();
        for index in 0..channels {
            for direction in ["submission", "completion"] {
                paths.push(directory.join(format!("source/{direction}-{index}.queue")));
            }
        }
        let incoming = match id {
            "dual-a" => Some(("input-to-dual", 2)),
            "dual-b" => Some(("dual-to-dual", 1)),
            "archive" => Some(("dual-to-archive", 1)),
            _ => None,
        };
        if let Some((flow, count)) = incoming {
            paths.extend((0..count).map(|channel| egress_queue_path(&directory, flow, channel)));
        }
        for path in paths {
            let metadata = std::fs::metadata(&path)?;
            files.insert(path, (metadata.dev(), metadata.ino()));
        }
    }
    assert_eq!(files.len(), 12);
    Ok(files)
}

fn states(status: &PipelineStatusSnapshot) -> BTreeMap<String, String> {
    status
        .plugin_instances
        .iter()
        .map(|instance| {
            let state = match instance.state() {
                PluginInstanceState::Starting => "starting",
                PluginInstanceState::Running => "running",
                PluginInstanceState::StartFailed => "start-failed",
                PluginInstanceState::RestartBackoff => "restart-backoff",
            };
            (instance.id.clone(), state.into())
        })
        .collect()
}

async fn wait_for_states(
    owner: &mut ActivePipeline,
    exceptions: &[(&str, PluginInstanceState)],
) -> TestResult {
    timeout(TEST_DEADLINE, async {
        loop {
            if owner
                .status_snapshot()
                .plugin_instances
                .iter()
                .all(|instance| {
                    instance.state()
                        == exceptions
                            .iter()
                            .find(|(id, _)| *id == instance.id)
                            .map_or(PluginInstanceState::Running, |(_, state)| *state)
                })
            {
                return Ok(());
            }
            owner.plugins.instances.next_event().await?;
        }
    })
    .await?
}

async fn read_record(queue: &mut QueueReader) -> TestResult<Vec<u8>> {
    timeout(TEST_DEADLINE, async {
        loop {
            match queue.try_read()? {
                ReadOutcome::Record(record) => return Ok(record.payload().to_vec()),
                ReadOutcome::Empty => tokio::task::yield_now().await,
            }
        }
    })
    .await?
}

fn submit(queue: &mut QueueWriter, record_id: u64) -> TestResult {
    match queue.try_write(
        &IngressRecord {
            record_id,
            payload: Vec::new().into(),
        }
        .encode_to_vec(),
    )? {
        WriteOutcome::Committed(_) => Ok(()),
        WriteOutcome::Full => Err("Fresh Submission Queue is full".into()),
    }
}
