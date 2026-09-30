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

//! Exercises one apply chain with real children and real Pipeline Queue traffic.
//! The controlled lifecycle children do not themselves consume business Queues.

use super::*;
use crate::pipeline::reconfigure::PipelineApplyOutcome;
use crate::pipeline::reconfigure::plan::tests::{flow, instance};
use serde_json::{Value, json};

mod configuration_handoff;
mod deadline;
mod during_apply;
mod failure;
mod lua_apply;
mod normal_shutdown;
mod record_limits;
mod resource_changes;
mod retained_lifecycle;
mod sink_config;

use during_apply::during_apply;

const COUNTING_LUA: &str = "local b = registry:getBuilder('com.example.archive@1.0.0'); local count = 0; function main(event) count = count + 1; b:setValue('stable:' .. count); emit(b:build()) end";
const ADDED_LUA: &str = "local b = registry:getBuilder('com.example.archive@1.0.0'); function main(event) b:setValue('added'); emit(b:build()) end";
const COMPLETING_LUA: &str =
    "function main(event) if event.payload.value ~= 'pending' then emit() end end";

#[tokio::test(flavor = "current_thread")]
async fn equivalent_apply_publishes_new_material_without_restarting_resources() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let pids = instance_pids(root)?;
            let files = queue_files(root)?;
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            submit(&mut source, 1)?;
            assert_eq!(read_value(&mut sink).await?, "stable:1");
            let next = next_revision(root, |document| {
                document["flows"]["dual-to-archive"]["delivery"] = json!("at-least-once");
                document["flows"]["dual-to-archive"]["maxPendingRecords"] = json!(100);
                document["flows"]["dual-to-archive"]["maxRecordBytes"] = json!(262144);
                if let Some(flow) = document["flows"]["dual-to-archive"].as_object_mut() {
                    flow.remove("parallelism");
                }
            })?;

            let PipelineApplyOutcome::Applied(status) = reconfigurer.apply(next, None).await?
            else {
                return Err("Equivalent apply unexpectedly stopped".into());
            };
            assert_eq!(status.document_etag, "updated");
            assert_eq!(status.plugin_instances.len(), 4);
            let published: Value =
                serde_json::from_str(&current(reconfigurer)?.target.document().strict_json())?;
            assert_eq!(
                published["flows"]["dual-to-archive"]["delivery"],
                "at-least-once"
            );
            assert!(
                published["flows"]["dual-to-archive"]
                    .get("parallelism")
                    .is_none()
            );
            assert_eq!(instance_pids(root)?, pids);
            assert_eq!(queue_files(root)?, files);
            assert_started_once(root, &["input", "dual-a", "dual-b", "archive"])?;
            submit(&mut source, 2)?;
            assert_eq!(read_value(&mut sink).await?, "stable:2");
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn added_flow_restarts_its_sink_and_preserves_old_queues_and_lua_state() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let pids = instance_pids(root)?;
            let files = queue_files(root)?;
            let mut old_source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            submit(&mut old_source, 1)?;
            assert_eq!(read_value(&mut sink).await?, "stable:1");
            let next = next_revision(root, |document| {
                add_source_flow(document, "added-input", "added-flow", "archive", ADDED_LUA)
            })?;

            let PipelineApplyOutcome::Applied(status) = reconfigurer.apply(next, None).await?
            else {
                return Err("Additive apply unexpectedly stopped".into());
            };
            assert_eq!(status.document_etag, "updated");
            assert_eq!(
                status
                    .plugin_instances
                    .iter()
                    .map(|instance| instance.id.as_str())
                    .collect::<Vec<_>>(),
                ["added-input", "archive", "dual-a", "dual-b", "input"]
            );
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let after = instance_pids(root)?;
            assert_ne!(after["archive"], pids["archive"]);
            assert_reaped(pids["archive"])?;
            for id in ["input", "dual-a", "dual-b"] {
                assert_eq!(after[id], pids[id]);
            }
            assert_eq!(queue_files(root)?, files);
            assert_started_once(root, &["input", "dual-a", "dual-b", "added-input"])?;
            submit(&mut old_source, 2)?;
            assert_eq!(read_value(&mut sink).await?, "stable:2");
            let mut added = source_writer(root, "added-input", "added-flow", 0)?;
            let mut added_sink = sink_reader(root, "archive", "added-flow", 0)?;
            submit(&mut added, 1)?;
            assert_eq!(read_value(&mut added_sink).await?, "added");
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn added_sources_share_one_new_sink_while_old_resources_continue() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let files = queue_files(root)?;
            let pids = instance_pids(root)?;
            let next = next_revision(root, |document| {
                document["pluginInstances"]["added-archive"] =
                    instance("com.example.archive", "new-disk");
                add_source_flow(
                    document,
                    "added-a",
                    "added-flow-a",
                    "added-archive",
                    ADDED_LUA,
                );
                add_source_flow(
                    document,
                    "added-b",
                    "added-flow-b",
                    "added-archive",
                    ADDED_LUA,
                );
            })?;
            let PipelineApplyOutcome::Applied(status) = reconfigurer.apply(next, None).await?
            else {
                return Err("Additive apply unexpectedly stopped".into());
            };
            assert_eq!(status.plugin_instances.len(), 7);
            wait_for_states(current(reconfigurer)?, &[]).await?;
            assert_eq!(instance_pids(root)?, pids);
            assert_eq!(queue_files(root)?, files);
            assert_started_once(root, &["added-a", "added-b", "added-archive"])?;
            for (id, flow) in [("added-a", "added-flow-a"), ("added-b", "added-flow-b")] {
                let mut sink = sink_reader(root, "added-archive", flow, 0)?;
                let mut source = source_writer(root, id, flow, 0)?;
                submit(&mut source, 1)?;
                assert_eq!(read_value(&mut sink).await?, "added");
                let mut completion = source_completion(root, id, flow, 0)?;
                assert_eq!(
                    IngressCompletion::decode(read_record(&mut completion).await?.as_slice())?
                        .record_id,
                    1
                );
            }
            let mut old_source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut old_sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            submit(&mut old_source, 1)?;
            assert_eq!(read_value(&mut old_sink).await?, "stable:1");
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
#[expect(
    clippy::expect_used,
    reason = "the fixture declares instances and flows as objects"
)]
async fn removal_reaps_only_selected_resources_and_publishes_the_target() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let pids = instance_pids(root)?;
            let files = queue_files(root)?;
            let next = next_revision(root, |document| {
                document["pluginInstances"]
                    .as_object_mut()
                    .expect("Fixture instances are an object")
                    .remove("input");
                document["flows"]
                    .as_object_mut()
                    .expect("Fixture flows are an object")
                    .remove("input-to-dual");
            })?;
            assert!(matches!(
                reconfigurer.apply(next, None).await?,
                PipelineApplyOutcome::Applied(_)
            ));
            assert_eq!(
                current(reconfigurer)?.status_snapshot().document_etag,
                "updated"
            );
            assert_reaped(pids["input"])?;
            let removed = instance_directory(root, "input")?;
            assert!(!removed.exists());
            wait_for_states(current(reconfigurer)?, &[]).await?;
            assert_reaped(pids["dual-a"])?;
            assert_ne!(
                recorded_pid(&instance_directory(root, "dual-a")?)?,
                pids["dual-a"]
            );
            let retired_sink = instance_directory(root, "dual-a")?.join("sink");
            assert!(!retired_sink.exists());
            for id in ["dual-b", "archive"] {
                assert_eq!(recorded_pid(&instance_directory(root, id)?)?, pids[id]);
            }
            for (path, identity) in files {
                if !path.starts_with(&removed) && !path.starts_with(&retired_sink) {
                    let metadata = std::fs::metadata(path)?;
                    assert_eq!((metadata.dev(), metadata.ino()), identity);
                }
            }
            Ok(())
        },
    )
    .await
}

pub(super) fn next_revision(
    root: &Path,
    edit: impl FnOnce(&mut Value),
) -> TestResult<PipelineRevision> {
    let mut next = revision(root.parent().ok_or("Fixture parent is missing")?, "normal")?;
    let mut document: Value = serde_json::from_str(&next.tenon_document_json)?;
    document["flows"]["dual-to-archive"]["process"]["script"] = json!(COUNTING_LUA);
    edit(&mut document);
    let instances = document["pluginInstances"]
        .as_object()
        .ok_or("Fixture instances must be an object")?;
    next.plugin_programs.retain(|program| {
        instances.values().any(|instance| {
            instance["programName"] == program.program_name
                && instance["exactVersion"] == program.exact_version
        })
    });
    next.tenon_document_json = serde_json::to_string_pretty(&document)?;
    next.document_etag = "updated".into();
    model(next)
}

fn add_source_flow(document: &mut Value, source: &str, flow_id: &str, sink: &str, script: &str) {
    document["pluginInstances"][source] = instance("com.example.input", "new-device");
    document["flows"][flow_id] = flow(source, 1, &[sink]);
    document["flows"][flow_id]["process"]["script"] = json!(script);
}

#[derive(prost::Message)]
struct TestPayload {
    #[prost(string, tag = "1")]
    value: String,
}

pub(super) async fn read_value(queue: &mut QueueReader) -> TestResult<String> {
    let record = EgressRecord::decode(read_record(queue).await?.as_slice())?;
    let payload = TestPayload::decode(record.payload.as_slice())?;
    queue.release(1)?;
    Ok(payload.value)
}

fn assert_started_once(root: &Path, ids: &[&str]) -> TestResult {
    for id in ids {
        assert_eq!(
            std::fs::read_to_string(instance_directory(root, id)?.join("starts.received"))?,
            "started\n",
            "{id}"
        );
    }
    Ok(())
}

async fn fill_completion(
    root: &Path,
    max_pending_records: u64,
) -> TestResult<(QueueWriter, QueueReader)> {
    let mut source = source_writer(root, "input", "input-to-dual", 0)?;
    let mut completion = source_completion(root, "input", "input-to-dual", 0)?;
    // Reproduce old-session completions plus newly admitted input without a second writer.
    for offset in 0..=max_pending_records {
        let id = u64::MAX - offset;
        submit(&mut source, id)?;
        let record = IngressCompletion::decode(read_record(&mut completion).await?.as_slice())?;
        assert_eq!(record.record_id, id);
        assert_eq!(record.status(), IngressCompletionStatus::Ok);
    }
    let WriteOutcome::Committed(receipt) = {
        let record = IngressRecord {
            record_id: 7,
            payload: TestPayload {
                value: String::from("pending"),
            }
            .encode_to_vec()
            .into(),
        };
        source.try_write_with(
            record.encoded_len(),
            |destination| record.encode(destination).map_err(std::io::Error::other),
            || {},
        )
    }?
    else {
        return Err("Pending fixture input could not enter Submission".into());
    };
    timeout(TEST_DEADLINE, async {
        while !source.is_released(&receipt)? {
            tokio::task::yield_now().await;
        }
        Ok::<_, tenon_ipc::queue::QueueRuntimeError>(())
    })
    .await??;
    Ok((source, completion))
}
