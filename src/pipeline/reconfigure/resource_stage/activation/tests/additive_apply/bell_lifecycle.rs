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

//! Checks that a sleeping Sink receives notifications after resource changes.

use super::*;
use tenon_ipc::bell::{BellRegion, TimedWaitOutcome};

#[tokio::test(flavor = "current_thread")]
async fn shared_sink_waits_survive_combined_updates() -> TestResult {
    for case in [
        "lua",
        "config",
        "config-lua",
        "add",
        "resize",
        "limits",
        "remove",
        "remove-lua",
        "source-lua",
        "delivery",
    ] {
        with_environment("normal", &[], async |reconfigurer, _, root, _| {
            let initial = next_revision(root, |document| {
                if case != "add" {
                    add_shared_flow(document);
                }
            })?;
            reconfigurer.apply(initial, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let bell_path = loops_bell_path(&instance_directory(root, "archive")?.join("sink"));
            let original = std::fs::File::open(&bell_path)?;
            let next = next_revision(root, |document| {
                if !matches!(case, "remove" | "remove-lua") {
                    add_shared_flow(document);
                }
                if matches!(case, "config" | "config-lua" | "delivery") {
                    document["pluginInstances"]["archive"]["config"]["endpoint"] = json!("after");
                }
                if matches!(case, "lua" | "config-lua" | "remove-lua" | "source-lua") {
                    document["flows"]["dual-to-archive"]["process"]["script"] = json!(ADDED_LUA);
                }
                match case {
                    "resize" => document["flows"]["dual-to-archive"]["parallelism"] = json!(0.5),
                    "limits" => {
                        document["flows"]["dual-to-archive"]["maxPendingRecords"] = json!(32)
                    }
                    "source-lua" => {
                        document["pluginInstances"]["dual-b"]["config"]["endpoint"] = json!("after")
                    }
                    "delivery" => {
                        document["flows"]["dual-to-archive"]["delivery"] = json!("at-most-once")
                    }
                    _ => {}
                }
            })?;
            reconfigurer.apply(next, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            for (source, flow) in [
                ("dual-b", "dual-to-archive"),
                ("shared-input", "shared-flow"),
            ] {
                if source == "shared-input" && matches!(case, "remove" | "remove-lua") {
                    continue;
                }
                assert_sleeping_sink_receives(root, "archive", source, flow).await?;
            }
            assert_eq!(
                original.metadata()?.ino(),
                std::fs::metadata(&bell_path)?.ino(),
                "{case}"
            );
            assert_eq!(original.metadata()?.nlink(), 1, "{case}");
            assert!(!root.join(".candidate").exists());
            Ok(())
        })
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn failed_sink_starts_before_lua_handoff_and_keeps_its_region() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        let initial = next_revision(root, |document| {
            add_shared_flow(document);
            document["pluginInstances"]["archive"]["config"]["behavior"] =
                json!("exit-before-ready");
        })?;
        reconfigurer.apply(initial, None).await?;
        wait_for_states(
            current(reconfigurer)?,
            &[("archive", PluginInstanceState::StartFailed)],
        )
        .await?;
        let next = next_revision(root, |document| {
            add_shared_flow(document);
            document["flows"]["dual-to-archive"]["process"]["script"] = json!(ADDED_LUA);
        })?;
        reconfigurer.apply(next, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        for (source, flow) in [
            ("dual-b", "dual-to-archive"),
            ("shared-input", "shared-flow"),
        ] {
            assert_sleeping_sink_receives(root, "archive", source, flow).await?;
        }
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn sink_region_follows_interface_removal_and_addition() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        let revision = |bound: bool| next_revision(root, |document| {
            document["flows"]["input-to-dual"]["sinks"] = if bound { json!(["dual-a"]) } else { json!([]) };
            document["flows"]["input-to-dual"]["process"]["script"] = if bound {
                json!("local b = registry:getBuilder('com.example.dual@1.0.0'); function main(event) b:setValue('joined'); emit(b:build()) end")
            } else { json!("function main(event) emit() end") };
        });
        reconfigurer.apply(revision(false)?, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let directory = instance_directory(root, "dual-a")?;
        let source_region = std::fs::File::open(loops_bell_path(&directory.join("source")))?;
        let sink_path = loops_bell_path(&directory.join("sink"));
        assert!(!sink_path.exists());
        reconfigurer.apply(revision(true)?, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let first_sink = std::fs::File::open(&sink_path)?;
        assert_sleeping_sink_receives(root, "dual-a", "input", "input-to-dual").await?;
        reconfigurer.apply(revision(false)?, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        assert!(!directory.join("sink").exists());
        assert_eq!(first_sink.metadata()?.nlink(), 0);
        reconfigurer.apply(revision(true)?, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        assert_ne!(first_sink.metadata()?.ino(), std::fs::metadata(&sink_path)?.ino());
        assert_sleeping_sink_receives(root, "dual-a", "input", "input-to-dual").await?;
        assert_eq!(source_region.metadata()?.ino(), std::fs::metadata(loops_bell_path(&directory.join("source")))?.ino());
        Ok(())
    }).await
}

fn add_shared_flow(document: &mut Value) {
    add_source_flow(
        document,
        "shared-input",
        "shared-flow",
        "archive",
        ADDED_LUA,
    );
}

async fn assert_sleeping_sink_receives(
    root: &Path,
    sink: &str,
    source: &str,
    flow: &str,
) -> TestResult {
    let directory = instance_directory(root, sink)?;
    let region = BellRegion::open(&loops_bell_path(&directory.join("sink")))?;
    let bell = region.loop_bell(0)?;
    let reader = QueueReader::open(
        egress_queue_path(&directory, flow, 0),
        Arc::clone(&bell),
        BellRegion::open(&flow_channel_bell_path(root, flow))?,
    )?;
    let mut writer = source_writer(root, source, flow, 0)?;
    let waiting = Arc::clone(&bell);
    let task = tokio::task::spawn_blocking(move || {
        let outcome =
            waiting.wait_until_for(std::time::Duration::from_secs(2), || reader.has_unread());
        (outcome, reader)
    });
    let sending = timeout(TEST_DEADLINE, async {
        while bell.platform_waits().0 == 0 {
            tokio::task::yield_now().await;
        }
        submit(&mut writer, 1)
    })
    .await;
    let (outcome, mut reader) = task.await?;
    sending??;
    assert_eq!(
        outcome?,
        TimedWaitOutcome::Ready,
        "{flow} did not wake the Sink"
    );
    assert!(!read_value(&mut reader).await?.is_empty());
    let mut completion = source_completion(root, source, flow, 0)?;
    assert_eq!(
        IngressCompletion::decode(read_record(&mut completion).await?.as_slice())?.record_id,
        1
    );
    completion.release(1)?;
    Ok(())
}
