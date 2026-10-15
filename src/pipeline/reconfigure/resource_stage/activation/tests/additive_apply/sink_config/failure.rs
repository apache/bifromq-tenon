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

//! Stage is reversible; failure after retirement begins terminates all resources.

use super::*;
use crate::pipeline::reconfigure::ReconfigureShutdown;

#[tokio::test(flavor = "current_thread")]
async fn failed_added_stage_keeps_old_sink_config_process_and_lua() -> TestResult {
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
                document["pluginInstances"]["archive"]["config"]["endpoint"] = json!("after");
                document["flows"]["dual-to-archive"]["process"]["script"] =
                    json!(format!("setTimeout(0); {ADDED_LUA}"));
                add_source_flow(
                    document,
                    "added-input",
                    "added-flow",
                    "archive",
                    "error('candidate failed')",
                );
            })?;
            assert!(matches!(
                reconfigurer.apply(next, None).await,
                Err(PipelineReconfigureError::RuntimeStart(_))
            ));
            assert_eq!(
                current(reconfigurer)?.status_snapshot().document_etag,
                "initial-cutover"
            );
            assert_eq!(instance_pids(root)?, pids);
            assert_eq!(queue_files(root)?, files);
            assert!(
                !instance_directory(root, "archive")?
                    .join("shutdown.received")
                    .exists()
            );
            assert!(!instance_directory(root, "added-input")?.exists());
            assert!(
                !crate::pipeline::reconfigure::runtime_files::flow_directory(root, "added-flow")
                    .exists()
            );
            assert!(!root.join(".candidate").exists());
            assert!(matches!(sink.try_read()?, ReadOutcome::Empty));
            submit(&mut source, 2)?;
            assert_eq!(read_value(&mut sink).await?, "stable:2");
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn stop_during_sink_shutdown_reaps_children_and_joins_workers_before_removing_files()
-> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        let initial = next_revision(root, |document| {
            document["pluginInstances"]["archive"]["config"]["behavior"] = json!("delay-shutdown")
        })?;
        reconfigurer.apply(initial, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let pids = instance_pids(root)?;
        let next = mixed_revision(root)?;
        let shutdown = reconfigurer.shutdown_handle();
        let outcome = during_apply(reconfigurer, next, async {
            wait_for_file(&instance_directory(root, "archive")?.join("shutdown.received")).await?;
            assert!(
                instance_directory(root, "added-input")?
                    .join("source/submission-0.queue")
                    .exists()
            );
            assert!(
                !instance_directory(root, "added-input")?
                    .join("starts.received")
                    .exists()
            );
            shutdown.request(ReconfigureShutdown::Force);
            Ok(())
        })
        .await?;
        assert!(matches!(outcome?, PipelineApplyOutcome::Stopped));
        assert!(reconfigurer.current.is_none());
        assert!(!root.exists());
        for pid in pids.into_values() {
            assert_reaped(pid)?;
        }
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_shutdown_reply_is_fatal_without_publishing_target() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        let initial = next_revision(root, |document| document["pluginInstances"]["archive"]["config"]["behavior"] = json!("message-after-shutdown"))?;
        reconfigurer.apply(initial, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let pids = instance_pids(root)?;
        let error = reconfigurer.apply(mixed_revision(root)?, None).await;
        assert!(matches!(error, Err(PipelineReconfigureError::PluginInstanceLifecycle(source)) if source.to_string().contains("archive")));
        assert!(reconfigurer.current.is_none());
        assert!(!root.exists());
        for pid in pids.into_values() { assert_reaped(pid)?; }
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn worker_failure_during_sink_shutdown_keeps_original_cause_and_cleans_every_owner()
-> TestResult {
    with_environment("normal", &[("dual-to-dual", DUAL_EMIT_LUA)], async |reconfigurer, _, root, _| {
        let initial = next_revision(root, |document| { document["pluginInstances"]["archive"]["config"]["behavior"] = json!("delay-shutdown"); document["flows"]["dual-to-dual"]["process"]["script"] = json!(DUAL_EMIT_LUA); })?;
        reconfigurer.apply(initial, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let pids = instance_pids(root)?;
        let queue = std::fs::OpenOptions::new().write(true).open(egress_queue_path(&instance_directory(root, "dual-b")?, "dual-to-dual", 0))?;
        let mut held = hold_dual_output(root).await?;
        let outcome = during_apply(reconfigurer, mixed_revision(root)?, async {
            wait_for_file(&instance_directory(root, "archive")?.join("shutdown.received")).await?;
            corrupt_egress_commit(&queue)?;
            held.release(1)?;
            Ok(())
        }).await?;
        assert!(matches!(outcome, Err(PipelineReconfigureError::DataPlaneFailure(source))
            if matches!(source.as_ref(), PipelineRuntimeError::FlowChannelFailed { flow_id, .. } if flow_id.as_str() == "dual-to-dual")));
        assert!(reconfigurer.current.is_none());
        assert!(!root.exists());
        for pid in pids.into_values() { assert_reaped(pid)?; }
        Ok(())
    }).await
}

fn mixed_revision(root: &Path) -> TestResult<PipelineRevision> {
    next_revision(root, |document| {
        document["flows"]["dual-to-dual"]["process"]["script"] = json!(DUAL_EMIT_LUA);
        document["pluginInstances"]["archive"]["config"]["endpoint"] = json!("after");
        document["flows"]["dual-to-archive"]["process"]["script"] =
            json!(format!("setTimeout(0); {ADDED_LUA}"));
        add_source_flow(
            document,
            "added-input",
            "added-flow",
            "archive",
            &format!("setTimeout(0); {ADDED_LUA}"),
        );
    })
}
