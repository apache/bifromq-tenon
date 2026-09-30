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
use crate::pipeline::reconfigure::ReconfigureShutdown;

#[tokio::test(flavor = "current_thread")]
async fn early_target_source_failure_aborts_and_reaps_the_entire_transition() -> TestResult {
    let vectors: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/contracts/plugin/test-fixtures/process_protocol_test_vectors.json"
    )))?;
    let cases = vectors["sourceFailureDuringConfiguration"]
        .as_array()
        .ok_or("Source failure vectors are missing")?;
    for case in cases {
        let behavior = case["behavior"]
            .as_str()
            .ok_or("Source failure behavior is missing")?;
        assert_eq!(
            case["expectedOutcome"],
            "pipeline-terminated-without-target-publication"
        );
        with_environment("normal", &[], async |reconfigurer, _, root, _| {
            let case = early_replacement_case()?;
            let mut initial = configuration_revision(root, &case, "old")?;
            edit_config(&mut initial, "cloud", |config| {
                config["behavior"] = json!("exit-before-ready")
            })?;
            reconfigurer.apply(model(initial)?, None).await?;
            wait_for_states(current(reconfigurer)?, &[("cloud", PluginInstanceState::StartFailed)]).await?;
            let pids = pids_for(root, &["cloud", "device", "unrelated", "unrelated-sink"])?;
            let mut next = configuration_revision(root, &case, "target")?;
            edit_config(&mut next, "cloud", |config| {
                config["behavior"] = json!(behavior);
                config["traffic"]["submissions"] = json!([]);
            })?;
            let result = timeout(TEST_DEADLINE, reconfigurer.apply(model(next)?, None)).await?;
            assert!(matches!(result, Err(PipelineReconfigureError::SourceFailedDuringReconfigure(id)) if id.as_str() == "cloud"));
            assert!(reconfigurer.current.is_none());
            assert!(!root.exists());
            for pid in pids.into_values() { assert_reaped(pid)?; }
            Ok(())
        }).await?;
    }
    Ok(())
}

/// A live old session that never finished starting still owes the ordered
/// handoff: it is the only Sink that can release the Egress record a Channel
/// already committed to it. Cutover must let it finish starting, quiesce it and
/// retire it last, instead of tearing down the peer the data plane waits on.
#[tokio::test(flavor = "current_thread")]
async fn starting_source_with_committed_input_finishes_starting_before_handoff() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        let case = early_replacement_case()?;
        let mut initial = configuration_revision(root, &case, "old")?;
        edit_config(&mut initial, "cloud", |config| {
            config["behavior"] = json!("delay-ready")
        })?;
        reconfigurer.apply(model(initial)?, None).await?;
        wait_for_states(
            current(reconfigurer)?,
            &[("cloud", PluginInstanceState::Starting)],
        )
        .await?;
        let cloud = instance_directory(root, "cloud")?;
        wait_for_file(&cloud.join("attach.received")).await?;
        let old = recorded_pid(&cloud)?;
        let device = instance_directory(root, "device")?;
        let result = during_apply(
            reconfigurer,
            model(configuration_revision(root, &case, "target")?)?,
            async {
                // Cutover entry must not retire the not-ready peer: its Egress
                // queue still holds the record this handoff has to settle.
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                assert_running(old)?;
                assert!(!cloud.join("quiesce-source.received").exists());
                // Only its own startup completion turns it into a handoff peer.
                std::fs::write(cloud.join("allow-ready"), [])?;
                wait_for_file(&cloud.join("quiesce-source.received")).await?;
                assert_running(old)?;
                assert!(!cloud.join("traffic-target-submitted.received").exists());
                std::fs::write(cloud.join("allow-egress-old"), [])?;
                std::fs::write(cloud.join("allow-completions-old"), [])?;
                wait_for_file(&device.join("source-quiesced.received")).await?;
                std::fs::write(device.join("allow-egress-old"), [])?;
                std::fs::write(device.join("allow-completions-old"), [])?;
                Ok(())
            },
        )
        .await?;
        assert!(matches!(result?, PipelineApplyOutcome::Applied(_)));
        wait_for_states(current(reconfigurer)?, &[]).await?;
        assert_reaped(old)?;
        assert!(!cloud.join("allow-completions-target").exists());
        // The retained Channel keeps its Lua VM count across the handoff, so the
        // target session's first Egress record is the peer's second record.
        assert_eq!(
            traffic_payload(&cloud, "target", 1).await?,
            "device-target:2"
        );
        Ok(())
    })
    .await
}

#[derive(Clone, Copy)]
enum Failure {
    Stop,
    Core,
}

#[tokio::test(flavor = "current_thread")]
async fn old_source_stream_loss_after_quiesced_aborts_without_target_publication() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        let case = early_replacement_case()?;
        let mut initial = configuration_revision(root, &case, "old")?;
        edit_config(&mut initial, "device", |config| {
            config["behavior"] = json!("stream-loss-after-quiesced")
        })?;
        reconfigurer.apply(model(initial)?, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let pids = pids_for(root, &["cloud", "device", "unrelated", "unrelated-sink"])?;
        let result = reconfigurer
            .apply(model(configuration_revision(root, &case, "target")?)?, None)
            .await;
        assert!(matches!(
            result,
            Err(PipelineReconfigureError::PluginInstanceLifecycle(_))
        ));
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
async fn stop_and_core_failure_during_old_completion_wait_reap_current_and_early_target()
-> TestResult {
    for failure in [Failure::Stop, Failure::Core] {
        with_environment("normal", &[], async |reconfigurer, _, root, _| {
            let case = early_replacement_case()?;
            let mut initial = configuration_revision(root, &case, "old")?;
            edit_config(&mut initial, "cloud", |config| config["behavior"] = json!("exit-before-ready"))?;
            reconfigurer.apply(model(initial)?, None).await?;
            wait_for_states(current(reconfigurer)?, &[("cloud", PluginInstanceState::StartFailed)]).await?;
            let mut pids = pids_for(root, &["cloud", "device", "unrelated", "unrelated-sink"])?;
            let mut unrelated_source = source_writer(root, "unrelated", "unrelated-flow", 0)?;
            let queue = std::fs::OpenOptions::new().write(true).open(egress_queue_path(&instance_directory(root, "unrelated-sink")?, "unrelated-flow", 0))?;
            let shutdown = reconfigurer.shutdown_handle();
            let result = during_apply(reconfigurer, model(configuration_revision(root, &case, "target")?)?, async {
                let device = instance_directory(root, "device")?;
                let cloud = instance_directory(root, "cloud")?;
                wait_for_file(&device.join("source-quiesced.received")).await?;
                wait_for_file(&cloud.join("traffic-target-submitted.received")).await?;
                pids.insert("target-cloud".into(), recorded_pid(&cloud)?);
                timeout(
                    TEST_DEADLINE,
                    wait_for_channel_park(root, "device", "telemetry", 0),
                )
                .await??;
                match failure {
                    Failure::Stop => shutdown.request(ReconfigureShutdown::Force),
                    Failure::Core => {
                        queue.write_all_at(&1_u64.to_le_bytes(), TEST_RELEASE_OFFSET)?;
                        submit_value(&mut unrelated_source, 901, "trigger-failure")?;
                    }
                }
                Ok(())
            }).await?;
            match failure {
                Failure::Stop => assert!(matches!(result?, PipelineApplyOutcome::Stopped)),
                Failure::Core => {
                    let Err(PipelineReconfigureError::DataPlaneFailure(source)) = result else { return Err("Handoff lost the core failure cause".into()) };
                    assert!(matches!(source.as_ref(), PipelineRuntimeError::FlowChannelFailed { flow_id, .. } if flow_id.as_str() == "unrelated-flow"));
                }
            }
            assert!(reconfigurer.current.is_none());
            assert!(!root.exists());
            for pid in pids.into_values() { assert_reaped(pid)?; }
            Ok(())
        }).await?;
    }
    Ok(())
}

fn early_replacement_case() -> TestResult<ConfigurationCase> {
    let vectors: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/contracts/plugin/test-fixtures/process_protocol_test_vectors.json"
    )))?;
    Ok(serde_json::from_value(
        vectors["configurationOnlyReplacement"][0].clone(),
    )?)
}

fn edit_config(
    revision: &mut PipelineRevisionPlan,
    id: &str,
    edit: impl FnOnce(&mut Value),
) -> TestResult {
    let mut document: Value = serde_json::from_str(&revision.tenon_document_json)?;
    edit(&mut document["pluginInstances"][id]["config"]);
    revision.tenon_document_json = serde_json::to_string(&document)?;
    Ok(())
}
