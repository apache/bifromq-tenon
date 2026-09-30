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

//! Replaces only the Sink process while real Queue and Channel owners remain live.

use super::*;

mod failure;
mod lifecycle;
mod waiting;

#[tokio::test(flavor = "current_thread")]
async fn sink_config_replaces_only_its_process_and_preserves_lua_state() -> TestResult {
    with_environment(
        "normal",
        &[("dual-to-archive", COUNTING_LUA)],
        async |reconfigurer, _, root, target| {
            reconfigurer.apply(target, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let before = instance_pids(root)?;
            let files = queue_files(root)?;
            let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
            let mut sink = sink_reader(root, "archive", "dual-to-archive", 0)?;
            submit(&mut source, 1)?;
            assert_eq!(read_value(&mut sink).await?, "stable:1");
            let next = next_revision(root, |document| {
                document["pluginInstances"]["archive"]["config"]["endpoint"] = json!("after");
            })?;

            let PipelineApplyOutcome::Applied(status) = reconfigurer.apply(next, None).await?
            else {
                return Err("Sink configuration apply unexpectedly stopped".into());
            };
            assert_eq!(status.document_etag, "updated");
            assert_eq!(status.plugin_instances.len(), 4);
            assert_reaped(before["archive"])?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let after = instance_pids(root)?;
            assert_ne!(after["archive"], before["archive"]);
            for id in ["input", "dual-a", "dual-b"] {
                assert_eq!(after[id], before[id]);
            }
            assert_eq!(queue_files(root)?, files);
            assert_started_once(root, &["input", "dual-a", "dual-b"])?;
            submit(&mut source, 2)?;
            assert_eq!(read_value(&mut sink).await?, "stable:2");
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn replacement_vector_replays_unreleased_output_before_source_completion() -> TestResult {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Vector {
        name: String,
        instance_id: String,
        current_config: Value,
        target_config: Value,
        payload_text: String,
    }
    let vectors: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/contracts/plugin/test-fixtures/process_protocol_test_vectors.json"
    )))?;
    let case: Vector = serde_json::from_value(vectors["sinkConfigurationReplacement"].clone())?;
    let script = format!(
        "local b = registry:getBuilder('com.example.archive@1.0.0'); function main(event) b:setValue('{}'); emit(b:build()) end",
        case.payload_text
    );
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        let initial = next_revision(root, |document| {
            document["pluginInstances"][&case.instance_id]["config"] = case.current_config.clone();
            document["flows"]["dual-to-archive"]["process"]["script"] = json!(script);
        })?;
        reconfigurer.apply(initial, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let old_pid = recorded_pid(&instance_directory(root, &case.instance_id)?)?;
        let files = queue_files(root)?;
        let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
        let mut completed = source_completion(root, "dual-b", "dual-to-archive", 0)?;
        let mut old_reader = sink_reader(root, &case.instance_id, "dual-to-archive", 0)?;
        submit(&mut source, 41)?;
        let pending = read_record(&mut old_reader).await?;
        assert!(matches!(completed.try_read()?, ReadOutcome::Empty));
        drop(old_reader);

        let next = next_revision(root, |document| {
            document["pluginInstances"][&case.instance_id]["config"] = case.target_config.clone();
            document["flows"]["dual-to-archive"]["process"]["script"] = json!(script);
        })?;
        assert!(
            matches!(
                reconfigurer.apply(next, None).await?,
                PipelineApplyOutcome::Applied(_)
            ),
            "{}",
            case.name
        );
        assert_reaped(old_pid)?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        assert_eq!(queue_files(root)?, files);
        let received = std::fs::read_to_string(
            instance_directory(root, &case.instance_id)?.join("configs.received"),
        )?;
        let received: Vec<Value> = received
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?;
        assert_eq!(received, [case.current_config, case.target_config]);
        // The controlled child proves process/config handoff; this real reader
        // proves replay and release, not Java SDK destination delivery.
        let mut new_reader = sink_reader(root, &case.instance_id, "dual-to-archive", 0)?;
        let replay = read_record(&mut new_reader).await?;
        assert_eq!(replay, pending);
        let record = EgressRecord::decode(replay.as_slice())?;
        assert_eq!(
            TestPayload::decode(record.payload.as_slice())?.value,
            case.payload_text
        );
        assert!(matches!(completed.try_read()?, ReadOutcome::Empty));
        new_reader.release(1)?;
        let completion = IngressCompletion::decode(read_record(&mut completed).await?.as_slice())?;
        assert_eq!(completion.record_id, 41);
        assert_eq!(completion.status(), IngressCompletionStatus::Ok);
        Ok(())
    })
    .await
}
