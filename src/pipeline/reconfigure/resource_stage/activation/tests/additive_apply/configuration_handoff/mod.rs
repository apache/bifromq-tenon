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

//! Configuration handoff with actual child-owned Submission, Completion and Egress endpoints.

use super::*;
use crate::contracts::core::PipelineRevisionPlan;
use crate::pipeline::reconfigure::test_support::bootstrap;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use rustix::process::{Pid, Signal, kill_process, test_kill_process};

mod lifecycle;

#[tokio::test(flavor = "current_thread")]
async fn source_only_config_finishes_every_channel_and_publishes_before_target_ready() -> TestResult
{
    const SCRIPT: &str = "local b = registry:getBuilder('com.example.dual@1.0.0'); local count = 0; function main(event) count = count + 1; if event.payload.value ~= 'pending' then b:setValue(event.payload.value .. ':' .. count); emit(b:build()) end end";
    with_environment("normal", &[], async |reconfigurer, _, root, _| {
        let make_revision = |session: &str| next_revision(root, |document| {
            document["flows"]["input-to-dual"]["process"]["script"] = json!(SCRIPT);
            document["pluginInstances"]["dual-a"]["config"]["traffic"] = json!({"session":"old"});
            document["pluginInstances"]["input"]["config"]["behavior"] = json!(if session == "old" { "normal" } else { "delay-ready" });
            document["pluginInstances"]["input"]["config"]["traffic"] = json!({
                "session":session, "completionGate": format!("allow-completions-{session}"),
                "submissions":(0..2).map(|channel| json!({"channelIndex":channel, "recordId":if session == "old" {1} else {2}, "payload":TestPayload{value:if session == "old" {"pending".into()} else {format!("new-{channel}")}}.encode_to_vec()})).collect::<Vec<_>>()
            });
        });
        reconfigurer.apply(make_revision("old")?, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let before = instance_pids(root)?;
        let files = queue_files(root)?;
        let input = instance_directory(root, "input")?;
        let result = during_apply(reconfigurer, make_revision("target")?, async {
            wait_for_file(&input.join("source-quiesced.received")).await?;
            for channel in 0..2 {
                timeout(
                    TEST_DEADLINE,
                    wait_for_channel_park(root, "input", "input-to-dual", channel),
                )
                .await??;
            }
            assert!(!input.join("shutdown.received").exists());
            std::fs::write(input.join("allow-completions-old"), [])?;
            Ok(())
        }).await?;
        let PipelineApplyOutcome::Applied(status) = result? else { return Err("Source handoff unexpectedly stopped".into()) };
        assert_eq!(states(&status)["input"], "starting");
        assert_reaped(before["input"])?;
        wait_for_states(current(reconfigurer)?, &[("input", PluginInstanceState::Starting)]).await?;
        let submitted = input.join("traffic-target-submitted.received");
        loop {
            tokio::select! {
                result = current(reconfigurer)?.plugins.instances.next_event() => { result?; }
                result = wait_for_file(&submitted) => { result?; break; }
            }
        }
        for channel in 0..2 {
            let completion = IngressCompletion::decode(traffic_record(&input, "old", &format!("completion-{channel}"), 1).await?.as_slice())?;
            assert_eq!(completion.record_id, 1);
            assert_eq!(completion.status(), IngressCompletionStatus::Retry);
        }
        let sink = instance_directory(root, "dual-a")?;
        let mut outputs = vec![traffic_payload(&sink, "old", 1).await?, traffic_payload(&sink, "old", 2).await?];
        outputs.sort();
        assert_eq!(outputs, ["new-0:2", "new-1:2"]);
        assert_eq!(queue_files(root)?, files);
        for id in ["dual-a", "dual-b", "archive"] { assert_eq!(recorded_pid(&instance_directory(root, id)?)?, before[id]); }
        assert!(!input.join("allow-ready").exists());
        std::fs::write(input.join("allow-ready"), [])?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        Ok(())
    }).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfigurationCase {
    name: String,
    flows: BTreeMap<String, [String; 2]>,
    current_phases: BTreeMap<String, String>,
    replaced_instances: Vec<String>,
    failure: Option<String>,
}

#[tokio::test(flavor = "current_thread")]
async fn configuration_vectors_keep_consumers_alive_until_old_source_responsibility_finishes()
-> TestResult {
    let vectors: Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/contracts/plugin/test-fixtures/process_protocol_test_vectors.json"
    )))?;
    let mut cases: Vec<ConfigurationCase> =
        serde_json::from_value(vectors["configurationOnlyReplacement"].clone())?;
    assert_eq!(cases.len(), 4);
    cases.push(ConfigurationCase {
        name: "Multiple Sources share one retained Sink".into(),
        flows: BTreeMap::from([
            ("device-data".into(), ["device".into(), "archive".into()]),
            ("cloud-data".into(), ["cloud".into(), "archive".into()]),
        ]),
        current_phases: BTreeMap::from([
            ("device".into(), "ready".into()),
            ("cloud".into(), "ready".into()),
            ("archive".into(), "ready".into()),
        ]),
        replaced_instances: vec!["device".into(), "cloud".into(), "archive".into()],
        failure: None,
    });
    for case in cases {
        with_environment("normal", &[], async |reconfigurer, _, root, _| {
            let initial = configuration_revision(root, &case, "old")?;
            set_retry_delay(reconfigurer, root, 60_000)?;
            reconfigurer.apply(model(initial)?, None).await?;
            wait_for_states(current(reconfigurer)?, &[]).await?;
            let ids: Vec<_> = case
                .current_phases
                .keys()
                .map(String::as_str)
                .chain(["unrelated", "unrelated-sink"])
                .collect();
            let before = pids_for(root, &ids)?;
            let files = configuration_queue_files(root, &ids)?;
            let mut unrelated_source = source_writer(root, "unrelated", "unrelated-flow", 0)?;
            for (id, phase) in &case.current_phases {
                if phase == "backoff" {
                    kill_process(before[id], Signal::KILL)?;
                    wait_for_states(
                        current(reconfigurer)?,
                        &[(id, PluginInstanceState::RestartBackoff)],
                    )
                    .await?;
                    assert_reaped(before[id])?;
                }
            }
            let target = model(configuration_revision(root, &case, "target")?)?;
            let healthy_sources: Vec<_> = case
                .replaced_instances
                .iter()
                .filter(|id| {
                    case.current_phases[*id] == "ready"
                        && case.flows.values().any(|[source, _]| source == *id)
                })
                .collect();
            let outcome = during_apply(reconfigurer, target, async {
                for id in &healthy_sources {
                    wait_for_file(&instance_directory(root, id)?.join("source-quiesced.received"))
                        .await?;
                }
                for (id, phase) in &case.current_phases {
                    if phase == "backoff" {
                        let directory = instance_directory(root, id)?;
                        wait_for_file(&directory.join("traffic-target-submitted.received")).await?;
                        assert_ne!(recorded_pid(&directory)?, before[id]);
                        assert_reaped(before[id])?;
                    }
                }
                for id in &healthy_sources {
                    let directory = instance_directory(root, id)?;
                    assert!(
                        !directory.join("shutdown.received").exists(),
                        "{}",
                        case.name
                    );
                    assert_eq!(recorded_pid(&directory)?, before[id.as_str()]);
                }
                // The retained Channel and child consumer keep working during handoff.
                submit_value(&mut unrelated_source, 900, "during-handoff")?;
                let unrelated_sink = instance_directory(root, "unrelated-sink")?;
                assert_eq!(
                    traffic_payload(&unrelated_sink, "old", 1).await?,
                    "during-handoff"
                );

                for id in case.current_phases.keys() {
                    std::fs::write(instance_directory(root, id)?.join("allow-egress-old"), [])?;
                }
                for [source, sink] in case.flows.values() {
                    if case.current_phases[source] == "backoff"
                        && case.current_phases[sink] == "ready"
                    {
                        // An early target Source can feed the still-live old Sink.
                        // Hold old Completion until that consumer has actually received it.
                        assert_eq!(
                            traffic_payload(&instance_directory(root, sink)?, "old", 2).await?,
                            format!("{source}-target:2")
                        );
                    }
                }
                for id in &healthy_sources {
                    let flow = case
                        .flows
                        .iter()
                        .find(|(_, [source, _])| source == *id)
                        .map(|(flow, _)| flow.as_str())
                        .ok_or("A healthy Source owns no Flow")?;
                    timeout(TEST_DEADLINE, wait_for_channel_park(root, id, flow, 0)).await??;
                    assert!(
                        !instance_directory(root, id)?
                            .join("shutdown.received")
                            .exists()
                    );
                }
                if case.failure.is_some() {
                    kill_process(before[healthy_sources[0].as_str()], Signal::KILL)?;
                } else {
                    for id in &healthy_sources {
                        std::fs::write(
                            instance_directory(root, id)?.join("allow-completions-old"),
                            [],
                        )?;
                    }
                }
                Ok(())
            })
            .await?;
            if case.failure.is_some() {
                assert!(
                    matches!(
                        outcome,
                        Err(PipelineReconfigureError::PluginInstanceLifecycle(_))
                    ),
                    "{}",
                    case.name
                );
                assert!(reconfigurer.current.is_none());
                assert!(!root.exists());
                for pid in before.into_values() {
                    assert_reaped(pid)?;
                }
                return Ok(());
            }
            let PipelineApplyOutcome::Applied(status) = outcome? else {
                return Err("Configuration handoff unexpectedly stopped".into());
            };
            assert_eq!(status.document_etag, "target");
            wait_for_states(current(reconfigurer)?, &[]).await?;
            assert_eq!(
                configuration_queue_files(root, &ids)?,
                files,
                "{}",
                case.name
            );
            for id in &case.replaced_instances {
                assert_reaped(before[id])?;
                let directory = instance_directory(root, id)?;
                assert_ne!(recorded_pid(&directory)?, before[id]);
                assert!(!directory.join("allow-completions-target").exists());
                if healthy_sources.contains(&id) {
                    let completion = IngressCompletion::decode(
                        traffic_record(&directory, "old", "completion-0", 1)
                            .await?
                            .as_slice(),
                    )?;
                    assert_eq!(completion.record_id, 1);
                    assert_eq!(completion.status(), IngressCompletionStatus::Ok);
                }
            }
            for sink in case
                .flows
                .values()
                .map(|[_, sink]| sink)
                .collect::<std::collections::BTreeSet<_>>()
            {
                // The original VMs' counts advance; target Source Completions are still held.
                let mut expected: Vec<_> = case
                    .flows
                    .values()
                    .filter(|[source, target]| {
                        target == sink
                            && !(case.current_phases[source] == "backoff"
                                && case.current_phases[sink] == "ready")
                    })
                    .map(|[source, _]| format!("{source}-target:2"))
                    .collect();
                let offset = if case.current_phases[sink] == "backoff" {
                    expected.len()
                } else {
                    0
                };
                let mut received = Vec::new();
                for ordinal in 1..=expected.len() {
                    received.push(
                        traffic_payload(
                            &instance_directory(root, sink)?,
                            "target",
                            offset + ordinal,
                        )
                        .await?,
                    );
                }
                received.sort();
                expected.sort();
                assert_eq!(received, expected);
            }
            for id in ["unrelated", "unrelated-sink"] {
                assert_eq!(recorded_pid(&instance_directory(root, id)?)?, before[id]);
            }
            submit_value(&mut unrelated_source, 901, "after-handoff")?;
            assert_eq!(
                traffic_payload(&instance_directory(root, "unrelated-sink")?, "old", 2).await?,
                "after-handoff"
            );
            Ok(())
        })
        .await?;
    }
    Ok(())
}

fn configuration_revision(
    root: &Path,
    case: &ConfigurationCase,
    session: &str,
) -> TestResult<PipelineRevisionPlan> {
    let mut revision = revision(root.parent().ok_or("Fixture parent is missing")?, "normal")?;
    let mut instances = serde_json::Map::new();
    let mut flows = serde_json::Map::new();
    for id in case
        .current_phases
        .keys()
        .map(String::as_str)
        .chain(["unrelated", "unrelated-sink"])
    {
        let program = match id {
            "archive" | "unrelated-sink" => "com.example.archive",
            "unrelated" => "com.example.input",
            _ => "com.example.dual",
        };
        let selected_session = if case
            .replaced_instances
            .iter()
            .any(|selected| selected == id)
        {
            session
        } else {
            "old"
        };
        let submissions = if case.flows.values().any(|[source, _]| source == id) {
            vec![
                json!({"channelIndex": 0, "recordId": if selected_session == "old" { 1 } else { 2 }, "payload": TestPayload { value: format!("{id}-{selected_session}") }.encode_to_vec()}),
            ]
        } else {
            Vec::new()
        };
        let mut value = instance(program, id);
        value["config"]["traffic"] = json!({
            "session": selected_session, "submissions": submissions,
            "completionGate": format!("allow-completions-{selected_session}"),
            "egressGate": if selected_session == "old" && id != "unrelated-sink" { Some("allow-egress-old") } else { None }
        });
        instances.insert(id.to_owned(), value);
    }
    for (id, [source, sink]) in &case.flows {
        let contract = if sink == "archive" {
            "com.example.archive"
        } else {
            "com.example.dual"
        };
        let mut value = flow(source, 1, &[sink]);
        value["process"]["script"] = json!(format!(
            "local b = registry:getBuilder('{contract}@1.0.0'); local count = 0; function main(event) count = count + 1; b:setValue(event.payload.value .. ':' .. count); emit(b:build()) end"
        ));
        flows.insert(id.clone(), value);
    }
    let mut unrelated = flow("unrelated", 1, &["unrelated-sink"]);
    unrelated["process"]["script"] = json!(
        "local b = registry:getBuilder('com.example.archive@1.0.0'); function main(event) b:setValue(event.payload.value); emit(b:build()) end"
    );
    flows.insert("unrelated-flow".into(), unrelated);
    revision.tenon_document_json = serde_json::to_string(
        &json!({"specVersion":"1", "id":"configuration-handoff", "name":"Configuration handoff", "pluginInstances":instances, "flows":flows}),
    )?;
    revision.document_etag = session.to_owned();
    revision.plugin_programs.retain(|program| {
        instances
            .values()
            .any(|instance| instance["programName"] == program.program_name)
    });
    Ok(revision)
}

fn set_retry_delay(reconfigurer: &mut Reconfigurer, root: &Path, delay_ms: u64) -> TestResult {
    let mut bootstrap = bootstrap(root)?;
    let backoff = bootstrap
        .environment
        .as_mut()
        .and_then(|environment| environment.retry_backoff.as_mut())
        .ok_or("Fixture retry backoff is missing")?;
    backoff.initial_delay_ms = delay_ms;
    backoff.maximum_delay_ms = delay_ms;
    let environment = bootstrap
        .environment
        .ok_or("Fixture environment is missing")?;
    reconfigurer.environment = Arc::new(environment);
    Ok(())
}

fn assert_running(process_id: Pid) -> TestResult {
    test_kill_process(process_id)
        .map_err(|error| format!("Controlled child is gone: {error}").into())
}

fn pids_for(root: &Path, ids: &[&str]) -> TestResult<BTreeMap<String, Pid>> {
    ids.iter()
        .map(|id| {
            Ok((
                (*id).to_owned(),
                recorded_pid(&instance_directory(root, id)?)?,
            ))
        })
        .collect()
}

fn configuration_queue_files(
    root: &Path,
    ids: &[&str],
) -> TestResult<BTreeMap<PathBuf, (u64, u64)>> {
    let mut files = BTreeMap::new();
    for id in ids {
        for kind in ["source", "sink"] {
            let directory = instance_directory(root, id)?.join(kind);
            if directory.exists() {
                for entry in std::fs::read_dir(directory)? {
                    let path = entry?.path();
                    let metadata = path.metadata()?;
                    files.insert(path, (metadata.dev(), metadata.ino()));
                }
            }
        }
    }
    Ok(files)
}

pub(super) async fn traffic_record(
    directory: &Path,
    session: &str,
    queue: &str,
    ordinal: usize,
) -> TestResult<Vec<u8>> {
    let path = directory.join(format!("traffic-{session}-{queue}.received"));
    timeout(TEST_DEADLINE, async {
        loop {
            if let Ok(text) = std::fs::read_to_string(&path)
                && let Some(line) = text
                    .split_inclusive('\n')
                    .filter(|line| line.ends_with('\n'))
                    .nth(ordinal - 1)
            {
                return Ok(STANDARD.decode(line.trim_end())?);
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|error| {
        format!(
            "Timed out reading record {ordinal} from {}: {error}",
            path.display()
        )
    })?
}

async fn traffic_payload(directory: &Path, session: &str, ordinal: usize) -> TestResult<String> {
    let record = EgressRecord::decode(
        traffic_record(directory, session, "egress", ordinal)
            .await?
            .as_slice(),
    )?;
    Ok(TestPayload::decode(record.payload.as_slice())?.value)
}

fn submit_value(writer: &mut QueueWriter, record_id: u64, value: &str) -> TestResult {
    let record = IngressRecord {
        record_id,
        payload: TestPayload {
            value: value.to_owned(),
        }
        .encode_to_vec()
        .into(),
    };
    match writer.try_write_with(
        record.encoded_len(),
        |destination| record.encode(destination).map_err(std::io::Error::other),
        || {},
    )? {
        WriteOutcome::Committed(_) => Ok(()),
        WriteOutcome::Full => Err("Fixture Submission Queue is full".into()),
    }
}
