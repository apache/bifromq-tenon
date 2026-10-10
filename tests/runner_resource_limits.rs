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

//! Resource-limit behavior through real Runner HTTP and child processes.

use plugin_fixture::file_tree;
#[path = "runner_cli/plugin_fixture.rs"]
mod plugin_fixture;
#[path = "support/runner_http.rs"]
mod runner_http_support;

#[cfg(target_os = "linux")]
#[path = "runner_resource_limits/linux.rs"]
mod linux;

use plugin_fixture::{PluginInterface, install_program};
use runner_http_support::{
    TestRunner, available_address, request, wait_for_http, wait_until, write_config,
};
use serde_json::{Value, json};
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::Path;

fn document(limits: Value) -> Value {
    json!({
        "specVersion":"1", "id":"limited", "resourceLimits":limits,
        "pluginInstances":{"gateway":{"programName":"com.example.gateway","exactVersion":"1.0.0","config":{}}},
        "flows":{"loop":{"source":"gateway","process":{"script":"function main(event) emit() end"},"sinks":["gateway"]}}
    })
}

fn put(address: SocketAddr, document: &Value, etag: Option<&str>) -> io::Result<String> {
    let condition = etag.map_or(("If-None-Match", "*"), |etag| ("If-Match", etag));
    let response = request(
        address,
        "PUT",
        &format!(
            "/documents/{}",
            document["id"]
                .as_str()
                .ok_or_else(|| io::Error::other("Document identity is missing"))?
        ),
        &[("Content-Type", "application/jsonc"), condition],
        &serde_json::to_vec(document)?,
    )?;
    assert_eq!(
        response.status,
        if etag.is_some() { 204 } else { 201 },
        "{}",
        response.body_text()
    );
    Ok(response.headers["etag"].clone())
}

fn details(address: SocketAddr) -> io::Result<Value> {
    let response = request(address, "GET", "/pipelines/limited", &[], &[])?;
    assert_eq!(response.status, 200, "{}", response.body_text());
    Ok(response.json())
}

fn wait_applied(address: SocketAddr, etag: &str) -> io::Result<()> {
    wait_until(|| {
        let state = details(address)?;
        Ok((state["appliedDocumentEtag"] == etag
            && state["pluginInstances"][0]["state"] == "running")
            .then_some(()))
    })
    .map_err(|error| io::Error::other(format!("{error}; Pipeline details: {:?}", details(address))))
}

fn pipeline_pid(state: &Path) -> io::Result<u32> {
    let files = file_tree::named_files(&state.join("pipelines"), "parent.pid")?;
    let path = files
        .first()
        .ok_or_else(|| io::Error::other("Plugin parent PID was not written"))?;
    fs::read_to_string(path)?
        .trim()
        .parse()
        .map_err(io::Error::other)
}

#[cfg(target_os = "macos")]
#[test]
fn macos_ignores_hard_limits_and_preserves_channels_and_plugin_process() -> io::Result<()> {
    let state = tempfile::tempdir()?;
    install_program(state.path(), PluginInterface::SourceAndSink)?;
    let address = available_address()?;
    let config = write_config(state.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    let mut desired = document(json!({"cpu":0.5,"memoryBytes":536870912}));
    desired["flows"]["loop"]["parallelism"] = json!(2);
    desired["flows"]["loop"]["process"]["script"] = json!(
        "local b = registry:getBuilder('com.example.gateway@1.0.0'); function main(event) emit(b:build()) end"
    );
    desired["pluginInstances"]["gateway"]["config"] = json!({"traffic":{"session":"channels"}});
    let first = put(address, &desired, None)?;
    wait_applied(address, &first)?;
    let pid = pipeline_pid(state.path())?;
    let plugin_path = file_tree::named_files(&state.path().join("pipelines"), "process.pid")?
        .into_iter()
        .next()
        .ok_or_else(|| io::Error::other("Plugin PID is missing"))?;
    let plugin_pid = fs::read_to_string(&plugin_path)?;
    let channels = 2 * std::thread::available_parallelism()?.get();
    assert_eq!(
        file_tree::named_files(
            &state.path().join("pipelines"),
            &format!("submission-{}.queue", channels - 1)
        )?
        .len(),
        1
    );
    assert!(
        file_tree::named_files(
            &state.path().join("pipelines"),
            &format!("submission-{channels}.queue")
        )?
        .is_empty()
    );
    assert_eq!(
        details(address)?["resourceLimits"],
        json!({"state":"ignored","reason":"platform_unsupported"})
    );
    transmit_on_all_channels(state.path(), channels)?;
    desired["resourceLimits"] = json!({"cpu":1.5,"memoryBytes":1});
    let second = put(address, &desired, Some(&first))?;
    wait_applied(address, &second)?;
    assert_eq!(pipeline_pid(state.path())?, pid);
    assert_eq!(fs::read_to_string(&plugin_path)?, plugin_pid);
    transmit_on_all_channels(state.path(), channels)?;
    desired["pluginInstances"]["gateway"]["exactVersion"] = json!("2.0.0");
    desired["resourceLimits"] = json!({});
    put(address, &desired, Some(&second))?;
    let latest = details(address)?;
    assert_eq!(latest["state"], "unready");
    assert_eq!(latest["appliedDocumentEtag"], second);
    assert_eq!(latest["resourceLimits"]["state"], "ignored");
    runner.terminate()?;
    assert!(file_tree::named_files(&state.path().join("pipelines"), "parent.pid")?.is_empty());
    Ok(())
}

#[cfg(target_os = "macos")]
fn transmit_on_all_channels(state: &Path, channels: usize) -> io::Result<()> {
    use prost::Message as _;
    use tenon::runner_test_support::contracts::source::IngressRecord;
    use tenon::runner_test_support::{flow_channel_bell_path, loops_bell_path};
    use tenon_ipc::queue::contract_test_support::open_writer;
    use tenon_ipc::queue::{QueueWriter, WriteOutcome};

    let paths = file_tree::named_files(&state.join("pipelines"), "submission-0.queue")?;
    let source = paths
        .first()
        .and_then(|path| path.parent())
        .ok_or_else(|| io::Error::other("Source directory is missing"))?;
    let plugin_directory = source
        .parent()
        .ok_or_else(|| io::Error::other("Plugin directory is missing"))?;
    let pipeline_directory = plugin_directory
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| io::Error::other("Pipeline directory is missing"))?;
    let log = plugin_directory.join("traffic-channels-egress.received");
    let before = if log.try_exists()? {
        fs::read_to_string(&log)?.lines().count()
    } else {
        0
    };
    let record = IngressRecord {
        record_id: 1,
        payload: Vec::new().into(),
    }
    .encode_to_vec();
    for channel in 0..channels {
        // The test plays the Source side: slot 0 of the Source loop's own region
        // is the one Submission doorbell every Channel's writer publishes, and
        // its commit rings the Flow Channel region the `gateway` Flow's Channels
        // park in.
        let mut writer: QueueWriter = open_writer(
            &source.join(format!("submission-{channel}.queue")),
            &loops_bell_path(source),
            0,
            &flow_channel_bell_path(pipeline_directory, "loop"),
        )?;
        assert!(matches!(
            writer
                .try_write_with(
                    record.len(),
                    |destination| std::io::Write::write_all(destination, &record),
                    || {}
                )
                .map_err(io::Error::other)?,
            WriteOutcome::Committed(_)
        ));
    }
    wait_until(|| {
        Ok(
            (log.try_exists()? && fs::read_to_string(&log)?.lines().count() == before + channels)
                .then_some(()),
        )
    })
}

#[cfg(target_os = "linux")]
#[test]
fn linux_without_delegation_reports_failure_and_keeps_unlimited_documents_working() -> io::Result<()>
{
    let state = tempfile::tempdir()?;
    install_program(state.path(), PluginInterface::SourceAndSink)?;
    let address = available_address()?;
    let config = write_config(state.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    let first = put(address, &document(json!({"cpu":0.5})), None)?;
    wait_until(|| {
        Ok(
            (details(address)?["lastError"]["code"] == "resource_limits_apply_failed")
                .then_some(()),
        )
    })?;
    let failed = details(address)?;
    assert!(failed.get("appliedDocumentEtag").is_none());
    assert!(failed.get("resourceLimits").is_none());
    assert_eq!(failed["lastError"]["documentEtag"], first);
    assert!(file_tree::named_files(&state.path().join("pipelines"), "parent.pid")?.is_empty());
    let fixed = put(address, &document(json!({})), Some(&first))?;
    wait_applied(address, &fixed)?;
    assert!(details(address)?.get("lastError").is_none());
    runner.terminate()?;
    assert!(
        runner
            .read_stderr()?
            .contains("runner.resource_limits_unavailable")
    );
    Ok(())
}
