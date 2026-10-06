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

#[test]
fn real_pipeline_flow_metrics_survive_replacement_and_retire_with_the_flow() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = configure(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    assert_eq!(
        request(
            address,
            "POST",
            "/plugins",
            &[("Content-Type", "application/octet-stream")],
            &plugin_package(PluginInterface::SourceAndSink)?
        )?
        .status,
        201
    );
    let mut original = document();
    original["pluginInstances"]["gateway"]["config"] = json!({"traffic": {
        "session":"metrics", "egressGate":"release-egress", "submissions":[
            {"channelIndex":0,"recordId":1,"payload":[8,1]},
            {"channelIndex":0,"recordId":2,"payload":[]},
            {"channelIndex":0,"recordId":3,"payload":[255]}
        ]
    }});
    original["flows"]["loop"]["process"]["script"] = json!(
        r#"
        local builder = registry:getBuilder("com.example.gateway@1.0.0")
        function main(event)
            emit(builder:build())
            emit()
            error("script failed after accepting output")
        end
    "#
    );
    assert_eq!(
        request(
            address,
            "PUT",
            "/documents/observed",
            &[
                ("If-None-Match", "*"),
                ("Content-Type", "application/jsonc")
            ],
            &serde_json::to_vec(&original)?
        )?
        .status,
        201
    );
    wait_running(address)?;
    // The Channel never blocks on an unreleased Sink: it accepts every record
    // the Source already committed, withholds the Completions their output
    // still owes, and rests in its own doorbell park, which reports `idle`.
    let blocked = wait_resource(address, "tenon.pipeline", |resource| {
        value(resource, "tenon.flow.waiting") == Some(5.0)
            && value(resource, "tenon.flow.input.records") == Some(3.0)
    })?;
    assert_eq!(value(&blocked, "tenon.flow.egress.records"), Some(2.0));
    assert_eq!(value(&blocked, "tenon.flow.egress.bytes"), Some(0.0));
    assert_eq!(
        phase_value(&blocked, "tenon.flow.errors", "lua_main"),
        Some(2.0)
    );
    assert_eq!(
        phase_value(&blocked, "tenon.flow.errors", "decode"),
        Some(1.0)
    );
    // Only the record the decoder rejected completed: the two accepted records
    // keep their Source Completions until the Sink releases their output.
    let withheld = points(&blocked, "tenon.flow.completion.records");
    assert_eq!(withheld.iter().map(point_value).sum::<f64>(), 1.0);
    assert!(
        withheld
            .iter()
            .all(|point| attribute(&point["attributes"], "result") == Some("error"))
    );
    let identity = resource_attribute(&blocked, "service.instance.id").to_owned();
    let markers = file_tree::named_files(directory.path(), "traffic-metrics-submitted.received")?;
    let root = markers
        .first()
        .and_then(|path| path.parent())
        .ok_or_else(|| io::Error::other("traffic marker missing"))?;
    fs::write(root.join("release-egress"), [])?;
    let completed = wait_resource(address, "tenon.pipeline", |resource| {
        value(resource, "tenon.flow.input.records") == Some(3.0) && completions_sum(resource) == 3.0
    })?;
    assert_eq!(value(&completed, "tenon.flow.input.bytes"), Some(3.0));
    assert_eq!(value(&completed, "tenon.flow.egress.records"), Some(2.0));
    assert_eq!(value(&completed, "tenon.flow.egress.bytes"), Some(0.0));
    let completions = points(&completed, "tenon.flow.completion.records");
    assert_eq!(completions.iter().map(point_value).sum::<f64>(), 3.0);
    assert_eq!(
        completions
            .iter()
            .find(|point| attribute(&point["attributes"], "result") == Some("error"))
            .map(point_value),
        Some(1.0)
    );
    assert_eq!(histogram_count(&completed, "tenon.flow.lua.duration"), 2);
    assert!(histogram_count(&completed, "tenon.flow.wait.duration") >= 1);
    wait_until(|| {
        let source_results =
            file_tree::named_files(directory.path(), "traffic-metrics-completion-0.received")?;
        Ok(source_results
            .first()
            .is_some_and(|path| {
                fs::read_to_string(path).is_ok_and(|text| text.lines().count() == 3)
            })
            .then_some(()))
    })?;
    let mut retired = original.clone();
    retired["flows"] = json!({"retained": {"source":"gateway","process":{"script":"function main(event) emit() end"},"sinks":["gateway"]}});
    retired["pluginInstances"]["gateway"]["config"] = json!({});
    update_document(address, &retired)?;
    let removed = wait_resource(address, "tenon.pipeline", |resource| {
        resource_attribute(resource, "service.instance.id") == identity
            && flow_points(resource, "tenon.flow.lua.memory", "loop").is_empty()
            && flow_points(resource, "tenon.flow.waiting", "loop").is_empty()
    })?;
    assert_eq!(value(&removed, "tenon.flow.input.records"), Some(3.0));
    assert!(flow_points(&removed, "tenon.queue.usage", "loop").is_empty());
    original["pluginInstances"]["gateway"]["config"]["traffic"]["egressGate"] = json!(null);
    update_document(address, &original)?;
    let recreated = wait_resource(address, "tenon.pipeline", |resource| {
        resource_attribute(resource, "service.instance.id") == identity
            && value(resource, "tenon.flow.input.records") == Some(6.0)
            && completions_sum(resource) == 6.0
    })?;
    assert_eq!(histogram_count(&recreated, "tenon.flow.lua.duration"), 4);
    assert!(value(&recreated, "tenon.flow.lua.memory").is_some_and(|bytes| bytes > 0.0));
    runner.terminate()?;
    Ok(())
}

fn completions_sum(process: &Value) -> f64 {
    points(process, "tenon.flow.completion.records")
        .iter()
        .map(point_value)
        .sum()
}

fn phase_value(process: &Value, name: &str, phase: &str) -> Option<f64> {
    points(process, name)
        .iter()
        .find(|point| attribute(&point["attributes"], "phase") == Some(phase))
        .map(point_value)
}

fn update_document(address: SocketAddr, document: &serde_json::Value) -> io::Result<()> {
    let current = request(address, "GET", "/documents/observed", &[], &[])?;
    let updated = request(
        address,
        "PUT",
        "/documents/observed",
        &[
            ("If-Match", &current.headers["etag"]),
            ("Content-Type", "application/jsonc"),
        ],
        &serde_json::to_vec(document)?,
    )?;
    assert_eq!(updated.status, 204, "{}", updated.body_text());
    wait_running(address)
}

fn histogram_count(resource: &Value, name: &str) -> u64 {
    points(resource, name)
        .iter()
        .filter_map(|point| point["count"].as_str()?.parse::<u64>().ok())
        .sum()
}

fn flow_points<'a>(resource: &'a Value, name: &str, flow: &str) -> Vec<&'a Value> {
    points(resource, name)
        .iter()
        .filter(|point| attribute(&point["attributes"], "tenon.flow.id") == Some(flow))
        .collect()
}
