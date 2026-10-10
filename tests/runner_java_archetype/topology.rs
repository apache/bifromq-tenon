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

//! Public Runner proofs for real generated Plugin routing and pressure isolation.

use super::*;

pub(super) fn run_dual_interface_routes(bundle: &[u8]) -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let java_free_path = tempfile::tempdir()?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn_with_path(&config, java_free_path.path())?;
    wait_for_http(&mut runner, address)?;
    install_bundle(address, bundle)?;
    let output_a = directory.path().join("A.txt");
    let output_b = directory.path().join("B.txt");
    let document = serde_json::json!({
        "specVersion": "1", "id": "dual-routes",
        "pluginInstances": {
            "A": {"programName": "com.example.plugin.source-and-sink", "exactVersion": "1.0.0",
                "config": {"message": "from-A", "queueIndex": 0, "outputFile": output_a}},
            "B": {"programName": "com.example.plugin.source-and-sink", "exactVersion": "1.0.0",
                "config": {"message": "from-B", "queueIndex": 0, "outputFile": output_b}}
        },
        "flows": {
            "from-a": {"parallelism": 1, "source": "A", "maxPendingRecords": 1, "maxRecordBytes": 1024,
                "process": {"script": forward_script("com.example.plugin.source-and-sink@1.0.0")},
                "sinks": ["A", "B"]},
            "from-b": {"parallelism": 1, "source": "B", "maxPendingRecords": 1, "maxRecordBytes": 1024,
                "process": {"script": forward_script("com.example.plugin.source-and-sink@1.0.0")},
                "sinks": ["A"]}
        }
    });
    let created = request(
        address,
        "PUT",
        "/documents/dual-routes",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        &serde_json::to_vec(&document)?,
    )?;
    assert_eq!(created.status, 201, "{}", created.body_text());
    wait_for_messages(&output_a, &["from-A", "from-B"])?;
    wait_for_messages(&output_b, &["from-A"])?;
    // Data delivery can precede the asynchronous Instance status report.
    wait_until(|| {
        let status = request(address, "GET", "/pipelines/dual-routes", &[], &[])?.json();
        let Some(instances) = status["pluginInstances"].as_array() else {
            return Ok(None);
        };
        let mut ids = instances
            .iter()
            .map(|instance| instance["id"].as_str().unwrap_or(""))
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, ["A", "B"]);
        assert!(
            instances.iter().all(|instance| {
                instance["state"] == "starting" || instance["state"] == "running"
            }),
            "unexpected Instance failure: {status}"
        );
        Ok(instances
            .iter()
            .all(|instance| instance["state"] == "running")
            .then_some(()))
    })?;
    verify_java_process_resources(address, &["A", "B"])?;
    runner.terminate()?;
    assert_eq!(messages(&output_a)?, ["from-A", "from-B"]);
    assert_eq!(messages(&output_b)?, ["from-A"]);
    Ok(())
}

pub(super) fn run_disjoint_pressure_routes(packages: &GeneratedJavaPackages) -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let java_free_path = tempfile::tempdir()?;
    let address = available_address()?;
    let bundles = [
        &packages.capacity_source,
        &packages.gated_sink,
        &packages.source,
        &packages.sink,
    ];
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn_with_path(&config, java_free_path.path())?;
    wait_for_http(&mut runner, address)?;
    for bundle in bundles {
        install_bundle(address, bundle)?;
    }
    let slow_output = directory.path().join("slow.txt");
    let fast_output = directory.path().join("fast.txt");
    let mut document: serde_json::Value = serde_json::from_str(&generated_java_document(
        "disjoint-pressure",
        &slow_output,
        DeliveryScenario::CapacitySaturation,
    )?)?;
    document["pluginInstances"]["fast-source"] = serde_json::json!({
        "programName": "com.example.plugin.source", "exactVersion": "1.0.0",
        "config": {"message": "fast-before-pressure", "queueIndex": 0}
    });
    document["pluginInstances"]["fast-sink"] = serde_json::json!({
        "programName": "com.example.plugin.sink", "exactVersion": "1.0.0",
        "config": {"outputFile": fast_output}
    });
    document["flows"]["fast"] = serde_json::json!({
        "parallelism": 1, "source": "fast-source", "maxPendingRecords": 1, "maxRecordBytes": 1024,
        "process": {"script": forward_script("com.example.plugin.sink@1.0.0")},
        "sinks": ["fast-sink"]
    });
    let original = document.to_string();
    let path = "/documents/disjoint-pressure";
    let created = request(
        address,
        "PUT",
        path,
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        original.as_bytes(),
    )?;
    assert_eq!(created.status, 201, "{}", created.body_text());
    verify_sink_holds_source_completion(directory.path(), &original, &slow_output)?;
    wait_for_source_results(&slow_output, "backpressure\n")?;
    wait_for_messages(&fast_output, &["fast-before-pressure"])?;
    let instances = java_instance_root(directory.path(), &original)?
        .ok_or_else(|| io::Error::other("Instance root is missing"))?;
    let completion = instances
        .join(
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(b"fast-source")),
        )
        .join("source/completion-0.queue");
    wait_until(|| Ok(queue_commit_was_released(&completion)?.then_some(())))?;
    let fetched = request(address, "GET", path, &[], &[])?;
    document["pluginInstances"]["fast-source"]["config"]["message"] =
        serde_json::json!("fast-during-pressure");
    let updated = request(
        address,
        "PUT",
        path,
        &[
            ("Content-Type", "application/jsonc"),
            ("If-Match", &fetched.headers["etag"]),
        ],
        &serde_json::to_vec(&document)?,
    )?;
    assert_eq!(updated.status, 204, "{}", updated.body_text());
    wait_for_messages(
        &fast_output,
        &["fast-before-pressure", "fast-during-pressure"],
    )?;
    // The slow Source still has no OK after the new, independent fast message
    // completed. Its queues were retained by the config-only fast replacement.
    verify_sink_holds_source_completion(directory.path(), &original, &slow_output)?;
    assert_eq!(
        fs::read_to_string(path_with_suffix(&slow_output, ".source-results"))?,
        "backpressure\n"
    );
    fs::write(
        path_with_suffix(&slow_output, ".write-release"),
        b"release\n",
    )?;
    wait_for_java_round_trip(
        directory.path(),
        &original,
        &slow_output,
        DeliveryScenario::CapacitySaturation,
    )?;
    wait_for_source_results(&slow_output, "backpressure\nok\n")?;
    runner.terminate()?;
    assert_eq!(
        messages(&fast_output)?,
        ["fast-before-pressure", "fast-during-pressure"]
    );
    Ok(())
}

fn install_bundle(address: SocketAddr, bundle: &[u8]) -> io::Result<()> {
    let installed = request(
        address,
        "POST",
        "/plugins",
        &[("Content-Type", "application/octet-stream")],
        bundle,
    )?;
    assert_eq!(installed.status, 201, "{}", installed.body_text());
    Ok(())
}

fn forward_script(contract: &str) -> String {
    format!(
        "local builder = registry:getBuilder('{contract}')\nfunction main(event)\n builder:setMessage(event.payload.message)\n emit(builder:build())\nend"
    )
}

fn wait_for_messages(path: &Path, expected: &[&str]) -> io::Result<()> {
    wait_until(|| {
        let actual = match messages(path) {
            Ok(actual) => actual,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        assert!(
            actual.len() <= expected.len(),
            "unexpected extra messages: {actual:?}"
        );
        Ok((actual == expected).then_some(()))
    })
}

fn messages(path: &Path) -> io::Result<Vec<String>> {
    let mut messages = fs::read_to_string(path)?
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    messages.sort();
    Ok(messages)
}
