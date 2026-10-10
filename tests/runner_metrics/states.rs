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
use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;

#[test]
fn multiple_objects_report_transitions_and_only_successful_automatic_spawns() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = configure(directory.path(), address)?;
    let mut settings: serde_json::Value = serde_json::from_slice(&fs::read(&config)?)?;
    settings["pipeline"]["retryBackoff"] = json!({"initialDelayMs":1000,"maximumDelayMs":1000});
    fs::write(&config, serde_json::to_vec(&settings)?)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;

    // A package-owned executable lets the test cause a real exec permission failure.
    let command = plugin_fixture::controlled_program_command(PluginInterface::SourceAndSink)?;
    let quoted = command
        .iter()
        .map(|arg| format!("'{}'", arg.replace('\'', "'\\''")))
        .collect::<Vec<_>>()
        .join(" ");
    let script = format!("#!/bin/sh\nexec {quoted} \"$@\"\n");
    let package = plugin_fixture::plugin_package_with_program(
        PluginInterface::SourceAndSink,
        script.as_bytes(),
    )?;
    assert_eq!(
        request(
            address,
            "POST",
            "/plugins",
            &[("Content-Type", "application/octet-stream")],
            &package
        )?
        .status,
        201
    );

    let mut alpha = document();
    alpha["id"] = json!("alpha");
    alpha["pluginInstances"]["gateway"]["config"] =
        json!({"endpoint":"alpha","behavior":"delay-ready"});
    alpha["pluginInstances"]["other"] = alpha["pluginInstances"]["gateway"].clone();
    alpha["pluginInstances"]["other"]["config"] =
        json!({"endpoint":"alpha-other","behavior":"exit-before-ready"});
    alpha["flows"]["other"] = alpha["flows"]["loop"].clone();
    alpha["flows"]["other"]["source"] = json!("other");
    alpha["flows"]["other"]["sinks"] = json!(["other"]);
    let mut beta = document();
    beta["id"] = json!("beta");
    beta["pluginInstances"]["gateway"]["config"] =
        json!({"endpoint":"beta","behavior":"delay-quiesce"});
    for document in [&alpha, &beta] {
        assert_eq!(
            request(
                address,
                "PUT",
                &format!("/documents/{}", document["id"].as_str().unwrap_or_default()),
                &[
                    ("If-None-Match", "*"),
                    ("Content-Type", "application/jsonc")
                ],
                &serde_json::to_vec(document)?
            )?
            .status,
            201
        );
    }
    wait_plugin_values(
        address,
        "alpha",
        "tenon.plugin.state",
        &[("gateway", 0.0), ("other", 2.0)],
    )?;
    wait_plugin_values(address, "beta", "tenon.plugin.state", &[("gateway", 1.0)])?;
    let states = wait_resource(address, "tenon.runner", |r| {
        object_values(r, "tenon.pipeline.state", "tenon.pipeline.id")
            == BTreeMap::from([("alpha".into(), 3.0), ("beta".into(), 3.0)])
    })?;
    assert_eq!(
        object_values(
            &states,
            "tenon.pipeline.configuration.applied",
            "tenon.pipeline.id"
        ),
        BTreeMap::from([("alpha".into(), 1.0), ("beta".into(), 1.0)])
    );
    let alpha_instance = instance_directory(directory.path(), "alpha")?;
    fs::write(alpha_instance.join("allow-ready"), [])?;
    wait_plugin_values(
        address,
        "alpha",
        "tenon.plugin.state",
        &[("gateway", 1.0), ("other", 2.0)],
    )?;

    // The old beta instance holds reconfiguration until Source quiesce completes.
    let beta_instance = instance_directory(directory.path(), "beta")?;
    let fetched = request(address, "GET", "/documents/beta", &[], &[])?;
    beta["pluginInstances"]["gateway"]["config"] = json!({"endpoint":"beta-updated"});
    assert_eq!(
        request(
            address,
            "PUT",
            "/documents/beta",
            &[
                ("If-Match", &fetched.headers["etag"]),
                ("Content-Type", "application/jsonc")
            ],
            &serde_json::to_vec(&beta)?
        )?
        .status,
        204
    );
    wait_until(|| {
        Ok(beta_instance
            .join("quiesce-source.received")
            .exists()
            .then_some(()))
    })?;
    let updating = wait_resource(address, "tenon.runner", |r| {
        object_values(r, "tenon.pipeline.state", "tenon.pipeline.id").get("beta") == Some(&2.0)
    })?;
    assert_eq!(
        object_values(
            &updating,
            "tenon.pipeline.configuration.applied",
            "tenon.pipeline.id"
        )
        .get("beta"),
        Some(&0.0)
    );
    assert_eq!(
        object_values(&updating, "tenon.pipeline.state", "tenon.pipeline.id").get("alpha"),
        Some(&3.0)
    );
    fs::write(beta_instance.join("allow-quiesce"), [])?;
    wait_resource(address, "tenon.runner", |r| {
        object_values(r, "tenon.pipeline.state", "tenon.pipeline.id").get("beta") == Some(&3.0)
            && object_values(
                r,
                "tenon.pipeline.configuration.applied",
                "tenon.pipeline.id",
            )
            .get("beta")
                == Some(&1.0)
    })?;
    let beta_running =
        wait_plugin_values(address, "beta", "tenon.plugin.state", &[("gateway", 1.0)])?;
    assert!(points(&beta_running, "tenon.plugin.restarts").is_empty());

    kill(read_pid(&alpha_instance)?)?;
    wait_plugin_values(
        address,
        "alpha",
        "tenon.plugin.state",
        &[("gateway", 3.0), ("other", 2.0)],
    )?;
    let restarted = wait_plugin_values(
        address,
        "alpha",
        "tenon.plugin.state",
        &[("gateway", 1.0), ("other", 2.0)],
    )?;
    assert_eq!(
        object_values(
            &restarted,
            "tenon.plugin.restarts",
            "tenon.plugin.instance.id"
        ),
        BTreeMap::from([("gateway".into(), 1.0)])
    );
    let executable = plugin_fixture::runtime_program_directory(
        directory.path(),
        "com.example.gateway",
        "1.0.0",
    )?
    .join("plugin.sh");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o600))?;
    kill(read_pid(&alpha_instance)?)?;
    wait_plugin_values(
        address,
        "alpha",
        "tenon.plugin.state",
        &[("gateway", 3.0), ("other", 2.0)],
    )?;
    let failed = wait_plugin_values(
        address,
        "alpha",
        "tenon.plugin.state",
        &[("gateway", 2.0), ("other", 2.0)],
    )?;
    assert_eq!(
        object_values(&failed, "tenon.plugin.restarts", "tenon.plugin.instance.id"),
        BTreeMap::from([("gateway".into(), 1.0)])
    );
    let beta_running =
        wait_plugin_values(address, "beta", "tenon.plugin.state", &[("gateway", 1.0)])?;
    assert!(points(&beta_running, "tenon.plugin.restarts").is_empty());
    runner.terminate()?;
    Ok(())
}

fn wait_plugin_values(
    address: SocketAddr,
    pipeline: &str,
    metric: &str,
    expected: &[(&str, f64)],
) -> io::Result<Value> {
    let expected = expected
        .iter()
        .map(|(id, value)| ((*id).to_owned(), *value))
        .collect();
    wait_resource(address, "tenon.pipeline", |resource| {
        resource_attribute(resource, "tenon.pipeline.id") == pipeline
            && object_values(resource, metric, "tenon.plugin.instance.id") == expected
    })
}

fn object_values(resource: &Value, metric: &str, key: &str) -> BTreeMap<String, f64> {
    let points = points(resource, metric);
    let values: BTreeMap<_, _> = points
        .iter()
        .map(|point| {
            (
                attribute(&point["attributes"], key)
                    .unwrap_or_default()
                    .to_owned(),
                point_value(point),
            )
        })
        .collect();
    assert_eq!(values.len(), points.len(), "duplicate object samples");
    values
}

fn instance_directory(root: &Path, endpoint: &str) -> io::Result<PathBuf> {
    let mut result = None;
    wait_until(|| {
        for file in file_tree::named_files(root, "config.received")? {
            let config: serde_json::Value = serde_json::from_slice(&fs::read(&file)?)?;
            if config["endpoint"] == endpoint {
                result = file.parent().map(Path::to_path_buf);
                return Ok(Some(()));
            }
        }
        Ok(None)
    })?;
    result.ok_or_else(|| io::Error::other("instance path has no parent"))
}

fn read_pid(instance: &Path) -> io::Result<i32> {
    fs::read_to_string(instance.join("process.pid"))?
        .trim()
        .parse()
        .map_err(io::Error::other)
}
