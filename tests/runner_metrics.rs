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

//! Verifies HTTP metrics against real Runner and Plugin process behavior.

use plugin_fixture::file_tree;
#[path = "runner_cli/plugin_fixture.rs"]
mod plugin_fixture;
#[path = "support/runner_http.rs"]
mod runner_http_support;

#[path = "runner_metrics/flows.rs"]
mod flows;
#[path = "runner_metrics/performance.rs"]
mod performance;
#[path = "runner_metrics/states.rs"]
mod states;

use plugin_fixture::{PluginInterface, plugin_package};
use runner_http_support::{
    TestRunner, available_address, request, wait_for_http, wait_until, write_config,
};
use serde_json::Value;
use serde_json::json;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

#[test]
fn real_core_snapshots_follow_configuration_and_actual_process_restarts() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = configure(directory.path(), address)?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_tenon"));
    command
        .arg("--config")
        .arg(&config)
        .env("OTEL_SERVICE_NAME", "wrong-service")
        .env("OTEL_RESOURCE_ATTRIBUTES", "tenon.node.id=wrong-node")
        .env(
            "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
            "authorization=environment-secret,extra=environment-only",
        )
        .env(
            "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION",
            "invalid-compression",
        )
        .env("OTEL_METRIC_EXPORT_INTERVAL", "invalid-interval");
    let mut runner = TestRunner::spawn_command(command)?;
    wait_for_http(&mut runner, address)?;
    let empty = wait_resource(address, "tenon.runner", |resource| {
        !points(resource, "tenon.process.cpu").is_empty()
    })?;
    assert!(point_value(&points(&empty, "tenon.process.memory")[0]) > 0.0);
    assert!(points(&empty, "tenon.pipeline.state").is_empty());
    let mut document = document();
    let created = request(
        address,
        "PUT",
        "/documents/observed",
        &[
            ("If-None-Match", "*"),
            ("Content-Type", "application/jsonc"),
        ],
        &serde_json::to_vec(&document)?,
    )?;
    assert_eq!(created.status, 201, "{}", created.body_text());
    wait_resource(address, "tenon.runner", |r| {
        value(r, "tenon.pipeline.state") == Some(0.0)
            && value(r, "tenon.pipeline.configuration.applied") == Some(0.0)
    })?;
    let installed = request(
        address,
        "POST",
        "/plugins",
        &[("Content-Type", "application/octet-stream")],
        &plugin_package(PluginInterface::SourceAndSink)?,
    )?;
    assert_eq!(installed.status, 201, "{}", installed.body_text());
    wait_running(address)?;
    let running = wait_resource(address, "tenon.runner", |r| {
        value(r, "tenon.pipeline.state") == Some(3.0)
            && value(r, "tenon.pipeline.configuration.applied") == Some(1.0)
    })?;
    assert!(points(&running, "tenon.pipeline.restarts").is_empty());
    let pipeline = wait_resource(address, "tenon.pipeline", |r| {
        value(r, "tenon.plugin.state") == Some(1.0) && value(r, "tenon.pipeline.cpu").is_some()
    })?;
    assert_eq!(points(&pipeline, "tenon.plugin.state").len(), 1);
    assert!(points(&pipeline, "tenon.plugin.restarts").is_empty());
    let pipeline_identity = resource_attribute(&pipeline, "service.instance.id").to_owned();
    assert_eq!(
        resource_attribute(&pipeline, "tenon.pipeline.id"),
        "observed"
    );
    let plugin_pid = marker_pid(directory.path(), "process.pid")?;
    kill(plugin_pid)?;
    let restarted = wait_resource(address, "tenon.pipeline", |r| {
        value(r, "tenon.plugin.restarts") == Some(1.0)
            && value(r, "tenon.plugin.state") == Some(1.0)
    })?;
    assert_eq!(
        resource_attribute(&restarted, "service.instance.id"),
        pipeline_identity
    );
    assert_ne!(marker_pid(directory.path(), "process.pid")?, plugin_pid);
    let pipeline_pid = marker_pid(directory.path(), "parent.pid")?;
    kill(pipeline_pid)?;
    wait_resource(address, "tenon.runner", |r| {
        value(r, "tenon.pipeline.restarts") == Some(1.0)
            && value(r, "tenon.pipeline.state") == Some(3.0)
    })?;
    let replacement = wait_resource(address, "tenon.pipeline", |r| {
        resource_attribute(r, "service.instance.id") != pipeline_identity
            && value(r, "tenon.plugin.state") == Some(1.0)
    })?;
    assert!(points(&replacement, "tenon.plugin.restarts").is_empty());
    let fetched = request(address, "GET", "/documents/observed", &[], &[])?;
    document["pluginInstances"]["gateway"]["exactVersion"] = json!("2.0.0");
    let updated = request(
        address,
        "PUT",
        "/documents/observed",
        &[
            ("If-Match", &fetched.headers["etag"]),
            ("Content-Type", "application/jsonc"),
        ],
        &serde_json::to_vec(&document)?,
    )?;
    assert_eq!(updated.status, 204, "{}", updated.body_text());
    wait_resource(address, "tenon.runner", |r| {
        value(r, "tenon.pipeline.state") == Some(0.0)
            && value(r, "tenon.pipeline.configuration.applied") == Some(0.0)
    })?;
    let current = request(address, "GET", "/pipelines/observed", &[], &[])?.json();
    assert_eq!(current["state"], "unready");
    assert_eq!(current["pluginInstances"][0]["state"], "running");
    let configs = file_tree::named_files(directory.path(), "configs.received")?;
    for config in configs {
        assert_eq!(fs::read_to_string(config)?.trim(), "{}");
    }
    let fetched = request(address, "GET", "/documents/observed", &[], &[])?;
    let deleted = request(
        address,
        "DELETE",
        "/documents/observed",
        &[("If-Match", &fetched.headers["etag"])],
        &[],
    )?;
    assert_eq!(deleted.status, 204, "{}", deleted.body_text());
    wait_resource(address, "tenon.runner", |r| {
        points(r, "tenon.pipeline.state").is_empty()
            && points(r, "tenon.pipeline.configuration.applied").is_empty()
    })?;
    runner.terminate()?;
    let stderr = runner.read_stderr()?;
    assert!(
        !stderr.contains("secret"),
        "unexpected sensitive error output"
    );
    Ok(())
}

fn document() -> serde_json::Value {
    json!({"specVersion":"1","id":"observed","pluginInstances":{"gateway":{"programName":"com.example.gateway","exactVersion":"1.0.0","config":{}}},"flows":{"loop":{"source":"gateway","process":{"script":"function main(event) emit() end"},"sinks":["gateway"]}}})
}

fn configure(root: &Path, address: SocketAddr) -> io::Result<PathBuf> {
    let config = write_config(root, address)?;
    let mut settings: serde_json::Value = serde_json::from_slice(&fs::read(&config)?)?;
    settings["metrics"] = json!({"nodeId":"test-node","collectionTimeoutMs":200});
    fs::write(&config, serde_json::to_vec(&settings)?)?;
    Ok(config)
}

fn wait_running(address: SocketAddr) -> io::Result<()> {
    wait_until(|| {
        let state = request(address, "GET", "/pipelines/observed", &[], &[])?.json();
        Ok(
            (state["state"] == "running" && state["pluginInstances"][0]["state"] == "running")
                .then_some(()),
        )
    })
}

fn attribute<'a>(attributes: &'a Value, key: &str) -> Option<&'a str> {
    attributes[key].as_str()
}

fn resource_attribute<'a>(process: &'a Value, key: &str) -> &'a str {
    attribute(&process["resource"], key).unwrap_or_default()
}

fn points<'a>(process: &'a Value, name: &str) -> &'a [Value] {
    process["metrics"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|metric| metric["name"] == name)
        .and_then(|metric| metric["points"].as_array())
        .map_or(&[], Vec::as_slice)
}

fn point_value(point: &Value) -> f64 {
    point["value"]
        .as_f64()
        .or_else(|| point["value"].as_str().and_then(|value| value.parse().ok()))
        .unwrap_or(f64::NAN)
}

fn value(process: &Value, name: &str) -> Option<f64> {
    points(process, name).first().map(point_value)
}

fn wait_resource(
    address: SocketAddr,
    service: &str,
    inspect: impl Fn(&Value) -> bool,
) -> io::Result<Value> {
    let mut found = None;
    wait_until(|| {
        let response = request(address, "GET", "/metrics", &[], &[])?;
        assert_eq!(response.status, 200, "{}", response.body_text());
        let mut body = response.json();
        let processes = body["processes"]
            .as_array_mut()
            .ok_or_else(|| io::Error::other("Missing processes"))?;
        for process in processes.iter() {
            assert_eq!(resource_attribute(process, "tenon.node.id"), "test-node");
            assert_eq!(resource_attribute(process, "service.namespace"), "tenon");
            assert_eq!(
                resource_attribute(process, "service.version"),
                env!("CARGO_PKG_VERSION")
            );
            assert!(
                ["tenon.runner", "tenon.pipeline"]
                    .contains(&resource_attribute(process, "service.name"))
            );
        }
        found = processes
            .iter()
            .find(|process| {
                resource_attribute(process, "service.name") == service && inspect(process)
            })
            .cloned();
        Ok(found.as_ref().map(|_| ()))
    })?;
    found.ok_or_else(|| io::Error::other("Expected process metrics were not observed"))
}

fn marker_pid(root: &Path, name: &str) -> io::Result<i32> {
    let paths = file_tree::named_files(root, name)?;
    let path = paths
        .first()
        .ok_or_else(|| io::Error::other("process marker was not created"))?;
    fs::read_to_string(path)?
        .trim()
        .parse()
        .map_err(io::Error::other)
}
fn kill(pid: i32) -> io::Result<()> {
    let pid =
        rustix::process::Pid::from_raw(pid).ok_or_else(|| io::Error::other("invalid test PID"))?;
    rustix::process::kill_process(pid, rustix::process::Signal::KILL).map_err(io::Error::from)
}

#[test]
fn metrics_work_without_configuration_and_use_exact_original_name_filters() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    let response = request(address, "GET", "/metrics", &[], &[])?;
    assert_eq!(response.status, 200);
    assert_eq!(response.headers["content-type"], "application/json");
    assert_eq!(response.headers["cache-control"], "no-store");
    assert!(!response.headers.contains_key("etag"));
    let body = response.json();
    assert_eq!(body["processes"].as_array().map(Vec::len), Some(1));
    assert!(
        body["processes"][0]["resource"]
            .get("tenon.node.id")
            .is_none()
    );
    assert!(body["processes"][0]["resource"]["service.instance.id"].is_string());
    for format in ["json", "prometheus"] {
        let response = request(
            address,
            "GET",
            &format!(
                "/metrics?format={format}&include=tenon.process.memory,%20tenon.process.memory"
            ),
            &[("If-None-Match", "*")],
            &[],
        )?;
        assert_eq!(response.status, 200);
        assert_eq!(response.headers["cache-control"], "no-store");
        if format == "json" {
            assert_eq!(
                response.json()["processes"][0]["metrics"]
                    .as_array()
                    .map(Vec::len),
                Some(1)
            );
            assert!(
                value(&response.json()["processes"][0], "tenon.process.memory")
                    .is_some_and(|value| value > 0.0)
            );
        } else {
            assert_eq!(
                response.headers["content-type"],
                "text/plain; version=0.0.4; charset=utf-8"
            );
            assert!(
                response
                    .body_text()
                    .contains("# TYPE tenon_process_memory_bytes gauge")
            );
            assert!(!response.body_text().contains("tenon_process_cpu"));
        }
    }
    for query in [
        "format=otlp",
        "include=",
        "include=tenon.*",
        "include=tenon_process_memory_bytes",
        "include=tenon.process.cpu,",
        "format=json&format=json",
        "x=1",
    ] {
        let response = request(address, "GET", &format!("/metrics?{query}"), &[], &[])?;
        assert_eq!(response.status, 400, "{query}");
        assert_eq!(response.json()["error"]["code"], "invalid_metrics_query");
    }
    runner.terminate()
}

#[test]
fn metrics_reuses_the_http_tls_and_client_certificate_boundary() -> io::Result<()> {
    use runner_http_support::TestTransport;
    for transport in [TestTransport::Https, TestTransport::MutualTls] {
        let directory = tempfile::tempdir()?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        transport.configure(&config)?;
        let mut runner = TestRunner::spawn(&config)?;
        wait_until(|| {
            Ok(transport
                .request(address, "GET", "/metrics", &[], &[])
                .ok()
                .filter(|response| response.status == 200)
                .map(|_| ()))
        })?;
        let response = transport.request(address, "GET", "/metrics?format=prometheus", &[], &[])?;
        assert_eq!(response.status, 200);
        assert!(response.body_text().contains("tenon_process_memory_bytes"));
        if matches!(transport, TestTransport::MutualTls) {
            assert!(
                TestTransport::Https
                    .request(address, "GET", "/metrics", &[], &[])
                    .is_err()
            );
        }
        runner.terminate()?;
    }
    Ok(())
}
