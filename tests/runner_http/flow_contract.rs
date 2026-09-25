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

//! Public rejection and diagnostics proofs for the Instance/Flow contract.

use std::collections::BTreeMap;
use std::io::{BufRead as _, BufReader};
use std::path::{Path, PathBuf};

use base64::Engine as _;
use sha2::{Digest as _, Sha256};

use super::*;

#[test]
fn invalid_document_requests_preserve_the_document_and_live_resources() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    install_interfaces(address)?;
    let original = loop_document("protected", "com.example.gateway").to_string();
    let path = "/documents/protected";
    let created = put_new(address, path, original.as_bytes())?;
    assert_eq!(created.status, 201, "{}", created.body_text());
    wait_until(|| {
        let state = request(address, "GET", "/pipelines/protected", &[], &[])?.json();
        Ok((state["pluginInstances"][0]["state"] == "running").then_some(()))
    })?;
    let fetched = request(address, "GET", path, &[], &[])?;
    let etag = &fetched.headers["etag"];
    let runtime_root = directory.path().join("pipelines");
    let before = runtime_snapshot(&runtime_root)?;
    let semantic: serde_json::Value = serde_json::from_str(include_str!(
        "../../contracts/tenon-document/v1.semantic-test-vectors.json"
    ))?;
    let schema: serde_json::Value = serde_json::from_str(include_str!(
        "../../contracts/tenon-document/v1.test-vectors.json"
    ))?;
    let verification: serde_json::Value = serde_json::from_str(include_str!(
        "../../contracts/tenon-document/v1.verification-test-vectors.json"
    ))?;
    let cases = semantic["invalid"]
        .as_array()
        .ok_or_else(|| io::Error::other("semantic cases missing"))?
        .iter()
        .filter(|case| {
            matches!(
                case["document"]["id"].as_str(),
                Some("missing-source" | "missing-sink" | "source-reused" | "unused-instance")
            )
        })
        .chain(
            schema["invalid"]
                .as_array()
                .ok_or_else(|| io::Error::other("schema cases missing"))?
                .iter()
                .filter(|case| {
                    matches!(
                        case["document"]["id"].as_str(),
                        Some(
                            "empty-sinks"
                                | "duplicate-sinks"
                                | "missing-version"
                                | "numeric-version"
                                | "unsupported-version"
                                | "flow-limits"
                        )
                    )
                }),
        )
        .chain(
            verification["invalid"]
                .as_array()
                .ok_or_else(|| io::Error::other("verification cases missing"))?
                .iter()
                .filter(|case| case["document"]["id"] == "flow-limits"),
        );
    let mut checked = 0;
    for case in cases {
        let mut invalid = case["document"].clone();
        let new_path = format!("/documents/{}", invalid["id"].as_str().unwrap_or(""));
        let rejected_create = put_new(address, &new_path, &serde_json::to_vec(&invalid)?)?;
        assert_eq!(
            rejected_create.status,
            422,
            "{}",
            rejected_create.body_text()
        );
        assert_eq!(request(address, "GET", &new_path, &[], &[])?.status, 404);
        invalid["id"] = serde_json::json!("protected");
        let rejected = request(
            address,
            "PUT",
            path,
            &[("Content-Type", "application/jsonc"), ("If-Match", etag)],
            &serde_json::to_vec(&invalid)?,
        )?;
        assert_eq!(rejected.status, 422, "{}", rejected.body_text());
        let body = rejected.json();
        let issues = body["error"]["issues"]
            .as_array()
            .ok_or_else(|| io::Error::other("public issues missing"))?;
        if let Some(expected) = case["expectedIssues"].as_array() {
            assert_eq!(issues.len(), expected.len(), "{}", rejected.body_text());
            for expected in expected {
                assert!(
                    issues
                        .iter()
                        .any(|actual| actual["code"] == expected["code"]
                            && actual["path"] == expected["instancePath"]),
                    "{}",
                    rejected.body_text()
                );
            }
        } else {
            assert!(
                issues
                    .iter()
                    .any(|issue| issue["path"] == case["expectedInstancePointer"]),
                "{}",
                rejected.body_text()
            );
        }
        let unchanged = request(address, "GET", path, &[], &[])?;
        assert_eq!(unchanged.body, original.as_bytes());
        assert_eq!(&unchanged.headers["etag"], etag);
        assert_eq!(
            fs::read(
                directory
                    .path()
                    .join("tenon-documents")
                    .join(format!("{}.jsonc", sha256_hex(b"protected")))
            )?,
            original.as_bytes()
        );
        let state = request(address, "GET", "/pipelines/protected", &[], &[])?.json();
        assert_eq!(state["appliedDocumentEtag"], *etag);
        assert_eq!(state["pluginInstances"][0]["state"], "running");
        assert_eq!(runtime_snapshot(&runtime_root)?, before, "{}", case["name"]);
        checked += 1;
    }
    assert_eq!(checked, 26);
    runner.terminate()
}

#[test]
fn missing_interfaces_persist_as_unready_without_launching() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    install_interfaces(address)?;
    let before = runtime_snapshot(&directory.path().join("pipelines"))?;
    for (document, expected) in [
        (
            loop_document("missing-source-interface", "com.example.kafka"),
            "plugin_source_interface_missing",
        ),
        (
            loop_document("missing-sink-interface", "com.example.modbus"),
            "plugin_sink_interface_missing",
        ),
    ] {
        let id = document["id"]
            .as_str()
            .ok_or_else(|| io::Error::other("fixture id missing"))?;
        let bytes = serde_json::to_vec(&document)?;
        let path = format!("/documents/{id}");
        let created = put_new(address, &path, &bytes)?;
        assert_eq!(created.status, 201, "{}", created.body_text());
        assert_eq!(request(address, "GET", &path, &[], &[])?.body, bytes);
        let state = request(address, "GET", &format!("/pipelines/{id}"), &[], &[])?.json();
        assert_eq!(state["state"], "unready");
        assert!(state.get("appliedDocumentEtag").is_none());
        assert!(state.get("pluginInstances").is_none());
        assert!(
            state["runtimeIssues"]
                .as_array()
                .is_some_and(|issues| issues.iter().any(|issue| issue["code"] == expected)),
            "{state}"
        );
        assert_eq!(
            runtime_snapshot(&directory.path().join("pipelines"))?,
            before
        );
    }
    runner.terminate()
}

#[test]
fn public_sse_keeps_dual_process_sequence_and_equal_channel_indices_distinct() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    install_interfaces(address)?;
    let mut document = serde_json::json!({"specVersion": "1", "id": "diagnostic-identity",
        "pluginInstances": {}, "flows": {}});
    for (instance, flow, marker) in [
        ("gateway-a", "telemetry", "telemetry-probe"),
        ("gateway-b", "commands", "commands-probe"),
    ] {
        document["pluginInstances"][instance] = serde_json::json!({
            "programName": "com.example.gateway", "exactVersion": "1.0.0", "config": {"behavior": "delay-ready"}
        });
        document["flows"][flow] = serde_json::json!({
            "parallelism": 1, "source": instance, "sinks": [instance],
            "process": {"script": format!("setTimeout(5)\nfunction main(event) print('{marker}'); setTimeout(5) end")}
        });
    }
    let bytes = serde_json::to_vec(&document)?;
    let created = put_new(address, "/documents/diagnostic-identity", &bytes)?;
    assert_eq!(created.status, 201, "{}", created.body_text());
    let mut plugin =
        DiagnosticStream::subscribe(address, "diagnostic-identity", "plugin%3Agateway-a")?;
    let mut telemetry = DiagnosticStream::subscribe(
        address,
        "diagnostic-identity",
        "flow%3Atelemetry%2Fchannel%3A0",
    )?;
    let mut commands = DiagnosticStream::subscribe(
        address,
        "diagnostic-identity",
        "flow%3Acommands%2Fchannel%3A0",
    )?;
    let first_a = telemetry.next_diagnostic()?;
    let first_b = commands.next_diagnostic()?;
    assert_flow(&first_a, "telemetry", "telemetry-probe");
    assert_flow(&first_b, "commands", "commands-probe");
    assert_ne!(first_a["channelInstanceId"], first_b["channelInstanceId"]);
    assert_eq!(first_a["pipelineInstanceId"], first_b["pipelineInstanceId"]);
    // Real Lua output proves the complete latest interest snapshot reached the
    // Pipeline. The earlier, still-open Plugin subscription is in that snapshot.
    let roots = directory
        .path()
        .join("pipelines")
        .read_dir()?
        .collect::<Result<Vec<_>, _>>()?;
    let [root] = roots.as_slice() else {
        return Err(io::Error::other("expected one Pipeline root"));
    };
    let instances = root.path().join(sha256_hex(&bytes)).join("instances");
    for instance in ["gateway-a", "gateway-b"] {
        let instance = instances.join(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(Sha256::digest(instance.as_bytes())),
        );
        fs::write(instance.join("allow-ready"), b"ready\n")?;
    }
    let mut records = Vec::new();
    while !["diagnostic-after-ready", "diagnostic-stderr-after-ready"]
        .iter()
        .all(|marker| {
            records
                .iter()
                .any(|record: &serde_json::Value| record["text"] == *marker)
        })
    {
        records.push(plugin.next_diagnostic()?);
    }
    let mut sequences = std::collections::BTreeSet::new();
    for record in &records {
        assert_eq!(record["pipelineId"], "diagnostic-identity");
        assert_eq!(record["pluginInstanceId"], "gateway-a");
        assert_eq!(record["pipelineInstanceId"], first_a["pipelineInstanceId"]);
        assert!(record.get("flowId").is_none());
        assert!(
            record["pluginProcessInstanceId"]
                .as_str()
                .is_some_and(|id| !id.is_empty())
        );
        assert_eq!(
            record["pluginProcessInstanceId"],
            records[0]["pluginProcessInstanceId"]
        );
        assert!(
            sequences.insert(
                record["sequence"]
                    .as_str()
                    .ok_or_else(|| io::Error::other("diagnostic sequence is missing"))?
            )
        );
    }
    let by_stream = records
        .iter()
        .filter(|record| {
            matches!(
                record["text"].as_str(),
                Some("diagnostic-after-ready" | "diagnostic-stderr-after-ready")
            )
        })
        .map(|record| {
            (
                record["stream"].as_str().unwrap_or(""),
                record["text"].as_str().unwrap_or(""),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        by_stream,
        BTreeMap::from([
            ("stdout", "diagnostic-after-ready"),
            ("stderr", "diagnostic-stderr-after-ready")
        ])
    );
    for _ in 0..3 {
        assert_flow(
            &telemetry.next_diagnostic()?,
            "telemetry",
            "telemetry-probe",
        );
        assert_flow(&commands.next_diagnostic()?, "commands", "commands-probe");
    }
    drop((plugin, telemetry, commands));
    runner.terminate()
}

#[test]
fn flow_channel_error_diagnostic_reaches_a_live_sse_subscriber() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    install_interfaces(address)?;
    // `main` fails on the timer event the script schedules, so the first failing
    // record is not the only one: the subscription needs no race against it.
    let document = serde_json::json!({"specVersion": "1", "id": "diagnostic-identity",
        "pluginInstances": {"gateway-a": {
            "programName": "com.example.gateway", "exactVersion": "1.0.0", "config": {}
        }},
        "flows": {"failing": {
            "parallelism": 1, "source": "gateway-a", "sinks": ["gateway-a"],
            "process": {"script": "setTimeout(0)\nfunction main(event)\n  print('qa-before-intentional-error')\n  setTimeout(5)\n  error('qa-intentional-runtime-error')\nend"}
        }}
    });
    let created = put_new(
        address,
        "/documents/diagnostic-identity",
        &serde_json::to_vec(&document)?,
    )?;
    assert_eq!(created.status, 201, "{}", created.body_text());
    let mut failing = DiagnosticStream::subscribe(
        address,
        "diagnostic-identity",
        "flow%3Afailing%2Fchannel%3A0",
    )?;
    let (mut printed, mut failed) = (None, None);
    while printed.is_none() || failed.is_none() {
        let record = failing.next_diagnostic()?;
        match record["stream"].as_str() {
            Some("lua-print") => printed = Some(record),
            Some("lua-error") => failed = Some(record),
            other => {
                return Err(io::Error::other(format!(
                    "unexpected Flow Channel diagnostic stream: {other:?}"
                )));
            }
        }
    }
    let printed = printed.ok_or_else(|| io::Error::other("print diagnostic missing"))?;
    let failed = failed.ok_or_else(|| io::Error::other("error diagnostic missing"))?;
    assert_eq!(printed["pipelineId"], "diagnostic-identity");
    assert_eq!(printed["flowId"], "failing");
    assert_eq!(printed["channelIndex"], 0);
    assert_eq!(printed["text"], "qa-before-intentional-error");
    assert!(printed.get("phase").is_none());
    assert_eq!(failed["pipelineId"], "diagnostic-identity");
    assert_eq!(failed["flowId"], "failing");
    assert_eq!(failed["channelIndex"], 0);
    assert_eq!(failed["phase"], "lua_main");
    assert_eq!(failed["code"], "process.lua_main_failed");
    // The subscriber reads the message the script author wrote: neither the
    // position prefix nor the traceback mlua attaches reaches the wire.
    assert_eq!(failed["text"], "qa-intentional-runtime-error");
    assert!(
        failed["luaVmInstanceId"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    assert!(
        failed["channelInstanceId"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    assert_eq!(failed["channelInstanceId"], printed["channelInstanceId"]);
    assert_ne!(failed["sequence"], printed["sequence"]);
    drop(failing);
    runner.terminate()
}

#[test]
fn diagnostic_subscription_rejects_eof_during_headers() -> io::Result<()> {
    const ADDRESS_ENV: &str = "TENON_DIAGNOSTIC_EOF_ADDRESS";
    if let Ok(address) = std::env::var(ADDRESS_ENV) {
        let address = address.parse().map_err(io::Error::other)?;
        let error = DiagnosticStream::subscribe(address, "diagnostic-identity", "plugin:gateway")
            .err()
            .ok_or_else(|| io::Error::other("incomplete diagnostic headers were accepted"))?;
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        return Ok(());
    }

    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.set_nonblocking(true)?;
    let mut child = DiagnosticProbe(
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "flow_contract::diagnostic_subscription_rejects_eof_during_headers",
                "--exact",
                "--nocapture",
            ])
            .env(ADDRESS_ENV, listener.local_addr()?.to_string())
            .spawn()?,
    );
    wait_until(|| match listener.accept() {
        Ok((mut stream, _)) => {
            stream.set_read_timeout(Some(DEADLINE))?;
            read_until(&mut stream, b"\r\n\r\n")?;
            stream.write_all(b"HTTP/1.0 200 OK\r\nContent-Type: text/event-stream\r\n")?;
            stream.shutdown(Shutdown::Write)?;
            Ok(Some(()))
        }
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(error),
    })?;
    wait_until(|| {
        Ok(child.0.try_wait()?.map(|status| {
            assert!(status.success(), "diagnostic EOF probe failed: {status}");
        }))
    })
}

fn assert_flow(record: &serde_json::Value, flow: &str, text: &str) {
    assert_eq!(record["pipelineId"], "diagnostic-identity");
    assert_eq!(record["flowId"], flow);
    assert_eq!(record["channelIndex"], 0);
    assert_eq!(record["stream"], "lua-print");
    assert_eq!(record["text"], text);
    assert!(record.get("pluginInstanceId").is_none());
}

fn loop_document(id: &str, program: &str) -> serde_json::Value {
    serde_json::json!({"specVersion": "1", "id": id,
        "pluginInstances": {"gateway": {"programName": program, "exactVersion": "1.0.0", "config": {}}},
        "flows": {"main": {"parallelism": 1, "source": "gateway", "maxPendingRecords": 4, "maxRecordBytes": 1024,
            "process": {"script": "function main(event) emit() end"}, "sinks": ["gateway"]}}
    })
}

fn install_interfaces(address: std::net::SocketAddr) -> io::Result<()> {
    for interface in [
        PluginInterface::Source,
        PluginInterface::Sink,
        PluginInterface::SourceAndSink,
    ] {
        let response = request(
            address,
            "POST",
            "/plugins",
            &[(
                "Content-Type",
                "application/vnd.apache.tenon.plugin+tar+gzip",
            )],
            &plugin_package(interface)?,
        )?;
        assert_eq!(response.status, 201, "{}", response.body_text());
    }
    Ok(())
}

fn put_new(address: std::net::SocketAddr, path: &str, document: &[u8]) -> io::Result<HttpResponse> {
    request(
        address,
        "PUT",
        path,
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        document,
    )
}

fn runtime_snapshot(root: &Path) -> io::Result<BTreeMap<PathBuf, Vec<u8>>> {
    let mut snapshot = BTreeMap::new();
    if !root.try_exists()? {
        return Ok(snapshot);
    }
    for entry in root.read_dir()? {
        let entry = entry?;
        let path = entry.path();
        let kind = entry.file_type()?;
        snapshot.insert(
            path.clone(),
            if kind.is_file() {
                fs::read(&path)?
            } else {
                Vec::new()
            },
        );
        if kind.is_dir() {
            snapshot.extend(runtime_snapshot(&path)?);
        }
    }
    Ok(snapshot)
}

struct DiagnosticProbe(std::process::Child);

impl Drop for DiagnosticProbe {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct DiagnosticStream(BufReader<TcpStream>);

impl DiagnosticStream {
    fn subscribe(
        address: std::net::SocketAddr,
        pipeline_id: &str,
        target: &str,
    ) -> io::Result<Self> {
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
        stream.set_read_timeout(Some(DEADLINE))?;
        // HTTP/1.0 uses a close-delimited body, so the standard buffered reader
        // can consume SSE lines without duplicating an HTTP chunk decoder.
        write!(
            stream,
            "GET /pipelines/{pipeline_id}/diagnostics?target={target} HTTP/1.0\r\nHost: {address}\r\nAccept: text/event-stream\r\n\r\n"
        )?;
        stream.flush()?;
        let mut stream = Self(BufReader::new(stream));
        let mut status = String::new();
        stream.0.read_line(&mut status)?;
        assert!(status.starts_with("HTTP/1.0 200"), "{status}");
        loop {
            let mut line = String::new();
            if stream.0.read_line(&mut line)? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "SSE ended before the response headers",
                ));
            }
            if line == "\r\n" {
                break;
            }
            assert!(!line.to_ascii_lowercase().starts_with("transfer-encoding:"));
        }
        assert_eq!(stream.next_event()?.0, "attached");
        Ok(stream)
    }

    fn next_event(&mut self) -> io::Result<(String, serde_json::Value)> {
        let mut event = String::new();
        let mut data = String::new();
        loop {
            let mut line = String::new();
            if self.0.read_line(&mut line)? == 0 {
                return Err(io::Error::other("SSE ended before the expected event"));
            }
            let line = line.trim_end_matches(['\r', '\n']);
            if line.is_empty() && !event.is_empty() {
                return Ok((event, serde_json::from_str(&data)?));
            }
            if let Some(value) = line.strip_prefix("event:") {
                event = value.trim_start().to_owned();
            }
            if let Some(value) = line.strip_prefix("data:") {
                data.push_str(value.trim_start());
            }
        }
    }

    fn next_diagnostic(&mut self) -> io::Result<serde_json::Value> {
        let (event, record) = self.next_event()?;
        assert_eq!(event, "diagnostic");
        Ok(record)
    }
}
