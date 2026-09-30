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

use std::fs;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::os::unix::fs::PermissionsExt as _;
use std::time::Duration;

#[path = "runner_http/flow_contract.rs"]
mod flow_contract;
#[path = "runner_cli/plugin_fixture.rs"]
mod plugin_fixture;
#[path = "support/plugin_platform.rs"]
mod plugin_platform;
#[path = "support/runner_http.rs"]
mod runner_http_support;

use plugin_fixture::{
    PluginInterface, install_program, plugin_package, plugin_package_with_program,
};
use runner_http_support::{
    DEADLINE, HttpResponse, TestRunner, TestTransport, available_address, read_continue_response,
    request, sha256_hex, wait_for_http, wait_for_server, wait_until, write_config,
};

#[test]
fn unified_document_reaches_real_pipeline_and_keeps_applied_when_latest_is_unready()
-> io::Result<()> {
    for (transport, expected_issue) in [
        (TestTransport::Http, "plugin_program_missing"),
        (TestTransport::Https, "plugin_program_missing"),
        (TestTransport::MutualTls, "plugin_program_missing"),
        (TestTransport::Http, "plugin_platform_mismatch"),
    ] {
        let directory = tempfile::tempdir()?;
        install_program(directory.path(), PluginInterface::SourceAndSink)?;
        let foreign_manifest = if expected_issue == "plugin_platform_mismatch" {
            let namespace = directory
                .path()
                .join("plugins/programs/com.example.gateway");
            let foreign = namespace.join("2.0.0");
            fs::create_dir(&foreign)?;
            fs::set_permissions(&foreign, fs::Permissions::from_mode(0o700))?;
            for entry in fs::read_dir(namespace.join("1.0.0"))? {
                let entry = entry?;
                let mut bytes = fs::read(entry.path())?;
                if entry.file_name() == "manifest.json" {
                    let mut manifest: serde_json::Value = serde_json::from_slice(&bytes)?;
                    manifest["exactVersion"] = serde_json::json!("2.0.0");
                    manifest["platforms"] =
                        serde_json::json!([plugin_platform::foreign_platform()]);
                    bytes = serde_json::to_vec(&manifest)?;
                }
                let target = foreign.join(entry.file_name());
                fs::write(&target, bytes)?;
                fs::set_permissions(target, fs::Permissions::from_mode(0o500))?;
            }
            Some(fs::read(foreign.join("manifest.json"))?)
        } else {
            None
        };
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        transport.configure(&config)?;
        let mut runner = TestRunner::spawn(&config)?;
        wait_for_server(&mut runner, address, transport)?;
        let mut document = serde_json::json!({
            "specVersion": "1", "id": "unified-chain",
            "pluginInstances": {
                "gateway": {"programName": "com.example.gateway", "exactVersion": "1.0.0", "config": {}}
            },
            "flows": {"loop": {
                "parallelism": 1, "source": "gateway",
                "process": {"script": "function main(event) emit() end"}, "sinks": ["gateway"]
            }}
        });
        let path = "/documents/unified-chain";
        let created = transport.request(
            address,
            "PUT",
            path,
            &[
                ("Content-Type", "application/jsonc"),
                ("If-None-Match", "*"),
            ],
            &serde_json::to_vec(&document)?,
        )?;
        assert_eq!(created.status, 201, "{}", created.body_text());
        let fetched = transport.request(address, "GET", path, &[], &[])?;
        let etag = &fetched.headers["etag"];
        assert_eq!(created.headers.get("etag"), Some(etag));
        wait_until(|| {
            let response =
                transport.request(address, "GET", "/pipelines/unified-chain", &[], &[])?;
            Ok((response.json()["appliedDocumentEtag"] == *etag
                && response.json()["pluginInstances"][0]["state"] == "running")
                .then_some(()))
        })?;
        let applied = transport
            .request(address, "GET", "/pipelines/unified-chain", &[], &[])?
            .json();
        let assert_summary = |details: &serde_json::Value| -> io::Result<()> {
            let summary = transport
                .request(address, "GET", "/pipelines", &[], &[])?
                .json();
            assert_eq!(
                summary,
                serde_json::json!({"pipelines": [{
                    "id": details["id"], "documentEtag": details["documentEtag"],
                    "state": details["state"], "appliedDocumentEtag": details["appliedDocumentEtag"]
                }]})
            );
            Ok(())
        };
        assert_summary(&applied)?;
        assert_eq!(applied["pluginInstances"][0]["id"], "gateway");
        assert_eq!(applied["pluginInstances"][0]["exactVersion"], "1.0.0");
        assert_eq!(applied["pluginInstances"][0]["state"], "running");
        document["pluginInstances"]["gateway"]["exactVersion"] = serde_json::json!("2.0.0");
        let updated = transport.request(
            address,
            "PUT",
            path,
            &[("Content-Type", "application/jsonc"), ("If-Match", etag)],
            &serde_json::to_vec(&document)?,
        )?;
        assert_eq!(updated.status, 204, "{}", updated.body_text());
        assert_eq!(
            updated.headers.get("etag").map(String::as_str),
            Some(format!("\"{}\"", sha256_hex(&serde_json::to_vec(&document)?)).as_str())
        );
        let latest = transport
            .request(address, "GET", "/pipelines/unified-chain", &[], &[])?
            .json();
        assert_eq!(latest["state"], "unready");
        assert_eq!(latest["appliedDocumentEtag"], *etag);
        assert_eq!(latest["pluginInstances"][0]["exactVersion"], "1.0.0");
        assert_eq!(latest["runtimeIssues"][0]["code"], expected_issue);
        assert_summary(&latest)?;
        if let Some(manifest_bytes) = foreign_manifest {
            let summary = request(
                address,
                "GET",
                "/plugins/com.example.gateway/2.0.0",
                &[],
                &[],
            )?;
            assert_eq!(summary.status, 200, "{}", summary.body_text());
            assert_eq!(
                summary.json()["platforms"],
                serde_json::json!([plugin_platform::foreign_platform()])
            );
            assert_eq!(
                fs::read(
                    directory
                        .path()
                        .join("plugins/programs/com.example.gateway/2.0.0/manifest.json")
                )?,
                manifest_bytes
            );
            let in_use = request(
                address,
                "DELETE",
                "/plugins/com.example.gateway/2.0.0",
                &[],
                &[],
            )?;
            assert_eq!(in_use.status, 409, "{}", in_use.body_text());
        }
        let in_use = transport.request(
            address,
            "DELETE",
            "/plugins/com.example.gateway/1.0.0",
            &[],
            &[],
        )?;
        assert_eq!(in_use.status, 409, "{}", in_use.body_text());
        let fetched = transport.request(address, "GET", path, &[], &[])?;
        let removed = transport.request(
            address,
            "DELETE",
            path,
            &[("If-Match", &fetched.headers["etag"])],
            &[],
        )?;
        assert_eq!(removed.status, 204, "{}", removed.body_text());
        assert!(!removed.headers.contains_key("etag"));
        wait_until(|| {
            let response = transport.request(
                address,
                "DELETE",
                "/plugins/com.example.gateway/1.0.0",
                &[],
                &[],
            )?;
            Ok((response.status == 204).then_some(()))
        })?;
        runner.terminate()?;
    }
    Ok(())
}

#[test]
fn diagnostics_sse_stops_cleanly_during_runner_shutdown() -> io::Result<()> {
    for transport in [
        TestTransport::Http,
        TestTransport::Https,
        TestTransport::MutualTls,
    ] {
        let directory = tempfile::tempdir()?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        transport.configure(&config)?;
        let mut runner = TestRunner::spawn(&config)?;
        wait_for_server(&mut runner, address, transport)?;

        let source = document("diagnostics");
        let created = transport.request(
            address,
            "PUT",
            "/documents/diagnostics",
            &[
                ("Content-Type", "application/jsonc"),
                ("If-None-Match", "*"),
            ],
            source.as_bytes(),
        )?;
        assert_eq!(created.status, 201, "{}", created.body_text());
        let invalid = transport.request(
            address,
            "GET",
            "/pipelines/diagnostics/diagnostics?target=plugin",
            &[],
            &[],
        )?;
        assert_eq!(invalid.status, 400, "{}", invalid.body_text());
        let missing = transport.request(
            address,
            "GET",
            "/pipelines/missing/diagnostics?target=flow%3Amain%2Fchannel%3A0",
            &[],
            &[],
        )?;
        assert_eq!(missing.status, 404, "{}", missing.body_text());

        let mut stream = transport.connect(address)?;
        write!(
            stream,
            "GET /pipelines/diagnostics/diagnostics?target=flow%3Amain%2Fchannel%3A0 HTTP/1.1\r\nHost: {address}\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n",
        )?;
        stream.flush()?;
        let mut response = read_until(&mut stream, b"event: attached")?;
        assert!(
            response.starts_with(b"HTTP/1.1 200"),
            "{}",
            String::from_utf8_lossy(&response),
        );
        assert!(
            String::from_utf8_lossy(&response).contains("content-type: text/event-stream"),
            "{}",
            String::from_utf8_lossy(&response),
        );

        runner.terminate()?;
        stream.read_to_end(&mut response)?;
        let response = String::from_utf8_lossy(&response);
        assert!(response.contains("event: closed"), "{response}");
    }
    Ok(())
}

#[test]
fn unified_program_delete_is_exact_durable_and_idempotent() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    for interface in [
        PluginInterface::Source,
        PluginInterface::Sink,
        PluginInterface::SourceAndSink,
    ] {
        install_program(directory.path(), interface)?;
    }
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;

    let absent = request(
        address,
        "DELETE",
        "/plugins/com.example.modbus/9.9.9",
        &[],
        &[],
    )?;
    assert_eq!(absent.status, 204, "{}", absent.body_text());
    assert_eq!(
        request(
            address,
            "GET",
            "/plugins/com.example.modbus/1.0.0",
            &[],
            &[]
        )?
        .status,
        200
    );
    for name in [
        "com.example.modbus",
        "com.example.kafka",
        "com.example.gateway",
    ] {
        let path = format!("/plugins/{name}/1.0.0");
        for _ in 0..2 {
            let deleted = request(address, "DELETE", &path, &[], &[])?;
            assert_eq!(deleted.status, 204, "{}", deleted.body_text());
            assert!(deleted.body.is_empty());
        }
        for suffix in ["", "/config-schema", "/payload-contract"] {
            let missing = request(address, "GET", &format!("{path}{suffix}"), &[], &[])?;
            assert_eq!(missing.status, 404, "{}", missing.body_text());
            assert_eq!(missing.json()["error"]["code"], "plugin_not_found");
        }
        assert!(
            directory
                .path()
                .join("plugins/programs")
                .join(name)
                .read_dir()?
                .next()
                .is_none()
        );
    }
    assert_eq!(
        request(address, "GET", "/plugins", &[], &[])?.json(),
        serde_json::json!({"plugins": []})
    );
    runner.terminate()?;
    Ok(())
}

#[test]
fn unified_program_delete_preserves_all_document_references_including_missing_programs()
-> io::Result<()> {
    let directory = tempfile::tempdir()?;
    install_program(directory.path(), PluginInterface::Source)?;
    install_program(directory.path(), PluginInterface::Sink)?;
    install_program(directory.path(), PluginInterface::SourceAndSink)?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    let mut documents = Vec::new();
    for (id, source) in [
        ("zeta", document("zeta")),
        ("alpha", document("alpha")),
        ("missing", document_with_source_version("missing", "2.0.0")),
        ("dual-role", {
            let source = document("dual-role")
                .replace("com.example.modbus", "com.example.gateway")
                .replace("com.example.kafka", "com.example.gateway");
            let mut document: serde_json::Value = serde_json::from_str(&source)?;
            document["flows"]["return"] = serde_json::json!({
                "parallelism": 1, "source": "primary",
                "process": {"script": "function main(event) emit() end"},
                "sinks": ["source"]
            });
            document.to_string()
        }),
    ] {
        let path = format!("/documents/{id}");
        let created = request(
            address,
            "PUT",
            &path,
            &[
                ("Content-Type", "application/jsonc"),
                ("If-None-Match", "*"),
            ],
            source.as_bytes(),
        )?;
        assert_eq!(created.status, 201, "{}", created.body_text());
        let etag = format!("\"{}\"", sha256_hex(source.as_bytes()));
        documents.push((path, etag));
        let pipeline = request(address, "GET", &format!("/pipelines/{id}"), &[], &[])?;
        if id == "missing" {
            assert_eq!(pipeline.json()["state"], "unready");
        } else {
            wait_until(|| {
                let pipeline = request(address, "GET", &format!("/pipelines/{id}"), &[], &[])?;
                Ok((pipeline.json()["state"] == "running").then_some(()))
            })?;
        }
    }

    for (identity, references) in [
        (
            "com.example.modbus/1.0.0",
            serde_json::json!(["alpha", "zeta"]),
        ),
        ("com.example.modbus/2.0.0", serde_json::json!(["missing"])),
        (
            "com.example.kafka/1.0.0",
            serde_json::json!(["alpha", "missing", "zeta"]),
        ),
        (
            "com.example.gateway/1.0.0",
            serde_json::json!(["dual-role"]),
        ),
    ] {
        let response = request(address, "DELETE", &format!("/plugins/{identity}"), &[], &[])?;
        assert_eq!(response.status, 409, "{}", response.body_text());
        assert_eq!(
            response.json(),
            serde_json::json!({"error": {
                "code": "plugin_in_use", "message": "Plugin is still referenced", "referencedBy": references
            }})
        );
    }
    for (path, etag) in documents {
        let removed = request(address, "DELETE", &path, &[("If-Match", &etag)], &[])?;
        assert_eq!(removed.status, 204, "{}", removed.body_text());
        assert!(!removed.headers.contains_key("etag"));
    }
    for identity in [
        "com.example.modbus/1.0.0",
        "com.example.modbus/2.0.0",
        "com.example.kafka/1.0.0",
        "com.example.gateway/1.0.0",
    ] {
        wait_until(|| {
            let removed = request(address, "DELETE", &format!("/plugins/{identity}"), &[], &[])?;
            match removed.status {
                204 => Ok(Some(())),
                409 => Ok(None),
                _ => Err(io::Error::other(removed.body_text())),
            }
        })?;
    }
    runner.terminate()?;
    Ok(())
}

#[test]
fn unified_program_delete_rejects_invalid_paths_without_touching_the_store() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    install_program(directory.path(), PluginInterface::Source)?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;

    for identity in [
        "example.modbus/1.0.0",
        "Com.example.modbus/1.0.0",
        "com.example.modbus/1.0",
        "com.example.modbus/v1.0.0",
        "com.example.modbus/01.0.0",
        "com.example.modbus/1.0.0%2Bbuild.1",
        "com.example.modbus/1.0.0%252Fignored",
        "com.example.modbus%2Fother/1.0.0",
    ] {
        let response = request(address, "DELETE", &format!("/plugins/{identity}"), &[], &[])?;
        assert_eq!(response.status, 400, "{identity}: {}", response.body_text());
    }
    let entry = request(
        address,
        "GET",
        "/plugins/com.example.modbus/1.0.0",
        &[],
        &[],
    )?;
    assert_eq!(entry.status, 200, "{}", entry.body_text());
    assert!(
        directory
            .path()
            .join("plugins/programs/com.example.modbus/1.0.0/manifest.json")
            .is_file()
    );
    runner.terminate()?;
    Ok(())
}

#[test]
fn unified_program_queries_only_return_usable_metadata_summaries() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    install_program(directory.path(), PluginInterface::Source)?;
    install_program(directory.path(), PluginInterface::Sink)?;
    install_program(directory.path(), PluginInterface::SourceAndSink)?;
    let program_root = directory.path().join("plugins/programs");
    let invalid = program_root.join("com.example.kafka/1.0.0/manifest.json");
    fs::set_permissions(&invalid, fs::Permissions::from_mode(0o700))?;
    fs::write(&invalid, b"{}")?;
    fs::set_permissions(&invalid, fs::Permissions::from_mode(0o500))?;
    let broken_namespace = program_root.join("com.example.broken");
    fs::write(&broken_namespace, b"not a namespace")?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    let document = document("program-query");
    let created = request(
        address,
        "PUT",
        "/documents/program-query",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        document.as_bytes(),
    )?;
    assert_eq!(created.status, 201, "{}", created.body_text());

    let programs = request(address, "GET", "/plugins", &[], &[])?;
    assert_eq!(programs.status, 200, "{}", programs.body_text());
    let expected = serde_json::json!({"plugins": [
        {"programName": "com.example.gateway", "exactVersion": "1.0.0", "interface": "source-and-sink", "displayName": "Example Plugin", "description": "Read and write example records.", "platforms": plugin_fixture::platforms()},
        {"programName": "com.example.modbus", "exactVersion": "1.0.0", "interface": "source", "displayName": "Example Plugin", "description": "Read and write example records.", "platforms": plugin_fixture::platforms()}
    ]});
    assert_eq!(programs.json(), expected);
    for (index, name) in ["com.example.gateway", "com.example.modbus"]
        .into_iter()
        .enumerate()
    {
        let exact = request(address, "GET", &format!("/plugins/{name}/1.0.0"), &[], &[])?;
        assert_eq!(exact.status, 200, "{}", exact.body_text());
        assert_eq!(exact.json(), expected["plugins"][index]);
    }
    for identity in [
        "com.example.kafka/1.0.0",
        "com.example.broken/1.0.0",
        "com.example.modbus/9.9.9",
    ] {
        for suffix in ["", "/config-schema", "/payload-contract"] {
            let missing = request(
                address,
                "GET",
                &format!("/plugins/{identity}{suffix}"),
                &[],
                &[],
            )?;
            assert_eq!(missing.status, 404, "{}", missing.body_text());
            assert_eq!(missing.json()["error"]["code"], "plugin_not_found");
        }
    }
    assert!(!program_root.join("com.example.kafka/1.0.0").exists());
    assert!(!broken_namespace.exists());

    runner.terminate()?;
    Ok(())
}

#[test]
fn unified_program_filters_select_interfaces_and_reject_invalid_parameters() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    for interface in [
        PluginInterface::Source,
        PluginInterface::Sink,
        PluginInterface::SourceAndSink,
    ] {
        install_program(directory.path(), interface)?;
    }
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;

    for (filter, expected) in [
        (
            "source",
            serde_json::json!([
                {"programName": "com.example.gateway", "exactVersion": "1.0.0", "interface": "source-and-sink", "displayName": "Example Plugin", "description": "Read and write example records.", "platforms": plugin_fixture::platforms()},
                {"programName": "com.example.modbus", "exactVersion": "1.0.0", "interface": "source", "displayName": "Example Plugin", "description": "Read and write example records.", "platforms": plugin_fixture::platforms()},
            ]),
        ),
        (
            "sink",
            serde_json::json!([
                {"programName": "com.example.gateway", "exactVersion": "1.0.0", "interface": "source-and-sink", "displayName": "Example Plugin", "description": "Read and write example records.", "platforms": plugin_fixture::platforms()},
                {"programName": "com.example.kafka", "exactVersion": "1.0.0", "interface": "sink", "displayName": "Example Plugin", "description": "Read and write example records.", "platforms": plugin_fixture::platforms()},
            ]),
        ),
    ] {
        let response = request(
            address,
            "GET",
            &format!("/plugins?interface={filter}"),
            &[],
            &[],
        )?;
        assert_eq!(response.status, 200, "{}", response.body_text());
        assert_eq!(response.json(), serde_json::json!({"plugins": expected}));
    }
    for query in [
        "interface=source-and-sink",
        "interface=all",
        "interface=",
        "interface=source&interface=sink",
        "interface=source&interface=source",
        "kind=source",
    ] {
        let response = request(address, "GET", &format!("/plugins?{query}"), &[], &[])?;
        assert_eq!(response.status, 400, "{}", response.body_text());
        assert_eq!(response.json()["error"]["code"], "invalid_plugin_filter");
    }
    runner.terminate()?;
    Ok(())
}

#[test]
fn unified_program_material_queries_preserve_original_bytes() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let fixtures = [
        (
            "com.example.modbus",
            install_program(directory.path(), PluginInterface::Source)?,
        ),
        (
            "com.example.kafka",
            install_program(directory.path(), PluginInterface::Sink)?,
        ),
        (
            "com.example.gateway",
            install_program(directory.path(), PluginInterface::SourceAndSink)?,
        ),
    ];
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;

    for (name, fixture) in fixtures {
        for (resource, content_type, bytes) in [
            (
                "config-schema",
                "application/schema+json",
                fixture.config_schema,
            ),
            (
                "payload-contract",
                "application/x-protobuf",
                fixture.payload_descriptor,
            ),
        ] {
            let response = request(
                address,
                "GET",
                &format!("/plugins/{name}/1.0.0/{resource}"),
                &[],
                &[],
            )?;
            assert_eq!(response.status, 200, "{}", response.body_text());
            assert_eq!(response.body, bytes);
            assert_eq!(
                response.headers.get("content-type").map(String::as_str),
                Some(content_type)
            );
        }
    }
    runner.terminate()?;
    Ok(())
}

#[test]
fn document_plugin_and_reconcile_surface_is_one_consistent_boundary() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;

    let source = document("managed");
    let mismatched = request(
        address,
        "PUT",
        "/documents/other",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        source.as_bytes(),
    )?;
    assert_eq!(mismatched.status, 422);
    assert_eq!(
        mismatched.json()["error"]["code"],
        "tenon_document_id_mismatch"
    );
    let documents = request(address, "GET", "/documents", &[], &[])?;
    assert_eq!(documents.json()["documents"], serde_json::json!([]));

    let missing_condition = request(
        address,
        "PUT",
        "/documents/managed",
        &[("Content-Type", "application/jsonc")],
        source.as_bytes(),
    )?;
    assert_eq!(missing_condition.status, 428);

    let created = request(
        address,
        "PUT",
        "/documents/managed",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        source.as_bytes(),
    )?;
    assert_eq!(created.status, 201, "{}", created.body_text());
    assert_eq!(
        created.headers.get("tenon-version").map(String::as_str),
        Some("0.1.0")
    );

    let fetched = request(address, "GET", "/documents/managed", &[], &[])?;
    assert_eq!(fetched.status, 200);
    assert_eq!(fetched.body, source.as_bytes());
    let etag = format!("\"{}\"", sha256_hex(source.as_bytes()));
    assert_eq!(fetched.headers.get("etag"), Some(&etag));
    let documents = request(address, "GET", "/documents", &[], &[])?;
    assert_eq!(documents.status, 200);
    assert_eq!(
        documents.json()["documents"],
        serde_json::json!([{"id": "managed", "etag": etag.as_str()}])
    );

    let identical_without_condition = request(
        address,
        "PUT",
        "/documents/managed",
        &[("Content-Type", "application/jsonc")],
        source.as_bytes(),
    )?;
    assert_eq!(identical_without_condition.status, 428);
    assert!(!identical_without_condition.headers.contains_key("etag"));
    assert!(
        identical_without_condition.json()["error"]
            .get("issues")
            .is_none()
    );
    let identical_with_stale_condition = request(
        address,
        "PUT",
        "/documents/managed",
        &[
            ("Content-Type", "application/jsonc"),
            (
                "If-Match",
                "\"0000000000000000000000000000000000000000000000000000000000000000\"",
            ),
        ],
        source.as_bytes(),
    )?;
    assert_eq!(identical_with_stale_condition.status, 412);
    let identical_retry = request(
        address,
        "PUT",
        "/documents/managed",
        &[("Content-Type", "application/jsonc"), ("If-Match", &etag)],
        source.as_bytes(),
    )?;
    assert_eq!(identical_retry.status, 204);
    assert_eq!(identical_retry.headers.get("etag"), Some(&etag));
    let duplicate_condition = request(
        address,
        "PUT",
        "/documents/managed",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-Match", &etag),
            ("If-Match", &etag),
        ],
        source.as_bytes(),
    )?;
    assert_eq!(duplicate_condition.status, 400);

    let concurrent_statuses = std::thread::scope(|scope| {
        let first = document_with_endpoint("managed", "tcp://first");
        let second = document_with_endpoint("managed", "tcp://second");
        let etag = &etag;
        let first_request = scope.spawn(move || {
            request(
                address,
                "PUT",
                "/documents/managed",
                &[("Content-Type", "application/jsonc"), ("If-Match", etag)],
                first.as_bytes(),
            )
            .map(|response| response.status)
        });
        let second_request = scope.spawn(move || {
            request(
                address,
                "PUT",
                "/documents/managed",
                &[("Content-Type", "application/jsonc"), ("If-Match", etag)],
                second.as_bytes(),
            )
            .map(|response| response.status)
        });
        let first_status = first_request
            .join()
            .map_err(|_| io::Error::other("First concurrent request panicked"))??;
        let second_status = second_request
            .join()
            .map_err(|_| io::Error::other("Second concurrent request panicked"))??;
        Ok::<_, io::Error>([first_status, second_status])
    })?;
    let mut concurrent_statuses = concurrent_statuses;
    concurrent_statuses.sort_unstable();
    assert_eq!(concurrent_statuses, [204, 412]);
    let stale_replacement = document_with_source_version("managed", "9.9.9");
    let stale = request(
        address,
        "PUT",
        "/documents/managed",
        &[
            ("Content-Type", "application/jsonc"),
            (
                "If-Match",
                "\"0000000000000000000000000000000000000000000000000000000000000000\"",
            ),
        ],
        stale_replacement.as_bytes(),
    )?;
    assert_eq!(stale.status, 412);

    let pipeline = request(address, "GET", "/pipelines/managed", &[], &[])?;
    assert_eq!(pipeline.status, 200);
    assert_eq!(pipeline.json()["state"], "unready");
    let pipelines = request(address, "GET", "/pipelines", &[], &[])?;
    assert_eq!(pipelines.status, 200);
    assert_eq!(pipelines.json()["pipelines"][0]["id"], "managed");
    assert_eq!(pipelines.json()["pipelines"][0]["state"], "unready");
    assert!(pipelines.json()["pipelines"][0]["runtimeIssues"].is_null());

    for kind in [PluginInterface::Source, PluginInterface::Sink] {
        let package = plugin_package(kind)?;
        let installed = request(
            address,
            "POST",
            "/plugins",
            &[(
                "Content-Type",
                "application/vnd.apache.tenon.plugin+tar+gzip",
            )],
            &package,
        )?;
        assert_eq!(installed.status, 201, "{}", installed.body_text());
        let unchanged = request(
            address,
            "POST",
            "/plugins",
            &[(
                "Content-Type",
                "application/vnd.apache.tenon.plugin+tar+gzip",
            )],
            &package,
        )?;
        assert_eq!(unchanged.status, 204, "{}", unchanged.body_text());
    }

    let sources = request(address, "GET", "/plugins?interface=source", &[], &[])?;
    assert_eq!(sources.status, 200);
    assert_eq!(
        sources.json(),
        serde_json::json!({"plugins": [{
            "programName": "com.example.modbus", "exactVersion": "1.0.0", "interface": "source", "displayName": "Example Plugin", "description": "Read and write example records.", "platforms": plugin_fixture::platforms()
        }]})
    );
    let source_entry = request(
        address,
        "GET",
        "/plugins/com.example.modbus/1.0.0",
        &[],
        &[],
    )?;
    assert_eq!(source_entry.status, 200);
    assert_eq!(source_entry.json(), sources.json()["plugins"][0]);
    let source_contract = request(
        address,
        "GET",
        "/plugins/com.example.modbus/1.0.0/payload-contract",
        &[],
        &[],
    )?;
    assert_eq!(source_contract.status, 200);
    assert_eq!(
        source_contract
            .headers
            .get("content-type")
            .map(String::as_str),
        Some("application/x-protobuf")
    );
    assert!(!source_contract.body.is_empty());

    let conflicting_package =
        plugin_package_with_program(PluginInterface::Source, b"#!/bin/sh\nexit 0\n")?;
    let conflict = request(
        address,
        "POST",
        "/plugins",
        &[(
            "Content-Type",
            "application/vnd.apache.tenon.plugin+tar+gzip",
        )],
        &conflicting_package,
    )?;
    assert_eq!(conflict.status, 409, "{}", conflict.body_text());
    assert_eq!(conflict.json()["error"]["code"], "plugin_version_conflict");

    wait_until(|| {
        let response = request(address, "GET", "/pipelines/managed", &[], &[])?;
        let state = response.json()["state"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        Ok((state == "running").then_some(()))
    })?;

    let schema_path = "/plugins/com.example.modbus/1.0.0/config-schema";
    let original_schema = request(address, "GET", schema_path, &[], &[])?;
    assert_eq!(original_schema.status, 200);
    let private_schema = directory
        .path()
        .join("plugins/programs/com.example.modbus/1.0.0/config.schema.json");
    fs::set_permissions(&private_schema, fs::Permissions::from_mode(0o700))?;
    fs::write(&private_schema, br#"{"type":"null"}"#)?;
    let schema_after_private_edit = request(address, "GET", schema_path, &[], &[])?;
    assert_eq!(schema_after_private_edit.status, 200);
    assert_eq!(schema_after_private_edit.body, original_schema.body);

    let running_document = request(address, "GET", "/documents/managed", &[], &[])?;
    let running_etag = running_document
        .headers
        .get("etag")
        .cloned()
        .ok_or_else(|| io::Error::other("Running ETag is missing"))?;
    let unavailable_source = document_with_source_version("managed", "9.9.9");
    let updated = request(
        address,
        "PUT",
        "/documents/managed",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-Match", &running_etag),
        ],
        unavailable_source.as_bytes(),
    )?;
    assert_eq!(updated.status, 204);
    let unavailable_etag = format!("\"{}\"", sha256_hex(unavailable_source.as_bytes()));
    let unready = request(address, "GET", "/pipelines/managed", &[], &[])?;
    assert_eq!(unready.json()["state"], "unready");
    assert!(unready.json()["appliedDocumentEtag"].is_string());

    let in_use = request(
        address,
        "DELETE",
        "/plugins/com.example.modbus/1.0.0",
        &[],
        &[],
    )?;
    assert_eq!(in_use.status, 409);
    assert_eq!(in_use.json()["error"]["code"], "plugin_in_use");
    assert_eq!(
        in_use.json()["error"]["referencedBy"],
        serde_json::json!([])
    );

    let deleted = request(
        address,
        "DELETE",
        "/documents/managed",
        &[("If-Match", &unavailable_etag)],
        &[],
    )?;
    assert_eq!(deleted.status, 204, "{}", deleted.body_text());
    let deleted_again = request(address, "DELETE", "/documents/managed", &[], &[])?;
    assert_eq!(deleted_again.status, 204, "{}", deleted_again.body_text());
    wait_until(|| {
        let response = request(
            address,
            "DELETE",
            "/plugins/com.example.modbus/1.0.0",
            &[],
            &[],
        )?;
        Ok((response.status == 204).then_some(()))
    })?;
    let plugin_deleted_again = request(
        address,
        "DELETE",
        "/plugins/com.example.modbus/1.0.0",
        &[],
        &[],
    )?;
    assert_eq!(plugin_deleted_again.status, 204);

    runner.terminate()?;
    Ok(())
}

#[test]
fn current_document_schema_is_exact_and_legacy_routes_are_absent() -> io::Result<()> {
    for transport in [
        TestTransport::Http,
        TestTransport::Https,
        TestTransport::MutualTls,
    ] {
        let directory = tempfile::tempdir()?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        transport.configure(&config)?;
        let mut runner = TestRunner::spawn(&config)?;
        wait_for_server(&mut runner, address, transport)?;

        let schema = transport.request(address, "GET", "/document-schema", &[], &[])?;
        assert_eq!(schema.status, 200, "{transport:?}: {}", schema.body_text());
        assert_eq!(schema.headers["content-type"], "application/schema+json");
        assert_eq!(
            schema.body,
            include_bytes!("../contracts/tenon-document/v1.schema.json")
        );
        for (method, path) in [
            ("GET", "/dsl-versions"),
            ("GET", "/dsl-versions/1/tenon-document-schema"),
            ("GET", "/dsl-versions/2/tenon-document-schema"),
            ("GET", "/tenon-documents"),
            ("GET", "/tenon-documents/removed"),
            ("PUT", "/tenon-documents/removed"),
            ("DELETE", "/tenon-documents/removed"),
            ("POST", "/tenon-document-validations"),
            ("POST", "/document-validations"),
            ("POST", "/process-simulations"),
        ] {
            let removed = transport.request(address, method, path, &[], &[])?;
            assert_eq!(
                removed.status,
                404,
                "{transport:?} {method} {path}: {}",
                removed.body_text()
            );
            assert_eq!(removed.json()["error"]["code"], "route_not_found");
        }
        runner.terminate()?;
    }
    Ok(())
}

#[test]
fn openapi_document_is_served_and_covers_every_public_operation() -> io::Result<()> {
    for transport in [
        TestTransport::Http,
        TestTransport::Https,
        TestTransport::MutualTls,
    ] {
        let directory = tempfile::tempdir()?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        transport.configure(&config)?;
        let mut runner = TestRunner::spawn(&config)?;
        wait_for_server(&mut runner, address, transport)?;

        let response = transport.request(address, "GET", "/openapi.json", &[], &[])?;
        assert_eq!(
            response.status,
            200,
            "{transport:?}: {}",
            response.body_text()
        );
        assert_eq!(response.headers["content-type"], "application/json");
        assert_eq!(
            response.headers.get("tenon-version").map(String::as_str),
            Some("0.1.0")
        );

        let document = response.json();
        assert!(
            document["openapi"]
                .as_str()
                .is_some_and(|version| version.starts_with("3.1")),
            "{transport:?}: unexpected openapi version {}",
            document["openapi"]
        );
        assert_eq!(document["info"]["version"], "0.1.0");
        let paths = document["paths"]
            .as_object()
            .ok_or_else(|| io::Error::other("OpenAPI paths object is missing"))?;
        for path in [
            "/documents",
            "/documents/{id}",
            "/document-schema",
            "/pipelines",
            "/pipelines/{id}",
            "/pipelines/{id}/diagnostics",
            "/plugins",
            "/plugins/{program_name}/{exact_version}",
            "/plugins/{program_name}/{exact_version}/config-schema",
            "/plugins/{program_name}/{exact_version}/payload-contract",
        ] {
            assert!(
                paths.contains_key(path),
                "{transport:?}: missing path {path}"
            );
        }
        let document_item = &document["paths"]["/documents/{id}"];
        assert!(
            document_item["get"].is_object(),
            "{transport:?}: GET missing"
        );
        assert!(
            document_item["put"].is_object(),
            "{transport:?}: PUT missing"
        );
        assert!(
            document_item["delete"].is_object(),
            "{transport:?}: DELETE missing"
        );
        assert!(
            document["paths"]["/plugins"]["post"].is_object(),
            "{transport:?}: POST /plugins missing"
        );
        assert!(
            document["paths"]["/plugins/{program_name}/{exact_version}"]["delete"].is_object(),
            "{transport:?}: DELETE program missing"
        );
        let schemas = document["components"]["schemas"]
            .as_object()
            .ok_or_else(|| io::Error::other("OpenAPI components.schemas is missing"))?;
        for schema in [
            "DocumentListBody",
            "PipelineDetailBody",
            "ProgramListBody",
            "ErrorEnvelope",
            "RuntimeResolutionIssue",
            "PluginInterface",
            "Platform",
        ] {
            assert!(
                schemas.contains_key(schema),
                "{transport:?}: missing schema {schema}"
            );
        }
        assert!(
            document["paths"]["/documents"]["get"]["responses"]["200"]["content"]
                ["application/json"]["schema"]
                .is_object(),
            "{transport:?}: list documents 200 has no JSON schema"
        );
        let body = response.body_text();
        for camel in ["pluginInstanceIds", "currentPlatform"] {
            assert!(
                body.contains(camel),
                "{transport:?}: RuntimeResolutionIssue schema is missing camelCase {camel}"
            );
        }
        for snake in ["plugin_instance_ids", "current_platform", "flow_ids"] {
            assert!(
                !body.contains(snake),
                "{transport:?}: RuntimeResolutionIssue schema leaked snake_case {snake}"
            );
        }
        runner.terminate()?;
    }
    Ok(())
}

#[test]
fn invalid_document_versions_fail_startup_without_rewriting_the_store() -> io::Result<()> {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../contracts/tenon-document/test-fixtures/v1.test-vectors.json"
    ))?;
    let cases = vectors["invalid"]
        .as_array()
        .ok_or_else(|| io::Error::other("Schema cases missing"))?
        .iter()
        .filter(|case| {
            case["expectedInstancePointer"] == "/specVersion"
                || case["document"].get("specVersion").is_none()
        });
    let mut checked = 0;
    for case in cases {
        let directory = tempfile::tempdir()?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        let store = directory.path().join("tenon-documents");
        fs::create_dir(&store)?;
        fs::set_permissions(&store, fs::Permissions::from_mode(0o700))?;
        let id = case["document"]["id"]
            .as_str()
            .ok_or_else(|| io::Error::other("Document id missing"))?;
        let path = store.join(format!("{}.jsonc", sha256_hex(id.as_bytes())));
        let source = serde_json::to_vec(&case["document"])?;
        fs::write(&path, &source)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;

        let mut runner = TestRunner::spawn(&config)?;
        runner.wait_for_failure("runner.tenon_document_verification_failed")?;
        assert_eq!(fs::read(&path)?, source, "{}", case["name"]);
        checked += 1;
    }
    assert_eq!(checked, 3);
    Ok(())
}

#[test]
fn validation_and_restart_recovery_are_publicly_observable() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;

    let unknown = request(address, "GET", "/not-a-runner-route", &[], &[])?;
    assert_eq!(unknown.status, 404);
    assert_eq!(unknown.json()["error"]["code"], "route_not_found");
    assert_eq!(
        unknown.headers.get("tenon-version").map(String::as_str),
        Some("0.1.0")
    );

    for kind in [PluginInterface::Source, PluginInterface::Sink] {
        let package = plugin_package(kind)?;
        let installed = request(
            address,
            "POST",
            "/plugins",
            &[(
                "Content-Type",
                "application/vnd.apache.tenon.plugin+tar+gzip",
            )],
            &package,
        )?;
        assert_eq!(installed.status, 201, "{}", installed.body_text());
    }

    let invalid = request(
        address,
        "PUT",
        "/documents/missing-fields",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        br#"{"id":"missing-fields"}"#,
    )?;
    assert_eq!(invalid.status, 422);

    let source = document("restart-persisted");
    let created = request(
        address,
        "PUT",
        "/documents/restart-persisted",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        source.as_bytes(),
    )?;
    assert_eq!(created.status, 201);
    wait_until(|| {
        let response = request(address, "GET", "/pipelines/restart-persisted", &[], &[])?;
        Ok((response.json()["state"] == "running").then_some(()))
    })?;
    runner.terminate()?;

    let mut restarted = TestRunner::spawn(&config)?;
    wait_for_http(&mut restarted, address)?;
    let recovered = request(address, "GET", "/documents/restart-persisted", &[], &[])?;
    assert_eq!(recovered.status, 200);
    assert_eq!(recovered.body, source.as_bytes());
    let recovered_plugin = request(
        address,
        "GET",
        "/plugins/com.example.modbus/1.0.0",
        &[],
        &[],
    )?;
    assert_eq!(recovered_plugin.status, 200);
    wait_until(|| {
        let response = request(address, "GET", "/pipelines/restart-persisted", &[], &[])?;
        Ok((response.json()["state"] == "running").then_some(()))
    })?;
    restarted.terminate()?;
    Ok(())
}

#[test]
fn graceful_shutdown_drains_a_started_plugin_upload() -> io::Result<()> {
    for transport in [
        TestTransport::Http,
        TestTransport::Https,
        TestTransport::MutualTls,
    ] {
        let directory = tempfile::tempdir()?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        transport.configure(&config)?;
        let mut runner = TestRunner::spawn(&config)?;
        wait_for_server(&mut runner, address, transport)?;

        let package = plugin_package(PluginInterface::Source)?;
        let mut stream = transport.connect(address)?;
        write!(
            stream,
            "POST /plugins HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/vnd.apache.tenon.plugin+tar+gzip\r\nContent-Length: {}\r\nExpect: 100-continue\r\n\r\n",
            package.len()
        )?;
        stream.flush()?;
        read_continue_response(&mut stream)?;

        runner.signal_terminate()?;
        wait_until(|| {
            Ok(
                TcpStream::connect_timeout(&address, Duration::from_millis(50))
                    .err()
                    .map(|_| ()),
            )
        })?;
        stream.write_all(&package)?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response)?;
        let response = HttpResponse::parse(response)?;
        assert_eq!(response.status, 201, "{}", response.body_text());
        runner.wait_for_exit()?;
    }
    Ok(())
}

#[test]
fn incomplete_plugin_bodies_never_publish_a_plugin() -> io::Result<()> {
    for transport in [
        TestTransport::Http,
        TestTransport::Https,
        TestTransport::MutualTls,
    ] {
        let directory = tempfile::tempdir()?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        transport.configure(&config)?;
        let mut runner = TestRunner::spawn(&config)?;
        wait_for_server(&mut runner, address, transport)?;

        let mut large = transport.connect(address)?;
        write!(
            large,
            "POST /plugins HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/vnd.apache.tenon.plugin+tar+gzip\r\nContent-Length: {}\r\nExpect: 100-continue\r\n\r\n",
            64_u64 * 1024 * 1024 + 1,
        )?;
        large.flush()?;
        read_continue_response(&mut large)?;
        large.socket().shutdown(Shutdown::Both)?;
        drop(large);

        let package = plugin_package(PluginInterface::Source)?;
        let mut truncated = transport.connect(address)?;
        write!(
            truncated,
            "POST /plugins HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/vnd.apache.tenon.plugin+tar+gzip\r\nContent-Length: {}\r\n\r\n",
            package.len() + 16,
        )?;
        truncated.write_all(&package)?;
        truncated.flush()?;
        truncated.socket().shutdown(Shutdown::Both)?;
        drop(truncated);

        let plugins = transport.request(address, "GET", "/plugins?interface=source", &[], &[])?;
        assert_eq!(plugins.status, 200);
        assert_eq!(plugins.json()["plugins"], serde_json::json!([]));

        runner.terminate()?;
        assert!(
            !directory
                .path()
                .join("plugins/programs/com.example.modbus/1.0.0")
                .exists()
        );
    }
    Ok(())
}

#[test]
fn plugin_staging_failure_stops_the_runner_without_publishing_a_program() -> io::Result<()> {
    for transport in [
        TestTransport::Http,
        TestTransport::Https,
        TestTransport::MutualTls,
    ] {
        let directory = tempfile::tempdir()?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        transport.configure(&config)?;
        let mut runner = TestRunner::spawn(&config)?;
        wait_for_server(&mut runner, address, transport)?;
        fs::remove_dir_all(directory.path().join("plugins"))?;

        let response = transport.request(
            address,
            "POST",
            "/plugins",
            &[(
                "Content-Type",
                "application/vnd.apache.tenon.plugin+tar+gzip",
            )],
            b"package",
        );

        // Fatal Store errors may close the connection before a response is flushed.
        if let Ok(response) = response {
            assert_eq!(response.status, 503, "{}", response.body_text());
            assert_eq!(response.json()["error"]["code"], "runner_shutting_down");
        }
        let Err(failure) = runner.wait_for_exit() else {
            return Err(io::Error::other("Store failure must stop the Runner"));
        };
        assert!(
            failure
                .to_string()
                .contains("Runner Plugin Program Store failed"),
            "{failure}"
        );
        assert!(!directory.path().join("plugins/programs").exists());
    }
    Ok(())
}

#[test]
fn large_document_validates_commits_and_recovers_exact_source() -> io::Result<()> {
    let source = format!("/*{}*/\n{}", "x".repeat(2 * 1024 * 1024), document("large"));
    for transport in [
        TestTransport::Http,
        TestTransport::Https,
        TestTransport::MutualTls,
    ] {
        let directory = tempfile::tempdir()?;
        install_program(directory.path(), PluginInterface::Source)?;
        install_program(directory.path(), PluginInterface::Sink)?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        transport.configure(&config)?;
        let mut runner = TestRunner::spawn(&config)?;
        wait_for_server(&mut runner, address, transport)?;

        let created = transport.request(
            address,
            "PUT",
            "/documents/large",
            &[
                ("Content-Type", "application/jsonc"),
                ("If-None-Match", "*"),
            ],
            source.as_bytes(),
        )?;
        assert_eq!(created.status, 201, "{}", created.body_text());
        let etag = format!("\"{}\"", sha256_hex(source.as_bytes()));
        for restart in [false, true] {
            if restart {
                runner.terminate()?;
                runner = TestRunner::spawn(&config)?;
                wait_for_server(&mut runner, address, transport)?;
            }
            let fetched = transport.request(address, "GET", "/documents/large", &[], &[])?;
            assert_eq!(fetched.status, 200, "{}", fetched.body_text());
            assert_eq!(fetched.body, source.as_bytes());
            assert_eq!(fetched.headers.get("etag"), Some(&etag));
            wait_until(|| {
                let pipeline = transport.request(address, "GET", "/pipelines/large", &[], &[])?;
                Ok((pipeline.json()["appliedDocumentEtag"] == etag).then_some(()))
            })?;
        }
        runner.terminate()?;
    }
    Ok(())
}

#[test]
fn large_plugin_package_installs_with_content_length_and_chunked_transfer() -> io::Result<()> {
    let base = plugin_package(PluginInterface::Source)?;
    let mut base = tar::Archive::new(flate2::read::GzDecoder::new(base.as_slice()));
    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::none());
    let mut archive = tar::Builder::new(encoder);
    for entry in base.entries()? {
        let mut entry = entry?;
        archive.append(&entry.header().clone(), &mut entry)?;
    }
    let resource_bytes = 64 * 1024 * 1024;
    let mut header = tar::Header::new_gnu();
    header.set_size(resource_bytes);
    header.set_mode(0o500);
    header.set_cksum();
    archive.append_data(
        &mut header,
        "resource.bin",
        io::repeat(0).take(resource_bytes),
    )?;
    let package = archive.into_inner()?.finish()?;
    assert!(package.len() > 64 * 1024 * 1024);
    for transport in [
        TestTransport::Http,
        TestTransport::Https,
        TestTransport::MutualTls,
    ] {
        let directory = tempfile::tempdir()?;
        let address = available_address()?;
        let config = write_config(directory.path(), address)?;
        transport.configure(&config)?;
        let mut runner = TestRunner::spawn(&config)?;
        wait_for_server(&mut runner, address, transport)?;

        let response = transport.request(
            address,
            "POST",
            "/plugins",
            &[(
                "Content-Type",
                "application/vnd.apache.tenon.plugin+tar+gzip",
            )],
            &package,
        )?;
        assert_eq!(response.status, 201, "{}", response.body_text());
        let mut stream = transport.connect(address)?;
        write!(
            stream,
            "POST /plugins HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/vnd.apache.tenon.plugin+tar+gzip\r\nTransfer-Encoding: chunked\r\n\r\n",
        )?;
        for chunk in package.chunks(64 * 1024) {
            write!(stream, "{:x}\r\n", chunk.len())?;
            stream.write_all(chunk)?;
            stream.write_all(b"\r\n")?;
        }
        stream.write_all(b"0\r\n\r\n")?;
        stream.flush()?;

        let mut response = Vec::new();
        stream.read_to_end(&mut response)?;
        let response = HttpResponse::parse(response)?;
        assert_eq!(response.status, 204, "{}", response.body_text());
        let plugins = transport.request(address, "GET", "/plugins?interface=source", &[], &[])?;
        assert_eq!(plugins.status, 200);
        assert_eq!(plugins.json()["plugins"].as_array().map(Vec::len), Some(1));
        runner.terminate()?;
    }
    Ok(())
}

fn document(id: &str) -> String {
    document_with_source_version(id, "1.0.0")
}

fn read_until(stream: &mut impl Read, needle: &[u8]) -> io::Result<Vec<u8>> {
    let mut response = Vec::new();
    let mut buffer = [0_u8; 512];
    while !response
        .windows(needle.len())
        .any(|window| window == needle)
    {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            return Err(io::Error::other(
                "HTTP stream ended before the expected SSE event",
            ));
        }
        response.extend_from_slice(&buffer[..read]);
    }
    Ok(response)
}

fn document_with_source_version(id: &str, version: &str) -> String {
    document_with_source_version_and_endpoint(id, version, "tcp://source")
}

fn document_with_endpoint(id: &str, endpoint: &str) -> String {
    document_with_source_version_and_endpoint(id, "1.0.0", endpoint)
}

fn document_with_source_version_and_endpoint(id: &str, version: &str, endpoint: &str) -> String {
    serde_json::json!({
        "specVersion": "1",
        "id": id,
        "pluginInstances": {
            "source": {
                "programName": "com.example.modbus", "exactVersion": version,
                "config": {"endpoint": endpoint}
            },
            "primary": {
                "programName": "com.example.kafka", "exactVersion": "1.0.0",
                "config": {"endpoint": "tcp://sink"}
            }
        },
        "flows": {
            "main": {
                "parallelism": 1, "source": "source",
                "process": {"script": "function main(event) emit() end"},
                "sinks": ["primary"]
            }
        }
    })
    .to_string()
}
