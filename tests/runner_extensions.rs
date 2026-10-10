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

//! Exercises a separately linked distribution through real HTTP and child processes.

#[path = "runner_extensions/artifacts.rs"]
mod artifacts;
use plugin_fixture::file_tree;
#[path = "runner_extensions/http_authorization.rs"]
mod http_authorization;
#[path = "runner_cli/plugin_fixture.rs"]
mod plugin_fixture;
#[path = "runner_extensions/policy_changes.rs"]
mod policy_changes;
#[path = "support/runner_http.rs"]
mod runner_http_support;

use plugin_fixture::{PluginInterface, install_program};
use runner_http_support::{
    TestRunner, available_address, request, sha256_hex, wait_for_http, write_config,
};
use rustix::io::Errno;
use rustix::process;
use serde_json::Value;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Barrier;
use std::{fs, io, thread};

const EXECUTABLE: &str = env!("CARGO_BIN_EXE_runner-extension-fixture");

fn configure(root: &Path, address: SocketAddr, extra: Value) -> io::Result<PathBuf> {
    let config = write_config(root, address)?;
    let mut value: Value = serde_json::from_slice(&fs::read(&config)?)?;
    value["extra"] = extra;
    fs::write(&config, serde_json::to_vec(&value)?)?;
    Ok(config)
}

fn document(id: &str, endpoint: &str) -> Vec<u8> {
    serde_json::json!({
        "specVersion": "1", "id": id,
        "pluginInstances": {
            "source": {"programName": "com.example.modbus", "exactVersion": "1.0.0", "config": {"endpoint": endpoint}},
            "sink": {"programName": "com.example.kafka", "exactVersion": "1.0.0", "config": {"endpoint": "tcp://sink"}}
        },
        "flows": {"main": {"parallelism": 1, "source": "source", "process": {"script": "function main(event) emit() end"}, "sinks": ["sink"]}}
    }).to_string().into_bytes()
}

fn formal_file(root: &Path, id: &str) -> PathBuf {
    root.join("tenon-documents")
        .join(format!("{}.jsonc", sha256_hex(id.as_bytes())))
}

#[test]
fn protected_document_round_trips_and_partial_failure_preserves_committed_bytes() -> io::Result<()>
{
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    let config = configure(root.path(), address, serde_json::json!({}))?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let mut source = b"// Preserve exact authored bytes\n".to_vec();
    source.extend(document("protected", "secret-fixture-value"));
    let path = "/documents/protected";
    let created = request(
        address,
        "PUT",
        path,
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        &source,
    )?;
    assert_eq!(created.status, 201, "{}", created.body_text());
    let before = request(address, "GET", path, &[], &[])?;
    assert_eq!(before.body, source);
    let stored = fs::read(formal_file(root.path(), "protected"))?;
    assert!(stored.starts_with(b"fixture:"));
    assert!(
        !stored
            .windows(b"secret-fixture-value".len())
            .any(|part| part == b"secret-fixture-value")
    );
    let rejected = request(
        address,
        "PUT",
        path,
        &[
            ("Content-Type", "application/jsonc"),
            ("If-Match", &before.headers["etag"]),
        ],
        &document("protected", "fail-protection"),
    )?;
    assert_eq!(rejected.status, 500, "{}", rejected.body_text());
    assert_eq!(fs::read(formal_file(root.path(), "protected"))?, stored);
    assert_eq!(request(address, "GET", path, &[], &[])?.body, source);
    runner.terminate()?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let recovered = request(address, "GET", path, &[], &[])?;
    assert_eq!(recovered.body, source);
    assert_eq!(recovered.headers["etag"], before.headers["etag"]);
    runner.terminate()?;

    fs::write(formal_file(root.path(), "protected"), &source)?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    runner.wait_for_failure("Fixture protected header is missing")?;
    assert_eq!(fs::read(formal_file(root.path(), "protected"))?, source);
    Ok(())
}

#[test]
fn concurrent_candidates_cannot_both_take_the_last_document_slot() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"maximumDocuments": 1}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let barrier = Barrier::new(2);
    let responses = thread::scope(|scope| -> io::Result<_> {
        let create = |id| {
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                request(
                    address,
                    "PUT",
                    &format!("/documents/{id}"),
                    &[
                        ("Content-Type", "application/jsonc"),
                        ("If-None-Match", "*"),
                    ],
                    &document(id, "capacity"),
                )
            })
        };
        let first = create("first");
        let second = create("second");
        Ok([
            first
                .join()
                .map_err(|_| io::Error::other("First request panicked"))??,
            second
                .join()
                .map_err(|_| io::Error::other("Second request panicked"))??,
        ])
    })?;
    let mut statuses = responses
        .iter()
        .map(|response| response.status)
        .collect::<Vec<_>>();
    statuses.sort_unstable();
    assert_eq!(statuses, [201, 403]);
    let denied = responses
        .iter()
        .find(|response| response.status == 403)
        .ok_or_else(|| io::Error::other("Capacity refusal is missing"))?;
    assert_eq!(denied.json()["error"]["code"], "fixture.capacity");
    assert_eq!(
        ["first", "second"]
            .iter()
            .filter(|id| formal_file(root.path(), id).exists())
            .count(),
        1
    );
    runner.terminate()
}

#[test]
fn initializer_failure_precedes_store_creation() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let config = configure(
        root.path(),
        available_address()?,
        serde_json::json!({"failInitialization": true}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    runner.wait_for_failure("Fixture initialization rejected")?;
    assert!(!root.path().join("tenon-documents").exists());
    Ok(())
}

#[test]
fn startup_policy_rejection_never_starts_a_pipeline() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    seed_running_document(root.path(), address)?;
    let stored = fs::read(formal_file(root.path(), "runtime"))?;
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"maximumDocuments": 0}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let visible = request(address, "GET", "/documents/runtime", &[], &[])?;
    assert_eq!(visible.status, 200);
    let pipeline = request(address, "GET", "/pipelines/runtime", &[], &[])?;
    assert_eq!(pipeline.status, 404);
    assert_eq!(fs::read(formal_file(root.path(), "runtime"))?, stored);
    assert!(file_tree::named_files(&root.path().join("pipelines"), "parent.pid")?.is_empty());
    runner.terminate()
}

#[test]
fn startup_accepts_cumulatively_and_continues_after_a_middle_rejection() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    let config = configure(root.path(), address, serde_json::json!({}))?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    for id in ["doc3", "doc2", "doc1"] {
        assert_eq!(
            request(
                address,
                "PUT",
                &format!("/documents/{id}"),
                &[
                    ("Content-Type", "application/jsonc"),
                    ("If-None-Match", "*")
                ],
                &document(id, id)
            )?
            .status,
            201
        );
    }
    runner.terminate()?;
    let log = root.path().join("scopes.jsonl");
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"deniedDocument": "doc2", "scopeLog": log}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let scopes: Vec<Value> = fs::read_to_string(&log)?
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    let ids = scopes
        .iter()
        .map(|scope| {
            let mut ids = scope
                .as_array()
                .into_iter()
                .flatten()
                .map(|doc| doc["id"].as_str().unwrap_or_default())
                .collect::<Vec<_>>();
            ids.sort_unstable();
            ids
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        [
            vec![],
            vec!["doc1"],
            vec!["doc1", "doc2"],
            vec!["doc1", "doc3"]
        ]
    );
    for id in ["doc1", "doc2", "doc3"] {
        assert_eq!(
            request(address, "GET", &format!("/documents/{id}"), &[], &[])?.status,
            200
        );
        assert_eq!(
            request(address, "GET", &format!("/pipelines/{id}"), &[], &[])?.status,
            if id == "doc2" { 404 } else { 200 }
        );
    }
    runner.terminate()
}

#[test]
fn rejected_startup_document_can_retry_identical_bytes_after_capacity_is_freed() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    let config = configure(root.path(), address, serde_json::json!({}))?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    for id in ["first", "second"] {
        assert_eq!(
            request(
                address,
                "PUT",
                &format!("/documents/{id}"),
                &[
                    ("Content-Type", "application/jsonc"),
                    ("If-None-Match", "*")
                ],
                &document(id, id)
            )?
            .status,
            201
        );
    }
    runner.terminate()?;
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"maximumDocuments": 1}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let second = request(address, "GET", "/documents/second", &[], &[])?;
    let retry = || {
        request(
            address,
            "PUT",
            "/documents/second",
            &[
                ("Content-Type", "application/jsonc"),
                ("If-Match", &second.headers["etag"]),
            ],
            &second.body,
        )
    };
    assert_eq!(retry()?.status, 403);
    assert_eq!(
        request(address, "GET", "/pipelines/second", &[], &[])?.status,
        404
    );
    assert_eq!(
        request(address, "GET", "/documents/second", &[], &[])?.headers["etag"],
        second.headers["etag"]
    );
    let first = request(address, "GET", "/documents/first", &[], &[])?;
    assert_eq!(
        request(
            address,
            "DELETE",
            "/documents/first",
            &[("If-Match", &first.headers["etag"])],
            &[]
        )?
        .status,
        204
    );
    assert_eq!(retry()?.status, 204);
    assert_eq!(
        request(address, "GET", "/pipelines/second", &[], &[])?.status,
        200
    );
    assert_eq!(
        request(address, "GET", "/documents/second", &[], &[])?.body,
        second.body
    );
    runner.terminate()
}

#[test]
fn plugin_installation_uses_the_already_authorized_desired_state() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    let config = configure(root.path(), address, serde_json::json!({}))?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let created = request(
        address,
        "PUT",
        "/documents/waiting",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        &document("waiting", "unready"),
    )?;
    assert_eq!(created.status, 201);
    runner.terminate()?;
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"maximumAuthorizations": 2}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let package = plugin_fixture::plugin_package(plugin_fixture::PluginInterface::Source)?;
    let installed = request(
        address,
        "POST",
        "/plugins",
        &[("Content-Type", "application/octet-stream")],
        &package,
    )?;
    assert_eq!(installed.status, 201, "{}", installed.body_text());
    assert!(
        plugin_fixture::stored_package_path(root.path(), "com.example.modbus", "1.0.0").is_file()
    );
    assert_eq!(
        request(address, "GET", "/documents/waiting", &[], &[])?.status,
        200
    );
    runner.terminate()
}

fn seed_running_document(root: &Path, address: SocketAddr) -> io::Result<()> {
    install_program(root, PluginInterface::Source)?;
    install_program(root, PluginInterface::Sink)?;
    let config = configure(root, address, serde_json::json!({}))?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let response = request(
        address,
        "PUT",
        "/documents/runtime",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        &document("runtime", "tcp://source"),
    )?;
    assert_eq!(response.status, 201, "{}", response.body_text());
    runner.terminate()
}

fn running_processes(root: &Path) -> io::Result<Vec<process::Pid>> {
    let mut processes = Vec::new();
    runner_http_support::wait_until(|| {
        let ready = file_tree::named_files(&root.join("pipelines"), "ready.received")?;
        if ready.len() != 2 {
            return Ok(None);
        }
        let markers = file_tree::named_files(&root.join("pipelines"), "process.pid")?
            .into_iter()
            .chain(file_tree::named_files(
                &root.join("pipelines"),
                "parent.pid",
            )?);
        processes.clear();
        for marker in markers {
            let pid = fs::read_to_string(marker)?
                .parse::<i32>()
                .map_err(io::Error::other)?;
            processes.push(
                process::Pid::from_raw(pid)
                    .ok_or_else(|| io::Error::other("Fixture process id is invalid"))?,
            );
        }
        processes.sort_unstable_by_key(|pid| pid.as_raw_pid());
        processes.dedup();
        Ok((processes.len() == 3).then_some(()))
    })?;
    Ok(processes)
}

fn assert_reaped(processes: &[process::Pid]) {
    for pid in processes {
        assert_eq!(
            process::test_kill_process(*pid),
            Err(Errno::SRCH),
            "Process {pid} remains alive"
        );
    }
}

#[test]
fn expiry_without_http_requests_stops_real_processes_and_initializes_only_runner() -> io::Result<()>
{
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    seed_running_document(root.path(), address)?;
    let marker = root.path().join("initialized");
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"expiresAfterMs": 2500, "initializeMarker": marker}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    // Only filesystem observations follow startup: no HTTP call drives expiry.
    let processes = running_processes(root.path())?;
    assert!(marker.is_file());
    runner.wait_for_failure("runner.execution_permit_expired")?;
    assert_reaped(&processes);
    assert_eq!(fs::read_dir(root.path().join("pipelines"))?.count(), 0);
    Ok(())
}

#[test]
fn failed_document_write_keeps_the_new_authorization_deadline() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    seed_running_document(root.path(), address)?;
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"expiresAfterMs": 1500, "expiresFromAuthorization": 3}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let processes = running_processes(root.path())?;
    let before = request(address, "GET", "/documents/runtime", &[], &[])?;
    let stored = fs::read(formal_file(root.path(), "runtime"))?;
    let failed = request(
        address,
        "PUT",
        "/documents/runtime",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-Match", &before.headers["etag"]),
        ],
        &document("runtime", "fail-protection"),
    )?;
    assert_eq!(failed.status, 500, "{}", failed.body_text());
    let after = request(address, "GET", "/documents/runtime", &[], &[])?;
    assert_eq!(after.body, before.body);
    assert_eq!(after.headers["etag"], before.headers["etag"]);
    assert_eq!(fs::read(formal_file(root.path(), "runtime"))?, stored);
    // Startup was unlimited. Only the failed PUT supplied this deadline.
    runner.wait_for_failure("runner.execution_permit_expired")?;
    assert_reaped(&processes);
    Ok(())
}

#[test]
fn rejected_document_updates_entitlement_and_expires_running_processes() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    seed_running_document(root.path(), address)?;
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"expiresAfterMs": 1500, "expiresFromAuthorization": 3, "maximumDocuments": 1}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let processes = running_processes(root.path())?;
    let before = request(address, "GET", "/documents/runtime", &[], &[])?;
    let stored = fs::read(formal_file(root.path(), "runtime"))?;
    let rejected = request(
        address,
        "PUT",
        "/documents/rejected",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        &document("rejected", "second"),
    )?;
    assert_eq!(rejected.status, 403, "{}", rejected.body_text());
    assert!(!formal_file(root.path(), "rejected").exists());
    let after = request(address, "GET", "/documents/runtime", &[], &[])?;
    assert_eq!(after.body, before.body);
    assert_eq!(after.headers["etag"], before.headers["etag"]);
    assert_eq!(fs::read(formal_file(root.path(), "runtime"))?, stored);
    // Startup was unlimited. The rejected PUT supplied the entitlement deadline.
    runner.wait_for_failure("runner.execution_permit_expired")?;
    assert_reaped(&processes);
    Ok(())
}

#[test]
fn automatic_pipeline_restart_uses_the_already_authorized_desired_state() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    seed_running_document(root.path(), address)?;
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"maximumAuthorizations": 2, "initializeMarker": root.path().join("initialized")}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    let processes = running_processes(root.path())?;
    let marker = file_tree::named_files(&root.path().join("pipelines"), "parent.pid")?.remove(0);
    let parent = fs::read_to_string(marker)?
        .parse::<i32>()
        .map_err(io::Error::other)?;
    let pid = process::Pid::from_raw(parent)
        .ok_or_else(|| io::Error::other("Pipeline pid is invalid"))?;
    process::kill_process(pid, process::Signal::KILL)?;
    runner_http_support::wait_until(|| {
        let parents = file_tree::named_files(&root.path().join("pipelines"), "parent.pid")?;
        Ok(parents
            .iter()
            .any(|marker| fs::read_to_string(marker).is_ok_and(|value| value != parent.to_string()))
            .then_some(()))
    })?;
    assert_reaped(&processes);
    runner.terminate()
}

#[test]
fn unfinished_upload_cannot_delay_expiry_even_after_normal_shutdown_begins() -> io::Result<()> {
    use std::io::{Read as _, Write as _};
    for begin_normal_shutdown in [false, true] {
        let root = tempfile::tempdir()?;
        let address = available_address()?;
        seed_running_document(root.path(), address)?;
        let config = configure(
            root.path(),
            address,
            serde_json::json!({"expiresAfterMs": 2500}),
        )?;
        let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
        wait_for_http(&mut runner, address)?;
        let processes = running_processes(root.path())?;
        let mut upload = TcpStream::connect(address)?;
        upload.set_read_timeout(Some(runner_http_support::DEADLINE))?;
        write!(
            upload,
            "POST /plugins HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/octet-stream\r\nContent-Length: 1000000\r\nExpect: 100-continue\r\n\r\n"
        )?;
        runner_http_support::read_continue_response(&mut upload)?;
        if begin_normal_shutdown {
            runner.signal_terminate()?;
        }
        runner.wait_for_failure("runner.execution_permit_expired")?;
        let mut remaining = Vec::new();
        match upload.read_to_end(&mut remaining) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
            Err(error) => return Err(error),
        }
        assert_reaped(&processes);
    }
    Ok(())
}

#[test]
fn deletion_removes_desired_capacity_while_the_old_process_is_still_retiring() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    install_program(root.path(), PluginInterface::Source)?;
    install_program(root.path(), PluginInterface::Sink)?;
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"maximumDocuments": 1}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let mut source: Value = serde_json::from_slice(&document("retiring", "old"))?;
    source["pluginInstances"]["sink"]["config"]["behavior"] = "delay-shutdown".into();
    let created = request(
        address,
        "PUT",
        "/documents/retiring",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        &serde_json::to_vec(&source)?,
    )?;
    assert_eq!(created.status, 201);
    let processes = running_processes(root.path())?;
    let removed = request(
        address,
        "DELETE",
        "/documents/retiring",
        &[(
            "If-Match",
            &format!("\"{}\"", sha256_hex(&serde_json::to_vec(&source)?)),
        )],
        &[],
    )?;
    assert_eq!(removed.status, 204);
    let mut shutdown_markers = Vec::new();
    runner_http_support::wait_until(|| {
        shutdown_markers =
            file_tree::named_files(&root.path().join("pipelines"), "shutdown.received")?;
        Ok((!shutdown_markers.is_empty()).then_some(()))
    })?;
    let put = || {
        request(
            address,
            "PUT",
            "/documents/replacement",
            &[
                ("Content-Type", "application/jsonc"),
                ("If-None-Match", "*"),
            ],
            &document("replacement", "new"),
        )
    };
    let accepted = put()?;
    assert_eq!(accepted.status, 201, "{}", accepted.body_text());
    assert!(formal_file(root.path(), "replacement").exists());
    assert!(
        processes
            .iter()
            .any(|pid| process::test_kill_process(*pid).is_ok())
    );
    for marker in shutdown_markers {
        fs::write(marker.with_file_name("allow-shutdown"), b"release")?;
    }
    runner_http_support::wait_until(|| {
        Ok(processes
            .iter()
            .all(|pid| process::test_kill_process(*pid) == Err(Errno::SRCH))
            .then_some(()))
    })?;
    runner.terminate()
}

#[test]
fn policy_observes_only_desired_documents_while_old_revisions_are_still_running() -> io::Result<()>
{
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    install_program(root.path(), PluginInterface::Source)?;
    install_program(root.path(), PluginInterface::Sink)?;
    let scope_log = root.path().join("scopes.jsonl");
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"scopeLog": scope_log}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let mut old: Value = serde_json::from_slice(&document("updating", "old"))?;
    old["pluginInstances"]["source"]["config"]["behavior"] = "delay-quiesce".into();
    let created = request(
        address,
        "PUT",
        "/documents/updating",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        &serde_json::to_vec(&old)?,
    )?;
    assert_eq!(created.status, 201);
    let _ = running_processes(root.path())?;
    let updated = request(
        address,
        "PUT",
        "/documents/updating",
        &[
            ("Content-Type", "application/jsonc"),
            (
                "If-Match",
                &format!("\"{}\"", sha256_hex(&serde_json::to_vec(&old)?)),
            ),
        ],
        &document("updating", "new"),
    )?;
    assert_eq!(updated.status, 204);
    let mut quiesce_markers = Vec::new();
    runner_http_support::wait_until(|| {
        quiesce_markers =
            file_tree::named_files(&root.path().join("pipelines"), "quiesce-source.received")?;
        Ok((!quiesce_markers.is_empty()).then_some(()))
    })?;
    let mut unready: Value = serde_json::from_slice(&document("updating", "unready"))?;
    unready["pluginInstances"]["source"]["exactVersion"] = "9.9.9".into();
    let stored = request(
        address,
        "PUT",
        "/documents/updating",
        &[
            ("Content-Type", "application/jsonc"),
            (
                "If-Match",
                &format!("\"{}\"", sha256_hex(&document("updating", "new"))),
            ),
        ],
        &serde_json::to_vec(&unready)?,
    )?;
    assert_eq!(stored.status, 204);
    let probe = request(
        address,
        "PUT",
        "/documents/probe",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        &document("probe", "probe"),
    )?;
    assert_eq!(probe.status, 201, "{}", probe.body_text());
    let logged = fs::read_to_string(&scope_log)?;
    let scopes: Vec<Value> = logged
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    assert_eq!(scopes.len(), 5);
    let mut final_scope = scopes[4]
        .as_array()
        .ok_or_else(|| io::Error::other("Scope is not an array"))?
        .clone();
    final_scope.sort_by_key(|document| document["id"].as_str().map(str::to_owned));
    assert_eq!(
        final_scope,
        serde_json::json!([
            {"id": "probe", "endpoint": "probe"},
            {"id": "updating", "endpoint": "unready"}
        ])
        .as_array()
        .ok_or_else(|| io::Error::other("Expected scope is not an array"))?
        .clone()
    );
    for marker in quiesce_markers {
        fs::write(marker.with_file_name("allow-quiesce"), b"release")?;
    }
    runner.terminate()
}
