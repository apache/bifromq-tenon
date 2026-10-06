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
use runner_http_support::{HttpResponse, TestTransport, wait_for_response};
use std::io::{Read as _, Write as _};

const READER: (&str, &str) = ("Authorization", "Bearer reader");
const WRITER: (&str, &str) = ("Authorization", "Bearer writer");

#[test]
fn authorization_controls_real_http_and_tls_requests_before_upload() -> io::Result<()> {
    for transport in [
        TestTransport::Http,
        TestTransport::Https,
        TestTransport::MutualTls,
    ] {
        let root = tempfile::tempdir()?;
        let address = available_address()?;
        let config = configure(root.path(), address, serde_json::json!({"httpAuth": true}))?;
        transport.configure(&config)?;
        let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
        wait_for_response(&mut runner, || {
            transport.request(address, "GET", "/document-schema", &[READER], &[])
        })?;
        for headers in [&[][..], &[("Authorization", "Bearer wrong")][..]] {
            let denied = transport.request(address, "GET", "/document-schema", headers, &[])?;
            assert_eq!(denied.status, 401, "{transport:?}: {}", denied.body_text());
            assert_eq!(
                denied.headers["www-authenticate"],
                "Bearer realm=\"fixture\""
            );
            assert_eq!(denied.headers["tenon-version"], env!("CARGO_PKG_VERSION"));
            assert_eq!(denied.json()["error"]["code"], "unauthorized");
        }
        if matches!(transport, TestTransport::MutualTls) {
            assert!(
                TestTransport::Https
                    .request(address, "GET", "/document-schema", &[WRITER], &[])
                    .is_err()
            );
        }
        let head = transport.request(address, "HEAD", "/document-schema", &[READER], &[])?;
        assert_eq!(head.status, 200);
        assert!(head.body.is_empty());
        let path = "/documents/authorized";
        let source = document("authorized", "http-authorization");
        for method in ["PUT", "DELETE"] {
            let denied = transport.request(address, method, path, &[READER], &source)?;
            assert_eq!(denied.status, 403);
            assert_eq!(denied.json()["error"]["code"], "forbidden");
            assert!(!formal_file(root.path(), "authorized").exists());
        }
        let created = transport.request(
            address,
            "PUT",
            path,
            &[
                WRITER,
                ("Content-Type", "application/jsonc"),
                ("If-None-Match", "*"),
            ],
            &source,
        )?;
        assert_eq!(created.status, 201, "{}", created.body_text());
        let read = transport.request(address, "GET", path, &[READER], &[])?;
        assert_eq!(read.body, source);
        assert_eq!(
            transport
                .request(
                    address,
                    "DELETE",
                    path,
                    &[WRITER, ("If-Match", &read.headers["etag"])],
                    &[]
                )?
                .status,
            204
        );
        // Send headers only: rejection must arrive without an interim 100 or body bytes.
        let mut upload = transport.connect(address)?;
        write!(
            upload,
            "POST /plugins HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/octet-stream\r\nContent-Length: 1000000\r\nExpect: 100-continue\r\n\r\n"
        )?;
        upload.flush()?;
        let mut bytes = Vec::new();
        upload.read_to_end(&mut bytes)?;
        let rejected = HttpResponse::parse(bytes)?;
        assert_eq!(rejected.status, 401);
        assert_eq!(
            fs::read_dir(root.path().join("plugins/programs"))?.count(),
            0
        );
        assert_eq!(fs::read_dir(root.path().join("plugins"))?.count(), 1);
        runner.terminate()?;
    }
    Ok(())
}

#[test]
fn sse_connections_are_authorized_individually_and_existing_stream_drains_on_shutdown()
-> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    let config = configure(root.path(), address, serde_json::json!({"httpAuth": true}))?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_response(&mut runner, || {
        request(address, "GET", "/document-schema", &[READER], &[])
    })?;
    assert_eq!(
        request(
            address,
            "PUT",
            "/documents/stream",
            &[
                WRITER,
                ("Content-Type", "application/jsonc"),
                ("If-None-Match", "*")
            ],
            &document("stream", "unready")
        )?
        .status,
        201
    );
    let path = "/pipelines/stream/diagnostics?target=flow%3Amain%2Fchannel%3A0";
    assert_eq!(request(address, "GET", path, &[], &[])?.status, 401);
    let mut stream = TestTransport::Http.connect(address)?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer reader\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n"
    )?;
    stream.flush()?;
    let mut bytes = Vec::new();
    let mut byte = [0_u8; 1];
    while !bytes.ends_with(b"event: attached") {
        stream.read_exact(&mut byte)?;
        bytes.push(byte[0]);
    }
    assert!(bytes.starts_with(b"HTTP/1.1 200"));
    // A new connection must supply its own credentials, even while one is already open.
    assert_eq!(request(address, "GET", path, &[], &[])?.status, 401);
    runner.terminate()?;
    stream.read_to_end(&mut bytes)?;
    assert!(String::from_utf8_lossy(&bytes).contains("event: closed"));
    Ok(())
}

#[test]
fn http_refusal_preserves_running_processes_and_does_not_refresh_execution_deadline()
-> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    seed_running_document(root.path(), address)?;
    let log = root.path().join("scopes.jsonl");
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"httpAuth": true, "expiresAfterMs": 2500, "scopeLog": log}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    let processes = running_processes(root.path())?;
    let source = fs::read(formal_file(root.path(), "runtime"))?;
    let rejected = request(
        address,
        "PUT",
        "/documents/runtime",
        &[
            READER,
            ("Content-Type", "application/jsonc"),
            ("If-Match", "*"),
        ],
        &document("runtime", "denied"),
    )?;
    assert_eq!(rejected.status, 403);
    assert_eq!(fs::read(formal_file(root.path(), "runtime"))?, source);
    assert_eq!(fs::read_to_string(&log)?.lines().count(), 2);
    for pid in &processes {
        assert!(process::test_kill_process(*pid).is_ok());
    }
    runner.wait_for_failure("runner.execution_permit_expired")?;
    assert_reaped(&processes);
    Ok(())
}
