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
use crate::payload_contract::PluginInterface;
use crate::runner::diagnostics::RunnerDiagnostics;
use crate::runner::extensions::RunnerHooks;
use crate::runner::management::{RunnerManagementEvent, RunnerManagementSupervisor};
use crate::runner::plugin::package::tests::{
    equivalent_source_program_package, program_package, source_program_package_with_command,
    source_program_package_with_resource, valid_config_schema, valid_program_package,
};
use crate::runner::plugin::store::PluginProgramStore;
use crate::runner::recovery::recover;
use crate::runner::state_directory::prepare_runner_state_directory;
use crate::runner::test_support::load_config;
use axum::Router;
use axum::body::to_bytes;
use axum::http::Request;
use axum::routing::get;
use bytes::Bytes;
use serde_json::{Value, json};
use std::fs;
use std::io::{self, Read as _, Write as _};
use tonic::codegen::Service as _;

#[tokio::test(flavor = "current_thread")]
async fn upload_publishes_all_interfaces_with_exact_locations_and_idempotent_retries()
-> io::Result<()> {
    let (directory, mut supervisor, mut app) = program_http()?;
    for (interface, name) in [
        (PluginInterface::Source, "com.example.source"),
        (PluginInterface::Sink, "com.example.sink"),
        (PluginInterface::SourceAndSink, "com.example.gateway"),
    ] {
        let package = valid_program_package(interface)?;
        for status in [StatusCode::CREATED, StatusCode::NO_CONTENT] {
            let request = Request::post("/plugins")
                .header("Content-Type", "application/octet-stream")
                .body(Body::from(package.clone()))
                .map_err(io::Error::other)?;
            let response = exchange(&mut supervisor, &mut app, request).await?;
            assert_eq!(response.status(), status);
            assert_eq!(
                response.headers()["location"],
                format!("/plugins/{name}/1.0.0")
            );
            assert!(
                to_bytes(response.into_body(), 1024)
                    .await
                    .map_err(io::Error::other)?
                    .is_empty()
            );

            let request = Request::get(format!("/plugins/{name}/1.0.0"))
                .body(Body::empty())
                .map_err(io::Error::other)?;
            let response = exchange(&mut supervisor, &mut app, request).await?;
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = to_bytes(response.into_body(), 1024)
                .await
                .map_err(io::Error::other)?;
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes)?,
                json!({
                    "programName": name, "exactVersion": "1.0.0", "interface": interface,
                    "displayName": "Example Plugin", "description": "Read and write example records.",
                    "platforms": [Platform::CURRENT],
                })
            );
        }
    }
    supervisor.shutdown().await.map_err(io::Error::other)?;
    drop(supervisor);
    let recovered = PluginProgramStore::recover(
        directory.path().join("plugins/programs"),
        directory.path().join("plugins/programs"),
        std::sync::Arc::new(crate::runner::extensions::ByPass),
    )
    .map_err(io::Error::other)?;
    assert_eq!(recovered.programs().count(), 3);
    for (_, _, entry) in recovered.programs() {
        assert_eq!(entry.display_name(), "Example Plugin");
        assert_eq!(entry.description(), "Read and write example records.");
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn normalized_retry_and_conflict_keep_the_committed_package() -> io::Result<()> {
    let (directory, mut supervisor, mut app) = program_http()?;
    for (package, expected) in [
        (
            valid_program_package(PluginInterface::Source)?,
            StatusCode::CREATED,
        ),
        (equivalent_source_program_package()?, StatusCode::NO_CONTENT),
        (
            source_program_package_with_resource("resources/data.bin", b"changed")?,
            StatusCode::CONFLICT,
        ),
    ] {
        let response = exchange(
            &mut supervisor,
            &mut app,
            upload_request(Body::from(package))?,
        )
        .await?;
        assert_eq!(response.status(), expected);
        if expected == StatusCode::CONFLICT {
            assert_eq!(
                response_json(response).await?["error"]["code"],
                "plugin_version_conflict"
            );
        }
    }
    let root = directory.path().join("plugins/programs");
    assert_eq!(
        fs::read(root.join("com.example.source/.tenon-artifact-92521fc3cbd964bdc9f584a991b89fddaa5754ed1cc96d6d42445338669c1305"))?,
        valid_program_package(PluginInterface::Source)?
    );
    assert_eq!(fs::read_dir(root)?.count(), 1);
    supervisor.shutdown().await.map_err(io::Error::other)?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_uploads_and_expansion_limits_do_not_poison_the_store() -> io::Result<()> {
    let (directory, mut supervisor, mut app) = program_http()?;
    for (package, status, code) in [
        (
            b"not gzip".to_vec(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "plugin_package_invalid",
        ),
        (
            source_program_package_with_command("")?,
            StatusCode::UNPROCESSABLE_ENTITY,
            "plugin_manifest_invalid",
        ),
        (
            program_package(PluginInterface::Source, vec![], b"{}".to_vec())?,
            StatusCode::UNPROCESSABLE_ENTITY,
            "plugin_config_schema_invalid",
        ),
        (
            program_package(PluginInterface::Source, vec![], valid_config_schema())?,
            StatusCode::UNPROCESSABLE_ENTITY,
            "plugin_payload_contract_invalid",
        ),
        (
            oversized_manifest_archive()?,
            StatusCode::PAYLOAD_TOO_LARGE,
            "plugin_package_too_large",
        ),
        (
            truncated_manifest_archive()?,
            StatusCode::UNPROCESSABLE_ENTITY,
            "plugin_package_invalid",
        ),
    ] {
        let response = exchange(
            &mut supervisor,
            &mut app,
            upload_request(Body::from(package))?,
        )
        .await?;
        assert_eq!(response.status(), status, "{code}");
        assert_eq!(response_json(response).await?["error"]["code"], code);
        assert_empty_store(&directory, &mut supervisor, &mut app).await?;
    }
    let response = exchange(
        &mut supervisor,
        &mut app,
        upload_request(Body::from(valid_program_package(PluginInterface::Source)?))?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    supervisor.shutdown().await.map_err(io::Error::other)?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_content_type_and_failed_body_publish_nothing() -> io::Result<()> {
    let package = valid_program_package(PluginInterface::Source)?;
    let (directory, mut supervisor, mut app) = program_http()?;
    for media_type in [
        "application/json",
        "application/vnd.apache.tenon.plugin+tar+gzip",
    ] {
        let request = Request::post("/plugins")
            .header("Content-Type", media_type)
            .body(Body::empty())
            .map_err(io::Error::other)?;
        assert_eq!(
            exchange(&mut supervisor, &mut app, request).await?.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
    }
    let complete_archive = Bytes::from(package.clone());
    let body = Body::from_stream(tokio_stream::iter([
        Ok(complete_archive),
        Err(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "Client disconnected",
        )),
    ]));
    // Early body rejection does not wait for the owned upload to finish.
    let (response, event) = tokio::join!(
        app.call(upload_request(body)?),
        supervisor.next_event(|_| Ok(()))
    );
    publish_management_event(event)?;
    assert_eq!(
        response.map_err(io::Error::other)?.status(),
        StatusCode::BAD_REQUEST
    );
    assert_empty_store(&directory, &mut supervisor, &mut app).await?;

    let response = exchange(
        &mut supervisor,
        &mut app,
        upload_request(Body::from(package))?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    supervisor.shutdown().await.map_err(io::Error::other)?;
    Ok(())
}

fn oversized_manifest_archive() -> io::Result<Vec<u8>> {
    let size = 16 * 1024 * 1024 + 1;
    let mut header = tar::Header::new_gnu();
    header.set_path("manifest.json")?;
    header.set_mode(0o500);
    header.set_size(size);
    header.set_cksum();
    let gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut archive = tar::Builder::new(gzip);
    archive.append(&header, io::repeat(0).take(size))?;
    archive.into_inner()?.finish()
}

fn truncated_manifest_archive() -> io::Result<Vec<u8>> {
    let mut header = tar::Header::new_gnu();
    header.set_path("manifest.json")?;
    header.set_mode(0o500);
    header.set_size(1024 * 1024 * 1024);
    header.set_cksum();
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(header.as_bytes())?;
    gzip.finish()
}

fn upload_request(body: Body) -> io::Result<Request<Body>> {
    Request::post("/plugins")
        .header("Content-Type", "application/octet-stream")
        .body(body)
        .map_err(io::Error::other)
}

async fn response_json(response: Response) -> io::Result<Value> {
    let bytes = to_bytes(response.into_body(), 4096)
        .await
        .map_err(io::Error::other)?;
    serde_json::from_slice(&bytes).map_err(io::Error::other)
}

async fn assert_empty_store(
    directory: &tempfile::TempDir,
    supervisor: &mut RunnerManagementSupervisor,
    app: &mut Router,
) -> io::Result<()> {
    let request = Request::get("/plugins")
        .body(Body::empty())
        .map_err(io::Error::other)?;
    let response = exchange(supervisor, app, request).await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response_json(response).await?, json!({"plugins": []}));
    assert!(
        fs::read_dir(directory.path().join("plugins/programs"))?
            .next()
            .is_none()
    );
    Ok(())
}

fn program_http() -> io::Result<(tempfile::TempDir, RunnerManagementSupervisor, Router)> {
    let directory = tempfile::tempdir()?;
    let layout = prepare_runner_state_directory(directory.path()).map_err(io::Error::other)?;
    let config = load_config(directory.path())?;
    let recovered = recover(
        &config,
        &layout,
        &RunnerHooks::default().artifact_protection,
        &layout.pipeline_runtime_directory(),
    )
    .map_err(io::Error::other)?;
    let recovered = RunnerManagementSupervisor::recover(
        directory.path(),
        config.script_vm_limits(),
        recovered,
        RunnerHooks::default().artifact_protection,
        None,
        |_| Ok(()),
    )?;
    let (supervisor, management, directives) = RunnerManagementSupervisor::start(
        recovered,
        config.tenon_document_verifier().map_err(io::Error::other)?,
    );
    assert!(directives.is_empty());
    let app = Router::new()
        .route("/plugins", get(list_programs).post(install_program))
        .route(
            "/plugins/{program_name}/{exact_version}",
            get(get_program).delete(delete_program),
        )
        .with_state(HttpServices {
            management,
            diagnostics: RunnerDiagnostics::new(),
            metrics: crate::runner::metrics::test_support::empty()?,
        });
    Ok((directory, supervisor, app))
}

async fn exchange(
    supervisor: &mut RunnerManagementSupervisor,
    app: &mut Router,
    request: Request<Body>,
) -> io::Result<Response> {
    let response = app.call(request);
    tokio::pin!(response);
    loop {
        tokio::select! {
            result = &mut response => return result.map_err(io::Error::other),
            event = supervisor.next_event(|_| Ok(())) => publish_management_event(event)?,
        }
    }
}

fn publish_management_event(event: RunnerManagementEvent) -> io::Result<()> {
    match event {
        RunnerManagementEvent::CommitReady(commit) => commit.publish_after(|directives| {
            assert!(directives.is_empty());
            Ok(())
        }),
        RunnerManagementEvent::Failure(error) => Err(io::Error::other(error)),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn incompatible_upload_is_a_redacted_422_and_does_not_stop_management() -> io::Result<()> {
    use crate::runner::plugin::package::tests::{foreign_platform, package_with_platforms};
    use crate::runner::plugin::platform::Platform;
    let (directory, mut supervisor, mut app) = program_http()?;
    let foreign: Platform = serde_json::from_value(foreign_platform())?;
    let package = package_with_platforms(
        &valid_program_package(PluginInterface::Source)?,
        &json!([foreign]),
    )?;
    let response = exchange(
        &mut supervisor,
        &mut app,
        upload_request(Body::from(package))?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let bytes = to_bytes(response.into_body(), 4096)
        .await
        .map_err(io::Error::other)?;
    let error: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(error["error"]["code"], "plugin_platform_mismatch");
    assert_eq!(
        error["error"]["message"],
        format!(
            "Plugin platforms [{foreign}] do not include Runner platform {}",
            Platform::CURRENT
        )
    );
    assert_eq!(
        fs::read_dir(directory.path().join("plugins/programs"))?.count(),
        0
    );
    let response = exchange(
        &mut supervisor,
        &mut app,
        upload_request(Body::from(valid_program_package(PluginInterface::Source)?))?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    supervisor.shutdown().await.map_err(io::Error::other)
}
