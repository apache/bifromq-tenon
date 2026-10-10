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

//! Plugin package HTTP transport handlers.

use super::app::HttpServices;
use super::response::{
    ErrorEnvelope, RunnerHttpResponse, SCHEMA_JSON, bad_path, has_content_type,
    service_unavailable, unsupported_media_type,
};
use crate::identifiers::{ExactVersion, ProgramName};
use crate::payload_contract::PluginInterface;
use crate::runner::management::{
    PluginDeleteFailure, PluginOperationFailure, PluginProgramInterfaceFilter,
    PluginProgramResource, PluginProgramView, PluginResourceKind, RunnerPluginUpload,
};
use crate::runner::plugin::platform::Platform;
use crate::runner::plugin::store::PluginProgramInstallResult;
use axum::body::Body;
use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt as _;
use utoipa::ToSchema;

/// The Program collection response: one summary per available Program.
#[derive(Serialize, ToSchema)]
struct ProgramListBody<'a> {
    plugins: Vec<ProgramViewBody<'a>>,
}

/// The Program metadata summary shared by the list and single reads.
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct ProgramViewBody<'a> {
    program_name: &'a str,
    exact_version: &'a str,
    display_name: &'a str,
    description: &'a str,
    interface: PluginInterface,
    platforms: &'a [Platform],
}

/// The 409 body returned when a Program is still referenced.
#[derive(Serialize, ToSchema)]
struct PluginInUseEnvelope {
    error: PluginInUseBody,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct PluginInUseBody {
    code: &'static str,
    message: &'static str,
    referenced_by: Vec<String>,
}

#[utoipa::path(
    post,
    path = "/plugins",
    tag = "plugins",
    request_body(content(
        (Vec<u8> = "application/octet-stream"),
    ), description = "Plugin package bytes for the configured decoder"),
    responses(
        (status = 201, description = "Installed; the Location header holds the Program path"),
        (status = 204, description = "Unchanged; the Location header holds the Program path"),
        (status = 400, description = "The request body could not be read", body = ErrorEnvelope),
        (status = 409, description = "A different package already holds this version", body = ErrorEnvelope),
        (status = 413, description = "The package exceeds the size limit", body = ErrorEnvelope),
        (status = 415, description = "The Content-Type is not application/octet-stream", body = ErrorEnvelope),
        (status = 422, description = "Package access was rejected, package validation failed, or the platform does not match", body = ErrorEnvelope),
        (status = 503, description = "Runner is shutting down", body = ErrorEnvelope),
    )
)]
/// Installs a complete Program upload before returning its immutable identity.
pub(super) async fn install_program(
    State(state): State<HttpServices>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    if !has_content_type(&headers, "application/octet-stream") {
        return unsupported_media_type();
    }
    let Ok(upload) = state.management.begin_program_install().await else {
        return service_unavailable();
    };
    receive_upload(body, upload, program_install_response).await
}

async fn receive_upload(
    body: Body,
    mut upload: RunnerPluginUpload,
    render: fn(Result<PluginProgramInstallResult, PluginOperationFailure>) -> Response,
) -> Response {
    let mut body = body.into_data_stream();
    while let Some(chunk) = body.next().await {
        let Ok(chunk) = chunk else {
            return RunnerHttpResponse::error(
                StatusCode::BAD_REQUEST,
                "invalid_request_body",
                "Plugin package request body could not be read",
            )
            .into_response();
        };
        if upload.write(chunk).await.is_err() {
            return match upload.finish().await {
                Ok(result) => render(result),
                Err(_) => service_unavailable(),
            };
        }
    }
    let Ok(result) = upload.finish().await else {
        return service_unavailable();
    };
    render(result)
}

/// The only supported filter on the unified Program collection.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProgramListQuery {
    interface: Option<String>,
}

#[utoipa::path(
    get,
    path = "/plugins",
    tag = "plugins",
    params(("interface" = Option<String>, Query, description = "Optional interface filter: source or sink")),
    responses(
        (status = 200, description = "Program summaries, optionally filtered by interface", body = ProgramListBody),
        (status = 400, description = "The interface filter is invalid", body = ErrorEnvelope),
        (status = 503, description = "Runner is shutting down", body = ErrorEnvelope),
    )
)]
/// Returns available Program summaries, optionally filtered by one interface.
pub(super) async fn list_programs(
    State(state): State<HttpServices>,
    query: Result<Query<ProgramListQuery>, QueryRejection>,
) -> Response {
    let Ok(Query(query)) = query else {
        return invalid_plugin_filter();
    };
    let interface = match query.interface.as_deref() {
        None => None,
        Some("source") => Some(PluginProgramInterfaceFilter::Source),
        Some("sink") => Some(PluginProgramInterfaceFilter::Sink),
        Some(_) => return invalid_plugin_filter(),
    };
    let Ok(programs) = state.management.list_programs(interface).await else {
        return service_unavailable();
    };
    program_list_response(&programs)
}

fn invalid_plugin_filter() -> Response {
    RunnerHttpResponse::error(
        StatusCode::BAD_REQUEST,
        "invalid_plugin_filter",
        "Plugin interface filter is invalid",
    )
    .into_response()
}

#[utoipa::path(
    get,
    path = "/plugins/{program_name}/{exact_version}",
    tag = "plugins",
    params(
        ("program_name" = String, Path, description = "Reverse-domain Program name"),
        ("exact_version" = String, Path, description = "Immutable package version"),
    ),
    responses(
        (status = 200, description = "The Program metadata summary", body = ProgramViewBody),
        (status = 400, description = "The request path is invalid", body = ErrorEnvelope),
        (status = 404, description = "The Program does not exist", body = ErrorEnvelope),
        (status = 503, description = "Runner is shutting down", body = ErrorEnvelope),
    )
)]
/// Returns the same minimal summary as the collection or a not-found response.
pub(super) async fn get_program(
    State(state): State<HttpServices>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Response {
    let Ok(Path(path)) = path else {
        return bad_path();
    };
    get_program_resource(state, path, PluginResourceKind::Entry).await
}

#[utoipa::path(
    get,
    path = "/plugins/{program_name}/{exact_version}/config-schema",
    tag = "plugins",
    params(
        ("program_name" = String, Path, description = "Reverse-domain Program name"),
        ("exact_version" = String, Path, description = "Immutable package version"),
    ),
    responses(
        (status = 200, description = "The Program's original Config Schema", content_type = "application/schema+json"),
        (status = 400, description = "The request path is invalid", body = ErrorEnvelope),
        (status = 404, description = "The Program does not exist", body = ErrorEnvelope),
        (status = 503, description = "Runner is shutting down", body = ErrorEnvelope),
    )
)]
/// Returns the original Config Schema bytes of an available Program.
pub(super) async fn get_program_config_schema(
    State(state): State<HttpServices>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Response {
    let Ok(Path(path)) = path else {
        return bad_path();
    };
    get_program_resource(state, path, PluginResourceKind::ConfigSchema).await
}

#[utoipa::path(
    get,
    path = "/plugins/{program_name}/{exact_version}/payload-contract",
    tag = "plugins",
    params(
        ("program_name" = String, Path, description = "Reverse-domain Program name"),
        ("exact_version" = String, Path, description = "Immutable package version"),
    ),
    responses(
        (status = 200, description = "The Program's original FileDescriptorSet", content_type = "application/x-protobuf"),
        (status = 400, description = "The request path is invalid", body = ErrorEnvelope),
        (status = 404, description = "The Program does not exist", body = ErrorEnvelope),
        (status = 503, description = "Runner is shutting down", body = ErrorEnvelope),
    )
)]
/// Returns the original FileDescriptorSet bytes of an available Program.
pub(super) async fn get_program_payload_contract(
    State(state): State<HttpServices>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Response {
    let Ok(Path(path)) = path else {
        return bad_path();
    };
    get_program_resource(state, path, PluginResourceKind::PayloadContract).await
}

async fn get_program_resource(
    state: HttpServices,
    (program_name, exact_version): (String, String),
    resource: PluginResourceKind,
) -> Response {
    let (Ok(program_name), Ok(exact_version)) = (
        ProgramName::try_from(program_name),
        ExactVersion::try_from(exact_version),
    ) else {
        return bad_path();
    };
    let Ok(result) = state
        .management
        .program(program_name, exact_version, resource)
        .await
    else {
        return service_unavailable();
    };
    program_resource_result_response(result)
}

#[utoipa::path(
    delete,
    path = "/plugins/{program_name}/{exact_version}",
    tag = "plugins",
    params(
        ("program_name" = String, Path, description = "Reverse-domain Program name"),
        ("exact_version" = String, Path, description = "Immutable package version"),
    ),
    responses(
        (status = 204, description = "Deleted, or idempotently absent"),
        (status = 400, description = "The request path is invalid", body = ErrorEnvelope),
        (status = 409, description = "The Program is still referenced by a Document", body = PluginInUseEnvelope),
        (status = 503, description = "Runner is shutting down", body = ErrorEnvelope),
    )
)]
/// Deletes an exact unified Program without exposing Store failure states.
pub(super) async fn delete_program(
    State(state): State<HttpServices>,
    path: Result<Path<(String, String)>, PathRejection>,
) -> Response {
    let Ok(Path((program_name, exact_version))) = path else {
        return bad_path();
    };
    let (Ok(program_name), Ok(exact_version)) = (
        ProgramName::try_from(program_name),
        ExactVersion::try_from(exact_version),
    ) else {
        return bad_path();
    };
    let Ok(result) = state
        .management
        .delete_program(program_name, exact_version)
        .await
    else {
        return service_unavailable();
    };
    plugin_delete_response(result)
}

fn program_install_response(
    result: Result<PluginProgramInstallResult, PluginOperationFailure>,
) -> Response {
    let (status, identity) = match result {
        Ok(PluginProgramInstallResult::Installed(identity)) => (StatusCode::CREATED, identity),
        Ok(PluginProgramInstallResult::Unchanged(identity)) => (StatusCode::NO_CONTENT, identity),
        Err(error) => return plugin_operation_failure_response(error),
    };
    RunnerHttpResponse::empty(status)
        .with_location(format!(
            "/plugins/{}/{}",
            identity.program_name(),
            identity.exact_version()
        ))
        .into_response()
}

fn program_list_response(programs: &[PluginProgramView]) -> Response {
    let body = ProgramListBody {
        plugins: programs.iter().map(program_view_body).collect(),
    };
    RunnerHttpResponse::serialized(StatusCode::OK, &body).into_response()
}

fn program_view_body(program: &PluginProgramView) -> ProgramViewBody<'_> {
    ProgramViewBody {
        program_name: program.program_name.as_str(),
        exact_version: program.exact_version.as_str(),
        display_name: &program.display_name,
        description: &program.description,
        interface: program.interface,
        platforms: &program.platforms,
    }
}

fn program_resource_result_response(result: Option<PluginProgramResource>) -> Response {
    match result {
        Some(PluginProgramResource::Entry(program)) => {
            RunnerHttpResponse::serialized(StatusCode::OK, &program_view_body(&program))
                .into_response()
        }
        Some(PluginProgramResource::ConfigSchema(bytes)) => {
            RunnerHttpResponse::bytes(StatusCode::OK, SCHEMA_JSON, bytes.into_vec()).into_response()
        }
        Some(PluginProgramResource::PayloadContract(bytes)) => {
            RunnerHttpResponse::bytes(StatusCode::OK, "application/x-protobuf", bytes.into_vec())
                .into_response()
        }
        None => RunnerHttpResponse::error(
            StatusCode::NOT_FOUND,
            "plugin_not_found",
            "Plugin Program was not found",
        )
        .into_response(),
    }
}

fn plugin_delete_response(result: Result<(), PluginDeleteFailure>) -> Response {
    match result {
        Ok(()) => RunnerHttpResponse::empty(StatusCode::NO_CONTENT).into_response(),
        Err(PluginDeleteFailure::InUse { referenced_by }) => RunnerHttpResponse::serialized(
            StatusCode::CONFLICT,
            &PluginInUseEnvelope {
                error: PluginInUseBody {
                    code: "plugin_in_use",
                    message: "Plugin is still referenced",
                    referenced_by: referenced_by
                        .iter()
                        .map(|id| id.as_str().to_owned())
                        .collect(),
                },
            },
        )
        .into_response(),
    }
}

fn plugin_operation_failure_response(error: PluginOperationFailure) -> Response {
    let (status, code) = match error {
        PluginOperationFailure::PlatformMismatch { platforms } => {
            return RunnerHttpResponse::error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "plugin_platform_mismatch",
                &crate::runner::plugin::store::PluginStoreError::PlatformMismatch { platforms }
                    .to_string(),
            )
            .into_response();
        }
        PluginOperationFailure::TooLarge => {
            (StatusCode::PAYLOAD_TOO_LARGE, "plugin_package_too_large")
        }
        PluginOperationFailure::Conflict { code } => (StatusCode::CONFLICT, code),
        PluginOperationFailure::Invalid { code } => (StatusCode::UNPROCESSABLE_ENTITY, code),
    };
    RunnerHttpResponse::error(status, code, "Plugin operation failed").into_response()
}

#[cfg(test)]
mod tests;
