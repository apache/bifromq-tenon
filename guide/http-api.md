<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Runner HTTP API

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../DISCLAIMER).

The running binary serves OpenAPI 3.1 at `GET /openapi.json`. Use it for exact request/response schemas and error details. All responses include `Tenon-Version`. The management surface is intended for trusted operators; see [transport and access](runner.md#transport-and-access) and the [security policy and threat model](../SECURITY.md). Package installation and Document writes control executable workloads; read access can expose configuration secrets and diagnostic data.

| Method | Path | Operation |
| --- | --- | --- |
| GET | `/documents` | List saved Document ids and ETags |
| GET, PUT, DELETE | `/documents/{id}` | Read, save or remove a Document |
| GET | `/document-schema` | Read this binary's Document Schema |
| GET | `/pipelines` | List concise runtime status |
| GET | `/pipelines/{id}` | Read status, issues and plugin details |
| GET | `/pipelines/{id}/diagnostics?target=...` | Subscribe to live SSE diagnostics |
| GET, POST | `/plugins` | List Programs or install a package |
| GET, DELETE | `/plugins/{programName}/{exactVersion}` | Inspect or uninstall one Program |
| GET | `/plugins/{programName}/{exactVersion}/config-schema` | Read Config Schema |
| GET | `/plugins/{programName}/{exactVersion}/payload-contract` | Read FileDescriptorSet bytes |
| GET | `/metrics` | Collect current metrics |

There is no separate health or Document-validation endpoint. Reading `/openapi.json` or `/plugins` can check management availability without creating state. Percent-encode each resource identity as one URL path segment.

## Conditional Document writes

GET returns original `application/jsonc` bytes and a strong ETag. Preserve the complete quoted ETag. Creation requires `If-None-Match: *`; replacement requires `If-Match` with the currently read ETag. The path id must exactly match the body id. Creation returns `201`; replacement returns `204` with the new ETag. Missing preconditions produce `428`, stale preconditions `412`, and malformed headers `400`. Static invalidity produces `422 invalid_tenon_document` with issues. A custom execution policy can reject a valid candidate with `403` before persistence.

DELETE of an existing Document requires the exact `If-Match` and returns `204` once the saved Document has been removed. An absent Document is an idempotent `204`. A successful save means the original bytes are durable and admitted; application proceeds separately. After a lost response, read the Document and compare bytes/ETags before deciding whether to retry. Do not blindly overwrite concurrent edits.

The API imposes no total Document or compressed upload byte limit. Deployment admission and resource controls must fit the environment. JSONC applies to Document/configuration input, while ordinary API responses are JSON. Schemas use `application/schema+json`; payload descriptors use `application/x-protobuf`.

## Runtime status

Pipeline states are `unready`, `starting`, `updating`, `running` and `restart-backoff`. Plugin-instance states are `starting`, `running`, `start-failed` and `restart-backoff`. There is no independent Flow health field.

`documentEtag` always describes the latest persistent Document. `appliedDocumentEtag` appears when a live Pipeline has applied a configuration. Different ETags mean an update is pending or the saved Document is unready while the old configuration continues to run. Check plugin status and external service connectivity separately.

An unready status includes `runtimeIssues`, such as missing Program, platform mismatch, invalid plugin configuration, missing interface or invalid Lua runtime binding. Binding an instance as Source or Sink requires the Program to support that interface; leaving its other supported interface unbound is valid. Details identify the affected Program, Instance or Flow. Restart backoff can include a sanitized `lastError`; when an ETag is included, it identifies the configuration that failed. Resource enforcement is reported for the applied configuration, not an unexecuted desired one.

## Plugin metadata

`GET /plugins` returns a `plugins` array. Each entry and the corresponding `GET /plugins/{programName}/{exactVersion}` response contains `programName`, `exactVersion`, `displayName`, `description`, `interface`, and `platforms`. Display metadata comes from the installed manifest; it is validated on installation and recovery and preserved verbatim. See the [field requirements](plugins.md#display-metadata). Queries do not expose the launch command or the complete manifest.

## Packages and errors

POST `/plugins` accepts raw gzip tar bytes with `Content-Type: application/vnd.apache.tenon.plugin+tar+gzip`. It does not accept multipart, Base64 or a download URL. Identity and interface come from `manifest.json`. A new installation returns `201`, an identical normalized file set returns `204`, and conflicting content for an installed identity returns `409`. See [package rules](plugins.md).

Uninstallation is blocked while persistent Documents or active execution still reference the Program. Removing a Document does not guarantee that its old processes have already exited. Query again after shutdown rather than forcing removal.

Ordinary error bodies have an `error` object with a stable English `code` and `message`, and endpoint-specific details where applicable. Unknown routes return `404`; unsupported operations and invalid inputs follow the running OpenAPI contract. Shutdown stops admission with `503`. Plugin storage failures can shut down the Runner. A Document storage failure returns `500 tenon_document_store_failed`. After a failed or lost write response, read the Document to check whether the change was saved before retrying.

See [observability](observability.md) for metric formats and diagnostic stream lifetimes.
