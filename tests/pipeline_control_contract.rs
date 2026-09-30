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

use std::collections::BTreeMap;
use std::io;

use prost::Message;
use prost_types::{FileDescriptorProto, FileDescriptorSet};
use serde::Deserialize;
use tenon::runner_test_support::contracts::core::{
    ChannelDiagnosticKind, PipelineDiagnosticsToRunner, PipelineToRunner, PluginDiagnosticStream,
    PluginInstanceState, RunnerToPipeline, RunnerToPipelineDiagnostics, pipeline_diagnostic_record,
    pipeline_diagnostics_to_runner, pipeline_to_runner, runner_to_pipeline,
};

const TEST_VECTORS: &[u8] =
    include_bytes!("../contracts/core/test-fixtures/pipeline_control_test_vectors.json");
const DIAGNOSTICS_TEST_VECTORS: &[u8] =
    include_bytes!("../contracts/core/test-fixtures/pipeline_diagnostics_test_vectors.json");
const PIPELINE_FIELD_REGISTRY: &[u8] =
    include_bytes!("../contracts/core/test-fixtures/pipeline_control_field_registry.json");
const PIPELINE_CONTROL_DESCRIPTOR: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/pipeline_control_descriptor.pb"));
const PROCESS_FIELD_REGISTRY: &[u8] =
    include_bytes!("../contracts/plugin/test-fixtures/process_control_field_registry.json");
const PROCESS_CONTROL_DESCRIPTOR: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/process_control_descriptor.pb"));

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PipelineControlTestVectors {
    format_version: u32,
    valid: Vec<ValidVector>,
    #[serde(rename = "reconfiguration")]
    _reconfiguration: Vec<serde_json::Value>,
    malformed: Vec<MalformedVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ValidVector {
    name: String,
    direction: Direction,
    message_kind: MessageKind,
    encoded: Vec<u8>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MalformedVector {
    name: String,
    direction: Direction,
    encoded: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Direction {
    PipelineToRunner,
    RunnerToPipeline,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum MessageKind {
    Attach,
    Bootstrap,
    RevisionPlan,
    StatusSnapshot,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PipelineDiagnosticsTestVectors {
    format_version: u32,
    valid: Vec<DiagnosticsValidVector>,
    malformed: Vec<DiagnosticsMalformedVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DiagnosticsValidVector {
    name: String,
    direction: Direction,
    message_kind: DiagnosticsMessageKind,
    encoded: Vec<u8>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DiagnosticsMalformedVector {
    name: String,
    direction: Direction,
    encoded: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum DiagnosticsMessageKind {
    Attach,
    Interest,
    Batch,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FieldRegistry {
    format_version: u32,
    package: String,
    service: ServiceRegistry,
    diagnostics_service: Option<ServiceRegistry>,
    metrics_service: Option<ServiceRegistry>,
    messages: BTreeMap<String, BTreeMap<String, i32>>,
    oneofs: BTreeMap<String, BTreeMap<String, Vec<String>>>,
    enums: BTreeMap<String, BTreeMap<String, i32>>,
    reserved_field_numbers: BTreeMap<String, Vec<i32>>,
    reserved_field_names: BTreeMap<String, Vec<String>>,
    reserved_enum_numbers: BTreeMap<String, Vec<i32>>,
    reserved_enum_names: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ServiceRegistry {
    name: String,
    method: String,
    client_streaming: bool,
    server_streaming: bool,
    input_type: String,
    output_type: String,
}

#[test]
fn shared_vectors_lock_exact_control_envelope_bytes() -> io::Result<()> {
    let vectors: PipelineControlTestVectors =
        serde_json::from_slice(TEST_VECTORS).map_err(io::Error::other)?;
    assert_eq!(
        vectors.format_version, 1,
        "Unexpected vector format version"
    );

    for vector in vectors.valid {
        match vector.direction {
            Direction::PipelineToRunner => {
                let envelope = PipelineToRunner::decode(vector.encoded.as_slice())
                    .map_err(io::Error::other)?;
                let message_kind = match &envelope.message {
                    Some(pipeline_to_runner::Message::Attach(attach)) => {
                        assert_eq!(
                            attach.launch_id,
                            (0_u8..16).collect::<Vec<_>>(),
                            "{}",
                            vector.name
                        );
                        MessageKind::Attach
                    }
                    Some(pipeline_to_runner::Message::StatusSnapshot(status)) => {
                        assert_eq!(status.document_etag, "aaa", "{}", vector.name);
                        assert_eq!(status.plugin_instances.len(), 1);
                        assert_eq!(status.plugin_instances[0].id, "device");
                        assert_eq!(
                            status.plugin_instances[0].state,
                            PluginInstanceState::Running as i32
                        );
                        MessageKind::StatusSnapshot
                    }
                    None => {
                        return Err(io::Error::other(format!(
                            "Envelope has no message: {}",
                            vector.name
                        )));
                    }
                };
                assert_eq!(message_kind, vector.message_kind, "{}", vector.name);
                assert_eq!(envelope.encode_to_vec(), vector.encoded, "{}", vector.name);
            }
            Direction::RunnerToPipeline => {
                let envelope = RunnerToPipeline::decode(vector.encoded.as_slice())
                    .map_err(io::Error::other)?;
                let message_kind = match &envelope.message {
                    Some(runner_to_pipeline::Message::Bootstrap(bootstrap)) => {
                        let revision_plan = bootstrap
                            .revision_plan
                            .as_ref()
                            .ok_or_else(|| io::Error::other("Vector revision plan is missing"))?;
                        assert_eq!(revision_plan.tenon_document_json, "doc", "{}", vector.name);
                        assert_eq!(
                            bootstrap
                                .environment
                                .as_ref()
                                .ok_or_else(|| io::Error::other("Vector environment is missing"))?
                                .pipeline_working_directory,
                            "/pipelines/a",
                            "{}",
                            vector.name
                        );
                        assert_eq!(revision_plan.plugin_programs.len(), 1);
                        let program = &revision_plan.plugin_programs[0];
                        assert_eq!(program.program_name, "com.example.dual");
                        assert_eq!(program.exact_version, "1.0.0");
                        assert_eq!(program.command, ["./start"]);
                        assert_eq!(program.plugin_interface, 2);
                        assert_eq!(program.payload_descriptor_set, b"d");
                        MessageKind::Bootstrap
                    }
                    Some(runner_to_pipeline::Message::RevisionPlan(revision_plan)) => {
                        assert_eq!(revision_plan.document_etag, "bbb");
                        assert_eq!(revision_plan.tenon_document_json, "doc", "{}", vector.name);
                        MessageKind::RevisionPlan
                    }
                    None => {
                        return Err(io::Error::other(format!(
                            "Envelope has no message: {}",
                            vector.name
                        )));
                    }
                };
                assert_eq!(message_kind, vector.message_kind, "{}", vector.name);
                assert_eq!(envelope.encode_to_vec(), vector.encoded, "{}", vector.name);
            }
        }
    }

    Ok(())
}

#[test]
fn malformed_shared_control_envelopes_are_rejected() -> io::Result<()> {
    let vectors: PipelineControlTestVectors =
        serde_json::from_slice(TEST_VECTORS).map_err(io::Error::other)?;

    for vector in vectors.malformed {
        let rejected = match vector.direction {
            Direction::PipelineToRunner => {
                PipelineToRunner::decode(vector.encoded.as_slice()).is_err()
            }
            Direction::RunnerToPipeline => {
                RunnerToPipeline::decode(vector.encoded.as_slice()).is_err()
            }
        };
        assert!(rejected, "Malformed vector decoded: {}", vector.name);
    }

    Ok(())
}

#[test]
fn shared_vectors_lock_exact_diagnostics_envelope_bytes() -> io::Result<()> {
    let vectors: PipelineDiagnosticsTestVectors =
        serde_json::from_slice(DIAGNOSTICS_TEST_VECTORS).map_err(io::Error::other)?;
    assert_eq!(
        vectors.format_version, 1,
        "Unexpected diagnostics vector format version"
    );

    for vector in vectors.valid {
        match vector.direction {
            Direction::PipelineToRunner => {
                let envelope = PipelineDiagnosticsToRunner::decode(vector.encoded.as_slice())
                    .map_err(io::Error::other)?;
                let message_kind = match &envelope.message {
                    Some(pipeline_diagnostics_to_runner::Message::Attach(attach)) => {
                        assert_eq!(
                            attach.launch_id,
                            (0_u8..16).collect::<Vec<_>>(),
                            "{}",
                            vector.name
                        );
                        DiagnosticsMessageKind::Attach
                    }
                    Some(pipeline_diagnostics_to_runner::Message::Batch(batch)) => {
                        assert_eq!(batch.records.len(), 1, "{}", vector.name);
                        let record = &batch.records[0];
                        match record.record.as_ref() {
                            Some(pipeline_diagnostic_record::Record::Channel(record)) => {
                                assert_eq!(record.flow_id, "telemetry", "{}", vector.name);
                                assert_eq!(record.channel_index, 2, "{}", vector.name);
                                assert_eq!(record.channel_instance_id, 3, "{}", vector.name);
                                assert_eq!(record.observed_at_unix_millis, 4, "{}", vector.name);
                                assert_eq!(record.sequence, 7, "{}", vector.name);
                                assert_eq!(record.lua_vm_instance_id, 9, "{}", vector.name);
                                match ChannelDiagnosticKind::try_from(record.kind) {
                                    Ok(ChannelDiagnosticKind::Print) => {
                                        assert_eq!(record.text, "hello", "{}", vector.name);
                                        assert!(record.truncated, "{}", vector.name);
                                        assert!(record.invalid_utf8, "{}", vector.name);
                                        assert!(record.phase.is_empty(), "{}", vector.name);
                                        assert!(record.code.is_empty(), "{}", vector.name);
                                    }
                                    Ok(ChannelDiagnosticKind::Error) => {
                                        assert_eq!(record.text, "runtime error", "{}", vector.name);
                                        assert!(!record.truncated, "{}", vector.name);
                                        assert!(!record.invalid_utf8, "{}", vector.name);
                                        assert_eq!(record.phase, "lua_main", "{}", vector.name);
                                        assert_eq!(
                                            record.code, "process.lua_main_failed",
                                            "{}",
                                            vector.name
                                        );
                                    }
                                    Err(_) => {
                                        return Err(io::Error::other(format!(
                                            "Unknown Channel diagnostic kind: {}",
                                            vector.name
                                        )));
                                    }
                                }
                            }
                            Some(pipeline_diagnostic_record::Record::Plugin(record)) => {
                                assert_eq!(record.plugin_process_instance_id, 5, "{}", vector.name);
                                assert_eq!(record.observed_at_unix_millis, 4, "{}", vector.name);
                                assert_eq!(record.text, "hello", "{}", vector.name);
                                assert!(record.truncated, "{}", vector.name);
                                assert!(record.invalid_utf8, "{}", vector.name);
                                assert_eq!(record.sequence, 7, "{}", vector.name);
                                assert_eq!(record.plugin_instance_id, "device", "{}", vector.name);
                                assert_eq!(
                                    record.stream,
                                    PluginDiagnosticStream::Stderr as i32,
                                    "{}",
                                    vector.name
                                );
                            }
                            None => {
                                return Err(io::Error::other(format!(
                                    "Diagnostic record is missing: {}",
                                    vector.name
                                )));
                            }
                        }
                        DiagnosticsMessageKind::Batch
                    }
                    None => {
                        return Err(io::Error::other(format!(
                            "Diagnostics envelope has no message: {}",
                            vector.name
                        )));
                    }
                };
                assert_eq!(message_kind, vector.message_kind, "{}", vector.name);
                assert_eq!(envelope.encode_to_vec(), vector.encoded, "{}", vector.name);
            }
            Direction::RunnerToPipeline => {
                let envelope = RunnerToPipelineDiagnostics::decode(vector.encoded.as_slice())
                    .map_err(io::Error::other)?;
                let interest = envelope.interest.as_ref().ok_or_else(|| {
                    io::Error::other(format!("Diagnostics interest is missing: {}", vector.name))
                })?;
                assert_eq!(interest.channels.len(), 1, "{}", vector.name);
                assert_eq!(interest.channels[0].flow_id, "telemetry", "{}", vector.name);
                assert_eq!(interest.channels[0].channel_index, 2, "{}", vector.name);
                assert_eq!(interest.plugin_instance_ids, ["device"], "{}", vector.name);
                assert_eq!(
                    vector.message_kind,
                    DiagnosticsMessageKind::Interest,
                    "{}",
                    vector.name
                );
                assert_eq!(envelope.encode_to_vec(), vector.encoded, "{}", vector.name);
            }
        }
    }

    Ok(())
}

#[test]
fn malformed_shared_diagnostics_envelopes_are_rejected() -> io::Result<()> {
    let vectors: PipelineDiagnosticsTestVectors =
        serde_json::from_slice(DIAGNOSTICS_TEST_VECTORS).map_err(io::Error::other)?;

    for vector in vectors.malformed {
        let rejected = match vector.direction {
            Direction::PipelineToRunner => {
                PipelineDiagnosticsToRunner::decode(vector.encoded.as_slice()).is_err()
            }
            Direction::RunnerToPipeline => {
                RunnerToPipelineDiagnostics::decode(vector.encoded.as_slice()).is_err()
            }
        };
        assert!(rejected, "Malformed vector decoded: {}", vector.name);
    }

    Ok(())
}

#[test]
fn pipeline_field_registry_matches_the_generated_protobuf_descriptor() -> io::Result<()> {
    assert_field_registry_matches_descriptor(PIPELINE_FIELD_REGISTRY, PIPELINE_CONTROL_DESCRIPTOR)
}

#[test]
fn process_field_registry_matches_the_generated_protobuf_descriptor() -> io::Result<()> {
    assert_field_registry_matches_descriptor(PROCESS_FIELD_REGISTRY, PROCESS_CONTROL_DESCRIPTOR)
}

fn assert_field_registry_matches_descriptor(
    field_registry: &[u8],
    descriptor: &[u8],
) -> io::Result<()> {
    let registry: FieldRegistry =
        serde_json::from_slice(field_registry).map_err(io::Error::other)?;
    assert_eq!(registry.format_version, 1, "Unexpected registry version");
    let descriptor_set = FileDescriptorSet::decode(descriptor).map_err(io::Error::other)?;
    let file = descriptor_set
        .file
        .iter()
        .find(|file| file.package.as_deref() == Some(registry.package.as_str()))
        .ok_or_else(|| io::Error::other("Contract descriptor is missing"))?;
    assert_eq!(
        file.service.len(),
        usize::from(registry.diagnostics_service.is_some())
            + usize::from(registry.metrics_service.is_some())
            + 1,
        "Unexpected internal service count"
    );

    let actual_messages = file
        .message_type
        .iter()
        .map(|message| {
            let name = message
                .name
                .clone()
                .ok_or_else(|| io::Error::other("Descriptor message name is missing"))?;
            let fields = message
                .field
                .iter()
                .map(|field| {
                    let field_name = field
                        .name
                        .clone()
                        .ok_or_else(|| io::Error::other("Descriptor field name is missing"))?;
                    let number = field
                        .number
                        .ok_or_else(|| io::Error::other("Descriptor field number is missing"))?;
                    Ok((field_name, number))
                })
                .collect::<io::Result<BTreeMap<_, _>>>()?;
            Ok((name, fields))
        })
        .collect::<io::Result<BTreeMap<_, _>>>()?;
    assert_eq!(actual_messages, registry.messages);

    let actual_oneofs = file
        .message_type
        .iter()
        .filter_map(|message| {
            let message_name = message.name.clone()?;
            let groups = message
                .oneof_decl
                .iter()
                .enumerate()
                .filter_map(|(oneof_index, oneof)| {
                    let fields = message
                        .field
                        .iter()
                        .filter(|field| {
                            field.oneof_index == i32::try_from(oneof_index).ok()
                                && !field.proto3_optional.unwrap_or(false)
                        })
                        .filter_map(|field| field.name.clone())
                        .collect::<Vec<_>>();
                    if fields.is_empty() {
                        None
                    } else {
                        oneof.name.clone().map(|name| (name, fields))
                    }
                })
                .collect::<BTreeMap<_, _>>();
            (!groups.is_empty()).then_some((message_name, groups))
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual_oneofs, registry.oneofs);

    let actual_enums =
        file.enum_type
            .iter()
            .map(|enumeration| {
                let name = enumeration
                    .name
                    .clone()
                    .ok_or_else(|| io::Error::other("Descriptor enum name is missing"))?;
                let values = enumeration
                    .value
                    .iter()
                    .map(|value| {
                        let value_name = value.name.clone().ok_or_else(|| {
                            io::Error::other("Descriptor enum value name is missing")
                        })?;
                        let number = value.number.ok_or_else(|| {
                            io::Error::other("Descriptor enum value number is missing")
                        })?;
                        Ok((value_name, number))
                    })
                    .collect::<io::Result<BTreeMap<_, _>>>()?;
                Ok((name, values))
            })
            .collect::<io::Result<BTreeMap<_, _>>>()?;
    assert_eq!(actual_enums, registry.enums);

    assert_service(file, &registry.package, &registry.service)?;
    if let Some(diagnostics_service) = &registry.diagnostics_service {
        assert_service(file, &registry.package, diagnostics_service)?;
    }

    if let Some(metrics_service) = &registry.metrics_service {
        assert_service(file, &registry.package, metrics_service)?;
    }

    let actual_reserved_fields = file
        .message_type
        .iter()
        .filter_map(|message| {
            let name = message.name.clone()?;
            let numbers = message
                .reserved_range
                .iter()
                .flat_map(|range| range.start.unwrap_or_default()..range.end.unwrap_or_default())
                .collect::<Vec<_>>();
            (!numbers.is_empty()).then_some((name, numbers))
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual_reserved_fields, registry.reserved_field_numbers);

    let actual_reserved_field_names = file
        .message_type
        .iter()
        .filter_map(|message| {
            let name = message.name.clone()?;
            (!message.reserved_name.is_empty()).then_some((name, message.reserved_name.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual_reserved_field_names, registry.reserved_field_names);

    let actual_reserved_enums = file
        .enum_type
        .iter()
        .filter_map(|enumeration| {
            let name = enumeration.name.clone()?;
            let numbers = enumeration
                .reserved_range
                .iter()
                .flat_map(|range| range.start.unwrap_or_default()..=range.end.unwrap_or_default())
                .collect::<Vec<_>>();
            (!numbers.is_empty()).then_some((name, numbers))
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual_reserved_enums, registry.reserved_enum_numbers);

    let actual_reserved_enum_names = file
        .enum_type
        .iter()
        .filter_map(|enumeration| {
            let name = enumeration.name.clone()?;
            (!enumeration.reserved_name.is_empty())
                .then_some((name, enumeration.reserved_name.clone()))
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual_reserved_enum_names, registry.reserved_enum_names);
    Ok(())
}

fn assert_service(
    file: &FileDescriptorProto,
    package: &str,
    expected: &ServiceRegistry,
) -> io::Result<()> {
    let service = file
        .service
        .iter()
        .find(|service| service.name.as_deref() == Some(expected.name.as_str()))
        .ok_or_else(|| io::Error::other("Internal service is missing"))?;
    let method = service
        .method
        .iter()
        .find(|method| method.name.as_deref() == Some(expected.method.as_str()))
        .ok_or_else(|| io::Error::other("Internal service method is missing"))?;
    assert_eq!(
        service.method.len(),
        1,
        "Unexpected internal service method"
    );
    assert_eq!(
        method.client_streaming.unwrap_or(false),
        expected.client_streaming
    );
    assert_eq!(
        method.server_streaming.unwrap_or(false),
        expected.server_streaming
    );
    assert_eq!(
        method.input_type.as_deref(),
        Some(format!(".{package}.{}", expected.input_type).as_str())
    );
    assert_eq!(
        method.output_type.as_deref(),
        Some(format!(".{package}.{}", expected.output_type).as_str())
    );
    Ok(())
}

#[test]
fn metrics_stream_round_trips_the_shared_byte_vectors() -> io::Result<()> {
    use tenon::runner_test_support::contracts::core::{
        PipelineMetricsToRunner, RunnerToPipelineMetrics, pipeline_metrics_to_runner,
    };
    let vectors: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../contracts/core/test-fixtures/pipeline_metrics_test_vectors.json"
    ))?;
    assert_eq!(vectors["formatVersion"], 1);
    let valid: Vec<serde_json::Value> = serde_json::from_value(vectors["valid"].clone())?;
    for vector in valid {
        let bytes: Vec<u8> = serde_json::from_value(vector["encoded"].clone())?;
        if vector["direction"] == "pipelineToRunner" {
            let message =
                PipelineMetricsToRunner::decode(bytes.as_slice()).map_err(io::Error::other)?;
            let kind = match message.message {
                Some(pipeline_metrics_to_runner::Message::Attach(_)) => "attach",
                Some(pipeline_metrics_to_runner::Message::Snapshot(_)) => "snapshot",
                None => return Err(io::Error::other("Metrics message variant is missing")),
            };
            assert_eq!(vector["messageKind"], kind, "{}", vector["name"]);
            assert_eq!(message.encode_to_vec(), bytes, "{}", vector["name"]);
        } else {
            let message =
                RunnerToPipelineMetrics::decode(bytes.as_slice()).map_err(io::Error::other)?;
            assert_eq!(message.encode_to_vec(), bytes, "{}", vector["name"]);
        }
    }
    let malformed: Vec<serde_json::Value> = serde_json::from_value(vectors["malformed"].clone())?;
    for vector in malformed {
        let bytes: Vec<u8> = serde_json::from_value(vector["encoded"].clone())?;
        let invalid = if vector["direction"] == "pipelineToRunner" {
            PipelineMetricsToRunner::decode(bytes.as_slice()).is_err()
        } else {
            RunnerToPipelineMetrics::decode(bytes.as_slice()).is_err()
        };
        assert!(invalid, "{}", vector["name"]);
    }
    Ok(())
}
