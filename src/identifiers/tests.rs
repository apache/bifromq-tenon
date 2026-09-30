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

use super::{
    ExactVersion, FlowId, IdentifierParseError, PluginInstanceId, PluginProgramIdentity,
    ProgramName, SinkContractId, TenonDocumentId,
};
use crate::contracts::tenon_document::v1_schema_bytes;
use proptest::prelude::*;
use proptest::test_runner::TestRunner;
use serde::Deserialize;
use serde_json::Value;
use std::io;

const TEST_VECTORS: &[u8] =
    include_bytes!("../../contracts/core/test-fixtures/domain-identifiers.test-vectors.json");

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentifierTestVectors {
    valid: Vec<ValidVector>,
    invalid: Vec<InvalidVector>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidVector {
    name: String,
    kind: IdentifierKind,
    value: String,
    expected: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvalidVector {
    name: String,
    kind: IdentifierKind,
    value: String,
    expected_error_code: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
enum IdentifierKind {
    TenonDocumentId,
    PluginInstanceId,
    FlowId,
    ProgramName,
    ExactVersion,
    SinkContractId,
}

#[test]
fn tenon_document_and_sink_ids_preserve_valid_input() -> Result<(), IdentifierParseError> {
    let tenon_document_id = TenonDocumentId::try_from(" pipeline-A ")?;
    let sink_id = PluginInstanceId::try_from("kafka-primary")?;

    assert_eq!(tenon_document_id.as_str(), " pipeline-A ");
    assert_eq!(sink_id.as_str(), "kafka-primary");

    Ok(())
}

#[test]
fn tenon_document_and_sink_ids_reject_invalid_boundaries() {
    assert_eq!(
        TenonDocumentId::try_from("").map_err(|error| error.code()),
        Err("identifier.empty")
    );
    assert_eq!(
        PluginInstanceId::try_from("sink\u{001f}id").map_err(|error| error.code()),
        Err("identifier.control_character")
    );
}

#[test]
fn program_name_and_exact_version_preserve_canonical_input() -> Result<(), IdentifierParseError> {
    let program_name = ProgramName::try_from("com.example.kafka")?;
    let exact_version = ExactVersion::try_from("2.0.0-rc.1")?;

    assert_eq!(program_name.as_str(), "com.example.kafka");
    assert_eq!(exact_version.as_str(), "2.0.0-rc.1");

    Ok(())
}

#[test]
fn program_name_and_exact_version_reject_noncanonical_input() {
    assert_eq!(
        ProgramName::try_from("Com.example.kafka").map_err(|error| error.code()),
        Err("program_name.invalid")
    );
    assert_eq!(
        ExactVersion::try_from("1.0.0+build.1").map_err(|error| error.code()),
        Err("exact_version.invalid")
    );
}

#[test]
fn sink_contract_id_round_trips_through_validated_parts() -> Result<(), IdentifierParseError> {
    let parsed = SinkContractId::try_from("com.example.kafka@2.0.0-rc.1")?;
    let from_parts = SinkContractId::from_parts(
        ProgramName::try_from("com.example.kafka")?,
        ExactVersion::try_from("2.0.0-rc.1")?,
    );

    assert_eq!(parsed, from_parts);
    assert_eq!(parsed.program_name().as_str(), "com.example.kafka");
    assert_eq!(parsed.exact_version().as_str(), "2.0.0-rc.1");
    assert_eq!(parsed.to_string(), "com.example.kafka@2.0.0-rc.1");

    Ok(())
}

#[test]
fn sink_contract_id_rejects_bad_join_and_preserves_component_error_codes() {
    assert_eq!(
        SinkContractId::try_from("com.example.kafka").map_err(|error| error.code()),
        Err("sink_contract_id.invalid")
    );
    assert_eq!(
        SinkContractId::try_from("Com.example.kafka@1.0.0").map_err(|error| error.code()),
        Err("program_name.invalid")
    );
    assert_eq!(
        SinkContractId::try_from("com.example.kafka@1.0").map_err(|error| error.code()),
        Err("exact_version.invalid")
    );
}

#[test]
fn shared_vectors_fix_canonical_values_and_stable_error_codes() -> io::Result<()> {
    let vectors: IdentifierTestVectors =
        serde_json::from_slice(TEST_VECTORS).map_err(io::Error::other)?;

    for vector in vectors.valid {
        let actual = parse_identifier(vector.kind, &vector.value).map_err(io::Error::other)?;
        assert_eq!(
            actual, vector.expected,
            "valid vector {} must preserve its canonical value",
            vector.name
        );
    }

    for vector in vectors.invalid {
        let Err(error) = parse_identifier(vector.kind, &vector.value) else {
            return Err(io::Error::other(format!(
                "invalid vector {} was accepted",
                vector.name
            )));
        };
        assert_eq!(
            error.code(),
            vector.expected_error_code,
            "invalid vector {} must produce one stable error code",
            vector.name
        );
    }

    Ok(())
}

#[test]
fn program_and_version_parsers_match_the_schema_for_arbitrary_input() -> io::Result<()> {
    let program_name_validator = schema_definition_validator("programName")?;
    let exact_version_validator = schema_definition_validator("exactVersion")?;
    let strategy = (any::<String>(), any::<String>());
    let mut runner = TestRunner::default();

    runner
        .run(&strategy, |(program_name, exact_version)| {
            let program_name_accepted = ProgramName::try_from(program_name.as_str()).is_ok();
            let exact_version_accepted = ExactVersion::try_from(exact_version.as_str()).is_ok();

            prop_assert_eq!(
                program_name_validator.is_valid(&Value::String(program_name)),
                program_name_accepted
            );
            prop_assert_eq!(
                exact_version_validator.is_valid(&Value::String(exact_version)),
                exact_version_accepted
            );

            Ok(())
        })
        .map_err(io::Error::other)
}

fn schema_definition_validator(name: &str) -> io::Result<jsonschema::Validator> {
    let schema: Value = serde_json::from_slice(v1_schema_bytes()).map_err(io::Error::other)?;
    let pointer = format!("/$defs/{name}");
    let definition = schema.pointer(&pointer).ok_or_else(|| {
        io::Error::other(format!("missing Tenon Document Schema definition {name}"))
    })?;

    jsonschema::draft202012::new(definition).map_err(io::Error::other)
}

fn parse_identifier(kind: IdentifierKind, value: &str) -> Result<String, IdentifierParseError> {
    match kind {
        IdentifierKind::TenonDocumentId => {
            TenonDocumentId::try_from(value).map(|id| id.to_string())
        }
        IdentifierKind::PluginInstanceId => {
            PluginInstanceId::try_from(value).map(|id| id.to_string())
        }
        IdentifierKind::FlowId => {
            FlowId::try_from(value.to_owned()).map(|id| id.as_str().to_owned())
        }
        IdentifierKind::ProgramName => ProgramName::try_from(value).map(|id| id.to_string()),
        IdentifierKind::ExactVersion => ExactVersion::try_from(value).map(|id| id.to_string()),
        IdentifierKind::SinkContractId => SinkContractId::try_from(value).map(|id| id.to_string()),
    }
}

proptest! {
    #[test]
    fn local_id_acceptance_matches_the_utf8_contract(value in any::<String>()) {
        let expected = !value.is_empty()
            && value.len() <= 128
            && !value.chars().any(|character| {
                matches!(u32::from(character), 0x00..=0x1f | 0x7f..=0x9f)
            });

        prop_assert_eq!(TenonDocumentId::try_from(value.as_str()).is_ok(), expected);
        prop_assert_eq!(PluginInstanceId::try_from(value.as_str()).is_ok(), expected);
        prop_assert_eq!(FlowId::try_from(value.clone()).is_ok(), expected);
    }

    #[test]
    fn accepted_identifier_text_is_never_normalized(value in any::<String>()) {
        if let Ok(program_name) = ProgramName::try_from(value.as_str()) {
            prop_assert_eq!(program_name.as_str(), value.as_str());
        }
        if let Ok(exact_version) = ExactVersion::try_from(value.as_str()) {
            prop_assert_eq!(exact_version.as_str(), value.as_str());
        }
        if let Ok(sink_contract_id) = SinkContractId::try_from(value.as_str()) {
            prop_assert_eq!(sink_contract_id.to_string(), value.as_str());
        }
    }

    #[test]
    fn generated_program_versions_and_sink_contract_ids_round_trip(
        program_name in "[a-z0-9]{1,8}\\.[a-z0-9]{1,8}\\.[a-z0-9]{1,8}",
        major in any::<u16>(),
        minor in any::<u16>(),
        patch in any::<u16>(),
        prerelease in prop::option::of("[a-z][a-z0-9-]{0,7}"),
    ) {
        let exact_version = match prerelease {
            Some(prerelease) => format!("{major}.{minor}.{patch}-{prerelease}"),
            None => format!("{major}.{minor}.{patch}"),
        };
        let sink_contract_id = format!("{program_name}@{exact_version}");

        let Ok(parsed_program_name) = ProgramName::try_from(program_name.as_str()) else {
            return Err(TestCaseError::fail("generated program name was rejected"));
        };
        let Ok(parsed_exact_version) = ExactVersion::try_from(exact_version.as_str()) else {
            return Err(TestCaseError::fail("generated exact version was rejected"));
        };
        let Ok(parsed_sink_contract_id) = SinkContractId::try_from(sink_contract_id.as_str()) else {
            return Err(TestCaseError::fail("generated Sink Contract identity was rejected"));
        };

        prop_assert_eq!(parsed_program_name.to_string(), program_name);
        prop_assert_eq!(parsed_exact_version.to_string(), exact_version);
        prop_assert_eq!(parsed_sink_contract_id.to_string(), sink_contract_id);
    }
}

#[test]
fn program_identity_keys_distinguish_names_and_exact_versions() -> io::Result<()> {
    let identity = |name: &str, version: &str| -> io::Result<PluginProgramIdentity> {
        Ok(PluginProgramIdentity::from_parts(
            ProgramName::try_from(name).map_err(io::Error::other)?,
            ExactVersion::try_from(version).map_err(io::Error::other)?,
        ))
    };
    let first = identity("com.example.source", "1.0.0")?;
    let next_version = identity("com.example.source", "1.0.1")?;
    let other_program = identity("com.example.sink", "1.0.0")?;
    let programs = std::collections::HashMap::from([
        (first.clone(), 1),
        (next_version.clone(), 2),
        (other_program.clone(), 3),
    ]);
    assert_eq!(programs.len(), 3);
    assert_eq!(
        programs.get(&identity("com.example.source", "1.0.0")?),
        Some(&1)
    );
    assert_eq!(programs.get(&next_version), Some(&2));
    assert_eq!(programs.get(&other_program), Some(&3));
    let borrowed: std::collections::HashMap<_, _> = programs.iter().collect();
    assert_eq!(
        borrowed.get(&identity("com.example.source", "1.0.0")?),
        Some(&&1)
    );
    Ok(())
}
