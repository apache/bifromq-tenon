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

use super::{TenonDocumentVerifier, VerifiedTenonDocument};
use crate::config::ScriptVmLimits;
use crate::identifiers::{ExactVersion, FlowId, PluginInstanceId, ProgramName, TenonDocumentId};
use crate::tenon_document::{
    SourceDelivery, TenonDocumentVerificationError, UnverifiedTenonDocument,
};
use proptest::prelude::*;
use proptest::test_runner::{Config, RngSeed, TestRunner};
use serde::Deserialize;
use serde_json::{Value, json};
use std::io;
use std::num::NonZeroUsize;
use std::time::Duration;

mod record_limits;

#[test]
fn resource_limits_preserve_authored_numbers_and_omission() -> io::Result<()> {
    let verifier = verifier()?;
    for limits in [
        "{}",
        "{\"cpu\":1.50,\"memoryBytes\":536870912}",
        "{\"cpu\":1e0,\"memoryBytes\":4.096e3}",
        "{\"memoryBytes\":18446744073709551615}",
    ] {
        let mut document = self_route();
        document["resourceLimits"] = serde_json::from_str(limits)?;
        let verified = verify_value(&verifier, &document)?;
        assert_eq!(serde_json::to_value(&verified)?, document);
        let reconstructed = VerifiedTenonDocument::from_runner_json(&verified.strict_json());
        assert_eq!(serde_json::to_value(reconstructed)?, document);
    }
    let document = self_route();
    assert_eq!(
        serde_json::to_value(verify_value(&verifier, &document)?)?,
        document
    );
    Ok(())
}

#[test]
fn resource_limit_verification_establishes_exact_integer_projections() -> io::Result<()> {
    let verifier = verifier()?;
    for (limits, cpu, memory) in [
        (
            "{\"cpu\":0.29,\"memoryBytes\":9007199254740993}",
            Some(29),
            Some(9007199254740993),
        ),
        (
            "{\"cpu\":1.500e0,\"memoryBytes\":4.09600e3}",
            Some(150),
            Some(4096),
        ),
        (
            "{\"cpu\":184467440737095.51,\"memoryBytes\":18446744073709551615}",
            Some(18446744073709551),
            Some(u64::MAX),
        ),
    ] {
        let mut document = self_route();
        document["resourceLimits"] = serde_json::from_str(limits)?;
        let verified = verify_value(&verifier, &document)?;
        let limits = verified
            .resource_limits()
            .ok_or_else(|| io::Error::other("Resource limits were lost"))?;
        assert_eq!(limits.cpu_hundredths(), cpu);
        assert_eq!(limits.memory_bytes(), memory);
    }
    for limits in [
        "{\"cpu\":0.009999999999999999999}",
        "{\"cpu\":184467440737095.52}",
        "{\"memoryBytes\":9007199254740993.1}",
        "{\"memoryBytes\":18446744073709551616}",
        "{\"cpu\":1e999999}",
        "{\"memoryBytes\":1.00e-9223372036854775807}",
    ] {
        let mut document = self_route();
        document["resourceLimits"] = serde_json::from_str(limits)?;
        let error = verifier
            .verify(parse_value(&document)?)
            .err()
            .ok_or_else(|| io::Error::other(format!("Accepted {limits}")))?;
        assert_eq!(
            issues_json(&error)[0]["code"],
            "tenon_document.schema_invalid"
        );
    }
    Ok(())
}

#[test]
fn resource_limit_verifier_preserves_exact_values_across_numeric_notations() -> io::Result<()> {
    let verifier = verifier()?;
    let mut runner = TestRunner::new(Config {
        failure_persistence: None,
        rng_seed: RngSeed::Fixed(1),
        ..Config::default()
    });
    runner
        .run(
            &(1_u64..=100_000, 1_u64..=u64::MAX, 1_u64..=1_000_000_000),
            |(cpu_hundredths, memory_bytes, exponent_memory_bytes)| {
                for (cpu, memory, expected_memory) in [
                    (
                        format!("{}.{:02}", cpu_hundredths / 100, cpu_hundredths % 100),
                        memory_bytes.to_string(),
                        memory_bytes,
                    ),
                    (
                        format!("{cpu_hundredths}e-2"),
                        format!("{exponent_memory_bytes}000e-3"),
                        exponent_memory_bytes,
                    ),
                ] {
                    let mut document = self_route();
                    document["resourceLimits"] = serde_json::from_str(&format!(
                        "{{\"cpu\":{cpu},\"memoryBytes\":{memory}}}"
                    ))?;
                    let verified = verify_value(&verifier, &document)?;
                    let limits = verified
                        .resource_limits()
                        .ok_or_else(|| io::Error::other("Resource limits were lost"))?;
                    prop_assert_eq!(limits.cpu_hundredths(), Some(cpu_hundredths));
                    prop_assert_eq!(limits.memory_bytes(), Some(expected_memory));
                    prop_assert_eq!(serde_json::to_value(verified)?, document);
                }
                for limits in [
                    format!("{{\"memoryBytes\":{memory_bytes}.1}}"),
                    format!(
                        "{{\"cpu\":{}.{:02}1}}",
                        cpu_hundredths / 100,
                        cpu_hundredths % 100
                    ),
                ] {
                    let mut document = self_route();
                    document["resourceLimits"] = serde_json::from_str(&limits)?;
                    let error = verifier
                        .verify(parse_value(&document)?)
                        .err()
                        .ok_or_else(|| io::Error::other(format!("Accepted {limits}")))?;
                    prop_assert_eq!(
                        &issues_json(&error)[0]["code"],
                        "tenon_document.schema_invalid"
                    );
                }
                Ok(())
            },
        )
        .map_err(io::Error::other)
}

#[test]
#[ignore = "jsonschema 0.53.0 rounds the decimal u64 upper bound before comparison"]
fn resource_limit_extreme_decimal_upper_bound_is_an_equivalent_integer() -> io::Result<()> {
    let verifier = verifier()?;
    let mut document = self_route();
    document["resourceLimits"] = serde_json::from_str("{\"memoryBytes\":18446744073709551615.00}")?;
    let verified = verify_value(&verifier, &document)?;
    assert_eq!(serde_json::to_value(verified)?, document);
    Ok(())
}

#[test]
fn flat_flow_fields_preserve_authored_delivery_and_script() -> io::Result<()> {
    let verifier = verifier()?;
    let mut document = json!({
        "specVersion": "1", "id": "flat-flow",
        "pluginInstances": {
            "source": {"programName": "com.example.source", "exactVersion": "1.0.0", "config": {}},
            "sink": {"programName": "com.example.sink", "exactVersion": "1.0.0", "config": {}}
        },
        "flows": {"main": {
            "source": "source",
            "process": {"script": "function main(event) emit() end"},
            "sinks": ["sink"]
        }}
    });
    for delivery in [None, Some("at-most-once"), Some("at-least-once")] {
        if let Some(delivery) = delivery {
            document["flows"]["main"]["delivery"] = delivery.into();
        }
        let verified = verify_value(&verifier, &document)?;
        assert_eq!(
            serde_json::from_str::<Value>(&verified.strict_json())?,
            document
        );
    }
    document["flows"]["main"]["process"] = json!({"source": "function main(event) emit() end"});
    assert!(verifier.verify(parse_value(&document)?).is_err());
    Ok(())
}

#[test]
fn rejects_invalid_schema_vectors_at_the_original_locations() -> io::Result<()> {
    let vectors: SharedVectors = serde_json::from_slice(include_bytes!(
        "../../../contracts/tenon-document/test-fixtures/v1.test-vectors.json"
    ))?;
    let verifier = verifier()?;
    assert!(!vectors.invalid.is_empty());
    for vector in vectors.invalid {
        let errors = verifier
            .verify(parse_value(&vector["document"])?)
            .err()
            .ok_or_else(|| io::Error::other(format!("Accepted {}", vector["name"])))?;
        assert_eq!(
            issues_json(&errors),
            json!([{
                "code": "tenon_document.schema_invalid",
                "instancePath": vector["expectedInstancePointer"]
            }]),
            "{}",
            vector["name"]
        );
    }
    for vector in vectors.valid {
        verify_value(&verifier, &vector["document"])?;
    }
    Ok(())
}

#[test]
fn schema_issues_are_sorted_escaped_and_redacted() -> io::Result<()> {
    let mut document = self_route();
    document["pluginInstances"]["device"]["config"] = json!("private-config-marker");
    document["flows"]["broken/~"] = document["flows"]["main"].clone();
    document["flows"]["broken/~"]["parallelism"] = json!("private-number-marker");
    document["flows"]["broken/~"]["process"]["script"] = json!({"secret": "private-script-marker"});
    let errors = verifier()?
        .verify(parse_value(&document)?)
        .err()
        .ok_or_else(|| io::Error::other("Invalid Schema values were accepted"))?;
    assert_eq!(
        issues_json(&errors),
        json!([
            {"code": "tenon_document.schema_invalid", "instancePath": "/flows/broken~1~0/parallelism"},
            {"code": "tenon_document.schema_invalid", "instancePath": "/flows/broken~1~0/process/script"}
        ])
    );
    for exposed in [
        format!("{errors:?}"),
        errors.to_string(),
        issues_json(&errors).to_string(),
    ] {
        assert!(!exposed.contains("private-"));
    }
    Ok(())
}

#[test]
fn rejects_semantic_and_lua_vectors_without_inventing_dependent_errors() -> io::Result<()> {
    let verifier = verifier()?;
    for bytes in [
        include_bytes!(
            "../../../contracts/tenon-document/test-fixtures/v1.semantic-test-vectors.json"
        )
        .as_slice(),
        include_bytes!(
            "../../../contracts/tenon-document/test-fixtures/v1.verification-test-vectors.json"
        )
        .as_slice(),
    ] {
        let vectors: SharedVectors = serde_json::from_slice(bytes)?;
        assert!(!vectors.invalid.is_empty());
        for vector in vectors.invalid {
            let errors = verifier
                .verify(parse_value(&vector["document"])?)
                .err()
                .ok_or_else(|| io::Error::other(format!("Accepted {}", vector["name"])))?;
            assert_eq!(
                issues_json(&errors),
                vector["expectedIssues"],
                "{}",
                vector["name"]
            );
        }
        for vector in vectors.valid {
            let verified = verify_value(&verifier, &vector["document"])?;
            assert_eq!(
                serde_json::from_str::<Value>(&verified.strict_json())?,
                vector["document"]
            );
            if let Some(id) = vector["expectedId"].as_str() {
                assert_eq!(verified.id().as_str(), id);
                assert_eq!(
                    json!(
                        verified
                            .plugin_instances()
                            .keys()
                            .map(PluginInstanceId::as_str)
                            .collect::<Vec<_>>()
                    ),
                    vector["expectedPluginInstanceIds"]
                );
                assert_eq!(
                    json!(
                        verified
                            .flows()
                            .keys()
                            .map(FlowId::as_str)
                            .collect::<Vec<_>>()
                    ),
                    vector["expectedFlowIds"]
                );
            }
        }
    }
    Ok(())
}

#[test]
fn aggregates_independent_flow_failures_and_escapes_json_pointers() -> io::Result<()> {
    let mut document = self_route();
    document["pluginInstances"]["device"]["config"] = json!({"token": "private-config-marker"});
    document["pluginInstances"]["unused/~"] = document["pluginInstances"]["device"].clone();
    document["flows"]["broken/~"] = json!({
        "parallelism": 1, "source": "absent-source",
        "process": {"script": "local secret = 'private-lua-marker'; function main(event)"},
        "sinks": ["absent-sink"]
    });
    document["flows"]["second"] = document["flows"]["main"].clone();
    document["flows"]["second"]["process"]["script"] = json!("local = invalid");
    let errors = verifier()?
        .verify(parse_value(&document)?)
        .err()
        .ok_or_else(|| io::Error::other("Invalid relationships and Lua were accepted"))?;
    assert_eq!(
        issues_json(&errors),
        json!([
            {"code": "process.lua_syntax_invalid", "instancePath": "/flows/broken~1~0/process/script"},
            {"code": "flow.sink_reference_invalid", "instancePath": "/flows/broken~1~0/sinks/0"},
            {"code": "flow.source_reference_invalid", "instancePath": "/flows/broken~1~0/source"},
            {"code": "process.lua_syntax_invalid", "instancePath": "/flows/second/process/script"},
            {"code": "flow.source_reused", "instancePath": "/flows/second/source"},
            {"code": "plugin_instance.unused", "instancePath": "/pluginInstances/unused~1~0"}
        ])
    );
    for exposed in [
        format!("{errors:?}"),
        errors.to_string(),
        issues_json(&errors).to_string(),
    ] {
        assert!(!exposed.contains("private-config-marker"));
        assert!(!exposed.contains("private-lua-marker"));
    }
    Ok(())
}

#[test]
fn rejects_source_queue_configuration() -> io::Result<()> {
    let verifier = verifier()?;
    for field in ["queueCount", "parallelism"] {
        let mut document = self_route();
        document["flows"]["main"]["source"] = json!({"plugin": "device"});
        let source = document["flows"]["main"]["source"]
            .as_object_mut()
            .ok_or_else(|| io::Error::other("Source binding is missing"))?;
        source.insert(field.into(), json!(1));
        let errors = verifier
            .verify(parse_value(&document)?)
            .err()
            .ok_or_else(|| io::Error::other(format!("Accepted Source field {field}")))?;
        assert_eq!(
            issues_json(&errors),
            json!([{
                "code": "tenon_document.schema_invalid",
                "instancePath": "/flows/main/source"
            }]),
            "Source field {field}"
        );
    }
    Ok(())
}

#[test]
fn accepts_flow_parallelism_without_rewriting_authored_values() -> io::Result<()> {
    let verifier = verifier()?;
    for number in [
        None,
        Some("0.01"),
        Some("0.5"),
        Some("1"),
        Some("1.0"),
        Some("1e0"),
        Some("1.5"),
        Some("10"),
        Some("10.0"),
        Some("1e1"),
    ] {
        let mut document = self_route();
        document["flows"]["main"]
            .as_object_mut()
            .ok_or_else(|| io::Error::other("Flow is missing"))?
            .remove("parallelism");
        if let Some(number) = number {
            document["flows"]["main"]["parallelism"] = serde_json::from_str(number)?;
        }
        let verified = verify_value(&verifier, &document)?;
        let serialized = verified.strict_json();
        assert_eq!(serde_json::from_str::<Value>(&serialized)?, document);
        let received = VerifiedTenonDocument::from_runner_json(&serialized);
        assert_eq!(
            serde_json::from_str::<Value>(&received.strict_json())?,
            document
        );
        assert_eq!(verified.strict_json(), received.strict_json());
    }
    Ok(())
}

#[test]
fn derives_channels_independently_of_document_cpu_from_ratio_vectors() -> io::Result<()> {
    let verifier = verifier()?;
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../contracts/tenon-document/test-fixtures/v1.parallelism-test-vectors.json"
    ))?;
    let cases = vectors["cases"]
        .as_array()
        .ok_or_else(|| io::Error::other("Channel-count vectors are missing"))?;
    assert!(!cases.is_empty());
    for case in cases {
        let mut document = self_route();
        document["flows"]["main"]
            .as_object_mut()
            .ok_or_else(|| io::Error::other("Flow is missing"))?
            .remove("parallelism");
        if let Some(ratio) = case["parallelism"].as_str() {
            document["flows"]["main"]["parallelism"] = serde_json::from_str(ratio)?;
        }
        // A memory-only limit must leave CPU-derived parallelism unchanged.
        document["resourceLimits"] = serde_json::json!({"memoryBytes": 1048576});
        if let Some(cpu) = case["cpu"].as_str() {
            document["resourceLimits"]["cpu"] = serde_json::from_str(cpu)?;
        }
        let verified = verify_value(&verifier, &document)?;
        let flow_id = verified
            .flows()
            .keys()
            .next()
            .ok_or_else(|| io::Error::other("Flow is missing"))?;
        let cpus: usize = serde_json::from_value(case["availableCpuCount"].clone())?;
        let cpus = std::num::NonZeroUsize::new(cpus)
            .ok_or_else(|| io::Error::other("CPU count must be positive"))?;
        assert_eq!(
            verified.channel_count(flow_id, cpus).get(),
            case["channelCount"],
            "{}",
            case["name"]
        );
        let received = VerifiedTenonDocument::from_runner_json(&verified.strict_json());
        assert_eq!(
            received.channel_count(flow_id, cpus),
            verified.channel_count(flow_id, cpus)
        );
        assert_eq!(
            serde_json::from_str::<Value>(&received.strict_json())?,
            document
        );
    }
    Ok(())
}

#[test]
fn channel_counts_match_integer_arithmetic_across_decimal_notations() -> io::Result<()> {
    let verifier = verifier()?;
    let mut runner = TestRunner::new(Config {
        cases: 128,
        failure_persistence: None,
        rng_seed: RngSeed::Fixed(246),
        ..Config::default()
    });
    runner
        .run(
            &(1_u64..=1_000_000, 1_u64..=3200, 1_usize..=32),
            |(ratio, cpu, cpus)| {
                let expected = (ratio * cpus as u64).div_ceil(100_000);
                for number in [
                    format!("{}.{:05}", ratio / 100_000, ratio % 100_000),
                    format!("{ratio}e-5"),
                    format!("0.{ratio:08}e3"),
                ] {
                    let mut document = self_route();
                    document["flows"]["main"]["parallelism"] = serde_json::from_str(&number)?;
                    document["resourceLimits"] =
                        serde_json::from_str(&format!("{{\"cpu\":{cpu}e-2}}"))?;
                    let verified = verify_value(&verifier, &document)?;
                    let flow_id = FlowId::try_from(String::from("main"))?;
                    let cpus = NonZeroUsize::new(cpus)
                        .ok_or_else(|| io::Error::other("CPU count is positive"))?;
                    prop_assert_eq!(
                        u64::from(verified.channel_count(&flow_id, cpus).get()),
                        expected
                    );
                }
                Ok(())
            },
        )
        .map_err(io::Error::other)
}

#[test]
fn rejects_invalid_flow_parallelism_at_its_original_location() -> io::Result<()> {
    let verifier = verifier()?;
    for number in [
        "0",
        "-1",
        "10.0001",
        "1.00e-9223372036854775807",
        "true",
        "\"1\"",
        "null",
    ] {
        assert_invalid_parallelism(&verifier, number)?;
    }
    Ok(())
}

#[test]
#[ignore = "jsonschema rounds a ratio just above ten down to its maximum"]
fn rejects_parallelism_above_ten_without_rounding_down() -> io::Result<()> {
    assert_invalid_parallelism(&verifier()?, "10.0000000000000000000000000000000000001")
}

#[test]
#[ignore = "upstream jsonschema panics on 1e400"]
fn rejects_huge_parallelism_without_panicking() -> io::Result<()> {
    assert_invalid_parallelism(&verifier()?, "1e400")
}

#[test]
fn keeps_authored_fields_and_config_while_projecting_runtime_defaults() -> io::Result<()> {
    let verifier = verifier()?;
    for delivery in [
        None,
        Some(json!("at-least-once")),
        Some(json!("at-most-once")),
    ] {
        for config in [
            Value::Null,
            json!(true),
            json!([1, "data"]),
            json!({"token": "private-config"}),
            serde_json::from_str("123456789012345678901234567890.125")?,
        ] {
            let mut document = self_route();
            document["pluginInstances"]["device"]["config"] = config;
            if let Some(delivery) = &delivery {
                document["flows"]["main"]["delivery"] = delivery.clone();
            }
            let verified = verify_value(&verifier, &document)?;
            assert_eq!(
                serde_json::from_str::<Value>(&verified.strict_json())?,
                document
            );
            let received = VerifiedTenonDocument::from_runner_json(&verified.strict_json());
            assert_eq!(serde_json::to_value(&received)?, document);
            let instance = verified
                .plugin_instances()
                .values()
                .next()
                .ok_or_else(|| io::Error::other("Instance is missing"))?;
            assert_eq!(instance.program_name().as_str(), "com.example.device");
            assert_eq!(instance.exact_version().as_str(), "1.0.0");
            assert_eq!(
                instance.config(),
                &document["pluginInstances"]["device"]["config"]
            );
            let flow = verified
                .flows()
                .values()
                .next()
                .ok_or_else(|| io::Error::other("Flow is missing"))?;
            assert_eq!(flow.source().as_str(), "device");
            assert_eq!(
                flow.sinks()
                    .iter()
                    .map(PluginInstanceId::as_str)
                    .collect::<Vec<_>>(),
                ["device"]
            );
            let expected = if delivery
                .as_ref()
                .is_some_and(|value| value == "at-most-once")
            {
                SourceDelivery::AtMostOnce
            } else {
                SourceDelivery::AtLeastOnce
            };
            assert_eq!(flow.delivery(), expected);
        }
    }
    Ok(())
}

#[test]
fn preserves_config_objects_with_serde_numeric_marker_keys() -> io::Result<()> {
    let verifier = verifier()?;
    for config in [
        json!({"$serde_json::private::Number": "1"}),
        json!({"$serde_json::private::Number": "not-a-number"}),
        json!({"nested": [{"$serde_json::private::Number": "1"}]}),
    ] {
        let mut document = self_route();
        document["pluginInstances"]["device"]["config"] = config;
        let verified = verify_value(&verifier, &document)?;
        let instance = verified
            .plugin_instances()
            .values()
            .next()
            .ok_or_else(|| io::Error::other("Instance is missing"))?;
        assert_eq!(
            instance.config(),
            &document["pluginInstances"]["device"]["config"]
        );
        let reparsed = UnverifiedTenonDocument::parse(verified.strict_json().as_bytes())
            .map_err(io::Error::other)?;
        assert_eq!(reparsed.as_json(), &document);
        let received = VerifiedTenonDocument::from_runner_json(&verified.strict_json());
        assert_eq!(serde_json::to_value(&received)?, document);
    }
    Ok(())
}

#[test]
fn runner_reconstruction_preserves_verified_documents() -> io::Result<()> {
    let verifier = verifier()?;
    let mut runner = TestRunner::new(Config {
        failure_persistence: None,
        rng_seed: RngSeed::Fixed(1),
        ..Config::default()
    });
    runner
        .run(
            &(any::<String>(), any::<i64>(), 0.000001_f64..=1.0_f64),
            |(text, number, parallelism)| {
                let mut document = self_route();
                document["pluginInstances"]["device"]["config"] = json!({
                    "text": text,
                    "number": number,
                    "$serde_json::private::Number": "not-a-number"
                });
                document["flows"]["main"]["parallelism"] = json!(parallelism);
                let verified = verify_value(&verifier, &document)?;
                let received = VerifiedTenonDocument::from_runner_json(&verified.strict_json());
                prop_assert_eq!(serde_json::to_value(&received)?, document);
                Ok(())
            },
        )
        .map_err(io::Error::other)
}

#[test]
fn compiles_each_flow_without_top_level_execution_or_program_lookup() -> io::Result<()> {
    let verifier = verifier()?;
    let mut document = self_route();
    for source in [
        "error(\"private-startup-marker\")",
        "registry:getBuilder(\"com.example.not-installed@1.0.0\")",
        "while true do end",
        "local initialized = true",
    ] {
        document["flows"]["main"]["process"]["script"] = json!(source);
        let verified = verify_value(&verifier, &document)?;
        let flow = verified
            .flows()
            .values()
            .next()
            .ok_or_else(|| io::Error::other("Flow is missing"))?;
        assert_eq!(flow.lua_source(), source);
    }
    Ok(())
}

#[test]
fn public_verifier_rejects_the_old_document_shape() -> io::Result<()> {
    let old_document = json!({
        "specVersion": "1", "id": "old-document",
        "source": {"programName": "com.example.device", "exactVersion": "1.0.0", "config": {}, "queueCount": 1},
        "process": {"script": "function main(event) end"}, "sinks": []
    });
    let verifier = super::TenonDocumentVerifier::try_new(limits()?).map_err(io::Error::other)?;
    assert!(verifier.verify(parse_value(&old_document)?).is_err());
    verifier
        .verify(parse_value(&self_route())?)
        .map_err(io::Error::other)?;
    let root_error = verifier
        .verify(parse_value(&json!(true))?)
        .err()
        .ok_or_else(|| io::Error::other("Non-object Document was accepted"))?;
    assert_eq!(
        issues_json(&root_error),
        json!([{"code": "tenon_document.schema_invalid", "instancePath": ""}])
    );
    Ok(())
}

#[test]
fn domain_projection_moves_config_and_lua_and_redacts_debug() -> io::Result<()> {
    let mut document = self_route();
    document["pluginInstances"]["device"]["config"] = json!("private-config-marker");
    document["flows"]["main"]["process"]["script"] = json!("local marker = 'private-lua-marker'");
    let parsed = parse_value(&document)?;
    let config_pointer = parsed.as_json()["pluginInstances"]["device"]["config"]
        .as_str()
        .ok_or_else(|| io::Error::other("Config marker is missing"))?
        .as_ptr();
    let lua_pointer = parsed.as_json()["flows"]["main"]["process"]["script"]
        .as_str()
        .ok_or_else(|| io::Error::other("Lua marker is missing"))?
        .as_ptr();
    let verified = verifier()?.verify(parsed).map_err(verification_failed)?;
    let instance = verified
        .plugin_instances()
        .values()
        .next()
        .ok_or_else(|| io::Error::other("Instance is missing"))?;
    let flow = verified
        .flows()
        .values()
        .next()
        .ok_or_else(|| io::Error::other("Flow is missing"))?;
    assert_eq!(
        instance
            .config()
            .as_str()
            .ok_or_else(|| io::Error::other("Config marker is missing"))?
            .as_ptr(),
        config_pointer
    );
    assert_eq!(flow.lua_source().as_ptr(), lua_pointer);
    let debug = format!("{verified:?} {instance:?} {flow:?}");
    assert!(!debug.contains("private-config-marker"));
    assert!(!debug.contains("private-lua-marker"));
    Ok(())
}

#[test]
fn target_identifier_vectors_preserve_domain_types_and_exact_text() -> io::Result<()> {
    let vectors: SharedVectors = serde_json::from_slice(include_bytes!(
        "../../../contracts/core/test-fixtures/domain-identifiers.test-vectors.json"
    ))?;
    let parse = |vector: &Value| -> Result<String, crate::identifiers::IdentifierParseError> {
        let text = vector["value"].as_str().unwrap_or_default().to_owned();
        match vector["kind"].as_str() {
            Some("tenonDocumentId") => {
                TenonDocumentId::try_from(text).map(|id| id.as_str().to_owned())
            }
            Some("pluginInstanceId") => {
                PluginInstanceId::try_from(text).map(|id| id.as_str().to_owned())
            }
            Some("flowId") => FlowId::try_from(text).map(|id| id.as_str().to_owned()),
            Some("programName") => ProgramName::try_from(text).map(|id| id.as_str().to_owned()),
            Some("exactVersion") => ExactVersion::try_from(text).map(|id| id.as_str().to_owned()),
            _ => unreachable!("Target vectors must use a known identifier kind"),
        }
    };
    for vector in vectors.valid {
        assert_eq!(
            json!(parse(&vector).map_err(io::Error::other)?),
            vector["expected"],
            "{}",
            vector["name"]
        );
    }
    for vector in vectors.invalid {
        let error = parse(&vector)
            .err()
            .ok_or_else(|| io::Error::other("Invalid identifier was accepted"))?;
        assert_eq!(
            json!(error.code()),
            vector["expectedErrorCode"],
            "{}",
            vector["name"]
        );
    }
    let mut runner = TestRunner::new(Config {
        failure_persistence: None,
        rng_seed: RngSeed::Fixed(1),
        ..Config::default()
    });
    runner
        .run(&any::<String>(), |value| {
            let expected =
                TenonDocumentId::try_from(value.clone()).map(|id| id.as_str().to_owned());
            prop_assert_eq!(
                PluginInstanceId::try_from(value.clone()).map(|id| id.as_str().to_owned()),
                expected.clone()
            );
            prop_assert_eq!(
                FlowId::try_from(value).map(|id| id.as_str().to_owned()),
                expected
            );
            Ok(())
        })
        .map_err(io::Error::other)
}

#[derive(Deserialize)]
struct SharedVectors {
    valid: Vec<Value>,
    invalid: Vec<Value>,
}

fn limits() -> io::Result<ScriptVmLimits> {
    let memory = NonZeroUsize::new(4 * 1024 * 1024)
        .ok_or_else(|| io::Error::other("Memory limit must be positive"))?;
    ScriptVmLimits::try_new(memory, Duration::from_secs(1)).map_err(io::Error::other)
}

fn verifier() -> io::Result<TenonDocumentVerifier> {
    TenonDocumentVerifier::try_new(limits()?).map_err(io::Error::other)
}

fn parse_value(value: &Value) -> io::Result<UnverifiedTenonDocument> {
    UnverifiedTenonDocument::parse(&serde_json::to_vec(value)?).map_err(io::Error::other)
}

fn verify_value(
    verifier: &TenonDocumentVerifier,
    value: &Value,
) -> io::Result<super::VerifiedTenonDocument> {
    verifier
        .verify(parse_value(value)?)
        .map_err(verification_failed)
}

fn verification_failed(error: TenonDocumentVerificationError) -> io::Error {
    io::Error::other(error)
}

fn issues_json(error: &TenonDocumentVerificationError) -> Value {
    Value::Array(
        error
            .issues()
            .iter()
            .map(|issue| json!({"code": issue.code(), "instancePath": issue.instance_path()}))
            .collect(),
    )
}

fn assert_invalid_parallelism(verifier: &TenonDocumentVerifier, number: &str) -> io::Result<()> {
    let mut document = self_route();
    document["flows"]["main"]["parallelism"] = serde_json::from_str(number)?;
    let errors = verifier
        .verify(parse_value(&document)?)
        .err()
        .ok_or_else(|| io::Error::other(format!("Accepted parallelism {number}")))?;
    assert_eq!(
        issues_json(&errors),
        json!([{
            "code": "tenon_document.schema_invalid",
            "instancePath": "/flows/main/parallelism"
        }]),
        "parallelism {number}"
    );
    Ok(())
}

fn self_route() -> Value {
    json!({
        "specVersion": "1", "id": "self-route",
        "pluginInstances": {"device": {"programName": "com.example.device", "exactVersion": "1.0.0", "config": {}}},
        "flows": {"main": {"parallelism": 1, "source": "device", "process": {"script": "function main(event) emit() end"}, "sinks": ["device"]}}
    })
}
