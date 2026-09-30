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

use std::error::Error as _;
use std::io;
use std::num::NonZeroUsize;
use std::time::Duration;
use tenon::{ScriptVmLimits, VerifiedTenonDocument};

use serde::Deserialize;
use serde_json::Value;
use tenon::runner_test_support::tenon_document::{
    TenonDocumentVerificationError, TenonDocumentVerifier, UnverifiedTenonDocument,
};

const TEST_MEMORY_LIMIT_BYTES: usize = 4 * 1024 * 1024;
const TEST_VECTORS: &[u8] =
    include_bytes!("../contracts/tenon-document/test-fixtures/v1.verification-test-vectors.json");

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct VerificationTestVectors {
    format_version: u32,
    valid: Vec<ValidVector>,
    invalid: Vec<InvalidVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ValidVector {
    name: String,
    document: Value,
    expected_id: String,
    expected_plugin_instance_ids: Vec<String>,
    expected_flow_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvalidVector {
    name: String,
    document: Value,
    expected_issues: Vec<ExpectedIssue>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExpectedIssue {
    code: String,
    instance_path: String,
}

#[test]
fn shared_verification_vectors_cover_document_local_static_checks() -> io::Result<()> {
    let vectors = vectors()?;
    assert_eq!(vectors.format_version, 1);
    let verifier = verifier()?;

    for vector in vectors.valid {
        let document = parse_value(&vector.document)?;
        let verified = verifier.verify(document).map_err(|error| {
            io::Error::other(format!("Valid vector {} failed: {error}", vector.name))
        })?;

        assert_eq!(
            verified.id().as_str(),
            vector.expected_id,
            "{}",
            vector.name
        );
        let strict: Value =
            serde_json::from_str(&verified.strict_json()).map_err(io::Error::other)?;
        assert_eq!(strict, vector.document, "{}", vector.name);
        let keys = |name: &str| -> io::Result<Vec<String>> {
            strict[name]
                .as_object()
                .map(|entries| entries.keys().cloned().collect())
                .ok_or_else(|| io::Error::other(format!("Missing {name}")))
        };
        assert_eq!(
            keys("pluginInstances")?,
            vector.expected_plugin_instance_ids
        );
        assert_eq!(keys("flows")?, vector.expected_flow_ids);
    }

    for vector in vectors.invalid {
        let document = parse_value(&vector.document)?;
        let Err(error) = verifier.verify(document) else {
            return Err(io::Error::other(format!(
                "Invalid vector {} was accepted",
                vector.name
            )));
        };
        let actual = error
            .issues()
            .iter()
            .map(|issue| ExpectedIssue {
                code: issue.code().to_owned(),
                instance_path: issue.instance_path().to_owned(),
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, vector.expected_issues, "{}", vector.name);
        assert_eq!(error.code(), "tenon_document.verification_failed");
        assert_eq!(error.to_string(), "Tenon Document verification failed");
        assert!(error.source().is_none());
    }

    Ok(())
}

#[test]
fn public_verifier_compiles_without_executing_top_level_or_requiring_main() -> io::Result<()> {
    let vectors = vectors()?;
    let mut document = vectors
        .valid
        .first()
        .map(|vector| vector.document.clone())
        .ok_or_else(|| io::Error::other("Valid verification vector is missing"))?;
    document["flows"]["main"]["process"]["script"] = Value::String(String::from(
        "registry:getBuilder(\"com.example.iotdb@1.0.0\")\nfunction main(event) end",
    ));
    let verifier = verifier()?;

    verifier
        .verify(parse_value(&document)?)
        .map_err(io::Error::other)?;

    let source_marker = "private-startup-marker";
    document["flows"]["main"]["process"]["script"] = Value::String(format!(
        "error(\"{source_marker}\")\nfunction main(event) end"
    ));
    verifier
        .verify(parse_value(&document)?)
        .map_err(io::Error::other)?;

    document["flows"]["main"]["process"]["script"] =
        Value::String(String::from("local initialized = true"));
    verifier
        .verify(parse_value(&document)?)
        .map_err(io::Error::other)?;
    Ok(())
}

#[test]
fn verified_document_preserves_authored_default_presence() -> io::Result<()> {
    let vectors = vectors()?;
    let omitted = vectors
        .valid
        .first()
        .map(|vector| vector.document.clone())
        .ok_or_else(|| io::Error::other("Valid verification vector is missing"))?;
    let mut explicit = omitted.clone();
    explicit["flows"]["main"]["delivery"] = serde_json::json!("at-least-once");

    let verifier = verifier()?;
    let omitted = verifier
        .verify(parse_value(&omitted)?)
        .map_err(io::Error::other)?;
    let explicit = verifier
        .verify(parse_value(&explicit)?)
        .map_err(io::Error::other)?;

    let explicit: Value = serde_json::from_str(&explicit.strict_json())?;
    let omitted: Value = serde_json::from_str(&omitted.strict_json())?;
    assert_eq!(explicit["flows"]["main"]["delivery"], "at-least-once");
    assert!(omitted["flows"]["main"].get("delivery").is_none());
    Ok(())
}

#[test]
fn public_verification_types_can_cross_runner_worker_boundaries() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<TenonDocumentVerifier>();
    assert_send_sync::<VerifiedTenonDocument>();
    assert_send_sync::<TenonDocumentVerificationError>();
}

#[test]
fn zero_cpu_limit_is_rejected_before_verifier_construction() -> io::Result<()> {
    let memory_bytes = memory_limit()?;
    let error = ScriptVmLimits::try_new(memory_bytes, Duration::ZERO)
        .err()
        .ok_or_else(|| io::Error::other("Zero CPU limit was accepted"))?;
    assert_eq!(error.code(), "script_vm.cpu_time_zero");
    Ok(())
}

fn vectors() -> io::Result<VerificationTestVectors> {
    serde_json::from_slice(TEST_VECTORS).map_err(io::Error::other)
}

fn verifier() -> io::Result<TenonDocumentVerifier> {
    let limits = ScriptVmLimits::try_new(memory_limit()?, Duration::from_secs(1))
        .map_err(io::Error::other)?;
    TenonDocumentVerifier::try_new(limits).map_err(io::Error::other)
}

fn memory_limit() -> io::Result<NonZeroUsize> {
    NonZeroUsize::new(TEST_MEMORY_LIMIT_BYTES)
        .ok_or_else(|| io::Error::other("Test memory limit must be non-zero"))
}

fn parse_value(value: &Value) -> io::Result<UnverifiedTenonDocument> {
    let source = serde_json::to_vec(value).map_err(io::Error::other)?;
    UnverifiedTenonDocument::parse(&source).map_err(io::Error::other)
}
