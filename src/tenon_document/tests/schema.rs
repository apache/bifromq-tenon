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

use crate::contracts::tenon_document::v1_schema_bytes;
use crate::tenon_document::UnverifiedTenonDocument;
use crate::tenon_document::static_validation::TenonDocumentV1SchemaValidator;
use serde::Deserialize;
use serde_json::Value;
use std::io;

const TEST_VECTORS: &[u8] =
    include_bytes!("../../../contracts/tenon-document/test-fixtures/v1.test-vectors.json");

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TenonDocumentTestVectors {
    valid: Vec<ValidVector>,
    invalid: Vec<InvalidVector>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidVector {
    name: String,
    document: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvalidVector {
    name: String,
    document: Value,
    expected_instance_pointer: String,
}

#[test]
fn embedded_schema_is_the_exact_repository_schema() {
    let repository_schema = include_bytes!("../../../contracts/tenon-document/v1.schema.json");

    assert_eq!(v1_schema_bytes(), repository_schema);
}

#[test]
fn embedded_schema_is_valid_draft_2020_12_and_has_only_internal_refs() -> io::Result<()> {
    let schema: Value = serde_json::from_slice(v1_schema_bytes()).map_err(io::Error::other)?;

    assert!(
        jsonschema::draft202012::meta::validate(&schema).is_ok(),
        "the embedded Tenon Document v1 Schema must pass Draft 2020-12 meta-schema validation"
    );
    assert!(
        refs_are_internal(&schema),
        "all Tenon Document v1 Schema $ref values must point into the same document"
    );

    Ok(())
}

#[test]
fn shared_vectors_have_one_stable_result_and_error_pointer() -> io::Result<()> {
    let vectors: TenonDocumentTestVectors =
        serde_json::from_slice(TEST_VECTORS).map_err(io::Error::other)?;
    let validator = TenonDocumentV1SchemaValidator::compile_schema(v1_schema_bytes())
        .map_err(io::Error::other)?;

    for vector in vectors.valid {
        let source = serde_json::to_vec(&vector.document).map_err(io::Error::other)?;
        let parsed = UnverifiedTenonDocument::parse(&source).map_err(io::Error::other)?;
        validator.validate(&parsed).map_err(|error| {
            io::Error::other(format!(
                "valid vector {} was rejected: {error:?}",
                vector.name
            ))
        })?;
    }

    for vector in vectors.invalid {
        let source = serde_json::to_vec(&vector.document).map_err(io::Error::other)?;
        let parsed = UnverifiedTenonDocument::parse(&source).map_err(io::Error::other)?;
        let Err(error) = validator.validate(&parsed) else {
            return Err(io::Error::other(format!(
                "invalid vector {} was accepted",
                vector.name
            )));
        };
        assert!(
            error
                .iter()
                .all(|issue| issue.code() == "tenon_document.schema_invalid")
        );
        let error_pointers = error
            .iter()
            .map(|issue| issue.instance_path())
            .collect::<Vec<_>>();
        assert_eq!(
            error_pointers,
            [vector.expected_instance_pointer],
            "invalid vector {} must produce exactly one stable error location",
            vector.name
        );
    }

    Ok(())
}

#[test]
fn parsed_non_object_fails_schema_at_the_document_root() -> io::Result<()> {
    let validator = TenonDocumentV1SchemaValidator::compile_schema(v1_schema_bytes())
        .map_err(io::Error::other)?;
    let parsed = UnverifiedTenonDocument::parse(b"true").map_err(io::Error::other)?;

    let Err(error) = validator.validate(&parsed) else {
        return Err(io::Error::other(
            "non-object Tenon Document passed Schema validation",
        ));
    };
    let pointers = error
        .iter()
        .map(|issue| issue.instance_path())
        .collect::<Vec<_>>();
    assert_eq!(pointers, [""]);

    Ok(())
}

#[test]
fn successful_schema_validation_does_not_replace_the_json_tree() -> io::Result<()> {
    let vectors: TenonDocumentTestVectors =
        serde_json::from_slice(TEST_VECTORS).map_err(io::Error::other)?;
    let Some(vector) = vectors.valid.first() else {
        return Err(io::Error::other("valid vector list is empty"));
    };
    let source = serde_json::to_vec(&vector.document).map_err(io::Error::other)?;
    let parsed = UnverifiedTenonDocument::parse(&source).map_err(io::Error::other)?;
    let Some(id_before) = parsed.as_json()["id"].as_str() else {
        return Err(io::Error::other("valid vector id is not a string"));
    };
    let id_allocation_before = id_before.as_ptr();

    let validator = TenonDocumentV1SchemaValidator::compile_schema(v1_schema_bytes())
        .map_err(io::Error::other)?;
    validator.validate(&parsed).map_err(|issues| {
        io::Error::other(format!("Valid Schema input was rejected: {issues:?}"))
    })?;
    let Some(id_after) = parsed.as_json()["id"].as_str() else {
        return Err(io::Error::other("validated id is not a string"));
    };

    assert_eq!(id_after.as_ptr(), id_allocation_before);

    Ok(())
}

fn refs_are_internal(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().all(refs_are_internal),
        Value::Object(entries) => {
            let local_ref = entries.get("$ref").is_none_or(|reference| {
                reference.as_str().is_some_and(|path| path.starts_with('#'))
            });

            local_ref && entries.values().all(refs_are_internal)
        }
        _ => true,
    }
}
