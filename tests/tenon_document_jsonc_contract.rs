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

use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::io;
use tenon::runner_test_support::tenon_document::UnverifiedTenonDocument;

const TEST_VECTORS: &[u8] =
    include_bytes!("../contracts/tenon-document/test-fixtures/jsonc_parser_test_vectors.json");

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TestVectors {
    valid: Vec<ValidVector>,
    invalid: Vec<InvalidVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ValidVector {
    name: String,
    source: String,
    expected_json: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvalidVector {
    name: String,
    source: Option<String>,
    source_bytes: Option<Vec<u8>>,
    expected_error_code: String,
}

#[test]
fn shared_jsonc_vectors_match_the_syntax_boundary() -> io::Result<()> {
    let vectors: TestVectors = serde_json::from_slice(TEST_VECTORS).map_err(io::Error::other)?;

    for vector in vectors.valid {
        let parsed =
            UnverifiedTenonDocument::parse(vector.source.as_bytes()).map_err(io::Error::other)?;
        assert_eq!(
            parsed.as_json(),
            &vector.expected_json,
            "valid vector {} produced another JSON value",
            vector.name
        );
    }

    for vector in vectors.invalid {
        let source = match (vector.source, vector.source_bytes) {
            (Some(source), None) => source.into_bytes(),
            (None, Some(source)) => source,
            _ => {
                return Err(io::Error::other(
                    "invalid vector must define exactly one source representation",
                ));
            }
        };
        let Err(error) = UnverifiedTenonDocument::parse(&source) else {
            return Err(io::Error::other("invalid vector was accepted"));
        };
        assert_eq!(
            error.code(),
            vector.expected_error_code,
            "invalid vector {} produced another error",
            vector.name
        );
    }

    Ok(())
}

#[test]
fn arbitrary_precision_json_number_is_not_narrowed() -> io::Result<()> {
    let source = br#"{"value":123456789012345678901234567890.123456789012345678901234567890}"#;
    let parsed = UnverifiedTenonDocument::parse(source).map_err(io::Error::other)?;

    assert_eq!(
        parsed.as_json()["value"].to_string(),
        "123456789012345678901234567890.123456789012345678901234567890"
    );

    Ok(())
}

#[test]
fn parsed_tree_owns_data_after_the_input_is_dropped() -> io::Result<()> {
    let parsed = {
        let source = String::from(r#"{"value":"owned"}"#);
        UnverifiedTenonDocument::parse(source.as_bytes()).map_err(io::Error::other)?
    };

    assert_eq!(parsed.as_json()["value"], "owned");

    Ok(())
}

fn arbitrary_json() -> impl Strategy<Value = Value> {
    let string = prop::collection::vec(any::<char>(), 0..16)
        .prop_map(|characters| characters.into_iter().collect::<String>());
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(|value| Value::Number(value.into())),
        string.clone().prop_map(Value::String),
    ];

    leaf.prop_recursive(8, 256, 10, move |inner| {
        let array = prop::collection::vec(inner.clone(), 0..8).prop_map(Value::Array);
        let object = prop::collection::btree_map(string.clone(), inner, 0..8).prop_map(
            |entries: BTreeMap<String, Value>| {
                Value::Object(entries.into_iter().collect::<serde_json::Map<_, _>>())
            },
        );
        prop_oneof![array, object]
    })
}

proptest! {
    #[test]
    fn strict_json_serialized_by_serde_json_round_trips(value in arbitrary_json()) {
        let source = serde_json::to_vec(&value)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        let parsed = UnverifiedTenonDocument::parse(&source)
        .map_err(|error| TestCaseError::fail(error.to_string()))?;

        prop_assert_eq!(parsed.as_json(), &value);
    }
}
