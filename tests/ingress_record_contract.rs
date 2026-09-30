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

use std::io;

use prost::Message;
use serde::Deserialize;
use tenon::runner_test_support::contracts::source::{
    IngressCompletion, IngressCompletionStatus, IngressRecord,
};
use tenon::runner_test_support::ingress_queue::COMPLETION_MAX_PAYLOAD_SIZE;

const TEST_VECTORS: &[u8] =
    include_bytes!("../contracts/source/test-fixtures/ingress_record_test_vectors.json");

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IngressRecordVectors {
    format_version: u32,
    valid: Vec<IngressRecordVector>,
    completion_valid: Vec<IngressCompletionVector>,
    malformed: Vec<MalformedVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IngressRecordVector {
    name: String,
    record_id: u64,
    payload: Vec<u8>,
    encoded: Vec<u8>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IngressCompletionVector {
    name: String,
    record_id: u64,
    status: CompletionStatusVector,
    encoded: Vec<u8>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum CompletionStatusVector {
    Ok,
    Retry,
    Backpressure,
    Error,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MalformedVector {
    name: String,
    encoded: Vec<u8>,
}

#[test]
fn shared_vectors_fix_ingress_record_and_completion_bytes() -> io::Result<()> {
    let vectors = vectors()?;
    assert_eq!(vectors.format_version, 1);

    for vector in vectors.valid {
        let message = IngressRecord {
            record_id: vector.record_id,
            payload: vector.payload.clone().into(),
        };
        assert_eq!(message.encode_to_vec(), vector.encoded, "{}", vector.name);
        assert_eq!(
            IngressRecord::decode(vector.encoded.as_slice()).map_err(io::Error::other)?,
            message,
            "{}",
            vector.name
        );
    }

    for vector in vectors.completion_valid {
        let message = IngressCompletion {
            record_id: vector.record_id,
            status: completion_status(vector.status) as i32,
        };
        assert_eq!(message.encode_to_vec(), vector.encoded, "{}", vector.name);
        assert_eq!(
            IngressCompletion::decode(vector.encoded.as_slice()).map_err(io::Error::other)?,
            message,
            "{}",
            vector.name
        );
    }
    Ok(())
}

#[test]
fn maximum_completion_size_and_malformed_input_are_fixed() -> io::Result<()> {
    let vectors = vectors()?;
    let maximum = vectors
        .completion_valid
        .iter()
        .map(|vector| vector.encoded.len())
        .max()
        .ok_or_else(|| io::Error::other("completion vectors must not be empty"))?;
    assert_eq!(maximum, COMPLETION_MAX_PAYLOAD_SIZE);

    for vector in vectors.malformed {
        assert!(
            IngressRecord::decode(vector.encoded.as_slice()).is_err(),
            "{}",
            vector.name
        );
    }
    Ok(())
}

fn vectors() -> io::Result<IngressRecordVectors> {
    serde_json::from_slice(TEST_VECTORS).map_err(io::Error::other)
}

const fn completion_status(status: CompletionStatusVector) -> IngressCompletionStatus {
    match status {
        CompletionStatusVector::Ok => IngressCompletionStatus::Ok,
        CompletionStatusVector::Retry => IngressCompletionStatus::Retry,
        CompletionStatusVector::Backpressure => IngressCompletionStatus::Backpressure,
        CompletionStatusVector::Error => IngressCompletionStatus::Error,
    }
}
