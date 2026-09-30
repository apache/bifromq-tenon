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

use proptest::prelude::*;
use prost::Message;
use serde::Deserialize;
use tenon::runner_test_support::contracts::sink::EgressRecord;

const TEST_VECTORS: &[u8] =
    include_bytes!("../contracts/sink/test-fixtures/egress_record_test_vectors.json");

#[derive(Debug, Deserialize)]
struct EgressRecordTestVectors {
    valid: Vec<ValidVector>,
    malformed: Vec<MalformedVector>,
}

#[derive(Debug, Deserialize)]
struct ValidVector {
    name: String,
    payload: Vec<u8>,
    encoded: Vec<u8>,
}

#[derive(Debug, Deserialize)]
struct MalformedVector {
    name: String,
    encoded: Vec<u8>,
}

#[test]
fn shared_golden_vector_decodes_to_the_expected_record() -> io::Result<()> {
    let vectors: EgressRecordTestVectors =
        serde_json::from_slice(TEST_VECTORS).map_err(io::Error::other)?;

    for vector in vectors.valid {
        let record = EgressRecord::decode(vector.encoded.as_slice()).map_err(io::Error::other)?;
        assert_eq!(record.payload, vector.payload, "{}", vector.name);
    }

    Ok(())
}

#[test]
fn malformed_shared_vectors_are_rejected() -> io::Result<()> {
    let vectors: EgressRecordTestVectors =
        serde_json::from_slice(TEST_VECTORS).map_err(io::Error::other)?;

    for vector in vectors.malformed {
        assert!(
            EgressRecord::decode(vector.encoded.as_slice()).is_err(),
            "Malformed vector decoded successfully: {}",
            vector.name
        );
    }

    Ok(())
}

proptest! {
    #[test]
    fn generated_record_round_trips_arbitrary_payloads(
        payload in prop::collection::vec(any::<u8>(), 0..4096),
    ) {
        let record = EgressRecord { payload };

        let decoded = EgressRecord::decode(record.encode_to_vec().as_slice())?;

        prop_assert_eq!(decoded, record);
    }
}
