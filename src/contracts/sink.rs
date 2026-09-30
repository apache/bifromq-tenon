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

//! Sink wire contracts and Egress framing shared by Lua and Queue delivery.
//!
//! Lua checks the complete encoded size before allocating or accepting output.
//! Lua encodes the complete record once; delivery moves it to all target Queues.
//! Encoding always writes field 1,
//! including an empty payload, because an empty Queue body is a wrap marker.

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct EncodedEgressRecord {
    bytes: Vec<u8>,
}

impl EncodedEgressRecord {
    /// Writes framing and payload into one fallibly reserved buffer.
    /// The encoder must append exactly `payload_len` bytes or return its error.
    pub(crate) fn try_encode<E: From<std::collections::TryReserveError>>(
        payload_len: usize,
        encode_payload: impl FnOnce(&mut Vec<u8>) -> Result<(), E>,
    ) -> Result<Self, E> {
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(encoded_len(payload_len))?;
        // Emit even an empty payload: a zero-length Queue record is a wrap marker.
        prost::encoding::encode_key(1, prost::encoding::WireType::LengthDelimited, &mut bytes);
        prost::encoding::encode_varint(payload_len as u64, &mut bytes);
        encode_payload(&mut bytes)?;
        Ok(Self { bytes })
    }

    pub(crate) fn len(&self) -> usize {
        self.bytes.len()
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    #[expect(
        clippy::expect_used,
        reason = "try_encode writes the framing before invoking the payload encoder"
    )]
    pub(crate) fn payload(&self) -> &[u8] {
        let mut payload = &self.bytes[prost::encoding::key_len(1)..];
        prost::encoding::decode_varint(&mut payload)
            .expect("Encoded Egress record has a length prefix");
        payload
    }
}

pub(crate) fn encoded_len(payload_len: usize) -> usize {
    prost::encoding::key_len(1)
        + prost::encoding::encoded_len_varint(payload_len as u64)
        + payload_len
}

// Production writes the record directly; contract tests use the generated decoder.
#[cfg(any(test, feature = "repository-test-support"))]
include!(concat!(env!("OUT_DIR"), "/tenon.sink.rs"));

#[cfg(test)]
mod tests {
    use super::{EgressRecord, EncodedEgressRecord, encoded_len};
    use prost::Message as _;
    use std::collections::TryReserveError;

    #[test]
    fn length_matches_encoding_across_varint_boundaries() -> Result<(), Box<dyn std::error::Error>>
    {
        for payload_len in [0, 1, 127, 128, 16383, 16384] {
            let record = EgressRecord {
                payload: vec![0; payload_len],
            };
            let encoded =
                EncodedEgressRecord::try_encode::<TryReserveError>(payload_len, |bytes| {
                    bytes.extend_from_slice(&record.payload);
                    Ok(())
                })?;
            assert_eq!(encoded.len(), encoded_len(payload_len));
            assert_eq!(encoded.payload(), record.payload);
            assert_eq!(EgressRecord::decode(encoded.as_bytes())?, record);
            if payload_len == 0 {
                assert_eq!(encoded.as_bytes(), [0x0a, 0x00]);
            }
        }
        Ok(())
    }
}
