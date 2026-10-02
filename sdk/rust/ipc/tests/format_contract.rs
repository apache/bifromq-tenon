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

//! Shared Queue and Bell format vectors, corruption cases, and append properties.

use std::collections::VecDeque;
use std::fs;
use std::io;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::Path;

use proptest::prelude::*;
use serde::Deserialize;
use tempfile::tempdir;
use tenon_ipc::bell::{BellError, BellRegion, create_bell_region};
use tenon_ipc::queue::{
    AppendDecision, DataCapacity, FORMAT_VERSION, FRAME_ALIGNMENT, FRAME_HEADER_LEN, Frame,
    FrameHeader, HEADER_LEN, Header, LogicalPosition, QueueRuntimeError, arm_bell_slot,
    create_queue_file, decode_frame, encode_record_frame, encode_wrap_frame, inspect_frame_header,
    plan_append, record_frame_len,
};

const TEST_VECTORS: &[u8] = include_bytes!("../contracts/test-fixtures/queue_v1_test_vectors.json");

/// Doorbell slot indices this test's model publishes.
///
/// They are deliberately above one so a slot index can never be confused with
/// the armed/notified word the same slot holds.
const READER_BELL_SLOT: u32 = 3;
const WRITER_BELL_SLOT: u32 = 5;

/// The notified word of one Bell Region slot, fixed by the shared contract.
const DOORBELL_ARMED: u32 = 0;
const DOORBELL_NOTIFIED: u32 = 1;

/// Fixed byte offset of the reader loop's doorbell ordinal in a Queue header.
///
/// The writer reads it to ring the reader's loop, so it is the ordinal a writer
/// resolves when it commits a record.
const HEADER_READER_BELL_OFFSET: usize = 72;

/// Fixed byte offset of the writer loop's doorbell ordinal in a Queue header,
/// read by the reader to ring the writer's loop when it releases records.
const HEADER_WRITER_BELL_OFFSET: usize = 136;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct QueueTestVectors {
    format_version: u32,
    header_length: usize,
    frame_header_length: usize,
    frame_alignment: usize,
    unbound_bell_slot: u32,
    headers: HeaderVectors,
    frames: FrameVectors,
    append: Vec<AppendVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HeaderVectors {
    valid: Vec<ValidHeaderVector>,
    invalid_file_lengths: Vec<InvalidFileLengthVector>,
    invalid: Vec<InvalidHeaderVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ValidHeaderVector {
    name: String,
    file_length: String,
    max_payload_size: String,
    commit: String,
    reader_bell_slot: u32,
    release: String,
    writer_bell_slot: u32,
    encoded_hex: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BellRegionVectors {
    format_version: u32,
    header_length: usize,
    slot_length: usize,
    page_length: u64,
    armed_value: u32,
    notified_value: u32,
    valid: Vec<ValidBellRegionVector>,
    invalid: Vec<InvalidBellRegionVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ValidBellRegionVector {
    name: String,
    slot_count: u32,
    epoch: String,
    encoded_hex: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvalidBellRegionVector {
    name: String,
    encoded_hex: String,
    expected_error_code: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvalidFileLengthVector {
    name: String,
    file_length: String,
    expected_error_code: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvalidHeaderVector {
    name: String,
    file_length: String,
    encoded_hex: String,
    expected_error_code: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FrameVectors {
    max_payload_size: String,
    valid_records: Vec<ValidRecordFrameVector>,
    wrap: WrapVector,
    invalid: Vec<InvalidFrameVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ValidRecordFrameVector {
    name: String,
    record_hex: String,
    encoded_hex: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WrapVector {
    name: String,
    encoded_hex: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvalidFrameVector {
    name: String,
    encoded_hex: String,
    expected_error_code: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AppendVector {
    name: String,
    data_capacity: String,
    max_payload_size: String,
    commit: String,
    release: String,
    record_length: usize,
    result: AppendResultVector,
    write_offset: Option<String>,
    frame_offset: Option<String>,
    frame_length: Option<usize>,
    wrap_length: Option<String>,
    next_commit: Option<String>,
    expected_error_code: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum AppendResultVector {
    Ready,
    Full,
    Error,
}

#[test]
fn shared_header_vectors_fix_exact_bytes_and_errors() -> io::Result<()> {
    let vectors = vectors()?;
    assert_eq!(FORMAT_VERSION, vectors.format_version);
    assert_eq!(HEADER_LEN, vectors.header_length);
    assert_eq!(FRAME_HEADER_LEN, vectors.frame_header_length);
    assert_eq!(FRAME_ALIGNMENT, vectors.frame_alignment);

    for vector in vectors.headers.valid {
        let capacity = DataCapacity::from_file_len(parse_u64(&vector.file_length)?)
            .map_err(io::Error::other)?;
        let header = header_from_vector(&vector, capacity)?;
        let encoded = decode_hex(&vector.encoded_hex)?;
        assert_eq!(header.encode(), encoded.as_slice(), "{}", vector.name);
        assert_eq!(
            Header::decode(capacity, &encoded).map_err(io::Error::other)?,
            header,
            "{}",
            vector.name
        );
    }

    for vector in vectors.headers.invalid_file_lengths {
        assert_eq!(
            DataCapacity::from_file_len(parse_u64(&vector.file_length)?)
                .map_err(|error| error.code()),
            Err(vector.expected_error_code.as_str()),
            "{}",
            vector.name
        );
    }

    for vector in vectors.headers.invalid {
        let capacity = DataCapacity::from_file_len(parse_u64(&vector.file_length)?)
            .map_err(io::Error::other)?;
        let encoded = decode_hex(&vector.encoded_hex)?;
        let Err(error) = Header::decode(capacity, &encoded) else {
            return Err(io::Error::other(format!(
                "invalid shared header vector was accepted: {}",
                vector.name
            )));
        };
        assert_eq!(error.code(), vector.expected_error_code, "{}", vector.name);
    }
    Ok(())
}

#[test]
fn shared_bell_region_vectors_fix_exact_bytes_and_errors() -> io::Result<()> {
    let region: BellRegionVectors = serde_json::from_slice(include_bytes!(
        "../contracts/test-fixtures/bell_v1_test_vectors.json"
    ))?;
    assert_eq!(region.format_version, 1);
    assert_eq!(region.armed_value, DOORBELL_ARMED);
    assert_eq!(region.notified_value, DOORBELL_NOTIFIED);
    assert_eq!(
        region.page_length % u64::try_from(region.slot_length).map_err(io::Error::other)?,
        0,
        "One page must hold a whole number of Bell Region slots"
    );

    let directory = tempdir()?;

    for (index, vector) in region.valid.iter().enumerate() {
        let slot_count = NonZeroU32::new(vector.slot_count)
            .ok_or_else(|| io::Error::other("a valid vector needs a non-zero slot count"))?;
        let path = directory.path().join(format!("valid-{index}.bells"));
        create_bell_region(&path, slot_count, parse_u64(&vector.epoch)?)
            .map_err(io::Error::other)?;
        let created = fs::read(&path)?;
        let expected = decode_hex(&vector.encoded_hex)?;
        assert_eq!(
            u64::try_from(expected.len()).map_err(io::Error::other)? % region.page_length,
            0,
            "A Bell Region file is a whole number of pages: {}",
            vector.name
        );
        assert_eq!(created, expected, "{}", vector.name);
        assert_eq!(
            BellRegion::open(&path)
                .map_err(io::Error::other)?
                .slot_count(),
            slot_count,
            "{}",
            vector.name
        );

        // Arming one slot is the only other write this format allows, so it
        // pins the armed word every language must agree on.
        arm_bell_slot(&path, 0).map_err(io::Error::other)?;
        let mut armed_expected = created.clone();
        armed_expected[region.header_length..region.header_length + 4]
            .copy_from_slice(&region.armed_value.to_le_bytes());
        assert_eq!(fs::read(&path)?, armed_expected, "{}", vector.name);
        assert_eq!(
            BellRegion::open(&path)
                .map_err(io::Error::other)?
                .slot_count(),
            slot_count,
            "an armed region must reopen: {}",
            vector.name
        );
    }

    for (index, vector) in region.invalid.iter().enumerate() {
        let path = directory.path().join(format!("invalid-{index}.bells"));
        let encoded = decode_hex(&vector.encoded_hex)?;
        fs::write(&path, &encoded)?;
        assert_eq!(
            pinned_error_code(BellRegion::open(&path).err().ok_or_else(|| {
                io::Error::other("an invalid Bell Region vector was accepted")
            })?),
            vector.expected_error_code,
            "{}",
            vector.name
        );
        assert_eq!(fs::read(&path)?, encoded, "{}", vector.name);
    }
    Ok(())
}

/// Returns the stable code a shared contract pins for one failure.
fn pinned_error_code(error: BellError) -> String {
    error.code().to_owned()
}

#[test]
fn a_fresh_queue_header_publishes_no_doorbell_slot() -> io::Result<()> {
    let vectors = vectors()?;
    let directory = tempdir()?;
    for (index, capacity_units) in [64_u64, 4096].into_iter().enumerate() {
        let capacity = DataCapacity::try_from(capacity_units).map_err(io::Error::other)?;
        let max_payload_size = NonZeroU64::new(capacity.get() - FRAME_HEADER_LEN as u64)
            .ok_or_else(|| io::Error::other("Queue payload limit must be non-zero"))?;
        let path = directory.path().join(format!("queue-{index}.queue"));
        create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
        let bytes = fs::read(&path)?;
        for offset in [HEADER_READER_BELL_OFFSET, HEADER_WRITER_BELL_OFFSET] {
            let word = bytes[offset..offset + 4]
                .try_into()
                .map_err(io::Error::other)?;
            assert_eq!(
                u32::from_le_bytes(word),
                vectors.unbound_bell_slot,
                "byte {offset} of a fresh Queue header is not the unbound slot"
            );
        }
    }
    Ok(())
}

#[test]
fn shared_frame_vectors_fix_record_wrap_and_error_bytes() -> io::Result<()> {
    let vectors = vectors()?;
    let capacity = DataCapacity::try_from(4096).map_err(io::Error::other)?;
    let max_payload_size = parse_non_zero(&vectors.frames.max_payload_size)?;

    for vector in vectors.frames.valid_records {
        let record = decode_hex(&vector.record_hex)?;
        let expected = decode_hex(&vector.encoded_hex)?;
        let mut encoded = vec![255; expected.len()];
        let written = encode_record_frame(capacity, max_payload_size, &record, &mut encoded)
            .map_err(io::Error::other)?;
        assert_eq!(written, expected.len(), "{}", vector.name);
        assert_eq!(encoded, expected, "{}", vector.name);
        let Frame::Record(decoded) =
            decode_frame(capacity, max_payload_size, &encoded).map_err(io::Error::other)?
        else {
            return Err(io::Error::other(format!(
                "record vector decoded as a wrap marker: {}",
                vector.name
            )));
        };
        assert_eq!(decoded, record, "{}", vector.name);
    }

    let mut wrap = vec![255; FRAME_HEADER_LEN];
    encode_wrap_frame(&mut wrap).map_err(io::Error::other)?;
    assert_eq!(
        wrap,
        decode_hex(&vectors.frames.wrap.encoded_hex)?,
        "{}",
        vectors.frames.wrap.name
    );
    assert_eq!(
        decode_frame(capacity, max_payload_size, &wrap).map_err(io::Error::other)?,
        Frame::Wrap
    );

    for vector in vectors.frames.invalid {
        let encoded = decode_hex(&vector.encoded_hex)?;
        let Err(error) = decode_frame(capacity, max_payload_size, &encoded) else {
            return Err(io::Error::other(format!(
                "invalid shared frame vector was accepted: {}",
                vector.name
            )));
        };
        assert_eq!(error.code(), vector.expected_error_code, "{}", vector.name);
    }
    Ok(())
}

#[test]
fn frame_header_inspection_does_not_require_the_record_body() -> io::Result<()> {
    let capacity = DataCapacity::try_from(4096).map_err(io::Error::other)?;
    let max_payload_size = parse_non_zero("4088")?;
    let header = [9, 0, 0, 0, 0, 0, 0, 0];

    assert_eq!(
        inspect_frame_header(capacity, max_payload_size, &header).map_err(io::Error::other)?,
        FrameHeader::Record { frame_len: 24 }
    );
    assert_eq!(
        decode_frame(capacity, max_payload_size, &header).map_err(|error| error.code()),
        Err("ipc.queue.frame_truncated")
    );
    Ok(())
}

#[test]
fn shared_append_vectors_fix_wrap_full_and_exhaustion() -> io::Result<()> {
    let vectors = vectors()?;
    for vector in vectors.append {
        let capacity =
            DataCapacity::try_from(parse_u64(&vector.data_capacity)?).map_err(io::Error::other)?;
        let max_payload_size = parse_non_zero(&vector.max_payload_size)?;
        let commit =
            LogicalPosition::try_from(parse_u64(&vector.commit)?).map_err(io::Error::other)?;
        let release =
            LogicalPosition::try_from(parse_u64(&vector.release)?).map_err(io::Error::other)?;
        let result = plan_append(
            capacity,
            max_payload_size,
            commit,
            release,
            vector.record_length,
        );
        match vector.result {
            AppendResultVector::Ready => {
                let AppendDecision::Ready(plan) = result.map_err(io::Error::other)? else {
                    return Err(io::Error::other(format!(
                        "ready append vector returned Full: {}",
                        vector.name
                    )));
                };
                assert_eq!(
                    plan.write_offset(),
                    parse_optional_u64(vector.write_offset.as_deref())?,
                    "{}",
                    vector.name
                );
                assert_eq!(
                    plan.frame_offset(),
                    parse_optional_u64(vector.frame_offset.as_deref())?,
                    "{}",
                    vector.name
                );
                assert_eq!(plan.frame_len(), vector.frame_length.unwrap_or_default());
                assert_eq!(
                    plan.wrap_len(),
                    parse_optional_u64(vector.wrap_length.as_deref())?,
                    "{}",
                    vector.name
                );
                assert_eq!(
                    plan.next_commit().get(),
                    parse_optional_u64(vector.next_commit.as_deref())?,
                    "{}",
                    vector.name
                );
            }
            AppendResultVector::Full => {
                assert_eq!(result.map_err(io::Error::other)?, AppendDecision::Full);
            }
            AppendResultVector::Error => {
                let Err(error) = result else {
                    return Err(io::Error::other(format!(
                        "error append vector was accepted: {}",
                        vector.name
                    )));
                };
                assert_eq!(
                    Some(error.code()),
                    vector.expected_error_code.as_deref(),
                    "{}",
                    vector.name
                );
            }
        }
    }
    Ok(())
}

#[test]
fn full_is_zero_mutation_and_release_controls_reuse() -> io::Result<()> {
    let mut model = QueueModel::new(64)?;
    let first = vec![1; 8];
    let second = vec![2; 8];
    let third = vec![3; 8];
    let fourth = vec![4; 16];
    assert_eq!(model.append(&first)?, ModelAppendOutcome::Appended);
    assert_eq!(model.append(&second)?, ModelAppendOutcome::Appended);
    assert_eq!(model.append(&third)?, ModelAppendOutcome::Appended);

    let first_owned = model
        .copy_next()?
        .ok_or_else(|| io::Error::other("missing first record"))?;
    let second_owned = model
        .copy_next()?
        .ok_or_else(|| io::Error::other("missing second record"))?;
    assert_eq!(first_owned.record()?, first);
    assert_eq!(second_owned.record()?, second);

    let before = model.snapshot();
    assert_eq!(model.append(&fourth)?, ModelAppendOutcome::Full);
    assert_eq!(model.snapshot(), before);

    model.release_through(first_owned.end)?;
    assert_eq!(model.append(&[4; 8])?, ModelAppendOutcome::Appended);
    assert_eq!(first_owned.record()?, first);
    assert_eq!(second_owned.record()?, second);
    Ok(())
}

#[test]
fn reader_skips_the_complete_wrap_tail_without_losing_a_record() -> io::Result<()> {
    let mut model = QueueModel::new(64)?;
    let first = vec![1; 40];
    let second = vec![2; 16];
    assert_eq!(model.append(&first)?, ModelAppendOutcome::Appended);
    let first_owned = model
        .copy_next()?
        .ok_or_else(|| io::Error::other("missing first record"))?;
    assert_eq!(first_owned.record()?, first);
    model.release_through(first_owned.end)?;

    assert_eq!(model.append(&second)?, ModelAppendOutcome::Appended);
    let second_owned = model
        .copy_next()?
        .ok_or_else(|| io::Error::other("missing wrapped record"))?;
    assert_eq!(second_owned.record()?, second);
    assert_eq!(second_owned.end, 88);
    Ok(())
}

#[test]
fn doorbell_rings_coalesce_outside_the_queue_header() -> io::Result<()> {
    let mut model = QueueModel::new(64)?;

    assert_eq!(model.append(&[1])?, ModelAppendOutcome::Appended);
    assert_eq!(model.reader_doorbell, DOORBELL_NOTIFIED);
    assert_eq!(model.append(&[2])?, ModelAppendOutcome::Appended);
    assert_eq!(model.reader_doorbell, DOORBELL_NOTIFIED);

    let first = model
        .copy_next()?
        .ok_or_else(|| io::Error::other("missing first signaled record"))?;
    let second = model
        .copy_next()?
        .ok_or_else(|| io::Error::other("missing second signaled record"))?;
    assert_eq!(first.record()?, [1]);
    assert_eq!(second.record()?, [2]);
    model.release_through(first.end)?;
    assert_eq!(model.writer_doorbell, DOORBELL_NOTIFIED);
    model.release_through(second.end)?;
    assert_eq!(model.writer_doorbell, DOORBELL_NOTIFIED);
    assert_eq!(model.commit, model.release);

    // A doorbell word is peer-owned state outside this file. The header still
    // carries only the two slot indices both loops published when they bound.
    let snapshot = model.snapshot();
    assert_eq!(snapshot.reader_bell_slot, READER_BELL_SLOT);
    assert_eq!(snapshot.writer_bell_slot, WRITER_BELL_SLOT);
    Ok(())
}

#[test]
fn a_peer_ordinal_the_bell_region_cannot_hold_is_reported_and_left_in_place() -> io::Result<()> {
    let directory = tempdir()?;
    let path = directory.path().join("corrupt-peer.queue");
    let capacity = DataCapacity::try_from(64).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(capacity.get() - FRAME_HEADER_LEN as u64)
        .ok_or_else(|| io::Error::other("Queue payload limit must be non-zero"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let own_region = directory.path().join("writer.bells");
    let peer_region = directory.path().join("reader.bells");
    for region in [&own_region, &peer_region] {
        create_bell_region(
            region,
            NonZeroU32::new(2).ok_or_else(|| io::Error::other("slots must be positive"))?,
            1,
        )
        .map_err(io::Error::other)?;
    }
    let mut writer = open_writer(&path, &own_region, 0, &peer_region).map_err(io::Error::other)?;

    // Any 32-bit word is a legal Queue-level ordinal, so an ordinal this peer's
    // region cannot hold fails at the ring that resolves it, and the writer
    // reports it rather than rewriting the peer's published ordinal.
    write_header_word(&path, HEADER_READER_BELL_OFFSET, 2)?;
    let error = writer
        .try_write_with(
            b"A".len(),
            |destination| std::io::Write::write_all(destination, b"A"),
            || {},
        )
        .err()
        .ok_or_else(|| io::Error::other("a peer ordinal beyond the Bell Region was accepted"))?;
    let QueueRuntimeError::Bell(bell) = &error else {
        return Err(io::Error::other(format!("unexpected error: {error}")));
    };
    assert_eq!(bell.code(), "ipc.bell.slot_out_of_range");
    assert_eq!(read_header_word(&path, HEADER_READER_BELL_OFFSET)?, 2);
    Ok(())
}

/// Writes one little-endian word into a Queue header through a separate handle.
fn write_header_word(path: &Path, offset: usize, value: u32) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    let file = fs::OpenOptions::new().write(true).open(path)?;
    file.write_all_at(&value.to_le_bytes(), offset as u64)
}

/// Reads one little-endian word from a Queue header.
fn read_header_word(path: &Path, offset: usize) -> io::Result<u32> {
    use std::os::unix::fs::FileExt;

    let file = fs::File::open(path)?;
    let mut word = [0_u8; 4];
    file.read_exact_at(&mut word, offset as u64)?;
    Ok(u32::from_le_bytes(word))
}

#[test]
fn record_size_boundaries_have_stable_errors() -> io::Result<()> {
    let small_capacity = DataCapacity::try_from(4096).map_err(io::Error::other)?;
    let minimum_capacity = DataCapacity::from_file_len(208).map_err(io::Error::other)?;
    let protocol_capacity = DataCapacity::from_file_len(17_825_992).map_err(io::Error::other)?;
    let small_limit = parse_non_zero("4088")?;
    let minimum_limit = parse_non_zero("8")?;
    let protocol_limit = parse_non_zero("17825792")?;

    assert_eq!(
        record_frame_len(small_capacity, small_limit, 0).map_err(|error| error.code()),
        Err("ipc.queue.record_empty")
    );
    assert!(record_frame_len(small_capacity, small_limit, 4088).is_ok());
    assert_eq!(
        record_frame_len(small_capacity, small_limit, 4089).map_err(|error| error.code()),
        Err("ipc.queue.record_too_large")
    );
    assert!(record_frame_len(minimum_capacity, minimum_limit, 8).is_ok());
    assert_eq!(
        record_frame_len(minimum_capacity, minimum_limit, 9).map_err(|error| error.code()),
        Err("ipc.queue.record_too_large")
    );
    assert!(record_frame_len(protocol_capacity, protocol_limit, 17 * 1024 * 1024).is_ok());
    assert_eq!(
        record_frame_len(protocol_capacity, protocol_limit, 17 * 1024 * 1024 + 1)
            .map_err(|error| error.code()),
        Err("ipc.queue.record_too_large")
    );
    assert_eq!(
        DataCapacity::try_from(15).map_err(|error| error.code()),
        Err("ipc.queue.capacity_invalid")
    );
    assert_eq!(minimum_capacity.get(), 16);
    assert_eq!(
        DataCapacity::from_file_len(i64::MAX.unsigned_abs() + 1).map_err(|error| error.code()),
        Err("ipc.queue.capacity_invalid")
    );
    Ok(())
}

#[test]
fn configured_payload_above_sixteen_mebibytes_round_trips() -> io::Result<()> {
    let capacity =
        DataCapacity::try_from(u64::try_from(17 * 1024 * 1024 + 8).map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
    let max_payload_size = parse_non_zero("17825792")?;
    let record = vec![42; 17 * 1024 * 1024];
    let mut encoded = vec![255; 17 * 1024 * 1024 + 8];
    let written = encode_record_frame(capacity, max_payload_size, &record, &mut encoded)
        .map_err(io::Error::other)?;
    assert_eq!(written, 17 * 1024 * 1024 + 8);
    assert_eq!(
        decode_frame(capacity, max_payload_size, &encoded).map_err(io::Error::other)?,
        Frame::Record(record.as_slice())
    );
    Ok(())
}

proptest! {
    #[test]
    fn valid_headers_round_trip_for_arbitrary_state(
        release_units in 0_u64..=(u64::MAX / 8 - 512),
        occupied_units in 0_u64..=512,
        reader_bell_slot: u32,
        writer_bell_slot: u32,
    ) {
        let capacity = DataCapacity::try_from(4096)?;
        let max_payload_size = NonZeroU64::new(capacity.get() - FRAME_HEADER_LEN as u64)
            .ok_or_else(|| TestCaseError::fail("Queue payload limit must be non-zero"))?;
        let release = LogicalPosition::try_from(release_units * 8)?;
        let commit = LogicalPosition::try_from((release_units + occupied_units) * 8)?;
        let header = Header::new(
            max_payload_size,
            commit,
            reader_bell_slot,
            release,
            writer_bell_slot,
            capacity,
        )?;
        let encoded = header.encode();
        prop_assert_eq!(Header::decode(capacity, &encoded)?, header);
    }



    #[test]
    fn arbitrary_bytes_are_rejected_or_decoded_without_panicking(
        bytes in prop::collection::vec(any::<u8>(), 0..1024),
    ) {
        let capacity = DataCapacity::try_from(4096)?;
        let max_payload_size = NonZeroU64::new(capacity.get() - FRAME_HEADER_LEN as u64)
            .ok_or_else(|| TestCaseError::fail("Queue payload limit must be non-zero"))?;
        let _header_result = Header::decode(capacity, &bytes);
        let _frame_result = decode_frame(capacity, max_payload_size, &bytes);
    }

    #[test]
    fn every_successful_append_is_read_once_in_order_without_silent_loss(
        records in prop::collection::vec(prop::collection::vec(any::<u8>(), 1..49), 0..128),
        drain_after_append in prop::collection::vec(any::<bool>(), 0..128),
    ) {
        let mut model = QueueModel::new(256)?;
        let mut expected = VecDeque::new();

        for (index, record) in records.into_iter().enumerate() {
            let before = model.snapshot();
            match model.append(&record)? {
                ModelAppendOutcome::Appended => expected.push_back(record),
                ModelAppendOutcome::Full => prop_assert_eq!(model.snapshot(), before),
            }

            if drain_after_append.get(index).copied().unwrap_or(false)
                && let Some(owned) = model.copy_next()?
            {
                let expected_record = expected.pop_front().ok_or_else(|| {
                    TestCaseError::fail("reader returned a record that was never committed")
                })?;
                prop_assert_eq!(owned.record()?, expected_record);
                model.release_through(owned.end)?;
            }
        }

        while let Some(owned) = model.copy_next()? {
            let expected_record = expected.pop_front().ok_or_else(|| {
                TestCaseError::fail("reader returned a record that was never committed")
            })?;
            prop_assert_eq!(owned.record()?, expected_record);
            model.release_through(owned.end)?;
        }
        prop_assert!(expected.is_empty());
        prop_assert_eq!(model.release, model.commit);
    }


}

fn vectors() -> io::Result<QueueTestVectors> {
    serde_json::from_slice(TEST_VECTORS).map_err(io::Error::other)
}

fn header_from_vector(vector: &ValidHeaderVector, capacity: DataCapacity) -> io::Result<Header> {
    Header::new(
        parse_non_zero(&vector.max_payload_size)?,
        LogicalPosition::try_from(parse_u64(&vector.commit)?).map_err(io::Error::other)?,
        vector.reader_bell_slot,
        LogicalPosition::try_from(parse_u64(&vector.release)?).map_err(io::Error::other)?,
        vector.writer_bell_slot,
        capacity,
    )
    .map_err(io::Error::other)
}

fn parse_non_zero(value: &str) -> io::Result<NonZeroU64> {
    NonZeroU64::new(parse_u64(value)?).ok_or_else(|| io::Error::other("expected non-zero u64"))
}

fn parse_u64(value: &str) -> io::Result<u64> {
    value.parse().map_err(io::Error::other)
}

fn parse_optional_u64(value: Option<&str>) -> io::Result<u64> {
    parse_u64(value.ok_or_else(|| io::Error::other("missing expected u64 field"))?)
}

fn decode_hex(source: &str) -> io::Result<Vec<u8>> {
    if !source.len().is_multiple_of(2) {
        return Err(io::Error::other("hex string length must be even"));
    }
    source
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let digits = std::str::from_utf8(pair).map_err(io::Error::other)?;
            u8::from_str_radix(digits, 16).map_err(io::Error::other)
        })
        .collect()
}

#[path = "support/queue_model.rs"]
mod queue_model;
use queue_model::{ModelAppendOutcome, QueueModel};
use tenon_ipc::queue::contract_test_support::open_writer;

proptest! {
    #[test]
    fn append_never_overwrites_live_bytes(slots in 2_u64..1000,used in 0_u64..1000,wraps in 0_u64..1000,body in 1_usize..5000) {
        let capacity=DataCapacity::try_from(slots*8)?;
        let release=LogicalPosition::try_from(wraps*capacity.get())?;
        let commit=LogicalPosition::try_from(release.get()+(used%(slots+1))*8)?;
        let maximum=NonZeroU64::new(capacity.get()-8).ok_or_else(||TestCaseError::fail("nonzero maximum"))?;
        if let Ok(AppendDecision::Ready(plan))=plan_append(capacity,maximum,commit,release,body) {
            prop_assert!(plan.next_commit()>commit);
            prop_assert!(plan.next_commit().get()-release.get()<=capacity.get());
            prop_assert_eq!(plan.next_commit().get()%8,0);
            prop_assert!(plan.write_offset()+plan.wrap_len()<=capacity.get());
        }
    }
}
