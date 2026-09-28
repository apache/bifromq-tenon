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
use std::{collections::VecDeque, io, num::NonZeroU64};
use tenon::runner_test_support::ingress_queue::{
    COMPLETION_MAX_PAYLOAD_SIZE, completion_capacity, max_pending_records, submission_capacity,
    validate_pair,
};
use tenon_ipc::queue::{AppendDecision, DataCapacity, Header, LogicalPosition, plan_append};
#[expect(
    dead_code,
    reason = "Admission properties use only the shared model capacity and FIFO operations"
)]
#[path = "../sdk/rust/ipc/tests/support/queue_model.rs"]
mod queue_model;
use queue_model::{ModelAppendOutcome, QueueModel};
fn parse_non_zero(value: &str) -> io::Result<NonZeroU64> {
    value.parse().map_err(io::Error::other)
}
#[test]
fn configured_record_limit_can_exceed_sixteen_mebibytes() -> io::Result<()> {
    let maximum = parse_non_zero("17825792")?;
    let capacity = submission_capacity(parse_non_zero("1")?, maximum).map_err(io::Error::other)?;
    let zero = LogicalPosition::try_from(0).map_err(io::Error::other)?;
    let header = Header::new(maximum, zero, 1, zero, 1, capacity).map_err(io::Error::other)?;
    assert_eq!(
        Header::decode(capacity, &header.encode()).map_err(io::Error::other)?,
        header
    );
    assert!(matches!(
        plan_append(capacity, maximum, zero, zero, 17 * 1024 * 1024).map_err(io::Error::other)?,
        AppendDecision::Ready(_)
    ));
    Ok(())
}
#[test]
fn ingress_pair_recovers_the_configured_pending_limit() -> io::Result<()> {
    let configured_pending_limit = parse_non_zero("3")?;
    let maximum_submission_payload_size = parse_non_zero("1016")?;
    let submission_data_capacity =
        submission_capacity(configured_pending_limit, maximum_submission_payload_size)
            .map_err(io::Error::other)?;
    let completion_data_capacity =
        completion_capacity(configured_pending_limit).map_err(io::Error::other)?;
    let zero = LogicalPosition::try_from(0).map_err(io::Error::other)?;
    let submission = Header::new(
        maximum_submission_payload_size,
        zero,
        0,
        zero,
        0,
        submission_data_capacity,
    )
    .map_err(io::Error::other)?;
    let completion = Header::new(
        NonZeroU64::new(u64::try_from(COMPLETION_MAX_PAYLOAD_SIZE).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("completion payload limit must be non-zero"))?,
        zero,
        0,
        zero,
        0,
        completion_data_capacity,
    )
    .map_err(io::Error::other)?;

    validate_pair(
        submission,
        submission_data_capacity,
        completion,
        completion_data_capacity,
    )
    .map_err(io::Error::other)?;
    let max_pending_records =
        max_pending_records(submission, submission_data_capacity).map_err(io::Error::other)?;
    assert_eq!(max_pending_records, configured_pending_limit);
    assert_eq!(
        completion_data_capacity,
        completion_capacity(max_pending_records).map_err(io::Error::other)?
    );

    let mismatched_submission_capacity =
        DataCapacity::try_from(submission_data_capacity.get() - 8).map_err(io::Error::other)?;
    let mismatched_submission = Header::new(
        submission.max_payload_size(),
        submission.commit(),
        submission.reader_bell_slot(),
        submission.release(),
        submission.writer_bell_slot(),
        mismatched_submission_capacity,
    )
    .map_err(io::Error::other)?;
    assert_eq!(
        validate_pair(
            mismatched_submission,
            mismatched_submission_capacity,
            completion,
            completion_data_capacity,
        )
        .map_err(|error| error.code()),
        Err("ipc.queue.capacity_invalid")
    );

    let completion_with_wrong_payload_limit = Header::new(
        parse_non_zero("12")?,
        completion.commit(),
        completion.reader_bell_slot(),
        completion.release(),
        completion.writer_bell_slot(),
        completion_data_capacity,
    )
    .map_err(io::Error::other)?;
    assert_eq!(
        validate_pair(
            submission,
            submission_data_capacity,
            completion_with_wrong_payload_limit,
            completion_data_capacity,
        )
        .map_err(|error| error.code()),
        Err("ipc.queue.capacity_invalid")
    );

    let mismatched_completion_capacity =
        DataCapacity::try_from(completion_data_capacity.get() + 24).map_err(io::Error::other)?;
    assert_eq!(
        validate_pair(
            submission,
            submission_data_capacity,
            completion,
            mismatched_completion_capacity,
        )
        .map_err(|error| error.code()),
        Err("ipc.queue.capacity_invalid")
    );
    Ok(())
}
#[test]
fn ingress_capacity_arithmetic_rejects_overflow() -> io::Result<()> {
    let maximum_pending =
        NonZeroU64::new(u64::MAX).ok_or_else(|| io::Error::other("u64::MAX must be non-zero"))?;
    let one = NonZeroU64::new(1).ok_or_else(|| io::Error::other("one must be non-zero"))?;

    assert_eq!(
        submission_capacity(maximum_pending, one).map_err(|error| error.code()),
        Err("ipc.queue.capacity_invalid")
    );
    assert_eq!(
        completion_capacity(maximum_pending).map_err(|error| error.code()),
        Err("ipc.queue.capacity_invalid")
    );
    Ok(())
}
proptest! {
    #[test]
    fn submission_headers_round_trip_and_recover_the_pending_limit(
        pending_limit_value in 1_u16..=1024,
        max_record_size_bytes in 1_u16..=4096,
    ) {
        let configured_pending_limit = NonZeroU64::new(u64::from(pending_limit_value))
            .ok_or_else(|| TestCaseError::fail("generated pending limit must be non-zero"))?;
        let max_record_size_bytes = NonZeroU64::new(u64::from(max_record_size_bytes))
            .ok_or_else(|| TestCaseError::fail("generated record limit must be non-zero"))?;
        let capacity = submission_capacity(configured_pending_limit, max_record_size_bytes)?;
        let zero = LogicalPosition::try_from(0)?;
        let header = Header::new(
            max_record_size_bytes,
            zero,
            0,
            zero,
            0,
            capacity,
        )?;
        let encoded = header.encode();

        prop_assert_eq!(Header::decode(capacity, &encoded)?, header);
        prop_assert_eq!(
            max_pending_records(header, capacity)?,
            configured_pending_limit
        );
    }
    #[test]
    fn submission_wrap_reserve_prevents_full_below_the_pending_limit(
        pending_limit in 1_u16..=16,
        maximum_payload_size in 1_u16..=256,
        operations in prop::collection::vec((any::<bool>(), any::<u16>()), 0..256),
    ) {
        let pending_limit_usize = usize::from(pending_limit);
        let maximum_payload_size_usize = usize::from(maximum_payload_size);
        let pending_limit = NonZeroU64::new(u64::from(pending_limit))
            .ok_or_else(|| TestCaseError::fail("generated pending limit must be non-zero"))?;
        let maximum_payload_size = NonZeroU64::new(u64::from(maximum_payload_size))
            .ok_or_else(|| TestCaseError::fail("generated payload limit must be non-zero"))?;
        let capacity = submission_capacity(pending_limit, maximum_payload_size)?;
        let mut model = QueueModel::with_max_payload_size(capacity, maximum_payload_size)?;
        let mut pending = VecDeque::new();

        for (write, record_seed) in operations {
            if write && pending.len() < pending_limit_usize {
                let record_length = 1 + usize::from(record_seed) % maximum_payload_size_usize;
                let record = vec![record_seed.to_le_bytes()[0]; record_length];
                prop_assert_eq!(model.append(&record)?, ModelAppendOutcome::Appended);
                pending.push_back(record);
            } else if let Some(expected) = pending.pop_front() {
                let owned = model.copy_next()?.ok_or_else(|| {
                    TestCaseError::fail("pending Ingress record was not readable")
                })?;
                prop_assert_eq!(owned.record()?, expected);
                model.release_through(owned.end)?;
            }
        }
    }
}
