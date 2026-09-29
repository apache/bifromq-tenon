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

use crate::metrics::capture_test_support::Capture;
use crate::pipeline::channel::metrics::FlowMetrics;
use crate::pipeline::channel::metrics::{ChannelMetrics, WaitKind};
use std::io;
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use prost::Message;

use super::{
    IngressCompletionWriteOutcome, IngressQueueError, IngressQueuePair, completion_capacity,
    submission_capacity,
};
use crate::contracts::source::{IngressCompletion, IngressCompletionStatus, IngressRecord};
use tenon_ipc::bell::{BellInterrupter, BellRegion, WaitOutcome, create_bell_region};
use tenon_ipc::queue::{
    QueueReader, QueueWaiter, QueueWriter, ReadOutcome, WriteOutcome, create_queue_file,
    queue_waiter_is_armed,
};

#[test]
fn receive_returns_an_owned_record_and_releases_submission() -> io::Result<()> {
    let fixture = IngressPairFixture::new(2, 128)?;
    let mut source_writer = fixture.source_submission_writer()?;
    let expected = IngressRecord {
        record_id: 41,
        payload: vec![1, 2, 3].into(),
    };
    let WriteOutcome::Committed(receipt) = source_writer
        .try_write(&expected.encode_to_vec())
        .map_err(io::Error::other)?
    else {
        return Err(io::Error::other("Submission record was not committed"));
    };
    let mut pair = fixture.open_pair()?;
    assert!(pair.readable().map_err(io::Error::other)?);

    let actual = pair
        .try_receive(&ChannelMetrics::default())
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("Committed Submission was not readable"))?;

    assert_eq!(actual, expected);
    assert!(
        source_writer
            .is_released(&receipt)
            .map_err(io::Error::other)?
    );
    Ok(())
}

#[test]
fn protobuf_decoding_reuses_the_owned_input_allocation() -> io::Result<()> {
    let encoded = Bytes::from(
        IngressRecord {
            record_id: 41,
            payload: vec![1, 2, 3, 4].into(),
        }
        .encode_to_vec(),
    );
    let allocation_start = encoded.as_ptr() as usize;
    let allocation_end = allocation_start + encoded.len();

    let decoded = IngressRecord::decode(encoded).map_err(io::Error::other)?;

    let payload_start = decoded.payload.as_ptr() as usize;
    let payload_end = payload_start + decoded.payload.len();
    assert!(payload_start >= allocation_start);
    assert!(payload_end <= allocation_end);
    Ok(())
}

#[test]
fn complete_commits_the_exact_completion_record() -> io::Result<()> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Vectors {
        completion_valid: Vec<CompletionVector>,
    }
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct CompletionVector {
        name: String,
        record_id: u64,
        status: String,
        encoded: Vec<u8>,
    }
    let vectors: Vectors = serde_json::from_slice(include_bytes!(
        "../../../contracts/source/ingress_record_test_vectors.json"
    ))
    .map_err(io::Error::other)?;
    assert!(!vectors.completion_valid.is_empty());
    let fixture = IngressPairFixture::new(2, 128)?;
    let mut pair = fixture.open_pair()?;
    let mut source_reader = fixture.source_completion_reader()?;

    for vector in vectors.completion_valid {
        let status = match vector.status.as_str() {
            "OK" => IngressCompletionStatus::Ok,
            "RETRY" => IngressCompletionStatus::Retry,
            "ERROR" => IngressCompletionStatus::Error,
            // Admission backpressure never enters the Pipeline writer.
            "BACKPRESSURE" => continue,
            _ => return Err(io::Error::other("Unknown completion vector status")),
        };
        assert_eq!(
            pair.complete(
                vector.record_id,
                status,
                &ChannelMetrics::default(),
                &mut ChannelMetrics::default().wait(WaitKind::CompletionCapacity)
            )
            .map_err(io::Error::other)?,
            IngressCompletionWriteOutcome::Committed,
            "{}",
            vector.name
        );
        let ReadOutcome::Record(record) = source_reader.try_read().map_err(io::Error::other)?
        else {
            return Err(io::Error::other("Completion record was not committed"));
        };
        assert_eq!(record.payload(), vector.encoded, "{}", vector.name);
        source_reader.release(1).map_err(io::Error::other)?;
    }
    Ok(())
}

#[test]
fn submission_interruption_returns_control_without_reading() -> io::Result<()> {
    let fixture = IngressPairFixture::new(2, 128)?;
    let mut pair = fixture.open_pair()?;
    let interrupter = pair.submission.wait_interrupter();

    interrupter.interrupt().map_err(io::Error::other)?;

    assert_eq!(
        pair.submission.wait_readable().map_err(io::Error::other)?,
        WaitOutcome::Interrupted
    );
    assert!(!pair.readable().map_err(io::Error::other)?);
    let mut source_writer = fixture.source_submission_writer()?;
    assert!(matches!(
        source_writer
            .try_write(
                &IngressRecord {
                    record_id: 42,
                    payload: vec![4, 2].into(),
                }
                .encode_to_vec()
            )
            .map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    assert_eq!(
        pair.try_receive(&ChannelMetrics::default())
            .map_err(io::Error::other)?
            .map(|record| record.record_id),
        Some(42)
    );
    Ok(())
}

#[test]
fn an_empty_submission_probe_leaves_the_queue_reusable() -> io::Result<()> {
    let fixture = IngressPairFixture::new(2, 128)?;
    let mut pair = fixture.open_pair()?;

    assert!(!pair.readable().map_err(io::Error::other)?);
    assert!(
        pair.try_receive(&ChannelMetrics::default())
            .map_err(io::Error::other)?
            .is_none()
    );

    let mut source_writer = fixture.source_submission_writer()?;
    assert!(matches!(
        source_writer
            .try_write(
                &IngressRecord {
                    record_id: 43,
                    payload: vec![4, 3].into(),
                }
                .encode_to_vec()
            )
            .map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    assert_eq!(
        pair.try_receive(&ChannelMetrics::default())
            .map_err(io::Error::other)?
            .map(|record| record.record_id),
        Some(43)
    );
    Ok(())
}

#[test]
fn completion_interruption_leaves_a_full_queue_unchanged() -> io::Result<()> {
    let fixture = IngressPairFixture::new(1, 128)?;
    let encoded = IngressCompletion {
        record_id: u64::MAX,
        status: IngressCompletionStatus::Error as i32,
    }
    .encode_to_vec();
    assert_eq!(encoded.len(), super::COMPLETION_MAX_PAYLOAD_SIZE);
    let mut initial_writer = fixture.completion_filler()?;
    for _ in 0..2 {
        assert!(matches!(
            initial_writer
                .try_write(&encoded)
                .map_err(io::Error::other)?,
            WriteOutcome::Committed(_)
        ));
    }
    drop(initial_writer);
    let before = std::fs::read(&fixture.completion_path)?;
    let mut pair = fixture.open_pair()?;
    let interrupter = pair.completion.wait_interrupter();

    interrupter.interrupt().map_err(io::Error::other)?;

    assert_eq!(
        pair.complete(
            42,
            IngressCompletionStatus::Ok,
            &ChannelMetrics::default(),
            &mut ChannelMetrics::default().wait(WaitKind::CompletionCapacity)
        )
        .map_err(io::Error::other)?,
        IngressCompletionWriteOutcome::Interrupted
    );
    assert_eq!(std::fs::read(&fixture.completion_path)?, before);
    Ok(())
}

#[test]
fn completion_wait_resumes_after_the_source_releases_space() -> io::Result<()> {
    let fixture = IngressPairFixture::new(1, 128)?;
    let encoded = maximum_completion(u64::MAX);
    let mut initial_writer = fixture.completion_filler()?;
    for _ in 0..2 {
        assert!(matches!(
            initial_writer
                .try_write(&encoded)
                .map_err(io::Error::other)?,
            WriteOutcome::Committed(_)
        ));
    }
    drop(initial_writer);
    let pair = fixture.open_pair()?;
    let interrupter = pair.completion.wait_interrupter();
    let mut source_reader = fixture.source_completion_reader()?;
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let writer = std::thread::spawn(move || {
        let mut pair = pair;
        drop(sender.send(pair.complete(
            u64::MAX - 1,
            IngressCompletionStatus::Error,
            &ChannelMetrics::default(),
            &mut ChannelMetrics::default().wait(WaitKind::CompletionCapacity),
        )));
    });

    let release_result = (|| -> io::Result<()> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !queue_waiter_is_armed(
            &fixture.completion_path,
            &fixture._directory.path().join("channels.bells"),
            QueueWaiter::Writer,
        )
        .map_err(io::Error::other)?
        {
            if std::time::Instant::now() >= deadline {
                return Err(io::Error::other(
                    "Completion writer never armed its doorbell",
                ));
            }
            std::thread::yield_now();
        }
        if !matches!(
            source_reader.try_read().map_err(io::Error::other)?,
            ReadOutcome::Record(_)
        ) {
            return Err(io::Error::other("Full Completion Queue was not readable"));
        }
        source_reader.release(1).map_err(io::Error::other)
    })();
    if let Err(source) = release_result {
        stop_completion_writer(&interrupter, writer)?;
        return Err(source);
    }

    let completion = match receiver.recv_timeout(std::time::Duration::from_secs(10)) {
        Ok(completion) => completion,
        Err(source) => {
            stop_completion_writer(&interrupter, writer)?;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("Completion writer did not resume: {source}"),
            ));
        }
    };
    writer
        .join()
        .map_err(|_| io::Error::other("Completion writer thread panicked"))?;
    let completion = completion.map_err(io::Error::other)?;
    assert_eq!(completion, IngressCompletionWriteOutcome::Committed);
    for expected in [encoded, maximum_completion(u64::MAX - 1)] {
        let ReadOutcome::Record(record) = source_reader.try_read().map_err(io::Error::other)?
        else {
            return Err(io::Error::other("Completion record was not committed"));
        };
        assert_eq!(record.payload(), expected);
        source_reader.release(1).map_err(io::Error::other)?;
    }
    assert!(matches!(
        source_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    ));
    Ok(())
}

#[test]
fn opening_rejects_queue_pairs_with_different_pending_limits() -> io::Result<()> {
    let fixture = IngressPairFixture::new(2, 128)?;
    let mismatched_capacity = completion_capacity(
        NonZeroU64::new(3)
            .ok_or_else(|| io::Error::other("Pending record limit must be positive"))?,
    )
    .map_err(io::Error::other)?;
    std::fs::OpenOptions::new()
        .write(true)
        .open(&fixture.completion_path)?
        .set_len(mismatched_capacity.file_len())?;

    assert!(matches!(
        IngressQueuePair::open(
            &fixture.submission_path,
            &fixture.completion_path,
            &fixture.loop_bell()?,
            &fixture.source_region,
        ),
        Err(IngressQueueError::PairLayout(_))
    ));
    Ok(())
}

#[test]
fn malformed_submission_is_not_released() -> io::Result<()> {
    let fixture = IngressPairFixture::new(2, 128)?;
    let mut source_writer = fixture.source_submission_writer()?;
    let WriteOutcome::Committed(receipt) = source_writer
        .try_write(&[0x12, 0x02, 0x01])
        .map_err(io::Error::other)?
    else {
        return Err(io::Error::other("Malformed Submission was not committed"));
    };
    let mut pair = fixture.open_pair()?;

    assert!(matches!(
        pair.try_receive(&ChannelMetrics::default()),
        Err(IngressQueueError::IngressRecordDecode(_))
    ));
    drop(pair);
    assert!(
        !source_writer
            .is_released(&receipt)
            .map_err(io::Error::other)?
    );
    let mut reopened = fixture.open_pair()?;
    assert!(matches!(
        reopened.try_receive(&ChannelMetrics::default()),
        Err(IngressQueueError::IngressRecordDecode(_))
    ));
    Ok(())
}

#[test]
fn unspecified_completion_is_rejected_without_queue_changes() -> io::Result<()> {
    let fixture = IngressPairFixture::new(2, 128)?;
    let mut pair = fixture.open_pair()?;
    let before = std::fs::read(&fixture.completion_path)?;

    assert!(matches!(
        pair.complete(
            41,
            IngressCompletionStatus::Unspecified,
            &ChannelMetrics::default(),
            &mut ChannelMetrics::default().wait(WaitKind::CompletionCapacity)
        ),
        Err(IngressQueueError::InvalidCompletionStatus)
    ));
    assert_eq!(std::fs::read(&fixture.completion_path)?, before);
    assert_eq!(
        pair.complete(
            41,
            IngressCompletionStatus::Ok,
            &ChannelMetrics::default(),
            &mut ChannelMetrics::default().wait(WaitKind::CompletionCapacity)
        )
        .map_err(io::Error::other)?,
        IngressCompletionWriteOutcome::Committed
    );
    Ok(())
}

fn maximum_completion(record_id: u64) -> Vec<u8> {
    IngressCompletion {
        record_id,
        status: IngressCompletionStatus::Error as i32,
    }
    .encode_to_vec()
}

proptest! {
    #[test]
    fn arbitrary_binary_submission_round_trips_and_releases(
        record_id in any::<u64>(),
        payload in prop::collection::vec(any::<u8>(), 0..256),
    ) {
        let fixture = IngressPairFixture::new(2, 512).map_err(test_case_error)?;
        let mut source_writer = fixture.source_submission_writer().map_err(test_case_error)?;
        let expected = IngressRecord {
            record_id,
            payload: payload.into(),
        };
        let WriteOutcome::Committed(receipt) = source_writer
            .try_write(&expected.encode_to_vec())
            .map_err(test_case_error)?
        else {
            return Err(TestCaseError::fail("Generated Submission record was not committed"));
        };
        let mut pair = fixture.open_pair().map_err(test_case_error)?;

        let actual = pair
            .try_receive(&ChannelMetrics::default())
            .map_err(test_case_error)?
            .ok_or_else(|| TestCaseError::fail("Generated Submission was not readable"))?;

        prop_assert_eq!(actual, expected);
        prop_assert!(source_writer.is_released(&receipt).map_err(test_case_error)?);
    }
}

fn test_case_error(error: impl std::fmt::Display) -> TestCaseError {
    TestCaseError::fail(error.to_string())
}

fn stop_completion_writer(
    interrupter: &BellInterrupter,
    writer: std::thread::JoinHandle<()>,
) -> io::Result<()> {
    let interrupt_result = interrupter.interrupt().map_err(io::Error::other);
    let join_result = writer
        .join()
        .map_err(|_| io::Error::other("Completion writer thread panicked"));
    interrupt_result?;
    join_result
}

struct IngressPairFixture {
    _directory: tempfile::TempDir,
    submission_path: PathBuf,
    completion_path: PathBuf,
    /// The Channel loop's own doorbell region, one slot.
    channel_region: Arc<BellRegion>,
    /// The Source loop's doorbell region, two slots: submission, then completion.
    source_region: Arc<BellRegion>,
}

impl IngressPairFixture {
    fn new(max_pending_records: u64, max_record_size_bytes: u64) -> io::Result<Self> {
        let directory = tempfile::tempdir()?;
        let submission_path = directory.path().join("submission.queue");
        let completion_path = directory.path().join("completion.queue");
        let max_pending_records = NonZeroU64::new(max_pending_records)
            .ok_or_else(|| io::Error::other("Pending record limit must be positive"))?;
        let max_record_size_bytes = NonZeroU64::new(max_record_size_bytes)
            .ok_or_else(|| io::Error::other("Record size limit must be positive"))?;
        create_queue_file(
            &submission_path,
            submission_capacity(max_pending_records, max_record_size_bytes)
                .map_err(io::Error::other)?,
            max_record_size_bytes,
        )
        .map_err(io::Error::other)?;
        create_queue_file(
            &completion_path,
            completion_capacity(max_pending_records).map_err(io::Error::other)?,
            NonZeroU64::new(super::COMPLETION_MAX_PAYLOAD_SIZE as u64)
                .ok_or_else(|| io::Error::other("Completion payload limit must be positive"))?,
        )
        .map_err(io::Error::other)?;
        let channel_region = create_test_region(&directory.path().join("channels.bells"), 1)?;
        let source_region = create_test_region(&directory.path().join("source-loops.bells"), 2)?;
        Ok(Self {
            _directory: directory,
            submission_path,
            completion_path,
            channel_region,
            source_region,
        })
    }

    fn open_pair(&self) -> io::Result<IngressQueuePair> {
        IngressQueuePair::open(
            &self.submission_path,
            &self.completion_path,
            &self.loop_bell()?,
            &self.source_region,
        )
        .map_err(io::Error::other)
    }

    /// The Channel loop's doorbell, as the production Channel would own it.
    fn loop_bell(&self) -> io::Result<Arc<tenon_ipc::bell::LoopBell>> {
        self.channel_region.loop_bell(0).map_err(io::Error::other)
    }

    /// Opens the Source-side Submission writer the way the Source Instance does.
    fn source_submission_writer(&self) -> io::Result<QueueWriter> {
        QueueWriter::open(
            &self.submission_path,
            self.source_region.loop_bell(0).map_err(io::Error::other)?,
            Arc::clone(&self.channel_region),
        )
        .map_err(io::Error::other)
    }

    /// Opens the Source-side Completion reader the way the Source Instance does.
    fn source_completion_reader(&self) -> io::Result<QueueReader> {
        QueueReader::open(
            &self.completion_path,
            self.source_region.loop_bell(1).map_err(io::Error::other)?,
            Arc::clone(&self.channel_region),
        )
        .map_err(io::Error::other)
    }

    /// Opens a bare Completion writer for tests that only need a full Queue.
    fn completion_filler(&self) -> io::Result<QueueWriter> {
        QueueWriter::open(
            &self.completion_path,
            self.loop_bell()?,
            Arc::clone(&self.channel_region),
        )
        .map_err(io::Error::other)
    }
}

fn create_test_region(path: &std::path::Path, slots: u32) -> io::Result<Arc<BellRegion>> {
    let slot_count = NonZeroU32::new(slots).ok_or_else(|| io::Error::other("slots"))?;
    create_bell_region(path, slot_count, 1).map_err(io::Error::other)?;
    BellRegion::open(path).map_err(io::Error::other)
}

#[test]
fn completion_retry_after_interrupt_records_ready_when_capacity_has_returned() -> io::Result<()> {
    let fixture = IngressPairFixture::new(1, 128)?;
    let encoded = IngressCompletion {
        record_id: u64::MAX,
        status: IngressCompletionStatus::Error as i32,
    }
    .encode_to_vec();
    let mut writer = fixture.completion_filler()?;
    while matches!(
        writer.try_write(&encoded).map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ) {}
    drop(writer);
    let mut pair = fixture.open_pair()?;
    let captured = Capture::new();
    let metrics = FlowMetrics::new(&captured.meter());
    let flow = metrics.flow("flow");
    let channel = flow.channel(0);
    channel.bind();
    let mut wait = channel.wait(WaitKind::CompletionCapacity);
    pair.completion
        .wait_interrupter()
        .interrupt()
        .map_err(io::Error::other)?;
    assert_eq!(
        pair.complete(42, IngressCompletionStatus::Ok, &channel, &mut wait)
            .map_err(io::Error::other)?,
        IngressCompletionWriteOutcome::Interrupted
    );
    assert_eq!(
        captured.collect()?.number("tenon.flow.waiting", &[]),
        Some(3.0)
    );
    let mut reader = fixture.source_completion_reader()?;
    while let ReadOutcome::Record(_) = reader.try_read().map_err(io::Error::other)? {
        reader.release(1).map_err(io::Error::other)?;
    }
    assert_eq!(
        pair.complete(42, IngressCompletionStatus::Ok, &channel, &mut wait)
            .map_err(io::Error::other)?,
        IngressCompletionWriteOutcome::Committed
    );
    drop(wait);
    let snapshot = captured.collect()?;
    assert_eq!(
        snapshot
            .histogram("tenon.flow.wait.duration", &[("result", "ready")])
            .map(|point| point.count),
        Some(1)
    );
    assert_eq!(
        snapshot
            .histogram("tenon.flow.wait.duration", &[("result", "cancelled")])
            .map(|point| point.count),
        None
    );
    assert_eq!(snapshot.number("tenon.flow.waiting", &[]), Some(0.0));
    assert_eq!(
        snapshot.number("tenon.flow.completion.records", &[]),
        Some(1.0)
    );
    Ok(())
}
