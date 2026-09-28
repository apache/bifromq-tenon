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

//! Mapped Queue ordering, release, replay, and cross-process wait/wake contracts.

use std::fs;
use std::io;
use std::num::NonZeroU64;
use std::process::Command;
use std::thread;

use proptest::prelude::*;
use tenon_ipc::bell::{BellInterrupter, WaitOutcome};
use tenon_ipc::queue::{
    DataCapacity, HEADER_LEN, Header, LogicalPosition, QueueReader, QueueRuntimeError, QueueWaiter,
    QueueWriter, ReadOutcome, UNBOUND_BELL_SLOT, WriteOutcome, create_queue_file,
};

#[path = "support/wait.rs"]
mod common;
#[path = "support/doorbell_queue.rs"]
mod doorbell_queue;
#[path = "support/ipc_queue_model.rs"]
mod ipc_queue_model;

use common::wait_for_armed_loop;
use doorbell_queue::{
    open_queue_reader, open_queue_writer, queue_bell_region, queue_bell_region_path,
};

#[test]
fn mapped_queue_wraps_without_losing_reader_owned_bytes() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(48).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(16).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;

    let mut writer = open_queue_writer(&path).map_err(io::Error::other)?;
    let mut reader = open_queue_reader(&path).map_err(io::Error::other)?;

    assert!(matches!(
        writer.try_write(b"record-one").map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    assert!(matches!(
        writer.try_write(b"B").map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));

    let ReadOutcome::Record(first) = reader.try_read().map_err(io::Error::other)? else {
        return Err(io::Error::other("first record was not available"));
    };
    assert_eq!(first.payload(), b"record-one");
    reader.release(1).map_err(io::Error::other)?;

    assert!(matches!(
        writer.try_write(b"record-two").map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    assert_eq!(first.payload(), b"record-one");

    let ReadOutcome::Record(second) = reader.try_read().map_err(io::Error::other)? else {
        return Err(io::Error::other("second record was not available"));
    };
    let ReadOutcome::Record(third) = reader.try_read().map_err(io::Error::other)? else {
        return Err(io::Error::other("wrapped record was not available"));
    };
    assert_eq!(second.payload(), b"B");
    assert_eq!(third.payload(), b"record-two");
    reader.release(2).map_err(io::Error::other)?;
    assert_eq!(
        reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );
    Ok(())
}

#[test]
fn write_receipts_follow_the_shared_release_prefix() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(32).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let mut writer = open_queue_writer(&path).map_err(io::Error::other)?;
    let mut reader = open_queue_reader(&path).map_err(io::Error::other)?;

    let WriteOutcome::Committed(first_receipt) =
        writer.try_write(b"A").map_err(io::Error::other)?
    else {
        return Err(io::Error::other("first record was not committed"));
    };
    let WriteOutcome::Committed(second_receipt) =
        writer.try_write(b"B").map_err(io::Error::other)?
    else {
        return Err(io::Error::other("second record was not committed"));
    };
    assert!(
        !writer
            .is_released(&first_receipt)
            .map_err(io::Error::other)?
    );
    assert!(
        !writer
            .is_released(&second_receipt)
            .map_err(io::Error::other)?
    );

    assert!(matches!(
        reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    reader.release(1).map_err(io::Error::other)?;
    assert!(
        writer
            .is_released(&first_receipt)
            .map_err(io::Error::other)?
    );
    assert!(
        !writer
            .is_released(&second_receipt)
            .map_err(io::Error::other)?
    );

    assert!(matches!(
        reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    reader.release(1).map_err(io::Error::other)?;
    assert!(
        writer
            .is_released(&second_receipt)
            .map_err(io::Error::other)?
    );
    Ok(())
}

#[test]
fn write_receipts_are_bound_to_one_opened_writer() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let first_path = directory.path().join("first.queue");
    let second_path = directory.path().join("second.queue");
    let capacity = DataCapacity::try_from(16).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&first_path, capacity, max_payload_size).map_err(io::Error::other)?;
    create_queue_file(&second_path, capacity, max_payload_size).map_err(io::Error::other)?;

    let mut first_writer = open_queue_writer(&first_path).map_err(io::Error::other)?;
    let WriteOutcome::Committed(receipt) =
        first_writer.try_write(b"A").map_err(io::Error::other)?
    else {
        return Err(io::Error::other("first record was not committed"));
    };
    let mut second_writer = open_queue_writer(&second_path).map_err(io::Error::other)?;
    assert!(matches!(
        second_writer.try_write(b"B").map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    let mut second_reader = open_queue_reader(&second_path).map_err(io::Error::other)?;
    assert!(matches!(
        second_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    second_reader.release(1).map_err(io::Error::other)?;
    assert!(matches!(
        second_writer.is_released(&receipt),
        Err(QueueRuntimeError::WriteReceiptOwnerMismatch)
    ));

    let mut first_reader = open_queue_reader(&first_path).map_err(io::Error::other)?;
    assert!(matches!(
        first_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    first_reader.release(1).map_err(io::Error::other)?;
    drop(first_writer);
    let mut reopened_writer = open_queue_writer(first_path).map_err(io::Error::other)?;
    assert!(matches!(
        reopened_writer.wait_released(&receipt),
        Err(QueueRuntimeError::WriteReceiptOwnerMismatch)
    ));
    Ok(())
}

#[test]
fn wrapped_write_receipt_includes_the_skipped_tail() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(48).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(16).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let mut writer = open_queue_writer(&path).map_err(io::Error::other)?;
    let mut reader = open_queue_reader(&path).map_err(io::Error::other)?;

    assert!(matches!(
        writer.try_write(b"record-one").map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    assert!(matches!(
        writer.try_write(b"B").map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    assert!(matches!(
        reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    reader.release(1).map_err(io::Error::other)?;

    let WriteOutcome::Committed(wrapped_receipt) =
        writer.try_write(b"record-two").map_err(io::Error::other)?
    else {
        return Err(io::Error::other("wrapped record was not committed"));
    };
    assert!(matches!(
        reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    assert!(matches!(
        reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));

    reader.release(1).map_err(io::Error::other)?;
    assert!(
        !writer
            .is_released(&wrapped_receipt)
            .map_err(io::Error::other)?
    );
    reader.release(1).map_err(io::Error::other)?;
    assert!(
        writer
            .is_released(&wrapped_receipt)
            .map_err(io::Error::other)?
    );
    Ok(())
}

#[test]
fn wait_released_wakes_when_the_reader_releases_the_receipt() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(32).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let mut writer = open_queue_writer(&path).map_err(io::Error::other)?;
    let WriteOutcome::Committed(receipt) = writer.try_write(b"A").map_err(io::Error::other)? else {
        return Err(io::Error::other("record was not committed"));
    };
    let mut reader = open_queue_reader(&path).map_err(io::Error::other)?;
    assert!(matches!(
        reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let waiter = thread::spawn(move || {
        drop(sender.send(writer.wait_released(&receipt)));
    });

    wait_for_armed_loop(&path, &queue_bell_region_path(&path)?, QueueWaiter::Writer)?;
    reader.release(1).map_err(io::Error::other)?;
    let outcome = receiver
        .recv_timeout(std::time::Duration::from_secs(10))
        .map_err(|_| io::Error::other("writer did not observe the released receipt"))?
        .map_err(io::Error::other)?;
    waiter
        .join()
        .map_err(|_| io::Error::other("writer waiter thread panicked"))?;
    assert_eq!(outcome, WaitOutcome::Ready);
    Ok(())
}

#[test]
fn local_interrupter_wakes_a_writer_waiting_for_release() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(32).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let mut writer = open_queue_writer(&path).map_err(io::Error::other)?;
    let WriteOutcome::Committed(receipt) = writer.try_write(b"A").map_err(io::Error::other)? else {
        return Err(io::Error::other("record was not committed"));
    };
    let interrupter = writer.wait_interrupter();
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let waiter = thread::spawn(move || {
        drop(sender.send(writer.wait_released(&receipt)));
    });

    wait_for_armed_loop(&path, &queue_bell_region_path(&path)?, QueueWaiter::Writer)?;
    interrupter.interrupt().map_err(io::Error::other)?;
    let outcome = receiver
        .recv_timeout(std::time::Duration::from_secs(10))
        .map_err(|_| io::Error::other("release wait did not observe the local interrupt"))?
        .map_err(io::Error::other)?;
    waiter
        .join()
        .map_err(|_| io::Error::other("writer waiter thread panicked"))?;
    assert_eq!(outcome, WaitOutcome::Interrupted);
    Ok(())
}

#[test]
fn full_write_does_not_change_any_queue_byte() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(32).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let mut writer = open_queue_writer(&path).map_err(io::Error::other)?;

    assert!(matches!(
        writer.try_write(b"A").map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    assert!(matches!(
        writer.try_write(b"B").map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    let before = fs::read(&path)?;
    assert_eq!(
        writer.try_write(b"C").map_err(io::Error::other)?,
        WriteOutcome::Full
    );
    assert_eq!(fs::read(path)?, before);
    Ok(())
}

#[test]
fn empty_read_does_not_change_any_queue_byte() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(32).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let created = fs::read(&path)?;
    let zero = LogicalPosition::try_from(0).map_err(io::Error::other)?;
    let expected_header = Header::new(
        max_payload_size,
        zero,
        UNBOUND_BELL_SLOT,
        zero,
        UNBOUND_BELL_SLOT,
        capacity,
    )
    .map_err(io::Error::other)?
    .encode();
    assert_eq!(
        created.len(),
        usize::try_from(capacity.file_len()).map_err(io::Error::other)?
    );
    assert_eq!(&created[..HEADER_LEN], expected_header.as_slice());
    assert!(created[HEADER_LEN..].iter().all(|byte| *byte == 0));
    let mut reader = open_queue_reader(&path).map_err(io::Error::other)?;
    // Opening publishes this loop's doorbell slot, so the unchanged-bytes
    // assertion starts after the open: the read below writes nothing.
    let opened = fs::read(&path)?;

    assert_eq!(reader.data_capacity(), capacity);
    assert_eq!(reader.max_payload_size(), max_payload_size);
    assert_eq!(
        reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );
    assert_eq!(fs::read(path)?, opened);
    Ok(())
}

#[test]
fn opening_rejects_corrupt_immutable_header_bytes() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(32).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let mut bytes = fs::read(&path)?;
    bytes[0] = b'X';
    fs::write(&path, bytes)?;

    let region = queue_bell_region(&path)?;
    let error = QueueReader::open(
        &path,
        region.loop_bell(1).map_err(io::Error::other)?,
        region,
    )
    .err();
    assert_eq!(format_error_code(error), Some("ipc.queue.magic_invalid"));
    Ok(())
}

#[test]
fn opening_rejects_unaligned_live_position() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(32).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let mut bytes = fs::read(&path)?;
    bytes[64..72].copy_from_slice(&1_u64.to_le_bytes());
    fs::write(&path, bytes)?;

    let region = queue_bell_region(&path)?;
    let error = QueueWriter::open(
        &path,
        region.loop_bell(0).map_err(io::Error::other)?,
        region,
    )
    .err();
    assert_eq!(
        format_error_code(error),
        Some("ipc.queue.position_unaligned")
    );
    Ok(())
}

#[test]
fn reading_rejects_corrupt_committed_frame_bytes() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(32).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let mut writer = open_queue_writer(&path).map_err(io::Error::other)?;
    writer.try_write(b"A").map_err(io::Error::other)?;
    drop(writer);

    let mut bytes = fs::read(&path)?;
    bytes[HEADER_LEN + 4] = 1;
    fs::write(&path, bytes)?;
    let mut reader = open_queue_reader(path).map_err(io::Error::other)?;
    let error = reader.try_read().err();
    assert_eq!(
        format_error_code(error),
        Some("ipc.queue.frame_header_padding_nonzero")
    );
    Ok(())
}

#[test]
fn unreleased_record_is_replayed_after_reader_reopens() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(32).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let mut writer = open_queue_writer(&path).map_err(io::Error::other)?;
    writer.try_write(b"A").map_err(io::Error::other)?;

    let mut reader = open_queue_reader(&path).map_err(io::Error::other)?;
    let ReadOutcome::Record(record) = reader.try_read().map_err(io::Error::other)? else {
        return Err(io::Error::other("record was not available"));
    };
    assert_eq!(record.payload(), b"A");
    drop(reader);

    let mut reopened = open_queue_reader(&path).map_err(io::Error::other)?;
    let ReadOutcome::Record(replayed) = reopened.try_read().map_err(io::Error::other)? else {
        return Err(io::Error::other("unreleased record was not replayed"));
    };
    assert_eq!(replayed.payload(), b"A");
    reopened.release(1).map_err(io::Error::other)?;
    drop(reopened);

    let mut released = open_queue_reader(path).map_err(io::Error::other)?;
    assert_eq!(
        released.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );
    Ok(())
}

#[test]
fn invalid_release_count_does_not_publish_capacity() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(32).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let mut writer = open_queue_writer(&path).map_err(io::Error::other)?;
    writer.try_write(b"A").map_err(io::Error::other)?;

    let mut reader = open_queue_reader(&path).map_err(io::Error::other)?;
    assert!(matches!(
        reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    let error = reader.release(2).err();
    assert!(matches!(
        error,
        Some(QueueRuntimeError::InvalidReleaseCount)
    ));
    drop(reader);

    let mut reopened = open_queue_reader(path).map_err(io::Error::other)?;
    assert!(matches!(
        reopened.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    Ok(())
}

#[test]
fn create_does_not_replace_an_existing_queue() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(32).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let before = fs::read(&path)?;

    let error = create_queue_file(&path, capacity, max_payload_size).err();
    assert!(matches!(error, Some(QueueRuntimeError::Io { .. })));
    assert_eq!(fs::read(path)?, before);
    Ok(())
}

#[test]
fn mapped_writer_and_reader_preserve_order_across_threads() -> io::Result<()> {
    const RECORD_COUNT: u64 = 2_000;
    const MAX_ATTEMPTS_WITHOUT_PROGRESS: usize = 1_000_000;

    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(256).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let writer_path = path.clone();
    let reader_path = path;

    let writer = thread::spawn(move || -> io::Result<()> {
        let mut writer = open_queue_writer(writer_path).map_err(io::Error::other)?;
        for sequence in 0..RECORD_COUNT {
            let record = sequence.to_le_bytes();
            let mut committed = false;
            for _ in 0..MAX_ATTEMPTS_WITHOUT_PROGRESS {
                match writer.try_write(&record).map_err(io::Error::other)? {
                    WriteOutcome::Committed(_) => {
                        committed = true;
                        break;
                    }
                    WriteOutcome::Full => thread::yield_now(),
                }
            }
            if !committed {
                return Err(io::Error::other("writer made no progress"));
            }
        }
        Ok(())
    });
    let reader = thread::spawn(move || -> io::Result<()> {
        let mut reader = open_queue_reader(reader_path).map_err(io::Error::other)?;
        for expected in 0..RECORD_COUNT {
            let mut received = false;
            for _ in 0..MAX_ATTEMPTS_WITHOUT_PROGRESS {
                match reader.try_read().map_err(io::Error::other)? {
                    ReadOutcome::Record(record) => {
                        assert_eq!(record.payload(), expected.to_le_bytes());
                        reader.release(1).map_err(io::Error::other)?;
                        received = true;
                        break;
                    }
                    ReadOutcome::Empty => thread::yield_now(),
                }
            }
            if !received {
                return Err(io::Error::other("reader made no progress"));
            }
        }
        Ok(())
    });

    writer
        .join()
        .map_err(|_| io::Error::other("writer thread panicked"))??;
    reader
        .join()
        .map_err(|_| io::Error::other("reader thread panicked"))??;
    Ok(())
}

#[test]
fn mapped_positions_are_visible_across_processes() -> io::Result<()> {
    if std::env::var_os("TENON_TEST_QUEUE_PATH").is_some() {
        return write_from_process_environment();
    }

    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(16).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;

    run_writer_process(&path, "first")?;
    let mut reader = open_queue_reader(&path).map_err(io::Error::other)?;
    let ReadOutcome::Record(first) = reader.try_read().map_err(io::Error::other)? else {
        return Err(io::Error::other("child record was not visible"));
    };
    assert_eq!(first.payload(), b"first");
    reader.release(1).map_err(io::Error::other)?;

    run_writer_process(&path, "second")?;
    let ReadOutcome::Record(second) = reader.try_read().map_err(io::Error::other)? else {
        return Err(io::Error::other("released capacity was not reusable"));
    };
    assert_eq!(second.payload(), b"second");
    Ok(())
}

fn write_from_process_environment() -> io::Result<()> {
    let path = std::env::var_os("TENON_TEST_QUEUE_PATH")
        .ok_or(io::Error::other("Queue path is missing"))?;
    let payload = std::env::var("TENON_TEST_QUEUE_PAYLOAD")
        .map_err(|error| io::Error::other(error.to_string()))?;
    let mut writer = open_queue_writer(path).map_err(io::Error::other)?;
    assert!(matches!(
        writer
            .try_write(payload.as_bytes())
            .map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    Ok(())
}

fn run_writer_process(path: &std::path::Path, payload: &str) -> io::Result<()> {
    let status = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("mapped_positions_are_visible_across_processes")
        .env("TENON_TEST_QUEUE_PATH", path)
        .env("TENON_TEST_QUEUE_PAYLOAD", payload)
        .status()?;
    if !status.success() {
        return Err(io::Error::other("mapped Queue writer process failed"));
    }
    Ok(())
}

#[test]
fn local_interrupt_before_wait_coalesces_without_changing_queue_bytes() -> io::Result<()> {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<BellInterrupter>();

    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(16).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let mut reader = open_queue_reader(&path).map_err(io::Error::other)?;
    let interrupter = reader.wait_interrupter();
    // Opening published this loop's doorbell slot; an interrupt rings the Bell
    // Region and must not touch one Queue byte.
    let opened = fs::read(&path)?;

    interrupter.interrupt().map_err(io::Error::other)?;
    interrupter.interrupt().map_err(io::Error::other)?;
    assert_eq!(fs::read(&path)?, opened);
    assert_eq!(
        reader.wait_readable().map_err(io::Error::other)?,
        WaitOutcome::Interrupted
    );

    let mut writer = open_queue_writer(&path).map_err(io::Error::other)?;
    assert!(matches!(
        writer.try_write(b"A").map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    assert_eq!(
        reader.wait_readable().map_err(io::Error::other)?,
        WaitOutcome::Ready
    );
    Ok(())
}

#[test]
fn local_interrupter_wakes_a_blocked_reader() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(16).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let mut reader = open_queue_reader(&path).map_err(io::Error::other)?;
    let interrupter = reader.wait_interrupter();
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let waiter = thread::spawn(move || {
        drop(sender.send(reader.wait_readable()));
    });

    wait_for_armed_loop(&path, &queue_bell_region_path(&path)?, QueueWaiter::Reader)?;
    interrupter.interrupt().map_err(io::Error::other)?;
    let outcome = receiver
        .recv_timeout(std::time::Duration::from_secs(10))
        .map_err(|_| io::Error::other("reader wait did not observe the local interrupt"))?
        .map_err(io::Error::other)?;
    waiter
        .join()
        .map_err(|_| io::Error::other("reader waiter thread panicked"))?;
    assert_eq!(outcome, WaitOutcome::Interrupted);
    Ok(())
}

#[test]
fn local_interrupter_wakes_a_blocked_writer() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(16).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let mut writer = open_queue_writer(&path).map_err(io::Error::other)?;
    assert!(matches!(
        writer.try_write(b"full").map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    let interrupter = writer.wait_interrupter();
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let waiter = thread::spawn(move || {
        drop(sender.send(writer.wait_writable(1)));
    });

    wait_for_armed_loop(&path, &queue_bell_region_path(&path)?, QueueWaiter::Writer)?;
    interrupter.interrupt().map_err(io::Error::other)?;
    let outcome = receiver
        .recv_timeout(std::time::Duration::from_secs(10))
        .map_err(|_| io::Error::other("writer wait did not observe the local interrupt"))?
        .map_err(io::Error::other)?;
    waiter
        .join()
        .map_err(|_| io::Error::other("writer waiter thread panicked"))?;
    assert_eq!(outcome, WaitOutcome::Interrupted);
    Ok(())
}

#[test]
fn local_interrupter_is_a_noop_after_its_owner_is_dropped() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(16).map_err(io::Error::other)?;
    create_queue_file(&path, capacity, NonZeroU64::MIN).map_err(io::Error::other)?;
    let reader = open_queue_reader(path).map_err(io::Error::other)?;
    let interrupter = reader.wait_interrupter();
    drop(reader);

    interrupter.interrupt().map_err(io::Error::other)
}

#[test]
fn mapped_waits_across_processes() -> io::Result<()> {
    if let Some(mode) = std::env::var_os("TENON_TEST_WAIT_MODE") {
        return run_wait_child(&mode);
    }

    let directory = tempfile::tempdir()?;
    let capacity = DataCapacity::try_from(16).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;

    let data_path = directory.path().join("data.queue");
    let data_ready = directory.path().join("data.ready");
    create_queue_file(&data_path, capacity, max_payload_size).map_err(io::Error::other)?;
    let data_child = spawn_wait_child("read", &data_path, &data_ready, "data")?;
    wait_for_marker(&data_ready)?;
    wait_for_armed_loop(
        &data_path,
        &queue_bell_region_path(&data_path)?,
        QueueWaiter::Reader,
    )?;
    let mut data_writer = open_queue_writer(&data_path).map_err(io::Error::other)?;
    assert!(matches!(
        data_writer.try_write(b"data").map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    data_child.wait()?;

    let space_path = directory.path().join("space.queue");
    let space_ready = directory.path().join("space.ready");
    create_queue_file(&space_path, capacity, max_payload_size).map_err(io::Error::other)?;
    let mut space_writer = open_queue_writer(&space_path).map_err(io::Error::other)?;
    assert!(matches!(
        space_writer.try_write(b"full").map_err(io::Error::other)?,
        WriteOutcome::Committed(_)
    ));
    drop(space_writer);
    let space_child = spawn_wait_child("write", &space_path, &space_ready, "next")?;
    wait_for_marker(&space_ready)?;
    wait_for_armed_loop(
        &space_path,
        &queue_bell_region_path(&space_path)?,
        QueueWaiter::Writer,
    )?;
    let mut space_reader = open_queue_reader(&space_path).map_err(io::Error::other)?;
    assert!(matches!(
        space_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Record(_)
    ));
    space_reader.release(1).map_err(io::Error::other)?;
    space_child.wait()?;
    let ReadOutcome::Record(next) = space_reader.try_read().map_err(io::Error::other)? else {
        return Err(io::Error::other("woken writer did not publish its record"));
    };
    assert_eq!(next.payload(), b"next");
    Ok(())
}

#[test]
fn mapped_wait_stress_preserves_order_without_polling() -> io::Result<()> {
    const RECORD_COUNT: u64 = 10_000;

    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let capacity = DataCapacity::try_from(64).map_err(io::Error::other)?;
    let max_payload_size = NonZeroU64::new(8).ok_or(io::Error::other("zero payload limit"))?;
    create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
    let writer_path = path.clone();

    let writer = thread::spawn(move || -> io::Result<()> {
        let mut writer = open_queue_writer(writer_path).map_err(io::Error::other)?;
        for sequence in 0..RECORD_COUNT {
            let record = sequence.to_le_bytes();
            loop {
                match writer.try_write(&record).map_err(io::Error::other)? {
                    WriteOutcome::Committed(_) => break,
                    WriteOutcome::Full => match writer
                        .wait_writable(record.len())
                        .map_err(io::Error::other)?
                    {
                        WaitOutcome::Ready => {}
                        WaitOutcome::Interrupted => {
                            return Err(io::Error::other("writer wait was interrupted"));
                        }
                    },
                }
            }
        }
        Ok(())
    });
    let reader = thread::spawn(move || -> io::Result<()> {
        let mut reader = open_queue_reader(path).map_err(io::Error::other)?;
        for expected in 0..RECORD_COUNT {
            loop {
                match reader.try_read().map_err(io::Error::other)? {
                    ReadOutcome::Record(record) => {
                        assert_eq!(record.payload(), expected.to_le_bytes());
                        reader.release(1).map_err(io::Error::other)?;
                        break;
                    }
                    ReadOutcome::Empty => {
                        if reader.wait_readable().map_err(io::Error::other)?
                            == WaitOutcome::Interrupted
                        {
                            return Err(io::Error::other("reader wait was interrupted"));
                        }
                    }
                }
            }
        }
        Ok(())
    });

    writer
        .join()
        .map_err(|_| io::Error::other("writer thread panicked"))??;
    reader
        .join()
        .map_err(|_| io::Error::other("reader thread panicked"))??;
    Ok(())
}

fn run_wait_child(mode: &std::ffi::OsStr) -> io::Result<()> {
    let path = std::env::var_os("TENON_TEST_QUEUE_PATH")
        .ok_or(io::Error::other("Queue path is missing"))?;
    let marker = std::env::var_os("TENON_TEST_WAIT_MARKER")
        .ok_or(io::Error::other("wait marker path is missing"))?;
    let payload = std::env::var("TENON_TEST_QUEUE_PAYLOAD")
        .map_err(|error| io::Error::other(error.to_string()))?;
    fs::write(marker, b"ready")?;

    match mode.to_str() {
        Some("read") => {
            let mut reader = open_queue_reader(path).map_err(io::Error::other)?;
            if reader.wait_readable().map_err(io::Error::other)? != WaitOutcome::Ready {
                return Err(io::Error::other("reader wait was interrupted"));
            }
            let ReadOutcome::Record(record) = reader.try_read().map_err(io::Error::other)? else {
                return Err(io::Error::other("reader woke without a record"));
            };
            assert_eq!(record.payload(), payload.as_bytes());
            reader.release(1).map_err(io::Error::other)?;
        }
        Some("write") => {
            let mut writer = open_queue_writer(path).map_err(io::Error::other)?;
            if writer
                .wait_writable(payload.len())
                .map_err(io::Error::other)?
                != WaitOutcome::Ready
            {
                return Err(io::Error::other("writer wait was interrupted"));
            }
            assert!(matches!(
                writer
                    .try_write(payload.as_bytes())
                    .map_err(io::Error::other)?,
                WriteOutcome::Committed(_)
            ));
        }
        _ => return Err(io::Error::other("unknown wait child mode")),
    }
    Ok(())
}

fn spawn_wait_child(
    mode: &str,
    path: &std::path::Path,
    marker: &std::path::Path,
    payload: &str,
) -> io::Result<WaitChild> {
    let child = Command::new(std::env::current_exe()?)
        .arg("--exact")
        .arg("mapped_waits_across_processes")
        .env("TENON_TEST_WAIT_MODE", mode)
        .env("TENON_TEST_QUEUE_PATH", path)
        .env("TENON_TEST_WAIT_MARKER", marker)
        .env("TENON_TEST_QUEUE_PAYLOAD", payload)
        .spawn()?;
    Ok(WaitChild(Some(child)))
}

fn wait_for_marker(path: &std::path::Path) -> io::Result<()> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !path.exists() {
        if std::time::Instant::now() >= deadline {
            return Err(io::Error::other("wait child did not become ready"));
        }
        thread::sleep(std::time::Duration::from_millis(1));
    }
    Ok(())
}

#[derive(Debug)]
struct WaitChild(Option<std::process::Child>);

impl WaitChild {
    fn wait(mut self) -> io::Result<()> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let child = self
                .0
                .as_mut()
                .ok_or(io::Error::other("wait child ownership is missing"))?;
            if let Some(status) = child.try_wait()? {
                self.0 = None;
                return if status.success() {
                    Ok(())
                } else {
                    Err(io::Error::other("wait child process failed"))
                };
            }
            if std::time::Instant::now() >= deadline {
                child.kill()?;
                child.wait()?;
                self.0 = None;
                return Err(io::Error::other("wait child process timed out"));
            }
            thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

impl Drop for WaitChild {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            drop(child.kill());
            drop(child.wait());
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn mapped_runtime_matches_fifo_and_replay_model(
        actions in prop::collection::vec(
            (any::<u8>(), prop::collection::vec(any::<u8>(), 1..=16)),
            1..128,
        ),
    ) {
        ipc_queue_model::verify_queue_model(64, 16, actions).map_err(test_case_error)?;
    }
}

fn test_case_error(error: impl std::fmt::Display) -> TestCaseError {
    TestCaseError::fail(error.to_string())
}

fn format_error_code(error: Option<QueueRuntimeError>) -> Option<&'static str> {
    match error {
        Some(QueueRuntimeError::Format(error)) => Some(error.code()),
        _ => None,
    }
}
