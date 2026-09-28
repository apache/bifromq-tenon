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

//! Reader batches preserve frame boundaries and shared release progress.

use std::{io, num::NonZeroU64};
use tenon_ipc::{
    bell::WaitOutcome,
    queue::{DataCapacity, QueueReader, QueueWriter, ReadOutcome, WriteOutcome, create_queue_file},
};
#[path = "support/doorbell_queue.rs"]
mod doorbell_queue;
use doorbell_queue::{open_queue_reader, open_queue_writer};

fn queue_pair(
    capacity: u64,
    maximum: u64,
) -> io::Result<(tempfile::TempDir, QueueWriter, QueueReader)> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue");
    create_queue_file(
        &path,
        DataCapacity::try_from(capacity).map_err(io::Error::other)?,
        NonZeroU64::new(maximum).ok_or_else(|| io::Error::other("nonzero maximum"))?,
    )?;
    let writer = open_queue_writer(&path)?;
    let reader = open_queue_reader(&path)?;
    Ok((directory, writer, reader))
}

#[test]
fn a_batch_checkpoint_releases_only_its_copied_prefix() -> io::Result<()> {
    let (directory, mut writer, mut reader) = queue_pair(64, 24)?;
    for value in [1, 2, 3] {
        assert!(matches!(
            writer.try_write(&[value; 8])?,
            WriteOutcome::Committed(_)
        ));
    }
    let ReadOutcome::Record(first) = reader.try_read()? else {
        return Err(io::Error::other("first record missing"));
    };
    let boundary = reader.checkpoint()?;
    let ReadOutcome::Record(second) = reader.try_read()? else {
        return Err(io::Error::other("second record missing"));
    };
    reader.release_through(boundary)?;
    assert!(reader.release_through(boundary).is_err());
    drop(reader);
    let mut replay = open_queue_reader(directory.path().join("queue"))?;
    for expected in [2, 3] {
        let ReadOutcome::Record(record) = replay.try_read()? else {
            return Err(io::Error::other("unreleased record missing"));
        };
        assert_eq!(record.payload(), &[expected; 8]);
    }
    replay.release(2)?;
    assert!(matches!(
        writer.try_write(&[9; 24])?,
        WriteOutcome::Committed(_)
    ));
    assert_eq!(first.payload(), &[1; 8]);
    assert_eq!(second.payload(), &[2; 8]);
    Ok(())
}

#[test]
fn a_pending_interrupt_precedes_ready_data_and_space() -> io::Result<()> {
    let (_directory, mut writer, mut reader) = queue_pair(64, 56)?;
    writer.wait_interrupter().interrupt()?;
    assert_eq!(writer.wait_writable(8)?, WaitOutcome::Interrupted);
    reader.wait_interrupter().interrupt()?;
    writer.try_write(&[1; 8])?;
    assert_eq!(reader.wait_readable()?, WaitOutcome::Interrupted);
    assert_eq!(reader.wait_readable()?, WaitOutcome::Ready);
    Ok(())
}
