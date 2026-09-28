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

//! A FIFO/replay oracle shared by property tests and coverage-guided fuzzing.

use std::collections::VecDeque;
use std::error::Error;
use std::num::NonZeroU64;

use crate::doorbell_queue::{open_queue_reader, open_queue_writer};
use tenon_ipc::queue::{DataCapacity, ReadOutcome, WriteOutcome, create_queue_file};

pub(super) fn verify_queue_model(
    capacity: u64,
    max_payload_size: u64,
    actions: impl IntoIterator<Item = (u8, Vec<u8>)>,
) -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    create_queue_file(
        &path,
        DataCapacity::try_from(capacity)?,
        NonZeroU64::new(max_payload_size).ok_or("zero payload limit")?,
    )?;
    let mut writer = open_queue_writer(&path)?;
    let mut reader = open_queue_reader(&path)?;
    let mut unread = VecDeque::new();
    let mut unreleased = VecDeque::new();
    for (operation, payload) in actions {
        match operation % 5 {
            0 => {
                let before = std::fs::read(&path)?;
                match writer.try_write(&payload)? {
                    WriteOutcome::Committed(_) => unread.push_back(payload),
                    WriteOutcome::Full => assert_eq!(std::fs::read(&path)?, before),
                }
            }
            1 => {
                let before = std::fs::read(&path)?;
                match (unread.pop_front(), reader.try_read()?) {
                    (Some(expected), ReadOutcome::Record(record)) => {
                        assert_eq!(record.payload(), expected);
                        unreleased.push_back(expected);
                    }
                    (None, ReadOutcome::Empty) => assert_eq!(std::fs::read(&path)?, before),
                    _ => return Err("Queue diverged from the FIFO model".into()),
                }
            }
            2 if !unreleased.is_empty() => {
                reader.release(1)?;
                let _ = unreleased.pop_front();
            }
            3 => {
                drop(reader);
                reader = open_queue_reader(&path)?;
                unreleased.append(&mut unread);
                std::mem::swap(&mut unread, &mut unreleased);
            }
            4 => {
                drop(writer);
                writer = open_queue_writer(&path)?;
            }
            _ => {}
        }
    }
    drop(reader);
    let mut replay = open_queue_reader(path)?;
    for expected in unreleased.into_iter().chain(unread) {
        let ReadOutcome::Record(record) = replay.try_read()? else {
            return Err("replay record was missing".into());
        };
        assert_eq!(record.payload(), expected);
        replay.release(1)?;
    }
    assert_eq!(replay.try_read()?, ReadOutcome::Empty);
    Ok(())
}
