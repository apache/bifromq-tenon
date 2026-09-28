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

use std::{io, num::NonZeroU64};
use tenon_ipc::queue::{
    AppendDecision, DataCapacity, FRAME_HEADER_LEN, Frame, FrameHeader, Header, LogicalPosition,
    decode_frame, encode_record_frame, encode_wrap_frame, inspect_frame_header, plan_append,
};
const READER_BELL_SLOT: u32 = 3;
const WRITER_BELL_SLOT: u32 = 5;
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueueSnapshot {
    bytes: Vec<u8>,
    pub(crate) commit: u64,
    pub(crate) release: u64,
    pub(crate) reader_bell_slot: u32,
    pub(crate) writer_bell_slot: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ModelAppendOutcome {
    Appended,
    Full,
}

#[derive(Debug)]
pub(crate) struct QueueModel {
    capacity: DataCapacity,
    max_payload_size: NonZeroU64,
    bytes: Vec<u8>,
    pub(crate) commit: u64,
    pub(crate) release: u64,
    local_read: u64,
    pub(crate) reader_bell_slot: u32,
    pub(crate) writer_bell_slot: u32,
    /// The word in the reader loop's doorbell. It lives in that loop's Bell
    /// Region, never in this Queue header, and it saturates at one.
    pub(crate) reader_doorbell: u32,
    /// The word in the writer loop's doorbell, with the same properties.
    pub(crate) writer_doorbell: u32,
}

impl QueueModel {
    pub(crate) fn new(capacity: u64) -> io::Result<Self> {
        let capacity = DataCapacity::try_from(capacity).map_err(io::Error::other)?;
        let max_payload_size = NonZeroU64::new(capacity.get() - FRAME_HEADER_LEN as u64)
            .ok_or_else(|| io::Error::other("Queue payload limit must be non-zero"))?;
        Self::with_max_payload_size(capacity, max_payload_size)
    }

    pub(crate) fn with_max_payload_size(
        capacity: DataCapacity,
        max_payload_size: NonZeroU64,
    ) -> io::Result<Self> {
        Header::new(
            max_payload_size,
            LogicalPosition::try_from(0).map_err(io::Error::other)?,
            READER_BELL_SLOT,
            LogicalPosition::try_from(0).map_err(io::Error::other)?,
            WRITER_BELL_SLOT,
            capacity,
        )
        .map_err(io::Error::other)?;
        Ok(Self {
            capacity,
            max_payload_size,
            bytes: vec![0; usize::try_from(capacity.get()).map_err(io::Error::other)?],
            commit: 0,
            release: 0,
            local_read: 0,
            reader_bell_slot: READER_BELL_SLOT,
            writer_bell_slot: WRITER_BELL_SLOT,
            reader_doorbell: 0,
            writer_doorbell: 0,
        })
    }

    pub(crate) fn snapshot(&self) -> QueueSnapshot {
        QueueSnapshot {
            bytes: self.bytes.clone(),
            commit: self.commit,
            release: self.release,
            reader_bell_slot: self.reader_bell_slot,
            writer_bell_slot: self.writer_bell_slot,
        }
    }

    pub(crate) fn append(&mut self, record: &[u8]) -> io::Result<ModelAppendOutcome> {
        let decision = plan_append(
            self.capacity,
            self.max_payload_size,
            LogicalPosition::try_from(self.commit).map_err(io::Error::other)?,
            LogicalPosition::try_from(self.release).map_err(io::Error::other)?,
            record.len(),
        )
        .map_err(io::Error::other)?;
        let AppendDecision::Ready(plan) = decision else {
            return Ok(ModelAppendOutcome::Full);
        };

        if plan.wrap_len() != 0 {
            let wrap_offset = usize::try_from(plan.write_offset()).map_err(io::Error::other)?;
            encode_wrap_frame(&mut self.bytes[wrap_offset..]).map_err(io::Error::other)?;
        }
        let frame_offset = usize::try_from(plan.frame_offset()).map_err(io::Error::other)?;
        encode_record_frame(
            self.capacity,
            self.max_payload_size,
            record,
            &mut self.bytes[frame_offset..],
        )
        .map_err(io::Error::other)?;
        self.commit = plan.next_commit().get();
        // The committing writer rings the reader loop's doorbell. Ringing twice
        // before that loop wakes is one wake, so the word stays at one.
        self.reader_doorbell = 1;
        Ok(ModelAppendOutcome::Appended)
    }

    pub(crate) fn copy_next(&mut self) -> io::Result<Option<OwnedFrame>> {
        while self.local_read < self.commit {
            let offset =
                usize::try_from(self.local_read % self.capacity.get()).map_err(io::Error::other)?;
            let tail_len = self.bytes.len() - offset;
            match inspect_frame_header(self.capacity, self.max_payload_size, &self.bytes[offset..])
            {
                Ok(FrameHeader::Wrap) => {
                    let next_read = self
                        .local_read
                        .checked_add(u64::try_from(tail_len).map_err(io::Error::other)?)
                        .ok_or_else(|| io::Error::other("model read position overflow"))?;
                    if next_read > self.commit {
                        return Err(io::Error::other("wrap marker exceeds committed bytes"));
                    }
                    self.local_read = next_read;
                }
                Ok(FrameHeader::Record { frame_len }) => {
                    if frame_len > tail_len {
                        return Err(io::Error::other("record frame crosses the physical tail"));
                    }
                    let end = self
                        .local_read
                        .checked_add(u64::try_from(frame_len).map_err(io::Error::other)?)
                        .ok_or_else(|| io::Error::other("model read position overflow"))?;
                    if end > self.commit {
                        return Err(io::Error::other("record frame exceeds committed bytes"));
                    }
                    let bytes = self.bytes[offset..offset + frame_len].to_vec();
                    self.local_read = end;
                    return Ok(Some(OwnedFrame {
                        capacity: self.capacity,
                        max_payload_size: self.max_payload_size,
                        bytes,
                        end,
                    }));
                }
                Err(error) => return Err(io::Error::other(error)),
            }
        }
        Ok(None)
    }

    pub(crate) fn release_through(&mut self, end: u64) -> io::Result<()> {
        if end < self.release || end > self.local_read {
            return Err(io::Error::other("invalid model release position"));
        }
        self.release = end;
        // The releasing reader rings the writer loop's doorbell the same way.
        self.writer_doorbell = 1;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct OwnedFrame {
    capacity: DataCapacity,
    max_payload_size: NonZeroU64,
    bytes: Vec<u8>,
    pub(crate) end: u64,
}

impl OwnedFrame {
    pub(crate) fn record(&self) -> io::Result<&[u8]> {
        let Frame::Record(record) = decode_frame(self.capacity, self.max_payload_size, &self.bytes)
            .map_err(io::Error::other)?
        else {
            return Err(io::Error::other("owned record decoded as wrap"));
        };
        Ok(record)
    }
}
