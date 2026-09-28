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

//! Language-neutral format and mapped runtime for Tenon IPC Queue version 1.
//!
//! This module owns the fixed file header, framed record bytes, per-Queue
//! payload limit, append arithmetic, and the mapped reader/writer. The public
//! wait API is platform-neutral; the mapped runtime hides the operating-system
//! wait/wake backend. It does not choose a Queue role or decode an outer
//! Protobuf record. The mapped reader copies each complete frame into stable
//! reader-owned memory before decoding it.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;

use crate::little_endian::{U32_LEN, read_u32, read_u64, write_u32, write_u64};

#[allow(
    unsafe_code,
    reason = "The mmap boundary requires raw byte access and aligned atomic header words"
)]
mod mapped;

pub use mapped::{OwnedRecord, QueueObserver, ReadPosition};
pub use mapped::{
    QueueReader, QueueRuntimeError, QueueWriter, ReadOutcome, WriteOutcome, WriteReceipt,
    create_queue_file,
};
#[cfg(any(test, feature = "repository-test-support"))]
pub use mapped::{QueueWaiter, arm_bell_slot, queue_waiter_is_armed};

/// Initial and currently supported generic IPC Queue format version.
pub const FORMAT_VERSION: u32 = 1;

/// Exact byte length of the fixed file header.
pub const HEADER_LEN: usize = 192;

/// Exact byte length of every frame header, including a wrap marker.
pub const FRAME_HEADER_LEN: usize = 8;

/// Alignment shared by the data capacity, logical positions, and every frame.
pub const FRAME_ALIGNMENT: usize = 8;

const MAGIC: [u8; 8] = *b"TENONQ\0\0";
const FORMAT_VERSION_OFFSET: usize = 8;
const MAX_PAYLOAD_SIZE_OFFSET: usize = 16;
const IMMUTABLE_PADDING_START: usize = 24;
const COMMIT_OFFSET: usize = 64;
const READER_BELL_SLOT_OFFSET: usize = 72;
const READER_BELL_PADDING_START: usize = 76;
/// Header value of a doorbell slot that its owner has not published yet.
///
/// A Queue file is created before either endpoint binds, so both slot fields
/// start here. No slot index is valid, and a peer that reads this value must
/// not ring anything: the loop it would wake has not bound this Queue yet, and
/// a ring published before the binding would be lost anyway.
pub const UNBOUND_BELL_SLOT: u32 = u32::MAX;
const RELEASE_OFFSET: usize = 128;
const WRITER_BELL_SLOT_OFFSET: usize = 136;
const WRITER_BELL_PADDING_START: usize = 140;
const FRAME_HEADER_PADDING_OFFSET: usize = 4;
const HEADER_LEN_U64: u64 = 192;
const FRAME_ALIGNMENT_U64: u64 = 8;
const MIN_DATA_CAPACITY_U64: u64 = 16;
const MAX_FILE_LEN_U64: u64 = i64::MAX.unsigned_abs();

#[cfg(any(test, feature = "repository-test-support"))]
/// Offset used by repository corruption tests to edit the shared release word.
pub const TEST_RELEASE_OFFSET: u64 = RELEASE_OFFSET as u64;

/// Computes the aligned frame length for a compile-time, bounded payload.
///
/// # Panics
///
/// The caller must keep the payload and alignment additions within `usize`.
pub const fn const_aligned_frame_len(record_len: usize) -> usize {
    let unpadded = FRAME_HEADER_LEN + record_len;
    unpadded + (FRAME_ALIGNMENT - unpadded % FRAME_ALIGNMENT) % FRAME_ALIGNMENT
}

/// A stable reason why IPC Queue bytes or append arithmetic are invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FormatError {
    /// Fewer than [`HEADER_LEN`] bytes were available.
    HeaderTooShort,
    /// The header does not begin with Tenon's fixed magic bytes.
    InvalidMagic,
    /// The header declares a format version this implementation does not support.
    UnsupportedFormatVersion,
    /// The complete file length cannot represent a supported data region.
    InvalidCapacity,
    /// The per-Queue payload limit is zero, too large, or cannot fit capacity.
    InvalidMaxPayloadSize,
    /// A cache-line padding byte in the header is non-zero.
    NonZeroHeaderPadding,
    /// A logical Queue position is not frame-aligned.
    UnalignedPosition,
    /// The reader release position is ahead of the writer commit position.
    ReleaseAheadOfCommit,
    /// The committed but unreleased byte count exceeds the data capacity.
    OccupancyExceedsCapacity,
    /// Advancing a monotonic logical position would overflow `u64`.
    PositionExhausted,
    /// An empty body would be indistinguishable from a wrap marker.
    EmptyRecord,
    /// The record body exceeds the protocol or current Queue limit.
    RecordTooLarge,
    /// The caller-provided frame destination cannot hold the complete frame.
    FrameDestinationTooSmall,
    /// The frame header, body, or padding is incomplete.
    TruncatedFrame,
    /// A fixed frame-header padding byte is non-zero.
    NonZeroFrameHeaderPadding,
    /// A record frame contains non-zero alignment padding.
    NonZeroFramePadding,
}

impl FormatError {
    /// Returns the stable machine-readable code for this error.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::HeaderTooShort => "ipc.queue.header_too_short",
            Self::InvalidMagic => "ipc.queue.magic_invalid",
            Self::UnsupportedFormatVersion => "ipc.queue.format_version_unsupported",
            Self::InvalidCapacity => "ipc.queue.capacity_invalid",
            Self::InvalidMaxPayloadSize => "ipc.queue.max_payload_size_invalid",
            Self::NonZeroHeaderPadding => "ipc.queue.header_padding_nonzero",
            Self::UnalignedPosition => "ipc.queue.position_unaligned",
            Self::ReleaseAheadOfCommit => "ipc.queue.release_ahead_of_commit",
            Self::OccupancyExceedsCapacity => "ipc.queue.occupancy_exceeds_capacity",
            Self::PositionExhausted => "ipc.queue.position_exhausted",
            Self::EmptyRecord => "ipc.queue.record_empty",
            Self::RecordTooLarge => "ipc.queue.record_too_large",
            Self::FrameDestinationTooSmall => "ipc.queue.frame_destination_too_small",
            Self::TruncatedFrame => "ipc.queue.frame_truncated",
            Self::NonZeroFrameHeaderPadding => "ipc.queue.frame_header_padding_nonzero",
            Self::NonZeroFramePadding => "ipc.queue.frame_padding_nonzero",
        }
    }
}

impl fmt::Display for FormatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::HeaderTooShort => "IPC Queue header is too short",
            Self::InvalidMagic => "invalid IPC Queue magic",
            Self::UnsupportedFormatVersion => "unsupported IPC Queue format version",
            Self::InvalidCapacity => "invalid IPC Queue data capacity",
            Self::InvalidMaxPayloadSize => "invalid IPC Queue maximum payload size",
            Self::NonZeroHeaderPadding => "IPC Queue header padding bytes must be zero",
            Self::UnalignedPosition => "IPC Queue logical position is not aligned",
            Self::ReleaseAheadOfCommit => "IPC Queue release position is ahead of commit",
            Self::OccupancyExceedsCapacity => "IPC Queue occupancy exceeds data capacity",
            Self::PositionExhausted => "IPC Queue logical position is exhausted",
            Self::EmptyRecord => "IPC Queue record body must not be empty",
            Self::RecordTooLarge => "IPC Queue record body exceeds the current limit",
            Self::FrameDestinationTooSmall => "IPC Queue frame destination is too small",
            Self::TruncatedFrame => "IPC Queue frame is truncated",
            Self::NonZeroFrameHeaderPadding => "IPC Queue frame header padding must be zero",
            Self::NonZeroFramePadding => "IPC Queue frame padding must be zero",
        })
    }
}

impl Error for FormatError {}

/// A validated byte capacity for the Queue data region.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DataCapacity(u64);

impl DataCapacity {
    /// Returns the validated number of data-region bytes.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Derives and validates data-region capacity from the complete file length.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidCapacity`] when the file cannot be mapped
    /// by Tenon's supported-platform signed 64-bit file APIs, is too short, or
    /// is unaligned.
    pub fn from_file_len(file_len: u64) -> Result<Self, FormatError> {
        if file_len > MAX_FILE_LEN_U64 {
            return Err(FormatError::InvalidCapacity);
        }
        let capacity = file_len
            .checked_sub(HEADER_LEN_U64)
            .ok_or(FormatError::InvalidCapacity)?;
        Self::try_from(capacity)
    }

    /// Returns the complete file length represented by this capacity.
    #[must_use]
    pub const fn file_len(self) -> u64 {
        HEADER_LEN_U64 + self.0
    }

    const fn physical_offset(self, position: LogicalPosition) -> u64 {
        if self.0.is_power_of_two() {
            position.get() & (self.0 - 1)
        } else {
            position.get() % self.0
        }
    }
}

impl TryFrom<u64> for DataCapacity {
    type Error = FormatError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let file_len = value
            .checked_add(HEADER_LEN_U64)
            .ok_or(FormatError::InvalidCapacity)?;
        if value < MIN_DATA_CAPACITY_U64
            || !value.is_multiple_of(FRAME_ALIGNMENT_U64)
            || file_len > MAX_FILE_LEN_U64
        {
            return Err(FormatError::InvalidCapacity);
        }
        Ok(Self(value))
    }
}

/// One aligned monotonic byte position used for both commit and release.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalPosition(u64);

impl LogicalPosition {
    /// Returns the unsigned logical byte position.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    fn checked_advance(self, bytes: u64) -> Result<Self, FormatError> {
        let value = self
            .0
            .checked_add(bytes)
            .ok_or(FormatError::PositionExhausted)?;
        Self::try_from(value)
    }
}

impl TryFrom<u64> for LogicalPosition {
    type Error = FormatError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if !value.is_multiple_of(FRAME_ALIGNMENT_U64) {
            return Err(FormatError::UnalignedPosition);
        }
        Ok(Self(value))
    }
}

/// A validated version-1 IPC Queue file header snapshot.
///
/// The two doorbell fields name a slot in the Bell Region that the waiting
/// peer owns, not a word inside this Queue. `reader_bell_slot` is published by
/// the reader when it binds and is read by the writer to ring it after a
/// commit; `writer_bell_slot` is published by the writer and read by the
/// reader to ring it after a release. Neither field carries a count, a
/// readiness fact, or any recovery state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    max_payload_size: NonZeroU64,
    commit: LogicalPosition,
    reader_bell_slot: u32,
    release: LogicalPosition,
    writer_bell_slot: u32,
}

impl Header {
    /// Creates a header snapshot after validating immutable metadata and state.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError`] when the payload limit is invalid, release is
    /// ahead of commit, or occupancy exceeds capacity.
    pub fn new(
        max_payload_size: NonZeroU64,
        commit: LogicalPosition,
        reader_bell_slot: u32,
        release: LogicalPosition,
        writer_bell_slot: u32,
        capacity: DataCapacity,
    ) -> Result<Self, FormatError> {
        validate_max_payload_size(capacity, max_payload_size)?;
        validate_positions(capacity, commit, release)?;
        Ok(Self {
            max_payload_size,
            commit,
            reader_bell_slot,
            release,
            writer_bell_slot,
        })
    }

    /// Returns this Queue's maximum payload size.
    #[must_use]
    pub const fn max_payload_size(self) -> NonZeroU64 {
        self.max_payload_size
    }

    /// Returns the writer-published logical commit position.
    #[must_use]
    pub const fn commit(self) -> LogicalPosition {
        self.commit
    }

    /// Returns the reader-published logical release position.
    #[must_use]
    pub const fn release(self) -> LogicalPosition {
        self.release
    }

    /// Returns the reader-published doorbell slot of the reader loop.
    ///
    /// The value names a slot of the Bell Region the reading loop parked on, so
    /// the writer can ring exactly one loop after a commit.
    #[must_use]
    pub const fn reader_bell_slot(self) -> u32 {
        self.reader_bell_slot
    }

    /// Returns the writer-published doorbell slot of the writer loop.
    ///
    /// The value names a slot of the Bell Region the writing loop parked on, so
    /// the reader can ring exactly one loop after a release.
    #[must_use]
    pub const fn writer_bell_slot(self) -> u32 {
        self.writer_bell_slot
    }

    /// Encodes the exact 192-byte version-1 header snapshot.
    #[must_use]
    pub fn encode(self) -> [u8; HEADER_LEN] {
        let mut encoded = [0_u8; HEADER_LEN];
        encoded[..MAGIC.len()].copy_from_slice(&MAGIC);
        write_u32(&mut encoded, FORMAT_VERSION_OFFSET, FORMAT_VERSION);
        write_u64(
            &mut encoded,
            MAX_PAYLOAD_SIZE_OFFSET,
            self.max_payload_size.get(),
        );
        write_u64(&mut encoded, COMMIT_OFFSET, self.commit.get());
        write_u32(&mut encoded, READER_BELL_SLOT_OFFSET, self.reader_bell_slot);
        write_u64(&mut encoded, RELEASE_OFFSET, self.release.get());
        write_u32(&mut encoded, WRITER_BELL_SLOT_OFFSET, self.writer_bell_slot);
        encoded
    }

    /// Decodes one stable header snapshot using the actual file capacity.
    ///
    /// Bytes after the fixed header are ignored. A live mapped Queue must load
    /// its positions through the atomic runtime API instead of decoding them as
    /// ordinary bytes.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError`] when the input is truncated or any fixed field,
    /// padding byte, position, or occupancy is invalid.
    pub fn decode(capacity: DataCapacity, input: &[u8]) -> Result<Self, FormatError> {
        if input.len() < HEADER_LEN {
            return Err(FormatError::HeaderTooShort);
        }
        if input[..MAGIC.len()] != MAGIC {
            return Err(FormatError::InvalidMagic);
        }
        if read_u32(input, FORMAT_VERSION_OFFSET) != FORMAT_VERSION {
            return Err(FormatError::UnsupportedFormatVersion);
        }

        if input[FORMAT_VERSION_OFFSET + U32_LEN..MAX_PAYLOAD_SIZE_OFFSET]
            .iter()
            .chain(input[IMMUTABLE_PADDING_START..COMMIT_OFFSET].iter())
            .chain(input[READER_BELL_PADDING_START..RELEASE_OFFSET].iter())
            .chain(input[WRITER_BELL_PADDING_START..HEADER_LEN].iter())
            .any(|byte| *byte != 0)
        {
            return Err(FormatError::NonZeroHeaderPadding);
        }

        let commit = LogicalPosition::try_from(read_u64(input, COMMIT_OFFSET))?;
        let release = LogicalPosition::try_from(read_u64(input, RELEASE_OFFSET))?;
        let max_payload_size = NonZeroU64::new(read_u64(input, MAX_PAYLOAD_SIZE_OFFSET))
            .ok_or(FormatError::InvalidMaxPayloadSize)?;
        Self::new(
            max_payload_size,
            commit,
            read_u32(input, READER_BELL_SLOT_OFFSET),
            release,
            read_u32(input, WRITER_BELL_SLOT_OFFSET),
            capacity,
        )
    }
}

/// A decoded version-1 IPC Queue frame.
///
/// Record bytes borrow directly from the stable input slice. The outer decoder
/// does not allocate or decode the caller-selected Protobuf body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Frame<'a> {
    /// The reader must skip the complete physical tail and continue at offset zero.
    Wrap,
    /// One complete caller-selected record body, excluding frame padding.
    Record(&'a [u8]),
}

/// The information available by inspecting only one fixed frame header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameHeader {
    /// The reader must skip the complete physical tail.
    Wrap,
    /// The reader must copy exactly this many contiguous bytes before decoding.
    Record {
        /// Complete aligned frame length, including header and padding.
        frame_len: usize,
    },
}

/// The normal result of planning one append against one header snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendDecision {
    /// There is not enough released space; no Queue bytes may be changed.
    Full,
    /// The append can reserve the returned exact physical and logical range.
    Ready(AppendPlan),
}

/// An immutable plan for writing one record and optionally one wrap marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendPlan {
    write_offset: u64,
    frame_offset: u64,
    frame_len: usize,
    wrap_len: u64,
    next_commit: LogicalPosition,
}

impl AppendPlan {
    /// Returns the first data-region offset the writer touches.
    ///
    /// This is the frame offset without wrap and the wrap-marker offset with wrap.
    #[must_use]
    pub const fn write_offset(self) -> u64 {
        self.write_offset
    }

    /// Returns the data-region offset at which the record frame begins.
    #[must_use]
    pub const fn frame_offset(self) -> u64 {
        self.frame_offset
    }

    /// Returns the complete aligned record frame length.
    #[must_use]
    pub const fn frame_len(self) -> usize {
        self.frame_len
    }

    /// Returns zero or the complete physical tail reserved by a wrap marker.
    #[must_use]
    pub const fn wrap_len(self) -> u64 {
        self.wrap_len
    }

    /// Returns the commit position published after all planned bytes are written.
    #[must_use]
    pub const fn next_commit(self) -> LogicalPosition {
        self.next_commit
    }
}

/// Computes the only legal placement for one record without changing Queue state.
///
/// [`AppendDecision::Full`] is a normal result and guarantees that the caller
/// has not been given any writable range. A ready plan reserves the entire
/// physical tail when a wrap marker is needed, not merely its eight-byte marker.
///
/// # Errors
///
/// Returns [`FormatError`] for an invalid record, inconsistent positions, or a
/// logical position too close to `u64::MAX` to advance without wrapping.
pub fn plan_append(
    capacity: DataCapacity,
    max_payload_size: NonZeroU64,
    commit: LogicalPosition,
    release: LogicalPosition,
    record_len: usize,
) -> Result<AppendDecision, FormatError> {
    validate_positions(capacity, commit, release)?;
    let frame_len = record_frame_len(capacity, max_payload_size, record_len)?;
    let frame_len_u64 = u64::try_from(frame_len).map_err(|_| FormatError::RecordTooLarge)?;
    let used = commit
        .get()
        .checked_sub(release.get())
        .ok_or(FormatError::ReleaseAheadOfCommit)?;
    let free = capacity
        .get()
        .checked_sub(used)
        .ok_or(FormatError::OccupancyExceedsCapacity)?;
    let physical_commit = capacity.physical_offset(commit);
    let tail_len = capacity.get() - physical_commit;
    let wrap_len = if tail_len < frame_len_u64 {
        tail_len
    } else {
        0
    };
    // A complete frame fits a signed 32-bit array length and wrap is smaller.
    let required = wrap_len + frame_len_u64;
    if required > free {
        return Ok(AppendDecision::Full);
    }
    let next_commit = commit.checked_advance(required)?;
    Ok(AppendDecision::Ready(AppendPlan {
        write_offset: physical_commit,
        frame_offset: if wrap_len == 0 { physical_commit } else { 0 },
        frame_len,
        wrap_len,
        next_commit,
    }))
}

/// Returns the aligned frame length required for a record body.
///
/// # Errors
///
/// Returns [`FormatError::EmptyRecord`] for zero bytes and
/// [`FormatError::RecordTooLarge`] when the body exceeds this Queue's
/// `maxPayloadSize`.
pub fn record_frame_len(
    capacity: DataCapacity,
    max_payload_size: NonZeroU64,
    record_len: usize,
) -> Result<usize, FormatError> {
    validate_max_payload_size(capacity, max_payload_size)?;
    if record_len == 0 {
        return Err(FormatError::EmptyRecord);
    }
    if u64::try_from(record_len).map_err(|_| FormatError::RecordTooLarge)? > max_payload_size.get()
    {
        return Err(FormatError::RecordTooLarge);
    }
    aligned_frame_len(record_len).ok_or(FormatError::RecordTooLarge)
}

/// Encodes one record frame into caller-owned memory without allocating.
///
/// # Errors
///
/// Returns [`FormatError`] when `record` violates the effective size boundary
/// or `destination` cannot hold the complete aligned frame.
pub fn encode_record_frame(
    capacity: DataCapacity,
    max_payload_size: NonZeroU64,
    record: &[u8],
    destination: &mut [u8],
) -> Result<usize, FormatError> {
    let frame_len = record_frame_len(capacity, max_payload_size, record.len())?;
    if destination.len() < frame_len {
        return Err(FormatError::FrameDestinationTooSmall);
    }

    let record_len = u32::try_from(record.len()).map_err(|_| FormatError::RecordTooLarge)?;
    destination[..frame_len].fill(0);
    write_u32(destination, 0, record_len);
    destination[FRAME_HEADER_LEN..FRAME_HEADER_LEN + record.len()].copy_from_slice(record);
    Ok(frame_len)
}

/// Encodes the fixed eight-byte wrap marker into caller-owned memory.
///
/// # Errors
///
/// Returns [`FormatError::FrameDestinationTooSmall`] when fewer than
/// [`FRAME_HEADER_LEN`] bytes are available.
pub fn encode_wrap_frame(destination: &mut [u8]) -> Result<usize, FormatError> {
    if destination.len() < FRAME_HEADER_LEN {
        return Err(FormatError::FrameDestinationTooSmall);
    }
    destination[..FRAME_HEADER_LEN].fill(0);
    Ok(FRAME_HEADER_LEN)
}

/// Inspects only the fixed frame header without reading or borrowing the body.
///
/// A mapped reader uses this result to copy one exact frame into reader-owned
/// memory. It must also prove that the returned frame end does not exceed the
/// acquire-loaded commit position before copying.
///
/// # Errors
///
/// Returns [`FormatError`] when the fixed header is truncated, its padding is
/// non-zero, or the declared record length exceeds the Queue limit.
pub fn inspect_frame_header(
    capacity: DataCapacity,
    max_payload_size: NonZeroU64,
    input: &[u8],
) -> Result<FrameHeader, FormatError> {
    if input.len() < FRAME_HEADER_LEN {
        return Err(FormatError::TruncatedFrame);
    }
    if input[FRAME_HEADER_PADDING_OFFSET..FRAME_HEADER_LEN]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(FormatError::NonZeroFrameHeaderPadding);
    }

    let record_len =
        usize::try_from(read_u32(input, 0)).map_err(|_| FormatError::RecordTooLarge)?;
    if record_len == 0 {
        return Ok(FrameHeader::Wrap);
    }
    Ok(FrameHeader::Record {
        frame_len: record_frame_len(capacity, max_payload_size, record_len)?,
    })
}

/// Decodes one complete frame from stable reader-owned bytes without allocating.
///
/// Bytes after the decoded frame are ignored. For [`Frame::Wrap`], the caller
/// owns the logical position and therefore skips the entire physical tail.
///
/// # Errors
///
/// Returns [`FormatError`] when the frame is truncated, oversized, or contains
/// non-zero header or alignment padding bytes.
pub fn decode_frame(
    capacity: DataCapacity,
    max_payload_size: NonZeroU64,
    input: &[u8],
) -> Result<Frame<'_>, FormatError> {
    let FrameHeader::Record { frame_len } =
        inspect_frame_header(capacity, max_payload_size, input)?
    else {
        return Ok(Frame::Wrap);
    };
    if input.len() < frame_len {
        return Err(FormatError::TruncatedFrame);
    }
    let record_len =
        usize::try_from(read_u32(input, 0)).map_err(|_| FormatError::RecordTooLarge)?;
    let record_end = FRAME_HEADER_LEN + record_len;
    if input[record_end..frame_len].iter().any(|byte| *byte != 0) {
        return Err(FormatError::NonZeroFramePadding);
    }
    Ok(Frame::Record(&input[FRAME_HEADER_LEN..record_end]))
}

fn validate_max_payload_size(
    capacity: DataCapacity,
    max_payload_size: NonZeroU64,
) -> Result<(), FormatError> {
    if maximum_frame_len(max_payload_size)? as u64 > capacity.get() {
        return Err(FormatError::InvalidMaxPayloadSize);
    }
    Ok(())
}

fn validate_positions(
    capacity: DataCapacity,
    commit: LogicalPosition,
    release: LogicalPosition,
) -> Result<(), FormatError> {
    let occupied = commit
        .get()
        .checked_sub(release.get())
        .ok_or(FormatError::ReleaseAheadOfCommit)?;
    if occupied > capacity.get() {
        return Err(FormatError::OccupancyExceedsCapacity);
    }
    Ok(())
}

/// Reserves one maximum frame per pending record plus one frame for wrapping.
///
/// `maximum_frame_len` is an aligned length returned by [`maximum_frame_len`].
///
/// # Errors
///
/// Returns [`FormatError::InvalidCapacity`] if arithmetic overflows or the
/// resulting byte capacity cannot represent a valid Queue file.
pub fn capacity_for_record_limit(
    record_limit: NonZeroU64,
    maximum_frame_len: usize,
) -> Result<DataCapacity, FormatError> {
    let frame_count = record_limit
        .get()
        .checked_add(1)
        .ok_or(FormatError::InvalidCapacity)?;
    let maximum_frame_len =
        u64::try_from(maximum_frame_len).map_err(|_| FormatError::InvalidCapacity)?;
    let bytes = frame_count
        .checked_mul(maximum_frame_len)
        .ok_or(FormatError::InvalidCapacity)?;
    DataCapacity::try_from(bytes)
}

/// Returns the aligned frame length for a Queue's largest allowed payload.
///
/// # Errors
///
/// Returns [`FormatError::InvalidMaxPayloadSize`] if the complete frame cannot
/// be represented by every supported language implementation.
pub fn maximum_frame_len(max_payload_size: NonZeroU64) -> Result<usize, FormatError> {
    let payload_size =
        usize::try_from(max_payload_size.get()).map_err(|_| FormatError::InvalidMaxPayloadSize)?;
    let frame_len = aligned_frame_len(payload_size).ok_or(FormatError::InvalidMaxPayloadSize)?;
    // The official SDK copies a complete frame into a Java byte array.
    i32::try_from(frame_len).map_err(|_| FormatError::InvalidMaxPayloadSize)?;
    Ok(frame_len)
}

fn aligned_frame_len(record_len: usize) -> Option<usize> {
    let unpadded = FRAME_HEADER_LEN.checked_add(record_len)?;
    unpadded.checked_add(padding_len(unpadded))
}

const fn padding_len(unpadded_len: usize) -> usize {
    (FRAME_ALIGNMENT - unpadded_len % FRAME_ALIGNMENT) % FRAME_ALIGNMENT
}

/// Queue endpoint constructors for repository contract and interoperability tests.
#[cfg(any(test, feature = "repository-test-support"))]
pub mod contract_test_support {
    use std::io;
    use std::path::Path;
    use std::sync::Arc;

    use crate::bell::{BellRegion, LoopBell};

    use super::{QueueReader, QueueWriter};

    /// Opens one Queue writer bound to the doorbell `slot` of its own loop.
    ///
    /// A repository test that plays one side of a Queue opens the same two files
    /// the real process does: the Queue, and the Bell Region holding that loop's
    /// doorbell. `peer_region_path` is the Bell Region of the loop on the other
    /// side of this Queue, which a commit rings.
    pub fn open_writer(
        queue_path: &Path,
        own_region_path: &Path,
        slot: u32,
        peer_region_path: &Path,
    ) -> io::Result<QueueWriter> {
        QueueWriter::open(
            queue_path,
            own_loop_bell(own_region_path, slot)?,
            peer_region(peer_region_path)?,
        )
        .map_err(io::Error::other)
    }

    /// Opens one Queue reader bound to the doorbell `slot` of its own loop.
    ///
    /// This is the reading half of [`open_writer`]: `peer_region_path` is the
    /// Bell Region of the writer's loop, which a release rings.
    pub fn open_reader(
        queue_path: &Path,
        own_region_path: &Path,
        slot: u32,
        peer_region_path: &Path,
    ) -> io::Result<QueueReader> {
        QueueReader::open(
            queue_path,
            own_loop_bell(own_region_path, slot)?,
            peer_region(peer_region_path)?,
        )
        .map_err(io::Error::other)
    }

    fn own_loop_bell(region_path: &Path, slot: u32) -> io::Result<Arc<LoopBell>> {
        let region = BellRegion::open(region_path).map_err(io::Error::other)?;
        region.loop_bell(slot).map_err(io::Error::other)
    }

    fn peer_region(region_path: &Path) -> io::Result<Arc<BellRegion>> {
        BellRegion::open(region_path).map_err(io::Error::other)
    }
}
