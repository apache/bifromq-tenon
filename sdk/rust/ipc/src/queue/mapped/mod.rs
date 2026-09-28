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

//! Safe access to one memory-mapped IPC Queue.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::mem::MaybeUninit;
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering, fence};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use memmap2::MmapRaw;

use super::{
    COMMIT_OFFSET, DataCapacity, FRAME_HEADER_LEN, FormatError, Frame, FrameHeader, HEADER_LEN,
    Header, LogicalPosition, READER_BELL_PADDING_START, READER_BELL_SLOT_OFFSET, RELEASE_OFFSET,
    UNBOUND_BELL_SLOT, WRITER_BELL_PADDING_START, WRITER_BELL_SLOT_OFFSET, decode_frame,
    inspect_frame_header, plan_append, validate_positions, write_u32, write_u64,
};
#[cfg(any(test, feature = "repository-test-support"))]
use crate::bell::SLOT_ARMED;
use crate::bell::mapping::{
    copy_from_mapping, copy_from_mapping_uninit, copy_nonoverlapping_bytes, load_atomic_u32,
    load_atomic_u64, store_atomic_u32, store_atomic_u64,
};
use crate::bell::wait::{
    SignalWaitOutcome, WaitArmOutcome, WaitFront, WaitInterruption, wait_indefinitely,
};
use crate::bell::{
    BellError, BellInterrupter, BellRegion, LoopBell, SLOT_SNAPSHOT_ORDERING, WaitOutcome,
};

#[cfg(not(target_has_atomic = "64"))]
compile_error!("Tenon IPC Queue requires native 64-bit atomics");
#[cfg(not(target_endian = "little"))]
compile_error!("Tenon IPC Queue requires a little-endian target");

const ZERO_FRAME_PADDING: [u8; FRAME_HEADER_LEN - 1] = [0; FRAME_HEADER_LEN - 1];
const POSITION_LOAD_ORDERING: Ordering = Ordering::Acquire;
const POSITION_STORE_ORDERING: Ordering = Ordering::Release;

/// Reports one operating-system failure through this module's error vocabulary.
fn io_error(operation: &'static str, source: io::Error) -> QueueRuntimeError {
    QueueRuntimeError::Io { operation, source }
}

/// Load ordering for a peer's published doorbell slot index.
///
/// The load must observe the peer's release store whenever the peer armed
/// before this ring began; see the doorbell publication rule in the
/// IPC Queue contract.
const BELL_SLOT_LOAD_ORDERING: Ordering = Ordering::Acquire;
/// Store ordering used when a waiting endpoint publishes its own slot index.
const BELL_SLOT_STORE_ORDERING: Ordering = Ordering::Release;

/// A stable failure while creating, opening, reading, writing, or releasing a mapped Queue.
#[derive(Debug)]
#[non_exhaustive]
pub enum QueueRuntimeError {
    /// An operating-system file or mmap operation failed.
    Io {
        /// Stable operation name that failed.
        operation: &'static str,
        /// Original operating-system error.
        source: io::Error,
    },
    /// Queue bytes or logical state violate the version-1 format.
    Format(FormatError),
    /// A Bell Region mapping or doorbell operation failed.
    Bell(BellError),
    /// A reader tried to release more records than it has read and not released.
    InvalidReleaseCount,
    /// The writer commit position moved behind this reader's local position.
    CommitRegressed,
    /// A frame changed between fixed-header inspection and the owned frame copy.
    FrameChangedDuringRead,
    /// A checked byte range fell outside the mapped Queue.
    MappingRangeInvalid,
    /// A write receipt was used with a different opened writer.
    WriteReceiptOwnerMismatch,
}

impl fmt::Display for QueueRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { operation, source } => {
                write!(formatter, "IPC Queue {operation} failed: {source}")
            }
            Self::Format(error) => error.fmt(formatter),
            Self::Bell(error) => error.fmt(formatter),
            Self::InvalidReleaseCount => {
                formatter.write_str("IPC Queue release count exceeds pending records")
            }
            Self::CommitRegressed => {
                formatter.write_str("IPC Queue commit position moved behind the local reader")
            }
            Self::FrameChangedDuringRead => {
                formatter.write_str("IPC Queue frame changed while the reader copied owned bytes")
            }
            Self::MappingRangeInvalid => {
                formatter.write_str("IPC Queue mapped byte range is invalid")
            }
            Self::WriteReceiptOwnerMismatch => {
                formatter.write_str("IPC Queue write receipt belongs to another writer")
            }
        }
    }
}

impl Error for QueueRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Format(error) => Some(error),
            Self::Bell(error) => Some(error),
            Self::InvalidReleaseCount
            | Self::CommitRegressed
            | Self::FrameChangedDuringRead
            | Self::MappingRangeInvalid
            | Self::WriteReceiptOwnerMismatch => None,
        }
    }
}

impl From<FormatError> for QueueRuntimeError {
    fn from(error: FormatError) -> Self {
        Self::Format(error)
    }
}

impl From<BellError> for QueueRuntimeError {
    fn from(error: BellError) -> Self {
        Self::Bell(error)
    }
}

impl From<QueueRuntimeError> for io::Error {
    fn from(error: QueueRuntimeError) -> Self {
        io::Error::other(error)
    }
}

/// The normal result of one nonblocking writer attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The complete record is visible to the reader at the returned boundary.
    Committed(WriteReceipt),
    /// Released capacity cannot hold this record and no Queue byte changed.
    Full,
}

/// An opaque boundary returned after one complete record is committed.
///
/// A receipt must only be queried through the exact [`QueueWriter`] that
/// produced it; a private process-local identity enforces that association.
/// The receipt owns no Queue resource and does not itself mean that the reader
/// has released the record.
#[derive(Debug, Clone)]
pub struct WriteReceipt {
    exclusive_end: LogicalPosition,
    writer_identity: Arc<WriterIdentity>,
}

impl PartialEq for WriteReceipt {
    fn eq(&self, other: &Self) -> bool {
        self.exclusive_end == other.exclusive_end
            && Arc::ptr_eq(&self.writer_identity, &other.writer_identity)
    }
}

impl Eq for WriteReceipt {}

#[derive(Debug)]
struct WriterIdentity;

/// The normal result of one nonblocking reader attempt.
#[derive(Debug, PartialEq, Eq)]
pub enum ReadOutcome {
    /// No committed record is currently available.
    Empty,
    /// One complete record copied into reader-owned memory.
    Record(OwnedRecord),
}

/// One complete reader-owned Queue record.
///
/// The payload view keeps the complete validated frame allocation alive, and no
/// reference into the mmap can escape.
#[derive(Debug, PartialEq, Eq)]
pub struct OwnedRecord {
    payload: Bytes,
}

impl OwnedRecord {
    /// Returns the record payload without its fixed frame header or alignment padding.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Consumes this record and returns its payload without copying its backing bytes.
    #[must_use]
    pub fn into_payload_bytes(self) -> Bytes {
        self.payload
    }
}

/// Creates and initializes one new Queue file without replacing an existing path.
///
/// The file contains an exact version-1 header with zero positions and both
/// doorbell slots unbound, followed by a zero-filled data region. Each waiting
/// endpoint publishes its own Bell Region slot before it can arm that doorbell,
/// so a peer that rings an unbound slot has nothing to wake.
///
/// # Errors
///
/// Returns [`QueueRuntimeError::Format`] for invalid capacity or payload limits,
/// or [`QueueRuntimeError::Io`] when the file cannot be created, sized, or initialized.
pub fn create_queue_file(
    path: impl AsRef<Path>,
    capacity: DataCapacity,
    max_payload_size: NonZeroU64,
) -> Result<(), QueueRuntimeError> {
    let zero = LogicalPosition::try_from(0)?;
    let header = Header::new(
        max_payload_size,
        zero,
        UNBOUND_BELL_SLOT,
        zero,
        UNBOUND_BELL_SLOT,
        capacity,
    )?;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| io_error("create", source))?;
    file.set_len(capacity.file_len())
        .map_err(|source| io_error("resize", source))?;
    file.write_all(&header.encode())
        .map_err(|source| io_error("initialize header", source))?;
    Ok(())
}

/// A bounded read-only view; the endpoint remains the mapping's lifetime owner.
#[derive(Debug)]
pub struct QueueObserver(Weak<MmapRaw>);

impl QueueObserver {
    /// Returns used and total data bytes, skipping a racing or retired mapping.
    #[allow(
        clippy::expect_used,
        reason = "only validated endpoint mappings create observers"
    )]
    pub fn sample(&self) -> Option<(u64, u64)> {
        let mapping = self.0.upgrade()?;
        let capacity = DataCapacity::from_file_len(mapping.len() as u64)
            .expect("a live endpoint has validated mapping capacity");
        sample_usage(
            capacity,
            || load_atomic_u64(&mapping, RELEASE_OFFSET, POSITION_LOAD_ORDERING),
            || load_atomic_u64(&mapping, COMMIT_OFFSET, POSITION_LOAD_ORDERING),
        )
        .map(|usage| (usage, capacity.get()))
    }
}

/// The unique writer for one mapped Queue.
///
/// The process supervisor owns the cross-process SPSC uniqueness rule. This
/// value is neither cloneable nor internally shared, and every write requires
/// exclusive Rust access to it.
#[derive(Debug)]
pub struct QueueWriter {
    core: WriterCore<MappedQueueMemory>,
}

impl QueueWriter {
    /// Opens and validates an existing Queue for its unique writer.
    ///
    /// `bell` is the doorbell of the loop that waits on this writer. Opening
    /// publishes that loop's slot into the header so a releasing reader can ring
    /// it. `peer_region` is the Bell Region of the loop that reads this Queue;
    /// committing a record rings the slot the reader published there.
    ///
    /// # Errors
    ///
    /// Returns [`QueueRuntimeError`] when the file cannot be opened or mapped,
    /// or its header and live positions are invalid.
    pub fn open(
        path: impl AsRef<Path>,
        bell: Arc<LoopBell>,
        peer_region: Arc<BellRegion>,
    ) -> Result<Self, QueueRuntimeError> {
        let memory = MappedQueueMemory::open(path.as_ref(), OpenRole::Writer, bell, peer_region)?;
        Ok(Self {
            core: WriterCore::new(memory),
        })
    }

    /// Returns the data-region capacity derived from the mapped file length.
    #[must_use]
    pub fn data_capacity(&self) -> DataCapacity {
        self.core.memory.capacity()
    }

    /// Returns the Queue-specific payload limit from the validated header.
    #[must_use]
    pub fn max_payload_size(&self) -> NonZeroU64 {
        self.core.memory.max_payload_size()
    }

    /// Returns the current committed boundary owned by this writer.
    ///
    /// Callers may derive a business record identity before appending. Reading
    /// this boundary neither reserves bytes nor changes the Queue.
    ///
    /// # Errors
    ///
    /// Returns an error if the live committed position is not aligned.
    pub fn committed_position(&self) -> Result<LogicalPosition, QueueRuntimeError> {
        self.core.memory.load_commit()
    }

    /// Reports whether this exact record length fits without changing Queue bytes.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid length or corrupt live positions.
    pub fn can_write(&self, record_len: usize) -> Result<bool, QueueRuntimeError> {
        Ok(matches!(
            append_decision(&self.core.memory, record_len)?,
            super::AppendDecision::Ready(_)
        ))
    }

    /// Returns a thread-safe handle for waking the loop that waits on this writer.
    ///
    /// Every returned handle rings the loop doorbell this writer bound to. A
    /// control thread must publish its own event or lifecycle fact first.
    #[must_use]
    pub fn wait_interrupter(&self) -> BellInterrupter {
        self.core.memory.wait_interrupter()
    }

    /// Tries once to publish one non-empty record without waiting.
    ///
    /// [`WriteOutcome::Full`] is normal backpressure and changes no Queue byte.
    ///
    /// # Errors
    ///
    /// Returns [`QueueRuntimeError`] for an invalid record, corrupt live state,
    /// exhausted logical position, an internal mapped-range violation, or a
    /// notification failure. Any error is terminal for this Queue and must not
    /// be retried; a notification error can occur after `commit` is visible.
    pub fn try_write(&mut self, record: &[u8]) -> Result<WriteOutcome, QueueRuntimeError> {
        self.core.try_write(record)
    }

    /// Reports a physical commit before its potentially failing wake operation.
    ///
    /// # Errors
    ///
    /// Has the same terminal errors as [`Self::try_write`]. The callback runs
    /// after commit publication even when the subsequent wake fails.
    pub fn try_write_observed(
        &mut self,
        record: &[u8],
        committed: impl FnOnce(),
    ) -> Result<WriteOutcome, QueueRuntimeError> {
        self.core.try_write_observed(record, committed)
    }

    /// Observes this mapping without acquiring another reader or writer endpoint.
    pub fn observer(&self) -> QueueObserver {
        QueueObserver(Arc::downgrade(&self.core.memory.mapping))
    }

    /// Returns whether shared `release` has reached this committed record's end.
    ///
    /// `receipt` must have been returned by this exact opened writer. This
    /// method does not wait and does not change Queue state.
    ///
    /// # Errors
    ///
    /// Returns [`QueueRuntimeError`] when the receipt belongs to another opened
    /// writer or the live Queue positions are corrupt.
    pub fn is_released(&self, receipt: &WriteReceipt) -> Result<bool, QueueRuntimeError> {
        self.core.is_released(receipt)
    }

    /// Waits until shared `release` reaches this committed record's end.
    ///
    /// `receipt` must have been returned by this exact opened writer. This
    /// method never writes a record. A signal interruption returns control
    /// without changing Queue data or the receipt.
    ///
    /// # Errors
    ///
    /// Returns [`QueueRuntimeError`] when the receipt belongs to another opened
    /// writer, the live Queue positions are corrupt, or the selected platform
    /// wait fails.
    pub fn wait_released(
        &mut self,
        receipt: &WriteReceipt,
    ) -> Result<WaitOutcome, QueueRuntimeError> {
        self.core.wait_released(receipt)
    }

    /// Waits for the current committed prefix, including frames inherited on open.
    /// The exclusive writer cannot append during this wait. Reports each failed
    /// predicate to its owner without changing the existing interruption protocol.
    ///
    /// # Errors
    ///
    /// Returns an error for corrupt live positions or a failed platform wait.
    pub fn wait_all_released_observed(
        &mut self,
        mut blocked: impl FnMut(),
    ) -> Result<WaitOutcome, QueueRuntimeError> {
        let exclusive_end = self.core.memory.load_commit()?;
        let memory = &self.core.memory;
        wait_indefinitely(memory, || {
            let ready = receipt_released(memory, exclusive_end)?;
            if !ready {
                blocked();
            }
            Ok(ready)
        })
    }

    /// Waits until a record body of exactly `record_len` bytes can be appended.
    ///
    /// This method never writes a record. Under the required single-writer
    /// ownership, [`WaitOutcome::Ready`] remains true until this writer changes
    /// `commit`, so the caller can safely retry [`Self::try_write`] with the same
    /// length. A signal interruption returns control without changing Queue data.
    ///
    /// # Errors
    ///
    /// Returns [`QueueRuntimeError`] when the record length or live Queue state
    /// is invalid, or when the selected platform wait fails.
    pub fn wait_writable(&mut self, record_len: usize) -> Result<WaitOutcome, QueueRuntimeError> {
        self.core.wait_writable(record_len)
    }

    /// Reports capacity pressure without changing the wait or retry protocol.
    ///
    /// # Errors
    ///
    /// Has the same invalid-length, live-state and wait errors as
    /// [`Self::wait_writable`].
    pub fn wait_writable_observed(
        &mut self,
        record_len: usize,
        blocked: impl FnMut(),
    ) -> Result<WaitOutcome, QueueRuntimeError> {
        self.core.wait_writable_observed(record_len, blocked)
    }
}

/// The unique reader for one mapped Queue.
///
/// Reading advances only a private cursor. [`Self::release`] is the sole action
/// that publishes reusable capacity to the writer.
#[derive(Debug)]
pub struct QueueReader {
    core: ReaderCore<MappedQueueMemory>,
}

impl QueueReader {
    /// Observes this mapping without owning its endpoint lifetime.
    pub fn observer(&self) -> QueueObserver {
        QueueObserver(Arc::downgrade(&self.core.memory.mapping))
    }

    /// Opens and validates an existing Queue for its unique reader.
    ///
    /// `bell` is the doorbell of the loop that waits on this reader; opening
    /// publishes that loop's slot into the header so a committing writer can ring
    /// it. `peer_region` is the Bell Region of the loop that writes this Queue;
    /// releasing records rings the slot the writer published there.
    ///
    /// The private cursor starts at shared `release`, so records read but not
    /// released by a previous reader instance are replayed.
    ///
    /// # Errors
    ///
    /// Returns [`QueueRuntimeError`] when the file cannot be opened or mapped,
    /// or its header and live positions are invalid.
    pub fn open(
        path: impl AsRef<Path>,
        bell: Arc<LoopBell>,
        peer_region: Arc<BellRegion>,
    ) -> Result<Self, QueueRuntimeError> {
        let memory = MappedQueueMemory::open(path.as_ref(), OpenRole::Reader, bell, peer_region)?;
        Ok(Self {
            core: ReaderCore::new(memory),
        })
    }

    /// Returns the data-region capacity derived from the mapped file length.
    #[must_use]
    pub fn data_capacity(&self) -> DataCapacity {
        self.core.memory.capacity()
    }

    /// Returns the Queue-specific payload limit from the validated header.
    #[must_use]
    pub fn max_payload_size(&self) -> NonZeroU64 {
        self.core.memory.max_payload_size()
    }

    /// Returns a thread-safe handle for waking the loop that waits on this reader.
    ///
    /// Every returned handle rings the loop doorbell this reader bound to. A
    /// control thread must publish its own event or lifecycle fact first.
    #[must_use]
    pub fn wait_interrupter(&self) -> BellInterrupter {
        self.core.memory.wait_interrupter()
    }

    /// Tries once to copy and validate the next committed record without waiting.
    ///
    /// A wrap marker is consumed internally and never appears as a record.
    ///
    /// # Errors
    ///
    /// Returns [`QueueRuntimeError`] when committed state or frame bytes are corrupt.
    pub fn try_read(&mut self) -> Result<ReadOutcome, QueueRuntimeError> {
        self.core.try_read()
    }

    /// Waits until at least one committed record is available to this reader.
    ///
    /// This method never copies or releases a record. Under the required
    /// single-reader ownership, [`WaitOutcome::Ready`] remains true until this
    /// reader calls [`Self::try_read`]. A signal interruption returns control
    /// without changing Queue data.
    ///
    /// # Errors
    ///
    /// Returns [`QueueRuntimeError`] when live Queue state is invalid, or when
    /// the selected platform wait fails.
    pub fn wait_readable(&mut self) -> Result<WaitOutcome, QueueRuntimeError> {
        self.core.wait_readable()
    }

    /// Reports whether a committed record is waiting for this reader.
    ///
    /// This is the condition of [`Self::wait_readable`] exposed without waiting,
    /// so an event loop can test it inside a larger condition set of its own.
    /// The probe never reads, releases, or changes a Queue record.
    ///
    /// # Errors
    ///
    /// Returns [`QueueRuntimeError`] when live Queue state is invalid.
    pub fn has_unread(&self) -> Result<bool, QueueRuntimeError> {
        self.core.has_unread()
    }

    /// Captures the end of the records this reader has copied so far.
    ///
    /// Batch owners keep this opaque boundary while later batches are pending.
    /// It is valid only for this open reader and never owns the mapped bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the shared released position is not aligned.
    pub fn checkpoint(&self) -> Result<ReadPosition, QueueRuntimeError> {
        Ok(ReadPosition(match self.core.release_boundaries.back() {
            Some(position) => *position,
            None => self.core.memory.load_release()?,
        }))
    }

    /// Releases the copied prefix ending at this reader's batch boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when the boundary is not pending in this reader or when
    /// publishing release/notification fails. Notification failure is terminal.
    pub fn release_through(&mut self, position: ReadPosition) -> Result<(), QueueRuntimeError> {
        let count = self
            .core
            .release_boundaries
            .iter()
            .position(|pending| *pending == position.0)
            .ok_or(QueueRuntimeError::InvalidReleaseCount)?
            + 1;
        self.core.release(count)
    }

    /// Releases the oldest `count` records as one continuous prefix.
    ///
    /// `count == 0` is a no-op. Reader-owned [`OwnedRecord`] values remain valid
    /// after release because they do not borrow the mmap.
    ///
    /// # Errors
    ///
    /// Returns [`QueueRuntimeError::InvalidReleaseCount`] when `count` exceeds
    /// the number of records returned by [`Self::try_read`] and not yet released.
    /// Any other error is terminal for this Queue and must not be retried; a
    /// notification error can occur after `release` is visible and local release
    /// bookkeeping has advanced.
    pub fn release(&mut self, count: usize) -> Result<(), QueueRuntimeError> {
        self.core.release(count)
    }
}

/// An opaque copied-record boundary belonging to one open reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReadPosition(LogicalPosition);

/// One bound Queue endpoint: shared Queue bytes plus the loop doorbell it waits on.
///
/// Every wait operation acts on the doorbell of the loop that waits on this
/// endpoint; the peer loop's doorbell is reached only through [`Self::ring_peer`].
trait QueueMemory: WaitFront {
    fn capacity(&self) -> DataCapacity;
    fn max_payload_size(&self) -> NonZeroU64;
    fn load_commit(&self) -> Result<LogicalPosition, QueueRuntimeError>;
    fn publish_commit(&self, position: LogicalPosition);
    fn load_release(&self) -> Result<LogicalPosition, QueueRuntimeError>;
    fn publish_release(&self, position: LogicalPosition);
    /// Rings the doorbell of the loop that waits on the peer side of this Queue.
    fn ring_peer(&self) -> Result<(), QueueRuntimeError>;
    fn read_data(&self, offset: u64, destination: &mut [u8]) -> Result<(), QueueRuntimeError>;
    /// Returning `Ok` guarantees that every destination byte is initialized.
    fn read_data_uninit(
        &self,
        offset: u64,
        destination: &mut [MaybeUninit<u8>],
    ) -> Result<(), QueueRuntimeError>;
    fn write_data(&self, offset: u64, source: &[u8]) -> Result<(), QueueRuntimeError>;
}

#[derive(Debug)]
struct WriterCore<M> {
    memory: M,
    identity: Arc<WriterIdentity>,
}

impl<M: QueueMemory> WriterCore<M> {
    fn new(memory: M) -> Self {
        Self {
            memory,
            identity: Arc::new(WriterIdentity),
        }
    }

    fn try_write(&mut self, record: &[u8]) -> Result<WriteOutcome, QueueRuntimeError> {
        self.try_write_observed(record, || {})
    }

    fn try_write_observed(
        &mut self,
        record: &[u8],
        committed: impl FnOnce(),
    ) -> Result<WriteOutcome, QueueRuntimeError> {
        let decision = append_decision(&self.memory, record.len())?;
        let super::AppendDecision::Ready(plan) = decision else {
            return Ok(WriteOutcome::Full);
        };

        validate_write_ranges(self.memory.capacity(), plan)?;
        if plan.wrap_len() != 0 {
            self.memory
                .write_data(plan.write_offset(), &[0_u8; FRAME_HEADER_LEN])?;
        }
        write_record(&self.memory, plan.frame_offset(), plan.frame_len(), record)?;
        self.memory.publish_commit(plan.next_commit());
        committed();
        self.memory.ring_peer()?;
        Ok(WriteOutcome::Committed(WriteReceipt {
            exclusive_end: plan.next_commit(),
            writer_identity: Arc::clone(&self.identity),
        }))
    }

    fn is_released(&self, receipt: &WriteReceipt) -> Result<bool, QueueRuntimeError> {
        self.validate_receipt_owner(receipt)?;
        receipt_released(&self.memory, receipt.exclusive_end)
    }

    fn wait_released(&mut self, receipt: &WriteReceipt) -> Result<WaitOutcome, QueueRuntimeError> {
        self.validate_receipt_owner(receipt)?;
        let exclusive_end = receipt.exclusive_end;
        let memory = &self.memory;
        wait_indefinitely(memory, || receipt_released(memory, exclusive_end))
    }

    fn validate_receipt_owner(&self, receipt: &WriteReceipt) -> Result<(), QueueRuntimeError> {
        if Arc::ptr_eq(&self.identity, &receipt.writer_identity) {
            Ok(())
        } else {
            Err(QueueRuntimeError::WriteReceiptOwnerMismatch)
        }
    }

    fn wait_writable(&mut self, record_len: usize) -> Result<WaitOutcome, QueueRuntimeError> {
        self.wait_writable_observed(record_len, || {})
    }

    fn wait_writable_observed(
        &mut self,
        record_len: usize,
        mut blocked: impl FnMut(),
    ) -> Result<WaitOutcome, QueueRuntimeError> {
        let memory = &self.memory;
        wait_indefinitely(memory, || {
            let ready = matches!(
                append_decision(memory, record_len)?,
                super::AppendDecision::Ready(_)
            );
            if !ready {
                blocked();
            }
            Ok(ready)
        })
    }
}

#[derive(Debug)]
struct ReaderCore<M> {
    memory: M,
    release_boundaries: VecDeque<LogicalPosition>,
}

impl<M: QueueMemory> ReaderCore<M> {
    fn new(memory: M) -> Self {
        Self {
            memory,
            release_boundaries: VecDeque::new(),
        }
    }

    fn try_read(&mut self) -> Result<ReadOutcome, QueueRuntimeError> {
        let (commit, mut read) =
            reader_positions(&self.memory, self.release_boundaries.back().copied())?;
        if commit == read {
            return Ok(ReadOutcome::Empty);
        }

        loop {
            let physical_offset = self.memory.capacity().physical_offset(read);
            let tail_len = self.memory.capacity().get() - physical_offset;
            let mut encoded_header = [0_u8; FRAME_HEADER_LEN];
            self.memory
                .read_data(physical_offset, &mut encoded_header)?;
            match inspect_frame_header(
                self.memory.capacity(),
                self.memory.max_payload_size(),
                &encoded_header,
            )? {
                FrameHeader::Wrap => {
                    let next = read.checked_advance(tail_len)?;
                    if next >= commit {
                        return Err(FormatError::TruncatedFrame.into());
                    }
                    read = next;
                }
                FrameHeader::Record { frame_len } => {
                    let frame_len_u64 = u64::try_from(frame_len)
                        .map_err(|_| QueueRuntimeError::MappingRangeInvalid)?;
                    if frame_len_u64 > tail_len {
                        return Err(FormatError::TruncatedFrame.into());
                    }
                    let next = read.checked_advance(frame_len_u64)?;
                    if next > commit {
                        return Err(FormatError::TruncatedFrame.into());
                    }

                    let mut frame = BytesMut::with_capacity(frame_len);
                    self.memory.read_data_uninit(
                        physical_offset,
                        &mut frame.spare_capacity_mut()[..frame_len],
                    )?;
                    // SAFETY: `read_data_uninit` returned `Ok` after initializing
                    // every byte in the exact `frame_len` destination range.
                    unsafe { frame.set_len(frame_len) };
                    let payload_len = match decode_frame(
                        self.memory.capacity(),
                        self.memory.max_payload_size(),
                        &frame,
                    )? {
                        Frame::Wrap => return Err(QueueRuntimeError::FrameChangedDuringRead),
                        Frame::Record(payload) => payload.len(),
                    };
                    self.release_boundaries.push_back(next);
                    frame.advance(FRAME_HEADER_LEN);
                    frame.truncate(payload_len);
                    return Ok(ReadOutcome::Record(OwnedRecord {
                        payload: frame.freeze(),
                    }));
                }
            }
        }
    }

    fn wait_readable(&mut self) -> Result<WaitOutcome, QueueRuntimeError> {
        let local_read = self.release_boundaries.back().copied();
        let memory = &self.memory;
        wait_indefinitely(memory, || {
            reader_positions(memory, local_read).map(|(commit, read)| commit != read)
        })
    }

    fn has_unread(&self) -> Result<bool, QueueRuntimeError> {
        let local_read = self.release_boundaries.back().copied();
        reader_positions(&self.memory, local_read).map(|(commit, read)| commit != read)
    }

    fn release(&mut self, count: usize) -> Result<(), QueueRuntimeError> {
        if count == 0 {
            return Ok(());
        }
        let position = self
            .release_boundaries
            .get(count - 1)
            .copied()
            .ok_or(QueueRuntimeError::InvalidReleaseCount)?;
        self.memory.publish_release(position);
        drop(self.release_boundaries.drain(..count));
        self.memory.ring_peer()?;
        Ok(())
    }
}

fn append_decision<M: QueueMemory>(
    memory: &M,
    record_len: usize,
) -> Result<super::AppendDecision, QueueRuntimeError> {
    Ok(plan_append(
        memory.capacity(),
        memory.max_payload_size(),
        memory.load_commit()?,
        memory.load_release()?,
        record_len,
    )?)
}

fn sample_usage(
    capacity: DataCapacity,
    mut release: impl FnMut() -> u64,
    commit: impl FnOnce() -> u64,
) -> Option<u64> {
    let release_before = release();
    let commit = commit();
    let release_after = release();
    if release_before != release_after {
        return None;
    }
    let commit = LogicalPosition::try_from(commit).ok()?;
    let release = LogicalPosition::try_from(release_after).ok()?;
    validate_positions(capacity, commit, release).ok()?;
    Some(commit.get() - release.get())
}

fn receipt_released<M: QueueMemory>(
    memory: &M,
    exclusive_end: LogicalPosition,
) -> Result<bool, QueueRuntimeError> {
    let commit = memory.load_commit()?;
    let release = memory.load_release()?;
    validate_positions(memory.capacity(), commit, release)?;
    Ok(release >= exclusive_end)
}

fn reader_positions<M: QueueMemory>(
    memory: &M,
    local_read: Option<LogicalPosition>,
) -> Result<(LogicalPosition, LogicalPosition), QueueRuntimeError> {
    let release = memory.load_release()?;
    let commit = memory.load_commit()?;
    validate_positions(memory.capacity(), commit, release)?;
    let read = local_read.unwrap_or(release);
    if commit < read {
        return Err(QueueRuntimeError::CommitRegressed);
    }
    Ok((commit, read))
}

fn validate_write_ranges(
    capacity: DataCapacity,
    plan: super::AppendPlan,
) -> Result<(), QueueRuntimeError> {
    if plan.wrap_len() != 0
        && checked_data_end(plan.write_offset(), FRAME_HEADER_LEN)? > capacity.get()
    {
        return Err(QueueRuntimeError::MappingRangeInvalid);
    }
    if checked_data_end(plan.frame_offset(), plan.frame_len())? > capacity.get() {
        return Err(QueueRuntimeError::MappingRangeInvalid);
    }
    Ok(())
}

fn write_record<M: QueueMemory>(
    memory: &M,
    offset: u64,
    frame_len: usize,
    record: &[u8],
) -> Result<(), QueueRuntimeError> {
    let record_len = u32::try_from(record.len()).map_err(|_| FormatError::RecordTooLarge)?;
    let mut header = [0_u8; FRAME_HEADER_LEN];
    write_u32(&mut header, 0, record_len);
    memory.write_data(offset, &header)?;
    let body_offset = checked_data_end(offset, FRAME_HEADER_LEN)?;
    memory.write_data(body_offset, record)?;
    let body_end = FRAME_HEADER_LEN
        .checked_add(record.len())
        .ok_or(QueueRuntimeError::MappingRangeInvalid)?;
    let padding_len = frame_len
        .checked_sub(body_end)
        .ok_or(QueueRuntimeError::MappingRangeInvalid)?;
    if padding_len != 0 {
        let padding_offset = checked_data_end(offset, body_end)?;
        memory.write_data(padding_offset, &ZERO_FRAME_PADDING[..padding_len])?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenRole {
    Writer,
    Reader,
}

impl OpenRole {
    /// Returns the header field this endpoint publishes its own doorbell in.
    const fn own_bell_slot_offset(self) -> usize {
        match self {
            Self::Writer => WRITER_BELL_SLOT_OFFSET,
            Self::Reader => READER_BELL_SLOT_OFFSET,
        }
    }

    /// Returns the header field holding the doorbell of the loop on the other side.
    const fn peer_bell_slot_offset(self) -> usize {
        match self {
            Self::Writer => READER_BELL_SLOT_OFFSET,
            Self::Reader => WRITER_BELL_SLOT_OFFSET,
        }
    }
}

/// One mapped Queue endpoint bound to the doorbell of the loop that waits on it.
///
/// The endpoint publishes its own loop's slot index at open so that a
/// committing writer or a releasing reader can ring that loop. It never waits
/// on a Queue word: every wait parks on the loop doorbell and rechecks the
/// Queue positions.
#[derive(Debug)]
struct MappedQueueMemory {
    mapping: Arc<MmapRaw>,
    capacity: DataCapacity,
    max_payload_size: NonZeroU64,
    bell: Arc<LoopBell>,
    peer_region: Arc<BellRegion>,
    peer_bell_slot_offset: usize,
}

impl MappedQueueMemory {
    fn open(
        path: &Path,
        role: OpenRole,
        bell: Arc<LoopBell>,
        peer_region: Arc<BellRegion>,
    ) -> Result<Self, QueueRuntimeError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|source| io_error("open", source))?;
        let capacity = capacity_from_file(&file)?;
        let expected_len = usize::try_from(capacity.file_len())
            .map_err(|_| QueueRuntimeError::MappingRangeInvalid)?;
        let mapping = Arc::new(MmapRaw::map_raw(&file).map_err(|source| io_error("map", source))?);
        if mapping.len() != expected_len || !header_atomics_are_aligned(&mapping) {
            return Err(QueueRuntimeError::MappingRangeInvalid);
        }
        let header = decode_live_header(&mapping, capacity, role)?;
        // Publish this loop's doorbell for the peer that will ring it. The
        // fence pairs with the one in `ring_peer`; see that method for why the
        // pair, and not a single release store, makes the ring sound.
        store_atomic_u32(
            &mapping,
            role.own_bell_slot_offset(),
            bell.slot().index(),
            BELL_SLOT_STORE_ORDERING,
        );
        fence(Ordering::SeqCst);
        Ok(Self {
            mapping,
            capacity,
            max_payload_size: header.max_payload_size(),
            bell,
            peer_region,
            peer_bell_slot_offset: role.peer_bell_slot_offset(),
        })
    }

    fn wait_interrupter(&self) -> BellInterrupter {
        self.bell.interrupter()
    }

    fn data_mapping_offset(&self, offset: u64, length: usize) -> Result<usize, QueueRuntimeError> {
        let data_offset =
            usize::try_from(offset).map_err(|_| QueueRuntimeError::MappingRangeInvalid)?;
        let start = HEADER_LEN
            .checked_add(data_offset)
            .ok_or(QueueRuntimeError::MappingRangeInvalid)?;
        let end = start
            .checked_add(length)
            .ok_or(QueueRuntimeError::MappingRangeInvalid)?;
        if end > self.mapping.len() {
            return Err(QueueRuntimeError::MappingRangeInvalid);
        }
        Ok(start)
    }
}

impl WaitFront for MappedQueueMemory {
    fn take_wait_interruption(&self) -> Option<WaitInterruption> {
        self.bell.take_wait_interruption()
    }

    fn arm_or_take_wait_interruption(&self) -> Result<WaitArmOutcome, BellError> {
        self.bell.arm_or_take_wait_interruption()
    }

    fn publish_wait_notification(&self) -> Result<(), BellError> {
        self.bell.publish_wait_notification()
    }

    fn wait_bell(&self) -> Result<SignalWaitOutcome, BellError> {
        self.bell.wait_bell()
    }

    fn wait_bell_for(&self, timeout: Duration) -> Result<SignalWaitOutcome, BellError> {
        self.bell.wait_bell_for(timeout)
    }
}

impl QueueMemory for MappedQueueMemory {
    fn capacity(&self) -> DataCapacity {
        self.capacity
    }

    fn max_payload_size(&self) -> NonZeroU64 {
        self.max_payload_size
    }

    fn load_commit(&self) -> Result<LogicalPosition, QueueRuntimeError> {
        LogicalPosition::try_from(load_atomic_u64(
            &self.mapping,
            COMMIT_OFFSET,
            POSITION_LOAD_ORDERING,
        ))
        .map_err(Into::into)
    }

    fn publish_commit(&self, position: LogicalPosition) {
        store_atomic_u64(
            &self.mapping,
            COMMIT_OFFSET,
            position.get(),
            POSITION_STORE_ORDERING,
        );
    }

    fn load_release(&self) -> Result<LogicalPosition, QueueRuntimeError> {
        LogicalPosition::try_from(load_atomic_u64(
            &self.mapping,
            RELEASE_OFFSET,
            POSITION_LOAD_ORDERING,
        ))
        .map_err(Into::into)
    }

    fn publish_release(&self, position: LogicalPosition) {
        store_atomic_u64(
            &self.mapping,
            RELEASE_OFFSET,
            position.get(),
            POSITION_STORE_ORDERING,
        );
    }

    /// Rings the doorbell of the loop that waits on the peer side of this Queue.
    ///
    /// The peer publishes its slot index while this endpoint may already be
    /// ringing, so the index is re-read on every ring. Two facts make the ring
    /// sound with that extra hop:
    ///
    /// - when this ring reads the peer's index, the ring is an atomic swap on
    ///   the word the peer arms, so exactly one of "the wake reaches the peer"
    ///   and "the peer's post-arm recheck sees this fact" holds;
    /// - when this ring still reads an unbound index, this endpoint's fact is
    ///   ordered before the peer's slot publication by the sequencing fences,
    ///   so the peer's post-arm recheck sees the fact.
    ///
    /// An unbound index is therefore not a lost ring: the peer has not bound
    /// this Queue yet, so no loop is parked on its behalf and its first recheck
    /// runs after the binding.
    fn ring_peer(&self) -> Result<(), QueueRuntimeError> {
        fence(Ordering::SeqCst);
        let slot_index = load_atomic_u32(
            &self.mapping,
            self.peer_bell_slot_offset,
            BELL_SLOT_LOAD_ORDERING,
        );
        if slot_index == UNBOUND_BELL_SLOT {
            return Ok(());
        }
        Ok(self.peer_region.slot(slot_index)?.ring()?)
    }

    fn read_data(&self, offset: u64, destination: &mut [u8]) -> Result<(), QueueRuntimeError> {
        let start = self.data_mapping_offset(offset, destination.len())?;
        copy_from_mapping(&self.mapping, start, destination);
        Ok(())
    }

    fn read_data_uninit(
        &self,
        offset: u64,
        destination: &mut [MaybeUninit<u8>],
    ) -> Result<(), QueueRuntimeError> {
        let start = self.data_mapping_offset(offset, destination.len())?;
        copy_from_mapping_uninit(&self.mapping, start, destination);
        Ok(())
    }

    fn write_data(&self, offset: u64, source: &[u8]) -> Result<(), QueueRuntimeError> {
        let start = self.data_mapping_offset(offset, source.len())?;
        copy_to_mapping(&self.mapping, start, source);
        Ok(())
    }
}

fn capacity_from_file(file: &File) -> Result<DataCapacity, QueueRuntimeError> {
    let file_len = file
        .metadata()
        .map_err(|source| io_error("read metadata", source))?
        .len();
    DataCapacity::from_file_len(file_len).map_err(Into::into)
}

fn decode_live_header(
    mapping: &MmapRaw,
    capacity: DataCapacity,
    role: OpenRole,
) -> Result<Header, QueueRuntimeError> {
    let mut snapshot = [0_u8; HEADER_LEN];
    copy_from_mapping(mapping, 0, &mut snapshot[..READER_BELL_SLOT_OFFSET]);
    copy_from_mapping(
        mapping,
        READER_BELL_PADDING_START,
        &mut snapshot[READER_BELL_PADDING_START..RELEASE_OFFSET],
    );
    copy_from_mapping(
        mapping,
        WRITER_BELL_PADDING_START,
        &mut snapshot[WRITER_BELL_PADDING_START..],
    );

    let (commit, release) = match role {
        OpenRole::Writer => {
            // The previous writer is gone, so commit stays fixed while the
            // reader-owned release may advance during this snapshot.
            let commit = load_atomic_u64(mapping, COMMIT_OFFSET, POSITION_LOAD_ORDERING);
            let release = load_atomic_u64(mapping, RELEASE_OFFSET, POSITION_LOAD_ORDERING);
            (commit, release)
        }
        OpenRole::Reader => {
            // The previous reader is gone, so release stays fixed while the
            // writer-owned commit may advance during this snapshot.
            let release = load_atomic_u64(mapping, RELEASE_OFFSET, POSITION_LOAD_ORDERING);
            let commit = load_atomic_u64(mapping, COMMIT_OFFSET, POSITION_LOAD_ORDERING);
            (commit, release)
        }
    };
    write_u64(&mut snapshot, COMMIT_OFFSET, commit);
    write_u64(&mut snapshot, RELEASE_OFFSET, release);
    // Both doorbell fields belong to the peer endpoint and may be published
    // while this snapshot is taken, so they are loaded as atomics.
    for offset in [READER_BELL_SLOT_OFFSET, WRITER_BELL_SLOT_OFFSET] {
        write_u32(
            &mut snapshot,
            offset,
            load_atomic_u32(mapping, offset, SLOT_SNAPSHOT_ORDERING),
        );
    }
    Header::decode(capacity, &snapshot).map_err(Into::into)
}

fn checked_data_end(offset: u64, length: usize) -> Result<u64, QueueRuntimeError> {
    offset
        .checked_add(u64::try_from(length).map_err(|_| QueueRuntimeError::MappingRangeInvalid)?)
        .ok_or(QueueRuntimeError::MappingRangeInvalid)
}

fn header_atomics_are_aligned(mapping: &MmapRaw) -> bool {
    mapping
        .as_ptr()
        .wrapping_add(COMMIT_OFFSET)
        .cast::<AtomicU64>()
        .is_aligned()
        && mapping
            .as_ptr()
            .wrapping_add(RELEASE_OFFSET)
            .cast::<AtomicU64>()
            .is_aligned()
        && mapping
            .as_ptr()
            .wrapping_add(READER_BELL_SLOT_OFFSET)
            .cast::<AtomicU32>()
            .is_aligned()
        && mapping
            .as_ptr()
            .wrapping_add(WRITER_BELL_SLOT_OFFSET)
            .cast::<AtomicU32>()
            .is_aligned()
}

fn copy_to_mapping(mapping: &MmapRaw, offset: usize, source: &[u8]) {
    // SAFETY: Every caller checks that `offset..offset + source.len()` is inside
    // this live mapping. The pure append plan restricts writes to released
    // capacity owned by the unique writer, and commit is published only after
    // these copies complete. This module never exposes the destination pointer;
    // direct external mutation of Runner-private Queue files is outside the API,
    // so the source and destination cannot overlap.
    unsafe {
        copy_nonoverlapping_bytes(
            source.as_ptr(),
            mapping.as_mut_ptr().add(offset),
            source.len(),
        );
    }
}

/// Which waiting endpoint published the doorbell slot inside one Queue header.
#[cfg(any(test, feature = "repository-test-support"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueWaiter {
    /// The endpoint that reads this Queue and waits for committed records.
    Reader,
    /// The endpoint that writes this Queue and waits for released space.
    Writer,
}

/// Reports whether one Queue endpoint's waiting loop is parked on its doorbell.
///
/// Both halves of the answer live in files: the endpoint publishes its own Bell
/// Region slot into the Queue header when it opens, and the slot word itself
/// says whether that loop is parked. A peer process therefore observes the same
/// two facts a cross-process wake depends on. Repository tests use this to
/// reach the same state a real peer reaches.
///
/// # Errors
///
/// Returns [`QueueRuntimeError`] when either file cannot be read.
#[cfg(any(test, feature = "repository-test-support"))]
pub fn queue_waiter_is_armed(
    queue_path: &Path,
    region_path: &Path,
    waiter: QueueWaiter,
) -> Result<bool, QueueRuntimeError> {
    let slot_offset = match waiter {
        QueueWaiter::Reader => READER_BELL_SLOT_OFFSET,
        QueueWaiter::Writer => WRITER_BELL_SLOT_OFFSET,
    };
    let slot = u32::from_le_bytes(read_word_at(queue_path, slot_offset as u64)?);
    if slot == UNBOUND_BELL_SLOT {
        return Ok(false);
    }
    let offset = crate::bell::slot_offset(slot) as u64;
    Ok(u32::from_le_bytes(read_word_at(region_path, offset)?) == SLOT_ARMED)
}

#[cfg(any(test, feature = "repository-test-support"))]
fn read_word_at(path: &Path, offset: u64) -> Result<[u8; 4], QueueRuntimeError> {
    use std::fs::File;
    use std::os::unix::fs::FileExt;

    let file = File::open(path).map_err(|source| io_error("read word", source))?;
    let mut word = [0_u8; 4];
    file.read_exact_at(&mut word, offset)
        .map_err(|source| io_error("read word", source))?;
    Ok(word)
}

/// Arms one Bell Region slot the way a parked waiting loop leaves it.
///
/// A ring publishes a notification into a slot and wakes the slot owner only
/// when the slot was armed, so a repository test that must observe whether a
/// peer's ring landed first puts the slot into the state a parked loop leaves
/// it in. It runs the region's own arm operation, so the test sets up exactly
/// the state the loop sets up rather than a second write of the same word.
///
/// # Errors
///
/// Returns [`QueueRuntimeError`] when the region cannot be opened, its slot
/// state is invalid, or `slot` is at or beyond the region's slot count.
#[cfg(any(test, feature = "repository-test-support"))]
pub fn arm_bell_slot(region_path: &Path, slot: u32) -> Result<(), QueueRuntimeError> {
    Ok(BellRegion::open(region_path)?.slot(slot)?.arm()?)
}

#[cfg(test)]
mod tests {
    mod crash;

    use std::num::NonZeroU32;

    use loom::cell::UnsafeCell;
    use loom::sync::Arc;
    use loom::sync::atomic::AtomicBool;
    use loom::sync::atomic::{AtomicU32 as LoomAtomicU32, AtomicU64 as LoomAtomicU64, AtomicUsize};
    use loom::sync::{Condvar, Mutex};
    use loom::thread;

    use super::*;
    use crate::bell::mapping::{
        copy_nonoverlapping_bytes, load_atomic_u32_pointer, load_atomic_u64_pointer,
        store_atomic_u64_pointer, swap_atomic_u32_pointer,
    };
    use crate::bell::wait::{PlatformWait, WaitLoopOutcome, wait_with_next_platform_wait};
    use crate::bell::{
        BELL_FORMAT_VERSION, BELL_MAGIC, SLOT_ARM_ORDERING, SLOT_NOTIFIED, SLOT_NOTIFY_ORDERING,
        TimedWaitOutcome, bell_region_len, create_bell_region, read_bell_region_epoch,
    };
    use crate::queue::{read_u32, read_u64, write_u32};

    /// Creates one Queue with one endpoint per side and one doorbell each.
    ///
    /// Both endpoints map the same Bell Region: the writer binds slot zero and
    /// the reader slot one, so each end rings the slot the other published.
    fn endpoint_pair(
        directory: &Path,
        name: &str,
        capacity: u64,
        max_payload_size: u64,
    ) -> io::Result<(QueueWriter, QueueReader, std::sync::Arc<BellRegion>)> {
        let path = directory.join(name);
        let max_payload_size =
            NonZeroU64::new(max_payload_size).ok_or_else(|| io::Error::other("positive limit"))?;
        create_queue_file(
            &path,
            DataCapacity::try_from(capacity).map_err(io::Error::other)?,
            max_payload_size,
        )
        .map_err(io::Error::other)?;
        let region = bell_region(directory, 2)?;
        let writer = QueueWriter::open(
            &path,
            LoopBell::new(region.slot(0).map_err(io::Error::other)?),
            std::sync::Arc::clone(&region),
        )
        .map_err(io::Error::other)?;
        let reader = QueueReader::open(
            &path,
            LoopBell::new(region.slot(1).map_err(io::Error::other)?),
            std::sync::Arc::clone(&region),
        )
        .map_err(io::Error::other)?;
        Ok((writer, reader, region))
    }

    /// Creates one Bell Region holding `slots` unbound doorbells.
    fn bell_region(directory: &Path, slots: u32) -> io::Result<std::sync::Arc<BellRegion>> {
        let region_path = directory.join("loops.bells");
        let slot_count =
            NonZeroU32::new(slots).ok_or_else(|| io::Error::other("slots must be positive"))?;
        create_bell_region(&region_path, slot_count, 1).map_err(io::Error::other)?;
        BellRegion::open(&region_path).map_err(io::Error::other)
    }

    #[test]
    fn raw_atomic_and_copy_primitives_keep_their_safety_contract() {
        #[repr(align(64))]
        struct AlignedHeader([u8; HEADER_LEN]);

        let mut header = AlignedHeader([0; HEADER_LEN]);
        let base = header.0.as_mut_ptr();
        let commit: *mut u64 = base.wrapping_add(COMMIT_OFFSET).cast();
        let slot: *mut u32 = base.wrapping_add(READER_BELL_SLOT_OFFSET).cast();
        assert!(commit.is_aligned());
        assert!(slot.is_aligned());
        // SAFETY: `AlignedHeader` remains alive and exclusively owned for the
        // complete call. Both fixed offsets satisfy the target atomic alignment,
        // and these words have no simultaneous non-atomic access.
        unsafe {
            store_atomic_u64_pointer(commit, 16, POSITION_STORE_ORDERING);
            assert_eq!(load_atomic_u64_pointer(commit, POSITION_LOAD_ORDERING), 16);
            assert_eq!(load_atomic_u32_pointer(slot, Ordering::Relaxed), 0);
            assert_eq!(
                swap_atomic_u32_pointer(slot, SLOT_NOTIFIED, SLOT_NOTIFY_ORDERING),
                0
            );
            assert_eq!(
                load_atomic_u32_pointer(slot, SLOT_SNAPSHOT_ORDERING),
                SLOT_NOTIFIED
            );
        }

        let source = *b"abcdef";
        let mut destination = [0_u8; 6];
        // SAFETY: Both arrays are live and initialized, have the same exact
        // length, and reside in separate stack allocations.
        unsafe {
            copy_nonoverlapping_bytes(source.as_ptr(), destination.as_mut_ptr(), source.len());
        }
        assert_eq!(destination, source);

        let mut owned = BytesMut::with_capacity(source.len());
        let uninitialized = &mut owned.spare_capacity_mut()[..source.len()];
        // SAFETY: `source` is fully initialized, the destination is writable
        // spare capacity of the same length, and the allocations do not overlap.
        unsafe {
            copy_nonoverlapping_bytes(
                source.as_ptr(),
                uninitialized.as_mut_ptr().cast(),
                source.len(),
            );
        }
        // SAFETY: The preceding copy initialized every byte up to `source.len()`.
        unsafe { owned.set_len(source.len()) };
        owned.advance(1);
        owned.truncate(4);
        assert_eq!(owned.freeze(), Bytes::from_static(b"bcde"));
    }

    #[test]
    fn platform_wait_reports_a_notified_bell_before_sleep() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let region = bell_region(directory.path(), 1)?;
        let path = directory.path().join("queue.mmap");
        let capacity = DataCapacity::try_from(16).map_err(io::Error::other)?;
        create_queue_file(&path, capacity, NonZeroU64::MIN).map_err(io::Error::other)?;
        let memory = MappedQueueMemory::open(
            &path,
            OpenRole::Reader,
            LoopBell::new(region.slot(0).map_err(io::Error::other)?),
            region,
        )
        .map_err(io::Error::other)?;

        assert_eq!(
            memory.wait_bell().map_err(io::Error::other)?,
            SignalWaitOutcome::Progress
        );
        Ok(())
    }

    #[test]
    fn a_timed_doorbell_wait_expires_without_changing_queue_bytes() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("queue.mmap");
        let (mut writer, mut reader, region) =
            endpoint_pair(directory.path(), "queue.mmap", 32, 8)?;
        let before = std::fs::read(&path)?;
        let bell = LoopBell::new(region.slot(1).map_err(io::Error::other)?);

        assert_eq!(
            bell.wait_until_for(Duration::from_millis(1), || reader.has_unread())
                .map_err(io::Error::other)?,
            TimedWaitOutcome::TimedOut
        );
        assert_eq!(std::fs::read(&path)?, before);

        assert!(matches!(
            writer.try_write(b"payload").map_err(io::Error::other)?,
            WriteOutcome::Committed(_)
        ));
        assert_eq!(
            bell.wait_until_for(Duration::from_secs(1), || reader.has_unread())
                .map_err(io::Error::other)?,
            TimedWaitOutcome::Ready
        );
        let ReadOutcome::Record(record) = reader.try_read().map_err(io::Error::other)? else {
            return Err(io::Error::other("Committed record was not readable"));
        };
        assert_eq!(record.payload(), b"payload");
        Ok(())
    }

    #[test]
    fn a_local_interrupt_wakes_a_timed_doorbell_wait() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let region = bell_region(directory.path(), 2)?;
        let bell = LoopBell::new(region.slot(0).map_err(io::Error::other)?);
        let interrupter = bell.interrupter();
        let waiter = std::thread::spawn(move || {
            bell.wait_until_for(Duration::from_secs(10), || Ok::<bool, BellError>(false))
        });

        interrupter.interrupt().map_err(io::Error::other)?;

        assert_eq!(
            waiter
                .join()
                .map_err(|_| io::Error::other("Timed Queue waiter panicked"))?
                .map_err(io::Error::other)?,
            TimedWaitOutcome::Interrupted
        );
        Ok(())
    }

    #[test]
    fn a_commit_wakes_a_timed_wait_on_the_reader_loop_doorbell() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let (mut writer, reader, region) = endpoint_pair(directory.path(), "queue.mmap", 16, 8)?;
        let bell = LoopBell::new(region.slot(1).map_err(io::Error::other)?);
        let waiter = std::thread::spawn(move || {
            let outcome = bell.wait_until_for(Duration::from_secs(10), || reader.has_unread());
            (reader, outcome)
        });

        assert!(matches!(
            writer.try_write(b"A").map_err(io::Error::other)?,
            WriteOutcome::Committed(_)
        ));

        let (mut reader, outcome) = waiter
            .join()
            .map_err(|_| io::Error::other("Timed Queue waiter panicked"))?;
        assert_eq!(outcome.map_err(io::Error::other)?, TimedWaitOutcome::Ready);
        let ReadOutcome::Record(record) = reader.try_read().map_err(io::Error::other)? else {
            return Err(io::Error::other("Committed record was not readable"));
        };
        assert_eq!(record.payload(), b"A");
        Ok(())
    }

    #[test]
    fn consuming_an_owned_record_reuses_its_payload_allocation() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let (mut writer, mut reader, _region) =
            endpoint_pair(directory.path(), "queue.mmap", 32, 8)?;
        assert!(matches!(
            writer.try_write(b"payload").map_err(io::Error::other)?,
            WriteOutcome::Committed(_)
        ));
        let ReadOutcome::Record(record) = reader.try_read().map_err(io::Error::other)? else {
            return Err(io::Error::other("Committed record was not readable"));
        };
        let payload_pointer = record.payload().as_ptr();

        let payload = record.into_payload_bytes();

        assert_eq!(payload.as_ref(), b"payload");
        assert_eq!(payload.as_ptr(), payload_pointer);
        Ok(())
    }

    #[test]
    fn loom_commit_publishes_complete_record_bytes() {
        loom::model(|| {
            let memory = ModelMemory::new();
            let probe = memory.clone();
            let mut writer = WriterCore::new(memory.clone());
            let mut reader = ReaderCore::new(memory);

            let writer_thread = thread::spawn(move || writer.try_write(b"A"));
            let reader_thread = thread::spawn(move || {
                let outcome = reader.try_read();
                (reader, outcome)
            });

            let writer_result = writer_thread.join();
            assert!(writer_result.is_ok(), "writer thread panicked");
            let Ok(writer_result) = writer_result else {
                return;
            };
            assert!(matches!(writer_result, Ok(WriteOutcome::Committed(_))));

            let reader_result = reader_thread.join();
            assert!(reader_result.is_ok(), "reader thread panicked");
            let Ok((mut reader, outcome)) = reader_result else {
                return;
            };
            let outcome = successful(outcome, "reader rejected a valid frame");
            let record = match outcome {
                Some(ReadOutcome::Empty) => successful(
                    reader.try_read(),
                    "reader rejected the frame after writer completion",
                )
                .and_then(record),
                Some(outcome) => record(outcome),
                None => None,
            };
            assert!(record.is_some(), "published record was not visible");
            let Some(record) = record else {
                return;
            };
            assert_eq!(record.payload(), b"A");
            assert_eq!(probe.complete_frame_read_count(), 1);
        });
    }

    #[test]
    fn loom_release_precedes_writer_reuse() {
        loom::model(|| {
            let memory = ModelMemory::new();
            let mut writer = WriterCore::new(memory.clone());
            assert!(matches!(
                writer.try_write(b"A"),
                Ok(WriteOutcome::Committed(_))
            ));
            let mut reader = ReaderCore::new(memory);

            let writer_thread = thread::spawn(move || {
                loop {
                    match writer.try_write(b"B") {
                        Ok(WriteOutcome::Full) => thread::yield_now(),
                        outcome => return (writer, outcome),
                    }
                }
            });
            let reader_thread = thread::spawn(move || {
                let outcome = reader.try_read();
                let record =
                    successful(outcome, "initial record was not readable").and_then(record);
                let Some(record) = record else {
                    return reader;
                };
                assert_eq!(record.payload(), b"A");
                assert!(reader.release(1).is_ok());
                reader
            });

            let writer_result = writer_thread.join();
            assert!(writer_result.is_ok(), "writer thread panicked");
            let Ok((_writer, outcome)) = writer_result else {
                return;
            };
            let reader_result = reader_thread.join();
            assert!(reader_result.is_ok(), "reader thread panicked");
            let Ok(mut reader) = reader_result else {
                return;
            };
            let Some(outcome) = successful(outcome, "second write failed") else {
                return;
            };
            assert!(matches!(outcome, WriteOutcome::Committed(_)));
            let next =
                successful(reader.try_read(), "second record was not readable").and_then(record);
            assert!(
                next.is_some(),
                "reused capacity did not contain the second record"
            );
            let Some(next) = next else {
                return;
            };
            assert_eq!(next.payload(), b"B");
        });
    }

    #[test]
    fn loom_write_receipt_tracks_the_reader_release() {
        loom::model(|| {
            let memory = ModelMemory::new();
            let mut writer = WriterCore::new(memory.clone());
            let outcome = writer.try_write(b"A");
            assert!(matches!(&outcome, Ok(WriteOutcome::Committed(_))));
            let Some(WriteOutcome::Committed(receipt)) = outcome.ok() else {
                return;
            };
            let mut reader = ReaderCore::new(memory);
            let record =
                successful(reader.try_read(), "initial record was not readable").and_then(record);
            assert!(record.is_some(), "initial record was not copied");

            let observer = thread::spawn(move || {
                let before = writer.is_released(&receipt);
                thread::yield_now();
                (writer, receipt, before)
            });
            let releaser = thread::spawn(move || reader.release(1));

            let release_result = releaser.join();
            assert!(release_result.is_ok(), "reader thread panicked");
            let Ok(release_result) = release_result else {
                return;
            };
            assert!(release_result.is_ok(), "reader failed to publish release");
            let observer_result = observer.join();
            assert!(observer_result.is_ok(), "writer thread panicked");
            let Ok((writer, receipt, before)) = observer_result else {
                return;
            };
            assert!(before.is_ok(), "writer rejected valid Queue state");
            assert_eq!(writer.is_released(&receipt).ok(), Some(true));
        });
    }

    #[test]
    fn loom_wait_for_receipt_covers_every_arm_release_wait_interleaving() {
        loom::model(|| {
            let memory = ModelMemory::new();
            let mut writer = WriterCore::new(memory.clone());
            let outcome = writer.try_write(b"A");
            assert!(matches!(&outcome, Ok(WriteOutcome::Committed(_))));
            let Some(WriteOutcome::Committed(receipt)) = outcome.ok() else {
                return;
            };
            let mut reader = ReaderCore::new(memory);
            let record =
                successful(reader.try_read(), "initial record was not readable").and_then(record);
            assert!(record.is_some(), "initial record was not copied");

            let waiter = thread::spawn(move || writer.wait_released(&receipt));
            let releaser = thread::spawn(move || reader.release(1));

            let release_result = releaser.join();
            assert!(release_result.is_ok(), "reader thread panicked");
            let Ok(release_result) = release_result else {
                return;
            };
            assert!(release_result.is_ok(), "reader failed to publish release");
            let wait_result = waiter.join();
            assert!(wait_result.is_ok(), "writer thread panicked");
            let Ok(wait_result) = wait_result else {
                return;
            };
            assert_eq!(wait_result.ok(), Some(WaitOutcome::Ready));
        });
    }

    #[test]
    fn loom_write_receipt_rejects_corrupt_live_positions() {
        loom::model(|| {
            let memory = ModelMemory::new();
            let mut writer = WriterCore::new(memory.clone());
            let outcome = writer.try_write(b"A");
            assert!(matches!(&outcome, Ok(WriteOutcome::Committed(_))));
            let Some(WriteOutcome::Committed(receipt)) = outcome.ok() else {
                return;
            };
            let invalid_release = LogicalPosition::try_from(24);
            assert!(invalid_release.is_ok(), "test release must be aligned");
            let Some(invalid_release) = invalid_release.ok() else {
                return;
            };
            memory.publish_release(invalid_release);

            assert!(matches!(
                writer.is_released(&receipt),
                Err(QueueRuntimeError::Format(FormatError::ReleaseAheadOfCommit))
            ));
        });
    }

    #[test]
    fn loom_wait_for_data_covers_every_arm_publish_wait_interleaving() {
        loom::model(|| {
            let memory = ModelMemory::new();
            let mut writer = WriterCore::new(memory.clone());
            let mut reader = ReaderCore::new(memory);

            let reader_thread = thread::spawn(move || {
                let wait = reader.wait_readable();
                let read = reader.try_read();
                (wait, read)
            });
            let writer_thread = thread::spawn(move || writer.try_write(b"A"));

            let writer_result = writer_thread.join();
            assert!(writer_result.is_ok(), "writer thread panicked");
            let Ok(writer_result) = writer_result else {
                return;
            };
            assert!(matches!(writer_result, Ok(WriteOutcome::Committed(_))));

            let reader_result = reader_thread.join();
            assert!(reader_result.is_ok(), "reader thread panicked");
            let Ok((wait, read)) = reader_result else {
                return;
            };
            assert_eq!(wait.ok(), Some(WaitOutcome::Ready));
            let record = successful(read, "reader rejected the published frame").and_then(record);
            assert!(record.is_some(), "reader woke without a published frame");
        });
    }

    #[test]
    fn loom_timed_wait_and_publish_preserve_the_committed_record() {
        loom::model(|| {
            let memory = ModelMemory::new();
            let mut writer = WriterCore::new(memory.clone());
            let reader = ReaderCore::new(memory);

            let reader_thread = thread::spawn(move || {
                let local_read = reader.release_boundaries.back().copied();
                let first_wait = std::cell::Cell::new(true);
                let wait = wait_with_next_platform_wait(
                    &reader.memory,
                    || {
                        reader_positions(&reader.memory, local_read)
                            .map(|(commit, read)| commit != read)
                    },
                    || {
                        if first_wait.replace(false) {
                            Ok(PlatformWait::Timed(Duration::from_secs(1)))
                        } else {
                            Err(())
                        }
                    },
                );
                (reader, wait)
            });
            let writer_thread = thread::spawn(move || writer.try_write(b"A"));

            let writer_result = writer_thread.join();
            assert!(writer_result.is_ok(), "writer thread panicked");
            let Ok(writer_result) = writer_result else {
                return;
            };
            assert!(matches!(writer_result, Ok(WriteOutcome::Committed(_))));

            let reader_result = reader_thread.join();
            assert!(reader_result.is_ok(), "reader thread panicked");
            let Ok((mut reader, wait)) = reader_result else {
                return;
            };
            assert!(matches!(
                wait,
                Ok(WaitLoopOutcome::Ready | WaitLoopOutcome::Expired(()))
            ));
            let record = successful(reader.try_read(), "committed frame was hidden by timeout")
                .and_then(record);
            assert!(record.is_some(), "committed frame was lost");
        });
    }

    #[test]
    fn loom_wait_for_space_covers_every_arm_release_wait_interleaving() {
        loom::model(|| {
            let memory = ModelMemory::new();
            let mut writer = WriterCore::new(memory.clone());
            assert!(matches!(
                writer.try_write(b"A"),
                Ok(WriteOutcome::Committed(_))
            ));
            let mut reader = ReaderCore::new(memory);
            let initial = successful(reader.try_read(), "initial frame was not readable");
            assert!(initial.and_then(record).is_some());

            let writer_thread = thread::spawn(move || {
                let wait = writer.wait_writable(1);
                let write = writer.try_write(b"B");
                (wait, write)
            });
            let reader_thread = thread::spawn(move || reader.release(1));

            let reader_result = reader_thread.join();
            assert!(reader_result.is_ok(), "reader thread panicked");
            let Ok(reader_result) = reader_result else {
                return;
            };
            assert!(reader_result.is_ok(), "reader failed to release capacity");

            let writer_result = writer_thread.join();
            assert!(writer_result.is_ok(), "writer thread panicked");
            let Ok((wait, write)) = writer_result else {
                return;
            };
            assert_eq!(wait.ok(), Some(WaitOutcome::Ready));
            assert!(matches!(write, Ok(WriteOutcome::Committed(_))));
        });
    }

    /// One doorbell must carry several conditions published by several ringers.
    ///
    /// The ADR's verification asks this protocol to hold with several ringers
    /// and several conditions. Each publisher here publishes exactly one fact
    /// and rings the one doorbell, and the loop only leaves when every
    /// subscribed condition holds, so a wakeup lost for either ringer leaves
    /// the loop asleep and Loom reports the deadlock instead of a pass. The
    /// read counts then show that the recheck covered the whole subscription
    /// set rather than only the condition whose ringer woke it.
    ///
    /// The search is preemption-bounded to two switches: an exhaustive search of
    /// this three-thread shape with a blocking condvar is not tractable, and two
    /// preemptions are what it takes to interleave both ringers with the loop's
    /// arm and recheck, which is the ordering a lost wakeup needs.
    #[test]
    fn loom_one_doorbell_rechecks_every_condition_of_every_ringer() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(2);
        builder.check(|| {
            let memory = ModelMemory::new();
            let subscriptions = Arc::new(ModelSubscriptions::new());
            let ringers = (0..MODEL_CONDITION_COUNT)
                .map(|index| {
                    let memory = memory.clone();
                    let subscriptions = Arc::clone(&subscriptions);
                    thread::spawn(move || {
                        subscriptions.publish(index);
                        memory.ring_peer()
                    })
                })
                .collect::<Vec<_>>();
            let loop_memory = memory.clone();
            let loop_subscriptions = Arc::clone(&subscriptions);
            let loop_thread = thread::spawn(move || {
                wait_with_next_platform_wait(
                    &loop_memory,
                    || {
                        Ok::<bool, QueueRuntimeError>(
                            loop_subscriptions.recheck() == MODEL_CONDITION_COUNT,
                        )
                    },
                    || Ok::<PlatformWait, std::convert::Infallible>(PlatformWait::Indefinite),
                )
            });

            for ringer in ringers {
                assert!(matches!(ringer.join(), Ok(Ok(()))), "a ringer failed");
            }
            let outcome = loop_thread.join();
            assert!(outcome.is_ok(), "the loop thread panicked");
            let Ok(outcome) = outcome else {
                return;
            };
            assert!(
                matches!(outcome, Ok(WaitLoopOutcome::Ready)),
                "the loop slept through a published condition"
            );
            // The recheck walks the subscription list as a set: every pass
            // reads every condition, so all counts are equal and non-zero.
            let passes = subscriptions.rechecks(0);
            assert!(passes > 0, "the loop never rechecked its conditions");
            for index in 1..MODEL_CONDITION_COUNT {
                assert_eq!(
                    subscriptions.rechecks(index),
                    passes,
                    "the recheck skipped or repeated condition {index}"
                );
            }
        });
    }

    #[test]
    fn loom_ready_paths_do_not_enter_platform_wait_or_wake() {
        loom::model(|| {
            let memory = ModelMemory::new();
            let probe = memory.clone();
            let mut writer = WriterCore::new(memory.clone());
            let mut reader = ReaderCore::new(memory);

            assert!(matches!(
                writer.try_write(b"A"),
                Ok(WriteOutcome::Committed(_))
            ));
            assert_eq!(probe.wake_call_count(), 0);
            assert_eq!(reader.wait_readable().ok(), Some(WaitOutcome::Ready));
            assert_eq!(probe.wait_call_count(), 0);
            assert!(successful(reader.try_read(), "frame was not readable").is_some());
            assert!(reader.release(1).is_ok());
            assert_eq!(probe.wake_call_count(), 0);
            assert_eq!(writer.wait_writable(1).ok(), Some(WaitOutcome::Ready));
            assert_eq!(probe.wait_call_count(), 0);
        });
    }

    #[test]
    fn loom_interrupted_wait_disarms_without_changing_queue_facts() {
        loom::model(|| {
            let memory = ModelMemory::new();
            memory.interrupt_next_wait();
            let mut reader = ReaderCore::new(memory.clone());

            assert_eq!(reader.wait_readable().ok(), Some(WaitOutcome::Interrupted));
            assert_eq!(memory.bell_value(), SLOT_NOTIFIED);
            assert_eq!(memory.load_commit().ok().map(LogicalPosition::get), Some(0));
            assert_eq!(
                memory.load_release().ok().map(LogicalPosition::get),
                Some(0)
            );
        });
    }

    #[test]
    fn loom_local_interrupt_covers_every_wait_registration_window() {
        loom::model(|| {
            let memory = ModelMemory::new();
            let control = memory.clone();
            let mut reader = ReaderCore::new(memory.clone());

            let waiter = thread::spawn(move || reader.wait_readable());
            let interrupter = thread::spawn(move || control.request_wait_interruption());

            let interrupt_result = interrupter.join();
            assert!(interrupt_result.is_ok(), "interrupter thread panicked");
            let Ok(interrupt_result) = interrupt_result else {
                return;
            };
            assert!(interrupt_result.is_ok(), "local interrupt failed");

            let wait_result = waiter.join();
            assert!(wait_result.is_ok(), "waiter thread panicked");
            let Ok(wait_result) = wait_result else {
                return;
            };
            assert_eq!(wait_result.ok(), Some(WaitOutcome::Interrupted));
            assert!(!memory.wait_interruption_pending());
            assert_eq!(memory.bell_value(), SLOT_NOTIFIED);
            assert_eq!(memory.load_commit().ok().map(LogicalPosition::get), Some(0));
            assert_eq!(
                memory.load_release().ok().map(LogicalPosition::get),
                Some(0)
            );
        });
    }

    #[test]
    fn loom_repeated_local_interrupts_coalesce() {
        loom::model(|| {
            let memory = ModelMemory::new();
            let first_control = memory.clone();
            let second_control = memory.clone();
            let first = thread::spawn(move || first_control.request_wait_interruption());
            let second = thread::spawn(move || second_control.request_wait_interruption());

            assert!(first.join().is_ok_and(|result| result.is_ok()));
            assert!(second.join().is_ok_and(|result| result.is_ok()));
            let mut reader = ReaderCore::new(memory.clone());

            assert_eq!(reader.wait_readable().ok(), Some(WaitOutcome::Interrupted));
            assert!(!memory.wait_interruption_pending());
            assert_eq!(memory.wait_call_count(), 0);
        });
    }

    #[test]
    fn loom_local_interrupt_wake_failure_preserves_the_pending_request() {
        loom::model(|| {
            let memory = ModelMemory::new();
            memory.swap_bell(SLOT_ARMED, SLOT_NOTIFY_ORDERING);
            memory.fail_next_wake();

            assert!(memory.request_wait_interruption().is_err());
            assert!(memory.wait_interruption_pending());
            assert_eq!(memory.bell_value(), SLOT_NOTIFIED);
            assert_eq!(memory.load_commit().ok().map(LogicalPosition::get), Some(0));
            assert_eq!(
                memory.load_release().ok().map(LogicalPosition::get),
                Some(0)
            );

            let mut reader = ReaderCore::new(memory);
            assert_eq!(reader.wait_readable().ok(), Some(WaitOutcome::Interrupted));
        });
    }

    #[test]
    fn loom_queue_progress_and_local_interrupt_preserve_the_queue_fact() {
        for interrupt_first in [false, true] {
            loom::model(move || {
                let memory = ModelMemory::new();
                let control = memory.clone();
                let mut reader = ReaderCore::new(memory.clone());

                let waiter = thread::spawn(move || {
                    let outcome = reader.wait_readable();
                    (reader, outcome)
                });
                let notifier = thread::spawn(move || -> Result<(), QueueRuntimeError> {
                    let position = LogicalPosition::try_from(8)?;
                    if interrupt_first {
                        control.request_wait_interruption()?;
                    }
                    control.publish_commit(position);
                    control.ring_peer()?;
                    if !interrupt_first {
                        control.request_wait_interruption()?;
                    }
                    Ok(())
                });

                let notification_result = notifier.join();
                assert!(notification_result.is_ok(), "notifier thread panicked");
                let Ok(notification_result) = notification_result else {
                    return;
                };
                assert!(notification_result.is_ok(), "notification failed");

                let waiter_result = waiter.join();
                assert!(waiter_result.is_ok(), "waiter thread panicked");
                let Ok((mut reader, wait_result)) = waiter_result else {
                    return;
                };
                assert!(matches!(
                    &wait_result,
                    Ok(WaitOutcome::Ready | WaitOutcome::Interrupted)
                ));
                assert_eq!(memory.load_commit().ok().map(LogicalPosition::get), Some(8));
                if matches!(&wait_result, Ok(WaitOutcome::Ready)) {
                    assert_eq!(reader.wait_readable().ok(), Some(WaitOutcome::Interrupted));
                }
            });
        }
    }

    #[test]
    fn loom_local_interrupt_publishes_shutdown_before_wakeup() {
        loom::model(|| {
            let memory = ModelMemory::new();
            let control = memory.clone();
            let stopping = Arc::new(AtomicBool::new(false));
            let published_stopping = Arc::clone(&stopping);
            let mut reader = ReaderCore::new(memory);

            let waiter = thread::spawn(move || {
                let outcome = reader.wait_readable();
                let observed_stopping = stopping.load(Ordering::Relaxed);
                (outcome, observed_stopping)
            });
            let interrupter = thread::spawn(move || {
                published_stopping.store(true, Ordering::Relaxed);
                control.request_wait_interruption()
            });

            let interrupt_result = interrupter.join();
            assert!(interrupt_result.is_ok(), "interrupter thread panicked");
            let Ok(interrupt_result) = interrupt_result else {
                return;
            };
            assert!(interrupt_result.is_ok(), "local interrupt failed");

            let wait_result = waiter.join();
            assert!(wait_result.is_ok(), "waiter thread panicked");
            let Ok((outcome, observed_stopping)) = wait_result else {
                return;
            };
            assert_eq!(outcome.ok(), Some(WaitOutcome::Interrupted));
            assert!(
                observed_stopping,
                "shutdown fact was not visible after wakeup"
            );
        });
    }

    #[test]
    fn loom_wake_failure_does_not_hide_published_commit_or_release() {
        loom::model(|| {
            let memory = ModelMemory::new();
            memory.swap_bell(SLOT_ARMED, SLOT_NOTIFY_ORDERING);
            memory.fail_next_wake();
            let mut writer = WriterCore::new(memory.clone());
            let mut committed = 0;
            assert!(
                writer
                    .try_write_observed(b"A", || {
                        assert_eq!(
                            memory.load_commit().ok().map(LogicalPosition::get),
                            Some(16)
                        );
                        committed += 1;
                    })
                    .is_err()
            );
            assert_eq!(committed, 1);

            let mut reader = ReaderCore::new(memory.clone());
            assert!(successful(reader.try_read(), "committed frame was hidden").is_some());
            memory.swap_bell(SLOT_ARMED, SLOT_NOTIFY_ORDERING);
            memory.fail_next_wake();
            assert!(reader.release(1).is_err());
            assert!(matches!(
                reader.release(1),
                Err(QueueRuntimeError::InvalidReleaseCount)
            ));
            assert!(matches!(
                writer.try_write(b"B"),
                Ok(WriteOutcome::Committed(_))
            ));
        });
    }

    #[test]
    fn queue_observer_counts_wrap_space_without_retaining_the_endpoint() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let (mut writer, mut reader, _region) =
            endpoint_pair(directory.path(), "observed.queue", 64, 24)?;
        let observer = writer.observer();
        assert_eq!(observer.sample(), Some((0, 64)));
        for payload in [&[1; 17][..], &[2][..]] {
            assert!(matches!(
                writer.try_write(payload),
                Ok(WriteOutcome::Committed(_))
            ));
            assert!(matches!(reader.try_read(), Ok(ReadOutcome::Record(_))));
            reader.release(1).map_err(io::Error::other)?;
        }
        assert!(matches!(
            writer.try_write(&[3; 17]),
            Ok(WriteOutcome::Committed(_))
        ));
        assert_eq!(observer.sample(), Some((48, 64)));
        assert!(matches!(reader.try_read(), Ok(ReadOutcome::Record(_))));
        assert_eq!(
            observer.sample(),
            Some((48, 64)),
            "reading alone does not release space"
        );
        reader.release(1).map_err(io::Error::other)?;
        assert_eq!(observer.sample(), Some((0, 64)));
        drop(writer);
        assert_eq!(observer.sample(), None);
        Ok(())
    }

    #[test]
    fn bell_region_layout_is_exact_and_starts_every_slot_notified() -> io::Result<()> {
        assert_eq!(
            bell_region_len(NonZeroU32::new(1).ok_or_else(|| io::Error::other("slot"))?)
                .map_err(io::Error::other)?,
            4096
        );
        assert_eq!(
            bell_region_len(NonZeroU32::new(63).ok_or_else(|| io::Error::other("slot"))?)
                .map_err(io::Error::other)?,
            4096
        );
        assert_eq!(
            bell_region_len(NonZeroU32::new(64).ok_or_else(|| io::Error::other("slot"))?)
                .map_err(io::Error::other)?,
            8192
        );

        let directory = tempfile::tempdir()?;
        let region_path = directory.path().join("loops.bells");
        create_bell_region(
            &region_path,
            NonZeroU32::new(2).ok_or(io::Error::other("slot"))?,
            7,
        )
        .map_err(io::Error::other)?;
        let bytes = std::fs::read(&region_path)?;
        assert_eq!(bytes.len(), 4096);
        assert_eq!(&bytes[..8], &BELL_MAGIC);
        assert_eq!(read_u32(&bytes, 8), BELL_FORMAT_VERSION);
        assert_eq!(read_u32(&bytes, 12), 2);
        assert_eq!(read_u64(&bytes, 16), 7);
        assert!(bytes[24..64].iter().all(|byte| *byte == 0));
        assert_eq!(read_u32(&bytes, 64), SLOT_NOTIFIED);
        assert_eq!(read_u32(&bytes, 128), SLOT_NOTIFIED);
        assert!(bytes[68..128].iter().all(|byte| *byte == 0));
        assert!(bytes[132..192].iter().all(|byte| *byte == 0));
        assert!(bytes[192..].iter().all(|byte| *byte == 0));
        assert_eq!(read_bell_region_epoch(&region_path), Some(7));

        let region = BellRegion::open(&region_path).map_err(io::Error::other)?;
        assert_eq!(region.slot_count().get(), 2);
        assert!(region.slot(2).is_err());
        Ok(())
    }

    #[test]
    fn bell_region_rejects_every_corrupt_header_and_slot() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let region_path = directory.path().join("loops.bells");
        create_bell_region(
            &region_path,
            NonZeroU32::new(2).ok_or(io::Error::other("slot"))?,
            1,
        )
        .map_err(io::Error::other)?;
        let intact = std::fs::read(&region_path)?;

        for (name, offset, patch) in [
            ("magic", 0_usize, 0_u8),
            ("header padding", 24, 1),
            ("slot padding", 68, 1),
            ("second slot padding", 132, 1),
        ] {
            let mut bytes = intact.clone();
            bytes[offset] = patch;
            std::fs::write(&region_path, &bytes)?;
            assert!(
                BellRegion::open(&region_path).is_err(),
                "{name} was accepted"
            );
        }
        std::fs::write(&region_path, &intact)?;

        for (name, value) in [("zero slots", 0_u32), ("too many slots", 64)] {
            let mut bytes = intact.clone();
            write_u32(&mut bytes, 12, value);
            std::fs::write(&region_path, &bytes)?;
            assert!(
                BellRegion::open(&region_path).is_err(),
                "{name} was accepted"
            );
        }

        let mut bytes = intact.clone();
        bytes.truncate(2048);
        std::fs::write(&region_path, &bytes)?;
        assert!(
            BellRegion::open(&region_path).is_err(),
            "short file accepted"
        );
        std::fs::write(&region_path, &intact[..192])?;
        assert!(
            BellRegion::open(&region_path).is_err(),
            "header-only file accepted"
        );

        let mut bytes = intact.clone();
        write_u32(&mut bytes, 8, BELL_FORMAT_VERSION + 1);
        std::fs::write(&region_path, &bytes)?;
        assert!(BellRegion::open(&region_path).is_err(), "version accepted");
        Ok(())
    }

    #[test]
    fn bell_region_rejects_a_slot_word_that_is_neither_armed_nor_notified() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let region_path = directory.path().join("loops.bells");
        create_bell_region(
            &region_path,
            NonZeroU32::new(1).ok_or(io::Error::other("slot"))?,
            1,
        )
        .map_err(io::Error::other)?;
        let intact = std::fs::read(&region_path)?;
        let mut bytes = intact;
        write_u32(&mut bytes, 64, 2);
        std::fs::write(&region_path, &bytes)?;
        assert!(matches!(
            BellRegion::open(&region_path),
            Err(BellError::SlotState)
        ));
        Ok(())
    }

    #[test]
    fn ringing_an_unbound_peer_slot_is_not_an_error() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("queue.mmap");
        create_queue_file(
            &path,
            DataCapacity::try_from(32).map_err(io::Error::other)?,
            NonZeroU64::MIN,
        )
        .map_err(io::Error::other)?;
        let region = bell_region(directory.path(), 2)?;
        // Only the writer binds, so its peer slot still names no loop.
        let writer_slot = region.slot(0).map_err(io::Error::other)?;
        let expected_writer_slot = writer_slot.index();
        let mut writer = QueueWriter::open(
            &path,
            LoopBell::new(writer_slot),
            std::sync::Arc::clone(&region),
        )
        .map_err(io::Error::other)?;
        assert!(matches!(
            writer.try_write(b"A"),
            Ok(WriteOutcome::Committed(_))
        ));
        // No reader ever bound, so the slot the writer rings still names no
        // loop; the commit above proved that ring is a no-op and not an error.
        assert_eq!(
            u32::from_le_bytes(
                std::fs::read(&path)?[72..76]
                    .try_into()
                    .map_err(|_| io::Error::other("header word"))?
            ),
            UNBOUND_BELL_SLOT
        );
        assert_eq!(
            u32::from_le_bytes(
                std::fs::read(&path)?[136..140]
                    .try_into()
                    .map_err(|_| io::Error::other("header word"))?
            ),
            expected_writer_slot
        );
        Ok(())
    }

    #[test]
    fn arming_a_notified_slot_clears_the_ring_and_rejects_an_invalid_word() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let region_path = directory.path().join("loops.bells");
        create_bell_region(
            &region_path,
            NonZeroU32::new(1).ok_or(io::Error::other("slot"))?,
            1,
        )
        .map_err(io::Error::other)?;
        let slot_offset = crate::bell::slot_offset(0);
        let word = |path: &Path| -> io::Result<u32> {
            Ok(u32::from_le_bytes(
                read_word_at(path, slot_offset as u64).map_err(io::Error::other)?,
            ))
        };
        // A freshly created slot holds a pending ring, so arming it proves the
        // swap clears a notification rather than only re-armed parked state.
        let region = BellRegion::open(&region_path).map_err(io::Error::other)?;
        let slot = region.slot(0).map_err(io::Error::other)?;
        assert_eq!(word(&region_path)?, SLOT_NOTIFIED);
        slot.arm().map_err(io::Error::other)?;
        assert_eq!(word(&region_path)?, SLOT_ARMED);
        // Arming the already-armed slot is the state a second park leaves.
        slot.arm().map_err(io::Error::other)?;
        assert_eq!(word(&region_path)?, SLOT_ARMED);
        // A peer can corrupt the word after the region is open, so the guard is
        // reachable. Arming validates the word it displaced and reports it
        // rather than continuing on a corrupted region; like `publish`, the
        // swap precedes the check, so the word ends up armed either way.
        write_slot_word(&region_path, slot_offset, 2)?;
        assert!(matches!(slot.arm(), Err(BellError::SlotState)));
        assert_eq!(word(&region_path)?, SLOT_ARMED);
        Ok(())
    }

    /// Writes one doorbell slot word through a separate handle, the way a peer
    /// writing to the same mapped file would.
    fn write_slot_word(path: &Path, offset: usize, value: u32) -> io::Result<()> {
        use std::fs::OpenOptions;
        use std::os::unix::fs::FileExt;

        let file = OpenOptions::new().write(true).open(path)?;
        file.write_all_at(&value.to_le_bytes(), offset as u64)
    }

    #[test]
    fn loom_queue_observation_skips_races_and_never_exceeds_capacity() {
        loom::model(|| {
            let memory = ModelMemory::new();
            memory.shared.commit.store(16, POSITION_STORE_ORDERING);
            let releasing = memory.clone();
            let advance = thread::spawn(move || {
                releasing.shared.release.store(16, POSITION_STORE_ORDERING);
                releasing.shared.commit.store(32, POSITION_STORE_ORDERING);
            });
            let sample = sample_usage(
                memory.capacity(),
                || memory.shared.release.load(POSITION_LOAD_ORDERING),
                || memory.shared.commit.load(POSITION_LOAD_ORDERING),
            );
            assert!(sample.is_none_or(|usage| usage == 0 || usage == 16));
            assert!(advance.join().is_ok());
            assert_eq!(
                sample_usage(
                    memory.capacity(),
                    || memory.shared.release.load(POSITION_LOAD_ORDERING),
                    || memory.shared.commit.load(POSITION_LOAD_ORDERING)
                ),
                Some(16)
            );
        });
    }

    fn successful<T>(result: Result<T, QueueRuntimeError>, message: &'static str) -> Option<T> {
        assert!(result.is_ok(), "{message}: {:?}", result.as_ref().err());
        result.ok()
    }

    fn record(outcome: ReadOutcome) -> Option<OwnedRecord> {
        match outcome {
            ReadOutcome::Record(record) => Some(record),
            ReadOutcome::Empty => None,
        }
    }

    /// The Loom stand-in for one Queue as its two endpoint cores drive it.
    ///
    /// The model merges both endpoints into one memory: the tests build a
    /// [`WriterCore`] and a [`ReaderCore`] over clones of it, so a single
    /// doorbell carries every ring in the model. Which Bell Region slot a real
    /// peer rings is decided by the mapped runtime and covered by the mapped and
    /// byte-vector tests; this model exists to exercise the arm, recheck, and
    /// sleep protocol under every interleaving of several ringers, one local
    /// interrupt, and one or several subscribed conditions behind the one
    /// doorbell.
    #[derive(Debug, Clone)]
    struct ModelMemory {
        shared: Arc<ModelShared>,
    }

    #[derive(Debug)]
    struct ModelShared {
        commit: LoomAtomicU64,
        release: LoomAtomicU64,
        bell: ModelBell,
        complete_frame_reads: AtomicUsize,
        bytes: Vec<UnsafeCell<u8>>,
    }

    impl ModelMemory {
        fn new() -> Self {
            Self {
                shared: Arc::new(ModelShared {
                    commit: LoomAtomicU64::new(0),
                    release: LoomAtomicU64::new(0),
                    bell: ModelBell::new(),
                    complete_frame_reads: AtomicUsize::new(0),
                    bytes: (0..16).map(|_| UnsafeCell::new(0)).collect(),
                }),
            }
        }

        fn checked_range(
            &self,
            offset: u64,
            length: usize,
        ) -> Result<std::ops::Range<usize>, QueueRuntimeError> {
            let start =
                usize::try_from(offset).map_err(|_| QueueRuntimeError::MappingRangeInvalid)?;
            let end = start
                .checked_add(length)
                .ok_or(QueueRuntimeError::MappingRangeInvalid)?;
            if end > self.shared.bytes.len() {
                return Err(QueueRuntimeError::MappingRangeInvalid);
            }
            Ok(start..end)
        }

        fn complete_frame_read_count(&self) -> usize {
            self.shared.complete_frame_reads.load(Ordering::Relaxed)
        }

        fn bell(&self) -> &ModelBell {
            &self.shared.bell
        }

        /// Forces the doorbell word into one state, for corruption tests only.
        fn swap_bell(&self, value: u32, ordering: Ordering) -> u32 {
            self.bell().value.swap(value, ordering)
        }

        fn interrupt_next_wait(&self) {
            self.bell()
                .interrupt_next_wait
                .store(true, Ordering::Relaxed);
        }

        fn request_wait_interruption(&self) -> Result<(), QueueRuntimeError> {
            let mut pending = self.bell().lock_wait_interruption();
            *pending = true;
            self.bell().ring()
        }

        fn wait_interruption_pending(&self) -> bool {
            *self.bell().lock_wait_interruption()
        }

        fn fail_next_wake(&self) {
            self.bell().fail_next_wake.store(true, Ordering::Relaxed);
        }

        fn wake_call_count(&self) -> usize {
            self.bell().wake_calls.load(Ordering::Relaxed)
        }

        fn wait_call_count(&self) -> usize {
            self.bell().wait_calls.load(Ordering::Relaxed)
        }

        fn bell_value(&self) -> u32 {
            self.bell().value.load(Ordering::Relaxed)
        }
    }

    /// One loop doorbell in the model, with the same word semantics as a slot.
    #[derive(Debug)]
    struct ModelBell {
        value: LoomAtomicU32,
        wait_interruption: Mutex<bool>,
        interrupt_next_wait: AtomicBool,
        fail_next_wake: AtomicBool,
        wake_calls: AtomicUsize,
        wait_calls: AtomicUsize,
        gate: Mutex<()>,
        condition: Condvar,
    }

    impl ModelBell {
        fn new() -> Self {
            Self {
                value: LoomAtomicU32::new(SLOT_NOTIFIED),
                wait_interruption: Mutex::new(false),
                interrupt_next_wait: AtomicBool::new(false),
                fail_next_wake: AtomicBool::new(false),
                wake_calls: AtomicUsize::new(0),
                wait_calls: AtomicUsize::new(0),
                gate: Mutex::new(()),
                condition: Condvar::new(),
            }
        }

        /// Publishes one notification, waking the loop only when it was parked.
        fn ring(&self) -> Result<(), QueueRuntimeError> {
            let previous = self.value.swap(SLOT_NOTIFIED, SLOT_NOTIFY_ORDERING);
            if previous > SLOT_NOTIFIED {
                return Err(BellError::SlotState.into());
            }
            if previous == SLOT_ARMED {
                self.wake()?;
            }
            Ok(())
        }

        fn wait(&self) -> Result<SignalWaitOutcome, BellError> {
            self.wait_calls.fetch_add(1, Ordering::Relaxed);
            if self.interrupt_next_wait.swap(false, Ordering::Relaxed) {
                return Ok(SignalWaitOutcome::Interrupted);
            }
            let guard = self
                .gate
                .lock()
                .map_err(|_| BellError::MappingRangeInvalid)?;
            if self.value.load(Ordering::Acquire) != SLOT_ARMED {
                return Ok(SignalWaitOutcome::Progress);
            }
            let _guard = self
                .condition
                .wait(guard)
                .map_err(|_| BellError::MappingRangeInvalid)?;
            Ok(SignalWaitOutcome::Progress)
        }

        fn wait_for(&self, _timeout: Duration) -> Result<SignalWaitOutcome, BellError> {
            self.wait_calls.fetch_add(1, Ordering::Relaxed);
            if self.interrupt_next_wait.swap(false, Ordering::Relaxed) {
                return Ok(SignalWaitOutcome::Interrupted);
            }
            thread::yield_now();
            if self.value.load(Ordering::Acquire) == SLOT_ARMED {
                Ok(SignalWaitOutcome::TimedOut)
            } else {
                Ok(SignalWaitOutcome::Progress)
            }
        }

        fn wake(&self) -> Result<(), BellError> {
            let _guard = self
                .gate
                .lock()
                .map_err(|_| BellError::MappingRangeInvalid)?;
            self.wake_calls.fetch_add(1, Ordering::Relaxed);
            if self.fail_next_wake.swap(false, Ordering::Relaxed) {
                return Err(BellError::Io {
                    operation: "run model wake",
                    source: io::Error::other("model wake failed"),
                });
            }
            self.condition.notify_one();
            Ok(())
        }

        fn lock_wait_interruption(&self) -> loom::sync::MutexGuard<'_, bool> {
            self.wait_interruption
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        }
    }

    /// How many independent conditions one model loop subscribes to.
    ///
    /// Two is the smallest set that can show the property: one condition alone
    /// cannot tell "the loop rechecked the condition that rang" apart from "the
    /// loop rechecked its whole subscription set", and two independent ringers
    /// are the smallest fan-in one doorbell has to carry.
    const MODEL_CONDITION_COUNT: usize = 2;

    /// The conditions one model loop subscribes to, rechecked as one set.
    ///
    /// Real loops subscribe to several independent facts behind one doorbell: a
    /// submission commit, a completion release, a business timer. This type is
    /// that subscription list. The publishers and the loop's recheck walk the
    /// same array, so the recheck set cannot omit a subscribed condition
    /// without also removing that condition's fact, and the per-condition read
    /// counts below make a later change that rechecked only the condition whose
    /// ringer rang show up as an unequal count.
    #[derive(Debug)]
    struct ModelSubscriptions {
        facts: [LoomAtomicU64; MODEL_CONDITION_COUNT],
        reads: [AtomicUsize; MODEL_CONDITION_COUNT],
    }

    impl ModelSubscriptions {
        fn new() -> Self {
            Self {
                facts: std::array::from_fn(|_| LoomAtomicU64::new(0)),
                reads: std::array::from_fn(|_| AtomicUsize::new(0)),
            }
        }

        /// Publishes one condition's fact; that condition's ringer rings after.
        fn publish(&self, index: usize) {
            self.facts[index].store(1, POSITION_STORE_ORDERING);
        }

        /// Rechecks every subscribed condition and returns how many hold.
        fn recheck(&self) -> usize {
            let mut held = 0;
            for (index, fact) in self.facts.iter().enumerate() {
                self.reads[index].fetch_add(1, Ordering::Relaxed);
                if fact.load(POSITION_LOAD_ORDERING) != 0 {
                    held += 1;
                }
            }
            held
        }

        /// Returns how many times the loop rechecked one subscribed condition.
        fn rechecks(&self, index: usize) -> usize {
            self.reads[index].load(Ordering::Relaxed)
        }
    }

    impl WaitFront for ModelMemory {
        fn take_wait_interruption(&self) -> Option<WaitInterruption> {
            let mut pending = self.bell().lock_wait_interruption();
            let interrupted = *pending;
            *pending = false;
            interrupted.then_some(WaitInterruption)
        }

        fn arm_or_take_wait_interruption(&self) -> Result<WaitArmOutcome, BellError> {
            let mut pending = self.bell().lock_wait_interruption();
            if *pending {
                *pending = false;
                return Ok(WaitArmOutcome::Interrupted);
            }
            self.bell().value.swap(SLOT_ARMED, SLOT_ARM_ORDERING);
            Ok(WaitArmOutcome::Armed)
        }

        fn publish_wait_notification(&self) -> Result<(), BellError> {
            self.bell().value.swap(SLOT_NOTIFIED, SLOT_NOTIFY_ORDERING);
            Ok(())
        }

        fn wait_bell(&self) -> Result<SignalWaitOutcome, BellError> {
            self.bell().wait()
        }

        fn wait_bell_for(&self, timeout: Duration) -> Result<SignalWaitOutcome, BellError> {
            self.bell().wait_for(timeout)
        }
    }

    impl QueueMemory for ModelMemory {
        fn capacity(&self) -> DataCapacity {
            DataCapacity(16)
        }

        fn max_payload_size(&self) -> NonZeroU64 {
            NonZeroU64::MIN
        }

        fn load_commit(&self) -> Result<LogicalPosition, QueueRuntimeError> {
            LogicalPosition::try_from(self.shared.commit.load(POSITION_LOAD_ORDERING))
                .map_err(Into::into)
        }

        fn publish_commit(&self, position: LogicalPosition) {
            self.shared
                .commit
                .store(position.get(), POSITION_STORE_ORDERING);
        }

        fn load_release(&self) -> Result<LogicalPosition, QueueRuntimeError> {
            LogicalPosition::try_from(self.shared.release.load(POSITION_LOAD_ORDERING))
                .map_err(Into::into)
        }

        fn publish_release(&self, position: LogicalPosition) {
            self.shared
                .release
                .store(position.get(), POSITION_STORE_ORDERING);
        }

        fn ring_peer(&self) -> Result<(), QueueRuntimeError> {
            self.bell().ring()
        }

        fn read_data(&self, offset: u64, destination: &mut [u8]) -> Result<(), QueueRuntimeError> {
            let range = self.checked_range(offset, destination.len())?;
            if destination.len() > FRAME_HEADER_LEN {
                self.shared
                    .complete_frame_reads
                    .fetch_add(1, Ordering::Relaxed);
            }
            for (destination, source) in destination.iter_mut().zip(&self.shared.bytes[range]) {
                source.with(|pointer| {
                    // SAFETY: Loom owns this byte for the closure and verifies
                    // that commit/release ordering excludes a concurrent writer.
                    *destination = unsafe { *pointer };
                });
            }
            Ok(())
        }

        fn read_data_uninit(
            &self,
            offset: u64,
            destination: &mut [MaybeUninit<u8>],
        ) -> Result<(), QueueRuntimeError> {
            let range = self.checked_range(offset, destination.len())?;
            if destination.len() > FRAME_HEADER_LEN {
                self.shared
                    .complete_frame_reads
                    .fetch_add(1, Ordering::Relaxed);
            }
            for (destination, source) in destination.iter_mut().zip(&self.shared.bytes[range]) {
                source.with(|pointer| {
                    // SAFETY: Loom owns this byte for the closure and verifies
                    // that commit/release ordering excludes a concurrent writer.
                    destination.write(unsafe { *pointer });
                });
            }
            Ok(())
        }

        fn write_data(&self, offset: u64, source: &[u8]) -> Result<(), QueueRuntimeError> {
            let range = self.checked_range(offset, source.len())?;
            for (source, destination) in source.iter().zip(&self.shared.bytes[range]) {
                destination.with_mut(|pointer| {
                    // SAFETY: Loom owns this byte for the closure and verifies
                    // that commit/release ordering excludes a concurrent reader.
                    unsafe { *pointer = *source };
                });
            }
            Ok(())
        }
    }
}
