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

//! One page-aligned shared mapping whose slots are waiting-loop doorbells.
//!
//! A Bell Region holds no record, position, length, or business fact. Every
//! slot is one `atomic u32` whose only values are zero (armed) and one
//! (notified), exactly like the Queue signal word it replaces. A slot belongs
//! to the one event loop that parks on it; every peer that must wake that loop
//! writes the same slot through this mapping. Because one slot has exactly one
//! owner, the platform wake is precise and never thundering.
//!
//! The module owns the whole doorbell: the region format, the park protocol its
//! owner runs, its own error vocabulary, and the operating-system wait and wake
//! backend. Nothing here names a Queue type or touches a Queue byte, so any
//! event loop can park on a Bell Region.
//!
//! Runner and SDK use this one implementation and the same language-neutral
//! format vectors. No business codec, lifecycle owner or metrics backend lives here.

use std::error::Error;
use std::fmt;
use std::fs::OpenOptions;
use std::io;
use std::io::Read as _;
use std::io::Write;
use std::mem::size_of;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use memmap2::MmapRaw;

use crate::little_endian::{read_u32, read_u64, write_u32, write_u64};

use self::mapping::{copy_from_mapping, io_error, load_atomic_u32, swap_atomic_u32};
use self::wait::{
    SignalWaitOutcome, WaitArmOutcome, WaitFront, WaitInterruption, wait_for, wait_indefinitely,
};

pub(crate) mod mapping;
mod platform;
pub(crate) mod wait;

pub use self::wait::TimedWaitOutcome;
pub use self::wait::WaitOutcome;

/// Load ordering used to read a doorbell slot word without owning it.
pub(crate) const SLOT_SNAPSHOT_ORDERING: Ordering = Ordering::Relaxed;
/// Swap ordering that clears a doorbell slot: it takes any pending ring with it.
pub(crate) const SLOT_ARM_ORDERING: Ordering = Ordering::AcqRel;
/// Swap ordering that publishes a ring for the loop that owns the slot.
pub(crate) const SLOT_NOTIFY_ORDERING: Ordering = Ordering::Release;
/// Doorbell slot value meaning "the owner is about to sleep on this slot".
pub(crate) const SLOT_ARMED: u32 = 0;
/// Doorbell slot value meaning "some peer rang this slot".
pub(crate) const SLOT_NOTIFIED: u32 = 1;

/// Fixed magic that identifies a Bell Region mapping.
pub(crate) const BELL_MAGIC: [u8; 8] = *b"TENONBEL";

/// Initial and currently supported Bell Region format version.
pub(crate) const BELL_FORMAT_VERSION: u32 = 1;

/// Exact byte length of the immutable Bell Region header.
pub(crate) const BELL_HEADER_LEN: usize = 64;

/// Exact byte length of one slot, including its cache-line padding.
pub(crate) const BELL_SLOT_LEN: usize = 64;

/// Alignment unit of a Bell Region file length.
pub(crate) const BELL_PAGE_LEN: u64 = 4096;

const BELL_VERSION_OFFSET: usize = 8;
const BELL_SLOT_COUNT_OFFSET: usize = 12;
const BELL_EPOCH_OFFSET: usize = 16;
const BELL_HEADER_PADDING_START: usize = 24;
const BELL_ARM_WAIT_OPERATION: &str = "wait on the loop doorbell";
const BELL_NOTIFY_OPERATION: &str = "notify the loop doorbell";

/// A stable failure while creating, mapping, arming, waiting on, or ringing a Bell Region.
#[derive(Debug)]
#[non_exhaustive]
pub enum BellError {
    /// An operating-system file, mapping, or wake operation failed.
    Io {
        /// Stable operation name that failed.
        operation: &'static str,
        /// Original operating-system error.
        source: io::Error,
    },
    /// The mapped Bell Region is shorter than its immutable header.
    RegionTooShort,
    /// The Bell Region does not begin with Tenon's fixed magic bytes.
    Magic,
    /// The Bell Region declares a format version this implementation does not support.
    UnsupportedFormatVersion,
    /// The Bell Region declares zero slots or a slot count its length cannot hold.
    SlotCount,
    /// A Bell Region header or slot padding byte is non-zero.
    Padding,
    /// The Bell Region file length is not the exact aligned length of its slot count.
    RegionLength,
    /// A doorbell slot index is at or beyond the region's slot count.
    SlotOutOfRange,
    /// A doorbell slot word holds a value other than zero (armed) or one (notified).
    SlotState,
    /// A checked byte range fell outside the mapped Bell Region.
    MappingRangeInvalid,
}

impl BellError {
    /// Returns the stable machine-readable code for this error.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Io { .. } => "ipc.bell.io",
            Self::RegionTooShort => "ipc.bell.region_too_short",
            Self::Magic => "ipc.bell.magic_invalid",
            Self::UnsupportedFormatVersion => "ipc.bell.format_version_unsupported",
            Self::SlotCount => "ipc.bell.slot_count_invalid",
            Self::Padding => "ipc.bell.padding_nonzero",
            Self::RegionLength => "ipc.bell.region_length_invalid",
            Self::SlotOutOfRange => "ipc.bell.slot_out_of_range",
            Self::SlotState => "ipc.bell.slot_state_invalid",
            Self::MappingRangeInvalid => "ipc.bell.mapping_range_invalid",
        }
    }
}

impl fmt::Display for BellError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { operation, source } => {
                write!(formatter, "IPC Bell Region {operation} failed: {source}")
            }
            Self::RegionTooShort => {
                formatter.write_str("IPC Bell Region is too short to hold its header")
            }
            Self::Magic => formatter.write_str("invalid IPC Bell Region magic"),
            Self::UnsupportedFormatVersion => {
                formatter.write_str("unsupported IPC Bell Region format version")
            }
            Self::SlotCount => formatter.write_str("invalid IPC Bell Region slot count"),
            Self::Padding => formatter.write_str("IPC Bell Region padding bytes must be zero"),
            Self::RegionLength => {
                formatter.write_str("IPC Bell Region length does not match its slot count")
            }
            Self::SlotOutOfRange => {
                formatter.write_str("IPC Bell Region slot index is out of range")
            }
            Self::SlotState => {
                formatter.write_str("IPC Bell Region slot word is neither armed nor notified")
            }
            Self::MappingRangeInvalid => {
                formatter.write_str("IPC Bell Region mapped byte range is invalid")
            }
        }
    }
}

impl Error for BellError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::RegionTooShort
            | Self::Magic
            | Self::UnsupportedFormatVersion
            | Self::SlotCount
            | Self::Padding
            | Self::RegionLength
            | Self::SlotOutOfRange
            | Self::SlotState
            | Self::MappingRangeInvalid => None,
        }
    }
}

impl From<BellError> for io::Error {
    fn from(error: BellError) -> Self {
        io::Error::other(error)
    }
}

/// Returns the exact file length of a Bell Region holding `slot_count` slots.
pub(crate) fn bell_region_len(slot_count: NonZeroU32) -> Result<u64, BellError> {
    let unaligned = (BELL_HEADER_LEN as u64)
        .checked_add(
            (BELL_SLOT_LEN as u64)
                .checked_mul(u64::from(slot_count.get()))
                .ok_or(BellError::SlotCount)?,
        )
        .ok_or(BellError::SlotCount)?;
    let remainder = unaligned % BELL_PAGE_LEN;
    if remainder == 0 {
        Ok(unaligned)
    } else {
        Ok(unaligned + (BELL_PAGE_LEN - remainder))
    }
}

/// Returns the byte offset of one slot's state word.
pub(crate) const fn slot_offset(index: u32) -> usize {
    BELL_HEADER_LEN + BELL_SLOT_LEN * index as usize
}

/// Creates and initializes one Bell Region without replacing an existing path.
///
/// Every slot starts at one: no waiting loop has armed that slot yet. `epoch`
/// is diagnostic only and never participates in any correctness decision.
pub fn create_bell_region(
    path: &Path,
    slot_count: NonZeroU32,
    epoch: u64,
) -> Result<(), BellError> {
    let region_len = bell_region_len(slot_count)?;
    let mut bytes =
        vec![0_u8; usize::try_from(region_len).map_err(|_| BellError::MappingRangeInvalid)?];
    bytes[..BELL_MAGIC.len()].copy_from_slice(&BELL_MAGIC);
    write_u32(&mut bytes, BELL_VERSION_OFFSET, BELL_FORMAT_VERSION);
    write_u32(&mut bytes, BELL_SLOT_COUNT_OFFSET, slot_count.get());
    write_u64(&mut bytes, BELL_EPOCH_OFFSET, epoch);
    for index in 0..slot_count.get() {
        write_u32(&mut bytes, slot_offset(index), SLOT_NOTIFIED);
    }
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| io_error("create Bell Region", source))?;
    file.write_all(&bytes)
        .map_err(|source| io_error("initialize Bell Region", source))?;
    Ok(())
}

/// Reads the diagnostic epoch of an existing Bell Region, if it is readable.
pub fn read_bell_region_epoch(path: &Path) -> Option<u64> {
    let mut header = [0_u8; BELL_HEADER_LEN];
    std::fs::File::open(path)
        .ok()?
        .read_exact(&mut header)
        .ok()?;
    if header[..BELL_MAGIC.len()] != BELL_MAGIC {
        return None;
    }
    Some(read_u64(&header, BELL_EPOCH_OFFSET))
}

/// One validated Bell Region mapping.
#[derive(Debug)]
pub struct BellRegion {
    mapping: Arc<MmapRaw>,
    slot_count: NonZeroU32,
}

impl BellRegion {
    /// Opens and validates one existing Bell Region.
    ///
    /// # Errors
    ///
    /// Returns [`BellError`] when the file cannot be opened or mapped or
    /// its header, length, padding, or slot state violates the Bell Region
    /// format.
    pub fn open(path: &Path) -> Result<Arc<Self>, BellError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|source| io_error("open Bell Region", source))?;
        let file_len = file
            .metadata()
            .map_err(|source| io_error("read Bell Region metadata", source))?
            .len();
        let mapping = Arc::new(
            MmapRaw::map_raw(&file).map_err(|source| io_error("map Bell Region", source))?,
        );
        if mapping.len() < BELL_HEADER_LEN {
            return Err(BellError::RegionTooShort);
        }
        if !mapping
            .as_ptr()
            .wrapping_add(BELL_HEADER_LEN)
            .cast::<u32>()
            .is_aligned()
        {
            return Err(BellError::MappingRangeInvalid);
        }
        let mut header = [0_u8; BELL_HEADER_LEN];
        copy_from_mapping(&mapping, 0, &mut header);
        if header[..BELL_MAGIC.len()] != BELL_MAGIC {
            return Err(BellError::Magic);
        }
        if read_u32(&header, BELL_VERSION_OFFSET) != BELL_FORMAT_VERSION {
            return Err(BellError::UnsupportedFormatVersion);
        }
        let slot_count = NonZeroU32::new(read_u32(&header, BELL_SLOT_COUNT_OFFSET))
            .ok_or(BellError::SlotCount)?;
        if header[BELL_HEADER_PADDING_START..]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(BellError::Padding);
        }
        let region_len = bell_region_len(slot_count)?;
        if file_len != region_len || (mapping.len() as u64) < region_len {
            return Err(BellError::RegionLength);
        }
        if !slot_state_is_valid(&mapping, slot_count) {
            return Err(BellError::SlotState);
        }
        if !slot_padding_is_zero(&mapping, slot_count) {
            return Err(BellError::Padding);
        }
        Ok(Arc::new(Self {
            mapping,
            slot_count,
        }))
    }

    /// Returns how many doorbells this region holds.
    #[must_use]
    pub const fn slot_count(&self) -> NonZeroU32 {
        self.slot_count
    }

    /// Builds the doorbell of one waiting loop inside this region.
    ///
    /// # Errors
    ///
    /// Returns [`BellError::SlotOutOfRange`] when `index` is at or beyond
    /// [`Self::slot_count`].
    pub fn loop_bell(self: &Arc<Self>, index: u32) -> Result<Arc<LoopBell>, BellError> {
        Ok(LoopBell::new(self.slot(index)?))
    }

    /// Resolves one in-range slot of this region.
    ///
    /// # Errors
    ///
    /// Returns [`BellError::SlotOutOfRange`] when `index` is at or beyond
    /// [`Self::slot_count`].
    pub(crate) fn slot(self: &Arc<Self>, index: u32) -> Result<BellSlot, BellError> {
        if index >= self.slot_count.get() {
            return Err(BellError::SlotOutOfRange);
        }
        Ok(BellSlot {
            region: Arc::clone(self),
            index,
        })
    }
}

fn slot_padding_is_zero(mapping: &MmapRaw, slot_count: NonZeroU32) -> bool {
    let mut padding = [0_u8; BELL_SLOT_LEN - size_of::<u32>()];
    for index in 0..slot_count.get() {
        copy_from_mapping(mapping, slot_offset(index) + size_of::<u32>(), &mut padding);
        if padding.iter().any(|byte| *byte != 0) {
            return false;
        }
    }
    true
}

fn slot_state_is_valid(mapping: &MmapRaw, slot_count: NonZeroU32) -> bool {
    for index in 0..slot_count.get() {
        if load_atomic_u32(mapping, slot_offset(index), SLOT_SNAPSHOT_ORDERING) > SLOT_NOTIFIED {
            return false;
        }
    }
    true
}

/// One doorbell slot that any peer may ring.
#[derive(Clone, Debug)]
pub(crate) struct BellSlot {
    region: Arc<BellRegion>,
    index: u32,
}

impl BellSlot {
    const fn offset(&self) -> usize {
        slot_offset(self.index)
    }

    /// Returns this slot's ordinal inside its Bell Region.
    ///
    /// A waiting loop publishes this integer into the Queue header of every
    /// Queue it waits on, so its peers can ring this exact slot.
    pub(crate) const fn index(&self) -> u32 {
        self.index
    }

    /// Publishes one notification and wakes the slot owner only when it was armed.
    ///
    /// # Errors
    ///
    /// Returns [`BellError::SlotState`] when the slot word is neither zero
    /// nor one, or [`BellError`] when the platform wake fails.
    pub fn ring(&self) -> Result<(), BellError> {
        let previous = self.publish_notification()?;
        if previous == SLOT_ARMED {
            self.wake()?;
        }
        Ok(())
    }

    /// Arms this slot, the state a parked waiting loop leaves it in.
    ///
    /// This is the one implementation of "arm one doorbell". The loop that owns
    /// the slot runs it under its own pending hint, so a ring that races the arm
    /// is never swallowed, and repository test support runs the same operation
    /// instead of writing the word a second way.
    ///
    /// # Errors
    ///
    /// Returns [`BellError::SlotState`] when the slot word is neither zero
    /// nor one.
    pub(crate) fn arm(&self) -> Result<(), BellError> {
        let previous = swap_atomic_u32(
            &self.region.mapping,
            self.offset(),
            SLOT_ARMED,
            SLOT_ARM_ORDERING,
        );
        if previous > SLOT_NOTIFIED {
            return Err(BellError::SlotState);
        }
        Ok(())
    }

    /// Wakes the slot owner unconditionally, without reading the slot value.
    ///
    /// A ring that skipped this wake because the slot already read one leaves a
    /// permanently armed owner asleep. Recovery from a departed peer therefore
    /// needs an entry that never takes that shortcut. Waking an address with no
    /// waiter is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`BellError::SlotState`] when the slot word is neither zero
    /// nor one, or [`BellError`] when the platform wake fails.
    pub(crate) fn force_ring(&self) -> Result<(), BellError> {
        self.publish_notification()?;
        self.wake()
    }

    fn publish_notification(&self) -> Result<u32, BellError> {
        let previous = swap_atomic_u32(
            &self.region.mapping,
            self.offset(),
            SLOT_NOTIFIED,
            SLOT_NOTIFY_ORDERING,
        );
        if previous > SLOT_NOTIFIED {
            return Err(BellError::SlotState);
        }
        Ok(previous)
    }

    fn wake(&self) -> Result<(), BellError> {
        #[cfg(all(
            any(test, feature = "repository-test-support"),
            not(feature = "loom-model")
        ))]
        {
            platform::wake_with_test_injection(
                &self.region.mapping,
                self.offset(),
                BELL_NOTIFY_OPERATION,
            )
        }
        #[cfg(not(all(
            any(test, feature = "repository-test-support"),
            not(feature = "loom-model")
        )))]
        platform::wake(&self.region.mapping, self.offset(), BELL_NOTIFY_OPERATION)
    }
}

/// One waiting loop's doorbell: the slot it parks on and its process-local hint.
///
/// The hint and the slot word are published under one mutex, exactly as the
/// per-Queue interruption was. That mutual exclusion is what makes an
/// interrupt either arrive before the arm or leave the slot at one, so the
/// kernel wait can never miss it.
#[derive(Debug)]
pub struct LoopBell {
    slot: BellSlot,
    pending: Mutex<bool>,
    /// Counts platform wakes of this loop that found no subscribed fact.
    ///
    /// One doorbell serves every condition of the loop, so a ring for a fact
    /// this loop does not cover, or an unconditional recovery wake, costs one
    /// full recheck and produces nothing. The owning loop reports the count.
    spurious_wakes: AtomicU64,
    /// Counts platform waits this loop entered, and how many had a deadline.
    ///
    /// A sleep deadline is only ever a business timer's exact remaining
    /// duration. An idle loop that sleeps for a fixed interval instead would
    /// show up here as timed waits it has no timer for.
    platform_waits: AtomicU64,
    timed_platform_waits: AtomicU64,
}

impl LoopBell {
    /// Creates the shared doorbell of one waiting loop.
    pub(crate) fn new(slot: BellSlot) -> Arc<Self> {
        Arc::new(Self {
            slot,
            pending: Mutex::new(false),
            spurious_wakes: AtomicU64::new(0),
            platform_waits: AtomicU64::new(0),
            timed_platform_waits: AtomicU64::new(0),
        })
    }

    /// Signals that a subscribed condition changed, without requesting interruption.
    ///
    /// # Errors
    ///
    /// Returns an invalid live slot-state or platform wake error; either can
    /// occur after the notification has been published.
    pub fn ring(&self) -> Result<(), BellError> {
        self.slot.ring()
    }

    /// Returns the slot this loop publishes so peers can ring it.
    pub(crate) fn slot(&self) -> &BellSlot {
        &self.slot
    }

    /// Returns how many platform wakes this loop observed with no work waiting.
    pub fn spurious_wakes(&self) -> u64 {
        self.spurious_wakes.load(Ordering::Relaxed)
    }

    /// Returns a process-local handle that interrupts this loop's wait.
    pub fn interrupter(self: &Arc<Self>) -> BellInterrupter {
        BellInterrupter {
            bell: Arc::downgrade(self),
        }
    }

    /// Parks until `condition` holds, a local interrupt arrives, or the wait expires.
    ///
    /// Every subscribed condition must be part of `condition`; the doorbell only
    /// says that something may have changed.
    ///
    /// # Errors
    ///
    /// Returns the condition's own error, or [`BellError`] when a condition read
    /// or the platform wait fails.
    pub fn wait_until<E: From<BellError>>(
        &self,
        condition: impl FnMut() -> Result<bool, E>,
    ) -> Result<WaitOutcome, E> {
        wait_indefinitely(self, condition)
    }

    /// Parks with one explicit local timeout instead of an indefinite wait.
    ///
    /// # Errors
    ///
    /// Returns the condition's own error, or [`BellError`] when a condition read
    /// or the platform wait fails.
    pub fn wait_until_for<E: From<BellError>>(
        &self,
        timeout: Duration,
        condition: impl FnMut() -> Result<bool, E>,
    ) -> Result<TimedWaitOutcome, E> {
        wait_for(self, timeout, condition)
    }

    fn clear_pending(&self) {
        let mut pending = self.lock_pending();
        *pending = false;
    }

    /// Sets the local hint, then rings this loop's own doorbell.
    fn interrupt(&self) -> Result<(), BellError> {
        let mut pending = self.lock_pending();
        *pending = true;
        let bell = self.slot.ring();
        drop(pending);
        bell
    }

    /// Wakes this loop unconditionally for a departed peer.
    pub(crate) fn force_wake(&self) -> Result<(), BellError> {
        self.slot.force_ring()
    }

    fn lock_pending(&self) -> MutexGuard<'_, bool> {
        // This mutex protects only a coalescing hint and the arm-vs-notify
        // ordering. Either boolean value remains safe if a panic poisoned it.
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl WaitFront for LoopBell {
    fn take_wait_interruption(&self) -> Option<WaitInterruption> {
        let mut pending = self.lock_pending();
        let interrupted = *pending;
        *pending = false;
        drop(pending);
        interrupted.then_some(WaitInterruption)
    }

    fn arm_or_take_wait_interruption(&self) -> Result<WaitArmOutcome, BellError> {
        let pending = self.lock_pending();
        if *pending {
            drop(pending);
            self.clear_pending();
            return Ok(WaitArmOutcome::Interrupted);
        }
        // Armed while the pending hint is still held: a ring ordered after the
        // arm either sees the armed word or leaves the hint set.
        let armed = self.slot.arm();
        drop(pending);
        armed?;
        Ok(WaitArmOutcome::Armed)
    }

    fn publish_wait_notification(&self) -> Result<(), BellError> {
        self.slot.publish_notification().map(drop)
    }

    fn wait_bell(&self) -> Result<SignalWaitOutcome, BellError> {
        self.platform_waits.fetch_add(1, Ordering::Relaxed);
        platform::wait(
            &self.slot.region.mapping,
            self.slot.offset(),
            BELL_ARM_WAIT_OPERATION,
        )
    }

    fn wait_bell_for(&self, timeout: Duration) -> Result<SignalWaitOutcome, BellError> {
        self.platform_waits.fetch_add(1, Ordering::Relaxed);
        self.timed_platform_waits.fetch_add(1, Ordering::Relaxed);
        platform::wait_for(
            &self.slot.region.mapping,
            self.slot.offset(),
            timeout,
            BELL_ARM_WAIT_OPERATION,
        )
    }

    fn observe_spurious_wake(&self) {
        self.spurious_wakes.fetch_add(1, Ordering::Relaxed);
    }
}

/// A process-local handle that interrupts one waiting loop.
#[derive(Clone, Debug)]
pub struct BellInterrupter {
    bell: Weak<LoopBell>,
}

impl BellInterrupter {
    /// Interrupts the loop's current wait or its immediately following wait.
    ///
    /// Calling this after the bound loop has been dropped is a no-op.
    ///
    /// # Errors
    ///
    /// Returns [`BellError`] when the platform cannot wake an already
    /// blocked owner. The caller must treat that loop as failed.
    pub fn interrupt(&self) -> Result<(), BellError> {
        match self.bell.upgrade() {
            Some(bell) => bell.interrupt(),
            None => Ok(()),
        }
    }

    /// Wakes the loop unconditionally after its peer must stop waiting.
    ///
    /// # Errors
    ///
    /// Returns [`BellError`] when the platform cannot wake an already
    /// blocked owner.
    pub fn force_wake(&self) -> Result<(), BellError> {
        match self.bell.upgrade() {
            Some(bell) => bell.force_wake(),
            None => Ok(()),
        }
    }
}

/// Makes the next platform wake of a bell slot fail.
///
/// Test support for the recovery path: a wake may fail after the ring was
/// published, and the loop must survive it. Declared here, next to the bell it
/// belongs to, so every test hook of this module sits in one place.
#[cfg(all(
    any(test, feature = "repository-test-support"),
    not(feature = "loom-model")
))]
pub fn fail_next_platform_wake() {
    platform::fail_next_wake();
}

/// Drops the next platform wake of a bell slot after its ring was published.
///
/// Test support for the interleaving a departing peer leaves behind: the peer
/// published its notification and exited before its wake reached the kernel, so
/// the wire state is one notified slot whose owner still sleeps and no caller
/// ever saw an error. Only a force wake can release that owner.
#[cfg(all(
    any(test, feature = "repository-test-support"),
    not(feature = "loom-model")
))]
pub fn drop_next_platform_wake() {
    platform::drop_next_wake();
}

/// Parks inside the next platform wake, after its ring and before the platform call.
///
/// Test support for the strong kill the recovery protocol must survive: the
/// peer published its notification, its wake had not reached the kernel, and
/// its process died there. The parking process is killed by its own test.
#[cfg(all(
    any(test, feature = "repository-test-support"),
    not(feature = "loom-model")
))]
pub fn park_next_platform_wake() {
    platform::park_next_wake();
}

#[cfg(any(test, feature = "repository-test-support"))]
mod test_support {
    use super::*;

    impl LoopBell {
        /// Returns how many platform waits this loop entered, and how many had a deadline.
        ///
        /// Test-only: this is how a test asserts that an idle loop slept without a
        /// deadline, instead of trusting that a fixed interval was long enough.
        pub fn platform_waits(&self) -> (u64, u64) {
            (
                self.platform_waits.load(Ordering::Relaxed),
                self.timed_platform_waits.load(Ordering::Relaxed),
            )
        }
    }
}

#[cfg(all(test, not(feature = "loom-model")))]
mod tests {
    use std::io;
    use std::num::NonZeroU32;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;

    const WAIT_LIMIT: Duration = Duration::from_secs(10);

    /// How long the test waits before reading one park or wake as settled.
    ///
    /// A parked loop has nothing else to do, so a dropped wake leaves it asleep
    /// for any bounded window, and a wake that did arrive releases it far inside
    /// this one.
    const SETTLE: Duration = Duration::from_millis(100);

    #[test]
    fn a_dropped_wake_leaves_the_parked_loop_asleep_until_a_force_wake() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let region_path = directory.path().join("loops.bells");
        create_bell_region(
            &region_path,
            NonZeroU32::new(1).ok_or_else(|| io::Error::other("slot"))?,
            1,
        )
        .map_err(io::Error::other)?;
        // Two opens of one region model the loop and its peer, which map the
        // same slot from separate processes.
        let owner = BellRegion::open(&region_path).map_err(io::Error::other)?;
        let bell = owner.loop_bell(0).map_err(io::Error::other)?;
        let peer = BellRegion::open(&region_path).map_err(io::Error::other)?;
        let peer_slot = peer.slot(0).map_err(io::Error::other)?;

        let published = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&published);
        let parked = Arc::clone(&bell);
        let owner_loop = thread::spawn(move || {
            parked.wait_until(|| Ok::<bool, BellError>(observed.load(Ordering::Acquire)))
        });

        wait_until_armed(&region_path)?;
        wait_until_platform_wait(&bell)?;
        thread::sleep(SETTLE);

        // The peer publishes its fact, rings, and its wake never reaches the
        // kernel, so the ring reports success with the owner still asleep.
        drop_next_platform_wake();
        published.store(true, Ordering::Release);
        peer_slot.ring().map_err(io::Error::other)?;

        assert_eq!(
            slot_word(&region_path)?,
            SLOT_NOTIFIED,
            "the ring did not publish its notification"
        );
        thread::sleep(SETTLE);
        assert!(
            !owner_loop.is_finished(),
            "a ring whose wake was dropped still released the parked loop"
        );

        bell.force_wake().map_err(io::Error::other)?;
        let outcome = owner_loop
            .join()
            .map_err(|_| io::Error::other("the parked loop panicked"))?;
        assert_eq!(outcome.map_err(io::Error::other)?, WaitOutcome::Ready);
        Ok(())
    }

    /// Waits until the loop owns a slot its peer may ring.
    fn wait_until_armed(region_path: &Path) -> io::Result<()> {
        let deadline = Instant::now() + WAIT_LIMIT;
        while slot_word(region_path)? != SLOT_ARMED {
            if Instant::now() >= deadline {
                return Err(io::Error::other("the loop never armed its doorbell"));
            }
            thread::yield_now();
        }
        Ok(())
    }

    /// Waits until the loop has entered a platform wait on its doorbell.
    fn wait_until_platform_wait(bell: &Arc<LoopBell>) -> io::Result<()> {
        let deadline = Instant::now() + WAIT_LIMIT;
        while bell.platform_waits().0 == 0 {
            if Instant::now() >= deadline {
                return Err(io::Error::other("the loop never entered a platform wait"));
            }
            thread::yield_now();
        }
        Ok(())
    }

    /// Reads the first doorbell slot word the way the format defines it.
    fn slot_word(region_path: &Path) -> io::Result<u32> {
        use std::fs::File;
        use std::os::unix::fs::FileExt;

        let file = File::open(region_path)?;
        let mut word = [0_u8; 4];
        file.read_exact_at(&mut word, slot_offset(0) as u64)?;
        Ok(u32::from_le_bytes(word))
    }

    #[test]
    fn ringing_one_slot_preserves_its_neighbour() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("loops.bells");
        create_bell_region(&path, NonZeroU32::new(2).ok_or("two slots are nonzero")?, 0)?;
        let region = BellRegion::open(&path)?;
        let first = region.slot(0)?;
        let second = region.slot(1)?;
        second.arm()?;
        let before = std::fs::read(&path)?;
        first.arm()?;
        first.ring()?;
        first.ring()?;
        assert_eq!(std::fs::read(&path)?, before);
        second.ring()?;
        Ok(())
    }
}
