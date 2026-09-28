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

//! Raw access to one live memory mapping.
//!
//! Every caller proves two facts before calling in: the addressed range lies
//! inside the live map, and an addressed word is aligned for the atomic width
//! it is accessed with. This module owns only how one word or one byte range
//! moves in and out of a mapping. Which offset holds which field, and which
//! ordering that field needs, belong to the format that defines it.

use std::io;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use memmap2::MmapRaw;

use super::BellError;

/// Reports one operating-system failure through this module's error vocabulary.
pub(crate) fn io_error(operation: &'static str, source: io::Error) -> BellError {
    BellError::Io { operation, source }
}

pub(crate) fn load_atomic_u64(mapping: &MmapRaw, offset: usize, ordering: Ordering) -> u64 {
    let pointer = mapping.as_mut_ptr().wrapping_add(offset).cast::<u64>();
    // SAFETY: The caller proved that this exact address is a live, writable,
    // `AtomicU64`-aligned word inside `mapping`, and that every participant
    // accesses it atomically only. The temporary reference cannot outlive the
    // mapping or escape this function.
    unsafe { load_atomic_u64_pointer(pointer, ordering) }
}

pub(crate) fn store_atomic_u64(mapping: &MmapRaw, offset: usize, value: u64, ordering: Ordering) {
    let pointer = mapping.as_mut_ptr().wrapping_add(offset).cast::<u64>();
    // SAFETY: The alignment, lifetime, and atomic-only proof is identical to
    // `load_atomic_u64`. The caller additionally holds this word's only
    // protocol-designated store.
    unsafe { store_atomic_u64_pointer(pointer, value, ordering) }
}

pub(crate) fn load_atomic_u32(mapping: &MmapRaw, offset: usize, ordering: Ordering) -> u32 {
    let pointer = mapping.as_mut_ptr().wrapping_add(offset).cast::<u32>();
    // SAFETY: The caller proved that this exact address is a live, writable,
    // `AtomicU32`-aligned word inside `mapping`, and that every participant
    // accesses it atomically only. The temporary reference cannot outlive the
    // mapping or escape this function.
    unsafe { load_atomic_u32_pointer(pointer, ordering) }
}

pub(crate) fn swap_atomic_u32(
    mapping: &MmapRaw,
    offset: usize,
    value: u32,
    ordering: Ordering,
) -> u32 {
    let pointer = mapping.as_mut_ptr().wrapping_add(offset).cast::<u32>();
    // SAFETY: The alignment, lifetime, and atomic-only proof is identical to
    // `load_atomic_u32`. Several peers may update one shared signal word, and
    // atomic swap provides its single modification order.
    unsafe { swap_atomic_u32_pointer(pointer, value, ordering) }
}

pub(crate) fn store_atomic_u32(mapping: &MmapRaw, offset: usize, value: u32, ordering: Ordering) {
    let pointer = mapping.as_mut_ptr().wrapping_add(offset).cast::<u32>();
    // SAFETY: The alignment, lifetime, and atomic-only proof is identical to
    // `load_atomic_u32`. The caller additionally holds this word's only
    // protocol-designated store.
    unsafe { store_atomic_u32_pointer(pointer, value, ordering) }
}

pub(crate) fn copy_from_mapping(mapping: &MmapRaw, offset: usize, destination: &mut [u8]) {
    // SAFETY: Every caller proves that `offset..offset + destination.len()` is
    // inside this live mapping, and that no byte of that range is written
    // concurrently for the duration of this copy. `destination` is caller-owned
    // memory and cannot overlap this private map.
    unsafe {
        copy_nonoverlapping_bytes(
            mapping.as_ptr().add(offset),
            destination.as_mut_ptr(),
            destination.len(),
        );
    }
}

pub(crate) fn copy_from_mapping_uninit(
    mapping: &MmapRaw,
    offset: usize,
    destination: &mut [MaybeUninit<u8>],
) {
    // SAFETY: The range and concurrency proof is identical to
    // `copy_from_mapping`. The private mapping cannot overlap caller-owned
    // destination capacity, and copying `u8` values initializes every element.
    unsafe {
        copy_nonoverlapping_bytes(
            mapping.as_ptr().add(offset),
            destination.as_mut_ptr().cast(),
            destination.len(),
        );
    }
}

/// # Safety
///
/// `pointer` must be a live, correctly aligned `u64` inside a mapping that
/// every participant accesses atomically for the duration of this call.
pub(crate) unsafe fn load_atomic_u64_pointer(pointer: *mut u64, ordering: Ordering) -> u64 {
    // SAFETY: The caller guarantees a live, correctly aligned word that is
    // accessed atomically for the duration of the returned temporary reference.
    unsafe { AtomicU64::from_ptr(pointer).load(ordering) }
}

/// # Safety
///
/// `pointer` must be a live, correctly aligned `u64` inside a mapping that
/// every participant accesses atomically, with one protocol owner for this
/// store.
pub(crate) unsafe fn store_atomic_u64_pointer(pointer: *mut u64, value: u64, ordering: Ordering) {
    // SAFETY: The caller guarantees a live, correctly aligned word that is
    // accessed atomically and has one protocol-designated store owner.
    unsafe { AtomicU64::from_ptr(pointer).store(value, ordering) }
}

/// # Safety
///
/// `pointer` must be a live, correctly aligned `u32` inside a mapping that
/// every participant accesses atomically for the duration of this call.
pub(crate) unsafe fn load_atomic_u32_pointer(pointer: *mut u32, ordering: Ordering) -> u32 {
    // SAFETY: The caller guarantees a live, correctly aligned word that is
    // accessed atomically for the duration of the returned temporary reference.
    unsafe { AtomicU32::from_ptr(pointer).load(ordering) }
}

/// # Safety
///
/// `pointer` must be a live, correctly aligned `u32` inside a mapping that
/// every participant accesses atomically.
pub(crate) unsafe fn swap_atomic_u32_pointer(
    pointer: *mut u32,
    value: u32,
    ordering: Ordering,
) -> u32 {
    // SAFETY: The caller guarantees a live, correctly aligned word that is
    // accessed atomically by every participant.
    unsafe { AtomicU32::from_ptr(pointer).swap(value, ordering) }
}

/// # Safety
///
/// `pointer` must be a live, correctly aligned `u32` inside a mapping that
/// every participant accesses atomically, with one protocol owner for this
/// store.
pub(crate) unsafe fn store_atomic_u32_pointer(pointer: *mut u32, value: u32, ordering: Ordering) {
    // SAFETY: The caller guarantees a live, correctly aligned word that is
    // accessed atomically and has one protocol-designated store owner.
    unsafe { AtomicU32::from_ptr(pointer).store(value, ordering) }
}

/// # Safety
///
/// `source` must point to `length` initialized bytes, `destination` to
/// `length` writable bytes, and the ranges must not overlap.
pub(crate) unsafe fn copy_nonoverlapping_bytes(
    source: *const u8,
    destination: *mut u8,
    length: usize,
) {
    // SAFETY: The caller guarantees a live initialized source, a writable
    // destination of `length` bytes, and proves that the ranges do not overlap.
    unsafe { std::ptr::copy_nonoverlapping(source, destination, length) }
}
