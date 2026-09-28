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

//! macOS shared-address wait and wake implementation.

use std::io;
use std::time::Duration;

use memmap2::MmapRaw;

use super::super::BellError;
use super::super::SLOT_ARMED;
use super::super::mapping::io_error;
use super::super::wait::SignalWaitOutcome;

const OS_SYNC_WAIT_ON_ADDRESS_SHARED: u32 = 1;
const OS_SYNC_WAKE_BY_ADDRESS_SHARED: u32 = 1;
const OS_CLOCK_MACH_ABSOLUTE_TIME: u32 = 32;

pub(in crate::bell) fn wait(
    mapping: &MmapRaw,
    offset: usize,
    operation: &'static str,
) -> Result<SignalWaitOutcome, BellError> {
    let pointer = mapping
        .as_mut_ptr()
        .wrapping_add(offset)
        .cast::<libc::c_void>();
    // SAFETY: the caller proved that `offset` addresses a live, writable,
    // 32-bit-aligned shared word. The shared wait call compares that word only
    // for this call; the mapping remains alive, and every participant uses
    // atomic access and the same shared flag.
    let result = unsafe {
        os_sync_wait_on_address(
            pointer,
            u64::from(SLOT_ARMED),
            std::mem::size_of::<u32>(),
            OS_SYNC_WAIT_ON_ADDRESS_SHARED,
        )
    };
    if result >= 0 {
        return Ok(SignalWaitOutcome::Progress);
    }

    let source = io::Error::last_os_error();
    if source.raw_os_error() == Some(libc::EINTR) {
        Ok(SignalWaitOutcome::Interrupted)
    } else {
        Err(io_error(operation, source))
    }
}

pub(in crate::bell) fn wait_for(
    mapping: &MmapRaw,
    offset: usize,
    timeout: Duration,
    operation: &'static str,
) -> Result<SignalWaitOutcome, BellError> {
    if timeout.is_zero() {
        return Ok(SignalWaitOutcome::TimedOut);
    }
    let timeout_nanoseconds = u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX);
    let pointer = mapping
        .as_mut_ptr()
        .wrapping_add(offset)
        .cast::<libc::c_void>();
    // SAFETY: The pointer proof is identical to `wait`. The relative timeout
    // is nonzero and expressed against Mach absolute time as required by the
    // macOS API; no pointer escapes this call.
    let result = unsafe {
        os_sync_wait_on_address_with_timeout(
            pointer,
            u64::from(SLOT_ARMED),
            std::mem::size_of::<u32>(),
            OS_SYNC_WAIT_ON_ADDRESS_SHARED,
            OS_CLOCK_MACH_ABSOLUTE_TIME,
            timeout_nanoseconds,
        )
    };
    if result >= 0 {
        return Ok(SignalWaitOutcome::Progress);
    }

    let source = io::Error::last_os_error();
    match source.raw_os_error() {
        Some(libc::EINTR) => Ok(SignalWaitOutcome::Interrupted),
        Some(libc::ETIMEDOUT) => Ok(SignalWaitOutcome::TimedOut),
        _ => Err(io_error(operation, source)),
    }
}

pub(in crate::bell) fn wake(
    mapping: &MmapRaw,
    offset: usize,
    operation: &'static str,
) -> Result<(), BellError> {
    let pointer = mapping
        .as_mut_ptr()
        .wrapping_add(offset)
        .cast::<libc::c_void>();
    // SAFETY: The pointer proof is identical to `wait`. The matching shared wake
    // operation does not dereference another pointer and wakes at most the
    // single owner that one slot permits.
    let result = unsafe {
        os_sync_wake_by_address_any(
            pointer,
            std::mem::size_of::<u32>(),
            OS_SYNC_WAKE_BY_ADDRESS_SHARED,
        )
    };
    if result == 0 {
        return Ok(());
    }

    let source = io::Error::last_os_error();
    if source.raw_os_error() == Some(libc::ENOENT) {
        Ok(())
    } else {
        Err(io_error(operation, source))
    }
}

#[link(name = "System")]
unsafe extern "C" {
    fn os_sync_wait_on_address(
        address: *mut libc::c_void,
        value: u64,
        size: usize,
        flags: u32,
    ) -> libc::c_int;

    fn os_sync_wait_on_address_with_timeout(
        address: *mut libc::c_void,
        value: u64,
        size: usize,
        flags: u32,
        clock_id: u32,
        timeout_nanoseconds: u64,
    ) -> libc::c_int;

    fn os_sync_wake_by_address_any(
        address: *mut libc::c_void,
        size: usize,
        flags: u32,
    ) -> libc::c_int;
}
