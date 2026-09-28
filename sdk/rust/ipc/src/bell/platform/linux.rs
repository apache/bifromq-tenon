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

//! Linux futex wait and wake implementation.

use std::io;
use std::time::Duration;

use memmap2::MmapRaw;

use super::super::BellError;
use super::super::SLOT_ARMED;
use super::super::mapping::io_error;
use super::super::wait::SignalWaitOutcome;

pub(in crate::bell) fn wait(
    mapping: &MmapRaw,
    offset: usize,
    operation: &'static str,
) -> Result<SignalWaitOutcome, BellError> {
    let pointer = mapping.as_mut_ptr().wrapping_add(offset).cast::<u32>();
    // SAFETY: the caller proved that `offset` addresses a live, writable,
    // 32-bit-aligned word of one shared word that every participant accesses
    // atomically under the same protocol. The no-timeout shared FUTEX_WAIT
    // operation reads that word only for the duration of this call; the
    // mapping remains owned by `mapping`.
    let result = unsafe {
        libc::syscall(
            libc::SYS_futex,
            pointer,
            libc::FUTEX_WAIT,
            SLOT_ARMED,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0_u32,
        )
    };
    if result == 0 {
        return Ok(SignalWaitOutcome::Progress);
    }

    let source = io::Error::last_os_error();
    match source.raw_os_error() {
        Some(libc::EAGAIN) => Ok(SignalWaitOutcome::Progress),
        Some(libc::EINTR) => Ok(SignalWaitOutcome::Interrupted),
        _ => Err(io_error(operation, source)),
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
    let seconds = libc::time_t::try_from(timeout.as_secs()).unwrap_or(libc::time_t::MAX);
    let nanoseconds = libc::c_long::from(timeout.subsec_nanos());
    let timeout = libc::timespec {
        tv_sec: seconds,
        tv_nsec: nanoseconds,
    };
    let pointer = mapping.as_mut_ptr().wrapping_add(offset).cast::<u32>();
    // SAFETY: The pointer proof is identical to `wait`. `timeout` is a live,
    // normalized relative timespec for this call, and FUTEX_WAIT reads neither
    // pointer after returning.
    let result = unsafe {
        libc::syscall(
            libc::SYS_futex,
            pointer,
            libc::FUTEX_WAIT,
            SLOT_ARMED,
            &raw const timeout,
            std::ptr::null::<u32>(),
            0_u32,
        )
    };
    if result == 0 {
        return Ok(SignalWaitOutcome::Progress);
    }

    let source = io::Error::last_os_error();
    match source.raw_os_error() {
        Some(libc::EAGAIN) => Ok(SignalWaitOutcome::Progress),
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
    let pointer = mapping.as_mut_ptr().wrapping_add(offset).cast::<u32>();
    // SAFETY: The pointer proof is identical to `wait`. Shared FUTEX_WAKE does
    // not dereference any additional pointer and may wake at most the single
    // owner that one slot or one Queue direction permits.
    let result = unsafe {
        libc::syscall(
            libc::SYS_futex,
            pointer,
            libc::FUTEX_WAKE,
            1_u32,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0_u32,
        )
    };
    if result >= 0 {
        Ok(())
    } else {
        Err(io_error(operation, io::Error::last_os_error()))
    }
}
