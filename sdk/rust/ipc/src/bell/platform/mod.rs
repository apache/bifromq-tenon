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

//! Compile-time selection of the operating-system wait backend.
//!
//! Both backends wait on and wake one 4-byte-aligned word inside a shared
//! mapping. The caller supplies that word's offset; the surrounding protocol,
//! not this layer, decides whether the word belongs to a Queue signal slot or
//! to a Bell Region slot.

use memmap2::MmapRaw;

use super::BellError;

#[cfg(all(
    any(test, feature = "repository-test-support"),
    not(feature = "loom-model")
))]
use std::sync::atomic::{AtomicU8, Ordering};

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
compile_error!("Tenon supports a waiting loop only on Linux and macOS");

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

#[cfg(target_os = "linux")]
pub(super) use linux::{wait, wait_for};
#[cfg(target_os = "macos")]
pub(super) use macos::{wait, wait_for};

#[cfg(all(
    any(test, feature = "repository-test-support"),
    not(feature = "loom-model")
))]
const NO_WAKE_INJECTION: u8 = 0;
/// The next wake reports the failure a calling loop must survive.
#[cfg(all(
    any(test, feature = "repository-test-support"),
    not(feature = "loom-model")
))]
const FAIL_WAKE: u8 = 1;
/// The next wake is dropped, leaving a notified slot whose owner was not woken.
#[cfg(all(
    any(test, feature = "repository-test-support"),
    not(feature = "loom-model")
))]
const DROP_WAKE: u8 = 2;
/// The next wake parks before its platform call instead of making it.
#[cfg(all(
    any(test, feature = "repository-test-support"),
    not(feature = "loom-model")
))]
const PARK_WAKE: u8 = 3;

#[cfg(all(
    any(test, feature = "repository-test-support"),
    not(feature = "loom-model")
))]
static WAKE_INJECTION: AtomicU8 = AtomicU8::new(NO_WAKE_INJECTION);

/// Makes the next platform wake of one slot fail.
#[cfg(all(
    any(test, feature = "repository-test-support"),
    not(feature = "loom-model")
))]
pub(super) fn fail_next_wake() {
    WAKE_INJECTION.store(FAIL_WAKE, Ordering::Release);
}

/// Drops the next platform wake of one slot without calling the platform.
///
/// A peer that publishes a notification and then exits before its wake reaches
/// the kernel leaves the doorbell in exactly this state: one notified slot
/// whose owner still sleeps, and no caller that ever saw an error. Test support
/// needs that state to prove the owner's recovery path.
#[cfg(all(
    any(test, feature = "repository-test-support"),
    not(feature = "loom-model")
))]
pub(super) fn drop_next_wake() {
    WAKE_INJECTION.store(DROP_WAKE, Ordering::Release);
}

/// Parks inside the next wake, before its platform call.
///
/// Test support for a strong kill at the exact boundary the wake protocol owns:
/// the notification is published, the wake has not reached the kernel, and the
/// process dies there. The test that arms this kills its own child, so the
/// parking call never returns.
#[cfg(all(
    any(test, feature = "repository-test-support"),
    not(feature = "loom-model")
))]
pub(super) fn park_next_wake() {
    WAKE_INJECTION.store(PARK_WAKE, Ordering::Release);
}

pub(super) fn wake(
    mapping: &MmapRaw,
    offset: usize,
    operation: &'static str,
) -> Result<(), BellError> {
    #[cfg(target_os = "linux")]
    {
        linux::wake(mapping, offset, operation)
    }
    #[cfg(target_os = "macos")]
    {
        macos::wake(mapping, offset, operation)
    }
}

#[cfg(all(
    any(test, feature = "repository-test-support"),
    not(feature = "loom-model")
))]
pub(super) fn wake_with_test_injection(
    mapping: &MmapRaw,
    offset: usize,
    operation: &'static str,
) -> Result<(), BellError> {
    match WAKE_INJECTION.swap(NO_WAKE_INJECTION, Ordering::AcqRel) {
        FAIL_WAKE => {
            return Err(super::mapping::io_error(
                operation,
                std::io::Error::other("Injected platform wake failure"),
            ));
        }
        DROP_WAKE => return Ok(()),
        PARK_WAKE => loop {
            std::thread::park_timeout(std::time::Duration::from_secs(3_600));
        },
        _ => {}
    }
    wake(mapping, offset, operation)
}
