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

use std::io;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use tenon_ipc::queue::{QueueWaiter, queue_waiter_is_armed};

/// Waits until one Queue endpoint's waiting loop has parked on its doorbell.
///
/// A test that must observe or interrupt a blocked endpoint first proves that
/// endpoint reached the park: the Queue header names the slot that loop
/// published, and that slot's word says whether the loop is armed.
pub(crate) fn wait_for_armed_loop(
    queue_path: &Path,
    region_path: &Path,
    waiter: QueueWaiter,
) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if queue_waiter_is_armed(queue_path, region_path, waiter).map_err(io::Error::other)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other("Queue waiter did not arm its doorbell"));
        }
        thread::sleep(Duration::from_millis(1));
    }
}
