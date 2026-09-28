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

//! Reads the kernel's current resident size for this process alone.
#![allow(unsafe_code)]

use std::io;
use std::mem::MaybeUninit;

#[allow(
    deprecated,
    reason = "the installed libc exposes the required stable Mach ABI without another dependency"
)]
pub(super) fn resident_bytes() -> io::Result<u64> {
    let mut info = MaybeUninit::<libc::mach_task_basic_info>::uninit();
    let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
    // SAFETY: The current task port is valid for this process. The output is
    // aligned and has exactly the size required by MACH_TASK_BASIC_INFO. The
    // kernel writes it synchronously; no pointer escapes this call.
    let result = unsafe {
        libc::task_info(
            libc::mach_task_self(),
            libc::MACH_TASK_BASIC_INFO,
            info.as_mut_ptr().cast(),
            &mut count,
        )
    };
    if result != libc::KERN_SUCCESS || count != libc::MACH_TASK_BASIC_INFO_COUNT {
        return Err(io::Error::other("process RSS could not be sampled"));
    }
    // SAFETY: The successful call and exact count confirm the entire structure was written.
    Ok(unsafe { info.assume_init() }.resident_size)
}
