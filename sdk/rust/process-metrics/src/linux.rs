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

//! Reads the kernel's approximate current RSS without scanning mappings.
#![allow(unsafe_code)]

use std::io;

pub(super) fn resident_bytes() -> io::Result<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm")?;
    // SAFETY: sysconf takes a constant query selector and no pointers.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page_size = u64::try_from(page_size)
        .map_err(|_| io::Error::other("process page size is unavailable"))?;
    let pages = statm
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| io::Error::other("process RSS could not be parsed"))?;
    pages
        .checked_mul(page_size)
        .ok_or_else(|| io::Error::other("process RSS exceeds byte representation"))
}
