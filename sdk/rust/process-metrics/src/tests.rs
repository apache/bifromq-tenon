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

use super::*;
use std::io;
use std::time::Duration;

#[test]
fn cpu_failure_preserves_the_successful_baseline_and_ratios_can_exceed_one() -> io::Result<()> {
    let mut sampler = ProcessSampler::default();
    let first = ProcessTime::try_now()?;
    let start = Instant::now();
    assert!(sampler.observe(Some(first), start).is_none());
    while first.try_elapsed()? < Duration::from_millis(10) {
        std::hint::spin_loop();
    }
    let second = ProcessTime::try_now()?;
    let elapsed = second.duration_since(first) / 2;
    assert!(sampler.observe(None, start + elapsed / 2).is_none());
    let value = sampler
        .observe(Some(second), start + elapsed)
        .ok_or_else(|| io::Error::other("second successful CPU observation was skipped"))?;
    assert!((1.99..2.01).contains(&value));
    Ok(())
}

#[test]
fn real_process_rss_is_current_and_positive() -> io::Result<()> {
    assert!(memory()? > 0);
    let mut allocation = vec![0_u8; 32 * 1024 * 1024];
    for page in allocation.chunks_mut(4096) {
        page[0] = 1;
    }
    std::hint::black_box(&allocation);
    assert!(memory()? >= 32 * 1024 * 1024);
    Ok(())
}
