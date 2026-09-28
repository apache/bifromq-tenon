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

//! Samples only the calling process; CPU history spans successful observations.

use cpu_time::ProcessTime;
use std::time::Instant;

#[cfg(target_os = "linux")]
#[path = "linux.rs"]
mod platform;
#[cfg(target_os = "macos")]
#[path = "macos.rs"]
mod platform;

#[derive(Debug, Default)]
/// CPU baseline owned by one process collector.
pub struct ProcessSampler {
    // This is the previous successful sample, not a projection of current state.
    previous: Option<(ProcessTime, Instant)>,
}

impl ProcessSampler {
    /// Reads CPU cores since the last successful sample.
    /// Returns an error when the operating system cannot read the process clock.
    pub fn cpu(&mut self) -> std::io::Result<Option<f64>> {
        Ok(self.observe(Some(ProcessTime::try_now()?), Instant::now()))
    }

    fn observe(&mut self, cpu: Option<ProcessTime>, now: Instant) -> Option<f64> {
        let cpu = cpu?;
        if self
            .previous
            .is_some_and(|(_, previous_at)| now <= previous_at)
        {
            return None;
        }
        self.previous
            .replace((cpu, now))
            .map(|(previous_cpu, previous_at)| {
                cpu.duration_since(previous_cpu).as_secs_f64()
                    / now.duration_since(previous_at).as_secs_f64()
            })
    }
}

/// Reads current own-process RSS in bytes.
pub fn memory() -> std::io::Result<u64> {
    platform::resident_bytes()
}

#[cfg(test)]
mod tests;
