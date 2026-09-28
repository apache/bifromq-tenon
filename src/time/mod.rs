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

//! Shared time budget that is never restarted between waits.

use std::time::Duration;

use tokio::time::Instant;

/// One cancellation-safe deadline whose elapsed time is never restarted.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Deadline {
    started_at: Instant,
    timeout: Duration,
}

impl Deadline {
    /// Starts one deadline at the current monotonic instant.
    #[must_use]
    pub(crate) fn start(timeout: Duration) -> Self {
        Self {
            started_at: Instant::now(),
            timeout,
        }
    }

    /// Returns a sleep for only the time still remaining on this deadline.
    pub(crate) fn wait(self) -> tokio::time::Sleep {
        tokio::time::sleep(
            self.timeout
                .saturating_sub(Instant::now().saturating_duration_since(self.started_at)),
        )
    }

    /// The remaining request budget without extending its deadline.
    pub(crate) fn remaining(self) -> Duration {
        self.timeout
            .saturating_sub(Instant::now().saturating_duration_since(self.started_at))
    }

    /// Tests the same absolute budget without constructing or polling a timer.
    pub(crate) fn has_elapsed(self) -> bool {
        Instant::now().duration_since(self.started_at) >= self.timeout
    }
}
