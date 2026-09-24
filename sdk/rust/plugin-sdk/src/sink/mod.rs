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

//! Sink author interface: one business owner and the identity of each input batch.
//!
//! SinkProgram::run owns process setup and final business cleanup. The private session
//! owns the Instance's one Egress loop and advances only consecutive successful batches.

mod program;
pub(crate) mod session;

pub use program::SinkProgram;

use crate::Error;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::path::{Path, PathBuf};

/// One business owner shared by all of a Plugin's input Queues.
///
/// Lifecycle methods run on the synchronous program thread. The Instance's one
/// Egress loop serves every input Queue in turn, so only one `write` method body
/// runs at a time; the owner still protects whatever it shares with threads or
/// executors it started itself.
pub trait TenonSink<P>: Send + Sync + 'static {
    /// Prepares local business resources. If startup fails, later cleanup is not promised.
    fn start(&mut self);

    /// Accepts one nonempty, ordered batch from one Flow channel.
    ///
    /// The method body runs on the Instance's one Egress loop, outside SDK locks.
    /// Its returned future observes work started on business-owned threads or
    /// executors; the Egress loop polls it without an async runtime. One Queue's
    /// unfinished future never blocks another Queue, and futures finish in
    /// whatever order the business completes them. They may borrow the owner and
    /// remain on the Egress loop until completion or shutdown.
    /// An error fails the Instance without releasing this batch or its suffix.
    /// Shutdown drops unfinished observers without waiting or acknowledging.
    fn write(
        &self,
        channel: FlowChannel,
        records: Box<[P]>,
    ) -> impl Future<Output = Result<(), Error>>;

    /// Reclaims business resources after all SDK readers and write bodies stop.
    fn close(&mut self);
}

/// The Flow and its zero-based channel that produced a Sink batch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FlowChannel {
    /// The exact Flow id authored in the Tenon Document.
    pub flow_id: String,
    /// The channel index within that Flow's parallelism.
    pub channel_id: u32,
}

/// SDK-internal wiring for one Sink input. Plugin code only sees `FlowChannel`.
#[derive(Clone, Debug)]
pub(crate) struct SinkInput {
    pub(crate) channel: FlowChannel,
    pub(crate) channel_bell_path: PathBuf,
}

impl FlowChannel {
    /// The directory holding every Queue and Region this Sink side owns.
    pub(crate) fn side_directory(working_directory: &Path) -> PathBuf {
        working_directory.join(SINK_DIRECTORY_NAME)
    }

    fn queue_path(&self, working_directory: &Path) -> PathBuf {
        Self::side_directory(working_directory)
            .join(URL_SAFE_NO_PAD.encode(Sha256::digest(self.flow_id.as_bytes())))
            .join(format!("egress-{}.queue", self.channel_id))
    }
}

/// The fixed directory holding one Sink side's Queues, Regions, and layers.
pub(crate) const SINK_DIRECTORY_NAME: &str = "sink";
