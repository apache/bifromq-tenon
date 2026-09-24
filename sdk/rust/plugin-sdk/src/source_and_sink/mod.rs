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

//! Source-and-sink author interface: one owner for shared business resources.
//!
//! SourceAndSinkProgram owns assembly and final cleanup. The independent Source
//! and Sink sessions retain their existing queue, concurrency and failure rules.

mod program;

pub use program::SourceAndSinkProgram;

use crate::{Error, FlowChannel, PayloadSender};
use std::future::Future;

/// The transport for the Source direction bound to this Instance.
#[derive(Debug)]
pub struct Ingress<P> {
    /// The positive number of ordered channels in the bound Flow.
    pub parallelism: usize,
    /// The concurrent sender shared by the Instance's Source producers.
    pub sender: PayloadSender<P>,
}

/// One business owner serving both Plugin interfaces through shared resources.
pub trait TenonSourceAndSink<P>: Send + Sync + 'static {
    /// Starts all resources needed by the combined Source and Sink program.
    fn start(&mut self);

    /// Stops new Source production while shared resources remain available to Sink processing.
    fn quiesce(&self);

    /// Accepts one batch under the same concurrency and result rules as [`crate::TenonSink::write`].
    fn write(
        &self,
        channel: FlowChannel,
        records: Box<[P]>,
    ) -> impl Future<Output = Result<(), Error>>;

    /// Closes shared resources after Source producers and all Sink workers stop.
    fn close(&mut self);
}
