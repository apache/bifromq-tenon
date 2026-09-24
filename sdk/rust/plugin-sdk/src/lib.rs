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

//! The author API and runtime support for Tenon Rust Plugins.
//!
//! Business code implements [`TenonSource`], [`TenonSink`] or [`TenonSourceAndSink`].
//! Entrypoints use [`SourceProgram`], [`SinkProgram`] or [`SourceAndSinkProgram`]
//! with the corresponding business factory.
//! The SDK owns one private lifecycle connection and the bounded Source Queue
//! pumps and per-Queue Sink coordinators; business code owns its clients and executors. Source
//! admission, commit handoff and completion remain separate responsibilities.
//! Sink batches release only through the continuous successful prefix. Normal
//! shutdown joins SDK workers before closing the business owner. Losing
//! the parent stdin or lifecycle stream terminates the process immediately.
//!
//! The private wire and mmap adapters follow language-neutral contract files,
//! with no dependency on the Tenon executable or its internal modules.
//! The complete release toolchain remains under development.

mod json;
mod process;
mod sink;
mod source;
mod source_and_sink;
mod wire;

const LOOPS_BELL_FILE_NAME: &str = "loops.bells";

pub use prost;
pub use serde_json::Value;
pub use sink::{FlowChannel, SinkProgram, TenonSink};
pub use source::{
    AckCode, Completion, InvalidChannel, PayloadSender, SendError, SourceProgram, TenonSource,
};
pub use source_and_sink::{Ingress, SourceAndSinkProgram, TenonSourceAndSink};

/// An operation failure from the SDK or a business callback.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Repository-only protocol and Queue access; absent from ordinary builds.
#[cfg(feature = "repository-test-support")]
#[doc(hidden)]
pub mod repository_test_support;

#[cfg(test)]
mod test_support;
