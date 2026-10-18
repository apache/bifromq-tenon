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

//! Terminal failures produced by one Flow Channel data loop.
//!
//! This leaf module depends only on the Queue, Lua, and Egress failures that a
//! Channel can surface. The data loop and replacement protocol both depend on
//! this taxonomy; the taxonomy never depends on either implementation.

use std::error::Error;
use std::fmt;

use super::egress::EgressError;
use crate::lua::LuaVmErrorKind;
use crate::pipeline::ingress_queue::IngressQueueError;
use tenon_ipc::bell::BellError;
use tenon_ipc::queue::QueueRuntimeError;

/// A terminal failure in one Flow Channel.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum FlowChannelError {
    /// The paired Submission reader or Completion writer failed.
    IngressQueue {
        /// Original Queue adapter failure.
        source: IngressQueueError,
    },
    /// A Lua VM could not be loaded initially or after a failed `main` call.
    LuaVmLoad {
        /// Stable Lua category; interpreter details remain on the owner thread.
        kind: LuaVmErrorKind,
    },
    /// Sending a Lua output batch failed.
    EgressBatch {
        /// Original Queue failure.
        source: EgressError,
    },
    /// An accepted payload did not reach its all-target release boundary.
    EgressRelease {
        /// Original release failure.
        source: EgressError,
    },
    /// The Channel's single idle wait re-read a condition and a Queue failed.
    ///
    /// One wait covers the Submission positions and every Egress release this
    /// Channel might act on, so its failure is reported as the wait itself
    /// rather than as one of the endpoints it read.
    ChannelWait {
        /// Original Queue failure while probing Source input or Sink release.
        source: QueueRuntimeError,
    },
    /// The Channel's own doorbell failed while arming, rechecking, or waking.
    Bell {
        /// Original Bell Region failure.
        source: BellError,
    },
    /// The ordered pending record set could not reserve memory.
    ResourceLimitExceeded,
    /// The Pipeline monotonic millisecond value exceeded Lua's signed integer range.
    TimestampOverflow,
    /// Private channel state violated its ownership invariant.
    InternalInvariantViolation,
}

impl fmt::Display for FlowChannelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IngressQueue { .. } => formatter.write_str("Flow Channel Ingress Queue failed"),
            Self::LuaVmLoad { kind } => {
                write!(formatter, "Flow Channel Lua VM load failed: {kind:?}")
            }
            Self::EgressBatch { .. } => formatter.write_str("Flow Channel output batch failed"),
            Self::EgressRelease { .. } => formatter.write_str("Flow Channel Egress release failed"),
            Self::ChannelWait { .. } => formatter.write_str("Flow Channel wait failed"),
            Self::Bell { .. } => formatter.write_str("Flow Channel doorbell failed"),
            Self::ResourceLimitExceeded => {
                formatter.write_str("Flow Channel resource limit was exceeded")
            }
            Self::TimestampOverflow => {
                formatter.write_str("Flow Channel timestamp exceeded its range")
            }
            Self::InternalInvariantViolation => {
                formatter.write_str("Flow Channel invariant was violated")
            }
        }
    }
}

impl Error for FlowChannelError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::IngressQueue { source } => Some(source),
            Self::EgressBatch { source } => Some(source),
            Self::EgressRelease { source } => Some(source),
            Self::ChannelWait { source } => Some(source),
            Self::Bell { source } => Some(source),
            Self::LuaVmLoad { .. }
            | Self::ResourceLimitExceeded
            | Self::TimestampOverflow
            | Self::InternalInvariantViolation => None,
        }
    }
}

impl From<IngressQueueError> for FlowChannelError {
    fn from(source: IngressQueueError) -> Self {
        Self::IngressQueue { source }
    }
}

impl From<BellError> for FlowChannelError {
    fn from(source: BellError) -> Self {
        Self::Bell { source }
    }
}
