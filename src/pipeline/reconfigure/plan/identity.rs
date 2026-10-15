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

//! Stable identities and canonical ordering for Instance/Flow runtime resources.

use std::cmp::Ordering;

use crate::identifiers::{ExactVersion, FlowId, PluginInstanceId, ProgramName, SinkContractId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::pipeline::reconfigure) enum RuntimeResourceIdentity {
    PluginProcess {
        instance_id: PluginInstanceId,
    },
    SinkBellRegion {
        instance_id: PluginInstanceId,
    },
    FlowBellRegion {
        flow_id: FlowId,
    },
    SourceQueuePair {
        flow_id: FlowId,
        channel_index: u32,
    },
    FlowChannel {
        flow_id: FlowId,
        channel_index: u32,
    },
    EgressQueue {
        instance_id: PluginInstanceId,
        flow_id: FlowId,
        channel_index: u32,
    },
    FlowRoute {
        flow_id: FlowId,
        sink_contract_id: SinkContractId,
    },
}

/// Declaration order is the canonical cross-kind resource order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum RuntimeResourceIdentityRef<'a> {
    PluginProcess {
        instance_id: &'a PluginInstanceId,
    },
    SinkBellRegion {
        instance_id: &'a PluginInstanceId,
    },
    FlowBellRegion {
        flow_id: &'a FlowId,
    },
    SourceQueuePair {
        flow_id: &'a FlowId,
        channel_index: u32,
    },
    FlowChannel {
        flow_id: &'a FlowId,
        channel_index: u32,
    },
    EgressQueue {
        instance_id: &'a PluginInstanceId,
        flow_id: &'a FlowId,
        channel_index: u32,
    },
    FlowRoute {
        flow_id: &'a FlowId,
        sink_contract_id: SinkContractIdRef<'a>,
    },
}

impl RuntimeResourceIdentityRef<'_> {
    pub(super) fn to_owned(self) -> RuntimeResourceIdentity {
        match self {
            Self::PluginProcess { instance_id } => RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id.clone(),
            },
            Self::SinkBellRegion { instance_id } => RuntimeResourceIdentity::SinkBellRegion {
                instance_id: instance_id.clone(),
            },
            Self::FlowBellRegion { flow_id } => RuntimeResourceIdentity::FlowBellRegion {
                flow_id: flow_id.clone(),
            },
            Self::SourceQueuePair {
                flow_id,
                channel_index,
            } => RuntimeResourceIdentity::SourceQueuePair {
                flow_id: flow_id.clone(),
                channel_index,
            },
            Self::FlowChannel {
                flow_id,
                channel_index,
            } => RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id.clone(),
                channel_index,
            },
            Self::EgressQueue {
                instance_id,
                flow_id,
                channel_index,
            } => RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id.clone(),
                flow_id: flow_id.clone(),
                channel_index,
            },
            Self::FlowRoute {
                flow_id,
                sink_contract_id,
            } => RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id.clone(),
                sink_contract_id: sink_contract_id.to_owned(),
            },
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct SinkContractIdRef<'a> {
    program_name: &'a ProgramName,
    exact_version: &'a ExactVersion,
}

impl<'a> SinkContractIdRef<'a> {
    pub(super) const fn new(
        program_name: &'a ProgramName,
        exact_version: &'a ExactVersion,
    ) -> Self {
        Self {
            program_name,
            exact_version,
        }
    }

    fn to_owned(self) -> SinkContractId {
        SinkContractId::from_parts(self.program_name.clone(), self.exact_version.clone())
    }
}

impl Ord for SinkContractIdRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        SinkContractId::cmp_parts(
            self.program_name,
            self.exact_version,
            other.program_name,
            other.exact_version,
        )
    }
}

impl PartialOrd for SinkContractIdRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
