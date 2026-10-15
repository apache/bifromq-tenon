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

//! Derives target Instance/Flow resource actions without runtime side effects.
//!
//! Each received revision is projected into one canonical resource sequence.
//! Resource material borrows the sole validated revision; only emitted actions
//! own identity values. The shared ordered merge then visits every union
//! identity once. Initial and later applications consume the same resource work.

mod identity;
mod projection;

use super::error::PipelineReconfigureError;
use crate::identifiers::{FlowId, PluginInstanceId};
use crate::pipeline::reconfigure::revision::PipelineRevision;
use identity::RuntimeResourceIdentity;
use projection::{diff_runtime_resources, project_runtime_resources};
use std::collections::BTreeMap;
use std::num::NonZeroUsize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ResourceAction {
    Add(RuntimeResourceIdentity),
    Replace(RuntimeResourceIdentity),
    Remove(RuntimeResourceIdentity),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResourceMutation {
    Add,
    Replace,
    Remove,
}

/// One transient target and the complete actions derived for its application.
pub(super) struct ReconfigurePlan {
    pub(super) target: PipelineRevision,
    pub(super) actions: Box<[ResourceAction]>,
    available_cpu_count: NonZeroUsize,
}

impl ReconfigurePlan {
    pub(super) fn derive(
        current: Option<&PipelineRevision>,
        target: PipelineRevision,
        available_cpu_count: NonZeroUsize,
    ) -> Result<Self, PipelineReconfigureError> {
        if let Some(current) = current
            && current.document().id() != target.document().id()
        {
            return Err(PipelineReconfigureError::DocumentIdentityMismatch);
        }
        let current_resources = current
            .map(|revision| project_runtime_resources(revision, available_cpu_count))
            .transpose()?
            .unwrap_or_default();
        let target_resources = project_runtime_resources(&target, available_cpu_count)?;
        let actions = diff_runtime_resources(&current_resources, &target_resources)?;
        Ok(Self {
            target,
            actions,
            available_cpu_count,
        })
    }

    /// Groups resource actions by their runtime owner without filtering combinations.
    pub(super) fn compile(self) -> Result<CompiledResourceChanges, PipelineReconfigureError> {
        let mut changes = CompiledResourceChanges {
            target: self.target,
            available_cpu_count: self.available_cpu_count,
            processes: Vec::new(),
            sink_bells: BTreeMap::new(),
            flow_bells: BTreeMap::new(),
            flows: BTreeMap::new(),
            egress_queues: Vec::new(),
        };
        for action in self.actions {
            let (identity, mutation) = match action {
                ResourceAction::Add(identity) => (identity, ResourceMutation::Add),
                ResourceAction::Replace(identity) => (identity, ResourceMutation::Replace),
                ResourceAction::Remove(identity) => (identity, ResourceMutation::Remove),
            };
            match identity {
                RuntimeResourceIdentity::PluginProcess { instance_id } => {
                    changes
                        .processes
                        .try_reserve(1)
                        .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;
                    changes.processes.push((instance_id, mutation));
                }
                RuntimeResourceIdentity::SinkBellRegion { instance_id } => {
                    changes.sink_bells.insert(instance_id, mutation);
                }
                RuntimeResourceIdentity::FlowBellRegion { flow_id } => {
                    changes.flow_bells.insert(flow_id, mutation);
                }
                RuntimeResourceIdentity::SourceQueuePair {
                    flow_id,
                    channel_index: 0,
                } => {
                    // A layout change replaces every Queue pair in this Flow.
                    // The validated queue count is positive, so index zero owns
                    // the one whole-layout operation rather than a second list.
                    changes.flows.insert(
                        flow_id,
                        match mutation {
                            ResourceMutation::Add => FlowChange::Add,
                            ResourceMutation::Replace => FlowChange::ReplaceQueues,
                            ResourceMutation::Remove => FlowChange::Remove,
                        },
                    );
                }
                RuntimeResourceIdentity::FlowChannel {
                    flow_id,
                    channel_index: 0,
                } => {
                    // Source layout actions precede Channel actions. Without a
                    // layout action, the existing threads prepare a new definition.
                    if mutation == ResourceMutation::Replace {
                        changes
                            .flows
                            .entry(flow_id)
                            .or_insert(FlowChange::ReplaceDefinition);
                    }
                }
                RuntimeResourceIdentity::SourceQueuePair { .. }
                | RuntimeResourceIdentity::FlowChannel { .. } => {}
                RuntimeResourceIdentity::EgressQueue {
                    instance_id,
                    flow_id,
                    channel_index,
                } => {
                    changes
                        .egress_queues
                        .try_reserve(1)
                        .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;
                    changes.egress_queues.push(EgressQueueChange {
                        instance_id,
                        flow_id,
                        channel_index,
                        mutation,
                    });
                }
                RuntimeResourceIdentity::FlowRoute { flow_id, .. } => {
                    changes
                        .flows
                        .entry(flow_id)
                        .or_insert(FlowChange::UpdateRoutes);
                }
            }
        }
        Ok(changes)
    }
}

/// One Flow owner's operation, including Queue replacement when their layout changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FlowChange {
    Add,
    ReplaceQueues,
    ReplaceDefinition,
    UpdateRoutes,
    Remove,
}

/// The complete resource work, retained until Stage and Cutover consume it.
pub(super) struct CompiledResourceChanges {
    pub(super) target: PipelineRevision,
    pub(super) available_cpu_count: NonZeroUsize,
    pub(super) processes: Vec<(PluginInstanceId, ResourceMutation)>,
    pub(super) sink_bells: BTreeMap<PluginInstanceId, ResourceMutation>,
    pub(super) flow_bells: BTreeMap<FlowId, ResourceMutation>,
    pub(super) flows: BTreeMap<FlowId, FlowChange>,
    pub(super) egress_queues: Vec<EgressQueueChange>,
}

impl CompiledResourceChanges {
    pub(super) fn process_mutation(&self, id: &PluginInstanceId) -> Option<ResourceMutation> {
        self.processes
            .binary_search_by(|(instance_id, _)| instance_id.cmp(id))
            .ok()
            .map(|index| self.processes[index].1)
    }

    pub(super) fn new_queue_flows(&self) -> impl Iterator<Item = &FlowId> {
        self.flows.iter().filter_map(|(id, change)| {
            matches!(change, FlowChange::Add | FlowChange::ReplaceQueues).then_some(id)
        })
    }

    pub(super) fn launched_instances(&self) -> impl Iterator<Item = &PluginInstanceId> {
        self.processes
            .iter()
            .filter_map(|(id, mutation)| (*mutation != ResourceMutation::Remove).then_some(id))
    }

    pub(super) fn added_instances(&self) -> impl Iterator<Item = &PluginInstanceId> {
        self.processes
            .iter()
            .filter_map(|(id, mutation)| (*mutation == ResourceMutation::Add).then_some(id))
    }
}

/// One exact Queue file operation; retained files have no entry.
pub(super) struct EgressQueueChange {
    pub(super) instance_id: PluginInstanceId,
    pub(super) flow_id: FlowId,
    pub(super) channel_index: u32,
    pub(super) mutation: ResourceMutation,
}

#[cfg(test)]
pub(super) mod tests;
