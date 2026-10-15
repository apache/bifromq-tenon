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

//! Transient resource material projected from one complete Instance/Flow revision.
//!
//! Document fields remain borrowed. Each unique Program interface materializes
//! its structural Contract comparison value once and shares it within this
//! projection; no index or derived material survives the resulting plan.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::rc::Rc;

use super::ResourceAction;
use super::identity::{RuntimeResourceIdentityRef, SinkContractIdRef};
use crate::identifiers::{FlowId, PluginInstanceId, PluginProgramIdentity};
use crate::payload_contract::PayloadContractProjectionMaterial;
use crate::pipeline::reconfigure::error::PipelineReconfigureError;
use crate::pipeline::reconfigure::revision::{PipelineRevision, ProgramRuntime};
use crate::tenon_document::verified::{Flow, PluginInstance};

pub(super) enum RuntimeResourceSpec<'a> {
    PluginProcess {
        instance_id: &'a PluginInstanceId,
        instance: &'a PluginInstance,
        source_layout: Option<FlowQueueLayout<'a>>,
        sink_layout: Rc<[FlowQueueLayout<'a>]>,
    },
    SinkBellRegion {
        instance_id: &'a PluginInstanceId,
    },
    FlowBellRegion {
        flow_id: &'a FlowId,
        slot_count: NonZeroU32,
    },
    SourceQueuePair {
        channel_index: u32,
        flow: Rc<FlowMaterial<'a>>,
    },
    FlowChannel {
        channel_index: u32,
        flow: Rc<FlowMaterial<'a>>,
    },
    EgressQueue {
        instance_id: &'a PluginInstanceId,
        flow_id: &'a FlowId,
        channel_index: u32,
        program: Rc<ProgramProjection>,
        flow: &'a Flow,
    },
    FlowRoute {
        contract_index: usize,
        flow: Rc<FlowMaterial<'a>>,
    },
}

impl RuntimeResourceSpec<'_> {
    pub(super) fn identity(&self) -> RuntimeResourceIdentityRef<'_> {
        match self {
            Self::PluginProcess { instance_id, .. } => {
                RuntimeResourceIdentityRef::PluginProcess { instance_id }
            }
            Self::SinkBellRegion { instance_id } => {
                RuntimeResourceIdentityRef::SinkBellRegion { instance_id }
            }
            Self::FlowBellRegion { flow_id, .. } => {
                RuntimeResourceIdentityRef::FlowBellRegion { flow_id }
            }
            Self::SourceQueuePair {
                channel_index,
                flow,
            } => RuntimeResourceIdentityRef::SourceQueuePair {
                flow_id: flow.flow_id,
                channel_index: *channel_index,
            },
            Self::FlowChannel {
                channel_index,
                flow,
            } => RuntimeResourceIdentityRef::FlowChannel {
                flow_id: flow.flow_id,
                channel_index: *channel_index,
            },
            Self::EgressQueue {
                instance_id,
                flow_id,
                channel_index,
                ..
            } => RuntimeResourceIdentityRef::EgressQueue {
                instance_id,
                flow_id,
                channel_index: *channel_index,
            },
            Self::FlowRoute {
                contract_index,
                flow,
            } => RuntimeResourceIdentityRef::FlowRoute {
                flow_id: flow.flow_id,
                sink_contract_id: flow.sink_contracts[*contract_index].id,
            },
        }
    }

    pub(super) fn has_same_material(&self, other: &Self) -> bool {
        match self {
            // A Sink has one waiting loop, independent of its process and Queues.
            Self::SinkBellRegion { .. } => true,
            Self::FlowBellRegion { slot_count, .. } => {
                let Self::FlowBellRegion {
                    slot_count: other_slots,
                    ..
                } = other
                else {
                    unreachable!("equal resource identities must have the same resource kind")
                };
                slot_count == other_slots
            }
            Self::PluginProcess {
                instance,
                source_layout,
                sink_layout,
                ..
            } => {
                let Self::PluginProcess {
                    instance: other_instance,
                    source_layout: other_source_layout,
                    sink_layout: other_sink_layout,
                    ..
                } = other
                else {
                    unreachable!("equal resource identities must have the same resource kind")
                };
                instance.program_identity() == other_instance.program_identity()
                    && instance.config() == other_instance.config()
                    && instance.has_same_startup_settings(other_instance)
                    && source_layout == other_source_layout
                    && sink_layout == other_sink_layout
            }
            Self::SourceQueuePair { flow, .. } => {
                let Self::SourceQueuePair {
                    flow: other_flow, ..
                } = other
                else {
                    unreachable!("equal resource identities must have the same resource kind")
                };
                has_same_source_stream_material(flow, other_flow)
            }
            Self::FlowChannel { flow, .. } => {
                let Self::FlowChannel {
                    flow: other_flow, ..
                } = other
                else {
                    unreachable!("equal resource identities must have the same resource kind")
                };
                has_same_source_stream_material(flow, other_flow)
                    && flow.definition.delivery() == other_flow.definition.delivery()
                    && flow.definition.lua_source() == other_flow.definition.lua_source()
                    && has_same_sink_registry(flow, other_flow)
            }
            Self::EgressQueue { program, flow, .. } => {
                let Self::EgressQueue {
                    program: other_program,
                    flow: other_flow,
                    ..
                } = other
                else {
                    unreachable!("equal resource identities must have the same resource kind")
                };
                program.sink_contract() == other_program.sink_contract()
                    && flow.max_pending_records() == other_flow.max_pending_records()
                    && flow.max_record_bytes() == other_flow.max_record_bytes()
            }
            Self::FlowRoute {
                contract_index,
                flow,
            } => {
                let Self::FlowRoute {
                    contract_index: other_contract_index,
                    flow: other_flow,
                } = other
                else {
                    unreachable!("equal resource identities must have the same resource kind")
                };
                flow.sink_contracts[*contract_index].targets
                    == other_flow.sink_contracts[*other_contract_index].targets
            }
        }
    }
}

pub(super) struct FlowMaterial<'a> {
    flow_id: &'a FlowId,
    definition: &'a Flow,
    parallelism: NonZeroU32,
    source_program: Rc<ProgramProjection>,
    sink_contracts: Box<[FlowSinkContract<'a>]>,
}

struct FlowSinkContract<'a> {
    id: SinkContractIdRef<'a>,
    program: Rc<ProgramProjection>,
    targets: Box<[&'a PluginInstanceId]>,
}

struct BuildingFlowSinkContract<'a> {
    program: Rc<ProgramProjection>,
    targets: Vec<&'a PluginInstanceId>,
}

pub(super) enum ProgramProjection {
    Source {
        contract: PayloadContractProjectionMaterial,
    },
    Sink {
        contract: PayloadContractProjectionMaterial,
    },
    SourceAndSink {
        source_contract: PayloadContractProjectionMaterial,
        sink_contract: PayloadContractProjectionMaterial,
    },
}

impl ProgramProjection {
    fn source_contract(&self) -> Option<&PayloadContractProjectionMaterial> {
        match self {
            Self::Source { contract } => Some(contract),
            Self::Sink { .. } => None,
            Self::SourceAndSink {
                source_contract, ..
            } => Some(source_contract),
        }
    }

    fn sink_contract(&self) -> Option<&PayloadContractProjectionMaterial> {
        match self {
            Self::Source { .. } => None,
            Self::Sink { contract } => Some(contract),
            Self::SourceAndSink { sink_contract, .. } => Some(sink_contract),
        }
    }
}

struct InstanceProjection<'a> {
    instance: &'a PluginInstance,
    source_layout: Option<FlowQueueLayout<'a>>,
    sink_layout: Rc<[FlowQueueLayout<'a>]>,
    program: Rc<ProgramProjection>,
}

/// Transient startup material: a process must reopen its Queues when any limit changes.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct FlowQueueLayout<'a> {
    flow_id: &'a FlowId,
    channel_count: NonZeroU32,
    max_pending_records: NonZeroU64,
    max_record_bytes: NonZeroU64,
}

pub(super) fn project_runtime_resources(
    model: &PipelineRevision,
    available_cpu_count: NonZeroUsize,
) -> Result<Vec<RuntimeResourceSpec<'_>>, PipelineReconfigureError> {
    let programs = project_programs(model)?;
    let instances = project_instances(model, &programs, available_cpu_count)?;
    let mut flows = Vec::new();
    flows
        .try_reserve_exact(model.document().flows().len())
        .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;
    let mut queue_count = 0_usize;
    let mut route_count = 0_usize;
    let mut egress_queue_count = 0_usize;
    for (flow_id, flow) in model.document().flows() {
        let material = project_flow(
            &instances,
            flow_id,
            flow,
            model.document().channel_count(flow_id, available_cpu_count),
        )?;
        queue_count = queue_count
            .checked_add(
                usize::try_from(material.parallelism.get())
                    .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?,
            )
            .ok_or(PipelineReconfigureError::ResourceLimitExceeded)?;
        route_count = route_count
            .checked_add(material.sink_contracts.len())
            .ok_or(PipelineReconfigureError::ResourceLimitExceeded)?;
        egress_queue_count = egress_queue_count
            .checked_add(
                usize::try_from(material.parallelism.get())
                    .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?
                    .checked_mul(flow.sinks().len())
                    .ok_or(PipelineReconfigureError::ResourceLimitExceeded)?,
            )
            .ok_or(PipelineReconfigureError::ResourceLimitExceeded)?;
        flows.push(Rc::new(material));
    }

    let capacity = model
        .document()
        .plugin_instances()
        .len()
        .checked_add(
            instances
                .values()
                .filter(|instance| !instance.sink_layout.is_empty())
                .count(),
        )
        .and_then(|count| count.checked_add(flows.len()))
        .and_then(|count| count.checked_add(queue_count.checked_mul(2)?))
        .and_then(|count| count.checked_add(egress_queue_count))
        .and_then(|count| count.checked_add(route_count))
        .ok_or(PipelineReconfigureError::ResourceLimitExceeded)?;
    let mut resources = Vec::new();
    resources
        .try_reserve_exact(capacity)
        .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;

    for instance_id in model.document().plugin_instances().keys() {
        let instance = instance_projection(&instances, instance_id);
        resources.push(RuntimeResourceSpec::PluginProcess {
            instance_id,
            instance: instance.instance,
            source_layout: instance.source_layout,
            sink_layout: Rc::clone(&instance.sink_layout),
        });
    }
    for instance_id in model.document().plugin_instances().keys() {
        if !instance_projection(&instances, instance_id)
            .sink_layout
            .is_empty()
        {
            resources.push(RuntimeResourceSpec::SinkBellRegion { instance_id });
        }
    }
    for flow in &flows {
        resources.push(RuntimeResourceSpec::FlowBellRegion {
            flow_id: flow.flow_id,
            slot_count: flow.parallelism,
        });
    }
    for flow in &flows {
        for channel_index in 0..flow.parallelism.get() {
            resources.push(RuntimeResourceSpec::SourceQueuePair {
                channel_index,
                flow: Rc::clone(flow),
            });
        }
    }
    for flow in &flows {
        for channel_index in 0..flow.parallelism.get() {
            resources.push(RuntimeResourceSpec::FlowChannel {
                channel_index,
                flow: Rc::clone(flow),
            });
        }
    }
    for instance_id in model.document().plugin_instances().keys() {
        let instance = instance_projection(&instances, instance_id);
        for layout in instance.sink_layout.iter() {
            for channel_index in 0..layout.channel_count.get() {
                resources.push(RuntimeResourceSpec::EgressQueue {
                    instance_id,
                    flow_id: layout.flow_id,
                    channel_index,
                    program: Rc::clone(&instance.program),
                    flow: &model.document().flows()[layout.flow_id],
                });
            }
        }
    }
    for flow in flows {
        for contract_index in 0..flow.sink_contracts.len() {
            resources.push(RuntimeResourceSpec::FlowRoute {
                contract_index,
                flow: Rc::clone(&flow),
            });
        }
    }
    Ok(resources)
}

#[allow(
    clippy::expect_used,
    reason = "the same-build received revision guarantees each used interface projection"
)]
fn project_flow<'a>(
    instances: &HashMap<&'a PluginInstanceId, InstanceProjection<'a>>,
    flow_id: &'a FlowId,
    flow: &'a Flow,
    parallelism: NonZeroU32,
) -> Result<FlowMaterial<'a>, PipelineReconfigureError> {
    let source_instance = instance_projection(instances, flow.source());
    source_instance
        .program
        .source_contract()
        .expect("a received Flow source must retain its Source Contract projection");
    let mut sink_contracts: HashMap<_, BuildingFlowSinkContract<'a>> = HashMap::new();
    sink_contracts
        .try_reserve(flow.sinks().len())
        .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;
    for instance_id in flow.sinks() {
        let target = instance_projection(instances, instance_id);
        let instance = target.instance;
        let id = SinkContractIdRef::new(instance.program_name(), instance.exact_version());
        target
            .program
            .sink_contract()
            .expect("a received Flow target must retain its Sink Contract projection");
        let binding = sink_contracts
            .entry(id)
            .or_insert_with(|| BuildingFlowSinkContract {
                program: Rc::clone(&target.program),
                targets: Vec::new(),
            });
        binding
            .targets
            .try_reserve(1)
            .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;
        binding.targets.push(instance_id);
    }
    let mut ordered_contracts = Vec::new();
    ordered_contracts
        .try_reserve_exact(sink_contracts.len())
        .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;
    for (id, mut binding) in sink_contracts {
        binding.targets.sort_unstable();
        ordered_contracts.push(FlowSinkContract {
            id,
            program: binding.program,
            targets: binding.targets.into_boxed_slice(),
        });
    }
    ordered_contracts.sort_unstable_by(|left, right| left.id.cmp(&right.id));
    Ok(FlowMaterial {
        flow_id,
        definition: flow,
        parallelism,
        source_program: Rc::clone(&source_instance.program),
        sink_contracts: ordered_contracts.into_boxed_slice(),
    })
}

pub(super) fn diff_runtime_resources(
    current: &[RuntimeResourceSpec<'_>],
    target: &[RuntimeResourceSpec<'_>],
) -> Result<Box<[ResourceAction]>, PipelineReconfigureError> {
    let maximum_action_count = current
        .len()
        .checked_add(target.len())
        .ok_or(PipelineReconfigureError::ResourceLimitExceeded)?;
    let mut actions = Vec::new();
    actions
        .try_reserve_exact(maximum_action_count)
        .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;
    let mut current_index = 0;
    let mut target_index = 0;

    while current_index < current.len() && target_index < target.len() {
        let current_resource = &current[current_index];
        let target_resource = &target[target_index];
        match current_resource.identity().cmp(&target_resource.identity()) {
            Ordering::Less => {
                actions.push(ResourceAction::Remove(
                    current_resource.identity().to_owned(),
                ));
                current_index += 1;
            }
            Ordering::Equal => {
                if !current_resource.has_same_material(target_resource) {
                    actions.push(ResourceAction::Replace(
                        target_resource.identity().to_owned(),
                    ));
                }
                current_index += 1;
                target_index += 1;
            }
            Ordering::Greater => {
                actions.push(ResourceAction::Add(target_resource.identity().to_owned()));
                target_index += 1;
            }
        }
    }
    for resource in &current[current_index..] {
        actions.push(ResourceAction::Remove(resource.identity().to_owned()));
    }
    for resource in &target[target_index..] {
        actions.push(ResourceAction::Add(resource.identity().to_owned()));
    }

    Ok(actions.into_boxed_slice())
}

fn has_same_source_stream_material(current: &FlowMaterial<'_>, target: &FlowMaterial<'_>) -> bool {
    current.definition.source() == target.definition.source()
        && current.parallelism == target.parallelism
        && current.definition.max_pending_records() == target.definition.max_pending_records()
        && current.definition.max_record_bytes() == target.definition.max_record_bytes()
        && current.source_program.source_contract() == target.source_program.source_contract()
}

fn has_same_sink_registry(current: &FlowMaterial<'_>, target: &FlowMaterial<'_>) -> bool {
    current.sink_contracts.len() == target.sink_contracts.len()
        && current
            .sink_contracts
            .iter()
            .zip(target.sink_contracts.iter())
            .all(|(current, target)| {
                current.id == target.id
                    && current.program.sink_contract() == target.program.sink_contract()
            })
}

#[allow(
    clippy::expect_used,
    reason = "the same-build received revision retains every referenced Program and interface"
)]
fn instance_projection<'a, 'index>(
    instances: &'index HashMap<&'a PluginInstanceId, InstanceProjection<'a>>,
    instance_id: &PluginInstanceId,
) -> &'index InstanceProjection<'a> {
    instances
        .get(instance_id)
        .expect("a received revision must retain every referenced Plugin Instance")
}

fn project_programs(
    model: &PipelineRevision,
) -> Result<HashMap<&PluginProgramIdentity, Rc<ProgramProjection>>, PipelineReconfigureError> {
    let mut projections = HashMap::new();
    projections
        .try_reserve(model.programs().len())
        .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;
    for (identity, program) in model.programs() {
        let _ = projections.insert(identity, Rc::new(project_program(program)));
    }
    Ok(projections)
}

fn project_program(program: &ProgramRuntime) -> ProgramProjection {
    let contract = program.payload_contract();
    match (contract.source_projection(), contract.sink_projection()) {
        (Some(source), None) => ProgramProjection::Source {
            contract: source.structural_material(),
        },
        (None, Some(sink)) => ProgramProjection::Sink {
            contract: sink.structural_material(),
        },
        (Some(source), Some(sink)) => ProgramProjection::SourceAndSink {
            source_contract: source.structural_material(),
            sink_contract: sink.structural_material(),
        },
        (None, None) => {
            unreachable!("a validated Program must retain at least one Contract projection")
        }
    }
}

#[allow(
    clippy::expect_used,
    reason = "the sole same-build Runner sends one material for every referenced Program"
)]
fn project_instances<'a>(
    model: &'a PipelineRevision,
    programs: &HashMap<&'a PluginProgramIdentity, Rc<ProgramProjection>>,
    available_cpu_count: NonZeroUsize,
) -> Result<HashMap<&'a PluginInstanceId, InstanceProjection<'a>>, PipelineReconfigureError> {
    let mut source_layouts = HashMap::new();
    source_layouts
        .try_reserve(model.document().flows().len())
        .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;
    for (flow_id, flow) in model.document().flows() {
        let _ = source_layouts.insert(
            flow.source(),
            FlowQueueLayout {
                flow_id,
                channel_count: model.document().channel_count(flow_id, available_cpu_count),
                max_pending_records: flow.max_pending_records(),
                max_record_bytes: flow.max_record_bytes(),
            },
        );
    }

    let mut instances = HashMap::new();
    instances
        .try_reserve(model.document().plugin_instances().len())
        .map_err(|_| PipelineReconfigureError::ResourceLimitExceeded)?;
    for (instance_id, instance) in model.document().plugin_instances() {
        let program = programs
            .get(instance.program_identity())
            .expect("a received revision must retain every referenced Program material");
        let _ = instances.insert(
            instance_id,
            InstanceProjection {
                instance,
                source_layout: source_layouts.get(instance_id).copied(),
                sink_layout: model
                    .document()
                    .flows()
                    .iter()
                    .filter(|(_, flow)| flow.sinks().contains(instance_id))
                    .map(|(flow_id, flow)| FlowQueueLayout {
                        flow_id,
                        channel_count: model.document().channel_count(flow_id, available_cpu_count),
                        max_pending_records: flow.max_pending_records(),
                        max_record_bytes: flow.max_record_bytes(),
                    })
                    .collect(),
                program: Rc::clone(program),
            },
        );
    }
    Ok(instances)
}
