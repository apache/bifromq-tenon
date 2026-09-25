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

//! Resolves Instance/Flow Documents against one immutable borrow of the Program Store.
//!
//! This Runner-only boundary performs no I/O and starts no Plugin processes.
//! It borrows each exact Entry once, aggregates independently decidable issues,
//! and initializes each Flow's Lua VMs with only that Flow's validated roots.
//! Temporary VMs are dropped on this synchronous thread, including on failure.
//! Only complete success retains the original Document and one shared Entry per
//! Program. No partial plan, config copy, topology cache, or second registry escapes.

use super::programs::PipelineProgramSnapshot;
use crate::config::ScriptVmLimits;
use crate::identifiers::{ExactVersion, FlowId, PluginInstanceId, ProgramName, SinkContractId};
use crate::lua::LuaVm;
use crate::payload_contract::PluginInterface;
use crate::runner::plugin::platform::Platform;
use crate::runner::plugin::store::{PluginProgramEntry, PluginProgramStore};
use crate::tenon_document::verified::VerifiedTenonDocument;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Checks complete runtime materials without changing the Store or applied revision.
#[derive(Debug)]
pub(in crate::runner) struct RuntimeResolver {
    limits: ScriptVmLimits,
    available_cpu_count: NonZeroUsize,
}

impl RuntimeResolver {
    pub(in crate::runner) fn new(
        limits: ScriptVmLimits,
        available_cpu_count: NonZeroUsize,
    ) -> Self {
        Self {
            limits,
            available_cpu_count,
        }
    }

    /// Returns all independent issues or a complete plan retaining its exact Entries.
    pub(in crate::runner) fn resolve(
        &self,
        document: &Arc<VerifiedTenonDocument>,
        store: &PluginProgramStore,
    ) -> RuntimeResolution {
        let mut issues = Vec::new();
        let mut references: HashMap<_, Vec<_>> = HashMap::new();
        for (id, instance) in document.plugin_instances() {
            references
                .entry(instance.program_identity())
                .or_default()
                .push(id);
        }
        let mut references: Vec<_> = references.into_iter().collect();
        references.sort_unstable_by_key(|(identity, _)| {
            (
                identity.program_name().as_str(),
                identity.exact_version().as_str(),
            )
        });
        let programs: HashMap<_, _> = references
            .into_iter()
            .filter_map(|(identity, ids)| {
                match store.lookup(identity.program_name(), identity.exact_version()) {
                    Some(entry) if !entry.platforms().contains(&Platform::CURRENT) => {
                        issues.push(RuntimeResolutionIssue::PluginPlatformMismatch {
                            program_name: identity.program_name().clone(),
                            exact_version: identity.exact_version().clone(),
                            platforms: entry.platforms().into(),
                            current_platform: Platform::CURRENT,
                            plugin_instance_ids: ids.into_iter().cloned().collect(),
                        });
                        None
                    }
                    Some(entry) => Some((identity, entry)),
                    None => {
                        issues.push(RuntimeResolutionIssue::PluginProgramMissing {
                            program_name: identity.program_name().clone(),
                            exact_version: identity.exact_version().clone(),
                            plugin_instance_ids: ids.into_iter().cloned().collect(),
                        });
                        None
                    }
                }
            })
            .collect();
        let program_for_instance = |id: &PluginInstanceId| {
            let instance = &document.plugin_instances()[id];
            programs.get(instance.program_identity()).copied()
        };

        // These borrowed groups exist only during validation, never in the plan.
        let mut bindings: BTreeMap<_, InstanceBindings<'_>> = BTreeMap::new();
        for (flow_id, flow) in document.flows() {
            bindings.entry(flow.source()).or_default().source = Some(flow_id);
            for sink_id in flow.sinks() {
                bindings.entry(sink_id).or_default().sinks.push(flow_id);
            }
        }
        for (id, instance) in document.plugin_instances() {
            let Some(program) = program_for_instance(id) else {
                continue;
            };
            if !program.accepts_config(instance.config()) {
                issues.push(RuntimeResolutionIssue::PluginConfigSchemaMismatch {
                    plugin_instance_id: id.clone(),
                });
            }
            bindings[id].validate(id, program, &mut issues);
        }

        for (flow_id, flow) in document.flows() {
            let Some(source) =
                program_for_instance(flow.source()).and_then(|entry| entry.source_projection())
            else {
                continue;
            };
            let contracts: Option<HashMap<_, _>> = flow
                .sinks()
                .iter()
                .map(|id| {
                    let instance = &document.plugin_instances()[id];
                    let sink = program_for_instance(id)?.sink_projection()?;
                    Some((
                        SinkContractId::from_parts(
                            instance.program_name().clone(),
                            instance.exact_version().clone(),
                        ),
                        sink.root_message(),
                    ))
                })
                .collect();
            let Some(contracts) = contracts else {
                continue;
            };
            for _ in 0..document
                .channel_count(flow_id, self.available_cpu_count)
                .get()
            {
                if LuaVm::load(
                    flow.lua_source(),
                    self.limits,
                    flow.max_record_bytes(),
                    source.root_message(),
                    contracts.clone(),
                    None,
                    || false,
                )
                .is_err()
                {
                    issues.push(RuntimeResolutionIssue::FlowLuaRuntimeBindingInvalid {
                        flow_id: flow_id.clone(),
                    });
                    break;
                }
            }
        }

        if issues.is_empty() {
            RuntimeResolution::Ready(ResolvedPipelinePlan {
                document: Arc::clone(document),
                programs: PipelineProgramSnapshot::retain(programs),
                available_cpu_count: self.available_cpu_count,
            })
        } else {
            RuntimeResolution::Unready(issues.into_boxed_slice())
        }
    }
}

/// Complete runtime material; validation VMs and borrowed work maps are not retained.
#[derive(Debug)]
pub(in crate::runner) struct ResolvedPipelinePlan {
    document: Arc<VerifiedTenonDocument>,
    programs: PipelineProgramSnapshot,
    available_cpu_count: NonZeroUsize,
}

impl ResolvedPipelinePlan {
    pub(in crate::runner) const fn document(&self) -> &Arc<VerifiedTenonDocument> {
        &self.document
    }

    pub(super) const fn programs(&self) -> &PipelineProgramSnapshot {
        &self.programs
    }

    pub(super) const fn available_cpu_count(&self) -> NonZeroUsize {
        self.available_cpu_count
    }
}

/// No incomplete runtime materials can be mistaken for a ready revision.
#[derive(Debug)]
pub(in crate::runner) enum RuntimeResolution {
    Ready(ResolvedPipelinePlan),
    Unready(Box<[RuntimeResolutionIssue]>),
}

/// Stable redacted issues contain only the identities defined by the Document contract.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
#[serde(
    tag = "code",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub(in crate::runner) enum RuntimeResolutionIssue {
    #[schema(rename_all = "camelCase")]
    PluginProgramMissing {
        #[schema(value_type = String)]
        program_name: ProgramName,
        #[schema(value_type = String)]
        exact_version: ExactVersion,
        #[schema(value_type = Vec<String>)]
        plugin_instance_ids: Box<[PluginInstanceId]>,
    },
    #[schema(rename_all = "camelCase")]
    PluginPlatformMismatch {
        #[schema(value_type = String)]
        program_name: ProgramName,
        #[schema(value_type = String)]
        exact_version: ExactVersion,
        platforms: Box<[Platform]>,
        current_platform: Platform,
        #[schema(value_type = Vec<String>)]
        plugin_instance_ids: Box<[PluginInstanceId]>,
    },
    #[schema(rename_all = "camelCase")]
    PluginConfigSchemaMismatch {
        #[schema(value_type = String)]
        plugin_instance_id: PluginInstanceId,
    },
    #[schema(rename_all = "camelCase")]
    PluginSourceInterfaceMissing {
        #[schema(value_type = String)]
        plugin_instance_id: PluginInstanceId,
        #[schema(value_type = String)]
        flow_id: FlowId,
    },
    #[schema(rename_all = "camelCase")]
    PluginSinkInterfaceMissing {
        #[schema(value_type = String)]
        plugin_instance_id: PluginInstanceId,
        #[schema(value_type = Vec<String>)]
        flow_ids: Box<[FlowId]>,
    },
    #[schema(rename_all = "camelCase")]
    FlowLuaRuntimeBindingInvalid {
        #[schema(value_type = String)]
        flow_id: FlowId,
    },
}

#[derive(Default)]
struct InstanceBindings<'a> {
    source: Option<&'a FlowId>,
    sinks: Vec<&'a FlowId>,
}

impl InstanceBindings<'_> {
    fn validate(
        &self,
        id: &PluginInstanceId,
        program: &PluginProgramEntry,
        issues: &mut Vec<RuntimeResolutionIssue>,
    ) {
        if let (PluginInterface::Sink, Some(flow)) = (program.interface(), self.source) {
            issues.push(RuntimeResolutionIssue::PluginSourceInterfaceMissing {
                plugin_instance_id: id.clone(),
                flow_id: flow.clone(),
            });
        }
        if program.interface() == PluginInterface::Source && !self.sinks.is_empty() {
            issues.push(RuntimeResolutionIssue::PluginSinkInterfaceMissing {
                plugin_instance_id: id.clone(),
                flow_ids: self.sinks.iter().copied().cloned().collect(),
            });
        }
    }
}

#[cfg(test)]
mod tests;
