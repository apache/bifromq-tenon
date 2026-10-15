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

//! Exhaustive small-model checks use fixture labels, never production material equality.

use super::super::identity::RuntimeResourceIdentity;
use super::super::{FlowChange, ReconfigurePlan, ResourceAction, ResourceMutation};
use super::{AVAILABLE_CPU_COUNT, TestResult, changed_contract, flow, model, program};
use crate::contracts::core::{PipelineRevisionPlan, PluginInterface};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
enum Variant {
    Original,
    Changed,
}

#[derive(Clone, Copy, Debug)]
struct Scenario {
    source_config: Variant,
    source_build: Variant,
    source_shape: Variant,
    sink_shape: Variant,
    has_audit_flow: bool,
    processing: Variant,
    limits: Variant,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Resource {
    Process(String),
    SinkBell(String),
    FlowBell(String),
    SourceQueue(String, u32),
    Channel(String, u32),
    EgressQueue(String, String, u32),
    Route(String, String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mutation {
    Add,
    Replace,
    Remove,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FlowOperation {
    Add,
    ReplaceQueues,
    ReplaceDefinition,
    UpdateRoutes,
    Remove,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FlowFacts {
    stream: Value,
    definition: Value,
    routes: Value,
}

#[test]
fn all_128_scenarios_and_every_transition_match_an_independent_resource_model() -> TestResult {
    let scenarios: Vec<_> = (0..128)
        .map(|bits| {
            let variant = |mask| {
                if bits & mask == 0 {
                    Variant::Original
                } else {
                    Variant::Changed
                }
            };
            Scenario {
                source_config: variant(1),
                source_build: variant(2),
                source_shape: variant(4),
                sink_shape: variant(8),
                has_audit_flow: bits & 16 != 0,
                processing: variant(32),
                limits: variant(64),
            }
        })
        .collect();
    let revisions = scenarios
        .iter()
        .map(Scenario::revision)
        .collect::<Result<Vec<_>, _>>()?;
    let models = revisions
        .iter()
        .cloned()
        .map(model)
        .collect::<Result<Vec<_>, _>>()?;
    let resources: Vec<_> = scenarios.iter().map(Scenario::resources).collect();
    let flows: Vec<_> = scenarios.iter().map(Scenario::flow_facts).collect();

    for (target_index, target) in revisions.iter().enumerate() {
        for current_index in std::iter::once(None).chain((0..scenarios.len()).map(Some)) {
            let current_resources = current_index.map(|index| &resources[index]);
            let expected = expected_changes(current_resources, &resources[target_index]);
            let plan = ReconfigurePlan::derive(
                current_index.map(|index| &models[index]),
                model(target.clone())?,
                AVAILABLE_CPU_COUNT,
            )?;
            let actual: Vec<_> = plan.actions.iter().map(normalize_action).collect();
            assert_eq!(
                actual, expected,
                "resource actions: {current_index:?} -> {target_index}"
            );
            let compiled = plan.compile()?;

            let mut owners: Vec<_> = compiled
                .processes
                .iter()
                .map(|(id, mutation)| {
                    (
                        Resource::Process(id.as_str().to_owned()),
                        normalize_mutation(*mutation),
                    )
                })
                .chain(compiled.sink_bells.iter().map(|(id, mutation)| {
                    (
                        Resource::SinkBell(id.as_str().to_owned()),
                        normalize_mutation(*mutation),
                    )
                }))
                .chain(compiled.flow_bells.iter().map(|(id, mutation)| {
                    (
                        Resource::FlowBell(id.as_str().to_owned()),
                        normalize_mutation(*mutation),
                    )
                }))
                .chain(compiled.egress_queues.iter().map(|queue| {
                    (
                        Resource::EgressQueue(
                            queue.instance_id.as_str().to_owned(),
                            queue.flow_id.as_str().to_owned(),
                            queue.channel_index,
                        ),
                        normalize_mutation(queue.mutation),
                    )
                }))
                .collect();
            owners.sort_by(|left, right| left.0.cmp(&right.0));
            let expected_owners: Vec<_> = expected
                .into_iter()
                .filter(|(resource, _)| {
                    matches!(
                        resource,
                        Resource::Process(_)
                            | Resource::SinkBell(_)
                            | Resource::FlowBell(_)
                            | Resource::EgressQueue(_, _, _)
                    )
                })
                .collect();
            assert_eq!(
                owners, expected_owners,
                "compiled owners: {current_index:?} -> {target_index}"
            );

            let actual_flows: BTreeMap<_, _> = compiled
                .flows
                .iter()
                .map(|(id, operation)| {
                    (
                        id.as_str().to_owned(),
                        match operation {
                            FlowChange::Add => FlowOperation::Add,
                            FlowChange::ReplaceQueues => FlowOperation::ReplaceQueues,
                            FlowChange::ReplaceDefinition => FlowOperation::ReplaceDefinition,
                            FlowChange::UpdateRoutes => FlowOperation::UpdateRoutes,
                            FlowChange::Remove => FlowOperation::Remove,
                        },
                    )
                })
                .collect();
            assert_eq!(
                actual_flows,
                expected_flow_changes(
                    current_index.map(|index| &flows[index]),
                    &flows[target_index]
                ),
                "compiled Flows: {current_index:?} -> {target_index}",
            );
        }
    }
    Ok(())
}

impl Scenario {
    fn source_version(&self) -> &'static str {
        match (self.source_shape, self.source_build) {
            (Variant::Original, Variant::Original) => "1.0.0",
            (Variant::Original, Variant::Changed) => "1.0.1",
            (Variant::Changed, Variant::Original) => "2.0.0",
            (Variant::Changed, Variant::Changed) => "2.0.1",
        }
    }

    fn sink_version(&self) -> &'static str {
        match self.sink_shape {
            Variant::Original => "1.0.0",
            Variant::Changed => "2.0.0",
        }
    }

    fn parallelism(&self) -> u32 {
        match self.processing {
            Variant::Original => 1,
            Variant::Changed => 3,
        }
    }

    fn targets(&self, flow_id: &str) -> Vec<&'static str> {
        if flow_id == "audit" || (self.has_audit_flow && self.processing == Variant::Changed) {
            vec!["output-main", "output-side"]
        } else {
            vec!["output-main"]
        }
    }

    fn registry(&self, flow_id: &str) -> BTreeMap<String, Variant> {
        self.targets(flow_id)
            .into_iter()
            .map(|id| match id {
                "output-main" => (
                    format!("com.example.output@{}", self.sink_version()),
                    self.sink_shape,
                ),
                "output-side" => (
                    "com.example.side-output@1.0.0".to_owned(),
                    Variant::Original,
                ),
                _ => unreachable!("the fixture contains only two Sink instances"),
            })
            .collect()
    }

    fn flow_ids(&self) -> impl Iterator<Item = &'static str> {
        std::iter::once("telemetry").chain(self.has_audit_flow.then_some("audit"))
    }

    fn revision(&self) -> TestResult<PipelineRevisionPlan> {
        let mut instances = json!({
            "source-main": {
                "programName": "com.example.source", "exactVersion": self.source_version(),
                "config": {"selection": self.source_config}
            },
            "output-main": {
                "programName": "com.example.output", "exactVersion": self.sink_version(), "config": {}
            }
        });
        let mut source = program(
            "com.example.source",
            self.source_version(),
            PluginInterface::Source,
        )?;
        if self.source_shape == Variant::Changed {
            source.payload_descriptor_set =
                changed_contract(&source.payload_descriptor_set, "SourceRecordPayload")?;
        }
        let mut sink = program(
            "com.example.output",
            self.sink_version(),
            PluginInterface::Sink,
        )?;
        if self.sink_shape == Variant::Changed {
            sink.payload_descriptor_set =
                changed_contract(&sink.payload_descriptor_set, "SinkRecordPayload")?;
        }
        let mut programs = vec![source, sink];
        let mut flows = json!({
            "telemetry": flow("source-main", self.parallelism(), &self.targets("telemetry"))
        });
        if self.limits == Variant::Changed {
            flows["telemetry"]["maxPendingRecords"] = json!(7);
            flows["telemetry"]["maxRecordBytes"] = json!(4096);
        }
        if self.processing == Variant::Changed {
            flows["telemetry"]["delivery"] = json!("at-most-once");
            flows["telemetry"]["process"]["script"] =
                json!("function main(event) emit(); emit() end");
        }
        if self.has_audit_flow {
            instances["source-side"] = json!({
                "programName": "com.example.side-source", "exactVersion": "1.0.0", "config": {}
            });
            instances["output-side"] = json!({
                "programName": "com.example.side-output", "exactVersion": "1.0.0", "config": {}
            });
            flows["audit"] = flow("source-side", 2, &self.targets("audit"));
            programs.push(program(
                "com.example.side-source",
                "1.0.0",
                PluginInterface::Source,
            )?);
            programs.push(program(
                "com.example.side-output",
                "1.0.0",
                PluginInterface::Sink,
            )?);
        }
        programs.sort_by(|left, right| {
            (&left.program_name, &left.exact_version)
                .cmp(&(&right.program_name, &right.exact_version))
        });
        Ok(PipelineRevisionPlan {
            document_etag: format!("{self:?}"),
            tenon_document_json: json!({
                "specVersion": "1", "id": "bounded-resource-model",
                "pluginInstances": instances, "flows": flows,
            })
            .to_string(),
            plugin_programs: programs,
        })
    }

    fn flow_facts(&self) -> BTreeMap<String, FlowFacts> {
        self.flow_ids()
            .map(|id| {
                let stream = if id == "telemetry" {
                    json!([
                        "source-main",
                        self.parallelism(),
                        self.source_shape,
                        self.limits
                    ])
                } else {
                    json!(["source-side", 2, Variant::Original])
                };
                let processing = if id == "telemetry" {
                    self.processing
                } else {
                    Variant::Original
                };
                (
                    id.to_owned(),
                    FlowFacts {
                        stream,
                        definition: json!([processing, self.registry(id)]),
                        routes: json!(self.targets(id)),
                    },
                )
            })
            .collect()
    }

    fn sink_layout(&self, instance: &str) -> BTreeMap<&'static str, Value> {
        self.flow_ids()
            .filter(|id| self.targets(id).contains(&instance))
            .map(|id| {
                (
                    id,
                    if id == "telemetry" {
                        json!([self.parallelism(), self.limits])
                    } else {
                        json!([2, Variant::Original])
                    },
                )
            })
            .collect()
    }

    fn resources(&self) -> BTreeMap<Resource, Value> {
        let mut resources = BTreeMap::from([
            (
                Resource::Process("source-main".into()),
                json!([
                    "com.example.source",
                    self.source_version(),
                    self.source_config,
                    ["telemetry", self.parallelism(), self.limits]
                ]),
            ),
            (
                Resource::Process("output-main".into()),
                json!([
                    "com.example.output",
                    self.sink_version(),
                    {},
                    self.sink_layout("output-main")
                ]),
            ),
        ]);
        if self.has_audit_flow {
            resources.insert(
                Resource::Process("source-side".into()),
                json!(["com.example.side-source", "1.0.0", {}, ["audit", 2]]),
            );
            resources.insert(
                Resource::Process("output-side".into()),
                json!([
                    "com.example.side-output",
                    "1.0.0",
                    {},
                    self.sink_layout("output-side")
                ]),
            );
        }
        for (id, facts) in self.flow_facts() {
            let count = if id == "telemetry" {
                self.parallelism()
            } else {
                2
            };
            resources.insert(Resource::FlowBell(id.clone()), json!(count));
            for target in self.targets(&id) {
                resources.insert(Resource::SinkBell(target.into()), json!(1));
            }
            for index in 0..count {
                for target in self.targets(&id) {
                    resources.insert(
                        Resource::EgressQueue(target.into(), id.clone(), index),
                        json!([
                            if target == "output-main" {
                                self.sink_shape
                            } else {
                                Variant::Original
                            },
                            if id == "telemetry" {
                                self.limits
                            } else {
                                Variant::Original
                            },
                        ]),
                    );
                }
                resources.insert(
                    Resource::SourceQueue(id.clone(), index),
                    facts.stream.clone(),
                );
                resources.insert(
                    Resource::Channel(id.clone(), index),
                    json!([facts.stream, facts.definition]),
                );
            }
            for contract in self.registry(&id).keys() {
                let target = if contract.starts_with("com.example.output@") {
                    "output-main"
                } else {
                    "output-side"
                };
                resources.insert(
                    Resource::Route(id.clone(), contract.clone()),
                    json!([target]),
                );
            }
        }
        resources
    }
}

fn expected_changes(
    current: Option<&BTreeMap<Resource, Value>>,
    target: &BTreeMap<Resource, Value>,
) -> Vec<(Resource, Mutation)> {
    let keys: BTreeSet<_> = current
        .into_iter()
        .flat_map(|map| map.keys())
        .chain(target.keys())
        .collect();
    keys.into_iter()
        .filter_map(|key| {
            let mutation = match (current.and_then(|map| map.get(key)), target.get(key)) {
                (None, Some(_)) => Mutation::Add,
                (Some(_), None) => Mutation::Remove,
                (Some(before), Some(after)) if before != after => Mutation::Replace,
                _ => return None,
            };
            Some((key.clone(), mutation))
        })
        .collect()
}

fn expected_flow_changes(
    current: Option<&BTreeMap<String, FlowFacts>>,
    target: &BTreeMap<String, FlowFacts>,
) -> BTreeMap<String, FlowOperation> {
    let keys: BTreeSet<_> = current
        .into_iter()
        .flat_map(|map| map.keys())
        .chain(target.keys())
        .collect();
    keys.into_iter()
        .filter_map(|key| {
            let operation = match (current.and_then(|map| map.get(key)), target.get(key)) {
                (None, Some(_)) => FlowOperation::Add,
                (Some(_), None) => FlowOperation::Remove,
                (Some(before), Some(after)) if before.stream != after.stream => {
                    FlowOperation::ReplaceQueues
                }
                (Some(before), Some(after)) if before.definition != after.definition => {
                    FlowOperation::ReplaceDefinition
                }
                (Some(before), Some(after)) if before.routes != after.routes => {
                    FlowOperation::UpdateRoutes
                }
                _ => return None,
            };
            Some((key.clone(), operation))
        })
        .collect()
}

fn normalize_action(action: &ResourceAction) -> (Resource, Mutation) {
    let (id, mutation) = match action {
        ResourceAction::Add(id) => (id, Mutation::Add),
        ResourceAction::Replace(id) => (id, Mutation::Replace),
        ResourceAction::Remove(id) => (id, Mutation::Remove),
    };
    let resource = match id {
        RuntimeResourceIdentity::PluginProcess { instance_id } => {
            Resource::Process(instance_id.as_str().to_owned())
        }
        RuntimeResourceIdentity::SinkBellRegion { instance_id } => {
            Resource::SinkBell(instance_id.as_str().to_owned())
        }
        RuntimeResourceIdentity::FlowBellRegion { flow_id } => {
            Resource::FlowBell(flow_id.as_str().to_owned())
        }
        RuntimeResourceIdentity::SourceQueuePair {
            flow_id,
            channel_index,
        } => Resource::SourceQueue(flow_id.as_str().to_owned(), *channel_index),
        RuntimeResourceIdentity::FlowChannel {
            flow_id,
            channel_index,
        } => Resource::Channel(flow_id.as_str().to_owned(), *channel_index),
        RuntimeResourceIdentity::EgressQueue {
            instance_id,
            flow_id,
            channel_index,
        } => Resource::EgressQueue(
            instance_id.as_str().to_owned(),
            flow_id.as_str().to_owned(),
            *channel_index,
        ),
        RuntimeResourceIdentity::FlowRoute {
            flow_id,
            sink_contract_id,
        } => Resource::Route(flow_id.as_str().to_owned(), sink_contract_id.to_string()),
    };
    (resource, mutation)
}

fn normalize_mutation(mutation: ResourceMutation) -> Mutation {
    match mutation {
        ResourceMutation::Add => Mutation::Add,
        ResourceMutation::Replace => Mutation::Replace,
        ResourceMutation::Remove => Mutation::Remove,
    }
}
