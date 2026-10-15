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

use super::identity::{RuntimeResourceIdentity, RuntimeResourceIdentityRef, SinkContractIdRef};
use super::projection::{RuntimeResourceSpec, diff_runtime_resources, project_runtime_resources};
use super::{ReconfigurePlan, ResourceAction};
use crate::contracts::core::{PipelineRevisionPlan, PluginInterface, PluginProgramRuntime};
use crate::identifiers::{FlowId, PluginInstanceId, SinkContractId};
use crate::payload_contract::PluginInterface as ContractInterface;
use crate::pipeline::reconfigure::PipelineReconfigureError;
use crate::pipeline::reconfigure::test_support::AVAILABLE_CPU_COUNT;
use crate::runner::test_support::valid_program_descriptor;
use prost::Message as _;
use prost_types::FileDescriptorSet;
use serde_json::{Value, json};
use std::cmp::Ordering;
use std::error::Error;
use std::io;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type DocumentUpdate = fn(&mut Value);

mod compilation;
mod record_limits;
mod state_space;

#[test]
fn equivalent_target_changes_only_etag_and_keeps_every_resource() -> TestResult {
    let current = model(base_revision("current")?)?;
    let target = model(base_revision("target")?)?;

    let plan = ReconfigurePlan::derive(Some(&current), target, AVAILABLE_CPU_COUNT)?;

    assert!(plan.actions.is_empty());
    Ok(())
}

#[test]
fn equivalent_parallelism_keeps_every_resource() -> TestResult {
    let forms = [Some("1.26"), Some("1.5"), Some("1.50"), Some("15e-1")];
    for current_form in forms {
        for target_form in forms {
            let mut revisions = [base_revision("current")?, base_revision("target")?];
            for (revision, form) in revisions.iter_mut().zip([current_form, target_form]) {
                let value = form.map(serde_json::from_str::<Value>).transpose()?;
                update_document(revision, |document| {
                    if let Some(value) = value {
                        document["flows"]["telemetry"]["parallelism"] = value;
                    } else if let Some(flow) = document["flows"]["telemetry"].as_object_mut() {
                        flow.remove("parallelism");
                    }
                })?;
            }
            let [current, target] = revisions;
            let current = model(current)?;
            let plan =
                ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?;
            assert!(
                plan.actions.is_empty(),
                "{current_form:?} -> {target_form:?}"
            );
        }
    }
    Ok(())
}

#[test]
fn cpu_changes_keep_every_resource_independently_of_parallelism() -> TestResult {
    for omitted in [false, true] {
        let mut revisions = [base_revision("current")?, base_revision("target")?];
        for (revision, cpu) in revisions.iter_mut().zip([0.01, 32.0]) {
            update_document(revision, |document| {
                document["resourceLimits"] = json!({"cpu": cpu});
                for flow in document["flows"]
                    .as_object_mut()
                    .unwrap_or_else(|| std::process::abort())
                    .values_mut()
                {
                    if omitted {
                        flow.as_object_mut()
                            .unwrap_or_else(|| std::process::abort())
                            .remove("parallelism");
                    } else {
                        flow["parallelism"] = json!(10);
                    }
                }
            })?;
        }
        let [current, target] = revisions;
        assert!(
            ReconfigurePlan::derive(Some(&model(current)?), model(target)?, AVAILABLE_CPU_COUNT)?
                .actions
                .is_empty()
        );
    }
    Ok(())
}

#[test]
fn instance_config_changes_only_its_process() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut target = base_revision("target")?;
    update_document(&mut target, |document| {
        document["pluginInstances"]["output-a"]["config"] = json!({"endpoint": "changed"});
    })?;

    assert_eq!(
        ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [ResourceAction::Replace(
            RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("output-a")?,
            }
        )]
    );
    Ok(())
}

#[test]
fn parallelism_replaces_its_process_existing_stream_and_adds_new_channel() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut target = base_revision("target")?;
    update_document(&mut target, |document| {
        document["flows"]["telemetry"]["parallelism"] = json!(0.5);
    })?;

    assert_eq!(
        ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [
            ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("output-a")?,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("source-a")?,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowBellRegion {
                flow_id: flow_id("telemetry")?
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::SourceQueuePair {
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::SourceQueuePair {
                flow_id: flow_id("telemetry")?,
                channel_index: 1,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("telemetry")?,
                channel_index: 1,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("output-a")?,
                flow_id: flow_id("telemetry")?,
                channel_index: 1,
            }),
        ]
    );
    Ok(())
}

#[test]
fn delivery_change_replaces_only_its_flow_channels() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut target = base_revision("target")?;
    update_document(&mut target, |document| {
        document["flows"]["telemetry"]["delivery"] = json!("at-most-once");
    })?;

    assert_eq!(
        ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [ResourceAction::Replace(
            RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }
        )]
    );
    Ok(())
}

#[test]
fn lua_change_replaces_only_its_flow_channels() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut target = base_revision("target")?;
    update_document(&mut target, |document| {
        document["flows"]["telemetry"]["process"]["script"] =
            json!("function main(event) emit(); emit() end");
    })?;

    assert_eq!(
        ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [ResourceAction::Replace(
            RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }
        )]
    );
    Ok(())
}

#[test]
fn adding_same_contract_target_updates_the_route_and_exact_sink_layout() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut target = base_revision("target")?;
    update_document(&mut target, |document| {
        document["flows"]["telemetry"]["sinks"] = json!(["output-a", "output-b"]);
    })?;

    assert_eq!(
        ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [
            ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("output-b")?,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("output-b")?,
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("telemetry")?,
                sink_contract_id: sink_contract_id("com.example.output@1.0.0")?,
            })
        ]
    );
    Ok(())
}

#[test]
fn adding_new_contract_rebuilds_flow_registry_and_adds_its_route() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut target = base_revision("target")?;
    update_document(&mut target, |document| {
        document["flows"]["telemetry"]["sinks"] = json!(["output-a", "archive"]);
    })?;

    assert_eq!(
        ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [
            ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("archive")?,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("archive")?,
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("telemetry")?,
                sink_contract_id: sink_contract_id("com.example.archive@1.0.0")?,
            }),
        ]
    );
    Ok(())
}

#[test]
fn adding_a_whole_flow_adds_only_its_exclusive_instances_and_flow_resources() -> TestResult {
    let current = model(single_flow_revision("current")?)?;
    let target = model(base_revision("target")?)?;

    assert_eq!(
        ReconfigurePlan::derive(Some(&current), target, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [
            ResourceAction::Add(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("archive")?,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("output-b")?,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("source-b")?,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::SinkBellRegion {
                instance_id: instance_id("archive")?
            }),
            ResourceAction::Add(RuntimeResourceIdentity::SinkBellRegion {
                instance_id: instance_id("output-b")?
            }),
            ResourceAction::Add(RuntimeResourceIdentity::FlowBellRegion {
                flow_id: flow_id("audit")?
            }),
            ResourceAction::Add(RuntimeResourceIdentity::SourceQueuePair {
                flow_id: flow_id("audit")?,
                channel_index: 0,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::SourceQueuePair {
                flow_id: flow_id("audit")?,
                channel_index: 1,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("audit")?,
                channel_index: 0,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("audit")?,
                channel_index: 1,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("archive")?,
                flow_id: flow_id("audit")?,
                channel_index: 0,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("archive")?,
                flow_id: flow_id("audit")?,
                channel_index: 1,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("output-b")?,
                flow_id: flow_id("audit")?,
                channel_index: 0,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("output-b")?,
                flow_id: flow_id("audit")?,
                channel_index: 1,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("audit")?,
                sink_contract_id: sink_contract_id("com.example.archive@1.0.0")?,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("audit")?,
                sink_contract_id: sink_contract_id("com.example.output@1.0.0")?,
            }),
        ]
    );
    Ok(())
}

#[test]
fn removing_a_whole_flow_removes_only_its_exclusive_instances_and_flow_resources() -> TestResult {
    let current = model(base_revision("current")?)?;
    let target = model(single_flow_revision("target")?)?;

    assert_eq!(
        ReconfigurePlan::derive(Some(&current), target, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [
            ResourceAction::Remove(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("archive")?,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("output-b")?,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("source-b")?,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::SinkBellRegion {
                instance_id: instance_id("archive")?
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::SinkBellRegion {
                instance_id: instance_id("output-b")?
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::FlowBellRegion {
                flow_id: flow_id("audit")?
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::SourceQueuePair {
                flow_id: flow_id("audit")?,
                channel_index: 0,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::SourceQueuePair {
                flow_id: flow_id("audit")?,
                channel_index: 1,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("audit")?,
                channel_index: 0,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("audit")?,
                channel_index: 1,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("archive")?,
                flow_id: flow_id("audit")?,
                channel_index: 0,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("archive")?,
                flow_id: flow_id("audit")?,
                channel_index: 1,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("output-b")?,
                flow_id: flow_id("audit")?,
                channel_index: 0,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("output-b")?,
                flow_id: flow_id("audit")?,
                channel_index: 1,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("audit")?,
                sink_contract_id: sink_contract_id("com.example.archive@1.0.0")?,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("audit")?,
                sink_contract_id: sink_contract_id("com.example.output@1.0.0")?,
            }),
        ]
    );
    Ok(())
}

#[test]
fn swapping_source_instances_replaces_both_queue_users_before_rebinding() -> TestResult {
    let mut current = base_revision("current")?;
    update_document(&mut current, |document| {
        document["flows"]["audit"]["parallelism"] = json!(0.25);
    })?;
    let mut target = base_revision("target")?;
    update_document(&mut target, |document| {
        document["flows"]["audit"]["parallelism"] = json!(0.25);
        document["flows"]["audit"]["source"] = json!("source-a");
        document["flows"]["telemetry"]["source"] = json!("source-b");
    })?;

    assert_eq!(
        ReconfigurePlan::derive(Some(&model(current)?), model(target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [
            ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("source-a")?,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("source-b")?,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::SourceQueuePair {
                flow_id: flow_id("audit")?,
                channel_index: 0,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::SourceQueuePair {
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("audit")?,
                channel_index: 0,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
        ]
    );
    Ok(())
}

#[test]
fn removing_same_contract_target_updates_the_route_and_exact_sink_layout() -> TestResult {
    let mut current = base_revision("current")?;
    update_document(&mut current, |document| {
        document["flows"]["telemetry"]["sinks"] = json!(["output-a", "output-b"]);
    })?;

    assert_eq!(
        ReconfigurePlan::derive(
            Some(&model(current)?),
            model(base_revision("target")?)?,
            AVAILABLE_CPU_COUNT
        )?
        .actions
        .as_ref(),
        [
            ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("output-b")?,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("output-b")?,
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("telemetry")?,
                sink_contract_id: sink_contract_id("com.example.output@1.0.0")?,
            })
        ]
    );
    Ok(())
}

#[test]
fn removing_one_contract_rebuilds_the_flow_registry_and_removes_its_route() -> TestResult {
    let mut current = base_revision("current")?;
    update_document(&mut current, |document| {
        document["flows"]["telemetry"]["sinks"] = json!(["output-a", "archive"]);
    })?;

    assert_eq!(
        ReconfigurePlan::derive(
            Some(&model(current)?),
            model(base_revision("target")?)?,
            AVAILABLE_CPU_COUNT
        )?
        .actions
        .as_ref(),
        [
            ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("archive")?,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("archive")?,
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("telemetry")?,
                sink_contract_id: sink_contract_id("com.example.archive@1.0.0")?,
            }),
        ]
    );
    Ok(())
}

#[test]
fn explicit_default_delivery_is_equivalent_to_omission() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut target = base_revision("target")?;
    update_document(&mut target, |document| {
        document["flows"]["telemetry"]["delivery"] = json!("at-least-once");
    })?;

    assert!(
        ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .is_empty()
    );
    Ok(())
}

#[test]
fn shared_sink_contract_change_updates_its_queues_and_every_referencing_flow() -> TestResult {
    let current = model(shared_sink_revision("current")?)?;
    let mut target = shared_sink_revision("target")?;
    let sink_descriptor = changed_contract(
        &target.plugin_programs[1].payload_descriptor_set,
        "SinkRecordPayload",
    )?;
    select_program_version(
        &mut target,
        "output-a",
        "com.example.output",
        "2.0.0",
        Some(sink_descriptor),
    )?;

    assert_eq!(
        ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [
            ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("output-a")?,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("audit")?,
                channel_index: 0,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("audit")?,
                channel_index: 1,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("output-a")?,
                flow_id: flow_id("audit")?,
                channel_index: 0,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("output-a")?,
                flow_id: flow_id("audit")?,
                channel_index: 1,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("output-a")?,
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("audit")?,
                sink_contract_id: sink_contract_id("com.example.output@1.0.0")?,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("audit")?,
                sink_contract_id: sink_contract_id("com.example.output@2.0.0")?,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("telemetry")?,
                sink_contract_id: sink_contract_id("com.example.output@1.0.0")?,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("telemetry")?,
                sink_contract_id: sink_contract_id("com.example.output@2.0.0")?,
            }),
        ]
    );
    Ok(())
}

#[test]
fn sink_array_order_does_not_change_registry_or_route_material() -> TestResult {
    let mut current = base_revision("current")?;
    update_document(&mut current, |document| {
        document["flows"]["telemetry"]["sinks"] = json!(["output-a", "output-b"]);
    })?;
    let mut target = base_revision("target")?;
    update_document(&mut target, |document| {
        document["flows"]["telemetry"]["sinks"] = json!(["output-b", "output-a"]);
    })?;

    assert!(
        ReconfigurePlan::derive(Some(&model(current)?), model(target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .is_empty()
    );
    Ok(())
}

#[test]
fn compatible_source_program_upgrade_restarts_only_its_instance() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut source_target = base_revision("source-target")?;
    select_program_version(
        &mut source_target,
        "source-a",
        "com.example.source",
        "2.0.0",
        None,
    )?;
    assert_eq!(
        ReconfigurePlan::derive(Some(&current), model(source_target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [ResourceAction::Replace(
            RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("source-a")?,
            }
        )]
    );
    Ok(())
}

#[test]
fn compatible_sink_program_upgrade_preserves_its_endpoint() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut sink_target = base_revision("sink-target")?;
    select_program_version(
        &mut sink_target,
        "output-a",
        "com.example.output",
        "2.0.0",
        None,
    )?;
    assert_eq!(
        ReconfigurePlan::derive(Some(&current), model(sink_target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [
            ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("output-a")?,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("telemetry")?,
                sink_contract_id: sink_contract_id("com.example.output@1.0.0")?,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("telemetry")?,
                sink_contract_id: sink_contract_id("com.example.output@2.0.0")?,
            }),
        ]
    );
    Ok(())
}

#[test]
fn source_contract_change_replaces_only_its_source_data_plane() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut source_target = base_revision("source-target")?;
    let source_descriptor = changed_contract(
        &source_target.plugin_programs[0].payload_descriptor_set,
        "SourceRecordPayload",
    )?;
    select_program_version(
        &mut source_target,
        "source-a",
        "com.example.source",
        "2.0.0",
        Some(source_descriptor),
    )?;
    assert_eq!(
        ReconfigurePlan::derive(Some(&current), model(source_target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [
            ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("source-a")?,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::SourceQueuePair {
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
        ]
    );
    Ok(())
}

#[test]
fn sink_contract_change_replaces_only_its_sink_data_plane() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut sink_target = base_revision("sink-target")?;
    let sink_descriptor = changed_contract(
        &sink_target.plugin_programs[1].payload_descriptor_set,
        "SinkRecordPayload",
    )?;
    select_program_version(
        &mut sink_target,
        "output-a",
        "com.example.output",
        "2.0.0",
        Some(sink_descriptor),
    )?;
    assert_eq!(
        ReconfigurePlan::derive(Some(&current), model(sink_target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [
            ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("output-a")?,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("output-a")?,
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("telemetry")?,
                sink_contract_id: sink_contract_id("com.example.output@1.0.0")?,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("telemetry")?,
                sink_contract_id: sink_contract_id("com.example.output@2.0.0")?,
            }),
        ]
    );
    Ok(())
}

#[test]
fn dual_interface_source_contract_change_preserves_its_sink_queues() -> TestResult {
    let current = model(dual_revision("current")?)?;
    let mut source_target = dual_revision("source-target")?;
    let source_descriptor = changed_contract(
        &source_target.plugin_programs[0].payload_descriptor_set,
        "SourceRecordPayload",
    )?;
    select_program_version(
        &mut source_target,
        "gateway",
        "com.example.gateway",
        "2.0.0",
        Some(source_descriptor),
    )?;
    assert_eq!(
        ReconfigurePlan::derive(Some(&current), model(source_target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [
            ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("gateway")?,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::SourceQueuePair {
                flow_id: flow_id("loop")?,
                channel_index: 0,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("loop")?,
                channel_index: 0,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("loop")?,
                sink_contract_id: sink_contract_id("com.example.gateway@1.0.0")?,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("loop")?,
                sink_contract_id: sink_contract_id("com.example.gateway@2.0.0")?,
            }),
        ]
    );
    Ok(())
}

#[test]
fn dual_interface_sink_contract_change_preserves_its_source_queues() -> TestResult {
    let current = model(dual_revision("current")?)?;
    let mut sink_target = dual_revision("sink-target")?;
    let sink_descriptor = changed_contract(
        &sink_target.plugin_programs[0].payload_descriptor_set,
        "SinkRecordPayload",
    )?;
    select_program_version(
        &mut sink_target,
        "gateway",
        "com.example.gateway",
        "2.0.0",
        Some(sink_descriptor),
    )?;
    assert_eq!(
        ReconfigurePlan::derive(Some(&current), model(sink_target)?, AVAILABLE_CPU_COUNT)?
            .actions
            .as_ref(),
        [
            ResourceAction::Replace(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("gateway")?,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowChannel {
                flow_id: flow_id("loop")?,
                channel_index: 0,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("gateway")?,
                flow_id: flow_id("loop")?,
                channel_index: 0,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("loop")?,
                sink_contract_id: sink_contract_id("com.example.gateway@1.0.0")?,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("loop")?,
                sink_contract_id: sink_contract_id("com.example.gateway@2.0.0")?,
            }),
        ]
    );
    Ok(())
}

#[test]
fn flow_identity_change_replaces_its_source_process_and_flow_resources() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut renamed_flow = base_revision("flow-target")?;
    rename_flow(&mut renamed_flow, "telemetry", "telemetry-v2")?;
    let actions =
        ReconfigurePlan::derive(Some(&current), model(renamed_flow)?, AVAILABLE_CPU_COUNT)?;
    assert_eq!(actions.actions.len(), 12);
    assert!(
        actions
            .actions
            .iter()
            .all(|action| match action_identity(action) {
                RuntimeResourceIdentityRef::FlowBellRegion { flow_id }
                | RuntimeResourceIdentityRef::SourceQueuePair { flow_id, .. }
                | RuntimeResourceIdentityRef::FlowChannel { flow_id, .. }
                | RuntimeResourceIdentityRef::FlowRoute { flow_id, .. } => {
                    matches!(flow_id.as_str(), "telemetry" | "telemetry-v2")
                }
                RuntimeResourceIdentityRef::PluginProcess { instance_id }
                | RuntimeResourceIdentityRef::SinkBellRegion { instance_id } => {
                    matches!(instance_id.as_str(), "source-a" | "output-a")
                }
                RuntimeResourceIdentityRef::EgressQueue {
                    instance_id,
                    flow_id,
                    channel_index,
                } => {
                    instance_id.as_str() == "output-a"
                        && matches!(flow_id.as_str(), "telemetry" | "telemetry-v2")
                        && channel_index == 0
                }
            })
    );
    Ok(())
}

#[test]
fn sink_instance_identity_change_moves_only_its_process_queues_and_route() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut replaced_instance = base_revision("instance-target")?;
    rename_instance(&mut replaced_instance, "output-a", "output-c")?;
    update_document(&mut replaced_instance, |document| {
        document["flows"]["telemetry"]["sinks"] = json!(["output-c"]);
    })?;
    assert_eq!(
        ReconfigurePlan::derive(
            Some(&current),
            model(replaced_instance)?,
            AVAILABLE_CPU_COUNT
        )?
        .actions
        .as_ref(),
        [
            ResourceAction::Remove(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("output-a")?,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::PluginProcess {
                instance_id: instance_id("output-c")?,
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::SinkBellRegion {
                instance_id: instance_id("output-a")?
            }),
            ResourceAction::Add(RuntimeResourceIdentity::SinkBellRegion {
                instance_id: instance_id("output-c")?
            }),
            ResourceAction::Remove(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("output-a")?,
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Add(RuntimeResourceIdentity::EgressQueue {
                instance_id: instance_id("output-c")?,
                flow_id: flow_id("telemetry")?,
                channel_index: 0,
            }),
            ResourceAction::Replace(RuntimeResourceIdentity::FlowRoute {
                flow_id: flow_id("telemetry")?,
                sink_contract_id: sink_contract_id("com.example.output@1.0.0")?,
            }),
        ]
    );
    Ok(())
}

#[test]
fn finite_topology_state_space_matches_quadratic_merge_oracle() -> TestResult {
    let revisions = topology_state_space()?;
    let models = revisions
        .into_iter()
        .map(model)
        .collect::<Result<Vec<_>, _>>()?;
    let projections = models
        .iter()
        .map(|model| project_runtime_resources(model, AVAILABLE_CPU_COUNT))
        .collect::<Result<Vec<_>, _>>()?;

    for resources in &projections {
        assert_projection_is_strictly_ordered(resources);
        assert!(diff_runtime_resources(resources, resources)?.is_empty());
        let initial = diff_runtime_resources(&[], resources)?;
        assert_eq!(
            initial.as_ref(),
            resources
                .iter()
                .map(|resource| ResourceAction::Add(resource.identity().to_owned()))
                .collect::<Vec<_>>()
        );
        assert_actions_are_strictly_ordered(&initial);
    }

    for current in &projections {
        for target in &projections {
            let actions = diff_runtime_resources(current, target)?;
            assert_eq!(actions.as_ref(), quadratic_merge_oracle(current, target));
            assert_actions_are_strictly_ordered(&actions);
        }
    }
    Ok(())
}

#[test]
fn document_identity_cannot_change_inside_one_pipeline() -> TestResult {
    let current = model(base_revision("current")?)?;
    let mut target = base_revision("target")?;
    update_document(&mut target, |document| {
        document["id"] = json!("other-pipeline");
    })?;

    assert!(matches!(
        ReconfigurePlan::derive(Some(&current), model(target)?, AVAILABLE_CPU_COUNT),
        Err(PipelineReconfigureError::DocumentIdentityMismatch)
    ));
    Ok(())
}

fn topology_state_space() -> TestResult<Vec<PipelineRevisionPlan>> {
    let mut revisions = vec![base_revision("base")?];
    let updates: [(&str, DocumentUpdate); 5] = [
        ("queue", |document: &mut Value| {
            document["flows"]["telemetry"]["parallelism"] = json!(0.5);
        }),
        ("delivery", |document: &mut Value| {
            document["flows"]["audit"]["delivery"] = json!("at-most-once");
        }),
        ("lua", |document: &mut Value| {
            document["flows"]["audit"]["process"]["script"] =
                json!("function main(event) emit(); emit() end");
        }),
        ("same-contract-target", |document: &mut Value| {
            document["flows"]["telemetry"]["sinks"] = json!(["output-a", "output-b"]);
        }),
        ("new-contract-target", |document: &mut Value| {
            document["flows"]["telemetry"]["sinks"] = json!(["output-a", "archive"]);
        }),
    ];
    for (etag, update) in updates {
        let mut revision = base_revision(etag)?;
        update_document(&mut revision, update)?;
        revisions.push(revision);
    }
    let mut config = base_revision("config")?;
    update_document(&mut config, |document| {
        document["pluginInstances"]["output-b"]["config"] = json!({"endpoint": "changed"});
    })?;
    revisions.push(config);
    Ok(revisions)
}

// This deliberately shares the production identity and material relations. It
// proves only the ordered merge; explicit action tests above prove dependencies.
fn quadratic_merge_oracle(
    current: &[RuntimeResourceSpec<'_>],
    target: &[RuntimeResourceSpec<'_>],
) -> Vec<ResourceAction> {
    let mut actions = Vec::new();
    for current_resource in current {
        match target.iter().find(|target_resource| {
            current_resource.identity().cmp(&target_resource.identity()) == Ordering::Equal
        }) {
            Some(target_resource) if !current_resource.has_same_material(target_resource) => {
                actions.push(ResourceAction::Replace(
                    target_resource.identity().to_owned(),
                ));
            }
            Some(_) => {}
            None => actions.push(ResourceAction::Remove(
                current_resource.identity().to_owned(),
            )),
        }
    }
    for target_resource in target {
        if !current.iter().any(|current_resource| {
            current_resource.identity().cmp(&target_resource.identity()) == Ordering::Equal
        }) {
            actions.push(ResourceAction::Add(target_resource.identity().to_owned()));
        }
    }
    actions.sort_by(|left, right| action_identity(left).cmp(&action_identity(right)));
    actions
}

fn assert_projection_is_strictly_ordered(resources: &[RuntimeResourceSpec<'_>]) {
    for pair in resources.windows(2) {
        assert_eq!(
            pair[0].identity().cmp(&pair[1].identity()),
            Ordering::Less,
            "Instance/Flow resource projection is not strictly ordered"
        );
    }
}

fn assert_actions_are_strictly_ordered(actions: &[ResourceAction]) {
    for pair in actions.windows(2) {
        assert_eq!(
            action_identity(&pair[0]).cmp(&action_identity(&pair[1])),
            Ordering::Less,
            "Instance/Flow resource actions contain duplicate or unordered identities"
        );
    }
}

fn action_identity(action: &ResourceAction) -> RuntimeResourceIdentityRef<'_> {
    let identity = match action {
        ResourceAction::Add(identity)
        | ResourceAction::Replace(identity)
        | ResourceAction::Remove(identity) => identity,
    };
    match identity {
        RuntimeResourceIdentity::PluginProcess { instance_id } => {
            RuntimeResourceIdentityRef::PluginProcess { instance_id }
        }
        RuntimeResourceIdentity::SinkBellRegion { instance_id } => {
            RuntimeResourceIdentityRef::SinkBellRegion { instance_id }
        }
        RuntimeResourceIdentity::FlowBellRegion { flow_id } => {
            RuntimeResourceIdentityRef::FlowBellRegion { flow_id }
        }
        RuntimeResourceIdentity::SourceQueuePair {
            flow_id,
            channel_index,
        } => RuntimeResourceIdentityRef::SourceQueuePair {
            flow_id,
            channel_index: *channel_index,
        },
        RuntimeResourceIdentity::FlowChannel {
            flow_id,
            channel_index,
        } => RuntimeResourceIdentityRef::FlowChannel {
            flow_id,
            channel_index: *channel_index,
        },
        RuntimeResourceIdentity::EgressQueue {
            instance_id,
            flow_id,
            channel_index,
        } => RuntimeResourceIdentityRef::EgressQueue {
            instance_id,
            flow_id,
            channel_index: *channel_index,
        },
        RuntimeResourceIdentity::FlowRoute {
            flow_id,
            sink_contract_id,
        } => RuntimeResourceIdentityRef::FlowRoute {
            flow_id,
            sink_contract_id: SinkContractIdRef::new(
                sink_contract_id.program_name(),
                sink_contract_id.exact_version(),
            ),
        },
    }
}

pub(in crate::pipeline::reconfigure) fn base_revision(
    etag: &str,
) -> io::Result<PipelineRevisionPlan> {
    Ok(PipelineRevisionPlan {
        document_etag: etag.into(),
        tenon_document_json: serde_json::to_string(&json!({
            "specVersion": "1",
            "id": "resource-plan-test",
            "pluginInstances": {
                "source-a": instance("com.example.source", "source-a"),
                "source-b": instance("com.example.source", "source-b"),
                "output-a": instance("com.example.output", "output-a"),
                "output-b": instance("com.example.output", "output-b"),
                "archive": instance("com.example.archive", "archive")
            },
            "flows": {
                "telemetry": flow("source-a", 1, &["output-a"]),
                "audit": flow("source-b", 2, &["output-b", "archive"])
            }
        }))?,
        plugin_programs: vec![
            program("com.example.source", "1.0.0", PluginInterface::Source)?,
            program("com.example.output", "1.0.0", PluginInterface::Sink)?,
            program("com.example.archive", "1.0.0", PluginInterface::Sink)?,
        ],
    })
}

fn single_flow_revision(etag: &str) -> io::Result<PipelineRevisionPlan> {
    let mut revision = base_revision(etag)?;
    let mut document: Value = serde_json::from_str(&revision.tenon_document_json)?;
    remove_document_member(&mut document, "flows", "audit")?;
    for instance in ["source-b", "output-b", "archive"] {
        remove_document_member(&mut document, "pluginInstances", instance)?;
    }
    revision.tenon_document_json = serde_json::to_string(&document)?;
    revision
        .plugin_programs
        .retain(|program| program.program_name != "com.example.archive");
    Ok(revision)
}

fn shared_sink_revision(etag: &str) -> io::Result<PipelineRevisionPlan> {
    let mut revision = base_revision(etag)?;
    let mut document: Value = serde_json::from_str(&revision.tenon_document_json)?;
    remove_document_member(&mut document, "pluginInstances", "output-b")?;
    document["flows"]["audit"]["sinks"] = json!(["output-a", "archive"]);
    revision.tenon_document_json = serde_json::to_string(&document)?;
    Ok(revision)
}

fn remove_document_member(document: &mut Value, collection: &str, member: &str) -> io::Result<()> {
    document[collection]
        .as_object_mut()
        .and_then(|members| members.remove(member))
        .map(|_| ())
        .ok_or_else(|| io::Error::other("fixture document member is missing"))
}

fn dual_revision(etag: &str) -> io::Result<PipelineRevisionPlan> {
    Ok(PipelineRevisionPlan {
        document_etag: etag.into(),
        tenon_document_json: serde_json::to_string(&json!({
            "specVersion": "1",
            "id": "dual-resource-plan-test",
            "pluginInstances": {
                "gateway": instance("com.example.gateway", "gateway")
            },
            "flows": {
                "loop": flow("gateway", 1, &["gateway"])
            }
        }))?,
        plugin_programs: vec![program(
            "com.example.gateway",
            "1.0.0",
            PluginInterface::SourceAndSink,
        )?],
    })
}

pub(in crate::pipeline::reconfigure) fn instance(program_name: &str, endpoint: &str) -> Value {
    json!({
        "programName": program_name,
        "exactVersion": "1.0.0",
        "config": {"endpoint": endpoint}
    })
}

pub(in crate::pipeline::reconfigure) fn flow(
    source: &str,
    channel_count: u32,
    sinks: &[&str],
) -> Value {
    json!({
        "parallelism": f64::from(channel_count) / AVAILABLE_CPU_COUNT.get() as f64,
        "source": source,
        "process": {"script": "function main(event) emit() end"},
        "sinks": sinks
    })
}

pub(in crate::pipeline::reconfigure) fn program(
    name: &str,
    version: &str,
    interface: PluginInterface,
) -> io::Result<PluginProgramRuntime> {
    let contract_interface = match interface {
        PluginInterface::Source => ContractInterface::Source,
        PluginInterface::Sink => ContractInterface::Sink,
        PluginInterface::SourceAndSink => ContractInterface::SourceAndSink,
    };
    Ok(PluginProgramRuntime {
        program_name: name.into(),
        exact_version: version.into(),
        program_directory: format!("/plugins/programs/{name}/{version}"),
        command: vec!["./start".into()],
        plugin_interface: interface as i32,
        payload_descriptor_set: valid_program_descriptor(contract_interface)?,
    })
}

pub(in crate::pipeline::reconfigure) fn update_document(
    revision: &mut PipelineRevisionPlan,
    update: impl FnOnce(&mut Value),
) -> io::Result<()> {
    let mut document = serde_json::from_str(&revision.tenon_document_json)?;
    update(&mut document);
    revision.tenon_document_json = serde_json::to_string(&document)?;
    Ok(())
}

fn rename_flow(revision: &mut PipelineRevisionPlan, from: &str, to: &str) -> io::Result<()> {
    let mut document: Value = serde_json::from_str(&revision.tenon_document_json)?;
    let flow = document["flows"]
        .as_object_mut()
        .and_then(|flows| flows.remove(from))
        .ok_or_else(|| io::Error::other("fixture Flow is missing"))?;
    document["flows"][to] = flow;
    revision.tenon_document_json = serde_json::to_string(&document)?;
    Ok(())
}

fn rename_instance(revision: &mut PipelineRevisionPlan, from: &str, to: &str) -> io::Result<()> {
    let mut document: Value = serde_json::from_str(&revision.tenon_document_json)?;
    let instance = document["pluginInstances"]
        .as_object_mut()
        .and_then(|instances| instances.remove(from))
        .ok_or_else(|| io::Error::other("fixture Instance is missing"))?;
    document["pluginInstances"][to] = instance;
    revision.tenon_document_json = serde_json::to_string(&document)?;
    Ok(())
}

fn select_program_version(
    revision: &mut PipelineRevisionPlan,
    instance: &str,
    program_name: &str,
    exact_version: &str,
    descriptor: Option<Vec<u8>>,
) -> io::Result<()> {
    let mut document: Value = serde_json::from_str(&revision.tenon_document_json)?;
    let previous_version = document["pluginInstances"][instance]["exactVersion"]
        .as_str()
        .ok_or_else(|| io::Error::other("fixture exact version is missing"))?
        .to_owned();
    let mut runtime = revision
        .plugin_programs
        .iter()
        .find(|runtime| {
            runtime.program_name == program_name && runtime.exact_version == previous_version
        })
        .cloned()
        .ok_or_else(|| io::Error::other("fixture Program is missing"))?;
    document["pluginInstances"][instance]["exactVersion"] = json!(exact_version);
    let previous_still_used = document["pluginInstances"]
        .as_object()
        .ok_or_else(|| io::Error::other("fixture instances are missing"))?
        .values()
        .any(|candidate| {
            candidate["programName"] == program_name
                && candidate["exactVersion"] == previous_version
        });
    revision.tenon_document_json = serde_json::to_string(&document)?;
    if !previous_still_used {
        revision.plugin_programs.retain(|candidate| {
            candidate.program_name != program_name || candidate.exact_version != previous_version
        });
    }
    runtime.exact_version = exact_version.into();
    runtime.program_directory = format!("/plugins/programs/{program_name}/{exact_version}");
    if let Some(descriptor) = descriptor {
        runtime.payload_descriptor_set = descriptor;
    }
    revision.plugin_programs.push(runtime);
    Ok(())
}

fn changed_contract(descriptor: &[u8], root_name: &str) -> io::Result<Vec<u8>> {
    let mut descriptor = FileDescriptorSet::decode(descriptor).map_err(io::Error::other)?;
    let root = descriptor
        .file
        .iter_mut()
        .flat_map(|file| &mut file.message_type)
        .find(|message| message.name.as_deref() == Some(root_name))
        .ok_or_else(|| io::Error::other("fixture root message is missing"))?;
    let field = root
        .field
        .first_mut()
        .ok_or_else(|| io::Error::other("fixture root field is missing"))?;
    field.name = Some("changed_value".into());
    field.json_name = None;
    Ok(descriptor.encode_to_vec())
}

pub(in crate::pipeline::reconfigure) fn model(
    revision: PipelineRevisionPlan,
) -> Result<crate::pipeline::reconfigure::revision::PipelineRevision, Box<dyn Error>> {
    Ok(crate::pipeline::reconfigure::revision::PipelineRevision::from_runner(revision))
}

fn instance_id(value: &str) -> Result<PluginInstanceId, Box<dyn Error>> {
    PluginInstanceId::try_from(value.to_owned()).map_err(Into::into)
}

fn flow_id(value: &str) -> Result<FlowId, Box<dyn Error>> {
    FlowId::try_from(value.to_owned()).map_err(Into::into)
}

fn sink_contract_id(value: &str) -> Result<SinkContractId, Box<dyn Error>> {
    SinkContractId::try_from(value).map_err(Into::into)
}
