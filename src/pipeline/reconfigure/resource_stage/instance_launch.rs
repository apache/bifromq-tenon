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

//! Derives initial and retry launch material from the sole complete revision.

use std::num::NonZeroUsize;
use std::path::Path;

use crate::identifiers::FlowId;
use crate::identifiers::PluginInstanceId;
use crate::payload_contract::PluginInterface;
use crate::pipeline::plugin::{
    ControlledPluginLaunch, PluginBells, PluginControlLauncher, PluginLaunch, SinkChannel,
};
use crate::pipeline::reconfigure::revision::PipelineRevision;
use crate::pipeline::reconfigure::runtime_files::{
    INSTANCES_DIRECTORY_NAME, flow_channel_bell_path, instance_working_directory,
};

/// Returns every Sink input Channel of one Plugin Instance in startup order.
///
/// The order is the target Document's Flow id order, then the Channel index,
/// and it is the one order that the startup record the Instance receives and
/// the Egress Queues the Runner creates both share. Which slot a Sink publishes
/// is the Sink's own choice: the Sink contract runs one Egress loop over every
/// Egress Queue, so it points every handle at that loop's single doorbell and
/// the Runner owes a one-slot region, not one numbered by this list.
pub(super) fn sink_channels_of(
    target: &PipelineRevision,
    instance_id: &PluginInstanceId,
    available_cpu_count: NonZeroUsize,
) -> Vec<(FlowId, u32)> {
    target
        .document()
        .flows()
        .iter()
        .filter(|(_, flow)| flow.sinks().contains(instance_id))
        .flat_map(|(flow_id, _)| {
            (0..target
                .document()
                .channel_count(flow_id, available_cpu_count)
                .get())
                .map(move |channel_id| (flow_id.clone(), channel_id))
        })
        .collect()
}

pub(super) fn build_instance_launch<'a>(
    target: &'a PipelineRevision,
    pipeline_directory: &Path,
    instance_id: &PluginInstanceId,
    control: &'a PluginControlLauncher,
    available_cpu_count: NonZeroUsize,
) -> ControlledPluginLaunch<'a> {
    let instance = &target.document().plugin_instances()[instance_id];
    let program = target.program_for_instance(instance_id);
    let source_channel_region = target
        .document()
        .flows()
        .iter()
        .find(|(_, flow)| flow.source() == instance_id)
        .map(|(flow_id, _)| flow_channel_bell_path(pipeline_directory, flow_id.as_str()));
    let sink_inputs: Vec<_> = sink_channels_of(target, instance_id, available_cpu_count)
        .into_iter()
        .map(|(flow_id, channel_id)| SinkChannel {
            channel_bell_path: flow_channel_bell_path(pipeline_directory, flow_id.as_str()),
            flow_id,
            channel_id,
        })
        .collect();
    let interface = match (source_channel_region.is_some(), !sink_inputs.is_empty()) {
        (true, true) => PluginInterface::SourceAndSink,
        (true, false) => PluginInterface::Source,
        (false, true) => PluginInterface::Sink,
        (false, false) => unreachable!("a verified Instance is referenced by at least one Flow"),
    };
    let sink_inputs = (!sink_inputs.is_empty()).then_some(sink_inputs);
    ControlledPluginLaunch::new(
        PluginLaunch {
            program_directory: program.program_directory(),
            command: program.command(),
            working_directory: instance_working_directory(
                &pipeline_directory.join(INSTANCES_DIRECTORY_NAME),
                instance_id,
            ),
            config: instance.config(),
            extra_args: instance.extra_args(),
            env: instance.env(),
            bells: PluginBells {
                source_channel_region,
                sink_inputs,
            },
        },
        interface,
        control,
    )
}
