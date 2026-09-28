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

//! Builds one paused data plane from the target Flow material.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use crate::config::ScriptVmLimits;
use crate::contracts::core::PipelineEnvironment;
use crate::identifiers::{FlowId, SinkContractId};
use crate::pipeline::channel::metrics::FlowMetrics;
use crate::pipeline::channel::{FlowChannelSpec, PreparedEgressRoutes};
use crate::pipeline::diagnostics::PipelineDiagnosticsPublisher;
use crate::pipeline::reconfigure::PipelineReconfigureError;
use crate::pipeline::reconfigure::plan::CompiledResourceChanges;
use crate::pipeline::reconfigure::revision::PipelineRevision;
use crate::pipeline::reconfigure::runtime_files::SOURCE_DIRECTORY_NAME;
use crate::pipeline::runtime::{
    FlowRuntimeSpec, PipelineRuntime, PreparedPipelineRuntime, StartupControl,
};

use super::directories::ResourceDirectories;
use super::queue_layout::{
    PreparedQueueLayout, create_source_channels, prepare_routes, staged_instance_directory,
};

/// Freezes the same Flow material for both new Channels and in-place VM replacement.
#[allow(
    clippy::expect_used,
    reason = "validated Flow bindings retain their interface roots"
)]
pub(in crate::pipeline::reconfigure) fn channel_spec(
    target: &PipelineRevision,
    flow_id: &FlowId,
    limits: ScriptVmLimits,
) -> FlowChannelSpec {
    let flow = &target.document().flows()[flow_id];
    let source = target
        .program_for_instance(flow.source())
        .payload_contract()
        .source_root_message()
        .expect("a received Source binding must retain its Source Contract root");
    let sinks = flow
        .sinks()
        .iter()
        .map(|id| {
            let instance = &target.document().plugin_instances()[id];
            let contract = SinkContractId::from_parts(
                instance.program_name().clone(),
                instance.exact_version().clone(),
            );
            let root = target
                .program_for_instance(id)
                .payload_contract()
                .sink_root_message()
                .expect("a received Sink binding must retain its Sink Contract root");
            (contract, root)
        })
        .collect();
    FlowChannelSpec::new(
        flow.source().as_str(),
        flow.lua_source(),
        limits,
        flow.max_record_bytes(),
        source,
        sinks,
        flow.delivery(),
    )
}

#[allow(
    clippy::expect_used,
    reason = "the compiled plan guarantees prepared routes for each new Flow"
)]
pub(super) fn prepare(
    changes: &CompiledResourceChanges,
    environment: &PipelineEnvironment,
    diagnostics: PipelineDiagnosticsPublisher,
    working_directory: &mut ResourceDirectories,
    startup: Arc<StartupControl>,
    retained: Option<Instant>,
    metrics: Option<&FlowMetrics>,
) -> Result<
    (
        PreparedPipelineRuntime,
        BTreeMap<FlowId, Vec<PreparedEgressRoutes>>,
    ),
    PipelineReconfigureError,
> {
    let root = working_directory.path().to_owned();
    let PreparedQueueLayout {
        mut channel_routes,
        flow_bells,
    } = prepare_routes(changes, environment, working_directory)?;
    let target = &changes.target;
    let lua_limits = environment.lua_limits();
    let mut flows = BTreeMap::new();
    for flow_id in changes.new_queue_flows() {
        let flow = &target.document().flows()[flow_id];
        let source_directory =
            staged_instance_directory(changes, &root, flow.source()).join(SOURCE_DIRECTORY_NAME);
        let routes = channel_routes
            .remove(flow_id)
            .expect("a new Flow has prepared routes");
        let channels = create_source_channels(
            &source_directory,
            flow.max_pending_records(),
            flow.max_record_bytes(),
            routes,
            &flow_bells[flow_id],
        )?;
        let spec = channel_spec(target, flow_id, lua_limits);
        let prior = flows.insert(
            flow_id.clone(),
            FlowRuntimeSpec::new(
                spec,
                channels,
                metrics.map(|metrics| metrics.flow(flow_id.as_str())),
            ),
        );
        assert!(
            prior.is_none(),
            "a validated revision cannot repeat a Flow id"
        );
    }

    let runtime = PipelineRuntime::prepare_resources(diagnostics, flows, retained, startup)
        .map_err(PipelineReconfigureError::RuntimeStart)?;
    Ok((runtime, channel_routes))
}
