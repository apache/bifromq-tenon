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

//! Immutable definition shared by every FlowChannel in one Flow revision.
//!
//! The specification owns all inputs required to create or rebuild a FlowChannel
//! Lua VM. Runtime routes remain separate live owners; this value only
//! verifies that their Sink Contract identities match the frozen Payload registry.

use std::cell::Cell;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use prost_reflect::MessageDescriptor;

use super::metrics::ChannelMetrics;
use crate::config::ScriptVmLimits;
use crate::identifiers::SinkContractId;
use crate::lua::{LuaVm, LuaVmError, LuaVmErrorKind};
use crate::pipeline::diagnostics::{ChannelDiagnosticPublisher, LuaDiagnosticPublisher};

use crate::tenon_document::SourceDelivery;

/// Immutable inputs shared by every FlowChannel in one Flow revision.
#[derive(Clone, Debug)]
pub(crate) struct FlowChannelSpec {
    lua_source: Arc<str>,
    lua_limits: ScriptVmLimits,
    max_record_bytes: NonZeroU64,
    source_payload_root: MessageDescriptor,
    sink_payload_roots: Arc<HashMap<SinkContractId, MessageDescriptor>>,
    delivery: SourceDelivery,
}

impl FlowChannelSpec {
    /// Freezes the validated message roots used for initial load and VM rebuilds.
    pub(crate) fn new(
        lua_source: impl AsRef<str>,
        lua_limits: ScriptVmLimits,
        max_record_bytes: NonZeroU64,
        source_payload_root: MessageDescriptor,
        sink_payload_roots: HashMap<SinkContractId, MessageDescriptor>,
        delivery: SourceDelivery,
    ) -> Self {
        Self {
            lua_source: Arc::from(lua_source.as_ref()),
            lua_limits,
            max_record_bytes,
            source_payload_root,
            sink_payload_roots: Arc::new(sink_payload_roots),
            delivery,
        }
    }

    /// Reports whether runtime routes exactly match the frozen Sink Contract set.
    pub(crate) fn matches_routes<T>(&self, routes: &HashMap<SinkContractId, T>) -> bool {
        self.sink_payload_roots.len() == routes.len()
            && self
                .sink_payload_roots
                .keys()
                .all(|sink_contract_id| routes.contains_key(sink_contract_id))
    }

    pub(super) const fn delivery(&self) -> SourceDelivery {
        self.delivery
    }

    pub(super) fn load_vm(
        &self,
        diagnostics: ChannelDiagnosticPublisher,
        metrics: &ChannelMetrics,
        stop_requested: impl Fn() -> bool + 'static,
        timer_origin: Instant,
        event_order: Rc<Cell<u64>>,
    ) -> Result<(LuaVm, LuaDiagnosticPublisher), LuaVmError> {
        let lua_vm_diagnostics = diagnostics.lua_vm();
        let lua_vm = LuaVm::load_observed_at(
            &self.lua_source,
            self.lua_limits,
            self.max_record_bytes,
            self.source_payload_root.clone(),
            self.sink_payload_roots.as_ref().clone(),
            lua_vm_diagnostics.clone().into_print_callback(),
            stop_requested,
            metrics.lua(),
            timer_origin,
            event_order,
        )
        .inspect_err(|error| {
            if error.kind() != LuaVmErrorKind::ExecutionStopped {
                metrics.error("lua_initialize", error.kind().tenon_document_issue_code());
            }
        })?;
        Ok((lua_vm, lua_vm_diagnostics))
    }
}
