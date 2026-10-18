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

//! Exclusive per-Channel Queue writers and their finite replacement handoff.
//!
//! Lua has already admitted the record against the shared size limit.
//! All target writable positions are checked before the first commit.
//! A successful capacity wait stays valid because no other writer owns these
//! Queues and readers can only release more space. An OS failure after the first
//! commit is terminal; the enclosing Pipeline owns recovery, never rollback.

use std::collections::{BTreeMap, HashMap};
use std::error::Error;
use std::fmt;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use super::metrics::{ChannelMetrics, TargetMetrics, WaitKind};
use crate::contracts::sink::EncodedEgressRecord;
use crate::identifiers::{PluginInstanceId, SinkContractId};
use crate::lua::{EmitBoundary, EmitTargets};
use tenon_ipc::bell::{BellRegion, LoopBell, WaitOutcome};
use tenon_ipc::queue::QueueObserver;
use tenon_ipc::queue::{QueueRuntimeError, QueueWriter, WriteOutcome, WriteReceipt};

pub(crate) type EgressRoutes = HashMap<SinkContractId, EgressRoute>;
pub(crate) type PreparedEgressRoutes =
    HashMap<SinkContractId, BTreeMap<PluginInstanceId, PreparedEgressQueue>>;

/// New files are opened during Stage; retained files wait for exclusive handoff.
#[derive(Debug)]
pub(crate) enum PreparedEgressQueue {
    New(QueueWriter),
    Retained {
        path: PathBuf,
        peer_region: Arc<BellRegion>,
    },
}

pub(super) enum EgressHandoff {
    PreviousGenerationJoined,
    CurrentChannel(EgressRoutes),
}

/// One Lua output batch waiting for Sink results.
///
/// Each Queue contributes only its last receipt: releasing it also proves that
/// its earlier records finished. Batches may span several Sink Contracts.
pub(super) struct PendingRelease {
    targets: Vec<(SinkContractId, PluginInstanceId, WriteReceipt)>,
    record_ids: Vec<u64>,
}

impl PendingRelease {
    pub(super) fn set_record_ids(&mut self, record_ids: Vec<u64>) {
        self.record_ids = record_ids;
    }

    /// Returns the Source records whose Completions this batch owns.
    pub(super) fn into_record_ids(self) -> Vec<u64> {
        self.record_ids
    }
}

/// Result of checking and sending one Lua call's outputs.
pub(super) enum BatchSendOutcome {
    Committed(PendingRelease),
    TooLarge,
    Interrupted,
}

/// Checks every Queue before writing any output. Repeated emits stay separate.
#[allow(clippy::expect_used, reason = "Lua validates every output target")]
pub(super) fn send_batch(
    routes: &mut EgressRoutes,
    boundaries: &[EmitBoundary],
    metrics: &ChannelMetrics,
    stopped: impl Fn() -> bool,
) -> Result<BatchSendOutcome, EgressError> {
    let mut targets: BTreeMap<PluginInstanceId, SinkContractId> = BTreeMap::new();
    for boundary in boundaries {
        if let EmitBoundary::Payload {
            sink_contract_id,
            instance_id,
            ..
        } = boundary
        {
            let route = routes.get(sink_contract_id).expect("validated contract");
            for instance in route
                .targets
                .keys()
                .filter(|id| instance_id.as_ref().is_none_or(|selected| selected == *id))
            {
                targets
                    .entry(instance.clone())
                    .or_insert_with(|| sink_contract_id.clone());
            }
        }
    }
    let mut spaces = Vec::with_capacity(targets.len());
    // Reject impossible batches before waiting on any Queue.
    for (instance, contract) in targets {
        let target = &routes[&contract].targets[&instance];
        // Read lengths directly from the output buffer instead of copying one
        // length per output per target for broadcasts.
        let lengths = boundaries.iter().filter_map(|boundary| match boundary {
            EmitBoundary::Payload {
                sink_contract_id,
                instance_id,
                record,
            } if sink_contract_id == &contract
                && instance_id.as_ref().is_none_or(|id| id == &instance) =>
            {
                Some(record.len())
            }
            _ => None,
        });
        let Some(space) = target.writer.batch_space(lengths)? else {
            return Ok(BatchSendOutcome::TooLarge);
        };
        spaces.push((contract, instance, space));
    }
    for (contract, instance, space) in spaces {
        let target = routes
            .get_mut(&contract)
            .expect("validated contract")
            .targets
            .get_mut(&instance)
            .expect("validated target");
        let mut wait = metrics.wait(WaitKind::EgressCapacity(&target.metrics));
        loop {
            if stopped() {
                return Ok(BatchSendOutcome::Interrupted);
            }
            if target
                .writer
                .wait_batch_writable_observed(space, || wait.blocked())?
                == WaitOutcome::Ready
            {
                wait.ready();
                break;
            }
        }
    }
    if stopped() {
        return Ok(BatchSendOutcome::Interrupted);
    }
    let mut receipts = BTreeMap::new();
    for boundary in boundaries {
        if let EmitBoundary::Payload {
            sink_contract_id,
            instance_id,
            record,
        } = boundary
        {
            let route = routes
                .get_mut(sink_contract_id)
                .expect("validated contract");
            for (instance, target) in &mut route.targets {
                if instance_id
                    .as_ref()
                    .is_none_or(|selected| selected == instance)
                {
                    receipts.insert(
                        instance.clone(),
                        (sink_contract_id.clone(), target.commit(record, metrics)?),
                    );
                }
            }
        }
    }
    Ok(BatchSendOutcome::Committed(PendingRelease {
        targets: receipts
            .into_iter()
            .map(|(instance, (contract, receipt))| (contract, instance, receipt))
            .collect(),
        record_ids: Vec::new(),
    }))
}

#[derive(Debug)]
pub(crate) struct EgressRoute {
    targets: BTreeMap<PluginInstanceId, EgressTarget>,
}

impl EgressRoute {
    pub(crate) fn new(
        targets: BTreeMap<PluginInstanceId, QueueWriter>,
        metrics: &ChannelMetrics,
    ) -> Self {
        assert!(
            !targets.is_empty(),
            "a validated Sink Contract has actual targets"
        );
        Self {
            targets: targets
                .into_iter()
                .map(|(id, writer)| {
                    let target = EgressTarget {
                        writer,
                        metrics: metrics.target(id.as_str()),
                    };
                    (id, target)
                })
                .collect(),
        }
    }

    pub(super) fn observations(&self) -> impl Iterator<Item = (&TargetMetrics, QueueObserver)> {
        self.targets
            .values()
            .map(|target| (&target.metrics, target.writer.observer()))
    }
}

/// Binds only after the previous Channel generation has joined. In-place
/// changes move retained writers from the same worker without opening a copy.
#[allow(
    clippy::expect_used,
    reason = "the compiled handoff retains only this Channel's writers"
)]
pub(super) fn bind_routes(
    prepared: PreparedEgressRoutes,
    handoff: EgressHandoff,
    bell: &Arc<LoopBell>,
    metrics: &ChannelMetrics,
) -> Result<EgressRoutes, EgressError> {
    let mut retained = match handoff {
        EgressHandoff::PreviousGenerationJoined => None,
        EgressHandoff::CurrentChannel(current) => Some(
            current
                .into_values()
                .flat_map(|route| {
                    route
                        .targets
                        .into_iter()
                        .map(|(id, target)| (id, target.writer))
                })
                .collect::<BTreeMap<_, _>>(),
        ),
    };
    let mut routes = HashMap::new();
    for (contract, targets) in prepared {
        let mut writers = BTreeMap::new();
        for (id, queue) in targets {
            let writer = match queue {
                PreparedEgressQueue::New(writer) => writer,
                PreparedEgressQueue::Retained { path, peer_region } => match &mut retained {
                    Some(writers) => writers
                        .remove(&id)
                        .expect("a retained in-place Queue belongs to this Channel"),
                    None => QueueWriter::open(path, Arc::clone(bell), peer_region)
                        .map_err(EgressError)?,
                },
            };
            writers.insert(id, writer);
        }
        routes.insert(contract, EgressRoute::new(writers, metrics));
    }
    Ok(routes)
}

#[derive(Debug)]
struct EgressTarget {
    writer: QueueWriter,
    metrics: TargetMetrics,
}

impl EgressTarget {
    fn commit(
        &mut self,
        record: &EncodedEgressRecord,
        metrics: &ChannelMetrics,
    ) -> Result<WriteReceipt, EgressError> {
        let outcome = self
            .writer
            .try_write_with(
                record.len(),
                |destination| Write::write_all(destination, record.as_bytes()),
                || metrics.egress(&self.metrics, record.payload().len()),
            )
            .map_err(EgressError)?;
        let WriteOutcome::Committed(receipt) = outcome else {
            unreachable!(
                "exclusive writers cannot lose space after every selected target is writable"
            );
        };
        Ok(receipt)
    }
}

/// Captures the bound instances for Lua calls in this route generation.
pub(super) fn emit_targets(routes: &EgressRoutes) -> EmitTargets {
    routes
        .iter()
        .flat_map(|(contract, route)| {
            route
                .targets
                .keys()
                .map(|id| (id.clone(), contract.clone()))
        })
        .collect()
}

/// Returns whether the Sink already released every target of one accepted
/// boundary. This check never waits and never changes Queue state, so an event
/// loop can fold it into its own condition set.
///
/// # Errors
///
/// Returns [`QueueRuntimeError`] when a live Queue position is corrupt or a receipt
/// no longer belongs to its opened writer.
#[allow(
    clippy::expect_used,
    reason = "the Channel settles every accepted boundary before it changes routes"
)]
pub(super) fn is_released(
    routes: &EgressRoutes,
    pending: &PendingRelease,
) -> Result<bool, QueueRuntimeError> {
    for (contract, instance, receipt) in &pending.targets {
        let target = &routes
            .get(contract)
            .expect("a pending batch keeps its route")
            .targets
            .get(instance)
            .expect("a pending batch keeps its targets");
        if !target.writer.is_released(receipt)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Returns whether one target Instance left the Pipeline.
///
/// A settlement wait subscribes to this fact for every target of its boundary.
/// One departure ends the whole boundary: that target's release can never
/// arrive, and the remaining targets must not hold the wait open for it.
pub(super) fn target_departed(
    pending: &PendingRelease,
    departed: impl Fn(&PluginInstanceId) -> bool,
) -> bool {
    pending
        .targets
        .iter()
        .any(|(_, instance, _)| departed(instance))
}

/// Returns the target metrics one settlement wait is attributed to.
///
/// A settlement wait parks once for the complete condition set of the oldest
/// boundary, so no single target owns it. It names that boundary's first target
/// in Instance order, which is a stable choice for the same boundary.
#[allow(
    clippy::expect_used,
    reason = "the Channel settles every accepted boundary before it changes routes"
)]
pub(super) fn settlement_metrics<'a>(
    routes: &'a EgressRoutes,
    pending: &PendingRelease,
) -> &'a TargetMetrics {
    let (contract, instance, _) = pending
        .targets
        .first()
        .expect("an accepted boundary has at least one target");
    &routes
        .get(contract)
        .expect("an accepted boundary keeps its route until the Channel settles it")
        .targets
        .get(instance)
        .expect("an accepted boundary keeps its targets until the Channel settles it")
        .metrics
}

#[derive(Debug)]
pub(crate) struct EgressError(QueueRuntimeError);

impl From<QueueRuntimeError> for EgressError {
    fn from(source: QueueRuntimeError) -> Self {
        Self(source)
    }
}

impl fmt::Display for EgressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Egress Queue operation failed")
    }
}

impl Error for EgressError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

#[cfg(all(test, not(feature = "loom-model")))]
mod tests;
