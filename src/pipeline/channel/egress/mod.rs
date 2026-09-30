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
use std::path::PathBuf;
use std::sync::Arc;

use super::metrics::{ChannelMetrics, TargetMetrics, WaitKind};
use crate::contracts::sink::EncodedEgressRecord;
use crate::identifiers::{PluginInstanceId, SinkContractId};
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

/// The normal result of one Egress fan-out attempt.
pub(super) enum SendOutcome {
    /// Every target committed the complete record at these release boundaries,
    /// in target order.
    Committed(Vec<(PluginInstanceId, WriteReceipt)>),
    /// Planned stop interrupted the capacity wait; no target was committed.
    Interrupted,
}

/// One accepted Egress boundary whose Source Completions still wait for the Sink.
///
/// The Channel keeps these in emit order and settles them from the front, so
/// Source Completions keep record order. Egress only answers whether the Sink
/// released the exact records this boundary committed.
pub(super) struct PendingRelease {
    sink_contract_id: SinkContractId,
    targets: Vec<(PluginInstanceId, WriteReceipt)>,
    record_ids: Vec<u64>,
}

impl PendingRelease {
    pub(super) fn new(
        sink_contract_id: SinkContractId,
        targets: Vec<(PluginInstanceId, WriteReceipt)>,
        record_ids: Vec<u64>,
    ) -> Self {
        Self {
            sink_contract_id,
            targets,
            record_ids,
        }
    }

    /// Returns the Source records whose Completions this boundary owns.
    pub(super) fn into_record_ids(self) -> Vec<u64> {
        self.record_ids
    }
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

    pub(super) fn send(
        &mut self,
        record: EncodedEgressRecord,
        metrics: &ChannelMetrics,
        stopped: impl Fn() -> bool,
    ) -> Result<SendOutcome, EgressError> {
        let payload_bytes = record.payload().len();
        for target in self.targets.values_mut() {
            let mut wait = metrics.wait(WaitKind::EgressCapacity(&target.metrics));
            loop {
                if stopped() {
                    return Ok(SendOutcome::Interrupted);
                }
                if target
                    .writer
                    .wait_writable_observed(record.len(), || wait.blocked())
                    .map_err(EgressError)?
                    == WaitOutcome::Ready
                {
                    wait.ready();
                    break;
                }
            }
        }
        if stopped() {
            return Ok(SendOutcome::Interrupted);
        }
        let mut receipts = Vec::with_capacity(self.targets.len());
        for (instance, target) in &mut self.targets {
            let outcome = target
                .writer
                .try_write_observed(record.as_bytes(), || {
                    metrics.egress(&target.metrics, payload_bytes);
                })
                .map_err(EgressError)?;
            assert!(
                matches!(outcome, WriteOutcome::Committed(_)),
                "exclusive writers cannot lose space after every target is writable"
            );
            if let WriteOutcome::Committed(receipt) = outcome {
                receipts.push((instance.clone(), receipt));
            }
        }
        Ok(SendOutcome::Committed(receipts))
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
    let route = routes
        .get(&pending.sink_contract_id)
        .expect("an accepted boundary keeps its route until the Channel settles it");
    for (instance, receipt) in &pending.targets {
        let target = route
            .targets
            .get(instance)
            .expect("an accepted boundary keeps its targets until the Channel settles it");
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
        .any(|(instance, _)| departed(instance))
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
    let (instance, _) = pending
        .targets
        .first()
        .expect("an accepted boundary has at least one target");
    &routes
        .get(&pending.sink_contract_id)
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
