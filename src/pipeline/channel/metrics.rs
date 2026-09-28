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

//! Core-owned Flow observations. Workers publish scalar VM snapshots and own
//! Channel registrations; collection only reads those snapshots and Queue headers.
//! Registries hold weak references, so metrics cannot retain business resources.

use crate::metrics::observations::Observations;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError, Weak};
use std::time::Instant;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};

use crate::lua::EmitBoundary;
use crate::lua::metrics::{LuaFlowMetrics, LuaMetrics, VmMetrics};
use crate::metrics::queue::{QueueMetric, QueueMetrics};
use tenon_ipc::queue::QueueObserver;

/// Pipeline-scoped instruments and the live Flow registry.
#[derive(Clone, Debug)]
pub(crate) struct FlowMetrics {
    instruments: Arc<Instruments>,
    flows: Arc<Mutex<HashMap<String, Weak<FlowObservation>>>>,
}

impl FlowMetrics {
    pub(crate) fn new(meter: &Meter) -> Self {
        let flows = Arc::new(Mutex::new(HashMap::<String, Weak<FlowObservation>>::new()));
        let observed = Arc::clone(&flows);
        meter
            .u64_observable_gauge("tenon.flow.waiting")
            .with_unit("1")
            .with_callback(move |observer| {
                visit_channels(&observed, |channel| {
                    observer.observe(channel.waiting.load(Ordering::Relaxed), &channel.attributes);
                });
            })
            .build();
        Self {
            instruments: Arc::new(Instruments::new(meter)),
            flows,
        }
    }

    pub(crate) fn flow(&self, id: &str) -> Arc<FlowObservation> {
        let mut flows = lock(&self.flows);
        if let Some(flow) = flows.get(id).and_then(Weak::upgrade) {
            return flow;
        }
        if flows.len() == flows.capacity() {
            flows.retain(|_, flow| flow.strong_count() != 0);
            let capacity = flows.capacity();
            // Amortize cleanup and bound retired identities between exports.
            if flows.len() > capacity / 2 {
                flows.reserve(capacity);
            }
        }
        let flow = Arc::new(FlowObservation {
            attributes: [KeyValue::new("tenon.flow.id", Arc::<str>::from(id))],
            instruments: Arc::clone(&self.instruments),
            lua: self.instruments.lua.flow(id),
            channels: Observations::new(),
        });
        flows.insert(id.to_owned(), Arc::downgrade(&flow));
        flow
    }
}

/// A Flow registration survives VM rebuild gaps and retires with its last owner.
#[derive(Debug)]
pub(crate) struct FlowObservation {
    attributes: [KeyValue; 1],
    instruments: Arc<Instruments>,
    lua: LuaFlowMetrics,
    channels: Observations<ChannelObservation>,
}

impl FlowObservation {
    pub(crate) fn channel(self: &Arc<Self>, index: u32) -> ChannelMetrics {
        ChannelMetrics {
            observation: Some(Arc::new(ChannelObservation {
                attributes: [
                    self.attributes[0].clone(),
                    KeyValue::new("tenon.channel.index", i64::from(index)),
                ],
                flow: Arc::clone(self),
                waiting: AtomicU64::new(0),
            })),
            queues: Vec::new(),
        }
    }
}

/// One worker's measurement context; only bind grants Gauge publication rights.
#[derive(Default, Debug)]
pub(crate) struct ChannelMetrics {
    observation: Option<Arc<ChannelObservation>>,
    queues: Vec<Arc<QueueMetric>>,
}

impl ChannelMetrics {
    pub(crate) fn bind(&self) {
        if let Some(channel) = &self.observation {
            channel.flow.channels.register(channel);
        }
    }

    pub(crate) fn queues(&mut self, queues: impl Iterator<Item = Arc<QueueMetric>>) {
        self.queues = queues.collect();
    }

    pub(crate) fn queue(
        &self,
        kind: &'static str,
        source: &str,
        queue: QueueObserver,
    ) -> Option<Arc<QueueMetric>> {
        self.observation.as_ref().map(|channel| {
            let mut attributes = vec![
                channel.attributes[0].clone(),
                channel.attributes[1].clone(),
                KeyValue::new("tenon.queue.kind", kind),
            ];
            attributes.push(KeyValue::new("tenon.plugin.instance.id", source.to_owned()));
            channel.flow.instruments.queues.register(attributes, queue)
        })
    }

    pub(crate) fn egress_queue(
        &self,
        target: &TargetMetrics,
        queue: QueueObserver,
    ) -> Option<Arc<QueueMetric>> {
        self.observation
            .as_ref()
            .zip(target.0.as_ref())
            .map(|(channel, attributes)| {
                let mut attributes = attributes.to_vec();
                attributes.push(KeyValue::new("tenon.queue.kind", "egress"));
                channel.flow.instruments.queues.register(attributes, queue)
            })
    }

    pub(crate) fn target(&self, id: &str) -> TargetMetrics {
        TargetMetrics(self.observation.as_ref().map(|channel| {
            [
                channel.attributes[0].clone(),
                channel.attributes[1].clone(),
                KeyValue::new("tenon.plugin.instance.id", Arc::<str>::from(id)),
            ]
        }))
    }

    pub(crate) fn input(&self, bytes: usize) {
        if let Some(channel) = &self.observation {
            channel
                .flow
                .instruments
                .input_records
                .add(1, &channel.attributes);
            channel
                .flow
                .instruments
                .input_bytes
                .add(bytes as u64, &channel.attributes);
        }
    }

    pub(crate) fn error(&self, phase: &'static str, kind: &'static str) {
        if let Some(channel) = &self.observation {
            channel.flow.instruments.errors.add(
                1,
                &[
                    channel.attributes[0].clone(),
                    channel.attributes[1].clone(),
                    KeyValue::new("phase", phase),
                    KeyValue::new("error.type", kind),
                ],
            );
        }
    }

    pub(crate) fn emits(&self, boundaries: &[EmitBoundary]) {
        if let Some(channel) = &self.observation {
            let payloads = boundaries
                .iter()
                .filter(|boundary| matches!(boundary, EmitBoundary::Payload { .. }))
                .count() as u64;
            for (kind, count) in [
                ("payload", payloads),
                ("boundary", boundaries.len() as u64 - payloads),
            ] {
                if count != 0 {
                    channel.flow.instruments.emits.add(
                        count,
                        &[
                            channel.attributes[0].clone(),
                            channel.attributes[1].clone(),
                            KeyValue::new("kind", kind),
                        ],
                    );
                }
            }
        }
    }

    pub(crate) fn completion(&self, result: &'static str) {
        if let Some(channel) = &self.observation {
            channel.flow.instruments.completion_records.add(
                1,
                &[
                    channel.attributes[0].clone(),
                    channel.attributes[1].clone(),
                    KeyValue::new("result", result),
                ],
            );
        }
    }

    pub(crate) fn egress(&self, target: &TargetMetrics, bytes: usize) {
        if let (Some(channel), Some(attributes)) = (&self.observation, &target.0) {
            channel.flow.instruments.egress_records.add(1, attributes);
            channel
                .flow
                .instruments
                .egress_bytes
                .add(bytes as u64, attributes);
        }
    }

    pub(crate) fn lua(&self) -> Option<VmMetrics> {
        self.observation
            .as_ref()
            .map(|channel| channel.flow.lua.vm(channel.attributes.clone()))
    }

    pub(crate) fn wait<'a>(&'a self, kind: WaitKind<'a>) -> WaitMeasurement<'a> {
        WaitMeasurement {
            channel: self.observation.as_deref(),
            kind,
            started: None,
        }
    }

    /// Reports platform wakes that found no subscribed fact for this Channel.
    ///
    /// One doorbell serves every fact the loop subscribes to, so a ring for
    /// another loop's fact, or an unconditional recovery wake, costs a full
    /// recheck and completes nothing. This is that funnel cost.
    pub(crate) fn spurious_wakes(&self, count: u64) {
        if count == 0 {
            return;
        }
        if let Some(channel) = &self.observation {
            channel
                .flow
                .instruments
                .spurious_wakes
                .add(count, &channel.attributes);
        }
    }
}

#[derive(Debug)]
struct ChannelObservation {
    attributes: [KeyValue; 2],
    flow: Arc<FlowObservation>,
    // These are snapshots of worker-owned observations at distinct times.
    waiting: AtomicU64,
}

/// Frozen target attributes avoid allocating identities for each output record.
#[derive(Debug)]
pub(crate) struct TargetMetrics(Option<[KeyValue; 3]>);

#[derive(Clone, Copy, Debug)]
pub(crate) enum WaitKind<'a> {
    EgressCapacity(&'a TargetMetrics),
    SinkRelease(&'a TargetMetrics),
    CompletionCapacity,
    CompletionDrain,
    /// The Channel's single event-loop park, waiting for any subscribed fact.
    Idle,
}

impl WaitKind<'_> {
    const fn name(self) -> &'static str {
        match self {
            Self::EgressCapacity(_) => "egress_capacity",
            Self::SinkRelease(_) => "sink_release",
            Self::CompletionCapacity => "completion_capacity",
            Self::CompletionDrain => "completion_drain",
            Self::Idle => "idle",
        }
    }

    const fn value(self) -> u64 {
        match self {
            Self::EgressCapacity(_) => 1,
            Self::SinkRelease(_) => 2,
            Self::CompletionCapacity => 3,
            Self::CompletionDrain => 4,
            Self::Idle => 5,
        }
    }
}

/// One actual shortage, including interruptions and spurious wakeups.
/// Drop records cancellation on stop or Queue error; ready ends the span early.
pub(crate) struct WaitMeasurement<'a> {
    channel: Option<&'a ChannelObservation>,
    kind: WaitKind<'a>,
    started: Option<Instant>,
}

impl WaitMeasurement<'_> {
    pub(crate) fn blocked(&mut self) {
        if let Some(channel) = self.channel
            && self.started.is_none()
        {
            self.started = Some(Instant::now());
            channel.waiting.store(self.kind.value(), Ordering::Relaxed);
        }
    }

    pub(crate) fn ready(&mut self) {
        self.finish("ready");
    }

    fn finish(&mut self, result: &'static str) {
        if let Some(started) = self.started.take()
            && let Some(channel) = self.channel
        {
            let elapsed = started.elapsed().as_secs_f64();
            channel.waiting.store(0, Ordering::Relaxed);
            let attributes = [
                channel.attributes[0].clone(),
                channel.attributes[1].clone(),
                KeyValue::new("wait.kind", self.kind.name()),
                KeyValue::new("result", result),
            ];
            match self.kind {
                WaitKind::EgressCapacity(target) | WaitKind::SinkRelease(target) => {
                    if let Some(target) = &target.0 {
                        channel.flow.instruments.wait_duration.record(
                            elapsed,
                            &[
                                attributes[0].clone(),
                                attributes[1].clone(),
                                attributes[2].clone(),
                                attributes[3].clone(),
                                target[2].clone(),
                            ],
                        );
                    }
                }
                WaitKind::Idle | WaitKind::CompletionCapacity | WaitKind::CompletionDrain => {
                    channel
                        .flow
                        .instruments
                        .wait_duration
                        .record(elapsed, &attributes);
                }
            }
        }
    }
}

impl Drop for WaitMeasurement<'_> {
    fn drop(&mut self) {
        self.finish("cancelled");
    }
}

#[derive(Debug)]
struct Instruments {
    input_records: Counter<u64>,
    input_bytes: Counter<u64>,
    errors: Counter<u64>,
    lua: LuaMetrics,
    queues: QueueMetrics,
    emits: Counter<u64>,
    completion_records: Counter<u64>,
    egress_records: Counter<u64>,
    egress_bytes: Counter<u64>,
    wait_duration: Histogram<f64>,
    spurious_wakes: Counter<u64>,
}

impl Instruments {
    fn new(meter: &Meter) -> Self {
        Self {
            input_records: meter
                .u64_counter("tenon.flow.input.records")
                .with_unit("{record}")
                .build(),
            input_bytes: meter
                .u64_counter("tenon.flow.input.bytes")
                .with_unit("By")
                .build(),
            errors: meter
                .u64_counter("tenon.flow.errors")
                .with_unit("{error}")
                .build(),
            lua: LuaMetrics::new(meter),
            queues: QueueMetrics::new(meter),
            emits: meter
                .u64_counter("tenon.flow.emits")
                .with_unit("{emit}")
                .build(),
            completion_records: meter
                .u64_counter("tenon.flow.completion.records")
                .with_unit("{record}")
                .build(),
            egress_records: meter
                .u64_counter("tenon.flow.egress.records")
                .with_unit("{record}")
                .build(),
            egress_bytes: meter
                .u64_counter("tenon.flow.egress.bytes")
                .with_unit("By")
                .build(),
            wait_duration: meter
                .f64_histogram("tenon.flow.wait.duration")
                .with_unit("s")
                .with_boundaries(Vec::new())
                .build(),
            spurious_wakes: meter
                .u64_counter("tenon.flow.wake.spurious")
                .with_unit("{wake}")
                .build(),
        }
    }
}

#[allow(
    clippy::panic,
    reason = "poisoning proves an internal Flow registry invariant failed"
)]
fn visit_channels(
    flows: &Mutex<HashMap<String, Weak<FlowObservation>>>,
    mut visit: impl FnMut(&ChannelObservation),
) {
    let snapshot = {
        let mut flows = match flows.try_lock() {
            Ok(flows) => flows,
            Err(TryLockError::WouldBlock) => return,
            Err(TryLockError::Poisoned(_)) => panic!("Flow observation registry poisoned"),
        };
        let mut snapshot = Vec::with_capacity(flows.len());
        flows.retain(|_, weak| {
            let Some(flow) = weak.upgrade() else {
                return false;
            };
            snapshot.push(flow);
            true
        });
        snapshot
    };
    for flow in snapshot {
        if let Some(channels) = flow.channels.try_snapshot() {
            for channel in channels {
                visit(&channel);
            }
        }
    }
}

#[allow(
    clippy::expect_used,
    reason = "observation locks never span user code or await"
)]
fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value.lock().expect("Flow observation lock poisoned")
}
