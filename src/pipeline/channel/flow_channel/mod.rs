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

//! One Flow Channel from an Ingress Queue pair through Lua to Egress and Completion.
//!
//! The outer Pipeline worker constructs and runs this value on one thread.
//! [`FlowChannel::run`] serializes Source and timer events through the same
//! Lua VM. The event loop parks on its own doorbell with one condition set that
//! covers every fact it subscribes to, and passes the Lua timer's exact
//! remaining duration as that park's only timeout. No timer thread, callback,
//! polling loop, or second Lua entry path exists.
//!
//! At-least-once record ids stay in one ordered `pending_records` vector until
//! the first emit boundary accepted after them. That boundary takes the whole
//! vector in one ownership move. Later boundaries from the same synchronous
//! `main` call therefore own no Source records. That boundary and its records
//! move into `pending_ack`, and its Completions are written only after the Sink
//! released every target. The Channel itself does not wait for that release: it
//! keeps reading Source records and emitting, so accepted output stays in flight
//! up to the Source's own `maxPendingRecords` permits instead of one record at a
//! time. `pending_ack` is settled oldest first, so an earlier boundary always
//! completes before a later one and no later Sink can drag or bypass it. A
//! completion-only boundary owns no EgressRecord and completes its records
//! immediately. At-most-once writes `OK` immediately after the Ingress adapter
//! has copied and released the Submission frame, before entering Lua.
//!
//! A Source payload that cannot be decoded never enters Lua. Under
//! at-least-once, if it is the only pending record, the channel reports `ERROR`
//! and preserves the VM. If older records are pending, those records become
//! `RETRY`, the current record becomes `ERROR`, and the VM is rebuilt because
//! its state already reflects work that was rolled back.
//!
//! Source replacement first stops the sole Submission writer, then requests a
//! drain. The channel finishes every committed old record without running old
//! timers, resolves any remaining at-least-once pending prefix as `RETRY`, and
//! exits only after old input and committed output have drained. Terminal stop
//! instead publishes Stop and rings the Channel's doorbell before the Pipeline
//! joins its Channel workers.
//!
//! A healthy old Source session can instead finish after quiescing its writer:
//! the same drain algorithm resolves its records, then waits for the Source
//! reader to consume every committed Completion. This one-shot command keeps
//! the Channel thread, Lua state, timer, routes, and Queues; the original event
//! loop resumes immediately, without a separate activation or pause state.
//!
//! Replacing one Channel definition keeps this thread and both Queues. The thread first
//! completes its current old-VM event, including accepted Egress responsibility.
//! It keeps the old VM running after creating a candidate VM, including while
//! finishing an old Source session. Once every sibling is prepared and cutover
//! is authorized, at-least-once input that has no emit boundary receives
//! `RETRY`, then installs the target Lua source, Payload registry, and route
//! bindings together. No candidate can process Source or timer events until
//! every Channel has installed the same replacement.

use std::borrow::Cow;
use std::cell::Cell;
use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::flow_channel_command::FlowChannelCommandControl;
use super::flow_channel_command::{
    CandidatePreparationOutcome, ChannelCommand, ChannelCommandInbox, ChannelDefinitionChange,
    ChannelReplacementRequest, ChannelReplacementSession, control_pair,
};
use super::flow_channel_error::FlowChannelError;
use super::flow_channel_spec::FlowChannelSpec;
use super::flow_control::{ChannelDirective, FlowChannelControl};
use super::metrics::{ChannelMetrics, WaitKind};
use super::queue_paths::{FlowChannelBells, FlowChannelQueuePaths};
use super::wake::{ChannelWake, EgressWaitRegistration};

use super::egress::{
    EgressError, EgressHandoff, EgressRoutes, PendingRelease, PreparedEgressRoutes, SendOutcome,
    bind_routes, is_released, settlement_metrics, target_departed,
};
use crate::contracts::source::{IngressCompletionStatus, IngressRecord};
use crate::lua::{EmitBoundary, LuaMainOutcome, LuaVm, LuaVmErrorKind, TimerSchedule};
use crate::pipeline::diagnostics::{ChannelDiagnosticPublisher, LuaDiagnosticPublisher};
use crate::pipeline::ingress_queue::{IngressCompletionWriteOutcome, IngressQueuePair};
use crate::tenon_document::SourceDelivery;
use tenon_ipc::bell::{LoopBell, TimedWaitOutcome, WaitOutcome};

/// Stable failure phases shared by this Channel's error metrics and diagnostics.
const SOURCE_DECODE_PHASE: &str = "decode";
const LUA_MAIN_PHASE: &str = "lua_main";

/// The normal result of one Source-side channel step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
enum FlowChannelStep {
    /// One Source or timer event finished processing.
    EventProcessed,
    /// Planned stop interrupted the current Queue wait.
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LuaReloadProgress {
    Reloaded,
    Stopped,
}

/// One fact this Channel's single event loop can act on right now.
///
/// Every variant is a fact the loop both dispatches on and resubscribes to, so
/// the doorbell it parks on never has to carry a cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChannelWork {
    /// The Flow directive is `Drain`: finish Source input and resolve its records.
    DrainSource,
    /// The Flow directive is `DrainEgress` or `Stop`: leave the event loop.
    LeaveLoop,
    /// The Sink already released the oldest accepted boundary.
    ///
    /// A boundary whose target left the Pipeline settles with the same status,
    /// but only the settlement paths wait for that fact. It is deliberately not
    /// work here: the loop completes what a release already paid for, so a
    /// departure cannot make the loop act without a new release.
    SettleReleased,
    /// A replacement or old-session command is waiting in the single slot.
    Command,
    /// The Lua timer's deadline arrived.
    TimerDue,
    /// One committed Source record is readable.
    SourceRecord,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SourceReadiness {
    observed_at: Instant,
    order: u64,
    generation: u64,
}

/// Thread-affine Lua and Source resources prepared before the old writer handoff.
pub(crate) struct PreparedFlowChannel {
    ingress: IngressQueuePair,
    pipeline_started_at: Instant,
    spec: FlowChannelSpec,
    diagnostics: ChannelDiagnosticPublisher,
    metrics: ChannelMetrics,
    lua_vm: LuaVm,
    lua_vm_diagnostics: LuaDiagnosticPublisher,
    routes: PreparedEgressRoutes,
    egress_waits: Arc<Mutex<EgressWaitRegistration>>,
    bell: Arc<LoopBell>,
    control: Arc<FlowChannelControl>,
    commands: ChannelCommandInbox,
    event_order: Rc<Cell<u64>>,
}

impl PreparedFlowChannel {
    /// The caller has joined the previous generation before granting this handoff.
    pub(crate) fn bind(self) -> Result<FlowChannel, FlowChannelError> {
        let routes = bind_routes(
            self.routes,
            EgressHandoff::PreviousGenerationJoined,
            &self.bell,
            &self.metrics,
        )
        .map_err(|source| FlowChannelError::EgressRelease { source })?;
        let mut channel = FlowChannel {
            ingress: self.ingress,
            pipeline_started_at: self.pipeline_started_at,
            spec: self.spec,
            diagnostics: self.diagnostics,
            metrics: self.metrics,
            lua_vm: Some(self.lua_vm),
            lua_vm_diagnostics: Some(self.lua_vm_diagnostics),
            routes,
            egress_waits: self.egress_waits,
            bell: self.bell,
            pending_records: Vec::new(),
            pending_ack: Vec::new(),
            control: self.control,
            commands: self.commands,
            candidate: None,
            source_readiness: Cell::new(None),
            next_event_order: self.event_order,
            vm_generation: Cell::new(0),
        };
        channel.metrics.bind();
        channel.bind_route_generation();
        Ok(channel)
    }
}

/// Single-thread owner of one Submission/Lua/Completion channel.
pub(crate) struct FlowChannel {
    ingress: IngressQueuePair,
    pipeline_started_at: Instant,
    spec: FlowChannelSpec,
    diagnostics: ChannelDiagnosticPublisher,
    metrics: ChannelMetrics,
    lua_vm: Option<LuaVm>,
    lua_vm_diagnostics: Option<LuaDiagnosticPublisher>,
    routes: EgressRoutes,
    egress_waits: Arc<Mutex<EgressWaitRegistration>>,
    bell: Arc<LoopBell>,
    pending_records: Vec<u64>,
    pending_ack: Vec<PendingRelease>,
    control: Arc<FlowChannelControl>,
    commands: ChannelCommandInbox,
    candidate: Option<PreparedChannelDefinition>,
    source_readiness: Cell<Option<SourceReadiness>>,
    next_event_order: Rc<Cell<u64>>,
    vm_generation: Cell<u64>,
}

impl FlowChannel {
    /// Opens one Queue pair and creates the channel's first Lua VM.
    ///
    /// `pipeline_started_at` must be the same origin supplied to every channel
    /// in the Pipeline so `event.timestamp` uses one monotonic millisecond axis.
    ///
    /// # Errors
    ///
    /// Returns [`FlowChannelError`] when the Queue pair or exact Lua runtime
    /// cannot be opened.
    #[expect(
        clippy::too_many_arguments,
        reason = "Channel preparation transfers independent resources and lifecycle controls"
    )]
    pub(crate) fn prepare(
        queue_paths: FlowChannelQueuePaths,
        bells: FlowChannelBells,
        pipeline_started_at: Instant,
        spec: FlowChannelSpec,
        diagnostics: ChannelDiagnosticPublisher,
        routes: PreparedEgressRoutes,
        control: Arc<FlowChannelControl>,
        startup_aborted: impl Fn() -> bool + 'static,
        metrics: ChannelMetrics,
    ) -> Result<(PreparedFlowChannel, ChannelWake, FlowChannelCommandControl), FlowChannelError>
    {
        let (bell, source_region) = bells.into_parts();
        let (submission_path, completion_path) = queue_paths.into_parts();
        let ingress =
            IngressQueuePair::open(submission_path, completion_path, &bell, &source_region)?;
        let (commands, command_control) = control_pair(bell.interrupter());
        let event_order = Rc::new(Cell::new(0));
        let execution_control = Arc::clone(&control);
        let (lua_vm, lua_vm_diagnostics) = spec
            .load_vm(
                diagnostics.clone(),
                &metrics,
                move || startup_aborted() || execution_control.is_stopping(),
                pipeline_started_at,
                Rc::clone(&event_order),
            )
            .map_err(|source| FlowChannelError::LuaVmLoad {
                kind: source.kind(),
            })?;
        let wake = ChannelWake::new(bell.interrupter());
        Ok((
            PreparedFlowChannel {
                ingress,
                pipeline_started_at,
                spec,
                diagnostics,
                metrics,
                lua_vm,
                lua_vm_diagnostics,
                routes,
                egress_waits: wake.egress_registration(),
                bell,
                control,
                commands,
                event_order,
            },
            wake,
            command_control,
        ))
    }

    /// Runs this channel's strict serial Source/timer event loop.
    ///
    /// The caller must dedicate the current thread to this method. The method
    /// returns only after planned stop or a terminal channel failure.
    ///
    /// # Errors
    ///
    /// Returns [`FlowChannelError`] for a terminal Queue, Lua, Egress,
    /// resource, or private invariant failure.
    pub(crate) fn run(mut self) -> Result<(), FlowChannelError> {
        let result = self.run_events();
        if result.is_ok() {
            self.drain_egress()?;
        }
        result
    }

    fn run_events(&mut self) -> Result<(), FlowChannelError> {
        loop {
            self.advance_replacement()?;
            let Some(work) = self.pending_work()? else {
                self.park_until_work()?;
                continue;
            };
            match work {
                ChannelWork::DrainSource => return self.drain_source_records().map(drop),
                ChannelWork::LeaveLoop => return Ok(()),
                ChannelWork::SettleReleased => {
                    if self.drain_released()? == CompletionProgress::Stopped {
                        return Ok(());
                    }
                }
                ChannelWork::Command => {
                    let Some(command) = self.commands.try_take() else {
                        continue;
                    };
                    match command {
                        ChannelCommand::Replace(request) => self.prepare_replacement(request)?,
                        ChannelCommand::FinishSourceSession(finished) => {
                            if self.finish_source_session()? == CompletionProgress::Completed {
                                let _ = finished.send(());
                            }
                        }
                    }
                }
                ChannelWork::TimerDue => {
                    if self.process_timer()? == FlowChannelStep::Stopped {
                        return Ok(());
                    }
                }
                ChannelWork::SourceRecord => {
                    if let Some(step) = self.process_available_source()?
                        && step == FlowChannelStep::Stopped
                    {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Reports the one fact the event loop must act on now, or `None` when every
    /// fact it subscribes to is still waiting.
    ///
    /// The loop head dispatches on exactly this value and its park rechecks it
    /// instead of a hand-written condition list, so the recheck set is the
    /// dispatch set by construction: a new fact must be added here, and its
    /// `match` arm in [`Self::run_events`] then cannot be omitted.
    ///
    /// A due Lua timer and a ready Source record are ordered by their eligible
    /// time and the Channel-local sequence tie-breaker. A timer that is still
    /// waiting is not work here: its exact remaining duration becomes the
    /// park's only timeout instead.
    fn pending_work(&self) -> Result<Option<ChannelWork>, FlowChannelError> {
        match self.control.directive() {
            ChannelDirective::Continue => {}
            ChannelDirective::Drain => return Ok(Some(ChannelWork::DrainSource)),
            ChannelDirective::DrainEgress | ChannelDirective::Stop => {
                return Ok(Some(ChannelWork::LeaveLoop));
            }
        }
        // A released boundary is completed without waiting. A boundary whose
        // target left is not work here on purpose: its status is `Retry`, and
        // only the settlement paths act on that.
        if self.release_ready()? {
            return Ok(Some(ChannelWork::SettleReleased));
        }
        if self.commands.has_pending() {
            return Ok(Some(ChannelWork::Command));
        }
        let timer = self.lua_vm()?.next_timer_event();
        let source_ready = self
            .ingress
            .readable()
            .map_err(|source| FlowChannelError::ChannelWait { source })?;
        if source_ready {
            self.observe_source_readiness()?;
        } else {
            self.source_readiness.set(None);
        }
        let timer_due = timer.as_ref().is_some_and(|timer| {
            timer_readiness_at(timer.schedule, Instant::now()) == TimerReadiness::Due
        });
        if timer_due && source_ready {
            let source = self
                .source_readiness
                .get()
                .ok_or(FlowChannelError::InternalInvariantViolation)?;
            let timer = timer.ok_or(FlowChannelError::InternalInvariantViolation)?;
            let timer_first = timer.deadline < source.observed_at
                || (timer.deadline == source.observed_at && timer.sequence <= source.order);
            return Ok(Some(if timer_first {
                ChannelWork::TimerDue
            } else {
                ChannelWork::SourceRecord
            }));
        }
        if timer_due {
            return Ok(Some(ChannelWork::TimerDue));
        }
        if source_ready {
            return Ok(Some(ChannelWork::SourceRecord));
        }
        Ok(None)
    }

    fn observe_source_readiness(&self) -> Result<(), FlowChannelError> {
        let generation = self.vm_generation.get();
        if self
            .source_readiness
            .get()
            .is_some_and(|readiness| readiness.generation == generation)
        {
            return Ok(());
        }
        let order = self
            .next_event_order()
            .ok_or(FlowChannelError::InternalInvariantViolation)?;
        self.source_readiness.set(Some(SourceReadiness {
            observed_at: Instant::now(),
            order,
            generation,
        }));
        Ok(())
    }

    fn next_event_order(&self) -> Option<u64> {
        let next = self.next_event_order.get().checked_add(1)?;
        self.next_event_order.set(next);
        Some(next)
    }

    /// Reports the earliest Lua timer's readiness from the current instant.
    fn timer_readiness(&self) -> Result<Option<TimerReadiness>, FlowChannelError> {
        Ok(self
            .lua_vm()?
            .next_timer_event()
            .map(|timer| timer_readiness_at(timer.schedule, Instant::now())))
    }

    /// Processes at most one already committed Source record without waiting.
    ///
    /// `None` proves that the Submission Queue was empty at this read point, so
    /// the event loop can arm its doorbell knowing no accepted input is pending.
    /// A Source handoff uses the same proof only after its sole writer quiesced.
    ///
    /// # Errors
    ///
    /// Returns [`FlowChannelError`] for a terminal Queue, Lua reload, Egress,
    /// resource, or private invariant failure.
    fn process_available_source(&mut self) -> Result<Option<FlowChannelStep>, FlowChannelError> {
        let Some(record) = self.ingress.try_receive(&self.metrics)? else {
            self.source_readiness.set(None);
            return Ok(None);
        };
        let result = self.process_source_record(record);
        self.source_readiness.set(None);
        result.map(Some)
    }

    fn prepare_replacement(
        &mut self,
        request: ChannelReplacementRequest,
    ) -> Result<(), FlowChannelError> {
        assert!(
            self.candidate.is_none(),
            "The previous candidate must finish before the next request"
        );
        let (change, routes, session) = request.into_session(Arc::clone(&self.control));
        let definition = match change {
            ChannelDefinitionChange::KeepLua => PreparedDefinition::Retained,
            ChannelDefinitionChange::Replace(spec) => {
                assert!(
                    spec.matches_routes(&routes),
                    "a replacement uses one complete target registry"
                );
                let replacement_control = Arc::clone(&self.control);
                let (vm, vm_diagnostics) = match spec.load_vm(
                    self.diagnostics.clone(),
                    &self.metrics,
                    move || replacement_control.is_stopping(),
                    self.pipeline_started_at,
                    Rc::clone(&self.next_event_order),
                ) {
                    Ok(prepared) => prepared,
                    Err(source) => {
                        drop(routes);
                        return session.candidate_failed(source.kind());
                    }
                };
                PreparedDefinition::Replacement {
                    spec,
                    vm,
                    vm_diagnostics,
                }
            }
        };
        session.candidate_prepared()?;
        self.candidate = Some(PreparedChannelDefinition {
            definition,
            routes,
            session,
        });
        Ok(())
    }

    fn advance_replacement(&mut self) -> Result<(), FlowChannelError> {
        let Some(candidate) = self.candidate.take() else {
            return Ok(());
        };
        let PreparedChannelDefinition {
            definition,
            routes,
            session,
        } = candidate;
        let permit = match session.try_cutover()? {
            CandidatePreparationOutcome::Pending(session) => {
                self.candidate = Some(PreparedChannelDefinition {
                    definition,
                    routes,
                    session,
                });
                return Ok(());
            }
            CandidatePreparationOutcome::Cutover(permit) => permit,
            CandidatePreparationOutcome::Aborted(session) => {
                drop(definition);
                drop(routes);
                session.report_aborted();
                return Ok(());
            }
        };
        // The old routes stay bound until every accepted boundary they own is
        // settled, because the Sink releases exactly the frames this Channel
        // committed through them. Older `pending_ack` records therefore also
        // complete before the newer `pending_records` of this Channel return
        // `RETRY`.
        if self.settle_pending_forward(SettlementInterruption::PlannedStop)?
            == CompletionProgress::Stopped
        {
            return Ok(());
        }
        if let PreparedDefinition::Replacement {
            spec,
            vm,
            vm_diagnostics,
        } = definition
        {
            if self.spec.delivery() == SourceDelivery::AtLeastOnce
                && !self.pending_records.is_empty()
            {
                let pending = std::mem::take(&mut self.pending_records);
                if self.complete_all(pending, IngressCompletionStatus::Retry)?
                    == CompletionProgress::Stopped
                {
                    return Ok(());
                }
            }
            self.spec = spec;
            self.lua_vm = Some(vm);
            self.lua_vm_diagnostics = Some(vm_diagnostics);
            self.reset_event_observations();
        }
        self.routes = bind_routes(
            routes,
            EgressHandoff::CurrentChannel(std::mem::take(&mut self.routes)),
            &self.bell,
            &self.metrics,
        )
        .map_err(|source| FlowChannelError::EgressRelease { source })?;
        self.bind_route_generation();
        permit.complete_and_wait_for_activation()
    }

    fn finish_source_session(&mut self) -> Result<CompletionProgress, FlowChannelError> {
        if self.drain_source_records()? == CompletionProgress::Stopped {
            return Ok(CompletionProgress::Stopped);
        }
        let mut wait = self.metrics.wait(WaitKind::CompletionDrain);
        while !self.is_stopping() {
            match self.ingress.wait_completions_consumed(&mut wait)? {
                WaitOutcome::Ready => {
                    wait.ready();
                    return Ok(CompletionProgress::Completed);
                }
                WaitOutcome::Interrupted => {}
            }
        }
        Ok(CompletionProgress::Stopped)
    }

    fn drain_source_records(&mut self) -> Result<CompletionProgress, FlowChannelError> {
        self.source_readiness.set(None);
        loop {
            if self.is_stopping() {
                return Ok(CompletionProgress::Stopped);
            }
            match self.process_available_source()? {
                None => break,
                Some(FlowChannelStep::Stopped) => return Ok(CompletionProgress::Stopped),
                Some(FlowChannelStep::EventProcessed) => {}
            }
        }
        // Every record this drain processed already owns an accepted Egress
        // boundary, so its Completion is written only after the Sink released
        // that boundary. Draining Source input therefore also means settling
        // accepted output, and it must happen before the older boundaries are
        // followed by the `RETRY` of the newer records below.
        if self.settle_pending_forward(SettlementInterruption::PlannedStop)?
            == CompletionProgress::Stopped
        {
            return Ok(CompletionProgress::Stopped);
        }
        if self.spec.delivery() == SourceDelivery::AtLeastOnce && !self.pending_records.is_empty() {
            let pending = std::mem::take(&mut self.pending_records);
            return self.complete_all(pending, IngressCompletionStatus::Retry);
        }
        Ok(CompletionProgress::Completed)
    }

    /// Parks this Channel's single wait until one subscribed fact needs attention.
    ///
    /// The doorbell belongs to this loop, so every wait of the Channel parks on
    /// the same address and a ring carries no cause. The condition set is
    /// [`Self::pending_work`] itself, so it covers exactly what the loop
    /// dispatches on: this Channel's own directive, the settlement of the oldest
    /// accepted boundary, a queued command, the Lua timer, and accepted Source
    /// input.
    /// A process-local command or retired target reaches the same park through
    /// the doorbell's local interruption, which is why no further condition names
    /// them here. The directive cannot rely on that interruption: the loop reads
    /// it at the head, and the candidate cutover between that read and this park
    /// consumes the very interruption the directive change published, so a park
    /// that skipped the re-read would outlive its own Stop.
    ///
    /// The arm-and-recheck step re-reads this same set, so a fact that becomes
    /// actionable between the loop head and here makes the park return at once
    /// instead of sleeping; only a fact missing from [`Self::pending_work`] could
    /// be lost, and the `match` in [`Self::run_events`] makes adding one a compile
    /// error rather than a silent omission.
    ///
    /// The Lua timer's exact remaining duration is this park's only timeout. It
    /// is a business deadline, never a poll interval, and it decides only when
    /// that timer runs.
    fn park_until_work(&self) -> Result<(), FlowChannelError> {
        let mut wait = self.metrics.wait(WaitKind::Idle);
        let condition = || self.pending_work().map(|work| work.is_some());
        let timer = self.timer_readiness()?;
        wait.blocked();
        let spurious_before = self.bell.spurious_wakes();
        let outcome = match timer {
            // The deadline passed between the loop head and this point, so the
            // next dispatch runs the timer instead of parking for zero duration.
            Some(TimerReadiness::Due) => Ok(()),
            Some(TimerReadiness::Waiting(remaining)) => {
                match self.bell.wait_until_for(remaining, condition) {
                    Ok(TimedWaitOutcome::Ready | TimedWaitOutcome::TimedOut) => {
                        wait.ready();
                        Ok(())
                    }
                    Ok(TimedWaitOutcome::Interrupted) => Ok(()),
                    Err(source) => Err(source),
                }
            }
            None => match self.bell.wait_until(condition) {
                Ok(WaitOutcome::Ready) => {
                    wait.ready();
                    Ok(())
                }
                Ok(WaitOutcome::Interrupted) => Ok(()),
                Err(source) => Err(source),
            },
        };
        self.metrics
            .spurious_wakes(self.bell.spurious_wakes() - spurious_before);
        outcome
    }

    fn process_source_record(
        &mut self,
        record: IngressRecord,
    ) -> Result<FlowChannelStep, FlowChannelError> {
        let record_id = record.record_id;
        if self.spec.delivery() == SourceDelivery::AtMostOnce
            && matches!(
                self.complete(record_id, IngressCompletionStatus::Ok)?,
                CompletionProgress::Stopped
            )
        {
            return Ok(FlowChannelStep::Stopped);
        }

        if self.spec.delivery() == SourceDelivery::AtLeastOnce {
            self.pending_records
                .try_reserve(1)
                .map_err(|_| FlowChannelError::ResourceLimitExceeded)?;
            self.pending_records.push(record_id);
        }

        let timestamp = self.timestamp_millis()?;
        let outcome = match self.lua_vm_mut()?.call_source(timestamp, record.payload) {
            Ok(outcome) => outcome,
            Err(error) => {
                let code = error.metric_type();
                self.report_error(SOURCE_DECODE_PHASE, code, || error.diagnostic_detail());
                if self.spec.delivery() == SourceDelivery::AtLeastOnce {
                    let has_earlier_pending = self.pending_records.len() > 1;
                    if let CompletionProgress::Stopped = self.fail_pending_current()? {
                        return Ok(FlowChannelStep::Stopped);
                    }
                    if has_earlier_pending && self.reload_lua_vm()? == LuaReloadProgress::Stopped {
                        return Ok(FlowChannelStep::Stopped);
                    }
                }
                return Ok(FlowChannelStep::EventProcessed);
            }
        };
        self.finish_lua_outcome(outcome, LuaFailureCompletion::Source)
    }

    fn finish_lua_outcome(
        &mut self,
        outcome: LuaMainOutcome,
        failure_completion: LuaFailureCompletion,
    ) -> Result<FlowChannelStep, FlowChannelError> {
        let (result, boundaries) = outcome.into_parts();
        if let Err(error) = &result
            && error.kind() != LuaVmErrorKind::ExecutionStopped
        {
            let code = error.kind().tenon_document_issue_code();
            self.report_error(LUA_MAIN_PHASE, code, || error.diagnostic_detail());
        }
        self.metrics.emits(&boundaries);
        if self.is_stopping()
            && result
                .as_ref()
                .is_err_and(|error| error.kind() == LuaVmErrorKind::ExecutionStopped)
        {
            return Ok(FlowChannelStep::Stopped);
        }
        let mut claimed_records =
            if self.spec.delivery() == SourceDelivery::AtLeastOnce && !boundaries.is_empty() {
                std::mem::take(&mut self.pending_records)
            } else {
                Vec::new()
            };

        for boundary in boundaries {
            // Only the first boundary of this call owns the pending Source
            // records; every later boundary takes the now empty batch.
            let records = std::mem::take(&mut claimed_records);
            if self.deliver_boundary(boundary, records)? == FlowChannelStep::Stopped {
                return Ok(FlowChannelStep::Stopped);
            }
        }

        if result.is_err() {
            if self.spec.delivery() == SourceDelivery::AtLeastOnce
                && !self.pending_records.is_empty()
            {
                let completion = match failure_completion {
                    LuaFailureCompletion::Source => self.fail_pending_current()?,
                    LuaFailureCompletion::Timer => {
                        let pending = std::mem::take(&mut self.pending_records);
                        self.complete_all(pending, IngressCompletionStatus::Retry)?
                    }
                };
                if completion == CompletionProgress::Stopped {
                    return Ok(FlowChannelStep::Stopped);
                }
            }
            if self.reload_lua_vm()? == LuaReloadProgress::Stopped {
                return Ok(FlowChannelStep::Stopped);
            }
        }
        Ok(FlowChannelStep::EventProcessed)
    }

    /// Commits one Lua emit boundary and records the Source records it owns.
    ///
    /// A payload boundary that committed to every target is accepted without
    /// waiting: its records complete later, once [`Self::drain_released`] or
    /// [`Self::settle_pending_forward`] observes the Sink release. A
    /// completion-only boundary produces no EgressRecord, so it completes its
    /// records here.
    #[allow(
        clippy::expect_used,
        reason = "Lua and routes use the same verified Contract registry"
    )]
    fn deliver_boundary(
        &mut self,
        boundary: EmitBoundary,
        record_ids: Vec<u64>,
    ) -> Result<FlowChannelStep, FlowChannelError> {
        let EmitBoundary::Payload {
            sink_contract_id,
            payload,
        } = boundary
        else {
            if self.complete_all(record_ids, IngressCompletionStatus::Ok)?
                == CompletionProgress::Stopped
            {
                return Ok(FlowChannelStep::Stopped);
            }
            return Ok(FlowChannelStep::EventProcessed);
        };
        let route = self
            .routes
            .get_mut(&sink_contract_id)
            .expect("Lua emits only registered Sink Contracts from this Channel definition");
        let sent = route
            .send(payload, &self.metrics, || self.control.is_stopping())
            .map_err(|source| FlowChannelError::EgressRoute {
                sink_contract_id: sink_contract_id.clone(),
                source,
            })?;
        let SendOutcome::Committed(targets) = sent else {
            return Ok(FlowChannelStep::Stopped);
        };
        self.pending_ack
            .push(PendingRelease::new(sink_contract_id, targets, record_ids));
        Ok(FlowChannelStep::EventProcessed)
    }

    /// Completes every accepted boundary the Sink already released, oldest first.
    ///
    /// Settlement stops at the first unreleased boundary, so Source Completions
    /// keep emit order. The check is one shared-memory read per boundary and never
    /// waits, which is what lets the Channel keep accepting input while Sink
    /// releases lag behind.
    fn drain_released(&mut self) -> Result<CompletionProgress, FlowChannelError> {
        while self.release_ready()? {
            let pending = self.pending_ack.remove(0);
            if self.complete_all(pending.into_record_ids(), IngressCompletionStatus::Ok)?
                == CompletionProgress::Stopped
            {
                return Ok(CompletionProgress::Stopped);
            }
        }
        Ok(CompletionProgress::Completed)
    }

    /// Reports whether the Sink already released the oldest accepted boundary.
    ///
    /// This is the one evaluation both the loop head's dispatch and
    /// [`Self::drain_released`] use, so "can complete now" has a single answer.
    fn release_ready(&self) -> Result<bool, FlowChannelError> {
        let Some(pending) = self.pending_ack.first() else {
            return Ok(false);
        };
        is_released(&self.routes, pending)
            .map_err(EgressError::from)
            .map_err(|source| FlowChannelError::EgressRelease { source })
    }

    /// Waits for every accepted boundary, oldest first, and stops when planned stop ends it.
    ///
    /// This is where a Channel still waits for Sink releases: it may not drop a
    /// route, hand off its writers, or exit while a Sink owes a release for output
    /// this Channel committed.
    fn settle_pending_forward(
        &mut self,
        interruption: SettlementInterruption,
    ) -> Result<CompletionProgress, FlowChannelError> {
        while !self.pending_ack.is_empty() {
            if interruption.requested(&self.control) {
                return Ok(CompletionProgress::Stopped);
            }
            let Some(status) = self.settlement_status()? else {
                self.park_for_settlement()?;
                continue;
            };
            let pending = self.pending_ack.remove(0);
            if self.complete_all(pending.into_record_ids(), status)? == CompletionProgress::Stopped
            {
                return Ok(CompletionProgress::Stopped);
            }
        }
        Ok(CompletionProgress::Completed)
    }

    /// Reports how the oldest accepted boundary can now settle.
    ///
    /// `None` means neither subscribed fact holds yet: no Sink released the
    /// boundary, and no target left the Pipeline.
    fn settlement_status(&self) -> Result<Option<IngressCompletionStatus>, FlowChannelError> {
        let pending = &self.pending_ack[0];
        // A released boundary delivered its records. A boundary whose target
        // left the Pipeline never will, so those records must redeliver.
        if is_released(&self.routes, pending)
            .map_err(EgressError::from)
            .map_err(|source| FlowChannelError::EgressRelease { source })?
        {
            return Ok(Some(IngressCompletionStatus::Ok));
        }
        Ok(self
            .target_departed(pending)
            .then_some(IngressCompletionStatus::Retry))
    }

    /// Reports whether any target of one accepted boundary left the Pipeline.
    fn target_departed(&self, pending: &PendingRelease) -> bool {
        let departed = self
            .egress_waits
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        target_departed(pending, |instance| departed.has_departed(instance))
    }

    /// Parks the Channel's doorbell until the oldest accepted boundary settles.
    ///
    /// The wait rechecks [`Self::settlement_status`] itself, so the facts it
    /// subscribes to are the facts that decide settlement, and a ring is never a
    /// cause but always an invitation to re-read the shared release position. A
    /// planned stop reaches this park through the doorbell's local interruption,
    /// exactly like the event loop, which is why no third condition names it here.
    fn park_for_settlement(&self) -> Result<(), FlowChannelError> {
        let pending = &self.pending_ack[0];
        let mut wait = self.metrics.wait(WaitKind::SinkRelease(settlement_metrics(
            &self.routes,
            pending,
        )));
        // The Sink may release after the caller's probe. The wait must accept
        // that progress and return Ready without entering the platform wait.
        wait.blocked();
        let spurious_before = self.bell.spurious_wakes();
        match self.bell.wait_until(|| -> Result<bool, FlowChannelError> {
            Ok(self.settlement_status()?.is_some())
        })? {
            WaitOutcome::Ready => wait.ready(),
            WaitOutcome::Interrupted => {}
        }
        self.metrics
            .spurious_wakes(self.bell.spurious_wakes() - spurious_before);
        Ok(())
    }

    /// Waits for the Sink to release every accepted boundary before this thread exits.
    ///
    /// A normal egress drain still waits; only a force stop cancels the wait.
    fn drain_egress(&mut self) -> Result<(), FlowChannelError> {
        self.settle_pending_forward(SettlementInterruption::ForceStopOnly)
            .map(drop)
    }

    /// Binds the metrics of the routes this Channel now owns and starts their
    /// departure generation.
    fn bind_route_generation(&mut self) {
        // Departures describe the route generation that observed them, so this
        // call discards them. That is only sound because a Channel settles every
        // boundary of its current generation before it binds the next one, and a
        // Channel that cannot settle returns before reaching here.
        debug_assert!(self.pending_ack.is_empty());
        let ingress = self
            .ingress
            .observers()
            .into_iter()
            .filter_map(|(kind, queue)| self.metrics.queue(kind, queue));
        let egress = self
            .routes
            .values()
            .flat_map(super::egress::EgressRoute::observations)
            .filter_map(|(target, queue)| self.metrics.egress_queue(target, queue));
        let queues: Vec<_> = ingress.chain(egress).collect();
        self.metrics.queues(queues.into_iter());

        self.egress_waits
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .start_generation();
    }

    fn fail_pending_current(&mut self) -> Result<CompletionProgress, FlowChannelError> {
        let current = self
            .pending_records
            .pop()
            .ok_or(FlowChannelError::InternalInvariantViolation)?;
        let earlier = std::mem::take(&mut self.pending_records);
        if let CompletionProgress::Stopped =
            self.complete_all(earlier, IngressCompletionStatus::Retry)?
        {
            return Ok(CompletionProgress::Stopped);
        }
        self.complete(current, IngressCompletionStatus::Error)
    }

    fn complete_all(
        &mut self,
        record_ids: Vec<u64>,
        status: IngressCompletionStatus,
    ) -> Result<CompletionProgress, FlowChannelError> {
        for record_id in record_ids {
            if let CompletionProgress::Stopped = self.complete(record_id, status)? {
                return Ok(CompletionProgress::Stopped);
            }
        }
        Ok(CompletionProgress::Completed)
    }

    fn complete(
        &mut self,
        record_id: u64,
        status: IngressCompletionStatus,
    ) -> Result<CompletionProgress, FlowChannelError> {
        let mut wait = self.metrics.wait(WaitKind::CompletionCapacity);
        loop {
            match self
                .ingress
                .complete(record_id, status, &self.metrics, &mut wait)?
            {
                IngressCompletionWriteOutcome::Committed => {
                    return Ok(CompletionProgress::Completed);
                }
                IngressCompletionWriteOutcome::Interrupted if self.is_stopping() => {
                    return Ok(CompletionProgress::Stopped);
                }
                IngressCompletionWriteOutcome::Interrupted => {}
            }
        }
    }

    fn reload_lua_vm(&mut self) -> Result<LuaReloadProgress, FlowChannelError> {
        self.reset_event_observations();
        drop(self.lua_vm.take());
        drop(self.lua_vm_diagnostics.take());
        let reload_control = Arc::clone(&self.control);
        let (lua_vm, lua_vm_diagnostics) = match self.spec.load_vm(
            self.diagnostics.clone(),
            &self.metrics,
            move || reload_control.is_stopping(),
            self.pipeline_started_at,
            Rc::clone(&self.next_event_order),
        ) {
            Ok(prepared) => prepared,
            Err(source)
                if self.is_stopping() && source.kind() == LuaVmErrorKind::ExecutionStopped =>
            {
                return Ok(LuaReloadProgress::Stopped);
            }
            Err(source) => {
                return Err(FlowChannelError::LuaVmLoad {
                    kind: source.kind(),
                });
            }
        };
        self.lua_vm = Some(lua_vm);
        self.lua_vm_diagnostics = Some(lua_vm_diagnostics);
        Ok(LuaReloadProgress::Reloaded)
    }

    fn reset_event_observations(&self) {
        self.source_readiness.set(None);
        self.vm_generation
            .set(self.vm_generation.get().wrapping_add(1));
    }

    fn lua_vm_mut(&mut self) -> Result<&mut LuaVm, FlowChannelError> {
        self.lua_vm
            .as_mut()
            .ok_or(FlowChannelError::InternalInvariantViolation)
    }

    fn lua_vm(&self) -> Result<&LuaVm, FlowChannelError> {
        self.lua_vm
            .as_ref()
            .ok_or(FlowChannelError::InternalInvariantViolation)
    }

    fn is_stopping(&self) -> bool {
        self.control.is_stopping()
    }

    fn process_timer(&mut self) -> Result<FlowChannelStep, FlowChannelError> {
        match self.control.directive() {
            ChannelDirective::Continue => {}
            // The event loop re-reads the directive at the top of its next turn,
            // so a Drain observed here only defers this timer.
            ChannelDirective::Drain => return Ok(FlowChannelStep::EventProcessed),
            ChannelDirective::DrainEgress | ChannelDirective::Stop => {
                return Ok(FlowChannelStep::Stopped);
            }
        }
        let timer = self
            .lua_vm_mut()?
            .begin_timer()
            .map_err(|_| FlowChannelError::InternalInvariantViolation)?;
        let timestamp = self.timestamp_millis()?;
        let outcome = self.lua_vm_mut()?.call_timer_event(timestamp, timer);
        self.finish_lua_outcome(outcome, LuaFailureCompletion::Timer)
    }

    fn timestamp_millis(&self) -> Result<i64, FlowChannelError> {
        i64::try_from(self.pipeline_started_at.elapsed().as_millis())
            .map_err(|_| FlowChannelError::TimestampOverflow)
    }

    /// Records one Channel failure on metrics and, while a subscriber is
    /// present, publishes the matching best-effort error diagnostic.
    ///
    /// Diagnostics never block or change completion, Egress, or VM recovery;
    /// the failure detail is rendered only when at least one subscriber
    /// selected this Channel.
    fn report_error<'a>(
        &self,
        phase: &'static str,
        code: &'static str,
        detail: impl FnOnce() -> Cow<'a, str>,
    ) {
        self.metrics.error(phase, code);
        if let Some(diagnostics) = self.lua_vm_diagnostics.as_ref() {
            diagnostics.publish_error(phase, code, detail);
        }
    }
}

impl fmt::Debug for FlowChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FlowChannel")
            .field("spec", &self.spec)
            .field("route_count", &self.routes.len())
            .field("pending_record_count", &self.pending_records.len())
            .field("directive", &self.control.directive())
            .finish_non_exhaustive()
    }
}

/// A separate VM and its target bindings, owned by the same Channel thread.
struct PreparedChannelDefinition {
    definition: PreparedDefinition,
    routes: PreparedEgressRoutes,
    session: ChannelReplacementSession,
}

#[allow(
    clippy::large_enum_variant,
    reason = "one temporary candidate per Channel avoids a second heap allocation"
)]
enum PreparedDefinition {
    Retained,
    Replacement {
        spec: FlowChannelSpec,
        vm: LuaVm,
        vm_diagnostics: LuaDiagnosticPublisher,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SettlementInterruption {
    /// Any planned stop ends the blocked wait.
    PlannedStop,
    /// Only a force stop ends the blocked wait; a normal stop still drains output.
    ForceStopOnly,
}

impl SettlementInterruption {
    fn requested(self, control: &FlowChannelControl) -> bool {
        match self {
            Self::PlannedStop => control.is_stopping(),
            Self::ForceStopOnly => control.is_force_stopping(),
        }
    }
}

/// Whether the requested Completion writes finished before planned stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionProgress {
    Completed,
    Stopped,
}

fn timer_readiness_at(schedule: TimerSchedule, now: Instant) -> TimerReadiness {
    let elapsed = now.saturating_duration_since(schedule.scheduled_at);
    let remaining = schedule.delay.saturating_sub(elapsed);
    if remaining.is_zero() {
        TimerReadiness::Due
    } else {
        TimerReadiness::Waiting(remaining)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimerReadiness {
    Due,
    Waiting(Duration),
}

#[derive(Debug, Clone, Copy)]
enum LuaFailureCompletion {
    Source,
    Timer,
}

#[cfg(test)]
mod tests;
