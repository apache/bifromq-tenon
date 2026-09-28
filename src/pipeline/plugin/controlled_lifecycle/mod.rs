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

//! Integrates one Plugin Control session with its sole child-process owner.
//!
//! One state machine owns the pending launch, attached stream, stdin
//! lifetime, child, retry progress, and staged shutdown. The collection drives
//! Source quiesce before data-plane drain and final Shutdown afterwards.

use std::fmt;
use std::task::{Context, Poll};
use std::time::Duration;

use super::lifecycle::{
    PluginLifecycleError, PluginProcess, PluginRetryBackoff, PluginSpawnOutcome,
    PluginStartFailure, PluginStateEvent, PluginStatusState, StartFailureCleanup, spawn_plugin,
};
use crate::contracts::core::ControlError;
use crate::pipeline::diagnostics::PluginDiagnosticPublisher;

mod active;
mod retry;
mod startup;

use active::{
    ControlledPluginShutdown, QuiesceFailure, QuiescedControlledPlugin, QuiescingControlledPlugin,
    ReadyControlledPlugin, ReadyShutdownFailure, ReadySourceQuiesce,
};
use retry::{ControlledRestartBackoff, ControlledRuntimeFailure, ControlledRuntimeFailureCleanup};
pub(in crate::pipeline) use startup::ControlledPluginLaunch;
use startup::{ControlStartup, ControlledPluginStartup, ControlledStartupOutcome};

/// A snapshot used once to decide whether Cutover can preserve this session.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::pipeline) enum HandoffReadiness {
    /// The old session is Ready and owes the ordered handoff.
    Ready,
    /// The old child is still starting. It is alive and owes the same ordered
    /// handoff once it reaches Ready, so it must never be retired as gone.
    NotReady,
    /// No owner holds this identity any more.
    Unavailable,
    /// The old session already failed and must be reported before any handoff.
    FailureObserved,
}

pub(super) struct PluginInstanceOwner {
    state: ControlledPluginState,
    retry_backoff: PluginRetryBackoff,
    next_retry_delay: Duration,
}

impl PluginInstanceOwner {
    pub(super) fn launch_controlled(
        launch: ControlledPluginLaunch<'_>,
        retry_backoff: PluginRetryBackoff,
        diagnostics: PluginDiagnosticPublisher,
    ) -> Self {
        Self {
            state: Self::launch_state(launch, diagnostics, || {}),
            retry_backoff,
            next_retry_delay: retry_backoff.initial_delay,
        }
    }

    pub(super) fn restart_controlled(
        &mut self,
        launch: ControlledPluginLaunch<'_>,
        diagnostics: PluginDiagnosticPublisher,
        on_created: impl FnOnce(),
    ) -> Result<(), PluginLifecycleError> {
        if !matches!(self.state, ControlledPluginState::RestartBackoff(_)) {
            return Err(PluginLifecycleError::InvalidTransition);
        }
        self.state = Self::launch_state(launch, diagnostics, on_created);
        Ok(())
    }

    pub(super) fn poll_event(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<PluginStateEvent, PluginLifecycleError>> {
        loop {
            match &mut self.state {
                ControlledPluginState::Starting(starting) => match starting.poll(context) {
                    Poll::Ready(Ok(ControlledStartupOutcome::Ready(ready))) => {
                        self.state = ControlledPluginState::Ready(ready);
                        return Poll::Ready(Ok(PluginStateEvent::StatusChanged));
                    }
                    Poll::Ready(Ok(ControlledStartupOutcome::Failed(failure))) => {
                        self.state = ControlledPluginState::StartFailed(failure);
                        return Poll::Ready(Ok(PluginStateEvent::ProcessFailed));
                    }
                    Poll::Ready(Ok(ControlledStartupOutcome::Cleanup(cleanup))) => {
                        self.state = ControlledPluginState::CleaningStartFailure(cleanup);
                    }
                    Poll::Ready(Err(source)) => return Poll::Ready(Err(source)),
                    Poll::Pending => return Poll::Pending,
                },
                ControlledPluginState::CleaningStartFailure(cleanup) => {
                    match cleanup.poll(context) {
                        Poll::Ready(Ok(failure)) => {
                            self.state = ControlledPluginState::StartFailed(failure);
                            return Poll::Ready(Ok(PluginStateEvent::ProcessFailed));
                        }
                        Poll::Ready(Err(source)) => return Poll::Ready(Err(source)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                ControlledPluginState::Ready(ready) => match ready.poll(context) {
                    Poll::Ready(Ok(failure)) => {
                        self.begin_runtime_backoff_cleanup(failure);
                    }
                    Poll::Ready(Err(source)) => return Poll::Ready(Err(source)),
                    Poll::Pending => return Poll::Pending,
                },
                ControlledPluginState::CleaningRuntimeFailure(cleanup) => {
                    match cleanup.poll(context) {
                        Poll::Ready(Ok(failure)) => {
                            let delay = self.next_retry_delay;
                            self.next_retry_delay = self.retry_backoff.next_delay(delay);
                            self.state = ControlledPluginState::RestartBackoff(
                                ControlledRestartBackoff::new(failure, delay),
                            );
                            return Poll::Ready(Ok(PluginStateEvent::ProcessFailed));
                        }
                        Poll::Ready(Err(source)) => return Poll::Ready(Err(source)),
                        Poll::Pending => return Poll::Pending,
                    }
                }
                ControlledPluginState::RestartBackoff(backoff) => {
                    if backoff.poll_due(context) {
                        return Poll::Ready(Ok(PluginStateEvent::RestartDue));
                    }
                    return Poll::Pending;
                }
                ControlledPluginState::StartFailed(_)
                | ControlledPluginState::Quiescing(_)
                | ControlledPluginState::Quiesced(_)
                | ControlledPluginState::ShuttingDown(_)
                | ControlledPluginState::ForceStopping(_)
                | ControlledPluginState::Stopped => return Poll::Pending,
            }
        }
    }

    pub(super) fn status(&self) -> (PluginStatusState, Option<ControlError>) {
        match &self.state {
            ControlledPluginState::Starting(_) | ControlledPluginState::CleaningStartFailure(_) => {
                (PluginStatusState::Starting, None)
            }
            ControlledPluginState::Ready(_)
            | ControlledPluginState::CleaningRuntimeFailure(_)
            | ControlledPluginState::Quiescing(_)
            | ControlledPluginState::Quiesced(_)
            | ControlledPluginState::ShuttingDown(_)
            | ControlledPluginState::ForceStopping(_) => (PluginStatusState::Running, None),
            ControlledPluginState::StartFailed(failure) => (
                PluginStatusState::StartFailed,
                Some(failure.control_error()),
            ),
            ControlledPluginState::RestartBackoff(backoff) => (
                PluginStatusState::RestartBackoff,
                Some(backoff.control_error()),
            ),
            ControlledPluginState::Stopped => {
                unreachable!("a reaped Plugin Instance must be removed before status publication")
            }
        }
    }

    /// Freezes whether this old session owes a healthy handoff at Cutover entry.
    pub(super) fn handoff_readiness(
        &mut self,
        context: &mut Context<'_>,
    ) -> Result<HandoffReadiness, PluginLifecycleError> {
        if matches!(
            self.state,
            ControlledPluginState::Starting(_) | ControlledPluginState::Ready(_)
        ) && let Poll::Ready(result) = self.poll_event(context)
            && matches!(result?, PluginStateEvent::ProcessFailed)
        {
            return Ok(HandoffReadiness::FailureObserved);
        }
        let readiness = match self.state {
            ControlledPluginState::Ready(_) => HandoffReadiness::Ready,
            ControlledPluginState::Starting(_) => HandoffReadiness::NotReady,
            ControlledPluginState::CleaningStartFailure(_)
            | ControlledPluginState::CleaningRuntimeFailure(_) => HandoffReadiness::FailureObserved,
            _ => HandoffReadiness::Unavailable,
        };
        Ok(readiness)
    }

    /// Keeps an old healthy session alive through Quiesced, without runtime retry.
    pub(super) fn poll_handoff(
        &mut self,
        context: &mut Context<'_>,
    ) -> Result<(), PluginLifecycleError> {
        match &self.state {
            // This Pipeline already force stopped the pending launch, so it can
            // only become Stopped; the quiesce wait observes that.
            ControlledPluginState::Starting(starting) if starting.stop_requested() => {
                return match self.poll_source_quiesced(context) {
                    Poll::Ready(Ok(())) => Ok(()),
                    Poll::Ready(Err(source)) => Err(source),
                    Poll::Pending => Ok(()),
                };
            }
            ControlledPluginState::Starting(_) => return self.poll_handoff_startup(context),
            ControlledPluginState::StartFailed(_) | ControlledPluginState::Stopped => return Ok(()),
            _ => {}
        }
        let failure = match &mut self.state {
            ControlledPluginState::Ready(ready) => ready.poll(context),
            ControlledPluginState::Quiesced(quiesced) => quiesced.poll(context),
            ControlledPluginState::Quiescing(_) => {
                return match self.poll_source_quiesced(context) {
                    Poll::Ready(Ok(())) => self.poll_handoff(context),
                    Poll::Ready(Err(source)) => Err(source),
                    Poll::Pending => Ok(()),
                };
            }
            _ => unreachable!("Handoff observation selects only frozen healthy old sessions"),
        };
        match failure {
            Poll::Ready(Ok(ControlledRuntimeFailure::ProcessExited(status))) => {
                Err(PluginLifecycleError::ExitedDuringShutdown(status))
            }
            Poll::Ready(Ok(ControlledRuntimeFailure::Control(source))) => {
                Err(PluginLifecycleError::Control(source))
            }
            Poll::Ready(Err(source)) => Err(source),
            Poll::Pending => Ok(()),
        }
    }

    /// Advances one not-yet-ready old session until it is a normal handoff peer.
    fn poll_handoff_startup(
        &mut self,
        context: &mut Context<'_>,
    ) -> Result<(), PluginLifecycleError> {
        match self.poll_event(context) {
            Poll::Ready(Ok(PluginStateEvent::StatusChanged)) => self.poll_handoff(context),
            Poll::Ready(Ok(PluginStateEvent::ProcessFailed)) => {
                let ControlledPluginState::StartFailed(failure) = &self.state else {
                    unreachable!("a failed pending launch becomes StartFailed")
                };
                Err(PluginLifecycleError::StartFailed(failure.control_error()))
            }
            Poll::Ready(Ok(PluginStateEvent::RestartDue)) => {
                unreachable!("a pending launch has no retry sequence to expire")
            }
            Poll::Ready(Err(source)) => Err(source),
            Poll::Pending => Ok(()),
        }
    }

    pub(super) fn begin_source_quiesce(&mut self) -> Result<(), PluginLifecycleError> {
        let state = std::mem::replace(&mut self.state, ControlledPluginState::Stopped);
        let (state, result) = match state {
            ControlledPluginState::Starting(mut starting) => {
                let result = starting.request_force_stop();
                (ControlledPluginState::Starting(starting), result)
            }
            ControlledPluginState::CleaningStartFailure(mut cleanup) => {
                let result = cleanup
                    .request_force_stop()
                    .map_err(PluginLifecycleError::Stop);
                (ControlledPluginState::CleaningStartFailure(cleanup), result)
            }
            ControlledPluginState::Ready(ready) => match ready.begin_source_quiesce() {
                Ok(ReadySourceQuiesce::SinkOnly(ready)) => {
                    (ControlledPluginState::Ready(ready), Ok(()))
                }
                Ok(ReadySourceQuiesce::Quiescing(quiescing)) => {
                    (ControlledPluginState::Quiescing(quiescing), Ok(()))
                }
                Err((mut process, source)) => {
                    let stop = process
                        .request_force_stop()
                        .map_err(PluginLifecycleError::Stop);
                    (
                        ControlledPluginState::ForceStopping(*process),
                        stop.and(Err(PluginLifecycleError::Control(source))),
                    )
                }
            },
            ControlledPluginState::CleaningRuntimeFailure(mut cleanup) => {
                let result = cleanup.request_force_stop();
                (
                    ControlledPluginState::CleaningRuntimeFailure(cleanup),
                    result,
                )
            }
            state @ (ControlledPluginState::StartFailed(_)
            | ControlledPluginState::RestartBackoff(_)
            | ControlledPluginState::Stopped) => (state, Ok(())),
            state @ (ControlledPluginState::Quiescing(_)
            | ControlledPluginState::Quiesced(_)
            | ControlledPluginState::ShuttingDown(_)
            | ControlledPluginState::ForceStopping(_)) => {
                (state, Err(PluginLifecycleError::InvalidTransition))
            }
        };
        self.state = state;
        result
    }

    pub(super) fn poll_source_quiesced(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), PluginLifecycleError>> {
        match &mut self.state {
            ControlledPluginState::Starting(starting) => {
                let result = starting.poll_stopped(context);
                if matches!(result, Poll::Ready(Ok(()))) {
                    self.state = ControlledPluginState::Stopped;
                }
                result
            }
            ControlledPluginState::CleaningStartFailure(cleanup) => match cleanup.poll(context) {
                Poll::Ready(Ok(_)) => {
                    self.state = ControlledPluginState::Stopped;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(source)) => Poll::Ready(Err(source)),
                Poll::Pending => Poll::Pending,
            },
            ControlledPluginState::CleaningRuntimeFailure(cleanup) => match cleanup.poll(context) {
                Poll::Ready(Ok(_)) => {
                    self.state = ControlledPluginState::Stopped;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(source)) => Poll::Ready(Err(source)),
                Poll::Pending => Poll::Pending,
            },
            ControlledPluginState::Ready(ready) if ready.is_sink_only() => {
                Poll::Ready(self.poll_handoff(context))
            }
            ControlledPluginState::Quiescing(quiescing) => match quiescing.poll(context) {
                Poll::Ready(Ok(quiesced)) => {
                    self.state = ControlledPluginState::Quiesced(quiesced);
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(QuiesceFailure::ProcessExited(status))) => {
                    self.state = ControlledPluginState::Stopped;
                    Poll::Ready(Err(PluginLifecycleError::ExitedDuringShutdown(status)))
                }
                Poll::Ready(Err(QuiesceFailure::Control {
                    mut process,
                    source,
                })) => {
                    let stop = process
                        .request_force_stop()
                        .map_err(PluginLifecycleError::Stop);
                    self.state = ControlledPluginState::ForceStopping(process);
                    Poll::Ready(stop.and(Err(PluginLifecycleError::Control(source))))
                }
                Poll::Ready(Err(QuiesceFailure::Status(source))) => {
                    Poll::Ready(Err(PluginLifecycleError::Status(source)))
                }
                Poll::Ready(Err(QuiesceFailure::InvalidTransition)) => {
                    Poll::Ready(Err(PluginLifecycleError::InvalidTransition))
                }
                Poll::Pending => Poll::Pending,
            },
            ControlledPluginState::Quiesced(_) => Poll::Ready(self.poll_handoff(context)),
            ControlledPluginState::StartFailed(_)
            | ControlledPluginState::RestartBackoff(_)
            | ControlledPluginState::Stopped => Poll::Ready(Ok(())),
            ControlledPluginState::Ready(_)
            | ControlledPluginState::ShuttingDown(_)
            | ControlledPluginState::ForceStopping(_) => {
                Poll::Ready(Err(PluginLifecycleError::InvalidTransition))
            }
        }
    }

    /// Reports whether this old child still owes the ordered handoff because it
    /// has not finished starting.
    ///
    /// A pending launch may already hold Egress output a Channel committed to
    /// it, and only that child can ever release it. The handoff therefore drives
    /// the launch to Ready before it asks for Quiesce; quiescing a pending
    /// launch force stops the very peer that still owes those releases. Readiness
    /// says whether the peer can hand off yet, never whether it owes anything.
    pub(super) fn has_pending_handoff_launch(&self) -> bool {
        matches!(&self.state, ControlledPluginState::Starting(_))
    }

    pub(super) fn is_source_quiesced(&self) -> bool {
        match &self.state {
            ControlledPluginState::Ready(ready) => ready.is_sink_only(),
            ControlledPluginState::Quiesced(_)
            | ControlledPluginState::StartFailed(_)
            | ControlledPluginState::RestartBackoff(_)
            | ControlledPluginState::Stopped => true,
            _ => false,
        }
    }

    pub(super) fn request_planned_stop(&mut self) -> Result<(), PluginLifecycleError> {
        let state = std::mem::replace(&mut self.state, ControlledPluginState::Stopped);
        let (state, result) = match state {
            ControlledPluginState::Starting(mut starting) => {
                let result = starting.request_force_stop();
                (ControlledPluginState::Starting(starting), result)
            }
            ControlledPluginState::CleaningStartFailure(mut cleanup) => {
                let result = cleanup
                    .request_force_stop()
                    .map_err(PluginLifecycleError::Stop);
                (ControlledPluginState::CleaningStartFailure(cleanup), result)
            }
            ControlledPluginState::Ready(ready) => match ready.shutdown_sink() {
                Ok(shutdown) => (ControlledPluginState::ShuttingDown(shutdown), Ok(())),
                Err(ReadyShutdownFailure::SourceCapable(ready)) => (
                    ControlledPluginState::Ready(ready),
                    Err(PluginLifecycleError::InvalidTransition),
                ),
                Err(ReadyShutdownFailure::Control {
                    mut process,
                    source,
                }) => {
                    let stop = process
                        .request_force_stop()
                        .map_err(PluginLifecycleError::Stop);
                    (
                        ControlledPluginState::ForceStopping(*process),
                        stop.and(Err(PluginLifecycleError::Control(source))),
                    )
                }
            },
            ControlledPluginState::Quiesced(quiesced) => match quiesced.shutdown() {
                Ok(shutdown) => (ControlledPluginState::ShuttingDown(shutdown), Ok(())),
                Err((mut process, source)) => {
                    let stop = process
                        .request_force_stop()
                        .map_err(PluginLifecycleError::Stop);
                    (
                        ControlledPluginState::ForceStopping(*process),
                        stop.and(Err(PluginLifecycleError::Control(source))),
                    )
                }
            },
            ControlledPluginState::CleaningRuntimeFailure(mut cleanup) => {
                let result = cleanup.request_force_stop();
                (
                    ControlledPluginState::CleaningRuntimeFailure(cleanup),
                    result,
                )
            }
            ControlledPluginState::ForceStopping(mut process) => {
                let result = process
                    .request_force_stop()
                    .map_err(PluginLifecycleError::Stop);
                (ControlledPluginState::ForceStopping(process), result)
            }
            state @ (ControlledPluginState::StartFailed(_)
            | ControlledPluginState::RestartBackoff(_)
            | ControlledPluginState::ShuttingDown(_)
            | ControlledPluginState::Stopped) => (state, Ok(())),
            state @ ControlledPluginState::Quiescing(_) => {
                (state, Err(PluginLifecycleError::InvalidTransition))
            }
        };
        self.state = state;
        result
    }

    pub(super) fn request_force_stop(&mut self) -> Result<(), PluginLifecycleError> {
        match &mut self.state {
            ControlledPluginState::Starting(starting) => starting.request_force_stop(),
            ControlledPluginState::CleaningStartFailure(cleanup) => cleanup
                .request_force_stop()
                .map_err(PluginLifecycleError::Stop),
            ControlledPluginState::Ready(ready) => ready.request_force_stop(),
            ControlledPluginState::CleaningRuntimeFailure(cleanup) => cleanup.request_force_stop(),
            ControlledPluginState::Quiescing(quiescing) => quiescing.request_force_stop(),
            ControlledPluginState::Quiesced(quiesced) => quiesced.request_force_stop(),
            ControlledPluginState::ShuttingDown(shutdown) => shutdown.request_force_stop(),
            ControlledPluginState::ForceStopping(process) => process
                .request_force_stop()
                .map_err(PluginLifecycleError::Stop),
            ControlledPluginState::StartFailed(_)
            | ControlledPluginState::RestartBackoff(_)
            | ControlledPluginState::Stopped => Ok(()),
        }
    }

    #[cfg(feature = "repository-test-support")]
    pub(super) fn close_lifetime_channel(&mut self) -> Result<(), PluginLifecycleError> {
        match &mut self.state {
            ControlledPluginState::Ready(ready) => ready.close_lifetime_channel(),
            _ => Err(PluginLifecycleError::InvalidTransition),
        }
    }

    pub(super) async fn reap_after_stop(&mut self) -> Result<(), PluginLifecycleError> {
        let result = match &mut self.state {
            ControlledPluginState::Starting(starting) => starting.reap_after_stop().await,
            ControlledPluginState::CleaningStartFailure(cleanup) => cleanup
                .reap_after_stop()
                .await
                .map_err(PluginLifecycleError::Stop),
            ControlledPluginState::Ready(ready) => ready.reap_after_stop().await,
            ControlledPluginState::CleaningRuntimeFailure(cleanup) => {
                cleanup.reap_after_stop().await
            }
            ControlledPluginState::Quiescing(quiescing) => quiescing.reap_after_stop().await,
            ControlledPluginState::Quiesced(quiesced) => quiesced.reap_after_stop().await,
            ControlledPluginState::ShuttingDown(shutdown) => shutdown.finish().await,
            ControlledPluginState::ForceStopping(process) => process
                .reap_after_stop()
                .await
                .map_err(PluginLifecycleError::Stop),
            ControlledPluginState::StartFailed(_)
            | ControlledPluginState::RestartBackoff(_)
            | ControlledPluginState::Stopped => Ok(()),
        };
        if result.is_ok() {
            self.state = ControlledPluginState::Stopped;
        }
        result
    }

    fn launch_state(
        launch: ControlledPluginLaunch<'_>,
        diagnostics: PluginDiagnosticPublisher,
        on_created: impl FnOnce(),
    ) -> ControlledPluginState {
        let ControlledPluginLaunch {
            metrics_attributes,
            process,
            interface,
            control,
        } = launch;
        let pending = control.register(interface);
        let launch_id = *pending.launch_id();
        let metrics = control.register_metrics(&launch_id, metrics_attributes);
        match spawn_plugin(
            process,
            control.socket_path(),
            &launch_id,
            diagnostics,
            metrics,
        ) {
            PluginSpawnOutcome::Started { process, config } => {
                on_created();
                ControlledPluginState::Starting(ControlledPluginStartup {
                    process: Some(process),
                    config,
                    control: ControlStartup::Attaching(Box::pin(pending.attach())),
                })
            }
            PluginSpawnOutcome::Failed(failure) => ControlledPluginState::StartFailed(failure),
            PluginSpawnOutcome::Cleanup(cleanup) => {
                on_created();
                ControlledPluginState::CleaningStartFailure(cleanup)
            }
        }
    }

    fn begin_runtime_backoff_cleanup(&mut self, failure: ControlledRuntimeFailure) {
        let state = std::mem::replace(&mut self.state, ControlledPluginState::Stopped);
        let ready = match state {
            ControlledPluginState::Ready(ready) => ready,
            state => {
                self.state = state;
                unreachable!("runtime failure cleanup must start from the Ready state")
            }
        };
        self.state = ControlledPluginState::CleaningRuntimeFailure(
            ControlledRuntimeFailureCleanup::new(ready.process, failure),
        );
    }
}

impl fmt::Debug for PluginInstanceOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginInstanceOwner")
            .field("state", &self.state)
            .field("next_retry_delay", &self.next_retry_delay)
            .finish()
    }
}

#[derive(Debug)]
enum ControlledPluginState {
    Starting(ControlledPluginStartup),
    CleaningStartFailure(StartFailureCleanup),
    Ready(Box<ReadyControlledPlugin>),
    CleaningRuntimeFailure(ControlledRuntimeFailureCleanup),
    StartFailed(PluginStartFailure),
    RestartBackoff(ControlledRestartBackoff),
    Quiescing(QuiescingControlledPlugin),
    Quiesced(QuiescedControlledPlugin),
    ShuttingDown(ControlledPluginShutdown),
    ForceStopping(PluginProcess),
    Stopped,
}

#[cfg(test)]
mod tests;
