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

//! Runs the Pipeline process main-thread control loop.
//!
//! This module owns control transport, parent lifetime, and outbound status.
//! Serialized revision reconstruction and application live behind the independent
//! [`PipelineController`] actor boundary.

use super::metrics::PluginMetrics;
use crate::metrics::{CoreProcess, MetricsRuntime};
use std::error::Error;
use std::fmt;
use std::io;
use std::os::fd::AsFd as _;
use std::path::Path;

use tokio::net::unix::pipe::Receiver;
use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::watch;
use tokio_stream::{Stream, StreamExt as _};
use tonic::transport::{Channel, Endpoint};

use crate::contracts::core;
use crate::pipeline::controller::{PipelineController, PipelineControllerError};
use crate::pipeline::diagnostics::PipelineDiagnostics;
use crate::pipeline::metrics_stream::PipelineMetrics;
use crate::pipeline::plugin::{PluginControlServer, PluginControlServerError};
use crate::pipeline::reconfigure::revision::PipelineRevision;
use crate::pipeline::reconfigure::start_controller;
use crate::pipeline::terminate_pipeline;
use crate::plugin_control_directory;
use tokio_stream::wrappers::WatchStream;

/// Immutable process arguments consumed by one Pipeline Control Loop.
pub(super) struct PipelineLaunch {
    pub(super) control_socket: Box<str>,
    pub(super) launch_id: Vec<u8>,
}

/// Runs one Pipeline control stream until a terminal lifecycle or runtime event.
pub(crate) async fn run(launch: PipelineLaunch) -> Result<(), PipelineControlLoopError> {
    let lifetime = PipelineLifetimeChannel::open()?;
    let mut termination = signal(SignalKind::terminate())
        .map_err(PipelineControlLoopError::TerminationSignalInstall)?;
    let ready = tokio::select! {
        biased;
        outcome = lifetime.wait() => {
            return Err(match outcome {
                Ok(LifetimeChannelOutcome::Closed) => terminate_pipeline(),
                other => parent_lifetime_error(other),
            });
        }
        received = termination.recv() => {
            return termination_before_bootstrap(received);
        }
        result = bootstrap(launch) => result?,
    };

    run_control_loop(ready, lifetime, &mut termination).await
}

fn termination_before_bootstrap(received: Option<()>) -> Result<(), PipelineControlLoopError> {
    received.ok_or(PipelineControlLoopError::TerminationSignalStreamClosed)
}

struct BootstrapReady<Incoming> {
    inbound: Incoming,
    statuses: watch::Sender<Option<core::PipelineStatusSnapshot>>,
    resources: PipelineProcessResources,
}

/// Bundles the Runner control duplex the control loop polls each iteration.
struct RunnerLink<Incoming> {
    inbound: Incoming,
    statuses: watch::Sender<Option<core::PipelineStatusSnapshot>>,
}

/// The Runner dropped the status receiver, so the control stream is gone.
struct StatusStreamClosed;

impl<Incoming> RunnerLink<Incoming>
where
    Incoming: Stream<Item = Result<core::PipelineRevisionPlan, PipelineControlLoopError>> + Unpin,
{
    async fn next_revision(
        &mut self,
    ) -> Option<Result<core::PipelineRevisionPlan, PipelineControlLoopError>> {
        self.inbound.next().await
    }

    fn publish_status(
        &self,
        status: core::PipelineStatusSnapshot,
    ) -> Result<(), StatusStreamClosed> {
        self.statuses
            .send(Some(status))
            .map_err(|_| StatusStreamClosed)
    }
}

/// Owns the process resources that share one ordered teardown after bootstrap.
struct PipelineProcessResources {
    controller: PipelineController,
    plugin_control: PluginControlServer,
    diagnostics: PipelineDiagnostics,
    metrics: PipelineMetrics,
    observations: PluginMetrics,
}

/// One steady-state observation from the owned process resources.
enum ProcessObservation {
    Status(core::PipelineStatusSnapshot),
    ControllerFailed(PipelineControllerError),
    PluginControlStopped(Result<(), PluginControlServerError>),
}

impl PipelineProcessResources {
    fn submit_latest(&self, revision: core::PipelineRevisionPlan) {
        self.controller.submit_latest(revision);
    }

    /// Observes the next steady-state event, keeping controller priority over plugin control.
    async fn observe(&mut self) -> ProcessObservation {
        tokio::select! {
            biased;
            result = self.controller.next_status() => match result {
                Ok(status) => {
                    self.observations.publish(&status);
                    ProcessObservation::Status(status)
                }
                Err(source) => ProcessObservation::ControllerFailed(source),
            },
            result = self.plugin_control.wait() => ProcessObservation::PluginControlStopped(result),
        }
    }

    /// Runs the graceful controller shutdown while a Runner/parent loss still force-kills.
    async fn finish_planned<Incoming>(
        self,
        link: &mut RunnerLink<Incoming>,
        lifetime: &PipelineLifetimeChannel,
    ) -> Result<(), PipelineControlLoopError>
    where
        Incoming:
            Stream<Item = Result<core::PipelineRevisionPlan, PipelineControlLoopError>> + Unpin,
    {
        let Self {
            controller,
            plugin_control,
            diagnostics,
            metrics,
            observations: _,
        } = self;
        let mut finishing = std::pin::pin!(controller.shutdown_and_wait());
        let result = loop {
            tokio::select! {
                biased;
                _ = lifetime.wait() => terminate_pipeline(),
                message = link.next_revision() => match message {
                    Some(Ok(_)) => {}
                    None | Some(Err(_)) => terminate_pipeline(),
                },
                result = &mut finishing => {
                    break result.map_err(PipelineControlLoopError::Controller);
                }
            }
        };
        shutdown_side_channels(diagnostics, plugin_control, metrics)
            .await
            .map_err(PipelineControlLoopError::PluginControl)?;
        result
    }

    /// Force-stops the controller and side channels, then returns the terminal error.
    async fn finish_forced(
        self,
        error: PipelineControlLoopError,
    ) -> Result<(), PipelineControlLoopError> {
        let Self {
            controller,
            plugin_control,
            diagnostics,
            metrics,
            observations: _,
        } = self;
        let _cleanup = controller.force_shutdown_and_wait().await;
        let _cleanup = shutdown_side_channels(diagnostics, plugin_control, metrics).await;
        Err(error)
    }
}

/// The single ordered teardown for the bootstrap side channels.
async fn shutdown_side_channels(
    diagnostics: PipelineDiagnostics,
    plugin_control: PluginControlServer,
    metrics: PipelineMetrics,
) -> Result<(), PluginControlServerError> {
    diagnostics.shutdown().await;
    let plugin_result = plugin_control.shutdown().await;
    metrics.shutdown().await;
    plugin_result
}

#[allow(
    clippy::expect_used,
    reason = "Pipeline metrics reuse their existing launch identity and cannot fail random generation"
)]
async fn bootstrap(
    launch: PipelineLaunch,
) -> Result<
    BootstrapReady<
        impl Stream<Item = Result<core::PipelineRevisionPlan, PipelineControlLoopError>> + Unpin,
    >,
    PipelineControlLoopError,
> {
    let channel = connect_control(&launch.control_socket).await?;
    let mut client = core::pipeline_control_client::PipelineControlClient::new(channel.clone())
        .max_decoding_message_size(usize::MAX);
    let attach = core::PipelineToRunner {
        message: Some(core::pipeline_to_runner::Message::Attach(
            core::PipelineAttach {
                launch_id: launch.launch_id.clone(),
            },
        )),
    };
    let (statuses, status_stream) = watch::channel(None);
    let response = client
        .run(
            tokio_stream::iter([attach]).chain(WatchStream::new(status_stream).filter_map(
                |status| {
                    status.map(|status| core::PipelineToRunner {
                        message: Some(core::pipeline_to_runner::Message::StatusSnapshot(status)),
                    })
                },
            )),
        )
        .await
        .map_err(PipelineControlLoopError::ControlStream)?;
    let mut runner_to_pipeline = response.into_inner();
    let bootstrap = receive_bootstrap(&mut runner_to_pipeline).await?;
    let environment = bootstrap
        .environment
        .expect("Runner supplies the Pipeline environment");
    let revision = bootstrap
        .revision_plan
        .expect("Runner supplies the initial revision");
    let revision = tokio::task::spawn_blocking(move || PipelineRevision::from_runner(revision))
        .await
        .map_err(PipelineControlLoopError::ControlMessageTask)?;
    let plugin_control = PluginControlServer::start(
        plugin_control_directory(Path::new(launch.control_socket.as_ref()), &launch.launch_id)
            .join(crate::PLUGIN_CONTROL_SOCKET_NAME),
    )
    .map_err(PipelineControlLoopError::PluginControl)?;
    let metrics = MetricsRuntime::start(
        environment.metrics_node_id.as_deref(),
        CoreProcess::Pipeline {
            document_id: revision.document().id().as_str(),
            launch_id: &launch.launch_id,
        },
    )
    .expect("Pipeline metrics use the existing launch identity without randomness");
    let metrics = PipelineMetrics::start(
        metrics,
        &launch.control_socket,
        launch.launch_id.clone(),
        plugin_control.process_metrics(),
    );
    let meter = metrics.meter();
    let diagnostics = PipelineDiagnostics::start(channel, launch.launch_id);
    let launcher = plugin_control
        .launcher()
        .with_node_id(environment.metrics_node_id.clone())
        .with_metrics(Some(&meter));
    let controller = start_controller(
        revision,
        environment,
        diagnostics.publisher(),
        launcher,
        Some(&meter),
    );
    let observations = PluginMetrics::new(&meter);
    Ok(BootstrapReady {
        inbound: runner_to_pipeline.map(|message| {
            let message = message.map_err(PipelineControlLoopError::ControlStream)?;
            match message.message {
                Some(core::runner_to_pipeline::Message::RevisionPlan(revision)) => Ok(revision),
                _ => unreachable!("Runner sends only revisions after Bootstrap"),
            }
        }),
        statuses,
        resources: PipelineProcessResources {
            controller,
            plugin_control,
            diagnostics,
            metrics,
            observations,
        },
    })
}

async fn connect_control(socket: &str) -> Result<Channel, PipelineControlLoopError> {
    Endpoint::from_shared(format!("unix://{socket}"))
        .map_err(PipelineControlLoopError::ControlConnection)?
        .connect()
        .await
        .map_err(PipelineControlLoopError::ControlConnection)
}

async fn receive_bootstrap(
    incoming: &mut tonic::Streaming<core::RunnerToPipeline>,
) -> Result<core::PipelineBootstrap, PipelineControlLoopError> {
    let message = incoming
        .message()
        .await
        .map_err(PipelineControlLoopError::ControlStream)?
        .ok_or(PipelineControlLoopError::ControlStreamDisconnected)?;
    match message.message {
        Some(core::runner_to_pipeline::Message::Bootstrap(bootstrap)) => Ok(bootstrap),
        _ => unreachable!("Runner sends Bootstrap as its first message"),
    }
}

async fn run_control_loop<Incoming>(
    ready: BootstrapReady<Incoming>,
    lifetime: PipelineLifetimeChannel,
    termination: &mut Signal,
) -> Result<(), PipelineControlLoopError>
where
    Incoming: Stream<Item = Result<core::PipelineRevisionPlan, PipelineControlLoopError>> + Unpin,
{
    let BootstrapReady {
        inbound,
        statuses,
        mut resources,
    } = ready;
    let mut link = RunnerLink { inbound, statuses };

    loop {
        let decision = tokio::select! {
            biased;
            outcome = lifetime.wait() => match outcome {
                Ok(LifetimeChannelOutcome::Closed) => ControlLoopDecision::RunnerLost,
                other => ControlLoopDecision::Terminal(parent_lifetime_error(other)),
            },
            received = termination.recv() => match received {
                Some(()) => ControlLoopDecision::PlannedShutdown,
                None => ControlLoopDecision::Terminal(
                    PipelineControlLoopError::TerminationSignalStreamClosed,
                ),
            },
            observation = resources.observe() => match observation {
                ProcessObservation::Status(status) => match link.publish_status(status) {
                    Ok(()) => ControlLoopDecision::Continue,
                    Err(StatusStreamClosed) => ControlLoopDecision::RunnerLost,
                },
                ProcessObservation::ControllerFailed(source) => ControlLoopDecision::Terminal(
                    PipelineControlLoopError::Controller(source),
                ),
                ProcessObservation::PluginControlStopped(result) => {
                    ControlLoopDecision::Terminal(match result {
                        Ok(()) => PipelineControlLoopError::PluginControlStopped,
                        Err(source) => PipelineControlLoopError::PluginControl(source),
                    })
                }
            },
            message = link.next_revision() => match message {
                Some(Ok(revision)) => {
                    resources.submit_latest(revision);
                    ControlLoopDecision::Continue
                },
                None | Some(Err(PipelineControlLoopError::ControlStream(_))) => ControlLoopDecision::RunnerLost,
                Some(Err(error)) => ControlLoopDecision::Terminal(error),
            },
        };
        match decision {
            ControlLoopDecision::Continue => {}
            ControlLoopDecision::PlannedShutdown => {
                return resources.finish_planned(&mut link, &lifetime).await;
            }
            ControlLoopDecision::RunnerLost => terminate_pipeline(),
            ControlLoopDecision::Terminal(error) => return resources.finish_forced(error).await,
        }
    }
}

enum ControlLoopDecision {
    Continue,
    PlannedShutdown,
    RunnerLost,
    Terminal(PipelineControlLoopError),
}

fn parent_lifetime_error(outcome: io::Result<LifetimeChannelOutcome>) -> PipelineControlLoopError {
    match outcome {
        Ok(LifetimeChannelOutcome::Closed) => {
            unreachable!("Closed Pipeline lifetime bypassed Runner-loss handling")
        }
        Ok(LifetimeChannelOutcome::DataReceived) => {
            PipelineControlLoopError::ParentLifetimeDataReceived
        }
        Err(source) => PipelineControlLoopError::ParentLifetimeFailed(source),
    }
}

struct PipelineLifetimeChannel {
    input: Receiver,
}

impl PipelineLifetimeChannel {
    fn open() -> Result<Self, PipelineControlLoopError> {
        let input = io::stdin()
            .as_fd()
            .try_clone_to_owned()
            .map_err(PipelineControlLoopError::ParentLifetimeFailed)?;
        let input = Receiver::from_owned_fd(input)
            .map_err(PipelineControlLoopError::ParentLifetimeFailed)?;
        Ok(Self { input })
    }

    async fn wait(&self) -> io::Result<LifetimeChannelOutcome> {
        let mut byte = [0; 1];
        loop {
            self.input.readable().await?;
            match self.input.try_read(&mut byte) {
                Ok(0) => return Ok(LifetimeChannelOutcome::Closed),
                Ok(_) => return Ok(LifetimeChannelOutcome::DataReceived),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
        }
    }
}

enum LifetimeChannelOutcome {
    Closed,
    DataReceived,
}

/// A terminal failure owned by the Pipeline Control Loop boundary.
#[derive(Debug)]
pub(crate) enum PipelineControlLoopError {
    ControlConnection(tonic::transport::Error),
    ControlStream(tonic::Status),
    ControlStreamDisconnected,
    ControlMessageTask(tokio::task::JoinError),
    Controller(PipelineControllerError),
    PluginControl(PluginControlServerError),
    PluginControlStopped,
    TerminationSignalInstall(io::Error),
    TerminationSignalStreamClosed,
    ParentLifetimeDataReceived,
    ParentLifetimeFailed(io::Error),
}

impl PipelineControlLoopError {
    /// Returns the stable local diagnostic code for this terminal failure.
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::ControlConnection(_) => "pipeline.control_connection_failed",
            Self::ControlStream(_) => "pipeline.control_stream_failed",
            Self::ControlStreamDisconnected => "pipeline.control_stream_disconnected",
            Self::ControlMessageTask(_) => "pipeline.control_message_reconstruction_failed",
            Self::Controller(error) => error.code(),
            Self::PluginControl(_) | Self::PluginControlStopped => "pipeline.plugin_control_failed",
            Self::TerminationSignalInstall(_) => "pipeline.termination_signal_install_failed",
            Self::TerminationSignalStreamClosed => "pipeline.termination_signal_stream_closed",
            Self::ParentLifetimeDataReceived => "pipeline.parent_lifetime_data_received",
            Self::ParentLifetimeFailed(_) => "pipeline.parent_lifetime_failed",
        }
    }
}

impl fmt::Display for PipelineControlLoopError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ControlConnection(_) => {
                formatter.write_str("Pipeline could not connect to the Runner control socket")
            }
            Self::ControlStream(_) => formatter.write_str("Pipeline Runner control stream failed"),
            Self::ControlStreamDisconnected => {
                formatter.write_str("Runner disconnected the Pipeline control stream")
            }
            Self::ControlMessageTask(_) => {
                formatter.write_str("Pipeline control message reconstruction task failed")
            }
            Self::Controller(_) => formatter.write_str("Pipeline Controller failed"),
            Self::PluginControl(_) | Self::PluginControlStopped => {
                formatter.write_str("Pipeline Plugin Control service stopped")
            }
            Self::TerminationSignalInstall(_) => {
                formatter.write_str("Pipeline termination signal listener could not be installed")
            }
            Self::TerminationSignalStreamClosed => {
                formatter.write_str("Pipeline termination signal stream closed")
            }
            Self::ParentLifetimeDataReceived => {
                formatter.write_str("Pipeline lifetime channel received unexpected data")
            }
            Self::ParentLifetimeFailed(_) => {
                formatter.write_str("Pipeline lifetime channel failed")
            }
        }
    }
}

impl Error for PipelineControlLoopError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ControlConnection(error) => Some(error),
            Self::ControlStream(error) => Some(error),
            Self::ControlMessageTask(error) => Some(error),
            Self::Controller(error) => Some(error),
            Self::PluginControl(error) => Some(error),
            Self::TerminationSignalInstall(error) => Some(error),
            Self::ParentLifetimeFailed(error) => Some(error),
            Self::ControlStreamDisconnected
            | Self::PluginControlStopped
            | Self::TerminationSignalStreamClosed
            | Self::ParentLifetimeDataReceived => None,
        }
    }
}

#[cfg(test)]
mod tests;
