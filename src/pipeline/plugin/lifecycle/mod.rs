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

//! Spawns Plugin processes and owns their pipes, config delivery, and final reap.
//!
//! The Instance lifecycle owns protocol stages and retries. This module owns
//! each operating-system resource and preserves partial config-write progress.

use std::error::Error;
use std::fmt;
use std::future::Future as _;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::{Pin, pin};
use std::process::{ExitStatus, Stdio};
use std::task::{Context, Poll};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use tokio::io::AsyncWrite;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use zeroize::Zeroizing;

use crate::contracts::core::ControlError;
use crate::identifiers::FlowId;
use crate::pipeline::diagnostics::PluginDiagnosticPublisher;
use crate::tenon_document::verified::{ExtraArgs, ExtraArgsPosition};

use super::control::{
    PLUGIN_LAUNCH_ID_LENGTH, PluginControlAttachmentError, PluginControlSessionError,
};
pub(super) use super::controlled_lifecycle::PluginInstanceOwner;
use super::output::PluginOutput;

/// Immutable retry parameters shared by every Plugin in one Pipeline.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PluginRetryBackoff {
    pub(super) initial_delay: Duration,
    maximum_delay: Duration,
}

impl PluginRetryBackoff {
    /// Creates one already validated non-zero capped exponential range.
    #[must_use]
    pub(crate) const fn new(initial_delay: Duration, maximum_delay: Duration) -> Self {
        Self {
            initial_delay,
            maximum_delay,
        }
    }

    pub(super) fn next_delay(self, current: Duration) -> Duration {
        current.saturating_mul(2).min(self.maximum_delay)
    }
}

/// Launch material borrowed from the sole validated applied revision.
pub(crate) struct PluginLaunch<'a> {
    pub(crate) program_directory: &'a Path,
    pub(crate) command: &'a [String],
    pub(crate) working_directory: PathBuf,
    pub(crate) config: &'a serde_json::Value,
    pub(crate) extra_args: Option<&'a ExtraArgs>,
    pub(crate) env: Option<&'a std::collections::BTreeMap<String, String>>,
    pub(crate) bells: PluginBells,
}

/// The doorbell wiring one Instance process is launched with.
///
/// An Interface that Sources a Flow rings exactly one Channel doorbell Region;
/// an Interface that consumes Flows rings one Region per Sink input. A key is
/// absent exactly when the Interface does not include that direction, and the
/// SDK turns a direction it does implement but was not told about into a
/// startup failure rather than a fallback.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PluginBells {
    /// The Channel doorbell Region the Source side of this Instance rings.
    ///
    /// A Plugin Interface that includes Source is bound to exactly one Flow, so
    /// this is one path or absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_channel_region: Option<PathBuf>,
    /// Every Sink input in startup order, for an Interface that includes Sink.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sink_inputs: Option<Vec<SinkChannel>>,
}

/// One Sink input identity in the target revision's startup material.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SinkChannel {
    /// Exact authored Flow identity.
    pub(crate) flow_id: FlowId,
    /// Zero-based Channel index within that Flow.
    pub(crate) channel_id: u32,
    /// The Channel doorbell region the Sink rings when it releases this input.
    ///
    /// The ring targets the slot that Channel published, so this region belongs
    /// to the waiting Channel loop and not to the Sink: the Sink's own doorbell
    /// lives in its instance's `sink/loops.bells`.
    pub(crate) channel_bell_path: PathBuf,
}

/// Everything the Pipeline tells one Plugin process before it starts.
///
/// One reserved startup option carries the whole document as compact JSON, so
/// every Instance process has the same argument shape whatever its Plugin
/// Interface and whatever arguments the Program manifest declares. Both SDKs
/// reject an unknown key, so a misspelled direction fails startup instead of
/// silently idling.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PluginSdkConfig<'a> {
    /// The Instance directory the Plugin reads its Queues and Regions under.
    working_directory: &'a Path,
    /// The control endpoint the Plugin attaches its lifecycle stream to.
    control_socket: &'a Path,
    /// One per-process launch identity, as canonical padded Base64.
    launch_id: String,
    #[serde(flatten)]
    bells: &'a PluginBells,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PluginStatusState {
    Starting,
    Running,
    StartFailed,
    RestartBackoff,
}

pub(super) enum PluginStateEvent {
    StatusChanged,
    ProcessFailed,
    RestartDue,
}

pub(super) enum PluginSpawnOutcome {
    Started {
        process: PluginProcess,
        config: PluginStartupWriter,
    },
    Failed(PluginStartFailure),
    Cleanup(StartFailureCleanup),
}

pub(super) fn spawn_plugin(
    launch: PluginLaunch<'_>,
    socket_path: &Path,
    launch_id: &[u8; PLUGIN_LAUNCH_ID_LENGTH],
    diagnostics: PluginDiagnosticPublisher,
    metrics: super::metrics::Registration,
) -> PluginSpawnOutcome {
    let Some((program, arguments)) = launch.command.split_first() else {
        return PluginSpawnOutcome::Failed(PluginStartFailure::CommandMissing);
    };
    let sdk_config = match serde_json::to_string(&PluginSdkConfig {
        working_directory: &launch.working_directory,
        control_socket: socket_path,
        launch_id: STANDARD.encode(launch_id),
        bells: &launch.bells,
    }) {
        Ok(sdk_config) => sdk_config,
        Err(source) => {
            return PluginSpawnOutcome::Failed(PluginStartFailure::SdkConfigSerialization(source));
        }
    };
    let config = match PluginStartupWriter::new(launch.config) {
        Ok(config) => config,
        Err(source) => {
            return PluginSpawnOutcome::Failed(PluginStartFailure::ConfigSerialization(source));
        }
    };

    let mut command = Command::new(resolve_program(launch.program_directory, program));
    let extra_args = launch.extra_args.map_or(&[][..], ExtraArgs::args);
    match launch
        .extra_args
        .map_or(ExtraArgsPosition::Append, ExtraArgs::position)
    {
        ExtraArgsPosition::Append => command.args(arguments).args(extra_args),
        ExtraArgsPosition::Prepend => command.args(extra_args).args(arguments),
    };
    command.arg("--sdk-config").arg(sdk_config);
    command
        .current_dir(launch.program_directory)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .envs(launch.env.into_iter().flat_map(|env| env.iter()))
        .kill_on_drop(true);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(source) => return PluginSpawnOutcome::Failed(PluginStartFailure::Spawn(source)),
    };
    let Some(lifetime_channel) = child.stdin.take() else {
        return PluginSpawnOutcome::Cleanup(StartFailureCleanup::new(
            child,
            PluginStartFailure::PipeUnavailable,
        ));
    };
    let Some(stdout) = child.stdout.take() else {
        return PluginSpawnOutcome::Cleanup(StartFailureCleanup::new(
            child,
            PluginStartFailure::PipeUnavailable,
        ));
    };
    let Some(stderr) = child.stderr.take() else {
        return PluginSpawnOutcome::Cleanup(StartFailureCleanup::new(
            child,
            PluginStartFailure::PipeUnavailable,
        ));
    };
    PluginSpawnOutcome::Started {
        process: PluginProcess::new(
            lifetime_channel,
            child,
            stdout,
            stderr,
            diagnostics,
            metrics,
        ),
        config,
    }
}

fn resolve_program(program_directory: &Path, program: &str) -> PathBuf {
    let path = Path::new(program);
    if path.is_absolute() || path.components().count() == 1 {
        path.to_owned()
    } else {
        program_directory.join(path)
    }
}

pub(super) struct PluginStartupWriter {
    startup_input: Zeroizing<Vec<u8>>,
    written: usize,
}

impl PluginStartupWriter {
    fn new(config: &serde_json::Value) -> Result<Self, serde_json::Error> {
        let mut startup_input = Zeroizing::new(serde_json::to_vec(config)?);
        startup_input.push(b'\n');
        Ok(Self {
            startup_input,
            written: 0,
        })
    }

    pub(super) fn poll(
        &mut self,
        lifetime_channel: &mut ChildStdin,
        context: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        while self.written < self.startup_input.len() {
            match Pin::new(&mut *lifetime_channel)
                .poll_write(context, &self.startup_input[self.written..])
            {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "Plugin config channel accepted zero bytes",
                    )));
                }
                Poll::Ready(Ok(count)) => self.written += count,
                Poll::Ready(Err(source)) => return Poll::Ready(Err(source)),
                Poll::Pending => return Poll::Pending,
            }
        }
        Pin::new(lifetime_channel).poll_flush(context)
    }
}

impl fmt::Debug for PluginStartupWriter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginStartupWriter")
            .field("config_length", &self.startup_input.len())
            .field("written", &self.written)
            .finish()
    }
}

pub(super) struct PluginProcess {
    metrics: Option<super::metrics::Registration>,
    #[cfg(not(feature = "repository-test-support"))]
    lifetime_channel: ChildStdin,
    // Repository tests can independently release this OS resource to reproduce owner loss.
    #[cfg(feature = "repository-test-support")]
    lifetime_channel: Option<ChildStdin>,
    child: Child,
    _output: PluginOutput,
    observed_exit: Option<ExitStatus>,
    stop_request: ProcessStopRequest,
}

impl PluginProcess {
    fn new(
        lifetime_channel: ChildStdin,
        child: Child,
        stdout: ChildStdout,
        stderr: ChildStderr,
        diagnostics: PluginDiagnosticPublisher,
        metrics: super::metrics::Registration,
    ) -> Self {
        Self {
            #[cfg(not(feature = "repository-test-support"))]
            lifetime_channel,
            #[cfg(feature = "repository-test-support")]
            lifetime_channel: Some(lifetime_channel),
            child,
            _output: PluginOutput::new(stdout, stderr, diagnostics),
            metrics: Some(metrics),
            observed_exit: None,
            stop_request: ProcessStopRequest::None,
        }
    }

    pub(super) fn process_id(&self) -> Option<u32> {
        self.child.id()
    }

    pub(super) const fn has_exited(&self) -> bool {
        self.observed_exit.is_some()
    }

    /// Reports whether this child was already asked to stop.
    pub(super) const fn stop_requested(&self) -> bool {
        !matches!(self.stop_request, ProcessStopRequest::None)
    }

    #[cfg(feature = "repository-test-support")]
    pub(super) fn close_lifetime_channel(&mut self) {
        let Some(lifetime_channel) = self.lifetime_channel.take() else {
            unreachable!("lifetime channel can only be closed once");
        };
        drop(lifetime_channel);
    }

    pub(super) fn poll_config(
        &mut self,
        config: &mut PluginStartupWriter,
        context: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        #[cfg(not(feature = "repository-test-support"))]
        let lifetime_channel = &mut self.lifetime_channel;
        #[cfg(feature = "repository-test-support")]
        let Some(lifetime_channel) = self.lifetime_channel.as_mut() else {
            unreachable!("lifetime channel must remain open while plugin config is written");
        };
        config.poll(lifetime_channel, context)
    }

    pub(super) fn poll_exit(&mut self, context: &mut Context<'_>) -> Poll<io::Result<ExitStatus>> {
        if let Some(status) = self.observed_exit {
            return Poll::Ready(Ok(status));
        }
        let mut waiting = pin!(self.child.wait());
        match waiting.as_mut().poll(context) {
            Poll::Ready(Ok(status)) => {
                self.observed_exit = Some(status);
                self.metrics.take();
                Poll::Ready(Ok(status))
            }
            Poll::Ready(Err(source)) => Poll::Ready(Err(source)),
            Poll::Pending => Poll::Pending,
        }
    }

    pub(super) fn request_force_stop(&mut self) -> io::Result<()> {
        if self.stop_request == ProcessStopRequest::Force || self.observed_exit.is_some() {
            return Ok(());
        }
        request_force_kill(&mut self.child)?;
        self.stop_request = ProcessStopRequest::Force;
        Ok(())
    }

    pub(super) async fn reap_after_stop(&mut self) -> io::Result<()> {
        if self.observed_exit.is_some() {
            return Ok(());
        }
        if self.stop_request == ProcessStopRequest::None {
            return Err(io::Error::other(
                "Plugin process stop was not successfully requested",
            ));
        }
        self.wait_for_exit().await.map(|_| ())
    }

    pub(super) async fn wait_for_exit(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.observed_exit {
            return Ok(status);
        }
        let status = self.child.wait().await?;
        self.observed_exit = Some(status);
        self.metrics.take();
        Ok(status)
    }

    pub(super) fn into_child(self) -> Child {
        self.child
    }
}

impl fmt::Debug for PluginProcess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginProcess")
            .field("process_id", &self.process_id())
            .field("observed_exit", &self.observed_exit)
            .field("stop_request", &self.stop_request)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessStopRequest {
    None,
    Force,
}

pub(super) struct StartFailureCleanup {
    child: Child,
    failure: Option<PluginStartFailure>,
    stop: CleanupStop,
}

impl StartFailureCleanup {
    pub(super) fn new(child: Child, failure: PluginStartFailure) -> Self {
        Self {
            child,
            failure: Some(failure),
            stop: CleanupStop::Pending,
        }
    }

    pub(super) fn poll(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Result<PluginStartFailure, PluginLifecycleError>> {
        if self.stop == CleanupStop::Reaped {
            return Poll::Ready(
                self.failure
                    .take()
                    .ok_or(PluginLifecycleError::InvalidTransition),
            );
        }
        if self.stop == CleanupStop::Pending {
            if let Err(source) = request_force_kill(&mut self.child) {
                return Poll::Ready(Err(PluginLifecycleError::Cleanup(source)));
            }
            self.stop = CleanupStop::Requested;
        }
        let mut waiting = pin!(self.child.wait());
        match waiting.as_mut().poll(context) {
            Poll::Ready(Ok(_)) => Poll::Ready(
                self.failure
                    .take()
                    .ok_or(PluginLifecycleError::InvalidTransition),
            ),
            Poll::Ready(Err(source)) => Poll::Ready(Err(PluginLifecycleError::Cleanup(source))),
            Poll::Pending => Poll::Pending,
        }
    }

    pub(super) fn request_force_stop(&mut self) -> io::Result<()> {
        if self.stop != CleanupStop::Pending {
            return Ok(());
        }
        request_force_kill(&mut self.child)?;
        self.stop = CleanupStop::Requested;
        Ok(())
    }

    pub(super) async fn reap_after_stop(&mut self) -> io::Result<()> {
        match self.stop {
            CleanupStop::Pending => Err(io::Error::other(
                "Plugin process stop was not successfully requested",
            )),
            CleanupStop::Requested => {
                self.child.wait().await?;
                self.stop = CleanupStop::Reaped;
                Ok(())
            }
            CleanupStop::Reaped => Ok(()),
        }
    }
}

impl fmt::Debug for StartFailureCleanup {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StartFailureCleanup")
            .field("process_id", &self.child.id())
            .field("failure", &self.failure)
            .field("stop", &self.stop)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupStop {
    Pending,
    Requested,
    Reaped,
}

/// One local Plugin process failure before READY.
#[derive(Debug)]
pub(super) enum PluginStartFailure {
    CommandMissing,
    ConfigSerialization(serde_json::Error),
    SdkConfigSerialization(serde_json::Error),
    Spawn(io::Error),
    PipeUnavailable,
    ConfigWrite(io::Error),
    ExitedBeforeReady,
    ControlAttachment(PluginControlAttachmentError),
    ControlSession(PluginControlSessionError),
}

impl PluginStartFailure {
    pub(super) fn control_error(&self) -> ControlError {
        match self {
            Self::CommandMissing => {
                control_error("plugin.command_missing", "Plugin launch command is missing")
            }
            Self::ConfigSerialization(_) => control_error(
                "plugin.config_serialization_failed",
                "Plugin configuration could not be serialized",
            ),
            Self::SdkConfigSerialization(_) => control_error(
                "plugin.sdk_config_serialization_failed",
                "Plugin startup document could not be serialized",
            ),
            Self::Spawn(_) => {
                control_error("plugin.spawn_failed", "Plugin process could not be spawned")
            }
            Self::PipeUnavailable => control_error(
                "plugin.process_channel_unavailable",
                "Plugin process channel is unavailable",
            ),
            Self::ConfigWrite(_) => control_error(
                "plugin.config_write_failed",
                "Plugin configuration could not be written",
            ),
            Self::ExitedBeforeReady => control_error(
                "plugin.exited_before_ready",
                "Plugin process exited before READY",
            ),
            Self::ControlAttachment(_) => control_error(
                "plugin.control_attach_failed",
                "Plugin process did not attach its control stream",
            ),
            Self::ControlSession(_) => control_error(
                "plugin.control_ready_failed",
                "Plugin process did not report Ready through its control stream",
            ),
        }
    }
}

impl fmt::Display for PluginStartFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.control_error().message)
    }
}

impl Error for PluginStartFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConfigSerialization(source) | Self::SdkConfigSerialization(source) => {
                Some(source)
            }
            Self::Spawn(source) | Self::ConfigWrite(source) => Some(source),
            Self::ControlAttachment(source) => Some(source),
            Self::ControlSession(source) => Some(source),
            Self::CommandMissing | Self::PipeUnavailable | Self::ExitedBeforeReady => None,
        }
    }
}

#[derive(Debug)]
pub(crate) enum PluginLifecycleError {
    Status(io::Error),
    Cleanup(io::Error),
    Stop(io::Error),
    Control(PluginControlSessionError),
    /// A pending old session failed before it could reach Ready during handoff.
    StartFailed(ControlError),
    ExitedDuringShutdown(ExitStatus),
    InvalidTransition,
}

impl fmt::Display for PluginLifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status(_) => formatter.write_str("Plugin process status could not be read"),
            Self::Cleanup(_) => formatter.write_str("Plugin process cleanup failed"),
            Self::Stop(_) => formatter.write_str("Plugin process could not be stopped"),
            Self::Control(_) => formatter.write_str("Plugin control lifecycle failed"),
            Self::StartFailed(error) => {
                write!(
                    formatter,
                    "Plugin process failed before Ready: {}",
                    error.message
                )
            }
            Self::ExitedDuringShutdown(status) => {
                write!(
                    formatter,
                    "Plugin process exited during staged shutdown: {status}"
                )
            }
            Self::InvalidTransition => {
                formatter.write_str("Plugin lifecycle transition is invalid")
            }
        }
    }
}

impl Error for PluginLifecycleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Status(source) | Self::Stop(source) | Self::Cleanup(source) => Some(source),
            Self::Control(source) => Some(source),
            Self::StartFailed(_) | Self::ExitedDuringShutdown(_) | Self::InvalidTransition => None,
        }
    }
}

/// Requests forceful termination without waiting for the child to exit.
fn request_force_kill(child: &mut Child) -> io::Result<()> {
    match child.start_kill() {
        Ok(()) => Ok(()),
        Err(termination) => match child.try_wait() {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(io::Error::new(
                termination.kind(),
                format!("Child process could not be terminated: {termination}"),
            )),
            Err(status) => Err(io::Error::new(
                status.kind(),
                format!(
                    "Child process termination failed: {termination}; status check failed: {status}"
                ),
            )),
        },
    }
}

fn control_error(code: &'static str, message: &'static str) -> ControlError {
    ControlError {
        code: code.to_owned(),
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests;
