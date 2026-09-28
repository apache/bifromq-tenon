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

//! Narrow repository-test access to the production Plugin Control lifecycle.

use std::future::poll_fn;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;
use tokio::time::timeout;

use crate::contracts::core::{
    PipelineDiagnosticRecord, PluginDiagnosticStream, pipeline_diagnostic_record,
};
use crate::payload_contract::PluginInterface;
use crate::pipeline::diagnostics::repository_test_support::capture_plugin;
use crate::pipeline::plugin::control::{
    PluginControlLauncher, TestPluginControlServer as PluginControlServer,
};
use crate::pipeline::plugin::controlled_lifecycle::ControlledPluginLaunch;
use crate::pipeline::plugin::lifecycle::{
    PluginInstanceOwner, PluginLaunch, PluginRetryBackoff, PluginSpawnOutcome, PluginStateEvent,
    PluginStatusState, spawn_plugin,
};

use super::lifecycle::{PluginBells, SinkChannel};

const TEST_DEADLINE: Duration = Duration::from_secs(30);
const RETRY_DELAY: Duration = Duration::from_millis(10);

/// Borrowed material for one real Plugin Program launched by a repository test.
#[derive(Debug)]
pub struct PluginProgramLaunch<'a> {
    program_directory: &'a Path,
    command: &'a [String],
    working_directory: PathBuf,
    config: &'a Value,
    sink_channels: Vec<SinkChannel>,
    channel_bell_path: Option<PathBuf>,
}

impl<'a> PluginProgramLaunch<'a> {
    /// Creates one launch and validates its Flow identities at the production boundary.
    ///
    /// # Errors
    ///
    /// Returns the production identifier error for an invalid Flow id.
    pub fn new(
        program_directory: &'a Path,
        command: &'a [String],
        working_directory: PathBuf,
        config: &'a Value,
        sink_channels: Vec<(String, u32)>,
    ) -> Result<Self, crate::identifiers::IdentifierParseError> {
        let sink_channels = sink_channels
            .into_iter()
            .map(|(flow_id, channel_id)| {
                flow_id.try_into().map(|flow_id| SinkChannel {
                    flow_id,
                    channel_id,
                    channel_bell_path: working_directory.join("channels.bells"),
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            program_directory,
            command,
            channel_bell_path: Some(working_directory.join("channels.bells")),
            working_directory,
            config,
            sink_channels,
        })
    }
}

/// Returns the doorbell wiring one launch is told about.
///
/// A launch names the one Flow region its Source side rings, and every Sink
/// input in startup order. Each direction is present exactly when the Interface
/// includes it, so a standalone Source never names a Sink input.
fn bells(launch: &PluginProgramLaunch<'_>, interface: PluginInterface) -> PluginBells {
    PluginBells {
        source_channel_region: match interface {
            PluginInterface::Source | PluginInterface::SourceAndSink => {
                launch.channel_bell_path.clone()
            }
            PluginInterface::Sink => None,
        },
        sink_inputs: match interface {
            PluginInterface::Sink | PluginInterface::SourceAndSink => {
                Some(launch.sink_channels.clone())
            }
            PluginInterface::Source => None,
        },
    }
}

/// Runs one Source Program through Ready, Source quiesce, and final Shutdown.
pub async fn run_source_program<Ready>(
    launch: PluginProgramLaunch<'_>,
    ready: Ready,
) -> io::Result<()>
where
    Ready: FnOnce() -> io::Result<()>,
{
    run_program(
        launch,
        PluginInterface::Source,
        ready,
        None::<fn() -> io::Result<()>>,
    )
    .await
}

/// Runs one Sink Program through Ready and final Shutdown.
pub async fn run_sink_program<Ready>(
    launch: PluginProgramLaunch<'_>,
    ready: Ready,
) -> io::Result<()>
where
    Ready: FnOnce() -> io::Result<()>,
{
    run_program(
        launch,
        PluginInterface::Sink,
        ready,
        None::<fn() -> io::Result<()>>,
    )
    .await
}

/// Runs one combined Program and observes Sink work after Source quiescence.
pub async fn run_source_and_sink_program<Ready, SourceQuiesced>(
    launch: PluginProgramLaunch<'_>,
    ready: Ready,
    source_quiesced: SourceQuiesced,
) -> io::Result<()>
where
    Ready: FnOnce() -> io::Result<()>,
    SourceQuiesced: FnOnce() -> io::Result<()>,
{
    run_program(
        launch,
        PluginInterface::SourceAndSink,
        ready,
        Some(source_quiesced),
    )
    .await
}

/// Proves that one ready Java client exits after its Rust control stream disappears.
pub async fn run_sink_program_with_control_stream_loss(
    launch: PluginProgramLaunch<'_>,
) -> io::Result<()> {
    let server = PluginControlServer::start().map_err(io::Error::other)?;
    let launcher = server.launcher();
    let pending = launcher.register(PluginInterface::Sink);
    let (publisher, records) = capture_plugin(
        crate::identifiers::PluginInstanceId::try_from(String::from("fixture"))
            .map_err(io::Error::other)?,
    );
    let diagnostic_capture = tokio::spawn(collect_diagnostics(records));
    let bells = bells(&launch, PluginInterface::Sink);
    let spawned = spawn_plugin(
        PluginLaunch {
            program_directory: launch.program_directory,
            command: launch.command,
            working_directory: launch.working_directory,
            config: launch.config,
            extra_args: None,
            env: None,
            bells,
        },
        launcher.socket_path(),
        pending.launch_id(),
        publisher,
        launcher.register_metrics(pending.launch_id(), vec![]),
    );
    let operation = match spawned {
        PluginSpawnOutcome::Started {
            mut process,
            mut config,
        } => {
            let operation = timeout(TEST_DEADLINE, async {
                poll_fn(|context| process.poll_config(&mut config, context)).await?;
                let control = pending
                    .attach()
                    .await
                    .map_err(io::Error::other)?
                    .wait_for_ready()
                    .await
                    .map_err(io::Error::other)?;
                // Retire the real Rust stream while retaining stdin and the
                // child. No Instance supervisor can force-kill on stream loss.
                drop(control);
                let status = process.wait_for_exit().await?;
                if status.code().is_some_and(|code| code != 0) {
                    Ok(())
                } else {
                    Err(io::Error::other(format!(
                        "Plugin did not exit itself with an error: {status}"
                    )))
                }
            })
            .await
            .map_err(|_| io::Error::other("Plugin did not exit autonomously after stream loss"))
            .and_then(|result| result);
            let cleanup = if operation.is_err() {
                let requested = process.request_force_stop();
                let reaped = process.reap_after_stop().await;
                combine(requested, reaped, "Plugin failure cleanup reap failed")
            } else {
                Ok(())
            };
            combine(operation, cleanup, "Plugin failure cleanup failed")
        }
        PluginSpawnOutcome::Failed(failure) => Err(io::Error::other(format!(
            "Plugin spawn failed: {failure:?}"
        ))),
        PluginSpawnOutcome::Cleanup(mut cleanup) => {
            let failure = poll_fn(|context| cleanup.poll(context))
                .await
                .map_err(io::Error::other)?;
            Err(io::Error::other(format!(
                "Plugin spawn required cleanup: {failure:?}"
            )))
        }
    };
    let diagnostics = diagnostic_capture.await.map_err(io::Error::other)?;
    combine(
        attach_diagnostics(operation, &diagnostics),
        server.shutdown().await.map_err(io::Error::other),
        "Plugin Control server shutdown failed",
    )
}

/// Proves that one ready Java client exits when its Pipeline stdin owner disappears.
pub async fn run_sink_program_with_owner_loss(launch: PluginProgramLaunch<'_>) -> io::Result<()> {
    let mut program = LaunchedPluginProgram::start(launch, PluginInterface::Sink)?;
    let mut operation = wait_until_ready(&mut program.owner).await;
    if operation.is_ok() {
        operation = program
            .owner
            .close_lifetime_channel()
            .map_err(io::Error::other);
    }
    if operation.is_ok() {
        operation = wait_until_runtime_failure(&mut program.owner).await;
    }
    program.finish(operation).await
}

/// Proves that the Rust owner can force-kill and reap one ready Java client.
pub async fn force_stop_sink_program(launch: PluginProgramLaunch<'_>) -> io::Result<()> {
    let mut program = LaunchedPluginProgram::start(launch, PluginInterface::Sink)?;
    let mut operation = wait_until_ready(&mut program.owner).await;
    if operation.is_ok() {
        operation = force_stop(&mut program.owner).await;
    }
    program.finish(operation).await
}

async fn run_program<Ready, SourceQuiesced>(
    launch: PluginProgramLaunch<'_>,
    interface: PluginInterface,
    ready: Ready,
    source_quiesced: Option<SourceQuiesced>,
) -> io::Result<()>
where
    Ready: FnOnce() -> io::Result<()>,
    SourceQuiesced: FnOnce() -> io::Result<()>,
{
    let mut program = LaunchedPluginProgram::start(launch, interface)?;
    let operation = drive_program(&mut program.owner, interface, ready, source_quiesced).await;
    program.finish(operation).await
}

struct LaunchedPluginProgram {
    server: PluginControlServer,
    owner: PluginInstanceOwner,
    diagnostic_capture: tokio::task::JoinHandle<String>,
}

impl LaunchedPluginProgram {
    fn start(launch: PluginProgramLaunch<'_>, interface: PluginInterface) -> io::Result<Self> {
        let server = PluginControlServer::start().map_err(io::Error::other)?;
        let launcher = server.launcher();
        Ok(Self::start_with_server(
            launch, interface, server, &launcher,
        ))
    }

    #[allow(
        clippy::expect_used,
        reason = "the fixed repository fixture id is valid"
    )]
    fn start_with_server(
        launch: PluginProgramLaunch<'_>,
        interface: PluginInterface,
        server: PluginControlServer,
        launcher: &PluginControlLauncher,
    ) -> Self {
        let (diagnostic_publisher, diagnostics_receiver) = capture_plugin(
            crate::identifiers::PluginInstanceId::try_from(String::from("fixture"))
                .expect("the fixture id is valid"),
        );
        let diagnostic_capture = tokio::spawn(collect_diagnostics(diagnostics_receiver));
        let bells = bells(&launch, interface);
        let process = PluginLaunch {
            program_directory: launch.program_directory,
            command: launch.command,
            working_directory: launch.working_directory,
            config: launch.config,
            extra_args: None,
            env: None,
            bells,
        };
        let controlled = ControlledPluginLaunch::new(process, interface, launcher);
        let retry = PluginRetryBackoff::new(RETRY_DELAY, RETRY_DELAY);
        let owner = PluginInstanceOwner::launch_controlled(controlled, retry, diagnostic_publisher);
        Self {
            server,
            owner,
            diagnostic_capture,
        }
    }

    async fn finish(mut self, operation: io::Result<()>) -> io::Result<()> {
        let process_cleanup = if operation.is_ok() {
            Ok(())
        } else {
            force_stop(&mut self.owner).await
        };
        let operation = combine(operation, process_cleanup, "Plugin process cleanup failed");
        drop(self.owner);
        let diagnostics = self.diagnostic_capture.await.map_err(|source| {
            io::Error::other(format!("Plugin diagnostics task failed: {source}"))
        })?;
        let operation = attach_diagnostics(operation, &diagnostics);
        let server_shutdown = self.server.shutdown().await.map_err(io::Error::other);
        combine(
            operation,
            server_shutdown,
            "Plugin Control server shutdown failed",
        )
    }
}

async fn collect_diagnostics(
    mut records: tokio::sync::mpsc::Receiver<PipelineDiagnosticRecord>,
) -> String {
    let mut output = String::new();
    while let Some(record) = records.recv().await {
        let Some(pipeline_diagnostic_record::Record::Plugin(plugin)) = record.record else {
            continue;
        };
        let stream = if plugin.stream == PluginDiagnosticStream::Stderr as i32 {
            "stderr"
        } else {
            "stdout"
        };
        output.push_str(stream);
        output.push_str(": ");
        output.push_str(&plugin.text);
        output.push('\n');
    }
    output
}

fn attach_diagnostics(result: io::Result<()>, diagnostics: &str) -> io::Result<()> {
    match result {
        Err(source) if !diagnostics.is_empty() => Err(io::Error::other(format!(
            "{source}\nPlugin process diagnostics:\n{diagnostics}"
        ))),
        result => result,
    }
}

async fn drive_program<Ready, SourceQuiesced>(
    owner: &mut PluginInstanceOwner,
    interface: PluginInterface,
    ready: Ready,
    source_quiesced: Option<SourceQuiesced>,
) -> io::Result<()>
where
    Ready: FnOnce() -> io::Result<()>,
    SourceQuiesced: FnOnce() -> io::Result<()>,
{
    wait_until_ready(owner).await?;
    ready()?;

    if matches!(
        interface,
        PluginInterface::Source | PluginInterface::SourceAndSink
    ) {
        owner.begin_source_quiesce().map_err(io::Error::other)?;
        timeout(
            TEST_DEADLINE,
            poll_fn(|context| owner.poll_source_quiesced(context)),
        )
        .await
        .map_err(|_| {
            io::Error::other("Plugin Program did not quiesce Source before the test deadline")
        })?
        .map_err(io::Error::other)?;
        if let Some(source_quiesced) = source_quiesced {
            source_quiesced()?;
        }
    }

    owner.request_planned_stop().map_err(io::Error::other)?;
    timeout(TEST_DEADLINE, owner.reap_after_stop())
        .await
        .map_err(|_| io::Error::other("Plugin Program did not stop before the test deadline"))?
        .map_err(io::Error::other)
}

async fn wait_until_ready(owner: &mut PluginInstanceOwner) -> io::Result<()> {
    let event = timeout(TEST_DEADLINE, poll_fn(|context| owner.poll_event(context)))
        .await
        .map_err(|_| {
            io::Error::other("Plugin Program did not become ready before the test deadline")
        })?
        .map_err(io::Error::other)?;
    if !matches!(
        event,
        PluginStateEvent::StatusChanged | PluginStateEvent::ProcessFailed
    ) {
        return Err(io::Error::other(
            "Plugin Program requested a retry before becoming ready",
        ));
    }
    let (status, failure) = owner.status();
    if status == PluginStatusState::Running {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "Plugin Program startup failed: {failure:?}"
        )))
    }
}

async fn wait_until_runtime_failure(owner: &mut PluginInstanceOwner) -> io::Result<()> {
    let event = timeout(TEST_DEADLINE, poll_fn(|context| owner.poll_event(context)))
        .await
        .map_err(|_| {
            io::Error::other("Plugin Program did not report runtime failure before the deadline")
        })?
        .map_err(io::Error::other)?;
    if !matches!(event, PluginStateEvent::ProcessFailed) {
        return Err(io::Error::other(
            "Plugin Program requested a retry without publishing runtime failure",
        ));
    }
    let (status, failure) = owner.status();
    if status == PluginStatusState::RestartBackoff && failure.is_some() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "Plugin Program did not enter failed restart backoff: {status:?}, {failure:?}"
        )))
    }
}

async fn force_stop(owner: &mut PluginInstanceOwner) -> io::Result<()> {
    let request = owner.request_force_stop().map_err(io::Error::other);
    let reap = timeout(TEST_DEADLINE, owner.reap_after_stop())
        .await
        .map_err(|_| io::Error::other("Plugin Program force-stop exceeded the test deadline"))?
        .map_err(io::Error::other);
    combine(request, reap, "Plugin Program reap after force-stop failed")
}

fn combine(
    primary: io::Result<()>,
    secondary: io::Result<()>,
    secondary_context: &str,
) -> io::Result<()> {
    match (primary, secondary) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(secondary)) => Err(secondary),
        (Err(primary), Err(secondary)) => Err(io::Error::other(format!(
            "{primary}; {secondary_context}: {secondary}"
        ))),
    }
}
