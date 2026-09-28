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

use std::error::Error;
use std::future::Future as _;
use std::future::poll_fn;
use std::io;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use rustix::process::{WaitId, WaitIdOptions, WaitOptions, waitid, waitpid};
use serde_json::json;
use tempfile::TempDir;
use tokio::time::timeout;

use super::super::control::TestPluginControlServer as PluginControlServer;
use super::super::lifecycle::{
    PluginInstanceOwner, PluginLifecycleError, PluginSpawnOutcome, PluginStateEvent,
    PluginStatusState, spawn_plugin,
};
use super::super::test_support::{
    ControlledTestLaunch, TEST_DEADLINE, assert_reaped, recorded_pid, retry_backoff, wait_for_file,
};
use crate::contracts::core::{PluginDiagnosticStream, pipeline_diagnostic_record};
use crate::payload_contract::PluginInterface;
use crate::pipeline::diagnostics::{
    PluginDiagnosticPublisher, test_support as diagnostic_test_support,
};

#[tokio::test(flavor = "current_thread")]
async fn source_and_sink_uses_exact_arguments_and_staged_shutdown() -> Result<(), Box<dyn Error>> {
    let root = TempDir::new()?;
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let launch = ControlledTestLaunch::new(
        root.path(),
        "gateway",
        PluginInterface::SourceAndSink,
        json!({"behavior": "normal"}),
    )?;
    let id = crate::identifiers::PluginInstanceId::try_from("gateway")?;
    let (diagnostics, mut records) =
        diagnostic_test_support::interested_instances([id.clone()], 16);
    let mut owner = PluginInstanceOwner::launch_controlled(
        launch.borrowed(&launcher),
        retry_backoff(),
        diagnostics.instance_plugin(id),
    );

    assert!(matches!(
        next_event(&mut owner).await?,
        PluginStateEvent::StatusChanged
    ));
    assert_eq!(owner.status().0, PluginStatusState::Running);
    assert_eq!(
        std::fs::read_to_string(launch.working_directory().join("configs.received"))?,
        "{\"behavior\":\"normal\"}\n"
    );
    assert_eq!(
        std::fs::read_to_string(launch.working_directory().join("program.received"))?,
        root.path().canonicalize()?.display().to_string()
    );
    assert_diagnostics_from_first_byte(&mut records).await?;

    owner.begin_source_quiesce()?;
    timeout(
        TEST_DEADLINE,
        poll_fn(|context| owner.poll_source_quiesced(context)),
    )
    .await??;
    let process_id = recorded_pid(launch.working_directory())?;
    assert!(waitpid(Some(process_id), WaitOptions::NOHANG)?.is_none());
    assert_eq!(
        std::fs::read_to_string(launch.working_directory().join("lifecycle.received"))?,
        "attach\nready\nquiesce-source\nsource-quiesced\n"
    );

    owner.request_planned_stop()?;
    timeout(TEST_DEADLINE, owner.reap_after_stop()).await??;
    assert_reaped(process_id)?;
    assert_eq!(
        std::fs::read_to_string(launch.working_directory().join("lifecycle.received"))?,
        "attach\nready\nquiesce-source\nsource-quiesced\nshutdown\nexit\n"
    );
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stream_loss_after_ready_reaps_before_one_retry_sequence() -> Result<(), Box<dyn Error>> {
    let root = TempDir::new()?;
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let launch = ControlledTestLaunch::new(
        root.path(),
        "source",
        PluginInterface::Source,
        json!({"behavior": "stream-loss-after-ready"}),
    )?;
    let mut owner = PluginInstanceOwner::launch_controlled(
        launch.borrowed(&launcher),
        retry_backoff(),
        diagnostics(),
    );

    assert!(matches!(
        next_event(&mut owner).await?,
        PluginStateEvent::StatusChanged
    ));
    let first_process = recorded_pid(launch.working_directory())?;
    assert!(matches!(
        next_event(&mut owner).await?,
        PluginStateEvent::ProcessFailed
    ));
    assert_eq!(owner.status().0, PluginStatusState::RestartBackoff);
    assert_eq!(
        owner.status().1.as_ref().map(|error| error.code.as_str()),
        Some("plugin.control_stream_failed")
    );
    assert_reaped(first_process)?;
    assert!(matches!(
        next_event(&mut owner).await?,
        PluginStateEvent::RestartDue
    ));

    owner.restart_controlled(launch.borrowed(&launcher), diagnostics(), || {})?;
    assert!(matches!(
        next_event(&mut owner).await?,
        PluginStateEvent::StatusChanged
    ));
    let second_process = recorded_pid(launch.working_directory())?;
    assert_ne!(first_process, second_process);
    assert!(matches!(
        next_event(&mut owner).await?,
        PluginStateEvent::ProcessFailed
    ));
    assert_reaped(second_process)?;
    let launch_ids =
        std::fs::read_to_string(launch.working_directory().join("launch-ids.received"))?;
    let launch_ids = launch_ids.lines().collect::<Vec<_>>();
    assert_eq!(launch_ids.len(), 2);
    assert_ne!(launch_ids[0], launch_ids[1]);

    owner.request_force_stop()?;
    owner.reap_after_stop().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn force_stop_cancels_attached_startup_and_reaps_the_child() -> Result<(), Box<dyn Error>> {
    let root = TempDir::new()?;
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let launch = ControlledTestLaunch::new(
        root.path(),
        "delayed",
        PluginInterface::Sink,
        json!({"behavior": "delay-ready"}),
    )?;
    let mut owner = PluginInstanceOwner::launch_controlled(
        launch.borrowed(&launcher),
        retry_backoff(),
        diagnostics(),
    );
    let attached_path = launch.working_directory().join("attach.received");
    timeout(TEST_DEADLINE, async {
        tokio::select! {
            _result = poll_fn(|context| owner.poll_event(context)) => {
                Err(io::Error::other("Delayed Plugin completed startup unexpectedly"))
            }
            result = wait_for_file(&attached_path) => result,
        }
    })
    .await??;
    let process_id = recorded_pid(launch.working_directory())?;

    owner.request_force_stop()?;
    timeout(TEST_DEADLINE, owner.reap_after_stop()).await??;
    assert_reaped(process_id)?;
    assert_eq!(
        std::fs::read_to_string(launch.working_directory().join("lifecycle.received"))?,
        "attach\n"
    );
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_hung_shutdown_remains_force_recoverable() -> Result<(), Box<dyn Error>> {
    let root = TempDir::new()?;
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let launch = ControlledTestLaunch::new(
        root.path(),
        "sink",
        PluginInterface::Sink,
        json!({"behavior": "delay-shutdown"}),
    )?;
    let mut owner = PluginInstanceOwner::launch_controlled(
        launch.borrowed(&launcher),
        retry_backoff(),
        diagnostics(),
    );
    let _ready = next_event(&mut owner).await?;
    let process_id = recorded_pid(launch.working_directory())?;
    owner.request_planned_stop()?;

    {
        let mut pending_shutdown = pin!(owner.reap_after_stop());
        let mut context = Context::from_waker(Waker::noop());
        assert!(pending_shutdown.as_mut().poll(&mut context).is_pending());
    }
    wait_for_file(&launch.working_directory().join("shutdown.received")).await?;
    assert!(
        std::fs::read_to_string(launch.working_directory().join("lifecycle.received"))?
            .contains("shutdown\n")
    );
    owner.request_force_stop()?;
    timeout(TEST_DEADLINE, owner.reap_after_stop()).await??;
    assert_reaped(process_id)?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn child_exit_before_ready_is_a_non_retrying_start_failure() -> Result<(), Box<dyn Error>> {
    let root = TempDir::new()?;
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let launch = ControlledTestLaunch::new(
        root.path(),
        "source",
        PluginInterface::Source,
        json!({"behavior": "exit-before-ready"}),
    )?;
    let mut owner = PluginInstanceOwner::launch_controlled(
        launch.borrowed(&launcher),
        retry_backoff(),
        diagnostics(),
    );

    assert!(matches!(
        next_event(&mut owner).await?,
        PluginStateEvent::ProcessFailed
    ));
    assert_eq!(owner.status().0, PluginStatusState::StartFailed);
    // Process exit and the resulting control-stream closure can be observed in either order.
    assert!(matches!(
        owner.status().1.as_ref().map(|error| error.code.as_str()),
        Some("plugin.exited_before_ready" | "plugin.control_ready_failed")
    ));
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(owner.poll_event(&mut context), Poll::Pending));
    assert_reaped(recorded_pid(launch.working_directory())?)?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn child_exit_during_source_quiesce_is_reaped_and_reported() -> Result<(), Box<dyn Error>> {
    let root = TempDir::new()?;
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let launch = ControlledTestLaunch::new(
        root.path(),
        "source",
        PluginInterface::Source,
        json!({"behavior": "exit-during-quiesce"}),
    )?;
    let mut owner = PluginInstanceOwner::launch_controlled(
        launch.borrowed(&launcher),
        retry_backoff(),
        diagnostics(),
    );
    let _ready = next_event(&mut owner).await?;
    assert_eq!(owner.status().0, PluginStatusState::Running);
    let process_id = recorded_pid(launch.working_directory())?;

    owner.begin_source_quiesce()?;
    // Make exit observable before polling quiesce, without stealing the owner's reap.
    // Otherwise the equally valid control-stream failure can be observed first.
    timeout(TEST_DEADLINE, async {
        while waitid(
            WaitId::Pid(process_id),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        )?
        .is_none()
        {
            tokio::task::yield_now().await;
        }
        Ok::<(), io::Error>(())
    })
    .await??;
    let failure = timeout(
        TEST_DEADLINE,
        poll_fn(|context| owner.poll_source_quiesced(context)),
    )
    .await?;
    assert!(matches!(
        failure,
        Err(PluginLifecycleError::ExitedDuringShutdown(status)) if status.code() == Some(33)
    ));
    assert_reaped(process_id)?;
    owner.request_force_stop()?;
    owner.reap_after_stop().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stream_loss_during_source_quiesce_is_reported_and_reaped() -> Result<(), Box<dyn Error>> {
    let root = TempDir::new()?;
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let launch = ControlledTestLaunch::new(
        root.path(),
        "source",
        PluginInterface::Source,
        json!({"behavior": "stream-loss-during-quiesce"}),
    )?;
    let mut owner = PluginInstanceOwner::launch_controlled(
        launch.borrowed(&launcher),
        retry_backoff(),
        diagnostics(),
    );
    let _ready = next_event(&mut owner).await?;
    assert_eq!(owner.status().0, PluginStatusState::Running);
    let process_id = recorded_pid(launch.working_directory())?;

    owner.begin_source_quiesce()?;
    let failure = timeout(
        TEST_DEADLINE,
        poll_fn(|context| owner.poll_source_quiesced(context)),
    )
    .await?;
    assert!(matches!(failure, Err(PluginLifecycleError::Control(_))));
    owner.request_force_stop()?;
    timeout(TEST_DEADLINE, owner.reap_after_stop()).await??;
    assert_reaped(process_id)?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn illegal_message_after_shutdown_is_reported_after_child_reap() -> Result<(), Box<dyn Error>>
{
    let root = TempDir::new()?;
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let launch = ControlledTestLaunch::new(
        root.path(),
        "sink",
        PluginInterface::Sink,
        json!({"behavior": "message-after-shutdown"}),
    )?;
    let mut owner = PluginInstanceOwner::launch_controlled(
        launch.borrowed(&launcher),
        retry_backoff(),
        diagnostics(),
    );
    let _ready = next_event(&mut owner).await?;
    let process_id = recorded_pid(launch.working_directory())?;
    owner.request_planned_stop()?;

    let failure = timeout(TEST_DEADLINE, owner.reap_after_stop()).await?;
    assert!(matches!(failure, Err(PluginLifecycleError::Control(_))));
    assert_reaped(process_id)?;
    owner.request_force_stop()?;
    owner.reap_after_stop().await?;
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stdin_eof_makes_the_real_plugin_exit_without_shutdown() -> Result<(), Box<dyn Error>> {
    let root = TempDir::new()?;
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let test_launch = ControlledTestLaunch::new(
        root.path(),
        "sink",
        PluginInterface::Sink,
        json!({"behavior": "normal"}),
    )?;
    let pending = launcher.register(PluginInterface::Sink);
    let launch_id = *pending.launch_id();
    let super::ControlledPluginLaunch {
        process: process_launch,
        ..
    } = test_launch.borrowed(&launcher);
    let PluginSpawnOutcome::Started {
        mut process,
        mut config,
    } = spawn_plugin(
        process_launch,
        launcher.socket_path(),
        &launch_id,
        diagnostics(),
        launcher.register_metrics(&launch_id, vec![]),
    )
    else {
        return Err(io::Error::other("Controlled child did not spawn").into());
    };
    timeout(
        TEST_DEADLINE,
        poll_fn(|context| process.poll_config(&mut config, context)),
    )
    .await??;
    let attached = timeout(TEST_DEADLINE, pending.attach()).await??;
    let _control = timeout(TEST_DEADLINE, attached.wait_for_ready()).await??;
    let process_id = recorded_pid(test_launch.working_directory())?;

    let mut child = process.into_child();
    let status = timeout(TEST_DEADLINE, child.wait()).await??;
    assert_eq!(status.code(), Some(41));
    assert_reaped(process_id)?;
    assert_eq!(
        std::fs::read_to_string(test_launch.working_directory().join("lifecycle.received"))?,
        "attach\nready\n"
    );
    server.shutdown().await?;
    Ok(())
}

async fn next_event(owner: &mut PluginInstanceOwner) -> Result<PluginStateEvent, Box<dyn Error>> {
    timeout(TEST_DEADLINE, poll_fn(|context| owner.poll_event(context)))
        .await?
        .map_err(Into::into)
}

async fn assert_diagnostics_from_first_byte(
    records: &mut tokio::sync::mpsc::Receiver<crate::contracts::core::PipelineDiagnosticRecord>,
) -> Result<(), Box<dyn Error>> {
    let mut stdout_before = false;
    let mut stdout_after = false;
    let mut stderr_before = false;
    let mut stderr_after = false;
    timeout(TEST_DEADLINE, async {
        while !(stdout_before && stdout_after && stderr_before && stderr_after) {
            let record = records
                .recv()
                .await
                .ok_or_else(|| io::Error::other("Plugin diagnostic stream ended"))?;
            let Some(pipeline_diagnostic_record::Record::Plugin(record)) = record.record else {
                continue;
            };
            match (record.stream, record.text.as_str()) {
                (stream, "diagnostic-before-ready")
                    if stream == PluginDiagnosticStream::Stdout as i32 =>
                {
                    stdout_before = true;
                }
                (stream, "diagnostic-after-ready")
                    if stream == PluginDiagnosticStream::Stdout as i32 =>
                {
                    stdout_after = true;
                }
                (stream, "diagnostic-stderr-before-ready")
                    if stream == PluginDiagnosticStream::Stderr as i32 =>
                {
                    stderr_before = true;
                }
                (stream, "diagnostic-stderr-after-ready")
                    if stream == PluginDiagnosticStream::Stderr as i32 =>
                {
                    stderr_after = true;
                }
                _ => {}
            }
        }
        Ok::<(), io::Error>(())
    })
    .await??;
    Ok(())
}

fn diagnostics() -> PluginDiagnosticPublisher {
    diagnostic_test_support::publisher().instance_plugin(diagnostic_test_support::plugin_id())
}

#[tokio::test(flavor = "current_thread")]
async fn handoff_observation_does_not_hide_starting_or_ready_process_failure()
-> Result<(), Box<dyn Error>> {
    for behavior in ["normal", "exit-before-ready"] {
        let root = TempDir::new()?;
        let server = PluginControlServer::start()?;
        let launcher = server.launcher();
        let launch = ControlledTestLaunch::new(
            root.path(),
            "source",
            PluginInterface::Source,
            json!({"behavior": behavior}),
        )?;
        let mut owner = PluginInstanceOwner::launch_controlled(
            launch.borrowed(&launcher),
            retry_backoff(),
            diagnostics(),
        );
        if behavior == "normal" {
            let _ready = next_event(&mut owner).await?;
            rustix::process::kill_process(
                recorded_pid(launch.working_directory())?,
                rustix::process::Signal::KILL,
            )?;
        }
        timeout(
            TEST_DEADLINE,
            poll_fn(|context| match owner.handoff_readiness(context) {
                Ok(super::HandoffReadiness::FailureObserved) => Poll::Ready(Ok(())),
                Ok(_) => Poll::Pending,
                Err(source) => Poll::Ready(Err(source)),
            }),
        )
        .await??;
        let pid = recorded_pid(launch.working_directory())?;
        owner.request_force_stop()?;
        timeout(TEST_DEADLINE, owner.reap_after_stop()).await??;
        assert_reaped(pid)?;
        server.shutdown().await?;
    }
    Ok(())
}
