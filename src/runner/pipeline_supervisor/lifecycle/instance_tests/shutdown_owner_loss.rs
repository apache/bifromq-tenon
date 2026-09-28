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

//! Isolates owner loss from Runner timeouts and from process-owner Drop cleanup.

use super::*;
use crate::runner::pipeline::test_support::wait_for_attachment;

use crate::runner::pipeline::{
    RunnerPipelineControl, RunnerPipelineDiagnostics, RunnerPipelineLaunchRegistry,
};
use crate::runner::process_tree::PipelineProcessTree;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use rustix::process::{Pid, Signal, kill_process};
use std::os::unix::process::ExitStatusExt as _;
use std::process::Stdio;
use tenon_ipc::queue::{QueueWaiter, queue_waiter_is_armed};
use tokio_stream::wrappers::UnixListenerStream;

#[tokio::test(flavor = "current_thread")]
async fn owner_loss_kills_a_pipeline_waiting_for_completion_or_final_shutdown() -> TestResult {
    for phase in [WaitingPhase::Completion, WaitingPhase::FinalShutdown] {
        for loss in [OwnerLoss::ControlStream, OwnerLoss::LifetimePipe] {
            owner_loss_case(phase, loss).await?;
        }
    }
    Ok(())
}

async fn owner_loss_case(phase: WaitingPhase, loss: OwnerLoss) -> TestResult {
    let parent = tempfile::tempdir_in("/tmp")?;
    let root = parent.path().join("pipeline");
    let config = crate::runner::test_support::load_config(parent.path())?;
    let programs = parent.path().join("programs");
    std::fs::create_dir(&programs)?;
    let fixture = TargetFixture::new(&programs, &config)?;
    let value = document(
        "owner-loss",
        "A",
        match phase {
            WaitingPhase::Completion => "normal",
            WaitingPhase::FinalShutdown => "delay-shutdown",
        },
    );
    let target = fixture.resolve(&value)?.ok_or("target is unready")?;
    let registry = RunnerPipelineLaunchRegistry::try_new()?;
    let mut pending = registry.register(
        TenonDocumentId::try_from("owner-loss")?,
        target.bootstrap(&config, &root),
    );
    let socket = parent.path().join("control.sock");
    std::fs::create_dir(crate::plugin_control_directory(
        &socket,
        pending.launch_id(),
    ))?;
    let listener = tokio::net::UnixListener::bind(&socket)?;
    let control = RunnerPipelineControl::new(registry.clone());
    let diagnostics = RunnerPipelineDiagnostics::new(registry, RunnerDiagnostics::new());
    let mut server = tokio::task::JoinSet::new();
    server.spawn(async move {
        tonic::transport::Server::builder()
            .add_service(control.into_service())
            .add_service(diagnostics.into_service())
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await
    });
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "pipeline::main_loop::tests::instance_flow_pipeline_child",
            "--ignored",
            "--nocapture",
        ])
        .env("TENON_TEST_PIPELINE_RECORDS", parent.path())
        .env("TENON_TEST_PIPELINE_CONTROL_SOCKET", &socket)
        .env(
            "TENON_TEST_PIPELINE_LAUNCH_ID",
            STANDARD.encode(pending.launch_id()),
        )
        .stdin(Stdio::piped());
    command.process_group(0).kill_on_drop(true);
    let mut child = command.spawn()?;
    let lifetime = child.stdin.take().ok_or("child lifetime pipe is missing")?;
    let pid = Pid::from_raw(i32::try_from(child.id().ok_or("child pid is missing")?)?)
        .ok_or("child pid is invalid")?;
    let mut tree = PipelineProcessTree::from_spawned_child(child)?;
    let mut checking = Box::pin(async {
        let mut session = wait_for_attachment(&mut pending).await?;
        loop {
            let message = session.receive_pipeline_message().await?;
            if let Some(crate::contracts::core::pipeline_to_runner::Message::StatusSnapshot(status)) =
                message.message
                && status
                    .plugin_instances
                    .iter()
                    .all(|instance| instance.state() == PluginInstanceState::Running)
            {
                break;
            }
        }
        let pids = instance_pids(&root)?;
        if matches!(phase, WaitingPhase::Completion) {
            traffic(&root, 1).await?;
        }
        // Signal only the leader; no Runner shutdown timer or group signal exists.
        kill_process(pid, Signal::TERM)?;
        match phase {
            WaitingPhase::Completion => {
                let input = instance_directory(&root, "input")?;
                wait_for_file(input.join("source-quiesced.received")).await?;
                // The Channel owns the writer of a Completion Queue, so the loop
                // that arms this Queue's writer slot is the Flow's Channel loop,
                // and its own region is the Flow's Channel region.
                let completion = input.join("source/completion-0.queue");
                let channel_bells = flow_channel_bell_path(&root, "forward");
                while !queue_waiter_is_armed(&completion, &channel_bells, QueueWaiter::Writer)
                    .map_err(io::Error::other)?
                {
                    tokio::task::yield_now().await;
                }
                assert!(!input.join("shutdown.received").exists());
            }
            WaitingPhase::FinalShutdown => {
                let sink = instance_directory(&root, "archive")?;
                wait_for_file(sink.join("shutdown.received")).await?;
                assert!(!sink.join("allow-shutdown").exists());
            }
        }
        match loss {
            OwnerLoss::ControlStream => {
                drop(session);
            }
            OwnerLoss::LifetimePipe => {
                drop(lifetime);
            }
        }
        // Verify descendant death before tree.wait can issue its own group cleanup.
        // In FinalShutdown the gated child ignores EOF until its gate opens, so
        // a leader-only abort cannot satisfy this boundary.
        tokio::time::timeout(Duration::from_secs(2), wait_for_processes_gone(&pids)).await??;
        let status = tokio::time::timeout(Duration::from_secs(2), tree.wait()).await??;
        // On macOS the existing abort fallback can win after successful group
        // SIGKILL. Both statuses are terminal; neither replaces the PID proof above.
        assert!(
            matches!(status.signal(), Some(libc::SIGKILL | libc::SIGABRT)),
            "{phase:?}, {loss:?}: {status}"
        );
        assert_reaped(pid)?;
        Ok::<_, Box<dyn Error>>(())
    });
    let outcome = tokio::time::timeout(
        TEST_LIMIT,
        poll_fn(|context| {
            match catch_unwind(AssertUnwindSafe(|| checking.as_mut().poll(context))) {
                Ok(result) => result.map(Ok),
                Err(panic) => Poll::Ready(Err(panic)),
            }
        }),
    )
    .await;
    drop(checking);
    if tree.process_id().is_some() {
        tree.force_kill_and_reap().await?;
    }
    server.shutdown().await;
    match outcome? {
        Ok(result) => result,
        Err(panic) => resume_unwind(panic),
    }
}

#[derive(Debug, Clone, Copy)]
enum WaitingPhase {
    Completion,
    FinalShutdown,
}

#[derive(Debug, Clone, Copy)]
enum OwnerLoss {
    ControlStream,
    LifetimePipe,
}
