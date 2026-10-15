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

use super::super::PipelineCleanupError;
use super::super::test_support::TargetReferenceProbe;
use super::*;
use crate::plugin_control_directory;

#[tokio::test(flavor = "current_thread")]
async fn start_registers_before_spawn_and_returns_one_process_owner() -> io::Result<()> {
    let endpoints = endpoints()?;
    let fixture = LaunchFixture::new()?;
    let server = TestServer::start(endpoints.clone()).await?;
    let program = CapturedChildProgram::blocking()?;
    let first_reference = TargetReferenceProbe::new(&fixture.target);
    let bootstrap = fixture
        .target
        .bootstrap(&fixture.config, fixture.directory.path());

    let mut process = launcher_test_support::start_pipeline_with_spawn(
        &server.launcher,
        &program.executable,
        Arc::clone(&fixture.target),
        &fixture.config,
        fixture.directory.path(),
        |command, launch_id| {
            assert!(
                launch_registry_test_support::contains(&endpoints.launches, launch_id),
                "Pipeline launch must be registered before spawn"
            );
            command.spawn()
        },
    )
    .map_err(io::Error::other)?;
    let attach = async {
        let arguments = program.captured_arguments().await?;
        let launch_id = captured_launch_id(&arguments, &server.socket_path)?;
        server.open(launch_id).await.map_err(io::Error::other)
    };
    let (attached, peer) = tokio::join!(process.wait_for_attachment(), attach);
    attached.map_err(io::Error::other)?;
    let (pipeline, mut response) = peer?;
    let first = response
        .message()
        .await
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("Attached child did not receive Bootstrap"))?;
    assert_eq!(
        first,
        RunnerToPipeline {
            message: Some(runner_to_pipeline::Message::Bootstrap(bootstrap)),
        }
    );

    let mut cancelled_startup = Box::pin(process.wait_for_startup_status());
    tokio::select! {
        result = &mut cancelled_startup => {
            return Err(io::Error::other(format!(
                "Empty startup-status wait completed unexpectedly: {result:?}"
            )));
        }
        () = tokio::task::yield_now() => {}
    }
    drop(cancelled_startup);

    let startup_status = PipelineStatusSnapshot {
        document_etag: status_etag(),
        plugin_instances: vec![
            PluginInstanceStatus {
                id: String::from("source"),
                state: PluginInstanceState::Starting as i32,
                last_error: None,
            },
            PluginInstanceStatus {
                id: String::from("primary"),
                state: PluginInstanceState::Starting as i32,
                last_error: None,
            },
        ],
    };
    pipeline
        .send(PipelineToRunner {
            message: Some(pipeline_to_runner::Message::StatusSnapshot(startup_status)),
        })
        .await
        .map_err(io::Error::other)?;
    let startup_status = process
        .wait_for_startup_status()
        .await
        .map_err(io::Error::other)?;
    assert_eq!(startup_status.document_etag().strong_value(), status_etag());

    let mut cancelled = Box::pin(process.next_event());
    tokio::select! {
        result = &mut cancelled => {
            return Err(io::Error::other(format!(
                "Empty process event completed unexpectedly: {result:?}"
            )));
        }
        () = tokio::task::yield_now() => {}
    }
    drop(cancelled);

    let status = PipelineToRunner {
        message: Some(pipeline_to_runner::Message::StatusSnapshot(
            PipelineStatusSnapshot {
                document_etag: status_etag(),
                plugin_instances: ["source", "primary"]
                    .into_iter()
                    .map(|id| PluginInstanceStatus {
                        id: id.to_owned(),
                        state: PluginInstanceState::Running as i32,
                        last_error: None,
                    })
                    .collect(),
            },
        )),
    };
    pipeline
        .send(status.clone())
        .await
        .map_err(io::Error::other)?;
    let event = process.next_event().await.map_err(io::Error::other)?;
    assert!(matches!(
        event,
        RunnerPipelineProcessEvent::Status(state) if state.document_etag().strong_value() == status_etag()
    ));

    let next_target = Arc::new(target(
        fixture.directory.path(),
        &fixture.config,
        "pipeline-a",
        "function main(event) emit() emit() end",
    )?);
    let next_reference = TargetReferenceProbe::new(&next_target);
    let revision = next_target.revision();
    let next_status = PipelineStatusSnapshot {
        document_etag: revision.document_etag.clone(),
        plugin_instances: startup_status.snapshot().plugin_instances.clone(),
    };
    process
        .publish_target(next_target)
        .map_err(io::Error::other)?;
    let second = response
        .message()
        .await
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("Attached child did not receive Revision Plan"))?;
    assert_eq!(
        second,
        RunnerToPipeline {
            message: Some(runner_to_pipeline::Message::RevisionPlan(revision)),
        }
    );

    drop(fixture.target);
    first_reference.assert_retained()?;
    pipeline
        .send(PipelineToRunner {
            message: Some(pipeline_to_runner::Message::StatusSnapshot(
                next_status.clone(),
            )),
        })
        .await
        .map_err(io::Error::other)?;
    let RunnerPipelineProcessEvent::Status(applied) =
        process.next_event().await.map_err(io::Error::other)?
    else {
        return Err(io::Error::other("Live child unexpectedly exited"));
    };
    assert_eq!(applied.snapshot(), &next_status);
    first_reference.assert_released()?;
    next_reference.assert_retained()?;

    process
        .force_kill_and_reap()
        .await
        .map_err(io::Error::other)?;
    let arguments = program.captured_arguments().await?;
    let launch_id = captured_launch_id(&arguments, &server.socket_path)?;
    let control_directory =
        plugin_control_directory(Path::new(server.socket_path.as_ref()), &launch_id);
    assert!(control_directory.is_dir());
    process
        .cleanup_control_directory()
        .await
        .map_err(io::Error::other)?;
    assert!(!control_directory.exists());
    drop(process);
    next_reference.assert_released()?;
    drop(pipeline);
    drop(response);
    server.finish().await
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_attachment_wait_retains_child_ownership_for_explicit_cleanup() -> io::Result<()>
{
    let endpoints = endpoints()?;
    let fixture = LaunchFixture::new()?;
    let server = TestServer::start(endpoints.clone()).await?;
    let program = CapturedChildProgram::blocking()?;
    let mut process = server
        .launcher
        .start_pipeline(
            &program.executable,
            Arc::clone(&fixture.target),
            &fixture.config,
            fixture.directory.path(),
        )
        .map_err(io::Error::other)?;
    let arguments = program.captured_arguments().await?;
    let launch_id = captured_launch_id(&arguments, &server.socket_path)?;
    let process_id = program.captured_process_id().await?;

    let mut attachment = Box::pin(process.wait_for_attachment());
    tokio::select! {
        result = &mut attachment => {
            return Err(io::Error::other(format!(
                "Unclaimed Pipeline attachment completed unexpectedly: {result:?}"
            )));
        }
        () = tokio::task::yield_now() => {}
    }
    drop(attachment);

    assert!(process_exists(process_id)?, "Cancelled wait lost the child");
    assert!(
        launch_registry_test_support::contains(&endpoints.launches, &launch_id),
        "Cancelled wait lost the pending registration"
    );
    process
        .force_kill_and_reap()
        .await
        .map_err(io::Error::other)?;
    assert!(!process_exists(process_id)?, "Cleaned child still exists");
    let Err(rejected) = server.attach(launch_id).await else {
        return Err(io::Error::other("Cleaned launch still accepted Attach"));
    };
    assert_eq!(rejected.code(), Code::FailedPrecondition);

    server.finish().await
}

#[tokio::test(flavor = "current_thread")]
async fn child_exit_before_attach_removes_its_pending_launch() -> io::Result<()> {
    let endpoints = endpoints()?;
    let fixture = LaunchFixture::new()?;
    let server = TestServer::start(endpoints.clone()).await?;
    let program = CapturedChildProgram::exiting(23)?;

    let mut process = server
        .launcher
        .start_pipeline(
            &program.executable,
            Arc::clone(&fixture.target),
            &fixture.config,
            fixture.directory.path(),
        )
        .map_err(io::Error::other)?;
    let result = process.wait_for_attachment().await;
    assert!(matches!(
        result,
        Err(RunnerPipelineLaunchError::ExitedBeforeAttachmentCompleted(status))
            if status.code() == Some(23)
    ));
    let arguments = program.captured_arguments().await?;
    let launch_id = captured_launch_id(&arguments, &server.socket_path)?;
    let Err(rejected) = server.attach(launch_id).await else {
        return Err(io::Error::other("Exited child retained a pending launch"));
    };
    assert_eq!(rejected.code(), Code::FailedPrecondition);

    server.finish().await
}

#[tokio::test(flavor = "current_thread")]
async fn attached_process_reports_child_exit_without_losing_its_session() -> io::Result<()> {
    let endpoints = endpoints()?;
    let fixture = LaunchFixture::new()?;
    let server = TestServer::start(endpoints.clone()).await?;
    let program = CapturedChildProgram::blocking()?;

    let mut process = server
        .launcher
        .start_pipeline(
            &program.executable,
            Arc::clone(&fixture.target),
            &fixture.config,
            fixture.directory.path(),
        )
        .map_err(io::Error::other)?;
    let attach = async {
        let arguments = program.captured_arguments().await?;
        let launch_id = captured_launch_id(&arguments, &server.socket_path)?;
        server.open(launch_id).await.map_err(io::Error::other)
    };
    let (attached, peer) = tokio::join!(process.wait_for_attachment(), attach);
    attached.map_err(io::Error::other)?;
    let (pipeline, response) = peer?;
    let process_id = program.captured_process_id().await?;
    pipeline
        .send(PipelineToRunner {
            message: Some(pipeline_to_runner::Message::StatusSnapshot(
                PipelineStatusSnapshot::default(),
            )),
        })
        .await
        .map_err(io::Error::other)?;
    let status = std::process::Command::new("kill")
        .args(["-TERM", &process_id.to_string()])
        .status()?;
    assert!(status.success(), "Test child could not be terminated");
    wait_for_process_exit_without_reaping(process_id)?;

    let event = tokio::time::timeout(TEST_TIMEOUT, process.next_event())
        .await
        .map_err(|_| io::Error::other("Pipeline child exit event timed out"))?
        .map_err(io::Error::other)?;
    assert!(
        matches!(
            event,
            RunnerPipelineProcessEvent::Exited(status) if !status.success()
        ),
        "Child exit must invalidate buffered Pipeline status"
    );

    process
        .force_kill_and_reap()
        .await
        .map_err(io::Error::other)?;
    drop(pipeline);
    drop(response);
    server.finish().await
}

#[tokio::test(flavor = "current_thread")]
async fn child_exit_invalidates_a_buffered_first_status() -> io::Result<()> {
    let endpoints = endpoints()?;
    let fixture = LaunchFixture::new()?;
    let server = TestServer::start(endpoints.clone()).await?;
    let program = CapturedChildProgram::blocking()?;
    let mut process = server
        .launcher
        .start_pipeline(
            &program.executable,
            Arc::clone(&fixture.target),
            &fixture.config,
            fixture.directory.path(),
        )
        .map_err(io::Error::other)?;
    let attach = async {
        let arguments = program.captured_arguments().await?;
        let launch_id = captured_launch_id(&arguments, &server.socket_path)?;
        server.open(launch_id).await.map_err(io::Error::other)
    };
    let (attached, peer) = tokio::join!(process.wait_for_attachment(), attach);
    attached.map_err(io::Error::other)?;
    let (pipeline, response) = peer?;
    let process_id = program.captured_process_id().await?;
    pipeline
        .send(PipelineToRunner {
            message: Some(pipeline_to_runner::Message::StatusSnapshot(
                PipelineStatusSnapshot {
                    document_etag: status_etag(),
                    plugin_instances: vec![
                        PluginInstanceStatus {
                            id: String::from("source"),
                            state: PluginInstanceState::Running as i32,
                            last_error: None,
                        },
                        PluginInstanceStatus {
                            id: String::from("primary"),
                            state: PluginInstanceState::Running as i32,
                            last_error: None,
                        },
                    ],
                },
            )),
        })
        .await
        .map_err(io::Error::other)?;
    let status = std::process::Command::new("kill")
        .args(["-TERM", &process_id.to_string()])
        .status()?;
    assert!(status.success(), "Test child could not be terminated");
    wait_for_process_exit_without_reaping(process_id)?;

    let result = tokio::time::timeout(TEST_TIMEOUT, process.wait_for_startup_status())
        .await
        .map_err(|_| io::Error::other("Pipeline startup exit observation timed out"))?;
    assert!(matches!(
        result,
        Err(RunnerPipelineStartupError::Failed(
            RunnerPipelineStartupFailure::ExitedBeforeCompletion(status)
        )) if !status.success()
    ));

    drop(pipeline);
    drop(response);
    server.finish().await
}

#[tokio::test(flavor = "current_thread")]
async fn attachment_deadline_kills_reaps_and_unregisters_the_child() -> io::Result<()> {
    let endpoints = endpoints()?;
    let fixture = LaunchFixture::new()?;
    let server = TestServer::start(endpoints.clone()).await?;
    let program = CapturedChildProgram::blocking()?;

    let mut process = server
        .launcher
        .start_pipeline(
            &program.executable,
            Arc::clone(&fixture.target),
            &fixture.config,
            fixture.directory.path(),
        )
        .map_err(io::Error::other)?;
    let arguments = program.captured_arguments().await?;
    let launch_id = captured_launch_id(&arguments, &server.socket_path)?;
    let process_id = program.captured_process_id().await?;

    let result = process.wait_for_attachment().await;
    assert!(matches!(
        result,
        Err(RunnerPipelineLaunchError::AttachmentTimedOut)
    ));
    let Err(rejected) = server.attach(launch_id).await else {
        return Err(io::Error::other(
            "Timed-out child retained a pending launch",
        ));
    };
    assert_eq!(rejected.code(), Code::FailedPrecondition);
    assert!(
        !process_exists(process_id)?,
        "Timed-out child was not reaped"
    );

    server.finish().await
}

#[tokio::test(flavor = "current_thread")]
async fn startup_timeout_after_attachment_reaps_the_child() -> io::Result<()> {
    let endpoints = endpoints()?;
    let fixture = LaunchFixture::new()?;
    let server = TestServer::start(endpoints.clone()).await?;
    let program = CapturedChildProgram::blocking()?;
    let mut process = server
        .launcher
        .start_pipeline(
            &program.executable,
            Arc::clone(&fixture.target),
            &fixture.config,
            fixture.directory.path(),
        )
        .map_err(io::Error::other)?;
    let attach = async {
        let arguments = program.captured_arguments().await?;
        let launch_id = captured_launch_id(&arguments, &server.socket_path)?;
        server.open(launch_id).await.map_err(io::Error::other)
    };
    let (attached, peer) = tokio::join!(process.wait_for_attachment(), attach);
    launcher_test_support::expire_startup_deadline(&mut process);
    attached.map_err(io::Error::other)?;
    let (pipeline, mut response) = peer?;
    let bootstrap = response
        .message()
        .await
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("Attached child did not receive Bootstrap"))?;
    assert!(matches!(
        bootstrap.message,
        Some(runner_to_pipeline::Message::Bootstrap(_))
    ));
    let process_id = program.captured_process_id().await?;

    let result = tokio::time::timeout(TEST_TIMEOUT, process.wait_for_startup_status())
        .await
        .map_err(|_| io::Error::other("Expired startup deadline did not finish startup"))?;
    assert!(matches!(
        result,
        Err(RunnerPipelineStartupError::Failed(
            RunnerPipelineStartupFailure::TimedOut
        ))
    ));
    assert!(
        !process_exists(process_id)?,
        "Startup-timeout child was not reaped"
    );

    drop(pipeline);
    drop(response);
    server.finish().await
}

#[tokio::test(flavor = "current_thread")]
async fn startup_stream_failure_is_retained_and_the_child_is_reaped() -> io::Result<()> {
    let endpoints = endpoints()?;
    let fixture = LaunchFixture::new()?;
    let server = TestServer::start(endpoints.clone()).await?;
    let program = CapturedChildProgram::blocking()?;
    let mut process = server
        .launcher
        .start_pipeline(
            &program.executable,
            Arc::clone(&fixture.target),
            &fixture.config,
            fixture.directory.path(),
        )
        .map_err(io::Error::other)?;
    let attach = async {
        let arguments = program.captured_arguments().await?;
        let launch_id = captured_launch_id(&arguments, &server.socket_path)?;
        server.open(launch_id).await.map_err(io::Error::other)
    };
    let (attached, peer) = tokio::join!(process.wait_for_attachment(), attach);
    attached.map_err(io::Error::other)?;
    let (pipeline, mut response) = peer?;
    let _bootstrap = response
        .message()
        .await
        .map_err(io::Error::other)?
        .ok_or_else(|| io::Error::other("Attached child did not receive Bootstrap"))?;
    let process_id = program.captured_process_id().await?;
    drop(pipeline);

    let result = process.wait_for_startup_status().await;
    assert!(matches!(
        result,
        Err(RunnerPipelineStartupError::Failed(
            RunnerPipelineStartupFailure::Control(_)
        ))
    ));
    assert!(
        !process_exists(process_id)?,
        "Disconnected-startup child was not reaped"
    );
    let repeated = process.wait_for_startup_status().await;
    assert!(matches!(
        repeated,
        Err(RunnerPipelineStartupError::Failed(
            RunnerPipelineStartupFailure::Control(_)
        ))
    ));

    drop(response);
    server.finish().await
}

#[tokio::test(flavor = "current_thread")]
async fn spawn_failure_removes_the_pending_launch() -> io::Result<()> {
    let endpoints = endpoints()?;
    let fixture = LaunchFixture::new()?;
    let temporary_directory = tempfile::tempdir()?;
    let launcher = RunnerPipelineLauncher::new(
        endpoints.launches.clone(),
        temporary_directory.path().join("control.sock"),
        crate::runner::process_resources::test_support::unavailable(),
    );
    let result = launcher.start_pipeline(
        &temporary_directory.path().join("missing-tenon"),
        Arc::clone(&fixture.target),
        &fixture.config,
        fixture.directory.path(),
    );

    assert!(matches!(
        result,
        Err(RunnerPipelineLaunchError::SpawnFailed(_))
    ));
    assert!(launch_registry_test_support::is_empty(&endpoints.launches));
    assert_eq!(
        std::fs::read_dir(temporary_directory.path())?.count(),
        0,
        "failed spawn must remove its newly created Plugin Control directory"
    );
    Ok(())
}
#[tokio::test(flavor = "current_thread")]
async fn failed_spawn_retains_cleanup_until_the_lifecycle_retries() -> io::Result<()> {
    let endpoints = endpoints()?;
    let fixture = LaunchFixture::new()?;
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("control.sock");
    let launcher = RunnerPipelineLauncher::new(
        endpoints.launches.clone(),
        socket.clone(),
        crate::runner::process_resources::test_support::unavailable(),
    );
    let failure = launcher_test_support::start_pipeline_with_spawn(
        &launcher,
        &directory.path().join("missing-tenon"),
        Arc::clone(&fixture.target),
        &fixture.config,
        fixture.directory.path(),
        |_, launch_id| {
            fs::write(
                plugin_control_directory(&socket, launch_id).join("external-file"),
                b"test",
            )?;
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "spawn denied",
            ))
        },
    )
    .err()
    .ok_or_else(|| io::Error::other("The test spawn function did not return an error"))?;
    let RunnerPipelineLaunchError::LaunchCleanupPending { mut resources, .. } = failure else {
        return Err(io::Error::other("The launch error has no cleanup owner"));
    };
    assert!(launch_registry_test_support::is_empty(&endpoints.launches));
    assert_eq!(fs::read_dir(directory.path())?.count(), 1);
    resources.cleanup().await.map_err(io::Error::other)?;
    assert_eq!(fs::read_dir(directory.path())?.count(), 0);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn directory_cleanup_failure_retains_reaped_process_references_until_retry() -> io::Result<()>
{
    let endpoints = endpoints()?;
    let fixture = LaunchFixture::new()?;
    let server = TestServer::start(endpoints.clone()).await?;
    let program = CapturedChildProgram::blocking()?;
    let reference = TargetReferenceProbe::new(&fixture.target);
    let mut process = server
        .launcher
        .start_pipeline(
            &program.executable,
            Arc::clone(&fixture.target),
            &fixture.config,
            fixture.directory.path(),
        )
        .map_err(io::Error::other)?;
    drop(fixture.target);
    let arguments = program.captured_arguments().await?;
    let launch_id = captured_launch_id(&arguments, &server.socket_path)?;
    let control_directory =
        plugin_control_directory(Path::new(server.socket_path.as_ref()), &launch_id);
    let displaced = fixture.directory.path().join("displaced-control");
    fs::rename(&control_directory, &displaced)?;
    fs::write(&control_directory, b"not a directory")?;
    reference.assert_retained()?;
    process.force_kill_and_reap().await?;
    reference.assert_retained()?;
    let failure = process.cleanup_control_directory().await;
    assert!(
        matches!(failure, Err(PipelineCleanupError::Filesystem { ref path, ref source }) if path == &control_directory && source.kind() == io::ErrorKind::NotADirectory)
    );
    reference.assert_retained()?;
    fs::remove_file(&control_directory)?;
    fs::rename(&displaced, &control_directory)?;
    process
        .cleanup_control_directory()
        .await
        .map_err(io::Error::other)?;
    drop(process);
    reference.assert_released()?;
    assert!(launch_registry_test_support::is_empty(&endpoints.launches));
    assert!(!control_directory.exists());
    assert!(!displaced.exists());
    server.finish().await
}

#[test]
fn startup_and_cleanup_diagnostics_render_each_cause_once() {
    let startup = RunnerPipelineStartupError::Failed(RunnerPipelineStartupFailure::TimedOut);
    assert_eq!(
        ErrorChain(&startup).to_string(),
        "Pipeline startup attempt ended in failure: Pipeline process startup timed out"
    );

    let startup_cleanup = RunnerPipelineStartupError::CleanupFailed {
        failure: RunnerPipelineStartupFailure::TimedOut,
        cleanup: io::Error::other("startup cleanup root"),
    };
    assert_eq!(
        ErrorChain(&startup_cleanup).to_string(),
        "Pipeline startup failed: Pipeline process startup timed out; Pipeline process cleanup also failed: startup cleanup root"
    );

    let launch_cleanup = RunnerPipelineLaunchError::CleanupFailed {
        failure: Box::new(RunnerPipelineLaunchError::SpawnFailed(io::Error::other(
            "launch root",
        ))),
        cleanup: io::Error::other("launch cleanup root"),
    };
    assert_eq!(
        ErrorChain(&launch_cleanup).to_string(),
        "Pipeline launch failed: Pipeline process could not be started: launch root; Pipeline process cleanup also failed: launch cleanup root"
    );
}
