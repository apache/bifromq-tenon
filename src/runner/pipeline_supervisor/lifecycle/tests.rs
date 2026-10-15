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

use super::{
    PipelineAttemptOutcome, PipelineLifecycleInput, RetryBackoffSequence,
    RunnerPipelineLifecycleError, wait_for_ready_target,
};
use crate::payload_contract::PluginInterface;
use crate::runner::pipeline::test_support::TargetReferenceProbe;
use crate::runner::pipeline::{PipelineCleanupError, cleanup_pipeline_directory};
use crate::runner::pipeline_supervisor::lifecycle::test_support::target;
use crate::runner::test_support::{install_plugin, load_config};
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::{task, time};

#[tokio::test(flavor = "current_thread")]
async fn target_control_keeps_only_the_latest_runnable_revision() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    install_plugin(state_directory.path(), PluginInterface::Source)?;
    install_plugin(state_directory.path(), PluginInterface::Sink)?;
    let config = load_config(state_directory.path())?;
    let first = Arc::new(target(
        state_directory.path(),
        &config,
        "latest-target",
        "function main(event) emit() end",
    )?);
    let latest = Arc::new(target(
        state_directory.path(),
        &config,
        "latest-target",
        "function main(event) local value = event emit() end",
    )?);
    let latest_etag = latest.document_etag();
    let first_targets = TargetReferenceProbe::new(&first);
    let (control, mut input) = PipelineLifecycleInput::new();

    control.set_target(Some(Arc::clone(&first)));
    drop(first);
    first_targets.assert_retained()?;
    control.set_target(None);
    first_targets.assert_released()?;
    control.set_target(Some(latest));

    let selected = wait_for_ready_target(&mut input, None)
        .await
        .ok_or_else(|| io::Error::other("Latest runnable target was not selected"))?;
    assert_eq!(selected.document_etag(), latest_etag);
    control.stop();
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn unready_target_waits_without_ending_the_lifecycle() -> io::Result<()> {
    let (control, mut input) = PipelineLifecycleInput::new();
    control.set_target(None);
    {
        let wait = wait_for_ready_target(&mut input, None);
        tokio::pin!(wait);

        tokio::select! {
            biased;
            target = &mut wait => {
                return Err(io::Error::other(format!(
                    "Unready target unexpectedly completed lifecycle wait: {}",
                    target.is_some()
                )));
            }
            () = task::yield_now() => {}
        }
    }
    control.stop();
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn backoff_preserves_its_deadline_but_selects_the_latest_target() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    install_plugin(state_directory.path(), PluginInterface::Source)?;
    install_plugin(state_directory.path(), PluginInterface::Sink)?;
    let config = load_config(state_directory.path())?;
    let first = Arc::new(target(
        state_directory.path(),
        &config,
        "backoff-target",
        "function main(event) emit() end",
    )?);
    let replacement = Arc::new(target(
        state_directory.path(),
        &config,
        "backoff-target",
        "function main(event) emit() emit() end",
    )?);
    let latest = Arc::new(target(
        state_directory.path(),
        &config,
        "backoff-target",
        "function main(event) local value = event emit() end",
    )?);
    let latest_etag = latest.document_etag();
    let (control, mut input) = PipelineLifecycleInput::new();
    control.set_target(Some(first));
    let _ = wait_for_ready_target(&mut input, None)
        .await
        .ok_or_else(|| io::Error::other("Initial target was not selected"))?;

    let wait = wait_for_ready_target(&mut input, Some(Duration::from_millis(25)));
    tokio::pin!(wait);
    control.set_target(Some(replacement));
    control.set_target(None);
    control.set_target(Some(latest));
    assert!(
        time::timeout(Duration::from_millis(2), &mut wait)
            .await
            .is_err(),
        "Target updates bypassed the existing restart deadline"
    );
    let selected = wait
        .await
        .ok_or_else(|| io::Error::other("Backoff did not select the latest ready target"))?;
    assert_eq!(selected.document_etag(), latest_etag);
    control.stop();
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn planned_stop_wins_over_an_already_ready_target() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    install_plugin(state_directory.path(), PluginInterface::Source)?;
    install_plugin(state_directory.path(), PluginInterface::Sink)?;
    let config = load_config(state_directory.path())?;
    let ready = Arc::new(target(
        state_directory.path(),
        &config,
        "stop-priority",
        "function main(event) emit() end",
    )?);
    let (control, mut input) = PipelineLifecycleInput::new();
    control.set_target(Some(ready));
    control.stop();

    assert!(wait_for_ready_target(&mut input, None).await.is_none());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn closed_restart_admission_prevents_an_already_ready_spawn() -> io::Result<()> {
    let state_directory = tempfile::tempdir()?;
    install_plugin(state_directory.path(), PluginInterface::Source)?;
    install_plugin(state_directory.path(), PluginInterface::Sink)?;
    let config = load_config(state_directory.path())?;
    let ready = Arc::new(target(
        state_directory.path(),
        &config,
        "closed-restart-admission",
        "function main(event) emit() end",
    )?);
    let (control, mut input) = PipelineLifecycleInput::new();
    control.set_target(Some(ready));
    control.close_execution_admission();

    assert!(wait_for_ready_target(&mut input, None).await.is_none());
    control.stop();
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn directory_cleanup_failure_preserves_the_shutdown_timeout() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("not-a-directory");
    std::fs::write(&path, [])?;
    let outcome = PipelineAttemptOutcome::Stopped {
        shutdown_timed_out: true,
    };
    let failure = outcome
        .with_cleanup(
            cleanup_pipeline_directory(path.clone())
                .await
                .map_err(RunnerPipelineLifecycleError::Cleanup),
        )
        .err()
        .ok_or_else(|| io::Error::other("Directory cleanup unexpectedly succeeded"))?;
    assert!(failure.includes_shutdown_timeout());
    assert!(failure.process_owner_recovered());
    assert!(
        matches!(failure, RunnerPipelineLifecycleError::CleanupAfterShutdownTimeout(
        PipelineCleanupError::Filesystem { path: failed_path, source }
    ) if failed_path == path && source.kind() == io::ErrorKind::NotADirectory)
    );
    Ok(())
}

#[test]
fn restart_backoff_doubles_until_the_configured_maximum() {
    let mut backoff =
        RetryBackoffSequence::new(Duration::from_millis(3), Duration::from_millis(10));

    assert_eq!(backoff.take_next(), Duration::from_millis(3));
    assert_eq!(backoff.take_next(), Duration::from_millis(6));
    assert_eq!(backoff.take_next(), Duration::from_millis(10));
    assert_eq!(backoff.take_next(), Duration::from_millis(10));
}
