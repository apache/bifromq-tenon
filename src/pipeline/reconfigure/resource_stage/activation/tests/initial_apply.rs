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

//! Exercises the complete initial apply while reusing the real child/Queue fixture.

use super::*;
use crate::contracts::core::pipeline_diagnostic_record;
use crate::lua::LuaVmErrorKind;
use crate::pipeline::channel::FlowChannelError;
use crate::pipeline::diagnostics::test_support::interested_flow_channel;
use crate::pipeline::reconfigure::test_support::bootstrap;
use crate::pipeline::reconfigure::{PipelineApplyOutcome, ReconfigureShutdown};
use std::os::unix::process::ExitStatusExt as _;

#[tokio::test(flavor = "current_thread")]
async fn first_apply_publishes_before_ready_and_activates_real_output() -> TestResult {
    with_environment("delay-ready", &[], async |reconfigurer, _, root, target| {
        let PipelineApplyOutcome::Applied(status) = reconfigurer.apply(target, None).await? else {
            return Err("Initial apply unexpectedly stopped".into());
        };
        assert_eq!(status.document_etag, "initial-cutover");
        assert_eq!(status.plugin_instances.len(), 4);
        assert!(
            status
                .plugin_instances
                .iter()
                .all(|instance| instance.state() == PluginInstanceState::Starting)
        );
        assert_eq!(current(reconfigurer)?.status_snapshot(), status);
        let mut archive = sink_reader(root, "archive", "dual-to-archive", 0)?;
        assert!(
            EgressRecord::decode(read_record(&mut archive).await?.as_slice())?
                .payload
                .is_empty()
        );
        wait_for_states(
            current(reconfigurer)?,
            &[("dual-b", PluginInstanceState::Starting)],
        )
        .await?;
        let delayed = instance_directory(root, "dual-b")?;
        wait_for_file(&delayed.join("attach.received")).await?;
        assert_eq!(lifecycle_events(&delayed)?, "attach\n");
        assert_eq!(
            current(reconfigurer)?.status_snapshot().document_etag,
            status.document_etag
        );
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn first_apply_keeps_independent_start_failures_in_the_complete_result() -> TestResult {
    with_environment(
        "missing-dual-program",
        &[],
        async |reconfigurer, _, _, target| {
            let PipelineApplyOutcome::Applied(status) = reconfigurer.apply(target, None).await?
            else {
                return Err("Initial apply unexpectedly stopped".into());
            };
            assert_eq!(
                states(&status),
                BTreeMap::from([
                    ("input".into(), "starting".into()),
                    ("archive".into(), "starting".into()),
                    ("dual-a".into(), "start-failed".into()),
                    ("dual-b".into(), "start-failed".into()),
                ])
            );
            wait_for_states(
                current(reconfigurer)?,
                &[
                    ("dual-a", PluginInstanceState::StartFailed),
                    ("dual-b", PluginInstanceState::StartFailed),
                ],
            )
            .await?;
            Ok(())
        },
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn source_config_and_lua_replace_the_session_and_definition_together() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, target| {
        let mut replacement =
            revision(root.parent().ok_or("Fixture parent is missing")?, "normal")?;
        replacement.document_etag = "later-target".into();
        let mut document: serde_json::Value =
            serde_json::from_str(&replacement.tenon_document_json)?;
        document["pluginInstances"]["dual-b"]["config"]["endpoint"] = "changed".into();
        document["flows"]["dual-to-archive"]["process"]["script"] =
            "function main(event) emit() end".into();
        replacement.tenon_document_json = serde_json::to_string(&document)?;
        reconfigurer.apply(target, None).await?;
        wait_for_states(current(reconfigurer)?, &[]).await?;
        let pids = instance_pids(root)?;
        let files = queue_files(root)?;
        let mut archive = sink_reader(root, "archive", "dual-to-archive", 0)?;
        read_record(&mut archive).await?;
        archive.release(1)?;

        assert!(matches!(
            reconfigurer.apply(model(replacement)?, None).await?,
            PipelineApplyOutcome::Applied(_)
        ));
        assert_eq!(
            current(reconfigurer)?.status_snapshot().document_etag,
            "later-target"
        );
        wait_for_states(current(reconfigurer)?, &[]).await?;
        assert_reaped(pids["dual-b"])?;
        assert_ne!(
            recorded_pid(&instance_directory(root, "dual-b")?)?,
            pids["dual-b"]
        );
        for id in ["input", "dual-a", "archive"] {
            assert_eq!(recorded_pid(&instance_directory(root, id)?)?, pids[id]);
        }
        assert_eq!(queue_files(root)?, files);
        let mut source = source_writer(root, "dual-b", "dual-to-archive", 0)?;
        assert!(matches!(
            {
                let record = IngressRecord {
                    record_id: 42,
                    payload: Vec::new().into(),
                };
                source.try_write_with(
                    record.encoded_len(),
                    |destination| record.encode(destination).map_err(std::io::Error::other),
                    || {},
                )
            }?,
            WriteOutcome::Committed(_)
        ));
        let mut completion = source_completion(root, "dual-b", "dual-to-archive", 0)?;
        let result = IngressCompletion::decode(read_record(&mut completion).await?.as_slice())?;
        assert_eq!(result.record_id, 42);
        assert_eq!(result.status(), IngressCompletionStatus::Ok);
        assert!(matches!(archive.try_read()?, ReadOutcome::Empty));
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn stop_before_first_apply_creates_no_runtime_resources() -> TestResult {
    with_environment("normal", &[], async |reconfigurer, _, root, target| {
        reconfigurer
            .shutdown_handle()
            .request(ReconfigureShutdown::Planned);
        assert!(matches!(
            reconfigurer.apply(target, None).await?,
            PipelineApplyOutcome::Stopped
        ));
        assert!(reconfigurer.current.is_none());
        assert!(!root.exists());
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn initial_apply_returns_the_original_lua_failure_after_cleanup() -> TestResult {
    with_environment("normal", &[("dual-to-dual", "error('initialization failed')")], async |reconfigurer, _, root, target| {
        assert!(matches!(reconfigurer.apply(target, None).await,
            Err(PipelineReconfigureError::RuntimeStart(PipelineRuntimeError::FlowChannelOpen {
                flow_id, channel_index: 0, source: FlowChannelError::LuaVmLoad { kind: LuaVmErrorKind::TopLevelFailed }
            })) if flow_id.as_str() == "dual-to-dual"
        ));
        assert!(reconfigurer.current.is_none());
        assert!(!root.exists());
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn stop_during_initial_apply_interrupts_lua_and_awaits_cleanup() -> TestResult {
    with_environment("normal", &[("dual-to-dual", "print('initializing'); while true do end")], async |reconfigurer, _, root, target| {
        let mut bootstrap = bootstrap(root)?;
        bootstrap.environment.as_mut().and_then(|environment| environment.lua_limits.as_mut()).ok_or("Fixture Lua limits are missing")?.cpu_time_limit_ms = 60_000;
        let environment = bootstrap.environment.ok_or("Fixture environment is missing")?;
        reconfigurer.environment = Arc::new(environment);
        let (diagnostics, mut records) = interested_flow_channel(&FlowId::try_from(String::from("dual-to-dual"))?, 0);
        reconfigurer.diagnostics = diagnostics;
        let shutdown = reconfigurer.shutdown_handle();
        let mut applying = Box::pin(reconfigurer.apply(target, None));
        let started = tokio::select! {
            started = timeout(TEST_DEADLINE, records.recv()) => started,
            result = &mut applying => { result?; return Err("Apply completed before initialization was interrupted".into()); }
        };
        shutdown.request(ReconfigureShutdown::Force);
        // Even a timeout must retain and await the apply owner through terminal cleanup.
        let stopped = timeout(TEST_DEADLINE, &mut applying).await;
        if stopped.is_err() { applying.as_mut().await?; }
        assert!(matches!(stopped??, PipelineApplyOutcome::Stopped));
        drop(applying);
        assert!(matches!(started?.ok_or("Initialization diagnostic is missing")?.record,
            Some(pipeline_diagnostic_record::Record::Channel(record)) if record.text == "initializing"
        ));
        assert!(reconfigurer.current.is_none());
        assert!(!root.exists());
        Ok(())
    }).await
}

#[tokio::test(flavor = "current_thread")]
async fn unreaped_child_failure_aborts_with_diagnostics_before_deleting_queues() -> TestResult {
    let parent = tempfile::tempdir()?;
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command.args(["--exact", "pipeline::reconfigure::resource_stage::activation::tests::initial_apply::unreaped_failure_child", "--ignored", "--nocapture"])
        .env("TENON_TEST_UNREAPED_FAILURE_ROOT", parent.path()).kill_on_drop(true);
    let output = timeout(TEST_DEADLINE, command.output()).await??;
    assert_eq!(output.status.signal(), Some(libc::SIGABRT));
    assert!(
        String::from_utf8(output.stderr)?
            .contains("pipeline.plugin_cleanup_failed: simulated wait failure")
    );
    assert!(
        instance_directory(&parent.path().join("pipeline"), "input")?
            .join("source/submission-0.queue")
            .exists()
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "child process entry point invoked by the parent cleanup test"]
async fn unreaped_failure_child() -> TestResult {
    let Some(parent) = std::env::var_os("TENON_TEST_UNREAPED_FAILURE_ROOT") else {
        return Ok(());
    };
    let root = Path::new(&parent).join("pipeline");
    let server = PluginControlServer::start()?;
    let environment = fixture_environment(&root)?;
    let mut reconfigurer = Reconfigurer::new(environment, publisher(), server.launcher());
    reconfigurer
        .apply(
            model(revision(Path::new(&parent), "missing-all-programs")?)?,
            None,
        )
        .await?;
    // This exercises only the fatal boundary, not a kernel kill/wait failure.
    // Missing executables ensure abort cannot orphan real Plugin children.
    server.shutdown().await?;
    abort_with_unreaped_plugins(&std::io::Error::other("simulated wait failure"));
}
