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

//! Real Controller and reconfiguration with controlled task-completion delivery.

use super::super::*;
use crate::contracts::core::PluginInstanceState;
use crate::identifiers::{FlowId, PluginInstanceId};
use crate::pipeline::plugin::test_support::{TEST_DEADLINE, recorded_pid};
use crate::pipeline::reconfigure::test_support::clock::with_frozen_clock;
use crate::pipeline::reconfigure::test_support::controller::{
    hold_flow_exit, observe_shutdown, revision, with_pipeline_environment,
};
use crate::pipeline::reconfigure::test_support::instance_directory;
use crate::runner::test_support::run_pipeline_child_until_exit;
use std::error::Error;
use std::os::unix::process::ExitStatusExt as _;
use std::path::Path;
use tokio::time::timeout;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const CHILD_ROOT: &str = "TENON_TEST_CONTROLLER_BOUNDARY_ROOT";
const RECONFIGURE_TIMEOUT: Duration = Duration::from_millis(500);

#[tokio::test(flavor = "current_thread")]
async fn reconstruction_deadline_does_not_wait_for_the_blocking_job() -> TestResult {
    expires_at_boundary("reconstruction").await
}

#[tokio::test(flavor = "current_thread")]
async fn failed_apply_keeps_its_deadline_until_cleanup_finishes() -> TestResult {
    expires_at_boundary("cleanup").await
}

#[tokio::test(flavor = "current_thread")]
async fn latest_pending_and_runtime_status_do_not_extend_the_active_deadline() -> TestResult {
    expires_at_boundary("reconstruction-status").await
}

#[tokio::test(flavor = "current_thread")]
async fn a_published_update_does_not_remove_the_next_updates_deadline() -> TestResult {
    expires_at_boundary("second-update").await
}

async fn expires_at_boundary(boundary: &str) -> TestResult {
    let parent = tempfile::tempdir()?;
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "pipeline::controller::tests::reconfiguration::blocked_boundary_child",
            "--ignored",
            "--nocapture",
        ])
        .env(CHILD_ROOT, parent.path())
        .env("TENON_TEST_CONTROLLER_BOUNDARY", boundary)
        // The parent owns files left behind by the deliberate process-group kill.
        .env("TMPDIR", parent.path());
    let status =
        run_pipeline_child_until_exit(command, &parent.path().join("boundary-entered")).await?;
    assert_eq!(status.signal(), Some(libc::SIGKILL));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "child process entry point invoked by the parent controller test"]
async fn blocked_boundary_child() -> TestResult {
    let Some(parent) = std::env::var_os(CHILD_ROOT) else {
        return Ok(());
    };
    let boundary = std::env::var("TENON_TEST_CONTROLLER_BOUNDARY")?;
    with_pipeline_environment(async |reconfigurer, root, target| {
        let (mut ports, _shutdown) = ControllerPorts::new(RECONFIGURE_TIMEOUT);
        let phase = ports.start_ready(reconfigurer, target).await?;
        with_frozen_clock(check_deadline_boundary(
            reconfigurer,
            root,
            phase,
            ports,
            Path::new(&parent),
            &boundary,
        ))
        .await
    })
    .await
}

async fn check_deadline_boundary(
    reconfigurer: &mut PipelineReconfigurer,
    root: &Path,
    mut phase: ControllerPhase,
    mut ports: ControllerPorts,
    parent: &Path,
    boundary: &str,
) -> TestResult {
    let mut raw = revision(root.parent().ok_or("Pipeline parent is missing")?, "normal")?;
    if boundary == "second-update" {
        raw.document_etag = "published-update".into();
        ports.pending.replace(raw.clone());
        phase = ports.advance(reconfigurer, phase).await?;
        phase = ports.advance(reconfigurer, phase).await?;
        phase = ports.advance(reconfigurer, phase).await?;
        assert!(matches!(phase, ControllerPhase::WaitingForRevision));
        assert_eq!(
            ports
                .statuses
                .borrow()
                .as_ref()
                .ok_or("Missing status")?
                .document_etag,
            "published-update"
        );
        tokio::time::advance(RECONFIGURE_TIMEOUT * 2).await;
    }
    raw.document_etag = "blocked-update".into();
    if boundary == "cleanup" {
        let mut document: serde_json::Value = serde_json::from_str(&raw.tenon_document_json)?;
        document["flows"]["dual-to-archive"]["parallelism"] = serde_json::json!(0.5);
        std::fs::write(root.join(".candidate"), b"Occupied candidate directory")?;
        raw.tenon_document_json = serde_json::to_string(&document)?;
    }
    ports.pending.replace(raw.clone());
    phase = ports.advance(reconfigurer, phase).await?;
    let held_exit = if boundary == "cleanup" {
        Some(hold_flow_exit(
            reconfigurer,
            &FlowId::try_from("dual-to-archive".to_owned())?,
        )?)
    } else {
        None
    };
    let (mut held_exit, delivery) = match held_exit {
        Some((held, delivery)) => (Some(held), Some(delivery)),
        None => (None, None),
    };
    let _release_reconstruction = if held_exit.is_none() {
        Some(hold_reconstruction(&mut phase).await?)
    } else {
        None
    };
    let pending = Arc::clone(&ports.pending);
    let mut statuses = ports.statuses.subscribe();
    let controller = ports.run(reconfigurer, phase);
    tokio::pin!(controller);
    let actions = async move {
        if let Some(held) = &mut held_exit {
            held.wait().await?;
        }
        let remaining = if boundary == "reconstruction-status" {
            tokio::time::advance(RECONFIGURE_TIMEOUT - Duration::from_millis(1)).await;
            raw.document_etag = "initial-cutover".into();
            pending.replace(raw);
            // A real pure Sink failure publishes a status without changing
            // the reconstruction deadline or replacing the active target.
            let directory =
                instance_directory(root, &PluginInstanceId::try_from("archive".to_owned())?);
            rustix::process::kill_process(
                recorded_pid(&directory)?,
                rustix::process::Signal::KILL,
            )?;
            loop {
                statuses.changed().await?;
                if statuses.borrow_and_update().as_ref().is_some_and(|status| {
                    status.plugin_instances.iter().any(|instance| {
                        instance.id == "archive"
                            && instance.state() == PluginInstanceState::RestartBackoff
                    })
                }) {
                    break;
                }
            }
            Duration::from_millis(1)
        } else {
            RECONFIGURE_TIMEOUT
        };
        std::fs::write(parent.join("boundary-entered"), [])?;
        tokio::time::advance(remaining + Duration::from_millis(1)).await;
        std::future::pending::<()>().await;
        Ok::<(), Box<dyn Error>>(())
    };
    let checking = async {
        tokio::select! {
            result = &mut controller => { result?; Err("Controller returned before the deadline killed its process group".into()) }
            result = actions => result,
        }
    };
    let (result, ()) = tokio::join!(checking, async {
        if let Some(delivery) = delivery {
            delivery.await;
        }
    });
    result
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_reaches_selected_recovery_and_cleanup_before_reconstruction_finishes()
-> TestResult {
    for reconstructing in [false, true] {
        with_pipeline_environment(async |reconfigurer, root, target| {
            let (mut ports, shutdown) = ControllerPorts::new(Duration::from_secs(30));
            let mut phase = ports.start_ready(reconfigurer, target).await?;
            let mut release_reconstruction = if reconstructing {
                let mut raw =
                    revision(root.parent().ok_or("Pipeline parent is missing")?, "normal")?;
                raw.document_etag = "blocked-update".into();
                ports.pending.replace(raw);
                phase = ports.advance(reconfigurer, phase).await?;
                Some(hold_reconstruction(&mut phase).await?)
            } else {
                None
            };
            let (mut recovering, recovery_delivery) = hold_flow_exit(
                reconfigurer,
                &FlowId::try_from("dual-to-archive".to_owned())?,
            )?;
            let (mut cleanup, cleanup_delivery) =
                hold_flow_exit(reconfigurer, &FlowId::try_from("dual-to-dual".to_owned())?)?;
            let mut shutdown_observed = observe_shutdown(reconfigurer);
            let source =
                instance_directory(root, &PluginInstanceId::try_from("dual-b".to_owned())?);
            rustix::process::kill_process(recorded_pid(&source)?, rustix::process::Signal::KILL)?;
            let controller = ports.run(reconfigurer, phase);
            tokio::pin!(controller);
            let actions = async move {
                // This Flow stops only after the Controller selects the actual
                // reaped Source failure and enters its recovery operation.
                timeout(TEST_DEADLINE, recovering.wait()).await??;
                shutdown
                    .send(ReconfigureShutdown::Force)
                    .map_err(|_| "Controller shutdown receiver closed")?;
                timeout(
                    TEST_DEADLINE,
                    shutdown_observed.wait_for(|value| value.is_some()),
                )
                .await??;
                recovering.release();
                // A different Flow stops only when terminal cleanup begins,
                // after the selected recovery has returned to the Controller.
                timeout(TEST_DEADLINE, cleanup.wait()).await??;
                cleanup.release();
                // Cleanup began while the running reconstruction still could
                // not finish. Only now let its real blocking thread return.
                release_reconstruction.take();
                Ok::<(), Box<dyn Error>>(())
            };
            let (result, actions, (), ()) =
                tokio::join!(controller, actions, recovery_delivery, cleanup_delivery);
            actions?;
            result?;
            assert!(!root.exists());
            Ok(())
        })
        .await?;
    }
    Ok(())
}

/// Drives production transitions before a test delays an external task result.
struct ControllerPorts {
    reconfigure_timeout: Duration,
    pending: Arc<LatestRevisionSlot<PipelineRevisionPlan>>,
    statuses: watch::Sender<Option<PipelineStatusSnapshot>>,
    shutdown: oneshot::Receiver<ReconfigureShutdown>,
}

impl ControllerPorts {
    fn new(reconfigure_timeout: Duration) -> (Self, oneshot::Sender<ReconfigureShutdown>) {
        let (shutdown, receiver) = oneshot::channel();
        (
            Self {
                reconfigure_timeout,
                pending: Arc::new(LatestRevisionSlot::new()),
                statuses: watch::channel(None).0,
                shutdown: receiver,
            },
            shutdown,
        )
    }

    async fn start_ready(
        &mut self,
        reconfigurer: &mut PipelineReconfigurer,
        target: PipelineRevision,
    ) -> TestResult<ControllerPhase> {
        let mut phase = self
            .advance(
                reconfigurer,
                ControllerPhase::Applying {
                    target: Box::new(target),
                    deadline: None,
                },
            )
            .await?;
        while !self.statuses.borrow().as_ref().is_some_and(|status| {
            status
                .plugin_instances
                .iter()
                .all(|instance| instance.state() == PluginInstanceState::Running)
        }) {
            phase = self.advance(reconfigurer, phase).await?;
        }
        Ok(phase)
    }

    async fn advance(
        &mut self,
        reconfigurer: &mut PipelineReconfigurer,
        phase: ControllerPhase,
    ) -> TestResult<ControllerPhase> {
        match phase
            .advance(
                reconfigurer,
                self.reconfigure_timeout,
                &self.pending,
                &self.statuses,
                &mut self.shutdown,
            )
            .await?
        {
            ControlFlow::Continue(phase) => Ok(phase),
            ControlFlow::Break(_) => Err("Controller stopped during fixture setup".into()),
        }
    }

    async fn run(
        self,
        reconfigurer: &mut PipelineReconfigurer,
        phase: ControllerPhase,
    ) -> Result<(), PipelineControllerError> {
        run_controller(
            reconfigurer,
            phase,
            self.reconfigure_timeout,
            self.pending,
            self.statuses,
            self.shutdown,
        )
        .await
    }
}

async fn hold_reconstruction(
    phase: &mut ControllerPhase,
) -> TestResult<std::sync::mpsc::Sender<()>> {
    let ControllerPhase::Reconstructing { reconstruction, .. } = phase else {
        return Err("Controller did not enter reconstruction".into());
    };
    // Consume the real parser's output and completed handle. Delay only its
    // delivery using another actually running blocking task; do not detach it.
    let target = (&mut reconstruction.task).await?;
    let (started, observed) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    reconstruction.task = tokio::task::spawn_blocking(move || {
        let _ = started.send(());
        let _ = released.recv();
        target
    });
    observed.await?;
    Ok(release)
}
