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

//! Uses real Pipeline processes for tests with local filesystem errors.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn control_directory_creation_failure_keeps_other_pipeline_running() -> TestResult {
    if process::geteuid().is_root() {
        return Ok(());
    }
    with_fixture(async |fixture| {
        let healthy = fixture
            .set(&document("healthy", "A", "normal"))?
            .ok_or("The target for Pipeline healthy is not ready")?;
        fixture.running(healthy.document_etag()).await?;
        let root = fixture.working_directory(&healthy);
        let pids = instance_pids(&root)?;
        let socket = fixture.socket_directory(process_group(pids[0])?)?;
        let parent = socket
            .parent()
            .ok_or("Control directory parent is missing")?
            .to_path_buf();
        let permissions = fs::metadata(&parent)?.permissions();
        let restore = RestorePermissions {
            path: parent.clone(),
            permissions,
        };
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o500))?;
        let blocked = fixture
            .set(&document("blocked", "A", "normal"))?
            .ok_or("The target for Pipeline blocked is not ready")?;
        loop {
            match fixture.supervisor.next_event().await {
                PipelineEvent::State { update, .. }
                    if update.document_id.as_str() == "blocked"
                        && matches!(update.state, PipelineLifecycleState::RestartBackoff(_)) =>
                {
                    break;
                }
                PipelineEvent::State { .. } => {}
                PipelineEvent::Failure(error) => return Err(error.into()),
            }
        }
        assert_eq!(fixture.launch_count()?, 1);
        fixture.observe(traffic(&root, 1)).await?;
        assert_eq!(instance_pids(&root)?, pids);
        drop(restore);
        fixture.running(blocked.document_etag()).await?;
        assert_eq!(fixture.launch_count()?, 2);
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn requested_runner_shutdown_ends_a_pending_cleanup_retry() -> TestResult {
    with_fixture(async |fixture| {
        let target = fixture
            .set(&document("blocked-cleanup", "A", "normal"))?
            .ok_or("The target is not ready")?;
        fixture.running(target.document_etag()).await?;
        let root = fixture.working_directory(&target);
        let pid = process_group(instance_pids(&root)?[0])?;
        let socket = fixture.socket_directory(pid)?;
        let displaced = fixture.root.path().join("displaced-control");
        fs::rename(&socket, &displaced)?;
        fs::write(&socket, b"not a directory")?;
        fixture
            .supervisor
            .apply(Box::new([PipelineDirective::Stop {
                document_id: TenonDocumentId::try_from("blocked-cleanup")?,
            }]))?;
        loop {
            match fixture.supervisor.next_event().await {
                PipelineEvent::State { update, .. }
                    if matches!(update.state, PipelineLifecycleState::RestartBackoff(ref error)
                        if error.code == "runner.pipeline_cleanup_failed") =>
                {
                    break;
                }
                PipelineEvent::State { .. } => {}
                PipelineEvent::Failure(error) => return Err(error.into()),
            }
        }
        let result = fixture.supervisor.shutdown().await;
        assert_eq!(result.failures.len(), 1);
        assert_eq!(result.cleanup, PipelineCleanup::Complete);
        assert_eq!(fixture.launch_count()?, 1);
        assert_reaped(pid)?;
        assert!(socket.is_file());
        fs::remove_file(&socket)?;
        fs::rename(displaced, socket)?;
        Ok(())
    })
    .await
}

struct RestorePermissions {
    path: PathBuf,
    permissions: fs::Permissions,
}

impl Drop for RestorePermissions {
    fn drop(&mut self) {
        let _ = fs::set_permissions(&self.path, self.permissions.clone());
    }
}
