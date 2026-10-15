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

use super::{PipelineProcessTree, PipelineShutdownError, PipelineShutdownOutcome};
use crate::error::ErrorChain;
use rustix::io::Errno;
use rustix::process::{Pid, Signal, kill_process, kill_process_group, test_kill_process};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tempfile::TempDir;
use tokio::process::Command;
use tokio::time::Instant;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn shutdown_errors_render_each_cause_once() {
    let primary_only = PipelineShutdownError::BeforeDeadline {
        operation: "Pipeline test operation failed",
        primary: io::Error::other("primary root"),
        cleanup: None,
    };
    assert_eq!(
        ErrorChain(&primary_only).to_string(),
        "Pipeline test operation failed: primary root"
    );

    let primary_and_cleanup = PipelineShutdownError::BeforeDeadline {
        operation: "Pipeline test operation failed",
        primary: io::Error::other("primary root"),
        cleanup: Some(io::Error::other("cleanup root")),
    };
    assert_eq!(
        ErrorChain(&primary_and_cleanup).to_string(),
        "Pipeline test operation failed: primary root; process-group cleanup also failed: cleanup root"
    );

    let deadline_cleanup =
        PipelineShutdownError::CleanupAfterDeadline(io::Error::other("deadline cleanup root"));
    assert_eq!(
        ErrorChain(&deadline_cleanup).to_string(),
        "Pipeline shutdown deadline elapsed and process-group cleanup failed: deadline cleanup root"
    );
}

struct TestHierarchy {
    _directory: TempDir,
    tree: PipelineProcessTree,
    root_process_id: u32,
    plugin_process_id: u32,
    grandchild_process_id: u32,
    helper_process_id: u32,
    orderly_marker: PathBuf,
}

impl TestHierarchy {
    async fn start(behavior: ShutdownBehavior) -> io::Result<Self> {
        let directory = tempfile::tempdir()?;
        let root_process_id = directory.path().join("pipeline.pid");
        let plugin_process_id = directory.path().join("plugin.pid");
        let grandchild_process_id = directory.path().join("grandchild.pid");
        let helper_process_id = directory.path().join("helper.pid");
        let orderly_marker = directory.path().join("orderly.marker");
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", behavior.program(), "pipeline-tree-test"])
            .args([
                root_process_id.as_os_str(),
                plugin_process_id.as_os_str(),
                grandchild_process_id.as_os_str(),
                helper_process_id.as_os_str(),
                orderly_marker.as_os_str(),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .kill_on_drop(true);
        let child = command.spawn()?;
        let tree = PipelineProcessTree::from_spawned_child(child)?;
        let root_process_id = read_process_id(&root_process_id).await?;
        let plugin_process_id = read_process_id(&plugin_process_id).await?;
        let grandchild_process_id = read_process_id(&grandchild_process_id).await?;
        let helper_process_id = read_process_id(&helper_process_id).await?;
        if tree.process_id() != Some(root_process_id) {
            return Err(io::Error::other(
                "Pipeline process group leader does not match its direct child",
            ));
        }

        Ok(Self {
            _directory: directory,
            tree,
            root_process_id,
            plugin_process_id,
            grandchild_process_id,
            helper_process_id,
            orderly_marker,
        })
    }

    async fn assert_all_processes_gone(&self) -> io::Result<()> {
        for process_id in [
            self.root_process_id,
            self.plugin_process_id,
            self.grandchild_process_id,
            self.helper_process_id,
        ] {
            wait_until_process_gone(process_id).await?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum ShutdownBehavior {
    Orderly,
    IgnoreTermination,
}

impl ShutdownBehavior {
    const fn program(self) -> &'static str {
        match self {
            Self::Orderly => {
                "#!/bin/sh\n\
                 root_pid_file=$1\n\
                 plugin_pid_file=$2\n\
                 grandchild_pid_file=$3\n\
                 helper_pid_file=$4\n\
                 marker=$5\n\
                 (\n\
                   (\n\
                     trap 'printf \"grandchild\\n\" >> \"$marker\"; exit 0' TERM\n\
                     : > \"${grandchild_pid_file}.ready\"\n\
                     while :; do sleep 1; done\n\
                   ) &\n\
                   grandchild=$!\n\
                   printf '%s\\n' \"$grandchild\" > \"$grandchild_pid_file\"\n\
                   while [ ! -f \"${grandchild_pid_file}.ready\" ]; do sleep 0.01; done\n\
                   sleep 86400 &\n\
                   helper=$!\n\
                   printf '%s\\n' \"$helper\" > \"$helper_pid_file\"\n\
                   trap 'kill -TERM \"$grandchild\" \"$helper\" 2>/dev/null; wait \"$grandchild\"; wait \"$helper\" 2>/dev/null; printf \"plugin\\n\" >> \"$marker\"; exit 0' TERM\n\
                   : > \"${plugin_pid_file}.ready\"\n\
                   while :; do sleep 1; done\n\
                 ) &\n\
                 plugin=$!\n\
                 printf '%s\\n' \"$plugin\" > \"$plugin_pid_file\"\n\
                 while [ ! -f \"${plugin_pid_file}.ready\" ]; do sleep 0.01; done\n\
                 trap 'kill -TERM \"$plugin\" 2>/dev/null; wait \"$plugin\"; printf \"pipeline\\n\" >> \"$marker\"; exit 0' TERM\n\
                 printf '%s\\n' \"$$\" > \"$root_pid_file\"\n\
                 while :; do sleep 1; done\n"
            }
            Self::IgnoreTermination => {
                "#!/bin/sh\n\
                 root_pid_file=$1\n\
                 plugin_pid_file=$2\n\
                 grandchild_pid_file=$3\n\
                 helper_pid_file=$4\n\
                 trap '' TERM\n\
                 (\n\
                   trap '' TERM\n\
                   (\n\
                     trap '' TERM\n\
                     while :; do sleep 1; done\n\
                   ) &\n\
                   grandchild=$!\n\
                   printf '%s\\n' \"$grandchild\" > \"$grandchild_pid_file\"\n\
                   sleep 86400 &\n\
                   helper=$!\n\
                   printf '%s\\n' \"$helper\" > \"$helper_pid_file\"\n\
                   while :; do sleep 1; done\n\
                 ) &\n\
                 plugin=$!\n\
                 printf '%s\\n' \"$plugin\" > \"$plugin_pid_file\"\n\
                 printf '%s\\n' \"$$\" > \"$root_pid_file\"\n\
                 while :; do sleep 1; done\n"
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn planned_shutdown_preserves_pipeline_owned_orderly_cleanup() -> io::Result<()> {
    let mut hierarchy = TestHierarchy::start(ShutdownBehavior::Orderly).await?;

    let outcome = hierarchy.tree.terminate_and_reap(TEST_TIMEOUT).await?;

    assert_eq!(outcome, PipelineShutdownOutcome::ExitedBeforeDeadline);
    hierarchy.assert_all_processes_gone().await?;
    assert_eq!(
        fs::read_to_string(&hierarchy.orderly_marker)?,
        "grandchild\nplugin\npipeline\n"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_deadline_force_kills_plugin_and_grandchild_that_ignore_termination()
-> io::Result<()> {
    let mut hierarchy = TestHierarchy::start(ShutdownBehavior::IgnoreTermination).await?;

    let outcome = hierarchy
        .tree
        .terminate_and_reap(Duration::from_millis(100))
        .await?;

    assert_eq!(outcome, PipelineShutdownOutcome::ForcedAfterDeadline);
    hierarchy.assert_all_processes_gone().await
}

#[tokio::test(flavor = "current_thread")]
async fn direct_pipeline_kill_is_followed_by_complete_process_group_cleanup() -> io::Result<()> {
    let mut hierarchy = TestHierarchy::start(ShutdownBehavior::IgnoreTermination).await?;
    kill_process(to_pid(hierarchy.root_process_id)?, Signal::KILL).map_err(io::Error::from)?;

    let status = hierarchy.tree.wait().await?;

    assert!(!status.success());
    hierarchy.assert_all_processes_gone().await
}

#[tokio::test(flavor = "current_thread")]
async fn forced_cleanup_reaps_a_child_whose_group_is_already_disappearing() -> io::Result<()> {
    for _ in 0..100 {
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "while :; do sleep 1; done"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .kill_on_drop(true);
        let child = command.spawn()?;
        let mut tree = PipelineProcessTree::from_spawned_child(child)?;
        let process_id = tree
            .process_id()
            .ok_or_else(|| io::Error::other("Pipeline process id is unavailable"))?;

        kill_process_group(to_pid(process_id)?, Signal::KILL).map_err(io::Error::from)?;
        let status = tree.force_kill_and_reap().await?;

        assert!(!status.success());
        wait_until_process_gone(process_id).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn nonblocking_exit_observation_cleans_descendants_before_returning_status() -> io::Result<()>
{
    let mut hierarchy = TestHierarchy::start(ShutdownBehavior::IgnoreTermination).await?;
    kill_process(to_pid(hierarchy.root_process_id)?, Signal::KILL).map_err(io::Error::from)?;

    let status = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if let Some(status) = hierarchy.tree.try_wait()? {
                return Ok::<_, io::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(|_| io::Error::other("Nonblocking Pipeline exit observation timed out"))??;

    assert!(!status.success());
    hierarchy.assert_all_processes_gone().await
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_shutdown_wait_keeps_the_original_deadline() -> io::Result<()> {
    let mut hierarchy = TestHierarchy::start(ShutdownBehavior::IgnoreTermination).await?;
    let mut first_wait = Box::pin(
        hierarchy
            .tree
            .terminate_and_reap(Duration::from_millis(200)),
    );
    tokio::select! {
        result = &mut first_wait => {
            return Err(io::Error::other(format!(
                "Shutdown completed before cancellation: {result:?}"
            )));
        }
        () = tokio::time::sleep(Duration::from_millis(50)) => {}
    }
    drop(first_wait);

    let resumed_at = Instant::now();
    let outcome = hierarchy
        .tree
        .terminate_and_reap(Duration::from_secs(30))
        .await?;

    assert_eq!(outcome, PipelineShutdownOutcome::ForcedAfterDeadline);
    assert!(
        resumed_at.elapsed() < Duration::from_secs(1),
        "Cancelled shutdown wait restarted its deadline"
    );
    hierarchy.assert_all_processes_gone().await
}

#[tokio::test(flavor = "current_thread")]
async fn one_pipeline_group_cleanup_does_not_signal_a_sibling_pipeline() -> io::Result<()> {
    let mut first = TestHierarchy::start(ShutdownBehavior::IgnoreTermination).await?;
    let mut second = TestHierarchy::start(ShutdownBehavior::IgnoreTermination).await?;

    let _ = first.tree.force_kill_and_reap().await?;

    first.assert_all_processes_gone().await?;
    assert!(process_exists(second.root_process_id)?);
    assert!(process_exists(second.plugin_process_id)?);
    assert!(process_exists(second.grandchild_process_id)?);
    assert!(process_exists(second.helper_process_id)?);
    let _ = second.tree.force_kill_and_reap().await?;
    second.assert_all_processes_gone().await
}

async fn read_process_id(path: &Path) -> io::Result<u32> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match fs::read_to_string(path) {
                Ok(source) if source.ends_with('\n') => {
                    return source.trim().parse().map_err(io::Error::other);
                }
                Ok(_) => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(source) if source.kind() == io::ErrorKind::NotFound => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(source) => return Err(source),
            }
        }
    })
    .await
    .map_err(|_| io::Error::other("Test process id capture timed out"))?
}

async fn wait_until_process_gone(process_id: u32) -> io::Result<()> {
    tokio::time::timeout(TEST_TIMEOUT, async {
        while process_exists(process_id)? {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(())
    })
    .await
    .map_err(|_| io::Error::other(format!("Process {process_id} did not exit")))?
}

fn process_exists(process_id: u32) -> io::Result<bool> {
    match test_kill_process(to_pid(process_id)?) {
        Ok(()) => Ok(true),
        // The original child runs as this test user and is always signalable.
        // EPERM therefore means its numeric pid has already been reused by a
        // protected system process, not that the original child survived.
        Err(Errno::SRCH | Errno::PERM) => Ok(false),
        Err(source) => Err(io::Error::from(source)),
    }
}

fn to_pid(process_id: u32) -> io::Result<Pid> {
    i32::try_from(process_id)
        .ok()
        .and_then(Pid::from_raw)
        .ok_or_else(|| io::Error::other("Test process id is outside POSIX range"))
}

#[test]
fn shutdown_error_identifies_its_deadline_and_remaining_cleanup_responsibility() {
    let timeout = PipelineShutdownError::CleanupAfterDeadline(io::Error::other(
        "injected deadline cleanup failure",
    ));
    assert!(timeout.deadline_elapsed());

    let recovered_before_deadline = PipelineShutdownError::BeforeDeadline {
        operation: "Injected shutdown operation failed",
        primary: io::Error::other("injected operation failure"),
        cleanup: None,
    };
    assert!(!recovered_before_deadline.deadline_elapsed());

    let unrecovered_before_deadline = PipelineShutdownError::BeforeDeadline {
        operation: "Injected shutdown operation failed",
        primary: io::Error::other("injected operation failure"),
        cleanup: Some(io::Error::other("injected cleanup failure")),
    };
    assert!(!unrecovered_before_deadline.deadline_elapsed());
}
