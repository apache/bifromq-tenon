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

use super::super::{Deadline, enforce};
use crate::runner::test_support::run_pipeline_child_until_exit;
use std::error::Error;
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

type TestResult = Result<(), Box<dyn Error>>;
const CHILD_ROOT: &str = "TENON_TEST_DEADLINE_DROP_ROOT";

#[tokio::test(flavor = "current_thread")]
async fn expiration_kills_before_dropping_an_operation_with_blocked_cleanup() -> TestResult {
    expires_without_drop("pending").await
}

#[tokio::test(flavor = "current_thread")]
async fn an_expired_deadline_precedes_an_already_ready_operation() -> TestResult {
    expires_without_drop("ready").await
}

async fn expires_without_drop(operation: &str) -> TestResult {
    let parent = tempfile::tempdir()?;
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "pipeline::controller::tests::deadline::deadline_drop_child",
            "--ignored",
            "--nocapture",
        ])
        .env(CHILD_ROOT, parent.path())
        .env("TENON_TEST_DEADLINE_OPERATION", operation);
    let status = run_pipeline_child_until_exit(command, &parent.path().join("constructed")).await?;
    assert_eq!(status.signal(), Some(libc::SIGKILL));
    assert!(!parent.path().join("dropped").exists());
    if operation == "ready" {
        assert!(!parent.path().join("polled").exists());
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "child process entry point invoked by the parent deadline test"]
async fn deadline_drop_child() -> TestResult {
    let Some(parent) = std::env::var_os(CHILD_ROOT) else {
        return Ok(());
    };
    let parent = Path::new(&parent);
    tokio::time::pause();
    let deadline = Deadline::start(Duration::from_millis(500));
    let guard = BlockedDrop(parent.join("dropped"));
    let operation = std::env::var("TENON_TEST_DEADLINE_OPERATION")?;
    if operation == "ready" {
        tokio::time::advance(Duration::from_millis(500)).await;
    }
    let future = async {
        let _guard = guard;
        std::fs::write(parent.join("polled"), [])?;
        if operation == "pending" {
            std::future::pending::<()>().await;
        }
        Ok::<_, std::io::Error>(())
    };
    std::fs::write(parent.join("constructed"), [])?;
    enforce(deadline, future).await?;
    Err("Expired operation returned without terminating the Pipeline".into())
}

struct BlockedDrop(PathBuf);

impl Drop for BlockedDrop {
    fn drop(&mut self) {
        let _ = std::fs::write(&self.0, []);
        loop {
            std::thread::park();
        }
    }
}
