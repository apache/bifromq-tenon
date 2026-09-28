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

//! The test executable enters the actual target control loop using unchanged launch arguments.

use std::error::Error;
use std::path::PathBuf;

#[test]
#[ignore = "child process entry point invoked by the parent pipeline test"]
fn instance_flow_pipeline_child() -> Result<(), Box<dyn Error>> {
    let Some(record_directory) = std::env::var_os("TENON_TEST_PIPELINE_RECORDS") else {
        return Ok(());
    };
    let records = PathBuf::from(record_directory);
    let socket = std::env::var_os("TENON_TEST_PIPELINE_CONTROL_SOCKET")
        .ok_or("test control socket is missing")?;
    let launch_id = std::env::var_os("TENON_TEST_PIPELINE_LAUNCH_ID")
        .ok_or("test launch identity is missing")?;
    let launch = crate::pipeline::parse_arguments([
        "--control-socket".into(),
        socket,
        "--launch-id".into(),
        launch_id,
    ])?;
    let preconditions = records.join("must-be-removed.json");
    if preconditions.exists() {
        let paths: Vec<PathBuf> = serde_json::from_slice(&std::fs::read(preconditions)?)?;
        for path in paths {
            assert!(
                !path.exists(),
                "a new Pipeline was spawned before old resource cleanup: {}",
                path.display()
            );
        }
    }
    let record = records.join(format!("{}.json", std::process::id()));
    let temporary = record.with_extension("tmp");
    std::fs::write(
        &temporary,
        serde_json::to_vec(&crate::plugin_control_directory(
            std::path::Path::new(launch.control_socket.as_ref()),
            &launch.launch_id,
        ))?,
    )?;
    std::fs::rename(temporary, record)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let outcome = runtime.block_on(super::run(launch));
    runtime.shutdown_timeout(std::time::Duration::ZERO);
    outcome.map_err(Into::into)
}
