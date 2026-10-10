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

//! Shared fixtures for Runner and private control-boundary tests.

pub(crate) use super::plugin::package::tests::valid_program_descriptor;
#[cfg(not(feature = "loom-model"))]
pub(crate) use super::process_tree::test_support::run_pipeline_child_until_exit;

use super::executable::CapturedRunnerExecutable;
use super::plugin::package::tests::valid_program_package;
use super::plugin::store::PluginProgramStore;
use super::state_directory::prepare_runner_state_directory;
use crate::config::RunnerConfig;
use crate::payload_contract::PluginInterface;
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::extensions::RunnerHooks;
use crate::runner::pipeline::{PipelineLifecycleTarget, RuntimeResolution, RuntimeResolver};
use crate::runner::process_resources;
use crate::runner::recovery::recover;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::fs;
use std::io::{self, Cursor};
use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::DirBuilderExt as _;
use std::path::Path;

pub(super) fn captured_runner_executable() -> io::Result<CapturedRunnerExecutable> {
    let image = tempfile::NamedTempFile::new()?;
    fs::write(image.path(), b"captured runner test image")?;
    CapturedRunnerExecutable::from_file(image.reopen()?)
}

pub(super) fn install_plugin(state_directory: &Path, interface: PluginInterface) -> io::Result<()> {
    let layout = prepare_runner_state_directory(state_directory).map_err(io::Error::other)?;
    let runtime = tempfile::tempdir_in(layout.pipeline_runtime_directory())?;
    PluginProgramStore::recover(
        layout.plugin_program_store_directory(),
        runtime.path().to_path_buf(),
        std::sync::Arc::new(crate::runner::extensions::ByPass),
    )
    .map_err(io::Error::other)?
    .install(Cursor::new(valid_program_package(interface)?))
    .map(drop)
    .map_err(io::Error::other)
}

pub(super) fn write_document(
    state_directory: &Path,
    file_id: &str,
    source: String,
) -> io::Result<()> {
    let directory = state_directory.join("tenon-documents");
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&directory)?;
    fs::write(
        directory.join(format!("{}.jsonc", sha256_hex(file_id.as_bytes()))),
        source,
    )
}

pub(super) fn document(id: &str) -> String {
    format!(
        r#"{{"specVersion":"1","id":"{id}","pluginInstances":{{"source":{{"programName":"com.example.source","exactVersion":"1.0.0","config":{{"endpoint":"tcp://source"}}}},"primary":{{"programName":"com.example.sink","exactVersion":"1.0.0","config":{{"endpoint":"tcp://sink"}}}}}},"flows":{{"main":{{"parallelism": 1, "source":"source","process":{{"script":"function main(event) emit() end"}},"sinks":["primary"]}}}}}}"#,
    )
}

pub(super) fn load_config(state_directory: &Path) -> io::Result<RunnerConfig> {
    let path = state_directory.join("runner.jsonc");
    let http_address = available_http_address()?;
    fs::write(
        &path,
        format!(
            r#"{{"stateDirectory":{},"http":{{"listenAddress":"{http_address}"}},"pipeline":{{"startupTimeoutMs":2000,"shutdownTimeoutMs":2000,"retryBackoff":{{"initialDelayMs":10,"maximumDelayMs":20}}}},"lua":{{"cpuTimeLimitMs":50,"memoryLimitBytes":16777216}}}}"#,
            serde_json::to_string(&state_directory.to_string_lossy()).map_err(io::Error::other)?,
        ),
    )?;
    RunnerConfig::load(&path).map_err(io::Error::other)
}

fn available_http_address() -> io::Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.local_addr()
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(super) fn target(
    state_directory: &Path,
    config: &RunnerConfig,
    document_id: &str,
    lua_source: &str,
) -> io::Result<PipelineLifecycleTarget> {
    let mut source: Value =
        serde_json::from_str(&document(document_id)).map_err(io::Error::other)?;
    source["flows"]["main"]["process"]["script"] = Value::String(lua_source.to_owned());
    write_document(
        state_directory,
        document_id,
        serde_json::to_string(&source).map_err(io::Error::other)?,
    )?;
    let state_layout = prepare_runner_state_directory(state_directory).map_err(io::Error::other)?;
    let (documents, programs) = recover(
        config,
        &state_layout,
        &RunnerHooks::default().artifact_protection,
        &state_layout.pipeline_runtime_directory(),
    )
    .map_err(io::Error::other)?
    .into_parts();
    let mut documents = documents.into_vec();
    if documents.len() != 1 {
        return Err(io::Error::other(
            "Test recovery did not produce exactly one Tenon Document",
        ));
    }
    let document = documents
        .pop()
        .ok_or_else(|| io::Error::other("Recovered test document is missing"))?;
    let (source, document) = document.into_parts();
    let document_etag = TenonDocumentEtag::for_source(&source);
    let RuntimeResolution::Ready(plan) = RuntimeResolver::new(
        config.script_vm_limits(),
        process_resources::available_cpu_count()?,
    )
    .resolve(&document, &programs) else {
        return Err(io::Error::other("Recovered test document is not ready"));
    };
    Ok(PipelineLifecycleTarget::new(plan, document_etag))
}
