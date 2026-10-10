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

//! Real installed Program and Runtime Integrity fixtures for lifecycle tests.

pub(in crate::runner) use super::launch_registry::test_support::wait_for_attachment;
use super::runtime_resolution::{RuntimeResolution, RuntimeResolver};
use super::target::{PipelineLifecycleTarget, PipelineRunningState};
use crate::config::{RunnerConfig, ScriptVmLimits};
use crate::contracts::core::{
    ControlError, PipelineStatusSnapshot, PluginInstanceState, PluginInstanceStatus,
};
use crate::identifiers::{ExactVersion, ProgramName};
use crate::payload_contract::PluginInterface;
use crate::pipeline::test_support::controlled_program_command;
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::plugin::package::tests::{
    archive, valid_config_schema, valid_program_descriptor,
};
use crate::runner::plugin::store::{PluginProgramStore, PluginStoreError, PluginUninstallOutcome};
use crate::runner::process_resources;
use crate::tenon_document::UnverifiedTenonDocument;
use crate::tenon_document::verified::TenonDocumentVerifier;
use serde_json::{Value, json};
use std::io::{self, Cursor};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{Arc, Weak};
use std::time::Duration;

pub(in crate::runner) struct TargetFixture {
    pub(in crate::runner) store: PluginProgramStore,
    verifier: TenonDocumentVerifier,
    resolver: RuntimeResolver,
}

impl TargetFixture {
    pub(in crate::runner) fn new(directory: &Path, config: &RunnerConfig) -> io::Result<Self> {
        let store = PluginProgramStore::recover(
            directory.to_owned(),
            directory.to_owned(),
            std::sync::Arc::new(crate::runner::extensions::ByPass),
        )
        .map_err(io::Error::other)?;
        let limits = config.script_vm_limits();
        let mut fixture = Self {
            store,
            verifier: TenonDocumentVerifier::try_new(limits).map_err(io::Error::other)?,
            resolver: RuntimeResolver::new(limits, process_resources::available_cpu_count()?),
        };
        fixture.install("com.example.input", "1.0.0", PluginInterface::Source)?;
        fixture.install("com.example.archive", "1.0.0", PluginInterface::Sink)?;
        Ok(fixture)
    }

    pub(in crate::runner) fn install(
        &mut self,
        program_name: &str,
        exact_version: &str,
        interface: PluginInterface,
    ) -> io::Result<()> {
        let interface_name = match interface {
            PluginInterface::Source => "source",
            PluginInterface::Sink => "sink",
            PluginInterface::SourceAndSink => "source-and-sink",
        };
        let command = controlled_program_command(interface)?;
        let command = command
            .iter()
            .map(|argument| shell_quote(argument))
            .collect::<Vec<_>>()
            .join(" ");
        let manifest = json!({"programName": program_name, "exactVersion": exact_version, "interface": interface_name, "platforms": [crate::runner::plugin::platform::Platform::CURRENT], "displayName": "Example Plugin", "description": "Read and write example records.", "command": ["./bin/start"]});
        let package = archive(vec![
            ("manifest.json", serde_json::to_vec(&manifest)?),
            ("config.schema.json", valid_config_schema()),
            (
                "payload.descriptor.pb",
                valid_program_descriptor(interface)?,
            ),
            (
                "bin/start",
                format!("#!/bin/sh\nexec {command} \"$@\"\n").into_bytes(),
            ),
        ])?;
        self.store
            .install(Cursor::new(package))
            .map_err(io::Error::other)?;
        Ok(())
    }

    pub(in crate::runner) fn resolve(
        &self,
        document: &Value,
    ) -> io::Result<Option<Arc<PipelineLifecycleTarget>>> {
        let source = serde_json::to_vec(document)?;
        let parsed = UnverifiedTenonDocument::parse(&source).map_err(io::Error::other)?;
        let verified = Arc::new(self.verifier.verify(parsed).map_err(|issues| {
            io::Error::other(format!("invalid lifecycle fixture: {issues:?}"))
        })?);
        Ok(match self.resolver.resolve(&verified, &self.store) {
            RuntimeResolution::Ready(plan) => Some(Arc::new(PipelineLifecycleTarget::new(
                plan,
                TenonDocumentEtag::for_source(&source),
            ))),
            RuntimeResolution::Unready(issues) => {
                assert!(
                    !issues.is_empty(),
                    "unready resolution must explain missing runtime material"
                );
                None
            }
        })
    }
}

pub(super) struct RevisionFixture {
    store: PluginProgramStore,
    verifier: TenonDocumentVerifier,
    resolver: RuntimeResolver,
    _directory: tempfile::TempDir,
}

impl RevisionFixture {
    pub(super) fn new(programs: &[&str]) -> io::Result<Self> {
        let directory = tempfile::tempdir()?;
        let mut store = PluginProgramStore::recover(
            directory.path().to_owned(),
            directory.path().to_owned(),
            std::sync::Arc::new(crate::runner::extensions::ByPass),
        )
        .map_err(io::Error::other)?;
        let descriptor = valid_program_descriptor(PluginInterface::SourceAndSink)?;
        for name in programs {
            let manifest = json!({"programName": name, "exactVersion": "1.0.0", "interface": "source-and-sink", "platforms": [crate::runner::plugin::platform::Platform::CURRENT], "displayName": "Example Plugin", "description": "Read and write example records.", "command": ["./bin/start"]});
            let package = archive(vec![
                ("manifest.json", serde_json::to_vec(&manifest)?),
                ("config.schema.json", valid_config_schema()),
                ("payload.descriptor.pb", descriptor.clone()),
                (
                    "bin/start",
                    b"not executed by revision retention tests".to_vec(),
                ),
            ])?;
            store
                .install(Cursor::new(package))
                .map_err(io::Error::other)?;
        }
        let memory = NonZeroUsize::new(4 * 1024 * 1024)
            .ok_or_else(|| io::Error::other("memory limit must be positive"))?;
        let limits = ScriptVmLimits::try_new(memory, Duration::from_millis(100))
            .map_err(io::Error::other)?;
        Ok(Self {
            store,
            verifier: TenonDocumentVerifier::try_new(limits).map_err(io::Error::other)?,
            resolver: RuntimeResolver::new(limits, process_resources::available_cpu_count()?),
            _directory: directory,
        })
    }

    pub(super) fn target(
        &self,
        program_name: &str,
        comment: &str,
    ) -> io::Result<Arc<PipelineLifecycleTarget>> {
        let document = json!({
            "specVersion": "1", "id": "revision-retention",
            "pluginInstances": {
                "left": {"programName": program_name, "exactVersion": "1.0.0", "config": {"endpoint": "private left value"}},
                "right": {"programName": program_name, "exactVersion": "1.0.0", "config": {"endpoint": "private right value"}}
            },
            "flows": {
                "forward": {"parallelism": 1, "source": "left", "process": {"script": "function main(event) emit() end"}, "sinks": ["right"]},
                "reverse": {"parallelism": 1, "source": "right", "process": {"script": "function main(event) emit() end"}, "sinks": ["left"]}
            }
        });
        let source = format!("// {comment}\n{}", serde_json::to_string(&document)?);
        let parsed = UnverifiedTenonDocument::parse(source.as_bytes()).map_err(io::Error::other)?;
        let verified = Arc::new(
            self.verifier
                .verify(parsed)
                .map_err(|issues| io::Error::other(format!("invalid fixture: {issues:?}")))?,
        );
        let RuntimeResolution::Ready(plan) = self.resolver.resolve(&verified, &self.store) else {
            return Err(io::Error::other("fixture is not runtime ready"));
        };
        Ok(Arc::new(PipelineLifecycleTarget::new(
            plan,
            TenonDocumentEtag::for_source(source.as_bytes()),
        )))
    }

    pub(super) fn assert_in_use(&mut self, name: &str) -> io::Result<()> {
        let (name, version) = identity(name)?;
        assert!(matches!(
            self.store.uninstall(&name, &version),
            Err(PluginStoreError::ProgramInUse)
        ));
        let entry = self
            .store
            .lookup(&name, &version)
            .ok_or_else(|| io::Error::other("in-use Program disappeared"))?;
        assert!(entry.directory().is_dir());
        Ok(())
    }

    pub(super) fn uninstall(&mut self, name: &str) -> io::Result<()> {
        let (name, version) = identity(name)?;
        let directory = self
            .store
            .lookup(&name, &version)
            .ok_or_else(|| io::Error::other("Program disappeared before uninstall"))?
            .directory()
            .to_owned();
        assert_eq!(
            self.store
                .uninstall(&name, &version)
                .map_err(io::Error::other)?,
            PluginUninstallOutcome::Uninstalled
        );
        assert!(self.store.lookup(&name, &version).is_none());
        assert!(!directory.exists());
        Ok(())
    }
}

// These probes check lifecycle target retention. Exact Store-entry ownership is
// exercised independently through real uninstall attempts in revision tests.
pub(in crate::runner) struct TargetReferenceProbe(Weak<PipelineLifecycleTarget>);

impl TargetReferenceProbe {
    pub(in crate::runner) fn new(target: &Arc<PipelineLifecycleTarget>) -> Self {
        Self(Arc::downgrade(target))
    }

    pub(in crate::runner) fn assert_retained(&self) -> io::Result<()> {
        self.0
            .upgrade()
            .map(drop)
            .ok_or_else(|| io::Error::other("Target was released too early"))
    }

    pub(in crate::runner) fn assert_released(&self) -> io::Result<()> {
        if self.0.upgrade().is_none() {
            Ok(())
        } else {
            Err(io::Error::other("Obsolete target was retained"))
        }
    }
}

pub(in crate::runner) fn applied_state(
    target: &PipelineLifecycleTarget,
    snapshot: PipelineStatusSnapshot,
) -> PipelineRunningState {
    target.running_state(snapshot)
}

pub(in crate::runner) fn snapshot(state: &PipelineRunningState) -> &PipelineStatusSnapshot {
    state.snapshot()
}

pub(in crate::runner) fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

pub(super) fn instance_status(id: &str, state: PluginInstanceState) -> PluginInstanceStatus {
    PluginInstanceStatus {
        id: id.to_owned(),
        state: state as i32,
        last_error: match state {
            PluginInstanceState::StartFailed | PluginInstanceState::RestartBackoff => {
                Some(ControlError {
                    code: String::from("start_failed"),
                    message: String::from("Program could not start"),
                })
            }
            PluginInstanceState::Starting | PluginInstanceState::Running => None,
        },
    }
}

pub(super) fn revision_status(etag: &str, state: PluginInstanceState) -> PipelineStatusSnapshot {
    PipelineStatusSnapshot {
        document_etag: etag.to_owned(),
        plugin_instances: vec![
            instance_status("left", state),
            instance_status("right", state),
        ],
    }
}

fn identity(name: &str) -> io::Result<(ProgramName, ExactVersion)> {
    Ok((
        ProgramName::try_from(name).map_err(io::Error::other)?,
        ExactVersion::try_from("1.0.0").map_err(io::Error::other)?,
    ))
}

pub(in crate::runner) use super::launch_registry::PendingPipelineLaunch;
