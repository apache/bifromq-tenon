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

use super::PipelineProgramSnapshot;
use crate::contracts::core::PluginInterface as ProtocolPluginInterface;
use crate::identifiers::{ExactVersion, PluginProgramIdentity, ProgramName};
use crate::payload_contract::PluginInterface;
use crate::runner::plugin::package::tests::{
    equivalent_source_program_package, valid_program_package, valid_source_program_package,
};
use crate::runner::plugin::store::{PluginProgramInstallResult, PluginStoreError};
use crate::runner::plugin::store::{PluginProgramStore, PluginUninstallOutcome};
use std::collections::HashMap;
use std::fs;
use std::io::{self, Cursor};
use std::path::Path;
use tempfile::TempDir;

#[test]
fn all_interfaces_share_one_sorted_runtime_per_exact_program() -> io::Result<()> {
    let (_directory, mut store) = empty_store()?;
    for interface in [
        PluginInterface::Source,
        PluginInterface::Sink,
        PluginInterface::SourceAndSink,
    ] {
        store
            .install(Cursor::new(valid_program_package(interface)?))
            .map_err(io::Error::other)?;
    }
    let source = identity("com.example.source")?;
    let sink = identity("com.example.sink")?;
    let gateway = identity("com.example.gateway")?;
    let snapshot = capture_programs(
        &store,
        [source.clone(), sink, gateway.clone(), source, gateway],
    )?;

    let runtimes = snapshot.runtimes();
    assert_eq!(
        runtimes
            .iter()
            .map(|runtime| runtime.program_name.as_str())
            .collect::<Vec<_>>(),
        [
            "com.example.gateway",
            "com.example.sink",
            "com.example.source"
        ],
    );
    for (runtime, interface) in runtimes.iter().zip([
        ProtocolPluginInterface::SourceAndSink,
        ProtocolPluginInterface::Sink,
        ProtocolPluginInterface::Source,
    ]) {
        let selected = identity(&runtime.program_name)?;
        let entry = store
            .lookup(selected.program_name(), selected.exact_version())
            .ok_or_else(|| io::Error::other("selected Program is missing"))?;
        assert_eq!(runtime.exact_version, "1.0.0");
        assert_eq!(Path::new(&runtime.program_directory), entry.directory());
        assert_eq!(runtime.command, ["./bin/start"]);
        assert_eq!(runtime.command, entry.command());
        assert_eq!(runtime.plugin_interface, interface as i32);
        assert_eq!(
            runtime.payload_descriptor_set,
            entry.payload_descriptor_bytes(),
        );
        assert_eq!(
            runtime.payload_descriptor_set,
            fs::read(entry.directory().join("payload.descriptor.pb"))?,
        );
    }
    assert_eq!(snapshot.runtimes(), runtimes);
    assert!(capture_programs(&store, [])?.runtimes().is_empty());
    Ok(())
}

#[test]
fn only_the_last_snapshot_release_allows_selected_program_uninstall() -> io::Result<()> {
    let (_directory, mut store) = empty_store()?;
    for interface in [PluginInterface::Source, PluginInterface::Sink] {
        store
            .install(Cursor::new(valid_program_package(interface)?))
            .map_err(io::Error::other)?;
    }
    let source = identity("com.example.source")?;
    let sink = identity("com.example.sink")?;
    let first = capture_programs(&store, [source.clone(), source.clone()])?;
    let second = capture_programs(&store, [source.clone()])?;
    let source_directory = std::path::PathBuf::from(&first.runtimes()[0].program_directory);
    let sink_directory = store
        .lookup(sink.program_name(), sink.exact_version())
        .ok_or_else(|| io::Error::other("Sink Program is missing"))?
        .directory()
        .to_path_buf();

    assert!(matches!(
        store.uninstall(source.program_name(), source.exact_version()),
        Err(PluginStoreError::ProgramInUse),
    ));
    assert!(source_directory.is_dir());
    assert!(
        store
            .lookup(source.program_name(), source.exact_version())
            .is_some()
    );
    assert_eq!(
        store
            .uninstall(sink.program_name(), sink.exact_version())
            .map_err(io::Error::other)?,
        PluginUninstallOutcome::Uninstalled,
    );
    assert!(
        store
            .lookup(sink.program_name(), sink.exact_version())
            .is_none()
    );
    assert!(!sink_directory.exists());

    drop(first);
    assert!(matches!(
        store.uninstall(source.program_name(), source.exact_version()),
        Err(PluginStoreError::ProgramInUse),
    ));
    assert_eq!(fs::read(source_directory.join("bin/start"))?, b"program");

    drop(second);
    assert_eq!(
        store
            .uninstall(source.program_name(), source.exact_version())
            .map_err(io::Error::other)?,
        PluginUninstallOutcome::Uninstalled,
    );
    assert!(
        store
            .lookup(source.program_name(), source.exact_version())
            .is_none()
    );
    assert!(!source_directory.exists());
    Ok(())
}

#[test]
fn equivalent_install_and_unrelated_changes_preserve_snapshot_material_and_retention()
-> io::Result<()> {
    let (_directory, mut store) = empty_store()?;
    store
        .install(Cursor::new(valid_source_program_package()?))
        .map_err(io::Error::other)?;
    let source = identity("com.example.source")?;
    let snapshot = capture_programs(&store, [source.clone()])?;
    let original_runtimes = snapshot.runtimes();

    let retry = store
        .install(Cursor::new(equivalent_source_program_package()?))
        .map_err(io::Error::other)?;
    assert!(matches!(retry, PluginProgramInstallResult::Unchanged(identity) if identity == source));
    store
        .install(Cursor::new(valid_program_package(PluginInterface::Sink)?))
        .map_err(io::Error::other)?;
    assert_eq!(snapshot.runtimes(), original_runtimes);
    assert!(matches!(
        store.uninstall(source.program_name(), source.exact_version()),
        Err(PluginStoreError::ProgramInUse),
    ));

    drop(snapshot);
    assert_eq!(
        store
            .uninstall(source.program_name(), source.exact_version())
            .map_err(io::Error::other)?,
        PluginUninstallOutcome::Uninstalled,
    );
    let sink = identity("com.example.sink")?;
    assert!(
        store
            .lookup(sink.program_name(), sink.exact_version())
            .is_some()
    );
    Ok(())
}

fn empty_store() -> io::Result<(TempDir, PluginProgramStore)> {
    let directory = tempfile::tempdir()?;
    let store = PluginProgramStore::recover(
        directory.path().to_owned(),
        directory.path().to_owned(),
        std::sync::Arc::new(crate::runner::extensions::ByPass),
    )
    .map_err(io::Error::other)?;
    Ok((directory, store))
}

fn identity(program_name: &str) -> io::Result<PluginProgramIdentity> {
    Ok(PluginProgramIdentity::from_parts(
        ProgramName::try_from(program_name.to_owned()).map_err(io::Error::other)?,
        ExactVersion::try_from(String::from("1.0.0")).map_err(io::Error::other)?,
    ))
}

fn capture_programs(
    store: &PluginProgramStore,
    identities: impl IntoIterator<Item = PluginProgramIdentity>,
) -> io::Result<PipelineProgramSnapshot> {
    let identities: Vec<_> = identities.into_iter().collect();
    let programs = identities
        .iter()
        .map(|identity| {
            store
                .lookup(identity.program_name(), identity.exact_version())
                .map(|entry| (identity, entry))
                .ok_or_else(|| io::Error::other("Required Program fixture is absent"))
        })
        .collect::<io::Result<HashMap<_, _>>>()?;
    Ok(PipelineProgramSnapshot::retain(programs))
}
