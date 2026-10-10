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

use super::*;
use crate::runner::extensions::{ArtifactProtection, ByPass, PluginPackageOutput};
use std::io::Read;

struct RejectAfterWrite;

impl ArtifactProtection for RejectAfterWrite {
    fn open_plugin_package(
        &self,
        source: &mut dyn Read,
        output: &mut dyn PluginPackageOutput,
    ) -> io::Result<()> {
        ByPass.open_plugin_package(source, output)?;
        Err(io::Error::from(io::ErrorKind::PermissionDenied))
    }
}

#[test]
fn access_failure_discards_output_and_saved_damage_stops_recovery() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let store_dir = prepare_store_root(root.path())?;
    let runtime = tempfile::tempdir()?;
    let mut store = PluginProgramStore::recover(
        store_dir.clone(),
        runtime.path().to_path_buf(),
        Arc::new(RejectAfterWrite),
    )
    .map_err(io::Error::other)?;
    let bytes = valid_source_program_package()?;
    assert!(matches!(
        store.install(Cursor::new(&bytes)),
        Err(PluginStoreError::PackageAccessRejected)
    ));
    assert_eq!(store_dir.read_dir()?.count(), 0);
    assert_eq!(runtime.path().read_dir()?.count(), 0);
    store.protection = Arc::new(crate::runner::extensions::ByPass);
    store
        .install(Cursor::new(&bytes))
        .map_err(io::Error::other)?;
    let path = store_dir.join("com.example.source/.tenon-artifact-92521fc3cbd964bdc9f584a991b89fddaa5754ed1cc96d6d42445338669c1305");
    fs::write(&path, b"damaged")?;
    assert!(matches!(
        store.install(Cursor::new(bytes)),
        Err(PluginStoreError::StoreIntegrityInvalid { .. })
    ));
    let protection = Arc::clone(&store.protection);
    drop(store);
    let recovery = PluginProgramStore::recover(store_dir, runtime.path().to_path_buf(), protection);
    assert!(matches!(
        recovery,
        Err(PluginStoreError::InstalledPackageInvalid { .. })
    ));
    assert_eq!(fs::read(path)?, b"damaged");
    Ok(())
}

#[test]
fn long_versions_install_recover_and_uninstall_original_packages() -> io::Result<()> {
    let version =
        ExactVersion::try_from(format!("1.0.0-{}", "a".repeat(240))).map_err(io::Error::other)?;
    let bytes = source_package_version(&version)?;
    let root = tempfile::tempdir()?;
    let store_dir = prepare_store_root(root.path())?;
    let runtime = tempfile::tempdir()?;
    let protection: Arc<dyn ArtifactProtection> = Arc::new(crate::runner::extensions::ByPass);
    let mut store = PluginProgramStore::recover(
        store_dir.clone(),
        runtime.path().to_path_buf(),
        Arc::clone(&protection),
    )
    .map_err(io::Error::other)?;
    assert!(matches!(
        store
            .install(Cursor::new(&bytes))
            .map_err(io::Error::other)?,
        PluginProgramInstallResult::Installed(_)
    ));
    assert!(matches!(
        store
            .install(Cursor::new(&bytes))
            .map_err(io::Error::other)?,
        PluginProgramInstallResult::Unchanged(_)
    ));
    drop(store);
    drop(runtime);

    let runtime = tempfile::tempdir()?;
    let mut store =
        PluginProgramStore::recover(store_dir.clone(), runtime.path().to_path_buf(), protection)
            .map_err(io::Error::other)?;
    let name = source_program_name()?;
    let entry = store
        .lookup(&name, &version)
        .ok_or_else(|| io::Error::other("Program is missing after recovery"))?;
    let material = entry.directory().to_path_buf();
    assert_eq!(fs::read(material.join("bin/start"))?, b"program");
    assert_eq!(
        store.uninstall(&name, &version).map_err(io::Error::other)?,
        PluginUninstallOutcome::Uninstalled
    );
    assert!(store.lookup(&name, &version).is_none());
    assert!(!material.exists());
    assert_eq!(store_dir.join(name.as_str()).read_dir()?.count(), 0);
    Ok(())
}

#[test]
fn original_filename_must_match_the_validated_version() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let store_dir = prepare_store_root(root.path())?;
    let runtime = tempfile::tempdir()?;
    let protection: Arc<dyn ArtifactProtection> = Arc::new(crate::runner::extensions::ByPass);
    let mut store = PluginProgramStore::recover(
        store_dir.clone(),
        runtime.path().to_path_buf(),
        Arc::clone(&protection),
    )
    .map_err(io::Error::other)?;
    let bytes = valid_source_program_package()?;
    store
        .install(Cursor::new(&bytes))
        .map_err(io::Error::other)?;
    drop(store);
    let namespace = store_dir.join("com.example.source");
    let saved = namespace
        .read_dir()?
        .next()
        .ok_or_else(|| io::Error::other("Saved package is missing"))??
        .path();
    let wrong_path = namespace.join(format!(".tenon-artifact-{}", "0".repeat(64)));
    fs::rename(saved, &wrong_path)?;
    assert!(matches!(
        PluginProgramStore::recover(store_dir, runtime.path().to_path_buf(), protection),
        Err(PluginStoreError::StoreIntegrityInvalid { path }) if path == wrong_path
    ));
    assert_eq!(fs::read(wrong_path)?, bytes);
    Ok(())
}

fn source_package_version(version: &ExactVersion) -> io::Result<Vec<u8>> {
    use crate::runner::plugin::package::tests::{
        archive, valid_config_schema, valid_program_descriptor,
    };
    let manifest = serde_json::to_vec(&serde_json::json!({
        "programName": "com.example.source",
        "exactVersion": version.as_str(),
        "interface": "source",
        "platforms": [crate::runner::plugin::platform::Platform::CURRENT],
        "displayName": "Example Plugin",
        "description": "Read example records.",
        "command": ["./bin/start"]
    }))?;
    archive(vec![
        ("manifest.json", manifest),
        ("config.schema.json", valid_config_schema()),
        (
            "payload.descriptor.pb",
            valid_program_descriptor(PluginInterface::Source)?,
        ),
        ("bin/start", b"program".to_vec()),
    ])
}
