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

use super::PluginProgramInstallResult;
use super::filesystem::{DurablePublicationFilesystem, sync_directory};
use super::test_support::program_count;
use super::{
    PluginProgramEntry, PluginProgramStore, PluginStoreError, PluginUninstallOutcome,
    PublicationFilesystem,
};
use crate::identifiers::{ExactVersion, PluginProgramIdentity, ProgramName};
use crate::payload_contract::PluginInterface;
use crate::runner::plugin::package::tests::STAGING_DIRECTORY_PREFIX;
use crate::runner::plugin::package::tests::{
    equivalent_source_program_package, source_program_package_with_command,
    source_program_package_with_resource, valid_program_package, valid_source_program_package,
};
use std::cell::Cell;
use std::fs;
use std::io::{self, Cursor};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

#[test]
fn first_install_publishes_disk_and_memory_in_one_store_mutation() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;

    let package = valid_source_program_package()?;
    let outcome = store
        .install(Cursor::new(&package))
        .map_err(io::Error::other)?;
    let PluginProgramInstallResult::Installed(identity) = outcome else {
        return Err(io::Error::other("first install was reported as unchanged"));
    };
    assert_eq!(
        identity,
        PluginProgramIdentity::from_parts(source_program_name()?, exact_version()?)
    );
    let entry = available_entry(&store, "com.example.source")?;

    assert!(entry.directory().starts_with(&store.runtime_directory));
    assert!(!entry.directory().starts_with(&root));
    assert_eq!(fs::read(&entry.original.path)?, package);
    assert_eq!(fs::read(entry.directory().join("bin/start"))?, b"program");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        for directory in [
            &root,
            &root.join("com.example.source"),
            entry.directory(),
            &entry.directory().join("bin"),
            &entry.directory().join("resources"),
        ] {
            assert_eq!(fs::metadata(directory)?.permissions().mode() & 0o777, 0o700);
        }
        for file in [
            "manifest.json",
            "config.schema.json",
            "payload.descriptor.pb",
            "bin/start",
            "resources/data.bin",
        ] {
            assert_eq!(
                fs::metadata(entry.directory().join(file))?
                    .permissions()
                    .mode()
                    & 0o777,
                0o500
            );
        }
    }
    assert_available(&store, "com.example.source")?;
    Ok(())
}

#[test]
fn equivalent_retry_reuses_the_same_entry_arc() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root)?;
    store
        .install(Cursor::new(valid_source_program_package()?))
        .map_err(io::Error::other)?;
    let first = Arc::clone(available_entry(&store, "com.example.source")?);

    let retry = store
        .install(Cursor::new(equivalent_source_program_package()?))
        .map_err(io::Error::other)?;
    let PluginProgramInstallResult::Unchanged(identity) = retry else {
        return Err(io::Error::other(
            "equivalent retry was reported as installed",
        ));
    };

    assert_eq!(
        identity,
        PluginProgramIdentity::from_parts(source_program_name()?, exact_version()?)
    );
    assert!(Arc::ptr_eq(
        &first,
        available_entry(&store, "com.example.source")?
    ));
    Ok(())
}

#[test]
fn same_identity_with_different_content_is_never_overwritten() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    store
        .install(Cursor::new(valid_source_program_package()?))
        .map_err(io::Error::other)?;

    let error = store
        .install(Cursor::new(source_program_package_with_command(
            "./bin/other",
        )?))
        .err()
        .ok_or_else(|| io::Error::other("different Program content was accepted"))?;

    assert_eq!(error.code(), "plugin_version_conflict");
    assert!(
        fs::read_to_string(
            available_entry(&store, "com.example.source")?
                .directory()
                .join("manifest.json")
        )?
        .contains("./bin/start")
    );
    assert_available(&store, "com.example.source")?;
    Ok(())
}

#[test]
fn recovery_owns_all_interfaces_in_one_identity_map() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    for interface in [
        PluginInterface::Source,
        PluginInterface::Sink,
        PluginInterface::SourceAndSink,
    ] {
        store
            .install(Cursor::new(valid_program_package(interface)?))
            .map_err(io::Error::other)?;
    }

    let old_runtime = store.runtime_directory.clone();
    drop(store);
    fs::remove_dir_all(old_runtime)?;
    let recovered = recover_store(root)?;

    assert_eq!(program_count(&recovered), 3);
    for (program_name, interface) in [
        ("com.example.source", PluginInterface::Source),
        ("com.example.sink", PluginInterface::Sink),
        ("com.example.gateway", PluginInterface::SourceAndSink),
    ] {
        let entry = available_entry(&recovered, program_name)?;
        assert_eq!(entry.interface(), interface);
        assert_eq!(entry.command(), ["./bin/start"]);
        assert_eq!(
            entry.manifest_bytes.as_ref(),
            fs::read(entry.directory().join("manifest.json"))?
        );
        assert_eq!(
            entry.config_schema_bytes(),
            fs::read(entry.directory().join("config.schema.json"))?
        );
        assert_eq!(
            entry.payload_descriptor_bytes(),
            fs::read(entry.directory().join("payload.descriptor.pb"))?
        );
    }
    Ok(())
}

#[test]
fn recovery_preserves_invalid_original_packages_and_valid_siblings() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    for interface in [PluginInterface::Source, PluginInterface::Sink] {
        store
            .install(Cursor::new(valid_program_package(interface)?))
            .map_err(io::Error::other)?;
    }
    let invalid = available_entry(&store, "com.example.sink")?
        .original
        .path
        .clone();
    let valid = available_entry(&store, "com.example.source")?
        .original
        .path
        .clone();
    let valid_bytes = fs::read(&valid)?;
    drop(store);
    fs::write(&invalid, b"invalid package")?;
    assert!(recover_store(root).is_err());
    assert_eq!(fs::read(invalid)?, b"invalid package");
    assert_eq!(fs::read(valid)?, valid_bytes);
    Ok(())
}

#[test]
fn healthy_namespace_absence_is_missing_without_a_saved_state() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let namespace = root.join("com.example.source");
    fs::create_dir(&namespace)?;
    make_private_directory(&namespace)?;

    let store = recover_store(root)?;

    assert!(lookup(&store, "com.example.source")?.is_none());
    assert_eq!(program_count(&store), 0);
    Ok(())
}

#[cfg(unix)]
#[test]
fn recovery_preserves_original_packages_with_identity_permission_or_link_errors() -> io::Result<()>
{
    use std::os::unix::fs::{PermissionsExt as _, symlink};
    enum Corruption {
        Identity,
        Permission,
        SymbolicLink,
        HardLink,
    }
    for corruption in [
        Corruption::Identity,
        Corruption::Permission,
        Corruption::SymbolicLink,
        Corruption::HardLink,
    ] {
        let parent = tempfile::tempdir()?;
        let root = prepare_store_root(parent.path())?;
        let mut store = recover_store(root.clone())?;
        store
            .install(Cursor::new(valid_source_program_package()?))
            .map_err(io::Error::other)?;
        let saved = available_entry(&store, "com.example.source")?
            .original
            .path
            .clone();
        drop(store);
        let outside = parent.path().join("outside-package");
        fs::write(&outside, b"external-owner")?;
        make_private_file(&outside)?;
        let invalid = match corruption {
            Corruption::Identity => {
                let destination =
                    saved.with_file_name(format!(".tenon-artifact-{}", "0".repeat(64)));
                fs::rename(&saved, &destination)?;
                destination
            }
            Corruption::Permission => {
                fs::set_permissions(&saved, fs::Permissions::from_mode(0o644))?;
                saved
            }
            Corruption::SymbolicLink => {
                fs::remove_file(&saved)?;
                symlink(&outside, &saved)?;
                saved
            }
            Corruption::HardLink => {
                fs::remove_file(&saved)?;
                fs::hard_link(&outside, &saved)?;
                saved
            }
        };
        assert!(recover_store(root).is_err());
        assert!(fs::symlink_metadata(invalid).is_ok());
        assert_eq!(fs::read(&outside)?, b"external-owner");
        assert_eq!(fs::metadata(&outside)?.permissions().mode() & 0o777, 0o500);
    }
    Ok(())
}

#[test]
fn recovery_deletes_a_broken_namespace_and_allows_a_new_install() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let namespace = root.join("com.example.source");
    fs::write(&namespace, b"not a directory")?;

    let mut store = recover_store(root)?;

    assert!(lookup(&store, "com.example.source")?.is_none());
    assert!(!namespace.exists());
    store
        .install(Cursor::new(valid_source_program_package()?))
        .map_err(io::Error::other)?;
    assert_available(&store, "com.example.source")?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn install_reports_namespace_drift_without_repairing_it() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    let namespace = root.join("com.example.source");
    std::os::unix::fs::symlink("com.example.source", &namespace)?;

    let error = store
        .install(Cursor::new(valid_source_program_package()?))
        .err()
        .ok_or_else(|| io::Error::other("broken Program namespace was treated as missing"))?;

    assert_eq!(error.code(), "plugin_store_integrity_invalid");
    assert!(fs::symlink_metadata(namespace)?.is_symlink());
    Ok(())
}

#[test]
fn recovery_deletes_unknown_root_and_version_entries() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let unknown = root.join("unknown");
    fs::write(&unknown, b"unexpected")?;
    let namespace = root.join("com.example.source");
    fs::create_dir(&namespace)?;
    make_private_directory(&namespace)?;
    let latest = namespace.join("latest");
    fs::write(&latest, b"unexpected")?;

    let store = recover_store(root)?;

    assert!(!unknown.exists());
    assert!(!latest.exists());
    assert!(lookup(&store, "com.example.source")?.is_none());
    Ok(())
}

#[test]
fn recovery_removes_owned_staging_directories() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let stale = root.join(format!("{STAGING_DIRECTORY_PREFIX}crashed"));
    fs::create_dir(&stale)?;
    make_private_directory(&stale)?;

    let store = recover_store(root)?;

    assert!(!stale.exists());
    assert_eq!(program_count(&store), 0);
    Ok(())
}

#[test]
fn recovery_removes_a_staging_prefix_file() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let staging_file = root.join(format!("{STAGING_DIRECTORY_PREFIX}not-a-directory"));
    fs::write(&staging_file, b"unexpected")?;

    let store = recover_store(root)?;

    assert!(!staging_file.exists());
    assert_eq!(program_count(&store), 0);
    Ok(())
}

#[test]
fn install_failures_before_rename_recover_without_publishing_an_entry() -> io::Result<()> {
    for fail_at in [
        PublicationPoint::SyncProgramDirectory,
        PublicationPoint::SyncStoreRoot,
        PublicationPoint::SyncPackageFile,
        PublicationPoint::Rename,
    ] {
        let parent = tempfile::tempdir()?;
        let root = prepare_store_root(parent.path())?;
        let mut store = recover_store(root.clone())?;
        let publication = FaultingPublicationFilesystem::new(fail_at);
        let package = valid_source_program_package()?;

        let error = store
            .install_with_publication_filesystem(Cursor::new(package.clone()), &publication)
            .err()
            .ok_or_else(|| io::Error::other("injected pre-rename failure was ignored"))?;

        assert_eq!(error.code(), "plugin_store_internal_error");
        assert!(std::error::Error::source(&error).is_some());
        assert_eq!(program_count(&store), 0);
        assert_eq!(fs::read_dir(&store.runtime_directory)?.count(), 0);
        drop(store);
        assert!(!source_package_path(&root)?.exists());
        let mut recovered = recover_store(root)?;
        assert!(lookup(&recovered, "com.example.source")?.is_none());
        let retry = recovered
            .install(Cursor::new(package))
            .map_err(io::Error::other)?;
        assert!(matches!(retry, PluginProgramInstallResult::Installed(_)));
    }
    Ok(())
}

#[test]
fn install_failure_after_rename_recovers_the_published_program() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    let package = valid_source_program_package()?;
    let publication = FaultingPublicationFilesystem::new(PublicationPoint::SyncPublishedDirectory);

    let error = store
        .install_with_publication_filesystem(Cursor::new(package.clone()), &publication)
        .err()
        .ok_or_else(|| io::Error::other("injected post-rename failure was ignored"))?;

    assert_eq!(error.code(), "plugin_store_internal_error");
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(program_count(&store), 0);
    assert_eq!(fs::read_dir(&store.runtime_directory)?.count(), 0);
    assert_eq!(fs::read(source_package_path(&root)?)?, package);
    drop(store);
    let mut recovered = recover_store(root)?;
    assert_available(&recovered, "com.example.source")?;
    let retry = recovered
        .install(Cursor::new(package))
        .map_err(io::Error::other)?;
    assert!(matches!(retry, PluginProgramInstallResult::Unchanged(_)));
    Ok(())
}

#[test]
fn install_reports_external_deletion_and_retains_entry_ownership_until_shutdown() -> io::Result<()>
{
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    let package = valid_source_program_package()?;
    store
        .install(Cursor::new(package.clone()))
        .map_err(io::Error::other)?;
    let entry = Arc::clone(available_entry(&store, "com.example.source")?);
    let weak = Arc::downgrade(&entry);
    fs::remove_file(source_package_path(&root)?)?;

    let error = store
        .install(Cursor::new(package))
        .err()
        .ok_or_else(|| io::Error::other("external deletion was silently repaired"))?;

    assert_eq!(error.code(), "plugin_store_internal_error");
    drop(entry);
    assert!(weak.upgrade().is_some());
    drop(store);
    assert!(weak.upgrade().is_none());
    Ok(())
}

#[test]
fn install_reports_external_byte_drift_before_conflict_comparison() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    store
        .install(Cursor::new(valid_source_program_package()?))
        .map_err(io::Error::other)?;
    let resource = available_entry(&store, "com.example.source")?
        .directory()
        .join("resources/data.bin");
    make_writable(&resource)?;
    fs::write(&resource, b"beta")?;
    make_private_file(&resource)?;

    let error = store
        .install(Cursor::new(source_program_package_with_resource(
            "resources/data.bin",
            b"gamma",
        )?))
        .err()
        .ok_or_else(|| io::Error::other("external byte drift was treated as a conflict"))?;

    assert_eq!(error.code(), "plugin_store_integrity_invalid");
    assert_eq!(fs::read(&resource)?, b"beta");
    Ok(())
}

#[test]
fn uninstall_removes_disk_and_memory_in_one_store_mutation() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    store
        .install(Cursor::new(valid_source_program_package()?))
        .map_err(io::Error::other)?;
    let program_name = source_program_name()?;
    let exact_version = exact_version()?;

    let outcome = store
        .uninstall(&program_name, &exact_version)
        .map_err(io::Error::other)?;

    assert_eq!(outcome, PluginUninstallOutcome::Uninstalled);
    assert!(store.lookup(&program_name, &exact_version).is_none());
    assert!(!source_package_path(&root)?.exists());
    assert!(!deletion_tombstone(&root).exists());
    Ok(())
}

#[test]
fn uninstall_rejects_an_entry_with_an_external_reference() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    store
        .install(Cursor::new(valid_source_program_package()?))
        .map_err(io::Error::other)?;
    let entry = Arc::clone(available_entry(&store, "com.example.source")?);
    let program_name = source_program_name()?;
    let exact_version = exact_version()?;

    let error = store
        .uninstall(&program_name, &exact_version)
        .err()
        .ok_or_else(|| io::Error::other("referenced Program was uninstalled"))?;

    assert_eq!(error.code(), "plugin_in_use");
    assert!(source_package_path(&root)?.is_file());
    assert!(matches!(
        store.lookup(&program_name, &exact_version),
        Some(current) if Arc::ptr_eq(current, &entry)
    ));
    Ok(())
}

#[test]
fn uninstall_rejects_a_weak_reference_until_it_is_released() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    store
        .install(Cursor::new(valid_source_program_package()?))
        .map_err(io::Error::other)?;
    let entry = Arc::clone(available_entry(&store, "com.example.source")?);
    let weak = Arc::downgrade(&entry);
    drop(entry);

    let error = store
        .uninstall(&source_program_name()?, &exact_version()?)
        .err()
        .ok_or_else(|| io::Error::other("weakly referenced Program was uninstalled"))?;

    assert_eq!(error.code(), "plugin_in_use");
    assert!(source_package_path(&root)?.is_file());
    drop(weak);
    assert_eq!(
        store
            .uninstall(&source_program_name()?, &exact_version()?)
            .map_err(io::Error::other)?,
        PluginUninstallOutcome::Uninstalled,
    );
    Ok(())
}

#[test]
fn uninstall_reports_an_exact_path_removed_outside_the_store() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    store
        .install(Cursor::new(valid_source_program_package()?))
        .map_err(io::Error::other)?;
    fs::remove_file(source_package_path(&root)?)?;

    let error = store
        .uninstall(&source_program_name()?, &exact_version()?)
        .err()
        .ok_or_else(|| io::Error::other("external deletion was silently accepted"))?;

    assert_eq!(error.code(), "plugin_store_internal_error");
    assert!(!deletion_tombstone(&root).exists());
    drop(store);
    let recovered = recover_store(root)?;
    assert!(lookup(&recovered, "com.example.source")?.is_none());
    Ok(())
}

#[test]
fn uninstall_of_a_missing_identity_is_idempotent() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;

    let missing = store
        .uninstall(&source_program_name()?, &exact_version()?)
        .map_err(io::Error::other)?;

    assert_eq!(missing, PluginUninstallOutcome::NotFound);
    assert!(fs::read_dir(root)?.next().is_none());
    Ok(())
}

#[test]
fn uninstall_failures_before_rename_recover_the_existing_program() -> io::Result<()> {
    for fail_at in [UninstallPoint::InspectTombstone, UninstallPoint::Rename] {
        let parent = tempfile::tempdir()?;
        let root = prepare_store_root(parent.path())?;
        let mut store = recover_store(root.clone())?;
        store
            .install(Cursor::new(valid_source_program_package()?))
            .map_err(io::Error::other)?;
        let filesystem = FaultingUninstallFilesystem::new(fail_at);

        let error = store
            .uninstall_with_publication_filesystem(
                &source_program_name()?,
                &exact_version()?,
                &filesystem,
            )
            .err()
            .ok_or_else(|| io::Error::other("injected uninstall failure was ignored"))?;

        assert_eq!(error.code(), "plugin_store_internal_error");
        assert!(std::error::Error::source(&error).is_some());
        drop(store);
        let mut recovered = recover_store(root.clone())?;
        assert_available(&recovered, "com.example.source")?;
        let outcome = recovered
            .uninstall(&source_program_name()?, &exact_version()?)
            .map_err(io::Error::other)?;
        assert_eq!(outcome, PluginUninstallOutcome::Uninstalled);
        assert!(!source_package_path(&root)?.exists());
    }
    Ok(())
}

#[test]
fn uninstall_failures_after_rename_recover_as_missing() -> io::Result<()> {
    for fail_at in [
        UninstallPoint::SyncParent,
        UninstallPoint::RemoveRuntime,
        UninstallPoint::RemoveTombstone,
        UninstallPoint::SyncCleanup,
    ] {
        let parent = tempfile::tempdir()?;
        let root = prepare_store_root(parent.path())?;
        let mut store = recover_store(root.clone())?;
        store
            .install(Cursor::new(valid_source_program_package()?))
            .map_err(io::Error::other)?;
        let filesystem = FaultingUninstallFilesystem::new(fail_at);

        let error = store
            .uninstall_with_publication_filesystem(
                &source_program_name()?,
                &exact_version()?,
                &filesystem,
            )
            .err()
            .ok_or_else(|| io::Error::other("injected post-rename failure was ignored"))?;

        assert_eq!(error.code(), "plugin_store_internal_error");
        assert!(std::error::Error::source(&error).is_some());
        assert!(!source_package_path(&root)?.exists());
        assert_eq!(
            deletion_tombstone(&root).exists(),
            fail_at != UninstallPoint::SyncCleanup
        );
        drop(store);
        let recovered = recover_store(root.clone())?;
        assert!(lookup(&recovered, "com.example.source")?.is_none());
        assert!(!deletion_tombstone(&root).exists());
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn recovery_removes_invalid_links_without_following_their_targets() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    store
        .install(Cursor::new(valid_source_program_package()?))
        .map_err(io::Error::other)?;
    drop(store);
    let outside = parent.path().join("outside");
    fs::create_dir(&outside)?;
    fs::write(outside.join("keep"), b"keep")?;
    let links = [
        root.join("com.example.link"),
        root.join(format!("{STAGING_DIRECTORY_PREFIX}link")),
        root.join("invalid-name"),
        root.join("com.example.source/2.0.0"),
        root.join("com.example.source/latest"),
        deletion_tombstone(&root),
    ];
    for path in &links {
        std::os::unix::fs::symlink(&outside, path)?;
    }
    let invalid_tree = root.join("invalid-tree");
    fs::create_dir(&invalid_tree)?;
    std::os::unix::fs::symlink(&outside, invalid_tree.join("link"))?;

    let recovered = recover_store(root.clone())?;

    assert_available(&recovered, "com.example.source")?;
    assert_eq!(program_count(&recovered), 1);
    for path in &links {
        let error = fs::symlink_metadata(path)
            .err()
            .ok_or_else(|| io::Error::other("invalid link was retained"))?;
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }
    assert!(!invalid_tree.exists());
    assert_eq!(fs::read(outside.join("keep"))?, b"keep");
    assert!(root.is_dir());
    Ok(())
}

#[test]
fn recovery_collects_the_namespace_before_removing_a_tombstone() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    store
        .install(Cursor::new(valid_source_program_package()?))
        .map_err(io::Error::other)?;
    let tombstone = deletion_tombstone(&root);
    fs::create_dir(&tombstone)?;
    make_private_directory(&tombstone)?;

    drop(store);
    let recovered = recover_store(root.clone())?;

    assert_available(&recovered, "com.example.source")?;
    assert!(!tombstone.exists());
    Ok(())
}

#[test]
fn incomplete_namespace_enumeration_deletes_the_whole_namespace() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    for interface in [PluginInterface::Source, PluginInterface::Sink] {
        store
            .install(Cursor::new(valid_program_package(interface)?))
            .map_err(io::Error::other)?;
    }
    drop(store);
    let namespace = root.join("com.example.source");
    let filesystem = FaultingRecoveryFilesystem {
        fail_at: RecoveryPoint::Enumerate,
        path: namespace.clone(),
    };

    let recovered = recover_with_filesystem(root, &filesystem).map_err(io::Error::other)?;

    assert!(!namespace.exists());
    assert!(lookup(&recovered, "com.example.source")?.is_none());
    assert_available(&recovered, "com.example.sink")?;
    assert_eq!(program_count(&recovered), 1);
    Ok(())
}

#[test]
fn incomplete_root_enumeration_aborts_without_deleting_the_root() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let unknown = root.join("unknown");
    fs::write(&unknown, b"unread entry")?;
    let filesystem = FaultingRecoveryFilesystem {
        fail_at: RecoveryPoint::Enumerate,
        path: root.clone(),
    };

    let error = recover_with_filesystem(root.clone(), &filesystem)
        .err()
        .ok_or_else(|| io::Error::other("incomplete root enumeration was accepted"))?;

    assert!(matches!(
        error,
        PluginStoreError::DirectoryReadFailed { .. }
    ));
    assert!(root.is_dir());
    assert!(unknown.is_file());
    Ok(())
}

#[test]
fn cleanup_and_parent_sync_failures_abort_recovery_and_can_be_retried_at_startup() -> io::Result<()>
{
    for fail_at in [RecoveryPoint::Remove, RecoveryPoint::SyncParent] {
        let parent = tempfile::tempdir()?;
        let root = prepare_store_root(parent.path())?;
        let mut store = recover_store(root.clone())?;
        store
            .install(Cursor::new(valid_source_program_package()?))
            .map_err(io::Error::other)?;
        drop(store);
        let namespace = root.join("com.example.source");
        let invalid = namespace.join("latest");
        fs::write(&invalid, b"invalid version")?;
        let filesystem = FaultingRecoveryFilesystem {
            fail_at,
            path: match fail_at {
                RecoveryPoint::Remove => invalid.clone(),
                RecoveryPoint::SyncParent => namespace,
                RecoveryPoint::Enumerate => unreachable!("only cleanup failures are tested"),
            },
        };

        let error = recover_with_filesystem(root.clone(), &filesystem)
            .err()
            .ok_or_else(|| io::Error::other("cleanup failure was ignored"))?;

        assert_eq!(error.code(), "plugin_store_internal_error");
        assert!(std::error::Error::source(&error).is_some());
        assert!(root.is_dir());
        assert_eq!(invalid.exists(), fail_at == RecoveryPoint::Remove);
        let recovered = recover_store(root)?;
        assert_available(&recovered, "com.example.source")?;
        assert!(!invalid.exists());
    }
    Ok(())
}

#[test]
fn missing_root_is_fatal_and_is_not_created() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("plugins/programs");

    let error = PluginProgramStore::recover(
        root.clone(),
        root.clone(),
        Arc::new(crate::runner::extensions::ByPass),
    )
    .err()
    .ok_or_else(|| io::Error::other("missing Program Store was treated as empty"))?;

    assert_eq!(error.code(), "plugin_store_internal_error");
    assert!(!root.exists());
    Ok(())
}

#[test]
#[ignore = "requires the pinned Java bundle toolchain"]
fn java_target_bundles_install_idempotently_and_recover() -> io::Result<()> {
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let bundles = parent.path().join("bundles");
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let java_workspace = workspace.join("sdk/java");
    let status = Command::new(java_workspace.join("mvnw"))
        .current_dir(&java_workspace)
        .args([
            "--batch-mode",
            "--no-transfer-progress",
            "-pl",
            "maven-plugin",
            "-Dtest=ProgramBundleBuilderTest#allInterfacesProduceCurrentBundles",
        ])
        .arg(format!(
            "-Dmaven.repo.local={}",
            workspace
                .join("target/verify-java-maven-repository")
                .display()
        ))
        .arg(format!(
            "-Dtenon.test.bundleOutputDirectory={}",
            bundles.display()
        ))
        .arg("test")
        .status()?;
    if !status.success() {
        return Err(io::Error::other(format!(
            "Java target bundle generation failed: {status}"
        )));
    }

    let cases = [
        ("source", "1.0.0", PluginInterface::Source),
        ("sink", "1.0.1", PluginInterface::Sink),
        ("source-and-sink", "1.0.2", PluginInterface::SourceAndSink),
    ];
    let program_name = source_program_name()?;
    let mut store = recover_store(root.clone())?;
    for (folder, version, interface) in cases {
        let version = ExactVersion::try_from(version.to_owned()).map_err(io::Error::other)?;
        let generated = bundles.join(folder);
        let archive = generated.join(format!(
            "example-{}-tenon-plugin-linux-amd64.tar.gz",
            version.as_str()
        ));
        let outcome = store
            .install(fs::File::open(&archive)?)
            .map_err(io::Error::other)?;
        let PluginProgramInstallResult::Installed(identity) = outcome else {
            return Err(io::Error::other("generated Java Program was not installed"));
        };
        assert_eq!(
            identity,
            PluginProgramIdentity::from_parts(program_name.clone(), version.clone())
        );
        let entry = store
            .lookup(&program_name, &version)
            .cloned()
            .ok_or_else(|| io::Error::other("generated Java Program is missing"))?;
        assert_eq!(entry.interface(), interface);
        assert_eq!(entry.command()[0], "runtime/bin/java");
        assert_eq!(
            entry.config_schema_bytes(),
            fs::read(generated.join("config.schema.json"))?
        );
        assert_eq!(
            entry.payload_descriptor_bytes(),
            fs::read(generated.join("payload.descriptor.pb"))?
        );
        let retry = store
            .install(fs::File::open(&archive)?)
            .map_err(io::Error::other)?;
        let PluginProgramInstallResult::Unchanged(identity) = retry else {
            return Err(io::Error::other(
                "generated Java Program was installed twice",
            ));
        };
        assert_eq!(
            identity,
            PluginProgramIdentity::from_parts(program_name.clone(), version.clone())
        );
        assert!(Arc::ptr_eq(
            &entry,
            store
                .lookup(&program_name, &version)
                .ok_or_else(|| io::Error::other("reinstalled Java Program is missing"))?
        ));
    }

    drop(store);
    let recovered = recover_store(root)?;
    assert_eq!(program_count(&recovered), cases.len());
    for (_, version, interface) in cases {
        let version = ExactVersion::try_from(version.to_owned()).map_err(io::Error::other)?;
        let entry = recovered
            .lookup(&program_name, &version)
            .ok_or_else(|| io::Error::other("recovered Java Program is missing"))?;
        assert_eq!(entry.interface(), interface);
        assert_eq!(
            entry.source_projection().is_some(),
            interface != PluginInterface::Sink
        );
        assert_eq!(
            entry.sink_projection().is_some(),
            interface != PluginInterface::Source
        );
    }
    Ok(())
}

fn recover_with_filesystem(
    directory: PathBuf,
    filesystem: &impl PublicationFilesystem,
) -> Result<PluginProgramStore, PluginStoreError> {
    let parent = directory
        .parent()
        .ok_or_else(|| PluginStoreError::StoreIntegrityInvalid {
            path: directory.clone(),
        })?;
    let runtime = tempfile::tempdir_in(parent)
        .map_err(|source| PluginStoreError::FilesystemOperationFailed {
            path: directory.clone(),
            source,
        })?
        .keep();
    PluginProgramStore::recover_using(
        directory,
        runtime,
        Arc::new(crate::runner::extensions::ByPass),
        filesystem,
    )
}

fn recover_store(root: PathBuf) -> io::Result<PluginProgramStore> {
    recover_with_filesystem(root, &DurablePublicationFilesystem).map_err(io::Error::other)
}

fn source_package_path(root: &Path) -> io::Result<PathBuf> {
    Ok(root
        .join("com.example.source")
        .join(super::original_file_name(&exact_version()?)))
}

fn lookup<'a>(
    store: &'a PluginProgramStore,
    program_name: &str,
) -> io::Result<Option<&'a Arc<PluginProgramEntry>>> {
    let program_name = ProgramName::try_from(program_name.to_owned()).map_err(io::Error::other)?;
    let exact_version = ExactVersion::try_from(String::from("1.0.0")).map_err(io::Error::other)?;
    Ok(store.lookup(&program_name, &exact_version))
}

fn source_program_name() -> io::Result<ProgramName> {
    ProgramName::try_from(String::from("com.example.source")).map_err(io::Error::other)
}

fn exact_version() -> io::Result<ExactVersion> {
    ExactVersion::try_from(String::from("1.0.0")).map_err(io::Error::other)
}

fn deletion_tombstone(root: &Path) -> PathBuf {
    root.join("com.example.source/.tenon-plugin-delete-92521fc3cbd964bdc9f584a991b89fddaa5754ed1cc96d6d42445338669c1305")
}

fn available_entry<'a>(
    store: &'a PluginProgramStore,
    program_name: &str,
) -> io::Result<&'a Arc<PluginProgramEntry>> {
    match lookup(store, program_name)? {
        Some(entry) => Ok(entry),
        _ => Err(io::Error::other("Program is not available")),
    }
}

fn assert_available(store: &PluginProgramStore, program_name: &str) -> io::Result<()> {
    available_entry(store, program_name).map(|_| ())
}

fn prepare_store_root(parent: &Path) -> io::Result<PathBuf> {
    let plugins = parent.join("plugins");
    let programs = plugins.join("programs");
    fs::create_dir(&plugins)?;
    make_private_directory(&plugins)?;
    fs::create_dir(&programs)?;
    make_private_directory(&programs)?;
    Ok(programs)
}

#[cfg(unix)]
fn make_writable(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn make_writable(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn make_private_directory(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn make_private_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn make_private_file(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    fs::set_permissions(path, fs::Permissions::from_mode(0o500))
}

#[cfg(not(unix))]
fn make_private_file(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PublicationPoint {
    SyncProgramDirectory,
    SyncStoreRoot,
    SyncPackageFile,
    Rename,
    SyncPublishedDirectory,
}

struct FaultingPublicationFilesystem {
    fail_at: PublicationPoint,
    directory_sync_count: Cell<usize>,
}

impl FaultingPublicationFilesystem {
    fn new(fail_at: PublicationPoint) -> Self {
        Self {
            fail_at,
            directory_sync_count: Cell::new(0),
        }
    }

    fn record(&self, point: PublicationPoint, path: &Path) -> Result<(), PluginStoreError> {
        if self.fail_at == point {
            Err(PluginStoreError::FilesystemOperationFailed {
                path: path.to_path_buf(),
                source: io::Error::other("Injected publication failure"),
            })
        } else {
            Ok(())
        }
    }

    fn next_directory_sync_point(&self) -> PublicationPoint {
        let count = self.directory_sync_count.get();
        self.directory_sync_count.set(count + 1);
        match count {
            0 => PublicationPoint::SyncProgramDirectory,
            1 => PublicationPoint::SyncStoreRoot,
            _ => PublicationPoint::SyncPublishedDirectory,
        }
    }
}

impl PublicationFilesystem for FaultingPublicationFilesystem {
    fn sync_file(&self, path: &Path) -> Result<(), PluginStoreError> {
        self.record(PublicationPoint::SyncPackageFile, path)?;
        DurablePublicationFilesystem.sync_file(path)
    }
    fn rename(&self, source: &Path, target: &Path) -> Result<(), PluginStoreError> {
        self.record(PublicationPoint::Rename, target)?;
        fs::rename(source, target).map_err(|source| PluginStoreError::FilesystemOperationFailed {
            path: target.to_path_buf(),
            source,
        })
    }

    fn sync_directory(&self, path: &Path) -> Result<(), PluginStoreError> {
        self.record(self.next_directory_sync_point(), path)?;
        sync_directory(path)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UninstallPoint {
    InspectTombstone,
    Rename,
    SyncParent,
    RemoveRuntime,
    RemoveTombstone,
    SyncCleanup,
}

struct FaultingUninstallFilesystem {
    fail_at: UninstallPoint,
    directory_sync_count: Cell<usize>,
}

impl FaultingUninstallFilesystem {
    const fn new(fail_at: UninstallPoint) -> Self {
        Self {
            fail_at,
            directory_sync_count: Cell::new(0),
        }
    }
}

impl PublicationFilesystem for FaultingUninstallFilesystem {
    fn rename(&self, source: &Path, target: &Path) -> Result<(), PluginStoreError> {
        if self.fail_at == UninstallPoint::Rename {
            return Err(PluginStoreError::FilesystemOperationFailed {
                path: target.to_path_buf(),
                source: io::Error::other("Injected uninstall rename failure"),
            });
        }
        fs::rename(source, target).map_err(|source| PluginStoreError::FilesystemOperationFailed {
            path: target.to_path_buf(),
            source,
        })
    }

    fn symlink_metadata(&self, path: &Path) -> io::Result<fs::Metadata> {
        if self.fail_at == UninstallPoint::InspectTombstone {
            return Err(io::Error::other("Injected uninstall metadata failure"));
        }
        fs::symlink_metadata(path)
    }

    fn sync_directory(&self, path: &Path) -> Result<(), PluginStoreError> {
        let count = self.directory_sync_count.get();
        self.directory_sync_count.set(count + 1);
        if (self.fail_at == UninstallPoint::SyncParent && count == 0)
            || (self.fail_at == UninstallPoint::SyncCleanup && count == 1)
        {
            return Err(PluginStoreError::FilesystemOperationFailed {
                path: path.to_path_buf(),
                source: io::Error::other("Injected uninstall parent sync failure"),
            });
        }
        sync_directory(path)
    }

    fn remove_entry(&self, path: &Path) -> Result<(), PluginStoreError> {
        let is_tombstone = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(super::DELETION_TOMBSTONE_PREFIX));
        if (self.fail_at == UninstallPoint::RemoveTombstone && is_tombstone)
            || (self.fail_at == UninstallPoint::RemoveRuntime && !is_tombstone)
        {
            return Err(PluginStoreError::FilesystemOperationFailed {
                path: path.to_path_buf(),
                source: io::Error::other("Injected tombstone cleanup failure"),
            });
        }
        DurablePublicationFilesystem.remove_entry(path)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveryPoint {
    Enumerate,
    Remove,
    SyncParent,
}

struct FaultingRecoveryFilesystem {
    fail_at: RecoveryPoint,
    path: PathBuf,
}

impl PublicationFilesystem for FaultingRecoveryFilesystem {
    fn read_directory(&self, path: &Path) -> Result<Vec<fs::DirEntry>, PluginStoreError> {
        if self.fail_at == RecoveryPoint::Enumerate && path == self.path {
            let read_one = || {
                let entry = fs::read_dir(path)?.next().transpose()?;
                entry.ok_or_else(|| io::Error::other("enumeration fixture must have a child"))?;
                Err(io::Error::other(
                    "Injected failure after one directory entry",
                ))
            };
            return read_one().map_err(|source| PluginStoreError::DirectoryReadFailed {
                path: path.to_path_buf(),
                source,
            });
        }
        DurablePublicationFilesystem.read_directory(path)
    }

    fn remove_entry(&self, path: &Path) -> Result<(), PluginStoreError> {
        if self.fail_at == RecoveryPoint::Remove && path == self.path {
            return Err(PluginStoreError::FilesystemOperationFailed {
                path: path.to_path_buf(),
                source: io::Error::other("Injected recovery cleanup failure"),
            });
        }
        DurablePublicationFilesystem.remove_entry(path)
    }

    fn sync_directory(&self, path: &Path) -> Result<(), PluginStoreError> {
        if self.fail_at == RecoveryPoint::SyncParent && path == self.path {
            return Err(PluginStoreError::FilesystemOperationFailed {
                path: path.to_path_buf(),
                source: io::Error::other("Injected recovery parent sync failure"),
            });
        }
        sync_directory(path)
    }

    fn rename(&self, source: &Path, target: &Path) -> Result<(), PluginStoreError> {
        DurablePublicationFilesystem.rename(source, target)
    }
}

#[test]
fn platform_rejection_precedes_conflict_and_leaves_published_bytes_untouched() -> io::Result<()> {
    use crate::runner::plugin::package::tests::{foreign_platform, package_with_platforms};
    use crate::runner::plugin::platform::Platform;
    use serde_json::json;
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root.clone())?;
    let original = valid_source_program_package()?;
    let foreign = package_with_platforms(&original, &json!([foreign_platform()]))?;
    assert!(matches!(
        store.install(Cursor::new(&foreign)),
        Err(PluginStoreError::PlatformMismatch { .. })
    ));
    assert_eq!(fs::read_dir(&root)?.count(), 0);
    store
        .install(Cursor::new(&original))
        .map_err(io::Error::other)?;
    let entry = Arc::clone(available_entry(&store, "com.example.source")?);
    let before = fs::read(entry.directory().join("manifest.json"))?;
    assert!(matches!(
        store.install(Cursor::new(foreign)),
        Err(PluginStoreError::PlatformMismatch { .. })
    ));
    assert_eq!(fs::read(entry.directory().join("manifest.json"))?, before);
    assert!(Arc::ptr_eq(
        &entry,
        available_entry(&store, "com.example.source")?
    ));
    assert_eq!(fs::read_dir(&root)?.count(), 1);
    drop(entry);
    assert_eq!(
        store
            .uninstall(&source_program_name()?, &exact_version()?)
            .map_err(io::Error::other)?,
        PluginUninstallOutcome::Uninstalled
    );
    for platforms in [
        json!([Platform::CURRENT, foreign_platform()]),
        json!([foreign_platform(), Platform::CURRENT]),
    ] {
        store
            .install(Cursor::new(package_with_platforms(&original, &platforms)?))
            .map_err(io::Error::other)?;
        assert_eq!(
            serde_json::to_value(available_entry(&store, "com.example.source")?.platforms())?,
            platforms
        );
        assert_eq!(
            store
                .uninstall(&source_program_name()?, &exact_version()?)
                .map_err(io::Error::other)?,
            PluginUninstallOutcome::Uninstalled
        );
    }
    Ok(())
}

#[test]
fn installation_does_not_execute_or_probe_the_declared_command() -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let parent = tempfile::tempdir()?;
    let root = prepare_store_root(parent.path())?;
    let mut store = recover_store(root)?;
    let script = parent.path().join("marker-command");
    fs::write(&script, b"#!/bin/sh\nprintf executed > \"$0.executed\"\n")?;
    fs::set_permissions(&script, fs::Permissions::from_mode(0o500))?;
    store
        .install(Cursor::new(source_program_package_with_command(
            script
                .to_str()
                .ok_or_else(|| io::Error::other("Test path is not UTF-8"))?,
        )?))
        .map_err(io::Error::other)?;
    assert!(!parent.path().join("marker-command.executed").exists());
    Ok(())
}

mod protected;
