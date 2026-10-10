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

//! Durable and in-memory ownership of current unified Plugin Programs.
//!
//! The Store keeps received packages and reconstructs temporary runtime files.
//! Package access or validation failure stops recovery and keeps the saved input.
//! The only runtime identity map contains immutable validated Entries.
//! There is no saved failure or missing state.
//! Runtime mutations are exclusive. Persistence or installed-layout failures
//! must terminate the Runner; only the next startup may recover the disk.

mod filesystem;
mod package_input;
mod protected;

pub(crate) use filesystem::PluginStoreError;

use super::package::{PluginConfigSchema, PluginPackageError};
use super::package::{
    ValidatedPluginProgramMaterial, ValidatedPluginProgramSnapshot,
    validate_plugin_program_directory,
};
use super::platform::Platform;
use crate::error::ErrorChain;
use crate::identifiers::{ExactVersion, PluginProgramIdentity, ProgramName};
use crate::payload_contract::{
    PluginInterface, PluginProgramPayloadContract, PluginProgramPayloadContractProjection,
};
use crate::runner::extensions::ArtifactProtection;
use crate::runner::private_filesystem::has_owner_only_directory_permission;
use filesystem::{
    DurablePublicationFilesystem, PublicationFilesystem, parse_program_entry,
    set_private_directory_permission,
};
use protected::{ORIGINAL_PREFIX, StoredOriginal, original_file_name, version_digest};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Installation disposition and the validated identity, without an execution lease.
#[derive(Debug)]
pub(crate) enum PluginProgramInstallResult {
    /// A previously absent exact identity was published.
    Installed(PluginProgramIdentity),
    /// The exact immutable package was already installed.
    Unchanged(PluginProgramIdentity),
}

const DELETION_TOMBSTONE_PREFIX: &str = ".tenon-plugin-delete-";

/// The only owner of durable Program layout and the validated runtime index.
pub(crate) struct PluginProgramStore {
    directory: PathBuf,
    runtime_directory: PathBuf,
    protection: Arc<dyn ArtifactProtection>,
    programs: HashMap<PluginProgramIdentity, Arc<PluginProgramEntry>>,
}

impl std::fmt::Debug for PluginProgramStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PluginProgramStore")
            .field("directory", &self.directory)
            .field("programs", &self.programs)
            .finish_non_exhaustive()
    }
}

impl PluginProgramStore {
    /// Reconstructs runtime files from the saved packages in the prepared root.
    ///
    /// Package access or validation failure stops recovery and keeps the saved input.
    /// An invalid or unreadable namespace is removed as a whole. Cleanup removes
    /// unsupported entries without following symbolic links or removing the fixed root.
    ///
    /// # Errors
    ///
    /// Returns [`PluginStoreError`] for a package failure, incomplete root
    /// enumeration, or a required cleanup or directory sync failure.
    pub(crate) fn recover(
        directory: PathBuf,
        runtime_directory: PathBuf,
        protection: Arc<dyn ArtifactProtection>,
    ) -> Result<Self, PluginStoreError> {
        Self::recover_using(
            directory,
            runtime_directory,
            protection,
            &DurablePublicationFilesystem,
        )
    }

    /// Installs and publishes one immutable Program in the same owner mutation.
    /// Returns its identity without lending the caller an execution reference.
    ///
    /// # Errors
    ///
    /// Returns [`PluginStoreError`] for an invalid upload, identity conflict,
    /// durability failure, or filesystem integrity drift. Persistence and
    /// integrity errors require Runner termination, not a retry on this Store.
    pub(crate) fn install(
        &mut self,
        package: impl Read,
    ) -> Result<PluginProgramInstallResult, PluginStoreError> {
        self.install_with_publication_filesystem(package, &DurablePublicationFilesystem)
    }

    /// Durably removes one exact Program that has no external Entry references.
    ///
    /// # Errors
    ///
    /// Returns [`PluginStoreError`] when the Program is still referenced or
    /// cannot be durably removed. Persistence and integrity errors require
    /// Runner termination, not a retry on this Store.
    pub(crate) fn uninstall(
        &mut self,
        program_name: &ProgramName,
        exact_version: &ExactVersion,
    ) -> Result<PluginUninstallOutcome, PluginStoreError> {
        self.uninstall_with_publication_filesystem(
            program_name,
            exact_version,
            &DurablePublicationFilesystem,
        )
    }

    /// Borrows one validated Entry without consulting the filesystem.
    #[must_use]
    pub(crate) fn lookup(
        &self,
        program_name: &ProgramName,
        exact_version: &ExactVersion,
    ) -> Option<&Arc<PluginProgramEntry>> {
        self.programs.get(&PluginProgramIdentity::from_parts(
            program_name.clone(),
            exact_version.clone(),
        ))
    }

    /// Iterates only validated Programs without consulting the filesystem.
    pub(crate) fn programs(
        &self,
    ) -> impl Iterator<Item = (&ProgramName, &ExactVersion, &Arc<PluginProgramEntry>)> {
        self.programs
            .iter()
            .map(|(identity, entry)| (identity.program_name(), identity.exact_version(), entry))
    }

    fn recover_using(
        directory: PathBuf,
        runtime_directory: PathBuf,
        protection: Arc<dyn ArtifactProtection>,
        filesystem: &impl PublicationFilesystem,
    ) -> Result<Self, PluginStoreError> {
        let mut store = Self {
            directory,
            runtime_directory,
            protection,
            programs: HashMap::new(),
        };
        // Collect each directory before deleting children, so a failed
        // enumeration cannot publish a partial namespace or hide a sibling.
        for namespace in filesystem.read_directory(&store.directory)? {
            let path = namespace.path();
            let (program_name, versions) = match parse_program_entry(&namespace).and_then(|name| {
                filesystem
                    .read_directory(&path)
                    .map(|versions| (name, versions))
            }) {
                Ok(namespace) => namespace,
                Err(issue) => {
                    discard_invalid_entry(&path, &store.directory, &issue, filesystem)?;
                    continue;
                }
            };
            for version in versions {
                let target = version.path();
                if version
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(ORIGINAL_PREFIX))
                {
                    let (exact_version, entry) = store.load_original(&target, &program_name)?;
                    store.programs.insert(
                        PluginProgramIdentity::from_parts(program_name.clone(), exact_version),
                        Arc::new(entry),
                    );
                    continue;
                }
                discard_invalid_entry(
                    &target,
                    &path,
                    &PluginStoreError::StoreIntegrityInvalid {
                        path: target.clone(),
                    },
                    filesystem,
                )?;
            }
        }
        Ok(store)
    }

    fn install_with_publication_filesystem(
        &mut self,
        package: impl Read,
        publication: &impl PublicationFilesystem,
    ) -> Result<PluginProgramInstallResult, PluginStoreError> {
        let mut package = package_input::receive(package, &self.directory)?;
        let staged = package_input::open(
            package.as_file_mut(),
            &self.runtime_directory,
            self.protection.as_ref(),
        )?;
        if !staged.platforms().contains(&Platform::CURRENT) {
            let platforms = staged.platforms().into();
            staged
                .close()
                .map_err(|source| PluginStoreError::FilesystemOperationFailed {
                    path: self.directory.clone(),
                    source,
                })?;
            return Err(PluginStoreError::PlatformMismatch { platforms });
        }
        let program_name = staged.program_name().clone();
        let identity =
            PluginProgramIdentity::from_parts(program_name.clone(), staged.exact_version().clone());
        let parent = self.directory.join(program_name.as_str());
        let target = parent.join(original_file_name(staged.exact_version()));

        let namespace_presence = inspect_program_namespace(&parent)?;
        if matches!(namespace_presence, ProgramNamespacePresence::Absent)
            && self
                .programs
                .keys()
                .any(|identity| identity.program_name() == &program_name)
        {
            return Err(PluginStoreError::StoreIntegrityInvalid { path: parent });
        }

        if let Some(entry) = self.programs.get(&identity) {
            entry.original.verify()?;
            let target = &entry.directory;
            let existing = load_installed(target)?;
            validate_identity(
                &existing,
                staged.program_name(),
                staged.exact_version(),
                target,
            )?;
            if !existing.matches_snapshot(&entry.manifest_bytes, &entry.file_digests) {
                return Err(PluginStoreError::StoreIntegrityInvalid {
                    path: target.clone(),
                });
            }
            let identical = staged
                .has_same_files_as(&existing, target)
                .map_err(|source| PluginStoreError::InstalledPackageInvalid {
                    path: target.clone(),
                    source,
                })?;
            staged
                .close()
                .map_err(|source| PluginStoreError::FilesystemOperationFailed {
                    path: self.directory.clone(),
                    source,
                })?;
            if !identical {
                return Err(PluginStoreError::VersionConflict);
            }
            return Ok(PluginProgramInstallResult::Unchanged(identity));
        }

        require_absent(&target, publication)?;
        self.prepare_program_directory(&parent, namespace_presence, publication)?;
        let original = StoredOriginal::from_file(target, package.as_file_mut())?;
        publication.sync_file(package.path())?;
        publication.rename(package.path(), &original.path)?;
        publication.sync_directory(&parent)?;
        let (directory, snapshot) = staged.into_runtime();
        let entry = program_entry(directory, original, snapshot);
        let entry = Arc::new(entry);
        self.programs.insert(identity.clone(), entry);
        Ok(PluginProgramInstallResult::Installed(identity))
    }

    fn uninstall_with_publication_filesystem(
        &mut self,
        program_name: &ProgramName,
        exact_version: &ExactVersion,
        publication: &impl PublicationFilesystem,
    ) -> Result<PluginUninstallOutcome, PluginStoreError> {
        let identity =
            PluginProgramIdentity::from_parts(program_name.clone(), exact_version.clone());
        let Some(entry) = self.programs.get_mut(&identity) else {
            return Ok(PluginUninstallOutcome::NotFound);
        };
        if Arc::get_mut(entry).is_none() {
            return Err(PluginStoreError::ProgramInUse);
        }

        let parent = self.directory.join(program_name.as_str());
        let target = entry.original.path.clone();
        if matches!(
            inspect_program_namespace(&parent)?,
            ProgramNamespacePresence::Absent
        ) {
            return Err(PluginStoreError::StoreIntegrityInvalid { path: parent });
        }
        entry.original.verify()?;
        let tombstone = deletion_tombstone_path(&parent, exact_version);
        require_absent(&tombstone, publication)?;
        publication.rename(&target, &tombstone)?;
        publication.sync_directory(&parent)?;
        let removed = self.programs.remove(&identity);
        assert!(
            removed.is_some(),
            "Program must remain owned until uninstall commits"
        );
        if let Some(entry) = &removed {
            publication.remove_entry(&entry.directory)?;
        }
        publication.remove_entry(&tombstone)?;
        publication.sync_directory(&parent)?;
        Ok(PluginUninstallOutcome::Uninstalled)
    }

    fn prepare_program_directory(
        &self,
        directory: &Path,
        presence: ProgramNamespacePresence,
        publication: &impl PublicationFilesystem,
    ) -> Result<(), PluginStoreError> {
        if matches!(presence, ProgramNamespacePresence::Absent) {
            fs::create_dir(directory).map_err(|source| {
                PluginStoreError::FilesystemOperationFailed {
                    path: directory.to_path_buf(),
                    source,
                }
            })?;
            set_private_directory_permission(directory)?;
        }
        publication.sync_directory(directory)?;
        publication.sync_directory(&self.directory)
    }
}

/// The observable result of one exact Program uninstall attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub(crate) enum PluginUninstallOutcome {
    /// The exact Program was durably removed.
    Uninstalled,
    /// The exact Program was absent from the runtime index.
    NotFound,
}

/// One immutable validated Program shared by the Store and ready Plans.
pub(crate) struct PluginProgramEntry {
    directory: PathBuf,
    original: StoredOriginal,
    manifest_bytes: Box<[u8]>,
    display_name: String,
    description: String,
    interface: PluginInterface,
    command: Box<[String]>,
    platforms: Box<[Platform]>,
    config_schema: PluginConfigSchema,
    payload_contract: PluginProgramPayloadContract,
    // This freezes the validation-time file snapshot so a later Store mutation
    // can distinguish upload conflict from external filesystem drift.
    file_digests: BTreeMap<Box<str>, [u8; 32]>,
}

impl PluginProgramEntry {
    /// Returns the validated runtime directory for this Program.
    #[must_use]
    pub(crate) fn directory(&self) -> &Path {
        &self.directory
    }

    /// Borrows the package author's validated display name.
    pub(crate) fn display_name(&self) -> &str {
        &self.display_name
    }

    /// Borrows the package author's validated plain-text description.
    pub(crate) fn description(&self) -> &str {
        &self.description
    }

    /// Returns the Program's closed interface capability.
    #[must_use]
    pub(crate) const fn interface(&self) -> PluginInterface {
        self.interface
    }

    /// Borrows the immutable declaration, including recovered foreign targets.
    pub(crate) fn platforms(&self) -> &[Platform] {
        &self.platforms
    }

    /// Returns the exact shell-free launch argument vector.
    #[must_use]
    pub(crate) fn command(&self) -> &[String] {
        &self.command
    }

    /// Returns the exact validated Config Schema bytes.
    #[must_use]
    pub(crate) fn config_schema_bytes(&self) -> &[u8] {
        self.config_schema.bytes()
    }

    /// Checks an Instance against this exact Program's already compiled Schema.
    pub(crate) fn accepts_config(&self, config: &serde_json::Value) -> bool {
        self.config_schema.accepts_config(config)
    }

    /// Returns the original validated FileDescriptorSet bytes.
    #[must_use]
    pub(crate) fn payload_descriptor_bytes(&self) -> &[u8] {
        self.payload_contract.descriptor_bytes()
    }

    /// Borrows this Program's Source Contract projection when implemented.
    #[must_use]
    pub(crate) fn source_projection(&self) -> Option<PluginProgramPayloadContractProjection<'_>> {
        self.payload_contract.source_projection()
    }

    /// Borrows this Program's Sink Contract projection when implemented.
    #[must_use]
    pub(crate) fn sink_projection(&self) -> Option<PluginProgramPayloadContractProjection<'_>> {
        self.payload_contract.sink_projection()
    }
}

impl std::fmt::Debug for PluginProgramEntry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PluginProgramEntry")
            .field("directory", &self.directory)
            .field("interface", &self.interface)
            .field("command", &self.command)
            .finish_non_exhaustive()
    }
}

fn program_entry(
    directory: PathBuf,
    original: StoredOriginal,
    snapshot: ValidatedPluginProgramSnapshot,
) -> PluginProgramEntry {
    let ValidatedPluginProgramSnapshot {
        validated,
        file_digests,
    } = snapshot;
    let ValidatedPluginProgramMaterial {
        manifest_bytes,
        display_name,
        description,
        interface,
        command,
        platforms,
        config_schema,
        payload_contract,
    } = validated.into_material();
    PluginProgramEntry {
        directory,
        original,
        manifest_bytes,
        display_name,
        description,
        interface,
        command,
        platforms,
        config_schema,
        payload_contract,
        file_digests,
    }
}

fn deletion_tombstone_path(parent: &Path, exact_version: &ExactVersion) -> PathBuf {
    parent.join(format!(
        "{DELETION_TOMBSTONE_PREFIX}{}",
        version_digest(exact_version)
    ))
}

fn discard_invalid_entry(
    path: &Path,
    parent: &Path,
    issue: &PluginStoreError,
    filesystem: &impl PublicationFilesystem,
) -> Result<(), PluginStoreError> {
    let (outcome, operation_path) = match filesystem.remove_entry(path) {
        Ok(()) => (filesystem.sync_directory(parent), parent),
        Err(error) => (Err(error), path),
    };
    eprintln!(
        "{}",
        serde_json::json!({
            "event": "plugin_store_entry_cleanup",
            "path": path.to_string_lossy(),
            "reason": {"code": issue.code(), "message": ErrorChain(issue).to_string()},
            "outcome": if outcome.is_ok() { "removed" } else { "failed" },
            "failedPath": outcome.as_ref().err().map(|_| operation_path.to_string_lossy()),
        })
    );
    outcome
}

fn require_absent(
    path: &Path,
    filesystem: &impl PublicationFilesystem,
) -> Result<(), PluginStoreError> {
    match filesystem.symlink_metadata(path) {
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(PluginStoreError::StoreIntegrityInvalid {
            path: path.to_path_buf(),
        }),
        Err(source) => Err(PluginStoreError::FilesystemOperationFailed {
            path: path.to_path_buf(),
            source,
        }),
    }
}

#[derive(Clone, Copy)]
enum ProgramNamespacePresence {
    Absent,
    Present,
}

fn inspect_program_namespace(
    directory: &Path,
) -> Result<ProgramNamespacePresence, PluginStoreError> {
    match fs::symlink_metadata(directory) {
        Ok(metadata) if metadata.is_dir() && has_owner_only_directory_permission(&metadata) => {
            Ok(ProgramNamespacePresence::Present)
        }
        Ok(_) => Err(PluginStoreError::StoreIntegrityInvalid {
            path: directory.to_path_buf(),
        }),
        Err(source) if source.kind() == io::ErrorKind::NotFound => {
            Ok(ProgramNamespacePresence::Absent)
        }
        Err(source) => Err(PluginStoreError::FilesystemOperationFailed {
            path: directory.to_path_buf(),
            source,
        }),
    }
}

fn load_installed(path: &Path) -> Result<ValidatedPluginProgramSnapshot, PluginStoreError> {
    validate_plugin_program_directory(path).map_err(|source| {
        PluginStoreError::InstalledPackageInvalid {
            path: path.to_path_buf(),
            source,
        }
    })
}

fn validate_identity(
    installed: &ValidatedPluginProgramSnapshot,
    expected_program_name: &ProgramName,
    expected_exact_version: &ExactVersion,
    path: &Path,
) -> Result<(), PluginStoreError> {
    if installed.program_name() == expected_program_name
        && installed.exact_version() == expected_exact_version
    {
        Ok(())
    } else {
        Err(PluginStoreError::StoreIntegrityInvalid {
            path: path.to_path_buf(),
        })
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::{HashMap, PathBuf, PluginProgramStore};

    pub(crate) fn empty_store(directory: PathBuf) -> PluginProgramStore {
        PluginProgramStore {
            runtime_directory: directory.clone(),
            protection: std::sync::Arc::new(crate::runner::extensions::ByPass),
            directory,
            programs: HashMap::new(),
        }
    }

    pub(crate) fn program_count(store: &PluginProgramStore) -> usize {
        store.programs.len()
    }
}

#[cfg(test)]
mod tests;
