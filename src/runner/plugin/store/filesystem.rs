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

//! Filesystem publication operations and shared Store failures.
//!
//! The Program Store owns mutation ordering and the live reference index.
//! This boundary synchronizes, renames, and inspects its private filesystem.

use crate::identifiers::ProgramName;
use crate::runner::plugin::package::PluginPackageError;
use crate::runner::plugin::platform::Platform;
use crate::runner::private_filesystem::{
    has_owner_only_directory_permission, set_owner_only_directory_permission,
};
use std::error::Error;
use std::fmt;
use std::fs::{self, DirEntry};
use std::io;
use std::path::{Path, PathBuf};

/// Filesystem operations whose ordering defines Store recovery and publication.
pub(super) trait PublicationFilesystem {
    fn sync_file(&self, path: &Path) -> Result<(), PluginStoreError> {
        fs::File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(|source| PluginStoreError::FilesystemOperationFailed {
                path: path.to_path_buf(),
                source,
            })
    }
    /// Reads all children before recovery mutates the directory.
    ///
    /// # Errors
    ///
    /// Returns [`PluginStoreError`] if opening or completing enumeration fails.
    fn read_directory(&self, path: &Path) -> Result<Vec<DirEntry>, PluginStoreError> {
        fs::read_dir(path)
            .and_then(Iterator::collect)
            .map_err(|source| PluginStoreError::DirectoryReadFailed {
                path: path.to_path_buf(),
                source,
            })
    }

    /// Removes one child tree or non-directory entry without following links.
    ///
    /// # Errors
    ///
    /// Returns [`PluginStoreError`] if inspecting or removing the child fails.
    fn remove_entry(&self, path: &Path) -> Result<(), PluginStoreError> {
        let remove = || {
            if fs::symlink_metadata(path)?.is_dir() {
                fs::remove_dir_all(path)
            } else {
                fs::remove_file(path)
            }
        };
        remove().map_err(|source| PluginStoreError::FilesystemOperationFailed {
            path: path.to_path_buf(),
            source,
        })
    }

    /// Atomically moves the package file to its final path.
    ///
    /// # Errors
    ///
    /// Returns [`PluginStoreError`] when the filesystem rename fails.
    fn rename(&self, source: &Path, target: &Path) -> Result<(), PluginStoreError>;

    /// Reads metadata for one exact path without following a symbolic link.
    ///
    /// # Errors
    ///
    /// Returns the host filesystem error when metadata cannot be read.
    fn symlink_metadata(&self, path: &Path) -> io::Result<fs::Metadata> {
        fs::symlink_metadata(path)
    }

    /// Makes the named directory's current entries durable.
    ///
    /// # Errors
    ///
    /// Returns [`PluginStoreError`] when the directory cannot be opened or synchronized.
    fn sync_directory(&self, path: &Path) -> Result<(), PluginStoreError>;
}

/// Production publication operations backed by the host filesystem.
pub(super) struct DurablePublicationFilesystem;

impl PublicationFilesystem for DurablePublicationFilesystem {
    fn rename(&self, source: &Path, target: &Path) -> Result<(), PluginStoreError> {
        fs::rename(source, target).map_err(|source| PluginStoreError::FilesystemOperationFailed {
            path: target.to_path_buf(),
            source,
        })
    }

    fn sync_directory(&self, path: &Path) -> Result<(), PluginStoreError> {
        sync_directory(path)
    }
}

/// A stable failure from package validation, Store state, or private filesystem I/O.
#[derive(Debug)]
pub(crate) enum PluginStoreError {
    /// The distribution did not grant package access.
    PackageAccessRejected,
    /// The uploaded package failed the shared package boundary.
    PackageInvalid {
        /// The precise package failure.
        source: PluginPackageError,
    },
    /// The package does not declare this Runner target.
    PlatformMismatch { platforms: Box<[Platform]> },
    /// The exact immutable identity already names different content.
    VersionConflict,
    /// A live owner can still upgrade or hold the immutable Program Entry.
    ProgramInUse,
    /// A committed package no longer passes its complete package contract.
    InstalledPackageInvalid {
        /// The package or runtime directory.
        path: PathBuf,
        /// The precise package failure.
        source: PluginPackageError,
    },
    /// The private Store contains an unknown or unsafe filesystem object.
    StoreIntegrityInvalid {
        /// The invalid path.
        path: PathBuf,
    },
    /// A private Store directory could not be enumerated.
    DirectoryReadFailed {
        /// The directory or entry being read.
        path: PathBuf,
        /// The underlying operating-system failure.
        source: io::Error,
    },
    /// The private filesystem could not complete an installation operation.
    FilesystemOperationFailed {
        /// The exact path being prepared, synchronized, or published.
        path: PathBuf,
        /// The underlying operating-system failure.
        source: io::Error,
    },
}

impl PluginStoreError {
    /// Returns the stable API-facing failure category.
    #[must_use]
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::PackageAccessRejected => "plugin_package_open_failed",
            Self::PackageInvalid { source } => source.code(),
            Self::PlatformMismatch { .. } => "plugin_platform_mismatch",
            Self::VersionConflict => "plugin_version_conflict",
            Self::ProgramInUse => "plugin_in_use",
            Self::InstalledPackageInvalid { .. } | Self::StoreIntegrityInvalid { .. } => {
                "plugin_store_integrity_invalid"
            }
            Self::DirectoryReadFailed { .. } | Self::FilesystemOperationFailed { .. } => {
                "plugin_store_internal_error"
            }
        }
    }
}

impl fmt::Display for PluginStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PackageAccessRejected => {
                formatter.write_str("Plugin package access was rejected")
            }
            Self::PackageInvalid { .. } => formatter.write_str("Plugin package is invalid"),
            Self::PlatformMismatch { platforms } => write!(
                formatter,
                "Plugin platforms [{}] do not include Runner platform {}",
                platforms
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
                Platform::CURRENT
            ),
            Self::VersionConflict => {
                formatter.write_str("Plugin version conflicts with installed content")
            }
            Self::ProgramInUse => formatter.write_str("Plugin Program is still referenced"),
            Self::InstalledPackageInvalid { path, .. } => write!(
                formatter,
                "Installed Plugin package is invalid: {}",
                path.display()
            ),
            Self::StoreIntegrityInvalid { path } => write!(
                formatter,
                "Plugin Store integrity is invalid: {}",
                path.display()
            ),
            Self::DirectoryReadFailed { path, .. } => {
                write!(
                    formatter,
                    "Plugin Store directory cannot be read: {}",
                    path.display()
                )
            }
            Self::FilesystemOperationFailed { path, .. } => write!(
                formatter,
                "Plugin Store filesystem operation failed: {}",
                path.display()
            ),
        }
    }
}

impl Error for PluginStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::PackageAccessRejected => None,
            Self::PackageInvalid { source } | Self::InstalledPackageInvalid { source, .. } => {
                Some(source)
            }
            Self::DirectoryReadFailed { source, .. }
            | Self::FilesystemOperationFailed { source, .. } => Some(source),
            Self::PlatformMismatch { .. }
            | Self::VersionConflict
            | Self::ProgramInUse
            | Self::StoreIntegrityInvalid { .. } => None,
        }
    }
}

/// Validates one private directory entry and parses its name as a Program identity.
///
/// # Errors
///
/// Returns [`PluginStoreError`] when the entry is unsafe or its name is not canonical.
pub(super) fn parse_program_entry(entry: &DirEntry) -> Result<ProgramName, PluginStoreError> {
    validate_private_directory_entry(entry)?;
    let name = entry
        .file_name()
        .into_string()
        .map_err(|_| PluginStoreError::StoreIntegrityInvalid { path: entry.path() })?;
    ProgramName::try_from(name)
        .map_err(|_| PluginStoreError::StoreIntegrityInvalid { path: entry.path() })
}

/// Accepts only an owner-private ordinary directory without following a link.
///
/// # Errors
///
/// Returns [`PluginStoreError`] when metadata cannot be read or the entry is unsafe.
pub(super) fn validate_private_directory_entry(entry: &DirEntry) -> Result<(), PluginStoreError> {
    let path = entry.path();
    let file_type = entry
        .file_type()
        .map_err(|source| PluginStoreError::DirectoryReadFailed {
            path: path.clone(),
            source,
        })?;
    let metadata =
        fs::symlink_metadata(&path).map_err(|source| PluginStoreError::DirectoryReadFailed {
            path: path.clone(),
            source,
        })?;
    if file_type.is_dir() && metadata.is_dir() && has_owner_only_directory_permission(&metadata) {
        Ok(())
    } else {
        Err(PluginStoreError::StoreIntegrityInvalid { path })
    }
}

/// Synchronizes one directory entry set to stable storage.
///
/// # Errors
///
/// Returns [`PluginStoreError`] when the directory cannot be opened or synchronized.
pub(super) fn sync_directory(path: &Path) -> Result<(), PluginStoreError> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| PluginStoreError::FilesystemOperationFailed {
            path: path.to_path_buf(),
            source,
        })
}

/// Restricts one Store-owned directory to its owner.
///
/// # Errors
///
/// Returns [`PluginStoreError`] when the permission change fails.
pub(super) fn set_private_directory_permission(path: &Path) -> Result<(), PluginStoreError> {
    set_owner_only_directory_permission(path).map_err(|source| {
        PluginStoreError::FilesystemOperationFailed {
            path: path.to_path_buf(),
            source,
        }
    })
}
