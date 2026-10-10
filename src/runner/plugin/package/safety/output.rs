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

//! Writes decoded package files while the core owns their temporary directory.

use super::{
    COPY_BUFFER_BYTES, MANIFEST_PATH, NormalizedPackagePath, PackageEntryKind, PackageLimits,
    PluginPackageError, control_file_limit, create_owner_only_directories, register_path_shape,
};
use crate::runner::extensions::PluginPackageOutput;
use crate::runner::private_filesystem::set_owner_only_file_permission;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::path::Path;

/// Writes package files and records failures for core validation.
#[derive(Debug)]
pub(super) struct PackageOutput<'a> {
    root: &'a Path,
    limits: PackageLimits,
    explicit_paths: HashSet<Box<str>>,
    directories: HashSet<Box<str>>,
    files: HashSet<Box<str>>,
    file_digests: BTreeMap<Box<str>, [u8; 32]>,
    file_bytes: u64,
    failure: Option<PluginPackageError>,
}

impl<'a> PackageOutput<'a> {
    pub(super) fn new(root: &'a Path, limits: PackageLimits) -> Self {
        Self {
            root,
            limits,
            explicit_paths: HashSet::new(),
            directories: HashSet::new(),
            files: HashSet::new(),
            file_digests: BTreeMap::new(),
            file_bytes: 0,
            failure: None,
        }
    }

    pub(super) fn finish(
        self,
        result: io::Result<()>,
    ) -> Result<BTreeMap<Box<str>, [u8; 32]>, PluginPackageError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        result.map_err(|error| match error.kind() {
            io::ErrorKind::FileTooLarge => PluginPackageError::PackageTooLarge,
            io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof => {
                PluginPackageError::ArchiveInvalid { source: error }
            }
            _ => PluginPackageError::AccessRejected,
        })?;
        Ok(self.file_digests)
    }

    fn require_usable(&self) -> io::Result<()> {
        if self.failure.is_some() {
            Err(io::Error::other("Plugin package output has already failed"))
        } else {
            Ok(())
        }
    }

    fn record(&mut self, result: Result<(), PluginPackageError>) -> io::Result<()> {
        result.map_err(|source| {
            let kind = match &source {
                PluginPackageError::FilesystemOperationFailed { source } => source.kind(),
                PluginPackageError::PackageTooLarge => io::ErrorKind::FileTooLarge,
                _ => io::ErrorKind::InvalidData,
            };
            self.failure = Some(source);
            io::Error::new(kind, "Plugin package output failed")
        })
    }

    fn register(
        &mut self,
        path: &str,
        kind: PackageEntryKind,
    ) -> Result<NormalizedPackagePath, PluginPackageError> {
        let path = NormalizedPackagePath::parse(path.as_bytes(), kind)?;
        if !self.explicit_paths.insert(path.text.clone()) {
            return Err(PluginPackageError::PackageInvalid);
        }
        register_path_shape(&path, &mut self.directories, &mut self.files)?;
        if self.directories.len() + self.files.len() > self.limits.entries {
            return Err(PluginPackageError::PackageTooLarge);
        }
        Ok(path)
    }

    fn write_file_contents(
        &mut self,
        path: &str,
        source: &mut dyn Read,
    ) -> Result<(), PluginPackageError> {
        let path = self.register(path, PackageEntryKind::File)?;
        let limit = control_file_limit(path.as_str(), self.limits)
            .unwrap_or(self.limits.extracted_file_bytes);
        let target = path.to_path(self.root);
        let parents = path.components.len().saturating_sub(1);
        create_owner_only_directories(self.root, &path.components[..parents])?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)
            .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
        let mut file_bytes = 0_u64;
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; COPY_BUFFER_BYTES];
        loop {
            let count = match source.read(&mut buffer) {
                Ok(count) => count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    return Err(match error.kind() {
                        io::ErrorKind::FileTooLarge => PluginPackageError::PackageTooLarge,
                        _ => PluginPackageError::PackageInvalid,
                    });
                }
            };
            if count == 0 {
                break;
            }
            file_bytes = file_bytes
                .checked_add(count as u64)
                .ok_or(PluginPackageError::PackageTooLarge)?;
            self.file_bytes = self
                .file_bytes
                .checked_add(count as u64)
                .ok_or(PluginPackageError::PackageTooLarge)?;
            if file_bytes > limit || self.file_bytes > self.limits.extracted_file_bytes {
                return Err(PluginPackageError::PackageTooLarge);
            }
            file.write_all(&buffer[..count])
                .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
            digest.update(&buffer[..count]);
        }
        file.flush()
            .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
        set_owner_only_file_permission(&target)
            .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
        if path.as_str() != MANIFEST_PATH {
            self.file_digests
                .insert(path.text, digest.finalize().into());
        }
        Ok(())
    }
}

impl PluginPackageOutput for PackageOutput<'_> {
    fn write_file(&mut self, path: &str, source: &mut dyn Read) -> io::Result<()> {
        self.require_usable()?;
        let result = self.write_file_contents(path, source);
        self.record(result)
    }

    fn create_dir(&mut self, path: &str) -> io::Result<()> {
        self.require_usable()?;
        let result = self
            .register(path, PackageEntryKind::Directory)
            .and_then(|path| create_owner_only_directories(self.root, &path.components));
        self.record(result)
    }
}

#[cfg(test)]
mod tests;
