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

//! Opens saved original packages and creates validated runtime files.

use super::{
    PluginProgramEntry, PluginProgramStore, PluginStoreError, package_input, program_entry,
};
use crate::identifiers::{ExactVersion, ProgramName};
use sha2::{Digest as _, Sha256};
use std::fs::{self, File};
use std::io::{self, Read as _, Seek as _};
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

pub(super) const ORIGINAL_PREFIX: &str = ".tenon-artifact-";

pub(super) fn original_file_name(version: &ExactVersion) -> String {
    format!("{ORIGINAL_PREFIX}{}", version_digest(version))
}

pub(super) fn version_digest(version: &ExactVersion) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in Sha256::digest(version.as_str().as_bytes()) {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

pub(super) struct StoredOriginal {
    pub(super) path: PathBuf,
    digest: [u8; 32],
}

impl StoredOriginal {
    pub(super) fn from_file(path: PathBuf, file: &mut File) -> Result<Self, PluginStoreError> {
        let digest =
            file_digest(file).map_err(|source| PluginStoreError::FilesystemOperationFailed {
                path: path.clone(),
                source,
            })?;
        Ok(Self { path, digest })
    }

    pub(super) fn verify(&self) -> Result<(), PluginStoreError> {
        let mut file = open_original(&self.path)?;
        let current = Self::from_file(self.path.clone(), &mut file)?;
        if current.digest != self.digest {
            return Err(PluginStoreError::StoreIntegrityInvalid {
                path: self.path.clone(),
            });
        }
        Ok(())
    }
}

impl PluginProgramStore {
    pub(super) fn load_original(
        &self,
        path: &Path,
        name: &ProgramName,
    ) -> Result<(ExactVersion, PluginProgramEntry), PluginStoreError> {
        let mut input = open_original(path)?;
        let original = StoredOriginal::from_file(path.to_path_buf(), &mut input)?;
        let staged = package_input::open(
            &mut input,
            &self.runtime_directory,
            self.protection.as_ref(),
        )
        .map_err(|error| match error {
            PluginStoreError::PackageInvalid { source } => {
                PluginStoreError::InstalledPackageInvalid {
                    path: path.to_path_buf(),
                    source,
                }
            }
            error => error,
        })?;
        if staged.program_name() != name
            || path.file_name()
                != Some(std::ffi::OsStr::new(&original_file_name(
                    staged.exact_version(),
                )))
        {
            return Err(PluginStoreError::StoreIntegrityInvalid {
                path: path.to_path_buf(),
            });
        }
        let version = staged.exact_version().clone();
        let (directory, snapshot) = staged.into_runtime();
        let entry = program_entry(directory, original, snapshot);
        Ok((version, entry))
    }
}

fn open_original(path: &Path) -> Result<File, PluginStoreError> {
    let metadata = fs::symlink_metadata(path).map_err(|source| {
        PluginStoreError::FilesystemOperationFailed {
            path: path.to_path_buf(),
            source,
        }
    })?;
    if !metadata.is_file()
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.nlink() != 1
    {
        return Err(PluginStoreError::StoreIntegrityInvalid {
            path: path.to_path_buf(),
        });
    }
    File::open(path).map_err(|source| PluginStoreError::FilesystemOperationFailed {
        path: path.to_path_buf(),
        source,
    })
}

fn file_digest(file: &mut File) -> io::Result<[u8; 32]> {
    file.rewind()?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    file.rewind()?;
    Ok(digest.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saved_original_above_eight_gib_can_be_opened() -> io::Result<()> {
        let original = tempfile::NamedTempFile::new()?;
        let length = 8 * 1024 * 1024 * 1024 + 1;
        original.as_file().set_len(length)?;
        let file = open_original(original.path()).map_err(io::Error::other)?;
        assert_eq!(file.metadata()?.len(), length);
        Ok(())
    }
}
