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

//! Safe staging and validation for Plugin Program packages.
//!
//! This module checks directory paths and Config Schemas.
//! It does not publish Store state, expose HTTP resources, or start processes.

mod safety;

use super::platform::Platform;
use crate::contracts::plugin::manifest_schema_bytes;
use crate::identifiers::{ExactVersion, ProgramName};
use crate::payload_contract::{PluginInterface, PluginProgramPayloadContract};
use crate::runner::extensions;
use crate::strict_jsonc::parse_json;
use extensions::ArtifactProtection;
pub(super) use safety::PluginConfigSchema;
pub(crate) use safety::PluginPackageError;
use safety::{
    InspectedPackageDirectory, StagedPackageDirectory, inspect_package_directory,
    package_files_are_identical, stage_package_directory,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::io::{self, Read};
use std::path::Path;

/// A validated current Program tree removed automatically unless later published.
#[must_use = "dropping a staged Program package removes its temporary directory"]
#[derive(Debug)]
pub(crate) struct StagedPluginProgramPackage {
    directory: StagedPackageDirectory,
    validated: ValidatedPluginProgramPackage,
}

impl StagedPluginProgramPackage {
    /// Removes an unused validated tree without silently losing cleanup errors.
    ///
    /// # Errors
    ///
    /// Returns the filesystem error when the staging tree cannot be removed.
    pub(super) fn close(self) -> io::Result<()> {
        self.directory.close()
    }

    /// Returns the root of the private validated temporary tree.
    #[must_use]
    pub(crate) fn path(&self) -> &Path {
        self.directory.path()
    }

    /// Returns the validated Program name used for the Store identity path.
    #[must_use]
    pub(super) const fn program_name(&self) -> &ProgramName {
        &self.validated.manifest.program_name
    }

    /// Returns the validated exact version used for the Store identity path.
    #[must_use]
    pub(super) const fn exact_version(&self) -> &ExactVersion {
        &self.validated.manifest.exact_version
    }

    /// Borrows the package declaration before installation admission.
    pub(super) fn platforms(&self) -> &[Platform] {
        &self.validated.manifest.platforms
    }

    /// Compares this staged tree with one fully validated committed tree.
    ///
    /// # Errors
    ///
    /// Returns [`PluginPackageError`] when either tree can no longer be read.
    pub(super) fn has_same_files_as(
        &self,
        existing: &ValidatedPluginProgramSnapshot,
        existing_root: &Path,
    ) -> Result<bool, PluginPackageError> {
        package_files_are_identical(
            self.path(),
            &self.validated.manifest_bytes,
            self.directory.file_digests(),
            existing_root,
            &existing.validated.manifest_bytes,
            &existing.file_digests,
        )
    }

    /// Releases the staging guard after publication and returns validated
    /// materials with their frozen ordinary-file digest snapshot.
    #[must_use]
    pub(super) fn into_snapshot(self) -> ValidatedPluginProgramSnapshot {
        ValidatedPluginProgramSnapshot {
            validated: self.validated,
            file_digests: self.directory.into_file_digests(),
        }
    }

    /// Transfers file cleanup to the Runner runtime owner.
    pub(super) fn into_runtime(mut self) -> (std::path::PathBuf, ValidatedPluginProgramSnapshot) {
        self.directory.keep();
        (self.path().to_path_buf(), self.into_snapshot())
    }
}

/// Parsed immutable Program materials shared by staging and startup recovery.
#[derive(Debug)]
pub(super) struct ValidatedPluginProgramPackage {
    manifest_bytes: Box<[u8]>,
    manifest: PluginProgramManifest,
    config_schema: PluginConfigSchema,
    payload_contract: PluginProgramPayloadContract,
}

impl ValidatedPluginProgramPackage {
    /// Returns the manifest identity component used for the Store directory.
    #[must_use]
    pub(super) const fn program_name(&self) -> &ProgramName {
        &self.manifest.program_name
    }

    /// Returns the manifest version component used for the Store directory.
    #[must_use]
    pub(super) const fn exact_version(&self) -> &ExactVersion {
        &self.manifest.exact_version
    }

    /// Transfers the exact package artifacts into the Store entry boundary.
    #[must_use]
    pub(super) fn into_material(self) -> ValidatedPluginProgramMaterial {
        ValidatedPluginProgramMaterial {
            manifest_bytes: self.manifest_bytes,
            display_name: self.manifest.display_name,
            description: self.manifest.description,
            interface: self.manifest.interface,
            command: self.manifest.command,
            platforms: self.manifest.platforms,
            config_schema: self.config_schema,
            payload_contract: self.payload_contract,
        }
    }
}

/// Named transfer of validated package facts into the Store owner.
pub(super) struct ValidatedPluginProgramMaterial {
    pub(super) manifest_bytes: Box<[u8]>,
    pub(super) display_name: String,
    pub(super) description: String,
    pub(super) interface: PluginInterface,
    pub(super) command: Box<[String]>,
    pub(super) platforms: Box<[Platform]>,
    pub(super) config_schema: PluginConfigSchema,
    pub(super) payload_contract: PluginProgramPayloadContract,
}

/// Validated Program materials and their frozen ordinary-file digest snapshot.
pub(super) struct ValidatedPluginProgramSnapshot {
    pub(super) validated: ValidatedPluginProgramPackage,
    pub(super) file_digests: BTreeMap<Box<str>, [u8; 32]>,
}

impl ValidatedPluginProgramSnapshot {
    /// Returns the recovered Program name that must match its parent directory.
    #[must_use]
    pub(super) const fn program_name(&self) -> &ProgramName {
        self.validated.program_name()
    }

    /// Returns the recovered exact version that must match its directory name.
    #[must_use]
    pub(super) const fn exact_version(&self) -> &ExactVersion {
        self.validated.exact_version()
    }

    /// Tests whether this scan still matches a frozen Store snapshot.
    #[must_use]
    pub(super) fn matches_snapshot(
        &self,
        manifest_bytes: &[u8],
        file_digests: &BTreeMap<Box<str>, [u8; 32]>,
    ) -> bool {
        self.validated.manifest_bytes.as_ref() == manifest_bytes
            && &self.file_digests == file_digests
    }
}

/// Stages and validates one current unified Plugin Program package.
///
/// # Errors
///
/// Returns [`PluginPackageError`] before any Store state is published when the
/// archive, fixed files, manifest, Config Schema, or shared
/// payload descriptor violates its current contract.
pub(crate) fn stage_plugin_program_package(
    protection: &dyn ArtifactProtection,
    package: impl Read,
    staging_parent: &Path,
) -> Result<StagedPluginProgramPackage, PluginPackageError> {
    let directory = stage_package_directory(protection, package, staging_parent)?;
    let validated = (|| {
        validate_program_materials(
            directory.read_manifest()?,
            directory.read_config_schema()?,
            directory.read_payload_descriptor()?,
        )
    })();
    match validated {
        Ok(validated) => Ok(StagedPluginProgramPackage {
            directory,
            validated,
        }),
        Err(error) => {
            directory
                .close()
                .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
            Err(error)
        }
    }
}

/// Revalidates one Program runtime directory through the shared tree boundary.
///
/// # Errors
///
/// Returns [`PluginPackageError`] when the directory or any current Program
/// contract is invalid.
pub(super) fn validate_plugin_program_directory(
    root: &Path,
) -> Result<ValidatedPluginProgramSnapshot, PluginPackageError> {
    let InspectedPackageDirectory {
        manifest_bytes,
        config_schema_bytes,
        descriptor_bytes,
        file_digests,
    } = inspect_package_directory(root)?;
    let validated =
        validate_program_materials(manifest_bytes, config_schema_bytes, descriptor_bytes)?;
    Ok(ValidatedPluginProgramSnapshot {
        validated,
        file_digests,
    })
}

/// One current manifest after Schema and domain projection validation.
#[derive(Debug)]
struct PluginProgramManifest {
    program_name: ProgramName,
    exact_version: ExactVersion,
    display_name: String,
    description: String,
    interface: PluginInterface,
    command: Box<[String]>,
    platforms: Box<[Platform]>,
}

fn validate_program_materials(
    manifest_bytes: Vec<u8>,
    config_schema_bytes: Vec<u8>,
    descriptor_bytes: Vec<u8>,
) -> Result<ValidatedPluginProgramPackage, PluginPackageError> {
    let manifest = parse_manifest(&manifest_bytes)?;
    let config_schema = PluginConfigSchema::parse_program(config_schema_bytes)
        .map_err(|source| PluginPackageError::ConfigSchemaInvalid { source })?;
    let payload_contract =
        PluginProgramPayloadContract::parse(descriptor_bytes, manifest.interface)
            .map_err(|source| PluginPackageError::PayloadContractInvalid { source })?;

    Ok(ValidatedPluginProgramPackage {
        manifest_bytes: manifest_bytes.into_boxed_slice(),
        manifest,
        config_schema,
        payload_contract,
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawPluginProgramManifest {
    program_name: String,
    exact_version: String,
    display_name: String,
    description: String,
    interface: PluginInterface,
    command: Vec<String>,
    platforms: Box<[Platform]>,
}

fn parse_manifest(bytes: &[u8]) -> Result<PluginProgramManifest, PluginPackageError> {
    let schema = parse_json(manifest_schema_bytes())
        .map_err(|source| PluginPackageError::InternalContractInvalid { source })?;
    let validator = jsonschema::draft202012::new(&schema)
        .map_err(|source| PluginPackageError::InternalManifestSchemaCompilationFailed { source })?;
    let value =
        parse_json(bytes).map_err(|source| PluginPackageError::ManifestJsonInvalid { source })?;
    validator
        .validate(&value)
        .map_err(|source| PluginPackageError::ManifestSchemaInvalid {
            source: source.to_owned(),
        })?;
    let raw: RawPluginProgramManifest = serde_json::from_value(value)
        .map_err(|source| PluginPackageError::ManifestProjectionFailed { source })?;
    let program_name = ProgramName::try_from(raw.program_name)
        .map_err(|source| PluginPackageError::ManifestIdentifierProjectionFailed { source })?;
    let exact_version = ExactVersion::try_from(raw.exact_version)
        .map_err(|source| PluginPackageError::ManifestIdentifierProjectionFailed { source })?;

    Ok(PluginProgramManifest {
        program_name,
        exact_version,
        display_name: raw.display_name,
        description: raw.description,
        interface: raw.interface,
        command: raw.command.into_boxed_slice(),
        platforms: raw.platforms,
    })
}

#[cfg(test)]
mod test_support {
    use super::{PluginInterface, StagedPluginProgramPackage};
    use crate::identifiers::{ExactVersion, ProgramName};
    use prost_reflect::MessageDescriptor;

    pub(super) fn manifest_fields(
        staged: &StagedPluginProgramPackage,
    ) -> (&ProgramName, &ExactVersion, PluginInterface, &[String]) {
        (
            &staged.validated.manifest.program_name,
            &staged.validated.manifest.exact_version,
            staged.validated.manifest.interface,
            &staged.validated.manifest.command,
        )
    }

    pub(super) fn root_messages(
        staged: &StagedPluginProgramPackage,
    ) -> (Option<MessageDescriptor>, Option<MessageDescriptor>) {
        (
            staged.validated.payload_contract.source_root_message(),
            staged.validated.payload_contract.sink_root_message(),
        )
    }
}

#[cfg(test)]
pub(crate) mod tests;
