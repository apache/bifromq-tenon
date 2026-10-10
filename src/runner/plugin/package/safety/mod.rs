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

//! Safe package directories and Config Schemas.
//!
//! The selected hook writes files through a bounded directory writer.
//! Directory scans reject unsafe filesystem objects. Package validation owns
//! manifest and payload semantics. This module does not publish Store state.

mod output;

use crate::identifiers::IdentifierParseError;
use crate::payload_contract::PayloadContractError;
use crate::runner::extensions::ArtifactProtection;
use crate::runner::private_filesystem::{
    has_owner_only_directory_permission, has_owner_only_file_permission,
    set_owner_only_directory_permission,
};
use crate::strict_jsonc::{StrictJsonError, parse_json};
use output::PackageOutput;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

const MANIFEST_PATH: &str = "manifest.json";
const CONFIG_SCHEMA_PATH: &str = "config.schema.json";
const PAYLOAD_DESCRIPTOR_PATH: &str = "payload.descriptor.pb";
pub(crate) const STAGING_DIRECTORY_PREFIX: &str = ".tenon-plugin-";
const COPY_BUFFER_BYTES: usize = 64 * 1024;
const MAX_PACKAGE_ENTRIES: usize = 100_000;
const MAX_EXTRACTED_FILE_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CONFIG_SCHEMA_BYTES: u64 = 16 * 1024 * 1024;
const MAX_PAYLOAD_DESCRIPTOR_BYTES: u64 = 64 * 1024 * 1024;
const MAX_PACKAGE_PATH_BYTES: usize = 4096;
const MAX_PACKAGE_PATH_COMPONENT_BYTES: usize = 255;
const DRAFT_2020_12_URI: &str = "https://json-schema.org/draft/2020-12/schema";

/// One self-contained Config Schema after Tenon's Draft 2020-12 profile check.
pub(in crate::runner::plugin) struct PluginConfigSchema {
    bytes: Box<[u8]>,
    validator: jsonschema::Validator,
}

impl PluginConfigSchema {
    /// Parses a current Program Schema whose root must explicitly be an object.
    ///
    /// # Errors
    ///
    /// Returns [`PluginConfigSchemaError`] when the bytes are not strict JSON,
    /// omit the explicit object root, or violate the shared Draft 2020-12
    /// profile.
    pub(super) fn parse_program(bytes: Vec<u8>) -> Result<Self, PluginConfigSchemaError> {
        let schema =
            parse_json(&bytes).map_err(|source| PluginConfigSchemaError::JsonInvalid { source })?;
        if schema
            .as_object()
            .and_then(|entries| entries.get("type"))
            .and_then(Value::as_str)
            != Some("object")
        {
            return Err(PluginConfigSchemaError::ProfileViolation {
                path: String::from("/type").into_boxed_str(),
            });
        }
        Self::compile(bytes, schema)
    }

    fn compile(bytes: Vec<u8>, schema: Value) -> Result<Self, PluginConfigSchemaError> {
        if schema
            .as_object()
            .and_then(|entries| entries.get("$schema"))
            .and_then(Value::as_str)
            != Some(DRAFT_2020_12_URI)
        {
            return Err(PluginConfigSchemaError::ProfileViolation {
                path: String::from("/$schema").into_boxed_str(),
            });
        }
        if let Err(error) = jsonschema::draft202012::meta::validate(&schema) {
            return Err(PluginConfigSchemaError::DraftInvalid {
                path: error.instance_path().as_str().into(),
            });
        }
        validate_config_schema_profile(&schema, "")?;
        let validator = jsonschema::draft202012::new(&schema)
            .map_err(|source| PluginConfigSchemaError::CompilationFailed { source })?;

        Ok(Self {
            bytes: bytes.into_boxed_slice(),
            validator,
        })
    }

    /// Returns the exact validated Config Schema bytes.
    #[must_use]
    pub(in crate::runner::plugin) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Checks one Instance config with the validator compiled at package validation.
    pub(in crate::runner::plugin) fn accepts_config(&self, config: &Value) -> bool {
        self.validator.is_valid(config)
    }
}

impl fmt::Debug for PluginConfigSchema {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginConfigSchema")
            .field("byte_len", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// Writes package contents into a private directory through the selected hook.
pub(super) fn stage_package_directory(
    protection: &dyn ArtifactProtection,
    package: impl Read,
    staging_parent: &Path,
) -> Result<StagedPackageDirectory, PluginPackageError> {
    extract_package_directory(
        protection,
        package,
        staging_parent,
        PackageLimits::production(),
    )
}

fn extract_package_directory(
    protection: &dyn ArtifactProtection,
    mut package: impl Read,
    staging_parent: &Path,
    limits: PackageLimits,
) -> Result<StagedPackageDirectory, PluginPackageError> {
    let directory = tempfile::Builder::new()
        .prefix(STAGING_DIRECTORY_PREFIX)
        .tempdir_in(staging_parent)
        .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
    set_owner_only_directory_permission(directory.path())
        .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
    let mut output = PackageOutput::new(directory.path(), limits);
    let result = protection.open_plugin_package(&mut package, &mut output);
    let file_digests = match output.finish(result) {
        Ok(file_digests) => file_digests,
        Err(error) => {
            directory
                .close()
                .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
            return Err(error);
        }
    };
    Ok(StagedPackageDirectory {
        directory,
        file_digests,
    })
}

/// One safely inspected committed package tree before contract-specific parsing.
pub(super) struct InspectedPackageDirectory {
    /// Exact committed `manifest.json` bytes used for parsing and identity comparison.
    pub(super) manifest_bytes: Vec<u8>,
    /// Exact committed `config.schema.json` bytes used for contract validation.
    pub(super) config_schema_bytes: Vec<u8>,
    /// Exact committed `payload.descriptor.pb` bytes used for contract validation.
    pub(super) descriptor_bytes: Vec<u8>,
    /// Transient normalized-path digests for every ordinary file except `manifest.json`.
    pub(super) file_digests: BTreeMap<Box<str>, [u8; 32]>,
}

/// Safely enumerates and reads one committed package tree.
///
/// # Errors
///
/// Returns [`PluginPackageError`] when the tree violates the shared path,
/// permission, object-type, hard-link, size, or fixed-file boundary.
pub(super) fn inspect_package_directory(
    root: &Path,
) -> Result<InspectedPackageDirectory, PluginPackageError> {
    let limits = PackageLimits::production();
    let root_metadata = fs::symlink_metadata(root)
        .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
    if !root_metadata.is_dir() || !has_owner_only_directory_permission(&root_metadata) {
        return Err(PluginPackageError::PackageInvalid);
    }

    let mut directories = vec![root.to_path_buf()];
    let mut file_digests = BTreeMap::new();
    let mut manifest_bytes = None;
    let mut config_schema_bytes = None;
    let mut descriptor_bytes = None;
    let mut entry_count = 0_usize;
    let mut total_file_bytes = 0_u64;

    while let Some(directory) = directories.pop() {
        let entries = fs::read_dir(&directory)
            .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
        for entry in entries {
            entry_count = entry_count
                .checked_add(1)
                .ok_or(PluginPackageError::PackageTooLarge)?;
            if entry_count > limits.entries {
                return Err(PluginPackageError::PackageTooLarge);
            }

            let entry =
                entry.map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .map_err(|_| PluginPackageError::PackageInvalid)?;
            let relative = relative
                .to_str()
                .ok_or(PluginPackageError::PackageInvalid)?;
            let file_type = entry
                .file_type()
                .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
            let entry_kind = if file_type.is_dir() {
                PackageEntryKind::Directory
            } else if file_type.is_file() {
                PackageEntryKind::File
            } else {
                return Err(PluginPackageError::PackageInvalid);
            };
            let normalized = NormalizedPackagePath::parse(relative.as_bytes(), entry_kind)?;
            let metadata = fs::symlink_metadata(&path)
                .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;

            if normalized.kind == PackageEntryKind::Directory {
                if !metadata.is_dir() || !has_owner_only_directory_permission(&metadata) {
                    return Err(PluginPackageError::PackageInvalid);
                }
                directories.push(path);
                continue;
            }
            if !metadata.is_file()
                || !has_owner_only_file_permission(&metadata)
                || has_multiple_hard_links(&metadata)
            {
                return Err(PluginPackageError::PackageInvalid);
            }

            let size = metadata.len();
            total_file_bytes = total_file_bytes
                .checked_add(size)
                .ok_or(PluginPackageError::PackageTooLarge)?;
            if total_file_bytes > limits.extracted_file_bytes
                || control_file_limit(normalized.as_str(), limits).is_some_and(|limit| size > limit)
            {
                return Err(PluginPackageError::PackageTooLarge);
            }

            let capture = control_file_limit(normalized.as_str(), limits).is_some();
            let (digest, bytes) = read_installed_file(&path, size, capture)?;
            match normalized.as_str() {
                MANIFEST_PATH => manifest_bytes = bytes,
                CONFIG_SCHEMA_PATH => config_schema_bytes = bytes,
                PAYLOAD_DESCRIPTOR_PATH => descriptor_bytes = bytes,
                _ => {}
            }
            if normalized.as_str() != MANIFEST_PATH {
                file_digests.insert(normalized.text, digest);
            }
        }
    }

    Ok(InspectedPackageDirectory {
        manifest_bytes: manifest_bytes.ok_or(PluginPackageError::PackageInvalid)?,
        config_schema_bytes: config_schema_bytes.ok_or(PluginPackageError::PackageInvalid)?,
        descriptor_bytes: descriptor_bytes.ok_or(PluginPackageError::PackageInvalid)?,
        file_digests,
    })
}

/// Compares the normalized ordinary-file sets and exact bytes of two safe trees.
///
/// The digest maps provide a fast inequality check only. Equal digests are
/// followed by a streaming byte comparison so the result implements exact
/// content equality rather than hash equality.
///
/// # Errors
///
/// Returns [`PluginPackageError`] when either already-inspected tree can no
/// longer be read completely.
pub(super) fn package_files_are_identical(
    left_root: &Path,
    left_manifest_bytes: &[u8],
    left_file_digests: &BTreeMap<Box<str>, [u8; 32]>,
    right_root: &Path,
    right_manifest_bytes: &[u8],
    right_file_digests: &BTreeMap<Box<str>, [u8; 32]>,
) -> Result<bool, PluginPackageError> {
    if left_manifest_bytes != right_manifest_bytes || left_file_digests != right_file_digests {
        return Ok(false);
    }
    for path in left_file_digests.keys() {
        if !files_are_identical(
            &left_root.join(path.as_ref()),
            &right_root.join(path.as_ref()),
        )? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn files_are_identical(left: &Path, right: &Path) -> Result<bool, PluginPackageError> {
    let mut left = fs::File::open(left)
        .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
    let mut right = fs::File::open(right)
        .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
    let mut left_buffer = [0_u8; COPY_BUFFER_BYTES];
    let mut right_buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let left_count = left
            .read(&mut left_buffer)
            .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
        if left_count == 0 {
            let mut right_probe = [0_u8; 1];
            let right_count = right
                .read(&mut right_probe)
                .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
            return Ok(right_count == 0);
        }
        match right.read_exact(&mut right_buffer[..left_count]) {
            Ok(()) => {}
            Err(source) if source.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(source) => {
                return Err(PluginPackageError::FilesystemOperationFailed { source });
            }
        }
        if left_buffer[..left_count] != right_buffer[..left_count] {
            return Ok(false);
        }
    }
}

fn read_installed_file(
    path: &Path,
    expected_size: u64,
    capture: bool,
) -> Result<([u8; 32], Option<Vec<u8>>), PluginPackageError> {
    let mut file = fs::File::open(path)
        .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
    let opened_metadata = file
        .metadata()
        .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
    if !opened_metadata.is_file()
        || opened_metadata.len() != expected_size
        || !has_owner_only_file_permission(&opened_metadata)
        || has_multiple_hard_links(&opened_metadata)
    {
        return Err(PluginPackageError::PackageInvalid);
    }

    let mut captured = if capture {
        let capacity =
            usize::try_from(expected_size).map_err(|_| PluginPackageError::PackageTooLarge)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| PluginPackageError::PackageTooLarge)?;
        Some(bytes)
    } else {
        None
    };
    let mut digest = Sha256::new();
    let mut observed = 0_u64;
    let mut buffer = [0_u8; COPY_BUFFER_BYTES];
    loop {
        let remaining_with_probe = expected_size.saturating_sub(observed).saturating_add(1);
        let available = usize::try_from(remaining_with_probe)
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let count = file
            .read(&mut buffer[..available])
            .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
        if count == 0 {
            break;
        }
        observed = observed
            .checked_add(count as u64)
            .ok_or(PluginPackageError::PackageTooLarge)?;
        if observed > expected_size {
            return Err(PluginPackageError::PackageInvalid);
        }
        digest.update(&buffer[..count]);
        if let Some(bytes) = &mut captured {
            bytes.extend_from_slice(&buffer[..count]);
        }
    }
    if observed != expected_size {
        return Err(PluginPackageError::PackageInvalid);
    }

    Ok((digest.finalize().into(), captured))
}

#[cfg(unix)]
fn has_multiple_hard_links(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    metadata.nlink() != 1
}

#[cfg(not(unix))]
fn has_multiple_hard_links(_metadata: &fs::Metadata) -> bool {
    false
}

#[derive(Clone, Copy, Debug)]
struct PackageLimits {
    entries: usize,
    extracted_file_bytes: u64,
    manifest_bytes: u64,
    config_schema_bytes: u64,
    payload_descriptor_bytes: u64,
}

impl PackageLimits {
    const fn production() -> Self {
        Self {
            entries: MAX_PACKAGE_ENTRIES,
            extracted_file_bytes: MAX_EXTRACTED_FILE_BYTES,
            manifest_bytes: MAX_MANIFEST_BYTES,
            config_schema_bytes: MAX_CONFIG_SCHEMA_BYTES,
            payload_descriptor_bytes: MAX_PAYLOAD_DESCRIPTOR_BYTES,
        }
    }
}

/// One safely extracted private package tree owned until this value is dropped.
///
/// Extraction has validated archive structure and filesystem safety only;
/// callers still own all fixed-file parsing and contract validation.
#[derive(Debug)]
pub(super) struct StagedPackageDirectory {
    directory: TempDir,
    file_digests: BTreeMap<Box<str>, [u8; 32]>,
}

impl StagedPackageDirectory {
    /// Removes a rejected or unused temporary tree and reports cleanup failure.
    ///
    /// # Errors
    ///
    /// Returns the filesystem error when the owned tree cannot be removed.
    pub(super) fn close(self) -> io::Result<()> {
        self.directory.close()
    }

    /// Returns the owned temporary tree root.
    #[must_use]
    pub(super) fn path(&self) -> &Path {
        self.directory.path()
    }

    /// Returns the extraction-time digest snapshot for every ordinary file
    /// except `manifest.json`.
    #[must_use]
    pub(super) const fn file_digests(&self) -> &BTreeMap<Box<str>, [u8; 32]> {
        &self.file_digests
    }

    /// Releases the staging directory guard and returns its digest snapshot.
    #[must_use]
    pub(super) fn into_file_digests(self) -> BTreeMap<Box<str>, [u8; 32]> {
        self.file_digests
    }

    pub(super) fn keep(&mut self) {
        self.directory.disable_cleanup(true);
    }

    /// Reads the bounded fixed `manifest.json` file.
    ///
    /// # Errors
    ///
    /// Returns [`PluginPackageError`] when the file is missing, not ordinary,
    /// exceeds its fixed limit, or cannot be read.
    pub(super) fn read_manifest(&self) -> Result<Vec<u8>, PluginPackageError> {
        read_control_file(self.path(), MANIFEST_PATH, MAX_MANIFEST_BYTES)
    }

    /// Reads the bounded fixed `config.schema.json` file.
    ///
    /// # Errors
    ///
    /// Returns [`PluginPackageError`] when the file is missing, not ordinary,
    /// exceeds its fixed limit, or cannot be read.
    pub(super) fn read_config_schema(&self) -> Result<Vec<u8>, PluginPackageError> {
        read_control_file(self.path(), CONFIG_SCHEMA_PATH, MAX_CONFIG_SCHEMA_BYTES)
    }

    /// Reads the bounded fixed `payload.descriptor.pb` file.
    ///
    /// # Errors
    ///
    /// Returns [`PluginPackageError`] when the file is missing, not ordinary,
    /// exceeds its fixed limit, or cannot be read.
    pub(super) fn read_payload_descriptor(&self) -> Result<Vec<u8>, PluginPackageError> {
        read_control_file(
            self.path(),
            PAYLOAD_DESCRIPTOR_PATH,
            MAX_PAYLOAD_DESCRIPTOR_BYTES,
        )
    }
}

fn control_file_limit(path: &str, limits: PackageLimits) -> Option<u64> {
    match path {
        MANIFEST_PATH => Some(limits.manifest_bytes),
        CONFIG_SCHEMA_PATH => Some(limits.config_schema_bytes),
        PAYLOAD_DESCRIPTOR_PATH => Some(limits.payload_descriptor_bytes),
        _ => None,
    }
}

fn register_path_shape(
    path: &NormalizedPackagePath,
    known_directories: &mut HashSet<Box<str>>,
    known_files: &mut HashSet<Box<str>>,
) -> Result<(), PluginPackageError> {
    let mut prefix = String::new();
    let component_count = path.components.len();
    for (index, component) in path.components.iter().enumerate() {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(component);
        let is_target = index + 1 == component_count;
        if !is_target || path.kind == PackageEntryKind::Directory {
            if known_files.contains(prefix.as_str()) {
                return Err(PluginPackageError::PackageInvalid);
            }
            known_directories.insert(prefix.clone().into_boxed_str());
        } else {
            if known_directories.contains(prefix.as_str()) {
                return Err(PluginPackageError::PackageInvalid);
            }
            known_files.insert(prefix.as_str().into());
        }
    }
    Ok(())
}

fn read_control_file(
    root: &Path,
    relative_path: &str,
    limit: u64,
) -> Result<Vec<u8>, PluginPackageError> {
    let path = root.join(relative_path);
    let metadata = fs::metadata(&path).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            PluginPackageError::PackageInvalid
        } else {
            PluginPackageError::FilesystemOperationFailed { source }
        }
    })?;
    if !metadata.is_file() {
        return Err(PluginPackageError::PackageInvalid);
    }
    if metadata.len() > limit {
        return Err(PluginPackageError::PackageTooLarge);
    }
    fs::read(path).map_err(|source| PluginPackageError::FilesystemOperationFailed { source })
}

fn validate_config_schema_profile(
    schema: &Value,
    path: &str,
) -> Result<(), PluginConfigSchemaError> {
    let Value::Object(entries) = schema else {
        return Ok(());
    };
    for (keyword, value) in entries {
        let keyword_path = json_pointer_child(path, keyword);
        if keyword == "default" || keyword == "$vocabulary" {
            return Err(PluginConfigSchemaError::ProfileViolation {
                path: keyword_path.into_boxed_str(),
            });
        }
        if !is_draft_2020_12_keyword(keyword) {
            return Err(PluginConfigSchemaError::ProfileViolation {
                path: keyword_path.into_boxed_str(),
            });
        }
        if keyword == "$schema" && value.as_str() != Some(DRAFT_2020_12_URI) {
            return Err(PluginConfigSchemaError::ProfileViolation {
                path: keyword_path.into_boxed_str(),
            });
        }
        if matches!(keyword.as_str(), "$ref" | "$dynamicRef")
            && value
                .as_str()
                .is_some_and(|reference| !is_internal_schema_reference(reference))
        {
            return Err(PluginConfigSchemaError::ProfileViolation {
                path: keyword_path.into_boxed_str(),
            });
        }
        visit_child_schemas(keyword, value, &keyword_path)?;
    }
    Ok(())
}

fn is_internal_schema_reference(reference: &str) -> bool {
    reference.is_empty() || reference.starts_with('#')
}

fn visit_child_schemas(
    keyword: &str,
    value: &Value,
    path: &str,
) -> Result<(), PluginConfigSchemaError> {
    match keyword {
        "$defs" | "properties" | "patternProperties" | "dependentSchemas" => {
            if let Value::Object(schemas) = value {
                for (name, schema) in schemas {
                    validate_config_schema_profile(schema, &json_pointer_child(path, name))?;
                }
            }
        }
        "prefixItems" | "allOf" | "anyOf" | "oneOf" => {
            if let Value::Array(schemas) = value {
                for (index, schema) in schemas.iter().enumerate() {
                    validate_config_schema_profile(schema, &format!("{path}/{index}"))?;
                }
            }
        }
        "additionalProperties"
        | "unevaluatedProperties"
        | "propertyNames"
        | "contains"
        | "items"
        | "unevaluatedItems"
        | "not"
        | "if"
        | "then"
        | "else"
        | "contentSchema" => validate_config_schema_profile(value, path)?,
        _ => {}
    }
    Ok(())
}

fn is_draft_2020_12_keyword(keyword: &str) -> bool {
    matches!(
        keyword,
        "$schema"
            | "$id"
            | "$ref"
            | "$anchor"
            | "$dynamicRef"
            | "$dynamicAnchor"
            | "$comment"
            | "$defs"
            | "prefixItems"
            | "items"
            | "contains"
            | "additionalProperties"
            | "properties"
            | "patternProperties"
            | "dependentSchemas"
            | "propertyNames"
            | "if"
            | "then"
            | "else"
            | "allOf"
            | "anyOf"
            | "oneOf"
            | "not"
            | "unevaluatedItems"
            | "unevaluatedProperties"
            | "type"
            | "const"
            | "enum"
            | "multipleOf"
            | "maximum"
            | "exclusiveMaximum"
            | "minimum"
            | "exclusiveMinimum"
            | "maxLength"
            | "minLength"
            | "pattern"
            | "maxItems"
            | "minItems"
            | "uniqueItems"
            | "maxContains"
            | "minContains"
            | "maxProperties"
            | "minProperties"
            | "required"
            | "dependentRequired"
            | "title"
            | "description"
            | "deprecated"
            | "readOnly"
            | "writeOnly"
            | "examples"
            | "format"
            | "contentEncoding"
            | "contentMediaType"
            | "contentSchema"
    )
}

fn json_pointer_child(parent: &str, segment: &str) -> String {
    let escaped = segment.replace('~', "~0").replace('/', "~1");
    format!("{parent}/{escaped}")
}

fn create_owner_only_directories(
    root: &Path,
    components: &[Box<str>],
) -> Result<(), PluginPackageError> {
    let mut directory = root.to_path_buf();
    for component in components {
        directory.push(component.as_ref());
        fs::create_dir(&directory)
            .or_else(|source| {
                if source.kind() == io::ErrorKind::AlreadyExists && directory.is_dir() {
                    Ok(())
                } else {
                    Err(source)
                }
            })
            .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
        set_owner_only_directory_permission(&directory)
            .map_err(|source| PluginPackageError::FilesystemOperationFailed { source })?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PackageEntryKind {
    File,
    Directory,
}

#[derive(Debug)]
struct NormalizedPackagePath {
    text: Box<str>,
    components: Box<[Box<str>]>,
    kind: PackageEntryKind,
}

impl NormalizedPackagePath {
    fn parse(raw: &[u8], kind: PackageEntryKind) -> Result<Self, PluginPackageError> {
        let text = std::str::from_utf8(raw).map_err(|_| PluginPackageError::PackageInvalid)?;
        let text = match kind {
            PackageEntryKind::File => text,
            PackageEntryKind::Directory => text.strip_suffix('/').unwrap_or(text),
        };
        if text.is_empty()
            || text.len() > MAX_PACKAGE_PATH_BYTES
            || text.contains('\0')
            || text.starts_with('/')
            || (kind == PackageEntryKind::File && text.ends_with('/'))
        {
            return Err(PluginPackageError::PackageInvalid);
        }
        let components = text
            .split('/')
            .map(|component| {
                if component.is_empty()
                    || component.len() > MAX_PACKAGE_PATH_COMPONENT_BYTES
                    || matches!(component, "." | "..")
                {
                    Err(PluginPackageError::PackageInvalid)
                } else {
                    Ok(Box::<str>::from(component))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let normalized = components
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<&str>>()
            .join("/")
            .into_boxed_str();
        Ok(Self {
            text: normalized,
            components: components.into_boxed_slice(),
            kind,
        })
    }

    fn as_str(&self) -> &str {
        &self.text
    }

    fn to_path(&self, root: &Path) -> PathBuf {
        let mut path = root.to_path_buf();
        for component in &self.components {
            path.push(component.as_ref());
        }
        path
    }
}

/// A stable failure category for Plugin package staging and validation.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum PluginPackageError {
    /// The selected package hook could not open the package.
    AccessRejected,
    /// The expanded archive exceeds a fixed Runner implementation limit.
    PackageTooLarge,
    /// The archive structure, path set, or exact ordinary file set is invalid.
    PackageInvalid,
    /// Package decoding failed.
    ArchiveInvalid {
        /// The underlying decoder failure.
        source: io::Error,
    },
    /// The embedded manifest Schema cannot be parsed.
    InternalContractInvalid {
        /// The embedded strict JSON failure.
        source: StrictJsonError,
    },
    /// The embedded manifest Schema cannot be compiled.
    InternalManifestSchemaCompilationFailed {
        /// The embedded Schema compilation failure.
        source: jsonschema::ValidationError<'static>,
    },
    /// `manifest.json` is not strict JSON.
    ManifestJsonInvalid {
        /// The strict JSON failure.
        source: StrictJsonError,
    },
    /// `manifest.json` violates the current embedded Schema.
    ManifestSchemaInvalid {
        /// The precise owned Schema validation failure.
        source: jsonschema::ValidationError<'static>,
    },
    /// The validated manifest and Rust projection disagree.
    ManifestProjectionFailed {
        /// The defensive Serde projection failure.
        source: serde_json::Error,
    },
    /// The validated manifest and domain identifier parser disagree.
    ManifestIdentifierProjectionFailed {
        /// The precise identifier projection failure.
        source: IdentifierParseError,
    },
    /// `config.schema.json` violates the Tenon Schema profile.
    ConfigSchemaInvalid {
        /// The precise Config Schema failure.
        source: PluginConfigSchemaError,
    },
    /// `payload.descriptor.pb` violates its declared Payload Contract.
    PayloadContractInvalid {
        /// The precise shared Payload Contract failure.
        source: PayloadContractError,
    },
    /// The private package filesystem could not complete an operation.
    FilesystemOperationFailed {
        /// The underlying filesystem failure.
        source: io::Error,
    },
}

impl PluginPackageError {
    /// Returns the stable API-facing failure category.
    #[must_use]
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::AccessRejected => "plugin_package_open_failed",
            Self::PackageTooLarge => "plugin_package_too_large",
            Self::PackageInvalid | Self::ArchiveInvalid { .. } => "plugin_package_invalid",
            Self::ManifestJsonInvalid { .. } | Self::ManifestSchemaInvalid { .. } => {
                "plugin_manifest_invalid"
            }
            Self::ConfigSchemaInvalid { .. } => "plugin_config_schema_invalid",
            Self::PayloadContractInvalid { .. } => "plugin_payload_contract_invalid",
            Self::InternalContractInvalid { .. }
            | Self::InternalManifestSchemaCompilationFailed { .. }
            | Self::ManifestProjectionFailed { .. }
            | Self::ManifestIdentifierProjectionFailed { .. }
            | Self::FilesystemOperationFailed { .. } => "plugin_package_internal_error",
        }
    }
}

impl fmt::Display for PluginPackageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AccessRejected => "Plugin package access was rejected",
            Self::PackageTooLarge => "Plugin package exceeds an implementation limit",
            Self::PackageInvalid => "Plugin package structure is invalid",
            Self::ArchiveInvalid { .. } => "Plugin package format is invalid",
            Self::InternalContractInvalid { .. }
            | Self::InternalManifestSchemaCompilationFailed { .. } => {
                "Embedded Plugin manifest contract is invalid"
            }
            Self::ManifestJsonInvalid { .. } | Self::ManifestSchemaInvalid { .. } => {
                "Plugin manifest is invalid"
            }
            Self::ManifestProjectionFailed { .. }
            | Self::ManifestIdentifierProjectionFailed { .. } => {
                "Plugin manifest projection does not match the embedded contract"
            }
            Self::ConfigSchemaInvalid { .. } => "Plugin Config Schema is invalid",
            Self::PayloadContractInvalid { .. } => "Plugin Payload Contract is invalid",
            Self::FilesystemOperationFailed { .. } => "Plugin package filesystem operation failed",
        })
    }
}

impl Error for PluginPackageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ArchiveInvalid { source } | Self::FilesystemOperationFailed { source } => {
                Some(source)
            }
            Self::InternalContractInvalid { source } | Self::ManifestJsonInvalid { source } => {
                Some(source)
            }
            Self::InternalManifestSchemaCompilationFailed { source } => Some(source),
            Self::ManifestSchemaInvalid { source } => Some(source),
            Self::ManifestProjectionFailed { source } => Some(source),
            Self::ManifestIdentifierProjectionFailed { source } => Some(source),
            Self::ConfigSchemaInvalid { source } => Some(source),
            Self::PayloadContractInvalid { source } => Some(source),
            Self::AccessRejected | Self::PackageTooLarge | Self::PackageInvalid => None,
        }
    }
}

/// A precise failure to compile the restricted Plugin Config Schema.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum PluginConfigSchemaError {
    /// The file is not strict JSON.
    JsonInvalid {
        /// The strict JSON failure.
        source: StrictJsonError,
    },
    /// The document is not a valid Draft 2020-12 Schema.
    DraftInvalid {
        /// The JSON Pointer of the first invalid Schema value.
        path: Box<str>,
    },
    /// The document uses a forbidden dialect, keyword, default, or external reference.
    ProfileViolation {
        /// The JSON Pointer of the forbidden value.
        path: Box<str>,
    },
    /// The valid profile could not be compiled by the runtime validator.
    CompilationFailed {
        /// The runtime compiler failure.
        source: jsonschema::ValidationError<'static>,
    },
}

impl fmt::Display for PluginConfigSchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::JsonInvalid { .. } => {
                formatter.write_str("Plugin Config Schema is not strict JSON")
            }
            Self::DraftInvalid { path } => {
                write!(
                    formatter,
                    "Plugin Config Schema is not valid Draft 2020-12 at {path}"
                )
            }
            Self::ProfileViolation { path } => {
                write!(
                    formatter,
                    "Plugin Config Schema violates the Tenon profile at {path}"
                )
            }
            Self::CompilationFailed { .. } => {
                formatter.write_str("Plugin Config Schema cannot be compiled")
            }
        }
    }
}

impl Error for PluginConfigSchemaError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::JsonInvalid { source } => Some(source),
            Self::CompilationFailed { source } => Some(source),
            Self::DraftInvalid { .. } | Self::ProfileViolation { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests;
