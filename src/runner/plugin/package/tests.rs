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

use super::test_support::{manifest_fields, root_messages};
use super::{parse_manifest, stage_plugin_program_package};
use crate::contracts::plugin::manifest_schema_bytes;
use crate::payload_contract::{PluginInterface, PluginProgramPayloadContract};
use crate::runner::plugin::package::PluginPackageError;
#[path = "../../../../tests/support/plugin_platform.rs"]
mod plugin_platform;
pub(crate) use plugin_platform::foreign_platform;

use flate2::Compression;
use flate2::write::GzEncoder;
use serde::Deserialize;
use serde_json::{Value, json};
use std::fs;
use std::io::{self, Cursor};
use std::process::Command;
use tar::{Builder, Header};

const MANIFEST_VECTORS: &[u8] =
    include_bytes!("../../../../contracts/plugin/test-fixtures/manifest.test-vectors.json");
const PAYLOAD_VECTORS: &[u8] =
    include_bytes!("../../../../contracts/plugin/test-fixtures/payload_contract_test_vectors.json");

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestVectors {
    valid: Vec<ManifestVector>,
    invalid: Vec<ManifestVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestVector {
    name: String,
    manifest: Value,
    #[serde(default)]
    expected_instance_pointer: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PayloadVectors {
    format_version: u32,
    valid: Vec<ValidPayloadVector>,
    #[serde(rename = "projectionChanges")]
    projection_changes: Vec<ProjectionChangeVector>,
    invalid: Vec<InvalidPayloadVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ValidPayloadVector {
    name: String,
    interface: PluginInterface,
    files: Vec<ProtoSourceFile>,
    entry_files: Vec<String>,
    expected_source_root_message: Option<String>,
    expected_sink_root_message: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvalidPayloadVector {
    name: String,
    interface: PluginInterface,
    files: Vec<ProtoSourceFile>,
    entry_files: Vec<String>,
    expected_error_code: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProjectionChangeVector {
    name: String,
    interface: PluginInterface,
    before_files: Vec<ProtoSourceFile>,
    after_files: Vec<ProtoSourceFile>,
    entry_files: Vec<String>,
    expected_changed_interfaces: Vec<PluginInterface>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProtoSourceFile {
    name: String,
    content: String,
}

#[test]
fn current_manifest_vectors_match_the_new_parser() -> io::Result<()> {
    let vectors: ManifestVectors = serde_json::from_slice(MANIFEST_VECTORS)?;
    for vector in vectors.valid {
        let bytes = serde_json::to_vec(&vector.manifest)?;
        parse_manifest(&bytes).map_err(|error| {
            io::Error::other(format!(
                "valid manifest vector {} was rejected: {error}",
                vector.name
            ))
        })?;
    }
    for vector in vectors.invalid {
        let bytes = serde_json::to_vec(&vector.manifest)?;
        let error = match parse_manifest(&bytes) {
            Ok(_) => {
                return Err(io::Error::other(format!(
                    "invalid manifest vector {} was accepted",
                    vector.name
                )));
            }
            Err(error) => error,
        };
        let PluginPackageError::ManifestSchemaInvalid { source } = error else {
            return Err(io::Error::other(format!(
                "invalid manifest vector {} returned {error}",
                vector.name
            )));
        };
        assert_eq!(
            source.instance_path().as_str(),
            vector.expected_instance_pointer.as_deref().unwrap_or(""),
            "{}",
            vector.name
        );
    }
    Ok(())
}

#[test]
fn embedded_manifest_schema_is_the_current_repository_contract() {
    assert_eq!(
        manifest_schema_bytes(),
        include_bytes!("../../../../contracts/plugin/manifest.schema.json")
    );
}

#[test]
fn current_payload_vectors_stage_one_shared_program_contract() -> io::Result<()> {
    let vectors: PayloadVectors = serde_json::from_slice(PAYLOAD_VECTORS)?;
    assert_eq!(vectors.format_version, 1);

    for vector in vectors.valid {
        let descriptor = compile_descriptor(&vector.files, &vector.entry_files)?;
        let package = program_package(vector.interface, descriptor.clone(), valid_config_schema())?;
        let staging_parent = tempfile::tempdir()?;
        let staged = stage_plugin_program_package(Cursor::new(package), staging_parent.path())
            .map_err(|error| {
                io::Error::other(format!(
                    "valid payload vector {} was rejected: {error}",
                    vector.name
                ))
            })?;

        let (program_name, exact_version, interface, command) = manifest_fields(&staged);
        assert_eq!(interface, vector.interface);
        assert_eq!(exact_version.as_str(), "1.0.0");
        assert_eq!(command, ["./bin/start"]);
        assert_eq!(
            program_name.as_str(),
            match vector.interface {
                PluginInterface::Source => "com.example.source",
                PluginInterface::Sink => "com.example.sink",
                PluginInterface::SourceAndSink => "com.example.gateway",
            }
        );
        let (source_root, sink_root) = root_messages(&staged);
        assert_eq!(
            source_root.as_ref().map(|message| message.full_name()),
            vector.expected_source_root_message.as_deref(),
            "{}",
            vector.name
        );
        assert_eq!(
            sink_root.as_ref().map(|message| message.full_name()),
            vector.expected_sink_root_message.as_deref(),
            "{}",
            vector.name
        );
        assert_eq!(fs::read(staged.path().join("bin/start"))?, b"program");
    }
    Ok(())
}

#[test]
fn current_projection_change_vectors_follow_reachable_type_closures() -> io::Result<()> {
    let vectors: PayloadVectors = serde_json::from_slice(PAYLOAD_VECTORS)?;
    for vector in vectors.projection_changes {
        let before = PluginProgramPayloadContract::parse(
            compile_descriptor(&vector.before_files, &vector.entry_files)?,
            vector.interface,
        )
        .map_err(io::Error::other)?;
        let after = PluginProgramPayloadContract::parse(
            compile_descriptor(&vector.after_files, &vector.entry_files)?,
            vector.interface,
        )
        .map_err(io::Error::other)?;

        let source_changed = !before
            .source_projection()
            .zip(after.source_projection())
            .is_some_and(|(before, after)| {
                before.structural_material() == after.structural_material()
            });
        let sink_changed = !before
            .sink_projection()
            .zip(after.sink_projection())
            .is_some_and(|(before, after)| {
                before.structural_material() == after.structural_material()
            });

        assert_eq!(
            source_changed,
            vector
                .expected_changed_interfaces
                .contains(&PluginInterface::Source),
            "{}",
            vector.name
        );
        assert_eq!(
            sink_changed,
            vector
                .expected_changed_interfaces
                .contains(&PluginInterface::Sink),
            "{}",
            vector.name
        );
    }
    Ok(())
}

#[test]
fn duplicate_standard_roots_are_rejected_before_publication() -> io::Result<()> {
    let descriptor = compile_descriptor(
        &[
            ProtoSourceFile {
                name: String::from("first.proto"),
                content: String::from(
                    "syntax = \"proto3\"; package first; message SourceRecordPayload {}\n",
                ),
            },
            ProtoSourceFile {
                name: String::from("second.proto"),
                content: String::from(
                    "syntax = \"proto3\"; package second; message SourceRecordPayload {}\n",
                ),
            },
        ],
        &[String::from("first.proto"), String::from("second.proto")],
    )?;
    let error = stage_error(program_package(
        PluginInterface::Source,
        descriptor,
        valid_config_schema(),
    )?)?;
    assert_eq!(
        program_payload_contract_code(&error),
        Some("payload_contract.source_root_not_unique")
    );
    Ok(())
}

#[test]
fn current_payload_invalid_vectors_preserve_their_root_error_codes() -> io::Result<()> {
    let vectors: PayloadVectors = serde_json::from_slice(PAYLOAD_VECTORS)?;
    for vector in vectors.invalid {
        let descriptor = compile_descriptor(&vector.files, &vector.entry_files)?;
        let package = program_package(vector.interface, descriptor, valid_config_schema())?;
        let error = stage_error(package)?;
        assert_eq!(
            program_payload_contract_code(&error),
            Some(vector.expected_error_code.as_str()),
            "{}",
            vector.name
        );
    }
    Ok(())
}

#[test]
fn fixed_contract_files_are_required_but_other_files_come_from_the_archive() -> io::Result<()> {
    let descriptor = source_descriptor()?;
    for missing in [
        "manifest.json",
        "config.schema.json",
        "payload.descriptor.pb",
    ] {
        let package = program_package_without(
            PluginInterface::Source,
            descriptor.clone(),
            valid_config_schema(),
            missing,
        )?;
        assert_eq!(stage_error(package)?.code(), "plugin_package_invalid");
    }

    let package = program_package(PluginInterface::Source, descriptor, valid_config_schema())?;
    let staging_parent = tempfile::tempdir()?;
    let staged = stage_plugin_program_package(Cursor::new(package), staging_parent.path())
        .map_err(io::Error::other)?;
    assert_eq!(fs::read(staged.path().join("resources/data.bin"))?, b"data");
    Ok(())
}

#[test]
fn current_config_schema_requires_an_explicit_object_root() -> io::Result<()> {
    let invalid_schema = serde_json::to_vec(&json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "string"
    }))?;
    let package = program_package(
        PluginInterface::Source,
        source_descriptor()?,
        invalid_schema,
    )?;
    assert_eq!(stage_error(package)?.code(), "plugin_config_schema_invalid");
    Ok(())
}

#[test]
fn malformed_manifest_json_and_duplicate_identity_fields_are_rejected() -> io::Result<()> {
    for manifest in [
        b"{".as_slice(),
        br#"{"programName":"com.example.source","programName":"com.example.other","exactVersion":"1.0.0","interface":"source","command":["./bin/start"]}"#,
    ] {
        let package = archive(vec![
            ("manifest.json", manifest.to_vec()),
            ("config.schema.json", valid_config_schema()),
            ("payload.descriptor.pb", source_descriptor()?),
            ("bin/start", b"program".to_vec()),
        ])?;
        assert_eq!(stage_error(package)?.code(), "plugin_manifest_invalid");
    }
    Ok(())
}

#[test]
fn failed_validation_and_owner_drop_remove_the_staging_tree() -> io::Result<()> {
    let staging_parent = tempfile::tempdir()?;
    let invalid = program_package(
        PluginInterface::Sink,
        source_descriptor()?,
        valid_config_schema(),
    )?;
    assert!(stage_plugin_program_package(Cursor::new(invalid), staging_parent.path()).is_err());
    assert!(staging_parent.path().read_dir()?.next().is_none());

    let staged = stage_plugin_program_package(
        Cursor::new(program_package(
            PluginInterface::Source,
            source_descriptor()?,
            valid_config_schema(),
        )?),
        staging_parent.path(),
    )
    .map_err(io::Error::other)?;
    let path = staged.path().to_path_buf();
    drop(staged);
    assert!(!path.exists());
    Ok(())
}

fn stage_error(package: Vec<u8>) -> io::Result<PluginPackageError> {
    let staging_parent = tempfile::tempdir()?;
    stage_plugin_program_package(Cursor::new(package), staging_parent.path())
        .err()
        .ok_or_else(|| io::Error::other("invalid Plugin Program package was accepted"))
}

#[cfg(unix)]
mod cleanup {
    use super::*;
    use std::io::Read;
    use std::os::unix::fs::PermissionsExt as _;
    use std::path::{Path, PathBuf};

    #[test]
    fn rejection_cleanup_failure_is_not_disguised_as_invalid_input() -> io::Result<()> {
        if rustix::process::geteuid().is_root() {
            return Ok(());
        }
        for package in [
            b"not gzip".to_vec(),
            source_program_package_with_command("")?,
        ] {
            let directory = tempfile::tempdir()?;
            let permission = ParentPermissions::new(directory.path())?;
            let input = RevokeParentWrite {
                input: Cursor::new(package),
                permission: &permission,
            };
            let error = stage_plugin_program_package(input, directory.path())
                .err()
                .ok_or_else(|| io::Error::other("Invalid input was accepted"))?;
            drop(permission);
            assert!(
                matches!(error, PluginPackageError::FilesystemOperationFailed { source }
                if source.kind() == io::ErrorKind::PermissionDenied)
            );
        }
        Ok(())
    }

    #[test]
    fn unused_validated_tree_cleanup_reports_filesystem_failure() -> io::Result<()> {
        if rustix::process::geteuid().is_root() {
            return Ok(());
        }
        let directory = tempfile::tempdir()?;
        let staged = stage_plugin_program_package(
            Cursor::new(valid_source_program_package()?),
            directory.path(),
        )
        .map_err(io::Error::other)?;
        let permission = ParentPermissions::new(directory.path())?;
        permission.revoke_write()?;
        let error = staged
            .close()
            .err()
            .ok_or_else(|| io::Error::other("Cleanup failure was ignored"))?;
        drop(permission);
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        Ok(())
    }

    struct ParentPermissions {
        path: PathBuf,
        original: fs::Permissions,
    }

    impl ParentPermissions {
        fn new(path: &Path) -> io::Result<Self> {
            Ok(Self {
                path: path.to_owned(),
                original: fs::metadata(path)?.permissions(),
            })
        }

        fn revoke_write(&self) -> io::Result<()> {
            fs::set_permissions(&self.path, fs::Permissions::from_mode(0o500))
        }
    }

    impl Drop for ParentPermissions {
        fn drop(&mut self) {
            let _ = fs::set_permissions(&self.path, self.original.clone());
        }
    }

    struct RevokeParentWrite<'a> {
        input: Cursor<Vec<u8>>,
        permission: &'a ParentPermissions,
    }

    impl Read for RevokeParentWrite<'_> {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            self.permission.revoke_write()?;
            self.input.read(output)
        }
    }
}

fn program_payload_contract_code(error: &PluginPackageError) -> Option<&'static str> {
    let PluginPackageError::PayloadContractInvalid { source } = error else {
        return None;
    };
    Some(source.code())
}

fn source_descriptor() -> io::Result<Vec<u8>> {
    compile_descriptor(
        &[ProtoSourceFile {
            name: String::from("source_record_payload.proto"),
            content: String::from(
                "syntax = \"proto3\"; message SourceRecordPayload { string value = 1; }\n",
            ),
        }],
        &[String::from("source_record_payload.proto")],
    )
}

pub(crate) fn valid_config_schema() -> Vec<u8> {
    br#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}"#.to_vec()
}

pub(crate) fn valid_source_program_package() -> io::Result<Vec<u8>> {
    source_program_package_with_resource_shape(
        "resources/data.bin",
        b"data",
        "./bin/start",
        ArchiveShape::Canonical,
    )
}

pub(crate) fn valid_program_package(interface: PluginInterface) -> io::Result<Vec<u8>> {
    program_package(
        interface,
        valid_program_descriptor(interface)?,
        valid_config_schema(),
    )
}

pub(crate) fn valid_program_descriptor(interface: PluginInterface) -> io::Result<Vec<u8>> {
    match interface {
        PluginInterface::Source => source_descriptor(),
        PluginInterface::Sink => compile_descriptor(
            &[ProtoSourceFile {
                name: String::from("sink_record_payload.proto"),
                content: String::from(
                    "syntax = \"proto3\"; message SinkRecordPayload { string value = 1; }\n",
                ),
            }],
            &[String::from("sink_record_payload.proto")],
        ),
        PluginInterface::SourceAndSink => compile_descriptor(
            &[ProtoSourceFile {
                name: String::from("record_payloads.proto"),
                content: String::from(
                    "syntax = \"proto3\"; message SourceRecordPayload { string value = 1; } message SinkRecordPayload { string value = 1; }\n",
                ),
            }],
            &[String::from("record_payloads.proto")],
        ),
    }
}

pub(crate) fn equivalent_source_program_package() -> io::Result<Vec<u8>> {
    source_program_package_with_resource_shape(
        "resources/data.bin",
        b"data",
        "./bin/start",
        ArchiveShape::ReorderedWithDirectories,
    )
}

pub(crate) fn source_program_package_with_resource(
    path: &str,
    bytes: &[u8],
) -> io::Result<Vec<u8>> {
    source_program_package_with_resource_shape(path, bytes, "./bin/start", ArchiveShape::Canonical)
}

pub(crate) fn source_program_package_with_command(command: &str) -> io::Result<Vec<u8>> {
    source_program_package_with_resource_shape(
        "resources/data.bin",
        b"data",
        command,
        ArchiveShape::Canonical,
    )
}

fn source_program_package_with_resource_shape(
    path: &str,
    bytes: &[u8],
    command: &str,
    shape: ArchiveShape,
) -> io::Result<Vec<u8>> {
    let manifest = serde_json::to_vec(&json!({
        "programName": "com.example.source",
        "exactVersion": "1.0.0",
        "interface": "source",
        "platforms": [crate::runner::plugin::platform::Platform::CURRENT], "displayName": "Example Plugin", "description": "Read and write example records.", "command": [command]
    }))?;
    archive_with_shape(
        vec![
            ("manifest.json", manifest),
            ("config.schema.json", valid_config_schema()),
            ("payload.descriptor.pb", source_descriptor()?),
            ("bin/start", b"program".to_vec()),
            (path, bytes.to_vec()),
        ],
        shape,
    )
}

pub(crate) fn program_package(
    interface: PluginInterface,
    descriptor: Vec<u8>,
    config_schema: Vec<u8>,
) -> io::Result<Vec<u8>> {
    program_package_without(interface, descriptor, config_schema, "")
}

fn program_package_without(
    interface: PluginInterface,
    descriptor: Vec<u8>,
    config_schema: Vec<u8>,
    missing: &str,
) -> io::Result<Vec<u8>> {
    let manifest = serde_json::to_vec(&json!({
        "programName": match interface {
            PluginInterface::Source => "com.example.source",
            PluginInterface::Sink => "com.example.sink",
            PluginInterface::SourceAndSink => "com.example.gateway",
        },
        "exactVersion": "1.0.0",
        "interface": interface_text(interface),
        "platforms": [crate::runner::plugin::platform::Platform::CURRENT], "displayName": "Example Plugin", "description": "Read and write example records.", "command": ["./bin/start"]
    }))?;
    let mut files = vec![
        ("manifest.json", manifest),
        ("config.schema.json", config_schema),
        ("payload.descriptor.pb", descriptor),
        ("bin/start", b"program".to_vec()),
        ("resources/data.bin", b"data".to_vec()),
    ];
    files.retain(|(path, _)| *path != missing);
    archive(files)
}

const fn interface_text(interface: PluginInterface) -> &'static str {
    match interface {
        PluginInterface::Source => "source",
        PluginInterface::Sink => "sink",
        PluginInterface::SourceAndSink => "source-and-sink",
    }
}

pub(crate) fn archive(files: Vec<(&str, Vec<u8>)>) -> io::Result<Vec<u8>> {
    archive_with_shape(files, ArchiveShape::Canonical)
}

#[derive(Clone, Copy)]
enum ArchiveShape {
    Canonical,
    ReorderedWithDirectories,
}

fn archive_with_shape(mut files: Vec<(&str, Vec<u8>)>, shape: ArchiveShape) -> io::Result<Vec<u8>> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut archive = Builder::new(encoder);
    if matches!(shape, ArchiveShape::ReorderedWithDirectories) {
        for path in ["bin/", "resources/"] {
            let mut header = Header::new_gnu();
            header.set_entry_type(tar::EntryType::Directory);
            header.set_mode(0o777);
            header.set_size(0);
            header.set_cksum();
            archive.append_data(&mut header, path, io::empty())?;
        }
        files.reverse();
    }
    for (path, bytes) in files {
        let mut header = Header::new_gnu();
        header.set_mode(match shape {
            ArchiveShape::Canonical => 0o755,
            ArchiveShape::ReorderedWithDirectories => 0o600,
        });
        header.set_size(bytes.len() as u64);
        header.set_cksum();
        archive.append_data(&mut header, path, bytes.as_slice())?;
    }
    let encoder = archive.into_inner()?;
    encoder.finish()
}

fn compile_descriptor(files: &[ProtoSourceFile], entry_files: &[String]) -> io::Result<Vec<u8>> {
    let source_directory = tempfile::tempdir()?;
    for file in files {
        let path = source_directory.path().join(&file.name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, file.content.as_bytes())?;
    }
    let descriptor_path = source_directory.path().join("payload.descriptor.pb");
    let protoc = protoc_bin_vendored::protoc_bin_path().map_err(io::Error::other)?;
    let mut command = Command::new(protoc);
    command
        .arg(format!(
            "--proto_path={}",
            source_directory.path().display()
        ))
        .arg(format!(
            "--descriptor_set_out={}",
            descriptor_path.display()
        ))
        .arg("--include_imports")
        .arg("--include_source_info");
    for entry_file in entry_files {
        command.arg(entry_file);
    }
    let output = command.output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "protoc failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    fs::read(descriptor_path)
}
pub(crate) use super::safety::STAGING_DIRECTORY_PREFIX;

pub(crate) fn package_with_platforms(package: &[u8], platforms: &Value) -> io::Result<Vec<u8>> {
    use std::io::Read as _;
    let mut input = tar::Archive::new(flate2::read::GzDecoder::new(package));
    let mut files = Vec::new();
    for entry in input.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_string_lossy().into_owned();
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes)?;
        if path == "manifest.json" {
            let mut manifest: Value = serde_json::from_slice(&bytes)?;
            manifest["platforms"] = platforms.clone();
            bytes = serde_json::to_vec(&manifest)?;
        }
        files.push((path, bytes));
    }
    archive(
        files
            .iter()
            .map(|(path, bytes)| (path.as_str(), bytes.clone()))
            .collect(),
    )
}

#[test]
fn platform_membership_preserves_complete_pairs_and_ignores_order() -> io::Result<()> {
    use crate::runner::plugin::platform::Platform;
    let mut manifest = json!({
        "programName": "com.example.portable", "exactVersion": "1.0.0",
        "interface": "source", "displayName": "Example Plugin", "description": "Read and write example records.", "command": ["java"],
        "platforms": [{"os":"linux","architecture":"amd64"},{"os":"darwin","architecture":"arm64"}]
    });
    for _ in 0..2 {
        let parsed = parse_manifest(&serde_json::to_vec(&manifest)?).map_err(io::Error::other)?;
        for (os, architecture, expected) in [
            ("linux", "amd64", true),
            ("linux", "arm64", false),
            ("darwin", "amd64", false),
            ("darwin", "arm64", true),
        ] {
            let platform: Platform =
                serde_json::from_value(json!({"os":os,"architecture":architecture}))?;
            assert_eq!(
                parsed.platforms.contains(&platform),
                expected,
                "{os}/{architecture}"
            );
        }
        manifest["platforms"]
            .as_array_mut()
            .ok_or_else(|| io::Error::other("Test platform array is missing"))?
            .reverse();
    }
    Ok(())
}
