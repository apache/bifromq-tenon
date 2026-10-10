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

#![allow(
    dead_code,
    reason = "Shared fixture exports differ by integration test"
)]

use flate2::Compression;
use flate2::write::GzEncoder;
use prost::Message as _;
use prost_types::{
    DescriptorProto, FileDescriptorProto, FileDescriptorSet, SourceCodeInfo, source_code_info,
};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};

#[path = "../support/file_tree.rs"]
pub(crate) mod file_tree;

pub(crate) use tenon::runner_test_support::contracts::core::PluginInterface;
#[path = "../support/controlled_plugin.rs"]
mod controlled_plugin;
pub(crate) use controlled_plugin::controlled_program_command;

/// Original contract material for checking read-only Program responses.
pub(crate) struct InstalledProgramFixture {
    pub(crate) config_schema: Vec<u8>,
    pub(crate) payload_descriptor: Vec<u8>,
}

/// Seeds the current on-disk layout with the same runnable Program used by uploads.
pub(crate) fn install_program(
    state_directory: &Path,
    interface: PluginInterface,
) -> io::Result<InstalledProgramFixture> {
    let files = program_files(interface, None)?;
    let (program_name, _) = program_identity(interface);
    let fixture = InstalledProgramFixture {
        config_schema: files["config.schema.json"].clone(),
        payload_descriptor: files["payload.descriptor.pb"].clone(),
    };
    write_package(state_directory, program_name, "1.0.0", &archive(files)?)?;
    Ok(fixture)
}

pub(crate) fn stored_package_path(state: &Path, name: &str, version: &str) -> PathBuf {
    let hash: String = Sha256::digest(version.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    state
        .join("plugins/programs")
        .join(name)
        .join(format!(".tenon-artifact-{hash}"))
}

pub(crate) fn write_package(
    state: &Path,
    name: &str,
    version: &str,
    bytes: &[u8],
) -> io::Result<()> {
    let plugins = state.join("plugins");
    let programs = plugins.join("programs");
    let namespace = programs.join(name);
    for directory in [&plugins, &programs, &namespace] {
        create_private_directory(directory)?;
    }
    let path = stored_package_path(state, name, version);
    fs::write(&path, bytes)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

pub(crate) fn runtime_program_directory(
    state: &Path,
    name: &str,
    version: &str,
) -> io::Result<PathBuf> {
    for manifest in file_tree::named_files(&state.join("pipelines"), "manifest.json")? {
        let value: serde_json::Value = serde_json::from_slice(&fs::read(&manifest)?)?;
        if value["programName"] == name && value["exactVersion"] == version {
            return manifest
                .parent()
                .map(Path::to_path_buf)
                .ok_or_else(|| io::Error::other("Runtime manifest has no parent"));
        }
    }
    Err(io::Error::other("Runtime Program directory is missing"))
}

pub(crate) fn plugin_package(interface: PluginInterface) -> io::Result<Vec<u8>> {
    archive(program_files(interface, None)?)
}

pub(crate) fn plugin_package_with_program(
    interface: PluginInterface,
    program: &[u8],
) -> io::Result<Vec<u8>> {
    archive(program_files(interface, Some(program))?)
}

pub(crate) fn program_files(
    interface: PluginInterface,
    program: Option<&[u8]>,
) -> io::Result<BTreeMap<String, Vec<u8>>> {
    let (program_name, interface_text) = program_identity(interface);
    let command = if program.is_some() {
        vec!["./plugin.sh".to_owned()]
    } else {
        controlled_program_command(interface)?
    };
    let mut files = BTreeMap::from([
        (
            "manifest.json".into(),
            serde_json::to_vec(&json!({
                "programName": program_name, "exactVersion": "1.0.0",
                "interface": interface_text, "displayName": "Example Plugin", "description": "Read and write example records.", "command": command,
                "platforms": platforms()
            }))?,
        ),
        (
            "config.schema.json".into(),
            serde_json::to_vec(&json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "endpoint": {"type": "string"},
                    "behavior": {"type": "string"},
                    "traffic": {"type": "object"}
                },
                "additionalProperties": false
            }))?,
        ),
        (
            "payload.descriptor.pb".into(),
            program_payload_descriptor(interface),
        ),
    ]);
    if let Some(program) = program {
        files.insert("plugin.sh".into(), program.to_vec());
    }
    Ok(files)
}

fn program_identity(interface: PluginInterface) -> (&'static str, &'static str) {
    match interface {
        PluginInterface::Source => ("com.example.modbus", "source"),
        PluginInterface::Sink => ("com.example.kafka", "sink"),
        PluginInterface::SourceAndSink => ("com.example.gateway", "source-and-sink"),
    }
}

pub(crate) fn archive(files: BTreeMap<String, Vec<u8>>) -> io::Result<Vec<u8>> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut archive = tar::Builder::new(encoder);
    for (path, bytes) in files {
        let mut header = tar::Header::new_gnu();
        header.set_size(u64::try_from(bytes.len()).map_err(io::Error::other)?);
        header.set_mode(0o500);
        header.set_cksum();
        archive.append_data(&mut header, &path, bytes.as_slice())?;
    }
    archive.into_inner()?.finish()
}

pub(crate) fn program_payload_descriptor(interface: PluginInterface) -> Vec<u8> {
    let file = match interface {
        PluginInterface::Source => {
            vec![payload_descriptor_file(PluginInterface::Source)]
        }
        PluginInterface::Sink => {
            vec![payload_descriptor_file(PluginInterface::Sink)]
        }
        PluginInterface::SourceAndSink => vec![
            payload_descriptor_file(PluginInterface::Source),
            payload_descriptor_file(PluginInterface::Sink),
        ],
    };
    FileDescriptorSet { file }.encode_to_vec()
}

fn payload_descriptor_file(kind: PluginInterface) -> FileDescriptorProto {
    let (file_name, message_name, package) = match kind {
        PluginInterface::Source => (
            "source_record_payload.proto",
            "SourceRecordPayload",
            "com.example.modbus",
        ),
        PluginInterface::SourceAndSink => unreachable!("Each file describes exactly one interface"),
        PluginInterface::Sink => (
            "sink_record_payload.proto",
            "SinkRecordPayload",
            "com.example.kafka",
        ),
    };
    FileDescriptorProto {
        name: Some(String::from(file_name)),
        package: Some(String::from(package)),
        message_type: vec![DescriptorProto {
            name: Some(String::from(message_name)),
            ..Default::default()
        }],
        source_code_info: Some(SourceCodeInfo {
            location: vec![source_code_info::Location::default()],
        }),
        syntax: Some(String::from("proto3")),
        ..Default::default()
    }
}

fn create_private_directory(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

pub(crate) fn platforms() -> serde_json::Value {
    json!([{"os":"linux","architecture":"amd64"},{"os":"linux","architecture":"arm64"},{"os":"darwin","architecture":"amd64"},{"os":"darwin","architecture":"arm64"}])
}
