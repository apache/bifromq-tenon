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

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::payload_contract::{PluginInterface, PluginProgramPayloadContract};
use proptest::prelude::*;
use prost::Message;
use prost_types::{FileDescriptorSet, field_descriptor_proto};
use serde::Deserialize;

const TEST_VECTORS: &[u8] =
    include_bytes!("../../../contracts/sink/test-fixtures/payload_contract_test_vectors.json");
const SOURCE_TEST_VECTORS: &[u8] =
    include_bytes!("../../../contracts/source/test-fixtures/payload_contract_test_vectors.json");
static NEXT_TEMPORARY_DIRECTORY: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PayloadContractTestVectors {
    format_version: u32,
    valid: Vec<ValidVector>,
    invalid: Vec<InvalidVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ValidVector {
    name: String,
    files: Vec<SourceFile>,
    entry_files: Vec<String>,
    expected_root_message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvalidVector {
    name: String,
    files: Vec<SourceFile>,
    entry_files: Vec<String>,
    #[serde(default = "enabled")]
    include_imports: bool,
    #[serde(default = "enabled")]
    include_source_info: bool,
    mutation: Option<Mutation>,
    expected_error_code: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceFile {
    name: String,
    content: String,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Mutation {
    Truncate,
    ClearFiles,
    ClearRootFileName,
    DuplicateRootFile,
    InjectEditionMarker,
    DuplicateRootMessage,
    ReplaceFieldWithMissingMessage,
}

#[derive(Debug)]
struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn create() -> io::Result<Self> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let sequence = NEXT_TEMPORARY_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "tenon-payload-contract-test-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn sink_payload_contract_vectors_have_unique_results() -> io::Result<()> {
    let vectors: PayloadContractTestVectors =
        serde_json::from_slice(TEST_VECTORS).map_err(io::Error::other)?;
    assert_eq!(
        vectors.format_version, 1,
        "Unexpected vector format version"
    );
    let temporary_directory = TemporaryDirectory::create()?;

    for (index, vector) in vectors.valid.iter().enumerate() {
        let descriptor = compile_descriptor(
            temporary_directory.path(),
            index,
            &vector.files,
            &vector.entry_files,
            true,
            true,
        )?;
        let contract =
            PluginProgramPayloadContract::parse(descriptor.clone(), PluginInterface::Sink)
                .map_err(|error| {
                    io::Error::other(format!("Valid vector failed: {}: {error}", vector.name))
                })?;

        let reconstructed =
            PluginProgramPayloadContract::from_runner_bytes(contract.descriptor_bytes().to_vec());
        assert_eq!(
            reconstructed.descriptor_bytes(),
            descriptor,
            "{}",
            vector.name
        );
        assert_eq!(
            reconstructed.interface(),
            contract.interface(),
            "{}",
            vector.name
        );
        assert_eq!(
            reconstructed
                .descriptor_pool
                .files()
                .map(|file| file.file_descriptor_proto().clone())
                .collect::<Vec<_>>(),
            contract
                .descriptor_pool
                .files()
                .map(|file| file.file_descriptor_proto().clone())
                .collect::<Vec<_>>(),
            "{}",
            vector.name
        );
        assert_eq!(contract.descriptor_bytes(), descriptor, "{}", vector.name);
        assert_eq!(
            contract
                .sink_root_message()
                .ok_or_else(|| io::Error::other("Sink root is missing"))?
                .full_name(),
            vector.expected_root_message,
            "{}",
            vector.name
        );
    }

    let invalid_offset = vectors.valid.len();
    for (index, vector) in vectors.invalid.iter().enumerate() {
        let descriptor = compile_descriptor(
            temporary_directory.path(),
            invalid_offset + index,
            &vector.files,
            &vector.entry_files,
            vector.include_imports,
            vector.include_source_info,
        )?;
        let descriptor = mutate_descriptor(descriptor, vector.mutation)?;

        match PluginProgramPayloadContract::parse(descriptor, PluginInterface::Sink) {
            Ok(_) => {
                return Err(io::Error::other(format!(
                    "Invalid vector passed: {}",
                    vector.name
                )));
            }
            Err(error) => assert_eq!(
                error.code(),
                vector.expected_error_code,
                "{}: {error}",
                vector.name
            ),
        }
    }

    Ok(())
}

#[test]
fn source_payload_contract_vectors_use_the_source_root() -> io::Result<()> {
    let vectors: PayloadContractTestVectors =
        serde_json::from_slice(SOURCE_TEST_VECTORS).map_err(io::Error::other)?;
    assert_eq!(
        vectors.format_version, 1,
        "Unexpected vector format version"
    );
    let temporary_directory = TemporaryDirectory::create()?;

    for (index, vector) in vectors.valid.iter().enumerate() {
        let descriptor = compile_descriptor(
            temporary_directory.path(),
            index,
            &vector.files,
            &vector.entry_files,
            true,
            true,
        )?;
        let contract =
            PluginProgramPayloadContract::parse(descriptor.clone(), PluginInterface::Source)
                .map_err(|error| {
                    io::Error::other(format!(
                        "Valid Source vector failed: {}: {error}",
                        vector.name
                    ))
                })?;

        let reconstructed =
            PluginProgramPayloadContract::from_runner_bytes(contract.descriptor_bytes().to_vec());
        assert_eq!(
            reconstructed.descriptor_bytes(),
            descriptor,
            "{}",
            vector.name
        );
        assert_eq!(
            reconstructed.interface(),
            contract.interface(),
            "{}",
            vector.name
        );
        assert_eq!(
            reconstructed
                .descriptor_pool
                .files()
                .map(|file| file.file_descriptor_proto().clone())
                .collect::<Vec<_>>(),
            contract
                .descriptor_pool
                .files()
                .map(|file| file.file_descriptor_proto().clone())
                .collect::<Vec<_>>(),
            "{}",
            vector.name
        );
        assert_eq!(contract.descriptor_bytes(), descriptor, "{}", vector.name);
        assert_eq!(
            contract
                .source_root_message()
                .ok_or_else(|| io::Error::other("Source root is missing"))?
                .full_name(),
            vector.expected_root_message,
            "{}",
            vector.name
        );
    }

    for (index, vector) in vectors.invalid.iter().enumerate() {
        let descriptor = compile_descriptor(
            temporary_directory.path(),
            vectors.valid.len() + index,
            &vector.files,
            &vector.entry_files,
            vector.include_imports,
            vector.include_source_info,
        )?;
        match PluginProgramPayloadContract::parse(descriptor, PluginInterface::Source) {
            Ok(_) => {
                return Err(io::Error::other(format!(
                    "Invalid Source vector passed: {}",
                    vector.name
                )));
            }
            Err(error) => assert_eq!(
                error.code(),
                vector.expected_error_code,
                "{}: {error}",
                vector.name
            ),
        }
    }

    Ok(())
}

proptest! {
    #[test]
    fn arbitrary_descriptor_bytes_are_rejected_or_fully_validated(
        descriptor in prop::collection::vec(any::<u8>(), 0..8192),
    ) {
        let original = descriptor.clone();
        if let Ok(contract) = PluginProgramPayloadContract::parse(descriptor, PluginInterface::Sink) {
            prop_assert_eq!(contract.descriptor_bytes(), original);
            let root = contract.sink_root_message().ok_or_else(||
                TestCaseError::fail("Accepted Sink Program must contain its root"))?;
            prop_assert_eq!(root.name(), "SinkRecordPayload");
            prop_assert!(contract.source_root_message().is_none());
        }
    }
}

fn enabled() -> bool {
    true
}

fn compile_descriptor(
    temporary_root: &Path,
    index: usize,
    files: &[SourceFile],
    entry_files: &[String],
    include_imports: bool,
    include_source_info: bool,
) -> io::Result<Vec<u8>> {
    let case_directory = temporary_root.join(index.to_string());
    fs::create_dir(&case_directory)?;

    for source in files {
        let path = case_directory.join(&source.name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, &source.content)?;
    }

    let descriptor_path = case_directory.join("payload.descriptor.pb");
    let protoc = protoc_bin_vendored::protoc_bin_path().map_err(io::Error::other)?;
    let bundled_includes = protoc_bin_vendored::include_path().map_err(io::Error::other)?;
    let mut command = Command::new(protoc);
    command
        .current_dir(&case_directory)
        .arg(format!(
            "--descriptor_set_out={}",
            descriptor_path.display()
        ))
        .arg("--experimental_editions")
        .arg("--proto_path=.")
        .arg(format!("--proto_path={}", bundled_includes.display()));
    if include_imports {
        command.arg("--include_imports");
    }
    if include_source_info {
        command.arg("--include_source_info");
    }
    command.args(entry_files);

    let output = command.output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "Payload Contract fixture compilation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    fs::read(descriptor_path)
}

fn mutate_descriptor(mut descriptor: Vec<u8>, mutation: Option<Mutation>) -> io::Result<Vec<u8>> {
    let Some(mutation) = mutation else {
        return Ok(descriptor);
    };

    if matches!(mutation, Mutation::Truncate) {
        descriptor.pop();
        return Ok(descriptor);
    }
    if matches!(mutation, Mutation::InjectEditionMarker) {
        return inject_edition_marker(&descriptor);
    }

    let mut descriptor_set =
        FileDescriptorSet::decode(descriptor.as_slice()).map_err(io::Error::other)?;
    match mutation {
        Mutation::ClearFiles => descriptor_set.file.clear(),
        Mutation::ClearRootFileName => root_file_mut(&mut descriptor_set)?.name = None,
        Mutation::DuplicateRootFile => {
            let root = root_file_mut(&mut descriptor_set)?.clone();
            descriptor_set.file.push(root);
        }
        Mutation::DuplicateRootMessage => {
            let root_file = root_file_mut(&mut descriptor_set)?;
            let root_message = root_file
                .message_type
                .iter()
                .find(|message| message.name.as_deref() == Some("SinkRecordPayload"))
                .cloned()
                .ok_or_else(|| io::Error::other("Root message fixture is missing"))?;
            root_file.message_type.push(root_message);
        }
        Mutation::ReplaceFieldWithMissingMessage => {
            let root_file = root_file_mut(&mut descriptor_set)?;
            let field = root_file
                .message_type
                .first_mut()
                .and_then(|message| message.field.first_mut())
                .ok_or_else(|| io::Error::other("Field fixture is missing"))?;
            field.r#type = Some(field_descriptor_proto::Type::Message as i32);
            field.type_name = Some(".missing.Type".to_owned());
        }
        Mutation::Truncate | Mutation::InjectEditionMarker => {
            return Err(io::Error::other(
                "Mutation was handled in an earlier branch",
            ));
        }
    }

    Ok(descriptor_set.encode_to_vec())
}

fn root_file_mut(
    descriptor_set: &mut FileDescriptorSet,
) -> io::Result<&mut prost_types::FileDescriptorProto> {
    descriptor_set
        .file
        .iter_mut()
        .find(|file| file.name.as_deref() == Some("sink_record_payload.proto"))
        .ok_or_else(|| io::Error::other("Root file fixture is missing"))
}

fn inject_edition_marker(descriptor: &[u8]) -> io::Result<Vec<u8>> {
    if descriptor.first() != Some(&0x0a) {
        return Err(io::Error::other(
            "Descriptor fixture does not start with a file",
        ));
    }

    let (file_length, length_bytes) = decode_varint(&descriptor[1..])?;
    let file_start = 1 + length_bytes;
    let file_end = file_start
        .checked_add(file_length as usize)
        .filter(|end| *end <= descriptor.len())
        .ok_or_else(|| io::Error::other("Descriptor fixture has an invalid file length"))?;
    let mut file_bytes = descriptor[file_start..file_end].to_vec();
    file_bytes.push(0x70);
    encode_varint(1001, &mut file_bytes);

    let mut result = Vec::with_capacity(descriptor.len() + 4);
    result.push(0x0a);
    encode_varint(file_bytes.len() as u64, &mut result);
    result.extend_from_slice(&file_bytes);
    result.extend_from_slice(&descriptor[file_end..]);
    Ok(result)
}

fn decode_varint(bytes: &[u8]) -> io::Result<(u64, usize)> {
    let mut value = 0_u64;
    for (index, byte) in bytes.iter().copied().enumerate().take(10) {
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
    }
    Err(io::Error::other("Descriptor fixture has an invalid varint"))
}

fn encode_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}
