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

//! Runs the real CLI against the published contract fixtures.

use prost::Message;
use prost_types::{FileDescriptorSet, field_descriptor_proto};
use serde_json::{Value, json};
use std::error::Error;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};
use tempfile::TempDir;

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn manifest_vectors_have_the_published_results() -> TestResult {
    let vectors: Value = serde_json::from_str(include_str!(
        "../contracts/test-fixtures/manifest.test-vectors.json"
    ))?;
    let directory = package("source")?;
    for group in ["valid", "invalid"] {
        let cases = vectors[group]
            .as_array()
            .ok_or("missing manifest vectors")?;
        assert!(!cases.is_empty());
        for case in cases {
            let manifest = &case["manifest"];
            fs::write(
                directory.path().join("manifest.json"),
                serde_json::to_vec(manifest)?,
            )?;
            let interface = manifest["interface"].as_str().unwrap_or("source");
            fs::write(
                directory.path().join("payload.descriptor.pb"),
                descriptor_for(interface)?,
            )?;
            let output = check(directory.path())?;
            assert_eq!(
                output.status.success(),
                group == "valid",
                "{}: {}",
                case["name"],
                String::from_utf8_lossy(&output.stderr)
            );
            if group == "invalid" {
                let error: Value = serde_json::from_slice(&output.stderr)?;
                assert_eq!(
                    error["error"]["code"], "manifest.invalid",
                    "{}",
                    case["name"]
                );
            }
        }
    }
    Ok(())
}

#[test]
fn payload_vectors_have_the_published_roots_and_errors() -> TestResult {
    for (text, interface) in [
        (
            include_str!("../contracts/test-fixtures/plugin-payload.test-vectors.json"),
            None,
        ),
        (
            include_str!("../contracts/test-fixtures/source-payload.test-vectors.json"),
            Some("source"),
        ),
        (
            include_str!("../contracts/test-fixtures/sink-payload.test-vectors.json"),
            Some("sink"),
        ),
    ] {
        let vectors: Value = serde_json::from_str(text)?;
        assert_eq!(vectors["formatVersion"], 1);
        for group in ["valid", "invalid"] {
            let cases = vectors[group].as_array().ok_or("missing payload vectors")?;
            assert!(!cases.is_empty());
            for case in cases {
                let interface = interface
                    .or_else(|| case["interface"].as_str())
                    .ok_or("missing interface")?;
                let directory = package(interface)?;
                let descriptor = compile_descriptor(case)?;
                fs::write(directory.path().join("payload.descriptor.pb"), descriptor)?;
                let output = check(directory.path())?;
                assert_eq!(
                    output.status.success(),
                    group == "valid",
                    "{}: {}",
                    case["name"],
                    String::from_utf8_lossy(&output.stderr)
                );
                if group == "valid" {
                    let result: Value = serde_json::from_slice(&output.stdout)?;
                    if let Some(root) = case.get("expectedRootMessage") {
                        let key = if interface == "source" {
                            "sourceRoot"
                        } else {
                            "sinkRoot"
                        };
                        assert_eq!(&result[key], root, "{}", case["name"]);
                    } else {
                        assert_eq!(
                            result["sourceRoot"], case["expectedSourceRootMessage"],
                            "{}",
                            case["name"]
                        );
                        assert_eq!(
                            result["sinkRoot"], case["expectedSinkRootMessage"],
                            "{}",
                            case["name"]
                        );
                    }
                } else {
                    let result: Value = serde_json::from_slice(&output.stderr)?;
                    assert_eq!(
                        result["error"]["code"], case["expectedErrorCode"],
                        "{}",
                        case["name"]
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn identifier_vectors_use_the_manifest_contract() -> TestResult {
    let vectors: Value = serde_json::from_str(include_str!(
        "../contracts/test-fixtures/domain-identifiers.test-vectors.json"
    ))?;
    let directory = package("source")?;
    for group in ["valid", "invalid"] {
        for case in vectors[group]
            .as_array()
            .ok_or("missing identifier vectors")?
        {
            let Some(kind @ ("programName" | "exactVersion")) = case["kind"].as_str() else {
                continue;
            };
            let mut manifest = manifest("source");
            manifest[kind] = case["value"].clone();
            fs::write(
                directory.path().join("manifest.json"),
                serde_json::to_vec(&manifest)?,
            )?;
            let output = check(directory.path())?;
            assert_eq!(
                output.status.success(),
                group == "valid",
                "{}: {}",
                case["name"],
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
    Ok(())
}

#[test]
fn missing_contract_files_fail_without_changing_the_directory() -> TestResult {
    for missing in [
        "manifest.json",
        "config.schema.json",
        "payload.descriptor.pb",
    ] {
        let directory = package("source")?;
        fs::remove_file(directory.path().join(missing))?;
        let before = directory_contents(directory.path())?;
        let output = check(directory.path())?;
        assert!(!output.status.success());
        let result: Value = serde_json::from_slice(&output.stderr)?;
        assert_eq!(result["error"]["code"], "package.read_failed");
        assert!(
            result["error"]["message"]
                .as_str()
                .ok_or("missing message")?
                .contains(missing)
        );
        assert_eq!(directory_contents(directory.path())?, before);
    }
    Ok(())
}

#[cfg(unix)]
#[test]
fn linked_contract_files_are_rejected_without_reading_the_target() -> TestResult {
    let directory = package("source")?;
    let path = directory.path().join("payload.descriptor.pb");
    fs::remove_file(&path)?;
    std::os::unix::fs::symlink("absent-target", path)?;
    let output = check(directory.path())?;
    assert!(!output.status.success());
    let result: Value = serde_json::from_slice(&output.stderr)?;
    assert_eq!(result["error"]["code"], "package.invalid_file");
    Ok(())
}

#[test]
fn checking_never_runs_the_command_or_rewrites_contracts() -> TestResult {
    let directory = package("source-and-sink")?;
    let marker = directory.path().join("command-was-run");
    let mut manifest = manifest("source-and-sink");
    manifest["command"] = json!(["/usr/bin/touch", marker]);
    fs::write(
        directory.path().join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    let before = directory_contents(directory.path())?;
    let output = check(directory.path())?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(directory_contents(directory.path())?, before);
    assert!(!marker.exists());
    Ok(())
}

#[test]
fn duplicate_contract_keys_keep_the_json_error_category() -> TestResult {
    for (file, code, input) in [
        (
            "manifest.json",
            "manifest.invalid_json",
            r#"{"interface":"source","\u0069nterface":"sink"}"#,
        ),
        (
            "config.schema.json",
            "config_schema.invalid_json",
            r#"{"type":"object","\u0074ype":"object"}"#,
        ),
    ] {
        let directory = package("source")?;
        fs::write(directory.path().join(file), input)?;
        let output = check(directory.path())?;
        assert!(!output.status.success());
        let result: Value = serde_json::from_slice(&output.stderr)?;
        assert_eq!(result["error"]["code"], code);
    }
    Ok(())
}

#[test]
fn invalid_config_schemas_are_rejected() -> TestResult {
    let directory = package("sink")?;
    let invalid = [
        String::from("{} {}"),
        String::from("{\"type\":\"object\",\"type\":\"object\"}"),
        String::from("{\"type\":\"object\",}"),
        String::from("/* comment */ {}"),
        String::from("{}"),
        schema(json!({"type": ["object"]})).to_string(),
        schema(json!({"$schema": "https://json-schema.org/draft/2019-09/schema"})).to_string(),
        schema(json!({"default": {}})).to_string(),
        schema(json!({"$vocabulary": {}})).to_string(),
        schema(json!({"customValidation": true})).to_string(),
        schema(json!({"properties": {"x": {"$ref": "https://example.invalid/schema"}}}))
            .to_string(),
        schema(json!({"$dynamicRef": "file:///missing/schema.json"})).to_string(),
        schema(json!({"$ref": "#/$defs/missing"})).to_string(),
        schema(json!({"properties": {"x": {"minLength": -1}}})).to_string(),
        schema(json!({"properties": {"x": {"pattern": "["}}})).to_string(),
        schema(json!({"$defs": {"x": {"default": null}}})).to_string(),
    ];
    for input in invalid {
        fs::write(directory.path().join("config.schema.json"), &input)?;
        let before = directory_contents(directory.path())?;
        let output = check(directory.path())?;
        assert!(
            !output.status.success(),
            "invalid Config Schema passed: {input}"
        );
        assert_eq!(directory_contents(directory.path())?, before);
        let result: Value = serde_json::from_slice(&output.stderr)?;
        assert!(
            result["error"]["code"]
                .as_str()
                .ok_or("missing code")?
                .starts_with("config_schema.")
        );
    }
    Ok(())
}

#[test]
fn config_schema_annotations_and_internal_references_are_preserved() -> TestResult {
    let directory = package("source")?;
    let input = schema(json!({
        "$defs": {"value": {"type": "integer", "minimum": 18446744073709551616_u128}},
        "properties": {"value": {"$ref": "#/$defs/value"}},
        "examples": [{"default": "business property", "$ref": "literal value"}]
    }));
    fs::write(
        directory.path().join("config.schema.json"),
        serde_json::to_vec_pretty(&input)?,
    )?;
    let before = directory_contents(directory.path())?;
    let output = check(directory.path())?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(directory_contents(directory.path())?, before);
    Ok(())
}

#[test]
fn cli_rejects_unknown_commands_and_extra_arguments() -> TestResult {
    for arguments in [
        vec!["unknown"],
        vec!["build", "--unknown"],
        vec!["bundle", "--target"],
        vec!["check"],
        vec!["check", ".", "extra"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_cargo-tenon"))
            .arg("tenon")
            .args(arguments)
            .output()?;
        assert!(!output.status.success());
        let result: Value = serde_json::from_slice(&output.stderr)?;
        assert_eq!(result["error"]["code"], "cli.usage");
    }
    let help = Command::new(env!("CARGO_BIN_EXE_cargo-tenon"))
        .args(["tenon", "--help"])
        .output()?;
    assert!(help.status.success());
    assert!(String::from_utf8(help.stdout)?.contains("check <package-directory>"));
    Ok(())
}

fn check(path: &Path) -> Result<Output, std::io::Error> {
    Command::new(env!("CARGO_BIN_EXE_cargo-tenon"))
        .arg("tenon")
        .arg("check")
        .arg(path)
        .output()
}

fn manifest(interface: &str) -> Value {
    json!({"programName": "com.example.plugin", "exactVersion": "0.1.0", "interface": interface,
        "platforms": [{"os": "linux", "architecture": "amd64"}], "displayName": "Example Plugin", "description": "Read and write example records.", "command": ["./bin/plugin"]})
}

fn schema(overrides: Value) -> Value {
    let mut schema =
        json!({"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object"});
    if let (Some(schema), Some(overrides)) = (schema.as_object_mut(), overrides.as_object()) {
        schema.extend(overrides.clone());
    }
    schema
}

fn package(interface: &str) -> Result<TempDir, Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    fs::write(
        directory.path().join("manifest.json"),
        serde_json::to_vec(&manifest(interface))?,
    )?;
    fs::write(
        directory.path().join("config.schema.json"),
        serde_json::to_vec(&schema(json!({})))?,
    )?;
    fs::write(
        directory.path().join("payload.descriptor.pb"),
        descriptor_for(interface)?,
    )?;
    Ok(directory)
}

fn descriptor_for(interface: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let roots = match interface {
        "sink" => "message SinkRecordPayload {}",
        "source-and-sink" => "message SourceRecordPayload {} message SinkRecordPayload {}",
        _ => "message SourceRecordPayload {}",
    };
    compile_descriptor(
        &json!({"files": [{"name": "payload.proto", "content": format!("syntax = \"proto3\"; {roots}")}], "entryFiles": ["payload.proto"]}),
    )
}

fn compile_descriptor(case: &Value) -> Result<Vec<u8>, Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    for file in case["files"].as_array().ok_or("missing proto files")? {
        let path = directory
            .path()
            .join(file["name"].as_str().ok_or("missing file name")?);
        fs::create_dir_all(path.parent().ok_or("missing parent")?)?;
        fs::write(
            path,
            file["content"].as_str().ok_or("missing proto contents")?,
        )?;
    }
    let mut command = Command::new(protoc_bin_vendored::protoc_bin_path()?);
    command
        .current_dir(directory.path())
        .arg("--proto_path=.")
        .arg("--proto_path")
        .arg(protoc_bin_vendored::include_path()?)
        .arg("--descriptor_set_out=payload.descriptor.pb")
        .arg("--experimental_editions");
    if case["includeImports"] != false {
        command.arg("--include_imports");
    }
    if case["includeSourceInfo"] != false {
        command.arg("--include_source_info");
    }
    for entry in case["entryFiles"]
        .as_array()
        .ok_or("missing proto entry files")?
    {
        command.arg(entry.as_str().ok_or("invalid entry file")?);
    }
    let output = command.output()?;
    if !output.status.success() {
        return Err(String::from_utf8(output.stderr)?.into());
    }
    let mut bytes = fs::read(directory.path().join("payload.descriptor.pb"))?;
    let Some(mutation) = case["mutation"].as_str() else {
        return Ok(bytes);
    };
    if mutation == "truncate" {
        bytes.pop();
        return Ok(bytes);
    }
    let mut descriptor = FileDescriptorSet::decode(bytes.as_slice())?;
    if mutation == "clearFiles" {
        descriptor.file.clear();
        return Ok(descriptor.encode_to_vec());
    }
    let root_index = descriptor
        .file
        .iter()
        .position(|file| {
            file.message_type
                .iter()
                .any(|message| message.name.as_deref() == Some("SinkRecordPayload"))
        })
        .ok_or("missing root file")?;
    let root = &mut descriptor.file[root_index];
    match mutation {
        "clearRootFileName" => root.name = None,
        "duplicateRootFile" => {
            let duplicate = root.clone();
            descriptor.file.push(duplicate);
        }
        "duplicateRootMessage" => {
            let duplicate = root
                .message_type
                .iter()
                .find(|message| message.name.as_deref() == Some("SinkRecordPayload"))
                .ok_or("missing root")?
                .clone();
            root.message_type.push(duplicate);
        }
        "replaceFieldWithMissingMessage" => {
            let field = root
                .message_type
                .first_mut()
                .and_then(|message| message.field.first_mut())
                .ok_or("missing field")?;
            field.r#type = Some(field_descriptor_proto::Type::Message as i32);
            field.type_name = Some(".missing.Type".into());
        }
        "injectEditionMarker" => {
            let mut raw = root.encode_to_vec();
            raw.extend_from_slice(&[0x70, 0xe9, 0x07]);
            let mut output = Vec::new();
            for (index, file) in descriptor.file.iter().enumerate() {
                let file_bytes = if index == root_index {
                    raw.clone()
                } else {
                    file.encode_to_vec()
                };
                output.push(0x0a);
                prost::encoding::encode_varint(file_bytes.len() as u64, &mut output);
                output.extend_from_slice(&file_bytes);
            }
            return Ok(output);
        }
        _ => return Err(format!("unknown vector mutation: {mutation}").into()),
    }
    Ok(descriptor.encode_to_vec())
}

fn directory_contents(path: &Path) -> Result<Vec<(std::ffi::OsString, Vec<u8>)>, std::io::Error> {
    let mut files = fs::read_dir(path)?
        .map(|entry| {
            let entry = entry?;
            Ok((entry.file_name(), fs::read(entry.path())?))
        })
        .collect::<Result<Vec<_>, std::io::Error>>()?;
    files.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(files)
}
