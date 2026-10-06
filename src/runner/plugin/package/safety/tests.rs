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

use super::{
    MAX_PACKAGE_PATH_BYTES, MAX_PACKAGE_PATH_COMPONENT_BYTES, PackageEntryKind, PackageLimits,
    PluginConfigSchema, PluginPackageError, extract_package_directory, package_files_are_identical,
    stage_package_directory,
};
use flate2::Compression;
use flate2::read::GzDecoder as ReadGzDecoder;
use flate2::write::GzEncoder;
use proptest::prelude::*;
use serde_json::json;
use std::collections::BTreeMap;
use std::io::{self, Cursor, Read, Write};
use tar::{Builder, EntryType, Header};

#[test]
fn exact_file_comparison_does_not_treat_matching_digests_as_matching_bytes() -> io::Result<()> {
    let left = tempfile::tempdir()?;
    let right = tempfile::tempdir()?;
    std::fs::write(left.path().join("program.bin"), b"left")?;
    std::fs::write(right.path().join("program.bin"), b"rght")?;
    let files = BTreeMap::from([(Box::<str>::from("program.bin"), [7_u8; 32])]);

    assert!(
        !package_files_are_identical(
            left.path(),
            b"manifest",
            &files,
            right.path(),
            b"manifest",
            &files,
        )
        .map_err(io::Error::other)?
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn archive_modes_do_not_change_private_staging_permissions() -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let restrictive_parent = tempfile::tempdir()?;
    let permissive_parent = tempfile::tempdir()?;
    let restrictive = stage_package_directory(
        &crate::runner::extensions::ByPass,
        Cursor::new(package_with_modes(0o000)?),
        restrictive_parent.path(),
    )
    .map_err(io::Error::other)?;
    let permissive = stage_package_directory(
        &crate::runner::extensions::ByPass,
        Cursor::new(package_with_modes(0o777)?),
        permissive_parent.path(),
    )
    .map_err(io::Error::other)?;

    for relative in ["manifest.json", "config.schema.json", "lib/program.jar"] {
        let restrictive_mode = std::fs::metadata(restrictive.path().join(relative))?
            .permissions()
            .mode()
            & 0o777;
        let permissive_mode = std::fs::metadata(permissive.path().join(relative))?
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(restrictive_mode, 0o500);
        assert_eq!(permissive_mode, restrictive_mode);
    }
    for staged in [&restrictive, &permissive] {
        let implicit_directory_mode = std::fs::metadata(staged.path().join("lib"))?
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(implicit_directory_mode, 0o700);
    }
    Ok(())
}

#[test]
fn staged_directory_is_removed_when_owner_is_dropped() -> io::Result<()> {
    let staging_parent = tempfile::tempdir()?;
    let staged = stage_package_directory(
        &crate::runner::extensions::ByPass,
        Cursor::new(archive_fixture()?),
        staging_parent.path(),
    )
    .map_err(io::Error::other)?;
    let path = staged.path().to_path_buf();
    assert!(path.is_dir());

    drop(staged);

    assert!(!path.exists());
    Ok(())
}

#[test]
fn unsafe_archive_paths_are_rejected() -> io::Result<()> {
    for path in [
        b"../escape".as_slice(),
        b"/absolute".as_slice(),
        b"./dot".as_slice(),
        b"nested//empty".as_slice(),
        b"nested/../escape".as_slice(),
        b"invalid-\xff".as_slice(),
    ] {
        let package = raw_package(&[(path, EntryType::Regular, b"value")])?;
        assert_error_code(package, "plugin_package_invalid")?;
    }
    Ok(())
}

#[test]
fn filesystem_component_and_package_path_limits_are_rejected() -> io::Result<()> {
    let oversized_component = "x".repeat(MAX_PACKAGE_PATH_COMPONENT_BYTES + 1);
    let package = package_with_path(&oversized_component, b"value")?;
    assert_error_code(package, "plugin_package_invalid")?;

    let oversized_path = format!(
        "{}/file.bin",
        (0..20)
            .map(|_| "x".repeat(MAX_PACKAGE_PATH_COMPONENT_BYTES))
            .collect::<Vec<_>>()
            .join("/")
    );
    assert!(oversized_path.len() > MAX_PACKAGE_PATH_BYTES);
    let package = package_with_path(&oversized_path, b"value")?;
    assert_error_code(package, "plugin_package_invalid")
}

#[test]
fn links_and_special_files_are_rejected() -> io::Result<()> {
    for entry_type in [
        EntryType::Symlink,
        EntryType::Link,
        EntryType::Fifo,
        EntryType::Char,
        EntryType::Block,
        EntryType::Continuous,
        EntryType::GNUSparse,
    ] {
        let package = raw_package(&[(b"entry", entry_type, b"")])?;
        assert_error_code(package, "plugin_package_invalid")?;
    }
    Ok(())
}

#[test]
fn duplicate_paths_and_file_directory_collisions_are_rejected() -> io::Result<()> {
    let duplicate = raw_package(&[
        (b"same", EntryType::Regular, b"first"),
        (b"same", EntryType::Regular, b"second"),
    ])?;
    assert_error_code(duplicate, "plugin_package_invalid")?;

    let collision = raw_package(&[
        (b"parent", EntryType::Directory, b""),
        (b"parent", EntryType::Regular, b"value"),
    ])?;
    assert_error_code(collision, "plugin_package_invalid")?;
    Ok(())
}

#[test]
fn bounded_gnu_and_pax_paths_remain_ordinary_program_files() -> io::Result<()> {
    let gnu_path = format!("lib/{}.bin", "long-name-".repeat(20));
    let gnu_package = package_with_path(&gnu_path, b"gnu path")?;
    let staging_parent = tempfile::tempdir()?;
    let staged = stage_package_directory(
        &crate::runner::extensions::ByPass,
        Cursor::new(gnu_package),
        staging_parent.path(),
    )
    .map_err(io::Error::other)?;
    assert_eq!(std::fs::read(staged.path().join(gnu_path))?, b"gnu path");

    let pax_path = format!("resources/{}.txt", "pax-name-".repeat(20));
    let pax_package = package_with_pax_path(&pax_path)?;
    let staging_parent = tempfile::tempdir()?;
    let staged = stage_package_directory(
        &crate::runner::extensions::ByPass,
        Cursor::new(pax_package),
        staging_parent.path(),
    )
    .map_err(io::Error::other)?;
    assert_eq!(std::fs::read(staged.path().join(pax_path))?, b"pax path");
    Ok(())
}

#[test]
fn pax_size_cannot_disagree_with_the_tar_header() -> io::Result<()> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut archive = Builder::new(encoder);
    archive.append_pax_extensions([("size", b"99".as_slice())])?;
    append_file(&mut archive, "entry", b"value", 0o644)?;
    let encoder = archive.into_inner()?;
    assert_error_code(encoder.finish()?, "plugin_package_invalid")
}

#[test]
fn truncated_or_appended_archives_are_rejected() -> io::Result<()> {
    let mut truncated_gzip = archive_fixture()?;
    truncated_gzip.truncate(truncated_gzip.len().saturating_sub(4));
    assert_error_code(truncated_gzip, "plugin_package_invalid")?;

    let valid = archive_fixture()?;
    let mut decoder = ReadGzDecoder::new(valid.as_slice());
    let mut tar_bytes = Vec::new();
    decoder.read_to_end(&mut tar_bytes)?;
    tar_bytes.truncate(tar_bytes.len().saturating_sub(512));
    assert_error_code(gzip(tar_bytes)?, "plugin_package_invalid")?;

    let mut appended = archive_fixture()?;
    appended.extend_from_slice(b"trailing data");
    assert_error_code(appended, "plugin_package_invalid")?;
    Ok(())
}

#[test]
fn config_schema_profile_allows_internal_refs_only() -> io::Result<()> {
    let schema = serde_json::to_vec(&json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": {
            "endpoint": {"type": "string"}
        },
        "type": "object",
        "properties": {
            "endpoint": {"$ref": "#/$defs/endpoint"}
        }
    }))?;
    let schema = PluginConfigSchema::parse_program(schema).map_err(io::Error::other)?;
    assert!(
        schema
            .validator
            .is_valid(&json!({"endpoint": "tcp://device"}))
    );
    assert!(!schema.validator.is_valid(&json!({"endpoint": 42})));
    assert!(!schema.bytes.is_empty());

    let empty_reference = serde_json::to_vec(&json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "$ref": ""
    }))?;
    PluginConfigSchema::parse_program(empty_reference).map_err(io::Error::other)?;
    Ok(())
}

#[test]
fn config_schema_profile_rejects_forbidden_capabilities() -> io::Result<()> {
    let invalid_schemas = [
        json!({"type": "object"}),
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "default": {}
        }),
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "customValidation": true
        }),
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "$vocabulary": {}
        }),
        json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "$ref": "https://example.com/config.schema.json"
        }),
        json!({
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "type": "object"
        }),
    ];

    for schema in invalid_schemas {
        let bytes = serde_json::to_vec(&schema)?;
        let error = PluginConfigSchema::parse_program(bytes)
            .err()
            .ok_or_else(|| io::Error::other("invalid Config Schema was accepted"))?;
        let super::PluginConfigSchemaError::ProfileViolation { path } = &error else {
            return Err(io::Error::other(
                "invalid Schema did not report a profile violation",
            ));
        };
        assert!(error.to_string().ends_with(path.as_ref()));
    }
    Ok(())
}

#[test]
fn implementation_limits_fail_before_unbounded_extraction() -> io::Result<()> {
    let package = archive_fixture()?;
    for limits in [
        PackageLimits {
            entries: 2,
            ..PackageLimits::production()
        },
        PackageLimits {
            extracted_file_bytes: 1,
            ..PackageLimits::production()
        },
    ] {
        assert_package_limit(package.clone(), limits)?;
    }
    Ok(())
}

#[test]
fn fixed_control_files_are_bounded_before_reading() -> io::Result<()> {
    let staging_parent = tempfile::tempdir()?;
    let staged = stage_package_directory(
        &crate::runner::extensions::ByPass,
        Cursor::new(archive_fixture()?),
        staging_parent.path(),
    )
    .map_err(io::Error::other)?;
    for path in [
        "manifest.json",
        "config.schema.json",
        "payload.descriptor.pb",
    ] {
        let error = super::read_control_file(staged.path(), path, 1)
            .err()
            .ok_or_else(|| io::Error::other("Oversized control file was accepted"))?;
        assert_eq!(error.code(), "plugin_package_too_large");
    }
    Ok(())
}

#[test]
fn materialized_entry_limit_includes_implicit_directories() -> io::Result<()> {
    let package = raw_package(&[(b"one/two/program.bin", EntryType::Regular, b"program")])?;
    assert_package_limit(
        package,
        PackageLimits {
            entries: 2,
            ..PackageLimits::production()
        },
    )
}

#[test]
fn oversized_archive_metadata_is_rejected_before_allocation_can_grow_unbounded() -> io::Result<()> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut archive = Builder::new(encoder);
    let oversized = vec![b'x'; 64 * 1024];
    archive.append_pax_extensions([("comment", oversized.as_slice())])?;
    append_file(&mut archive, "entry", b"value", 0o644)?;
    let encoder = archive.into_inner()?;
    assert_error_code(encoder.finish()?, "plugin_package_too_large")?;
    Ok(())
}

proptest! {
    #[test]
    fn accepted_paths_remain_below_the_staging_root(
        components in prop::collection::vec("[a-z][a-z0-9_-]{0,12}", 1..8)
    ) {
        let raw = components.join("/");
        let normalized = super::NormalizedPackagePath::parse(
            raw.as_bytes(),
            PackageEntryKind::File,
        )?;
        let root = std::path::Path::new("/private/staging");
        let target = normalized.to_path(root);
        prop_assert!(target.starts_with(root));
        prop_assert_eq!(normalized.as_str(), raw);
    }

    #[test]
    fn arbitrary_path_bytes_never_escape_or_bypass_normalization(raw in prop::collection::vec(any::<u8>(), 0..5000)) {
        if let Ok(normalized) =
            super::NormalizedPackagePath::parse(&raw, PackageEntryKind::File)
        {
            let root = std::path::Path::new("/private/staging");
            let target = normalized.to_path(root);
            let text = normalized.as_str();
            prop_assert!(target.starts_with(root));
            prop_assert_eq!(text.as_bytes(), raw.as_slice());
            prop_assert!(text.len() <= MAX_PACKAGE_PATH_BYTES);
            let components_are_valid = text.split('/').all(|component| {
                !component.is_empty()
                    && component.len() <= MAX_PACKAGE_PATH_COMPONENT_BYTES
                    && !matches!(component, "." | "..")
            });
            prop_assert!(components_are_valid);
        }
    }
}

fn assert_package_limit(package: Vec<u8>, limits: PackageLimits) -> io::Result<()> {
    let staging_parent = tempfile::tempdir()?;
    let error = extract_package_directory(
        &crate::runner::extensions::ByPass,
        Cursor::new(package),
        staging_parent.path(),
        limits,
    )
    .err()
    .ok_or_else(|| io::Error::other("oversized Plugin package was accepted"))?;
    assert_eq!(error.code(), "plugin_package_too_large");
    assert!(staging_parent.path().read_dir()?.next().is_none());
    Ok(())
}

fn assert_error_code(package: Vec<u8>, expected: &str) -> io::Result<()> {
    let error = stage_error(package)?;
    assert_eq!(error.code(), expected);
    Ok(())
}

fn stage_error(package: Vec<u8>) -> io::Result<PluginPackageError> {
    let staging_parent = tempfile::tempdir()?;
    stage_package_directory(
        &crate::runner::extensions::ByPass,
        Cursor::new(package),
        staging_parent.path(),
    )
    .err()
    .ok_or_else(|| io::Error::other("invalid Plugin package was accepted"))
}

fn archive_fixture() -> io::Result<Vec<u8>> {
    package_with_modes(0o644)
}

fn package_with_modes(mode: u32) -> io::Result<Vec<u8>> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut archive = Builder::new(encoder);
    for (path, bytes) in [
        ("manifest.json", b"manifest".as_slice()),
        ("config.schema.json", b"schema"),
        ("payload.descriptor.pb", b"descriptor"),
        ("lib/program.jar", b"program"),
    ] {
        append_file(&mut archive, path, bytes, mode)?;
    }
    archive.into_inner()?.finish()
}

fn package_with_path(path: &str, bytes: &[u8]) -> io::Result<Vec<u8>> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut archive = Builder::new(encoder);
    append_file(&mut archive, path, bytes, 0o644)?;
    archive.into_inner()?.finish()
}

fn package_with_pax_path(pax_path: &str) -> io::Result<Vec<u8>> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut archive = Builder::new(encoder);
    archive.append_pax_extensions([("path", pax_path.as_bytes()), ("mtime", b"0".as_slice())])?;
    append_file(&mut archive, "pax-placeholder", b"pax path", 0o644)?;
    archive.into_inner()?.finish()
}

fn append_file<W: Write>(
    archive: &mut Builder<W>,
    path: &str,
    bytes: &[u8],
    mode: u32,
) -> io::Result<()> {
    let mut header = Header::new_gnu();
    header.set_mode(mode);
    header.set_size(bytes.len() as u64);
    header.set_cksum();
    archive.append_data(&mut header, path, bytes)
}

fn raw_package(entries: &[(&[u8], EntryType, &[u8])]) -> io::Result<Vec<u8>> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut archive = Builder::new(encoder);
    for (path, entry_type, bytes) in entries {
        let mut header = Header::new_old();
        if path.len() > 100 {
            return Err(io::Error::other("raw test path exceeds the tar field"));
        }
        header.as_mut_bytes()[..path.len()].copy_from_slice(path);
        header.set_entry_type(*entry_type);
        header.set_mode(0o755);
        header.set_size(bytes.len() as u64);
        header.set_cksum();
        archive.append(&header, *bytes)?;
    }
    let encoder = archive.into_inner()?;
    encoder.finish()
}

fn gzip(bytes: Vec<u8>) -> io::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&bytes)?;
    encoder.finish()
}
