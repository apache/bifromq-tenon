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

use super::*;
use crate::runner::extensions::ArtifactProtection;
use crate::runner::plugin::package::safety::stage_package_directory;
use std::io::Cursor;

#[test]
fn size_limits_reject_excess_bytes_before_writing_them() -> io::Result<()> {
    for (path, limits) in [
        (
            "program",
            PackageLimits {
                extracted_file_bytes: 3,
                ..PackageLimits::production()
            },
        ),
        (
            "manifest.json",
            PackageLimits {
                manifest_bytes: 3,
                ..PackageLimits::production()
            },
        ),
    ] {
        let directory = tempfile::tempdir()?;
        let mut output = PackageOutput::new(directory.path(), limits);
        let mut source = Cursor::new(b"abc").chain(Cursor::new(b"d"));
        assert!(output.write_file(path, &mut source).is_err());
        assert_eq!(std::fs::read(directory.path().join(path))?, b"abc");
        assert!(matches!(
            output.finish(Ok(())),
            Err(PluginPackageError::PackageTooLarge)
        ));
    }
    Ok(())
}

#[test]
fn ignored_path_and_filesystem_errors_keep_the_output_invalid() -> io::Result<()> {
    for path in ["../outside", "occupied"] {
        let directory = tempfile::tempdir()?;
        std::fs::write(directory.path().join("occupied"), b"existing")?;
        let mut output = PackageOutput::new(directory.path(), PackageLimits::production());
        assert!(output.write_file(path, &mut Cursor::new(b"new")).is_err());
        assert!(output.create_dir("later").is_err());
        let error = output
            .finish(Ok(()))
            .err()
            .ok_or_else(|| io::Error::other("Failed output was accepted"))?;
        if path == "occupied" {
            assert!(matches!(
                error,
                PluginPackageError::FilesystemOperationFailed { .. }
            ));
        } else {
            assert!(matches!(error, PluginPackageError::PackageInvalid));
        }
        assert_eq!(
            std::fs::read(directory.path().join("occupied"))?,
            b"existing"
        );
    }
    Ok(())
}

#[test]
fn a_hook_cannot_accept_partial_file_content_after_a_read_failure() -> io::Result<()> {
    struct FailedContent(io::ErrorKind);
    impl Read for FailedContent {
        fn read(&mut self, _output: &mut [u8]) -> io::Result<usize> {
            Err(self.0.into())
        }
    }
    struct IgnoreFailure(io::ErrorKind);
    impl ArtifactProtection for IgnoreFailure {
        fn open_plugin_package(
            &self,
            _source: &mut dyn Read,
            output: &mut dyn PluginPackageOutput,
        ) -> io::Result<()> {
            let _ = output.write_file(
                "program",
                &mut Cursor::new(b"partial").chain(FailedContent(self.0)),
            );
            Ok(())
        }
    }
    for (kind, code) in [
        (io::ErrorKind::PermissionDenied, "plugin_package_invalid"),
        (io::ErrorKind::UnexpectedEof, "plugin_package_invalid"),
        (io::ErrorKind::FileTooLarge, "plugin_package_too_large"),
    ] {
        let parent = tempfile::tempdir()?;
        let error = stage_package_directory(&IgnoreFailure(kind), io::empty(), parent.path())
            .err()
            .ok_or_else(|| io::Error::other("Failed content was accepted"))?;
        assert_eq!(error.code(), code);
        assert!(parent.path().read_dir()?.next().is_none());
    }
    Ok(())
}
