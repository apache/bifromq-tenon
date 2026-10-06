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

//! Receives new packages and opens local packages for installation and recovery.
//!
//! Incoming stream failure rejects only the installation. Local file failures
//! require Runner exit. The core records these failures so a hook cannot hide them.

use super::{PluginPackageError, PluginStoreError};
use crate::runner::extensions::ArtifactProtection;
use crate::runner::plugin::package::{StagedPluginProgramPackage, stage_plugin_program_package};
use std::io::{self, Read, Seek as _, Write};
use std::path::Path;
use tempfile::NamedTempFile;

pub(super) const MAX_PACKAGE_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Receives the full input into a temporary file under the Store root.
///
/// # Errors
///
/// Input read failure is an invalid upload; local file I/O requires Runner exit.
pub(super) fn receive(
    mut input: impl Read,
    directory: &Path,
) -> Result<NamedTempFile, PluginStoreError> {
    let filesystem_error = |source| PluginStoreError::FilesystemOperationFailed {
        path: directory.to_path_buf(),
        source,
    };
    let mut spool = tempfile::Builder::new()
        .prefix(".tenon-plugin-upload-")
        .tempfile_in(directory)
        .map_err(filesystem_error)?;
    receive_into(&mut input, &mut spool, directory)?;
    spool.rewind().map_err(filesystem_error)?;
    Ok(spool)
}

pub(super) fn open(
    input: &mut dyn Read,
    directory: &Path,
    protection: &dyn ArtifactProtection,
) -> Result<StagedPluginProgramPackage, PluginStoreError> {
    let filesystem_error = |source| PluginStoreError::FilesystemOperationFailed {
        path: directory.to_path_buf(),
        source,
    };
    let mut input = LocalPackageReader {
        input,
        failure: None,
    };
    let result = stage_plugin_program_package(protection, &mut input, directory);
    if let Some(source) = input.failure {
        if let Ok(staged) = result {
            staged.close().map_err(filesystem_error)?;
        }
        return Err(filesystem_error(source));
    }
    result.map_err(|source| match source {
        PluginPackageError::AccessRejected => PluginStoreError::PackageAccessRejected,
        PluginPackageError::FilesystemOperationFailed { source } => filesystem_error(source),
        source => PluginStoreError::PackageInvalid { source },
    })
}

struct LocalPackageReader<'a> {
    input: &'a mut dyn Read,
    failure: Option<io::Error>,
}

impl Read for LocalPackageReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if let Some(source) = &self.failure {
            return Err(io::Error::new(
                source.kind(),
                "Plugin package input has already failed",
            ));
        }
        loop {
            match self.input.read(output) {
                Ok(count) => return Ok(count),
                Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
                Err(source) => {
                    let error = io::Error::new(source.kind(), "Plugin package input read failed");
                    self.failure = Some(source);
                    return Err(error);
                }
            }
        }
    }
}

fn receive_into(
    input: &mut impl Read,
    spool: &mut impl Write,
    directory: &Path,
) -> Result<(), PluginStoreError> {
    let mut buffer = [0_u8; 64 * 1024];
    let mut remaining = MAX_PACKAGE_BYTES;
    loop {
        let count = input
            .read(&mut buffer)
            .map_err(|source| PluginStoreError::PackageInvalid {
                source: PluginPackageError::ArchiveInvalid { source },
            })?;
        if count == 0 {
            return Ok(());
        }
        remaining =
            remaining
                .checked_sub(count as u64)
                .ok_or(PluginStoreError::PackageInvalid {
                    source: PluginPackageError::PackageTooLarge,
                })?;
        spool.write_all(&buffer[..count]).map_err(|source| {
            PluginStoreError::FilesystemOperationFailed {
                path: directory.to_path_buf(),
                source,
            }
        })?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::extensions::ByPass;
    use crate::runner::plugin::package::tests::valid_source_program_package;
    use std::io::Cursor;

    #[test]
    fn input_failure_after_a_complete_archive_still_rejects_the_upload() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let input = Cursor::new(valid_source_program_package()?)
            .chain(FailingIo(io::ErrorKind::ConnectionReset));
        let error = receive(input, directory.path())
            .err()
            .ok_or_else(|| io::Error::other("Interrupted body was accepted"))?;
        assert!(matches!(error, PluginStoreError::PackageInvalid {
            source: PluginPackageError::ArchiveInvalid { source }
        } if source.kind() == io::ErrorKind::ConnectionReset));
        assert!(directory.path().read_dir()?.next().is_none());
        Ok(())
    }

    #[test]
    fn spool_write_failure_is_not_an_invalid_upload() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let error = receive_into(
            &mut Cursor::new(b"body"),
            &mut FailingIo(io::ErrorKind::StorageFull),
            directory.path(),
        )
        .err()
        .ok_or_else(|| io::Error::other("Disk write failure was ignored"))?;
        assert!(
            matches!(error, PluginStoreError::FilesystemOperationFailed { path, source }
            if path == directory.path() && source.kind() == io::ErrorKind::StorageFull)
        );
        Ok(())
    }

    #[test]
    fn local_read_failures_survive_gzip_tar_and_trailer_decoding() -> io::Result<()> {
        let package = valid_source_program_package()?;
        for (offset, kind) in [
            (0, io::ErrorKind::Other),
            (10, io::ErrorKind::Other),
            (package.len() / 2, io::ErrorKind::Other),
            (package.len() - 4, io::ErrorKind::Other),
            (package.len(), io::ErrorKind::Other),
            (0, io::ErrorKind::FileTooLarge),
        ] {
            let directory = tempfile::tempdir()?;
            let mut input = Cursor::new(&package[..offset]).chain(FailingIo(kind));
            let error = open(&mut input, directory.path(), &ByPass)
                .err()
                .ok_or_else(|| io::Error::other("Local read failure was ignored"))?;
            assert!(
                matches!(error, PluginStoreError::FilesystemOperationFailed { source, .. }
                if source.kind() == kind),
                "offset {offset}, kind {kind}"
            );
            assert!(directory.path().read_dir()?.next().is_none());
        }
        Ok(())
    }

    #[test]
    fn input_size_limit_accepts_the_boundary_and_rejects_the_next_byte() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        receive_into(
            &mut io::repeat(0).take(MAX_PACKAGE_BYTES),
            &mut io::sink(),
            directory.path(),
        )
        .map_err(io::Error::other)?;
        assert!(matches!(
            receive_into(
                &mut io::repeat(0).take(MAX_PACKAGE_BYTES + 1),
                &mut io::sink(),
                directory.path(),
            ),
            Err(PluginStoreError::PackageInvalid {
                source: PluginPackageError::PackageTooLarge
            })
        ));
        Ok(())
    }

    #[test]
    fn local_reader_failure_cannot_be_hidden_by_a_successful_hook() -> io::Result<()> {
        struct IgnoreReadFailure(Vec<u8>);
        impl ArtifactProtection for IgnoreReadFailure {
            fn open_plugin_package(
                &self,
                source: &mut dyn Read,
                output: &mut dyn crate::PluginPackageOutput,
            ) -> io::Result<()> {
                let _ = source.read(&mut [0; 1]);
                ByPass.open_plugin_package(&mut self.0.as_slice(), output)
            }
        }
        let directory = tempfile::tempdir()?;
        let protection = IgnoreReadFailure(valid_source_program_package()?);
        let result = open(
            &mut FailingIo(io::ErrorKind::PermissionDenied),
            directory.path(),
            &protection,
        );
        assert!(
            matches!(result, Err(PluginStoreError::FilesystemOperationFailed { source, .. }) if source.kind() == io::ErrorKind::PermissionDenied)
        );
        assert!(directory.path().read_dir()?.next().is_none());
        Ok(())
    }

    #[test]
    fn hook_error_kinds_select_package_failures_and_remove_partial_output() -> io::Result<()> {
        struct Reject(io::ErrorKind);
        impl ArtifactProtection for Reject {
            fn open_plugin_package(
                &self,
                _source: &mut dyn Read,
                output: &mut dyn crate::PluginPackageOutput,
            ) -> io::Result<()> {
                output.write_file("program", &mut io::Cursor::new(b"partial"))?;
                Err(io::Error::new(self.0, "Fixture package access failed"))
            }
        }
        let directory = tempfile::tempdir()?;
        for (kind, code) in [
            (io::ErrorKind::InvalidData, Some("plugin_package_invalid")),
            (io::ErrorKind::UnexpectedEof, Some("plugin_package_invalid")),
            (
                io::ErrorKind::FileTooLarge,
                Some("plugin_package_too_large"),
            ),
            (io::ErrorKind::PermissionDenied, None),
            (io::ErrorKind::Other, None),
        ] {
            let error = open(&mut io::empty(), directory.path(), &Reject(kind))
                .err()
                .ok_or_else(|| io::Error::other("Rejected package was accepted"))?;
            match (error, code) {
                (PluginStoreError::PackageInvalid { source }, Some(code)) => {
                    assert_eq!(source.code(), code);
                    if matches!(
                        kind,
                        io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                    ) {
                        assert_eq!(
                            std::error::Error::source(&source).map(ToString::to_string),
                            Some(String::from("Fixture package access failed")),
                        );
                    }
                }
                (PluginStoreError::PackageAccessRejected, None) => {}
                (error, code) => {
                    return Err(io::Error::other(format!(
                        "Unexpected package failure: {error:?}; expected {code:?}"
                    )));
                }
            }
            assert!(directory.path().read_dir()?.next().is_none());
        }
        Ok(())
    }

    struct FailingIo(io::ErrorKind);

    impl Read for FailingIo {
        fn read(&mut self, _output: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(self.0, "Injected read failure"))
        }
    }

    impl Write for FailingIo {
        fn write(&mut self, _input: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(self.0, "Injected write failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
