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

//! Physical boundary for the private Tenon Document store.
//!
//! This module receives the exact fixed store directory prepared by Runner
//! startup. Recovery removes stale Tenon temporary files before any writer can
//! be active. Startup then reads committed Tenon Documents lazily,
//! one fixed-length owned source at a time. The first physical error ends the
//! iteration, and no directory snapshot or source collection is retained.
//!
//! It deliberately does not parse JSONC, extract a Tenon Document identifier,
//! compare that identifier with the file name, select Sink Programs, or run the
//! Tenon Document verifier. Runner startup passes each source through the same
//! verifier used by the HTTP boundary and treats any storage or verification
//! error as fatal before opening the API. During normal operation, only the
//! Runner API mutates this private store and directly submits verified desired
//! state to the Runner supervisor. This module does not watch or reconcile
//! external filesystem changes.

use crate::identifiers::TenonDocumentId;
use crate::runner::extensions::DocumentProtection;
use sha2::{Digest as _, Sha256};
use std::error::Error;
use std::ffi::OsStr;
use std::fs::{self, DirEntry, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, iter, mem};
use zeroize::Zeroizing;

const COMMITTED_FILE_EXTENSION: &str = "jsonc";
const TEMPORARY_FILE_EXTENSION: &str = "tmp";
const SHA256_BYTES: usize = 32;
const SHA256_HEX_BYTES: usize = SHA256_BYTES * 2;

/// The exact SHA-256 identity of one committed Tenon Document source.
///
/// The same digest has two deliberately separate text projections: a quoted
/// strong HTTP ETag and an unquoted lowercase name safe for private directories.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TenonDocumentEtag([u8; SHA256_BYTES]);

impl TenonDocumentEtag {
    /// Derives the identity of exact Tenon Document source bytes.
    #[must_use]
    pub(crate) fn for_source(source: &[u8]) -> Self {
        Self(Sha256::digest(source).into())
    }

    /// Returns the quoted strong ETag used by Runner protocols and HTTP.
    #[must_use]
    pub(crate) fn strong_value(self) -> String {
        format!("\"{}\"", HexDigest(&self.0))
    }

    /// Parses the quoted lowercase SHA-256 form used at protocol boundaries.
    #[must_use]
    pub(crate) fn from_strong_value(value: &str) -> Option<Self> {
        let bytes = value.as_bytes();
        if bytes.len() != SHA256_HEX_BYTES + 2
            || bytes.first() != Some(&b'"')
            || bytes.last() != Some(&b'"')
        {
            return None;
        }
        let mut digest = [0_u8; SHA256_BYTES];
        for (output, pair) in digest
            .iter_mut()
            .zip(bytes[1..=SHA256_HEX_BYTES].as_chunks::<2>().0)
        {
            *output = decode_lower_hex(pair[0])? << 4 | decode_lower_hex(pair[1])?;
        }
        Some(Self(digest))
    }

    /// Returns the unquoted lowercase digest used as a private directory name.
    #[must_use]
    pub(crate) fn directory_name(self) -> String {
        HexDigest(&self.0).to_string()
    }
}

impl fmt::Debug for TenonDocumentEtag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("TenonDocumentEtag")
            .field(&self.strong_value())
            .finish()
    }
}

/// One exact fixed physical store prepared by Runner startup.
#[derive(Debug)]
pub(crate) struct TenonDocumentStore {
    directory: PathBuf,
}

impl TenonDocumentStore {
    /// Adopts the exact fixed store path without reading or creating it.
    #[must_use]
    pub(crate) fn new(directory: PathBuf) -> Self {
        Self { directory }
    }

    /// Removes stale direct-child regular `*.tmp` files during startup.
    ///
    /// The caller must guarantee that no live writer can own a temporary file.
    /// The fixed Store directory must already have been prepared by Runner
    /// startup. Directories, symbolic links, and unrelated entries are preserved.
    ///
    /// # Errors
    ///
    /// Returns [`TenonDocumentStoreError`] when the directory cannot be
    /// enumerated or a stale temporary file cannot be inspected or removed.
    pub(crate) fn recover_stale_temporary_files(&self) -> Result<(), TenonDocumentStoreError> {
        let entries = fs::read_dir(&self.directory).map_err(|source| {
            TenonDocumentStoreError::DirectoryReadFailed {
                path: self.directory.clone(),
                source,
            }
        })?;

        for entry in entries {
            let entry = entry.map_err(|source| TenonDocumentStoreError::DirectoryReadFailed {
                path: self.directory.clone(),
                source,
            })?;
            if has_extension(&entry.file_name(), TEMPORARY_FILE_EXTENSION) {
                remove_temporary_entry(&entry)?;
            }
        }
        Ok(())
    }

    /// Lazily reads committed Tenon Document sources for startup restoration.
    ///
    /// The fixed Store directory must already have been prepared by Runner
    /// startup. Each successful step owns only that file's exact bytes and digests. Any
    /// unexpected entry or read failure is returned once and then ends the
    /// iterator. Callers must pass each source directly to the shared Tenon Document
    /// verifier instead of collecting raw sources.
    ///
    /// # Errors
    ///
    /// Returns [`TenonDocumentStoreError`] immediately when the directory cannot
    /// be opened. Each iterator item returns a later directory-entry or file
    /// failure.
    pub(crate) fn sources(
        &self,
        protection: Arc<dyn DocumentProtection>,
    ) -> Result<
        impl Iterator<Item = Result<StoredTenonDocumentSource, TenonDocumentStoreError>> + use<>,
        TenonDocumentStoreError,
    > {
        let mut entries = Some(fs::read_dir(&self.directory).map_err(|source| {
            TenonDocumentStoreError::DirectoryReadFailed {
                path: self.directory.clone(),
                source,
            }
        })?);
        let directory = self.directory.clone();
        Ok(iter::from_fn(move || {
            let result = {
                let entries = entries.as_mut()?;
                let entry = entries.next()?;
                match entry {
                    Ok(entry) => read_source_entry(entry, protection.as_ref()),
                    Err(source) => Err(TenonDocumentStoreError::DirectoryReadFailed {
                        path: directory.clone(),
                        source,
                    }),
                }
            };
            if result.is_err() {
                entries = None;
            }
            Some(result)
        }))
    }

    /// Atomically replaces the committed source for one validated identity.
    ///
    /// The caller serializes mutations and performs conditional HTTP checks
    /// before entering this physical boundary.
    pub(crate) fn commit(
        &self,
        document_id: &TenonDocumentId,
        source: &[u8],
        protection: &dyn DocumentProtection,
    ) -> Result<TenonDocumentEtag, TenonDocumentStoreError> {
        let id_sha256: [u8; SHA256_BYTES] = Sha256::digest(document_id.as_str().as_bytes()).into();
        let committed = self.directory.join(format!(
            "{}.{}",
            HexDigest(&id_sha256),
            COMMITTED_FILE_EXTENSION
        ));
        let temporary = self.directory.join(format!(
            "{}.{}",
            HexDigest(&id_sha256),
            TEMPORARY_FILE_EXTENSION
        ));
        let mut file = File::create(&temporary).map_err(|source| {
            TenonDocumentStoreError::FileWriteFailed {
                path: temporary.clone(),
                source,
            }
        })?;
        protection
            .protect(source, &mut file)
            .map_err(|source| TenonDocumentStoreError::ProtectionFailed { source })?;
        file.sync_all()
            .map_err(|source| TenonDocumentStoreError::FileWriteFailed {
                path: temporary.clone(),
                source,
            })?;
        fs::rename(&temporary, &committed).map_err(|source| {
            TenonDocumentStoreError::FileWriteFailed {
                path: committed,
                source,
            }
        })?;
        sync_directory(&self.directory)?;
        Ok(TenonDocumentEtag::for_source(source))
    }

    /// Durably removes one committed source known to exist in Runner memory.
    pub(crate) fn delete(
        &self,
        document_id: &TenonDocumentId,
    ) -> Result<(), TenonDocumentStoreError> {
        let id_sha256: [u8; SHA256_BYTES] = Sha256::digest(document_id.as_str().as_bytes()).into();
        let committed = self.directory.join(format!(
            "{}.{}",
            HexDigest(&id_sha256),
            COMMITTED_FILE_EXTENSION
        ));
        match fs::remove_file(&committed) {
            Ok(()) => sync_directory(&self.directory),
            Err(source) => Err(TenonDocumentStoreError::FileDeleteFailed {
                path: committed,
                source,
            }),
        }
    }
}

/// One committed file's short-lived owned bytes and filename identity.
pub(crate) struct StoredTenonDocumentSource {
    expected_id_sha256: [u8; SHA256_BYTES],
    source: Zeroizing<Box<[u8]>>,
}

impl StoredTenonDocumentSource {
    /// Returns the SHA-256 encoded by the committed file name.
    #[must_use]
    pub(crate) const fn expected_id_sha256(&self) -> &[u8; SHA256_BYTES] {
        &self.expected_id_sha256
    }

    /// Returns the safe committed file name used to identify startup errors.
    #[must_use]
    pub(crate) fn committed_file_name(&self) -> String {
        format!(
            "{}.{}",
            HexDigest(&self.expected_id_sha256),
            COMMITTED_FILE_EXTENSION
        )
    }

    /// Returns the exact owned source bytes without parsing or re-encoding.
    #[must_use]
    pub(crate) fn source(&self) -> &[u8] {
        &self.source
    }

    /// Transfers the exact original bytes into the in-memory desired record.
    #[must_use]
    pub(crate) fn into_source(self) -> Zeroizing<Box<[u8]>> {
        self.source
    }
}

impl fmt::Debug for StoredTenonDocumentSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredTenonDocumentSource")
            .field("expected_id_sha256", &HexDigest(&self.expected_id_sha256))
            .field("source_bytes", &self.source.len())
            .finish()
    }
}

/// A physical store failure that makes startup restoration impossible.
#[derive(Debug)]
pub(crate) enum TenonDocumentStoreError {
    ProtectionFailed {
        source: io::Error,
    },
    UnprotectionFailed {
        source: io::Error,
    },
    DirectoryReadFailed {
        path: PathBuf,
        source: io::Error,
    },
    TemporaryFileCleanupFailed {
        path: PathBuf,
        source: io::Error,
    },
    EntryInvalid {
        path: PathBuf,
    },
    FileReadFailed {
        path: PathBuf,
        source: io::Error,
    },
    ChangedDuringRead {
        path: PathBuf,
        expected_bytes: usize,
        observed_bytes: usize,
    },
    DirectoryWriteFailed {
        path: PathBuf,
        source: io::Error,
    },
    FileWriteFailed {
        path: PathBuf,
        source: io::Error,
    },
    FileDeleteFailed {
        path: PathBuf,
        source: io::Error,
    },
}

impl TenonDocumentStoreError {
    /// Returns a stable category without exposing an operating-system message.
    #[must_use]
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::ProtectionFailed { .. } => "tenon_document_store.protection_failed",
            Self::UnprotectionFailed { .. } => "tenon_document_store.unprotection_failed",
            Self::DirectoryReadFailed { .. } => "tenon_document_store.directory_read_failed",
            Self::TemporaryFileCleanupFailed { .. } => {
                "tenon_document_store.temporary_file_cleanup_failed"
            }
            Self::EntryInvalid { .. } => "tenon_document_store.entry_invalid",
            Self::FileReadFailed { .. } => "tenon_document_store.committed_file_read_failed",
            Self::ChangedDuringRead { .. } => {
                "tenon_document_store.committed_file_changed_during_read"
            }
            Self::DirectoryWriteFailed { .. } => "tenon_document_store.directory_write_failed",
            Self::FileWriteFailed { .. } => "tenon_document_store.committed_file_write_failed",
            Self::FileDeleteFailed { .. } => "tenon_document_store.committed_file_delete_failed",
        }
    }
}

impl fmt::Display for TenonDocumentStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProtectionFailed { .. } => {
                formatter.write_str("Tenon Document protection failed")
            }
            Self::UnprotectionFailed { .. } => {
                formatter.write_str("Tenon Document unprotection failed")
            }
            Self::DirectoryReadFailed { path, .. } => write!(
                formatter,
                "Tenon Document store cannot be read: {}",
                path.display()
            ),
            Self::TemporaryFileCleanupFailed { path, .. } => write!(
                formatter,
                "Tenon Document temporary file cannot be removed: {}",
                path.display()
            ),
            Self::EntryInvalid { path } => write!(
                formatter,
                "Tenon Document store entry is invalid: {}",
                path.display()
            ),
            Self::FileReadFailed { path, .. } => write!(
                formatter,
                "Tenon Document committed file cannot be read: {}",
                path.display()
            ),
            Self::ChangedDuringRead {
                path,
                expected_bytes,
                observed_bytes,
            } => write!(
                formatter,
                "Tenon Document committed file metadata reported {expected_bytes} bytes but the read boundary observed {observed_bytes} bytes: {}",
                path.display()
            ),
            Self::DirectoryWriteFailed { path, .. } => write!(
                formatter,
                "Tenon Document store cannot be prepared: {}",
                path.display()
            ),
            Self::FileWriteFailed { path, .. } => write!(
                formatter,
                "Tenon Document committed file cannot be written: {}",
                path.display()
            ),
            Self::FileDeleteFailed { path, .. } => write!(
                formatter,
                "Tenon Document committed file cannot be deleted: {}",
                path.display()
            ),
        }
    }
}

impl Error for TenonDocumentStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::DirectoryReadFailed { source, .. }
            | Self::ProtectionFailed { source }
            | Self::UnprotectionFailed { source }
            | Self::TemporaryFileCleanupFailed { source, .. }
            | Self::FileReadFailed { source, .. }
            | Self::DirectoryWriteFailed { source, .. }
            | Self::FileWriteFailed { source, .. }
            | Self::FileDeleteFailed { source, .. } => Some(source),
            Self::EntryInvalid { .. } | Self::ChangedDuringRead { .. } => None,
        }
    }
}

fn sync_directory(path: &Path) -> Result<(), TenonDocumentStoreError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| TenonDocumentStoreError::DirectoryWriteFailed {
            path: path.to_path_buf(),
            source,
        })
}

fn remove_temporary_entry(entry: &DirEntry) -> Result<(), TenonDocumentStoreError> {
    let file_type = entry.file_type().map_err(|source| {
        TenonDocumentStoreError::TemporaryFileCleanupFailed {
            path: entry.path(),
            source,
        }
    })?;
    if !file_type.is_file() {
        return Ok(());
    }
    fs::remove_file(entry.path()).map_err(|source| {
        TenonDocumentStoreError::TemporaryFileCleanupFailed {
            path: entry.path(),
            source,
        }
    })
}

fn read_source_entry(
    entry: DirEntry,
    protection: &dyn DocumentProtection,
) -> Result<StoredTenonDocumentSource, TenonDocumentStoreError> {
    let path = entry.path();
    let file_name = entry.file_name();
    if !has_extension(&file_name, COMMITTED_FILE_EXTENSION) {
        return Err(TenonDocumentStoreError::EntryInvalid { path });
    }
    let Some(expected_id_sha256) = decode_committed_file_name(&file_name) else {
        return Err(TenonDocumentStoreError::EntryInvalid { path });
    };
    let file_type =
        entry
            .file_type()
            .map_err(|source| TenonDocumentStoreError::FileReadFailed {
                path: path.clone(),
                source,
            })?;
    if !file_type.is_file() {
        return Err(TenonDocumentStoreError::EntryInvalid { path });
    }

    let stored = read_source_file(&path)?;
    let mut source = Zeroizing::new(Vec::new());
    protection
        .unprotect(&stored, &mut *source)
        .map_err(|source| TenonDocumentStoreError::UnprotectionFailed { source })?;
    Ok(StoredTenonDocumentSource {
        expected_id_sha256,
        source: Zeroizing::new(mem::take(&mut *source).into_boxed_slice()),
    })
}

fn read_source_file(path: &Path) -> Result<Zeroizing<Box<[u8]>>, TenonDocumentStoreError> {
    let file = File::open(path).map_err(|source| TenonDocumentStoreError::FileReadFailed {
        path: path.to_path_buf(),
        source,
    })?;
    let metadata = file
        .metadata()
        .map_err(|source| TenonDocumentStoreError::FileReadFailed {
            path: path.to_path_buf(),
            source,
        })?;
    if !metadata.is_file() {
        return Err(TenonDocumentStoreError::EntryInvalid {
            path: path.to_path_buf(),
        });
    }

    let expected_bytes = usize::try_from(metadata.len()).map_err(|source| {
        TenonDocumentStoreError::FileReadFailed {
            path: path.to_path_buf(),
            source: io::Error::other(source),
        }
    })?;
    read_fixed_length(file, path, expected_bytes)
}

fn read_fixed_length(
    mut reader: impl Read,
    path: &Path,
    expected_bytes: usize,
) -> Result<Zeroizing<Box<[u8]>>, TenonDocumentStoreError> {
    let mut source = Zeroizing::new(vec![0_u8; expected_bytes].into_boxed_slice());
    let mut observed_bytes = 0;
    while observed_bytes < expected_bytes {
        let read_bytes = read_retrying_interrupts(&mut reader, &mut source[observed_bytes..])
            .map_err(|source| TenonDocumentStoreError::FileReadFailed {
                path: path.to_path_buf(),
                source,
            })?;
        if read_bytes == 0 {
            return Err(TenonDocumentStoreError::ChangedDuringRead {
                path: path.to_path_buf(),
                expected_bytes,
                observed_bytes,
            });
        }
        observed_bytes += read_bytes;
    }

    let mut extra_byte = Zeroizing::new([0_u8; 1]);
    let extra_bytes =
        read_retrying_interrupts(&mut reader, &mut *extra_byte).map_err(|source| {
            TenonDocumentStoreError::FileReadFailed {
                path: path.to_path_buf(),
                source,
            }
        })?;
    if extra_bytes != 0 {
        return Err(TenonDocumentStoreError::ChangedDuringRead {
            path: path.to_path_buf(),
            expected_bytes,
            observed_bytes: expected_bytes.saturating_add(extra_bytes),
        });
    }

    Ok(source)
}

fn read_retrying_interrupts(reader: &mut impl Read, buffer: &mut [u8]) -> io::Result<usize> {
    loop {
        match reader.read(buffer) {
            Err(source) if source.kind() == io::ErrorKind::Interrupted => {}
            result => return result,
        }
    }
}

fn has_extension(file_name: &OsStr, extension: &str) -> bool {
    Path::new(file_name).extension() == Some(OsStr::new(extension))
}

fn decode_committed_file_name(file_name: &OsStr) -> Option<[u8; SHA256_BYTES]> {
    let file_name = file_name.to_str()?;
    let digest = file_name.strip_suffix(".jsonc")?.as_bytes();
    if digest.len() != SHA256_HEX_BYTES {
        return None;
    }

    let mut decoded = [0_u8; SHA256_BYTES];
    for (target, pair) in decoded.iter_mut().zip(digest.as_chunks::<2>().0) {
        *target = decode_lower_hex(pair[0])? << 4 | decode_lower_hex(pair[1])?;
    }
    Some(decoded)
}

const fn decode_lower_hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

struct HexDigest<'a>(&'a [u8; SHA256_BYTES]);

impl fmt::Display for HexDigest<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for HexDigest<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

#[cfg(test)]
mod tests;
