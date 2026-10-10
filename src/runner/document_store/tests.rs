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
    SHA256_BYTES, TenonDocumentEtag, TenonDocumentStore, TenonDocumentStoreError, read_fixed_length,
};
use crate::identifiers::TenonDocumentId;
use crate::runner::extensions::{ArtifactProtection, ByPass};
use crate::runner::state_directory::TENON_DOCUMENT_STORE_DIRECTORY_NAME;
use sha2::{Digest as _, Sha256};
use std::error::Error as _;
use std::io::{self, Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{env, fs, process};

struct PartialProtectionFailure;

impl ArtifactProtection for PartialProtectionFailure {
    fn protect(&self, _source: &[u8], output: &mut dyn io::Write) -> io::Result<()> {
        output.write_all(b"partial protected output")?;
        Err(io::Error::other("Test protection failed"))
    }

    fn unprotect(&self, _stored: &[u8], output: &mut dyn io::Write) -> io::Result<()> {
        output.write_all(b"partial recovered output")?;
        Err(io::Error::other("Test unprotection failed"))
    }
}

#[test]
fn partial_protection_failure_preserves_the_committed_file() -> io::Result<()> {
    let directory = TemporaryDirectory::create()?;
    let root = directory.create_store()?;
    let id = TenonDocumentId::try_from("protected").map_err(io::Error::other)?;
    let store = directory.store();
    let original = b"original source";
    store
        .commit(&id, original, &ByPass)
        .map_err(io::Error::other)?;
    let error = store
        .commit(&id, b"replacement", &PartialProtectionFailure)
        .err()
        .ok_or_else(|| io::Error::other("Partial protection unexpectedly succeeded"))?;
    assert_eq!(error.code(), "tenon_document_store.protection_failed");
    assert_eq!(
        fs::read(root.join(committed_file_name("protected")))?,
        original
    );
    store
        .recover_stale_temporary_files()
        .map_err(io::Error::other)?;
    assert_eq!(fs::read_dir(&root)?.count(), 1);
    Ok(())
}

#[test]
fn partial_unprotection_failure_never_yields_a_source() -> io::Result<()> {
    let directory = TemporaryDirectory::create()?;
    let root = directory.create_store()?;
    fs::write(root.join(committed_file_name("protected")), b"stored bytes")?;
    let mut sources = directory
        .store()
        .sources(Arc::new(PartialProtectionFailure))
        .map_err(io::Error::other)?;
    let error = sources
        .next()
        .ok_or_else(|| io::Error::other("Stored source is missing"))?
        .err()
        .ok_or_else(|| io::Error::other("Partial plaintext escaped protection"))?;
    assert_eq!(error.code(), "tenon_document_store.unprotection_failed");
    assert!(sources.next().is_none());
    assert_eq!(
        fs::read(root.join(committed_file_name("protected")))?,
        b"stored bytes"
    );
    Ok(())
}

static TEMPORARY_DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn strong_etag_round_trip_rejects_noncanonical_text() {
    let etag = TenonDocumentEtag::for_source(&[0xab; SHA256_BYTES]);
    assert_eq!(
        TenonDocumentEtag::from_strong_value(&etag.strong_value()),
        Some(etag)
    );
    assert!(TenonDocumentEtag::from_strong_value(&etag.directory_name()).is_none());
    assert!(
        TenonDocumentEtag::from_strong_value(
            "\"ABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABABAB\""
        )
        .is_none()
    );
}

#[derive(Debug)]
struct TemporaryDirectory(PathBuf);

impl TemporaryDirectory {
    fn create() -> io::Result<Self> {
        let sequence = TEMPORARY_DIRECTORY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "tenon-tenon-document-store-test-{}-{sequence}",
            process::id()
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn create_store(&self) -> io::Result<PathBuf> {
        let path = self.0.join(TENON_DOCUMENT_STORE_DIRECTORY_NAME);
        fs::create_dir(&path)?;
        Ok(path)
    }

    fn store(&self) -> TenonDocumentStore {
        TenonDocumentStore::new(self.path().join(TENON_DOCUMENT_STORE_DIRECTORY_NAME))
    }
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn missing_store_is_a_read_failure_and_is_not_created() -> io::Result<()> {
    let state_directory = TemporaryDirectory::create()?;
    let store_directory = state_directory
        .path()
        .join(TENON_DOCUMENT_STORE_DIRECTORY_NAME);

    let error = state_directory
        .store()
        .sources(Arc::new(ByPass))
        .err()
        .ok_or_else(|| io::Error::other("missing Store was treated as empty"))?;

    assert_eq!(error.code(), "tenon_document_store.directory_read_failed");
    assert!(!store_directory.exists());
    Ok(())
}

#[test]
fn source_bytes_are_read_when_iteration_reaches_the_file() -> io::Result<()> {
    let state_directory = TemporaryDirectory::create()?;
    let store_directory = state_directory.create_store()?;
    let path = store_directory.join(committed_file_name("document"));
    fs::write(&path, b"old document")?;
    let store = state_directory.store();
    let mut sources = store.sources(Arc::new(ByPass)).map_err(io::Error::other)?;

    fs::write(path, b"current source")?;
    let source = sources
        .next()
        .ok_or_else(|| io::Error::other("stored Tenon Document is missing"))?
        .map_err(io::Error::other)?;

    assert_eq!(source.expected_id_sha256(), &id_sha256("document"));
    assert_eq!(source.source(), b"current source");
    assert_eq!(
        TenonDocumentEtag::for_source(source.source())
            .strong_value()
            .len(),
        SHA256_BYTES * 2 + 2
    );
    assert!(sources.next().is_none());
    let source_debug = format!("{source:?}");
    assert!(source_debug.contains("source_bytes"));
    assert!(!source_debug.contains("current source"));
    Ok(())
}

#[test]
fn first_physical_error_stops_source_iteration() -> io::Result<()> {
    let state_directory = TemporaryDirectory::create()?;
    let store_directory = state_directory.create_store()?;
    fs::write(store_directory.join("operator-note.txt"), b"note")?;
    let store = state_directory.store();
    let mut sources = store.sources(Arc::new(ByPass)).map_err(io::Error::other)?;

    let result = sources
        .next()
        .ok_or_else(|| io::Error::other("unexpected entry was ignored"))?;
    let Err(error) = result else {
        return Err(io::Error::other("unexpected entry was accepted"));
    };

    assert_eq!(error.code(), "tenon_document_store.entry_invalid");
    assert!(sources.next().is_none());
    Ok(())
}

#[test]
fn malformed_committed_file_name_stops_source_iteration() -> io::Result<()> {
    let state_directory = TemporaryDirectory::create()?;
    let store_directory = state_directory.create_store()?;
    fs::write(store_directory.join("not-a-sha256.jsonc"), b"invalid")?;
    let store = state_directory.store();
    let mut sources = store.sources(Arc::new(ByPass)).map_err(io::Error::other)?;

    let result = sources
        .next()
        .ok_or_else(|| io::Error::other("malformed committed file was ignored"))?;
    let Err(error) = result else {
        return Err(io::Error::other("malformed committed file was accepted"));
    };

    assert_eq!(error.code(), "tenon_document_store.entry_invalid");
    assert!(sources.next().is_none());
    Ok(())
}

#[test]
fn startup_recovery_removes_only_direct_regular_temporary_files() -> io::Result<()> {
    let state_directory = TemporaryDirectory::create()?;
    let store_directory = state_directory.create_store()?;
    let stale_temporary_file = store_directory.join("abandoned-write.tmp");
    let unrelated_file = store_directory.join("operator-note.txt");
    let unrelated_directory = store_directory.join("archive");
    let temporary_named_directory = store_directory.join("reserved.tmp");
    fs::write(&stale_temporary_file, b"partial")?;
    fs::write(&unrelated_file, b"note")?;
    fs::create_dir(&unrelated_directory)?;
    fs::create_dir(&temporary_named_directory)?;

    state_directory
        .store()
        .recover_stale_temporary_files()
        .map_err(io::Error::other)?;

    assert!(!stale_temporary_file.exists());
    assert!(unrelated_file.exists());
    assert!(unrelated_directory.exists());
    assert!(temporary_named_directory.exists());
    Ok(())
}

#[test]
fn startup_recovery_fails_without_recreating_a_missing_store() -> io::Result<()> {
    let state_directory = TemporaryDirectory::create()?;
    let store_directory = state_directory
        .path()
        .join(TENON_DOCUMENT_STORE_DIRECTORY_NAME);

    let error = state_directory
        .store()
        .recover_stale_temporary_files()
        .err()
        .ok_or_else(|| io::Error::other("missing Store was silently recreated"))?;

    assert_eq!(error.code(), "tenon_document_store.directory_read_failed");
    assert!(!store_directory.exists());
    Ok(())
}

#[test]
fn commit_does_not_recreate_a_missing_store() -> io::Result<()> {
    let state_directory = TemporaryDirectory::create()?;
    let store_directory = state_directory.create_store()?;
    fs::remove_dir(&store_directory)?;
    let document_id = TenonDocumentId::try_from("document").map_err(io::Error::other)?;

    let error = state_directory
        .store()
        .commit(&document_id, b"document source", &ByPass)
        .err()
        .ok_or_else(|| io::Error::other("commit recreated a missing Store"))?;

    assert_eq!(
        error.code(),
        "tenon_document_store.committed_file_write_failed"
    );
    assert!(!store_directory.exists());
    Ok(())
}

#[test]
fn delete_reports_a_committed_file_that_disappeared() -> io::Result<()> {
    let state_directory = TemporaryDirectory::create()?;
    state_directory.create_store()?;
    let document_id = TenonDocumentId::try_from("document").map_err(io::Error::other)?;

    let error = state_directory
        .store()
        .delete(&document_id)
        .err()
        .ok_or_else(|| io::Error::other("missing committed file was ignored"))?;

    assert_eq!(
        error.code(),
        "tenon_document_store.committed_file_delete_failed"
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn startup_recovery_preserves_temporary_symbolic_links() -> io::Result<()> {
    use std::os::unix::fs::symlink;

    let state_directory = TemporaryDirectory::create()?;
    let store_directory = state_directory.create_store()?;
    let target = state_directory.path().join("operator-owned.tmp");
    let link = store_directory.join("linked.tmp");
    fs::write(&target, b"external")?;
    symlink(&target, &link)?;

    state_directory
        .store()
        .recover_stale_temporary_files()
        .map_err(io::Error::other)?;

    assert!(link.symlink_metadata()?.file_type().is_symlink());
    assert_eq!(fs::read(target)?, b"external");
    Ok(())
}

#[test]
fn fixed_length_read_rejects_shorter_and_longer_content() -> io::Result<()> {
    let path = Path::new("/test/document.jsonc");
    let exact = read_fixed_length(Cursor::new(b"1234"), path, 4).map_err(io::Error::other)?;
    assert_eq!(&**exact, b"1234");

    let Err(shorter) = read_fixed_length(Cursor::new(b"123"), path, 4) else {
        return Err(io::Error::other("shorter content was accepted"));
    };
    assert!(matches!(
        shorter,
        TenonDocumentStoreError::ChangedDuringRead {
            expected_bytes: 4,
            observed_bytes: 3,
            ..
        }
    ));

    let Err(longer) = read_fixed_length(Cursor::new(b"12345"), path, 4) else {
        return Err(io::Error::other("longer content was accepted"));
    };
    assert!(matches!(
        &longer,
        TenonDocumentStoreError::ChangedDuringRead {
            expected_bytes: 4,
            observed_bytes: 5,
            ..
        }
    ));
    assert_eq!(
        longer.code(),
        "tenon_document_store.committed_file_changed_during_read"
    );
    Ok(())
}

#[test]
fn fixed_length_read_retries_interrupted_reads() -> io::Result<()> {
    let path = Path::new("/test/document.jsonc");
    let reader = InterruptBeforeEachRead {
        inner: Cursor::new(b"1234"),
        interrupt_next: true,
    };

    let source = read_fixed_length(reader, path, 4).map_err(io::Error::other)?;

    assert_eq!(&**source, b"1234");
    Ok(())
}

struct InterruptBeforeEachRead<R> {
    inner: R,
    interrupt_next: bool,
}

impl<R: Read> Read for InterruptBeforeEachRead<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if self.interrupt_next {
            self.interrupt_next = false;
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        self.interrupt_next = true;
        self.inner.read(buffer)
    }
}

#[test]
fn committed_directory_is_rejected() -> io::Result<()> {
    let state_directory = TemporaryDirectory::create()?;
    let store_directory = state_directory.create_store()?;
    fs::create_dir(store_directory.join(committed_file_name("directory")))?;
    let store = state_directory.store();
    let result = store
        .sources(Arc::new(ByPass))
        .map_err(io::Error::other)?
        .next()
        .ok_or_else(|| io::Error::other("committed directory was ignored"))?;
    let Err(error) = result else {
        return Err(io::Error::other("committed directory was accepted"));
    };

    assert!(matches!(
        error,
        TenonDocumentStoreError::EntryInvalid { .. }
    ));
    Ok(())
}

#[cfg(unix)]
#[test]
fn committed_symbolic_link_is_rejected() -> io::Result<()> {
    use std::os::unix::fs::symlink;

    let state_directory = TemporaryDirectory::create()?;
    let store_directory = state_directory.create_store()?;
    let target = state_directory.path().join("target.jsonc");
    fs::write(&target, b"target source")?;
    symlink(
        target,
        store_directory.join(committed_file_name("symbolic-link")),
    )?;
    let store = state_directory.store();
    let result = store
        .sources(Arc::new(ByPass))
        .map_err(io::Error::other)?
        .next()
        .ok_or_else(|| io::Error::other("committed symbolic link was ignored"))?;
    let Err(error) = result else {
        return Err(io::Error::other("committed symbolic link was accepted"));
    };

    assert!(matches!(
        error,
        TenonDocumentStoreError::EntryInvalid { .. }
    ));
    Ok(())
}

#[test]
fn existing_store_path_must_be_a_directory() -> io::Result<()> {
    let state_directory = TemporaryDirectory::create()?;
    fs::write(
        state_directory
            .path()
            .join(TENON_DOCUMENT_STORE_DIRECTORY_NAME),
        b"not a directory",
    )?;

    let Err(error) = state_directory.store().sources(Arc::new(ByPass)) else {
        return Err(io::Error::other("store file was accepted as a directory"));
    };
    assert_eq!(error.code(), "tenon_document_store.directory_read_failed");
    assert!(error.source().is_some());
    Ok(())
}

fn committed_file_name(id: &str) -> String {
    format!("{}.jsonc", digest_hex(id_sha256(id)))
}

fn digest_hex(digest: [u8; SHA256_BYTES]) -> String {
    let mut name = String::with_capacity(SHA256_BYTES * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(name, "{byte:02x}");
    }
    name
}

fn id_sha256(id: &str) -> [u8; SHA256_BYTES] {
    Sha256::digest(id.as_bytes()).into()
}
