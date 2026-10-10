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

//! Decodes tar.gz packages for the default ArtifactProtection method.

use super::PluginPackageOutput;
use flate2::bufread::GzDecoder;
use std::io::{self, BufReader, Read};
use tar::{Archive, PaxExtensions};

const MAX_ARCHIVE_ENTRIES: usize = 100_000;
const MAX_ARCHIVE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const MAX_ARCHIVE_METADATA_BYTES: u64 = 64 * 1024;
const TAR_END_MARKER_BYTES: u64 = 512;
const TAR_READ_BUFFER_BYTES: usize = 64 * 1024;

pub(super) fn open_tar_gz(
    package: &mut dyn Read,
    output: &mut dyn PluginPackageOutput,
) -> io::Result<()> {
    let input = BufReader::new(package);
    let gzip = GzDecoder::new(input);
    let bounded = BoundedReader::new(gzip, MAX_ARCHIVE_BYTES);
    let mut archive = Archive::new(bounded);
    let mut pending_metadata = PendingArchiveMetadata::default();
    let mut entry_count = 0_usize;
    {
        let entries = archive.entries().map_err(archive_error)?.raw(true);
        for entry in entries {
            entry_count += 1;
            if entry_count > MAX_ARCHIVE_ENTRIES {
                return Err(io::Error::from(io::ErrorKind::FileTooLarge));
            }
            let mut entry = entry.map_err(archive_error)?;
            let entry_type = entry.header().entry_type();
            if entry_type.is_gnu_longname() {
                if pending_metadata.gnu_path.is_some() {
                    return Err(io::Error::from(io::ErrorKind::InvalidData));
                }
                let mut path = read_archive_metadata(&mut entry)?;
                if path.last() == Some(&0) {
                    path.pop();
                }
                if path.is_empty() {
                    return Err(io::Error::from(io::ErrorKind::InvalidData));
                }
                pending_metadata.gnu_path = Some(path);
                continue;
            }
            if entry_type.is_pax_local_extensions() {
                if pending_metadata.has_pax_local() {
                    return Err(io::Error::from(io::ErrorKind::InvalidData));
                }
                let metadata = read_archive_metadata(&mut entry)?;
                pending_metadata.set_pax_local(&metadata)?;
                continue;
            }
            if entry_type.is_pax_global_extensions() {
                if pending_metadata.has_local() {
                    return Err(io::Error::from(io::ErrorKind::InvalidData));
                }
                let metadata = read_archive_metadata(&mut entry)?;
                validate_pax_global(&metadata)?;
                continue;
            }
            if !entry_type.is_file() && !entry_type.is_dir() {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            let size = entry.header().size().map_err(archive_error)?;
            let path_bytes = pending_metadata.take_path(entry.header().path_bytes().as_ref())?;
            if pending_metadata
                .take_size()
                .is_some_and(|value| value != size)
            {
                return Err(io::Error::from(io::ErrorKind::InvalidData));
            }
            let path = std::str::from_utf8(&path_bytes)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
            if entry_type.is_dir() {
                if size != 0 {
                    return Err(io::Error::from(io::ErrorKind::InvalidData));
                }
                output.create_dir(path)?;
            } else {
                let mut content = entry.take(size);
                output.write_file(path, &mut content)?;
                if content.limit() != 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }
        }
    }
    if pending_metadata.has_local() {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    let mut bounded = archive.into_inner();
    validate_tar_end(&mut bounded)?;
    let gzip = bounded.into_inner();
    let mut compressed_input = gzip.into_inner();
    let mut trailing = [0_u8; 1];
    if compressed_input
        .read(&mut trailing)
        .map_err(archive_error)?
        != 0
    {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    Ok(())
}

#[derive(Debug, Default)]
struct PendingArchiveMetadata {
    gnu_path: Option<Vec<u8>>,
    pax_path: Option<Vec<u8>>,
    pax_size: Option<u64>,
    pax_local_seen: bool,
}

impl PendingArchiveMetadata {
    fn has_pax_local(&self) -> bool {
        self.pax_local_seen
    }

    fn has_local(&self) -> bool {
        self.gnu_path.is_some() || self.has_pax_local()
    }

    fn set_pax_local(&mut self, metadata: &[u8]) -> io::Result<()> {
        self.pax_local_seen = true;
        for extension in PaxExtensions::new(metadata) {
            let extension = extension.map_err(archive_error)?;
            match extension.key_bytes() {
                b"path" => {
                    if self.pax_path.is_some() || extension.value_bytes().is_empty() {
                        return Err(io::Error::from(io::ErrorKind::InvalidData));
                    }
                    self.pax_path = Some(extension.value_bytes().to_vec());
                }
                b"size" => {
                    if self.pax_size.is_some() {
                        return Err(io::Error::from(io::ErrorKind::InvalidData));
                    }
                    let value = std::str::from_utf8(extension.value_bytes())
                        .ok()
                        .and_then(|value| value.parse::<u64>().ok())
                        .ok_or(io::Error::from(io::ErrorKind::InvalidData))?;
                    self.pax_size = Some(value);
                }
                b"linkpath" => return Err(io::Error::from(io::ErrorKind::InvalidData)),
                key if key.starts_with(b"GNU.sparse.") => {
                    return Err(io::Error::from(io::ErrorKind::InvalidData));
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn take_path(&mut self, header_path: &[u8]) -> io::Result<Vec<u8>> {
        match (self.gnu_path.take(), self.pax_path.take()) {
            (Some(_), Some(_)) => Err(io::Error::from(io::ErrorKind::InvalidData)),
            (Some(path), None) | (None, Some(path)) => Ok(path),
            (None, None) => Ok(header_path.to_vec()),
        }
    }

    fn take_size(&mut self) -> Option<u64> {
        self.pax_local_seen = false;
        self.pax_size.take()
    }
}

fn validate_pax_global(metadata: &[u8]) -> io::Result<()> {
    for extension in PaxExtensions::new(metadata) {
        let extension = extension.map_err(archive_error)?;
        let key = extension.key_bytes();
        if matches!(key, b"path" | b"size" | b"linkpath") || key.starts_with(b"GNU.sparse.") {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
    }
    Ok(())
}

fn read_archive_metadata(input: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    input
        .take(MAX_ARCHIVE_METADATA_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(archive_error)?;
    if bytes.len() as u64 > MAX_ARCHIVE_METADATA_BYTES {
        return Err(io::Error::from(io::ErrorKind::FileTooLarge));
    }
    Ok(bytes)
}

fn validate_tar_end(input: &mut impl Read) -> io::Result<()> {
    let mut trailing_bytes = 0_u64;
    let mut buffer = [0_u8; TAR_READ_BUFFER_BYTES];
    loop {
        let count = input.read(&mut buffer).map_err(archive_error)?;
        if count == 0 {
            break;
        }
        if buffer[..count].iter().any(|byte| *byte != 0) {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        trailing_bytes = trailing_bytes
            .checked_add(count as u64)
            .ok_or(io::Error::from(io::ErrorKind::FileTooLarge))?;
    }
    if trailing_bytes < TAR_END_MARKER_BYTES {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    }
    Ok(())
}

#[derive(Debug)]
struct BoundedReader<R> {
    inner: R,
    remaining: u64,
}

impl<R> BoundedReader<R> {
    const fn new(inner: R, limit: u64) -> Self {
        Self {
            inner,
            remaining: limit,
        }
    }

    fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read> Read for BoundedReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            let mut probe = [0_u8; 1];
            return match self.inner.read(&mut probe)? {
                0 => Ok(0),
                _ => Err(io::ErrorKind::FileTooLarge.into()),
            };
        }
        let available = usize::try_from(self.remaining)
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let count = self.inner.read(&mut buffer[..available])?;
        self.remaining -= count as u64;
        Ok(count)
    }
}

fn archive_error(source: io::Error) -> io::Error {
    if source.kind() == io::ErrorKind::FileTooLarge {
        source
    } else {
        io::Error::new(io::ErrorKind::InvalidData, source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_limit_accepts_the_boundary_and_rejects_one_more_byte() -> io::Result<()> {
        for length in [3, 4] {
            let mut input = BoundedReader::new(io::repeat(0).take(length), 3);
            let result = io::copy(&mut input, &mut io::sink());
            if length == 3 {
                assert_eq!(result?, 3);
            } else {
                assert!(
                    matches!(result, Err(error) if error.kind() == io::ErrorKind::FileTooLarge)
                );
            }
        }
        Ok(())
    }
}
