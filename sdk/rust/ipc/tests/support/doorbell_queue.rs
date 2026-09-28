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

//! Queue endpoints a single-sided test opens through a real doorbell.
//!
//! A test that plays both halves of one Queue in one process still opens each
//! half through the real doorbell protocol, so both halves bind and ring a real
//! Region. This fixture names it beside the Queue file, exactly as a plugin side
//! keeps its own-loop Region beside that side's Queue files: the writer half
//! binds the first slot and the reader half the second, so each commit rings the
//! bound reader and each release rings the bound writer.

use std::io;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use tenon_ipc::bell::{BellError, BellRegion, create_bell_region};
use tenon_ipc::queue::{QueueReader, QueueWriter};

/// Serializes publishing one Queue test's Bell Region.
///
/// Both halves of a Queue this fixture opens may ask for the Region at the same
/// moment. Creation writes the whole file before it reports success, so a peer
/// that maps those bytes while they are still being written would read a torn
/// Region. One lock per process makes the second half wait for the first.
static REGION_PUBLICATION: Mutex<()> = Mutex::new(());

/// Returns the Bell Region one single-sided Queue test's two halves ring.
pub(crate) fn queue_bell_region(queue_path: impl AsRef<Path>) -> io::Result<Arc<BellRegion>> {
    let path = queue_bell_region_path(queue_path)?;
    let _publication = REGION_PUBLICATION
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    match create_bell_region(&path, two_slots()?, 0) {
        Ok(()) => {}
        Err(BellError::Io { source, .. }) if source.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(io::Error::other(error)),
    }
    BellRegion::open(&path).map_err(io::Error::other)
}

/// Returns the path [`queue_bell_region`] names for one Queue file.
pub(crate) fn queue_bell_region_path(queue_path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let queue_path = queue_path.as_ref();
    let stem = queue_path
        .file_stem()
        .ok_or_else(|| io::Error::other("Queue path has no file name"))?;
    let mut name = stem.to_os_string();
    name.push(".bells");
    Ok(queue_path.with_file_name(name))
}

/// Opens the writer half of a single-sided Queue test through its doorbell.
pub(crate) fn open_queue_writer(queue_path: impl AsRef<Path>) -> io::Result<QueueWriter> {
    let queue_path = queue_path.as_ref();
    let region = queue_bell_region(queue_path)?;
    QueueWriter::open(
        queue_path,
        region.loop_bell(0).map_err(io::Error::other)?,
        region,
    )
    .map_err(io::Error::other)
}

/// Opens the reader half of a single-sided Queue test through its doorbell.
pub(crate) fn open_queue_reader(queue_path: impl AsRef<Path>) -> io::Result<QueueReader> {
    let queue_path = queue_path.as_ref();
    let region = queue_bell_region(queue_path)?;
    QueueReader::open(
        queue_path,
        region.loop_bell(1).map_err(io::Error::other)?,
        region,
    )
    .map_err(io::Error::other)
}

fn two_slots() -> io::Result<NonZeroU32> {
    NonZeroU32::new(2).ok_or_else(|| io::Error::other("a Queue test opens two doorbells"))
}
