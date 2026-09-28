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

//! Kill real Queue owners at observed byte-copy and atomic-publication boundaries.

use std::num::NonZeroU32;
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use super::super::*;
use crate::bell::{SLOT_NOTIFIED, WaitOutcome, create_bell_region, slot_offset};

const PAYLOAD: &[u8] = b"candidate";
const PREFIX: &[u8] = b"committed-prefix";
const POINT_ENV: &str = "TENON_QUEUE_CRASH_POINT";
const PATH_ENV: &str = "TENON_QUEUE_CRASH_PATH";

#[derive(Clone, Copy, Debug)]
enum Layout {
    Plain,
    Wrap,
}

#[test]
fn producer_crash_preserves_only_committed_records() -> Result<(), Box<dyn Error>> {
    if let Some(point) = std::env::var_os(POINT_ENV) {
        let path = std::env::var_os(PATH_ENV).ok_or("missing child Queue path")?;
        let memory = CrashMemory::open(Path::new(&path), OpenRole::Writer, point)?;
        let _ = WriterCore::new(memory).try_write(PAYLOAD)?;
        return Err("producer did not reach its crash point".into());
    }
    for layout in [Layout::Plain, Layout::Wrap] {
        let regions = match layout {
            Layout::Plain => ["header", "body", "padding"].as_slice(),
            Layout::Wrap => ["wrap", "header", "body", "padding"].as_slice(),
        };
        let points = regions
            .iter()
            .flat_map(|region| {
                ["before", "half", "after"].map(|edge| format!("write_{region}_{edge}"))
            })
            .chain(["commit_before", "commit_after", "ring_peer_after"].map(str::to_owned));
        for point in points {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("queue.mmap");
            let region = seed_queue(&path, layout)?;
            let memory = MappedQueueMemory::open(
                &path,
                OpenRole::Reader,
                endpoint_bell(&region, OpenRole::Reader)?,
                Arc::clone(&region),
            )?;
            let commit_before = memory.load_commit()?;
            let release_before = memory.load_release()?;
            kill_at_point(
                "producer_crash_preserves_only_committed_records",
                &path,
                &point,
            )?;
            let committed = matches!(point.as_str(), "commit_after" | "ring_peer_after");
            assert_eq!(
                memory.load_commit()? > commit_before,
                committed,
                "{layout:?}/{point}"
            );
            assert_eq!(memory.load_release()?, release_before, "{layout:?}/{point}");
            let mut reader = open_reader(&path, &region)?;
            read_payload(&mut reader, PREFIX)?;
            if committed {
                read_payload(&mut reader, PAYLOAD)?;
            }
            assert_eq!(reader.try_read()?, ReadOutcome::Empty, "{layout:?}/{point}");
            verify_reusable(&path, &region, &mut reader)?;
        }
    }
    Ok(())
}

#[test]
fn consumer_crash_replays_until_shared_release() -> Result<(), Box<dyn Error>> {
    if let Some(point) = std::env::var_os(POINT_ENV) {
        let path = std::env::var_os(PATH_ENV).ok_or("missing child Queue path")?;
        let memory = CrashMemory::open(Path::new(&path), OpenRole::Reader, point)?;
        let mut reader = ReaderCore::new(memory);
        assert!(matches!(reader.try_read()?, ReadOutcome::Record(_)));
        reader.release(1)?;
        return Err("consumer did not reach its crash point".into());
    }
    for layout in [Layout::Plain, Layout::Wrap] {
        for point in [
            "copy_before",
            "copy_half",
            "copy_after",
            "release_before",
            "release_after",
            "ring_peer_after",
        ] {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("queue.mmap");
            let region = seed_queue(&path, layout)?;
            let mut writer = open_writer(&path, &region)?;
            assert!(matches!(
                writer.try_write(PAYLOAD)?,
                WriteOutcome::Committed(_)
            ));
            let mut reader = open_reader(&path, &region)?;
            read_payload(&mut reader, PREFIX)?;
            drop(reader);
            let memory = MappedQueueMemory::open(
                &path,
                OpenRole::Reader,
                endpoint_bell(&region, OpenRole::Reader)?,
                Arc::clone(&region),
            )?;
            let commit_before = memory.load_commit()?;
            let release_before = memory.load_release()?;
            kill_at_point("consumer_crash_replays_until_shared_release", &path, point)?;
            let released = matches!(point, "release_after" | "ring_peer_after");
            assert_eq!(memory.load_commit()?, commit_before, "{layout:?}/{point}");
            assert_eq!(
                memory.load_release()? > release_before,
                released,
                "{layout:?}/{point}"
            );
            let mut reader = open_reader(&path, &region)?;
            if !released {
                read_payload(&mut reader, PAYLOAD)?;
            }
            assert_eq!(reader.try_read()?, ReadOutcome::Empty, "{layout:?}/{point}");
            verify_reusable(&path, &region, &mut reader)?;
        }
    }
    Ok(())
}

/// A peer killed between publishing its ring and waking the owner leaves the
/// owner asleep on a notified doorbell, and only a force wake frees it.
#[cfg(not(feature = "loom-model"))]
#[test]
fn peer_killed_between_ring_and_wake_leaves_a_recoverable_doorbell() -> Result<(), Box<dyn Error>> {
    if let Some(point) = std::env::var_os(POINT_ENV) {
        let path = std::env::var_os(PATH_ENV).ok_or("missing child Queue path")?;
        let path = Path::new(&path);
        let region = BellRegion::open(&region_path(path))?;
        let mut writer = open_writer(path, &region)?;
        // The kill lands after this peer published its ring and before its wake
        // reached the kernel, so the call below never returns.
        crate::bell::park_next_platform_wake();
        let _ = writer.try_write(PAYLOAD)?;
        return Err(format!("the child never parked at {point:?}").into());
    }
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("queue.mmap");
    let region = seed_queue(&path, Layout::Plain)?;
    // Opening the reader publishes into the header the doorbell the committing
    // writer must ring, and the parked loop below takes ownership of that slot.
    let mut reader = open_reader(&path, &region)?;
    let bell = region.loop_bell(1)?;
    let observed = Arc::new(AtomicBool::new(false));
    let parked_condition = Arc::clone(&observed);
    let parked_bell = Arc::clone(&bell);
    let parked_loop = thread::spawn(move || {
        parked_bell.wait_until(|| Ok::<bool, BellError>(parked_condition.load(Ordering::Acquire)))
    });
    wait_until_armed(&path)?;
    wait_until_platform_wait(&bell)?;

    kill_parked_child(
        "peer_killed_between_ring_and_wake_leaves_a_recoverable_doorbell",
        &path,
        "ring_before_wake",
        || Ok(bell_slot_word(&path, 1)? == SLOT_NOTIFIED),
    )?;
    assert_eq!(
        bell_slot_word(&path, 1)?,
        SLOT_NOTIFIED,
        "the dead peer never published its ring"
    );
    assert!(
        !parked_loop.is_finished(),
        "a wake that never reached the kernel released the parked owner"
    );

    // Only the force wake a departed peer requires can free the owner, which
    // then observes the fact the dead peer committed.
    observed.store(true, Ordering::Release);
    bell.force_wake()?;
    let outcome = parked_loop.join().map_err(|_| "the parked loop panicked")?;
    assert_eq!(outcome?, WaitOutcome::Ready);

    read_payload(&mut reader, PREFIX)?;
    read_payload(&mut reader, PAYLOAD)?;
    assert_eq!(reader.try_read()?, ReadOutcome::Empty);
    verify_reusable(&path, &region, &mut reader)?;
    Ok(())
}

/// Waits until the parked loop has armed the doorbell its peer rings.
#[cfg(not(feature = "loom-model"))]
fn wait_until_armed(path: &Path) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while bell_slot_word(path, 1)? != SLOT_ARMED {
        if Instant::now() >= deadline {
            return Err("the parked loop never armed its doorbell".into());
        }
        thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

/// Waits until the parked loop has entered a platform wait on its doorbell.
#[cfg(not(feature = "loom-model"))]
fn wait_until_platform_wait(bell: &Arc<LoopBell>) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    while bell.platform_waits().0 == 0 {
        if Instant::now() >= deadline {
            return Err("the parked loop never entered a platform wait".into());
        }
        thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

/// Reads one doorbell slot word the way the format defines it.
#[cfg(not(feature = "loom-model"))]
fn bell_slot_word(path: &Path, slot: u32) -> Result<u32, Box<dyn Error>> {
    let word = read_word_at(&region_path(path), slot_offset(slot) as u64)?;
    Ok(u32::from_le_bytes(word))
}

/// Creates one Queue plus the Bell Region both endpoint roles bind inside.
fn seed_queue(path: &Path, layout: Layout) -> Result<Arc<BellRegion>, Box<dyn Error>> {
    create_queue_file(
        path,
        DataCapacity::try_from(64)?,
        NonZeroU64::new(24).ok_or("zero limit")?,
    )?;
    let region = create_regions(path)?;
    let mut writer = open_writer(path, &region)?;
    if matches!(layout, Layout::Wrap) {
        // A released 32-byte frame leaves the live prefix at offset 32 and the
        // next append at offset 56, forcing an 8-byte wrap before the candidate.
        assert!(matches!(
            writer.try_write(&[b'x'; 24])?,
            WriteOutcome::Committed(_)
        ));
        read_payload(&mut open_reader(path, &region)?, &[b'x'; 24])?;
    }
    assert!(matches!(
        writer.try_write(PREFIX)?,
        WriteOutcome::Committed(_)
    ));
    Ok(region)
}

/// Creates the one Bell Region holding both endpoint roles' doorbells.
fn create_regions(path: &Path) -> Result<Arc<BellRegion>, Box<dyn Error>> {
    create_bell_region(
        &region_path(path),
        NonZeroU32::new(2).ok_or("zero slots")?,
        1,
    )?;
    Ok(BellRegion::open(&region_path(path))?)
}

/// Returns the doorbell one endpoint role parks on.
fn endpoint_bell(
    region: &Arc<BellRegion>,
    role: OpenRole,
) -> Result<Arc<LoopBell>, QueueRuntimeError> {
    let index = match role {
        OpenRole::Writer => 0,
        OpenRole::Reader => 1,
    };
    Ok(LoopBell::new(region.slot(index)?))
}

fn open_writer(path: &Path, region: &Arc<BellRegion>) -> Result<QueueWriter, Box<dyn Error>> {
    Ok(QueueWriter::open(
        path,
        endpoint_bell(region, OpenRole::Writer)?,
        Arc::clone(region),
    )?)
}

fn open_reader(path: &Path, region: &Arc<BellRegion>) -> Result<QueueReader, Box<dyn Error>> {
    Ok(QueueReader::open(
        path,
        endpoint_bell(region, OpenRole::Reader)?,
        Arc::clone(region),
    )?)
}

fn region_path(path: &Path) -> std::path::PathBuf {
    path.with_extension("bells")
}

fn read_payload(reader: &mut QueueReader, expected: &[u8]) -> Result<(), Box<dyn Error>> {
    let ReadOutcome::Record(record) = reader.try_read()? else {
        return Err("committed record was missing".into());
    };
    assert_eq!(record.payload(), expected);
    reader.release(1)?;
    Ok(())
}

fn verify_reusable(
    path: &Path,
    region: &Arc<BellRegion>,
    reader: &mut QueueReader,
) -> Result<(), Box<dyn Error>> {
    let mut writer = open_writer(path, region)?;
    assert!(matches!(
        writer.try_write(b"recovered")?,
        WriteOutcome::Committed(_)
    ));
    read_payload(reader, b"recovered")?;
    assert_eq!(reader.try_read()?, ReadOutcome::Empty);
    Ok(())
}

/// Spawns this test as a child parked at `point` and kills it once `ready` holds.
///
/// `ready` must become true while the child is stopped at `point`: a child that
/// already exited cannot be killed at that boundary, and the test must say so
/// rather than pass on a child that never reached it.
fn kill_parked_child(
    test: &str,
    path: &Path,
    point: &str,
    ready: impl Fn() -> Result<bool, Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let mut child = CrashChild(
        Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                &format!("queue::mapped::tests::crash::{test}"),
                "--nocapture",
            ])
            .env(POINT_ENV, point)
            .env(PATH_ENV, path)
            .stdout(Stdio::null())
            .spawn()?,
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if ready()? {
            break;
        }
        if let Some(status) = child.0.try_wait()? {
            return Err(format!("child exited before {point}: {status}").into());
        }
        if Instant::now() >= deadline {
            return Err(format!("child did not reach {point}").into());
        }
        thread::sleep(Duration::from_millis(1));
    }
    child.0.kill()?;
    assert_eq!(child.0.wait()?.signal(), Some(9), "{point}");
    Ok(())
}

fn kill_at_point(test: &str, path: &Path, point: &str) -> Result<(), Box<dyn Error>> {
    kill_parked_child(test, path, point, || {
        Ok(path.with_extension("ready").exists())
    })
}

struct CrashChild(Child);

impl Drop for CrashChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// One endpoint whose every publication is gated on a named crash point.
struct CrashMemory {
    inner: MappedQueueMemory,
    point: std::ffi::OsString,
    marker: std::path::PathBuf,
}

impl CrashMemory {
    fn open(
        path: &Path,
        role: OpenRole,
        point: std::ffi::OsString,
    ) -> Result<Self, QueueRuntimeError> {
        let region = BellRegion::open(&region_path(path))?;
        Ok(Self {
            inner: MappedQueueMemory::open(path, role, endpoint_bell(&region, role)?, region)?,
            point,
            marker: path.with_extension("ready"),
        })
    }

    fn gate(&self, point: &str) {
        if self.point == point {
            assert!(
                std::fs::write(&self.marker, b"ready").is_ok(),
                "crash marker write failed"
            );
            loop {
                std::thread::park();
            }
        }
    }
}

impl WaitFront for CrashMemory {
    fn take_wait_interruption(&self) -> Option<WaitInterruption> {
        self.inner.take_wait_interruption()
    }

    fn arm_or_take_wait_interruption(&self) -> Result<WaitArmOutcome, BellError> {
        self.inner.arm_or_take_wait_interruption()
    }

    fn publish_wait_notification(&self) -> Result<(), BellError> {
        self.inner.publish_wait_notification()
    }

    fn wait_bell(&self) -> Result<SignalWaitOutcome, BellError> {
        self.inner.wait_bell()
    }

    fn wait_bell_for(&self, timeout: Duration) -> Result<SignalWaitOutcome, BellError> {
        self.inner.wait_bell_for(timeout)
    }
}

impl QueueMemory for CrashMemory {
    fn capacity(&self) -> DataCapacity {
        self.inner.capacity()
    }
    fn max_payload_size(&self) -> NonZeroU64 {
        self.inner.max_payload_size()
    }
    fn load_commit(&self) -> Result<LogicalPosition, QueueRuntimeError> {
        self.inner.load_commit()
    }
    fn publish_commit(&self, position: LogicalPosition) {
        self.gate("commit_before");
        self.inner.publish_commit(position);
        self.gate("commit_after");
    }
    fn load_release(&self) -> Result<LogicalPosition, QueueRuntimeError> {
        self.inner.load_release()
    }
    fn publish_release(&self, position: LogicalPosition) {
        self.gate("release_before");
        self.inner.publish_release(position);
        self.gate("release_after");
    }
    fn ring_peer(&self) -> Result<(), QueueRuntimeError> {
        let ring = self.inner.ring_peer();
        self.gate("ring_peer_after");
        ring
    }
    fn read_data(&self, offset: u64, destination: &mut [u8]) -> Result<(), QueueRuntimeError> {
        self.inner.read_data(offset, destination)
    }
    fn read_data_uninit(
        &self,
        offset: u64,
        destination: &mut [MaybeUninit<u8>],
    ) -> Result<(), QueueRuntimeError> {
        self.gate("copy_before");
        let half = destination.len() / 2;
        self.inner
            .read_data_uninit(offset, &mut destination[..half])?;
        self.gate("copy_half");
        self.inner
            .read_data_uninit(offset + half as u64, &mut destination[half..])?;
        self.gate("copy_after");
        Ok(())
    }
    fn write_data(&self, offset: u64, source: &[u8]) -> Result<(), QueueRuntimeError> {
        // This fixture uses distinct body/padding sizes; the real core still
        // owns all encoding, offsets, capacity decisions and publication.
        let region = match source.len() {
            FRAME_HEADER_LEN if source == [0; FRAME_HEADER_LEN] => "wrap",
            FRAME_HEADER_LEN => "header",
            9 => "body",
            7 => "padding",
            _ => unreachable!("unexpected crash fixture write"),
        };
        self.gate(&format!("write_{region}_before"));
        let half = source.len() / 2;
        self.inner.write_data(offset, &source[..half])?;
        self.gate(&format!("write_{region}_half"));
        self.inner
            .write_data(offset + half as u64, &source[half..])?;
        self.gate(&format!("write_{region}_after"));
        Ok(())
    }
}
