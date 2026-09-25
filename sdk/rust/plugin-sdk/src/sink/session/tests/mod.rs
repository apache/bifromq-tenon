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
use crate::test_support::region;
use std::collections::BTreeMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;
use tenon_ipc::queue::ReadOutcome;
use tenon_ipc::queue::{QueueWriter as Writer, WriteOutcome};
use tokio::sync::oneshot;

const DEADLINE: Duration = Duration::from_secs(5);

/// The fixed name of the Region a Flow's Channel loops park in.
///
/// Only the Pipeline creates this file; the Sink learns its absolute path from
/// each input's `channelBellPath`.
const CHANNELS_BELL_FILE_NAME: &str = "channels.bells";

/// The inputs one Sink fixture owns: two Channels of one Flow and one of another.
fn inputs(_directory: &Path) -> Vec<FlowChannel> {
    [("flow-a", 0_u32), ("flow-a", 1), ("flow-b", 0)]
        .into_iter()
        .map(|(flow_id, channel_id)| FlowChannel {
            flow_id: flow_id.to_owned(),
            channel_id,
        })
        .collect()
}

fn sink_inputs(directory: &Path, channels: &[FlowChannel]) -> Vec<SinkInput> {
    channels
        .iter()
        .cloned()
        .map(|channel| SinkInput {
            channel_bell_path: flow_bell_path(directory, &channel.flow_id),
            channel,
        })
        .collect()
}

/// The Region record the Pipeline hands one Sink for one Flow.
fn flow_bell_path(directory: &Path, flow_id: &str) -> PathBuf {
    directory
        .join("flows")
        .join(flow_id)
        .join(CHANNELS_BELL_FILE_NAME)
}

/// Creates one Bell Region together with the directories above it.
fn region_at(path: &Path, slots: u32) -> Result<Arc<BellRegion>, Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(region(path, slots)?)
}

/// The Bell Regions one Sink fixture creates, and the endpoints they ring.
///
/// A Sink's own loop owns one doorbell, while each writer parks in the Region
/// its Flow created. Both halves live in files, so opening a Queue through this
/// fixture reaches the same two addresses the real Sink and the real Channel
/// loops do.
struct Bells {
    loops: Arc<BellRegion>,
    flows: BTreeMap<String, Arc<BellRegion>>,
}

impl Bells {
    /// Creates the Regions a Core runtime creates before it launches a Sink.
    fn create(directory: &Path, channels: &[FlowChannel]) -> Result<Self, Error> {
        let loops = region_at(
            &FlowChannel::side_directory(directory).join(LOOPS_BELL_FILE_NAME),
            1,
        )?;
        let mut flows: BTreeMap<String, Arc<BellRegion>> = BTreeMap::new();
        let mut flow_slots: BTreeMap<String, u32> = BTreeMap::new();
        for channel in channels {
            let slots = channel
                .channel_id
                .checked_add(1)
                .ok_or("channel id does not fit a slot count")?;
            flow_slots
                .entry(channel.flow_id.clone())
                .and_modify(|existing| *existing = (*existing).max(slots))
                .or_insert(slots);
        }
        for (flow_id, slots) in flow_slots {
            let path = flow_bell_path(directory, &flow_id);
            flows.insert(flow_id, region_at(&path, slots)?);
        }
        Ok(Self { loops, flows })
    }

    /// The Region of the Flow that wrote one input.
    fn flow(&self, channel: &FlowChannel) -> Result<&Arc<BellRegion>, Error> {
        self.flows
            .get(&channel.flow_id)
            .ok_or_else(|| "missing Flow Bell Region".into())
    }

    /// Opens one Egress Queue the way its Flow Channel loop does.
    fn write(&self, path: &Path, channel: &FlowChannel) -> Result<Writer, Error> {
        Ok(Writer::open(
            path,
            self.flow(channel)?.loop_bell(channel.channel_id)?,
            Arc::clone(&self.loops),
        )?)
    }

    /// Opens one Egress Queue the way the Sink's own Egress loop does.
    fn read(&self, path: &Path, channel: &FlowChannel) -> Result<Reader, Error> {
        Ok(Reader::open(
            path,
            self.loops.loop_bell(0)?,
            Arc::clone(self.flow(channel)?),
        )?)
    }
}

/// The Queue header field that publishes the reading loop's own slot ordinal.
///
/// This pins the Sink's publication against the frozen header layout instead of
/// asking the SDK for its own value.
const READER_BELL_SLOT_OFFSET: u64 = 72;

/// Reads the slot ordinal one Queue's reader has published to its writer.
fn published_reader_slot(path: &Path) -> Result<u32, Error> {
    let file = File::open(path)?;
    let mut word = [0_u8; 4];
    file.read_exact_at(&mut word, READER_BELL_SLOT_OFFSET)?;
    Ok(u32::from_le_bytes(word))
}

struct WriteCall {
    channel: FlowChannel,
    records: Box<[String]>,
    complete: oneshot::Sender<Result<(), Error>>,
    observed: mpsc::Receiver<()>,
}

struct RecordingSink(mpsc::Sender<WriteCall>);

impl BatchWriter<String> for RecordingSink {
    fn write(
        &self,
        channel: FlowChannel,
        records: Box<[String]>,
    ) -> impl Future<Output = Result<(), Error>> {
        let (complete, result) = oneshot::channel();
        let (observed, receipt) = mpsc::channel();
        self.0
            .send(WriteCall {
                channel,
                records,
                complete,
                observed: receipt,
            })
            .expect("test caller remains alive");
        async move {
            let result = result.await?;
            let _ = observed.send(());
            result
        }
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    session: Option<Session>,
    channels: Vec<FlowChannel>,
    paths: Vec<PathBuf>,
    bells: Bells,
    writers: Vec<Writer>,
    writes: mpsc::Receiver<WriteCall>,
    events: mpsc::Receiver<Error>,
}

impl Fixture {
    fn paused() -> Result<Self, Error> {
        let directory = tempfile::tempdir()?;
        let channels = inputs(directory.path());
        let bells = Bells::create(directory.path(), &channels)?;
        let paths = channels
            .iter()
            .map(|channel| channel.queue_path(directory.path()))
            .collect::<Vec<_>>();
        let mut writers = Vec::new();
        for (path, channel) in paths.iter().zip(&channels) {
            std::fs::create_dir_all(path.parent().ok_or("missing queue directory")?)?;
            crate::test_support::create(path, 256, 64)?;
            writers.push(bells.write(path, channel)?);
        }
        let (writes, received) = mpsc::channel();
        let (events, notifications) = mpsc::channel();
        let session = Session::start(
            Queues::open(directory.path(), sink_inputs(directory.path(), &channels))?,
            Arc::new(RecordingSink(writes)),
            move |error| {
                events.send(error).expect("test observes Queue failure");
            },
        )?;
        Ok(Self {
            _directory: directory,
            session: Some(session),
            channels,
            paths,
            bells,
            writers,
            writes: received,
            events: notifications,
        })
    }

    fn write(&mut self, channel: usize, value: &str) -> Result<(), Error> {
        let encoded = crate::wire::sink::EgressRecord {
            payload: value.to_owned().encode_to_vec().into(),
        }
        .encode_to_vec();
        assert!(matches!(
            self.writers[channel].try_write(&encoded)?,
            WriteOutcome::Committed(_)
        ));
        Ok(())
    }

    fn close(&mut self) -> Result<(), Error> {
        self.session.take().expect("session already closed").close()
    }

    fn replay(&self, channel: usize) -> Result<Vec<String>, Error> {
        let mut reader = self
            .bells
            .read(&self.paths[channel], &self.channels[channel])?;
        let mut values = Vec::new();
        while let ReadOutcome::Record(record) = reader.try_read()? {
            values.push(decode(record)?);
        }
        Ok(values)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(mut session) = self.session.take() {
            session.close().expect("session close");
        }
    }
}

#[test]
fn flow_paths_and_payload_decoding_match_shared_vectors() -> Result<(), Error> {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../contracts/process_protocol_test_vectors.json"
    ))?;
    for vector in vectors["startup"]["valid"]
        .as_array()
        .ok_or("missing vectors")?
    {
        if vector["sdkConfig"]["sinkInputs"].is_null() {
            continue;
        }
        let channels: Vec<FlowChannel> =
            serde_json::from_value(vector["sdkConfig"]["sinkInputs"].clone())?;
        let paths = channels
            .iter()
            .map(|channel| channel.queue_path(Path::new("")))
            .collect::<Vec<_>>();
        assert_eq!(serde_json::to_value(paths)?, vector["relativeQueuePaths"]);
    }
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../contracts/payload_decode_test_vectors.json"
    ))?;
    #[derive(Clone, PartialEq, Message)]
    struct Payload {
        #[prost(string, tag = "1")]
        destination: String,
        #[prost(bytes = "vec", tag = "2")]
        body: Vec<u8>,
    }
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("egress.queue");
    let bells = region_at(&directory.path().join("loops.bells"), 2)?;
    crate::test_support::create(&path, 256, 64)?;
    let mut writer = Writer::open(&path, bells.loop_bell(0)?, Arc::clone(&bells))?;
    let mut reader = Reader::open(&path, bells.loop_bell(1)?, Arc::clone(&bells))?;
    let encoded: Vec<u8> = serde_json::from_value(vectors["matching"]["encoded"].clone())?;
    assert!(matches!(
        writer.try_write(&encoded)?,
        WriteOutcome::Committed(_)
    ));
    let payload: Payload = decode(match reader.try_read()? {
        ReadOutcome::Record(record) => record,
        ReadOutcome::Empty => return Err("missing frame".into()),
    })?;
    assert_eq!(payload.destination, "events");
    assert_eq!(payload.body, [0, 127, 128, 255]);
    reader.release(1)?;
    for vector in vectors["malformed"]
        .as_array()
        .ok_or("missing malformed vectors")?
    {
        let encoded: Vec<u8> = serde_json::from_value(vector["encoded"].clone())?;
        assert!(matches!(
            writer.try_write(&encoded)?,
            WriteOutcome::Committed(_)
        ));
        let error = decode::<Payload>(match reader.try_read()? {
            ReadOutcome::Record(record) => record,
            ReadOutcome::Empty => return Err("missing invalid frame".into()),
        })
        .expect_err("malformed payload must fail");
        assert_eq!(
            error.to_string(),
            vector["expectedErrorCode"]
                .as_str()
                .ok_or("missing error")?
        );
        assert!(error.source().is_some());
        reader.release(1)?;
    }
    Ok(())
}

#[test]
fn queued_records_form_one_batch_and_unfinished_results_do_not_block_close() -> Result<(), Error> {
    let mut fixture = Fixture::paused()?;
    fixture.write(0, "first")?;
    fixture.write(0, "second")?;
    fixture
        .session
        .as_ref()
        .ok_or("missing session")?
        .activate();
    let batch = fixture.writes.recv_timeout(DEADLINE)?;
    assert_eq!(batch.channel, fixture.channels[0]);
    assert_eq!(&*batch.records, &["first", "second"]);
    fixture.close()?;
    assert_eq!(fixture.replay(0)?, ["first", "second"]);
    assert!(
        batch.complete.send(Ok(())).is_err(),
        "stopped coordinator drops its result observer"
    );
    assert_eq!(&*batch.records, &["first", "second"]);
    Ok(())
}

#[test]
fn every_input_publishes_the_single_slot_of_the_one_egress_loop() -> Result<(), Error> {
    let fixture = Fixture::paused()?;
    assert_eq!(fixture.channels.len(), 3);
    for path in &fixture.paths {
        assert_eq!(published_reader_slot(path)?, 0);
    }
    Ok(())
}

#[test]
fn an_unfinished_batch_does_not_stall_another_input() -> Result<(), Error> {
    let mut fixture = Fixture::paused()?;
    fixture
        .session
        .as_ref()
        .ok_or("missing session")?
        .activate();
    fixture.write(0, "slow")?;
    let slow = fixture.writes.recv_timeout(DEADLINE)?;
    fixture.write(1, "fast")?;
    let fast = fixture.writes.recv_timeout(DEADLINE)?;
    fast.complete
        .send(Ok(()))
        .map_err(|_| "fast observer disappeared")?;
    fast.observed.recv_timeout(DEADLINE)?;
    fixture.write(1, "later")?;
    let later = fixture.writes.recv_timeout(DEADLINE)?;
    assert_eq!(&*later.records, &["later"]);
    fixture.close()?;
    assert_eq!(fixture.replay(1)?, ["later"]);
    assert_eq!(fixture.replay(0)?, ["slow"]);
    drop(slow);
    drop(later);
    Ok(())
}

#[test]
fn later_success_cannot_release_past_an_earlier_unfinished_batch() -> Result<(), Error> {
    let mut fixture = Fixture::paused()?;
    fixture
        .session
        .as_ref()
        .ok_or("missing session")?
        .activate();
    fixture.write(0, "first")?;
    let first = fixture.writes.recv_timeout(DEADLINE)?;
    fixture.write(0, "second")?;
    let second = fixture.writes.recv_timeout(DEADLINE)?;
    second
        .complete
        .send(Ok(()))
        .map_err(|_| "second observer disappeared")?;
    second.observed.recv_timeout(DEADLINE)?;
    fixture.write(0, "third")?;
    let third = fixture.writes.recv_timeout(DEADLINE)?;
    assert_eq!(fixture.replay(0)?, ["first", "second", "third"]);
    first
        .complete
        .send(Ok(()))
        .map_err(|_| "first observer disappeared")?;
    first.observed.recv_timeout(DEADLINE)?;
    fixture.write(0, "fourth")?;
    let fourth = fixture.writes.recv_timeout(DEADLINE)?;
    assert_eq!(&*fourth.records, &["fourth"]);
    assert_eq!(fixture.replay(0)?, ["third", "fourth"]);
    fixture.close()?;
    drop(third);
    drop(fourth);
    Ok(())
}

#[test]
fn failure_keeps_its_suffix_and_other_queues_keep_their_independent_release() -> Result<(), Error> {
    let mut fixture = Fixture::paused()?;
    fixture
        .session
        .as_ref()
        .ok_or("missing session")?
        .activate();
    fixture.write(0, "first")?;
    let failed = fixture.writes.recv_timeout(DEADLINE)?;
    fixture.write(1, "independent")?;
    let independent = fixture.writes.recv_timeout(DEADLINE)?;
    assert_eq!(independent.channel.channel_id, 1);
    independent
        .complete
        .send(Ok(()))
        .map_err(|_| "independent observer disappeared")?;
    independent.observed.recv_timeout(DEADLINE)?;
    fixture.write(1, "pending")?;
    let pending = fixture.writes.recv_timeout(DEADLINE)?;
    assert_eq!(fixture.replay(1)?, ["pending"]);
    failed
        .complete
        .send(Err("Expected write failure".into()))
        .map_err(|_| "failure observer disappeared")?;
    let error = fixture.events.recv_timeout(DEADLINE)?;
    assert!(fixture.close().is_err());
    assert!(error.to_string().contains("Expected write failure"));
    assert_eq!(fixture.replay(0)?, ["first"]);
    assert_eq!(fixture.replay(1)?, ["pending"]);
    drop(pending);
    Ok(())
}

#[test]
fn the_first_failed_input_fails_the_instance_and_later_failures_stay_unobserved()
-> Result<(), Error> {
    let mut fixture = Fixture::paused()?;
    fixture
        .session
        .as_ref()
        .ok_or("missing session")?
        .activate();
    fixture.write(0, "opened-first")?;
    let opened_first = fixture.writes.recv_timeout(DEADLINE)?;
    fixture.write(1, "opened-second")?;
    let opened_second = fixture.writes.recv_timeout(DEADLINE)?;
    opened_second
        .complete
        .send(Err("First failure".into()))
        .map_err(|_| "first observer disappeared")?;
    let first = fixture.events.recv_timeout(DEADLINE)?;
    assert!(fixture.close().is_err());
    assert!(
        first
            .to_string()
            .contains("channel 1 failed: First failure")
    );
    // One loop serves every input, so the first failure already ended it. A
    // failure that settles afterwards is neither observed nor reported: the
    // Instance has failed once, on the cause that stopped it.
    let _ = opened_first.complete.send(Err("Second failure".into()));
    assert!(
        fixture.events.try_recv().is_err(),
        "the stopped loop reports one cause"
    );
    Ok(())
}

#[test]
fn stopping_during_a_result_poll_ignores_the_late_result_and_late_wakes() -> Result<(), Error> {
    for outcome in [Ok(()), Err("Expected late failure".into())] {
        let directory = tempfile::tempdir()?;
        let channel = FlowChannel {
            flow_id: "poll-race".into(),
            channel_id: 0,
        };
        let bells = Bells::create(directory.path(), std::slice::from_ref(&channel))?;
        let path = channel.queue_path(directory.path());
        std::fs::create_dir_all(path.parent().ok_or("missing Queue directory")?)?;
        crate::test_support::create(&path, 256, 64)?;
        let mut writer = bells.write(&path, &channel)?;
        let (polling, observed) = mpsc::channel();
        let (resume, permission) = mpsc::channel();
        struct PausedPoll {
            polling: mpsc::Sender<std::task::Waker>,
            permission: Mutex<mpsc::Receiver<Result<(), Error>>>,
        }
        impl BatchWriter<String> for PausedPoll {
            fn write(
                &self,
                _: FlowChannel,
                _: Box<[String]>,
            ) -> impl Future<Output = Result<(), Error>> {
                std::future::poll_fn(|context| {
                    self.polling
                        .send(context.waker().clone())
                        .expect("test watches polling");
                    Poll::Ready(
                        self.permission
                            .lock()
                            .expect("test permission lock")
                            .recv_timeout(DEADLINE)
                            .expect("test resumes polling"),
                    )
                })
            }
        }
        let (events, failures) = mpsc::channel();
        let mut session = Session::start(
            Queues::open(
                directory.path(),
                sink_inputs(directory.path(), std::slice::from_ref(&channel)),
            )?,
            Arc::new(PausedPoll {
                polling,
                permission: Mutex::new(permission),
            }),
            move |error| {
                events.send(error).expect("test observes Queue failure");
            },
        )?;
        session.activate();
        let encoded = wire::sink::EgressRecord {
            payload: "pending".to_owned().encode_to_vec().into(),
        }
        .encode_to_vec();
        assert!(matches!(
            writer.try_write(&encoded)?,
            WriteOutcome::Committed(_)
        ));
        let waker = observed.recv_timeout(DEADLINE)?;
        let admission = session.worker.admission.clone();
        let (stopped, finished) = mpsc::channel();
        let closer = thread::spawn(move || {
            session.close().expect("session close");
            stopped.send(()).expect("test awaits close");
        });
        let phase = admission.phase.lock().expect("test admission lock");
        let (phase, deadline) = admission
            .activated
            .wait_timeout_while(phase, DEADLINE, |phase| !matches!(*phase, Phase::Stopping))
            .expect("test stop wait");
        assert!(!deadline.timed_out());
        drop(phase);
        resume.send(outcome).map_err(|_| "poll ended early")?;
        finished.recv_timeout(DEADLINE)?;
        closer.join().expect("test closer must finish");
        waker.wake();
        assert!(failures.try_recv().is_err());
        let mut replay = bells.read(&path, &channel)?;
        assert_eq!(
            decode::<String>(match replay.try_read()? {
                ReadOutcome::Record(record) => record,
                ReadOutcome::Empty => return Err("unconfirmed record disappeared".into()),
            })?,
            "pending"
        );
    }
    Ok(())
}
