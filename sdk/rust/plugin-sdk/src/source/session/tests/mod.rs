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
use crate::Error;
use crate::process::FailureBoundary;
use crate::test_support::create;
use crate::test_support::region;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tenon_ipc::queue::ReadOutcome;

/// The Bell Regions one Source fixture creates, and the endpoints they ring.
///
/// The Source's own loops own exactly two doorbells, one per loop, while the
/// fixture's Channel loops park in one Flow Region numbered by Channel index.
/// Opening a Queue through this fixture therefore reaches the same addresses the
/// real Instance and the real Channel loop do.
struct Bells {
    directory: PathBuf,
    /// The directory the Pipeline put the Flow's Region in.
    ///
    /// A Source's own directory holds its Queue pairs and its own-loop Region,
    /// and nothing else; the Flow's Region belongs to the Pipeline, so the
    /// fixture keeps it in a directory of its own and hands over its path. The
    /// fixture holds the directory open for exactly as long as it needs it.
    _flow: tempfile::TempDir,
    /// The Region record the Pipeline hands the Source on startup.
    channels_path: PathBuf,
    loops: Arc<BellRegion>,
    channels: Arc<BellRegion>,
}

impl Bells {
    /// Creates the two Regions a Core runtime would create before a launch.
    fn create(directory: &Path, channels: u32) -> Result<Self, Error> {
        let flow = tempfile::tempdir()?;
        let channels_path = flow.path().join("channels.bells");
        let loops = region(&directory.join(LOOPS_BELL_FILE_NAME), 2)?;
        let channels = region(&channels_path, channels)?;
        Ok(Self {
            directory: directory.to_owned(),
            _flow: flow,
            channels_path,
            loops,
            channels,
        })
    }

    /// Opens one Submission Queue the way its Flow Channel loop does.
    fn read_submission(&self, channel: u32) -> io::Result<Reader> {
        Reader::open(
            self.directory.join(format!("submission-{channel}.queue")),
            self.channels.loop_bell(channel)?,
            Arc::clone(&self.loops),
        )
        .map_err(io::Error::from)
    }

    /// Opens one Completion Queue the way its Flow Channel loop does.
    fn write_completion(&self, channel: u32) -> io::Result<Writer> {
        Writer::open(
            self.directory.join(format!("completion-{channel}.queue")),
            self.channels.loop_bell(channel)?,
            Arc::clone(&self.loops),
        )
        .map_err(io::Error::from)
    }
}

fn failures() -> (FailureBoundary, mpsc::Receiver<Error>) {
    let (reported, received) = mpsc::channel();
    (
        Arc::new(move |error| {
            let _ = reported.send(error);
        }),
        received,
    )
}

fn queues(directory: &Path, channels: usize, pending: usize, maximum: usize) -> Result<(), Error> {
    for index in 0..channels {
        create(
            &directory.join(format!("submission-{index}.queue")),
            (pending + 1) * ((maximum + 15) & !7),
            maximum,
        )?;
        create(
            &directory.join(format!("completion-{index}.queue")),
            (pending + 1) * 24,
            13,
        )?;
    }
    Ok(())
}

fn read(reader: &mut Reader) -> Result<IngressRecord, Error> {
    let start = Instant::now();
    loop {
        if let ReadOutcome::Record(record) = reader.try_read()? {
            let record = IngressRecord::decode(record.payload())?;
            reader.release(1)?;
            return Ok(record);
        }
        if start.elapsed() > Duration::from_secs(5) {
            return Err("Submission never committed".into());
        }
        thread::yield_now();
    }
}

fn complete(writer: &mut Writer, id: u64, status: i32) -> Result<(), Error> {
    assert!(matches!(
        {
            let record = IngressCompletion {
                record_id: id,
                status,
            };
            writer.try_write_with(
                record.encoded_len(),
                |destination| record.encode(destination).map_err(std::io::Error::other),
                || {},
            )
        }?,
        WriteOutcome::Committed(_)
    ));
    Ok(())
}

#[test]
fn invalid_layout_fails_before_any_submission() -> Result<(), Error> {
    for invalid in [
        "missing-pair",
        "gap",
        "wrong-completion",
        "inconsistent",
        "unexpected",
    ] {
        let directory = tempfile::tempdir()?;
        queues(directory.path(), 2, 1, 64)?;
        match invalid {
            "missing-pair" => std::fs::remove_file(directory.path().join("completion-1.queue"))?,
            "gap" => std::fs::rename(
                directory.path().join("submission-1.queue"),
                directory.path().join("submission-2.queue"),
            )?,
            "wrong-completion" => {
                std::fs::remove_file(directory.path().join("completion-1.queue"))?;
                create(&directory.path().join("completion-1.queue"), 48, 12)?;
            }
            "inconsistent" => {
                std::fs::remove_file(directory.path().join("submission-1.queue"))?;
                create(&directory.path().join("submission-1.queue"), 160, 72)?;
            }
            _ => {
                std::fs::write(directory.path().join("unrelated"), [])?;
            }
        }
        let bells = Bells::create(directory.path(), 2)?;
        let (failed, _received) = failures();
        assert!(
            Session::open(directory.path(), &bells.channels_path, failed).is_err(),
            "{invalid}"
        );
        assert!(matches!(
            bells.read_submission(0)?.try_read()?,
            ReadOutcome::Empty
        ));
    }
    Ok(())
}

#[test]
fn dropped_result_retains_admission_until_real_completion_and_channels_are_independent()
-> Result<(), Error> {
    let directory = tempfile::tempdir()?;
    queues(directory.path(), 2, 1, 64)?;
    let bells = Bells::create(directory.path(), 2)?;
    let (failed, _received) = failures();
    let mut session = Session::open(directory.path(), &bells.channels_path, failed)?;
    let sender = session.sender::<Vec<u8>>();
    assert!(sender.send(2, &vec![]).is_err());
    let result = sender.send(0, &vec![1])?;
    drop(result);
    assert_eq!(sender.send(0, &vec![2])?.wait()?, AckCode::Backpressure);
    let second = sender.send(1, &vec![3])?;
    let mut submission0 = bells.read_submission(0)?;
    let mut submission1 = bells.read_submission(1)?;
    let first = read(&mut submission0)?;
    let other = read(&mut submission1)?;
    let mut completion0 = bells.write_completion(0)?;
    let mut completion1 = bells.write_completion(1)?;
    complete(&mut completion1, other.record_id, 2)?;
    assert_eq!(second.wait()?, AckCode::Retry);
    assert_eq!(sender.send(0, &vec![2])?.wait()?, AckCode::Backpressure);
    complete(&mut completion0, first.record_id, 1)?;
    let start = Instant::now();
    while session.shared.channels[0].permits.available_permits() == 0 {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "real completion did not return admission"
        );
        thread::yield_now();
    }
    let next = sender.send(0, &vec![4])?;
    session.stop_accepting();
    session.quiesce()?;
    let next_record = read(&mut submission0)?;
    assert!(next_record.record_id > first.record_id);
    assert!(
        sender
            .send(0, &vec![5])?
            .wait()
            .expect_err("quiesced session rejects new sends")
            .is_session_closed()
    );
    complete(&mut completion0, next_record.record_id, 1)?;
    assert_eq!(next.wait()?, AckCode::Ok);
    session.close()?;
    assert!(
        sender
            .send(0, &vec![5])?
            .wait()
            .expect_err("closed session rejects new sends")
            .is_session_closed()
    );
    Ok(())
}

#[test]
fn full_submission_retains_payload_until_commit_or_close() -> Result<(), Error> {
    for resume in [false, true] {
        let directory = tempfile::tempdir()?;
        queues(directory.path(), 1, 1, 64)?;
        let bells = Bells::create(directory.path(), 1)?;
        let path = directory.path().join("submission-0.queue");
        // A previous session can leave the Queue full despite fresh permits.
        let mut previous = Writer::open(
            &path,
            bells.loops.loop_bell(0)?,
            Arc::clone(&bells.channels),
        )?;
        for _ in 0..2 {
            assert!(matches!(
                previous.try_write_with(
                    [42; 64].len(),
                    |destination| std::io::Write::write_all(destination, &[42; 64]),
                    || {}
                )?,
                WriteOutcome::Committed(_)
            ));
        }
        let expected_id = previous.committed_position()?.get() + 1;
        drop(previous);
        let (failed, _received) = failures();
        let mut session = Session::open(directory.path(), &bells.channels_path, failed)?;
        let payload = Bytes::from(vec![7; 32]);
        let result = session.shared.send(0, |_| Ok(payload.clone()))?;
        let shared = Arc::clone(&session.shared);
        let channel = &shared.channels[0];
        let start = Instant::now();
        while channel.state.lock().expect("test mutex").pending.is_empty() {
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "record was not held"
            );
            thread::yield_now();
        }
        assert!(!payload.is_unique());
        assert_eq!(channel.permits.available_permits(), 0);
        if resume {
            let mut reader = bells.read_submission(0)?;
            for _ in 0..2 {
                assert!(matches!(reader.try_read()?, ReadOutcome::Record(_)));
                reader.release(1)?;
            }
            let record = read(&mut reader)?;
            assert_eq!(record.record_id, expected_id);
            assert_eq!(record.payload, payload);
            assert_eq!(channel.permits.available_permits(), 0);
            let mut completion = bells.write_completion(0)?;
            complete(&mut completion, record.record_id, 1)?;
            assert_eq!(result.wait()?, AckCode::Ok);
            session.close()?;
        } else {
            session.close()?;
            assert!(
                result
                    .wait()
                    .expect_err("close fails an uncommitted request")
                    .is_session_closed()
            );
        }
        assert!(payload.is_unique(), "the held payload must be released");
        assert_eq!(channel.permits.available_permits(), 1);
    }
    Ok(())
}

#[test]
fn direct_submission_encoding_preserves_empty_and_varint_boundaries() -> Result<(), Error> {
    let directory = tempfile::tempdir()?;
    queues(directory.path(), 1, 1, 65_560)?;
    let bells = Bells::create(directory.path(), 1)?;
    let (failed, _received) = failures();
    let mut session = Session::open(directory.path(), &bells.channels_path, failed)?;
    let sender = session.sender::<Vec<u8>>();
    let mut reader = bells.read_submission(0)?;
    let mut completion_writer = bells.write_completion(0)?;
    for length in [
        0, 125, 126, 127, 128, 16_380, 16_381, 16_383, 16_384, 65_536,
    ] {
        let payload = vec![42; length];
        let completion = sender.send(0, &payload)?;
        let record = read(&mut reader)?;
        assert_eq!(record.payload.as_ref(), payload.encode_to_vec());
        complete(&mut completion_writer, record.record_id, 1)?;
        assert_eq!(completion.wait()?, AckCode::Ok);
    }
    session.close()?;
    Ok(())
}

#[test]
fn failed_encoding_and_complete_record_limit_return_admission_without_commit() -> Result<(), Error>
{
    let directory = tempfile::tempdir()?;
    queues(directory.path(), 1, 1, 16)?;
    let bells = Bells::create(directory.path(), 1)?;
    let (failed, _received) = failures();
    let mut session = Session::open(directory.path(), &bells.channels_path, failed)?;
    let shared = &session.shared.channels[0];
    assert_eq!(session.shared.send(0, |_| Err(()))?.wait()?, AckCode::Error);
    assert_eq!(
        session
            .shared
            .send(0, |_| Ok(Bytes::from_static(&[1; 16])))?
            .wait()?,
        AckCode::Error
    );
    let sender = session.sender::<Vec<u8>>();
    assert_eq!(sender.send(0, &vec![1; 100])?.wait()?, AckCode::Error);
    assert!(matches!(
        bells.read_submission(0)?.try_read()?,
        ReadOutcome::Empty
    ));
    assert_eq!(shared.permits.available_permits(), 1);
    // Twelve payload bytes plus the two one-byte keys, id and length fill 16 bytes.
    let accepted = session
        .shared
        .send(0, |_| Ok(Bytes::from_static(&[7; 12])))?;
    let record = read(&mut bells.read_submission(0)?)?;
    assert_eq!(record.encoded_len(), 16);
    complete(&mut bells.write_completion(0)?, record.record_id, 1)?;
    assert_eq!(accepted.wait()?, AckCode::Ok);
    session.close()?;
    Ok(())
}

#[test]
fn quiesce_waits_for_entered_encoding_and_submission_but_keeps_completion_alive()
-> Result<(), Error> {
    let directory = tempfile::tempdir()?;
    queues(directory.path(), 1, 1, 64)?;
    let bells = Bells::create(directory.path(), 1)?;
    let (failed, _received) = failures();
    let mut session = Session::open(directory.path(), &bells.channels_path, failed)?;
    let state = session.shared.clone();
    let shared = &state.channels[0];
    let (entered, encoding) = mpsc::sync_channel(0);
    let (finish, proceed) = mpsc::sync_channel(0);
    thread::scope(|scope| -> Result<(), Error> {
        let encoding_session = state.clone();
        let send = scope.spawn(move || {
            encoding_session.send(0, |_| {
                entered.send(()).expect("encoding observer remains alive");
                proceed.recv().expect("test releases encoding");
                Ok(Bytes::from_static(b"payload"))
            })
        });
        encoding.recv()?;
        let stopped = scope.spawn(|| {
            session.stop_accepting();
            session.quiesce()
        });
        let start = Instant::now();
        while matches!(shared.state.lock().expect("test mutex").phase, Phase::Open) {
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "quiesce never closed admission"
            );
            thread::yield_now();
        }
        assert!(!stopped.is_finished());
        finish.send(())?;
        let result = send.join().expect("encoding thread")?;
        stopped.join().expect("quiesce thread")?;
        let mut reader = bells.read_submission(0)?;
        let record = read(&mut reader)?;
        let mut writer = bells.write_completion(0)?;
        complete(&mut writer, record.record_id, 1)?;
        assert_eq!(result.wait()?, AckCode::Ok);
        Ok(())
    })?;
    session.close()?;
    Ok(())
}

#[test]
fn unknown_old_completions_do_not_complete_new_session_and_bad_status_fails_owner()
-> Result<(), Error> {
    let directory = tempfile::tempdir()?;
    queues(directory.path(), 1, 2, 64)?;
    let bells = Bells::create(directory.path(), 1)?;
    let (failed, received) = failures();
    let mut session = Session::open(directory.path(), &bells.channels_path, failed)?;
    let sender = session.sender::<Vec<u8>>();
    let first = sender.send(0, &vec![1])?;
    let mut reader = bells.read_submission(0)?;
    let old = read(&mut reader)?;
    session.close()?;
    assert!(first.wait().is_err());
    let (failed, _) = failures();
    let mut replacement = Session::open(directory.path(), &bells.channels_path, failed)?;
    let next = replacement.sender::<Vec<u8>>().send(0, &vec![2])?;
    let current = read(&mut reader)?;
    let mut writer = bells.write_completion(0)?;
    complete(&mut writer, old.record_id, 1)?;
    complete(&mut writer, current.record_id, 4)?;
    assert_eq!(next.wait()?, AckCode::Error);
    replacement.close()?;
    drop(received);
    let (failure_boundary, received) = failures();
    let mut failed_session =
        Session::open(directory.path(), &bells.channels_path, failure_boundary)?;
    let pending = failed_session.sender::<Vec<u8>>().send(0, &vec![3])?;
    let record = read(&mut reader)?;
    complete(&mut writer, record.record_id, 0)?;
    let reported = received.recv_timeout(Duration::from_secs(5))?;
    assert!(
        reported
            .to_string()
            .contains("Invalid Ingress completion status")
    );
    assert!(failed_session.close().is_err());
    assert!(
        Pin::new(&mut { pending })
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
            .is_pending(),
        "failed close must not continue resolving business callbacks"
    );
    let error = failed_session
        .check_running()
        .expect_err("invalid completion fails the session");
    let cause = std::error::Error::source(&error)
        .expect("the original Queue failure remains available")
        .downcast_ref::<io::Error>()
        .expect("Queue failures preserve their IO error");
    assert_eq!(cause.kind(), io::ErrorKind::InvalidData);
    Ok(())
}

#[test]
fn close_preserves_a_queue_failure_arriving_during_join() -> Result<(), Error> {
    let directory = tempfile::tempdir()?;
    queues(directory.path(), 1, 2, 64)?;
    let bells = Bells::create(directory.path(), 1)?;
    let (failed, _) = failures();
    let mut session = Session::open(directory.path(), &bells.channels_path, failed)?;
    let pending = session.sender::<Vec<u8>>().send(0, &vec![1])?;
    let worker_state = session.shared.clone();
    let completion = session
        .completion
        .take()
        .expect("opened session owns its Completion worker");
    // Model an entered IO operation that reports failure only after stopping
    // begins. The production close path must join it before deciding its result.
    session.completion = Some(thread::spawn(move || {
        completion.join().expect("Completion worker must stop");
        worker_state.failed(io::Error::other("Queue failure during close"));
    }));
    let error = session
        .close()
        .expect_err("late Queue failure must fail close");
    assert_eq!(error.to_string(), "Queue failure during close");
    let mut pending = pending;
    assert!(
        Pin::new(&mut pending)
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
            .is_pending(),
        "close must stop before resolving pending business results after failure"
    );
    Ok(())
}

#[test]
fn cleanup_wakes_registered_futures_after_releasing_source_locks() -> Result<(), Error> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Wake, Waker};

    struct Observer {
        session: Arc<SessionState>,
        unlocked: AtomicBool,
    }
    impl Wake for Observer {
        fn wake(self: Arc<Self>) {
            self.unlocked.store(
                self.session.channels[0].state.try_lock().is_ok(),
                Ordering::Release,
            );
        }
    }
    let directory = tempfile::tempdir()?;
    queues(directory.path(), 1, 1, 64)?;
    let bells = Bells::create(directory.path(), 1)?;
    let (failed, _received) = failures();
    let mut session = Session::open(directory.path(), &bells.channels_path, failed)?;
    let mut completion = session.sender::<Vec<u8>>().send(0, &vec![1])?;
    let observer = Arc::new(Observer {
        session: session.shared.clone(),
        unlocked: AtomicBool::new(false),
    });
    let waker = Waker::from(observer.clone());
    assert!(
        Pin::new(&mut completion)
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    session.close()?;
    assert!(observer.unlocked.load(Ordering::Acquire));
    assert!(completion.wait().is_err());
    Ok(())
}

#[test]
fn concurrent_producers_keep_record_bytes_order_and_completion_across_many_wraps()
-> Result<(), Error> {
    let directory = tempfile::tempdir()?;
    queues(directory.path(), 2, 8, 64)?;
    let bells = Bells::create(directory.path(), 2)?;
    let (failed, _received) = failures();
    let mut session = Session::open(directory.path(), &bells.channels_path, failed)?;
    let mut endpoints = (0..2)
        .map(|index| {
            Ok((
                bells.read_submission(index)?,
                bells.write_completion(index)?,
            ))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    thread::scope(|scope| -> Result<(), Error> {
        let mut producers = Vec::new();
        for producer in 0..4_u8 {
            let sender = session.sender::<Vec<u8>>();
            producers.push(scope.spawn(move || -> Result<(), Error> {
                for sequence in 1..=200_u64 {
                    let mut payload = vec![producer];
                    payload.extend_from_slice(&sequence.to_le_bytes());
                    assert_eq!(
                        sender.send(usize::from(producer) % 2, &payload)?.wait()?,
                        AckCode::Ok
                    );
                }
                Ok(())
            }));
        }
        let mut sequences = [0; 4];
        let start = Instant::now();
        while sequences.iter().sum::<u64>() != 800 {
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "concurrent Source traffic stalled"
            );
            for (reader, writer) in &mut endpoints {
                if let ReadOutcome::Record(record) = reader.try_read()? {
                    let record = IngressRecord::decode(record.payload())?;
                    reader.release(1)?;
                    let payload = Vec::<u8>::decode(record.payload)?;
                    let producer = usize::from(payload[0]);
                    let sequence = u64::from_le_bytes(payload[1..].try_into()?);
                    assert_eq!(sequence, sequences[producer] + 1);
                    sequences[producer] = sequence;
                    complete(writer, record.record_id, 1)?;
                }
            }
            thread::yield_now();
        }
        for producer in producers {
            producer.join().expect("business producer must not panic")?
        }
        Ok(())
    })?;
    session.stop_accepting();
    session.quiesce()?;
    session.close()?;
    Ok(())
}
