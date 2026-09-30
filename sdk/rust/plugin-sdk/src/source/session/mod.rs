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

//! Source admission and the Source's two owned loops.
//!
//! A Source opens one Queue pair per Channel but exactly two waiting loops: one
//! writes every Submission Queue and one reads every Completion Queue. The
//! thread count is this SDK's local decision and never follows the Channel
//! count, so both loops publish one doorbell ordinal of their own Region and
//! every Queue handle points at the loop that serves it.

#![expect(
    clippy::expect_used,
    reason = "Poisoning or a missing owned worker denotes an SDK bug"
)]

use crate::Error;
use crate::LOOPS_BELL_FILE_NAME;
use crate::process::FailureBoundary;
use crate::wire::source::{IngressCompletion, IngressRecord};
use bytes::Bytes;
use prost::Message;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::task::{Context, Poll};
use std::thread::{self, JoinHandle};
use tenon_ipc::bell::{BellInterrupter, BellRegion, LoopBell};
use tenon_ipc::queue::ReadOutcome;
use tenon_ipc::queue::{QueueReader as Reader, QueueWriter as Writer, WriteOutcome};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};

/// The Submission loop's doorbell. Every Submission Queue publishes this
/// ordinal, so any Channel that frees space wakes the one loop that writes.
const SUBMISSION_BELL_SLOT: u32 = 0;
/// The Completion loop's doorbell. Every Completion Queue publishes this
/// ordinal, so any Channel that commits a result wakes the one loop that reads.
const COMPLETION_BELL_SLOT: u32 = 1;

/// A terminal result for one Source send; mapping it to an external ACK is the author's job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckCode {
    /// The configured delivery boundary was reached.
    Ok,
    /// No completion boundary was reached; the author may choose to retry.
    Retry,
    /// This attempt could not acquire local admission.
    Backpressure,
    /// This record could not complete normally.
    Error,
}

/// A Source session ended before this attempt received a terminal result.
#[derive(Clone, Debug)]
pub struct SendError {
    cause: Arc<io::Error>,
    session_closed: bool,
}

impl SendError {
    fn closed() -> Self {
        Self {
            cause: io::Error::other("Source session ended before completion").into(),
            session_closed: true,
        }
    }

    fn failed(cause: io::Error) -> Self {
        Self {
            cause: cause.into(),
            session_closed: false,
        }
    }

    /// Returns true when the SDK closed Source admission or ended the Queue session.
    pub fn is_session_closed(&self) -> bool {
        self.session_closed
    }
}

impl fmt::Display for SendError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.cause.fmt(output)
    }
}

impl std::error::Error for SendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

/// An admitted send's result. Dropping this future does not cancel the send or return its permit.
#[derive(Debug)]
pub struct Completion(oneshot::Receiver<Result<AckCode, SendError>>);

impl Completion {
    /// Waits on a business thread. Do not call inside an asynchronous runtime.
    pub fn wait(self) -> Result<AckCode, SendError> {
        self.0
            .blocking_recv()
            .expect("every request owner resolves its result")
    }

    fn resolved(result: AckCode) -> Self {
        let (send, receive) = oneshot::channel();
        let _ = send.send(Ok(result));
        Self(receive)
    }

    fn failed(error: SendError) -> Self {
        let (send, receive) = oneshot::channel();
        let _ = send.send(Err(error));
        Self(receive)
    }
}

impl Future for Completion {
    type Output = Result<AckCode, SendError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(context)
            .map(|result| result.expect("every request owner resolves its result"))
    }
}

/// A channel index outside this Source interface's channel set.
#[derive(Clone, Copy, Debug)]
pub struct InvalidChannel;

impl fmt::Display for InvalidChannel {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("Source channel index is out of range")
    }
}

impl std::error::Error for InvalidChannel {}

/// A thread-safe sender for the Source payload generated in this Plugin build.
///
/// Encoding runs synchronously on the caller. Each channel preserves enqueue order;
/// result futures run on the executor explicitly chosen by the business program.
#[derive(Debug)]
pub struct PayloadSender<P> {
    session: Arc<SessionState>,
    payload: PhantomData<fn(P)>,
}

impl<P: Message> PayloadSender<P> {
    /// Validates the channel, reserves admission, then encodes and queues one attempt.
    /// Oversize payloads return `Error`; lack of admission returns `Backpressure`
    /// before encoding. The SDK never resends an attempt automatically.
    pub fn send(&self, channel: usize, payload: &P) -> Result<Completion, InvalidChannel> {
        self.session.send(channel, |maximum| {
            let length = payload.encoded_len();
            if length > maximum {
                return Err(());
            }
            let mut bytes = Vec::with_capacity(length);
            payload.encode(&mut bytes).map_err(|_| ())?;
            Ok(Bytes::from(bytes))
        })
    }
}

impl<P> Clone for PayloadSender<P> {
    fn clone(&self) -> Self {
        Self {
            session: self.session.clone(),
            payload: PhantomData,
        }
    }
}

/// Owns both Queue loops until explicit close. Each loop owns its own mappings.
#[derive(Debug)]
pub(crate) struct Session {
    shared: Arc<SessionState>,
    submission: Option<JoinHandle<()>>,
    completion: Option<JoinHandle<()>>,
    submission_interrupt: BellInterrupter,
    completion_interrupt: BellInterrupter,
    progress: mpsc::Receiver<PumpEvent>,
}

impl Session {
    /// Opens one Queue pair per Channel and the doorbells both pumps park on.
    ///
    /// `channel_bell_path` is the Region of the Flow this Source feeds. Both
    /// pumps ring it and publish their own ordinals in `loops.bells` instead,
    /// so releasing a Submission or committing a Completion wakes the exact
    /// Channel that is waiting for it. This side's own Region holds one slot
    /// per loop, not per Channel: every Submission handle publishes the
    /// Submission loop's ordinal and every Completion handle publishes the
    /// Completion loop's, whatever the Channel count is.
    pub(crate) fn open(
        directory: &Path,
        channel_bell_path: &Path,
        failed: FailureBoundary,
    ) -> io::Result<Self> {
        let count = discover(directory)?;
        let loops = BellRegion::open(&directory.join(LOOPS_BELL_FILE_NAME))?;
        let submission_bell = loops.loop_bell(SUBMISSION_BELL_SLOT)?;
        let completion_bell = loops.loop_bell(COMPLETION_BELL_SLOT)?;
        let channels_region = BellRegion::open(channel_bell_path)?;
        let mut endpoints = Vec::with_capacity(count);
        let mut channels = Vec::with_capacity(count);
        let mut previous = None;
        for index in 0..count {
            let writer = Writer::open(
                directory.join(format!("submission-{index}.queue")),
                Arc::clone(&submission_bell),
                Arc::clone(&channels_region),
            )?;
            let reader = Reader::open(
                directory.join(format!("completion-{index}.queue")),
                Arc::clone(&completion_bell),
                Arc::clone(&channels_region),
            )?;
            let limit = pair_limit(&writer, &reader)?;
            let layout = (limit, writer.max_payload_size().get() as usize);
            if previous.is_some_and(|previous| previous != layout) {
                return Err(invalid("Source channels have inconsistent limits"));
            }
            previous = Some(layout);
            channels.push(Shared {
                state: Mutex::new(State {
                    phase: Phase::Open,
                    encoders: 0,
                    preflight: VecDeque::new(),
                    pending: BTreeMap::new(),
                }),
                changed: Condvar::new(),
                permits: Arc::new(Semaphore::new(limit)),
                maximum_record_bytes: writer.max_payload_size().get() as usize,
            });
            endpoints.push((writer, reader));
        }
        let submission_interrupt = submission_bell.interrupter();
        let completion_interrupt = completion_bell.interrupter();
        let (progress, received) = mpsc::channel();
        let mut session = Self {
            shared: Arc::new(SessionState {
                channels,
                submission_bell,
                failed,
                progress,
                failure: Mutex::new(None),
            }),
            submission: None,
            completion: None,
            submission_interrupt,
            completion_interrupt,
            progress: received,
        };
        let (writers, readers): (Vec<_>, Vec<_>) = endpoints.into_iter().unzip();
        let shared = session.shared.clone();
        session.submission = Some(
            thread::Builder::new()
                .name("tenon-source-submission".into())
                .spawn(move || {
                    if let Err(error) =
                        submissions(writers, &shared, Arc::clone(&shared.submission_bell))
                    {
                        shared.failed(error);
                    }
                    let _ = shared.progress.send(PumpEvent::Exited { submission: true });
                })?,
        );
        let shared = session.shared.clone();
        let completion_bell = Arc::clone(&completion_bell);
        session.completion = Some(
            thread::Builder::new()
                .name("tenon-source-completion".into())
                .spawn(move || {
                    if let Err(error) = completions(readers, &shared, completion_bell) {
                        shared.failed(error);
                    }
                    let _ = shared
                        .progress
                        .send(PumpEvent::Exited { submission: false });
                })?,
        );
        Ok(session)
    }

    pub(crate) fn sender<P>(&self) -> PayloadSender<P> {
        PayloadSender {
            session: self.shared.clone(),
            payload: PhantomData,
        }
    }

    pub(crate) fn parallelism(&self) -> usize {
        self.shared.channels.len()
    }

    pub(crate) fn stop_accepting(&self) {
        // Hold the complete channel set across the admission transition. Normal
        // sends still lock only their own channel; no parallel state is needed.
        let mut states = self
            .shared
            .channels
            .iter()
            .map(|channel| channel.state.lock().expect("Source state must not panic"))
            .collect::<Vec<_>>();
        for state in &mut states {
            if matches!(state.phase, Phase::Open) {
                state.phase = Phase::Quiescing
            }
        }
        drop(states);
        for channel in &self.shared.channels {
            channel.changed.notify_all();
        }
        // The Submission loop parks with nothing left to write, and a quiesced
        // Channel is only handed over once that loop re-reads its phase, so the
        // admitting side rings the doorbell the loop waits on.
        if let Err(error) = self.shared.submission_bell.ring() {
            self.shared.failed(error.into());
        }
    }

    pub(crate) fn quiesce(&mut self) -> Result<(), SendError> {
        while self.submission.is_some() {
            self.join_next()?;
        }
        self.check_running()
    }

    pub(crate) fn check_running(&self) -> Result<(), SendError> {
        self.shared.check_failure()
    }

    pub(crate) fn failure_handler(&self) -> impl Fn(Error) + Clone + Send + Sync + 'static {
        let shared = self.shared.clone();
        move |error| {
            // A combined Sink failure must wake Source pumps even while the
            // lifecycle caller is joining a blocked Submission during quiesce.
            shared.failed(io::Error::other(error));
        }
    }

    fn join_next(&mut self) -> Result<(), SendError> {
        self.check_running()?;
        match self
            .progress
            .recv()
            .expect("live pumps own the progress channel")
        {
            PumpEvent::Failed(error) => Err(error),
            PumpEvent::Exited { submission } => {
                join(if submission {
                    &mut self.submission
                } else {
                    &mut self.completion
                });
                self.check_running()
            }
        }
    }

    pub(crate) fn close(&mut self) -> Result<(), SendError> {
        self.check_running()?;
        self.shared.stop_all()?;
        self.submission_interrupt
            .interrupt()
            .map_err(|error| SendError::failed(error.into()))?;
        self.completion_interrupt
            .interrupt()
            .map_err(|error| SendError::failed(error.into()))?;
        while self.submission.is_some() || self.completion.is_some() {
            self.join_next()?;
        }
        for channel in &self.shared.channels {
            let (preflight, pending, error) = {
                let mut state = channel.state.lock().expect("Source cleanup must not panic");
                while state.encoders != 0 {
                    state = channel
                        .changed
                        .wait(state)
                        .expect("Source cleanup must not panic");
                }
                let Phase::Stopped = &state.phase else {
                    unreachable!("close sets Stopped before joining")
                };
                let error = SendError::closed();
                (
                    std::mem::take(&mut state.preflight),
                    std::mem::take(&mut state.pending),
                    error,
                )
            };
            // Future wakeups can enter business executors, so resolve outside locks.
            for submission in preflight {
                submission.request.complete(Err(error.clone()));
            }
            for request in pending.into_values() {
                request.complete(Err(error.clone()));
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Request {
    permit: OwnedSemaphorePermit,
    result: oneshot::Sender<Result<AckCode, SendError>>,
}

impl Request {
    fn complete(self, result: Result<AckCode, SendError>) {
        // Returning admission precedes waking the business caller.
        drop(self.permit);
        let _ = self.result.send(result);
    }
}

#[derive(Debug)]
struct Submission {
    payload: Bytes,
    request: Request,
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    Open,
    Quiescing,
    Stopped,
}

#[derive(Debug)]
struct State {
    phase: Phase,
    encoders: usize,
    preflight: VecDeque<Submission>,
    pending: BTreeMap<u64, Request>,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    permits: Arc<Semaphore>,
    // Freeze the externally validated startup limit for callers after the writer
    // moves into its pump. No business caller can access or retain the mapping.
    maximum_record_bytes: usize,
}

impl Shared {
    fn send(&self, encode: impl FnOnce(usize) -> Result<Bytes, ()>) -> Completion {
        let permit = {
            let mut state = self.state.lock().expect("Source admission must not panic");
            if !matches!(state.phase, Phase::Open) {
                return Completion::failed(SendError::closed());
            }
            let Ok(permit) = self.permits.clone().try_acquire_owned() else {
                return Completion::resolved(AckCode::Backpressure);
            };
            state.encoders += 1;
            permit
        };
        let encoding = Encoding(self);
        let Ok(payload) = encode(self.maximum_record_bytes) else {
            drop(permit);
            return Completion::resolved(AckCode::Error);
        };
        let (result, receive) = oneshot::channel();
        let request = Request { permit, result };
        {
            let mut state = self.state.lock().expect("Source admission must not panic");
            if let Phase::Stopped = &state.phase {
                request.complete(Err(SendError::closed()));
            } else {
                state.preflight.push_back(Submission { payload, request });
            }
        }
        drop(encoding);
        Completion(receive)
    }

    fn stop(&self) {
        self.state
            .lock()
            .expect("Source state must not panic")
            .phase = Phase::Stopped;
        self.changed.notify_all();
    }

    fn stopped(&self) -> bool {
        matches!(self.phase(), Phase::Stopped)
    }

    /// Reports that this Channel left the Submission loop: it is either
    /// stopped, or quiesced with nothing admitted left to hand over.
    fn handed_over(&self) -> bool {
        let state = self.state.lock().expect("Source state must not panic");
        match state.phase {
            Phase::Open => false,
            Phase::Quiescing => state.encoders == 0 && state.preflight.is_empty(),
            Phase::Stopped => true,
        }
    }

    /// Returns the next admitted submission in this Channel's enqueue order.
    fn next_submission(&self) -> Option<Submission> {
        self.state
            .lock()
            .expect("Source state must not panic")
            .preflight
            .pop_front()
    }

    fn has_submission(&self) -> bool {
        !self
            .state
            .lock()
            .expect("Source state must not panic")
            .preflight
            .is_empty()
    }

    /// Registers the visible result before the frame can be committed, so a
    /// Completion that returns before this loop finishes looking it up is kept.
    fn register(&self, record_id: u64, request: Request) {
        self.state
            .lock()
            .expect("Source state must not panic")
            .pending
            .insert(record_id, request);
    }

    fn take_completion(&self, record_id: u64) -> Option<Request> {
        self.state
            .lock()
            .expect("Source state must not panic")
            .pending
            .remove(&record_id)
    }

    fn phase(&self) -> Phase {
        self.state
            .lock()
            .expect("Source state must not panic")
            .phase
    }
}

struct Encoding<'a>(&'a Shared);

impl Drop for Encoding<'_> {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("Source encoder cleanup must not panic")
            .encoders -= 1;
        self.0.changed.notify_all();
    }
}

#[derive(Debug)]
enum PumpEvent {
    Exited { submission: bool },
    Failed(SendError),
}

struct SessionState {
    channels: Vec<Shared>,
    submission_bell: Arc<LoopBell>,
    failed: FailureBoundary,
    progress: mpsc::Sender<PumpEvent>,
    failure: Mutex<Option<SendError>>,
}

impl fmt::Debug for SessionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionState")
            .finish_non_exhaustive()
    }
}

impl SessionState {
    /// Admits one attempt on `channel` and wakes the loop that writes it.
    ///
    /// The one Submission loop parks with nothing left to write, so every
    /// admitting thread rings the doorbell it waits on. A ring only asks the
    /// loop to re-read its conditions, a lost race is caught by the re-check it
    /// performs after arming, and a needless ring is a spurious wake.
    fn send(
        &self,
        channel: usize,
        encode: impl FnOnce(usize) -> Result<Bytes, ()>,
    ) -> Result<Completion, InvalidChannel> {
        let shared = self.channels.get(channel).ok_or(InvalidChannel)?;
        let completion = shared.send(encode);
        if let Err(error) = self.submission_bell.ring() {
            self.failed(error.into());
        }
        Ok(completion)
    }

    fn check_failure(&self) -> Result<(), SendError> {
        match &*self
            .failure
            .lock()
            .expect("Source failure lock must not panic")
        {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    fn stop_all(&self) -> Result<(), SendError> {
        for channel in &self.channels {
            self.check_failure()?;
            channel.stop();
        }
        Ok(())
    }

    /// Reports that every Channel left the Source session.
    fn stopped(&self) -> bool {
        self.channels.iter().all(Shared::stopped)
    }

    fn failed(&self, error: io::Error) {
        let error = SendError::failed(error);
        {
            let mut failure = self
                .failure
                .lock()
                .expect("Source failure lock must not panic");
            if failure.is_some() {
                return;
            }
            *failure = Some(error.clone());
        }
        // Report immediately. Neither another channel's wake nor cleanup can
        // delay the Program from terminating at the first failure.
        let _ = self.progress.send(PumpEvent::Failed(error.clone()));
        (self.failed)(Box::new(error));
    }
}

fn join(handle: &mut Option<JoinHandle<()>>) {
    if let Some(handle) = handle.take() {
        handle.join().expect("Source pump must not panic")
    }
}

/// What the Submission loop does with one Channel in one round.
///
/// The loop body and the doorbell condition ask the same plan, so a Channel that
/// frees Queue space, admits a record, or reaches hand-over can never leave the
/// loop parked with work owed.
enum Plan {
    /// Write the record this loop holds, or pick up the Channel's next admitted one.
    Write,
    /// The Queue is full, or this Channel has not admitted a record yet.
    Wait,
    /// Nothing is owed: the Channel is stopped, or quiesced with nothing left.
    Done,
}

fn plan(writer: &Writer, channel: &Shared, held: Option<&IngressRecord>) -> io::Result<Plan> {
    if channel.stopped() {
        return Ok(Plan::Done);
    }
    if held.is_none() && channel.handed_over() {
        return Ok(Plan::Done);
    }
    match held {
        // A held record is the one this loop owns until the Queue takes it whole.
        Some(record) if writer.can_write(record.encoded_len())? => Ok(Plan::Write),
        Some(_) => Ok(Plan::Wait),
        None if channel.has_submission() => Ok(Plan::Write),
        None => Ok(Plan::Wait),
    }
}

/// Advances one Channel by committing or rejecting its next owed record.
///
/// The record is registered before the frame can be committed, so a Completion
/// that returns before this loop looks it up is not lost.
fn write(
    writer: &mut Writer,
    channel: &Shared,
    held: &mut Option<IngressRecord>,
) -> io::Result<bool> {
    if held.is_none() {
        let record_id = writer.committed_position()?.get() + 1;
        let Some(submission) = channel.next_submission() else {
            return Ok(false);
        };
        let record = IngressRecord {
            record_id,
            payload: submission.payload,
        };
        if record.encoded_len() > writer.max_payload_size().get() as usize {
            submission.request.complete(Ok(AckCode::Error));
            return Ok(true);
        }
        // Register before publication so a fast Completion cannot be lost.
        channel.register(record_id, submission.request);
        *held = Some(record);
    }
    let record = held.as_ref().expect("a record is held before writing");
    let committed = matches!(
        writer.try_write_with(
            record.encoded_len(),
            |destination| { record.encode(destination).map_err(io::Error::other) },
            || {}
        )?,
        WriteOutcome::Committed(_)
    );
    if committed {
        *held = None;
    }
    Ok(committed)
}

/// Writes every Channel's Submission Queue from the one loop that owns them.
///
/// Each Channel keeps its own enqueue order, and at most one admitted record per
/// Channel waits here for space, so a full Queue blocks nothing but its own
/// Channel. Channels are served in a fixed order and a Channel that always has
/// space is drained first in every round, so fairness across Channels is not
/// promised.
fn submissions(
    mut writers: Vec<Writer>,
    shared: &SessionState,
    bell: Arc<LoopBell>,
) -> io::Result<()> {
    // One owned record per Channel waits for space without a second encoding buffer.
    let mut held: Vec<Option<IngressRecord>> = (0..writers.len()).map(|_| None).collect();
    loop {
        let mut waiting = false;
        let mut progressed = false;
        for index in 0..writers.len() {
            let channel = &shared.channels[index];
            match plan(&writers[index], channel, held[index].as_ref())? {
                // Stopping drops what this loop still holds: close resolves the
                // registered requests, and a frame the Pipeline never received
                // must not count as delivered. A handed-over Channel holds none.
                Plan::Done => held[index] = None,
                Plan::Wait => waiting = true,
                Plan::Write => {
                    if write(&mut writers[index], channel, &mut held[index])? {
                        progressed = true;
                    } else {
                        // The plan promised a write the Queue would not take, so
                        // this Channel now waits for its peer to free space.
                        waiting = true;
                    }
                }
            }
        }
        if !waiting && !progressed {
            return Ok(());
        }
        if !progressed {
            // Park on the one doorbell every Submission Queue's peer rings, and
            // re-read every Channel after every wake.
            let _ = bell.wait_until::<io::Error>(|| {
                let mut waiting = false;
                for index in 0..writers.len() {
                    match plan(
                        &writers[index],
                        &shared.channels[index],
                        held[index].as_ref(),
                    )? {
                        Plan::Done => {}
                        Plan::Write => return Ok(true),
                        Plan::Wait => waiting = true,
                    }
                }
                // Nothing to write, so the loop leaves its last Channel handed over.
                Ok(!waiting)
            })?;
        }
    }
}

/// Reads every Channel's Completion Queue from the one loop that owns them.
///
/// Each Channel's results are read and resolved in that Channel's own completion
/// order; results of different Channels may interleave.
fn completions(
    mut readers: Vec<Reader>,
    shared: &SessionState,
    bell: Arc<LoopBell>,
) -> io::Result<()> {
    loop {
        if shared.stopped() {
            return Ok(());
        }
        let mut progressed = false;
        for (index, reader) in readers.iter_mut().enumerate() {
            while let ReadOutcome::Record(record) = reader.try_read()? {
                let completion =
                    IngressCompletion::decode(record.payload()).map_err(io::Error::other)?;
                let ack = match completion.status {
                    1 => AckCode::Ok,
                    2 => AckCode::Retry,
                    3 => AckCode::Backpressure,
                    4 => AckCode::Error,
                    _ => return Err(invalid("Invalid Ingress completion status")),
                };
                // Even if Shutdown arrives after release, an already-read result
                // keeps its original status. close joins this loop before
                // failing the requests that are left.
                reader.release(1)?;
                if let Some(request) = shared.channels[index].take_completion(completion.record_id)
                {
                    request.complete(Ok(ack))
                }
                progressed = true;
            }
        }
        if progressed {
            continue;
        }
        // Park on the one doorbell every Completion Queue's peer rings, and
        // re-read every Channel after every wake.
        let _ = bell.wait_until::<io::Error>(|| {
            for reader in &readers {
                if reader.has_unread()? {
                    return Ok(true);
                }
            }
            Ok(false)
        })?;
    }
}

fn discover(directory: &Path) -> io::Result<usize> {
    let mut submissions = BTreeSet::new();
    let mut completions = BTreeSet::new();
    for entry in std::fs::read_dir(directory)? {
        let name = entry?.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| invalid("Source Queue file name is not UTF-8"))?;
        if name == LOOPS_BELL_FILE_NAME {
            continue;
        }
        let (indices, index) = if let Some(index) = name.strip_prefix("submission-") {
            (&mut submissions, index)
        } else if let Some(index) = name.strip_prefix("completion-") {
            (&mut completions, index)
        } else {
            return Err(invalid("Unexpected file in Source directory"));
        };
        let index = index
            .strip_suffix(".queue")
            .ok_or_else(|| invalid("Invalid Source Queue file name"))?;
        let number = index
            .parse::<u32>()
            .map_err(|_| invalid("Invalid Source channel index"))?;
        if index != number.to_string() {
            return Err(invalid("Noncanonical Source channel index"));
        }
        indices.insert(number);
    }
    if submissions.is_empty()
        || submissions != completions
        || submissions.iter().copied().ne(0..submissions.len() as u32)
    {
        return Err(invalid(
            "Source Queues must form continuous pairs from zero",
        ));
    }
    Ok(submissions.len())
}

fn pair_limit(writer: &Writer, reader: &Reader) -> io::Result<usize> {
    let frame =
        tenon_ipc::queue::maximum_frame_len(writer.max_payload_size()).map_err(io::Error::other)?;
    let completion_frame =
        tenon_ipc::queue::maximum_frame_len(reader.max_payload_size()).map_err(io::Error::other)?;
    let frames = (writer.data_capacity().get() as usize) / frame;
    if !(writer.data_capacity().get() as usize).is_multiple_of(frame)
        || frames < 2
        || frames - 1 > i32::MAX as usize
        || reader.max_payload_size().get() as usize != 13
        || !(reader.data_capacity().get() as usize).is_multiple_of(completion_frame)
        || (reader.data_capacity().get() as usize) / completion_frame != frames
    {
        return Err(invalid("Invalid Source Queue record-limit layout"));
    }
    Ok(frames - 1)
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests;
