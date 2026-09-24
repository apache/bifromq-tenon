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

//! The single Egress loop of one Sink Instance and its completion ordering.
//! Business work remains owned by the Plugin; the loop only observes its
//! asynchronous results, and it serves every one of the Instance's Egress
//! Queues in turn instead of giving each Queue its own thread.

#![expect(
    clippy::expect_used,
    reason = "Egress loop locks, batches and joins are SDK-owned invariants"
)]

use super::{FlowChannel, SINK_DIRECTORY_NAME, SinkInput};
use crate::LOOPS_BELL_FILE_NAME;
use crate::{Error, wire};
use futures_util::{Stream, stream::FuturesUnordered};
use prost::Message;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::task::{Context, Poll, Wake};
use std::thread::{self, JoinHandle};
use tenon_ipc::bell::{BellInterrupter, BellRegion, LoopBell};
use tenon_ipc::queue::ReadOutcome;
use tenon_ipc::queue::{OwnedRecord as Record, QueueReader as Reader, ReadPosition};

/// The prepared endpoints, before a business object has entered start ownership.
///
/// One Sink Instance reads every one of its Egress Queues from one waiting loop,
/// and a doorbell slot belongs to the loop that parks on it, not to a Queue. The
/// Runner therefore gives the Sink side exactly one slot, `0`, and every Egress
/// Queue reader publishes that same ordinal.
pub(crate) struct Queues {
    bell: Arc<LoopBell>,
    queues: Vec<(FlowChannel, Reader)>,
}

impl Queues {
    pub(crate) fn open(working_directory: &Path, channels: Vec<SinkInput>) -> Result<Self, Error> {
        let loops = BellRegion::open(
            &working_directory
                .join(SINK_DIRECTORY_NAME)
                .join(LOOPS_BELL_FILE_NAME),
        )?;
        let bell = loops.loop_bell(0)?;
        let queues = channels
            .into_iter()
            .map(|input| {
                let peer = BellRegion::open(&input.channel_bell_path)?;
                let channel = input.channel;
                let reader = Reader::open(
                    channel.queue_path(working_directory),
                    Arc::clone(&bell),
                    peer,
                )?;
                Ok((channel, reader))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(Self { bell, queues })
    }
}

/// Owns the Instance's Egress loop, stopping it before joining it.
pub(crate) struct Session {
    worker: Worker,
    progress: mpsc::Receiver<Result<(), Error>>,
}

impl Session {
    pub(crate) fn start<P, S>(
        queues: Queues,
        sink: Arc<S>,
        failed: impl Fn(Error) + Send + Sync + 'static,
    ) -> Result<Self, Error>
    where
        P: Message + Default,
        S: BatchWriter<P>,
    {
        let (progress, received) = mpsc::channel();
        let errors = progress.clone();
        let report: Arc<dyn Fn(Error) + Send + Sync> = Arc::new(move |error| {
            let error: Arc<dyn std::error::Error + Send + Sync> = error.into();
            let _ = errors.send(Err(Box::new(error.clone()) as Error));
            failed(Box::new(error));
        });
        let admission = Arc::new(Admission {
            phase: Mutex::new(Phase::Paused),
            activated: Condvar::new(),
            wake: queues.bell.interrupter(),
            failed: report,
        });
        let shared = admission.clone();
        let completed = progress.clone();
        let thread = thread::Builder::new()
            .name("tenon-sink-egress".into())
            .spawn(move || {
                shared.await_activation();
                if let Err(error) = coordinate(queues, sink.as_ref(), &shared) {
                    (shared.failed)(error);
                }
                drop(sink);
                drop(shared);
                let _ = completed.send(Ok(()));
            })?;
        Ok(Self {
            worker: Worker {
                admission,
                thread: Some(thread),
            },
            progress: received,
        })
    }

    pub(crate) fn activate(&self) {
        let mut phase = self
            .worker
            .admission
            .phase
            .lock()
            .expect("activation lock must not panic");
        assert!(
            matches!(*phase, Phase::Paused),
            "the Egress loop activates once after Ready"
        );
        *phase = Phase::Running;
        self.worker.admission.activated.notify_one();
    }

    fn join_worker(&mut self) {
        if let Some(thread) = self.worker.thread.take() {
            thread
                .join()
                .expect("an Egress loop panic terminates the process");
        }
    }

    fn check_progress(&mut self) -> Result<(), Error> {
        while let Ok(result) = self.progress.try_recv() {
            result?;
            self.join_worker();
        }
        Ok(())
    }

    pub(crate) fn close(&mut self) -> Result<(), Error> {
        self.check_progress()?;
        if self.worker.thread.is_some() {
            *self
                .worker
                .admission
                .phase
                .lock()
                .expect("stop lock must not panic") = Phase::Stopping;
            self.worker.admission.activated.notify_one();
            // A failed native wake returns before a join. The worker continues
            // owning its reader and mapping until the Program terminates.
            self.worker.admission.wake.interrupt()?;
        }
        while self.worker.thread.is_some() {
            self.progress
                .recv()
                .expect("the Egress loop owns the progress channel")?;
            self.join_worker();
        }
        self.check_progress()
    }
}

/// Queue workers require writes, not ownership of a business lifecycle.
pub(crate) trait BatchWriter<P>: Send + Sync + 'static {
    fn write(
        &self,
        channel: FlowChannel,
        records: Box<[P]>,
    ) -> impl Future<Output = Result<(), Error>>;
}

struct Worker {
    admission: Arc<Admission>,
    thread: Option<JoinHandle<()>>,
}

enum Phase {
    Paused,
    Running,
    Stopping,
}

struct Admission {
    phase: Mutex<Phase>,
    activated: Condvar,
    wake: BellInterrupter,
    failed: Arc<dyn Fn(Error) + Send + Sync>,
}

impl Admission {
    fn await_activation(&self) {
        let mut phase = self.phase.lock().expect("activation lock must not panic");
        while matches!(*phase, Phase::Paused) {
            phase = self
                .activated
                .wait(phase)
                .expect("activation wait must not panic");
        }
    }

    fn accepting(&self) -> bool {
        matches!(
            *self.phase.lock().expect("admission lock must not panic"),
            Phase::Running
        )
    }
}

impl Wake for Admission {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if !self.accepting() {
            return;
        }
        if let Err(error) = self.wake.interrupt() {
            (self.failed)(error.into());
        }
    }
}

enum BatchState {
    Pending,
    Succeeded,
}

/// One Egress Queue as the Instance's single loop sees it.
///
/// Batching, the copied-but-unreleased position and the consecutive-success
/// prefix stay per Queue: only the waiting loop is shared, so one Queue can be
/// mid-batch while another is idle or waiting for a peer.
struct Egress<P> {
    channel: FlowChannel,
    reader: Reader,
    records: Vec<P>,
    pending: BTreeMap<ReadPosition, BatchState>,
}

/// Attributes one Queue's failure to that Queue inside the shared loop.
fn failed(channel: &FlowChannel, cause: Error) -> Error {
    Box::new(QueueFailure {
        channel: channel.clone(),
        cause,
    })
}

fn coordinate<P: Message + Default, S: BatchWriter<P>>(
    queues: Queues,
    sink: &S,
    admission: &Arc<Admission>,
) -> Result<(), Error> {
    let Queues { bell, queues } = queues;
    let mut egress = queues
        .into_iter()
        .map(|(channel, reader)| Egress {
            channel,
            reader,
            records: Vec::new(),
            pending: BTreeMap::new(),
        })
        .collect::<Vec<Egress<P>>>();
    let mut completions = FuturesUnordered::new();
    let waker = admission.clone().into();
    let mut context = Context::from_waker(&waker);
    let result = (|| -> Result<(), Error> {
        loop {
            while admission.accepting() {
                let Poll::Ready(Some(settled)) = Pin::new(&mut completions).poll_next(&mut context)
                else {
                    break;
                };
                let (index, position, result): (usize, ReadPosition, Result<(), Error>) = settled;
                // Polling runs outside the admission lock. Shutdown may have
                // overtaken that observation; late ordinary results do not release.
                if !admission.accepting() {
                    break;
                }
                let queue = &mut egress[index];
                match result {
                    Ok(()) => {
                        *queue
                            .pending
                            .get_mut(&position)
                            .expect("every future has an admitted batch") = BatchState::Succeeded
                    }
                    Err(cause) => return Err(failed(&queue.channel, cause)),
                }
            }
            for queue in &mut egress {
                let mut release = None;
                while let Some(entry) = queue.pending.first_entry() {
                    if !matches!(entry.get(), BatchState::Succeeded) {
                        break;
                    }
                    release = Some(entry.remove_entry().0);
                }
                if let Some(position) = release {
                    let released = queue.reader.release_through(position);
                    released.map_err(|cause| failed(&queue.channel, Error::from(cause)))?;
                }
            }
            if !admission.accepting() {
                return Ok(());
            }
            let mut progressed = false;
            for (index, queue) in egress.iter_mut().enumerate() {
                while admission.accepting() {
                    match queue.reader.try_read() {
                        Ok(ReadOutcome::Record(record)) => {
                            let record =
                                decode(record).map_err(|cause| failed(&queue.channel, cause))?;
                            queue.records.push(record);
                            progressed = true;
                        }
                        Ok(ReadOutcome::Empty) if !queue.records.is_empty() => {
                            // This check is the batch admission point, ordered with stop.
                            // Its write body may start afterward, but cleanup joins it.
                            if !admission.accepting() {
                                return Ok(());
                            }
                            let position = queue.reader.checkpoint()?;
                            queue.pending.insert(position, BatchState::Pending);
                            let future = sink.write(
                                queue.channel.clone(),
                                std::mem::take(&mut queue.records).into_boxed_slice(),
                            );
                            completions.push(async move { (index, position, future.await) });
                            progressed = true;
                        }
                        Ok(ReadOutcome::Empty) => break,
                        Err(cause) if admission.accepting() => {
                            return Err(failed(&queue.channel, Error::from(cause)));
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
            if progressed {
                continue;
            }
            // Every Queue is drained, so the loop parks on the one doorbell all
            // of their peers ring, and rechecks every Queue after every wake.
            // Queues are served in a fixed order and fairness is not promised: a
            // Queue that always has data is drained first in every round.
            let _ = bell.wait_until::<std::io::Error>(|| {
                for queue in &egress {
                    if queue.reader.has_unread()? {
                        return Ok(true);
                    }
                }
                Ok(false)
            })?;
        }
    })();
    if result.is_err() {
        // A terminal error belongs to the Program. Do not run pending business
        // destructors before that boundary reports and exits the process.
        std::mem::forget(completions);
    }
    result
}

fn decode<P: Message + Default>(record: Record) -> Result<P, Error> {
    let record =
        wire::sink::EgressRecord::decode(record.into_payload_bytes()).map_err(|cause| {
            DecodeFailure {
                code: "sink.egress_record_invalid",
                cause,
            }
        })?;
    P::decode(record.payload).map_err(|cause| {
        DecodeFailure {
            code: "sink.payload_invalid",
            cause,
        }
        .into()
    })
}

#[derive(Debug)]
struct DecodeFailure {
    code: &'static str,
    cause: prost::DecodeError,
}
impl std::fmt::Display for DecodeFailure {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str(self.code)
    }
}
impl std::error::Error for DecodeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.cause)
    }
}

#[derive(Debug)]
struct QueueFailure {
    channel: FlowChannel,
    cause: Error,
}
impl std::fmt::Display for QueueFailure {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            output,
            "Sink Queue {} channel {} failed: {}",
            self.channel.flow_id, self.channel.channel_id, self.cause
        )
    }
}
impl std::error::Error for QueueFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod model_tests;
