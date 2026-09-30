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

//! Queue layout and Pipeline-side adapter for one Source Submission and Ingress Completion pair.
//!
//! Pipeline creation uses the public functions to size the two Queue files. A
//! Source SDK uses the same rules to recover `maxPendingRecords` and
//! reject a mismatched pair while opening its working directory. The private
//! runtime owner receives Submission records and writes Completion results.
//! Generic Queue bytes, framing, append planning, waits, and payload limits
//! remain in [`tenon_ipc::queue`].

use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;

use prost::Message;

use crate::pipeline::channel::metrics::{ChannelMetrics, WaitMeasurement};
use tenon_ipc::queue::QueueObserver;

use crate::contracts::source::{IngressCompletion, IngressCompletionStatus, IngressRecord};
use tenon_ipc::bell::{BellRegion, LoopBell, WaitOutcome};
use tenon_ipc::queue::{
    DataCapacity, FormatError, QueueReader, QueueRuntimeError, QueueWriter, ReadOutcome,
    WriteOutcome, capacity_for_record_limit, const_aligned_frame_len, maximum_frame_len,
};

/// Maximum encoded `IngressCompletion` payload size for the fixed v1 schema.
pub const COMPLETION_MAX_PAYLOAD_SIZE: usize = 13;

const COMPLETION_MAX_FRAME_LEN: usize = const_aligned_frame_len(COMPLETION_MAX_PAYLOAD_SIZE);

/// Returns the Submission data capacity for one Source channel.
///
/// The capacity holds every admitted maximum-size record plus one additional
/// maximum-size frame of space for a physical wrap.
///
/// # Errors
///
/// Returns [`FormatError`] when `max_record_size_bytes` exceeds the Queue
/// protocol limit or the capacity calculation overflows.
pub fn submission_capacity(
    max_pending_records: NonZeroU64,
    max_record_size_bytes: NonZeroU64,
) -> Result<DataCapacity, FormatError> {
    capacity_for_record_limit(
        max_pending_records,
        maximum_frame_len(max_record_size_bytes)?,
    )
}

/// Returns the Completion data capacity paired with one Source channel.
///
/// # Errors
///
/// Returns [`FormatError::InvalidCapacity`] when the capacity calculation
/// overflows.
pub fn completion_capacity(max_pending_records: NonZeroU64) -> Result<DataCapacity, FormatError> {
    capacity_for_record_limit(max_pending_records, COMPLETION_MAX_FRAME_LEN)
}

/// A stable failure while opening or consuming one Pipeline-side Ingress Queue pair.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum IngressQueueError {
    /// The Submission Queue failed.
    SubmissionQueue(QueueRuntimeError),
    /// The Completion Queue failed.
    CompletionQueue(QueueRuntimeError),
    /// The two Queue files do not describe one compatible Source channel.
    PairLayout(FormatError),
    /// A committed Submission frame is not a valid `IngressRecord`.
    IngressRecordDecode(prost::DecodeError),
    /// The generated Protobuf sentinel is not a deliverable completion result.
    InvalidCompletionStatus,
}

impl fmt::Display for IngressQueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SubmissionQueue(source) => write!(formatter, "Submission Queue failed: {source}"),
            Self::CompletionQueue(source) => write!(formatter, "Completion Queue failed: {source}"),
            Self::PairLayout(source) => {
                write!(formatter, "Ingress Queue pair is invalid: {source}")
            }
            Self::IngressRecordDecode(source) => {
                write!(formatter, "IngressRecord decoding failed: {source}")
            }
            Self::InvalidCompletionStatus => {
                formatter.write_str("Ingress completion status is unspecified")
            }
        }
    }
}

impl Error for IngressQueueError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SubmissionQueue(source) | Self::CompletionQueue(source) => Some(source),
            Self::PairLayout(source) => Some(source),
            Self::IngressRecordDecode(source) => Some(source),
            Self::InvalidCompletionStatus => None,
        }
    }
}

/// The normal result of writing one terminal Ingress completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) enum IngressCompletionWriteOutcome {
    /// The complete result is visible to the Source SDK.
    Committed,
    /// A process-local lifecycle request interrupted the capacity wait.
    Interrupted,
}

/// The Pipeline-side owner of one Source Submission/Completion Queue pair.
#[derive(Debug)]
pub(crate) struct IngressQueuePair {
    submission: QueueReader,
    completion: QueueWriter,
}

impl IngressQueuePair {
    /// Opens and cross-validates one Submission reader and Completion writer.
    ///
    /// `bell` is the doorbell of the Channel loop that waits on this Queue pair;
    /// both endpoints publish that loop's slot so the Source can ring it. The
    /// Source Instance owns `source_region`, the Bell Region whose slots its own
    /// loops park on: committing or releasing here rings the Source, not a Queue.
    ///
    /// # Errors
    ///
    /// Returns [`IngressQueueError`] when either Queue cannot be opened or the
    /// two layouts do not encode the same `maxPendingRecords` value.
    pub(crate) fn open(
        submission_path: impl AsRef<Path>,
        completion_path: impl AsRef<Path>,
        bell: &Arc<LoopBell>,
        source_region: &Arc<BellRegion>,
    ) -> Result<Self, IngressQueueError> {
        let submission =
            QueueReader::open(submission_path, Arc::clone(bell), Arc::clone(source_region))
                .map_err(IngressQueueError::SubmissionQueue)?;
        let completion =
            QueueWriter::open(completion_path, Arc::clone(bell), Arc::clone(source_region))
                .map_err(IngressQueueError::CompletionQueue)?;
        validate_pair_layout(
            submission.max_payload_size(),
            submission.data_capacity(),
            completion.max_payload_size(),
            completion.data_capacity(),
        )
        .map_err(IngressQueueError::PairLayout)?;
        Ok(Self {
            submission,
            completion,
        })
    }

    /// Reports whether a committed Submission record is waiting for this Channel.
    ///
    /// Both this probe and the blocked receive read the same two live positions,
    /// so an event loop can fold "Submission is readable" into its own condition
    /// set without a second Queue word. The probe never reads a record.
    ///
    /// # Errors
    ///
    /// Returns [`QueueRuntimeError`] when live Submission state is invalid.
    pub(crate) fn readable(&self) -> Result<bool, QueueRuntimeError> {
        self.submission.has_unread()
    }

    /// Receives one already committed record without waiting.
    ///
    /// `None` proves that the Submission Queue was empty at this read point.
    /// A Source handoff uses that fact only after its sole writer has quiesced
    /// or stopped, so no later old-session record can appear in this Queue.
    pub(crate) fn try_receive(
        &mut self,
        metrics: &ChannelMetrics,
    ) -> Result<Option<IngressRecord>, IngressQueueError> {
        let outcome = self
            .submission
            .try_read()
            .map_err(IngressQueueError::SubmissionQueue)?;
        let ReadOutcome::Record(record) = outcome else {
            return Ok(None);
        };
        let record = IngressRecord::decode(record.into_payload_bytes())
            .map_err(IngressQueueError::IngressRecordDecode)?;
        metrics.input(record.payload.len());
        self.submission
            .release(1)
            .map_err(IngressQueueError::SubmissionQueue)?;
        Ok(Some(record))
    }

    /// Writes one terminal result, waiting when the Completion Queue is full.
    ///
    /// Capacity pressure blocks this channel and therefore prevents it from
    /// consuming another Submission record. An interruption returns control to
    /// the lifecycle owner without changing an uncommitted completion.
    ///
    /// # Errors
    ///
    /// Returns [`IngressQueueError`] for an unspecified status or terminal
    /// Completion Queue failure.
    pub(crate) fn complete(
        &mut self,
        record_id: u64,
        status: IngressCompletionStatus,
        metrics: &ChannelMetrics,
        wait: &mut WaitMeasurement<'_>,
    ) -> Result<IngressCompletionWriteOutcome, IngressQueueError> {
        if status == IngressCompletionStatus::Unspecified {
            return Err(IngressQueueError::InvalidCompletionStatus);
        }
        let result = match status {
            IngressCompletionStatus::Ok => "ok",
            IngressCompletionStatus::Retry => "retry",
            IngressCompletionStatus::Error => "error",
            IngressCompletionStatus::Unspecified => {
                unreachable!("completion status was checked above")
            }
            IngressCompletionStatus::Backpressure => unreachable!(
                "admission backpressure is local to the Source SDK, never a Pipeline completion"
            ),
        };
        let completion = IngressCompletion {
            record_id,
            status: status as i32,
        };
        let encoded_len = completion.encoded_len();
        loop {
            let outcome = self
                .completion
                .try_write_with(
                    encoded_len,
                    |destination| {
                        completion
                            .encode(destination)
                            .map_err(std::io::Error::other)
                    },
                    || {
                        wait.ready();
                        metrics.completion(result);
                    },
                )
                .map_err(IngressQueueError::CompletionQueue)?;
            match outcome {
                WriteOutcome::Committed(_) => {
                    return Ok(IngressCompletionWriteOutcome::Committed);
                }
                WriteOutcome::Full => {
                    wait.blocked();
                    match self.completion.wait_writable(encoded_len) {
                        Err(source) => {
                            return Err(IngressQueueError::CompletionQueue(source));
                        }
                        Ok(WaitOutcome::Ready) => wait.ready(),
                        Ok(WaitOutcome::Interrupted) => {
                            return Ok(IngressCompletionWriteOutcome::Interrupted);
                        }
                    }
                }
            }
        }
    }

    /// Waits for the Source reader to release every committed Completion.
    /// The sole writer must not append between interrupted retries. Queue
    /// consumption does not prove that external business callbacks have finished.
    /// Any Queue error ends the owning Channel, as with Completion writes.
    pub(crate) fn wait_completions_consumed(
        &mut self,
        wait: &mut WaitMeasurement<'_>,
    ) -> Result<WaitOutcome, IngressQueueError> {
        self.completion
            .wait_all_released_observed(|| wait.blocked())
            .map_err(IngressQueueError::CompletionQueue)
    }

    pub(crate) fn observers(&self) -> [(&'static str, QueueObserver); 2] {
        [
            ("submission", self.submission.observer()),
            ("completion", self.completion.observer()),
        ]
    }
}

fn validate_pair_layout(
    submission_max_payload_size: NonZeroU64,
    submission_capacity: DataCapacity,
    completion_max_payload_size: NonZeroU64,
    completion_data_capacity: DataCapacity,
) -> Result<(), FormatError> {
    let max_pending_records =
        max_pending_records_for_layout(submission_max_payload_size, submission_capacity)?;
    let completion_max_payload_size_expected =
        u64::try_from(COMPLETION_MAX_PAYLOAD_SIZE).map_err(|_| FormatError::InvalidCapacity)?;
    if completion_max_payload_size.get() != completion_max_payload_size_expected
        || completion_capacity(max_pending_records)? != completion_data_capacity
    {
        return Err(FormatError::InvalidCapacity);
    }
    Ok(())
}

fn max_pending_records_for_layout(
    submission_max_payload_size: NonZeroU64,
    submission_capacity: DataCapacity,
) -> Result<NonZeroU64, FormatError> {
    let maximum_frame_len = u64::try_from(maximum_frame_len(submission_max_payload_size)?)
        .map_err(|_| FormatError::InvalidCapacity)?;
    if !submission_capacity.get().is_multiple_of(maximum_frame_len) {
        return Err(FormatError::InvalidCapacity);
    }
    submission_capacity
        .get()
        .checked_div(maximum_frame_len)
        .and_then(|frame_count| frame_count.checked_sub(1))
        .and_then(NonZeroU64::new)
        .ok_or(FormatError::InvalidCapacity)
}

#[cfg(any(test, feature = "repository-test-support"))]
pub(crate) mod contract_test_support {
    use super::{max_pending_records_for_layout, validate_pair_layout};
    use std::num::NonZeroU64;
    use tenon_ipc::queue::{DataCapacity, FormatError, Header};

    /// Recovers `maxPendingRecords` from a Submission Queue.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidCapacity`] when the actual data capacity is
    /// not an exact positive record limit plus one maximum-frame wrap reserve.
    pub fn max_pending_records(
        submission: Header,
        submission_capacity: DataCapacity,
    ) -> Result<NonZeroU64, FormatError> {
        max_pending_records_for_layout(submission.max_payload_size(), submission_capacity)
    }

    /// Validates that one Submission/Completion pair encodes the same pending limit.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::InvalidCapacity`] when Completion does not use the
    /// fixed payload size or the two file capacities encode different limits.
    pub fn validate_pair(
        submission: Header,
        submission_capacity: DataCapacity,
        completion: Header,
        completion_data_capacity: DataCapacity,
    ) -> Result<(), FormatError> {
        validate_pair_layout(
            submission.max_payload_size(),
            submission_capacity,
            completion.max_payload_size(),
            completion_data_capacity,
        )
    }
}

#[cfg(test)]
mod tests;
