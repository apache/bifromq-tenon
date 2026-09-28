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

//! The park protocol one waiting loop runs over its own doorbell.
//!
//! A doorbell carries no cause: a ring only says that something the loop
//! subscribed to may have changed. Every wait therefore runs the same three
//! steps — arm the slot, recheck the complete condition set, then sleep — and
//! every exit path returns the slot to notified, so a ring that arrives after
//! the decision is never swallowed.
//!
//! This module writes that protocol exactly once over [`WaitFront`], so the
//! mapped doorbell and the Loom model cannot drift apart.

use std::convert::Infallible;
use std::time::{Duration, Instant};

use super::BellError;

/// The normal result of one indefinite doorbell wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum WaitOutcome {
    /// The requested condition is currently true.
    Ready,
    /// A process-local request or platform signal interrupted the wait.
    Interrupted,
}

/// The normal result of one doorbell wait with an explicit local timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum TimedWaitOutcome {
    /// The requested condition is currently true.
    Ready,
    /// A process-local request or platform signal interrupted the wait.
    Interrupted,
    /// The timeout elapsed without changing the observed facts.
    TimedOut,
}

/// The parking primitives of exactly one waiting loop's doorbell.
///
/// [`wait_indefinitely`] and [`wait_for`] write the arm, recheck, and sleep
/// protocol once over this trait. A doorbell is owned by the loop that parks on
/// it; every peer rings it.
pub(crate) trait WaitFront {
    /// Consumes a process-local interrupt requested for this loop.
    fn take_wait_interruption(&self) -> Option<WaitInterruption>;
    /// Clears and arms this loop's slot, or reports a pending local interrupt.
    fn arm_or_take_wait_interruption(&self) -> Result<WaitArmOutcome, BellError>;
    /// Returns this loop's slot to notified without calling the platform.
    fn publish_wait_notification(&self) -> Result<(), BellError>;
    fn wait_bell(&self) -> Result<SignalWaitOutcome, BellError>;
    fn wait_bell_for(&self, timeout: Duration) -> Result<SignalWaitOutcome, BellError>;
    /// Reports one platform wake of this loop that found no subscribed fact.
    ///
    /// A doorbell carries no cause, so a peer may ring for a fact this loop does
    /// not cover, and an unconditional recovery wake publishes nothing at all.
    /// The loop then re-arms and sleeps again. That costs one whole recheck, so
    /// the owning loop can count it instead of hiding it. A front that owns no
    /// such counter accepts the wake silently.
    fn observe_spurious_wake(&self) {}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WaitArmOutcome {
    Armed,
    Interrupted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WaitInterruption;

pub(crate) fn wait_indefinitely<F: WaitFront + ?Sized, E: From<BellError>>(
    front: &F,
    condition: impl FnMut() -> Result<bool, E>,
) -> Result<WaitOutcome, E> {
    match wait_with_next_platform_wait(front, condition, || {
        Ok::<PlatformWait, Infallible>(PlatformWait::Indefinite)
    })? {
        WaitLoopOutcome::Ready => Ok(WaitOutcome::Ready),
        WaitLoopOutcome::Interrupted => Ok(WaitOutcome::Interrupted),
        WaitLoopOutcome::Expired(infallible) => match infallible {},
    }
}

pub(crate) fn wait_for<F: WaitFront + ?Sized, E: From<BellError>>(
    front: &F,
    timeout: Duration,
    condition: impl FnMut() -> Result<bool, E>,
) -> Result<TimedWaitOutcome, E> {
    let started_at = Instant::now();
    let next_platform_wait = || {
        timeout
            .checked_sub(started_at.elapsed())
            .filter(|remaining| !remaining.is_zero())
            .map(PlatformWait::Timed)
            .ok_or(())
    };
    match wait_with_next_platform_wait(front, condition, next_platform_wait)? {
        WaitLoopOutcome::Ready => Ok(TimedWaitOutcome::Ready),
        WaitLoopOutcome::Interrupted => Ok(TimedWaitOutcome::Interrupted),
        WaitLoopOutcome::Expired(()) => Ok(TimedWaitOutcome::TimedOut),
    }
}

/// Runs the arm, recheck, and sleep protocol over one loop doorbell and one
/// condition set.
///
/// `condition` must cover every fact the loop subscribed to, because a ring
/// carries no cause: it only says that one of them may have changed. Every
/// exit path returns the slot to notified, so a ring that arrives after the
/// decision is not swallowed.
pub(crate) fn wait_with_next_platform_wait<F: WaitFront + ?Sized, ConditionError, ExpiredError>(
    front: &F,
    mut condition: impl FnMut() -> Result<bool, ConditionError>,
    mut next_platform_wait: impl FnMut() -> Result<PlatformWait, ExpiredError>,
) -> Result<WaitLoopOutcome<ExpiredError>, ConditionError>
where
    ConditionError: From<BellError>,
{
    // Whether the previous platform wait ended without a subscribed fact, so
    // the recheck below is the whole cost of that wake.
    let mut woke_from_platform = false;
    loop {
        if front.take_wait_interruption().is_some() {
            return Ok(WaitLoopOutcome::Interrupted);
        }
        if condition()? {
            return Ok(WaitLoopOutcome::Ready);
        }
        if std::mem::take(&mut woke_from_platform) {
            front.observe_spurious_wake();
        }

        match front
            .arm_or_take_wait_interruption()
            .map_err(ConditionError::from)?
        {
            WaitArmOutcome::Armed => {}
            WaitArmOutcome::Interrupted => return Ok(WaitLoopOutcome::Interrupted),
        }
        match condition() {
            Ok(true) => {
                front
                    .publish_wait_notification()
                    .map_err(ConditionError::from)?;
                return Ok(WaitLoopOutcome::Ready);
            }
            Ok(false) => {}
            Err(error) => {
                front
                    .publish_wait_notification()
                    .map_err(ConditionError::from)?;
                return Err(error);
            }
        }

        let platform_wait = match next_platform_wait() {
            Ok(platform_wait) => platform_wait,
            Err(expired) => {
                front
                    .publish_wait_notification()
                    .map_err(ConditionError::from)?;
                return Ok(WaitLoopOutcome::Expired(expired));
            }
        };
        let wait = match platform_wait {
            PlatformWait::Indefinite => front.wait_bell(),
            PlatformWait::Timed(timeout) => front.wait_bell_for(timeout),
        };
        match wait {
            Ok(SignalWaitOutcome::Progress) => woke_from_platform = true,
            Ok(SignalWaitOutcome::Interrupted) => {
                let _cleared_interruption = front.take_wait_interruption();
                front
                    .publish_wait_notification()
                    .map_err(ConditionError::from)?;
                return Ok(WaitLoopOutcome::Interrupted);
            }
            Ok(SignalWaitOutcome::TimedOut) => {
                front
                    .publish_wait_notification()
                    .map_err(ConditionError::from)?;
                if front.take_wait_interruption().is_some() {
                    return Ok(WaitLoopOutcome::Interrupted);
                }
                if let Err(expired) = next_platform_wait() {
                    return Ok(WaitLoopOutcome::Expired(expired));
                }
                // A platform timeout that still has time left means the kernel
                // returned early, so this recheck is also a wake with no fact.
                woke_from_platform = true;
            }
            Err(error) => {
                front
                    .publish_wait_notification()
                    .map_err(ConditionError::from)?;
                return Err(ConditionError::from(error));
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WaitLoopOutcome<E> {
    Ready,
    Interrupted,
    Expired(E),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum PlatformWait {
    Indefinite,
    Timed(Duration),
}

/// The outcome of one operating-system wait on a doorbell slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignalWaitOutcome {
    Progress,
    Interrupted,
    TimedOut,
}
