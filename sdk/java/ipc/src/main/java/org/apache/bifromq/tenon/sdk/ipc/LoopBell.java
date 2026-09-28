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

package org.apache.bifromq.tenon.sdk.ipc;

import java.io.IOException;
import java.util.Objects;

/** One waiting loop's doorbell: the slot it parks on and its process-local hint. */
public final class LoopBell {
  /** The normal result of one wait on this loop's doorbell. */
  public enum WaitOutcome {
    /** A peer rang the slot or the condition already held. */
    READY,
    /** A local interrupt was already pending, or the platform interrupted the park. */
    INTERRUPTED
  }

  /** One condition this loop waits for; the doorbell only says that something may have changed. */
  @FunctionalInterface
  public interface Condition {
    boolean ready() throws IOException;
  }

  private final BellSlot slot;
  private final Object pendingLock = new Object();
  private boolean pending;

  LoopBell(BellSlot slot) {
    this.slot = Objects.requireNonNull(slot, "slot");
  }

  /** Returns the slot this loop publishes so its peers can ring it. */
  BellSlot slot() {
    return slot;
  }

  /** Returns a process-local handle that interrupts this loop's wait. */
  public BellInterrupter interrupter() {
    return new BellInterrupter(this);
  }

  /**
   * Rings this loop's own doorbell without interrupting it.
   *
   * <p>An admitting thread, or a peer that freed Queue space, only asks the loop to re-read the
   * conditions it subscribed to. A ring never hands control back by itself: the loop wakes at most
   * once and parks again while none of its conditions holds.
   *
   * @throws IOException when the platform cannot wake an already blocked owner; the caller must
   *     treat that loop as failed
   */
  public void ring() throws IOException {
    slot.ring();
  }

  /**
   * Parks until {@code ready} holds, the wait is interrupted, or the platform fails.
   *
   * <p>One waiting loop owns this doorbell: it parks here, and {@link #interrupter()} wakes that
   * same wait from inside the process.
   *
   * <p>Every condition the loop subscribed to must be part of {@code ready}; the doorbell only says
   * that something may have changed. The slot is armed under the same lock that clears a pending
   * interrupt, so an interrupt either precedes the arm or wakes the wait that arm registered.
   *
   * <p>A bare ring never hands control back by itself: it wakes the wait at most once and the loop
   * parks again while {@code ready} still does not hold. Only a local interrupt or a platform-level
   * interruption returns {@link WaitOutcome#INTERRUPTED}.
   */
  public WaitOutcome until(Condition ready) throws IOException {
    while (true) {
      synchronized (pendingLock) {
        if (pending) {
          pending = false;
          return WaitOutcome.INTERRUPTED;
        }
      }
      if (ready.ready()) {
        return WaitOutcome.READY;
      }
      synchronized (pendingLock) {
        if (pending) {
          pending = false;
          return WaitOutcome.INTERRUPTED;
        }
        slot.arm();
      }
      if (ready.ready()) {
        slot.publishNotification();
        continue;
      }
      if (slot.park() == BellWaitOutcome.INTERRUPTED) {
        slot.publishNotification();
        return WaitOutcome.INTERRUPTED;
      }
    }
  }

  /** Sets the local hint, then rings this loop's own doorbell. */
  void interrupt() throws IOException {
    synchronized (pendingLock) {
      pending = true;
      slot.ring();
    }
  }
}
