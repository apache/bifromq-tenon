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

/**
 * One doorbell slot that its owning loop arms and every peer rings.
 *
 * <p>The slot keeps its Region alive, so a handle held by a peer always addresses the same live
 * word.
 */
final class BellSlot {
  private final BellRegion region;
  private final int index;

  BellSlot(BellRegion region, int index) {
    this.region = Objects.requireNonNull(region, "region");
    this.index = index;
  }

  /**
   * Returns this slot's ordinal inside its Bell Region.
   *
   * <p>A waiting loop publishes this integer into the Queue headers of every Queue it waits on, so
   * its peers can ring this exact slot.
   */
  int index() {
    return index;
  }

  /**
   * Publishes one notification and wakes the owner only when it was armed.
   *
   * @throws IOException when the slot word is neither armed nor notified, or when the wake fails
   */
  void ring() throws IOException {
    if (region.notify(index) == BellRegion.ARMED_VALUE) {
      PlatformWaiter.wake(region.word(index));
    }
  }

  /** Arms this slot for the next ring and reports a corrupt word. */
  void arm() throws IOException {
    region.arm(index);
  }

  /** Returns this slot to notified without calling the platform. */
  void publishNotification() throws IOException {
    region.notify(index);
  }

  /** Parks until a peer rings this slot or the platform interrupts the wait. */
  BellWaitOutcome park() throws IOException {
    return switch (PlatformWaiter.waitOn(region.word(index))) {
      case PROGRESS -> BellWaitOutcome.READY;
      case INTERRUPTED -> BellWaitOutcome.INTERRUPTED;
    };
  }
}
