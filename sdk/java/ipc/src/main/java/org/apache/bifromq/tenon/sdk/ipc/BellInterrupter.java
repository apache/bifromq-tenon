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
import java.lang.ref.WeakReference;
import java.util.Objects;

/** A process-local handle that interrupts one waiting loop. */
public final class BellInterrupter {
  private final WeakReference<LoopBell> bell;

  BellInterrupter(LoopBell bell) {
    this.bell = new WeakReference<>(Objects.requireNonNull(bell, "bell"));
  }

  /**
   * Interrupts the loop's current wait or its immediately following wait.
   *
   * <p>Calling this after the bound loop has been collected is a no-op.
   *
   * @throws IOException when the platform cannot wake an already blocked owner; the caller must
   *     treat that loop as failed
   */
  public void interrupt() throws IOException {
    var target = bell.get();
    if (target != null) {
      target.interrupt();
    }
  }
}
