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

package org.apache.bifromq.tenon.sdk.ipc.platform;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.util.Locale;

/** Internal shared-address wait backend; not part of the Plugin author API. */
public abstract class WaitBackend {
  private static final MemoryLayout CAPTURE_LAYOUT = Linker.Option.captureStateLayout();
  static final long ERRNO_OFFSET =
      CAPTURE_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("errno"));
  // Wait and wake calls never nest, so each caller thread can reuse one native capture area.
  static final ThreadLocal<MemorySegment> CALL_STATE =
      ThreadLocal.withInitial(() -> Arena.ofAuto().allocate(CAPTURE_LAYOUT));

  WaitBackend() {}

  /** Result of a native wait, before the owning loop rechecks its conditions. */
  public enum WaitStatus {
    /** Recheck the subscribed facts; a wake alone does not establish progress. */
    PROGRESS,
    /** Return control to the owner after a platform interruption. */
    INTERRUPTED
  }

  /** Waits while the shared 32-bit word is zero; reports native failures as I/O errors. */
  public abstract WaitStatus waitOn(MemorySegment address) throws IOException;

  /** Wakes one waiter on the shared 32-bit word; reports native failures as I/O errors. */
  public abstract void wake(MemorySegment address) throws IOException;

  /** Selects the host backend, rejecting unsupported operating systems or architectures. */
  public static WaitBackend open() {
    var name = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    if (name.contains("mac")) {
      return new MacOsWaiter();
    }
    if (name.contains("linux")) {
      return new LinuxWaiter();
    }
    throw new ExceptionInInitializerError("Tenon IPC Queue supports only Linux and macOS");
  }

  static IOException invocationFailure(String operation, Throwable error) {
    return new IOException("Failed to " + operation, error);
  }
}
