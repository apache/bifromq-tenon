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
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

final class MacOsWaiter extends WaitBackend {
  private static final int SHARED = 1;
  private static final int INTERRUPTED = 4;
  private static final int NO_ENTRY = 2;
  private final MethodHandle wait;
  private final MethodHandle wake;

  @SuppressWarnings("restricted")
  MacOsWaiter() {
    var linker = Linker.nativeLinker();
    var symbols = linker.defaultLookup();
    wait =
        linker.downcallHandle(
            symbols.findOrThrow("os_sync_wait_on_address"),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT),
            Linker.Option.captureCallState("errno"));
    wake =
        linker.downcallHandle(
            symbols.findOrThrow("os_sync_wake_by_address_any"),
            FunctionDescriptor.of(
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT),
            Linker.Option.captureCallState("errno"));
  }

  @Override
  public WaitStatus waitOn(MemorySegment address) throws IOException {
    try {
      var state = CALL_STATE.get();
      var result = (int) wait.invokeExact(state, address, 0L, (long) Integer.BYTES, SHARED);
      if (result >= 0) {
        return WaitStatus.PROGRESS;
      }
      if (state.get(ValueLayout.JAVA_INT, ERRNO_OFFSET) == INTERRUPTED) {
        return WaitStatus.INTERRUPTED;
      }
      throw new IOException("macOS failed to wait for a Queue signal");
    } catch (IOException error) {
      throw error;
    } catch (Throwable error) {
      throw invocationFailure("wait for Queue signal", error);
    }
  }

  @Override
  public void wake(MemorySegment address) throws IOException {
    try {
      var state = CALL_STATE.get();
      var result = (int) wake.invokeExact(state, address, (long) Integer.BYTES, SHARED);
      if (result != 0 && state.get(ValueLayout.JAVA_INT, ERRNO_OFFSET) != NO_ENTRY) {
        throw new IOException("macOS failed to wake a Queue waiter");
      }
    } catch (IOException error) {
      throw error;
    } catch (Throwable error) {
      throw invocationFailure("wake Queue waiter", error);
    }
  }
}
