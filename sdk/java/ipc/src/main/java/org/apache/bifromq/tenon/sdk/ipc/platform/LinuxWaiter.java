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

final class LinuxWaiter extends WaitBackend {
  private static final int FUTEX_WAIT = 0;
  private static final int FUTEX_WAKE = 1;
  private static final int INTERRUPTED = 4;
  private static final int TRY_AGAIN = 11;
  private final MethodHandle syscall;
  private final long futexSystemCall;

  @SuppressWarnings("restricted")
  LinuxWaiter() {
    var linker = Linker.nativeLinker();
    syscall =
        linker.downcallHandle(
            linker.defaultLookup().findOrThrow("syscall"),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_INT,
                ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_INT),
            Linker.Option.firstVariadicArg(1),
            Linker.Option.captureCallState("errno"));
    futexSystemCall =
        switch (System.getProperty("os.arch", "")) {
          case "amd64", "x86_64" -> 202;
          case "aarch64" -> 98;
          default -> throw new ExceptionInInitializerError("Unsupported Linux CPU architecture");
        };
  }

  @Override
  public WaitStatus waitOn(MemorySegment address) throws IOException {
    var call = invoke(address, FUTEX_WAIT, 0);
    if (call.result() >= 0 || call.errorNumber() == TRY_AGAIN) {
      return WaitStatus.PROGRESS;
    }
    if (call.errorNumber() == INTERRUPTED) {
      return WaitStatus.INTERRUPTED;
    }
    throw new IOException("Linux futex wait failed with errno " + call.errorNumber());
  }

  @Override
  public void wake(MemorySegment address) throws IOException {
    var call = invoke(address, FUTEX_WAKE, 1);
    if (call.result() < 0) {
      throw new IOException("Linux futex wake failed with errno " + call.errorNumber());
    }
  }

  private SystemCall invoke(MemorySegment address, int operation, int value) throws IOException {
    try {
      var state = CALL_STATE.get();
      var result =
          (long)
              syscall.invokeExact(
                  state,
                  futexSystemCall,
                  address,
                  operation,
                  value,
                  MemorySegment.NULL,
                  MemorySegment.NULL,
                  0);
      return new SystemCall(result, state.get(ValueLayout.JAVA_INT, ERRNO_OFFSET));
    } catch (Throwable error) {
      throw invocationFailure("invoke Linux futex", error);
    }
  }

  private record SystemCall(long result, int errorNumber) {}
}
