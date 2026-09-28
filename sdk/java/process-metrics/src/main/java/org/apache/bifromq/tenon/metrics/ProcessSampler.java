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

package org.apache.bifromq.tenon.metrics;

import com.sun.management.OperatingSystemMXBean;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalDouble;

/**
 * Own-process CPU baseline and current RSS, independent of instrumentation and transport. Call
 * {@link #cpu()} from one owner; retain this sampler across collection reconnects. No background
 * work or external resources require closing.
 */
public final class ProcessSampler {
  private final OperatingSystemMXBean process =
      (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
  private long previousCpu = -1;
  private long previousTime;

  /**
   * Returns consumed CPU cores since the previous successful sample, or empty for the first sample
   * or a nonpositive elapsed interval. Failed samples retain the baseline.
   *
   * @throws IOException if the process CPU counter is unavailable or regresses
   */
  public OptionalDouble cpu() throws IOException {
    long cpu = process.getProcessCpuTime();
    long now = System.nanoTime();
    if (cpu < 0 || (previousCpu >= 0 && cpu < previousCpu))
      throw new IOException("Process CPU unavailable");
    if (previousCpu >= 0 && now - previousTime <= 0) return OptionalDouble.empty();
    var value =
        previousCpu < 0
            ? OptionalDouble.empty()
            : OptionalDouble.of((double) (cpu - previousCpu) / (now - previousTime));
    previousCpu = cpu;
    previousTime = now;
    return value;
  }

  /**
   * Reads current own-process resident bytes without changing any CPU baseline.
   *
   * @throws IOException if the platform, native call, counters, or byte conversion is unavailable
   */
  @SuppressWarnings("restricted")
  public static long memory() throws IOException {
    try {
      var linker = Linker.nativeLinker();
      var symbols = linker.defaultLookup();
      if (System.getProperty("os.name").equals("Linux")) {
        var pageSize =
            linker.downcallHandle(
                symbols.find("getpagesize").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.JAVA_INT));
        int page = (int) pageSize.invokeExact();
        long pages =
            Long.parseLong(Files.readString(Path.of("/proc/self/statm")).strip().split("\\s+")[1]);
        if (page <= 0 || pages < 0) throw new IOException("Invalid RSS counters");
        return Math.multiplyExact(pages, page);
      }
      if (System.getProperty("os.name").equals("Mac OS X")) {
        int task =
            symbols
                .find("mach_task_self_")
                .orElseThrow()
                .reinterpret(4)
                .get(ValueLayout.JAVA_INT, 0);
        var infoCall =
            linker.downcallHandle(
                symbols.find("task_info").orElseThrow(),
                FunctionDescriptor.of(
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.JAVA_INT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS));
        try (var arena = Arena.ofConfined()) {
          var info = arena.allocate(48, 8);
          var count = arena.allocate(ValueLayout.JAVA_INT);
          count.set(ValueLayout.JAVA_INT, 0, 12);
          int result = (int) infoCall.invokeExact(task, 20, info, count);
          if (result != 0 || count.get(ValueLayout.JAVA_INT, 0) != 12)
            throw new IOException("RSS task_info failed");
          return info.get(ValueLayout.JAVA_LONG, 8);
        }
      }
      throw new IOException("Unsupported process metrics platform");
    } catch (IOException failure) {
      throw failure;
    } catch (Throwable failure) {
      throw new IOException("Process RSS unavailable", failure);
    }
  }
}
