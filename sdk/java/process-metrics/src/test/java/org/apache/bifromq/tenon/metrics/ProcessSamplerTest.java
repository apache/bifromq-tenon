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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import org.junit.jupiter.api.Test;

class ProcessSamplerTest {
  @Test
  void realSamplesKeepCpuBaselineLocalAndReportResidentBytes() throws Exception {
    var sampler = new ProcessSampler();
    assertTrue(ProcessSampler.memory() > 0);
    assertTrue(sampler.cpu().isEmpty());
    try (var arena = Arena.ofConfined()) {
      var allocation = arena.allocate(32 * 1024 * 1024);
      allocation.fill((byte) 1);
      assertTrue(ProcessSampler.memory() >= allocation.byteSize());
      assertTrue(sampler.cpu().orElseThrow() >= 0);
      assertTrue(allocation.get(ValueLayout.JAVA_BYTE, 0) == 1);
    }
    assertTrue(new ProcessSampler().cpu().isEmpty());
  }

  @Test
  void unsupportedRssPlatformReturnsAnErrorWithoutResettingCpu() throws Exception {
    var sampler = new ProcessSampler();
    assertTrue(sampler.cpu().isEmpty());
    String platform = System.getProperty("os.name");
    try {
      System.setProperty("os.name", "Unsupported");
      assertThrows(IOException.class, ProcessSampler::memory);
    } finally {
      System.setProperty("os.name", platform);
    }
    assertTrue(sampler.cpu().isPresent());
  }
}
