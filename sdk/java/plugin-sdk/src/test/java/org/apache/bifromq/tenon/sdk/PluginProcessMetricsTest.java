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

package org.apache.bifromq.tenon.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class PluginProcessMetricsTest {
  @Test
  void sharedMetricsWireVectors() throws Exception {
    var vectors = PluginProgramTestVectors.load("process_metrics_test_vectors.json");
    for (String kind : List.of("valid", "malformed")) {
      for (var vector : vectors.required(kind)) {
        byte[] encoded = PluginProgramTestVectors.bytes(vector.required("encoded"));
        org.junit.jupiter.api.function.Executable parse =
            () -> {
              byte[] actual =
                  vector.required("direction").stringValue().equals("plugin")
                      ? org.apache.bifromq.tenon.contracts.plugin.ProcessMetrics
                          .PluginToPipelineMetrics.parseFrom(encoded)
                          .toByteArray()
                      : org.apache.bifromq.tenon.contracts.plugin.ProcessMetrics
                          .PipelineToPluginMetrics.parseFrom(encoded)
                          .toByteArray();
              org.junit.jupiter.api.Assertions.assertArrayEquals(encoded, actual);
            };
        if (kind.equals("malformed"))
          org.junit.jupiter.api.Assertions.assertThrows(
              com.google.protobuf.InvalidProtocolBufferException.class, parse);
        else org.junit.jupiter.api.Assertions.assertDoesNotThrow(parse);
      }
    }
  }

  @Test
  void samplesOwnProcessAndKeepsFirstCpuMissingUntilItIsRequested() throws Exception {
    try (var collector = new PluginProcessMetrics(new byte[16])) {
      assertTrue(collector.collect(List.of("tenon.plugin.memory")).length > 0);
      assertEquals(0, collector.collect(List.of("tenon.plugin.cpu")).length);
      assertTrue(collector.collect(List.of("tenon.plugin.cpu")).length > 0);
      assertEquals(0, collector.collect(List.of("unknown")).length);
    }
    try (var restarted = new PluginProcessMetrics(new byte[16])) {
      assertEquals(0, restarted.collect(List.of("tenon.plugin.cpu")).length);
    }
  }
}
