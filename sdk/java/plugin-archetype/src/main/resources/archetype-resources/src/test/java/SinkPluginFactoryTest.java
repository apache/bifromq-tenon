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

package ${package};

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ${package}.payload.SinkRecordPayload;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.bifromq.tenon.sdk.FlowChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.ObjectMapper;

final class SinkPluginFactoryTest {
  @TempDir Path directory;

  @Test
  void writesPayloadsBeforeClosingResources() throws Exception {
    var output = directory.resolve("received.txt");
    var config = new ObjectMapper().readTree("{\"outputFile\":\"" + output + "\"}");
    var sink = new SinkPluginFactory().create(config);
    sink.start();

    sink.write(new FlowChannel("example", 0), List.of(payload("first"), payload("second")))
        .toCompletableFuture()
        .get();
    sink.close();

    assertEquals(List.of("first", "second"), Files.readAllLines(output));
  }

  @Test
  void createRejectsRelativeOutputPath() throws Exception {
    var config = new ObjectMapper().readTree("{\"outputFile\":\"relative.txt\"}");

    var error = assertThrows(IOException.class, () -> new SinkPluginFactory().create(config));

    assertEquals("outputFile must be an absolute path", error.getMessage());
  }

  private static SinkRecordPayload payload(String message) {
    return SinkRecordPayload.newBuilder().setMessage(message).build();
  }
}
