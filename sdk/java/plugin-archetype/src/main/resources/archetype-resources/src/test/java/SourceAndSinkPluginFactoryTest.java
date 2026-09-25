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
import static org.junit.jupiter.api.Assertions.assertFalse;

import ${package}.payload.SinkRecordPayload;
import ${package}.payload.SourceRecordPayload;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bifromq.tenon.sdk.AckCode;
import org.apache.bifromq.tenon.sdk.FlowChannel;
import org.apache.bifromq.tenon.sdk.Ingress;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.ObjectMapper;

final class SourceAndSinkPluginFactoryTest {
  @TempDir Path directory;

  @Test
  void sourceQuiesceKeepsPendingResultsAndTheSharedSinkAlive() throws Exception {
    var output = directory.resolve("received.txt");
    var config =
        new ObjectMapper()
            .readTree(
                "{\"message\":\"outbound\",\"queueIndex\":0,\"outputFile\":\"" + output + "\"}");
    var sentPayload = new AtomicReference<SourceRecordPayload>();
    var result = new CompletableFuture<AckCode>();
    var owner =
        new SourceAndSinkPluginFactory()
            .create(
                config,
                Optional.of(
                    new Ingress<>(
                        1,
                        (queue, payload) -> {
                          sentPayload.set(payload);
                          return result;
                        })),
                Set.of(new FlowChannel("example", 0)));
    owner.start();

    owner.quiesce();
    assertFalse(result.isDone());
    owner
        .write(
            new FlowChannel("example", 0),
            List.of(SinkRecordPayload.newBuilder().setMessage("inbound").build()))
        .toCompletableFuture()
        .get();
    result.complete(AckCode.OK);
    owner.close();

    assertEquals("outbound", sentPayload.get().getMessage());
    assertEquals(List.of("inbound"), Files.readAllLines(output));
  }

  @Test
  void sinkOnlyDoesNotRequireSourceConfiguration() throws Exception {
    var output = directory.resolve("sink-only.txt");
    var config = new ObjectMapper().readTree("{\"outputFile\":\"" + output + "\"}");
    var owner =
        new SourceAndSinkPluginFactory()
            .create(config, Optional.empty(), Set.of(new FlowChannel("example", 0)));
    owner.start();
    owner
        .write(
            new FlowChannel("example", 0),
            List.of(SinkRecordPayload.newBuilder().setMessage("inbound").build()))
        .toCompletableFuture()
        .get();
    owner.close();

    assertEquals(List.of("inbound"), Files.readAllLines(output));
  }
}
