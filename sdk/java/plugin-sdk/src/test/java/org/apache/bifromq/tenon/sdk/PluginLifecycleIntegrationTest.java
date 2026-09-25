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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.BytesValue;
import com.google.protobuf.StringValue;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.MultiThreadIoEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioIoHandler;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerDomainSocketChannel;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.tenon.contracts.plugin.PluginLifecycleGrpc;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.PipelineToPlugin;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.PluginToPipeline;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.QuiesceSource;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.Shutdown;
import org.apache.bifromq.tenon.contracts.sink.EgressRecordOuterClass.EgressRecord;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressCompletion;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressCompletionStatus;
import org.apache.bifromq.tenon.contracts.source.IngressRecordOuterClass.IngressRecord;
import org.apache.bifromq.tenon.sdk.ipc.BellRegion;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueue;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.json.JsonMapper;

final class PluginLifecycleIntegrationTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(5);
  private static final byte[] LAUNCH_ID = sequence(16);
  private static final java.util.Map<String, ?> SNAPSHOT_PROBE =
      java.util.Map.of(
          "number", new BigDecimal("123456789012345678901234567890.1234567890123456789"));

  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"source", "source-and-sink"})
  void sourceOnlyBindingUsesUdsQueuesAndTwoStageShutdown(String programInterface) throws Exception {
    var workingDirectory = directory.resolve("source-instance");
    var sourceDirectory = workingDirectory.resolve("source");
    var events = directory.resolve("source-events.log");
    Files.createDirectories(sourceDirectory);
    createSourceQueues(sourceDirectory);

    var bells = SideFixtures.Bells.create(workingDirectory, "main", 1, java.util.List.of());
    try (var server = LifecycleServer.start(directory.resolve("source-control.sock"));
        var submission = bells.readSubmission(0);
        var completion = bells.writeCompletion(0)) {
      var process = startProbe(programInterface, bells, server.socket(), events);
      try {
        writeConfig(process, events);
        assertAttach(server.awaitMessage());
        assertTrue(server.awaitMessage().hasReady());
        assertFalse(Files.exists(workingDirectory.resolve("sink")));

        var record = awaitSubmission(submission);
        assertEquals("telemetry", StringValue.parseFrom(record.getPayload()).getValue());
        submission.release(1);

        server.send(
            PipelineToPlugin.newBuilder()
                .setQuiesceSource(QuiesceSource.getDefaultInstance())
                .build());
        assertTrue(server.awaitMessage().hasSourceQuiesced());
        assertFalse(Files.readString(events).contains("source-close"));
        assertFalse(Files.readString(events).contains("source-ack-"));
        assertFalse(submission.tryRead().isPresent());

        assertTrue(
            completion.tryWrite(
                    IngressCompletion.newBuilder()
                        .setRecordId(record.getRecordId())
                        .setStatus(IngressCompletionStatus.INGRESS_COMPLETION_STATUS_OK)
                        .build()
                        .toByteArray())
                instanceof IpcQueue.Committed);
        awaitEvent(events, "source-ack-OK");

        server.send(
            PipelineToPlugin.newBuilder().setShutdown(Shutdown.getDefaultInstance()).build());
        assertTrue(server.awaitClientHalfClose());
        var result = awaitExit(process);

        assertEquals(0, result.exitCode());
        assertArrayEquals(new byte[0], result.output());
        assertFalse(result.error().contains("READY"));
        assertFalse(
            result.error().contains("Unknown channel option 'SO_KEEPALIVE'"), result.error());
        var sourceEvents =
            "source-start\nsource-quiesce\nsource-admission-closed\nsource-ack-OK\nsource-close\n";
        assertEquals(
            programInterface.equals("source")
                ? sourceEvents
                : "owner-create\nsource-create\nowner-start\n" + sourceEvents + "owner-close\n",
            Files.readString(events).replace(System.lineSeparator(), "\n"));
      } finally {
        stop(process);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"sink", "source-and-sink"})
  void sinkOnlyBindingUsesTheSameUdsLifecycleAndReleasesEgress(String programInterface)
      throws Exception {
    var vector = PluginProgramTestVectors.lifecycle("sink-shutdown");
    var workingDirectory = directory.resolve("sink-instance");
    var events = directory.resolve("sink-events.log");
    var channel = SideFixtures.flowChannel(workingDirectory, "main", 0);
    var queue = sinkQueue(workingDirectory, channel);
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);

    var bells = SideFixtures.Bells.create(workingDirectory, null, 0, java.util.List.of(channel));
    try (var server = LifecycleServer.start(directory.resolve("sink-control.sock"));
        var egress = bells.writeEgress(queue, channel)) {
      var committed =
          (IpcQueue.Committed)
              egress.tryWrite(
                  EgressRecord.newBuilder()
                      .setPayload(StringValue.of("command").toByteString())
                      .build()
                      .toByteArray());
      var process = startProbe(programInterface, bells, server.socket(), events);
      try {
        writeConfig(process, events);
        assertAttach(server.awaitMessage());
        assertTrue(server.awaitMessage().hasReady());
        assertFalse(Files.exists(workingDirectory.resolve("source")));
        awaitRelease(egress, committed.receipt());
        awaitEvent(events, "sink-write-command");

        server.send(
            PipelineToPlugin.newBuilder().setShutdown(Shutdown.getDefaultInstance()).build());
        assertTrue(server.awaitClientHalfClose());
        var result = awaitExit(process);

        if (programInterface.equals("sink")) {
          PluginProgramTestVectors.assertBusinessEvents(vector, events);
        }
        assertEquals(0, result.exitCode());
        assertArrayEquals(new byte[0], result.output());
        assertFalse(result.error().contains("READY"));
        assertEquals(
            programInterface.equals("sink")
                ? "sink-start\nsink-write-command\nsink-close\n"
                : "owner-create\nowner-start\nsink-write-command\nowner-close\n",
            Files.readString(events).replace(System.lineSeparator(), "\n"));
      } finally {
        stop(process);
      }
    }
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "TENON_TEST_RUST_SINK_BINARY", matches = ".+")
  void rustSinkConsumesJavaEgressAndClosesTheJavaControlSession() throws Exception {
    var workingDirectory = directory.resolve("rust-sink");
    var channel = SideFixtures.flowChannel(workingDirectory, "interop/flow", 2);
    var queue = sinkQueue(workingDirectory, channel);
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    var bells = SideFixtures.Bells.create(workingDirectory, null, 0, java.util.List.of(channel));
    try (var server = LifecycleServer.start(directory.resolve("rust.sock"));
        var egress = bells.writeEgress(queue, channel)) {
      var process =
          new ProcessBuilder(
                  System.getenv("TENON_TEST_RUST_SINK_BINARY"),
                  "--sdk-config",
                  bells.sdkConfig(server.socket(), LAUNCH_ID))
              .start();
      try {
        writeConfig(process, java.util.Map.of());
        assertAttach(server.awaitMessage());
        assertTrue(server.awaitMessage().hasReady());
        var committed =
            (IpcQueue.Committed)
                egress.tryWrite(
                    EgressRecord.newBuilder()
                        .setPayload(StringValue.of("java-to-rust").toByteString())
                        .build()
                        .toByteArray());
        awaitRelease(egress, committed.receipt());
        server.send(
            PipelineToPlugin.newBuilder().setShutdown(Shutdown.getDefaultInstance()).build());
        assertTrue(server.awaitClientHalfClose());
        var result = awaitExit(process);
        assertEquals(0, result.exitCode(), result.error());
        var output = new String(result.output(), StandardCharsets.UTF_8);
        assertTrue(output.contains("write interop/flow 2 [\"java-to-rust\"]"), output);
        assertEquals(1, output.lines().filter("close"::equals).count(), output);
      } finally {
        stop(process);
      }
    }
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "TENON_TEST_RUST_SOURCE_AND_SINK_BINARY", matches = ".+")
  void rustSharedOwnerExchangesBothQueueDirectionsWithJava() throws Exception {
    var workingDirectory = directory.resolve("rust-shared");
    var sourceDirectory = workingDirectory.resolve("source");
    Files.createDirectories(sourceDirectory);
    createSourceQueues(sourceDirectory);
    var channel = SideFixtures.flowChannel(workingDirectory, "interop/flow", 2);
    var queue = sinkQueue(workingDirectory, channel);
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    var bells = SideFixtures.Bells.create(workingDirectory, "main", 1, java.util.List.of(channel));
    try (var server = LifecycleServer.start(directory.resolve("rust-shared.sock"));
        var submission = bells.readSubmission(0);
        var completion = bells.writeCompletion(0);
        var egress = bells.writeEgress(queue, channel)) {
      var process =
          new ProcessBuilder(
                  System.getenv("TENON_TEST_RUST_SOURCE_AND_SINK_BINARY"),
                  "--sdk-config",
                  bells.sdkConfig(server.socket(), LAUNCH_ID))
              .start();
      try {
        writeConfig(
            process, java.util.Map.of("resource", directory.resolve("rust-connection").toString()));
        assertAttach(server.awaitMessage());
        assertTrue(server.awaitMessage().hasReady());
        var record = awaitSubmission(submission);
        assertArrayEquals(
            new byte[8], BytesValue.parseFrom(record.getPayload()).getValue().toByteArray());
        submission.release(1);
        server.send(
            PipelineToPlugin.newBuilder()
                .setQuiesceSource(QuiesceSource.getDefaultInstance())
                .build());
        assertTrue(server.awaitMessage().hasSourceQuiesced());
        var committed =
            (IpcQueue.Committed)
                egress.tryWrite(
                    EgressRecord.newBuilder()
                        .setPayload(StringValue.of("java-after-quiesce").toByteString())
                        .build()
                        .toByteArray());
        awaitRelease(egress, committed.receipt());
        assertTrue(
            completion.tryWrite(
                    IngressCompletion.newBuilder()
                        .setRecordId(record.getRecordId())
                        .setStatus(IngressCompletionStatus.INGRESS_COMPLETION_STATUS_OK)
                        .build()
                        .toByteArray())
                instanceof IpcQueue.Committed);
        awaitEvent(directory.resolve("rust-connection"), "result 0 Ok(Ok)");
        server.send(
            PipelineToPlugin.newBuilder().setShutdown(Shutdown.getDefaultInstance()).build());
        assertTrue(server.awaitClientHalfClose());
        var result = awaitExit(process);
        assertEquals(0, result.exitCode(), result.error());
        var output = new String(result.output(), StandardCharsets.UTF_8);
        assertTrue(output.contains("write interop/flow 2 [\"java-after-quiesce\"]"), output);
        assertTrue(output.contains("result 0 Ok(Ok)"), output);
        assertEquals(1, output.lines().filter("opened"::equals).count(), output);
        assertEquals(1, output.lines().filter("source-close"::equals).count(), output);
        assertEquals(1, output.lines().filter("shared-close"::equals).count(), output);
      } finally {
        stop(process);
      }
    }
  }

  @Test
  void sourceAndSinkProgramKeepsOneSharedOwnerAliveUntilFinalShutdown() throws Exception {
    var vector = PluginProgramTestVectors.lifecycle("shared-owner");
    var workingDirectory = directory.resolve("source-and-sink-instance");
    var sourceDirectory = workingDirectory.resolve("source");
    var events = directory.resolve("source-and-sink-events.log");
    Files.createDirectories(sourceDirectory);
    createSourceQueues(sourceDirectory);
    var channel = SideFixtures.flowChannel(workingDirectory, "main", 0);
    var queue = sinkQueue(workingDirectory, channel);
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);

    var bells = SideFixtures.Bells.create(workingDirectory, "main", 1, java.util.List.of(channel));
    try (var server = LifecycleServer.start(directory.resolve("dual.sock"));
        var submission = bells.readSubmission(0);
        var completion = bells.writeCompletion(0);
        var egress = bells.writeEgress(queue, channel)) {
      var process = startProbe("source-and-sink", bells, server.socket(), events);
      try {
        writeConfig(
            process,
            java.util.Map.of("eventsFile", events.toString(), "snapshotProbe", SNAPSHOT_PROBE));
        assertAttach(server.awaitMessage());
        assertTrue(server.awaitMessage().hasReady());

        var sourceRecord = awaitSubmission(submission);
        submission.release(1);
        server.send(
            PipelineToPlugin.newBuilder()
                .setQuiesceSource(QuiesceSource.getDefaultInstance())
                .build());
        assertTrue(server.awaitMessage().hasSourceQuiesced());
        assertFalse(Files.readString(events).contains("source-close"));
        assertFalse(Files.readString(events).contains("source-ack-"));
        assertFalse(Files.readString(events).contains("owner-close"));
        assertFalse(submission.tryRead().isPresent());

        var sinkRecord =
            (IpcQueue.Committed)
                egress.tryWrite(
                    EgressRecord.newBuilder()
                        .setPayload(
                            StringValue.of(vector.required("sinkWriteAfterQuiesce").stringValue())
                                .toByteString())
                        .build()
                        .toByteArray());
        awaitRelease(egress, sinkRecord.receipt());
        awaitEvent(events, "sink-write-" + vector.required("sinkWriteAfterQuiesce").stringValue());

        assertTrue(
            completion.tryWrite(
                    IngressCompletion.newBuilder()
                        .setRecordId(sourceRecord.getRecordId())
                        .setStatus(IngressCompletionStatus.INGRESS_COMPLETION_STATUS_OK)
                        .build()
                        .toByteArray())
                instanceof IpcQueue.Committed);
        awaitEvent(events, "source-ack-" + vector.required("completionAfterQuiesce").stringValue());

        server.send(
            PipelineToPlugin.newBuilder().setShutdown(Shutdown.getDefaultInstance()).build());
        assertTrue(server.awaitClientHalfClose());
        var result = awaitExit(process);

        PluginProgramTestVectors.assertBusinessEvents(vector, events);
        assertEquals(0, result.exitCode());
        assertArrayEquals(new byte[0], result.output());
        assertFalse(result.error().contains("READY"));
        assertEquals(
            "owner-create\n"
                + "source-create\n"
                + "owner-start\n"
                + "source-start\n"
                + "source-quiesce\n"
                + "source-admission-closed\n"
                + "sink-write-after-quiesce\n"
                + "source-ack-OK\n"
                + "source-close\n"
                + "owner-close\n",
            Files.readString(events).replace(System.lineSeparator(), "\n"));
      } finally {
        stop(process);
      }
    }
  }

  @Test
  void controlStreamLossExitsImmediatelyWithoutClosingSourceBusinessCode() throws Exception {
    var workingDirectory = directory.resolve("stream-loss-instance");
    var sourceDirectory = workingDirectory.resolve("source");
    var events = directory.resolve("stream-loss-events.log");
    Files.createDirectories(sourceDirectory);
    createSourceQueues(sourceDirectory);

    var bells = SideFixtures.Bells.create(workingDirectory, "main", 1, java.util.List.of());
    try (var server = LifecycleServer.start(directory.resolve("stream-loss-control.sock"))) {
      var process = startProbe("source", bells, server.socket(), events);
      try {
        writeConfig(process, events);
        assertAttach(server.awaitMessage());
        assertTrue(server.awaitMessage().hasReady());

        server.endResponseStream();
        var result = awaitExit(process);

        assertEquals(1, result.exitCode());
        assertArrayEquals(new byte[0], result.output());
        assertTrue(result.error().contains("Plugin control stream ended before process shutdown"));
        assertTrue(Files.readString(events).contains("source-start"));
        assertFalse(Files.readString(events).contains("source-close"));
      } finally {
        stop(process);
      }
    }
  }

  @Test
  void stdinOwnerLossExitsEvenWhileTheSinkEgressLoopIsParked() throws Exception {
    var workingDirectory = directory.resolve("parked-sink");
    var events = directory.resolve("parked-sink-events.log");
    var channel = SideFixtures.flowChannel(workingDirectory, "main", 0);
    var queue = sinkQueue(workingDirectory, channel);
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);

    var bells = SideFixtures.Bells.create(workingDirectory, null, 0, java.util.List.of(channel));
    try (var server = LifecycleServer.start(directory.resolve("parked-sink-control.sock"))) {
      var process = startProbe("sink", bells, server.socket(), events);
      try {
        writeConfig(process, events);
        assertAttach(server.awaitMessage());
        assertTrue(server.awaitMessage().hasReady());
        awaitEvent(events, "sink-start");
        // The Egress Queue is empty and nothing rings it, so the Sink's single
        // Egress loop stays parked on its own doorbell.
        awaitArmedSlot(
            workingDirectory.resolve("sink").resolve(BellRegion.LOOPS_BELL_FILE_NAME), 0);
        assertTrue(process.isAlive());

        // The Runner ends a Plugin by closing stdin. The Plugin owns no force
        // wake, and must not need one: a parked loop cannot hold it open.
        process.getOutputStream().close();
        var result = awaitExit(process);

        assertEquals(1, result.exitCode());
        assertTrue(result.error().contains("Pipeline stdin owner channel closed unexpectedly"));
        assertFalse(Files.readString(events).contains("sink-close"));
      } finally {
        stop(process);
      }
    }
  }

  /** Waits until {@code slot} of the Bell Region at {@code region} reads armed. */
  private static void awaitArmedSlot(Path region, int slot) throws Exception {
    var deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      var bytes = Files.readAllBytes(region);
      // The contract's layout: one 64-byte header, then 64-byte slots whose
      // state word is armed at 0. A peer reads the same bytes.
      var word = ByteBuffer.wrap(bytes, 64 + 64 * slot, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
      if (word == 0) {
        return;
      }
      Thread.sleep(10);
    }
    throw new AssertionError("Bell Region slot " + slot + " was never armed");
  }

  @Test
  void stdinOwnerLossExitsImmediatelyWithoutClosingSinkBusinessCode() throws Exception {
    var workingDirectory = directory.resolve("stdin-loss-instance");
    var events = directory.resolve("stdin-loss-events.log");
    var channel = SideFixtures.flowChannel(workingDirectory, "main", 0);
    var queue = sinkQueue(workingDirectory, channel);
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);

    var bells = SideFixtures.Bells.create(workingDirectory, null, 0, java.util.List.of(channel));
    try (var server = LifecycleServer.start(directory.resolve("stdin-loss-control.sock"))) {
      var process = startProbe("sink", bells, server.socket(), events);
      try {
        writeConfig(process, events);
        assertAttach(server.awaitMessage());
        assertTrue(server.awaitMessage().hasReady());

        process.getOutputStream().close();
        var result = awaitExit(process);

        assertEquals(1, result.exitCode());
        assertArrayEquals(new byte[0], result.output());
        assertTrue(result.error().contains("Pipeline stdin owner channel closed unexpectedly"));
        assertTrue(Files.readString(events).contains("sink-start"));
        assertFalse(Files.readString(events).contains("sink-close"));
      } finally {
        stop(process);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = SinkProbeFactory.BlockedMethod.class,
      names = {"START", "CLOSE"})
  void stdinOwnerLossExitsEvenWhileSinkBusinessLifecycleIsBlocked(
      SinkProbeFactory.BlockedMethod blockedMethod) throws Exception {
    var workingDirectory = directory.resolve("blocked-sink");
    var events = directory.resolve("blocked-sink-events.log");
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    var channel = SideFixtures.flowChannel(workingDirectory, "main", 0);
    IpcQueue.create(
        sinkQueue(workingDirectory, channel),
        capacity,
        capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    var bells = SideFixtures.Bells.create(workingDirectory, null, 0, java.util.List.of(channel));
    try (var server = LifecycleServer.start(directory.resolve("blocked-sink.sock"))) {
      var process = startProbe("sink", bells, server.socket(), events);
      try {
        writeConfig(
            process,
            java.util.Map.of(
                "eventsFile", events.toString(), "blockedMethod", blockedMethod.name()));
        assertAttach(server.awaitMessage());
        if (blockedMethod == SinkProbeFactory.BlockedMethod.CLOSE) {
          assertTrue(server.awaitMessage().hasReady());
          server.send(
              PipelineToPlugin.newBuilder().setShutdown(Shutdown.getDefaultInstance()).build());
        }
        awaitEvent(
            events,
            blockedMethod == SinkProbeFactory.BlockedMethod.START ? "sink-start" : "sink-close");
        assertTrue(process.isAlive());
        process.getOutputStream().close();
        var result = awaitExit(process);
        assertEquals(1, result.exitCode());
        assertTrue(result.error().contains("Pipeline stdin owner channel closed unexpectedly"));
      } finally {
        stop(process);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void uncaughtBusinessThreadFailureTerminatesWithoutInvokingPreviousHandler(
      boolean hasPreviousHandler) throws Exception {
    var workingDirectory = directory.resolve("ordinary-thread-failure");
    var events = directory.resolve("ordinary-thread-events.log");
    Files.createDirectories(workingDirectory.resolve("source"));
    createSourceQueues(workingDirectory.resolve("source"));
    var bells = SideFixtures.Bells.create(workingDirectory, "main", 1, java.util.List.of());
    try (var server = LifecycleServer.start(directory.resolve("ordinary-thread.sock"))) {
      var process =
          startProbe(
              "source",
              bells,
              server.socket(),
              events,
              "-Dtenon.test.uncaught-handler=" + hasPreviousHandler);
      try {
        writeConfig(
            process,
            java.util.Map.of("eventsFile", events.toString(), "failurePoint", "EXCEPTION_THREAD"));
        var result = awaitExit(process);
        assertEquals(1, result.exitCode());
        assertEquals(1, occurrences(result.error(), "Ordinary business thread failure"));
        assertFalse(result.error().contains("Existing uncaught handler:"));
        assertFalse(Files.readAllLines(events).contains("source-close"));
      } finally {
        stop(process);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = SourceProbeFactory.FailurePoint.class,
      names = {"START", "QUIESCE", "CLOSE"})
  void sourceLifecycleFailureTerminatesBeforeLaterCallbacks(
      SourceProbeFactory.FailurePoint failurePoint) throws Exception {
    var workingDirectory = directory.resolve("failed-source");
    var sourceDirectory = workingDirectory.resolve("source");
    var events = directory.resolve("failed-source-events.log");
    Files.createDirectories(sourceDirectory);
    createSourceQueues(sourceDirectory);
    var bells = SideFixtures.Bells.create(workingDirectory, "main", 1, java.util.List.of());
    try (var server = LifecycleServer.start(directory.resolve("failed-source.sock"));
        var submission = bells.readSubmission(0);
        var completion = bells.writeCompletion(0)) {
      var process = startProbe("source", bells, server.socket(), events);
      try {
        writeConfig(
            process,
            java.util.Map.of("eventsFile", events.toString(), "failurePoint", failurePoint.name()));
        if (failurePoint != SourceProbeFactory.FailurePoint.START) {
          assertAttach(server.awaitMessage());
          assertTrue(server.awaitMessage().hasReady());
          var record = awaitSubmission(submission);
          submission.release(1);
          server.send(
              PipelineToPlugin.newBuilder()
                  .setQuiesceSource(QuiesceSource.getDefaultInstance())
                  .build());
          if (failurePoint == SourceProbeFactory.FailurePoint.CLOSE) {
            assertTrue(server.awaitMessage().hasSourceQuiesced());
            var committed =
                (IpcQueue.Committed)
                    completion.tryWrite(
                        IngressCompletion.newBuilder()
                            .setRecordId(record.getRecordId())
                            .setStatus(IngressCompletionStatus.INGRESS_COMPLETION_STATUS_OK)
                            .build()
                            .toByteArray());
            awaitRelease(completion, committed.receipt());
            server.send(
                PipelineToPlugin.newBuilder().setShutdown(Shutdown.getDefaultInstance()).build());
          }
        }
        var result = awaitExit(process);
        assertEquals(1, result.exitCode());
        assertTrue(result.error().contains("Source probe failed during " + failurePoint));
        var recorded = Files.readAllLines(events);
        if (failurePoint == SourceProbeFactory.FailurePoint.CLOSE) {
          assertTrue(recorded.indexOf("source-ack-OK") >= 0);
          assertTrue(recorded.indexOf("source-ack-OK") < recorded.indexOf("source-close"));
          assertEquals(1, recorded.stream().filter("source-close"::equals).count());
        } else {
          assertFalse(recorded.contains("source-close"));
          assertFalse(recorded.contains("source-result-failed"));
        }
      } finally {
        stop(process);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(OwnerLoss.class)
  void dualProgramOwnerLossAfterQuiesceDoesNotWaitForPendingResults(OwnerLoss ownerLoss)
      throws Exception {
    var workingDirectory = directory.resolve("dual-owner-loss");
    var sourceDirectory = workingDirectory.resolve("source");
    var events = directory.resolve("dual-owner-loss-events.log");
    Files.createDirectories(sourceDirectory);
    createSourceQueues(sourceDirectory);
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    var channel = SideFixtures.flowChannel(workingDirectory, "main", 0);
    IpcQueue.create(
        sinkQueue(workingDirectory, channel),
        capacity,
        capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    var bells = SideFixtures.Bells.create(workingDirectory, "main", 1, java.util.List.of(channel));
    try (var server = LifecycleServer.start(directory.resolve("dual-loss.sock"))) {
      var process = startProbe("source-and-sink", bells, server.socket(), events);
      try {
        writeConfig(process, events);
        assertAttach(server.awaitMessage());
        assertTrue(server.awaitMessage().hasReady());
        server.send(
            PipelineToPlugin.newBuilder()
                .setQuiesceSource(QuiesceSource.getDefaultInstance())
                .build());
        assertTrue(server.awaitMessage().hasSourceQuiesced());
        switch (ownerLoss) {
          case CONTROL_STREAM -> server.endResponseStream();
          case STDIN -> process.getOutputStream().close();
        }
        var result = awaitExit(process);
        assertEquals(1, result.exitCode());
        assertTrue(
            result
                .error()
                .contains(
                    switch (ownerLoss) {
                      case CONTROL_STREAM -> "Plugin control stream ended before process shutdown";
                      case STDIN -> "Pipeline stdin owner channel closed unexpectedly";
                    }));
        var recorded = Files.readString(events);
        assertTrue(recorded.contains("source-quiesce"));
        assertFalse(recorded.contains("source-ack-"));
        assertFalse(recorded.contains("source-close"));
        assertFalse(recorded.contains("owner-close"));
      } finally {
        stop(process);
      }
    }
  }

  @Test
  void oneSourceUsesTheWholeQueueSessionWhileConfigReturnsIndependentSnapshots() throws Exception {
    var workingDirectory = directory.resolve("source-context");
    var sourceDirectory = workingDirectory.resolve("source");
    var events = directory.resolve("source-context.log");
    Files.createDirectories(sourceDirectory);
    createSourceQueues(sourceDirectory, 2);
    var bells = SideFixtures.Bells.create(workingDirectory, "main", 2, java.util.List.of());
    try (var server = LifecycleServer.start(directory.resolve("source-context.sock"));
        var firstSubmission = bells.readSubmission(0);
        var firstCompletion = bells.writeCompletion(0);
        var secondSubmission = bells.readSubmission(1);
        var secondCompletion = bells.writeCompletion(1)) {
      var process = startProbe("source", bells, server.socket(), events);
      try {
        writeConfig(
            process,
            java.util.Map.of(
                "eventsFile",
                events.toString(),
                "workerCount",
                2,
                "snapshotProbe",
                SNAPSHOT_PROBE));
        assertAttach(server.awaitMessage());
        assertTrue(server.awaitMessage().hasReady());
        awaitEvent(events, "source-context-verified");
        for (var channel = 0; channel < 2; channel++) {
          var submission = channel == 0 ? firstSubmission : secondSubmission;
          var completion = channel == 0 ? firstCompletion : secondCompletion;
          var record = awaitSubmission(submission);
          submission.release(1);
          assertTrue(
              completion.tryWrite(
                      IngressCompletion.newBuilder()
                          .setRecordId(record.getRecordId())
                          .setStatus(IngressCompletionStatus.INGRESS_COMPLETION_STATUS_OK)
                          .build()
                          .toByteArray())
                  instanceof IpcQueue.Committed);
        }
        server.send(
            PipelineToPlugin.newBuilder()
                .setQuiesceSource(QuiesceSource.getDefaultInstance())
                .build());
        assertTrue(server.awaitMessage().hasSourceQuiesced());
        server.send(
            PipelineToPlugin.newBuilder().setShutdown(Shutdown.getDefaultInstance()).build());
        assertTrue(server.awaitClientHalfClose());
        var result = awaitExit(process);
        assertEquals(0, result.exitCode(), result.error());
        var recorded = Files.readAllLines(events);
        assertEquals(1, recorded.stream().filter("source-start"::equals).count());
        assertEquals(1, recorded.stream().filter("source-close"::equals).count());
      } finally {
        stop(process);
      }
    }
  }

  @Test
  void fatalSourceFailureReportsItsCompleteCauseOnceAndExitsImmediately() throws Exception {
    var workingDirectory = directory.resolve("source-cause");
    var sourceDirectory = workingDirectory.resolve("source");
    var events = directory.resolve("source-cause.log");
    Files.createDirectories(sourceDirectory);
    createSourceQueues(sourceDirectory);
    var bells = SideFixtures.Bells.create(workingDirectory, "main", 1, java.util.List.of());
    try (var server = LifecycleServer.start(directory.resolve("source-cause.sock"))) {
      var process = startProbe("source", bells, server.socket(), events);
      try {
        writeConfig(
            process,
            java.util.Map.of("eventsFile", events.toString(), "failurePoint", "START_AND_CLOSE"));
        var result = awaitExit(process);
        assertEquals(1, result.exitCode());
        assertArrayEquals(new byte[0], result.output());
        assertEquals(1, occurrences(result.error(), "Expected Source startup failure"));
        assertEquals(0, occurrences(result.error(), "Expected Source cleanup failure"));
        assertEquals(1, occurrences(result.error(), "Expected Source root cause"));
        assertFalse(result.error().contains("Suppressed:"));
        assertFalse(Files.readAllLines(events).contains("source-close"));
        assertTrue(result.error().contains("Caused by: java.lang.IllegalStateException"));
        assertFalse(result.error().contains("Exception in thread"));
      } finally {
        stop(process);
      }
    }
  }

  @Test
  void sinkFailureReportsItsOriginalCauseWithoutBusinessClose() throws Exception {
    var workingDirectory = directory.resolve("sink-cause");
    var events = directory.resolve("sink-cause.log");
    var channel = SideFixtures.flowChannel(workingDirectory, "main", 0);
    var queue = sinkQueue(workingDirectory, channel);
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    var bells = SideFixtures.Bells.create(workingDirectory, null, 0, java.util.List.of(channel));
    try (var server = LifecycleServer.start(directory.resolve("sink-cause.sock"));
        var egress = bells.writeEgress(queue, channel)) {
      var process = startProbe("sink", bells, server.socket(), events);
      try {
        writeConfig(process, java.util.Map.of("eventsFile", events.toString(), "failWrite", true));
        assertAttach(server.awaitMessage());
        assertTrue(server.awaitMessage().hasReady());
        assertTrue(
            egress.tryWrite(
                    EgressRecord.newBuilder()
                        .setPayload(StringValue.of("failed").toByteString())
                        .build()
                        .toByteArray())
                instanceof IpcQueue.Committed);
        var result = awaitExit(process);
        assertEquals(1, result.exitCode());
        assertArrayEquals(new byte[0], result.output());
        assertEquals(1, occurrences(result.error(), "Expected Sink write failure"));
        assertEquals(1, occurrences(result.error(), "Expected Sink suppressed failure"));
        assertEquals(1, occurrences(result.error(), "Expected Sink root cause"));
        assertTrue(result.error().contains("Suppressed: java.io.IOException"));
        assertTrue(result.error().contains("Caused by: java.lang.IllegalStateException"));
        assertEquals(0, Files.readAllLines(events).stream().filter("sink-close"::equals).count());
      } finally {
        stop(process);
      }
    }
  }

  @Test
  void sharedInvalidLifecycleVectorsFailRealJavaPrograms() throws Exception {
    var vectors =
        PluginProgramTestVectors.load("process_protocol_test_vectors.json")
            .required("lifecycleInvalid");
    var index = 0;
    for (var vector : vectors) {
      if (!vector.required("receiver").stringValue().equals("sdk")) continue;
      var pluginInterface = vector.required("interface").stringValue();
      var working = directory.resolve("invalid-vector-" + index++);
      var events = working.resolve("events");
      Files.createDirectories(working);
      if (!pluginInterface.equals("sink")) {
        Files.createDirectories(working.resolve("source"));
        createSourceQueues(working.resolve("source"));
      }
      var hasSource = !pluginInterface.equals("sink");
      var channel =
          pluginInterface.equals("source") ? null : SideFixtures.flowChannel(working, "main", 0);
      if (!pluginInterface.equals("source")) {
        var queue = sinkQueue(working, channel);
        IpcQueue.create(queue, IpcQueueFormat.DataCapacity.of(4096), 1024);
      }
      var bells =
          SideFixtures.Bells.create(
              working,
              hasSource ? "main" : null,
              hasSource ? 1 : 0,
              channel == null ? java.util.List.of() : java.util.List.of(channel));
      try (var server = LifecycleServer.start(directory.resolve("invalid-" + index + ".sock"))) {
        var process = startProbe(pluginInterface, bells, server.socket(), events);
        try {
          if (pluginInterface.equals("source")) writeConfig(process, events);
          else writeConfig(process, events);
          for (var event : vector.required("events")) {
            switch (event.stringValue()) {
              case "control.attach" -> assertAttach(server.awaitMessage());
              case "control.ready" -> assertTrue(server.awaitMessage().hasReady());
              case "control.quiesce-source" ->
                  server.send(
                      PipelineToPlugin.newBuilder()
                          .setQuiesceSource(QuiesceSource.getDefaultInstance())
                          .build());
              case "control.shutdown" ->
                  server.send(
                      PipelineToPlugin.newBuilder()
                          .setShutdown(Shutdown.getDefaultInstance())
                          .build());
              case "stdin.eof" -> process.getOutputStream().close();
              case "control.stream-eof" -> server.endResponseStream();
              default -> throw new AssertionError("Unhandled lifecycle event: " + event);
            }
          }
          var result = awaitExit(process);
          assertEquals(vector.required("exitCode").intValue(), result.exitCode(), result.error());
          assertFalse(result.error().isBlank());
          var closeEvents = java.util.Set.of("source-close", "sink-close", "owner-close");
          assertEquals(
              vector.required("businessCloseCount").longValue(),
              Files.readAllLines(events).stream().filter(closeEvents::contains).count());
        } finally {
          stop(process);
        }
      }
    }
  }

  private static int occurrences(String text, String expected) {
    return text.split(java.util.regex.Pattern.quote(expected), -1).length - 1;
  }

  @Test
  void sharedStartFailureTerminatesWithoutBusinessClose() throws Exception {
    var workingDirectory = directory.resolve("shared-start-failure");
    var sourceDirectory = workingDirectory.resolve("source");
    Files.createDirectories(sourceDirectory);
    createSourceQueues(sourceDirectory);
    var channel = SideFixtures.flowChannel(workingDirectory, "main", 0);
    var queue = sinkQueue(workingDirectory, channel);
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    var events = directory.resolve("shared-start-failure.log");
    var bells = SideFixtures.Bells.create(workingDirectory, "main", 1, java.util.List.of(channel));
    try (var server = LifecycleServer.start(directory.resolve("shared-start-failure.sock"))) {
      var process = startProbe("source-and-sink", bells, server.socket(), events);
      try {
        writeConfig(
            process, java.util.Map.of("eventsFile", events.toString(), "failurePoint", "START"));
        var result = awaitExit(process);
        assertEquals(1, result.exitCode());
        assertTrue(result.error().contains("Expected shared start failure"));
        var recorded = Files.readAllLines(events);
        assertFalse(recorded.contains("owner-close"));
        assertFalse(recorded.contains("owner-result-failed"));
        assertFalse(recorded.contains("source-start"));
      } finally {
        stop(process);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = SourceAndSinkProbeFactory.FailurePoint.class,
      names = {"SOURCE_START", "SOURCE_START_AND_CLOSE"})
  void sharedSourceStartFailureTerminatesWithoutBusinessClose(
      SourceAndSinkProbeFactory.FailurePoint failurePoint) throws Exception {
    var workingDirectory = directory.resolve("shared-source-failure");
    var sourceDirectory = workingDirectory.resolve("source");
    Files.createDirectories(sourceDirectory);
    createSourceQueues(sourceDirectory);
    var channel = SideFixtures.flowChannel(workingDirectory, "main", 0);
    var queue = sinkQueue(workingDirectory, channel);
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    var events = directory.resolve("shared-source-failure.log");
    var bells = SideFixtures.Bells.create(workingDirectory, "main", 1, java.util.List.of(channel));
    try (var server = LifecycleServer.start(directory.resolve("shared-source-failure.sock"))) {
      var process = startProbe("source-and-sink", bells, server.socket(), events);
      try {
        writeConfig(
            process,
            java.util.Map.of("eventsFile", events.toString(), "failurePoint", failurePoint.name()));
        var result = awaitExit(process);
        assertEquals(1, result.exitCode());
        var recorded = Files.readAllLines(events);
        assertEquals(1, recorded.stream().filter("owner-start"::equals).count());
        assertFalse(recorded.contains("owner-close"));
        assertFalse(recorded.contains("source-close"));
        if (failurePoint == SourceAndSinkProbeFactory.FailurePoint.SOURCE_START_AND_CLOSE) {
          assertEquals(1, occurrences(result.error(), "Expected Source startup failure"));
          assertEquals(1, occurrences(result.error(), "Expected Source root cause"));
          assertEquals(0, occurrences(result.error(), "Expected Source cleanup failure"));
        } else {
          assertEquals(1, occurrences(result.error(), "Source probe failed during START"));
        }
      } finally {
        stop(process);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"READY", "QUIESCED"})
  void sharedSinkFailureTerminatesWithoutBusinessClose(String phase) throws Exception {
    var workingDirectory = directory.resolve("shared-write-failure");
    var sourceDirectory = workingDirectory.resolve("source");
    Files.createDirectories(sourceDirectory);
    createSourceQueues(sourceDirectory);
    var channel = SideFixtures.flowChannel(workingDirectory, "main", 0);
    var queue = sinkQueue(workingDirectory, channel);
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    var events = directory.resolve("shared-write-failure.log");
    var bells = SideFixtures.Bells.create(workingDirectory, "main", 1, java.util.List.of(channel));
    try (var server = LifecycleServer.start(directory.resolve("shared-write-failure.sock"));
        var egress = bells.writeEgress(queue, channel)) {
      var process = startProbe("source-and-sink", bells, server.socket(), events);
      try {
        writeConfig(
            process, java.util.Map.of("eventsFile", events.toString(), "failurePoint", "WRITE"));
        assertAttach(server.awaitMessage());
        assertTrue(server.awaitMessage().hasReady());
        if (phase.equals("QUIESCED")) {
          server.send(
              PipelineToPlugin.newBuilder()
                  .setQuiesceSource(QuiesceSource.getDefaultInstance())
                  .build());
          assertTrue(server.awaitMessage().hasSourceQuiesced());
        }
        var committed =
            (IpcQueue.Committed)
                egress.tryWrite(
                    EgressRecord.newBuilder()
                        .setPayload(StringValue.of("failed-command").toByteString())
                        .build()
                        .toByteArray());
        var result = awaitExit(process);
        assertEquals(1, result.exitCode());
        assertEquals(1, occurrences(result.error(), "Expected shared Sink failure"));
        assertFalse(egress.isReleased(committed.receipt()));
        var recorded = Files.readAllLines(events);
        assertFalse(recorded.contains("source-close"));
        assertFalse(recorded.contains("owner-close"));
      } finally {
        stop(process);
      }
    }
  }

  @Test
  void sharedSinkFailureInterruptsQuiesceOnInheritedSubmissionOccupancy() throws Exception {
    var workingDirectory = directory.resolve("shared-inherited-occupancy");
    var sourceDirectory = workingDirectory.resolve("source");
    Files.createDirectories(sourceDirectory);
    IpcQueue.create(
        sourceDirectory.resolve("submission-0.queue"),
        IpcQueueFormat.dataCapacityForRecordLimit(1, 1024),
        1024);
    IpcQueue.create(
        sourceDirectory.resolve("completion-0.queue"),
        IpcQueueFormat.dataCapacityForRecordLimit(1, 13),
        13);
    var channel = SideFixtures.flowChannel(workingDirectory, "main", 0);
    var queue = sinkQueue(workingDirectory, channel);
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    var events = directory.resolve("shared-inherited-occupancy.log");
    var bells = SideFixtures.Bells.create(workingDirectory, "main", 1, java.util.List.of(channel));
    try (var submission = bells.readSubmission(0);
        var egress = bells.writeEgress(queue, channel);
        var server = LifecycleServer.start(directory.resolve("inherited.sock"))) {
      // Each real prior session has one credit; its unreleased record survives reopening.
      for (var index = 0; index < 2; index++) {
        try (var previous =
            SourceSession.<StringValue>open(sourceDirectory, bells.sourceChannelBellPath())) {
          previous.sender().send(0, StringValue.of("x".repeat(1014)));
          awaitSubmission(submission);
        }
      }
      var process = startProbe("source-and-sink", bells, server.socket(), events);
      try {
        writeConfig(
            process, java.util.Map.of("eventsFile", events.toString(), "failurePoint", "WRITE"));
        assertAttach(server.awaitMessage());
        assertTrue(server.awaitMessage().hasReady());
        server.send(
            PipelineToPlugin.newBuilder()
                .setQuiesceSource(QuiesceSource.getDefaultInstance())
                .build());
        awaitEvent(events, "source-quiesce");
        var committed =
            (IpcQueue.Committed)
                egress.tryWrite(
                    EgressRecord.newBuilder()
                        .setPayload(StringValue.of("failed-command").toByteString())
                        .build()
                        .toByteArray());
        var result = awaitExit(process);
        assertEquals(1, result.exitCode());
        assertEquals(1, occurrences(result.error(), "Expected shared Sink failure"));
        assertFalse(egress.isReleased(committed.receipt()));
        var recorded = Files.readAllLines(events);
        assertFalse(recorded.contains("source-close"));
        assertFalse(recorded.contains("owner-close"));
      } finally {
        stop(process);
      }
    }
  }

  private enum OwnerLoss {
    CONTROL_STREAM,
    STDIN
  }

  @Test
  void oneFailedQueueTerminatesTheWholeProcessAndEachQueueReplaysFromItsOwnRelease()
      throws Exception {
    var workingDirectory = directory.resolve("parallel-failure");
    var channels =
        java.util.List.of(
            SideFixtures.flowChannel(workingDirectory, "alpha", 0),
            SideFixtures.flowChannel(workingDirectory, "beta", 0));
    var firstQueue = sinkQueue(workingDirectory, channels.getFirst());
    var secondQueue = sinkQueue(workingDirectory, channels.getLast());
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(firstQueue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    IpcQueue.create(secondQueue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    var bells = SideFixtures.Bells.create(workingDirectory, null, 0, channels);
    try (var first = bells.writeEgress(firstQueue, channels.getFirst());
        var second = bells.writeEgress(secondQueue, channels.getLast())) {
      IpcQueue.Committed firstCommit;
      IpcQueue.Committed secondCommit;
      var failedEvents = directory.resolve("parallel-failure-events.log");
      try (var server = LifecycleServer.start(directory.resolve("parallel-failure.sock"))) {
        var process = startProbe("sink", bells, server.socket(), failedEvents);
        try {
          writeConfig(
              process,
              java.util.Map.of(
                  "eventsFile", failedEvents.toString(), "failWrite", true, "holdPayload", "held"));
          assertAttach(server.awaitMessage());
          assertTrue(server.awaitMessage().hasReady());
          // The first Queue's batch stays in flight, so its record is read but never released,
          // while the failure of the second Queue terminates the process.
          firstCommit =
              (IpcQueue.Committed)
                  first.tryWrite(
                      EgressRecord.newBuilder()
                          .setPayload(StringValue.of("held").toByteString())
                          .build()
                          .toByteArray());
          secondCommit =
              (IpcQueue.Committed)
                  second.tryWrite(
                      EgressRecord.newBuilder()
                          .setPayload(StringValue.of("failure").toByteString())
                          .build()
                          .toByteArray());

          var result = awaitExit(process);
          assertEquals(1, result.exitCode());
          var events = Files.readString(failedEvents);
          assertTrue(events.contains("sink-write-held"));
          assertTrue(events.contains("sink-write-failure"));
          assertEquals(0, events.lines().filter("sink-close"::equals).count());
          assertFalse(first.isReleased(firstCommit.receipt()));
          assertFalse(second.isReleased(secondCommit.receipt()));
        } finally {
          stop(process);
        }
      }
      var replayEvents = directory.resolve("parallel-replay-events.log");
      try (var server = LifecycleServer.start(directory.resolve("parallel-replay.sock"))) {
        var process = startProbe("sink", bells, server.socket(), replayEvents);
        try {
          writeConfig(process, java.util.Map.of("eventsFile", replayEvents.toString()));
          assertAttach(server.awaitMessage());
          assertTrue(server.awaitMessage().hasReady());
          awaitRelease(first, firstCommit.receipt());
          awaitRelease(second, secondCommit.receipt());
          server.send(
              PipelineToPlugin.newBuilder().setShutdown(Shutdown.getDefaultInstance()).build());
          assertTrue(server.awaitClientHalfClose());
          assertEquals(0, awaitExit(process).exitCode());
          var events = Files.readString(replayEvents);
          assertEquals(1, events.lines().filter("sink-write-held"::equals).count());
          assertEquals(1, events.lines().filter("sink-write-failure"::equals).count());
          assertEquals(1, events.lines().filter("sink-close"::equals).count());
        } finally {
          stop(process);
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = SourceProbeFactory.FailurePoint.class,
      names = {"ERROR_FACTORY", "ERROR_START", "ERROR_THREAD", "ERROR_QUIESCE", "ERROR_CLOSE"})
  void fatalSourceCallbackStopsBeforeLaterBusinessCallbacks(
      SourceProbeFactory.FailurePoint failurePoint) throws Exception {
    var workingDirectory = directory.resolve("fatal-callback");
    var events = directory.resolve("fatal-callback-events.log");
    Files.createDirectories(workingDirectory.resolve("source"));
    createSourceQueues(workingDirectory.resolve("source"), 2);
    var bells = SideFixtures.Bells.create(workingDirectory, "main", 2, java.util.List.of());
    try (var server = LifecycleServer.start(directory.resolve("fatal-callback.sock"))) {
      var process = startProbe("source", bells, server.socket(), events);
      try {
        writeConfig(
            process,
            java.util.Map.of(
                "eventsFile",
                events.toString(),
                "failurePoint",
                failurePoint.name(),
                "workerCount",
                2));
        if (failurePoint == SourceProbeFactory.FailurePoint.ERROR_QUIESCE
            || failurePoint == SourceProbeFactory.FailurePoint.ERROR_CLOSE) {
          assertAttach(server.awaitMessage());
          assertTrue(server.awaitMessage().hasReady());
          server.send(
              PipelineToPlugin.newBuilder()
                  .setQuiesceSource(QuiesceSource.getDefaultInstance())
                  .build());
          if (failurePoint == SourceProbeFactory.FailurePoint.ERROR_CLOSE) {
            assertTrue(server.awaitMessage().hasSourceQuiesced());
            server.send(
                PipelineToPlugin.newBuilder().setShutdown(Shutdown.getDefaultInstance()).build());
          }
        }
        var result = awaitExit(process);
        assertEquals(1, result.exitCode());
        assertTrue(result.error().contains("java.lang.AssertionError: Fatal Source"));
        assertFalse(result.error().contains("Error escaped the SDK callback boundary"));
        var recorded = Files.readAllLines(events);
        assertEquals(
            failurePoint == SourceProbeFactory.FailurePoint.ERROR_CLOSE ? 1 : 0,
            recorded.stream().filter("source-close"::equals).count());
        if (failurePoint == SourceProbeFactory.FailurePoint.ERROR_QUIESCE) {
          assertEquals(1, recorded.stream().filter("source-quiesce"::equals).count());
        }
      } finally {
        stop(process);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = SinkProbeFactory.ErrorPoint.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = {"NONE"})
  void sinkFailuresRespectTheSynchronousCallbackBoundary(SinkProbeFactory.ErrorPoint errorPoint)
      throws Exception {
    var workingDirectory = directory.resolve("fatal-sink-callback");
    var events = directory.resolve("fatal-sink-callback-events.log");
    var channel = SideFixtures.flowChannel(workingDirectory, "main", 0);
    var queue = sinkQueue(workingDirectory, channel);
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    var bells = SideFixtures.Bells.create(workingDirectory, null, 0, java.util.List.of(channel));
    try (var server = LifecycleServer.start(directory.resolve("fatal-sink-callback.sock"));
        var egress = bells.writeEgress(queue, channel)) {
      var process = startProbe("sink", bells, server.socket(), events);
      try {
        writeConfig(
            process,
            java.util.Map.of("eventsFile", events.toString(), "errorPoint", errorPoint.name()));
        IpcQueue.WriteReceipt receipt = null;
        if (errorPoint != SinkProbeFactory.ErrorPoint.FACTORY
            && errorPoint != SinkProbeFactory.ErrorPoint.START) {
          assertAttach(server.awaitMessage());
          assertTrue(server.awaitMessage().hasReady());
          if (errorPoint != SinkProbeFactory.ErrorPoint.CLOSE) {
            var committed =
                (IpcQueue.Committed)
                    egress.tryWrite(
                        EgressRecord.newBuilder()
                            .setPayload(StringValue.of("command").toByteString())
                            .build()
                            .toByteArray());
            receipt = committed.receipt();
            awaitEvent(events, "sink-write-command");
          }
          if (errorPoint == SinkProbeFactory.ErrorPoint.CLOSE
              || errorPoint == SinkProbeFactory.ErrorPoint.WRITE_AFTER_STOP
              || errorPoint == SinkProbeFactory.ErrorPoint.ASYNC_WRITE_AFTER_STOP) {
            server.send(
                PipelineToPlugin.newBuilder().setShutdown(Shutdown.getDefaultInstance()).build());
          }
        }
        var result = awaitExit(process);
        var abandoned = errorPoint == SinkProbeFactory.ErrorPoint.ASYNC_WRITE_AFTER_STOP;
        assertEquals(abandoned ? 0 : 1, result.exitCode());
        assertEquals(
            !abandoned,
            result.error().contains("java.lang.AssertionError: Fatal Sink"),
            result.error());
        assertFalse(result.error().contains("Error escaped the SDK callback boundary"));
        var recorded = Files.readAllLines(events);
        assertEquals(
            errorPoint == SinkProbeFactory.ErrorPoint.CLOSE || abandoned ? 1 : 0,
            recorded.stream().filter("sink-close"::equals).count());
        if (errorPoint == SinkProbeFactory.ErrorPoint.WRITE_AFTER_STOP
            || errorPoint == SinkProbeFactory.ErrorPoint.ASYNC_WRITE_AFTER_STOP) {
          assertTrue(recorded.contains("sink-stop-observed"));
        }
        if (receipt != null) {
          assertFalse(egress.isReleased(receipt));
        }
      } finally {
        stop(process);
      }
    }
  }

  @ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"source", "sink", "source-and-sink"})
  void providerConstructionErrorCannotEscapeTheSdk(String pluginInterface) throws Exception {
    var workingDirectory = directory.resolve("provider-error");
    var events = directory.resolve("provider-error-events.log");
    Files.createDirectories(workingDirectory.resolve("source"));
    createSourceQueues(workingDirectory.resolve("source"));
    var hasSource = !pluginInterface.equals("sink");
    var hasSink = !pluginInterface.equals("source");
    var channel = hasSink ? SideFixtures.flowChannel(workingDirectory, "main", 0) : null;
    var bells =
        SideFixtures.Bells.create(
            workingDirectory,
            hasSource ? "main" : null,
            hasSource ? 1 : 0,
            channel == null ? java.util.List.of() : java.util.List.of(channel));
    try (var server = LifecycleServer.start(directory.resolve("provider-error.sock"))) {
      var process =
          startProbe(
              pluginInterface, bells, server.socket(), events, "-Dtenon.test.provider.error=true");
      try {
        if (pluginInterface.equals("source")) {
          writeConfig(process, events);
        } else {
          writeConfig(process, events);
        }
        var result = awaitExit(process);
        assertEquals(1, result.exitCode());
        assertTrue(result.error().contains("Fatal provider construction failure"), result.error());
        assertFalse(result.error().contains("Error escaped the SDK callback boundary"));
      } finally {
        stop(process);
      }
    }
  }

  private Process startProbe(
      String pluginInterface,
      SideFixtures.Bells bells,
      Path socket,
      Path events,
      String... vmOptions)
      throws IOException {
    var javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java");
    var command =
        new java.util.ArrayList<>(
            java.util.List.of(
                javaExecutable.toString(),
                "--enable-native-access=ALL-UNNAMED",
                "-D" + PluginProgramProbe.INTERFACE_PROPERTY + "=" + pluginInterface));
    command.addAll(java.util.List.of(vmOptions));
    command.addAll(
        java.util.List.of(
            "-cp",
            System.getProperty("java.class.path"),
            PluginProgramProbe.class.getName(),
            "--sdk-config",
            bells.sdkConfig(socket, LAUNCH_ID)));
    return new ProcessBuilder(command).start();
  }

  private static Path sinkQueue(Path workingDirectory, FlowChannel channel) throws IOException {
    var queue = EgressQueueLayout.queue(workingDirectory, channel);
    Files.createDirectories(queue.getParent());
    return queue;
  }

  private static void writeConfig(Process process, Path events) throws IOException {
    writeConfig(process, java.util.Map.of("eventsFile", events.toString()));
  }

  private static void writeConfig(Process process, java.util.Map<String, ?> values)
      throws IOException {
    var config = new JsonMapper().writeValueAsString(values);
    process.getOutputStream().write((config + "\n").getBytes(StandardCharsets.UTF_8));
    process.getOutputStream().flush();
  }

  private static void createSourceQueues(Path directory) throws Exception {
    createSourceQueues(directory, 1);
  }

  private static void createSourceQueues(Path directory, int parallelism) throws Exception {
    for (var index = 0; index < parallelism; index++) {
      IpcQueue.create(
          directory.resolve("submission-" + index + ".queue"),
          IpcQueueFormat.dataCapacityForRecordLimit(4, 1024),
          1024);
      IpcQueue.create(
          directory.resolve("completion-" + index + ".queue"),
          IpcQueueFormat.dataCapacityForRecordLimit(4, 13),
          13);
    }
  }

  private static void assertAttach(PluginToPipeline message) {
    assertTrue(message.hasAttach());
    assertArrayEquals(LAUNCH_ID, message.getAttach().getLaunchId().toByteArray());
  }

  private static IngressRecord awaitSubmission(IpcQueue.Reader submission) throws Exception {
    var deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      var record = submission.tryRead();
      if (record.isPresent()) {
        return IngressRecord.parseFrom(record.orElseThrow());
      }
      Thread.sleep(10);
    }
    throw new AssertionError("Source Program did not commit a Submission record");
  }

  private static void awaitRelease(IpcQueue.Writer writer, IpcQueue.WriteReceipt receipt)
      throws Exception {
    var deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      if (writer.isReleased(receipt)) {
        return;
      }
      Thread.sleep(10);
    }
    throw new AssertionError("Sink Program did not release the Egress record");
  }

  static void awaitEvent(Path events, String expected) throws Exception {
    var deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (System.nanoTime() < deadline) {
      if (Files.exists(events) && Files.readString(events).contains(expected)) {
        return;
      }
      Thread.sleep(10);
    }
    throw new AssertionError("Plugin Program did not record event " + expected);
  }

  private static ProbeResult awaitExit(Process process) throws Exception {
    assertTrue(process.waitFor(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "Probe did not exit");
    return new ProbeResult(
        process.exitValue(),
        process.getInputStream().readAllBytes(),
        new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
  }

  private static void stop(Process process) throws IOException, InterruptedException {
    process.destroyForcibly();
    assertTrue(process.waitFor(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "Probe was not reaped");
    process.getOutputStream().close();
  }

  private static byte[] sequence(int length) {
    var bytes = new byte[length];
    for (var index = 0; index < length; index++) {
      bytes[index] = (byte) index;
    }
    return bytes;
  }

  private record ProbeResult(int exitCode, byte[] output, String error) {}

  static final class LifecycleServer implements AutoCloseable {
    private final Path socket;
    private final EventLoopGroup boss;
    private final EventLoopGroup worker;
    private final Server server;
    private final Service service;

    private LifecycleServer(
        Path socket, EventLoopGroup boss, EventLoopGroup worker, Server server, Service service) {
      this.socket = socket;
      this.boss = boss;
      this.worker = worker;
      this.server = server;
      this.service = service;
    }

    static LifecycleServer start(Path socket) throws IOException {
      var boss = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
      var worker = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
      var service = new Service();
      try {
        var server =
            NettyServerBuilder.forAddress(UnixDomainSocketAddress.of(socket))
                .channelType(NioServerDomainSocketChannel.class)
                // Accepted Unix-domain sockets do not support gRPC's TCP-only default.
                .withChildOption(ChannelOption.SO_KEEPALIVE, null)
                .bossEventLoopGroup(boss)
                .workerEventLoopGroup(worker)
                .directExecutor()
                .intercept(
                    new ServerInterceptor() {
                      @Override
                      public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                          ServerCall<ReqT, RespT> call,
                          Metadata headers,
                          ServerCallHandler<ReqT, RespT> next) {
                        // Match the Pipeline's eager response headers. Tonic awaits
                        // them before returning its bidirectional response stream.
                        call.sendHeaders(new Metadata());
                        return next.startCall(
                            new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                              @Override
                              public void sendHeaders(Metadata responseHeaders) {
                                // This test service adds no response metadata.
                              }
                            },
                            headers);
                      }
                    })
                .addService(service)
                .build()
                .start();
        return new LifecycleServer(socket, boss, worker, server, service);
      } catch (IOException | RuntimeException | Error error) {
        boss.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
        worker.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
        throw error;
      }
    }

    Path socket() {
      return socket;
    }

    PluginToPipeline awaitMessage() throws Exception {
      return service.awaitMessage();
    }

    void send(PipelineToPlugin message) throws Exception {
      service.send(message);
    }

    void endResponseStream() throws Exception {
      service.endResponseStream();
    }

    boolean awaitClientHalfClose() throws InterruptedException {
      return service.clientHalfClose.await(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws IOException {
      server.shutdownNow();
      try {
        server.awaitTermination(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
      boss.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
      worker.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
      Files.deleteIfExists(socket);
    }
  }

  private static final class Service extends PluginLifecycleGrpc.PluginLifecycleImplBase {
    private final ArrayBlockingQueue<PluginToPipeline> messages = new ArrayBlockingQueue<>(3);
    private final CompletableFuture<StreamObserver<PipelineToPlugin>> response =
        new CompletableFuture<>();
    private final CompletableFuture<Throwable> failure = new CompletableFuture<>();
    private final CountDownLatch clientHalfClose = new CountDownLatch(1);

    @Override
    public StreamObserver<PluginToPipeline> run(StreamObserver<PipelineToPlugin> responseObserver) {
      response.complete(responseObserver);
      return new StreamObserver<>() {
        @Override
        public void onNext(PluginToPipeline message) {
          if (!messages.offer(message)) {
            failure.complete(new AssertionError("Plugin sent too many lifecycle messages"));
          }
        }

        @Override
        public void onError(Throwable error) {
          failure.complete(error);
        }

        @Override
        public void onCompleted() {
          clientHalfClose.countDown();
          responseObserver.onCompleted();
        }
      };
    }

    PluginToPipeline awaitMessage() throws Exception {
      var message = messages.poll(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      if (message != null) {
        return message;
      }
      if (failure.isDone()) {
        throw new AssertionError("Plugin lifecycle stream failed", failure.join());
      }
      throw new AssertionError("Plugin did not send the expected lifecycle message");
    }

    void send(PipelineToPlugin message) throws Exception {
      response.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).onNext(message);
    }

    void endResponseStream() throws Exception {
      response.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS).onCompleted();
    }
  }
}
