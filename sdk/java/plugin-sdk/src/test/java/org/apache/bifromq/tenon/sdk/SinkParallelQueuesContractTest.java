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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.StringValue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.tenon.contracts.sink.EgressRecordOuterClass.EgressRecord;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueue;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SinkParallelQueuesContractTest {
  @TempDir Path directory;

  @Test
  void anUnfinishedWriteOnOneFlowDoesNotHoldAnotherFlowsReleaseBack() throws Exception {
    var first = SideFixtures.flowChannel(directory, "first/flow", 0);
    var second = SideFixtures.flowChannel(directory, "second/flow", 0);
    var channels = List.of(first, second);
    var bells = createQueues(channels);
    var sink = new HeldResultSink();
    var owner =
        open(
            sink,
            channels,
            (thread, error) -> {
              throw new AssertionError(error);
            });
    try (var firstWriter = bells.write(EgressQueueLayout.queue(directory, first), first);
        var secondWriter = bells.write(EgressQueueLayout.queue(directory, second), second)) {
      var firstReceipt = write(firstWriter, "first payload");
      var secondReceipt = write(secondWriter, "second payload");
      sink.start();
      owner.startPaused();
      owner.activate();

      // One loop serves both Queues, and the first Flow's unfinished result only holds its own
      // Queue back.
      assertTrue(sink.served.await(2, TimeUnit.SECONDS));
      assertEquals(
          1, sink.servedBy.size(), "Both Queues must be served by this Instance's one Egress loop");
      assertEquals("first payload", sink.records.get(first).getFirst().getValue());
      assertEquals("second payload", sink.records.get(second).getFirst().getValue());
      assertFalse(firstWriter.isReleased(firstReceipt));
      assertFalse(secondWriter.isReleased(secondReceipt));

      sink.results.get(second).complete(null);
      awaitReleased(secondWriter, secondReceipt);
      assertFalse(firstWriter.isReleased(firstReceipt));

      sink.results.get(first).complete(null);
      awaitReleased(firstWriter, firstReceipt);
      owner.shutdown();
      sink.close();
      assertEquals(1, sink.closeCount);
    }
    assertEquals(1, sink.startCount);
  }

  @Test
  void shutdownStopsTheLoopBeforeWaitingForABlockedWrite() throws Exception {
    var first = SideFixtures.flowChannel(directory, "flow", 0);
    var second = SideFixtures.flowChannel(directory, "flow", 1);
    var channels = List.of(first, second);
    var bells = createQueues(channels);
    var sink = new BlockedBodySink();
    var owner = open(sink, channels, (thread, error) -> {});
    var finished = new CompletableFuture<Void>();
    Thread shutdown = null;
    try (var firstWriter = bells.write(EgressQueueLayout.queue(directory, first), first);
        var secondWriter = bells.write(EgressQueueLayout.queue(directory, second), second)) {
      write(firstWriter, "blocked");
      sink.start();
      owner.startPaused();
      owner.activate();
      await(() -> sink.records.containsKey(first));
      shutdown =
          Thread.ofPlatform()
              .start(
                  () -> {
                    try {
                      owner.shutdown();
                      finished.complete(null);
                    } catch (Throwable error) {
                      finished.completeExceptionally(error);
                    }
                  });
      var shutdownThread = shutdown;
      await(() -> shutdownThread.getState() == Thread.State.WAITING);
      write(secondWriter, "must remain unread");
      assertFalse(sink.entered.await(100, TimeUnit.MILLISECONDS));
      assertFalse(sink.records.containsKey(second));
      assertFalse(finished.isDone());
      sink.returnFromWrite.countDown();
      finished.get(2, TimeUnit.SECONDS);
      sink.close();
      assertEquals(1, sink.closeCount);
    } finally {
      sink.returnFromWrite.countDown();
      if (shutdown != null) {
        shutdown.join();
      }
    }
  }

  private SideFixtures.SinkBells createQueues(List<FlowChannel> channels) throws Exception {
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    for (var channel : channels) {
      var queue = EgressQueueLayout.queue(directory, channel);
      Files.createDirectories(queue.getParent());
      IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    }
    return SideFixtures.SinkBells.create(directory, channels);
  }

  private SinkProgramOwner<StringValue> open(
      TenonSink<StringValue> sink,
      List<FlowChannel> channels,
      Thread.UncaughtExceptionHandler ignored)
      throws Exception {
    return SinkProgramOwner.open(
        sink,
        directory,
        channels.stream()
            .map(
                channel ->
                    new SinkInput(channel, SideFixtures.flowBellPath(directory, channel.flowId())))
            .toList(),
        StringValue.parser());
  }

  private static IpcQueue.WriteReceipt write(IpcQueue.Writer writer, String payload)
      throws Exception {
    var bytes =
        EgressRecord.newBuilder()
            .setPayload(StringValue.of(payload).toByteString())
            .build()
            .toByteArray();
    return assertInstanceOf(IpcQueue.Committed.class, writer.tryWrite(bytes)).receipt();
  }

  private static void awaitReleased(IpcQueue.Writer writer, IpcQueue.WriteReceipt receipt)
      throws Exception {
    await(() -> writer.isReleased(receipt));
  }

  private static void await(CheckedCondition condition) throws Exception {
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (!condition.test()) {
      assertTrue(System.nanoTime() < deadline, "Timed out waiting for Queue progress");
      Thread.sleep(1);
    }
  }

  @FunctionalInterface
  private interface CheckedCondition {
    boolean test() throws Exception;
  }

  /** Records every batch and returns a result the test finishes by hand. */
  private static final class HeldResultSink implements TenonSink<StringValue> {
    private final CountDownLatch served = new CountDownLatch(2);
    private final Set<Thread> servedBy = ConcurrentHashMap.newKeySet();
    private final Map<FlowChannel, List<StringValue>> records = new ConcurrentHashMap<>();
    private final Map<FlowChannel, CompletableFuture<Void>> results = new ConcurrentHashMap<>();
    private int startCount;
    private int closeCount;

    @Override
    public void start() {
      startCount++;
    }

    @Override
    public CompletionStage<Void> write(FlowChannel channel, List<StringValue> batch) {
      records.put(channel, batch);
      servedBy.add(Thread.currentThread());
      var result = new CompletableFuture<Void>();
      results.put(channel, result);
      served.countDown();
      return result;
    }

    @Override
    public void close() {
      closeCount++;
    }
  }

  /** Blocks the write method body until the test releases it. */
  private static final class BlockedBodySink implements TenonSink<StringValue> {
    private final CountDownLatch entered = new CountDownLatch(2);
    private final CountDownLatch returnFromWrite = new CountDownLatch(1);
    private final Map<FlowChannel, List<StringValue>> records = new ConcurrentHashMap<>();
    private int startCount;
    private int closeCount;

    @Override
    public void start() {
      startCount++;
    }

    @Override
    public CompletionStage<Void> write(FlowChannel channel, List<StringValue> batch) {
      records.put(channel, batch);
      entered.countDown();
      try {
        returnFromWrite.await();
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        return CompletableFuture.failedFuture(error);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
      closeCount++;
    }
  }
}
