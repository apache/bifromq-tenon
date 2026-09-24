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

import com.google.protobuf.StringValue;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueue;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class SinkProgramOwnerContractTest {
  @TempDir Path directory;

  @Test
  void plannedShutdownStartsAndClosesTheBusinessOwnerExactlyOnce() throws Exception {
    var sink = new RecordingSink();
    var owner = owner(sink);
    sink.start();
    owner.startPaused();
    owner.activate();
    owner.shutdown();
    sink.close();

    assertEquals(1, sink.startCount);
    assertEquals(1, sink.closeCount);
  }

  @Test
  void ownerDoesNotInvokeBusinessLifecycleCallbacks() throws Exception {
    var sink = new RecordingSink();
    var owner = owner(sink);
    owner.startPaused();
    owner.activate();
    owner.shutdown();
    assertEquals(0, sink.startCount);
    assertEquals(0, sink.closeCount);
  }

  private SinkProgramOwner<StringValue> owner(RecordingSink sink) throws Exception {
    return owner(
        sink,
        directory,
        new PrintStream(OutputStream.nullOutputStream(), true, StandardCharsets.UTF_8));
  }

  private SinkProgramOwner<StringValue> owner(RecordingSink sink, Path root, PrintStream output)
      throws Exception {
    var channel = SideFixtures.flowChannel(root, "test", 0);
    var queue = EgressQueueLayout.queue(root, channel);
    Files.createDirectories(queue.getParent());
    var capacity = IpcQueueFormat.DataCapacity.of(4096);
    IpcQueue.create(queue, capacity, capacity.bytes() - IpcQueueFormat.FRAME_HEADER_LENGTH);
    SideFixtures.createRegion(
        root.resolve("sink")
            .resolve(org.apache.bifromq.tenon.sdk.ipc.BellRegion.LOOPS_BELL_FILE_NAME),
        1);
    SideFixtures.createRegion(SideFixtures.flowBellPath(root, channel.flowId()), 1);
    return SinkProgramOwner.open(
        sink,
        root,
        List.of(new SinkInput(channel, SideFixtures.flowBellPath(root, channel.flowId()))),
        StringValue.parser());
  }

  private static class RecordingSink implements TenonSink<StringValue> {
    private int startCount;
    private int closeCount;

    @Override
    public void start() {
      startCount++;
    }

    @Override
    public CompletionStage<Void> write(FlowChannel channel, List<StringValue> records) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
      closeCount++;
    }
  }
}
