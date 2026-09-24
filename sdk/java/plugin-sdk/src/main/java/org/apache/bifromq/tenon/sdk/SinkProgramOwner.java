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

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import org.apache.bifromq.tenon.sdk.ipc.BellRegion;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueue;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormatException;

/** Owns one Sink-capable business lifecycle and the Instance's single Egress loop. */
final class SinkProgramOwner<P extends MessageLite> {
  private final SinkCoordinator<P> coordinator;
  private final List<BellRegion> regions;
  private final CompletableFuture<Void> failure;
  private volatile Phase phase = Phase.PREPARED;

  private SinkProgramOwner(
      SinkCoordinator<P> coordinator, List<BellRegion> regions, CompletableFuture<Void> failure) {
    this.coordinator = coordinator;
    this.regions = List.copyOf(regions);
    this.failure = failure;
  }

  /** Opens the existing Egress Queues for one standalone Sink business object. */
  static <P extends MessageLite> SinkProgramOwner<P> open(
      TenonSink<P> sink, Path workingDirectory, List<SinkInput> channels, Parser<P> payloadParser)
      throws IOException, IpcQueueFormatException {
    Objects.requireNonNull(sink, "sink");
    return open(sink::write, workingDirectory, channels, payloadParser);
  }

  /** Opens the existing Egress Queues for one source-and-sink shared business owner. */
  static <P extends MessageLite> SinkProgramOwner<P> open(
      TenonSourceAndSink<P> owner,
      Path workingDirectory,
      List<SinkInput> channels,
      Parser<P> payloadParser)
      throws IOException, IpcQueueFormatException {
    Objects.requireNonNull(owner, "owner");
    return open(owner::write, workingDirectory, channels, payloadParser);
  }

  private static <P extends MessageLite> SinkProgramOwner<P> open(
      BiFunction<FlowChannel, List<P>, CompletionStage<Void>> writer,
      Path workingDirectory,
      List<SinkInput> channels,
      Parser<P> payloadParser)
      throws IOException {
    var readers = new ArrayList<IpcQueue.Reader>(channels.size());
    var regions = new ArrayList<BellRegion>();
    try {
      var loops =
          openRegion(
              workingDirectory.resolve("sink").resolve(BellRegion.LOOPS_BELL_FILE_NAME), regions);
      // One loop reads every Egress Queue of this Instance, so every reader publishes the same
      // slot ordinal: a doorbell belongs to the loop that parks on it, not to a Queue.
      var bell = loops.loopBell(0);
      var inputs = new ArrayList<SinkCoordinator.Input<P>>(channels.size());
      var failure = new CompletableFuture<Void>();
      for (var input : channels) {
        var channel = input.channel();
        var peerRegion = openRegion(input.channelBellPath(), regions);
        var reader =
            IpcQueue.openReader(
                EgressQueueLayout.queue(workingDirectory, channel), bell, peerRegion);
        readers.add(reader);
        inputs.add(new SinkCoordinator.Input<>(reader, records -> writer.apply(channel, records)));
      }
      var coordinator =
          new SinkCoordinator<>(bell, inputs, payloadParser, failure::completeExceptionally);
      return new SinkProgramOwner<>(coordinator, regions, failure);
    } catch (IOException | RuntimeException error) {
      readers.forEach(IpcQueue.Reader::close);
      closeRegions(regions);
      throw error;
    }
  }

  private static BellRegion openRegion(Path path, List<BellRegion> regions) throws IOException {
    var region = BellRegion.open(path);
    regions.add(region);
    return region;
  }

  private static void closeRegions(List<BellRegion> regions) {
    for (var region : regions.reversed()) {
      region.close();
    }
  }

  /** Starts the business owner and coordinator without allowing Queue reads before Ready. */
  void startPaused() {
    requirePhase(Phase.PREPARED);
    coordinator.startPaused();
    phase = Phase.PAUSED;
  }

  /** Allows Queue reads only after the Program has published Ready. */
  void activate() {
    requirePhase(Phase.PAUSED);
    coordinator.activate();
    phase = Phase.RUNNING;
  }

  /** Stops Queue admission, waits for the coordinator, and closes the business owner once. */
  void shutdown() throws Exception {
    requirePhase(Phase.RUNNING);
    stopWorkers();
    phase = Phase.STOPPED;
  }

  CompletionStage<Void> failure() {
    return failure;
  }

  void stopWorkers() throws Exception {
    requirePhase(Phase.RUNNING);
    phase = Phase.STOPPING;
    // Stop the loop before a blocked write body can delay the join that follows it.
    coordinator.requestStop();
    coordinator.awaitTermination();
    coordinator.close();
    closeRegions(regions);
  }

  private void requirePhase(Phase expected) {
    if (phase != expected) {
      throw new IllegalStateException(
          "Sink Program expected phase " + expected + " but was " + phase);
    }
  }

  private enum Phase {
    PREPARED,
    PAUSED,
    RUNNING,
    STOPPING,
    STOPPED
  }
}
