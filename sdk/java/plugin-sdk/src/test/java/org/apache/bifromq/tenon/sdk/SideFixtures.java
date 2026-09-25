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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bifromq.tenon.sdk.ipc.BellRegion;
import org.apache.bifromq.tenon.sdk.ipc.BellRegionTestSupport;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueue;
import org.apache.bifromq.tenon.sdk.ipc.IpcQueueFormatException;
import org.apache.bifromq.tenon.sdk.ipc.LoopBell;

/**
 * Creates the Bell Regions a Core runtime creates before it launches a Side, and opens the
 * endpoints of the Channel loops one test plays.
 *
 * <p>Every region a Side rings lives in a file, so a fixture that reaches the same two addresses
 * the real Instance and the real Channel loop use exercises the production wiring rather than a
 * substitute for it.
 */
final class SideFixtures {
  /** The fixed name of the Region a Flow's Channel loops park in. */
  static final String CHANNELS_BELL_FILE_NAME = "channels.bells";

  /** How many own-loop doorbells a Source needs: one Submission loop and one Completion loop. */
  private static final int SOURCE_LOOP_SLOTS = 2;

  private SideFixtures() {}

  /** Creates one Region file together with the directories above it. */
  static void createRegion(Path path, int slots) throws IOException {
    Files.createDirectories(path.getParent());
    BellRegionTestSupport.create(path, slots, 0);
  }

  /** Creates and opens one Region file. */
  static BellRegion region(Path path, int slots) throws IOException {
    createRegion(path, slots);
    return BellRegion.open(path);
  }

  /** The Region record the Pipeline hands one Source or Sink for one Flow. */
  static Path flowBellPath(Path directory, String flowId) {
    return directory.resolve("flows").resolve(flowId).resolve(CHANNELS_BELL_FILE_NAME);
  }

  /** One Sink input whose Flow Region this fixture places under {@code directory}. */
  static FlowChannel flowChannel(Path directory, String flowId, int channelId) {
    return new FlowChannel(flowId, channelId);
  }

  /** The Bell Regions one Source fixture creates, and the endpoints its Channel loops ring. */
  static final class SourceBells {
    private final Path directory;
    private final Path channelsPath;
    private final BellRegion loops;
    private final BellRegion channels;

    private SourceBells(Path directory, Path channelsPath, BellRegion loops, BellRegion channels) {
      this.directory = directory;
      this.channelsPath = channelsPath;
      this.loops = loops;
      this.channels = channels;
    }

    /**
     * Creates the two Regions a Core runtime creates before it launches a Source.
     *
     * <p>A Source's own directory holds its Queue pairs and its own-loop Region, and nothing else,
     * so the Flow's Region belongs outside it exactly as the Pipeline places it. The Source's own
     * Region holds one slot per waiting loop, not per Channel.
     */
    static SourceBells create(Path directory, int channels) throws IOException {
      var loops = region(directory.resolve(BellRegion.LOOPS_BELL_FILE_NAME), SOURCE_LOOP_SLOTS);
      var channelsPath =
          directory
              .resolveSibling(directory.getFileName() + "-flow")
              .resolve(CHANNELS_BELL_FILE_NAME);
      return new SourceBells(directory, channelsPath, loops, region(channelsPath, channels));
    }

    /** Returns the Flow Region path the Pipeline hands over as the Channel doorbell record. */
    Path channelsPath() {
      return channelsPath;
    }

    /** Returns the Region this Source's own loops park in. */
    Path loopsPath() {
      return directory.resolve(BellRegion.LOOPS_BELL_FILE_NAME);
    }

    /** Opens one Submission Queue the way its Flow Channel loop does. */
    IpcQueue.Reader readSubmission(int channel) throws IOException, IpcQueueFormatException {
      return IpcQueue.openReader(
          directory.resolve("submission-" + channel + ".queue"), channels.loopBell(channel), loops);
    }

    /** Opens one Completion Queue the way its Flow Channel loop does. */
    IpcQueue.Writer writeCompletion(int channel) throws IOException, IpcQueueFormatException {
      return IpcQueue.openWriter(
          directory.resolve("completion-" + channel + ".queue"), channels.loopBell(channel), loops);
    }
  }

  /** The Bell Regions one Sink fixture creates, and the endpoints its Channel loops ring. */
  static final class SinkBells {
    private final BellRegion loops;
    private final Map<String, BellRegion> flows;

    private SinkBells(BellRegion loops, Map<String, BellRegion> flows) {
      this.loops = loops;
      this.flows = flows;
    }

    /** Creates the Regions a Core runtime creates before it launches a Sink. */
    static SinkBells create(Path directory, List<FlowChannel> channels) throws IOException {
      var loops = region(directory.resolve("sink").resolve(BellRegion.LOOPS_BELL_FILE_NAME), 1);
      var slotCounts = new HashMap<String, Integer>();
      for (var channel : channels) {
        slotCounts.merge(channel.flowId(), channel.channelId() + 1, Math::max);
      }
      var flows = new HashMap<String, BellRegion>();
      for (var entry : slotCounts.entrySet()) {
        var path = flowBellPath(directory, entry.getKey());
        flows.put(entry.getKey(), region(path, entry.getValue()));
      }
      return new SinkBells(loops, flows);
    }

    /** Opens one Egress Queue the way its Flow Channel loop does. */
    IpcQueue.Writer write(Path queue, FlowChannel channel)
        throws IOException, IpcQueueFormatException {
      return IpcQueue.openWriter(
          queue, flows.get(channel.flowId()).loopBell(channel.channelId()), loops);
    }

    /** Returns the doorbell of the Sink's single Egress loop. */
    LoopBell sinkLoop() throws IOException {
      return loops.loopBell(0);
    }

    /** Opens one Egress Queue the way the Sink's single Egress loop does. */
    IpcQueue.Reader read(Path queue, FlowChannel channel)
        throws IOException, IpcQueueFormatException {
      return IpcQueue.openReader(queue, sinkLoop(), flows.get(channel.flowId()));
    }
  }

  /**
   * Creates the Bell Regions one launch needs, and opens the Channel-loop endpoints a test plays.
   *
   * <p>A Core runtime creates every one of these before it starts any process: each Flow's Channel
   * Region, where that Flow's Channel loops park, and the launched Interface's own-loop Region. One
   * Flow's Channels share one Region, so the Source that writes into them and every Sink that reads
   * them ring the same file, exactly as the Pipeline places it beside the Instance directories.
   */
  static final class Bells {
    private final Path workingDirectory;
    private final String sourceFlow;
    private final List<FlowChannel> inputs;
    private final Map<String, BellRegion> flows;
    private final BellRegion sourceLoops;
    private final BellRegion sinkLoops;

    private Bells(
        Path workingDirectory,
        String sourceFlow,
        List<FlowChannel> inputs,
        Map<String, BellRegion> flows,
        BellRegion sourceLoops,
        BellRegion sinkLoops) {
      this.workingDirectory = workingDirectory;
      this.sourceFlow = sourceFlow;
      this.inputs = inputs;
      this.flows = flows;
      this.sourceLoops = sourceLoops;
      this.sinkLoops = sinkLoops;
    }

    /**
     * Creates the Regions of one launched Interface.
     *
     * @param sourceFlow the Flow a launched Source writes into, or {@code null} without a Source
     * @param sourceChannels how many Channels that Flow has
     * @param sinkInputs the identity of every Sink input, in startup order
     */
    static Bells create(
        Path workingDirectory, String sourceFlow, int sourceChannels, List<FlowChannel> sinkInputs)
        throws IOException {
      var slotCounts = new HashMap<String, Integer>();
      if (sourceFlow != null) {
        slotCounts.put(sourceFlow, sourceChannels);
      }
      for (var input : sinkInputs) {
        slotCounts.merge(input.flowId(), input.channelId() + 1, Math::max);
      }
      var flows = new HashMap<String, BellRegion>();
      for (var entry : slotCounts.entrySet()) {
        flows.put(
            entry.getKey(),
            region(flowBellPath(workingDirectory, entry.getKey()), entry.getValue()));
      }
      return new Bells(
          workingDirectory,
          sourceFlow,
          List.copyOf(sinkInputs),
          flows,
          sourceFlow == null
              ? null
              : region(
                  workingDirectory.resolve("source").resolve(BellRegion.LOOPS_BELL_FILE_NAME),
                  SOURCE_LOOP_SLOTS),
          sinkInputs.isEmpty()
              ? null
              : region(
                  workingDirectory.resolve("sink").resolve(BellRegion.LOOPS_BELL_FILE_NAME), 1));
    }

    /** Returns the working directory the launched Interface runs in. */
    Path workingDirectory() {
      return workingDirectory;
    }

    /** Returns the Sink inputs handed to the launched Interface, in startup order. */
    List<FlowChannel> inputs() {
      return inputs;
    }

    /** Returns the Flow Region a launched Source is handed, or {@code null} without a Source. */
    Path sourceChannelBellPath() {
      return sourceFlow == null ? null : flowBellPath(workingDirectory, sourceFlow);
    }

    /** Builds the one startup document the Pipeline appends for this launch. */
    String sdkConfig(Path socket, byte[] launchId) {
      return SideFixtures.sdkConfig(
          workingDirectory, socket, launchId, sourceChannelBellPath(), inputs);
    }

    /** Opens one Submission Queue the way its Flow's Channel loop does. */
    IpcQueue.Reader readSubmission(int channel) throws IOException, IpcQueueFormatException {
      return IpcQueue.openReader(
          workingDirectory.resolve("source").resolve("submission-" + channel + ".queue"),
          flows.get(sourceFlow).loopBell(channel),
          sourceLoops);
    }

    /** Opens one Completion Queue the way its Flow's Channel loop does. */
    IpcQueue.Writer writeCompletion(int channel) throws IOException, IpcQueueFormatException {
      return IpcQueue.openWriter(
          workingDirectory.resolve("source").resolve("completion-" + channel + ".queue"),
          flows.get(sourceFlow).loopBell(channel),
          sourceLoops);
    }

    /** Opens one Egress Queue the way its Flow's Channel loop does. */
    IpcQueue.Writer writeEgress(Path queue, FlowChannel input)
        throws IOException, IpcQueueFormatException {
      return IpcQueue.openWriter(
          queue, flows.get(input.flowId()).loopBell(input.channelId()), sinkLoops);
    }
  }

  /**
   * Builds the one startup document the Pipeline appends for a launched Side.
   *
   * <p>A direction the Interface does not include is absent from the document rather than present
   * with a placeholder, so the Program's own startup check decides what it implements.
   */
  static String sdkConfig(
      Path workingDirectory,
      Path socket,
      byte[] launchId,
      Path sourceChannelBellPath,
      List<FlowChannel> sinkInputs) {
    var json = new StringBuilder("{\"workingDirectory\":");
    json.append(quoted(workingDirectory.toString()));
    json.append(",\"controlSocket\":").append(quoted(socket.toString()));
    json.append(",\"launchId\":").append(quoted(Base64.getEncoder().encodeToString(launchId)));
    if (sourceChannelBellPath != null) {
      json.append(",\"sourceChannelRegion\":").append(quoted(sourceChannelBellPath.toString()));
    }
    if (!sinkInputs.isEmpty()) {
      json.append(",\"sinkInputs\":[");
      for (var index = 0; index < sinkInputs.size(); index++) {
        var input = sinkInputs.get(index);
        if (index > 0) {
          json.append(',');
        }
        json.append("{\"flowId\":")
            .append(quoted(input.flowId()))
            .append(",\"channelId\":")
            .append(input.channelId())
            .append(",\"channelBellPath\":")
            .append(quoted(flowBellPath(workingDirectory, input.flowId()).toString()))
            .append('}');
      }
      json.append(']');
    }
    return json.append('}').toString();
  }

  private static String quoted(String value) {
    return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
