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

import ${package}.payload.SinkRecordPayload;
import ${package}.payload.SourceRecordPayload;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.bifromq.tenon.sdk.AckCode;
import org.apache.bifromq.tenon.sdk.FlowChannel;
import org.apache.bifromq.tenon.sdk.Ingress;
import org.apache.bifromq.tenon.sdk.PayloadSender;
import org.apache.bifromq.tenon.sdk.TenonSource;
import org.apache.bifromq.tenon.sdk.TenonSourceAndSink;
import org.apache.bifromq.tenon.sdk.TenonSourceAndSinkFactory;
import tools.jackson.databind.JsonNode;

/** Replace this example with one owner of the shared external connection. */
public final class SourceAndSinkPluginFactory
    implements TenonSourceAndSinkFactory<SourceRecordPayload, SinkRecordPayload> {
  @Override
  public TenonSourceAndSink<SinkRecordPayload> create(
      JsonNode config,
      Optional<Ingress<SourceRecordPayload>> source,
      Set<FlowChannel> egressChannels)
      throws IOException {
    var parsed =
        PluginConfig.parse(
            config,
            source.map(Ingress::parallelism).orElse(0),
            source.isPresent(),
            !egressChannels.isEmpty());
    return new PluginOwner(
        parsed, source.map(Ingress::sender).orElse(null), !egressChannels.isEmpty());
  }

  private static final class PluginOwner implements TenonSourceAndSink<SinkRecordPayload> {
    private final PluginSource source;
    private final FileChannel output;

    private PluginOwner(
        PluginConfig config, PayloadSender<SourceRecordPayload> sender, boolean sinkBound)
        throws IOException {
      this.output = sinkBound ? openOutput(config.outputFile()) : null;
      this.source = sender == null ? null : new PluginSource(config, sender);
    }

    @Override
    public void start() {
      if (source != null) source.start();
    }

    @Override
    public void quiesce() {
      if (source != null) source.quiesce();
    }

    @Override
    public void close() {
      if (source != null) source.close();
      // Close the shared connection only here, during final Shutdown.
      try {
        if (output != null) {
          output.close();
        }
      } catch (IOException error) {
        throw new UncheckedIOException(error);
      }
    }

    @Override
    public synchronized CompletionStage<Void> write(
        FlowChannel channel, List<SinkRecordPayload> records) {
      try {
        if (output == null) {
          throw new IllegalStateException("Sink direction is not bound");
        }
        writeMessages(output, records);
        return CompletableFuture.completedFuture(null);
      } catch (IOException error) {
        return CompletableFuture.failedFuture(error);
      }
    }
  }

  private static final class PluginSource implements TenonSource {
    private final PluginConfig config;
    private final PayloadSender<SourceRecordPayload> sender;

    private PluginSource(PluginConfig config, PayloadSender<SourceRecordPayload> sender) {
      this.config = config;
      this.sender = sender;
    }

    @Override
    public void start() {
      var payload = SourceRecordPayload.newBuilder().setMessage(config.message()).build();
      sender.send(config.queueIndex(), payload).thenAccept(this::handleAcknowledgement);
    }

    @Override
    public void quiesce() {
      // This one-shot example has no further production to stop; Sink keeps the shared owner alive.
    }

    @Override
    public void close() {
      // Release only this producer's resources after the SDK has ended its pending results.
    }

    private void handleAcknowledgement(AckCode acknowledgement) {
      switch (acknowledgement) {
        case OK -> {}
        case RETRY -> System.err.println("Example Source payload should be retried");
        case BACKPRESSURE ->
            System.err.println("Example Source should pause and retry the payload later");
        case ERROR -> System.err.println("Example Source payload was rejected");
      }
    }
  }

  private record PluginConfig(String message, int queueIndex, Path outputFile) {
    private static PluginConfig parse(
        JsonNode config, int parallelism, boolean sourceBound, boolean sinkBound)
        throws IOException {
      var queueIndex = sourceBound ? config.required("queueIndex").intValue() : 0;
      if (sourceBound && queueIndex >= parallelism) {
        throw new IllegalArgumentException("queueIndex must be less than Flow parallelism");
      }
      var message = sourceBound ? config.required("message").stringValue() : "";
      var outputFile =
          sinkBound ? Path.of(config.required("outputFile").stringValue()) : Path.of("/");
      if (sinkBound && !outputFile.isAbsolute()) {
        throw new IOException("outputFile must be an absolute path");
      }
      return new PluginConfig(message, queueIndex, outputFile);
    }
  }

  private static FileChannel openOutput(Path outputFile) throws IOException {
    return FileChannel.open(
        outputFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
  }

  private static void writeMessages(FileChannel output, List<SinkRecordPayload> records)
      throws IOException {
    var text = new StringBuilder();
    records.forEach(record -> text.append(record.getMessage()).append('\n'));
    ByteBuffer bytes = StandardCharsets.UTF_8.encode(text.toString());
    while (bytes.hasRemaining()) {
      output.write(bytes);
    }
    output.force(true);
  }
}
