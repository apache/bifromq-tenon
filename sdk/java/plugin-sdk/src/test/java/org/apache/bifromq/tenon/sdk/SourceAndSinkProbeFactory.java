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

import com.google.protobuf.StringValue;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import tools.jackson.databind.JsonNode;

/** Provides the single shared business owner used by the source-and-sink child-process test. */
public final class SourceAndSinkProbeFactory
    implements TenonSourceAndSinkFactory<StringValue, StringValue> {
  private static final AtomicBoolean DEVICE_OPEN = new AtomicBoolean();

  public SourceAndSinkProbeFactory() {
    if (Boolean.getBoolean("tenon.test.provider.error")) {
      throw new AssertionError("Fatal provider construction failure");
    }
  }

  @Override
  public TenonSourceAndSink<StringValue> create(
      JsonNode config, Optional<Ingress<StringValue>> ingress, Set<FlowChannel> egressChannels) {
    var sender = ingress.map(Ingress::sender).orElse(null);
    if (ingress.isPresent() && ingress.get().parallelism() != 1) {
      throw new IllegalArgumentException("Source-and-Sink probe requires exactly one Queue");
    }
    if (!DEVICE_OPEN.compareAndSet(false, true)) {
      throw new IllegalStateException("Source-and-Sink probe device can only be opened once");
    }
    var events = ProbeEvents.path(config);
    ProbeEvents.append(events, "owner-create");
    var owner =
        new SharedOwner(
            events,
            sender,
            config.has("failurePoint")
                ? FailurePoint.valueOf(config.required("failurePoint").stringValue())
                : FailurePoint.NONE);
    return owner;
  }

  enum FailurePoint {
    NONE,
    START,
    SOURCE_START,
    SOURCE_START_AND_CLOSE,
    WRITE
  }

  private static final class SharedOwner implements TenonSourceAndSink<StringValue> {
    private final Path events;
    private final PayloadSender<StringValue> sender;
    private final FailurePoint failurePoint;
    private final SourceProbeFactory.SourceProbe source;
    private Thread startingResult;

    private SharedOwner(Path events, PayloadSender<StringValue> sender, FailurePoint failurePoint) {
      this.events = events;
      this.sender = sender;
      this.failurePoint = failurePoint;
      if (sender != null) ProbeEvents.append(events, "source-create");
      this.source =
          sender == null
              ? null
              : new SourceProbeFactory.SourceProbe(
                  events,
                  sender,
                  1,
                  switch (failurePoint) {
                    case SOURCE_START -> SourceProbeFactory.FailurePoint.START;
                    case SOURCE_START_AND_CLOSE -> SourceProbeFactory.FailurePoint.START_AND_CLOSE;
                    default -> SourceProbeFactory.FailurePoint.NONE;
                  });
    }

    @Override
    public void start() {
      ProbeEvents.append(events, "owner-start");
      if (failurePoint == FailurePoint.START) {
        var result = sender.send(0, StringValue.of("startup-telemetry")).toCompletableFuture();
        startingResult =
            Thread.ofPlatform()
                .name("shared-start-result")
                .start(
                    () -> {
                      try {
                        result.join();
                      } catch (CompletionException expected) {
                        ProbeEvents.append(events, "owner-result-failed");
                      }
                    });
        throw new IllegalStateException("Expected shared start failure");
      }
      if (source != null) source.start();
    }

    @Override
    public void quiesce() {
      if (source != null) source.quiesce();
    }

    @Override
    public void close() {
      try {
        if (source != null) source.close();
        if (startingResult != null) {
          try {
            startingResult.join();
          } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(error);
          }
        }
      } finally {
        ProbeEvents.append(events, "owner-close");
        DEVICE_OPEN.set(false);
      }
    }

    @Override
    public CompletionStage<Void> write(FlowChannel channel, List<StringValue> records) {
      ProbeEvents.append(events, "sink-write-" + records.getFirst().getValue());
      if (failurePoint == FailurePoint.WRITE) {
        return CompletableFuture.failedFuture(new IOException("Expected shared Sink failure"));
      }
      return CompletableFuture.completedFuture(null);
    }
  }
}
