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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import tools.jackson.databind.JsonNode;

/** Owns one source-and-sink Program with one control stream and one shared business owner. */
public final class SourceAndSinkProgram<S extends MessageLite, T extends MessageLite> {
  private final PluginProgramRuntime runtime;
  private final SourceSession<S> sourceSession;
  private final TenonSourceAndSink<T> business;
  private final SinkProgramOwner<T> sinkOwner;

  private SourceAndSinkProgram(
      PluginProgramRuntime runtime,
      SourceSession<S> sourceSession,
      TenonSourceAndSink<T> business,
      SinkProgramOwner<T> sinkOwner) {
    this.runtime = Objects.requireNonNull(runtime, "runtime");
    this.sourceSession = sourceSession;
    this.business = Objects.requireNonNull(business, "business");
    this.sinkOwner = sinkOwner;
  }

  /**
   * Attaches control, creates the shared owner once, and opens both interfaces without Ready.
   *
   * @param arguments the exact reserved arguments received by the Plugin entry point
   * @param sinkPayloadParser the generated parser for this Plugin's SinkRecordPayload
   * @return the Program whose shared owner lifecycle is owned by the SDK
   */
  public static <S extends MessageLite, T extends MessageLite> SourceAndSinkProgram<S, T> run(
      String[] arguments, Parser<T> sinkPayloadParser) {
    try {
      return run0(arguments, sinkPayloadParser);
    } catch (Throwable error) {
      return PluginProgramRuntime.terminateProcess(error);
    }
  }

  private static <S extends MessageLite, T extends MessageLite> SourceAndSinkProgram<S, T> run0(
      String[] arguments, Parser<T> sinkPayloadParser) throws Exception {
    Objects.requireNonNull(sinkPayloadParser, "sinkPayloadParser");
    var startup = PluginProgramRuntime.startSourceAndSink(arguments);
    var runtime = startup.runtime();
    SourceSession<S> sourceSession = null;
    if (startup.channelBellPath() != null) {
      sourceSession =
          SourceSession.open(
              runtime.workingDirectory().resolve("source"), startup.channelBellPath());
    }
    TenonSourceAndSink<T> sharedOwner;
    var factory = SourceAndSinkProgram.<S, T>loadFactory();
    sharedOwner =
        Objects.requireNonNull(
            factory.create(
                runtime.config(),
                sourceSession == null
                    ? Optional.empty()
                    : Optional.of(
                        new Ingress<>(sourceSession.parallelism(), sourceSession.sender())),
                startup.channels() == null
                    ? Set.of()
                    : startup.channels().stream()
                        .map(SinkInput::channel)
                        .collect(Collectors.toUnmodifiableSet())),
            "TenonSourceAndSinkFactory returned null");

    SinkProgramOwner<T> sinkOwner = null;
    if (startup.channels() != null && !startup.channels().isEmpty()) {
      sinkOwner =
          SinkProgramOwner.open(
              sharedOwner, runtime.workingDirectory(), startup.channels(), sinkPayloadParser);
    }
    if (sourceSession != null) {
      var source = sourceSession;
      if (sinkOwner != null) {
        sinkOwner.failure().whenComplete((ignored, error) -> source.fail(error));
      }
      source
          .failure()
          .whenComplete(
              (ignored, error) -> {
                if (error != null) PluginProgramRuntime.terminateProcess(error);
              });
    }
    if (sourceSession == null) {
      sinkOwner
          .failure()
          .whenComplete(
              (ignored, error) -> {
                if (error != null) PluginProgramRuntime.terminateProcess(error);
              });
    }
    sharedOwner.start();
    if (sinkOwner != null) {
      sinkOwner.startPaused();
    }
    return new SourceAndSinkProgram<>(runtime, sourceSession, sharedOwner, sinkOwner);
  }

  /** Returns an independent copy of the validated process configuration. */
  public JsonNode config() {
    return runtime.config().deepCopy();
  }

  /** Publishes one Ready, quiesces Source alone, then closes the shared owner on final Shutdown. */
  public void awaitShutdown() {
    try {
      runtime.publishReady();
      if (sinkOwner != null) {
        sinkOwner.activate();
      }
      if (sourceSession != null) {
        runtime.awaitSourceQuiesce(sourceSession.failure());
        sourceSession.stopAccepting();
        business.quiesce();
        sourceSession.quiesce();
        runtime.publishSourceQuiesced();
        runtime.awaitShutdown(sourceSession.failure());
        sourceSession.close();
      } else {
        runtime.awaitShutdown(sinkOwner.failure());
      }
      if (sinkOwner != null) {
        sinkOwner.stopWorkers();
      }
      business.close();
      runtime.completeShutdown();
    } catch (Throwable error) {
      PluginProgramRuntime.terminateProcess(error);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static <S extends MessageLite, T extends MessageLite>
      TenonSourceAndSinkFactory<S, T> loadFactory() {
    List<TenonSourceAndSinkFactory<?, ?>> factories = new ArrayList<>();
    for (var provider : ServiceLoader.load(TenonSourceAndSinkFactory.class).stream().toList()) {
      factories.add(provider.get());
    }
    if (factories.size() != 1) {
      throw new IllegalStateException("Exactly one TenonSourceAndSinkFactory must be registered");
    }
    return (TenonSourceAndSinkFactory<S, T>) factories.getFirst();
  }
}
