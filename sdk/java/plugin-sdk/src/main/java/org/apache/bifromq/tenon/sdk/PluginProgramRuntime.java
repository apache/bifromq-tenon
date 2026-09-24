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
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.PluginToPipeline;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.Ready;
import org.apache.bifromq.tenon.contracts.plugin.ProcessControl.SourceQuiesced;
import tools.jackson.databind.JsonNode;

/** Owns startup, UDS lifecycle control, and the process-failure boundary of one Plugin Program. */
final class PluginProgramRuntime {
  private static final Thread.UncaughtExceptionHandler PROCESS_FAILURE_HANDLER =
      (thread, failure) -> terminateProcess(failure);

  private final Path workingDirectory;
  private final JsonNode config;
  private final PluginControlConnection control;
  private Phase phase = Phase.ATTACHED;

  private PluginProgramRuntime(
      Path workingDirectory, JsonNode config, PluginControlConnection control) {
    this.workingDirectory = workingDirectory;
    this.config = config;
    this.control = control;
  }

  /**
   * Reads Source startup arguments, attaches the UDS stream, and begins monitoring the stdin owner
   * channel.
   */
  static SourceStartup startSource(String[] arguments) throws IOException {
    var startup = readStartup(arguments);
    var channelBellPath = PluginProgramProtocol.requireChannelBellPath(startup);
    return new SourceStartup(attach(startup), channelBellPath);
  }

  /** Reads Sink identities before the lifetime watcher takes ownership of stdin. */
  static SinkStartup startSink(String[] arguments) throws IOException {
    var startup = readStartup(arguments);
    var channels = PluginProgramProtocol.requireSinkInputs(startup);
    return new SinkStartup(attach(startup), channels);
  }

  /** Reads Sink identities before the lifetime watcher takes ownership of stdin. */
  static SourceAndSinkStartup startSourceAndSink(String[] arguments) throws IOException {
    var startup = readStartup(arguments);
    var channels = startup.bells().sinkInputs();
    var channelBellPath = startup.bells().sourceChannelRegion();
    if (channelBellPath == null && (channels == null || channels.isEmpty())) {
      throw new IOException("plugin.startup.no_bound_interface");
    }
    return new SourceAndSinkStartup(attach(startup), channels, channelBellPath);
  }

  private static PluginProgramProtocol.Startup readStartup(String[] arguments) throws IOException {
    Thread.currentThread().setUncaughtExceptionHandler(PROCESS_FAILURE_HANDLER);
    Thread.setDefaultUncaughtExceptionHandler((thread, failure) -> terminateProcess(failure));
    Objects.requireNonNull(arguments, "arguments");
    return PluginProgramProtocol.readStartup(arguments, System.in);
  }

  private static PluginProgramRuntime attach(PluginProgramProtocol.Startup startup) {
    var control =
        PluginControlConnection.open(
            startup.controlSocket(), startup.launchId(), PROCESS_FAILURE_HANDLER);
    Thread.ofPlatform()
        .daemon(true)
        .name("tenon-plugin-owner-lifetime")
        .start(PluginProgramRuntime::watchOwnerLifetime);
    return new PluginProgramRuntime(startup.workingDirectory(), startup.config(), control);
  }

  /** Returns the absolute Instance working directory supplied by Pipeline. */
  Path workingDirectory() {
    return workingDirectory;
  }

  /** Returns the exact validated Plugin configuration supplied by Pipeline. */
  JsonNode config() {
    return config;
  }

  /** Reports that every direction bound for this Instance has completed local startup. */
  synchronized void publishReady() throws IOException {
    requirePhase(Phase.ATTACHED);
    control.send(PluginToPipeline.newBuilder().setReady(Ready.getDefaultInstance()).build());
    phase = Phase.READY;
  }

  /** Waits for the sole Source quiesce command accepted by a source-capable Program. */
  synchronized void awaitSourceQuiesce(CompletionStage<Void> localFailure) throws Exception {
    requirePhase(Phase.READY);
    var command = control.awaitCommand(localFailure);
    if (!command.hasQuiesceSource()) {
      throw new IOException("Plugin lifecycle expected QuiesceSource");
    }
    phase = Phase.QUIESCING_SOURCE;
  }

  /** Reports the Source admission boundary after all entered synchronous sends have returned. */
  synchronized void publishSourceQuiesced() throws IOException {
    requirePhase(Phase.QUIESCING_SOURCE);
    control.send(
        PluginToPipeline.newBuilder()
            .setSourceQuiesced(SourceQuiesced.getDefaultInstance())
            .build());
    phase = Phase.SOURCE_QUIESCED;
  }

  /** Waits for final shutdown while also observing a local capability failure. */
  synchronized void awaitShutdown(CompletionStage<Void> localFailure) throws Exception {
    if (phase != Phase.READY && phase != Phase.SOURCE_QUIESCED) {
      throw new IllegalStateException("Plugin lifecycle cannot await Shutdown in phase " + phase);
    }
    var command = control.awaitCommand(localFailure);
    if (!command.hasShutdown()) {
      throw new IOException("Plugin lifecycle expected Shutdown");
    }
    phase = Phase.SHUTTING_DOWN;
  }

  /** Half-closes the lifecycle stream after all local resources have been released. */
  synchronized void completeShutdown() throws IOException {
    requirePhase(Phase.SHUTTING_DOWN);
    control.finish();
    phase = Phase.CLOSED;
  }

  private void requirePhase(Phase expected) {
    if (phase != expected) {
      throw new IllegalStateException(
          "Plugin lifecycle expected phase " + expected + " but was " + phase);
    }
  }

  private static void watchOwnerLifetime() {
    try {
      while (System.in.read() != -1) {}
    } catch (IOException ignored) {
      // A read failure and EOF both mean that the Pipeline owner disappeared.
    }
    terminateProcess(new IOException("Pipeline stdin owner channel closed unexpectedly"));
  }

  static Throwable unwrapFailure(Throwable failure) {
    var current = failure;
    while (current instanceof CompletionException && current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }

  static synchronized <T> T terminateProcess(Throwable failure) {
    // Concurrent failures must not halt the process before the first diagnostic is flushed.
    failure.printStackTrace(System.err);
    System.err.flush();
    Runtime.getRuntime().halt(1);
    return null;
  }

  record SourceStartup(PluginProgramRuntime runtime, Path channelBellPath) {}

  record SinkStartup(PluginProgramRuntime runtime, List<SinkInput> channels) {}

  record SourceAndSinkStartup(
      PluginProgramRuntime runtime, List<SinkInput> channels, Path channelBellPath) {}

  private enum Phase {
    ATTACHED,
    READY,
    QUIESCING_SOURCE,
    SOURCE_QUIESCED,
    SHUTTING_DOWN,
    CLOSED
  }
}
