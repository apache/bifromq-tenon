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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.channel.MultiThreadIoEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioIoHandler;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerDomainSocketChannel;
import io.grpc.stub.StreamObserver;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.bifromq.tenon.contracts.plugin.PluginMetricsGrpc;
import org.apache.bifromq.tenon.contracts.plugin.ProcessMetrics.PipelineToPluginMetrics;
import org.apache.bifromq.tenon.contracts.plugin.ProcessMetrics.PluginToPipelineMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PluginMetricsConnectionTest {
  @TempDir Path directory;

  @Test
  void reconnectKeepsCpuBaselineAndCloseReclaimsThreadsWithoutPeerReply() throws Exception {
    var socket = directory.resolve("metrics.sock");
    var eventLoop = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    var sessions = new ArrayBlockingQueue<Session>(1);
    var server =
        NettyServerBuilder.forAddress(UnixDomainSocketAddress.of(socket))
            .channelType(NioServerDomainSocketChannel.class)
            .withChildOption(ChannelOption.SO_KEEPALIVE, null)
            .bossEventLoopGroup(eventLoop)
            .workerEventLoopGroup(eventLoop)
            .directExecutor()
            .addService(
                new PluginMetricsGrpc.PluginMetricsImplBase() {
                  @Override
                  public StreamObserver<PluginToPipelineMetrics> stream(
                      StreamObserver<PipelineToPluginMetrics> responses) {
                    var session = new Session(responses, new ArrayBlockingQueue<>(2));
                    return new StreamObserver<>() {
                      @Override
                      public void onNext(PluginToPipelineMetrics value) {
                        if (value.hasAttach()) assertTrue(sessions.offer(session));
                        else assertTrue(session.snapshots.offer(value));
                      }

                      @Override
                      public void onError(Throwable error) {}

                      @Override
                      public void onCompleted() {}
                    };
                  }
                })
            .build()
            .start();
    Set<Thread> before = Thread.getAllStackTraces().keySet();
    var connection = PluginMetricsConnection.open(socket, new byte[16]);
    try {
      for (int attempt = 0; attempt < 8; attempt++) {
        var session = sessions.poll(5, TimeUnit.SECONDS);
        assertNotNull(session, "SDK must reconnect after the previous stream ends");
        session.responses.onNext(
            PipelineToPluginMetrics.newBuilder().addInclude("tenon.plugin.cpu").build());
        var reply = session.snapshots.poll(5, TimeUnit.SECONDS);
        assertNotNull(reply);
        assertTrue(reply.hasSnapshot());
        assertEquals(
            attempt == 0,
            reply.getSnapshot().getMetrics().isEmpty(),
            "Reconnect retains the CPU baseline");
        session.responses.onCompleted();
      }
      // Disconnect immediately after sending Collect, including the window in
      // which the worker replaces its completed request future.
      for (int attempt = 0; attempt < 8; attempt++) {
        var session = sessions.poll(5, TimeUnit.SECONDS);
        assertNotNull(session);
        session.responses.onNext(
            PipelineToPluginMetrics.newBuilder().addInclude("tenon.plugin.memory").build());
        session.responses.onCompleted();
      }
      assertNotNull(sessions.poll(5, TimeUnit.SECONDS));
      var owned =
          Thread.getAllStackTraces().keySet().stream()
              .filter(
                  thread ->
                      !before.contains(thread)
                          && thread.getName().startsWith("tenon-plugin-metrics"))
              .toList();
      assertFalse(owned.isEmpty());
      connection.close();
      for (var thread : owned) {
        thread.join(5000);
        assertFalse(thread.isAlive(), "Metrics thread must exit without waiting for a peer reply");
      }
    } finally {
      connection.close();
      server.shutdownNow();
      assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
      eventLoop.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync();
    }
  }

  private record Session(
      StreamObserver<PipelineToPluginMetrics> responses,
      ArrayBlockingQueue<PluginToPipelineMetrics> snapshots) {}
}
