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

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.MultiThreadIoEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.nio.NioIoHandler;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioDomainSocketChannel;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.io.IOException;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bifromq.tenon.contracts.plugin.PluginMetricsGrpc;
import org.apache.bifromq.tenon.contracts.plugin.ProcessMetrics.PipelineToPluginMetrics;
import org.apache.bifromq.tenon.contracts.plugin.ProcessMetrics.PluginMetricsAttach;
import org.apache.bifromq.tenon.contracts.plugin.ProcessMetrics.PluginMetricsSnapshot;
import org.apache.bifromq.tenon.contracts.plugin.ProcessMetrics.PluginToPipelineMetrics;

/** A separate gRPC channel owns observation callbacks, reconnects, and collector cleanup. */
final class PluginMetricsConnection implements AutoCloseable {
  private final EventLoopGroup eventLoop;
  private final ManagedChannel channel;
  private final PluginProcessMetrics collector;
  private final byte[] launchId;
  private final AtomicBoolean closed = new AtomicBoolean();

  private PluginMetricsConnection(Path socket, byte[] launchId) {
    this.launchId = launchId.clone();
    collector = new PluginProcessMetrics(launchId);
    eventLoop =
        new MultiThreadIoEventLoopGroup(
            1,
            Thread.ofPlatform()
                .daemon(true)
                .name("tenon-plugin-metrics-io-", 0)
                .uncaughtExceptionHandler((thread, failure) -> collector.report(failure))
                .factory(),
            NioIoHandler.newFactory());
    try {
      channel =
          NettyChannelBuilder.forAddress(UnixDomainSocketAddress.of(socket))
              .overrideAuthority("localhost")
              .channelType(NioDomainSocketChannel.class, UnixDomainSocketAddress.class)
              .withOption(ChannelOption.SO_KEEPALIVE, null)
              .eventLoopGroup(eventLoop)
              .executor(eventLoop)
              .maxInboundMessageSize(64 * 1024)
              .usePlaintext()
              .build();
    } catch (RuntimeException | Error failure) {
      collector.close();
      eventLoop.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
      throw failure;
    }
  }

  static PluginMetricsConnection open(Path socket, byte[] launchId) {
    var connection = new PluginMetricsConnection(socket, launchId);
    connection.eventLoop.execute(connection::attach);
    return connection;
  }

  private void attach() {
    if (closed.get()) return;
    var requests =
        PluginMetricsGrpc.newStub(channel).stream(
            new ClientResponseObserver<PluginToPipelineMetrics, PipelineToPluginMetrics>() {
              private ClientCallStreamObserver<PluginToPipelineMetrics> outbound;

              @Override
              public void beforeStart(ClientCallStreamObserver<PluginToPipelineMetrics> request) {
                outbound = request;
                request.disableAutoRequestWithInitial(1);
              }

              @Override
              public void onNext(PipelineToPluginMetrics request) {
                if (closed.get()) return;
                try {
                  byte[] metrics = collector.collect(request.getIncludeList());
                  if (closed.get()) return;
                  if (metrics.length > 64 * 1024 - 16)
                    throw new IOException("Metrics snapshot exceeds limit");
                  outbound.onNext(
                      PluginToPipelineMetrics.newBuilder()
                          .setSnapshot(
                              PluginMetricsSnapshot.newBuilder()
                                  .setMetrics(ByteString.copyFrom(metrics)))
                          .build());
                  outbound.request(1);
                } catch (Exception failure) {
                  collector.report(failure);
                  outbound.cancel("Metrics collection failed", failure);
                }
              }

              @Override
              public void onError(Throwable failure) {
                reconnect(failure);
              }

              @Override
              public void onCompleted() {
                reconnect(new IOException("Metrics stream closed"));
              }
            });
    requests.onNext(
        PluginToPipelineMetrics.newBuilder()
            .setAttach(PluginMetricsAttach.newBuilder().setLaunchId(ByteString.copyFrom(launchId)))
            .build());
  }

  private void reconnect(Throwable failure) {
    if (closed.get()) return;
    collector.report(failure);
    eventLoop.schedule(this::attach, 250, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) return;
    channel.shutdownNow();
    // Cleanup runs after any sampling callback on the same owner. The lifecycle
    // caller does not wait for the peer, a sample, or a final flush.
    eventLoop.execute(collector::close);
    eventLoop.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
  }
}
