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

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.internal.otlp.metrics.MetricsRequestMarshaler;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.export.CollectionRegistration;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import org.apache.bifromq.tenon.metrics.ProcessSampler;

/** One private on-demand reader; the metrics event loop owns its sampler. */
final class PluginProcessMetrics implements AutoCloseable {
  private final OnDemandReader reader = new OnDemandReader();
  private final SdkMeterProvider provider;
  private final ProcessSampler sampler = new ProcessSampler();
  private long lastDiagnostic;
  private boolean diagnosed;
  private List<String> include = List.of();

  PluginProcessMetrics(byte[] launchId) {
    String sdkVersion = sdkVersion();
    provider =
        SdkMeterProvider.builder()
            .setResource(
                Resource.create(
                    Attributes.builder()
                        .put("service.namespace", "tenon")
                        .put("service.name", "tenon.plugin")
                        .put(
                            "service.instance.id",
                            Base64.getUrlEncoder().withoutPadding().encodeToString(launchId))
                        .build()))
            .registerMetricReader(reader)
            .build();
    var meter =
        provider
            .meterBuilder("tenon-java-plugin-sdk")
            .setInstrumentationVersion(sdkVersion)
            .build();
    meter
        .gaugeBuilder("tenon.plugin.cpu")
        .setUnit("1")
        .buildWithCallback(
            observation -> {
              if (!selected("tenon.plugin.cpu")) return;
              try {
                sampler.cpu().ifPresent(observation::record);
              } catch (IOException failure) {
                report(failure);
              }
            });
    meter
        .gaugeBuilder("tenon.plugin.memory")
        .ofLongs()
        .setUnit("By")
        .buildWithCallback(
            observation -> {
              if (!selected("tenon.plugin.memory")) return;
              try {
                observation.record(ProcessSampler.memory());
              } catch (IOException failure) {
                report(failure);
              }
            });
  }

  byte[] collect(List<String> names) throws IOException {
    include = List.copyOf(names);
    var data =
        reader.registration.collectAllMetrics().stream()
            .filter(metric -> selected(metric.getName()) && !metric.isEmpty())
            .toList();
    var bytes = new ByteArrayOutputStream();
    // ExportMetricsServiceRequest and MetricsData both encode ResourceMetrics in field 1.
    // This pinned internal converter is shaded with the SDK; no exporter is instantiated.
    MetricsRequestMarshaler.create(data).writeBinaryTo(bytes);
    return bytes.toByteArray();
  }

  private static String sdkVersion() {
    try (var input = PluginProcessMetrics.class.getResourceAsStream("version.properties")) {
      if (input == null) throw new IOException("SDK version resource missing");
      var properties = new java.util.Properties();
      properties.load(input);
      String version = properties.getProperty("version");
      if (version == null || version.isBlank()) throw new IOException("SDK version missing");
      return version;
    } catch (IOException failure) {
      throw new IllegalStateException("SDK version unavailable", failure);
    }
  }

  void report(Throwable failure) {
    long now = System.nanoTime();
    if (!diagnosed || now - lastDiagnostic >= java.util.concurrent.TimeUnit.SECONDS.toNanos(60)) {
      diagnosed = true;
      lastDiagnostic = now;
      System.err.println("Plugin metrics unavailable: " + failure);
    }
  }

  private boolean selected(String name) {
    return include.isEmpty() || include.contains(name);
  }

  @Override
  public void close() {
    provider.shutdown();
  }

  private static final class OnDemandReader implements MetricReader {
    private CollectionRegistration registration;

    @Override
    public void register(CollectionRegistration value) {
      registration = value;
    }

    @Override
    public AggregationTemporality getAggregationTemporality(InstrumentType type) {
      return AggregationTemporality.CUMULATIVE;
    }

    @Override
    public CompletableResultCode forceFlush() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      return CompletableResultCode.ofSuccess();
    }
  }
}
