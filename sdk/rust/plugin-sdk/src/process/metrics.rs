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

//! Private, on-demand process observations; reconnecting never replaces the CPU baseline.

use crate::wire::plugin::{self, plugin_to_pipeline_metrics::Message};
use Message::Snapshot;
use base64::Engine as _;
use hyper_util::rt::TokioIo;
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::{InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::MetricsData;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::reader::MetricReader;
use opentelemetry_sdk::metrics::{
    InstrumentKind, ManualReader, Pipeline, SdkMeterProvider, Temporality,
};
use plugin::PluginMetricsSnapshot;
use plugin::plugin_metrics_client::PluginMetricsClient;
use prost::Message as _;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_stream::{StreamExt as _, wrappers::ReceiverStream};
use tonic::transport::{Channel, Endpoint};

struct Collector {
    provider: SdkMeterProvider,
    reader: SharedReader,
    include: Arc<Mutex<Vec<String>>>,
    diagnostic: Arc<Diagnostic>,
}

impl Collector {
    fn new(launch_id: &[u8]) -> Self {
        let reader = SharedReader(Arc::new(ManualReader::builder().build()));
        let provider = SdkMeterProvider::builder()
            .with_resource(
                Resource::builder_empty()
                    .with_attributes([
                        KeyValue::new("service.namespace", "tenon"),
                        KeyValue::new("service.name", "tenon.plugin"),
                        KeyValue::new(
                            "service.instance.id",
                            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(launch_id),
                        ),
                    ])
                    .build(),
            )
            .with_reader(reader.clone())
            .build();
        let meter = provider.meter_with_scope(
            InstrumentationScope::builder("tenon-plugin-sdk")
                .with_version(env!("CARGO_PKG_VERSION"))
                .build(),
        );
        let diagnostic = Arc::new(Diagnostic::default());
        let include = Arc::new(Mutex::new(Vec::<String>::new()));
        let cpu_include = include.clone();
        let cpu_diagnostic = diagnostic.clone();
        let sampler = Mutex::new(tenon_process_metrics::ProcessSampler::default());
        meter
            .f64_observable_gauge("tenon.plugin.cpu")
            .with_unit("1")
            .with_callback(move |observer| {
                if selected(&cpu_include, "tenon.plugin.cpu")
                    && let Ok(mut sampler) = sampler.lock()
                {
                    match sampler.cpu() {
                        Ok(Some(value)) => observer.observe(value, &[]),
                        Ok(None) => {}
                        Err(error) => cpu_diagnostic.report(&error),
                    }
                }
            })
            .build();
        let memory_include = include.clone();
        let memory_diagnostic = diagnostic.clone();
        meter
            .u64_observable_gauge("tenon.plugin.memory")
            .with_unit("By")
            .with_callback(move |observer| {
                if selected(&memory_include, "tenon.plugin.memory") {
                    match tenon_process_metrics::memory() {
                        Ok(value) => observer.observe(value, &[]),
                        Err(error) => memory_diagnostic.report(&error),
                    }
                }
            })
            .build();
        Self {
            provider,
            reader,
            include,
            diagnostic,
        }
    }

    fn collect(&self, include: Vec<String>) -> Result<Vec<u8>, crate::Error> {
        *self.include.lock().map_err(|_| "metrics filter poisoned")? = include;
        let mut data = ResourceMetrics::default();
        self.reader.collect(&mut data)?;
        let mut data = MetricsData {
            resource_metrics: ExportMetricsServiceRequest::from(&data).resource_metrics,
        };
        for resource in &mut data.resource_metrics {
            for scope in &mut resource.scope_metrics {
                scope.metrics.retain(|metric| selected(&self.include, &metric.name)
                    && matches!(&metric.data, Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge)) if !gauge.data_points.is_empty()));
            }
            resource
                .scope_metrics
                .retain(|scope| !scope.metrics.is_empty());
        }
        data.resource_metrics
            .retain(|resource| !resource.scope_metrics.is_empty());
        Ok(data.encode_to_vec())
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        let _ = self.provider.shutdown_with_timeout(Duration::ZERO);
    }
}

fn selected(include: &Mutex<Vec<String>>, name: &str) -> bool {
    include
        .lock()
        .is_ok_and(|include| include.is_empty() || include.iter().any(|entry| entry == name))
}

pub(super) async fn run(
    path: std::path::PathBuf,
    launch_id: Vec<u8>,
    executor: super::TrackedExecutor,
) {
    let collector = Collector::new(&launch_id);
    loop {
        let path = path.clone();
        let channel = Endpoint::from_static("http://[::]:50051")
            .executor(executor.clone())
            .connect_with_connector(tower::service_fn(move |_| {
                let path = path.clone();
                async move { UnixStream::connect(path).await.map(TokioIo::new) }
            }))
            .await;
        match channel {
            Ok(channel) => match serve(channel, &launch_id, &collector).await {
                Ok(()) => collector.diagnostic.report(&"metrics stream closed"),
                Err(error) => collector.diagnostic.report(&error),
            },
            Err(error) => collector.diagnostic.report(&error),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn serve(
    channel: Channel,
    launch_id: &[u8],
    collector: &Collector,
) -> Result<(), crate::Error> {
    let (snapshots, outgoing) = mpsc::channel(1);
    let attach = plugin::PluginToPipelineMetrics {
        message: Some(Message::Attach(plugin::PluginMetricsAttach {
            launch_id: launch_id.to_vec(),
        })),
    };
    let mut client = PluginMetricsClient::new(channel)
        .max_decoding_message_size(64 * 1024)
        .max_encoding_message_size(64 * 1024);
    let mut requests = client
        .stream(tokio_stream::iter([attach]).chain(ReceiverStream::new(outgoing)))
        .await?
        .into_inner();
    while let Some(request) = requests.message().await? {
        let metrics = collector.collect(request.include)?;
        snapshots
            .send(plugin::PluginToPipelineMetrics {
                message: Some(Snapshot(PluginMetricsSnapshot { metrics })),
            })
            .await?;
    }
    Ok(())
}

/// All observation failures share one per-process diagnostic rate limit.
#[derive(Default)]
struct Diagnostic(Mutex<Option<std::time::Instant>>);
impl Diagnostic {
    fn report(&self, error: &dyn std::fmt::Display) {
        if let Ok(mut previous) = self.0.lock() {
            let now = std::time::Instant::now();
            if previous
                .is_none_or(|previous| now.duration_since(previous) >= Duration::from_secs(60))
            {
                *previous = Some(now);
                use std::io::Write as _;
                let _ = writeln!(std::io::stderr(), "Plugin metrics unavailable: {error}");
            }
        }
    }
}

#[derive(Debug, Clone)]
struct SharedReader(Arc<ManualReader>);
impl MetricReader for SharedReader {
    fn register_pipeline(&self, pipeline: Weak<Pipeline>) {
        self.0.register_pipeline(pipeline);
    }
    fn collect(&self, metrics: &mut ResourceMetrics) -> OTelSdkResult {
        self.0.collect(metrics)
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.0.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }
    fn temporality(&self, kind: InstrumentKind) -> Temporality {
        self.0.temporality(kind)
    }
}

#[cfg(test)]
mod tests;
