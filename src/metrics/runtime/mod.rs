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

//! Owns one private provider and synchronous cumulative collection.

use base64::Engine as _;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Meter, MeterProvider as _};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::MetricsData;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::reader::MetricReader;
use opentelemetry_sdk::metrics::{
    InstrumentKind, ManualReader, Pipeline, SdkMeterProvider, Stream, Temporality,
};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use super::process::{self, ProcessSampler};

#[derive(Clone, Copy)]
pub(crate) enum CoreProcess<'a> {
    Runner,
    Pipeline {
        document_id: &'a str,
        launch_id: &'a [u8],
    },
}

/// The process boundary must explicitly shut down this owner before its runtime.
pub(crate) struct MetricsRuntime {
    provider: SdkMeterProvider,
    reader: SharedReader,
}

impl MetricsRuntime {
    #[allow(
        clippy::expect_used,
        reason = "metric cardinality is a fixed supported SDK option"
    )]
    pub(crate) fn start(
        node_id: Option<&str>,
        process: CoreProcess<'_>,
    ) -> Result<Self, getrandom::Error> {
        let instance_id = match process {
            CoreProcess::Runner => {
                let mut identity = [0_u8; 16];
                getrandom::fill(&mut identity)?;
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(identity)
            }
            CoreProcess::Pipeline { launch_id, .. } => {
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(launch_id)
            }
        };
        let service_name = match process {
            CoreProcess::Runner => "tenon.runner",
            CoreProcess::Pipeline { .. } => "tenon.pipeline",
        };
        let mut attributes = vec![
            KeyValue::new("service.namespace", "tenon"),
            KeyValue::new("service.name", service_name),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("service.instance.id", instance_id),
        ];
        if let Some(node_id) = node_id {
            attributes.push(KeyValue::new("tenon.node.id", node_id.to_owned()));
        }
        if let CoreProcess::Pipeline { document_id, .. } = process {
            attributes.push(KeyValue::new("tenon.pipeline.id", document_id.to_owned()));
        }
        let reader = SharedReader(Arc::new(
            ManualReader::builder()
                .with_temporality(Temporality::Cumulative)
                .build(),
        ));
        let provider = SdkMeterProvider::builder()
            .with_resource(
                Resource::builder_empty()
                    .with_attributes(attributes)
                    .build(),
            )
            .with_reader(reader.clone())
            .with_view(|_| {
                Some(
                    Stream::builder()
                        .with_cardinality_limit(2000)
                        .build()
                        .expect("fixed positive metric cardinality"),
                )
            })
            .build();
        let meter = provider.meter("tenon");
        register_process_metrics(&meter, process);
        Ok(Self { provider, reader })
    }

    pub(crate) fn meter(&self) -> Meter {
        self.provider.meter("tenon")
    }

    #[allow(
        clippy::expect_used,
        reason = "process owners keep the provider active until all collectors have stopped"
    )]
    pub(crate) fn collect(&self, include: &[String]) -> MetricsData {
        let mut metrics = ResourceMetrics::default();
        self.reader
            .collect(&mut metrics)
            .expect("active metrics reader remains registered and unpoisoned");
        let mut snapshot = MetricsData {
            resource_metrics: ExportMetricsServiceRequest::from(&metrics).resource_metrics,
        };
        for resource in &mut snapshot.resource_metrics {
            for scope in &mut resource.scope_metrics {
                scope.metrics.retain(|metric| {
                    (include.is_empty() || include.contains(&metric.name)) && has_points(metric)
                });
            }
            resource
                .scope_metrics
                .retain(|scope| !scope.metrics.is_empty());
        }
        snapshot
            .resource_metrics
            .retain(|resource| !resource.scope_metrics.is_empty());
        snapshot
    }

    #[allow(
        clippy::expect_used,
        reason = "the process closes its collector entry points before shutting down the reader"
    )]
    pub(crate) fn shutdown(&self) {
        self.provider
            .shutdown_with_timeout(Duration::ZERO)
            .expect("active ManualReader closes without concurrent collection");
    }
}

fn has_points(metric: &opentelemetry_proto::tonic::metrics::v1::Metric) -> bool {
    use opentelemetry_proto::tonic::metrics::v1::metric::Data;
    match &metric.data {
        Some(Data::Gauge(data)) => !data.data_points.is_empty(),
        Some(Data::Sum(data)) => !data.data_points.is_empty(),
        Some(Data::Histogram(data)) => !data.data_points.is_empty(),
        _ => unreachable!("core metrics only record gauges, counters and explicit histograms"),
    }
}

#[allow(
    clippy::expect_used,
    reason = "the sole collector invokes this callback synchronously on its current-thread runtime"
)]
fn register_process_metrics(meter: &Meter, process: CoreProcess<'_>) {
    let (cpu_name, memory_name) = match process {
        CoreProcess::Runner => ("tenon.process.cpu", "tenon.process.memory"),
        CoreProcess::Pipeline { .. } => ("tenon.pipeline.cpu", "tenon.pipeline.memory"),
    };
    let sampler = Mutex::new(ProcessSampler::default());
    meter
        .f64_observable_gauge(cpu_name)
        .with_unit("1")
        .with_callback(move |observer| {
            if let Ok(Some(value)) = sampler.lock().expect("CPU sampler poisoned").cpu() {
                observer.observe(value, &[]);
            }
        })
        .build();
    meter
        .u64_observable_gauge(memory_name)
        .with_unit("By")
        .with_callback(|observer| {
            if let Ok(value) = process::memory() {
                observer.observe(value, &[]);
            }
        })
        .build();
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
