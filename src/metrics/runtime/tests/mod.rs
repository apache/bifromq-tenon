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

use super::*;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value;
use opentelemetry_proto::tonic::metrics::v1::{metric, number_data_point};
use std::io;

mod resources;

#[test]
fn process_identity_is_stable_and_follows_its_source() -> io::Result<()> {
    let mut identities = Vec::new();
    for process in [
        CoreProcess::Runner,
        CoreProcess::Runner,
        CoreProcess::Pipeline {
            document_id: "document",
            launch_id: b"first-launch",
        },
        CoreProcess::Pipeline {
            document_id: "document",
            launch_id: b"second-launch",
        },
    ] {
        let runtime =
            MetricsRuntime::start(Some("test-node"), process).map_err(io::Error::other)?;
        let mut samples = Vec::new();
        for _ in 0..2 {
            let export = runtime.collect(&[]);
            let identity = export
                .resource_metrics
                .iter()
                .filter_map(|metrics| metrics.resource.as_ref())
                .flat_map(|resource| &resource.attributes)
                .find(|attribute| attribute.key == "service.instance.id")
                .and_then(|attribute| attribute.value.as_ref())
                .and_then(|value| value.value.as_ref());
            let Some(any_value::Value::StringValue(identity)) = identity else {
                return Err(io::Error::other("missing process identity"));
            };
            samples.push(identity.clone());
        }
        assert_eq!(samples[0], samples[1]);
        identities.push(samples.remove(0));
        runtime.shutdown();
    }
    assert_ne!(identities[0], identities[1]);
    for identity in &identities[..2] {
        assert_eq!(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(identity)
                .map_err(io::Error::other)?
                .len(),
            16
        );
    }
    assert_eq!(
        identities[2],
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"first-launch")
    );
    assert_eq!(
        identities[3],
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"second-launch")
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn time_and_shutdown_never_collect_without_a_request() -> io::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let runtime = MetricsRuntime::start(None, CoreProcess::Runner).map_err(io::Error::other)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    runtime
        .meter()
        .u64_observable_gauge("tenon.pipeline.state")
        .with_callback(move |observer| {
            observed.fetch_add(1, Ordering::Relaxed);
            observer.observe(1, &[]);
        })
        .build();
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    let counter = runtime
        .meter()
        .u64_counter("tenon.pipeline.restarts")
        .build();
    counter.add(2, &[]);
    assert!(
        runtime
            .collect(&["tenon.flow.input.records".to_owned()])
            .resource_metrics
            .is_empty()
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    counter.add(3, &[]);
    let snapshot = runtime.collect(&["tenon.pipeline.restarts".to_owned()]);
    let Some(metric::Data::Sum(sum)) =
        &snapshot.resource_metrics[0].scope_metrics[0].metrics[0].data
    else {
        return Err(io::Error::other("missing cumulative restart counter"));
    };
    assert_eq!(sum.aggregation_temporality, 2);
    assert_eq!(
        sum.data_points[0].value,
        Some(number_data_point::Value::AsInt(5))
    );
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    runtime.shutdown();
    assert_eq!(calls.load(Ordering::Relaxed), 2);
    Ok(())
}

#[test]
fn cardinality_overflow_is_visible_in_cumulative_snapshots() -> io::Result<()> {
    let runtime = MetricsRuntime::start(None, CoreProcess::Runner).map_err(io::Error::other)?;
    let counter = runtime
        .meter()
        .u64_counter("tenon.pipeline.restarts")
        .with_unit("{restart}")
        .build();
    for index in 0..2010 {
        counter.add(
            1,
            &[KeyValue::new(
                "tenon.pipeline.id",
                format!("pipeline-{index}"),
            )],
        );
    }
    let export = runtime.collect(&[]);
    let sum = export
        .resource_metrics
        .iter()
        .flat_map(|resource| &resource.scope_metrics)
        .flat_map(|scope| &scope.metrics)
        .find_map(|metric| match &metric.data {
            Some(metric::Data::Sum(sum)) if metric.name == "tenon.pipeline.restarts" => Some(sum),
            _ => None,
        })
        .ok_or_else(|| io::Error::other("restart counter was not exported"))?;
    assert!(sum.is_monotonic);
    assert_eq!(sum.aggregation_temporality, 2);
    assert_eq!(sum.data_points.len(), 2001);
    let overflow = sum
        .data_points
        .iter()
        .find(|point| {
            point
                .attributes
                .iter()
                .any(|attribute| attribute.key == "otel.metric.overflow")
        })
        .ok_or_else(|| io::Error::other("overflow was not exported"))?;
    assert_eq!(overflow.value, Some(number_data_point::Value::AsInt(10)));
    runtime.shutdown();
    Ok(())
}

#[test]
fn filtering_before_conversion_preserves_the_official_snapshot() -> io::Result<()> {
    use opentelemetry::InstrumentationScope;
    use prost::Message as _;

    let reader = SharedReader(Arc::new(ManualReader::builder().build()));
    let provider = SdkMeterProvider::builder()
        .with_reader(reader.clone())
        .with_resource(
            Resource::builder_empty()
                .with_schema_url(
                    [KeyValue::new("service.name", "tenon.pipeline")],
                    "https://example.test/resource",
                )
                .build(),
        )
        .build();
    let meter = provider.meter_with_scope(
        InstrumentationScope::builder("measured")
            .with_version("1.0")
            .with_schema_url("https://example.test/scope")
            .with_attributes([KeyValue::new("scope.attribute", "value")])
            .build(),
    );
    let attributes = [KeyValue::new("tenon.channel.index", 7_i64)];
    meter
        .u64_counter("records")
        .with_unit("{record}")
        .with_description("Recorded inputs")
        .build()
        .add(9_007_199_254_740_993, &attributes);
    meter.i64_gauge("waiting").build().record(-1, &attributes);
    let histogram = meter
        .f64_histogram("duration")
        .with_boundaries(Vec::new())
        .build();
    histogram.record(0.25, &attributes);
    histogram.record(0.75, &attributes);
    meter
        .u64_observable_gauge("missing")
        .with_callback(|_| {})
        .build();
    provider
        .meter("other")
        .u64_gauge("memory")
        .build()
        .record(64, &[]);
    provider
        .meter("empty")
        .u64_observable_gauge("no_points")
        .with_callback(|_| {})
        .build();

    let mut data = ResourceMetrics::default();
    reader.collect(&mut data).map_err(io::Error::other)?;
    for names in [
        vec![],
        vec!["records"],
        vec!["duration", "waiting"],
        vec!["memory"],
        vec!["records", "records", "unknown"],
        vec!["missing", "no_points"],
        vec!["unknown"],
    ] {
        let include: Vec<String> = names.into_iter().map(str::to_owned).collect();
        let mut expected = MetricsData {
            resource_metrics: ExportMetricsServiceRequest::from(&data).resource_metrics,
        };
        for resource in &mut expected.resource_metrics {
            for scope in &mut resource.scope_metrics {
                scope.metrics.retain(|metric| {
                    (include.is_empty() || include.contains(&metric.name)) && has_points(metric)
                });
            }
            resource
                .scope_metrics
                .retain(|scope| !scope.metrics.is_empty());
        }
        expected
            .resource_metrics
            .retain(|resource| !resource.scope_metrics.is_empty());
        let actual = filtered_snapshot(&data, &include);
        assert_eq!(actual, expected, "include={include:?}");
        assert_eq!(
            actual.encode_to_vec(),
            expected.encode_to_vec(),
            "include={include:?}"
        );
    }
    provider.shutdown().map_err(io::Error::other)?;
    Ok(())
}
