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
use crate::metrics::{CoreProcess, MetricsRuntime};
use opentelemetry::KeyValue;
use std::io;

#[test]
fn both_formats_preserve_identity_integer_precision_and_cumulative_histograms() -> io::Result<()> {
    let runtime = MetricsRuntime::start(
        Some("test-node"),
        CoreProcess::Pipeline {
            document_id: "observed",
            launch_id: b"launch-a",
        },
    )
    .map_err(io::Error::other)?;
    let meter = runtime.meter();
    let counter = meter
        .u64_counter("tenon.flow.input.records")
        .with_unit("{record}")
        .build();
    let histogram = meter
        .f64_histogram("tenon.flow.lua.duration")
        .with_unit("s")
        .with_boundaries(Vec::new())
        .build();
    let labels = [
        KeyValue::new("tenon.flow.id", "quoted\"\\flow"),
        KeyValue::new("tenon.channel.index", 7_i64),
    ];
    counter.add(9_007_199_254_740_993, &labels);
    histogram.record(0.25, &labels);
    histogram.record(0.75, &labels);
    let include = vec![
        "tenon.flow.input.records".to_owned(),
        "tenon.flow.lua.duration".to_owned(),
    ];
    let snapshot = runtime.collect(&include);
    let body: serde_json::Value =
        serde_json::from_slice(&serde_json::to_vec(&MetricsResponse::from(&snapshot))?)?;
    assert_eq!(body["processes"].as_array().map(Vec::len), Some(1));
    let resource = &body["processes"][0]["resource"];
    assert_eq!(resource["tenon.node.id"], "test-node");
    assert_eq!(resource["tenon.pipeline.id"], "observed");
    let metrics = body["processes"][0]["metrics"]
        .as_array()
        .ok_or_else(|| io::Error::other("Missing metrics"))?;
    let counter = metrics
        .iter()
        .find(|metric| metric["type"] == "counter")
        .ok_or_else(|| io::Error::other("Missing counter"))?;
    assert_eq!(counter["temporality"], "cumulative");
    assert_eq!(counter["points"][0]["value"], "9007199254740993");
    assert_eq!(counter["points"][0]["attributes"]["tenon.channel.index"], 7);
    assert!(
        counter["points"][0]["timeUnixNano"]
            .as_str()
            .and_then(|time| time.parse::<u64>().ok())
            .is_some()
    );
    assert!(counter["points"][0]["startTimeUnixNano"].is_string());
    let histogram = metrics
        .iter()
        .find(|metric| metric["type"] == "histogram")
        .ok_or_else(|| io::Error::other("Missing histogram"))?;
    assert_eq!(histogram["points"][0]["count"], "2");
    assert_eq!(histogram["points"][0]["sum"], 1.0);
    assert_eq!(histogram["points"][0]["min"], 0.25);
    assert_eq!(histogram["points"][0]["max"], 0.75);

    let text = prometheus::render(&snapshot);
    assert!(text.contains("# TYPE tenon_flow_input_records_total counter\n"));
    assert!(text.contains(" 9007199254740993\n"));
    assert!(text.contains("tenon_channel_index=\"7\""));
    assert!(text.contains("tenon_flow_id=\"quoted\\\"\\\\flow\""));
    for line in text.lines().filter(|line| !line.starts_with('#')) {
        assert!(line.contains("service_instance_id="));
    }
    assert!(text.contains("# TYPE tenon_flow_lua_duration_seconds summary\n"));
    assert!(text.contains("# TYPE tenon_flow_lua_duration_seconds_min gauge\n"));
    assert!(text.contains("# TYPE tenon_flow_lua_duration_seconds_max gauge\n"));
    assert!(text.lines().any(
        |line| line.starts_with("tenon_flow_lua_duration_seconds_count{") && line.ends_with(" 2")
    ));
    assert!(text.lines().any(
        |line| line.starts_with("tenon_flow_lua_duration_seconds_sum{") && line.ends_with(" 1")
    ));
    assert!(!text.contains("quantile="));
    assert!(!text.contains("_bucket"));
    assert!(!text.contains("target_info"));

    // Neither presentation nor a filtered read resets the SDK's accumulation.
    runtime.collect(&["tenon.pipeline.memory".to_owned()]);
    let repeated = serde_json::to_value(MetricsResponse::from(&runtime.collect(&include)))?;
    let repeated_metrics = repeated["processes"][0]["metrics"]
        .as_array()
        .ok_or_else(|| io::Error::other("Missing repeated metrics"))?;
    for original in metrics {
        let repeated = repeated_metrics
            .iter()
            .find(|metric| metric["name"] == original["name"])
            .ok_or_else(|| io::Error::other("Metric disappeared"))?;
        assert_eq!(
            original["points"][0]["value"],
            repeated["points"][0]["value"]
        );
        assert_eq!(
            original["points"][0]["count"],
            repeated["points"][0]["count"]
        );
        assert_eq!(
            original["points"][0]["startTimeUnixNano"],
            repeated["points"][0]["startTimeUnixNano"]
        );
    }
    runtime.shutdown();
    Ok(())
}

#[test]
fn multiple_processes_share_family_headers_without_merging_series() -> io::Result<()> {
    let first = MetricsRuntime::start(
        None,
        CoreProcess::Pipeline {
            document_id: "first",
            launch_id: b"first",
        },
    )
    .map_err(io::Error::other)?;
    let second = MetricsRuntime::start(
        None,
        CoreProcess::Pipeline {
            document_id: "second",
            launch_id: b"second",
        },
    )
    .map_err(io::Error::other)?;
    let include = vec!["tenon.pipeline.memory".to_owned()];
    let mut snapshot = first.collect(&include);
    snapshot
        .resource_metrics
        .extend(second.collect(&include).resource_metrics);
    let body = serde_json::to_value(MetricsResponse::from(&snapshot))?;
    assert_eq!(body["processes"].as_array().map(Vec::len), Some(2));
    assert!(
        body["processes"][0]["resource"]
            .get("tenon.node.id")
            .is_none()
    );
    let text = prometheus::render(&snapshot);
    assert_eq!(
        text.matches("# TYPE tenon_pipeline_memory_bytes gauge\n")
            .count(),
        1
    );
    assert_eq!(
        text.lines()
            .filter(|line| line.starts_with("tenon_pipeline_memory_bytes{"))
            .count(),
        2
    );
    assert!(text.contains("tenon_pipeline_id=\"first\""));
    assert!(text.contains("tenon_pipeline_id=\"second\""));
    first.shutdown();
    second.shutdown();
    Ok(())
}

#[test]
fn json_preserves_decimal_boundaries_and_omits_absent_values() -> io::Result<()> {
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Histogram, HistogramDataPoint, Metric, MetricsData, NumberDataPoint,
        ResourceMetrics, ScopeMetrics, metric, number_data_point,
    };

    assert_eq!(
        serde_json::to_vec(&MetricsResponse::from(&MetricsData::default()))?,
        br#"{"processes":[]}"#
    );
    let integers = [
        (i64::MIN, "-9223372036854775808"),
        (0, "0"),
        (9_007_199_254_740_993, "9007199254740993"),
        (i64::MAX, "9223372036854775807"),
    ];
    let mut points: Vec<_> = integers
        .iter()
        .map(|&(value, _)| NumberDataPoint {
            time_unix_nano: u64::MAX,
            value: Some(number_data_point::Value::AsInt(value)),
            ..Default::default()
        })
        .collect();
    points.push(NumberDataPoint {
        value: Some(number_data_point::Value::AsDouble(0.25)),
        ..Default::default()
    });
    let snapshot = MetricsData {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![
                    Metric {
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: points,
                        })),
                        ..Default::default()
                    },
                    Metric {
                        data: Some(metric::Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                time_unix_nano: u64::MAX,
                                count: u64::MAX,
                                ..Default::default()
                            }],
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let body: serde_json::Value =
        serde_json::from_slice(&serde_json::to_vec(&MetricsResponse::from(&snapshot))?)?;
    let metrics = &body["processes"][0]["metrics"];
    for (index, (_, expected)) in integers.iter().enumerate() {
        let point = &metrics[0]["points"][index];
        assert_eq!(point["value"], *expected);
        assert_eq!(point["timeUnixNano"], "18446744073709551615");
        assert!(point.get("startTimeUnixNano").is_none());
    }
    assert_eq!(metrics[0]["points"][4]["value"], 0.25);
    assert_eq!(metrics[0]["points"][4]["timeUnixNano"], "0");
    let histogram = &metrics[1]["points"][0];
    assert_eq!(histogram["count"], "18446744073709551615");
    assert_eq!(histogram["timeUnixNano"], "18446744073709551615");
    assert_eq!(histogram["startTimeUnixNano"], "0");
    for optional in ["sum", "min", "max"] {
        assert!(histogram.get(optional).is_none());
    }
    Ok(())
}
