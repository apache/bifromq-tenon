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
use opentelemetry_proto::tonic::metrics::v1::Sum;
use std::collections::HashSet;
use std::io;

#[test]
fn catalog_prometheus_names_and_histogram_expansions_do_not_collide() -> io::Result<()> {
    let catalog: serde_json::Value = serde_json::from_slice(include_bytes!(
        "../../../../../contracts/metrics/catalog.json"
    ))?;
    let mut names = HashSet::new();
    for definition in catalog["metrics"]
        .as_array()
        .ok_or_else(|| io::Error::other("Missing catalog"))?
    {
        let metric = Metric {
            name: definition["name"].as_str().unwrap_or_default().to_owned(),
            unit: definition["unit"].as_str().unwrap_or_default().to_owned(),
            data: (definition["kind"] == "counter").then(|| metric::Data::Sum(Sum::default())),
            ..Metric::default()
        };
        let name = name(&metric);
        assert!(names.insert(name.clone()), "{name}");
        if definition["kind"] == "histogram" {
            for suffix in ["count", "sum", "min", "max"] {
                assert!(names.insert(format!("{name}_{suffix}")), "{name}_{suffix}");
            }
        }
    }
    Ok(())
}

#[test]
fn histogram_series_preserve_escaped_unicode_labels_and_absent_statistics() {
    use opentelemetry_proto::tonic::common::v1::AnyValue;
    use opentelemetry_proto::tonic::metrics::v1::{
        Histogram, HistogramDataPoint, ResourceMetrics, ScopeMetrics,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;

    let attribute = |key: &str, value| KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    };
    let snapshot = MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![attribute(
                    "tenon.node.id",
                    any_value::Value::StringValue("node".into()),
                )],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "tenon.flow.lua.duration".into(),
                    unit: "s".into(),
                    data: Some(metric::Data::Histogram(Histogram {
                        data_points: vec![
                            HistogramDataPoint {
                                attributes: vec![
                                    attribute(
                                        "tenon.flow.id",
                                        any_value::Value::StringValue("quoted\"\\flow\nµ".into()),
                                    ),
                                    attribute("tenon.channel.index", any_value::Value::IntValue(7)),
                                ],
                                count: 2,
                                sum: Some(1.0),
                                min: Some(0.25),
                                max: Some(0.75),
                                ..Default::default()
                            },
                            HistogramDataPoint::default(),
                        ],
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let labels =
        r#"{tenon_node_id="node",tenon_flow_id="quoted\"\\flow\nµ",tenon_channel_index="7"}"#;
    assert_eq!(
        render(&snapshot),
        format!(
            "# HELP tenon_flow_lua_duration_seconds tenon.flow.lua.duration\n\
             # TYPE tenon_flow_lua_duration_seconds summary\n\
             tenon_flow_lua_duration_seconds_count{labels} 2\n\
             tenon_flow_lua_duration_seconds_sum{labels} 1\n\
             tenon_flow_lua_duration_seconds_count{{tenon_node_id=\"node\"}} 0\n\
             # HELP tenon_flow_lua_duration_seconds_min tenon.flow.lua.duration cumulative min\n\
             # TYPE tenon_flow_lua_duration_seconds_min gauge\n\
             tenon_flow_lua_duration_seconds_min{labels} 0.25\n\
             # HELP tenon_flow_lua_duration_seconds_max tenon.flow.lua.duration cumulative max\n\
             # TYPE tenon_flow_lua_duration_seconds_max gauge\n\
             tenon_flow_lua_duration_seconds_max{labels} 0.75\n"
        )
    );
}
