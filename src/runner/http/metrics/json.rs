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

//! Presents standard process snapshots as typed, lossless Tenon JSON.

use opentelemetry_proto::tonic::common::v1::{KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    MetricsData, NumberDataPoint, metric, number_data_point,
};
use serde::{Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt::Display;
use utoipa::ToSchema;

#[derive(Serialize, ToSchema)]
pub(super) struct MetricsResponse<'a> {
    processes: Vec<Process<'a>>,
}

impl<'a> From<&'a MetricsData> for MetricsResponse<'a> {
    fn from(snapshot: &'a MetricsData) -> Self {
        let mut processes = Vec::new();
        for resource in &snapshot.resource_metrics {
            let mut metrics = Vec::new();
            for metric in resource
                .scope_metrics
                .iter()
                .flat_map(|scope| &scope.metrics)
            {
                let (kind, temporality, points) = match &metric.data {
                    Some(metric::Data::Gauge(data)) => (
                        Kind::Gauge,
                        None,
                        data.data_points
                            .iter()
                            .map(|point| number(point, None))
                            .collect(),
                    ),
                    Some(metric::Data::Sum(data)) => (
                        Kind::Counter,
                        Some(Temporality::Cumulative),
                        data.data_points
                            .iter()
                            .map(|point| number(point, Some(point.start_time_unix_nano)))
                            .collect(),
                    ),
                    Some(metric::Data::Histogram(data)) => (
                        Kind::Histogram,
                        Some(Temporality::Cumulative),
                        data.data_points
                            .iter()
                            .map(|point| Point::Histogram {
                                observation: Observation {
                                    attributes: attributes(&point.attributes),
                                    time_unix_nano: point.time_unix_nano,
                                    start_time_unix_nano: Some(point.start_time_unix_nano),
                                },
                                count: point.count,
                                sum: point.sum,
                                min: point.min,
                                max: point.max,
                            })
                            .collect(),
                    ),
                    _ => unreachable!(
                        "core metrics only record gauges, counters and explicit histograms"
                    ),
                };
                metrics.push(Metric {
                    name: &metric.name,
                    kind,
                    unit: &metric.unit,
                    temporality,
                    points,
                });
            }
            processes.push(Process {
                resource: attributes(
                    resource
                        .resource
                        .as_ref()
                        .map_or(&[], |value| value.attributes.as_slice()),
                ),
                metrics,
            });
        }
        Self { processes }
    }
}

#[derive(Serialize, ToSchema)]
struct Process<'a> {
    resource: Attributes<'a>,
    metrics: Vec<Metric<'a>>,
}

type Attributes<'a> = BTreeMap<&'a str, Attribute<'a>>;

#[derive(Serialize, ToSchema)]
#[serde(untagged)]
enum Attribute<'a> {
    Text(&'a str),
    Integer(i64),
    Boolean(bool),
}

#[derive(Serialize, ToSchema)]
struct Metric<'a> {
    name: &'a str,
    #[serde(rename = "type")]
    kind: Kind,
    unit: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    temporality: Option<Temporality>,
    points: Vec<Point<'a>>,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
enum Kind {
    Gauge,
    Counter,
    Histogram,
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
enum Temporality {
    Cumulative,
}

#[derive(Serialize, ToSchema)]
#[serde(untagged)]
enum Point<'a> {
    Number {
        #[serde(flatten)]
        observation: Observation<'a>,
        value: Number,
    },
    Histogram {
        #[serde(flatten)]
        observation: Observation<'a>,
        #[serde(serialize_with = "decimal_string")]
        #[schema(value_type = String)]
        count: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        sum: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        min: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max: Option<f64>,
    },
}

#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct Observation<'a> {
    attributes: Attributes<'a>,
    #[serde(serialize_with = "decimal_string")]
    #[schema(value_type = String)]
    time_unix_nano: u64,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "optional_decimal_string"
    )]
    #[schema(value_type = Option<String>)]
    start_time_unix_nano: Option<u64>,
}

#[derive(Serialize, ToSchema)]
#[serde(untagged)]
enum Number {
    /// An exact 64-bit integer encoded as a decimal string.
    #[schema(value_type = String)]
    Integer(#[serde(serialize_with = "decimal_string")] i64),
    Float(f64),
}

fn number(point: &NumberDataPoint, start: Option<u64>) -> Point<'_> {
    Point::Number {
        observation: Observation {
            attributes: attributes(&point.attributes),
            time_unix_nano: point.time_unix_nano,
            start_time_unix_nano: start,
        },
        value: match point.value {
            Some(number_data_point::Value::AsInt(value)) => Number::Integer(value),
            Some(number_data_point::Value::AsDouble(value)) => Number::Float(value),
            None => unreachable!("the SDK supplies each number data point's value"),
        },
    }
}

fn decimal_string<T: Display, S: Serializer>(value: &T, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

fn optional_decimal_string<S: Serializer>(
    value: &Option<u64>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(value) => decimal_string(value, serializer),
        None => serializer.serialize_none(),
    }
}

fn attributes(values: &[KeyValue]) -> Attributes<'_> {
    values
        .iter()
        .map(|pair| {
            let value = match pair.value.as_ref().and_then(|value| value.value.as_ref()) {
                Some(any_value::Value::StringValue(value)) => Attribute::Text(value),
                Some(any_value::Value::IntValue(value)) => Attribute::Integer(*value),
                Some(any_value::Value::BoolValue(value)) => Attribute::Boolean(*value),
                _ => unreachable!(
                    "core resource and point attributes are strings, bounded integers or booleans"
                ),
            };
            (pair.key.as_str(), value)
        })
        .collect()
}
