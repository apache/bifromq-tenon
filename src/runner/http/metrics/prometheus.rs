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

//! Writes Prometheus text without inventing buckets, quantiles or process aliases.

use opentelemetry_proto::tonic::common::v1::{KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{
    Metric, MetricsData, NumberDataPoint, metric, number_data_point,
};
use std::collections::BTreeMap;
use std::fmt::Write as _;

#[allow(clippy::expect_used, reason = "formatting into a String cannot fail")]
pub(super) fn render(snapshot: &MetricsData) -> String {
    let mut families = BTreeMap::<&str, Vec<(&[KeyValue], &Metric)>>::new();
    for resource in &snapshot.resource_metrics {
        let attributes: &[KeyValue] = resource
            .resource
            .as_ref()
            .map_or(&[], |resource| resource.attributes.as_slice());
        for metric in resource
            .scope_metrics
            .iter()
            .flat_map(|scope| &scope.metrics)
        {
            families
                .entry(&metric.name)
                .or_default()
                .push((attributes, metric));
        }
    }
    let mut output = String::new();
    for (raw_name, metrics) in families {
        let first = metrics[0].1;
        let name = name(first);
        let kind = match first.data {
            Some(metric::Data::Gauge(_)) => "gauge",
            Some(metric::Data::Sum(_)) => "counter",
            Some(metric::Data::Histogram(_)) => "summary",
            _ => unreachable!("core metrics only record gauges, counters and explicit histograms"),
        };
        writeln!(output, "# HELP {name} {raw_name}\n# TYPE {name} {kind}").expect("String write");
        for (resource, metric) in &metrics {
            match &metric.data {
                Some(metric::Data::Gauge(data)) => {
                    write_numbers(&mut output, &name, resource, &data.data_points)
                }
                Some(metric::Data::Sum(data)) => {
                    write_numbers(&mut output, &name, resource, &data.data_points)
                }
                Some(metric::Data::Histogram(data)) => {
                    for point in &data.data_points {
                        write!(output, "{name}_count").expect("String write");
                        let labels_start = output.len();
                        write_labels(&mut output, resource, &point.attributes);
                        let labels_end = output.len();
                        writeln!(output, " {}", point.count).expect("String write");
                        if let Some(sum) = point.sum {
                            write!(output, "{name}_sum").expect("String write");
                            // Reuse count's labels without formatting them again.
                            output.extend_from_within(labels_start..labels_end);
                            writeln!(output, " {sum}").expect("String write");
                        }
                    }
                }
                _ => unreachable!(
                    "core metrics only record gauges, counters and explicit histograms"
                ),
            }
        }
        if let Some(metric::Data::Histogram(_)) = first.data {
            for statistic in ["min", "max"] {
                writeln!(output, "# HELP {name}_{statistic} {raw_name} cumulative {statistic}\n# TYPE {name}_{statistic} gauge").expect("String write");
                for (resource, metric) in &metrics {
                    let Some(metric::Data::Histogram(data)) = &metric.data else {
                        unreachable!("a catalog name has exactly one kind");
                    };
                    for point in &data.data_points {
                        if let Some(value) = if statistic == "min" {
                            point.min
                        } else {
                            point.max
                        } {
                            write!(output, "{name}_{statistic}").expect("String write");
                            write_labels(&mut output, resource, &point.attributes);
                            writeln!(output, " {value}").expect("String write");
                        }
                    }
                }
            }
        }
    }
    output
}

fn name(metric: &Metric) -> String {
    let mut name = metric.name.replace('.', "_");
    let suffix = match metric.unit.as_str() {
        "By" => "_bytes",
        "s" => "_seconds",
        _ => "",
    };
    if !name.ends_with(suffix) {
        name.push_str(suffix);
    }
    if matches!(metric.data, Some(metric::Data::Sum(_))) {
        name.push_str("_total");
    }
    name
}

#[allow(clippy::expect_used, reason = "formatting into a String cannot fail")]
fn write_numbers(
    output: &mut String,
    name: &str,
    resource: &[KeyValue],
    points: &[NumberDataPoint],
) {
    for point in points {
        write!(output, "{name}").expect("String write");
        write_labels(output, resource, &point.attributes);
        match point.value {
            Some(number_data_point::Value::AsInt(value)) => {
                writeln!(output, " {value}")
            }
            Some(number_data_point::Value::AsDouble(value)) => {
                writeln!(output, " {value}")
            }
            None => unreachable!("the SDK supplies each number data point's value"),
        }
        .expect("String write");
    }
}

#[allow(clippy::expect_used, reason = "formatting into a String cannot fail")]
fn write_labels(output: &mut String, resource: &[KeyValue], point: &[KeyValue]) {
    output.push('{');
    for (index, pair) in resource.iter().chain(point).enumerate() {
        if index > 0 {
            output.push(',');
        }
        for (part_index, part) in pair.key.split('.').enumerate() {
            if part_index > 0 {
                output.push('_');
            }
            output.push_str(part);
        }
        output.push_str("=\"");
        match pair.value.as_ref().and_then(|value| value.value.as_ref()) {
            Some(any_value::Value::StringValue(value)) => {
                for character in value.chars() {
                    match character {
                        '\\' => output.push_str("\\\\"),
                        '"' => output.push_str("\\\""),
                        '\n' => output.push_str("\\n"),
                        other => output.push(other),
                    }
                }
            }
            Some(any_value::Value::IntValue(value)) => {
                write!(output, "{value}").expect("String write")
            }
            Some(any_value::Value::BoolValue(value)) => {
                write!(output, "{value}").expect("String write")
            }
            _ => unreachable!(
                "core resource and point attributes are strings, bounded integers or booleans"
            ),
        }
        output.push('"');
    }
    output.push('}');
}

#[cfg(test)]
mod tests;
