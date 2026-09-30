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

#[test]
fn first_cpu_sample_is_missing_and_filters_do_not_sample_cpu() -> Result<(), crate::Error> {
    let collector = Collector::new(&[7; 16]);
    let memory = MetricsData::decode(
        collector
            .collect(vec!["tenon.plugin.memory".into()])?
            .as_slice(),
    )?;
    assert_eq!(
        memory.resource_metrics[0].scope_metrics[0].metrics[0].name,
        "tenon.plugin.memory"
    );
    let first = MetricsData::decode(
        collector
            .collect(vec!["tenon.plugin.cpu".into()])?
            .as_slice(),
    )?;
    assert!(first.resource_metrics.is_empty());
    let second = MetricsData::decode(
        collector
            .collect(vec!["tenon.plugin.cpu".into()])?
            .as_slice(),
    )?;
    assert_eq!(
        second.resource_metrics[0].scope_metrics[0].metrics[0].name,
        "tenon.plugin.cpu"
    );
    let unknown = MetricsData::decode(collector.collect(vec!["unknown".into()])?.as_slice())?;
    assert!(unknown.resource_metrics.is_empty());
    Ok(())
}

#[test]
fn shared_metrics_wire_vectors() -> Result<(), crate::Error> {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../contracts/test-fixtures/process_metrics_test_vectors.json"
    ))?;
    for kind in ["valid", "malformed"] {
        for vector in vectors[kind].as_array().ok_or("vectors missing")? {
            let bytes: Vec<u8> = serde_json::from_value(vector["encoded"].clone())?;
            let encoded = if vector["direction"] == "plugin" {
                plugin::PluginToPipelineMetrics::decode(bytes.as_slice())
                    .map(|message| message.encode_to_vec())
            } else {
                plugin::PipelineToPluginMetrics::decode(bytes.as_slice())
                    .map(|message| message.encode_to_vec())
            };
            if kind == "valid" {
                assert_eq!(encoded?, bytes);
            } else {
                assert!(encoded.is_err());
            }
        }
    }
    Ok(())
}

#[cfg(feature = "repository-test-support")]
mod connection;
