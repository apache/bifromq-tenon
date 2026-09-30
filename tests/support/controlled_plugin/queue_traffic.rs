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

//! Optional finite business traffic through the real controlled child's Queue endpoints.
//! Only the test driver polls; Queue framing, publication and release use the shared core.

#[path = "queue_traffic/benchmark.rs"]
mod benchmark;

use super::*;
use benchmark::{Benchmark, BenchmarkConfig};
use prost::Message as _;
use serde::Deserialize;
use tenon::runner_test_support::contracts::source::IngressRecord;
use tenon::runner_test_support::{loops_bell_path, sink_directory_name, source_directory_name};
use tenon_ipc::queue::contract_test_support::{open_reader, open_writer};
use tenon_ipc::queue::{QueueReader, ReadOutcome, WriteOutcome};

pub(super) struct QueueTraffic {
    benchmark: Option<Benchmark>,
    session: String,
    completion_gate: Option<String>,
    egress_gate: Option<String>,
    completions: Vec<(String, QueueReader)>,
    egress: Vec<QueueReader>,
}

impl QueueTraffic {
    pub(super) fn open(
        root: &Path,
        config: &Value,
        channels: Vec<SinkChannel>,
        channel_bell_path: Option<&Path>,
    ) -> Result<Option<Self>, Box<dyn Error>> {
        let Some(config) = config.get("traffic") else {
            return Ok(None);
        };
        let config: TrafficConfig = serde_json::from_value(config.clone())?;
        // A Source's own loops park on exactly two doorbells inside the region
        // under the Source directory, and ring the Flow's Channel region the
        // Runner named on startup when they commit or release.
        let source_directory = root.join(source_directory_name());
        let source_region = loops_bell_path(&source_directory);
        let mut completions = Vec::new();
        if source_directory.exists() {
            let channel_region = channel_bell_path.ok_or("Source launch has no Channel region")?;
            for entry in std::fs::read_dir(&source_directory)? {
                let path = entry?.path();
                let name = path
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .ok_or("Fixture Queue name is not UTF-8")?;
                let Some(_) = name
                    .strip_prefix("completion-")
                    .and_then(|index| index.parse::<u32>().ok())
                else {
                    continue;
                };
                completions.push((
                    name.to_owned(),
                    open_reader(&path, &source_region, 1, channel_region)?,
                ));
            }
        }
        for submission in &config.submissions {
            let path =
                source_directory.join(format!("submission-{}.queue", submission.channel_index));
            let record = IngressRecord {
                record_id: submission.record_id,
                payload: submission.payload.clone().into(),
            };
            let mut writer = open_writer(
                &path,
                &source_region,
                0,
                channel_bell_path.ok_or("Source launch has no Channel region")?,
            )?;
            if !matches!(
                writer.try_write_with(
                    record.encoded_len(),
                    |destination| record.encode(destination).map_err(std::io::Error::other),
                    || {}
                )?,
                WriteOutcome::Committed(_)
            ) {
                return Err("Fixture Source admission exceeded its bounded Queue".into());
            }
        }
        std::fs::write(
            root.join(format!("traffic-{}-submitted.received", config.session)),
            [],
        )?;

        // A Sink reads every Egress input from one loop, so all of them publish
        // the same own-loop doorbell, and releasing an input rings that input's
        // Channel region.
        let sink_directory = root.join(sink_directory_name());
        let mut egress = Vec::new();
        for channel in channels {
            egress.push(open_reader(
                &tenon::runner_test_support::egress_queue_path(
                    root,
                    &channel.flow_id,
                    channel.channel_id,
                ),
                &loops_bell_path(&sink_directory),
                0,
                &channel.channel_bell_path,
            )?);
        }
        Ok(Some(Self {
            benchmark: config
                .benchmark
                .map(|config| Benchmark::open(root, config, channel_bell_path))
                .transpose()?,
            session: config.session,
            completion_gate: config.completion_gate,
            egress_gate: config.egress_gate,
            completions,
            egress,
        }))
    }

    fn tick(&mut self, root: &Path) -> Result<(), Box<dyn Error>> {
        if let Some(benchmark) = &mut self.benchmark {
            return benchmark.tick(root, &mut self.completions, &mut self.egress);
        }
        if self
            .completion_gate
            .as_ref()
            .is_none_or(|gate| root.join(gate).exists())
        {
            for (name, queue) in &mut self.completions {
                drain(
                    queue,
                    &root.join(format!("traffic-{}-{name}.received", self.session)),
                )?;
            }
        }
        if self
            .egress_gate
            .as_ref()
            .is_none_or(|gate| root.join(gate).exists())
        {
            for queue in &mut self.egress {
                drain(
                    queue,
                    &root.join(format!("traffic-{}-egress.received", self.session)),
                )?;
            }
        }
        Ok(())
    }
}

pub(super) async fn run(traffic: Option<QueueTraffic>, root: &Path) -> Result<(), Box<dyn Error>> {
    let Some(mut traffic) = traffic else {
        return std::future::pending().await;
    };
    loop {
        traffic.tick(root)?;
        if traffic.benchmark.is_some() {
            tokio::task::yield_now().await;
        } else {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
}

fn drain(queue: &mut QueueReader, log: &Path) -> Result<(), Box<dyn Error>> {
    while let ReadOutcome::Record(record) = queue.try_read()? {
        let bytes = record.payload().to_vec();
        queue.release(1)?;
        append(log, &format!("{}\n", STANDARD.encode(bytes)))?;
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrafficConfig {
    benchmark: Option<BenchmarkConfig>,
    session: String,
    #[serde(default)]
    submissions: Vec<Submission>,
    completion_gate: Option<String>,
    egress_gate: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Submission {
    channel_index: u32,
    record_id: u64,
    payload: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SinkChannel {
    flow_id: String,
    channel_id: u32,
    channel_bell_path: PathBuf,
}
