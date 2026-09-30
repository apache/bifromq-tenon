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

//! Bounded end-to-end traffic; no per-record logging or polling timer.
use super::*;
use std::collections::VecDeque;
use std::time::Instant;
use tenon::runner_test_support::contracts::source::{IngressCompletion, IngressCompletionStatus};
use tenon_ipc::queue::QueueWriter;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct BenchmarkConfig {
    start_gate: Option<String>,
    warmup_records: u64,
    records: u64,
    max_in_flight: usize,
    payload: Vec<u8>,
}

pub(super) struct Benchmark {
    config: BenchmarkConfig,
    writer: QueueWriter,
    pending: VecDeque<(u64, Instant)>,
    completed: u64,
    egress_received: u64,
    started: Option<Instant>,
    latencies: Vec<u64>,
    encoded: Vec<u8>,
}

impl Benchmark {
    pub(super) fn open(
        root: &Path,
        config: BenchmarkConfig,
        channel_bell_path: Option<&Path>,
    ) -> Result<Self, Box<dyn Error>> {
        if config.records == 0 || config.max_in_flight == 0 {
            return Err("Benchmark needs records and in-flight capacity".into());
        }
        let source_directory = root.join(source_directory_name());
        Ok(Self {
            writer: open_writer(
                &source_directory.join("submission-0.queue"),
                &loops_bell_path(&source_directory),
                0,
                channel_bell_path.ok_or("Source launch has no Channel region")?,
            )?,
            pending: VecDeque::with_capacity(config.max_in_flight),
            latencies: Vec::with_capacity(config.records as usize),
            config,
            completed: 0,
            egress_received: 0,
            started: None,
            encoded: Vec::new(),
        })
    }

    pub(super) fn tick(
        &mut self,
        root: &Path,
        completions: &mut [(String, QueueReader)],
        egress: &mut [QueueReader],
    ) -> Result<(), Box<dyn Error>> {
        let total = self.config.warmup_records + self.config.records;
        if self.completed == total {
            return Ok(());
        }
        for reader in egress {
            while let ReadOutcome::Record(_) = reader.try_read()? {
                reader.release(1)?;
                self.egress_received += 1;
            }
        }
        for (_, reader) in completions {
            while let ReadOutcome::Record(record) = reader.try_read()? {
                let completion = IngressCompletion::decode(record.payload())?;
                let (expected, submitted) =
                    self.pending.pop_front().ok_or("Unexpected Completion")?;
                if completion.record_id != expected
                    || completion.status != IngressCompletionStatus::Ok as i32
                {
                    return Err(format!(
                        "Benchmark expected OK for record {expected}, got record {} with status {}",
                        completion.record_id, completion.status
                    )
                    .into());
                }
                if expected > self.config.warmup_records {
                    self.latencies.push(submitted.elapsed().as_nanos() as u64);
                }
                self.completed += 1;
                reader.release(1)?;
            }
        }
        if self.completed == total {
            if self.egress_received != total {
                return Err("Benchmark lost Egress records".into());
            }
            let elapsed = self
                .started
                .ok_or("Benchmark did not start")?
                .elapsed()
                .as_secs_f64();
            self.latencies.sort_unstable();
            let result = serde_json::json!({"records":self.config.records,"elapsedSeconds":elapsed,"recordsPerSecond":self.config.records as f64 / elapsed,"completionP99Ns":self.latencies[(self.latencies.len() * 99 / 100).min(self.latencies.len()-1)],"egressRecords":self.egress_received});
            let path = root.join("benchmark-result.json");
            std::fs::write(
                root.join("benchmark-result.tmp"),
                serde_json::to_vec(&result)?,
            )?;
            std::fs::rename(root.join("benchmark-result.tmp"), path)?;
            return Ok(());
        }
        if self.completed == self.config.warmup_records
            && self.started.is_none()
            && self
                .config
                .start_gate
                .as_ref()
                .is_some_and(|gate| !root.join(gate).exists())
        {
            let ready = root.join("benchmark-ready.received");
            if !ready.exists() {
                std::fs::write(ready, [])?;
            }
            return Ok(());
        }
        // Drain the entire warm-up window before timing measurement traffic.
        let limit = if self.completed < self.config.warmup_records {
            self.config.warmup_records
        } else {
            total
        };
        while self.pending.len() < self.config.max_in_flight {
            let id = self.completed + self.pending.len() as u64 + 1;
            if id > limit {
                break;
            }
            if id == self.config.warmup_records + 1 && self.started.is_none() {
                std::fs::write(root.join("benchmark-started.received"), [])?;
                self.started = Some(Instant::now());
            }
            self.encoded.clear();
            IngressRecord {
                record_id: id,
                payload: self.config.payload.clone().into(),
            }
            .encode(&mut self.encoded)?;
            let submitted = Instant::now();
            match self.writer.try_write_with(
                self.encoded.len(),
                |destination| std::io::Write::write_all(destination, &self.encoded),
                || {},
            )? {
                WriteOutcome::Committed(_) => self.pending.push_back((id, submitted)),
                WriteOutcome::Full => break,
            }
        }
        Ok(())
    }
}
