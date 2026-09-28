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
use cpu_time::ProcessTime;
use opentelemetry_proto::tonic::metrics::v1::number_data_point;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};

/// A snapshot and the in-process measurement of the same window must agree within this rate.
const CPU_RATE_TOLERANCE: f64 = 0.3;

#[tokio::test(flavor = "current_thread")]
async fn pull_snapshot_tracks_cpu_and_current_rss_without_including_children() -> io::Result<()> {
    let runtime = MetricsRuntime::start(Some("resource-test"), CoreProcess::Runner)
        .map_err(io::Error::other)?;
    let idle = sample_phase(&runtime, "idle").await?;
    assert!(
        idle.cpu <= idle.expected + CPU_RATE_TOLERANCE,
        "idle: Snapshot {} above measured process interval {}",
        idle.cpu,
        idle.expected
    );
    for threads in [1, 2] {
        let load = CpuLoad::start(threads)?;
        let sample = sample_phase(&runtime, &format!("{threads}-thread CPU")).await?;
        drop(load);
        assert!(sample.cpu > 0.4, "busy CPU was {}", sample.cpu);
        if threads == 2 && std::thread::available_parallelism()?.get() >= 2 {
            assert!(sample.cpu > 1.2, "two-core CPU was {}", sample.cpu);
        }
    }
    let mut memory = memmap2::MmapMut::map_anon(64 * 1024 * 1024)?;
    getrandom::fill(&mut memory).map_err(io::Error::other)?;
    std::hint::black_box(&memory);
    let allocated = sample_phase(&runtime, "64 MiB allocated").await?;
    assert_same_os_rss(allocated.rss)?;
    drop(memory);
    let released = sample_phase(&runtime, "mapping released").await?;
    assert_same_os_rss(released.rss)?;

    let directory = tempfile::tempdir()?;
    let ready = directory.path().join("ready");
    let mut child = tokio::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "metrics::runtime::tests::resources::resource_load_child",
            "--ignored",
            "--nocapture",
        ])
        .env("TENON_METRICS_TEST_CHILD_READY", &ready)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ready.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .map_err(io::Error::other)?;
    let child_load = sample_phase(&runtime, "only child busy with 128 MiB").await?;
    drop(child.stdin.take());
    assert!(child.wait().await?.success());
    // Child work must not appear in this process's own rate, whatever else shares the process.
    assert!(
        child_load.cpu <= child_load.expected + CPU_RATE_TOLERANCE,
        "child CPU leaked into parent: Snapshot {} process interval {}",
        child_load.cpu,
        child_load.expected
    );
    assert!(child_load.rss < released.rss + 32 * 1024 * 1024);
    runtime.shutdown();
    Ok(())
}

#[test]
#[ignore = "child process entry point invoked by the parent resource test"]
fn resource_load_child() -> io::Result<()> {
    let Some(ready) = std::env::var_os("TENON_METRICS_TEST_CHILD_READY") else {
        return Ok(());
    };
    let mut memory = memmap2::MmapMut::map_anon(128 * 1024 * 1024)?;
    for page in memory.chunks_mut(4096) {
        page[0] = 1;
    }
    std::hint::black_box(&memory);
    let _load = CpuLoad::start(1)?;
    std::fs::write(Path::new(&ready), [])?;
    std::io::Read::read_to_end(&mut std::io::stdin(), &mut Vec::new())?;
    Ok(())
}

struct Sample {
    cpu: f64,
    expected: f64,
    rss: u64,
}

async fn sample_phase(runtime: &MetricsRuntime, name: &str) -> io::Result<Sample> {
    // Establish the sampling baseline at the start of this measured workload.
    runtime.collect(&[]);
    let start = std::time::Instant::now();
    let cpu_start = ProcessTime::try_now()?;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let expected = cpu_start.try_elapsed()?.as_secs_f64() / start.elapsed().as_secs_f64();
    let mut cpus = Vec::new();
    let mut rss = None;
    {
        let export = runtime.collect(&[]);
        for metric in export
            .resource_metrics
            .iter()
            .flat_map(|resource| &resource.scope_metrics)
            .flat_map(|scope| &scope.metrics)
        {
            if let Some(metric::Data::Gauge(gauge)) = &metric.data {
                for point in &gauge.data_points {
                    match (&*metric.name, &point.value) {
                        ("tenon.process.cpu", Some(number_data_point::Value::AsDouble(value))) => {
                            cpus.push(*value)
                        }
                        ("tenon.process.memory", Some(number_data_point::Value::AsInt(value))) => {
                            rss = Some(*value as u64)
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    assert_eq!(cpus.len(), 1, "one CPU sample for {name}");
    let cpu = cpus.iter().sum::<f64>() / cpus.len() as f64;
    assert!(
        (cpu - expected).abs() < CPU_RATE_TOLERANCE,
        "{name}: Snapshot {cpu}, measured process interval {expected}"
    );
    let rss = rss.ok_or_else(|| io::Error::other("RSS sample missing"))?;
    println!("{name}: Snapshot CPU {cpu:.3}, interval CPU {expected:.3}, RSS {rss} bytes");
    Ok(Sample { cpu, expected, rss })
}

fn assert_same_os_rss(observed: u64) -> io::Result<()> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()?;
    assert!(output.status.success());
    let expected = String::from_utf8(output.stdout)
        .map_err(io::Error::other)?
        .trim()
        .parse::<u64>()
        .map_err(io::Error::other)?
        * 1024;
    assert!(
        observed.abs_diff(expected) < 16 * 1024 * 1024,
        "Snapshot RSS {observed}, platform ps RSS {expected}"
    );
    Ok(())
}

struct CpuLoad {
    stop: Arc<AtomicBool>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl CpuLoad {
    fn start(count: usize) -> io::Result<Self> {
        let mut load = Self {
            stop: Arc::default(),
            threads: Vec::new(),
        };
        for _ in 0..count {
            let stop = Arc::clone(&load.stop);
            load.threads
                .push(std::thread::Builder::new().spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        std::hint::spin_loop();
                    }
                })?);
        }
        Ok(load)
    }
}

impl Drop for CpuLoad {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}
