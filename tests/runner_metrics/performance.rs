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
#[ignore = "release-only end-to-end performance comparison; run without other build or test loads"]
fn compare_metrics_business_overhead() -> io::Result<()> {
    if cfg!(debug_assertions) {
        return Err(io::Error::other("run this comparison with --release"));
    }
    let mut runs = Vec::new();
    for round in 0..3 {
        for offset in 0..3 {
            let mode =
                [Mode::NoScrape, Mode::Scrape, Mode::ConcurrentScrapes][(round + offset) % 3];
            runs.push(measure(mode, round)?);
        }
    }
    let report = json!({"target":std::env::consts::ARCH,"os":std::env::consts::OS,"profile":"release","parallelism":1,"targets":1,"payloadBytes":256,"maxInFlight":32,"warmupRecords":20_000,"runs":runs});
    let output = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/metrics-performance.json");
    fs::write(&output, serde_json::to_vec_pretty(&report)?)?;
    println!("Performance comparison saved to {}", output.display());
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    NoScrape,
    Scrape,
    ConcurrentScrapes,
}

fn measure(mode: Mode, round: usize) -> io::Result<serde_json::Value> {
    let name = match mode {
        Mode::NoScrape => "no-scrape",
        Mode::Scrape => "scrape",
        Mode::ConcurrentScrapes => "concurrent-scrapes",
    };
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = configure(directory.path(), address)?;
    let mut runner = TestRunner::spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    assert_eq!(
        request(
            address,
            "POST",
            "/plugins",
            &[("Content-Type", "application/octet-stream")],
            &plugin_package(PluginInterface::SourceAndSink)?
        )?
        .status,
        201
    );
    let mut document = document();
    document["flows"]["loop"]["maxPendingRecords"] = json!(64);
    document["flows"]["loop"]["maxRecordBytes"] = json!(1024);
    // Field 1 is unknown to this empty test contract, but the payload is valid Protobuf.
    let mut payload = vec![10, 253, 1];
    payload.extend([42; 253]);
    document["pluginInstances"]["gateway"]["config"] = json!({"traffic":{"session":"benchmark","benchmark":{"warmupRecords":20_000,"records":200_000,"maxInFlight":32,"payload":payload}}});
    document["flows"]["loop"]["process"]["script"] = json!(
        "local builder = registry:getBuilder('com.example.gateway@1.0.0'); function main(event) emit(builder:build()); emit() end"
    );
    let created = request(
        address,
        "PUT",
        "/documents/observed",
        &[
            ("If-None-Match", "*"),
            ("Content-Type", "application/jsonc"),
        ],
        &serde_json::to_vec(&document)?,
    )?;
    assert_eq!(created.status, 201, "{}", created.body_text());
    wait_until(|| {
        Ok(
            (!file_tree::named_files(directory.path(), "benchmark-started.received")?.is_empty())
                .then_some(()),
        )
    })?;
    let marker = file_tree::named_files(directory.path(), "benchmark-started.received")?.remove(0);
    let root = marker
        .parent()
        .ok_or_else(|| io::Error::other("benchmark marker must have a parent"))?;
    let pid = marker_pid(directory.path(), "parent.pid")?;
    let mut peak_rss = 0;
    let mut peak_threads = 0;
    let mut cpu_samples = Vec::new();
    let mut requests = 0;
    let mut bytes = 0;
    let mut scrape_seconds = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(120);
    let result_path = root.join("benchmark-result.json");
    while !result_path.exists() {
        if Instant::now() > deadline {
            return Err(io::Error::other("Benchmark did not finish"));
        }
        let output = Command::new("ps")
            .args(["-p", &pid.to_string(), "-o", "rss=", "-o", "%cpu="])
            .output()?;
        assert!(output.status.success());
        let text = String::from_utf8_lossy(&output.stdout);
        let fields: Vec<_> = text.split_whitespace().collect();
        peak_rss = peak_rss.max(fields[0].parse::<u64>().map_err(io::Error::other)?);
        cpu_samples.push(fields[1].parse::<f64>().map_err(io::Error::other)?);
        #[cfg(target_os = "macos")]
        let threads = Command::new("ps")
            .args(["-M", "-p", &pid.to_string(), "-o", "pid="])
            .output()?;
        #[cfg(target_os = "linux")]
        let threads = Command::new("ps")
            .args(["-T", "-p", &pid.to_string(), "-o", "pid="])
            .output()?;
        assert!(threads.status.success());
        peak_threads = peak_threads.max(String::from_utf8_lossy(&threads.stdout).lines().count());
        if mode != Mode::NoScrape {
            let responses = std::thread::scope(|scope| {
                let mut handles = Vec::new();
                let count = if mode == Mode::ConcurrentScrapes {
                    2
                } else {
                    1
                };
                for _ in 0..count {
                    handles.push(scope.spawn(move || {
                        let started = Instant::now();
                        let response = request(address, "GET", "/metrics", &[], &[])?;
                        assert_eq!(response.status, 200);
                        Ok::<_, io::Error>((started.elapsed().as_secs_f64(), response.body.len()))
                    }));
                }
                handles
                    .into_iter()
                    .map(|handle| {
                        handle
                            .join()
                            .map_err(|_| io::Error::other("Scrape worker panicked"))?
                    })
                    .collect::<io::Result<Vec<_>>>()
            })?;
            for (seconds, size) in responses {
                requests += 1;
                bytes += size;
                scrape_seconds.push(seconds);
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let mut result: serde_json::Value = serde_json::from_slice(&fs::read(result_path)?)?;
    if round == 0 && mode == Mode::Scrape {
        let response = request(address, "GET", "/metrics?format=prometheus", &[], &[])?;
        assert_eq!(response.status, 200);
        fs::write(
            Path::new(env!("CARGO_MANIFEST_DIR")).join("target/metrics-performance.prom"),
            response.body,
        )?;
    }
    let stopping = Instant::now();
    runner.terminate()?;
    result["exitSeconds"] = json!(stopping.elapsed().as_secs_f64());
    result["mode"] = json!(name);
    result["round"] = json!(round);
    result["pipelinePeakRssKiB"] = json!(peak_rss);
    result["pipelinePeakThreads"] = json!(peak_threads);
    result["pipelinePsCpuPercentSamples"] = json!(cpu_samples);
    result["metricsHttpRequests"] = json!(requests);
    result["metricsJsonBytes"] = json!(bytes);
    result["scrapeSeconds"] = json!(scrape_seconds);
    assert!(
        result["exitSeconds"]
            .as_f64()
            .is_some_and(|seconds| seconds < 2.0)
    );
    assert_eq!(result["records"], 200_000);
    assert_eq!(requests == 0, mode == Mode::NoScrape);
    println!("{name} round {round}: {result}");
    Ok(result)
}
