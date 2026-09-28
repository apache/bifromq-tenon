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

//! Isolated IPC baseline; UDS here is never a Plugin transport or test substitute.

use std::error::Error;
use std::fs;
use std::io::{self, Read as _, Write as _};
use std::num::NonZeroU64;
use std::os::unix::net::{UnixListener, UnixStream};
use std::os::unix::process::CommandExt as _;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use cpu_time::ProcessTime;
use rustix::process::{Pid, Signal, WaitId, WaitIdOptions, kill_process_group, waitid};
use serde::{Deserialize, Serialize};
use tenon_ipc::bell::WaitOutcome;
use tenon_ipc::queue::{DataCapacity, ReadOutcome, WriteOutcome, create_queue_file};

#[path = "support/doorbell_queue.rs"]
mod doorbell_queue;

use doorbell_queue::{open_queue_reader, open_queue_writer};

const WARMUP_RECORDS: usize = 32;
const DEADLINE: Duration = Duration::from_secs(30);
const RESULT_PREFIX: &str = "TENON_IPC_BENCH ";

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum Transport {
    Mmap,
    Uds,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    transport: Transport,
    payload_bytes: usize,
    stall_ms: u64,
}

impl Case {
    fn records(self) -> usize {
        (256 * 1024 * 1024 / self.payload_bytes).clamp(256, 65_536)
    }
}

#[test]
#[ignore = "run explicitly in release mode without concurrent builds or benchmarks"]
fn ipc_transport_baseline() -> Result<(), Box<dyn Error>> {
    match std::env::var("TENON_IPC_BENCH_ROLE").ok().as_deref() {
        Some("producer") => {
            let case: Case = serde_json::from_str(&std::env::var("TENON_IPC_BENCH_CASE")?)?;
            println!("{RESULT_PREFIX}{}", run_producer(case)?);
        }
        Some("consumer") => {
            let case: Case = serde_json::from_str(&std::env::var("TENON_IPC_BENCH_CASE")?)?;
            run_consumer(
                case,
                Path::new(
                    &std::env::var_os("TENON_IPC_BENCH_DIRECTORY")
                        .ok_or("missing benchmark directory")?,
                ),
            )?;
        }
        Some(_) => return Err("unknown benchmark child role".into()),
        None => {
            let output = std::env::var_os("TENON_IPC_BENCH_OUTPUT")
                .ok_or("set TENON_IPC_BENCH_OUTPUT to save the benchmark evidence")?;
            let mut samples = Vec::new();
            for repetition in 0..3 {
                for payload_bytes in [1024, 64 * 1024, 1024 * 1024, 16 * 1024 * 1024] {
                    for transport in [Transport::Uds, Transport::Mmap] {
                        let case = Case {
                            transport,
                            payload_bytes,
                            stall_ms: 0,
                        };
                        let mut result = producer_result(spawn_child(ChildRole::Producer, case)?)?;
                        result["repetition"] = serde_json::json!(repetition);
                        result["parallelQueues"] = serde_json::json!(1);
                        println!("{RESULT_PREFIX}{result}");
                        samples.push(result);
                    }
                }
            }
            // Each pair remains SPSC. One delayed reader must not stop the other
            // independent pairs; this measures transport concurrency, not Flow scheduling.
            for transport in [Transport::Uds, Transport::Mmap] {
                let mut children = Vec::new();
                for index in 0..4 {
                    children.push(spawn_child(
                        ChildRole::Producer,
                        Case {
                            transport,
                            payload_bytes: 64 * 1024,
                            stall_ms: if index == 0 { 100 } else { 0 },
                        },
                    )?);
                }
                for child in children {
                    let mut result = producer_result(child)?;
                    result["parallelQueues"] = serde_json::json!(4);
                    println!("{RESULT_PREFIX}{result}");
                    samples.push(result);
                }
            }
            fs::write(
                output,
                serde_json::to_vec_pretty(&serde_json::json!({
                    "platform": format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
                    "workload": "one in-flight record, complete byte comparison before ACK, 32 warmups",
                    "samples": samples,
                }))?,
            )?;
        }
    }
    Ok(())
}

fn run_producer(case: Case) -> Result<serde_json::Value, Box<dyn Error>> {
    let directory =
        std::env::var_os("TENON_IPC_BENCH_DIRECTORY").ok_or("missing benchmark directory")?;
    let directory = Path::new(&directory);
    let queue_path = directory.join("queue");
    let mut writer = match case.transport {
        Transport::Mmap => {
            create_queue_file(
                &queue_path,
                DataCapacity::try_from(((case.payload_bytes + 8) * 2) as u64)?,
                NonZeroU64::new(case.payload_bytes as u64).ok_or("zero benchmark payload")?,
            )?;
            Some(open_queue_writer(&queue_path)?)
        }
        Transport::Uds => None,
    };
    let listener = UnixListener::bind(directory.join("control.sock"))?;
    let mut child = spawn_child(ChildRole::Consumer(directory), case)?;
    wait_for_ready(child.process(), &directory.join("ready"))?;
    let (mut stream, _) = listener.accept()?;
    stream.set_read_timeout(Some(DEADLINE))?;
    stream.set_write_timeout(Some(DEADLINE))?;
    let mut payload = vec![0xa5; case.payload_bytes];
    let mut latencies = Vec::with_capacity(case.records());
    let mut started = Instant::now();
    let mut cpu_started = ProcessTime::try_now()?;
    for sequence in 0..WARMUP_RECORDS + case.records() {
        if sequence == WARMUP_RECORDS {
            started = Instant::now();
            cpu_started = ProcessTime::try_now()?;
        }
        payload[..8].copy_from_slice(&(sequence as u64).to_le_bytes());
        let record_started = Instant::now();
        match writer.as_mut() {
            Some(writer) => {
                let WriteOutcome::Committed(receipt) = writer.try_write(&payload)? else {
                    return Err("single in-flight record unexpectedly filled the Queue".into());
                };
                if writer.wait_released(&receipt)? != WaitOutcome::Ready {
                    return Err("Queue producer exceeded the benchmark deadline".into());
                }
            }
            None => {
                stream.write_all(&(payload.len() as u64).to_le_bytes())?;
                stream.write_all(&payload)?;
                let mut ack = [0];
                stream.read_exact(&mut ack)?;
                assert_eq!(ack, [1]);
            }
        }
        if sequence >= WARMUP_RECORDS {
            latencies.push(record_started.elapsed().as_secs_f64() * 1_000_000.0);
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let producer_cpu = cpu_started.try_elapsed()?.as_secs_f64();
    let producer_rss = peak_rss_bytes()?;
    let mut report = String::new();
    stream.read_to_string(&mut report)?;
    let consumer: serde_json::Value = serde_json::from_str(&report)?;
    assert!(
        child.process().wait()?.success(),
        "benchmark consumer failed"
    );
    child.child.take();
    latencies.sort_by(f64::total_cmp);
    let percentile = |percent: usize| latencies[(latencies.len() * percent).div_ceil(100) - 1];
    Ok(serde_json::json!({
        "case": case, "records": case.records(), "elapsedSeconds": elapsed,
        "mibPerSecond": case.records() as f64 * case.payload_bytes as f64 / (1024.0 * 1024.0) / elapsed,
        "p50Micros": percentile(50), "p99Micros": percentile(99), "maxMicros": latencies.last(),
        "producerCpuSeconds": producer_cpu, "consumerCpuSeconds": consumer["cpuSeconds"],
        "producerPeakRssBytes": producer_rss, "consumerPeakRssBytes": consumer["peakRssBytes"],
    }))
}

fn run_consumer(case: Case, directory: &Path) -> Result<(), Box<dyn Error>> {
    let mut reader = match case.transport {
        Transport::Mmap => Some(open_queue_reader(directory.join("queue"))?),
        Transport::Uds => None,
    };
    let mut stream = UnixStream::connect(directory.join("control.sock"))?;
    stream.set_read_timeout(Some(DEADLINE))?;
    stream.set_write_timeout(Some(DEADLINE))?;
    let mut expected = vec![0xa5; case.payload_bytes];
    // Reuse the UDS reader's owned buffer: the benchmark does not give mmap an
    // artificial advantage by zero-initializing another UDS allocation per record.
    let mut socket_payload = match case.transport {
        Transport::Uds => vec![0; case.payload_bytes],
        Transport::Mmap => Vec::new(),
    };
    fs::write(directory.join("ready"), b"ready")?;
    let mut cpu_started = ProcessTime::try_now()?;
    for sequence in 0..WARMUP_RECORDS + case.records() {
        if sequence == WARMUP_RECORDS {
            cpu_started = ProcessTime::try_now()?;
            if case.stall_ms != 0 {
                std::thread::sleep(Duration::from_millis(case.stall_ms));
            }
        }
        expected[..8].copy_from_slice(&(sequence as u64).to_le_bytes());
        match reader.as_mut() {
            Some(reader) => {
                let record = loop {
                    match reader.try_read()? {
                        ReadOutcome::Record(record) => break record,
                        ReadOutcome::Empty => {
                            if reader.wait_readable()? != WaitOutcome::Ready {
                                return Err("Queue consumer exceeded the benchmark deadline".into());
                            }
                        }
                    }
                };
                assert_eq!(record.payload(), expected);
                std::hint::black_box(record.payload());
                reader.release(1)?;
            }
            None => {
                let mut header = [0; 8];
                stream.read_exact(&mut header)?;
                assert_eq!(u64::from_le_bytes(header), case.payload_bytes as u64);
                stream.read_exact(&mut socket_payload)?;
                assert_eq!(socket_payload, expected);
                std::hint::black_box(&socket_payload);
                stream.write_all(&[1])?;
            }
        }
    }
    let cpu_seconds = cpu_started.try_elapsed()?.as_secs_f64();
    let peak_rss_bytes = peak_rss_bytes()?;
    stream.write_all(
        serde_json::to_string(&serde_json::json!({
            "cpuSeconds": cpu_seconds, "peakRssBytes": peak_rss_bytes,
        }))?
        .as_bytes(),
    )?;
    Ok(())
}

fn spawn_child(role: ChildRole<'_>, case: Case) -> io::Result<BenchmarkChild> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args([
            "--ignored",
            "--exact",
            "ipc_transport_baseline",
            "--nocapture",
        ])
        .env(
            "TENON_IPC_BENCH_ROLE",
            match role {
                ChildRole::Producer => "producer",
                ChildRole::Consumer(_) => "consumer",
            },
        )
        .env("TENON_IPC_BENCH_CASE", serde_json::to_string(&case)?)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let scope = match role {
        ChildRole::Producer => {
            let directory = tempfile::Builder::new()
                .prefix("tenon-ipc-bench-")
                .tempdir_in("/tmp")?;
            command
                .process_group(0)
                .env("TENON_IPC_BENCH_DIRECTORY", directory.path());
            ChildScope::ProcessGroup {
                _directory: directory,
            }
        }
        ChildRole::Consumer(directory) => {
            command.env("TENON_IPC_BENCH_DIRECTORY", directory);
            ChildScope::Process
        }
    };
    Ok(BenchmarkChild {
        child: Some(command.spawn()?),
        scope,
    })
}

fn producer_result(mut child: BenchmarkChild) -> Result<serde_json::Value, Box<dyn Error>> {
    // The outer owner supervises the complete process group independently of
    // the Queue under test. Child stdout is one bounded JSON result plus libtest
    // framing; even a blocked output pipe must hit this same finite deadline.
    child.wait_for_exit(DEADLINE)?;
    let mut output = String::new();
    let mut stdout = child
        .process()
        .stdout
        .take()
        .ok_or("producer stdout is missing")?;
    let status = child.terminate_and_reap()?;
    stdout.read_to_string(&mut output)?;
    assert!(status.success(), "benchmark producer failed: {output}");
    let result = output
        .lines()
        .find_map(|line| line.split_once(RESULT_PREFIX).map(|(_, value)| value))
        .ok_or("producer result is missing")?;
    Ok(serde_json::from_str(result)?)
}

fn wait_for_ready(child: &mut Child, marker: &Path) -> io::Result<()> {
    let deadline = Instant::now() + DEADLINE;
    while !marker.is_file() {
        if child.try_wait()?.is_some() {
            return Err(io::Error::other("consumer exited before readiness"));
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other("consumer readiness timed out"));
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    Ok(())
}

enum ChildRole<'a> {
    Producer,
    Consumer(&'a Path),
}

enum ChildScope {
    // The outer owner removes these files even when SIGKILL bypasses child Drop.
    ProcessGroup { _directory: tempfile::TempDir },
    Process,
}

struct BenchmarkChild {
    child: Option<Child>,
    scope: ChildScope,
}

impl BenchmarkChild {
    #[allow(
        clippy::expect_used,
        reason = "only a successfully reaped child relinquishes this fixture's ownership"
    )]
    fn process(&mut self) -> &mut Child {
        self.child.as_mut().expect("benchmark child is still owned")
    }

    fn wait_for_exit(&mut self, timeout: Duration) -> io::Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            // Keep the leader waitable until the last group signal, so its PID
            // cannot be reused for another process group during cleanup.
            if waitid(
                WaitId::Pid(Pid::from_child(self.process())),
                WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
            )?
            .is_some()
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "benchmark process group exceeded its deadline",
                ));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn terminate_and_reap(&mut self) -> io::Result<ExitStatus> {
        if matches!(self.scope, ChildScope::ProcessGroup { .. }) {
            match kill_process_group(Pid::from_child(self.process()), Signal::KILL) {
                // macOS returns EPERM for a group containing only its zombie
                // leader. These benchmark children never change OS identity.
                Ok(()) | Err(rustix::io::Errno::SRCH | rustix::io::Errno::PERM) => {}
                Err(error) => return Err(error.into()),
            }
        } else {
            let _ = self.process().kill();
        }
        let status = self.process().wait()?;
        self.child.take();
        Ok(status)
    }
}

impl Drop for BenchmarkChild {
    fn drop(&mut self) {
        if self.child.is_some() {
            let _ = self.terminate_and_reap();
        }
    }
}

#[test]
fn supervisor_timeout_kills_the_entire_benchmark_group() -> io::Result<()> {
    use std::os::unix::process::ExitStatusExt as _;
    let directory = tempfile::tempdir()?;
    let path = directory.path().to_owned();
    let leader = Command::new("sleep").arg("600").process_group(0).spawn()?;
    let mut owner = BenchmarkChild {
        child: Some(leader),
        scope: ChildScope::ProcessGroup {
            _directory: directory,
        },
    };
    let member = Command::new("sleep")
        .arg("600")
        .process_group(Pid::from_child(owner.process()).as_raw_pid())
        .spawn()?;
    let mut member = BenchmarkChild {
        child: Some(member),
        scope: ChildScope::Process,
    };
    assert!(
        matches!(owner.wait_for_exit(Duration::ZERO), Err(error) if error.kind() == io::ErrorKind::TimedOut)
    );
    drop(owner);
    member.wait_for_exit(DEADLINE)?;
    assert_eq!(member.terminate_and_reap()?.signal(), Some(9));
    assert!(!path.exists(), "outer owner must remove benchmark files");
    Ok(())
}

#[test]
fn failed_group_leader_stays_waitable_until_cleanup() -> io::Result<()> {
    let leader = Command::new("sh")
        .args(["-c", "exit 7"])
        .process_group(0)
        .spawn()?;
    let mut owner = BenchmarkChild {
        child: Some(leader),
        scope: ChildScope::ProcessGroup {
            _directory: tempfile::tempdir()?,
        },
    };
    owner.wait_for_exit(DEADLINE)?;
    // A second NOWAIT observation still succeeds: no PID reuse window opened.
    owner.wait_for_exit(Duration::ZERO)?;
    assert_eq!(owner.terminate_and_reap()?.code(), Some(7));
    assert!(owner.child.is_none());
    Ok(())
}

#[allow(
    unsafe_code,
    reason = "read-only benchmark resource measurement at the libc boundary"
)]
fn peak_rss_bytes() -> io::Result<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: libc receives a writable, aligned rusage slot. It initializes the
    // complete struct on success; no field is read on the error path.
    let usage = unsafe {
        if libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) != 0 {
            return Err(io::Error::last_os_error());
        }
        usage.assume_init()
    };
    let maximum = u64::try_from(usage.ru_maxrss).map_err(io::Error::other)?;
    #[cfg(target_os = "linux")]
    let maximum = maximum * 1024;
    Ok(maximum)
}
