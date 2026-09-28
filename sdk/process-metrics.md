<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Tenon process metrics

This module owns process CPU and resident-memory sampling semantics across languages. Implementations live in `sdk/rust/process-metrics` and `sdk/java/process-metrics` and are packaged as independent, reusable language libraries. Runtime callers and Plugin SDKs use these implementations without depending on each other. An additional language must implement the same semantics in its own language directory.

The module has no OpenTelemetry dependency, instrument names, process identity labels, transport, logging, background work, or global state. The caller owns the sampler and decides when to request a value, report a failure, and publish an observation through its language's OpenTelemetry SDK.

## Common sampling contract

CPU is the increase in the calling process's cumulative user plus system CPU time divided by monotonic elapsed time between two successful requested samples. The result is consumed logical CPU cores: `1.0` means one fully occupied core, and values above `1.0` are valid. Do not normalize by the machine's core count or substitute a host-wide CPU percentage. Child processes are excluded.

Each sampler owns one previous successful CPU sample. The first request establishes that baseline and returns no CPU value. A sampling failure or nonpositive elapsed interval returns no observation and preserves the baseline. Not calling CPU sampling does not advance it. Reuse the same sampler across collection reconnects; a new process or new sampler starts with no baseline. Calls to a sampler must be serialized by its owner; the module creates no worker or synchronization task.

Memory is current own-process RSS in bytes. It includes resident native and managed allocations, code, stacks, and mapped/shared resident pages. It is neither managed-heap usage, virtual address space, peak RSS, unique physical memory, nor a process-tree total. Shared pages may occur in more than one process's RSS, so adding these values does not measure unique host memory. Memory sampling is independent of the CPU baseline.

| Platform | RSS implementation and limits |
| --- | --- |
| Linux | Read the resident page count in `/proc/self/statm` and multiply by the OS page size using checked arithmetic. The kernel counters are approximate. Do not scan `smaps`, mappings, or process memory. |
| macOS | Call `task_info` for the current task with `MACH_TASK_BASIC_INFO` and use `resident_size`. Validate the call result and structure count. |

These are on-demand reads of existing process counters. They do not walk children, scan application allocations, or trigger garbage collection. System calls and scheduling still have a cost; no fixed latency or atomic snapshot of CPU and memory is promised. Unsupported platforms, failed calls, invalid counters, and byte-conversion failures remain errors, never a zero observation. Callers must isolate these errors from business processing.

## Rust implementation

`tenon_process_metrics::ProcessSampler::cpu(&mut self)` returns `io::Result<Option<f64>>`. `None` means no CPU observation; `Err` means sampling failed. The `cpu-time` crate reads own-process CPU time; `std::time::Instant` measures elapsed time. `tenon_process_metrics::memory()` returns `io::Result<u64>` without changing any CPU baseline.

The `tenon-process-metrics` crate is shared by Tenon core and the Rust Plugin SDK and can be consumed independently through Cargo. It uses OS primitives and has no dependency on the Runner, Plugin SDK, IPC, or OpenTelemetry crates.

## Java implementation

`org.apache.bifromq.tenon.metrics.ProcessSampler.cpu()` returns `OptionalDouble` and throws `IOException` on sampling failure. An empty result means no CPU observation. `OperatingSystemMXBean.getProcessCpuTime()` supplies own-process CPU nanoseconds, and `System.nanoTime()` measures elapsed nanoseconds. An unavailable or regressing CPU counter is an error and preserves the previous baseline.

`ProcessSampler.memory()` returns a `long` byte count or throws `IOException`, independently of any sampler. It uses JDK Foreign Function and Memory calls for native page size and macOS task information, and the JDK file API for Linux `statm`. Launch with `--enable-native-access=ALL-UNNAMED`. The pinned repository JDK supplies these APIs; no JNI library or native worker is required.

The Java artifact `org.apache.bifromq.tenon:tenon-process-metrics` has no runtime dependencies beyond the JDK. It is installed and deployed independently, with a consumer POM that does not depend on the repository build parent. The Plugin SDK declares it as a normal dependency and combines it with its private OpenTelemetry implementation. Plugin authors receive the sampler transitively; other applications can depend on it directly.

## Verification

Use `cargo test --locked -p tenon-process-metrics` for Rust and `sdk/java/mvnw --file sdk/java/pom.xml -pl :tenon-process-metrics -am test` for Java. Both run against real process clocks and RSS. The repository's complete Rust/Java verification also checks SDK first-sample/filter/reconnect behavior, real Plugin processes, archive contents, isolated Maven consumers, and shutdown. Validate native OS reads on every declared target; a cross-built archive alone does not prove native sampling.
