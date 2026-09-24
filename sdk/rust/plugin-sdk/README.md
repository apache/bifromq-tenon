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

# tenon-plugin-sdk

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../../../DISCLAIMER).

The Rust SDK for writing Tenon plugins. A **Source** brings messages into a flow, a **Sink** processes messages emitted by a flow, and a **Source-and-sink** plugin provides both interfaces through one shared business object.

The SDK owns the Tenon control connection, bounded queues, send results, and shutdown sequence. Your code owns its external clients, worker threads, async executors, and upstream acknowledgement policy.

## Start a project

You need Rust 1.97.1, Cargo, and a native compiler/linker. Generated projects use edition 2024 and supply `protoc` through a build dependency.

Install the generator from crates.io, then generate a project from the Git template:

```bash
cargo install cargo-generate --version 0.24.0 --locked
cargo generate --git https://github.com/apache/incubator-bifromq-tenon.git \
  sdk/rust/rust-plugin-scaffold \
  --name example-source --define interface=source \
  --silent --vcs none --no-workspace
cd example-source
cargo test
```

Choose `sink` or `source-and-sink` instead of `source` for the other interfaces. Add `--revision <commit>` or `--tag <tag>` to pin the template revision. The generated dependency is resolved from crates.io:

```toml
[dependencies]
tenon-plugin-sdk = "=0.1.0"
```

Use the same dependency declaration in an existing project. Commit the generated `Cargo.lock` and use `--locked` after the first dependency resolution.

Use one SDK dependency for all three interfaces. `Error` is `Box<dyn std::error::Error + Send + Sync>`, `Value` is the SDK's re-exported JSON value with arbitrary-precision numbers, and `prost` is the re-exported Protobuf runtime. Generate payload types against `::tenon_plugin_sdk::prost`, as the scaffold does.

Tenon launches the program and supplies configuration, queues, and the private control connection. These entrypoints are not standalone `cargo run` applications. Standard output and standard error are diagnostic streams; do not use stdin for business input.

## Source lifecycle

Implement `TenonSource` with three synchronous methods that return no value:

| Method | Responsibility |
| --- | --- |
| `start(&mut self)` | Start this producer's business work. |
| `quiesce(&mut self)` | Stop producing new messages without waiting for outstanding send results. |
| `close(&mut self)` | Initiate business shutdown after the SDK has resolved send results; do not wait for workers or remote acknowledgements. |

In a generated Source project, `src/main.rs` wires the supplied producer into the SDK:

```rust
mod payload;
mod source;

use tenon_plugin_sdk::SourceProgram;

fn main() {
    let program = SourceProgram::run(|config, _channel_count, sender| {
        Ok(source::Source::new(&config, sender))
    });
    program.await_shutdown();
}
```

The factory receives the complete configuration, the effective Source channel count as a `usize`, and a typed `PayloadSender<P>`. It runs once during `run`. The returned Source can manage multiple upstream clients, subscriptions or workers.

The SDK starts the single Source during `run`; `await_shutdown(self)` consumes the program, reports readiness to Tenon, and blocks through shutdown. A lifecycle panic or SDK error reports to stderr, flushes, and exits with code 1; no error result is returned to the author. Quiesce and close are owned by the SDK.

### Sending and observing results

Call `sender.send(channel, &payload)` with a zero-based channel index. An invalid index returns `InvalidChannel` immediately. A valid index returns a `Completion`, whose result is `Result<AckCode, SendError>`. Await it on your own async executor, or call `wait()` on an ordinary business thread; do not call `wait()` on an async runtime thread.

| Result | Meaning and author responsibility |
| --- | --- |
| `AckCode::Ok` | The flow's configured delivery boundary was reached. Map this to the appropriate upstream acknowledgement. |
| `AckCode::Retry` | The delivery boundary was not reached. Your business policy decides whether and how to retry. |
| `AckCode::Error` | This record could not be processed successfully, including an oversized payload or record. Handle it as a record failure. |
| `AckCode::Backpressure` | The channel has no available admission slot. No payload was encoded or submitted for this attempt. Slow or pause upstream production. |
| `SendError` | The session failed or shut down before this request obtained a final result. External side effects may already have occurred. |

For `at-most-once` delivery, success means the Pipeline has copied the input out of the Source queue; Lua processing and Sink writes may still be pending. For `at-least-once`, success means one Lua `emit` call has had its records successfully processed and released by all of that call's selected Sinks, or Lua explicitly acknowledged the input with an argument-free `emit()`. This does not aggregate every `emit` in a Lua invocation. The Sink implementation defines what successful processing means for its external system.

An admitted request keeps its slot until it fails before submission or the SDK processes its final result. Dropping `Completion` does not cancel the send or return the slot. The SDK never resends automatically. Upstream acknowledgements, retry delays, and duplicate handling belong to your plugin.

## Sink lifecycle and concurrency

Implement `TenonSink<P>` for the Sink payload type. The business object must be `Send + Sync + 'static`. It provides synchronous `start(&mut self)` and `close(&mut self)` methods that return no value, plus:

```rust
fn write(
    &self,
    channel: FlowChannel,
    records: Box<[P]>,
) -> impl Future<Output = Result<(), Error>>;
```

Here `Future` is `std::future::Future`; `FlowChannel` and `Error` come from the SDK. `FlowChannel` contains the originating `flow_id` and zero-based `channel_id`. The batch is nonempty and ordered. Your code owns the decoded records and may retain them.

The generated Sink project entrypoint uses its supplied `file_program` and `output` modules:

```rust
mod file_program;
mod output;
mod payload;

use tenon_plugin_sdk::SinkProgram;

fn main() {
    let program = SinkProgram::run(file_program::FileProgram::new);
    program.await_shutdown();
}
```

The factory receives the complete configuration and returns one business object. The SDK calls `start` during `run`; `await_shutdown` reports readiness and waits for shutdown.

`write` calls run one at a time and must return promptly. Returned futures may overlap and complete out of order. They may borrow the business object and need not be `Send`. The SDK does not supply an async runtime; run clients that require one on your own executor and return a future that observes their result. Neither `write` nor its future's `poll` method may block.

Only the continuous successful prefix of a queue is released. A later successful batch cannot acknowledge an earlier unfinished or failed batch. A write failure fails the instance without releasing that batch or its suffix. When the surviving Pipeline restarts a Sink against retained queues, unreleased records can be replayed, including records whose external side effects already happened. Design for duplicates where required; this is not durable replay across Pipeline or machine loss.

Final shutdown drops unfinished result futures and calls business `close` once after successful SDK cleanup. Dropping a future may leave work running on your executor; signal that work to stop in `close`. Cleanup is not guaranteed after a failure or panic.

## One shared Source-and-sink object

Implement `TenonSourceAndSink<P>` when both interfaces share a connection or another resource. `P` is the **Sink** payload type; the factory's `PayloadSender` has the separate Source payload type. The shared object is `Send + Sync + 'static` and provides:

- `start(&mut self)` for all shared Source and Sink resources; the SDK calls it during initialization.
- `quiesce(&self)` for Source production; the SDK calls it during quiesce.
- `close(&mut self)` for shared resources after Source and Sink shutdown.
- `write(&self, FlowChannel, Box<[P]>)` with the same future and concurrency rules as a Sink.

It does not need a separate `TenonSink` implementation. In the generated combined project:

```rust
mod file_program;
mod output;
mod payload;
mod source;

use tenon_plugin_sdk::SourceAndSinkProgram;

fn main() {
    let program = SourceAndSinkProgram::run(|config, ingress, channels| {
        file_program::FileProgram::new(config, ingress, channels)
    });
    program.await_shutdown();
}
```

The shared factory and `start` each run once. Runner activates each direction from the current Flow bindings. The factory receives `Option<Ingress<S>>` and the actual `BTreeSet<FlowChannel>` Sink inputs, so the same Program can run Source-only, Sink-only, or in both directions. An unbound direction has no queues or SDK workers. `FlowChannel` contains only the Flow id and channel id.

Quiescing stops only Source production; Sink writes and Source results can still use the shared connection. Final shutdown resolves Source results, stops Sink workers, then closes the shared object once. For example, a device can stop publishing events while still receiving the final commands through the same connection.

## Shutdown and process failure

During Source quiesce, new sends are rejected and `quiesce` must stop production without waiting for pending results. Source results and any Sink interface remain active.

Final shutdown preserves results already read, fails remaining requests, and calls `close` after successful SDK cleanup. Business `close` must return promptly without waiting for worker termination or remote acknowledgements. Tenon may forcibly terminate a plugin that exceeds its shutdown deadline.

Factory, lifecycle, queue, write and shutdown failures are reported on stderr and exit the process with code 1. Panics on business threads do the same. Neither `run` nor `await_shutdown` returns an error to the author. Dropping a Program before shutdown completes also exits with failure. Do not depend on later callbacks or destructors after a failure; use these entrypoints in a process dedicated to the plugin.

## Payload contracts, packaging, and testing

Your `config.schema.json` describes the complete instance configuration; Tenon validates it before passing the value to the factory. Check business requirements that a schema cannot express, such as opening a device or authenticating to a service, in your factory or a business worker; lifecycle callbacks must not wait for external progress.

Payload definitions use proto3. Source requires one top-level `SourceRecordPayload`, Sink requires one top-level `SinkRecordPayload`, and a combined plugin has both. The scaffold's `build.rs` creates Rust types and a self-contained `payload.descriptor.pb` with source information. The descriptor, configuration schema, and executable must describe the same program. Runtime `.proto` compilation is not used.

Set the required `display-name` and `description` in Cargo.toml under `[package.metadata.tenon]`; see the [display metadata requirements](../../../guide/plugins.md#display-metadata).

To build an installation bundle from the generated project:

```bash
cargo install cargo-tenon --version 0.1.0 --locked
cargo tenon bundle --release
```

The JSON result's `bundle` field gives the archive path. It contains `program`, `manifest.json`, `config.schema.json`, and `payload.descriptor.pb`; it does not collect extra resources or dynamic libraries. Without `--target`, the build uses the host reported by `rustc -vV`. For another platform, pass an explicit target and provide its compiler/linker and runtime prerequisites.

Test business logic directly with your own clients or fakes, then install the bundle in Tenon and exercise real messages, backpressure, shutdown, and external failures. A bundle build checks the contracts; it does not prove the external business operation or all target platforms work.
