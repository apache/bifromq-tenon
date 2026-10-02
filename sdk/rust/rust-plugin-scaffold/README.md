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

# Rust plugin scaffold

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../../../DISCLAIMER).

Generate a Rust plugin project for Tenon with `cargo-generate`. Choose `source` to produce messages, `sink` to consume messages, or `source-and-sink` to do both through one shared business object.

The template includes working example code, payload definitions, a configuration schema, and a build script. The SDK manages Tenon's process lifecycle and queues; your code owns its clients, tasks, and external resources. `cargo-tenon` compiles the project and creates its installation bundle.

## Prerequisites

- Rust 1.99.0 with Cargo and a native compiler/linker. Generated projects use edition 2024.
- `cargo-generate` 0.25.0, the version required by this template.
- `cargo-tenon` 0.1.0 for contract checking and installation bundles.
- Registry access for dependencies, or a populated Cargo cache. A system `protoc` installation is not needed; the generated build dependencies supply it.

Install the generator and build tool from crates.io:

```bash
cargo install cargo-generate --version 0.25.0 --locked
cargo install cargo-tenon --version 0.1.0 --locked
```

Ensure Cargo's installation bin directory (normally `$HOME/.cargo/bin`) is on `PATH`.

## Generate a project

Run this from the directory that should contain your new project:

```bash
cargo generate --git https://github.com/apache/incubator-bifromq-tenon.git \
  sdk/rust/rust-plugin-scaffold \
  --name example-source --define interface=source \
  --silent --vcs none --no-workspace
cd example-source
```

This creates `example-source/`. Change `interface` to `sink` or `source-and-sink` and choose a different project name for the other variants. Omit `--define interface=...` and `--silent` to choose interactively. `--vcs none` skips Git initialization; `--no-workspace` prevents adding the project to a surrounding workspace. You can pass `--destination /absolute/existing/directory` to choose a different parent directory.

The template lives in Git, not in a crate. The repository URL and `sdk/rust/rust-plugin-scaffold` subdirectory are separate arguments; do not use a GitHub `/tree/` URL. Add `--revision <commit>` or `--tag <tag>` to pin a template revision for repeatable generation.

## Test and build

The generated `Cargo.toml` declares `tenon-plugin-sdk = "=0.1.0"`, which Cargo resolves from crates.io:

```bash
cargo test
cargo tenon build
cargo tenon bundle --release
```

Keep the generated `Cargo.lock` in version control and add `--locked` after the first successful dependency resolution.

`bundle` prints a JSON result with a `bundle` field containing the archive path. By default it builds for the host reported by `rustc -vV`; pass `--target <triple>` explicitly for another target. You must supply any cross compiler, linker, and target libraries yourself.

Do not use `cargo run` to test the plugin lifecycle in isolation. Tenon starts the executable and supplies its configuration, control connection, and queues. Install the bundle in Tenon and configure a flow to test actual message delivery.

## What the examples do

| Interface | Behavior | Required configuration |
| --- | --- | --- |
| `source` | Sends `message` once at startup on channel 0. | `message`: a nonempty string. |
| `sink` | Appends each received message as a line in a file. | `outputFile`: an absolute path to a writable file. |
| `source-and-sink` | Sends once and appends received messages to a file. | Both fields above. |

For example, the combined plugin accepts:

```json
{
  "message": "example message",
  "outputFile": "/tmp/plugin-messages.txt"
}
```

For a Source-only or Sink-only project, provide only its listed field. The generated schema rejects extra properties. The parent directory of `outputFile` must already exist and be writable by the plugin process.

The Source example sends once. For a continuous source, observe send results during normal operation and acknowledge the upstream system according to your delivery policy. Stopping production must not wait for outstanding results.

The Sink example synchronizes each batch to the output file before reporting success. A failed write may leave partial output, so replay can produce duplicates. Newlines inside messages are preserved; the example does not deduplicate them.

A combined plugin is not automatically connected to itself. A Tenon flow must connect its Source output to a compatible Sink input, or connect the two sides to other plugins.

## Files to customize

| File or directory | What to change |
| --- | --- |
| `Cargo.toml` | Package name/version and `package.metadata.tenon.program-name`. The `interface` matches the generated implementation. |
| `src/main.rs` | SDK entrypoint and custom initialization before `await_shutdown`. |
| `src/source/` | Source producer behavior, when present. |
| `src/file_program/`, `src/output/` | Sink behavior and shared resources, when present. |
| `proto/` | The business payloads. Source uses top-level `SourceRecordPayload`; Sink uses top-level `SinkRecordPayload`; a combined plugin declares both. |
| `config.schema.json` | The instance configuration schema, including required fields and constraints. |
| `build.rs`, `src/payload/` | Generation and inclusion of payload Rust types and `payload.descriptor.pb`. |

`build.rs` generates both Rust types and a self-contained proto3 descriptor with source information from the same `.proto` inputs. Keep `prost_path("::tenon_plugin_sdk::prost")` so the generated code uses the SDK's Protobuf runtime. The only ordinary dependency is `tenon-plugin-sdk`; Protobuf tools are build dependencies, and Sink file tests use `tempfile` only as a test dependency.

Choose your published program identity by editing `package.metadata.tenon.program-name`, initially `com.example.<project-name>`. Release versions come only from `package.version`. Keep payload definitions and configuration schemas consistent with the behavior you ship.

Set `display-name` and `description` under `[package.metadata.tenon]` in the generated Cargo.toml. The bundler validates and preserves them for Console; see the [display metadata requirements](../../../guide/plugins.md#display-metadata).

## Installation bundle

`cargo tenon bundle` creates an archive containing:

```text
config.schema.json
manifest.json
payload.descriptor.pb
program
```

Only these four regular files are included. The tool does not gather extra resources or dynamic libraries. Explain the bundle's ABI and native-library requirements to the deployer and test it with Tenon on the intended platform.

To check an already unpacked bundle, run:

```bash
cargo tenon check /absolute/path/to/unpacked-bundle
```

This command validates the three contract files without executing the program. It does not take a Cargo project directory or a compressed archive. `cargo package` creates a registry source archive, not a Tenon installation bundle.
