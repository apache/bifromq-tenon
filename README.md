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

# Apache BifroMQ Tenon

Tenon is a satellite project of Apache BifroMQ (Incubating), connecting BifroMQ with external systems through MQTT client connections.

Tenon's plugin architecture is general-purpose. Source and Sink plugins can connect any systems for which suitable plugins are available, independently of BifroMQ.

See the [incubation disclaimer](DISCLAIMER).

Tenon combines typed plugins with Lua data processing. A standalone Runner manages installed plugin programs and declarative Tenon Documents. Each Document describes a Pipeline; its Flows read from Source instances, transform records in Lua, and write to Sink instances.

```text
Source Plugin → Flow Channel / Lua → Sink Plugin
                    ↑
          Runner manages configuration,
          processes, packages and diagnostics
```

Write plugins in Rust or Java using the SDKs and project scaffolds. Start with the [plugin development guide](guide/plugins.md). Developers adding a language SDK and scaffold should use the [SDK implementation contract](sdk/plugin-sdk-contract.md).

Plugins run in separate processes. A plugin can implement Source, Sink, or both interfaces; both interfaces share one process and configuration when they belong to the same instance.

This repository contains the Runner, IPC implementations, language SDKs (including reusable process-metrics libraries), plugin generators, and an MQTT plugin.

## Build and try

The Rust toolchain is pinned in [rust-toolchain.toml](rust-toolchain.toml). Install Rust through rustup, a native C compiler and linker, and Python 3 with `jsonschema==4.23.0` for contract checks. The Rust workspace builds its vendored Lua runtime and Protocol Buffers tools.

```sh
cargo build --locked --bin tenon
python3 tools/validate-contracts.py
python3 tools/validate-public-docs.py
```

Follow the [local quickstart](guide/quickstart.md) to generate a plugin from source, install it, run a Lua Flow, and verify a real output file. It does not require an external message broker or previously published Tenon artifacts.

Supported build targets are Linux and macOS on x86-64 and AArch64. macOS requires 14.4 or later. Platform names in plugin manifests are `linux`/`darwin` and `amd64`/`arm64`. A package must declare the target it actually supports; an accepted declaration does not verify external dependencies or connectivity.

Java development uses the Maven wrapper under `sdk/java/` and the exact JDK recorded in [tenon-toolchain.properties](sdk/java/.mvn/tenon-toolchain.properties). See the [Java SDK](sdk/java/README.md), [Java plugin generator](sdk/java/README.md#generate-a-plugin), [Rust SDK](sdk/rust/plugin-sdk/README.md), and [Rust plugin scaffold](sdk/rust/rust-plugin-scaffold/README.md).

## Core concepts

A **Program** is an immutable installed package identified by its name and exact version. An **Instance** is one configured use of that Program. A **Flow** binds one Source instance to Lua and one or more Sink instances. A **Channel** processes records in order; parallel channels have independent Lua state.

The Runner saves Documents and installed packages across restarts. Saving a Document and applying it are separate outcomes. Use Pipeline status and its desired/applied ETags to see progress; local `running` status does not prove delivery to an external system.

Queues, Lua state and timers are volatile. Delivery completion follows the selected Flow policy and plugin behavior. Tenon does not provide a durable message log, distributed transactions, or end-to-end exactly-once delivery. See [Documents and delivery](guide/tenon-document.md) before choosing replay and acknowledgement behavior.

Management write access grants control over executable workloads. The default Runner has no HTTP request authorization and stores Documents as plaintext; mTLS authenticates clients but does not assign operation permissions. Establish deployment access controls and review the [security policy and threat model](SECURITY.md) before exposing the API or accepting plugins and Documents.

## Documentation

- [Security policy, threat model and vulnerability reporting](SECURITY.md)
- [Runner configuration, deployment and recovery](guide/runner.md)
- [Documents, updates and delivery](guide/tenon-document.md)
- [Lua processing API](guide/lua.md)
- [HTTP management API](guide/http-api.md)
- [Plugin packaging and development](guide/plugins.md)
- [Metrics and live diagnostics](guide/observability.md)
- [Runner extension interfaces](guide/runner-extensions.md)
- [Developing a language SDK and scaffold](sdk/plugin-sdk-contract.md)
- [IPC protocol for SDK developers](ipc/README.md)
- [Process sampling contract and language implementations](sdk/process-metrics.md)
- [Contributing and verification](CONTRIBUTING.md)

Machine-readable schemas and shared test vectors live in [contracts/](contracts/). The running binary serves its own API description at `/openapi.json` and its Document Schema at `/document-schema`.

## License

The source is licensed under [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) and the licenses shipped with dependencies and generated runtime bundles.
