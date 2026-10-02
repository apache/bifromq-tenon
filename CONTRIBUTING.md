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

# Contributing

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](DISCLAIMER).

Build and validate changes using the toolchains fixed by the repository. Keep changes focused on one behavior or engineering boundary, and explain the observable result, verification performed, and any untested platform in the pull request.

## Working with the code

| Directory | Responsibility |
| --- | --- |
| `src/` | Runner, Pipeline, Lua execution, configuration and management |
| `contracts/` | Schemas, protocol sources, descriptors and shared conformance vectors |
| `sdk/rust/ipc/`, `sdk/java/ipc/` | Rust and Java shared-memory queue and native wait implementations |
| `sdk/` | Public SDK contract, language SDKs and plugin generators |
| `plugin/` | Plugins developed with the standard SDK and packaging tools |
| `tests/` | Integration tests and public test fixtures |
| `tools/` | Reproducible engineering validation |

Public behavior belongs in the relevant English guide or module contract. Keep schemas, examples, vectors and tests consistent with that behavior. Avoid duplicating protocol definitions across language implementations. Source comments, diagnostics and contributor documentation use English.

Use `cargo fmt` and Clippy for Rust and the configured Maven format checks for Java. Keep ownership, shutdown and failure handling explicit. Test-only modules must have an explicit test build condition; repository integration helpers use the `repository-test-support` feature and must not leak into normal library builds.

## Verification

Install Python 3 and `jsonschema==4.23.0`, then run from the repository root:

```sh
python3 tools/validate-license-headers.py
python3 tools/validate-contracts.py
python3 tools/validate-public-docs.py
python3 -B -m unittest discover -s tools/tests
```

Run the relevant complete language verification before proposing changes to that language or shared contracts:

```sh
tools/verify-rust.sh
tools/verify-java.sh
```

The Rust verifier requires `cargo-generate` at the version in [cargo-generate.toml](sdk/rust/rust-plugin-scaffold/cargo-generate.toml), a native C toolchain, and the Java toolchain for interoperability checks. The Java verifier uses the Maven wrapper and an isolated repository under `target/verify-java-maven-repository`. Its exact Java and Maven requirements are enforced by the POMs and [toolchain properties](sdk/java/.mvn/tenon-toolchain.properties).

For a fast local iteration, run the specific Cargo package/test or Maven module affected by the change. A filtered test command must actually execute the intended test. Complete validation also exercises generated plugin projects, native process lifecycle, cross-language behavior and failure paths. Do not replace real-process tests with mocks for lifecycle or interoperability changes.

CI covers Linux/macOS on amd64/arm64, compares platform-independent Java bundle contracts, and checks Linux resource limits and Lua C sources under sanitizers. Linux resource checks cover systemd services and containers with and without init or cgroup permissions. To reproduce the container checks, run `python3 tools/verify-container-resource-limits.py /path/to/default-linux-tenon /path/to/runner_resource_limits-test-binary` on a machine with Docker. Build that test executable with `cargo test --locked --features repository-test-support --test runner_resource_limits --no-run`; use a separate default build for the Runner. The driver creates and removes its own containers and local image. A successful run on one workstation is evidence for that platform only. Preserve these jobs when changing build scripts.

Tests cover ordinary errors as well as malformed inputs, closure, cancellation and recovery. Add tests for a newly changed boundary, rather than copying implementation logic into a second implementation. Public fixtures must have clear provenance and reproducible generation instructions; test credentials are never deployment credentials.

## Build cache lifecycle

The Rust and Java verification scripts inspect checkout-local build outputs before and after validation. The maintenance line is 30 GiB across Runner, Rust SDK and Java output directories. Above that line, idle development profiles and generated documentation are removed as complete directories. Release outputs, the latest verified Java bundles and contract evidence remain available. This is maintenance between tasks, not a hard quota during compilation.

Validation and maintenance share a repository lock across Git worktrees. Maintenance uses `lsof` to refuse deletion of open build files. Install `lsof` alongside Python and the native build toolchains. External build directories, linked build directories and tracked files are not deleted automatically. For custom Cargo target/build directories, use checkout-local paths; the tool reads Cargo metadata and the corresponding environment variables.

Verified Java Plugin bundles retain one successfully validated input version. A new version replaces the previous cache only after its behavior checks pass. Failed builds preserve the last valid version. Temporary staging is removed on normal failure; abandoned staging is reclaimed on the next successful cache reuse or publication.

Run from the checkout root:

```sh
python3 tools/build-cache.py maintain
python3 tools/build-cache.py run -- cargo test --locked --lib
python3 tools/build-cache.py finish
```

`run` owns the command and its process group until it exits. `finish` is for a completed temporary checkout: it removes recognized, unused build outputs while preserving source files, Git history, changes and contract evidence. It does not remove the checkout itself. It refuses maintenance while another managed task owns the repository or selected build files are open. Unexpected interruption cannot guarantee immediate cleanup; the next idle maintenance boundary reclaims abandoned experiments.

## Plugin contributions

Use the standard [Rust scaffold](sdk/rust/rust-plugin-scaffold/README.md) or [Java archetype](sdk/java/README.md#generate-a-plugin), and package plugins through `cargo tenon bundle` or the Java bundle goal. Validate them through normal Runner installation and lifecycle APIs. For a new language SDK or scaffold, follow the [SDK implementation contract](sdk/plugin-sdk-contract.md) and [IPC protocol](sdk/ipc_contract.md).

## Licensing

Preserve existing license and attribution notices. Identify third-party source or assets introduced by a change, and keep their license information with the distributed material. Generated binary bundles must retain their runtime and dependency notices.
