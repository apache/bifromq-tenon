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

# IPC Queue Fuzz Testing

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../DISCLAIMER).

Use `queue_format` to fuzz Queue headers and frames, and `queue_runtime` to fuzz record ordering, Full/Empty results, release and reopening.

Install the pinned tools, then run verification:

```sh
rustup toolchain install nightly-2026-10-02
cargo install cargo-fuzz --version 0.13.2 --locked
python3 tools/verify-ipc-fuzz.py
```

If an existing `nightly` alias points to exactly the same compiler, you can set `TENON_FUZZ_TOOLCHAIN=nightly`. The verification script checks the full compiler identity and rejects other builds. `fuzz/Cargo.lock` pins the verification programs' dependencies; the product continues to use the root `Cargo.lock`.

The script generates initial inputs from the single shared set of queue test vectors, then runs coverage-guided fuzzing for 60 and 90 seconds, respectively, with the default AddressSanitizer, debug assertions, and overflow checks enabled. `fuzz/corpus/` retains automatically discovered inputs, `fuzz/artifacts/` retains failure inputs, and `target/ipc-fuzz/` contains the full logs. These are local verification artifacts, not release materials. Each rerun overwrites the logs; archive both the logs and the corpus used when preserving results. A fixed random seed does not guarantee the same exploration sequence in a time-limited run; exact replay requires the specific input. Replay a failure using the same target name and failure file, for example:

```sh
cargo +nightly-2026-10-02 fuzz run queue_runtime fuzz/artifacts/queue_runtime/crash-<hash> --dev
```

Format inputs are limited to 4096 bytes. Runtime inputs use at most 128 actions and capacities from 16 to 2056 bytes. Run the [repository checks](../CONTRIBUTING.md#verification) for larger records and cross-process behavior.

Tool invocation follows the [Rust Fuzz Book](https://rust-fuzz.github.io/book/cargo-fuzz/guide.html) and [cargo-fuzz 0.13.2](https://github.com/rust-fuzz/cargo-fuzz/releases/tag/0.13.2).
