#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Run bounded, reproducible Queue fuzz campaigns with the fixed sanitizer toolchain."""

import json
import os
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parent.parent
TOOLCHAIN = os.environ.get("TENON_FUZZ_TOOLCHAIN", "nightly-2026-10-02")


def version(command: list[str]) -> str:
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def seed_corpus() -> None:
    vectors = json.loads((ROOT / "contracts/ipc/test-fixtures/queue_v1_test_vectors.json").read_text(encoding="utf-8"))
    corpus = ROOT / "fuzz/corpus/queue_format"
    corpus.mkdir(parents=True, exist_ok=True)
    for kind in ("headers", "frames"):
        for name, cases in vectors[kind].items():
            if isinstance(cases, list):
                for index, case in enumerate(cases):
                    if isinstance(case, dict) and "encodedHex" in case:
                        (corpus / f"shared-{kind}-{name}-{index}").write_bytes(bytes.fromhex(case["encodedHex"]))
    runtime = ROOT / "fuzz/corpus/queue_runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    # Capacity 64; append/read/release plus owner reopen operations. All actions
    # use the same input grammar as queue_runtime, not a second Queue algorithm.
    (runtime / "fifo-replay-wrap").write_bytes(
        bytes([6]) + b"".join(bytes([operation]) + bytes([value]) * 64
                            for operation, value in [(0, 1), (1, 0), (3, 0), (1, 0),
                                                     (2, 0), (4, 0), (0, 2), (1, 0), (2, 0)])
    )


def main() -> None:
    compiler = version(["rustc", f"+{TOOLCHAIN}", "--version"])
    if compiler != "rustc 1.101.0-nightly (c36f14571 2026-10-01)":
        raise RuntimeError(f"Unexpected fuzz compiler: {compiler}")
    fuzzer = version(["cargo", f"+{TOOLCHAIN}", "fuzz", "--version"])
    if fuzzer != "cargo-fuzz 0.13.2":
        raise RuntimeError(f"Expected cargo-fuzz 0.13.2, got {fuzzer}")
    subprocess.run(["cargo", f"+{TOOLCHAIN}", "fetch", "--manifest-path", "fuzz/Cargo.toml", "--locked"], cwd=ROOT, check=True)
    seed_corpus()
    output = ROOT / "target/ipc-fuzz"
    output.mkdir(parents=True, exist_ok=True)
    environment = dict(os.environ, CARGO_NET_OFFLINE="true")
    for target, seconds, maximum_length in [("queue_format", 60, 4096), ("queue_runtime", 90, 8321)]:
        command = ["cargo", f"+{TOOLCHAIN}", "fuzz", "run", target, "--dev", "--",
                   f"-max_total_time={seconds}", f"-max_len={maximum_length}",
                   "-seed=20260907", "-print_final_stats=1"]
        log = output / f"{target}.log"
        print(f"Running {target}; full evidence: {log}", flush=True)
        with log.open("w", encoding="utf-8") as stream:
            stream.write(f"{compiler}\n{fuzzer}\n{' '.join(command)}\n")
            stream.flush()
            subprocess.run(command, cwd=ROOT, env=environment, stdout=stream, stderr=subprocess.STDOUT, check=True)
        print("\n".join(log.read_text(encoding="utf-8").splitlines()[-6:]), flush=True)


if __name__ == "__main__":
    main()
