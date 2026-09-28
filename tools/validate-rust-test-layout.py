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

from pathlib import Path
import argparse
import json
import subprocess
import tempfile
import re
import sys


REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
SOURCE_ROOTS = [REPOSITORY_ROOT / "src",
                REPOSITORY_ROOT / "sdk/rust/ipc/src",
                REPOSITORY_ROOT / "sdk/rust/process-metrics/src",
                REPOSITORY_ROOT / "sdk/rust/plugin-sdk/src",
                REPOSITORY_ROOT / "sdk/rust/cargo-tenon/src"]
ALLOWED_TEST_MODULE = re.compile(
    r"(?:pub(?:\((?:crate|super|self|in\s+[a-z0-9_:]+)\))?\s+)?"
    r"mod\s+(?:tests|test_support|[a-z0-9_]+_(?:tests|test_support))\b"
)
CRATE_SELF_ALIAS = "extern crate self as tenon;"


def next_code_line(lines: list[str], start: int) -> tuple[int, str] | None:
    for index in range(start, len(lines)):
        stripped = lines[index].strip()
        if stripped and not stripped.startswith(("//", "#[path")):
            return index, stripped
    return None


def validate(path: Path) -> list[str]:
    return validate_source(path.read_text(encoding="utf-8"), path.relative_to(REPOSITORY_ROOT))


def validate_source(source: str, relative: Path) -> list[str]:
    failures: list[str] = []
    lines = source.splitlines()
    attributes: list[str] = []
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith(("#[", "//")) or not stripped:
            attributes.append(stripped)
        else:
            if ALLOWED_TEST_MODULE.match(stripped):
                gates = {attribute.replace(" ", "") for attribute in attributes}
                if not any(
                    gate in {'#[cfg(test)]', '#[cfg(feature="repository-test-support")]',
                             '#[cfg(any(test,feature="repository-test-support"))]'}
                    or gate.startswith('#[cfg(all(test,')
                    for gate in gates
                ):
                    failures.append(f"{relative}:{index + 1}: test module requires an explicit test build condition")
            attributes.clear()
        if line.strip() != "#[cfg(test)]":
            continue
        line_number = index + 1
        if line != "#[cfg(test)]":
            failures.append(
                f"{relative}:{line_number}: nested #[cfg(test)] mixes test code into production"
            )
            continue
        declaration = next_code_line(lines, index + 1)
        if declaration is None or (
            ALLOWED_TEST_MODULE.match(declaration[1]) is None
            and declaration[1] != CRATE_SELF_ALIAS
        ):
            failures.append(
                f"{relative}:{line_number}: top-level #[cfg(test)] must declare a test module or the crate test alias"
            )
    return failures


def library_metadata(features: list[str], manifest: str, package: str) -> Path:
    command = ["cargo", "check", "--manifest-path", manifest, "--package", package, "--lib", "--locked",
               "--message-format=json", *features]
    result = subprocess.run(command, cwd=REPOSITORY_ROOT, text=True, stdout=subprocess.PIPE)
    if result.returncode:
        print(result.stdout, file=sys.stderr)
        raise RuntimeError("Library compilation failed")
    for line in result.stdout.splitlines():
        message = json.loads(line)
        if (message.get("reason") == "compiler-artifact"
                and message["target"]["name"] == package.replace("-", "_")
                and "lib" in message["target"]["kind"]):
            return next(Path(name) for name in message["filenames"] if name.endswith(".rmeta"))
    raise RuntimeError(f"Cargo did not report the {package} library metadata")


def compile_caller(directory: Path, library: Path, source: str, crate: str) -> subprocess.CompletedProcess:
    caller = directory / "caller.rs"
    caller.write_text(source, encoding="utf-8")
    return subprocess.run(
        ["rustc", "--edition=2024", "--crate-type=lib", "--emit=metadata",
         "--error-format=json", "--extern", f"{crate}={library}",
         "-L", f"dependency={library.parent}", "-o", str(directory / "caller.rmeta"), str(caller)],
        cwd=REPOSITORY_ROOT, text=True, capture_output=True,
    )


def validate_compile_boundary() -> None:
    packages = [
        ("Cargo.toml", "tenon-ipc", "pub use tenon_ipc::{queue::{QueueReader, QueueWriter, OwnedRecord, ReadPosition, WriteReceipt}, bell::{BellRegion, LoopBell, BellInterrupter}};", "queue::contract_test_support", ["bell::fail_next_platform_wake", "queue::queue_waiter_is_armed"]),
        ("Cargo.toml", "tenon", "pub use tenon::{RunnerConfig, RunnerHooks, ScriptVmLimits, ExactVersion};", "runner_test_support", []),
        ("sdk/rust/Cargo.toml", "tenon-plugin-sdk", "pub use tenon_plugin_sdk::{TenonSource, SourceProgram, TenonSink, FlowChannel, PayloadSender, Completion, AckCode, SinkProgram, TenonSourceAndSink, SourceAndSinkProgram};", "repository_test_support", ["PluginInterface", "parse_json", "JsonError"]),
    ]
    with tempfile.TemporaryDirectory(prefix="tenon-test-boundary-") as temporary:
        directory = Path(temporary)
        for manifest, package, formal_source, test_module, internal_exports in packages:
            crate = package.replace("-", "_")
            library = library_metadata([], manifest, package)
            formal = compile_caller(directory, library, formal_source, crate)
            if formal.returncode:
                raise RuntimeError(f"Default public API import failed: {formal.stderr}")
            for name in [test_module, *internal_exports]:
                source = f"pub use {crate}::{name};"
                hidden = compile_caller(directory, library, source, crate)
                diagnostics = [json.loads(line) for line in hidden.stderr.splitlines()]
                errors = [entry for entry in diagnostics if entry.get("code") is not None
                          and entry.get("level") == "error"]
                if (hidden.returncode == 0 or len(errors) != 1
                        or errors[0]["code"]["code"] != "E0432"
                        or f"unresolved import `{crate}::{name}`" not in errors[0]["message"]):
                    raise RuntimeError(f"Expected the missing export error E0432: {hidden.stderr}")
            library = library_metadata(["--features", "repository-test-support"], manifest, package)
            source = f"pub use {crate}::{test_module};"
            enabled = compile_caller(directory, library, source, crate)
            if enabled.returncode:
                raise RuntimeError(f"Explicit test API import failed: {enabled.stderr}")
    print("Rust test boundary compilation passed: default APIs available, test APIs absent, feature APIs available")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--compile-boundary", action="store_true")
    arguments = parser.parse_args()
    failures = [
        failure
        for root in SOURCE_ROOTS
        for path in sorted(root.rglob("*.rs"))
        for failure in validate(path)
    ]
    if failures:
        print("Rust test layout validation failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print("Rust test layout validation passed")
    if arguments.compile_boundary:
        validate_compile_boundary()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
