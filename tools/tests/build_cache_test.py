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

import importlib.util
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[1] / "build-cache.py"
spec = importlib.util.spec_from_file_location("build_cache", SCRIPT)
cache = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cache)


def checkout(root, name="fixture"):
    root.mkdir(parents=True, exist_ok=True)
    (root / "rust-toolchain.toml").write_bytes((SCRIPT.parents[1] / "rust-toolchain.toml").read_bytes())
    (root / "Cargo.toml").write_text(f'[package]\nname="{name}"\nversion="0.1.0"\nedition="2024"\n[workspace]\n')
    (root / "Cargo.lock").write_text(f'version = 4\n[[package]]\nname="{name}"\nversion="0.1.0"\n')
    (root / "src").mkdir()
    (root / "src/lib.rs").write_text("pub fn fixture() {}\n")
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    subprocess.run(["git", "-C", str(root), "add", "Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "src/lib.rs"], check=True)


class BuildCacheTest(unittest.TestCase):
    def test_budget_counts_runner_sdk_and_java_and_preserves_source_release_and_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "code"
            checkout(root)
            checkout(root / "sdk/rust", "sdk_fixture")
            java = root / "sdk/java/plugin"
            java.mkdir(parents=True)
            (java / "pom.xml").write_text("<project/>\n")
            for relative in ["target/debug", "sdk/rust/target/debug", "sdk/java/plugin/target"]:
                path = root / relative
                path.mkdir(parents=True)
                (path / "built").write_bytes(b"compiled" * 1024)
            for relative in ["target/release/runner", "target/runner-java-archetype/contracts/proof.json",
                             "src/runner/pipeline/target/source.rs"]:
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("retained\n")
            roots = cache.build_directories(root)
            self.assertGreater(cache.size_bytes(roots), cache.size_bytes([root / "target"]))
            with cache.session(root), mock.patch.object(cache, "LIMIT_BYTES", 1):
                cache.maintain(root)
            self.assertFalse((root / "target/debug").exists())
            self.assertFalse((root / "sdk/rust/target/debug").exists())
            self.assertTrue((root / "target/release/runner").exists())
            nested = root / "target/debug/nested/debug"
            nested.mkdir(parents=True)
            (nested / "built").write_text("nested custom build output")
            with mock.patch.dict(os.environ, {"CARGO_BUILD_BUILD_DIR": str(nested.parent)}), cache.session(root):
                cache.maintain(root, finish=True)
            self.assertFalse((java / "target").exists())
            self.assertTrue((root / "src/runner/pipeline/target/source.rs").exists())
            self.assertTrue((root / "target/runner-java-archetype/contracts/proof.json").exists())
            self.assertEqual((root / "src/lib.rs").read_text(), "pub fn fixture() {}\n")

    def test_active_file_and_managed_session_block_removal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "code"
            checkout(root)
            built = root / "target/debug/built"
            built.parent.mkdir(parents=True)
            built.write_text("in use")
            with cache.session(root):
                result = subprocess.run([sys.executable, str(SCRIPT), "--root", str(root), "finish"],
                                        text=True, capture_output=True)
                self.assertEqual(result.returncode, 2)
                self.assertIn("owns this repository", result.stderr)
            child = subprocess.Popen([sys.executable, "-c",
                                      "import sys; held=open(sys.argv[1]); print('ready',flush=True); sys.stdin.read()",
                                      str(built)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            try:
                self.assertEqual(child.stdout.readline().strip(), "ready")
                with cache.session(root), self.assertRaises(cache.CacheBusy):
                    cache.maintain(root, finish=True)
                self.assertEqual(built.read_text(), "in use")
            finally:
                child.communicate(timeout=10)
            with cache.session(root):
                cache.maintain(root, finish=True)
            self.assertFalse(built.exists())

    def test_external_symlink_and_tracked_outputs_are_refused_before_removal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "code"
            checkout(root)
            outside = Path(directory) / "outside"
            outside.mkdir()
            (outside / "keep").write_text("retained")
            (root / "target").symlink_to(outside, target_is_directory=True)
            with cache.session(root), self.assertRaises(ValueError):
                cache.maintain(root, finish=True)
            (root / "target").unlink()
            alias = root / "inside"
            alias.symlink_to(root, target_is_directory=True)
            with mock.patch.dict(os.environ, {"CARGO_TARGET_DIR": str(alias / "target")}), cache.session(root), self.assertRaises(ValueError):
                cache.maintain(root, finish=True)
            alias.unlink()
            for configured in [str(outside), str(root)]:
                with mock.patch.dict(os.environ, {"CARGO_TARGET_DIR": configured}), cache.session(root), self.assertRaises(ValueError):
                    cache.maintain(root, finish=True)
            source = root / "target/debug/source.rs"
            source.parent.mkdir(parents=True)
            source.write_text("tracked source")
            subprocess.run(["git", "-C", str(root), "add", str(source)], check=True)
            with cache.session(root), self.assertRaises(ValueError):
                cache.maintain(root, finish=True)
            self.assertTrue(source.exists())
            self.assertEqual((outside / "keep").read_text(), "retained")

    def test_experiment_cleanup_on_failure_and_next_boundary_reaps_abandoned_build(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "code"
            checkout(root)
            with self.assertRaises(RuntimeError):
                with cache.experiment(root) as environment:
                    temporary = Path(environment["CARGO_TARGET_DIR"]).parent
                    self.assertTrue(temporary.exists())
                    Path(environment["CARGO_TARGET_DIR"]).mkdir()
                    (Path(environment["CARGO_TARGET_DIR"]) / "compiled").write_text("built")
                    raise RuntimeError("failed experiment")
            self.assertFalse(temporary.exists())
            stale = root / "target/experiments/tenon-probe-abandoned"
            stale.mkdir()
            (stale / "compiled").write_text("left by an interrupted owner")
            with cache.session(root):
                cache.maintain(root)
            self.assertFalse(stale.exists())

    def test_failed_command_status_and_signal_reaping(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "code"
            checkout(root)
            self.assertEqual(cache.run_owned(root, [sys.executable, "-c", "raise SystemExit(7)"]), 7)
            command = [sys.executable, "-c", "import signal; print('child-ready',flush=True); signal.pause()"]
            process = subprocess.Popen([sys.executable, str(SCRIPT), "--root", str(root), "run", "--", *command],
                                       text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                while "child-ready" not in process.stdout.readline():
                    self.assertIsNone(process.poll())
                process.send_signal(signal.SIGTERM)
                output, errors = process.communicate(timeout=20)
                self.assertNotEqual(process.returncode, 0)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()
            with cache.session(root):
                cache.maintain(root, finish=True)

    def test_command_exit_and_cancellation_stop_stubborn_descendants(self):
        for mode in ("exit", "cancel"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "code"
                checkout(root)
                child = "import signal; signal.signal(signal.SIGTERM, signal.SIG_IGN); print('ready',flush=True); signal.pause()"
                leader = ("import pathlib,signal,subprocess,sys; "
                          "child=subprocess.Popen([sys.executable,'-c',sys.argv[1]],stdout=subprocess.PIPE,text=True); "
                          "child.stdout.readline(); pathlib.Path('child-pid').write_text(str(child.pid)); "
                          "signal.pause() if sys.argv[2]=='cancel' else None")
                process = subprocess.Popen([sys.executable, str(SCRIPT), "--root", str(root), "run", "--",
                                            sys.executable, "-c", leader, child, mode],
                                           text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    deadline = time.monotonic() + 10
                    while not (root / "child-pid").exists() and process.poll() is None and time.monotonic() < deadline:
                        time.sleep(0.01)
                    self.assertTrue((root / "child-pid").exists())
                    if mode == "cancel":
                        process.send_signal(signal.SIGTERM)
                    output, errors = process.communicate(timeout=20)
                    self.assertEqual(process.returncode == 0, mode == "exit", output + errors)
                    pid = int((root / "child-pid").read_text())
                    status = subprocess.run(["ps", "-p", str(pid), "-o", "stat="], text=True, capture_output=True)
                    self.assertTrue(status.returncode != 0 or status.stdout.strip().startswith("Z"), status.stdout)
                finally:
                    if process.poll() is None:
                        process.kill()
                        process.communicate()


if __name__ == "__main__":
    unittest.main()
