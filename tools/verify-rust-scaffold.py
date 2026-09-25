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

"""Generate and verify all official Rust Plugin project shapes outside the repository."""

from contextlib import ExitStack
import json
import http.client
import io
import os
import signal
from pathlib import Path
import re
import shutil
import subprocess
import sys
import socket
import time
import tarfile
import tempfile

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "sdk/rust/rust-plugin-scaffold"
WORKSPACE = ROOT / "sdk/rust/Cargo.toml"


def run(arguments, *, cwd=ROOT, env=None):
    print("+ " + " ".join(map(str, arguments)), flush=True)
    result = subprocess.run(arguments, cwd=cwd, env=env, text=True, stdout=subprocess.PIPE)
    if result.returncode:
        print(result.stdout, flush=True)
        result.check_returncode()
    return result.stdout


def messages(arguments, **kwargs):
    return [json.loads(line) for line in run(arguments, **kwargs).splitlines()]


def verify(directory):
    workspace = json.loads(run(["cargo", "metadata", "--no-deps", "--format-version=1", "--locked"]))
    packages = [next(package for package in workspace["packages"] if package["name"] == name)
                for name in ["tenon-ipc", "tenon-plugin-sdk"]]
    patch = []
    for package in packages:
        # Resolve the unpublished dependency at its locked workspace path while
        # packaging. Author projects below use only the two extracted archives.
        package_patch = [] if package["name"] == "tenon-ipc" else [
            "--config", f"patch.crates-io.tenon-ipc.path={json.dumps(str(Path(packages[0]['manifest_path']).parent))}"
        ]
        run(["cargo", "package", "-p", package["name"], "--registry", "crates-io",
             "--locked", "--no-verify", "--allow-dirty", *package_patch])
        archive = Path(workspace["target_directory"]) / "package" / f"{package['name']}-{package['version']}.crate"
        # These archives are produced above by Cargo from the current sources.
        with tarfile.open(archive) as contents:
            contents.extractall(directory / "packages")
        package_path = directory / "packages" / f"{package['name']}-{package['version']}"
        patch += ["--config", f"patch.crates-io.{package['name']}.path={json.dumps(str(package_path))}"]
    environment = dict(os.environ, CARGO_TARGET_DIR=str(directory / "target"))
    cli_messages = messages(["cargo", "build", "--manifest-path", str(WORKSPACE),
                             "-p", "cargo-tenon", "--locked", "--message-format=json"])
    checker = next(message["executable"] for message in cli_messages
                   if message.get("executable") and message["target"]["name"] == "cargo-tenon")

    invalid = subprocess.run(["cargo", "generate", "--path", str(TEMPLATE), "--name", "invalid",
                              "--define", "interface=invalid", "--silent", "--destination", str(directory),
                              "--vcs", "none", "--no-workspace"], text=True, capture_output=True)
    assert invalid.returncode != 0 and "interface" in invalid.stderr, invalid.stderr
    assert not (directory / "invalid/Cargo.toml").exists()

    bundles = {}
    target = next(line.removeprefix("host: ") for line in run(["rustc", "-vV"]).splitlines() if line.startswith("host: "))
    for interface in ["source", "sink", "source-and-sink"]:
        name = f"example-{interface}"
        run(["cargo", "generate", "--path", str(TEMPLATE), "--name", name,
             "--define", f"interface={interface}", "--silent", "--destination", str(directory),
             "--vcs", "none", "--no-workspace"])
        project = directory / name
        validator = Draft202012Validator(json.loads((project / "config.schema.json").read_text(encoding="utf-8")))
        example = json.loads(re.search(r"```json\n(.*?)\n```", (project / "README.md").read_text(encoding="utf-8"), re.S)[1])
        validator.validate(example)
        assert not validator.is_valid(dict(example, unexpected="value"))
        for field in example:
            invalid_config = dict(example)
            del invalid_config[field]
            if interface != "source-and-sink":
                assert not validator.is_valid(invalid_config), f"Missing {field} must be rejected"
            else:
                # A combined plugin can run with either direction unbound. The
                # runtime requires a field only when its corresponding direction
                # is actually present in the Flow topology.
                assert validator.is_valid(invalid_config), f"Missing {field} must be accepted for a combined plugin"
            invalid_config[field] = 42
            assert not validator.is_valid(invalid_config), f"Wrong {field} type must be rejected"
        if "message" in example:
            assert not validator.is_valid(dict(example, message=""))
        if "outputFile" in example:
            assert not validator.is_valid(dict(example, outputFile="relative.txt"))
        original_manifest = (project / "Cargo.toml").read_bytes()
        assert not list(project.rglob("*.liquid"))
        assert not (project / "manifest.json").exists()
        for role in ["source", "sink"]:
            required = role in interface
            assert (project / f"proto/{role}_record_payload.proto").exists() == required
        assert (project / "src/source").exists() == (interface != "sink")
        assert (project / "src/output").exists() == (interface != "source")
        assert (project / "src/file_program").exists() == (interface != "source")
        run(["cargo", "fmt", "--", "--check"], cwd=project, env=environment)
        run(["cargo", "generate-lockfile", *patch], cwd=project, env=environment)
        metadata = json.loads(run(["cargo", "metadata", "--format-version=1", "--locked", *patch],
                                  cwd=project, env=environment))
        package = next(package for package in metadata["packages"] if package["name"] == name)
        assert [dep["name"] for dep in package["dependencies"] if dep["kind"] is None] == ["tenon-plugin-sdk"]
        sdk_package = next(package for package in metadata["packages"] if package["name"] == "tenon-plugin-sdk")
        sdk_node = next(node for node in metadata["resolve"]["nodes"] if node["id"] == sdk_package["id"])
        assert "repository-test-support" not in sdk_node["features"]
        ipc_package = next(package for package in metadata["packages"] if package["name"] == "tenon-ipc")
        ipc_node = next(node for node in metadata["resolve"]["nodes"] if node["id"] == ipc_package["id"])
        assert "repository-test-support" not in ipc_node["features"]
        assert not any(dep["name"] in {"tenon", "tenon-plugin-sdk"} for dep in ipc_package["dependencies"])
        assert package["metadata"]["tenon"] == {"program-name": f"com.example.{name}", "interface": interface,
                                               "display-name": name, "description": f"Example Tenon {interface} plugin."}
        run(["cargo", "test", "--locked", *patch], cwd=project, env=environment)
        run(["cargo", "clippy", "--all-targets", "--locked", *patch, "--", "-D", "warnings"],
            cwd=project, env=environment)
        built = messages(["cargo", "build", "--locked", "--message-format=json", *patch],
                         cwd=project, env=environment)
        output = Path(next(message["out_dir"] for message in built
                           if message["reason"] == "build-script-executed" and message["package_id"] == package["id"]))
        executable = next(message["executable"] for message in built
                          if message.get("executable") and message["package_id"] == package["id"])
        assert os.access(executable, os.X_OK)
        assert (project / "Cargo.toml").read_bytes() == original_manifest

        build_command = [checker, "build", "--locked", "--release", "--target", target, *patch]
        build_report = json.loads(run(build_command, cwd=project, env=environment))
        bundle_command = [checker, "bundle", "--locked", "--release", "--target", target, *patch]
        bundled = json.loads(run(bundle_command, cwd=project, env=environment))
        assert bundled["package"] == build_report["package"]
        bundle = Path(bundled["bundle"])
        previous_bundle = bundle.read_bytes()
        run(bundle_command, cwd=project, env=environment)
        assert bundle.read_bytes() == previous_bundle, "Identical inputs must produce identical archives"
        bundles[interface] = bundle
        contracts = directory / f"contracts-{interface}"
        with tarfile.open(bundle) as archive:
            assert archive.getnames() == ["config.schema.json", "manifest.json", "payload.descriptor.pb", "program"]
            assert all(member.isfile() for member in archive.getmembers())
            # This fixed-layout archive was produced immediately above by cargo-tenon.
            archive.extractall(contracts)
        report = json.loads(run([checker, "check", str(contracts)]))
        assert report == bundled["package"]
        assert report["interface"] == interface
        for role in ["source", "sink"]:
            assert report[f"{role}Root"] == (f"plugin.{role.title()}RecordPayload" if role in interface else None)
        if interface == "source":
            descriptor = output / "payload.descriptor.pb"
            previous = descriptor.read_bytes()
            proto = project / "proto/source_record_payload.proto"
            proto.write_text(proto.read_text(encoding="utf-8").replace(
                "// One message emitted by this program.", "// A changed description retained in source info."),
                encoding="utf-8")
            run(["cargo", "build", "--locked", *patch], cwd=project, env=environment)
            assert descriptor.read_bytes() != previous, "Cargo must regenerate descriptor source info after proto edits"
            shutil.copy2(descriptor, contracts)
            run([checker, "check", str(contracts)])
        print(f"PASS: {interface} generation, tests, contracts and reproducible release bundle", flush=True)
    runner = os.environ.get("TENON_TEST_RUNNER_BINARY")
    if runner is None:
        built = messages(["cargo", "build", "--package", "tenon", "--bin", "tenon",
                          "--locked", "--message-format=json"])
        runner = next(message["executable"] for message in built
                      if message.get("executable") and message["target"]["name"] == "tenon")
    def rebuild_bundle(interface):
        project = directory / f"example-{interface}"
        run(["cargo", "fmt"], cwd=project, env=environment)
        run(["cargo", "clippy", "--all-targets", "--locked", *patch, "--", "-D", "warnings"],
            cwd=project, env=environment)
        bundled = json.loads(run([checker, "bundle", "--locked", "--release", "--target", target, *patch],
                                 cwd=project, env=environment))
        return Path(bundled["bundle"])

    run([sys.executable, ROOT / "tools/verify-debug-plugins.py", runner, checker,
         bundles["source"], bundles["sink"]])

    for scenario in ["drained", "runner-lost"]:
        verify_runner(runner, bundles, scenario)
    verify_runner(runner, bundles, "drained", chain_programs=("source-and-sink", "source-and-sink"))
    for interface in bundles:
        instrument_business(directory / f"example-{interface}", interface)
        bundles[interface] = rebuild_bundle(interface)
    for scenario in ["sink-replay", "sink-killed"]:
        verify_runner(runner, bundles, scenario)
    for interface in ["source", "source-and-sink"]:
        instrument_quiesce(directory / f"example-{interface}")
        bundles[interface] = rebuild_bundle(interface)
    for scenario in ["in-flight", "timed-out"]:
        verify_runner(runner, bundles, scenario)


def replace(path, before, after):
    source = path.read_text()
    assert source.count(before) == 1, (path, before)
    path.write_text(source.replace(before, after))


def instrument_business(project, interface):
    # Customize only the generated business code, just as a Plugin author would.
    # The SDK, official template and process protocol retain their default builds.
    shutil.copytree(ROOT / "tools/fixtures/rust-scaffold-shutdown", project / "src/shutdown_probe")

    replace(project / "src/main.rs", "mod payload;", "mod payload;\nmod shutdown_probe;")
    if interface != "sink":
        source = project / "src/source/mod.rs"
        replace(source, "    payload: SourceRecordPayload,",
                "    completion: Option<tenon_plugin_sdk::Completion>,\n    payload: SourceRecordPayload,")
        replace(source, "        Ok(Self {", "        Ok(Self {\n            completion: None,")
        replace(source, '        std::thread::spawn(move || eprintln!("Source completion: {:?}", completion.wait()));',
                '        self.completion = Some(completion);')
        replace(source, '        // The SDK has settled the completion; no external resource remains.', '''        crate::shutdown_probe::event("source-close").expect("record Source close");
        let mut completion = self.completion.take().expect("the finite Source started once");
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        let std::task::Poll::Ready(result) = std::future::Future::poll(
            std::pin::Pin::new(&mut completion), &mut context) else {
            panic!("SDK must settle Source completion before business close");
        };
        crate::shutdown_probe::event(&format!("completion {result:?}")).expect("record completion");''')
    if interface != "source":
        business = project / "src/file_program/mod.rs"
        write_call = ('        ready(self.output.write(&records))'
                      if interface == "sink" else '''        ready(match &self.output {
            Some(output) => output.write(&records),
            None => Err(std::io::Error::other("Sink direction is not bound").into()),
        })''')
        replace(business, write_call, '''        ready((|| {
            crate::shutdown_probe::event(&format!("write-entered {}", std::process::id()))?;
            let mut permission = [0];
            std::io::Read::read_exact(
                &mut std::fs::File::open(crate::shutdown_probe::directory().join("release"))?,
                &mut permission,
            )?;
            self.output.write(&records)?;
            match permission[0] {
                b'1' => Ok(()),
                b'F' => Err("Expected Sink failure after the external write".into()),
                _ => unreachable!("the verifier sends only success or failure"),
            }
        })())''')
        if interface == "source-and-sink":
            replace(business, '            self.output.write(&records)?;',
                    '            self.output.as_ref().expect("Sink is bound in this scenario").write(&records)?;')
        replace(business, '        // Every successful batch is already durable; dropping this owner closes the file.',
                '        crate::shutdown_probe::event("sink-close").expect("record Sink close");\n'
                '        // Every successful batch is already durable; dropping this owner closes the file.')


def instrument_quiesce(project):
    source = project / "src/source/mod.rs"
    replace(source, '    fn quiesce(&mut self) {', '''    fn quiesce(&mut self) {
        let completion = self.completion.as_mut().expect("the finite Source started once");
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(std::pin::Pin::new(completion), &mut context).is_pending(),
                "Sink has not completed its write, so Source completion must still be pending");
        crate::shutdown_probe::event("quiesce-pending").expect("record quiesce");''')


def verify_runner(runner, bundles, scenario, *, chain_programs=("source", "sink")):
    # Keep the state root short enough for the platform's UDS path limit.
    state = Path(tempfile.mkdtemp(prefix="tenon-rust-runner-", dir="/tmp"))
    gates = ExitStack()
    try:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        config = {"stateDirectory": str(state), "http": {"listenAddress": f"127.0.0.1:{port}"},
                  "pipeline": {"startupTimeoutMs": 5000, "shutdownTimeoutMs": 5000,
                               "retryBackoff": {"initialDelayMs": 10, "maximumDelayMs": 20}},
                  "lua": {"cpuTimeLimitMs": 50, "memoryLimitBytes": 16777216}}
        Draft202012Validator(json.loads((ROOT / "contracts/runner/config.schema.json").read_text())).validate(config)
        config_path = state / "runner.json"
        config_path.write_text(json.dumps(config))

        def request(method, path, body=b"", headers=None):
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
            try:
                connection.request(method, path, body, headers or {})
                response = connection.getresponse()
                return response.status, dict(response.getheaders()), response.read()
            finally:
                connection.close()

        def events(identity, interface):
            path = state / f"{identity}-{interface}" / "events"
            return path.read_text().splitlines() if path.exists() else []

        documents = {}
        release_writers = {}
        # Preserve the first launch identities to verify recovery creates new owners.
        previous_pipeline_launch_ids = {}
        for launch in range(2 if scenario in ("drained", "runner-lost") else 1):
            with (state / f"runner-{launch}.log").open("w+") as log:
                process = subprocess.Popen([runner, "--config", str(config_path)],
                                           stdin=subprocess.DEVNULL, stdout=log, stderr=log)
                try:
                    deadline = time.monotonic() + 30
                    while True:
                        assert process.poll() is None, "Runner exited before HTTP was ready"
                        try:
                            status, _, _ = request("GET", "/plugins")
                            assert status == 200
                            break
                        except ConnectionRefusedError:
                            assert time.monotonic() < deadline, "Runner did not open HTTP"
                            time.sleep(0.01)
                    if launch == 0:
                        if scenario == "drained":
                            verify_rejected_uploads(request, bundles["source"])
                        for bundle in bundles.values():
                            for expected in [201, 204]:
                                status, _, body = request("POST", "/plugins", bundle.read_bytes(),
                                                          {"Content-Type": "application/vnd.apache.tenon.plugin+tar+gzip"})
                                assert status == expected, (status, body)
                        for identity, source, sink in [("rust-chain", *chain_programs),
                                                       ("rust-loop", "source-and-sink", "source-and-sink")]:
                            output = state / f"{identity}.txt"
                            if scenario not in ("drained", "runner-lost"):
                                for role in {source, sink}:
                                    control = state / f"{identity}-{role}"
                                    control.mkdir()
                                    if role != "source":
                                        fifo = control / "release"
                                        os.mkfifo(fifo)
                                        # Own both ends so opening or releasing cannot block the verifier.
                                        descriptor = os.open(fifo, os.O_RDWR | os.O_NONBLOCK)
                                        release_writers[identity] = gates.enter_context(
                                            os.fdopen(descriptor, "wb", buffering=0))
                            instance = lambda role, config: {"programName": f"com.example.example-{role}",
                                                              "exactVersion": "0.1.0", "config": config}
                            if identity == "rust-loop":
                                instances = {"shared": instance(source, {"message": identity, "outputFile": str(output)})}
                                source_id = sink_id = "shared"
                            else:
                                instances = {"source": instance(source, {"message": identity}),
                                             "sink": instance(sink, {"outputFile": str(output)})}
                                source_id, sink_id = "source", "sink"
                            if scenario not in ("drained", "runner-lost"):
                                for item in instances.values():
                                    role = item["programName"].removeprefix("com.example.example-")
                                    item["env"] = {"TENON_SCAFFOLD_CONTROL": str(state / f"{identity}-{role}")}
                            script = f'local builder = registry:getBuilder("com.example.example-{sink}@0.1.0")\nfunction main(event)\n builder:setMessage(event.payload.message)\n emit(builder:build())\nend'
                            document = {"specVersion": "1", "id": identity, "pluginInstances": instances,
                                        "flows": {"main": {"maxPendingRecords": 4, "maxRecordBytes": 1024, "source": source_id, "sinks": [sink_id],
                                                           "delivery": "at-least-once", "process": {"script": script}}}}
                            status, headers, body = request("PUT", f"/documents/{identity}", json.dumps(document).encode(),
                                                            {"Content-Type": "application/jsonc", "If-None-Match": "*"})
                            assert status == 201, (status, body)
                            etag = next(value for name, value in headers.items() if name.lower() == "etag")
                            documents[identity] = etag
                    status, _, body = request("GET", "/plugins")
                    assert status == 200 and len(json.loads(body)["plugins"]) == 3, (status, body)
                    for identity, etag in documents.items():
                        status, headers, body = request("GET", f"/documents/{identity}")
                        assert status == 200, (status, body)
                        assert next(value for name, value in headers.items() if name.lower() == "etag") == etag
                        output = state / f"{identity}.txt"
                        deadline = time.monotonic() + 30
                        while True:
                            status, _, body = request("GET", f"/pipelines/{identity}")
                            assert status == 200, (status, body)
                            pipeline = json.loads(body)
                            if (pipeline["state"] == "running" and pipeline["appliedDocumentEtag"] == etag
                                    and all(item["state"] == "running" for item in pipeline["pluginInstances"])
                                    and (scenario not in ("drained", "runner-lost") or
                                         output.exists() and output.read_text() == (identity + "\n") * (launch + 1))):
                                break
                            assert time.monotonic() < deadline, pipeline
                            time.sleep(0.01)
                    pipeline_launch_ids = wait_for_pipeline_records(
                        request, documents, "drained" if scenario in ("drained", "runner-lost") else "pending")
                    if scenario not in ("drained", "runner-lost"):
                        deadline = time.monotonic() + 30
                        while any(len(events(identity, role)) != 1
                                  or re.fullmatch(r"write-entered [0-9]+", events(identity, role)[0]) is None
                                  for identity, role in [("rust-chain", "sink"), ("rust-loop", "source-and-sink")]):
                            assert process.poll() is None, "Runner exited before the held writes"
                            assert time.monotonic() < deadline, "Generated Sinks did not enter their held writes"
                            time.sleep(0.01)
                        assert all((state / f"{identity}.txt").read_text() == "" for identity in documents)
                        first_writes = {identity: events(identity, role)[0] for identity, role in
                                        [("rust-chain", "sink"), ("rust-loop", "source-and-sink")]}

                    if launch == 0:
                        previous_pipeline_launch_ids.update(pipeline_launch_ids)
                    else:
                        assert all(pipeline_launch_ids[identity] != previous_pipeline_launch_ids[identity]
                                   for identity in documents)
                    parents = {int(pid): int(parent) for pid, parent in
                               (line.split() for line in run(["ps", "-A", "-o", "pid=,ppid="]).splitlines())}
                    pipelines = {pid for pid, parent in parents.items() if parent == process.pid}
                    assert len(pipelines) == 2, pipelines
                    plugins = {pid for pid, parent in parents.items() if parent in pipelines}
                    assert sorted(sum(parent == pid for parent in parents.values()) for pid in pipelines) == [1, 2]
                    assert all(parent not in plugins for parent in parents.values()), "Generated plugins have no child processes"
                    if scenario in ("sink-replay", "sink-killed"):
                        old_sink = int(first_writes["rust-chain"].split()[1])
                        assert old_sink in plugins, (old_sink, plugins)
                        before_replay = [first_writes["rust-chain"]]
                        if scenario == "sink-replay":
                            assert release_writers["rust-chain"].write(b"F") == 1
                        else:
                            # This live Sink is held in write under the Runner's process tree.
                            # Inject one failure; its Pipeline still owns reaping and restart.
                            assert process.poll() is None, "Runner exited before Sink fault injection"
                            assert events("rust-chain", "sink") == before_replay
                            os.kill(old_sink, signal.SIGKILL)
                        deadline = time.monotonic() + 30
                        while True:
                            replay_events = events("rust-chain", "sink")
                            status, _, body = request("GET", "/pipelines/rust-chain")
                            assert status == 200, (status, body)
                            if (len(replay_events) == len(before_replay) + 1
                                    and replay_events[:-1] == before_replay
                                    and re.fullmatch(r"write-entered [0-9]+", replay_events[-1])
                                    and all(item["state"] == "running" for item in json.loads(body)["pluginInstances"])):
                                break
                            assert process.poll() is None, "Runner exited before Sink replay"
                            assert time.monotonic() < deadline, ("Sink did not restart and replay", replay_events, body)
                            time.sleep(0.01)
                        new_sink = int(replay_events[-1].split()[1])
                        assert new_sink != old_sink, replay_events
                        current_parents = {int(pid): int(parent) for pid, parent in
                                           (line.split() for line in run(["ps", "-A", "-o", "pid=,ppid="]).splitlines())}
                        assert old_sink not in current_parents, "Old Sink must be reaped before its replacement"
                        assert {pid for pid, parent in current_parents.items() if parent == process.pid} == pipelines
                        assert current_parents[new_sink] == parents[old_sink]
                        current_plugins = {pid for pid, parent in current_parents.items() if parent in pipelines}
                        assert current_plugins == (plugins - {old_sink}) | {new_sink}, (plugins, current_plugins)
                        assert all(current_parents[pid] == parents[pid] for pid in plugins - {old_sink})
                        assert wait_for_pipeline_records(request, documents, "pending") == pipeline_launch_ids
                        assert (state / "rust-chain.txt").read_text() == ("rust-chain\n" if scenario == "sink-replay" else "")
                        assert (state / "rust-loop.txt").read_text() == ""
                        for writer in release_writers.values():
                            assert writer.write(b"1") == 1
                        assert wait_for_pipeline_records(request, documents, "drained") == pipeline_launch_ids
                        assert (state / "rust-chain.txt").read_text() == "rust-chain\n" * (2 if scenario == "sink-replay" else 1)
                        assert (state / "rust-loop.txt").read_text() == "rust-loop\n"
                        plugins = current_plugins
                        print(f"PASS: {scenario} replays one committed record in a new Sink; Pipeline, Source and unrelated processes retained", flush=True)
                    runtime_roots = list((state / "pipelines").iterdir())
                    assert len(runtime_roots) == 1, "One Runner owns one runtime directory"
                    if scenario == "runner-lost":
                        if launch == 0:
                            crashed_runtime_root = runtime_roots[0]
                            process.kill()
                            assert process.wait(timeout=30) == -signal.SIGKILL
                            # Pipeline owns group termination after Runner loss;
                            # the OS init process then reaps the orphaned children.
                            deadline = time.monotonic() + 30
                            for pid in pipelines | plugins:
                                while True:
                                    try:
                                        os.kill(pid, 0)
                                    except ProcessLookupError:
                                        break
                                    assert time.monotonic() < deadline, f"Process {pid} survived Runner loss"
                                    time.sleep(0.01)
                            assert crashed_runtime_root.is_dir(), "Crashed Runner leaves runtime cleanup for startup"
                            print("PASS: Runner loss terminates all five children and retains runtime files for recovery", flush=True)
                            continue
                        assert not crashed_runtime_root.exists(), "Restarted Runner did not remove its predecessor's runtime files"
                    process.terminate()
                    if scenario in ("in-flight", "timed-out"):
                        deadline = time.monotonic() + 30
                        while any("quiesce-pending" not in events(identity, role)
                                  for identity, role in [("rust-chain", "source"), ("rust-loop", "source-and-sink")]):
                            assert process.poll() is None, "Runner exited before Source quiesce observed pending results"
                            assert time.monotonic() < deadline, "Sources did not quiesce during held Sink writes"
                            time.sleep(0.01)
                        if scenario == "in-flight":
                            for writer in release_writers.values():
                                assert writer.write(b"1") == 1
                    assert process.wait(timeout=30) == 0, "Runner failed during shutdown"
                    log.seek(0)
                    timeouts = [line for line in log.read().splitlines()
                                if line.startswith("runner.pipeline_shutdown_timed_out:")]
                    if scenario == "timed-out":
                        assert sorted(timeouts) == [
                            f"runner.pipeline_shutdown_timed_out: Pipeline shutdown exceeded its deadline: {identity}"
                            for identity in sorted(documents)], timeouts
                    else:
                        assert not timeouts, ("Runner forced a timed-out Pipeline to stop", timeouts)
                    if scenario not in ("drained", "runner-lost"):
                        source_events = ["quiesce-pending"] if scenario in ("in-flight", "timed-out") else []
                        if scenario != "timed-out":
                            source_events += ["source-close", "completion Ok(Ok)"]
                        sink_events = (replay_events if scenario in ("sink-replay", "sink-killed") else [first_writes["rust-chain"]])
                        if scenario != "timed-out":
                            sink_events = [*sink_events, "sink-close"]
                        assert events("rust-chain", "source") == source_events
                        assert events("rust-chain", "sink") == sink_events
                        loop_events = [first_writes["rust-loop"], *source_events]
                        if scenario != "timed-out":
                            loop_events.append("sink-close")
                        assert events("rust-loop", "source-and-sink") == loop_events
                        for identity in documents:
                            copies = 2 if scenario == "sink-replay" and identity == "rust-chain" else 1
                            expected = (identity + "\n") * copies if scenario != "timed-out" else ""
                            assert (state / f"{identity}.txt").read_text() == expected

                    for pid in pipelines | plugins:
                        try:
                            os.kill(pid, 0)
                        except ProcessLookupError:
                            continue
                        raise AssertionError(f"Process {pid} survived normal Runner shutdown")
                    assert not list((state / "pipelines").iterdir()), "Runner left runtime files after normal shutdown"
                    print(f"PASS: {scenario}, chain {chain_programs}, launch {launch + 1}: both flows verified, all five children reaped and runtime files removed", flush=True)
                except BaseException:
                    log.flush()
                    log.seek(0)
                    print(log.read(), flush=True)
                    raise
                finally:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=30)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
    except BaseException:
        # Only Runner owns the child process groups. Keep evidence if its
        # shutdown contract fails; historical PIDs are not cleanup handles.
        print(f"FAIL: retained Runner state and logs at {state}", flush=True)
        raise
    else:
        shutil.rmtree(state)
    finally:
        gates.close()


def verify_rejected_uploads(request, bundle):
    foreign_bundle = io.BytesIO()
    # Keep the real generated package valid; change only its declared CPU target.
    with tarfile.open(bundle) as original, tarfile.open(fileobj=foreign_bundle, mode="w:gz") as foreign:
        for member in original:
            content = original.extractfile(member).read()
            if member.name == "manifest.json":
                manifest = json.loads(content)
                platform = manifest["platforms"][0]
                platform["architecture"] = "amd64" if platform["architecture"] == "arm64" else "arm64"
                content = json.dumps(manifest).encode()
                member.size = len(content)
            foreign.addfile(member, io.BytesIO(content))
    for content, code in [(b"not gzip", "plugin_package_invalid"),
                          (foreign_bundle.getvalue(), "plugin_platform_mismatch")]:
        status, _, body = request("POST", "/plugins", content,
                                  {"Content-Type": "application/vnd.apache.tenon.plugin+tar+gzip"})
        assert status == 422 and json.loads(body)["error"]["code"] == code, (status, body)
        status, _, body = request("GET", "/plugins")
        assert status == 200 and json.loads(body)["plugins"] == [], (status, body)
    print("PASS: invalid and foreign-platform uploads rejected without publishing a Program", flush=True)


def wait_for_pipeline_records(request, documents, completion_state):
    names = ["tenon.flow.input.records", "tenon.flow.egress.records",
             "tenon.flow.completion.records", "tenon.queue.usage"]
    deadline = time.monotonic() + 30
    while True:
        status, _, body = request("GET", "/metrics?include=" + ",".join(names))
        assert status == 200, (status, body)
        observed = {}
        for process in json.loads(body)["processes"]:
            resource = process["resource"]
            identity = resource.get("tenon.pipeline.id")
            if resource["service.name"] != "tenon.pipeline" or identity not in documents:
                continue
            metrics = {metric["name"]: metric["points"] for metric in process["metrics"]}
            # Each finite producer sends one record to one Sink. Missing samples
            # remain unavailable; they must never be interpreted as empty Queues.
            if not all(len(metrics.get(name, [])) == 1 and int(metrics[name][0]["value"]) == 1
                       for name in names[:2]):
                continue
            if completion_state == "drained":
                completion = metrics.get("tenon.flow.completion.records", [])
                if (len(completion) != 1 or int(completion[0]["value"]) != 1
                        or completion[0]["attributes"]["result"] != "ok"):
                    continue
            queues = metrics.get("tenon.queue.usage", [])
            if (len(queues) == 3
                    and {point["attributes"]["tenon.queue.kind"] for point in queues}
                    == {"submission", "completion", "egress"}
                    and (all(int(point["value"]) == 0 for point in queues) if completion_state == "drained" else
                         any(point["attributes"]["tenon.queue.kind"] == "egress" and int(point["value"]) > 0
                             for point in queues))):
                observed[identity] = resource["service.instance.id"]
        if observed.keys() == documents.keys():
            return observed
        assert time.monotonic() < deadline, ("Generated Pipeline records did not reach the expected state", completion_state, body)
        time.sleep(0.01)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="tenon-rust-scaffold-") as temporary:
        verify(Path(temporary))
