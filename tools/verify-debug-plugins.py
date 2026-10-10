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

"""Verify debugging plugins with a default Runner and generated author plugins.

Invoked by verify-rust-scaffold.py with Runner, cargo-tenon, Source bundle and
Sink bundle paths. All processes and state belong to this isolated test.
"""

from contextlib import ExitStack
from datetime import datetime, timezone
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


def run(arguments):
    result = subprocess.run(list(map(str, arguments)), cwd=ROOT, text=True,
                            stdout=subprocess.PIPE, check=True)
    return result.stdout


def event(response):
    kind, data = "", ""
    while True:
        line = response.readline()
        assert line, "Diagnostics ended before the expected event"
        line = line.decode().rstrip("\r\n")
        if not line and kind:
            return kind, json.loads(data)
        if line.startswith("event:"):
            kind = line[6:].strip()
        elif line.startswith("data:"):
            data += line[5:].strip()


def instance(program, config=None):
    return {"programName": program, "exactVersion": "0.1.0", "config": config or {}}


def verify(runner, checker, source_bundle, sink_bundle):
    bundles = [Path(source_bundle), Path(sink_bundle)]
    for role in ["dummy-source", "stdout-sink"]:
        report = json.loads(run([checker, "bundle", "--locked", "--manifest-path",
                                 ROOT / f"plugin/bifromq-tenon-{role}-plugin/Cargo.toml"]))
        assert report["package"]["interface"] == ("source" if role == "dummy-source" else "sink")
        bundles.append(Path(report["bundle"]))

    state = Path(tempfile.mkdtemp(prefix="tenon-debug-", dir="/tmp"))
    try:
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        config = {"stateDirectory": str(state), "http": {"listenAddress": f"127.0.0.1:{port}"},
                  "pipeline": {"startupTimeoutMs": 5000, "shutdownTimeoutMs": 5000,
                               "retryBackoff": {"initialDelayMs": 10, "maximumDelayMs": 20}},
                  "lua": {"cpuTimeLimitMs": 50, "memoryLimitBytes": 16777216}}
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

        def wait_until(predicate):
            deadline = time.monotonic() + 30
            while True:
                value = predicate()
                if value:
                    return value
                assert process.poll() is None, "Runner exited unexpectedly"
                assert time.monotonic() < deadline, "Timed out waiting for test state"
                time.sleep(0.02)

        def running(identity):
            status, _, body = request("GET", f"/pipelines/{identity}")
            if status != 200:
                return None
            value = json.loads(body)
            return value if (value["state"] == "running"
                             and value["appliedDocumentEtag"] == value["documentEtag"]
                             and all(item["state"] == "running" for item in value["pluginInstances"])) else None

        def subscribe(stack, identity):
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
            stack.callback(connection.close)
            connection.request("GET", f"/pipelines/{identity}/diagnostics?target=plugin%3Aconsole",
                               headers={"Accept": "text/event-stream"})
            response = connection.getresponse()
            stack.callback(response.close)
            assert response.status == 200
            assert event(response)[0] == "attached"
            return response

        # Exercise the complete README Document, not a separately maintained copy.
        readme = (ROOT / "plugin/README.md").read_text()
        self_check = json.loads(re.search(r"```json\n(.*?)\n```", readme, re.S)[1])
        stdout_builder = "local b = registry:getBuilder('org.apache.bifromq.tenon.stdout-sink@0.1.0')\n"
        boundary_script = stdout_builder + r'''
local messages = {"", "中文 🦀", "first\r\nsecond", "literal \\n", string.rep("界", 10000), "after-long"}
local index = 1
setTimeout(20)
function main(event)
  assert(event.type == 'timer', 'Dummy Source emitted a record')
  b:setMessage(messages[index])
  emit(b:build())
  index = index % #messages + 1
  setTimeout(20)
end
'''
        source_script = stdout_builder + '''
local message
setTimeout(100)
function main(event)
  if event.type == 'source' then message = event.payload.message end
  if event.type == 'timer' then
    if message then b:setMessage(message); emit(b:build()) end
    setTimeout(100)
  end
end
'''
        sink_script = '''local b = registry:getBuilder('com.example.example-sink@0.1.0')
local count = 0
setTimeout(0)
function main(event)
  assert(event.type == 'timer', 'Dummy Source emitted a record')
  count = count + 1
  b:setMessage('custom-sink-' .. count)
  emit(b:build())
  if count < 3 then setTimeout(20) end
end
'''
        documents = {"debug": self_check}
        for identity, source, sink, script in [
            ("custom-source", instance("com.example.example-source", {"message": "custom-source-中文"}),
             instance("org.apache.bifromq.tenon.stdout-sink"), source_script),
            ("custom-sink", instance("org.apache.bifromq.tenon.dummy-source"),
             instance("com.example.example-sink", {"outputFile": str(state / "custom-sink.txt")}), sink_script),
        ]:
            documents[identity] = {"specVersion": "1", "id": identity,
                                   "pluginInstances": {"input": source, "console": sink},
                                   "flows": {"probe": {"source": "input", "sinks": ["console"],
                                                       "delivery": "at-least-once",
                                                       "process": {"script": script}}}}
        previous_pipelines = None
        for launch in range(2):
            with (state / f"runner-{launch}.log").open("w+") as log:
                process = subprocess.Popen([runner, "--config", str(config_path)],
                                           stdin=subprocess.DEVNULL, stdout=log, stderr=log)
                try:
                    def ready():
                        try:
                            return request("GET", "/plugins")[0] == 200
                        except ConnectionRefusedError:
                            return False
                    wait_until(ready)
                    if launch == 0:
                        for bundle in bundles:
                            for expected in [201, 204]:
                                status, _, body = request("POST", "/plugins", bundle.read_bytes(),
                                    {"Content-Type": "application/octet-stream"})
                                assert status == expected, (status, body)
                        for identity, document in documents.items():
                            status, _, body = request("PUT", f"/documents/{identity}", json.dumps(document).encode(),
                                {"Content-Type": "application/jsonc", "If-None-Match": "*"})
                            assert status == 201, (status, body)
                    for identity in documents:
                        wait_until(lambda: running(identity))
                    with ExitStack() as stack:
                        output = subscribe(stack, "debug")
                        if launch == 0:
                            kind, record = event(output)
                            assert kind == "diagnostic" and record["stream"] == "stdout", record
                            assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z probe [1-9]\d*", record["text"]), record
                            status, headers, _ = request("GET", "/documents/debug")
                            assert status == 200
                            self_check["flows"]["probe"]["process"]["script"] = boundary_script
                            etag = next(value for key, value in headers.items() if key.lower() == "etag")
                            status, _, body = request("PUT", "/documents/debug", json.dumps(self_check).encode(),
                                {"Content-Type": "application/jsonc", "If-Match": etag})
                            assert status == 204, (status, body)
                        observed = set()
                        expected = {"", "中文 🦀", r"first\r\nsecond", r"literal \\n", "long", "after-long"}
                        deadline = time.monotonic() + 30
                        while observed != expected:
                            assert time.monotonic() < deadline, ("Missing boundary output", observed)
                            kind, record = event(output)
                            assert kind == "diagnostic" and record["stream"] == "stdout", record
                            text = record["text"]
                            timestamp = datetime.strptime(text[:24], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
                            assert abs((datetime.now(timezone.utc) - timestamp).total_seconds()) < 30
                            message = text[25:]
                            if message.startswith("probe "):
                                continue
                            if record["truncated"]:
                                assert message and set(message) == {"界"}, record
                                assert 16381 <= len(text.encode()) <= 16384
                                observed.add("long")
                            else:
                                assert message in expected, record
                                observed.add(message)
                        source = subscribe(stack, "custom-source")
                        kind, record = event(source)
                        assert kind == "diagnostic" and record["stream"] == "stdout", record
                        assert record["text"][25:] == "custom-source-中文", record
                    expected_file = "custom-sink-1\ncustom-sink-2\ncustom-sink-3\n" * (launch + 1)
                    wait_until(lambda: (state / "custom-sink.txt").exists()
                               and (state / "custom-sink.txt").read_text() == expected_file)
                    parents = {int(pid): int(parent) for pid, parent in
                               (line.split() for line in run(["ps", "-A", "-o", "pid=,ppid="]).splitlines())}
                    pipelines = {pid for pid, parent in parents.items() if parent == process.pid}
                    plugins = {pid for pid, parent in parents.items() if parent in pipelines}
                    assert len(pipelines) == 3 and len(plugins) == 6, (pipelines, plugins)
                    if previous_pipelines is not None:
                        assert pipelines.isdisjoint(previous_pipelines)
                        for identity in documents:
                            status, headers, _ = request("GET", f"/documents/{identity}")
                            assert status == 200
                            etag = next(value for key, value in headers.items() if key.lower() == "etag")
                            status, _, body = request("DELETE", f"/documents/{identity}", headers={"If-Match": etag})
                            assert status == 204, (status, body)
                    previous_pipelines = pipelines
                    process.terminate()
                    assert process.wait(timeout=30) == 0
                    for pid in pipelines | plugins:
                        try:
                            os.kill(pid, 0)
                        except ProcessLookupError:
                            continue
                        raise AssertionError(f"Process {pid} survived shutdown")
                    assert not list((state / "pipelines").iterdir())
                    log.seek(0)
                    assert "runner.pipeline_shutdown_timed_out:" not in log.read()
                    print(f"PASS: debug plugins launch {launch + 1}: README, timer-only output, UTF-8/empty/escaped/long lines, custom Source/Sink and clean shutdown", flush=True)
                finally:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=30)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
    except BaseException:
        print(f"FAIL: retained debugging-plugin state and logs at {state}", flush=True)
        raise
    else:
        shutil.rmtree(state)


if __name__ == "__main__":
    verify(*sys.argv[1:])
