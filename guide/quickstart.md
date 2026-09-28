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

# Local quickstart

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../DISCLAIMER).

This example builds from a source checkout. A generated dual-interface plugin sends one text record through Lua and writes it to a file, so no broker or published Tenon package is needed. Run on a supported native platform with the [build prerequisites](../README.md#build-and-try), Python 3 and curl. Use a free local port; the example uses 18080.

## Build the Runner and tools

From the Tenon source root:

```sh
tenon_root="$PWD"
cargo build --locked --bin tenon
cargo install cargo-generate --version 0.24.0 --locked
cargo install --path sdk/rust/cargo-tenon --locked
demo_root="$(mktemp -d "${TMPDIR:-/tmp}/tenon-quickstart.XXXXXX")"
cd "$demo_root"
cargo generate --path "$tenon_root/sdk/rust/rust-plugin-scaffold" \
  --name hello-tenon --define interface=source-and-sink \
  --silent --vcs none --no-workspace
cd hello-tenon
cargo tenon bundle \
  --config "patch.crates-io.tenon-plugin-sdk.path=\"$tenon_root/sdk/rust/plugin-sdk\"" \
  --config "patch.crates-io.tenon-ipc.path=\"$tenon_root/sdk/rust/ipc\"" \
  > "$demo_root/bundle.json"
bundle="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["bundle"])' "$demo_root/bundle.json")"
```

The local Cargo patches select the SDK and IPC code in this checkout. The bundle command reports the actual bundle path in JSON. Keep this shell open so the variables remain available.

## Configure and start

```sh
python3 - "$demo_root" <<'PYTHON'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
(root / "runner.jsonc").write_text(json.dumps({
    "stateDirectory": str(root / "state"),
    "http": {"listenAddress": "127.0.0.1:18080"},
    "pipeline": {"retryBackoff": {"initialDelayMs": 100, "maximumDelayMs": 30000}},
    "lua": {"cpuTimeLimitMs": 50, "memoryLimitBytes": 16777216}
}))
(root / "hello.jsonc").write_text(json.dumps({
    "specVersion": "1", "id": "hello",
    "pluginInstances": {"example": {
        "programName": "com.example.hello-tenon", "exactVersion": "0.1.0",
        "config": {"message": "Hello Tenon", "outputFile": str(root / "output.txt")}
    }},
    "flows": {"main": {
        "source": "example", "sinks": ["example"],
        "process": {"script": 'local b = registry:getBuilder("com.example.hello-tenon@0.1.0")\nfunction main(event)\n b:setMessage(event.payload.message)\n emit(b:build())\nend'}
    }}
}))
PYTHON
"$tenon_root/target/debug/tenon" --config "$demo_root/runner.jsonc" \
  > "$demo_root/runner.log" 2>&1 &
runner_pid=$!
```

Wait until this request succeeds; if the process exits, inspect `runner.log`:

```sh
curl --fail http://127.0.0.1:18080/openapi.json > "$demo_root/openapi.json"
```

## Install and run

```sh
curl --fail -i -X POST http://127.0.0.1:18080/plugins \
  -H 'Content-Type: application/vnd.apache.tenon.plugin+tar+gzip' \
  --data-binary "@$bundle"
curl --fail -i -X PUT http://127.0.0.1:18080/documents/hello \
  -H 'Content-Type: application/jsonc' -H 'If-None-Match: *' \
  --data-binary "@$demo_root/hello.jsonc"
curl --fail http://127.0.0.1:18080/pipelines/hello
cat "$demo_root/output.txt"
```

Installation and creation return 201. Application is asynchronous: repeat the status read until `state` is `running`, `appliedDocumentEtag` equals `documentEtag`, and the plugin is running. The output file must contain `Hello Tenon`. Wait until the file appears and contains that line. Each fresh plugin process emits the example message, so a restart can append another line.

## Stop and clean up

```sh
etag="$(curl --fail -sS -D - -o /dev/null http://127.0.0.1:18080/documents/hello \
  | tr -d '\r' | sed -n 's/^[Ee][Tt][Aa][Gg]: //p')"
curl --fail -i -X DELETE http://127.0.0.1:18080/documents/hello \
  -H "If-Match: $etag"
kill -TERM "$runner_pid"
wait "$runner_pid"
```

The Document is removed and the Runner stops its processes. The temporary directory retains the package, logs, configuration and result for inspection; remove it when no longer needed. A deployed system should use its service manager and an appropriate persistent state directory instead.
