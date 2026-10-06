#!/usr/bin/env bash
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

set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
tenon_root="$(cd -- "$script_dir/../.." && pwd)"

if ! command -v cargo >/dev/null 2>&1; then
  printf 'cargo is not on PATH. Install Rust using the project toolchain, then retry.\n' >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  printf 'python3 is required to prepare the example Document.\n' >&2
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  printf 'curl is required to call the local Runner API.\n' >&2
  exit 1
fi

# Cargo locates installed subcommands through PATH. IDEs can invoke Cargo by
# absolute path while omitting its sibling binaries from PATH.
cargo_bin="$(command -v cargo)"
export PATH="$(dirname -- "$cargo_bin"):$PATH"

generator_version="$(sed -n 's/^cargo_generate_version = "=\([^"]*\)"/\1/p' "$tenon_root/sdk/rust/rust-plugin-scaffold/cargo-generate.toml")"
installed_generator_version="$(cargo generate --version 2>/dev/null | awk '{print $NF}' || true)"
if [[ "$installed_generator_version" != "$generator_version" ]]; then
  printf 'Installing the scaffold-required cargo-generate %s...\n' "$generator_version"
  cargo install cargo-generate --version "$generator_version" --locked
fi

port="${TENON_PORT:-18080}"
case "$port" in
  ''|*[!0-9]*)
    printf 'TENON_PORT must be a port number.\n' >&2
    exit 1
    ;;
esac

demo_root="$(mktemp -d "${TMPDIR:-/tmp}/tenon-quickstart.XXXXXX")"
bundle_json="$demo_root/bundle.json"
runner_pid=''
diagnostics_pid=''

cleanup() {
  if [[ -n "$diagnostics_pid" ]] && kill -0 "$diagnostics_pid" 2>/dev/null; then
    kill "$diagnostics_pid" 2>/dev/null || true
    wait "$diagnostics_pid" 2>/dev/null || true
  fi
  if [[ -n "$runner_pid" ]] && kill -0 "$runner_pid" 2>/dev/null; then
    kill -TERM "$runner_pid" 2>/dev/null || true
    wait "$runner_pid" 2>/dev/null || true
  fi
  printf '\nQuickstart files are kept at: %s\n' "$demo_root"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

printf 'Building the Tenon Runner...\n'
cargo build --locked --bin tenon --manifest-path "$tenon_root/Cargo.toml"

printf 'Generating the example source-and-sink plugin...\n'
cargo generate --path "$tenon_root/sdk/rust/rust-plugin-scaffold" \
  --name hello-tenon --define interface=source-and-sink \
  --silent --vcs none --no-workspace --destination "$demo_root"

printf 'Bundling the plugin against the SDK in this checkout...\n'
cargo run --manifest-path "$tenon_root/sdk/rust/Cargo.toml" \
  --package cargo-tenon -- bundle \
  --manifest-path "$demo_root/hello-tenon/Cargo.toml" \
  --config "patch.crates-io.tenon-plugin-sdk.path=\"$tenon_root/sdk/rust/plugin-sdk\"" \
  --config "patch.crates-io.tenon-ipc.path=\"$tenon_root/sdk/rust/ipc\"" \
  > "$bundle_json"
bundle="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["bundle"])' "$bundle_json")"

printf 'Bundling the built-in Dummy Source and Stdout Sink...\n'
builtin_bundles=()
for plugin in dummy-source stdout-sink; do
  case "$plugin" in
    dummy-source) manifest="$tenon_root/plugin/bifromq-tenon-dummy-source-plugin/Cargo.toml" ;;
    stdout-sink) manifest="$tenon_root/plugin/bifromq-tenon-stdout-sink-plugin/Cargo.toml" ;;
  esac
  report="$demo_root/$plugin-bundle.json"
  cargo run --manifest-path "$tenon_root/sdk/rust/Cargo.toml" \
    --package cargo-tenon -- bundle \
    --manifest-path "$manifest" \
    --config "patch.crates-io.tenon-plugin-sdk.path=\"$tenon_root/sdk/rust/plugin-sdk\"" \
    --config "patch.crates-io.tenon-ipc.path=\"$tenon_root/sdk/rust/ipc\"" \
    > "$report"
  builtin_bundles+=("$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["bundle"])' "$report")")
done

printf 'Preparing the local Runner configuration and example Document...\n'
python3 - "$demo_root" "$port" <<'PYTHON'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
port = int(sys.argv[2])
(root / "runner.jsonc").write_text(json.dumps({
    "stateDirectory": str(root / "state"),
    "http": {"listenAddress": f"127.0.0.1:{port}"},
    "pipeline": {"retryBackoff": {"initialDelayMs": 100, "maximumDelayMs": 30000}},
    "lua": {"cpuTimeLimitMs": 50, "memoryLimitBytes": 16777216},
}))
(root / "hello.jsonc").write_text(json.dumps({
    "specVersion": "1",
    "id": "hello",
    "pluginInstances": {"example": {
        "programName": "com.example.hello-tenon",
        "exactVersion": "0.1.0",
        "config": {
            "message": "Hello Tenon",
            "outputFile": str(root / "output.txt"),
        },
    }},
    "flows": {"main": {
        "source": "example",
        "sinks": ["example"],
        "process": {"script": (
            'local b = registry:getBuilder("com.example.hello-tenon@0.1.0")\n'
            'function main(event)\n'
            '  b:setMessage(event.payload.message)\n'
            '  emit(b:build())\n'
            'end'
        )},
    }},
}))
(root / "builtins.jsonc").write_text(json.dumps({
    "specVersion": "1",
    "id": "builtins",
    "pluginInstances": {
        "idle": {
            "programName": "org.apache.bifromq.tenon.dummy-source",
            "exactVersion": "0.1.0",
            "config": {},
        },
        "console": {
            "programName": "org.apache.bifromq.tenon.stdout-sink",
            "exactVersion": "0.1.0",
            "config": {},
        },
    },
    "flows": {"probe": {
        "source": "idle",
        "sinks": ["console"],
        "process": {"script": (
            'local b = registry:getBuilder("org.apache.bifromq.tenon.stdout-sink@0.1.0")\n'
            'local count = 0\n'
            'setTimeout(1000)\n'
            'function main(event)\n'
            '  if event.type == "timer" then\n'
            '    count = count + 1\n'
            '    b:setMessage("quickstart " .. count)\n'
            '    emit(b:build())\n'
            '    setTimeout(1000)\n'
            '  end\n'
            'end'
        )},
    }},
}))
PYTHON

api="http://127.0.0.1:$port"
if curl --silent --fail --max-time 1 "$api/openapi.json" -o /dev/null 2>/dev/null; then
  printf 'A Tenon Runner already answers on port %s. Set TENON_PORT to a free port.\n' \
    "$port" >&2
  exit 1
fi

printf 'Starting Tenon at %s...\n' "$api"
"$tenon_root/target/debug/tenon" --config "$demo_root/runner.jsonc" \
  > "$demo_root/runner.log" 2>&1 &
runner_pid=$!

runner_ready=0
for _ in $(seq 1 60); do
  if ! kill -0 "$runner_pid" 2>/dev/null; then
    break
  fi
  if curl --silent --fail --max-time 1 "$api/openapi.json" \
    -o "$demo_root/openapi.json" 2>/dev/null; then
    runner_ready=1
    break
  fi
  sleep 1
done
if [[ "$runner_ready" != 1 ]]; then
  printf 'Runner did not become ready. See %s/runner.log\n' "$demo_root" >&2
  exit 1
fi

printf 'Installing the generated and built-in Programs...\n'
for package in "$bundle" "${builtin_bundles[@]}"; do
  curl --silent --show-error --fail -X POST "$api/plugins" \
    -H 'Content-Type: application/octet-stream' \
    --data-binary "@$package" -o /dev/null
done
printf 'Saving the two Pipeline Documents...\n'
for document in hello builtins; do
  curl --silent --show-error --fail -X PUT "$api/documents/$document" \
    -H 'Content-Type: application/jsonc' -H 'If-None-Match: *' \
    --data-binary "@$demo_root/$document.jsonc" -o /dev/null
done

printf 'Waiting for the Pipeline and plugin to run...\n'
status_file="$demo_root/pipeline.json"
ready=0
for _ in $(seq 1 60); do
  if curl --silent --show-error --fail "$api/pipelines/hello" -o "$status_file" \
    && python3 - "$status_file" <<'PYTHON'
import json
import sys

status = json.load(open(sys.argv[1]))
instances = status.get("pluginInstances", [])
raise SystemExit(not (
    status.get("state") == "running"
    and status.get("appliedDocumentEtag") == status.get("documentEtag")
    and instances
    and all(instance.get("state") == "running" for instance in instances)
))
PYTHON
  then
    ready=1
    break
  fi
  sleep 1
done
if [[ "$ready" != 1 ]]; then
  printf 'Pipeline did not become ready. See %s/runner.log and %s/pipeline.json\n' \
    "$demo_root" "$demo_root" >&2
  exit 1
fi

printf 'Waiting for the built-in Pipeline and its plugin instances to run...\n'
builtin_status_file="$demo_root/builtins-pipeline.json"
builtin_ready=0
for _ in $(seq 1 60); do
  if curl --silent --show-error --fail "$api/pipelines/builtins" -o "$builtin_status_file" \
    && python3 - "$builtin_status_file" <<'PYTHON'
import json
import sys

status = json.load(open(sys.argv[1]))
instances = status.get("pluginInstances", [])
raise SystemExit(not (
    status.get("state") == "running"
    and status.get("appliedDocumentEtag") == status.get("documentEtag")
    and len(instances) == 2
    and all(instance.get("state") == "running" for instance in instances)
))
PYTHON
  then
    builtin_ready=1
    break
  fi
  sleep 1
done
if [[ "$builtin_ready" != 1 ]]; then
  printf 'Built-in Pipeline did not become ready. See %s/runner.log and %s\n' \
    "$demo_root" "$builtin_status_file" >&2
  exit 1
fi

for _ in $(seq 1 30); do
  if [[ -f "$demo_root/output.txt" ]] && grep -Fq 'Hello Tenon' "$demo_root/output.txt"; then
    break
  fi
  sleep 1
done
if [[ ! -f "$demo_root/output.txt" ]] || ! grep -Fq 'Hello Tenon' "$demo_root/output.txt"; then
  printf 'The example output did not appear. See %s/runner.log\n' "$demo_root" >&2
  exit 1
fi

printf 'Checking output from the built-in Dummy Source → Lua → Stdout Sink Flow...\n'
curl --silent --show-error --fail -N \
  -H 'Accept: text/event-stream' \
  "$api/pipelines/builtins/diagnostics?target=plugin%3Aconsole" \
  > "$demo_root/stdout-events.txt" 2>/dev/null &
diagnostics_pid=$!
for _ in $(seq 1 15); do
  if grep -Fq 'quickstart' "$demo_root/stdout-events.txt"; then
    break
  fi
  sleep 1
done
if ! grep -Fq 'quickstart' "$demo_root/stdout-events.txt"; then
  printf 'Built-in Stdout Sink output did not appear. See %s/stdout-events.txt\n' \
    "$demo_root" >&2
  exit 1
fi

printf '\nSuccess: %s/output.txt contains:\n' "$demo_root"
cat "$demo_root/output.txt"
printf '\nBuilt-in plugin diagnostic output:\n'
cat "$demo_root/stdout-events.txt"
printf '\nRunner log: %s/runner.log\nPress Ctrl-C to stop Tenon.\n\n' "$demo_root"
tail -f "$demo_root/runner.log"
