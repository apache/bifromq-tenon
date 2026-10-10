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

# Plugins and local debugging

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../DISCLAIMER).

These are ordinary SDK plugins maintained with Tenon. Build and install their bundles before referencing them in a Document; the Runner does not install them automatically.

| Plugin | Program identity | Purpose |
| --- | --- | --- |
| Dummy Source | `org.apache.bifromq.tenon.dummy-source@0.1.0` | Produce no records; let Lua timers drive a custom Sink. |
| Standard Output (Stdout Sink) | `org.apache.bifromq.tenon.stdout-sink@0.1.0` | Print Lua-authored messages while developing a custom Source. |
| [MQTT](bifromq-tenon-mqtt-plugin/README.md) | `org.apache.bifromq.tenon.mqtt@0.1.0` | Receive and publish MQTT messages. |

The display names and descriptions shown in Console come from `[package.metadata.tenon]` in each plugin's Cargo.toml. See the [display metadata requirements](../guide/plugins.md#display-metadata).

Both debugging plugins accept only an empty configuration object, `{}`. They have no network clients, business workers, files or configurable output formats. The SDK still owns their normal process and queue lifecycle.

## Build and install

From the Tenon repository root, with the [Rust prerequisites](../sdk/rust/rust-plugin-scaffold/README.md#prerequisites):

```sh
cargo install --path sdk/rust/cargo-tenon --locked
cargo tenon bundle --locked \
  --manifest-path plugin/bifromq-tenon-dummy-source-plugin/Cargo.toml > /tmp/tenon-dummy-bundle.json
cargo tenon bundle --locked \
  --manifest-path plugin/bifromq-tenon-stdout-sink-plugin/Cargo.toml > /tmp/tenon-stdout-bundle.json
```

These are native debug builds. Use `--release` for distribution and build a bundle for each intended platform. See [distribution guidance](../guide/plugins.md#build-artifacts-for-distribution).

Start a local Runner with the [quickstart script](../guide/quickstart.md#run-the-complete-example), or use your existing development Runner. The following examples assume `http://127.0.0.1:18080`; supply your deployment's authentication headers when required.

```sh
for report in /tmp/tenon-dummy-bundle.json /tmp/tenon-stdout-bundle.json; do
  bundle="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["bundle"])' "$report")"
  curl --fail -i -X POST http://127.0.0.1:18080/plugins \
    -H 'Content-Type: application/octet-stream' \
    --data-binary "@$bundle"
done
```

A new installation returns 201; installing identical bytes again returns 204.

## Self-check without an external service

Save this complete Document as `debug.jsonc`:

```json
{
  "specVersion": "1",
  "id": "debug",
  "pluginInstances": {
    "idle": {
      "programName": "org.apache.bifromq.tenon.dummy-source",
      "exactVersion": "0.1.0",
      "config": {}
    },
    "console": {
      "programName": "org.apache.bifromq.tenon.stdout-sink",
      "exactVersion": "0.1.0",
      "config": {}
    }
  },
  "flows": {
    "probe": {
      "source": "idle",
      "sinks": ["console"],
      "process": {
        "script": "local b = registry:getBuilder('org.apache.bifromq.tenon.stdout-sink@0.1.0')\nlocal count = 0\nsetTimeout(1000)\nfunction main(event)\n  if event.type == 'timer' then\n    count = count + 1\n    b:setMessage('probe ' .. count)\n    emit(b:build())\n    setTimeout(1000)\n  end\nend"
      }
    }
  }
}
```

Create the Document, then check that its current revision and both plugin instances are running:

```sh
curl --fail -i -X PUT http://127.0.0.1:18080/documents/debug \
  -H 'Content-Type: application/jsonc' -H 'If-None-Match: *' \
  --data-binary @debug.jsonc
curl --fail http://127.0.0.1:18080/pipelines/debug
curl --fail -N -H 'Accept: text/event-stream' \
  'http://127.0.0.1:18080/pipelines/debug/diagnostics?target=plugin%3Aconsole'
```

The SSE stream starts with `attached`. Subsequent `diagnostic` events have `stream: "stdout"` and text such as `2026-09-21T06:30:12.345Z probe 3`. The counter may already exceed one because diagnostics do not replay earlier output. Stop curl with Ctrl-C; this only closes the subscription, not the Pipeline.

## Develop a custom Sink

Use the Dummy Source as `flows.probe.source` and your installed custom Sink as its sink. Replace the `console` instance in the self-check Document with your Program identity and configuration. Replace the Builder identity and setters with those of your Sink contract.

For example, a custom Sink with `string message = 1` can use:

```lua
local b = registry:getBuilder("com.example.my-sink@0.1.0")
local count = 0
setTimeout(1000)
function main(event)
  if event.type == "timer" then
    count = count + 1
    b:setMessage("probe " .. count)
    emit(b:build())
    setTimeout(1000)
  end
end
```

Generate a real custom Sink project with the [standard Rust scaffold](../sdk/rust/rust-plugin-scaffold/README.md), selecting `interface=sink`. Its initial file-writing example requires `config.outputFile`, an absolute writable path. Inspect that file or the actual downstream system to verify delivery; a running process alone does not establish delivery.

The Dummy Source declares `message SourceRecordPayload {}` and never sends even an empty record. Do not wait for a Source event to start your timer. Timers are one-shot and each Channel has independent Lua state, so omit `parallelism` to use the default single Channel. An explicit `parallelism` is a multiplier of the Runner's allowed logical CPU count; `1` does not mean one Channel. Schedule the next timer explicitly, as above. Timing is approximate and subject to processing/backpressure; this is not a load generator. VM recreation or process restart resets the counter and starts the timer again. Timer-generated messages have no upstream Source record to recover after a crash.

## Develop a custom Source

Install your custom Source and the Stdout Sink. In the self-check Document, replace the `idle` instance's Program identity and configuration with your Source. Keep `console` and its Sink binding. Replace the Lua script with a transformation of your actual Source payload, for example:

```lua
local b = registry:getBuilder("org.apache.bifromq.tenon.stdout-sink@0.1.0")
function main(event)
  if event.type == "source" then
    b:setMessage("received: " .. event.payload.message)
    emit(b:build())
  end
end
```

This example assumes your Source declares `string message = 1`. Change the field access to match your contract; for several fields, construct an ordinary Lua table and pass it to `json.encode`. A generated `interface=source` scaffold accepts `config.message` and emits it once at startup. For that one-shot example, retain the message in Lua and emit it on a recurring timer while observing diagnostics, or observe its output with a file Sink.

Subscribe to `target=plugin%3Aconsole` **before triggering your external Source data**. To inspect the custom Source's own stdout/stderr, select its instance instead, such as `target=plugin%3Aidle`. See the [Lua API](../guide/lua.md) for payload access and Builder methods; method calls use `:`.

## Output and delivery boundaries

The Stdout Sink contract is `message SinkRecordPayload { string message = 1; }`. Empty strings are valid and print a timestamp followed by a space. Each record gets its current UTC wall-clock time with exactly three fractional digits. This is Sink output time, not Source event time; system clock adjustments can change its ordering.

Backslashes, carriage returns and line feeds in the message become `\\`, `\r` and `\n`, respectively. Thus each payload produces one LF-terminated line, and literal escape sequences remain distinguishable. Other UTF-8 text is preserved. Records in a batch are written in order; no ordering across different Channels is promised.

The Sink reports success only after the entire batch is written and stdout is flushed. A write or flush failure fails that batch through the SDK. Partial output followed by failure can be printed again on replay. Output is not durable and successful writing does not confirm that any diagnostic subscriber received it. Printing every record can become a throughput bottleneck; this plugin is intended for local development and debugging.

The [diagnostic API](../guide/observability.md#diagnostic-streams) is live and best effort. There is no history, replay or resume, and slow subscribers can lose records. Each text record is capped at 16,384 UTF-8 bytes, including the timestamp prefix; longer lines are marked truncated. The Sink writes the complete line and Tenon applies this display limit. Without a subscriber, Tenon still drains stdout and discards diagnostic text.

## Stop the debugging Pipeline

Use the Document's current ETag when deleting it:

```sh
etag="$(curl --fail -sS -D - -o /dev/null http://127.0.0.1:18080/documents/debug \
  | tr -d '\r' | sed -n 's/^[Ee][Tt][Aa][Gg]: //p')"
curl --fail -i -X DELETE http://127.0.0.1:18080/documents/debug \
  -H "If-Match: $etag"
```

The Runner stops the associated processes. Installed plugin packages remain available for the next debugging session.

## Verification

Run the focused Rust checks with:

```sh
cargo test --locked -p bifromq-tenon-dummy-source-plugin -p bifromq-tenon-stdout-sink-plugin
cargo clippy --locked -p bifromq-tenon-dummy-source-plugin -p bifromq-tenon-stdout-sink-plugin --all-targets -- -D warnings
```

`tools/verify-rust.sh` also verifies standard installation, no-input timer output, text boundaries through HTTP diagnostics, a generated custom Source and Sink, Runner restart and process cleanup in an isolated local environment.
