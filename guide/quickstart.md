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

This example builds from a source checkout and runs two independent Pipelines in one Runner. One Pipeline uses a generated source-and-sink plugin and writes a record to a file. The other uses the built-in Dummy Source and Stdout Sink, with a Lua timer producing output. This shows both the plugin scaffold path and prebuilt repository plugins; no broker or published Tenon package is needed. Run on a supported native platform with the [build prerequisites](../README.md#build-and-try), Python 3 and curl. The script uses local port 18080; set `TENON_PORT` to choose another port.

## Run the complete example

From the Tenon source root:

```sh
tools/example/local.sh
```

The script builds the Runner, generates and bundles the scaffold plugin, bundles the two built-in plugins, starts Tenon, installs all three Programs, saves two Documents, and checks both Pipelines. It installs `cargo-generate` 0.24.0 if the Cargo subcommand is missing. This provides the `cargo generate` command; installing the Rust toolchain alone does not install it. The script adds Cargo's binary directory to `PATH` so Cargo can find the generator.

It writes the generated project, the three bundles, `runner.jsonc`, and two input Documents in a temporary directory. `runner.jsonc` tells Tenon where to keep local state and to listen on `127.0.0.1`. `hello.jsonc` defines one Instance of the generated source-and-sink Program and one Flow that binds it as both Source and Sink; Lua copies the Source message into its Sink payload. `builtins.jsonc` defines a second Pipeline with one Flow from Dummy Source to Stdout Sink; Lua's timer emits a message every second. Both Flows omit `parallelism`, so each uses one channel. The script polls `/pipelines/hello` and `/pipelines/builtins`; the JSON files it saves from those GET responses are status snapshots, not the Documents submitted to Tenon. It checks `output.txt` for the scaffold Pipeline and subscribes to the built-in Pipeline's diagnostic stream to confirm stdout output.

When the example succeeds, the script prints both outputs and follows the Runner log. Press Ctrl-C to stop the Runner. It leaves its temporary directory in place and prints its path so you can inspect the generated project, Documents, status snapshots, bundles, logs and output. The Runner listens only on localhost. No Document cleanup is needed because stopping the temporary Runner ends this example.

If port 18080 is already in use, choose another port:

```sh
TENON_PORT=18081 tools/example/local.sh
```

## Manual steps

To run each stage yourself, first build the Runner and install the two Cargo tools:

```sh
cargo build --locked --bin tenon
cargo install cargo-generate --version 0.24.0 --locked
cargo install --path sdk/rust/cargo-tenon --locked
```

`cargo generate` is provided by `cargo-generate`. The install command adds that executable to Cargo's bin directory. Then follow the script's steps: generate the scaffold, bundle it against the local SDK, bundle the two built-in plugins, write `runner.jsonc`, `hello.jsonc` and `builtins.jsonc`, start the Runner, install the three Programs, and PUT both Documents. The script at [tools/example/local.sh](../tools/example/local.sh) contains the exact commands and example JSON.

Once the Runner is running, the essential API calls are:

```sh
curl --fail -i -X POST http://127.0.0.1:18080/plugins \
  -H 'Content-Type: application/vnd.apache.tenon.plugin+tar+gzip' \
  --data-binary @/absolute/path/to/hello-tenon.tar.gz
curl --fail -i -X PUT http://127.0.0.1:18080/documents/hello \
  -H 'Content-Type: application/jsonc' -H 'If-None-Match: *' \
  --data-binary @/absolute/path/to/hello.jsonc
curl --fail http://127.0.0.1:18080/pipelines/hello
cat /absolute/path/to/output.txt
```

Installation and creation return 201. Application is asynchronous: repeat the status read until `state` is `running`, `appliedDocumentEtag` equals `documentEtag`, and the plugin is running. The output file must contain `Hello Tenon`. Wait until the file appears and contains that line. Each fresh plugin process emits the example message, so a restart can append another line.

The Document PUT response confirms it was saved, not that the Pipeline has applied it. Poll `GET /pipelines/hello` until the state is `running`, `appliedDocumentEtag` matches `documentEtag`, and the plugin instance is running. Then check the output file. Replace the example absolute paths above with the paths where you generated those files.

To stop a manually started Runner, send it SIGTERM and wait for it to exit:

```sh
kill -TERM "$runner_pid"
wait "$runner_pid"
```

A deployed system should use its service manager and an appropriate persistent state directory instead.
