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

# Java Plugin SDK and tooling

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../../DISCLAIMER).

Use `tenon-plugin-sdk` to write Source, Sink or combined plugins, `tenon-plugin-archetype` to generate a project, and `tenon-maven-plugin` to package it. The generated README describes the callbacks and files to customize. The [SDK implementation contract](../SDK-impl-contract.md) is for developers adding or maintaining a language SDK.

## Build from source

Use the checked-in Maven wrapper and the JDK specified by [.mvn/tenon-toolchain.properties](.mvn/tenon-toolchain.properties). The required versions are JDK 25.0.3 and Maven 3.9.16. From this directory:

```sh
./mvnw --batch-mode --no-transfer-progress clean install
```

The repository-level `tools/verify-java.sh` runs this with an isolated Maven repository under `target/verify-java-maven-repository`. When another project consumes artifacts from that verification, pass the same absolute `-Dmaven.repo.local` path so a stale artifact in the default cache cannot replace the tested SDK.

## Generate a plugin

After the build above completes, use Maven's archetype goal in the directory that will contain the new project. This command uses the default local Maven repository populated by the build above:

```sh
/path/to/tenon/sdk/java/mvnw org.apache.maven.plugins:maven-archetype-plugin:3.4.1:generate -B   -DarchetypeGroupId=org.apache.bifromq.tenon   -DarchetypeArtifactId=tenon-plugin-archetype -DarchetypeVersion=0.1.0   -DgroupId=com.example -DartifactId=hello-tenon -Dversion=1.0.0   -Dpackage=com.example.hello -Dinterface=source-and-sink   -DprogramName=com.example.hello -DtenonVersion=0.1.0
cd hello-tenon
./mvnw package
```

`interface` is `source`, `sink` or `source-and-sink`. Edit the generated factory, proto3 payload messages and `src/main/tenon/config.schema.json`. Use stdout and stderr for diagnostics. The generated README explains the selected interface and files.

## Plugin callbacks and results

The generated `Main` selects `SourceProgram`, `SinkProgram` or `SourceAndSinkProgram`, calls `run`, then `awaitShutdown`. Keep the generated factory registration under `META-INF/services`. Tenon launches the packaged program and provides its configuration.

| Interface | Methods to implement |
| --- | --- |
| `TenonSource` | `start()`, `quiesce()`, `close()` |
| `TenonSink<P>` | `start()`, `write(FlowChannel, List<P>)`, `close()` |
| `TenonSourceAndSink<P>` | All four methods on one shared object; `P` is the Sink payload type |

The SDK calls the factory and `start` once. Lifecycle methods must return promptly and must not throw. Use them to signal your clients or workers; do not wait for network acknowledgements or worker termination. Quiesce stops Source production while pending Source results and any Sink interface remain available. `close` is not guaranteed after a crash, fatal error or forced termination.

A Source factory receives the configuration, effective channel count and a `PayloadSender<P>`. Call `send(channelId, payload)` with a zero-based index below that count and observe the returned `CompletionStage<AckCode>`:

- `OK`: the Flow's [delivery boundary](../../guide/tenon-document.md#completion-and-delivery) was reached; apply your upstream acknowledgement policy.
- `RETRY`: the record did not complete; decide whether and how to replay it.
- `BACKPRESSURE`: the channel has no free pending slot; slow production and retry later.
- `ERROR`: handle a record failure, such as an oversized payload.

An invalid channel throws `IllegalArgumentException`. A closed session completes unfinished sends exceptionally with `SourceSessionClosedException`. The SDK does not resend automatically. Keep synchronous completion callbacks short; use an explicit executor for asynchronous or blocking work.

Sink `write` receives a nonempty, ordered, immutable batch and returns `CompletionStage<Void>`. Calls do not overlap, but returned stages can finish out of order. Return promptly and complete the stage successfully only when every record reaches your downstream delivery guarantee. A failed stage terminates the plugin; a restart can replay unacknowledged batches, including partial external effects. Plan for duplicates.

## Packaging

The generated POM configures `tenon-maven-plugin` to create a standard `.tar.gz` under `target/`. The normal package contains a native launcher and its own fixed Java runtime, so it does not need a system JVM on the Runner host. Build and test the bundle on each target platform you support.

The package contains `manifest.json`, `config.schema.json`, `payload.descriptor.pb`, program dependencies and retained license material. Descriptor generation is a required build output. Runtime distribution notices remain under `runtime/`; preserve them when redistributing. Install through the [normal Runner API](../../guide/plugins.md), not by writing its private state tree.

Set the required `tenon.displayName` and `tenon.description` properties in the generated POM before distributing the plugin. They become the manifest's `displayName` and `description`, which Console displays. Names allow 1-80 Unicode code points on one line; descriptions allow 1-1024 and may contain line breaks. Both require non-whitespace plain text. The bundler validates the complete manifest using the shared Schema and preserves these values without defaults or truncation. See the [packaging-tool contract](../SDK-impl-contract.md#11-packaging-tools-and-generated-projects).

## Verification

Run `./mvnw verify` in the generated project, then install its bundle in a Runner and test delivery with your external systems. Check acknowledgement, retry and shutdown behavior as well as successful startup. See [Contributing](../../CONTRIBUTING.md#verification) for repository checks.

## Project licensing

The archetype does not generate a LICENSE or NOTICE for your plugin and does not
ask for a copyright owner. Choose your project's license and redistribution terms
yourself. The bundler includes either project file when you provide it; it always
preserves the third-party runtime's legal files and dependency JARs. Review all
bundled components before distributing your plugin. Tenon's own Maven artifacts
carry their Apache LICENSE, NOTICE and incubation DISCLAIMER under META-INF.

## Combined Programs

A combined Program may be bound as Source only, Sink only, or both. The SDK creates only the sessions represented by the current Flow bindings. Its factory receives `Optional<Ingress<S>>` for the bound Source direction and the actual `Set<FlowChannel>` Sink inputs. `FlowChannel` contains only `flowId` and `channelId`.
