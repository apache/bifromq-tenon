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

# Tenon Documents

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../DISCLAIMER).

A Tenon Document is UTF-8 JSONC describing one Pipeline. Set `"specVersion": "1"`. Unknown fields and incompatible versions are rejected. The complete field structure, defaults and bounds are defined by the [Document Schema](../contracts/tenon-document/v1.schema.json), also available at `/document-schema` on the Runner.

The root contains `specVersion`, `id`, `pluginInstances`, `flows`, and optional `resourceLimits`. An Instance names an installed `programName` and `exactVersion`, contains its program-level `config`, and may set environment variables and extra launch arguments as defined by the Schema. Configuration is ordinary JSON: no implicit secrets, templates or environment interpolation are applied.

A Flow has one `source` instance, a `process.script` containing Lua source, and a nonempty `sinks` list. Source and Sink bindings must implement those interfaces. A dual-interface instance may be used only as Source, only as Sink, or as both, including both sides of the same Flow. Its active interfaces are determined by the Document's Flow bindings. An unbound interface has no queues or SDK workers. Each Source interface may be bound by only one Flow; a Sink may be shared by multiple Flows. No instance may remain completely unreferenced. Document, Instance and Flow ids are 1–128 UTF-8 bytes without C0/C1 control characters; object keys supply Instance/Flow ids, without another id field in their values. See the [quickstart](quickstart.md) for a complete executable example.

`extraArgs.args` is an argv array passed without shell interpretation or string splitting. Its optional `position` defaults to `append`: manifest command arguments, extra arguments, then Tenon startup arguments. `prepend` inserts extra arguments immediately after the executable. Each process first inherits the Pipeline environment, then applies `env` overrides. Omitted and empty overrides are equivalent; changing effective launch settings restarts the plugin process.

## Channels and limits

| Flow field | Omitted value | Meaning |
| --- | --- | --- |
| `parallelism` | One channel | When explicitly supplied, a ratio of allowed CPUs; channel count is `ceil(allowedCpuCount × parallelism)` |
| `maxPendingRecords` | `100` | Maximum incomplete Source submissions per channel |
| `maxRecordBytes` | `262144` | Maximum complete encoded record size, IngressRecord/EgressRecord, excluding the Queue frame header and padding |
| `delivery` | `at-least-once` | Source completion policy |

Omitted `parallelism` and explicit `1` are different: the latter uses all allowed logical CPUs. The count is independent of a Document's CPU quota. Each channel processes events in order with its own Lua state. Different channels can run concurrently and do not share Lua globals, timers or ordering.

Records exceeding `maxRecordBytes` are rejected. When pending records reach the configured limit, further submissions wait for capacity. Higher limits require more memory.

Optional `resourceLimits` requests Pipeline-group CPU and memory limits. See [deployment behavior](runner.md#cpu-and-memory) for Linux enforcement and macOS reporting. They do not impose plugin-specific or Flow-specific budgets.

## Validation, saving and application

Saving a Document checks its format, field values, references and Lua syntax. A rejected PUT identifies the invalid fields. Saving does not run Lua initialization or require the referenced plugins to be installed.

To run, a Document also needs the exact plugin versions installed, compatible platforms and payloads, valid plugin settings and successful Lua initialization. Otherwise its status is `unready`, with details in `runtimeIssues`. An existing Pipeline may continue using its previous configuration until the new one is ready.

`documentEtag` identifies the latest saved raw bytes. `appliedDocumentEtag` identifies the configuration used by the live Pipeline. Matching ETags mean the saved configuration has been applied. Check plugin status and your downstream system separately for connectivity and delivery. See [HTTP status](http-api.md) for state fields.

## Completion and delivery

`at-most-once` and `at-least-once` describe how Tenon completes Source records; external acknowledgement, replay and durable delivery still depend on the plugin. Queues are volatile and there is no end-to-end exactly-once guarantee.

With `at-most-once`, Source receives `OK` when the Pipeline has read the complete input record, before Lua processing or Sink delivery. Later failures do not change that result.

With `at-least-once`, Source records remain pending in input order until an `emit` completes them. The first successful `emit` in a `main` call takes all records pending at that point. A payload emit sends to every matching Sink instance declared by that Flow and waits for every target Sink to report completion before completing that group. A zero-argument `emit()` produces no Sink record and completes the group without waiting for a Sink. Returning without any accepted emit leaves records pending for a later completion boundary.

Later emits in the same call produce additional outputs without changing that Source group's completion. Multiple emits are not a transaction; a later error cannot roll back an earlier successful boundary. Each Sink plugin defines when it reports successful delivery. Source `RETRY` requires the Source implementation to decide replay; it does not create a durable Runner retry log. Plugin authors should handle completion results as described by their [language SDK](plugins.md#sdks-and-generators).

## Updates and failure

Updates apply asynchronously. If several are saved while an update is running, the latest pending version is used next. A new PUT does not extend the current update deadline. If that deadline expires, the Pipeline restarts with the latest valid saved configuration.

A script-only update lets the affected channels finish their current event and accepted outputs, then starts the new script with fresh Lua state and timers. Inputs still waiting for an emit boundary receive `RETRY`. Unchanged Flows continue. If the new script cannot initialize, the old configuration stays active. A failure while switching to the new configuration can restart the Pipeline.

Changing effective channel count, record limits or relevant bindings can restart plugin instances and reset Lua state. Changing only a parallelism ratio without changing effective channel count preserves the corresponding VM state. Script errors reset the affected VM; Pipeline/Runner restart loses all in-memory VM and queue state. Forced shutdown and reconfiguration timeout can lose or duplicate in-flight records. Upstream durability and downstream deduplication must cover those boundaries.
