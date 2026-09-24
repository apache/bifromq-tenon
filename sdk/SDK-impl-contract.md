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

# Developing a Plugin SDK and scaffold

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../DISCLAIMER).

This reference is for developers adding a language SDK and project scaffold to Tenon, or maintaining those tools. It defines the behavior required for compatibility with the Runner and other SDKs. For developing a concrete plugin with existing tools, use the [Rust SDK](rust/plugin-sdk/README.md), [Rust scaffold](rust/rust-plugin-scaffold/README.md) or [Java SDK and archetype](java/README.md).

A **Program** is an installed plugin package; an **Instance** is one configured process of that Program. The Runner supervises Pipelines. Each Pipeline contains Flows from one Source through Lua processing to Sinks; each Flow has independently ordered channels.

An SDK provides the plugin author API for startup, Source submission, Sink delivery and shutdown. It handles transport, memory mappings and lifecycle control. Plugin code supplies payloads, manages external clients, and decides when to acknowledge upstream or downstream operations. A combined Program exposes both capabilities through one business object.

Implement the [IPC protocol](../ipc/README.md) together with the startup, record, completion and lifecycle requirements below. Use the linked machine-readable definitions and test vectors to verify compatibility.

## 1. Common consistency rules

Every SDK must implement the same language-neutral facts: startup material; Plugin Control Protobuf messages and lifecycle; Queue v1 file, frame, append, wait, commit, and release semantics; Source ingress records, completions, acknowledgement codes, and admission credit; Sink egress records, batches, and shared release; combined-owner lifecycle; and failure, recovery, and process-exit boundaries.

Each fact has one machine-readable source. SDKs must consume shared protocol definitions, field registries, schemas, and test vectors. They must not maintain independently editable copies.

Use the shared [Queue vectors](../contracts/ipc/queue_v1_test_vectors.json) and [Bell vectors](../contracts/ipc/bell_v1_test_vectors.json). Follow the IPC binding and wait ordering, including condition rechecks and notification after publication. Verify native wait/wake behavior across processes as well as matching file bytes and error codes.

The machine-readable sources are:

| Contract | Shared files |
| --- | --- |
| Program manifest and payload declarations | [manifest Schema](../contracts/plugin/manifest.schema.json), [manifest vectors](../contracts/plugin/manifest.test-vectors.json), [payload contract vectors](../contracts/plugin/payload_contract_test_vectors.json) |
| Launch arguments and stdin | [process protocol vectors](../contracts/plugin/process_protocol_test_vectors.json) |
| Lifecycle messages, field ownership, and phase validation | [control Protobuf](../contracts/plugin/process_control.proto), [field registry](../contracts/plugin/process_control_field_registry.json), [control vectors](../contracts/plugin/process_control_test_vectors.json) |
| Source record and Completion encoding | [Ingress Protobuf](../contracts/source/ingress_record.proto), [record vectors](../contracts/source/ingress_record_test_vectors.json), [Source payload vectors](../contracts/source/payload_contract_test_vectors.json) |
| Sink record and payload decoding | [Egress Protobuf](../contracts/sink/egress_record.proto), [record vectors](../contracts/sink/egress_record_test_vectors.json), [payload vectors](../contracts/sink/payload_contract_test_vectors.json), [decode vectors](../contracts/sink/payload_decode_test_vectors.json) |

Factory discovery, exception versus `Result`, `Future` versus `CompletionStage`, lifecycle naming, ownership syntax, and SDK-internal threads or runtimes may differ by language. They must not change success boundaries, submission order, backpressure timing, completion results, shutdown order, failure visibility, or retry responsibility.

### 1.1 Packaging tools and generated projects

An SDK distribution's packaging tool must produce a complete, installable Plugin Program archive, and its scaffold must generate a project that works with that tool. A successful build or bundle must validate the generated manifest against the canonical [manifest Schema](../contracts/plugin/manifest.schema.json), plus the Config Schema and payload descriptor against their shared contracts. The bundle root contains `manifest.json`, `config.schema.json`, and `payload.descriptor.pb`, together with its executable, launcher, libraries, resources, and retained license material. Follow the [package contract](../guide/plugins.md) for archive layout, platform declarations, and immutability. A packaging tool must not report success or publish a new archive after validation fails.

The author must explicitly supply `displayName` and `description` in the project's packaging configuration. The display name is single-line plain text of 1-80 Unicode code points; the description is plain text of 1-1024 code points and permits LF/CR line breaks. Both require non-whitespace content. The shared Schema owns exact whitespace and control-character rules. Count Unicode code points, not UTF-8 bytes or UTF-16 code units. Preserve values without trimming, normalization, truncation, or a fallback derived from the Program identifier, package name, or configuration-schema title. Generated projects must include editable valid values for both fields.

Rust's `cargo-tenon` reads `display-name` and `description` from `[package.metadata.tenon]`; Java's Maven plugin accepts `displayName` and `description` parameters through `tenon.displayName` and `tenon.description`. Both write the same camelCase manifest fields. A packaged tool must carry the canonical Schema from its build, not load a Runner's private implementation or require the author's source checkout at execution time. Schema resources must be copied or linked from the canonical contract during the build; do not maintain a separate hand-edited validator or Schema copy for display metadata.

The metadata describes one immutable Program version. It does not change Program identity, interface capability, launch arguments, Instance configuration, or payload definitions. Platform variants of the same version must contain identical display metadata. Rebuilding an installed identity with changed metadata remains a content conflict. This contract requires both fields; no legacy manifest defaults or aliases are defined.

Packaging verification must exercise the shared positive and negative manifest vectors through the actual validator used by the tool, including missing, empty, whitespace-only, oversized, multilingual, supplementary Unicode, and control-character inputs. Verify that generated projects for all three interfaces build installable packages, that the archive preserves the configured strings, and that installing and recovering those packages exposes the same metadata through Runner list and single-Program queries. Cross-platform bundle comparisons must include both display fields.

## 2. Startup and control connection

### 2.1 Launch input

Pipeline executes the manifest command without a shell and appends exactly one reserved argument pair, `--sdk-config <compact-json>`, after Program arguments and configured extra arguments. The manifest must not supply that reserved option itself. The compact JSON object has these fields:

| Field | Requirement |
| --- | --- |
| `workingDirectory` | Absolute path to this Instance's Pipeline-owned directory |
| `controlSocket` | Absolute path to the private Unix-domain socket for lifecycle gRPC |
| `launchId` | Canonical padded Base64 encoding of exactly 16 bytes; preserve those bytes in `Attach` |
| `sourceChannelRegion` | Absolute Channel Bell Region path, present when this Instance is bound as a Source |
| `sinkInputs` | Array present when this Instance is bound as a Sink; each element contains original `flowId`, nonnegative integer `channelId`, and absolute `channelBellPath` |

An unbound direction omits its directional field. Unknown fields, missing required fields, and invalid types are startup errors. Pipeline supplies Sink inputs ordered by `flowId`, then `channelId`; these are exact identities, not paths or a channel-count estimate.

Standard input contains exactly one UTF-8 JSON configuration value followed by LF, then remains open as the parent lifetime channel. There is no second startup line. Reject malformed UTF-8, CR framing, duplicate object keys, JSON extensions, extra JSON values, and EOF before LF; preserve arbitrary-precision numbers. Configuration may be any valid JSON value admitted by the Program's Schema. The lifetime watcher takes ownership of stdin only after this line is parsed. Unexpected later EOF or read failure means owner loss.

### 2.2 Queue paths and Bell ownership

Pipeline creates and validates these files before the SDK opens them:

```text
<workingDirectory>/
  source/
    loops.bells
    submission-0.queue
    completion-0.queue
    ...
  sink/
    loops.bells
    <flow-directory>/
      egress-0.queue
      ...
```

Only directories for bound directions exist. The Source SDK discovers matching, contiguous Submission/Completion pairs starting at channel zero; their count is the Source's channel count. All pairs must express the same capacity policy. `flow-directory` is URL-safe Base64 without padding of SHA-256 over the original `flowId` UTF-8 bytes. Never interpolate a raw Flow id into a file path. A Sink opens only the `egress-<channelId>.queue` files identified by `sinkInputs`; it does not scan for additional inputs.

A Channel's Bell Region is supplied by `sourceChannelRegion` or `channelBellPath`; the Channel's slot ordinal is its zero-based channel id. The SDK does not derive those Region paths from private Pipeline internals. Its own `source/loops.bells` has two slots: `0` for the one Submission loop and `1` for the one Completion loop. Its own `sink/loops.bells` has one slot, `0`, for the one Sink coordinator, regardless of Queue count.

| Queue | Writer publishes `writerBellSlot` | Reader publishes `readerBellSlot` |
| --- | --- | --- |
| Source Submission | SDK Submission loop, slot `0` in `source/loops.bells` | Channel slot in the supplied Flow Region |
| Source Completion | Channel slot in the supplied Flow Region | SDK Completion loop, slot `1` in `source/loops.bells` |
| Sink Egress | Channel slot in that input's supplied Flow Region | SDK Sink coordinator, slot `0` in `sink/loops.bells` |

Each endpoint binds its own slot through the IPC protocol; peers read the published ordinal to notify it. Many Queues may subscribe to the same loop. That loop must recheck every subscribed Queue and its local events before sleeping. Files, permissions, endpoint replacement, and final deletion remain Pipeline responsibilities; the SDK never creates or repairs them.

### 2.3 Control connection and phases

Invalid startup material fails before business Queues open or business entry points start. Partial parsing is never passed to the business factory.

The startup order is fixed: parse all launch arguments and the config line; start watching the stdin lifetime; connect the lifecycle stream and send `Attach`; open every Queue and Bell Region required by the bound directions; invoke the business factory once and complete local `start`; then send `Ready` when the author enters `awaitShutdown`/`await_shutdown`. The lifetime watcher must not consume startup bytes, and readiness must not precede complete local initialization.

Each Instance has one exclusive bidirectional lifecycle stream. The first message is `Attach`, followed by `Ready`; Instances with a bound Source direction may then receive `QuiesceSource` and reply `SourceQuiesced`; Pipeline finally sends `Shutdown`, after which the process exits. Unknown, repeated, out-of-order, incomplete, and interface-inapplicable messages are rejected. No request ids, heartbeats, status polling, capability negotiation, generic envelope, or `ShutdownComplete` are added.

`Ready` proves local setup only. It does not prove external health, data processing, or an applied Pipeline revision. Unexpected stream EOF, error, or half-close means owner loss: exit nonzero immediately, without reconnecting or graceful business cleanup. Standard output and error are diagnostics only, from their first byte; never emit `READY\n` or other control frames there. Pipeline drains both streams throughout the process lifetime, even without diagnostic subscribers. Emit UTF-8 text with LF-terminated lines. The diagnostic view retains at most 16,384 UTF-8 bytes per line, replaces malformed UTF-8 with replacement characters, and may truncate or drop records. Delivery is best effort, without historical replay; diagnostics cannot establish readiness, record success, or acknowledgement. Diagnostic collection must not block lifecycle control or business data flow.

## 3. Source capability

An active Source interface is attached to exactly one Flow. The SDK's `parallelism` argument is the positive count of independently ordered channels, discovered from the prepared Queue pairs. It is not the Document's `parallelism` ratio: Runner turns that optional ratio into a channel count using its frozen allowed CPU count; an omitted ratio selects one channel. SDKs must not recompute it from host CPUs or CPU quotas. Each channel has independent Queues, admission credit, and pending requests. One Submission loop and one Completion loop serve all Source channels; this does not require one SDK worker per channel or prescribe the shape of the business Source.
A combined Program is activated independently in each direction. Runner includes `sourceChannelRegion` only when the Instance is bound as a Flow Source and includes `sinkInputs` only when it is bound as a Flow Sink. The SDK must create only the sessions represented by those fields: a combined Program can run Source-only, Sink-only, or in both directions. Runner still rejects an Instance that is not referenced by any Flow. The combined factory therefore receives an optional `Ingress<S>` and the actual set of Sink `FlowChannel` identities. The author-facing `FlowChannel` contains only `flowId` and `channelId`; Bell Region paths remain SDK-internal launch material.

The Source factory is called exactly once while a Source direction is initialized. It receives the validated configuration, the positive Source channel count, and one `PayloadSender`, and returns the one business Source owned by the Program. The business Source may contain any number of upstream clients, workers, subscriptions, or other internal producers. The author does not create Source objects or handles after `run`; the SDK invokes the Source lifecycle automatically.

Every exposed business lifecycle callback has the same strict execution contract: Source and source-and-sink owners expose `start`, `quiesce`, and `close`; Sink owners expose `start` and `close`. All return no error and must not throw any `Throwable` or panic. They must return promptly without waiting for network acknowledgements, worker termination, or other external progress. They may signal business workers to start or stop; any subsequent worker failure still reaches the SDK's process-failure boundary. A callback that throws, including an unchecked exception or `Error`, is a process-fatal Plugin failure: the SDK writes the failure to stderr, flushes it, and terminates with exit code 1 immediately. A callback that blocks is also a contract violation; the Pipeline's bounded Plugin startup/lifecycle timeout is the authority that forcibly terminates the process. SDK-owned Queue worker joins are distinct from these business callbacks.

Once the factory returns, final close responsibility during normal shutdown belongs to the SDK. `run` invokes the business start exactly once and returns a Program; `awaitShutdown`/`await_shutdown` then publishes `Ready` exactly once and owns the blocking lifecycle wait. Both methods are process boundaries: they expose no checked exception or error result to the author. Any internal error, `Throwable`, or panic is reported to stderr, flushed, and terminates the process with exit code 1. Internal owners propagate failure and do not make their own process-exit decisions. Asynchronous failures reach this same boundary through the SDK-owned failure channel or uncaught-exception/panic handler. The business entry point never closes a capability manually. This same shape applies to Source-only, Sink-only, and combined Programs. Sink resources are started by the SDK during Program initialization; the author may run ordinary custom logic before calling `awaitShutdown`.

`send(channel, payload)` may be called concurrently. It validates the channel before side effects, obtains bounded credit before encoding, and uses successful preflight enqueue as the admission and submission linearization point. It encodes synchronously and returns `ERROR` without commit for encoding or complete-record size failure. Credit remains held until pre-commit failure or terminal Completion. `OK` means the configured delivery boundary was reached. `RETRY` means no completion boundary was reached and the business may retry. `BACKPRESSURE` means the open session had no local admission credit. `ERROR` means this record cannot complete normally. A call after Source admission closes, and an admitted call that remains unresolved when the Source Queue session closes, complete with the language's distinct Source-session-closed error; this is not `BACKPRESSURE` or `ERROR`. The SDK never resends and never treats Submission release as Completion. Completion can be awaited by a business thread or a business-selected async executor. Dropping it does not cancel an admitted send or return credit early. External acknowledgements remain business responsibility.

After `QuiesceSource`, the SDK first closes new admission. The business Source then stops new events and sends; entered synchronous sends finish handoff or fail; Completion workers and combined resources remain alive; and no later Submission commit is possible. Only then is `SourceQuiesced` sent. Quiesce does not await asynchronous Completion or close the business Source. After `Shutdown`, Source Queue workers stop and join, already-read completions finish normally, and remaining requests receive the session-closed result. Bound Sink directions then stop and join the Instance's single Sink coordinator. Only after their Queue sessions close successfully does the SDK invoke the one business owner's `close`. A combined Program closes its shared owner exactly once.

### 3.1 Record encoding and admission credit

Submission carries `IngressRecord`: `uint64 record_id` at Protobuf field 1 and `bytes payload` at field 2. Payload bytes encode the exact Program's declared `SourceRecordPayload`. The SDK assigns each record id from the Submission Queue's current logical commit position plus one, with checked arithmetic. Only the Submission loop assigns ids and writes frames. Reopening a retained Queue continues its position sequence and must not reuse ids from that Queue generation.

Completion carries `IngressCompletion`: `uint64 record_id` at field 1 and status at field 2. Wire status values are `OK = 1`, `RETRY = 2`, `BACKPRESSURE = 3`, and `ERROR = 4`; zero/unspecified and unknown values are invalid. These messages are defined by the shared Protobuf, not by per-language copies. Completion transports neither exceptions nor credentials. The SDK matches ids only within the current channel's pending map; unknown or late ids are released and ignored, never attached to a new request.

`maxPendingRecords` bounds the unfinished sends in one channel's current Source session; its Document default is 100. `maxRecordBytes` bounds a complete encoded outer record, not just its business payload; its default is 262,144 bytes and minimum is 1,024 bytes. Recover these values from the prepared Queue header and actual file length instead of introducing startup options or SDK-specific limits:

```text
maximumSubmissionFrameSize = align8(8 + submission.maxPayloadSize)
maxPendingRecords = submission.dataCapacity / maximumSubmissionFrameSize - 1
completion.dataCapacity = (maxPendingRecords + 1) * 24
```

Division must be exact and the derived pending limit positive. Submission's `maxPayloadSize` is `maxRecordBytes`; Completion's must be 13 bytes, the maximum encoded Completion body. Completion's aligned maximum frame is 24 bytes. Each Sink Egress Queue uses `maxRecordBytes` and `(maxPendingRecords + 1) * maximumSubmissionFrameSize` bytes, but does not participate in Source permit accounting. All arithmetic and representation checks follow IPC v1. The extra maximum frame reserves wrap space; it is not an additional admission permit.

A nonblocking permit acquisition decides whether an open session has capacity. A send holds its one permit throughout synchronous encoding, preflight enqueue, Submission, Pipeline processing, and Completion. Successful preflight enqueue determines submission order. Register the pending id before publishing Submission commit so a fast Completion cannot outrun registration. A `Full` result retains the current request and lets the Submission loop serve other channels; after a pass with no progress, arm its one Bell and recheck all requests, space, and stop facts. Retained bytes from an earlier session may temporarily fill a Queue despite fresh session permits; this wait does not issue another `BACKPRESSURE` result.

The Completion loop validates an owned record, publishes that frame's shared release, removes a matching pending request, returns its permit, and only then completes the business-visible result. This lets a completion callback send again using the returned capacity. Non-async callbacks run on that loop in each channel's observed completion order; business code chooses an async callback/executor when needed. Closing or dropping a future does not cancel the admitted cross-process request.

The total unfinished sends in a channel, across encoding, submission, processing and completion, must never exceed `maxPendingRecords`. Return credit only on pre-submission failure or terminal Completion. A full Completion Queue stops new input consumption for that channel until space is released; do not overwrite results or add an unbounded overflow buffer.

### 3.2 What a Source acknowledgement proves

With `at-most-once`, Pipeline produces `OK` after copying the complete IngressRecord into owned memory. Later Lua or Sink failures do not change it.

With `at-least-once`, input records remain pending until the first accepted `emit` boundary in a Lua invocation takes the previously pending records and that invocation's Source input. A payload emission freezes its actual target Sinks, waits until every target Queue can accept the complete frame, then commits to them. `OK` is produced only after every target's shared release crosses that emission's committed frame boundary. Pipeline can continue processing while release is pending; successful boundaries settle in order, with record order preserved within each boundary. Error and retry results are handled when their failure occurs and do not use that successful-boundary list.

A no-argument `emit()` establishes a boundary without Sink output or waiting for Sink release; Completion capacity can still block it. Further emissions in the same invocation create outputs but do not take the Source records already assigned to the first boundary. A timer invocation can establish a boundary for previously pending inputs. Successful boundaries are never rolled back.

Before any new boundary, a Source payload decode or Lua failure yields `ERROR` for the current Source record and `RETRY` for older pending inputs. A failed decode without older pending input can retain the VM; older pending input or a Lua invocation failure requires VM replacement. A timer failure retries all pending input. An oversized Egress record is rejected before its emission is accepted. These failures must not fabricate a successful Sink release or revise an already established `at-most-once` result.

The Sink defines the downstream acknowledgement or durability represented by successful `write`. Tenon provides no cross-Sink transaction, exactly-once guarantee, or durable replay promise after Pipeline or machine failure. A crash between target commits, external effects, or release publication can produce duplicates on retry. Source business code owns upstream acknowledgement and retry policy.

### 3.3 Healthy session handoff and failed Source recovery

For a healthy Source replacement, `SourceQuiesced` stops new Submission commits while its Completion reader stays alive. Existing Channels finish the old session's admitted responsibility using the old processing and routing; input not assigned to an accepted at-least-once boundary receives `RETRY`. Pipeline waits until the old SDK has released the last corresponding Completion before closing that process. This uses the existing write receipt and shared release, not a second acknowledgement protocol. The SDK must finish already-read results and join its Queue workers before final close. This does not prove completion of business-selected asynchronous callbacks or external acknowledgements.

Source process failure is different: the old pending map and upstream session are gone. Pipeline first reaps the old process and stops its Flow's Channels, then rebuilds Submission/Completion files and Channel state. It retains already committed Egress and never advances Sink release on their behalf. New Source admission begins only with the replacement input Queues. If a Source failure appears during configuration preparation or handoff, terminate the entire Pipeline; do not continue the handoff or pretend the old session consumed its results. A lifecycle deadline may force process termination; no path fabricates `OK`, shared release, or an old session's successful result for a new session.

## 4. Sink capability

Each `(sinkInstanceId, flowId, channelId)` has one independent Egress Queue. The SDK opens only Pipeline-created files and never creates, changes permissions, or repairs them. Each frame is copied once into owned memory before decoding and batching; business code never retains a mapping slice.

One Queue invokes non-empty, immutable batches for one FlowChannel in read order, serializing callbacks for that Queue. The instance's single Egress loop serves every Queue in turn, so batches from different Queues are in flight together, their method bodies never overlap, and returned async completions may finish out of order. That loop is the only waiter on the Sink side, so the instance's `sink/loops.bells` Region holds exactly the one doorbell it parks on: every Egress Queue reader publishes the same slot ordinal `0`, and the Pipeline reserves no further slot per Egress channel. Shared release advances only after business completion succeeds. Decode failure does not release the frame and fails the Plugin. The SDK does not provide single-record writes, a second batch API, generic context, or business retry, rate-limit, idempotency, or success policy.

A combined Program has one shared business owner. Its Sink coordinator, Egress readers, and shared connections remain alive during Source quiesce.

### 4.1 Ordered asynchronous batch completion

Egress carries `EgressRecord`, with `bytes payload` at Protobuf field 1. The payload encodes the exact Sink Program's declared `SinkRecordPayload`; the Queue identity already supplies the Flow and Channel. There is no Egress record id, status, or reverse Completion Queue.

Each Queue maintains a local read position and shared release. The former includes records copied and passed to `write`; the latter includes only the contiguous prefix of successfully completed batches. Several batches may be in flight and their asynchronous results may complete out of order. A later successful batch cannot release space past an earlier unresolved or failed batch. Reading ahead is bounded by the Queue's own data capacity; there is no unbounded extra buffer.

Use one Sink coordinator to access Queue readers and advance release. Async completions must wake it without accessing readers concurrently. After waking, process completed results, release successful prefixes and check every input before waiting again. Shutdown must also wake a waiting coordinator.

A successful result must mean the whole batch reached the Sink's configured success boundary, not merely that it entered a volatile internal buffer. A failed result, synchronous exception, invalid/null result, or decode error terminates the Instance without releasing that batch or the records after it. Other Queues retain only releases actually published for them. A result that never completes stops release and naturally applies backpressure. Restart replays from the retained shared release; partial external effects and out-of-order successful batches may therefore be duplicated. The SDK does not retry a failed batch inside the process.

Fan-out can fail between target commits, so external effects may be partial even when no Source `OK` was received. Preserve committed but unreleased records on Sink restart and never permit concurrent writers for a retained Queue.

## 5. Failure, cancellation, and ownership

An unrecoverable Source worker, Sink reader, control stream, or parent lifetime failure becomes visible and terminates the Instance at the common process boundary. Other channels must not remain permanently blocked, and a combined interface is not recovered only halfway.

An OS wake, copy, or close error after commit or release does not prove that the operation did not happen. If a blocked worker cannot be proven to terminate, the process is the recovery boundary and its mapping must not be freed.

Factory, Queue, protocol, and `write` failures are process-boundary failures; the SDK reports them to stderr and terminates without promising business cleanup. Lifecycle callback failures are never ordinary business errors: a thrown Java `Throwable`, Rust panic, or equivalent fatal runtime signal is reported to stderr and terminates the process immediately; later business callbacks are not invoked and cleanup is not promised.

Stop shutdown at its first failure and terminate the plugin. Do not invoke later cleanup callbacks after that failure. A Sink result observed before shutdown closes admission retains its success or failure meaning; abandon unresolved results and ignore later completions. Committed but unreleased records remain eligible for replay. Do not remove or replace Queue files until the old process has exited.

Cancellation never fabricates Completion, acknowledgement, or shared release. Normal shutdown drains and closes resources in the specified order only while each action succeeds; its first failure terminates the process. After the Pipeline or Runner deadline, the whole process group is reclaimed. The SDK does not add an independent product deadline.

Ownership is unique: Pipeline owns Queue files; the SDK owns adapters, workers, wake interruption, and session requests; the business owner owns external connections, threads, executors, and external acknowledgements.

## 6. Observable performance boundaries

Source unfinished requests are bounded by Queue capacity and admission credit. Sink Queues retain only their own batches and owned payloads. Queue access remains single-producer/single-consumer. Independent channels and Queues do not block through a global SDK lock. Business callbacks run without SDK locks or live mapping borrows. Claimed performance improvements require repeatable benchmarks.

## 7. Interoperability and release acceptance

Add a language in this order: implement `ipc/<language>`; implement lifecycle and Source/Sink adapters in `sdk/<language>/plugin-sdk`; provide that language's project scaffold; prove bundle creation, installation, and real Runner data flow. IPC format, runtime, crash, and bidirectional interoperability tests precede the SDK's Source, Sink, and combined-owner lifecycle proof. Official support requires applicable native acceptance on every declared target.

Rust Runner and SDK use the same [IPC crate](../ipc/rust/tenon-ipc/). The [Java internal IPC module](../ipc/java/tenon-ipc/) is embedded in the sole author SDK. Java's public artifacts remain `tenon-plugin-sdk`, `tenon-maven-plugin`, and `tenon-plugin-archetype`; Plugin authors do not select an IPC artifact or separate Source/Sink SDKs. Test fixtures must not enter the production SDK JAR or its published dependency POM.

Before claiming an interface, an SDK must use the shared contracts and vectors; keep protocol, Queue, and test-only exports out of the ordinary author API; test normal, backpressure, oversize, invalid-phase, Queue-failure, owner-loss, cancellation, and shutdown paths; interoperate with another language through real Queue and UDS/gRPC communication; exercise real processes for Attach, Ready, data exchange, Completion/shared release, quiesce, Shutdown, stream loss, stdin EOF, and force kill; and repeat applicable validation on every declared OS/CPU combination.

Document supported interfaces and tested platforms explicitly. When cross-language behavior changes, update this reference, machine contracts, test vectors, affected SDKs and interoperability tests together.

Shared acceptance tests for the Source author interface must cover factory invocation exactly once, SDK start timing, lifecycle violation process termination, quiesce and final close ordering, invalid channel handling, queue failure, owner loss, and combined Sink shutdown. Tests must also prove that a business Source may own multiple upstream clients or workers without exposing a second SDK Source object.

### 7.1 Contributor verification

Use the checked-in Rust toolchain, Cargo locks, Maven wrapper, and [Java toolchain manifest](java/.mvn/tenon-toolchain.properties). From the repository root:

```sh
# Java IPC, SDK, Maven Plugin, and generated Plugin projects.
tools/verify-java.sh
# Rust workspaces plus real Java/Rust Plugin and Runner interoperability.
tools/verify-rust.sh
```

Generate `source`, `sink` and `source-and-sink` projects that depend only on the public SDK. Build their bundles, install them through the Runner API and test startup, data exchange, acknowledgements and shutdown. Validate published artifacts from extracted Rust archives or an isolated Maven repository; exclude test controls and private build dependencies from the public SDK.

Self-contained Java bundles include the pinned Temurin runtime and required transport/dependencies and run without a system JVM. The public SDK remains an OS/CPU-independent JAR. Generated bundles declare their exact platform and preserve the same configuration Schema, normalized descriptor, and business JAR across targets. Descriptor normalization sorts the outer FileDescriptorSet files by name only; message, field, dependency, and source-info order within each file remains intact. Installation preserves uploaded bytes.

Run applicable checks on Linux amd64/arm64 and macOS amd64/arm64, with macOS 14.4 or newer; see the [native CI matrix](../.github/workflows/ci.yml). Cover both languages as Queue writer and reader, data and space wakeups, delayed and failed Sink completion, replay, Source quiesce with active Sink work, malformed lifecycle phases, stdin EOF, control-stream loss and forced termination. Record the tested revision and native target.
