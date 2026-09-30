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

# IPC protocol for Plugin SDK developers

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../DISCLAIMER).

Use this reference when implementing IPC for a Tenon Plugin SDK in a new language or maintaining an existing SDK. It specifies Queue v1 and Bell Region v1 file formats, memory ordering, endpoint lifetime and failure behavior required for interoperability. The [SDK implementation contract](plugin-sdk-contract.md) covers startup, Source/Sink adapters and scaffolding. To develop a plugin with an existing SDK, start with the [plugin development guide](../guide/plugins.md#sdks-and-generators).

A Queue transports bounded, non-overwriting, single-producer/single-consumer (SPSC) framed bytes through a shared memory-mapped file. A Bell is a coalescing notification that asks one waiting loop to recheck its conditions. Queue positions and the caller's local state establish facts; notifications do not.

## Module responsibilities

| Implementation | Consumers | Distribution |
| --- | --- | --- |
| [Rust `tenon-ipc`](rust/ipc/) | Runner and Rust Plugin SDK | An independently reusable crate, versioned with the SDK. The SDK includes IPC as a transitive dependency. |
| [Java `tenon-ipc`](java/ipc/) | Java Plugin SDK | An independently reusable Maven artifact, versioned with the SDK. The SDK includes IPC as a transitive dependency. |

IPC owns file validation, frame planning, mappings, atomic positions, reader/writer operations, owned record bytes, and Bell wait/wake. It does not depend on Runner, Pipeline, SDK lifecycle, business Protobuf messages, Lua, or metrics.

The upper layer creates files and assigns exactly one live writer and one live reader per Queue. It also assigns one waiting loop to each Bell slot and owns paths, business encoding, completion policy, shutdown, and resource removal. A loop may own several Queue endpoints, and many peers may ring its slot. Neither the Queue nor the Bell file contains an ownership lock, reference count, or second liveness protocol.

The [SDK implementation contract](plugin-sdk-contract.md) defines how Source submission, Source completion, and Sink egress use these primitives. IPC itself does not interpret those records.

## Queue v1 layout

All integers are little-endian. Access fields by their specified offsets, never by a language struct's native padding. The fixed 192-byte header precedes the data region.

| Byte range | Encoding / owner | Meaning |
| --- | --- | --- |
| `0..8` | immutable bytes | Magic `TENONQ\0\0` |
| `8..12` | immutable u32 | `formatVersion = 1` |
| `12..16` | zero padding | Must be zero |
| `16..24` | immutable u64 | Nonzero `maxPayloadSize`, the largest permitted frame body in bytes |
| `24..64` | zero padding | Must be zero |
| `64..72` | atomic u64, writer | `commit`: exclusive end of the published byte prefix |
| `72..76` | atomic u32, published by reader | `readerBellSlot`: reader loop's ordinal in its Bell Region |
| `76..128` | zero padding | Must be zero |
| `128..136` | atomic u64, reader | `release`: exclusive end of the reusable byte prefix |
| `136..140` | atomic u32, published by writer | `writerBellSlot`: writer loop's ordinal in its Bell Region |
| `140..192` | zero padding | Must be zero |

Publish slot ordinals when endpoints bind. The corresponding wait words reside in Bell Regions.

The actual file length is the only capacity source:

```text
dataCapacity = fileLength - 192
maximumFrameSize = align8(8 + maxPayloadSize)
```

`align8` rounds up to a multiple of eight bytes with checked arithmetic. `dataCapacity` must be at least 16 bytes and divisible by eight. The complete file length must fit the supported platforms' signed 64-bit file-length APIs. `maximumFrameSize` must fit both the data region and a signed 32-bit Java array length. These representation rules imply a maximum body length of 2,147,483,632 bytes; IPC adds no separate business-size policy.

`commit` and `release` are aligned, monotonically increasing unsigned 64-bit logical byte positions. They include wrap gaps and never wrap back to zero:

```text
release <= commit
commit - release <= dataCapacity
```

New files start with `commit = release = 0` and both Bell ordinals set to the unbound sentinel `0xffffffff`. An exhausted logical position fails the current Queue; the resource owner must replace it through its recovery lifecycle. Opening a file never resets positions or repairs its contents.

## Frames, append, read, and release

A record consists of a four-byte unsigned body length, four zero bytes, the body, and enough zero padding to align the whole frame to eight bytes. An empty body is invalid: `bodyLength = 0` denotes an eight-byte wrap marker.

Check body length, frame representability, and position arithmetic before writing anything. Given the aligned `frameLength` in bytes, the append plan is:

```text
used = commit - release
free = dataCapacity - used
offset = commit % dataCapacity
tail = dataCapacity - offset
wrap = tail < frameLength ? tail : 0
required = wrap + frameLength
```

If `required > free`, return `Full` without changing data bytes, positions, or Bell words. Otherwise, if `wrap > 0`, write the wrap marker at the current offset and account for the entire remaining tail, then write the record at data offset zero. Write the complete body and zero padding before release-storing `commit + required`. Only then notify the reader.

Rust `QueueWriter::try_write_with(exactBodyLength, encoder, onCommitted)` supports synchronous encoding directly into the planned frame body. The encoder receives a temporary mutable slice cursor limited to that body and must consume it completely. Invalid length and `Full` do not invoke the encoder or mutate the Queue. The encoder is called at most once, is not retained, and must not retain the mapped borrow. An encoder error, panic, or successful return with bytes remaining does not publish commit; partially written free bytes remain invisible. The caller retains any record needed for a later `Full` retry. This is the only Rust append interface; callers with encoded bytes copy them into the supplied cursor. After publication, `onCommitted` runs once before notification, including when that notification subsequently fails. It is skipped on `Full` and encoding failure. Both callbacks must return promptly; the post-commit callback must not panic. Business completion responsibility remains with the upper layer.

The reader acquire-loads `commit`. Its local read position may lead shared `release`, but never `commit`. It inspects only the fixed frame header in the live mapping, proves that the complete frame is within the committed prefix, and copies that frame once into owned memory. Full frame validation and upper-layer decoding use the owned copy. A wrap marker skips the entire tail. `Empty` does not advance positions or modify bytes.

Only the reader advances shared `release`, and only over a contiguous prefix that its upper layer has finished. It release-stores the new position before notifying the writer. The writer acquire-loads `release` before planning reuse and must never overwrite the unreleased interval. These rules combine visibility of complete frames with exclusive ownership of free space.

Owned records remain valid after release or mapping closure. Rust `ReadPosition` is an opaque checkpoint of an already-read prefix; `release_through` releases that prefix after the upper layer completes it. A checkpoint belongs to the same open reader that produced it. Reopening starts at shared `release` and replays the unreleased prefix, even if an earlier reader had already copied those bytes.

A Rust `WriteReceipt` identifies the exclusive end of a committed append, including any wrap gap. It is bound to the same open writer that created it; using it with another writer or a reopened writer is rejected. `is_released` and `wait_released` read the existing shared `release`. A receipt is neither a business record identifier nor an acknowledgement, and it does not introduce another shared position.

## Bell Region v1 layout

All integers are little-endian. The region has a 64-byte header, followed by one 64-byte slot per waiting loop.

| Byte range | Encoding | Meaning |
| --- | --- | --- |
| `0..8` | bytes | Magic `TENONBEL` |
| `8..12` | u32 | `formatVersion = 1` |
| `12..16` | u32 | Nonzero `slotCount` |
| `16..24` | u64 | Diagnostic `epoch`; every bit pattern is valid |
| `24..64` | zero padding | Must be zero |
| `64 + i * 64 .. 68 + i * 64` | aligned atomic u32 | Slot `i`: `0 = armed`, `1 = notified` |
| remaining 60 bytes of each slot | zero padding | Must be zero |

The exact file length is `align4096(64 + slotCount * 64)`. The 4096-byte alignment is a wire-format rule, independent of the host's physical page size. Creation zeroes all padding and the final alignment area and initializes every slot to `1`. Opening validates the header, exact length, every slot value, and slot padding. The final alignment area carries no state.

Queue and Bell versions are validated independently. Unknown versions are rejected. Changing an existing field's meaning requires a new format version and an explicit upgrade plan; v1 files must not be silently reinterpreted.

The epoch is diagnostic only. It does not select an owner, validate a binding, prove success, control replay, or reset Queue positions. Pipeline increments it with saturation when replacing a Region; reaching the largest u64 does not prevent recovery.

## Binding and waiting without lost wakeups

An endpoint knows both its own loop's Bell Region and the peer's Region from its upper layer. A slot ordinal is meaningful only within that specific Region. Two processes may map the same file at different virtual addresses; native wait/wake must refer to the same shared backing word.

The required ordering is:

1. Open and validate the Queue and both Regions. Release-store the endpoint's own slot ordinal into the Queue header, then execute a sequentially consistent fence before the first arm and condition recheck.
2. A publisher first writes the actual fact, such as `commit` or `release`. Queue notification then executes a sequentially consistent fence before acquire-loading the peer's slot ordinal. If it still sees `0xffffffff`, skip notification: the binding side's subsequent recheck must see the fact. Any other out-of-range ordinal is an error.
3. The waiting loop checks its complete set of subscribed conditions. If none holds, atomically exchange its slot to `0` with acquire semantics, then recheck that same complete set. Only if no condition holds may it compare-and-wait on `0`.
4. Ringing release-exchanges the word to `1`. Wake the platform waiter only if the old value was `0`; an old value of `1` coalesces the notification. A wake or a changed word returns to the condition loop and does not itself establish readiness.

Use the same ordering when waiting for free space, with `release` and the writer's slot.

A local interrupt must either remain pending for the next wait or wake an already registered wait. Publish the local event or stopping condition before interrupting. Coordinate interruption with arming and rechecking so it cannot be lost; repeated interrupts may coalesce. Do not hold a registration lock during record copying or a blocking platform wait.

A local interrupt or OS `EINTR` returns `Interrupted`, allowing the owner to inspect lifecycle and local events. It does not mean data, free space, closure, or failure. Spurious wakes and value-changed results recheck conditions. The interrupter adds no shared Queue state and is not a Plugin business API. Periodic polling or a fixed timeout must not replace this protocol; an actual timer deadline is a separate subscribed condition.

Queue operations keep attempts separate from waits. Rust writers use `try_write_with -> Full -> wait_writable(exactBodyLength) -> retry`; readers use `try_read -> Empty -> wait_readable -> retry`. Waits do not copy, commit, or release records. A write wait must use the current record's exact size, including its possible wrap requirement. Waiting for a receipt uses the same writer slot as waiting for free space.

## Platform implementations

Supported target families are Linux amd64/arm64 and macOS amd64/arm64. macOS requires version 14.4 or newer. Linux uses shared futex wait/wake, never the process-private variants. macOS uses matching `SHARED` flags with `os_sync_wait_on_address` and `os_sync_wake_by_address_any`.

Use process-shared wait/wake operations on the same backing word in both processes. Preserve the native error from a failed operation; interruption and a changed value require a condition recheck rather than a fatal error.

For an additional platform, document the supported OS/CPU combinations and verify shared-address waits, interruption, changed values, notifications with no waiter and fatal errors using native cross-process tests.

## Errors, closure, and recovery

Stable format error codes are shared across languages. Exception types and human-readable messages are not the wire protocol. The following Queue codes identify rejected bytes or operations:

| Code suffix after `ipc.queue.` | Rejected condition |
| --- | --- |
| `header_too_short` | Fewer than 192 header bytes |
| `magic_invalid`, `format_version_unsupported` | Wrong magic or unsupported version |
| `capacity_invalid`, `max_payload_size_invalid` | Invalid capacity or unrepresentable/unsupported frame size |
| `header_padding_nonzero` | Nonzero reserved header bytes |
| `position_unaligned`, `release_ahead_of_commit`, `occupancy_exceeds_capacity` | Invalid shared positions |
| `position_exhausted` | Logical position cannot advance without overflow |
| `record_empty`, `record_too_large` | Invalid record body length |
| `frame_destination_too_small`, `frame_truncated` | Insufficient frame output space or incomplete frame bytes |
| `frame_header_padding_nonzero`, `frame_padding_nonzero` | Nonzero frame padding |

| Code suffix after `ipc.bell.` | Rejected condition |
| --- | --- |
| `region_too_short` | Fewer than 64 header bytes |
| `magic_invalid`, `format_version_unsupported` | Wrong magic or unsupported version |
| `slot_count_invalid` | Zero slot count or unrepresentable creation length |
| `region_length_invalid` | File length disagrees with slot count |
| `padding_nonzero` | Nonzero header or slot padding |
| `slot_state_invalid` | Slot word other than zero or one |
| `slot_out_of_range` | Binding or notification addresses an absent slot |

Opening failures must not repair files or establish partial endpoints. Runtime corruption and OS wait/wake failures stop the current use and propagate to the lifecycle owner. Preserve the native cause of I/O failures instead of relabeling them as format errors. Corrupt records must not be skipped.

Notification can fail after `commit` or `release` has already been published. An error therefore does not prove that the write or release did not happen; callers must not retry that same operation. If a blocked worker cannot be proven to stop, recover at the process boundary and do not unmap memory underneath it.

Normal closure publishes stopping facts, interrupts waits, joins the owners, and then releases mappings. Do not truncate or replace a Region while any waiter or notifier may still use it. Reopening a retained Queue requires the previous endpoint to be reaped before binding its replacement. Uncommitted bytes remain invisible after writer failure; committed but unreleased records remain replayable after reader failure. This is not a durable log or a promise of replay after Pipeline or machine failure.

Queue metrics are upper-layer observations. An observer may hold a weak mapping reference and take bounded atomic snapshots; it must not create another endpoint, read records, extend mapping lifetime, or invent a sample when concurrent progress prevents a consistent observation.

## Build and acceptance

Use the versions pinned by [`rust-toolchain.toml`](../rust-toolchain.toml), the Cargo lockfiles, and the [Java toolchain properties](java/.mvn/tenon-toolchain.properties). The Maven wrapper selects the pinned JDK and Maven. Run from the repository root:

```sh
cargo test -p tenon-ipc --all-targets --features repository-test-support --locked -- --test-threads=1
sdk/java/mvnw --batch-mode --file sdk/java/ipc/pom.xml verify
tools/verify-rust.sh
python3 tools/verify-ipc-fuzz.py
```

The first two commands test the language IPC modules. `tools/verify-rust.sh` also runs cross-language and Runner/SDK integration checks. See the [fuzzing guide](../fuzz/README.md) for its required toolchain. Rust fault-injection exports require `repository-test-support`; Java test fixtures use an internal test JAR. Keep both out of ordinary SDK distributions.

Consume the shared [Queue vectors](../contracts/ipc/test-fixtures/queue_v1_test_vectors.json) and [Bell vectors](../contracts/ipc/test-fixtures/bell_v1_test_vectors.json) directly. Packaging may materialize these files in an archive; do not maintain another editable copy.

| Required proof | Executable evidence |
| --- | --- |
| Exact layouts, append/wrap, corrupt input, and stable error codes | Shared vectors; Rust [format tests](rust/ipc/tests/format_contract.rs); Java [contract tests](java/ipc/src/test/java/org/apache/bifromq/tenon/sdk/ipc/) |
| FIFO, Full/Empty with no mutation, owned copies, prefix release, reopen, and interruption | Rust [runtime tests](rust/ipc/tests/runtime_contract.rs) and [reader batches](rust/ipc/tests/reader_batches.rs); Java runtime tests |
| Publish before arm, before sleep, or after sleep; local interruption, closure, and notification failure | Rust Bell concurrency models and real native wait/wake tests |
| Writer failure during header/body/padding/wrap/commit/notification; reader failure during copy/release | Real mappings and child-process `SIGKILL` [tests](rust/ipc/src/queue/mapped/tests/crash.rs) |
| Rust writer to Java reader and Java writer to Rust reader; lifecycle and recovery | [Runner Java integration tests](../tests/runner_java_archetype.rs), Java SDK lifecycle tests, and Rust SDK process tests |
| Arbitrary bytes and bounded operation sequences | [Fuzz targets](../fuzz/) and the shared Queue model |

Check packaged artifacts as well as source builds: generate the Rust IPC/SDK archives and build projects from their extracted contents; for Java, use an isolated Maven repository containing IPC, process-metrics, the Plugin SDK, Maven Plugin and Archetype. The [CI matrix](../.github/workflows/ci.yml) lists native OS/CPU targets. Run applicable tests on every supported target and record the revision and platform.

For performance changes, measure throughput, latency, CPU and memory at equivalent acknowledgement boundaries. Include independent-channel stalls; the benchmark is excluded from ordinary correctness runs.

## Adding a language

Start with `sdk/<language>/ipc` and prove this contract using the shared vectors, corruption and crash cases, real wait/wake, and bidirectional interoperability. Next implement lifecycle and Source/Sink adapters under `sdk/<language>/plugin-sdk` using the [SDK contract](plugin-sdk-contract.md). Then supply source, sink, and source-and-sink project scaffolds, bundle/install validation, and real Runner data-flow and failure tests. Declare official support only after the applicable acceptance passes on every claimed target.
