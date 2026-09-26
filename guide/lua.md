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

# Lua processing

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../DISCLAIMER).

A Flow embeds UTF-8 Lua 5.5 source in `process.script` and defines `function main(event)`. ASCII identifiers and normal Lua 5.5 declarations are supported; strings and comments may contain Unicode. `specVersion` selects the language/API contract.

Each channel runs the script independently. It executes top-level initialization once, then calls `main` for Source and timer events one at a time. Other channels have independent state and can execute concurrently. Static Document validation compiles source without running initialization; actual initialization requires the resolved Source and Sink contracts. After initialization, `main` and the built-in globals are read-only. Custom globals and closure state remain mutable.

## Events and payloads

| Field | Source event | Timer event |
| --- | --- | --- |
| `type` | `"source"` | `"timer"` |
| `timestamp` | Dispatch time, in integer milliseconds since Pipeline startup | Dispatch time, on the same timeline |
| `payload` | Read-only Source payload | Absent (`nil`) |
| `id` | Absent (`nil`) | Timer name (string), or `nil` for the anonymous timer |
| `eligibleAt` | Absent (`nil`) | Scheduled deadline, in integer milliseconds since Pipeline startup |

Events and nested payload values are deeply read-only; modifying them is a sandbox violation.

`event.timestamp` is a monotonic integer millisecond count since Pipeline startup, sampled when the channel dispatches the event. It is not Unix time. Use the wall-clock functions below for a real-world date or timestamp.

The exact Program version's proto3 `SourceRecordPayload` defines the input fields. Field access uses Protobuf JSON names (`device_id` normally becomes `deviceId`).

| Protobuf value | Lua representation |
| --- | --- |
| bool | boolean |
| Signed integers, uint32, fixed32 | integer |
| uint64, fixed64 | Canonical unsigned decimal string |
| float, double | number |
| string | Valid UTF-8 string |
| bytes | Arbitrary binary string |
| enum | Descriptor symbol name |
| message | Read-only nested view; absent message is nil |
| repeated | Read-only sequence indexed from 1 |
| map | Read-only table with keys represented by their declared type |

Ordinary scalar fields expose their proto3 default. Optional fields and unselected oneof alternatives have nil presence. Only fields in the current Contract are exposed; invalid payloads are rejected before `main`.

## Builders and output

The registry contains only Sink contracts bound to the current Flow. A contract id is `programName@exactVersion`; multiple instances of the same Program identity share that contract.

```lua
local builder = registry:getBuilder("com.example.hello-tenon@0.1.0")
function main(event)
  if event.type == "source" then
    builder:setMessage(event.payload.message)
    emit(builder:build())
  end
end
```

Builder methods derive from the original proto field name, not its JSON name: remove underscores and uppercase the first character and each character following an underscore. For field `device_id`, the suffix is `DeviceId`.

| Field kind | Builder operations |
| --- | --- |
| Scalar, string, bytes, enum | `setXxx(value)`, `clearXxx()` |
| Message | `getXxxBuilder()` |
| Repeated scalar/string/bytes/enum | `addXxx(value)` |
| Repeated message | `addXxxBuilder()` |
| Map with scalar values | `putXxx(key, value)` |
| Map with message values | `putXxx(key)` returns the replacement value's Builder |

Only a root Builder exposes `build()`. Nested Builders edit the corresponding field in the root. Replacing a map value updates existing Builders for that value; a oneof change can invalidate a nested Builder. Setting a oneof alternative clears the others. Optional scalar presence is preserved and can be cleared.

Use `:` for method calls. Receivers must belong to the same exact registry binding and message type; argument counts are exact. Enum values use descriptor symbol names, strings must be UTF-8, bytes are binary, and floats must be finite. Unsigned 64-bit values use decimal strings without signs, whitespace or leading zeros (except `"0"`), within the unsigned 64-bit range. Map keys follow the same representation rules. Business validation such as a downstream topic's allowed characters belongs to the plugin, not the Builder.

`build()` produces an immutable snapshot. Later Builder changes do not alter it. `emit(snapshot)` sends the value returned by `build()` to every matching Sink instance declared by the Flow.

`emit()` with no arguments produces no Sink record and establishes a completion boundary. `emit(nil)` is an error. Output is allowed only during `main` or its helpers, not at top level. One invocation may emit zero or more times. Each successful emit is accepted in order; permanent record-size rejection happens before acceptance with `egress.record_too_large`. If a Sink cannot accept output yet, the channel waits; other channels continue.

With at-least-once delivery, the first successful emit completes the pending Source group once all matching Sinks report success. A zero-argument emit completes the group without Sink output. Subsequent emits are separate outputs and cannot bypass, extend or roll back the first group's completion. See [delivery semantics](tenon-document.md#completion-and-delivery) and the [SDK contract](../sdk/SDK-impl-contract.md) for terminal outcomes.

Registry lookup, Builder receiver/argument/type/range errors and timer argument errors are ordinary Lua errors catchable with `pcall`. Resource-limit failures and sandbox violations are not recoverable inside the invalidated VM.

## Timers and state

Each VM has an anonymous one-shot timer slot plus independently named one-shot timers:

```lua
setTimeout(durationMs)
setTimeout(durationMs, id)
clearTimeout()
clearTimeout(id)
hasTimeout()
hasTimeout(id)
```

The delay must be a nonnegative Lua integer. It starts at the call, on a monotonic timeline. Setting again replaces the pending timer with the same id; different ids remain independent. `clearTimeout` is idempotent, and `hasTimeout` observes the selected timer immediately. A timer is removed before its event enters `main`, so it reports false during that event unless another timer was scheduled. The id must be a non-empty UTF-8 string of at most 128 bytes. The maximum accepted integer is `9223372036854775807`; if conversion or deadline arithmetic cannot represent it, the catchable error is `setTimeout delay is out of range`.

These functions are allowed at top level and in `main`. A zero delay schedules eligibility on the next event-loop iteration. Execution may be later because the channel is busy. Periodic behavior explicitly schedules the next one-shot timer from the current timer event. Negative, non-integer, string, or missing delays raise `setTimeout delay must be a non-negative integer`; a delay that cannot be represented by the monotonic deadline or `eligibleAt` calculation raises `setTimeout delay is out of range`.

Omitting `id` or passing `nil` selects the anonymous timer. Invalid ids raise the catchable error `timer id must be a non-empty string of at most 128 bytes`.

`event.eligibleAt` is the timer's deadline in integer milliseconds since Pipeline startup, on the same monotonic timeline as `event.timestamp`. The difference `event.timestamp - event.eligibleAt` is its dispatch delay. Timers with the same deadline run in registration order. Among due timers and readable Source input, the earlier deadline or first observed Source readiness runs first; ties use their registration or observation order. Source readiness keeps its place until one record is consumed, so a timer that repeatedly reschedules itself with zero delay cannot indefinitely prevent readable Source input from running. Sink backpressure and lifecycle work can still delay either kind of event.

Named timers share the VM's batch completion behavior: the first successful `emit` in an event can complete all pending Source records in that VM. A timer id does not provide independent reliable completion for records with that key.

Lua state, Builders, snapshots and timers count toward the VM's memory allowance. Each pending timer is charged 2 KiB plus three times its id length in UTF-8 bytes (zero id bytes for the anonymous timer). The fixed charge includes an allowance for the timer and its indexes; it is an estimate, not an exact allocation or process-memory measurement. There is no separate timer-count limit. Top-level initialization and each `main` invocation are bounded by the Runner's Lua CPU limit. State lives only in the current VM. A script/resource/sandbox failure invalidates the VM and its timers; rebuilding executes top-level code again. Relevant Document updates and process restarts also recreate state. Script replacement can briefly hold both old and new VMs, each with its own configured allowance.

## Wall-clock time and dates

The read-only `currentTimeMillis()` function returns the current system time as a signed 64-bit Lua integer: milliseconds since 1970-01-01 00:00:00 UTC. It reads the clock on every call, during either top-level initialization or `main`, and discards sub-millisecond fractions. Extra arguments are ignored, following ordinary Lua function behavior. Times before the Unix epoch or outside the signed 64-bit millisecond range raise the catchable error `currentTimeMillis system time is out of range`.

The read-only `os` namespace exposes only these official Lua 5.5 functions:

| Function | Behavior |
| --- | --- |
| `os.time([date])` | With no argument or nil, returns current Unix **seconds** as an integer on supported Tenon platforms. A date table is interpreted in local time and normalized in place. `year`, `month` and `day` are required; `hour` defaults to 12, `min` and `sec` to 0, and optional `isdst` selects daylight saving time. |
| `os.date([format [, time]])` | Formats Unix **seconds**, defaulting to the current time and format `%c`. A leading `!` selects UTC; otherwise it uses local time. `*t` returns a table with `year`, `month`, `day`, `hour`, `min`, `sec`, `wday` (Sunday = 1), `yday` and `isdst` when available. `!*t` selects UTC fields. Other formats use the host's supported `strftime` conversions and locale. |
| `os.difftime(t2, t1)` | Returns `t2 - t1` in seconds as a Lua number. Both arguments are Unix seconds, not milliseconds. |

Invalid arguments or unrepresentable dates raise ordinary catchable Lua errors. `os.time(date)` needs a mutable date table. Pass Unix seconds to `os.date`; convert milliseconds to seconds first:

```lua
local timestampMs = currentTimeMillis()
local utc = os.date("!%Y-%m-%dT%H:%M:%SZ", timestampMs // 1000)
```

Wall-clock readings can repeat or move backward or forward after system clock adjustments. They are not unique identifiers, monotonic elapsed time, or the original Source event time. Multiple calls in one `main`, replay, or VM reinitialization can observe different values. Capture one reading when an output needs internally consistent fields. Timer delays continue to use the monotonic clock; these functions do not change scheduling or guarantee precise firing times. The names `os` and `currentTimeMillis` are reserved built-ins; scripts that previously used them as mutable globals must rename those globals.

## Available standard environment

The basic globals are `_G`, `_VERSION`, `assert`, `error`, `ipairs`, `next`, `pairs`, `pcall`, `print`, `select`, `tonumber`, `tostring`, `type` and `xpcall`. `_VERSION` is `Lua 5.5`.

The following standard-library members are available:

| Library | Members |
| --- | --- |
| string | `byte char find format gmatch gsub len lower match pack packsize rep reverse sub unpack upper` |
| table | `concat insert move pack remove sort unpack` |
| math | `abs acos asin atan ceil cos deg exp floor fmod frexp huge ldexp log max min modf pi rad random randomseed sin sqrt tan tointeger type ult maxinteger mininteger` |
| utf8 | `char charpattern codepoint codes len offset` |
| os | `date difftime time` |

All listed members follow official Lua 5.5 semantics. Use `math.randomseed(...)` when a script needs a repeatable random sequence. `string.format` produces ad-hoc text and can expose implementation-specific float text or object identity such as `%p`. Pack formats use the host ABI unless the format specifies endianness, width and alignment. Math results can have host-library low-bit differences. UTF-8 helpers operate on code points. `print` writes to [live diagnostics](observability.md).

## JSON

`json.decode(text)` parses strict UTF-8 JSON. Duplicate object keys are rejected at every depth. Integer tokens must fit signed 64-bit; decimal/exponent tokens become finite binary64 floats. Large identifiers outside this range must use strings. `json.encode(value)` serializes representable Lua values to UTF-8 JSON. It preserves integer/float distinctions, including `1`, `1.0` and negative zero.

`json.null` is one immutable userdata value representing JSON null; Lua nil means absence. Decoding null returns that same value. An ordinary empty table encodes as `{}`; `json.array()` creates an empty array. Decoded arrays retain their array identity even after all elements are removed. Tables whose positive integer keys consecutively cover 1 through their size encode as arrays; sparse or mixed-key tables fail. Cycles fail, but shared acyclic values may appear at multiple positions. Object keys are emitted in UTF-8 byte order and arrays retain their element order.

Errors are ordinary catchable Lua errors. Fixed messages are `json.decode input must be a string`, `json.decode input must be valid UTF-8`, `json.decode input must be valid JSON`, `json.decode object field names must be unique`, `json.decode integer must fit signed 64-bit`, `json.decode number must be finite`, and `json.encode value is not representable as JSON`. Encoding rejects nil, nonfinite floats, invalid UTF-8, unsupported values, table shapes and cycles.

## Binary data

The read-only `bytes` namespace operates on Lua strings containing arbitrary bytes. Offsets are positive integer positions starting at 1, lengths are nonnegative integers, and floating-point `1.0` is not an integer offset. An out-of-range read raises an error; a successful read returns exactly the requested region. Required arguments must be present; extra arguments follow ordinary Lua function behavior and are ignored.

| API | Result and constraints |
| --- | --- |
| `bytes.len(data)` | Byte count |
| `bytes.slice(data, offset, length)` | Complete selected region; an empty region permits offsets 1 through `#data + 1` |
| `bytes.byte(data, offset)` | Integer byte in 0–255 |
| `bytes.read_u8(data, offset)`, `bytes.read_i8(data, offset)` | One unsigned or two's-complement signed byte |
| `bytes.read_u16_be/le(data, offset)`, `bytes.read_i16_be/le(data, offset)` | Two-byte unsigned/signed integer, with explicit big/little endian suffix |
| `bytes.read_u32_be/le(data, offset)`, `bytes.read_i32_be/le(data, offset)` | Four-byte unsigned/signed integer |
| `bytes.read_u64_be/le(data, offset)` | Eight-byte unsigned value as a canonical decimal string, directly usable by uint64/fixed64 Builders |
| `bytes.read_i64_be/le(data, offset)` | Eight-byte two's-complement Lua integer |
| `bytes.read_f32_be/le(data, offset)` | IEEE binary32 expanded exactly to binary64; preserves subnormals and negative zero, rejects NaN/infinity |
| `bytes.read_f64_be/le(data, offset)` | IEEE binary64, with the same finite-value requirement |
| `bytes.bcd_to_string(data)` | Each high then low nibble becomes one decimal digit; nibble > 9 fails, including sign/padding nibbles |
| `bytes.to_hex(data)`, `bytes.from_hex(text)` | Lowercase output; input permits either ASCII case, even length, no prefix, whitespace or separators |
| `bytes.to_base64(data)`, `bytes.from_base64(text)` | Canonical padded RFC 4648 standard alphabet with zero unused bits |
| `bytes.crc16(data, polynomial, initial, xorOut, bitOrder)` | All five arguments required; numeric parameters are integers 0–65535 and bitOrder is exactly `"msb"` or `"lsb"` |

In the table, `be/le` denotes two separately named functions, for example `bytes.read_u16_be` and `bytes.read_u16_le`. Hex, Base64 and BCD accept empty input and return empty output. All data/text arguments must be strings, but encoded-input validation operates on ASCII byte syntax without first requiring UTF-8.

For CRC16, supply the polynomial in normal form for `"msb"` or reflected form for `"lsb"`. The result applies `xorOut` after processing all bytes. For CRC-16/MODBUS, use `bytes.crc16(data, 0xA001, 0xFFFF, 0, "lsb")`; for CRC-16/IBM-3740, use `bytes.crc16(data, 0x1021, 0xFFFF, 0, "msb")`.

Binary errors are catchable and have fixed messages: `bytes input must be a string`, `bytes offset must be a positive integer`, `bytes length must be a non-negative integer`, `bytes range is out of bounds`, `bytes floating-point input must be finite`, `bytes BCD input must contain only decimal nibbles`, `bytes hex input must be even-length ASCII hexadecimal`, `bytes base64 input must be canonical padded RFC 4648`, `bytes CRC16 parameters must be integers from 0 to 65535`, and `bytes CRC16 bit order must be "msb" or "lsb"`.

Use Lua's `&`, `|`, `~`, `<<` and `>>` on integer read results for bit fields. `string.pack`, `string.unpack` and `string.packsize` provide general Lua binary handling; for a byte format shared across hosts, specify endianness, width and alignment explicitly. The `bytes` API provides Tenon's fixed-width, explicit-endian binary contract.

## Failed inputs and execution

For at-least-once delivery, a Source decode failure completes the current record with `ERROR`. If earlier records are pending, they receive `RETRY` and the VM is rebuilt; otherwise the existing VM state and timer survive. If `main` fails before its first successful `emit`, its current Source record receives `ERROR` and earlier pending records receive `RETRY`. A failing timer retries inputs still pending in the VM. Inputs already transferred to an accepted output boundary keep that boundary's outcome, even if execution later fails. Accepted output boundaries still run in order and completed boundaries do not roll back. The Source implementation owns any external replay decision.

CPU/memory exhaustion, writes to read-only inputs or built-ins, replacing frozen `main` and other sandbox violations cannot be caught by nested `pcall`/`xpcall`. They terminate the current execution. An unexpected Source process failure resets its Flow's Lua state, timers and pending inputs. A planned configuration update can preserve compatible Lua state; see [Document updates](tenon-document.md#updates-and-failure). Outputs already sent downstream are not withdrawn.
