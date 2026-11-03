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

# Rust IPC Library

`tenon-ipc` implements the Queue and Bell operations used by the Runner and Rust Plugin SDK. The [IPC contract](../../ipc-contract.md) describes the shared file formats and behavior required across SDK languages.

## Queue API Notes

- `try_write_with(exactBodyLength, encoder, onCommitted)` gives the encoder a cursor limited to the body. The encoder runs at most once and must fill it exactly. It must not retain the mapped borrow.
- Invalid length and `Full` do not call the encoder. An encoder error, panic, or incomplete write does not publish `commit`; partial bytes remain invisible.
- `onCommitted` runs once after publication and before notification, even if notification fails. It is skipped on `Full` and encoding failure and must not panic.
- This is the Rust writer's only append interface. A caller with encoded bytes copies them into the supplied cursor and retains the source record if it may retry after `Full`.
- `ReadPosition` belongs to the reader that produced it. `release_through` releases that prefix. Reopened readers cannot use old positions.
- `WriteReceipt` is the end of one committed append, including wrap. It belongs to the open writer that created it; another or reopened writer rejects it. It is not a business record ID or acknowledgement and adds no shared position.
- `is_released` and `wait_released` compare a receipt with shared `release`.
- `free_bytes()` returns `dataCapacity - (commit - release)` after validating positions. Invalid positions return a Queue error. It reserves nothing and changes no state. Headers, padding, and wrap gaps all consume this space.
- `plan_append` calculates space for several records, and the bound `LoopBell::wait_until` waits for it.

Callbacks return promptly. The SDK layer remains responsible for business completion.
