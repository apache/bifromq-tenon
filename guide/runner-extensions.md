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

# Building a Runner distribution

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../DISCLAIMER).

A Rust executable can use Tenon's Runner and Pipeline with three hooks: HTTP request authorization, execution admission, and artifact protection. Tenon does not supply licensing, billing, encryption algorithms or credential management. The `tenon` library exports the public interfaces.

The [security policy and threat model](../SECURITY.md#extension-responsibilities) describes the limits of each hook and the responsibilities of a custom distribution. Hook availability alone does not enable access control or turn plugin processes into security sandboxes.

```rust
fn main() -> std::process::ExitCode {
    tenon::run_main_with(|_config| Ok(tenon::RunnerHooks::default()))
}
```

`run_main_with` receives a one-time initializer over validated `RunnerConfig` and returns `ExitCode`. It runs once at Runner startup, before saved Documents are loaded or HTTP requests are accepted. Distribution settings belong to the optional `extra` object. Sanitize errors returned during initialization.

`RunnerHooks::new(policy, protection, authorization)` installs concrete implementations. Defaults are `AllowAll`, `ByPass` and `NoHttpAuth`.

## ArtifactProtection

`ArtifactProtection: Send + Sync` controls Document storage bytes and package decoding. All three methods have default implementations. `protect` and `unprotect` write unchanged Document bytes. `open_plugin_package` decodes a standard `tar.gz` package. `ByPass` uses these defaults without encryption or decryption.

A custom implementation can override any method. It can call the default decoder after it opens its own envelope:

```rust
use std::io::{self, Read};
use tenon::{ArtifactProtection, ByPass, PluginPackageOutput};

struct DistributionProtection;

impl ArtifactProtection for DistributionProtection {
    fn open_plugin_package(
        &self,
        source: &mut dyn Read,
        output: &mut dyn PluginPackageOutput,
    ) -> io::Result<()> {
        ByPass.open_plugin_package(source, output)
    }
}
```

This example delegates unchanged input. An envelope implementation supplies its decoded reader instead. The Document methods keep their defaults.

The core owns files, paths, permissions, commits, and cleanup. The package hook receives a reader and a directory writer. It does not receive a destination path or a process handle. Methods run in blocking Store work and can run concurrently. The implementation must bound its memory use and work. Cancellation does not interrupt a file operation that has started.

### Documents

`protect` writes the stored representation. `unprotect` writes the original Document bytes. If `protect` fails before file replacement, the saved Document stays unchanged. A failure after replacement does not imply rollback. If `unprotect` fails, startup stops and keeps the saved file. The core discards partial output.

ETags, GET responses, and plugin configuration use the original plaintext. Random protected bytes do not change the ETag. Document protection does not provide API authorization or protect runtime memory.

### Plugin packages

The core receives the full upload in a temporary file inside the Program Store. Installation and recovery read saved package files through the same decoding and validation process. After installation checks pass, the core renames the temporary original to its final path. A rejected package does not replace an installed package.

`PluginPackageOutput` is a public trait. The Runner supplies its private implementation as `&mut dyn PluginPackageOutput`. The trait exposes only `write_file` and `create_dir`. The Runner owns the directory, failure records, and final validation. The hook decodes its format and sends each file through this trait. The core does not require a tar archive between the hook and directory validation.

`output.write_file(relative_path, reader)` writes one ordinary file to EOF and creates missing parent directories. `output.create_dir(relative_path)` creates an explicit directory. The writer checks paths, duplicates, file conflicts, entry counts, and byte limits. It sets fixed permissions. It has no operation for links or special files. See the [directory contract and limits](plugins.md#package-structure).

The hook must finish format, integrity, and access checks before it returns success. A recognized envelope that fails a check must return an error. It must not fall back to another format. A streaming encryption format must detect truncation and missing or reordered content. No output can run before the hook succeeds and the core validates the complete directory.

| Result | Core action |
| --- | --- |
| `Ok(())` with no recorded stream or output failure | Validate the directory. Save the received input without changes. Publish the temporary runtime files. |
| Hook error with `InvalidData` or `UnexpectedEof` | Reject a new upload with HTTP 422 and `plugin_package_invalid`. Discard the temporary directory. |
| Hook error with `FileTooLarge` | Reject a new upload with HTTP 413 and `plugin_package_too_large`. Discard the temporary directory. |
| Other hook error | Reject a new upload with HTTP 422 and `plugin_package_open_failed`. Discard the temporary directory. |
| Invalid output | Return the package validation error. A size limit returns HTTP 413. Discard the temporary directory. |
| Local input or output filesystem failure | Stop the Runner. Discard the temporary directory if cleanup succeeds. |

The method returns `io::Result<()>`. A hook can put a custom error in `io::Error`. The core uses the error kind for the categories above. It does not inspect the custom error type. Use `PermissionDenied` to reject package access. Do not include secrets in errors.

The core records failures at its own reader and writer. These failures take priority over the hook result. The core does not identify local filesystem failures from `ErrorKind` alone. Returning success cannot hide a recorded failure. Any failure during recovery stops startup and keeps the saved package.

The default decoder checks gzip, tar headers, archive metadata, and end markers. It uses only the public directory writer and `io::Error`, as a custom decoder does. The writer checks file size limits against the bytes it reads. The decoder also checks that each file has the length stated in its tar header. The core checks the manifest, platform, Schema, descriptor, and normalized file contents. HTTP accepts only `application/octet-stream` for every format.

All plugins use the same storage process. A standard package stays as the received `tar.gz`. An envelope stays as the received envelope. The hook does not choose a storage form. The Program Store owns package files, validation, publication, and cleanup.

Each Runner startup calls the hook for every saved package and reconstructs the runtime files. A stopped Runner can receive either package type through a file copy. The [package storage contract](plugins.md#package-storage-and-recovery) defines paths, permissions, identity checks, and recovery failures. The default Runner accepts standard archives at these paths. Other formats require a custom hook.

Identity stays `programName + exactVersion`. An identical normalized file set returns HTTP 204 and keeps the existing storage bytes and Entry. A new envelope does not replace an existing one. Different content returns HTTP 409. Envelope replacement requires uninstall and installation, or a new version.

The hook runs at installation and each Runner recovery. Process restarts use the verified runtime files again. Plans keep the existing shared Entry until their process owners finish. Uninstall requires no external Entry references. It removes the stored input and runtime files.

Normal shutdown removes runtime files after process cleanup. If process ownership is uncertain, the existing recovery rules keep the runtime directory.

A host administrator can copy plaintext runtime files. This interface does not prevent that copy or prevent execution of a repackaged standard plugin elsewhere.

## ExecutionPolicy

```rust
pub type PolicyChanges = tokio::sync::watch::Receiver<()>;

pub trait ExecutionPolicy {
    fn authorize(&self, scope: ExecutionScope<'_>)
        -> Result<ExecutionPermit, ExecutionDenied>;

    fn changes(&self) -> Option<PolicyChanges> {
        None
    }
}
```

`authorize` receives the full proposed set of accepted, verified Documents. The Runner uses these references only during this call. Set order is unspecified. Accepted Documents with missing plugins stay in the set.

The Runner thread calls the policy synchronously. The policy needs neither `Send` nor `Sync`. It must return promptly without blocking I/O. Callback counts are not a quota ledger or a commit receipt.

The Runner calls `changes()` once, before its first decision. It first evaluates an empty set before Store recovery, even if no Documents exist. Denial stops startup. Recovery then tries Documents one by one in id byte order. A denied candidate stays readable and deletable, with no accepted Pipeline. PUT of identical bytes can retry a denied Document.

PUT evaluates the full set with the candidate after static and conditional checks, before persistence. Candidate denial returns the policy's public code and message as HTTP 403. It keeps the previous accepted set. DELETE removes a Document without a policy call. Plugin installation and process restarts do not cause another decision for unchanged Documents.

Both outcomes carry `entitlement_until: Option<Instant>`. Each outcome immediately replaces the single current deadline. `None` removes it. File success or failure does not change this deadline. Expiry starts bounded shutdown, including when there is no API traffic. It does not mean all processes disappear at once.

A policy updater must validate a full replacement and publish its cached state before it sends a notification. Invalid replacement data must not extend the old deadline. The notification is a latest-state hint; several notifications can merge. The Runner keeps the same receiver across waits. It evaluates the full accepted set when notified, including an empty set. Denial of this current set starts shutdown and keeps Documents.

If a notification arrives during a Document commit, the Runner also evaluates the full set after a successful commit, before Pipeline publication. Denial starts shutdown and keeps the saved Document. Failed commits need no second decision. Without a notification, a commit does not cause a second decision.

The Runner processes pending notifications from recovery before initial Pipeline execution. A closed channel emits `runner.policy_changes_closed`, keeps the current deadline, and disables the notification wait. External shutdown and an already expired deadline take priority in the main loop. An update must be accepted before shutdown starts. Later updates cannot reopen the Runner.

The notification has no generation or acknowledgement. It does not atomically change package keys, cancel package work, or add a policy call before each process spawn. Work already started can finish its disk commit during shutdown. It cannot publish new Pipeline targets then.

Distributions must keep access identity stable during ordinary renewal. A change of customer or decryption identity requires a Runner restart. Old saved envelopes still need compatible keys.

Existing policy implementations can omit `changes()`. The initial empty-set call is new behavior. Implementations must support it even when they do not supply notifications.

## HttpApiAuthorization

`HttpApiAuthorization: Send + Sync` asynchronously checks the original HTTP `Method`, URI path and `HeaderMap`, returning a Send future of `Result<(), HttpAuthRejection>`. It sees all methods and routes, including unknown routes, before endpoint body processing. It does not receive the query, request body, trailers or TLS client identity. Paths retain percent encoding; decode each segment once when interpreting resource ids. Preserve repeated and non-text header values.

`Unauthorized` requires a `WWW-Authenticate` challenge and yields 401; `Forbidden` yields 403 with an optional challenge. Core responses contain stable public messages and do not echo credentials or internal errors. HEAD responses have no body. Denial does not modify execution admission, entitlement deadlines or running Pipelines.

The hook may await external services but must not block executor threads. Implement timeouts and failures in the distribution. A cancelled request can drop the future before completion; release any resources it holds. Do not rely on completion of an abandoned callback for an irreversible operation. Requests already rejected by shutdown receive 503 without invoking authorization. With mutual TLS, transport authentication happens first; each new SSE connection is authorized, but an existing stream is not continuously reauthenticated.
