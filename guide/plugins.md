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

# Plugin packages and development

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../DISCLAIMER).

A Program is identified by `programName` and an exact semantic version. It implements `source`, `sink` or `source-and-sink`. An Instance runs the Program with its own configuration. Choose a language SDK and generator from the links below to write a plugin.

## Package structure

The core requires a plugin directory with these fixed root files:

```text
manifest.json
config.schema.json
payload.descriptor.pb
```

Other ordinary files contain the launcher, executable, libraries, optional runtime and resources. The [manifest Schema](../contracts/plugin/manifest.schema.json) defines identity, display metadata, interface, a nonempty `platforms` list and an argv `command`. The descriptor must include the standard top-level `SourceRecordPayload` and/or `SinkRecordPayload` roots exactly for the interfaces implemented. Include source information in the descriptor; the standard packaging tools generate it.

Package paths must be relative and canonical. Absolute paths, `.`/`..`, duplicate normalized paths, symlinks, hard links and special files are rejected. Directory entries and package permission bits are not package identity. Extracted runtime directories use `0700` and ordinary files `0500`.

The standard generators produce `tar.gz` packages. The default package hook decodes this format. A custom hook can decode another format into the same directory contract. Upload every format with `Content-Type: application/octet-stream`.

Received packages have an 8 GiB limit. Runtime contents have a 4 GiB total file limit and a 100,000-entry limit, including implicit directories. `manifest.json` and `config.schema.json` each have a 16 MiB limit. `payload.descriptor.pb` has a 64 MiB limit. Paths have a 4,096-byte limit; each component has a 255-byte limit.

The default decoder also limits each compressed and decompressed archive stream to 8 GiB. It permits at most 100,000 tar entries. Each archive metadata record has a 64 KiB limit. Custom decoders must bound their format-specific work. The core applies directory limits to all formats.

The Runner executes `command` without a shell, with the extracted runtime directory as cwd. Bare executable names use the inherited PATH. Tenon appends its reserved `--sdk-config` argument; do not include it yourself. Use stdout and stderr for diagnostic text.

## Display metadata

Every manifest requires `displayName` and `description`. The display name is 1-80 Unicode code points of single-line plain text; the description is 1-1024 code points of plain text and may contain LF or CR line breaks. Both must contain a non-whitespace character. C0/C1 control characters are forbidden except LF and CR in descriptions; display names also forbid Unicode line and paragraph separators. The [manifest Schema](../contracts/plugin/manifest.schema.json) defines the exact character rules.

Values are preserved without trimming, normalization, truncation, or inferred defaults. They are package metadata, not HTML or Markdown. They do not participate in `programName + exactVersion` identity, Document references, interface selection, or launch behavior. The Runner returns both fields in list and single-Program responses. Console uses the display name as the title, a shortened description on cards, and the complete description on the inspection page.

Platform variants of the same Program version must carry identical display metadata. Editing either field changes the immutable package content; uploading changed text under an already installed identity conflicts just like any other package change. Rebuild plugins and generators together when updating the manifest contract; packages missing either field are invalid.

## Platform and immutability

Manifest platforms use `os` = `linux` or `darwin` and `architecture` = `amd64` or `arm64`. No aliases, wildcard or duplicate entries are accepted. Installation rejects packages that do not support the Runner's platform. One package has one command regardless of the number of declared platforms.

Package authors must declare genuine compatibility and document external dependencies. A portable JAR may rely on an external JVM; the Runner does not execute a package during installation to probe its dependencies. Missing dependencies become startup failures.

On the same Runner, the normalized ordinary-file paths and every file's original bytes determine duplicate installation. Equal content is idempotent; any difference for the same identity conflicts. Across platform variants of one logical version, interface, Config Schema, payload descriptors and business semantics must remain identical, while launcher/runtime/platform material may differ. Change the exact version for a semantic change.

Install through POST `/plugins`, then refer to the exact identity from a Document. To upgrade, install the new version, update the Document, observe application and shutdown of the old instance, then uninstall the old version. Do not modify saved packages or runtime files while the Runner runs. A missing package leaves dependent Documents `unready`. A saved package that fails validation stops startup. Stop the Runner before you restore a damaged package from a known valid copy.

## Package storage and recovery

The Program Store keeps the received package bytes without changes. Standard packages remain `tar.gz` archives. Distribution envelopes remain envelopes. Extracted runtime files are temporary. They are not the durable source.

All packages use this path under the state directory:

```text
plugins/programs/<programName>/.tenon-artifact-<versionHash>
```

`versionHash` is the lowercase hexadecimal SHA-256 of the exact version string in UTF-8. It keeps the filename length fixed. It does not authenticate the package. Package files must be ordinary files with mode `0600` and one hard link. Namespace directories must have mode `0700`.

Installation first calls the package hook to write a private runtime directory. The core then validates that directory. It then synchronizes the received package file, renames it to the final path, and synchronizes the parent directory. Only then does the Store publish the validated Entry. An equal normalized file set keeps the existing package bytes and Entry, even if the new archive bytes differ.

Each Runner startup reconstructs runtime files from the saved packages before it serves requests or starts Pipelines. Recovery validates package contents and requires the package identity to match its path. A package access, format, permission, or identity failure stops startup and keeps that package. Recovery keeps valid packages for other platforms; their Documents remain `unready` on this platform.

Recovery removes incomplete uploads, deletion tombstones, and entries outside the supported layout. An invalid or unreadable namespace is removed as a whole. Cleanup does not follow symbolic links. A failure to read the Store root or complete required cleanup stops startup.

A stopped Runner can receive a package through a file copy to the path above, with the required permissions. Runtime files are never a recovery source. Plugin process restarts within one Runner reuse the validated runtime files. Normal shutdown removes these files after process cleanup. If process ownership is uncertain, the runtime directory remains for the existing recovery process.

Uninstall requires the absence of Document and runtime references. The Store renames the saved package to a deletion tombstone and synchronizes its parent. It then removes the Entry, runtime files, and tombstone, and synchronizes the parent again. A failure after rename can leave a committed deletion. After Runner recovery, query the identity before a retry.

## SDKs and generators

- [Rust SDK](../sdk/rust/plugin-sdk/README.md), [scaffold](../sdk/rust/rust-plugin-scaffold/README.md), and [cargo-tenon](../sdk/rust/cargo-tenon/README.md).
- [Java SDK](../sdk/java/README.md), [archetype](../sdk/java/README.md#generate-a-plugin), and [Maven packaging plugin](../sdk/java/README.md#packaging).
- [Repository plugins and local debugging](../plugin/README.md), including the Dummy Source, Stdout Sink and MQTT plugin.

Use the generators' standard build and bundle operations. Document how the plugin acknowledges, retries and rejects records. A Sink should report success only after delivery reaches its documented guarantee. Test normal shutdown, failures, replay and actual external delivery as well as startup.

The [quickstart](quickstart.md) builds an entirely local generated plugin and exercises the normal installation API. [Contributing](../CONTRIBUTING.md) describes repository-wide and cross-platform validation.

## Build artifacts for distribution

Use `cargo tenon bundle --release` for a Rust distribution; the quickstart's default debug bundle is for local testing. Rust binaries can contain build-machine paths in diagnostics and debug information. For a distributable, set compiler `--remap-path-prefix` mappings for the checkout, Cargo cache and any local dependencies through `RUSTFLAGS` or `CARGO_ENCODED_RUSTFLAGS`, then inspect the finished archive. A release profile alone does not remove every embedded source path. Keep build logs and local test results outside the published package.

## Custom package formats

A custom Runner can decode another package format through `ArtifactProtection`. It can also decode an envelope around a standard package. The core validates the resulting directory and owns all files and processes. Standard generators and the default Runner continue to use `tar.gz`. See the [extension contract](runner-extensions.md#plugin-packages) for storage, recovery, access failures, and runtime cleanup.
