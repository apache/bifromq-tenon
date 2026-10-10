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

# Running Tenon

Tenon is part of Apache BifroMQ (Incubating). See the [incubation disclaimer](../DISCLAIMER).

Start the Runner with one absolute configuration path:

```sh
tenon --config /absolute/path/runner.jsonc
```

The configuration file uses UTF-8 JSONC. Restart the Runner to apply changes. An unreadable or invalid configuration prevents startup. See the [configuration Schema](../contracts/runner/config.schema.json) for all fields and allowed values.

```jsonc
{
  "stateDirectory": "/var/lib/tenon",
  "http": {"listenAddress": "127.0.0.1:8080"},
  "pipeline": {
    "startupTimeoutMs": 30000,
    "reconfigureTimeoutMs": 30000,
    "shutdownTimeoutMs": 30000,
    "retryBackoff": {"initialDelayMs": 100, "maximumDelayMs": 30000}
  },
  "lua": {"cpuTimeLimitMs": 50, "memoryLimitBytes": 16777216}
}
```

`stateDirectory`, HTTP listen address, retry backoff and Lua limits are required. The three Pipeline deadlines default to 30,000 ms each. Backoff maximum must be at least its initial delay. Optional `extra` is an object interpreted by a custom distribution, not a way to override core fields.

The deadlines limit Pipeline startup, configuration updates and shutdown. If an update times out, the Pipeline restarts with the latest valid saved configuration. Submitting another update does not extend the deadline. These settings do not control how long a plugin waits for an external service.

## Files, recovery and stopping

Give each Runner its own writable `stateDirectory`, protected from other writers. The Runner creates its state directories with mode `0700`; existing directories must have that mode and must not be symlinks. Modify Documents and installed packages through the HTTP API.

Saved Documents and plugin packages survive restarts. Each startup reconstructs plugin runtime files from the saved packages. A missing package leaves dependent Documents `unready`. A saved package that fails access or validation checks stops startup and remains on disk. Invalid or unreadable saved Documents also prevent startup. Back up configuration and packages with a consistent filesystem snapshot or while the Runner is stopped. See [package storage and recovery](plugins.md#package-storage-and-recovery).

In-flight messages, Lua state and timers do not survive restarts. Use upstream replay and downstream deduplication as required by your delivery policy.

Use SIGINT or SIGTERM for normal shutdown. Allow enough time for the configured Pipeline shutdown deadline before forcing termination. Storage or installed-package corruption can stop the Runner; retain stderr and the exit status for diagnosis.

## Transport and access

Use the [security policy and threat model](../SECURITY.md) to define the deployment's management access, trusted workloads, secrets and host isolation before exposing this listener.

Without `http.tls`, the configured port is HTTP. Set `http.tls` to the following object for HTTPS:

```json
{
  "certificateChainFile": "/etc/tenon/server-chain.pem",
  "privateKeyFile": "/etc/tenon/server-key.pem",
  "clientCaFile": "/etc/tenon/client-ca.pem",
  "handshakeTimeoutMs": 10000
}
```

Certificate and key paths are absolute. The server certificate comes first in the PEM chain. The private key is unencrypted PEM in PKCS#8, PKCS#1 or SEC1 format. `clientCaFile` is optional: when present every HTTP connection requires a trusted client certificate; when absent only the server has a TLS identity. The positive handshake timeout defaults to 10 seconds and is not renewed by partial input. TLS configuration creates an HTTPS-only listener, with no plaintext fallback or redirect.

Materials are read once. Rotation requires a restart. There is no online revocation or live certificate reload; existing authenticated connections are not reauthenticated. HTTP authorization is a separate [extension](runner-extensions.md). The default Runner has no request authorization and uses plaintext Document storage. A trusted management network, reverse proxy, client-certificate policy or custom distribution must provide the required deployment controls. Native plugins are trusted code, not untrusted sandboxed applications.

## CPU and memory

Set CPU and memory limits in each [Document](tenon-document.md). On Linux, enforcement requires kernel 5.14 or newer, cgroup v2, and a dedicated writable cgroup scope with the requested controllers delegated. Tenon prepares the required subgroups automatically.

### Host deployment

Use the supplied [systemd unit](../deploy/systemd/tenon.service) with systemd 251 or newer:

1. Create the `tenon` user and group.
2. Install the binary and configuration at the paths specified in the unit. Set `stateDirectory` to `/var/lib/tenon`.
3. Install the unit as `/etc/systemd/system/tenon.service` and run:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now tenon
```

The unit's `Delegate=cpu memory` setting enables Document resource limits. For an interactive session, use the following when your systemd user manager has those controllers delegated:

```sh
systemd-run --user --scope -p 'Delegate=cpu memory' \
  /absolute/path/tenon --config /absolute/path/runner.jsonc
```

### Container deployment

Use the [Docker Compose configuration or Podman instructions](../deploy/container/README.md). Give each Runner its own container and state volume. The runtime must supply a private cgroup namespace and permission to manage it. The supplied configurations support a container init with Tenon as its direct child.

### Enforcement and troubleshooting

Limits cover the Pipeline and its plugins. They exclude the Runner and container init. Container or service limits may impose tighter bounds. A cgroup scope must be dedicated to one Runner; shared scopes and competing Runners are rejected at startup.

Without writable delegation, the Runner reports `runner.resource_limits_unavailable` and can run Documents that have no resource limits. A limited Document reports `resource_limits_apply_failed` if its limits cannot be applied. Check the Runner diagnostic and the service or container permissions before retrying.

On macOS, valid limits are retained but reported as `ignored` with reason `platform_unsupported`; the Pipeline still runs. Pipeline detail reports enforcement for the applied Document, which may differ from the latest saved Document.

Flow channel counts use the allowed logical CPU count at Runner startup, independently of Document CPU quotas. Account for plugin processes and one Lua VM per channel when planning memory. Script replacement can temporarily use memory for both old and new VMs.

See [observability](observability.md) for metrics settings. Record-size and pending-record limits are configured per Flow.
