#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
repo_root="$(cd "${script_dir}/.." && pwd -P)"

if [[ "${TENON_BUILD_CACHE_OWNER:-}" != "${repo_root}" ]]; then
  exec python3 "${script_dir}/build-cache.py" run -- bash "${script_dir}/verify-rust.sh" "$@"
fi

cd "${repo_root}"

python3 -B -m unittest discover -s tools/tests -p test_validate_rust_test_layout.py
python3 tools/validate-rust-test-layout.py
python3 -B tools/tests/build_cache_test.py
workspace_manifests=(Cargo.toml sdk/rust/Cargo.toml)
resource_test="metrics::runtime::tests::resources::pull_snapshot_tracks_cpu_and_current_rss_without_including_children"
for manifest in "${workspace_manifests[@]}"; do
  cargo fmt --manifest-path "${manifest}" --all -- --check
  cargo check --manifest-path "${manifest}" --workspace --all-targets --locked
  cargo clippy --manifest-path "${manifest}" --workspace --all-targets --locked -- -D warnings
  # macOS socket creation and close-on-exec are separate operations. Independent
  # tests must not spawn a process while another test is reserving a listener.
  test_options=(--test-threads=1)
  if [[ "${manifest}" == Cargo.toml ]]; then
    # RSS deltas need a fresh process: the OS may reclaim earlier tests' resident pages.
    test_options+=(--skip "${resource_test}")
  fi
  cargo test --manifest-path "${manifest}" --workspace --all-targets --locked -- "${test_options[@]}"
done
cargo test --package tenon --lib --locked -- "${resource_test}" --exact --list --format terse |
  grep --fixed-strings --line-regexp "${resource_test}: test"
cargo test --package tenon --lib --locked -- "${resource_test}" --exact --test-threads=1
# Save the actual default executable before enabling repository-only controls.
verification_directory="$(mktemp -d "${TMPDIR:-/tmp}/tenon-default-runner.XXXXXX")"
trap 'rm -rf "${verification_directory}"' EXIT
cargo build --package tenon --bin tenon --locked --message-format=json |
  python3 -c 'import json, shutil, sys
for line in sys.stdin:
    message = json.loads(line)
    if (message.get("reason") == "compiler-artifact"
            and message["target"]["name"] == "tenon" and message.get("executable")):
        shutil.copy2(message["executable"], sys.argv[1])' "${verification_directory}/tenon"
test -x "${verification_directory}/tenon"
# Generate author projects and run their bundles with the default Runner.
TENON_TEST_RUNNER_BINARY="${verification_directory}/tenon" python3 tools/verify-rust-scaffold.py
python3 tools/validate-rust-test-layout.py --compile-boundary

feature_test_targets() {
  local manifest="$1" package="$2"
  local feature_test_names
  feature_test_names="$(cargo metadata --manifest-path "${manifest}" --no-deps --format-version 1 --locked | python3 -c '
import json, sys
for package in json.load(sys.stdin)["packages"]:
    if package["name"] == sys.argv[1]:
        for target in package["targets"]:
            if "test" in target["kind"] and "repository-test-support" in target.get("required-features", []):
                print(target["name"])' "${package}")"
  [[ -n "${feature_test_names}" ]]
  feature_targets=()
  while IFS= read -r target; do
    feature_targets+=(--test "${target}")
  done <<<"${feature_test_names}"
}

feature_test_targets Cargo.toml tenon-ipc
cargo clippy --package tenon-ipc "${feature_targets[@]}" \
  --features repository-test-support --locked -- -D warnings
cargo test --package tenon-ipc "${feature_targets[@]}" \
  --features repository-test-support --locked -- --test-threads=1

feature_test_targets Cargo.toml tenon
cargo clippy --package tenon "${feature_targets[@]}" --bin runner-extension-fixture \
  --features repository-test-support --locked -- -D warnings
cargo test --package tenon "${feature_targets[@]}" \
  --features repository-test-support --locked -- --test-threads=1
# Exercise the real MQTT Source worker against the SDK admission boundary.
feature_test_targets Cargo.toml bifromq-tenon-mqtt-plugin
cargo clippy --package bifromq-tenon-mqtt-plugin "${feature_targets[@]}" --example mqtt-source-process-fixture \
  --features repository-test-support --locked -- -D warnings
mqtt_source_fixture="$(cargo build --package bifromq-tenon-mqtt-plugin --example mqtt-source-process-fixture \
  --features repository-test-support --locked --message-format=json | python3 -c '
import json, sys
for line in sys.stdin:
    message = json.loads(line)
    if message.get("reason") == "compiler-artifact" and message["target"]["name"] == "mqtt-source-process-fixture" and message.get("executable"):
        print(message["executable"])')"
test -x "${mqtt_source_fixture}"
TENON_TEST_MQTT_SOURCE_BINARY="${mqtt_source_fixture}" \
  cargo test --package bifromq-tenon-mqtt-plugin "${feature_targets[@]}" \
  --features repository-test-support --locked -- --test-threads=1
default_runner_test="ready_document_runs_plugins_restarts_after_pipeline_crash_and_stops_cleanly"
cargo test --package tenon --test runner_cli --features repository-test-support --locked -- \
  "${default_runner_test}" --exact --list --format terse |
  grep --fixed-strings --line-regexp "${default_runner_test}: test"
TENON_TEST_RUNNER_BINARY="${verification_directory}/tenon" \
  cargo test --package tenon --test runner_cli --features repository-test-support --locked -- \
  "${default_runner_test}" --exact --test-threads=1

# Link a real business entrypoint against the SDK's default library before
# enabling its test exports. The fixture source uses only the author API.
cargo build --manifest-path sdk/rust/Cargo.toml --package tenon-plugin-sdk --lib --locked --message-format=json |
  python3 -c 'import json, subprocess, sys
from pathlib import Path
for line in sys.stdin:
    message = json.loads(line)
    if (message.get("reason") == "compiler-artifact" and message["target"]["name"] == "tenon_plugin_sdk"):
        library = next(Path(path) for path in message["filenames"] if path.endswith(".rlib"))
        dependencies = library.parent / "deps"
        for capability in ["source", "sink", "source_and_sink"]:
            subprocess.run(["rustc", "--edition=2024", f"sdk/rust/plugin-sdk/tests/support/{capability}_process_fixture.rs",
                            "--extern", f"tenon_plugin_sdk={library}", "-L", f"dependency={dependencies}",
                            "-o", str(Path(sys.argv[1]) / capability)], check=True)' "${verification_directory}"
test -x "${verification_directory}/source"
test -x "${verification_directory}/sink"
test -x "${verification_directory}/source_and_sink"
feature_test_targets sdk/rust/Cargo.toml tenon-plugin-sdk
cargo clippy --manifest-path sdk/rust/Cargo.toml --package tenon-plugin-sdk \
  "${feature_targets[@]}" --bin source-process-fixture --bin sink-process-fixture --bin source-and-sink-process-fixture \
  --features repository-test-support --locked -- -D warnings
cargo test --manifest-path sdk/rust/Cargo.toml --package tenon-plugin-sdk \
  "${feature_targets[@]}" --features repository-test-support --locked -- --test-threads=1
# An exact filter that no longer exists must fail instead of silently running zero tests.
for smoke in \
  source_process:one_source_preserves_completion_and_cleanup \
  sink_process:normal_sink_process_releases_each_queue_and_closes_one_owner \
  source_and_sink_process:one_shared_owner_keeps_sink_and_completion_alive_through_source_quiesce; do
  target="${smoke%%:*}"
  name="${smoke#*:}"
  cargo test --manifest-path sdk/rust/Cargo.toml --package tenon-plugin-sdk \
    --test "${target}" --features repository-test-support --locked -- "${name}" --exact --list --format terse |
    grep --fixed-strings --line-regexp "${name}: test"
done
TENON_TEST_SOURCE_BINARY="${verification_directory}/source" \
  cargo test --manifest-path sdk/rust/Cargo.toml --package tenon-plugin-sdk \
  --test source_process --features repository-test-support --locked -- \
  one_source_preserves_completion_and_cleanup --exact --test-threads=1
TENON_TEST_SINK_BINARY="${verification_directory}/sink" \
  cargo test --manifest-path sdk/rust/Cargo.toml --package tenon-plugin-sdk \
  --test sink_process --features repository-test-support --locked -- \
  normal_sink_process_releases_each_queue_and_closes_one_owner --exact --test-threads=1
TENON_TEST_SOURCE_AND_SINK_BINARY="${verification_directory}/source_and_sink" \
  cargo test --manifest-path sdk/rust/Cargo.toml --package tenon-plugin-sdk \
  --test source_and_sink_process --features repository-test-support --locked -- \
  one_shared_owner_keeps_sink_and_completion_alive_through_source_quiesce --exact --test-threads=1
TENON_TEST_RUST_SINK_BINARY="${verification_directory}/sink" \
TENON_TEST_RUST_SOURCE_AND_SINK_BINARY="${verification_directory}/source_and_sink" \
  sdk/java/mvnw --batch-mode --no-transfer-progress --file sdk/java/pom.xml \
  -Dmaven.repo.local="${repo_root}/target/verify-java-maven-repository" \
  -pl plugin-sdk -am -Dtest=PluginLifecycleIntegrationTest#rustSinkConsumesJavaEgressAndClosesTheJavaControlSession+rustSharedOwnerExchangesBothQueueDirectionsWithJava \
  -Dsurefire.failIfNoSpecifiedTests=false test

run_loom_tests() {
  local test_filter="$1"
  cargo test --package tenon --lib --features loom-model --locked -- "${test_filter}" --list |
    grep --fixed-strings "${test_filter}::"
  # Each Loom model owns one simulated scheduler; libtest must not overlap model runs.
  cargo test --package tenon --lib --features loom-model --locked -- "${test_filter}" --test-threads=1
}

run_loom_tests pipeline::runtime::loom_tests
run_loom_tests pipeline::channel::flow_control::loom_tests
for manifest in "${workspace_manifests[@]}"; do
  cargo test --manifest-path "${manifest}" --workspace --doc --locked
  RUSTDOCFLAGS="-D warnings" cargo doc \
    --manifest-path "${manifest}" \
    --workspace \
    --no-deps \
    --document-private-items \
    --locked
done
