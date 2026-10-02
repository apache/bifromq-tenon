/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::env;
use std::ffi::OsString;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::num::{NonZeroU32, NonZeroU64};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use base64::Engine as _;
use flate2::read::GzDecoder;
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use tar::Archive;
use tenon::runner_test_support::contracts::sink::EgressRecord;
use tenon::runner_test_support::contracts::source::{
    IngressCompletion, IngressCompletionStatus, IngressRecord,
};
use tenon::runner_test_support::ingress_queue::{
    COMPLETION_MAX_PAYLOAD_SIZE, completion_capacity, submission_capacity,
};
use tenon::runner_test_support::loops_bell_path;
use tenon::runner_test_support::plugin_program::{
    PluginProgramLaunch, force_stop_sink_program, run_sink_program,
    run_sink_program_with_control_stream_loss, run_sink_program_with_owner_loss,
    run_source_and_sink_program, run_source_program,
};
use tenon_ipc::bell::{BellError, BellRegion, create_bell_region};
use tenon_ipc::queue::contract_test_support::{open_reader, open_writer};
use tenon_ipc::queue::{DataCapacity, HEADER_LEN, Header};
use tenon_ipc::queue::{
    QueueReader, QueueWriter, ReadOutcome, WriteOutcome, WriteReceipt, create_queue_file,
};

#[path = "support/runner_http.rs"]
mod runner_http_support;
#[path = "runner_java_archetype/topology.rs"]
mod topology;

use runner_http_support::{
    TestRunner, available_address, request, sha256_hex, wait_for_http, wait_until, write_config,
};

#[test]
fn generated_java_plugins_enforce_delivery_boundaries_without_system_java() -> io::Result<()> {
    let (packages, pending_cache, _cache_guard) = prepare_generated_java_plugins()?;
    exercise_generated_java_plugins(&packages)?;
    run_generated_java_pipeline(&packages.source, &packages.sink, DeliveryScenario::Success)?;
    run_generated_java_pipeline(
        &packages.source,
        &packages.gated_sink,
        DeliveryScenario::DelayedSinkSuccess,
    )?;
    run_generated_java_pipeline(
        &packages.capacity_source,
        &packages.gated_sink,
        DeliveryScenario::CapacitySaturation,
    )?;
    run_generated_java_pipeline(
        &packages.source,
        &packages.replay_sink,
        DeliveryScenario::ReplayAfterFailure,
    )?;
    verify_external_java_packages(&packages)?;
    topology::run_dual_interface_routes(&packages.source_and_sink)?;
    topology::run_disjoint_pressure_routes(&packages)?;
    write_java_bundle_contract_evidence(&packages)?;
    if let Some(pending_cache) = pending_cache {
        pending_cache.publish()?;
    }
    Ok(())
}

#[test]
fn java_build_input_hash_tracks_sources_and_ignores_build_outputs() -> io::Result<()> {
    let workspace = tempfile::tempdir()?;
    let inputs = workspace.path().join("inputs");
    fs::create_dir_all(inputs.join("src"))?;
    fs::write(inputs.join("src/Main.java"), b"first")?;
    let original = input_tree_digest(workspace.path(), Path::new("inputs"))?;

    fs::write(inputs.join("src/Main.java"), b"second")?;
    let changed = input_tree_digest(workspace.path(), Path::new("inputs"))?;
    assert_ne!(original, changed);

    fs::create_dir_all(inputs.join("target"))?;
    fs::write(inputs.join("target/generated.class"), b"ignored")?;
    assert_eq!(
        changed,
        input_tree_digest(workspace.path(), Path::new("inputs"))?
    );
    Ok(())
}

#[test]
fn generated_java_package_cache_requires_every_bundle() -> io::Result<()> {
    let cache_root = tempfile::tempdir()?;
    let mut destination = cache_root.path().join("a".repeat(64));
    let packages = GeneratedJavaPackages {
        source: b"source".to_vec(),
        capacity_source: b"capacity-source".to_vec(),
        sink: b"sink".to_vec(),
        replay_sink: b"replay-sink".to_vec(),
        gated_sink: b"gated-sink".to_vec(),
        source_and_sink: b"source-and-sink".to_vec(),
    };
    let pending =
        PendingJavaPackageCache::stage(cache_root.path(), destination.clone(), &packages)?;
    assert!(!destination.exists());
    pending.publish()?;
    assert_eq!(packages, GeneratedJavaPackages::read_from(&destination)?);

    let cancelled = cache_root.path().join("b".repeat(64));
    drop(PendingJavaPackageCache::stage(
        cache_root.path(),
        cancelled.clone(),
        &packages,
    )?);
    assert!(!cancelled.exists());
    assert_eq!(packages, GeneratedJavaPackages::read_from(&destination)?);
    fs::write(cache_root.path().join("notes.txt"), b"retained evidence")?;
    for version in ["b", "c"] {
        let next = cache_root.path().join(version.repeat(64));
        let pending = PendingJavaPackageCache::stage(cache_root.path(), next.clone(), &packages)?;
        assert!(destination.exists());
        pending.publish()?;
        assert!(
            !destination.exists(),
            "Superseded Java bundles were retained"
        );
        destination = next;
        assert_eq!(packages, GeneratedJavaPackages::read_from(&destination)?);
    }
    assert!(cache_root.path().join("notes.txt").exists());

    fs::remove_file(destination.join("gated-sink.tar.gz"))?;
    let Err(error) = GeneratedJavaPackages::read_from(&destination) else {
        return Err(io::Error::other(
            "Incomplete Java bundle cache was accepted",
        ));
    };
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    Ok(())
}

#[test]
fn isolated_maven_repository_keeps_dependencies_but_removes_tenon_artifacts() -> io::Result<()> {
    let cache_root = tempfile::tempdir()?;
    let repository = cache_root.path().join("maven-repository");
    fs::create_dir_all(repository.join("org/apache/bifromq/tenon/old-artifact"))?;
    fs::create_dir_all(repository.join("com/google/protobuf"))?;
    fs::write(repository.join("archetype-catalog.xml"), b"stale")?;

    assert_eq!(
        repository,
        prepare_isolated_maven_repository(cache_root.path())?
    );
    assert!(!repository.join("org/apache/bifromq/tenon").exists());
    assert!(!repository.join("archetype-catalog.xml").exists());
    assert!(repository.join("com/google/protobuf").is_dir());
    Ok(())
}

fn run_generated_java_pipeline(
    source_package: &[u8],
    sink_package: &[u8],
    scenario: DeliveryScenario,
) -> io::Result<()> {
    let java_free_path = tempfile::tempdir()?;
    run_generated_java_pipeline_with_path(
        source_package,
        sink_package,
        scenario,
        java_free_path.path(),
    )
}

fn run_generated_java_pipeline_with_path(
    source_package: &[u8],
    sink_package: &[u8],
    scenario: DeliveryScenario,
    executable_path: &Path,
) -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let address = available_address()?;
    let config = write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn_with_path(&config, executable_path)?;
    wait_for_http(&mut runner, address)?;

    let output = directory.path().join("received.txt");
    let document_id = scenario.document_id();
    eprintln!("Verifying generated Java delivery: {document_id}");
    let document = generated_java_document(document_id, &output, scenario)?;
    let document_path = format!("/documents/{document_id}");
    let created = request(
        address,
        "PUT",
        &document_path,
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        document.as_bytes(),
    )?;
    assert_eq!(created.status, 201, "{}", created.body_text());
    let mut source_diagnostics = subscribe_to_diagnostics(address, document_id, "plugin%3Asource")?;
    let mut sink_diagnostics = scenario
        .captures_sink_diagnostics()
        .then(|| subscribe_to_diagnostics(address, document_id, "plugin%3Aprimary"))
        .transpose()?;

    let installed_source = request(
        address,
        "POST",
        "/plugins",
        &[(
            "Content-Type",
            "application/vnd.apache.tenon.plugin+tar+gzip",
        )],
        source_package,
    )?;
    assert_eq!(
        installed_source.status,
        201,
        "{}",
        installed_source.body_text()
    );

    let installed_sink = request(
        address,
        "POST",
        "/plugins",
        &[(
            "Content-Type",
            "application/vnd.apache.tenon.plugin+tar+gzip",
        )],
        sink_package,
    )?;
    assert_eq!(installed_sink.status, 201, "{}", installed_sink.body_text());

    let pipeline_path = format!("/pipelines/{document_id}");
    wait_until(|| {
        let response = request(address, "GET", &pipeline_path, &[], &[])?;
        Ok((response.json()["state"] == "running").then_some(()))
    })?;
    match scenario {
        DeliveryScenario::DelayedSinkSuccess => {
            verify_sink_holds_source_completion(directory.path(), &document, &output)?;
            fs::write(path_with_suffix(&output, ".write-release"), b"release\n")?;
        }
        DeliveryScenario::CapacitySaturation => {
            verify_sink_holds_source_completion(directory.path(), &document, &output)?;
            wait_for_source_results(&output, "backpressure\n")?;
            fs::write(path_with_suffix(&output, ".write-release"), b"release\n")?;
        }
        DeliveryScenario::ReplayAfterFailure => {
            // Sending Ready is asynchronous. Inject the runtime failure only
            // after Pipeline has actually classified this Instance as running.
            wait_until(|| {
                let status = request(address, "GET", &pipeline_path, &[], &[])?.json();
                let ready = status["pluginInstances"]
                    .as_array()
                    .is_some_and(|instances| {
                        instances.iter().any(|instance| {
                            instance["id"] == "primary" && instance["state"] == "running"
                        })
                    });
                Ok((ready && path_with_suffix(&output, ".failed-once").is_file()).then_some(()))
            })?;
            assert_eq!(
                fs::read_to_string(&output)?,
                DeliveryScenario::Success.expected_output()
            );
            assert_eq!(
                fs::read_to_string(path_with_suffix(&output, ".starts"))?,
                "start\n"
            );
            let queues = java_queue_paths(directory.path(), &document)?
                .ok_or_else(|| io::Error::other("Replay Queue paths are missing"))?;
            let sink = required_queue_progress(&queues.sink)?;
            assert!(sink.commit > 0 && sink.release == 0, "{sink:?}");
            assert_eq!(required_queue_progress(&queues.completion)?.commit, 0);
            fs::write(path_with_suffix(&output, ".fail-release"), b"release\n")?;
        }
        DeliveryScenario::Success => {}
    }
    wait_for_java_round_trip(directory.path(), &document, &output, scenario)?;
    if matches!(scenario, DeliveryScenario::ReplayAfterFailure) {
        assert!(
            !path_with_suffix(&output, ".closes").try_exists()?,
            "The failed Sink must exit without business cleanup"
        );
    }
    verify_source_diagnostic(&mut source_diagnostics)?;
    if matches!(scenario, DeliveryScenario::CapacitySaturation) {
        wait_for_source_results(&output, "backpressure\nok\n")?;
    }
    if let Some(diagnostics) = sink_diagnostics.as_mut() {
        verify_sink_diagnostic(diagnostics)?;
    }

    let source_entry = request(
        address,
        "GET",
        &format!(
            "/plugins/com.example.plugin.source/{}",
            scenario.source_exact_version()
        ),
        &[],
        &[],
    )?;
    assert_eq!(source_entry.status, 200, "{}", source_entry.body_text());
    assert_eq!(source_entry.json()["interface"], "source");
    assert_eq!(
        source_entry.json()["platforms"],
        generated_bundle_manifest(source_package)?["platforms"]
    );

    let sink_path = format!(
        "/plugins/com.example.plugin.sink/{}",
        scenario.sink_exact_version()
    );
    let sink_entry = request(address, "GET", &sink_path, &[], &[])?;
    assert_eq!(sink_entry.status, 200, "{}", sink_entry.body_text());
    assert_eq!(sink_entry.json()["interface"], "sink");
    assert_eq!(
        sink_entry.json()["platforms"],
        generated_bundle_manifest(sink_package)?["platforms"]
    );

    drop(source_diagnostics);
    drop(sink_diagnostics);
    if matches!(scenario, DeliveryScenario::Success) {
        verify_java_plugin_metrics(address)?;
    }
    runner.terminate()?;
    scenario.verify_shutdown(&output)
}

fn verify_java_plugin_metrics(address: std::net::SocketAddr) -> io::Result<()> {
    wait_until(|| {
        let response = request(
            address,
            "GET",
            "/metrics?include=tenon.plugin.state",
            &[],
            &[],
        )?;
        assert_eq!(response.status, 200, "{}", response.body_text());
        let body = response.json();
        let processes = body["processes"]
            .as_array()
            .ok_or_else(|| io::Error::other("Missing metric processes"))?;
        for metric in processes
            .iter()
            .flat_map(|process| process["metrics"].as_array().into_iter().flatten())
        {
            if metric["name"] != "tenon.plugin.state" {
                continue;
            }
            let points = metric["points"]
                .as_array()
                .ok_or_else(|| io::Error::other("Missing plugin states"))?;
            let mut ids = Vec::new();
            for point in points {
                if point["value"] != "1" {
                    return Ok(None);
                }
                let attributes = point["attributes"]
                    .as_object()
                    .ok_or_else(|| io::Error::other("Missing state attributes"))?;
                assert_eq!(attributes.len(), 1);
                if let Some(id) = attributes
                    .get("tenon.plugin.instance.id")
                    .and_then(serde_json::Value::as_str)
                {
                    ids.push(id);
                }
            }
            ids.sort_unstable();
            if ids == ["primary", "source"] {
                return Ok(Some(()));
            }
        }
        Ok(None)
    })?;
    verify_java_process_resources(address, &["primary", "source"])
}

fn verify_java_process_resources(
    address: std::net::SocketAddr,
    expected: &[&str],
) -> io::Result<()> {
    wait_until(|| {
        let response = request(
            address,
            "GET",
            "/metrics?include=tenon.plugin.cpu,tenon.plugin.memory",
            &[],
            &[],
        )?;
        assert_eq!(response.status, 200);
        let body = response.json();
        let processes = body["processes"]
            .as_array()
            .ok_or_else(|| io::Error::other("Missing processes"))?;
        let mut ids = Vec::new();
        for process in processes {
            assert_eq!(process["resource"]["service.name"], "tenon.plugin");
            let metrics = process["metrics"]
                .as_array()
                .ok_or_else(|| io::Error::other("Missing metrics"))?;
            if metrics.len() != 2 {
                return Ok(None);
            }
            for metric in metrics {
                let points = metric["points"]
                    .as_array()
                    .ok_or_else(|| io::Error::other("Missing points"))?;
                assert_eq!(points.len(), 1);
                if metric["name"] == "tenon.plugin.memory" {
                    assert_eq!(metric["unit"], "By");
                    assert!(
                        points[0]["value"]
                            .as_str()
                            .and_then(|v| v.parse::<u64>().ok())
                            .is_some_and(|v| v > 0)
                    );
                } else {
                    assert_eq!(metric["name"], "tenon.plugin.cpu");
                    assert_eq!(metric["unit"], "1");
                }
            }
            ids.push(
                process["resource"]["tenon.plugin.instance.id"]
                    .as_str()
                    .unwrap_or(""),
            );
        }
        ids.sort_unstable();
        Ok((ids == expected).then_some(()))
    })
}

fn generated_java_document(
    id: &str,
    output: &Path,
    scenario: DeliveryScenario,
) -> io::Result<String> {
    let output = output
        .to_str()
        .ok_or_else(|| io::Error::other("Generated Sink output path is not UTF-8"))?;
    let pending_records = match scenario {
        DeliveryScenario::CapacitySaturation => 1,
        _ => 4,
    };
    let sink_exact_version = scenario.sink_exact_version();
    let sink_contract_id = format!("com.example.plugin.sink@{sink_exact_version}");
    let source_config = match scenario {
        DeliveryScenario::CapacitySaturation => serde_json::json!({
            "message": "hello from generated Java Source",
            "queueIndex": 0,
            "resultFile": format!("{output}.source-results")
        }),
        DeliveryScenario::Success
        | DeliveryScenario::DelayedSinkSuccess
        | DeliveryScenario::ReplayAfterFailure => serde_json::json!({
            "message": "hello from generated Java Source",
            "queueIndex": 0
        }),
    };
    Ok(serde_json::json!({
        "specVersion": "1",
        "id": id,
        "pluginInstances": {
            "source": {
                "programName": "com.example.plugin.source",
                "exactVersion": scenario.source_exact_version(),
                "config": source_config
            },
            "primary": {
                "programName": "com.example.plugin.sink",
                "exactVersion": sink_exact_version,
                "config": {"outputFile": output}
            }
        },
        "flows": {
            "main": {
                "source": "source",
                "maxPendingRecords": pending_records,
                "maxRecordBytes": 1024,
                "process": {
                    "script": format!("local builder = registry:getBuilder(\"{sink_contract_id}\")\nfunction main(event)\n  builder:setMessage(event.payload.message)\n  emit(builder:build())\nend")
                },
                "sinks": ["primary"]
            }
        }
    })
    .to_string())
}

fn prepare_generated_java_plugins() -> io::Result<(
    GeneratedJavaPackages,
    Option<PendingJavaPackageCache>,
    fs::File,
)> {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"));
    let java_workspace = workspace.join("sdk/java");
    let classifier = java_plugin_platform_classifier()?;
    let cache_root = cargo_target_directory(workspace).join("runner-java-archetype");
    fs::create_dir_all(&cache_root)?;
    let cache_guard = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(cache_root.join(".lock"))?;
    cache_guard.lock()?;
    let package_cache_root = cache_root.join("packages");
    let cache_directory = package_cache_root.join(java_build_fingerprint(workspace, classifier)?);
    if cache_directory.try_exists()? {
        let started = Instant::now();
        match GeneratedJavaPackages::read_from(&cache_directory) {
            Ok(packages) => {
                retain_java_package_cache(&cache_directory)?;
                eprintln!(
                    "Reused verified Java Plugin bundles in {:.1?}",
                    started.elapsed()
                );
                return Ok((packages, None, cache_guard));
            }
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                fs::remove_dir_all(&cache_directory)?;
            }
            Err(source) => return Err(source),
        }
    }

    let started = Instant::now();
    let local_repository = prepare_isolated_maven_repository(&cache_root)?;
    let packages = build_generated_java_plugins_uncached(
        workspace,
        &java_workspace,
        &local_repository,
        classifier,
    )?;
    let pending_cache =
        PendingJavaPackageCache::stage(&package_cache_root, cache_directory, &packages)?;
    eprintln!(
        "Built and cached verified Java Plugin bundles in {:.1?}",
        started.elapsed()
    );
    Ok((packages, Some(pending_cache), cache_guard))
}

fn retain_java_package_cache(destination: &Path) -> io::Result<()> {
    let root = destination
        .parent()
        .ok_or_else(|| io::Error::other("Java package cache has no parent"))?;
    for entry in root.read_dir()? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let generated = (name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit()))
            || name.starts_with("runner-java-archetype-");
        if entry.path() != destination && generated && entry.file_type()?.is_dir() {
            fs::remove_dir_all(entry.path()).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "Cannot remove superseded Java package cache {}: {error}",
                        entry.path().display()
                    ),
                )
            })?;
        }
    }
    Ok(())
}

fn build_generated_java_plugins_uncached(
    workspace: &Path,
    java_workspace: &Path,
    local_repository: &Path,
    classifier: &str,
) -> io::Result<GeneratedJavaPackages> {
    let mut local_repository_argument = OsString::from("-Dmaven.repo.local=");
    local_repository_argument.push(local_repository);
    let mut command = Command::new(java_workspace.join("mvnw"));
    command
        .current_dir(java_workspace)
        .arg(&local_repository_argument)
        .args([
            "--batch-mode",
            "--no-transfer-progress",
            "-pl",
            ".,:tenon-ipc,:tenon-process-metrics,plugin-sdk,maven-plugin,plugin-archetype",
            "clean",
            "install",
        ]);
    let output = command.output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "Java Plugin Archetype verification failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )));
    }
    verify_java_release_repository(local_repository)?;
    let source_bundle_name = format!(
        "generated-plugin-source-1.0.0-tenon-plugin-{}.tar.gz",
        classifier
    );
    let sink_bundle_name = format!(
        "generated-plugin-sink-1.0.0-tenon-plugin-{}.tar.gz",
        classifier
    );
    let source_project = java_workspace.join(
        "plugin-archetype/target/test-classes/projects/source/project/generated-plugin-source",
    );
    add_generated_source_diagnostic(&source_project)?;
    build_generated_project(
        &source_project,
        &local_repository_argument,
        "Generated Java Source diagnostic fixture",
    )?;
    let source_bundle = source_project.join("target").join(source_bundle_name);
    let sink_project = java_workspace
        .join("plugin-archetype/target/test-classes/projects/sink/project/generated-plugin-sink");
    let sink_bundle = sink_project.join("target").join(&sink_bundle_name);
    let source = fs::read(source_bundle)?;
    let sink = fs::read(&sink_bundle)?;

    fs::copy(
        workspace.join("tests/fixtures/java-source-capacity/SourcePluginFactory.java"),
        source_project.join("src/main/java/com/example/plugin/source/SourcePluginFactory.java"),
    )?;
    fs::copy(
        workspace.join("tests/fixtures/java-source-capacity/config.schema.json"),
        source_project.join("src/main/tenon/config.schema.json"),
    )?;
    replace_generated_project_version(&source_project, "1.0.0", "1.0.1")?;
    build_generated_project(
        &source_project,
        &local_repository_argument,
        "Generated Java Source capacity fixture",
    )?;
    let capacity_bundle_name =
        format!("generated-plugin-source-1.0.1-tenon-plugin-{classifier}.tar.gz");
    let capacity_source = fs::read(source_project.join("target").join(capacity_bundle_name))?;

    fs::copy(
        workspace.join("tests/fixtures/java-sink-replay/SinkPluginFactory.java"),
        sink_project.join("src/main/java/com/example/plugin/sink/SinkPluginFactory.java"),
    )?;
    replace_generated_project_version(&sink_project, "1.0.0", "1.0.1")?;
    build_generated_project(
        &sink_project,
        &local_repository_argument,
        "Generated Java Sink replay fixture",
    )?;

    let replay_bundle_name =
        format!("generated-plugin-sink-1.0.1-tenon-plugin-{classifier}.tar.gz");
    let replay_bundle = sink_project.join("target").join(replay_bundle_name);
    let replay_sink = fs::read(replay_bundle)?;

    fs::copy(
        workspace.join("tests/fixtures/java-sink-gated/SinkPluginFactory.java"),
        sink_project.join("src/main/java/com/example/plugin/sink/SinkPluginFactory.java"),
    )?;
    replace_generated_project_version(&sink_project, "1.0.1", "1.0.2")?;
    build_generated_project(
        &sink_project,
        &local_repository_argument,
        "Generated Java Sink success-boundary fixture",
    )?;

    let gated_bundle_name = format!("generated-plugin-sink-1.0.2-tenon-plugin-{classifier}.tar.gz");
    let gated_bundle = sink_project.join("target").join(gated_bundle_name);
    let generated_projects = java_workspace.join("plugin-archetype/target/test-classes/projects");
    let source_and_sink = fs::read(
        generated_projects
            .join("source-and-sink/project/generated-plugin-source-and-sink/target")
            .join(format!(
                "generated-plugin-source-and-sink-1.0.0-tenon-plugin-{classifier}.tar.gz"
            )),
    )?;
    Ok(GeneratedJavaPackages {
        source,
        capacity_source,
        sink,
        replay_sink,
        gated_sink: fs::read(gated_bundle)?,
        source_and_sink,
    })
}

fn prepare_isolated_maven_repository(cache_root: &Path) -> io::Result<PathBuf> {
    let local_repository = cache_root.join("maven-repository");
    fs::create_dir_all(&local_repository)?;
    remove_directory_if_present(local_repository.join("org/apache/bifromq/tenon"))?;
    remove_file_if_present(local_repository.join("archetype-catalog.xml"))?;
    Ok(local_repository)
}

fn remove_directory_if_present(path: PathBuf) -> io::Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(source),
    }
}

fn remove_file_if_present(path: PathBuf) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(source),
    }
}

fn cargo_target_directory(workspace: &Path) -> PathBuf {
    env::var_os("CARGO_TARGET_DIR").map_or_else(
        || workspace.join("target"),
        |configured| {
            let configured = PathBuf::from(configured);
            if configured.is_absolute() {
                configured
            } else {
                workspace.join(configured)
            }
        },
    )
}

fn java_build_fingerprint(workspace: &Path, classifier: &str) -> io::Result<String> {
    let mut digest = Sha256::new();
    hash_field(&mut digest, "cache-format", b"1");
    hash_field(&mut digest, "platform", classifier.as_bytes());
    hash_field(
        &mut digest,
        "package-version",
        env!("CARGO_PKG_VERSION").as_bytes(),
    );
    hash_field(
        &mut digest,
        "test-builder",
        include_bytes!("runner_java_archetype.rs"),
    );
    for relative in [
        "sdk/java",
        "contracts/ipc",
        "contracts/source",
        "contracts/sink",
        "contracts/plugin",
        "tests/fixtures/java-source-capacity",
        "tests/fixtures/java-sink-replay",
        "tests/fixtures/java-sink-gated",
    ] {
        hash_input_tree(&mut digest, workspace, Path::new(relative))?;
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn input_tree_digest(workspace: &Path, relative: &Path) -> io::Result<Vec<u8>> {
    let mut digest = Sha256::new();
    hash_input_tree(&mut digest, workspace, relative)?;
    Ok(digest.finalize().to_vec())
}

fn hash_input_tree(digest: &mut Sha256, workspace: &Path, relative: &Path) -> io::Result<()> {
    let root = workspace.join(relative);
    let mut files = Vec::new();
    collect_build_inputs(&root, &mut files)?;
    files.sort_unstable();
    for path in files {
        let relative_path = relative.join(path.strip_prefix(&root).map_err(io::Error::other)?);
        let relative_bytes = relative_path.to_string_lossy();
        hash_field(digest, "path", relative_bytes.as_bytes());
        hash_field(digest, "content", &fs::read(path)?);
    }
    Ok(())
}

fn collect_build_inputs(path: &Path, files: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in path.read_dir()? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if file_type.is_symlink() {
            return Err(io::Error::other(format!(
                "Java build input must not be a symbolic link: {}",
                path.display()
            )));
        }
        if file_type.is_dir() {
            if matches!(file_name.as_ref(), "target" | ".idea") {
                continue;
            }
            collect_build_inputs(&path, files)?;
        } else if file_type.is_file()
            && file_name != ".flattened-pom.xml"
            && file_name != "dependency-reduced-pom.xml"
            && !file_name.ends_with(".iml")
        {
            files.push(path);
        }
    }
    Ok(())
}

fn hash_field(digest: &mut Sha256, label: &str, bytes: &[u8]) {
    digest.update(label.len().to_le_bytes());
    digest.update(label.as_bytes());
    digest.update(bytes.len().to_le_bytes());
    digest.update(bytes);
}

fn add_generated_source_diagnostic(project: &Path) -> io::Result<()> {
    let path = project.join("src/main/java/com/example/plugin/source/SourcePluginFactory.java");
    let source = fs::read_to_string(&path)?;
    let anchor = "        case OK -> {}\n";
    if source.matches(anchor).count() != 1 {
        return Err(io::Error::other(
            "Generated Source did not contain exactly one successful acknowledgement branch",
        ));
    }
    fs::write(
        path,
        source.replacen(
            anchor,
            "        case OK -> System.err.println(\"Generated Source payload acknowledged\");\n",
            1,
        ),
    )
}

fn build_generated_project(
    project: &Path,
    local_repository_argument: &OsString,
    context: &str,
) -> io::Result<()> {
    let output = Command::new(project.join("mvnw"))
        .current_dir(project)
        .arg(local_repository_argument)
        .args([
            "--batch-mode",
            "--no-transfer-progress",
            "-DskipTests",
            "clean",
            "verify",
        ])
        .output()?;
    if output.status.success() {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "{context} failed to package\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )))
}

fn verify_java_release_repository(local_repository: &Path) -> io::Result<()> {
    const RELEASE_ARTIFACTS: [&str; 5] = [
        "tenon-ipc",
        "tenon-maven-plugin",
        "tenon-plugin-archetype",
        "tenon-plugin-sdk",
        "tenon-process-metrics",
    ];

    let group_directory = local_repository.join("org/apache/bifromq/tenon");
    let mut actual = Vec::new();
    for entry in group_directory.read_dir()? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            actual.push(entry.file_name().to_string_lossy().into_owned());
        }
    }
    actual.sort_unstable();
    if actual != RELEASE_ARTIFACTS {
        return Err(io::Error::other(format!(
            "Java release repository must contain exactly five public artifacts; observed {actual:?}"
        )));
    }

    let version = env!("CARGO_PKG_VERSION");
    for artifact in RELEASE_ARTIFACTS {
        let pom = fs::read_to_string(
            group_directory
                .join(artifact)
                .join(version)
                .join(format!("{artifact}-{version}.pom")),
        )?;
        if pom.contains("<parent>") || pom.contains("tenon-java-build") {
            return Err(io::Error::other(format!(
                "Published {artifact} POM still depends on a private Java build module"
            )));
        }
        if pom.contains("<classifier>tests</classifier>") {
            return Err(io::Error::other(format!(
                "Published {artifact} POM still depends on test fixtures"
            )));
        }
        if artifact == "tenon-plugin-sdk" && !pom.contains("<artifactId>tenon-ipc</artifactId>") {
            return Err(io::Error::other(
                "Published SDK POM must retain its IPC dependency",
            ));
        }
    }
    Ok(())
}

fn replace_generated_project_version(
    project: &Path,
    current_version: &str,
    next_version: &str,
) -> io::Result<()> {
    let pom_path = project.join("pom.xml");
    let pom = fs::read_to_string(&pom_path)?;
    let current = format!("    <version>{current_version}</version>\n\n    <properties>");
    let next = format!("    <version>{next_version}</version>\n\n    <properties>");
    if pom.matches(&current).count() != 1 {
        return Err(io::Error::other(format!(
            "Generated Plugin POM did not contain exactly one project version {current_version}"
        )));
    }
    fs::write(pom_path, pom.replacen(&current, &next, 1))
}

fn exercise_generated_java_plugins(packages: &GeneratedJavaPackages) -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        run_generated_source_program(&packages.source).await?;
        run_generated_sink_program(&packages.sink).await?;
        run_generated_source_and_sink_program(&packages.source_and_sink).await?;
        run_generated_sink_failure_boundaries(&packages.sink).await
    })
}

async fn run_generated_source_program(bundle: &[u8]) -> io::Result<()> {
    let (root, program_directory, command) = extract_generated_program(bundle)?;
    let working_directory = root.path().join("working");
    fs::create_dir(&working_directory)?;
    let (submission, completion) = create_source_queue_pair(&working_directory)?;
    let config = serde_json::json!({
        "message": "source-through-rust",
        "queueIndex": 0,
    });
    run_source_program(
        PluginProgramLaunch::new(
            &program_directory,
            &command,
            working_directory.clone(),
            &config,
            Vec::new(),
        )
        .map_err(io::Error::other)?,
        || {
            complete_source_round_trip(
                &working_directory,
                &submission,
                &completion,
                "source-through-rust",
            )
        },
    )
    .await
}

async fn run_generated_sink_program(bundle: &[u8]) -> io::Result<()> {
    let (root, program_directory, command) = extract_generated_program(bundle)?;
    let working_directory = root.path().join("working");
    fs::create_dir(&working_directory)?;
    let egress = create_sink_queue(&working_directory)?;
    let output = root.path().join("sink-output.txt");
    let config = serde_json::json!({"outputFile": output});
    run_sink_program(
        PluginProgramLaunch::new(
            &program_directory,
            &command,
            working_directory.clone(),
            &config,
            sink_channels(),
        )
        .map_err(io::Error::other)?,
        || complete_sink_round_trip(&working_directory, &egress, &output, "sink-through-rust"),
    )
    .await
}

async fn run_generated_source_and_sink_program(bundle: &[u8]) -> io::Result<()> {
    let (root, program_directory, command) = extract_generated_program(bundle)?;
    let working_directory = root.path().join("working");
    fs::create_dir(&working_directory)?;
    let (submission, completion) = create_source_queue_pair(&working_directory)?;
    let egress = create_sink_queue(&working_directory)?;
    let output = root.path().join("source-and-sink-output.txt");
    let config = serde_json::json!({
        "message": "combined-source-through-rust",
        "queueIndex": 0,
        "outputFile": output,
    });
    let pending_record = std::cell::Cell::new(None);
    run_source_and_sink_program(
        PluginProgramLaunch::new(
            &program_directory,
            &command,
            working_directory.clone(),
            &config,
            sink_channels(),
        )
        .map_err(io::Error::other)?,
        || {
            pending_record.set(Some(read_source_submission(
                &working_directory,
                &submission,
                "combined-source-through-rust",
            )?));
            Ok(())
        },
        || {
            complete_sink_round_trip(
                &working_directory,
                &egress,
                &output,
                "combined-sink-after-quiesce",
            )?;
            let Some(record_id) = pending_record.take() else {
                unreachable!("Ready reads the Source record before SourceQuiesced");
            };
            complete_source_result(&working_directory, &completion, record_id)
        },
    )
    .await
}

async fn run_generated_sink_failure_boundaries(bundle: &[u8]) -> io::Result<()> {
    let (root, program_directory, command) = extract_generated_program(bundle)?;
    let working_directory = root.path().join("stream-loss-working");
    fs::create_dir(&working_directory)?;
    create_sink_queue(&working_directory)?;
    let output = root.path().join("stream-loss-output.txt");
    let config = serde_json::json!({"outputFile": output});
    run_sink_program_with_control_stream_loss(
        PluginProgramLaunch::new(
            &program_directory,
            &command,
            working_directory,
            &config,
            sink_channels(),
        )
        .map_err(io::Error::other)?,
    )
    .await?;

    let working_directory = root.path().join("owner-loss-working");
    fs::create_dir(&working_directory)?;
    create_sink_queue(&working_directory)?;
    let output = root.path().join("owner-loss-output.txt");
    let config = serde_json::json!({"outputFile": output});
    run_sink_program_with_owner_loss(
        PluginProgramLaunch::new(
            &program_directory,
            &command,
            working_directory,
            &config,
            sink_channels(),
        )
        .map_err(io::Error::other)?,
    )
    .await?;

    let working_directory = root.path().join("force-stop-working");
    fs::create_dir(&working_directory)?;
    create_sink_queue(&working_directory)?;
    let output = root.path().join("force-stop-output.txt");
    let config = serde_json::json!({"outputFile": output});
    force_stop_sink_program(
        PluginProgramLaunch::new(
            &program_directory,
            &command,
            working_directory,
            &config,
            sink_channels(),
        )
        .map_err(io::Error::other)?,
    )
    .await
}

fn extract_generated_program(
    bundle: &[u8],
) -> io::Result<(tempfile::TempDir, PathBuf, Vec<String>)> {
    let root = tempfile::tempdir()?;
    let program_directory = root.path().join("program");
    fs::create_dir(&program_directory)?;
    Archive::new(GzDecoder::new(bundle)).unpack(&program_directory)?;
    let command = generated_program_command(&program_directory)?;
    Ok((root, program_directory, command))
}

/// The Channel Region name one launch is told about, spelled as the launch spells it.
const CHANNEL_BELL_FILE_NAME: &str = "channels.bells";

/// Ensures one test-owned Bell Region holds exactly `slot_count` doorbells.
///
/// A Source and a Sink of the same Channel ring the same Region file, so the
/// second side to ask for it reuses what the first side published.
fn ensure_test_bell_region(path: &Path, slot_count: u32) -> io::Result<()> {
    let slot_count =
        NonZeroU32::new(slot_count).ok_or_else(|| io::Error::other("Bell Region has no slot"))?;
    match create_bell_region(path, slot_count, 0) {
        Ok(()) => Ok(()),
        Err(BellError::Io { source, .. }) if source.kind() == io::ErrorKind::AlreadyExists => {
            let existing = BellRegion::open(path).map_err(io::Error::other)?;
            if existing.slot_count() == slot_count {
                Ok(())
            } else {
                Err(io::Error::other(format!(
                    "Bell Region {} holds {} slots, expected {}",
                    path.display(),
                    existing.slot_count(),
                    slot_count
                )))
            }
        }
        Err(error) => Err(io::Error::other(error)),
    }
}

fn create_source_queue_pair(working_directory: &Path) -> io::Result<(PathBuf, PathBuf)> {
    let source_directory = working_directory.join("source");
    fs::create_dir(&source_directory)?;
    ensure_test_bell_region(&working_directory.join(CHANNEL_BELL_FILE_NAME), 1)?;
    ensure_test_bell_region(&loops_bell_path(&source_directory), 2)?;
    let max_pending_records =
        NonZeroU64::new(2).ok_or_else(|| io::Error::other("Source pending limit is zero"))?;
    let max_record_size =
        NonZeroU64::new(1_024).ok_or_else(|| io::Error::other("Source payload limit is zero"))?;
    let submission = source_directory.join("submission-0.queue");
    create_queue_file(
        &submission,
        submission_capacity(max_pending_records, max_record_size).map_err(io::Error::other)?,
        max_record_size,
    )
    .map_err(io::Error::other)?;
    let completion = source_directory.join("completion-0.queue");
    let completion_max_payload =
        NonZeroU64::new(u64::try_from(COMPLETION_MAX_PAYLOAD_SIZE).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("Completion payload limit is zero"))?;
    create_queue_file(
        &completion,
        completion_capacity(max_pending_records).map_err(io::Error::other)?,
        completion_max_payload,
    )
    .map_err(io::Error::other)?;
    Ok((submission, completion))
}

fn sink_channels() -> Vec<(String, u32)> {
    vec![(String::from("main"), 0)]
}

fn create_sink_queue(working_directory: &Path) -> io::Result<PathBuf> {
    let egress = tenon::runner_test_support::egress_queue_path(working_directory, "main", 0);
    fs::create_dir_all(
        egress
            .parent()
            .ok_or_else(|| io::Error::other("Queue parent is missing"))?,
    )?;
    ensure_test_bell_region(&working_directory.join(CHANNEL_BELL_FILE_NAME), 1)?;
    ensure_test_bell_region(&loops_bell_path(&working_directory.join("sink")), 1)?;
    let capacity = DataCapacity::try_from(4_096).map_err(io::Error::other)?;
    let max_payload_size =
        NonZeroU64::new(capacity.get() - tenon_ipc::queue::FRAME_HEADER_LEN as u64)
            .ok_or_else(|| io::Error::other("Egress Queue payload limit is zero"))?;
    create_queue_file(&egress, capacity, max_payload_size).map_err(io::Error::other)?;
    Ok(egress)
}

fn complete_source_round_trip(
    working_directory: &Path,
    submission: &Path,
    completion: &Path,
    expected_message: &str,
) -> io::Result<()> {
    complete_source_result(
        working_directory,
        completion,
        read_source_submission(working_directory, submission, expected_message)?,
    )
}

fn read_source_submission(
    working_directory: &Path,
    submission: &Path,
    expected_message: &str,
) -> io::Result<u64> {
    // The test plays the Flow Channel: its own doorbell is the Channel Region the
    // launch was told about, and its release rings the Source loop's first slot.
    let mut submission_reader: QueueReader = open_reader(
        submission,
        &working_directory.join(CHANNEL_BELL_FILE_NAME),
        0,
        &loops_bell_path(&working_directory.join("source")),
    )?;
    let bytes = wait_for_queue_record(&mut submission_reader)?;
    let ingress = IngressRecord::decode(bytes.as_slice()).map_err(io::Error::other)?;
    let payload = GeneratedPluginPayload::decode(ingress.payload).map_err(io::Error::other)?;
    if payload.message != expected_message {
        return Err(io::Error::other(format!(
            "Generated Source emitted {:?}, expected {expected_message:?}",
            payload.message
        )));
    }
    submission_reader.release(1).map_err(io::Error::other)?;
    Ok(ingress.record_id)
}

fn complete_source_result(
    working_directory: &Path,
    completion: &Path,
    record_id: u64,
) -> io::Result<()> {
    let completion_record = IngressCompletion {
        record_id,
        status: IngressCompletionStatus::Ok as i32,
    }
    .encode_to_vec();
    let mut completion_writer: QueueWriter = open_writer(
        completion,
        &working_directory.join(CHANNEL_BELL_FILE_NAME),
        0,
        &loops_bell_path(&working_directory.join("source")),
    )?;
    let receipt = commit_queue_record(&mut completion_writer, &completion_record)?;
    wait_for_queue_release(&completion_writer, &receipt)
}

fn complete_sink_round_trip(
    working_directory: &Path,
    egress: &Path,
    output: &Path,
    message: &str,
) -> io::Result<()> {
    let payload = GeneratedPluginPayload {
        message: message.to_owned(),
    }
    .encode_to_vec();
    let record = EgressRecord { payload }.encode_to_vec();
    let mut writer: QueueWriter = open_writer(
        egress,
        &working_directory.join(CHANNEL_BELL_FILE_NAME),
        0,
        &loops_bell_path(&working_directory.join("sink")),
    )?;
    let receipt = commit_queue_record(&mut writer, &record)?;
    wait_for_queue_release(&writer, &receipt)?;
    wait_until(|| {
        let actual = match fs::read_to_string(output) {
            Ok(actual) => actual,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(source),
        };
        Ok((actual == format!("{message}\n")).then_some(()))
    })
}

fn wait_for_queue_record(reader: &mut QueueReader) -> io::Result<Vec<u8>> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match reader.try_read().map_err(io::Error::other)? {
            ReadOutcome::Record(record) => return Ok(record.payload().to_vec()),
            ReadOutcome::Empty if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            ReadOutcome::Empty => {
                return Err(io::Error::other(
                    "Generated Plugin did not write a Queue record before the test deadline",
                ));
            }
        }
    }
}

fn commit_queue_record(writer: &mut QueueWriter, record: &[u8]) -> io::Result<WriteReceipt> {
    match writer
        .try_write_with(
            record.len(),
            |destination| std::io::Write::write_all(destination, record),
            || {},
        )
        .map_err(io::Error::other)?
    {
        WriteOutcome::Committed(receipt) => Ok(receipt),
        WriteOutcome::Full => Err(io::Error::other(
            "Empty generated Plugin test Queue rejected one record",
        )),
    }
}

fn wait_for_queue_release(writer: &QueueWriter, receipt: &WriteReceipt) -> io::Result<()> {
    wait_until(|| {
        writer
            .is_released(receipt)
            .map(|released| released.then_some(()))
            .map_err(io::Error::other)
    })
}

fn generated_bundle_manifest(bundle: &[u8]) -> io::Result<serde_json::Value> {
    for entry in Archive::new(GzDecoder::new(bundle)).entries()? {
        let entry = entry?;
        if entry.path()? == Path::new("manifest.json") {
            return serde_json::from_reader(entry).map_err(io::Error::other);
        }
    }
    Err(io::Error::other("Generated bundle manifest is missing"))
}

fn write_java_bundle_contract_evidence(packages: &GeneratedJavaPackages) -> io::Result<()> {
    let mut contracts = serde_json::Map::new();
    for (interface, bundle) in [
        ("source", &packages.source),
        ("sink", &packages.sink),
        ("source-and-sink", &packages.source_and_sink),
    ] {
        let manifest = generated_bundle_manifest(bundle)?;
        assert_eq!(manifest["interface"], interface);
        let mut hashes = std::collections::BTreeMap::new();
        let mut archive = Archive::new(GzDecoder::new(bundle.as_slice()));
        for entry in archive.entries()? {
            let mut entry = entry?;
            let path = entry.path()?.into_owned();
            if ["config.schema.json", "payload.descriptor.pb", "program.jar"]
                .iter()
                .any(|expected| path == Path::new(expected))
            {
                let mut bytes = Vec::new();
                entry.read_to_end(&mut bytes)?;
                hashes.insert(path.to_string_lossy().into_owned(), sha256_hex(&bytes));
            }
        }
        assert_eq!(
            hashes.len(),
            3,
            "generated bundle contract material is incomplete"
        );
        contracts.insert(
            interface.to_owned(),
            serde_json::json!({
                "programName": manifest["programName"], "exactVersion": manifest["exactVersion"],
                "displayName": manifest["displayName"], "description": manifest["description"],
                "interface": interface, "sha256": hashes,
            }),
        );
    }
    let classifier = java_plugin_platform_classifier()?;
    let output = cargo_target_directory(Path::new(env!("CARGO_MANIFEST_DIR")))
        .join("runner-java-archetype/contracts");
    fs::create_dir_all(&output)?;
    fs::write(
        output.join(format!("{classifier}.json")),
        serde_json::to_vec_pretty(
            &serde_json::json!({"platform": classifier, "contracts": contracts, "externalJvm": {
                "source": sha256_hex(&external_java_package(&packages.source)?),
                "replaySink": sha256_hex(&external_java_package(&packages.replay_sink)?),
            }}),
        )?,
    )
}

fn generated_program_command(program_directory: &Path) -> io::Result<Vec<String>> {
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(program_directory.join("manifest.json"))?)
            .map_err(io::Error::other)?;
    manifest["command"]
        .as_array()
        .ok_or_else(|| io::Error::other("Generated Plugin manifest command is not an array"))?
        .iter()
        .map(|argument| {
            argument
                .as_str()
                .map(ToOwned::to_owned)
                .ok_or_else(|| io::Error::other("Generated Plugin command argument is not text"))
        })
        .collect::<io::Result<Vec<_>>>()
        .and_then(|command| {
            if command.is_empty() {
                Err(io::Error::other(
                    "Generated Plugin manifest command is empty",
                ))
            } else {
                Ok(command)
            }
        })
}

#[derive(Clone, PartialEq, prost::Message)]
struct GeneratedPluginPayload {
    #[prost(string, tag = "1")]
    message: String,
}

fn java_plugin_platform_classifier() -> io::Result<&'static str> {
    match (env::consts::OS, env::consts::ARCH) {
        ("linux", "x86_64") => Ok("linux-amd64"),
        ("linux", "aarch64") => Ok("linux-arm64"),
        ("macos", "x86_64") => Ok("macos-amd64"),
        ("macos", "aarch64") => Ok("macos-arm64"),
        (operating_system, architecture) => Err(io::Error::other(format!(
            "Unsupported Java Plugin test platform: {operating_system}/{architecture}"
        ))),
    }
}

fn subscribe_to_diagnostics(
    address: SocketAddr,
    document_id: &str,
    encoded_target: &str,
) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    write!(
        stream,
        "GET /pipelines/{document_id}/diagnostics?target={encoded_target} HTTP/1.1\r\nHost: {address}\r\nAccept: text/event-stream\r\nConnection: close\r\n\r\n",
    )?;
    stream.flush()?;
    let response = read_until_complete_sse_event(&mut stream, b"event: attached")?;
    if response.starts_with(b"HTTP/1.1 200") {
        return Ok(stream);
    }
    Err(io::Error::other(format!(
        "Generated Plugin diagnostics subscription failed: {}",
        String::from_utf8_lossy(&response),
    )))
}

fn verify_source_diagnostic(stream: &mut TcpStream) -> io::Result<()> {
    const MARKER: &str = "Generated Source payload acknowledged";
    let response = read_until_complete_sse_event(stream, MARKER.as_bytes())?;
    let diagnostic = diagnostic_event_containing_marker(&response, MARKER)?;
    require_diagnostic_field(diagnostic, "\"pluginInstanceId\":\"source\"")?;
    require_diagnostic_field(diagnostic, "\"stream\":\"stderr\"")?;
    require_diagnostic_field(diagnostic, "\"sequence\":\"")
}

fn verify_sink_diagnostic(stream: &mut TcpStream) -> io::Result<()> {
    const MARKER: &str = "Generated Sink received a batch";
    let response = read_until_complete_sse_event(stream, MARKER.as_bytes())?;
    let diagnostic = diagnostic_event_containing_marker(&response, MARKER)?;
    require_diagnostic_field(diagnostic, "\"pluginInstanceId\":\"primary\"")?;
    require_diagnostic_field(diagnostic, "\"stream\":\"stdout\"")?;
    require_diagnostic_field(diagnostic, "\"pluginProcessInstanceId\":\"")
}

fn read_until_complete_sse_event(stream: &mut TcpStream, needle: &[u8]) -> io::Result<Vec<u8>> {
    let mut response = Vec::new();
    let mut buffer = [0_u8; 512];
    loop {
        let required_marker_end = response
            .windows(needle.len())
            .position(|window| window == needle)
            .map(|index| index + needle.len());
        if required_marker_end.is_some_and(|marker_end| {
            response[marker_end..]
                .windows(2)
                .any(|window| window == b"\n\n")
        }) {
            return Ok(response);
        }
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            return Err(io::Error::other(
                "Diagnostics stream ended before the expected Plugin record",
            ));
        }
        response.extend_from_slice(&buffer[..read]);
    }
}

fn diagnostic_event_containing_marker<'a>(response: &'a [u8], marker: &str) -> io::Result<&'a str> {
    let response = std::str::from_utf8(response).map_err(io::Error::other)?;
    let marker_index = response
        .find(marker)
        .ok_or_else(|| io::Error::other("Expected Plugin diagnostic marker is missing"))?;
    let event_index = response[..marker_index]
        .rfind("event: diagnostic")
        .ok_or_else(|| io::Error::other("Plugin diagnostic SSE event prefix is missing"))?;
    let event_end = response[marker_index..]
        .find("\n\n")
        .map(|index| marker_index + index)
        .ok_or_else(|| io::Error::other("Plugin diagnostic SSE event end is missing"))?;
    Ok(&response[event_index..event_end])
}

fn require_diagnostic_field(diagnostic: &str, field: &str) -> io::Result<()> {
    if diagnostic.contains(field) {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "Plugin diagnostic is missing {field}: {diagnostic}"
    )))
}

fn wait_for_java_round_trip(
    state_directory: &Path,
    document: &str,
    output: &Path,
    scenario: DeliveryScenario,
) -> io::Result<()> {
    wait_until(|| {
        let Some(queues) = java_queue_paths(state_directory, document)? else {
            return Ok(None);
        };
        let submission = queue_commit_was_released(&queues.submission)?;
        let completion = queue_commit_was_released(&queues.completion)?;
        let sink = queue_commit_was_released(&queues.sink)?;
        let received = match fs::read_to_string(output) {
            Ok(received) => received == scenario.expected_output(),
            Err(source) if source.kind() == io::ErrorKind::NotFound => false,
            Err(source) => return Err(source),
        };
        let process_count = match scenario {
            DeliveryScenario::Success
            | DeliveryScenario::DelayedSinkSuccess
            | DeliveryScenario::CapacitySaturation => true,
            DeliveryScenario::ReplayAfterFailure => {
                let starts = path_with_suffix(output, ".starts");
                match fs::read_to_string(starts) {
                    Ok(starts) => starts == "start\nstart\n",
                    Err(source) if source.kind() == io::ErrorKind::NotFound => false,
                    Err(source) => return Err(source),
                }
            }
        };
        Ok((submission && completion && sink && received && process_count).then_some(()))
    })
}

fn verify_sink_holds_source_completion(
    state_directory: &Path,
    document: &str,
    output: &Path,
) -> io::Result<()> {
    wait_until(|| {
        Ok(path_with_suffix(output, ".write-started")
            .is_file()
            .then_some(()))
    })?;
    let queues = java_queue_paths(state_directory, document)?
        .ok_or_else(|| io::Error::other("Generated Java Queue paths are missing"))?;
    let submission = required_queue_progress(&queues.submission)?;
    let completion = required_queue_progress(&queues.completion)?;
    let sink = required_queue_progress(&queues.sink)?;

    if submission.commit == 0 || submission.release != submission.commit {
        return Err(io::Error::other(format!(
            "Source Submission must be copied before Sink delivery waits; observed {submission:?}"
        )));
    }
    if completion.commit != 0 || completion.release != 0 {
        return Err(io::Error::other(format!(
            "Source Completion advanced before Sink shared release; observed {completion:?}"
        )));
    }
    if sink.commit == 0 || sink.release >= sink.commit {
        return Err(io::Error::other(format!(
            "Sink Egress must remain committed and unreleased while delivery waits; observed {sink:?}"
        )));
    }
    if !fs::read_to_string(output)?.is_empty() {
        return Err(io::Error::other(
            "Generated Sink produced output before its success boundary",
        ));
    }
    Ok(())
}

fn wait_for_source_results(output: &Path, expected: &str) -> io::Result<()> {
    let results = path_with_suffix(output, ".source-results");
    wait_until(|| {
        let actual = match fs::read_to_string(&results) {
            Ok(actual) => actual,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(source),
        };
        Ok((actual == expected).then_some(()))
    })
}

fn java_queue_paths(state_directory: &Path, document: &str) -> io::Result<Option<JavaQueuePaths>> {
    let Some(instances) = java_instance_root(state_directory, document)? else {
        return Ok(None);
    };
    let source_directory = instances
        .join(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(b"source")))
        .join("source");
    let sink = tenon::runner_test_support::egress_queue_path(
        &instances.join(
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(b"primary")),
        ),
        "main",
        0,
    );
    Ok(Some(JavaQueuePaths {
        submission: source_directory.join("submission-0.queue"),
        completion: source_directory.join("completion-0.queue"),
        sink,
    }))
}

fn java_instance_root(state_directory: &Path, document: &str) -> io::Result<Option<PathBuf>> {
    let entries = match state_directory.join("pipelines").read_dir() {
        Ok(entries) => entries,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(source),
    };
    let roots = entries
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    let [runtime_root] = roots.as_slice() else {
        return Ok(None);
    };
    Ok(Some(
        runtime_root
            .join(sha256_hex(document.as_bytes()))
            .join("instances"),
    ))
}

fn required_queue_progress(path: &Path) -> io::Result<QueueProgress> {
    queue_progress(path)?.ok_or_else(|| {
        io::Error::other(format!(
            "Generated Java Queue is missing or invalid: {}",
            path.display()
        ))
    })
}

fn queue_commit_was_released(path: &Path) -> io::Result<bool> {
    Ok(matches!(
        queue_progress(path)?,
        Some(progress) if progress.commit > 0 && progress.release == progress.commit
    ))
}

fn queue_progress(path: &Path) -> io::Result<Option<QueueProgress>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(source),
    };
    let capacity =
        DataCapacity::from_file_len(u64::try_from(bytes.len()).map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
    let header = match Header::decode(capacity, bytes.get(..HEADER_LEN).unwrap_or(&bytes)) {
        Ok(header) => header,
        Err(_) => return Ok(None),
    };
    Ok(Some(QueueProgress {
        commit: header.commit().get(),
        release: header.release().get(),
    }))
}

fn path_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut result = path.as_os_str().to_os_string();
    result.push(suffix);
    result.into()
}

#[derive(Debug, Eq, PartialEq)]
struct GeneratedJavaPackages {
    source: Vec<u8>,
    capacity_source: Vec<u8>,
    sink: Vec<u8>,
    replay_sink: Vec<u8>,
    gated_sink: Vec<u8>,
    source_and_sink: Vec<u8>,
}

impl GeneratedJavaPackages {
    fn read_from(directory: &Path) -> io::Result<Self> {
        Ok(Self {
            source: fs::read(directory.join("source.tar.gz"))?,
            capacity_source: fs::read(directory.join("capacity-source.tar.gz"))?,
            sink: fs::read(directory.join("sink.tar.gz"))?,
            replay_sink: fs::read(directory.join("replay-sink.tar.gz"))?,
            gated_sink: fs::read(directory.join("gated-sink.tar.gz"))?,
            source_and_sink: fs::read(directory.join("source-and-sink.tar.gz"))?,
        })
    }

    fn write_to(&self, directory: &Path) -> io::Result<()> {
        fs::write(directory.join("source.tar.gz"), &self.source)?;
        fs::write(
            directory.join("capacity-source.tar.gz"),
            &self.capacity_source,
        )?;
        fs::write(directory.join("sink.tar.gz"), &self.sink)?;
        fs::write(directory.join("replay-sink.tar.gz"), &self.replay_sink)?;
        fs::write(directory.join("gated-sink.tar.gz"), &self.gated_sink)?;
        fs::write(
            directory.join("source-and-sink.tar.gz"),
            &self.source_and_sink,
        )
    }
}

struct PendingJavaPackageCache {
    staging: tempfile::TempDir,
    destination: PathBuf,
}

impl PendingJavaPackageCache {
    fn stage(
        cache_root: &Path,
        destination: PathBuf,
        packages: &GeneratedJavaPackages,
    ) -> io::Result<Self> {
        fs::create_dir_all(cache_root)?;
        let staging = tempfile::Builder::new()
            .prefix("runner-java-archetype-")
            .tempdir_in(cache_root)?;
        packages.write_to(staging.path())?;
        Ok(Self {
            staging,
            destination,
        })
    }

    fn publish(self) -> io::Result<()> {
        let Self {
            staging,
            destination,
        } = self;
        fs::rename(staging.path(), &destination)?;
        let _ = staging.keep();
        retain_java_package_cache(&destination)
    }
}

struct JavaQueuePaths {
    submission: PathBuf,
    completion: PathBuf,
    sink: PathBuf,
}

#[derive(Debug)]
struct QueueProgress {
    commit: u64,
    release: u64,
}

#[derive(Clone, Copy)]
enum DeliveryScenario {
    Success,
    DelayedSinkSuccess,
    CapacitySaturation,
    ReplayAfterFailure,
}

impl DeliveryScenario {
    fn document_id(self) -> &'static str {
        match self {
            Self::Success => "generated-java-success",
            Self::DelayedSinkSuccess => "generated-java-sink-success-boundary",
            Self::CapacitySaturation => "generated-java-capacity-saturation",
            Self::ReplayAfterFailure => "generated-java-replay",
        }
    }

    fn expected_output(self) -> &'static str {
        match self {
            Self::Success | Self::DelayedSinkSuccess | Self::CapacitySaturation => {
                "hello from generated Java Source\n"
            }
            Self::ReplayAfterFailure => {
                "hello from generated Java Source\nhello from generated Java Source\n"
            }
        }
    }

    const fn sink_exact_version(self) -> &'static str {
        match self {
            Self::Success => "1.0.0",
            Self::DelayedSinkSuccess | Self::CapacitySaturation => "1.0.2",
            Self::ReplayAfterFailure => "1.0.1",
        }
    }

    const fn source_exact_version(self) -> &'static str {
        match self {
            Self::CapacitySaturation => "1.0.1",
            Self::Success | Self::DelayedSinkSuccess | Self::ReplayAfterFailure => "1.0.0",
        }
    }

    const fn captures_sink_diagnostics(self) -> bool {
        matches!(self, Self::ReplayAfterFailure)
    }

    fn verify_shutdown(self, output: &Path) -> io::Result<()> {
        if matches!(
            self,
            Self::Success | Self::DelayedSinkSuccess | Self::CapacitySaturation
        ) {
            return Ok(());
        }
        let closes = fs::read_to_string(path_with_suffix(output, ".closes"))?;
        if closes == "close\n" {
            return Ok(());
        }
        Err(io::Error::other(format!(
            "Only the planned replacement must close once; observed {closes:?}"
        )))
    }
}

fn external_java_package(bundle: &[u8]) -> io::Result<Vec<u8>> {
    let mut files = std::collections::BTreeMap::new();
    for entry in Archive::new(GzDecoder::new(bundle)).entries()? {
        let mut entry = entry?;
        if !entry.header().entry_type().is_file() {
            continue;
        }
        let path = entry.path()?.into_owned();
        if path.starts_with("runtime") {
            continue;
        }
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes)?;
        if path == Path::new("manifest.json") {
            let mut manifest: serde_json::Value = serde_json::from_slice(&bytes)?;
            manifest["command"][0] = serde_json::json!("java");
            manifest["platforms"] = serde_json::json!([
                {"os":"linux","architecture":"amd64"},
                {"os":"linux","architecture":"arm64"},
                {"os":"darwin","architecture":"amd64"},
                {"os":"darwin","architecture":"arm64"}
            ]);
            bytes = serde_json::to_vec(&manifest)?;
        } else if path == Path::new("README") {
            bytes = b"Tenon Plugin requires an externally installed JDK 25 matching the Runner target. Provide java on the Runner PATH. Dependency inventory: DEPENDENCIES. Project notices: LICENSE and NOTICE.\n".to_vec();
        } else if path == Path::new("DEPENDENCIES") {
            let text = String::from_utf8(bytes).map_err(io::Error::other)?;
            let (dependencies, _) = text
                .split_once("\nBundled Java runtime\n")
                .ok_or_else(|| io::Error::other("Generated runtime inventory is missing"))?;
            bytes =
                format!("{dependencies}\nExternal runtime: JDK 25 matching the Runner target.\n")
                    .into_bytes();
        }
        files.insert(path, bytes);
    }
    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    let mut archive = tar::Builder::new(encoder);
    for (path, bytes) in files {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o500);
        header.set_cksum();
        archive.append_data(&mut header, path, bytes.as_slice())?;
    }
    archive.into_inner()?.finish()
}

fn verify_external_java_packages(packages: &GeneratedJavaPackages) -> io::Result<()> {
    let deployed_runtime = tempfile::tempdir()?;
    let mut original = Archive::new(GzDecoder::new(packages.source.as_slice()));
    for entry in original.entries()? {
        let mut entry = entry?;
        if entry.path()?.starts_with("runtime") {
            entry.unpack_in(deployed_runtime.path())?;
        }
    }
    let source = external_java_package(&packages.source)?;
    let sink = external_java_package(&packages.replay_sink)?;
    for bundle in [&source, &sink] {
        assert_eq!(generated_bundle_manifest(bundle)?["command"][0], "java");
        for entry in Archive::new(GzDecoder::new(bundle.as_slice())).entries()? {
            assert!(!entry?.path()?.starts_with("runtime"));
        }
    }
    run_generated_java_pipeline_with_path(
        &source,
        &sink,
        DeliveryScenario::ReplayAfterFailure,
        &deployed_runtime.path().join("runtime/bin"),
    )?;
    verify_missing_external_java(&source, &sink)
}

fn verify_missing_external_java(source: &[u8], sink: &[u8]) -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let empty_path = tempfile::tempdir()?;
    let address = available_address()?;
    let config = runner_http_support::write_config(directory.path(), address)?;
    let mut runner = TestRunner::spawn_with_path(&config, empty_path.path())?;
    wait_for_http(&mut runner, address)?;
    for bundle in [source, sink] {
        let response = request(
            address,
            "POST",
            "/plugins",
            &[(
                "Content-Type",
                "application/vnd.apache.tenon.plugin+tar+gzip",
            )],
            bundle,
        )?;
        assert_eq!(response.status, 201, "{}", response.body_text());
    }
    let output = directory.path().join("missing-java-output");
    let document = generated_java_document(
        "external-java-missing",
        &output,
        DeliveryScenario::ReplayAfterFailure,
    )?;
    let created = request(
        address,
        "PUT",
        "/documents/external-java-missing",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        document.as_bytes(),
    )?;
    assert_eq!(created.status, 201, "{}", created.body_text());
    wait_until(|| {
        let response = request(address, "GET", "/pipelines/external-java-missing", &[], &[])?;
        let value = response.json();
        let failed = value["pluginInstances"]
            .as_array()
            .is_some_and(|instances| {
                instances.len() == 2
                    && instances
                        .iter()
                        .all(|instance| instance["state"] == "start-failed")
            });
        Ok(failed.then_some(()))
    })?;
    let state = request(address, "GET", "/pipelines/external-java-missing", &[], &[])?.json();
    assert_eq!(state["state"], "running");
    assert!(state.get("runtimeIssues").is_none());
    for instance in state["pluginInstances"]
        .as_array()
        .ok_or_else(|| io::Error::other("Instance status is missing"))?
    {
        assert_eq!(instance["lastError"]["code"], "plugin.spawn_failed");
    }
    assert!(!output.exists());
    runner.terminate()
}
