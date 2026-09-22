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

use rustix::io::Errno;
use rustix::process::{Pid, Signal, geteuid, kill_process, test_kill_process};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeSet, HashMap};
use std::io::{self, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};
use std::{fs, iter, thread};

#[path = "support/file_tree.rs"]
mod file_tree;
#[path = "runner_cli/plugin_fixture.rs"]
mod plugin_fixture;
use file_tree::named_files;
use plugin_fixture::{PluginInterface, install_program};

const PROCESS_DEADLINE: Duration = Duration::from_secs(30);
const REPEATED_STARTUP_DEADLINE: Duration = Duration::from_secs(60);
// These tests compete for process, signal, and filesystem timing on one host.
static RUNNER_CLI_TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn binary_rejects_invalid_runner_arguments_and_configuration() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    let missing = Command::new(env!("CARGO_BIN_EXE_tenon")).output()?;
    assert_eq!(missing.status.code(), Some(2));
    assert!(missing.stdout.is_empty());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("runner.arguments_invalid"));

    let duplicate = Command::new(env!("CARGO_BIN_EXE_tenon"))
        .args(["--config", "/tmp/one.jsonc", "--config", "/tmp/two.jsonc"])
        .output()?;
    assert_eq!(duplicate.status.code(), Some(2));

    let relative = Command::new(env!("CARGO_BIN_EXE_tenon"))
        .args(["--config", "runner.jsonc"])
        .output()?;
    assert_eq!(relative.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&relative.stderr).contains("runner_config.path_not_absolute"));

    let directory = tempfile::tempdir()?;
    let invalid_path = directory.path().join("invalid.jsonc");
    fs::write(&invalid_path, "{}")?;
    let invalid = runner_command(&invalid_path).output()?;
    assert_eq!(invalid.status.code(), Some(1));
    assert!(invalid.stdout.is_empty());
    assert!(String::from_utf8_lossy(&invalid.stderr).contains("runner_config.schema_invalid"));
    Ok(())
}

#[test]
fn empty_runner_stays_alive_until_sigterm_and_removes_runtime_state() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    let directory = tempfile::tempdir()?;
    let config_path = write_config(directory.path())?;
    let mut runner = TestRunner::spawn(&config_path)?;
    let runtime_root = wait_for_runtime_root(directory.path())?;

    assert!(runner.is_running()?);
    for path in [
        directory.path().join("tenon-documents"),
        directory.path().join("plugins"),
        directory.path().join("plugins/programs"),
        directory.path().join("pipelines"),
    ] {
        assert_eq!(path.metadata()?.permissions().mode() & 0o7777, 0o700);
    }

    let output = runner.terminate_and_wait()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
    assert_eq!(runner_diagnostics(&output.stderr)?, "");
    assert!(!runtime_root.exists());
    Ok(())
}

#[test]
fn restarted_runner_removes_the_previous_crashed_runtime_root() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    let directory = tempfile::tempdir()?;
    let config_path = write_config(directory.path())?;
    let mut first_runner = TestRunner::spawn(&config_path)?;
    let first_runtime_root = wait_for_runtime_root(directory.path())?;
    let captured_executable = first_runtime_root.join("tenon");
    assert!(captured_executable.is_file());
    assert_eq!(
        captured_executable.metadata()?.permissions().mode() & 0o777,
        0o500
    );

    let first_output = first_runner.kill_and_wait()?;
    assert!(!first_output.status.success());
    assert!(first_runtime_root.is_dir());

    let mut second_runner = TestRunner::spawn(&config_path)?;
    let second_runtime_root =
        wait_for_replaced_runtime_root(directory.path(), &first_runtime_root)?;
    assert!(!first_runtime_root.exists());
    assert_ne!(second_runtime_root, first_runtime_root);

    let second_output = second_runner.terminate_and_wait()?;
    assert!(
        second_output.status.success(),
        "{}",
        String::from_utf8_lossy(&second_output.stderr)
    );
    assert!(!second_runtime_root.exists());
    Ok(())
}

#[test]
fn ready_and_unready_documents_are_recovered_without_cross_effect() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    let directory = tempfile::tempdir()?;
    install_program(directory.path(), PluginInterface::Source)?;
    install_program(directory.path(), PluginInterface::Sink)?;
    let ready_source = document("ready-runtime");
    let unready_source = document_with_source_program("waiting-runtime", "9.9.9");
    write_document(directory.path(), "ready-runtime", &ready_source)?;
    write_document(directory.path(), "waiting-runtime", &unready_source)?;
    let config_path = write_config(directory.path())?;
    let mut runner = TestRunner::spawn(&config_path)?;
    let runtime_root = wait_for_runtime_root(directory.path())?;
    let ready_directory = runtime_root.join(sha256_hex(ready_source.as_bytes()));
    let unready_directory = runtime_root.join(sha256_hex(unready_source.as_bytes()));

    wait_for_plugin_markers(&ready_directory, None)?;
    assert!(runner.is_running()?);
    assert!(runtime_root.join("tenon").is_file());
    assert!(ready_directory.is_dir());
    assert!(!unready_directory.exists());

    let output = runner.terminate_and_wait()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!runtime_root.exists());
    Ok(())
}

#[test]
fn ready_document_runs_plugins_restarts_after_pipeline_crash_and_stops_cleanly() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    let directory = tempfile::tempdir()?;
    install_program(directory.path(), PluginInterface::Source)?;
    install_program(directory.path(), PluginInterface::Sink)?;
    let source = document("running");
    write_document(directory.path(), "running", &source)?;
    let config_path = write_config(directory.path())?;
    let executable = std::env::var_os("TENON_TEST_RUNNER_BINARY")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_tenon").into());
    let mut runner = TestRunner::spawn_command(runner_command_with_executable(
        Path::new(&executable),
        &config_path,
    ))?;
    let runtime_root = wait_for_runtime_root(directory.path())?;

    let first_pipeline = wait_for_plugin_markers(&runtime_root, None)?;
    assert!(runner.is_running()?);
    kill_process(first_pipeline, Signal::KILL).map_err(io::Error::from)?;

    let restarted_pipeline = wait_for_plugin_markers(&runtime_root, Some(first_pipeline))?;
    assert_ne!(restarted_pipeline, first_pipeline);
    kill_process(restarted_pipeline, Signal::KILL).map_err(io::Error::from)?;

    let second_restart = wait_for_plugin_markers(&runtime_root, Some(restarted_pipeline))?;
    assert_ne!(second_restart, restarted_pipeline);
    assert!(runner.is_running()?);

    let output = runner.terminate_and_wait()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
    assert_eq!(runner_diagnostics(&output.stderr)?, "");
    assert!(!runtime_root.exists());
    Ok(())
}

#[test]
fn one_pipeline_crash_restarts_only_its_own_process_tree() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    let directory = tempfile::tempdir()?;
    install_program(directory.path(), PluginInterface::Source)?;
    install_program(directory.path(), PluginInterface::Sink)?;
    let source_a = document("isolated-a");
    let source_b = document("isolated-b");
    write_document(directory.path(), "isolated-a", &source_a)?;
    write_document(directory.path(), "isolated-b", &source_b)?;
    let config_path = write_config(directory.path())?;
    let mut runner = TestRunner::spawn(&config_path)?;
    let runtime_root = wait_for_runtime_root(directory.path())?;
    let pipeline_a_directory = runtime_root.join(sha256_hex(source_a.as_bytes()));
    let pipeline_b_directory = runtime_root.join(sha256_hex(source_b.as_bytes()));
    let first_pipeline_a = wait_for_plugin_markers(&pipeline_a_directory, None)?;
    let pipeline_b = wait_for_plugin_markers(&pipeline_b_directory, None)?;
    let first_plugin_a_processes =
        wait_for_process_markers(&pipeline_a_directory, "process.pid", 2)?;
    let plugin_b_processes = wait_for_process_markers(&pipeline_b_directory, "process.pid", 2)?;

    kill_process(first_pipeline_a, Signal::KILL).map_err(io::Error::from)?;
    let restarted_pipeline_a =
        wait_for_plugin_markers(&pipeline_a_directory, Some(first_pipeline_a))?;
    let restarted_plugin_a_processes =
        wait_for_process_markers(&pipeline_a_directory, "process.pid", 2)?;

    assert_ne!(restarted_pipeline_a, first_pipeline_a);
    wait_for_processes_to_exit(
        iter::once(first_pipeline_a).chain(first_plugin_a_processes.iter().copied()),
    )?;
    assert!(
        first_plugin_a_processes
            .iter()
            .all(|process| !restarted_plugin_a_processes.contains(process))
    );
    assert_eq!(
        wait_for_plugin_markers(&pipeline_b_directory, None)?,
        pipeline_b
    );
    assert_eq!(
        wait_for_process_markers(&pipeline_b_directory, "process.pid", 2)?,
        plugin_b_processes
    );
    assert!(test_kill_process(pipeline_b).is_ok());
    assert!(
        plugin_b_processes
            .iter()
            .all(|process| test_kill_process(*process).is_ok())
    );
    assert!(runner.is_running()?);

    let output = runner.terminate_and_wait()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!runtime_root.exists());
    Ok(())
}

#[test]
fn running_runner_reuses_its_captured_image_after_deployment_replacement() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    let directory = tempfile::tempdir()?;
    install_program(directory.path(), PluginInterface::Source)?;
    install_program(directory.path(), PluginInterface::Sink)?;
    let source = document("stable-image");
    write_document(directory.path(), "stable-image", &source)?;
    let config_path = write_config(directory.path())?;
    let deployment = directory.path().join("deployment");
    fs::create_dir(&deployment)?;
    let executable = deployment.join("tenon");
    fs::copy(env!("CARGO_BIN_EXE_tenon"), &executable)?;
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o500))?;
    let replacement_marker = directory.path().join("replacement.started");
    let mut command = runner_command_with_executable(&executable, &config_path);
    command.env("TENON_REPLACEMENT_MARKER", &replacement_marker);
    let mut runner = TestRunner::spawn_command(command)?;
    let runtime_root = wait_for_runtime_root(directory.path())?;
    let first_pipeline = wait_for_plugin_markers(&runtime_root, None)?;

    replace_executable_with_script(
        &executable,
        "#!/bin/sh\n: > \"${TENON_REPLACEMENT_MARKER:?}\"\nexit 91\n",
    )?;
    kill_process(first_pipeline, Signal::KILL).map_err(io::Error::from)?;

    let restarted_pipeline = wait_for_plugin_markers(&runtime_root, Some(first_pipeline))?;
    assert_ne!(restarted_pipeline, first_pipeline);
    assert!(!replacement_marker.exists());
    assert!(runner.is_running()?);

    let output = runner.terminate_and_wait()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!replacement_marker.exists());
    assert!(!runtime_root.exists());
    Ok(())
}

#[test]
fn pipeline_startup_timeout_cleans_and_retries_repeatedly() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    let directory = tempfile::tempdir()?;
    install_program(directory.path(), PluginInterface::Source)?;
    install_program(directory.path(), PluginInterface::Sink)?;
    let source = document("startup-timeout");
    write_document(directory.path(), "startup-timeout", &source)?;
    let config_path = write_config_with_pipeline_timing(directory.path(), 2_000, 2_000, 100, 200)?;
    let startup_attempt_log = directory.path().join("startup-attempts.log");
    let mut command = runner_command(&config_path);
    command.env("TENON_STARTUP_ATTEMPT_LOG", &startup_attempt_log);
    let mut runner = TestRunner::spawn_command(command)?;
    let runtime_root = wait_for_runtime_root(directory.path())?;
    let first_pipeline = wait_for_plugin_markers(&runtime_root, None)?;

    replace_executable_with_script(
        &runtime_root.join("tenon"),
        "#!/bin/sh\nprintf '%s\\n' \"$$\" >> \"${TENON_STARTUP_ATTEMPT_LOG:?}\"\nwhile :; do sleep 0.05; done\n",
    )?;
    kill_process(first_pipeline, Signal::KILL).map_err(io::Error::from)?;

    let startup_attempts = wait_for_logged_pipeline_attempts(&mut runner, &startup_attempt_log, 4)
        .map_err(|source| test_step_error("Four Pipeline attempts did not start", source))?;
    let first_timed_out_pipeline = startup_attempts[0];
    let second_timed_out_pipeline = startup_attempts[1];
    let third_timed_out_pipeline = startup_attempts[2];
    let fourth_timed_out_pipeline = startup_attempts[3];

    assert_ne!(first_pipeline, first_timed_out_pipeline);
    assert_ne!(first_timed_out_pipeline, second_timed_out_pipeline);
    assert_ne!(second_timed_out_pipeline, third_timed_out_pipeline);
    assert_ne!(third_timed_out_pipeline, fourth_timed_out_pipeline);
    assert!(runner.is_running()?);

    let output = runner.terminate_and_wait()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!runtime_root.exists());
    Ok(())
}

#[test]
fn runner_shutdown_during_restart_backoff_does_not_launch_another_pipeline() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    let directory = tempfile::tempdir()?;
    install_program(directory.path(), PluginInterface::Source)?;
    install_program(directory.path(), PluginInterface::Sink)?;
    let source = document("backoff-shutdown");
    write_document(directory.path(), "backoff-shutdown", &source)?;
    let config_path =
        write_config_with_pipeline_timing(directory.path(), 2_000, 1_000, 5_000, 5_000)?;
    let mut runner = TestRunner::spawn(&config_path)?;
    let runtime_root = wait_for_runtime_root(directory.path())?;
    let pipeline_directory = runtime_root.join(sha256_hex(source.as_bytes()));
    let first_pipeline = wait_for_plugin_markers(&runtime_root, None)
        .map_err(|source| test_step_error("First Pipeline did not start", source))?;

    kill_process(first_pipeline, Signal::KILL).map_err(io::Error::from)?;
    wait_for_processes_to_exit([first_pipeline])
        .map_err(|source| test_step_error("First Pipeline did not exit", source))?;
    wait_until(|| Ok((!pipeline_directory.exists()).then_some(())))
        .map_err(|source| test_step_error("Pipeline working directory was not removed", source))?;
    thread::sleep(Duration::from_millis(300));
    assert!(!pipeline_directory.exists());
    assert!(named_files(&runtime_root, "starts.received")?.is_empty());
    assert!(runner.is_running()?);
    let output = runner.terminate_and_wait()?;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!runtime_root.exists());
    Ok(())
}

#[test]
fn runner_requests_all_pipeline_shutdowns_before_waiting_for_any_one() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    let directory = tempfile::tempdir()?;
    install_program(directory.path(), PluginInterface::Source)?;
    install_program(directory.path(), PluginInterface::Sink)?;
    for id in ["parallel-a", "parallel-b"] {
        let source = document_with_delayed_source_shutdown(id);
        write_document(directory.path(), id, &source)?;
    }
    let config_path = write_config(directory.path())?;
    let mut runner = TestRunner::spawn(&config_path)?;
    let runtime_root = wait_for_runtime_root(directory.path())?;
    wait_for_pipeline_ids(&runtime_root, 2, None).map_err(|source| {
        io::Error::new(
            source.kind(),
            format!("Two Pipelines did not reach Plugin READY: {source}"),
        )
    })?;

    runner.request_termination()?;
    let term_markers = wait_for_pipeline_shutdown_requests(&runtime_root, 2).map_err(|source| {
        io::Error::new(
            source.kind(),
            format!("Both Pipeline shutdown requests were not observed: {source}"),
        )
    })?;
    for marker in term_markers {
        fs::write(marker.with_file_name("allow-shutdown"), "release")?;
    }

    let output = runner.wait()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!runtime_root.exists());
    Ok(())
}

#[test]
fn pipeline_shutdown_timeout_is_recorded_without_failing_planned_runner_shutdown() -> io::Result<()>
{
    let _runner_cli_guard = runner_cli_test_guard();
    let directory = tempfile::tempdir()?;
    install_program(directory.path(), PluginInterface::Source)?;
    install_program(directory.path(), PluginInterface::Sink)?;
    let source = document_with_delayed_source_shutdown("forced-shutdown");
    write_document(directory.path(), "forced-shutdown", &source)?;
    let config_path = write_config_with_shutdown_timeout(directory.path(), 100)?;
    let mut runner = TestRunner::spawn(&config_path)?;
    let runtime_root = wait_for_runtime_root(directory.path())?;
    let pipeline = wait_for_plugin_markers(&runtime_root, None)?;
    let plugins = wait_for_process_markers(&runtime_root, "process.pid", 2)?;

    let output = runner.terminate_and_wait()?;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
    assert_eq!(
        runner_diagnostics(&output.stderr)?,
        "runner.pipeline_shutdown_timed_out: Pipeline shutdown exceeded its deadline: forced-shutdown\n"
    );
    wait_for_processes_to_exit(iter::once(pipeline).chain(plugins))?;
    assert!(!runtime_root.exists());
    Ok(())
}

#[test]
fn pipeline_shutdown_deadlines_run_in_parallel_and_reap_both_process_trees() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    let directory = tempfile::tempdir()?;
    install_program(directory.path(), PluginInterface::Source)?;
    install_program(directory.path(), PluginInterface::Sink)?;
    for id in ["deadline-a", "deadline-b"] {
        let source = document_with_delayed_source_shutdown(id);
        write_document(directory.path(), id, &source)?;
    }
    let config_path = write_config_with_shutdown_timeout(directory.path(), 1_000)?;
    let mut runner = TestRunner::spawn(&config_path)?;
    let runtime_root = wait_for_runtime_root(directory.path())?;
    let pipelines = wait_for_pipeline_ids(&runtime_root, 2, None)?;
    let plugins = wait_for_process_markers(&runtime_root, "process.pid", 4)?;

    runner.request_termination()?;
    wait_for_pipeline_shutdown_requests(&runtime_root, 2)?;
    let output = runner.wait()?;

    assert!(output.status.success());
    assert_eq!(
        runner_diagnostics(&output.stderr)?,
        "runner.pipeline_shutdown_timed_out: Pipeline shutdown exceeded its deadline: deadline-a\n\
runner.pipeline_shutdown_timed_out: Pipeline shutdown exceeded its deadline: deadline-b\n"
    );
    wait_for_processes_to_exit(pipelines.into_iter().chain(plugins))?;
    assert!(!runtime_root.exists());
    Ok(())
}

#[test]
fn unified_program_delete_failures_stop_all_pipelines_and_exit_nonzero() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    #[derive(Clone, Copy, Debug)]
    enum DeletionFault {
        NamespaceDrift,
        TombstoneCleanup,
    }

    for fault in [
        DeletionFault::NamespaceDrift,
        DeletionFault::TombstoneCleanup,
    ] {
        if matches!(fault, DeletionFault::TombstoneCleanup) && geteuid().is_root() {
            eprintln!("Skipping permission-denial scenario: it requires a non-root test user");
            continue;
        }
        let directory = tempfile::tempdir()?;
        install_program(directory.path(), PluginInterface::Source)?;
        install_program(directory.path(), PluginInterface::Sink)?;
        plugin_fixture::install_program(
            directory.path(),
            plugin_fixture::PluginInterface::SourceAndSink,
        )?;
        let namespace = directory
            .path()
            .join("plugins/programs/com.example.gateway");
        let package = namespace.join("1.0.0");
        let resources = package.join("resources");
        fs::create_dir(&resources)?;
        fs::set_permissions(&resources, fs::Permissions::from_mode(0o700))?;
        fs::write(resources.join("data.bin"), b"resource")?;
        fs::set_permissions(
            resources.join("data.bin"),
            fs::Permissions::from_mode(0o500),
        )?;
        for id in ["store-failure-a", "store-failure-b"] {
            write_document(
                directory.path(),
                id,
                &document_with_delayed_source_shutdown(id),
            )?;
        }
        let config_path = write_config_with_shutdown_timeout(directory.path(), 1_000)?;
        let config: serde_json::Value = serde_json::from_slice(&fs::read(&config_path)?)?;
        let address: SocketAddr = config["http"]["listenAddress"]
            .as_str()
            .ok_or_else(|| io::Error::other("HTTP address is missing from the fixture"))?
            .parse()
            .map_err(io::Error::other)?;
        let mut runner = TestRunner::spawn(&config_path)?;
        let runtime_root = wait_for_runtime_root(directory.path())?;
        let pipelines = wait_for_pipeline_ids(&runtime_root, 2, None)?;
        let plugins = wait_for_process_markers(&runtime_root, "process.pid", 4)?;
        let mut request = wait_until(|| {
            runner.ensure_running()?;
            match TcpStream::connect_timeout(&address, Duration::from_secs(1)) {
                Ok(stream) => Ok(Some(stream)),
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => Ok(None),
                Err(error) => Err(error),
            }
        })?;
        request.set_write_timeout(Some(PROCESS_DEADLINE))?;
        let _permission_guard = match fault {
            DeletionFault::NamespaceDrift => {
                fs::rename(
                    &namespace,
                    directory.path().join("externally-moved-program"),
                )?;
                fs::write(&namespace, b"not a namespace")?;
                None
            }
            DeletionFault::TombstoneCleanup => Some(NonWritableDirectory::new(&resources)?),
        };

        write!(
            request,
            "DELETE /plugins/com.example.gateway/1.0.0 HTTP/1.1\r\nHost: {address}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )?;
        request.flush()?;
        wait_for_pipeline_shutdown_requests(&runtime_root, 2)?;
        let output = runner.wait()?;

        let diagnostic = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "{fault:?}: {diagnostic}");
        assert!(output.stdout.is_empty());
        assert_eq!(
            diagnostic
                .lines()
                .filter(|line| line.starts_with("runner.management_failed:"))
                .count(),
            1,
            "{diagnostic}"
        );
        assert!(
            diagnostic.contains("Runner Plugin Program Store failed"),
            "{diagnostic}"
        );
        assert!(
            diagnostic.contains(namespace.to_string_lossy().as_ref()),
            "{diagnostic}"
        );
        match fault {
            DeletionFault::NamespaceDrift => {
                assert!(
                    diagnostic.contains("Plugin Store integrity is invalid"),
                    "{diagnostic}"
                );
                assert!(namespace.is_file());
            }
            DeletionFault::TombstoneCleanup => {
                assert!(
                    diagnostic.contains(".tenon-plugin-delete-1.0.0"),
                    "{diagnostic}"
                );
                assert!(diagnostic.contains("Permission denied"), "{diagnostic}");
                assert!(!package.exists());
                assert!(namespace.join(".tenon-plugin-delete-1.0.0").is_dir());
            }
        }
        wait_for_processes_to_exit(pipelines.into_iter().chain(plugins))?;
        assert!(!runtime_root.exists());
    }
    Ok(())
}

#[test]
fn committed_document_identity_mismatch_is_fatal_before_serving() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    let directory = tempfile::tempdir()?;
    let source = document("actual-id");
    write_document(directory.path(), "different-id", &source)?;
    let config_path = write_config(directory.path())?;

    let output = runner_command(&config_path).output()?;

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("runner.tenon_document_identity_mismatch")
    );
    assert!(
        directory
            .path()
            .join("pipelines")
            .read_dir()?
            .next()
            .is_none()
    );
    Ok(())
}

#[test]
fn unified_program_cleanup_failure_is_fatal_before_serving() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    if geteuid().is_root() {
        eprintln!("Skipping permission-denial scenario: it requires a non-root test user");
        return Ok(());
    }
    let directory = tempfile::tempdir()?;
    install_program(directory.path(), PluginInterface::Source)?;
    install_program(directory.path(), PluginInterface::Sink)?;
    let source = document("keep-document");
    write_document(directory.path(), "keep-document", &source)?;
    let namespace = directory.path().join("plugins/programs/com.example.modbus");
    let config_path = write_config(directory.path())?;
    let _permission_guard = NonWritableDirectory::new(&namespace)?;

    let output = TestRunner::spawn(&config_path)?.wait()?;

    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("plugin_store_internal_error"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("com.example.modbus"));
    let diagnostics = String::from_utf8_lossy(&output.stderr);
    let cleanup: serde_json::Value = diagnostics
        .lines()
        .find_map(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .filter(|value| value["event"] == "plugin_store_entry_cleanup")
        })
        .ok_or_else(|| io::Error::other("structured cleanup diagnostic is missing"))?;
    assert_eq!(cleanup["path"], namespace.to_string_lossy().as_ref());
    assert_eq!(cleanup["failedPath"], namespace.to_string_lossy().as_ref());
    assert_eq!(cleanup["reason"]["code"], "plugin_store_integrity_invalid");
    assert_eq!(cleanup["outcome"], "failed");
    assert!(
        directory
            .path()
            .join("pipelines")
            .read_dir()?
            .next()
            .is_none()
    );
    let document_path = directory
        .path()
        .join("tenon-documents")
        .join(format!("{}.jsonc", sha256_hex(b"keep-document")));
    assert_eq!(fs::read_to_string(document_path)?, source);
    Ok(())
}

#[test]
fn non_private_fixed_directory_is_fatal_before_serving() -> io::Result<()> {
    let _runner_cli_guard = runner_cli_test_guard();
    let directory = tempfile::tempdir()?;
    let document_store = directory.path().join("tenon-documents");
    fs::create_dir(&document_store)?;
    fs::set_permissions(&document_store, fs::Permissions::from_mode(0o755))?;
    let config_path = write_config(directory.path())?;

    let output = runner_command(&config_path).output()?;

    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("runner.private_directory_invalid"));
    assert_eq!(
        document_store.metadata()?.permissions().mode() & 0o7777,
        0o755
    );
    assert!(!directory.path().join("pipelines").exists());
    Ok(())
}

fn runner_cli_test_guard() -> MutexGuard<'static, ()> {
    RUNNER_CLI_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn runner_command(config_path: &Path) -> Command {
    runner_command_with_executable(Path::new(env!("CARGO_BIN_EXE_tenon")), config_path)
}

fn runner_diagnostics(stderr: &[u8]) -> io::Result<&str> {
    let diagnostics = std::str::from_utf8(stderr).map_err(io::Error::other)?;
    // Unlimited Documents remain usable without Linux cgroup delegation.
    // Allow its single startup notice while preserving every other diagnostic.
    if cfg!(target_os = "linux")
        && let Some((startup, remaining)) = diagnostics.split_once('\n')
        && startup.starts_with("runner.resource_limits_unavailable: ")
    {
        Ok(remaining)
    } else {
        Ok(diagnostics)
    }
}

fn runner_command_with_executable(executable: &Path, config_path: &Path) -> Command {
    let mut command = Command::new(executable);
    command
        .arg("--config")
        .arg(config_path)
        .env("TENON_STATE_DIRECTORY", "/ignored");
    command
}

fn replace_executable_with_script(executable: &Path, script: &str) -> io::Result<()> {
    let replacement = executable.with_extension("replacement");
    fs::write(&replacement, script)?;
    fs::set_permissions(&replacement, fs::Permissions::from_mode(0o500))?;
    fs::rename(replacement, executable)
}

fn write_config(state_directory: &Path) -> io::Result<PathBuf> {
    write_config_with_pipeline_timing(state_directory, 2_000, 2_000, 10, 20)
}

fn write_config_with_shutdown_timeout(
    state_directory: &Path,
    shutdown_timeout_ms: u64,
) -> io::Result<PathBuf> {
    write_config_with_pipeline_timing(state_directory, 2_000, shutdown_timeout_ms, 10, 20)
}

fn write_config_with_pipeline_timing(
    state_directory: &Path,
    startup_timeout_ms: u64,
    shutdown_timeout_ms: u64,
    retry_initial_delay_ms: u64,
    retry_maximum_delay_ms: u64,
) -> io::Result<PathBuf> {
    let path = state_directory.join("runner.jsonc");
    let http_address = TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    let state_directory = serde_json::to_string(
        state_directory
            .to_str()
            .ok_or_else(|| io::Error::other("Runner state path is not UTF-8"))?,
    )?;
    fs::write(
        &path,
        format!(
            r#"{{"stateDirectory":{state_directory},"http":{{"listenAddress":"{http_address}"}},"pipeline":{{"startupTimeoutMs":{startup_timeout_ms},"shutdownTimeoutMs":{shutdown_timeout_ms},"retryBackoff":{{"initialDelayMs":{retry_initial_delay_ms},"maximumDelayMs":{retry_maximum_delay_ms}}}}},"lua":{{"cpuTimeLimitMs":50,"memoryLimitBytes":16777216}}}}"#,
        ),
    )?;
    Ok(path)
}

fn write_document(state_directory: &Path, file_id: &str, source: &str) -> io::Result<()> {
    let directory = state_directory.join("tenon-documents");
    fs::create_dir_all(&directory)?;
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
    fs::write(
        directory.join(format!("{}.jsonc", sha256_hex(file_id.as_bytes()))),
        source,
    )
}

fn document(id: &str) -> String {
    document_with_source_program(id, "1.0.0")
}

#[expect(
    clippy::expect_used,
    reason = "the shared fixture serializes a JSON Value immediately before parsing"
)]
fn document_with_delayed_source_shutdown(id: &str) -> String {
    let mut document: serde_json::Value =
        serde_json::from_str(&document(id)).expect("The test document is valid JSON");
    document["pluginInstances"]["source"]["config"]["behavior"] = "delay-shutdown".into();
    document.to_string()
}

fn document_with_source_program(id: &str, source_exact_version: &str) -> String {
    serde_json::json!({
        "specVersion": "1",
        "id": id,
        "pluginInstances": {
            "source": {
                "programName": "com.example.modbus", "exactVersion": source_exact_version,
                "config": {"endpoint": "tcp://source"}
            },
            "primary": {
                "programName": "com.example.kafka", "exactVersion": "1.0.0",
                "config": {"endpoint": "tcp://sink"}
            }
        },
        "flows": {
            "main": {
                "parallelism": 1, "source": "source",
                "process": {"script": "function main(event) emit() end"},
                "sinks": ["primary"]
            }
        }
    })
    .to_string()
}

fn wait_for_runtime_root(state_directory: &Path) -> io::Result<PathBuf> {
    let pipelines = state_directory.join("pipelines");
    wait_until(|| {
        let entries = match pipelines.read_dir() {
            Ok(entries) => entries,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(source),
        };
        let mut roots = entries
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        roots.sort_unstable();
        match roots.as_slice() {
            [root] if root.join("tenon").is_file() => Ok(Some(root.clone())),
            [_] => Ok(None),
            [] => Ok(None),
            _ => Err(io::Error::other("Runner created multiple runtime roots")),
        }
    })
}

fn wait_for_replaced_runtime_root(state_directory: &Path, previous: &Path) -> io::Result<PathBuf> {
    let pipelines = state_directory.join("pipelines");
    wait_until(|| {
        let entries = match pipelines.read_dir() {
            Ok(entries) => entries,
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(source),
        };
        let roots = entries
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        if roots.len() != 1 || roots[0] == previous || !roots[0].join("tenon").is_file() {
            return Ok(None);
        }
        Ok(Some(roots[0].clone()))
    })
}

fn wait_for_plugin_markers(runtime_root: &Path, previous_pipeline: Option<Pid>) -> io::Result<Pid> {
    let pipelines = wait_for_pipeline_ids(runtime_root, 1, previous_pipeline)?;
    pipelines
        .into_iter()
        .next()
        .ok_or_else(|| io::Error::other("Ready Pipeline process id is unavailable"))
}

fn wait_for_logged_pipeline_attempts(
    runner: &mut TestRunner,
    path: &Path,
    expected_count: usize,
) -> io::Result<Vec<Pid>> {
    wait_until_for(REPEATED_STARTUP_DEADLINE, || {
        runner.ensure_running()?;
        let source = match fs::read_to_string(path) {
            Ok(source) if source.ends_with('\n') => source,
            Ok(_) => return Ok(None),
            Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(source),
        };
        let process_ids = source
            .lines()
            .map(parse_process_id)
            .collect::<io::Result<Vec<_>>>()?;
        if process_ids.len() < expected_count {
            return Ok(None);
        }
        Ok(Some(process_ids))
    })
}

fn wait_for_pipeline_ids(
    runtime_root: &Path,
    expected_count: usize,
    previous_pipeline: Option<Pid>,
) -> io::Result<Vec<Pid>> {
    wait_until(|| {
        let markers = named_files(runtime_root, "ready.received")?;
        if markers.len() != expected_count * 2 {
            return Ok(None);
        }
        let Some(process_ids) = markers
            .iter()
            .map(|path| read_complete_process_id(&path.with_file_name("parent.pid")))
            .collect::<io::Result<Option<Vec<_>>>>()?
        else {
            return Ok(None);
        };
        let mut counts = HashMap::new();
        for process_id in process_ids {
            *counts.entry(process_id).or_insert(0) += 1;
        }
        if counts.values().any(|count| *count != 2) {
            return Ok(None);
        }
        let mut pipelines = counts.into_keys().collect::<Vec<_>>();
        pipelines.sort_unstable_by_key(|process_id| process_id.as_raw_pid());
        if pipelines.len() != expected_count
            || previous_pipeline.is_some_and(|previous| pipelines.contains(&previous))
        {
            return Ok(None);
        }
        Ok(Some(pipelines))
    })
}

fn wait_for_pipeline_shutdown_requests(
    root: &Path,
    expected_pipelines: usize,
) -> io::Result<Vec<PathBuf>> {
    wait_until(|| {
        let files = named_files(root, "shutdown.received")?;
        let pipeline_directories = files
            .iter()
            .map(|path| {
                path.strip_prefix(root)
                    .map_err(io::Error::other)?
                    .components()
                    .next()
                    .map(|component| component.as_os_str().to_owned())
                    .ok_or_else(|| io::Error::other("Plugin marker has no Pipeline directory"))
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        Ok((pipeline_directories.len() == expected_pipelines).then_some(files))
    })
}

fn wait_for_process_markers(
    root: &Path,
    marker_name: &str,
    expected_count: usize,
) -> io::Result<Vec<Pid>> {
    wait_until(|| {
        let markers = named_files(root, marker_name)?;
        if markers.len() != expected_count {
            return Ok(None);
        }
        let Some(mut process_ids) = markers
            .iter()
            .map(|path| read_complete_process_id(path))
            .collect::<io::Result<Option<Vec<_>>>>()?
        else {
            return Ok(None);
        };
        process_ids.sort_unstable_by_key(|process_id| process_id.as_raw_pid());
        Ok(Some(process_ids))
    })
}

fn read_complete_process_id(path: &Path) -> io::Result<Option<Pid>> {
    let source = match fs::read_to_string(path) {
        Ok(source) if !source.is_empty() => source,
        Ok(_) => return Ok(None),
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(source),
    };
    parse_process_id(source.trim()).map(Some)
}

fn parse_process_id(source: &str) -> io::Result<Pid> {
    let process_id = source
        .parse::<i32>()
        .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))?;
    Pid::from_raw(process_id).ok_or_else(|| io::Error::other("Process id is invalid"))
}

fn wait_for_processes_to_exit(processes: impl IntoIterator<Item = Pid>) -> io::Result<()> {
    let processes = processes.into_iter().collect::<Vec<_>>();
    wait_until(|| {
        for process in &processes {
            match test_kill_process(*process) {
                Ok(()) => return Ok(None),
                Err(Errno::SRCH) => {}
                Err(source) => return Err(io::Error::from(source)),
            }
        }
        Ok(Some(()))
    })
}

fn wait_until<T>(probe: impl FnMut() -> io::Result<Option<T>>) -> io::Result<T> {
    wait_until_for(PROCESS_DEADLINE, probe)
}

fn wait_until_for<T>(
    timeout: Duration,
    mut probe: impl FnMut() -> io::Result<Option<T>>,
) -> io::Result<T> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = probe()? {
            return Ok(value);
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Runner condition did not become true before the test deadline",
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn test_step_error(step: &'static str, source: io::Error) -> io::Error {
    io::Error::new(source.kind(), format!("{step}: {source}"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

struct NonWritableDirectory(fs::File);

impl NonWritableDirectory {
    fn new(path: &Path) -> io::Result<Self> {
        // The open directory keeps cleanup valid even after a tombstone rename.
        let directory = fs::File::open(path)?;
        directory.set_permissions(fs::Permissions::from_mode(0o500))?;
        Ok(Self(directory))
    }
}

impl Drop for NonWritableDirectory {
    fn drop(&mut self) {
        let _ = self.0.set_permissions(fs::Permissions::from_mode(0o700));
    }
}

struct TestRunner {
    child: Option<Child>,
}

impl TestRunner {
    fn spawn(config_path: &Path) -> io::Result<Self> {
        Self::spawn_command(runner_command(config_path))
    }

    fn spawn_command(mut command: Command) -> io::Result<Self> {
        let child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        Ok(Self { child: Some(child) })
    }

    fn is_running(&mut self) -> io::Result<bool> {
        Ok(self.child_mut()?.try_wait()?.is_none())
    }

    fn ensure_running(&mut self) -> io::Result<()> {
        if self.child_mut()?.try_wait()?.is_none() {
            return Ok(());
        }
        let output = self
            .child
            .take()
            .ok_or_else(|| io::Error::other("Runner process owner is unavailable"))?
            .wait_with_output()?;
        Err(io::Error::other(format!(
            "Runner exited before the expected condition: status={}; stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )))
    }

    fn terminate_and_wait(&mut self) -> io::Result<Output> {
        self.request_termination()?;
        self.wait()
    }

    fn request_termination(&mut self) -> io::Result<()> {
        terminate(self.child_mut()?)
    }

    fn kill_and_wait(&mut self) -> io::Result<Output> {
        let process_id = child_process_id(self.child_mut()?)?;
        kill_process(process_id, Signal::KILL).map_err(io::Error::from)?;
        self.wait()
    }

    fn wait(&mut self) -> io::Result<Output> {
        let deadline = Instant::now() + PROCESS_DEADLINE;
        loop {
            if self.child_mut()?.try_wait()?.is_some() {
                return self
                    .child
                    .take()
                    .ok_or_else(|| io::Error::other("Runner process owner is unavailable"))?
                    .wait_with_output();
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Runner did not stop before the test deadline",
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn child_mut(&mut self) -> io::Result<&mut Child> {
        self.child
            .as_mut()
            .ok_or_else(|| io::Error::other("Runner process owner is unavailable"))
    }
}

impl Drop for TestRunner {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        if child.try_wait().ok().flatten().is_none() {
            let _ = terminate(&child);
        }
        let deadline = Instant::now() + PROCESS_DEADLINE;
        while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if child.try_wait().ok().flatten().is_none() {
            let _ = child.kill();
        }
        let _ = child.wait();
    }
}

fn terminate(child: &Child) -> io::Result<()> {
    kill_process(child_process_id(child)?, Signal::TERM).map_err(io::Error::from)
}

fn child_process_id(child: &Child) -> io::Result<Pid> {
    child
        .id()
        .try_into()
        .ok()
        .and_then(Pid::from_raw)
        .ok_or_else(|| io::Error::other("Runner process id is unavailable"))
}
