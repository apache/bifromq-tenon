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

//! Proves that a distribution can use package protection through public APIs.

use super::*;
use std::os::unix::fs::PermissionsExt as _;

fn envelope(interface: PluginInterface, customer: &str) -> io::Result<Vec<u8>> {
    let mut bytes = format!("fixture-package:{customer}\n").into_bytes();
    bytes.extend(
        plugin_fixture::plugin_package(interface)?
            .into_iter()
            .map(|byte| byte ^ 0x80),
    );
    Ok(bytes)
}

fn file_package(interface: PluginInterface, customer: &str) -> io::Result<Vec<u8>> {
    let mut bytes = format!("fixture-files:{customer}\n").into_bytes();
    serde_json::to_writer(&mut bytes, &plugin_fixture::program_files(interface, None)?)?;
    Ok(bytes)
}

fn install(address: SocketAddr, body: &[u8]) -> io::Result<runner_http_support::HttpResponse> {
    request(
        address,
        "POST",
        "/plugins",
        &[("Content-Type", "application/octet-stream")],
        body,
    )
}

#[test]
fn envelopes_survive_restart_while_runtime_materials_follow_process_lifetime() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    let policy = root.path().join("license.json");
    let log = root.path().join("scopes.jsonl");
    super::policy_changes::publish(&policy, true, 60_000)?;
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"policyFile": policy, "scopeLog": log}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let source = envelope(PluginInterface::Source, "customer-a")?;
    let sink = file_package(PluginInterface::Sink, "customer-a")?;
    for body in [&source, &sink] {
        assert_eq!(install(address, body)?.status, 201);
        assert_eq!(install(address, body)?.status, 204);
    }
    let saved = root
        .path()
        .join("plugins/programs/com.example.modbus/.tenon-artifact-92521fc3cbd964bdc9f584a991b89fddaa5754ed1cc96d6d42445338669c1305");
    assert_eq!(fs::read(&saved)?, source);
    assert!(file_tree::named_files(&root.path().join("plugins"), "manifest.json")?.is_empty());
    let created = request(
        address,
        "PUT",
        "/documents/protected-plugin",
        &[
            ("Content-Type", "application/jsonc"),
            ("If-None-Match", "*"),
        ],
        &document("protected-plugin", "test"),
    )?;
    assert_eq!(created.status, 201, "{}", created.body_text());
    let processes = running_processes(root.path())?;
    super::policy_changes::publish(&policy, true, 120_000)?;
    runner_http_support::wait_until(|| {
        Ok((fs::read_to_string(&log)?.lines().count() == 3).then_some(()))
    })?;
    assert_eq!(running_processes(root.path())?, processes);
    let material = file_tree::named_files(&root.path().join("pipelines"), "manifest.json")?;
    assert_eq!(material.len(), 2);
    assert_eq!(
        request(
            address,
            "DELETE",
            "/plugins/com.example.modbus/1.0.0",
            &[],
            &[]
        )?
        .status,
        409
    );
    runner.terminate()?;
    assert_reaped(&processes);
    assert!(material.iter().all(|path| !path.exists()));
    assert_eq!(fs::read(&saved)?, source);

    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    let processes = running_processes(root.path())?;
    wait_for_http(&mut runner, address)?;
    assert_eq!(
        request(address, "GET", "/documents/protected-plugin", &[], &[])?.status,
        200
    );
    super::policy_changes::publish(&policy, false, 120_000)?;
    runner.wait_for_failure("fixture.license_denied")?;
    assert_reaped(&processes);

    let wrong = configure(
        root.path(),
        address,
        serde_json::json!({"customer": "customer-b"}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&wrong, Path::new(EXECUTABLE))?;
    runner.wait_for_failure("plugin_package_open_failed")?;
    assert_eq!(fs::read(&saved)?, source);
    assert_eq!(fs::read_dir(root.path().join("pipelines"))?.count(), 0);
    Ok(())
}

#[test]
fn custom_file_format_import_uses_access_checks_and_stock_runner_preserves_it() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    let config = configure(root.path(), address, serde_json::json!({}))?;
    let namespace = root.path().join("plugins/programs/com.example.modbus");
    fs::create_dir_all(&namespace)?;
    for directory in [
        root.path().join("plugins"),
        root.path().join("plugins/programs"),
    ] {
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    }
    fs::set_permissions(&namespace, fs::Permissions::from_mode(0o700))?;
    let path = namespace
        .join(".tenon-artifact-92521fc3cbd964bdc9f584a991b89fddaa5754ed1cc96d6d42445338669c1305");
    let bytes = file_package(PluginInterface::Source, "customer-a")?;
    fs::write(&path, &bytes)?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    let mut stock = TestRunner::spawn(&config)?;
    stock.wait_for_failure("plugin_store_integrity_invalid")?;
    assert_eq!(fs::read(&path)?, bytes);
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    assert_eq!(
        request(
            address,
            "GET",
            "/plugins/com.example.modbus/1.0.0",
            &[],
            &[]
        )?
        .status,
        200
    );
    let wrong = envelope(PluginInterface::Sink, "customer-b")?;
    assert_eq!(install(address, &wrong)?.status, 422);
    let invalid = b"fixture-package:customer-a\ninvalid";
    assert_eq!(install(address, invalid)?.status, 422);
    assert_eq!(
        request(
            address,
            "DELETE",
            "/plugins/com.example.modbus/1.0.0",
            &[],
            &[]
        )?
        .status,
        204
    );
    assert!(!path.exists());
    assert!(file_tree::named_files(&root.path().join("pipelines"), "manifest.json")?.is_empty());
    runner.terminate()
}

#[test]
fn standard_packages_survive_restart_and_rebuild_runtime_materials() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    let config = configure(root.path(), address, serde_json::json!({}))?;
    let package = plugin_fixture::plugin_package(PluginInterface::Source)?;
    let saved = plugin_fixture::stored_package_path(root.path(), "com.example.modbus", "1.0.0");
    let mut stock = TestRunner::spawn(&config)?;
    wait_for_http(&mut stock, address)?;
    assert_eq!(install(address, &package)?.status, 201);
    let runtime =
        plugin_fixture::runtime_program_directory(root.path(), "com.example.modbus", "1.0.0")?;
    assert!(runtime.join("manifest.json").is_file());
    assert_eq!(fs::read(&saved)?, package);
    assert!(file_tree::named_files(&root.path().join("plugins"), "manifest.json")?.is_empty());
    stock.terminate()?;
    assert!(!runtime.exists());

    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    let rebuilt =
        plugin_fixture::runtime_program_directory(root.path(), "com.example.modbus", "1.0.0")?;
    assert_ne!(runtime, rebuilt);
    assert!(rebuilt.join("manifest.json").is_file());
    assert_eq!(fs::read(&saved)?, package);
    assert_eq!(
        request(
            address,
            "DELETE",
            "/plugins/com.example.modbus/1.0.0",
            &[],
            &[]
        )?
        .status,
        204
    );
    assert!(!saved.exists());
    assert!(!rebuilt.exists());
    runner.terminate()
}
