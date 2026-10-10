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

//! Exercises file renewal and revocation without a Document write.

use super::*;
use std::os::unix::fs::PermissionsExt as _;

pub(super) fn publish(path: &Path, allowed: bool, valid_for_ms: u64) -> io::Result<()> {
    let temporary = path.with_extension("new");
    fs::write(
        &temporary,
        serde_json::to_vec(&serde_json::json!({
            "customer": "customer-a", "allowed": allowed, "valid_for_ms": valid_for_ms,
        }))?,
    )?;
    fs::rename(temporary, path)
}

#[test]
fn initial_denial_precedes_document_recovery_even_when_no_document_can_parse() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    let policy = root.path().join("license.json");
    publish(&policy, false, 60_000)?;
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"policyFile": policy}),
    )?;
    let path = formal_file(root.path(), "invalid");
    fs::create_dir_all(
        path.parent()
            .ok_or_else(|| io::Error::other("Document parent is absent"))?,
    )?;
    fs::set_permissions(
        root.path().join("tenon-documents"),
        fs::Permissions::from_mode(0o700),
    )?;
    fs::write(&path, b"invalid")?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    runner.wait_for_failure("fixture.license_denied")?;
    assert_eq!(fs::read(path)?, b"invalid");
    Ok(())
}

#[test]
fn idle_runner_renews_and_rejects_an_invalid_replacement() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    let policy = root.path().join("license.json");
    let log = root.path().join("scopes.jsonl");
    publish(&policy, true, 60_000)?;
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"policyFile": policy, "scopeLog": log}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    publish(&policy, true, 1_500)?;
    runner_http_support::wait_until(|| {
        Ok((fs::read_to_string(&log)?.lines().count() == 2).then_some(()))
    })?;
    fs::write(&policy, b"invalid renewal")?;
    // Only the accepted update can set the deadline. No HTTP call follows it.
    runner.wait_for_failure("runner.execution_permit_expired")?;
    let scopes = fs::read_to_string(&log)?;
    assert!(scopes.lines().all(|line| line == "[]"));
    assert_eq!(scopes.lines().count(), 2);
    Ok(())
}

#[test]
fn revocation_rechecks_unready_documents_and_preserves_their_files() -> io::Result<()> {
    let root = tempfile::tempdir()?;
    let address = available_address()?;
    let policy = root.path().join("license.json");
    let log = root.path().join("scopes.jsonl");
    publish(&policy, true, 60_000)?;
    let config = configure(
        root.path(),
        address,
        serde_json::json!({"policyFile": policy, "scopeLog": log}),
    )?;
    let mut runner = TestRunner::spawn_with_executable(&config, Path::new(EXECUTABLE))?;
    wait_for_http(&mut runner, address)?;
    assert_eq!(
        request(
            address,
            "PUT",
            "/documents/waiting",
            &[
                ("Content-Type", "application/jsonc"),
                ("If-None-Match", "*"),
            ],
            &document("waiting", "unready")
        )?
        .status,
        201
    );
    let path = formal_file(root.path(), "waiting");
    let stored = fs::read(&path)?;
    publish(&policy, false, 60_000)?;
    runner.wait_for_failure("fixture.license_denied")?;
    assert_eq!(fs::read(&path)?, stored);
    let scopes = fs::read_to_string(&log)?;
    let last: Value = serde_json::from_str(
        scopes
            .lines()
            .last()
            .ok_or_else(|| io::Error::other("Policy scope is absent"))?,
    )?;
    assert_eq!(last[0]["id"], "waiting");
    assert_eq!(last.as_array().map(Vec::len), Some(1));
    Ok(())
}
