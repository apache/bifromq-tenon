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

use super::{RunnerConfig, RunnerConfigError, validate_schema};
use crate::contracts::runner::config_schema_bytes;
use crate::strict_jsonc::parse_jsonc;
use proptest::test_runner::TestCaseError;
use serde::Deserialize;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::{env, fs, io, process};

const SCHEMA_VECTORS: &[u8] =
    include_bytes!("../../contracts/runner/test-fixtures/config.schema-test-vectors.json");
const CONFIG_VECTORS: &[u8] =
    include_bytes!("../../contracts/runner/test-fixtures/config.test-vectors.json");
static TEMPORARY_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[test]
fn metrics_defaults_and_filters_are_validated_before_startup() -> io::Result<()> {
    let vectors: ConfigVectors = serde_json::from_slice(CONFIG_VECTORS)?;
    let mut source = parse_jsonc(vectors.valid[0].source.as_bytes()).map_err(io::Error::other)?;
    let default = RunnerConfig::parse(&serde_json::to_vec(&source)?).map_err(io::Error::other)?;
    assert_eq!(default.metrics().node_id(), None);
    assert_eq!(default.metrics().timeout(), Duration::from_millis(2000));
    source["metrics"] = serde_json::json!({"nodeId":"gateway-a","collectionTimeoutMs":100});
    let config = RunnerConfig::parse(&serde_json::to_vec(&source)?).map_err(io::Error::other)?;
    assert_eq!(config.metrics().node_id(), Some("gateway-a"));
    assert_eq!(config.metrics().timeout(), Duration::from_millis(100));
    Ok(())
}

proptest::proptest! {
    #[test]
    fn client_ca_paths_are_preserved_or_rejected(name in "[a-zA-Z0-9_-]{1,32}", absolute in proptest::bool::ANY) {
        let path = if absolute { format!("/etc/tenon/{name}.pem") } else { format!("{name}.pem") };
        let vectors: SchemaVectors = serde_json::from_slice(SCHEMA_VECTORS)?;
        let mut source = vectors.valid.into_iter().find(|vector| vector.name == "HTTPS server identity").ok_or_else(|| TestCaseError::fail("Missing HTTPS vector"))?.config;
        source["http"]["tls"]["clientCaFile"] = path.clone().into();
        let result = RunnerConfig::parse(&serde_json::to_vec(&source)?);
        if absolute {
            let config = result.map_err(|error| TestCaseError::fail(error.to_string()))?;
            proptest::prop_assert_eq!(config.http_tls().and_then(|tls| tls.client_ca_file.as_deref()), Some(Path::new(&path)));
        } else {
            proptest::prop_assert!(matches!(result, Err(RunnerConfigError::PathNotAbsolute { path: "/http/tls/clientCaFile" })), "Relative CA path was accepted");
        }
    }

    #[test]
    fn tls_settings_preserve_valid_inputs_and_reject_relative_paths(name in "[a-zA-Z0-9_-]{1,32}", absolute in proptest::bool::ANY, timeout_ms in 1_u64..=u64::MAX) {
        let prefix = if absolute { "/etc/tenon/" } else { "" };
        let certificate = format!("{prefix}{name}.pem");
        let key = format!("{prefix}{name}-key.pem");
        let source = serde_json::json!({
            "stateDirectory": "/var/lib/tenon",
            "http": {"listenAddress": "127.0.0.1:8080", "tls": {
                "certificateChainFile": certificate, "privateKeyFile": key, "handshakeTimeoutMs": timeout_ms
            }},
            "pipeline": {"retryBackoff": {"initialDelayMs": 100, "maximumDelayMs": 30000}},
            "lua": {"cpuTimeLimitMs": 50, "memoryLimitBytes": 16777216}
        });
        let result = RunnerConfig::parse(&serde_json::to_vec(&source)?);
        if absolute {
            let config = result.map_err(|error| TestCaseError::fail(error.to_string()))?;
            proptest::prop_assert_eq!(config.http_tls().map(|files| &files.certificate_chain_file), Some(&PathBuf::from(certificate)));
            proptest::prop_assert_eq!(config.http_tls().map(|files| &files.private_key_file), Some(&PathBuf::from(key)));
            proptest::prop_assert_eq!(config.http_tls().map(|tls| tls.handshake_timeout), Some(Duration::from_millis(timeout_ms)));
        } else {
            proptest::prop_assert!(matches!(result, Err(RunnerConfigError::PathNotAbsolute { path: "/http/tls/certificateChainFile" })), "Relative certificate path was accepted");
        }
    }
}

#[test]
fn extension_values_are_frozen_and_excluded_from_debug_output() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("runner.jsonc");
    let vectors: ConfigVectors = serde_json::from_slice(CONFIG_VECTORS)?;
    let mut source = parse_jsonc(vectors.valid[0].source.as_bytes()).map_err(io::Error::other)?;
    source["extra"] = serde_json::json!({
        "license": "private-test-license",
        "nested": [{"enabled": true}, null, 42]
    });
    fs::write(&path, serde_json::to_vec(&source)?)?;
    let config = RunnerConfig::load(&path).map_err(io::Error::other)?;
    fs::write(&path, b"invalid replacement")?;
    assert_eq!(serde_json::to_value(config.extra())?, source["extra"]);
    assert!(!format!("{config:?}").contains("private-test-license"));
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaVectors {
    valid: Vec<SchemaValidVector>,
    invalid: Vec<SchemaInvalidVector>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaValidVector {
    name: String,
    config: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SchemaInvalidVector {
    name: String,
    config: Value,
    expected_instance_pointer: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfigVectors {
    format_version: u32,
    valid: Vec<ValidConfigVector>,
    invalid: Vec<InvalidConfigVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ValidConfigVector {
    name: String,
    source: String,
    expected: ExpectedConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExpectedConfig {
    state_directory: String,
    http_listen_address: String,
    http_tls: Option<ExpectedTlsConfig>,
    startup_timeout_ms: u64,
    shutdown_timeout_ms: u64,
    reconfigure_timeout_ms: u64,
    retry_backoff_initial_delay_ms: u64,
    retry_backoff_maximum_delay_ms: u64,
    lua_cpu_time_limit_ms: u64,
    lua_memory_limit_bytes: usize,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExpectedTlsConfig {
    certificate_chain_file: PathBuf,
    private_key_file: PathBuf,
    client_ca_file: Option<PathBuf>,
    handshake_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvalidConfigVector {
    name: String,
    source: String,
    expected_code: String,
    expected_path: String,
}

#[test]
fn embedded_schema_is_exact_and_valid_draft_2020_12() -> io::Result<()> {
    let repository_schema = include_bytes!("../../contracts/runner/config.schema.json");
    assert_eq!(config_schema_bytes(), repository_schema);

    let schema: Value = serde_json::from_slice(config_schema_bytes()).map_err(io::Error::other)?;
    assert!(
        jsonschema::draft202012::meta::validate(&schema).is_ok(),
        "the embedded Runner configuration Schema must be valid Draft 2020-12"
    );
    assert!(refs_are_internal(&schema));
    Ok(())
}

#[test]
fn shared_schema_vectors_have_one_stable_result() -> io::Result<()> {
    let vectors: SchemaVectors =
        serde_json::from_slice(SCHEMA_VECTORS).map_err(io::Error::other)?;

    for vector in vectors.valid {
        validate_schema(&vector.config).map_err(|error| {
            io::Error::other(format!(
                "valid Runner configuration vector {} was rejected: {error}",
                vector.name
            ))
        })?;
    }
    for vector in vectors.invalid {
        let Err(error) = validate_schema(&vector.config) else {
            return Err(io::Error::other(format!(
                "invalid Runner configuration vector {} was accepted",
                vector.name
            )));
        };
        assert_eq!(error.code(), "runner_config.schema_invalid");
        assert_eq!(
            error_path(&error),
            vector.expected_instance_pointer,
            "invalid vector {} changed its error location",
            vector.name
        );
    }
    Ok(())
}

#[test]
fn shared_runtime_vectors_lock_defaults_and_domain_errors() -> io::Result<()> {
    let vectors: ConfigVectors =
        serde_json::from_slice(CONFIG_VECTORS).map_err(io::Error::other)?;
    assert_eq!(
        vectors.format_version, 1,
        "Unexpected vector format version"
    );

    for vector in vectors.valid {
        let config = RunnerConfig::parse(vector.source.as_bytes()).map_err(|error| {
            io::Error::other(format!(
                "valid Runner configuration vector {} was rejected: {error}",
                vector.name
            ))
        })?;
        assert_eq!(
            config.http_tls().map(|files| (
                &files.certificate_chain_file,
                &files.private_key_file,
                &files.client_ca_file
            )),
            vector.expected.http_tls.as_ref().map(|files| (
                &files.certificate_chain_file,
                &files.private_key_file,
                &files.client_ca_file
            )),
            "valid vector {} changed HTTPS identity paths",
            vector.name,
        );
        assert_eq!(
            config.http_tls().map(|tls| tls.handshake_timeout),
            vector
                .expected
                .http_tls
                .as_ref()
                .map(|tls| Duration::from_millis(tls.handshake_timeout_ms)),
            "valid vector {} changed HTTPS handshake timeout",
            vector.name,
        );
        assert_eq!(
            config.pipeline_startup_timeout(),
            Duration::from_millis(vector.expected.startup_timeout_ms),
            "valid vector {} changed startup timeout",
            vector.name
        );
        assert_eq!(
            config.pipeline_shutdown_timeout(),
            Duration::from_millis(vector.expected.shutdown_timeout_ms),
            "valid vector {} changed shutdown timeout",
            vector.name
        );
        assert_eq!(
            config.pipeline_reconfigure_timeout(),
            Duration::from_millis(vector.expected.reconfigure_timeout_ms),
            "valid vector {} changed reconfiguration timeout",
            vector.name
        );
    }

    for vector in vectors.invalid {
        let Err(error) = RunnerConfig::parse(vector.source.as_bytes()) else {
            return Err(io::Error::other(format!(
                "invalid Runner configuration vector {} was accepted",
                vector.name
            )));
        };
        assert_eq!(
            error.code(),
            vector.expected_code,
            "invalid vector {} changed its error category",
            vector.name
        );
        assert_eq!(
            error_path(&error),
            vector.expected_path,
            "invalid vector {} changed its error location",
            vector.name
        );
    }
    Ok(())
}

#[test]
fn reconfiguration_timeout_is_accepted_and_invalid_values_are_rejected() -> io::Result<()> {
    let vectors: ConfigVectors =
        serde_json::from_slice(CONFIG_VECTORS).map_err(io::Error::other)?;
    let mut config = parse_jsonc(vectors.valid[0].source.as_bytes()).map_err(io::Error::other)?;
    config["pipeline"]["reconfigureTimeoutMs"] = Value::from(15_000);
    RunnerConfig::parse(&serde_json::to_vec(&config)?).map_err(io::Error::other)?;

    for invalid in [
        Value::from(0),
        Value::from(-1),
        Value::from(0.5),
        Value::from("30000"),
        Value::Null,
    ] {
        config["pipeline"]["reconfigureTimeoutMs"] = invalid;
        let Err(error) = RunnerConfig::parse(&serde_json::to_vec(&config)?) else {
            return Err(io::Error::other(
                "Invalid reconfiguration timeout was accepted",
            ));
        };
        assert_eq!(error.code(), "runner_config.schema_invalid");
        assert_eq!(error_path(&error), "/pipeline/reconfigureTimeoutMs");
    }
    Ok(())
}

proptest::proptest! {
    #[test]
    fn positive_reconfiguration_timeouts_preserve_exact_milliseconds(timeout_ms in 1_u64..=u64::MAX) {
        let vectors: ConfigVectors = serde_json::from_slice(CONFIG_VECTORS)?;
        let mut source = parse_jsonc(vectors.valid[0].source.as_bytes())?;
        source["pipeline"]["reconfigureTimeoutMs"] = Value::from(timeout_ms);
        let config = RunnerConfig::parse(&serde_json::to_vec(&source)?)?;
        proptest::prop_assert_eq!(config.pipeline_reconfigure_timeout(), Duration::from_millis(timeout_ms));
        proptest::prop_assert_eq!(config.pipeline_startup_timeout(), Duration::from_millis(vectors.valid[0].expected.startup_timeout_ms));
        proptest::prop_assert_eq!(config.pipeline_shutdown_timeout(), Duration::from_millis(vectors.valid[0].expected.shutdown_timeout_ms));
    }
}

#[test]
fn complete_configuration_projects_into_exact_domain_types() -> io::Result<()> {
    let vectors: ConfigVectors =
        serde_json::from_slice(CONFIG_VECTORS).map_err(io::Error::other)?;
    let Some(vector) = vectors.valid.get(1) else {
        return Err(io::Error::other(
            "complete valid configuration vector is missing",
        ));
    };
    let config = RunnerConfig::parse(vector.source.as_bytes()).map_err(io::Error::other)?;

    assert_eq!(
        config.state_directory(),
        PathBuf::from(&vector.expected.state_directory)
    );
    assert_eq!(
        config.http_listen_address().to_string(),
        vector.expected.http_listen_address
    );
    assert_eq!(
        config.retry_initial_delay(),
        Duration::from_millis(vector.expected.retry_backoff_initial_delay_ms)
    );
    assert_eq!(
        config.retry_maximum_delay(),
        Duration::from_millis(vector.expected.retry_backoff_maximum_delay_ms)
    );
    assert_eq!(
        config.lua_cpu_time_limit(),
        Duration::from_millis(vector.expected.lua_cpu_time_limit_ms)
    );
    assert_eq!(
        config.lua_memory_limit_bytes().get(),
        vector.expected.lua_memory_limit_bytes
    );
    Ok(())
}

#[test]
fn loaded_snapshot_does_not_follow_later_file_changes() -> io::Result<()> {
    let vectors: ConfigVectors =
        serde_json::from_slice(CONFIG_VECTORS).map_err(io::Error::other)?;
    let Some(original) = vectors.valid.first() else {
        return Err(io::Error::other("default configuration vector is missing"));
    };
    let Some(replacement) = vectors.valid.get(1) else {
        return Err(io::Error::other("complete configuration vector is missing"));
    };
    let path = temporary_file_path();
    fs::write(&path, &original.source)?;
    let config = RunnerConfig::load(&path).map_err(io::Error::other)?;
    fs::write(&path, &replacement.source)?;
    fs::remove_file(&path)?;

    assert_eq!(
        config.state_directory(),
        PathBuf::from(&original.expected.state_directory)
    );
    assert_eq!(
        config.pipeline_reconfigure_timeout(),
        Duration::from_millis(original.expected.reconfigure_timeout_ms)
    );
    Ok(())
}

#[test]
fn load_rejects_relative_path_before_file_access() -> io::Result<()> {
    let Err(error) = RunnerConfig::load(PathBuf::from("runner.jsonc").as_path()) else {
        return Err(io::Error::other(
            "relative Runner configuration path was accepted",
        ));
    };
    assert_eq!(error.code(), "runner_config.path_not_absolute");
    Ok(())
}

#[test]
fn invalid_utf8_is_rejected_before_json_parsing() -> io::Result<()> {
    let Err(error) = RunnerConfig::parse(&[0xff]) else {
        return Err(io::Error::other(
            "invalid UTF-8 Runner configuration was accepted",
        ));
    };
    assert_eq!(error.code(), "runner_config.utf8_invalid");
    Ok(())
}

fn error_path(error: &RunnerConfigError) -> &str {
    match error {
        RunnerConfigError::SchemaInvalid { path } => path,
        RunnerConfigError::PathNotAbsolute { path } | RunnerConfigError::ValueInvalid { path } => {
            path
        }
        RunnerConfigError::RetryBackoffInvalid => "/pipeline/retryBackoff",
        _ => "",
    }
}

#[test]
fn immutable_configuration_can_cross_runner_worker_boundaries() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<RunnerConfig>();
}

fn temporary_file_path() -> PathBuf {
    let sequence = TEMPORARY_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    env::temp_dir().join(format!(
        "tenon-runner-config-{}-{sequence}.jsonc",
        process::id()
    ))
}

fn refs_are_internal(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().all(refs_are_internal),
        Value::Object(entries) => {
            let local_ref = entries.get("$ref").is_none_or(|reference| {
                reference.as_str().is_some_and(|path| path.starts_with('#'))
            });
            local_ref && entries.values().all(refs_are_internal)
        }
        _ => true,
    }
}
