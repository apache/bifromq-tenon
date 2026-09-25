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

use super::{RuntimeResolution, RuntimeResolver};
use crate::config::ScriptVmLimits;
use crate::identifiers::{ExactVersion, ProgramName, SinkContractId};
use crate::lua::{EmitBoundary, LuaVm};
use crate::payload_contract::PluginInterface;
use crate::pipeline::test_support::PipelineRevision;
use crate::runner::document_store::TenonDocumentEtag;
use crate::runner::pipeline::PipelineLifecycleTarget;
use crate::runner::plugin::package::tests::{archive, valid_program_descriptor};
use crate::runner::plugin::store::{PluginProgramInstallResult, PluginStoreError};
use crate::runner::plugin::store::{PluginProgramStore, PluginUninstallOutcome};
use crate::runner::process_resources;
use crate::tenon_document::UnverifiedTenonDocument;
use crate::tenon_document::verified::{TenonDocumentVerifier, VerifiedTenonDocument};
use prost::Message as _;
use prost_reflect::{DynamicMessage, Value as ProtobufValue};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs;
use std::io::{self, Cursor};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn shared_vectors_resolve_and_reconstruct_real_installed_programs() -> io::Result<()> {
    let vectors: RuntimeVectors = serde_json::from_slice(include_bytes!(
        "../../../../contracts/tenon-document/v1.runtime-resolution-test-vectors.json"
    ))?;
    assert_eq!(vectors.format_version, 1);
    assert_eq!(vectors.cases.len(), 8);
    let limits = limits()?;
    let verifier = TenonDocumentVerifier::try_new(limits).map_err(io::Error::other)?;
    let resolver = RuntimeResolver::new(limits, process_resources::available_cpu_count()?);

    for vector in vectors.cases {
        let (_directory, mut store) = empty_store()?;
        for program in &vector.available_programs {
            install(&mut store, program)?;
        }
        let document = verify(&verifier, &vector.document)?;
        let resolution = resolver.resolve(&document, &store);
        let actual = match resolution {
            RuntimeResolution::Ready(plan) => {
                assert!(Arc::ptr_eq(plan.document(), &document));
                let etag = TenonDocumentEtag::for_source(document.strict_json().as_bytes());
                let target = PipelineLifecycleTarget::new(plan, etag);
                let wire = target.revision();
                let decoded = crate::contracts::core::PipelineRevisionPlan::decode(
                    wire.encode_to_vec().as_slice(),
                )
                .map_err(io::Error::other)?;
                let received = PipelineRevision::from_runner(decoded);
                assert_eq!(received.document_etag(), etag.strong_value());
                assert_eq!(serde_json::to_value(received.document())?, vector.document);
                json!({
                    "kind": "ready",
                    "programCount": received.programs().len(),
                    "pluginInstanceCount": received.document().plugin_instances().len(),
                    "flowCount": received.document().flows().len()
                })
            }
            RuntimeResolution::Unready(issues) => json!({"kind": "unready", "issues": issues}),
        };
        assert_eq!(actual, vector.expected, "{}", vector.name);
        for program in &vector.available_programs {
            let (name, version) = identity(program)?;
            assert_eq!(
                store.uninstall(&name, &version).map_err(io::Error::other)?,
                PluginUninstallOutcome::Uninstalled,
                "{}",
                vector.name
            );
        }
    }
    Ok(())
}

#[test]
fn missing_programs_aggregate_without_hiding_independent_flow_failures() -> io::Result<()> {
    let (_directory, mut store) = empty_store()?;
    let available = program("com.example.available", "source-and-sink");
    install(&mut store, &available)?;
    let document = json!({
        "specVersion": "1", "id": "independent-errors",
        "pluginInstances": {
            "missing-a": instance("com.example.missing-a"),
            "missing-a-again": instance("com.example.missing-a"),
            "missing-b": instance("com.example.missing-b"),
            "installed": instance("com.example.available")
        },
        "flows": {
            "missing": flow("missing-a", &["missing-a-again", "missing-b"], "error('private runtime marker')"),
            "independent": flow("installed", &["installed"], "error('private runtime marker')")
        }
    });
    let (verifier, resolver) = validators()?;
    let verified = verify(&verifier, &document)?;
    let resolution = resolver.resolve(&verified, &store);
    assert_eq!(
        issues(&resolution)?,
        json!([
            {"code": "plugin_program_missing", "programName": "com.example.missing-a", "exactVersion": "1.0.0", "pluginInstanceIds": ["missing-a", "missing-a-again"]},
            {"code": "plugin_program_missing", "programName": "com.example.missing-b", "exactVersion": "1.0.0", "pluginInstanceIds": ["missing-b"]},
            {"code": "flow_lua_runtime_binding_invalid", "flowId": "independent"}
        ])
    );
    assert!(!format!("{resolution:?}").contains("private runtime marker"));
    let (name, version) = identity(&available)?;
    assert_eq!(
        store.uninstall(&name, &version).map_err(io::Error::other)?,
        PluginUninstallOutcome::Uninstalled
    );
    Ok(())
}

#[test]
fn config_errors_do_not_skip_lua_bindings_when_a_dual_program_is_used_as_source() -> io::Result<()>
{
    let (_directory, mut store) = empty_store()?;
    let mut dual = program("com.example.dual", "source-and-sink");
    dual["configSchema"]["required"] = json!(["endpoint"]);
    dual["configSchema"]["properties"] = json!({"endpoint": {"type": "string"}});
    let sink = program("com.example.sink", "sink");
    install(&mut store, &dual)?;
    install(&mut store, &sink)?;
    let (verifier, resolver) = validators()?;

    for source in [
        "local main = function(event) end",
        "main = 42",
        "local b = registry:getBuilder('com.example.sink@1.0.0'); b:setUnknown('value'); function main(event) end",
        "while true do end",
    ] {
        let document = json!({
            "specVersion": "1", "id": "independent-binding-errors",
            "pluginInstances": {
                "device": instance("com.example.dual"),
                "sink": instance("com.example.sink")
            },
            "flows": {"telemetry": flow("device", &["sink"], source)}
        });
        let verified = verify(&verifier, &document)?;
        assert_eq!(
            issues(&resolver.resolve(&verified, &store))?,
            json!([
                {"code": "plugin_config_schema_mismatch", "pluginInstanceId": "device"},
                {"code": "flow_lua_runtime_binding_invalid", "flowId": "telemetry"}
            ]),
            "{source}"
        );
    }
    for program in [&dual, &sink] {
        let (name, version) = identity(program)?;
        assert_eq!(
            store.uninstall(&name, &version).map_err(io::Error::other)?,
            PluginUninstallOutcome::Uninstalled
        );
    }
    Ok(())
}

#[test]
fn missing_sink_interface_groups_flows_and_skips_only_dependent_lua() -> io::Result<()> {
    let (_directory, mut store) = empty_store()?;
    install(&mut store, &program("com.example.source", "source"))?;
    install(&mut store, &program("com.example.sink", "sink"))?;
    let (verifier, resolver) = validators()?;
    let document = json!({
        "specVersion": "1", "id": "interface-errors",
        "pluginInstances": {
            "first": instance("com.example.source"),
            "second": instance("com.example.source"),
            "sink": instance("com.example.sink")
        },
        "flows": {
            "a": flow("first", &["first", "sink"], "error('not initialized')"),
            "b": flow("second", &["first"], "error('not initialized')")
        }
    });
    let verified = verify(&verifier, &document)?;
    assert_eq!(
        issues(&resolver.resolve(&verified, &store))?,
        json!([
            {"code": "plugin_sink_interface_missing", "pluginInstanceId": "first", "flowIds": ["a", "b"]}
        ])
    );
    Ok(())
}

#[test]
fn each_flow_sees_only_its_own_deduplicated_sink_contracts() -> io::Result<()> {
    let (_directory, mut store) = empty_store()?;
    for program in [
        program("com.example.source", "source"),
        program("com.example.first", "sink"),
        program("com.example.second", "sink"),
        program("com.example.unused", "sink"),
    ] {
        install(&mut store, &program)?;
    }
    let (verifier, resolver) = validators()?;
    let mut document = json!({
        "specVersion": "1", "id": "flow-local-contracts",
        "pluginInstances": {
            "source-a": instance("com.example.source"),
            "source-b": instance("com.example.source"),
            "sink-a": instance("com.example.first"),
            "sink-a-replica": instance("com.example.first"),
            "sink-b": instance("com.example.second")
        },
        "flows": {
            "a": flow("source-a", &["sink-a", "sink-a-replica"], "local b = registry:getBuilder('com.example.first@1.0.0'); b:setValue('one'); assert(not pcall(function() registry:getBuilder('com.example.second@1.0.0') end)); assert(not pcall(function() registry:getBuilder('com.example.unused@1.0.0') end)); function main(event) emit(b:build()) end"),
            "b": flow("source-b", &["sink-a", "sink-b"], "registry:getBuilder('com.example.first@1.0.0'); registry:getBuilder('com.example.second@1.0.0'); function main(event) emit() end")
        }
    });
    let verified = verify(&verifier, &document)?;
    let RuntimeResolution::Ready(plan) = resolver.resolve(&verified, &store) else {
        return Err(io::Error::other("Valid shared Sink bindings were rejected"));
    };
    assert_eq!(plan.programs().runtimes().len(), 3);
    for (name, version, entry) in store.programs() {
        assert_eq!(version.as_str(), "1.0.0");
        assert_eq!(
            Arc::strong_count(entry),
            if name.as_str() == "com.example.unused" {
                1
            } else {
                2
            }
        );
    }
    drop(plan);

    document["flows"]["a"]["process"]["script"] =
        json!("registry:getBuilder('com.example.second@1.0.0'); function main(event) end");
    let verified = verify(&verifier, &document)?;
    assert_eq!(
        issues(&resolver.resolve(&verified, &store))?,
        json!([{"code": "flow_lua_runtime_binding_invalid", "flowId": "a"}])
    );
    Ok(())
}

#[test]
fn bidirectional_plans_retain_original_entries_until_the_last_plan_is_dropped() -> io::Result<()> {
    let (directory, mut store) = empty_store()?;
    let gateway = program("com.example.gateway", "source-and-sink");
    let unrelated = program("com.example.unrelated", "sink");
    install(&mut store, &gateway)?;
    install(&mut store, &unrelated)?;
    let (name, version) = identity(&gateway)?;
    let original_entry = store.lookup(&name, &version).map(Arc::as_ptr);
    let (verifier, resolver) = validators()?;
    let mut document = dual_document();
    document["pluginInstances"]["left"]["config"] = json!({"endpoint": "private left"});
    document["pluginInstances"]["right"]["config"] = json!({"endpoint": "private right"});
    let verified = verify(&verifier, &document)?;
    let RuntimeResolution::Ready(first) = resolver.resolve(&verified, &store) else {
        return Err(io::Error::other(
            "Valid bidirectional Document was rejected",
        ));
    };
    let RuntimeResolution::Ready(second) = resolver.resolve(&verified, &store) else {
        return Err(io::Error::other("Repeated resolution was rejected"));
    };
    assert_eq!(first.programs().runtimes().len(), 1);
    assert_eq!(first.programs().runtimes(), second.programs().runtimes());
    assert_eq!(
        store.lookup(&name, &version).map(Arc::strong_count),
        Some(3)
    );
    assert_eq!(serde_json::to_value(first.document().as_ref())?, document);
    assert!(!format!("{first:?}").contains("private left"));
    assert!(!format!("{first:?}").contains("private right"));
    assert!(matches!(
        install(&mut store, &gateway)?,
        PluginProgramInstallResult::Unchanged(_)
    ));
    assert_eq!(
        store.lookup(&name, &version).map(Arc::as_ptr),
        original_entry
    );
    assert!(matches!(
        store.uninstall(&name, &version),
        Err(PluginStoreError::ProgramInUse)
    ));
    let (unrelated_name, unrelated_version) = identity(&unrelated)?;
    assert_eq!(
        store
            .uninstall(&unrelated_name, &unrelated_version)
            .map_err(io::Error::other)?,
        PluginUninstallOutcome::Uninstalled
    );

    drop(first);
    assert!(matches!(
        store.uninstall(&name, &version),
        Err(PluginStoreError::ProgramInUse)
    ));
    drop(second);
    assert_eq!(
        store.uninstall(&name, &version).map_err(io::Error::other)?,
        PluginUninstallOutcome::Uninstalled
    );
    assert!(!directory.path().join("com.example.gateway/1.0.0").exists());
    Ok(())
}

#[test]
fn absent_exact_version_can_be_installed_and_retried_without_static_reverification()
-> io::Result<()> {
    let (_directory, mut store) = empty_store()?;
    let gateway = program("com.example.gateway", "source-and-sink");
    let mut other_version = gateway.clone();
    other_version["exactVersion"] = json!("2.0.0");
    install(&mut store, &other_version)?;
    let (verifier, resolver) = validators()?;
    let verified = verify(&verifier, &dual_document())?;
    assert_eq!(
        issues(&resolver.resolve(&verified, &store))?,
        json!([{"code": "plugin_program_missing", "programName": "com.example.gateway", "exactVersion": "1.0.0", "pluginInstanceIds": ["left", "right"]}])
    );
    install(&mut store, &gateway)?;
    let RuntimeResolution::Ready(plan) = resolver.resolve(&verified, &store) else {
        return Err(io::Error::other("Installed exact Program did not resolve"));
    };
    assert!(Arc::ptr_eq(plan.document(), &verified));
    assert_eq!(plan.programs().runtimes()[0].exact_version, "1.0.0");
    Ok(())
}

#[test]
fn resolution_uses_the_validated_memory_snapshot_without_reopening_package_files() -> io::Result<()>
{
    let (directory, mut store) = empty_store()?;
    let gateway = program("com.example.gateway", "source-and-sink");
    install(&mut store, &gateway)?;
    let (verifier, resolver) = validators()?;
    let verified = verify(&verifier, &dual_document())?;
    let RuntimeResolution::Ready(before) = resolver.resolve(&verified, &store) else {
        return Err(io::Error::other("Valid Program did not resolve"));
    };
    let (name, version) = identity(&gateway)?;
    let entry = store
        .lookup(&name, &version)
        .ok_or_else(|| io::Error::other("Installed Program is absent"))?;
    fs::rename(entry.directory(), directory.path().join("moved-package"))?;
    let RuntimeResolution::Ready(after) = resolver.resolve(&verified, &store) else {
        return Err(io::Error::other("Resolution reopened an installed package"));
    };
    assert_eq!(before.programs().runtimes(), after.programs().runtimes());
    Ok(())
}

#[test]
fn unified_program_roots_decode_source_and_build_sink_with_the_shared_vm() -> io::Result<()> {
    let (_directory, mut store) = empty_store()?;
    let gateway = program("com.example.gateway", "source-and-sink");
    install(&mut store, &gateway)?;
    let (name, version) = identity(&gateway)?;
    let entry = store
        .lookup(&name, &version)
        .ok_or_else(|| io::Error::other("Installed Program is absent"))?;
    let source = entry
        .source_projection()
        .ok_or_else(|| io::Error::other("Dual-interface fixture has no Source"))?
        .root_message();
    let sink = entry
        .sink_projection()
        .ok_or_else(|| io::Error::other("Dual-interface fixture has no Sink"))?
        .root_message();
    assert_eq!(source.name(), "SourceRecordPayload");
    assert_eq!(sink.name(), "SinkRecordPayload");
    let sink_contract_id = SinkContractId::from_parts(name, version);
    let mut input = DynamicMessage::new(source.clone());
    input.set_field_by_name("value", ProtobufValue::String(String::from("input value")));
    let mut vm = LuaVm::load(
        "local b = registry:getBuilder('com.example.gateway@1.0.0'); function main(event) b:setValue(event.payload.value); emit(b:build()) end",
        limits()?,
        std::num::NonZeroU64::new(262_144).ok_or_else(|| std::io::Error::other("test record limit must be non-zero"))?,
        source,
        HashMap::from([(sink_contract_id.clone(), sink.clone())]),
        None,
        || false,
    ).map_err(|error| io::Error::other(error.to_string()))?;
    drop(store);
    let (result, boundaries) = vm
        .call_source(7, input.encode_to_vec().into())
        .map_err(io::Error::other)?
        .into_parts();
    result.map_err(|error| io::Error::other(error.to_string()))?;
    assert_eq!(
        boundaries,
        [EmitBoundary::Payload {
            sink_contract_id,
            payload: input.encode_to_vec(),
        }]
    );
    let [EmitBoundary::Payload { payload, .. }] = boundaries.as_slice() else {
        return Err(io::Error::other("Expected exactly one Sink payload"));
    };
    let output = DynamicMessage::decode(sink, payload.as_slice()).map_err(io::Error::other)?;
    assert_eq!(
        output.get_field_by_name("value").as_deref(),
        Some(&ProtobufValue::String(String::from("input value")))
    );
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeVectors {
    format_version: u32,
    cases: Vec<RuntimeCase>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeCase {
    name: String,
    available_programs: Vec<Value>,
    document: Value,
    expected: Value,
}

fn empty_store() -> io::Result<(TempDir, PluginProgramStore)> {
    let directory = tempfile::tempdir()?;
    let store =
        PluginProgramStore::recover(directory.path().to_owned()).map_err(io::Error::other)?;
    Ok((directory, store))
}

fn install(
    store: &mut PluginProgramStore,
    program: &Value,
) -> io::Result<crate::runner::plugin::store::PluginProgramInstallResult> {
    store
        .install(Cursor::new(package(program)?))
        .map_err(io::Error::other)
}

fn package(program: &Value) -> io::Result<Vec<u8>> {
    let interface: PluginInterface = serde_json::from_value(program["interface"].clone())?;
    let manifest = json!({
        "programName": program["programName"],
        "exactVersion": program["exactVersion"],
        "interface": interface,
        "platforms": [crate::runner::plugin::platform::Platform::CURRENT], "displayName": "Example Plugin", "description": "Read and write example records.", "command": ["./bin/start"]
    });
    archive(vec![
        ("manifest.json", serde_json::to_vec(&manifest)?),
        (
            "config.schema.json",
            serde_json::to_vec(&program["configSchema"])?,
        ),
        (
            "payload.descriptor.pb",
            valid_program_descriptor(interface)?,
        ),
        ("bin/start", b"not executed by Runtime Integrity".to_vec()),
    ])
}

fn identity(program: &Value) -> io::Result<(ProgramName, ExactVersion)> {
    Ok((
        serde_json::from_value(program["programName"].clone())?,
        serde_json::from_value(program["exactVersion"].clone())?,
    ))
}

fn program(name: &str, interface: &str) -> Value {
    json!({
        "programName": name, "exactVersion": "1.0.0", "interface": interface,
        "configSchema": {"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object"}
    })
}

fn instance(name: &str) -> Value {
    json!({"programName": name, "exactVersion": "1.0.0", "config": {}})
}

fn flow(source: &str, sinks: &[&str], lua: &str) -> Value {
    json!({"parallelism": 1, "source": source, "process": {"script": lua}, "sinks": sinks})
}

fn dual_document() -> Value {
    let source = "local b = registry:getBuilder('com.example.gateway@1.0.0'); b:setValue('ready'); setTimeout(0); function main(event) emit(b:build()) end";
    json!({
        "specVersion": "1", "id": "bidirectional",
        "pluginInstances": {
            "left": instance("com.example.gateway"),
            "right": instance("com.example.gateway")
        },
        "flows": {
            "forward": flow("left", &["right"], source),
            "reverse": flow("right", &["left"], source)
        }
    })
}

fn limits() -> io::Result<ScriptVmLimits> {
    let memory = NonZeroUsize::new(4 * 1024 * 1024)
        .ok_or_else(|| io::Error::other("Test memory limit must be positive"))?;
    ScriptVmLimits::try_new(memory, Duration::from_millis(100)).map_err(io::Error::other)
}

fn validators() -> io::Result<(TenonDocumentVerifier, RuntimeResolver)> {
    let limits = limits()?;
    Ok((
        TenonDocumentVerifier::try_new(limits).map_err(io::Error::other)?,
        RuntimeResolver::new(limits, process_resources::available_cpu_count()?),
    ))
}

fn verify(
    verifier: &TenonDocumentVerifier,
    value: &Value,
) -> io::Result<Arc<VerifiedTenonDocument>> {
    let parsed =
        UnverifiedTenonDocument::parse(&serde_json::to_vec(value)?).map_err(io::Error::other)?;
    verifier
        .verify(parsed)
        .map(Arc::new)
        .map_err(|issues| io::Error::other(format!("Invalid static fixture: {issues:?}")))
}

fn issues(resolution: &RuntimeResolution) -> io::Result<Value> {
    match resolution {
        RuntimeResolution::Unready(issues) => {
            serde_json::to_value(issues).map_err(io::Error::other)
        }
        RuntimeResolution::Ready(_) => Err(io::Error::other("Expected runtime issues")),
    }
}

#[test]
fn recovered_foreign_material_stays_queryable_while_old_and_unrelated_plans_survive()
-> io::Result<()> {
    use crate::runner::plugin::package::stage_plugin_program_package;
    use crate::runner::plugin::package::tests::package_with_platforms;
    use std::os::unix::fs::PermissionsExt as _;
    let (directory, mut store) = empty_store()?;
    install(
        &mut store,
        &program("com.example.gateway", "source-and-sink"),
    )?;
    let (verifier, resolver) = validators()?;
    let original = verify(&verifier, &dual_document())?;
    let foreign_program = program("com.example.foreign", "source-and-sink");
    let platforms = json!([crate::runner::plugin::package::tests::foreign_platform()]);
    let bytes = package_with_platforms(&package(&foreign_program)?, &platforms)?;
    let staged = stage_plugin_program_package(Cursor::new(bytes), directory.path())
        .map_err(io::Error::other)?;
    let manifest_before = fs::read(staged.path().join("manifest.json"))?;
    let namespace = directory.path().join("com.example.foreign");
    fs::create_dir(&namespace)?;
    fs::set_permissions(&namespace, fs::Permissions::from_mode(0o700))?;
    fs::rename(staged.path(), namespace.join("1.0.0"))?;
    drop(store);
    let mut recovered =
        PluginProgramStore::recover(directory.path().to_owned()).map_err(io::Error::other)?;
    let RuntimeResolution::Ready(old_plan) = resolver.resolve(&original, &recovered) else {
        return Err(io::Error::other("Original revision did not resolve"));
    };

    let (name, version) = identity(&foreign_program)?;
    let entry = recovered
        .lookup(&name, &version)
        .ok_or_else(|| io::Error::other("Foreign material was deleted"))?;
    assert_eq!(serde_json::to_value(entry.platforms())?, platforms);
    assert_eq!(
        fs::read(entry.directory().join("manifest.json"))?,
        manifest_before
    );
    let mut changed = dual_document();
    changed["pluginInstances"]["left"]["programName"] = json!("com.example.foreign");
    changed["pluginInstances"]["right"]["programName"] = json!("com.example.foreign");
    let desired = verify(&verifier, &changed)?;
    assert_eq!(
        issues(&resolver.resolve(&desired, &recovered))?,
        json!([{
            "code": "plugin_platform_mismatch",
            "programName": "com.example.foreign",
            "exactVersion": "1.0.0",
            "platforms": platforms,
            "currentPlatform": crate::runner::plugin::platform::Platform::CURRENT,
            "pluginInstanceIds": ["left", "right"]
        }])
    );
    assert!(Arc::ptr_eq(old_plan.document(), &original));
    let mut independent = dual_document();
    independent["id"] = json!("independent");
    let independent = verify(&verifier, &independent)?;
    assert!(matches!(
        resolver.resolve(&independent, &recovered),
        RuntimeResolution::Ready(_)
    ));
    assert_eq!(
        recovered
            .uninstall(&name, &version)
            .map_err(io::Error::other)?,
        PluginUninstallOutcome::Uninstalled
    );
    Ok(())
}
