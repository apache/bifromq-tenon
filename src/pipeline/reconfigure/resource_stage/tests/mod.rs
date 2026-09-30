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

use super::directories::OwnedDirectory;
use super::queue_layout::flow_egress_bindings;
use super::*;
use crate::contracts::core::{PipelineRevisionPlan, PluginInterface as ProtocolPluginInterface};
use crate::contracts::sink::EgressRecord;
use crate::contracts::source::{IngressCompletion, IngressCompletionStatus, IngressRecord};
use crate::identifiers::{PluginInstanceId, SinkContractId};
use crate::lua::LuaVmErrorKind;
use crate::pipeline::channel::FlowChannelError;
use crate::pipeline::diagnostics::test_support::publisher as test_publisher;
use crate::pipeline::reconfigure::plan::ReconfigurePlan;
use crate::pipeline::reconfigure::plan::tests::{flow, instance, model, program};
use crate::pipeline::reconfigure::resource_stage::directories::CANDIDATE_DIRECTORY_NAME;
use crate::pipeline::reconfigure::runtime_files::{
    FLOWS_DIRECTORY_NAME, INSTANCES_DIRECTORY_NAME, LOOPS_BELL_FILE_NAME, SINK_DIRECTORY_NAME,
    SOURCE_DIRECTORY_NAME, egress_queue_path, flow_channel_bell_path, instance_working_directory,
    loops_bell_path,
};
use crate::pipeline::reconfigure::test_support::{
    AVAILABLE_CPU_COUNT, environment as fixture_environment,
};
use crate::pipeline::runtime::{
    PipelineRuntimeError, test_support as prepared_runtime_test_support,
};
use prost::Message as _;
use serde_json::{Value, json};
use std::error::Error;
use std::io;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};
use tenon_ipc::queue::contract_test_support::{open_reader, open_writer};
use tenon_ipc::queue::{QueueReader, ReadOutcome, WriteOutcome};

type TestResult = Result<(), Box<dyn Error>>;

#[cfg(not(feature = "loom-model"))]
mod cancellation;

const GATEWAY_A_DIRECTORY_NAME: &str = "7_Cxp1nP78-ECSY9b6qGd2hQ9GbAbIx6Ibg2yqctDLU";
const GATEWAY_B_DIRECTORY_NAME: &str = "a1CqcX0FKJZxhUz9FX9MkYiN64Ry6WbxnkENxIVrekw";
const ARCHIVE_DIRECTORY_NAME: &str = "DrPja_sk3Nm7HRvs4VMSFrWVOaj94X7oAiSvBlPJKqM";
const A_TO_B_DIRECTORY_NAME: &str = "pBOYalD2SUQO-8mnLoQZUkVPQDgjG78qK3THEtett8c";
const SOURCE_ONLY_INSTANCE_ID: &str = "source/~/primary";
const SOURCE_ONLY_DIRECTORY_NAME: &str = "T2J8X6uh8tpr_Oba5A4TZWtp-apYfMn0iuXYPREvv7c";
const SINK_ONLY_INSTANCE_ID: &str = "sink/~/archive";
const SINK_ONLY_DIRECTORY_NAME: &str = "f-4TpPKpd2SGoAEcan9TjgN5V4prgWx191JOmGKAD4c";

#[test]
fn compile_accepts_an_equivalent_applied_revision_without_creating_resources() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let current = model(candidate_revision("current")?)?;
    let target = model(candidate_revision("target")?)?;

    ReconfigurePlan::derive(Some(&current), target, AVAILABLE_CPU_COUNT)?.compile()?;
    assert!(!root.exists());
    Ok(())
}

#[test]
fn stage_replacements_use_separate_files_and_leave_active_interfaces_unchanged() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let environment = environment(&root)?;
    let StagedResourceChanges {
        mut runtime,
        changes,
        working_directory,
        channel_routes: _,
    } = initial_plan("current")?.stage(
        &environment,
        test_publisher(),
        Arc::new(StartupControl::new()),
        None,
        None,
    )?;
    // Drop workers before their directory owner, including on an assertion failure.
    tokio::runtime::Builder::new_current_thread()
        .build()?
        .block_on(runtime.bind())?;
    let current = (runtime.activate(), working_directory, changes);
    let gateway = instance_directory(&root.join(INSTANCES_DIRECTORY_NAME), "gateway-a")?;
    let source = gateway.join(SOURCE_DIRECTORY_NAME);
    let old_names = directory_names(&source)?;
    let old_submission = std::fs::metadata(source.join("submission-0.queue"))?.ino();
    let old_egress = std::fs::metadata(egress_queue_path(&gateway, "b-to-a", 0))?.ino();
    let mut target = candidate_revision("target")?;
    crate::pipeline::reconfigure::plan::tests::update_document(&mut target, |document| {
        document["flows"]["a-to-b"]["parallelism"] = json!(0.75);
    })?;
    let staged = ReconfigurePlan::derive(
        Some(&current.2.target),
        model(target)?,
        environment.available_cpu_count(),
    )?
    .compile()?
    .stage(
        &environment,
        test_publisher(),
        Arc::new(StartupControl::new()),
        Some(current.0.started_at()),
        None,
    )?;

    assert_eq!(
        prepared_runtime_test_support::data_plane_counts(&staged.runtime),
        (1, 3)
    );
    assert_eq!(directory_names(&source)?, old_names);
    assert_eq!(
        std::fs::metadata(source.join("submission-0.queue"))?.ino(),
        old_submission
    );
    assert_eq!(
        std::fs::metadata(egress_queue_path(&gateway, "b-to-a", 0))?.ino(),
        old_egress
    );
    assert_eq!(
        directory_names(&root)?,
        [
            String::from(CANDIDATE_DIRECTORY_NAME),
            String::from(FLOWS_DIRECTORY_NAME),
            String::from(INSTANCES_DIRECTORY_NAME),
        ]
    );
    assert_eq!(
        directory_tree(&root.join(CANDIDATE_DIRECTORY_NAME))?,
        replacement_candidate_tree()
    );
    drop(staged);
    assert_eq!(
        directory_names(&root)?,
        [
            String::from(FLOWS_DIRECTORY_NAME),
            String::from(INSTANCES_DIRECTORY_NAME),
        ]
    );
    assert_eq!(directory_names(&source)?, old_names);
    drop(current);
    assert!(!root.exists());
    Ok(())
}

#[test]
fn stage_builds_exact_per_channel_egress_layout() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let environment = environment(&root)?;
    let staged = initial_plan("layout")?.stage(
        &environment,
        test_publisher(),
        Arc::new(StartupControl::new()),
        None,
        None,
    )?;

    assert_eq!(staged.changes.target.document_etag(), "layout");
    let (flow_count, channel_count) =
        prepared_runtime_test_support::data_plane_counts(&staged.runtime);
    assert_eq!(flow_count, 2);
    assert_eq!(channel_count, 3);
    assert_eq!(directory_mode(&root)?, 0o700);
    assert_eq!(
        directory_names(&root)?,
        [
            String::from(FLOWS_DIRECTORY_NAME),
            String::from(INSTANCES_DIRECTORY_NAME),
        ]
    );
    let instances = root.join(INSTANCES_DIRECTORY_NAME);
    assert_eq!(
        directory_names(&instances)?,
        [
            String::from(GATEWAY_A_DIRECTORY_NAME),
            String::from(ARCHIVE_DIRECTORY_NAME),
            String::from(GATEWAY_B_DIRECTORY_NAME),
        ]
    );

    assert_instance_layout(
        &instances,
        "gateway-a",
        Some(&[
            "completion-0.queue",
            "completion-1.queue",
            LOOPS_BELL_FILE_NAME,
            "submission-0.queue",
            "submission-1.queue",
        ]),
    )?;
    assert_instance_layout(
        &instances,
        "gateway-b",
        Some(&[
            "completion-0.queue",
            LOOPS_BELL_FILE_NAME,
            "submission-0.queue",
        ]),
    )?;
    assert_instance_layout(&instances, "archive", None)?;

    drop(staged);
    assert!(!root.exists());
    Ok(())
}

#[test]
fn stage_gives_source_only_and_sink_only_instances_only_their_declared_interface() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let environment = environment(&root)?;
    let staged = ReconfigurePlan::derive(
        None,
        model(single_interface_revision("single-interfaces")?)?,
        environment.available_cpu_count(),
    )?
    .compile()?
    .stage(
        &environment,
        test_publisher(),
        Arc::new(StartupControl::new()),
        None,
        None,
    )?;

    let (flow_count, channel_count) =
        prepared_runtime_test_support::data_plane_counts(&staged.runtime);
    assert_eq!(flow_count, 1);
    assert_eq!(channel_count, 1);

    let instances = root.join(INSTANCES_DIRECTORY_NAME);
    assert_eq!(
        directory_names(&instances)?,
        [
            String::from(SOURCE_ONLY_DIRECTORY_NAME),
            String::from(SINK_ONLY_DIRECTORY_NAME),
        ]
    );
    let source_instance = instances.join(SOURCE_ONLY_DIRECTORY_NAME);
    assert_eq!(directory_mode(&source_instance)?, 0o700);
    assert_eq!(
        directory_names(&source_instance)?,
        [String::from(SOURCE_DIRECTORY_NAME)]
    );
    assert_eq!(
        directory_names(&source_instance.join(SOURCE_DIRECTORY_NAME))?,
        [
            String::from("completion-0.queue"),
            String::from("loops.bells"),
            String::from("submission-0.queue"),
        ]
    );
    assert!(!source_instance.join(SINK_DIRECTORY_NAME).exists());

    let sink_instance = instances.join(SINK_ONLY_DIRECTORY_NAME);
    assert_eq!(directory_mode(&sink_instance)?, 0o700);
    assert_eq!(
        directory_names(&sink_instance)?,
        [String::from(SINK_DIRECTORY_NAME)]
    );
    assert_eq!(
        directory_names(&sink_instance.join(SINK_DIRECTORY_NAME))?,
        [
            flow_directory_name("source-to-sink"),
            String::from(LOOPS_BELL_FILE_NAME),
        ]
    );
    assert_eq!(
        directory_names(
            egress_queue_path(&sink_instance, "source-to-sink", 0)
                .parent()
                .ok_or("Queue parent is missing")?
        )?,
        [String::from("egress-0.queue")]
    );
    assert!(!sink_instance.join(SOURCE_DIRECTORY_NAME).exists());

    drop(staged);
    assert!(!root.exists());
    Ok(())
}

#[test]
fn stage_neither_executes_program_commands_nor_releases_paused_work() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let marker = parent.path().join("program-ran");
    let environment = environment(&root)?;
    let mut revision = candidate_revision("paused")?;
    set_timer_lua(&mut revision)?;
    for runtime in &mut revision.plugin_programs {
        runtime.program_directory = parent.path().to_string_lossy().into_owned();
        runtime.command = vec![
            String::from("/bin/sh"),
            String::from("-c"),
            format!("touch {}", marker.display()),
        ];
    }
    let staged =
        ReconfigurePlan::derive(None, model(revision)?, environment.available_cpu_count())?
            .compile()?
            .stage(
                &environment,
                test_publisher(),
                Arc::new(StartupControl::new()),
                None,
                None,
            )?;
    let instances = root.join(INSTANCES_DIRECTORY_NAME);
    let source_directory = instance_directory(&instances, "gateway-a")?.join(SOURCE_DIRECTORY_NAME);
    let sink_directory = instance_directory(&instances, "archive")?;
    let source_bells = loops_bell_path(&source_directory);
    let channel_bells = flow_channel_bell_path(&root, "a-to-b");
    let mut submission = open_writer(
        &source_directory.join("submission-0.queue"),
        &source_bells,
        0,
        &channel_bells,
    )?;
    let mut completion = open_reader(
        &source_directory.join("completion-0.queue"),
        &source_bells,
        1,
        &channel_bells,
    )?;
    let mut egress = open_reader(
        &egress_queue_path(&sink_directory, "a-to-b", 0),
        &sink_bells(&sink_directory),
        0,
        &channel_bells,
    )?;
    let receipt = match {
        let record = IngressRecord {
            record_id: 7,
            payload: Vec::new().into(),
        };
        submission.try_write_with(
            record.encoded_len(),
            |destination| record.encode(destination).map_err(std::io::Error::other),
            || {},
        )
    }? {
        WriteOutcome::Committed(receipt) => receipt,
        WriteOutcome::Full => {
            return Err(io::Error::other("Fresh Submission Queue was full").into());
        }
    };

    assert!(prepared_runtime_test_support::is_pending(&staged.runtime));
    assert!(!marker.exists());
    assert!(!submission.is_released(&receipt)?);
    assert!(matches!(completion.try_read()?, ReadOutcome::Empty));
    assert!(matches!(egress.try_read()?, ReadOutcome::Empty));

    drop(submission);
    drop(completion);
    drop(egress);
    drop(staged);
    assert!(!root.exists());
    Ok(())
}

#[test]
fn shared_sink_contract_folds_one_payload_root_and_groups_both_instances() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let environment = environment(&root)?;
    let target = model(shared_contract_revision("shared-contract")?)?;
    let flow_id = FlowId::try_from(String::from("fan-out"))?;
    let gateway = SinkContractId::try_from("com.example.gateway@1.0.0")?;
    let archive = SinkContractId::try_from("com.example.archive@1.0.0")?;

    // Three Sink instances, two Sink Contracts: the gateway pair shares one.
    assert_eq!(target.document().flows()[&flow_id].sinks().len(), 3);
    let bindings = flow_egress_bindings(&target, &flow_id);
    assert_eq!(bindings.len(), 2);
    // The fixture declares the gateway pair in descending order, so ascending
    // groups can only come from the binding's own sort. Downstream re-keys these
    // into a BTreeMap, so this freezes the function's own return contract.
    assert_eq!(
        bindings[&gateway],
        [
            PluginInstanceId::try_from(String::from("gateway-b"))?,
            PluginInstanceId::try_from(String::from("gateway-c"))?,
        ]
    );
    assert_eq!(
        bindings[&archive],
        [PluginInstanceId::try_from(String::from("archive"))?]
    );

    // The Channel specification keys Payload roots by Contract, not by instance,
    // so its frozen registry matches the grouped routes exactly.
    let spec = channel_spec(&target, &flow_id, environment.lua_limits());
    assert!(spec.matches_routes(&bindings));

    let staged = ReconfigurePlan::derive(None, target, environment.available_cpu_count())?
        .compile()?
        .stage(
            &environment,
            test_publisher(),
            Arc::new(StartupControl::new()),
            None,
            None,
        )?;
    let instances = root.join(INSTANCES_DIRECTORY_NAME);
    let gateway_a = instance_directory(&instances, "gateway-a")?;
    let gateway_b = instance_directory(&instances, "gateway-b")?;
    let gateway_c = instance_directory(&instances, "gateway-c")?;
    let archive = instance_directory(&instances, "archive")?;
    let source_directory = gateway_a.join(SOURCE_DIRECTORY_NAME);
    let channel_bells = flow_channel_bell_path(&root, "fan-out");
    let mut submission = open_writer(
        &source_directory.join("submission-0.queue"),
        &loops_bell_path(&source_directory),
        0,
        &channel_bells,
    )?;
    let mut gateway_b_egress = open_reader(
        &egress_queue_path(&gateway_b, "fan-out", 0),
        &sink_bells(&gateway_b),
        0,
        &channel_bells,
    )?;
    let mut gateway_c_egress = open_reader(
        &egress_queue_path(&gateway_c, "fan-out", 0),
        &sink_bells(&gateway_c),
        0,
        &channel_bells,
    )?;
    let mut archive_egress = open_reader(
        &egress_queue_path(&archive, "fan-out", 0),
        &sink_bells(&archive),
        0,
        &channel_bells,
    )?;
    let StagedResourceChanges {
        mut runtime,
        changes,
        working_directory,
        channel_routes: _,
    } = staged;
    let target = changes.target;
    tokio::runtime::Builder::new_current_thread()
        .build()?
        .block_on(runtime.bind())?;
    let runtime = runtime.activate();

    assert!(matches!(
        {
            let record = IngressRecord {
                record_id: 31,
                payload: Vec::new().into(),
            };
            submission.try_write_with(
                record.encoded_len(),
                |destination| record.encode(destination).map_err(std::io::Error::other),
                || {},
            )
        }?,
        WriteOutcome::Committed(_)
    ));
    for gateway_egress in [&mut gateway_b_egress, &mut gateway_c_egress] {
        let output =
            EgressRecord::decode(wait_queue_record(gateway_egress, "gateway Egress")?.as_slice())?;
        assert_eq!(output.payload.as_slice(), b"\x0a\x07gateway");
        gateway_egress.release(1)?;
    }
    let archive_output =
        EgressRecord::decode(wait_queue_record(&mut archive_egress, "archive Egress")?.as_slice())?;
    assert_eq!(archive_output.payload.as_slice(), b"\x0a\x07archive");
    archive_egress.release(1)?;

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(runtime.stop_and_join())?;
    drop(submission);
    drop(gateway_b_egress);
    drop(gateway_c_egress);
    drop(archive_egress);
    drop(target);
    working_directory.remove()?;
    assert!(!root.exists());
    Ok(())
}

#[test]
fn staged_routes_send_only_to_each_flows_declared_instances() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let environment = environment(&root)?;
    let mut revision = candidate_revision("routes")?;
    set_flow_lua(
        &mut revision,
        "a-to-b",
        r#"
        local gateway = registry:getBuilder("com.example.gateway@1.0.0")
        local archive = registry:getBuilder("com.example.archive@1.0.0")
        function main(event)
            gateway:setValue("a-to-b")
            archive:setValue("a-to-b")
            emit(gateway:build())
            emit(archive:build())
        end
        "#,
    )?;
    set_flow_lua(
        &mut revision,
        "b-to-a",
        r#"
        local gateway = registry:getBuilder("com.example.gateway@1.0.0")
        local archive = registry:getBuilder("com.example.archive@1.0.0")
        function main(event)
            gateway:setValue("b-to-a")
            archive:setValue("b-to-a")
            emit(gateway:build())
            emit(archive:build())
        end
        "#,
    )?;
    let staged =
        ReconfigurePlan::derive(None, model(revision)?, environment.available_cpu_count())?
            .compile()?
            .stage(
                &environment,
                test_publisher(),
                Arc::new(StartupControl::new()),
                None,
                None,
            )?;
    let instances = root.join(INSTANCES_DIRECTORY_NAME);
    let gateway_a = instance_directory(&instances, "gateway-a")?;
    let gateway_b = instance_directory(&instances, "gateway-b")?;
    let archive = instance_directory(&instances, "archive")?;
    let gateway_a_source = gateway_a.join(SOURCE_DIRECTORY_NAME);
    let gateway_b_source = gateway_b.join(SOURCE_DIRECTORY_NAME);
    let a_to_b_bells = flow_channel_bell_path(&root, "a-to-b");
    let b_to_a_bells = flow_channel_bell_path(&root, "b-to-a");
    let mut gateway_a_submission = open_writer(
        &gateway_a_source.join("submission-0.queue"),
        &loops_bell_path(&gateway_a_source),
        0,
        &a_to_b_bells,
    )?;
    let mut gateway_a_completion = open_reader(
        &gateway_a_source.join("completion-0.queue"),
        &loops_bell_path(&gateway_a_source),
        1,
        &a_to_b_bells,
    )?;
    let mut gateway_b_submission = open_writer(
        &gateway_b_source.join("submission-0.queue"),
        &loops_bell_path(&gateway_b_source),
        0,
        &b_to_a_bells,
    )?;
    let mut gateway_b_completion = open_reader(
        &gateway_b_source.join("completion-0.queue"),
        &loops_bell_path(&gateway_b_source),
        1,
        &b_to_a_bells,
    )?;
    let mut gateway_a_egress = open_reader(
        &egress_queue_path(&gateway_a, "b-to-a", 0),
        &sink_bells(&gateway_a),
        0,
        &b_to_a_bells,
    )?;
    let mut gateway_b_egress = open_reader(
        &egress_queue_path(&gateway_b, "a-to-b", 0),
        &sink_bells(&gateway_b),
        0,
        &a_to_b_bells,
    )?;
    let mut archive_egress = open_reader(
        &egress_queue_path(&archive, "a-to-b", 0),
        &sink_bells(&archive),
        0,
        &a_to_b_bells,
    )?;
    let mut second_archive_egress = open_reader(
        &egress_queue_path(&archive, "b-to-a", 0),
        &sink_bells(&archive),
        0,
        &b_to_a_bells,
    )?;
    let StagedResourceChanges {
        mut runtime,
        changes,
        working_directory,
        channel_routes: _,
    } = staged;
    let target = changes.target;
    tokio::runtime::Builder::new_current_thread()
        .build()?
        .block_on(runtime.bind())?;
    let runtime = runtime.activate();

    assert!(matches!(
        {
            let record = IngressRecord {
                record_id: 17,
                payload: Vec::new().into(),
            };
            gateway_a_submission.try_write_with(
                record.encoded_len(),
                |destination| record.encode(destination).map_err(std::io::Error::other),
                || {},
            )
        }?,
        WriteOutcome::Committed(_)
    ));
    assert!(matches!(
        {
            let record = IngressRecord {
                record_id: 23,
                payload: Vec::new().into(),
            };
            gateway_b_submission.try_write_with(
                record.encoded_len(),
                |destination| record.encode(destination).map_err(std::io::Error::other),
                || {},
            )
        }?,
        WriteOutcome::Committed(_)
    ));
    let gateway_b_output = EgressRecord::decode(
        wait_queue_record(&mut gateway_b_egress, "gateway-b Egress")?.as_slice(),
    )?;
    let gateway_a_output = EgressRecord::decode(
        wait_queue_record(&mut gateway_a_egress, "gateway-a Egress")?.as_slice(),
    )?;
    gateway_b_egress.release(1)?;
    gateway_a_egress.release(1)?;
    let first_archive_output =
        EgressRecord::decode(wait_queue_record(&mut archive_egress, "archive Egress")?.as_slice())?;
    archive_egress.release(1)?;
    let second_archive_output = EgressRecord::decode(
        wait_queue_record(&mut second_archive_egress, "archive Egress")?.as_slice(),
    )?;
    second_archive_egress.release(1)?;
    let a_to_b_payload = b"\x0a\x06a-to-b";
    let b_to_a_payload = b"\x0a\x06b-to-a";
    assert_eq!(gateway_b_output.payload.as_slice(), a_to_b_payload);
    assert_eq!(gateway_a_output.payload.as_slice(), b_to_a_payload);
    let mut archive_payloads = vec![
        first_archive_output.payload.to_vec(),
        second_archive_output.payload.to_vec(),
    ];
    archive_payloads.sort_unstable();
    assert_eq!(
        archive_payloads,
        [a_to_b_payload.to_vec(), b_to_a_payload.to_vec()]
    );

    let gateway_a_completed = IngressCompletion::decode(
        wait_queue_record(&mut gateway_a_completion, "gateway-a Source completion")?.as_slice(),
    )?;
    let gateway_b_completed = IngressCompletion::decode(
        wait_queue_record(&mut gateway_b_completion, "gateway-b Source completion")?.as_slice(),
    )?;
    assert_eq!(gateway_a_completed.record_id, 17);
    assert_eq!(gateway_a_completed.status(), IngressCompletionStatus::Ok);
    assert_eq!(gateway_b_completed.record_id, 23);
    assert_eq!(gateway_b_completed.status(), IngressCompletionStatus::Ok);
    gateway_a_completion.release(1)?;
    gateway_b_completion.release(1)?;
    assert!(matches!(gateway_a_egress.try_read()?, ReadOutcome::Empty));
    assert!(matches!(gateway_b_egress.try_read()?, ReadOutcome::Empty));
    assert!(matches!(archive_egress.try_read()?, ReadOutcome::Empty));

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(runtime.stop_and_join())?;
    drop(gateway_a_submission);
    drop(gateway_a_completion);
    drop(gateway_b_submission);
    drop(gateway_b_completion);
    drop(gateway_a_egress);
    drop(gateway_b_egress);
    drop(archive_egress);
    drop(second_archive_egress);
    drop(target);
    working_directory.remove()?;
    assert!(!root.exists());
    Ok(())
}

#[test]
fn abandoning_a_stage_removes_every_resource_and_allows_the_same_target_to_retry() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let environment = environment(&root)?;

    drop(initial_plan("same-target")?.stage(
        &environment,
        test_publisher(),
        Arc::new(StartupControl::new()),
        None,
        None,
    )?);
    assert!(!root.exists());
    drop(initial_plan("same-target")?.stage(
        &environment,
        test_publisher(),
        Arc::new(StartupControl::new()),
        None,
        None,
    )?);
    assert!(!root.exists());
    Ok(())
}

#[test]
fn root_permission_failure_removes_the_new_directory_and_allows_retry() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");

    let error = OwnedDirectory::create_with_permissions(&root, |path| {
        Err(PipelineReconfigureError::DirectoryPermission {
            path: path.to_owned(),
            source: io::Error::other("Injected permission failure"),
        })
    })
    .err()
    .ok_or_else(|| io::Error::other("Injected permission failure unexpectedly succeeded"))?;

    assert!(matches!(
        error,
        PipelineReconfigureError::DirectoryPermission { .. }
    ));
    assert!(!root.exists());
    drop(OwnedDirectory::create(&root)?);
    assert!(!root.exists());
    Ok(())
}

#[test]
fn later_flow_startup_failure_removes_every_partial_resource_and_allows_retry() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("pipeline");
    let environment = environment(&root)?;
    let mut revision = candidate_revision("runtime-failure")?;
    set_flow_lua(&mut revision, "b-to-a", "error(\"load failed\")")?;

    let error = ReconfigurePlan::derive(None, model(revision)?, environment.available_cpu_count())?
        .compile()?
        .stage(
            &environment,
            test_publisher(),
            Arc::new(StartupControl::new()),
            None,
            None,
        )
        .err()
        .ok_or_else(|| io::Error::other("Invalid runtime unexpectedly staged"))?;

    assert!(matches!(
        error,
        PipelineReconfigureError::RuntimeStart(PipelineRuntimeError::FlowChannelOpen {
            flow_id,
            channel_index: 0,
            source: FlowChannelError::LuaVmLoad {
                kind: LuaVmErrorKind::TopLevelFailed,
            },
        }) if flow_id.as_str() == "b-to-a"
    ));
    assert!(!root.exists());
    drop(initial_plan("runtime-failure")?.stage(
        &environment,
        test_publisher(),
        Arc::new(StartupControl::new()),
        None,
        None,
    )?);
    assert!(!root.exists());
    Ok(())
}

fn initial_plan(etag: &str) -> Result<CompiledResourceChanges, Box<dyn Error>> {
    Ok(
        ReconfigurePlan::derive(None, model(candidate_revision(etag)?)?, AVAILABLE_CPU_COUNT)?
            .compile()?,
    )
}

fn candidate_revision(etag: &str) -> io::Result<PipelineRevisionPlan> {
    Ok(PipelineRevisionPlan {
        document_etag: etag.into(),
        tenon_document_json: serde_json::to_string(&json!({
            "specVersion": "1",
            "id": "initial-stage-test",
            "pluginInstances": {
                "gateway-a": instance("com.example.gateway", "gateway-a"),
                "gateway-b": instance("com.example.gateway", "gateway-b"),
                "archive": instance("com.example.archive", "archive")
            },
            "flows": {
                "a-to-b": flow("gateway-a", 2, &["gateway-b", "archive"]),
                "b-to-a": flow("gateway-b", 1, &["gateway-a", "archive"])
            }
        }))?,
        plugin_programs: vec![
            program(
                "com.example.gateway",
                "1.0.0",
                ProtocolPluginInterface::SourceAndSink,
            )?,
            program(
                "com.example.archive",
                "1.0.0",
                ProtocolPluginInterface::Sink,
            )?,
        ],
    })
}

/// Two gateway instances share one Sink Contract while archive stands alone, so a
/// single Flow fans out to three instances across two Contracts.
fn shared_contract_revision(etag: &str) -> io::Result<PipelineRevisionPlan> {
    let mut document = json!({
        "specVersion": "1",
        "id": "shared-contract-stage-test",
        "pluginInstances": {
            "gateway-a": instance("com.example.gateway", "gateway-a"),
            "gateway-b": instance("com.example.gateway", "gateway-b"),
            "gateway-c": instance("com.example.gateway", "gateway-c"),
            "archive": instance("com.example.archive", "archive")
        },
        "flows": {
            "fan-out": flow("gateway-a", 1, &["gateway-c", "gateway-b", "archive"])
        }
    });
    document["flows"]["fan-out"]["process"]["script"] = json!(
        "local gateway = registry:getBuilder('com.example.gateway@1.0.0'); local archive = registry:getBuilder('com.example.archive@1.0.0'); function main(event) gateway:setValue('gateway'); archive:setValue('archive'); emit(gateway:build()); emit(archive:build()) end"
    );
    Ok(PipelineRevisionPlan {
        document_etag: etag.into(),
        tenon_document_json: serde_json::to_string(&document)?,
        plugin_programs: vec![
            program(
                "com.example.gateway",
                "1.0.0",
                ProtocolPluginInterface::SourceAndSink,
            )?,
            program(
                "com.example.archive",
                "1.0.0",
                ProtocolPluginInterface::Sink,
            )?,
        ],
    })
}

fn single_interface_revision(etag: &str) -> io::Result<PipelineRevisionPlan> {
    Ok(PipelineRevisionPlan {
        document_etag: etag.into(),
        tenon_document_json: serde_json::to_string(&json!({
            "specVersion": "1",
            "id": "single-interface-stage-test",
            "pluginInstances": {
                SOURCE_ONLY_INSTANCE_ID: instance("com.example.source", SOURCE_ONLY_INSTANCE_ID),
                SINK_ONLY_INSTANCE_ID: instance("com.example.archive", SINK_ONLY_INSTANCE_ID)
            },
            "flows": {
                "source-to-sink": flow(SOURCE_ONLY_INSTANCE_ID, 1, &[SINK_ONLY_INSTANCE_ID])
            }
        }))?,
        plugin_programs: vec![
            program(
                "com.example.source",
                "1.0.0",
                ProtocolPluginInterface::Source,
            )?,
            program(
                "com.example.archive",
                "1.0.0",
                ProtocolPluginInterface::Sink,
            )?,
        ],
    })
}

fn set_timer_lua(revision: &mut PipelineRevisionPlan) -> io::Result<()> {
    let mut document: Value = serde_json::from_str(&revision.tenon_document_json)?;
    for flow in document["flows"]
        .as_object_mut()
        .ok_or_else(|| io::Error::other("Fixture Flows are missing"))?
        .values_mut()
    {
        flow["process"]["script"] = json!(
            "local builder = registry:getBuilder(\"com.example.archive@1.0.0\"); setTimeout(0); function main(event) emit(builder:build()) end"
        );
    }
    revision.tenon_document_json = serde_json::to_string(&document)?;
    Ok(())
}

fn set_flow_lua(
    revision: &mut PipelineRevisionPlan,
    flow_id: &str,
    source: &str,
) -> io::Result<()> {
    let mut document: Value = serde_json::from_str(&revision.tenon_document_json)?;
    let flow = document["flows"]
        .as_object_mut()
        .and_then(|flows| flows.get_mut(flow_id))
        .ok_or_else(|| io::Error::other("Fixture Flow is missing"))?;
    flow["process"]["script"] = json!(source);
    revision.tenon_document_json = serde_json::to_string(&document)?;
    Ok(())
}

fn environment(root: &Path) -> Result<PipelineEnvironment, Box<dyn Error>> {
    let environment = fixture_environment(root)?;
    Ok(environment)
}

fn assert_instance_layout(
    instances: &Path,
    instance_id: &str,
    source_files: Option<&[&str]>,
) -> TestResult {
    let directory = instance_directory(instances, instance_id)?;
    assert_eq!(directory_mode(&directory)?, 0o700);
    let source = directory.join(SOURCE_DIRECTORY_NAME);
    match source_files {
        Some(source_files) => {
            assert_eq!(directory_mode(&source)?, 0o700);
            assert_eq!(
                directory_names(&source)?,
                source_files
                    .iter()
                    .map(|name| String::from(*name))
                    .collect::<Vec<_>>()
            );
        }
        None => assert!(!source.exists()),
    }
    let sink = directory.join(SINK_DIRECTORY_NAME);
    assert_eq!(directory_mode(&sink)?, 0o700);
    let expected: &[(&str, usize)] = match instance_id {
        "gateway-a" => &[("b-to-a", 1)],
        "gateway-b" => &[("a-to-b", 2)],
        "archive" => &[("a-to-b", 2), ("b-to-a", 1)],
        _ => return Err("Unknown fixture Instance".into()),
    };
    let mut names = expected
        .iter()
        .map(|(flow, _)| flow_directory_name(flow))
        .collect::<Vec<_>>();
    names.push(String::from(LOOPS_BELL_FILE_NAME));
    names.sort_unstable();
    assert_eq!(directory_names(&sink)?, names);
    for (flow, channels) in expected {
        let flow_directory = sink.join(flow_directory_name(flow));
        assert_eq!(directory_mode(&flow_directory)?, 0o700);
        assert_eq!(
            directory_names(&flow_directory)?,
            (0..*channels)
                .map(|channel| format!("egress-{channel}.queue"))
                .collect::<Vec<_>>()
        );
    }
    // One Egress loop reads every one of those Queues, so the region holds the
    // single doorbell it parks on however many Queues this Instance has.
    let region = super::queue_layout::open_bell_region(&sink.join(LOOPS_BELL_FILE_NAME))?;
    assert_eq!(region.slot_count().get(), 1);
    Ok(())
}

fn instance_directory(instances: &Path, value: &str) -> Result<PathBuf, Box<dyn Error>> {
    let id = PluginInstanceId::try_from(value.to_owned())?;
    Ok(instance_working_directory(instances, &id))
}

/// Returns the Bell Region that holds one Sink Instance's own loop doorbell.
///
/// The Instance runs one Egress loop over every Egress input, so the region
/// holds that loop's single slot and lives beside the Queue files it reads.
fn sink_bells(instance_directory: &Path) -> PathBuf {
    loops_bell_path(&instance_directory.join(SINK_DIRECTORY_NAME))
}

/// Describes the candidate tree staged when `a-to-b` rises from two Channels to
/// three: its Source gets a whole new three-Channel Queue set, each of its Sinks
/// gains only the added Channel 2 egress file, and no other instance, interface or
/// Flow appears. Both sides carry the own-loop region their loops park on.
/// `b-to-a` changes nothing, so `gateway-a` has no candidate `sink`
/// and `gateway-b` no candidate `source`.
fn replacement_candidate_tree() -> Vec<String> {
    let mut expected = vec![
        String::from(GATEWAY_A_DIRECTORY_NAME),
        format!("{GATEWAY_A_DIRECTORY_NAME}/source"),
        format!("{GATEWAY_A_DIRECTORY_NAME}/source/loops.bells"),
    ];
    for channel in 0..3 {
        expected.push(format!(
            "{GATEWAY_A_DIRECTORY_NAME}/source/completion-{channel}.queue"
        ));
        expected.push(format!(
            "{GATEWAY_A_DIRECTORY_NAME}/source/submission-{channel}.queue"
        ));
    }
    for instance in [ARCHIVE_DIRECTORY_NAME, GATEWAY_B_DIRECTORY_NAME] {
        expected.push(String::from(instance));
        expected.push(format!("{instance}/sink"));
        expected.push(format!("{instance}/sink/loops.bells"));
        expected.push(format!("{instance}/sink/{A_TO_B_DIRECTORY_NAME}"));
        expected.push(format!(
            "{instance}/sink/{A_TO_B_DIRECTORY_NAME}/egress-2.queue"
        ));
    }
    expected.push(String::from(FLOWS_DIRECTORY_NAME));
    expected.push(format!(
        "{FLOWS_DIRECTORY_NAME}/{}",
        flow_directory_name("a-to-b")
    ));
    expected.push(format!(
        "{FLOWS_DIRECTORY_NAME}/{}/channels.bells",
        flow_directory_name("a-to-b")
    ));
    expected.sort_unstable();
    expected
}

fn flow_directory_name(flow: &str) -> String {
    egress_queue_path(Path::new(""), flow, 0)
        .parent()
        .and_then(Path::file_name)
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

pub(in crate::pipeline::reconfigure::resource_stage) fn directory_names(
    path: &Path,
) -> io::Result<Vec<String>> {
    let mut names = std::fs::read_dir(path)?
        .map(|entry| {
            entry?.file_name().into_string().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Directory entry is not UTF-8")
            })
        })
        .collect::<io::Result<Vec<_>>>()?;
    names.sort_unstable();
    Ok(names)
}

fn directory_mode(path: &Path) -> io::Result<u32> {
    Ok(std::fs::metadata(path)?.mode() & 0o777)
}

/// Lists every entry below `path` as sorted root-relative paths.
fn directory_tree(path: &Path) -> io::Result<Vec<String>> {
    let mut entries = Vec::new();
    collect_directory_tree(path, Path::new(""), &mut entries)?;
    entries.sort_unstable();
    Ok(entries)
}

fn collect_directory_tree(
    directory: &Path,
    prefix: &Path,
    entries: &mut Vec<String>,
) -> io::Result<()> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let relative = prefix.join(entry.file_name());
        entries.push(
            relative
                .to_str()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "Directory entry is not UTF-8")
                })?
                .to_owned(),
        );
        if entry.file_type()?.is_dir() {
            collect_directory_tree(&entry.path(), &relative, entries)?;
        }
    }
    Ok(())
}

fn wait_queue_record(reader: &mut QueueReader, queue: &str) -> io::Result<Vec<u8>> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match reader.try_read().map_err(io::Error::other)? {
            ReadOutcome::Record(record) => return Ok(record.payload().to_vec()),
            ReadOutcome::Empty if Instant::now() < deadline => thread::yield_now(),
            ReadOutcome::Empty => {
                return Err(io::Error::other(format!(
                    "{queue} Queue record did not arrive"
                )));
            }
        }
    }
}
