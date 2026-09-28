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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, OnceLock, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use prost::Message;
use prost_reflect::{DynamicMessage, ReflectMessage, Value as ProtobufValue};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};
use tokio::sync::oneshot;

use super::ChannelRuntimeSpec;
use super::error::PipelineWorker;
use super::prepared_pipeline_runtime::test_support as prepared_runtime_test_support;
use super::startup::test_support as startup_test_support;
use super::worker::{WorkerCompletionContext, WorkerExit, WorkerRunResult, WorkerTask};
use super::{
    FlowRuntimeSpec, PipelineDrainObservation, PipelineRuntime, PipelineRuntimeError,
    StartupControl,
};
use crate::config::ScriptVmLimits;
use crate::contracts::sink::EgressRecord;
use crate::contracts::source::{IngressCompletion, IngressCompletionStatus, IngressRecord};
use crate::identifiers::{FlowId, PluginInstanceId, SinkContractId};
use crate::lua::LuaVmErrorKind;
use crate::lua::tests::{lua_builder_contract, lua_source_contract};
use crate::pipeline::channel::{
    ChannelDefinitionChange, PreparedEgressQueue, PreparedEgressRoutes,
};
use crate::pipeline::channel::{
    FlowChannelBells, FlowChannelError, FlowChannelQueuePaths, FlowChannelSpec,
};
use crate::pipeline::diagnostics::test_support::publisher as test_publisher;
use crate::pipeline::ingress_queue::{
    COMPLETION_MAX_PAYLOAD_SIZE, IngressQueueError, completion_capacity, submission_capacity,
};
use crate::tenon_document::SourceDelivery;
use tenon_ipc::bell::{BellRegion, LoopBell, create_bell_region, fail_next_platform_wake};
use tenon_ipc::queue::WriteReceipt;
use tenon_ipc::queue::{
    DataCapacity, QueueReader, QueueWaiter, QueueWriter, ReadOutcome, TEST_RELEASE_OFFSET,
    WriteOutcome, create_queue_file, queue_waiter_is_armed, record_frame_len,
};

const WAIT_LIMIT: Duration = Duration::from_secs(10);
const SOURCE_PENDING_LIMIT: u64 = 4;
const SOURCE_RECORD_SIZE: u64 = 512;
const EGRESS_CAPACITY: u64 = 4_096;
const TEST_MEMORY_LIMIT_BYTES: usize = 4 * 1024 * 1024;
const TEST_CPU_TIME_LIMIT: Duration = Duration::from_millis(100);
const SHUTDOWN_ABORT_CHILD: &str = "TENON_TEST_SHUTDOWN_ABORT_CHILD";

struct RuntimeInputs {
    diagnostics: crate::pipeline::diagnostics::PipelineDiagnosticsPublisher,
    flows: BTreeMap<FlowId, FlowRuntimeSpec>,
}

impl RuntimeInputs {
    fn new(
        diagnostics: crate::pipeline::diagnostics::PipelineDiagnosticsPublisher,
        flows: BTreeMap<FlowId, FlowRuntimeSpec>,
    ) -> Self {
        Self { diagnostics, flows }
    }
}

fn start_runtime(
    inputs: RuntimeInputs,
    startup: Arc<StartupControl>,
) -> Result<PipelineRuntime, PipelineRuntimeError> {
    PipelineRuntime::prepare_resources(inputs.diagnostics, inputs.flows, None, startup)
        .and_then(bind_and_activate)
}

fn start_runtime_with_spawner(
    inputs: RuntimeInputs,
    startup: Arc<StartupControl>,
    spawner: &mut impl FnMut(String, WorkerTask) -> io::Result<thread::JoinHandle<()>>,
) -> Result<PipelineRuntime, PipelineRuntimeError> {
    PipelineRuntime::prepare_resource_workers(
        inputs.diagnostics,
        inputs.flows,
        None,
        startup,
        spawner,
    )
    .and_then(bind_and_activate)
}

#[expect(clippy::expect_used, reason = "the test requires a local executor")]
fn bind_and_activate(
    mut prepared: super::PreparedPipelineRuntime,
) -> Result<PipelineRuntime, PipelineRuntimeError> {
    current_thread_executor()
        .expect("test executor must start")
        .block_on(prepared.bind())?;
    Ok(prepared.activate())
}

fn startup_control() -> Arc<StartupControl> {
    Arc::new(StartupControl::new())
}

#[test]
fn multiple_channels_own_independent_lua_state_and_egress_queues() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut first = SourceEndpoint::create(directory.path(), 0)?;
    let mut second = SourceEndpoint::create(directory.path(), 1)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let (second_binding, mut second_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")
        local count = 0

        function main(event)
            count = count + 1
            builder:setLabel(event.payload.deviceId .. ":" .. count)
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![first.queues(), second.queues()],
        vec![vec![binding], vec![second_binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;
    let pipeline = runtime
        .runtime
        .as_ref()
        .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
    let flow =
        &pipeline.flows[&FlowId::try_from(String::from("main")).map_err(io::Error::other)?];
    assert_eq!(flow.worker_count(), 2);

    first.submit(11, source_payload("first")?)?;
    second.submit(22, source_payload("second")?)?;
    let labels = HashSet::from([
        sink_label(&sink.wait_record()?.payload)?,
        sink_label(&second_sink.wait_record()?.payload)?,
    ]);
    assert_eq!(
        labels,
        HashSet::from([String::from("first:1"), String::from("second:1")])
    );
    sink.release(1)?;
    second_sink.release(1)?;
    assert_eq!(
        first.wait_completion()?,
        completion(11, IngressCompletionStatus::Ok)
    );
    assert_eq!(
        second.wait_completion()?,
        completion(22, IngressCompletionStatus::Ok)
    );

    runtime.stop()
}

#[test]
fn replacement_retries_old_unemitted_input_before_running_the_new_vm() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        function main(event)
            if event.payload.deviceId ~= "hold" then
                emit()
            end
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;

    let receipt = source.submit_with_receipt(101, source_payload("hold")?)?;
    source.wait_submission_release(&receipt)?;
    assert!(source.try_completion()?.is_none());

    runtime.replace_lua(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel("new-lua")
            emit(builder:build())
        end
        "#,
    )?;
    assert_eq!(
        source.wait_completion()?,
        completion(101, IngressCompletionStatus::Retry)
    );

    source.submit(102, source_payload("after")?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "new-lua");
    sink.release(1)?;
    assert_eq!(
        source.wait_completion()?,
        completion(102, IngressCompletionStatus::Ok)
    );
    runtime.stop()
}

#[test]
fn replacement_waits_for_an_accepted_egress_boundary_before_cutover() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel("old-lua")
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;

    source.submit(111, source_payload("before")?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "old-lua");
    assert!(source.try_completion()?.is_none());

    let replacement = runtime.replacement(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel("new-lua")
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
    )?;
    let (result_sender, result_receiver) = mpsc::sync_channel(1);
    let replacement = thread::spawn(move || {
        let result = replacement
            .prepare()
            .and_then(|mut prepared| {
                prepared.cutover_in_place()?;
                prepared.into_paused().activate()
            })
            .map_err(|error| error.to_string());
        let _ = result_sender.send(result);
    });
    assert!(
        result_receiver
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );

    sink.release(1)?;
    assert_eq!(
        source.wait_completion()?,
        completion(111, IngressCompletionStatus::Ok)
    );
    result_receiver
        .recv_timeout(WAIT_LIMIT)
        .map_err(|_| io::Error::other("Lua replacement did not finish after Egress release"))?
        .map_err(io::Error::other)?;
    replacement
        .join()
        .map_err(|_| io::Error::other("Lua replacement coordinator panicked"))?;

    source.submit(112, source_payload("after")?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "new-lua");
    sink.release(1)?;
    assert_eq!(
        source.wait_completion()?,
        completion(112, IngressCompletionStatus::Ok)
    );
    runtime.stop()
}

#[test]
fn cut_over_replacement_does_not_consume_source_before_activation() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel("old-lua")
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;
    let mut prepared = runtime
        .replacement(
            "function main(event) emit() end",
            SourceDelivery::AtLeastOnce,
        )?
        .prepare()
        .map_err(io::Error::other)?;
    prepared.cutover_in_place().map_err(io::Error::other)?;
    let paused = prepared.into_paused();

    let receipt = source.submit_with_receipt(114, source_payload("waiting")?)?;
    thread::sleep(Duration::from_millis(100));
    assert!(
        !source
            .submission
            .is_released(&receipt)
            .map_err(io::Error::other)?
    );
    assert!(source.try_completion()?.is_none());
    assert!(sink.try_record()?.is_none());

    paused.activate().map_err(io::Error::other)?;
    assert_eq!(
        source.wait_completion()?,
        completion(114, IngressCompletionStatus::Ok)
    );
    assert!(sink.try_record()?.is_none());
    runtime.stop()
}

#[test]
fn abandoning_a_prepared_replacement_keeps_the_existing_vm() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel("old-lua")
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;
    let prepared = runtime
        .replacement(
            r#"
            local builder = registry:getBuilder("com.example.kafka@1.0.0")

            function main(event)
                builder:setLabel("new-lua")
                emit(builder:build())
            end
            "#,
            SourceDelivery::AtLeastOnce,
        )?
        .prepare()
        .map_err(io::Error::other)?;

    drop(prepared);
    source.submit(115, source_payload("after-abort")?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "old-lua");
    sink.release(1)?;
    assert_eq!(
        source.wait_completion()?,
        completion(115, IngressCompletionStatus::Ok)
    );
    runtime.stop()
}

#[test]
fn planned_stop_ends_a_cut_over_replacement_waiting_for_activation() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, _sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        "function main(event) emit() end",
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;
    let mut prepared = runtime
        .replacement(
            "function main(event) emit(); emit() end",
            SourceDelivery::AtLeastOnce,
        )?
        .prepare()
        .map_err(io::Error::other)?;
    prepared.cutover_in_place().map_err(io::Error::other)?;
    let paused = prepared.into_paused();

    runtime.stop()?;
    drop(paused);
    Ok(())
}

#[test]
fn planned_stop_ends_a_replacement_waiting_on_old_egress_responsibility() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel("old-lua")
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;
    source.submit(116, source_payload("before")?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "old-lua");

    let replacement = runtime.replacement(
        "function main(event) emit() end",
        SourceDelivery::AtLeastOnce,
    )?;
    let (result_sender, result_receiver) = mpsc::sync_channel(1);
    let replacement = thread::spawn(move || {
        let result = replacement.prepare().and_then(|mut prepared| {
            prepared.cutover_in_place()?;
            prepared.into_paused().activate()
        });
        let _ = result_sender.send(result.map_err(|error| error.to_string()));
    });
    assert!(
        result_receiver
            .recv_timeout(Duration::from_millis(100))
            .is_err()
    );

    runtime.stop()?;
    assert!(
        result_receiver
            .recv_timeout(WAIT_LIMIT)
            .map_err(|_| io::Error::other("Lua replacement coordinator did not stop"))?
            .is_err()
    );
    replacement
        .join()
        .map_err(|_| io::Error::other("Lua replacement coordinator panicked"))?;
    Ok(())
}

#[test]
fn failed_replacement_keeps_every_existing_channel_vm() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut first = SourceEndpoint::create(directory.path(), 0)?;
    let mut second = SourceEndpoint::create(directory.path(), 1)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let (second_binding, mut second_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel("old-lua")
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![first.queues(), second.queues()],
        vec![vec![binding], vec![second_binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;

    let error = match runtime
        .replacement("function main(", SourceDelivery::AtLeastOnce)?
        .prepare()
    {
        Ok(_) => {
            return Err(io::Error::other(
                "Invalid replacement Lua unexpectedly applied",
            ));
        }
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PipelineRuntimeError::FlowChannelReplacementPreparation { .. }
    ));

    first.submit(121, source_payload("first")?)?;
    second.submit(122, source_payload("second")?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "old-lua");
    assert_eq!(sink_label(&second_sink.wait_record()?.payload)?, "old-lua");
    sink.release(1)?;
    second_sink.release(1)?;
    assert_eq!(
        first.wait_completion()?,
        completion(121, IngressCompletionStatus::Ok)
    );
    assert_eq!(
        second.wait_completion()?,
        completion(122, IngressCompletionStatus::Ok)
    );
    runtime.stop()
}

#[test]
fn slow_sink_type_blocks_only_the_channel_using_that_type() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut slow_source = SourceEndpoint::create(directory.path(), 0)?;
    let mut fast_source = SourceEndpoint::create(directory.path(), 1)?;
    let slow_id = sink_contract_id("com.example.slow@1.0.0")?;
    let fast_id = sink_contract_id("com.example.fast@1.0.0")?;
    let (slow_binding, mut slow_sink) = EgressEndpoint::create(directory.path(), "slow", &slow_id)?;
    let (fast_binding, mut fast_sink) = EgressEndpoint::create(directory.path(), "fast", &fast_id)?;
    let (unused_fast, _unused_fast_reader) =
        EgressEndpoint::create_for_channel(directory.path(), 0, "fast", &fast_id)?;
    let (unused_slow, _unused_slow_reader) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "slow", &slow_id)?;
    let spec = runtime_spec(
        r#"
        local slow = registry:getBuilder("com.example.slow@1.0.0")
        local fast = registry:getBuilder("com.example.fast@1.0.0")

        function main(event)
            if event.payload.deviceId == "slow" then
                slow:setLabel("slow-output")
                emit(slow:build())
            else
                fast:setLabel("fast-output")
                emit(fast:build())
            end
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&slow_id, &fast_id],
        vec![slow_source.queues(), fast_source.queues()],
        vec![
            vec![slow_binding, unused_fast],
            vec![unused_slow, fast_binding],
        ],
    )?;
    let runtime = RunningRuntime::start(spec)?;

    slow_source.submit(31, source_payload("slow")?)?;
    assert_eq!(
        sink_label(&slow_sink.wait_record()?.payload)?,
        "slow-output"
    );
    assert!(slow_source.try_completion()?.is_none());

    fast_source.submit(32, source_payload("fast")?)?;
    assert_eq!(
        sink_label(&fast_sink.wait_record()?.payload)?,
        "fast-output"
    );
    fast_sink.release(1)?;
    assert_eq!(
        fast_source.wait_completion()?,
        completion(32, IngressCompletionStatus::Ok)
    );
    assert!(slow_source.try_completion()?.is_none());

    slow_sink.release(1)?;
    assert_eq!(
        slow_source.wait_completion()?,
        completion(31, IngressCompletionStatus::Ok)
    );
    runtime.stop()
}

#[test]
fn planned_shutdown_wakes_a_channel_waiting_for_sink_release() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel("pending-output")
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;

    source.submit(41, source_payload("pending")?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "pending-output");
    assert!(source.try_completion()?.is_none());

    runtime.stop()
}

#[test]
fn prepared_runtime_keeps_source_and_timer_work_paused_until_activation() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")
        setTimeout(0)

        function main(event)
            if event.type == "timer" then
                builder:setLabel("timer")
            else
                builder:setLabel("source")
            end
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;

    let startup = startup_control();
    let decision_guard = startup_test_support::hold_decision_lock(&startup);
    let RuntimeInputs { diagnostics, flows } = spec;
    let mut prepared =
        PipelineRuntime::prepare_resources(diagnostics, flows, None, Arc::clone(&startup))
            .map_err(io::Error::other)?;
    source.submit(40, source_payload("pending")?)?;
    assert!(prepared_runtime_test_support::is_pending(&prepared));
    assert!(sink.try_record()?.is_none());
    assert!(source.try_completion()?.is_none());

    drop(decision_guard);
    current_thread_executor()?
        .block_on(prepared.bind())
        .map_err(io::Error::other)?;
    assert!(sink.try_record()?.is_none());
    assert!(source.try_completion()?.is_none());
    let runtime = prepared.activate();
    let first_label = sink_label(&sink.wait_record()?.payload)?;
    sink.release(1)?;
    let second_label = sink_label(&sink.wait_record()?.payload)?;
    sink.release(1)?;
    let labels = HashSet::from([first_label, second_label]);
    assert_eq!(
        labels,
        HashSet::from([String::from("source"), String::from("timer")])
    );
    assert_eq!(
        source.wait_completion()?,
        completion(40, IngressCompletionStatus::Ok)
    );
    RunningRuntime {
        runtime: Some(runtime),
        executor: current_thread_executor()?,
        initial_routes: Vec::new(),
    }
    .stop()
}

#[test]
fn one_channel_startup_failure_aborts_every_ready_sibling() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let missing = TestChannelQueues::at(directory.path(), 1)?;
    let (second_binding, mut second_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")
        setTimeout(0)

        function main(event)
            if event.type == "timer" then
                builder:setLabel("startup-leak")
                emit(builder:build())
            end
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues(), missing],
        vec![vec![binding], vec![second_binding]],
    )?;

    let error = start_error(spec)?;
    assert!(matches!(
        error,
        PipelineRuntimeError::FlowChannelOpen {
            flow_id,
            channel_index: 1,
            ..
        } if flow_id.as_str() == "main"
    ));
    assert!(sink.try_record()?.is_none());
    assert!(second_sink.try_record()?.is_none());
    Ok(())
}

#[test]
fn channel_thread_spawn_failure_aborts_and_joins_already_started_channels() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let first = SourceEndpoint::create(directory.path(), 0)?;
    let second = SourceEndpoint::create(directory.path(), 1)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let (second_binding, mut second_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")
        setTimeout(0)

        function main(event)
            if event.type == "timer" then
                builder:setLabel("startup-leak")
                emit(builder:build())
            end
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![first.queues(), second.queues()],
        vec![vec![binding], vec![second_binding]],
    )?;
    let mut spawner = fail_thread_spawn_at(1);

    let error = match start_runtime_with_spawner(spec, startup_control(), &mut spawner) {
        Ok(runtime) => {
            drop(runtime);
            return Err(io::Error::other("Pipeline runtime unexpectedly started"));
        }
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PipelineRuntimeError::WorkerSpawn {
            worker: PipelineWorker {
                flow_id,
                channel_index: 1,
            },
            ..
        } if flow_id.as_str() == "main"
    ));
    assert!(sink.try_record()?.is_none());
    assert!(second_sink.try_record()?.is_none());
    Ok(())
}

#[test]
fn startup_abort_interrupts_channel_lua_initialization() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, _sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        "while true do end",
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let startup = startup_control();
    startup.abort();

    let error = match start_runtime(spec, startup) {
        Ok(runtime) => {
            drop(runtime);
            return Err(io::Error::other(
                "Aborted startup unexpectedly completed Lua initialization",
            ));
        }
        Err(error) => error,
    };

    assert!(matches!(
        error,
        PipelineRuntimeError::FlowChannelOpen {
            flow_id,
            channel_index: 0,
            source: FlowChannelError::LuaVmLoad {
                kind: LuaVmErrorKind::ExecutionStopped,
            },
        } if flow_id.as_str() == "main"
    ));
    Ok(())
}

#[test]
fn one_channel_failure_stops_and_joins_the_whole_runtime() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut failed = SourceEndpoint::create(directory.path(), 0)?;
    let waiting = SourceEndpoint::create(directory.path(), 1)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, _sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let (second_binding, _second_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        "function main(event) end",
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![failed.queues(), waiting.queues()],
        vec![vec![binding], vec![second_binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;

    let receipt = failed.submit_raw_with_receipt(&[0x0a, 0x02, b'x'])?;
    let error = runtime.wait_for_failure()?;
    assert!(matches!(
        error,
        PipelineRuntimeError::FlowChannelFailed {
            flow_id,
            channel_index: 0,
            source: FlowChannelError::IngressQueue {
                source: IngressQueueError::IngressRecordDecode(_),
            },
        } if flow_id.as_str() == "main"
    ));
    assert!(
        !failed
            .submission
            .is_released(&receipt)
            .map_err(io::Error::other)?
    );
    assert!(failed.try_completion()?.is_none());
    Ok(())
}

#[test]
fn worker_exit_wait_is_cancellation_safe() -> io::Result<()> {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum SelectedEvent {
        Control,
        WorkerExit,
    }

    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel("control-loop")
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let mut runtime = start_runtime(spec, startup_control()).map_err(io::Error::other)?;
    let executor = current_thread_executor()?;
    let (control_sender, control_receiver) = oneshot::channel();

    let selected = executor.block_on(async {
        let sender_task = tokio::spawn(async move {
            let _ = control_sender.send(());
        });
        let selected = tokio::select! {
            () = runtime.wait_for_worker_exit() => SelectedEvent::WorkerExit,
            result = control_receiver => {
                result.map_err(|_| io::Error::other("Control event sender was dropped"))?;
                SelectedEvent::Control
            }
        };
        sender_task.await.map_err(io::Error::other)?;
        Ok::<SelectedEvent, io::Error>(selected)
    })?;
    assert_eq!(selected, SelectedEvent::Control);

    sink.corrupt_release_position()?;
    source.submit(61, source_payload("failure-after-control")?)?;
    executor
        .block_on(async { tokio::time::timeout(WAIT_LIMIT, runtime.wait_for_worker_exit()).await })
        .map_err(|_| io::Error::other("Pipeline worker exit was lost after select cancellation"))?;
    let error = match executor.block_on(runtime.stop_and_join()) {
        Ok(()) => return Err(io::Error::other("Corrupted Egress release was ignored")),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PipelineRuntimeError::FlowChannelFailed { .. }
    ));
    Ok(())
}

#[test]
fn final_stop_freezes_an_observed_failure_before_publishing_stop() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let mut runtime = start_runtime(spec, startup_control()).map_err(io::Error::other)?;
    let executor = current_thread_executor()?;

    sink.corrupt_release_position()?;
    source.submit(71, source_payload("failure-before-stop")?)?;
    executor
        .block_on(async { tokio::time::timeout(WAIT_LIMIT, runtime.wait_for_worker_exit()).await })
        .map_err(|_| io::Error::other("Pipeline worker exit was never observed"))?;

    // No shutdown directive has been published yet, so the frozen interpretation
    // must retain this failure. Publishing Stop first would make the same Flow
    // report a planned stop and lose the original cause.
    let flow = runtime
        .flows
        .values_mut()
        .next()
        .ok_or_else(|| io::Error::other("The runtime has no Flow"))?;
    let (context, _workers) = flow.begin_final_stop();
    assert_eq!(context, WorkerCompletionContext::FailureAlreadyObserved);

    let error = match executor.block_on(runtime.stop_and_join()) {
        Ok(()) => return Err(io::Error::other("Corrupted Egress release was ignored")),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PipelineRuntimeError::FlowChannelFailed { .. }
    ));
    Ok(())
}

#[test]
#[allow(
    clippy::panic,
    reason = "The test must cross the production worker panic boundary"
)]
fn finished_worker_panic_is_caught_by_nonblocking_exit_observation() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, _sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        "function main(event) end",
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let mut injected = false;
    let started = {
        let mut spawner = |name: String, mut task: WorkerTask| {
            if !injected {
                injected = true;
                let work = task.work;
                task.work = Box::new(move || {
                    let _ = work();
                    panic!("Injected Channel panic")
                });
            }
            thread::Builder::new().name(name).spawn(move || task.run())
        };
        start_runtime_with_spawner(spec, startup_control(), &mut spawner)
    };
    let mut runtime = started.map_err(io::Error::other)?;
    assert!(injected, "A Channel worker was not selected for injection");
    runtime.request_flow_retirement(&BTreeSet::from([flow_id("main")?]));

    let deadline = Instant::now() + WAIT_LIMIT;
    while !super::worker::test_support::has_finished_thread(
        runtime
            .flows
            .get_mut(&flow_id("main")?)
            .ok_or_else(|| io::Error::other("Flow is missing"))?
            .worker_set_mut(),
    ) {
        if Instant::now() >= deadline {
            return Err(io::Error::other("Injected Egress worker did not exit"));
        }
        thread::yield_now();
    }
    assert!(runtime.has_worker_exited_now());

    let executor = current_thread_executor()?;
    let error = match executor.block_on(runtime.stop_and_join()) {
        Ok(()) => return Err(io::Error::other("Injected worker panic was ignored")),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        PipelineRuntimeError::WorkerPanicked {
            worker: PipelineWorker { .. }
        }
    ));
    Ok(())
}

#[test]
fn shutdown_wake_failure_aborts_the_pipeline_process() -> io::Result<()> {
    if std::env::var_os(SHUTDOWN_ABORT_CHILD).is_some() {
        let directory = tempfile::tempdir()?;
        let source = SourceEndpoint::create(directory.path(), 0)?;
        let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
        let (binding, _sink) =
            EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
        let spec = runtime_spec(
            "function main(event) end",
            SourceDelivery::AtLeastOnce,
            [&sink_contract_id],
            vec![source.queues()],
            vec![vec![binding]],
        )?;
        let runtime = start_runtime(spec, startup_control()).map_err(io::Error::other)?;
        source.wait_submission_reader_armed()?;
        fail_next_platform_wake();
        current_thread_executor()?
            .block_on(runtime.stop_and_join())
            .map_err(io::Error::other)?;
        return Err(io::Error::other("Pipeline process did not abort"));
    }

    let status = Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "pipeline::runtime::tests::shutdown_wake_failure_aborts_the_pipeline_process",
        ])
        .env(SHUTDOWN_ABORT_CHILD, "1")
        .status()?;
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(libc::SIGABRT)
    );
    Ok(())
}

#[test]
fn egress_queue_failure_remains_the_primary_runtime_error() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel("queue-failure")
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;
    sink.corrupt_release_position()?;

    source.submit(71, source_payload("failure")?)?;
    let error = runtime.wait_for_failure()?;
    assert!(matches!(
        error,
        PipelineRuntimeError::FlowChannelFailed { .. }
    ));
    Ok(())
}

#[test]
fn channel_definition_replacement_installs_the_target_registry_and_routes() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let existing_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let added_id = sink_contract_id("com.example.iotdb@1.0.0")?;
    let (existing_binding, mut existing_sink) =
        EgressEndpoint::create(directory.path(), "existing", &existing_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel("existing")
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&existing_id],
        vec![source.queues()],
        vec![vec![existing_binding.clone()]],
    )?;
    let runtime = start_runtime(spec, startup_control()).map_err(io::Error::other)?;

    let (added_binding, mut added_sink) =
        EgressEndpoint::create(directory.path(), "added", &added_id)?;
    let mut target_routes = prepared_routes(
        vec![
            (existing_id.clone(), existing_binding.1),
            (added_id.clone(), added_binding.1),
        ],
        Some(&Arc::clone(&source.channel_region)),
    );
    let added_bell = added_sink.own_bell()?;
    for queue in target_routes
        .get_mut(&added_id)
        .into_iter()
        .flat_map(BTreeMap::values_mut)
    {
        let (path, peer_region) = match queue {
            PreparedEgressQueue::Retained { path, peer_region } => {
                (path.clone(), Arc::clone(peer_region))
            }
            PreparedEgressQueue::New(_) => unreachable!("fixture uses paths"),
        };
        *queue = PreparedEgressQueue::New(
            QueueWriter::open(&path, Arc::clone(&added_bell), peer_region)
                .map_err(io::Error::other)?,
        );
    }
    let target_spec = channel_spec(
        r#"
        local existing = registry:getBuilder("com.example.kafka@1.0.0")
        local added = registry:getBuilder("com.example.iotdb@1.0.0")

        function main(event)
            if event.payload.deviceId == "added" then
                added:setLabel("added")
                emit(added:build())
            else
                existing:setLabel("existing")
                emit(existing:build())
            end
        end
        "#,
        SourceDelivery::AtLeastOnce,
        target_routes.keys(),
    )?;
    let mut prepared = runtime
        .flow_definition_replacement(&flow_id("main")?)
        .map_err(io::Error::other)?
        .begin(
            ChannelDefinitionChange::Replace(target_spec),
            vec![target_routes],
        )
        .and_then(|pending| pending.wait())
        .map_err(io::Error::other)?;
    prepared.cutover_in_place().map_err(io::Error::other)?;
    let paused = prepared.into_paused();
    paused.activate().map_err(io::Error::other)?;

    source.submit(401, source_payload("added")?)?;
    assert_eq!(sink_label(&added_sink.wait_record()?.payload)?, "added");
    added_sink.release(1)?;
    assert_eq!(
        source.wait_completion()?,
        completion(401, IngressCompletionStatus::Ok)
    );
    source.submit(402, source_payload("existing")?)?;
    assert_eq!(
        sink_label(&existing_sink.wait_record()?.payload)?,
        "existing"
    );
    existing_sink.release(1)?;
    assert_eq!(
        source.wait_completion()?,
        completion(402, IngressCompletionStatus::Ok)
    );

    current_thread_executor()?
        .block_on(runtime.stop_and_join())
        .map_err(io::Error::other)
}

#[test]
fn planned_shutdown_wakes_a_channel_blocked_by_a_full_completion_queue() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, _sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        "function main(event) emit() end",
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;
    let runtime = RunningRuntime::start(spec)?;

    let mut committed_record_ids = Vec::new();
    for offset in 0..=SOURCE_PENDING_LIMIT {
        let record_id = u64::MAX - offset;
        let receipt = source.submit_with_receipt(record_id, source_payload("full")?)?;
        source.wait_submission_release(&receipt)?;
        committed_record_ids.push(record_id);
    }
    let blocked = source.submit_with_receipt(
        u64::MAX - (SOURCE_PENDING_LIMIT + 1),
        source_payload("blocked")?,
    )?;
    source.wait_submission_release(&blocked)?;
    let mut observed_record_ids = Vec::new();
    for _ in 0..committed_record_ids.len() {
        let completion = source
            .try_completion_without_release()?
            .ok_or_else(|| io::Error::other("Expected committed completion was missing"))?;
        observed_record_ids.push(completion.record_id);
    }
    assert_eq!(observed_record_ids, committed_record_ids);
    assert!(source.try_completion_without_release()?.is_none());

    runtime.stop()
}

#[test]
fn a_full_queue_does_not_block_another_channel_of_the_same_sink() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut first = SourceEndpoint::create(directory.path(), 0)?;
    let mut second = SourceEndpoint::create(directory.path(), 1)?;
    let contract = sink_contract_id("com.example.kafka@1.0.0")?;
    let (first_binding, mut first_sink) = EgressEndpoint::create_with_capacity(
        directory.path(),
        "kafka",
        &contract,
        egress_capacity_for("output")?,
    )?;
    let (second_binding, mut second_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &contract)?;
    let runtime = RunningRuntime::start(runtime_spec(
        r#"local builder = registry:getBuilder("com.example.kafka@1.0.0")
        function main(event) builder:setLabel("output"); emit(builder:build()) end"#,
        SourceDelivery::AtMostOnce,
        [&contract],
        vec![first.queues(), second.queues()],
        vec![vec![first_binding], vec![second_binding]],
    )?)?;
    first.submit(91, source_payload("first")?)?;
    assert_eq!(sink_label(&first_sink.wait_record()?.payload)?, "output");
    first.submit(92, source_payload("blocked")?)?;
    second.submit(93, source_payload("independent")?)?;
    assert_eq!(sink_label(&second_sink.wait_record()?.payload)?, "output");
    second_sink.release(1)?;
    assert!(first_sink.try_record()?.is_none());
    first_sink.release(1)?;
    assert_eq!(sink_label(&first_sink.wait_record()?.payload)?, "output");
    first_sink.release(1)?;
    runtime.stop()
}

#[test]
fn channel_set_replacement_drains_old_output_and_reopens_the_retained_queue() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    // Two Source generations of one Flow's Channel zero: the replacement Source
    // writes its Queues into its own directory while the Channel Region stays
    // where the Sink bound to this Channel already maps it.
    let flow_root = directory.path().join("flow");
    let old_source_directory = flow_root.join("old-source");
    let new_source_directory = flow_root.join("new-source");
    let mut old_source = SourceEndpoint::create_in_flow(&old_source_directory, 0, &flow_root)?;
    let mut new_source = SourceEndpoint::create_in_flow(&new_source_directory, 0, &flow_root)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let lua_source = r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel(event.payload.deviceId)
            emit(builder:build())
        end
        "#;
    let spec = runtime_spec(
        lua_source,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![old_source.queues()],
        vec![vec![binding.clone()]],
    )?;
    let runtime = start_runtime(spec, startup_control()).map_err(io::Error::other)?;

    old_source.submit(201, source_payload("old")?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "old");
    assert!(old_source.try_completion()?.is_none());
    old_source.submit(202, source_payload("queued")?)?;

    let (retired_sender, retired_receiver) = mpsc::sync_channel(1);
    let retire_thread = thread::spawn(move || {
        let result = current_thread_executor().and_then(|executor| {
            executor.block_on(async {
                let mut runtime = runtime;
                let flows = runtime.flows.keys().cloned().collect();
                runtime.request_flow_retirement(&flows);
                match std::future::poll_fn(|context| {
                    runtime.poll_resource_retirement(&flows, context)
                })
                .await
                {
                    PipelineDrainObservation::Drained => {
                        runtime
                            .finish_resource_retirement(&flows)
                            .await
                            .map_err(io::Error::other)?;
                        Ok(runtime)
                    }
                    PipelineDrainObservation::WorkerExited => Err(io::Error::other(
                        observed_retirement_failure(runtime).await?,
                    )),
                }
            })
        });
        let _ = retired_sender.send(result);
    });
    assert!(matches!(
        retired_receiver.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));

    sink.release(1)?;
    assert_eq!(
        old_source.wait_completion()?,
        completion(201, IngressCompletionStatus::Ok)
    );
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "queued");
    assert!(old_source.try_completion()?.is_none());
    assert!(matches!(
        retired_receiver.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    sink.release(1)?;
    assert_eq!(
        old_source.wait_completion()?,
        completion(202, IngressCompletionStatus::Ok)
    );
    let retained = retired_receiver
        .recv_timeout(WAIT_LIMIT)
        .map_err(io::Error::other)??;
    retire_thread
        .join()
        .map_err(|_| io::Error::other("Channel retirement thread panicked"))?;

    let replacement =
        flow_runtime_spec(lua_source, vec![new_source.queues()], vec![vec![binding]])?;
    let mut prepared = PipelineRuntime::prepare_resources(
        test_publisher(),
        BTreeMap::from([(flow_id("main")?, replacement)]),
        Some(retained.started_at()),
        startup_control(),
    )
    .map_err(io::Error::other)?;
    prepared.retain(retained);
    current_thread_executor()?
        .block_on(prepared.bind())
        .map_err(io::Error::other)?;
    let runtime = prepared.activate();

    new_source.submit(203, source_payload("new")?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "new");
    sink.release(1)?;
    assert_eq!(
        new_source.wait_completion()?,
        completion(203, IngressCompletionStatus::Ok)
    );
    current_thread_executor()?
        .block_on(runtime.stop_and_join())
        .map_err(io::Error::other)
}

#[test]
fn cancelling_channel_retirement_stops_egress_and_joins_the_channel_worker() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut source = SourceEndpoint::create(directory.path(), 0)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel(event.payload.deviceId)
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![source.queues()],
        vec![vec![binding]],
    )?;

    let (channel_exited_sender, channel_exited_receiver) = mpsc::sync_channel(1);
    let (allow_channel_join_sender, allow_channel_join_receiver) = mpsc::sync_channel(1);
    let mut channel_join_barrier = Some(allow_channel_join_receiver);
    let mut spawner = move |name: String, task: WorkerTask| {
        let channel_join_barrier = if name.starts_with("tenon-flow-main-channel-") {
            channel_join_barrier.take()
        } else {
            None
        };
        let channel_exited_sender = channel_exited_sender.clone();
        thread::Builder::new().name(name).spawn(move || {
            task.run();
            if let Some(channel_join_barrier) = channel_join_barrier {
                let _ = channel_exited_sender.send(());
                let _ = channel_join_barrier.recv();
            }
        })
    };
    let runtime = start_runtime_with_spawner(spec, startup_control(), &mut spawner)
        .map_err(io::Error::other)?;

    source.submit(301, source_payload("blocked")?)?;
    assert_eq!(sink_label(&sink.wait_record()?.payload)?, "blocked");

    let (retirement_sender, retirement_receiver) = mpsc::sync_channel(1);
    let retirement_thread = thread::spawn(move || {
        let mut runtime = runtime;
        let flows = runtime.flows.keys().cloned().collect();
        runtime.request_flow_retirement(&flows);
        let was_pending = runtime
            .poll_resource_retirement(
                &flows,
                &mut std::task::Context::from_waker(std::task::Waker::noop()),
            )
            .is_pending();
        drop(runtime);
        let _ = retirement_sender.send(was_pending);
    });

    if channel_exited_receiver.recv_timeout(WAIT_LIMIT).is_err() {
        sink.release(1)?;
        let _ = channel_exited_receiver.recv_timeout(WAIT_LIMIT);
        let _ = allow_channel_join_sender.send(());
        let _ = retirement_receiver.recv_timeout(WAIT_LIMIT);
        let _ = retirement_thread.join();
        return Err(io::Error::other(
            "Cancelling Channel retirement did not stop Egress before joining the Channel",
        ));
    }
    if retirement_receiver
        .recv_timeout(Duration::from_millis(100))
        .is_ok()
    {
        let _ = allow_channel_join_sender.send(());
        let _ = retirement_thread.join();
        return Err(io::Error::other(
            "Cancelled Channel retirement returned before joining the Channel worker",
        ));
    }

    allow_channel_join_sender
        .send(())
        .map_err(|_| io::Error::other("Channel join barrier disconnected"))?;
    assert!(
        retirement_receiver
            .recv_timeout(WAIT_LIMIT)
            .map_err(io::Error::other)?
    );
    retirement_thread
        .join()
        .map_err(|_| io::Error::other("Channel retirement thread panicked"))?;
    Ok(())
}

#[test]
fn channel_failure_interrupts_retirement_while_another_channel_waits_for_egress_release()
-> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut blocked_source = SourceEndpoint::create(directory.path(), 0)?;
    let mut failing_source = SourceEndpoint::create(directory.path(), 1)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let (second_binding, _second_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            if event.payload.deviceId == "blocked" then
                emit(builder:build())
            else
                emit()
            end
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![blocked_source.queues(), failing_source.queues()],
        vec![vec![binding], vec![second_binding]],
    )?;
    let runtime = start_runtime(spec, startup_control()).map_err(io::Error::other)?;

    blocked_source.submit(401, source_payload("blocked")?)?;
    let _blocked_record = sink.wait_record()?;
    failing_source.corrupt_completion_release_position()?;
    failing_source.submit(402, source_payload("failure")?)?;

    let error = current_thread_executor()?.block_on(async {
        let mut runtime = runtime;
        let flows = runtime.flows.keys().cloned().collect();
        runtime.request_flow_retirement(&flows);
        let observation = tokio::time::timeout(
            WAIT_LIMIT,
            std::future::poll_fn(|context| runtime.poll_resource_retirement(&flows, context)),
        )
        .await
        .map_err(|_| io::Error::other("Channel failure did not interrupt retirement"))?;
        assert_eq!(observation, PipelineDrainObservation::WorkerExited);
        observed_retirement_failure(runtime).await
    })?;
    assert!(matches!(
        error,
        PipelineRuntimeError::FlowChannelFailed {
            flow_id,
            channel_index: 1,
            ..
        } if flow_id.as_str() == "main"
    ));
    Ok(())
}

#[test]
fn egress_failure_interrupts_retirement_while_a_channel_waits_for_release() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut blocked_source = SourceEndpoint::create(directory.path(), 0)?;
    let mut pump_waker = SourceEndpoint::create(directory.path(), 1)?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let (binding, mut sink) = EgressEndpoint::create(directory.path(), "kafka", &sink_contract_id)?;
    let (second_binding, second_sink) =
        EgressEndpoint::create_for_channel(directory.path(), 1, "kafka", &sink_contract_id)?;
    let spec = runtime_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
        vec![blocked_source.queues(), pump_waker.queues()],
        vec![vec![binding], vec![second_binding]],
    )?;
    let runtime = start_runtime(spec, startup_control()).map_err(io::Error::other)?;

    blocked_source.submit(501, source_payload("blocked")?)?;
    let _blocked_record = sink.wait_record()?;
    second_sink.corrupt_release_position()?;
    pump_waker.submit(502, source_payload("wake-pump")?)?;

    let error = current_thread_executor()?.block_on(async {
        let mut runtime = runtime;
        let flows = runtime.flows.keys().cloned().collect();
        runtime.request_flow_retirement(&flows);
        let observation = tokio::time::timeout(
            WAIT_LIMIT,
            std::future::poll_fn(|context| runtime.poll_resource_retirement(&flows, context)),
        )
        .await
        .map_err(|_| io::Error::other("Egress failure did not interrupt retirement"))?;
        assert_eq!(observation, PipelineDrainObservation::WorkerExited);
        observed_retirement_failure(runtime).await
    })?;
    assert!(matches!(
        error,
        PipelineRuntimeError::FlowChannelFailed { .. }
    ));
    Ok(())
}

async fn observed_retirement_failure(runtime: PipelineRuntime) -> io::Result<PipelineRuntimeError> {
    match runtime.stop_and_join().await {
        Ok(()) => Err(io::Error::other(
            "Observed worker exit produced no runtime failure",
        )),
        Err(error) => Ok(error),
    }
}

fn runtime_spec<'a>(
    lua_source: &str,
    delivery: SourceDelivery,
    sink_contract_ids: impl IntoIterator<Item = &'a SinkContractId>,
    queues: Vec<TestChannelQueues>,
    targets_by_channel: Vec<Vec<EgressBinding>>,
) -> io::Result<RuntimeInputs> {
    assert_eq!(queues.len(), targets_by_channel.len());
    let channels = queues
        .into_iter()
        .zip(targets_by_channel)
        .map(|(queues, bindings)| {
            let TestChannelQueues {
                paths,
                bells,
                channel_region,
            } = queues;
            ChannelRuntimeSpec::new(
                paths,
                bells,
                prepared_routes(bindings, Some(&channel_region)),
            )
        })
        .collect();
    let flow = FlowRuntimeSpec::new(
        channel_spec(lua_source, delivery, sink_contract_ids)?,
        channels,
        None,
    );
    Ok(RuntimeInputs::new(
        test_publisher(),
        BTreeMap::from([(flow_id("main")?, flow)]),
    ))
}

/// Prepares one Channel's Egress routes and, when the fixture still owns the
/// live Sink endpoints, records the Channel Region those Sinks must ring,
/// exactly as a Runner hands both facts to a launched Sink.
fn prepared_routes(
    bindings: Vec<EgressBinding>,
    channel_region: Option<&Arc<BellRegion>>,
) -> PreparedEgressRoutes {
    bindings
        .into_iter()
        .map(|(contract, targets)| {
            (
                contract,
                targets
                    .into_iter()
                    .map(|target| {
                        if let Some(region) = channel_region {
                            target.bind(region);
                        }
                        let prepared = target.prepared();
                        (target.instance, prepared)
                    })
                    .collect(),
            )
        })
        .collect()
}

fn flow_runtime_spec(
    lua_source: &str,
    queues: Vec<TestChannelQueues>,
    targets_by_channel: Vec<Vec<EgressBinding>>,
) -> io::Result<FlowRuntimeSpec> {
    assert_eq!(queues.len(), targets_by_channel.len());
    let contracts = targets_by_channel.iter().flatten().map(|(id, _)| id);
    let spec = channel_spec(lua_source, SourceDelivery::AtLeastOnce, contracts)?;
    Ok(FlowRuntimeSpec::new(
        spec,
        queues
            .into_iter()
            .zip(targets_by_channel)
            .map(|(queues, bindings)| {
                let TestChannelQueues {
                    paths,
                    bells,
                    channel_region,
                } = queues;
                ChannelRuntimeSpec::new(
                    paths,
                    bells,
                    prepared_routes(bindings, Some(&channel_region)),
                )
            })
            .collect(),
        None,
    ))
}

fn channel_spec<'a>(
    lua_source: &str,
    delivery: SourceDelivery,
    sink_contract_ids: impl IntoIterator<Item = &'a SinkContractId>,
) -> io::Result<FlowChannelSpec> {
    let sink_contract = lua_builder_contract()?;
    let sink_contracts = sink_contract_ids
        .into_iter()
        .map(|sink_contract_id| (sink_contract_id.clone(), sink_contract.clone()))
        .collect::<HashMap<_, _>>();
    let memory_limit = NonZeroUsize::new(TEST_MEMORY_LIMIT_BYTES)
        .ok_or_else(|| io::Error::other("Lua memory limit must be positive"))?;
    Ok(FlowChannelSpec::new(
        "source",
        lua_source,
        ScriptVmLimits::try_new(memory_limit, TEST_CPU_TIME_LIMIT).map_err(io::Error::other)?,
        std::num::NonZeroU64::new(262_144)
            .ok_or_else(|| std::io::Error::other("test record limit must be non-zero"))?,
        lua_source_contract()?,
        sink_contracts,
        delivery,
    ))
}

fn start_error(spec: RuntimeInputs) -> io::Result<PipelineRuntimeError> {
    match start_runtime(spec, startup_control()) {
        Ok(runtime) => {
            drop(runtime);
            Err(io::Error::other("Pipeline runtime unexpectedly started"))
        }
        Err(error) => Ok(error),
    }
}

fn source_payload(device_id: &str) -> io::Result<Vec<u8>> {
    let contract = lua_source_contract()?;
    let mut payload = DynamicMessage::new(contract.clone());
    let field = payload
        .descriptor()
        .get_field_by_name("device_id")
        .ok_or_else(|| io::Error::other("Source fixture device_id field is missing"))?;
    payload
        .try_set_field(&field, ProtobufValue::String(device_id.to_owned()))
        .map_err(io::Error::other)?;
    Ok(payload.encode_to_vec())
}

fn sink_label(payload: &[u8]) -> io::Result<String> {
    let contract = lua_builder_contract()?;
    let message = DynamicMessage::decode(contract.clone(), payload).map_err(io::Error::other)?;
    let field = message
        .descriptor()
        .get_field_by_name("label")
        .ok_or_else(|| io::Error::other("Sink fixture label field is missing"))?;
    message
        .get_field(&field)
        .as_ref()
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| io::Error::other("Sink fixture label is not a string"))
}

fn sink_payload(label: &str) -> io::Result<Vec<u8>> {
    let contract = lua_builder_contract()?;
    let mut payload = DynamicMessage::new(contract.clone());
    let field = payload
        .descriptor()
        .get_field_by_name("label")
        .ok_or_else(|| io::Error::other("Sink fixture label field is missing"))?;
    payload
        .try_set_field(&field, ProtobufValue::String(label.to_owned()))
        .map_err(io::Error::other)?;
    Ok(payload.encode_to_vec())
}

fn egress_capacity_for(label: &str) -> io::Result<DataCapacity> {
    let record = EgressRecord {
        payload: sink_payload(label)?,
    };
    let generous_capacity = DataCapacity::try_from(EGRESS_CAPACITY).map_err(io::Error::other)?;
    let max_payload_size =
        NonZeroU64::new(generous_capacity.get() - tenon_ipc::queue::FRAME_HEADER_LEN as u64)
            .ok_or_else(|| io::Error::other("Egress payload limit must be positive"))?;
    let frame_len = record_frame_len(generous_capacity, max_payload_size, record.encoded_len())
        .map_err(io::Error::other)?;
    DataCapacity::try_from(u64::try_from(frame_len).map_err(io::Error::other)?)
        .map_err(io::Error::other)
}

fn completion(record_id: u64, status: IngressCompletionStatus) -> IngressCompletion {
    IngressCompletion {
        record_id,
        status: status as i32,
    }
}

fn sink_contract_id(value: &str) -> io::Result<SinkContractId> {
    SinkContractId::try_from(value).map_err(io::Error::other)
}

fn flow_id(value: &str) -> io::Result<FlowId> {
    FlowId::try_from(String::from(value)).map_err(io::Error::other)
}

fn fail_thread_spawn_at(
    failed_index: usize,
) -> impl FnMut(String, WorkerTask) -> io::Result<thread::JoinHandle<()>> {
    let mut spawn_count = 0_usize;
    move |name, task| {
        let current = spawn_count;
        spawn_count = spawn_count
            .checked_add(1)
            .ok_or_else(|| io::Error::other("Thread spawn count overflowed"))?;
        if current == failed_index {
            return Err(io::Error::other("Injected thread spawn failure"));
        }
        thread::Builder::new().name(name).spawn(move || task.run())
    }
}

/// The Channel doorbell Region of one fixture Flow.
///
/// A Runner keeps one Region per Flow, numbered by Channel index, and every
/// Source generation feeding that Flow maps this same file: replacing the
/// Source moves its Queue files, never the Region its Channels park in. The
/// fixture roots one Region at the directory that holds a Flow's Channel Queues
/// and Source Queues and numbers its slots by the index those Queues are named
/// by, so a Source generation and the one that replaces it reach one mapping and
/// a retained Sink keeps ringing the slots the Channels park in.
fn channel_region_path(flow_root: &Path) -> PathBuf {
    flow_root.join("channels.bells")
}

/// One Source loop parks on one doorbell per Queue endpoint it waits on, so the
/// fixture gives the Submission wait slot 0 and the Completion wait slot 1.
fn source_region_path(submission: &Path) -> PathBuf {
    submission.with_extension("source.bells")
}

/// One Sink loop parks on one doorbell for every Egress Queue it reads.
fn sink_region_path(egress: &Path) -> PathBuf {
    egress.with_extension("sink.bells")
}

const ONE_BELL_SLOT: u32 = 1;
const SOURCE_BELL_SLOTS: u32 = 2;
/// One slot per Channel index a fixture Flow root uses.
///
/// A Channel's Queues are named `submission-{index}.queue` inside its Flow root,
/// so the Channels of one root already differ by index; the Region that numbers
/// their doorbells by that index covers every one of them.
const FLOW_BELL_SLOTS: u32 = 8;

/// Creates or reuses one Bell Region, exactly as the Runner does.
///
/// A Region already holding this many slots is reused unchanged, so a peer that
/// mapped it earlier keeps reaching the loops that park in it.
fn bell_region(path: &Path, slots: u32) -> io::Result<Arc<BellRegion>> {
    let slots = NonZeroU32::new(slots)
        .ok_or_else(|| io::Error::other("a Bell Region must hold at least one slot"))?;
    if let Ok(existing) = BellRegion::open(path)
        && existing.slot_count() == slots
    {
        return Ok(existing);
    }
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    create_bell_region(path, slots, 0).map_err(io::Error::other)?;
    BellRegion::open(path).map_err(io::Error::other)
}

/// Returns one doorbell inside an opened Bell Region.
fn loop_bell(region: &Arc<BellRegion>, slot: u32) -> io::Result<Arc<LoopBell>> {
    region.loop_bell(slot).map_err(io::Error::other)
}

/// The Queue pair and Bell Regions one fixture Channel starts with.
///
/// These are the two facts a Runner hands a Channel: the existing Queue files
/// and the Regions its own loop and the Source loop on the other end bind.
struct TestChannelQueues {
    paths: FlowChannelQueuePaths,
    bells: FlowChannelBells,
    /// The Region the Channel loop parks on, which every Sink reading this
    /// Channel's Egress Queues rings when it releases.
    channel_region: Arc<BellRegion>,
}

impl TestChannelQueues {
    /// Binds one absent Queue pair of Channel `channel` of the Flow rooted at
    /// `flow_root` to the Bell Regions a Runner would create for it.
    fn at(flow_root: &Path, channel: u32) -> io::Result<Self> {
        let submission = flow_root.join(format!("missing-submission-{channel}.queue"));
        let completion = flow_root.join(format!("missing-completion-{channel}.queue"));
        let channel_region = bell_region(&channel_region_path(flow_root), FLOW_BELL_SLOTS)?;
        let source_region = bell_region(&source_region_path(&submission), SOURCE_BELL_SLOTS)?;
        Ok(Self {
            paths: FlowChannelQueuePaths::new(submission, completion),
            bells: FlowChannelBells::new(loop_bell(&channel_region, channel)?, source_region),
            channel_region,
        })
    }
}

struct SourceEndpoint {
    submission_path: std::path::PathBuf,
    completion_path: std::path::PathBuf,
    /// The Channel Region of the Flow this Source feeds.
    channel_region_path: std::path::PathBuf,
    /// The Bell Region the Channel loop on the other end of these Queues owns.
    channel_region: Arc<BellRegion>,
    /// The Channel's own doorbell, published into both Queue headers.
    channel_bell: Arc<LoopBell>,
    /// This Source loop's own doorbells, one per Queue endpoint it waits on.
    source_region: Arc<BellRegion>,
    submission: QueueWriter,
    completion: QueueReader,
}

impl SourceEndpoint {
    fn create(directory: &Path, index: usize) -> io::Result<Self> {
        Self::create_in_flow(directory, index, directory)
    }

    /// Creates one Source Instance of a Flow, writing its own files into
    /// `directory` while the Flow's Channel Region stays at `flow_root`.
    ///
    /// A Runner writes a replacement Source's Queues into a staged directory but
    /// keeps the Flow's Channel Region, so the Channels this Source feeds and the
    /// Channels its predecessor fed park on one mapping.
    fn create_in_flow(directory: &Path, index: usize, flow_root: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(directory)?;
        let submission_path = directory.join(format!("submission-{index}.queue"));
        let completion_path = directory.join(format!("completion-{index}.queue"));
        let max_pending = NonZeroU64::new(SOURCE_PENDING_LIMIT)
            .ok_or_else(|| io::Error::other("Source pending limit must be positive"))?;
        let max_record_size = NonZeroU64::new(SOURCE_RECORD_SIZE)
            .ok_or_else(|| io::Error::other("Source record size must be positive"))?;
        create_queue_file(
            &submission_path,
            submission_capacity(max_pending, max_record_size).map_err(io::Error::other)?,
            max_record_size,
        )
        .map_err(io::Error::other)?;
        create_queue_file(
            &completion_path,
            completion_capacity(max_pending).map_err(io::Error::other)?,
            NonZeroU64::new(COMPLETION_MAX_PAYLOAD_SIZE as u64)
                .ok_or_else(|| io::Error::other("Completion payload limit must be positive"))?,
        )
        .map_err(io::Error::other)?;
        let channel_region_path = channel_region_path(flow_root);
        let channel_region = bell_region(&channel_region_path, FLOW_BELL_SLOTS)?;
        let source_region = bell_region(&source_region_path(&submission_path), SOURCE_BELL_SLOTS)?;
        let channel_index =
            u32::try_from(index).map_err(|_| io::Error::other("Channel index overflowed"))?;
        let channel_bell = loop_bell(&channel_region, channel_index)?;
        let submission = QueueWriter::open(
            &submission_path,
            loop_bell(&source_region, 0)?,
            Arc::clone(&channel_region),
        )
        .map_err(io::Error::other)?;
        let completion = QueueReader::open(
            &completion_path,
            loop_bell(&source_region, 1)?,
            Arc::clone(&channel_region),
        )
        .map_err(io::Error::other)?;
        Ok(Self {
            submission_path,
            completion_path,
            channel_region_path,
            channel_region,
            channel_bell,
            source_region,
            submission,
            completion,
        })
    }

    fn queues(&self) -> TestChannelQueues {
        TestChannelQueues {
            paths: FlowChannelQueuePaths::new(
                self.submission_path.clone(),
                self.completion_path.clone(),
            ),
            bells: FlowChannelBells::new(
                Arc::clone(&self.channel_bell),
                Arc::clone(&self.source_region),
            ),
            channel_region: Arc::clone(&self.channel_region),
        }
    }

    fn submit(&mut self, record_id: u64, payload: Vec<u8>) -> io::Result<()> {
        self.submit_with_receipt(record_id, payload).map(drop)
    }

    fn submit_with_receipt(
        &mut self,
        record_id: u64,
        payload: Vec<u8>,
    ) -> io::Result<WriteReceipt> {
        self.submit_raw_with_receipt(
            &IngressRecord {
                record_id,
                payload: payload.into(),
            }
            .encode_to_vec(),
        )
    }

    fn submit_raw(&mut self, encoded: &[u8]) -> io::Result<()> {
        self.submit_raw_with_receipt(encoded).map(drop)
    }

    fn submit_raw_with_receipt(&mut self, encoded: &[u8]) -> io::Result<WriteReceipt> {
        match self
            .submission
            .try_write(encoded)
            .map_err(io::Error::other)?
        {
            WriteOutcome::Committed(receipt) => Ok(receipt),
            WriteOutcome::Full => Err(io::Error::other("Submission Queue was unexpectedly full")),
        }
    }

    fn wait_submission_release(&self, receipt: &WriteReceipt) -> io::Result<()> {
        let deadline = Instant::now() + WAIT_LIMIT;
        loop {
            if self
                .submission
                .is_released(receipt)
                .map_err(io::Error::other)?
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::other("Submission record was not released"));
            }
            thread::yield_now();
        }
    }

    fn wait_submission_reader_armed(&self) -> io::Result<()> {
        let deadline = Instant::now() + WAIT_LIMIT;
        loop {
            if queue_waiter_is_armed(
                &self.submission_path,
                &self.channel_region_path,
                QueueWaiter::Reader,
            )
            .map_err(io::Error::other)?
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::other("Submission reader did not arm its wait"));
            }
            thread::yield_now();
        }
    }

    fn corrupt_completion_release_position(&self) -> io::Result<()> {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&self.completion_path)?;
        file.write_all_at(&1_u64.to_le_bytes(), TEST_RELEASE_OFFSET)
    }

    fn try_completion(&mut self) -> io::Result<Option<IngressCompletion>> {
        let completion = self.try_completion_without_release()?;
        if completion.is_some() {
            self.completion.release(1).map_err(io::Error::other)?;
        }
        Ok(completion)
    }

    fn try_completion_without_release(&mut self) -> io::Result<Option<IngressCompletion>> {
        match self.completion.try_read().map_err(io::Error::other)? {
            ReadOutcome::Record(record) => IngressCompletion::decode(record.payload())
                .map(Some)
                .map_err(io::Error::other),
            ReadOutcome::Empty => Ok(None),
        }
    }

    fn wait_completion(&mut self) -> io::Result<IngressCompletion> {
        let deadline = Instant::now() + WAIT_LIMIT;
        loop {
            if let Some(completion) = self.try_completion()? {
                return Ok(completion);
            }
            if Instant::now() >= deadline {
                return Err(io::Error::other("Ingress completion did not arrive"));
            }
            thread::yield_now();
        }
    }
}

/// One Sink target of one Channel: the Instance that reads the Egress Queue and
/// the two Bell Regions the Queue pair must ring.
#[derive(Clone, Debug)]
struct EgressTarget {
    instance: PluginInstanceId,
    path: PathBuf,
    /// The Region of the loop that reads this Queue. The Channel rings the slot
    /// the Sink publishes here, so a Runner binds it when it prepares the Queue.
    sink_region: Arc<BellRegion>,
    /// The Region of the loop that writes this Queue. The Sink rings the slot
    /// the Channel publishes here. A Runner fills it while it prepares the
    /// Channel, which is why the fixture defers the reader that needs it.
    channel_region: Arc<OnceLock<Arc<BellRegion>>>,
}

impl EgressTarget {
    /// Records the Channel Region this Sink must ring, exactly as a Runner
    /// hands the fact to a Sink it launches.
    fn bind(&self, channel_region: &Arc<BellRegion>) {
        let _ = self.channel_region.set(Arc::clone(channel_region));
    }

    fn prepared(&self) -> PreparedEgressQueue {
        PreparedEgressQueue::Retained {
            path: self.path.clone(),
            peer_region: Arc::clone(&self.sink_region),
        }
    }
}

type EgressBinding = (SinkContractId, Vec<EgressTarget>);

struct EgressEndpoint {
    path: PathBuf,
    /// This Sink loop's own Bell Region, holding the one slot it parks on.
    sink_region: Arc<BellRegion>,
    /// The Channel Region this Sink rings when it releases, shared with the
    /// binding the Channel is prepared from.
    channel_region: Arc<OnceLock<Arc<BellRegion>>>,
    reader: Option<QueueReader>,
}

impl EgressEndpoint {
    fn create(
        directory: &Path,
        sink_id: &str,
        sink_contract_id: &SinkContractId,
    ) -> io::Result<(EgressBinding, Self)> {
        let capacity = DataCapacity::try_from(EGRESS_CAPACITY).map_err(io::Error::other)?;
        Self::create_with_capacity(directory, sink_id, sink_contract_id, capacity)
    }

    fn create_for_channel(
        directory: &Path,
        channel: usize,
        sink_id: &str,
        contract: &SinkContractId,
    ) -> io::Result<(EgressBinding, Self)> {
        let directory = directory.join(format!("channel-{channel}"));
        std::fs::create_dir_all(&directory)?;
        Self::create(&directory, sink_id, contract)
    }

    fn create_with_capacity(
        directory: &Path,
        sink_id: &str,
        sink_contract_id: &SinkContractId,
        capacity: DataCapacity,
    ) -> io::Result<(EgressBinding, Self)> {
        let path = directory.join(format!("egress-{sink_id}.queue"));
        let max_payload_size =
            NonZeroU64::new(capacity.get() - tenon_ipc::queue::FRAME_HEADER_LEN as u64)
                .ok_or_else(|| io::Error::other("Egress payload limit must be positive"))?;
        create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
        let sink_region = bell_region(&sink_region_path(&path), ONE_BELL_SLOT)?;
        let channel_region = Arc::new(OnceLock::new());
        let target = EgressTarget {
            instance: PluginInstanceId::try_from(sink_id).map_err(io::Error::other)?,
            path: path.clone(),
            sink_region: Arc::clone(&sink_region),
            channel_region: Arc::clone(&channel_region),
        };
        Ok((
            (sink_contract_id.clone(), vec![target]),
            Self {
                path,
                sink_region,
                channel_region,
                reader: None,
            },
        ))
    }

    /// Returns this Sink loop's doorbell, which the Channel rings when it
    /// commits an Egress record this Sink must read.
    fn own_bell(&self) -> io::Result<Arc<LoopBell>> {
        loop_bell(&self.sink_region, 0)
    }

    /// Opens one more reader of this Egress Queue, so a test can replay what the
    /// Queue still holds after the Sink stopped.
    fn replay_reader(&self) -> io::Result<QueueReader> {
        let channel_region = Arc::clone(self.channel_region.get().ok_or_else(|| {
            io::Error::other("Sink read its Egress Queue before its Channel was prepared")
        })?);
        QueueReader::open(&self.path, self.own_bell()?, channel_region).map_err(io::Error::other)
    }

    /// Opens the Egress reader on first use, once the Channel preparation has
    /// recorded the Region this Sink must ring. An unbound peer slot is not a
    /// lost ring, so publishing the slot after the Channel already committed is
    /// safe: the first read below sees the record.
    fn reader(&mut self) -> io::Result<&mut QueueReader> {
        if self.reader.is_none() {
            let channel_region = Arc::clone(self.channel_region.get().ok_or_else(|| {
                io::Error::other("Sink read its Egress Queue before its Channel was prepared")
            })?);
            let bell = self.own_bell()?;
            self.reader = Some(
                QueueReader::open(&self.path, bell, channel_region).map_err(io::Error::other)?,
            );
        }
        match &mut self.reader {
            Some(reader) => Ok(reader),
            None => Err(io::Error::other("Sink Egress reader is unavailable")),
        }
    }

    fn corrupt_release_position(&self) -> io::Result<()> {
        let file = std::fs::OpenOptions::new().write(true).open(&self.path)?;
        file.write_all_at(&1_u64.to_le_bytes(), TEST_RELEASE_OFFSET)
    }

    fn try_record(&mut self) -> io::Result<Option<EgressRecord>> {
        match self.reader()?.try_read().map_err(io::Error::other)? {
            ReadOutcome::Record(record) => EgressRecord::decode(record.payload())
                .map(Some)
                .map_err(io::Error::other),
            ReadOutcome::Empty => Ok(None),
        }
    }

    fn wait_record(&mut self) -> io::Result<EgressRecord> {
        let deadline = Instant::now() + WAIT_LIMIT;
        loop {
            if let Some(record) = self.try_record()? {
                return Ok(record);
            }
            if Instant::now() >= deadline {
                return Err(io::Error::other("Egress record did not arrive"));
            }
            thread::yield_now();
        }
    }

    fn release(&mut self, count: usize) -> io::Result<()> {
        self.reader()?.release(count).map_err(io::Error::other)
    }
}

struct TestFlowDefinitionReplacement {
    coordinator: super::flow_definition_replacement::FlowDefinitionReplacement,
    spec: FlowChannelSpec,
    routes: Vec<PreparedEgressRoutes>,
}

impl TestFlowDefinitionReplacement {
    fn prepare(self) -> Result<super::PreparedFlowDefinitionReplacement, PipelineRuntimeError> {
        self.coordinator
            .begin(ChannelDefinitionChange::Replace(self.spec), self.routes)?
            .wait()
    }
}

struct RunningRuntime {
    runtime: Option<PipelineRuntime>,
    executor: Runtime,
    initial_routes: Vec<Vec<EgressBinding>>,
}

impl RunningRuntime {
    fn start(spec: RuntimeInputs) -> io::Result<Self> {
        let initial_routes = spec
            .flows
            .get(&flow_id("main")?)
            .map(|flow| {
                flow.channels
                    .iter()
                    .map(|channel| {
                        channel
                            .routes
                            .iter()
                            .map(|(contract, targets)| {
                                (
                                    contract.clone(),
                                    targets
                                        .iter()
                                        .map(|(id, queue)| {
                                            let PreparedEgressQueue::Retained {
                                                path,
                                                peer_region,
                                            } = queue
                                            else {
                                                unreachable!("fixture uses existing paths")
                                            };
                                            EgressTarget {
                                                instance: id.clone(),
                                                path: path.clone(),
                                                sink_region: Arc::clone(peer_region),
                                                channel_region: Arc::new(OnceLock::new()),
                                            }
                                        })
                                        .collect(),
                                )
                            })
                            .collect()
                    })
                    .collect()
            })
            .unwrap_or_default();
        let runtime = start_runtime(spec, startup_control()).map_err(io::Error::other)?;
        Ok(Self {
            runtime: Some(runtime),
            initial_routes,
            executor: current_thread_executor()?,
        })
    }

    fn stop(mut self) -> io::Result<()> {
        self.stop_and_join()
    }

    fn replacement(
        &self,
        lua_source: &str,
        delivery: SourceDelivery,
    ) -> io::Result<TestFlowDefinitionReplacement> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
        let spec = channel_spec(
            lua_source,
            delivery,
            self.initial_routes.iter().flatten().map(|(id, _)| id),
        )?;
        let routes = self
            .initial_routes
            .clone()
            .into_iter()
            .map(|routes| prepared_routes(routes, None))
            .collect();
        let coordinator = runtime
            .flow_definition_replacement(&flow_id("main")?)
            .map_err(io::Error::other)?;
        Ok(TestFlowDefinitionReplacement {
            coordinator,
            spec,
            routes,
        })
    }

    fn replace_lua(&self, lua_source: &str) -> io::Result<()> {
        self.replacement(lua_source, SourceDelivery::AtLeastOnce)?
            .prepare()
            .and_then(|mut prepared| {
                prepared.cutover_in_place()?;
                prepared.into_paused().activate()
            })
            .map(|_| ())
            .map_err(io::Error::other)
    }

    fn wait_for_failure(mut self) -> io::Result<PipelineRuntimeError> {
        let mut runtime = self
            .runtime
            .take()
            .ok_or_else(|| io::Error::other("Pipeline runtime was already stopped"))?;
        self.executor
            .block_on(async {
                tokio::time::timeout(WAIT_LIMIT, runtime.wait_for_worker_exit()).await
            })
            .map_err(|_| io::Error::other("Pipeline worker did not exit"))?;
        let result = self.executor.block_on(runtime.stop_and_join());
        match result {
            Ok(()) => Err(io::Error::other("Pipeline runtime unexpectedly succeeded")),
            Err(error) => Ok(error),
        }
    }

    fn stop_and_join(&mut self) -> io::Result<()> {
        let Some(runtime) = self.runtime.take() else {
            return Ok(());
        };
        self.executor
            .block_on(runtime.stop_and_join())
            .map_err(io::Error::other)
    }
}

impl Drop for RunningRuntime {
    fn drop(&mut self) {
        if self.runtime.is_some() {
            let _ = self.stop_and_join();
        }
    }
}

mod flow_reconfiguration;
mod multi_flow;
mod planned_shutdown;
mod source_session_finish;

fn current_thread_executor() -> io::Result<Runtime> {
    RuntimeBuilder::new_current_thread().enable_time().build()
}
