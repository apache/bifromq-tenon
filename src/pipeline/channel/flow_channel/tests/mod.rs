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

use crate::pipeline::channel::metrics::ChannelMetrics;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io;
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prost::Message;
use prost_reflect::{DynamicMessage, ReflectMessage, Value as ProtobufValue};

use super::{
    ChannelWake, FlowChannel, FlowChannelControl, FlowChannelQueuePaths, FlowChannelSpec,
    FlowChannelStep, LuaReloadProgress, SettlementInterruption, TimerReadiness, timer_readiness_at,
};
use crate::config::ScriptVmLimits;
use crate::contracts::core::{
    ChannelDiagnosticKind, ChannelDiagnosticRecord, PipelineDiagnosticRecord,
    pipeline_diagnostic_record,
};
use crate::contracts::sink::EgressRecord;
use crate::contracts::source::{IngressCompletion, IngressCompletionStatus, IngressRecord};
use crate::identifiers::{PluginInstanceId, SinkContractId};
use crate::lua::TimerSchedule;
use crate::lua::tests::{lua_builder_contract, lua_source_contract};
use crate::pipeline::channel::FlowChannelBells;
use crate::pipeline::channel::flow_channel_command::{
    ChannelDefinitionChange, FlowChannelReplacementEvent,
};
use crate::pipeline::channel::{
    FlowChannelCommandControl, PreparedEgressQueue, PreparedEgressRoutes,
};
use crate::pipeline::diagnostics::ChannelDiagnosticPublisher;
use crate::pipeline::diagnostics::test_support::{
    channel_publisher as test_channel_publisher, interested_channel, uninterested_channel,
};
use crate::pipeline::ingress_queue::{
    COMPLETION_MAX_PAYLOAD_SIZE, completion_capacity, submission_capacity,
};
use crate::tenon_document::SourceDelivery;
use tenon_ipc::bell::{BellRegion, LoopBell, create_bell_region};
use tenon_ipc::queue::{
    DataCapacity, QueueReader, QueueWaiter, QueueWriter, ReadOutcome, WriteOutcome,
    create_queue_file, queue_waiter_is_armed,
};

const WAIT_LIMIT: Duration = Duration::from_secs(10);
const SOURCE_RECORD_LIMIT: u64 = 4;
const SOURCE_RECORD_SIZE: u64 = 512;
const EGRESS_CAPACITY: u64 = 1_024;
const TEST_MEMORY_LIMIT_BYTES: usize = 4 * 1024 * 1024;
const TEST_CPU_TIME_LIMIT: Duration = Duration::from_millis(100);

#[test]
fn channel_publishes_lua_print_with_runtime_identity() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let (diagnostics, mut records) = interested_channel(3);
    let spec = channel_spec(
        r#"
        print("initializing", 3)

        function main(event)
            print(event.payload.deviceId)
        end
        "#,
        SourceDelivery::AtMostOnce,
        [],
    )?;
    let (mut channel, _wake, _replacement) = open_channel(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        diagnostics,
        HashMap::new(),
        Arc::new(FlowChannelControl::new()),
        || false,
    )
    .map_err(channel_error)?;

    let initialized = channel_diagnostic(records.try_recv().map_err(io::Error::other)?)?;
    assert_eq!(initialized.channel_index, 3);
    assert_eq!(initialized.channel_instance_id, 1);
    assert_eq!(initialized.lua_vm_instance_id, 1);
    assert_eq!(initialized.sequence, 0);
    assert_eq!(initialized.text, "initializing\t3");

    source.submit(1, source_payload("device-1")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    let event = channel_diagnostic(records.try_recv().map_err(io::Error::other)?)?;
    assert_eq!(event.channel_index, 3);
    assert_eq!(event.channel_instance_id, 1);
    assert_eq!(event.lua_vm_instance_id, 1);
    assert_eq!(event.sequence, 1);
    assert_eq!(event.text, "device-1");
    Ok(())
}

#[test]
fn lua_main_failure_publishes_error_diagnostic_with_phase_and_code() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let (diagnostics, mut records) = interested_channel(3);
    let spec = channel_spec(
        r#"
        function main(event)
            error("qa-intentional-runtime-error")
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let (mut channel, _wake, _replacement) = open_channel(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        diagnostics,
        HashMap::new(),
        Arc::new(FlowChannelControl::new()),
        || false,
    )
    .map_err(channel_error)?;

    source.submit(1, source_payload("device-1")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        source.wait_completion()?,
        completion(1, IngressCompletionStatus::Error)
    );

    let failure = channel_diagnostic(records.try_recv().map_err(io::Error::other)?)?;
    assert_eq!(failure.channel_index, 3);
    assert_eq!(failure.channel_instance_id, 1);
    assert_eq!(failure.lua_vm_instance_id, 1);
    assert_eq!(failure.sequence, 0);
    assert_eq!(failure.kind, ChannelDiagnosticKind::Error as i32);
    assert_eq!(failure.phase, "lua_main");
    assert_eq!(failure.code, "process.lua_main_failed");
    assert_eq!(failure.text, "qa-intentional-runtime-error");
    assert!(!failure.truncated);
    assert!(!failure.invalid_utf8);
    assert!(records.try_recv().is_err());
    Ok(())
}

#[test]
fn source_decode_failure_publishes_error_diagnostic_with_phase_and_code() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let (diagnostics, mut records) = interested_channel(3);
    let spec = channel_spec(
        r#"
        function main(event)
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let (mut channel, _wake, _replacement) = open_channel(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        diagnostics,
        HashMap::new(),
        Arc::new(FlowChannelControl::new()),
        || false,
    )
    .map_err(channel_error)?;

    source.submit(1, malformed_source_payload())?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        source.wait_completion()?,
        completion(1, IngressCompletionStatus::Error)
    );

    let failure = channel_diagnostic(records.try_recv().map_err(io::Error::other)?)?;
    assert_eq!(failure.kind, ChannelDiagnosticKind::Error as i32);
    assert_eq!(failure.phase, "decode");
    assert_eq!(failure.code, "protobuf_invalid");
    assert_eq!(
        failure.text,
        "failed to decode Protobuf message: buffer underflow"
    );
    assert!(records.try_recv().is_err());
    Ok(())
}

#[test]
fn error_diagnostic_follows_the_current_vm_incarnation() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let (diagnostics, mut records) = interested_channel(3);
    let spec = channel_spec(
        r#"
        function main(event)
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let (mut channel, _wake, _replacement) = open_channel(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        diagnostics,
        HashMap::new(),
        Arc::new(FlowChannelControl::new()),
        || false,
    )
    .map_err(channel_error)?;

    source.submit(1, source_payload("hold")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    source.submit(2, malformed_source_payload())?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    let rebuilt = channel_diagnostic(records.try_recv().map_err(io::Error::other)?)?;
    assert_eq!(rebuilt.lua_vm_instance_id, 1);
    assert_eq!(rebuilt.sequence, 0);

    source.submit(3, malformed_source_payload())?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    let after_rebuild = channel_diagnostic(records.try_recv().map_err(io::Error::other)?)?;
    assert_eq!(after_rebuild.lua_vm_instance_id, 2);
    assert_eq!(after_rebuild.sequence, 1);
    assert_eq!(after_rebuild.phase, "decode");
    assert_eq!(after_rebuild.code, "protobuf_invalid");
    assert!(records.try_recv().is_err());
    Ok(())
}

#[test]
fn no_subscriber_means_no_error_diagnostic_but_the_same_completion() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let (diagnostics, mut records) = uninterested_channel(3);
    let spec = channel_spec(
        r#"
        function main(event)
            error("qa-intentional-runtime-error")
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let (mut channel, _wake, _replacement) = open_channel(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        diagnostics,
        HashMap::new(),
        Arc::new(FlowChannelControl::new()),
        || false,
    )
    .map_err(channel_error)?;

    source.submit(1, source_payload("device-1")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        source.wait_completion()?,
        completion(1, IngressCompletionStatus::Error)
    );
    assert!(records.try_recv().is_err());
    Ok(())
}

fn channel_diagnostic(record: PipelineDiagnosticRecord) -> io::Result<ChannelDiagnosticRecord> {
    match record.record {
        Some(pipeline_diagnostic_record::Record::Channel(record)) => Ok(record),
        Some(pipeline_diagnostic_record::Record::Plugin(_)) | None => {
            Err(io::Error::other("Expected one Channel diagnostic record"))
        }
    }
}

#[test]
fn first_payload_boundary_alone_owns_the_current_ingress_completion() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let sink_directory = tempfile::tempdir()?;
    let kafka_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let iotdb_id = sink_contract_id("com.example.iotdb@1.0.0")?;
    let mut kafka = RunningSink::start(
        sink_directory.path(),
        "kafka",
        Arc::clone(&source.channel_region),
    )?;
    let mut iotdb = RunningSink::start(
        sink_directory.path(),
        "iotdb",
        Arc::clone(&source.channel_region),
    )?;
    let spec = channel_spec(
        r#"
        local kafka = registry:getBuilder("com.example.kafka@1.0.0")
        local iotdb = registry:getBuilder("com.example.iotdb@1.0.0")

        function main(event)
            kafka:setLabel("kafka")
            emit(kafka:build())
            emit()
            iotdb:setLabel("iotdb")
            emit(iotdb:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&kafka_id, &iotdb_id],
    )?;
    let channel = RunningChannel::start(
        &source,
        spec,
        HashMap::from([
            (kafka_id.clone(), kafka.route()?),
            (iotdb_id.clone(), iotdb.route()?),
        ]),
    )?;
    source.submit(41, source_payload("device-41")?)?;

    assert_eq!(
        kafka.read_record()?.payload,
        expected_sink_payload("kafka")?
    );
    // Only the first payload boundary owns the Source record, so the later
    // boundaries of the same Lua call commit without waiting for its release.
    assert_eq!(
        iotdb.read_record()?.payload,
        expected_sink_payload("iotdb")?
    );
    assert!(source.try_completion()?.is_none());

    kafka.release(1)?;
    assert_eq!(
        source.wait_completion()?,
        completion(41, IngressCompletionStatus::Ok)
    );
    iotdb.release(1)?;
    channel.stop()
}

#[test]
fn completion_only_boundary_does_not_wait_for_a_later_payload_release() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let sink_directory = tempfile::tempdir()?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let mut sink = RunningSink::start(
        sink_directory.path(),
        "kafka",
        Arc::clone(&source.channel_region),
    )?;
    let spec = channel_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            emit()
            builder:setLabel("later-output")
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
    )?;
    let processing = run_one_source(
        &source,
        spec,
        HashMap::from([(sink_contract_id, sink.route()?)]),
    );
    source.submit(42, source_payload("device-42")?)?;

    assert_eq!(
        source.wait_completion()?,
        completion(42, IngressCompletionStatus::Ok)
    );
    assert_eq!(
        sink.read_record()?.payload,
        expected_sink_payload("later-output")?
    );
    assert_eq!(processing.finish()?, FlowChannelStep::EventProcessed);
    sink.release(1)?;
    Ok(())
}

#[test]
fn lua_failure_retries_older_pending_record_and_errors_current_record_in_order() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let spec = channel_spec(
        r#"
        function main(event)
            if event.payload.deviceId == "bad" then
                error("bad input")
            end
            if event.payload.deviceId == "flush" then
                emit()
            end
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let (mut channel, _wake, _replacement) = open_channel(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        test_channel_publisher(0),
        HashMap::new(),
        Arc::new(FlowChannelControl::new()),
        || false,
    )
    .map_err(channel_error)?;

    source.submit(1, source_payload("hold")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert!(source.try_completion()?.is_none());

    source.submit(2, source_payload("bad")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        source.wait_completion()?,
        completion(1, IngressCompletionStatus::Retry)
    );
    assert_eq!(
        source.wait_completion()?,
        completion(2, IngressCompletionStatus::Error)
    );

    source.submit(3, source_payload("flush")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        source.wait_completion()?,
        completion(3, IngressCompletionStatus::Ok)
    );
    Ok(())
}

#[test]
fn source_decode_failure_without_older_pending_preserves_lua_state() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let spec = channel_spec(
        r#"
        setTimeout(0)
        local count = 0

        function main(event)
            count = count + 1
            if event.payload.deviceId == "before" then
                clearTimeout()
            elseif event.payload.deviceId == "after" then
                if count ~= 2 or hasTimeout() then
                    error("Lua state was unexpectedly rebuilt")
                end
            end
            emit()
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let (mut channel, _wake, _replacement) = open_channel(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        test_channel_publisher(0),
        HashMap::new(),
        Arc::new(FlowChannelControl::new()),
        || false,
    )
    .map_err(channel_error)?;

    source.submit(21, source_payload("before")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        source.wait_completion()?,
        completion(21, IngressCompletionStatus::Ok)
    );

    source.submit(22, malformed_source_payload())?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        source.wait_completion()?,
        completion(22, IngressCompletionStatus::Error)
    );

    source.submit(23, source_payload("after")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        source.wait_completion()?,
        completion(23, IngressCompletionStatus::Ok)
    );
    Ok(())
}

#[test]
fn source_decode_failure_with_older_pending_rebuilds_lua_state() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let spec = channel_spec(
        r#"
        local count = 0

        function main(event)
            count = count + 1
            if event.payload.deviceId == "hold" then
                return
            end
            if event.payload.deviceId == "after" and count ~= 1 then
                error("Lua state was not rebuilt")
            end
            emit()
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let (mut channel, _wake, _replacement) = open_channel(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        test_channel_publisher(0),
        HashMap::new(),
        Arc::new(FlowChannelControl::new()),
        || false,
    )
    .map_err(channel_error)?;

    source.submit(31, source_payload("hold")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert!(source.try_completion()?.is_none());

    source.submit(32, malformed_source_payload())?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        source.wait_completion()?,
        completion(31, IngressCompletionStatus::Retry)
    );
    assert_eq!(
        source.wait_completion()?,
        completion(32, IngressCompletionStatus::Error)
    );

    source.submit(33, source_payload("after")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    assert_eq!(
        source.wait_completion()?,
        completion(33, IngressCompletionStatus::Ok)
    );
    Ok(())
}

#[test]
fn completion_boundary_completes_every_accumulated_record_in_input_order() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let spec = channel_spec(
        r#"
        function main(event)
            if event.payload.deviceId == "flush" then
                emit()
            end
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let (mut channel, _wake, _replacement) = open_channel(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        test_channel_publisher(0),
        HashMap::new(),
        Arc::new(FlowChannelControl::new()),
        || false,
    )
    .map_err(channel_error)?;

    source.submit(11, source_payload("hold")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );
    source.submit(12, source_payload("flush")?)?;
    assert_eq!(
        process_one_source(&mut channel)?,
        FlowChannelStep::EventProcessed
    );

    assert_eq!(
        source.wait_completion()?,
        completion(11, IngressCompletionStatus::Ok)
    );
    assert_eq!(
        source.wait_completion()?,
        completion(12, IngressCompletionStatus::Ok)
    );
    Ok(())
}

#[test]
fn at_most_once_completion_precedes_and_survives_lua_failure() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let spec = channel_spec(
        r#"
        function main(event)
            error("processing failed")
        end
        "#,
        SourceDelivery::AtMostOnce,
        [],
    )?;
    let (mut channel, _wake, _replacement) = open_channel(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        test_channel_publisher(0),
        HashMap::new(),
        Arc::new(FlowChannelControl::new()),
        || false,
    )
    .map_err(channel_error)?;

    for record_id in [7, 8] {
        source.submit(record_id, source_payload("device")?)?;
        assert_eq!(
            process_one_source(&mut channel)?,
            FlowChannelStep::EventProcessed
        );
        assert_eq!(
            source.wait_completion()?,
            completion(record_id, IngressCompletionStatus::Ok)
        );
    }
    Ok(())
}

#[test]
fn planned_stop_interrupts_an_empty_submission_wait() -> io::Result<()> {
    let source = SourceQueueFixture::new()?;
    let spec = channel_spec("function main(event) end", SourceDelivery::AtLeastOnce, [])?;
    let submission_path = source.submission_path.clone();
    let submission_probe_path = submission_path.clone();
    let channel_region_path = source.channel_region_path.clone();
    let bells = source.bells()?;
    let completion_path = source.completion_path.clone();
    let control = Arc::new(FlowChannelControl::new());
    let worker_control = Arc::clone(&control);
    let (wake_sender, wake_receiver) = mpsc::sync_channel(1);
    let (result_sender, result_receiver) = mpsc::sync_channel(1);
    let worker = thread::spawn(move || {
        let result = (|| -> Result<(), String> {
            let (channel, wake, _replacement) = open_channel(
                FlowChannelQueuePaths::new(submission_path, completion_path),
                bells,
                Instant::now(),
                spec,
                test_channel_publisher(0),
                HashMap::new(),
                worker_control,
                || false,
            )
            .map_err(|error| error.to_string())?;
            let _ = wake_sender.send(wake);
            channel.run().map_err(|error| error.to_string())
        })();
        let _ = result_sender.send(result);
    });
    let wake = wake_receiver
        .recv_timeout(WAIT_LIMIT)
        .map_err(|_| io::Error::other("Flow channel did not publish its Queue wake handle"))?;

    // An empty Submission Queue must leave the Channel parked on its own
    // doorbell, so that the planned stop below is what ends the wait.
    wait_until_armed(
        &submission_probe_path,
        &channel_region_path,
        QueueWaiter::Reader,
    )?;
    control.request_stop();
    wake.wake().map_err(io::Error::other)?;

    result_receiver
        .recv_timeout(WAIT_LIMIT)
        .map_err(|_| io::Error::other("Flow Channel did not stop"))?
        .map_err(io::Error::other)?;
    worker
        .join()
        .map_err(|_| io::Error::other("Flow Channel thread panicked"))?;
    Ok(())
}

#[test]
fn source_replacement_drain_retries_unemitted_input_without_running_old_timer() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    source.submit(50, source_payload("drain")?)?;
    let spec = channel_spec(
        r#"
        setTimeout(0)

        function main(event)
            if event.type == "timer" then
                error("Old timer ran during Source drain")
            end
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let control = Arc::new(FlowChannelControl::new());
    let (channel, wake, _replacement) = open_channel(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        test_channel_publisher(0),
        HashMap::new(),
        Arc::clone(&control),
        || false,
    )
    .map_err(channel_error)?;
    control.request_drain();
    wake.wake().map_err(io::Error::other)?;

    channel.run().map_err(channel_error)?;
    assert_eq!(
        source.wait_completion()?,
        completion(50, IngressCompletionStatus::Retry)
    );
    Ok(())
}

#[test]
#[cfg(not(feature = "loom-model"))]
fn source_session_finish_keeps_the_original_timer_and_lua_state_after_interruption()
-> io::Result<()> {
    for delivery in [SourceDelivery::AtLeastOnce, SourceDelivery::AtMostOnce] {
        let mut source = SourceQueueFixture::new()?;
        let (diagnostics, mut records) = interested_channel(0);
        let spec = channel_spec(
            r#"
            local count = 0
            function main(event)
                if event.type == "timer" then
                    print("timer", count, hasTimeout())
                    emit()
                else
                    count = count + 1
                    setTimeout(0)
                end
            end
            "#,
            delivery,
            [],
        )?;
        let control = Arc::new(FlowChannelControl::new());
        let (mut channel, wake, _commands) = open_channel(
            source.queue_paths(),
            source.bells()?,
            Instant::now(),
            spec,
            diagnostics,
            HashMap::new(),
            Arc::clone(&control),
            || false,
        )
        .map_err(channel_error)?;
        source.submit(71, source_payload("pending")?)?;
        assert_eq!(
            process_one_source(&mut channel)?,
            FlowChannelStep::EventProcessed
        );
        let original_timer = channel
            .lua_vm()
            .map_err(channel_error)?
            .next_timer_event()
            .map(|timer| timer.schedule);
        assert!(original_timer.is_some());
        // A non-stop interruption must only recheck the same Completion prefix.
        wake.wake().map_err(io::Error::other)?;
        let (progress, result) = thread::scope(|scope| {
            let consumer = scope.spawn(|| {
                let consumed = (|| {
                    wait_until_armed(
                        &source.completion_path,
                        &source.channel_region_path,
                        QueueWaiter::Writer,
                    )?;
                    source.wait_completion()
                })();
                if consumed.is_err() {
                    control.request_stop();
                    wake.wake().map_err(io::Error::other)?;
                }
                consumed
            });
            let progress = channel.finish_source_session().map_err(channel_error);
            (
                progress,
                consumer
                    .join()
                    .map_err(|_| io::Error::other("Completion consumer panicked")),
            )
        });
        assert_eq!(progress?, super::CompletionProgress::Completed);
        let expected = match delivery {
            SourceDelivery::AtLeastOnce => IngressCompletionStatus::Retry,
            SourceDelivery::AtMostOnce => IngressCompletionStatus::Ok,
        };
        assert_eq!(result??, completion(71, expected));
        assert_eq!(
            channel
                .lua_vm()
                .map_err(channel_error)?
                .next_timer_event()
                .map(|timer| timer.schedule),
            original_timer
        );
        assert_eq!(
            channel.process_timer().map_err(channel_error)?,
            FlowChannelStep::EventProcessed
        );
        let record = channel_diagnostic(records.try_recv().map_err(io::Error::other)?)?;
        assert_eq!(record.text, "timer\t1\tfalse");
        assert_eq!(record.lua_vm_instance_id, 1);
        assert!(
            channel
                .lua_vm()
                .map_err(channel_error)?
                .next_timer_event()
                .map(|timer| timer.schedule)
                .is_none()
        );
        assert!(source.try_completion()?.is_none());
    }
    Ok(())
}

#[test]
fn source_replacement_drain_processes_every_committed_at_most_once_input() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let sink_directory = tempfile::tempdir()?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let mut sink = RunningSink::start(
        sink_directory.path(),
        "kafka",
        Arc::clone(&source.channel_region),
    )?;
    let spec = channel_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel(event.payload.deviceId)
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtMostOnce,
        [&sink_contract_id],
    )?;
    source.submit(51, source_payload("first")?)?;
    source.submit(52, source_payload("second")?)?;
    let mut channel = RunningChannel::start(
        &source,
        spec,
        HashMap::from([(sink_contract_id, sink.route()?)]),
    )?;
    channel.control.request_drain();
    channel
        .wake
        .as_ref()
        .ok_or_else(|| io::Error::other("Channel wake is missing"))?
        .wake()
        .map_err(io::Error::other)?;
    assert_eq!(
        source.wait_completion()?,
        completion(51, IngressCompletionStatus::Ok)
    );
    assert_eq!(
        source.wait_completion()?,
        completion(52, IngressCompletionStatus::Ok)
    );
    assert_eq!(sink.read_record()?.payload, expected_sink_payload("first")?);
    assert_eq!(
        sink.read_record()?.payload,
        expected_sink_payload("second")?
    );
    assert!(matches!(
        channel.result.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    sink.release(2)?;
    channel
        .result
        .recv_timeout(WAIT_LIMIT)
        .map_err(io::Error::other)?
        .map_err(io::Error::other)?;
    channel
        .thread
        .take()
        .ok_or_else(|| io::Error::other("Channel thread is missing"))?
        .join()
        .map_err(|_| io::Error::other("Channel thread panicked"))?;
    Ok(())
}

#[test]
fn zero_delay_top_level_timer_runs_before_the_first_source_event() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    source.submit(51, source_payload("first")?)?;
    let spec = channel_spec(
        r#"
        local timerRan = false
        setTimeout(0)

        function main(event)
            if event.type == "timer" then
                timerRan = true
                return
            end
            if not timerRan then
                error("Top-level timer did not run first")
            end
            emit()
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let channel = RunningChannel::start(&source, spec, HashMap::new())?;

    assert_eq!(
        source.wait_completion()?,
        completion(51, IngressCompletionStatus::Ok)
    );
    channel.stop()
}

#[test]
fn ready_source_runs_before_a_future_timer() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    source.submit(52, source_payload("ready")?)?;
    let spec = channel_spec(
        r#"
        setTimeout(60000)

        function main(event)
            if event.type == "timer" then
                error("Future timer ran early")
            end
            clearTimeout()
            emit()
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let channel = RunningChannel::start(&source, spec, HashMap::new())?;

    assert_eq!(
        source.wait_completion()?,
        completion(52, IngressCompletionStatus::Ok)
    );
    channel.stop()
}

#[test]
fn timer_source_ties_and_vm_transitions_preserve_event_order() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    source.submit(53, source_payload("ready")?)?;
    let (diagnostics, _records) = interested_channel(0);
    let spec = channel_spec(
        "setTimeout(0); setTimeout(0); function main(event) end",
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let (mut channel, _wake, commands) = open_channel(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        diagnostics,
        HashMap::new(),
        Arc::new(FlowChannelControl::new()),
        || false,
    )
    .map_err(channel_error)?;
    let timer = channel
        .lua_vm()
        .map_err(channel_error)?
        .next_timer_event()
        .ok_or_else(|| io::Error::other("timer missing"))?;
    for (order, expected) in [
        (timer.sequence - 1, super::ChannelWork::SourceRecord),
        (timer.sequence + 1, super::ChannelWork::TimerDue),
    ] {
        channel.source_readiness.set(Some(super::SourceReadiness {
            observed_at: timer.deadline,
            order,
            generation: channel.vm_generation.get(),
        }));
        assert_eq!(
            channel.pending_work().map_err(channel_error)?,
            Some(expected)
        );
    }
    channel.reload_lua_vm().map_err(channel_error)?;
    assert!(channel.source_readiness.get().is_none());
    let replacement = channel
        .lua_vm()
        .map_err(channel_error)?
        .next_timer_event()
        .ok_or_else(|| io::Error::other("replacement timer missing"))?;
    assert!(replacement.sequence > timer.sequence);
    assert_eq!(
        channel.pending_work().map_err(channel_error)?,
        Some(super::ChannelWork::TimerDue)
    );
    let old_readiness = channel.source_readiness.get();
    assert!(old_readiness.is_some());
    let (events, observations) = mpsc::sync_channel(8);
    let ticket = commands
        .begin_replacement(
            0,
            ChannelDefinitionChange::Replace(channel.spec.clone()),
            HashMap::new(),
            events,
        )
        .map_err(io::Error::other)?;
    let Some(super::ChannelCommand::Replace(request)) = channel.commands.try_take() else {
        return Err(io::Error::other("replacement request missing"));
    };
    channel
        .prepare_replacement(request)
        .map_err(channel_error)?;
    assert!(matches!(
        observations
            .recv_timeout(WAIT_LIMIT)
            .map_err(io::Error::other)?,
        FlowChannelReplacementEvent::Prepared { .. }
    ));
    assert_eq!(channel.source_readiness.get(), old_readiness);
    ticket.cutover().map_err(io::Error::other)?;
    thread::scope(|scope| -> io::Result<()> {
        let activation = scope.spawn(move || -> io::Result<()> {
            assert!(matches!(
                observations
                    .recv_timeout(WAIT_LIMIT)
                    .map_err(io::Error::other)?,
                FlowChannelReplacementEvent::CutoverComplete { .. }
            ));
            ticket.activate().map_err(io::Error::other)
        });
        channel.advance_replacement().map_err(channel_error)?;
        activation
            .join()
            .map_err(|_| io::Error::other("activation thread panicked"))??;
        Ok(())
    })?;
    assert!(channel.source_readiness.get().is_none());
    assert_eq!(
        channel.pending_work().map_err(channel_error)?,
        Some(super::ChannelWork::TimerDue)
    );
    assert!(channel.source_readiness.get().is_some());
    channel.control.request_drain();
    assert_eq!(
        channel.drain_source_records().map_err(channel_error)?,
        super::CompletionProgress::Completed
    );
    assert!(channel.source_readiness.get().is_none());
    assert_eq!(
        source.wait_completion()?,
        completion(53, IngressCompletionStatus::Retry)
    );
    Ok(())
}

#[test]
fn recurring_zero_delay_timer_yields_to_continuously_ready_source() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    for record_id in 53..=55 {
        source.submit(record_id, source_payload("ready")?)?;
    }
    let spec = channel_spec(
        r#"
        local sourceCount = 0
        setTimeout(0)

        function main(event)
            if event.type == "timer" then
                if sourceCount < 3 then
                    setTimeout(0)
                end
                return
            end
            sourceCount = sourceCount + 1
            emit()
            if sourceCount >= 3 then
                clearTimeout()
            end
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let channel = RunningChannel::start(&source, spec, HashMap::new())?;

    for record_id in 53..=55 {
        assert_eq!(
            source.wait_completion()?,
            completion(record_id, IngressCompletionStatus::Ok)
        );
    }
    channel.stop()
}

#[test]
fn recurring_timer_emits_wall_clock_payloads_without_source_input() -> io::Result<()> {
    let source = SourceQueueFixture::new()?;
    let sink_directory = tempfile::tempdir()?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let mut sink = RunningSink::start(
        sink_directory.path(),
        "kafka",
        Arc::clone(&source.channel_region),
    )?;
    let spec = channel_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")
        local count = 0
        setTimeout(0)
        function main(event)
            assert(event.type == "timer")
            count = count + 1
            local millis = currentTimeMillis()
            builder:setLabel(json.encode({
                count = count,
                millis = millis,
                seconds = os.time(),
                utc = os.date("!%Y-%m-%dT%H:%M:%SZ", millis // 1000)
            }))
            emit(builder:build())
            if count < 2 then setTimeout(0) end
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
    )?;
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    let channel = RunningChannel::start(
        &source,
        spec,
        HashMap::from([(sink_contract_id, sink.route()?)]),
    )?;
    for count in 1..=2 {
        let record = sink.read_record()?;
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?;
        let payload = DynamicMessage::decode(lua_builder_contract()?, &record.payload[..])
            .map_err(io::Error::other)?;
        let label = payload
            .get_field_by_name("label")
            .ok_or_else(|| io::Error::other("Sink label is missing"))?;
        let ProtobufValue::String(label) = label.as_ref() else {
            return Err(io::Error::other("Sink label is not a string"));
        };
        let value: serde_json::Value = serde_json::from_str(label)?;
        let millis = value["millis"]
            .as_u64()
            .ok_or_else(|| io::Error::other("Sink timestamp is not an integer"))?;
        let seconds = value["seconds"]
            .as_u64()
            .ok_or_else(|| io::Error::other("Sink seconds are not an integer"))?;
        assert_eq!(value["count"], count);
        assert!((before.as_millis()..=after.as_millis()).contains(&u128::from(millis)));
        assert!((before.as_secs()..=after.as_secs()).contains(&seconds));
        let utc = value["utc"]
            .as_str()
            .ok_or_else(|| io::Error::other("Sink UTC date is not a string"))?;
        assert_eq!(utc.len(), 20);
        assert!(utc.ends_with('Z'));
        sink.release(1)?;
    }
    channel.stop()
}

#[test]
fn timer_payload_waits_for_sink_release_before_completing_pending_source() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let sink_directory = tempfile::tempdir()?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let mut sink = RunningSink::start(
        sink_directory.path(),
        "kafka",
        Arc::clone(&source.channel_region),
    )?;
    let spec = channel_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            if event.type == "source" then
                builder:setLabel("timer-output")
                setTimeout(0)
                return
            end
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
    )?;
    let channel = RunningChannel::start(
        &source,
        spec,
        HashMap::from([(sink_contract_id, sink.route()?)]),
    )?;

    source.submit(53, source_payload("pending")?)?;
    assert_eq!(
        sink.read_record()?.payload,
        expected_sink_payload("timer-output")?
    );
    assert!(source.try_completion()?.is_none());

    sink.release(1)?;
    assert_eq!(
        source.wait_completion()?,
        completion(53, IngressCompletionStatus::Ok)
    );
    channel.stop()?;
    Ok(())
}

#[test]
fn unreleased_output_does_not_stop_the_channel_from_accepting_more_input() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let sink_directory = tempfile::tempdir()?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let mut sink = RunningSink::start(
        sink_directory.path(),
        "kafka",
        Arc::clone(&source.channel_region),
    )?;
    let spec = channel_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel(event.payload.deviceId)
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
    )?;
    let channel = RunningChannel::start(
        &source,
        spec,
        HashMap::from([(sink_contract_id, sink.route()?)]),
    )?;

    source.submit(71, source_payload("first")?)?;
    source.submit(72, source_payload("second")?)?;
    assert_eq!(sink.read_record()?.payload, expected_sink_payload("first")?);
    // The Sink has not released anything yet, and the Channel already accepted,
    // ran Lua for, and committed the next Source record.
    assert_eq!(
        sink.read_record()?.payload,
        expected_sink_payload("second")?
    );
    assert!(source.try_completion()?.is_none());

    sink.release(2)?;
    assert_eq!(
        source.wait_completion()?,
        completion(71, IngressCompletionStatus::Ok)
    );
    assert_eq!(
        source.wait_completion()?,
        completion(72, IngressCompletionStatus::Ok)
    );
    channel.stop()
}

#[test]
fn a_departed_sink_ends_the_release_wait_by_retrying_its_records() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let departing_directory = tempfile::tempdir()?;
    let successor_directory = tempfile::tempdir()?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let departing = PluginInstanceId::try_from("kafka").map_err(io::Error::other)?;
    let successor = PluginInstanceId::try_from("successor").map_err(io::Error::other)?;
    let mut departing_sink = RunningSink::start(
        departing_directory.path(),
        "kafka",
        Arc::clone(&source.channel_region),
    )?;
    let successor_sink = RunningSink::start(
        successor_directory.path(),
        "successor",
        Arc::clone(&source.channel_region),
    )?;
    let spec = channel_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel("departing")
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
    )?;
    let channel = RunningChannel::start(
        &source,
        spec.clone(),
        HashMap::from([(sink_contract_id.clone(), departing_sink.route()?)]),
    )?;

    source.submit(81, source_payload("departing")?)?;
    assert_eq!(
        departing_sink.read_record()?.payload,
        expected_sink_payload("departing")?
    );
    assert!(source.try_completion()?.is_none());

    // The Sink left the Pipeline while it still owed this release, so the
    // release can never arrive and the accepted boundary must stop waiting.
    channel
        .wake
        .as_ref()
        .ok_or_else(|| io::Error::other("Running Channel has no wake registration"))?
        .depart_egress_targets(&BTreeSet::from([departing]))
        .map_err(io::Error::other)?;

    // A cut-over settles the old route, so it is where a departed target must
    // resolve instead of blocking the Channel forever.
    let (events, prepared) = mpsc::sync_channel(8);
    let ticket = channel
        .commands
        .begin_replacement(
            0,
            ChannelDefinitionChange::Replace(spec),
            HashMap::from([(
                sink_contract_id,
                BTreeMap::from([(
                    successor,
                    PreparedEgressQueue::New(successor_sink.writer()?),
                )]),
            )]),
            events,
        )
        .map_err(io::Error::other)?;
    assert!(matches!(
        prepared
            .recv_timeout(WAIT_LIMIT)
            .map_err(io::Error::other)?,
        FlowChannelReplacementEvent::Prepared { .. }
    ));
    ticket.cutover().map_err(io::Error::other)?;

    assert_eq!(
        source.wait_completion()?,
        completion(81, IngressCompletionStatus::Retry)
    );
    ticket.activate().map_err(io::Error::other)?;
    channel.stop()
}

#[test]
fn a_readded_sink_identity_releases_its_records_again() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let first_directory = tempfile::tempdir()?;
    let readded_directory = tempfile::tempdir()?;
    let successor_directory = tempfile::tempdir()?;
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let kafka = PluginInstanceId::try_from("kafka").map_err(io::Error::other)?;
    let successor = PluginInstanceId::try_from("successor").map_err(io::Error::other)?;
    let mut leaving = RunningSink::start(
        first_directory.path(),
        "kafka",
        Arc::clone(&source.channel_region),
    )?;
    let mut readded = RunningSink::start(
        readded_directory.path(),
        "kafka",
        Arc::clone(&source.channel_region),
    )?;
    let successor_sink = RunningSink::start(
        successor_directory.path(),
        "successor",
        Arc::clone(&source.channel_region),
    )?;
    let spec = channel_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel(event.payload.deviceId)
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
    )?;
    let channel = RunningChannel::start(
        &source,
        spec.clone(),
        HashMap::from([(sink_contract_id.clone(), leaving.route()?)]),
    )?;

    source.submit(91, source_payload("first")?)?;
    assert_eq!(
        leaving.read_record()?.payload,
        expected_sink_payload("first")?
    );
    assert!(source.try_completion()?.is_none());

    // The identity leaves the Pipeline while it still owes this release.
    channel
        .wake
        .as_ref()
        .ok_or_else(|| io::Error::other("Running Channel has no wake registration"))?
        .depart_egress_targets(&BTreeSet::from([kafka.clone()]))
        .map_err(io::Error::other)?;

    // The next generation binds the same identity again to a live Sink. The
    // departure belonged to the generation that observed it, so the boundary
    // this cut-over settles is still owed by the old, gone Sink.
    let (events, prepared) = mpsc::sync_channel(8);
    let ticket = channel
        .commands
        .begin_replacement(
            0,
            ChannelDefinitionChange::Replace(spec.clone()),
            HashMap::from([(
                sink_contract_id.clone(),
                BTreeMap::from([(kafka.clone(), PreparedEgressQueue::New(readded.writer()?))]),
            )]),
            events,
        )
        .map_err(io::Error::other)?;
    assert!(matches!(
        prepared
            .recv_timeout(WAIT_LIMIT)
            .map_err(io::Error::other)?,
        FlowChannelReplacementEvent::Prepared { .. }
    ));
    ticket.cutover().map_err(io::Error::other)?;
    assert_eq!(
        source.wait_completion()?,
        completion(91, IngressCompletionStatus::Retry)
    );
    ticket.activate().map_err(io::Error::other)?;

    // The re-added identity owes its own releases, so a cut-over that settles
    // its boundary must wait for the live Sink instead of reading it as gone.
    source.submit(92, source_payload("second")?)?;
    assert_eq!(
        readded.read_record()?.payload,
        expected_sink_payload("second")?
    );
    let releaser = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        let result = readded.release(1);
        (readded, result)
    });

    let (events, prepared) = mpsc::sync_channel(8);
    let ticket = channel
        .commands
        .begin_replacement(
            0,
            ChannelDefinitionChange::Replace(spec),
            HashMap::from([(
                sink_contract_id,
                BTreeMap::from([(
                    successor,
                    PreparedEgressQueue::New(successor_sink.writer()?),
                )]),
            )]),
            events,
        )
        .map_err(io::Error::other)?;
    assert!(matches!(
        prepared
            .recv_timeout(WAIT_LIMIT)
            .map_err(io::Error::other)?,
        FlowChannelReplacementEvent::Prepared { .. }
    ));
    ticket.cutover().map_err(io::Error::other)?;
    assert_eq!(
        source.wait_completion()?,
        completion(92, IngressCompletionStatus::Ok)
    );
    ticket.activate().map_err(io::Error::other)?;
    let (_readded, released) = releaser
        .join()
        .map_err(|_| io::Error::other("Sink releaser panicked"))?;
    released?;
    channel.stop()
}

/// One Channel with a single accepted boundary its Sink has not released.
///
/// The Channel loop owns the thread it runs on because the Lua VM is thread
/// affine. Everything else stays on the test thread, so a test can watch the
/// parked loop from outside and then release the boundary itself.
struct UnsettledBoundary {
    source: SourceQueueFixture,
    sink: RunningSink,
    _sink_directory: tempfile::TempDir,
    control: Arc<FlowChannelControl>,
    wake: ChannelWake,
    bell: Arc<LoopBell>,
    outcome: Receiver<Result<(), String>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl UnsettledBoundary {
    /// Waits for the Channel thread to stop and reports why it stopped.
    fn join(&mut self) -> io::Result<Result<(), String>> {
        let outcome = self
            .outcome
            .recv_timeout(WAIT_LIMIT)
            .map_err(|_| io::Error::other("Flow Channel did not stop"))?;
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| io::Error::other("Flow Channel thread panicked"))?;
        }
        Ok(outcome)
    }
}

/// Starts one Channel on its own thread over `source` and runs `drive` on it there.
///
/// The caller commits the Source record this scenario needs before calling, and
/// deliberately leaves the Sink unreleased: the Channel accepts the boundary and
/// then parks, which is what the test observes through `bell`. `drive` runs on
/// the Channel's thread and must return only after the Channel stopped.
fn unsettled_boundary_channel<F>(
    source: SourceQueueFixture,
    sink: RunningSink,
    sink_directory: tempfile::TempDir,
    drive: F,
) -> io::Result<UnsettledBoundary>
where
    F: FnOnce(FlowChannel) -> Result<(), String> + Send + 'static,
{
    let sink_contract_id = sink_contract_id("com.example.kafka@1.0.0")?;
    let spec = channel_spec(
        r#"
        local builder = registry:getBuilder("com.example.kafka@1.0.0")

        function main(event)
            builder:setLabel(event.payload.deviceId)
            emit(builder:build())
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [&sink_contract_id],
    )?;
    let bell = loop_bell(&source.channel_region, 0)?;
    let control = Arc::new(FlowChannelControl::new());
    let worker_control = Arc::clone(&control);
    let queues = source.queue_paths();
    let bells = FlowChannelBells::new(Arc::clone(&bell), Arc::clone(&source.source_region));
    let routes = HashMap::from([(sink_contract_id, sink.route()?)]);
    let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
    let (outcome_sender, outcome) = mpsc::sync_channel(1);
    let thread = thread::spawn(move || {
        let opened = open_channel(
            queues,
            bells,
            Instant::now(),
            spec,
            test_channel_publisher(0),
            routes,
            worker_control,
            || false,
        );
        let (channel, wake, _commands) = match opened {
            Ok(opened) => opened,
            Err(error) => {
                let _ = startup_sender.send(Err(error.to_string()));
                return;
            }
        };
        if startup_sender.send(Ok(wake)).is_err() {
            return;
        }
        let _ = outcome_sender.send(drive(channel));
    });
    let wake = match startup_receiver.recv_timeout(WAIT_LIMIT) {
        Ok(Ok(wake)) => wake,
        Ok(Err(error)) => {
            thread
                .join()
                .map_err(|_| io::Error::other("Flow Channel thread panicked"))?;
            return Err(io::Error::other(error));
        }
        Err(_) => return Err(io::Error::other("Flow Channel did not finish startup")),
    };
    Ok(UnsettledBoundary {
        source,
        sink,
        _sink_directory: sink_directory,
        control,
        wake,
        bell,
        outcome,
        thread: Some(thread),
    })
}

/// The Channel's event-loop park must never sleep under a deadline it has no timer for.
///
/// An accepted boundary the Sink has not released, with every Source record
/// consumed and no Lua timer, leaves all subscribed facts false. The loop must
/// then sleep without a deadline: the exact remaining duration of a Lua timer is
/// the only deadline this Channel may use, so an idle Channel that slept for a
/// fixed interval instead would show a timed wait here.
#[test]
fn an_idle_channel_with_an_unsettled_boundary_sleeps_without_a_deadline() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let sink_directory = tempfile::tempdir()?;
    let sink = RunningSink::start(
        sink_directory.path(),
        "kafka",
        Arc::clone(&source.channel_region),
    )?;
    source.submit(101, source_payload("unsettled")?)?;
    let mut boundary = unsettled_boundary_channel(source, sink, sink_directory, |channel| {
        channel.run().map_err(|error| error.to_string())
    })?;
    assert_eq!(
        boundary.sink.read_record()?.payload,
        expected_sink_payload("unsettled")?
    );
    assert!(boundary.source.try_completion()?.is_none());
    wait_until_platform_wait(&boundary.bell)?;
    // Give a fixed-interval poll a chance to show up, then assert it never did.
    thread::sleep(Duration::from_millis(50));
    let (total, timed) = boundary.bell.platform_waits();
    assert!(total > 0, "the Channel never entered a platform wait");
    assert_eq!(
        timed, 0,
        "an idle Channel with no Lua timer slept under a deadline"
    );
    // A normal stop still drains the boundary it accepted, so release it first.
    boundary.sink.release(1)?;
    boundary.control.request_stop();
    boundary.wake.wake().map_err(io::Error::other)?;
    boundary.join()?.map_err(io::Error::other)
}

/// The settlement park ends on the Sink's release and completes the old boundary.
///
/// A Channel that still owes a settlement parks on its own doorbell and re-reads
/// the release position, so the Sink's release is what ends the wait. That wait
/// is also deadline-free, like the event loop's.
#[test]
fn a_settlement_park_completes_the_boundary_when_the_sink_releases() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let sink_directory = tempfile::tempdir()?;
    let sink = RunningSink::start(
        sink_directory.path(),
        "kafka",
        Arc::clone(&source.channel_region),
    )?;
    source.submit(101, source_payload("unsettled")?)?;
    let mut boundary = unsettled_boundary_channel(source, sink, sink_directory, |mut channel| {
        let step = channel
            .process_available_source()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| String::from("the committed Source record was not available"))?;
        if step != FlowChannelStep::EventProcessed {
            return Err(format!("the Source record was not processed: {step:?}"));
        }
        channel
            .settle_pending_forward(SettlementInterruption::PlannedStop)
            .map(drop)
            .map_err(|error| error.to_string())
    })?;
    assert_eq!(
        boundary.sink.read_record()?.payload,
        expected_sink_payload("unsettled")?
    );
    wait_until_platform_wait(&boundary.bell)?;
    assert_eq!(
        boundary.bell.platform_waits().1,
        0,
        "the settlement park slept under a deadline it has no timer for"
    );
    boundary.sink.release(1)?;
    boundary.join()?.map_err(io::Error::other)?;
    assert_eq!(
        boundary.source.wait_completion()?,
        completion(101, IngressCompletionStatus::Ok)
    );
    Ok(())
}

/// A Sink may publish release after the Channel's probe but before its wait.
#[test]
fn settlement_release_between_probe_and_park_completes_without_sleeping() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let sink_directory = tempfile::tempdir()?;
    let sink = RunningSink::start(
        sink_directory.path(),
        "kafka",
        Arc::clone(&source.channel_region),
    )?;
    source.submit(101, source_payload("unsettled")?)?;
    let (probed, probe) = mpsc::sync_channel(1);
    let (released, release) = mpsc::sync_channel(1);
    let mut boundary =
        unsettled_boundary_channel(source, sink, sink_directory, move |mut channel| {
            channel
                .process_available_source()
                .map_err(|error| error.to_string())?;
            assert!(
                channel
                    .settlement_status()
                    .map_err(|error| error.to_string())?
                    .is_none()
            );
            probed.send(()).map_err(|error| error.to_string())?;
            release
                .recv_timeout(WAIT_LIMIT)
                .map_err(|error| error.to_string())?;
            channel
                .park_for_settlement()
                .map_err(|error| error.to_string())?;
            channel
                .settle_pending_forward(SettlementInterruption::PlannedStop)
                .map(drop)
                .map_err(|error| error.to_string())
        })?;
    assert_eq!(
        boundary.sink.read_record()?.payload,
        expected_sink_payload("unsettled")?
    );
    probe.recv_timeout(WAIT_LIMIT).map_err(io::Error::other)?;
    boundary.sink.release(1)?;
    released.send(()).map_err(io::Error::other)?;
    boundary.join()?.map_err(io::Error::other)?;
    assert_eq!(boundary.bell.platform_waits(), (0, 0));
    assert_eq!(
        boundary.source.wait_completion()?,
        completion(101, IngressCompletionStatus::Ok)
    );
    Ok(())
}

#[test]
fn timer_failure_retries_all_pending_sources_and_rebuilds_the_vm() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    source.submit(61, source_payload("first")?)?;
    source.submit(62, source_payload("second")?)?;
    let spec = channel_spec(
        r#"
        local invocationCount = 0

        function main(event)
            invocationCount = invocationCount + 1
            if event.type == "timer" then
                error("Timer failed")
            end
            if event.payload.deviceId == "second" then
                setTimeout(0)
            elseif event.payload.deviceId == "flush" then
                if invocationCount ~= 1 then
                    error("Lua VM was not rebuilt")
                end
                emit()
            end
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let channel = RunningChannel::start(&source, spec, HashMap::new())?;

    assert_eq!(
        source.wait_completion()?,
        completion(61, IngressCompletionStatus::Retry)
    );
    assert_eq!(
        source.wait_completion()?,
        completion(62, IngressCompletionStatus::Retry)
    );

    source.submit(63, source_payload("flush")?)?;
    assert_eq!(
        source.wait_completion()?,
        completion(63, IngressCompletionStatus::Ok)
    );
    channel.stop()
}

#[test]
fn planned_stop_interrupts_a_future_timer_wait() -> io::Result<()> {
    let source = SourceQueueFixture::new()?;
    let spec = channel_spec(
        "setTimeout(60000); function main(event) end",
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let channel = RunningChannel::start(&source, spec, HashMap::new())?;

    channel.stop()
}

#[test]
fn planned_stop_during_lua_reload_is_a_clean_channel_stop() -> io::Result<()> {
    let source = SourceQueueFixture::new()?;
    let spec = channel_spec(
        r#"
        local total = 0
        for index = 1, 10000 do
            total = total + index
        end

        function main(event)
        end
        "#,
        SourceDelivery::AtLeastOnce,
        [],
    )?;
    let control = Arc::new(FlowChannelControl::new());
    let (mut channel, _wake, _replacement) = open_channel(
        source.queue_paths(),
        source.bells()?,
        Instant::now(),
        spec,
        test_channel_publisher(0),
        HashMap::new(),
        Arc::clone(&control),
        || false,
    )
    .map_err(channel_error)?;

    control.request_stop();

    assert_eq!(
        channel.reload_lua_vm().map_err(channel_error)?,
        LuaReloadProgress::Stopped
    );
    Ok(())
}

#[test]
fn scheduled_timer_becomes_due_at_its_exact_deadline() {
    let scheduled_at = Instant::now();
    let timer = TimerSchedule {
        scheduled_at,
        delay: Duration::from_millis(10),
    };

    assert_eq!(
        timer_readiness_at(timer, scheduled_at),
        TimerReadiness::Waiting(Duration::from_millis(10))
    );
    assert_eq!(
        timer_readiness_at(timer, scheduled_at + Duration::from_millis(9)),
        TimerReadiness::Waiting(Duration::from_millis(1))
    );
    assert_eq!(
        timer_readiness_at(timer, scheduled_at + Duration::from_millis(10)),
        TimerReadiness::Due
    );
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
        .collect();
    Ok(FlowChannelSpec::new(
        lua_source,
        ScriptVmLimits::try_new(
            NonZeroUsize::new(TEST_MEMORY_LIMIT_BYTES)
                .ok_or_else(|| io::Error::other("Lua memory limit must be positive"))?,
            TEST_CPU_TIME_LIMIT,
        )
        .map_err(io::Error::other)?,
        std::num::NonZeroU64::new(262_144)
            .ok_or_else(|| std::io::Error::other("test record limit must be non-zero"))?,
        lua_source_contract()?,
        sink_contracts,
        delivery,
    ))
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

fn malformed_source_payload() -> Vec<u8> {
    vec![0x0a, 0x02, b'x']
}

fn expected_sink_payload(label: &str) -> io::Result<Vec<u8>> {
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

fn sink_contract_id(value: &str) -> io::Result<SinkContractId> {
    SinkContractId::try_from(value).map_err(io::Error::other)
}

fn completion(record_id: u64, status: IngressCompletionStatus) -> IngressCompletion {
    IngressCompletion {
        record_id,
        status: status as i32,
    }
}

fn channel_error(error: super::FlowChannelError) -> io::Error {
    io::Error::other(error.to_string())
}

fn create_test_bell_region(path: &Path, slots: u32) -> io::Result<Arc<BellRegion>> {
    let slot_count = NonZeroU32::new(slots).ok_or_else(|| io::Error::other("slots"))?;
    create_bell_region(path, slot_count, 1).map_err(io::Error::other)?;
    BellRegion::open(path).map_err(io::Error::other)
}

fn loop_bell(region: &Arc<BellRegion>, index: u32) -> io::Result<Arc<tenon_ipc::bell::LoopBell>> {
    region.loop_bell(index).map_err(io::Error::other)
}

/// Waits until `waiter`'s loop has armed its doorbell for the Queue at `queue_path`.
fn wait_until_armed(queue_path: &Path, region_path: &Path, waiter: QueueWaiter) -> io::Result<()> {
    let deadline = Instant::now() + WAIT_LIMIT;
    while !queue_waiter_is_armed(queue_path, region_path, waiter).map_err(io::Error::other)? {
        if Instant::now() >= deadline {
            return Err(io::Error::other(
                "the Queue waiter never armed its doorbell",
            ));
        }
        thread::yield_now();
    }
    Ok(())
}

/// Waits until the Channel loop has entered a platform wait on its doorbell.
fn wait_until_platform_wait(bell: &Arc<LoopBell>) -> io::Result<()> {
    let deadline = Instant::now() + WAIT_LIMIT;
    while bell.platform_waits().0 == 0 {
        if Instant::now() >= deadline {
            return Err(io::Error::other(
                "the Channel loop never entered a platform wait",
            ));
        }
        thread::yield_now();
    }
    Ok(())
}

/// Waits until the Channel loop counted at least `count` wakes with no work.
fn wait_until_spurious_wakes(bell: &Arc<LoopBell>, count: u64) -> io::Result<()> {
    let deadline = Instant::now() + WAIT_LIMIT;
    while bell.spurious_wakes() < count {
        if Instant::now() >= deadline {
            return Err(io::Error::other(
                "the Channel loop did not count the wake with no work",
            ));
        }
        thread::yield_now();
    }
    Ok(())
}

/// Processes the one Source record this test scenario committed.
///
/// The Channel loop reports "no input" instead of advancing, so a scenario that
/// committed exactly one record must observe the processed step.
fn process_one_source(channel: &mut FlowChannel) -> io::Result<FlowChannelStep> {
    channel
        .process_available_source()
        .map_err(channel_error)?
        .ok_or_else(|| io::Error::other("the committed Source record was not available"))
}

struct SourceQueueFixture {
    _directory: tempfile::TempDir,
    submission_path: PathBuf,
    completion_path: PathBuf,
    channel_region_path: PathBuf,
    channel_region: Arc<BellRegion>,
    source_region: Arc<BellRegion>,
    submission_writer: QueueWriter,
    completion_reader: QueueReader,
}

impl SourceQueueFixture {
    fn new() -> io::Result<Self> {
        let directory = tempfile::tempdir()?;
        let submission_path = directory.path().join("submission.queue");
        let completion_path = directory.path().join("completion.queue");
        // One Flow Channel slot rings the Channel loop; the Source owns two
        // slots in its own region, one per Queue role.
        let channel_region_path = directory.path().join("channels.bells");
        let source_region_path = directory.path().join("source-loops.bells");
        let channel_region = create_test_bell_region(&channel_region_path, 1)?;
        let source_region = create_test_bell_region(&source_region_path, 2)?;
        let max_pending_records = NonZeroU64::new(SOURCE_RECORD_LIMIT)
            .ok_or_else(|| io::Error::other("Source record limit must be positive"))?;
        let max_record_size = NonZeroU64::new(SOURCE_RECORD_SIZE)
            .ok_or_else(|| io::Error::other("Source record size must be positive"))?;
        create_queue_file(
            &submission_path,
            submission_capacity(max_pending_records, max_record_size).map_err(io::Error::other)?,
            max_record_size,
        )
        .map_err(io::Error::other)?;
        create_queue_file(
            &completion_path,
            completion_capacity(max_pending_records).map_err(io::Error::other)?,
            NonZeroU64::new(COMPLETION_MAX_PAYLOAD_SIZE as u64)
                .ok_or_else(|| io::Error::other("Completion payload size must be positive"))?,
        )
        .map_err(io::Error::other)?;
        let submission_writer = QueueWriter::open(
            &submission_path,
            loop_bell(&source_region, 0)?,
            Arc::clone(&channel_region),
        )
        .map_err(io::Error::other)?;
        let completion_reader = QueueReader::open(
            &completion_path,
            loop_bell(&source_region, 1)?,
            Arc::clone(&channel_region),
        )
        .map_err(io::Error::other)?;
        Ok(Self {
            _directory: directory,
            submission_path,
            completion_path,
            channel_region,
            source_region,
            channel_region_path,
            submission_writer,
            completion_reader,
        })
    }

    fn bells(&self) -> io::Result<FlowChannelBells> {
        Ok(FlowChannelBells::new(
            loop_bell(&self.channel_region, 0)?,
            Arc::clone(&self.source_region),
        ))
    }

    fn queue_paths(&self) -> FlowChannelQueuePaths {
        FlowChannelQueuePaths::new(self.submission_path.clone(), self.completion_path.clone())
    }

    /// Opens a bare Completion writer for tests that only need a full Queue.
    fn completion_filler(&self) -> io::Result<QueueWriter> {
        QueueWriter::open(
            &self.completion_path,
            loop_bell(&self.channel_region, 0)?,
            Arc::clone(&self.source_region),
        )
        .map_err(io::Error::other)
    }

    fn submit(&mut self, record_id: u64, payload: Vec<u8>) -> io::Result<()> {
        let encoded = IngressRecord {
            record_id,
            payload: payload.into(),
        }
        .encode_to_vec();
        match self
            .submission_writer
            .try_write(&encoded)
            .map_err(io::Error::other)?
        {
            WriteOutcome::Committed(_) => Ok(()),
            WriteOutcome::Full => Err(io::Error::other("Submission Queue was unexpectedly full")),
        }
    }

    fn try_completion(&mut self) -> io::Result<Option<IngressCompletion>> {
        match self
            .completion_reader
            .try_read()
            .map_err(io::Error::other)?
        {
            ReadOutcome::Record(record) => {
                let completion =
                    IngressCompletion::decode(record.payload()).map_err(io::Error::other)?;
                self.completion_reader
                    .release(1)
                    .map_err(io::Error::other)?;
                Ok(Some(completion))
            }
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

struct RunningChannel {
    commands: FlowChannelCommandControl,
    control: Arc<FlowChannelControl>,
    wake: Option<ChannelWake>,
    result: Receiver<Result<(), String>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl RunningChannel {
    fn start(
        source: &SourceQueueFixture,
        spec: FlowChannelSpec,
        routes: PreparedEgressRoutes,
    ) -> io::Result<Self> {
        Self::start_observed(source, spec, routes, ChannelMetrics::default())
    }

    fn start_observed(
        source: &SourceQueueFixture,
        spec: FlowChannelSpec,
        routes: PreparedEgressRoutes,
        metrics: ChannelMetrics,
    ) -> io::Result<Self> {
        let submission_path = source.submission_path.clone();
        let completion_path = source.completion_path.clone();
        let bells = source.bells()?;
        let control = Arc::new(FlowChannelControl::new());
        let worker_control = Arc::clone(&control);
        let (startup_sender, startup_receiver) = mpsc::sync_channel(1);
        let (result_sender, result) = mpsc::sync_channel(1);
        let thread = thread::spawn(move || {
            let opened = FlowChannel::prepare(
                FlowChannelQueuePaths::new(submission_path, completion_path),
                bells,
                Instant::now(),
                spec,
                test_channel_publisher(0),
                routes,
                worker_control,
                || false,
                metrics,
            )
            .and_then(|(prepared, wake, commands)| Ok((prepared.bind()?, wake, commands)));
            let (channel, wake, commands) = match opened {
                Ok(opened) => opened,
                Err(error) => {
                    let _ = startup_sender.send(Err(error.to_string()));
                    return;
                }
            };
            if startup_sender.send(Ok((wake, commands))).is_err() {
                return;
            }
            let _ = result_sender.send(channel.run().map_err(|error| error.to_string()));
        });
        let (wake, commands) = match startup_receiver.recv_timeout(WAIT_LIMIT) {
            Ok(Ok(wake)) => wake,
            Ok(Err(error)) => {
                thread
                    .join()
                    .map_err(|_| io::Error::other("Flow Channel thread panicked"))?;
                return Err(io::Error::other(error));
            }
            Err(_) => {
                return Err(io::Error::other("Flow Channel did not finish startup"));
            }
        };
        Ok(Self {
            commands,
            control,
            wake: Some(wake),
            result,
            thread: Some(thread),
        })
    }

    fn stop(mut self) -> io::Result<()> {
        self.stop_and_join()
    }

    fn stop_and_join(&mut self) -> io::Result<()> {
        self.control.request_stop();
        if let Some(wake) = self.wake.take() {
            wake.wake().map_err(io::Error::other)?;
        }
        let result = self
            .result
            .recv_timeout(WAIT_LIMIT)
            .map_err(|_| io::Error::other("Flow Channel did not stop"))?
            .map_err(io::Error::other);
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| io::Error::other("Flow Channel thread panicked"))?;
        }
        result
    }
}

impl Drop for RunningChannel {
    fn drop(&mut self) {
        if self.thread.is_some() {
            let _ = self.stop_and_join();
        }
    }
}

struct RunningSourceStep {
    result: Receiver<Result<FlowChannelStep, String>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl RunningSourceStep {
    fn finish(mut self) -> io::Result<FlowChannelStep> {
        let result = self
            .result
            .recv_timeout(WAIT_LIMIT)
            .map_err(|_| io::Error::other("Flow Channel step did not finish"))?
            .map_err(io::Error::other)?;
        if let Some(thread) = self.thread.take() {
            thread
                .join()
                .map_err(|_| io::Error::other("Flow Channel thread panicked"))?;
        }
        Ok(result)
    }
}

fn run_one_source(
    source: &SourceQueueFixture,
    spec: FlowChannelSpec,
    routes: PreparedEgressRoutes,
) -> RunningSourceStep {
    let submission_path = source.submission_path.clone();
    let completion_path = source.completion_path.clone();
    let bells = source.bells();
    let (sender, result) = mpsc::sync_channel(1);
    let thread = thread::spawn(move || {
        let result = (|| -> Result<FlowChannelStep, String> {
            let bells = bells.map_err(|error| error.to_string())?;
            let (mut channel, _wake, _replacement) = open_channel(
                FlowChannelQueuePaths::new(submission_path, completion_path),
                bells,
                Instant::now(),
                spec,
                test_channel_publisher(0),
                routes,
                Arc::new(FlowChannelControl::new()),
                || false,
            )
            .map_err(|error| error.to_string())?;
            channel
                .process_available_source()
                .map_err(|error| error.to_string())?
                .ok_or_else(|| String::from("the committed Source record was not available"))
        })();
        let _ = sender.send(result);
    });
    RunningSourceStep {
        result,
        thread: Some(thread),
    }
}

struct RunningSink {
    path: PathBuf,
    reader: QueueReader,
    region: Arc<BellRegion>,
    channel_region: Arc<BellRegion>,
}

impl RunningSink {
    /// Creates one Egress Queue whose Sink side owns slot zero of its own region.
    ///
    /// `channel_region` is the Flow's Channel region: the Channel publishes its
    /// own loop slot there, so this Sink's release rings the Channel loop in it.
    fn start(directory: &Path, name: &str, channel_region: Arc<BellRegion>) -> io::Result<Self> {
        let path = directory.join(format!("{name}.queue"));
        let capacity = DataCapacity::try_from(EGRESS_CAPACITY).map_err(io::Error::other)?;
        let max_payload_size =
            NonZeroU64::new(capacity.get() - tenon_ipc::queue::FRAME_HEADER_LEN as u64)
                .ok_or_else(|| io::Error::other("Egress payload size must be positive"))?;
        create_queue_file(&path, capacity, max_payload_size).map_err(io::Error::other)?;
        let region = create_test_bell_region(&directory.join(format!("{name}.bells")), 1)?;
        let reader = QueueReader::open(&path, loop_bell(&region, 0)?, Arc::clone(&channel_region))
            .map_err(io::Error::other)?;
        Ok(Self {
            path,
            reader,
            region,
            channel_region,
        })
    }

    fn route(&self) -> io::Result<BTreeMap<PluginInstanceId, PreparedEgressQueue>> {
        let name = self
            .path
            .file_stem()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("Sink fixture has no name"))?;
        Ok(BTreeMap::from([(
            PluginInstanceId::try_from(name).map_err(io::Error::other)?,
            PreparedEgressQueue::Retained {
                path: self.path.clone(),
                peer_region: Arc::clone(&self.region),
            },
        )]))
    }

    fn writer(&self) -> io::Result<QueueWriter> {
        // A stand-in for this input's Channel writer, which publishes the
        // Channel loop's slot and rings this Sink's own doorbell on commit.
        QueueWriter::open(
            &self.path,
            loop_bell(&self.channel_region, 0)?,
            Arc::clone(&self.region),
        )
        .map_err(io::Error::other)
    }

    fn read_record(&mut self) -> io::Result<EgressRecord> {
        let deadline = Instant::now() + WAIT_LIMIT;
        loop {
            match self.reader.try_read().map_err(io::Error::other)? {
                ReadOutcome::Record(record) => {
                    return EgressRecord::decode(record.payload()).map_err(io::Error::other);
                }
                ReadOutcome::Empty if Instant::now() < deadline => thread::yield_now(),
                ReadOutcome::Empty => {
                    return Err(io::Error::other("Egress record did not arrive"));
                }
            }
        }
    }

    fn try_read_record(&mut self) -> io::Result<Option<EgressRecord>> {
        match self.reader.try_read().map_err(io::Error::other)? {
            ReadOutcome::Record(record) => EgressRecord::decode(record.payload())
                .map(Some)
                .map_err(io::Error::other),
            ReadOutcome::Empty => Ok(None),
        }
    }

    fn release(&mut self, count: usize) -> io::Result<()> {
        self.reader.release(count).map_err(io::Error::other)
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the helper forwards the same independent resources to Channel preparation"
)]
fn open_channel(
    queues: FlowChannelQueuePaths,
    bells: FlowChannelBells,
    started_at: Instant,
    spec: FlowChannelSpec,
    diagnostics: ChannelDiagnosticPublisher,
    routes: PreparedEgressRoutes,
    control: Arc<FlowChannelControl>,
    startup_aborted: impl Fn() -> bool + 'static,
) -> Result<(FlowChannel, ChannelWake, FlowChannelCommandControl), super::FlowChannelError> {
    let (prepared, wake, commands) = FlowChannel::prepare(
        queues,
        bells,
        started_at,
        spec,
        diagnostics,
        routes,
        control,
        startup_aborted,
        ChannelMetrics::default(),
    )?;
    Ok((prepared.bind()?, wake, commands))
}

mod program_contracts;

mod candidate_cleanup;

mod metrics;
