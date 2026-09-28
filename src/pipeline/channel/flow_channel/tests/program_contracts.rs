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

use super::*;
use crate::payload_contract::{PluginInterface, PluginProgramPayloadContract};
use crate::runner::test_support::valid_program_descriptor;

#[test]
fn program_roots_process_real_queues_and_wait_for_sink_release() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let directory = tempfile::tempdir()?;
    let sink_contract_id = sink_contract_id("com.example.output@1.0.0")?;
    let spec = program_channel_spec(
        r#"
        local builder = registry:getBuilder("com.example.output@1.0.0")

        function main(event)
            builder:setOutputValue("forwarded:" .. event.payload.deviceId)
            emit(builder:build())
        end
        "#,
        &sink_contract_id,
    )?;
    // Drop the pump before the Channel so failed assertions also interrupt release waits.
    let channel;
    let mut sink = RunningSink::start(
        directory.path(),
        "output",
        Arc::clone(&source.channel_region),
    )?;
    channel = RunningChannel::start(
        &source,
        spec,
        HashMap::from([(sink_contract_id.clone(), sink.route()?)]),
    )?;

    source.submit(41, b"\x1a\x09device-17".to_vec())?;
    let record = sink.read_record()?;
    assert_eq!(record.payload.as_slice(), b"\x3a\x13forwarded:device-17");
    assert!(source.try_completion()?.is_none());

    sink.release(1)?;
    assert_eq!(
        source.wait_completion()?,
        completion(41, IngressCompletionStatus::Ok)
    );
    assert!(source.try_completion()?.is_none());
    assert!(sink.try_read_record()?.is_none());
    channel.stop()
}

#[test]
fn empty_sink_payload_remains_an_output_that_must_be_released() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let directory = tempfile::tempdir()?;
    let sink_contract_id = sink_contract_id("com.example.output@1.0.0")?;
    let spec = program_channel_spec(
        r#"
        local builder = registry:getBuilder("com.example.output@1.0.0")

        function main(event)
            emit(builder:build())
        end
        "#,
        &sink_contract_id,
    )?;
    // Drop the pump before the Channel so failed assertions also interrupt release waits.
    let channel;
    let mut sink = RunningSink::start(
        directory.path(),
        "output",
        Arc::clone(&source.channel_region),
    )?;
    channel = RunningChannel::start(
        &source,
        spec,
        HashMap::from([(sink_contract_id, sink.route()?)]),
    )?;

    source.submit(42, b"\x1a\x09device-17".to_vec())?;
    assert!(sink.read_record()?.payload.is_empty());
    assert!(source.try_completion()?.is_none());
    sink.release(1)?;
    assert_eq!(
        source.wait_completion()?,
        completion(42, IngressCompletionStatus::Ok)
    );
    assert!(source.try_completion()?.is_none());
    assert!(sink.try_read_record()?.is_none());
    channel.stop()
}

#[test]
fn program_roots_survive_vm_rebuild_after_source_decode_failure() -> io::Result<()> {
    let mut source = SourceQueueFixture::new()?;
    let directory = tempfile::tempdir()?;
    let sink_contract_id = sink_contract_id("com.example.output@1.0.0")?;
    let spec = program_channel_spec(
        r#"
        local count = 0
        local builder = registry:getBuilder("com.example.output@1.0.0")

        function main(event)
            count = count + 1
            if event.payload.deviceId == "hold" then
                return
            end
            builder:setOutputValue(event.payload.deviceId .. ":" .. count)
            emit(builder:build())
        end
        "#,
        &sink_contract_id,
    )?;
    // Drop the pump before the Channel so failed assertions also interrupt release waits.
    let channel;
    let mut sink = RunningSink::start(
        directory.path(),
        "output",
        Arc::clone(&source.channel_region),
    )?;
    channel = RunningChannel::start(
        &source,
        spec,
        HashMap::from([(sink_contract_id, sink.route()?)]),
    )?;

    source.submit(51, b"\x1a\x04hold".to_vec())?;
    source.submit(52, b"\x1a\x02x".to_vec())?;
    assert_eq!(
        source.wait_completion()?,
        completion(51, IngressCompletionStatus::Retry)
    );
    assert_eq!(
        source.wait_completion()?,
        completion(52, IngressCompletionStatus::Error)
    );
    assert!(sink.try_read_record()?.is_none());

    source.submit(53, b"\x1a\x05after".to_vec())?;
    assert_eq!(sink.read_record()?.payload.as_slice(), b"\x3a\x07after:1");
    assert!(source.try_completion()?.is_none());
    sink.release(1)?;
    assert_eq!(
        source.wait_completion()?,
        completion(53, IngressCompletionStatus::Ok)
    );
    assert!(source.try_completion()?.is_none());
    assert!(sink.try_read_record()?.is_none());
    channel.stop()
}

#[allow(
    clippy::expect_used,
    reason = "validated Program fixtures guarantee their roots and the test memory limit is positive"
)]
fn program_channel_spec(
    lua_source: &str,
    sink_contract_id: &SinkContractId,
) -> io::Result<FlowChannelSpec> {
    // These roots retain their descriptor pools after the original Program Contracts are dropped.
    let source_root = program_contract(PluginInterface::Source, "device_id", 3)?
        .source_root_message()
        .expect("the validated Source Program has a Source root");
    let sink_root = program_contract(PluginInterface::Sink, "output_value", 7)?
        .sink_root_message()
        .expect("the validated Sink Program has a Sink root");
    Ok(FlowChannelSpec::new(
        "source",
        lua_source,
        ScriptVmLimits::try_new(
            NonZeroUsize::new(TEST_MEMORY_LIMIT_BYTES).expect("the test memory limit is positive"),
            TEST_CPU_TIME_LIMIT,
        )
        .map_err(io::Error::other)?,
        std::num::NonZeroU64::new(262_144)
            .ok_or_else(|| std::io::Error::other("test record limit must be non-zero"))?,
        source_root,
        HashMap::from([(sink_contract_id.clone(), sink_root)]),
        SourceDelivery::AtLeastOnce,
    ))
}

fn program_contract(
    interface: PluginInterface,
    field_name: &str,
    field_number: i32,
) -> io::Result<PluginProgramPayloadContract> {
    let mut descriptor =
        prost_types::FileDescriptorSet::decode(valid_program_descriptor(interface)?.as_slice())
            .map_err(io::Error::other)?;
    let field = &mut descriptor.file[0].message_type[0].field[0];
    field.name = Some(field_name.to_owned());
    field.json_name = None;
    field.number = Some(field_number);
    PluginProgramPayloadContract::parse(descriptor.encode_to_vec(), interface)
        .map_err(io::Error::other)
}
