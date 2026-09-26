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

use super::emit::EmitBoundary;
use super::metrics::LuaMetrics;
use super::payload::BuiltPayload;
use super::{
    LuaMainOutcome, LuaPrintCallback, LuaPrintRecord, LuaVm, LuaVmError, LuaVmErrorKind,
    LuaVmFatalFault, lua_message_text,
};
use crate::config::ScriptVmLimits;
use crate::identifiers::SinkContractId;
use crate::metrics::capture_test_support::Capture;
use crate::payload_contract::{PluginInterface, PluginProgramPayloadContract};
use criterion::Criterion;
use mlua::{
    AnyUserData, FromLua, Function, LuaString, MetaMethod, Table, UserData, UserDataMethods, Value,
};
use opentelemetry::KeyValue;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use prost::Message as _;
use prost_reflect::{
    DynamicMessage, Kind, MapKey, MessageDescriptor, ReflectMessage, Value as ProtobufValue,
};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::error::Error as _;
use std::fs;
use std::hint::black_box;
use std::io;
use std::num::{NonZeroU64, NonZeroUsize};
use std::process::Command;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// General VM tests inject an anonymous timer event without scheduling a task.
// Timer lifecycle tests pass the event returned by begin_timer instead.
impl LuaVm {
    fn call_timer(&mut self, timestamp_millis: i64) -> LuaMainOutcome {
        let now = Instant::now();
        self.call_timer_event(
            timestamp_millis,
            super::TimerEvent {
                schedule: super::TimerSchedule {
                    scheduled_at: now,
                    delay: Duration::ZERO,
                },
                deadline: now,
                eligible_at: timestamp_millis,
                id: None,
                sequence: 0,
            },
        )
    }

    fn scheduled_timer(&self) -> Option<super::TimerSchedule> {
        self.next_timer_event().map(|timer| timer.schedule)
    }
}

const TEST_MEMORY_LIMIT_BYTES: usize = 4 * 1024 * 1024;
const TEST_CPU_TIME_LIMIT: Duration = Duration::from_millis(100);

#[derive(Debug)]
struct CapturedPrint {
    text: Box<str>,
    truncated: bool,
    invalid_utf8: bool,
}

fn recording_print_callback() -> (LuaPrintCallback, Rc<RefCell<Vec<CapturedPrint>>>) {
    let prints = Rc::new(RefCell::new(Vec::new()));
    let callback_prints = Rc::clone(&prints);
    let callback = Box::new(move |record: LuaPrintRecord| {
        let (text, truncated, invalid_utf8) = record.render()?;
        callback_prints.borrow_mut().push(CapturedPrint {
            text,
            truncated,
            invalid_utf8,
        });
        Ok(())
    });
    (Some(callback), prints)
}

trait LuaMainOutcomeTestExt {
    fn result(&self) -> Result<(), &LuaVmError>;
    fn emit_boundaries(&self) -> &[EmitBoundary];
    fn into_result_without_emit_boundaries(self) -> Result<(), LuaVmError>;
}

impl LuaMainOutcomeTestExt for LuaMainOutcome {
    fn result(&self) -> Result<(), &LuaVmError> {
        self.result.as_ref().copied()
    }

    fn emit_boundaries(&self) -> &[EmitBoundary] {
        &self.emit_boundaries
    }

    fn into_result_without_emit_boundaries(self) -> Result<(), LuaVmError> {
        assert!(
            self.emit_boundaries.is_empty(),
            "Test did not expect accepted emit boundaries"
        );
        self.result
    }
}

pub(super) fn limits() -> io::Result<ScriptVmLimits> {
    ScriptVmLimits::try_new(non_zero(TEST_MEMORY_LIMIT_BYTES)?, TEST_CPU_TIME_LIMIT)
        .map_err(io::Error::other)
}

pub(super) fn load_vm(source: &str, limits: ScriptVmLimits) -> Result<LuaVm, LuaVmError> {
    load_vm_with_contracts(source, limits, HashMap::new())
}

fn load_vm_with_contracts(
    source: &str,
    limits: ScriptVmLimits,
    sink_payload_contracts: HashMap<SinkContractId, MessageDescriptor>,
) -> Result<LuaVm, LuaVmError> {
    let source_contract = lua_source_contract()
        .map_err(|_| LuaVmError::without_source(LuaVmErrorKind::InitializationFailed))?;
    LuaVm::load(
        source,
        limits,
        std::num::NonZeroU64::new(262_144)
            .ok_or_else(|| LuaVmError::without_source(LuaVmErrorKind::InitializationFailed))?,
        source_contract,
        sink_payload_contracts,
        None,
        || false,
    )
}

fn non_zero(value: usize) -> io::Result<NonZeroUsize> {
    NonZeroUsize::new(value).ok_or_else(|| io::Error::other("test value must be non-zero"))
}

fn evaluate_result<T: FromLua>(vm: &LuaVm, source: &str) -> Result<T, LuaVmError> {
    let chunk = vm
        .lua
        .load(source)
        .set_mode(mlua::chunk::ChunkMode::Text)
        .set_environment(vm.environment.clone());
    let function = vm
        .run_with_budget(|| chunk.into_function())
        .map_err(|error| vm.map_error(error, LuaVmErrorKind::SyntaxInvalid))?;
    vm.run_with_budget(|| function.call::<T>(()))
        .map_err(|error| vm.map_error(error, LuaVmErrorKind::TopLevelFailed))
}

fn lua_has_timeout(vm: &LuaVm) -> Result<bool, LuaVmError> {
    evaluate_result(vm, "return hasTimeout()")
}

fn fatal_fault(vm: &LuaVm) -> Option<LuaVmErrorKind> {
    vm.fatal_fault.get().map(LuaVmFatalFault::error_kind)
}

fn test_error(error: LuaVmError) -> io::Error {
    io::Error::other(error.to_string())
}

fn test_error_ref(error: &LuaVmError) -> io::Error {
    io::Error::other(error.to_string())
}

fn mlua_test_error(_error: mlua::Error) -> io::Error {
    io::Error::other("Lua test operation failed")
}

pub(crate) fn lua_builder_contract() -> io::Result<MessageDescriptor> {
    let descriptor = compile_payload_contract_fixture(
        "tenon-lua-builder-test",
        "contracts/sink/test-fixtures/lua-builder",
        "sink_record_payload.proto",
    )?;
    PluginProgramPayloadContract::parse(descriptor, PluginInterface::Sink)
        .map_err(io::Error::other)?
        .sink_root_message()
        .ok_or_else(|| io::Error::other("Sink fixture root is missing"))
}

pub(crate) fn lua_source_contract() -> io::Result<MessageDescriptor> {
    static CONTRACT: OnceLock<Result<MessageDescriptor, String>> = OnceLock::new();

    match CONTRACT.get_or_init(|| {
        compile_payload_contract_fixture(
            "tenon-lua-source-test",
            "contracts/source/test-fixtures/lua-event",
            "source_record_payload.proto",
        )
        .and_then(|descriptor| {
            PluginProgramPayloadContract::parse(descriptor, PluginInterface::Source)
                .map_err(io::Error::other)?
                .source_root_message()
                .ok_or_else(|| io::Error::other("Source fixture root is missing"))
        })
        .map_err(|error| error.to_string())
    }) {
        Ok(contract) => Ok(contract.clone()),
        Err(message) => Err(io::Error::other(message.clone())),
    }
}

fn alternate_lua_source_contract() -> io::Result<MessageDescriptor> {
    let descriptor = compile_payload_contract_fixture(
        "tenon-lua-alternate-source-test",
        "contracts/source/test-fixtures/lua-event-alternate",
        "source_record_payload.proto",
    )?;
    PluginProgramPayloadContract::parse(descriptor, PluginInterface::Source)
        .map_err(io::Error::other)?
        .source_root_message()
        .ok_or_else(|| io::Error::other("Alternate Source fixture root is missing"))
}

fn compile_payload_contract_fixture(
    temporary_prefix: &str,
    relative_source_directory: &str,
    source_file: &str,
) -> io::Result<Vec<u8>> {
    let output_directory = tempfile::Builder::new()
        .prefix(temporary_prefix)
        .tempdir()?;
    let descriptor_path = output_directory.path().join("payload.descriptor.pb");
    let source_directory = format!("{}/{relative_source_directory}", env!("CARGO_MANIFEST_DIR"),);
    let protoc = protoc_bin_vendored::protoc_bin_path().map_err(io::Error::other)?;
    let status = Command::new(protoc)
        .current_dir(&source_directory)
        .arg(format!(
            "--descriptor_set_out={}",
            descriptor_path.display()
        ))
        .arg("--include_imports")
        .arg("--include_source_info")
        .arg("--proto_path=.")
        .arg(source_file)
        .status()?;
    if !status.success() {
        return Err(io::Error::other("Lua test descriptor compilation failed"));
    }
    fs::read(descriptor_path)
}

fn lua_builder_contracts() -> io::Result<HashMap<SinkContractId, MessageDescriptor>> {
    let contract = lua_builder_contract()?;
    let sink_contract_id =
        SinkContractId::try_from("com.example.lua-builder@1.0.0").map_err(io::Error::other)?;
    Ok(HashMap::from([(sink_contract_id, contract)]))
}

fn built_payload(vm: &LuaVm, global_name: &str) -> io::Result<AnyUserData> {
    vm.environment_values
        .raw_get::<AnyUserData>(global_name)
        .map_err(|_| io::Error::other("Built Payload global is unavailable"))
}

fn payload_boundary(boundary: &EmitBoundary) -> io::Result<(&SinkContractId, &[u8])> {
    let EmitBoundary::Payload {
        sink_contract_id,
        payload,
    } = boundary
    else {
        return Err(io::Error::other("Expected a payload emit boundary"));
    };
    Ok((sink_contract_id, payload))
}

fn protobuf_field(
    message: &prost_reflect::DynamicMessage,
    name: &str,
) -> io::Result<ProtobufValue> {
    let field = message
        .descriptor()
        .get_field_by_name(name)
        .ok_or_else(|| io::Error::other(format!("Protobuf test field is missing: {name}")))?;
    Ok(message.get_field(&field).into_owned())
}

fn set_protobuf_field(
    message: &mut DynamicMessage,
    name: &str,
    value: ProtobufValue,
) -> io::Result<()> {
    let field = message
        .descriptor()
        .get_field_by_name(name)
        .ok_or_else(|| io::Error::other(format!("Protobuf test field is missing: {name}")))?;
    message
        .try_set_field(&field, value)
        .map_err(io::Error::other)
}

fn child_message_descriptor(
    message: &DynamicMessage,
    field_name: &str,
) -> io::Result<prost_reflect::MessageDescriptor> {
    let field = message
        .descriptor()
        .get_field_by_name(field_name)
        .ok_or_else(|| io::Error::other(format!("Protobuf test field is missing: {field_name}")))?;
    let Kind::Message(descriptor) = field.kind() else {
        return Err(io::Error::other("Protobuf test field is not a message"));
    };
    Ok(descriptor)
}

fn full_source_message(contract: &MessageDescriptor) -> io::Result<DynamicMessage> {
    let mut payload = DynamicMessage::new(contract.clone());
    let child_descriptor = child_message_descriptor(&payload, "child")?;
    let mut child = DynamicMessage::new(child_descriptor);
    set_protobuf_field(
        &mut child,
        "name",
        ProtobufValue::String(String::from("primary")),
    )?;
    set_protobuf_field(
        &mut child,
        "value",
        ProtobufValue::Bytes(bytes::Bytes::from_static(b"\x00\xff")),
    )?;

    for (name, value) in [
        ("device_id", ProtobufValue::String(String::from("device-7"))),
        ("enabled", ProtobufValue::Bool(true)),
        ("signed_32", ProtobufValue::I32(i32::MIN)),
        ("signed_64", ProtobufValue::I64(i64::MIN + 1)),
        ("unsigned_64", ProtobufValue::U64(u64::MAX)),
        ("ratio", ProtobufValue::F32(1.5)),
        ("score", ProtobufValue::F64(2.25)),
        (
            "body",
            ProtobufValue::Bytes(bytes::Bytes::from_static(b"\x00\x80\xff")),
        ),
        ("status", ProtobufValue::EnumNumber(1)),
        ("child", ProtobufValue::Message(child.clone())),
        ("explicit_zero", ProtobufValue::I32(0)),
        (
            "text_choice",
            ProtobufValue::String(String::from("selected")),
        ),
        ("fixed_32", ProtobufValue::U32(u32::MAX)),
        ("fixed_64", ProtobufValue::U64(u64::MAX - 1)),
        ("signed_fixed_32", ProtobufValue::I32(i32::MIN + 1)),
        ("signed_fixed_64", ProtobufValue::I64(i64::MAX)),
    ] {
        set_protobuf_field(&mut payload, name, value)?;
    }
    for (name, value) in [
        (
            "registers",
            ProtobufValue::List(vec![ProtobufValue::U32(7), ProtobufValue::U32(11)]),
        ),
        (
            "children",
            ProtobufValue::List(vec![ProtobufValue::Message(child.clone())]),
        ),
        (
            "counts",
            ProtobufValue::Map(HashMap::from([(
                MapKey::String(String::from("alpha")),
                ProtobufValue::I32(3),
            )])),
        ),
        (
            "children_by_id",
            ProtobufValue::Map(HashMap::from([(
                MapKey::U64(u64::MAX),
                ProtobufValue::Message(child),
            )])),
        ),
        (
            "labels_by_flag",
            ProtobufValue::Map(HashMap::from([
                (
                    MapKey::Bool(false),
                    ProtobufValue::String(String::from("disabled")),
                ),
                (
                    MapKey::Bool(true),
                    ProtobufValue::String(String::from("enabled")),
                ),
            ])),
        ),
        (
            "packets",
            ProtobufValue::List(vec![
                ProtobufValue::Bytes(bytes::Bytes::from_static(b"first")),
                ProtobufValue::Bytes(bytes::Bytes::from_static(b"\x00\xff")),
            ]),
        ),
    ] {
        set_protobuf_field(&mut payload, name, value)?;
    }
    Ok(payload)
}

fn call_source(
    vm: &mut LuaVm,
    timestamp: i64,
    payload: impl Into<bytes::Bytes>,
) -> io::Result<LuaMainOutcome> {
    vm.call_source(timestamp, payload.into())
        .map_err(io::Error::other)
}

#[test]
fn channel_stop_probe_interrupts_lua_after_main_has_started() -> io::Result<()> {
    let stop_requested = Arc::new(AtomicBool::new(false));
    let stop_probe = Arc::clone(&stop_requested);
    let source_contract = lua_source_contract()?;
    let mut vm = LuaVm::load(
        r#"
        function main(event)
            requestStop()
            while true do
            end
        end
        "#,
        ScriptVmLimits::try_new(non_zero(TEST_MEMORY_LIMIT_BYTES)?, Duration::from_secs(30))
            .map_err(io::Error::other)?,
        std::num::NonZeroU64::new(262_144)
            .ok_or_else(|| std::io::Error::other("test record limit must be non-zero"))?,
        source_contract,
        HashMap::new(),
        None,
        move || stop_probe.load(Ordering::Acquire),
    )
    .map_err(test_error)?;
    let request_stop = vm
        .lua
        .create_function(move |_, ()| {
            stop_requested.store(true, Ordering::Release);
            Ok(())
        })
        .map_err(mlua_test_error)?;
    vm.environment
        .raw_set("requestStop", request_stop)
        .map_err(mlua_test_error)?;

    let error = match call_source(&mut vm, 1, empty_source_payload())?
        .into_result_without_emit_boundaries()
    {
        Ok(_) => {
            return Err(io::Error::other(
                "Lua main ignored the Channel stop request",
            ));
        }
        Err(error) => error,
    };
    assert_eq!(error.kind(), LuaVmErrorKind::ExecutionStopped);
    Ok(())
}

fn empty_source_payload() -> bytes::Bytes {
    bytes::Bytes::new()
}

fn full_source_payload() -> io::Result<bytes::Bytes> {
    let contract = lua_source_contract()?;
    Ok(full_source_message(&contract)?.encode_to_vec().into())
}

fn source_payload_with_body(body: Vec<u8>) -> io::Result<bytes::Bytes> {
    let contract = lua_source_contract()?;
    let mut payload = DynamicMessage::new(contract.clone());
    set_protobuf_field(
        &mut payload,
        "body",
        ProtobufValue::Bytes(bytes::Bytes::from(body)),
    )?;
    Ok(payload.encode_to_vec().into())
}

#[test]
fn top_level_build_returns_an_immutable_payload_snapshot() -> io::Result<()> {
    let vm = load_vm_with_contracts(
        r#"
        builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        builder:setLabel("before")
        child = builder:getChildBuilder()
        child:setName("before-child")
        initial_payload = builder:build()
        builder:setLabel("after")
        child:setName("after-child")

        function main(event)
        end
        "#,
        limits()?,
        lua_builder_contracts()?,
    )
    .map_err(test_error)?;

    let payload = built_payload(&vm, "initial_payload")?;
    let payload = payload
        .borrow::<BuiltPayload>()
        .map_err(|_| io::Error::other("Built Payload userdata type is unavailable"))?;
    assert_eq!(
        payload.sink_contract_id().to_string(),
        "com.example.lua-builder@1.0.0"
    );
    let message = payload.message();
    let label = message
        .descriptor()
        .get_field_by_name("label")
        .ok_or_else(|| io::Error::other("Label field is missing from the test descriptor"))?;
    assert_eq!(
        message.get_field(&label).as_ref(),
        &ProtobufValue::String(String::from("before")),
        "Built Payload changed after its Builder was reused"
    );
    let ProtobufValue::Message(child) = protobuf_field(message, "child")? else {
        return Err(io::Error::other("Child field is not a Protobuf message"));
    };
    assert_eq!(
        protobuf_field(&child, "name")?,
        ProtobufValue::String(String::from("before-child")),
        "Nested Built Payload field changed after its Builder was reused"
    );
    Ok(())
}

#[test]
fn main_can_create_and_build_a_payload() -> io::Result<()> {
    let mut vm = load_vm_with_contracts(
        r#"
        local builder = registry:getBuilder("com.example.lua-builder@1.0.0")

        function main(event)
            builder:setLabel(event.payload.deviceId)
            main_payload = builder:build()
        end
        "#,
        limits()?,
        lua_builder_contracts()?,
    )
    .map_err(test_error)?;

    call_source(&mut vm, 7, full_source_payload()?)?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    let payload = built_payload(&vm, "main_payload")?;
    let payload = payload
        .borrow::<BuiltPayload>()
        .map_err(|_| io::Error::other("Built Payload userdata type is unavailable"))?;
    assert_eq!(
        protobuf_field(payload.message(), "label")?,
        ProtobufValue::String(String::from("device-7"))
    );
    Ok(())
}

#[test]
fn emit_checks_complete_record_size_and_preserves_accepted_boundaries() -> io::Result<()> {
    let registry = HashMap::from([(
        SinkContractId::try_from("com.example.lua-builder@1.0.0").map_err(io::Error::other)?,
        lua_builder_contract()?,
    )]);
    let mut vm = LuaVm::load(
        r#"
        local builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        function main(event)
            builder:setLabel(string.rep("x", 1018))
            emit(builder:build())
            builder:setLabel(string.rep("x", 1019))
            local ok, message = pcall(emit, builder:build())
            assert(not ok and message == "egress.record_too_large")
            emit()
            emit(builder:build())
        end
        "#,
        limits()?,
        std::num::NonZeroU64::new(1024)
            .ok_or_else(|| io::Error::other("test record limit must be non-zero"))?,
        lua_source_contract()?,
        registry,
        None,
        || false,
    )
    .map_err(test_error)?;
    let outcome = vm.call_timer(1);
    let error = outcome
        .result()
        .err()
        .ok_or_else(|| io::Error::other("oversized emit must fail"))?;
    assert_eq!(error.kind(), LuaVmErrorKind::MainFailed);
    assert_eq!(outcome.emit_boundaries().len(), 2);
    let (_, payload) = payload_boundary(&outcome.emit_boundaries()[0])?;
    assert_eq!(crate::contracts::sink::encoded_len(payload.len()), 1024);
    assert_eq!(outcome.emit_boundaries()[1], EmitBoundary::CompletionOnly);
    Ok(())
}

#[test]
fn emit_accepts_built_payloads_and_preserves_the_accepted_prefix() -> io::Result<()> {
    let contract = lua_builder_contract()?;
    let root_descriptor = contract.clone();
    let sink_contract_id =
        SinkContractId::try_from("com.example.lua-builder@1.0.0").map_err(io::Error::other)?;
    let registry = HashMap::from([(sink_contract_id, contract)]);
    let mut vm = load_vm_with_contracts(
        r#"
        local builder = registry:getBuilder("com.example.lua-builder@1.0.0")

        function main(event)
            local ok, message = pcall(emit, "not-a-built-payload")
            assert(not ok)
            assert(message == "emit argument must be a built Payload")

            local nil_ok, nil_message = pcall(emit, nil)
            assert(not nil_ok)
            assert(nil_message == "emit argument must be a built Payload")

            local extra_ok, extra_message = pcall(
                emit,
                builder:build(),
                "extra-argument"
            )
            assert(not extra_ok)
            assert(extra_message == "emit arguments are invalid")

            builder:setLabel("first")
            emit(builder:build())
            builder:setLabel("second")
            emit(builder:build())
            builder:setLabel("after")
            error("stop after accepted emits")
        end
        "#,
        limits()?,
        registry,
    )
    .map_err(test_error)?;

    let outcome = vm.call_timer(7);
    let Err(error) = outcome.result() else {
        return Err(io::Error::other("main failure should remain observable"));
    };
    assert_eq!(error.kind(), LuaVmErrorKind::MainFailed);
    assert_eq!(outcome.emit_boundaries().len(), 2);

    for (boundary, expected_label) in outcome.emit_boundaries().iter().zip(["first", "second"]) {
        let (sink_contract_id, payload) = payload_boundary(boundary)?;
        assert_eq!(
            sink_contract_id.to_string(),
            "com.example.lua-builder@1.0.0"
        );
        let decoded =
            DynamicMessage::decode(root_descriptor.clone(), payload).map_err(io::Error::other)?;
        assert_eq!(
            protobuf_field(&decoded, "label")?,
            ProtobufValue::String(String::from(expected_label))
        );
    }

    let poisoned = vm.call_timer(8);
    assert_eq!(
        poisoned.result().err().map(LuaVmError::kind),
        Some(LuaVmErrorKind::MainFailed)
    );
    assert!(poisoned.emit_boundaries().is_empty());
    Ok(())
}

#[test]
fn payload_and_completion_only_emits_preserve_one_ordered_prefix() -> io::Result<()> {
    let contract = lua_builder_contract()?;
    let root_descriptor = contract.clone();
    let sink_contract_id =
        SinkContractId::try_from("com.example.lua-builder@1.0.0").map_err(io::Error::other)?;
    let registry = HashMap::from([(sink_contract_id, contract)]);
    let mut vm = load_vm_with_contracts(
        r#"
        local builder = registry:getBuilder("com.example.lua-builder@1.0.0")

        function main(event)
            emit()
            builder:setLabel("first")
            emit(builder:build())
            emit()
            builder:setLabel("second")
            emit(builder:build())
            error("stop after accepted boundaries")
        end
        "#,
        limits()?,
        registry,
    )
    .map_err(test_error)?;

    let outcome = vm.call_timer(7);
    assert_eq!(
        outcome.result().err().map(LuaVmError::kind),
        Some(LuaVmErrorKind::MainFailed)
    );
    let [
        EmitBoundary::CompletionOnly,
        EmitBoundary::Payload { payload: first, .. },
        EmitBoundary::CompletionOnly,
        EmitBoundary::Payload {
            payload: second, ..
        },
    ] = outcome.emit_boundaries()
    else {
        return Err(io::Error::other(
            "main should preserve four ordered emit boundaries",
        ));
    };
    for (payload, expected_label) in [(first, "first"), (second, "second")] {
        let decoded = DynamicMessage::decode(root_descriptor.clone(), payload.as_slice())
            .map_err(io::Error::other)?;
        assert_eq!(
            protobuf_field(&decoded, "label")?,
            ProtobufValue::String(String::from(expected_label))
        );
    }
    Ok(())
}

#[test]
fn emit_uses_canonical_field_and_map_key_order() -> io::Result<()> {
    let mut vm = load_vm_with_contracts(
        r#"
        local function fill(builder, reverse)
            if reverse then
                builder:putCounts("z", 9)
                builder:putCounts("a", 1)
                builder:putChildrenById("10"):setName("ten")
                builder:putChildrenById("2"):setName("two")
            else
                builder:putChildrenById("2"):setName("two")
                builder:putChildrenById("10"):setName("ten")
                builder:putCounts("a", 1)
                builder:putCounts("z", 9)
            end
        end

        function main(event)
            local first = registry:getBuilder("com.example.lua-builder@1.0.0")
            local second = registry:getBuilder("com.example.lua-builder@1.0.0")
            fill(first, true)
            fill(second, false)
            emit(first:build())
            emit(second:build())
        end
        "#,
        limits()?,
        lua_builder_contracts()?,
    )
    .map_err(test_error)?;

    let outcome = vm.call_timer(7);
    outcome.result().map_err(test_error_ref)?;
    let [first, second] = outcome.emit_boundaries() else {
        return Err(io::Error::other("main should accept exactly two emits"));
    };
    let (_, first) = payload_boundary(first)?;
    let (_, second) = payload_boundary(second)?;
    let expected = [
        0x72, 0x05, 0x0a, 0x01, b'a', 0x10, 0x01, 0x72, 0x05, 0x0a, 0x01, b'z', 0x10, 0x09, 0x7a,
        0x09, 0x08, 0x02, 0x12, 0x05, 0x0a, 0x03, b't', b'w', b'o', 0x7a, 0x09, 0x08, 0x0a, 0x12,
        0x05, 0x0a, 0x03, b't', b'e', b'n',
    ];
    assert_eq!(first, expected);
    assert_eq!(second, expected);
    Ok(())
}

#[test]
fn emit_encodes_every_supported_payload_field_shape() -> io::Result<()> {
    let contract = lua_builder_contract()?;
    let root_descriptor = contract.clone();
    let sink_contract_id =
        SinkContractId::try_from("com.example.lua-builder@1.0.0").map_err(io::Error::other)?;
    let registry = HashMap::from([(sink_contract_id, contract)]);
    let mut vm = load_vm_with_contracts(
        r#"
        local builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        builder:setEnabled(true)
        builder:setSigned32(-2147483648)
        builder:setSigned64(-9223372036854775807)
        builder:setUnsigned32(4294967295)
        builder:setUnsigned64("18446744073709551615")
        builder:setRatio(1.5)
        builder:setScore(2)
        builder:setLabel("ready")
        builder:setBody(string.char(0, 255))
        builder:setStatus("STATUS_READY")
        builder:getChildBuilder():setName("primary")
        builder:addSamples(-1)
        builder:addSamples(7)
        builder:addChildrenBuilder():setName("repeated")
        builder:putCounts("", 0)
        builder:putCounts("alpha", 3)
        builder:putChildrenById("0")
        builder:putChildrenById("18446744073709551615"):setName("mapped")
        builder:putNamedChildren("")
        builder:putNamedChildren("primary"):setName("named")
        builder:setExplicitZero(0)
        builder:setIntegerChoice(9)
        expected_payload = builder:build()

        function main(event)
            emit(expected_payload)
        end
        "#,
        limits()?,
        registry,
    )
    .map_err(test_error)?;

    let expected_payload = built_payload(&vm, "expected_payload")?;
    let expected_payload = expected_payload
        .borrow::<BuiltPayload>()
        .map_err(|_| io::Error::other("Built Payload userdata type is unavailable"))?;
    let outcome = vm.call_timer(7);
    outcome.result().map_err(test_error_ref)?;
    let [accepted] = outcome.emit_boundaries() else {
        return Err(io::Error::other("main should accept exactly one emit"));
    };
    let (_, accepted) = payload_boundary(accepted)?;
    let decoded = DynamicMessage::decode(root_descriptor, accepted).map_err(io::Error::other)?;
    assert_eq!(&decoded, expected_payload.message());
    Ok(())
}

#[test]
fn same_type_builders_reuse_method_functions() -> io::Result<()> {
    load_vm_with_contracts(
        r#"
        local first = registry:getBuilder("com.example.lua-builder@1.0.0")
        local second = registry:getBuilder("com.example.lua-builder@1.0.0")
        local first_child = first:getChildBuilder()
        local second_child = second:getChildBuilder()

        assert(first.setLabel == second.setLabel)
        assert(first.build == second.build)
        assert(first.missing == second.missing)
        assert(first_child.setName == second_child.setName)
        assert(first_child.missing == second_child.missing)

        function main(event)
        end
        "#,
        limits()?,
        lua_builder_contracts()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn incremental_payload_accounting_covers_growth_and_replacement_boundaries() -> io::Result<()> {
    let limits = ScriptVmLimits::try_new(non_zero(8 * 1024 * 1024)?, Duration::from_secs(2))
        .map_err(io::Error::other)?;
    let vm = load_vm_with_contracts(
        r#"
        local builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        for index = 1, 257 do
            builder:addSamples(index)
            builder:putCounts("point-" .. index, index)
        end
        for index = 1, 129 do
            builder:putCounts("point-" .. index, -index)
        end
        builder:setTextChoice("temporary")
        builder:setIntegerChoice(7)
        builder:clearIntegerChoice()
        boundary_payload = builder:build()

        function main(event)
        end
        "#,
        limits,
        lua_builder_contracts()?,
    )
    .map_err(test_error)?;

    let payload = built_payload(&vm, "boundary_payload")?;
    let payload = payload
        .borrow::<BuiltPayload>()
        .map_err(|_| io::Error::other("Built Payload userdata type is unavailable"))?;
    let ProtobufValue::List(samples) = protobuf_field(payload.message(), "samples")? else {
        return Err(io::Error::other("Samples field is not a Protobuf list"));
    };
    assert_eq!(samples.len(), 257);
    let ProtobufValue::Map(counts) = protobuf_field(payload.message(), "counts")? else {
        return Err(io::Error::other("Counts field is not a Protobuf map"));
    };
    assert_eq!(counts.len(), 257);
    assert_eq!(
        counts.get(&prost_reflect::MapKey::String(String::from("point-129"))),
        Some(&ProtobufValue::I32(-129))
    );
    Ok(())
}

#[test]
fn builder_maps_every_supported_protobuf_field_shape() -> io::Result<()> {
    let vm = load_vm_with_contracts(
        r#"
        local builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        builder:setEnabled(true)
        builder:setSigned32(-2147483648)
        builder:setSigned64(-9223372036854775807)
        builder:setUnsigned32(4294967295)
        builder:setUnsigned64("18446744073709551615")
        builder:setRatio(1.5)
        builder:setScore(2)
        builder:setLabel("ready")
        builder:setBody(string.char(0, 255))
        builder:setStatus("STATUS_READY")

        local child = builder:getChildBuilder()
        child:setName("primary")
        child:setValue("child-bytes")

        builder:addSamples(-1)
        builder:addSamples(7)
        local repeated_child = builder:addChildrenBuilder()
        repeated_child:setName("repeated")

        builder:putCounts("alpha", 3)
        local mapped_child = builder:putChildrenById("18446744073709551615")
        mapped_child:setName("replaced")
        local replacement_child = builder:putChildrenById("18446744073709551615")
        replacement_child:setName("mapped")
        local named_child = builder:putNamedChildren("primary")
        named_child:setName("named")
        builder:setExplicitZero(0)

        builder:setTextChoice("first")
        local choice_child = builder:getChildChoiceBuilder()
        choice_child:setName("choice-child")
        message_choice_payload = builder:build()
        builder:setIntegerChoice(9)
        complete_payload = builder:build()

        function main(event)
        end
        "#,
        limits()?,
        lua_builder_contracts()?,
    )
    .map_err(test_error)?;

    let payload = built_payload(&vm, "complete_payload")?;
    let payload = payload
        .borrow::<BuiltPayload>()
        .map_err(|_| io::Error::other("Built Payload userdata type is unavailable"))?;
    let message = payload.message();

    assert_eq!(
        protobuf_field(message, "enabled")?,
        ProtobufValue::Bool(true)
    );
    assert_eq!(
        protobuf_field(message, "signed_32")?,
        ProtobufValue::I32(i32::MIN)
    );
    assert_eq!(
        protobuf_field(message, "signed_64")?,
        ProtobufValue::I64(-9_223_372_036_854_775_807)
    );
    assert_eq!(
        protobuf_field(message, "unsigned_32")?,
        ProtobufValue::U32(u32::MAX)
    );
    assert_eq!(
        protobuf_field(message, "unsigned_64")?,
        ProtobufValue::U64(u64::MAX)
    );
    assert_eq!(protobuf_field(message, "ratio")?, ProtobufValue::F32(1.5));
    assert_eq!(protobuf_field(message, "score")?, ProtobufValue::F64(2.0));
    assert_eq!(
        protobuf_field(message, "label")?,
        ProtobufValue::String(String::from("ready"))
    );
    assert_eq!(
        protobuf_field(message, "body")?,
        ProtobufValue::Bytes(prost::bytes::Bytes::from_static(&[0, 255]))
    );
    assert_eq!(
        protobuf_field(message, "status")?,
        ProtobufValue::EnumNumber(1)
    );
    let explicit_zero = message
        .descriptor()
        .get_field_by_name("explicit_zero")
        .ok_or_else(|| io::Error::other("Explicit zero field is missing"))?;
    assert!(message.has_field(&explicit_zero));
    assert_eq!(
        message.get_field(&explicit_zero).as_ref(),
        &ProtobufValue::I32(0)
    );

    let ProtobufValue::Message(child) = protobuf_field(message, "child")? else {
        return Err(io::Error::other("Child field is not a Protobuf message"));
    };
    assert_eq!(
        protobuf_field(&child, "name")?,
        ProtobufValue::String(String::from("primary"))
    );
    assert_eq!(
        protobuf_field(&child, "value")?,
        ProtobufValue::Bytes(prost::bytes::Bytes::from_static(b"child-bytes"))
    );

    assert_eq!(
        protobuf_field(message, "samples")?,
        ProtobufValue::List(vec![ProtobufValue::I32(-1), ProtobufValue::I32(7)])
    );
    let ProtobufValue::List(children) = protobuf_field(message, "children")? else {
        return Err(io::Error::other(
            "Repeated child field is not a Protobuf list",
        ));
    };
    let Some(ProtobufValue::Message(repeated_child)) = children.first() else {
        return Err(io::Error::other(
            "Repeated child entry is not a Protobuf message",
        ));
    };
    assert_eq!(
        protobuf_field(repeated_child, "name")?,
        ProtobufValue::String(String::from("repeated"))
    );

    let ProtobufValue::Map(counts) = protobuf_field(message, "counts")? else {
        return Err(io::Error::other("Counts field is not a Protobuf map"));
    };
    assert_eq!(
        counts.get(&prost_reflect::MapKey::String(String::from("alpha"))),
        Some(&ProtobufValue::I32(3))
    );
    let ProtobufValue::Map(children_by_id) = protobuf_field(message, "children_by_id")? else {
        return Err(io::Error::other("Mapped child field is not a Protobuf map"));
    };
    let Some(ProtobufValue::Message(mapped_child)) =
        children_by_id.get(&prost_reflect::MapKey::U64(u64::MAX))
    else {
        return Err(io::Error::other(
            "Mapped child entry is not a Protobuf message",
        ));
    };
    assert_eq!(
        protobuf_field(mapped_child, "name")?,
        ProtobufValue::String(String::from("mapped"))
    );
    let ProtobufValue::Map(named_children) = protobuf_field(message, "named_children")? else {
        return Err(io::Error::other("Named child field is not a Protobuf map"));
    };
    let Some(ProtobufValue::Message(named_child)) =
        named_children.get(&prost_reflect::MapKey::String(String::from("primary")))
    else {
        return Err(io::Error::other(
            "Named child entry is not a Protobuf message",
        ));
    };
    assert_eq!(
        protobuf_field(named_child, "name")?,
        ProtobufValue::String(String::from("named"))
    );

    let text_choice = message
        .descriptor()
        .get_field_by_name("text_choice")
        .ok_or_else(|| io::Error::other("Text choice field is missing"))?;
    let integer_choice = message
        .descriptor()
        .get_field_by_name("integer_choice")
        .ok_or_else(|| io::Error::other("Integer choice field is missing"))?;
    assert!(!message.has_field(&text_choice));
    assert!(message.has_field(&integer_choice));
    assert_eq!(
        message.get_field(&integer_choice).as_ref(),
        &ProtobufValue::I64(9)
    );

    let message_choice_payload = built_payload(&vm, "message_choice_payload")?;
    let message_choice_payload = message_choice_payload
        .borrow::<BuiltPayload>()
        .map_err(|_| io::Error::other("Built Payload userdata type is unavailable"))?;
    let message_choice = message_choice_payload.message();
    assert!(!message_choice.has_field(&text_choice));
    assert!(!message_choice.has_field(&integer_choice));
    let child_choice = message_choice
        .descriptor()
        .get_field_by_name("child_choice")
        .ok_or_else(|| io::Error::other("Child choice field is missing"))?;
    assert!(message_choice.has_field(&child_choice));
    let ProtobufValue::Message(choice_child) = message_choice.get_field(&child_choice).into_owned()
    else {
        return Err(io::Error::other(
            "Child choice field is not a Protobuf message",
        ));
    };
    assert_eq!(
        protobuf_field(&choice_child, "name")?,
        ProtobufValue::String(String::from("choice-child"))
    );
    Ok(())
}

#[test]
fn registry_and_builder_input_errors_are_catchable_and_stable() -> io::Result<()> {
    let vm = load_vm_with_contracts(
        r#"
        local function expect_error(expected, operation)
            local ok, actual = pcall(operation)
            assert(not ok)
            assert(actual == expected)
        end

        expect_error(
            "registry sinkContractId must be a string",
            function() registry:getBuilder() end
        )
        expect_error(
            "registry method arguments are invalid",
            function()
                registry:getBuilder("com.example.lua-builder@1.0.0", "extra")
            end
        )
        expect_error(
            "registry method receiver is invalid",
            function()
                registry.getBuilder("com.example.lua-builder@1.0.0")
            end
        )
        expect_error(
            "registry sinkContractId must be normalized",
            function() registry:getBuilder("not-a-sink-contract-id") end
        )
        expect_error(
            "registry sinkContractId must be normalized",
            function() registry:getBuilder(string.char(255)) end
        )
        expect_error(
            "registry sinkContractId must be declared by the Flow",
            function() registry:getBuilder("com.example.unknown@1.0.0") end
        )

        local builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        local other_builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        local child_builder = builder:getChildBuilder()
        local detached_set_label = builder.setLabel
        detached_set_label(other_builder, "same-type")
        same_type_payload = other_builder:build()
        expect_error(
            "Payload Builder method receiver is invalid",
            function() detached_set_label(child_builder, "value") end
        )
        expect_error(
            "Payload Builder method is unavailable",
            function() builder:setMissing(1) end
        )
        expect_error(
            "Payload Builder method arguments are invalid",
            function() builder:setLabel() end
        )
        expect_error(
            "Payload Builder method arguments are invalid",
            function() builder:setLabel("value", "extra") end
        )
        expect_error(
            "Payload Builder method arguments are invalid",
            function() builder:clearLabel("extra") end
        )
        expect_error(
            "Payload Builder method arguments are invalid",
            function() builder:getChildBuilder("extra") end
        )
        expect_error(
            "Payload Builder method arguments are invalid",
            function() builder:addChildrenBuilder("extra") end
        )
        expect_error(
            "Payload Builder method arguments are invalid",
            function() builder:putCounts("key", 1, "extra") end
        )
        expect_error(
            "Payload Builder method arguments are invalid",
            function() builder:build("extra") end
        )
        expect_error(
            "Payload Builder value does not match the field type",
            function() builder:setSigned32(2147483648) end
        )
        expect_error(
            "Payload Builder value does not match the field type",
            function() builder:setUnsigned64("01") end
        )
        expect_error(
            "Payload Builder value does not match the field type",
            function() builder:setUnsigned64(1) end
        )
        expect_error(
            "Payload Builder value does not match the field type",
            function() builder:setRatio(1 / 0) end
        )
        expect_error(
            "Payload Builder value does not match the field type",
            function() builder:setLabel(string.char(255)) end
        )
        expect_error(
            "Payload Builder value does not match the field type",
            function() builder:setStatus("STATUS_MISSING") end
        )
        expect_error(
            "Payload Builder map key does not match the field type",
            function() builder:putCounts(1, 2) end
        )
        expect_error(
            "Payload Builder method is unavailable",
            function() builder:getChildBuilder():build() end
        )

        builder:setLabel("still-usable")
        recovered_payload = builder:build()

        function main(event)
        end
        "#,
        limits()?,
        lua_builder_contracts()?,
    )
    .map_err(test_error)?;

    assert_eq!(fatal_fault(&vm), None);
    let payload = built_payload(&vm, "recovered_payload")?;
    let payload = payload
        .borrow::<BuiltPayload>()
        .map_err(|_| io::Error::other("Built Payload userdata type is unavailable"))?;
    assert_eq!(
        protobuf_field(payload.message(), "label")?,
        ProtobufValue::String(String::from("still-usable"))
    );
    let same_type_payload = built_payload(&vm, "same_type_payload")?;
    let same_type_payload = same_type_payload
        .borrow::<BuiltPayload>()
        .map_err(|_| io::Error::other("Built Payload userdata type is unavailable"))?;
    assert_eq!(
        protobuf_field(same_type_payload.message(), "label")?,
        ProtobufValue::String(String::from("same-type"))
    );
    Ok(())
}

#[test]
fn replaced_nested_builder_views_fail_without_mutating_the_replacement() -> io::Result<()> {
    let vm = load_vm_with_contracts(
        r#"
        local function expect_stale(operation)
            local ok, actual = pcall(operation)
            assert(not ok)
            assert(actual == "Payload Builder method receiver is invalid")
        end

        local builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        local old_choice = builder:getChildChoiceBuilder()
        old_choice:setName("old-choice")
        builder:setIntegerChoice(7)
        expect_stale(function() old_choice:setName("unexpected-choice") end)

        recovered_payload = builder:build()

        function main(event)
        end
        "#,
        limits()?,
        lua_builder_contracts()?,
    )
    .map_err(test_error)?;

    assert_eq!(fatal_fault(&vm), None);
    let payload = built_payload(&vm, "recovered_payload")?;
    let payload = payload
        .borrow::<BuiltPayload>()
        .map_err(|_| io::Error::other("Built Payload userdata type is unavailable"))?;
    assert_eq!(
        protobuf_field(payload.message(), "integer_choice")?,
        ProtobufValue::I64(7)
    );
    Ok(())
}

#[test]
fn payload_memory_exhaustion_escapes_protected_calls() -> io::Result<()> {
    let limits = ScriptVmLimits::try_new(non_zero(1024 * 1024)?, Duration::from_secs(1))
        .map_err(io::Error::other)?;
    load_vm(
        r#"
        retained = string.rep("x", 600000)

        function main(event)
        end
        "#,
        limits,
    )
    .map_err(test_error)?;

    let Err(error) = load_vm_with_contracts(
        r#"
        local builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        local value = string.rep("x", 600000)
        local ok = pcall(function()
            builder:setLabel(value)
        end)

        function main(event)
        end
        "#,
        limits,
        lua_builder_contracts()?,
    ) else {
        return Err(io::Error::other(
            "Payload memory exhaustion should escape protected calls",
        ));
    };

    assert_eq!(error.kind(), LuaVmErrorKind::MemoryExceeded);
    Ok(())
}

#[test]
fn message_map_string_key_memory_exhaustion_escapes_protected_calls() -> io::Result<()> {
    let limits = ScriptVmLimits::try_new(non_zero(1024 * 1024)?, Duration::from_secs(1))
        .map_err(io::Error::other)?;
    load_vm(
        r#"
        retained = string.rep("x", 400000)

        function main(event)
        end
        "#,
        limits,
    )
    .map_err(test_error)?;

    let Err(error) = load_vm_with_contracts(
        r#"
        local builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        local key = string.rep("x", 400000)
        local ok = pcall(function()
            builder:putNamedChildren(key)
        end)

        function main(event)
        end
        "#,
        limits,
        lua_builder_contracts()?,
    ) else {
        return Err(io::Error::other(
            "Message map key memory exhaustion should escape protected calls",
        ));
    };

    assert_eq!(error.kind(), LuaVmErrorKind::MemoryExceeded);
    Ok(())
}

#[test]
fn garbage_collection_returns_payload_memory_to_the_vm_budget() -> io::Result<()> {
    let memory_limit = TEST_MEMORY_LIMIT_BYTES;
    let vm = load_vm_with_contracts(
        r#"
        retained_builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        retained_builder:setLabel("retained")
        retained_payload = retained_builder:build()

        function main(event)
        end
        "#,
        limits()?,
        lua_builder_contracts()?,
    )
    .map_err(test_error)?;

    let constrained_limit = current_lua_memory_limit(&vm, memory_limit)?;
    assert!(constrained_limit < memory_limit);

    vm.environment_values
        .raw_set("retained_payload", Value::Nil)
        .map_err(mlua_test_error)?;
    vm.environment_values
        .raw_set("retained_builder", Value::Nil)
        .map_err(mlua_test_error)?;
    // The first pass finalizes userdata; the second reclaims Lua values released by its fields.
    vm.lua.gc_collect().map_err(mlua_test_error)?;
    vm.lua.gc_collect().map_err(mlua_test_error)?;

    assert_eq!(current_lua_memory_limit(&vm, memory_limit)?, memory_limit);
    Ok(())
}

#[test]
fn replacing_and_clearing_payload_values_release_memory_charge() -> io::Result<()> {
    let memory_limit = TEST_MEMORY_LIMIT_BYTES;
    let vm = load_vm_with_contracts(
        r#"
        retained_builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        retained_builder:setLabel(string.rep("x", 65536))

        function main(event)
        end
        "#,
        limits()?,
        lua_builder_contracts()?,
    )
    .map_err(test_error)?;

    let large_value_limit = current_lua_memory_limit(&vm, memory_limit)?;
    evaluate_result::<bool>(&vm, r#"retained_builder:setLabel("x"); return true"#)
        .map_err(test_error)?;
    let short_value_limit = current_lua_memory_limit(&vm, memory_limit)?;
    assert!(short_value_limit > large_value_limit);

    evaluate_result::<bool>(&vm, "retained_builder:clearLabel(); return true")
        .map_err(test_error)?;
    let cleared_limit = current_lua_memory_limit(&vm, memory_limit)?;
    assert!(cleared_limit > short_value_limit);
    Ok(())
}

#[test]
#[ignore = "run explicitly to collect Payload Builder performance evidence"]
fn payload_builder_kafka_and_iotdb_benchmarks() -> io::Result<()> {
    run_payload_builder_benchmarks()
}

fn run_payload_builder_benchmarks() -> io::Result<()> {
    if std::env::var_os("CRITERION_HOME").is_none() {
        return Err(io::Error::other(
            "CRITERION_HOME must be set before running the Payload Builder benchmark",
        ));
    }
    let benchmark_limits =
        ScriptVmLimits::try_new(non_zero(64 * 1024 * 1024)?, Duration::from_secs(30))
            .map_err(io::Error::other)?;
    let kafka_vm = load_vm_with_contracts(
        r#"
        local builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        local topic = "devices/device-7/telemetry"
        local body = string.rep("x", 1024)

        function benchmarkKafka()
            builder:setLabel(topic)
            builder:setBody(body)
        end

        function main(event)
        end
        "#,
        benchmark_limits,
        lua_builder_contracts()?,
    )
    .map_err(test_error)?;
    let kafka = kafka_vm
        .environment_values
        .raw_get::<Function>("benchmarkKafka")
        .map_err(mlua_test_error)?;

    let iotdb_small_vm = load_vm_with_contracts(
        r#"
        local builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        builder:putCounts("point-1", 1)

        function benchmarkIotdb()
            builder:putCounts("point-1", 7)
        end

        function main(event)
        end
        "#,
        benchmark_limits,
        lua_builder_contracts()?,
    )
    .map_err(test_error)?;
    let iotdb_small = iotdb_small_vm
        .environment_values
        .raw_get::<Function>("benchmarkIotdb")
        .map_err(mlua_test_error)?;

    let iotdb_large_vm = load_vm_with_contracts(
        r#"
        local builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        for index = 1, 4096 do
            builder:putCounts("point-" .. index, index)
        end

        function benchmarkIotdb()
            builder:putCounts("point-2048", 7)
        end

        function main(event)
        end
        "#,
        benchmark_limits,
        lua_builder_contracts()?,
    )
    .map_err(test_error)?;
    let iotdb_large = iotdb_large_vm
        .environment_values
        .raw_get::<Function>("benchmarkIotdb")
        .map_err(mlua_test_error)?;

    let mut criterion = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_millis(200))
        .measurement_time(Duration::from_secs(1))
        .without_plots();
    criterion.bench_function(
        "payload_builder/kafka_topic_and_binary_payload",
        |bencher| {
            bencher.iter(|| black_box(kafka_vm.run_with_budget(|| kafka.call::<()>(()))));
        },
    );
    criterion.bench_function("payload_builder/iotdb_replace_one_of_1_point", |bencher| {
        bencher.iter(|| black_box(iotdb_small_vm.run_with_budget(|| iotdb_small.call::<()>(()))));
    });
    criterion.bench_function(
        "payload_builder/iotdb_replace_one_of_4096_points",
        |bencher| {
            bencher
                .iter(|| black_box(iotdb_large_vm.run_with_budget(|| iotdb_large.call::<()>(()))));
        },
    );
    criterion.final_summary();

    let small_latency = batched_latency_samples(&iotdb_small_vm, &iotdb_small)?;
    let large_latency = batched_latency_samples(&iotdb_large_vm, &iotdb_large)?;
    let small_median = percentile(&small_latency, 50)?;
    let large_median = percentile(&large_latency, 50)?;
    let small_tail = percentile(&small_latency, 99)?;
    let large_tail = percentile(&large_latency, 99)?;
    assert!(
        large_median <= small_median.saturating_mul(4),
        "Large-map median latency exceeded the four-times regression threshold"
    );
    assert!(
        large_tail <= small_tail.saturating_mul(4),
        "Large-map tail latency exceeded the four-times regression threshold"
    );

    kafka_vm
        .run_with_budget(|| kafka.call::<()>(()))
        .map_err(|error| test_error(kafka_vm.map_error(error, LuaVmErrorKind::TopLevelFailed)))?;
    iotdb_small_vm
        .run_with_budget(|| iotdb_small.call::<()>(()))
        .map_err(|error| {
            test_error(iotdb_small_vm.map_error(error, LuaVmErrorKind::TopLevelFailed))
        })?;
    iotdb_large_vm
        .run_with_budget(|| iotdb_large.call::<()>(()))
        .map_err(|error| {
            test_error(iotdb_large_vm.map_error(error, LuaVmErrorKind::TopLevelFailed))
        })?;
    Ok(())
}

fn batched_latency_samples(vm: &LuaVm, operation: &Function) -> io::Result<Vec<Duration>> {
    const BATCHES: usize = 100;
    const OPERATIONS_PER_BATCH: usize = 1_000;

    let mut samples = Vec::with_capacity(BATCHES);
    for _ in 0..BATCHES {
        let started = Instant::now();
        for _ in 0..OPERATIONS_PER_BATCH {
            vm.run_with_budget(|| operation.call::<()>(()))
                .map_err(|error| test_error(vm.map_error(error, LuaVmErrorKind::TopLevelFailed)))?;
        }
        samples.push(started.elapsed());
    }
    samples.sort_unstable();
    Ok(samples)
}

fn percentile(samples: &[Duration], percentile: usize) -> io::Result<Duration> {
    if samples.is_empty() || percentile > 100 {
        return Err(io::Error::other("Latency percentile input is invalid"));
    }
    let index = (samples.len() - 1)
        .checked_mul(percentile)
        .ok_or_else(|| io::Error::other("Latency percentile index overflowed"))?
        / 100;
    Ok(samples[index])
}

fn current_lua_memory_limit(vm: &LuaVm, probe_limit: usize) -> io::Result<usize> {
    let previous = vm
        .lua
        .set_memory_limit(probe_limit)
        .map_err(mlua_test_error)?;
    vm.lua.set_memory_limit(previous).map_err(mlua_test_error)?;
    Ok(previous)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum GeneratedJson {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(u64),
    String(String),
    Array(Vec<Self>),
    Object(BTreeMap<String, Self>),
}

impl GeneratedJson {
    fn to_serde_value(&self) -> Result<serde_json::Value, TestCaseError> {
        match self {
            Self::Null => Ok(serde_json::Value::Null),
            Self::Boolean(value) => Ok(serde_json::Value::Bool(*value)),
            Self::Integer(value) => Ok(serde_json::Value::Number((*value).into())),
            Self::Float(bits) => serde_json::Number::from_f64(f64::from_bits(*bits))
                .map(serde_json::Value::Number)
                .ok_or_else(|| TestCaseError::fail("generated float must be finite")),
            Self::String(value) => Ok(serde_json::Value::String(value.clone())),
            Self::Array(values) => values
                .iter()
                .map(Self::to_serde_value)
                .collect::<Result<Vec<_>, _>>()
                .map(serde_json::Value::Array),
            Self::Object(entries) => entries
                .iter()
                .map(|(field, value)| Ok((field.clone(), value.to_serde_value()?)))
                .collect::<Result<serde_json::Map<_, _>, _>>()
                .map(serde_json::Value::Object),
        }
    }

    fn matches_serde_value(&self, actual: &serde_json::Value) -> bool {
        match (self, actual) {
            (Self::Null, serde_json::Value::Null) => true,
            (Self::Boolean(expected), serde_json::Value::Bool(actual)) => expected == actual,
            (Self::Integer(expected), serde_json::Value::Number(actual)) => {
                actual.as_i64() == Some(*expected)
            }
            (Self::Float(expected), serde_json::Value::Number(actual)) => {
                actual.is_f64()
                    && actual
                        .as_f64()
                        .is_some_and(|actual| actual.to_bits() == *expected && actual.is_finite())
            }
            (Self::String(expected), serde_json::Value::String(actual)) => expected == actual,
            (Self::Array(expected), serde_json::Value::Array(actual)) => {
                expected.len() == actual.len()
                    && expected
                        .iter()
                        .zip(actual)
                        .all(|(expected, actual)| expected.matches_serde_value(actual))
            }
            (Self::Object(expected), serde_json::Value::Object(actual)) => {
                expected.len() == actual.len()
                    && expected.iter().all(|(field, expected)| {
                        actual
                            .get(field)
                            .is_some_and(|actual| expected.matches_serde_value(actual))
                    })
            }
            _ => false,
        }
    }
}

fn generated_json_string() -> impl Strategy<Value = String> {
    proptest::collection::vec(any::<char>(), 0..16)
        .prop_map(|characters| characters.into_iter().collect())
}

fn generated_json() -> impl Strategy<Value = GeneratedJson> {
    let leaf = prop_oneof![
        Just(GeneratedJson::Null),
        any::<bool>().prop_map(GeneratedJson::Boolean),
        any::<i64>().prop_map(GeneratedJson::Integer),
        any::<f64>()
            .prop_filter("float must be finite", |value| value.is_finite())
            .prop_map(|value| GeneratedJson::Float(value.to_bits())),
        generated_json_string().prop_map(GeneratedJson::String),
    ];

    leaf.prop_recursive(4, 64, 6, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..6).prop_map(GeneratedJson::Array),
            proptest::collection::vec((generated_json_string(), inner), 0..6)
                .prop_map(|entries| { GeneratedJson::Object(entries.into_iter().collect()) }),
        ]
    })
}

fn round_trip_through_json_api(
    generated: &GeneratedJson,
) -> Result<(Vec<u8>, Vec<u8>), TestCaseError> {
    let input_value = generated.to_serde_value()?;
    let input = serde_json::to_vec(&input_value)
        .map_err(|_| TestCaseError::fail("generated JSON must serialize"))?;
    let vm = load_vm(
        "function main(event) end",
        limits().map_err(|_| TestCaseError::fail("test limits must be valid"))?,
    )
    .map_err(|_| TestCaseError::fail("JSON test VM must load"))?;

    vm.run_with_budget(|| {
        let json = vm.environment_values.get::<Table>("json")?;
        let decode = json.get::<Function>("decode")?;
        let encode = json.get::<Function>("encode")?;
        let input = vm.lua.create_string(&input)?;
        let decoded = decode.call::<Value>(input)?;
        let canonical = encode.call::<LuaString>(decoded)?;
        let decoded_again = decode.call::<Value>(canonical.clone())?;
        let canonical_again = encode.call::<LuaString>(decoded_again)?;
        Ok((
            canonical.as_bytes().to_vec(),
            canonical_again.as_bytes().to_vec(),
        ))
    })
    .map_err(|_| TestCaseError::fail("generated JSON must round trip through the Lua API"))
}

fn exercise_generated_bytes_api(
    input: &[u8],
    prefix: &[u8],
    word: [u8; 4],
    wide_word: [u8; 8],
    crc_parameters: (u16, u16, u16, bool),
) -> Result<(), TestCaseError> {
    let (polynomial, initial, xor_out, lsb) = crc_parameters;
    let vm = load_vm(
        "function main(event) end",
        limits().map_err(|_| TestCaseError::fail("test limits must be valid"))?,
    )
    .map_err(|_| TestCaseError::fail("bytes test VM must load"))?;
    let mut positioned_word = Vec::with_capacity(prefix.len() + word.len());
    positioned_word.extend_from_slice(prefix);
    positioned_word.extend_from_slice(&word);
    let mut positioned_wide_word = Vec::with_capacity(prefix.len() + wide_word.len());
    positioned_wide_word.extend_from_slice(prefix);
    positioned_wide_word.extend_from_slice(&wide_word);
    let offset = i64::try_from(prefix.len() + 1)
        .map_err(|_| TestCaseError::fail("generated offset must fit Lua integer"))?;

    let (hex_round_trip, base64_round_trip, reads, signed_wide_reads, unsigned_wide_reads, crc) =
        vm.run_with_budget(|| {
            let bytes = vm.environment_values.get::<Table>("bytes")?;
            let to_hex = bytes.get::<Function>("to_hex")?;
            let from_hex = bytes.get::<Function>("from_hex")?;
            let to_base64 = bytes.get::<Function>("to_base64")?;
            let from_base64 = bytes.get::<Function>("from_base64")?;
            let read_u8 = bytes.get::<Function>("read_u8")?;
            let read_i8 = bytes.get::<Function>("read_i8")?;
            let read_u16_be = bytes.get::<Function>("read_u16_be")?;
            let read_u16_le = bytes.get::<Function>("read_u16_le")?;
            let read_i16_be = bytes.get::<Function>("read_i16_be")?;
            let read_i16_le = bytes.get::<Function>("read_i16_le")?;
            let read_u32_be = bytes.get::<Function>("read_u32_be")?;
            let read_u32_le = bytes.get::<Function>("read_u32_le")?;
            let read_i32_be = bytes.get::<Function>("read_i32_be")?;
            let read_i32_le = bytes.get::<Function>("read_i32_le")?;
            let read_u64_be = bytes.get::<Function>("read_u64_be")?;
            let read_u64_le = bytes.get::<Function>("read_u64_le")?;
            let read_i64_be = bytes.get::<Function>("read_i64_be")?;
            let read_i64_le = bytes.get::<Function>("read_i64_le")?;
            let crc16 = bytes.get::<Function>("crc16")?;

            let input = vm.lua.create_string(input)?;
            let hex = to_hex.call::<LuaString>(input.clone())?;
            let hex_round_trip = from_hex.call::<LuaString>(hex)?;
            let base64 = to_base64.call::<LuaString>(input.clone())?;
            let base64_round_trip = from_base64.call::<LuaString>(base64)?;

            let positioned_word = vm.lua.create_string(&positioned_word)?;
            let reads = [
                read_u8.call::<i64>((positioned_word.clone(), offset))?,
                read_i8.call::<i64>((positioned_word.clone(), offset))?,
                read_u16_be.call::<i64>((positioned_word.clone(), offset))?,
                read_u16_le.call::<i64>((positioned_word.clone(), offset))?,
                read_i16_be.call::<i64>((positioned_word.clone(), offset))?,
                read_i16_le.call::<i64>((positioned_word.clone(), offset))?,
                read_u32_be.call::<i64>((positioned_word.clone(), offset))?,
                read_u32_le.call::<i64>((positioned_word.clone(), offset))?,
                read_i32_be.call::<i64>((positioned_word.clone(), offset))?,
                read_i32_le.call::<i64>((positioned_word, offset))?,
            ];
            let positioned_wide_word = vm.lua.create_string(&positioned_wide_word)?;
            let signed_wide_reads = [
                read_i64_be.call::<i64>((positioned_wide_word.clone(), offset))?,
                read_i64_le.call::<i64>((positioned_wide_word.clone(), offset))?,
            ];
            let unsigned_wide_reads = [
                read_u64_be
                    .call::<LuaString>((positioned_wide_word.clone(), offset))?
                    .as_bytes()
                    .to_vec(),
                read_u64_le
                    .call::<LuaString>((positioned_wide_word, offset))?
                    .as_bytes()
                    .to_vec(),
            ];
            let bit_order = if lsb { "lsb" } else { "msb" };
            let crc = crc16.call::<i64>((
                input.clone(),
                i64::from(polynomial),
                i64::from(initial),
                i64::from(xor_out),
                bit_order,
            ))?;

            let _arbitrary_hex_result = from_hex.call::<LuaString>(input.clone());
            let _arbitrary_base64_result = from_base64.call::<LuaString>(input);

            Ok((
                hex_round_trip.as_bytes().to_vec(),
                base64_round_trip.as_bytes().to_vec(),
                reads,
                signed_wide_reads,
                unsigned_wide_reads,
                crc,
            ))
        })
        .map_err(|_| TestCaseError::fail("generated bytes API calls must complete"))?;

    if hex_round_trip != input {
        return Err(TestCaseError::fail("hex round trip must preserve bytes"));
    }
    if base64_round_trip != input {
        return Err(TestCaseError::fail("Base64 round trip must preserve bytes"));
    }
    let expected_reads = [
        i64::from(word[0]),
        i64::from(i8::from_be_bytes([word[0]])),
        i64::from(u16::from_be_bytes([word[0], word[1]])),
        i64::from(u16::from_le_bytes([word[0], word[1]])),
        i64::from(i16::from_be_bytes([word[0], word[1]])),
        i64::from(i16::from_le_bytes([word[0], word[1]])),
        i64::from(u32::from_be_bytes(word)),
        i64::from(u32::from_le_bytes(word)),
        i64::from(i32::from_be_bytes(word)),
        i64::from(i32::from_le_bytes(word)),
    ];
    if reads != expected_reads {
        return Err(TestCaseError::fail(
            "fixed-width reads must match explicit Rust byte order",
        ));
    }
    let expected_signed_wide_reads = [i64::from_be_bytes(wide_word), i64::from_le_bytes(wide_word)];
    if signed_wide_reads != expected_signed_wide_reads {
        return Err(TestCaseError::fail(
            "signed 64-bit reads must match explicit Rust byte order",
        ));
    }
    let expected_unsigned_wide_reads = [
        u64::from_be_bytes(wide_word).to_string().into_bytes(),
        u64::from_le_bytes(wide_word).to_string().into_bytes(),
    ];
    if unsigned_wide_reads != expected_unsigned_wide_reads {
        return Err(TestCaseError::fail(
            "unsigned 64-bit reads must use canonical decimal strings",
        ));
    }
    let expected_crc = reference_crc16(input, polynomial, initial, xor_out, lsb);
    if crc != i64::from(expected_crc) {
        return Err(TestCaseError::fail(
            "CRC16 must match the independent bit reference",
        ));
    }
    Ok(())
}

fn exercise_generated_float_reads(
    prefix: &[u8],
    single: f32,
    double: f64,
) -> Result<(), TestCaseError> {
    let vm = load_vm(
        "function main(event) end",
        limits().map_err(|_| TestCaseError::fail("test limits must be valid"))?,
    )
    .map_err(|_| TestCaseError::fail("bytes float test VM must load"))?;
    let offset = i64::try_from(prefix.len() + 1)
        .map_err(|_| TestCaseError::fail("generated offset must fit Lua integer"))?;

    let mut single_be = Vec::with_capacity(prefix.len() + size_of::<f32>());
    single_be.extend_from_slice(prefix);
    single_be.extend_from_slice(&single.to_be_bytes());
    let mut single_le = Vec::with_capacity(prefix.len() + size_of::<f32>());
    single_le.extend_from_slice(prefix);
    single_le.extend_from_slice(&single.to_le_bytes());
    let mut double_be = Vec::with_capacity(prefix.len() + size_of::<f64>());
    double_be.extend_from_slice(prefix);
    double_be.extend_from_slice(&double.to_be_bytes());
    let mut double_le = Vec::with_capacity(prefix.len() + size_of::<f64>());
    double_le.extend_from_slice(prefix);
    double_le.extend_from_slice(&double.to_le_bytes());

    let actual = vm
        .run_with_budget(|| {
            let bytes = vm.environment_values.get::<Table>("bytes")?;
            let read_f32_be = bytes.get::<Function>("read_f32_be")?;
            let read_f32_le = bytes.get::<Function>("read_f32_le")?;
            let read_f64_be = bytes.get::<Function>("read_f64_be")?;
            let read_f64_le = bytes.get::<Function>("read_f64_le")?;

            Ok([
                read_f32_be
                    .call::<f64>((vm.lua.create_string(single_be)?, offset))?
                    .to_bits(),
                read_f32_le
                    .call::<f64>((vm.lua.create_string(single_le)?, offset))?
                    .to_bits(),
                read_f64_be
                    .call::<f64>((vm.lua.create_string(double_be)?, offset))?
                    .to_bits(),
                read_f64_le
                    .call::<f64>((vm.lua.create_string(double_le)?, offset))?
                    .to_bits(),
            ])
        })
        .map_err(|_| TestCaseError::fail("generated float reads must complete"))?;
    let expected = [
        f64::from(single).to_bits(),
        f64::from(single).to_bits(),
        double.to_bits(),
        double.to_bits(),
    ];
    if actual != expected {
        return Err(TestCaseError::fail(
            "finite float reads must preserve IEEE 754 values",
        ));
    }
    Ok(())
}

fn reference_crc16(
    data: &[u8],
    polynomial: u16,
    mut register: u16,
    xor_out: u16,
    lsb: bool,
) -> u16 {
    for &value in data {
        for bit in 0..8 {
            if lsb {
                let input_bit_set = value & (1_u8 << bit) != 0;
                let feedback = (register & 1 != 0) ^ input_bit_set;
                register >>= 1;
                if feedback {
                    register ^= polynomial;
                }
            } else {
                let input_bit_set = value & (0x80_u8 >> bit) != 0;
                let feedback = (register & 0x8000 != 0) ^ input_bit_set;
                register = register.wrapping_shl(1);
                if feedback {
                    register ^= polynomial;
                }
            }
        }
    }
    register ^ xor_out
}

#[test]
fn loads_lua_5_5_and_freezes_main() -> io::Result<()> {
    let vm = load_vm(
        r#"
        assert(_VERSION == "Lua 5.5")
        state = 41

        function main(event)
            state = state + event
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    assert_eq!(
        evaluate_result::<i64>(&vm, "return state").map_err(test_error)?,
        41
    );
    let Err(error) = evaluate_result::<mlua::Value>(&vm, "pcall(function() main = 1 end)") else {
        return Err(io::Error::other("frozen main assignment should fail"));
    };
    assert_eq!(error.kind(), LuaVmErrorKind::SandboxViolation);
    assert_eq!(fatal_fault(&vm), Some(LuaVmErrorKind::SandboxViolation));
    Ok(())
}

#[test]
fn uses_official_lua_5_5_syntax_and_disables_legacy_global_compatibility() -> io::Result<()> {
    let vm = load_vm(
        r#"
        local holey_length = #{[1] = "first", [3] = "third"}
        local table_text = tostring({})
        global state, observedHoleyLength, observedTableText, main
        state = 2 ^ 3
        observedHoleyLength = holey_length
        observedTableText = table_text
        local closed <close> = nil

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    assert_eq!(
        evaluate_result::<f64>(&vm, "return state").map_err(test_error)?,
        8.0
    );
    let holey_length =
        evaluate_result::<i64>(&vm, "return observedHoleyLength").map_err(test_error)?;
    assert!(matches!(holey_length, 1 | 3));
    assert!(
        !evaluate_result::<String>(&vm, "return observedTableText")
            .map_err(test_error)?
            .is_empty()
    );

    let Err(error) = load_vm("local global = 1; function main(event) end", limits()?) else {
        return Err(io::Error::other(
            "legacy use of global as an identifier should fail",
        ));
    };
    assert_eq!(error.kind(), LuaVmErrorKind::SyntaxInvalid);
    Ok(())
}

#[test]
fn exposes_only_the_fixed_library_allowlist() -> io::Result<()> {
    let vm = load_vm(
        r#"
        assert(type(string.sub) == "function")
        assert(type(string.format) == "function")
        assert(type(string.pack) == "function")
        assert(type(string.packsize) == "function")
        assert(type(string.unpack) == "function")
        assert(string.dump == nil)
        assert(type(table.sort) == "function")
        assert(table.create == nil)
        assert(type(math.tointeger) == "function")
        assert(type(math.sqrt) == "function")
        assert(type(math.sin) == "function")
        assert(math.pi > 3.14 and math.pi < 3.15)
        assert(math.huge > 0)
        assert(type(math.random) == "function")
        assert(type(math.randomseed) == "function")
        assert(type(utf8.codepoint) == "function")
        assert(type(print) == "function")
        assert(type(setTimeout) == "function")
        assert(type(clearTimeout) == "function")
        assert(clearTimerTask == nil)
        assert(type(hasTimeout) == "function")
        assert(type(currentTimeMillis) == "function")
        assert(type(os.date) == "function")
        assert(type(os.difftime) == "function")
        assert(type(os.time) == "function")
        local osMembers = 0
        for name in pairs(os) do
            assert(name == "date" or name == "difftime" or name == "time")
            osMembers = osMembers + 1
        end
        assert(osMembers == 3)
        assert(getmetatable == nil)
        assert(setmetatable == nil)
        assert(rawget == nil)
        assert(rawset == nil)
        assert(rawlen == nil)
        assert(rawequal == nil)
        assert(coroutine == nil)
        assert(package == nil)
        assert(io == nil)
        assert(debug == nil)
        assert(load == nil)
        assert(require == nil)

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    assert_eq!(
        evaluate_result::<String>(&vm, r#"return ("payload"):sub(2)"#).map_err(test_error)?,
        "ayload"
    );
    Ok(())
}

#[test]
fn official_math_and_string_helpers_keep_lua_5_5_semantics() -> io::Result<()> {
    load_vm(
        r#"
        local mathFunctions = {
            "abs", "acos", "asin", "atan", "ceil", "cos", "deg", "exp", "floor",
            "fmod", "frexp", "ldexp", "log", "max", "min", "modf", "rad", "sin",
            "sqrt", "tan", "tointeger", "type", "ult", "random", "randomseed",
        }
        for _, name in ipairs(mathFunctions) do
            assert(type(math[name]) == "function")
        end
        assert(math.sqrt(9) == 3)
        assert(math.sin(0) == 0)
        local fraction, exponent = math.frexp(8)
        assert(fraction == 0.5 and exponent == 4)
        assert(math.ldexp(fraction, exponent) == 8)

        local seed1, seed2 = math.randomseed(20260921, 17)
        assert(seed1 == 20260921 and seed2 == 17)
        local fullInteger = math.random(0)
        local fractionFromZero = math.random()
        local bounded = math.random(7, 9)
        local fromOne = math.random(3)
        assert(math.type(fullInteger) == "integer")
        assert(math.type(fractionFromZero) == "float")
        assert(fractionFromZero >= 0 and fractionFromZero < 1)
        assert(bounded >= 7 and bounded <= 9)
        assert(fromOne >= 1 and fromOne <= 3)
        math.randomseed(20260921, 17)
        assert(math.random(0) == fullInteger)
        assert(math.random() == fractionFromZero)
        assert(math.random(7, 9) == bounded)
        assert(math.random(3) == fromOne)
        local generatedSeed1, generatedSeed2 = math.randomseed()
        assert(math.type(generatedSeed1) == "integer")
        assert(math.type(generatedSeed2) == "integer")
        assert(not pcall(math.random, 2, 1))

        assert(string.format("%s:%04x", "value", 15) == "value:000f")
        local packed = string.pack("<I4i2c3", 0x01020304, -2, "abc")
        assert(#packed == 9)
        assert(string.packsize("<I4i2c3") == 9)
        local b1, b2, b3, b4, b5, b6, b7, b8, b9 = string.byte(packed, 1, 9)
        assert(b1 == 4 and b2 == 3 and b3 == 2 and b4 == 1)
        assert(b5 == 0xfe and b6 == 0xff)
        assert(b7 == string.byte("a") and b8 == string.byte("b") and b9 == string.byte("c"))
        local unsigned, signed, text, nextPosition = string.unpack("<I4i2c3", packed)
        assert(unsigned == 0x01020304)
        assert(signed == -2)
        assert(text == "abc")
        assert(nextPosition == 10)
        assert(not pcall(string.unpack, "<I4", "abc"))
        assert(not pcall(string.packsize, "z"))

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn wall_clock_is_available_during_initialization_and_main() -> io::Result<()> {
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    let mut vm = load_vm(
        r#"
        local function readTime()
            local secondsBefore = os.time()
            local millis = currentTimeMillis()
            local secondsAfter = os.time()
            assert(math.type(millis) == "integer")
            assert(math.type(secondsBefore) == "integer")
            assert(millis // 1000 >= secondsBefore)
            assert(millis // 1000 <= secondsAfter)
            return millis
        end
        initializedAt = readTime()
        function main(event)
            processedAt = readTime()
            assert(event.timestamp == 27)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    vm.call_timer(27)
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    let after = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?;
    for name in ["initializedAt", "processedAt"] {
        let millis = evaluate_result::<i64>(&vm, &format!("return {name}")).map_err(test_error)?;
        let millis = u128::try_from(millis).map_err(io::Error::other)?;
        assert!((before.as_millis()..=after.as_millis()).contains(&millis));
    }
    Ok(())
}

#[test]
fn system_time_milliseconds_truncate_fractions_and_reject_out_of_range_times() -> io::Result<()> {
    let max_millis = u64::try_from(i64::MAX).map_err(io::Error::other)?;
    for (duration, expected) in [
        (Duration::ZERO, 0),
        (Duration::from_nanos(999_999), 0),
        (Duration::from_micros(1_234_567), 1_234),
        (Duration::from_millis(1_700_000_000_123), 1_700_000_000_123),
        (Duration::from_millis(max_millis), i64::MAX),
    ] {
        let time = UNIX_EPOCH
            .checked_add(duration)
            .ok_or_else(|| io::Error::other("System time fixture is not representable"))?;
        assert!(matches!(super::system_time_millis(time), Ok(actual) if actual == expected));
    }
    for time in [
        UNIX_EPOCH.checked_sub(Duration::from_nanos(1)),
        UNIX_EPOCH.checked_add(Duration::from_millis(max_millis + 1)),
    ] {
        let time =
            time.ok_or_else(|| io::Error::other("System time fixture is not representable"))?;
        assert!(
            matches!(super::system_time_millis(time), Err(super::LuaApiFailure::Api(message))
            if message == "currentTimeMillis system time is out of range")
        );
    }
    Ok(())
}

#[test]
fn date_time_functions_keep_lua_formatting_normalization_and_error_semantics() -> io::Result<()> {
    load_vm(
        r#"
        assert(os.date("!%Y-%m-%dT%H:%M:%SZ", 1700000000) == "2023-11-14T22:13:20Z")
        local utc = os.date("!*t", 1700000000)
        assert(utc.year == 2023 and utc.month == 11 and utc.day == 14)
        assert(utc.hour == 22 and utc.min == 13 and utc.sec == 20)
        local localDate = os.date("*t", 1700000000)
        assert(os.time(localDate) == 1700000000)
        local normalized = {year = 2024, month = 1, day = 32}
        local seconds = os.time(normalized)
        assert(normalized.year == 2024 and normalized.month == 2 and normalized.day == 1)
        assert(normalized.hour == 12 and normalized.min == 0 and normalized.sec == 0)
        assert(os.difftime(seconds + 90, seconds) == 90)
        assert(os.difftime(seconds, seconds + 90) == -90)
        assert(not pcall(os.time, {}))
        assert(not pcall(os.date, "%Q", 0))
        assert(not pcall(os.difftime, {}, 0))
        function main(event) end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn date_normalization_cannot_mutate_a_readonly_source_payload() -> io::Result<()> {
    let mut payload = DynamicMessage::new(lua_source_contract()?);
    set_protobuf_field(
        &mut payload,
        "counts",
        ProtobufValue::Map(HashMap::from([
            (MapKey::String("year".into()), ProtobufValue::I32(2024)),
            (MapKey::String("month".into()), ProtobufValue::I32(1)),
            (MapKey::String("day".into()), ProtobufValue::I32(32)),
        ])),
    )?;
    let mut vm = load_vm(
        "function main(event) pcall(os.time, event.payload.counts) end",
        limits()?,
    )
    .map_err(test_error)?;
    let Err(error) =
        call_source(&mut vm, 0, payload.encode_to_vec())?.into_result_without_emit_boundaries()
    else {
        return Err(io::Error::other(
            "Date normalization should reject read-only payloads",
        ));
    };
    assert_eq!(error.kind(), LuaVmErrorKind::SandboxViolation);
    Ok(())
}

#[test]
fn time_bindings_remain_readonly_during_main_even_inside_pcall() -> io::Result<()> {
    for mutation in ["os = {}", "os.time = nil", "currentTimeMillis = nil"] {
        let mut vm = load_vm(
            &format!("function main(event) pcall(function() {mutation} end) end"),
            limits()?,
        )
        .map_err(test_error)?;
        let Err(error) = vm.call_timer(0).into_result_without_emit_boundaries() else {
            return Err(io::Error::other(
                "Time binding mutation should invalidate the VM",
            ));
        };
        assert_eq!(error.kind(), LuaVmErrorKind::SandboxViolation);
    }
    Ok(())
}

#[test]
fn print_output_is_selected_outside_the_vm() -> io::Result<()> {
    let source = r#"
        print("initializing")

        function main(event)
            print("value", 7)
        end
    "#;
    let mut runtime_vm = load_vm(source, limits()?).map_err(test_error)?;
    let (runtime_result, _) = call_source(&mut runtime_vm, 1, empty_source_payload())?.into_parts();
    runtime_result.map_err(test_error)?;

    let (print_callback, recorded_prints) = recording_print_callback();
    let mut simulation_vm = LuaVm::load(
        source,
        limits()?,
        std::num::NonZeroU64::new(262_144)
            .ok_or_else(|| std::io::Error::other("test record limit must be non-zero"))?,
        lua_source_contract()?,
        HashMap::new(),
        print_callback,
        || false,
    )
    .map_err(test_error)?;
    recorded_prints.borrow_mut().clear();
    let (simulation_result, _) =
        call_source(&mut simulation_vm, 1, empty_source_payload())?.into_parts();
    simulation_result.map_err(test_error)?;
    let simulation_prints = recorded_prints.borrow();
    assert_eq!(simulation_prints.len(), 1);
    assert_eq!(simulation_prints[0].text.as_ref(), "value\t7");
    Ok(())
}

#[test]
fn print_callback_can_skip_lazy_rendering() -> io::Result<()> {
    let renders = Arc::new(AtomicUsize::new(0));
    let mut vm = LuaVm::load(
        r#"
        function main(event)
            print(render_probe)
        end
        "#,
        limits()?,
        std::num::NonZeroU64::new(262_144)
            .ok_or_else(|| std::io::Error::other("test record limit must be non-zero"))?,
        lua_source_contract()?,
        HashMap::new(),
        Some(Box::new(|_record: LuaPrintRecord| Ok(()))),
        || false,
    )
    .map_err(test_error)?;
    vm.environment_values
        .raw_set("render_probe", PrintRenderProbe(Arc::clone(&renders)))
        .map_err(|error| io::Error::other(error.to_string()))?;

    call_source(&mut vm, 1, empty_source_payload())?
        .into_parts()
        .0
        .map_err(test_error)?;
    assert_eq!(renders.load(Ordering::Relaxed), 0);
    Ok(())
}

#[test]
fn rendered_prints_are_utf8_safe_and_bounded() -> io::Result<()> {
    let (print_callback, recorded_prints) = recording_print_callback();
    let mut vm = LuaVm::load(
        r#"
        function main(event)
            print(string.rep("x", 20000))
            print(string.char(255))
            print(string.rep(string.char(255), 20000))
        end
        "#,
        limits()?,
        std::num::NonZeroU64::new(262_144)
            .ok_or_else(|| std::io::Error::other("test record limit must be non-zero"))?,
        lua_source_contract()?,
        HashMap::new(),
        print_callback,
        || false,
    )
    .map_err(test_error)?;

    call_source(&mut vm, 1, empty_source_payload())?
        .into_parts()
        .0
        .map_err(test_error)?;
    let prints = recorded_prints.borrow();
    assert_eq!(prints.len(), 3);
    let bounded = &prints[0];
    assert_eq!(bounded.text.len(), 16 * 1024);
    assert!(bounded.truncated);
    assert!(!bounded.invalid_utf8);
    let invalid = &prints[1];
    assert_eq!(invalid.text.as_ref(), "�");
    assert!(!invalid.truncated);
    assert!(invalid.invalid_utf8);
    let bounded_invalid = &prints[2];
    assert!(bounded_invalid.text.len() <= 16 * 1024);
    assert!(bounded_invalid.truncated);
    assert!(bounded_invalid.invalid_utf8);
    Ok(())
}

#[test]
fn permits_script_owned_global_state() -> io::Result<()> {
    let vm = load_vm(
        r#"
        counter = 1
        counter = counter + 1
        other_counter = 10
        other_counter = other_counter + 1

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    assert_eq!(
        evaluate_result::<i64>(&vm, "counter = counter + 1; return counter").map_err(test_error)?,
        3
    );
    assert_eq!(
        evaluate_result::<i64>(&vm, "return other_counter").map_err(test_error)?,
        11
    );
    Ok(())
}

#[test]
fn pcall_preserves_ordinary_lua_error_values() -> io::Result<()> {
    load_vm(
        r#"
        local pcall_marker = {}
        local pcall_ok, pcall_error = pcall(function()
            error(pcall_marker)
        end)
        assert(not pcall_ok)
        assert(pcall_error == pcall_marker)

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn xpcall_preserves_ordinary_lua_error_values() -> io::Result<()> {
    load_vm(
        r#"
        local original_error = {}
        local xpcall_marker = {}
        local xpcall_ok, xpcall_error = xpcall(
            function()
                error(original_error)
            end,
            function(error)
                assert(error == original_error)
                return xpcall_marker
            end
        )
        assert(not xpcall_ok)
        assert(xpcall_error == xpcall_marker)

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn rejects_missing_and_non_function_main() -> io::Result<()> {
    let Err(missing) = load_vm("state = 1", limits()?) else {
        return Err(io::Error::other("missing main should fail"));
    };
    assert_eq!(missing.kind(), LuaVmErrorKind::MainMissing);

    let Err(not_function) = load_vm("main = 1", limits()?) else {
        return Err(io::Error::other("non-function main should fail"));
    };
    assert_eq!(not_function.kind(), LuaVmErrorKind::MainNotFunction);
    Ok(())
}

#[test]
fn rejects_invalid_syntax_without_exposing_compiler_text() -> io::Result<()> {
    let Err(error) = load_vm("function main(", limits()?) else {
        return Err(io::Error::other("invalid syntax should fail"));
    };

    assert_eq!(error.kind(), LuaVmErrorKind::SyntaxInvalid);
    assert_eq!(error.to_string(), "Lua source syntax is invalid");
    assert!(error.source().is_some());
    assert_eq!(
        format!("{error:?}"),
        "LuaVmError { kind: SyntaxInvalid, source: Some(\"[REDACTED]\") }"
    );
    Ok(())
}

#[test]
fn rejects_load_time_runtime_failures() -> io::Result<()> {
    let Err(error) = load_vm(
        r#"
        error("load failed")
        function main(event)
        end
        "#,
        limits()?,
    ) else {
        return Err(io::Error::other("top-level failure should reject program"));
    };

    assert_eq!(error.kind(), LuaVmErrorKind::TopLevelFailed);
    assert_eq!(error.to_string(), "Lua top-level initialization failed");
    assert_eq!(error.diagnostic_detail(), "load failed");
    Ok(())
}

#[test]
fn diagnostic_detail_keeps_only_the_message_the_script_author_wrote() {
    assert_eq!(
        lua_message_text(
            "[string \"tenon_document.process.script\"]:4: qa-intentional\nstack traceback:\n\t[C]: in ?"
        ),
        "qa-intentional"
    );
    assert_eq!(
        lua_message_text(
            "[string \"tenon_document.process.script\"]:4: first\nsecond\nstack traceback:\n\t[C]: in ?"
        ),
        "first\nsecond"
    );
    assert_eq!(lua_message_text("no position prefix"), "no position prefix");
    assert_eq!(
        lua_message_text("kafka:9092: timed out"),
        "kafka:9092: timed out"
    );
}

#[test]
fn rejects_emit_during_top_level_initialization() -> io::Result<()> {
    for source in [
        "emit(); function main(event) end",
        "pcall(emit); function main(event) end",
    ] {
        let Err(error) = load_vm(source, limits()?) else {
            return Err(io::Error::other("top-level emit should fail during load"));
        };
        assert_eq!(error.kind(), LuaVmErrorKind::SandboxViolation);
    }
    Ok(())
}

#[test]
fn top_level_zero_timeout_waits_for_the_next_event_loop_turn() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        local calls = 0
        assert(not hasTimeout())
        setTimeout(25)
        assert(hasTimeout())
        clearTimeout()
        assert(not hasTimeout())
        setTimeout(0)
        assert(hasTimeout())

        function main(event)
            calls = calls + 1
            assert(event.type == "timer")
            assert(not hasTimeout())
        end

        function callCount()
            return calls
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    assert_eq!(
        evaluate_result::<i64>(&vm, "return callCount()").map_err(test_error)?,
        0
    );
    let Some(schedule) = vm.scheduled_timer() else {
        return Err(io::Error::other("top-level timeout schedule is missing"));
    };
    assert_eq!(schedule.delay, Duration::ZERO);
    assert!(lua_has_timeout(&vm).map_err(test_error)?);
    assert_eq!(vm.scheduled_timer(), Some(schedule));

    let event = vm
        .begin_timer()
        .map_err(|_| io::Error::other("timer missing"))?;
    assert!(!lua_has_timeout(&vm).map_err(test_error)?);
    assert!(vm.scheduled_timer().is_none());
    assert!(vm.begin_timer().is_err());
    vm.call_timer_event(1, event)
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    assert_eq!(
        evaluate_result::<i64>(&vm, "return callCount()").map_err(test_error)?,
        1
    );
    Ok(())
}

#[test]
fn timeout_delay_starts_at_the_lua_api_call() -> io::Result<()> {
    let before_load = Instant::now();
    let vm = load_vm("setTimeout(25); function main(event) end", limits()?).map_err(test_error)?;
    let after_load = Instant::now();

    let Some(schedule) = vm.scheduled_timer() else {
        return Err(io::Error::other("top-level timeout schedule is missing"));
    };
    assert!(schedule.scheduled_at >= before_load);
    assert!(schedule.scheduled_at <= after_load);
    assert_eq!(schedule.delay, Duration::from_millis(25));
    Ok(())
}

#[test]
fn every_new_vm_schedules_its_own_top_level_timeout() -> io::Result<()> {
    let source = "setTimeout(0); function main(event) end";
    let first = load_vm(source, limits()?).map_err(test_error)?;
    let second = load_vm(source, limits()?).map_err(test_error)?;

    assert_eq!(
        first.scheduled_timer().map(|schedule| schedule.delay),
        Some(Duration::ZERO)
    );
    assert_eq!(
        second.scheduled_timer().map(|schedule| schedule.delay),
        Some(Duration::ZERO)
    );
    Ok(())
}

#[test]
fn timer_apis_share_one_slot_in_top_level_and_source_main() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        clearTimeout()

        function main(event)
            if event.timestamp == 1 then
                assert(not hasTimeout())
                setTimeout(10)
                assert(hasTimeout())
                setTimeout(20)
                assert(hasTimeout())
            elseif event.timestamp == 2 then
                assert(hasTimeout())
                clearTimeout()
                assert(not hasTimeout())
            end
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    assert!(vm.scheduled_timer().is_none());
    call_source(&mut vm, 1, empty_source_payload())?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    let Some(schedule) = vm.scheduled_timer() else {
        return Err(io::Error::other("main timeout schedule is missing"));
    };
    assert_eq!(schedule.delay, Duration::from_millis(20));
    assert!(lua_has_timeout(&vm).map_err(test_error)?);

    call_source(&mut vm, 2, empty_source_payload())?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    assert!(vm.scheduled_timer().is_none());
    assert!(!lua_has_timeout(&vm).map_err(test_error)?);
    Ok(())
}

#[test]
fn latest_timeout_schedule_is_the_only_active_timer() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        function main(event)
            setTimeout(event.timestamp)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    call_source(&mut vm, 1, empty_source_payload())?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    let Some(first_schedule) = vm.scheduled_timer() else {
        return Err(io::Error::other("first timeout schedule is missing"));
    };

    call_source(&mut vm, 2, empty_source_payload())?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    let Some(second_schedule) = vm.scheduled_timer() else {
        return Err(io::Error::other("second timeout schedule is missing"));
    };

    assert_eq!(first_schedule.delay, Duration::from_millis(1));
    assert_eq!(second_schedule.delay, Duration::from_millis(2));
    assert!(vm.begin_timer().is_ok());
    assert!(!lua_has_timeout(&vm).map_err(test_error)?);
    Ok(())
}

#[test]
fn set_timeout_accepts_all_non_negative_lua_integers() -> io::Result<()> {
    let vm = load_vm(
        r#"
        local cases = {
            {value = -1, expected = "setTimeout delay must be a non-negative integer"},
            {value = 1.0, expected = "setTimeout delay must be a non-negative integer"},
            {value = "1", expected = "setTimeout delay must be a non-negative integer"}
        }
        for _, case in ipairs(cases) do
            local ok, reason = pcall(setTimeout, case.value)
            assert(not ok)
            assert(reason == case.expected)
        end
        local ok, reason = pcall(setTimeout)
        assert(not ok)
        assert(reason == "setTimeout delay must be a non-negative integer")

        setTimeout(0)
        assert(hasTimeout())
        local max_ok, max_reason = pcall(setTimeout, 9223372036854775807)
        if not max_ok then
            assert(max_reason == "setTimeout delay is out of range")
            setTimeout(0)
        end
        assert(hasTimeout())

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    let Some(schedule) = vm.scheduled_timer() else {
        return Err(io::Error::other("maximum timeout schedule is missing"));
    };
    assert!(
        schedule.delay == Duration::ZERO
            || schedule.delay == Duration::from_millis(i64::MAX.unsigned_abs())
    );
    Ok(())
}

#[test]
fn named_timers_replace_clear_and_expose_their_id() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        setTimeout(20, "flush")
        setTimeout(10, "heartbeat")
        assert(hasTimeout("flush"))
        assert(hasTimeout("heartbeat"))
        clearTimeout("flush")
        assert(not hasTimeout("flush"))
        function main(event)
            assert(event.type == "timer")
            assert(event.id == "heartbeat")
            assert(event.eligibleAt >= 10)
            assert(not hasTimeout("heartbeat"))
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    let event = vm
        .begin_timer()
        .map_err(|_| io::Error::other("timer missing"))?;
    vm.call_timer_event(10, event)
        .into_result_without_emit_boundaries()
        .map_err(test_error)
}

#[test]
fn zero_delay_timers_are_dispatched_in_registration_order() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        setTimeout(0, "first")
        setTimeout(0, "second")
        setTimeout(0)
        function main(event)
            assert(event.type == "timer")
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    let first = vm
        .begin_timer()
        .map_err(|_| io::Error::other("first timer missing"))?;
    assert_eq!(first.id.as_deref(), Some("first"));
    vm.call_timer_event(0, first)
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    let second = vm
        .begin_timer()
        .map_err(|_| io::Error::other("second timer missing"))?;
    assert_eq!(second.id.as_deref(), Some("second"));
    vm.call_timer_event(0, second)
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    let anonymous = vm
        .begin_timer()
        .map_err(|_| io::Error::other("anonymous timer missing"))?;
    assert_eq!(anonymous.id, None);
    Ok(())
}

#[test]
fn named_timer_ids_are_validated_without_changing_existing_timers() -> io::Result<()> {
    load_vm(
        r#"
        local valid = string.rep("é", 64)
        setTimeout(0, valid)
        setTimeout(0)
        for _, id in ipairs({"", true, 1, string.rep("é", 65), string.char(255)}) do
            for _, call in ipairs({
                function() setTimeout(0, id) end,
                function() clearTimeout(id) end,
                function() hasTimeout(id) end
            }) do
                local ok, reason = pcall(call)
                assert(not ok)
                assert(reason == "timer id must be a non-empty string of at most 128 bytes")
            end
        end
        assert(hasTimeout(valid))
        assert(hasTimeout(nil))
        clearTimeout(valid)
        assert(not hasTimeout(valid))
        assert(hasTimeout())
        assert(select('#', clearTimeout(nil)) == 0)
        assert(select('#', clearTimeout("missing")) == 0)
        assert(not hasTimeout())
        function main(event) end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn timer_memory_charge_is_fixed_per_pending_timer_plus_utf8_id_bytes() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        setTimeout(0, "é")
        function main(event)
            setTimeout(0)
            setTimeout(0, "é")
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    let budget = Rc::clone(&vm.native_memory_budget);
    // The public charge is 2 KiB per timer plus three copies of its UTF-8 id.
    assert_eq!(budget.used_bytes(), 2048 + 3 * 2);
    call_source(&mut vm, 1, empty_source_payload())?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    assert_eq!(budget.used_bytes(), 2 * 2048 + 3 * 2);
    let anonymous = vm
        .begin_timer()
        .map_err(|_| io::Error::other("anonymous timer missing"))?;
    assert_eq!(anonymous.id, None);
    assert_eq!(budget.used_bytes(), 2048 + 3 * 2);
    let named = vm
        .begin_timer()
        .map_err(|_| io::Error::other("named timer missing"))?;
    assert_eq!(named.id.as_deref(), Some("é"));
    assert_eq!(budget.used_bytes(), 0);
    Ok(())
}

#[test]
fn timer_memory_is_reused_on_replace_and_released_on_clear_dispatch_and_drop() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        function main(event)
            for i = 1, 128 do setTimeout(0, tostring(i)) end
            if event.timestamp == 2 then
                for i = 1, 128 do clearTimeout(tostring(i)) end
            end
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    let budget = Rc::clone(&vm.native_memory_budget);
    assert_eq!(budget.used_bytes(), 0);
    call_source(&mut vm, 1, empty_source_payload())?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    let allocated = budget.used_bytes();
    assert!(allocated > 0);
    call_source(&mut vm, 1, empty_source_payload())?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    assert_eq!(budget.used_bytes(), allocated);
    call_source(&mut vm, 2, empty_source_payload())?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    assert_eq!(budget.used_bytes(), 0);
    call_source(&mut vm, 1, empty_source_payload())?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    for _ in 0..128 {
        vm.begin_timer()
            .map_err(|_| io::Error::other("timer missing"))?;
    }
    assert_eq!(budget.used_bytes(), 0);
    call_source(&mut vm, 1, empty_source_payload())?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    drop(vm);
    assert_eq!(budget.used_bytes(), 0);
    Ok(())
}

#[test]
fn timer_memory_exhaustion_escapes_protected_calls() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        function main(event)
            pcall(function()
                for i = 1, 100000 do setTimeout(0, tostring(i)) end
            end)
            error("memory failure was swallowed")
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    let Err(error) =
        call_source(&mut vm, 1, empty_source_payload())?.into_result_without_emit_boundaries()
    else {
        return Err(io::Error::other("timer budget must be enforced"));
    };
    assert_eq!(error.kind(), LuaVmErrorKind::MemoryExceeded);
    assert!(vm.next_timer_event().is_none());
    Ok(())
}

#[test]
fn timer_delay_max_integer_rejects_overflow_and_preserves_the_old_timer() -> io::Result<()> {
    let origin = Instant::now()
        .checked_sub(Duration::from_millis(2))
        .ok_or_else(|| io::Error::other("test origin out of range"))?;
    let vm = LuaVm::load_observed_at(
        r#"
        setTimeout(7)
        local ok, message = pcall(setTimeout, 9223372036854775807)
        assert(not ok)
        assert(message == "setTimeout delay is out of range")
        assert(hasTimeout())
        function main(event) end
        "#,
        limits()?,
        NonZeroU64::new(262_144).ok_or_else(|| io::Error::other("record limit"))?,
        lua_source_contract()?,
        HashMap::new(),
        None,
        || false,
        None,
        origin,
        Rc::new(std::cell::Cell::new(0)),
    )
    .map_err(test_error)?;
    assert_eq!(
        vm.scheduled_timer().map(|timer| timer.delay),
        Some(Duration::from_millis(7))
    );
    Ok(())
}

#[test]
fn single_value_apis_ignore_extra_lua_arguments() -> io::Result<()> {
    let vm = load_vm(
        r#"
        assert(json.decode("true", "ignored"))
        assert(json.encode(1, "ignored") == "1")
        setTimeout(7, "ignored")

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    let Some(schedule) = vm.scheduled_timer() else {
        return Err(io::Error::other("timeout schedule is missing"));
    };
    assert_eq!(schedule.delay, Duration::from_millis(7));
    Ok(())
}

#[test]
fn failed_main_does_not_expose_its_timer_schedule() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        setTimeout(0)

        function main(event)
            setTimeout(10)
            error("main failed")
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    let Some(_schedule) = vm.scheduled_timer() else {
        return Err(io::Error::other("startup timeout schedule is missing"));
    };
    let event = vm
        .begin_timer()
        .map_err(|_| io::Error::other("timer missing"))?;
    let Err(error) = vm
        .call_timer_event(1, event)
        .into_result_without_emit_boundaries()
    else {
        return Err(io::Error::other("failing main should be rejected"));
    };
    assert_eq!(error.kind(), LuaVmErrorKind::MainFailed);
    assert!(vm.scheduled_timer().is_none());
    Ok(())
}

#[test]
fn protects_predefined_globals_and_libraries() -> io::Result<()> {
    for source in [
        "_VERSION = \"changed\"; function main(event) end",
        "_G._VERSION = \"changed\"; function main(event) end",
        "setTimeout = nil; function main(event) end",
        "clearTimeout = nil; function main(event) end",
        "hasTimeout = nil; function main(event) end",
        "currentTimeMillis = nil; function main(event) end",
        "os = {}; function main(event) end",
        "os.date = nil; function main(event) end",
        "os.difftime = nil; function main(event) end",
        "os.time = nil; function main(event) end",
        "string.sub = nil; function main(event) end",
        "string.pack = nil; function main(event) end",
        "math.sin = nil; function main(event) end",
        "math.pi = 3; function main(event) end",
        "math.random = nil; function main(event) end",
        "math.randomseed = nil; function main(event) end",
        "table.insert(string, \"changed\"); function main(event) end",
        "table.move({\"changed\"}, 1, 1, 1, string); function main(event) end",
        "local _, state = pairs(string); state.sub = nil; function main(event) end",
    ] {
        let Err(error) = load_vm(source, limits()?) else {
            return Err(io::Error::other("protected value mutation should fail"));
        };
        assert_eq!(error.kind(), LuaVmErrorKind::SandboxViolation);
    }
    Ok(())
}

#[test]
fn rejects_replacing_the_chunk_environment() -> io::Result<()> {
    let Err(error) = load_vm("_ENV = {}; function main(event) end", limits()?) else {
        return Err(io::Error::other(
            "chunk environment replacement should fail",
        ));
    };
    assert_eq!(error.kind(), LuaVmErrorKind::SyntaxInvalid);

    let Err(error) = load_vm("_G._ENV = {}; function main(event) end", limits()?) else {
        return Err(io::Error::other("environment name mutation should fail"));
    };
    assert_eq!(error.kind(), LuaVmErrorKind::SandboxViolation);
    Ok(())
}

#[test]
fn ordinary_tables_cover_the_supported_business_operations() -> io::Result<()> {
    load_vm(
        r#"
        local state = {
            value = 41,
            __index = "ordinary data",
            __gc = "ordinary data"
        }
        state.value = state.value + 1
        assert(state.value == 42)
        assert(state.__index == "ordinary data")
        assert(state.__gc == "ordinary data")

        local values = {10, 20, 30}
        assert(#values == 3)
        assert(state == state)
        assert(state ~= {})

        local call_ok = pcall(function()
            return state()
        end)
        assert(not call_ok)

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn json_namespace_exposes_the_fixed_surface() -> io::Result<()> {
    load_vm(
        r#"
        assert(type(json) == "table")
        assert(type(json.decode) == "function")
        assert(type(json.encode) == "function")
        assert(type(json.array) == "function")
        assert(type(json.null) == "userdata")

        local fields = {}
        for name in pairs(json) do
            fields[#fields + 1] = name
        end
        table.sort(fields)
        assert(table.concat(fields, ",") == "array,decode,encode,null")

        local null_write_ok = pcall(function()
            json.null.value = true
        end)
        assert(not null_write_ok)
        assert(json.decode("null") == json.null)

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn json_decode_preserves_json_shapes_and_number_kinds() -> io::Result<()> {
    load_vm(
        r#"
        local value = json.decode(
            '{"array":[1,1.0,null],"integer":9223372036854775807,"object":{"ok":true}}'
        )
        assert(math.type(value.array[1]) == "integer")
        assert(math.type(value.array[2]) == "float")
        assert(value.array[3] == json.null)
        assert(json.decode("null") == json.null)
        assert(value.integer == 9223372036854775807)
        assert(json.decode("-9223372036854775808") == math.mininteger)
        assert(json.decode("9007199254740993") == 9007199254740993)
        assert(math.type(json.decode("1e0")) == "float")
        assert(json.encode(json.decode("-0.0")) == "-0.0")
        assert(value.object.ok)
        assert(json.decode('"\\uD83D\\uDE00"') == utf8.char(128512))

        table.remove(value.array, 3)
        table.remove(value.array, 2)
        table.remove(value.array, 1)
        assert(json.encode(value.array) == "[]")

        local array = json.array()
        assert(json.encode(array) == "[]")
        array[1] = "first"
        assert(json.encode(array) == '["first"]')

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn json_encode_produces_one_canonical_text() -> io::Result<()> {
    load_vm(
        r#"
        assert(json.encode({second = 2, first = 1}) == '{"first":1,"second":2}')
        assert(
            json.encode({outer = {second = 2, first = 1}, first = 0})
                == '{"first":0,"outer":{"first":1,"second":2}}'
        )
        assert(json.encode({1, json.null, 3}) == '[1,null,3]')
        assert(json.encode(json.null) == 'null')
        assert(json.encode({}) == "{}")
        assert(json.encode("\n") == '"\\n"')
        assert(json.encode(1) == "1")
        assert(json.encode(1.0) == "1.0")
        assert(json.encode(-0.0) == "-0.0")
        assert(json.encode(0.01) == "0.01")
        assert(json.encode(120.0) == "120.0")
        assert(json.encode(1000.0) == "1e3")
        assert(json.encode(0.000001) == "1e-6")
        assert(json.encode(5e-324) == "5e-324")
        assert(
            json.encode(2.2250738585072014e-308)
                == "2.2250738585072014e-308"
        )
        assert(
            json.encode(1.7976931348623157e308)
                == "1.7976931348623157e308"
        )

        local e_acute = utf8.char(233)
        local han = utf8.char(20013)
        local emoji = utf8.char(128512)
        assert(
            json.encode({
                [emoji] = 3,
                [han] = 2,
                [e_acute] = 1,
                z = 0
            })
                == '{"z":0,"' .. e_acute .. '":1,"'
                    .. han .. '":2,"' .. emoji .. '":3}'
        )

        local shared = {value = 1}
        assert(
            json.encode({second = shared, first = shared})
                == '{"first":{"value":1},"second":{"value":1}}'
        )

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn json_decode_rejects_non_json_inputs_with_stable_catchable_errors() -> io::Result<()> {
    load_vm(
        r#"
        local cases = {
            {
                input = true,
                expected = "json.decode input must be a string"
            },
            {
                input = string.char(255),
                expected = "json.decode input must be valid UTF-8"
            },
            {
                input = '{"value":1,}',
                expected = "json.decode input must be valid JSON"
            },
            {
                input = '{"value":1} {"other":2}',
                expected = "json.decode input must be valid JSON"
            },
            {
                input = '{"value":/* comment */1}',
                expected = "json.decode input must be valid JSON"
            },
            {
                input = '{value:1}',
                expected = "json.decode input must be valid JSON"
            },
            {
                input = '{"first":1 "second":2}',
                expected = "json.decode input must be valid JSON"
            },
            {
                input = "{'value':1}",
                expected = "json.decode input must be valid JSON"
            },
            {
                input = "0x10",
                expected = "json.decode input must be valid JSON"
            },
            {
                input = "+1",
                expected = "json.decode input must be valid JSON"
            },
            {
                input = "01",
                expected = "json.decode input must be valid JSON"
            },
            {
                input = "1.",
                expected = "json.decode input must be valid JSON"
            },
            {
                input = ".1",
                expected = "json.decode input must be valid JSON"
            },
            {
                input = "NaN",
                expected = "json.decode input must be valid JSON"
            },
            {
                input = "Infinity",
                expected = "json.decode input must be valid JSON"
            },
            {
                input = "",
                expected = "json.decode input must be valid JSON"
            },
            {
                input = " \t\r\n",
                expected = "json.decode input must be valid JSON"
            },
            {
                input = '"\\uD800"',
                expected = "json.decode input must be valid JSON"
            },
            {
                input = '{"value":1,"\\u0076alue":2}',
                expected = "json.decode object field names must be unique"
            },
            {
                input = '{"nested":{"value":1,"value":2}}',
                expected = "json.decode object field names must be unique"
            },
            {
                input = "9223372036854775808",
                expected = "json.decode integer must fit signed 64-bit"
            },
            {
                input = "-9223372036854775809",
                expected = "json.decode integer must fit signed 64-bit"
            },
            {
                input = "1e400",
                expected = "json.decode number must be finite"
            }
        }

        for _, case in ipairs(cases) do
            local ok, error_value = pcall(json.decode, case.input)
            assert(not ok)
            assert(error_value == case.expected)
        end
        assert(json.decode("1") == 1)

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn json_encode_rejects_unrepresentable_lua_values_without_poisoning_the_vm() -> io::Result<()> {
    load_vm(
        r#"
        local cycle = {}
        cycle.self = cycle

        local indirect_cycle = {}
        local indirect_child = {parent = indirect_cycle}
        indirect_cycle.child = indirect_child

        local object_array_cycle = {}
        local cycle_array = {object_array_cycle}
        object_array_cycle.array = cycle_array

        local invalid_values = {
            function() end,
            0 / 0,
            1 / 0,
            string.char(255),
            {[2] = "sparse"},
            {[1] = "array", name = "mixed"},
            {[0] = "zero"},
            {[-1] = "negative"},
            {[1.5] = "fractional"},
            {[true] = "boolean"},
            {[string.char(255)] = "invalid key"},
            cycle,
            indirect_cycle,
            object_array_cycle,
            string
        }

        local marked_array = json.array()
        marked_array.name = "invalid"
        invalid_values[#invalid_values + 1] = marked_array

        for _, value in ipairs(invalid_values) do
            local ok, error_value = pcall(json.encode, value)
            assert(not ok)
            assert(error_value == "json.encode value is not representable as JSON")
        end

        local nil_ok, nil_error = pcall(json.encode, nil)
        assert(not nil_ok)
        assert(nil_error == "json.encode value is not representable as JSON")
        assert(json.encode({ok = true}) == '{"ok":true}')

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn json_namespace_mutation_is_a_fatal_sandbox_fault() -> io::Result<()> {
    for source in [
        "json = {}; function main(event) end",
        "json.encode = nil; function main(event) end",
        "json.null = nil; function main(event) end",
    ] {
        let Err(error) = load_vm(source, limits()?) else {
            return Err(io::Error::other("JSON namespace mutation should fail"));
        };
        assert_eq!(error.kind(), LuaVmErrorKind::SandboxViolation);
    }
    Ok(())
}

#[test]
fn json_memory_exhaustion_escapes_protected_calls() -> io::Result<()> {
    let limits = ScriptVmLimits::try_new(non_zero(1024 * 1024)?, Duration::from_secs(1))
        .map_err(io::Error::other)?;
    let Err(error) = load_vm(
        r#"
        function main(event)
        end

        local value = "leaf"
        for _ = 1, 16 do
            value = {left = value, right = value}
        end
        local ok = pcall(json.encode, value)
        "#,
        limits,
    ) else {
        return Err(io::Error::other(
            "JSON allocation failure should escape protected calls",
        ));
    };

    assert_eq!(error.kind(), LuaVmErrorKind::MemoryExceeded);
    Ok(())
}

#[test]
fn json_decode_memory_exhaustion_escapes_protected_calls() -> io::Result<()> {
    let limits = ScriptVmLimits::try_new(non_zero(2 * 1024 * 1024)?, Duration::from_secs(1))
        .map_err(io::Error::other)?;
    load_vm(
        r#"
        function main(event)
        end

        local input = "[" .. string.rep("0,", 200000) .. "0]"
        assert(#input == 400003)
        "#,
        limits,
    )
    .map_err(test_error)?;

    let Err(error) = load_vm(
        r#"
        function main(event)
        end

        local input = "[" .. string.rep("0,", 200000) .. "0]"
        local ok = pcall(json.decode, input)
        "#,
        limits,
    ) else {
        return Err(io::Error::other(
            "JSON decode allocation failure should escape protected calls",
        ));
    };

    assert_eq!(error.kind(), LuaVmErrorKind::MemoryExceeded);
    Ok(())
}

#[test]
fn json_calls_cannot_catch_the_elapsed_cpu_limit() -> io::Result<()> {
    let mut vm = load_vm("function main(event) end", limits()?).map_err(test_error)?;
    vm.environment_values
        .raw_set("json_cpu_call_completed", false)
        .map_err(|_| io::Error::other("JSON CPU marker must initialize"))?;
    let function = vm
        .lua
        .load(
            r#"
            local ok = pcall(json.decode, "0")
            json_cpu_call_completed = true
            return ok
            "#,
        )
        .set_mode(mlua::chunk::ChunkMode::Text)
        .set_environment(vm.environment.clone())
        .into_function()
        .map_err(|_| io::Error::other("JSON CPU test chunk must compile"))?;
    vm.cpu_time_limit = Duration::from_nanos(1);

    let Err(source) = vm.run_with_budget(|| function.call::<Value>(())) else {
        return Err(io::Error::other(
            "JSON call should not catch the elapsed CPU limit",
        ));
    };
    let error = vm.map_error(source, LuaVmErrorKind::TopLevelFailed);

    assert_eq!(error.kind(), LuaVmErrorKind::CpuTimeExceeded);
    let completed = vm
        .environment_values
        .raw_get::<bool>("json_cpu_call_completed")
        .map_err(|_| io::Error::other("JSON CPU marker must remain readable"))?;
    assert!(!completed);
    Ok(())
}

#[test]
fn bytes_namespace_exposes_the_fixed_surface() -> io::Result<()> {
    load_vm(
        r#"
        assert(type(bytes) == "table")

        local expected = {
            "bcd_to_string",
            "byte",
            "crc16",
            "from_base64",
            "from_hex",
            "len",
            "read_f32_be",
            "read_f32_le",
            "read_f64_be",
            "read_f64_le",
            "read_i16_be",
            "read_i16_le",
            "read_i32_be",
            "read_i32_le",
            "read_i64_be",
            "read_i64_le",
            "read_i8",
            "read_u16_be",
            "read_u16_le",
            "read_u32_be",
            "read_u32_le",
            "read_u64_be",
            "read_u64_le",
            "read_u8",
            "slice",
            "to_base64",
            "to_hex"
        }
        local actual = {}
        for name, value in pairs(bytes) do
            assert(type(value) == "function")
            actual[#actual + 1] = name
        end
        table.sort(actual)
        assert(#actual == #expected)
        for index, name in ipairs(expected) do
            assert(actual[index] == name)
        end

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn bytes_reads_slices_and_fixed_width_integers() -> io::Result<()> {
    load_vm(
        r#"
        local data = string.char(
            0x01, 0x02,
            0xff, 0xfe,
            0xfe, 0xdc, 0xba, 0x98,
            0x98, 0xba, 0xdc, 0xfe
        )

        assert(bytes.len(data) == 12)
        assert(bytes.slice(data, 1, 2) == string.char(0x01, 0x02))
        assert(bytes.slice(data, 13, 0) == "")
        assert(bytes.slice("", 1, 0) == "")
        assert(bytes.byte(data, 1) == 1)
        assert(bytes.byte(data, 3) == 255)
        assert(bytes.read_u8(data, 3) == 255)
        assert(bytes.read_i8(data, 3) == -1)
        assert(bytes.read_u16_be(data, 1) == 0x0102)
        assert(bytes.read_u16_le(data, 1) == 0x0201)
        assert(bytes.read_i16_be(data, 3) == -2)
        assert(bytes.read_i16_le(data, 3) == -257)
        assert(bytes.read_u32_be(data, 5) == 0xfedcba98)
        assert(bytes.read_u32_le(data, 9) == 0xfedcba98)
        assert(bytes.read_i32_be(data, 5) == -19088744)
        assert(bytes.read_i32_le(data, 9) == -19088744)

        local signed_be = string.char(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe)
        local signed_le = string.char(0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
        local minimum_be = string.char(0x80, 0, 0, 0, 0, 0, 0, 0)
        local minimum_le = string.char(0, 0, 0, 0, 0, 0, 0, 0x80)
        local maximum = string.char(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
        local zero = string.rep(string.char(0), 8)
        assert(bytes.read_i64_be(signed_be, 1) == -2)
        assert(bytes.read_i64_le(signed_le, 1) == -2)
        assert(bytes.read_i64_be(minimum_be, 1) == math.mininteger)
        assert(bytes.read_i64_le(minimum_le, 1) == math.mininteger)
        assert(bytes.read_u64_be(zero, 1) == "0")
        assert(bytes.read_u64_le(zero, 1) == "0")
        assert(bytes.read_u64_be(minimum_be, 1) == "9223372036854775808")
        assert(bytes.read_u64_le(minimum_le, 1) == "9223372036854775808")
        assert(bytes.read_u64_be(maximum, 1) == "18446744073709551615")
        assert(bytes.read_u64_le(maximum, 1) == "18446744073709551615")
        assert(math.type(bytes.read_i64_be(signed_be, 1)) == "integer")
        assert(type(bytes.read_u64_be(maximum, 1)) == "string")
        assert(bytes.len("a", "ignored") == 1)

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn bytes_reads_finite_ieee_754_floats_without_losing_bits() -> io::Result<()> {
    let vm = load_vm(
        r#"
        assert(bytes.read_f32_be(string.char(0x3f, 0xc0, 0, 0), 1) == 1.5)
        assert(bytes.read_f32_le(string.char(0, 0, 0xc0, 0x3f), 1) == 1.5)
        assert(
            math.type(bytes.read_f32_be(string.char(0x3f, 0xc0, 0, 0), 1))
                == "float"
        )
        assert(
            bytes.read_f64_be(string.char(0xc0, 0x02, 0, 0, 0, 0, 0, 0), 1)
                == -2.25
        )
        assert(
            bytes.read_f64_le(string.char(0, 0, 0, 0, 0, 0, 0x02, 0xc0), 1)
                == -2.25
        )
        assert(
            math.type(
                bytes.read_f64_be(string.char(0xc0, 0x02, 0, 0, 0, 0, 0, 0), 1)
            ) == "float"
        )

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    vm.run_with_budget(|| {
        let bytes = vm.environment_values.get::<Table>("bytes")?;
        let read_f32_be = bytes.get::<Function>("read_f32_be")?;
        let read_f64_be = bytes.get::<Function>("read_f64_be")?;
        let negative_zero_f32 = vm.lua.create_string(0x8000_0000_u32.to_be_bytes())?;
        let smallest_subnormal_f32 = vm.lua.create_string(1_u32.to_be_bytes())?;
        let negative_zero_f64 = vm
            .lua
            .create_string(0x8000_0000_0000_0000_u64.to_be_bytes())?;
        let smallest_subnormal_f64 = vm.lua.create_string(1_u64.to_be_bytes())?;

        assert_eq!(
            read_f32_be.call::<f64>((negative_zero_f32, 1))?.to_bits(),
            f64::from(-0.0_f32).to_bits()
        );
        assert_eq!(
            read_f32_be
                .call::<f64>((smallest_subnormal_f32, 1))?
                .to_bits(),
            f64::from(f32::from_bits(1)).to_bits()
        );
        assert_eq!(
            read_f64_be.call::<f64>((negative_zero_f64, 1))?.to_bits(),
            (-0.0_f64).to_bits()
        );
        assert_eq!(
            read_f64_be
                .call::<f64>((smallest_subnormal_f64, 1))?
                .to_bits(),
            1_u64
        );
        Ok(())
    })
    .map_err(|_| io::Error::other("finite float reads must preserve exact values"))?;
    Ok(())
}

#[test]
fn lua_bitwise_operators_cover_binary_flag_parsing() -> io::Result<()> {
    load_vm(
        r#"
        local flags = bytes.byte(string.char(0xa5), 1)
        assert((flags & 0x0f) == 0x05)
        assert((flags | 0x10) == 0xb5)
        assert((flags ~ 0xff) == 0x5a)
        assert((flags << 1) == 0x14a)
        assert((flags >> 4) == 0x0a)
        assert((1 << 64) == 0)
        assert((1 << -1) == 0)
        assert((8 >> -1) == 16)

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn bytes_converts_bcd_hex_base64_and_crc16() -> io::Result<()> {
    load_vm(
        r#"
        local binary = string.char(0x00, 0x01, 0x02, 0xff)
        assert(bytes.bcd_to_string(string.char(0x12, 0x90)) == "1290")
        assert(bytes.bcd_to_string("") == "")

        assert(bytes.to_hex(binary) == "000102ff")
        assert(bytes.from_hex("000102fF") == binary)
        assert(bytes.to_hex("") == "")
        assert(bytes.from_hex("") == "")

        assert(bytes.to_base64(binary) == "AAEC/w==")
        assert(bytes.from_base64("AAEC/w==") == binary)
        assert(bytes.to_base64("") == "")
        assert(bytes.from_base64("") == "")

        assert(bytes.crc16("123456789", 0x1021, 0xffff, 0, "msb") == 0x29b1)
        assert(bytes.crc16("123456789", 0xa001, 0, 0, "lsb") == 0xbb3d)
        assert(bytes.crc16("", 0x1021, 0xffff, 0, "msb", "ignored") == 0xffff)

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn bytes_rejects_invalid_inputs_with_stable_catchable_errors() -> io::Result<()> {
    load_vm(
        r#"
        local cases = {
            {
                call = function() return bytes.len(true) end,
                expected = "bytes input must be a string"
            },
            {
                call = function() return bytes.from_hex() end,
                expected = "bytes input must be a string"
            },
            {
                call = function() return bytes.byte("a", 0) end,
                expected = "bytes offset must be a positive integer"
            },
            {
                call = function() return bytes.byte("a", 1.0) end,
                expected = "bytes offset must be a positive integer"
            },
            {
                call = function() return bytes.byte("a") end,
                expected = "bytes offset must be a positive integer"
            },
            {
                call = function() return bytes.slice("a", 1, -1) end,
                expected = "bytes length must be a non-negative integer"
            },
            {
                call = function() return bytes.slice("a", 1, 1.0) end,
                expected = "bytes length must be a non-negative integer"
            },
            {
                call = function() return bytes.slice("a", 1) end,
                expected = "bytes length must be a non-negative integer"
            },
            {
                call = function() return bytes.byte("", 1) end,
                expected = "bytes range is out of bounds"
            },
            {
                call = function() return bytes.slice("a", 2, 1) end,
                expected = "bytes range is out of bounds"
            },
            {
                call = function() return bytes.slice("a", 3, 0) end,
                expected = "bytes range is out of bounds"
            },
            {
                call = function() return bytes.read_f64_be("abcdefg", 1) end,
                expected = "bytes range is out of bounds"
            },
            {
                call = function() return bytes.bcd_to_string(string.char(0x9a)) end,
                expected = "bytes BCD input must contain only decimal nibbles"
            },
            {
                call = function() return bytes.from_hex("0") end,
                expected = "bytes hex input must be even-length ASCII hexadecimal"
            },
            {
                call = function() return bytes.from_hex("0x") end,
                expected = "bytes hex input must be even-length ASCII hexadecimal"
            },
            {
                call = function() return bytes.from_hex(string.char(0xff, 0xff)) end,
                expected = "bytes hex input must be even-length ASCII hexadecimal"
            },
            {
                call = function() return bytes.from_base64("Zg") end,
                expected = "bytes base64 input must be canonical padded RFC 4648"
            },
            {
                call = function() return bytes.from_base64("Zh==") end,
                expected = "bytes base64 input must be canonical padded RFC 4648"
            },
            {
                call = function() return bytes.from_base64("Zg===") end,
                expected = "bytes base64 input must be canonical padded RFC 4648"
            },
            {
                call = function() return bytes.from_base64("Zg==\n") end,
                expected = "bytes base64 input must be canonical padded RFC 4648"
            },
            {
                call = function() return bytes.from_base64("_w==") end,
                expected = "bytes base64 input must be canonical padded RFC 4648"
            },
            {
                call = function()
                    return bytes.crc16("", -1, 0, 0, "msb")
                end,
                expected = "bytes CRC16 parameters must be integers from 0 to 65535"
            },
            {
                call = function()
                    return bytes.crc16("", 0x10000, 0, 0, "msb")
                end,
                expected = "bytes CRC16 parameters must be integers from 0 to 65535"
            },
            {
                call = function()
                    return bytes.crc16("", 0x1021, 0, nil, "msb")
                end,
                expected = "bytes CRC16 parameters must be integers from 0 to 65535"
            },
            {
                call = function()
                    return bytes.crc16("", 0x1021, 0.0, 0, "msb")
                end,
                expected = "bytes CRC16 parameters must be integers from 0 to 65535"
            },
            {
                call = function()
                    return bytes.crc16("", 0x1021, 0, 0, "MSB")
                end,
                expected = 'bytes CRC16 bit order must be "msb" or "lsb"'
            },
            {
                call = function()
                    return bytes.crc16("", 0x1021, 0, 0)
                end,
                expected = 'bytes CRC16 bit order must be "msb" or "lsb"'
            }
        }

        for _, case in ipairs(cases) do
            local ok, error_value = pcall(case.call)
            assert(not ok)
            assert(error_value == case.expected)
        end
        assert(bytes.to_hex("ok") == "6f6b")

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn bytes_rejects_non_finite_ieee_754_values_with_a_stable_error() -> io::Result<()> {
    load_vm(
        r#"
        local cases = {
            function()
                return bytes.read_f32_be(string.char(0x7f, 0x80, 0, 0), 1)
            end,
            function()
                return bytes.read_f32_le(string.char(0, 0, 0x80, 0xff), 1)
            end,
            function()
                return bytes.read_f32_be(string.char(0x7f, 0xc0, 0, 0), 1)
            end,
            function()
                return bytes.read_f64_be(
                    string.char(0x7f, 0xf0, 0, 0, 0, 0, 0, 0),
                    1
                )
            end,
            function()
                return bytes.read_f64_le(
                    string.char(1, 0, 0, 0, 0, 0, 0xf8, 0x7f),
                    1
                )
            end
        }
        for _, call in ipairs(cases) do
            local ok, error_value = pcall(call)
            assert(not ok)
            assert(error_value == "bytes floating-point input must be finite")
        end
        assert(bytes.read_f32_be(string.char(0x3f, 0x80, 0, 0), 1) == 1.0)

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn bytes_namespace_mutation_is_a_fatal_sandbox_fault() -> io::Result<()> {
    for source in [
        "bytes = {}; function main(event) end",
        "bytes.len = nil; function main(event) end",
        "bytes.extra = true; function main(event) end",
    ] {
        let Err(error) = load_vm(source, limits()?) else {
            return Err(io::Error::other("bytes namespace mutation should fail"));
        };
        assert_eq!(error.kind(), LuaVmErrorKind::SandboxViolation);
    }
    Ok(())
}

#[test]
fn bytes_memory_exhaustion_escapes_protected_calls() -> io::Result<()> {
    let limits = ScriptVmLimits::try_new(non_zero(1024 * 1024)?, Duration::from_secs(1))
        .map_err(io::Error::other)?;
    load_vm(
        r#"
        local input = string.rep("x", 400000)
        assert(#input == 400000)

        function main(event)
        end
        "#,
        limits,
    )
    .map_err(test_error)?;

    let Err(error) = load_vm(
        r#"
        local input = string.rep("x", 400000)
        local ok = pcall(bytes.to_hex, input)

        function main(event)
        end
        "#,
        limits,
    ) else {
        return Err(io::Error::other(
            "bytes allocation failure should escape protected calls",
        ));
    };

    assert_eq!(error.kind(), LuaVmErrorKind::MemoryExceeded);
    Ok(())
}

#[test]
fn bytes_base64_lua_output_memory_exhaustion_escapes_protected_calls() -> io::Result<()> {
    let limits = ScriptVmLimits::try_new(non_zero(1024 * 1024)?, Duration::from_secs(1))
        .map_err(io::Error::other)?;
    for (input_only, protected_call) in [
        (
            r#"
            local input = string.rep("x", 500000)
            assert(#input == 500000)

            function main(event)
            end
            "#,
            r#"
            local input = string.rep("x", 500000)
            local ok = pcall(bytes.to_base64, input)

            function main(event)
            end
            "#,
        ),
        (
            r#"
            local input = string.rep("eHh4", 170000)
            assert(#input == 680000)

            function main(event)
            end
            "#,
            r#"
            local input = string.rep("eHh4", 170000)
            local ok = pcall(bytes.from_base64, input)

            function main(event)
            end
            "#,
        ),
    ] {
        load_vm(input_only, limits).map_err(test_error)?;

        let Err(error) = load_vm(protected_call, limits) else {
            return Err(io::Error::other(
                "bytes Base64 Lua output allocation failure should escape protected calls",
            ));
        };

        assert_eq!(error.kind(), LuaVmErrorKind::MemoryExceeded);
    }
    Ok(())
}

#[test]
fn bytes_calls_cannot_catch_the_elapsed_cpu_limit() -> io::Result<()> {
    let mut vm = load_vm("function main(event) end", limits()?).map_err(test_error)?;
    vm.environment_values
        .raw_set("bytes_cpu_call_completed", false)
        .map_err(|_| io::Error::other("bytes CPU marker must initialize"))?;
    let function = vm
        .lua
        .load(
            r#"
            local ok = pcall(bytes.len, "value")
            bytes_cpu_call_completed = true
            return ok
            "#,
        )
        .set_mode(mlua::chunk::ChunkMode::Text)
        .set_environment(vm.environment.clone())
        .into_function()
        .map_err(|_| io::Error::other("bytes CPU test chunk must compile"))?;
    vm.cpu_time_limit = Duration::from_nanos(1);

    let Err(source) = vm.run_with_budget(|| function.call::<Value>(())) else {
        return Err(io::Error::other(
            "bytes call should not catch the elapsed CPU limit",
        ));
    };
    let error = vm.map_error(source, LuaVmErrorKind::TopLevelFailed);

    assert_eq!(error.kind(), LuaVmErrorKind::CpuTimeExceeded);
    let completed = vm
        .environment_values
        .raw_get::<bool>("bytes_cpu_call_completed")
        .map_err(|_| io::Error::other("bytes CPU marker must remain readable"))?;
    assert!(!completed);
    Ok(())
}

#[test]
fn main_receives_a_source_payload_from_the_exact_contract() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        local function field_count(value)
            local count = 0
            for _ in pairs(value) do
                count = count + 1
            end
            return count
        end

        function main(event)
            assert(type(event) == "table")
            assert(field_count(event) == 3)
            assert(event.type == "source")
            assert(math.type(event.timestamp) == "integer")
            assert(event.timestamp == 1700000000123)
            assert(type(event.payload) == "table")
            assert(field_count(event.payload) == 19)
            assert(event.payload.deviceId == "device-7")
            assert(#event.payload.registers == 2)
            assert(event.payload.registers[1] == 7)
            assert(event.payload.registers[2] == 11)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    call_source(
        &mut vm,
        1_700_000_000_123,
        bytes::Bytes::from_static(b"\x0a\x08device-7\x12\x02\x07\x0b\xa0\x06\x01"),
    )?
    .into_result_without_emit_boundaries()
    .map_err(test_error)
}

#[test]
fn each_vm_decodes_source_bytes_with_its_frozen_contract() -> io::Result<()> {
    let primary_contract = lua_source_contract()?;
    let alternate_contract = alternate_lua_source_contract()?;
    assert_ne!(
        primary_contract.descriptor_proto(),
        alternate_contract.descriptor_proto()
    );
    let payload = bytes::Bytes::from_static(b"\x08\x07");

    let mut primary_vm = LuaVm::load(
        r#"
        function main(event)
            assert(event.payload.deviceId == "")
            assert(event.payload.sequence == nil)
        end
        "#,
        limits()?,
        std::num::NonZeroU64::new(262_144)
            .ok_or_else(|| std::io::Error::other("test record limit must be non-zero"))?,
        primary_contract,
        HashMap::new(),
        None,
        || false,
    )
    .map_err(test_error)?;
    assert!(primary_vm.call_source(1, payload.clone()).is_err());
    call_source(&mut primary_vm, 2, empty_source_payload())?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;

    let mut alternate_vm = LuaVm::load(
        r#"
        function main(event)
            assert(event.payload.sequence == 7)
            assert(event.payload.deviceId == nil)
        end
        "#,
        limits()?,
        std::num::NonZeroU64::new(262_144)
            .ok_or_else(|| std::io::Error::other("test record limit must be non-zero"))?,
        alternate_contract,
        HashMap::new(),
        None,
        || false,
    )
    .map_err(test_error)?;
    call_source(&mut alternate_vm, 3, payload)?
        .into_result_without_emit_boundaries()
        .map_err(test_error)
}

#[test]
fn source_payload_projects_every_supported_protobuf_shape() -> io::Result<()> {
    let contract = lua_source_contract()?;
    let payload: bytes::Bytes = full_source_message(&contract)?.encode_to_vec().into();
    let mut vm = load_vm(
        r#"
        function main(event)
            local payload = event.payload
            assert(payload.deviceId == "device-7")
            assert(payload.enabled == true)
            assert(payload.signed32 == -2147483648)
            assert(payload.signed64 == -9223372036854775807)
            assert(payload.unsigned64 == "18446744073709551615")
            assert(payload.ratio == 1.5)
            assert(payload.score == 2.25)
            assert(payload.body == string.char(0, 128, 255))
            assert(payload.status == "STATUS_READY")
            assert(payload.child.name == "primary")
            assert(payload.child.value == string.char(0, 255))
            assert(#payload.children == 1)
            assert(payload.children[1].name == "primary")
            assert(payload.counts.alpha == 3)
            assert(payload.childrenById["18446744073709551615"].name == "primary")
            assert(payload.labelsByFlag[false] == "disabled")
            assert(payload.labelsByFlag[true] == "enabled")
            assert(payload.explicitZero == 0)
            assert(payload.textChoice == "selected")
            assert(payload.integerChoice == nil)
            assert(payload.childChoice == nil)
            assert(#payload.packets == 2)
            assert(payload.packets[1] == "first")
            assert(payload.packets[2] == string.char(0, 255))
            assert(payload.fixed32 == 4294967295)
            assert(payload.fixed64 == "18446744073709551614")
            assert(payload.signedFixed32 == -2147483647)
            assert(payload.signedFixed64 == 9223372036854775807)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    call_source(&mut vm, 73, payload)?
        .into_result_without_emit_boundaries()
        .map_err(test_error)
}

#[test]
fn source_payload_preserves_proto3_default_and_presence_semantics() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        function main(event)
            local payload = event.payload
            assert(payload.deviceId == "")
            assert(#payload.registers == 0)
            assert(payload.enabled == false)
            assert(payload.signed32 == 0)
            assert(payload.signed64 == 0)
            assert(payload.unsigned64 == "0")
            assert(payload.ratio == 0.0)
            assert(payload.score == 0.0)
            assert(payload.body == "")
            assert(payload.status == "STATUS_UNSPECIFIED")
            assert(payload.child == nil)
            assert(#payload.children == 0)
            assert(next(payload.counts) == nil)
            assert(next(payload.childrenById) == nil)
            assert(next(payload.labelsByFlag) == nil)
            assert(payload.explicitZero == nil)
            assert(payload.textChoice == nil)
            assert(payload.integerChoice == nil)
            assert(payload.childChoice == nil)
            assert(#payload.packets == 0)
            assert(payload.fixed32 == 0)
            assert(payload.fixed64 == "0")
            assert(payload.signedFixed32 == 0)
            assert(payload.signedFixed64 == 0)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    call_source(&mut vm, 74, empty_source_payload())?
        .into_result_without_emit_boundaries()
        .map_err(test_error)
}

#[test]
fn source_payload_rejects_malformed_utf8_and_unknown_enum_numbers() -> io::Result<()> {
    let mut vm = load_vm("function main(event) end", limits()?).map_err(test_error)?;
    let malformed = vm.call_source(75, bytes::Bytes::from_static(b"\x0a\x02x"));
    assert!(malformed.is_err());

    let invalid_utf8 = vm.call_source(76, bytes::Bytes::from_static(b"\x0a\x01\xff"));
    assert!(invalid_utf8.is_err());

    let unknown_enum = vm.call_source(77, bytes::Bytes::from_static(b"\x50\x07"));
    assert!(unknown_enum.is_err());
    Ok(())
}

#[test]
fn main_receives_exactly_one_timer_event_argument() -> io::Result<()> {
    let mut vm = load_vm(
        r##"
        function main(...)
            assert(select("#", ...) == 1)
            local event = select(1, ...)
            local count = 0
            for _ in pairs(event) do
                count = count + 1
            end
            assert(count == 3)
            assert(event.type == "timer")
            assert(event.timestamp == 27)
            assert(event.id == nil)
            assert(event.eligibleAt == 27)
            assert(event.payload == nil)
        end
        "##,
        limits()?,
    )
    .map_err(test_error)?;

    vm.call_timer(27)
        .into_result_without_emit_boundaries()
        .map_err(test_error)
}

#[test]
fn every_source_event_table_is_deeply_read_only() -> io::Result<()> {
    let mutations = [
        "event.type = \"changed\"",
        "event.timestamp = 0",
        "event.payload = {}",
        "event.payload.deviceId = \"changed\"",
        "event.payload.child.name = \"changed\"",
        "event.payload.children[1].name = \"changed\"",
        "event.payload.children[1] = {}",
        "event.payload.registers[1] = 2",
        "event.payload.counts.alpha = 4",
        "event.payload.childrenById[\"18446744073709551615\"] = {}",
        "table.insert(event.payload.children, {})",
        "table.remove(event.payload.children, 1)",
        "table.move(event.payload.children, 1, 1, 1)",
        "table.sort(event.payload.registers, function(left, right) return left > right end)",
    ];
    let contract = lua_source_contract()?;
    let payload: bytes::Bytes = full_source_message(&contract)?.encode_to_vec().into();

    for mutation in mutations {
        let source = format!(
            r#"
            attempts = 0

            function main(event)
                attempts = attempts + 1
                local ok = pcall(function()
                    {mutation}
                end)
                recovered = ok
            end
            "#
        );
        let mut vm = load_vm(&source, limits()?).map_err(test_error)?;

        let Err(first_error) = call_source(&mut vm, 1_700_000_000_123, payload.clone())?
            .into_result_without_emit_boundaries()
        else {
            return Err(io::Error::other(
                "event mutation should be a fatal sandbox fault",
            ));
        };
        assert_eq!(first_error.kind(), LuaVmErrorKind::SandboxViolation);

        let Err(second_error) = call_source(&mut vm, 1_700_000_000_123, payload.clone())?
            .into_result_without_emit_boundaries()
        else {
            return Err(io::Error::other(
                "failed Lua VM should not execute another main call",
            ));
        };
        assert_eq!(second_error.kind(), LuaVmErrorKind::SandboxViolation);
        assert_eq!(
            vm.environment_values
                .raw_get::<i64>("attempts")
                .map_err(|_| io::Error::other("attempt counter must remain readable"))?,
            1
        );
        assert!(
            vm.environment_values
                .raw_get::<Value>("recovered")
                .map_err(|_| io::Error::other("recovery marker must remain readable"))?
                .is_nil()
        );
    }
    Ok(())
}

#[test]
fn script_can_retain_an_old_event_across_main_calls() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        local saved
        calls = 0

        function main(event)
            calls = calls + 1
            if calls == 1 then
                saved = event
                return
            end

            assert(event.type == "timer")
            assert(saved.type == "source")
            assert(saved.payload.deviceId == "device-7")
            assert(saved.payload.body == string.char(0, 128, 255))
            assert(#saved.payload.children == 1)
            assert(saved.payload.children[1].name == "primary")
            assert(
                json.encode(saved.payload.registers) == '[7,11]'
            )
            saved = nil
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    let source = full_source_payload()?;
    call_source(&mut vm, 1_700_000_000_123, source.clone())?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    drop(source);
    vm.call_timer(1_700_000_000_124)
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;

    assert_eq!(
        vm.environment_values
            .raw_get::<i64>("calls")
            .map_err(|_| io::Error::other("call counter must remain readable"))?,
        2
    );
    Ok(())
}

#[test]
fn unretained_events_are_reclaimed_by_lua_garbage_collection() -> io::Result<()> {
    const PAYLOAD_SIZE: usize = 64 * 1024;
    const EVENT_COUNT: usize = 128;
    const RETAINED_MEMORY_ALLOWANCE: usize = 256 * 1024;

    let mut vm = load_vm(
        r#"
        function main(event)
            assert(#event.payload.body == 65536)
        end
        "#,
        ScriptVmLimits::try_new(non_zero(16 * 1024 * 1024)?, TEST_CPU_TIME_LIMIT)
            .map_err(io::Error::other)?,
    )
    .map_err(test_error)?;
    let payload = source_payload_with_body(vec![120; PAYLOAD_SIZE])?;

    call_source(&mut vm, 31, payload.clone())?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    vm.lua
        .gc_collect()
        .map_err(|_| io::Error::other("warm-up event must be collectable"))?;
    let baseline = vm.lua.used_memory();

    for _ in 0..EVENT_COUNT {
        call_source(&mut vm, 31, payload.clone())?
            .into_result_without_emit_boundaries()
            .map_err(test_error)?;
    }
    vm.lua
        .gc_collect()
        .map_err(|_| io::Error::other("event objects must be collectable"))?;
    let retained = vm.lua.used_memory().saturating_sub(baseline);

    assert!(
        retained <= RETAINED_MEMORY_ALLOWANCE,
        "unretained event projections should not accumulate"
    );
    Ok(())
}

#[test]
fn repeated_main_calls_keep_script_state_and_typed_values() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        calls = 0

        function main(event)
            calls = calls + 1
            if calls == 1 then
                assert(event.type == "source")
                assert(event.payload.deviceId == "device-7")
                assert(event.payload.status == "STATUS_READY")
            else
                assert(event.type == "timer")
            end
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    let source = full_source_payload()?;

    call_source(&mut vm, 41, source)?
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    vm.call_timer(42)
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    assert_eq!(
        vm.environment_values
            .raw_get::<i64>("calls")
            .map_err(|_| io::Error::other("call counter must remain readable"))?,
        2
    );
    Ok(())
}

#[test]
fn uncaught_main_error_makes_the_vm_unusable() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        attempts = 0

        function main(event)
            attempts = attempts + 1
            error("expected main failure")
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    let timestamp = 51;

    let Err(first_error) = vm
        .call_timer(timestamp)
        .into_result_without_emit_boundaries()
    else {
        return Err(io::Error::other("uncaught main error should fail"));
    };
    assert_eq!(first_error.kind(), LuaVmErrorKind::MainFailed);

    let Err(second_error) = vm
        .call_timer(timestamp)
        .into_result_without_emit_boundaries()
    else {
        return Err(io::Error::other("failed VM should remain unusable"));
    };
    assert_eq!(second_error.kind(), LuaVmErrorKind::MainFailed);

    let terminal_outcome = vm
        .call_source(52, bytes::Bytes::from_static(b"\x0a\x02x"))
        .map_err(|_| {
            io::Error::other("failed VM should report its terminal fault before decoding input")
        })?;
    let Err(terminal_error) = terminal_outcome.into_result_without_emit_boundaries() else {
        return Err(io::Error::other(
            "failed VM should reject malformed Source input with its terminal fault",
        ));
    };
    assert_eq!(terminal_error.kind(), LuaVmErrorKind::MainFailed);
    assert_eq!(
        vm.environment_values
            .raw_get::<i64>("attempts")
            .map_err(|_| io::Error::other("attempt counter must remain readable"))?,
        1
    );
    Ok(())
}

#[test]
fn caught_ordinary_lua_errors_do_not_make_the_vm_unusable() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        calls = 0

        function main(event)
            local ok, reason = pcall(function()
                error("expected business failure", 0)
            end)
            assert(not ok)
            assert(reason == "expected business failure")
            calls = calls + 1
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;

    vm.call_timer(61)
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    vm.call_timer(62)
        .into_result_without_emit_boundaries()
        .map_err(test_error)?;
    assert_eq!(
        vm.environment_values
            .raw_get::<i64>("calls")
            .map_err(|_| io::Error::other("call counter must remain readable"))?,
        2
    );
    Ok(())
}

#[test]
fn main_cpu_limit_makes_the_vm_unusable() -> io::Result<()> {
    let mut vm = load_vm(
        r#"
        function main(event)
            while true do
            end
        end
        "#,
        ScriptVmLimits::try_new(
            non_zero(TEST_MEMORY_LIMIT_BYTES)?,
            Duration::from_millis(10),
        )
        .map_err(io::Error::other)?,
    )
    .map_err(test_error)?;
    let timestamp = 71;

    let Err(first_error) = vm
        .call_timer(timestamp)
        .into_result_without_emit_boundaries()
    else {
        return Err(io::Error::other("infinite main should exceed CPU time"));
    };
    assert_eq!(first_error.kind(), LuaVmErrorKind::CpuTimeExceeded);

    let Err(second_error) = vm
        .call_timer(timestamp)
        .into_result_without_emit_boundaries()
    else {
        return Err(io::Error::other("CPU-limited VM should remain unusable"));
    };
    assert_eq!(second_error.kind(), LuaVmErrorKind::CpuTimeExceeded);
    Ok(())
}

#[test]
fn event_projection_memory_limit_makes_the_vm_unusable() -> io::Result<()> {
    let mut vm = load_vm(
        "function main(event) end",
        ScriptVmLimits::try_new(non_zero(512 * 1024)?, TEST_CPU_TIME_LIMIT)
            .map_err(io::Error::other)?,
    )
    .map_err(test_error)?;
    let captured = Capture::new();
    let metrics = LuaMetrics::new(&captured.meter());
    let flow = metrics.flow("flow");
    vm.metrics = Some(flow.vm([
        KeyValue::new("tenon.flow.id", "flow"),
        KeyValue::new("tenon.channel.index", 0_i64),
    ]));
    let payload = source_payload_with_body(vec![120; 1024 * 1024])?;

    let Err(first_error) =
        call_source(&mut vm, 81, payload.clone())?.into_result_without_emit_boundaries()
    else {
        return Err(io::Error::other(
            "oversized event projection should exceed VM memory",
        ));
    };
    assert_eq!(first_error.kind(), LuaVmErrorKind::MemoryExceeded);

    let Err(second_error) =
        call_source(&mut vm, 81, payload)?.into_result_without_emit_boundaries()
    else {
        return Err(io::Error::other("memory-limited VM should remain unusable"));
    };
    assert_eq!(second_error.kind(), LuaVmErrorKind::MemoryExceeded);
    assert!(
        captured
            .collect()?
            .histogram("tenon.flow.lua.duration", &[])
            .is_none()
    );
    Ok(())
}

fn exercise_generated_payload_mutations(
    operations: &[(u8, String, i32)],
) -> Result<(), TestCaseError> {
    let limits = ScriptVmLimits::try_new(
        non_zero(8 * 1024 * 1024)
            .map_err(|_| TestCaseError::fail("payload test memory limit must be valid"))?,
        Duration::from_secs(2),
    )
    .map_err(io::Error::other)?;
    let vm = load_vm_with_contracts(
        r#"
        generated_builder = registry:getBuilder("com.example.lua-builder@1.0.0")

        function applyMutation(operation, key, value)
            if operation == 0 then
                generated_builder:setLabel(key)
            elseif operation == 1 then
                generated_builder:clearLabel()
            elseif operation == 2 then
                generated_builder:addSamples(value)
            elseif operation == 3 then
                generated_builder:putCounts(key, value)
            else
                generated_builder:setTextChoice(key)
                generated_builder:setIntegerChoice(value)
            end
        end

        function finishMutations()
            generated_payload = generated_builder:build()
        end

        function main(event)
        end
        "#,
        limits,
        lua_builder_contracts()
            .map_err(|_| TestCaseError::fail("payload test registry must load"))?,
    )
    .map_err(|_| TestCaseError::fail("payload test VM must load"))?;
    let apply = vm
        .environment_values
        .raw_get::<Function>("applyMutation")
        .map_err(|_| TestCaseError::fail("payload mutation function must exist"))?;
    for (operation, key, value) in operations {
        vm.run_with_budget(|| apply.call::<()>((i64::from(*operation), key.as_str(), *value)))
            .map_err(|_| TestCaseError::fail("generated payload mutation must succeed"))?;
    }
    let finish = vm
        .environment_values
        .raw_get::<Function>("finishMutations")
        .map_err(|_| TestCaseError::fail("payload build function must exist"))?;
    vm.run_with_budget(|| finish.call::<()>(()))
        .map_err(|_| TestCaseError::fail("generated payload must build"))?;
    Ok(())
}

proptest! {
    #[test]
    fn generated_json_round_trips_through_the_script_visible_api(
        generated in generated_json()
    ) {
        let (canonical, canonical_again) = round_trip_through_json_api(&generated)?;
        let parsed = serde_json::from_slice::<serde_json::Value>(&canonical)
            .map_err(|_| TestCaseError::fail("canonical JSON must remain valid"))?;

        prop_assert_eq!(&canonical, &canonical_again);
        prop_assert!(generated.matches_serde_value(&parsed));
    }

    #[test]
    fn generated_bytes_preserve_codecs_integer_reads_and_crc16(
        input in proptest::collection::vec(any::<u8>(), 0..512),
        prefix in proptest::collection::vec(any::<u8>(), 0..16),
        word in any::<[u8; 4]>(),
        wide_word in any::<[u8; 8]>(),
        polynomial in any::<u16>(),
        initial in any::<u16>(),
        xor_out in any::<u16>(),
        lsb in any::<bool>(),
    ) {
        exercise_generated_bytes_api(
            &input,
            &prefix,
            word,
            wide_word,
            (polynomial, initial, xor_out, lsb),
        )?;
    }

    #[test]
    fn generated_finite_ieee_754_values_round_trip_through_explicit_endianness(
        prefix in proptest::collection::vec(any::<u8>(), 0..16),
        single in any::<f32>().prop_filter("single must be finite", |value| value.is_finite()),
        double in any::<f64>().prop_filter("double must be finite", |value| value.is_finite()),
    ) {
        exercise_generated_float_reads(&prefix, single, double)?;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn generated_payload_mutations_preserve_incremental_memory_accounting(
        operations in proptest::collection::vec(
            (0_u8..5, "[a-z0-9]{0,32}", any::<i32>()),
            1..64,
        )
    ) {
        exercise_generated_payload_mutations(&operations)?;
    }
}

#[test]
fn lua_5_5_1_rejects_impossible_utf8_prefixes_without_undefined_behavior() -> io::Result<()> {
    let lua = mlua::Lua::new();
    let rejected = lua
        .load(
            r#"
            local invalid = string.char(
                0xff,
                0x80, 0x80, 0x80, 0x80,
                0x80, 0x80, 0x80, 0x80
            )
            local length, position = utf8.len(invalid)
            return length == nil and position == 1
            "#,
        )
        .eval::<bool>()
        .map_err(mlua_test_error)?;

    assert!(rejected);
    Ok(())
}

#[test]
fn lua_5_5_1_resets_gmatch_depth_after_each_pattern_error() -> io::Result<()> {
    let lua = mlua::Lua::new();
    let every_call_failed_safely = lua
        .load(
            r#"
            local count = 400
            local iterator = string.gmatch(
                string.rep("a", count),
                string.rep("a?", count)
            )
            local first_ok = pcall(iterator)
            local second_ok = pcall(iterator)
            local third_ok = pcall(iterator)
            return not first_ok and not second_ok and not third_ok
            "#,
        )
        .eval::<bool>()
        .map_err(mlua_test_error)?;

    assert!(every_call_failed_safely);
    Ok(())
}

#[test]
fn unavailable_capabilities_fail_as_ordinary_lua_errors() -> io::Result<()> {
    load_vm(
        r#"
        local unavailable = {
            "getmetatable",
            "setmetatable",
            "rawget",
            "rawset",
            "rawlen",
            "rawequal"
        }
        for _, name in ipairs(unavailable) do
            local ok = pcall(function()
                return _G[name]({})
            end)
            assert(not ok)
        end

        function main(event)
        end
        "#,
        limits()?,
    )
    .map_err(test_error)?;
    Ok(())
}

#[test]
fn sandbox_faults_escape_nested_protected_calls() -> io::Result<()> {
    let Err(error) = load_vm(
        r#"
        local outer_ok = pcall(function()
            return pcall(function()
                string.sub = nil
            end)
        end)
        recovered = outer_ok

        function main(event)
        end
        "#,
        limits()?,
    ) else {
        return Err(io::Error::other(
            "nested pcall should not catch sandbox fault",
        ));
    };

    assert_eq!(error.kind(), LuaVmErrorKind::SandboxViolation);
    Ok(())
}

#[test]
fn xpcall_does_not_run_error_handler_after_sandbox_fault() -> io::Result<()> {
    let Err(error) = load_vm(
        r#"
        xpcall(
            function()
                string.sub = nil
            end,
            function()
                while true do
                end
            end
        )

        function main(event)
        end
        "#,
        ScriptVmLimits::try_new(non_zero(TEST_MEMORY_LIMIT_BYTES)?, Duration::from_millis(5))
            .map_err(io::Error::other)?,
    ) else {
        return Err(io::Error::other("xpcall should not catch sandbox fault"));
    };

    assert_eq!(error.kind(), LuaVmErrorKind::SandboxViolation);
    Ok(())
}

#[test]
fn stops_infinite_top_level_execution_by_thread_cpu_time() -> io::Result<()> {
    let limits =
        ScriptVmLimits::try_new(non_zero(TEST_MEMORY_LIMIT_BYTES)?, Duration::from_millis(5))
            .map_err(io::Error::other)?;
    let Err(error) = load_vm("while true do end", limits) else {
        return Err(io::Error::other("infinite loop should exceed CPU time"));
    };

    assert_eq!(error.kind(), LuaVmErrorKind::CpuTimeExceeded);
    Ok(())
}

#[test]
fn checks_cpu_time_even_when_instruction_hook_does_not_fire() -> io::Result<()> {
    let limits =
        ScriptVmLimits::try_new(non_zero(TEST_MEMORY_LIMIT_BYTES)?, Duration::from_nanos(1))
            .map_err(io::Error::other)?;
    let Err(error) = load_vm("function main(event) end", limits) else {
        return Err(io::Error::other(
            "short execution should still check elapsed CPU time",
        ));
    };

    assert_eq!(error.kind(), LuaVmErrorKind::CpuTimeExceeded);
    Ok(())
}

#[test]
fn stops_memory_exhaustion_even_inside_protected_calls() -> io::Result<()> {
    let limits = ScriptVmLimits::try_new(non_zero(512 * 1024)?, TEST_CPU_TIME_LIMIT)
        .map_err(io::Error::other)?;
    let Err(error) = load_vm(
        r#"
        local ok = pcall(function()
            local value = "x"
            while true do
                value = value .. value
            end
        end)

        function main(event)
        end
        "#,
        limits,
    ) else {
        return Err(io::Error::other("memory exhaustion should escape pcall"));
    };

    assert_eq!(error.kind(), LuaVmErrorKind::MemoryExceeded);
    Ok(())
}

#[test]
fn dropping_vm_releases_the_embedded_lua_state() -> io::Result<()> {
    let weak = {
        let vm = load_vm("function main(event) end", limits()?).map_err(test_error)?;
        vm.lua.weak()
    };

    assert!(weak.try_upgrade().is_none());
    Ok(())
}

struct PrintRenderProbe(Arc<AtomicUsize>);

impl UserData for PrintRenderProbe {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::ToString, |_, probe, ()| {
            probe.0.fetch_add(1, Ordering::Relaxed);
            Ok("rendered")
        });
    }
}

#[test]
fn production_memory_snapshot_tracks_payload_growth_clear_and_gc_at_main_boundaries()
-> io::Result<()> {
    let captured = Capture::new();
    let metrics = LuaMetrics::new(&captured.meter());
    let flow = metrics.flow("flow");
    let mut vm = LuaVm::load_observed(
        r#"
        retained_builder = registry:getBuilder("com.example.lua-builder@1.0.0")
        function main(event) end
        "#,
        limits()?,
        NonZeroU64::new(262_144).ok_or_else(|| io::Error::other("positive payload limit"))?,
        lua_source_contract()?,
        lua_builder_contracts()?,
        None,
        || false,
        Some(flow.vm([
            KeyValue::new("tenon.flow.id", "flow"),
            KeyValue::new("tenon.channel.index", 0_i64),
        ])),
    )
    .map_err(test_error)?;
    let initial = captured
        .collect()?
        .number("tenon.flow.lua.memory", &[])
        .ok_or_else(|| io::Error::other("Lua memory sample missing"))?;
    evaluate_result::<bool>(&vm, "retained_builder:setLabel(string.rep('x', 65536)); retained_payload = retained_builder:build(); return true").map_err(test_error)?;
    assert_eq!(
        captured.collect()?.number("tenon.flow.lua.memory", &[]),
        Some(initial),
        "collection must not enter Lua or read mutable host state"
    );
    assert!(vm.call_timer(1).result().is_ok());
    let grown = captured
        .collect()?
        .number("tenon.flow.lua.memory", &[])
        .ok_or_else(|| io::Error::other("Lua memory sample missing"))?;
    assert_eq!(
        grown,
        (vm.lua.used_memory() + TEST_MEMORY_LIMIT_BYTES
            - current_lua_memory_limit(&vm, TEST_MEMORY_LIMIT_BYTES)?) as f64
    );
    assert!(grown > initial + 131_072.0);
    evaluate_result::<bool>(
        &vm,
        "retained_builder:clearLabel(); retained_payload = nil; return true",
    )
    .map_err(test_error)?;
    vm.lua.gc_collect().map_err(mlua_test_error)?;
    vm.lua.gc_collect().map_err(mlua_test_error)?;
    assert_eq!(
        captured.collect()?.number("tenon.flow.lua.memory", &[]),
        Some(grown)
    );
    assert!(vm.call_timer(2).result().is_ok());
    let cleared = captured.collect()?;
    assert!(
        cleared
            .number("tenon.flow.lua.memory", &[])
            .ok_or_else(|| io::Error::other("Lua memory sample missing"))?
            < grown - 131_072.0
    );
    assert_eq!(
        cleared
            .histogram("tenon.flow.lua.duration", &[])
            .map(|point| point.count),
        Some(2)
    );
    drop(vm);
    assert_eq!(
        captured.collect()?.number("tenon.flow.lua.memory", &[]),
        Some(0.0)
    );
    drop(flow);
    assert_eq!(
        captured.collect()?.number("tenon.flow.lua.memory", &[]),
        None
    );
    Ok(())
}

#[test]
fn cancellation_inside_main_records_duration_once_and_terminal_reentry_records_nothing()
-> io::Result<()> {
    let captured = Capture::new();
    let metrics = LuaMetrics::new(&captured.meter());
    let flow = metrics.flow("flow");
    let stopped = Arc::new(AtomicBool::new(false));
    let request_stop = Arc::clone(&stopped);
    let mut vm = LuaVm::load_observed(
        "function main(event) print('request cancellation'); while true do end end",
        limits()?,
        NonZeroU64::new(262_144).ok_or_else(|| io::Error::other("positive payload limit"))?,
        lua_source_contract()?,
        HashMap::new(),
        Some(Box::new(move |_| {
            request_stop.store(true, Ordering::Relaxed);
            Ok(())
        })),
        move || stopped.load(Ordering::Relaxed),
        Some(flow.vm([
            KeyValue::new("tenon.flow.id", "flow"),
            KeyValue::new("tenon.channel.index", 0_i64),
        ])),
    )
    .map_err(test_error)?;
    for _ in 0..2 {
        let error = vm
            .call_timer(1)
            .into_result_without_emit_boundaries()
            .err()
            .ok_or_else(|| io::Error::other("main should stop"))?;
        assert_eq!(error.kind(), LuaVmErrorKind::ExecutionStopped);
        assert_eq!(
            captured
                .collect()?
                .histogram("tenon.flow.lua.duration", &[])
                .map(|point| point.count),
            Some(1)
        );
    }
    Ok(())
}
