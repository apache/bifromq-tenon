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

//! Lua `emit` boundary and deterministic Sink Payload encoding.
//!
//! `EmitSlot` is active only while one `main(event)` call is running. A payload
//! `emit` validates one immutable [`BuiltPayload`] and encodes its dynamic
//! Protobuf message with a stable field and map-key order. A no-argument `emit`
//! appends a completion-only boundary. Both forms append their accepted output
//! immediately, so a later script failure preserves the accepted prefix. This
//! module checks complete `EgressRecord` size before encoding, but does not write mmap
//! data, or resolve Ingress Completion.

use super::payload::BuiltPayload;
use super::{
    ExecutionBudget, LuaApiFailure, LuaApiResult, LuaVmFatalFault,
    create_catchable_api_wrapper_factory, finish_api_call, protect_name, sandbox_violation,
};
use crate::contracts::sink;
use crate::identifiers::SinkContractId;
use mlua::{Function, Lua, MultiValue, Table, Value as LuaValue};
use prost::Message;
use prost::bytes::BufMut;
use prost::encoding::{self, WireType};
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MapKey, Value};
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::num::NonZeroU64;
use std::rc::Rc;

const EMIT_ARGUMENT_ERROR: &str = "emit argument must be a built Payload";
const EMIT_ARGUMENTS_ERROR: &str = "emit arguments are invalid";

/// One ordered completion boundary accepted during a Lua `main` call.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum EmitBoundary {
    Payload {
        sink_contract_id: SinkContractId,
        record: sink::EncodedEgressRecord,
    },
    CompletionOnly,
}

#[derive(Debug, Default)]
enum EmitState {
    #[default]
    Inactive,
    Active(Vec<EmitBoundary>),
}

/// Per-VM call slot shared by the Lua callback and its Rust event-loop owner.
#[derive(Clone, Debug, Default)]
pub(super) struct EmitSlot {
    state: Rc<RefCell<EmitState>>,
}

impl EmitSlot {
    pub(super) fn begin_main(&self) -> LuaApiResult<()> {
        let mut state = self
            .state
            .try_borrow_mut()
            .map_err(|_| LuaApiFailure::InternalInvariantViolation)?;
        if !matches!(*state, EmitState::Inactive) {
            return Err(LuaApiFailure::InternalInvariantViolation);
        }
        *state = EmitState::Active(Vec::new());
        Ok(())
    }

    pub(super) fn finish_main(&self) -> LuaApiResult<Vec<EmitBoundary>> {
        let mut state = self
            .state
            .try_borrow_mut()
            .map_err(|_| LuaApiFailure::InternalInvariantViolation)?;
        match std::mem::take(&mut *state) {
            EmitState::Inactive => Err(LuaApiFailure::InternalInvariantViolation),
            EmitState::Active(boundaries) => Ok(boundaries),
        }
    }

    fn is_active(&self) -> LuaApiResult<bool> {
        self.state
            .try_borrow()
            .map(|state| matches!(*state, EmitState::Active(_)))
            .map_err(|_| LuaApiFailure::InternalInvariantViolation)
    }

    fn accept(&self, boundary: EmitBoundary) -> LuaApiResult<()> {
        let mut state = self
            .state
            .try_borrow_mut()
            .map_err(|_| LuaApiFailure::InternalInvariantViolation)?;
        let EmitState::Active(boundaries) = &mut *state else {
            return Err(LuaApiFailure::InternalInvariantViolation);
        };
        boundaries
            .try_reserve(1)
            .map_err(|_| LuaApiFailure::ResourceLimitExceeded)?;
        boundaries.push(boundary);
        Ok(())
    }
}

pub(super) fn install(
    lua: &Lua,
    environment_values: &Table,
    protected_names: Rc<RefCell<HashSet<Vec<u8>>>>,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
    execution_budget: Rc<RefCell<Option<ExecutionBudget>>>,
    slot: EmitSlot,
    max_record_bytes: NonZeroU64,
) -> mlua::Result<()> {
    let wrapper_factory = create_catchable_api_wrapper_factory(lua)?;
    let emit_fault = Rc::clone(&fatal_fault);
    let native_emit = lua.create_function(move |lua, arguments: MultiValue| {
        match slot.is_active() {
            Ok(true) => {}
            Ok(false) => return Err(sandbox_violation(&emit_fault)),
            Err(error) => {
                return finish_api_call(lua, Err(error), &emit_fault, &execution_budget);
            }
        }

        let result = accept_emit(arguments, &slot, max_record_bytes).map(|()| LuaValue::Nil);
        finish_api_call(lua, result, &emit_fault, &execution_budget)
    })?;
    environment_values.raw_set("emit", wrapper_factory.call::<Function>(native_emit)?)?;
    protect_name(&protected_names, "emit");
    Ok(())
}

fn accept_emit(
    mut arguments: MultiValue,
    slot: &EmitSlot,
    max_record_bytes: NonZeroU64,
) -> LuaApiResult<()> {
    if arguments.is_empty() {
        return slot.accept(EmitBoundary::CompletionOnly);
    }
    if arguments.len() != 1 {
        return Err(LuaApiFailure::Api(EMIT_ARGUMENTS_ERROR));
    }
    let Some(LuaValue::UserData(payload)) = arguments.pop_front() else {
        return Err(LuaApiFailure::Api(EMIT_ARGUMENT_ERROR));
    };
    if !payload.is::<BuiltPayload>() {
        return Err(LuaApiFailure::Api(EMIT_ARGUMENT_ERROR));
    }
    let payload = payload
        .borrow::<BuiltPayload>()
        .map_err(|_| LuaApiFailure::InternalInvariantViolation)?;
    let boundary = EmitBoundary::Payload {
        sink_contract_id: payload.sink_contract_id().clone(),
        record: encode_deterministically(payload.message(), max_record_bytes)?,
    };
    slot.accept(boundary)
}

fn encode_deterministically(
    message: &DynamicMessage,
    max_record_bytes: NonZeroU64,
) -> LuaApiResult<sink::EncodedEgressRecord> {
    let encoded_len = message.encoded_len();
    if sink::encoded_len(encoded_len) as u64 > max_record_bytes.get() {
        return Err(LuaApiFailure::Api("egress.record_too_large"));
    }
    sink::EncodedEgressRecord::try_encode(encoded_len, |output| {
        let payload_start = output.len();
        encode_message(message, output)?;
        if output.len() - payload_start != encoded_len {
            return Err(LuaApiFailure::InternalInvariantViolation);
        }
        Ok(())
    })
}

fn encode_message(message: &DynamicMessage, output: &mut Vec<u8>) -> LuaApiResult<()> {
    // prost-reflect 0.16.5 iterates its BTreeMap in field-number order. This is
    // not a public API guarantee; preserve emit_uses_canonical_field_and_map_key_order
    // when upgrading the dependency. Map entries still require their own sort.
    for (field, value) in message.fields() {
        encode_field(&field, value, output)?;
    }
    Ok(())
}

fn encode_field(field: &FieldDescriptor, value: &Value, output: &mut Vec<u8>) -> LuaApiResult<()> {
    if field.is_map() {
        return encode_map(field, value, output);
    }
    if field.is_list() {
        let Value::List(values) = value else {
            return Err(LuaApiFailure::InternalInvariantViolation);
        };
        if field.is_packed() {
            return encode_packed(field.number(), field.kind(), values, output);
        }
        for value in values {
            encode_singular(field.number(), field.kind(), value, output)?;
        }
        return Ok(());
    }
    encode_singular(field.number(), field.kind(), value, output)
}

fn encode_map(field: &FieldDescriptor, value: &Value, output: &mut Vec<u8>) -> LuaApiResult<()> {
    let (Value::Map(values), Kind::Message(entry_descriptor)) = (value, field.kind()) else {
        return Err(LuaApiFailure::InternalInvariantViolation);
    };
    let key_field = entry_descriptor
        .get_field(1)
        .ok_or(LuaApiFailure::InternalInvariantViolation)?;
    let value_field = entry_descriptor
        .get_field(2)
        .ok_or(LuaApiFailure::InternalInvariantViolation)?;
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(values.len())
        .map_err(|_| LuaApiFailure::ResourceLimitExceeded)?;
    entries.extend(values.iter());
    entries.sort_unstable_by_key(|(key, _)| *key);

    for (key, value) in entries {
        let encode_key = !map_key_is_default(key);
        let encode_value = value_field.supports_presence() || value != &value_field.default_value();
        let entry_len = map_key_encoded_len(&key_field, key, encode_key)?
            .checked_add(singular_encoded_len(&value_field, value, encode_value)?)
            .ok_or(LuaApiFailure::ResourceLimitExceeded)?;
        encoding::encode_key(field.number(), WireType::LengthDelimited, output);
        encoding::encode_varint(entry_len as u64, output);
        if encode_key {
            encode_map_key(&key_field, key, output)?;
        }
        if encode_value {
            encode_singular(value_field.number(), value_field.kind(), value, output)?;
        }
    }
    Ok(())
}

fn map_key_is_default(key: &MapKey) -> bool {
    match key {
        MapKey::Bool(value) => !value,
        MapKey::I32(value) => *value == 0,
        MapKey::I64(value) => *value == 0,
        MapKey::U32(value) => *value == 0,
        MapKey::U64(value) => *value == 0,
        MapKey::String(value) => value.is_empty(),
    }
}

fn map_key_encoded_len(field: &FieldDescriptor, key: &MapKey, encode: bool) -> LuaApiResult<usize> {
    if !encode {
        return Ok(0);
    }
    match (key, field.kind()) {
        (MapKey::Bool(value), Kind::Bool) => Ok(encoding::bool::encoded_len(field.number(), value)),
        (MapKey::I32(value), Kind::Int32) => {
            Ok(encoding::int32::encoded_len(field.number(), value))
        }
        (MapKey::I32(value), Kind::Sint32) => {
            Ok(encoding::sint32::encoded_len(field.number(), value))
        }
        (MapKey::I32(value), Kind::Sfixed32) => {
            Ok(encoding::sfixed32::encoded_len(field.number(), value))
        }
        (MapKey::I64(value), Kind::Int64) => {
            Ok(encoding::int64::encoded_len(field.number(), value))
        }
        (MapKey::I64(value), Kind::Sint64) => {
            Ok(encoding::sint64::encoded_len(field.number(), value))
        }
        (MapKey::I64(value), Kind::Sfixed64) => {
            Ok(encoding::sfixed64::encoded_len(field.number(), value))
        }
        (MapKey::U32(value), Kind::Uint32) => {
            Ok(encoding::uint32::encoded_len(field.number(), value))
        }
        (MapKey::U32(value), Kind::Fixed32) => {
            Ok(encoding::fixed32::encoded_len(field.number(), value))
        }
        (MapKey::U64(value), Kind::Uint64) => {
            Ok(encoding::uint64::encoded_len(field.number(), value))
        }
        (MapKey::U64(value), Kind::Fixed64) => {
            Ok(encoding::fixed64::encoded_len(field.number(), value))
        }
        (MapKey::String(value), Kind::String) => {
            Ok(encoding::string::encoded_len(field.number(), value))
        }
        _ => Err(LuaApiFailure::InternalInvariantViolation),
    }
}

fn encode_map_key(field: &FieldDescriptor, key: &MapKey, output: &mut Vec<u8>) -> LuaApiResult<()> {
    match (key, field.kind()) {
        (MapKey::Bool(value), Kind::Bool) => encoding::bool::encode(field.number(), value, output),
        (MapKey::I32(value), Kind::Int32) => encoding::int32::encode(field.number(), value, output),
        (MapKey::I32(value), Kind::Sint32) => {
            encoding::sint32::encode(field.number(), value, output)
        }
        (MapKey::I32(value), Kind::Sfixed32) => {
            encoding::sfixed32::encode(field.number(), value, output)
        }
        (MapKey::I64(value), Kind::Int64) => encoding::int64::encode(field.number(), value, output),
        (MapKey::I64(value), Kind::Sint64) => {
            encoding::sint64::encode(field.number(), value, output)
        }
        (MapKey::I64(value), Kind::Sfixed64) => {
            encoding::sfixed64::encode(field.number(), value, output)
        }
        (MapKey::U32(value), Kind::Uint32) => {
            encoding::uint32::encode(field.number(), value, output)
        }
        (MapKey::U32(value), Kind::Fixed32) => {
            encoding::fixed32::encode(field.number(), value, output)
        }
        (MapKey::U64(value), Kind::Uint64) => {
            encoding::uint64::encode(field.number(), value, output)
        }
        (MapKey::U64(value), Kind::Fixed64) => {
            encoding::fixed64::encode(field.number(), value, output)
        }
        (MapKey::String(value), Kind::String) => {
            encoding::string::encode(field.number(), value, output)
        }
        _ => return Err(LuaApiFailure::InternalInvariantViolation),
    }
    Ok(())
}

fn singular_encoded_len(
    field: &FieldDescriptor,
    value: &Value,
    encode: bool,
) -> LuaApiResult<usize> {
    if !encode {
        return Ok(0);
    }
    let number = field.number();
    match (value, field.kind()) {
        (Value::Bool(value), Kind::Bool) => Ok(encoding::bool::encoded_len(number, value)),
        (Value::I32(value), Kind::Int32) => Ok(encoding::int32::encoded_len(number, value)),
        (Value::I32(value), Kind::Sint32) => Ok(encoding::sint32::encoded_len(number, value)),
        (Value::I32(value), Kind::Sfixed32) => Ok(encoding::sfixed32::encoded_len(number, value)),
        (Value::I64(value), Kind::Int64) => Ok(encoding::int64::encoded_len(number, value)),
        (Value::I64(value), Kind::Sint64) => Ok(encoding::sint64::encoded_len(number, value)),
        (Value::I64(value), Kind::Sfixed64) => Ok(encoding::sfixed64::encoded_len(number, value)),
        (Value::U32(value), Kind::Uint32) => Ok(encoding::uint32::encoded_len(number, value)),
        (Value::U32(value), Kind::Fixed32) => Ok(encoding::fixed32::encoded_len(number, value)),
        (Value::U64(value), Kind::Uint64) => Ok(encoding::uint64::encoded_len(number, value)),
        (Value::U64(value), Kind::Fixed64) => Ok(encoding::fixed64::encoded_len(number, value)),
        (Value::F32(value), Kind::Float) => Ok(encoding::float::encoded_len(number, value)),
        (Value::F64(value), Kind::Double) => Ok(encoding::double::encoded_len(number, value)),
        (Value::String(value), Kind::String) => Ok(encoding::string::encoded_len(number, value)),
        (Value::Bytes(value), Kind::Bytes) => Ok(encoding::bytes::encoded_len(number, value)),
        (Value::EnumNumber(value), Kind::Enum(_)) => {
            Ok(encoding::int32::encoded_len(number, value))
        }
        (Value::Message(message), Kind::Message(_)) => {
            let len = message.encoded_len();
            Ok(encoding::key_len(number) + encoding::encoded_len_varint(len as u64) + len)
        }
        _ => Err(LuaApiFailure::InternalInvariantViolation),
    }
}

fn encode_singular(
    number: u32,
    kind: Kind,
    value: &Value,
    output: &mut Vec<u8>,
) -> LuaApiResult<()> {
    match (value, kind) {
        (Value::Bool(value), Kind::Bool) => encoding::bool::encode(number, value, output),
        (Value::I32(value), Kind::Int32) => encoding::int32::encode(number, value, output),
        (Value::I32(value), Kind::Sint32) => encoding::sint32::encode(number, value, output),
        (Value::I32(value), Kind::Sfixed32) => encoding::sfixed32::encode(number, value, output),
        (Value::I64(value), Kind::Int64) => encoding::int64::encode(number, value, output),
        (Value::I64(value), Kind::Sint64) => encoding::sint64::encode(number, value, output),
        (Value::I64(value), Kind::Sfixed64) => encoding::sfixed64::encode(number, value, output),
        (Value::U32(value), Kind::Uint32) => encoding::uint32::encode(number, value, output),
        (Value::U32(value), Kind::Fixed32) => encoding::fixed32::encode(number, value, output),
        (Value::U64(value), Kind::Uint64) => encoding::uint64::encode(number, value, output),
        (Value::U64(value), Kind::Fixed64) => encoding::fixed64::encode(number, value, output),
        (Value::F32(value), Kind::Float) => encoding::float::encode(number, value, output),
        (Value::F64(value), Kind::Double) => encoding::double::encode(number, value, output),
        (Value::String(value), Kind::String) => encoding::string::encode(number, value, output),
        (Value::Bytes(value), Kind::Bytes) => encoding::bytes::encode(number, value, output),
        (Value::EnumNumber(value), Kind::Enum(_)) => encoding::int32::encode(number, value, output),
        (Value::Message(message), Kind::Message(_)) => {
            let len = message.encoded_len();
            encoding::encode_key(number, WireType::LengthDelimited, output);
            encoding::encode_varint(len as u64, output);
            let start = output.len();
            encode_message(message, output)?;
            if output.len().checked_sub(start) != Some(len) {
                return Err(LuaApiFailure::InternalInvariantViolation);
            }
        }
        _ => return Err(LuaApiFailure::InternalInvariantViolation),
    }
    Ok(())
}

fn encode_packed(
    number: u32,
    kind: Kind,
    values: &[Value],
    output: &mut Vec<u8>,
) -> LuaApiResult<()> {
    if values.is_empty() {
        return Ok(());
    }
    let mut payload_len = 0_usize;
    for value in values {
        payload_len = payload_len
            .checked_add(packed_value_len(&kind, value)?)
            .ok_or(LuaApiFailure::ResourceLimitExceeded)?;
    }
    encoding::encode_key(number, WireType::LengthDelimited, output);
    encoding::encode_varint(payload_len as u64, output);
    let start = output.len();
    for value in values {
        encode_packed_value(&kind, value, output)?;
    }
    if output.len().checked_sub(start) != Some(payload_len) {
        return Err(LuaApiFailure::InternalInvariantViolation);
    }
    Ok(())
}

fn packed_value_len(kind: &Kind, value: &Value) -> LuaApiResult<usize> {
    let len = match (value, kind) {
        (Value::Bool(value), Kind::Bool) => encoding::encoded_len_varint(u64::from(*value)),
        (Value::I32(value), Kind::Int32) | (Value::EnumNumber(value), Kind::Enum(_)) => {
            encoding::encoded_len_varint(*value as u64)
        }
        (Value::I32(value), Kind::Sint32) => encoding::encoded_len_varint(zigzag_i32(*value)),
        (Value::I32(_), Kind::Sfixed32)
        | (Value::U32(_), Kind::Fixed32)
        | (Value::F32(_), Kind::Float) => 4,
        (Value::I64(value), Kind::Int64) => encoding::encoded_len_varint(*value as u64),
        (Value::I64(value), Kind::Sint64) => encoding::encoded_len_varint(zigzag_i64(*value)),
        (Value::I64(_), Kind::Sfixed64)
        | (Value::U64(_), Kind::Fixed64)
        | (Value::F64(_), Kind::Double) => 8,
        (Value::U32(value), Kind::Uint32) => encoding::encoded_len_varint(u64::from(*value)),
        (Value::U64(value), Kind::Uint64) => encoding::encoded_len_varint(*value),
        _ => return Err(LuaApiFailure::InternalInvariantViolation),
    };
    Ok(len)
}

fn encode_packed_value(kind: &Kind, value: &Value, output: &mut Vec<u8>) -> LuaApiResult<()> {
    match (value, kind) {
        (Value::Bool(value), Kind::Bool) => encoding::encode_varint(u64::from(*value), output),
        (Value::I32(value), Kind::Int32) | (Value::EnumNumber(value), Kind::Enum(_)) => {
            encoding::encode_varint(*value as u64, output)
        }
        (Value::I32(value), Kind::Sint32) => encoding::encode_varint(zigzag_i32(*value), output),
        (Value::I32(value), Kind::Sfixed32) => output.put_i32_le(*value),
        (Value::U32(value), Kind::Fixed32) => output.put_u32_le(*value),
        (Value::F32(value), Kind::Float) => output.put_f32_le(*value),
        (Value::I64(value), Kind::Int64) => encoding::encode_varint(*value as u64, output),
        (Value::I64(value), Kind::Sint64) => encoding::encode_varint(zigzag_i64(*value), output),
        (Value::I64(value), Kind::Sfixed64) => output.put_i64_le(*value),
        (Value::U64(value), Kind::Fixed64) => output.put_u64_le(*value),
        (Value::F64(value), Kind::Double) => output.put_f64_le(*value),
        (Value::U32(value), Kind::Uint32) => encoding::encode_varint(u64::from(*value), output),
        (Value::U64(value), Kind::Uint64) => encoding::encode_varint(*value, output),
        _ => return Err(LuaApiFailure::InternalInvariantViolation),
    }
    Ok(())
}

const fn zigzag_i32(value: i32) -> u64 {
    ((value << 1) ^ (value >> 31)) as u32 as u64
}

const fn zigzag_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}
