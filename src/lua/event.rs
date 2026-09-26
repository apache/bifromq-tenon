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

//! Owned Kernel input and its deeply read-only Lua projection.

use super::{LuaVmFatalFault, fatal_error, install_readonly_backing, record_fatal_fault};
use bytes::Bytes;
use mlua::{Lua, Table, Value as LuaValue};
use prost_reflect::{
    DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor, ReflectMessage,
    Value as ProtobufValue,
};
use std::borrow::Cow;
use std::cell::Cell;
use std::error::Error;
use std::fmt;
use std::rc::Rc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct KernelTimestampMillis(pub(super) i64);

#[derive(Debug, PartialEq)]
pub(super) enum ProcessEvent {
    Source {
        timestamp: KernelTimestampMillis,
        payload: DynamicMessage,
    },
    Timer {
        timestamp: KernelTimestampMillis,
        id: Option<Box<str>>,
        eligible_at: KernelTimestampMillis,
    },
}

pub(super) fn decode_source(
    timestamp: KernelTimestampMillis,
    root_message: MessageDescriptor,
    payload: Bytes,
) -> Result<ProcessEvent, SourcePayloadDecodeError> {
    let payload = DynamicMessage::decode(root_message, payload)
        .map_err(SourcePayloadDecodeError::Protobuf)?;
    validate_source_message(&payload)?;
    Ok(ProcessEvent::Source { timestamp, payload })
}

#[derive(Debug)]
pub(crate) enum SourcePayloadDecodeError {
    Protobuf(prost::DecodeError),
    UnknownEnumValue,
}

impl SourcePayloadDecodeError {
    pub(crate) const fn metric_type(&self) -> &'static str {
        match self {
            Self::Protobuf(_) => "protobuf_invalid",
            Self::UnknownEnumValue => "unknown_enum_value",
        }
    }

    /// Returns advisory Human-facing decode text for online diagnostics.
    ///
    /// The stable identity of a failure remains [`Self::metric_type`]; this
    /// text is not a versioned contract.
    pub(crate) fn diagnostic_detail(&self) -> Cow<'_, str> {
        match self {
            Self::Protobuf(source) => Cow::Owned(source.to_string()),
            Self::UnknownEnumValue => Cow::Owned(self.to_string()),
        }
    }
}

impl fmt::Display for SourcePayloadDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Source payload does not match its Payload Contract")
    }
}

impl Error for SourcePayloadDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Protobuf(source) => Some(source),
            Self::UnknownEnumValue => None,
        }
    }
}

fn validate_source_message(message: &DynamicMessage) -> Result<(), SourcePayloadDecodeError> {
    for field in message.descriptor().fields() {
        if field.supports_presence() && !message.has_field(&field) {
            continue;
        }
        validate_source_field(&field, message.get_field(&field).as_ref())?;
    }
    Ok(())
}

fn validate_source_field(
    field: &FieldDescriptor,
    value: &ProtobufValue,
) -> Result<(), SourcePayloadDecodeError> {
    if field.is_map() {
        let (ProtobufValue::Map(values), Kind::Message(entry_descriptor)) = (value, field.kind())
        else {
            return Ok(());
        };
        let Some(value_field) = entry_descriptor.get_field(2) else {
            return Ok(());
        };
        for value in values.values() {
            validate_source_scalar(&value_field, value)?;
        }
        return Ok(());
    }
    if field.is_list() {
        let ProtobufValue::List(values) = value else {
            return Ok(());
        };
        for value in values {
            validate_source_scalar(field, value)?;
        }
        return Ok(());
    }
    validate_source_scalar(field, value)
}

fn validate_source_scalar(
    field: &FieldDescriptor,
    value: &ProtobufValue,
) -> Result<(), SourcePayloadDecodeError> {
    match (value, field.kind()) {
        (ProtobufValue::EnumNumber(number), Kind::Enum(descriptor))
            if descriptor.get_value(*number).is_none() =>
        {
            Err(SourcePayloadDecodeError::UnknownEnumValue)
        }
        (ProtobufValue::Message(message), Kind::Message(_)) => validate_source_message(message),
        _ => Ok(()),
    }
}

pub(super) fn project(
    lua: &Lua,
    event: &ProcessEvent,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
) -> mlua::Result<Table> {
    let backing = lua.create_table()?;
    match event {
        ProcessEvent::Source { timestamp, payload } => {
            backing.raw_set("type", "source")?;
            backing.raw_set("timestamp", timestamp.0)?;
            backing.raw_set(
                "payload",
                project_source_message(lua, payload, Rc::clone(&fatal_fault))?,
            )?;
        }
        ProcessEvent::Timer {
            timestamp,
            id,
            eligible_at,
        } => {
            backing.raw_set("type", "timer")?;
            backing.raw_set("timestamp", timestamp.0)?;
            backing.raw_set("id", id.as_deref())?;
            backing.raw_set("eligibleAt", eligible_at.0)?;
        }
    }
    install_readonly_backing(lua, backing, fatal_fault)
}

fn project_source_message(
    lua: &Lua,
    message: &DynamicMessage,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
) -> mlua::Result<Table> {
    let backing = lua.create_table()?;
    for field in message.descriptor().fields() {
        if field.supports_presence() && !message.has_field(&field) {
            continue;
        }
        let value = message.get_field(&field);
        backing.raw_set(
            field.json_name(),
            project_source_field(lua, &field, value.as_ref(), Rc::clone(&fatal_fault))?,
        )?;
    }
    install_readonly_backing(lua, backing, fatal_fault)
}

fn project_source_field(
    lua: &Lua,
    field: &FieldDescriptor,
    value: &ProtobufValue,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
) -> mlua::Result<LuaValue> {
    if field.is_map() {
        return project_source_map(lua, field, value, fatal_fault).map(LuaValue::Table);
    }
    if field.is_list() {
        let ProtobufValue::List(values) = value else {
            return Err(source_projection_invariant(&fatal_fault));
        };
        let backing = lua.create_table()?;
        let kind = field.kind();
        for value in values {
            backing.raw_push(project_source_scalar(
                lua,
                &kind,
                value,
                Rc::clone(&fatal_fault),
            )?)?;
        }
        return install_readonly_backing(lua, backing, fatal_fault).map(LuaValue::Table);
    }
    project_source_scalar(lua, &field.kind(), value, fatal_fault)
}

fn project_source_map(
    lua: &Lua,
    field: &FieldDescriptor,
    value: &ProtobufValue,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
) -> mlua::Result<Table> {
    let (ProtobufValue::Map(values), Kind::Message(entry_descriptor)) = (value, field.kind())
    else {
        return Err(source_projection_invariant(&fatal_fault));
    };
    let Some(value_field) = entry_descriptor.get_field(2) else {
        return Err(source_projection_invariant(&fatal_fault));
    };
    let value_kind = value_field.kind();
    let backing = lua.create_table()?;
    for (key, value) in values {
        backing.raw_set(
            project_source_map_key(lua, key)?,
            project_source_scalar(lua, &value_kind, value, Rc::clone(&fatal_fault))?,
        )?;
    }
    install_readonly_backing(lua, backing, fatal_fault)
}

fn project_source_map_key(lua: &Lua, key: &MapKey) -> mlua::Result<LuaValue> {
    match key {
        MapKey::Bool(value) => Ok(LuaValue::Boolean(*value)),
        MapKey::I32(value) => Ok(LuaValue::Integer(i64::from(*value))),
        MapKey::I64(value) => Ok(LuaValue::Integer(*value)),
        MapKey::U32(value) => Ok(LuaValue::Integer(i64::from(*value))),
        MapKey::U64(value) => lua.create_string(value.to_string()).map(LuaValue::String),
        MapKey::String(value) => lua.create_string(value).map(LuaValue::String),
    }
}

fn project_source_scalar(
    lua: &Lua,
    kind: &Kind,
    value: &ProtobufValue,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
) -> mlua::Result<LuaValue> {
    match (value, kind) {
        (ProtobufValue::Bool(value), Kind::Bool) => Ok(LuaValue::Boolean(*value)),
        (ProtobufValue::I32(value), Kind::Int32 | Kind::Sint32 | Kind::Sfixed32) => {
            Ok(LuaValue::Integer(i64::from(*value)))
        }
        (ProtobufValue::I64(value), Kind::Int64 | Kind::Sint64 | Kind::Sfixed64) => {
            Ok(LuaValue::Integer(*value))
        }
        (ProtobufValue::U32(value), Kind::Uint32 | Kind::Fixed32) => {
            Ok(LuaValue::Integer(i64::from(*value)))
        }
        (ProtobufValue::U64(value), Kind::Uint64 | Kind::Fixed64) => {
            lua.create_string(value.to_string()).map(LuaValue::String)
        }
        (ProtobufValue::F32(value), Kind::Float) => Ok(LuaValue::Number(f64::from(*value))),
        (ProtobufValue::F64(value), Kind::Double) => Ok(LuaValue::Number(*value)),
        (ProtobufValue::String(value), Kind::String) => {
            lua.create_string(value).map(LuaValue::String)
        }
        (ProtobufValue::Bytes(value), Kind::Bytes) => {
            lua.create_string(value).map(LuaValue::String)
        }
        (ProtobufValue::EnumNumber(number), Kind::Enum(descriptor)) => {
            let Some(value) = descriptor.get_value(*number) else {
                return Err(source_projection_invariant(&fatal_fault));
            };
            lua.create_string(value.name()).map(LuaValue::String)
        }
        (ProtobufValue::Message(message), Kind::Message(_)) => {
            project_source_message(lua, message, fatal_fault).map(LuaValue::Table)
        }
        _ => Err(source_projection_invariant(&fatal_fault)),
    }
}

fn source_projection_invariant(fatal_fault: &Rc<Cell<Option<LuaVmFatalFault>>>) -> mlua::Error {
    fatal_error(record_fatal_fault(
        fatal_fault,
        LuaVmFatalFault::InternalInvariantViolation,
    ))
}
