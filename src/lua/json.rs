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

//! Deterministic JSON boundary for the Tenon Lua subset.
//!
//! The VM owns one JSON null value and one hidden array marker. Each encode call
//! owns its output and active recursion path while borrowing those VM-owned
//! values. Stable API failures remain catchable Lua errors, while VM resource
//! failures poison the VM and escape protected Lua calls.

use super::{
    ExecutionBudget, LuaApiFailure, LuaApiResult, LuaVmFatalFault,
    create_catchable_api_wrapper_factory, finish_api_call, proxy_backing,
    publish_readonly_namespace, record_vm_error,
};
use jsonc_parser::ast::Value as JsonValue;
use jsonc_parser::{CollectOptions, ParseOptions, parse_to_ast};
use mlua::{AnyUserData, Function, Lua, LuaString, Table, UserData, Value};
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::ffi::c_void;
use std::fmt::Write as _;
use std::rc::Rc;
use std::str::from_utf8;

const STRICT_JSON_OPTIONS: ParseOptions = ParseOptions {
    allow_comments: false,
    allow_loose_object_property_names: false,
    allow_trailing_commas: false,
    allow_missing_commas: false,
    allow_single_quoted_strings: false,
    allow_hexadecimal_numbers: false,
    allow_unary_plus_numbers: false,
};

const MAX_JSON_NESTING_DEPTH: usize = 128;
const DECODE_INPUT_TYPE_ERROR: &str = "json.decode input must be a string";
const DECODE_UTF8_ERROR: &str = "json.decode input must be valid UTF-8";
const DECODE_SYNTAX_ERROR: &str = "json.decode input must be valid JSON";
const DECODE_DUPLICATE_FIELD_ERROR: &str = "json.decode object field names must be unique";
const DECODE_INTEGER_RANGE_ERROR: &str = "json.decode integer must fit signed 64-bit";
const DECODE_FINITE_NUMBER_ERROR: &str = "json.decode number must be finite";
const ENCODE_VALUE_ERROR: &str = "json.encode value is not representable as JSON";

#[derive(Debug)]
struct JsonNull;

impl UserData for JsonNull {}

type JsonResult<T> = LuaApiResult<T>;

pub(super) fn install(
    lua: &Lua,
    environment_values: &Table,
    protected_names: Rc<RefCell<HashSet<Vec<u8>>>>,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
    execution_budget: Rc<RefCell<Option<ExecutionBudget>>>,
) -> mlua::Result<()> {
    let array_metatable = lua.create_table()?;
    let null = lua.create_userdata(JsonNull)?;
    let backing = lua.create_table()?;
    let api_wrapper_factory = create_catchable_api_wrapper_factory(lua)?;

    let decode_array_metatable = array_metatable.clone();
    let decode_null = null.clone();
    let decode_fault = Rc::clone(&fatal_fault);
    let decode_budget = Rc::clone(&execution_budget);
    let native_decode = lua.create_function(move |lua, input: Value| {
        let result = match input {
            Value::String(input) => decode(lua, &input, &decode_array_metatable, &decode_null),
            _ => Err(LuaApiFailure::Api(DECODE_INPUT_TYPE_ERROR)),
        };
        finish_api_call(lua, result, &decode_fault, &decode_budget)
    })?;
    backing.raw_set(
        "decode",
        api_wrapper_factory.call::<Function>(native_decode)?,
    )?;

    let encode_array_metatable = array_metatable.clone();
    let encode_null = null.clone();
    let encode_fault = Rc::clone(&fatal_fault);
    let encode_budget = execution_budget;
    let native_encode = lua.create_function(move |lua, value: Value| {
        let result = encode(value, &encode_array_metatable, &encode_null).and_then(|encoded| {
            lua.create_string(encoded)
                .map(Value::String)
                .map_err(LuaApiFailure::Vm)
        });
        finish_api_call(lua, result, &encode_fault, &encode_budget)
    })?;
    backing.raw_set(
        "encode",
        api_wrapper_factory.call::<Function>(native_encode)?,
    )?;

    let array_fault = Rc::clone(&fatal_fault);
    backing.raw_set(
        "array",
        lua.create_function(move |lua, ()| {
            let result = (|| {
                let array = lua.create_table()?;
                array.set_metatable(Some(array_metatable.clone()))?;
                Ok(array)
            })();
            result.map_err(|error| record_vm_error(&array_fault, error))
        })?,
    )?;
    backing.raw_set("null", null)?;

    publish_readonly_namespace(lua, environment_values, "json", backing, &protected_names)
}

fn decode(
    lua: &Lua,
    input: &LuaString,
    array_metatable: &Table,
    null: &AnyUserData,
) -> JsonResult<Value> {
    let bytes = input.as_bytes();
    let text = from_utf8(bytes.as_ref()).map_err(|_| LuaApiFailure::Api(DECODE_UTF8_ERROR))?;
    let parsed = parse_to_ast(text, &CollectOptions::default(), &STRICT_JSON_OPTIONS)
        .map_err(|_| LuaApiFailure::Api(DECODE_SYNTAX_ERROR))?;
    let value = parsed
        .value
        .ok_or_else(|| LuaApiFailure::Api(DECODE_SYNTAX_ERROR))?;
    decode_value(lua, value, array_metatable, null, 0)
}

fn decode_value(
    lua: &Lua,
    value: JsonValue<'_>,
    array_metatable: &Table,
    null: &AnyUserData,
    depth: usize,
) -> JsonResult<Value> {
    if depth > MAX_JSON_NESTING_DEPTH {
        return Err(LuaApiFailure::ResourceLimitExceeded);
    }

    match value {
        JsonValue::StringLit(value) => lua
            .create_string(value.value.as_bytes())
            .map(Value::String)
            .map_err(LuaApiFailure::Vm),
        JsonValue::NumberLit(value) => decode_number(value.value),
        JsonValue::BooleanLit(value) => Ok(Value::Boolean(value.value)),
        JsonValue::NullKeyword(_) => Ok(Value::UserData(null.clone())),
        JsonValue::Array(value) => {
            let table = lua.create_table()?;
            table.set_metatable(Some(array_metatable.clone()))?;
            for (offset, element) in value.elements.into_iter().enumerate() {
                let index = offset
                    .checked_add(1)
                    .and_then(|value| i64::try_from(value).ok())
                    .ok_or_else(|| LuaApiFailure::Api(DECODE_SYNTAX_ERROR))?;
                table.raw_set(
                    index,
                    decode_value(lua, element, array_metatable, null, depth + 1)?,
                )?;
            }
            Ok(Value::Table(table))
        }
        JsonValue::Object(value) => {
            let table = lua.create_table()?;
            let mut fields = HashSet::with_capacity(value.properties.len());
            for property in value.properties {
                let field = property.name.into_string();
                if !fields.insert(field.clone()) {
                    return Err(LuaApiFailure::Api(DECODE_DUPLICATE_FIELD_ERROR));
                }
                table.raw_set(
                    field,
                    decode_value(lua, property.value, array_metatable, null, depth + 1)?,
                )?;
            }
            Ok(Value::Table(table))
        }
    }
}

fn decode_number(text: &str) -> JsonResult<Value> {
    if text.contains(['.', 'e', 'E']) {
        let value = text
            .parse::<f64>()
            .map_err(|_| LuaApiFailure::Api(DECODE_FINITE_NUMBER_ERROR))?;
        if !value.is_finite() {
            return Err(LuaApiFailure::Api(DECODE_FINITE_NUMBER_ERROR));
        }
        return Ok(Value::Number(value));
    }

    text.parse::<i64>()
        .map(Value::Integer)
        .map_err(|_| LuaApiFailure::Api(DECODE_INTEGER_RANGE_ERROR))
}

fn encode(value: Value, array_metatable: &Table, null: &AnyUserData) -> JsonResult<String> {
    Encoder {
        output: String::new(),
        array_metatable,
        null,
        active_tables: HashSet::new(),
    }
    .encode(value)
}

struct Encoder<'a> {
    output: String,
    array_metatable: &'a Table,
    null: &'a AnyUserData,
    active_tables: HashSet<*const c_void>,
}

impl Encoder<'_> {
    fn encode(mut self, value: Value) -> JsonResult<String> {
        self.encode_value(value, 0)?;
        Ok(self.output)
    }

    fn encode_value(&mut self, value: Value, depth: usize) -> JsonResult<()> {
        if depth > MAX_JSON_NESTING_DEPTH {
            return Err(LuaApiFailure::ResourceLimitExceeded);
        }

        match value {
            Value::Boolean(value) => {
                self.output.push_str(if value { "true" } else { "false" });
            }
            Value::Integer(value) => {
                write!(self.output, "{value}")
                    .map_err(|_| LuaApiFailure::Api(ENCODE_VALUE_ERROR))?;
            }
            Value::Number(value) if value.is_finite() => {
                self.output.push_str(&format_float(value)?);
            }
            Value::String(value) => encode_string(&mut self.output, &value)?,
            Value::Table(value) => self.encode_table(&value, depth)?,
            Value::UserData(value) if value.to_pointer() == self.null.to_pointer() => {
                self.output.push_str("null");
            }
            Value::Nil
            | Value::LightUserData(_)
            | Value::Number(_)
            | Value::Function(_)
            | Value::Thread(_)
            | Value::UserData(_)
            | Value::Error(_)
            | Value::Other(_) => return Err(LuaApiFailure::Api(ENCODE_VALUE_ERROR)),
        }
        Ok(())
    }

    fn encode_table(&mut self, table: &Table, depth: usize) -> JsonResult<()> {
        let pointer = table.to_pointer();
        if !self.active_tables.insert(pointer) {
            return Err(LuaApiFailure::Api(ENCODE_VALUE_ERROR));
        }

        let result = (|| {
            let logical_table = proxy_backing(table)?.unwrap_or_else(|| table.clone());
            let marked_array = table.metatable().is_some_and(|metatable| {
                metatable.to_pointer() == self.array_metatable.to_pointer()
            });
            match classify_table(&logical_table, marked_array)? {
                TableShape::Array(length) => self.encode_array(&logical_table, length, depth + 1),
                TableShape::Object(fields) => self.encode_object(fields, depth + 1),
            }
        })();
        self.active_tables.remove(&pointer);
        result
    }

    fn encode_array(&mut self, table: &Table, length: i64, depth: usize) -> JsonResult<()> {
        self.output.push('[');
        for index in 1..=length {
            if index > 1 {
                self.output.push(',');
            }
            self.encode_value(table.raw_get::<Value>(index)?, depth)?;
        }
        self.output.push(']');
        Ok(())
    }

    fn encode_object(&mut self, fields: Vec<(String, Value)>, depth: usize) -> JsonResult<()> {
        self.output.push('{');
        for (index, (field, value)) in fields.into_iter().enumerate() {
            if index > 0 {
                self.output.push(',');
            }
            encode_utf8_string(&mut self.output, &field)?;
            self.output.push(':');
            self.encode_value(value, depth)?;
        }
        self.output.push('}');
        Ok(())
    }
}

enum TableShape {
    Array(i64),
    Object(Vec<(String, Value)>),
}

fn classify_table(table: &Table, marked_array: bool) -> JsonResult<TableShape> {
    let mut array_length = 0_i64;
    let mut array_entries = 0_usize;
    let mut object_fields = Vec::new();

    for pair in table.clone().pairs::<Value, Value>() {
        let (key, value) = pair?;
        match key {
            Value::Integer(index) if index > 0 && object_fields.is_empty() => {
                array_length = array_length.max(index);
                array_entries = array_entries
                    .checked_add(1)
                    .ok_or_else(|| LuaApiFailure::Api(ENCODE_VALUE_ERROR))?;
            }
            Value::String(field) if array_entries == 0 && !marked_array => {
                let field = field
                    .to_str()
                    .map_err(|_| LuaApiFailure::Api(ENCODE_VALUE_ERROR))?
                    .to_owned();
                object_fields.push((field, value));
            }
            _ => return Err(LuaApiFailure::Api(ENCODE_VALUE_ERROR)),
        }
    }

    if array_entries > 0 {
        if usize::try_from(array_length).ok() != Some(array_entries) {
            return Err(LuaApiFailure::Api(ENCODE_VALUE_ERROR));
        }
        return Ok(TableShape::Array(array_length));
    }
    if marked_array {
        return Ok(TableShape::Array(0));
    }
    object_fields.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    Ok(TableShape::Object(object_fields))
}

fn encode_string(output: &mut String, value: &LuaString) -> JsonResult<()> {
    let value = value
        .to_str()
        .map_err(|_| LuaApiFailure::Api(ENCODE_VALUE_ERROR))?;
    encode_utf8_string(output, value.as_ref())
}

fn encode_utf8_string(output: &mut String, value: &str) -> JsonResult<()> {
    let encoded =
        serde_json::to_string(value).map_err(|_| LuaApiFailure::Api(ENCODE_VALUE_ERROR))?;
    output.push_str(&encoded);
    Ok(())
}

fn format_float(value: f64) -> JsonResult<String> {
    let shortest =
        serde_json::to_string(&value).map_err(|_| LuaApiFailure::Api(ENCODE_VALUE_ERROR))?;
    let (negative, unsigned) = shortest
        .strip_prefix('-')
        .map_or((false, shortest.as_str()), |value| (true, value));
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (
            mantissa,
            exponent
                .parse::<i32>()
                .map_err(|_| LuaApiFailure::Api(ENCODE_VALUE_ERROR))?,
        ),
        None => (unsigned, 0_i32),
    };
    let decimal_offset = mantissa.find('.').unwrap_or(mantissa.len());
    let mut digits = mantissa.replace('.', "");
    let leading_zeros = digits.bytes().take_while(|byte| *byte == b'0').count();
    let decimal_position = i32::try_from(decimal_offset)
        .ok()
        .and_then(|position| position.checked_add(exponent))
        .and_then(|position| {
            i32::try_from(leading_zeros)
                .ok()
                .and_then(|zeros| position.checked_sub(zeros))
        })
        .ok_or_else(|| LuaApiFailure::Api(ENCODE_VALUE_ERROR))?;
    digits.drain(..leading_zeros);
    while digits.ends_with('0') {
        digits.pop();
    }

    if digits.is_empty() {
        return Ok(if negative {
            "-0.0".to_owned()
        } else {
            "0.0".to_owned()
        });
    }

    let plain = plain_float(&digits, decimal_position)?;
    let scientific = scientific_float(&digits, decimal_position)?;
    let unsigned_result = if plain.len() <= scientific.len() {
        plain
    } else {
        scientific
    };
    if negative {
        Ok(format!("-{unsigned_result}"))
    } else {
        Ok(unsigned_result)
    }
}

fn plain_float(digits: &str, decimal_position: i32) -> JsonResult<String> {
    let mut output = String::new();
    if decimal_position <= 0 {
        output.push_str("0.");
        let zeros = usize::try_from(decimal_position.unsigned_abs())
            .map_err(|_| LuaApiFailure::Api(ENCODE_VALUE_ERROR))?;
        output.extend(std::iter::repeat_n('0', zeros));
        output.push_str(digits);
        return Ok(output);
    }

    let position =
        usize::try_from(decimal_position).map_err(|_| LuaApiFailure::Api(ENCODE_VALUE_ERROR))?;
    if position >= digits.len() {
        output.push_str(digits);
        output.extend(std::iter::repeat_n('0', position - digits.len()));
        output.push_str(".0");
        return Ok(output);
    }

    output.push_str(&digits[..position]);
    output.push('.');
    output.push_str(&digits[position..]);
    Ok(output)
}

fn scientific_float(digits: &str, decimal_position: i32) -> JsonResult<String> {
    let mut output = String::new();
    let first = digits
        .chars()
        .next()
        .ok_or_else(|| LuaApiFailure::Api(ENCODE_VALUE_ERROR))?;
    output.push(first);
    if digits.len() > first.len_utf8() {
        output.push('.');
        output.push_str(&digits[first.len_utf8()..]);
    }
    output.push('e');
    let exponent = decimal_position
        .checked_sub(1)
        .ok_or_else(|| LuaApiFailure::Api(ENCODE_VALUE_ERROR))?;
    write!(output, "{exponent}").map_err(|_| LuaApiFailure::Api(ENCODE_VALUE_ERROR))?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::super::tests::lua_source_contract;
    use super::super::{LuaVm, LuaVmErrorKind, ScriptVmLimits};
    use super::{JsonNull, MAX_JSON_NESTING_DEPTH, decode, format_float};
    use mlua::Lua;
    use proptest::prelude::*;
    use proptest::test_runner::TestCaseError;
    use std::collections::HashMap;
    use std::io;
    use std::num::NonZeroUsize;
    use std::time::Duration;

    #[test]
    fn formats_representative_floats_canonically() {
        for (value, expected) in [
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (1.0, "1.0"),
            (0.01, "0.01"),
            (120.0, "120.0"),
            (1000.0, "1e3"),
            (0.000_001, "1e-6"),
            (f64::from_bits(1), "5e-324"),
            (f64::MIN_POSITIVE, "2.2250738585072014e-308"),
            (f64::MAX, "1.7976931348623157e308"),
        ] {
            assert_eq!(format_float(value).ok().as_deref(), Some(expected));
        }
    }

    #[test]
    fn nesting_boundary_succeeds_and_excessive_depth_is_fatal() -> io::Result<()> {
        let limit = MAX_JSON_NESTING_DEPTH;
        let excessive = limit
            .checked_add(1)
            .ok_or_else(|| io::Error::other("JSON nesting test depth must fit usize"))?;
        let within_source = format!(
            r#"
            local within_text = string.rep("[", {limit}) .. "0" .. string.rep("]", {limit})
            assert(json.encode(json.decode(within_text)) == within_text)

            local value = 0
            for _ = 1, {limit} do
                value = {{value}}
            end
            assert(json.encode(value) == within_text)

            function main(event)
            end
            "#
        );
        let memory_bytes = NonZeroUsize::new(4 * 1024 * 1024)
            .ok_or_else(|| io::Error::other("JSON test memory limit must be non-zero"))?;

        LuaVm::load(
            &within_source,
            ScriptVmLimits::try_new(memory_bytes, Duration::from_secs(1))
                .map_err(io::Error::other)?,
            std::num::NonZeroU64::new(262_144)
                .ok_or_else(|| std::io::Error::other("test record limit must be non-zero"))?,
            lua_source_contract()?,
            HashMap::new(),
            None,
            || false,
        )
        .map_err(|error| io::Error::other(error.to_string()))?;

        let decode_source = format!(
            r#"
            local excessive_text =
                string.rep("[", {excessive}) .. "0" .. string.rep("]", {excessive})
            pcall(json.decode, excessive_text)
            error("JSON decode safety limit should escape pcall")
            function main(event)
            end
            "#
        );
        let encode_source = format!(
            r#"
            local value = 0
            for _ = 1, {excessive} do
                value = {{value}}
            end
            pcall(json.encode, value)
            error("JSON encode safety limit should escape pcall")
            function main(event)
            end
            "#
        );
        for source in [decode_source, encode_source] {
            let Err(error) = LuaVm::load(
                &source,
                ScriptVmLimits::try_new(memory_bytes, Duration::from_secs(1))
                    .map_err(io::Error::other)?,
                std::num::NonZeroU64::new(262_144)
                    .ok_or_else(|| std::io::Error::other("test record limit must be non-zero"))?,
                lua_source_contract()?,
                HashMap::new(),
                None,
                || false,
            ) else {
                return Err(io::Error::other(
                    "JSON nesting safety limit should terminate the VM",
                ));
            };
            assert_eq!(error.kind(), LuaVmErrorKind::ResourceLimitExceeded);
        }
        Ok(())
    }

    proptest! {
        #[test]
        fn every_finite_float_round_trips_through_its_canonical_text(
            value in any::<f64>().prop_filter("value must be finite", |value| value.is_finite())
        ) {
            let Ok(encoded) = format_float(value) else {
                return Err(TestCaseError::fail("finite float should encode"));
            };
            let Ok(decoded) = encoded.parse::<f64>() else {
                return Err(TestCaseError::fail("encoded float should parse"));
            };

            prop_assert_eq!(decoded.to_bits(), value.to_bits());
            prop_assert!(encoded.contains(['.', 'e']));
            prop_assert!(!encoded.contains('E'));
            prop_assert!(!encoded.contains("e+"));
            if let Some((_, exponent)) = encoded.split_once('e') {
                let digits = exponent.strip_prefix('-').unwrap_or(exponent);
                prop_assert!(digits == "0" || !digits.starts_with('0'));
            }
        }

        #[test]
        fn arbitrary_input_bytes_never_panic(
            input in proptest::collection::vec(any::<u8>(), 0..2_048)
        ) {
            let lua = Lua::new();
            let Ok(array_metatable) = lua.create_table() else {
                return Err(TestCaseError::fail("array marker should be created"));
            };
            let Ok(null) = lua.create_userdata(JsonNull) else {
                return Err(TestCaseError::fail("JSON null should be created"));
            };
            let Ok(input) = lua.create_string(input) else {
                return Err(TestCaseError::fail("input string should be created"));
            };

            let _result = decode(&lua, &input, &array_metatable, &null);
        }
    }
}
