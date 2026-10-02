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

//! Deterministic byte helpers for the Tenon Lua subset.
//!
//! Each call borrows the input Lua string only for the duration of the call.
//! Stable argument failures remain catchable Lua errors, while VM resource
//! failures poison the VM and escape protected Lua calls.

use super::{
    ExecutionBudget, LuaApiFailure, LuaApiResult, LuaVmFatalFault,
    create_catchable_api_wrapper_factory, create_unsigned_decimal, finish_api_call,
    publish_readonly_namespace,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use mlua::{Function, Lua, LuaString, MultiValue, Table, Value};
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;

const INPUT_TYPE_ERROR: &str = "bytes input must be a string";
const OFFSET_ERROR: &str = "bytes offset must be a positive integer";
const LENGTH_ERROR: &str = "bytes length must be a non-negative integer";
const RANGE_ERROR: &str = "bytes range is out of bounds";
const FLOAT_ERROR: &str = "bytes floating-point input must be finite";
const BCD_ERROR: &str = "bytes BCD input must contain only decimal nibbles";
const HEX_ERROR: &str = "bytes hex input must be even-length ASCII hexadecimal";
const BASE64_ERROR: &str = "bytes base64 input must be canonical padded RFC 4648";
const CRC_PARAMETER_ERROR: &str = "bytes CRC16 parameters must be integers from 0 to 65535";
const CRC_BIT_ORDER_ERROR: &str = "bytes CRC16 bit order must be \"msb\" or \"lsb\"";
const LOWER_HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

type BytesFunction = fn(&Lua, MultiValue) -> BytesResult<Value>;
type BytesResult<T> = LuaApiResult<T>;

pub(super) fn install(
    lua: &Lua,
    environment_values: &Table,
    protected_names: Rc<RefCell<HashSet<Vec<u8>>>>,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
    execution_budget: Rc<RefCell<Option<ExecutionBudget>>>,
) -> mlua::Result<()> {
    let backing = lua.create_table()?;
    let api_wrapper_factory = create_catchable_api_wrapper_factory(lua)?;

    for (name, function) in [
        ("len", len as BytesFunction),
        ("slice", slice),
        ("byte", byte),
        ("read_u8", read_u8),
        ("read_i8", read_i8),
        ("read_u16_be", read_u16_be),
        ("read_u16_le", read_u16_le),
        ("read_i16_be", read_i16_be),
        ("read_i16_le", read_i16_le),
        ("read_u32_be", read_u32_be),
        ("read_u32_le", read_u32_le),
        ("read_i32_be", read_i32_be),
        ("read_i32_le", read_i32_le),
        ("read_u64_be", read_u64_be),
        ("read_u64_le", read_u64_le),
        ("read_i64_be", read_i64_be),
        ("read_i64_le", read_i64_le),
        ("read_f32_be", read_f32_be),
        ("read_f32_le", read_f32_le),
        ("read_f64_be", read_f64_be),
        ("read_f64_le", read_f64_le),
        ("bcd_to_string", bcd_to_string),
        ("to_hex", to_hex),
        ("from_hex", from_hex),
        ("to_base64", to_base64),
        ("from_base64", from_base64),
        ("crc16", crc16),
    ] {
        install_function(
            lua,
            &backing,
            &api_wrapper_factory,
            name,
            function,
            Rc::clone(&fatal_fault),
            Rc::clone(&execution_budget),
        )?;
    }

    publish_readonly_namespace(lua, environment_values, "bytes", backing, &protected_names)
}

fn install_function(
    lua: &Lua,
    backing: &Table,
    wrapper_factory: &Function,
    name: &'static str,
    function: BytesFunction,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
    execution_budget: Rc<RefCell<Option<ExecutionBudget>>>,
) -> mlua::Result<()> {
    let native = lua.create_function(move |lua, arguments: MultiValue| {
        finish_api_call(
            lua,
            function(lua, arguments),
            &fatal_fault,
            &execution_budget,
        )
    })?;
    backing.raw_set(name, wrapper_factory.call::<Function>(native)?)
}

fn len(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let input = input(&arguments)?;
    let length =
        i64::try_from(input.as_bytes().len()).map_err(|_| LuaApiFailure::ResourceLimitExceeded)?;
    Ok(Value::Integer(length))
}

fn slice(lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let input = input(&arguments)?;
    let offset = offset(&arguments)?;
    let length = length(&arguments)?;
    let input_bytes = input.as_bytes();
    let bytes = checked_range(input_bytes.as_ref(), offset, length)?;
    lua.create_string(bytes)
        .map(Value::String)
        .map_err(LuaApiFailure::Vm)
}

fn byte(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<1>(&arguments)?;
    Ok(Value::Integer(i64::from(value[0])))
}

fn read_u8(lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    byte(lua, arguments)
}

fn read_i8(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<1>(&arguments)?;
    Ok(Value::Integer(i64::from(i8::from_be_bytes(value))))
}

fn read_u16_be(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<2>(&arguments)?;
    Ok(Value::Integer(i64::from(u16::from_be_bytes(value))))
}

fn read_u16_le(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<2>(&arguments)?;
    Ok(Value::Integer(i64::from(u16::from_le_bytes(value))))
}

fn read_i16_be(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<2>(&arguments)?;
    Ok(Value::Integer(i64::from(i16::from_be_bytes(value))))
}

fn read_i16_le(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<2>(&arguments)?;
    Ok(Value::Integer(i64::from(i16::from_le_bytes(value))))
}

fn read_u32_be(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<4>(&arguments)?;
    Ok(Value::Integer(i64::from(u32::from_be_bytes(value))))
}

fn read_u32_le(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<4>(&arguments)?;
    Ok(Value::Integer(i64::from(u32::from_le_bytes(value))))
}

fn read_i32_be(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<4>(&arguments)?;
    Ok(Value::Integer(i64::from(i32::from_be_bytes(value))))
}

fn read_i32_le(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<4>(&arguments)?;
    Ok(Value::Integer(i64::from(i32::from_le_bytes(value))))
}

fn read_u64_be(lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<8>(&arguments)?;
    create_unsigned_decimal(lua, u64::from_be_bytes(value))
        .map(Value::String)
        .map_err(LuaApiFailure::Vm)
}

fn read_u64_le(lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<8>(&arguments)?;
    create_unsigned_decimal(lua, u64::from_le_bytes(value))
        .map(Value::String)
        .map_err(LuaApiFailure::Vm)
}

fn read_i64_be(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<8>(&arguments)?;
    Ok(Value::Integer(i64::from_be_bytes(value)))
}

fn read_i64_le(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<8>(&arguments)?;
    Ok(Value::Integer(i64::from_le_bytes(value)))
}

fn read_f32_be(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<4>(&arguments)?;
    finite_number(f64::from(f32::from_be_bytes(value)))
}

fn read_f32_le(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<4>(&arguments)?;
    finite_number(f64::from(f32::from_le_bytes(value)))
}

fn read_f64_be(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<8>(&arguments)?;
    finite_number(f64::from_be_bytes(value))
}

fn read_f64_le(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let value = read_array::<8>(&arguments)?;
    finite_number(f64::from_le_bytes(value))
}

fn finite_number(value: f64) -> BytesResult<Value> {
    if value.is_finite() {
        Ok(Value::Number(value))
    } else {
        Err(LuaApiFailure::Api(FLOAT_ERROR))
    }
}

fn bcd_to_string(lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let input = input(&arguments)?;
    let bytes = input.as_bytes();
    let output_length = bytes
        .len()
        .checked_mul(2)
        .ok_or(LuaApiFailure::ResourceLimitExceeded)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(output_length)
        .map_err(|_| LuaApiFailure::ResourceLimitExceeded)?;
    for &value in bytes.as_ref() {
        let high = value >> 4;
        let low = value & 0x0f;
        if high > 9 || low > 9 {
            return Err(LuaApiFailure::Api(BCD_ERROR));
        }
        output.push(b'0' + high);
        output.push(b'0' + low);
    }
    lua.create_string(output)
        .map(Value::String)
        .map_err(LuaApiFailure::Vm)
}

fn to_hex(lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let input = input(&arguments)?;
    let bytes = input.as_bytes();
    let output_length = bytes
        .len()
        .checked_mul(2)
        .ok_or(LuaApiFailure::ResourceLimitExceeded)?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(output_length)
        .map_err(|_| LuaApiFailure::ResourceLimitExceeded)?;
    for &value in bytes.as_ref() {
        output.push(LOWER_HEX_DIGITS[usize::from(value >> 4)]);
        output.push(LOWER_HEX_DIGITS[usize::from(value & 0x0f)]);
    }
    lua.create_string(output)
        .map(Value::String)
        .map_err(LuaApiFailure::Vm)
}

fn from_hex(lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let input = input(&arguments)?;
    let bytes = input.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(LuaApiFailure::Api(HEX_ERROR));
    }

    let mut output = Vec::new();
    output
        .try_reserve_exact(bytes.len() / 2)
        .map_err(|_| LuaApiFailure::ResourceLimitExceeded)?;
    for pair in bytes.as_chunks::<2>().0 {
        let high = decode_hex_nibble(pair[0]).ok_or_else(|| LuaApiFailure::Api(HEX_ERROR))?;
        let low = decode_hex_nibble(pair[1]).ok_or_else(|| LuaApiFailure::Api(HEX_ERROR))?;
        output.push((high << 4) | low);
    }
    lua.create_string(output)
        .map(Value::String)
        .map_err(LuaApiFailure::Vm)
}

fn to_base64(lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let input = input(&arguments)?;
    let encoded = encode_base64(input.as_bytes().as_ref())?;
    lua.create_string(encoded)
        .map(Value::String)
        .map_err(LuaApiFailure::Vm)
}

fn from_base64(lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let input = input(&arguments)?;
    let bytes = input.as_bytes();
    let decoded = decode_base64(bytes.as_ref())?;
    if encode_base64(&decoded)?.as_slice() != bytes.as_ref() {
        return Err(LuaApiFailure::Api(BASE64_ERROR));
    }
    lua.create_string(decoded)
        .map(Value::String)
        .map_err(LuaApiFailure::Vm)
}

fn encode_base64(input: &[u8]) -> BytesResult<Vec<u8>> {
    let output_length =
        base64::encoded_len(input.len(), true).ok_or(LuaApiFailure::ResourceLimitExceeded)?;
    let mut output = fallible_zeroed_buffer(output_length)?;
    let written = STANDARD
        .encode_slice(input, &mut output)
        .map_err(|_| LuaApiFailure::ResourceLimitExceeded)?;
    output.truncate(written);
    Ok(output)
}

fn decode_base64(input: &[u8]) -> BytesResult<Vec<u8>> {
    let output_length = input
        .len()
        .checked_div(4)
        .and_then(|complete_chunks| {
            complete_chunks.checked_add(usize::from(!input.len().is_multiple_of(4)))
        })
        .and_then(|chunks| chunks.checked_mul(3))
        .ok_or(LuaApiFailure::ResourceLimitExceeded)?;
    let mut output = fallible_zeroed_buffer(output_length)?;
    let written = STANDARD
        .decode_slice(input, &mut output)
        .map_err(|error| match error {
            base64::DecodeSliceError::DecodeError(_) => LuaApiFailure::Api(BASE64_ERROR),
            base64::DecodeSliceError::OutputSliceTooSmall => LuaApiFailure::ResourceLimitExceeded,
        })?;
    output.truncate(written);
    Ok(output)
}

fn fallible_zeroed_buffer(length: usize) -> BytesResult<Vec<u8>> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(length)
        .map_err(|_| LuaApiFailure::ResourceLimitExceeded)?;
    output.resize(length, 0);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::{LuaApiFailure, fallible_zeroed_buffer};

    #[test]
    fn impossible_buffer_capacity_is_reported_as_resource_exhaustion() {
        assert!(matches!(
            fallible_zeroed_buffer(usize::MAX),
            Err(LuaApiFailure::ResourceLimitExceeded)
        ));
    }
}

fn crc16(_lua: &Lua, arguments: MultiValue) -> BytesResult<Value> {
    let input = input(&arguments)?;
    let polynomial = crc_parameter(&arguments, 1)?;
    let initial = crc_parameter(&arguments, 2)?;
    let xor_out = crc_parameter(&arguments, 3)?;
    let bit_order = match arguments.get(4) {
        Some(Value::String(value)) if value.as_bytes().as_ref() == b"msb" => BitOrder::Msb,
        Some(Value::String(value)) if value.as_bytes().as_ref() == b"lsb" => BitOrder::Lsb,
        _ => return Err(LuaApiFailure::Api(CRC_BIT_ORDER_ERROR)),
    };
    let result = match bit_order {
        BitOrder::Msb => crc16_msb(input.as_bytes().as_ref(), polynomial, initial),
        BitOrder::Lsb => crc16_lsb(input.as_bytes().as_ref(), polynomial, initial),
    } ^ xor_out;
    Ok(Value::Integer(i64::from(result)))
}

fn input(arguments: &MultiValue) -> BytesResult<LuaString> {
    match arguments.front() {
        Some(Value::String(value)) => Ok(value.clone()),
        _ => Err(LuaApiFailure::Api(INPUT_TYPE_ERROR)),
    }
}

fn offset(arguments: &MultiValue) -> BytesResult<usize> {
    let value = match arguments.get(1) {
        Some(Value::Integer(value)) if *value > 0 => *value,
        _ => return Err(LuaApiFailure::Api(OFFSET_ERROR)),
    };
    usize::try_from(value)
        .ok()
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| LuaApiFailure::Api(OFFSET_ERROR))
}

fn length(arguments: &MultiValue) -> BytesResult<usize> {
    match arguments.get(2) {
        Some(Value::Integer(value)) if *value >= 0 => {
            usize::try_from(*value).map_err(|_| LuaApiFailure::Api(LENGTH_ERROR))
        }
        _ => Err(LuaApiFailure::Api(LENGTH_ERROR)),
    }
}

fn crc_parameter(arguments: &MultiValue, index: usize) -> BytesResult<u16> {
    match arguments.get(index) {
        Some(Value::Integer(value)) => {
            u16::try_from(*value).map_err(|_| LuaApiFailure::Api(CRC_PARAMETER_ERROR))
        }
        _ => Err(LuaApiFailure::Api(CRC_PARAMETER_ERROR)),
    }
}

fn checked_range(data: &[u8], offset: usize, length: usize) -> BytesResult<&[u8]> {
    let end = offset
        .checked_add(length)
        .ok_or_else(|| LuaApiFailure::Api(RANGE_ERROR))?;
    data.get(offset..end)
        .ok_or_else(|| LuaApiFailure::Api(RANGE_ERROR))
}

fn read_array<const LENGTH: usize>(arguments: &MultiValue) -> BytesResult<[u8; LENGTH]> {
    let input = input(arguments)?;
    let offset = offset(arguments)?;
    checked_range(input.as_bytes().as_ref(), offset, LENGTH)?
        .try_into()
        .map_err(|_| LuaApiFailure::Api(RANGE_ERROR))
}

const fn decode_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum BitOrder {
    Msb,
    Lsb,
}

fn crc16_msb(data: &[u8], polynomial: u16, mut register: u16) -> u16 {
    for &value in data {
        register ^= u16::from(value) << 8;
        for _ in 0..8 {
            let high_bit_set = register & 0x8000 != 0;
            register = register.wrapping_shl(1);
            if high_bit_set {
                register ^= polynomial;
            }
        }
    }
    register
}

fn crc16_lsb(data: &[u8], polynomial: u16, mut register: u16) -> u16 {
    for &value in data {
        register ^= u16::from(value);
        for _ in 0..8 {
            let low_bit_set = register & 1 != 0;
            register >>= 1;
            if low_bit_set {
                register ^= polynomial;
            }
        }
    }
    register
}
