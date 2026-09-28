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

//! Fixed-width little-endian words inside a byte buffer.
//!
//! Every Tenon shared-memory format — the IPC Queue header, frames, and logical
//! positions as well as the Bell Region header and slots — stores its fields as
//! 32-bit or 64-bit little-endian words at a fixed offset. These four calls are
//! that one encoding rule. A format module owns which offset holds which value;
//! none of them owns how a word is encoded or decoded.

/// Exact byte length of one encoded 32-bit word.
pub(crate) const U32_LEN: usize = 4;

/// Exact byte length of one encoded 64-bit word.
pub(crate) const U64_LEN: usize = 8;

/// Reads the little-endian 32-bit word at `offset`.
///
/// # Panics
///
/// Panics when `input` does not hold four bytes at `offset`. Every caller
/// bounds-checks the buffer against its own fixed layout before reading.
pub(crate) fn read_u32(input: &[u8], offset: usize) -> u32 {
    let mut encoded = [0_u8; U32_LEN];
    encoded.copy_from_slice(&input[offset..offset + U32_LEN]);
    u32::from_le_bytes(encoded)
}

/// Reads the little-endian 64-bit word at `offset`.
///
/// # Panics
///
/// Panics when `input` does not hold eight bytes at `offset`, for the same
/// reason as [`read_u32`].
pub(crate) fn read_u64(input: &[u8], offset: usize) -> u64 {
    let mut encoded = [0_u8; U64_LEN];
    encoded.copy_from_slice(&input[offset..offset + U64_LEN]);
    u64::from_le_bytes(encoded)
}

/// Writes one little-endian 32-bit word at `offset`.
///
/// # Panics
///
/// Panics when `output` does not hold four bytes at `offset`, for the same
/// reason as [`read_u32`].
pub(crate) fn write_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + U32_LEN].copy_from_slice(&value.to_le_bytes());
}

/// Writes one little-endian 64-bit word at `offset`.
///
/// # Panics
///
/// Panics when `output` does not hold eight bytes at `offset`, for the same
/// reason as [`read_u32`].
pub(crate) fn write_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + U64_LEN].copy_from_slice(&value.to_le_bytes());
}
