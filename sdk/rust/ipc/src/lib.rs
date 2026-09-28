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

//! Shared Queue and Bell implementation for Tenon processes and Rust Plugins.
//!
//! Queue endpoints own their mappings and publish only commit or release.
//! A waiting loop owns one Bell slot; endpoints retain that loop while bound.
//! Callers own unique endpoint assignment, business records, shutdown and files.
//! Operating-system waits, atomics and raw mappings remain private here.

#[allow(
    unsafe_code,
    reason = "Bell mappings and platform wait calls require raw shared words"
)]
pub mod bell;
mod little_endian;
pub mod queue;
