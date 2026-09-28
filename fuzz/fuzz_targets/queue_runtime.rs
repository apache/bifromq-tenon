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

#![no_main]

use libfuzzer_sys::fuzz_target;

#[path = "../../sdk/rust/ipc/tests/support/doorbell_queue.rs"]
mod doorbell_queue;
#[path = "../../sdk/rust/ipc/tests/support/ipc_queue_model.rs"]
mod ipc_queue_model;

fuzz_target!(|input: &[u8]| {
    let Some((&capacity_byte, actions)) = input.split_first() else {
        return;
    };
    let capacity = 16 + u64::from(capacity_byte) * 8;
    let payload_limit = (capacity - 8).min(64);
    let actions = actions.chunks(65).take(128).map(|action| {
        let length = (action.len() - 1).min(payload_limit as usize);
        let payload = if length == 0 {
            vec![action[0]]
        } else {
            action[1..=length].to_vec()
        };
        (action[0], payload)
    });
    ipc_queue_model::verify_queue_model(capacity, payload_limit, actions)
        .expect("mapped Queue violated its FIFO/replay oracle");
});
