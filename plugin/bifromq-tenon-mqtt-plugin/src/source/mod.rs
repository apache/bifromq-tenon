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

//! The Source's lifecycle, which is all that belongs to the Program.
//!
//! Each MQTT client event loop owns the records it admits: it encodes them,
//! holds their in-flight results and answers the broker for them. Nothing here
//! carries a record, so this type only tells the connections when they may
//! admit one and when they must stop.

use crate::mqtt::Inner;
use std::sync::Arc;
use tenon_plugin_sdk::TenonSource;

pub struct Source {
    inner: Arc<Inner>,
}

impl Source {
    pub(crate) fn new(inner: Arc<Inner>) -> Self {
        Self { inner }
    }
}

impl TenonSource for Source {
    fn start(&mut self) {
        for channel in self
            .inner
            .clients
            .iter()
            .filter(|channel| channel.source_enabled)
        {
            channel
                .control
                .lock()
                .expect("subscription control lock")
                .activate();
        }
    }

    fn quiesce(&mut self) {
        for channel in self
            .inner
            .clients
            .iter()
            .filter(|channel| channel.source_enabled)
        {
            channel
                .control
                .lock()
                .expect("subscription control lock")
                .quiesce();
        }
    }

    fn close(&mut self) {
        for channel in self
            .inner
            .clients
            .iter()
            .filter(|channel| channel.source_enabled)
        {
            channel
                .control
                .lock()
                .expect("subscription control lock")
                .close();
        }
    }
}
