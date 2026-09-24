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

//! A finite producer whose completion is observed by a business worker.

use crate::payload::SourceRecordPayload;
use tenon_plugin_sdk::{PayloadSender, TenonSource, Value};

pub(super) struct Source {
    payload: SourceRecordPayload,
    sender: PayloadSender<SourceRecordPayload>,
}

impl Source {
    pub(super) fn new(
        config: &Value,
        sender: PayloadSender<SourceRecordPayload>,
    ) -> Result<Self, tenon_plugin_sdk::Error> {
        let message = config["message"]
            .as_str()
            .ok_or_else(|| std::io::Error::other("message is required when Source is bound"))?;
        Ok(Self {
            payload: SourceRecordPayload {
                message: message.to_owned(),
            },
            sender,
        })
    }
}

impl TenonSource for Source {
    fn start(&mut self) {
        let completion = self
            .sender
            .send(0, &self.payload)
            .expect("Source send failed");
        std::thread::spawn(move || eprintln!("Source completion: {:?}", completion.wait()));
    }

    fn quiesce(&mut self) {
        // This finite producer cannot submit new records after start returns.
    }

    fn close(&mut self) {
        // The SDK has settled the completion; no external resource remains.
    }
}
