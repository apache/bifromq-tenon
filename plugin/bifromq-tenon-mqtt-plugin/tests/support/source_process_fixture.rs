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

//! Runs the real MQTT Source lifecycle inside the real SDK with observable MQTT requests.
#[path = "../../src/config/mod.rs"]
mod config;
#[path = "../../src/payload/mod.rs"]
mod payload;
#[path = "../../src/source/mod.rs"]
mod source;

#[allow(
    dead_code,
    reason = "The Source probe shares the production MQTT client types"
)]
mod mqtt {
    include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/mqtt/mod.rs"));

    /// Drives the production Source lifecycle over a client whose every request
    /// is observable, so "did not acknowledge or retry" can be asserted.
    pub(crate) struct Probe {
        source: Source,
        inner: Arc<Inner>,
        sender: PayloadSender<SourceRecordPayload>,
        requests: flume::Receiver<rumqttc::Request>,
    }

    impl Probe {
        pub(crate) fn new(
            _: Value,
            n: usize,
            sender: PayloadSender<SourceRecordPayload>,
        ) -> Result<Self, Error> {
            assert_eq!(n, 1);
            let config = Config::parse(&serde_json::json!({
                "endpoint": "mqtt://localhost",
                "clientIdPrefix": "test-",
                "source": {"subscriptions": [{"filter": "input"}], "maxPendingMessages": 1},
            }))?;
            let (requests, observable) = flume::unbounded();
            let client = AsyncClient::from_senders(requests);
            let control = Arc::new(Mutex::new(SubscriptionControl::new(
                client.clone(),
                subscriptions(&config),
            )));
            let inner = Arc::new(Inner {
                config,
                clients: vec![ChannelClient {
                    control,
                    writes: Writes::new(client).0,
                    source_enabled: true,
                }],
            });
            Ok(Self {
                source: Source::new(inner.clone()),
                inner,
                sender,
                requests: observable,
            })
        }
    }

    impl TenonSource for Probe {
        fn start(&mut self) {
            self.source.start();
        }
        fn quiesce(&mut self) {
            // The SDK closed its admission before this business quiesce ran, so a
            // record arriving now has to be refused without a panic, a retry, or an
            // acknowledgement. Only then does the broker-facing side stop too.
            self.source.quiesce();
            assert!(
                !self.inner.clients[0]
                    .control
                    .lock()
                    .expect("subscription control lock")
                    .admits(),
                "a quiesced Source must stop admitting records"
            );
            let completion = self
                .sender
                .send(0, &SourceRecordPayload::default())
                .expect("the Source has channel 0");
            let refused = completion.wait();
            assert!(
                matches!(refused, Err(ref error) if error.is_session_closed()),
                "closed SDK admission must refuse the record instead of admitting it"
            );
            assert!(
                self.requests.is_empty(),
                "a refused record must not be acknowledged, retried, or resubscribed"
            );
            println!("admission-race-checked");
        }
        fn close(&mut self) {
            self.source.close();
            println!("source-close");
        }
    }
}

fn main() {
    tenon_plugin_sdk::SourceProgram::run(mqtt::Probe::new).await_shutdown();
}
