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

//! Observe concurrent Sink writes and acknowledgements over a real TCP connection.
use super::confirmation_tests::{
    PENDING_RECORDS, complete_record, connack_properties, read_submission, receive, send,
    source_fixture,
};
use super::*;
use rumqttc::PubAckReason;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use tenon_plugin_sdk::repository_test_support::SourceFixture;
use v5::{ConnAck, ConnectReturnCode, Packet, PubAck, PubComp, PubRec};

struct PublishingClient {
    owner: Arc<ChannelClient>,
    client: AsyncClient,
    listener: TcpListener,
    stream: TcpStream,
    source: SourceFixture,
    cancel: Option<oneshot::Sender<()>>,
    events: Option<thread::JoinHandle<Result<(), Error>>>,
    writers: Vec<thread::JoinHandle<()>>,
}

impl PublishingClient {
    fn start() -> Self {
        Self::with_properties(None, &[])
    }

    fn with_properties(
        properties: Option<v5::ConnAckProperties>,
        filters: &[Subscription],
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut options = MqttOptions::new(
            "publish-test",
            Broker::tcp("127.0.0.1", listener.local_addr().unwrap().port()),
        );
        options.set_ack_mode(AckMode::Manual);
        let (client, eventloop) = AsyncClient::builder(options)
            .capacity(2)
            .try_build()
            .unwrap();
        let (writes, dispatcher) = Writes::new(client.clone());
        let source_acks = SourceAcks::new(client.clone());
        let control = Arc::new(Mutex::new(SubscriptionControl::new(
            client.clone(),
            filters,
        )));
        let owner = Arc::new(ChannelClient {
            control: control.clone(),
            writes: writes.clone(),
            source_enabled: false,
        });
        let source = source_fixture(1, PENDING_RECORDS);
        let sender = source.sender::<SourceRecordPayload>();
        let (cancel, cancelled) = oneshot::channel();
        let events = thread::spawn(move || {
            Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                tokio::select! {
                    _ = cancelled => Ok(()),
                    result = Connection { channel: 0, eventloop, sender: Some(sender), writes, dispatcher, control, source_acks, configured_clean_start: true }.run() => result,
                }
            })
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(std::time::Instant::now() < deadline, "connect deadline");
                    thread::yield_now();
                }
                Err(error) => panic!("accept client: {error}"),
            }
        };
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        assert!(matches!(receive(&mut stream), Packet::Connect(..)));
        send(
            &mut stream,
            Packet::ConnAck(ConnAck {
                session_present: false,
                code: ConnectReturnCode::Success,
                properties,
            }),
        );
        Self {
            owner,
            client,
            listener,
            stream,
            source,
            cancel: Some(cancel),
            events: Some(events),
            writers: Vec::new(),
        }
    }

    fn write(&mut self, records: Vec<SinkRecordPayload>) -> mpsc::Receiver<Result<(), String>> {
        let owner = self.owner.clone();
        let (completed, completion) = mpsc::channel();
        self.writers.push(thread::spawn(move || {
            let config = Config::parse(&serde_json::json!({
                "endpoint": "mqtt://localhost", "clientIdPrefix": "test-",
            }))
            .unwrap();
            let confirmation = owner.write(records.into_boxed_slice(), &config);
            let result = Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(5), confirmation)
                        .await
                        .map_err(|error| error.to_string())?
                        .map_err(|error| error.to_string())
                });
            let _ = completed.send(result);
        }));
        completion
    }
}

impl Drop for PublishingClient {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        if let Some(events) = self.events.take() {
            let _ = events.join();
        }
        for writer in self.writers.drain(..) {
            let _ = writer.join();
        }
    }
}

fn record(qos: u32, body: &str) -> SinkRecordPayload {
    SinkRecordPayload {
        message: Some(MqttMessage {
            topic: "output".into(),
            qos: Some(qos),
            body: body.as_bytes().to_vec(),
            ..Default::default()
        }),
    }
}

#[test]
fn qos_zero_write_finishes_while_another_write_waits_for_puback() {
    let mut client = PublishingClient::start();
    let first = client.write(vec![record(1, "first")]);
    let Packet::Publish(publish) = receive(&mut client.stream) else {
        panic!("expected publish")
    };
    let second = client.write(vec![record(0, "second")]);
    let Packet::Publish(zero) = receive(&mut client.stream) else {
        panic!("expected QoS 0 publish")
    };
    assert_eq!(zero.payload.as_ref(), b"second");
    assert_eq!(second.recv_timeout(Duration::from_secs(2)).unwrap(), Ok(()));
    assert!(matches!(first.try_recv(), Err(mpsc::TryRecvError::Empty)));
    send(
        &mut client.stream,
        Packet::PubAck(PubAck::new(publish.pkid, None)),
    );
    assert_eq!(first.recv_timeout(Duration::from_secs(2)).unwrap(), Ok(()));
}

#[test]
fn batch_publishes_before_acks_and_waits_for_every_confirmation() {
    let mut client = PublishingClient::start();
    let completion = client.write(vec![
        record(1, "first"),
        record(2, "second"),
        record(0, "third"),
    ]);
    let mut publishes = Vec::new();
    for expected in [b"first".as_slice(), b"second", b"third"] {
        let Packet::Publish(publish) = receive(&mut client.stream) else {
            panic!("expected publish")
        };
        assert_eq!(publish.payload.as_ref(), expected);
        publishes.push(publish);
    }
    send(
        &mut client.stream,
        Packet::PubRec(PubRec::new(publishes[1].pkid, None)),
    );
    assert!(matches!(receive(&mut client.stream), Packet::PubRel(_)));
    send(
        &mut client.stream,
        Packet::PubComp(PubComp::new(publishes[1].pkid, None)),
    );
    assert!(matches!(
        completion.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    send(
        &mut client.stream,
        Packet::PubAck(PubAck::new(publishes[0].pkid, None)),
    );
    assert_eq!(
        completion.recv_timeout(Duration::from_secs(2)).unwrap(),
        Ok(())
    );
}

#[test]
fn concurrent_writes_receive_their_own_out_of_order_acks() {
    let mut client = PublishingClient::start();
    let mut completions = Vec::new();
    for index in 0..16 {
        completions.push(client.write(vec![record(1, &index.to_string())]));
    }
    let mut publishes = Vec::new();
    for _ in 0..16 {
        let Packet::Publish(publish) = receive(&mut client.stream) else {
            panic!("expected publish")
        };
        publishes.push(publish);
    }
    for publish in publishes.into_iter().rev() {
        let index: usize = std::str::from_utf8(&publish.payload)
            .unwrap()
            .parse()
            .unwrap();
        send(
            &mut client.stream,
            Packet::PubAck(PubAck::new(publish.pkid, None)),
        );
        assert_eq!(
            completions[index]
                .recv_timeout(Duration::from_secs(2))
                .unwrap(),
            Ok(())
        );
    }
}

#[test]
fn rejection_fails_batch_while_later_publishes_wait_for_capacity() {
    let mut client = PublishingClient::start();
    let completion = client.write(
        (0..200)
            .map(|index| record(1, &index.to_string()))
            .collect(),
    );
    assert!(matches!(receive(&mut client.stream), Packet::Publish(_)));
    let Packet::Publish(second) = receive(&mut client.stream) else {
        panic!("expected second publish")
    };
    send(
        &mut client.stream,
        Packet::PubAck(PubAck {
            pkid: second.pkid,
            reason: PubAckReason::QuotaExceeded,
            properties: None,
        }),
    );
    assert!(
        completion
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .is_err()
    );
}

#[test]
fn disconnect_fails_every_unconfirmed_write() {
    let mut client = PublishingClient::start();
    let first = client.write(vec![record(1, "first")]);
    let second = client.write(vec![record(2, "second")]);
    for _ in 0..2 {
        assert!(matches!(receive(&mut client.stream), Packet::Publish(_)));
    }
    client.stream.shutdown(std::net::Shutdown::Both).unwrap();
    for completion in [first, second] {
        assert!(
            completion
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap_err()
                .contains("before publish confirmation")
        );
    }
}

#[test]
fn sink_publish_forwards_mqtt5_properties_on_the_wire() {
    use std::time::UNIX_EPOCH;
    let mut client = PublishingClient::start();
    let completion = client.write(vec![SinkRecordPayload {
        message: Some(MqttMessage {
            topic: "commands".into(),
            body: b"payload".to_vec(),
            qos: Some(1),
            retain: Some(false),
            expires_at_unix_ms: Some(
                (SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("test clock")
                    .as_millis() as i64)
                    + 5_000,
            ),
            mqtt5: Some(Mqtt5Properties {
                payload_format_indicator: Some(1),
                content_type: Some("application/json".into()),
                response_topic: Some("responses".into()),
                correlation_data: Some(b"corr".to_vec()),
                user_properties: vec![
                    UserProperty {
                        key: "x".into(),
                        value: "1".into(),
                    },
                    UserProperty {
                        key: "x".into(),
                        value: "2".into(),
                    },
                    UserProperty {
                        key: "y".into(),
                        value: "3".into(),
                    },
                ],
            }),
        }),
    }]);
    let Packet::Publish(publish) = receive(&mut client.stream) else {
        panic!("expected publish")
    };
    let properties = publish.properties.expect("MQTT 5 properties");
    assert_eq!(properties.payload_format_indicator, Some(1));
    assert!(matches!(properties.message_expiry_interval, Some(3..=5)));
    assert_eq!(properties.content_type.as_deref(), Some("application/json"));
    assert_eq!(properties.response_topic.as_deref(), Some("responses"));
    assert_eq!(
        properties.correlation_data.as_deref(),
        Some(b"corr".as_slice())
    );
    assert_eq!(
        properties.user_properties,
        vec![
            ("x".into(), "1".into()),
            ("x".into(), "2".into()),
            ("y".into(), "3".into()),
        ]
    );

    send(
        &mut client.stream,
        Packet::PubAck(PubAck::new(publish.pkid, None)),
    );
    assert_eq!(
        completion.recv_timeout(Duration::from_secs(2)).unwrap(),
        Ok(())
    );
}

/// Reads the next server-to-client PUBLISH, answering the SUBSCRIBE an active
/// Source makes before it admits anything.
fn expect_publish(stream: &mut TcpStream) -> v5::Publish {
    loop {
        match receive(stream) {
            Packet::Publish(publish) => return publish,
            Packet::Subscribe(subscribe) => send(
                stream,
                Packet::SubAck(v5::SubAck {
                    pkid: subscribe.pkid,
                    return_codes: vec![v5::SubscribeReasonCode::Success(subscribe.filters[0].qos)],
                    properties: None,
                }),
            ),
            packet => panic!("unexpected packet: {packet:?}"),
        }
    }
}

/// The Source acknowledgement and QoS 0 progress must not wait for a blocked
/// sink publish when the broker's receive window is full.
#[test]
fn source_ack_and_qos_zero_progress_with_receive_maximum_one() {
    let mut client = PublishingClient::with_properties(
        Some(v5::ConnAckProperties {
            receive_max: Some(1),
            ..connack_properties()
        }),
        &[Subscription {
            filter: "input/#".into(),
            qos: 2,
        }],
    );
    client.owner.control.lock().unwrap().activate();
    let sink = client.write(vec![record(1, "held")]);
    let held = expect_publish(&mut client.stream);
    let zero = client.write(vec![record(0, "zero")]);
    assert_eq!(expect_publish(&mut client.stream).qos, QoS::AtMostOnce);
    assert_eq!(zero.recv_timeout(Duration::from_secs(2)).unwrap(), Ok(()));
    let mut submission = client.source.submission(0).expect("Submission Queue");
    let mut incoming = v5::Publish::new("input", QoS::ExactlyOnce, b"source".to_vec(), None);
    incoming.pkid = 42;
    send(&mut client.stream, Packet::Publish(incoming));
    let (record_id, payload) = read_submission(&mut submission);
    let message = payload.message.expect("Source message");
    assert_eq!(message.body, b"source");
    let mut completion = client.source.completion(0).expect("Completion Queue");
    complete_record(&mut completion, record_id);
    assert!(matches!(receive(&mut client.stream), Packet::PubRec(ack) if ack.pkid == 42));
    send(
        &mut client.stream,
        Packet::PubRel(v5::PubRel::new(42, None)),
    );
    assert!(matches!(receive(&mut client.stream), Packet::PubComp(ack) if ack.pkid == 42));
    assert!(matches!(sink.try_recv(), Err(mpsc::TryRecvError::Empty)));
    send(
        &mut client.stream,
        Packet::PubAck(PubAck::new(held.pkid, None)),
    );
    assert_eq!(sink.recv_timeout(Duration::from_secs(2)).unwrap(), Ok(()));
}

#[test]
fn pubrel_and_subscribe_reserve_identifiers_while_new_publishes_continue() {
    let mut client = PublishingClient::with_properties(
        Some(v5::ConnAckProperties {
            receive_max: Some(3),
            ..connack_properties()
        }),
        &[],
    );
    let first = client.write(vec![record(2, "await pubcomp")]);
    let Packet::Publish(first_publish) = receive(&mut client.stream) else {
        panic!("expected first publish")
    };
    send(
        &mut client.stream,
        Packet::PubRec(PubRec::new(first_publish.pkid, None)),
    );
    assert!(matches!(receive(&mut client.stream), Packet::PubRel(_)));
    let subscribe = client
        .client
        .try_subscribe_tracked("input/#", QoS::AtLeastOnce)
        .unwrap();
    let Packet::Subscribe(subscription) = receive(&mut client.stream) else {
        panic!("expected subscribe")
    };
    assert_ne!(subscription.pkid, first_publish.pkid);
    for body in ["second", "third"] {
        let next = client.write(vec![record(1, body)]);
        let Packet::Publish(publish) = receive(&mut client.stream) else {
            panic!("expected next publish")
        };
        assert_ne!(publish.pkid, first_publish.pkid);
        assert_ne!(publish.pkid, subscription.pkid);
        send(
            &mut client.stream,
            Packet::PubAck(PubAck::new(publish.pkid, None)),
        );
        assert_eq!(next.recv_timeout(Duration::from_secs(2)).unwrap(), Ok(()));
    }
    send(
        &mut client.stream,
        Packet::SubAck(v5::SubAck {
            pkid: subscription.pkid,
            return_codes: vec![v5::SubscribeReasonCode::Success(QoS::AtLeastOnce)],
            properties: None,
        }),
    );
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(2), subscribe.wait_completion_async())
                .await
                .unwrap()
                .unwrap();
        });
    send(
        &mut client.stream,
        Packet::PubComp(PubComp::new(first_publish.pkid, None)),
    );
    assert_eq!(first.recv_timeout(Duration::from_secs(2)).unwrap(), Ok(()));
}

#[test]
fn out_of_range_qos_does_not_wrap_to_qos_zero() {
    let mut client = PublishingClient::start();
    let done = client.write(vec![record(256, "invalid")]);
    assert!(
        done.recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap_err()
            .contains("sink qos")
    );
}

#[test]
fn admission_order_and_success_do_not_depend_on_observer_polling() {
    let mut client = PublishingClient::start();
    let config = Config::parse(
        &serde_json::json!({"endpoint": "mqtt://localhost", "clientIdPrefix": "test-"}),
    )
    .unwrap();
    let mut observers = Vec::new();
    for index in 0..32 {
        observers.push(client.owner.write(
            vec![record(1, &index.to_string())].into_boxed_slice(),
            &config,
        ));
    }
    let mut acknowledgements = bytes::BytesMut::new();
    for index in 0..32 {
        let Packet::Publish(publish) = receive(&mut client.stream) else {
            panic!("expected admitted publish")
        };
        assert_eq!(publish.payload.as_ref(), index.to_string().as_bytes());
        Packet::PubAck(PubAck::new(publish.pkid, None))
            .write(&mut acknowledgements, None)
            .unwrap();
    }
    std::io::Write::write_all(&mut client.stream, &acknowledgements).unwrap();
    client.stream.shutdown(std::net::Shutdown::Write).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let mut resumed = loop {
        match client.listener.accept() {
            Ok((stream, _)) => break stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "completed writes must allow reconnect"
                );
                thread::yield_now();
            }
            Err(error) => panic!("accept reconnect: {error}"),
        }
    };
    resumed.set_nonblocking(false).unwrap();
    resumed
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    assert!(matches!(receive(&mut resumed), Packet::Connect(..)));
    send(
        &mut resumed,
        Packet::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success,
            properties: None,
        }),
    );
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            // Reverse observation after a bulk ACK and immediate disconnect.
            for observer in observers.into_iter().rev() {
                tokio::time::timeout(Duration::from_secs(2), observer)
                    .await
                    .unwrap()
                    .unwrap();
            }
        });
}

#[test]
fn dropping_an_observer_does_not_cancel_the_admitted_batch() {
    let mut client = PublishingClient::start();
    let config = Config::parse(
        &serde_json::json!({"endpoint": "mqtt://localhost", "clientIdPrefix": "test-"}),
    )
    .unwrap();
    let observer = client.owner.write(
        vec![record(1, "first"), record(1, "second")].into_boxed_slice(),
        &config,
    );
    drop(observer);
    for expected in [b"first".as_slice(), b"second"] {
        let Packet::Publish(publish) = receive(&mut client.stream) else {
            panic!("expected owned publish")
        };
        assert_eq!(publish.payload.as_ref(), expected);
        send(
            &mut client.stream,
            Packet::PubAck(PubAck::new(publish.pkid, None)),
        );
    }
}

#[test]
fn saturated_publish_owner_does_not_block_write_admission() {
    let mut client = PublishingClient::start();
    let owner = client.owner.clone();
    let (admitted, admission) = mpsc::channel();
    client.writers.push(thread::spawn(move || {
        let config = Config::parse(
            &serde_json::json!({"endpoint": "mqtt://localhost", "clientIdPrefix": "test-"}),
        )
        .unwrap();
        for index in 0..96 {
            // Shutdown joins this synchronous SDK call before closing its owner.
            drop(owner.write(
                vec![record(1, &index.to_string())].into_boxed_slice(),
                &config,
            ));
        }
        let _ = admitted.send(());
    }));
    for _ in 0..32 {
        assert!(matches!(receive(&mut client.stream), Packet::Publish(_)));
    }
    // The peer remains connected without acknowledging any of the publications.
    assert!(
        admission.recv_timeout(Duration::from_secs(2)).is_ok(),
        "write admission must return before the SDK can close the owner"
    );
}
