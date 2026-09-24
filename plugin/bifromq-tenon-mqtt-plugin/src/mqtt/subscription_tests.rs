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

//! Observe subscriptions on the wire using the production event loop.
use super::confirmation_tests::{
    complete_record, connack_properties, read_submission, receive, send, source_fixture,
    try_submission,
};
use super::*;
use rumqttc::PubCompReason;
use std::net::{TcpListener, TcpStream};
use std::thread;
use tenon_ipc::queue::{QueueReader, QueueWriter};
use tenon_plugin_sdk::repository_test_support::SourceFixture;
use v5::{ConnAck, ConnectReturnCode, Packet, SubAck, SubscribeReasonCode};

/// One Source Channel admits this many records before it refuses one. It is
/// larger than any burst these tests deliver, so admission never decides them.
const ADMISSION_WINDOW: usize = 128;

struct RunningClient {
    listener: TcpListener,
    client: AsyncClient,
    control: Arc<Mutex<SubscriptionControl>>,
    /// Held for as long as the Connection runs: dropping it closes the session.
    _source: SourceFixture,
    submission: Mutex<QueueReader>,
    completion: Mutex<QueueWriter>,
    cancel: Option<oneshot::Sender<()>>,
    events: Option<thread::JoinHandle<Result<(), Error>>>,
}

impl RunningClient {
    fn start() -> Self {
        let running = Self::dormant(&[Subscription {
            filter: "input/#".into(),
            qos: 1,
        }]);
        running.control.lock().unwrap().activate();
        running
    }

    fn dormant(filters: &[Subscription]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("broker listener");
        listener.set_nonblocking(true).unwrap();
        let mut options = MqttOptions::new(
            "subscription-test",
            Broker::tcp("127.0.0.1", listener.local_addr().unwrap().port()),
        );
        options.set_clean_start(false);
        options.set_ack_mode(AckMode::Manual);
        options.set_session_expiry_interval(Some(86400));
        options.set_keep_alive(5);
        let (client, eventloop) = AsyncClient::builder(options)
            .capacity(32)
            .try_build()
            .unwrap();
        let (writes, dispatcher) = Writes::new(client.clone());
        let source_acks = SourceAcks::new(client.clone());
        let control = Arc::new(Mutex::new(SubscriptionControl::new(
            client.clone(),
            filters,
        )));
        let event_control = control.clone();
        let source = source_fixture(1, ADMISSION_WINDOW);
        let sender = source.sender::<SourceRecordPayload>();
        let submission = Mutex::new(source.submission(0).expect("Submission Queue"));
        let completion = Mutex::new(source.completion(0).expect("Completion Queue"));
        let (cancel, cancelled) = oneshot::channel();
        let events = thread::spawn(move || {
            Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
                tokio::select! {
                    _ = cancelled => Ok(()),
                    result = Connection { channel: 0, eventloop, sender: Some(sender), writes, dispatcher, control: event_control, source_acks, configured_clean_start: false }.run() => result,
                }
            })
        });
        Self {
            listener,
            client,
            control,
            _source: source,
            submission,
            completion,
            cancel: Some(cancel),
            events: Some(events),
        }
    }

    fn connect(&self, session_present: bool) -> TcpStream {
        self.connect_with_properties(session_present, None)
    }

    fn connect_with_properties(
        &self,
        session_present: bool,
        properties: Option<v5::ConnAckProperties>,
    ) -> TcpStream {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut stream = loop {
            match self.listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(std::time::Instant::now() < deadline, "reconnect deadline");
                    thread::yield_now();
                }
                Err(error) => panic!("accept MQTT client: {error}"),
            }
        };
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let Packet::Connect(connect, _, _) = receive(&mut stream) else {
            panic!("expected CONNECT")
        };
        assert!(
            !connect.clean_start,
            "cleanStart must follow test configuration"
        );
        send(
            &mut stream,
            Packet::ConnAck(ConnAck {
                session_present,
                code: ConnectReturnCode::Success,
                properties,
            }),
        );
        stream
    }

    fn subscribe(&self, stream: &mut TcpStream) {
        let packet = receive(stream);
        let Packet::Subscribe(subscribe) = packet else {
            panic!("expected SUBSCRIBE after CONNACK, got {packet:?}");
        };
        assert_eq!(subscribe.filters.len(), 1);
        assert_eq!(subscribe.filters[0].path, "input/#");
        send(
            stream,
            Packet::SubAck(SubAck {
                pkid: subscribe.pkid,
                return_codes: vec![SubscribeReasonCode::Success(subscribe.filters[0].qos)],
                properties: None,
            }),
        );
    }

    fn barrier(&self, stream: &mut TcpStream) {
        self.client
            .try_publish(
                "barrier",
                b"marker".as_slice(),
                PublishOptions::new(QoS::AtMostOnce),
            )
            .unwrap();
        let packet = receive(stream);
        assert!(
            matches!(packet, Packet::Publish(_)),
            "unexpected control packet before barrier: {packet:?}"
        );
    }

    fn fail(&mut self, expected: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !self.events.as_ref().unwrap().is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "event loop must report {expected}"
            );
            thread::yield_now();
        }
        let error = self.events.take().unwrap().join().unwrap().unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }

    /// Sends one broker PUBLISH and returns the record the Source admitted.
    fn deliver_message(
        &self,
        stream: &mut TcpStream,
        publish: v5::Publish,
    ) -> (u64, SourceRecordPayload) {
        send(stream, Packet::Publish(publish));
        read_submission(&mut self.submission.lock().expect("Submission Queue lock"))
    }

    fn deliver(&self, stream: &mut TcpStream, body: &[u8]) -> u64 {
        let (record_id, payload) = self.deliver_message(
            stream,
            v5::Publish::new("input/one", QoS::AtMostOnce, body.to_vec(), None),
        );
        assert_eq!(payload.message.expect("Source message").body, body);
        record_id
    }

    /// Commits the terminal result its Flow Channel loop would write.
    fn complete(&self, record_id: u64) {
        complete_record(
            &mut self.completion.lock().expect("Completion Queue lock"),
            record_id,
        );
    }

    /// Asserts the Channel holds no record beyond the ones already read.
    fn admits_nothing(&self) {
        assert!(
            try_submission(&mut self.submission.lock().expect("Submission Queue lock")).is_none(),
            "the Source must not admit another record"
        );
    }
}

#[test]
fn individual_subscriptions_fit_broker_packet_limit() {
    let filters: Vec<_> = (0..40)
        .map(|i| Subscription {
            filter: format!("input/{i}"),
            qos: 1,
        })
        .collect();
    let running = RunningClient::dormant(&filters);
    running.control.lock().unwrap().activate();
    let mut stream = running.connect_with_properties(
        false,
        Some(v5::ConnAckProperties {
            max_packet_size: Some(256),
            ..connack_properties()
        }),
    );
    for filter in &filters {
        let Packet::Subscribe(subscribe) = receive(&mut stream) else {
            panic!("expected individual SUBSCRIBE");
        };
        assert_eq!(subscribe.filters.len(), 1);
        assert_eq!(subscribe.filters[0].path, filter.filter);
        send(
            &mut stream,
            Packet::SubAck(SubAck {
                pkid: subscribe.pkid,
                return_codes: vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
                properties: None,
            }),
        );
    }
    running.deliver(&mut stream, b"all subscriptions ready");
    running.barrier(&mut stream);
}

#[test]
fn qos_two_pubrel_finishes_after_session_resume() {
    let running = RunningClient::dormant(&[Subscription {
        filter: "input/#".into(),
        qos: 2,
    }]);
    running.control.lock().unwrap().activate();
    let mut stream = running.connect(false);
    let Packet::Subscribe(subscribe) = receive(&mut stream) else {
        panic!("expected SUBSCRIBE")
    };
    send(
        &mut stream,
        Packet::SubAck(SubAck {
            pkid: subscribe.pkid,
            return_codes: vec![SubscribeReasonCode::Success(QoS::ExactlyOnce)],
            properties: None,
        }),
    );
    let mut publish = v5::Publish::new("input/one", QoS::ExactlyOnce, b"record".to_vec(), None);
    publish.pkid = 42;
    let (record_id, _) = running.deliver_message(&mut stream, publish);
    // This is the same acknowledgement call the Source makes after Completion::Ok.
    running.complete(record_id);
    assert!(matches!(receive(&mut stream), Packet::PubRec(ack) if ack.pkid == 42));
    drop(stream);
    let mut stream = running.connect(true);
    send(&mut stream, Packet::PubRel(v5::PubRel::new(42, None)));
    assert!(
        matches!(receive(&mut stream), Packet::PubComp(ack) if ack.pkid == 42 && ack.reason == PubCompReason::Success)
    );
    // A lost PUBCOMP must also be recoverable, without another business delivery.
    drop(stream);
    let mut stream = running.connect(true);
    send(&mut stream, Packet::PubRel(v5::PubRel::new(42, None)));
    assert!(
        matches!(receive(&mut stream), Packet::PubComp(ack) if ack.pkid == 42 && ack.reason == PubCompReason::PacketIdentifierNotFound)
    );
    running.barrier(&mut stream);
    running.admits_nothing();
}

#[test]
fn qos_two_replayed_publish_does_not_repeat_delivery_or_ack_before_completion() {
    let running = RunningClient::dormant(&[Subscription {
        filter: "input/#".into(),
        qos: 2,
    }]);
    running.control.lock().unwrap().activate();
    let mut stream = running.connect(false);
    running.subscribe(&mut stream);
    let mut publish = v5::Publish::new("input/one", QoS::ExactlyOnce, b"record".to_vec(), None);
    publish.pkid = 42;
    let (record_id, _) = running.deliver_message(&mut stream, publish.clone());
    publish.dup = true;
    send(&mut stream, Packet::Publish(publish.clone()));
    // Same-direction marker proves that the replay was read without another delivery.
    running.deliver(&mut stream, b"after replay");
    running.barrier(&mut stream); // No premature PUBREC may precede this output.
    running.complete(record_id);
    assert!(matches!(receive(&mut stream), Packet::PubRec(ack) if ack.pkid == 42));
    drop(stream);
    let mut stream = running.connect(true);
    send(&mut stream, Packet::Publish(publish));
    assert!(matches!(receive(&mut stream), Packet::PubRec(ack) if ack.pkid == 42));
    send(&mut stream, Packet::PubRel(v5::PubRel::new(42, None)));
    assert!(
        matches!(receive(&mut stream), Packet::PubComp(ack) if ack.reason == PubCompReason::Success)
    );
    running.deliver(&mut stream, b"after resumed replay");
}

#[test]
fn lost_session_accepts_a_new_qos_two_publish_with_the_old_identifier() {
    let running = RunningClient::dormant(&[Subscription {
        filter: "input/#".into(),
        qos: 2,
    }]);
    running.control.lock().unwrap().activate();
    let mut stream = running.connect(false);
    running.subscribe(&mut stream);
    for body in [b"old".as_slice(), b"new"] {
        let mut publish = v5::Publish::new("input/one", QoS::ExactlyOnce, body.to_vec(), None);
        publish.pkid = 42;
        let (record_id, payload) = running.deliver_message(&mut stream, publish);
        assert_eq!(payload.message.expect("Source message").body, body);
        running.complete(record_id);
        assert!(matches!(receive(&mut stream), Packet::PubRec(_)));
        if body == b"old" {
            drop(stream);
            stream = running.connect(false);
            running.subscribe(&mut stream);
        }
    }
    send(&mut stream, Packet::PubRel(v5::PubRel::new(42, None)));
    assert!(
        matches!(receive(&mut stream), Packet::PubComp(ack) if ack.reason == PubCompReason::Success)
    );
}

#[test]
fn broker_only_session_without_local_state_fails_explicitly() {
    let mut running = RunningClient::start();
    let _stream = running.connect(true);
    running.fail("session_present=true");
}

impl Drop for RunningClient {
    fn drop(&mut self) {
        self.control.lock().unwrap().close();
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        if let Some(events) = self.events.take() {
            let _ = events.join();
        }
    }
}

#[test]
fn lost_broker_session_restores_subscription_and_source_messages() {
    let running = RunningClient::start();
    let mut stream = running.connect(false);
    running.subscribe(&mut stream);
    running.deliver(&mut stream, b"before restart");
    drop(stream);
    let mut stream = running.connect(false);
    running.subscribe(&mut stream);
    running.deliver(&mut stream, b"after restart");
    running.barrier(&mut stream);
}

#[test]
fn resumed_confirmed_session_keeps_subscription_without_duplicate() {
    let running = RunningClient::start();
    let mut stream = running.connect(false);
    running.subscribe(&mut stream);
    running.deliver(&mut stream, b"before reconnect");
    drop(stream);
    let mut stream = running.connect(true);
    running.deliver(&mut stream, b"resumed session");
    running.barrier(&mut stream);
}

#[test]
fn activation_after_connect_wakes_event_loop_and_subscribes() {
    let running = RunningClient::dormant(&[Subscription {
        filter: "input/#".into(),
        qos: 1,
    }]);
    let mut stream = running.connect(false);
    running.barrier(&mut stream);
    running.control.lock().unwrap().activate();
    running.subscribe(&mut stream);
    running.deliver(&mut stream, b"activated");
    running.barrier(&mut stream);
}

#[test]
fn reconnect_before_suback_reissues_one_complete_batch() {
    let running = RunningClient::start();
    let mut stream = running.connect(false);
    assert!(matches!(receive(&mut stream), Packet::Subscribe(_)));
    drop(stream);
    let mut stream = running.connect(true);
    running.subscribe(&mut stream);
    running.deliver(&mut stream, b"confirmed after reconnect");
    running.barrier(&mut stream);
}

#[test]
fn rejected_or_malformed_suback_fails_the_event_loop() {
    for response in ["reject", "wrong-id", "wrong-count"] {
        let mut running = RunningClient::start();
        let mut stream = running.connect(false);
        let Packet::Subscribe(subscribe) = receive(&mut stream) else {
            panic!("expected SUBSCRIBE");
        };
        send(
            &mut stream,
            Packet::SubAck(SubAck {
                pkid: if response == "wrong-id" {
                    subscribe.pkid + 1
                } else {
                    subscribe.pkid
                },
                return_codes: match response {
                    "reject" => vec![SubscribeReasonCode::NotAuthorized],
                    "wrong-count" => vec![SubscribeReasonCode::Success(QoS::AtLeastOnce); 2],
                    _ => vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
                },
                properties: None,
            }),
        );
        running.fail(if response == "wrong-id" {
            "unsolicited ack"
        } else {
            "SUBACK"
        });
    }
}

#[test]
fn quiesce_before_suback_unsubscribes_and_late_ack_cannot_restart_it() {
    let running = RunningClient::start();
    let mut stream = running.connect(false);
    let Packet::Subscribe(subscribe) = receive(&mut stream) else {
        panic!("expected SUBSCRIBE");
    };
    running.control.lock().unwrap().quiesce();
    send(
        &mut stream,
        Packet::SubAck(SubAck {
            pkid: subscribe.pkid,
            return_codes: vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
            properties: None,
        }),
    );
    let Packet::Unsubscribe(unsubscribe) = receive(&mut stream) else {
        panic!("expected UNSUBSCRIBE");
    };
    send(
        &mut stream,
        Packet::UnsubAck(v5::UnsubAck {
            pkid: unsubscribe.pkid,
            reasons: vec![v5::UnsubAckReason::Success],
            properties: None,
        }),
    );
    running.barrier(&mut stream);
    drop(stream);
    let mut stream = running.connect(true);
    running.barrier(&mut stream);
    drop(stream);
    let mut stream = running.connect(false);
    running.barrier(&mut stream);
}

#[test]
fn interrupted_unsubscribe_resumes_only_when_broker_session_survives() {
    for session_present in [true, false] {
        let running = RunningClient::start();
        let mut stream = running.connect(false);
        running.subscribe(&mut stream);
        running.deliver(&mut stream, b"ready");
        running.control.lock().unwrap().quiesce();
        assert!(matches!(receive(&mut stream), Packet::Unsubscribe(_)));
        drop(stream);
        let mut stream = running.connect(session_present);
        if session_present {
            let Packet::Unsubscribe(unsubscribe) = receive(&mut stream) else {
                panic!("expected resumed UNSUBSCRIBE");
            };
            send(
                &mut stream,
                Packet::UnsubAck(v5::UnsubAck {
                    pkid: unsubscribe.pkid,
                    reasons: vec![v5::UnsubAckReason::NoSubscriptionExisted],
                    properties: None,
                }),
            );
        }
        running.barrier(&mut stream);
    }
}

#[test]
fn individual_subscription_rejection_reports_the_filter_index() {
    let filters: Vec<_> = (0..40)
        .map(|i| Subscription {
            filter: format!("input/{i}"),
            qos: 1,
        })
        .collect();
    let mut running = RunningClient::dormant(&filters);
    running.control.lock().unwrap().activate();
    let mut stream = running.connect(false);
    for (index, expected) in filters.iter().enumerate().take(18) {
        let Packet::Subscribe(subscribe) = receive(&mut stream) else {
            panic!("expected individual SUBSCRIBE");
        };
        assert_eq!(subscribe.filters.len(), 1);
        assert_eq!(subscribe.filters[0].path, expected.filter);
        send(
            &mut stream,
            Packet::SubAck(SubAck {
                pkid: subscribe.pkid,
                return_codes: vec![if index == 17 {
                    SubscribeReasonCode::NotAuthorized
                } else {
                    SubscribeReasonCode::Success(QoS::AtLeastOnce)
                }],
                properties: None,
            }),
        );
    }
    running.fail("subscription 17");
}

#[test]
fn partially_confirmed_subscription_set_continues_after_reconnect() {
    let filters: Vec<_> = (0..3)
        .map(|index| Subscription {
            filter: format!("input/{index}"),
            qos: 1,
        })
        .collect();
    let running = RunningClient::dormant(&filters);
    running.control.lock().unwrap().activate();
    let mut stream = running.connect(false);
    let Packet::Subscribe(first) = receive(&mut stream) else {
        panic!("expected first subscription")
    };
    send(
        &mut stream,
        Packet::SubAck(SubAck {
            pkid: first.pkid,
            return_codes: vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
            properties: None,
        }),
    );
    assert!(matches!(receive(&mut stream), Packet::Subscribe(_)));
    drop(stream);
    let mut stream = running.connect(true);
    for expected in filters.into_iter().skip(1) {
        let Packet::Subscribe(subscribe) = receive(&mut stream) else {
            panic!("expected restored subscription")
        };
        assert_eq!(subscribe.filters.len(), 1);
        assert_eq!(subscribe.filters[0].path, expected.filter);
        send(
            &mut stream,
            Packet::SubAck(SubAck {
                pkid: subscribe.pkid,
                return_codes: vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
                properties: None,
            }),
        );
    }
    running.deliver(&mut stream, b"complete set restored");
    running.barrier(&mut stream);
}

#[test]
fn missing_suback_fails_even_when_broker_answers_keepalive() {
    use std::io::Read;
    let mut running = RunningClient::start();
    let mut stream = running.connect(false);
    assert!(matches!(receive(&mut stream), Packet::Subscribe(_)));
    stream
        .set_read_timeout(Some(Duration::from_secs(7)))
        .unwrap();
    loop {
        let mut ping = [0; 2];
        match stream.read_exact(&mut ping) {
            Ok(()) => {
                assert_eq!(ping, [0xc0, 0]);
                send(&mut stream, Packet::PingResp);
            }
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => panic!("broker connection failed before subscription timeout: {error}"),
        }
    }
    running.fail("subscription acknowledgement timed out");
}

#[test]
fn old_completion_cannot_ack_new_message_in_a_replaced_session() {
    let running = RunningClient::start();
    let mut stream = running.connect(false);
    running.subscribe(&mut stream);
    let mut original = v5::Publish::new("input/one", QoS::ExactlyOnce, b"old".to_vec(), None);
    original.pkid = 42;
    let (stale, _) = running.deliver_message(&mut stream, original.clone());
    drop(stream);
    let mut stream = running.connect(false);
    running.subscribe(&mut stream);
    original.payload = b"new".as_slice().into();
    let (current, payload) = running.deliver_message(&mut stream, original);
    assert_eq!(payload.message.expect("Source message").body, b"new");
    // Committing a later record first is a FIFO barrier for the stale result.
    let mut marker = v5::Publish::new("input/marker", QoS::AtLeastOnce, Vec::new(), None);
    marker.pkid = 43;
    let (marker_record, _) = running.deliver_message(&mut stream, marker);
    running.complete(stale);
    running.complete(marker_record);
    assert!(matches!(receive(&mut stream), Packet::PubAck(ack) if ack.pkid == 43));
    running.complete(current);
    assert!(matches!(receive(&mut stream), Packet::PubRec(ack) if ack.pkid == 42));
    send(&mut stream, Packet::PubRel(v5::PubRel::new(42, None)));
    assert!(matches!(receive(&mut stream), Packet::PubComp(_)));
}

#[test]
fn offline_completion_waits_for_replay_without_repeating_business_delivery() {
    for qos in [QoS::AtLeastOnce, QoS::ExactlyOnce] {
        let running = RunningClient::start();
        let mut stream = running.connect(false);
        running.subscribe(&mut stream);
        let mut publish = v5::Publish::new("input/one", qos, b"record".to_vec(), None);
        publish.pkid = 42;
        let (record_id, _) = running.deliver_message(&mut stream, publish.clone());
        drop(stream);
        let mut stream = running.connect(true);
        running.complete(record_id);
        let mut marker = v5::Publish::new("input/marker", QoS::AtLeastOnce, Vec::new(), None);
        marker.pkid = 43;
        let (marker_record, _) = running.deliver_message(&mut stream, marker);
        running.complete(marker_record);
        assert!(matches!(receive(&mut stream), Packet::PubAck(ack) if ack.pkid == 43));
        publish.dup = true;
        send(&mut stream, Packet::Publish(publish));
        match qos {
            QoS::AtLeastOnce => {
                assert!(matches!(receive(&mut stream), Packet::PubAck(ack) if ack.pkid == 42))
            }
            QoS::ExactlyOnce => {
                assert!(matches!(receive(&mut stream), Packet::PubRec(ack) if ack.pkid == 42));
                send(&mut stream, Packet::PubRel(v5::PubRel::new(42, None)));
                assert!(matches!(receive(&mut stream), Packet::PubComp(_)));
            }
            _ => unreachable!(),
        }
        running.deliver(&mut stream, b"no repeated business");
    }
}

#[test]
fn one_oversized_subscription_fails_instead_of_reconnecting_forever() {
    let mut running = RunningClient::dormant(&[Subscription {
        filter: "a".repeat(300),
        qos: 1,
    }]);
    running.control.lock().unwrap().activate();
    let _stream = running.connect_with_properties(
        false,
        Some(v5::ConnAckProperties {
            max_packet_size: Some(256),
            ..connack_properties()
        }),
    );
    running.fail("maximum packet size");
}

#[test]
fn completion_bursts_survive_disconnect_and_broker_replay() {
    let running = RunningClient::dormant(&[Subscription {
        filter: "input/#".into(),
        qos: 2,
    }]);
    running.control.lock().unwrap().activate();
    let mut stream = running.connect(false);
    running.subscribe(&mut stream);
    let mut records = Vec::new();
    for pkid in 1..=64 {
        let mut publish = v5::Publish::new("input/burst", QoS::ExactlyOnce, Vec::new(), None);
        publish.pkid = pkid;
        records.push(running.deliver_message(&mut stream, publish).0);
    }
    drop(stream);
    let mut stream = running.connect(true);
    // The broker still holds all 64: committing them while offline only makes
    // each next replay answerable.
    for record in records {
        running.complete(record);
    }
    for pkid in 1..=64 {
        let mut publish = v5::Publish::new("input/burst", QoS::ExactlyOnce, Vec::new(), None);
        publish.pkid = pkid;
        publish.dup = true;
        send(&mut stream, Packet::Publish(publish));
    }
    let mut acknowledged = std::collections::HashSet::new();
    for _ in 1..=64 {
        let Packet::PubRec(ack) = receive(&mut stream) else {
            panic!("expected PUBREC")
        };
        assert!(acknowledged.insert(ack.pkid));
    }
    assert_eq!(acknowledged.len(), 64);
    for pkid in 1..=64 {
        send(&mut stream, Packet::PubRel(v5::PubRel::new(pkid, None)));
    }
    for _ in 1..=64 {
        assert!(matches!(receive(&mut stream), Packet::PubComp(_)));
    }
    running.deliver(&mut stream, b"no repeated business");
}
