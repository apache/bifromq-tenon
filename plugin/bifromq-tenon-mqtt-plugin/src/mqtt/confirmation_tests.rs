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

//! Exercise the real client and event loop against a scripted TCP peer.
use super::*;
use bytes::BytesMut;
use rumqttc::mqttbytes::v5::{ConnAck, ConnectReturnCode, Packet, PubAck, PubComp, PubRec};
use rumqttc::{PubAckReason, PubCompReason, PubRecReason};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::thread;
use tenon_ipc::queue::{QueueReader, QueueWriter, WriteOutcome};
use tenon_plugin_sdk::prost::Message;
use tenon_plugin_sdk::repository_test_support::{
    SourceFixture,
    source::{IngressCompletion, IngressRecord},
};

/// The Flow `maxRecordBytes` these Connection tests run under.
pub(super) const MAXIMUM_RECORD_BYTES: usize = 1024;
/// The in-flight window one test Channel admits before it refuses a record.
pub(super) const PENDING_RECORDS: usize = 32;

/// Opens the real Source session a Connection delivers into.
pub(super) fn source_fixture(channels: u32, pending: usize) -> SourceFixture {
    SourceFixture::create(channels, pending, MAXIMUM_RECORD_BYTES).expect("Source fixture")
}

/// Reads the next Submission record the way its Flow Channel loop does.
pub(super) fn read_submission(submission: &mut QueueReader) -> (u64, SourceRecordPayload) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(record) = try_submission(submission) {
            return record;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the Source committed no record"
        );
        thread::yield_now();
    }
}

/// Decodes and releases the next Submission record, if the Channel has one.
pub(super) fn try_submission(submission: &mut QueueReader) -> Option<(u64, SourceRecordPayload)> {
    let tenon_ipc::queue::ReadOutcome::Record(record) =
        submission.try_read().expect("Submission Queue")
    else {
        return None;
    };
    let ingress = IngressRecord::decode(record.payload()).expect("Ingress record");
    let payload = SourceRecordPayload::decode(ingress.payload.as_ref()).expect("Source payload");
    submission.release(1).expect("release Submission record");
    Some((ingress.record_id, payload))
}

/// Commits the terminal result the Flow Channel loop would write.
pub(super) fn complete_record(completion: &mut QueueWriter, record_id: u64) {
    assert!(matches!(
        completion
            .try_write(
                &IngressCompletion {
                    record_id,
                    status: 1,
                }
                .encode_to_vec()
            )
            .expect("Completion Queue"),
        WriteOutcome::Committed(_)
    ));
}

struct Workers {
    cancel: Option<oneshot::Sender<()>>,
    events: Option<thread::JoinHandle<Result<(), Error>>>,
    writer: Option<thread::JoinHandle<()>>,
}

impl Drop for Workers {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        if let Some(events) = self.events.take() {
            let _ = events.join();
        }
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}

pub(super) fn receive(stream: &mut TcpStream) -> Packet {
    let mut bytes = BytesMut::new();
    loop {
        match Packet::read(&mut bytes, Some(4096)) {
            Ok(packet) => return packet,
            Err(rumqttc::mqttbytes::Error::InsufficientBytes(_)) => {
                let mut byte = [0];
                stream.read_exact(&mut byte).expect("peer packet");
                bytes.extend_from_slice(&byte);
            }
            Err(error) => panic!("invalid client packet: {error}"),
        }
    }
}

pub(super) fn send(stream: &mut TcpStream, packet: Packet) {
    let mut bytes = BytesMut::new();
    packet.write(&mut bytes, None).expect("encode peer packet");
    stream.write_all(&bytes).expect("send peer packet");
}

#[derive(Clone, Copy)]
enum Reply {
    PubAck(PubAckReason),
    PubRec(PubRecReason),
    PubComp(PubCompReason),
    Disconnect,
}

fn confirmed_publish(qos: u32, response: Reply) -> Result<(), String> {
    let listener = TcpListener::bind("127.0.0.1:0").expect("peer listener");
    let port = listener.local_addr().unwrap().port();
    let (client, eventloop) = AsyncClient::builder(MqttOptions::new(
        "confirmation-test",
        Broker::tcp("127.0.0.1", port),
    ))
    .capacity(32)
    .try_build()
    .unwrap();
    let (writes, dispatcher) = Writes::new(client.clone());
    let source_acks = SourceAcks::new(client.clone());
    let control = Arc::new(Mutex::new(SubscriptionControl::new(client.clone(), &[])));
    let owner = ChannelClient {
        writes: writes.clone(),
        control: control.clone(),
        source_enabled: false,
    };
    let source = source_fixture(1, PENDING_RECORDS);
    let sender = source.sender::<SourceRecordPayload>();
    let (cancel, cancelled) = oneshot::channel();
    let events = thread::spawn(move || {
        Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        tokio::select! {
            _ = cancelled => Ok(()),
            result = tokio::time::timeout(Duration::from_secs(5), Connection { channel: 0, eventloop, sender: Some(sender), writes, dispatcher, control, source_acks, configured_clean_start: true }.run()) => result.expect("event loop deadline"),
        }
    })
    });
    let mut workers = Workers {
        cancel: Some(cancel),
        events: Some(events),
        writer: None,
    };
    let (completed, completion) = mpsc::channel();
    let writer = thread::spawn(move || {
        let config = Config::parse(
            &serde_json::json!({"endpoint": "mqtt://localhost", "clientIdPrefix": "test-"}),
        )
        .unwrap();
        let pending = owner.write(
            vec![SinkRecordPayload {
                message: Some(MqttMessage {
                    topic: "output".into(),
                    body: b"record".to_vec(),
                    qos: Some(qos),
                    ..Default::default()
                }),
            }]
            .into_boxed_slice(),
            &config,
        );
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let result = tokio::time::timeout(Duration::from_secs(5), pending)
                    .await
                    .expect("publish must settle")
                    .map_err(|error| error.to_string());
                let success = result.is_ok();
                let _ = completed.send(result);
                if success {
                    let _ = client.disconnect().await;
                }
            });
    });
    workers.writer = Some(writer);
    listener.set_nonblocking(true).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let (mut stream, _) = loop {
        match listener.accept() {
            Ok(peer) => break peer,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "client connection deadline"
                );
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("accept client: {error}"),
        }
    };
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    assert!(matches!(receive(&mut stream), Packet::Connect(_, _, _)));
    send(
        &mut stream,
        Packet::ConnAck(ConnAck {
            session_present: false,
            code: ConnectReturnCode::Success,
            properties: None,
        }),
    );
    let Packet::Publish(publish) = receive(&mut stream) else {
        panic!("expected publish")
    };
    assert_eq!(publish.payload.as_ref(), b"record");
    let disconnect_reply = matches!(response, Reply::Disconnect);
    match response {
        Reply::Disconnect => {}
        Reply::PubAck(reason) => send(
            &mut stream,
            Packet::PubAck(PubAck {
                pkid: publish.pkid,
                reason,
                properties: None,
            }),
        ),
        Reply::PubRec(_) | Reply::PubComp(_) => {
            let reason = match response {
                Reply::PubRec(reason) => reason,
                _ => PubRecReason::Success,
            };
            send(
                &mut stream,
                Packet::PubRec(PubRec {
                    pkid: publish.pkid,
                    reason,
                    properties: None,
                }),
            );
            if matches!(
                reason,
                PubRecReason::Success | PubRecReason::NoMatchingSubscribers
            ) {
                assert!(matches!(receive(&mut stream), Packet::PubRel(_)));
                assert!(
                    matches!(completion.try_recv(), Err(mpsc::TryRecvError::Empty)),
                    "PUBREC must not finish QoS 2"
                );
                send(
                    &mut stream,
                    Packet::PubComp(PubComp {
                        pkid: publish.pkid,
                        reason: match response {
                            Reply::PubComp(reason) => reason,
                            _ => PubCompReason::Success,
                        },
                        properties: None,
                    }),
                );
            }
        }
    }
    if disconnect_reply {
        stream
            .shutdown(std::net::Shutdown::Both)
            .expect("close scripted peer");
    }
    let result = completion
        .recv_timeout(Duration::from_secs(5))
        .expect("write completion");
    if result.is_ok() {
        let mut disconnect = [0; 2];
        stream
            .read_exact(&mut disconnect)
            .expect("client disconnect after accepted publication");
        assert_eq!(disconnect, [0xe0, 0]);
    } else if !disconnect_reply {
        // A retryable error ends this connection; the SDK will replay the
        // unreleased Queue record after the owner is restarted.
        drop(stream);
    }
    workers.writer.take().unwrap().join().unwrap();
    let loop_result = workers.events.take().unwrap().join().unwrap();
    assert_eq!(loop_result.is_err(), result.is_err());
    result
}

#[test]
fn success_and_no_matching_subscribers_complete_the_write() {
    for reason in [PubAckReason::Success, PubAckReason::NoMatchingSubscribers] {
        assert_eq!(confirmed_publish(1, Reply::PubAck(reason)), Ok(()));
    }
    assert_eq!(
        confirmed_publish(2, Reply::PubRec(PubRecReason::NoMatchingSubscribers)),
        Ok(())
    );
}

#[test]
fn permanent_broker_rejections_complete_the_write() {
    for reason in [
        PubAckReason::NotAuthorized,
        PubAckReason::TopicNameInvalid,
        PubAckReason::PayloadFormatInvalid,
    ] {
        assert_eq!(
            confirmed_publish(1, Reply::PubAck(reason)),
            Ok(()),
            "{reason:?}"
        );
    }
    for reason in [
        PubRecReason::NotAuthorized,
        PubRecReason::TopicNameInvalid,
        PubRecReason::PayloadFormatInvalid,
    ] {
        assert_eq!(
            confirmed_publish(2, Reply::PubRec(reason)),
            Ok(()),
            "{reason:?}"
        );
    }
}

#[test]
fn transient_broker_rejections_fail_the_write() {
    for reason in [
        PubAckReason::UnspecifiedError,
        PubAckReason::ImplementationSpecificError,
        PubAckReason::PacketIdentifierInUse,
        PubAckReason::QuotaExceeded,
    ] {
        assert!(
            confirmed_publish(1, Reply::PubAck(reason)).is_err(),
            "{reason:?}"
        );
    }
    for reason in [
        PubRecReason::UnspecifiedError,
        PubRecReason::ImplementationSpecificError,
        PubRecReason::PacketIdentifierInUse,
        PubRecReason::QuotaExceeded,
    ] {
        assert!(
            confirmed_publish(2, Reply::PubRec(reason)).is_err(),
            "{reason:?}"
        );
    }
    assert!(confirmed_publish(2, Reply::PubComp(PubCompReason::PacketIdentifierNotFound)).is_err());
}

#[test]
fn qos_two_waits_for_pubcomp() {
    assert_eq!(
        confirmed_publish(2, Reply::PubComp(PubCompReason::Success)),
        Ok(())
    );
}

#[test]
fn disconnect_fails_unconfirmed_publish_and_stops_replay() {
    for qos in [1, 2] {
        assert!(
            confirmed_publish(qos, Reply::Disconnect)
                .unwrap_err()
                .contains("before publish confirmation")
        );
    }
}

pub(super) fn connack_properties() -> v5::ConnAckProperties {
    v5::ConnAckProperties {
        session_expiry_interval: None,
        receive_max: None,
        max_qos: None,
        retain_available: None,
        max_packet_size: None,
        assigned_client_identifier: None,
        topic_alias_max: None,
        reason_string: None,
        user_properties: Vec::new(),
        wildcard_subscription_available: None,
        subscription_identifiers_available: None,
        shared_subscription_available: None,
        server_keep_alive: None,
        response_information: None,
        server_reference: None,
        authentication_method: None,
        authentication_data: None,
    }
}
