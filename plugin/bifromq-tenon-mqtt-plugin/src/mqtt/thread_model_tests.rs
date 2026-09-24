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

//! Two Channels on two configured threads each keep their own MQTT client.
//!
//! The rule that decides which Clients share a thread is asserted beside
//! `pin_connection`; this drives the real plugin build end to end over a
//! scripted peer, so the wiring that spreads Clients over threads is exercised
//! rather than restated.
use super::confirmation_tests::{
    PENDING_RECORDS, complete_record, read_submission, receive, send, source_fixture,
};
use super::*;
use rumqttc::PubAckReason;
use rumqttc::mqttbytes::v5::{ConnAck, ConnectReturnCode, Packet, SubAck, SubscribeReasonCode};
use std::collections::BTreeMap;
use std::net::{TcpListener, TcpStream};

fn accept(listener: &TcpListener) -> TcpStream {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                // A non-blocking listener hands out non-blocking sockets on
                // BSD, so the accepted stream is returned to blocking mode
                // before it is read with a timeout.
                stream
                    .set_nonblocking(false)
                    .expect("blocking client stream");
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("read timeout");
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .expect("write timeout");
                return stream;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "client connection deadline"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("accept client: {error}"),
        }
    }
}

#[test]
fn every_channel_owns_its_client_when_clients_are_spread_over_threads() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("peer listener");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let port = listener.local_addr().expect("peer address").port();
    let source = source_fixture(2, PENDING_RECORDS);
    let mut plugin = MqttPlugin::new(
        serde_json::json!({
            "endpoint": format!("mqtt://127.0.0.1:{port}"),
            "clientIdPrefix": "threads-",
            "eventLoopThreads": 2,
            "source": {"subscriptions": [{"filter": "input", "qos": 1}]},
        }),
        Some(Ingress {
            parallelism: 2,
            sender: source.sender::<SourceRecordPayload>(),
        }),
        BTreeSet::new(),
    )
    .expect("plugin");
    assert_eq!(plugin.connections.len(), 2, "one Connection per Channel");
    // Each Channel connects on its own socket; the peer tells them apart by the
    // client ID the plugin generated for that Channel.
    let mut clients = BTreeMap::new();
    for _ in 0..2 {
        let mut stream = accept(&listener);
        let Packet::Connect(connect, _, _) = receive(&mut stream) else {
            panic!("expected CONNECT")
        };
        send(
            &mut stream,
            Packet::ConnAck(ConnAck {
                session_present: false,
                code: ConnectReturnCode::Success,
                properties: None,
            }),
        );
        clients.insert(connect.client_id, stream);
    }
    plugin.start();
    // Every Client restores the configured subscription on its own connection.
    for stream in clients.values_mut() {
        let Packet::Subscribe(subscribe) = receive(stream) else {
            panic!("expected SUBSCRIBE")
        };
        send(
            stream,
            Packet::SubAck(SubAck {
                pkid: subscribe.pkid,
                return_codes: vec![SubscribeReasonCode::Success(QoS::AtLeastOnce)],
                properties: None,
            }),
        );
    }
    // The second Channel's record must run to completion on its own thread and
    // reach the Broker on that Channel's own connection.
    let channel = 1;
    let mut publish = v5::Publish::new("input", QoS::AtLeastOnce, b"threaded".to_vec(), None);
    publish.pkid = 7;
    send(
        clients.get_mut("threads-1").expect("second client"),
        Packet::Publish(publish),
    );
    let mut submission = source.submission(channel).expect("Submission Queue");
    let (record_id, payload) = read_submission(&mut submission);
    assert_eq!(
        payload.message.expect("Source message").body,
        b"threaded",
        "the second Channel admits only its own record"
    );
    complete_record(
        &mut source.completion(channel).expect("Completion Queue"),
        record_id,
    );
    let Packet::PubAck(ack) = receive(clients.get_mut("threads-1").expect("second client")) else {
        panic!("expected PUBACK")
    };
    assert_eq!(ack.pkid, 7);
    assert_eq!(ack.reason, PubAckReason::Success);
    assert_eq!(source.failure(), None, "the Source session must survive");
    let started = std::time::Instant::now();
    plugin.close();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "close must join every Connection within its window"
    );
}

#[test]
fn sink_channels_beyond_source_parallelism_do_not_become_source_channels() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("peer listener");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let port = listener.local_addr().expect("peer address").port();
    let source = source_fixture(1, PENDING_RECORDS);
    let mut plugin = MqttPlugin::new(
        serde_json::json!({
            "endpoint": format!("mqtt://127.0.0.1:{port}"),
            "clientIdPrefix": "mixed-",
            "source": {"subscriptions": [{"filter": "input", "qos": 1}]},
        }),
        Some(Ingress {
            parallelism: 1,
            sender: source.sender::<SourceRecordPayload>(),
        }),
        [FlowChannel {
            flow_id: "sink-flow".into(),
            channel_id: 2,
        }]
        .into_iter()
        .collect(),
    )
    .expect("plugin");
    assert_eq!(plugin.inner.clients.len(), 3);
    assert!(plugin.inner.clients[0].source_enabled);
    assert!(
        plugin.inner.clients[1..]
            .iter()
            .all(|client| !client.source_enabled)
    );
    plugin.close();
}
