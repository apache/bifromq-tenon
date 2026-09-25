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

//! Real Plugin process behaviour on its shared MQTT connection: shutdown with
//! outstanding Sink work, and session start versus same-process reconnect.
use tenon_ipc::queue::WriteOutcome;
#[path = "../src/payload/mod.rs"]
#[allow(
    dead_code,
    reason = "This process test only publishes the generated Sink payload"
)]
mod payload;
#[path = "../../../sdk/rust/plugin-sdk/tests/support/process_peer.rs"]
mod peer;

use bytes::BytesMut;
use rumqttc::PubAckReason;
use rumqttc::mqttbytes::{
    QoS,
    v5::{ConnAck, ConnectReturnCode, Packet, PubAck},
};
use std::{path::Path, time::Duration};
use tenon_plugin_sdk::repository_test_support::sink;
use tenon_plugin_sdk::{Error, FlowChannel, prost::Message};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

async fn receive(stream: &mut TcpStream) -> Result<Packet, Error> {
    let mut bytes = BytesMut::new();
    loop {
        match Packet::read(&mut bytes, Some(4096)) {
            Ok(packet) => return Ok(packet),
            Err(rumqttc::mqttbytes::Error::InsufficientBytes(_)) => {
                bytes.extend_from_slice(&[stream.read_u8().await?]);
            }
            Err(error) => return Err(error.into()),
        }
    }
}

async fn accept_connect(listener: &TcpListener) -> Result<(TcpStream, bool), Error> {
    let (mut stream, _) = timeout(peer::DEADLINE, listener.accept()).await??;
    let Packet::Connect(connect, _, _) = timeout(peer::DEADLINE, receive(&mut stream)).await??
    else {
        return Err("expected CONNECT".into());
    };
    Ok((stream, connect.clean_start))
}

async fn send_connack(stream: &mut TcpStream, session_present: bool) -> Result<(), Error> {
    let mut bytes = BytesMut::new();
    Packet::ConnAck(ConnAck {
        session_present,
        code: ConnectReturnCode::Success,
        properties: None,
    })
    .write(&mut bytes, None)?;
    stream.write_all(&bytes).await?;
    Ok(())
}

/// The 96 Channels one saturated Sink owns, one Channel per Flow.
fn saturated_channels(_working: &Path) -> Vec<FlowChannel> {
    (0..96)
        .map(|index| {
            let flow_id = format!("flow-{index}");
            FlowChannel {
                flow_id,
                channel_id: 0,
            }
        })
        .collect()
}

/// The single Channel one reconnect test drives.
fn resumed_channel(_working: &Path) -> Vec<FlowChannel> {
    vec![FlowChannel {
        flow_id: "flow-0".into(),
        channel_id: 0,
    }]
}

#[tokio::test]
async fn shutdown_with_saturated_sink_work_exits_without_releasing_unconfirmed_records()
-> Result<(), Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut peer = peer::Peer::start_source_and_sink_binary(
        serde_json::json!({
            "endpoint": format!("mqtt://{}", listener.local_addr()?),
            "clientIdPrefix": "shutdown-test-",
            "session": {"cleanStart": false},
        }),
        saturated_channels,
        Path::new(env!("CARGO_BIN_EXE_bifromq-tenon-mqtt-plugin")),
    )
    .await?;
    if let Err(error) = peer.ready().await {
        let mut diagnostic = String::new();
        if let Some(mut stderr) = peer.child.stderr.take() {
            timeout(peer::DEADLINE, stderr.read_to_string(&mut diagnostic)).await??;
        }
        panic!("Plugin did not become ready: {error}; {diagnostic}");
    }
    let (mut stream, _) = timeout(peer::DEADLINE, listener.accept()).await??;
    let Packet::Connect(connect, _, _) = timeout(peer::DEADLINE, receive(&mut stream)).await??
    else {
        return Err("expected CONNECT".into());
    };
    assert!(
        connect.clean_start,
        "the first CONNECT of a process must start a clean session even though session.cleanStart is false"
    );
    let mut bytes = BytesMut::new();
    Packet::ConnAck(ConnAck {
        session_present: false,
        code: ConnectReturnCode::Success,
        properties: None,
    })
    .write(&mut bytes, None)?;
    stream.write_all(&bytes).await?;

    let record = sink::EgressRecord {
        payload: payload::SinkRecordPayload {
            message: Some(payload::MqttMessage {
                topic: "output".into(),
                qos: Some(1),
                body: b"unconfirmed".to_vec(),
                ..Default::default()
            }),
        }
        .encode_to_vec()
        .into(),
    }
    .encode_to_vec();
    let inputs = 0..peer.sink_channels.len();
    let paths: Vec<_> = inputs
        .clone()
        .map(|input| peer.egress_path(input))
        .collect::<Result<_, Error>>()?;
    for input in inputs {
        assert!(matches!(
            peer.egress(input)?.try_write(&record)?,
            WriteOutcome::Committed(_)
        ));
    }
    for _ in 0..32 {
        let Packet::Publish(publish) = timeout(peer::DEADLINE, receive(&mut stream)).await?? else {
            panic!("expected unconfirmed publish")
        };
        assert_eq!(publish.qos, QoS::AtLeastOnce);
    }
    // All 96 Queues contain work and 32 publications await ACKs. Synchronous
    // admission past the dispatcher limit is covered separately by the wire test.
    for path in &paths {
        let header = std::fs::read(path)?;
        assert_ne!(&header[64..72], &[0; 8]);
        assert_eq!(&header[128..136], &[0; 8]);
    }
    // Keep the connection alive throughout Shutdown. No network failure can wake it.
    let files: Vec<_> = paths
        .iter()
        .map(std::fs::File::open)
        .collect::<Result<_, _>>()?;
    peer.quiesce().await?;
    peer.quiesced().await?;
    timeout(Duration::from_secs(3), peer.finish()).await??;
    for mut file in files {
        let mut header = [0; 136];
        std::io::Read::read_exact(&mut file, &mut header)?;
        assert_eq!(
            &header[128..136],
            &[0; 8],
            "Shutdown must not confirm a batch"
        );
    }
    Ok(())
}

#[tokio::test]
async fn first_connect_is_clean_and_reconnect_follows_configured_clean_start() -> Result<(), Error>
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut peer = peer::Peer::start_source_and_sink_binary(
        serde_json::json!({
            "endpoint": format!("mqtt://{}", listener.local_addr()?),
            "clientIdPrefix": "session-test-",
            "session": {"cleanStart": false},
        }),
        resumed_channel,
        Path::new(env!("CARGO_BIN_EXE_bifromq-tenon-mqtt-plugin")),
    )
    .await?;
    if let Err(error) = peer.ready().await {
        let mut diagnostic = String::new();
        if let Some(mut stderr) = peer.child.stderr.take() {
            timeout(peer::DEADLINE, stderr.read_to_string(&mut diagnostic)).await??;
        }
        panic!("Plugin did not become ready: {error}; {diagnostic}");
    }
    let (mut first, clean_start) = accept_connect(&listener).await?;
    assert!(
        clean_start,
        "a fresh process holds no session, so its first CONNECT must ask for a new one"
    );
    send_connack(&mut first, false).await?;
    // Dropping the socket forces the still-running EventLoop to reconnect by itself.
    drop(first);

    let (mut second, clean_start) = accept_connect(&listener).await?;
    assert!(
        !clean_start,
        "a reconnect inside the same process must follow the configured session.cleanStart"
    );
    // The Broker may report a resumed session. The plugin holds matching in-memory
    // state, so it must accept that resume and keep carrying QoS 1 work over it.
    send_connack(&mut second, true).await?;
    let record = sink::EgressRecord {
        payload: payload::SinkRecordPayload {
            message: Some(payload::MqttMessage {
                topic: "output".into(),
                qos: Some(1),
                body: b"resumed".to_vec(),
                ..Default::default()
            }),
        }
        .encode_to_vec()
        .into(),
    }
    .encode_to_vec();
    assert!(matches!(
        peer.egress(0)?.try_write(&record)?,
        WriteOutcome::Committed(_)
    ));
    let Packet::Publish(publish) = timeout(peer::DEADLINE, receive(&mut second)).await?? else {
        panic!("expected a publish on the resumed session")
    };
    assert_eq!(publish.qos, QoS::AtLeastOnce);
    assert_eq!(publish.payload.as_ref(), b"resumed");
    let mut bytes = BytesMut::new();
    Packet::PubAck(PubAck {
        pkid: publish.pkid,
        reason: PubAckReason::Success,
        properties: None,
    })
    .write(&mut bytes, None)?;
    second.write_all(&bytes).await?;

    peer.quiesce().await?;
    peer.quiesced().await?;
    timeout(Duration::from_secs(3), peer.finish()).await??;
    Ok(())
}

/// Shutdown must stay bounded while the Broker is unreachable: the client is
/// retrying, so no network event can carry the close request to it.
#[tokio::test]
async fn shutdown_is_bounded_while_the_broker_is_unreachable() -> Result<(), Error> {
    // Binding and dropping the port leaves nothing listening, so every
    // connection attempt is refused and the client keeps retrying.
    let port = {
        let probe = TcpListener::bind("127.0.0.1:0").await?;
        let port = probe.local_addr()?.port();
        drop(probe);
        port
    };
    let mut peer = peer::Peer::start_source_and_sink_binary(
        serde_json::json!({
            "endpoint": format!("mqtt://127.0.0.1:{port}"),
            "clientIdPrefix": "unreachable-test-",
            // More threads than Channels: the value is an upper bound, not a
            // request to create threads.
            "eventLoopThreads": 2,
            "source": {"subscriptions": [{"filter": "input", "qos": 1}]},
        }),
        resumed_channel,
        Path::new(env!("CARGO_BIN_EXE_bifromq-tenon-mqtt-plugin")),
    )
    .await?;
    if let Err(error) = peer.ready().await {
        let mut diagnostic = String::new();
        if let Some(mut stderr) = peer.child.stderr.take() {
            timeout(peer::DEADLINE, stderr.read_to_string(&mut diagnostic)).await??;
        }
        panic!("Plugin did not become ready: {error}; {diagnostic}");
    }
    peer.quiesce().await?;
    peer.quiesced().await?;
    // The process must still exit on its own; a bounded close does not wait for
    // a Broker that never comes back.
    timeout(Duration::from_secs(10), peer.finish()).await??;
    Ok(())
}
