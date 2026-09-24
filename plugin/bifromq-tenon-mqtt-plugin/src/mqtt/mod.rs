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

mod source_acks;
mod subscriptions;
mod writes;

use self::source_acks::SourceAcks;
use self::subscriptions::SubscriptionControl;
use self::writes::{Dispatcher, Writes};
use crate::config::{Config, Subscription};
use crate::payload::{
    Mqtt5Properties, MqttMessage, SinkRecordPayload, SourceRecordPayload, UserProperty,
};
use crate::source::Source;
use futures_util::FutureExt;
use futures_util::stream::{FuturesUnordered, StreamExt};
use rumqttc::mqttbytes::v5::PublishProperties;
use rumqttc::mqttbytes::{QoS, v5};
use rumqttc::{
    AckMode, AsyncClient, Broker, Event, EventLoop, Incoming, MqttOptions, PublishNotice,
    PublishOptions,
};
use rumqttc::{Outgoing, Transport};
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tenon_plugin_sdk::{
    AckCode, Error, FlowChannel, Ingress, PayloadSender, TenonSource, TenonSourceAndSink, Value,
};
use tokio::runtime::Builder;
#[cfg(test)]
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::task::LocalPoolHandle;
use url::Url;

/// How long a closing Connection may take to observe the close request and to
/// drain what the broker already holds. Closing must be bounded, so a
/// connection that never comes back still ends within this window.
const CLOSE_WINDOW: Duration = Duration::from_secs(1);

pub(crate) struct ChannelClient {
    pub(crate) control: Arc<Mutex<SubscriptionControl>>,
    writes: Arc<Writes>,
    pub(crate) source_enabled: bool,
}
pub(crate) struct Inner {
    pub(crate) clients: Vec<ChannelClient>,
    pub(crate) config: Config,
}
pub struct MqttPlugin {
    inner: Arc<Inner>,
    source: Option<Mutex<Source>>,
    /// The threads every Connection is pinned to. It must outlive the joins in
    /// `close`, because dropping the pool cancels the tasks still on it.
    pool: LocalPoolHandle,
    connections: Vec<JoinHandle<()>>,
}

pub(crate) fn client_id(prefix: &str, channel: usize) -> String {
    format!("{prefix}{channel}")
}

/// Pins one Connection to the worker that will drive it.
///
/// Clients that map to the same worker are driven by the same thread, so this
/// rule decides which clients share one. It is the only place that decides that,
/// and `workers` never exceeds the number of Connections.
fn pin_connection<F, Fut>(
    pool: &LocalPoolHandle,
    workers: usize,
    channel: usize,
    run: F,
) -> JoinHandle<()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + 'static,
{
    pool.spawn_pinned_by_idx(run, channel % workers)
}

impl MqttPlugin {
    pub fn new(
        value: Value,
        source: Option<Ingress<SourceRecordPayload>>,
        egress_channels: BTreeSet<FlowChannel>,
    ) -> Result<Self, Error> {
        let config = Config::parse(&value)?;
        let source_parallelism = source.as_ref().map(|source| source.parallelism);
        if source_parallelism == Some(0) {
            return Err("parallelism must be positive".into());
        }
        let sink_parallelism = egress_channels
            .iter()
            .map(|channel| channel.channel_id as usize + 1)
            .max()
            .unwrap_or(0);
        let n = source_parallelism.unwrap_or(0).max(sink_parallelism);
        let sender = source.map(|source| source.sender);
        if n == 0 {
            return Err("parallelism must be positive".into());
        }
        let endpoint = Url::parse(&config.endpoint)?;
        let host = endpoint
            .host_str()
            .ok_or("endpoint host missing")?
            .to_string();
        let port = endpoint.port().unwrap_or(if endpoint.scheme() == "mqtts" {
            8883
        } else {
            1883
        });
        // One thread drives `min(eventLoopThreads, n)` clients. Parallelism is
        // the client count's upper bound, so a larger configured value never
        // creates another thread.
        let workers = config.event_loop_threads.min(n);
        let pool = LocalPoolHandle::new(workers);
        let mut clients = Vec::with_capacity(n);
        let mut connections = Vec::with_capacity(n);
        for i in 0..n {
            let id = client_id(&config.client_id_prefix, i);
            if id.len() > 65_535 {
                return Err("generated MQTT client ID is too long".into());
            }
            let mut options = MqttOptions::new(id, Broker::tcp(host.clone(), port));
            options.set_keep_alive(30);
            // A fresh process holds no session state, so it cannot resume a broker
            // session. The first CONNECT always asks for a new one; Connection::run
            // installs the configured cleanStart once that first CONNACK arrives.
            options.set_clean_start(true);
            options.set_session_expiry_interval(Some(config.session.expiry_interval_seconds));
            options.set_ack_mode(AckMode::Manual);
            // The broker must not hold more unacknowledged records than this
            // connection is willing to run, so the broker's own flow control is
            // where backpressure starts. A record still refused locally is
            // answered with QuotaExceeded instead of being queued here.
            options.set_receive_maximum(Some(config.receive_maximum()));
            if endpoint.scheme() == "mqtts" {
                options.set_transport(Transport::try_tls_with_default_config()?);
            }
            if let Some(a) = &config.auth {
                options.set_credentials(a.username.clone(), a.password.clone());
            }
            let (client, eventloop) = AsyncClient::builder(options).capacity(32).try_build()?;
            let (writes, dispatcher) = Writes::new(client.clone());
            let source_acks = SourceAcks::new(client.clone());
            let source_enabled = source_parallelism.is_some_and(|parallelism| i < parallelism);
            let control = Arc::new(Mutex::new(SubscriptionControl::new(
                client.clone(),
                if source_enabled {
                    subscriptions(&config)
                } else {
                    &[]
                },
            )));
            let connection = Connection {
                channel: i,
                eventloop,
                sender: source_enabled.then(|| sender.as_ref().expect("Source sender").clone()),
                writes: writes.clone(),
                dispatcher,
                control: control.clone(),
                source_acks,
                configured_clean_start: config.session.clean_start,
            };
            connections.push(pin_connection(&pool, workers, i, move || async move {
                if let Err(error) = connection.run().await {
                    // A failed Connection must end the process where it
                    // failed, as every other business failure does.
                    panic!("MQTT event loop channel {i} failed: {error}");
                }
            }));
            clients.push(ChannelClient {
                control,
                writes,
                source_enabled,
            });
        }
        let inner = Arc::new(Inner { clients, config });
        let source = sender
            .is_some()
            .then(|| Mutex::new(Source::new(inner.clone())));
        Ok(Self {
            inner,
            source,
            pool,
            connections,
        })
    }
}
/// Waits until a deadline that may not exist; a missing deadline never fires,
/// so a `select!` branch can carry a wait that is currently switched off.
async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

struct Connection {
    channel: usize,
    eventloop: EventLoop,
    /// The Source channel this Connection feeds. Encoding runs here, on the
    /// event loop, and the returned future stays with the record it belongs to.
    sender: Option<PayloadSender<SourceRecordPayload>>,
    writes: Arc<Writes>,
    dispatcher: Dispatcher,
    control: Arc<Mutex<SubscriptionControl>>,
    source_acks: SourceAcks,
    configured_clean_start: bool,
}

impl Connection {
    async fn run(self) -> Result<(), Error> {
        let Self {
            channel,
            mut eventloop,
            sender,
            writes,
            dispatcher,
            control,
            mut source_acks,
            configured_clean_start,
        } = self;
        let dispatcher = dispatcher.run();
        tokio::pin!(dispatcher);
        // The records this Connection admitted and has not answered for. Each
        // one carries the receipt it belongs to, so a result is applied to its
        // own delivery and nothing else observes it in between.
        let mut deliveries = FuturesUnordered::new();
        // A record that cannot complete normally stops this Channel from
        // admitting more work, so the broker keeps what this Connection cannot
        // deliver instead of the record being dropped silently.
        let mut stopped = false;
        let result: Result<(), Error> = async {
        let mut first_connection = true;
        // Set once this Connection has observed the close request, so the
        // window that bounds the closing wait starts when that wait starts.
        let mut closing_since: Option<Instant> = None;
        loop {
        // poll() owns connection setup and flush futures. Keep it alive while
        // application completions arrive, including during a reconnect handshake.
        let event = {
            let poll = eventloop.poll();
            tokio::pin!(poll);
            loop {
                source_acks.drive()?;
                let deadline = {
                    let mut control = control.lock().expect("subscription control lock");
                    control.drive();
                    control.deadline()
                };
                // A closing Connection waits one bounded window for the Broker
                // to take the DISCONNECT. A reconnecting EventLoop never
                // observes that request before it has a connection again, so
                // this window is what keeps the join bounded instead of
                // waiting for a Broker that never comes back.
                let close_window = if control.lock().expect("subscription control lock").closing()
                {
                    Some(*closing_since.get_or_insert_with(Instant::now) + CLOSE_WINDOW)
                } else {
                    closing_since = None;
                    None
                };
                tokio::select! {
                    biased;
                    progress = std::future::poll_fn(|cx| control.lock().expect("subscription control lock").poll(cx)) => progress?,
                    _ = wait_until(deadline) => return Err("MQTT subscription acknowledgement timed out".into()),
                    _ = wait_until(close_window) => return Ok(()),
                    result = &mut dispatcher => return result,
                    Some((receipt, result)) = deliveries.next() => match result {
                        Ok(AckCode::Ok) => source_acks.complete(receipt),
                        Ok(AckCode::Retry) => source_acks.abandon(receipt),
                        Ok(AckCode::Error) => {
                            stopped = true;
                            eprintln!(
                                "MQTT source channel {channel} cannot complete a record and stops admitting"
                            );
                            source_acks.abandon(receipt);
                        }
                        // Admission is decided before send() returns, and a
                        // closed session resolves a delivery without a terminal
                        // result. Neither can be answered locally, so the
                        // record stays unacknowledged for the broker to replay.
                        Ok(AckCode::Backpressure) | Err(_) => source_acks.abandon(receipt),
                    },
                    event = &mut poll => break event,
                }
            }
        };
        if let Ok(event) = &event {
            source_acks.event(event);
            control
                .lock()
                .expect("subscription control lock")
                .event(event)?;
        }
        // The forced clean start lasts until the process has a session to keep.
        // Once the first CONNACK proves the connection came up, the EventLoop holds
        // in-memory session state, so reconnects follow the configured cleanStart.
        if first_connection && matches!(&event, Ok(Event::Incoming(Incoming::ConnAck(_)))) {
            eventloop.options.set_clean_start(configured_clean_start);
            first_connection = false;
        }
        match event {
            Ok(event) => match event {
                Event::Incoming(Incoming::Publish(p)) => {
                    if source_acks.duplicate(&p) {
                        continue;
                    }
                    // Only an active subscription admits records, and a stopped
                    // Channel admits nothing, so the broker keeps holding what
                    // this Connection cannot deliver.
                    if stopped || !control.lock().expect("subscription control lock").admits() {
                        continue;
                    }
                    let received_at_unix_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_or(0, |d| {
                            d.as_millis().min(i128::from(i64::MAX) as u128) as i64
                        });
                    let payload = SourceRecordPayload {
                        message: Some(source_message(&p, received_at_unix_ms)),
                        dup: p.dup,
                        received_at_unix_ms,
                    };
                    // Encoding and local admission are decided before send()
                    // returns, so a result that is already settled here belongs
                    // to a record this Connection never admitted.
                    let Some(sender) = &sender else { continue; };
                    let mut completion = sender
                        .send(channel, &payload)
                        .expect("MQTT client index matches its Source channel");
                    // Polling through a reborrow decides the same question
                    // without giving the future up for a record that is not
                    // settled: an admitted delivery still owns its result.
                    match (&mut completion).now_or_never() {
                        Some(Ok(AckCode::Backpressure)) => source_acks.refuse(&p),
                        Some(Ok(AckCode::Error)) => {
                            stopped = true;
                            eprintln!(
                                "MQTT source channel {channel} cannot deliver a record and stops admitting"
                            );
                        }
                        // A closed Source session already refuses new work, and
                        // a terminal result cannot arrive without a pipeline
                        // round trip.
                        Some(_) => {}
                        None => {
                            if let Some(receipt) = source_acks.accept(&p) {
                                deliveries.push(completion.map(move |result| (receipt, result)));
                            }
                        }
                    }
                }
                Event::Outgoing(Outgoing::Disconnect) => return Ok(()),
                _ => {}
            },
            Err(error) => {
                // A closing Connection waits at most one window for the broker.
                // Once it expires the EventLoop has already normalized its
                // state, so there is nothing left to retry.
                if matches!(error, rumqttc::ConnectionError::DisconnectTimeout) {
                    return Ok(());
                }
                source_acks.disconnected();
                control
                    .lock()
                    .expect("subscription control lock")
                    .disconnected(&mut eventloop);
                // Drain completed notices without Tokio's cooperative budget
                // turning a ready result into Pending at this failure boundary.
                tokio::task::unconstrained(std::future::poll_fn(|cx| match dispatcher.as_mut().poll(cx) {
                    std::task::Poll::Ready(result) => std::task::Poll::Ready(result),
                    std::task::Poll::Pending => std::task::Poll::Ready(Ok(())),
                })).await?;
                if writes.has_pending() {
                    return Err(format!("MQTT connection failed before publish confirmation: {error}").into());
                }
                if matches!(error,
                    rumqttc::ConnectionError::MqttState(rumqttc::StateError::OutgoingPacketTooLarge { .. } | rumqttc::StateError::Deserialization(rumqttc::mqttbytes::Error::OutgoingPacketTooLarge { .. }) | rumqttc::StateError::ProtocolViolation(_) | rumqttc::StateError::Unsolicited(_))
                    | rumqttc::ConnectionError::SessionStateMismatch { .. }
                    | rumqttc::ConnectionError::SessionStore(_)
                    | rumqttc::ConnectionError::SessionRestore(_)
                ) {
                    return Err(error.into());
                }
                eprintln!("MQTT event loop channel {channel} will retry: {error}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        }
        }.await;
        writes.fail(
            result
                .as_ref()
                .err()
                .map_or_else(|| "MQTT event loop stopped".to_owned(), ToString::to_string),
        );
        result
    }
}

fn qos(q: u8) -> QoS {
    match q {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => unreachable!("validated MQTT QoS must be 0, 1, or 2"),
    }
}

fn source_message(publish: &v5::Publish, received_at_unix_ms: i64) -> MqttMessage {
    let properties = publish.properties.as_ref();
    let expires_at_unix_ms = properties
        .and_then(|properties| properties.message_expiry_interval)
        .and_then(|seconds| {
            received_at_unix_ms.checked_add(i64::from(seconds).saturating_mul(1_000))
        });
    let mqtt5 = properties.map(|properties| Mqtt5Properties {
        payload_format_indicator: properties.payload_format_indicator.map(u32::from),
        content_type: properties.content_type.clone(),
        response_topic: properties.response_topic.clone(),
        correlation_data: properties
            .correlation_data
            .as_ref()
            .map(|data| data.to_vec()),
        user_properties: properties
            .user_properties
            .iter()
            .map(|(key, value)| UserProperty {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
    });
    MqttMessage {
        topic: String::from_utf8_lossy(&publish.topic).into_owned(),
        body: publish.payload.to_vec(),
        qos: Some(match publish.qos {
            QoS::AtMostOnce => 0,
            QoS::AtLeastOnce => 1,
            QoS::ExactlyOnce => 2,
        }),
        retain: Some(publish.retain),
        expires_at_unix_ms,
        mqtt5,
    }
}
pub(crate) fn subscriptions(config: &Config) -> &[Subscription] {
    config
        .source
        .as_ref()
        .map(|s| s.subscriptions.as_slice())
        .unwrap_or(&[])
}

fn publish_properties(message: &MqttMessage) -> Result<Option<PublishProperties>, Error> {
    let properties = message.mqtt5.as_ref();
    let payload_format_indicator = properties
        .and_then(|properties| properties.payload_format_indicator)
        .map(|value| match value {
            0 | 1 => Ok(value as u8),
            _ => Err(Error::from("mqtt payloadFormatIndicator must be 0 or 1")),
        })
        .transpose()?;
    let message_expiry_interval = match message.expires_at_unix_ms {
        Some(deadline) => {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| "system clock is before Unix epoch")?
                .as_millis() as i64;
            let remaining = deadline.saturating_sub(now);
            if remaining <= 0 {
                return Err("mqtt message has expired".into());
            }
            Some((remaining / 1_000).min(i64::from(u32::MAX)) as u32)
        }
        None => None,
    };
    if properties.is_none() && message_expiry_interval.is_none() {
        return Ok(None);
    }
    Ok(Some(PublishProperties {
        payload_format_indicator,
        message_expiry_interval,
        response_topic: properties.and_then(|properties| properties.response_topic.clone()),
        correlation_data: properties
            .and_then(|properties| properties.correlation_data.as_ref())
            .map(|data| data.clone().into()),
        user_properties: properties
            .map(|properties| properties.user_properties.as_slice())
            .unwrap_or_default()
            .iter()
            .map(|property| (property.key.clone(), property.value.clone()))
            .collect(),
        content_type: properties.and_then(|properties| properties.content_type.clone()),
        ..PublishProperties::default()
    }))
}

async fn publish_message(
    client: &AsyncClient,
    message: MqttMessage,
    default_qos: u8,
    default_retain: bool,
) -> Result<PublishNotice, Error> {
    if message.topic.is_empty() {
        return Err("sink topic is required".into());
    }
    let q = message.qos.unwrap_or(u32::from(default_qos));
    if q > 2 {
        return Err("sink qos must be 0, 1, or 2".into());
    }
    let properties = publish_properties(&message)?;
    let topic = message.topic;
    let retain = message.retain.unwrap_or(default_retain);
    let mut options = PublishOptions::new(qos(q as u8)).retain(retain);
    if let Some(properties) = properties {
        options = options.properties(properties);
    }
    client
        .publish_tracked(topic, message.body, options)
        .await
        .map_err(Into::into)
}

impl ChannelClient {
    fn write(
        &self,
        records: Box<[SinkRecordPayload]>,
        config: &Config,
    ) -> impl Future<Output = Result<(), Error>> {
        self.writes.write(records, config)
    }
}

impl TenonSourceAndSink<SinkRecordPayload> for MqttPlugin {
    fn start(&mut self) {
        if let Some(source) = &mut self.source {
            source.get_mut().expect("source mutex").start();
        }
    }
    fn quiesce(&self) {
        if let Some(source) = &self.source {
            source.lock().expect("source mutex").quiesce();
        }
    }
    fn write(
        &self,
        channel: FlowChannel,
        records: Box<[SinkRecordPayload]>,
    ) -> impl Future<Output = Result<(), Error>> {
        let client = self
            .inner
            .clients
            .get(channel.channel_id as usize)
            .ok_or_else(|| format!("sink channel {} out of range", channel.channel_id));
        let result = client.map(|client| client.write(records, &self.inner.config));
        async move { result?.await }
    }
    fn close(&mut self) {
        for channel in &self.inner.clients {
            channel
                .control
                .lock()
                .expect("subscription control lock")
                .close();
        }
        let connections = std::mem::take(&mut self.connections);
        if connections.is_empty() {
            return;
        }
        // Lifecycle callbacks run on the synchronous main thread, so this wait
        // only has to park the thread until every Connection has returned. The
        // pool stays owned by `self` across the join: dropping it would cancel
        // the very tasks this waits for.
        let _pool = &self.pool;
        Builder::new_current_thread()
            .build()
            .expect("MQTT close runtime")
            .block_on(async move {
                for connection in connections {
                    let _ = connection.await;
                }
            });
    }
}

#[cfg(test)]
mod tests {
    use super::{Builder, client_id, pin_connection, source_message};
    use crate::config::Config;
    use crate::payload::{Mqtt5Properties, UserProperty};
    use rumqttc::mqttbytes::QoS;
    use rumqttc::mqttbytes::v5::{Publish, PublishProperties};
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio_util::task::LocalPoolHandle;

    #[test]
    fn clients_sharing_a_worker_are_driven_by_one_thread() {
        // K is the configured thread count, N the client count. Threads are
        // never over-provisioned: a value above N still yields N workers.
        for (configured, clients) in [(1_usize, 3_usize), (3, 3), (8, 3)] {
            let workers = configured.min(clients);
            let pool = LocalPoolHandle::new(workers);
            let seen = Arc::new(Mutex::new(Vec::new()));
            let mut handles = Vec::new();
            for channel in 0..clients {
                let seen = seen.clone();
                handles.push(pin_connection(
                    &pool,
                    workers,
                    channel,
                    move || async move {
                        seen.lock()
                            .expect("thread recording lock")
                            .push((channel, std::thread::current().id()));
                    },
                ));
            }
            Builder::new_current_thread()
                .build()
                .expect("test join runtime")
                .block_on(async move {
                    for handle in handles {
                        handle.await.expect("pinned Connection finished");
                    }
                });
            let observed = seen.lock().expect("thread recording lock").clone();
            let threads: std::collections::HashSet<_> =
                observed.iter().map(|(_, thread)| *thread).collect();
            assert_eq!(
                threads.len(),
                workers,
                "K={configured} over {clients} clients must drive min(K, clients) threads"
            );
            for (one, one_thread) in &observed {
                for (other, other_thread) in &observed {
                    assert_eq!(
                        one_thread == other_thread,
                        one % workers == other % workers,
                        "clients {one} and {other} share a thread exactly when they share a worker"
                    );
                }
            }
        }
    }
    #[test]
    fn channel_ids_are_stable() {
        assert_eq!(
            (0..3).map(|i| client_id("site-", i)).collect::<Vec<_>>(),
            vec!["site-0", "site-1", "site-2"]
        );
    }

    #[test]
    fn clean_start_follows_session_configuration() {
        let disabled = Config::parse(&serde_json::json!({
            "endpoint": "mqtt://localhost",
            "clientIdPrefix": "site-",
            "session": {"cleanStart": false}
        }))
        .expect("valid config");
        assert!(!disabled.session.clean_start);
        assert_eq!(disabled.session.expiry_interval_seconds, 86_400);

        let enabled = Config::parse(&serde_json::json!({
            "endpoint": "mqtt://localhost",
            "clientIdPrefix": "site-",
            "session": {"cleanStart": true, "expiryIntervalSeconds": 0}
        }))
        .expect("valid config");
        assert!(enabled.session.clean_start);
        assert_eq!(enabled.session.expiry_interval_seconds, 0);
    }

    #[test]
    fn source_publish_preserves_mqtt5_properties() {
        let received_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("test clock")
            .as_millis() as i64;
        let publish = Publish::new(
            "input",
            QoS::AtLeastOnce,
            b"payload".to_vec(),
            Some(PublishProperties {
                payload_format_indicator: Some(1),
                message_expiry_interval: Some(7),
                response_topic: Some("responses".into()),
                correlation_data: Some(b"corr".to_vec().into()),
                user_properties: vec![("x".into(), "1".into()), ("x".into(), "2".into())],
                ..PublishProperties::default()
            }),
        );
        let message = source_message(&publish, received_at);
        assert_eq!(
            message.mqtt5,
            Some(Mqtt5Properties {
                payload_format_indicator: Some(1),
                content_type: None,
                response_topic: Some("responses".into()),
                correlation_data: Some(b"corr".to_vec()),
                user_properties: vec![
                    UserProperty {
                        key: "x".into(),
                        value: "1".into()
                    },
                    UserProperty {
                        key: "x".into(),
                        value: "2".into()
                    },
                ],
            })
        );
        assert_eq!(message.expires_at_unix_ms, Some(received_at + 7_000));
    }
}

#[cfg(test)]
mod confirmation_tests;

#[cfg(test)]
mod publish_tests;

#[cfg(test)]
mod subscription_tests;

#[cfg(test)]
mod thread_model_tests;
