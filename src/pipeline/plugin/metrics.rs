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

//! Launch-bound Plugin metrics sessions, independent of lifecycle control.

use crate::contracts::plugin::{self, plugin_to_pipeline_metrics::Message};
use crate::time::Deadline;
use base64::Engine as _;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
use opentelemetry_proto::tonic::metrics::v1::{
    MetricsData, metric::Data, number_data_point::Value as Number,
};
use prost::Message as _;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use tokio::sync::{mpsc, watch};
use tokio_stream::{
    Stream,
    wrappers::{ReceiverStream, WatchStream},
};
use tonic::{Request, Response, Status, Streaming};

const MAXIMUM_MESSAGE_BYTES: usize = 64 * 1024;
#[derive(Debug, Default)]
struct Connection {
    session: Mutex<Option<Session>>,
    wake: futures_util::task::AtomicWaker,
}

#[derive(Clone, Debug, Default)]
pub(in crate::pipeline) struct PluginProcessMetrics(Arc<Mutex<HashMap<Vec<u8>, Entry>>>);

#[derive(Debug)]
struct Entry {
    attributes: Vec<KeyValue>,
    lifetime: watch::Receiver<()>,
    connection: Weak<Connection>,
}

/// The process owner alone retains registration; dropping it revokes all sessions.
#[derive(Debug)]
pub(super) struct Registration {
    registry: PluginProcessMetrics,
    launch_id: Vec<u8>,
    _lifetime: watch::Sender<()>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Ok(mut registry) = self.registry.0.lock() {
            registry.remove(&self.launch_id);
        }
    }
}

impl PluginProcessMetrics {
    pub(super) fn register(&self, launch_id: &[u8], attributes: Vec<KeyValue>) -> Registration {
        let (lifetime, receiver) = watch::channel(());
        if let Ok(mut entries) = self.0.lock() {
            entries.insert(
                launch_id.to_vec(),
                Entry {
                    attributes,
                    lifetime: receiver,
                    connection: Weak::new(),
                },
            );
        }
        Registration {
            registry: self.clone(),
            launch_id: launch_id.to_vec(),
            _lifetime: lifetime,
        }
    }

    pub(in crate::pipeline) fn into_service(
        self,
    ) -> plugin::plugin_metrics_server::PluginMetricsServer<Self> {
        plugin::plugin_metrics_server::PluginMetricsServer::new(self)
            .max_decoding_message_size(MAXIMUM_MESSAGE_BYTES)
            .max_encoding_message_size(MAXIMUM_MESSAGE_BYTES)
    }

    pub(in crate::pipeline) async fn collect(
        &self,
        include: &[String],
        deadline: Deadline,
    ) -> MetricsData {
        let mut output = MetricsData::default();
        if !crate::metrics::catalog::includes_owner(include, "plugin") || deadline.has_elapsed() {
            return output;
        }
        let available = match self.0.lock() {
            Ok(entries) => entries
                .iter()
                .filter_map(|(launch, entry)| {
                    Some((
                        launch.clone(),
                        entry.attributes.clone(),
                        entry.connection.upgrade()?,
                    ))
                })
                .collect::<Vec<_>>(),
            Err(_) => return output,
        };
        let mut pending = FuturesUnordered::new();
        for (launch, attributes, connection) in available {
            let session = connection
                .session
                .lock()
                .ok()
                .and_then(|mut session| session.take());
            if let Some(session) = session {
                if session
                    .commands
                    .try_send(Ok(plugin::PipelineToPluginMetrics {
                        include: include.to_vec(),
                    }))
                    .is_err()
                {
                    continue;
                }
                pending.push(collect_one(
                    connection, session, launch, attributes, include, deadline,
                ));
            }
        }
        while let Some(snapshot) = pending.next().await {
            if let Some(snapshot) = snapshot {
                output.resource_metrics.extend(snapshot.resource_metrics);
            }
        }
        output
    }
}

async fn collect_one(
    connection: Arc<Connection>,
    mut session: Session,
    launch: Vec<u8>,
    attributes: Vec<KeyValue>,
    include: &[String],
    deadline: Deadline,
) -> Option<MetricsData> {
    let reply = tokio::select! {
        biased;
        _ = session.lifetime.changed() => return None,
        _ = deadline.wait() => return None,
        reply = session.snapshots.message() => reply.ok()??,
    };
    let Message::Snapshot(reply) = reply.message? else {
        return None;
    };
    let mut snapshot = MetricsData::decode(reply.metrics.as_slice()).ok()?;
    validate(&snapshot, &launch, include)?;
    if session.lifetime.has_changed().is_err() {
        return None;
    }
    for resource in &mut snapshot.resource_metrics {
        resource
            .resource
            .as_mut()?
            .attributes
            .extend(attributes.clone());
    }
    *connection.session.lock().ok()? = Some(session);
    connection.wake.wake();
    Some(snapshot)
}

fn validate(snapshot: &MetricsData, launch: &[u8], include: &[String]) -> Option<()> {
    if snapshot.resource_metrics.is_empty() {
        return Some(());
    }
    if snapshot.resource_metrics.len() != 1 {
        return None;
    }
    let resource = &snapshot.resource_metrics[0];
    let attrs = &resource.resource.as_ref()?.attributes;
    let identity = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(launch);
    if attrs.len() != 3
        || ![
            attribute("service.namespace", "tenon"),
            attribute("service.name", "tenon.plugin"),
            attribute("service.instance.id", &identity),
        ]
        .iter()
        .all(|expected| attrs.contains(expected))
        || resource.scope_metrics.len() != 1
    {
        return None;
    }
    let scope = &resource.scope_metrics[0];
    let identity = scope.scope.as_ref()?;
    if identity.name.is_empty()
        || identity.version.is_empty()
        || !identity.attributes.is_empty()
        || scope.metrics.len() > 2
    {
        return None;
    }
    let mut names = std::collections::HashSet::new();
    for metric in &scope.metrics {
        if !names.insert(&metric.name) || (!include.is_empty() && !include.contains(&metric.name)) {
            return None;
        }
        let Data::Gauge(gauge) = metric.data.as_ref()? else {
            return None;
        };
        if gauge.data_points.len() != 1 {
            return None;
        }
        let point = &gauge.data_points[0];
        if !point.attributes.is_empty() || point.time_unix_nano == 0 || point.flags != 0 {
            return None;
        }
        match (
            metric.name.as_str(),
            metric.unit.as_str(),
            point.value.as_ref()?,
        ) {
            ("tenon.plugin.cpu", "1", Number::AsDouble(value))
                if value.is_finite() && *value >= 0.0 => {}
            ("tenon.plugin.memory", "By", Number::AsInt(value)) if *value >= 0 => {}
            _ => return None,
        }
    }
    Some(())
}

pub(in crate::pipeline) fn attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.into(),
        key_strindex: 0,
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.into())),
        }),
    }
}

#[tonic::async_trait]
impl plugin::plugin_metrics_server::PluginMetrics for PluginProcessMetrics {
    type StreamStream = MetricsStream;
    async fn stream(
        &self,
        request: Request<Streaming<plugin::PluginToPipelineMetrics>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let mut snapshots = request.into_inner();
        let Some(Message::Attach(attach)) = snapshots
            .message()
            .await?
            .and_then(|message| message.message)
        else {
            return Err(Status::invalid_argument("Metrics Attach is required"));
        };
        let mut entries = self
            .0
            .lock()
            .map_err(|_| Status::unavailable("Metrics registry unavailable"))?;
        let entry = entries
            .get_mut(&attach.launch_id)
            .ok_or_else(|| Status::failed_precondition("Plugin launch unavailable"))?;
        if entry.connection.upgrade().is_some() {
            return Err(Status::already_exists(
                "Plugin metrics stream already attached",
            ));
        }
        let (commands, requests) = mpsc::channel(1);
        let connection = Arc::new(Connection {
            session: Mutex::new(Some(Session {
                commands,
                snapshots,
                lifetime: entry.lifetime.clone(),
            })),
            wake: futures_util::task::AtomicWaker::new(),
        });
        entry.connection = Arc::downgrade(&connection);
        Ok(Response::new(MetricsStream {
            _connection: connection,
            requests: ReceiverStream::new(requests),
            lifetime: WatchStream::from_changes(entry.lifetime.clone()),
        }))
    }
}

pub(in crate::pipeline) struct MetricsStream {
    _connection: Arc<Connection>,
    requests: ReceiverStream<Result<plugin::PipelineToPluginMetrics, Status>>,
    lifetime: WatchStream<()>,
}
impl Stream for MetricsStream {
    type Item = Result<plugin::PipelineToPluginMetrics, Status>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if Pin::new(&mut this.lifetime).poll_next(cx).is_ready() {
            return Poll::Ready(None);
        }
        this._connection.wake.register(cx.waker());
        if let Ok(mut slot) = this._connection.session.lock()
            && let Some(session) = slot.as_mut()
            && Pin::new(&mut session.snapshots).poll_next(cx).is_ready()
        {
            // Any response while no Collect owns this session is unsolicited.
            slot.take();
            return Poll::Ready(None);
        }
        Pin::new(&mut this.requests).poll_next(cx)
    }
}
#[derive(Debug)]
struct Session {
    commands: mpsc::Sender<Result<plugin::PipelineToPluginMetrics, Status>>,
    snapshots: Streaming<plugin::PluginToPipelineMetrics>,
    lifetime: watch::Receiver<()>,
}

#[cfg(test)]
mod tests;
