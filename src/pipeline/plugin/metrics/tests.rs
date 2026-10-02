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

use super::*;
use opentelemetry_proto::tonic::{
    common::v1::InstrumentationScope,
    metrics::v1::{Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics},
    resource::v1::Resource,
};
use std::time::Duration;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Endpoint, Server};

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn sample(launch: &[u8]) -> MetricsData {
    MetricsData {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![
                    attribute("service.namespace", "tenon"),
                    attribute("service.name", "tenon.plugin"),
                    attribute(
                        "service.instance.id",
                        &base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(launch),
                    ),
                ],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "tenon-plugin-sdk".into(),
                    version: "0.1.0".into(),
                    ..Default::default()
                }),
                metrics: vec![Metric {
                    name: "tenon.plugin.memory".into(),
                    unit: "By".into(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 123,
                            value: Some(Number::AsInt(42)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

struct ServerOwner {
    registry: PluginProcessMetrics,
    _directory: tempfile::TempDir,
    socket: String,
    tasks: tokio::task::JoinSet<()>,
}
impl ServerOwner {
    fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let registry = PluginProcessMetrics::default();
        let directory = tempfile::tempdir_in("/tmp")?;
        let socket = directory.path().join("p.sock");
        let listener = UnixListener::bind(&socket)?;
        let service = registry.clone().into_service();
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            let _ = Server::builder()
                .add_service(service)
                .serve_with_incoming(UnixListenerStream::new(listener))
                .await;
        });
        Ok(Self {
            registry,
            _directory: directory,
            socket: format!("unix://{}", socket.display()),
            tasks,
        })
    }
    async fn peer(&self, launch: &[u8]) -> Result<Peer, tonic::Status> {
        let channel = Endpoint::from_shared(self.socket.clone())
            .map_err(|error| Status::unknown(error.to_string()))?
            .connect()
            .await
            .map_err(|error| Status::unknown(error.to_string()))?;
        let mut client = plugin::plugin_metrics_client::PluginMetricsClient::new(channel);
        let (outgoing, incoming) = mpsc::channel(1);
        outgoing
            .send(plugin::PluginToPipelineMetrics {
                message: Some(Message::Attach(plugin::PluginMetricsAttach {
                    launch_id: launch.to_vec(),
                })),
            })
            .await
            .map_err(|_| Status::unknown("peer closed"))?;
        let requests = client
            .stream(ReceiverStream::new(incoming))
            .await?
            .into_inner();
        Ok(Peer { outgoing, requests })
    }
}
impl Drop for ServerOwner {
    fn drop(&mut self) {
        self.tasks.abort_all();
    }
}
struct Peer {
    outgoing: mpsc::Sender<plugin::PluginToPipelineMetrics>,
    requests: Streaming<plugin::PipelineToPluginMetrics>,
}
impl Peer {
    async fn reply(&self, data: MetricsData) -> TestResult {
        self.outgoing
            .send(plugin::PluginToPipelineMetrics {
                message: Some(Message::Snapshot(plugin::PluginMetricsSnapshot {
                    metrics: data.encode_to_vec(),
                })),
            })
            .await?;
        Ok(())
    }
}
fn budget() -> Deadline {
    Deadline::start(Duration::from_secs(2))
}

#[test]
fn snapshots_require_the_launch_identity_fixed_gauges_and_requested_names() {
    let launch = [1; 16];
    let mut data = sample(&launch);
    assert!(validate(&data, &launch, &[]).is_some());
    data.resource_metrics[0].scope_metrics[0].scope = Some(InstrumentationScope {
        name: "example-language-sdk".into(),
        version: "1.0.0".into(),
        ..Default::default()
    });
    assert!(validate(&data, &launch, &[]).is_some());
    assert!(validate(&data, &[2; 16], &[]).is_none());
    assert!(validate(&data, &launch, &["tenon.plugin.cpu".into()]).is_none());
    data.resource_metrics[0].scope_metrics[0].metrics[0].unit = "KB".into();
    assert!(validate(&data, &launch, &[]).is_none());
    assert!(validate(&MetricsData::default(), &launch, &[]).is_some());
    let mut multiple = sample(&launch);
    multiple
        .resource_metrics
        .push(multiple.resource_metrics[0].clone());
    assert!(validate(&multiple, &launch, &[]).is_none());
}

#[tokio::test]
async fn empty_and_repeated_snapshots_preserve_the_session_and_registered_labels() -> TestResult {
    let server = ServerOwner::start()?;
    let launch = [5; 16];
    let labels = vec![
        attribute("tenon.node.id", "node"),
        attribute("tenon.plugin.instance.id", "plugin"),
    ];
    let _registration = server.registry.register(&launch, labels.clone());
    // A registered process may not have attached its metrics connection yet.
    assert!(
        server
            .registry
            .collect(&[], budget())
            .await
            .resource_metrics
            .is_empty()
    );
    let mut peer = server.peer(&launch).await?;
    for reply in [MetricsData::default(), sample(&launch), sample(&launch)] {
        let mut expected = reply.clone();
        if let Some(resource) = expected.resource_metrics.first_mut() {
            resource
                .resource
                .as_mut()
                .ok_or("resource missing")?
                .attributes
                .extend(labels.clone());
        }
        let registry = server.registry.clone();
        let pending = tokio::spawn(async move { registry.collect(&[], budget()).await });
        assert!(peer.requests.message().await?.is_some());
        peer.reply(reply).await?;
        assert_eq!(pending.await?, expected);
    }
    Ok(())
}

#[tokio::test]
async fn slow_peer_does_not_hide_fast_data_and_timeout_closes_its_stream() -> TestResult {
    let server = ServerOwner::start()?;
    let fast_id = [1; 16];
    let slow_id = [2; 16];
    let _fast = server.registry.register(
        &fast_id,
        vec![attribute("tenon.plugin.instance.id", "fast")],
    );
    let _slow = server.registry.register(&slow_id, vec![]);
    let mut fast = server.peer(&fast_id).await?;
    let mut slow = server.peer(&slow_id).await?;
    let registry = server.registry.clone();
    let collection = tokio::spawn(async move {
        registry
            .collect(&[], Deadline::start(Duration::from_millis(100)))
            .await
    });
    assert!(fast.requests.message().await?.is_some());
    assert!(slow.requests.message().await?.is_some());
    // An overlapping request does not queue behind either busy connection.
    assert!(
        server
            .registry
            .collect(&[], budget())
            .await
            .resource_metrics
            .is_empty()
    );
    fast.reply(sample(&fast_id)).await?;
    let data = collection.await?;
    assert_eq!(data.resource_metrics.len(), 1);
    assert!(
        data.resource_metrics[0]
            .resource
            .as_ref()
            .ok_or("resource missing")?
            .attributes
            .contains(&attribute("tenon.plugin.instance.id", "fast"))
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(2), slow.requests.message())
            .await??
            .is_none()
    );
    // No stale response or former session can be reused by a later collection.
    assert!(
        server
            .registry
            .collect(&["tenon.queue.usage".into()], budget())
            .await
            .resource_metrics
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_retirement_and_bad_payload_only_close_the_metrics_session() -> TestResult {
    let server = ServerOwner::start()?;
    let launch = [3; 16];
    assert!(server.peer(&launch).await.is_err());
    let registration = server.registry.register(&launch, vec![]);
    let mut peer = server.peer(&launch).await?;
    assert!(server.peer(&launch).await.is_err());
    let registry = server.registry.clone();
    let pending = tokio::spawn(async move { registry.collect(&[], budget()).await });
    assert!(peer.requests.message().await?.is_some());
    pending.abort();
    let _ = pending.await;
    assert!(
        tokio::time::timeout(Duration::from_secs(2), peer.requests.message())
            .await??
            .is_none()
    );
    let mut peer = server.peer(&launch).await?;
    let registry = server.registry.clone();
    let pending = tokio::spawn(async move { registry.collect(&[], budget()).await });
    assert!(peer.requests.message().await?.is_some());
    peer.reply(sample(&[9; 16])).await?;
    assert!(pending.await?.resource_metrics.is_empty());
    assert!(peer.requests.message().await?.is_none());
    let mut peer = server.peer(&launch).await?;
    drop(registration);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), peer.requests.message())
            .await??
            .is_none()
    );
    assert!(server.peer(&launch).await.is_err());
    Ok(())
}

#[tokio::test]
async fn unsolicited_and_oversized_snapshots_are_rejected() -> TestResult {
    let server = ServerOwner::start()?;
    let launch = [4; 16];
    let _registration = server.registry.register(&launch, vec![]);
    let mut peer = server.peer(&launch).await?;
    peer.reply(sample(&launch)).await?;
    assert!(
        tokio::time::timeout(Duration::from_secs(2), peer.requests.message())
            .await??
            .is_none()
    );
    let mut peer = server.peer(&launch).await?;
    let registry = server.registry.clone();
    let pending = tokio::spawn(async move { registry.collect(&[], budget()).await });
    assert!(peer.requests.message().await?.is_some());
    peer.outgoing
        .send(plugin::PluginToPipelineMetrics {
            message: Some(Message::Snapshot(plugin::PluginMetricsSnapshot {
                metrics: vec![0; MAXIMUM_MESSAGE_BYTES],
            })),
        })
        .await?;
    assert!(pending.await?.resource_metrics.is_empty());
    assert!(peer.requests.message().await?.is_none());
    Ok(())
}
