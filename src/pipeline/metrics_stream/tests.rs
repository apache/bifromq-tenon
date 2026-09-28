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
use crate::metrics::CoreProcess;
use opentelemetry_proto::tonic::metrics::v1::{MetricsData, metric, number_data_point};
use std::io;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Streaming};

#[tokio::test(flavor = "current_thread")]
async fn reconnect_preserves_accumulation_and_shutdown_closes_the_idle_stream() -> io::Result<()> {
    let directory = tempfile::tempdir_in("/tmp")?;
    let socket = directory.path().join("metrics.sock");
    let listener = UnixListener::bind(&socket)?;
    let (connections, mut connected) = mpsc::channel(1);
    let mut server = JoinSet::new();
    server.spawn(
        Server::builder()
            .add_service(core::pipeline_metrics_server::PipelineMetricsServer::new(
                PeerService { connections },
            ))
            .serve_with_incoming(UnixListenerStream::new(listener)),
    );
    let runtime = MetricsRuntime::start(
        None,
        CoreProcess::Pipeline {
            document_id: "observed",
            launch_id: b"launch-a",
        },
    )
    .map_err(io::Error::other)?;
    let counter = runtime
        .meter()
        .u64_counter("tenon.flow.input.records")
        .build();
    let metrics = PipelineMetrics::start(
        runtime,
        &socket.to_string_lossy(),
        b"launch-a".to_vec(),
        PluginProcessMetrics::default(),
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        for expected in [1, 2] {
            let mut peer = connected
                .recv()
                .await
                .ok_or_else(|| io::Error::other("Connection missing"))?;
            let attach = peer.snapshots.message().await.map_err(io::Error::other)?;
            assert!(matches!(
                attach.and_then(|message| message.message),
                Some(core::pipeline_metrics_to_runner::Message::Attach(attach))
                    if attach.launch_id == b"launch-a"
            ));
            counter.add(1, &[]);
            peer.requests
                .send(Ok(core::RunnerToPipelineMetrics {
                    include: vec!["tenon.flow.input.records".to_owned()],
                    remaining_timeout_ms: 1000,
                }))
                .await
                .map_err(io::Error::other)?;
            let reply = peer.snapshots.message().await.map_err(io::Error::other)?;
            let Some(core::pipeline_metrics_to_runner::Message::Snapshot(snapshot)) =
                reply.and_then(|message| message.message)
            else {
                return Err(io::Error::other("Snapshot missing"));
            };
            let snapshot =
                MetricsData::decode(snapshot.metrics.as_slice()).map_err(io::Error::other)?;
            let recorded = &snapshot.resource_metrics[0].scope_metrics[0].metrics;
            assert_eq!(recorded.len(), 1);
            assert_eq!(recorded[0].name, "tenon.flow.input.records");
            let Some(metric::Data::Sum(sum)) = &recorded[0].data else {
                return Err(io::Error::other("Counter missing"));
            };
            assert_eq!(
                sum.data_points[0].value,
                Some(number_data_point::Value::AsInt(expected))
            );
            if expected == 2 {
                metrics.shutdown().await;
                assert!(
                    peer.snapshots
                        .message()
                        .await
                        .map_err(io::Error::other)?
                        .is_none()
                );
                return Ok(());
            }
            // Ending only the metrics response forces a reconnect without rebuilding the provider.
            drop(peer);
        }
        unreachable!("the second connection shuts down the owner")
    })
    .await
    .map_err(io::Error::other)?
}

struct PeerService {
    connections: mpsc::Sender<Peer>,
}

#[tonic::async_trait]
impl core::pipeline_metrics_server::PipelineMetrics for PeerService {
    type StreamStream = ReceiverStream<Result<core::RunnerToPipelineMetrics, Status>>;

    async fn stream(
        &self,
        request: Request<Streaming<core::PipelineMetricsToRunner>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let (requests, incoming) = mpsc::channel(1);
        self.connections
            .send(Peer {
                requests,
                snapshots: request.into_inner(),
            })
            .await
            .map_err(|_| Status::cancelled("Test peer closed"))?;
        Ok(Response::new(ReceiverStream::new(incoming)))
    }
}

struct Peer {
    requests: mpsc::Sender<Result<core::RunnerToPipelineMetrics, Status>>,
    snapshots: Streaming<core::PipelineMetricsToRunner>,
}
