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

#[tokio::test]
async fn reconnect_retains_cpu_baseline_and_reaps_transport_tasks() -> Result<(), crate::Error> {
    use tokio_stream::wrappers::UnixListenerStream;
    let directory = tempfile::tempdir()?;
    let socket = directory.path().join("metrics.sock");
    let listener = tokio::net::UnixListener::bind(&socket)?;
    let (attached, mut sessions) = mpsc::channel(1);
    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(plugin::plugin_metrics_server::PluginMetricsServer::new(
                Peer(attached),
            ))
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await;
    });
    let executor = crate::process::TrackedExecutor::default();
    let tracked = executor.clone();
    tasks.spawn(run(socket, vec![7; 16], tracked));
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        for attempt in 0..12 {
            let (requests, mut snapshots) =
                sessions.recv().await.ok_or("metrics did not attach")?;
            requests
                .send(plugin::PipelineToPluginMetrics {
                    include: vec!["tenon.plugin.cpu".into()],
                })
                .await?;
            let message = snapshots.message().await?.ok_or("snapshot missing")?;
            let Some(Message::Snapshot(snapshot)) = message.message else {
                return Err::<(), crate::Error>("expected snapshot".into());
            };
            let data = MetricsData::decode(snapshot.metrics.as_slice())?;
            assert_eq!(data.resource_metrics.is_empty(), attempt == 0);
            // End the actual gRPC response stream. The SDK must reconnect without
            // replacing its collector or accumulating completed transport tasks.
            drop(requests);
            drop(snapshots);
            assert!(
                executor
                    .0
                    .lock()
                    .map_err(|_| "task registry poisoned")?
                    .len()
                    <= 6
            );
        }
        Ok::<(), crate::Error>(())
    })
    .await;
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    executor.stop().await;
    result??;
    Ok(())
}

struct Peer(
    mpsc::Sender<(
        mpsc::Sender<plugin::PipelineToPluginMetrics>,
        tonic::Streaming<plugin::PluginToPipelineMetrics>,
    )>,
);
#[tonic::async_trait]
impl plugin::plugin_metrics_server::PluginMetrics for Peer {
    type StreamStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<Item = Result<plugin::PipelineToPluginMetrics, tonic::Status>>
                + Send,
        >,
    >;
    async fn stream(
        &self,
        request: tonic::Request<tonic::Streaming<plugin::PluginToPipelineMetrics>>,
    ) -> Result<tonic::Response<Self::StreamStream>, tonic::Status> {
        let mut snapshots = request.into_inner();
        let attach = snapshots
            .message()
            .await?
            .ok_or_else(|| tonic::Status::invalid_argument("Attach missing"))?;
        assert!(matches!(attach.message, Some(Message::Attach(_))));
        let (requests, incoming) = mpsc::channel(1);
        self.0
            .send((requests, snapshots))
            .await
            .map_err(|_| tonic::Status::cancelled("test closed"))?;
        Ok(tonic::Response::new(Box::pin(
            ReceiverStream::new(incoming).map(Ok),
        )))
    }
}
