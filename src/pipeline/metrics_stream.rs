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

//! Owns the private provider and the reconnecting, request-driven metrics stream.

use super::plugin::metrics::PluginProcessMetrics;
use crate::contracts::core;
use crate::metrics::MetricsRuntime;
use crate::time::Deadline;
use opentelemetry::metrics::Meter;
use prost::Message as _;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_stream::StreamExt as _;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tonic::transport::{Channel, Endpoint};

pub(super) struct PipelineMetrics {
    runtime: Arc<MetricsRuntime>,
    task: JoinSet<()>,
}

impl PipelineMetrics {
    #[allow(
        clippy::expect_used,
        reason = "Runner supplies the exact private socket address"
    )]
    pub(super) fn start(
        runtime: MetricsRuntime,
        socket: &str,
        launch_id: Vec<u8>,
        plugins: PluginProcessMetrics,
    ) -> Self {
        let runtime = Arc::new(runtime);
        let collector = Arc::clone(&runtime);
        let endpoint = Endpoint::from_shared(format!("unix://{socket}"))
            .expect("Runner supplies a valid metrics socket address");
        let mut task = JoinSet::new();
        task.spawn(async move {
            loop {
                if let Ok(channel) = endpoint.connect().await {
                    let _disconnected = serve(channel, &launch_id, &collector, &plugins).await;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
        Self { runtime, task }
    }

    pub(super) fn meter(&self) -> Meter {
        self.runtime.meter()
    }

    pub(super) async fn shutdown(mut self) {
        self.task.abort_all();
        while let Some(result) = self.task.join_next().await {
            if let Err(error) = result {
                assert!(
                    error.is_cancelled(),
                    "Pipeline metrics transport task panicked: {error}"
                );
            }
        }
        self.runtime.shutdown();
    }
}

async fn serve(
    channel: Channel,
    launch_id: &[u8],
    runtime: &MetricsRuntime,
    plugins: &PluginProcessMetrics,
) -> Result<(), Status> {
    let mut client = core::pipeline_metrics_client::PipelineMetricsClient::new(channel)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);
    let (snapshots, outgoing) = mpsc::channel(1);
    let attach = core::PipelineMetricsToRunner {
        message: Some(core::pipeline_metrics_to_runner::Message::Attach(
            core::PipelineMetricsAttach {
                launch_id: launch_id.to_vec(),
            },
        )),
    };
    let mut requests = client
        .stream(tokio_stream::iter([attach]).chain(ReceiverStream::new(outgoing)))
        .await?
        .into_inner();
    while let Some(request) = requests.message().await? {
        let deadline = Deadline::start(Duration::from_millis(request.remaining_timeout_ms / 2));
        let mut snapshot = runtime.collect(&request.include);
        let collected = tokio::select! {
            biased;
            closed = requests.message() => {
                return match closed? {
                    None => Ok(()),
                    Some(_) => Err(Status::invalid_argument("Only one collection may be in flight")),
                };
            }
            collected = plugins.collect(&request.include, deadline) => collected,
        };
        snapshot.resource_metrics.extend(collected.resource_metrics);
        snapshots
            .send(core::PipelineMetricsToRunner {
                message: Some(core::pipeline_metrics_to_runner::Message::Snapshot(
                    core::PipelineMetricsSnapshot {
                        metrics: snapshot.encode_to_vec(),
                    },
                )),
            })
            .await
            .map_err(|_| Status::cancelled("Metrics stream closed"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
