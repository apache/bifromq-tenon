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

//! Owns one metrics session while idle or while exactly one HTTP request collects.

use super::RunnerMetrics;
use crate::contracts::core;
use crate::runner::pipeline::RunnerPipelineLaunchRegistry;
use crate::time::Deadline;
use core::{RunnerToPipelineMetrics, pipeline_metrics_to_runner};
use opentelemetry_proto::tonic::metrics::v1::MetricsData;
use pipeline_metrics_to_runner::Message;
use prost::Message as _;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::{mpsc, watch};
use tokio_stream::Stream;
use tokio_stream::wrappers::{ReceiverStream, WatchStream};
use tonic::{Request, Response, Status, Streaming};

pub(super) type Connection = Mutex<Option<Session>>;

pub(super) struct PendingCollection {
    connection: Arc<Connection>,
    session: Session,
}

impl PendingCollection {
    #[allow(
        clippy::expect_used,
        reason = "the session lock only transfers ownership without user code"
    )]
    pub(super) fn start(
        connection: Arc<Connection>,
        include: &[String],
        deadline: Deadline,
    ) -> Option<Self> {
        let session = connection
            .lock()
            .expect("metrics session lock must not be poisoned")
            .take()?;
        session
            .commands
            .try_send(Ok(RunnerToPipelineMetrics {
                include: include.to_vec(),
                remaining_timeout_ms: deadline
                    .remaining()
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
            }))
            .ok()?;
        Some(Self {
            connection,
            session,
        })
    }

    #[allow(
        clippy::expect_used,
        reason = "the same-build Pipeline produces one standard snapshot per request"
    )]
    pub(super) async fn finish(mut self, deadline: Deadline) -> Option<MetricsData> {
        let reply = tokio::select! {
            biased;
            _ = self.session.lifetime.changed() => return None,
            _ = deadline.wait() => return None,
            reply = self.session.snapshots.message() => reply.ok()??,
        };
        let Some(Message::Snapshot(snapshot)) = reply.message else {
            unreachable!("Pipeline sends only snapshots after its metrics attachment");
        };
        let snapshot = MetricsData::decode(snapshot.metrics.as_slice())
            .expect("Pipeline produces standard MetricsData protobuf");
        *self
            .connection
            .lock()
            .expect("metrics session lock must not be poisoned") = Some(self.session);
        Some(snapshot)
    }
}

#[derive(Clone)]
pub(in crate::runner) struct MetricsService {
    metrics: RunnerMetrics,
    launches: RunnerPipelineLaunchRegistry,
}

impl MetricsService {
    pub(super) fn new(metrics: RunnerMetrics, launches: RunnerPipelineLaunchRegistry) -> Self {
        Self { metrics, launches }
    }
}

#[tonic::async_trait]
impl core::pipeline_metrics_server::PipelineMetrics for MetricsService {
    type StreamStream = MetricsStream;

    async fn stream(
        &self,
        request: Request<Streaming<core::PipelineMetricsToRunner>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let mut snapshots = request.into_inner();
        let attach = snapshots
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("Metrics Attach is required"))?;
        let Some(Message::Attach(attach)) = attach.message else {
            unreachable!("Pipeline begins its metrics stream with Attach");
        };
        let lifetime = self
            .launches
            .metrics_lifetime(&attach.launch_id)
            .ok_or_else(|| Status::failed_precondition("Pipeline launch is unavailable"))?;
        let (commands, requests) = mpsc::channel(1);
        let connection = Arc::new(Mutex::new(Some(Session {
            commands,
            snapshots,
            lifetime: lifetime.clone(),
        })));
        self.metrics.attach(attach.launch_id, &connection)?;
        Ok(Response::new(MetricsStream {
            _connection: connection,
            requests: ReceiverStream::new(requests),
            lifetime: WatchStream::from_changes(lifetime),
        }))
    }
}

/// The response stream retains the idle session; the registry holds only a weak reference.
pub(in crate::runner) struct MetricsStream {
    _connection: Arc<Connection>,
    requests: ReceiverStream<Result<RunnerToPipelineMetrics, Status>>,
    lifetime: WatchStream<()>,
}

impl Stream for MetricsStream {
    type Item = Result<RunnerToPipelineMetrics, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if Pin::new(&mut this.lifetime).poll_next(cx).is_ready() {
            return Poll::Ready(None);
        }
        Pin::new(&mut this.requests).poll_next(cx)
    }
}

pub(super) struct Session {
    commands: mpsc::Sender<Result<RunnerToPipelineMetrics, Status>>,
    snapshots: Streaming<core::PipelineMetricsToRunner>,
    lifetime: watch::Receiver<()>,
}
