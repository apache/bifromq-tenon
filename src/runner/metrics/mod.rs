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

//! Collects a weak view of available process snapshots without owning Pipelines.

mod stream;

use crate::metrics::{self, MetricsRuntime};
use crate::time::Deadline;
use futures_util::stream::{FuturesUnordered, StreamExt as _};
use opentelemetry_proto::tonic::metrics::v1::MetricsData;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;
use stream::{Connection, PendingCollection};

#[derive(Clone)]
pub(in crate::runner) struct RunnerMetrics {
    shared: Arc<Shared>,
}

impl RunnerMetrics {
    pub(in crate::runner) fn new(runtime: Arc<MetricsRuntime>, timeout: Duration) -> Self {
        Self {
            shared: Arc::new(Shared {
                runtime,
                connections: Mutex::new(HashMap::new()),
                timeout,
            }),
        }
    }

    pub(in crate::runner) async fn collect(&self, include: &[String]) -> MetricsData {
        let deadline = Deadline::start(self.shared.timeout);
        let mut snapshot = if metrics::catalog::includes_owner(include, "runner") {
            self.shared.runtime.collect(include)
        } else {
            MetricsData::default()
        };
        let pending: Vec<_> = if metrics::catalog::includes_owner(include, "pipeline")
            || metrics::catalog::includes_owner(include, "plugin")
        {
            self.connections()
                .values()
                .filter_map(Weak::upgrade)
                .filter_map(|connection| PendingCollection::start(connection, include, deadline))
                .collect()
        } else {
            Vec::new()
        };
        // Every request is sent before waiting. All responses share one deadline.
        // Dropping this future drops the owned sessions and closes their streams.
        let mut pending: FuturesUnordered<_> = pending
            .into_iter()
            .map(|collection| collection.finish(deadline))
            .collect();
        while let Some(result) = pending.next().await {
            if let Some(data) = result {
                snapshot.resource_metrics.extend(data.resource_metrics);
            }
        }
        snapshot
    }

    pub(in crate::runner) fn into_service(
        self,
        launches: crate::runner::pipeline::RunnerPipelineLaunchRegistry,
    ) -> crate::contracts::core::pipeline_metrics_server::PipelineMetricsServer<
        stream::MetricsService,
    > {
        crate::contracts::core::pipeline_metrics_server::PipelineMetricsServer::new(
            stream::MetricsService::new(self, launches),
        )
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX)
    }

    fn attach(
        &self,
        launch_id: Vec<u8>,
        connection: &Arc<Connection>,
    ) -> Result<(), tonic::Status> {
        let mut connections = self.connections();
        connections.retain(|_, connection| connection.strong_count() > 0);
        if connections
            .get(&launch_id)
            .and_then(Weak::upgrade)
            .is_some()
        {
            return Err(tonic::Status::already_exists(
                "Pipeline metrics stream is already attached",
            ));
        }
        connections.insert(launch_id, Arc::downgrade(connection));
        Ok(())
    }

    #[allow(
        clippy::expect_used,
        reason = "the registry never runs user code while locked"
    )]
    fn connections(&self) -> MutexGuard<'_, HashMap<Vec<u8>, Weak<Connection>>> {
        self.shared
            .connections
            .lock()
            .expect("metrics registry lock must not be poisoned")
    }
}

struct Shared {
    runtime: Arc<MetricsRuntime>,
    connections: Mutex<HashMap<Vec<u8>, Weak<Connection>>>,
    timeout: Duration,
}

#[cfg(test)]
pub(in crate::runner) mod test_support {
    use super::*;
    pub(in crate::runner) fn empty() -> std::io::Result<RunnerMetrics> {
        Ok(RunnerMetrics::new(
            metrics::test_support::runner()?,
            Duration::from_secs(2),
        ))
    }
}

#[cfg(test)]
mod tests;
