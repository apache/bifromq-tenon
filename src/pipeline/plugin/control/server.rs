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

//! Private UDS listener and gRPC task in a Runner-owned directory.

use crate::identifiers::PluginInstanceId;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Counter;
use opentelemetry::metrics::Meter;
use std::error::Error;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::net::UnixListener;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

use super::super::metrics::PluginProcessMetrics;
use super::adapter::PluginControlAdapter;
use super::launch_registry::{PendingPluginControl, PluginControlLaunchRegistry};
use crate::payload_contract::PluginInterface;

type PluginControlServerTask = JoinHandle<Result<(), tonic::transport::Error>>;

/// Owns one Pipeline's Plugin Control listener, launch registry, and server task.
pub(in crate::pipeline) struct PluginControlServer {
    launcher: PluginControlLauncher,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<PluginControlServerTask>,
}

impl PluginControlServer {
    /// Binds the short private UDS path already owned by the Runner launch.
    pub(in crate::pipeline) fn start(
        socket_path: PathBuf,
    ) -> Result<Self, PluginControlServerError> {
        let launches = PluginControlLaunchRegistry::try_new()
            .map_err(PluginControlServerError::RandomnessUnavailable)?;
        let listener =
            UnixListener::bind(&socket_path).map_err(PluginControlServerError::SocketBind)?;
        let service = PluginControlAdapter::new(launches.clone()).into_service();
        let metrics = PluginProcessMetrics::default();
        let launcher = PluginControlLauncher {
            metrics: metrics.clone(),
            node_id: None,
            launches,
            socket_path: Arc::new(socket_path),
            restarts: None,
        };
        let (shutdown, shutdown_requested) = oneshot::channel();
        let task = tokio::spawn(
            Server::builder()
                .add_service(service)
                .add_service(metrics.into_service())
                .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async {
                    let _ = shutdown_requested.await;
                }),
        );
        Ok(Self {
            launcher,
            shutdown: Some(shutdown),
            task: Some(task),
        })
    }

    /// Returns the cloneable launch capability without transferring server ownership.
    pub(in crate::pipeline) fn process_metrics(&self) -> PluginProcessMetrics {
        self.launcher.metrics.clone()
    }

    pub(in crate::pipeline) fn launcher(&self) -> PluginControlLauncher {
        self.launcher.clone()
    }

    /// Observes service exit without detaching the task when a select branch is cancelled.
    #[expect(
        clippy::expect_used,
        reason = "service exit is consumed once by the control loop or shutdown"
    )]
    pub(in crate::pipeline) async fn wait(&mut self) -> Result<(), PluginControlServerError> {
        let result = self
            .task
            .as_mut()
            .expect("Plugin Control service exit was already consumed")
            .await;
        let _completed = self.task.take();
        result
            .map_err(PluginControlServerError::Task)?
            .map_err(PluginControlServerError::Serve)
    }

    /// Stops accepting connections and drains streams; the Runner removes the directory after reap.
    pub(in crate::pipeline) async fn shutdown(mut self) -> Result<(), PluginControlServerError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if self.task.is_some() {
            self.wait().await
        } else {
            Ok(())
        }
    }
}

impl fmt::Debug for PluginControlServer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginControlServer")
            .field("launcher", &self.launcher)
            .finish_non_exhaustive()
    }
}

/// Cloneable authority for registering children against one still-owned server.
#[derive(Clone)]
pub(in crate::pipeline) struct PluginControlLauncher {
    metrics: PluginProcessMetrics,
    node_id: Option<String>,
    launches: PluginControlLaunchRegistry,
    // This immutable path is shared because child owners outlive the borrow used
    // to create them while the server retains the actual socket resource owner.
    socket_path: Arc<PathBuf>,
    restarts: Option<Counter<u64>>,
}

impl PluginControlLauncher {
    pub(in crate::pipeline) fn with_node_id(mut self, node_id: Option<String>) -> Self {
        self.node_id = node_id;
        self
    }
    pub(in crate::pipeline::plugin) fn register_metrics(
        &self,
        launch: &[u8],
        mut attributes: Vec<opentelemetry_proto::tonic::common::v1::KeyValue>,
    ) -> super::super::metrics::Registration {
        if let Some(node) = &self.node_id {
            attributes.push(super::super::metrics::attribute("tenon.node.id", node));
        }
        self.metrics.register(launch, attributes)
    }

    pub(in crate::pipeline) fn with_metrics(mut self, meter: Option<&Meter>) -> Self {
        self.restarts = meter.map(|meter| {
            meter
                .u64_counter("tenon.plugin.restarts")
                .with_unit("{restart}")
                .build()
        });
        self
    }

    /// Registers one launch before its child process is spawned.
    pub(in crate::pipeline::plugin) fn register(
        &self,
        interface: PluginInterface,
    ) -> PendingPluginControl {
        self.launches.register(interface)
    }

    /// Borrows the exact absolute UDS path appended to the child command.
    pub(in crate::pipeline::plugin) fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub(in crate::pipeline::plugin) fn record_restart(&self, id: &PluginInstanceId) {
        if let Some(restarts) = &self.restarts {
            restarts.add(
                1,
                &[KeyValue::new(
                    "tenon.plugin.instance.id",
                    id.as_str().to_owned(),
                )],
            );
        }
    }
}

impl fmt::Debug for PluginControlLauncher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginControlLauncher")
            .field("launches", &self.launches)
            .field("socket_path", &self.socket_path)
            .finish()
    }
}

impl Drop for PluginControlServer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// A Pipeline-private Plugin Control server could not preserve its ownership boundary.
#[derive(Debug)]
pub(in crate::pipeline) enum PluginControlServerError {
    RandomnessUnavailable(getrandom::Error),
    SocketBind(io::Error),
    Serve(tonic::transport::Error),
    Task(JoinError),
}

impl fmt::Display for PluginControlServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RandomnessUnavailable(_) => {
                formatter.write_str("Plugin launch identity initialization failed")
            }
            Self::SocketBind(_) => formatter.write_str("Plugin Control socket could not be bound"),
            Self::Serve(_) | Self::Task(_) => formatter.write_str("Plugin Control service failed"),
        }
    }
}

impl Error for PluginControlServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::RandomnessUnavailable(source) => Some(source),
            Self::SocketBind(source) => Some(source),
            Self::Serve(source) => Some(source),
            Self::Task(source) => Some(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_wait_retains_the_task_and_shutdown_keeps_the_runner_directory()
    -> Result<(), Box<dyn Error>> {
        let directory = tempfile::Builder::new()
            .prefix("tenon-plugin-wait-")
            .tempdir_in("/tmp")?;
        let mut server =
            PluginControlServer::start(directory.path().join(crate::PLUGIN_CONTROL_SOCKET_NAME))?;
        {
            let waiting = server.wait();
            tokio::pin!(waiting);
            tokio::select! {
                biased;
                result = &mut waiting => return Err(format!("live service exited: {result:?}").into()),
                () = tokio::task::yield_now() => {}
            }
        }
        server.shutdown().await?;
        assert!(
            directory.path().is_dir(),
            "only the Runner may remove this directory"
        );
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn unexpected_service_exit_remains_observable_after_a_cancelled_wait()
    -> Result<(), Box<dyn Error>> {
        let directory = tempfile::Builder::new()
            .prefix("tenon-plugin-exit-")
            .tempdir_in("/tmp")?;
        let mut server =
            PluginControlServer::start(directory.path().join(crate::PLUGIN_CONTROL_SOCKET_NAME))?;
        {
            let waiting = server.wait();
            tokio::pin!(waiting);
            tokio::select! {
                biased;
                result = &mut waiting => return Err(format!("live service exited: {result:?}").into()),
                () = tokio::task::yield_now() => {}
            }
        }
        server
            .task
            .as_ref()
            .ok_or("server task is missing")?
            .abort();
        assert!(
            matches!(server.wait().await, Err(PluginControlServerError::Task(source)) if source.is_cancelled())
        );
        server.shutdown().await?;
        assert!(directory.path().is_dir());
        Ok(())
    }
}
