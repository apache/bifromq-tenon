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

//! Public Runner HTTP boundary.
//!
//! This module owns the listener and server task. Route modules translate
//! management operations and expose the embedded Document Schema.

mod app;
mod diagnostics;
mod documents;
mod lifecycle;
mod metrics;
mod pipelines;
mod plugins;
mod response;
pub(super) mod tls;
mod transport;

use crate::runner;
use crate::runner::diagnostics::RunnerDiagnostics;
use crate::runner::extensions::HttpApiAuthorization;
use crate::runner::management::RunnerManagementClient;
use app::HttpServices;
use lifecycle::HttpRequestLifecycle;
use std::error::Error;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};

/// Owns the complete lifetime of one Runner HTTP server task.
pub(crate) struct RunnerHttpServer {
    lifecycle: HttpRequestLifecycle,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl RunnerHttpServer {
    pub(crate) async fn start(
        listen_address: SocketAddr,
        tls: Option<tls::TlsServerConfig>,
        management: RunnerManagementClient,
        diagnostics: RunnerDiagnostics,
        metrics: runner::metrics::RunnerMetrics,
        http_authorization: Arc<dyn HttpApiAuthorization>,
    ) -> Result<Self, RunnerHttpServerError> {
        let listener = TcpListener::bind(listen_address)
            .await
            .map_err(RunnerHttpServerError::Bind)?;
        let (shutdown, shutdown_requested) = oneshot::channel();
        let lifecycle = HttpRequestLifecycle::new();
        let app = app::router(
            HttpServices {
                management,
                diagnostics,
                metrics,
            },
            http_authorization,
            lifecycle.clone(),
        );
        let task = tokio::spawn(transport::serve(
            listener,
            tls,
            app,
            shutdown_requested,
            lifecycle.cancellation(),
        ));
        Ok(Self {
            lifecycle,
            shutdown: Some(shutdown),
            task: Some(task),
        })
    }

    /// Waits for an unexpected server exit without detaching the task if this
    /// future loses a `select!` race.
    pub(crate) async fn wait(&mut self) -> RunnerHttpServerError {
        match self.join().await {
            Some(result) => RunnerHttpServerError::unexpected_exit(result),
            None => RunnerHttpServerError::Stopped,
        }
    }

    /// Stops admission. The server task then drains every accepted request.
    pub(crate) fn begin_shutdown(&mut self) {
        self.lifecycle.close_admission();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }

    /// Waits until every admitted handler has produced its response.
    ///
    /// Streaming response bodies are not included. Their lifetime ends after
    /// the diagnostics owner closes and Axum completes connection drain.
    pub(crate) async fn wait_for_handlers(&self) {
        self.lifecycle.wait_for_handlers().await;
    }

    /// Joins the server after graceful shutdown has been requested.
    pub(crate) async fn wait_for_shutdown(&mut self) -> Option<RunnerHttpServerError> {
        self.begin_shutdown();
        match self.join().await? {
            Ok(Ok(())) => None,
            Ok(Err(source)) => Some(RunnerHttpServerError::Serve(source)),
            Err(source) => Some(RunnerHttpServerError::Task(source)),
        }
    }

    /// Cancels admitted handlers and joins every connection, including handshakes.
    pub(crate) async fn abort(&mut self) -> Option<RunnerHttpServerError> {
        self.lifecycle.cancel();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        match self.join().await? {
            Ok(Ok(())) => None,
            Ok(Err(source)) => Some(RunnerHttpServerError::Serve(source)),
            Err(source) if source.is_cancelled() => None,
            Err(source) => Some(RunnerHttpServerError::Task(source)),
        }
    }

    async fn join(&mut self) -> Option<Result<io::Result<()>, JoinError>> {
        let result = self.task.as_mut()?.await;
        self.task = None;
        Some(result)
    }
}

impl Drop for RunnerHttpServer {
    fn drop(&mut self) {
        self.lifecycle.cancel();
        self.shutdown.take();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Debug)]
pub(crate) enum RunnerHttpServerError {
    Bind(io::Error),
    Serve(io::Error),
    Task(JoinError),
    Stopped,
}

impl RunnerHttpServerError {
    fn unexpected_exit(result: Result<io::Result<()>, JoinError>) -> Self {
        match result {
            Ok(Ok(())) => Self::Stopped,
            Ok(Err(source)) => Self::Serve(source),
            Err(source) => Self::Task(source),
        }
    }
}

impl fmt::Display for RunnerHttpServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind(_) => formatter.write_str("Runner HTTP listener could not be bound"),
            Self::Serve(_) | Self::Task(_) | Self::Stopped => {
                formatter.write_str("Runner HTTP service stopped unexpectedly")
            }
        }
    }
}

impl Error for RunnerHttpServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Bind(source) | Self::Serve(source) => Some(source),
            Self::Task(source) => Some(source),
            Self::Stopped => None,
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub(crate) fn server(
        shutdown: oneshot::Sender<()>,
        task: JoinHandle<io::Result<()>>,
    ) -> RunnerHttpServer {
        RunnerHttpServer {
            lifecycle: HttpRequestLifecycle::new(),
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    pub(crate) fn server_with_admitted_handler(
        shutdown: oneshot::Sender<()>,
        task: JoinHandle<io::Result<()>>,
    ) -> (RunnerHttpServer, oneshot::Sender<()>) {
        let server = server(shutdown, task);
        let (release, released) = oneshot::channel();
        let handler = server.lifecycle.enter();
        tokio::spawn(async move {
            let _ = released.await;
            drop(handler);
        });
        (server, release)
    }
}

#[cfg(test)]
mod server_tests {
    use super::test_support::server;
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    #[tokio::test(flavor = "current_thread")]
    async fn start_owns_listener_binding_failures() -> io::Result<()> {
        let occupied = TcpListener::bind("127.0.0.1:0").await?;
        let address = occupied.local_addr()?;
        let (management, _management_receiver) = runner::management::test_support::interface();

        let error = RunnerHttpServer::start(
            address,
            None,
            management,
            RunnerDiagnostics::new(),
            runner::metrics::test_support::empty()?,
            Arc::new(runner::extensions::NoHttpAuth),
        )
        .await
        .err()
        .ok_or_else(|| io::Error::other("Occupied HTTP address was accepted"))?;

        assert!(matches!(error, RunnerHttpServerError::Bind(_)));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn graceful_shutdown_signals_and_joins_the_server_task() -> io::Result<()> {
        let (shutdown, shutdown_requested) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = shutdown_requested.await;
            Ok(())
        });
        let mut server = server(shutdown, task);

        server.begin_shutdown();
        let cleanup = server.wait_for_shutdown().await;

        assert!(cleanup.is_none());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abnormal_shutdown_cancels_and_joins_an_admitted_handler() -> io::Result<()> {
        let address = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?;
        let (management, _management_receiver) = runner::management::test_support::interface();
        let mut server = RunnerHttpServer::start(
            address,
            None,
            management,
            RunnerDiagnostics::new(),
            runner::metrics::test_support::empty()?,
            Arc::new(runner::extensions::NoHttpAuth),
        )
        .await
        .map_err(io::Error::other)?;
        let mut connection = tokio::net::TcpStream::connect(address).await?;
        connection
            .write_all(
                format!(
                    "POST /plugins HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/octet-stream\r\nContent-Length: 1\r\nExpect: 100-continue\r\n\r\n"
                )
                .as_bytes(),
            )
            .await?;
        let mut interim = [0_u8; 25];
        tokio::time::timeout(Duration::from_secs(1), connection.read_exact(&mut interim))
            .await
            .map_err(|_| io::Error::other("Plugin handler did not enter its body read"))??;
        assert_eq!(&interim, b"HTTP/1.1 100 Continue\r\n\r\n");

        let cleanup = tokio::time::timeout(Duration::from_secs(1), server.abort())
            .await
            .map_err(|_| io::Error::other("Abnormal HTTP shutdown left a handler detached"))?;

        assert!(cleanup.is_none());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn normal_exit_is_unexpected_while_waiting() -> io::Result<()> {
        let (shutdown, _shutdown_requested) = oneshot::channel();
        let task = tokio::spawn(async { Ok(()) });
        let mut server = server(shutdown, task);

        let error = server.wait().await;

        assert!(matches!(error, RunnerHttpServerError::Stopped));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropping_owner_aborts_its_server_task() -> io::Result<()> {
        let (shutdown, _shutdown_requested) = oneshot::channel();
        let (started, started_receiver) = oneshot::channel();
        let (dropped, dropped_receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _guard = DropSignal(Some(dropped));
            let _ = started.send(());
            std::future::pending::<()>().await;
            Ok(())
        });
        let server = server(shutdown, task);
        let _ = started_receiver.await;

        drop(server);

        tokio::time::timeout(Duration::from_secs(1), dropped_receiver)
            .await
            .map_err(|_| io::Error::other("Dropped HTTP server task was not aborted"))?
            .map_err(|_| io::Error::other("HTTP server task dropped without its guard"))?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_wait_keeps_the_task_owned_for_a_later_wait() -> io::Result<()> {
        let (shutdown, _shutdown_requested) = oneshot::channel();
        let (finish, finish_requested) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = finish_requested.await;
            Ok(())
        });
        let mut server = server(shutdown, task);

        {
            let first_wait = server.wait();
            tokio::pin!(first_wait);
            tokio::select! {
                _ = &mut first_wait => {
                    return Err(io::Error::other("Pending HTTP server wait completed early"));
                }
                () = tokio::task::yield_now() => {}
            }
        }
        let _ = finish.send(());

        assert!(matches!(
            server.wait().await,
            RunnerHttpServerError::Stopped
        ));
        Ok(())
    }

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(signal) = self.0.take() {
                let _ = signal.send(());
            }
        }
    }
}
