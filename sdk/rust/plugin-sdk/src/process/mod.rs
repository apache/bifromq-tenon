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

//! Shared process control for Source and Sink programs.
//!
//! Programs own business lifecycle and drive the control connection. Its worker
//! exchanges lifecycle messages and watches parent stdin independently of business
//! callbacks. The first process failure terminates immediately without further cleanup.

#![expect(
    clippy::expect_used,
    reason = "Owned channels, locks and threads cannot disappear without an SDK bug"
)]

mod metrics;
pub(crate) mod startup;

use crate::Error;
use crate::wire::plugin::{self, pipeline_to_plugin, plugin_to_pipeline};
use hyper_util::rt::TokioIo;
use std::fmt;
use std::future::Future;
use std::io;
use std::os::fd::AsFd;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use tokio::io::unix::AsyncFd;
use tokio::net::UnixStream;
use tokio::sync::mpsc as asynchronous;
use tokio::task::{AbortHandle, JoinSet};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;

#[derive(Debug)]
pub(crate) struct ControlConnection {
    commands: asynchronous::Sender<Command>,
    worker: Option<JoinHandle<()>>,
}

impl ControlConnection {
    pub(crate) fn start(
        path: std::path::PathBuf,
        launch_id: Vec<u8>,
        lifecycle: Lifecycle,
        events: mpsc::Sender<Event>,
        failed: FailureBoundary,
    ) -> io::Result<Self> {
        let input = std::io::stdin().as_fd().try_clone_to_owned()?;
        let flags = rustix::fs::fcntl_getfl(&input)?;
        rustix::fs::fcntl_setfl(&input, flags | rustix::fs::OFlags::NONBLOCK)?;
        let (commands, receiver) = asynchronous::channel(1);
        let (attached, ready) = mpsc::sync_channel(0);
        let worker = thread::Builder::new()
            .name("tenon-plugin-control".into())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        failed(error.into());
                        return;
                    }
                };
                let result = runtime.block_on(async {
                    let input = AsyncFd::new(input)?;
                    let executor = TrackedExecutor::default();
                    let mut metrics = JoinSet::new();
                    let stop_metrics = metrics.spawn(metrics::run(path.clone(), launch_id.clone(), executor.clone()));
                    let result = tokio::select! {
                        biased;
                        result = watch_stdin(input) => result,
                        result = control_session(
                            path, launch_id, lifecycle, receiver, events, attached, executor.clone(), stop_metrics
                        ) => result,
                    };
                    metrics.shutdown().await;
                    executor.stop().await;
                    result
                });
                if let Err(error) = result {
                    failed(error);
                }
            })?;
        ready
            .recv()
            .expect("control either attaches or terminates the process");
        Ok(Self {
            commands,
            worker: Some(worker),
        })
    }

    pub(crate) fn publish(&self, message: Publish) {
        let (ack, received) = mpsc::sync_channel(0);
        self.commands
            .blocking_send(Command::Publish(message, ack))
            .expect("control remains alive until final cleanup");
        received
            .recv()
            .expect("control either publishes or terminates the process");
    }

    pub(crate) fn finish(&mut self) {
        if let Some(worker) = self.worker.take() {
            self.commands
                .blocking_send(Command::Finish)
                .expect("control remains alive until final cleanup");
            worker.join().expect("control worker must not panic");
        }
    }
}

/// The control sequence required by the compiled Program entry point.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Lifecycle {
    SourceCapable,
    SinkOnly,
}

#[derive(Debug)]
pub(crate) enum Event {
    Quiesce,
    Shutdown,
}

pub(crate) type FailureBoundary = Arc<dyn Fn(Error) + Send + Sync>;

#[derive(Debug)]
pub(crate) enum Publish {
    Ready,
    Quiesced,
}

pub(crate) fn install_panic_hook() {
    // Exit before unwinding can run business destructors, including on threads
    // created by the business owner rather than by the SDK.
    std::panic::set_hook(Box::new(|panic| {
        fatal(panic);
    }));
}

/// Resolves an operation while its Program still owns every live resource.
pub(crate) fn resolve<T, E: fmt::Display>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|error| fatal(&error))
}

pub(crate) fn fatal(error: &dyn fmt::Display) -> ! {
    eprintln!("Plugin process cannot continue: {error}");
    use std::io::Write;
    let _ = std::io::stderr().flush();
    std::process::exit(1)
}

#[derive(Debug)]
enum Command {
    Publish(Publish, mpsc::SyncSender<()>),
    Finish,
}

#[derive(Clone, Debug, Default)]
struct TrackedExecutor(Arc<Mutex<JoinSet<()>>>);

impl TrackedExecutor {
    async fn stop(self) {
        let mut workers =
            std::mem::take(&mut *self.0.lock().expect("task registry must not panic"));
        workers.abort_all();
        while workers.join_next().await.is_some() {}
    }
}

impl<F> hyper::rt::Executor<F> for TrackedExecutor
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, future: F) {
        let mut workers = self.0.lock().expect("task registry must not panic");
        // Reconnecting transports must not retain completed task records until shutdown.
        while workers.try_join_next().is_some() {}
        workers.spawn(future);
    }
}

#[derive(Debug)]
enum Phase {
    Attached,
    Ready,
    Quiescing,
    Quiesced,
    Closing,
}

#[expect(
    clippy::too_many_arguments,
    reason = "one worker owns control, metrics cancellation, and lifecycle publication"
)]
async fn control_session(
    path: std::path::PathBuf,
    launch_id: Vec<u8>,
    lifecycle: Lifecycle,
    mut commands: asynchronous::Receiver<Command>,
    events: mpsc::Sender<Event>,
    attached: mpsc::SyncSender<()>,
    executor: TrackedExecutor,
    stop_metrics: AbortHandle,
) -> Result<(), Error> {
    let channel = Endpoint::from_static("http://[::]:50051")
        .executor(executor)
        .connect_with_connector(tower::service_fn(move |_| {
            let path = path.clone();
            async move { UnixStream::connect(path).await.map(TokioIo::new) }
        }))
        .await?;
    let (outgoing, receiver) = asynchronous::channel(1);
    outgoing
        .send(plugin::PluginToPipeline {
            message: Some(plugin_to_pipeline::Message::Attach(plugin::Attach {
                launch_id,
            })),
        })
        .await?;
    let mut client = plugin::plugin_lifecycle_client::PluginLifecycleClient::new(channel);
    let mut incoming = client
        .run(ReceiverStream::new(receiver))
        .await?
        .into_inner();
    attached.send(()).expect("startup caller must remain alive");
    let mut phase = Phase::Attached;
    loop {
        tokio::select! {
            command = incoming.message() => {
                match (command?.and_then(|command| command.message), &phase) {
                    (Some(pipeline_to_plugin::Message::QuiesceSource(_)), Phase::Ready) if lifecycle == Lifecycle::SourceCapable => {
                        phase = Phase::Quiescing;
                        events.send(Event::Quiesce).expect("business owner must remain alive");
                    }
                    (Some(pipeline_to_plugin::Message::Shutdown(_)), Phase::Ready) if lifecycle == Lifecycle::SinkOnly => {
                        phase = Phase::Closing;
                        stop_metrics.abort();
                        events.send(Event::Shutdown).expect("business owner must remain alive");
                    }
                    (Some(pipeline_to_plugin::Message::Shutdown(_)), Phase::Quiesced) => {
                        phase = Phase::Closing;
                        stop_metrics.abort();
                        events.send(Event::Shutdown).expect("business owner must remain alive");
                    }
                    _ => return Err("Unexpected Plugin lifecycle command or stream termination".into()),
                }
            }
            command = commands.recv() => {
                match command.expect("control caller owns its command channel") {
                    Command::Publish(publish, ack) => {
                        let message = match (publish, &phase) {
                            (Publish::Ready, Phase::Attached) => {
                                phase = Phase::Ready;
                                plugin_to_pipeline::Message::Ready(plugin::Ready {})
                            }
                            (Publish::Quiesced, Phase::Quiescing) => {
                                phase = Phase::Quiesced;
                                plugin_to_pipeline::Message::SourceQuiesced(plugin::SourceQuiesced {})
                            }
                            _ => unreachable!("business owner publishes each phase exactly once"),
                        };
                        outgoing.send(plugin::PluginToPipeline { message: Some(message) }).await?;
                        ack.send(()).expect("publisher waits for this acknowledgment");
                    }
                    Command::Finish => {
                        assert!(matches!(phase, Phase::Closing), "normal cleanup follows Shutdown");
                        drop(outgoing);
                        if incoming.message().await?.is_some() { return Err("Lifecycle command arrived after final cleanup".into()) }
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn watch_stdin(input: AsyncFd<std::os::fd::OwnedFd>) -> Result<(), Error> {
    let mut byte = [0; 1];
    loop {
        let mut ready = input.readable().await?;
        match ready
            .try_io(|input| rustix::io::read(input.get_ref(), &mut byte).map_err(io::Error::from))
        {
            Ok(Ok(0)) => return Err("Plugin parent stdin closed".into()),
            Ok(Ok(_)) | Err(_) => {}
            Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => {}
            Ok(Err(error)) => return Err(error.into()),
        }
    }
}
