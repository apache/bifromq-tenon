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

use base64::Engine as _;
use prost::Message;
use prost_types::{FieldDescriptorProto, FileDescriptorSet};
use rustix::process::{Pid, Signal, kill_process};
use sha2::{Digest as _, Sha256};
use std::collections::VecDeque;
use std::io::{self, Write as _};
use std::os::unix::fs::{FileExt as _, MetadataExt as _, PermissionsExt as _};
use std::os::unix::net::UnixListener as StdUnixListener;
use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tenon::runner_test_support::contracts::core::pipeline_control_server::{
    PipelineControl, PipelineControlServer,
};
use tenon::runner_test_support::contracts::core::{
    LuaLimits, PipelineBootstrap, PipelineEnvironment, PipelineRevisionPlan,
    PipelineStatusSnapshot, PipelineToRunner, PluginInstanceState, PluginInterface,
    PluginProgramRuntime, RetryBackoff, RunnerToPipeline, pipeline_to_runner, runner_to_pipeline,
};
use tenon::runner_test_support::contracts::sink::EgressRecord;
use tenon::runner_test_support::contracts::source::{
    IngressCompletion, IngressCompletionStatus, IngressRecord,
};
use tenon::runner_test_support::egress_queue_path;
use tenon::runner_test_support::{flow_channel_bell_path, loops_bell_path};
use tenon_ipc::queue::contract_test_support::{open_reader, open_writer};
use tenon_ipc::queue::{QueueReader, QueueWriter, ReadOutcome, WriteOutcome};
use tokio::net::UnixListener;
use tokio::sync::oneshot;
use tokio_stream::Stream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status, Streaming};

#[path = "runner_cli/plugin_fixture.rs"]
mod plugin_fixture;
const LAUNCH_ID_BASE64: &str = "bGF1bmNoLWE=";
const IPC_QUEUE_RELEASE_POSITION_OFFSET: u64 = 128;
const STATEFUL_KEEP_RUNTIME_LUA: &str = "local builder = registry:getBuilder(\"com.example.kafka@1.0.0\")\nlocal count = 0\nfunction main(event)\n  count = count + 1\n  if count == 1 then\n    emit(builder:build())\n  else\n    emit()\n  end\nend";
const ALWAYS_EMIT_KAFKA_LUA: &str = "local builder = registry:getBuilder(\"com.example.kafka@1.0.0\")\nfunction main(event)\n  emit(builder:build())\nend";

#[derive(Clone, Copy, Debug)]
enum Scenario {
    PublishInitialStatus,
    PlannedShutdownStalledByQuiesce,
    SourceStartFailure,
    SourceConfigWriteStalled,
    RunnerLossWithPluginDescendant,
    LuaStartupStalled,
    WrongPhaseWhileLuaStartupStalled,
    EquivalentRevisionAfterBootstrap,
    SourceConfigRevisionAfterBootstrap,
    SourceAndSinkConfigsRevisionAfterBootstrap,
    SinkConfigsRevisionAfterBootstrap,
    SinkConfigRevisionStartFailure,
    SinkConfigRevisionTerminationStalled,
    SinkConfigRevisionWorkerFailure,
    LatestPendingRevision,
    LatestPendingRevisionAdmission,
    LuaReplacementAfterBootstrap,
    LuaReplacementWithStartFailedSinks,
    LuaAndSinkConfigRevisionAfterBootstrap,
    LuaPreparationFailureWithSourceConfig,
    UnparseableRevisionAfterBootstrap,
    SourceQueueDeliveryRevisionAfterBootstrap,
    SourceProgramContractRevisionAfterBootstrap,
    BootstrapMissingEnvironment,
    DuplicateBootstrap,
    RevisionBeforeBootstrap,
    DisconnectBeforeBootstrap,
    ControlStreamLossAfterBootstrap,
    KeepStreamOpenAfterBootstrap,
}

struct FakeRunnerStream {
    outbound: VecDeque<RunnerToPipeline>,
    deferred_revision: Option<RunnerToPipeline>,
    deferred_pending_revisions: VecDeque<RunnerToPipeline>,
    revision_gate: Option<oneshot::Receiver<()>>,
    pending_revisions_gate: Option<oneshot::Receiver<()>>,
    pending_revisions_remaining: usize,
    pending_revisions_sent_sender: SyncSender<()>,
    inbound: Streaming<PipelineToRunner>,
    status_sender: mpsc::Sender<PipelineToRunner>,
    control_stream_closed_sender: SyncSender<()>,
    lifetime: FakeStreamLifetime,
}

impl Drop for FakeRunnerStream {
    fn drop(&mut self) {
        let _ = self.control_stream_closed_sender.try_send(());
    }
}

#[derive(Clone, Copy)]
enum FakeStreamLifetime {
    CloseAfterMessages,
    CloseAfterStatus,
    UntilClientCloses,
}

impl Stream for FakeRunnerStream {
    type Item = Result<RunnerToPipeline, Status>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let stream = self.get_mut();
        if let Some(message) = take_outbound_message(stream) {
            return Poll::Ready(Some(Ok(message)));
        }
        if let Some(revision_gate) = stream.revision_gate.as_mut() {
            match std::future::Future::poll(Pin::new(revision_gate), context) {
                Poll::Ready(Ok(())) => {
                    stream.revision_gate = None;
                    return match stream.deferred_revision.take() {
                        Some(message) => Poll::Ready(Some(Ok(message))),
                        None => Poll::Ready(Some(Err(Status::internal(
                            "Revision gate has no deferred revision",
                        )))),
                    };
                }
                Poll::Ready(Err(_)) => {
                    stream.revision_gate = None;
                    stream.deferred_revision = None;
                    return Poll::Ready(Some(Err(Status::cancelled(
                        "Revision gate closed before release",
                    ))));
                }
                Poll::Pending => {}
            }
        }
        if let Some(pending_revisions_gate) = stream.pending_revisions_gate.as_mut() {
            match std::future::Future::poll(Pin::new(pending_revisions_gate), context) {
                Poll::Ready(Ok(())) => {
                    stream.pending_revisions_gate = None;
                    stream
                        .outbound
                        .append(&mut stream.deferred_pending_revisions);
                    stream.pending_revisions_remaining = stream.outbound.len();
                    return match take_outbound_message(stream) {
                        Some(message) => Poll::Ready(Some(Ok(message))),
                        None => Poll::Ready(Some(Err(Status::internal(
                            "Pending revision gate has no deferred revisions",
                        )))),
                    };
                }
                Poll::Ready(Err(_)) => {
                    stream.pending_revisions_gate = None;
                    stream.deferred_pending_revisions.clear();
                    return Poll::Ready(Some(Err(Status::cancelled(
                        "Pending revision gate closed before release",
                    ))));
                }
                Poll::Pending => {}
            }
        }
        if matches!(stream.lifetime, FakeStreamLifetime::CloseAfterMessages) {
            return Poll::Ready(None);
        }

        loop {
            match Pin::new(&mut stream.inbound).poll_next(context) {
                Poll::Ready(Some(Ok(message))) => {
                    let _ = stream.status_sender.send(message);
                    if matches!(stream.lifetime, FakeStreamLifetime::CloseAfterStatus) {
                        return Poll::Ready(None);
                    }
                }
                Poll::Ready(Some(Err(status))) => return Poll::Ready(Some(Err(status))),
                Poll::Ready(None) => {
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn take_outbound_message(stream: &mut FakeRunnerStream) -> Option<RunnerToPipeline> {
    let message = stream.outbound.pop_front()?;
    if stream.pending_revisions_remaining > 0 {
        stream.pending_revisions_remaining -= 1;
        if stream.pending_revisions_remaining == 0 {
            let _ = stream.pending_revisions_sent_sender.try_send(());
        }
    }
    Some(message)
}

#[derive(Clone, Debug)]
struct FakeRunnerService {
    scenario: Scenario,
    attach_sender: SyncSender<PipelineToRunner>,
    status_sender: mpsc::Sender<PipelineToRunner>,
    fixture: StartupFixture,
    revision_gate: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    pending_revisions_gate: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    pending_revisions_sent_sender: SyncSender<()>,
    control_stream_closed_sender: SyncSender<()>,
}

#[tonic::async_trait]
impl PipelineControl for FakeRunnerService {
    type RunStream = FakeRunnerStream;

    async fn run(
        &self,
        request: Request<Streaming<PipelineToRunner>>,
    ) -> Result<Response<Self::RunStream>, Status> {
        let mut inbound = request.into_inner();
        let attach = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("Pipeline Attach is missing"))?;
        self.attach_sender
            .try_send(attach)
            .map_err(|_| Status::already_exists("Pipeline opened more than one control stream"))?;
        let mut messages = scenario_messages(self.scenario, &self.fixture).map_err(|error| {
            Status::internal(format!("Fake Runner could not load test vectors: {error}"))
        })?;
        let defers_revision = matches!(
            self.scenario,
            Scenario::EquivalentRevisionAfterBootstrap
                | Scenario::WrongPhaseWhileLuaStartupStalled
                | Scenario::SourceConfigRevisionAfterBootstrap
                | Scenario::SourceAndSinkConfigsRevisionAfterBootstrap
                | Scenario::SinkConfigsRevisionAfterBootstrap
                | Scenario::SinkConfigRevisionStartFailure
                | Scenario::SinkConfigRevisionTerminationStalled
                | Scenario::SinkConfigRevisionWorkerFailure
                | Scenario::LatestPendingRevision
                | Scenario::LatestPendingRevisionAdmission
                | Scenario::LuaReplacementAfterBootstrap
                | Scenario::LuaReplacementWithStartFailedSinks
                | Scenario::LuaAndSinkConfigRevisionAfterBootstrap
                | Scenario::LuaPreparationFailureWithSourceConfig
                | Scenario::UnparseableRevisionAfterBootstrap
                | Scenario::SourceQueueDeliveryRevisionAfterBootstrap
                | Scenario::SourceProgramContractRevisionAfterBootstrap
        );
        let (deferred_revision, deferred_pending_revisions, revision_gate) = if defers_revision {
            let mut deferred_revisions = messages.split_off(1);
            if deferred_revisions.is_empty() {
                return Err(Status::internal("Fake Runner revision is missing"));
            }
            let deferred_revision = deferred_revisions.remove(0);
            let revision_gate = self
                .revision_gate
                .lock()
                .map_err(|_| Status::internal("Revision gate lock is poisoned"))?
                .take()
                .ok_or_else(|| Status::already_exists("Revision gate was already consumed"))?;
            (
                Some(deferred_revision),
                deferred_revisions.into(),
                Some(revision_gate),
            )
        } else {
            (None, VecDeque::new(), None)
        };
        let pending_revisions_gate = if deferred_pending_revisions.is_empty() {
            None
        } else {
            Some(
                self.pending_revisions_gate
                    .lock()
                    .map_err(|_| Status::internal("Pending revision gate lock is poisoned"))?
                    .take()
                    .ok_or_else(|| {
                        Status::already_exists("Pending revision gate was already consumed")
                    })?,
            )
        };
        let lifetime = match self.scenario {
            Scenario::PublishInitialStatus
            | Scenario::PlannedShutdownStalledByQuiesce
            | Scenario::SourceStartFailure
            | Scenario::SourceConfigWriteStalled
            | Scenario::RunnerLossWithPluginDescendant
            | Scenario::LuaStartupStalled
            | Scenario::WrongPhaseWhileLuaStartupStalled
            | Scenario::KeepStreamOpenAfterBootstrap => FakeStreamLifetime::UntilClientCloses,
            Scenario::DuplicateBootstrap | Scenario::ControlStreamLossAfterBootstrap => {
                FakeStreamLifetime::CloseAfterStatus
            }
            Scenario::EquivalentRevisionAfterBootstrap
            | Scenario::SourceConfigRevisionAfterBootstrap
            | Scenario::SourceAndSinkConfigsRevisionAfterBootstrap
            | Scenario::SinkConfigsRevisionAfterBootstrap
            | Scenario::SinkConfigRevisionStartFailure
            | Scenario::SinkConfigRevisionTerminationStalled
            | Scenario::SinkConfigRevisionWorkerFailure
            | Scenario::LatestPendingRevision
            | Scenario::LatestPendingRevisionAdmission
            | Scenario::LuaReplacementAfterBootstrap
            | Scenario::LuaReplacementWithStartFailedSinks
            | Scenario::LuaAndSinkConfigRevisionAfterBootstrap
            | Scenario::LuaPreparationFailureWithSourceConfig
            | Scenario::UnparseableRevisionAfterBootstrap
            | Scenario::SourceQueueDeliveryRevisionAfterBootstrap
            | Scenario::SourceProgramContractRevisionAfterBootstrap => {
                FakeStreamLifetime::UntilClientCloses
            }
            Scenario::BootstrapMissingEnvironment
            | Scenario::RevisionBeforeBootstrap
            | Scenario::DisconnectBeforeBootstrap => FakeStreamLifetime::CloseAfterMessages,
        };
        Ok(Response::new(FakeRunnerStream {
            outbound: messages.into(),
            deferred_revision,
            deferred_pending_revisions,
            revision_gate,
            pending_revisions_gate,
            pending_revisions_remaining: 0,
            pending_revisions_sent_sender: self.pending_revisions_sent_sender.clone(),
            inbound,
            status_sender: self.status_sender.clone(),
            control_stream_closed_sender: self.control_stream_closed_sender.clone(),
            lifetime,
        }))
    }
}

#[derive(Debug)]
struct FakeRunner {
    _temporary_directory: TempDir,
    socket_path: String,
    attach_receiver: Receiver<PipelineToRunner>,
    status_receiver: Receiver<PipelineToRunner>,
    fixture: StartupFixture,
    revision_gate_sender: Option<oneshot::Sender<()>>,
    pending_revisions_gate_sender: Option<oneshot::Sender<()>>,
    pending_revisions_sent_receiver: Receiver<()>,
    control_stream_closed_receiver: Receiver<()>,
    shutdown_sender: Option<oneshot::Sender<()>>,
    server_thread: Option<thread::JoinHandle<io::Result<()>>>,
}

impl FakeRunner {
    fn start(scenario: Scenario) -> io::Result<Self> {
        let temporary_directory = tempfile::tempdir()?;
        let socket_path = temporary_directory.path().join("control.sock");
        let plugin_control = temporary_directory
            .path()
            .join(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"launch-a"));
        std::fs::create_dir(&plugin_control)?;
        std::fs::set_permissions(&plugin_control, std::fs::Permissions::from_mode(0o700))?;
        let socket_path_string = socket_path
            .to_str()
            .ok_or_else(|| io::Error::other("Fake Runner socket path is not UTF-8"))?
            .to_owned();
        let (attach_sender, attach_receiver) = mpsc::sync_channel(1);
        let (status_sender, status_receiver) = mpsc::channel();
        let (revision_gate_sender, revision_gate_receiver) = oneshot::channel();
        let revision_gate = Arc::new(Mutex::new(Some(revision_gate_receiver)));
        let (pending_revisions_gate_sender, pending_revisions_gate_receiver) = oneshot::channel();
        let pending_revisions_gate = Arc::new(Mutex::new(Some(pending_revisions_gate_receiver)));
        let (pending_revisions_sent_sender, pending_revisions_sent_receiver) =
            mpsc::sync_channel(1);
        let (control_stream_closed_sender, control_stream_closed_receiver) = mpsc::sync_channel(1);
        let fixture = StartupFixture::create(temporary_directory.path())?;
        let service_fixture = fixture.clone();
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server_socket_path = socket_path_string.clone();
        let server_thread = thread::Builder::new()
            .name(String::from("fake-runner-grpc"))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()?;
                runtime.block_on(async move {
                    let listener = match UnixListener::bind(&server_socket_path) {
                        Ok(listener) => listener,
                        Err(error) => {
                            let _ = ready_sender.send(Err(error));
                            return Ok(());
                        }
                    };
                    ready_sender.send(Ok(())).map_err(io::Error::other)?;
                    tonic::transport::Server::builder()
                        .add_service(
                            PipelineControlServer::new(FakeRunnerService {
                                scenario,
                                attach_sender,
                                status_sender,
                                fixture: service_fixture,
                                revision_gate,
                                pending_revisions_gate,
                                pending_revisions_sent_sender,
                                control_stream_closed_sender,
                            })
                            .max_decoding_message_size(usize::MAX),
                        )
                        .serve_with_incoming_shutdown(
                            UnixListenerStream::new(listener),
                            async move {
                                let _ = shutdown_receiver.await;
                            },
                        )
                        .await
                        .map_err(io::Error::other)
                })
            })?;
        ready_receiver
            .recv()
            .map_err(io::Error::other)?
            .map_err(io::Error::other)?;

        Ok(Self {
            _temporary_directory: temporary_directory,
            socket_path: socket_path_string,
            attach_receiver,
            status_receiver,
            fixture,
            revision_gate_sender: Some(revision_gate_sender),
            pending_revisions_gate_sender: Some(pending_revisions_gate_sender),
            pending_revisions_sent_receiver,
            control_stream_closed_receiver,
            shutdown_sender: Some(shutdown_sender),
            server_thread: Some(server_thread),
        })
    }

    fn run_pipeline(&self) -> io::Result<Output> {
        self.spawn_pipeline()?
            .wait_with_timeout(Duration::from_secs(5))
    }

    fn spawn_pipeline(&self) -> io::Result<TestPipelineProcess> {
        TestPipelineProcess::spawn(&self.socket_path)
    }

    fn receive_attach(&self) -> io::Result<PipelineToRunner> {
        self.attach_receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)
    }

    fn receive_status(&self) -> io::Result<PipelineToRunner> {
        self.status_receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)
    }

    fn receive_status_for_etag(
        &self,
        expected_etag: &str,
    ) -> io::Result<tenon::runner_test_support::contracts::core::PipelineStatusSnapshot> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let envelope = self
                .status_receiver
                .recv_timeout(remaining)
                .map_err(io::Error::other)?;
            let status = status_snapshot(envelope)?;
            if status.document_etag == expected_etag {
                return Ok(status);
            }
        }
    }

    fn receive_status_matching(
        &self,
        expected_etag: &str,
        predicate: impl Fn(&tenon::runner_test_support::contracts::core::PipelineStatusSnapshot) -> bool,
    ) -> io::Result<tenon::runner_test_support::contracts::core::PipelineStatusSnapshot> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let envelope = self
                .status_receiver
                .recv_timeout(remaining)
                .map_err(io::Error::other)?;
            let status = status_snapshot(envelope)?;
            if status.document_etag == expected_etag && predicate(&status) {
                return Ok(status);
            }
        }
    }

    fn release_revision(&mut self) -> io::Result<()> {
        self.revision_gate_sender
            .take()
            .ok_or_else(|| io::Error::other("Revision gate was already released"))?
            .send(())
            .map_err(|_| io::Error::other("Revision gate receiver is unavailable"))
    }

    fn release_pending_revisions(&mut self) -> io::Result<()> {
        self.pending_revisions_gate_sender
            .take()
            .ok_or_else(|| io::Error::other("Pending revision gate was already released"))?
            .send(())
            .map_err(|_| io::Error::other("Pending revision gate receiver is unavailable"))
    }

    fn wait_until_pending_revisions_are_sent(&self) -> io::Result<()> {
        self.pending_revisions_sent_receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)
    }

    fn wait_until_control_stream_closes(&self) -> io::Result<()> {
        self.control_stream_closed_receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)
    }

    fn assert_no_status_for_etag(&self, forbidden_etag: &str) -> io::Result<()> {
        loop {
            match self.status_receiver.try_recv() {
                Err(mpsc::TryRecvError::Empty | mpsc::TryRecvError::Disconnected) => return Ok(()),
                Ok(message) => {
                    let status = status_snapshot(message)?;
                    if status.document_etag == forbidden_etag {
                        return Err(io::Error::other(format!(
                            "Pipeline unexpectedly published revision: {forbidden_etag}"
                        )));
                    }
                }
            }
        }
    }

    fn finish(mut self) -> io::Result<()> {
        self.stop()
    }

    fn stop(&mut self) -> io::Result<()> {
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }
        let Some(server_thread) = self.server_thread.take() else {
            return Ok(());
        };
        server_thread
            .join()
            .map_err(|_| io::Error::other("Fake Runner server thread panicked"))?
    }
}

#[derive(Clone, Debug)]
struct StartupFixture {
    pipeline_working_directory: String,
    plugin_directory: String,
}

impl StartupFixture {
    fn create(root: &std::path::Path) -> io::Result<Self> {
        let plugin_directory = root.join("plugin");
        std::fs::create_dir(&plugin_directory)?;
        let plugin_path = plugin_directory.join("plugin.sh");
        std::fs::write(&plugin_path, "#!/bin/sh\nexec \"$@\"\n")?;
        std::fs::set_permissions(&plugin_path, std::fs::Permissions::from_mode(0o500))?;
        std::fs::write(
            plugin_directory.join("stall.sh"),
            "#!/bin/sh\n[ \"$1\" = \"--sdk-config\" ] || exit 21\nworking_directory=\"${2#*workingDirectory\\\":\\\"}\"\nworking_directory=\"${working_directory%%\\\"*}\"\nprintf '%s' \"$$\" > \"$working_directory/stall.pid\"\nexec sleep 30\n",
        )?;
        let pipeline_working_directory = root.join("pipeline-runtime");
        Ok(Self {
            pipeline_working_directory: path_text(&pipeline_working_directory)?,
            plugin_directory: path_text(&std::fs::canonicalize(plugin_directory)?)?,
        })
    }
}

fn path_text(path: &std::path::Path) -> io::Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| io::Error::other("Test path is not UTF-8"))
}

impl Drop for FakeRunner {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[derive(Debug)]
struct StalledControlPeer {
    _temporary_directory: TempDir,
    socket_path: String,
    accepted_receiver: Receiver<()>,
    shutdown_sender: Option<oneshot::Sender<()>>,
    server_thread: Option<thread::JoinHandle<io::Result<()>>>,
}

impl StalledControlPeer {
    fn start() -> io::Result<Self> {
        let temporary_directory = tempfile::tempdir()?;
        let socket_path = temporary_directory.path().join("stalled-control.sock");
        let socket_path = socket_path
            .to_str()
            .ok_or_else(|| io::Error::other("Stalled control socket path is not UTF-8"))?
            .to_owned();
        let listener = StdUnixListener::bind(&socket_path)?;
        listener.set_nonblocking(true)?;
        let (accepted_sender, accepted_receiver) = mpsc::sync_channel(1);
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        let server_thread = thread::Builder::new()
            .name(String::from("stalled-control-peer"))
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .build()?;
                runtime.block_on(async move {
                    let listener = UnixListener::from_std(listener)?;
                    let (_stream, _) = tokio::select! {
                        biased;
                        _ = &mut shutdown_receiver => return Ok(()),
                        accepted = listener.accept() => accepted?,
                    };
                    accepted_sender.send(()).map_err(io::Error::other)?;
                    let _ = shutdown_receiver.await;
                    Ok(())
                })
            })?;

        Ok(Self {
            _temporary_directory: temporary_directory,
            socket_path,
            accepted_receiver,
            shutdown_sender: Some(shutdown_sender),
            server_thread: Some(server_thread),
        })
    }

    fn wait_until_connected(&self) -> io::Result<()> {
        self.accepted_receiver
            .recv_timeout(Duration::from_secs(5))
            .map_err(io::Error::other)
    }

    fn finish(mut self) -> io::Result<()> {
        self.stop()
    }

    fn stop(&mut self) -> io::Result<()> {
        if let Some(sender) = self.shutdown_sender.take() {
            let _ = sender.send(());
        }
        let Some(server_thread) = self.server_thread.take() else {
            return Ok(());
        };
        server_thread
            .join()
            .map_err(|_| io::Error::other("Stalled control peer thread panicked"))?
    }
}

impl Drop for StalledControlPeer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

#[derive(Debug)]
struct TestPipelineProcess {
    child: Option<Child>,
}

impl TestPipelineProcess {
    fn spawn(control_socket: &str) -> io::Result<Self> {
        let child = pipeline_command(control_socket).spawn()?;
        Ok(Self { child: Some(child) })
    }

    fn spawn_inherited_group(control_socket: &str) -> io::Result<Self> {
        let child = pipeline_command_without_private_group(control_socket).spawn()?;
        Ok(Self { child: Some(child) })
    }

    fn close_lifetime_channel(&mut self) -> io::Result<()> {
        drop(self.take_lifetime_channel()?);
        Ok(())
    }

    fn request_termination(&mut self) -> io::Result<()> {
        let process_id = self
            .child_mut()?
            .id()
            .try_into()
            .ok()
            .and_then(Pid::from_raw)
            .ok_or_else(|| io::Error::other("Pipeline process id is unavailable"))?;
        kill_process(process_id, Signal::TERM).map_err(io::Error::from)
    }

    fn take_lifetime_channel(&mut self) -> io::Result<ChildStdin> {
        self.child_mut()?
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("Pipeline lifetime channel is unavailable"))
    }

    fn wait_with_timeout(mut self, timeout: Duration) -> io::Result<Output> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.child_mut()?.try_wait() {
                Ok(Some(_)) => {
                    return self
                        .child
                        .take()
                        .ok_or_else(|| io::Error::other("Pipeline process owner is unavailable"))?
                        .wait_with_output();
                }
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(10));
                }
                Ok(None) => {
                    terminate_and_reap(self.child_mut()?).map_err(|error| {
                        io::Error::new(
                            error.kind(),
                            format!(
                                "Pipeline process exceeded the test deadline and cleanup failed: {error}"
                            ),
                        )
                    })?;
                    self.child = None;
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Pipeline process did not exit before the test deadline",
                    ));
                }
                Err(error) => {
                    return match terminate_and_reap(self.child_mut()?) {
                        Ok(()) => {
                            self.child = None;
                            Err(error)
                        }
                        Err(cleanup_error) => Err(io::Error::new(
                            cleanup_error.kind(),
                            format!(
                                "Pipeline process status failed: {error}; cleanup failed: {cleanup_error}"
                            ),
                        )),
                    };
                }
            }
        }
    }

    fn child_mut(&mut self) -> io::Result<&mut Child> {
        self.child
            .as_mut()
            .ok_or_else(|| io::Error::other("Pipeline process owner is unavailable"))
    }
}

impl Drop for TestPipelineProcess {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = terminate_and_reap(child);
        }
    }
}

fn pipeline_command(control_socket: &str) -> Command {
    let mut command = pipeline_command_without_private_group(control_socket);
    command.process_group(0);
    command
}

fn pipeline_command_without_private_group(control_socket: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tenon"));
    command.args([
        "pipeline",
        "--control-socket",
        control_socket,
        "--launch-id",
        LAUNCH_ID_BASE64,
    ]);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

#[test]
fn pipeline_publishes_the_first_applied_status() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::PublishInitialStatus)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let status = runner.receive_status()?;
    let Some(pipeline_to_runner::Message::StatusSnapshot(status)) = status.message else {
        return Err(io::Error::other(
            "Pipeline did not publish a status snapshot",
        ));
    };
    assert_eq!(status.document_etag, "document-etag-a");
    let source = status
        .plugin_instances
        .iter()
        .find(|instance| instance.id == "source")
        .cloned()
        .ok_or_else(|| io::Error::other("Pipeline status Source is missing"))?;
    assert_eq!(source.state, PluginInstanceState::Starting as i32);
    assert!(source.last_error.is_none());
    assert_eq!(status.plugin_instances.len(), 4);
    assert_eq!(status.plugin_instances[0].id, "iotdb/secondary");
    assert_eq!(
        status.plugin_instances[0].state,
        PluginInstanceState::Starting as i32
    );
    assert_eq!(status.plugin_instances[1].id, "kafka-primary");
    assert_eq!(
        status.plugin_instances[1].state,
        PluginInstanceState::Starting as i32
    );
    assert_eq!(status.plugin_instances[2].id, "kafka-standby");
    assert_eq!(
        status.plugin_instances[2].state,
        PluginInstanceState::Starting as i32
    );
    let pipeline_directory = std::path::Path::new(&runner.fixture.pipeline_working_directory);
    assert_eq!(directory_mode(pipeline_directory)?, 0o700);
    assert!(
        instance_directory(pipeline_directory, "source")
            .join("source/submission-0.queue")
            .is_file()
    );
    assert!(
        instance_directory(pipeline_directory, "source")
            .join("source/completion-1.queue")
            .is_file()
    );
    let source_directory = instance_directory(pipeline_directory, "source");
    assert_eq!(directory_mode(&source_directory)?, 0o700);
    assert_eq!(
        read_text_with_timeout(&source_directory.join("config.received"))?,
        r#"{"port":502}"#
    );
    assert_eq!(
        read_text_with_timeout(&source_directory.join("program.received"))?.trim(),
        runner.fixture.plugin_directory
    );
    let sinks_directory = pipeline_directory.join("instances");
    assert_eq!(directory_mode(&sinks_directory)?, 0o700);
    let sink_directories = plugin_working_directories(pipeline_directory)?
        .into_iter()
        .filter(|directory| egress_queue_path(directory, "main", 0).is_file())
        .collect::<Vec<_>>();
    assert_eq!(sink_directories.len(), 3);
    assert!(
        sink_directories
            .iter()
            .all(|entry| (0..2).all(|channel| egress_queue_path(entry, "main", channel).is_file()))
    );
    assert!(
        sink_directories
            .iter()
            .map(|entry| directory_mode(entry))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .all(|mode| mode == 0o700)
    );
    let mut sink_configs = sink_directories
        .iter()
        .map(|entry| read_text_with_timeout(&entry.join("config.received")))
        .collect::<Result<Vec<_>, _>>()?;
    sink_configs.sort_unstable();
    assert_eq!(
        sink_configs,
        [
            r#"{"cluster":"primary"}"#,
            r#"{"cluster":"standby"}"#,
            r#"{"database":"factory"}"#,
        ]
    );

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn pipeline_reports_a_source_spawn_failure_on_the_applied_revision() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::SourceStartFailure)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let status = runner.receive_status()?;
    let Some(pipeline_to_runner::Message::StatusSnapshot(status)) = status.message else {
        return Err(io::Error::other(
            "Pipeline did not publish a status snapshot",
        ));
    };
    assert_eq!(status.document_etag, "document-etag-a");
    let source = status
        .plugin_instances
        .iter()
        .find(|instance| instance.id == "source")
        .cloned()
        .ok_or_else(|| io::Error::other("Pipeline status Source is missing"))?;
    assert_eq!(source.state, PluginInstanceState::StartFailed as i32);
    assert_eq!(
        source
            .last_error
            .ok_or_else(|| io::Error::other("Source start failure has no error"))?
            .code,
        "plugin.spawn_failed"
    );
    assert!(
        instance_directory(
            Path::new(&runner.fixture.pipeline_working_directory),
            "source"
        )
        .join("source/submission-0.queue")
        .is_file()
    );
    assert!(
        status
            .plugin_instances
            .iter()
            .filter(|instance| instance.id != "source")
            .all(|sink| sink.state == PluginInstanceState::Starting as i32)
    );

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn pipeline_reports_backoff_and_restarts_only_the_source_that_exits_after_ready() -> io::Result<()>
{
    assert_instance_restart("source")
}

#[test]
fn pipeline_reports_backoff_and_restarts_only_the_sink_that_exits_after_ready() -> io::Result<()> {
    assert_instance_restart("kafka-primary")
}

fn assert_instance_restart(id: &str) -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::PublishInitialStatus)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    let root = Path::new(&runner.fixture.pipeline_working_directory);
    let directory = instance_directory(root, id);
    let original_processes = plugin_process_snapshot(root)?;
    // A deleted Queue inode can be reused unless the old file remains open.
    let _source_queue_identity_guard =
        hold_queue_file_identities(&instance_directory(root, "source").join("source"))?;
    let queues = queue_file_snapshot(root)?;
    let pid = process_id_for_directory(&original_processes, &directory)?;
    kill_process(
        Pid::from_raw(i32::try_from(pid).map_err(io::Error::other)?)
            .ok_or_else(|| io::Error::other("Fixture child PID is zero"))?,
        Signal::KILL,
    )?;
    let backoff = runner.receive_status_matching("document-etag-a", |status| {
        status.plugin_instances.iter().any(|instance| {
            instance.id == id && instance.state == PluginInstanceState::RestartBackoff as i32
        })
    })?;
    assert!(
        backoff
            .plugin_instances
            .iter()
            .find(|instance| instance.id == id)
            .is_some_and(|instance| instance.last_error.is_some())
    );
    runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    let restarted_processes = plugin_process_snapshot(root)?;
    for (path, original_pid) in &original_processes {
        let current_pid = process_id_for_directory(&restarted_processes, path)?;
        if path == &directory {
            assert_ne!(current_pid, *original_pid);
            wait_until_process_exits(*original_pid)?;
        } else {
            assert_eq!(current_pid, *original_pid);
        }
    }
    let restarted_queues = queue_file_snapshot(root)?;
    assert_eq!(restarted_queues.len(), queues.len());
    for ((path, inode), (old_path, old_inode)) in restarted_queues.iter().zip(&queues) {
        assert_eq!(path, old_path);
        if path.starts_with(directory.join("source")) {
            assert_ne!(inode, old_inode);
        } else {
            assert_eq!(inode, old_inode);
        }
    }
    assert_eq!(
        read_text_with_timeout(&directory.join("starts.received"))?,
        "started\nstarted\n"
    );
    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    wait_until_processes_exit(&restarted_processes)?;
    runner.finish()
}

#[test]
fn pipeline_sigterm_performs_one_planned_shutdown() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::PublishInitialStatus)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let _running = runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    let pipeline_directory = Path::new(&runner.fixture.pipeline_working_directory);
    let plugin_processes = plugin_process_snapshot(pipeline_directory)?;

    child.request_termination()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_eq!(output.status.code(), Some(0));
    assert!(stderr(&output).is_empty());
    assert!(!pipeline_directory.exists());
    wait_until_processes_exit(&plugin_processes)?;
    runner.finish()
}

#[test]
fn pipeline_sigterm_force_stops_a_plugin_that_has_not_reported_ready() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::SourceConfigWriteStalled)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let source_directory = instance_directory(
        Path::new(&runner.fixture.pipeline_working_directory),
        "source",
    );
    let source_process = read_process_id_with_timeout(&source_directory.join("stall.pid"))?;

    child.request_termination()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_eq!(output.status.code(), Some(0));
    assert!(stderr(&output).is_empty());
    wait_until_process_exits(source_process)?;
    runner.finish()
}

#[test]
fn pipeline_force_kills_its_group_when_the_parent_exits_during_a_stalled_planned_shutdown()
-> io::Result<()> {
    let runner = FakeRunner::start(Scenario::PlannedShutdownStalledByQuiesce)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let _running = runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    let pipeline_directory = Path::new(&runner.fixture.pipeline_working_directory);
    let plugin_processes = plugin_process_snapshot(pipeline_directory)?;
    let source_directory = instance_directory(pipeline_directory, "source");

    // SIGTERM begins the planned shutdown; the delay-quiesce source withholds SourceQuiesced,
    // so controller.shutdown_and_wait() is still in flight inside the drain loop.
    child.request_termination()?;
    wait_until_file_exists(&source_directory.join("quiesce-source.received"))?;

    // Parent loss during that in-flight graceful shutdown must escalate to a group SIGKILL
    // rather than block forever waiting for the stalled source to quiesce.
    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    wait_until_processes_exit(&plugin_processes)?;
    runner.finish()
}

#[test]
fn pipeline_observes_parent_eof_while_plugin_config_write_is_stalled() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::SourceConfigWriteStalled)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let source_directory = instance_directory(
        Path::new(&runner.fixture.pipeline_working_directory),
        "source",
    );
    let plugin_process_id = read_process_id_with_timeout(&source_directory.join("stall.pid"))?;

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    wait_until_process_exits(plugin_process_id)?;
    runner.finish()
}

#[test]
fn pipeline_observes_parent_eof_while_lua_startup_is_stalled() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::LuaStartupStalled)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let source_directory = instance_directory(
        Path::new(&runner.fixture.pipeline_working_directory),
        "source",
    );
    wait_until_file_exists(&source_directory.join("source/submission-0.queue"))?;

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn pipeline_panics_without_waiting_for_stalled_lua_on_wrong_message_phase() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::WrongPhaseWhileLuaStartupStalled)?;
    let child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let source_directory = instance_directory(
        Path::new(&runner.fixture.pipeline_working_directory),
        "source",
    );
    wait_until_file_exists(&source_directory.join("source/submission-0.queue"))?;
    runner.release_revision()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert!(!output.status.success());
    assert!(stderr(&output).contains("Runner sends only revisions after Bootstrap"));
    runner.finish()
}

#[test]
fn pipeline_sigterm_stops_cleanly_while_initial_apply_is_stalled() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::LuaStartupStalled)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let source_directory = instance_directory(
        Path::new(&runner.fixture.pipeline_working_directory),
        "source",
    );
    wait_until_file_exists(&source_directory.join("source/submission-0.queue"))?;

    child.request_termination()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_eq!(output.status.code(), Some(0));
    assert!(stderr(&output).is_empty());
    runner.finish()
}

#[test]
fn pipeline_keeps_all_runtime_owners_for_an_equivalent_revision() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::EquivalentRevisionAfterBootstrap)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;

    let initial_status =
        runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    assert_eq!(initial_status.document_etag, "document-etag-a");

    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    assert_processes_alive(&initial_processes)?;
    let initial_queues = queue_file_snapshot(pipeline_directory)?;

    let mut submission_writer =
        source_submission_writer(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut completion_reader =
        source_completion_reader(pipeline_directory, 0).map_err(io::Error::other)?;
    let kafka_primary_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"primary"}"#)?;
    let kafka_standby_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"standby"}"#)?;
    let mut kafka_primary_reader =
        sink_egress_reader(pipeline_directory, &kafka_primary_directory, 0)
            .map_err(io::Error::other)?;
    let mut kafka_standby_reader =
        sink_egress_reader(pipeline_directory, &kafka_standby_directory, 0)
            .map_err(io::Error::other)?;

    submit_ingress(&mut submission_writer, 1)?;
    let primary_record = read_egress_record(&mut kafka_primary_reader)?;
    let standby_record = read_egress_record(&mut kafka_standby_reader)?;
    assert_eq!(standby_record, primary_record);

    runner.release_revision()?;
    let revised_status = runner.receive_status_for_etag("document-etag-b")?;
    assert_eq!(revised_status.document_etag, "document-etag-b");
    assert_eq!(
        plugin_process_snapshot(pipeline_directory)?,
        initial_processes
    );
    assert_processes_alive(&initial_processes)?;
    assert_eq!(queue_file_snapshot(pipeline_directory)?, initial_queues);

    kafka_primary_reader.release(1).map_err(io::Error::other)?;
    kafka_standby_reader.release(1).map_err(io::Error::other)?;
    assert_ingress_completion(&mut completion_reader, 1)?;

    submit_ingress(&mut submission_writer, 2)?;
    assert_ingress_completion(&mut completion_reader, 2)?;

    assert_all_plugins_started_once(pipeline_directory)?;

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn pipeline_replaces_only_the_source_when_its_config_changes() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::SourceConfigRevisionAfterBootstrap)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;

    let initial_status =
        runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    assert_eq!(initial_status.document_etag, "document-etag-a");

    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let source_directory = instance_directory(pipeline_directory, "source");
    let kafka_primary_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"primary"}"#)?;
    let kafka_standby_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"standby"}"#)?;
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    let initial_queues = queue_file_snapshot(pipeline_directory)?;
    assert_processes_alive(&initial_processes)?;
    let initial_source_process = process_id_for_directory(&initial_processes, &source_directory)?;

    let mut submission_writer =
        source_submission_writer(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut completion_reader =
        source_completion_reader(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut kafka_primary_reader =
        sink_egress_reader(pipeline_directory, &kafka_primary_directory, 0)
            .map_err(io::Error::other)?;
    let mut kafka_standby_reader =
        sink_egress_reader(pipeline_directory, &kafka_standby_directory, 0)
            .map_err(io::Error::other)?;

    submit_ingress(&mut submission_writer, 1)?;
    let primary_record = read_egress_record(&mut kafka_primary_reader)?;
    assert_eq!(
        read_egress_record(&mut kafka_standby_reader)?,
        primary_record
    );

    runner.release_revision()?;
    wait_until_file_exists(&source_directory.join("source-quiesced.received"))?;
    runner.assert_no_status_for_etag("document-etag-b")?;
    kafka_primary_reader.release(1).map_err(io::Error::other)?;
    kafka_standby_reader.release(1).map_err(io::Error::other)?;
    assert_ingress_completion(&mut completion_reader, 1)?;
    let revised_status = runner.receive_status_for_etag("document-etag-b")?;
    assert_eq!(revised_status.document_etag, "document-etag-b");
    wait_until_file_exists(&source_directory.join("shutdown.received"))?;
    wait_until_text_equals(&source_directory.join("config.received"), r#"{"port":503}"#)?;
    wait_until_text_equals(
        &source_directory.join("starts.received"),
        "started\nstarted\n",
    )?;

    let revised_processes = plugin_process_snapshot(pipeline_directory)?;
    for (directory, initial_process_id) in &initial_processes {
        let revised_process_id = process_id_for_directory(&revised_processes, directory)?;
        if directory == &source_directory {
            if revised_process_id != *initial_process_id {
                wait_until_process_exits(*initial_process_id)?;
            }
        } else {
            assert_eq!(revised_process_id, *initial_process_id);
        }
    }
    assert_eq!(
        read_text_with_timeout(&source_directory.join("processes.received"))?
            .lines()
            .count(),
        2
    );
    if initial_source_process != process_id_for_directory(&revised_processes, &source_directory)? {
        assert!(!process_exists(initial_source_process)?);
    }
    assert_processes_alive(&revised_processes)?;
    assert_eq!(queue_file_snapshot(pipeline_directory)?, initial_queues);

    submit_ingress(&mut submission_writer, 2)?;
    assert_ingress_completion(&mut completion_reader, 2)?;

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn pipeline_combines_source_and_sink_config_actions_in_one_revision() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::SourceAndSinkConfigsRevisionAfterBootstrap)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;

    let initial_status =
        runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    assert_eq!(initial_status.document_etag, "document-etag-a");

    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let source_directory = instance_directory(pipeline_directory, "source");
    let kafka_primary_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"primary"}"#)?;
    let kafka_standby_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"standby"}"#)?;
    let iotdb_directory =
        sink_directory_for_config(pipeline_directory, r#"{"database":"factory"}"#)?;
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    let initial_queues = queue_file_snapshot(pipeline_directory)?;
    assert_processes_alive(&initial_processes)?;

    let mut submission_writer =
        source_submission_writer(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut completion_reader =
        source_completion_reader(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut kafka_primary_reader =
        sink_egress_reader(pipeline_directory, &kafka_primary_directory, 0)
            .map_err(io::Error::other)?;
    let mut kafka_standby_reader =
        sink_egress_reader(pipeline_directory, &kafka_standby_directory, 0)
            .map_err(io::Error::other)?;

    submit_ingress(&mut submission_writer, 1)?;
    let primary_record = read_egress_record(&mut kafka_primary_reader)?;
    assert_eq!(
        read_egress_record(&mut kafka_standby_reader)?,
        primary_record
    );

    runner.release_revision()?;
    wait_until_file_exists(&source_directory.join("source-quiesced.received"))?;
    runner.assert_no_status_for_etag("document-etag-b")?;
    kafka_primary_reader.release(1).map_err(io::Error::other)?;
    kafka_standby_reader.release(1).map_err(io::Error::other)?;
    assert_ingress_completion(&mut completion_reader, 1)?;
    let revised_status = runner.receive_status_for_etag("document-etag-b")?;
    assert_eq!(revised_status.document_etag, "document-etag-b");

    wait_until_text_equals(&source_directory.join("config.received"), r#"{"port":503}"#)?;
    wait_until_text_equals(
        &kafka_primary_directory.join("config.received"),
        r#"{"cluster":"replacement"}"#,
    )?;
    wait_until_text_equals(
        &iotdb_directory.join("config.received"),
        r#"{"database":"replacement"}"#,
    )?;
    let replaced_directories = [
        &source_directory,
        &kafka_primary_directory,
        &iotdb_directory,
    ];
    for directory in &replaced_directories {
        wait_until_text_equals(&directory.join("starts.received"), "started\nstarted\n")?;
    }

    let revised_processes = plugin_process_snapshot(pipeline_directory)?;
    for (directory, initial_process_id) in &initial_processes {
        let revised_process_id = process_id_for_directory(&revised_processes, directory)?;
        if directory == &kafka_standby_directory {
            assert_eq!(revised_process_id, *initial_process_id);
        } else {
            if revised_process_id != *initial_process_id {
                wait_until_process_exits(*initial_process_id)?;
            }
        }
    }
    assert_processes_alive(&revised_processes)?;
    assert_eq!(queue_file_snapshot(pipeline_directory)?, initial_queues);
    assert_eq!(
        read_text_with_timeout(&kafka_standby_directory.join("starts.received"))?,
        "started\n"
    );
    for directory in replaced_directories {
        assert_eq!(
            read_text_with_timeout(&directory.join("processes.received"))?
                .lines()
                .count(),
            2
        );
    }

    submit_ingress(&mut submission_writer, 2)?;
    assert_ingress_completion(&mut completion_reader, 2)?;
    assert_eq!(
        kafka_primary_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );
    assert_eq!(
        kafka_standby_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn pipeline_replaces_only_sinks_whose_configs_changed() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::SinkConfigsRevisionAfterBootstrap)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;

    let initial_status =
        runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    assert_eq!(initial_status.document_etag, "document-etag-a");

    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let source_directory = instance_directory(pipeline_directory, "source");
    let kafka_primary_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"primary"}"#)?;
    let kafka_standby_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"standby"}"#)?;
    let iotdb_directory =
        sink_directory_for_config(pipeline_directory, r#"{"database":"factory"}"#)?;
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    let initial_queues = queue_file_snapshot(pipeline_directory)?;
    assert_processes_alive(&initial_processes)?;
    let initial_primary_process =
        process_id_for_directory(&initial_processes, &kafka_primary_directory)?;
    let initial_iotdb_process = process_id_for_directory(&initial_processes, &iotdb_directory)?;

    let mut submission_writer =
        source_submission_writer(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut completion_reader =
        source_completion_reader(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut kafka_primary_reader =
        sink_egress_reader(pipeline_directory, &kafka_primary_directory, 0)
            .map_err(io::Error::other)?;
    let mut kafka_standby_reader =
        sink_egress_reader(pipeline_directory, &kafka_standby_directory, 0)
            .map_err(io::Error::other)?;

    submit_ingress(&mut submission_writer, 1)?;
    let primary_record = read_egress_record(&mut kafka_primary_reader)?;
    let standby_record = read_egress_record(&mut kafka_standby_reader)?;
    assert_eq!(standby_record, primary_record);

    runner.release_revision()?;
    let revised_status = runner.receive_status_for_etag("document-etag-b")?;
    assert_eq!(revised_status.document_etag, "document-etag-b");

    wait_until_file_exists(&kafka_primary_directory.join("shutdown.received"))?;
    wait_until_text_equals(
        &kafka_primary_directory.join("config.received"),
        r#"{"cluster":"replacement"}"#,
    )?;
    wait_until_text_equals(
        &kafka_primary_directory.join("starts.received"),
        "started\nstarted\n",
    )?;
    wait_until_file_exists(&iotdb_directory.join("shutdown.received"))?;
    wait_until_text_equals(
        &iotdb_directory.join("config.received"),
        r#"{"database":"replacement"}"#,
    )?;
    wait_until_text_equals(
        &iotdb_directory.join("starts.received"),
        "started\nstarted\n",
    )?;

    let revised_processes = plugin_process_snapshot(pipeline_directory)?;
    for (directory, initial_process_id) in &initial_processes {
        let revised_process_id = process_id_for_directory(&revised_processes, directory)?;
        if directory != &kafka_primary_directory && directory != &iotdb_directory {
            assert_eq!(revised_process_id, *initial_process_id);
        }
    }
    assert_processes_alive(&revised_processes)?;
    assert_eq!(queue_file_snapshot(pipeline_directory)?, initial_queues);
    assert_eq!(
        read_text_with_timeout(&kafka_primary_directory.join("processes.received"))?
            .lines()
            .count(),
        2
    );
    assert_eq!(
        read_text_with_timeout(&iotdb_directory.join("processes.received"))?
            .lines()
            .count(),
        2
    );
    if initial_primary_process
        != process_id_for_directory(&revised_processes, &kafka_primary_directory)?
    {
        assert!(!process_exists(initial_primary_process)?);
    }
    if initial_iotdb_process != process_id_for_directory(&revised_processes, &iotdb_directory)? {
        assert!(!process_exists(initial_iotdb_process)?);
    }
    assert_eq!(
        read_text_with_timeout(&source_directory.join("starts.received"))?,
        "started\n"
    );
    assert_eq!(
        read_text_with_timeout(&kafka_standby_directory.join("starts.received"))?,
        "started\n"
    );

    kafka_primary_reader.release(1).map_err(io::Error::other)?;
    kafka_standby_reader.release(1).map_err(io::Error::other)?;
    assert_ingress_completion(&mut completion_reader, 1)?;

    submit_ingress(&mut submission_writer, 2)?;
    assert_ingress_completion(&mut completion_reader, 2)?;

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn pipeline_applies_a_revision_when_replacement_sink_start_fails() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::SinkConfigRevisionStartFailure)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let initial_status =
        runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    assert_eq!(initial_status.document_etag, "document-etag-a");

    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let primary_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"primary"}"#)?;
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    let initial_queues = queue_file_snapshot(pipeline_directory)?;
    let initial_primary_process = process_id_for_directory(&initial_processes, &primary_directory)?;
    std::fs::remove_file(Path::new(&runner.fixture.plugin_directory).join("plugin.sh"))?;

    runner.release_revision()?;
    let revised_status = runner.receive_status_for_etag("document-etag-b")?;
    assert_eq!(revised_status.document_etag, "document-etag-b");
    let primary_status = revised_status
        .plugin_instances
        .iter()
        .find(|sink| sink.id == "kafka-primary")
        .ok_or_else(|| io::Error::other("Primary Kafka status was not found"))?;
    assert_eq!(
        primary_status.state,
        PluginInstanceState::StartFailed as i32
    );
    assert_eq!(
        primary_status
            .last_error
            .as_ref()
            .ok_or_else(|| io::Error::other("Primary Kafka start failure was not reported"))?
            .code,
        "plugin.spawn_failed"
    );
    wait_until_file_exists(&primary_directory.join("shutdown.received"))?;
    wait_until_process_exits(initial_primary_process)?;
    assert_eq!(queue_file_snapshot(pipeline_directory)?, initial_queues);
    let unaffected_processes = initial_processes
        .into_iter()
        .filter(|(directory, _)| directory != &primary_directory)
        .collect::<Vec<_>>();
    assert_processes_alive(&unaffected_processes)?;

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn pipeline_observes_parent_eof_while_a_replaced_sink_blocks_shutdown() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::SinkConfigRevisionTerminationStalled)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let initial_status =
        runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    assert_eq!(initial_status.document_etag, "document-etag-a");

    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let sink_directory = sink_directory_for_config(
        pipeline_directory,
        r#"{"behavior":"delay-shutdown","cluster":"primary"}"#,
    )?;
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;

    runner.release_revision()?;
    wait_until_file_exists(&sink_directory.join("shutdown.received"))?;

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    wait_until_processes_exit(&initial_processes)?;
    runner.finish()
}

#[test]
fn pipeline_observes_a_worker_failure_while_sink_replacement_is_stalled() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::SinkConfigRevisionWorkerFailure)?;
    let child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let initial_status =
        runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    assert_eq!(initial_status.document_etag, "document-etag-a");

    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    trigger_egress_worker_failure(pipeline_directory, |primary_directory| {
        runner.release_revision()?;
        wait_until_file_exists(&primary_directory.join("shutdown.received"))
    })?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("pipeline.data_plane_failed"));
    runner.wait_until_control_stream_closes()?;
    runner.assert_no_status_for_etag("document-etag-b")?;
    wait_until_processes_exit(&initial_processes)?;
    runner.finish()
}

#[test]
fn pipeline_observes_a_worker_failure_while_waiting_for_a_revision() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::SinkConfigRevisionWorkerFailure)?;
    let child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let initial_status =
        runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    assert_eq!(initial_status.document_etag, "document-etag-a");

    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    trigger_egress_worker_failure(pipeline_directory, |_| Ok(()))?;

    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("pipeline.data_plane_failed"));
    runner.wait_until_control_stream_closes()?;
    runner.assert_no_status_for_etag("document-etag-b")?;
    wait_until_processes_exit(&initial_processes)?;
    runner.finish()
}

#[test]
fn pipeline_replaces_a_waiting_revision_with_the_latest_target() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::LatestPendingRevision)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let initial_status =
        runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    assert_eq!(initial_status.document_etag, "document-etag-a");

    let pipeline_directory = Path::new(&runner.fixture.pipeline_working_directory);
    let primary_directory = sink_directory_for_config(
        pipeline_directory,
        r#"{"behavior":"delay-shutdown","cluster":"primary"}"#,
    )?;

    runner.release_revision()?;
    wait_until_file_exists(&primary_directory.join("shutdown.received"))?;
    runner.release_pending_revisions()?;
    runner.wait_until_pending_revisions_are_sent()?;
    std::fs::write(primary_directory.join("allow-shutdown"), b"released\n")?;

    let applied_d = runner.receive_status_for_etag("document-etag-d")?;
    assert_eq!(applied_d.document_etag, "document-etag-d");
    wait_until_text_equals(
        &primary_directory.join("config.received"),
        r#"{"cluster":"replacement-d"}"#,
    )?;
    let received_configs = read_text_with_timeout(&primary_directory.join("configs.received"))?;
    assert!(!received_configs.contains("replacement-c"));

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn pipeline_accepts_c_and_d_while_b_reconfiguration_is_active() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::LatestPendingRevisionAdmission)?;
    let child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let initial_status =
        runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    assert_eq!(initial_status.document_etag, "document-etag-a");

    let pipeline_directory = Path::new(&runner.fixture.pipeline_working_directory);
    let primary_directory = sink_directory_for_config(
        pipeline_directory,
        r#"{"behavior":"delay-shutdown","cluster":"primary"}"#,
    )?;

    runner.release_revision()?;
    wait_until_file_exists(&primary_directory.join("shutdown.received"))?;
    runner.release_pending_revisions()?;

    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert!(!output.status.success());
    assert!(stderr(&output).contains("Runner sends only revisions after Bootstrap"));
    runner.finish()
}

#[test]
fn pipeline_replaces_lua_vms_without_replacing_queues_or_plugins() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::LuaReplacementAfterBootstrap)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let initial_status =
        runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    assert_eq!(initial_status.document_etag, "document-etag-a");
    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let initial_queues = queue_file_snapshot(pipeline_directory)?;
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    assert_all_plugins_started_once(pipeline_directory)?;
    let kafka_primary_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"primary"}"#)?;
    let kafka_standby_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"standby"}"#)?;
    let mut submission_writer =
        source_submission_writer(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut completion_reader =
        source_completion_reader(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut kafka_primary_reader =
        sink_egress_reader(pipeline_directory, &kafka_primary_directory, 0)
            .map_err(io::Error::other)?;
    let mut kafka_standby_reader =
        sink_egress_reader(pipeline_directory, &kafka_standby_directory, 0)
            .map_err(io::Error::other)?;

    submit_ingress(&mut submission_writer, 1)?;
    let primary_record = read_egress_record(&mut kafka_primary_reader)?;
    let standby_record = read_egress_record(&mut kafka_standby_reader)?;
    assert_eq!(standby_record, primary_record);
    assert_eq!(
        completion_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );

    runner.release_revision()?;
    runner.assert_no_status_for_etag("document-etag-b")?;
    kafka_primary_reader.release(1).map_err(io::Error::other)?;
    kafka_standby_reader.release(1).map_err(io::Error::other)?;
    assert_ingress_completion(&mut completion_reader, 1)?;
    let revised_status = runner.receive_status_for_etag("document-etag-b")?;
    assert_eq!(revised_status.document_etag, "document-etag-b");
    assert_eq!(queue_file_snapshot(pipeline_directory)?, initial_queues);
    assert_all_plugins_started_once(pipeline_directory)?;
    assert_eq!(
        plugin_process_snapshot(pipeline_directory)?,
        initial_processes
    );
    assert_processes_alive(&initial_processes)?;

    submit_ingress(&mut submission_writer, 2)?;
    assert_ingress_completion(&mut completion_reader, 2)?;
    assert_eq!(
        kafka_primary_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );
    assert_eq!(
        kafka_standby_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;

    runner.finish()
}

#[test]
fn lua_replacement_preserves_existing_start_failed_sink_status() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::LuaReplacementWithStartFailedSinks)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let initial_status = runner
        .receive_status_matching("document-etag-a", |status| {
            status
                .plugin_instances
                .iter()
                .find(|instance| instance.id == "source")
                .is_some_and(|source| source.state == PluginInstanceState::Running as i32)
                && status
                    .plugin_instances
                    .iter()
                    .any(|sink| sink.state == PluginInstanceState::StartFailed as i32)
        })
        .map_err(|error| io::Error::other(format!("Initial status was not observed: {error}")))?;
    assert_eq!(initial_status.document_etag, "document-etag-a");

    runner.release_revision()?;
    let revised_status = runner
        .receive_status_for_etag("document-etag-b")
        .map_err(|error| io::Error::other(format!("Revised status was not observed: {error}")))?;
    assert_eq!(revised_status.document_etag, "document-etag-b");
    assert!(
        revised_status
            .plugin_instances
            .iter()
            .any(|sink| sink.state == PluginInstanceState::StartFailed as i32)
    );

    child.close_lifetime_channel()?;
    let output = child
        .wait_with_timeout(Duration::from_secs(5))
        .map_err(|error| io::Error::other(format!("Pipeline did not stop: {error}")))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn pipeline_keeps_new_lua_paused_until_same_revision_sink_replacement_finishes() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::LuaAndSinkConfigRevisionAfterBootstrap)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let initial_status =
        runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    assert_eq!(initial_status.document_etag, "document-etag-a");

    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let primary_directory = sink_directory_for_config(
        pipeline_directory,
        r#"{"behavior":"delay-shutdown","cluster":"primary"}"#,
    )?;
    let standby_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"standby"}"#)?;
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    let initial_queues = queue_file_snapshot(pipeline_directory)?;
    let initial_primary_process = process_id_for_directory(&initial_processes, &primary_directory)?;
    let mut submission_writer =
        source_submission_writer(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut completion_reader =
        source_completion_reader(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut primary_reader =
        sink_egress_reader(pipeline_directory, &primary_directory, 0).map_err(io::Error::other)?;
    let mut standby_reader =
        sink_egress_reader(pipeline_directory, &standby_directory, 0).map_err(io::Error::other)?;

    runner.release_revision()?;
    wait_until_file_exists(&primary_directory.join("shutdown.received"))?;
    submit_ingress(&mut submission_writer, 1)?;
    assert_eq!(
        completion_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );
    assert_eq!(
        primary_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );
    assert_eq!(
        standby_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );
    runner.assert_no_status_for_etag("document-etag-b")?;

    std::fs::write(primary_directory.join("allow-shutdown"), b"released\n")?;
    let revised_status = runner.receive_status_for_etag("document-etag-b")?;
    assert_eq!(revised_status.document_etag, "document-etag-b");
    wait_until_text_equals(
        &primary_directory.join("config.received"),
        r#"{"cluster":"replacement"}"#,
    )?;
    wait_until_text_equals(
        &primary_directory.join("starts.received"),
        "started\nstarted\n",
    )?;
    assert_ingress_completion(&mut completion_reader, 1)?;
    assert_eq!(
        primary_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );
    assert_eq!(
        standby_reader.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    );
    assert_eq!(queue_file_snapshot(pipeline_directory)?, initial_queues);

    let revised_processes = plugin_process_snapshot(pipeline_directory)?;
    for (directory, initial_process_id) in &initial_processes {
        let revised_process_id = process_id_for_directory(&revised_processes, directory)?;
        if directory != &primary_directory {
            assert_eq!(revised_process_id, *initial_process_id);
        }
    }
    assert_eq!(
        read_text_with_timeout(&primary_directory.join("processes.received"))?
            .lines()
            .count(),
        2
    );
    if initial_primary_process != process_id_for_directory(&revised_processes, &primary_directory)?
    {
        wait_until_process_exits(initial_primary_process)?;
    }
    assert_processes_alive(&revised_processes)?;

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn pipeline_stops_and_cleans_up_when_runner_breaks_the_revision_invariant() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::UnparseableRevisionAfterBootstrap)?;
    let child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let initial_status =
        runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    assert_eq!(initial_status.document_etag, "document-etag-a");
    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    assert_all_plugins_started_once(pipeline_directory)?;

    runner.release_revision()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("Runner must serialize its verified Document as strict JSON"));
    runner.wait_until_control_stream_closes()?;
    runner.assert_no_status_for_etag("document-etag-b")?;
    assert!(!pipeline_directory.exists());
    wait_until_processes_exit(&initial_processes)?;
    runner.finish()
}

#[test]
fn pipeline_replaces_channel_layout_and_restarts_every_affected_sink() -> io::Result<()> {
    assert_source_stream_replacement(
        Scenario::SourceQueueDeliveryRevisionAfterBootstrap,
        3,
        SourceStreamTargetBehavior::FailsAfterAtMostOnceAck,
    )
}

#[test]
fn pipeline_replaces_source_program_and_contract_without_replacing_sink_owners() -> io::Result<()> {
    assert_source_stream_replacement(
        Scenario::SourceProgramContractRevisionAfterBootstrap,
        2,
        SourceStreamTargetBehavior::EmitsKafka,
    )
}

#[test]
fn source_stream_cutover_failure_never_publishes_the_target_revision() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::SourceQueueDeliveryRevisionAfterBootstrap)?;
    let child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    let root_text = runner.fixture.pipeline_working_directory.clone();
    let root = Path::new(&root_text);
    let source = instance_directory(root, "source");
    let processes = plugin_process_snapshot(root)?;
    let mut submission = source_submission_writer(root, 0).map_err(io::Error::other)?;
    let mut completion = source_completion_reader(root, 0).map_err(io::Error::other)?;
    let mut primary = sink_egress_reader(root, &instance_directory(root, "kafka-primary"), 0)
        .map_err(io::Error::other)?;
    let mut standby = sink_egress_reader(root, &instance_directory(root, "kafka-standby"), 0)
        .map_err(io::Error::other)?;
    submit_ingress(&mut submission, 1)?;
    assert_eq!(
        read_egress_record(&mut primary)?,
        read_egress_record(&mut standby)?
    );
    runner.release_revision()?;
    wait_until_file_exists(&source.join("source-quiesced.received"))?;
    runner.assert_no_status_for_etag("document-etag-b")?;
    let candidate = root
        .join(".candidate")
        .join(
            source
                .file_name()
                .ok_or_else(|| io::Error::other("Instance path has no name"))?,
        )
        .join("source");
    // The prepared workers retain their open mappings, but installing their
    // actual directory must now fail after the old responsibility has drained.
    std::fs::rename(&candidate, candidate.with_file_name("source.saved"))?;
    primary.release(1).map_err(io::Error::other)?;
    standby.release(1).map_err(io::Error::other)?;
    assert_ingress_completion(&mut completion, 1)?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert!(stderr(&output).contains("pipeline.reconfigure_failed"));
    runner.wait_until_control_stream_closes()?;
    runner.assert_no_status_for_etag("document-etag-b")?;
    wait_until_processes_exit(&processes)?;
    assert!(!root.exists());
    runner.finish()
}

#[test]
fn source_stream_revision_is_applied_when_replacement_source_cannot_start() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::SourceProgramContractRevisionAfterBootstrap)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;

    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let source_directory = instance_directory(pipeline_directory, "source");
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    let initial_source_process = process_id_for_directory(&initial_processes, &source_directory)?;
    let old_source_queue_identity_guard =
        hold_queue_file_identities(&source_directory.join("source"))?;
    let initial_queues = queue_file_snapshot(pipeline_directory)?;
    std::fs::remove_file(Path::new(&runner.fixture.plugin_directory).join("plugin.sh"))?;

    runner.release_revision()?;
    let revised_status = runner.receive_status_for_etag("document-etag-b")?;
    let source_status = revised_status
        .plugin_instances
        .iter()
        .find(|instance| instance.id == "source")
        .cloned()
        .ok_or_else(|| io::Error::other("Replacement Source status was not found"))?;
    assert_eq!(source_status.state, PluginInstanceState::StartFailed as i32);
    assert_eq!(
        source_status
            .last_error
            .ok_or_else(|| io::Error::other("Replacement Source failure was not reported"))?
            .code,
        "plugin.spawn_failed"
    );
    assert!(
        revised_status
            .plugin_instances
            .iter()
            .filter(|instance| instance.id != "source")
            .all(|sink| sink.state == PluginInstanceState::Running as i32)
    );
    wait_until_process_exits(initial_source_process)?;
    let unchanged_sinks = initial_processes
        .iter()
        .filter(|(directory, _)| directory != &source_directory)
        .cloned()
        .collect::<Vec<_>>();
    assert_processes_alive(&unchanged_sinks)?;
    assert_source_queues_replaced(
        &initial_queues,
        &queue_file_snapshot(pipeline_directory)?,
        &source_directory,
        2,
    )?;
    drop(old_source_queue_identity_guard);

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn source_stream_drain_restarts_a_sink_before_completing_old_output() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::SourceProgramContractRevisionAfterBootstrap)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;

    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let source_directory = instance_directory(pipeline_directory, "source");
    let primary_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"primary"}"#)?;
    let standby_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"standby"}"#)?;
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    let initial_primary_process = process_id_for_directory(&initial_processes, &primary_directory)?;
    let mut submission =
        source_submission_writer(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut completion =
        source_completion_reader(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut primary_reader =
        sink_egress_reader(pipeline_directory, &primary_directory, 0).map_err(io::Error::other)?;
    let mut standby_reader =
        sink_egress_reader(pipeline_directory, &standby_directory, 0).map_err(io::Error::other)?;

    submit_ingress(&mut submission, 601)?;
    let primary_record = read_egress_record(&mut primary_reader)?;
    assert_eq!(read_egress_record(&mut standby_reader)?, primary_record);
    runner.release_revision()?;
    wait_until_file_exists(&source_directory.join("source-quiesced.received"))?;
    runner.assert_no_status_for_etag("document-etag-b")?;

    let primary_pid = Pid::from_raw(
        i32::try_from(initial_primary_process)
            .map_err(|_| io::Error::other("Sink process id exceeded the platform range"))?,
    )
    .ok_or_else(|| io::Error::other("Sink process id was zero"))?;
    kill_process(primary_pid, Signal::KILL).map_err(io::Error::from)?;
    wait_until_text_equals(
        &primary_directory.join("starts.received"),
        "started\nstarted\n",
    )?;
    let restarted_processes = plugin_process_snapshot(pipeline_directory)?;
    assert_ne!(
        process_id_for_directory(&restarted_processes, &primary_directory)?,
        initial_primary_process
    );

    primary_reader.release(1).map_err(io::Error::other)?;
    standby_reader.release(1).map_err(io::Error::other)?;
    assert_ingress_completion(&mut completion, 601)?;
    runner.receive_status_matching("document-etag-b", all_plugins_are_running)?;

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn sigterm_during_source_stream_drain_cleans_every_owner_without_waiting_for_sink_release()
-> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::SourceProgramContractRevisionAfterBootstrap)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;

    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let source_directory = instance_directory(pipeline_directory, "source");
    let primary_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"primary"}"#)?;
    let standby_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"standby"}"#)?;
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    let mut submission =
        source_submission_writer(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut primary_reader =
        sink_egress_reader(pipeline_directory, &primary_directory, 0).map_err(io::Error::other)?;
    let mut standby_reader =
        sink_egress_reader(pipeline_directory, &standby_directory, 0).map_err(io::Error::other)?;

    submit_ingress(&mut submission, 701)?;
    let primary_record = read_egress_record(&mut primary_reader)?;
    assert_eq!(read_egress_record(&mut standby_reader)?, primary_record);
    runner.release_revision()?;
    wait_until_file_exists(&source_directory.join("source-quiesced.received"))?;
    runner.assert_no_status_for_etag("document-etag-b")?;

    child.request_termination()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_eq!(
        output.status.code(),
        Some(0),
        "Pipeline stderr: {}",
        stderr(&output)
    );
    assert!(stderr(&output).is_empty());
    runner.assert_no_status_for_etag("document-etag-b")?;
    wait_until_processes_exit(&initial_processes)?;
    runner.finish()
}

#[derive(Clone, Copy)]
enum SourceStreamTargetBehavior {
    FailsAfterAtMostOnceAck,
    EmitsKafka,
}

fn assert_source_stream_replacement(
    scenario: Scenario,
    target_parallelism: usize,
    target_behavior: SourceStreamTargetBehavior,
) -> io::Result<()> {
    let mut runner = FakeRunner::start(scenario)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;

    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let source_directory = instance_directory(pipeline_directory, "source");
    let primary_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"primary"}"#)?;
    let standby_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"standby"}"#)?;
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    let initial_source_process = process_id_for_directory(&initial_processes, &source_directory)?;
    let initial_queues = queue_file_snapshot(pipeline_directory)?;

    let mut old_submission =
        source_submission_writer(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut old_completion =
        source_completion_reader(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut primary_reader =
        sink_egress_reader(pipeline_directory, &primary_directory, 0).map_err(io::Error::other)?;
    let mut standby_reader =
        sink_egress_reader(pipeline_directory, &standby_directory, 0).map_err(io::Error::other)?;

    submit_ingress(&mut old_submission, 301)?;
    let old_record = read_egress_record(&mut primary_reader)?;
    assert_eq!(read_egress_record(&mut standby_reader)?, old_record);
    submit_ingress(&mut old_submission, 302)?;

    runner.release_revision()?;
    wait_until_file_exists(&source_directory.join("source-quiesced.received"))?;
    runner.assert_no_status_for_etag("document-etag-b")?;

    primary_reader.release(1).map_err(io::Error::other)?;
    standby_reader.release(1).map_err(io::Error::other)?;
    assert_ingress_completion(&mut old_completion, 301)?;
    let drained_record = read_egress_record(&mut primary_reader)?;
    assert_eq!(read_egress_record(&mut standby_reader)?, drained_record);
    runner.assert_no_status_for_etag("document-etag-b")?;
    primary_reader.release(1).map_err(io::Error::other)?;
    standby_reader.release(1).map_err(io::Error::other)?;
    assert_ingress_completion(&mut old_completion, 302)?;
    runner.receive_status_matching("document-etag-b", all_plugins_are_running)?;

    let revised_processes = plugin_process_snapshot(pipeline_directory)?;
    let revised_source_process = process_id_for_directory(&revised_processes, &source_directory)?;
    assert_ne!(revised_source_process, initial_source_process);
    wait_until_process_exits(initial_source_process)?;
    for (directory, initial_process_id) in &initial_processes {
        if directory == &source_directory {
            continue;
        }
        if target_parallelism != 2 {
            let revised_process_id = process_id_for_directory(&revised_processes, directory)?;
            assert_ne!(revised_process_id, *initial_process_id);
            wait_until_process_exits(*initial_process_id)?;
        } else {
            assert_eq!(
                process_id_for_directory(&revised_processes, directory)?,
                *initial_process_id
            );
        }
    }
    assert_processes_alive(&revised_processes)?;
    assert_source_queues_replaced(
        &initial_queues,
        &queue_file_snapshot(pipeline_directory)?,
        &source_directory,
        target_parallelism,
    )?;
    submit_ingress(&mut old_submission, 900)?;
    assert!(matches!(
        old_completion.try_read().map_err(io::Error::other)?,
        ReadOutcome::Empty
    ));

    let mut new_submission =
        source_submission_writer(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut new_completion =
        source_completion_reader(pipeline_directory, 0).map_err(io::Error::other)?;
    let target_payload = match target_behavior {
        SourceStreamTargetBehavior::FailsAfterAtMostOnceAck => Vec::new(),
        SourceStreamTargetBehavior::EmitsKafka => source_revision_payload("new-contract")?,
    };
    submit_ingress_payload(&mut new_submission, 303, target_payload)?;
    match target_behavior {
        SourceStreamTargetBehavior::FailsAfterAtMostOnceAck => {
            assert_ingress_completion(&mut new_completion, 303)?;
        }
        SourceStreamTargetBehavior::EmitsKafka => {
            let new_record = read_egress_record(&mut primary_reader)?;
            assert_eq!(read_egress_record(&mut standby_reader)?, new_record);
            primary_reader.release(1).map_err(io::Error::other)?;
            standby_reader.release(1).map_err(io::Error::other)?;
            assert_ingress_completion(&mut new_completion, 303)?;
        }
    }

    assert_eq!(
        read_text_with_timeout(&source_directory.join("starts.received"))?,
        "started\nstarted\n"
    );
    for directory in plugin_working_directories(pipeline_directory)? {
        if directory != source_directory {
            let expected = if target_parallelism != 2 {
                "started\nstarted\n"
            } else {
                "started\n"
            };
            assert_eq!(
                read_text_with_timeout(&directory.join("starts.received"))?,
                expected
            );
        }
    }

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn failed_stage_keeps_current_material_until_structured_pipeline_shutdown() -> io::Result<()> {
    let mut runner = FakeRunner::start(Scenario::LuaPreparationFailureWithSourceConfig)?;
    let child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let initial_status =
        runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    assert_eq!(initial_status.document_etag, "document-etag-a");
    let pipeline_directory_text = runner.fixture.pipeline_working_directory.clone();
    let pipeline_directory = Path::new(&pipeline_directory_text);
    let source_directory = instance_directory(pipeline_directory, "source");
    let initial_processes = plugin_process_snapshot(pipeline_directory)?;
    assert_eq!(
        read_text_with_timeout(&source_directory.join("configs.received"))?,
        "{\"port\":502}\n"
    );
    assert!(!source_directory.join("shutdown.received").exists());

    runner.release_revision()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("pipeline.reconfigure_failed"));
    assert!(!pipeline_directory.exists());
    runner.wait_until_control_stream_closes()?;
    runner.assert_no_status_for_etag("document-etag-b")?;
    wait_until_processes_exit(&initial_processes)?;

    runner.finish()
}

#[test]
fn pipeline_rejects_bootstrap_without_environment() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::BootstrapMissingEnvironment)?;
    let output = runner.run_pipeline()?;
    assert!(!output.status.success());
    assert!(stderr(&output).contains("Runner supplies the Pipeline environment"));
    assert_expected_attach(runner.receive_attach()?)?;
    runner.finish()
}

#[test]
fn pipeline_rejects_a_second_bootstrap() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::DuplicateBootstrap)?;
    let output = runner.run_pipeline()?;
    assert!(!output.status.success());
    assert!(stderr(&output).contains("Runner sends only revisions after Bootstrap"));
    assert_expected_attach(runner.receive_attach()?)?;
    runner.finish()
}

#[test]
fn pipeline_rejects_revision_before_bootstrap() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::RevisionBeforeBootstrap)?;
    let output = runner.run_pipeline()?;
    assert!(!output.status.success());
    assert!(stderr(&output).contains("Runner sends Bootstrap as its first message"));
    assert_expected_attach(runner.receive_attach()?)?;
    runner.finish()
}

#[test]
fn pipeline_fails_when_runner_disconnects_before_bootstrap() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::DisconnectBeforeBootstrap)?;
    let output = runner.run_pipeline()?;
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("pipeline.control_stream_disconnected"));
    assert_expected_attach(runner.receive_attach()?)?;
    runner.finish()
}

#[test]
fn pipeline_force_kills_its_group_when_the_runner_control_stream_disappears() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::ControlStreamLossAfterBootstrap)?;
    let child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let _first_status = runner.receive_status()?;

    let output = child.wait_with_timeout(Duration::from_secs(5))?;

    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn pipeline_process_timeout_terminates_and_reaps_a_connected_child() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::KeepStreamOpenAfterBootstrap)?;
    let child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let status = runner.receive_status_for_etag("document-etag-a")?;
    assert_eq!(status.document_etag, "document-etag-a");
    let processes = plugin_process_snapshot(Path::new(&runner.fixture.pipeline_working_directory))?;
    let error = match child.wait_with_timeout(Duration::ZERO) {
        Ok(_) => {
            return Err(io::Error::other("Pipeline exited before the test deadline"));
        }
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    wait_until_processes_exit(&processes)?;
    runner.finish()
}

#[test]
fn pipeline_exits_when_the_runner_lifetime_channel_closes() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::KeepStreamOpenAfterBootstrap)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    child.close_lifetime_channel()?;

    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    runner.finish()
}

#[test]
fn runner_lifetime_loss_force_kills_plugins_and_ordinary_descendants() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::RunnerLossWithPluginDescendant)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let _running = runner.receive_status_matching("document-etag-a", all_plugins_are_running)?;
    let pipeline_directory = Path::new(&runner.fixture.pipeline_working_directory);
    let plugin_processes = plugin_process_snapshot(pipeline_directory)?;
    let descendant_process_id = read_process_id_with_timeout(
        &instance_directory(pipeline_directory, "source").join("descendant.pid"),
    )?;
    assert_processes_alive(&plugin_processes)?;
    if !process_exists(descendant_process_id)? {
        return Err(io::Error::other(
            "Source Plugin descendant exited before Runner loss",
        ));
    }

    child.close_lifetime_channel()?;
    let output = child.wait_with_timeout(Duration::from_secs(5))?;

    assert_pipeline_group_force_killed(&output)?;
    wait_until_processes_exit(&plugin_processes)?;
    wait_until_process_exits(descendant_process_id)?;
    runner.finish()
}

#[test]
fn pipeline_exits_when_the_runner_writes_lifetime_channel_data() -> io::Result<()> {
    let runner = FakeRunner::start(Scenario::KeepStreamOpenAfterBootstrap)?;
    let mut child = runner.spawn_pipeline()?;
    assert_expected_attach(runner.receive_attach()?)?;
    let mut lifetime_channel = child.take_lifetime_channel()?;
    lifetime_channel.write_all(b"x")?;
    lifetime_channel.flush()?;

    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr(&output).contains("pipeline.parent_lifetime_data_received"));
    runner.finish()
}

#[test]
fn pipeline_observes_parent_eof_while_the_control_handshake_is_stalled() -> io::Result<()> {
    let peer = StalledControlPeer::start()?;
    let mut child = TestPipelineProcess::spawn(&peer.socket_path)?;
    peer.wait_until_connected()?;
    child.close_lifetime_channel()?;

    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_pipeline_group_force_killed(&output)?;
    peer.finish()
}

#[test]
fn pipeline_refuses_to_signal_an_inherited_process_group_after_parent_loss() -> io::Result<()> {
    let peer = StalledControlPeer::start()?;
    let mut child = TestPipelineProcess::spawn_inherited_group(&peer.socket_path)?;
    peer.wait_until_connected()?;
    child.close_lifetime_channel()?;

    let output = child.wait_with_timeout(Duration::from_secs(5))?;

    assert_eq!(output.status.signal(), Some(libc::SIGABRT));
    peer.finish()
}

#[test]
fn pipeline_sigterm_stops_cleanly_while_the_control_handshake_is_stalled() -> io::Result<()> {
    let peer = StalledControlPeer::start()?;
    let mut child = TestPipelineProcess::spawn(&peer.socket_path)?;
    peer.wait_until_connected()?;
    child.request_termination()?;

    let output = child.wait_with_timeout(Duration::from_secs(5))?;
    assert_eq!(output.status.code(), Some(0));
    assert!(stderr(&output).is_empty());
    peer.finish()
}

#[test]
fn stalled_control_peer_can_stop_before_a_pipeline_connects() -> io::Result<()> {
    StalledControlPeer::start()?.finish()
}

fn scenario_messages(
    scenario: Scenario,
    fixture: &StartupFixture,
) -> io::Result<Vec<RunnerToPipeline>> {
    let mut bootstrap = startup_bootstrap(fixture)?;
    let revision = revision_from_bootstrap(&bootstrap, RevisionMutation::Equivalent)?;
    match scenario {
        Scenario::PublishInitialStatus => Ok(vec![bootstrap]),
        Scenario::PlannedShutdownStalledByQuiesce => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let revision = payload
                .revision_plan
                .as_mut()
                .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
            let mut document: serde_json::Value =
                serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
            document["pluginInstances"]["source"]["config"]["behavior"] =
                serde_json::Value::String(String::from("delay-quiesce"));
            revision.tenon_document_json =
                serde_json::to_string(&document).map_err(io::Error::other)?;
            Ok(vec![bootstrap])
        }
        Scenario::SourceStartFailure => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let source = payload
                .revision_plan
                .as_mut()
                .and_then(|revision| {
                    revision
                        .plugin_programs
                        .iter_mut()
                        .find(|program| program.plugin_interface == PluginInterface::Source as i32)
                })
                .ok_or_else(|| io::Error::other("Bootstrap Source runtime is missing"))?;
            source.command = vec![String::from("./missing-plugin")];
            Ok(vec![bootstrap])
        }
        Scenario::SourceConfigWriteStalled => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let revision = payload
                .revision_plan
                .as_mut()
                .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
            let source = revision
                .plugin_programs
                .iter_mut()
                .find(|program| program.plugin_interface == PluginInterface::Source as i32)
                .ok_or_else(|| io::Error::other("Bootstrap Source runtime is missing"))?;
            source.command = vec![String::from("/bin/sh"), String::from("stall.sh")];
            let mut document: serde_json::Value =
                serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
            document["pluginInstances"]["source"]["config"] = serde_json::json!({
                "payload": "x".repeat(1_048_576),
            });
            revision.tenon_document_json =
                serde_json::to_string(&document).map_err(io::Error::other)?;
            Ok(vec![bootstrap])
        }
        Scenario::RunnerLossWithPluginDescendant => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let source = payload
                .revision_plan
                .as_mut()
                .and_then(|revision| {
                    revision
                        .plugin_programs
                        .iter_mut()
                        .find(|program| program.plugin_interface == PluginInterface::Source as i32)
                })
                .ok_or_else(|| io::Error::other("Bootstrap Source runtime is missing"))?;
            let adapter = source
                .command
                .iter_mut()
                .find(|argument| argument.contains("TENON_TEST_PLUGIN_WORKING_DIRECTORY"))
                .ok_or_else(|| io::Error::other("Controlled child argument adapter is missing"))?;
            let launching = adapter.clone();
            *adapter = launching.replace(
                "exec \"$child\"",
                "sleep 86400 </dev/null >/dev/null 2>&1 &\nprintf '%s' \"$!\" > \"$TENON_TEST_PLUGIN_WORKING_DIRECTORY/descendant.pid\"\nexec \"$child\"",
            );
            if *adapter == launching {
                return Err(io::Error::other(
                    "Controlled child argument adapter cannot host a descendant",
                ));
            }
            Ok(vec![bootstrap])
        }
        Scenario::LuaStartupStalled | Scenario::WrongPhaseWhileLuaStartupStalled => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let revision = payload
                .revision_plan
                .as_mut()
                .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
            let mut document: serde_json::Value =
                serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
            document["flows"]["main"]["process"]["script"] = serde_json::Value::String(
                String::from("while true do end\nfunction main(event) emit() end"),
            );
            revision.tenon_document_json =
                serde_json::to_string(&document).map_err(io::Error::other)?;
            let lua_limits = payload
                .environment
                .as_mut()
                .and_then(|environment| environment.lua_limits.as_mut())
                .ok_or_else(|| io::Error::other("Bootstrap Lua limits are missing"))?;
            lua_limits.cpu_time_limit_ms = 30_000;
            if matches!(scenario, Scenario::WrongPhaseWhileLuaStartupStalled) {
                Ok(vec![bootstrap.clone(), bootstrap])
            } else {
                Ok(vec![bootstrap])
            }
        }
        Scenario::EquivalentRevisionAfterBootstrap => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let revision = payload
                .revision_plan
                .as_mut()
                .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
            let mut document: serde_json::Value =
                serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
            document["flows"]["main"]["process"]["script"] =
                serde_json::Value::String(String::from(STATEFUL_KEEP_RUNTIME_LUA));
            revision.tenon_document_json =
                serde_json::to_string(&document).map_err(io::Error::other)?;
            let revision = revision_from_bootstrap(&bootstrap, RevisionMutation::Equivalent)?;
            Ok(vec![bootstrap, revision])
        }
        Scenario::SourceConfigRevisionAfterBootstrap => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let revision = payload
                .revision_plan
                .as_mut()
                .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
            let mut document: serde_json::Value =
                serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
            document["flows"]["main"]["process"]["script"] =
                serde_json::Value::String(String::from(STATEFUL_KEEP_RUNTIME_LUA));
            revision.tenon_document_json =
                serde_json::to_string(&document).map_err(io::Error::other)?;
            let revision = revision_from_bootstrap(&bootstrap, RevisionMutation::SourceConfig)?;
            Ok(vec![bootstrap, revision])
        }
        Scenario::SourceAndSinkConfigsRevisionAfterBootstrap => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let revision = payload
                .revision_plan
                .as_mut()
                .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
            let mut document: serde_json::Value =
                serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
            document["flows"]["main"]["process"]["script"] =
                serde_json::Value::String(String::from(STATEFUL_KEEP_RUNTIME_LUA));
            revision.tenon_document_json =
                serde_json::to_string(&document).map_err(io::Error::other)?;
            let revision =
                revision_from_bootstrap(&bootstrap, RevisionMutation::SourceAndSinkConfigs)?;
            Ok(vec![bootstrap, revision])
        }
        Scenario::SinkConfigsRevisionAfterBootstrap => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let revision = payload
                .revision_plan
                .as_mut()
                .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
            let mut document: serde_json::Value =
                serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
            document["flows"]["main"]["process"]["script"] =
                serde_json::Value::String(String::from(STATEFUL_KEEP_RUNTIME_LUA));
            revision.tenon_document_json =
                serde_json::to_string(&document).map_err(io::Error::other)?;
            let revision = revision_from_bootstrap(&bootstrap, RevisionMutation::SinkConfigs)?;
            Ok(vec![bootstrap, revision])
        }
        Scenario::SinkConfigRevisionStartFailure => {
            let revision = revision_from_bootstrap(&bootstrap, RevisionMutation::SinkConfig)?;
            Ok(vec![bootstrap, revision])
        }
        Scenario::SinkConfigRevisionTerminationStalled => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let revision = payload
                .revision_plan
                .as_mut()
                .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
            let mut document: serde_json::Value =
                serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
            set_sink_config_string(&mut document, "kafka-primary", "behavior", "delay-shutdown")?;
            revision.tenon_document_json =
                serde_json::to_string(&document).map_err(io::Error::other)?;
            let revision = revision_from_bootstrap(&bootstrap, RevisionMutation::SinkConfig)?;
            Ok(vec![bootstrap, revision])
        }
        Scenario::SinkConfigRevisionWorkerFailure => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let revision = payload
                .revision_plan
                .as_mut()
                .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
            let mut document: serde_json::Value =
                serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
            set_sink_config_string(&mut document, "kafka-primary", "behavior", "delay-shutdown")?;
            document["flows"]["main"]["process"]["script"] =
                serde_json::Value::String(String::from(ALWAYS_EMIT_KAFKA_LUA));
            revision.tenon_document_json =
                serde_json::to_string(&document).map_err(io::Error::other)?;
            let revision = revision_from_bootstrap(&bootstrap, RevisionMutation::SinkConfig)?;
            Ok(vec![bootstrap, revision])
        }
        Scenario::LatestPendingRevision | Scenario::LatestPendingRevisionAdmission => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let revision = payload
                .revision_plan
                .as_mut()
                .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
            let mut document: serde_json::Value =
                serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
            set_sink_config_string(&mut document, "kafka-primary", "behavior", "delay-shutdown")?;
            revision.tenon_document_json =
                serde_json::to_string(&document).map_err(io::Error::other)?;
            let revision_b = sink_config_revision(&bootstrap, "document-etag-b", "replacement-b")?;
            let revision_c = sink_config_revision(&bootstrap, "document-etag-c", "replacement-c")?;
            let revision_d = sink_config_revision(&bootstrap, "document-etag-d", "replacement-d")?;
            let mut messages = vec![bootstrap.clone(), revision_b, revision_c, revision_d];
            if matches!(scenario, Scenario::LatestPendingRevisionAdmission) {
                messages.push(bootstrap);
            }
            Ok(messages)
        }
        Scenario::LuaReplacementAfterBootstrap => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let revision = payload
                .revision_plan
                .as_mut()
                .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
            let mut document: serde_json::Value =
                serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
            document["flows"]["main"]["process"]["script"] =
                serde_json::Value::String(String::from(ALWAYS_EMIT_KAFKA_LUA));
            revision.tenon_document_json =
                serde_json::to_string(&document).map_err(io::Error::other)?;
            let revision = revision_from_bootstrap(&bootstrap, RevisionMutation::LuaSource)?;
            Ok(vec![bootstrap, revision])
        }
        Scenario::LuaReplacementWithStartFailedSinks => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let revision = payload
                .revision_plan
                .as_mut()
                .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
            let sink = revision
                .plugin_programs
                .iter_mut()
                .find(|program| program.program_name == "com.example.iotdb")
                .ok_or_else(|| io::Error::other("Bootstrap Sink runtime is missing"))?;
            sink.command = vec![String::from("./missing-sink-plugin")];
            let revision = revision_from_bootstrap(&bootstrap, RevisionMutation::LuaSource)?;
            Ok(vec![bootstrap, revision])
        }
        Scenario::LuaAndSinkConfigRevisionAfterBootstrap => {
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            let revision = payload
                .revision_plan
                .as_mut()
                .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
            let mut document: serde_json::Value =
                serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
            document["flows"]["main"]["process"]["script"] =
                serde_json::Value::String(String::from(ALWAYS_EMIT_KAFKA_LUA));
            set_sink_config_string(&mut document, "kafka-primary", "behavior", "delay-shutdown")?;
            revision.tenon_document_json =
                serde_json::to_string(&document).map_err(io::Error::other)?;
            let revision =
                revision_from_bootstrap(&bootstrap, RevisionMutation::LuaSourceAndSinkConfig)?;
            Ok(vec![bootstrap, revision])
        }
        Scenario::LuaPreparationFailureWithSourceConfig => {
            let revision = revision_from_bootstrap(
                &bootstrap,
                RevisionMutation::LuaPreparationFailureWithSourceConfig,
            )?;
            Ok(vec![bootstrap, revision])
        }
        Scenario::UnparseableRevisionAfterBootstrap => {
            let mut revision = revision_from_bootstrap(&bootstrap, RevisionMutation::Equivalent)?;
            let Some(runner_to_pipeline::Message::RevisionPlan(plan)) = &mut revision.message
            else {
                return Err(io::Error::other(
                    "Revision vector has the wrong message kind",
                ));
            };
            plan.tenon_document_json = String::from("{");
            Ok(vec![bootstrap, revision])
        }
        Scenario::SourceQueueDeliveryRevisionAfterBootstrap => {
            set_bootstrap_lua_source(&mut bootstrap, ALWAYS_EMIT_KAFKA_LUA)?;
            let revision =
                revision_from_bootstrap(&bootstrap, RevisionMutation::SourceQueueAndDelivery)?;
            Ok(vec![bootstrap, revision])
        }
        Scenario::SourceProgramContractRevisionAfterBootstrap => {
            set_bootstrap_lua_source(&mut bootstrap, ALWAYS_EMIT_KAFKA_LUA)?;
            let revision =
                revision_from_bootstrap(&bootstrap, RevisionMutation::SourceProgramAndContract)?;
            Ok(vec![bootstrap, revision])
        }
        Scenario::BootstrapMissingEnvironment => {
            let mut bootstrap = bootstrap;
            let Some(runner_to_pipeline::Message::Bootstrap(payload)) = &mut bootstrap.message
            else {
                return Err(io::Error::other(
                    "Bootstrap vector has the wrong message kind",
                ));
            };
            payload.environment = None;
            Ok(vec![bootstrap])
        }
        Scenario::DuplicateBootstrap => Ok(vec![bootstrap.clone(), bootstrap]),
        Scenario::RevisionBeforeBootstrap => Ok(vec![revision]),
        Scenario::DisconnectBeforeBootstrap => Ok(Vec::new()),
        Scenario::ControlStreamLossAfterBootstrap => Ok(vec![bootstrap]),
        Scenario::KeepStreamOpenAfterBootstrap => Ok(vec![bootstrap]),
    }
}

#[derive(Clone, Copy)]
enum RevisionMutation {
    Equivalent,
    SourceConfig,
    SourceAndSinkConfigs,
    SinkConfig,
    SinkConfigs,
    LuaSource,
    LuaSourceAndSinkConfig,
    LuaPreparationFailureWithSourceConfig,
    SourceQueueAndDelivery,
    SourceProgramAndContract,
}

fn revision_from_bootstrap(
    bootstrap: &RunnerToPipeline,
    change: RevisionMutation,
) -> io::Result<RunnerToPipeline> {
    let Some(runner_to_pipeline::Message::Bootstrap(bootstrap)) = &bootstrap.message else {
        return Err(io::Error::other(
            "Bootstrap vector has the wrong message kind",
        ));
    };
    let mut revision = bootstrap
        .revision_plan
        .clone()
        .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
    revision.document_etag = String::from("document-etag-b");
    let mut document: serde_json::Value =
        serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
    match change {
        RevisionMutation::Equivalent => {}
        RevisionMutation::SourceConfig => {
            document["pluginInstances"]["source"]["config"]["port"] = serde_json::json!(503);
        }
        RevisionMutation::SourceAndSinkConfigs => {
            document["pluginInstances"]["source"]["config"]["port"] = serde_json::json!(503);
            set_sink_config_string(&mut document, "kafka-primary", "cluster", "replacement")?;
            set_sink_config_string(&mut document, "iotdb/secondary", "database", "replacement")?;
        }
        RevisionMutation::SinkConfig => {
            set_sink_config_string(&mut document, "kafka-primary", "cluster", "replacement")?;
        }
        RevisionMutation::SinkConfigs => {
            set_sink_config_string(&mut document, "kafka-primary", "cluster", "replacement")?;
            set_sink_config_string(&mut document, "iotdb/secondary", "database", "replacement")?;
        }
        RevisionMutation::LuaSource => {
            document["flows"]["main"]["process"]["script"] =
                serde_json::Value::String(String::from("function main(event) emit(); emit() end"));
        }
        RevisionMutation::LuaSourceAndSinkConfig => {
            document["flows"]["main"]["process"]["script"] =
                serde_json::Value::String(String::from("function main(event) emit() end"));
            set_sink_config_string(&mut document, "kafka-primary", "cluster", "replacement")?;
        }
        RevisionMutation::LuaPreparationFailureWithSourceConfig => {
            document["pluginInstances"]["source"]["config"]["port"] = serde_json::json!(503);
            document["flows"]["main"]["process"]["script"] =
                serde_json::Value::String(String::from(
                    "error(\"candidate initialization failed\")\nfunction main(event) emit() end",
                ));
        }
        RevisionMutation::SourceQueueAndDelivery => {
            document["flows"]["main"]["parallelism"] = serde_json::json!(0.75);
            document["flows"]["main"]["delivery"] = serde_json::json!("at-most-once");
            document["flows"]["main"]["process"]["script"] = serde_json::Value::String(
                String::from("function main(event) error(\"expected source failure\") end"),
            );
            set_sink_config_string(&mut document, "kafka-primary", "cluster", "replacement")?;
        }
        RevisionMutation::SourceProgramAndContract => {
            document["pluginInstances"]["source"]["programName"] =
                serde_json::Value::String(String::from("com.example.modbus2"));
            document["pluginInstances"]["source"]["exactVersion"] =
                serde_json::Value::String(String::from("2.0.0"));
            document["flows"]["main"]["process"]["script"] = serde_json::Value::String(
                String::from(
                    "local builder = registry:getBuilder(\"com.example.kafka@1.0.0\")\nfunction main(event)\n  if event.payload.revisionLabel ~= \"new-contract\" then error(\"unexpected Source Contract payload\") end\n  emit(builder:build())\nend",
                ),
            );
            let source = revision
                .plugin_programs
                .iter_mut()
                .find(|program| program.plugin_interface == PluginInterface::Source as i32)
                .ok_or_else(|| io::Error::other("Revision Source runtime is missing"))?;
            source.program_name = String::from("com.example.modbus2");
            source.exact_version = String::from("2.0.0");
            let descriptor = descriptor_with_source_revision_field(&source.payload_descriptor_set)?;
            source.payload_descriptor_set = descriptor;
        }
    }
    revision.tenon_document_json = serde_json::to_string(&document).map_err(io::Error::other)?;
    Ok(RunnerToPipeline {
        message: Some(runner_to_pipeline::Message::RevisionPlan(revision)),
    })
}

fn sink_config_revision(
    bootstrap: &RunnerToPipeline,
    document_etag: &str,
    cluster: &str,
) -> io::Result<RunnerToPipeline> {
    let Some(runner_to_pipeline::Message::Bootstrap(bootstrap)) = &bootstrap.message else {
        return Err(io::Error::other(
            "Bootstrap vector has the wrong message kind",
        ));
    };
    let mut revision = bootstrap
        .revision_plan
        .clone()
        .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
    revision.document_etag = document_etag.to_owned();
    let mut document: serde_json::Value =
        serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
    set_sink_config_string(&mut document, "kafka-primary", "cluster", cluster)?;
    revision.tenon_document_json = serde_json::to_string(&document).map_err(io::Error::other)?;
    Ok(RunnerToPipeline {
        message: Some(runner_to_pipeline::Message::RevisionPlan(revision)),
    })
}

fn set_sink_config_string(
    document: &mut serde_json::Value,
    sink_id: &str,
    field: &str,
    value: &str,
) -> io::Result<()> {
    let instance = document["pluginInstances"]
        .get_mut(sink_id)
        .ok_or_else(|| io::Error::other("Requested Sink Instance is missing"))?;
    instance["config"]
        .as_object_mut()
        .ok_or_else(|| io::Error::other("Instance config is not an object"))?
        .remove("behavior");
    instance["config"][field] = serde_json::Value::String(value.to_owned());
    Ok(())
}

fn set_bootstrap_lua_source(bootstrap: &mut RunnerToPipeline, source: &str) -> io::Result<()> {
    let Some(runner_to_pipeline::Message::Bootstrap(bootstrap)) = &mut bootstrap.message else {
        return Err(io::Error::other(
            "Bootstrap vector has the wrong message kind",
        ));
    };
    let revision = bootstrap
        .revision_plan
        .as_mut()
        .ok_or_else(|| io::Error::other("Bootstrap revision is missing"))?;
    let mut document: serde_json::Value =
        serde_json::from_str(&revision.tenon_document_json).map_err(io::Error::other)?;
    document["flows"]["main"]["process"]["script"] = serde_json::Value::String(source.to_owned());
    revision.tenon_document_json = serde_json::to_string(&document).map_err(io::Error::other)?;
    Ok(())
}

fn descriptor_with_source_revision_field(descriptor_bytes: &[u8]) -> io::Result<Vec<u8>> {
    let mut descriptor_set =
        FileDescriptorSet::decode(descriptor_bytes).map_err(io::Error::other)?;
    let logical_file = descriptor_set
        .file
        .iter_mut()
        .find(|file| file.name.as_deref() == Some("source_record_payload.proto"))
        .ok_or_else(|| io::Error::other("Payload Contract logical file is missing"))?;
    let root = logical_file
        .message_type
        .iter_mut()
        .find(|message| message.name.as_deref() == Some("SourceRecordPayload"))
        .ok_or_else(|| io::Error::other("Source Payload Contract root message is missing"))?;
    root.field.push(FieldDescriptorProto {
        name: Some(String::from("revision_label")),
        number: Some(100),
        label: Some(prost_types::field_descriptor_proto::Label::Optional as i32),
        r#type: Some(prost_types::field_descriptor_proto::Type::String as i32),
        json_name: Some(String::from("revisionLabel")),
        ..FieldDescriptorProto::default()
    });
    Ok(descriptor_set.encode_to_vec())
}

fn startup_bootstrap(fixture: &StartupFixture) -> io::Result<RunnerToPipeline> {
    let document = serde_json::json!({
        "specVersion":"1","id":"pipeline-a",
        "pluginInstances": {
            "source": {"programName":"com.example.modbus","exactVersion":"1.0.0","config":{"port":502}},
            "kafka-primary":{"programName":"com.example.kafka","exactVersion":"1.0.0","config":{"cluster":"primary"}},
            "kafka-standby":{"programName":"com.example.kafka","exactVersion":"1.0.0","config":{"cluster":"standby"}},
            "iotdb/secondary":{"programName":"com.example.iotdb","exactVersion":"2.0.0","config":{"database":"factory"}}
        },
        "flows":{"main":{
            "maxPendingRecords":4,"maxRecordBytes":1024,
            "parallelism": 0.5, "source": "source",
            "process":{"script":"function main(event) emit() end"},
            "sinks":["kafka-primary","kafka-standby","iotdb/secondary"]
        }}
    });
    let mut programs = Vec::new();
    for (name, version, interface) in [
        ("com.example.modbus", "1.0.0", PluginInterface::Source),
        ("com.example.kafka", "1.0.0", PluginInterface::Sink),
        ("com.example.iotdb", "2.0.0", PluginInterface::Sink),
    ] {
        let contract_interface = match interface {
            PluginInterface::Source => plugin_fixture::PluginInterface::Source,
            PluginInterface::Sink => plugin_fixture::PluginInterface::Sink,
            PluginInterface::SourceAndSink => unreachable!("The fixture is directional"),
        };
        let mut command = vec!["./plugin.sh".to_owned()];
        command.extend(plugin_fixture::controlled_program_command(
            contract_interface,
        )?);
        programs.push(PluginProgramRuntime {
            program_name: name.into(),
            exact_version: version.into(),
            program_directory: fixture.plugin_directory.clone(),
            command,
            plugin_interface: interface as i32,
            payload_descriptor_set: plugin_fixture::program_payload_descriptor(contract_interface),
        });
    }
    Ok(RunnerToPipeline {
        message: Some(runner_to_pipeline::Message::Bootstrap(PipelineBootstrap {
            revision_plan: Some(PipelineRevisionPlan {
                document_etag: "document-etag-a".into(),
                tenon_document_json: serde_json::to_string(&document)?,
                plugin_programs: programs,
            }),
            environment: Some(PipelineEnvironment {
                metrics_node_id: None,
                pipeline_working_directory: fixture.pipeline_working_directory.clone(),

                lua_limits: Some(LuaLimits {
                    cpu_time_limit_ms: 100,
                    memory_limit_bytes: 16_777_216,
                }),
                retry_backoff: Some(RetryBackoff {
                    initial_delay_ms: 100,
                    maximum_delay_ms: 30_000,
                }),
                reconfigure_timeout_ms: 30_000,
                available_cpu_count: 4,
            }),
        })),
    })
}

fn assert_expected_attach(envelope: PipelineToRunner) -> io::Result<()> {
    let Some(pipeline_to_runner::Message::Attach(attach)) = envelope.message else {
        return Err(io::Error::other("Pipeline did not send Attach first"));
    };
    assert_eq!(attach.launch_id, b"launch-a");
    Ok(())
}

fn status_snapshot(envelope: PipelineToRunner) -> io::Result<PipelineStatusSnapshot> {
    let Some(pipeline_to_runner::Message::StatusSnapshot(status)) = envelope.message else {
        return Err(io::Error::other(
            "Pipeline did not publish a status snapshot",
        ));
    };
    Ok(status)
}

fn all_plugins_are_running(status: &PipelineStatusSnapshot) -> bool {
    status
        .plugin_instances
        .iter()
        .all(|instance| instance.state == PluginInstanceState::Running as i32)
}

fn submit_ingress(writer: &mut QueueWriter, record_id: u64) -> io::Result<()> {
    submit_ingress_payload(writer, record_id, Vec::new())
}

fn submit_ingress_payload(
    writer: &mut QueueWriter,
    record_id: u64,
    payload: Vec<u8>,
) -> io::Result<()> {
    let encoded = IngressRecord {
        record_id,
        payload: payload.into(),
    }
    .encode_to_vec();
    match writer
        .try_write_with(
            encoded.len(),
            |destination| std::io::Write::write_all(destination, &encoded),
            || {},
        )
        .map_err(io::Error::other)?
    {
        WriteOutcome::Committed(_) => Ok(()),
        WriteOutcome::Full => Err(io::Error::other("Submission Queue was unexpectedly full")),
    }
}

fn source_revision_payload(value: &str) -> io::Result<Vec<u8>> {
    let length = u8::try_from(value.len())
        .map_err(|_| io::Error::other("Source revision fixture is too large"))?;
    let mut payload = Vec::with_capacity(value.len() + 3);
    payload.extend_from_slice(&[0xa2, 0x06, length]);
    payload.extend_from_slice(value.as_bytes());
    Ok(payload)
}

fn assert_ingress_completion(reader: &mut QueueReader, record_id: u64) -> io::Result<()> {
    let encoded = read_queue_record(reader)?;
    let completion = IngressCompletion::decode(encoded.as_slice()).map_err(io::Error::other)?;
    assert_eq!(completion.record_id, record_id);
    assert_eq!(completion.status, IngressCompletionStatus::Ok as i32);
    reader.release(1).map_err(io::Error::other)
}

fn read_egress_record(reader: &mut QueueReader) -> io::Result<EgressRecord> {
    EgressRecord::decode(read_queue_record(reader)?.as_slice()).map_err(io::Error::other)
}

fn trigger_egress_worker_failure(
    pipeline_directory: &Path,
    before_corruption: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let primary_directory = sink_directory_for_config(
        pipeline_directory,
        r#"{"behavior":"delay-shutdown","cluster":"primary"}"#,
    )?;
    let standby_directory =
        sink_directory_for_config(pipeline_directory, r#"{"cluster":"standby"}"#)?;
    let mut first_submission_writer =
        source_submission_writer(pipeline_directory, 0).map_err(io::Error::other)?;
    let mut second_submission_writer =
        source_submission_writer(pipeline_directory, 1).map_err(io::Error::other)?;
    let mut primary_reader =
        sink_egress_reader(pipeline_directory, &primary_directory, 0).map_err(io::Error::other)?;
    let mut standby_reader =
        sink_egress_reader(pipeline_directory, &standby_directory, 0).map_err(io::Error::other)?;

    submit_ingress(&mut first_submission_writer, 1)?;
    let primary_record = read_egress_record(&mut primary_reader)?;
    assert_eq!(read_egress_record(&mut standby_reader)?, primary_record);
    before_corruption(&primary_directory)?;
    corrupt_queue_release_position(&egress_queue_path(&primary_directory, "main", 1))?;
    submit_ingress(&mut second_submission_writer, 2)
}

fn read_queue_record(reader: &mut QueueReader) -> io::Result<Vec<u8>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match reader.try_read().map_err(io::Error::other)? {
            ReadOutcome::Record(record) => return Ok(record.payload().to_vec()),
            ReadOutcome::Empty if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            ReadOutcome::Empty => return Err(io::Error::other("Queue record did not arrive")),
        }
    }
}

fn sink_directory_for_config(pipeline_directory: &Path, config: &str) -> io::Result<PathBuf> {
    let deadline = Instant::now() + Duration::from_secs(5);
    let sinks_directory = pipeline_directory.join("instances");
    loop {
        let entries = std::fs::read_dir(&sinks_directory).map_err(|source| {
            io::Error::new(
                source.kind(),
                format!(
                    "Sink directory could not be listed: {}; source: {source}",
                    sinks_directory.display()
                ),
            )
        })?;
        for entry in entries {
            let directory = entry
                .map_err(|source| {
                    io::Error::new(
                        source.kind(),
                        format!(
                            "Sink directory entry could not be read: {}; source: {source}",
                            sinks_directory.display()
                        ),
                    )
                })?
                .path();
            match std::fs::read_to_string(directory.join("config.received")) {
                Ok(source) if source == config => return Ok(directory),
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(source) => {
                    return Err(io::Error::new(
                        source.kind(),
                        format!(
                            "Sink config observation failed: {}; source: {source}",
                            directory.display()
                        ),
                    ));
                }
            }
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other(format!(
                "Sink working directory was not found for config: {config}"
            )));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

/// The own-loop doorbell every Submission Queue of a Source publishes: the
/// Source's single Submission loop parks on this ordinal in `source/loops.bells`.
const SUBMISSION_LOOP_SLOT: u32 = 0;
/// The own-loop doorbell every Completion Queue of a Source publishes: the
/// Source's single Completion loop parks on this ordinal in `source/loops.bells`.
const COMPLETION_LOOP_SLOT: u32 = 1;

/// Opens the Source instance's writer of one Submission Queue.
///
/// The test plays the Source process, so it binds the same two files the real
/// Instance does: the `source/loops.bells` region its loop parks on, and the
/// Channel region of the Flow its commit rings. The Source owns one own-loop
/// doorbell per loop, not per Channel, so every Channel's Submission publishes
/// the one Submission slot.
fn source_submission_writer(pipeline_directory: &Path, channel: u32) -> io::Result<QueueWriter> {
    let source = instance_directory(pipeline_directory, "source").join("source");
    open_writer(
        &source.join(format!("submission-{channel}.queue")),
        &loops_bell_path(&source),
        SUBMISSION_LOOP_SLOT,
        &flow_channel_bell_path(pipeline_directory, "main"),
    )
}

/// Opens the Source instance's reader of one Completion Queue, the Completion loop's doorbell.
fn source_completion_reader(pipeline_directory: &Path, channel: u32) -> io::Result<QueueReader> {
    let source = instance_directory(pipeline_directory, "source").join("source");
    open_reader(
        &source.join(format!("completion-{channel}.queue")),
        &loops_bell_path(&source),
        COMPLETION_LOOP_SLOT,
        &flow_channel_bell_path(pipeline_directory, "main"),
    )
}

/// Opens one Sink instance's reader of an Egress Queue of the Flow `main`.
///
/// The test plays the Sink process, so it binds the `sink/loops.bells` slot the
/// real Instance parks on. A Sink reads every one of its Egress Queues from a
/// single loop, so that slot is 0 for each of them.
fn sink_egress_reader(
    pipeline_directory: &Path,
    sink_instance_directory: &Path,
    channel: u32,
) -> io::Result<QueueReader> {
    open_reader(
        &egress_queue_path(sink_instance_directory, "main", channel),
        &loops_bell_path(&sink_instance_directory.join("sink")),
        0,
        &flow_channel_bell_path(pipeline_directory, "main"),
    )
}

fn instance_directory(pipeline_directory: &Path, id: &str) -> PathBuf {
    pipeline_directory.join("instances").join(
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(id.as_bytes())),
    )
}

fn plugin_working_directories(pipeline_directory: &Path) -> io::Result<Vec<PathBuf>> {
    let mut directories = std::fs::read_dir(pipeline_directory.join("instances"))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<io::Result<Vec<_>>>()?;
    directories.sort_unstable();
    Ok(directories)
}

fn plugin_process_snapshot(pipeline_directory: &Path) -> io::Result<Vec<(PathBuf, u32)>> {
    plugin_working_directories(pipeline_directory)?
        .into_iter()
        .map(|directory| {
            let process_id = read_process_id_with_timeout(&directory.join("process.pid"))?;
            Ok((directory, process_id))
        })
        .collect()
}

fn process_id_for_directory(processes: &[(PathBuf, u32)], directory: &Path) -> io::Result<u32> {
    processes
        .iter()
        .find(|(candidate, _)| candidate == directory)
        .map(|(_, process_id)| *process_id)
        .ok_or_else(|| io::Error::other("Plugin process was not found"))
}

fn assert_processes_alive(processes: &[(PathBuf, u32)]) -> io::Result<()> {
    for (_, process_id) in processes {
        if !process_exists(*process_id)? {
            return Err(io::Error::other("Plugin process exited unexpectedly"));
        }
    }
    Ok(())
}

fn assert_all_plugins_started_once(pipeline_directory: &Path) -> io::Result<()> {
    for directory in plugin_working_directories(pipeline_directory)? {
        assert_eq!(
            read_text_with_timeout(&directory.join("starts.received"))?,
            "started\n"
        );
    }
    Ok(())
}

fn queue_file_snapshot(pipeline_directory: &Path) -> io::Result<Vec<(PathBuf, u64)>> {
    let mut paths =
        queue_file_paths(&instance_directory(pipeline_directory, "source").join("source"))?;
    for directory in plugin_working_directories(pipeline_directory)? {
        let flow_directory = egress_queue_path(&directory, "main", 0)
            .parent()
            .ok_or_else(|| io::Error::other("Egress path has no parent"))?
            .to_owned();
        if flow_directory.exists() {
            paths.extend(queue_file_paths(&flow_directory)?);
        }
    }
    paths.sort_unstable();
    paths
        .into_iter()
        .map(|path| Ok((path.clone(), std::fs::metadata(path)?.ino())))
        .collect()
}

fn queue_file_paths(directory: &Path) -> io::Result<Vec<PathBuf>> {
    std::fs::read_dir(directory)?
        .filter_map(|entry| match entry {
            Ok(entry)
                if entry
                    .path()
                    .extension()
                    .is_some_and(|value| value == "queue") =>
            {
                Some(Ok(entry.path()))
            }
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn hold_queue_file_identities(directory: &Path) -> io::Result<Vec<std::fs::File>> {
    queue_file_paths(directory)?
        .into_iter()
        .map(std::fs::File::open)
        .collect()
}

fn assert_source_queues_replaced(
    initial: &[(PathBuf, u64)],
    revised: &[(PathBuf, u64)],
    source_directory: &Path,
    target_parallelism: usize,
) -> io::Result<()> {
    let initial_egress = initial
        .iter()
        .filter(|(path, _)| !path.starts_with(source_directory))
        .cloned()
        .collect::<Vec<_>>();
    let revised_egress = revised
        .iter()
        .filter(|(path, _)| !path.starts_with(source_directory))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(initial_egress.len(), 6);
    assert_eq!(revised_egress.len(), target_parallelism * 3);
    for queue in &initial_egress {
        assert!(revised_egress.contains(queue));
    }
    for (path, _) in &initial_egress {
        let flow_directory = path
            .parent()
            .ok_or_else(|| io::Error::other("Egress path has no parent"))?;
        for channel in 0..target_parallelism {
            assert!(revised_egress.iter().any(|(candidate, _)| candidate
                == &flow_directory.join(format!("egress-{channel}.queue"))));
        }
    }

    let revised_source = revised
        .iter()
        .filter(|(path, _)| path.starts_with(source_directory))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(revised_source.len(), target_parallelism * 2);
    for (path, revised_inode) in &revised_source {
        if let Some((_, initial_inode)) = initial.iter().find(|(candidate, _)| candidate == path) {
            assert_ne!(
                revised_inode,
                initial_inode,
                "Replacement Source Queue reused the old file identity: {}",
                path.display()
            );
        }
    }
    for index in 0..target_parallelism {
        for file_name in [
            format!("submission-{index}.queue"),
            format!("completion-{index}.queue"),
        ] {
            let path = source_directory.join("source").join(file_name);
            if !revised_source
                .iter()
                .any(|(candidate, _)| candidate == &path)
            {
                return Err(io::Error::other(format!(
                    "Replacement Source Queue is missing: {}",
                    path.display()
                )));
            }
        }
    }
    Ok(())
}

fn corrupt_queue_release_position(path: &Path) -> io::Result<()> {
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    file.write_all_at(&1_u64.to_le_bytes(), IPC_QUEUE_RELEASE_POSITION_OFFSET)
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn assert_pipeline_group_force_killed(output: &Output) -> io::Result<()> {
    if output.status.signal() == Some(Signal::KILL.as_raw()) {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "Pipeline did not terminate itself with the process-group kill signal: {:?}; stderr: {}",
            output.status,
            stderr(output)
        )))
    }
}

fn read_text_with_timeout(path: &std::path::Path) -> io::Result<String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match std::fs::read_to_string(path) {
            Ok(source) if !source.is_empty() => return Ok(source),
            Ok(source) if Instant::now() >= deadline => return Ok(source),
            Ok(_) => thread::sleep(Duration::from_millis(10)),
            Err(error) if error.kind() == io::ErrorKind::NotFound && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(source) => {
                return Err(io::Error::new(
                    source.kind(),
                    format!(
                        "Test file could not be read: {}; source: {source}",
                        path.display()
                    ),
                ));
            }
        }
    }
}

fn read_process_id_with_timeout(path: &Path) -> io::Result<u32> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match std::fs::read_to_string(path) {
            Ok(source) => match source.trim().parse::<u32>() {
                Ok(process_id) => return Ok(process_id),
                Err(_) if Instant::now() < deadline => {}
                Err(error) => return Err(io::Error::other(error)),
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound && Instant::now() < deadline => {}
            Err(source) => {
                return Err(io::Error::new(
                    source.kind(),
                    format!(
                        "Test file did not become readable: {}; source: {source}",
                        path.display()
                    ),
                ));
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_until_text_equals(path: &Path, expected: &str) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match std::fs::read_to_string(path) {
            Ok(source) if source == expected => return Ok(()),
            Ok(_) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Ok(source) => {
                return Err(io::Error::other(format!(
                    "Test file did not reach the expected content: {}; actual: {source:?}",
                    path.display()
                )));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(source) => {
                return Err(io::Error::new(
                    source.kind(),
                    format!(
                        "Test file did not become readable: {}; source: {source}",
                        path.display()
                    ),
                ));
            }
        }
    }
}

fn wait_until_file_exists(path: &std::path::Path) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.is_file() {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("Test file was not created: {}", path.display()),
            ));
        }
        thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

fn directory_mode(path: &std::path::Path) -> io::Result<u32> {
    Ok(std::fs::metadata(path)?.permissions().mode() & 0o777)
}

fn wait_until_process_exits(process_id: u32) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if !process_exists(process_id)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other("Plugin process did not exit"));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_until_processes_exit(processes: &[(PathBuf, u32)]) -> io::Result<()> {
    for (_, process_id) in processes {
        wait_until_process_exits(*process_id)?;
    }
    Ok(())
}

fn process_exists(process_id: u32) -> io::Result<bool> {
    let output = Command::new("ps")
        .args(["-o", "stat=", "-p", &process_id.to_string()])
        .stderr(Stdio::null())
        .output()?;
    if !output.status.success() {
        return Ok(false);
    }

    let state = String::from_utf8(output.stdout).map_err(io::Error::other)?;
    Ok(state
        .split_whitespace()
        .next()
        .is_some_and(|state| !state.starts_with('Z')))
}

fn terminate_and_reap(child: &mut Child) -> io::Result<()> {
    let process_group = i32::try_from(child.id())
        .map_err(|_| io::Error::other("Pipeline process id does not fit a process group id"))?;
    let process_group = format!("-{process_group}");
    let kill = Command::new("/bin/kill")
        .args(["-KILL", "--", &process_group])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    if matches!(kill, Ok(status) if status.success()) {
        return child.wait().map(|_| ());
    }

    let kill_error = match kill {
        Ok(status) => io::Error::other(format!(
            "Pipeline process group termination exited with status {status}"
        )),
        Err(error) => error,
    };
    match child.try_wait() {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(kill_error),
        Err(status_error) => Err(io::Error::new(
            status_error.kind(),
            format!(
                "Pipeline process group termination failed: {kill_error}; status check failed: {status_error}"
            ),
        )),
    }
}
