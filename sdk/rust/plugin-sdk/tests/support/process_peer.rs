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

//! Shared child-process, lifecycle server and Queue fixtures for SDK capability tests.

#![allow(dead_code, reason = "Each test target uses its own capability subset")]

use tenon_ipc::bell::{BellRegion, LoopBell};
use tenon_ipc::queue::{QueueReader, QueueWriter, WriteOutcome};

use base64::Engine;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tenon_plugin_sdk::repository_test_support::{LOOPS_BELL_FILE_NAME, plugin};
use tenon_plugin_sdk::{Error, FlowChannel};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::UnixListener;
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::{Request, Response, Status, Streaming};

pub(super) const DEADLINE: Duration = Duration::from_secs(10);

/// The Flow a Source-only launch writes into.
///
/// Only the fixture needs a name here: a real Source learns the Flow's Channel
/// Region from its startup record, never from the id.
const SOURCE_FLOW: &str = "main";

/// The parallelism one launched Source was configured with.
fn source_parallelism(config: &serde_json::Value) -> Result<u32, Error> {
    u32::try_from(config["parallelism"].as_u64().unwrap_or(1))
        .map_err(|_| "parallelism does not fit u32".into())
}
type Reply = Result<plugin::PipelineToPlugin, Status>;
type Connection = (Streaming<plugin::PluginToPipeline>, mpsc::Sender<Reply>);

pub(super) enum QueueOccupancy {
    Empty,
    Full,
}
pub(super) enum ExpectedExit {
    Success,
    Failure,
    Forced,
}

/// How one launch obtains the Sink inputs the Pipeline would hand its Side.
///
/// Every input names the Channel Bell Region its Flow's loop parks in, and that
/// Region is named by the working directory the Side runs in. A launch that owns
/// its directory therefore cannot be handed finished identities.
enum Channels {
    Given(Vec<FlowChannel>),
    Named(fn(&Path) -> Vec<FlowChannel>),
}

impl Channels {
    fn resolve(&self, working: &Path) -> Vec<FlowChannel> {
        match self {
            Self::Given(channels) => channels.clone(),
            Self::Named(build) => build(working),
        }
    }
}

/// The Region record this fixture hands one Flow whose Channel loops it plays.
///
/// Only the Pipeline creates this file; a launched Sink learns its absolute path
/// from each input's `channelBellPath`.
pub(super) fn flow_bell_path(working: &Path, flow_id: &str) -> PathBuf {
    working.join("flows").join(flow_id).join("channels.bells")
}

/// Ensures one Bell Region holds exactly `slots` doorbells and returns it open.
///
/// A Region is named by the loop layout that numbers its slots, so one already
/// holding this count is reused and every endpoint that published into it keeps
/// ringing the loops it was bound to. This is the rule the Core runtime applies
/// before it launches a Side, and the fixture follows it so a replacement
/// process reaches the Regions its own earlier writers still ring.
fn ensure_bell_region(path: &Path, slots: u32) -> Result<Arc<BellRegion>, Error> {
    if let Ok(existing) = BellRegion::open(path)
        && existing.slot_count().get() == slots
    {
        return Ok(existing);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    tenon_ipc::bell::create_bell_region(
        path,
        NonZeroU32::new(slots).ok_or("a Bell Region needs a nonzero slot count")?,
        0,
    )?;
    Ok(BellRegion::open(path)?)
}

/// The Bell Regions one launch needs, and the endpoints that ring them.
///
/// A Core runtime creates every one of these before it starts any process: each
/// Flow's Channel Region, where that Flow's Channel loops park, and the launched
/// Interface's own-loop Region. A fixture that opens an endpoint therefore
/// reaches exactly the two addresses the real Channel loop and the real program
/// do.
pub(super) struct Bells {
    /// One Region per Flow this fixture plays Channel loops for.
    flows: BTreeMap<String, Arc<BellRegion>>,
    /// The Flow a launched Source writes into; its Channels are the Source's.
    source_flow: Option<String>,
    /// The launched Source's own-loop Region, when this Interface has a Source.
    source: Option<Arc<BellRegion>>,
    /// The launched Sink's own-loop Region, when this Interface has a Sink.
    sink: Option<Arc<BellRegion>>,
}

impl Bells {
    /// Creates the Regions a launch needs, exactly as the Core runtime does.
    ///
    /// `source_flow` names the Flow a launched Source writes into and how many
    /// Channels it has; `sink_inputs` lists the Channels a launched Sink owns.
    pub(super) fn create(
        working: &Path,
        source_flow: Option<(&str, u32)>,
        sink_inputs: Option<&[FlowChannel]>,
    ) -> Result<Self, Error> {
        let mut flow_slots: BTreeMap<String, u32> = BTreeMap::new();
        if let Some((flow_id, channels)) = source_flow {
            flow_slots.insert(flow_id.to_owned(), channels);
        }
        if let Some(channels) = sink_inputs {
            for channel in channels {
                let slots = channel
                    .channel_id
                    .checked_add(1)
                    .ok_or("a Channel id does not fit a slot count")?;
                flow_slots
                    .entry(channel.flow_id.clone())
                    .and_modify(|existing| *existing = (*existing).max(slots))
                    .or_insert(slots);
            }
        }
        let mut flows = BTreeMap::new();
        for (flow_id, slots) in flow_slots {
            let region = ensure_bell_region(&flow_bell_path(working, &flow_id), slots)?;
            flows.insert(flow_id, region);
        }
        let source = source_flow
            .map(|_| ensure_bell_region(&working.join("source").join(LOOPS_BELL_FILE_NAME), 2))
            .transpose()?;
        let sink = sink_inputs
            .map(|_| ensure_bell_region(&working.join("sink").join(LOOPS_BELL_FILE_NAME), 1))
            .transpose()?;
        Ok(Self {
            flows,
            source_flow: source_flow.map(|(flow_id, _)| flow_id.to_owned()),
            source,
            sink,
        })
    }

    /// The Region one Flow's Channel loops park in.
    fn flow(&self, flow_id: &str) -> Result<&Arc<BellRegion>, Error> {
        self.flows
            .get(flow_id)
            .ok_or_else(|| "missing Flow Bell Region".into())
    }

    /// The doorbell of one Channel loop the fixture plays for a launched Source.
    fn source_channel(&self, channel: u32) -> Result<Arc<LoopBell>, Error> {
        let flow = self
            .source_flow
            .as_ref()
            .ok_or("this Interface owns no Source loop")?;
        Ok(self.flow(flow)?.loop_bell(channel)?)
    }

    /// The launched Source's own-loop Region.
    fn source_region(&self) -> Result<&Arc<BellRegion>, Error> {
        self.source
            .as_ref()
            .ok_or_else(|| "this Interface owns no Source loop".into())
    }

    /// The launched Sink's own-loop Region.
    fn sink_region(&self) -> Result<&Arc<BellRegion>, Error> {
        self.sink
            .as_ref()
            .ok_or_else(|| "this Interface owns no Sink loop".into())
    }

    /// Opens one Submission Queue for reading, as its Flow Channel loop does.
    pub(super) fn submission_reader(
        &self,
        working: &Path,
        channel: u32,
    ) -> Result<QueueReader, Error> {
        Ok(QueueReader::open(
            working.join(format!("source/submission-{channel}.queue")),
            self.source_channel(channel)?,
            Arc::clone(self.source_region()?),
        )?)
    }

    /// Opens one Submission Queue for writing, as the fixture does when it
    /// fills the Queue before the Source starts.
    pub(super) fn submission_writer(
        &self,
        working: &Path,
        channel: u32,
    ) -> Result<QueueWriter, Error> {
        Ok(QueueWriter::open(
            working.join(format!("source/submission-{channel}.queue")),
            self.source_channel(channel)?,
            Arc::clone(self.source_region()?),
        )?)
    }

    /// Opens one Completion Queue for writing, as its Flow Channel loop does.
    pub(super) fn completion_writer(
        &self,
        working: &Path,
        channel: u32,
    ) -> Result<QueueWriter, Error> {
        Ok(QueueWriter::open(
            working.join(format!("source/completion-{channel}.queue")),
            self.source_channel(channel)?,
            Arc::clone(self.source_region()?),
        )?)
    }

    /// Opens one Egress Queue the way its Flow Channel loop does.
    pub(super) fn egress(
        &self,
        working: &Path,
        channel: &FlowChannel,
    ) -> Result<QueueWriter, Error> {
        Ok(QueueWriter::open(
            sink_queue(working, channel),
            self.flow(&channel.flow_id)?.loop_bell(channel.channel_id)?,
            Arc::clone(self.sink_region()?),
        )?)
    }
}

/// Creates every Bell Region a Sink launch into `working` needs.
///
/// A fixture that opens Egress endpoints before any process exists reaches the
/// same addresses the launched Side will: a Region already holding the right
/// slot count is reused unchanged.
pub(super) fn sink_bells(working: &Path, channels: &[FlowChannel]) -> Result<Bells, Error> {
    Bells::create(working, None, Some(channels))
}

/// Creates every Bell Region a Source-and-sink launch into `working` needs.
pub(super) fn shared_bells(working: &Path, channels: &[FlowChannel]) -> Result<Bells, Error> {
    let flow_id = channels
        .first()
        .ok_or("a Source-and-sink launch needs one input Channel")?
        .flow_id
        .clone();
    let count = u32::try_from(
        channels
            .iter()
            .filter(|channel| channel.flow_id == flow_id)
            .count(),
    )
    .map_err(|_| "Channel count does not fit u32")?;
    Bells::create(working, Some((&flow_id, count)), Some(channels))
}

pub(super) struct Peer {
    pub(super) directory: tempfile::TempDir,
    /// The directory the launched program was told to work in.
    pub(super) working: PathBuf,
    /// The Bell Regions this launch created, and the loops they hold.
    pub(super) bells: Bells,
    /// The Sink inputs this launch was given, in the Side's own order.
    pub(super) sink_channels: Vec<FlowChannel>,
    pub(super) child: Child,
    pub(super) stdin: Option<ChildStdin>,
    pub(super) stdout: Lines<BufReader<ChildStdout>>,
    pub(super) observed: Vec<String>,
    pub(super) incoming: Streaming<plugin::PluginToPipeline>,
    pub(super) outgoing: Option<mpsc::Sender<Reply>>,
    pub(super) stop: Option<oneshot::Sender<()>>,
    pub(super) server: JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Peer {
    /// Opens one Sink input's Egress Queue the way its Flow Channel loop does.
    pub(super) fn egress(&self, input: usize) -> Result<QueueWriter, Error> {
        self.bells.egress(&self.working, self.sink_input(input)?)
    }

    /// The Queue one Sink input reads, in this Side's own input order.
    pub(super) fn egress_path(&self, input: usize) -> Result<PathBuf, Error> {
        Ok(sink_queue(&self.working, self.sink_input(input)?))
    }

    fn sink_input(&self, input: usize) -> Result<&FlowChannel, Error> {
        self.sink_channels
            .get(input)
            .ok_or_else(|| "this Interface owns no such input".into())
    }

    pub(super) async fn start(
        config: serde_json::Value,
        occupancy: QueueOccupancy,
    ) -> Result<Self, Error> {
        Self::launch(config, Program::Source(occupancy), None, None).await
    }

    pub(super) async fn start_source_binary(
        config: serde_json::Value,
        executable: &Path,
    ) -> Result<Self, Error> {
        Self::launch(
            config,
            Program::Source(QueueOccupancy::Empty),
            None,
            Some(executable),
        )
        .await
    }

    pub(super) async fn start_sink(
        config: serde_json::Value,
        channels: fn(&Path) -> Vec<FlowChannel>,
    ) -> Result<Self, Error> {
        Self::launch(config, Program::Sink(Channels::Named(channels)), None, None).await
    }

    pub(super) async fn start_sink_in(
        config: serde_json::Value,
        channels: Vec<FlowChannel>,
        working_directory: &Path,
    ) -> Result<Self, Error> {
        Self::launch(
            config,
            Program::Sink(Channels::Given(channels)),
            Some(working_directory),
            None,
        )
        .await
    }

    pub(super) async fn start_source_and_sink(
        config: serde_json::Value,
        channels: fn(&Path) -> Vec<FlowChannel>,
    ) -> Result<Self, Error> {
        Self::launch(
            config,
            Program::SourceAndSink(Channels::Named(channels)),
            None,
            None,
        )
        .await
    }

    pub(super) async fn start_source_and_sink_in(
        config: serde_json::Value,
        channels: Vec<FlowChannel>,
        working_directory: &Path,
    ) -> Result<Self, Error> {
        Self::launch(
            config,
            Program::SourceAndSink(Channels::Given(channels)),
            Some(working_directory),
            None,
        )
        .await
    }

    pub(super) async fn start_source_and_sink_binary(
        config: serde_json::Value,
        channels: fn(&Path) -> Vec<FlowChannel>,
        executable: &Path,
    ) -> Result<Self, Error> {
        Self::launch(
            config,
            Program::SourceAndSink(Channels::Named(channels)),
            None,
            Some(executable),
        )
        .await
    }

    pub(super) async fn message(&mut self) -> Result<plugin::PluginToPipeline, Error> {
        timeout(DEADLINE, self.incoming.message())
            .await??
            .ok_or_else(|| "control stream closed unexpectedly".into())
    }

    pub(super) async fn ready(&mut self) -> Result<(), Error> {
        assert!(matches!(
            self.message().await?.message,
            Some(plugin::plugin_to_pipeline::Message::Ready(_))
        ));
        Ok(())
    }

    pub(super) async fn quiesce(&self) -> Result<(), Error> {
        self.command(plugin::pipeline_to_plugin::Message::QuiesceSource(
            plugin::QuiesceSource {},
        ))
        .await
    }

    pub(super) async fn shutdown(&self) -> Result<(), Error> {
        self.command(plugin::pipeline_to_plugin::Message::Shutdown(
            plugin::Shutdown {},
        ))
        .await
    }

    pub(super) async fn command(
        &self,
        message: plugin::pipeline_to_plugin::Message,
    ) -> Result<(), Error> {
        self.outgoing
            .as_ref()
            .ok_or("response stream closed")?
            .send(Ok(plugin::PipelineToPlugin {
                message: Some(message),
            }))
            .await?;
        Ok(())
    }

    pub(super) async fn quiesced(&mut self) -> Result<(), Error> {
        assert!(matches!(
            self.message().await?.message,
            Some(plugin::plugin_to_pipeline::Message::SourceQuiesced(_))
        ));
        Ok(())
    }

    pub(super) async fn event(&mut self, expected: &str) -> Result<(), Error> {
        loop {
            let line = timeout(DEADLINE, self.stdout.next_line())
                .await??
                .ok_or("business event stream closed")?;
            let matches = line == expected;
            self.observed.push(line);
            if matches {
                return Ok(());
            }
        }
    }

    pub(super) fn reader(&self) -> Result<QueueReader, Error> {
        self.submission(0)
    }

    pub(super) fn writer(&self) -> Result<QueueWriter, Error> {
        self.completion(0)
    }

    /// Opens one Submission Queue the way its Flow Channel loop does.
    pub(super) fn submission(&self, channel: u32) -> Result<QueueReader, Error> {
        self.bells.submission_reader(&self.working, channel)
    }

    /// Opens one Completion Queue the way its Flow Channel loop does.
    pub(super) fn completion(&self, channel: u32) -> Result<QueueWriter, Error> {
        self.bells.completion_writer(&self.working, channel)
    }

    pub(super) async fn finish(mut self) -> Result<Vec<String>, Error> {
        self.shutdown().await?;
        assert!(timeout(DEADLINE, self.incoming.message()).await??.is_none());
        self.outgoing.take();
        self.exit(ExpectedExit::Success).await
    }

    pub(super) async fn exit(mut self, expected: ExpectedExit) -> Result<Vec<String>, Error> {
        let status = timeout(DEADLINE, self.child.wait()).await??;
        match expected {
            ExpectedExit::Success => assert_eq!(status.code(), Some(0), "{status}"),
            ExpectedExit::Failure => assert_eq!(status.code(), Some(1), "{status}"),
            ExpectedExit::Forced => assert!(!status.success(), "{status}"),
        }
        self.outgoing.take();
        while let Some(line) = timeout(DEADLINE, self.stdout.next_line()).await?? {
            self.observed.push(line)
        }
        if let Some(mut stderr) = self.child.stderr.take() {
            let mut bytes = Vec::new();
            stderr.read_to_end(&mut bytes).await?;
            if !bytes.is_empty() {
                self.observed
                    .push(String::from_utf8_lossy(&bytes).into_owned());
            }
        }
        if matches!(expected, ExpectedExit::Failure) {
            assert!(
                self.observed
                    .iter()
                    .any(|line| line.starts_with("Plugin process cannot continue:")),
                "SDK failure must flush a diagnostic to stderr: {:?}",
                self.observed
            );
        }
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        timeout(DEADLINE, &mut self.server).await???;
        Ok(std::mem::take(&mut self.observed))
    }
    async fn launch(
        mut config: serde_json::Value,
        program: Program,
        existing_queues: Option<&Path>,
        executable: Option<&Path>,
    ) -> Result<Self, Error> {
        // A short UDS path also works under macOS's socket path limit.
        let directory = tempfile::Builder::new()
            .prefix("tenon-rust-")
            .tempdir_in("/tmp")?;
        let working = existing_queues.unwrap_or(directory.path()).to_owned();
        let sink_channels = match &program {
            Program::Source(_) => Vec::new(),
            Program::Sink(channels) | Program::SourceAndSink(channels) => {
                channels.resolve(&working)
            }
        };
        let source_flow = match &program {
            Program::Sink(_) => None,
            Program::Source(_) => Some((SOURCE_FLOW.to_owned(), source_parallelism(&config)?)),
            Program::SourceAndSink(_) if config["boundSource"] == false => None,
            Program::SourceAndSink(_) if sink_channels.is_empty() => {
                Some((SOURCE_FLOW.to_owned(), source_parallelism(&config)?))
            }
            Program::SourceAndSink(_) => {
                let flow_id = sink_channels
                    .first()
                    .ok_or("a Source-and-sink launch needs one input Channel")?
                    .flow_id
                    .clone();
                let count = u32::try_from(
                    sink_channels
                        .iter()
                        .filter(|channel| channel.flow_id == flow_id)
                        .count(),
                )
                .map_err(|_| "Channel count does not fit u32")?;
                assert_eq!(
                    count,
                    source_parallelism(&config)?,
                    "the fixture's Channels match the Source's parallelism"
                );
                Some((flow_id, count))
            }
        };
        let sink_inputs = match &program {
            Program::Source(_) => None,
            Program::Sink(_) | Program::SourceAndSink(_) => {
                (!sink_channels.is_empty()).then_some(sink_channels.as_slice())
            }
        };
        let bells = Bells::create(
            &working,
            source_flow
                .as_ref()
                .map(|(flow, count)| (flow.as_str(), *count)),
            sink_inputs,
        )?;
        if source_flow.is_some() && existing_queues.is_none() {
            let source = working.join("source");
            std::fs::create_dir_all(&source)?;
            let frames = config["pendingRecords"].as_u64().unwrap_or(1) as usize + 1;
            for channel in 0..source_parallelism(&config)? {
                queue(
                    &source.join(format!("submission-{channel}.queue")),
                    72 * frames,
                    64,
                )?;
                queue(
                    &source.join(format!("completion-{channel}.queue")),
                    24 * frames,
                    13,
                )?;
            }
            if matches!(program, Program::Source(QueueOccupancy::Full)) {
                let mut writer = bells.submission_writer(&working, 0)?;
                for value in [1, 2] {
                    assert!(matches!(
                        writer.try_write(&[value; 64])?,
                        WriteOutcome::Committed(_)
                    ))
                }
            }
        }
        if matches!(program, Program::Sink(_) | Program::SourceAndSink(_))
            && existing_queues.is_none()
        {
            for channel in &sink_channels {
                let path = sink_queue(&working, channel);
                std::fs::create_dir_all(path.parent().ok_or("missing Queue directory")?)?;
                queue(&path, 256, 64)?;
            }
        }
        if matches!(program, Program::SourceAndSink(..)) && executable.is_none() {
            config["resource"] = directory
                .path()
                .join("connection")
                .to_string_lossy()
                .into_owned()
                .into();
            config["callbackGate"] = directory
                .path()
                .join("callback-gate")
                .to_string_lossy()
                .into_owned()
                .into();
        }
        let socket = directory.path().join("control.sock");
        let listener = UnixListener::bind(&socket)?;
        let (connections, mut connected) = mpsc::channel(1);
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(plugin::plugin_lifecycle_server::PluginLifecycleServer::new(
                    Server { connections },
                ))
                .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async {
                    let _ = stopped.await;
                }),
        );
        let binary = if let Some(executable) = executable {
            executable.as_os_str().to_owned()
        } else {
            match &program {
                Program::Source(_) => {
                    std::env::var_os("TENON_TEST_SOURCE_BINARY").unwrap_or_else(|| {
                        option_env!("CARGO_BIN_EXE_source-process-fixture")
                            .unwrap_or_default()
                            .into()
                    })
                }
                Program::Sink(_) => sink_binary(),
                Program::SourceAndSink(..) => std::env::var_os("TENON_TEST_SOURCE_AND_SINK_BINARY")
                    .unwrap_or_else(|| {
                        option_env!("CARGO_BIN_EXE_source-and-sink-process-fixture")
                            .unwrap_or_default()
                            .into()
                    }),
            }
        };
        let mut sdk_config = serde_json::json!({
            "workingDirectory": working.to_string_lossy(),
            "controlSocket": socket.to_string_lossy(),
            "launchId": "AAECAwQFBgcICQoLDA0ODw==",
        });
        if let Some((flow_id, _)) = &source_flow {
            sdk_config["sourceChannelRegion"] =
                flow_bell_path(&working, flow_id).to_string_lossy().into();
        }
        if let Some(inputs) = sink_inputs {
            sdk_config["sinkInputs"] = serde_json::Value::Array(
                inputs
                    .iter()
                    .map(|channel| {
                        serde_json::json!({
                            "flowId": channel.flow_id,
                            "channelId": channel.channel_id,
                            "channelBellPath": flow_bell_path(&working, &channel.flow_id).to_string_lossy(),
                        })
                    })
                    .collect(),
            );
        }
        let mut command = Command::new(binary);
        command
            .arg("--sdk-config")
            .arg(serde_json::to_string(&sdk_config)?);
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let mut stdin = child.stdin.take().ok_or("child stdin missing")?;
        stdin.write_all(format!("{config}\n").as_bytes()).await?;
        let stdout = BufReader::new(child.stdout.take().ok_or("child stdout missing")?).lines();
        let (incoming, outgoing) = timeout(DEADLINE, connected.recv())
            .await?
            .ok_or("no control session")?;
        let mut peer = Self {
            directory,
            working,
            bells,
            sink_channels,
            child,
            stdin: Some(stdin),
            stdout,
            observed: Vec::new(),
            incoming,
            outgoing: Some(outgoing),
            stop: Some(stop),
            server,
        };
        let attach = peer.message().await?;
        let Some(plugin::plugin_to_pipeline::Message::Attach(attach)) = attach.message else {
            return Err("Attach was not first".into());
        };
        assert_eq!(attach.launch_id, (0..16).collect::<Vec<_>>());
        Ok(peer)
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.server.abort()
    }
}

pub(super) fn queue(path: &Path, capacity: usize, maximum: u64) -> Result<(), Error> {
    let mut bytes = vec![0; 192 + capacity];
    bytes[..8].copy_from_slice(b"TENONQ\0\0");
    bytes[8..12].copy_from_slice(&1_u32.to_le_bytes());
    bytes[16..24].copy_from_slice(&maximum.to_le_bytes());
    // Creation leaves both doorbell slots unbound. Each endpoint publishes its
    // own loop ordinal when it opens, and until then a peer rings nothing.
    for slot in [72, 136] {
        bytes[slot..slot + 4].copy_from_slice(&u32::MAX.to_le_bytes())
    }
    File::create_new(path)?.write_all(&bytes)?;
    Ok(())
}

#[derive(Debug)]
struct Server {
    connections: mpsc::Sender<Connection>,
}

#[tonic::async_trait]
impl plugin::plugin_lifecycle_server::PluginLifecycle for Server {
    type RunStream = ReceiverStream<Reply>;

    async fn run(
        &self,
        request: Request<Streaming<plugin::PluginToPipeline>>,
    ) -> Result<Response<Self::RunStream>, Status> {
        let (outgoing, receiver) = mpsc::channel(8);
        self.connections
            .send((request.into_inner(), outgoing))
            .await
            .map_err(|_| Status::internal("test peer disappeared"))?;
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

enum Program {
    Source(QueueOccupancy),
    Sink(Channels),
    SourceAndSink(Channels),
}

pub(super) fn sink_binary() -> std::ffi::OsString {
    std::env::var_os("TENON_TEST_SINK_BINARY").unwrap_or_else(|| {
        option_env!("CARGO_BIN_EXE_sink-process-fixture")
            .unwrap_or_default()
            .into()
    })
}

pub(super) fn sink_queue(working: &Path, channel: &FlowChannel) -> std::path::PathBuf {
    working
        .join("sink")
        .join(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(Sha256::digest(channel.flow_id.as_bytes())),
        )
        .join(format!("egress-{}.queue", channel.channel_id))
}

/// Shared cases are consumed by real process and UDS tests, not a second state machine.
pub(super) fn lifecycle_vectors() -> Result<serde_json::Value, Error> {
    Ok(serde_json::from_str(include_str!(
        "../../contracts/process_protocol_test_vectors.json"
    ))?)
}

pub(super) fn lifecycle_vector(scenario: &str) -> Result<serde_json::Value, Error> {
    lifecycle_vectors()?["lifecycle"]
        .as_array()
        .ok_or("missing lifecycle vectors")?
        .iter()
        .find(|vector| vector["scenario"] == scenario)
        .cloned()
        .ok_or_else(|| "missing lifecycle scenario".into())
}

pub(super) fn assert_business_events(
    vector: &serde_json::Value,
    observed: &[String],
) -> Result<(), Error> {
    let expected = vector["businessEvents"]
        .as_array()
        .ok_or("missing business events")?;
    let mut previous = None;
    for event in expected {
        let event = event.as_str().ok_or("invalid business event")?;
        let positions: Vec<_> = observed
            .iter()
            .enumerate()
            .filter(|(_, line)| normalize_business_event(line) == event)
            .map(|(index, _)| index)
            .collect();
        assert_eq!(positions.len(), 1, "{event}: {observed:?}");
        if let Some(previous) = previous {
            assert!(previous < positions[0], "{observed:?}");
        }
        previous = Some(positions[0]);
    }
    Ok(())
}

fn normalize_business_event(event: &str) -> &str {
    match event {
        "shared-start" | "start" => "owner.start",
        "source-start" => "source.start",
        "source-quiesce" => "source.quiesce",
        "quiesced-send SessionClosed" => "source.admission-closed",
        "source-close" => "source.close",
        "shared-close" | "close" => "owner.close",
        other => other,
    }
}
