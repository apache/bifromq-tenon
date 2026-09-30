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

use std::error::Error;
use std::io;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tonic::transport::{Channel, Endpoint};

use super::{
    PluginControlSessionError, ReadyPluginControl, TestPluginControlServer as PluginControlServer,
};
use crate::contracts::plugin::plugin_lifecycle_client::PluginLifecycleClient;
use crate::contracts::plugin::{
    Attach, PipelineToPlugin, PluginToPipeline, Ready, SourceQuiesced, pipeline_to_plugin,
    plugin_to_pipeline,
};
use crate::payload_contract::PluginInterface;

const WAIT_LIMIT: Duration = Duration::from_secs(2);

#[tokio::test(flavor = "current_thread")]
async fn source_capable_session_follows_the_complete_staged_sequence() -> Result<(), Box<dyn Error>>
{
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let socket_path = launcher.socket_path().to_owned();
    let socket_directory = socket_path
        .parent()
        .ok_or_else(|| io::Error::other("Plugin Control socket has no parent"))?
        .to_path_buf();
    assert_eq!(
        socket_directory.metadata()?.permissions().mode() & 0o777,
        0o700
    );
    let pending = launcher.register(PluginInterface::SourceAndSink);
    let mut plugin = FakePlugin::attach(&socket_path, pending.launch_id()).await?;
    let attached = pending.attach().await?;

    plugin.send(ready()).await?;
    let ReadyPluginControl::SourceCapable(source) = attached.wait_for_ready().await? else {
        return Err(io::Error::other("Source-capable launch produced a Sink-only session").into());
    };
    let quiescing = source.begin_source_quiesce()?;
    assert!(matches!(
        plugin.next_command().await?.message,
        Some(pipeline_to_plugin::Message::QuiesceSource(_))
    ));

    plugin.send(source_quiesced()).await?;
    let quiesced = quiescing.wait_for_source_quiesced().await?;
    let mut shutdown = quiesced.shutdown()?;
    assert!(matches!(
        plugin.next_command().await?.message,
        Some(pipeline_to_plugin::Message::Shutdown(_))
    ));
    plugin.close_messages();
    shutdown.wait_for_stream_end().await?;
    drop(shutdown);
    drop(plugin);

    tokio::time::timeout(WAIT_LIMIT, server.shutdown())
        .await
        .map_err(|_| io::Error::other("Plugin Control server shutdown timed out"))??;
    assert!(!socket_directory.exists());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn sink_only_session_proceeds_directly_from_ready_to_shutdown() -> Result<(), Box<dyn Error>>
{
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let pending = launcher.register(PluginInterface::Sink);
    let mut plugin = FakePlugin::attach(launcher.socket_path(), pending.launch_id()).await?;
    let attached = pending.attach().await?;

    plugin.send(ready()).await?;
    let ReadyPluginControl::SinkOnly(sink) = attached.wait_for_ready().await? else {
        return Err(io::Error::other("Sink-only launch produced a Source-capable session").into());
    };
    let shutdown = sink.shutdown()?;
    assert!(matches!(
        plugin.next_command().await?.message,
        Some(pipeline_to_plugin::Message::Shutdown(_))
    ));
    drop(shutdown);
    drop(plugin);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_first_message_does_not_consume_the_registered_launch() -> Result<(), Box<dyn Error>>
{
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let pending = launcher.register(PluginInterface::Sink);
    let socket_path = launcher.socket_path().to_owned();
    let mut client = connect(&socket_path).await?;
    let (messages, stream) = mpsc::channel(1);
    messages.send(ready()).await.map_err(send_failure)?;

    let result = client.run(ReceiverStream::new(stream)).await;
    let Err(rejected) = result else {
        return Err(io::Error::other("Ready was accepted as the first message").into());
    };
    assert_eq!(rejected.code(), tonic::Code::InvalidArgument);

    let plugin = FakePlugin::attach(&socket_path, pending.launch_id()).await?;
    let attached = pending.attach().await?;
    drop(attached);
    drop(plugin);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_unknown_and_already_claimed_launches_are_rejected() -> Result<(), Box<dyn Error>>
{
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let pending = launcher.register(PluginInterface::Sink);
    let socket_path = launcher.socket_path().to_owned();

    assert_eq!(
        rejected_attach(&socket_path, &[1; 15]).await?,
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        rejected_attach(&socket_path, &[2; 16]).await?,
        tonic::Code::FailedPrecondition
    );

    let launch_id = *pending.launch_id();
    let (first, second) = tokio::join!(
        FakePlugin::attach(&socket_path, &launch_id),
        FakePlugin::attach(&socket_path, &launch_id)
    );
    let (plugin, rejected) = match (first, second) {
        (Ok(plugin), Err(rejected)) | (Err(rejected), Ok(plugin)) => (plugin, rejected),
        _ => {
            return Err(io::Error::other(
                "Concurrent Plugin connections did not produce exactly one owner",
            )
            .into());
        }
    };
    assert_eq!(
        rejected
            .as_ref()
            .downcast_ref::<tonic::Status>()
            .map(tonic::Status::code),
        Some(tonic::Code::FailedPrecondition)
    );
    let attached = pending.attach().await?;
    drop(attached);
    drop(plugin);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn empty_envelope_after_attach_is_not_reported_as_disconnect() -> Result<(), Box<dyn Error>> {
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let pending = launcher.register(PluginInterface::Sink);
    let plugin = FakePlugin::attach(launcher.socket_path(), pending.launch_id()).await?;
    let attached = pending.attach().await?;

    plugin.send(PluginToPipeline { message: None }).await?;
    assert!(matches!(
        attached.wait_for_ready().await,
        Err(PluginControlSessionError::MessageMissing)
    ));
    drop(plugin);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn source_quiesced_before_ready_fails_the_session() -> Result<(), Box<dyn Error>> {
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let pending = launcher.register(PluginInterface::Source);
    let plugin = FakePlugin::attach(launcher.socket_path(), pending.launch_id()).await?;
    let attached = pending.attach().await?;

    plugin.send(source_quiesced()).await?;
    let failure = attached.wait_for_ready().await;
    assert!(matches!(
        failure,
        Err(PluginControlSessionError::UnexpectedMessage { .. })
    ));
    drop(plugin);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_ready_fails_an_already_ready_session() -> Result<(), Box<dyn Error>> {
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();
    let pending = launcher.register(PluginInterface::Sink);
    let plugin = FakePlugin::attach(launcher.socket_path(), pending.launch_id()).await?;
    let attached = pending.attach().await?;

    plugin.send(ready()).await?;
    let ReadyPluginControl::SinkOnly(mut sink) = attached.wait_for_ready().await? else {
        return Err(io::Error::other("Sink-only launch produced a Source-capable session").into());
    };
    plugin.send(ready()).await?;
    assert!(matches!(
        std::future::poll_fn(|context| sink.poll_failure(context)).await,
        PluginControlSessionError::UnexpectedMessage { .. }
    ));
    drop(plugin);
    server.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn stream_end_before_and_after_ready_is_a_session_failure() -> Result<(), Box<dyn Error>> {
    let server = PluginControlServer::start()?;
    let launcher = server.launcher();

    let before_ready = launcher.register(PluginInterface::Sink);
    let mut first = FakePlugin::attach(launcher.socket_path(), before_ready.launch_id()).await?;
    let attached = before_ready.attach().await?;
    first.close_messages();
    assert!(matches!(
        attached.wait_for_ready().await,
        Err(PluginControlSessionError::Disconnected)
    ));
    drop(first);

    let after_ready = launcher.register(PluginInterface::Source);
    let mut second = FakePlugin::attach(launcher.socket_path(), after_ready.launch_id()).await?;
    let attached = after_ready.attach().await?;
    second.send(ready()).await?;
    let ReadyPluginControl::SourceCapable(mut source) = attached.wait_for_ready().await? else {
        return Err(io::Error::other("Source launch produced a Sink-only session").into());
    };
    second.close_messages();
    assert!(matches!(
        std::future::poll_fn(|context| source.poll_failure(context)).await,
        PluginControlSessionError::Disconnected
    ));
    drop(second);

    server.shutdown().await?;
    Ok(())
}

async fn rejected_attach(
    socket_path: &Path,
    launch_id: &[u8],
) -> Result<tonic::Code, Box<dyn Error>> {
    let mut client = connect(socket_path).await?;
    let (messages, stream) = mpsc::channel(1);
    messages
        .send(attach(launch_id))
        .await
        .map_err(send_failure)?;
    let result = client.run(ReceiverStream::new(stream)).await;
    let Err(rejected) = result else {
        return Err(io::Error::other("Rejected Plugin launch attached successfully").into());
    };
    Ok(rejected.code())
}

async fn connect(socket_path: &Path) -> Result<PluginLifecycleClient<Channel>, Box<dyn Error>> {
    let endpoint = Endpoint::from_shared(format!("unix://{}", socket_path.display()))?;
    Ok(PluginLifecycleClient::new(endpoint.connect().await?))
}

struct FakePlugin {
    messages: Option<mpsc::Sender<PluginToPipeline>>,
    commands: Streaming<PipelineToPlugin>,
}

impl FakePlugin {
    async fn attach(socket_path: &Path, launch_id: &[u8]) -> Result<Self, Box<dyn Error>> {
        let mut client = connect(socket_path).await?;
        let (messages, stream) = mpsc::channel(8);
        messages
            .send(attach(launch_id))
            .await
            .map_err(send_failure)?;
        let commands = client.run(ReceiverStream::new(stream)).await?.into_inner();
        Ok(Self {
            messages: Some(messages),
            commands,
        })
    }

    async fn send(&self, message: PluginToPipeline) -> io::Result<()> {
        self.messages
            .as_ref()
            .ok_or_else(|| io::Error::other("Fake Plugin message stream is closed"))?
            .send(message)
            .await
            .map_err(send_failure)
    }

    async fn next_command(&mut self) -> Result<PipelineToPlugin, Box<dyn Error>> {
        tokio::time::timeout(WAIT_LIMIT, self.commands.message())
            .await
            .map_err(|_| io::Error::other("Plugin command timed out"))??
            .ok_or_else(|| io::Error::other("Plugin command stream ended").into())
    }

    fn close_messages(&mut self) {
        self.messages = None;
    }
}

fn attach(launch_id: &[u8]) -> PluginToPipeline {
    PluginToPipeline {
        message: Some(plugin_to_pipeline::Message::Attach(Attach {
            launch_id: launch_id.to_vec(),
        })),
    }
}

fn ready() -> PluginToPipeline {
    PluginToPipeline {
        message: Some(plugin_to_pipeline::Message::Ready(Ready {})),
    }
}

fn source_quiesced() -> PluginToPipeline {
    PluginToPipeline {
        message: Some(plugin_to_pipeline::Message::SourceQuiesced(
            SourceQuiesced {},
        )),
    }
}

fn send_failure<T>(_failure: mpsc::error::SendError<T>) -> io::Error {
    io::Error::other("Fake Plugin request stream ended")
}

#[tokio::test(flavor = "current_thread")]
async fn shared_invalid_lifecycle_vectors_are_rejected_by_the_real_control_session()
-> Result<(), Box<dyn Error>> {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/contracts/plugin/test-fixtures/process_protocol_test_vectors.json"
    )))?;
    for vector in vectors["lifecycleInvalid"]
        .as_array()
        .ok_or("missing lifecycle vectors")?
    {
        if vector["receiver"] != "pipeline" {
            continue;
        }
        let server = PluginControlServer::start()?;
        let launcher = server.launcher();
        let interface = match vector["interface"].as_str().ok_or("missing interface")? {
            "source" => PluginInterface::Source,
            "source-and-sink" => PluginInterface::SourceAndSink,
            _ => return Err("unhandled lifecycle interface".into()),
        };
        let pending = launcher.register(interface);
        let events = vector["events"]
            .as_array()
            .ok_or("missing lifecycle events")?;
        let observed = if events.len() == 1 {
            let mut client = connect(launcher.socket_path()).await?;
            let (messages, stream) = mpsc::channel(1);
            assert_eq!(events[0], "control.ready");
            messages.send(ready()).await?;
            let Err(rejection) = client.run(ReceiverStream::new(stream)).await else {
                return Err("Ready must not attach".into());
            };
            drop(pending);
            format!("{:?}", rejection.code())
        } else {
            assert_eq!(events[0], "control.attach");
            let launch_id = pending.launch_id().to_vec();
            let plugin = FakePlugin::attach(launcher.socket_path(), &launch_id).await?;
            let attached = pending.attach().await?;
            let failure = if events[1] == "control.attach" {
                assert_eq!(events.len(), 2);
                plugin.send(attach(&launch_id)).await?;
                match attached.wait_for_ready().await {
                    Err(error) => error,
                    Ok(_) => return Err("duplicate Attach accepted".into()),
                }
            } else {
                assert_eq!(events.len(), 3);
                assert_eq!(events[1], "control.ready");
                plugin.send(ready()).await?;
                let ReadyPluginControl::SourceCapable(mut source) =
                    attached.wait_for_ready().await?
                else {
                    return Err("wrong interface owner".into());
                };
                plugin
                    .send(match events[2].as_str().ok_or("invalid lifecycle event")? {
                        "control.ready" => ready(),
                        "control.source-quiesced" => source_quiesced(),
                        _ => return Err("unhandled lifecycle event".into()),
                    })
                    .await?;
                tokio::time::timeout(
                    WAIT_LIMIT,
                    std::future::poll_fn(|cx| source.poll_failure(cx)),
                )
                .await?
            };
            drop(plugin);
            match failure {
                PluginControlSessionError::UnexpectedMessage { .. } => "UnexpectedMessage".into(),
                other => return Err(other.into()),
            }
        };
        assert_eq!(
            observed,
            vector["expectedRejection"]
                .as_str()
                .ok_or("missing rejection")?,
            "{}",
            vector["name"]
        );
        server.shutdown().await?;
    }
    Ok(())
}
