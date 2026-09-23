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

//! One real Plugin lifecycle client shared by repository process tests.
//! The generated protocol and Queue adapters are the same contracts used by the executable.

use std::error::Error;
use std::fs::OpenOptions;
use std::io::{self, BufRead as _, Read as _, Write as _};
use std::path::{Path, PathBuf};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde_json::Value;
use tenon::runner_test_support::contracts::core::PluginInterface;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tonic::transport::{Channel, Endpoint};

// Include the single generated wire contract without adding a production test API.
#[allow(
    dead_code,
    reason = "The test peer uses only the client half of the generated contract"
)]
mod protocol {
    include!(concat!(env!("OUT_DIR"), "/tenon.plugin.rs"));
}
use protocol::plugin_lifecycle_client::PluginLifecycleClient;
use protocol::{
    Attach, PipelineToPlugin, PluginToPipeline, Ready, SourceQuiesced, pipeline_to_plugin,
    plugin_to_pipeline,
};

#[path = "controlled_plugin/queue_traffic.rs"]
mod queue_traffic;

const CHILD_ARGUMENT_ADAPTER: &str = r#"
# The Pipeline appends the reserved startup block last, so the child finds it
# from the end whatever arguments the Program manifest declares before it.
[ "$#" -ge 4 ] || exit 70
TENON_TEST_PLUGIN_INTERFACE="$2"
child="$1"
shift 2
while [ "$#" -gt 2 ]; do shift; done
[ "$1" = "--sdk-config" ] || exit 71
TENON_TEST_PLUGIN_SDK_CONFIG="$2"
# The child reads the whole startup document; this one field is also exported
# for the shell-level fixtures that plant a descendant beside the Plugin.
TENON_TEST_PLUGIN_WORKING_DIRECTORY="${2#*workingDirectory\":\"}"
TENON_TEST_PLUGIN_WORKING_DIRECTORY="${TENON_TEST_PLUGIN_WORKING_DIRECTORY%%\"*}"
export TENON_TEST_PLUGIN_INTERFACE TENON_TEST_PLUGIN_SDK_CONFIG
export TENON_TEST_PLUGIN_WORKING_DIRECTORY
exec "$child" --exact CHILD_TEST_NAME --nocapture
"#;

#[expect(
    clippy::expect_used,
    reason = "the shared child driver is only included below a test crate root"
)]
pub(crate) fn controlled_program_command(interface: PluginInterface) -> io::Result<Vec<String>> {
    let test_executable = std::env::current_exe()?
        .into_os_string()
        .into_string()
        .map_err(|_| io::Error::other("Test executable path is not UTF-8"))?;
    let interface_name = match interface {
        PluginInterface::Source => "source",
        PluginInterface::Sink => "sink",
        PluginInterface::SourceAndSink => "source-and-sink",
    };
    let test_module = module_path!()
        .split_once("::")
        .expect("The shared child driver is always included in a test submodule")
        .1;
    let child_test = format!("{test_module}::plugin_control_child_process");
    let argument_adapter = CHILD_ARGUMENT_ADAPTER.replace("CHILD_TEST_NAME", &child_test);
    Ok(vec![
        "/bin/sh".into(),
        "-c".into(),
        argument_adapter,
        "tenon-test-plugin".into(),
        test_executable,
        interface_name.into(),
    ])
}

#[tokio::test(flavor = "current_thread")]
async fn plugin_control_child_process() -> Result<(), Box<dyn Error>> {
    let Ok(context) = ChildContext::from_environment() else {
        return Ok(());
    };
    let mut children = OwnedChildren::start()?;
    let result = context.run().await;
    children.finish()?;
    result
}

/// A real Plugin owns and reaps its helper processes before normal exit.
struct OwnedChildren(Vec<std::process::Child>);

impl OwnedChildren {
    fn start() -> io::Result<Self> {
        let mut children = Self(Vec::new());
        if let Ok(command) = std::env::var("TENON_TEST_PLUGIN_CHILD_COMMAND") {
            let command: Vec<String> = serde_json::from_str(&command)?;
            for _ in 0..3 {
                children.0.push(
                    std::process::Command::new(&command[0])
                        .args(&command[1..])
                        .stdin(std::process::Stdio::null())
                        .stdout(std::process::Stdio::null())
                        .spawn()?,
                );
            }
        }
        Ok(children)
    }

    fn finish(&mut self) -> io::Result<()> {
        for child in &mut self.0 {
            child.kill()?;
        }
        for child in &mut self.0 {
            child.wait()?;
        }
        self.0.clear();
        Ok(())
    }
}

impl Drop for OwnedChildren {
    fn drop(&mut self) {
        for child in &mut self.0 {
            let _ = child.kill();
        }
        for child in &mut self.0 {
            let _ = child.wait();
        }
    }
}

struct ChildContext {
    interface: PluginInterface,
    working_directory: PathBuf,
    control_socket: PathBuf,
    launch_id: [u8; 16],
    channel_bell_path: Option<PathBuf>,
    sink_channels: Vec<queue_traffic::SinkChannel>,
}

impl ChildContext {
    fn from_environment() -> Result<Self, io::Error> {
        let interface = match std::env::var("TENON_TEST_PLUGIN_INTERFACE").as_deref() {
            Ok("source") => PluginInterface::Source,
            Ok("sink") => PluginInterface::Sink,
            Ok("source-and-sink") => PluginInterface::SourceAndSink,
            _ => return Err(io::Error::other("Controlled child environment is absent")),
        };
        // The child reads the same startup document every Plugin reads, so a
        // direction the Pipeline failed to fill fails this fixture too.
        let document: Value = serde_json::from_str(
            &std::env::var("TENON_TEST_PLUGIN_SDK_CONFIG")
                .map_err(|_| io::Error::other("Controlled child startup document is missing"))?,
        )
        .map_err(|_| io::Error::other("Controlled child startup document is not JSON"))?;
        let working_directory = required_path(&document, "workingDirectory")?;
        let control_socket = required_path(&document, "controlSocket")?;
        let encoded_launch_id = document
            .get("launchId")
            .and_then(Value::as_str)
            .ok_or_else(|| io::Error::other("Controlled child launch id is missing"))?;
        let decoded = STANDARD
            .decode(encoded_launch_id)
            .map_err(|_| io::Error::other("Controlled child launch id is malformed"))?;
        if STANDARD.encode(&decoded) != encoded_launch_id {
            return Err(io::Error::other(
                "Controlled child launch id is not canonical padded Base64",
            ));
        }
        let launch_id = decoded
            .try_into()
            .map_err(|_| io::Error::other("Controlled child launch id is not 16 bytes"))?;
        // The Pipeline fills a direction exactly for an Interface that declares
        // it, so an Interface whose direction is absent fails instead of idling.
        let channel_bell_path = match document.get("sourceChannelRegion") {
            Some(region) => Some(PathBuf::from(region.as_str().ok_or_else(|| {
                io::Error::other("Controlled child Channel region is not a path")
            })?)),
            None if matches!(
                interface,
                PluginInterface::Source | PluginInterface::SourceAndSink
            ) =>
            {
                return Err(io::Error::other(
                    "Controlled child Channel region is missing",
                ));
            }
            None => None,
        };
        let sink_channels = match document.get("sinkInputs") {
            Some(inputs) => serde_json::from_value(inputs.clone()).map_err(|_| {
                io::Error::other("Controlled child Sink inputs are not a Channel list")
            })?,
            None if matches!(
                interface,
                PluginInterface::Sink | PluginInterface::SourceAndSink
            ) =>
            {
                return Err(io::Error::other("Controlled child Sink inputs are missing"));
            }
            None => Vec::new(),
        };
        Ok(Self {
            interface,
            working_directory,
            control_socket,
            launch_id,
            channel_bell_path,
            sink_channels,
        })
    }

    async fn run(mut self) -> Result<(), Box<dyn Error>> {
        let mut config_line = String::new();
        if io::stdin().lock().read_line(&mut config_line)? == 0 || !config_line.ends_with('\n') {
            return Err(io::Error::other("Controlled child config line is missing").into());
        }
        let config: Value = serde_json::from_str(&config_line)?;
        std::fs::write(
            self.working_directory.join("config.received"),
            config_line.trim_end_matches('\n'),
        )?;
        append(
            &self.working_directory.join("configs.received"),
            &config_line,
        )?;
        // Cleanup may read the PID as soon as the file exists.
        let pending_pid = self.working_directory.join("process.pid.tmp");
        std::fs::write(&pending_pid, std::process::id().to_string())?;
        std::fs::rename(pending_pid, self.working_directory.join("process.pid"))?;
        append(
            &self.working_directory.join("processes.received"),
            &format!("{}\n", std::process::id()),
        )?;
        std::fs::write(
            self.working_directory.join("parent.pid"),
            rustix::process::getppid()
                .ok_or("Controlled child parent is missing")?
                .as_raw_nonzero()
                .to_string(),
        )?;
        append(&self.working_directory.join("starts.received"), "started\n")?;
        if let Some(path) = std::env::var_os("TENON_TEST_PLUGIN_STARTS") {
            append(Path::new(&path), &config_line)?;
        }
        append(
            &self.working_directory.join("launch-ids.received"),
            &format!("{}\n", STANDARD.encode(self.launch_id)),
        )?;
        std::fs::write(
            self.working_directory.join("program.received"),
            std::env::current_dir()?.display().to_string(),
        )?;
        println!("diagnostic-before-ready");
        eprintln!("diagnostic-stderr-before-ready");

        let traffic = queue_traffic::QueueTraffic::open(
            &self.working_directory,
            &config,
            std::mem::take(&mut self.sink_channels),
            self.channel_bell_path.as_deref(),
        )?;
        let behavior = config
            .get("behavior")
            .and_then(Value::as_str)
            .unwrap_or("normal");
        tokio::select! {
            result = self.run_control(behavior) => result,
            result = queue_traffic::run(traffic, &self.working_directory) => result,
        }
    }

    async fn run_control(&self, behavior: &str) -> Result<(), Box<dyn Error>> {
        let (messages, stream) = mpsc::channel(8);
        messages
            .send(plugin_message(plugin_to_pipeline::Message::Attach(
                Attach {
                    launch_id: self.launch_id.to_vec(),
                },
            )))
            .await
            .map_err(send_failure)?;
        let mut client = connect(&self.control_socket).await?;
        let mut commands = client.run(ReceiverStream::new(stream)).await?.into_inner();
        append_event(&self.working_directory, "attach")?;

        if behavior == "exit-before-ready" {
            std::process::exit(31);
        }
        if behavior == "delay-ready" {
            wait_for_path(&self.working_directory.join("allow-ready")).await;
        }

        messages
            .send(plugin_message(plugin_to_pipeline::Message::Ready(Ready {})))
            .await
            .map_err(send_failure)?;
        append_event(&self.working_directory, "ready")?;
        println!("diagnostic-after-ready");
        eprintln!("diagnostic-stderr-after-ready");
        if behavior == "stream-loss-after-ready" {
            drop(messages);
            return std::future::pending::<Result<(), Box<dyn Error>>>().await;
        }

        let mut owner_lost = monitor_stdin_eof();
        if matches!(
            self.interface,
            PluginInterface::Source | PluginInterface::SourceAndSink
        ) {
            let command = next_command(&mut commands, &mut owner_lost).await?;
            if !matches!(
                command.message,
                Some(pipeline_to_plugin::Message::QuiesceSource(_))
            ) {
                return Err(io::Error::other("Controlled child expected QuiesceSource").into());
            }
            append_event(&self.working_directory, "quiesce-source")?;
            if behavior == "exit-during-quiesce" {
                std::process::exit(33);
            }
            if behavior == "stream-loss-during-quiesce" {
                drop(messages);
                return std::future::pending::<Result<(), Box<dyn Error>>>().await;
            }
            if behavior == "delay-quiesce" {
                wait_for_path(&self.working_directory.join("allow-quiesce")).await;
            }
            messages
                .send(plugin_message(plugin_to_pipeline::Message::SourceQuiesced(
                    SourceQuiesced {},
                )))
                .await
                .map_err(send_failure)?;
            append_event(&self.working_directory, "source-quiesced")?;
            if behavior == "stream-loss-after-quiesced" {
                drop(messages);
                return std::future::pending::<Result<(), Box<dyn Error>>>().await;
            }
        }

        let command = next_command(&mut commands, &mut owner_lost).await?;
        if !matches!(
            command.message,
            Some(pipeline_to_plugin::Message::Shutdown(_))
        ) {
            return Err(io::Error::other("Controlled child expected Shutdown").into());
        }
        append_event(&self.working_directory, "shutdown")?;
        if behavior == "delay-shutdown" {
            wait_for_path(&self.working_directory.join("allow-shutdown")).await;
        }
        if behavior == "message-after-shutdown" {
            messages
                .send(plugin_message(plugin_to_pipeline::Message::Ready(Ready {})))
                .await
                .map_err(send_failure)?;
            std::future::pending::<()>().await;
        }

        drop(messages);
        append_event(&self.working_directory, "exit")?;
        Ok(())
    }
}

async fn connect(socket_path: &Path) -> Result<PluginLifecycleClient<Channel>, Box<dyn Error>> {
    let endpoint = Endpoint::from_shared(format!("unix://{}", socket_path.display()))?;
    Ok(PluginLifecycleClient::new(endpoint.connect().await?))
}

async fn next_command(
    commands: &mut Streaming<PipelineToPlugin>,
    owner_lost: &mut oneshot::Receiver<()>,
) -> Result<PipelineToPlugin, Box<dyn Error>> {
    tokio::select! {
        result = commands.message() => result?
            .ok_or_else(|| io::Error::other("Controlled child command stream ended").into()),
        _ = owner_lost => std::process::exit(41),
    }
}

fn monitor_stdin_eof() -> oneshot::Receiver<()> {
    let (sender, receiver) = oneshot::channel();
    std::thread::spawn(move || {
        let mut byte = [0_u8; 1];
        match io::stdin().read(&mut byte) {
            Ok(0) | Err(_) => {
                let _ = sender.send(());
            }
            Ok(_) => std::process::exit(42),
        }
    });
    receiver
}

fn plugin_message(message: plugin_to_pipeline::Message) -> PluginToPipeline {
    PluginToPipeline {
        message: Some(message),
    }
}

fn required_path(document: &Value, key: &str) -> io::Result<PathBuf> {
    document
        .get(key)
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::other(format!("Controlled child {key} is missing")))
}

fn append_event(working_directory: &Path, event: &str) -> io::Result<()> {
    append(
        &working_directory.join("lifecycle.received"),
        &format!("{event}\n"),
    )?;
    std::fs::write(working_directory.join(format!("{event}.received")), [])
}

fn append(path: &Path, text: &str) -> io::Result<()> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?
        .write_all(text.as_bytes())
}

async fn wait_for_path(path: &Path) {
    while !path.exists() {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

fn send_failure<T>(_failure: mpsc::error::SendError<T>) -> io::Error {
    io::Error::other("Controlled child request stream ended")
}
