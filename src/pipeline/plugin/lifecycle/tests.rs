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

use super::{PluginSpawnOutcome, spawn_plugin};
use crate::pipeline::diagnostics::test_support as diagnostic_test_support;
use crate::pipeline::plugin::metrics::PluginProcessMetrics;
use crate::pipeline::plugin::test_support::{TEST_DEADLINE, TestLaunch, wait_for_file};
use crate::tenon_document::verified::{ExtraArgs, ExtraArgsPosition};
use serde_json::json;
use std::collections::BTreeMap;
use std::error::Error;
use std::future::poll_fn;
use std::io;
use std::task::Poll;
use tempfile::TempDir;
use tokio::time::timeout;

#[tokio::test(flavor = "current_thread")]
async fn launch_applies_instance_arguments_and_environment_without_shell_expansion()
-> Result<(), Box<dyn Error>> {
    use std::os::unix::fs::PermissionsExt;
    for position in [ExtraArgsPosition::Append, ExtraArgsPosition::Prepend] {
        let root = TempDir::new()?;
        let executable = root.path().join("probe");
        std::fs::write(
            &executable,
            "#!/bin/sh\nprintf '%s\\0' \"$@\" > received\nprintf '%s' \"$TEST_REGION\" > environment\nIFS= read -r config\n",
        )?;
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))?;
        let command = vec![executable.display().to_string(), "base".into()];
        let extra: ExtraArgs = serde_json::from_value(
            json!({"args": ["a b", "", "$HOME", "line\nbreak"], "position": position}),
        )?;
        let env = BTreeMap::from([("TEST_REGION".into(), "cn-north".into())]);
        let socket = root.path().join("unused.sock");
        let input = json!({});
        let launch = super::PluginLaunch {
            program_directory: root.path(),
            command: &command,
            working_directory: root.path().to_owned(),
            config: &input,
            extra_args: Some(&extra),
            env: Some(&env),
            bells: super::PluginBells {
                source_channel_region: None,
                sink_inputs: None,
            },
        };
        let PluginSpawnOutcome::Started {
            mut process,
            mut config,
        } = spawn_plugin(
            launch,
            &socket,
            &[1; 16],
            diagnostic_test_support::publisher()
                .instance_plugin(diagnostic_test_support::plugin_id()),
            PluginProcessMetrics::default().register(&[1; 16], vec![]),
        )
        else {
            return Err(io::Error::other("Plugin did not spawn").into());
        };
        let result = timeout(TEST_DEADLINE, async {
            poll_fn(|cx| process.poll_config(&mut config, cx)).await?;
            process.wait_for_exit().await
        })
        .await;
        process.request_force_stop()?;
        process.reap_after_stop().await?;
        assert!(result??.success());
        let bytes = std::fs::read(root.path().join("received"))?;
        assert_eq!(bytes.last(), Some(&0));
        let actual: Vec<&[u8]> = bytes[..bytes.len() - 1].split(|byte| *byte == 0).collect();
        let mut expected = match position {
            ExtraArgsPosition::Append => vec!["base", "a b", "", "$HOME", "line\nbreak"],
            ExtraArgsPosition::Prepend => vec!["a b", "", "$HOME", "line\nbreak", "base"],
        };
        let sdk_config = serde_json::to_string(&super::PluginSdkConfig {
            working_directory: root.path(),
            control_socket: &socket,
            launch_id: String::from("AQEBAQEBAQEBAQEBAQEBAQ=="),
            bells: &super::PluginBells {
                source_channel_region: None,
                sink_inputs: None,
            },
        })?;
        expected.push("--sdk-config");
        expected.push(sdk_config.as_str());
        assert_eq!(
            actual,
            expected
                .iter()
                .map(|arg| arg.as_bytes())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            std::fs::read_to_string(root.path().join("environment"))?,
            "cn-north"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn spawned_process_drains_both_pipes_before_config_without_diagnostic_interest()
-> Result<(), Box<dyn Error>> {
    let root = TempDir::new()?;
    let launch = TestLaunch::new(
        root.path(),
        "source",
        r#"
chunk=x
index=0
while [ "$index" -lt 16 ]; do
    chunk="$chunk$chunk"
    index=$((index + 1))
done
index=0
while [ "$index" -lt 32 ]; do
    printf '%s' "$chunk"
    printf '%s' "$chunk" >&2
    index=$((index + 1))
done
IFS= read -r config || exit 12
printf '%s' "$config" > "$1/config.received.tmp"
mv "$1/config.received.tmp" "$1/config.received"
while IFS= read -r ignored; do :; done
"#,
        json!({"value": "configured"}),
    )?;
    let diagnostics =
        diagnostic_test_support::publisher().instance_plugin(diagnostic_test_support::plugin_id());
    let PluginSpawnOutcome::Started {
        mut process,
        mut config,
    } = spawn_plugin(
        launch.borrowed(),
        &root.path().join("unused-control.sock"),
        &[1; 16],
        diagnostics,
        PluginProcessMetrics::default().register(&[1; 16], vec![]),
    )
    else {
        return Err(io::Error::other("Output-drain child did not spawn").into());
    };
    // Each pipe receives 2 MiB before the child reads config. No caller starts
    // either drain, and nobody subscribes to the diagnostic stream.
    let received = timeout(TEST_DEADLINE, async {
        poll_fn(|context| process.poll_config(&mut config, context)).await?;
        let path = launch.working_directory().join("config.received");
        wait_for_file(&path).await?;
        std::fs::read_to_string(path)
    })
    .await;
    let stopped = process.request_force_stop();
    let reaped = timeout(TEST_DEADLINE, process.reap_after_stop()).await;
    stopped?;
    reaped??;
    assert_eq!(received??, serde_json::to_string(launch.config())?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_startup_write_resumes_without_repeating_bytes() -> Result<(), Box<dyn Error>> {
    let root = TempDir::new()?;
    let launch = TestLaunch::new(
        root.path(),
        "child",
        r#"
: > "$1/started"
while [ ! -f "$1/read-config" ]; do sleep 0.005; done
IFS= read -r config || exit 12
printf '%s' "$config" > "$1/config.received.tmp"
mv "$1/config.received.tmp" "$1/config.received"
while IFS= read -r ignored; do :; done
"#,
        json!({"value": "x".repeat(1024 * 1024)}),
    )?;
    let expected = serde_json::to_string(launch.config())?;
    let diagnostics =
        diagnostic_test_support::publisher().instance_plugin(diagnostic_test_support::plugin_id());
    let PluginSpawnOutcome::Started {
        mut process,
        mut config,
    } = spawn_plugin(
        launch.borrowed(),
        &root.path().join("unused-control.sock"),
        &[1; 16],
        diagnostics,
        PluginProcessMetrics::default().register(&[1; 16], vec![]),
    )
    else {
        return Err(io::Error::other("Config-writer child did not spawn").into());
    };
    wait_for_file(&launch.working_directory().join("started")).await?;
    timeout(
        TEST_DEADLINE,
        poll_fn(|context| match process.poll_config(&mut config, context) {
            Poll::Ready(_) => Poll::Ready(Err(io::Error::other(
                "Config write completed before the reader was released",
            ))),
            Poll::Pending if config.written > 0 => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }),
    )
    .await??;
    assert!(config.written < config.startup_input.len());
    let retained_offset = config.written;
    assert!(
        poll_fn(|context| Poll::Ready(process.poll_config(&mut config, context)))
            .await
            .is_pending()
    );
    assert_eq!(config.written, retained_offset);

    std::fs::write(launch.working_directory().join("read-config"), b"release")?;
    timeout(
        TEST_DEADLINE,
        poll_fn(|context| process.poll_config(&mut config, context)),
    )
    .await??;
    wait_for_file(&launch.working_directory().join("config.received")).await?;
    assert_eq!(
        std::fs::read_to_string(launch.working_directory().join("config.received"))?,
        expected
    );
    process.request_force_stop()?;
    timeout(TEST_DEADLINE, process.reap_after_stop()).await??;
    Ok(())
}

#[test]
fn startup_bytes_and_queue_paths_match_the_shared_contract() -> Result<(), Box<dyn Error>> {
    use super::{PluginBells, PluginSdkConfig, PluginStartupWriter, SinkChannel};
    use crate::identifiers::FlowId;
    use crate::pipeline::contract_test_support::egress_queue_path;
    use std::path::{Path, PathBuf};
    let vectors: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/contracts/plugin/process_protocol_test_vectors.json"
    )))?;
    for vector in vectors["startup"]["valid"]
        .as_array()
        .ok_or("Startup vectors must be an array")?
    {
        let sdk_config = &vector["sdkConfig"];
        let (has_source, has_sink) =
            match vector["interface"].as_str().ok_or("Interface is missing")? {
                "source" => (true, false),
                "sink" => (false, true),
                "source-and-sink" => (true, true),
                _ => return Err("Unexpected vector interface".into()),
            };
        let source_channel_region = sdk_config["sourceChannelRegion"]
            .as_str()
            .map(PathBuf::from);
        assert_eq!(
            source_channel_region.is_some(),
            has_source,
            "{}",
            vector["name"]
        );
        let sink_inputs = match sdk_config["sinkInputs"].as_array() {
            Some(identities) => {
                let mut inputs = Vec::new();
                for (index, identity) in identities.iter().enumerate() {
                    let flow = identity["flowId"]
                        .as_str()
                        .ok_or("Flow identity is missing")?;
                    let channel = u32::try_from(
                        identity["channelId"]
                            .as_u64()
                            .ok_or("Channel index is missing")?,
                    )?;
                    assert_eq!(
                        egress_queue_path(Path::new(""), flow, channel),
                        Path::new(
                            vector["relativeQueuePaths"][index]
                                .as_str()
                                .ok_or("Queue path is missing")?
                        )
                    );
                    inputs.push(SinkChannel {
                        flow_id: FlowId::try_from(flow.to_owned())?,
                        channel_id: channel,
                        channel_bell_path: PathBuf::from(
                            identity["channelBellPath"]
                                .as_str()
                                .ok_or("Channel doorbell path is missing")?,
                        ),
                    });
                }
                Some(inputs)
            }
            None => None,
        };
        assert_eq!(sink_inputs.is_some(), has_sink, "{}", vector["name"]);
        let bells = PluginBells {
            source_channel_region,
            sink_inputs,
        };
        let expected_arguments = vector["arguments"]
            .as_array()
            .ok_or("Startup arguments are missing")?;
        let launch_id = sdk_config["launchId"]
            .as_str()
            .ok_or("Launch identity is missing")?;
        assert_eq!(expected_arguments.len(), 2, "{}", vector["name"]);
        assert_eq!(
            expected_arguments[0].as_str(),
            Some("--sdk-config"),
            "{}",
            vector["name"]
        );
        let encoded = serde_json::to_string(&PluginSdkConfig {
            working_directory: Path::new(
                sdk_config["workingDirectory"]
                    .as_str()
                    .ok_or("Working directory is missing")?,
            ),
            control_socket: Path::new(
                sdk_config["controlSocket"]
                    .as_str()
                    .ok_or("Control socket is missing")?,
            ),
            launch_id: launch_id.to_owned(),
            bells: &bells,
        })?;
        assert_eq!(
            expected_arguments[1].as_str(),
            Some(encoded.as_str()),
            "{}",
            vector["name"]
        );
        assert_eq!(
            &serde_json::from_str::<serde_json::Value>(&encoded)?,
            sdk_config,
            "{}",
            vector["name"]
        );
        let writer = PluginStartupWriter::new(&vector["config"])?;
        assert_eq!(
            writer.startup_input.as_slice(),
            vector["stdin"]
                .as_str()
                .ok_or("Startup bytes are missing")?
                .as_bytes(),
            "{}",
            vector["name"]
        );
    }
    Ok(())
}
