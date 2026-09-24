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

use crate::json::parse_json;
use crate::sink::SinkInput;
use crate::{Error, FlowChannel, Value};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use std::ffi::OsString;
use std::fmt;
use std::io::{self, Read};
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;

/// The arguments the Pipeline reserves for itself.
///
/// The Pipeline appends this option after every Program argument and
/// `extraArgs` entry, so a Program that declares its own arguments still
/// starts. Everything before it belongs to the Program, and this reader
/// ignores it.
const RESERVED_ARGUMENTS: usize = 2;
const SDK_CONFIG_OPTION: &str = "--sdk-config";
const LAUNCH_ID_LENGTH: usize = 16;

#[derive(Debug)]
pub(crate) struct Startup {
    pub(crate) working_directory: PathBuf,
    pub(crate) control_socket: PathBuf,
    pub(crate) launch_id: Vec<u8>,
    /// The doorbell wiring this Instance was told about.
    pub(crate) bells: Bells,
    pub(crate) config: Value,
}

/// The Channel doorbell Regions one Instance rings.
///
/// A key is absent exactly when the Interface does not include that direction;
/// a direction this Interface does implement but was not told about is a
/// startup failure rather than a fallback.
#[derive(Debug)]
pub(crate) struct Bells {
    /// The Flow Region the Source side rings, when this Interface Sources one.
    pub(crate) source_channel_region: Option<PathBuf>,
    /// Every Sink input in startup order, when this Interface consumes Flows.
    pub(crate) sink_inputs: Option<Vec<SinkInput>>,
}

/// One Plugin process's whole startup document, as its reserved option carries
/// it.
///
/// Every key is required unless the direction it belongs to may be absent: a
/// missing key is malformed startup, and an unknown key is rejected rather than
/// ignored so a misspelled direction fails loudly.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SdkConfigValue {
    working_directory: String,
    control_socket: String,
    launch_id: String,
    #[serde(default)]
    source_channel_region: Option<String>,
    #[serde(default)]
    sink_inputs: Option<Vec<SinkInputValue>>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SinkInputValue {
    flow_id: String,
    channel_id: u32,
    channel_bell_path: PathBuf,
}

pub(crate) fn read(
    arguments: impl Iterator<Item = OsString>,
    input: &mut impl Read,
) -> Result<Startup, Error> {
    let arguments: Vec<_> = arguments.collect();
    let Some(reserved_start) = arguments.len().checked_sub(RESERVED_ARGUMENTS) else {
        return Err(invalid("plugin.startup.arguments_invalid").into());
    };
    if arguments[reserved_start] != SDK_CONFIG_OPTION {
        return Err(invalid("plugin.startup.arguments_invalid").into());
    }
    let sdk_config = read_sdk_config(&arguments[reserved_start + 1])?;
    let working_directory = absolute_path(
        &sdk_config.working_directory,
        "plugin.startup.working_directory_invalid",
    )?;
    let control_socket = absolute_path(
        &sdk_config.control_socket,
        "plugin.startup.control_socket_invalid",
    )?;
    let launch_id = launch_id(&sdk_config.launch_id)?;
    let source_channel_region = sdk_config
        .source_channel_region
        .map(|path| absolute_path(&path, "plugin.startup.channel_bell_path_invalid"))
        .transpose()?;
    let sink_inputs = sdk_config
        .sink_inputs
        .map(|inputs| {
            inputs
                .into_iter()
                .map(absolute_sink_input)
                .collect::<Result<Vec<_>, Error>>()
        })
        .transpose()?;
    let config = read_json_line(input, "plugin.startup.config_invalid")?;
    Ok(Startup {
        working_directory,
        control_socket,
        launch_id,
        bells: Bells {
            source_channel_region,
            sink_inputs,
        },
        config,
    })
}

/// Returns the Channel doorbell Region an Interface that Sources a Flow must ring.
///
/// A Source can neither ring the Channels that read its Submissions nor publish
/// the ordinals those Channels ring in return, so the Pipeline always names it
/// for those Interfaces. Its absence is a startup failure rather than a
/// fallback.
pub(crate) fn require_channel_bell_path(startup: &Startup) -> Result<PathBuf, Error> {
    startup
        .bells
        .source_channel_region
        .clone()
        .ok_or_else(|| invalid("plugin.startup.channel_bell_path_invalid").into())
}

/// Returns every Sink input an Interface that consumes Flows must be told about.
///
/// A Sink takes each Egress Queue from a list element and picks its own-loop
/// Bell slot itself, so the Pipeline always names the list for those
/// Interfaces. Its absence is a startup failure rather than an idle Sink.
pub(crate) fn require_sink_inputs(startup: &Startup) -> Result<Vec<SinkInput>, Error> {
    startup
        .bells
        .sink_inputs
        .clone()
        .ok_or_else(|| invalid("plugin.startup.sink_channels_invalid").into())
}

fn read_sdk_config(argument: &OsString) -> Result<SdkConfigValue, Error> {
    let text = argument
        .to_str()
        .ok_or_else(|| invalid("plugin.startup.sdk_config_invalid"))?;
    let value = parse_json(text.as_bytes()).map_err(|cause| malformed_startup(cause.into()))?;
    serde_json::from_value(value).map_err(|cause| malformed_startup(cause.into()).into())
}

fn launch_id(encoded: &str) -> Result<Vec<u8>, Error> {
    let decoded = STANDARD.decode(encoded).map_err(|cause| InvalidStartup {
        code: "plugin.startup.launch_id_invalid",
        cause: cause.into(),
    })?;
    if decoded.len() != LAUNCH_ID_LENGTH || STANDARD.encode(&decoded) != encoded {
        return Err(invalid("plugin.startup.launch_id_invalid").into());
    }
    Ok(decoded)
}

/// A startup document that is not one object of known keys.
fn malformed_startup(cause: Error) -> InvalidStartup {
    InvalidStartup {
        code: "plugin.startup.sdk_config_invalid",
        cause,
    }
}

fn absolute_path(value: &str, code: &'static str) -> Result<PathBuf, Error> {
    let path = PathBuf::from(value);
    if path.is_absolute() && !value.as_bytes().contains(&0) {
        Ok(path)
    } else {
        Err(invalid(code).into())
    }
}

fn absolute_sink_input(input: SinkInputValue) -> Result<SinkInput, Error> {
    if input.channel_bell_path.is_absolute()
        && !input.channel_bell_path.as_os_str().as_bytes().contains(&0)
    {
        Ok(SinkInput {
            channel: FlowChannel {
                flow_id: input.flow_id,
                channel_id: input.channel_id,
            },
            channel_bell_path: input.channel_bell_path,
        })
    } else {
        Err(invalid("plugin.startup.sink_channels_invalid").into())
    }
}

fn read_json_line(input: &mut impl Read, code: &'static str) -> Result<Value, Error> {
    let mut bytes = Vec::new();
    loop {
        let mut byte = [0];
        input
            .read_exact(&mut byte)
            .map_err(|cause| InvalidStartup {
                code,
                cause: cause.into(),
            })?;
        match byte[0] {
            b'\n' => break,
            b'\r' => {
                return Err(InvalidStartup {
                    code,
                    cause: invalid("Startup input contains CR").into(),
                }
                .into());
            }
            byte => bytes.push(byte),
        }
    }
    parse_json(&bytes).map_err(|cause| {
        InvalidStartup {
            code,
            cause: cause.into(),
        }
        .into()
    })
}

#[derive(Debug)]
struct InvalidStartup {
    code: &'static str,
    cause: Error,
}

impl fmt::Display for InvalidStartup {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(self.code)
    }
}

impl std::error::Error for InvalidStartup {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}

fn invalid(code: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, code)
}

#[cfg(test)]
mod tests;
