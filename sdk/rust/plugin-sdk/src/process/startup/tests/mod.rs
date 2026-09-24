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

use super::*;

const VECTORS: &str = include_str!("../../../../contracts/process_protocol_test_vectors.json");

fn sink_input_values(inputs: &[SinkInput]) -> Value {
    Value::Array(
        inputs
            .iter()
            .map(|input| {
                serde_json::json!({
                    "flowId": input.channel.flow_id,
                    "channelId": input.channel.channel_id,
                    "channelBellPath": input.channel_bell_path,
                })
            })
            .collect(),
    )
}

fn startups(outcome: &str) -> Result<Vec<Value>, Error> {
    let vectors: Value = serde_json::from_str(VECTORS)?;
    Ok(vectors["startup"][outcome]
        .as_array()
        .ok_or("missing startup vectors")?
        .clone())
}

fn arguments(vector: &Value) -> Result<Vec<OsString>, Error> {
    Ok(vector["arguments"]
        .as_array()
        .ok_or("missing arguments")?
        .iter()
        .map(|value| value.as_str().map(OsString::from).ok_or("invalid argument"))
        .collect::<Result<Vec<_>, _>>()?)
}

fn stdin(vector: &Value) -> Result<&str, Error> {
    Ok(vector["stdin"].as_str().ok_or("missing stdin")?)
}

fn expected_error_code(vector: &Value) -> Result<&str, Error> {
    Ok(vector["expectedErrorCode"]
        .as_str()
        .ok_or("missing error")?)
}

/// Asserts the startup document one vector was parsed into, key by key.
///
/// The vector states the document twice: as the reserved option text the
/// Pipeline writes and as the value both SDKs must recover from it. An
/// Interface that does not ring a direction carries no key for it at all.
fn assert_startup_document(startup: &Startup, vector: &Value) -> Result<(), Error> {
    let document = &vector["sdkConfig"];
    assert_eq!(
        startup.working_directory.to_str(),
        document["workingDirectory"].as_str(),
        "{}",
        vector["name"]
    );
    assert_eq!(
        startup.control_socket.to_str(),
        document["controlSocket"].as_str(),
        "{}",
        vector["name"]
    );
    assert_eq!(
        STANDARD.encode(&startup.launch_id),
        document["launchId"].as_str().expect("a launch id is text"),
        "{}",
        vector["name"]
    );
    assert_eq!(
        startup.bells.source_channel_region,
        document["sourceChannelRegion"].as_str().map(PathBuf::from),
        "{}",
        vector["name"]
    );
    assert_eq!(
        &startup
            .bells
            .sink_inputs
            .as_ref()
            .map(|inputs| sink_input_values(inputs))
            .unwrap_or(Value::Null),
        &document["sinkInputs"].clone(),
        "{}",
        vector["name"]
    );
    Ok(())
}

#[test]
fn startup_matches_shared_source_vectors() -> Result<(), Error> {
    for outcome in ["invalid", "valid"] {
        for vector in startups(outcome)? {
            if vector
                .get("interface")
                .is_some_and(|value| value != "source")
            {
                continue;
            }
            let mut input = stdin(&vector)?.as_bytes();
            let result = read(arguments(&vector)?.into_iter(), &mut input);
            if outcome == "invalid" {
                // A Source startup that parsed its startup document still has to
                // demand the Channel doorbell Region, so the vectors that omit it
                // fail there instead. Every vector must fail somewhere in that
                // sequence with the code it declares.
                let error = match result {
                    Err(error) => error,
                    Ok(startup) => {
                        require_channel_bell_path(&startup).expect_err("invalid startup must fail")
                    }
                };
                assert_eq!(error.to_string(), expected_error_code(&vector)?);
            } else {
                let startup = result?;
                assert_eq!(startup.config, vector["config"]);
                assert_startup_document(&startup, &vector)?;
                assert!(
                    input.is_empty(),
                    "the config line ends the Source startup input"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn program_arguments_and_extra_args_before_the_reserved_option_do_not_break_startup()
-> Result<(), Error> {
    let mut arguments = vec![OsString::from("--device"), OsString::from("/dev/tty0")];
    arguments.extend(sdk_config(&[("sinkInputs", serde_json::json!([]))]));
    let startup = read(arguments.into_iter(), &mut b"{}\n".as_slice())?;
    assert_eq!(startup.working_directory, PathBuf::from("/tmp/instance"));
    assert_eq!(startup.control_socket, PathBuf::from("/tmp/control"));
    assert_eq!(
        STANDARD.encode(&startup.launch_id),
        "AAECAwQFBgcICQoLDA0ODw=="
    );
    assert!(
        startup
            .bells
            .sink_inputs
            .as_ref()
            .is_some_and(Vec::is_empty)
    );
    assert_eq!(startup.config, serde_json::json!({}));
    Ok(())
}

#[test]
fn source_startup_rejects_a_missing_channel_doorbell_region() -> Result<(), Error> {
    let startup = read(sdk_config(&[]).into_iter(), &mut b"{}\n".as_slice())?;
    assert_eq!(startup.bells.source_channel_region, None);
    let error = require_channel_bell_path(&startup).expect_err("a Source demands its Region");
    assert_eq!(
        error.to_string(),
        "plugin.startup.channel_bell_path_invalid"
    );
    Ok(())
}

#[test]
fn rejects_duplicate_keys_cr_and_invalid_utf8_without_consuming_next_line() -> Result<(), Error> {
    let arguments = || sdk_config(&[]).into_iter();
    for input in [b"{\"x\":1,\"x\":2}\n".as_slice(), b"\xff\n", b"{}"] {
        let error = read(arguments(), &mut &input[..]).expect_err("invalid config must fail");
        assert_eq!(error.to_string(), "plugin.startup.config_invalid");
        assert!(
            error.source().is_some(),
            "original input failure is retained"
        );
    }
    assert!(read(arguments(), &mut b"{}\r\n".as_slice()).is_err());
    let mut invalid_startup = arguments().collect::<Vec<_>>();
    invalid_startup[1] = "{}".into();
    let error = read(invalid_startup.into_iter(), &mut b"{}\n".as_slice())
        .expect_err("a document without a launch id must fail");
    assert_eq!(error.to_string(), "plugin.startup.sdk_config_invalid");
    let mut invalid_launch_id = arguments().collect::<Vec<_>>();
    invalid_launch_id[1] = serde_json::to_string(&serde_json::json!({
        "workingDirectory": "/tmp/instance",
        "controlSocket": "/tmp/control",
        "launchId": "?",
    }))?
    .into();
    let error = read(invalid_launch_id.into_iter(), &mut b"{}\n".as_slice())
        .expect_err("invalid launch ID must fail");
    assert_eq!(error.to_string(), "plugin.startup.launch_id_invalid");
    assert!(
        error
            .source()
            .and_then(|cause| cause.downcast_ref::<base64::DecodeError>())
            .is_some(),
        "original Base64 failure is retained"
    );
    let mut input = b"123456789012345678901234567890.0123456789\nnext\n".as_slice();
    assert_eq!(
        read(arguments(), &mut input)?.config.to_string(),
        "123456789012345678901234567890.0123456789"
    );
    assert_eq!(input, b"next\n");
    Ok(())
}

#[test]
fn sink_inputs_come_from_the_shared_vectors() -> Result<(), Error> {
    for outcome in ["valid", "invalid"] {
        for vector in startups(outcome)? {
            if !matches!(
                vector["interface"].as_str(),
                Some("sink" | "source-and-sink")
            ) {
                continue;
            }
            let mut input = stdin(&vector)?.as_bytes();
            let result = read(arguments(&vector)?.into_iter(), &mut input);
            if outcome == "invalid" {
                // A Sink whose startup document parsed still has to demand its
                // inputs, so the vector that omits them fails there instead.
                let error = match result {
                    Err(error) => error,
                    Ok(startup) => {
                        require_sink_inputs(&startup).expect_err("invalid startup must fail")
                    }
                };
                assert_eq!(error.to_string(), expected_error_code(&vector)?);
            } else {
                let startup = result?;
                assert_eq!(
                    sink_input_values(&require_sink_inputs(&startup)?),
                    vector["sdkConfig"]["sinkInputs"]
                );
                assert_startup_document(&startup, &vector)?;
                assert!(
                    input.is_empty(),
                    "the config line ends the Sink startup input"
                );
            }
        }
    }
    Ok(())
}

/// The reserved option the Pipeline appends after every Program argument.
fn sdk_config(directions: &[(&str, Value)]) -> Vec<OsString> {
    let mut document = serde_json::Map::new();
    document.insert("workingDirectory".into(), "/tmp/instance".into());
    document.insert("controlSocket".into(), "/tmp/control".into());
    document.insert("launchId".into(), "AAECAwQFBgcICQoLDA0ODw==".into());
    for (key, value) in directions {
        document.insert((*key).to_owned(), value.clone());
    }
    vec![
        OsString::from(SDK_CONFIG_OPTION),
        OsString::from(serde_json::to_string(&Value::Object(document)).expect("serializable")),
    ]
}
