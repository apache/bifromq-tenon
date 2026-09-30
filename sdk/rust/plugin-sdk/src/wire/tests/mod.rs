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

use super::{plugin, sink, source};
use prost::Message;
use serde_json::Value;

#[test]
fn lifecycle_wire_matches_shared_vectors() -> Result<(), Box<dyn std::error::Error>> {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../contracts/test-fixtures/process_control_test_vectors.json"
    ))?;
    for item in vectors["valid"].as_array().ok_or("missing valid vectors")? {
        let bytes: Vec<u8> = serde_json::from_value(item["encoded"].clone())?;
        let encoded = match item["direction"].as_str() {
            Some("pluginToPipeline") => {
                plugin::PluginToPipeline::decode(bytes.as_slice())?.encode_to_vec()
            }
            Some("pipelineToPlugin") => {
                plugin::PipelineToPlugin::decode(bytes.as_slice())?.encode_to_vec()
            }
            _ => return Err("unknown vector direction".into()),
        };
        assert_eq!(encoded, bytes, "{}", item["name"]);
    }
    for item in vectors["malformed"]
        .as_array()
        .ok_or("missing malformed vectors")?
    {
        let bytes: Vec<u8> = serde_json::from_value(item["encoded"].clone())?;
        let rejected = match item["direction"].as_str() {
            Some("pluginToPipeline") => plugin::PluginToPipeline::decode(bytes.as_slice()).is_err(),
            Some("pipelineToPlugin") => plugin::PipelineToPlugin::decode(bytes.as_slice()).is_err(),
            _ => return Err("unknown vector direction".into()),
        };
        assert!(rejected, "{}", item["name"]);
    }
    Ok(())
}

#[test]
fn source_record_preserves_unsigned_identity_and_payload() -> Result<(), Box<dyn std::error::Error>>
{
    let record = source::IngressRecord {
        record_id: u64::MAX,
        payload: bytes::Bytes::from_static(b"record"),
    };
    assert_eq!(
        source::IngressRecord::decode(record.encode_to_vec().as_slice())?,
        record
    );
    Ok(())
}

#[test]
fn sink_record_decoding_matches_shared_vectors_including_empty_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../contracts/test-fixtures/egress_record_test_vectors.json"
    ))?;
    for vector in vectors["valid"].as_array().ok_or("missing Sink vectors")? {
        let encoded: Vec<u8> = serde_json::from_value(vector["encoded"].clone())?;
        let record = sink::EgressRecord::decode(encoded.as_slice())?;
        assert_eq!(
            record.payload.as_ref(),
            serde_json::from_value::<Vec<u8>>(vector["payload"].clone())?
        );
    }
    for vector in vectors["malformed"]
        .as_array()
        .ok_or("missing malformed Sink vectors")?
    {
        let encoded: Vec<u8> = serde_json::from_value(vector["encoded"].clone())?;
        assert!(sink::EgressRecord::decode(encoded.as_slice()).is_err());
    }
    Ok(())
}

#[test]
fn source_and_completion_bytes_match_shared_vectors() -> Result<(), Box<dyn std::error::Error>> {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../contracts/test-fixtures/ingress_record_test_vectors.json"
    ))?;
    for vector in vectors["valid"]
        .as_array()
        .ok_or("missing ingress vectors")?
    {
        let bytes: Vec<u8> = serde_json::from_value(vector["encoded"].clone())?;
        let record = source::IngressRecord::decode(bytes.as_slice())?;
        assert_eq!(
            record.record_id,
            vector["recordId"].as_u64().ok_or("missing record id")?
        );
        assert_eq!(
            record.payload.as_ref(),
            serde_json::from_value::<Vec<u8>>(vector["payload"].clone())?
        );
        assert_eq!(record.encode_to_vec(), bytes);
    }
    for vector in vectors["completionValid"]
        .as_array()
        .ok_or("missing completion vectors")?
    {
        let bytes: Vec<u8> = serde_json::from_value(vector["encoded"].clone())?;
        let completion = source::IngressCompletion::decode(bytes.as_slice())?;
        assert_eq!(
            completion.record_id,
            vector["recordId"].as_u64().ok_or("missing record id")?
        );
        let status = match vector["status"].as_str() {
            Some("OK") => 1,
            Some("RETRY") => 2,
            Some("BACKPRESSURE") => 3,
            Some("ERROR") => 4,
            _ => return Err("unknown completion status".into()),
        };
        assert_eq!(completion.status, status);
        assert_eq!(completion.encode_to_vec(), bytes);
        assert!(bytes.len() <= 13);
    }
    for vector in vectors["malformed"]
        .as_array()
        .ok_or("missing malformed ingress vectors")?
    {
        let bytes: Vec<u8> = serde_json::from_value(vector["encoded"].clone())?;
        assert!(source::IngressRecord::decode(bytes.as_slice()).is_err());
    }
    Ok(())
}

#[test]
fn generated_lifecycle_descriptor_matches_the_field_registry()
-> Result<(), Box<dyn std::error::Error>> {
    let registry: Value = serde_json::from_str(include_str!(
        "../../../contracts/test-fixtures/process_control_field_registry.json"
    ))?;
    let descriptor = prost_types::FileDescriptorSet::decode(
        include_bytes!(concat!(env!("OUT_DIR"), "/process_control_descriptor.pb")).as_slice(),
    )?;
    let file = descriptor
        .file
        .iter()
        .find(|file| file.package.as_deref() == Some("tenon.plugin"))
        .ok_or("missing package")?;
    let mut messages = serde_json::Map::new();
    let mut oneofs = serde_json::Map::new();
    for message in &file.message_type {
        assert!(message.reserved_name.is_empty() && message.reserved_range.is_empty());
        let name = message.name.as_deref().ok_or("missing message name")?;
        let mut fields = serde_json::Map::new();
        for field in &message.field {
            fields.insert(
                field.name.clone().ok_or("missing field name")?,
                Value::from(field.number.ok_or("missing field number")?),
            );
        }
        messages.insert(name.to_owned(), Value::Object(fields));
        let mut declarations = serde_json::Map::new();
        for (index, oneof) in message.oneof_decl.iter().enumerate() {
            let names = message
                .field
                .iter()
                .filter(|field| field.oneof_index == Some(index as i32))
                .map(|field| field.name.clone().ok_or("missing oneof field"))
                .collect::<Result<Vec<_>, _>>()?;
            declarations.insert(
                oneof.name.clone().ok_or("missing oneof name")?,
                serde_json::to_value(names)?,
            );
        }
        if !declarations.is_empty() {
            oneofs.insert(name.to_owned(), Value::Object(declarations));
        }
    }
    assert_eq!(Value::Object(messages), registry["messages"]);
    assert_eq!(Value::Object(oneofs), registry["oneofs"]);
    assert!(file.enum_type.is_empty());
    let service = file.service.first().ok_or("missing service")?;
    assert_eq!(
        service.name.as_deref(),
        registry["service"]["name"].as_str()
    );
    assert_eq!(service.method.len(), 1);
    let method = &service.method[0];
    assert_eq!(
        method.name.as_deref(),
        registry["service"]["method"].as_str()
    );
    assert_eq!(method.client_streaming, Some(true));
    assert_eq!(method.server_streaming, Some(true));
    assert_eq!(
        method.input_type.as_deref(),
        Some(".tenon.plugin.PluginToPipeline")
    );
    assert_eq!(
        method.output_type.as_deref(),
        Some(".tenon.plugin.PipelineToPlugin")
    );
    Ok(())
}
