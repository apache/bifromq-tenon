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

//! Generates the SDK's private messages from the shared wire contracts.

use std::env;
use std::io;
use std::path::PathBuf;

const INGRESS_RECORD_PROTO: &str = "contracts/ingress_record.proto";
const EGRESS_RECORD_PROTO: &str = "contracts/egress_record.proto";
const PLUGIN_PROCESS_CONTROL_PROTO: &str = "contracts/process_control.proto";
const PLUGIN_PROCESS_METRICS_PROTO: &str = "contracts/process_metrics.proto";

fn main() -> io::Result<()> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={PLUGIN_PROCESS_CONTROL_PROTO}");
    println!("cargo:rerun-if-changed={PLUGIN_PROCESS_METRICS_PROTO}");
    println!("cargo:rerun-if-changed={INGRESS_RECORD_PROTO}");
    println!("cargo:rerun-if-changed={EGRESS_RECORD_PROTO}");
    let protoc = protoc_bin_vendored::protoc_bin_path().map_err(io::Error::other)?;
    let output =
        PathBuf::from(env::var_os("OUT_DIR").ok_or_else(|| io::Error::other("missing OUT_DIR"))?);
    let mut config = prost_build::Config::new();
    config.protoc_executable(&protoc);
    config.bytes([
        ".tenon.source.IngressRecord.payload",
        ".tenon.sink.EgressRecord.payload",
    ]);
    config.compile_protos(&[INGRESS_RECORD_PROTO, EGRESS_RECORD_PROTO], &["contracts"])?;
    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc);
    config.file_descriptor_set_path(output.join("process_control_descriptor.pb"));
    tonic_prost_build::configure()
        .build_server(std::env::var_os("CARGO_FEATURE_REPOSITORY_TEST_SUPPORT").is_some())
        .compile_with_config(
            config,
            &[PLUGIN_PROCESS_CONTROL_PROTO, PLUGIN_PROCESS_METRICS_PROTO],
            &["contracts"],
        )
}
