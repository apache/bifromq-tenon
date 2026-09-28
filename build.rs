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

use std::env;
use std::io;
use std::path::PathBuf;

const INGRESS_RECORD_PROTO: &str = "contracts/source/ingress_record.proto";
const EGRESS_RECORD_PROTO: &str = "contracts/sink/egress_record.proto";
const PIPELINE_CONTROL_PROTO: &str = "contracts/core/pipeline_control.proto";
const PLUGIN_PROCESS_CONTROL_PROTO: &str = "contracts/plugin/process_control.proto";
const PLUGIN_PROCESS_METRICS_PROTO: &str = "contracts/plugin/process_metrics.proto";

fn main() -> io::Result<()> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={INGRESS_RECORD_PROTO}");
    println!("cargo:rerun-if-changed={EGRESS_RECORD_PROTO}");
    println!("cargo:rerun-if-changed={PIPELINE_CONTROL_PROTO}");
    println!("cargo:rerun-if-changed={PLUGIN_PROCESS_CONTROL_PROTO}");
    println!("cargo:rerun-if-changed={PLUGIN_PROCESS_METRICS_PROTO}");

    let protoc = protoc_bin_vendored::protoc_bin_path().map_err(io::Error::other)?;
    let out_directory = env::var_os("OUT_DIR")
        .map(PathBuf::from)
        .ok_or_else(|| io::Error::other("Cargo OUT_DIR is unavailable"))?;

    let mut source_config = prost_build::Config::new();
    source_config.protoc_executable(&protoc);
    source_config.bytes([".tenon.source.IngressRecord.payload"]);
    source_config.compile_protos(&[INGRESS_RECORD_PROTO], &["contracts/source"])?;

    let mut sink_config = prost_build::Config::new();
    sink_config.protoc_executable(&protoc);
    sink_config.compile_protos(&[EGRESS_RECORD_PROTO], &["contracts/sink"])?;

    let mut core_config = prost_build::Config::new();
    core_config.protoc_executable(&protoc);
    core_config.file_descriptor_set_path(out_directory.join("pipeline_control_descriptor.pb"));
    tonic_prost_build::configure().compile_with_config(
        core_config,
        &[PIPELINE_CONTROL_PROTO],
        &["contracts/core"],
    )?;

    let mut plugin_config = prost_build::Config::new();
    plugin_config.protoc_executable(&protoc);
    plugin_config.file_descriptor_set_path(out_directory.join("process_control_descriptor.pb"));
    tonic_prost_build::configure().compile_with_config(
        plugin_config,
        &[PLUGIN_PROCESS_CONTROL_PROTO, PLUGIN_PROCESS_METRICS_PROTO],
        &["contracts/plugin"],
    )
}
