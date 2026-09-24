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

//! Validates and identifies exact Source and Sink Payload Contract descriptor bytes.
//!
//! This module owns the pure boundary from untrusted `FileDescriptorSet` bytes to
//! validated Source or Sink contract values.
//! It validates Tenon's proto3 profile, resolves the complete Protobuf type graph,
//! preserves the original bytes, and derives each declared interface's root and
//! reachable type graph.
//!
//! It does not scan installation directories, expose HTTP, or create Lua and SDK
//! values. Callers perform I/O and then transfer the read buffer into
//! a contract parser.
//! A successful value owns immutable bytes and its descriptor graph. Callers can then
//! freeze complete discovered contracts under their exact Plugin identities. The
//! module has no mutable global state, concurrency, shutdown path, or unsafe code;
//! callers decide how to report a rejected installation and whether to continue
//! discovering other programs.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error;
use std::fmt;

use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, Kind, MessageDescriptor};
use prost_types::{DescriptorProto, EnumDescriptorProto, FileDescriptorSet};
use serde::{Deserialize, Serialize};

const SINK_ROOT_MESSAGE_NAME: &str = "SinkRecordPayload";
const SOURCE_ROOT_MESSAGE_NAME: &str = "SourceRecordPayload";
const OPTION_MESSAGE_NAMES: &[&str] = &[
    "google.protobuf.FileOptions",
    "google.protobuf.MessageOptions",
    "google.protobuf.FieldOptions",
    "google.protobuf.OneofOptions",
    "google.protobuf.EnumOptions",
    "google.protobuf.EnumValueOptions",
    "google.protobuf.ServiceOptions",
    "google.protobuf.MethodOptions",
    "google.protobuf.ExtensionRangeOptions",
];

/// The exact closed interface capability declared by one Plugin Program.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum PluginInterface {
    /// The Program produces records for one Flow.
    Source,
    /// The Program consumes records from one or more Flows.
    Sink,
    /// One process implements both standard interfaces.
    SourceAndSink,
}

/// One validated current Program descriptor with interface-specific standard roots.
pub(crate) struct PluginProgramPayloadContract {
    descriptor_bytes: Box<[u8]>,
    descriptor_pool: DescriptorPool,
}

impl fmt::Debug for PluginProgramPayloadContract {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginProgramPayloadContract")
            .field("descriptor_len", &self.descriptor_bytes.len())
            .field(
                "source_root",
                &self
                    .source_root_message()
                    .map(|message| message.full_name().to_owned()),
            )
            .field(
                "sink_root",
                &self
                    .sink_root_message()
                    .map(|message| message.full_name().to_owned()),
            )
            .finish()
    }
}

impl PluginProgramPayloadContract {
    /// Restores the same-build Runner's already verified descriptor graph.
    #[allow(
        clippy::expect_used,
        reason = "Runner forwards descriptor bytes from its verified Program Store"
    )]
    pub(crate) fn from_runner_bytes(descriptor_bytes: Vec<u8>) -> Self {
        let descriptor_pool = DescriptorPool::decode(descriptor_bytes.as_slice())
            .expect("Runner must preserve its verified descriptor graph");
        Self {
            descriptor_bytes: descriptor_bytes.into_boxed_slice(),
            descriptor_pool,
        }
    }

    /// Parses a descriptor whose standard roots must exactly match `interface`.
    ///
    /// # Errors
    ///
    /// Returns [`PayloadContractError`] when the bytes violate the shared
    /// descriptor profile or contain missing, duplicate, or undeclared standard
    /// interface roots.
    pub(crate) fn parse(
        descriptor_bytes: Vec<u8>,
        interface: PluginInterface,
    ) -> Result<Self, PayloadContractError> {
        let validated = validate_payload_descriptor_set(descriptor_bytes)?;
        validate_program_roots(&validated.descriptor_set, interface)?;
        let parsed = resolve_payload_descriptor(validated)?;
        Ok(Self {
            descriptor_bytes: parsed.descriptor_bytes,
            descriptor_pool: parsed.descriptor_pool,
        })
    }

    /// Derives the Program interface from the standard roots validated at parse time.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn interface(&self) -> PluginInterface {
        match (
            self.source_root_message().is_some(),
            self.sink_root_message().is_some(),
        ) {
            (true, false) => PluginInterface::Source,
            (false, true) => PluginInterface::Sink,
            (true, true) => PluginInterface::SourceAndSink,
            (false, false) => {
                unreachable!("a validated Plugin Program must expose a standard interface root")
            }
        }
    }

    /// Derives the Source root when the descriptor declares that interface.
    #[must_use]
    pub(crate) fn source_root_message(&self) -> Option<MessageDescriptor> {
        top_level_root_message(&self.descriptor_pool, SOURCE_ROOT_MESSAGE_NAME)
    }

    /// Derives the Sink root when the descriptor declares that interface.
    #[must_use]
    pub(crate) fn sink_root_message(&self) -> Option<MessageDescriptor> {
        top_level_root_message(&self.descriptor_pool, SINK_ROOT_MESSAGE_NAME)
    }

    /// Borrows the Source Contract projection rooted in this Program descriptor.
    #[must_use]
    pub(crate) fn source_projection(&self) -> Option<PluginProgramPayloadContractProjection<'_>> {
        self.source_root_message()
            .map(|_| PluginProgramPayloadContractProjection {
                contract: self,
                interface: PayloadContractProjectionInterface::Source,
            })
    }

    /// Borrows the Sink Contract projection rooted in this Program descriptor.
    #[must_use]
    pub(crate) fn sink_projection(&self) -> Option<PluginProgramPayloadContractProjection<'_>> {
        self.sink_root_message()
            .map(|_| PluginProgramPayloadContractProjection {
                contract: self,
                interface: PayloadContractProjectionInterface::Sink,
            })
    }

    /// Returns the original FileDescriptorSet bytes without normalization.
    #[must_use]
    pub(crate) fn descriptor_bytes(&self) -> &[u8] {
        &self.descriptor_bytes
    }
}

/// A borrowed interface view over one Program descriptor's reachable type graph.
pub(crate) struct PluginProgramPayloadContractProjection<'a> {
    contract: &'a PluginProgramPayloadContract,
    interface: PayloadContractProjectionInterface,
}

impl PluginProgramPayloadContractProjection<'_> {
    /// Materializes the reachable Protobuf types for transient structural comparison.
    #[must_use]
    pub(crate) fn structural_material(&self) -> PayloadContractProjectionMaterial {
        PayloadContractProjectionMaterial {
            nodes: reachable_contract_nodes(&self.root_message()),
        }
    }

    /// Returns the already validated interface root without reparsing descriptor bytes.
    #[allow(
        clippy::expect_used,
        reason = "the immutable validated contract cannot lose a root after projection construction"
    )]
    pub(crate) fn root_message(&self) -> MessageDescriptor {
        match self.interface {
            PayloadContractProjectionInterface::Source => self
                .contract
                .source_root_message()
                .expect("validated Source projection must retain its root"),
            PayloadContractProjectionInterface::Sink => self
                .contract
                .sink_root_message()
                .expect("validated Sink projection must retain its root"),
        }
    }
}

#[derive(Clone, Copy)]
enum PayloadContractProjectionInterface {
    Source,
    Sink,
}

/// One transient interface projection used while comparing complete revisions.
#[derive(PartialEq)]
pub(crate) struct PayloadContractProjectionMaterial {
    nodes: BTreeMap<String, ReachableContractNode>,
}

#[derive(PartialEq)]
enum ReachableContractNode {
    Message(DescriptorProto),
    Enum(EnumDescriptorProto),
}

fn reachable_contract_nodes(root: &MessageDescriptor) -> BTreeMap<String, ReachableContractNode> {
    let mut nodes = BTreeMap::new();
    let mut pending = vec![Kind::Message(root.clone())];
    while let Some(kind) = pending.pop() {
        match kind {
            Kind::Message(message) => {
                if nodes.contains_key(message.full_name()) {
                    continue;
                }
                pending.extend(message.fields().filter_map(|field| match field.kind() {
                    Kind::Message(message) => Some(Kind::Message(message)),
                    Kind::Enum(enumeration) => Some(Kind::Enum(enumeration)),
                    _ => None,
                }));
                let mut descriptor = message.descriptor_proto().clone();
                descriptor.nested_type.clear();
                descriptor.enum_type.clear();
                descriptor.extension.clear();
                nodes.insert(
                    message.full_name().to_owned(),
                    ReachableContractNode::Message(descriptor),
                );
            }
            Kind::Enum(enumeration) => {
                nodes
                    .entry(enumeration.full_name().to_owned())
                    .or_insert_with(|| {
                        ReachableContractNode::Enum(enumeration.enum_descriptor_proto().clone())
                    });
            }
            _ => {}
        }
    }
    nodes
}

struct ParsedPayloadDescriptor {
    descriptor_bytes: Box<[u8]>,
    descriptor_pool: DescriptorPool,
}

struct ValidatedPayloadDescriptorSet {
    descriptor_bytes: Box<[u8]>,
    descriptor_set: FileDescriptorSet,
}

fn validate_payload_descriptor_set(
    descriptor_bytes: Vec<u8>,
) -> Result<ValidatedPayloadDescriptorSet, PayloadContractError> {
    let descriptor_set = FileDescriptorSet::decode(descriptor_bytes.as_slice())
        .map_err(|_| PayloadContractError::DescriptorMalformed)?;
    let edition_probe = DescriptorSetEditionProbe::decode(descriptor_bytes.as_slice())
        .map_err(|_| PayloadContractError::DescriptorMalformed)?;

    if descriptor_set.file.is_empty() {
        return Err(PayloadContractError::DescriptorEmpty);
    }

    validate_file_names_and_dependencies(&descriptor_set)?;
    reject_editions(&edition_probe)?;
    reject_custom_option_definitions(&descriptor_set)?;
    validate_proto3_and_source_info(&descriptor_set)?;
    validate_portable_field_names(&descriptor_set)?;

    Ok(ValidatedPayloadDescriptorSet {
        descriptor_bytes: descriptor_bytes.into_boxed_slice(),
        descriptor_set,
    })
}

fn resolve_payload_descriptor(
    validated: ValidatedPayloadDescriptorSet,
) -> Result<ParsedPayloadDescriptor, PayloadContractError> {
    let descriptor_pool = DescriptorPool::decode(validated.descriptor_bytes.as_ref())
        .map_err(|_| PayloadContractError::DescriptorInvalid)?;
    if let Some(file_name) = custom_option_data_file(&descriptor_pool) {
        return Err(PayloadContractError::CustomOptionUnsupported { file_name });
    }

    Ok(ParsedPayloadDescriptor {
        descriptor_bytes: validated.descriptor_bytes,
        descriptor_pool,
    })
}

/// A stable failure category for Payload Contract validation.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PayloadContractError {
    /// The input is not a binary FileDescriptorSet.
    DescriptorMalformed,
    /// The FileDescriptorSet contains no files.
    DescriptorEmpty,
    /// A descriptor file has no non-empty logical name.
    FileNameMissing,
    /// More than one descriptor file has the same logical name.
    FileNameDuplicate { file_name: String },
    /// A declared import is absent from the FileDescriptorSet.
    DependencyMissing {
        file_name: String,
        dependency: String,
    },
    /// A file uses Protobuf Editions.
    EditionsUnsupported { file_name: String },
    /// A file declares or carries a custom option.
    CustomOptionUnsupported { file_name: String },
    /// A file does not use exact proto3 syntax.
    SyntaxUnsupported {
        file_name: String,
        syntax: Option<String>,
    },
    /// A file does not contain compiler source information.
    SourceInfoMissing { file_name: String },
    /// Two fields in one message collide under Tenon's portable name normalization.
    FieldNameCollision {
        file_name: String,
        message_name: String,
        first_field: String,
        second_field: String,
    },
    /// The descriptor graph cannot be resolved into valid Protobuf types.
    DescriptorInvalid,
    /// A Source-capable Program has no top-level Source standard root.
    SourceRootMissing,
    /// A Source-capable Program has more than one top-level Source standard root.
    SourceRootNotUnique,
    /// A Sink-capable Program has no top-level Sink standard root.
    SinkRootMissing,
    /// A Sink-capable Program has more than one top-level Sink standard root.
    SinkRootNotUnique,
    /// A single-interface Program contains the other standard interface root.
    UndeclaredInterfaceRoot,
}

impl fmt::Display for PayloadContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DescriptorMalformed => formatter.write_str("Descriptor bytes are malformed"),
            Self::DescriptorEmpty => formatter.write_str("Descriptor set contains no files"),
            Self::FileNameMissing => formatter.write_str("Descriptor file name is missing"),
            Self::FileNameDuplicate { file_name } => {
                write!(formatter, "Descriptor file name is duplicated: {file_name}")
            }
            Self::DependencyMissing {
                file_name,
                dependency,
            } => write!(
                formatter,
                "Descriptor dependency is missing: {file_name} imports {dependency}"
            ),
            Self::EditionsUnsupported { file_name } => {
                write!(formatter, "Protobuf Editions are unsupported: {file_name}")
            }
            Self::CustomOptionUnsupported { file_name } => {
                write!(
                    formatter,
                    "Custom Protobuf options are unsupported: {file_name}"
                )
            }
            Self::SyntaxUnsupported { file_name, syntax } => write!(
                formatter,
                "Protobuf syntax is unsupported: {file_name} uses {}",
                syntax.as_deref().unwrap_or("an unspecified syntax")
            ),
            Self::SourceInfoMissing { file_name } => {
                write!(formatter, "Descriptor source info is missing: {file_name}")
            }
            Self::FieldNameCollision {
                file_name,
                message_name,
                first_field,
                second_field,
            } => write!(
                formatter,
                "Payload Contract field names collide after portable normalization: \
                 {file_name}:{message_name}.{first_field} and {second_field}"
            ),
            Self::DescriptorInvalid => formatter.write_str("Descriptor graph is invalid"),
            Self::SourceRootMissing => formatter.write_str("SourceRecordPayload root is missing"),
            Self::SourceRootNotUnique => {
                formatter.write_str("SourceRecordPayload root is not unique")
            }
            Self::SinkRootMissing => formatter.write_str("SinkRecordPayload root is missing"),
            Self::SinkRootNotUnique => formatter.write_str("SinkRecordPayload root is not unique"),
            Self::UndeclaredInterfaceRoot => formatter
                .write_str("Plugin Program descriptor contains an undeclared interface root"),
        }
    }
}

impl Error for PayloadContractError {}

fn validate_file_names_and_dependencies(
    descriptor_set: &FileDescriptorSet,
) -> Result<(), PayloadContractError> {
    let mut file_names = HashSet::with_capacity(descriptor_set.file.len());
    for file in &descriptor_set.file {
        let file_name = file
            .name
            .as_deref()
            .filter(|name| !name.is_empty())
            .ok_or(PayloadContractError::FileNameMissing)?;
        if !file_names.insert(file_name) {
            return Err(PayloadContractError::FileNameDuplicate {
                file_name: file_name.to_owned(),
            });
        }
    }

    for file in &descriptor_set.file {
        let file_name = file.name.as_deref().unwrap_or_default();
        for dependency in &file.dependency {
            if !file_names.contains(dependency.as_str()) {
                return Err(PayloadContractError::DependencyMissing {
                    file_name: file_name.to_owned(),
                    dependency: dependency.clone(),
                });
            }
        }
    }

    Ok(())
}

fn reject_editions(descriptor_set: &DescriptorSetEditionProbe) -> Result<(), PayloadContractError> {
    if let Some(file) = descriptor_set
        .file
        .iter()
        .find(|file| file.edition.is_some())
    {
        return Err(PayloadContractError::EditionsUnsupported {
            file_name: file.name.clone().unwrap_or_default(),
        });
    }
    Ok(())
}

fn reject_custom_option_definitions(
    descriptor_set: &FileDescriptorSet,
) -> Result<(), PayloadContractError> {
    for file in &descriptor_set.file {
        if file.extension.iter().any(is_custom_option_extension)
            || file.message_type.iter().any(message_defines_custom_option)
        {
            return Err(PayloadContractError::CustomOptionUnsupported {
                file_name: file.name.clone().unwrap_or_default(),
            });
        }
    }
    Ok(())
}

fn message_defines_custom_option(message: &DescriptorProto) -> bool {
    message.extension.iter().any(is_custom_option_extension)
        || message
            .nested_type
            .iter()
            .any(message_defines_custom_option)
}

fn is_custom_option_extension(extension: &prost_types::FieldDescriptorProto) -> bool {
    extension
        .extendee
        .as_deref()
        .map(|name| name.strip_prefix('.').unwrap_or(name))
        .is_some_and(|name| OPTION_MESSAGE_NAMES.contains(&name))
}

fn validate_proto3_and_source_info(
    descriptor_set: &FileDescriptorSet,
) -> Result<(), PayloadContractError> {
    for file in &descriptor_set.file {
        let file_name = file.name.clone().unwrap_or_default();
        if file.syntax.as_deref() != Some("proto3") {
            return Err(PayloadContractError::SyntaxUnsupported {
                file_name,
                syntax: file.syntax.clone(),
            });
        }
        if file
            .source_code_info
            .as_ref()
            .is_none_or(|source_info| source_info.location.is_empty())
        {
            return Err(PayloadContractError::SourceInfoMissing { file_name });
        }
    }
    Ok(())
}

fn validate_portable_field_names(
    descriptor_set: &FileDescriptorSet,
) -> Result<(), PayloadContractError> {
    for file in &descriptor_set.file {
        let file_name = file.name.as_deref().unwrap_or_default();
        let package = file.package.as_deref().unwrap_or_default();
        for message in &file.message_type {
            validate_message_field_names(file_name, package, message)?;
        }
    }
    Ok(())
}

fn validate_message_field_names(
    file_name: &str,
    parent_name: &str,
    message: &DescriptorProto,
) -> Result<(), PayloadContractError> {
    let Some(local_name) = message.name.as_deref().filter(|name| !name.is_empty()) else {
        return Ok(());
    };
    let message_name = if parent_name.is_empty() {
        local_name.to_owned()
    } else {
        format!("{parent_name}.{local_name}")
    };
    let mut normalized_names: HashMap<String, &str> = HashMap::with_capacity(message.field.len());
    for field in &message.field {
        let Some(field_name) = field.name.as_deref().filter(|name| !name.is_empty()) else {
            continue;
        };
        match normalized_names.entry(portable_field_name(field_name)) {
            Entry::Occupied(entry) => {
                return Err(PayloadContractError::FieldNameCollision {
                    file_name: file_name.to_owned(),
                    message_name,
                    first_field: (*entry.get()).to_owned(),
                    second_field: field_name.to_owned(),
                });
            }
            Entry::Vacant(entry) => {
                entry.insert(field_name);
            }
        }
    }
    for nested in &message.nested_type {
        validate_message_field_names(file_name, &message_name, nested)?;
    }
    Ok(())
}

fn portable_field_name(name: &str) -> String {
    name.chars()
        .filter_map(|character| match character {
            '_' => None,
            _ => Some(character.to_ascii_lowercase()),
        })
        .collect()
}

fn validate_program_roots(
    descriptor_set: &FileDescriptorSet,
    interface: PluginInterface,
) -> Result<(), PayloadContractError> {
    let source_roots = top_level_root_count(descriptor_set, SOURCE_ROOT_MESSAGE_NAME);
    let sink_roots = top_level_root_count(descriptor_set, SINK_ROOT_MESSAGE_NAME);

    match interface {
        PluginInterface::Source => {
            require_source_root(source_roots)?;
            if sink_roots != 0 {
                return Err(PayloadContractError::UndeclaredInterfaceRoot);
            }
        }
        PluginInterface::Sink => {
            require_sink_root(sink_roots)?;
            if source_roots != 0 {
                return Err(PayloadContractError::UndeclaredInterfaceRoot);
            }
        }
        PluginInterface::SourceAndSink => {
            require_source_root(source_roots)?;
            require_sink_root(sink_roots)?;
        }
    }
    Ok(())
}

fn require_source_root(count: usize) -> Result<(), PayloadContractError> {
    match count {
        0 => Err(PayloadContractError::SourceRootMissing),
        1 => Ok(()),
        _ => Err(PayloadContractError::SourceRootNotUnique),
    }
}

fn require_sink_root(count: usize) -> Result<(), PayloadContractError> {
    match count {
        0 => Err(PayloadContractError::SinkRootMissing),
        1 => Ok(()),
        _ => Err(PayloadContractError::SinkRootNotUnique),
    }
}

fn top_level_root_count(descriptor_set: &FileDescriptorSet, root_name: &str) -> usize {
    descriptor_set
        .file
        .iter()
        .flat_map(|file| &file.message_type)
        .filter(|message| message.name.as_deref() == Some(root_name))
        .count()
}

fn top_level_root_message(
    descriptor_pool: &DescriptorPool,
    root_name: &str,
) -> Option<MessageDescriptor> {
    descriptor_pool
        .all_messages()
        .find(|message| message.parent_message().is_none() && message.name() == root_name)
}

fn custom_option_data_file(descriptor_pool: &DescriptorPool) -> Option<String> {
    descriptor_pool
        .files()
        .find(|file| options_contain_custom_data(file.options()))
        .map(|file| file.name().to_owned())
        .or_else(|| {
            descriptor_pool.all_messages().find_map(|message| {
                let has_custom_data = options_contain_custom_data(message.options())
                    || message
                        .fields()
                        .any(|field| options_contain_custom_data(field.options()))
                    || message
                        .oneofs()
                        .any(|oneof| options_contain_custom_data(oneof.options()));
                has_custom_data.then(|| message.parent_file().name().to_owned())
            })
        })
        .or_else(|| {
            descriptor_pool.all_enums().find_map(|enumeration| {
                let has_custom_data = options_contain_custom_data(enumeration.options())
                    || enumeration
                        .values()
                        .any(|value| options_contain_custom_data(value.options()));
                has_custom_data.then(|| enumeration.parent_file().name().to_owned())
            })
        })
        .or_else(|| {
            descriptor_pool.services().find_map(|service| {
                let has_custom_data = options_contain_custom_data(service.options())
                    || service
                        .methods()
                        .any(|method| options_contain_custom_data(method.options()));
                has_custom_data.then(|| service.parent_file().name().to_owned())
            })
        })
        .or_else(|| {
            descriptor_pool.all_extensions().find_map(|extension| {
                options_contain_custom_data(extension.options())
                    .then(|| extension.parent_file().name().to_owned())
            })
        })
}

fn options_contain_custom_data(options: DynamicMessage) -> bool {
    options.extensions().next().is_some() || options.unknown_fields().next().is_some()
}

#[derive(Clone, PartialEq, Message)]
struct DescriptorSetEditionProbe {
    #[prost(message, repeated, tag = "1")]
    file: Vec<FileEditionProbe>,
}

#[derive(Clone, PartialEq, Message)]
struct FileEditionProbe {
    #[prost(string, optional, tag = "1")]
    name: Option<String>,
    #[prost(int32, optional, tag = "14")]
    edition: Option<i32>,
}

#[cfg(test)]
mod tests;
