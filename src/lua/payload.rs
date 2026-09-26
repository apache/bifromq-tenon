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

//! Frozen Payload Contract registry and Lua Builder userdata.
//!
//! Each Flow supplies one immutable registry containing only its target
//! `SinkContractId` values. Each root Builder owns a dynamic
//! Protobuf message. Nested Builders share that root through single-threaded
//! reference counting and address their message by a stable descriptor path.
//! `build()` deep-clones the current message into an immutable userdata value;
//! later Builder mutations cannot change an existing snapshot.
//! Builder method tables are compiled once per exact binding and message type.
//! The shared tree keeps a conservative incremental memory charge, while debug
//! builds independently recompute the complete charge after every successful mutation.
//!
//! This module does not encode Sink Payload bytes or `EgressRecord`, discover
//! Sink Programs, or read installation files. The adjacent `emit` module owns
//! deterministic Sink Payload encoding.

use super::{
    ExecutionBudget, LuaApiFailure, LuaApiResult, LuaVmFatalFault,
    create_catchable_api_wrapper_factory, finish_api_call, memory::LuaNativeMemoryBudget,
    protect_name,
};
use crate::identifiers::SinkContractId;
use mlua::{
    AnyUserData, Function, Lua, LuaString, MetaMethod, MultiValue, Table, UserData, UserDataFields,
    UserDataMethods, Value as LuaValue,
};
use prost_reflect::{
    DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor, Value as ProtobufValue,
};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::str;

const REGISTRY_ID_TYPE_ERROR: &str = "registry sinkContractId must be a string";
const REGISTRY_ID_INVALID_ERROR: &str = "registry sinkContractId must be normalized";
const REGISTRY_ID_UNDECLARED_ERROR: &str = "registry sinkContractId must be declared by the Flow";
const REGISTRY_RECEIVER_ERROR: &str = "registry method receiver is invalid";
const REGISTRY_ARGUMENT_ERROR: &str = "registry method arguments are invalid";
const BUILDER_METHOD_ERROR: &str = "Payload Builder method is unavailable";
const BUILDER_RECEIVER_ERROR: &str = "Payload Builder method receiver is invalid";
const BUILDER_ARGUMENT_ERROR: &str = "Payload Builder method arguments are invalid";
const BUILDER_VALUE_ERROR: &str = "Payload Builder value does not match the field type";
const BUILDER_MAP_KEY_ERROR: &str = "Payload Builder map key does not match the field type";
// The charge deliberately exceeds one BTree entry. `prost-reflect` keeps message fields in a
// private BTreeMap, so Tenon charges a stable upper bound instead of depending on its node layout.
const MESSAGE_FIELD_CHARGE_BYTES: usize =
    16 * (size_of::<u32>() + size_of::<ProtobufValue>() + (2 * size_of::<usize>()));
const LIST_STORAGE_BASE_BYTES: usize = 4 * size_of::<ProtobufValue>();
const LIST_ELEMENT_CHARGE_BYTES: usize = 2 * size_of::<ProtobufValue>();
const MAP_SLOT_BYTES: usize =
    size_of::<MapKey>() + size_of::<ProtobufValue>() + (2 * size_of::<usize>());
const MAP_STORAGE_BASE_BYTES: usize = 8 * MAP_SLOT_BYTES;
const MAP_ENTRY_CHARGE_BYTES: usize = 4 * MAP_SLOT_BYTES;

/// An immutable Flow-local subset of validated Payload Contracts.
#[derive(Clone, Debug, Default)]
pub(super) struct FrozenPayloadRegistry {
    entries: Rc<HashMap<SinkContractId, Rc<PayloadBinding>>>,
}

impl FrozenPayloadRegistry {
    pub(super) fn new(entries: HashMap<SinkContractId, MessageDescriptor>) -> Self {
        let mut registry = HashMap::with_capacity(entries.len());
        for (sink_contract_id, root_message) in entries {
            let binding = Rc::new(PayloadBinding {
                sink_contract_id: sink_contract_id.clone(),
                root_message,
            });
            registry.insert(sink_contract_id, binding);
        }
        Self {
            entries: Rc::new(registry),
        }
    }

    fn get(&self, sink_contract_id: &SinkContractId) -> Option<Rc<PayloadBinding>> {
        self.entries.get(sink_contract_id).map(Rc::clone)
    }
}

#[derive(Debug)]
struct PayloadBinding {
    sink_contract_id: SinkContractId,
    root_message: MessageDescriptor,
}

struct LuaPayloadRuntime {
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
    execution_budget: Rc<RefCell<Option<ExecutionBudget>>>,
    memory_budget: Rc<LuaNativeMemoryBudget>,
    wrapper_factory: Function,
}

struct LuaPayloadApiContext {
    runtime: Rc<LuaPayloadRuntime>,
    prototypes: BuilderPrototypeCatalog,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum BuilderRole {
    Root,
    Nested,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct BuilderPrototypeKey {
    sink_contract_id: SinkContractId,
    message_name: Box<str>,
    role: BuilderRole,
}

struct BuilderPrototypeCatalog {
    methods: HashMap<BuilderPrototypeKey, Table>,
    unavailable_method: Function,
}

impl BuilderPrototypeCatalog {
    fn compile(
        lua: &Lua,
        registry: &FrozenPayloadRegistry,
        runtime: Rc<LuaPayloadRuntime>,
    ) -> mlua::Result<Self> {
        let unavailable_method =
            create_failed_method(lua, BUILDER_METHOD_ERROR, Rc::clone(&runtime))?;
        let mut methods = HashMap::new();
        let mut bindings = registry.entries.values().collect::<Vec<_>>();
        bindings.sort_unstable_by(|left, right| {
            left.sink_contract_id
                .program_name()
                .as_str()
                .cmp(right.sink_contract_id.program_name().as_str())
                .then_with(|| {
                    left.sink_contract_id
                        .exact_version()
                        .as_str()
                        .cmp(right.sink_contract_id.exact_version().as_str())
                })
        });
        for binding in bindings {
            let root = binding.root_message.clone();
            compile_builder_prototype(
                lua,
                &mut methods,
                binding,
                root,
                BuilderRole::Root,
                Rc::clone(&runtime),
            )?;
        }
        Ok(Self {
            methods,
            unavailable_method,
        })
    }

    fn get(
        &self,
        sink_contract_id: &SinkContractId,
        descriptor: &MessageDescriptor,
        role: BuilderRole,
    ) -> LuaApiResult<Table> {
        self.methods
            .get(&builder_prototype_key(sink_contract_id, descriptor, role))
            .cloned()
            .ok_or(LuaApiFailure::InternalInvariantViolation)
    }
}

struct RegistryUserData {
    get_builder: Function,
}

impl UserData for RegistryUserData {
    fn add_fields<F: UserDataFields<Self>>(fields: &mut F) {
        fields.add_field_method_get("getBuilder", |_, registry| Ok(registry.get_builder.clone()));
    }
}

/// A mutable root or nested message view exposed to Lua as userdata.
struct PayloadBuilder {
    state: Rc<RefCell<TrackedPayloadTree>>,
    binding: Rc<PayloadBinding>,
    path: Box<[BuilderPathSegment]>,
    message_descriptor: MessageDescriptor,
    methods: Table,
    context: Rc<LuaPayloadApiContext>,
    _memory: PayloadMemoryReservation,
}

impl UserData for PayloadBuilder {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_meta_method(MetaMethod::Index, |_, builder, key: LuaValue| match key {
            LuaValue::String(name) => {
                builder
                    .methods
                    .raw_get::<LuaValue>(name)
                    .map(|value| match value {
                        LuaValue::Nil => LuaValue::Function(
                            builder.context.prototypes.unavailable_method.clone(),
                        ),
                        value => value,
                    })
            }
            _ => Ok(LuaValue::Function(
                builder.context.prototypes.unavailable_method.clone(),
            )),
        });
    }
}

#[derive(Debug)]
struct TrackedPayloadTree {
    binding: Rc<PayloadBinding>,
    root_message: DynamicMessage,
    memory: PayloadMemoryReservation,
}

#[derive(Clone, Debug, PartialEq)]
enum BuilderPathSegment {
    Singular(FieldDescriptor),
    Repeated {
        field: FieldDescriptor,
        index: usize,
    },
    Map {
        field: FieldDescriptor,
        key: MapKey,
    },
}

/// An immutable Payload snapshot that remains bound to one exact `SinkContractId`.
#[derive(Debug)]
pub(super) struct BuiltPayload {
    binding: Rc<PayloadBinding>,
    message: DynamicMessage,
    _memory: PayloadMemoryReservation,
}

impl BuiltPayload {
    pub(super) fn sink_contract_id(&self) -> &SinkContractId {
        &self.binding.sink_contract_id
    }

    pub(super) const fn message(&self) -> &DynamicMessage {
        &self.message
    }
}

impl UserData for BuiltPayload {}

#[derive(Clone, Debug)]
enum BuilderOperation {
    Build,
    Set(FieldDescriptor),
    Clear(FieldDescriptor),
    GetMessage(FieldDescriptor),
    AddScalar(FieldDescriptor),
    AddMessage(FieldDescriptor),
    PutScalar {
        field: FieldDescriptor,
        key_kind: Kind,
        value_kind: Kind,
    },
    PutMessage {
        field: FieldDescriptor,
        key_kind: Kind,
        value_descriptor: MessageDescriptor,
    },
}

struct PayloadMutation<T> {
    result: T,
    removed_memory: usize,
    added_memory: usize,
}

impl<T> PayloadMutation<T> {
    const fn new(result: T, removed_memory: usize, added_memory: usize) -> Self {
        Self {
            result,
            removed_memory,
            added_memory,
        }
    }

    const fn unchanged(result: T) -> Self {
        Self::new(result, 0, 0)
    }
}

pub(super) fn install(
    lua: &Lua,
    environment_values: &Table,
    protected_names: Rc<RefCell<HashSet<Vec<u8>>>>,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
    execution_budget: Rc<RefCell<Option<ExecutionBudget>>>,
    registry: FrozenPayloadRegistry,
    memory_budget: Rc<LuaNativeMemoryBudget>,
) -> mlua::Result<()> {
    let runtime = Rc::new(LuaPayloadRuntime {
        fatal_fault,
        execution_budget,
        memory_budget,
        wrapper_factory: create_catchable_api_wrapper_factory(lua)?,
    });
    let prototypes = BuilderPrototypeCatalog::compile(lua, &registry, Rc::clone(&runtime))?;
    let context = Rc::new(LuaPayloadApiContext {
        runtime,
        prototypes,
    });
    let native_context = Rc::clone(&context);
    let native_get_builder = lua.create_function(move |lua, arguments: MultiValue| {
        let result = get_builder(lua, &registry, Rc::clone(&native_context), arguments);
        finish_api_call(
            lua,
            result,
            &native_context.runtime.fatal_fault,
            &native_context.runtime.execution_budget,
        )
    })?;
    let get_builder = context
        .runtime
        .wrapper_factory
        .call::<Function>(native_get_builder)?;
    let registry = lua.create_userdata(RegistryUserData { get_builder })?;
    environment_values.raw_set("registry", registry)?;
    protect_name(&protected_names, "registry");
    Ok(())
}

fn get_builder(
    lua: &Lua,
    registry: &FrozenPayloadRegistry,
    context: Rc<LuaPayloadApiContext>,
    mut arguments: MultiValue,
) -> LuaApiResult<LuaValue> {
    take_registry_receiver(&mut arguments)?;
    let sink_contract_id = match arguments.pop_front() {
        Some(LuaValue::String(value)) => value,
        _ => return Err(LuaApiFailure::Api(REGISTRY_ID_TYPE_ERROR)),
    };
    require_no_arguments(&arguments, REGISTRY_ARGUMENT_ERROR)?;
    let sink_contract_id = str::from_utf8(sink_contract_id.as_bytes().as_ref())
        .ok()
        .and_then(|value| SinkContractId::try_from(value).ok())
        .ok_or(LuaApiFailure::Api(REGISTRY_ID_INVALID_ERROR))?;
    let binding = registry
        .get(&sink_contract_id)
        .ok_or(LuaApiFailure::Api(REGISTRY_ID_UNDECLARED_ERROR))?;
    let root_descriptor = binding.root_message.clone();
    let root_message = DynamicMessage::new(root_descriptor.clone());
    let memory = PayloadMemoryReservation::reserve(
        Rc::clone(&context.runtime.memory_budget),
        tracked_payload_tree_memory_usage(&root_message)?,
    )?;
    let state = Rc::new(RefCell::new(TrackedPayloadTree {
        binding,
        root_message,
        memory,
    }));
    create_builder(lua, state, Vec::new(), root_descriptor, context).map(LuaValue::UserData)
}

fn take_registry_receiver(arguments: &mut MultiValue) -> LuaApiResult<()> {
    match arguments.pop_front() {
        Some(LuaValue::UserData(receiver)) if receiver.is::<RegistryUserData>() => Ok(()),
        _ => Err(LuaApiFailure::Api(REGISTRY_RECEIVER_ERROR)),
    }
}

fn create_builder(
    lua: &Lua,
    state: Rc<RefCell<TrackedPayloadTree>>,
    path: Vec<BuilderPathSegment>,
    message_descriptor: MessageDescriptor,
    context: Rc<LuaPayloadApiContext>,
) -> LuaApiResult<AnyUserData> {
    let binding = {
        let state = state
            .try_borrow()
            .map_err(|_| LuaApiFailure::InternalInvariantViolation)?;
        Rc::clone(&state.binding)
    };
    let memory = PayloadMemoryReservation::reserve(
        Rc::clone(&context.runtime.memory_budget),
        builder_view_memory_usage(&path)?,
    )?;
    let methods = context.prototypes.get(
        &binding.sink_contract_id,
        &message_descriptor,
        if path.is_empty() {
            BuilderRole::Root
        } else {
            BuilderRole::Nested
        },
    )?;
    lua.create_userdata(PayloadBuilder {
        state,
        binding,
        path: path.into_boxed_slice(),
        message_descriptor,
        methods,
        context,
        _memory: memory,
    })
    .map_err(LuaApiFailure::Vm)
}

fn compile_builder_prototype(
    lua: &Lua,
    prototypes: &mut HashMap<BuilderPrototypeKey, Table>,
    binding: &Rc<PayloadBinding>,
    descriptor: MessageDescriptor,
    role: BuilderRole,
    runtime: Rc<LuaPayloadRuntime>,
) -> mlua::Result<()> {
    let key = builder_prototype_key(&binding.sink_contract_id, &descriptor, role);
    if prototypes.contains_key(&key) {
        return Ok(());
    }

    let methods = lua.create_table()?;
    if role == BuilderRole::Root {
        install_builder_method(
            lua,
            &methods,
            String::from("build"),
            BuilderOperation::Build,
            Rc::clone(binding),
            descriptor.clone(),
            Rc::clone(&runtime),
        )?;
    }
    for field in descriptor.fields() {
        for (method_name, operation) in field_operations(&field) {
            install_builder_method(
                lua,
                &methods,
                method_name,
                operation,
                Rc::clone(binding),
                descriptor.clone(),
                Rc::clone(&runtime),
            )?;
        }
    }
    prototypes.insert(key, methods);

    for field in descriptor.fields() {
        let child = if field.is_map() {
            match map_entry_kinds(&field) {
                Some((_, Kind::Message(child))) => Some(child),
                _ => None,
            }
        } else {
            match field.kind() {
                Kind::Message(child) => Some(child),
                _ => None,
            }
        };
        if let Some(child) = child {
            compile_builder_prototype(
                lua,
                prototypes,
                binding,
                child,
                BuilderRole::Nested,
                Rc::clone(&runtime),
            )?;
        }
    }
    Ok(())
}

fn builder_prototype_key(
    sink_contract_id: &SinkContractId,
    descriptor: &MessageDescriptor,
    role: BuilderRole,
) -> BuilderPrototypeKey {
    BuilderPrototypeKey {
        sink_contract_id: sink_contract_id.clone(),
        message_name: descriptor.full_name().into(),
        role,
    }
}

fn install_builder_method(
    lua: &Lua,
    methods: &Table,
    method_name: String,
    operation: BuilderOperation,
    expected_binding: Rc<PayloadBinding>,
    expected_descriptor: MessageDescriptor,
    runtime: Rc<LuaPayloadRuntime>,
) -> mlua::Result<()> {
    let native_runtime = Rc::clone(&runtime);
    let native = lua.create_function(move |lua, arguments: MultiValue| {
        let result = invoke_builder_method(
            lua,
            &expected_binding,
            &expected_descriptor,
            &operation,
            arguments,
        );
        finish_api_call(
            lua,
            result,
            &native_runtime.fatal_fault,
            &native_runtime.execution_budget,
        )
    })?;
    let wrapped = runtime.wrapper_factory.call::<Function>(native)?;
    methods.raw_set(method_name, wrapped)
}

fn create_failed_method(
    lua: &Lua,
    message: &'static str,
    runtime: Rc<LuaPayloadRuntime>,
) -> mlua::Result<Function> {
    let native_runtime = Rc::clone(&runtime);
    let native = lua.create_function(move |lua, _: MultiValue| {
        finish_api_call(
            lua,
            Err(LuaApiFailure::Api(message)),
            &native_runtime.fatal_fault,
            &native_runtime.execution_budget,
        )
    })?;
    runtime.wrapper_factory.call::<Function>(native)
}

fn invoke_builder_method(
    lua: &Lua,
    expected_binding: &Rc<PayloadBinding>,
    expected_descriptor: &MessageDescriptor,
    operation: &BuilderOperation,
    mut arguments: MultiValue,
) -> LuaApiResult<LuaValue> {
    let receiver = match arguments.pop_front() {
        Some(LuaValue::UserData(receiver)) if receiver.is::<PayloadBuilder>() => receiver,
        _ => return Err(LuaApiFailure::Api(BUILDER_RECEIVER_ERROR)),
    };
    let builder = receiver.borrow::<PayloadBuilder>()?;
    if !Rc::ptr_eq(&builder.binding, expected_binding)
        || &builder.message_descriptor != expected_descriptor
    {
        return Err(LuaApiFailure::Api(BUILDER_RECEIVER_ERROR));
    }

    match operation {
        BuilderOperation::Build => {
            require_no_arguments(&arguments, BUILDER_ARGUMENT_ERROR)?;
            build(lua, &builder)
        }
        BuilderOperation::Set(field) => {
            let value = arguments
                .pop_front()
                .ok_or(LuaApiFailure::Api(BUILDER_ARGUMENT_ERROR))?;
            require_no_arguments(&arguments, BUILDER_ARGUMENT_ERROR)?;
            let mutation_memory = reserve_mutation_memory(
                &builder,
                protobuf_value_copy_bytes(field.kind(), &value)?,
                MESSAGE_FIELD_CHARGE_BYTES,
            )?;
            let value = protobuf_value(field.kind(), value)?;
            with_message_mut(&builder, mutation_memory, |message| {
                let removed_memory = affected_field_memory_usage(message, field)?;
                if !field.supports_presence() && value == field.default_value() {
                    message.clear_field(field);
                } else {
                    message
                        .try_set_field(field, value)
                        .map_err(|_| LuaApiFailure::InternalInvariantViolation)?;
                }
                Ok(PayloadMutation::new(
                    (),
                    removed_memory,
                    affected_field_memory_usage(message, field)?,
                ))
            })?;
            Ok(LuaValue::Nil)
        }
        BuilderOperation::Clear(field) => {
            require_no_arguments(&arguments, BUILDER_ARGUMENT_ERROR)?;
            let mutation_memory = reserve_mutation_memory(&builder, 0, 0)?;
            with_message_mut(&builder, mutation_memory, |message| {
                let removed_memory = field_memory_usage(message, field)?;
                message.clear_field(field);
                Ok(PayloadMutation::new((), removed_memory, 0))
            })?;
            Ok(LuaValue::Nil)
        }
        BuilderOperation::GetMessage(field) => {
            require_no_arguments(&arguments, BUILDER_ARGUMENT_ERROR)?;
            let mutation_memory = reserve_mutation_memory(
                &builder,
                0,
                MESSAGE_FIELD_CHARGE_BYTES + size_of::<DynamicMessage>(),
            )?;
            let child_descriptor = message_kind(field.kind())?;
            with_message_mut(&builder, mutation_memory, |message| {
                if message.has_field(field) {
                    return Ok(PayloadMutation::unchanged(()));
                }
                let removed_memory = affected_field_memory_usage(message, field)?;
                if !matches!(message.get_field_mut(field), ProtobufValue::Message(_)) {
                    return Err(LuaApiFailure::InternalInvariantViolation);
                }
                Ok(PayloadMutation::new(
                    (),
                    removed_memory,
                    MESSAGE_FIELD_CHARGE_BYTES + size_of::<DynamicMessage>(),
                ))
            })?;
            let path_memory = reserve_nested_path_memory(&builder, 0)?;
            let path = nested_builder_path(&builder, BuilderPathSegment::Singular(field.clone()))?;
            let result = create_builder(
                lua,
                Rc::clone(&builder.state),
                path,
                child_descriptor,
                Rc::clone(&builder.context),
            );
            drop(path_memory);
            result.map(LuaValue::UserData)
        }
        BuilderOperation::AddScalar(field) => {
            let value = arguments
                .pop_front()
                .ok_or(LuaApiFailure::Api(BUILDER_ARGUMENT_ERROR))?;
            require_no_arguments(&arguments, BUILDER_ARGUMENT_ERROR)?;
            let mutation_memory = reserve_mutation_memory(
                &builder,
                protobuf_value_copy_bytes(field.kind(), &value)?,
                MESSAGE_FIELD_CHARGE_BYTES + LIST_STORAGE_BASE_BYTES + LIST_ELEMENT_CHARGE_BYTES,
            )?;
            let value = protobuf_value(field.kind(), value)?;
            with_message_mut(&builder, mutation_memory, |message| {
                let field_was_present = message.has_field(field);
                let added_memory = list_element_memory_charge(field_was_present, &value)?;
                match message.get_field_mut(field) {
                    ProtobufValue::List(values) => {
                        values.push(value);
                        Ok(PayloadMutation::new((), 0, added_memory))
                    }
                    _ => Err(LuaApiFailure::InternalInvariantViolation),
                }
            })?;
            Ok(LuaValue::Nil)
        }
        BuilderOperation::AddMessage(field) => {
            require_no_arguments(&arguments, BUILDER_ARGUMENT_ERROR)?;
            let mutation_memory = reserve_mutation_memory(
                &builder,
                0,
                MESSAGE_FIELD_CHARGE_BYTES
                    + LIST_STORAGE_BASE_BYTES
                    + LIST_ELEMENT_CHARGE_BYTES
                    + size_of::<DynamicMessage>(),
            )?;
            let child_descriptor = message_kind(field.kind())?;
            let index = with_message_mut(&builder, mutation_memory, |message| {
                let field_was_present = message.has_field(field);
                let child = ProtobufValue::Message(DynamicMessage::new(child_descriptor.clone()));
                let added_memory = list_element_memory_charge(field_was_present, &child)?;
                match message.get_field_mut(field) {
                    ProtobufValue::List(values) => {
                        let index = values.len();
                        values.push(child);
                        Ok(PayloadMutation::new(index, 0, added_memory))
                    }
                    _ => Err(LuaApiFailure::InternalInvariantViolation),
                }
            })?;
            let path_memory = reserve_nested_path_memory(&builder, 0)?;
            let path = nested_builder_path(
                &builder,
                BuilderPathSegment::Repeated {
                    field: field.clone(),
                    index,
                },
            )?;
            let result = create_builder(
                lua,
                Rc::clone(&builder.state),
                path,
                child_descriptor,
                Rc::clone(&builder.context),
            );
            drop(path_memory);
            result.map(LuaValue::UserData)
        }
        BuilderOperation::PutScalar {
            field,
            key_kind,
            value_kind,
        } => {
            let key = arguments
                .pop_front()
                .ok_or(LuaApiFailure::Api(BUILDER_ARGUMENT_ERROR))?;
            let value = arguments
                .pop_front()
                .ok_or(LuaApiFailure::Api(BUILDER_ARGUMENT_ERROR))?;
            require_no_arguments(&arguments, BUILDER_ARGUMENT_ERROR)?;
            let copied_bytes = protobuf_value_copy_bytes(key_kind.clone(), &key)?
                .checked_add(protobuf_value_copy_bytes(value_kind.clone(), &value)?)
                .ok_or(LuaApiFailure::MemoryExceeded)?;
            let mutation_memory = reserve_mutation_memory(
                &builder,
                copied_bytes,
                MESSAGE_FIELD_CHARGE_BYTES + MAP_STORAGE_BASE_BYTES + MAP_ENTRY_CHARGE_BYTES,
            )?;
            let key = protobuf_map_key(key_kind.clone(), key)?;
            let value = protobuf_value(value_kind.clone(), value)?;
            with_message_mut(&builder, mutation_memory, |message| {
                let field_was_present = message.has_field(field);
                match message.get_field_mut(field) {
                    ProtobufValue::Map(values) => {
                        put_map_value(values, field_was_present, key, value)
                    }
                    _ => Err(LuaApiFailure::InternalInvariantViolation),
                }
            })?;
            Ok(LuaValue::Nil)
        }
        BuilderOperation::PutMessage {
            field,
            key_kind,
            value_descriptor,
        } => {
            let key = arguments
                .pop_front()
                .ok_or(LuaApiFailure::Api(BUILDER_ARGUMENT_ERROR))?;
            require_no_arguments(&arguments, BUILDER_ARGUMENT_ERROR)?;
            let copied_key_bytes = protobuf_value_copy_bytes(key_kind.clone(), &key)?;
            let path_memory = reserve_nested_path_memory(&builder, copied_key_bytes)?;
            let key = protobuf_map_key(key_kind.clone(), key)?;
            let path = nested_builder_path(
                &builder,
                BuilderPathSegment::Map {
                    field: field.clone(),
                    key,
                },
            )?;
            let path_key = match path.last() {
                Some(BuilderPathSegment::Map { key, .. }) => key,
                _ => return Err(LuaApiFailure::InternalInvariantViolation),
            };
            let mutation_memory = reserve_mutation_memory(
                &builder,
                copied_key_bytes,
                MESSAGE_FIELD_CHARGE_BYTES
                    + MAP_STORAGE_BASE_BYTES
                    + MAP_ENTRY_CHARGE_BYTES
                    + size_of::<DynamicMessage>(),
            )?;
            with_message_mut(&builder, mutation_memory, |message| {
                let field_was_present = message.has_field(field);
                match message.get_field_mut(field) {
                    ProtobufValue::Map(values) => {
                        let key = path_key.clone();
                        let value =
                            ProtobufValue::Message(DynamicMessage::new(value_descriptor.clone()));
                        put_map_value(values, field_was_present, key, value)
                    }
                    _ => Err(LuaApiFailure::InternalInvariantViolation),
                }
            })?;
            let result = create_builder(
                lua,
                Rc::clone(&builder.state),
                path,
                value_descriptor.clone(),
                Rc::clone(&builder.context),
            );
            drop(path_memory);
            result.map(LuaValue::UserData)
        }
    }
}

fn build(lua: &Lua, builder: &PayloadBuilder) -> LuaApiResult<LuaValue> {
    if !builder.path.is_empty() {
        return Err(LuaApiFailure::Api(BUILDER_METHOD_ERROR));
    }
    let state = builder
        .state
        .try_borrow()
        .map_err(|_| LuaApiFailure::InternalInvariantViolation)?;
    let snapshot_memory = built_payload_memory_usage_from_tree(state.memory.bytes)?;
    #[cfg(debug_assertions)]
    if snapshot_memory != built_payload_memory_usage(&state.root_message)? {
        return Err(LuaApiFailure::InternalInvariantViolation);
    }
    let memory =
        PayloadMemoryReservation::reserve(Rc::clone(&state.memory.budget), snapshot_memory)?;
    lua.create_userdata(BuiltPayload {
        binding: Rc::clone(&state.binding),
        message: state.root_message.clone(),
        _memory: memory,
    })
    .map(LuaValue::UserData)
    .map_err(LuaApiFailure::Vm)
}

fn with_message_mut<T>(
    builder: &PayloadBuilder,
    mut mutation_memory: PayloadMemoryReservation,
    operation: impl FnOnce(&mut DynamicMessage) -> LuaApiResult<PayloadMutation<T>>,
) -> LuaApiResult<T> {
    let mut state = builder
        .state
        .try_borrow_mut()
        .map_err(|_| LuaApiFailure::InternalInvariantViolation)?;
    let message = resolve_message_mut(&mut state.root_message, &builder.path)?;
    let mutation = operation(message)?;
    let next_tree_memory = state
        .memory
        .bytes
        .checked_sub(mutation.removed_memory)
        .and_then(|bytes| bytes.checked_add(mutation.added_memory))
        .ok_or(LuaApiFailure::InternalInvariantViolation)?;
    let preflight_memory = state
        .memory
        .bytes
        .checked_add(mutation_memory.bytes)
        .ok_or(LuaApiFailure::InternalInvariantViolation)?;
    if next_tree_memory > preflight_memory {
        return Err(LuaApiFailure::InternalInvariantViolation);
    }
    mutation_memory.transfer_to(&mut state.memory, next_tree_memory)?;
    #[cfg(debug_assertions)]
    validate_incremental_memory(&state)?;
    Ok(mutation.result)
}

#[cfg(debug_assertions)]
fn validate_incremental_memory(state: &TrackedPayloadTree) -> LuaApiResult<()> {
    if state.memory.bytes != tracked_payload_tree_memory_usage(&state.root_message)? {
        return Err(LuaApiFailure::InternalInvariantViolation);
    }
    Ok(())
}

fn reserve_mutation_memory(
    builder: &PayloadBuilder,
    copied_input_bytes: usize,
    structural_headroom_bytes: usize,
) -> LuaApiResult<PayloadMemoryReservation> {
    let state = builder
        .state
        .try_borrow()
        .map_err(|_| LuaApiFailure::InternalInvariantViolation)?;
    let reservation_bytes = copied_input_bytes
        .checked_add(structural_headroom_bytes)
        .ok_or(LuaApiFailure::MemoryExceeded)?;
    PayloadMemoryReservation::reserve(Rc::clone(&state.memory.budget), reservation_bytes)
}

fn reserve_nested_path_memory(
    builder: &PayloadBuilder,
    new_string_bytes: usize,
) -> LuaApiResult<PayloadMemoryReservation> {
    let path_bytes = builder_path_storage_memory_usage(&builder.path, 1)?
        .checked_add(new_string_bytes)
        .ok_or(LuaApiFailure::MemoryExceeded)?;
    PayloadMemoryReservation::reserve(Rc::clone(&builder._memory.budget), path_bytes)
}

fn nested_builder_path(
    builder: &PayloadBuilder,
    segment: BuilderPathSegment,
) -> LuaApiResult<Vec<BuilderPathSegment>> {
    let capacity = builder
        .path
        .len()
        .checked_add(1)
        .ok_or(LuaApiFailure::MemoryExceeded)?;
    let mut path = Vec::with_capacity(capacity);
    path.extend(builder.path.iter().cloned());
    path.push(segment);
    Ok(path)
}

fn protobuf_value_copy_bytes(kind: Kind, value: &LuaValue) -> LuaApiResult<usize> {
    match (kind, value) {
        (Kind::String | Kind::Bytes, LuaValue::String(value)) => Ok(value.as_bytes().len()),
        _ => Ok(0),
    }
}

fn require_no_arguments(arguments: &MultiValue, message: &'static str) -> LuaApiResult<()> {
    if arguments.is_empty() {
        Ok(())
    } else {
        Err(LuaApiFailure::Api(message))
    }
}

#[derive(Debug)]
struct PayloadMemoryReservation {
    budget: Rc<LuaNativeMemoryBudget>,
    bytes: usize,
}

impl PayloadMemoryReservation {
    fn reserve(budget: Rc<LuaNativeMemoryBudget>, bytes: usize) -> LuaApiResult<Self> {
        budget.replace(0, bytes)?;
        Ok(Self { budget, bytes })
    }

    fn transfer_to(&mut self, target: &mut Self, target_bytes: usize) -> LuaApiResult<()> {
        if !Rc::ptr_eq(&self.budget, &target.budget) {
            return Err(LuaApiFailure::InternalInvariantViolation);
        }
        let previous_bytes = self
            .bytes
            .checked_add(target.bytes)
            .ok_or(LuaApiFailure::InternalInvariantViolation)?;
        self.budget.replace(previous_bytes, target_bytes)?;
        self.bytes = 0;
        target.bytes = target_bytes;
        Ok(())
    }
}

impl Drop for PayloadMemoryReservation {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

fn message_memory_usage(message: &DynamicMessage) -> LuaApiResult<usize> {
    message
        .fields()
        .try_fold(size_of::<DynamicMessage>(), |total, (_, value)| {
            checked_add(
                checked_add(total, MESSAGE_FIELD_CHARGE_BYTES)?,
                protobuf_value_memory_usage(value)?,
            )
        })
        .ok_or(LuaApiFailure::MemoryExceeded)
}

fn tracked_payload_tree_memory_usage(message: &DynamicMessage) -> LuaApiResult<usize> {
    message_memory_usage(message)?
        .checked_add(size_of::<TrackedPayloadTree>().saturating_sub(size_of::<DynamicMessage>()))
        .ok_or(LuaApiFailure::MemoryExceeded)
}

#[cfg(debug_assertions)]
fn built_payload_memory_usage(message: &DynamicMessage) -> LuaApiResult<usize> {
    message_memory_usage(message)?
        .checked_add(size_of::<BuiltPayload>().saturating_sub(size_of::<DynamicMessage>()))
        .ok_or(LuaApiFailure::MemoryExceeded)
}

fn built_payload_memory_usage_from_tree(tree_memory: usize) -> LuaApiResult<usize> {
    tree_memory
        .checked_sub(size_of::<TrackedPayloadTree>())
        .and_then(|bytes| bytes.checked_add(size_of::<BuiltPayload>()))
        .ok_or(LuaApiFailure::InternalInvariantViolation)
}

fn affected_field_memory_usage(
    message: &DynamicMessage,
    field: &FieldDescriptor,
) -> LuaApiResult<usize> {
    let Some(oneof) = field.containing_oneof() else {
        return field_memory_usage(message, field);
    };
    oneof.fields().try_fold(0_usize, |total, member| {
        total
            .checked_add(field_memory_usage(message, &member)?)
            .ok_or(LuaApiFailure::MemoryExceeded)
    })
}

fn field_memory_usage(message: &DynamicMessage, field: &FieldDescriptor) -> LuaApiResult<usize> {
    if !message.has_field(field) {
        return Ok(0);
    }
    MESSAGE_FIELD_CHARGE_BYTES
        .checked_add(
            protobuf_value_memory_usage(message.get_field(field).as_ref())
                .ok_or(LuaApiFailure::MemoryExceeded)?,
        )
        .ok_or(LuaApiFailure::MemoryExceeded)
}

fn list_element_memory_charge(
    field_was_present: bool,
    value: &ProtobufValue,
) -> LuaApiResult<usize> {
    let mut charge = LIST_ELEMENT_CHARGE_BYTES
        .checked_add(protobuf_value_memory_usage(value).ok_or(LuaApiFailure::MemoryExceeded)?)
        .ok_or(LuaApiFailure::MemoryExceeded)?;
    if !field_was_present {
        charge = charge
            .checked_add(MESSAGE_FIELD_CHARGE_BYTES)
            .and_then(|bytes| bytes.checked_add(LIST_STORAGE_BASE_BYTES))
            .ok_or(LuaApiFailure::MemoryExceeded)?;
    }
    Ok(charge)
}

fn map_entry_memory_charge(key: &MapKey, value: &ProtobufValue) -> LuaApiResult<usize> {
    MAP_ENTRY_CHARGE_BYTES
        .checked_add(protobuf_map_key_memory_usage(key))
        .and_then(|bytes| bytes.checked_add(protobuf_value_memory_usage(value)?))
        .ok_or(LuaApiFailure::MemoryExceeded)
}

fn put_map_value(
    values: &mut HashMap<MapKey, ProtobufValue>,
    field_was_present: bool,
    key: MapKey,
    value: ProtobufValue,
) -> LuaApiResult<PayloadMutation<()>> {
    let new_value_memory =
        protobuf_value_memory_usage(&value).ok_or(LuaApiFailure::MemoryExceeded)?;
    let new_entry_memory = map_entry_memory_charge(&key, &value)?;
    let replaced = values.insert(key, value);
    let (removed_memory, added_memory) = match replaced {
        Some(value) => (
            protobuf_value_memory_usage(&value).ok_or(LuaApiFailure::MemoryExceeded)?,
            new_value_memory,
        ),
        None => (
            0,
            new_map_entry_memory_charge(field_was_present, new_entry_memory)?,
        ),
    };
    Ok(PayloadMutation::new((), removed_memory, added_memory))
}

fn new_map_entry_memory_charge(
    field_was_present: bool,
    entry_memory: usize,
) -> LuaApiResult<usize> {
    if field_was_present {
        return Ok(entry_memory);
    }
    entry_memory
        .checked_add(MESSAGE_FIELD_CHARGE_BYTES)
        .and_then(|bytes| bytes.checked_add(MAP_STORAGE_BASE_BYTES))
        .ok_or(LuaApiFailure::MemoryExceeded)
}

fn builder_view_memory_usage(path: &[BuilderPathSegment]) -> LuaApiResult<usize> {
    size_of::<PayloadBuilder>()
        .checked_add(builder_path_storage_memory_usage(path, 0)?)
        .ok_or(LuaApiFailure::MemoryExceeded)
}

fn builder_path_storage_memory_usage(
    path: &[BuilderPathSegment],
    additional_segments: usize,
) -> LuaApiResult<usize> {
    let shallow_path_bytes = path
        .len()
        .checked_add(additional_segments)
        .ok_or(LuaApiFailure::MemoryExceeded)?
        .checked_mul(size_of::<BuilderPathSegment>())
        .ok_or(LuaApiFailure::MemoryExceeded)?;
    let path_bytes = path.iter().try_fold(shallow_path_bytes, |total, segment| {
        let string_bytes = match segment {
            BuilderPathSegment::Map {
                key: MapKey::String(value),
                ..
            } => value.capacity(),
            BuilderPathSegment::Singular(_)
            | BuilderPathSegment::Repeated { .. }
            | BuilderPathSegment::Map { .. } => 0,
        };
        total.checked_add(string_bytes)
    });
    path_bytes.ok_or(LuaApiFailure::MemoryExceeded)
}

fn protobuf_value_memory_usage(value: &ProtobufValue) -> Option<usize> {
    match value {
        ProtobufValue::Bool(_)
        | ProtobufValue::I32(_)
        | ProtobufValue::I64(_)
        | ProtobufValue::U32(_)
        | ProtobufValue::U64(_)
        | ProtobufValue::F32(_)
        | ProtobufValue::F64(_)
        | ProtobufValue::EnumNumber(_) => Some(0),
        ProtobufValue::String(value) => Some(value.capacity()),
        ProtobufValue::Bytes(value) => Some(value.len()),
        ProtobufValue::Message(value) => message_memory_usage(value).ok(),
        ProtobufValue::List(values) => values.iter().try_fold(
            LIST_STORAGE_BASE_BYTES
                .checked_add(values.len().checked_mul(LIST_ELEMENT_CHARGE_BYTES)?)?,
            |total, value| checked_add(total, protobuf_value_memory_usage(value)?),
        ),
        ProtobufValue::Map(values) => values.iter().try_fold(
            MAP_STORAGE_BASE_BYTES
                .checked_add(values.len().checked_mul(MAP_ENTRY_CHARGE_BYTES)?)?,
            |total, (key, value)| {
                checked_add(
                    checked_add(total, protobuf_map_key_memory_usage(key))?,
                    protobuf_value_memory_usage(value)?,
                )
            },
        ),
    }
}

fn protobuf_map_key_memory_usage(key: &MapKey) -> usize {
    match key {
        MapKey::String(value) => value.capacity(),
        MapKey::Bool(_) | MapKey::I32(_) | MapKey::I64(_) | MapKey::U32(_) | MapKey::U64(_) => 0,
    }
}

fn checked_add(left: usize, right: usize) -> Option<usize> {
    left.checked_add(right)
}

fn resolve_message_mut<'a>(
    message: &'a mut DynamicMessage,
    path: &[BuilderPathSegment],
) -> LuaApiResult<&'a mut DynamicMessage> {
    let Some((segment, remaining)) = path.split_first() else {
        return Ok(message);
    };
    if !message.has_field(match segment {
        BuilderPathSegment::Singular(field)
        | BuilderPathSegment::Repeated { field, .. }
        | BuilderPathSegment::Map { field, .. } => field,
    }) {
        return Err(LuaApiFailure::Api(BUILDER_RECEIVER_ERROR));
    }
    let child = match segment {
        BuilderPathSegment::Singular(field) => match message.get_field_mut(field) {
            ProtobufValue::Message(child) => child,
            _ => return Err(LuaApiFailure::Api(BUILDER_RECEIVER_ERROR)),
        },
        BuilderPathSegment::Repeated { field, index } => match message.get_field_mut(field) {
            ProtobufValue::List(values) => match values.get_mut(*index) {
                Some(ProtobufValue::Message(child)) => child,
                _ => return Err(LuaApiFailure::Api(BUILDER_RECEIVER_ERROR)),
            },
            _ => return Err(LuaApiFailure::Api(BUILDER_RECEIVER_ERROR)),
        },
        BuilderPathSegment::Map { field, key } => match message.get_field_mut(field) {
            ProtobufValue::Map(values) => match values.get_mut(key) {
                Some(ProtobufValue::Message(child)) => child,
                _ => return Err(LuaApiFailure::Api(BUILDER_RECEIVER_ERROR)),
            },
            _ => return Err(LuaApiFailure::Api(BUILDER_RECEIVER_ERROR)),
        },
    };
    resolve_message_mut(child, remaining)
}

fn protobuf_value(kind: Kind, value: LuaValue) -> LuaApiResult<ProtobufValue> {
    match (kind, value) {
        (Kind::Bool, LuaValue::Boolean(value)) => Ok(ProtobufValue::Bool(value)),
        (Kind::Int32 | Kind::Sint32 | Kind::Sfixed32, LuaValue::Integer(value)) => {
            i32::try_from(value)
                .map(ProtobufValue::I32)
                .map_err(|_| LuaApiFailure::Api(BUILDER_VALUE_ERROR))
        }
        (Kind::Int64 | Kind::Sint64 | Kind::Sfixed64, LuaValue::Integer(value)) => {
            Ok(ProtobufValue::I64(value))
        }
        (Kind::Uint32 | Kind::Fixed32, LuaValue::Integer(value)) => u32::try_from(value)
            .map(ProtobufValue::U32)
            .map_err(|_| LuaApiFailure::Api(BUILDER_VALUE_ERROR)),
        (Kind::Uint64 | Kind::Fixed64, LuaValue::String(value)) => {
            parse_u64(value).map(ProtobufValue::U64)
        }
        (Kind::Float, LuaValue::Number(value)) if value.is_finite() => {
            let narrowed = value as f32;
            if narrowed.is_finite() {
                Ok(ProtobufValue::F32(narrowed))
            } else {
                Err(LuaApiFailure::Api(BUILDER_VALUE_ERROR))
            }
        }
        (Kind::Float, LuaValue::Integer(value)) => {
            let narrowed = value as f32;
            if narrowed.is_finite() {
                Ok(ProtobufValue::F32(narrowed))
            } else {
                Err(LuaApiFailure::Api(BUILDER_VALUE_ERROR))
            }
        }
        (Kind::Double, LuaValue::Number(value)) if value.is_finite() => {
            Ok(ProtobufValue::F64(value))
        }
        (Kind::Double, LuaValue::Integer(value)) => Ok(ProtobufValue::F64(value as f64)),
        (Kind::String, LuaValue::String(value)) => {
            let bytes = value.as_bytes();
            let value = str::from_utf8(bytes.as_ref())
                .map_err(|_| LuaApiFailure::Api(BUILDER_VALUE_ERROR))?;
            Ok(ProtobufValue::String(value.to_owned()))
        }
        (Kind::Bytes, LuaValue::String(value)) => Ok(ProtobufValue::Bytes(
            prost::bytes::Bytes::copy_from_slice(value.as_bytes().as_ref()),
        )),
        (Kind::Enum(enumeration), LuaValue::String(value)) => {
            let bytes = value.as_bytes();
            let symbol = str::from_utf8(bytes.as_ref())
                .map_err(|_| LuaApiFailure::Api(BUILDER_VALUE_ERROR))?;
            enumeration
                .get_value_by_name(symbol)
                .map(|value| ProtobufValue::EnumNumber(value.number()))
                .ok_or(LuaApiFailure::Api(BUILDER_VALUE_ERROR))
        }
        _ => Err(LuaApiFailure::Api(BUILDER_VALUE_ERROR)),
    }
}

fn protobuf_map_key(kind: Kind, value: LuaValue) -> LuaApiResult<MapKey> {
    match protobuf_value(kind, value) {
        Ok(ProtobufValue::Bool(value)) => Ok(MapKey::Bool(value)),
        Ok(ProtobufValue::I32(value)) => Ok(MapKey::I32(value)),
        Ok(ProtobufValue::I64(value)) => Ok(MapKey::I64(value)),
        Ok(ProtobufValue::U32(value)) => Ok(MapKey::U32(value)),
        Ok(ProtobufValue::U64(value)) => Ok(MapKey::U64(value)),
        Ok(ProtobufValue::String(value)) => Ok(MapKey::String(value)),
        _ => Err(LuaApiFailure::Api(BUILDER_MAP_KEY_ERROR)),
    }
}

fn parse_u64(value: LuaString) -> LuaApiResult<u64> {
    let bytes = value.as_bytes();
    let bytes = bytes.as_ref();
    if bytes.is_empty()
        || bytes.iter().any(|byte| !byte.is_ascii_digit())
        || (bytes.len() > 1 && bytes.first() == Some(&b'0'))
    {
        return Err(LuaApiFailure::Api(BUILDER_VALUE_ERROR));
    }
    str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or(LuaApiFailure::Api(BUILDER_VALUE_ERROR))
}

fn field_operations(field: &FieldDescriptor) -> Vec<(String, BuilderOperation)> {
    let suffix = builder_method_suffix(field.name());
    if field.is_map() {
        let Some((key_kind, value_kind)) = map_entry_kinds(field) else {
            return Vec::new();
        };
        let operation = match value_kind {
            Kind::Message(value_descriptor) => BuilderOperation::PutMessage {
                field: field.clone(),
                key_kind,
                value_descriptor,
            },
            value_kind => BuilderOperation::PutScalar {
                field: field.clone(),
                key_kind,
                value_kind,
            },
        };
        return vec![(format!("put{suffix}"), operation)];
    }
    if field.is_list() {
        return match field.kind() {
            Kind::Message(_) => vec![(
                format!("add{suffix}Builder"),
                BuilderOperation::AddMessage(field.clone()),
            )],
            _ => vec![(
                format!("add{suffix}"),
                BuilderOperation::AddScalar(field.clone()),
            )],
        };
    }
    match field.kind() {
        Kind::Message(_) => vec![(
            format!("get{suffix}Builder"),
            BuilderOperation::GetMessage(field.clone()),
        )],
        _ => vec![
            (format!("set{suffix}"), BuilderOperation::Set(field.clone())),
            (
                format!("clear{suffix}"),
                BuilderOperation::Clear(field.clone()),
            ),
        ],
    }
}

fn map_entry_kinds(field: &FieldDescriptor) -> Option<(Kind, Kind)> {
    let Kind::Message(entry) = field.kind() else {
        return None;
    };
    let key = entry.get_field_by_name("key")?;
    let value = entry.get_field_by_name("value")?;
    Some((key.kind(), value.kind()))
}

fn message_kind(kind: Kind) -> LuaApiResult<MessageDescriptor> {
    match kind {
        Kind::Message(message) => Ok(message),
        _ => Err(LuaApiFailure::InternalInvariantViolation),
    }
}

fn builder_method_suffix(field_name: &str) -> String {
    let mut suffix = String::with_capacity(field_name.len());
    let mut capitalize = true;
    for character in field_name.chars() {
        if character == '_' {
            capitalize = true;
        } else if capitalize {
            suffix.extend(character.to_uppercase());
            capitalize = false;
        } else {
            suffix.push(character);
        }
    }
    suffix
}
