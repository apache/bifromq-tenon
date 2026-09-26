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

//! Private embedded Lua VM boundary.
//!
//! This module owns real-source compilation, the interpreter, the initial
//! sandbox, load-time execution, resource enforcement, and the frozen `main`
//! binding. Static Tenon Document verification uses only the compiler boundary;
//! runtime integrity checks and the future Pipeline runtime use the full VM.
//!
//! `LuaVm` owns one interpreter and is intentionally thread-affine because the
//! `mlua` `send` feature is disabled. Construction and execution are
//! synchronous blocking work that must stay on the Pipeline executor assigned
//! to that VM. Dropping the owner releases the interpreter and every Lua value.
//! A CPU, memory, or sandbox fault permanently poisons the instance; this
//! module never resets or reuses it. The Pipeline caller owns recovery by
//! dropping the failed VM and constructing a fresh one from the same source.
//! The caller supplies only an optional callback for sandboxed `print`; this
//! module owns all `mlua` installation, fixed-size argument rendering, and
//! callback invocation without knowing the destination state.

use self::{memory::LuaNativeMemoryBudget, metrics::VmMetrics};
use crate::config::ScriptVmLimits;
use crate::contracts::core::DIAGNOSTIC_TEXT_MAXIMUM_BYTES;
use crate::identifiers::SinkContractId;
use ::bytes::Bytes;
use cpu_time::ThreadTime;
use mlua::chunk::ChunkMode;
use mlua::{
    Error as MluaError, Function, HookTriggers, Lua, LuaOptions, LuaString, MultiValue, StdLib,
    Table, Value, VmState,
};
use prost_reflect::MessageDescriptor;
use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::rc::Rc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod bytes;
mod emit;
mod event;
mod json;
mod memory;
pub(crate) mod metrics;
mod payload;
mod timer;

pub(crate) use emit::EmitBoundary;
pub(crate) use timer::{TimerEvent, TimerSchedule, TimerSlotInactive};

const HOOK_INSTRUCTION_INTERVAL: u32 = 1_000;
const INTERNAL_CPU_LIMIT_ERROR: &str = "Tenon Lua CPU time limit exceeded";
const INTERNAL_EXECUTION_STOPPED_ERROR: &str = "Tenon Lua execution stopped";
const INTERNAL_MEMORY_LIMIT_ERROR: &str = "Tenon Lua memory limit exceeded";
const INTERNAL_RESOURCE_LIMIT_ERROR: &str = "Tenon Lua resource safety limit exceeded";
const INTERNAL_INVARIANT_ERROR: &str = "Tenon Lua internal invariant was violated";
const INTERNAL_SANDBOX_ERROR: &str = "Tenon Lua sandbox violation";
const INTERNAL_MAIN_FAILED_ERROR: &str = "Tenon Lua main execution failed";
const CURRENT_TIME_ERROR: &str = "currentTimeMillis system time is out of range";
const SOURCE_ENVIRONMENT_PREFIX: &str = "local _ENV <const> = _ENV\n";
const PROXY_BACKING_METAFIELD: &str = "__tenon_proxy_backing";

const BASE_GLOBALS: &[&str] = &[
    "_VERSION", "assert", "error", "ipairs", "select", "tonumber", "tostring", "type",
];

const STRING_FUNCTIONS: &[&str] = &[
    "byte", "char", "find", "format", "gmatch", "gsub", "len", "lower", "match", "pack",
    "packsize", "rep", "reverse", "sub", "unpack", "upper",
];

const TABLE_FUNCTIONS: &[&str] = &[
    "concat", "insert", "move", "pack", "remove", "sort", "unpack",
];

const MATH_FIELDS: &[&str] = &[
    "abs",
    "acos",
    "asin",
    "atan",
    "ceil",
    "cos",
    "deg",
    "exp",
    "floor",
    "fmod",
    "frexp",
    "huge",
    "ldexp",
    "log",
    "max",
    "min",
    "modf",
    "pi",
    "rad",
    "random",
    "randomseed",
    "sin",
    "sqrt",
    "tan",
    "tointeger",
    "type",
    "ult",
    "maxinteger",
    "mininteger",
];

const UTF8_FIELDS: &[&str] = &["char", "charpattern", "codepoint", "codes", "len", "offset"];
const OS_FUNCTIONS: &[&str] = &["date", "difftime", "time"];

/// One Lua `print` invocation passed lazily to the caller-selected callback.
pub(crate) struct LuaPrintRecord {
    arguments: MultiValue,
    native_tostring: Function,
}

impl LuaPrintRecord {
    pub(crate) fn render(self) -> Result<(Box<str>, bool, bool), LuaPrintCallbackError> {
        render_print(self.arguments, &self.native_tostring).map_err(LuaPrintCallbackError)
    }
}

/// One selected `print` callback could not render or accept a record.
#[derive(Debug)]
pub(crate) struct LuaPrintCallbackError(MluaError);

impl LuaPrintCallbackError {
    fn into_mlua(self) -> MluaError {
        self.0
    }
}

/// `None` disables `print` statically; a callback may skip rendering the lazy
/// record from current runtime state.
pub(crate) type LuaPrintCallback =
    Option<Box<dyn Fn(LuaPrintRecord) -> Result<(), LuaPrintCallbackError>>>;

/// Stable category for a Lua VM construction or execution failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LuaVmErrorKind {
    InitializationFailed,
    SyntaxInvalid,
    TopLevelFailed,
    CpuTimeExceeded,
    ExecutionStopped,
    MemoryExceeded,
    ResourceLimitExceeded,
    InternalInvariantViolation,
    SandboxViolation,
    MainMissing,
    MainNotFunction,
    MainFailed,
}

impl LuaVmErrorKind {
    pub(crate) const fn tenon_document_issue_code(self) -> &'static str {
        match self {
            Self::InitializationFailed => "process.lua_initialization_failed",
            Self::SyntaxInvalid => "process.lua_syntax_invalid",
            Self::TopLevelFailed => "process.lua_top_level_failed",
            Self::CpuTimeExceeded => "process.lua_cpu_time_exceeded",
            Self::ExecutionStopped => "process.lua_execution_stopped",
            Self::MemoryExceeded => "process.lua_memory_exceeded",
            Self::ResourceLimitExceeded => "process.lua_resource_limit_exceeded",
            Self::InternalInvariantViolation => "process.lua_internal_invariant_failed",
            Self::SandboxViolation => "process.lua_sandbox_violation",
            Self::MainMissing => "process.lua_main_missing",
            Self::MainNotFunction => "process.lua_main_not_function",
            Self::MainFailed => "process.lua_main_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LuaVmFatalFault {
    CpuTimeExceeded,
    ExecutionStopped,
    MemoryExceeded,
    ResourceLimitExceeded,
    InternalInvariantViolation,
    SandboxViolation,
    MainFailed,
}

impl LuaVmFatalFault {
    const fn error_kind(self) -> LuaVmErrorKind {
        match self {
            Self::CpuTimeExceeded => LuaVmErrorKind::CpuTimeExceeded,
            Self::ExecutionStopped => LuaVmErrorKind::ExecutionStopped,
            Self::MemoryExceeded => LuaVmErrorKind::MemoryExceeded,
            Self::ResourceLimitExceeded => LuaVmErrorKind::ResourceLimitExceeded,
            Self::InternalInvariantViolation => LuaVmErrorKind::InternalInvariantViolation,
            Self::SandboxViolation => LuaVmErrorKind::SandboxViolation,
            Self::MainFailed => LuaVmErrorKind::MainFailed,
        }
    }
}

/// A Lua failure with implementation details removed from its stable text.
pub(crate) struct LuaVmError {
    kind: LuaVmErrorKind,
    source: Option<MluaError>,
}

impl LuaVmError {
    const fn without_source(kind: LuaVmErrorKind) -> Self {
        Self { kind, source: None }
    }

    fn with_source(kind: LuaVmErrorKind, source: MluaError) -> Self {
        Self {
            kind,
            source: Some(source),
        }
    }

    #[must_use]
    pub(crate) const fn kind(&self) -> LuaVmErrorKind {
        self.kind
    }

    /// Returns advisory Human-facing failure text for online diagnostics.
    ///
    /// Only the message the script author wrote is exposed: the position prefix
    /// and the traceback the runtime attaches to a Lua error value never become
    /// diagnostic text, and the stable identity of a failure remains
    /// [`Self::kind`]. The text is advisory because Lua error text is not a
    /// stable interface.
    pub(crate) fn diagnostic_detail(&self) -> Cow<'_, str> {
        self.source
            .as_ref()
            .and_then(lua_error_message)
            .map_or_else(
                || Cow::Owned(self.to_string()),
                |message| Cow::Borrowed(lua_message_text(message)),
            )
    }
}

/// Returns the Lua-level message of a nested mlua error, if it carries one.
fn lua_error_message(error: &MluaError) -> Option<&str> {
    match error {
        MluaError::RuntimeError(message) => Some(message),
        MluaError::BadArgument { cause, .. }
        | MluaError::CallbackError { cause, .. }
        | MluaError::WithContext { cause, .. } => lua_error_message(cause),
        _ => None,
    }
}

/// Removes the runtime position prefix and appended traceback from one message.
///
/// `mlua` runs every protected call through `luaL_traceback`, and Lua prefixes a
/// string raised by `error` with the chunk name and line. Both name internals of
/// this runtime rather than anything the script author wrote.
fn lua_message_text(message: &str) -> &str {
    let message = match message.split_once("\nstack traceback:") {
        Some((message, _)) => message,
        None => message,
    };
    let Some(rest) = message.strip_prefix("[string \"") else {
        return message;
    };
    let Some((_, rest)) = rest.split_once("\"]:") else {
        return message;
    };
    let Some((line, rest)) = rest.split_once(": ") else {
        return message;
    };
    if line.is_empty() || !line.bytes().all(|byte| byte.is_ascii_digit()) {
        return message;
    }
    rest
}

impl fmt::Debug for LuaVmError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LuaVmError")
            .field("kind", &self.kind)
            .field("source", &self.source.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

impl fmt::Display for LuaVmError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            LuaVmErrorKind::InitializationFailed => "Lua VM initialization failed",
            LuaVmErrorKind::SyntaxInvalid => "Lua source syntax is invalid",
            LuaVmErrorKind::TopLevelFailed => "Lua top-level initialization failed",
            LuaVmErrorKind::CpuTimeExceeded => "Lua CPU time limit exceeded",
            LuaVmErrorKind::ExecutionStopped => "Lua execution stopped",
            LuaVmErrorKind::MemoryExceeded => "Lua VM memory limit exceeded",
            LuaVmErrorKind::ResourceLimitExceeded => "Lua VM resource safety limit exceeded",
            LuaVmErrorKind::InternalInvariantViolation => "Lua VM internal invariant was violated",
            LuaVmErrorKind::SandboxViolation => "Lua sandbox boundary was violated",
            LuaVmErrorKind::MainMissing => "Lua global main is missing",
            LuaVmErrorKind::MainNotFunction => "Lua global main is not a function",
            LuaVmErrorKind::MainFailed => "Lua main execution failed",
        })
    }
}

impl Error for LuaVmError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_ref().map(|source| source as _)
    }
}

/// A private failure while compiling or initializing one Tenon Document source.
#[derive(Debug)]
pub(crate) struct TenonDocumentLuaValidationError {
    kind: LuaVmErrorKind,
}

impl TenonDocumentLuaValidationError {
    #[must_use]
    pub(crate) const fn issue_code(&self) -> &'static str {
        self.kind.tenon_document_issue_code()
    }

    fn vm(source: LuaVmError) -> Self {
        Self {
            kind: source.kind(),
        }
    }
}

impl fmt::Display for TenonDocumentLuaValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Tenon Document Lua validation failed")
    }
}

impl Error for TenonDocumentLuaValidationError {}

/// Compiles one Tenon Document source under resource limits without executing it.
pub(crate) fn validate_tenon_document_source_syntax(
    source: &str,
    limits: ScriptVmLimits,
) -> Result<(), TenonDocumentLuaValidationError> {
    let lua = Lua::new_with(StdLib::NONE, LuaOptions::default()).map_err(|source| {
        TenonDocumentLuaValidationError::vm(LuaVmError::with_source(
            LuaVmErrorKind::InitializationFailed,
            source,
        ))
    })?;
    if lua.used_memory() >= limits.memory_bytes().get() {
        return Err(TenonDocumentLuaValidationError::vm(
            LuaVmError::without_source(LuaVmErrorKind::InitializationFailed),
        ));
    }
    lua.set_memory_limit(limits.memory_bytes().get())
        .map_err(|source| {
            TenonDocumentLuaValidationError::vm(LuaVmError::with_source(
                LuaVmErrorKind::InitializationFailed,
                source,
            ))
        })?;

    let guarded_source = format!("{SOURCE_ENVIRONMENT_PREFIX}{source}");
    let started = ThreadTime::now();
    let result = lua
        .load(guarded_source)
        .set_name("tenon_document.process.script")
        .set_mode(ChunkMode::Text)
        .into_function();
    let result = match result {
        Err(source) if contains_memory_error(&source) => {
            return Err(TenonDocumentLuaValidationError::vm(
                LuaVmError::with_source(LuaVmErrorKind::MemoryExceeded, source),
            ));
        }
        result => result,
    };
    if started.elapsed() >= limits.cpu_time() {
        return Err(TenonDocumentLuaValidationError::vm(
            LuaVmError::without_source(LuaVmErrorKind::CpuTimeExceeded),
        ));
    }
    result.map(drop).map_err(|source| {
        TenonDocumentLuaValidationError::vm(LuaVmError::with_source(
            LuaVmErrorKind::SyntaxInvalid,
            source,
        ))
    })
}

#[derive(Debug)]
struct ExecutionBudget {
    started: ThreadTime,
    limit: Duration,
}

enum LuaApiFailure {
    Api(&'static str),
    MemoryExceeded,
    ResourceLimitExceeded,
    InternalInvariantViolation,
    Vm(MluaError),
}

impl From<MluaError> for LuaApiFailure {
    fn from(error: MluaError) -> Self {
        Self::Vm(error)
    }
}

type LuaApiResult<T> = Result<T, LuaApiFailure>;

#[derive(Debug)]
struct SandboxGuards {
    protected_names: Rc<RefCell<HashSet<Vec<u8>>>>,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
}

/// Complete observable result of one `main(event)` call.
#[derive(Debug)]
pub(crate) struct LuaMainOutcome {
    result: Result<(), LuaVmError>,
    emit_boundaries: Vec<emit::EmitBoundary>,
}

impl LuaMainOutcome {
    pub(crate) fn into_parts(self) -> (Result<(), LuaVmError>, Vec<emit::EmitBoundary>) {
        (self.result, self.emit_boundaries)
    }
}

/// One owned, thread-affine Lua state whose top-level program has loaded.
pub(crate) struct LuaVm {
    lua: Lua,
    source_payload_root: MessageDescriptor,
    environment: Table,
    environment_values: Table,
    protected_names: Rc<RefCell<HashSet<Vec<u8>>>>,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
    execution_budget: Rc<RefCell<Option<ExecutionBudget>>>,
    emit_slot: emit::EmitSlot,
    timer_slot: timer::TimerSlot,
    cpu_time_limit: Duration,
    native_memory_budget: Rc<LuaNativeMemoryBudget>,
    metrics: Option<VmMetrics>,
}

impl fmt::Debug for LuaVm {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LuaVm")
            .field("cpu_time_limit", &self.cpu_time_limit)
            .field("fatal_fault", &self.fatal_fault.get())
            .finish_non_exhaustive()
    }
}

impl LuaVm {
    /// Creates one VM from caller-selected, already validated interface roots.
    pub(crate) fn load(
        source: &str,
        limits: ScriptVmLimits,
        max_record_bytes: NonZeroU64,
        source_payload_root: MessageDescriptor,
        sink_payload_roots: HashMap<SinkContractId, MessageDescriptor>,
        print: LuaPrintCallback,
        stop_requested: impl Fn() -> bool + 'static,
    ) -> Result<Self, LuaVmError> {
        Self::load_observed(
            source,
            limits,
            max_record_bytes,
            source_payload_root,
            sink_payload_roots,
            print,
            stop_requested,
            None,
        )
    }

    /// Production callers supply a VM-lifetime observation; validation supplies none.
    #[expect(
        clippy::too_many_arguments,
        reason = "VM construction receives independent runtime, contract, and observation inputs"
    )]
    pub(crate) fn load_observed(
        source: &str,
        limits: ScriptVmLimits,
        max_record_bytes: NonZeroU64,
        source_payload_root: MessageDescriptor,
        sink_payload_roots: HashMap<SinkContractId, MessageDescriptor>,
        print: LuaPrintCallback,
        stop_requested: impl Fn() -> bool + 'static,
        metrics: Option<VmMetrics>,
    ) -> Result<Self, LuaVmError> {
        Self::load_observed_at(
            source,
            limits,
            max_record_bytes,
            source_payload_root,
            sink_payload_roots,
            print,
            stop_requested,
            metrics,
            Instant::now(),
            Rc::new(Cell::new(0)),
        )
    }

    /// Uses the Channel timeline and ordering sequence across VM replacements.
    #[expect(
        clippy::too_many_arguments,
        reason = "VM construction receives runtime, contract, observation, and Channel timeline inputs"
    )]
    pub(crate) fn load_observed_at(
        source: &str,
        limits: ScriptVmLimits,
        max_record_bytes: NonZeroU64,
        source_payload_root: MessageDescriptor,
        sink_payload_roots: HashMap<SinkContractId, MessageDescriptor>,
        print: LuaPrintCallback,
        stop_requested: impl Fn() -> bool + 'static,
        metrics: Option<VmMetrics>,
        timer_origin: Instant,
        event_order: Rc<Cell<u64>>,
    ) -> Result<Self, LuaVmError> {
        let lua = Lua::new_with(
            StdLib::STRING | StdLib::TABLE | StdLib::MATH | StdLib::UTF8 | StdLib::OS,
            LuaOptions::default(),
        )
        .map_err(|source| LuaVmError::with_source(LuaVmErrorKind::InitializationFailed, source))?;
        let fatal_fault = Rc::new(Cell::new(None));
        let execution_budget = Rc::new(RefCell::new(None));
        let sandbox = Sandbox::install(
            &lua,
            Rc::clone(&fatal_fault),
            Rc::clone(&execution_budget),
            payload::FrozenPayloadRegistry::new(sink_payload_roots),
            limits.memory_bytes().get(),
            max_record_bytes,
            print,
            timer_origin,
            event_order,
        )
        .map_err(|source| LuaVmError::with_source(LuaVmErrorKind::InitializationFailed, source))?;

        if lua.used_memory() >= limits.memory_bytes().get() {
            return Err(LuaVmError::without_source(
                LuaVmErrorKind::InitializationFailed,
            ));
        }
        lua.set_memory_limit(limits.memory_bytes().get())
            .map_err(|source| {
                LuaVmError::with_source(LuaVmErrorKind::InitializationFailed, source)
            })?;
        install_cpu_hook(
            &lua,
            Rc::clone(&fatal_fault),
            Rc::clone(&execution_budget),
            stop_requested,
        )
        .map_err(|source| LuaVmError::with_source(LuaVmErrorKind::InitializationFailed, source))?;

        let vm = Self {
            lua,
            source_payload_root,
            environment: sandbox.environment,
            environment_values: sandbox.environment_values,
            protected_names: sandbox.protected_names,
            fatal_fault,
            execution_budget,
            emit_slot: sandbox.emit_slot,
            timer_slot: sandbox.timer_slot,
            cpu_time_limit: limits.cpu_time(),
            native_memory_budget: sandbox.native_memory_budget,
            metrics,
        };
        vm.publish_memory();
        let loaded = vm.compile_and_run(source).and_then(|()| vm.freeze_main());
        vm.publish_memory();
        loaded?;
        Ok(vm)
    }

    pub(crate) fn call_source(
        &mut self,
        timestamp_millis: i64,
        payload: Bytes,
    ) -> Result<LuaMainOutcome, event::SourcePayloadDecodeError> {
        if let Some(outcome) = self.terminal_outcome() {
            return Ok(outcome);
        }
        let event = event::decode_source(
            event::KernelTimestampMillis(timestamp_millis),
            self.source_payload_root.clone(),
            payload,
        )?;
        Ok(self.call_main_ready(&event))
    }

    pub(crate) fn call_timer_event(
        &mut self,
        timestamp_millis: i64,
        timer: TimerEvent,
    ) -> LuaMainOutcome {
        if let Some(outcome) = self.terminal_outcome() {
            return outcome;
        }

        self.call_main_ready(&event::ProcessEvent::Timer {
            timestamp: event::KernelTimestampMillis(timestamp_millis),
            id: timer.id,
            eligible_at: event::KernelTimestampMillis(timer.eligible_at),
        })
    }

    pub(crate) fn next_timer_event(&self) -> Option<TimerEvent> {
        if self.terminal_error_kind().is_some() {
            None
        } else {
            self.timer_slot.next_event()
        }
    }

    pub(crate) fn begin_timer(&self) -> Result<TimerEvent, TimerSlotInactive> {
        self.timer_slot.begin()
    }

    fn publish_memory(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.publish_memory(self.lua.used_memory() + self.native_memory_budget.used_bytes());
        }
    }

    fn compile_and_run(&self, source: &str) -> Result<(), LuaVmError> {
        let guarded_source = format!("{SOURCE_ENVIRONMENT_PREFIX}{source}");
        let chunk = self
            .lua
            .load(guarded_source)
            .set_name("tenon_document.process.script")
            .set_mode(ChunkMode::Text)
            .set_environment(self.environment.clone());
        let function = self
            .run_with_budget(|| chunk.into_function())
            .map_err(|error| self.map_error(error, LuaVmErrorKind::SyntaxInvalid))?;

        self.run_with_budget(|| function.call::<()>(()))
            .map_err(|error| self.map_error(error, LuaVmErrorKind::TopLevelFailed))
    }

    fn freeze_main(&self) -> Result<(), LuaVmError> {
        match self
            .environment_values
            .raw_get::<Value>("main")
            .map_err(|source| {
                LuaVmError::with_source(LuaVmErrorKind::InitializationFailed, source)
            })? {
            Value::Nil => Err(LuaVmError::without_source(LuaVmErrorKind::MainMissing)),
            Value::Function(_) => {
                self.protected_names.borrow_mut().insert(b"main".to_vec());
                Ok(())
            }
            _ => Err(LuaVmError::without_source(LuaVmErrorKind::MainNotFunction)),
        }
    }

    fn call_main_ready(&mut self, event: &event::ProcessEvent) -> LuaMainOutcome {
        let outcome = self.call_main_once(event);
        self.publish_memory();
        if outcome.result.is_err() {
            record_fatal_fault(&self.fatal_fault, LuaVmFatalFault::MainFailed);
        }
        outcome
    }

    fn terminal_outcome(&self) -> Option<LuaMainOutcome> {
        self.terminal_error_kind().map(|kind| LuaMainOutcome {
            result: Err(LuaVmError::without_source(kind)),
            emit_boundaries: Vec::new(),
        })
    }

    fn terminal_error_kind(&self) -> Option<LuaVmErrorKind> {
        self.fatal_fault.get().map(LuaVmFatalFault::error_kind)
    }

    fn call_main_once(&self, event: &event::ProcessEvent) -> LuaMainOutcome {
        let event = match event::project(&self.lua, event, Rc::clone(&self.fatal_fault)) {
            Ok(event) => event,
            Err(error) => {
                return LuaMainOutcome {
                    result: Err(self.map_error(error, LuaVmErrorKind::MainFailed)),
                    emit_boundaries: Vec::new(),
                };
            }
        };
        let main = match self.environment_values.raw_get::<Function>("main") {
            Ok(main) => main,
            Err(error) => {
                return LuaMainOutcome {
                    result: Err(self.map_error(error, LuaVmErrorKind::MainFailed)),
                    emit_boundaries: Vec::new(),
                };
            }
        };
        if self.emit_slot.begin_main().is_err() {
            return LuaMainOutcome {
                result: Err(self.internal_invariant_error()),
                emit_boundaries: Vec::new(),
            };
        }
        let mut elapsed = None;
        let result = self
            .run_with_budget(|| {
                let started = self.metrics.as_ref().map(|_| Instant::now());
                let result = main.call::<()>(event);
                elapsed = started.map(|started| started.elapsed());
                result
            })
            .map_err(|error| self.map_error(error, LuaVmErrorKind::MainFailed));
        if let (Some(metrics), Some(elapsed)) = (&self.metrics, elapsed) {
            metrics.record_duration(elapsed);
        }
        let emit_boundaries = match self.emit_slot.finish_main() {
            Ok(emit_boundaries) => emit_boundaries,
            Err(_) => {
                return LuaMainOutcome {
                    result: Err(self.internal_invariant_error()),
                    emit_boundaries: Vec::new(),
                };
            }
        };
        LuaMainOutcome {
            result,
            emit_boundaries,
        }
    }

    fn run_with_budget<T>(&self, operation: impl FnOnce() -> mlua::Result<T>) -> mlua::Result<T> {
        if let Some(kind) = self.fatal_fault.get() {
            return Err(fatal_error(kind));
        }
        self.execution_budget.replace(Some(ExecutionBudget {
            started: ThreadTime::now(),
            limit: self.cpu_time_limit,
        }));
        let result = operation();
        let completed_budget = self.execution_budget.replace(None);

        if result.as_ref().is_err_and(contains_memory_error) {
            record_fatal_fault(&self.fatal_fault, LuaVmFatalFault::MemoryExceeded);
        }
        if self.fatal_fault.get().is_none()
            && completed_budget
                .as_ref()
                .is_some_and(|budget| budget.started.elapsed() >= budget.limit)
        {
            self.fatal_fault.set(Some(LuaVmFatalFault::CpuTimeExceeded));
        }
        if let Some(kind) = self.fatal_fault.get() {
            return Err(fatal_error(kind));
        }
        result
    }

    fn map_error(&self, error: MluaError, fallback: LuaVmErrorKind) -> LuaVmError {
        if let Some(kind) = self.fatal_fault.get() {
            return LuaVmError::with_source(kind.error_kind(), error);
        }
        if contains_memory_error(&error) {
            let kind = record_fatal_fault(&self.fatal_fault, LuaVmFatalFault::MemoryExceeded);
            return LuaVmError::with_source(kind.error_kind(), error);
        }
        LuaVmError::with_source(fallback, error)
    }

    fn internal_invariant_error(&self) -> LuaVmError {
        let fault = record_fatal_fault(
            &self.fatal_fault,
            LuaVmFatalFault::InternalInvariantViolation,
        );
        LuaVmError::with_source(fault.error_kind(), fatal_error(fault))
    }
}

struct Sandbox {
    environment: Table,
    environment_values: Table,
    protected_names: Rc<RefCell<HashSet<Vec<u8>>>>,
    emit_slot: emit::EmitSlot,
    timer_slot: timer::TimerSlot,
    native_memory_budget: Rc<LuaNativeMemoryBudget>,
}

impl Sandbox {
    #[expect(
        clippy::too_many_arguments,
        reason = "Sandbox installation binds VM resource owners and the Channel timeline"
    )]
    fn install(
        lua: &Lua,
        fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
        execution_budget: Rc<RefCell<Option<ExecutionBudget>>>,
        payload_registry: payload::FrozenPayloadRegistry,
        memory_limit_bytes: usize,
        max_record_bytes: NonZeroU64,
        print: LuaPrintCallback,
        timer_origin: Instant,
        event_order: Rc<Cell<u64>>,
    ) -> mlua::Result<Self> {
        let native_globals = lua.globals();
        let guards = SandboxGuards {
            protected_names: Rc::new(RefCell::new(HashSet::new())),
            fatal_fault,
        };
        let environment_values = lua.create_table()?;
        let environment = lua.create_table()?;
        let emit_slot = emit::EmitSlot::default();
        let timer_slot = timer::TimerSlot::new(timer_origin, event_order);

        install_environment_metatable(
            lua,
            &environment,
            &environment_values,
            Rc::clone(&guards.protected_names),
            Rc::clone(&guards.fatal_fault),
        )?;
        protect_name(&guards.protected_names, "_ENV");

        for &name in BASE_GLOBALS {
            copy_global(&native_globals, &environment_values, name)?;
            protect_name(&guards.protected_names, name);
        }
        install_print(
            lua,
            &environment_values,
            native_globals.get::<Function>("tostring")?,
            print,
        )?;
        protect_name(&guards.protected_names, "print");
        install_current_time(
            lua,
            &environment_values,
            Rc::clone(&guards.protected_names),
            Rc::clone(&guards.fatal_fault),
            Rc::clone(&execution_budget),
        )?;

        for (library_name, fields) in [
            ("string", STRING_FUNCTIONS),
            ("table", TABLE_FUNCTIONS),
            ("math", MATH_FIELDS),
            ("utf8", UTF8_FIELDS),
            ("os", OS_FUNCTIONS),
        ] {
            let native_library = native_globals.get::<Table>(library_name)?;
            let proxy = install_readonly_library(
                lua,
                &native_library,
                fields,
                Rc::clone(&guards.fatal_fault),
            )?;
            environment_values.raw_set(library_name, proxy.clone())?;
            protect_name(&guards.protected_names, library_name);

            if library_name == "string" {
                let actual_metatable = lua.create_table()?;
                actual_metatable.raw_set("__index", proxy)?;
                lua.set_type_metatable::<LuaString>(Some(actual_metatable));
            }
        }
        install_iteration_functions(
            lua,
            &native_globals,
            &environment_values,
            Rc::clone(&guards.protected_names),
        )?;
        install_protected_calls(
            lua,
            &native_globals,
            &environment_values,
            Rc::clone(&guards.protected_names),
            Rc::clone(&guards.fatal_fault),
        )?;
        json::install(
            lua,
            &environment_values,
            Rc::clone(&guards.protected_names),
            Rc::clone(&guards.fatal_fault),
            Rc::clone(&execution_budget),
        )?;
        bytes::install(
            lua,
            &environment_values,
            Rc::clone(&guards.protected_names),
            Rc::clone(&guards.fatal_fault),
            Rc::clone(&execution_budget),
        )?;
        let native_memory_budget = Rc::new(LuaNativeMemoryBudget::new(
            lua,
            memory_limit_bytes,
            Rc::clone(&guards.fatal_fault),
        ));
        payload::install(
            lua,
            &environment_values,
            Rc::clone(&guards.protected_names),
            Rc::clone(&guards.fatal_fault),
            Rc::clone(&execution_budget),
            payload_registry,
            Rc::clone(&native_memory_budget),
        )?;
        timer::install(
            lua,
            &environment_values,
            Rc::clone(&guards.protected_names),
            Rc::clone(&guards.fatal_fault),
            Rc::clone(&execution_budget),
            timer_slot.clone(),
            Rc::clone(&native_memory_budget),
        )?;

        environment_values.raw_set("_G", environment.clone())?;
        protect_name(&guards.protected_names, "_G");
        emit::install(
            lua,
            &environment_values,
            Rc::clone(&guards.protected_names),
            Rc::clone(&guards.fatal_fault),
            execution_budget,
            emit_slot.clone(),
            max_record_bytes,
        )?;

        Ok(Self {
            environment,
            environment_values,
            protected_names: guards.protected_names,
            emit_slot,
            timer_slot,
            native_memory_budget,
        })
    }
}

fn install_current_time(
    lua: &Lua,
    environment_values: &Table,
    protected_names: Rc<RefCell<HashSet<Vec<u8>>>>,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
    execution_budget: Rc<RefCell<Option<ExecutionBudget>>>,
) -> mlua::Result<()> {
    let native = lua.create_function(move |lua, ()| {
        let result = system_time_millis(SystemTime::now()).map(Value::Integer);
        finish_api_call(lua, result, &fatal_fault, &execution_budget)
    })?;
    environment_values.raw_set(
        "currentTimeMillis",
        create_catchable_api_wrapper_factory(lua)?.call::<Function>(native)?,
    )?;
    protect_name(&protected_names, "currentTimeMillis");
    Ok(())
}

fn system_time_millis(now: SystemTime) -> LuaApiResult<i64> {
    let elapsed = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| LuaApiFailure::Api(CURRENT_TIME_ERROR))?;
    i64::try_from(elapsed.as_millis()).map_err(|_| LuaApiFailure::Api(CURRENT_TIME_ERROR))
}

fn install_print(
    lua: &Lua,
    environment_values: &Table,
    native_tostring: Function,
    callback: LuaPrintCallback,
) -> mlua::Result<()> {
    let print = match callback {
        Some(callback) => lua.create_function(move |_, arguments: MultiValue| {
            callback(LuaPrintRecord {
                arguments,
                native_tostring: native_tostring.clone(),
            })
            .map_err(LuaPrintCallbackError::into_mlua)
        })?,
        None => lua.create_function(|_, _: MultiValue| Ok(()))?,
    };
    environment_values.raw_set("print", print)
}

#[allow(
    clippy::expect_used,
    reason = "the renderer appends only validated UTF-8 slices and a fixed UTF-8 replacement"
)]
fn render_print(
    arguments: MultiValue,
    native_tostring: &Function,
) -> mlua::Result<(Box<str>, bool, bool)> {
    let estimated_bytes = arguments.len() * 16;
    let mut text = Vec::with_capacity(DIAGNOSTIC_TEXT_MAXIMUM_BYTES.min(estimated_bytes));
    let mut truncated = false;
    let mut found_invalid_utf8 = false;
    for (index, argument) in arguments.into_iter().enumerate() {
        if index != 0 {
            append_print_text(&mut text, "\t", &mut truncated);
        }
        let rendered = native_tostring.call::<LuaString>(argument)?;
        for chunk in rendered.as_bytes().utf8_chunks() {
            append_print_text(&mut text, chunk.valid(), &mut truncated);
            if !chunk.invalid().is_empty() {
                found_invalid_utf8 = true;
                append_print_text(&mut text, "�", &mut truncated);
            }
        }
    }
    let text = String::from_utf8(text).expect("Lua print renderer only appends valid UTF-8");
    Ok((text.into_boxed_str(), truncated, found_invalid_utf8))
}

fn append_print_text(output: &mut Vec<u8>, value: &str, truncated: &mut bool) {
    let remaining = DIAGNOSTIC_TEXT_MAXIMUM_BYTES - output.len();
    let mut copied = remaining.min(value.len());
    while !value.is_char_boundary(copied) {
        copied -= 1;
    }
    output.extend_from_slice(&value.as_bytes()[..copied]);
    *truncated |= copied != value.len();
}

fn install_environment_metatable(
    lua: &Lua,
    environment: &Table,
    environment_values: &Table,
    protected_names: Rc<RefCell<HashSet<Vec<u8>>>>,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
) -> mlua::Result<()> {
    let metatable = lua.create_table()?;
    metatable.raw_set("__index", environment_values.clone())?;
    metatable.raw_set(PROXY_BACKING_METAFIELD, environment_values.clone())?;
    let values = environment_values.clone();
    metatable.raw_set(
        "__newindex",
        lua.create_function(move |_, (_table, key, value): (Table, Value, Value)| {
            write_environment_value(&values, &protected_names, &fatal_fault, key, value)
        })?,
    )?;
    environment.set_metatable(Some(metatable))
}

fn install_readonly_library(
    lua: &Lua,
    native_library: &Table,
    fields: &[&str],
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
) -> mlua::Result<Table> {
    let backing = lua.create_table()?;
    for field in fields {
        backing.raw_set(*field, native_library.get::<Value>(*field)?)?;
    }
    install_readonly_backing(lua, backing, fatal_fault)
}

fn install_readonly_backing(
    lua: &Lua,
    backing: Table,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
) -> mlua::Result<Table> {
    let proxy = lua.create_table()?;
    let metatable = lua.create_table()?;
    metatable.raw_set("__index", backing.clone())?;
    metatable.raw_set(PROXY_BACKING_METAFIELD, backing.clone())?;
    let length_fault = Rc::clone(&fatal_fault);
    metatable.raw_set(
        "__len",
        lua.create_function(move |_, proxy: Table| {
            let Some(backing) = proxy_backing(&proxy)? else {
                return Err(sandbox_violation(&length_fault));
            };
            i64::try_from(backing.raw_len()).map_err(|_| {
                fatal_error(record_fatal_fault(
                    &length_fault,
                    LuaVmFatalFault::ResourceLimitExceeded,
                ))
            })
        })?,
    )?;
    metatable.raw_set(
        "__newindex",
        lua.create_function(move |_, _: MultiValue| Err::<(), _>(sandbox_violation(&fatal_fault)))?,
    )?;
    proxy.set_metatable(Some(metatable))?;
    Ok(proxy)
}

fn proxy_backing(table: &Table) -> mlua::Result<Option<Table>> {
    table
        .metatable()
        .map(|metatable| metatable.raw_get::<Value>(PROXY_BACKING_METAFIELD))
        .transpose()
        .map(|value| match value {
            Some(Value::Table(backing)) => Some(backing),
            _ => None,
        })
}

fn publish_readonly_namespace(
    lua: &Lua,
    environment_values: &Table,
    name: &str,
    backing: Table,
    protected_names: &Rc<RefCell<HashSet<Vec<u8>>>>,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
) -> mlua::Result<()> {
    let proxy = install_readonly_backing(lua, backing, fatal_fault)?;
    environment_values.raw_set(name, proxy)?;
    protect_name(protected_names, name);
    Ok(())
}

fn install_iteration_functions(
    lua: &Lua,
    native_globals: &Table,
    environment_values: &Table,
    protected_names: Rc<RefCell<HashSet<Vec<u8>>>>,
) -> mlua::Result<()> {
    let native_next = native_globals.get::<Function>("next")?;
    let next_function = lua.create_function(move |_, (table, key): (Table, Value)| {
        let logical_table = proxy_backing(&table)?.unwrap_or_else(|| table.clone());
        native_next.call::<MultiValue>((logical_table, key))
    })?;
    environment_values.raw_set("next", next_function.clone())?;
    protect_name(&protected_names, "next");

    let pairs_next = next_function;
    environment_values.raw_set(
        "pairs",
        lua.create_function(move |_, table: Table| Ok((pairs_next.clone(), table, Value::Nil)))?,
    )?;
    protect_name(&protected_names, "pairs");
    Ok(())
}

fn install_protected_calls(
    lua: &Lua,
    native_globals: &Table,
    environment_values: &Table,
    protected_names: Rc<RefCell<HashSet<Vec<u8>>>>,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
) -> mlua::Result<()> {
    let native_xpcall = native_globals.get::<Function>("xpcall")?;
    let handler_factory = lua
        .load(
            r#"
            return function(mark_called, handler)
                return function(error)
                    mark_called()
                    if handler == nil then
                        return error
                    end
                    return handler(error)
                end
            end
            "#,
        )
        .eval::<Function>()?;
    let pcall_native_xpcall = native_xpcall.clone();
    let pcall_handler_factory = handler_factory.clone();
    let pcall_fault = Rc::clone(&fatal_fault);
    environment_values.raw_set(
        "pcall",
        lua.create_function(move |lua, mut arguments: MultiValue| {
            let handler_called = Rc::new(Cell::new(false));
            let callback_flag = Rc::clone(&handler_called);
            let callback_fault = Rc::clone(&pcall_fault);
            let mark_called = lua.create_function(move |_, ()| {
                if let Some(fault) = callback_fault.get() {
                    return Err(fatal_error(fault));
                }
                callback_flag.set(true);
                Ok(())
            })?;
            let identity_handler =
                pcall_handler_factory.call::<Function>((mark_called, Value::Nil))?;
            if arguments.is_empty() {
                arguments.push_back(Value::Nil);
            }
            arguments.insert(1, Value::Function(identity_handler));
            let result = pcall_native_xpcall.call::<MultiValue>(arguments)?;
            enforce_protected_call_result(&pcall_fault, handler_called.get(), &result)?;
            Ok(result)
        })?,
    )?;
    protect_name(&protected_names, "pcall");

    let xpcall_fault = fatal_fault;
    let xpcall_handler_factory = handler_factory;
    environment_values.raw_set(
        "xpcall",
        lua.create_function(move |lua, mut arguments: MultiValue| {
            let user_handler = match arguments.remove(1) {
                Some(Value::Function(handler)) => handler,
                _ => {
                    return Err(MluaError::RuntimeError(
                        "bad argument #2 to 'xpcall' (function expected)".to_owned(),
                    ));
                }
            };
            let handler_called = Rc::new(Cell::new(false));
            let callback_flag = Rc::clone(&handler_called);
            let callback_fault = Rc::clone(&xpcall_fault);
            let mark_called = lua.create_function(move |_, ()| {
                if let Some(fault) = callback_fault.get() {
                    return Err(fatal_error(fault));
                }
                callback_flag.set(true);
                Ok(())
            })?;
            let forwarding_handler =
                xpcall_handler_factory.call::<Function>((mark_called, user_handler))?;
            arguments.insert(1, Value::Function(forwarding_handler));
            let result = native_xpcall.call::<MultiValue>(arguments)?;
            enforce_protected_call_result(&xpcall_fault, handler_called.get(), &result)?;
            Ok(result)
        })?,
    )?;
    protect_name(&protected_names, "xpcall");
    Ok(())
}

fn copy_global(source: &Table, target: &Table, name: &str) -> mlua::Result<()> {
    target.raw_set(name, source.get::<Value>(name)?)
}

fn protect_name(names: &Rc<RefCell<HashSet<Vec<u8>>>>, name: &str) {
    names.borrow_mut().insert(name.as_bytes().to_vec());
}

fn write_environment_value(
    backing: &Table,
    protected_names: &Rc<RefCell<HashSet<Vec<u8>>>>,
    fatal_fault: &Rc<Cell<Option<LuaVmFatalFault>>>,
    key: Value,
    value: Value,
) -> mlua::Result<()> {
    if let Value::String(name) = &key
        && protected_names.borrow().contains(name.as_bytes().as_ref())
    {
        return Err(sandbox_violation(fatal_fault));
    }
    backing.raw_set(key, value)
}

fn install_cpu_hook(
    lua: &Lua,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
    execution_budget: Rc<RefCell<Option<ExecutionBudget>>>,
    stop_requested: impl Fn() -> bool + 'static,
) -> mlua::Result<()> {
    lua.set_hook(
        HookTriggers::new().every_nth_instruction(HOOK_INSTRUCTION_INTERVAL),
        move |_, _| {
            if stop_requested() {
                let fault = record_fatal_fault(&fatal_fault, LuaVmFatalFault::ExecutionStopped);
                return Err(fatal_error(fault));
            }
            enforce_elapsed_cpu_limit(&fatal_fault, &execution_budget)?;
            Ok(VmState::Continue)
        },
    )
}

fn enforce_elapsed_cpu_limit(
    fatal_fault: &Rc<Cell<Option<LuaVmFatalFault>>>,
    execution_budget: &Rc<RefCell<Option<ExecutionBudget>>>,
) -> mlua::Result<()> {
    let exceeded = execution_budget
        .borrow()
        .as_ref()
        .is_some_and(|budget| budget.started.elapsed() >= budget.limit);
    if exceeded {
        let fault = record_fatal_fault(fatal_fault, LuaVmFatalFault::CpuTimeExceeded);
        return Err(fatal_error(fault));
    }
    Ok(())
}

fn create_catchable_api_wrapper_factory(lua: &Lua) -> mlua::Result<Function> {
    lua.load(
        r#"
        return function(native)
            return function(...)
                local ok, result = native(...)
                if not ok then
                    error(result, 0)
                end
                return result
            end
        end
        "#,
    )
    .eval()
}

fn finish_api_call(
    lua: &Lua,
    result: LuaApiResult<Value>,
    fatal_fault: &Rc<Cell<Option<LuaVmFatalFault>>>,
    execution_budget: &Rc<RefCell<Option<ExecutionBudget>>>,
) -> mlua::Result<(bool, Value)> {
    match result {
        Ok(value) => {
            enforce_elapsed_cpu_limit(fatal_fault, execution_budget)?;
            Ok((true, value))
        }
        Err(LuaApiFailure::Api(message)) => {
            enforce_elapsed_cpu_limit(fatal_fault, execution_budget)?;
            lua.create_string(message)
                .map(|message| (false, Value::String(message)))
                .map_err(|error| record_vm_error(fatal_fault, error))
        }
        Err(LuaApiFailure::MemoryExceeded) => {
            let fault = record_fatal_fault(fatal_fault, LuaVmFatalFault::MemoryExceeded);
            Err(fatal_error(fault))
        }
        Err(LuaApiFailure::ResourceLimitExceeded) => {
            let fault = record_fatal_fault(fatal_fault, LuaVmFatalFault::ResourceLimitExceeded);
            Err(fatal_error(fault))
        }
        Err(LuaApiFailure::InternalInvariantViolation) => {
            let fault =
                record_fatal_fault(fatal_fault, LuaVmFatalFault::InternalInvariantViolation);
            Err(fatal_error(fault))
        }
        Err(LuaApiFailure::Vm(error)) => Err(record_vm_error(fatal_fault, error)),
    }
}

fn sandbox_violation(fatal_fault: &Rc<Cell<Option<LuaVmFatalFault>>>) -> MluaError {
    fatal_error(record_fatal_fault(
        fatal_fault,
        LuaVmFatalFault::SandboxViolation,
    ))
}

fn record_fatal_fault(
    fatal_fault: &Rc<Cell<Option<LuaVmFatalFault>>>,
    fault: LuaVmFatalFault,
) -> LuaVmFatalFault {
    if let Some(recorded_fault) = fatal_fault.get() {
        return recorded_fault;
    }
    fatal_fault.set(Some(fault));
    fault
}

fn fatal_error(fault: LuaVmFatalFault) -> MluaError {
    MluaError::RuntimeError(
        match fault {
            LuaVmFatalFault::CpuTimeExceeded => INTERNAL_CPU_LIMIT_ERROR,
            LuaVmFatalFault::ExecutionStopped => INTERNAL_EXECUTION_STOPPED_ERROR,
            LuaVmFatalFault::MemoryExceeded => INTERNAL_MEMORY_LIMIT_ERROR,
            LuaVmFatalFault::ResourceLimitExceeded => INTERNAL_RESOURCE_LIMIT_ERROR,
            LuaVmFatalFault::InternalInvariantViolation => INTERNAL_INVARIANT_ERROR,
            LuaVmFatalFault::SandboxViolation => INTERNAL_SANDBOX_ERROR,
            LuaVmFatalFault::MainFailed => INTERNAL_MAIN_FAILED_ERROR,
        }
        .to_owned(),
    )
}

fn enforce_protected_call_result(
    fatal_fault: &Rc<Cell<Option<LuaVmFatalFault>>>,
    handler_called: bool,
    result: &MultiValue,
) -> mlua::Result<()> {
    if let Some(kind) = fatal_fault.get() {
        return Err(fatal_error(kind));
    }
    if matches!(result.front(), Some(Value::Boolean(false))) && !handler_called {
        let fault = record_fatal_fault(fatal_fault, LuaVmFatalFault::MemoryExceeded);
        return Err(fatal_error(fault));
    }
    Ok(())
}

fn contains_memory_error(error: &MluaError) -> bool {
    match error {
        MluaError::MemoryError(_) => true,
        MluaError::BadArgument { cause, .. }
        | MluaError::CallbackError { cause, .. }
        | MluaError::WithContext { cause, .. } => contains_memory_error(cause),
        _ => false,
    }
}

fn record_vm_error(fatal_fault: &Rc<Cell<Option<LuaVmFatalFault>>>, error: MluaError) -> MluaError {
    if contains_memory_error(&error) {
        record_fatal_fault(fatal_fault, LuaVmFatalFault::MemoryExceeded);
    }
    error
}

#[cfg(test)]
pub(crate) mod tests;
