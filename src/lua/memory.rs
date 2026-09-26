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

//! Shared native-memory budget for one thread-affine Lua VM.
//!
//! Sandbox initialization creates one budget and shares it with Payload and
//! timer owners. Each owner supplies its memory charge and releases it when
//! its storage is dropped. The budget reduces the Lua allocator's allowance
//! so native charges and Lua allocations share the same VM limit.
//!
//! The Lua reference is weak: reservations may outlive the interpreter during
//! teardown without keeping it alive. Failed reservations leave both the
//! charge and allocator allowance unchanged. This module owns no payload or
//! timer storage and does not define their accounting estimates.

use super::{LuaApiFailure, LuaApiResult, LuaVmFatalFault, record_fatal_fault};
use mlua::{Lua, WeakLua};
use std::cell::Cell;
use std::fmt;
use std::rc::Rc;

pub(super) struct LuaNativeMemoryBudget {
    lua: WeakLua,
    limit_bytes: usize,
    used_bytes: Cell<usize>,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
}

impl LuaNativeMemoryBudget {
    pub(super) fn new(
        lua: &Lua,
        limit_bytes: usize,
        fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
    ) -> Self {
        Self {
            lua: lua.weak(),
            limit_bytes,
            used_bytes: Cell::new(0),
            fatal_fault,
        }
    }

    pub(super) fn used_bytes(&self) -> usize {
        self.used_bytes.get()
    }

    pub(super) fn replace(&self, previous_bytes: usize, next_bytes: usize) -> LuaApiResult<()> {
        let used_bytes = self.used_bytes.get();
        let retained_bytes = used_bytes
            .checked_sub(previous_bytes)
            .ok_or(LuaApiFailure::InternalInvariantViolation)?;
        let next_used_bytes = retained_bytes
            .checked_add(next_bytes)
            .ok_or(LuaApiFailure::MemoryExceeded)?;
        let lua = self
            .lua
            .try_upgrade()
            .ok_or(LuaApiFailure::InternalInvariantViolation)?;
        let next_lua_limit = self
            .limit_bytes
            .checked_sub(next_used_bytes)
            .ok_or(LuaApiFailure::MemoryExceeded)?;
        if lua.used_memory() > next_lua_limit {
            return Err(LuaApiFailure::MemoryExceeded);
        }
        lua.set_memory_limit(next_lua_limit)
            .map_err(|_| LuaApiFailure::InternalInvariantViolation)?;
        self.used_bytes.set(next_used_bytes);
        Ok(())
    }

    pub(super) fn release(&self, bytes: usize) {
        let Some(next_used_bytes) = self.used_bytes.get().checked_sub(bytes) else {
            record_fatal_fault(
                &self.fatal_fault,
                LuaVmFatalFault::InternalInvariantViolation,
            );
            return;
        };
        self.used_bytes.set(next_used_bytes);
        let Some(lua) = self.lua.try_upgrade() else {
            return;
        };
        let Some(next_lua_limit) = self.limit_bytes.checked_sub(next_used_bytes) else {
            record_fatal_fault(
                &self.fatal_fault,
                LuaVmFatalFault::InternalInvariantViolation,
            );
            return;
        };
        if lua.set_memory_limit(next_lua_limit).is_err() {
            record_fatal_fault(
                &self.fatal_fault,
                LuaVmFatalFault::InternalInvariantViolation,
            );
        }
    }
}
impl fmt::Debug for LuaNativeMemoryBudget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LuaNativeMemoryBudget")
            .field("limit_bytes", &self.limit_bytes)
            .field("used_bytes", &self.used_bytes.get())
            .finish_non_exhaustive()
    }
}
