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

//! One-shot timers shared by one Lua VM and its event-loop owner.

use super::{
    ExecutionBudget, LuaApiFailure, LuaVmFatalFault, create_catchable_api_wrapper_factory,
    finish_api_call, memory::LuaNativeMemoryBudget, protect_name,
};
use mlua::{Function, Lua, MultiValue, Table, Value};
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashSet};
use std::rc::Rc;
use std::str;
use std::time::{Duration, Instant};

const DELAY_ERROR: &str = "setTimeout delay must be a non-negative integer";
const DELAY_RANGE_ERROR: &str = "setTimeout delay is out of range";
const TIMER_ID_ERROR: &str = "timer id must be a non-empty string of at most 128 bytes";
const MAX_TIMER_ID_BYTES: usize = 128;
const TIMER_ID_STORAGE_COPIES: usize = 3;
// Charge each pending timer a fixed 2 KiB for its entry and indexes, plus
// its three owned id copies. This includes a coarse container allowance;
// it is not an exact allocation or RSS measurement and does not depend on
// BTreeMap's internal node layout.
const TIMER_ENTRY_CHARGE_BYTES: usize = 2 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimerSchedule {
    pub(crate) scheduled_at: Instant,
    pub(crate) delay: Duration,
}

#[derive(Clone, Debug)]
pub(crate) struct TimerEvent {
    pub(crate) schedule: TimerSchedule,
    pub(crate) deadline: Instant,
    pub(crate) eligible_at: i64,
    pub(crate) id: Option<Box<str>>,
    pub(crate) sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimerSlotInactive;

#[derive(Debug, Default)]
struct TimerState {
    anonymous: Option<TimerEntry>,
    named: BTreeMap<String, TimerEntry>,
    order: BTreeMap<(Instant, u64), Option<String>>,
}

impl TimerState {
    fn release_empty_storage(&mut self) {
        if self.order.is_empty() {
            // Empty BTreeMaps can retain their last leaves. Release them when
            // no pending timer remains to carry the container allowance.
            self.named = BTreeMap::new();
            self.order = BTreeMap::new();
        }
    }
}

#[derive(Debug)]
struct TimerEntry {
    schedule: TimerSchedule,
    deadline: Instant,
    eligible_at: i64,
    id: Option<Box<str>>,
    sequence: u64,
    _memory: TimerMemoryReservation,
}

#[derive(Debug)]
struct TimerMemoryReservation {
    budget: Rc<LuaNativeMemoryBudget>,
    bytes: usize,
}

impl TimerMemoryReservation {
    fn reserve(budget: Rc<LuaNativeMemoryBudget>, bytes: usize) -> Result<Self, LuaApiFailure> {
        budget.replace(0, bytes)?;
        Ok(Self { budget, bytes })
    }
}

impl Drop for TimerMemoryReservation {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

#[derive(Clone, Debug)]
pub(super) struct TimerSlot {
    state: Rc<RefCell<TimerState>>,
    origin: Instant,
    sequence: Rc<Cell<u64>>,
}

impl TimerSlot {
    pub(super) fn new(origin: Instant, sequence: Rc<Cell<u64>>) -> Self {
        Self {
            state: Rc::new(RefCell::new(TimerState::default())),
            origin,
            sequence,
        }
    }

    pub(super) fn next_event(&self) -> Option<TimerEvent> {
        let state = self.state.borrow();
        let (_, id) = state.order.first_key_value()?;
        let entry = match id {
            None => state.anonymous.as_ref()?,
            Some(id) => state.named.get(id)?,
        };
        Some(TimerEvent {
            schedule: entry.schedule,
            deadline: entry.deadline,
            eligible_at: entry.eligible_at,
            id: entry.id.clone(),
            sequence: entry.sequence,
        })
    }

    pub(super) fn next_event_sequence(&self) -> Result<u64, ()> {
        let sequence = self.sequence.get().checked_add(1).ok_or(())?;
        self.sequence.set(sequence);
        Ok(sequence)
    }

    pub(super) fn begin(&self) -> Result<TimerEvent, TimerSlotInactive> {
        let mut state = self.state.borrow_mut();
        let (order_key, key) = state
            .order
            .first_key_value()
            .map(|(key, id)| (*key, id.clone()))
            .ok_or(TimerSlotInactive)?;
        let entry = match key {
            None => state.anonymous.take(),
            Some(id) => state.named.remove(&id),
        }
        .ok_or(TimerSlotInactive)?;
        state.order.remove(&order_key);
        state.release_empty_storage();
        let event = TimerEvent {
            schedule: entry.schedule,
            deadline: entry.deadline,
            eligible_at: entry.eligible_at,
            id: entry.id,
            sequence: entry.sequence,
        };
        Ok(event)
    }

    pub(super) fn has_timeout(&self, id: Option<&str>) -> bool {
        let state = self.state.borrow();
        match id {
            None => state.anonymous.is_some(),
            Some(id) => state.named.contains_key(id),
        }
    }

    fn set_timeout(
        &self,
        budget: Rc<LuaNativeMemoryBudget>,
        delay: Duration,
        id: Option<String>,
    ) -> Result<(), LuaApiFailure> {
        let scheduled_at = Instant::now();
        let deadline = checked_deadline(scheduled_at, delay)?;
        let eligible_at = i64::try_from(
            deadline
                .checked_duration_since(self.origin)
                .ok_or(LuaApiFailure::Api(DELAY_RANGE_ERROR))?
                .as_millis(),
        )
        .map_err(|_| LuaApiFailure::Api(DELAY_RANGE_ERROR))?;
        let mut state = self.state.borrow_mut();
        let sequence = self
            .next_event_sequence()
            .map_err(|_| LuaApiFailure::Api(DELAY_RANGE_ERROR))?;
        let schedule = TimerSchedule {
            scheduled_at,
            delay,
        };
        let target = id.as_deref();
        let old_key = match target {
            None => state
                .anonymous
                .as_ref()
                .map(|current| (current.deadline, current.sequence)),
            Some(id) => state
                .named
                .get(id)
                .map(|current| (current.deadline, current.sequence)),
        };
        if let Some(old_key) = old_key {
            let expected_id = id.clone();
            if state.order.get(&old_key) != Some(&expected_id) {
                return Err(LuaApiFailure::InternalInvariantViolation);
            }
            {
                let current = match target {
                    None => state
                        .anonymous
                        .as_mut()
                        .ok_or(LuaApiFailure::InternalInvariantViolation)?,
                    Some(id) => state
                        .named
                        .get_mut(id)
                        .ok_or(LuaApiFailure::InternalInvariantViolation)?,
                };
                // The same id has the same charge, so keep its reservation.
                current.schedule = schedule;
                current.deadline = deadline;
                current.eligible_at = eligible_at;
                current.id = id.as_ref().map(|value| value.clone().into_boxed_str());
                current.sequence = sequence;
            }
            state.order.remove(&old_key);
            state.order.insert((deadline, sequence), expected_id);
            return Ok(());
        }

        let id_bytes = id.as_ref().map_or(Ok(0), |value| {
            value
                .len()
                .checked_mul(TIMER_ID_STORAGE_COPIES)
                .ok_or(LuaApiFailure::MemoryExceeded)
        })?;
        let bytes = TIMER_ENTRY_CHARGE_BYTES
            .checked_add(id_bytes)
            .ok_or(LuaApiFailure::MemoryExceeded)?;
        let memory = TimerMemoryReservation::reserve(budget, bytes)?;
        let entry = TimerEntry {
            schedule,
            deadline,
            eligible_at,
            id: id.clone().map(String::into_boxed_str),
            sequence,
            _memory: memory,
        };

        if let Some(id) = id {
            state.order.insert((deadline, sequence), Some(id.clone()));
            state.named.insert(id, entry);
        } else {
            state.order.insert((deadline, sequence), None);
            state.anonymous = Some(entry);
        }
        Ok(())
    }

    fn clear(&self, id: Option<&str>) {
        let mut state = self.state.borrow_mut();
        match id {
            None => {
                if let Some(entry) = state.anonymous.take() {
                    state.order.remove(&(entry.deadline, entry.sequence));
                }
            }
            Some(id) => {
                if let Some(entry) = state.named.remove(id) {
                    state.order.remove(&(entry.deadline, entry.sequence));
                }
            }
        }
        state.release_empty_storage();
    }
}

fn checked_deadline(scheduled_at: Instant, delay: Duration) -> Result<Instant, LuaApiFailure> {
    scheduled_at
        .checked_add(delay)
        .ok_or(LuaApiFailure::Api(DELAY_RANGE_ERROR))
}

pub(super) fn install(
    lua: &Lua,
    environment_values: &Table,
    protected_names: Rc<RefCell<HashSet<Vec<u8>>>>,
    fatal_fault: Rc<Cell<Option<LuaVmFatalFault>>>,
    execution_budget: Rc<RefCell<Option<ExecutionBudget>>>,
    slot: TimerSlot,
    memory_budget: Rc<LuaNativeMemoryBudget>,
) -> mlua::Result<()> {
    let api_wrapper_factory = create_catchable_api_wrapper_factory(lua)?;

    let set_slot = slot.clone();
    let set_budget = Rc::clone(&memory_budget);
    let set_fault = Rc::clone(&fatal_fault);
    let set_execution_budget = Rc::clone(&execution_budget);
    let native_set = lua.create_function(move |lua, mut arguments: MultiValue| {
        let result = parse_delay(&mut arguments).and_then(|delay| {
            let id = parse_optional_id(arguments.pop_front())?;
            set_slot
                .set_timeout(Rc::clone(&set_budget), delay, id)
                .map(|()| Value::Nil)
        });
        finish_api_call(lua, result, &set_fault, &set_execution_budget)
    })?;
    environment_values.raw_set(
        "setTimeout",
        api_wrapper_factory.call::<Function>(native_set)?,
    )?;
    protect_name(&protected_names, "setTimeout");

    let clear_slot = slot.clone();
    let clear_fault = Rc::clone(&fatal_fault);
    let clear_execution_budget = Rc::clone(&execution_budget);
    let native_clear = lua.create_function(move |lua, mut arguments: MultiValue| {
        let result = parse_optional_id(arguments.pop_front()).map(|id| {
            clear_slot.clear(id.as_deref());
            Value::Nil
        });
        finish_api_call(lua, result, &clear_fault, &clear_execution_budget)
    })?;
    // Preserve the cancellation API's zero return values after catchable error handling.
    let clear_timeout: Function = lua
        .load("local clear = ...; return function(...) clear(...) end")
        .call(api_wrapper_factory.call::<Function>(native_clear)?)?;
    environment_values.raw_set("clearTimeout", clear_timeout)?;
    protect_name(&protected_names, "clearTimeout");

    let has_slot = slot;
    let has_fault = Rc::clone(&fatal_fault);
    let has_execution_budget = Rc::clone(&execution_budget);
    let native_has = lua.create_function(move |lua, mut arguments: MultiValue| {
        let result = parse_optional_id(arguments.pop_front())
            .map(|id| Value::Boolean(has_slot.has_timeout(id.as_deref())));
        finish_api_call(lua, result, &has_fault, &has_execution_budget)
    })?;
    environment_values.raw_set(
        "hasTimeout",
        api_wrapper_factory.call::<Function>(native_has)?,
    )?;
    protect_name(&protected_names, "hasTimeout");
    Ok(())
}

fn parse_delay(arguments: &mut MultiValue) -> Result<Duration, LuaApiFailure> {
    match arguments.pop_front() {
        Some(Value::Integer(delay)) if delay >= 0 => u64::try_from(delay)
            .map(Duration::from_millis)
            .map_err(|_| LuaApiFailure::Api(DELAY_RANGE_ERROR)),
        _ => Err(LuaApiFailure::Api(DELAY_ERROR)),
    }
}

fn parse_optional_id(value: Option<Value>) -> Result<Option<String>, LuaApiFailure> {
    match value {
        None | Some(Value::Nil) => Ok(None),
        Some(Value::String(value)) => {
            let bytes = value.as_bytes();
            if bytes.is_empty() || bytes.len() > MAX_TIMER_ID_BYTES {
                return Err(LuaApiFailure::Api(TIMER_ID_ERROR));
            }
            let value = str::from_utf8(&bytes).map_err(|_| LuaApiFailure::Api(TIMER_ID_ERROR))?;
            Ok(Some(value.to_owned()))
        }
        Some(_) => Err(LuaApiFailure::Api(TIMER_ID_ERROR)),
    }
}

#[cfg(test)]
mod tests {
    use super::{DELAY_RANGE_ERROR, LuaApiFailure, checked_deadline};
    use std::time::{Duration, Instant};

    #[test]
    fn equal_deadlines_use_sequence_and_dispatch_releases_all_memory() -> std::io::Result<()> {
        let vm = super::super::tests::load_vm(
            "setTimeout(0, 'z'); setTimeout(0, 'a'); setTimeout(0); function main(event) end",
            super::super::tests::limits()?,
        )
        .map_err(|error| std::io::Error::other(error.to_string()))?;
        // Equal clock readings are valid on a coarse monotonic clock. Keep the
        // real registrations and reservations, and give them that same reading.
        let deadline = Instant::now();
        let eligible_at = i64::try_from(deadline.duration_since(vm.timer_slot.origin).as_millis())
            .map_err(std::io::Error::other)?;
        {
            let mut state = vm.timer_slot.state.borrow_mut();
            let mut keys = Vec::new();
            for (id, entry) in &mut state.named {
                entry.deadline = deadline;
                entry.schedule.scheduled_at = deadline;
                entry.eligible_at = eligible_at;
                keys.push(((deadline, entry.sequence), Some(id.clone())));
            }
            let anonymous = state
                .anonymous
                .as_mut()
                .ok_or_else(|| std::io::Error::other("anonymous timer missing"))?;
            anonymous.deadline = deadline;
            anonymous.schedule.scheduled_at = deadline;
            anonymous.eligible_at = eligible_at;
            keys.push(((deadline, anonymous.sequence), None));
            state.order = keys.into_iter().collect();
        }
        for expected in [Some("z"), Some("a"), None] {
            let event = vm
                .begin_timer()
                .map_err(|_| std::io::Error::other("pending timer missing"))?;
            assert_eq!(event.id.as_deref(), expected);
        }
        let state = vm.timer_slot.state.borrow();
        assert!(state.order.is_empty());
        assert!(state.named.is_empty());
        assert_eq!(vm.native_memory_budget.used_bytes(), 0);
        Ok(())
    }

    #[test]
    fn failed_registration_preserves_the_existing_timer_and_memory_charge() -> std::io::Result<()> {
        let limits = super::super::tests::limits()?;
        let vm = super::super::tests::load_vm(
            "setTimeout(0, 'existing'); function main(event) end",
            limits,
        )
        .map_err(|error| std::io::Error::other(error.to_string()))?;
        let budget = std::rc::Rc::clone(&vm.native_memory_budget);
        let existing_charge = budget.used_bytes();
        // Leave one byte less than the new timer needs. Registration must
        // fail without changing either index or its existing reservation.
        let remaining = 2 * 1024 + 3 * "first".len() - 1;
        let retained =
            limits.memory_bytes().get() - vm.lua.used_memory() - existing_charge - remaining;
        let reservation =
            super::TimerMemoryReservation::reserve(std::rc::Rc::clone(&budget), retained)
                .map_err(|_| std::io::Error::other("initial reservation failed"))?;
        assert!(matches!(
            vm.timer_slot.set_timeout(
                std::rc::Rc::clone(&budget),
                Duration::ZERO,
                Some("first".to_owned())
            ),
            Err(LuaApiFailure::MemoryExceeded)
        ));
        assert_eq!(budget.used_bytes(), existing_charge + retained);
        assert!(vm.timer_slot.has_timeout(Some("existing")));
        assert!(!vm.timer_slot.has_timeout(Some("first")));
        assert_eq!(vm.timer_slot.state.borrow().named.len(), 1);
        assert_eq!(vm.timer_slot.state.borrow().order.len(), 1);
        // Replacement needs no extra budget, even with too little for a new id.
        vm.timer_slot
            .set_timeout(
                std::rc::Rc::clone(&budget),
                Duration::ZERO,
                Some("existing".to_owned()),
            )
            .map_err(|_| std::io::Error::other("replacement failed"))?;
        assert_eq!(budget.used_bytes(), existing_charge + retained);
        drop(reservation);
        assert_eq!(budget.used_bytes(), existing_charge);
        vm.timer_slot.clear(Some("existing"));
        assert_eq!(budget.used_bytes(), 0);
        Ok(())
    }

    #[test]
    fn deadline_overflow_is_reported_as_the_catchable_range_error() {
        assert!(matches!(
            checked_deadline(Instant::now(), Duration::MAX),
            Err(LuaApiFailure::Api(DELAY_RANGE_ERROR))
        ));
    }
}
