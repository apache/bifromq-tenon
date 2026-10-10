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

//! A file updater owned by the test distribution, independent of the Runner internals.

use serde::Deserialize;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tenon::{ExecutionDenied, ExecutionPermit, PolicyChanges};
use tokio::sync::watch;

#[derive(Deserialize)]
struct FileState {
    customer: String,
    allowed: bool,
    valid_for_ms: u64,
}

pub(super) struct Snapshot {
    pub(super) customer: String,
    allowed: bool,
    deadline: Instant,
}

pub(super) struct LivePolicy {
    state: Arc<Mutex<Snapshot>>,
    updates: PolicyChanges,
    stop: mpsc::Sender<()>,
    worker: Option<JoinHandle<()>>,
}

impl LivePolicy {
    pub(super) fn start(path: PathBuf) -> io::Result<Self> {
        let initial = fs::read(&path)?;
        let snapshot =
            parse(&initial).ok_or_else(|| io::Error::other("Initial policy file is invalid"))?;
        let state = Arc::new(Mutex::new(snapshot));
        let (sender, updates) = watch::channel(());
        let (stop, stopped) = mpsc::channel();
        let shared = Arc::clone(&state);
        let worker = thread::spawn(move || {
            let mut last = initial;
            while matches!(
                stopped.recv_timeout(Duration::from_millis(5)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ) {
                let Ok(bytes) = fs::read(&path) else { continue };
                if bytes == last {
                    continue;
                }
                let Some(next) = parse(&bytes) else { continue };
                let Ok(mut state) = shared.lock() else { break };
                // This fixture permits renewal only for the original customer.
                if state.customer != next.customer {
                    continue;
                }
                *state = next;
                drop(state);
                last = bytes;
                sender.send_replace(());
            }
        });
        Ok(Self {
            state,
            updates,
            stop,
            worker: Some(worker),
        })
    }

    pub(super) fn decision(&self) -> Result<ExecutionPermit, ExecutionDenied> {
        let state = self.state.lock().map_err(|_| {
            ExecutionDenied::new(
                "fixture.state_failed",
                "Policy state is unavailable",
                Some(Instant::now()),
            )
        })?;
        let deadline = Some(state.deadline);
        if state.allowed {
            Ok(ExecutionPermit {
                entitlement_until: deadline,
            })
        } else {
            Err(ExecutionDenied::new(
                "fixture.license_denied",
                "Execution is not permitted",
                deadline,
            ))
        }
    }

    pub(super) fn changes(&self) -> PolicyChanges {
        self.updates.clone()
    }

    pub(super) fn state(&self) -> Arc<Mutex<Snapshot>> {
        Arc::clone(&self.state)
    }
}

impl Drop for LivePolicy {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn parse(bytes: &[u8]) -> Option<Snapshot> {
    let state: FileState = serde_json::from_slice(bytes).ok()?;
    let deadline = Instant::now().checked_add(Duration::from_millis(state.valid_for_ms))?;
    Some(Snapshot {
        customer: state.customer,
        allowed: state.allowed,
        deadline,
    })
}
