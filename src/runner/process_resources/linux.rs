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

//! cgroup v2 limits inside the scope claimed by this Runner at startup.
//! Pipeline owners retain the scope claim through confirmed kernel cleanup.

#![allow(unsafe_code)]

use super::{ResourceLimitsState, ResourcePreparationError};
use crate::tenon_document::VerifiedTenonDocument;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rustix::thread;
use std::fs::{self, File};
use std::io;
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd as _, FromRawFd as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod scope;
use scope::OwnedScope;
pub(in crate::runner) use scope::RunnerResources;
use std::time::Duration;
use tokio::process::Command;

pub(super) const APPLIED_STATE: ResourceLimitsState = ResourceLimitsState::Enforced;

/// Observes the inherited allowed CPUs without applying cgroup CPU-time quotas.
pub(in crate::runner) fn available_cpu_count() -> io::Result<NonZeroUsize> {
    let count = thread::sched_getaffinity(None)?.count();
    NonZeroUsize::new(count as usize)
        .ok_or_else(|| io::Error::other("Runner has no allowed logical CPUs"))
}

/// Owns one kernel resource group, independently of its leader's lifetime.
pub(in crate::runner) struct PipelineResourceGroup {
    path: PathBuf,
    _scope: Arc<OwnedScope>,
}

impl PipelineResourceGroup {
    pub(in crate::runner) fn prepare(
        resources: &RunnerResources,
        command: &mut Command,
        document: &VerifiedTenonDocument,
        launch_id: &[u8],
    ) -> Result<Option<Self>, ResourcePreparationError> {
        let Some(limits) = effective_limits(document)? else {
            return Ok(None);
        };
        let scope = resources.scope()?;
        let workload = scope.path.join("tenon.pipelines");
        enable_controllers(&scope.path, &limits)?;
        create_subtree(&workload)?;
        enable_controllers(&workload, &limits)?;
        let path = workload.join(format!("p-{}", URL_SAFE_NO_PAD.encode(launch_id)));
        fs::create_dir(&path)?;
        let prepared = (|| -> io::Result<()> {
            // Verify cleanup capability before allowing any process to enter.
            File::options().write(true).open(path.join("cgroup.kill"))?;
            if let Some(quota) = limits.cpu_quota {
                let requested = format!("{quota} 100000");
                fs::write(path.join("cpu.max"), &requested)?;
                if fs::read_to_string(path.join("cpu.max"))?.trim() != requested {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "CPU limit exceeds the kernel's exact bandwidth range",
                    ));
                }
            }
            if let Some(bytes) = limits.memory_bytes {
                fs::write(path.join("memory.max"), bytes.to_string())?;
                if fs::read_to_string(path.join("memory.max"))?.trim() == "max" {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Memory limit exceeds the kernel's finite accounting range",
                    ));
                }
                fs::write(path.join("memory.oom.group"), "1")?;
            }
            attach_before_exec(command, &path)
        })();
        if let Err(failure) = prepared {
            return Err(ResourcePreparationError {
                failure,
                group: Some(Self {
                    path,
                    _scope: scope,
                }),
            });
        }
        Ok(Some(Self {
            path,
            _scope: scope,
        }))
    }

    pub(in crate::runner) async fn cleanup(&self) -> io::Result<()> {
        kill_and_remove(&self.path).await
    }

    pub(in crate::runner) fn discard_empty(&self) -> io::Result<()> {
        fs::remove_dir(&self.path)
    }
}

impl Drop for PipelineResourceGroup {
    fn drop(&mut self) {
        let _ = fs::write(self.path.join("cgroup.kill"), "1");
    }
}

pub(in crate::runner) fn requires_replacement(
    current: &VerifiedTenonDocument,
    target: &VerifiedTenonDocument,
) -> bool {
    match (effective_limits(current), effective_limits(target)) {
        (Ok(current), Ok(target)) => current != target,
        // An unrepresentable target must reach the normal launch failure path.
        _ => true,
    }
}

#[derive(PartialEq, Eq)]
struct EffectiveLimits {
    cpu_quota: Option<u64>,
    memory_bytes: Option<u64>,
}

fn effective_limits(document: &VerifiedTenonDocument) -> io::Result<Option<EffectiveLimits>> {
    let Some(limits) = document
        .resource_limits()
        .filter(|limits| !limits.is_empty())
    else {
        return Ok(None);
    };
    let memory_bytes = limits
        .memory_bytes()
        .map(|bytes| {
            // SAFETY: sysconf reads a process-independent kernel constant and uses
            // no pointers or shared mutable memory.
            let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
            let page_size = u64::try_from(page_size)
                .ok()
                .filter(|size| *size > 0)
                .ok_or_else(|| io::Error::other("The kernel did not provide a valid page size"))?;
            bytes
                .checked_add(page_size - 1)
                .map(|rounded| rounded / page_size * page_size)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Memory limit cannot be represented at the host page size",
                    )
                })
        })
        .transpose()?;
    Ok(Some(EffectiveLimits {
        // Document verification bounds this exact multiplication before projection.
        cpu_quota: limits.cpu_hundredths().map(|cpu| cpu * 1000),
        memory_bytes,
    }))
}

fn unescape_mount_path(value: &str) -> io::Result<PathBuf> {
    let mut bytes = Vec::with_capacity(value.len());
    let mut input = value.as_bytes().iter().copied();
    while let Some(byte) = input.next() {
        if byte == b'\\' {
            let digits = [input.next(), input.next(), input.next()];
            let [
                Some(a @ b'0'..=b'3'),
                Some(b @ b'0'..=b'7'),
                Some(c @ b'0'..=b'7'),
            ] = digits
            else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid procfs mount path escape",
                ));
            };
            bytes.push((a - b'0') * 64 + (b - b'0') * 8 + c - b'0');
        } else {
            bytes.push(byte);
        }
    }
    use std::os::unix::ffi::OsStringExt as _;
    Ok(std::ffi::OsString::from_vec(bytes).into())
}

fn create_subtree(path: &Path) -> io::Result<()> {
    match fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

fn enable_controllers(path: &Path, limits: &EffectiveLimits) -> io::Result<()> {
    let controllers = match (limits.cpu_quota, limits.memory_bytes) {
        (Some(_), Some(_)) => "+cpu +memory",
        (Some(_), None) => "+cpu",
        (None, Some(_)) => "+memory",
        (None, None) => unreachable!("effective limits always contain a resource"),
    };
    fs::write(path.join("cgroup.subtree_control"), controllers).map_err(|error| {
        io::Error::new(error.kind(), format!(
            "Cannot enable {controllers} in {}: {error}; the parent must delegate the requested controllers and this group must contain no processes",
            path.display()
        ))
    })
}

fn attach_before_exec(command: &mut Command, path: &Path) -> io::Result<()> {
    let membership = File::options()
        .write(true)
        .open(path.join("cgroup.procs"))?;
    // SAFETY: fcntl duplicates a live owned descriptor; CLOEXEC prevents leaks
    // into Pipeline/Plugin code, and a descriptor above stderr survives stdio setup.
    let duplicate = unsafe { libc::fcntl(membership.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
    if duplicate < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful fcntl returned a fresh descriptor owned only here.
    let membership = unsafe { File::from_raw_fd(duplicate) };
    // SAFETY: this hook only invokes async-signal-safe write and reads errno.
    // The captured File lives through fork/exec; no allocator, lock, formatting,
    // logging, or destructor runs in the child hook. A failure prevents exec.
    unsafe {
        command.pre_exec(move || {
            let written = libc::write(membership.as_raw_fd(), b"0".as_ptr().cast(), 1);
            if written == 1 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        });
    }
    Ok(())
}

async fn kill_and_remove(path: &Path) -> io::Result<()> {
    match fs::write(path.join("cgroup.kill"), "1") {
        Err(error) if error.kind() == io::ErrorKind::NotFound && !path.try_exists()? => {
            return Ok(());
        }
        result => result?,
    }
    // Kernel teardown can outlive SIGKILL; a stuck task must leave its files for
    // recovery instead of allowing an unbounded restart or deleting live mappings.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let events = fs::read_to_string(path.join("cgroup.events"))?;
            if events.lines().any(|line| line == "populated 0") {
                return fs::remove_dir(path);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "Killed cgroup still contains live processes",
        )
    })?
}
