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

use super::*;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

#[path = "linux/queue_memory.rs"]
mod queue_memory;
#[path = "linux/scope.rs"]
mod scope;
#[path = "linux/updates.rs"]
mod updates;

/// Each real Runner receives its own dedicated scope, including on failure.
struct Scope {
    path: PathBuf,
}

impl Scope {
    fn new() -> io::Result<Self> {
        let root = std::env::var_os("TENON_TEST_CGROUP_ROOT")
            .ok_or_else(|| io::Error::other("Run the delegated Linux resource test harness"))?;
        let name = tempfile::tempdir()?;
        let path = PathBuf::from(root).join(
            name.path()
                .file_name()
                .ok_or_else(|| io::Error::other("Temporary scope name is missing"))?,
        );
        fs::create_dir(&path)?;
        let scope = Self { path };
        rustix::fs::setxattr(
            &scope.path,
            "user.delegate",
            b"1",
            rustix::fs::XattrFlags::empty(),
        )?;
        Ok(scope)
    }

    fn spawn(&self, config: &Path) -> io::Result<TestRunner> {
        let executable = std::env::var_os("TENON_TEST_RUNNER_BINARY")
            .map_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_tenon")), PathBuf::from);
        let mut command = Command::new("/bin/sh");
        command
            .args([
                "-ec",
                "printf 0 > \"$1/cgroup.procs\"; shift; exec \"$@\"",
                "tenon-resource-test",
            ])
            .arg(if self.path.join("tenon.runner").exists() {
                self.path.join("tenon.runner")
            } else {
                self.path.clone()
            })
            .arg(executable)
            .arg("--config")
            .arg(config);
        TestRunner::spawn_command(command)
    }

    fn group(&self) -> io::Result<PathBuf> {
        let mut groups = self.groups()?;
        if groups.len() != 1 {
            return Err(io::Error::other(format!(
                "Expected one resource group, got {}",
                groups.len()
            )));
        }
        Ok(groups.remove(0))
    }

    fn groups(&self) -> io::Result<Vec<PathBuf>> {
        let entries = match fs::read_dir(self.path.join("tenon.pipelines")) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut groups = Vec::new();
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                groups.push(entry.path());
            }
        }
        Ok(groups)
    }
}

impl Drop for Scope {
    fn drop(&mut self) {
        let _ = fs::write(self.path.join("cgroup.kill"), "1");
        let _ = wait_until(|| {
            Ok(fs::read_to_string(self.path.join("cgroup.events"))?
                .lines()
                .any(|line| line == "populated 0")
                .then_some(()))
        });
        fn remove(path: &Path) -> io::Result<()> {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    remove(&entry.path())?;
                }
            }
            fs::remove_dir(path)
        }
        let _ = remove(&self.path);
    }
}

fn membership(pid: u32) -> io::Result<String> {
    fs::read_to_string(format!("/proc/{pid}/cgroup"))?
        .lines()
        .find_map(|line| line.strip_prefix("0::").map(str::to_owned))
        .ok_or_else(|| io::Error::other("Missing process cgroup membership"))
}

fn usage(path: &Path, field: &str) -> io::Result<u64> {
    fs::read_to_string(path)?
        .lines()
        .find_map(|line| {
            line.split_once(' ')
                .filter(|(key, _)| *key == field)
                .map(|(_, value)| value.parse::<u64>())
        })
        .ok_or_else(|| io::Error::other(format!("Missing counter {field}")))?
        .map_err(io::Error::other)
}

fn kill(pid: u32, signal: rustix::process::Signal) -> io::Result<()> {
    let pid = rustix::process::Pid::from_raw(i32::try_from(pid).map_err(io::Error::other)?)
        .ok_or_else(|| io::Error::other("Invalid PID"))?;
    rustix::process::kill_process(pid, signal).map_err(io::Error::from)
}

fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn install_load(state: &Path, mode: &str, markers: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    install_program(state, PluginInterface::SourceAndSink)?;
    let directory = state.join("plugins/programs/com.example.gateway/1.0.0");
    let mut manifest: Value = serde_json::from_slice(&fs::read(directory.join("manifest.json"))?)?;
    let control = plugin_fixture::controlled_program_command(PluginInterface::SourceAndSink)?
        .iter()
        .map(|arg| quote(arg))
        .collect::<Vec<_>>()
        .join(" ");
    let executable = std::env::current_exe()?;
    let child_command = serde_json::to_string(&[
        executable.to_string_lossy().as_ref(),
        "--exact",
        "linux::resource_load_child",
        "--ignored",
        "--nocapture",
    ])?;
    let script = format!(
        "#!/bin/sh\nexport TENON_TEST_RESOURCE_MODE={} TENON_TEST_RESOURCE_MARKERS={} TENON_TEST_PLUGIN_CHILD_COMMAND={}\nexec {control} \"$@\"\n",
        quote(mode),
        quote(&markers.to_string_lossy()),
        quote(&child_command)
    );
    let script_path = directory.join("resource-plugin.sh");
    fs::write(&script_path, script)?;
    fs::set_permissions(&script_path, fs::Permissions::from_mode(0o500))?;
    manifest["command"] = json!(["/bin/sh", "./resource-plugin.sh"]);
    // Installation is a fixture setup before the Runner establishes its Store.
    fs::set_permissions(
        directory.join("manifest.json"),
        fs::Permissions::from_mode(0o600),
    )?;
    fs::write(
        directory.join("manifest.json"),
        serde_json::to_vec(&manifest)?,
    )?;
    fs::set_permissions(
        directory.join("manifest.json"),
        fs::Permissions::from_mode(0o500),
    )?;
    Ok(())
}

#[test]
#[ignore = "child process entry point invoked by the delegated resource harness"]
fn resource_load_child() -> io::Result<()> {
    let Ok(mode) = std::env::var("TENON_TEST_RESOURCE_MODE") else {
        return Ok(());
    };
    let markers = PathBuf::from(
        std::env::var_os("TENON_TEST_RESOURCE_MARKERS")
            .ok_or_else(|| io::Error::other("Load marker directory is missing"))?,
    );
    fs::write(markers.join(std::process::id().to_string()), "started")?;
    if mode == "memory" {
        while !markers.join("allocate").exists() {
            std::thread::sleep(Duration::from_millis(10));
        }
        let memory = vec![0x5a_u8; 48 * 1024 * 1024];
        loop {
            std::hint::black_box(&memory);
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    loop {
        std::hint::spin_loop();
    }
}

#[test]
#[ignore = "Requires the delegated Linux resource test harness"]
fn cpu_limit_covers_plugins_and_descendants_and_replacement_waits_for_cleanup() -> io::Result<()> {
    let scope = Scope::new()?;
    let state = tempfile::tempdir()?;
    let markers = tempfile::tempdir()?;
    install_load(state.path(), "cpu", markers.path())?;
    let address = available_address()?;
    let config = write_config(state.path(), address)?;
    let mut runner = scope.spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    let mut desired = document(json!({"cpu":0.5,"memoryBytes":536870912}));
    let first = put(address, &desired, None)?;
    wait_applied(address, &first)?;
    let first_pid = pipeline_pid(state.path())?;
    let group = scope.group()?;
    assert_eq!(
        fs::read_to_string(group.join("cpu.max"))?.trim(),
        "50000 100000"
    );
    assert_eq!(
        fs::read_to_string(group.join("memory.max"))?.trim(),
        "536870912"
    );
    assert_eq!(details(address)?["resourceLimits"]["state"], "enforced");
    wait_until(|| Ok((fs::read_dir(markers.path())?.count() == 3).then_some(())))?;
    let mut descendants = Vec::new();
    for entry in fs::read_dir(markers.path())? {
        let pid: u32 = entry?
            .file_name()
            .to_string_lossy()
            .parse()
            .map_err(io::Error::other)?;
        assert_eq!(membership(pid)?, membership(first_pid)?);
        descendants.push(pid);
    }
    let before = usage(&group.join("cpu.stat"), "usage_usec")?;
    let start = Instant::now();
    std::thread::sleep(Duration::from_secs(2));
    let cores = (usage(&group.join("cpu.stat"), "usage_usec")? - before) as f64
        / start.elapsed().as_micros() as f64;
    eprintln!("Measured aggregate CPU use: {cores:.3} cores with a 0.5 core ceiling");
    assert!(
        cores > 0.2 && cores < 0.7,
        "CPU ceiling measured {cores} cores"
    );
    assert!(usage(&group.join("cpu.stat"), "nr_throttled")? > 0);
    desired["resourceLimits"] = serde_json::from_str("{\"cpu\":5e-1,\"memoryBytes\":536870912.0}")?;
    let equivalent = put(address, &desired, Some(&first))?;
    wait_applied(address, &equivalent)?;
    assert_eq!(pipeline_pid(state.path())?, first_pid);
    desired["resourceLimits"]["cpu"] = json!(1.0);
    let replacement = put(address, &desired, Some(&equivalent))?;
    wait_applied(address, &replacement)?;
    assert_ne!(pipeline_pid(state.path())?, first_pid);
    assert!(!group.exists());
    for pid in descendants {
        assert!(
            !PathBuf::from(format!("/proc/{pid}")).exists(),
            "Normally stopped Plugin must reap its helper {pid}"
        );
    }
    runner.terminate()?;
    assert!(scope.groups()?.is_empty());
    Ok(())
}

#[test]
#[ignore = "Requires the delegated Linux resource test harness"]
fn aggregate_memory_oom_ends_the_whole_pipeline_and_runner_recovers() -> io::Result<()> {
    let scope = Scope::new()?;
    let state = tempfile::tempdir()?;
    let markers = tempfile::tempdir()?;
    install_load(state.path(), "memory", markers.path())?;
    let address = available_address()?;
    let config = write_config(state.path(), address)?;
    let mut configuration: Value = serde_json::from_slice(&fs::read(&config)?)?;
    configuration["pipeline"]["retryBackoff"] =
        json!({"initialDelayMs":5000,"maximumDelayMs":5000});
    fs::write(&config, serde_json::to_vec(&configuration)?)?;
    let mut runner = scope.spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    let first = put(address, &document(json!({"memoryBytes":100663296})), None)?;
    wait_applied(address, &first)?;
    let old_pid = pipeline_pid(state.path())?;
    let old_group = scope.group()?;
    assert_eq!(
        fs::read_to_string(old_group.join("memory.oom.group"))?.trim(),
        "1"
    );
    wait_until(|| Ok((fs::read_dir(markers.path())?.count() == 3).then_some(())))?;
    let events = scope.path.join("tenon.pipelines/memory.events");
    let killed_before = usage(&events, "oom_kill")?;
    fs::write(markers.path().join("allocate"), "go")?;
    wait_until(|| Ok((details(address)?["state"] == "restart-backoff").then_some(())))?;
    let failed = details(address)?;
    assert!(failed.get("appliedDocumentEtag").is_none());
    assert!(failed["lastError"].get("documentEtag").is_none());
    wait_until(|| Ok((!old_group.exists()).then_some(())))?;
    assert!(
        usage(&events, "oom_kill")? > killed_before,
        "The kernel must confirm OOM, not just a generic process failure"
    );
    fs::remove_file(markers.path().join("allocate"))?;
    wait_applied(address, &first)?;
    assert_ne!(pipeline_pid(state.path())?, old_pid);
    runner.terminate()?;
    Ok(())
}

#[test]
#[ignore = "Requires the delegated Linux resource test harness"]
fn crashed_runner_recovers_frozen_descendants_before_removing_runtime_files() -> io::Result<()> {
    let scope = Scope::new()?;
    let state = tempfile::tempdir()?;
    install_program(state.path(), PluginInterface::SourceAndSink)?;
    let address = available_address()?;
    let config = write_config(state.path(), address)?;
    let mut runner = scope.spawn(&config)?;
    wait_for_http(&mut runner, address)?;
    let first = put(address, &document(json!({"cpu":1})), None)?;
    wait_applied(address, &first)?;
    let old_group = scope.group()?;
    let old_pid = pipeline_pid(state.path())?;
    let old_files = file_tree::named_files(&state.path().join("pipelines"), "parent.pid")?;
    fs::write(old_group.join("cgroup.freeze"), "1")?;
    wait_until(|| {
        Ok(fs::read_to_string(old_group.join("cgroup.events"))?
            .contains("frozen 1")
            .then_some(()))
    })?;
    let runner_pid = fs::read_to_string(scope.path.join("tenon.runner/cgroup.procs"))?
        .trim()
        .parse()
        .map_err(io::Error::other)?;
    kill(runner_pid, rustix::process::Signal::KILL)?;
    // Do not drain inherited stderr while frozen descendants still hold it.
    drop(runner);
    assert!(old_group.exists());
    assert!(old_files.iter().all(|path| path.exists()));
    let mut replacement = scope.spawn(&config)?;
    wait_for_http(&mut replacement, address)?;
    wait_applied(address, &first)?;
    assert!(!old_group.exists());
    assert!(old_files.iter().all(|path| !path.exists()));
    assert_ne!(pipeline_pid(state.path())?, old_pid);
    replacement.terminate()?;
    Ok(())
}
