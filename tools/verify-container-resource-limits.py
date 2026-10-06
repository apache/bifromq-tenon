#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Verify Docker bootstrap with real default Runner and Plugin processes.

Pass native Linux Runner and runner_resource_limits test executables. The latter
exports the repository's existing protocol peer; production needs no test hooks.
All created containers/images are disposable. No host cgroup bind is used.
"""

import argparse
import http.client
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]


def docker(*arguments, check=True):
    result = subprocess.run(["docker", *map(str, arguments)], text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    if check and result.returncode:
        raise RuntimeError(f"docker {' '.join(map(str, arguments))}: {result.stderr}")
    return result.stdout.strip()


def wait_for(probe):
    deadline = time.monotonic() + 40
    last = None
    while time.monotonic() < deadline:
        try:
            last = probe()
            if last:
                return last
        except (OSError, http.client.HTTPException) as error:
            last = error
        time.sleep(0.1)
    raise AssertionError(f"Condition did not become true: {last}")


def verify(image, fixtures, *, init, grant):
    name = f"tenon-cgroup-check-{uuid.uuid4().hex[:10]}"
    observer = name + "-observer"
    state_volume = name + "-state"
    arguments = ["run", "-d", "--name", name, "--cgroupns=private",
                 "--mount", f"type=volume,src={state_volume},dst=/var/lib/tenon",
                 "--tmpfs", "/tmp:rw,exec,size=1g",
                 "-p", "127.0.0.1::8080", "--mount",
                 f"type=bind,src={fixtures},dst=/test,readonly"]
    if init:
        arguments.append("--init")
    if grant:
        arguments += ["--cap-add", "SYS_ADMIN"]
    try:
        docker(*arguments, image, "--config", "/test/runner.json")
        port = int(docker("port", name, "8080/tcp").rsplit(":", 1)[1])

        def request(method, path, body=b"", headers=None):
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
            try:
                connection.request(method, path, body, headers or {})
                response = connection.getresponse()
                return response.status, dict(response.getheaders()), response.read()
            finally:
                connection.close()

        wait_for(lambda: request("GET", "/documents")[0] == 200)
        status, _, body = request("POST", "/plugins", (fixtures / "plugin.tar.gz").read_bytes(),
                                  {"Content-Type": "application/octet-stream"})
        assert status == 201, (status, body)
        document = json.loads((fixtures / "document.json").read_text())

        def put(etag=None):
            condition = {"If-Match": etag} if etag else {"If-None-Match": "*"}
            status, headers, body = request("PUT", "/documents/limited", json.dumps(document),
                                            {"Content-Type": "application/jsonc", **condition})
            assert status == (204 if etag else 201), (status, body)
            return headers["etag"]

        def details():
            status, _, body = request("GET", "/pipelines/limited")
            assert status == 200, (status, body)
            return json.loads(body)

        etag = put()
        if not grant:
            wait_for(lambda: details().get("lastError", {}).get("code") == "resource_limits_apply_failed")
            assert "appliedDocumentEtag" not in details(), details()
            document["resourceLimits"] = {}
            etag = put(etag)
        wait_for(lambda: details().get("appliedDocumentEtag") == etag
                 and details().get("pluginInstances", [{}])[0].get("state") == "running")
        if grant:
            for generation in range(2):
                assert details()["resourceLimits"]["state"] == "enforced", details()
                # Inspect through a separate container: docker exec on the Runner can
                # add a process to its now-empty scope. The observer keeps its own
                # cgroup and only reads the target mount through /proc/1/root.
                docker("run", "-d", "--name", observer, f"--pid=container:{name}",
                       "--cap-add", "SYS_PTRACE", "--entrypoint", "/bin/sleep", image, "infinity")
                root = "/proc/1/root/sys/fs/cgroup"

                def read(path):
                    return docker("exec", observer, "cat", path)

                assert read(root + "/cgroup.procs") == ""
                supervisors = read(root + "/tenon.runner/cgroup.procs").splitlines()
                assert "1" in supervisors and len(supervisors) == (2 if init else 1), supervisors
                groups = docker("exec", observer, "sh", "-ec",
                                f'for group in {root}/tenon.pipelines/*; do test -d "$group" && echo "${{group##*/}}"; done').splitlines()
                assert len(groups) == 1, groups
                group = root + "/tenon.pipelines/" + groups[0]
                assert read(group + "/cpu.max") == "50000 100000"
                assert read(group + "/memory.max") == "536870912"
                assert read(group + "/memory.oom.group") == "1"
                members = read(group + "/cgroup.procs").splitlines()
                assert len(members) >= 2 and not set(members).intersection(supervisors), members
                commands = [docker("exec", observer, "sh", "-ec",
                                   f"tr '\\000' ' ' < /proc/{int(pid)}/cmdline") for pid in members]
                assert any("/tenon pipeline --control-socket " in command for command in commands), commands
                assert any("/test/resource-tests" in command for command in commands), commands
                # Check the target's mount attributes from its own mountinfo.
                mountinfo = read("/proc/1/mountinfo")
                mount = next(line.split()[5] for line in mountinfo.splitlines()
                             if line.split()[4] == "/sys/fs/cgroup")
                assert {"rw", "nosuid", "nodev", "noexec"} <= set(mount.split(",")), mount
                if generation == 0:
                    docker("rm", "-f", observer)
                    docker("restart", "--time", "40", name)
                    port = int(docker("port", name, "8080/tcp").rsplit(":", 1)[1])
                    wait_for(lambda: request("GET", "/documents")[0] == 200)
                    wait_for(lambda: details().get("appliedDocumentEtag") == etag
                             and details().get("pluginInstances", [{}])[0].get("state") == "running")
                    continue
                status, _, body = request("DELETE", "/documents/limited", headers={"If-Match": etag})
                assert status == 204, (status, body)
                wait_for(lambda: docker("exec", observer, "sh", "-ec",
                                        f'test ! -d "{group}" && echo removed', check=False) == "removed")
                assert read(root + "/tenon.runner/cgroup.procs").splitlines() == supervisors
        else:
            assert "runner.resource_limits_unavailable" in subprocess.check_output(
                ["docker", "logs", name], stderr=subprocess.STDOUT, text=True)
        docker("stop", "--time", "40", name)
        assert docker("inspect", "--format", "{{.State.ExitCode}}", name) == "0"
        print(f"PASS: init={init}, writable-delegation-grant={grant}, persistent-restart={grant}; real Plugin and shutdown", flush=True)
    except BaseException:
        subprocess.run(["docker", "logs", name], check=False)
        raise
    finally:
        docker("rm", "-f", observer, name, check=False)
        docker("volume", "rm", state_volume, check=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runner", type=Path)
    parser.add_argument("test_binary", type=Path)
    args = parser.parse_args()
    image = "tenon-cgroup-check:" + uuid.uuid4().hex[:10]
    with tempfile.TemporaryDirectory(prefix="tenon-container-check-") as temporary:
        directory = Path(temporary)
        distribution = directory / "distribution"
        (distribution / "bin").mkdir(parents=True)
        shutil.copy2(args.runner, distribution / "bin/tenon")
        # This local verification image is never published. Release images use
        # the complete distribution, including generated dependency licenses.
        for name in ["LICENSE", "NOTICE", "DISCLAIMER"]:
            shutil.copy2(ROOT / name, distribution / name)
        fixtures = directory / "fixtures"
        fixtures.mkdir()
        shutil.copy2(args.test_binary, fixtures / "resource-tests")
        shutil.copy2(ROOT / "deploy/container/runner.json", fixtures / "runner.json")
        try:
            docker("build", "-q", "-t", image, "-f", ROOT / "deploy/container/Dockerfile", distribution)
            docker("run", "--rm", "--mount", f"type=bind,src={fixtures},dst=/test",
                   "--env", "TENON_TEST_DEPLOYMENT_DIRECTORY=/test", "--entrypoint", "/test/resource-tests",
                   image, "--ignored", "--exact", "linux::scope::export_deployment_fixture")
            assert (fixtures / "plugin.tar.gz").is_file(), "Fixture export did not execute"
            for init, grant in [(False, True), (True, True), (True, False)]:
                verify(image, fixtures, init=init, grant=grant)
        finally:
            docker("image", "rm", image, check=False)


if __name__ == "__main__":
    main()
