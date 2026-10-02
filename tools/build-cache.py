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

"""Own validation processes and maintain checkout-local generated build files."""

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile

LIMIT_BYTES = 30 * 1024**3
OWNER_VARIABLE = "TENON_BUILD_CACHE_OWNER"


class CacheBusy(RuntimeError):
    pass


def build_directories(root, environment=None):
    environment = os.environ if environment is None else environment
    directories = [root / "target"]
    for current, children, files in os.walk(root, followlinks=False):
        children[:] = [name for name in children if name not in
                       {".git", "target", ".archives", ".local", "node_modules"}]
        if "Cargo.toml" in files or "pom.xml" in files:
            directories.append(Path(current) / "target")
    for name in ("CARGO_TARGET_DIR", "CARGO_BUILD_BUILD_DIR"):
        if environment.get(name):
            directories.append(root / environment[name])
    for manifest in ("Cargo.toml", "sdk/rust/Cargo.toml"):
        if not (root / manifest).exists():
            continue
        metadata = json.loads(subprocess.check_output(
            ["cargo", "metadata", "--no-deps", "--locked", "--format-version=1",
             "--manifest-path", str(root / manifest)], cwd=root, env=environment, text=True))
        directories.append(Path(metadata["target_directory"]))
        if metadata.get("build_directory"):
            directories.append(Path(metadata["build_directory"]))
    return sorted(set(directories))


def local_path(root, path):
    resolved = path.resolve()
    if resolved == root or not resolved.is_relative_to(root):
        raise ValueError(f"Refusing to maintain an external or root build directory: {path}")
    inside_checkout = False
    for part in reversed([path, *path.parents]):
        if not inside_checkout:
            inside_checkout = part.resolve() == root
        elif part.is_symlink():
            raise ValueError(f"Refusing to maintain an external or linked build directory: {path}")
    if not inside_checkout:
        raise ValueError(f"Refusing to maintain an external or linked build directory: {path}")
    return resolved


def generated_paths(root, directories):
    paths = []
    for directory in directories:
        local_path(root, directory)
        if (directory.parent / "pom.xml").is_file() and directory.name == "target":
            paths.append(directory)
            continue
        for name in ("debug", "release", "doc", "miri", "package", "tmp", "tenon", "experiments"):
            paths.append(directory / name)
        if directory.exists():
            for child in directory.iterdir():
                if child.name.endswith("maven-repository"):
                    paths.append(child)
                elif child.is_dir() and not child.is_symlink():
                    for profile in ("debug", "release"):
                        if (child / profile).is_dir():
                            paths.append(child / profile)
        paths.extend(directory / "runner-java-archetype" / name
                     for name in ("packages", "maven-repository"))
    return [local_path(root, path) for path in sorted(set(paths)) if path.exists()]


def size_bytes(directories):
    existing = [path for path in directories if path.exists()]
    roots = [path for path in existing if not any(path != parent and path.is_relative_to(parent)
                                                 for parent in existing)]
    return sum(int(subprocess.check_output(["du", "-sk", str(path)]).split()[0]) * 1024
               for path in roots)


def require_idle(directories):
    for directory in directories:
        if directory.exists():
            result = subprocess.run(["lsof", "+D", str(directory), "-Fn"],
                                    text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if result.returncode != 1 or result.stdout or result.stderr:
                raise CacheBusy(f"Build files are in use or cannot be inspected: {directory}")


def remove_generated(root, paths):
    paths = [path for path in paths if path.exists()]
    paths = [path for path in paths if not any(path != parent and path.is_relative_to(parent)
                                              for parent in paths)]
    for path in paths:
        local_path(root, path)
    if (root / ".git").exists():
        relative = [str(path.relative_to(root)) for path in paths]
        if relative and subprocess.check_output(["git", "-C", str(root), "ls-files", "-z", "--", *relative]):
            raise ValueError("Refusing to remove tracked files from a build directory")
    for path in paths:
        shutil.rmtree(path)


@contextmanager
def session(root):
    root = root.resolve()
    common = subprocess.run(["git", "-C", str(root), "rev-parse", "--path-format=absolute", "--git-common-dir"],
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    identity = common.stdout.strip() if common.returncode == 0 else str(root)
    locks = Path(tempfile.gettempdir()) / "tenon-build-locks"
    locks.mkdir(exist_ok=True)
    lock = locks / (hashlib.sha256(identity.encode()).hexdigest() + ".lock")
    with lock.open("a") as owned:
        try:
            fcntl.flock(owned, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise CacheBusy("Another validation or cache maintenance task owns this repository") from error
        yield root


def maintain(root, finish=False, environment=None):
    root = root.resolve()
    directories = sorted(set(local_path(root, directory) for directory in build_directories(root, environment)))
    before = size_bytes(directories)
    paths = generated_paths(root, directories)
    if finish:
        require_idle(paths)
        remove_generated(root, paths)
    else:
        # Reap interrupted experiments even when the ordinary cache is below budget.
        experiments = root / "target/experiments"
        if experiments.exists():
            abandoned = [path for path in experiments.iterdir()
                         if path.name.startswith("tenon-probe-") and path.is_dir()]
            require_idle(abandoned)
            remove_generated(root, abandoned)
        if before > LIMIT_BYTES:
            development = [path for path in paths if path.name in {"debug", "doc"}]
            for directory in directories:
                if directory != root / "target":
                    development.extend(directory / name for name in ("debug", "doc"))
            require_idle([path for path in development if path.exists()])
            remove_generated(root, sorted(set(development)))
    after = size_bytes(directories)
    print(f"Build cache: {before / 1024**3:.2f} -> {after / 1024**3:.2f} GiB (maintenance line: 30 GiB)", flush=True)
    if after > LIMIT_BYTES:
        print("Remaining build outputs exceed the maintenance line; use finish after they are no longer needed.", flush=True)


def run_owned(root, command):
    with session(root) as root:
        maintain(root)
        environment = dict(os.environ, **{OWNER_VARIABLE: str(root)})
        def interrupted(signum, frame):
            raise KeyboardInterrupt
        previous = signal.signal(signal.SIGTERM, interrupted)
        try:
            with subprocess.Popen(command, cwd=root, env=environment, start_new_session=True) as process:
                try:
                    status = process.wait()
                except BaseException:
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        pass
                    raise
                finally:
                    # The command may exit before its descendants. End the
                    # whole owned group before maintenance or lock release.
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
            return status
        finally:
            signal.signal(signal.SIGTERM, previous)
            maintain(root)



@contextmanager
def experiment(root):
    with session(root) as root:
        environment = dict(os.environ)
        for name in ("CARGO_TARGET_DIR", "CARGO_BUILD_BUILD_DIR"):
            environment.pop(name, None)
        directories = [local_path(root, directory) for directory in build_directories(root, environment)]
        require_idle(directories)
        maintain(root, environment=environment)
        scratch = local_path(root, root / "target/experiments")
        scratch.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix="tenon-probe-", dir=scratch))
        try:
            yield dict(os.environ, CARGO_TARGET_DIR=str(Path(temporary) / "target"),
                       CARGO_BUILD_BUILD_DIR=str(Path(temporary) / "build"))
        finally:
            require_idle([temporary])
            remove_generated(root, [temporary])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("run", "maintain", "finish"))
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    try:
        if args.action == "run":
            if not command:
                parser.error("run requires a command after --")
            return run_owned(args.root, command)
        if command:
            parser.error("maintain and finish do not accept a command")
        with session(args.root) as root:
            maintain(root, finish=args.action == "finish")
        return 0
    except (CacheBusy, ValueError, OSError, subprocess.CalledProcessError) as error:
        print(f"Build cache maintenance refused: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
