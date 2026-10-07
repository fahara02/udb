#!/usr/bin/env python3
"""Run serial live tests, dumping native thread stacks if progress stops."""

import argparse
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import time


def dump_stacks(root_pid: int) -> None:
    process_rows = subprocess.run(
        ["ps", "-eo", "pid=,ppid=,comm="], capture_output=True, text=True, check=True
    ).stdout.splitlines()
    processes = {}
    for row in process_rows:
        pid, parent, name = row.strip().split(maxsplit=2)
        processes[int(pid)] = (int(parent), name)
    descendants = {root_pid}
    while True:
        children = {pid for pid, (parent, _) in processes.items() if parent in descendants}
        if children <= descendants:
            break
        descendants.update(children)
    for pid in sorted(descendants):
        name = processes.get(pid, (0, "exited"))[1]
        print(f"live-test watchdog: pid={pid} name={name}", flush=True)
        for thread in sorted(Path(f"/proc/{pid}/task").glob("*/wchan")):
            try:
                print(f"  thread={thread.parent.name} wait={thread.read_text().strip()}", flush=True)
            except OSError:
                pass
        if name.startswith("udb-") or name.startswith("udb_"):
            try:
                subprocess.run(
                    ["sudo", "-n", "gdb", "--batch", "-ex", "set pagination off",
                     "-ex", "thread apply all bt 30", "-p", str(pid)],
                    timeout=45, check=False,
                )
            except (OSError, subprocess.TimeoutExpired) as error:
                print(f"live-test watchdog: stack capture failed: {error}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stall-seconds", type=float, default=300)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command[:1] == ["--"]:
        command = command[1:]
    if not command or args.stall_seconds <= 0:
        parser.error("a command and a positive stall budget are required")
    child = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True
    )
    last_progress = time.monotonic()
    try:
        while True:
            ready, _, _ = select.select([child.stdout], [], [], 1)
            if ready:
                chunk = os.read(child.stdout.fileno(), 65536)
                if not chunk:
                    return child.wait()
                sys.stdout.buffer.write(chunk)
                sys.stdout.buffer.flush()
                last_progress = time.monotonic()
            if time.monotonic() - last_progress >= args.stall_seconds:
                print("\nlive-test watchdog: no output for "
                      f"{args.stall_seconds:g}s; capturing the stalled test", flush=True)
                dump_stacks(child.pid)
                return 124
    finally:
        if child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()


if __name__ == "__main__":
    sys.exit(main())
