#!/usr/bin/env python3
"""Install Linux CI build dependencies without the runner's Azure mirror chain.

APT still validates the official Ubuntu repository signatures. Preserve suites,
components and third-party repositories; bound both network and process waits.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path


PACKAGES = (
    "build-essential", "cmake", "clang", "perl", "nasm", "ninja-build",
    "pkg-config", "libssl-dev",
)
APT_OPTIONS = (
    "-o", "Acquire::Retries=3",
    "-o", "Acquire::http::Timeout=30",
    "-o", "Acquire::https::Timeout=30",
    "-o", "Acquire::Languages=none",
    "-o", "DPkg::Lock::Timeout=60",
)


def direct_ubuntu_sources(text: str, architecture: str) -> str:
    ports = architecture not in {"amd64", "i386"}
    archive = "https://ports.ubuntu.com/ubuntu-ports" if ports else "https://archive.ubuntu.com/ubuntu"
    security = archive if ports else "https://security.ubuntu.com/ubuntu"
    text = text.replace("mirror+file:/etc/apt/apt-security-mirrors.txt", security)
    text = text.replace("mirror+file:/etc/apt/apt-mirrors.txt", archive)
    return re.sub(r"https?://azure\.archive\.ubuntu\.com/ubuntu(?=[\s/]|$)", archive, text)


def selftest() -> None:
    legacy = "deb http://azure.archive.ubuntu.com/ubuntu jammy main universe\n"
    expected = "deb https://archive.ubuntu.com/ubuntu jammy main universe\n"
    assert direct_ubuntu_sources(legacy, "amd64") == expected
    deb822 = (
        "Types: deb\nURIs: mirror+file:/etc/apt/apt-mirrors.txt\n"
        "Suites: noble noble-updates\nComponents: main universe\n"
        "Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\n\n"
        "Types: deb\nURIs: mirror+file:/etc/apt/apt-security-mirrors.txt\n"
        "Suites: noble-security\nComponents: main universe\n"
    )
    rewritten = direct_ubuntu_sources(deb822, "amd64")
    assert "URIs: https://archive.ubuntu.com/ubuntu\n" in rewritten
    assert "URIs: https://security.ubuntu.com/ubuntu\n" in rewritten
    assert "Suites: noble noble-updates\nComponents: main universe\n" in rewritten
    assert "Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg" in rewritten
    assert direct_ubuntu_sources(rewritten, "amd64") == rewritten
    arm = direct_ubuntu_sources(deb822, "arm64")
    assert arm.count("URIs: https://ports.ubuntu.com/ubuntu-ports\n") == 2
    unrelated = "deb https://packages.microsoft.com/ubuntu/24.04/prod noble main\n"
    assert direct_ubuntu_sources(unrelated, "amd64") == unrelated
    print("CI native dependency source regressions passed")


def install() -> None:
    architecture = subprocess.check_output(["dpkg", "--print-architecture"], text=True).strip()
    files = [Path("/etc/apt/sources.list")]
    directory = Path("/etc/apt/sources.list.d")
    files.extend(sorted(directory.glob("*.list")))
    files.extend(sorted(directory.glob("*.sources")))
    for source in files:
        if not source.is_file():
            continue
        before = source.read_text(encoding="utf-8")
        after = direct_ubuntu_sources(before, architecture)
        if after != before:
            source.write_text(after, encoding="utf-8")
            print(f"Using direct Ubuntu mirrors in {source}", flush=True)
    environment = {**os.environ, "DEBIAN_FRONTEND": "noninteractive"}
    # GNU timeout controls the complete APT process group, including download
    # workers, so a stalled child cannot consume the job's full build budget.
    subprocess.run([
        "timeout", "--kill-after=15s", "5m", "apt-get", *APT_OPTIONS,
        "-o", "APT::Update::Error-Mode=any", "update",
    ], env=environment, check=True)
    subprocess.run([
        "timeout", "--kill-after=15s", "8m", "apt-get", *APT_OPTIONS,
        "install", "-y", "--no-install-recommends", *PACKAGES,
    ], env=environment, check=True)


if __name__ == "__main__":
    if sys.argv[1:] == ["--selftest"]:
        selftest()
    elif sys.argv[1:]:
        raise SystemExit("usage: install-ci-native-deps.py [--selftest]")
    else:
        install()
