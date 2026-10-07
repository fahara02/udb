#!/usr/bin/env python3
"""Install Linux CI build dependencies without the runner's Azure mirror chain.

APT still validates the official Ubuntu repository signatures. Preserve suites,
components and third-party repositories; bound both network and process waits.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
from pathlib import Path


PACKAGES = (
    "build-essential", "cmake", "clang", "perl", "nasm", "ninja-build",
    "pkg-config", "libssl-dev",
)
PACKAGE_SETS = {
    "native": PACKAGES,
    "aarch64-cross": ("gcc-aarch64-linux-gnu", "libc6-dev-arm64-cross"),
    "protobuf": ("protobuf-compiler",),
    "debugger": ("gdb",),
    "ffmpeg": ("ffmpeg",),
    "postgres-client": ("postgresql-client",),
}
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
    assert PACKAGE_SETS["native"] == PACKAGES
    assert PACKAGE_SETS["aarch64-cross"] == ("gcc-aarch64-linux-gnu", "libc6-dev-arm64-cross")
    assert all(packages and all(not package.startswith("-") for package in packages)
               for packages in PACKAGE_SETS.values())
    print("CI native dependency source regressions passed")


def install(packages: tuple[str, ...]) -> None:
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
        "install", "-y", "--no-install-recommends", *packages,
    ], env=environment, check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true")
    parser.add_argument("--packages", choices=PACKAGE_SETS, default="native")
    options = parser.parse_args()
    if options.selftest:
        selftest()
    else:
        install(PACKAGE_SETS[options.packages])
