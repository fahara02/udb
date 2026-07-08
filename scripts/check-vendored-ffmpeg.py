#!/usr/bin/env python3
"""Verify or install the vendored ffmpeg binary used by AssetService.

The broker runtime searches:

    third_party/ffmpeg/bin/<platform>/ffmpeg(.exe)

where platform is linux, macos, or windows. This helper is intentionally small
and dependency-free so release jobs, local Windows shells, and container smokes
can all use the same packaging contract.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = REPO_ROOT / "third_party" / "ffmpeg"
MANIFEST_NAME = "vendored-ffmpeg.json"


def current_platform() -> str:
    name = platform.system().lower()
    if name == "windows":
        return "windows"
    if name == "darwin":
        return "macos"
    if name == "linux":
        return "linux"
    raise SystemExit(f"unsupported platform for vendored ffmpeg: {platform.system()}")


def binary_name(platform_name: str) -> str:
    return "ffmpeg.exe" if platform_name == "windows" else "ffmpeg"


def expected_path(root: Path, platform_name: str) -> Path:
    return root / "bin" / platform_name / binary_name(platform_name)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_executable(path: Path) -> None:
    if os.name == "nt":
        return
    mode = path.stat().st_mode
    if not mode & stat.S_IXUSR:
        path.chmod(mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def ffmpeg_version_line(path: Path) -> str:
    try:
        proc = subprocess.run(
            [str(path), "-hide_banner", "-version"],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=10,
        )
    except FileNotFoundError as exc:
        raise SystemExit(f"ffmpeg binary not found: {path}") from exc
    except subprocess.TimeoutExpired as exc:
        raise SystemExit(f"ffmpeg -version timed out: {path}") from exc
    if proc.returncode != 0:
        raise SystemExit(f"ffmpeg -version failed for {path}:\n{proc.stdout}")
    first = (proc.stdout or "").splitlines()[0].strip() if proc.stdout else ""
    if not first.lower().startswith("ffmpeg version"):
        raise SystemExit(f"{path} does not look like ffmpeg; first line: {first!r}")
    return first


def install_from(source: Path, target: Path) -> None:
    if not source.is_file():
        raise SystemExit(f"--install-from path is not a file: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    shutil.copy2(source, tmp)
    ensure_executable(tmp)
    tmp.replace(target)


def verify(path: Path) -> dict[str, object]:
    if not path.is_file():
        raise SystemExit(
            f"vendored ffmpeg missing at {path}\n"
            "Install it there or run this script with --install-from <ffmpeg-binary>."
        )
    ensure_executable(path)
    version = ffmpeg_version_line(path)
    return {
        "path": str(path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path),
        "size": path.stat().st_size,
        "sha256": sha256(path),
        "version": version,
    }


def write_manifest(root: Path, platform_name: str, data: dict[str, object]) -> Path:
    manifest_path = root / MANIFEST_NAME
    existing: dict[str, object] = {}
    if manifest_path.is_file():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
    platforms = dict(existing.get("platforms") or {})
    platforms[platform_name] = data
    payload = {
        "schema": "udb.vendored-ffmpeg.v1",
        "runtime_env": {
            "bin": "UDB_FFMPEG_BIN",
            "root": "UDB_FFMPEG_ROOT",
        },
        "layout": "third_party/ffmpeg/bin/<platform>/ffmpeg(.exe)",
        "platforms": platforms,
    }
    manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest_path


def relative_path(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path)


def load_manifest(root: Path) -> tuple[Path, dict[str, object]]:
    manifest_path = root / MANIFEST_NAME
    if not manifest_path.is_file():
        raise SystemExit(
            f"vendored ffmpeg manifest missing at {manifest_path}\n"
            "Run this script with --write-manifest after installing reviewed binaries."
        )
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"vendored ffmpeg manifest is not valid JSON: {manifest_path}: {exc}") from exc
    if payload.get("schema") != "udb.vendored-ffmpeg.v1":
        raise SystemExit(f"unsupported vendored ffmpeg manifest schema in {manifest_path}")
    if payload.get("layout") != "third_party/ffmpeg/bin/<platform>/ffmpeg(.exe)":
        raise SystemExit(f"unexpected vendored ffmpeg manifest layout in {manifest_path}")
    if not isinstance(payload.get("platforms"), dict):
        raise SystemExit(f"vendored ffmpeg manifest missing object field 'platforms': {manifest_path}")
    return manifest_path, payload


def verify_manifest(root: Path, platforms: list[str]) -> dict[str, object]:
    manifest_path, payload = load_manifest(root)
    manifest_platforms = payload["platforms"]
    current = current_platform()
    verified: dict[str, object] = {}

    for platform_name in platforms:
        entry = manifest_platforms.get(platform_name)
        if not isinstance(entry, dict):
            raise SystemExit(f"vendored ffmpeg manifest missing platform entry: {platform_name}")
        target = expected_path(root, platform_name)
        if not target.is_file():
            raise SystemExit(f"vendored ffmpeg binary missing for manifest platform {platform_name}: {target}")
        expected_rel = relative_path(target)
        if entry.get("path") != expected_rel:
            raise SystemExit(
                f"vendored ffmpeg manifest path mismatch for {platform_name}: "
                f"expected {expected_rel!r}, got {entry.get('path')!r}"
            )
        actual_size = target.stat().st_size
        if entry.get("size") != actual_size:
            raise SystemExit(
                f"vendored ffmpeg manifest size mismatch for {platform_name}: "
                f"expected {entry.get('size')!r}, got {actual_size}"
            )
        actual_sha = sha256(target)
        if entry.get("sha256") != actual_sha:
            raise SystemExit(
                f"vendored ffmpeg manifest sha256 mismatch for {platform_name}: "
                f"expected {entry.get('sha256')!r}, got {actual_sha}"
            )
        version = entry.get("version")
        if not isinstance(version, str) or not version.lower().startswith("ffmpeg version"):
            raise SystemExit(f"vendored ffmpeg manifest version is invalid for {platform_name}: {version!r}")
        if platform_name == current:
            actual_version = ffmpeg_version_line(target)
            if version != actual_version:
                raise SystemExit(
                    f"vendored ffmpeg manifest version mismatch for {platform_name}: "
                    f"expected {version!r}, got {actual_version!r}"
                )
        verified[platform_name] = {
            "path": expected_rel,
            "sha256": actual_sha,
            "size": actual_size,
            "version_checked": platform_name == current,
        }

    return {
        "manifest": relative_path(manifest_path),
        "platforms": verified,
    }


def write_selftest_platform(root: Path, platform_name: str, data: bytes) -> dict[str, object]:
    target = expected_path(root, platform_name)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    return {
        "path": relative_path(target),
        "size": len(data),
        "sha256": sha256(target),
        "version": "ffmpeg version selftest",
    }


def run_selftest() -> None:
    non_host = next(
        platform_name
        for platform_name in ("linux", "macos", "windows")
        if platform_name != current_platform()
    )
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "third_party" / "ffmpeg"
        entry = write_selftest_platform(root, non_host, b"not-a-real-ffmpeg-binary")
        manifest = {
            "schema": "udb.vendored-ffmpeg.v1",
            "runtime_env": {
                "bin": "UDB_FFMPEG_BIN",
                "root": "UDB_FFMPEG_ROOT",
            },
            "layout": "third_party/ffmpeg/bin/<platform>/ffmpeg(.exe)",
            "platforms": {
                non_host: entry,
            },
        }
        (root / MANIFEST_NAME).write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        result = verify_manifest(root, [non_host])
        if non_host not in result["platforms"]:
            raise AssertionError("selftest verifier did not report the checked platform")

        bad_manifest = json.loads((root / MANIFEST_NAME).read_text(encoding="utf-8"))
        bad_manifest["platforms"][non_host]["sha256"] = "0" * 64
        (root / MANIFEST_NAME).write_text(
            json.dumps(bad_manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        try:
            verify_manifest(root, [non_host])
        except SystemExit as exc:
            if "sha256 mismatch" not in str(exc):
                raise
        else:
            raise AssertionError("selftest failed to catch manifest sha256 drift")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--selftest",
        action="store_true",
        help="run fixture-based verifier success and manifest-drift checks",
    )
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="vendored ffmpeg root")
    parser.add_argument(
        "--platform",
        choices=("linux", "macos", "windows"),
        default=current_platform(),
        help="target platform layout to verify",
    )
    parser.add_argument(
        "--install-from",
        type=Path,
        help="copy this already-approved ffmpeg binary into the expected layout before verifying",
    )
    parser.add_argument(
        "--write-manifest",
        action="store_true",
        help=f"write/update {MANIFEST_NAME} after verification",
    )
    parser.add_argument(
        "--verify-manifest",
        action="store_true",
        help=f"verify {MANIFEST_NAME} matches committed binaries; executes -version only on the host platform",
    )
    parser.add_argument(
        "--all-platforms",
        action="store_true",
        help="with --verify-manifest, hash-check linux, macos, and windows entries",
    )
    args = parser.parse_args(argv)

    if args.selftest:
        run_selftest()
        print("vendored ffmpeg verifier selftest passed")
        return 0

    root = args.root.resolve()
    if args.all_platforms and not args.verify_manifest:
        raise SystemExit("--all-platforms is only valid with --verify-manifest")
    if args.install_from is not None and args.verify_manifest:
        raise SystemExit("--install-from and --verify-manifest are mutually exclusive")

    if args.verify_manifest:
        platforms = ["linux", "macos", "windows"] if args.all_platforms else [args.platform]
        print(json.dumps(verify_manifest(root, platforms), indent=2, sort_keys=True))
        return 0

    target = expected_path(root, args.platform)
    if args.install_from is not None:
        install_from(args.install_from.resolve(), target)
    data = verify(target)
    if args.write_manifest:
        manifest = write_manifest(root, args.platform, data)
        data["manifest"] = str(manifest.relative_to(REPO_ROOT) if manifest.is_relative_to(REPO_ROOT) else manifest)
    print(json.dumps(data, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
