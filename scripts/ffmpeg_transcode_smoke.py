#!/usr/bin/env python3
"""Smoke-test the ffmpeg command used by AssetService TRANSCODE steps.

This is a binary/container proof: it verifies the packaged ffmpeg can generate
and transcode a tiny MP4 using the same allowlisted encoder flags that
`asset_service::run_ffmpeg_transcode` uses. It deliberately does not claim the
full broker served-path proof; that still needs a live AssetService pipeline
run with storage attached.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = REPO_ROOT / "third_party" / "ffmpeg"
VERIFIER = REPO_ROOT / "scripts" / "check-vendored-ffmpeg.py"
MAX_FFMPEG_COMMAND_TIMEOUT_SECONDS = 300.0
TIMEOUT_DECIMAL_PATTERN = re.compile(r"^(?:[1-9]\d*(?:\.\d+)?|0\.\d*[1-9]\d*)$")


def current_platform() -> str:
    name = platform.system().lower()
    if name == "windows":
        return "windows"
    if name == "darwin":
        return "macos"
    if name == "linux":
        return "linux"
    raise SystemExit(f"unsupported platform for vendored ffmpeg: {platform.system()}")


def normalize_timeout_seconds(timeout: float | str) -> float:
    if isinstance(timeout, str):
        raw = timeout
        stripped = raw.strip()
        if raw != stripped:
            raise SystemExit("--timeout must not include surrounding whitespace")
        if not TIMEOUT_DECIMAL_PATTERN.fullmatch(stripped):
            raise SystemExit("--timeout must be a positive decimal number of seconds")
        parsed = float(stripped)
    else:
        parsed = float(timeout)
    if parsed <= 0:
        raise SystemExit("--timeout must be positive")
    if parsed > MAX_FFMPEG_COMMAND_TIMEOUT_SECONDS:
        raise SystemExit("--timeout must be <= 300 seconds")
    return parsed


def run_checked(cmd: list[str], *, timeout: float, label: str) -> subprocess.CompletedProcess[str]:
    try:
        proc = subprocess.run(
            cmd,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise SystemExit(f"{label} timed out after {timeout}s: {' '.join(cmd)}") from exc
    except FileNotFoundError as exc:
        raise SystemExit(f"{label} executable not found: {cmd[0]}") from exc
    if proc.returncode != 0:
        stderr = (proc.stderr or proc.stdout or "").strip()
        raise SystemExit(f"{label} failed with exit {proc.returncode}: {stderr}")
    return proc


def binary_from_verifier(root: Path, platform_name: str) -> Path:
    proc = run_checked(
        [
            sys.executable,
            str(VERIFIER),
            "--root",
            str(root),
            "--platform",
            platform_name,
        ],
        timeout=15,
        label="vendored ffmpeg verifier",
    )
    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"vendored ffmpeg verifier did not return JSON: {proc.stdout}") from exc
    path = payload.get("path")
    if not isinstance(path, str) or not path:
        raise SystemExit(f"vendored ffmpeg verifier JSON missing path: {payload!r}")
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = REPO_ROOT / candidate
    return candidate


def resolve_ffmpeg(args: argparse.Namespace) -> Path:
    if args.ffmpeg_bin is not None:
        candidate = args.ffmpeg_bin.resolve()
        if not candidate.is_file():
            raise SystemExit(f"--ffmpeg-bin does not point to a file: {candidate}")
        return candidate
    env_bin = os.environ.get("UDB_FFMPEG_BIN", "").strip()
    if env_bin:
        candidate = Path(env_bin).resolve()
        if not candidate.is_file():
            raise SystemExit(f"UDB_FFMPEG_BIN points to a missing file: {candidate}")
        return candidate
    env_root = os.environ.get("UDB_FFMPEG_ROOT", "").strip()
    root = Path(env_root).resolve() if env_root else args.root.resolve()
    return binary_from_verifier(root, args.platform)


def generate_input_cmd(ffmpeg: Path, output_path: Path) -> list[str]:
    return [
        str(ffmpeg),
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "testsrc=size=160x120:rate=15",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=440:sample_rate=48000",
        "-t",
        "1",
        "-pix_fmt",
        "yuv420p",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-c:a",
        "aac",
        "-shortest",
        str(output_path),
    ]


def runtime_transcode_cmd(ffmpeg: Path, input_path: Path, output_path: Path) -> list[str]:
    return [
        str(ffmpeg),
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-i",
        str(input_path),
        "-map",
        "0:v:0?",
        "-map",
        "0:a:0?",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-movflags",
        "+faststart",
        "-c:a",
        "aac",
        "-f",
        "mp4",
        str(output_path),
    ]


def decode_check_cmd(ffmpeg: Path, output_path: Path) -> list[str]:
    return [
        str(ffmpeg),
        "-nostdin",
        "-v",
        "error",
        "-i",
        str(output_path),
        "-f",
        "null",
        "-",
    ]


def assert_mp4_container(path: Path) -> None:
    data = path.read_bytes()[:64]
    if b"ftyp" not in data:
        raise SystemExit(f"transcode output does not look like an MP4 container: {path}")


def selftest() -> int:
    ffmpeg = Path("/opt/udb/third_party/ffmpeg/bin/linux/ffmpeg")
    input_path = Path("/tmp/udb-transcode/input.mp4")
    output_path = Path("/tmp/udb-transcode/output.mp4")
    generate = generate_input_cmd(ffmpeg, input_path)
    transcode = runtime_transcode_cmd(ffmpeg, input_path, output_path)
    decode = decode_check_cmd(ffmpeg, output_path)

    required_runtime_flags = [
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-map",
        "0:v:0?",
        "-map",
        "0:a:0?",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-movflags",
        "+faststart",
        "-c:a",
        "aac",
        "-f",
        "mp4",
    ]
    missing = [flag for flag in required_runtime_flags if flag not in transcode]
    if missing:
        raise SystemExit(f"runtime transcode command missing flags: {missing}")
    if "lavfi" not in generate or "testsrc=size=160x120:rate=15" not in generate:
        raise SystemExit("input generator must be deterministic lavfi test video")
    if decode[-2:] != ["null", "-"]:
        raise SystemExit(f"decode check must render to null muxer, got: {decode}")
    if normalize_timeout_seconds("30.0") != 30.0:
        raise SystemExit("canonical ffmpeg timeout string was rejected")
    for value, expected in (
        (" 30 ", "surrounding whitespace"),
        ("1e2", "positive decimal"),
        ("301", "<= 300 seconds"),
    ):
        try:
            normalize_timeout_seconds(value)
        except SystemExit as error:
            if expected not in str(error):
                raise
        else:
            raise SystemExit(f"ffmpeg timeout regression was not caught: {value}")
    print("ffmpeg transcode smoke selftest ok")
    return 0


def smoke(args: argparse.Namespace) -> int:
    ffmpeg = resolve_ffmpeg(args)
    version = run_checked(
        [str(ffmpeg), "-hide_banner", "-version"],
        timeout=10,
        label="ffmpeg -version",
    ).stdout.splitlines()[0]

    with tempfile.TemporaryDirectory(prefix="udb-ffmpeg-smoke-") as tmp:
        tmpdir = Path(tmp)
        input_path = tmpdir / "input.mp4"
        output_path = tmpdir / "output.mp4"
        run_checked(generate_input_cmd(ffmpeg, input_path), timeout=args.timeout, label="generate input MP4")
        run_checked(runtime_transcode_cmd(ffmpeg, input_path, output_path), timeout=args.timeout, label="runtime transcode")
        run_checked(decode_check_cmd(ffmpeg, output_path), timeout=args.timeout, label="decode transcode output")
        assert_mp4_container(output_path)

        summary = {
            "ffmpeg": str(ffmpeg),
            "version": version,
            "input_bytes": input_path.stat().st_size,
            "output_bytes": output_path.stat().st_size,
            "transcode_flags": runtime_transcode_cmd(Path("ffmpeg"), Path("input.mp4"), Path("output.mp4"))[1:-1],
        }
        if args.artifact_dir is not None:
            artifact_dir = args.artifact_dir.resolve()
            artifact_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(input_path, artifact_dir / "input.mp4")
            shutil.copy2(output_path, artifact_dir / "output.mp4")
            (artifact_dir / "summary.json").write_text(
                json.dumps(summary, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            summary["artifact_dir"] = str(artifact_dir)

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="vendored ffmpeg root")
    parser.add_argument(
        "--platform",
        choices=("linux", "macos", "windows"),
        default=current_platform(),
        help="vendored platform layout to verify when --ffmpeg-bin is not set",
    )
    parser.add_argument("--ffmpeg-bin", type=Path, help="explicit ffmpeg binary to exercise")
    parser.add_argument("--timeout", default="30", help="per-ffmpeg-command timeout in seconds")
    parser.add_argument("--artifact-dir", type=Path, help="copy input/output MP4s and summary JSON here")
    parser.add_argument("--selftest", action="store_true", help="validate command construction without running ffmpeg")
    args = parser.parse_args(argv)
    args.timeout = normalize_timeout_seconds(args.timeout)
    if args.selftest:
        return selftest()
    return smoke(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
