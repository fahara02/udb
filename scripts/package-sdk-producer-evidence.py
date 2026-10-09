#!/usr/bin/env python3
"""Package successful CI SDK generation without running a producer or a build.

The workflow must gate this helper on the current build's success and write its
marker only after the actual all-language generation and non-writing check pass.
Digests describe captured bytes; they do not declare API compatibility.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from collect_sdk_bench_results import _load_canonical_rpc_contract


ROOT = Path(__file__).resolve().parents[1]
PRODUCER_INPUTS = (
    "src", "crates", "proto", "third_party", "sdk-templates", ".cargo",
    "build.rs", "Cargo.toml", "Cargo.lock", "versions.json", "rust-toolchain*",
    "scripts/package-sdk-producer-evidence.py", "scripts/collect_sdk_bench_results.py",
)
SHA256 = re.compile(r"[0-9a-f]{64}")
COMMIT = re.compile(r"[0-9a-f]{40}")


def fail(message: str) -> None:
    raise ValueError(message)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def write_json(path: Path, value: Any) -> None:
    path.write_bytes(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False).encode("utf-8") + b"\n")


def owned_path(value: str, label: str, *, existing: bool = True, nonempty: bool = True) -> Path:
    candidate = Path(value)
    if not candidate.is_absolute():
        candidate = ROOT / candidate
    path = candidate.resolve(strict=existing)
    if not path.is_relative_to(ROOT):
        fail(f"{label} must be inside the checked-out repository")
    if existing and (not path.is_file() or (nonempty and path.stat().st_size == 0)):
        fail(f"{label} must be a non-empty regular file")
    return path


def git(*arguments: str) -> bytes:
    result = subprocess.run(["git", *arguments], cwd=ROOT, capture_output=True, check=False)
    if result.returncode != 0:
        fail("Git could not verify the producer source snapshot")
    return result.stdout


def read_json(path: Path, label: str) -> Any:
    try:
        return json.loads(path.read_bytes())
    except (OSError, UnicodeError, ValueError) as error:
        raise ValueError(f"{label} must contain valid JSON") from error


def positive_integer(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        fail(f"{label} must be a positive integer")
    return value


def nonempty_string(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        fail(f"{label} must be a non-empty canonical string")
    return value


def rpc_surface(document: Any, expected: int) -> dict[str, tuple[str, str]]:
    if not isinstance(document, dict):
        fail("RPC manifest must contain an object")
    if positive_integer(document.get("rpc_count"), "RPC manifest rpc_count") != expected:
        fail("RPC manifest rpc_count differs from the explicitly expected count")
    nonempty_string(document.get("udb_version"), "RPC manifest udb_version")
    nonempty_string(document.get("protocol_version"), "RPC manifest protocol_version")
    services = document.get("services")
    if not isinstance(services, list) or not services:
        fail("RPC manifest services must be a non-empty array")
    if positive_integer(document.get("service_count"), "RPC manifest service_count") != len(services):
        fail("RPC manifest service_count differs from the actual services")
    surface: dict[str, tuple[str, str]] = {}
    service_names: set[str] = set()
    for service in services:
        if not isinstance(service, dict):
            fail("RPC manifest service must be an object")
        name = nonempty_string(service.get("service"), "RPC manifest service name")
        if not re.fullmatch(r"udb(?:\.[A-Za-z_][A-Za-z0-9_]*)+", name) or name in service_names:
            fail("RPC manifest has an invalid or repeated fully qualified service")
        service_names.add(name)
        methods = service.get("rpcs")
        if not isinstance(methods, list) or not methods:
            fail("RPC manifest service rpcs must be a non-empty array")
        if positive_integer(service.get("rpc_count"), "Service rpc_count") != len(methods):
            fail("Service rpc_count differs from its actual methods")
        for method in methods:
            if not isinstance(method, dict):
                fail("RPC manifest method must be an object")
            method_name = nonempty_string(method.get("method"), "RPC method name")
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", method_name):
                fail("RPC manifest method name is invalid")
            if method.get("path") != f"/{name}/{method_name}":
                fail("RPC manifest method path differs from its actual service/method")
            wire_rpc = f"{name.rsplit('.', 1)[1]}/{method_name}"
            if wire_rpc in surface:
                fail("RPC manifest repeats a canonical wire identity")
            surface[wire_rpc] = (
                nonempty_string(method.get("method_alias_snake"), "RPC API alias"),
                nonempty_string(method.get("rest_operation_id"), "RPC operation ID"),
            )
    if len(surface) != expected:
        fail("RPC manifest actual method count differs from rpc_count")
    return surface


def verify_marker(marker: Any, source_sha: str, binary: Path, captured: dict[str, Path]) -> None:
    if not isinstance(marker, dict) or type(marker.get("schema_version")) is not int or marker["schema_version"] != 1:
        fail("Generation/check marker must use schema_version 1")
    if marker.get("source_sha") != source_sha or marker.get("binary_sha256") != digest(binary):
        fail("Generation/check marker does not identify the current source and binary")
    for field in ("generation_exit_code", "check_exit_code"):
        if type(marker.get(field)) is not int or marker[field] != 0:
            fail(f"Generation/check marker {field} must prove success")
    command = [binary.relative_to(ROOT).as_posix(), "sdk", "generate", "--lang", "all", "--out", "sdk"]
    if marker.get("generation_command") != command or marker.get("check_command") != [*command, "--check"]:
        fail("Generation/check marker must identify the canonical all-language commands")
    for label, path in captured.items():
        declared = marker.get(f"{label}_sha256")
        if not isinstance(declared, str) or not SHA256.fullmatch(declared) or declared != digest(path):
            fail(f"Generation/check marker does not match captured {label}")


def source_ledger() -> list[dict[str, Any]]:
    git("diff", "--quiet", "HEAD", "--", *PRODUCER_INPUTS)
    paths = git("ls-files", "-z", "--", *PRODUCER_INPUTS).split(b"\0")
    result = []
    for raw in sorted(path for path in paths if path):
        relative = os.fsdecode(raw)
        path = owned_path(relative, "Producer input", nonempty=False)
        result.append({"path": relative, "size": path.stat().st_size, "sha256": digest(path)})
    if not result:
        fail("Tracked producer input ledger is empty")
    return result


def sdk_archive(destination: Path) -> list[dict[str, Any]]:
    sdk = (ROOT / "sdk").resolve(strict=True)
    if not sdk.is_dir() or sdk != ROOT / "sdk":
        fail("Canonical SDK tree must be a repository directory")
    files = sorted(sdk.rglob("*"))
    result = []
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for path in files:
            if path.is_symlink():
                fail("SDK evidence must not follow symlinks")
            if path.is_dir():
                continue
            if not path.is_file():
                fail("SDK evidence contains a non-regular file")
            relative = path.relative_to(ROOT).as_posix()
            mode = stat.S_IMODE(path.stat().st_mode)
            info = zipfile.ZipInfo(relative, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | mode) << 16
            value = hashlib.sha256()
            size = 0
            with path.open("rb") as source, archive.open(info, "w") as output:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    value.update(chunk)
                    size += len(chunk)
                    output.write(chunk)
            result.append({"path": relative, "size": size, "mode": mode, "sha256": value.hexdigest()})
    if not result:
        fail("Canonical SDK evidence tree is empty")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--rpc-manifest", required=True)
    parser.add_argument("--canonical-manifest", default="docs/generated/bench-bodies.json")
    parser.add_argument("--generation-check-marker", required=True)
    parser.add_argument("--generation-log", required=True)
    parser.add_argument("--check-log", required=True)
    parser.add_argument("--expected-rpc-count", type=int, required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    source_sha = os.environ.get("GITHUB_SHA", "")
    if not COMMIT.fullmatch(source_sha) or git("rev-parse", "HEAD").decode("ascii").strip() != source_sha:
        fail("GITHUB_SHA must equal the exact checked-out Git HEAD")
    positive_integer(args.expected_rpc_count, "Expected RPC count")
    binary = owned_path(args.binary, "Current producer binary")
    rpc = owned_path(args.rpc_manifest, "Actual RPC manifest")
    canonical = owned_path(args.canonical_manifest, "Canonical benchmark manifest")
    marker = owned_path(args.generation_check_marker, "Successful generation/check marker")
    captured = {
        "rpc_manifest": rpc,
        "generation_log": owned_path(args.generation_log, "Generation log"),
        "check_log": owned_path(args.check_log, "Generation check log"),
    }
    verify_marker(read_json(marker, "Generation/check marker"), source_sha, binary, captured)
    rpc_document = read_json(rpc, "RPC manifest")
    surface = rpc_surface(rpc_document, args.expected_rpc_count)
    benchmark_contract, canonical_surface = _load_canonical_rpc_contract(canonical)
    if len(canonical_surface) != args.expected_rpc_count or surface != canonical_surface:
        fail("Actual RPC identities, aliases and operation IDs differ from the canonical benchmark manifest")
    # This packet must not quietly regenerate a changed benchmark contract and
    # then call its new count unchanged. Verify against the committed snapshot.
    canonical_relative = canonical.relative_to(ROOT).as_posix()
    if hashlib.sha256(git("show", f"HEAD:{canonical_relative}")).hexdigest() != digest(canonical):
        fail("Canonical benchmark manifest differs from the committed source snapshot")
    inputs = source_ledger()
    output = owned_path(args.out, "Evidence output", existing=False)
    if output == ROOT or output.is_relative_to(ROOT / "sdk") or output.exists():
        fail("Evidence output must be a new directory outside the SDK tree")
    if output in (binary.parent, rpc.parent, marker.parent):
        fail("Evidence output must not replace producer inputs")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".sdk-producer-evidence-", dir=output.parent) as temporary:
        stage = Path(temporary)
        files = sdk_archive(stage / "sdk-tree.zip")
        write_json(stage / "sdk-files.json", files)
        write_json(stage / "producer-input-files.json", inputs)
        for name, path in {
            "rpc-manifest.json": rpc,
            "canonical-bench-manifest.json": canonical,
            "generation-check.json": marker,
            "sdk-generate.log": captured["generation_log"],
            "sdk-generate-check.log": captured["check_log"],
        }.items():
            shutil.copyfile(path, stage / name)
        api_digest = {
            "schema_version": 1,
            "source_sha": source_sha,
            "rpc_count": len(surface),
            "rpc_manifest_sha256": digest(rpc),
            "canonical_bench_manifest_sha256": benchmark_contract["canonical_manifest_sha256"],
            "sdk_tree_sha256": hashlib.sha256(canonical_json(files)).hexdigest(),
            "producer_inputs_sha256": hashlib.sha256(canonical_json(inputs)).hexdigest(),
            "digest_semantics": "Captured manifest and SDK bytes; no API-compatibility verdict",
        }
        write_json(stage / "api-digest.json", api_digest)
        write_json(stage / "producer-provenance.json", {
            "schema_version": 1,
            "source_sha": source_sha,
            "github_run_id": os.environ.get("GITHUB_RUN_ID", ""),
            "github_run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", ""),
            "producer_binary": binary.relative_to(ROOT).as_posix(),
            "producer_binary_sha256": digest(binary),
            "producer_binary_size": binary.stat().st_size,
            "udb_version": rpc_document["udb_version"],
            "protocol_version": rpc_document["protocol_version"],
            "rpc_count": len(surface),
            "sdk_file_count": len(files),
            "build_precondition": "Workflow requires current Build all targets outcome == success",
            "api_digest_sha256": digest(stage / "api-digest.json"),
            "artifact_files": [
                {"path": path.name, "size": path.stat().st_size, "sha256": digest(path)}
                for path in sorted(stage.iterdir()) if path.is_file()
            ],
        })
        stage.rename(output)
    print(f"SDK producer evidence packaged: {len(surface)} RPCs; {len(files)} SDK files; source {source_sha}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        raise SystemExit(f"SDK producer evidence refused: {error}") from error
