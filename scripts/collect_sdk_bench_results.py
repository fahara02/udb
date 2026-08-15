#!/usr/bin/env python3
"""Collect SDK live benchmark Markdown reports into one Pages JSON artifact."""

from __future__ import annotations

import argparse
from collections import Counter
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

SDK_REPORTS = {
    "go": ROOT / "sdk/go/udbclient/perf_report_go.md",
    "python": ROOT / "sdk/python/perf_report_python.md",
    "typescript": ROOT / "sdk/typescript/perf_report_ts.md",
    "php": ROOT / "sdk/php/perf_report_php.md",
}

SDK_NAMES = {
    "go": "Go",
    "python": "Python",
    "typescript": "TypeScript",
    "php": "PHP",
    "csharp": "C#",
    "java": "Java",
}

MISSING_HARNESS = {
    "csharp": "No live per-RPC benchmark harness exists yet; SDK build/unit conformance still runs in CI.",
    "java": "No live per-RPC benchmark harness exists yet; SDK compile/unit conformance still runs in CI.",
}

CANONICAL_RPC_MANIFEST = ROOT / "docs/generated/bench-bodies.json"
CANONICAL_RPC_MANIFEST_LABEL = "docs/generated/bench-bodies.json"
MEASURED_SDK_IDS = tuple(SDK_REPORTS)
SKIPPED_SDK_IDS = tuple(MISSING_HARNESS)

GRPC_STATUS_CODES = {
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
}

HARNESS_STATUS_CODES = {
    "CAPABILITY_SKIPPED",
    "SEED_BLOCKED",
    "SKIP_NO_BODY",
}

NON_FATAL_HARNESS_STATUS_CODES = {
    "CAPABILITY_SKIPPED",
}

# Language harnesses spell the "no manifest body could be hydrated" status
# differently — the Go harness emits `NO-BODY` (→ `NO_BODY` after normalization),
# the shared collector schema names it `SKIP_NO_BODY`. Alias the variants onto the
# canonical token so a missing body is a countable, fatal failure (never a crash).
HARNESS_STATUS_ALIASES = {
    "NO_BODY": "SKIP_NO_BODY",
    "NOBODY": "SKIP_NO_BODY",
}


def _benchmark_contract(
    canonical_rpcs: dict[str, tuple[str, str]],
    manifest_sha256: str,
) -> dict[str, Any]:
    return {
        "canonical_manifest": CANONICAL_RPC_MANIFEST_LABEL,
        "canonical_manifest_sha256": manifest_sha256,
        "canonical_rpc_count": len(canonical_rpcs),
        "measured_sdk_ids": list(MEASURED_SDK_IDS),
        "skipped_sdk_ids": list(SKIPPED_SDK_IDS),
        "expected_measured_rpc_count": len(canonical_rpcs) * len(MEASURED_SDK_IDS),
    }


def _load_canonical_rpc_contract(
    path: Path = CANONICAL_RPC_MANIFEST,
) -> tuple[dict[str, Any], dict[str, tuple[str, str]]]:
    try:
        raw = path.read_bytes()
        rows = json.loads(raw)
    except Exception as exc:
        raise ValueError(f"cannot read canonical benchmark RPC manifest {path}: {exc}") from exc
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"canonical benchmark RPC manifest {path} must be a non-empty JSON array")

    canonical_rows: list[tuple[str, str, str]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"canonical benchmark RPC manifest row {index} must be an object")
        service = row.get("service")
        rpc = row.get("rpc")
        wire_rpc = row.get("wire_rpc")
        api_alias = row.get("api_alias")
        operation_id = row.get("operation_id")
        if not all(
            isinstance(value, str) and value.strip()
            for value in (service, rpc, wire_rpc, api_alias, operation_id)
        ):
            raise ValueError(
                f"canonical benchmark RPC manifest row {index} must contain non-empty "
                "service/rpc/wire_rpc/api_alias/operation_id"
            )
        if "." in rpc:
            rpc_service, rpc_name = rpc.rsplit(".", 1)
            if rpc_service != service:
                raise ValueError(
                    f"canonical benchmark RPC manifest row {index} has qualified rpc={rpc!r} "
                    f"outside service={service!r}"
                )
        else:
            rpc_name = rpc
        expected_wire_rpc = f"{service}/{rpc_name}"
        if wire_rpc != expected_wire_rpc:
            raise ValueError(
                f"canonical benchmark RPC manifest row {index} has wire_rpc={wire_rpc!r}, "
                f"expected {expected_wire_rpc!r}"
            )
        canonical_rows.append((wire_rpc, api_alias, operation_id))

    duplicates = sorted(
        rpc for rpc, count in Counter(wire_rpc for wire_rpc, _, _ in canonical_rows).items() if count != 1
    )
    if duplicates:
        raise ValueError(
            "canonical benchmark RPC manifest contains duplicate wire identities: "
            + _preview(duplicates)
        )
    surface = {
        wire_rpc: (api_alias, operation_id)
        for wire_rpc, api_alias, operation_id in canonical_rows
    }
    return _benchmark_contract(surface, hashlib.sha256(raw).hexdigest()), surface


def _preview(values: list[str], limit: int = 5) -> str:
    shown = values[:limit]
    suffix = f" (+{len(values) - limit} more)" if len(values) > limit else ""
    return ", ".join(shown) + suffix


def _full_row_wire_api(row: Any) -> str | None:
    if not isinstance(row, dict):
        return None
    wire_api = row.get("wire_api")
    if not isinstance(wire_api, str) or not wire_api.strip():
        return None
    return wire_api


def _cmd(args: list[str]) -> str | None:
    try:
        return subprocess.check_output(args, cwd=ROOT, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return None


def _read_status(status_dir: Path, sdk: str) -> dict[str, Any]:
    path = status_dir / f"{sdk}.json"
    if not path.is_file():
        return {"exit_code": None, "status": "missing"}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"exit_code": None, "status": "invalid", "note": str(exc)}


def _duration_ms(value: str) -> float | None:
    raw = value.strip().replace("μ", "µ")
    if raw in {"", "-"}:
        return None
    m = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*([a-zµ]+)?$", raw, re.I)
    if not m:
        return None
    n = float(m.group(1))
    unit = (m.group(2) or "ms").lower()
    if unit in {"ms", "millisecond", "milliseconds"}:
        return n
    if unit in {"µs", "us", "microsecond", "microseconds"}:
        return n / 1000.0
    if unit in {"ns", "nanosecond", "nanoseconds"}:
        return n / 1_000_000.0
    if unit in {"s", "sec", "second", "seconds"}:
        return n * 1000.0
    return None


def _cells(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def _first(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value:
            return value.strip()
    return ""


def _identity(service: str, rpc: str, api_alias: str = "", operation_id: str = "") -> tuple[str, str]:
    wire_api = f"{service}/{rpc}" if service and rpc else rpc
    return operation_id or api_alias or wire_api, wire_api


def _canonical_grpc_status(raw: str) -> str:
    value = raw.strip().strip("`")
    wrapped = re.fullmatch(r"(?:FAILED|ERROR)\s*\(([^)]+)\)", value, re.I)
    if wrapped:
        value = wrapped.group(1).strip()
    value = value.split("::")[-1].split(".")[-1]
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    token = re.sub(r"[\s-]+", "_", value).upper()
    token = HARNESS_STATUS_ALIASES.get(token, token)
    if token in HARNESS_STATUS_CODES:
        return token
    if token not in GRPC_STATUS_CODES:
        raise ValueError(
            f"benchmark report err token {raw!r} is not a canonical gRPC status"
        )
    return token


def _normalized_failure_status(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"benchmark report err token must be a string or null, got {value!r}")
    raw = value.strip()
    if raw in {"", "-", "OK", "ok"}:
        return None
    status = _canonical_grpc_status(raw)
    if status in NON_FATAL_HARNESS_STATUS_CODES:
        return None
    return status


def _parse_report(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    measured = None
    m = re.search(r"RPCs measured:\s*([0-9]+)", text)
    if m:
        measured = int(m.group(1))
    harness_error = None
    h = re.search(r"(?ms)^## Harness error\s+(.+?)(?:\n## |\Z)", text)
    if h:
        harness_error = " ".join(line.replace("`", "").strip() for line in h.group(1).splitlines() if line.strip())

    services: list[dict[str, Any]] = []
    slowest: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    full_rpcs: list[dict[str, Any]] = []
    section = ""
    headers: list[str] = []

    for line in lines:
        low = line.lower()
        if low.startswith("## per-service mean"):
            section = "services"
            headers = []
            continue
        if low.startswith("## full per-rpc table"):
            section = "full"
            headers = []
            continue
        if low.startswith("## slowest"):
            section = "slowest"
            headers = []
            continue
        if low.startswith("## failures"):
            section = "failures"
            headers = []
            continue
        if low.startswith("## "):
            section = ""
            headers = []
            continue
        if not section or not line.startswith("|"):
            continue
        cells = _cells(line)
        if not cells or set(cells[0]) <= {"-"}:
            continue
        if not headers:
            headers = [c.lower().replace(" ", "_") for c in cells]
            continue
        if len(cells) < len(headers):
            continue
        row = dict(zip(headers, cells))
        if section == "services":
            name = row.get("service")
            if not name:
                continue
            services.append({
                "service": name,
                "rpc_count": int(row.get("rpcs", "0") or "0"),
                "mean_ms": _duration_ms(row.get("mean_ms") or row.get("mean") or ""),
            })
        elif section == "slowest":
            rpc = row.get("rpc")
            if not rpc:
                continue
            service = row.get("service", "")
            if "/" in rpc and not service:
                service, rpc_name = rpc.split("/", 1)
            else:
                rpc_name = rpc
            api_alias = _first(row, "api_alias", "alias", "sdk_alias")
            operation_id = _first(row, "operation_id", "operationid", "operation")
            api, wire_api = _identity(service, rpc_name, api_alias, operation_id)
            slowest.append({
                "rpc": rpc,
                "service": service,
                "wire_rpc": rpc_name,
                "wire_api": wire_api,
                "api": api,
                "api_alias": api_alias,
                "operation_id": operation_id,
                "kind": row.get("kind", ""),
                # err_code: None for OK rows; absent column (older report) also → None.
                "err_code": _normalized_failure_status(row.get("err")),
                "p50_ms": _duration_ms(row.get("p50_ms") or row.get("p50") or ""),
                "p99_ms": _duration_ms(row.get("p99_ms") or row.get("p99") or ""),
                "mean_ms": _duration_ms(row.get("mean_ms") or row.get("mean") or ""),
            })
        elif section == "failures":
            rpc = row.get("rpc")
            if not rpc:
                continue
            service = row.get("service", "")
            if "/" in rpc and not service:
                service, rpc_name = rpc.split("/", 1)
            else:
                rpc_name = rpc
            api_alias = _first(row, "api_alias", "alias", "sdk_alias")
            operation_id = _first(row, "operation_id", "operationid", "operation")
            api, wire_api = _identity(service, rpc_name, api_alias, operation_id)
            failures.append({
                "rpc": rpc,
                "service": service,
                "wire_rpc": rpc_name,
                "wire_api": wire_api,
                "api": api,
                "api_alias": api_alias,
                "operation_id": operation_id,
                "kind": row.get("kind", ""),
                "err_code": _normalized_failure_status(row.get("err")) or "UNKNOWN",
                "p99_ms": _duration_ms(row.get("p99_ms") or row.get("p99") or ""),
                "mean_ms": _duration_ms(row.get("mean_ms") or row.get("mean") or ""),
            })
        elif section == "full":
            service = row.get("service")
            rpc = row.get("rpc")
            if not service or not rpc:
                continue
            api_alias = _first(row, "api_alias", "alias", "sdk_alias")
            operation_id = _first(row, "operation_id", "operationid", "operation")
            api, wire_api = _identity(service, rpc, api_alias, operation_id)
            iters = None
            try:
                raw_iters = row.get("iters") or row.get("iterations") or ""
                iters = int(raw_iters) if raw_iters.strip() else None
            except ValueError:
                iters = None
            full_rpcs.append({
                "service": service,
                "rpc": rpc,
                "wire_api": wire_api,
                "api": api,
                "api_alias": api_alias,
                "operation_id": operation_id,
                "kind": row.get("kind", ""),
                "err_code": _normalized_failure_status(row.get("err")),
                "p50_ms": _duration_ms(row.get("p50_ms") or row.get("p50") or ""),
                "p99_ms": _duration_ms(row.get("p99_ms") or row.get("p99") or ""),
                "mean_ms": _duration_ms(row.get("mean_ms") or row.get("mean") or ""),
                "min_ms": _duration_ms(row.get("min_ms") or row.get("min") or ""),
                "max_ms": _duration_ms(row.get("max_ms") or row.get("max") or ""),
                "iters": iters,
                "note": row.get("note", ""),
            })

    service_means = [s["mean_ms"] for s in services if isinstance(s.get("mean_ms"), (int, float))]
    # Authoritative failure set = the Failures subsection, unioned with any failed
    # row present in the slowest OR full per-RPC tables. Key by wire API, not bare
    # method name, so same-named RPCs on different services cannot collapse.
    # Backward compatible: an older report with no failure section and no err
    # column yields 0 failures.
    def failure_key(row: dict[str, Any]) -> str:
        return str(row.get("wire_api") or row.get("api") or row.get("rpc") or "")

    failed_by_rpc: dict[str, dict[str, Any]] = {}
    for f in failures:
        failed_by_rpc[failure_key(f)] = f
    for s in slowest:
        key = failure_key(s)
        if s.get("err_code") and key not in failed_by_rpc:
            failed_by_rpc[key] = {
                "rpc": s["rpc"], "service": s.get("service", ""), "wire_rpc": s.get("wire_rpc", ""),
                "wire_api": s.get("wire_api", ""), "api": s.get("api", ""),
                "kind": s.get("kind", ""), "err_code": s["err_code"],
                "p99_ms": s.get("p99_ms"), "mean_ms": s.get("mean_ms"),
            }
    for r in full_rpcs:
        key = failure_key(r)
        if r.get("err_code") and key not in failed_by_rpc:
            failed_by_rpc[key] = {
                "rpc": r["rpc"], "service": r.get("service", ""), "wire_rpc": r.get("rpc", ""),
                "wire_api": r.get("wire_api", ""), "api": r.get("api", ""),
                "api_alias": r.get("api_alias", ""), "operation_id": r.get("operation_id", ""),
                "kind": r.get("kind", ""), "err_code": r["err_code"],
                "p99_ms": r.get("p99_ms"), "mean_ms": r.get("mean_ms"),
            }
    failed_rpcs = sorted(failed_by_rpc.values(), key=lambda x: x.get("wire_api") or x["rpc"])

    summary: dict[str, Any] = {
        "rpc_count": measured,
        "service_count": len(services),
        "slowest_count": len(slowest),
        "failed_rpc_count": len(failed_rpcs),
    }
    if service_means:
        summary["mean_service_latency_ms"] = sum(service_means) / len(service_means)
        summary["slowest_service_mean_ms"] = max(service_means)

    try:
        report_path = str(path.relative_to(ROOT)).replace("\\", "/")
    except ValueError:
        report_path = str(path)

    parsed = {
        "summary": summary,
        "services": services,
        "slowest": slowest,
        "failed_rpcs": failed_rpcs,
        "full_rpcs": full_rpcs,
        "report_path": report_path,
    }
    if harness_error:
        parsed["harness_error"] = harness_error[:2000]
    return parsed


def _benchmark_gate_failures(
    payload: dict[str, Any],
    expected_contract: dict[str, Any],
    canonical_rpcs: dict[str, tuple[str, str]],
) -> list[str]:
    summary = payload.get("summary", {})
    sdks = payload.get("sdks", [])
    if not isinstance(summary, dict):
        summary = {}
    if not isinstance(sdks, list):
        sdks = []
    bad_sdks = [
        s.get("name") or s.get("id")
        for s in sdks
        if isinstance(s, dict) and s.get("status") not in {"ok", "skipped"}
    ]
    try:
        failed_rpcs = int(summary.get("failed_rpc_count") or 0)
    except (TypeError, ValueError):
        failed_rpcs = 0

    failures: list[str] = []
    if bad_sdks:
        failures.append(f"bad_sdks={bad_sdks}")
    if failed_rpcs:
        failures.append(f"failed_rpc_count={failed_rpcs}")

    if payload.get("benchmark_contract") != expected_contract:
        failures.append("benchmark_contract does not match the canonical RPC manifest")

    sdk_rows = [sdk for sdk in sdks if isinstance(sdk, dict)]
    sdk_ids = [sdk.get("id") for sdk in sdk_rows]
    invalid_ids = [sdk_id for sdk_id in sdk_ids if not isinstance(sdk_id, str) or not sdk_id]
    if invalid_ids or len(sdk_rows) != len(sdks):
        failures.append("benchmark SDK entries must be objects with non-empty string ids")
    counts = Counter(sdk_id for sdk_id in sdk_ids if isinstance(sdk_id, str) and sdk_id)
    duplicate_sdk_ids = sorted(sdk_id for sdk_id, count in counts.items() if count != 1)
    if duplicate_sdk_ids:
        failures.append(f"duplicate SDK entries: {_preview(duplicate_sdk_ids)}")

    expected_sdk_ids = set(MEASURED_SDK_IDS) | set(SKIPPED_SDK_IDS)
    actual_sdk_ids = set(counts)
    missing_sdk_ids = sorted(expected_sdk_ids - actual_sdk_ids)
    unexpected_sdk_ids = sorted(actual_sdk_ids - expected_sdk_ids)
    if missing_sdk_ids:
        failures.append(f"missing SDK entries: {_preview(missing_sdk_ids)}")
    if unexpected_sdk_ids:
        failures.append(f"unexpected SDK entries: {_preview(unexpected_sdk_ids)}")

    expected_rpc_count = len(canonical_rpcs)
    canonical_counts = Counter(canonical_rpcs)
    by_id: dict[str, dict[str, Any]] = {}
    for sdk in sdk_rows:
        sdk_id = sdk.get("id")
        if isinstance(sdk_id, str) and counts.get(sdk_id) == 1:
            by_id[sdk_id] = sdk
    for sdk_id in MEASURED_SDK_IDS:
        sdk = by_id.get(sdk_id)
        if sdk is None:
            continue
        if sdk.get("status") != "ok":
            failures.append(f"measured SDK {sdk_id} must have status=ok")
        sdk_summary = sdk.get("summary")
        if not isinstance(sdk_summary, dict):
            sdk_summary = {}
        if sdk_summary.get("rpc_count") != expected_rpc_count:
            failures.append(
                f"measured SDK {sdk_id} rpc_count={sdk_summary.get('rpc_count')!r}, "
                f"expected {expected_rpc_count}"
            )
        if sdk_summary.get("failed_rpc_count") != 0:
            failures.append(f"measured SDK {sdk_id} must have failed_rpc_count=0")

        full_rows = sdk.get("full_rpcs")
        if not isinstance(full_rows, list):
            failures.append(f"measured SDK {sdk_id} full_rpcs must be a list")
            continue
        if len(full_rows) != expected_rpc_count:
            failures.append(
                f"measured SDK {sdk_id} full_rpcs has {len(full_rows)} rows, "
                f"expected {expected_rpc_count}"
            )
        row_identities = [_full_row_wire_api(row) for row in full_rows]
        invalid_row_count = sum(identity is None for identity in row_identities)
        if invalid_row_count:
            failures.append(
                f"measured SDK {sdk_id} has {invalid_row_count} full_rpcs rows without wire_api"
            )
        row_counts = Counter(identity for identity in row_identities if identity is not None)
        missing_rpcs = sorted((canonical_counts - row_counts).elements())
        unexpected_rpcs = sorted((row_counts - canonical_counts).elements())
        duplicate_rpcs = sorted(rpc for rpc, count in row_counts.items() if count > 1)
        if missing_rpcs:
            failures.append(f"measured SDK {sdk_id} missing RPCs: {_preview(missing_rpcs)}")
        if unexpected_rpcs:
            failures.append(f"measured SDK {sdk_id} unexpected RPCs: {_preview(unexpected_rpcs)}")
        if duplicate_rpcs:
            failures.append(f"measured SDK {sdk_id} duplicate RPCs: {_preview(duplicate_rpcs)}")

        failed_full_rows: list[str] = []
        for index, row in enumerate(full_rows):
            wire_api = row_identities[index]
            row_label = wire_api or f"row[{index}]"
            if isinstance(row, dict) and wire_api in canonical_rpcs:
                expected_alias, expected_operation_id = canonical_rpcs[wire_api]
                if row.get("api_alias") != expected_alias:
                    failures.append(
                        f"measured SDK {sdk_id} {wire_api} api_alias={row.get('api_alias')!r}, "
                        f"expected {expected_alias!r}"
                    )
                if row.get("operation_id") != expected_operation_id:
                    failures.append(
                        f"measured SDK {sdk_id} {wire_api} operation_id={row.get('operation_id')!r}, "
                        f"expected {expected_operation_id!r}"
                    )

            if not isinstance(row, dict):
                failed_full_rows.append(f"row[{index}]=invalid")
                continue
            error_fields = [field for field in ("err_code", "err") if field in row]
            if not error_fields:
                failures.append(
                    f"measured SDK {sdk_id} full_rpcs row {row_label!r} has no err_code/err evidence"
                )
                failed_full_rows.append(f"{row_label}=missing-status")
                continue
            row_failures: set[str] = set()
            for field in error_fields:
                try:
                    status = _normalized_failure_status(row.get(field))
                except ValueError as exc:
                    failures.append(
                        f"measured SDK {sdk_id} full_rpcs row {row_label!r} "
                        f"has invalid {field}: {exc}"
                    )
                    row_failures.add("INVALID_STATUS")
                    continue
                if status is not None:
                    row_failures.add(status)
            if row_failures:
                failed_full_rows.append(
                    f"{row_label}={'+'.join(sorted(row_failures))}"
                )
        if failed_full_rows:
            failures.append(
                f"measured SDK {sdk_id} has {len(failed_full_rows)} failed/invalid full_rpcs rows: "
                + _preview(failed_full_rows)
            )

    for sdk_id in SKIPPED_SDK_IDS:
        sdk = by_id.get(sdk_id)
        if sdk is None:
            continue
        if sdk.get("status") != "skipped":
            failures.append(f"non-measured SDK {sdk_id} must remain an explicit skip")
        sdk_summary = sdk.get("summary")
        if not isinstance(sdk_summary, dict) or sdk_summary.get("rpc_count") is not None:
            failures.append(f"non-measured SDK {sdk_id} must have rpc_count=null")
        if isinstance(sdk_summary, dict) and sdk_summary.get("failed_rpc_count") != 0:
            failures.append(f"non-measured SDK {sdk_id} must have failed_rpc_count=0")
        if sdk.get("full_rpcs") != []:
            failures.append(f"non-measured SDK {sdk_id} must not contain full_rpcs rows")

    expected_summary = {
        "sdk_count": len(expected_sdk_ids),
        "ok": len(MEASURED_SDK_IDS),
        "failed": 0,
        "skipped": len(SKIPPED_SDK_IDS),
        "canonical_rpc_count": expected_rpc_count,
        "measured_sdk_count": len(MEASURED_SDK_IDS),
        "expected_measured_rpc_count": expected_rpc_count * len(MEASURED_SDK_IDS),
        "measured_rpc_count": expected_rpc_count * len(MEASURED_SDK_IDS),
        "failed_rpc_count": 0,
    }
    for field, expected in expected_summary.items():
        if summary.get(field) != expected:
            failures.append(f"summary.{field}={summary.get(field)!r}, expected {expected!r}")
    return failures


def _gate_results(
    path: Path,
    expected_contract: dict[str, Any],
    canonical_rpcs: dict[str, tuple[str, str]],
) -> int:
    target = path if path.is_absolute() else ROOT / path
    if not target.is_file():
        print(f"ERROR: benchmark results JSON was not produced: {target}")
        return 1
    payload = json.loads(target.read_text(encoding="utf-8"))
    failures = _benchmark_gate_failures(payload, expected_contract, canonical_rpcs)
    if failures:
        print(f"Benchmark gate failed: {', '.join(failures)}")
        return 1
    print(
        "Benchmark gate passed: every measured SDK contains the complete canonical RPC surface "
        "exactly once, explicit skips are intact, and no RPC failed."
    )
    return 0


def _selftest() -> int:
    with tempfile.TemporaryDirectory(prefix="udb-bench-collector-") as tmp:
        path = Path(tmp) / "perf_report.md"

        # Regression pin: even if a language harness forgets to render the
        # Failures subsection, any full-table err != OK must still fail the
        # aggregate Pages JSON and final workflow gate.
        path.write_text(
            """# UDB SDK Live Perf

RPCs measured: 2

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | kind | err | p50 ms | p99 ms | mean ms | iters |
|---|---|---|---|--:|--:|--:|--:|
| DataBroker | Select | read_only | OK | 0.20 | 0.30 | 0.22 | 25 |
| DataBroker | ApproveMigrationPlan | mutation | RESOURCE_EXHAUSTED | 0.25 | 0.25 | 0.25 | 1 |
""",
            encoding="utf-8",
        )
        parsed = _parse_report(path)
        assert parsed["summary"]["failed_rpc_count"] == 1, parsed
        assert parsed["failed_rpcs"][0]["wire_api"] == "DataBroker/ApproveMigrationPlan", parsed
        assert parsed["failed_rpcs"][0]["err_code"] == "RESOURCE_EXHAUSTED", parsed

        # Language harnesses report status names in different spellings. The
        # collector owns the public Pages schema, so normalize them to gRPC's
        # canonical UPPER_SNAKE tokens before the page or gate sees them.
        path.write_text(
            """# UDB SDK Live Perf

RPCs measured: 2

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | kind | err | p50 ms | p99 ms | mean ms | iters |
|---|---|---|---|--:|--:|--:|--:|
| RoomService | GetRoom | read_only | ResourceExhausted | 0.20 | 0.30 | 0.22 | 1 |
| TenantService | GetTenantConfig | read_only | FAILED (ResourceExhausted) | 0.20 | 0.30 | 0.22 | 1 |
""",
            encoding="utf-8",
        )
        parsed = _parse_report(path)
        assert parsed["summary"]["rpc_count"] == 2, parsed
        assert parsed["summary"]["failed_rpc_count"] == 2, parsed
        assert {row["err_code"] for row in parsed["failed_rpcs"]} == {"RESOURCE_EXHAUSTED"}, parsed

        path.write_text(
            """# UDB SDK Live Perf

RPCs measured: 3

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | kind | err | p50 ms | p99 ms | mean ms | iters |
|---|---|---|---|--:|--:|--:|--:|
| RoomService | StartRoomComposite | mutation | CAPABILITY_SKIPPED | 0.20 | 0.30 | 0.22 | 1 |
| BackupService | GetBackup | read_only | SEED_BLOCKED | 0.00 | 0.00 | 0.00 | 0 |
| VaultService | Encrypt | mutation | SKIP_NO_BODY | 0.20 | 0.30 | 0.22 | 1 |
""",
            encoding="utf-8",
        )
        parsed = _parse_report(path)
        assert parsed["summary"]["rpc_count"] == 3, parsed
        assert parsed["summary"]["failed_rpc_count"] == 2, parsed
        assert {row["err_code"] for row in parsed["failed_rpcs"]} == {
            "SEED_BLOCKED",
            "SKIP_NO_BODY",
        }, parsed

        # The Go harness spells the missing-body status `NO-BODY`; it must alias
        # onto the canonical `SKIP_NO_BODY` (a countable failure) rather than
        # crashing the whole per-SDK parse (which silently marked the SDK failed
        # with a null rpc_count).
        path.write_text(
            """# UDB SDK Live Perf

RPCs measured: 2

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | kind | err | p50 ms | p99 ms | mean ms | iters |
|---|---|---|---|--:|--:|--:|--:|
| DataBroker | Select | read_only | OK | 0.20 | 0.30 | 0.22 | 25 |
| AuthnService | TransferServiceAccountGrant | destructive | NO-BODY | 0.06 | 0.06 | 0.06 | 1 |
""",
            encoding="utf-8",
        )
        parsed = _parse_report(path)
        assert parsed["summary"]["failed_rpc_count"] == 1, parsed
        assert parsed["failed_rpcs"][0]["err_code"] == "SKIP_NO_BODY", parsed

        path.write_text(
            """# UDB SDK Live Perf

RPCs measured: 1

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | kind | err | p50 ms | p99 ms | mean ms | iters |
|---|---|---|---|--:|--:|--:|--:|
| DataBroker | Select | read_only | ResrcExhausted | 0.20 | 0.30 | 0.22 | 1 |
""",
            encoding="utf-8",
        )
        try:
            _parse_report(path)
            raise AssertionError("unknown benchmark status token regression was not caught")
        except ValueError as exc:
            assert "not a canonical gRPC status" in str(exc), exc

        # Bare RPC-name collisions across services must not collapse into one
        # failure. This mirrors native services that share names such as List/Get.
        path.write_text(
            """# UDB SDK Live Perf

RPCs measured: 2

## Failures (2)

| RPC | kind | err | p99 ms |
|---|---|---|--:|
| RoomService/GetRoom | read_only | RESOURCE_EXHAUSTED | 0.25 |
| TenantService/GetRoom | read_only | FAILED_PRECONDITION | 0.30 |
""",
            encoding="utf-8",
        )
        parsed = _parse_report(path)
        assert parsed["summary"]["failed_rpc_count"] == 2, parsed

        canonical_rpcs = {
            "AlphaService/GetAlpha": ("get_alpha", "getAlpha"),
            "BetaService/PutBeta": ("put_beta", "putBeta"),
        }
        contract = _benchmark_contract(canonical_rpcs, "a" * 64)

        def complete_payload() -> dict[str, Any]:
            measured = [
                {
                    "id": sdk_id,
                    "name": SDK_NAMES[sdk_id],
                    "status": "ok",
                    "summary": {"rpc_count": 2, "failed_rpc_count": 0},
                    "full_rpcs": [
                        {
                            "wire_api": rpc,
                            "api_alias": canonical_rpcs[rpc][0],
                            "operation_id": canonical_rpcs[rpc][1],
                            "err_code": None,
                        }
                        for rpc in canonical_rpcs
                    ],
                }
                for sdk_id in MEASURED_SDK_IDS
            ]
            skipped = [
                {
                    "id": sdk_id,
                    "name": SDK_NAMES[sdk_id],
                    "status": "skipped",
                    "summary": {"rpc_count": None, "failed_rpc_count": 0},
                    "full_rpcs": [],
                }
                for sdk_id in SKIPPED_SDK_IDS
            ]
            return {
                "benchmark_contract": contract,
                "summary": {
                    "sdk_count": len(MEASURED_SDK_IDS) + len(SKIPPED_SDK_IDS),
                    "ok": len(MEASURED_SDK_IDS),
                    "failed": 0,
                    "skipped": len(SKIPPED_SDK_IDS),
                    "canonical_rpc_count": 2,
                    "measured_sdk_count": len(MEASURED_SDK_IDS),
                    "expected_measured_rpc_count": 2 * len(MEASURED_SDK_IDS),
                    "measured_rpc_count": 2 * len(MEASURED_SDK_IDS),
                    "failed_rpc_count": 0,
                },
                "sdks": measured + skipped,
            }

        def cloned_payload() -> dict[str, Any]:
            return json.loads(json.dumps(complete_payload()))

        assert _benchmark_gate_failures(complete_payload(), contract, canonical_rpcs) == []

        missing_rpc = cloned_payload()
        missing_rpc["sdks"][0]["full_rpcs"].pop()
        failures = _benchmark_gate_failures(missing_rpc, contract, canonical_rpcs)
        assert any("full_rpcs has 1 rows" in failure for failure in failures), failures
        assert any("missing RPCs: BetaService/PutBeta" in failure for failure in failures), failures

        extra_rpc = cloned_payload()
        extra_rpc["sdks"][0]["full_rpcs"].append({"wire_api": "GammaService/GetGamma"})
        failures = _benchmark_gate_failures(extra_rpc, contract, canonical_rpcs)
        assert any("unexpected RPCs: GammaService/GetGamma" in failure for failure in failures), failures

        duplicate_rpc = cloned_payload()
        duplicate_rpc["sdks"][0]["full_rpcs"][1] = {
            "wire_api": "AlphaService/GetAlpha",
            "api_alias": "get_alpha",
            "operation_id": "getAlpha",
            "err_code": None,
        }
        failures = _benchmark_gate_failures(duplicate_rpc, contract, canonical_rpcs)
        assert any("duplicate RPCs: AlphaService/GetAlpha" in failure for failure in failures), failures
        assert any("missing RPCs: BetaService/PutBeta" in failure for failure in failures), failures

        header_mismatch = cloned_payload()
        header_mismatch["sdks"][0]["summary"]["rpc_count"] = 1
        failures = _benchmark_gate_failures(header_mismatch, contract, canonical_rpcs)
        assert any("rpc_count=1, expected 2" in failure for failure in failures), failures

        failed_row_zero_summary = cloned_payload()
        failed_row_zero_summary["sdks"][0]["full_rpcs"][0]["err"] = "FAILED (ResourceExhausted)"
        failures = _benchmark_gate_failures(failed_row_zero_summary, contract, canonical_rpcs)
        assert any("failed/invalid full_rpcs rows" in failure for failure in failures), failures

        missing_identity = cloned_payload()
        del missing_identity["sdks"][0]["full_rpcs"][0]["wire_api"]
        failures = _benchmark_gate_failures(missing_identity, contract, canonical_rpcs)
        assert any("rows without wire_api" in failure for failure in failures), failures

        alias_mismatch = cloned_payload()
        alias_mismatch["sdks"][0]["full_rpcs"][0]["api_alias"] = "wrong_alias"
        failures = _benchmark_gate_failures(alias_mismatch, contract, canonical_rpcs)
        assert any("api_alias='wrong_alias'" in failure for failure in failures), failures

        operation_mismatch = cloned_payload()
        operation_mismatch["sdks"][0]["full_rpcs"][0]["operation_id"] = "wrongOperation"
        failures = _benchmark_gate_failures(operation_mismatch, contract, canonical_rpcs)
        assert any("operation_id='wrongOperation'" in failure for failure in failures), failures

        missing_sdk = cloned_payload()
        missing_sdk["sdks"] = [sdk for sdk in missing_sdk["sdks"] if sdk["id"] != "php"]
        failures = _benchmark_gate_failures(missing_sdk, contract, canonical_rpcs)
        assert any("missing SDK entries: php" in failure for failure in failures), failures

        extra_sdk = cloned_payload()
        extra_sdk["sdks"].append({
            "id": "ruby",
            "name": "Ruby",
            "status": "ok",
            "summary": {"rpc_count": 2, "failed_rpc_count": 0},
            "full_rpcs": [
                {
                    "wire_api": rpc,
                    "api_alias": canonical_rpcs[rpc][0],
                    "operation_id": canonical_rpcs[rpc][1],
                    "err_code": None,
                }
                for rpc in canonical_rpcs
            ],
        })
        failures = _benchmark_gate_failures(extra_sdk, contract, canonical_rpcs)
        assert any("unexpected SDK entries: ruby" in failure for failure in failures), failures

        duplicate_sdk = cloned_payload()
        duplicate_sdk["sdks"].append(duplicate_sdk["sdks"][0])
        failures = _benchmark_gate_failures(duplicate_sdk, contract, canonical_rpcs)
        assert any("duplicate SDK entries: go" in failure for failure in failures), failures

        aggregate_tamper = cloned_payload()
        aggregate_tamper["summary"]["measured_rpc_count"] -= 1
        failures = _benchmark_gate_failures(aggregate_tamper, contract, canonical_rpcs)
        assert any("summary.measured_rpc_count" in failure for failure in failures), failures

        illegal_measured_skip = cloned_payload()
        illegal_measured_skip["sdks"][0]["status"] = "skipped"
        failures = _benchmark_gate_failures(illegal_measured_skip, contract, canonical_rpcs)
        assert any("measured SDK go must have status=ok" in failure for failure in failures), failures

        illegal_static_measurement = cloned_payload()
        java = next(sdk for sdk in illegal_static_measurement["sdks"] if sdk["id"] == "java")
        java["status"] = "ok"
        failures = _benchmark_gate_failures(illegal_static_measurement, contract, canonical_rpcs)
        assert any("non-measured SDK java must remain an explicit skip" in failure for failure in failures), failures

        contract_tamper = cloned_payload()
        contract_tamper["benchmark_contract"]["canonical_rpc_count"] = 1
        failures = _benchmark_gate_failures(contract_tamper, contract, canonical_rpcs)
        assert any("benchmark_contract does not match" in failure for failure in failures), failures

        manifest = Path(tmp) / "bench-bodies.json"
        manifest.write_text(
            json.dumps([
                {
                    "service": "AlphaService",
                    "rpc": "GetAlpha",
                    "wire_rpc": "AlphaService/GetAlpha",
                    "api_alias": "get_alpha",
                    "operation_id": "getAlpha",
                },
                {
                    "service": "BetaService",
                    "rpc": "PutBeta",
                    "wire_rpc": "BetaService/PutBeta",
                    "api_alias": "put_beta",
                    "operation_id": "putBeta",
                },
            ]),
            encoding="utf-8",
        )
        loaded_contract, loaded_rpcs = _load_canonical_rpc_contract(manifest)
        assert loaded_rpcs == canonical_rpcs, loaded_rpcs
        assert loaded_contract["canonical_rpc_count"] == len(canonical_rpcs), loaded_contract
        assert loaded_contract["expected_measured_rpc_count"] == len(canonical_rpcs) * len(MEASURED_SDK_IDS)

        manifest.write_text(
            json.dumps([
                {
                    "service": "CacheService",
                    "rpc": "CacheService.Delete",
                    "wire_rpc": "CacheService/Delete",
                    "api_alias": "cache_delete",
                    "operation_id": "cacheDelete",
                },
            ]),
            encoding="utf-8",
        )
        _, qualified_rpcs = _load_canonical_rpc_contract(manifest)
        assert qualified_rpcs == {
            "CacheService/Delete": ("cache_delete", "cacheDelete")
        }, qualified_rpcs

        manifest.write_text(
            json.dumps([
                {
                    "service": "AlphaService",
                    "rpc": "GetAlpha",
                    "wire_rpc": "AlphaService/GetAlpha",
                    "api_alias": "get_alpha",
                    "operation_id": "getAlpha",
                },
                {
                    "service": "AlphaService",
                    "rpc": "GetAlpha",
                    "wire_rpc": "AlphaService/GetAlpha",
                    "api_alias": "get_alpha",
                    "operation_id": "getAlpha",
                },
            ]),
            encoding="utf-8",
        )
        try:
            _load_canonical_rpc_contract(manifest)
            raise AssertionError("duplicate canonical RPC identity regression was not caught")
        except ValueError as exc:
            assert "duplicate wire identities" in str(exc), exc

    print("collect_sdk_bench_results selftest passed")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true", help="run parser/gate regression fixtures and exit")
    ap.add_argument("--out", default="docs/site/bench-results.json")
    ap.add_argument("--status-dir", default="bench-output/status")
    ap.add_argument("--release-tag", default=os.getenv("UDB_BENCH_RELEASE_TAG", ""))
    ap.add_argument("--release-asset", default=os.getenv("UDB_BENCH_RELEASE_ASSET", ""))
    ap.add_argument("--release-url", default=os.getenv("UDB_BENCH_RELEASE_URL", ""))
    ap.add_argument("--release-sha256", default=os.getenv("UDB_BENCH_BINARY_SHA256", ""))
    ap.add_argument("--previous", default="", help="previous bench-results.json to append history from")
    ap.add_argument("--gate", default="", help="fail if an existing bench-results.json has bad SDKs or failed RPCs")
    ap.add_argument(
        "--canonical-manifest",
        default=str(CANONICAL_RPC_MANIFEST.relative_to(ROOT)),
        help="canonical generated benchmark RPC manifest used for completeness validation",
    )
    args = ap.parse_args()
    if args.selftest:
        return _selftest()
    canonical_manifest = Path(args.canonical_manifest)
    if not canonical_manifest.is_absolute():
        canonical_manifest = ROOT / canonical_manifest
    try:
        benchmark_contract, canonical_rpcs = _load_canonical_rpc_contract(canonical_manifest)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        return 1
    if args.gate:
        return _gate_results(Path(args.gate), benchmark_contract, canonical_rpcs)

    status_dir = (ROOT / args.status_dir).resolve()
    sdks: list[dict[str, Any]] = []

    for sdk, path in SDK_REPORTS.items():
        status = _read_status(status_dir, sdk)
        exit_code = status.get("exit_code")
        report_exists = path.is_file()
        entry: dict[str, Any] = {
            "id": sdk,
            "name": SDK_NAMES[sdk],
            "status": "ok" if exit_code == 0 and report_exists else "failed",
            "exit_code": exit_code,
        }
        if report_exists:
            try:
                entry.update(_parse_report(path))
                if entry.get("harness_error"):
                    entry["note"] = entry["harness_error"]
            except ValueError as exc:
                entry["status"] = "failed"
                entry["note"] = f"Benchmark report parse failed: {exc}"
                entry["summary"] = {
                    "rpc_count": None,
                    "service_count": 0,
                    "slowest_count": 0,
                    "failed_rpc_count": 0,
                }
                entry["services"] = []
                entry["slowest"] = []
                entry["failed_rpcs"] = []
                entry["full_rpcs"] = []
                try:
                    entry["report_path"] = str(path.relative_to(ROOT)).replace("\\", "/")
                except ValueError:
                    entry["report_path"] = str(path)
        else:
            entry["status"] = "failed" if exit_code not in (None, 0) else "missing"
            entry["note"] = "Benchmark command did not produce a Markdown report."
        if status.get("note"):
            entry["note"] = status["note"]
        sdks.append(entry)

    for sdk, note in MISSING_HARNESS.items():
        sdks.append({
            "id": sdk,
            "name": SDK_NAMES[sdk],
            "status": "skipped",
            "exit_code": None,
            "note": note,
            "summary": {"rpc_count": None, "service_count": 0, "slowest_count": 0, "failed_rpc_count": 0},
            "services": [],
            "slowest": [],
            "failed_rpcs": [],
            "full_rpcs": [],
        })

    ok = sum(1 for s in sdks if s["status"] == "ok")
    failed = sum(1 for s in sdks if s["status"] == "failed")
    skipped = sum(1 for s in sdks if s["status"] == "skipped")

    run_point = {
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "release_tag": args.release_tag,
        "release_sha256": args.release_sha256,
        "short_commit": _cmd(["git", "rev-parse", "--short", "HEAD"]),
        "sdks": [
            {
                "id": s["id"],
                "status": s["status"],
                "rpc_count": s.get("summary", {}).get("rpc_count"),
                "failed_rpc_count": s.get("summary", {}).get("failed_rpc_count", 0),
                "mean_service_latency_ms": s.get("summary", {}).get("mean_service_latency_ms"),
                "slowest_service_mean_ms": s.get("summary", {}).get("slowest_service_mean_ms"),
            }
            for s in sdks
        ],
    }

    history: list[dict[str, Any]] = []
    previous = Path(args.previous) if args.previous else None
    if previous and previous.is_file():
        try:
            prev = json.loads(previous.read_text(encoding="utf-8"))
            if isinstance(prev.get("history"), list):
                history = prev["history"][-24:]
            elif prev.get("generated_at"):
                history = [{
                    "generated_at": prev.get("generated_at"),
                    "release_tag": prev.get("release", {}).get("tag"),
                    "release_sha256": prev.get("release", {}).get("sha256"),
                    "short_commit": prev.get("git", {}).get("short_commit"),
                    "sdks": [
                        {
                            "id": s.get("id"),
                            "status": s.get("status"),
                            "rpc_count": s.get("summary", {}).get("rpc_count"),
                            "failed_rpc_count": s.get("summary", {}).get("failed_rpc_count", 0),
                            "mean_service_latency_ms": s.get("summary", {}).get("mean_service_latency_ms"),
                            "slowest_service_mean_ms": s.get("summary", {}).get("slowest_service_mean_ms"),
                        }
                        for s in prev.get("sdks", [])
                    ],
                }]
        except Exception as exc:
            print(f"warning: could not read previous history: {exc}")
    history.append(run_point)

    payload = {
        "schema_version": 1,
        "generated_at": run_point["generated_at"],
        "source": "github-actions",
        "benchmark_contract": benchmark_contract,
        "release": {
            "tag": args.release_tag,
            "asset": args.release_asset,
            "url": args.release_url,
            "sha256": args.release_sha256,
        },
        "git": {
            "commit": _cmd(["git", "rev-parse", "HEAD"]),
            "short_commit": _cmd(["git", "rev-parse", "--short", "HEAD"]),
            "branch": os.getenv("GITHUB_REF_NAME") or _cmd(["git", "branch", "--show-current"]),
        },
        "environment": {
            "runner_os": os.getenv("RUNNER_OS", ""),
            "run_id": os.getenv("GITHUB_RUN_ID", ""),
            "run_attempt": os.getenv("GITHUB_RUN_ATTEMPT", ""),
            "workflow": os.getenv("GITHUB_WORKFLOW", ""),
        },
        "summary": {
            "sdk_count": len(sdks),
            "ok": ok,
            "failed": failed,
            "skipped": skipped,
            "canonical_rpc_count": benchmark_contract["canonical_rpc_count"],
            "measured_sdk_count": len(MEASURED_SDK_IDS),
            "expected_measured_rpc_count": benchmark_contract["expected_measured_rpc_count"],
            "measured_rpc_count": sum((s.get("summary", {}).get("rpc_count") or 0) for s in sdks),
            "failed_rpc_count": sum((s.get("summary", {}).get("failed_rpc_count") or 0) for s in sdks),
        },
        "history": history[-25:],
        "sdks": sdks,
    }

    out = (ROOT / args.out).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    try:
        display_out = out.relative_to(ROOT)
    except ValueError:
        display_out = out
    print(f"wrote {display_out}")
    print(json.dumps(payload["summary"], indent=2))
    # This is the REPORTING step: per-SDK and per-RPC failures are DATA and must
    # be written before the workflow decides pass/fail. The benchmark workflow
    # uploads this JSON first, then a final gate fails the job on any bad SDK or
    # failed RPC so the debug artifact is still available. Only a TOTAL collection
    # breakdown is fatal here.
    if ok == 0 and skipped < len(sdks):
        print("ERROR: no SDK produced benchmark data (total collection failure)")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
