#!/usr/bin/env python3
"""Collect SDK live benchmark Markdown reports into one Pages JSON artifact."""

from __future__ import annotations

import argparse
import datetime as dt
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

    # An err cell of "OK"/""/"-" means the RPC succeeded; anything else is a non-OK
    # gRPC status code (UNAVAILABLE, FAILED_PRECONDITION, …) and counts as a failure.
    def _norm_err(value: str | None) -> str | None:
        raw = (value or "").strip()
        if raw in {"", "-", "OK", "ok"}:
            return None
        status = _canonical_grpc_status(raw)
        if status in NON_FATAL_HARNESS_STATUS_CODES:
            return None
        return status

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
                "err_code": _norm_err(row.get("err")),
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
                "err_code": _norm_err(row.get("err")) or "UNKNOWN",
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
                "err_code": _norm_err(row.get("err")),
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


def _benchmark_gate_failures(payload: dict[str, Any]) -> list[str]:
    summary = payload.get("summary", {})
    bad_sdks = [
        s.get("name") or s.get("id")
        for s in payload.get("sdks", [])
        if s.get("status") not in {"ok", "skipped"}
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
    return failures


def _gate_results(path: Path) -> int:
    target = path if path.is_absolute() else ROOT / path
    if not target.is_file():
        print(f"ERROR: benchmark results JSON was not produced: {target}")
        return 1
    payload = json.loads(target.read_text(encoding="utf-8"))
    failures = _benchmark_gate_failures(payload)
    if failures:
        print(f"Benchmark gate failed: {', '.join(failures)}")
        return 1
    print("Benchmark gate passed: no failed SDKs and no failed RPCs.")
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
        assert parsed["summary"]["failed_rpc_count"] == 2, parsed
        assert {row["err_code"] for row in parsed["failed_rpcs"]} == {"RESOURCE_EXHAUSTED"}, parsed

        path.write_text(
            """# UDB SDK Live Perf

RPCs measured: 2

## Full per-RPC table (sorted by service, then RPC)

| Service | RPC | kind | err | p50 ms | p99 ms | mean ms | iters |
|---|---|---|---|--:|--:|--:|--:|
| RoomService | StartRoomComposite | mutation | CAPABILITY_SKIPPED | 0.20 | 0.30 | 0.22 | 1 |
| VaultService | Encrypt | mutation | SKIP_NO_BODY | 0.20 | 0.30 | 0.22 | 1 |
""",
            encoding="utf-8",
        )
        parsed = _parse_report(path)
        assert parsed["summary"]["failed_rpc_count"] == 1, parsed
        assert {row["err_code"] for row in parsed["failed_rpcs"]} == {"SKIP_NO_BODY"}, parsed

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

        assert _benchmark_gate_failures({
            "summary": {"failed_rpc_count": 0},
            "sdks": [{"name": "Go", "status": "ok"}, {"name": "Java", "status": "skipped"}],
        }) == []
        assert _benchmark_gate_failures({
            "summary": {"failed_rpc_count": 3},
            "sdks": [{"name": "Go", "status": "ok"}],
        }) == ["failed_rpc_count=3"]
        assert _benchmark_gate_failures({
            "summary": {"failed_rpc_count": 0},
            "sdks": [{"name": "PHP", "status": "failed"}],
        }) == ["bad_sdks=['PHP']"]

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
    ap.add_argument("--previous", default="", help="previous bench-results.json to append history from")
    ap.add_argument("--gate", default="", help="fail if an existing bench-results.json has bad SDKs or failed RPCs")
    args = ap.parse_args()
    if args.selftest:
        return _selftest()
    if args.gate:
        return _gate_results(Path(args.gate))

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
        })

    ok = sum(1 for s in sdks if s["status"] == "ok")
    failed = sum(1 for s in sdks if s["status"] == "failed")
    skipped = sum(1 for s in sdks if s["status"] == "skipped")

    run_point = {
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat(),
        "release_tag": args.release_tag,
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
        "release": {
            "tag": args.release_tag,
            "asset": args.release_asset,
            "url": args.release_url,
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
