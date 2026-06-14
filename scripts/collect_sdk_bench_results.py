#!/usr/bin/env python3
"""Collect SDK live benchmark Markdown reports into one Pages JSON artifact."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
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


def _parse_report(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    measured = None
    m = re.search(r"RPCs measured:\s*([0-9]+)", text)
    if m:
        measured = int(m.group(1))

    services: list[dict[str, Any]] = []
    slowest: list[dict[str, Any]] = []
    section = ""
    headers: list[str] = []

    for line in lines:
        low = line.lower()
        if low.startswith("## per-service mean"):
            section = "services"
            headers = []
            continue
        if low.startswith("## slowest"):
            section = "slowest"
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
            slowest.append({
                "rpc": rpc,
                "kind": row.get("kind", ""),
                "p50_ms": _duration_ms(row.get("p50_ms") or row.get("p50") or ""),
                "p99_ms": _duration_ms(row.get("p99_ms") or row.get("p99") or ""),
                "mean_ms": _duration_ms(row.get("mean_ms") or row.get("mean") or ""),
            })

    service_means = [s["mean_ms"] for s in services if isinstance(s.get("mean_ms"), (int, float))]
    summary: dict[str, Any] = {
        "rpc_count": measured,
        "service_count": len(services),
        "slowest_count": len(slowest),
    }
    if service_means:
        summary["mean_service_latency_ms"] = sum(service_means) / len(service_means)
        summary["slowest_service_mean_ms"] = max(service_means)

    return {
        "summary": summary,
        "services": services,
        "slowest": slowest,
        "report_path": str(path.relative_to(ROOT)).replace("\\", "/"),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="docs/site/bench-results.json")
    ap.add_argument("--status-dir", default="bench-output/status")
    ap.add_argument("--release-tag", default=os.getenv("UDB_BENCH_RELEASE_TAG", ""))
    ap.add_argument("--release-asset", default=os.getenv("UDB_BENCH_RELEASE_ASSET", ""))
    ap.add_argument("--release-url", default=os.getenv("UDB_BENCH_RELEASE_URL", ""))
    ap.add_argument("--previous", default="", help="previous bench-results.json to append history from")
    args = ap.parse_args()

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
            entry.update(_parse_report(path))
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
            "summary": {"rpc_count": None, "service_count": 0, "slowest_count": 0},
            "services": [],
            "slowest": [],
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
        },
        "history": history[-25:],
        "sdks": sdks,
    }

    out = (ROOT / args.out).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out.relative_to(ROOT)}")
    print(json.dumps(payload["summary"], indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
