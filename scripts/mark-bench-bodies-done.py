#!/usr/bin/env python3
"""Tick the `done` column of docs/bench-bodies/*.md from benchmark evidence.

A row is `[x]` only when the live SDK benchmark measured that RPC OK in EVERY SDK
of the given report; otherwise it is `[ ]`. The column used to be ticked by hand
and was never ticked at all, so all 426 rows read as unfinished while the benchmark
measured 420 of them clean. Run it with the `bench-results.json` of a finished
`Benchmark · SDKs` run (the `sdk-benchmark-results` artifact):

    python scripts/mark-bench-bodies-done.py --results path/to/bench-results.json
    python scripts/mark-bench-bodies-done.py --results ... --check   # exit 1 on drift
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BODIES = ROOT / "docs" / "bench-bodies"
ROW = re.compile(r"^\| \[(?P<mark>[ x])\] \| (?P<rpc>[A-Za-z0-9_.]+) \|")
HEADING = re.compile(r"^## (?P<service>[A-Za-z0-9_]+)\s*$")


def measured_ok(results: dict) -> tuple[set[tuple[str, str]], str]:
    """(service, rpc) pairs every SDK in the report measured OK."""
    sdks = results.get("sdks") or []
    if not sdks:
        raise SystemExit("the report lists no SDKs")
    ok: set[tuple[str, str]] | None = None
    for sdk in sdks:
        here = {
            (row["service"], row["rpc"])
            for row in sdk.get("full_rpcs") or []
            if row.get("result_status") == "ok"
        }
        ok = here if ok is None else ok & here
    tag = (results.get("release") or {}).get("tag", "?")
    return ok or set(), f"{tag} ({len(sdks)} SDKs)"


def mark(text: str, ok: set[tuple[str, str]]) -> tuple[str, int, list[str]]:
    service = ""
    out, ticked, open_rows = [], 0, []
    for line in text.split("\n"):
        heading = HEADING.match(line)
        if heading:
            service = heading.group("service")
        row = ROW.match(line)
        # A row names `Service.Rpc` where an RPC name repeats across services (or the
        # file's heading is not a single service), and needs no heading then.
        if row and (service or "." in row.group("rpc")):
            row_service, _, rpc = row.group("rpc").rpartition(".")
            key = (row_service or service, rpc)
            done = key in ok
            ticked += done
            if not done:
                open_rows.append(f"{key[0]}/{key[1]}")
            line = f"| [{'x' if done else ' '}]" + line[len("| [ ]"):]
        out.append(line)
    return "\n".join(out), ticked, open_rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--results", required=True, help="bench-results.json of a finished run")
    parser.add_argument("--check", action="store_true", help="report drift instead of writing")
    args = parser.parse_args()
    ok, label = measured_ok(json.loads(Path(args.results).read_text(encoding="utf-8")))
    drift, ticked_total, open_total = [], 0, []
    for path in sorted(BODIES.glob("*.md")):
        before = path.read_text(encoding="utf-8")
        after, ticked, open_rows = mark(before, ok)
        ticked_total += ticked
        open_total += open_rows
        if after != before:
            drift.append(path.name)
            if not args.check:
                path.write_text(after, encoding="utf-8", newline="\n")
    print(f"evidence: {label}; {ticked_total} rows measured OK in every SDK, {len(open_total)} open")
    for row in open_total:
        print(f"  open: {row}")
    if args.check and drift:
        print("done column differs from the evidence in: " + ", ".join(drift))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
