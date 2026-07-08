#!/usr/bin/env python3
"""Gate native-load smoke output against tracked p99 baselines.

`scripts/native-load-test.sh` uses ghz against the real broker/native service
surface. Some smoke cases intentionally exercise placeholder-id fast-reject
paths, so raw ghz exit status is not the performance contract. This gate reads
the captured ghz summary output, requires every tracked scenario to report a
p99 latency, and fails when p99 regresses beyond the configured percentage.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile


CASE_RE = re.compile(r"^==\s*(?P<name>.+?)\s*==\s*$")
P99_RE = re.compile(
    r"^\s*99(?:\.0+)?%\s+(?:in\s+)?(?P<value>[0-9]+(?:\.[0-9]+)?)\s*(?P<unit>[a-zA-Z\u00b5\u03bc]+)\s*$"
)
UNIT_TO_MS = {
    "ns": 0.000001,
    "us": 0.001,
    "\u00b5s": 0.001,
    "\u03bcs": 0.001,
    "ms": 1.0,
    "s": 1000.0,
}


def _unit_to_ms(value: str, unit: str) -> float:
    factor = UNIT_TO_MS.get(unit.lower())
    if factor is None:
        raise ValueError(f"unsupported latency unit `{unit}`")
    return float(value) * factor


def parse_native_load_output(path: str) -> dict[str, float]:
    """Return {case_name: p99_ms} parsed from ghz summary sections."""
    results: dict[str, float] = {}
    current: str | None = None
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            case = CASE_RE.match(line)
            if case:
                current = case.group("name")
                continue
            if current is None:
                continue
            p99 = P99_RE.match(line)
            if p99:
                results[current] = _unit_to_ms(p99.group("value"), p99.group("unit"))
    return results


def load_baseline(path: str) -> dict[str, float]:
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    cases = data.get("cases")
    if not isinstance(cases, dict) or not cases:
        raise SystemExit(f"{path}: missing non-empty `cases` object")

    baseline: dict[str, float] = {}
    for name, entry in cases.items():
        if not isinstance(entry, dict) or "p99_ms" not in entry:
            raise SystemExit(f"{path}: case `{name}` missing `p99_ms`")
        baseline[name] = float(entry["p99_ms"])
    return baseline


def run_gate(output_path: str, baseline_path: str, max_regression: float) -> int:
    measured = parse_native_load_output(output_path)
    baseline = load_baseline(baseline_path)

    missing = sorted(name for name in baseline if name not in measured)
    unexpected = sorted(name for name in measured if name not in baseline)
    if missing or unexpected:
        if missing:
            print("missing p99 measurements:")
            for name in missing:
                print(f"  - {name}")
        if unexpected:
            print("unexpected native-load cases not in baseline:")
            for name in unexpected:
                print(f"  - {name}")
        return 2

    print(
        f"native load p99 gate - {os.path.basename(baseline_path)} - "
        f"fail if any case regressed > {max_regression:g}%"
    )
    violations: list[str] = []
    for name in sorted(baseline):
        base = baseline[name]
        cur = measured[name]
        allowed = base * (1.0 + max_regression / 100.0)
        delta = ((cur - base) / base * 100.0) if base else 0.0
        flag = "OK " if cur <= allowed else "FAIL"
        print(f"  [{flag}] {name}: baseline {base:.3f} ms -> p99 {cur:.3f} ms ({delta:+.1f}%)")
        if cur > allowed:
            violations.append(
                f"{name}: p99 {cur:.3f} ms exceeds {allowed:.3f} ms "
                f"(baseline {base:.3f} ms + {max_regression:g}%)"
            )

    if violations:
        print("\nNATIVE LOAD P99 REGRESSIONS:")
        for violation in violations:
            print(f"  - {violation}")
        return 1
    print("\nall native-load p99 measurements are within the regression threshold.")
    return 0


def _selftest() -> int:
    sample = """== storage register upload ==
Summary:
  Count: 20
Latency distribution:
  95% in 10.0 ms
  99% in 11.0 ms
Status code distribution:
  [OK] 20 responses
== webrtc signal fan-out ==
Summary:
  Count: 20
Latency distribution:
  99% in 1.1 s
"""
    with tempfile.TemporaryDirectory() as tmp:
        output = os.path.join(tmp, "native-load.txt")
        baseline = os.path.join(tmp, "baseline.json")
        with open(output, "w", encoding="utf-8") as fh:
            fh.write(sample)
        with open(baseline, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "cases": {
                        "storage register upload": {"p99_ms": 10.0},
                        "webrtc signal fan-out": {"p99_ms": 1000.0},
                    }
                },
                fh,
            )
        got = run_gate(output, baseline, 15.0)
        if got != 0:
            print(f"selftest expected pass, got {got}", file=sys.stderr)
            return 1

        with open(baseline, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "cases": {
                        "storage register upload": {"p99_ms": 1.0},
                        "webrtc signal fan-out": {"p99_ms": 1000.0},
                    }
                },
                fh,
            )
        got = run_gate(output, baseline, 15.0)
        if got != 1:
            print(f"selftest expected p99 regression error, got {got}", file=sys.stderr)
            return 1

        with open(baseline, "w", encoding="utf-8") as fh:
            json.dump({"cases": {"storage register upload": {"p99_ms": 5.0}}}, fh)
        got = run_gate(output, baseline, 15.0)
        if got != 2:
            print(f"selftest expected missing/unexpected error, got {got}", file=sys.stderr)
            return 1

        print("selftest: ALL PASS")
        return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="/tmp/native-load.txt", help="captured native-load ghz output")
    ap.add_argument(
        "--baseline",
        default=os.path.join("scripts", "native_load_smoke_baseline.json"),
        help="tracked native-load baseline JSON",
    )
    ap.add_argument("--max-regression", type=float, default=15.0, help="max p99 regression percent")
    ap.add_argument("--selftest", action="store_true", help="run inline parser/gate fixtures")
    args = ap.parse_args()

    if args.selftest:
        return _selftest()
    return run_gate(args.input, args.baseline, args.max_regression)


if __name__ == "__main__":
    raise SystemExit(main())
