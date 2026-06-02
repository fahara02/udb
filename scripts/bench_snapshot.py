#!/usr/bin/env python3
"""Persist a timestamped benchmark snapshot + compare to the previous run (D.2).

Criterion's own baselines live under `target/` (wiped by `cargo clean`, git-
ignored). This harvests the current results and writes a DURABLE, TRACKED
snapshot to `bench-history/bench-<UTC>.json`, then prints (and writes a `.md`) a
comparison against the most recent prior snapshot — flagging improvements vs
regressions. That's the before/after feedback loop for the D.3 / D.6 work.

Usage (after running the benches):
    python scripts/bench_snapshot.py --label "D.3 borrowed body parse"
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bench_charts import ROOT, collect, fmt_time  # noqa: E402

# Windows consoles default to cp1252; the comparison table uses ✅/⚠️/→/·.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HIST = os.path.join(ROOT, "bench-history")


def _cmd(args: list[str]) -> str | None:
    try:
        return subprocess.check_output(args, cwd=ROOT, text=True, stderr=subprocess.DEVNULL).strip()
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="", help="free-text note (e.g. the optimization under test)")
    args = ap.parse_args()

    rows = collect()
    if not rows:
        print("no Criterion results under target/criterion — run a bench first.")
        return 0

    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results = {}
    for r in rows:
        entry = {"median_ns": r["median_ns"]}
        if r["thrpt"]:
            entry["throughput_kind"], entry["throughput_per_s"] = r["thrpt"]
        results[f'{r["group"]}/{r["fn"]}'] = entry

    snap = {
        "timestamp": ts,
        "label": args.label,
        "git_commit": _cmd(["git", "rev-parse", "--short", "HEAD"]),
        "rustc": _cmd(["rustc", "--version"]),
        "os": sys.platform,
        "results": results,
    }
    os.makedirs(HIST, exist_ok=True)
    with open(os.path.join(HIST, f"bench-{ts}.json"), "w", encoding="utf-8") as fh:
        json.dump(snap, fh, indent=2)

    prior = sorted(
        f for f in os.listdir(HIST)
        if f.startswith("bench-") and f.endswith(".json") and f != f"bench-{ts}.json"
    )
    out = [
        f"# Benchmark snapshot `{ts}`",
        "",
        f"- commit `{snap['git_commit']}` · {snap['rustc']} · os `{snap['os']}`",
        f"- label: {args.label or '(none)'}",
        "",
    ]
    if prior:
        with open(os.path.join(HIST, prior[-1]), encoding="utf-8") as fh:
            old = json.load(fh)
        out += [
            f"## Δ vs `{old['timestamp']}` — median time (negative Δ = faster), throughput (positive Δ = faster)",
            "",
            "| benchmark | prev | now | Δ time | Δ thrpt |",
            "|---|---|---|---|---|",
        ]
        deltas = []
        for key, cur in sorted(results.items()):
            o = old["results"].get(key)
            if not o:
                out.append(f"| `{key}` | — | {fmt_time(cur['median_ns'])} | _new_ | |")
                continue
            dt = (cur["median_ns"] - o["median_ns"]) / o["median_ns"] * 100.0
            deltas.append((key, dt))
            tcol = ""
            if "throughput_per_s" in cur and "throughput_per_s" in o and o["throughput_per_s"]:
                dth = (cur["throughput_per_s"] - o["throughput_per_s"]) / o["throughput_per_s"] * 100.0
                tcol = f"{dth:+.1f}%"
            mark = " ✅" if dt <= -3.0 else (" ⚠️" if dt >= 3.0 else "")
            out.append(
                f"| `{key}` | {fmt_time(o['median_ns'])} | {fmt_time(cur['median_ns'])} | {dt:+.1f}%{mark} | {tcol} |"
            )
        # Noise floor: median |Δ| across ALL benches (incl. ones nothing changed)
        # estimates run-to-run variance. Treat individual deltas near it as noise,
        # not a real win/regression — honesty guard for the "numbers must sustain"
        # rule. A targeted change should move ITS bench well beyond this floor.
        if deltas:
            mags = sorted(abs(d) for _, d in deltas)
            noise = mags[len(mags) // 2]
            worst = max(deltas, key=lambda kv: abs(kv[1]))
            out += [
                "",
                f"_Noise floor ≈ **{noise:.1f}%** (median |Δ| across all {len(deltas)} benches; "
                f"largest unrelated swing: `{worst[0]}` {worst[1]:+.1f}%). Deltas within ±{noise:.1f}% "
                f"are run-to-run variance — re-run on a quiet machine / CI (D.8) for precise figures._",
            ]
    else:
        out += [
            "(first snapshot — no prior to compare; future runs diff against this one.)",
            "",
            "| benchmark | median |",
            "|---|---|",
        ]
        out += [f"| `{key}` | {fmt_time(cur['median_ns'])} |" for key, cur in sorted(results.items())]

    md = "\n".join(out) + "\n"
    with open(os.path.join(HIST, f"bench-{ts}.md"), "w", encoding="utf-8") as fh:
        fh.write(md)
    with open(os.path.join(HIST, "latest.md"), "w", encoding="utf-8") as fh:
        fh.write(md)
    print(md)
    print(f"snapshot → bench-history/bench-{ts}.json  (+ bench-{ts}.md, latest.md)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
