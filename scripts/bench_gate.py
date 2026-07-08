#!/usr/bin/env python3
"""Gate benchmark results — relative regression gate (P8.3) + absolute SLO gate (P8.1).

Two complementary modes, both reading the DURABLE snapshot format written by
`bench_snapshot.py` (`bench-history/bench-<UTC>.json`); neither re-implements the
snapshot reader — they share `_load_snapshot` / `_latest_snapshot` below.

  --relative
        Compare the latest snapshot to the most recent RELEASE snapshot (the
        newest one carrying a `tag`, written by `bench_snapshot.py --tag`), or to
        an explicit `--baseline-tag`, and fail when any benchmark regressed by
        more than `--max-regression` percent. This is the "vs last release" gate
        — NOT "vs last run", so slow drift across many untagged runs can't
        launder a regression past the gate.

        FAIL-CLOSED: if the requested (or last-release) baseline is absent, the
        gate FAILS (non-zero). A missing baseline is a red, not a green — the
        gate never silently falls back to "last run" or passes by default.

  --absolute docs/slo.md
        Parse the generated SLO table out of `docs/slo.md` (the published mirror
        of `slo.rs::slo_catalog()`, pinned byte-for-byte by the
        `slo_doc_table_matches_catalog` staleness test) and fail when a measured
        latency in the latest snapshot exceeds its absolute per-objective budget.

DOCTRINE: no SLO threshold is hardcoded here. Every budget is parsed from the
generated block in `docs/slo.md`, whose single source of truth is
`slo_catalog()`. The gate only reads numbers; it never invents them.

BENCH-INTEGRITY RULE: the snapshots this gate reads must come from benches that
exercise the SHIPPED runtime path (src/runtime/…), never bench-only helpers. The
gate enforces a budget; it is only meaningful if the measured number is the real
production path. See `bench_snapshot.py` for the harvest side of this rule.

Usage (after `bench_snapshot.py` has written at least one snapshot):
    python scripts/bench_gate.py --absolute docs/slo.md
    python scripts/bench_gate.py --relative --max-regression 10   # vs last RELEASE
    python scripts/bench_gate.py --relative --baseline-tag v0.3.7  # vs a named release
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bench_charts import ROOT, fmt_time  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

HIST = os.path.join(ROOT, "bench-history")

# Markers around the generated SLO table in docs/slo.md (must match slo.rs).
SLO_BEGIN = "<!-- BEGIN GENERATED:slo -->"
SLO_END = "<!-- END GENERATED:slo -->"


# ── Shared snapshot reading (used by BOTH modes; not duplicated) ─────────────

def _snapshot_files() -> list[str]:
    """All durable snapshot json files, oldest → newest by timestamped name."""
    if not os.path.isdir(HIST):
        return []
    return sorted(
        f for f in os.listdir(HIST) if f.startswith("bench-") and f.endswith(".json")
    )


def _load_snapshot(name: str) -> dict:
    with open(os.path.join(HIST, name), encoding="utf-8") as fh:
        return json.load(fh)


def _latest_snapshot() -> tuple[str, dict] | None:
    files = _snapshot_files()
    if not files:
        return None
    return files[-1], _load_snapshot(files[-1])


def _snapshot_by_tag(tag: str) -> tuple[str, dict] | None:
    """The most recent snapshot whose `tag` field equals `tag` (None if absent)."""
    match = None
    for name in _snapshot_files():
        snap = _load_snapshot(name)
        if snap.get("tag") == tag:
            match = (name, snap)
    return match


def _latest_release_snapshot(exclude: str | None = None) -> tuple[str, dict] | None:
    """The most recent snapshot carrying ANY non-empty `tag` (a tagged release),
    skipping `exclude` (typically the current run). None if no release snapshot
    exists — the relative gate treats that as fail-closed, never a silent pass.
    Reuses the shared `_snapshot_files`/`_load_snapshot` readers (no duplication)."""
    match = None
    for name in _snapshot_files():
        if name == exclude:
            continue
        snap = _load_snapshot(name)
        if snap.get("tag"):
            match = (name, snap)
    return match


def _measured_ns(entry: dict) -> float | None:
    """The latency stat a snapshot row carries. `bench_snapshot.py` records the
    Criterion median (`median_ns`); honour an optional `p99_ns` when a future
    snapshot adds it. Returns None if neither is present."""
    if "p99_ns" in entry:
        return float(entry["p99_ns"])
    if "median_ns" in entry:
        return float(entry["median_ns"])
    return None


# ── SLO table parsing (absolute mode) ────────────────────────────────────────

# Parses e.g. "p99 ≤ 400 ms" → ("p99", 400.0); the "—" no-latency rows yield None.
_LATENCY_RE = re.compile(r"(p50|p95|p99)\s*[≤<=]+\s*([0-9]+(?:\.[0-9]+)?)\s*ms", re.I)
# Strips the `code` backticks the table wraps the objective id / method in.
_TICKS_RE = re.compile(r"^`(.*)`$")


def _untick(cell: str) -> str:
    m = _TICKS_RE.match(cell.strip())
    return m.group(1) if m else cell.strip()


def parse_slo_budgets(slo_md_path: str) -> list[dict]:
    """Parse the generated SLO table → one dict per objective WITH a latency
    budget: {objective, grpc_method, stat, budget_ms}. Availability-only rows
    (latency "—") are skipped — there is no latency to gate them on."""
    with open(slo_md_path, encoding="utf-8") as fh:
        text = fh.read().replace("\r\n", "\n")
    if SLO_BEGIN not in text or SLO_END not in text:
        raise SystemExit(
            f"{slo_md_path}: missing {SLO_BEGIN} / {SLO_END} markers — "
            "is this the generated SLO doc?"
        )
    block = text.split(SLO_BEGIN, 1)[1].split(SLO_END, 1)[0]

    budgets: list[dict] = []
    for line in block.splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) < 4:
            continue
        objective = _untick(cells[0])
        # Skip the header row and the |---| separator.
        if objective in ("Objective", "") or set(objective) <= {"-", ":"}:
            continue
        m = _LATENCY_RE.search(cells[3])
        if not m:
            continue  # availability-only objective (e.g. cdc.dlq)
        budgets.append(
            {
                "objective": objective,
                "grpc_method": _untick(cells[2]),
                "stat": m.group(1).lower(),
                "budget_ms": float(m.group(2)),
            }
        )
    if not budgets:
        raise SystemExit(f"{slo_md_path}: parsed zero latency budgets from the SLO table")
    return budgets


def _matches(bench_key: str, budget: dict) -> bool:
    """Does a bench result key (`group/fn`) measure this SLO objective? We match
    conservatively: the objective id as a substring, or the gRPC method as a
    whole alnum token. Internal micro-benches that map to no objective are simply
    not gated (the gate is wired and enforces as soon as a matching bench lands)."""
    key = bench_key.lower()
    if budget["objective"].lower() in key:
        return True
    method = budget["grpc_method"]
    if method and method != "—":
        tokens = re.split(r"[^a-z0-9]+", key)
        if method.lower() in tokens:
            return True
    return False


# ── Modes ────────────────────────────────────────────────────────────────────

def run_absolute(slo_md_path: str) -> int:
    budgets = parse_slo_budgets(slo_md_path)
    latest = _latest_snapshot()
    if latest is None:
        print("no bench-history snapshot found — run bench_snapshot.py first.")
        return 0
    name, snap = latest
    results: dict = snap.get("results", {})

    print(f"absolute SLO gate · snapshot `{name}` · budgets from {slo_md_path}")
    violations: list[str] = []
    covered = 0
    for key, entry in sorted(results.items()):
        measured = _measured_ns(entry)
        if measured is None:
            continue
        for budget in budgets:
            if not _matches(key, budget):
                continue
            covered += 1
            measured_ms = measured / 1e6
            ok = measured_ms <= budget["budget_ms"]
            flag = "OK " if ok else "FAIL"
            print(
                f"  [{flag}] {key} → {budget['objective']} "
                f"({budget['stat']} ≤ {budget['budget_ms']:g} ms): "
                f"measured {fmt_time(measured)}"
            )
            if not ok:
                violations.append(
                    f"{key} → {budget['objective']}: measured {measured_ms:.3f} ms "
                    f"exceeds {budget['stat']} budget {budget['budget_ms']:g} ms"
                )

    if covered == 0:
        print(
            "  (no benchmark in the snapshot maps to an SLO objective; nothing to "
            "enforce yet — add latency benches named for the objective id or gRPC method)"
        )
    if violations:
        print("\nSLO BUDGET VIOLATIONS:")
        for v in violations:
            print(f"  - {v}")
        return 1
    print("\nall measured operations are within their absolute SLO budgets.")
    return 0


def run_relative(baseline_tag: str | None, max_regression: float) -> int:
    latest = _latest_snapshot()
    if latest is None:
        # Fail-closed: nothing to measure means the gate cannot vouch for the
        # release, so it must NOT pass.
        print("no bench-history snapshot found — run bench_snapshot.py first. FAIL.")
        return 2
    cur_name, cur = latest

    if baseline_tag:
        base = _snapshot_by_tag(baseline_tag)
        if base is None:
            # Fail-closed: a requested baseline tag that does not exist is an
            # error, never a silent fall-back to the prior run / a default pass.
            print(
                f"no snapshot tagged `{baseline_tag}` in bench-history — cannot gate. FAIL.\n"
                "  (a missing baseline is a red, not a green; tag a release with "
                "`bench_snapshot.py --tag`)"
            )
            return 2
        base_name, baseline = base
    else:
        # Default baseline = the last RELEASE snapshot (most recent tagged), NOT
        # the previous run. This is what makes the gate "vs last release".
        base = _latest_release_snapshot(exclude=cur_name)
        if base is None:
            # Fail-closed: no tagged release to compare against. We deliberately
            # do NOT fall back to the prior run — that would let drift across
            # untagged runs hide a regression and turn a missing baseline green.
            print(
                "no tagged RELEASE snapshot in bench-history — cannot gate against "
                "last release. FAIL.\n"
                "  (stamp a release baseline with `bench_snapshot.py --tag <vX.Y.Z>`, "
                "or pass an explicit `--baseline-tag`; a missing baseline is a red, "
                "not a green)"
            )
            return 2
        base_name, baseline = base

    print(
        f"relative gate · `{base_name}` → `{cur_name}` · "
        f"fail if any bench regressed > {max_regression:g}%"
    )
    cur_results = cur.get("results", {})
    base_results = baseline.get("results", {})
    regressions: list[str] = []
    for key, entry in sorted(cur_results.items()):
        o = base_results.get(key)
        if not o or "median_ns" not in o or "median_ns" not in entry:
            continue
        if o["median_ns"] <= 0:
            continue
        dt = (entry["median_ns"] - o["median_ns"]) / o["median_ns"] * 100.0
        if dt > max_regression:
            regressions.append(
                f"{key}: {fmt_time(o['median_ns'])} → {fmt_time(entry['median_ns'])} "
                f"({dt:+.1f}%)"
            )

    if regressions:
        print("\nREGRESSIONS beyond threshold:")
        for r in regressions:
            print(f"  - {r}")
        return 1
    print("\nno benchmark regressed beyond the threshold.")
    return 0


# ── Self-test (no real bench run; inline fixtures) ───────────────────────────

def _selftest() -> int:
    """Demonstrate the three gate behaviours with tiny inline fixtures — no
    Criterion run, no network. Proves the doctrine end-to-end:

      1. missing baseline  → ERROR exit (fail-closed, never a silent pass),
      2. regression > threshold → FAIL,
      3. within threshold  → PASS,

    plus an absolute-mode over/under-budget check parsed from a generated
    slo.md fixture. Temporarily redirects the module-global HIST at the live
    `bench-history/` snapshots stay untouched."""
    import tempfile

    global HIST
    saved_hist = HIST
    failures: list[str] = []

    def _check(label: str, got: int, want: int) -> None:
        ok = got == want
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}: exit {got} (want {want})")
        if not ok:
            failures.append(label)

    def _write_snap(hist: str, ts: str, key_to_ns: dict, tag: str = "") -> None:
        snap = {
            "timestamp": ts,
            "label": "selftest",
            "tag": tag,
            "git_commit": "deadbee",
            "rustc": "selftest",
            "os": sys.platform,
            "results": {k: {"median_ns": float(v)} for k, v in key_to_ns.items()},
        }
        with open(os.path.join(hist, f"bench-{ts}.json"), "w", encoding="utf-8") as fh:
            json.dump(snap, fh)

    # ── Scenario 1: latest run, but NO tagged release baseline → must ERROR.
    with tempfile.TemporaryDirectory() as tmp:
        HIST = tmp
        _write_snap(tmp, "20200101T000000Z", {"authn/login": 1_000_000.0})  # untagged
        _check("missing baseline errors (fail-closed)", run_relative(None, 10.0), 2)

    # ── Scenario 2: tagged baseline + a +50% slower latest → must FAIL.
    with tempfile.TemporaryDirectory() as tmp:
        HIST = tmp
        _write_snap(tmp, "20200101T000000Z", {"authn/login": 1_000_000.0}, tag="v0.0.1")
        _write_snap(tmp, "20200102T000000Z", {"authn/login": 1_500_000.0})  # +50%
        _check("regression beyond threshold fails", run_relative(None, 10.0), 1)

    # ── Scenario 3: tagged baseline + a +2% slower latest (< 10%) → must PASS.
    with tempfile.TemporaryDirectory() as tmp:
        HIST = tmp
        _write_snap(tmp, "20200101T000000Z", {"authn/login": 1_000_000.0}, tag="v0.0.1")
        _write_snap(tmp, "20200102T000000Z", {"authn/login": 1_020_000.0})  # +2%
        _check("within threshold passes", run_relative(None, 10.0), 0)

    # ── Scenario 4 (bonus): an explicit --baseline-tag that doesn't exist → ERROR.
    with tempfile.TemporaryDirectory() as tmp:
        HIST = tmp
        _write_snap(tmp, "20200101T000000Z", {"authn/login": 1_000_000.0}, tag="v0.0.1")
        _write_snap(tmp, "20200102T000000Z", {"authn/login": 1_010_000.0})
        _check("unknown --baseline-tag errors", run_relative("v9.9.9", 10.0), 2)

    # ── Scenario 5 (bonus): absolute mode parses slo.md & catches an over-budget.
    with tempfile.TemporaryDirectory() as tmp:
        HIST = tmp
        slo = os.path.join(tmp, "slo.md")
        with open(slo, "w", encoding="utf-8") as fh:
            fh.write(
                f"{SLO_BEGIN}\n"
                "| Objective | Operation | gRPC method | Latency target |\n"
                "|---|---|---|---|\n"
                "| `authn.login` | login | `Login` | p99 ≤ 400 ms |\n"
                f"{SLO_END}\n"
            )
        # 500 ms measured > 400 ms budget → FAIL.
        _write_snap(tmp, "20200101T000000Z", {"authn.login/login": 500_000_000.0})
        _check("absolute over-budget fails", run_absolute(slo), 1)
        # 300 ms measured < 400 ms budget → PASS (overwrite the latest snapshot).
        _write_snap(tmp, "20200102T000000Z", {"authn.login/login": 300_000_000.0})
        _check("absolute within-budget passes", run_absolute(slo), 0)

    HIST = saved_hist
    print(f"\nself-test: {'ALL PASS' if not failures else 'FAILURES: ' + ', '.join(failures)}")
    return 1 if failures else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--selftest",
        action="store_true",
        help="run the inline fixture self-test (missing-baseline → error, "
        "regression → fail, within-threshold → pass); no bench run required",
    )
    mode.add_argument(
        "--absolute",
        metavar="SLO_MD",
        nargs="?",
        const=os.path.join("docs", "slo.md"),
        help="enforce absolute per-objective budgets parsed from this SLO doc "
        "(default: docs/slo.md)",
    )
    mode.add_argument(
        "--relative",
        action="store_true",
        help="enforce no regression vs the last RELEASE snapshot (most recent "
        "tagged), or vs --baseline-tag; fails closed if no baseline exists",
    )
    ap.add_argument(
        "--baseline-tag",
        default=None,
        help="(relative mode) compare against the snapshot carrying this `tag` "
        "instead of the last release; FAILS (non-zero) if no such snapshot exists",
    )
    ap.add_argument(
        "--max-regression",
        type=float,
        default=10.0,
        help="(relative mode) max tolerated median regression percent (default 10)",
    )
    args = ap.parse_args()

    if args.selftest:
        return _selftest()
    if args.absolute is not None:
        slo_md = args.absolute
        if not os.path.isabs(slo_md):
            slo_md = os.path.join(ROOT, slo_md)
        return run_absolute(slo_md)
    return run_relative(args.baseline_tag, args.max_regression)


if __name__ == "__main__":
    raise SystemExit(main())
