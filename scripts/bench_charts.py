#!/usr/bin/env python3
"""Render vibrant benchmark-result charts as artifacts (D.2).

Criterion already writes interactive HTML plots to `target/criterion/`; this
script harvests its machine-readable JSON and produces self-contained, vibrant
SVG dashboards under `bench-artifacts/` (latency + throughput) plus an
`index.html` that embeds them — portable artifacts that render in any browser,
on GitHub, or in a PR, with zero Python dependencies.

Usage (after `cargo bench --features bench-internals`):
    python scripts/bench_charts.py
"""

from __future__ import annotations

import html
import json
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CRIT = os.path.join(ROOT, "target", "criterion")
OUT = os.path.join(ROOT, "bench-artifacts")

# Vibrant, high-contrast categorical palette (one colour per benchmark group).
PALETTE = [
    "#FF6B6B", "#4ECDC4", "#FFD93D", "#6A67CE", "#1A936F",
    "#FF9F1C", "#3A86FF", "#E84393", "#00B894", "#A55EEA",
]


def collect():
    """Walk target/criterion for `<group>/<function>/new/estimates.json`."""
    rows = []
    if not os.path.isdir(CRIT):
        return rows
    for group in sorted(os.listdir(CRIT)):
        gdir = os.path.join(CRIT, group)
        if group == "report" or not os.path.isdir(gdir):
            continue
        for fn in sorted(os.listdir(gdir)):
            est = os.path.join(gdir, fn, "new", "estimates.json")
            if not os.path.isfile(est):
                continue
            try:
                with open(est, encoding="utf-8") as fh:
                    median_ns = json.load(fh)["median"]["point_estimate"]
            except (OSError, KeyError, ValueError):
                continue
            thrpt = None
            bj = os.path.join(gdir, fn, "new", "benchmark.json")
            if os.path.isfile(bj):
                try:
                    with open(bj, encoding="utf-8") as fh:
                        t = json.load(fh).get("throughput")
                    if isinstance(t, dict) and t:
                        kind, n = next(iter(t.items()))
                        per_s = n / (median_ns * 1e-9)
                        thrpt = (kind, per_s)
                except (OSError, ValueError):
                    pass
            rows.append({"group": group, "fn": fn, "median_ns": median_ns, "thrpt": thrpt})
    return rows


def fmt_time(ns: float) -> str:
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("µs", 1e3), ("ns", 1.0)):
        if ns >= scale:
            return f"{ns / scale:.2f} {unit}"
    return f"{ns:.2f} ns"


def fmt_thrpt(kind: str, per_s: float) -> str:
    if kind == "Bytes":
        for unit, scale in (("GiB/s", 2 ** 30), ("MiB/s", 2 ** 20), ("KiB/s", 2 ** 10)):
            if per_s >= scale:
                return f"{per_s / scale:.2f} {unit}"
        return f"{per_s:.0f} B/s"
    suffix = {"Elements": "elem/s"}.get(kind, kind + "/s")
    if per_s >= 1e6:
        return f"{per_s / 1e6:.2f} M{suffix.split('/')[0]}/s" if suffix.endswith("/s") else f"{per_s:.0f}"
    return f"{per_s:,.0f} {suffix}"


def svg_bars(title, rows, value_key, label_fn, group_colors):
    """Horizontal bar chart SVG. `value_key(row)` -> float (bar length basis)."""
    rows = [r for r in rows if value_key(r) is not None]
    if not rows:
        return f'<svg xmlns="http://www.w3.org/2000/svg" width="600" height="60"><text x="20" y="35" fill="#888" font-family="sans-serif">{html.escape(title)}: no data</text></svg>'
    pad_l, pad_r, pad_t, row_h = 320, 140, 70, 34
    width, height = 1100, pad_t + row_h * len(rows) + 30
    vmax = max(value_key(r) for r in rows) or 1.0
    bar_w = width - pad_l - pad_r
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" font-family="Segoe UI, Helvetica, Arial, sans-serif">',
        f'<rect width="{width}" height="{height}" fill="#0f1226"/>',
        f'<text x="{width/2}" y="38" fill="#ffffff" font-size="24" font-weight="700" text-anchor="middle">{html.escape(title)}</text>',
    ]
    for i, r in enumerate(rows):
        y = pad_t + i * row_h
        v = value_key(r)
        w = max(2.0, bar_w * (v / vmax))
        color = group_colors[r["group"]]
        name = f'{r["group"]} / {r["fn"]}'
        parts.append(f'<text x="{pad_l-12}" y="{y+row_h*0.62}" fill="#c9d1e8" font-size="14" text-anchor="end">{html.escape(name)}</text>')
        parts.append(f'<rect x="{pad_l}" y="{y+5}" width="{w:.1f}" height="{row_h-12}" rx="5" fill="{color}"/>')
        parts.append(f'<text x="{pad_l+w+10:.1f}" y="{y+row_h*0.62}" fill="#ffffff" font-size="14" font-weight="600">{html.escape(label_fn(r))}</text>')
    parts.append("</svg>")
    return "\n".join(parts)


def main() -> int:
    rows = collect()
    os.makedirs(OUT, exist_ok=True)
    if not rows:
        print("no Criterion results under target/criterion — run a bench first.")
        return 0

    groups = sorted({r["group"] for r in rows})
    group_colors = {g: PALETTE[i % len(PALETTE)] for i, g in enumerate(groups)}

    # Latency: shorter is better.
    lat = svg_bars(
        "UDB benchmark — median latency (shorter = faster)",
        sorted(rows, key=lambda r: (r["group"], r["median_ns"])),
        lambda r: r["median_ns"],
        lambda r: fmt_time(r["median_ns"]),
        group_colors,
    )
    with open(os.path.join(OUT, "latency.svg"), "w", encoding="utf-8") as fh:
        fh.write(lat)

    # Throughput: longer is better.
    thr = svg_bars(
        "UDB benchmark — throughput (longer = faster)",
        sorted([r for r in rows if r["thrpt"]], key=lambda r: -r["thrpt"][1]),
        lambda r: r["thrpt"][1] if r["thrpt"] else None,
        lambda r: fmt_thrpt(*r["thrpt"]),
        group_colors,
    )
    with open(os.path.join(OUT, "throughput.svg"), "w", encoding="utf-8") as fh:
        fh.write(thr)

    legend = " ".join(
        f'<span style="color:{c};font-weight:700">&#9632; {html.escape(g)}</span>'
        for g, c in group_colors.items()
    )
    index = f"""<!doctype html><html><head><meta charset="utf-8">
<title>UDB benchmark artifacts</title>
<style>body{{background:#0f1226;color:#c9d1e8;font-family:Segoe UI,Arial,sans-serif;margin:24px}}
h1{{color:#fff}} img{{max-width:100%;display:block;margin:18px 0;border-radius:10px}}</style></head>
<body><h1>UDB benchmark results</h1>
<p>Groups: {legend}</p>
<p>Interactive Criterion plots: <code>target/criterion/report/index.html</code></p>
<img src="latency.svg" alt="median latency"><img src="throughput.svg" alt="throughput">
</body></html>"""
    with open(os.path.join(OUT, "index.html"), "w", encoding="utf-8") as fh:
        fh.write(index)

    print(f"wrote {len(rows)} benchmark(s) to bench-artifacts/:")
    print("  latency.svg  throughput.svg  index.html")
    print("  (open bench-artifacts/index.html; Criterion HTML at target/criterion/report/index.html)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
