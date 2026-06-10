#!/usr/bin/env python3
"""Generate docs/generated/codebase-map.md — the agent-facing codebase map.

Two macro-level views, regenerated from source so they can never rot:
  1. Module dependency graphs (Mermaid): top-level src/ modules, and the
     src/runtime/ subsystems (runtime is ~70% of the code, so it gets its own
     graph).
  2. A per-file public-symbol index: file -> module doc summary + public
     fns/types/traits/consts, so an agent can find the canonical symbol name to
     grep instead of discovery-looping.

Deterministic output (sorted walks, no timestamps) so CI can diff-gate
freshness exactly like docs/generated/udb-native-contract.json.

Usage:  python scripts/generate-codebase-map.py [--check]
        --check: exit 1 if the committed map differs from the regenerated one.
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "docs" / "generated" / "codebase-map.md"

SCAN_ROOTS = [
    ("src", REPO / "src"),
    ("crates/udb-portable/src", REPO / "crates" / "udb-portable" / "src"),
    ("crates/udb-wasm/src", REPO / "crates" / "udb-wasm" / "src"),
]

PUB_RE = re.compile(
    r"^\s*pub(?:\((?:crate|super)\))?\s+"
    r"(?:async\s+)?(?:unsafe\s+)?(?:extern\s+\"C\"\s+)?"
    r"(fn|struct|trait|enum|const|type|static)\s+([A-Za-z_][A-Za-z0-9_]*)"
)
USE_CRATE_RE = re.compile(r"^\s*use\s+crate::([a-z_][a-z0-9_]*)(?:::([a-z_][a-z0-9_]*))?")
MODDOC_RE = re.compile(r"^//!\s?(.*)$")
TEST_MOD_RE = re.compile(r"^\s*(?:#\[cfg\(test\)\]|mod\s+tests\b)")

MAX_NAMES_PER_KIND = 40
KIND_LABEL = {"fn": "fns", "struct": "types", "enum": "types", "type": "types",
              "trait": "traits", "const": "consts", "static": "consts"}


def module_of(rel: str) -> str:
    """Map a repo-relative rust file path to its macro module node."""
    parts = rel.replace("\\", "/").split("/")
    if parts[0] == "crates":
        return f"crates/{parts[1]}"
    # src/<top>/... ; runtime gets one extra level (it is ~70% of the code).
    if len(parts) == 2:  # src/lib.rs etc.
        return "src (root)"
    top = parts[1]
    if top == "runtime":
        if len(parts) == 3:  # src/runtime/<file>.rs
            return "runtime (core files)"
        sub = parts[2]
        if sub == "service":
            # split the 54k-line service layer one more level
            if len(parts) == 4:
                return "runtime/service (core)"
            return f"runtime/service/{parts[3]}"
        return f"runtime/{sub}"
    return top


def edge_target(top: str, sub: str | None, src_module: str) -> str | None:
    """Resolve a `use crate::top(::sub)` to a macro module node, or None."""
    if top == "runtime":
        if sub is None:
            return "runtime (core files)"
        if sub == "service":
            return "runtime/service (core)"
        return f"runtime/{sub}"
    return top


def scan_file(path: Path) -> tuple[str, dict[str, list[str]], set[str]]:
    """Return (moddoc_summary, {kind_label: [names]}, use_targets(top,sub))."""
    doc_lines: list[str] = []
    symbols: dict[str, list[str]] = defaultdict(list)
    uses: set[tuple[str, str | None]] = set()
    in_doc_header = True
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return "", {}, set()
    for line in text.splitlines():
        if in_doc_header:
            m = MODDOC_RE.match(line)
            if m:
                doc_lines.append(m.group(1).strip())
                continue
            if line.strip() and not line.startswith("//"):
                in_doc_header = False
        if TEST_MOD_RE.match(line):
            break  # tests live at the bottom by convention; stop indexing
        m = USE_CRATE_RE.match(line)
        if m:
            uses.add((m.group(1), m.group(2)))
        m = PUB_RE.match(line)
        if m:
            kind, name = m.group(1), m.group(2)
            symbols[KIND_LABEL[kind]].append(name)
    doc = " ".join(doc_lines)
    doc = re.sub(r"\s+", " ", doc).strip()
    if len(doc) > 220:
        doc = doc[:217] + "…"
    return doc, dict(symbols), uses


def mermaid(name: str, edges: set[tuple[str, str]], nodes: set[str]) -> list[str]:
    def nid(n: str) -> str:
        return re.sub(r"[^A-Za-z0-9_]", "_", n)

    out = [f"### {name}", "", "```mermaid", "graph LR"]
    for n in sorted(nodes):
        out.append(f"  {nid(n)}[\"{n}\"]")
    for a, b in sorted(edges):
        out.append(f"  {nid(a)} --> {nid(b)}")
    out += ["```", ""]
    return out


def main() -> int:
    files: dict[str, tuple[str, dict[str, list[str]]]] = {}
    by_module: dict[str, list[str]] = defaultdict(list)
    raw_edges: set[tuple[str, str]] = set()

    for label, root in SCAN_ROOTS:
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*.rs")):
            rel = str(path.relative_to(REPO)).replace("\\", "/")
            if "/target/" in rel or rel.endswith("/tests.rs") and "live" not in rel:
                continue
            doc, symbols, uses = scan_file(path)
            files[rel] = (doc, symbols)
            mod = module_of(rel)
            by_module[mod].append(rel)
            for top, sub in uses:
                tgt = edge_target(top, sub, mod)
                if tgt and tgt != mod:
                    raw_edges.add((mod, tgt))

    # Normalize edge targets to KNOWN module nodes: `use crate::runtime::system`
    # points at a top-level runtime FILE, not a directory — collapse those into
    # "runtime (core files)" so the graphs only show real macro nodes.
    known = set(by_module)
    known_tops = {m.split("/")[0] for m in known} | known

    def normalize(n: str) -> str | None:
        if n in known:
            return n
        if n.startswith("runtime/service"):
            return "runtime/service (core)"
        if n.startswith("runtime/"):
            return "runtime (core files)"
        return n if n in known_tops else None

    edges: set[tuple[str, str]] = set()
    for a, b in raw_edges:
        na, nb = normalize(a), normalize(b)
        if na and nb and na != nb:
            edges.add((na, nb))

    # Split edges into the two macro graphs.
    top_nodes = {m for m in by_module if "/" not in m or m.startswith("crates/")}
    top_nodes |= {"runtime"}  # collapse all runtime/* into one node for graph 1
    def collapse(n: str) -> str:
        return "runtime" if n.startswith("runtime") else n
    top_edges = {(collapse(a), collapse(b)) for a, b in edges
                 if collapse(a) != collapse(b)
                 and collapse(a) in top_nodes and collapse(b) in top_nodes}

    rt_nodes = {m for m in by_module if m.startswith("runtime")}
    # service/* collapses into one node in the runtime graph to stay readable
    def rt_collapse(n: str) -> str:
        return "runtime/service" if n.startswith("runtime/service") else n
    rt_edges = {(rt_collapse(a), rt_collapse(b)) for a, b in edges
                if a.startswith("runtime") and b.startswith("runtime")
                and rt_collapse(a) != rt_collapse(b)}
    rt_graph_nodes = {rt_collapse(n) for n in rt_nodes}

    lines: list[str] = []
    lines += [
        "# UDB codebase map (GENERATED — do not edit)",
        "",
        "Regenerate: `python scripts/generate-codebase-map.py`  ·  CI freshness-gates",
        "this file. Read this FIRST to locate a subsystem/symbol, then grep the",
        "symbol name — that beats discovery-grepping every time.",
        "",
        "- **Graphs:** macro module dependencies (`use crate::…` edges, aggregated).",
        "- **Index:** per file — module-doc summary + public symbols (test modules",
        "  excluded). `pub(crate)` items are included: they ARE the internal API.",
        "",
        "## Module dependency graphs",
        "",
    ]
    lines += mermaid("Top-level modules", top_edges, {collapse(n) for n in top_nodes if collapse(n) in {collapse(x) for x, _ in top_edges} | {collapse(y) for _, y in top_edges}} or {collapse(n) for n in top_nodes})
    lines += mermaid("Inside src/runtime", rt_edges, rt_graph_nodes)

    lines += ["## Public-symbol index", ""]
    for mod in sorted(by_module):
        total = sum(1 for _ in by_module[mod])
        lines.append(f"### {mod}  ({total} files)")
        lines.append("")
        for rel in sorted(by_module[mod]):
            doc, symbols = files[rel]
            parts: list[str] = []
            if doc:
                parts.append(doc)
            for kind in ("traits", "types", "fns", "consts"):
                names = symbols.get(kind, [])
                if not names:
                    continue
                shown = names[:MAX_NAMES_PER_KIND]
                extra = f" +{len(names) - len(shown)} more" if len(names) > len(shown) else ""
                parts.append(f"{kind}: " + ", ".join(f"`{n}`" for n in shown) + extra)
            body = " · ".join(parts) if parts else "(no public items)"
            lines.append(f"- **{rel}** — {body}")
        lines.append("")

    content = "\n".join(lines) + "\n"

    if "--check" in sys.argv:
        current = OUT.read_text(encoding="utf-8") if OUT.is_file() else ""
        if current != content:
            print(f"STALE: {OUT.relative_to(REPO)} differs from regenerated output. "
                  f"Run: python scripts/generate-codebase-map.py", file=sys.stderr)
            return 1
        print("codebase-map up to date")
        return 0

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(content, encoding="utf-8")
    print(f"wrote {OUT.relative_to(REPO)} ({len(content)} bytes, "
          f"{len(files)} files indexed, {len(by_module)} modules)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
