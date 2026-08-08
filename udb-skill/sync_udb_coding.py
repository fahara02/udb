#!/usr/bin/env python3
"""Regenerate every udb-coding provider wrapper from canonical sources.

Run from anywhere:
    python udb-skill/sync_udb_coding.py
    python udb-skill/sync_udb_coding.py --check
"""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SHARED = ROOT / "shared" / "udb-coding.md"
REFERENCE = (
    ROOT
    / "plugins"
    / "udb"
    / "skills"
    / "udb-coding"
    / "references"
    / "udb-coding.md"
)
COMPANIONS = (
    ROOT / "shared" / "udb-coding-subsystem-map.md",
    ROOT / "shared" / "udb-coding-rust-stack.md",
    ROOT / "shared" / "udb-coding-backends.md",
    ROOT / "shared" / "udb-coding-subsystem-checklist.md",
)
WRAPPERS = {
    "openai": ROOT / "openai" / "instructions-udb-coding.md",
    "ollama": ROOT / "ollama" / "Modelfile.udb-coding",
}
TRIPLE_QUOTE = '"' * 3


def read(path: Path) -> str:
    if not path.is_file():
        raise SystemExit(f"canonical udb-coding source is missing: {path}")
    return path.read_text(encoding="utf-8")


def preamble_of(current: str, marker: str) -> str:
    idx = current.find(marker)
    if idx < 0:
        raise SystemExit(
            "canonical body marker not found in wrapper; expected line:\n  " + marker
        )
    return current[:idx]


def knowledge() -> str:
    parts = [read(SHARED).rstrip()]
    parts.extend(read(path).rstrip() for path in COMPANIONS)
    return "\n\n---\n\n".join(parts) + "\n"


def render(kind: str, current: str, body: str, marker: str) -> str:
    preamble = preamble_of(current, marker)
    if kind == "openai":
        return preamble + body
    if kind == "ollama":
        return preamble + body + "\n" + TRIPLE_QUOTE + "\n"
    raise ValueError(kind)


def main() -> int:
    check = "--check" in sys.argv[1:]
    shared = read(SHARED)
    marker = shared.splitlines()[0]
    body = knowledge()
    wanted = {REFERENCE: shared}
    for kind, path in WRAPPERS.items():
        wanted[path] = render(kind, read(path), body, marker)

    drift: list[Path] = []
    for path, content in wanted.items():
        if read(path) == content:
            continue
        if check:
            drift.append(path)
            continue
        path.write_text(content, encoding="utf-8", newline="\n")
        print(f"synced {path.relative_to(ROOT).as_posix()}")

    if drift:
        print("OUT OF SYNC (run: python udb-skill/sync_udb_coding.py):")
        for path in drift:
            print(f"  - {path.relative_to(ROOT).as_posix()}")
        return 1
    if check:
        print("all udb-coding wrappers in sync")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
