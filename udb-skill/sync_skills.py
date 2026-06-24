#!/usr/bin/env python3
# Regenerate the provider wrappers for the `using-udb` skill from its canonical
# source (`shared/using-udb.md`), so the Claude reference, OpenAI instructions,
# and Ollama Modelfile never drift.
#
# Each wrapper = a fixed PREAMBLE (everything before the canonical body's first
# heading) + the canonical body verbatim. Ollama additionally closes the
# SYSTEM triple-quote block. The Claude references/using-udb.md is a byte-for-byte
# copy of the shared body.
#
# Run from anywhere (paths resolve relative to this file):
#     python sync_skills.py            # rewrite the wrappers
#     python sync_skills.py --check    # exit 1 if any wrapper is out of sync (CI)
#
# NOTE: scoped to `using-udb`. The `udb-coding` OpenAI/Ollama wrappers inline
# extra companion files (backends, rust-stack); regenerate those with their own
# flow rather than this script.

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SKILL = "using-udb"
SHARED = ROOT / "shared" / (SKILL + ".md")
WRAPPERS = {
    "reference": ROOT / "plugins" / "udb" / "skills" / SKILL / "references" / (SKILL + ".md"),
    "openai": ROOT / "openai" / "instructions.md",
    "ollama": ROOT / "ollama" / "Modelfile",
}
TRIPLE_QUOTE = '"' * 3


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def preamble_of(current: str, marker: str) -> str:
    # Everything in an existing wrapper before the canonical body begins.
    idx = current.find(marker)
    if idx < 0:
        raise SystemExit(
            "canonical body marker not found in wrapper; expected line:\n  " + marker
        )
    return current[:idx]


def render(kind: str, preamble: str, body: str) -> str:
    if kind == "reference":
        return body
    if kind == "openai":
        return preamble + body
    if kind == "ollama":
        # Close the SYSTEM triple-quote block opened in the preamble.
        return preamble + body + "\n" + TRIPLE_QUOTE + "\n"
    raise ValueError(kind)


def main() -> int:
    check = "--check" in sys.argv[1:]
    body = read(SHARED)
    marker = body.splitlines()[0]  # the canonical H1 line
    drift = []
    for kind, path in WRAPPERS.items():
        preamble = "" if kind == "reference" else preamble_of(read(path), marker)
        want = render(kind, preamble, body)
        if read(path) == want:
            continue
        if check:
            drift.append(str(path.relative_to(ROOT)))
        else:
            path.write_text(want, encoding="utf-8", newline="\n")
            print("synced " + str(path.relative_to(ROOT)))
    if check and drift:
        print("OUT OF SYNC (run: python sync_skills.py):")
        for d in drift:
            print("  - " + d)
        return 1
    if check:
        print("all using-udb wrappers in sync")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
