#!/usr/bin/env python3
"""Sync generated/canonical UDB references bundled with the Claude skills.

Run from anywhere:
    python udb-skill/sync_references.py
    python udb-skill/sync_references.py --check
"""

from __future__ import annotations

import sys
from pathlib import Path


SKILL_ROOT = Path(__file__).resolve().parent
REPO_ROOT = SKILL_ROOT.parent
PAIRS = {
    REPO_ROOT / "docs" / "generated" / "codebase-map.md": (
        SKILL_ROOT / "plugins" / "udb" / "skills" / "udb-coding" / "references" / "codebase-map.md"
    ),
    REPO_ROOT / "docs" / "generated" / "authn-authz-rpc-inventory.md": (
        SKILL_ROOT / "plugins" / "udb" / "skills" / "using-udb" / "references" / "rpc-inventory.md"
    ),
    REPO_ROOT / "docs" / "generated" / "authn-authz-sensitive-fields.md": (
        SKILL_ROOT / "plugins" / "udb" / "skills" / "using-udb" / "references" / "sensitive-fields.md"
    ),
    SKILL_ROOT / "shared" / "udb-coding-subsystem-checklist.md": (
        SKILL_ROOT
        / "plugins"
        / "udb"
        / "skills"
        / "udb-coding"
        / "references"
        / "subsystem-checklist.md"
    ),
    SKILL_ROOT / "shared" / "udb-coding-subsystem-map.md": (
        SKILL_ROOT
        / "plugins"
        / "udb"
        / "skills"
        / "udb-coding"
        / "references"
        / "subsystem_map.md"
    ),
}


def display(path: Path) -> str:
    return path.relative_to(REPO_ROOT).as_posix()


def main() -> int:
    check = "--check" in sys.argv[1:]
    drift: list[tuple[Path, Path]] = []

    for source, destination in PAIRS.items():
        if not source.is_file():
            raise SystemExit(f"canonical reference is missing: {display(source)}")
        source_bytes = source.read_bytes()
        if destination.is_file() and destination.read_bytes() == source_bytes:
            continue
        if check:
            drift.append((source, destination))
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source_bytes)
        print(f"synced {display(source)} -> {display(destination)}")

    if drift:
        print("OUT OF SYNC (run: python udb-skill/sync_references.py):")
        for source, destination in drift:
            print(f"  - {display(destination)} (from {display(source)})")
        return 1
    if check:
        print("all bundled UDB references in sync")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
