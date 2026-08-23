#!/usr/bin/env python3
"""Fail on any case-insensitive path collision in the git index.

Why this exists
---------------
Two directories differing only in case (`sdk/csharp/gen/Udb` and
`sdk/csharp/gen/udb`) made the repository root un-packageable as a Go module:
`golang.org/x/mod/zip` refuses to build a zip whose file set cannot be extracted
on a case-insensitive filesystem, so every `go install github.com/fahara02/udb@vX`
failed. Published tags cannot be repaired — the module proxy caches immutably.

Why the obvious check does NOT work
-----------------------------------
Comparing whole paths case-insensitively:

    git ls-files | tr 'A-Z' 'a-z' | sort | uniq -d

reported nothing on this repository even while it was broken, because the files
under `Udb/` had no lowercase twins. The collision lived in a directory PREFIX,
hidden behind filenames that did not themselves collide. A guard written that way
passes and hands you false assurance, which is worse than no guard.

So this compares every path PREFIX, not just whole paths.

Why the git index and not the filesystem
----------------------------------------
On Windows (NTFS) and macOS (APFS default) the working tree cannot represent the
collision at all: both index paths collapse into one physical directory and
`git status` reports clean. A filesystem walk finds nothing on the machines most
contributors use. Only the index carries the truth.

Usage:  python scripts/check-path-case-collisions.py [--selftest]
"""

from __future__ import annotations

import subprocess
import sys


def tracked_paths(repo: str | None = None) -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-z"],
        capture_output=True,
        check=True,
        cwd=repo,
    ).stdout
    return [p for p in out.decode("utf-8", "surrogateescape").split("\0") if p]


def collisions(paths: list[str]) -> list[tuple[str, str]]:
    """Every pair of tracked path prefixes that differ only by case."""
    prefixes: set[str] = set()
    for path in paths:
        parts = path.split("/")
        for i in range(1, len(parts) + 1):
            prefixes.add("/".join(parts[:i]))

    seen: dict[str, str] = {}
    found: list[tuple[str, str]] = []
    for prefix in sorted(prefixes):
        key = prefix.lower()
        if key in seen:
            if seen[key] != prefix:
                found.append((seen[key], prefix))
        else:
            seen[key] = prefix
    return found


def selftest() -> int:
    """The naive whole-path check must fail these; this one must catch them."""
    cases = [
        # (paths, expected collision count, description)
        ([], 0, "empty"),
        (["a/b.txt", "a/c.txt"], 0, "no collision"),
        (["Gen/x/A.cs", "gen/b.cs"], 1, "directory prefix collides, filenames do not"),
        (["gen/b.cs"], 0, "colliding sibling removed"),
        (["sdk/csharp/gen/Udb/Entity/V1/Admin.cs", "sdk/csharp/gen/udb/core/X.cs"], 1, "the real defect"),
        (["a/B.txt", "a/b.txt"], 1, "plain filename collision still caught"),
        (["x/y/Z/f", "x/y/z/g", "P/q", "p/r"], 2, "two independent collisions"),
    ]
    failed = 0
    for paths, expected, desc in cases:
        got = len(collisions(paths))
        ok = got == expected
        if not ok:
            failed += 1
        print(f"  [{'ok' if ok else 'FAIL'}] {desc}: expected {expected}, got {got}")

    # The naive check must be demonstrably blind to the prefix case, otherwise
    # this script is not earning its keep.
    naive_blind = ["Gen/x/A.cs", "gen/b.cs"]
    lowered = [p.lower() for p in naive_blind]
    if len(lowered) != len(set(lowered)):
        print("  [FAIL] naive whole-path check was expected to be blind here")
        failed += 1
    else:
        print("  [ok] naive whole-path check is blind to this, as documented")

    if failed:
        print(f"selftest FAILED ({failed} case(s))")
        return 1
    print("path-case-collision selftest passed")
    return 0


def main(argv: list[str]) -> int:
    if "--selftest" in argv:
        return selftest()

    found = collisions(tracked_paths())
    if not found:
        print("no case-insensitive path collisions in the git index")
        return 0

    print("::error::case-insensitive path collision(s) in the git index.")
    print("These make the repository un-packageable as a Go module and cannot be")
    print("observed from a Windows or macOS working tree. Resolve before tagging:")
    for a, b in found:
        print(f"  {a}  <->  {b}")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
