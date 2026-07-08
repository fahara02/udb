#!/usr/bin/env python3
"""Fail if a hand-maintained service/RPC count literal lives in tracked docs.

UDB's service/RPC surface counts have ONE source of truth: the embedded
descriptor. They are rendered into `docs/generated/` (by `udb native docs`) and
into the marked block of `README.md` (fenced by
`<!-- BEGIN/END GENERATED:services -->`, asserted by the
`readme_services_block_matches_embedded_descriptor` staleness test). Any other
tracked doc that hard-codes a literal like "15 services" or "188 RPCs" will
drift the moment the descriptor changes.

This script greps tracked docs for those literals and reports every hit OUTSIDE
the two allowed homes (the README generated block and `docs/generated/`). It is
a standalone guard — deliberately NOT wired into CI here; the maintainer owns CI
wiring.

Usage:
    python scripts/check-doc-service-counts.py        # scan, exit 1 on any hit
    python scripts/check-doc-service-counts.py --list  # also print scanned files
    python scripts/check-doc-service-counts.py --selftest  # fixture checks

Exit codes: 0 = clean, 1 = stray hand-maintained counts found, 2 = usage error.
"""

from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

# Literal hand-maintained native service/RPC surface counts, e.g. "15 native
# services", "188 native RPCs", or "15 services / 188 RPCs". These are the
# descriptor-derived service-surface counts this guard governs. Per-service
# bench prose such as "All 7 RPCs require auth" is intentionally out of scope.
COUNT_PATTERN = re.compile(
    r"(?:"
    r"(?<!Phase\s)\b\d+\s+native\s+services?\b"
    r"|\b\d+\s+native\s+RPCs?\b"
    r"|\b\d+\s+services?\s*(?:/|,|\band\b|\+|with)\s*\d+\s+(?:native\s+)?RPCs?\b"
    r"|\b\d+\s+(?:native\s+)?RPCs?\s*(?:/|,|\band\b|\+|with)\s*\d+\s+services?\b"
    r")",
    re.IGNORECASE,
)

REPO_ROOT = Path(__file__).resolve().parent.parent

# Generated docs are descriptor-derived by construction — counts there are fine.
ALLOWED_DIR_PREFIXES = ("docs/generated/",)

# README is allowed ONLY inside its generated, test-guarded block.
README_REL = "README.md"
README_BEGIN = "<!-- BEGIN GENERATED:services -->"
README_END = "<!-- END GENERATED:services -->"

# Doc extensions to scan. Counts in source/tests are guarded by the Rust
# staleness tests and are not the hand-drift target this script protects.
DOC_SUFFIXES = (".md", ".mdx", ".rst", ".txt", ".adoc")


def tracked_docs(root: Path = REPO_ROOT) -> list[str]:
    """All git-tracked doc files (relative POSIX paths)."""
    out = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    files = [p for p in out.split("\0") if p]
    return [p for p in files if p.endswith(DOC_SUFFIXES)]


def is_allowed_dir(rel_path: str) -> bool:
    return any(rel_path.startswith(prefix) for prefix in ALLOWED_DIR_PREFIXES)


def readme_marked_span(text: str) -> tuple[int, int] | None:
    """Char span [begin, end) of README's generated block, if both markers exist."""
    begin = text.find(README_BEGIN)
    end = text.find(README_END)
    if begin == -1 or end == -1 or begin >= end:
        return None
    return (begin, end + len(README_END))


def scan_file(rel_path: str, root: Path = REPO_ROOT) -> list[tuple[int, str]]:
    """Return [(line_number, line)] of stray count literals in this file."""
    abs_path = root / rel_path
    try:
        text = abs_path.read_text(encoding="utf-8")
    except (UnicodeDecodeError, OSError):
        return []

    # README: only flag hits OUTSIDE the generated block.
    span: tuple[int, int] | None = None
    if rel_path == README_REL:
        span = readme_marked_span(text)
        if span is None:
            # Markers missing/garbled — treat the whole README as unguarded so
            # the loss of the guard is visible rather than silent.
            span = (0, 0)

    hits: list[tuple[int, str]] = []
    offset = 0
    for lineno, line in enumerate(text.splitlines(keepends=True), start=1):
        line_start = offset
        offset += len(line)
        if not COUNT_PATTERN.search(line):
            continue
        if span is not None and span[0] <= line_start < span[1]:
            continue  # inside README's generated, test-guarded block
        hits.append((lineno, line.rstrip("\n")))
    return hits


def scan_docs(docs: list[str], root: Path = REPO_ROOT) -> list[tuple[str, int, str]]:
    failures: list[tuple[str, int, str]] = []
    for rel_path in docs:
        if is_allowed_dir(rel_path):
            continue
        for lineno, line in scan_file(rel_path, root):
            failures.append((rel_path, lineno, line))
    return failures


def write_fixture(root: Path, rel_path: str, text: str) -> None:
    target = root / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def run_selftest() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        write_fixture(
            root,
            README_REL,
            "\n".join(
                [
                    "# UDB",
                    README_BEGIN,
                    "UDB exposes 17 services and 264 RPCs.",
                    README_END,
                    "Narrative without descriptor count literals.",
                    "",
                ]
            ),
        )
        write_fixture(
            root,
            "docs/generated/native-services.md",
            "Generated table says 17 services and 264 RPCs.\n",
        )
        write_fixture(root, "docs/guide.md", "Use generated docs for service counts.\n")
        docs = [README_REL, "docs/generated/native-services.md", "docs/guide.md"]
        failures = scan_docs(docs, root)
        if failures:
            raise AssertionError(f"selftest clean fixture failed: {failures}")

        write_fixture(root, "docs/guide.md", "Stale prose says 15 services / 188 RPCs.\n")
        failures = scan_docs(docs, root)
        if not any(path == "docs/guide.md" for path, _, _ in failures):
            raise AssertionError("selftest failed to catch stray service count")

        write_fixture(
            root,
            README_REL,
            "# UDB\nREADME prose says 15 services / 188 RPCs without generated markers.\n",
        )
        failures = scan_docs([README_REL], root)
        if not any(path == README_REL for path, _, _ in failures):
            raise AssertionError("selftest failed to catch unguarded README count")


def main(argv: list[str]) -> int:
    list_files = False
    for arg in argv[1:]:
        if arg in ("--list", "-l"):
            list_files = True
        elif arg == "--selftest":
            run_selftest()
            print("doc service-count guard selftest passed")
            return 0
        elif arg in ("--help", "-h"):
            print(__doc__)
            return 0
        else:
            print(f"unknown argument: {arg}", file=sys.stderr)
            return 2

    docs = tracked_docs()
    if list_files:
        for rel_path in docs:
            print(f"scanning {rel_path}")

    failures = scan_docs(docs)

    if not failures:
        print("OK: no hand-maintained service/RPC counts outside the generated homes")
        return 0

    print(
        "Hand-maintained service/RPC count literals found outside "
        "docs/generated/ and the README generated block:\n",
        file=sys.stderr,
    )
    for rel_path, lineno, line in failures:
        print(f"  {rel_path}:{lineno}: {line.strip()}", file=sys.stderr)
    print(
        "\nThese counts have one source of truth (the embedded descriptor). "
        "Move the count into docs/generated/ (regenerate with `udb native docs`) "
        "or the README's <!-- BEGIN/END GENERATED:services --> block, or remove "
        "the literal.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
