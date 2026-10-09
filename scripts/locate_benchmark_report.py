#!/usr/bin/env python3
"""Locate the unique canonical report in a downloaded benchmark artifact."""
from __future__ import annotations

import argparse
from pathlib import Path
import tempfile


def locate_report(directory: Path) -> Path:
    root = directory.resolve()
    reports = []
    for path in root.rglob("bench-results.json"):
        if path.relative_to(root).parts[-3:] != ("docs", "site", "bench-results.json"):
            continue
        if not path.is_file() or not path.resolve().is_relative_to(root):
            raise ValueError("benchmark report must be a regular file inside the artifact")
        reports.append(path)
    if len(reports) != 1:
        raise ValueError(f"benchmark artifact must contain exactly one canonical report; found {len(reports)}")
    return reports[0]


def selftest() -> None:
    with tempfile.TemporaryDirectory(prefix="udb-benchmark-layout-") as tmp:
        root = Path(tmp)
        for layout in ("docs/site", "home/runner/work/udb/udb/docs/site"):
            artifact = root / str(len(list(root.iterdir())))
            report = artifact / layout / "bench-results.json"
            report.parent.mkdir(parents=True)
            original = b'{"summary":{"failed_rpc_count":0}}\n'
            report.write_bytes(original)
            assert locate_report(artifact) == report.resolve()
            assert report.read_bytes() == original
        empty = root / "missing"
        empty.mkdir()
        unrelated = empty / "bench-results.json"
        unrelated.write_text("{}", encoding="utf-8")
        ambiguous = root / "ambiguous"
        for layout in ("docs/site", "home/runner/work/udb/udb/docs/site"):
            path = ambiguous / layout / "bench-results.json"
            path.parent.mkdir(parents=True)
            path.write_text("{}", encoding="utf-8")
        for invalid in (empty, ambiguous):
            try:
                locate_report(invalid)
            except ValueError:
                pass
            else:
                raise AssertionError("missing or ambiguous report must refuse")
    print("benchmark artifact layout selftest passed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", nargs="?", type=Path)
    parser.add_argument("--selftest", action="store_true")
    args = parser.parse_args()
    if args.selftest:
        selftest()
        return 0
    if args.directory is None:
        parser.error("directory is required")
    try:
        print(locate_report(args.directory))
    except ValueError as error:
        parser.exit(1, f"{error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
