#!/usr/bin/env python3
"""Source guard for the active private masterplan todo-board status.

The active board intentionally has no unchecked numbered-chapter rows left.
The only non-closed rows are the 10 current proof/source tails in Chapters
14 and 15. This guard keeps README/PLAN/orchestration wording from drifting
away from the chapter rows.
"""

from __future__ import annotations

import argparse
import re
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TODO_ROOT = Path("private/masterplan/todos")

ROW_RE = re.compile(
    r"^-\s\[(?P<mark>[ x~])\]\s\*\*"
    r"(?P<id>(?:\d+\.A\.\d+|\d+(?:\.\d+)*))\s*[-\u2014]"
)
CHAPTER_RE = re.compile(r"^(?:0[1-9]|1[0-5])-.+\.md$")

EXPECTED_OPEN_BY_FILE: dict[str, tuple[str, ...]] = {
    "14-api-sdk-standardization.md": (
        "14.8.6",
        "14.9.9",
        "14.9.12",
    ),
    "15-ci-workflow-consolidation.md": ("15.A.5", "15.10.1"),
}
EXPECTED_OPEN_IDS = {
    todo_id for ids in EXPECTED_OPEN_BY_FILE.values() for todo_id in ids
}

EXPECTED_R7_OPEN_ITEMS = (
    "Commit Rust/proto/codegen changes separately from SDK helper aliases.",
    "Commit generated artifacts only from the official regen flow.",
    "Do not mix unrelated working-tree changes into the closeout commit.",
    "CI dry-run / remote CI verification. Public unauthenticated evidence",
)

DOC_REQUIREMENTS: dict[str, tuple[str, ...]] = {
    "private/masterplan/todos/README.md": (
        "active non-closed atomic rows remain",
        "all marked `[~]` rather than unchecked",
        "validation/live API/SDK proof rows",
        "Chapter 15 (2",
        "runner/parity proof rows",
        "There are no",
        "`[ ]` unchecked atomic rows in the numbered chapter files",
    ),
    "private/masterplan/todos/PLAN.md": (
        "5 active non-closed",
        "all are `[~]` partial/proof tails, not unchecked rows",
        "Chapter 14 (3) and Chapter 15 (2)",
    ),
    "private/masterplan/todos/00-orchestration.md": (
        "366 atomic todos across 15 numbered chapters",
        "remaining active work is 5",
        "`[~]` proof/source tails in Chapters 14 and 15",
        "no numbered chapter",
        "has an unchecked `[ ]` atomic row",
    ),
    "private/masterplan/revised_todo.md": (
        "The simple-client architecture is in place",
        "remaining work is evidence and",
        "2026-07-05 top-down todo audit",
        "The public root master-plan artifact had",
        "2026-07-09 update: both are CLOSED",
        "Chapter 05 has no active numbered proof tail left",
        "`05.2.3.1` BatchUpsert",
        "`05.6.1.1`",
        "Chapter 14 has three live/served validation tails",
        "`14.8.6`, `14.9.9`, and `14.9.12`",
        "Chapter 15 has two remote evidence tails: `15.A.5` and `15.10.1`",
        "The R7 closeout below remains the non-code landing bucket",
        "R7 landing plus the explicit live-proof",
    ),
}


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def chapter_files(root: Path) -> list[Path]:
    todo_root = root / TODO_ROOT
    return sorted(path for path in todo_root.glob("*.md") if CHAPTER_RE.match(path.name))


def private_board_available(root: Path) -> bool:
    return (root / TODO_ROOT).is_dir()


def scan_rows(root: Path) -> tuple[dict[str, tuple[str, str]], list[tuple[str, str]]]:
    rows: dict[str, tuple[str, str]] = {}
    unchecked: list[tuple[str, str]] = []
    for path in chapter_files(root):
        for line in read_text(path).splitlines():
            match = ROW_RE.match(line)
            if not match:
                continue
            todo_id = match.group("id")
            mark = match.group("mark")
            rows[todo_id] = (path.name, mark)
            if mark == " ":
                unchecked.append((path.name, todo_id))
    return rows, unchecked


def check(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    has_private_board = private_board_available(root)
    rows, unchecked = scan_rows(root)

    if has_private_board:
        if unchecked:
            formatted = ", ".join(f"{file}:{todo_id}" for file, todo_id in unchecked)
            failures.append(f"numbered chapter files still have unchecked rows: {formatted}")

        actual_open = {
            todo_id: file for todo_id, (file, mark) in rows.items() if mark != "x"
        }
        expected_open = {
            todo_id: file
            for file, ids in EXPECTED_OPEN_BY_FILE.items()
            for todo_id in ids
        }
        missing = sorted(set(expected_open) - set(actual_open))
        extra = sorted(set(actual_open) - set(expected_open))
        if missing:
            failures.append(f"expected open rows are closed or missing: {', '.join(missing)}")
        if extra:
            failures.append(f"unexpected non-closed rows found: {', '.join(extra)}")

        for todo_id, expected_file in sorted(expected_open.items()):
            actual = rows.get(todo_id)
            if not actual:
                continue
            actual_file, mark = actual
            if actual_file != expected_file:
                failures.append(f"{todo_id}: expected in {expected_file}, found in {actual_file}")
            if mark != "~":
                failures.append(f"{todo_id}: expected [~], found [{mark}]")

        for file, expected_ids in EXPECTED_OPEN_BY_FILE.items():
            actual_count = sum(1 for actual_file in actual_open.values() if actual_file == file)
            if actual_count != len(expected_ids):
                failures.append(f"{file}: expected {len(expected_ids)} open rows, found {actual_count}")

    for rel, needles in DOC_REQUIREMENTS.items():
        if rel.startswith("private/") and not has_private_board:
            continue
        path = root / rel
        if not path.is_file():
            failures.append(f"{rel}: file is missing")
            continue
        text = read_text(path)
        for needle in needles:
            if needle not in text:
                failures.append(f"{rel}: missing status text: {needle}")

    revised_path = root / "private/masterplan/revised_todo.md"
    if has_private_board and revised_path.is_file():
        revised_text = read_text(revised_path)
        for item in EXPECTED_R7_OPEN_ITEMS:
            marker = f"- [ ] {item}"
            if marker not in revised_text:
                failures.append(f"private/masterplan/revised_todo.md: missing open R7 item: {item}")
            closed_marker = f"- [x] {item}"
            if closed_marker in revised_text:
                failures.append(f"private/masterplan/revised_todo.md: R7 item was closed without guard update: {item}")

    return failures


def write_file(root: Path, rel: str, text: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def write_good_fixture(root: Path) -> None:
    todo_root = root / TODO_ROOT
    todo_root.mkdir(parents=True, exist_ok=True)
    for file, ids in EXPECTED_OPEN_BY_FILE.items():
        rows = ["- [x] **00.0.0 - closed fixture row**"]
        rows.extend(f"- [~] **{todo_id} - expected open fixture row**" for todo_id in ids)
        write_file(root, str(todo_root / file), "\n".join(rows) + "\n")
    for rel, needles in DOC_REQUIREMENTS.items():
        write_file(root, rel, "\n".join(needles) + "\n")
    revised_path = root / "private/masterplan/revised_todo.md"
    write_file(
        root,
        "private/masterplan/revised_todo.md",
        read_text(revised_path)
        + "\n".join(f"- [ ] {item}" for item in EXPECTED_R7_OPEN_ITEMS)
        + "\n",
    )


def run_selftest() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        write_good_fixture(root)
        failures = check(root)
        if failures:
            raise AssertionError(f"good fixture failed: {failures}")

        ch14 = root / TODO_ROOT / "14-api-sdk-standardization.md"
        ch14.write_text(read_text(ch14).replace("[~] **14.8.6", "[x] **14.8.6"), encoding="utf-8")
        failures = check(root)
        if not any("expected open rows are closed or missing" in failure for failure in failures):
            raise AssertionError(f"missing closed-row detection: {failures}")

        write_good_fixture(root)
        ch14 = root / TODO_ROOT / "14-api-sdk-standardization.md"
        ch14.write_text(read_text(ch14) + "- [ ] **14.99.9 - unexpected unchecked fixture row**\n", encoding="utf-8")
        failures = check(root)
        if not any("unchecked rows" in failure for failure in failures):
            raise AssertionError(f"missing unchecked-row detection: {failures}")

        write_good_fixture(root)
        readme = root / "private/masterplan/todos/README.md"
        readme.write_text(
            read_text(readme).replace(
                "active non-closed atomic rows remain",
                "active non-closed atomic rows drifted",
            ),
            encoding="utf-8",
        )
        failures = check(root)
        if not any("README.md: missing status text" in failure for failure in failures):
            raise AssertionError(f"missing doc-status detection: {failures}")

        write_good_fixture(root)
        revised = root / "private/masterplan/revised_todo.md"
        revised.write_text(read_text(revised).replace("- [ ] CI dry-run / remote CI verification", "- [x] CI dry-run / remote CI verification"), encoding="utf-8")
        failures = check(root)
        if not any("R7 item was closed without guard update" in failure for failure in failures):
            raise AssertionError(f"missing R7 closeout-item detection: {failures}")

    print("todo-board status guard selftest passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true", help="run no-repo fixture assertions")
    args = parser.parse_args()
    if args.selftest:
        return run_selftest()

    failures = check()
    if failures:
        for failure in failures:
            print(f"::error::{failure}")
        return 1
    print("todo-board status guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
