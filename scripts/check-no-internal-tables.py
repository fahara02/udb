#!/usr/bin/env python3
"""No-Internal-Tables lint (masterplan §12): a published SDK helper must NOT use
`GenericDispatch` to run SQL against an internal `udb_*` service schema to repair
or seed native state.

`GenericDispatch` is the legitimate raw escape hatch for *user* data-plane SQL,
and it is exposed by every language's raw generated client. What this lint bans is
a *published helper* (the hand-written / templated robustness + workflow layer that
ships to app developers) reaching into the broker's own private control-plane
tables (`udb_system`, `udb_authn`, `udb_authz`, `udb_notification`, `udb_cdc`,
`udb_tenant`). Those repairs belong to served RPCs / bootstrap paths
(device-via-Login, the env-gated FAILED-log, EnsureBaseline), not to SDK code.

Scope / exclusions (a match is only a violation in a *published helper* file):
  - EXCLUDE generated proto/gRPC stubs        (`sdk/*/gen/**`)
  - EXCLUDE the raw generated robustness client itself — that file legitimately
    *binds* the GenericDispatch method (generatedClient.ts, generated_client.go,
    generated_client.py, Generated/GeneratedClient.php, GeneratedUdbClient.java,
    GeneratedClient.cs)
  - EXCLUDE all test/harness files            (`*/tests/**`, `*_test.*`,
    `*.test.ts`, `*/Live/**`, `dist*/**`) and build/log artifacts
  - EXCLUDE proto sources and markdown/log files (not shipped helper code)

A violation = a published helper file where a `GenericDispatch` token appears
within WINDOW lines of a `udb_(system|authn|authz|notification|cdc|tenant)`
schema token (the co-location heuristic: a raw internal-table dispatch). The
lint prints the offending file:line and the served-RPC / bootstrap remediation.

Exit code 0 = clean; 1 = a published helper violates the rule; 2 = usage/IO error.

Run locally:
  python scripts/check-no-internal-tables.py
  python scripts/check-no-internal-tables.py --selftest
CI wires this into the same Linux `rust` job as the other doc/coverage guards.
"""

from __future__ import annotations

import re
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SDK = REPO / "sdk"

# Co-location window: a GenericDispatch and an internal-schema token within this
# many lines of each other in the same file is treated as a raw internal-table
# dispatch. Small enough to avoid an unrelated coincidental pairing, large enough
# to span a multi-line request body.
WINDOW = 6

DISPATCH_RE = re.compile(r"GenericDispatch")
INTERNAL_SCHEMA_RE = re.compile(r"udb_(?:system|authn|authz|notification|cdc|tenant)\b")

# Source extensions that can be a published helper (per shipped SDK language).
SOURCE_SUFFIXES = {".ts", ".go", ".py", ".php", ".java", ".cs"}

# Path-fragment exclusions (POSIX-style, matched against the repo-relative path).
# A file is skipped if any fragment is a path component / substring match.
EXCLUDE_DIR_PARTS = {"gen", "tests", "test", "Live", "node_modules", "vendor"}

# The raw generated robustness clients (legitimate GenericDispatch binding).
RAW_BINDING_NAMES = {
    "generatedclient.ts",
    "generatedclient.js",
    "generated_client.go",
    "generated_client.py",
    "generatedclient.php",
    "generatedudbclient.java",
}


def is_excluded(path: Path, root: Path = REPO) -> bool:
    rel = path.relative_to(root)
    parts = rel.parts
    # Generated stubs, test trees, dist outputs, Live integration dirs.
    for part in parts:
        if part in EXCLUDE_DIR_PARTS:
            return True
        if part.startswith("dist"):
            return True
    name = path.name.lower()
    # Test files by naming convention.
    if name.endswith(".test.ts") or name.endswith(".test.js"):
        return True
    if "_test." in name or name.startswith("test_"):
        return True
    # The raw generated robustness client itself binds GenericDispatch — allowed.
    if name in RAW_BINDING_NAMES:
        return True
    return False


def candidate_files(root: Path = REPO) -> list[Path]:
    files: list[Path] = []
    sdk = root / "sdk"
    if not sdk.is_dir():
        print(f"error: SDK tree not found at {sdk}", file=sys.stderr)
        sys.exit(2)
    for path in sdk.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix not in SOURCE_SUFFIXES:
            continue
        if is_excluded(path, root):
            continue
        files.append(path)
    return sorted(files)


def violations_in(path: Path) -> list[tuple[int, str]]:
    """Return (line_no, line_text) for each GenericDispatch co-located with an
    internal-schema token within WINDOW lines."""
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return []
    lines = text.splitlines()
    # Pre-index the lines that mention an internal schema token.
    schema_lines = [i for i, ln in enumerate(lines) if INTERNAL_SCHEMA_RE.search(ln)]
    if not schema_lines:
        return []
    hits: list[tuple[int, str]] = []
    for i, ln in enumerate(lines):
        if not DISPATCH_RE.search(ln):
            continue
        # GenericDispatch on this line: is an internal-schema token nearby?
        if any(abs(j - i) <= WINDOW for j in schema_lines):
            hits.append((i + 1, ln.strip()))
    return hits


def scan(root: Path = REPO) -> list[str]:
    files = candidate_files(root)
    failures: list[str] = []
    for path in files:
        for line_no, line_text in violations_in(path):
            rel = path.relative_to(root)
            failures.append(f"{rel}:{line_no}: {line_text}")
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
            "sdk/typescript/project.ts",
            "export function ok() { return 'served RPC only'; }\n",
        )
        write_fixture(
            root,
            "sdk/typescript/generatedClient.ts",
            "const method = 'GenericDispatch';\nconst table = 'udb_system';\n",
        )
        write_fixture(
            root,
            "sdk/typescript/tests/helper.test.ts",
            "GenericDispatch({ sql: 'select * from udb_authn.users' });\n",
        )
        failures = scan(root)
        if failures:
            raise AssertionError(f"selftest clean fixture failed: {failures}")

        write_fixture(
            root,
            "sdk/typescript/project.ts",
            "\n".join(
                [
                    "export function bad() {",
                    "  const req = { sql: 'select * from udb_system.bootstrap_state' };",
                    "  return GenericDispatch(req);",
                    "}",
                    "",
                ]
            ),
        )
        failures = scan(root)
        normalized = [failure.replace("\\", "/") for failure in failures]
        if not any("sdk/typescript/project.ts" in failure for failure in normalized):
            raise AssertionError("selftest failed to catch internal GenericDispatch helper")


def main(argv: list[str]) -> int:
    if len(argv) > 2 or (len(argv) == 2 and argv[1] not in ("--selftest", "--help", "-h")):
        print(f"unknown argument: {argv[1]}", file=sys.stderr)
        return 2
    if len(argv) == 2 and argv[1] in ("--help", "-h"):
        print(__doc__)
        return 0
    if len(argv) == 2 and argv[1] == "--selftest":
        run_selftest()
        print("no-internal-tables guard selftest passed")
        return 0

    files = candidate_files()
    failures = scan()

    if failures:
        print("::error::No-internal-tables rule violated: a published SDK helper")
        print("runs GenericDispatch against an internal udb_* control-plane schema.")
        print()
        for f in failures:
            print(f"  {f}")
        print()
        print(
            "Fix: do not repair/seed native state with raw GenericDispatch SQL "
            "against udb_* tables from shipped SDK code. Use the served RPC / "
            "bootstrap path instead (device-via-Login, the env-gated FAILED-log, "
            "or DataBroker.EnsureBaseline). Test-only harness usage is allowed "
            "but must live under tests/ / *_test.* / *.test.ts / Live/."
        )
        return 1

    print(
        f"No-internal-tables OK — scanned {len(files)} published SDK helper file(s); "
        "no GenericDispatch against a udb_* control-plane schema."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
