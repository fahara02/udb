#!/usr/bin/env python3
"""Render the Rust reason registry without compiling locally.

The Rust render_reasons_markdown unit test verifies this artifact independently.
Use --check for CI freshness; edit reasons! in error_reasons.rs to change it.
"""

import argparse
import json
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "src/runtime/error_reasons.rs"
OUTPUTS = (ROOT / "docs/error-reasons.md", ROOT / "docs/reference/error-reasons.md")
LITERAL = r'"(?:\\.|[^"\\])*"'
ENTRY = re.compile(
    rf'\s*[A-Z_]+\s*=\s*({LITERAL}),\s*(\w+),\s*(\w+),\s*'
    rf'({LITERAL}),\s*({LITERAL});'
)
HEADER = (
    "# Error reasons\n\n"
    "<!-- Generated from src/runtime/error_reasons.rs; edit the table there. -->\n\n"
    "Every refusal the broker sends carries a typed `ErrorDetail` (the "
    "`udb-error-detail-bin` trailer). Its `reason` is one of the codes below. "
    "A code is never renamed once shipped: branch on it, never on the message "
    "text. The SDKs expose it as `Reason()` / `reason`.\n\n"
    "Shared helper reasons retain the original gRPC code and kind. The "
    "table lists each reason's default code; a specific domain reason takes "
    "precedence over a shared helper reason.\n\n"
    "| Reason | Default gRPC code | Kind | When | Fix |\n"
    "|---|---|---|---|---|\n"
)


def render(source):
    table = source.split("\nreasons! {", 1)[1].split("\n}", 1)[0]
    output = HEADER
    position = 0
    codes = set()
    while table[position:].strip():
        match = ENTRY.match(table, position)
        if match is None:
            raise ValueError("unrecognized Rust reason declaration; update the generator")
        code, kind, status, summary, hint = match.groups()
        code, summary, hint = map(json.loads, (code, summary, hint))
        if not code.startswith("UDB_") or code in codes:
            raise ValueError(f"invalid or duplicate reason: {code}")
        codes.add(code)
        kind = "ERROR_KIND_" + re.sub(r"(?<!^)(?=[A-Z])", "_", kind).upper()
        output += f"| `{code}` | {status} | {kind} | {summary} | {hint} |\n"
        position = match.end()
    if not codes:
        raise ValueError("reason registry must not be empty")
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()
    output = render(SOURCE.read_text(encoding="utf-8"))
    if arguments.check:
        for target in OUTPUTS:
            if not target.exists() or target.read_text(encoding="utf-8") != output:
                raise SystemExit(f"{target.relative_to(ROOT)} stale; run python scripts/generate-error-reasons.py")
        print("error reasons up to date")
    else:
        for target in OUTPUTS:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(output, encoding="utf-8", newline="\n")
        print("rendered both error-reason references from the Rust registry")


if __name__ == "__main__":
    main()
