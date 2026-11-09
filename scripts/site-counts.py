#!/usr/bin/env python3
"""Keep the surface counts on the GitHub Pages site equal to the descriptor.

The site states how many RPCs and services UDB serves. Those numbers were typed by
hand and went stale with the first new RPC. Every count on the site is now a marked
element, `<span data-udb-count="KEY">N</span>` (an animated counter also carries
`data-count="N"`), and this script fills N from docs/generated/udb-native-contract.json,
the descriptor-derived contract `udb native manifest` writes.

    python scripts/site-counts.py           # rewrite the counts
    python scripts/site-counts.py --check   # exit 1 when a count differs (CI)
    python scripts/site-counts.py --selftest

Keys: data_rpcs (DataBroker), native_services, native_rpcs, total_rpcs.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "docs" / "generated" / "udb-native-contract.json"
SITE = ROOT / "docs" / "site"
DATA_BROKER = "udb.services.v1.DataBroker"
ELEMENT = re.compile(r'(<span(?P<attrs>[^>]*?)data-udb-count="(?P<key>[a-z_]+)"(?P<rest>[^>]*)>)(?P<n>\d+)(</span>)')


def counts(contract: dict) -> dict[str, int]:
    data_rpcs, native_services, native_rpcs = 0, 0, 0
    for service in contract["services"]:
        methods = len(service.get("rpcs") or service.get("methods") or [])
        if service["service"] == DATA_BROKER:
            data_rpcs = methods
        elif service.get("native_service"):
            native_services += 1
            native_rpcs += methods
    if not data_rpcs:
        raise SystemExit(f"{DATA_BROKER} is missing from {CONTRACT.relative_to(ROOT)}")
    return {
        "data_rpcs": data_rpcs,
        "native_services": native_services,
        "native_rpcs": native_rpcs,
        "total_rpcs": data_rpcs + native_rpcs,
    }


def render(text: str, values: dict[str, int]) -> str:
    def replace(match: re.Match) -> str:
        key = match.group("key")
        if key not in values:
            raise SystemExit(f"unknown site count key {key!r}")
        value = str(values[key])
        opening = re.sub(r'data-count="\d+"', f'data-count="{value}"', match.group(1))
        return f"{opening}{value}{match.group(6)}"

    return ELEMENT.sub(replace, text)


def selftest() -> None:
    values = {"data_rpcs": 84, "native_services": 28, "native_rpcs": 342, "total_rpcs": 426}
    text = '<b><span data-udb-count="native_rpcs">340</span> RPCs</b> <span data-count="84" data-udb-count="data_rpcs">80</span>'
    out = render(text, values)
    assert '<span data-udb-count="native_rpcs">342</span>' in out, out
    assert '<span data-count="84" data-udb-count="data_rpcs">84</span>' in out, out
    assert render(out, values) == out
    try:
        render('<span data-udb-count="nope">1</span>', values)
    except SystemExit:
        pass
    else:
        raise AssertionError("an unknown key must fail")
    print("site counts selftest passed")


def main() -> int:
    if "--selftest" in sys.argv:
        selftest()
        return 0
    check = "--check" in sys.argv
    values = counts(json.loads(CONTRACT.read_text(encoding="utf-8")))
    stale = []
    for page in sorted(SITE.glob("*.html")):
        before = page.read_text(encoding="utf-8")
        after = render(before, values)
        if after != before:
            stale.append(page.name)
            if not check:
                page.write_text(after, encoding="utf-8", newline="\n")
    summary = ", ".join(f"{key}={value}" for key, value in values.items())
    if check and stale:
        print(f"site counts differ from the descriptor ({summary}) in: {', '.join(stale)}")
        print("run: python scripts/site-counts.py")
        return 1
    print(f"site counts match the descriptor ({summary})" if check else f"site counts written ({summary}); updated: {', '.join(stale) or 'none'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
