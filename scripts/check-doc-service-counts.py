#!/usr/bin/env python3
"""Doc-drift guard: README / native-services.md native-service counts must match
the generated native contract.

The native control-plane service/RPC counts in the prose (README.md,
docs/native-services.md) have silently drifted from the contract before (the
contract grew Asset/Storage/WebRTC/IdentityProvider/ControlPlane services while
the prose still said "6 services, 77 RPCs"). This script asserts the prose states
the SAME native-service count and native-RPC total that
docs/generated/udb-native-contract.json enumerates, so the drift cannot recur
unnoticed.

"Native" here means every service in the contract EXCEPT the data-plane
`udb.services.v1.DataBroker` (which is the public broker listener, counted
separately in the prose as "76 RPCs").

Exit code 0 = in sync; 1 = drift (prints what to fix); 2 = usage/IO error.

Run locally:  python scripts/check-doc-service-counts.py
CI wires this into the same Linux job that regenerates the contract manifest.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CONTRACT = REPO / "docs" / "generated" / "udb-native-contract.json"
DATA_PLANE_SERVICE = "udb.services.v1.DataBroker"

# Files that must state the native-service count and native-RPC total, and the
# exact phrases (with the authoritative numbers substituted) each must contain.
# Keeping the expected phrases explicit makes the failure message actionable.
DOCS = [
    REPO / "README.md",
    REPO / "docs" / "native-services.md",
]


def load_native_totals() -> tuple[int, int]:
    """Return (native_service_count, native_rpc_total) from the contract."""
    if not CONTRACT.exists():
        print(
            f"error: contract not found at {CONTRACT}. "
            "Generate it with: udb native manifest > docs/generated/udb-native-contract.json",
            file=sys.stderr,
        )
        sys.exit(2)
    data = json.loads(CONTRACT.read_text(encoding="utf-8"))
    services = data.get("services")
    if not isinstance(services, list):
        print("error: contract has no 'services' array", file=sys.stderr)
        sys.exit(2)
    native = [s for s in services if s.get("service") != DATA_PLANE_SERVICE]
    service_count = len(native)
    rpc_total = 0
    for s in native:
        # Prefer the precomputed rpc_count; fall back to len(rpcs).
        n = s.get("rpc_count")
        if n is None:
            n = len(s.get("rpcs") or [])
        rpc_total += int(n)
    return service_count, rpc_total


def main() -> int:
    service_count, rpc_total = load_native_totals()

    # The prose must state the native service count and RPC total. Accept the two
    # equivalent phrasings the docs use — plain ("15 services") and the explicit
    # "native" form ("15 native services") — so the guard enforces the *number*
    # against the contract without over-fitting to one wording (the brand pass
    # reworded "N services / M RPCs" → "N native services / M native RPCs").
    needle_variants = [
        (f"{service_count} services", f"{service_count} native services"),
        (f"{rpc_total} RPCs", f"{rpc_total} native RPCs"),
    ]

    failures: list[str] = []
    for doc in DOCS:
        if not doc.exists():
            failures.append(f"{doc} is missing")
            continue
        text = doc.read_text(encoding="utf-8")
        for variants in needle_variants:
            if not any(variant in text for variant in variants):
                failures.append(
                    f"{doc.relative_to(REPO)} does not state '{variants[0]}' "
                    f"(or '{variants[1]}') "
                    f"(contract says {service_count} native services, {rpc_total} RPCs)"
                )

    if failures:
        print("Doc service-count drift detected:")
        for f in failures:
            print(f"  - {f}")
        print()
        print(
            f"Fix: update the prose to state '{service_count} services' and "
            f"'{rpc_total} RPCs' for the native control plane "
            "(authoritative source: docs/generated/udb-native-contract.json)."
        )
        return 1

    print(
        f"OK: README.md and docs/native-services.md agree with the contract "
        f"({service_count} native services, {rpc_total} RPCs)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
