#!/usr/bin/env python3
"""Fail CI if vector canonical-store CAS posture drifts from the source plan."""

from __future__ import annotations

import argparse
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(root: Path, path: str) -> str:
    return (root / path).read_text(encoding="utf-8")


def require(source: str, needle: str, label: str) -> None:
    if needle not in source:
        raise SystemExit(f"missing {label}: {needle}")


def reject(source: str, needle: str, label: str) -> None:
    if needle in source:
        raise SystemExit(f"forbidden {label}: {needle}")


def check_vector_cas_posture(root: Path) -> None:
    qdrant = read(root, "src/runtime/canonical_store/qdrant.rs")
    vector = read(root, "src/runtime/canonical_store/vector_system.rs")
    plan = read(root, "UDB_MASTERPLAN_2026.md")

    reject(
        vector,
        "gives them a real canonical system-state plane",
        "stale vector-system capability wording",
    )
    require(
        vector,
        "Only Elasticsearch currently has a backend-native",
        "vector-system capability caveat",
    )
    require(
        qdrant,
        "full SystemStores registration still fails closed",
        "qdrant capability caveat",
    )

    require(
        qdrant,
        'Err(self.cas_unsupported("system state"))',
        "qdrant system-state fail-closed gate",
    )
    require(
        qdrant,
        'Err(self.cas_unsupported("advisory leases"))',
        "qdrant advisory-lease fail-closed gate",
    )
    require(
        qdrant,
        'Qdrant-native conditional write',
        "qdrant native-CAS diagnostic",
    )
    require(
        qdrant,
        "advisory_lease_fails_closed_until_qdrant_native_cas_exists",
        "qdrant fail-closed regression test",
    )

    require(
        vector,
        "VectorSystemClient::Elasticsearch(_) => Ok(())",
        "elasticsearch CAS-capable exception",
    )
    require(
        vector,
        'Err(self.cas_unsupported("system state"))',
        "non-ES vector system-state fail-closed gate",
    )
    require(
        vector,
        'Err(self.cas_unsupported("advisory leases"))',
        "non-ES vector advisory-lease fail-closed gate",
    )
    require(
        vector,
        'Err(self.cas_unsupported("outbox sequence allocation"))',
        "non-ES vector sequence-allocation fail-closed gate",
    )
    require(
        vector,
        "backend-native conditional write",
        "non-ES vector native-CAS diagnostic",
    )
    require(
        vector,
        "vector_native_cas_fail_closed_tests",
        "non-ES vector fail-closed regression tests",
    )

    require(
        plan,
        "Qdrant/Pinecone/Weaviate fail closed",
        "masterplan 3.2 non-ES vector fail-closed status",
    )


def write_fixture(root: Path, path: str, body: str) -> None:
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(body, encoding="utf-8")


def run_selftest() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        write_fixture(
            root,
            "src/runtime/canonical_store/qdrant.rs",
            """
// full SystemStores registration still fails closed
fn ensure_system_tables() { Err(self.cas_unsupported("system state")) }
fn try_acquire_advisory_lease() { Err(self.cas_unsupported("advisory leases")) }
fn diagnostic() { "Qdrant-native conditional write"; }
fn advisory_lease_fails_closed_until_qdrant_native_cas_exists() {}
""",
        )
        write_fixture(
            root,
            "src/runtime/canonical_store/vector_system.rs",
            """
// Only Elasticsearch currently has a backend-native CAS primitive.
fn ensure_cas_capable() { VectorSystemClient::Elasticsearch(_) => Ok(()) }
fn ensure_system_tables() { Err(self.cas_unsupported("system state")) }
fn try_acquire_advisory_lease() { Err(self.cas_unsupported("advisory leases")) }
fn next_seq_value() { Err(self.cas_unsupported("outbox sequence allocation")) }
fn diagnostic() { "backend-native conditional write"; }
mod vector_native_cas_fail_closed_tests {}
""",
        )
        write_fixture(
            root,
            "UDB_MASTERPLAN_2026.md",
            "P3.2: Qdrant/Pinecone/Weaviate fail closed until native CAS exists.\n",
        )
        check_vector_cas_posture(root)

        vector_path = root / "src/runtime/canonical_store/vector_system.rs"
        original_vector = vector_path.read_text(encoding="utf-8")
        vector_path.write_text(
            original_vector.replace(
                "Only Elasticsearch currently has a backend-native CAS primitive.",
                "All vector backends have a canonical system-state plane.",
            ),
            encoding="utf-8",
        )
        try:
            check_vector_cas_posture(root)
        except SystemExit as exc:
            if "vector-system capability caveat" not in str(exc):
                raise
        else:
            raise AssertionError("selftest failed to catch missing vector caveat")

        vector_path.write_text(
            original_vector
            + "\n// gives them a real canonical system-state plane\n",
            encoding="utf-8",
        )
        try:
            check_vector_cas_posture(root)
        except SystemExit as exc:
            if "stale vector-system capability wording" not in str(exc):
                raise
        else:
            raise AssertionError("selftest failed to catch stale capability wording")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check vector canonical-store CAS posture."
    )
    parser.add_argument(
        "--selftest",
        action="store_true",
        help="run fixture-based positive and negative checks",
    )
    args = parser.parse_args()

    if args.selftest:
        run_selftest()
        print("vector CAS posture guard selftest passed")
        return 0

    check_vector_cas_posture(ROOT)

    print("vector CAS posture guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
