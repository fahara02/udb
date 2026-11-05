"""An embedded broker meets the same table contract as the fake and a live broker.

Skipped unless ``UDB_EMBEDDED_SDK_TESTS=1``. CI's embedded lane sets it and names its
compiled broker in ``UDB_TEST_BINARY``.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from udb_client import udbtest
from udb_client.udbtest import conformance

pytestmark = pytest.mark.skipif(
    os.getenv("UDB_EMBEDDED_SDK_TESTS") != "1",
    reason="requires an embedded broker (set UDB_EMBEDDED_SDK_TESTS=1 and UDB_TEST_BINARY)",
)

REPO = Path(__file__).resolve().parents[3]
CASES_FILE = REPO / "sdk" / "conformance" / "table-cases.json"
POLICY = """policies:
  - id: embedded-table-contract
    action: '*'
    resource: udb.core.storage.entity.v1.File
    purpose: udbtest.embedded
    required_scopes: [udb:write, udb:read]
"""


def test_an_embedded_broker_meets_the_table_contract(tmp_path: Path) -> None:
    policy = tmp_path / "policies.yaml"
    policy.write_text(POLICY, encoding="utf-8")
    with udbtest.start_pair(str(REPO / "proto"), str(policy)) as broker:
        tenants = [broker.client.canonical_tenant_id, broker.peer.canonical_tenant_id]
        by_tenant = dict(zip(tenants, (broker.client, broker.peer)))
        connect = lambda tenant: by_tenant.get(tenant, broker.client)  # noqa: E731
        conformance.run_table(connect, tenants)
        conformance.run_table_cases(connect, CASES_FILE, tenants)
