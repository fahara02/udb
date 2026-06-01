"""Progressive, simplest→advanced tour of UDB's native auth services from Python.

This is the *consumer* side — how an application authenticates callers and makes
authorization decisions against the UDB control plane via the ``udb_client``
SDK's ``UdbAuthClient`` wrapper:

1. Authenticate an API key → resolved principal (simplest).
2. Authorize a resource/action — ``can`` (allowed vs. denied).
3. Native DB fast-path grant — ``native_access`` (advanced, Stage 2).

Provisioning users/roles/keys is an admin concern; do it once with the Go
example (``examples/native-services/go``) or your admin tooling, then export the
minted key as ``UDB_API_KEY`` for this consumer to authenticate.

Prerequisites:
  - A running UDB broker with native auth (e.g. the repo integration stack).
  - The generated stubs on PYTHONPATH: from ``sdk/python`` run
        buf generate --include-imports     # emits the udb.* packages
    and ``pip install grpcio``.

Run:
    UDB_TARGET=127.0.0.1:50051 UDB_API_KEY=udbk_... python main.py
"""

from __future__ import annotations

import os
import sys

# Make the in-repo SDK importable when run from this folder without installing.
# `udb_client` lives under sdk/python; the generated `udb.*` stubs under
# sdk/python/gen (buf output). Insert gen FIRST so `udb.core.*` resolves there.
_SDK = os.path.join(os.path.dirname(__file__), "..", "..", "..", "sdk", "python")
sys.path.insert(0, _SDK)
sys.path.insert(0, os.path.join(_SDK, "gen"))

from udb_client.auth import UdbAuthClient  # noqa: E402
from udb_client.metadata import Metadata  # noqa: E402
from udb.core.authz.services.v1 import core_pb2 as authz  # noqa: E402

TARGET = os.getenv("UDB_TARGET", "127.0.0.1:50051")
API_KEY = os.getenv("UDB_API_KEY", "")


def main() -> None:
    meta = Metadata(
        tenant_id="acme",
        project_id="billing",
        purpose="control-plane",
        correlation_id="native-py-example",
        scopes=("udb:*",),
        service_identity="examples.native-py",
    )

    with UdbAuthClient(TARGET, meta) as auth:
        # ── Step 1 (simplest): authenticate an API key → principal ───────────
        if API_KEY:
            principal = auth.authenticate_api_key(API_KEY).principal
            print(f"1) api key authenticated → user_id={principal.user_id} scopes={list(principal.scopes)}")
        else:
            print("1) set UDB_API_KEY (mint one with the Go example) to see authentication")

        # ── Step 2: authorize a resource/action ──────────────────────────────
        invoice = authz.ResourceRef(resource_name="invoice", message_type="invoice")
        allowed, decision = auth.can(invoice, "data.select")
        print(f"2) can data.select on invoice → {allowed} (decision_id={decision.decision_id})")
        denied, _ = auth.can(invoice, "data.delete")
        print(f"   can data.delete on invoice → {denied}")

        # ── Step 3 (advanced): native DB fast-path grant ─────────────────────
        try:
            grant = auth.native_access(invoice, "data.select")
            if grant is None:
                print("3) access allowed; no native grant minted (server native-access not configured)")
            else:
                print(
                    f"3) native grant: role={grant.role} session_vars={len(grant.session_variables)} "
                    "(open a psycopg conn on grant.dsn, then auth.native_transaction(...))"
                )
        except PermissionError as exc:
            print(f"3) native access denied: {exc}")


if __name__ == "__main__":
    main()
