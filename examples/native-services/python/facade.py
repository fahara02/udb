"""Phase 7 SDK ergonomics tour: the unified ``UdbProject`` facade.

One ``create_udb(...)`` call gives you data + auth + authz + the native control
plane behind a single shared config and metadata. This example shows:

1. Build the facade with one :class:`UdbConfig`.
2. authz ergonomics — ``require`` (raises on deny), ``explain`` (never raises),
   ``batch_can`` (many checks in one RPC), all routed through the TTL cache.
3. Token lifecycle — password ``login`` + single-flight ``refresh_if_needed``.
4. Convenience wrappers — ``send_notification`` / ``create_api_key`` /
   ``create_tenant``, plus storage/vector helpers under ``project.storage``.

Run (against a running broker with native auth):
    UDB_TARGET=127.0.0.1:50051 python facade.py
"""

from __future__ import annotations

import os
import sys

_SDK = os.path.join(os.path.dirname(__file__), "..", "..", "..", "sdk", "python")
sys.path.insert(0, _SDK)
sys.path.insert(0, os.path.join(_SDK, "gen"))

from udb_client import UdbAuthzDenied, UdbConfig, create_udb  # noqa: E402
from udb_client.auth import TokenSession  # noqa: E402

TARGET = os.getenv("UDB_TARGET", "127.0.0.1:50051")


def main() -> None:
    config = UdbConfig(
        target=TARGET,
        # auth_target defaults to target for single-process deployments.
        tenant_id="acme",
        project_id="billing",
        user_id="u-123",
        purpose="control-plane",
        correlation_id="facade-py-example",
        scopes=("udb:*",),
        service_identity="examples.facade-py",
    )

    with create_udb(config) as udb:
        # ── authz: require / explain / batch_can ─────────────────────────────
        try:
            udb.authz.require("invoice", "data.select")
            print("1) require data.select on invoice → allowed")
        except UdbAuthzDenied as denied:
            print(f"1) require denied: {denied}")

        decision = udb.authz.explain("invoice", "data.delete")
        print(
            f"2) explain data.delete → allowed={decision.allowed} "
            f"reason={decision.deny_reason!r}"
        )

        results = udb.authz.batch_can(
            [("invoice", "data.select"), ("invoice", "data.delete")]
        )
        print(f"3) batch_can → {results}")

        # ── token lifecycle: login + single-flight refresh ───────────────────
        session = TokenSession(udb.auth)
        username = os.getenv("UDB_USERNAME")
        password = os.getenv("UDB_PASSWORD")
        if username and password:
            session.login(username, password)
            token = session.access_token()  # refreshes transparently if near expiry
            print(f"4) logged in; access token len={len(token)}")
        else:
            print("4) set UDB_USERNAME/UDB_PASSWORD to exercise login + refresh")

        # ── convenience wrappers ─────────────────────────────────────────────
        # These call the native control-plane services; uncomment against a
        # broker that has them enabled:
        #   udb.send_notification("invoice.created", recipient_id="u-123")
        #   resp = udb.create_api_key("ci-key", scopes=["data.select"])
        #   udb.create_tenant(code="beta", name="Beta Co")
        print("5) facade ready: udb.data / udb.auth / udb.authz / udb.storage /")
        print("   udb.apikey / udb.tenant / udb.notification / udb.analytics")


if __name__ == "__main__":
    main()
