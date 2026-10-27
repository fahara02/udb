"""Live refresh-token rotation conformance.

Skipped unless ``UDB_LIVE_SDK_TESTS=1``; CI's "SDK refresh-token rotation
(live)" job runs it against the broker it just built, on every push to main.

The broker rotates the refresh token on every ``AuthnService.RefreshToken``
call and each one works once. An SDK that kept the token it logged in with
refreshed once and then failed with UNAUTHENTICATED "invalid credential" — a
production outage no single-refresh test could catch. This drives the SDK's
own refresh path (``TokenSession.refresh_if_needed(force=True)``) several
times in a row.
"""

from __future__ import annotations

import os
from dataclasses import replace

import grpc
import pytest

from udb.core.authn.entity.v1 import enums_pb2 as authn_enum_pb
from udb.core.authn.services.v1 import authn_service_pb2_grpc as authn_grpc
from udb.core.authn.services.v1 import core_pb2 as authn_pb2
from udb_client.auth import InMemoryTokenStore, UdbAuthClient
from udb_client.exceptions import UdbRpcError
from udb_client.metadata import Metadata

pytestmark = pytest.mark.skipif(
    os.getenv("UDB_LIVE_SDK_TESTS") != "1",
    reason="requires live UDB broker",
)

REFRESHES = 3


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise AssertionError(f"{name} is required when UDB_LIVE_SDK_TESTS=1")
    return value


def test_live_refresh_token_rotation_survives_repeated_refreshes() -> None:
    target = required_env("UDB_GRPC_TARGET")
    auth_target = os.getenv("UDB_AUTH_GRPC_TARGET", target)
    meta = Metadata(
        tenant_id=os.getenv("UDB_LIVE_TENANT", "sdk-live"),
        project_id=os.getenv("UDB_LIVE_PROJECT", "default"),
        purpose="python.live.refresh-rotation",
        correlation_id="python-live-refresh-rotation",
        service_identity="python.sdk.live",
    )
    auth = UdbAuthClient(auth_target, meta, timeout=10.0)
    channel = grpc.insecure_channel(auth_target)
    try:
        store = InMemoryTokenStore()
        session = auth.login_session(store)
        login = session.login(required_env("UDB_LIVE_USERNAME"), required_env("UDB_LIVE_PASSWORD"))
        first = login.refresh_token
        assert login.access_token and first, "login must store an access and a refresh token"

        # The login JWT carries the canonical tenant UUID, not the human code;
        # authenticated calls must assert that tenant.
        principal = auth.authenticate_bearer(login.access_token).principal
        assert principal.tenant_id, "the verified principal must carry its canonical tenant"
        authn = authn_grpc.AuthnServiceStub(channel)

        previous = first
        for i in range(1, REFRESHES + 1):
            refreshed = session.refresh_if_needed(force=True)
            assert refreshed is not None and refreshed.access_token, f"refresh {i} stored no access token"
            assert refreshed.refresh_token, f"refresh {i} stored no refresh token"
            assert refreshed.refresh_token != previous, (
                f"refresh {i}: the refresh token did not change — the broker stopped "
                "rotating it or the SDK kept the spent one"
            )
            assert store.load().refresh_token == refreshed.refresh_token, f"refresh {i}: rotated token not persisted"

            # An authenticated call carrying the refreshed bearer.
            authed = replace(
                meta,
                tenant_id=principal.tenant_id,
                project_id=principal.project_id or meta.project_id,
                bearer_token=refreshed.access_token,
            )
            validated = authn.ValidateToken(
                authn_pb2.ValidateTokenRequest(
                    token=refreshed.access_token, token_type=authn_enum_pb.TOKEN_TYPE_JWT_ACCESS
                ),
                metadata=authed.to_grpc_metadata(),
                timeout=8.0,
            )
            assert validated.valid, f"refresh {i}: the refreshed access token does not validate"
            previous = refreshed.refresh_token

        # The first refresh token was spent three rotations ago: the broker must
        # refuse it. (Replaying the immediately-previous token instead would trip
        # reuse detection and revoke every session of this shared live user.)
        with pytest.raises(UdbRpcError) as refused:
            auth.refresh_token(first)
        assert refused.value.code == grpc.StatusCode.UNAUTHENTICATED, (
            f"spent refresh token returned {refused.value.code.name}: {refused.value.details}"
        )
    finally:
        channel.close()
        auth.close()
