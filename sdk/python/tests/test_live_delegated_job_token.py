"""Live delegated job tokens (``AuthnService.ExchangeTokenForJob``).

Skipped unless ``UDB_LIVE_SDK_TESTS=1`` and ``UDB_LIVE_SERVICE_ACCOUNT_TESTS=1``;
CI's "SDK service accounts (live)" job runs it after
``scripts/provision-service-account-live.py`` gave the service account's grant
``udb:token:delegate``.

It proves, through :meth:`UdbAuthClient.exchange_token_for_job`:

1. the service mints a delegated token for the human three times in a row:
   subject = the human, actor = the service, purpose and job id echoed, a
   ``dlg_`` jti, a bounded lifetime, never the delegate scope;
2. each token authenticates as the human and authorizes ``CheckAccess`` for the
   human on the native listener, repeatedly;
3. a delegated token is never exchanged again, a person cannot mint one, and a
   token that is not an access token is refused (``PERMISSION_DENIED``).
"""

from __future__ import annotations

import os
import time
import uuid
from dataclasses import replace

import grpc
import pytest

from udb.core.authz.services.v1 import core_pb2 as authz_pb2
from udb_client.auth import UdbAuthClient
from udb_client.exceptions import UdbRpcError
from udb_client.metadata import Metadata

pytestmark = pytest.mark.skipif(
    os.getenv("UDB_LIVE_SDK_TESTS") != "1" or os.getenv("UDB_LIVE_SERVICE_ACCOUNT_TESTS") != "1",
    reason="requires a live UDB broker and the CI service-account fixtures",
)

DELEGATE_SCOPE = "udb:token:delegate"
CHECK_ACCESS_SCOPE = "udb:authz:check-access"
CLOCK_SKEW = 60
MAX_TTL = 24 * 60 * 60
TIMEOUT = 15.0


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise AssertionError(f"{name} is required when UDB_LIVE_SDK_TESTS=1")
    return value


def test_live_delegated_job_token() -> None:
    target = required_env("UDB_GRPC_TARGET")
    auth_target = os.getenv("UDB_AUTH_GRPC_TARGET", "").strip() or target
    tenant_hint = required_env("UDB_LIVE_TENANT")
    project_hint = required_env("UDB_LIVE_PROJECT")

    hints = Metadata(
        tenant_id=tenant_hint,
        project_id=project_hint,
        purpose="python.live.delegated-job-token",
        correlation_id=f"python-live-delegated-{uuid.uuid4().hex}",
        service_identity="python.sdk.live",
    )
    auth = UdbAuthClient(auth_target, hints, timeout=TIMEOUT)
    try:
        human_login = auth.login(
            required_env("UDB_LIVE_USERNAME"),
            required_env("UDB_LIVE_PASSWORD"),
            device_name="python-live-delegated-job-token",
        )
        human = auth.authenticate_bearer(human_login.access_token).principal
        home_tenant = human.tenant_id
        assert home_tenant, "the human principal carries no canonical tenant_id"
        human_meta = replace(
            hints,
            tenant_id=home_tenant,
            project_id=human.project_id or project_hint,
            bearer_token=human_login.access_token,
        )
        service_login = auth.login(
            required_env("UDB_LIVE_SERVICE_USERNAME"),
            required_env("UDB_LIVE_SERVICE_PASSWORD"),
            device_name="python-live-delegated-job-token",
        )
        service = auth.authenticate_bearer(service_login.access_token).principal
        assert DELEGATE_SCOPE in list(service.scopes), (
            f"the service bearer must carry {DELEGATE_SCOPE} (provisioned grant); scopes={list(service.scopes)}"
        )
        service_meta = replace(
            hints,
            tenant_id=home_tenant,
            project_id=service.project_id or project_hint,
            bearer_token=service_login.access_token,
        )

        # ── 1. mint, repeatedly ───────────────────────────────────────────
        minted = []
        for _ in range(3):
            job_id = f"py-live-export-{uuid.uuid4()}"
            before = int(time.time())
            token = auth.exchange_token_for_job(
                human_login.access_token,
                purpose="export",
                job_id=job_id,
                ttl_seconds=600,
                metadata=service_meta,
            )
            assert token.jti.startswith("dlg_"), f"jti {token.jti!r}"
            assert token.subject == human.user_id, "the token acts for the user"
            assert token.actor == service.service_identity, "the service is the actor"
            assert (token.purpose, token.job_id, token.tenant_id) == ("export", job_id, home_tenant)
            assert before < token.expires_at_unix <= before + 600 + CLOCK_SKEW, token.expires_at_unix
            assert CHECK_ACCESS_SCOPE in token.scopes and DELEGATE_SCOPE not in token.scopes, token.scopes
            minted.append(token)
        before = int(time.time())
        capped = auth.exchange_token_for_job(
            human_login.access_token,
            purpose="export",
            job_id=f"py-live-cap-{uuid.uuid4()}",
            ttl_seconds=10_000_000,
            metadata=service_meta,
        )
        assert capped.expires_at_unix <= before + MAX_TTL + CLOCK_SKEW, "a long request is capped"

        # ── 2. use, repeatedly ────────────────────────────────────────────
        for token in minted:
            for _ in range(2):
                who = auth.authenticate_bearer(token.access_token).principal
                assert (who.user_id, who.tenant_id) == (human.user_id, home_tenant)
            decision = auth.check_access(
                authz_pb2.CheckAccessRequest(user_id=human.user_id, object="sdk-live-chart", action="read"),
                metadata=replace(human_meta, bearer_token=token.access_token),
            )
            assert decision.evaluated_subject == human.user_id, "the call is evaluated for the user"

        # ── 3. refusals ───────────────────────────────────────────────────
        refusals = {
            "re-exchange": (minted[0].access_token, service_meta),
            "a person minting": (human_login.access_token, human_meta),
            "a token that is not an access token": ("not.a.token", service_meta),
        }
        for step, (subject_token, caller) in refusals.items():
            with pytest.raises(UdbRpcError) as refused:
                auth.exchange_token_for_job(
                    subject_token,
                    purpose="export",
                    job_id=f"py-live-refused-{uuid.uuid4()}",
                    metadata=caller,
                )
            assert refused.value.code == grpc.StatusCode.PERMISSION_DENIED, (
                f"{step}: expected PERMISSION_DENIED, got {refused.value.code.name}: {refused.value.details}"
            )
    finally:
        auth.close()
