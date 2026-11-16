"""Delegated job tokens without a broker: the token-refusal reason helpers and
the request/response mapping of ``UdbAuthClient.exchange_token_for_job``."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import grpc
import pytest

from udb.entity.v1 import error_pb2
from udb_client import ErrorCode, code_of, is_token_expired, is_token_revoked
from udb_client.auth import DelegatedJobToken, UdbAuthClient
from udb_client.exceptions import UdbConfigurationError, UdbRpcError
from udb_client.metadata import Metadata


class _Refused(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode):
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return "refused"


def _refusal(reason: str) -> UdbRpcError:
    error = UdbRpcError("AuthnService/Authenticate", _Refused(grpc.StatusCode.UNAUTHENTICATED))
    error.error_detail = error_pb2.ErrorDetail(reason=reason, kind=error_pb2.ERROR_KIND_POLICY)
    return error


def test_expired_and_revoked_bearers_are_told_apart_and_stay_unauthenticated() -> None:
    expired = _refusal("UDB_TOKEN_EXPIRED")
    revoked = _refusal("UDB_TOKEN_REVOKED")
    assert code_of(expired) is ErrorCode.UNAUTHENTICATED
    assert code_of(revoked) is ErrorCode.UNAUTHENTICATED
    assert is_token_expired(expired) and not is_token_revoked(expired)
    assert is_token_revoked(revoked) and not is_token_expired(revoked)
    assert not is_token_expired(_refusal("UDB_AUTHENTICATION_FAILED"))
    assert not is_token_expired(ValueError("UDB_TOKEN_EXPIRED")), "text is never matched"
    assert not is_token_revoked(None)


class _FakeAuthn:
    def __init__(self) -> None:
        self.requests: list[Any] = []

    def ExchangeTokenForJob(self, request: Any, metadata: Any = None, timeout: Any = None) -> Any:
        self.requests.append(request)
        return SimpleNamespace(
            access_token="delegated.jwt",
            jti="dlg_1",
            expires_at_unix=1_900_000_000,
            subject="user-1",
            actor="spiffe://acme/exporter",
            purpose=request.purpose,
            job_id=request.job_id,
            scopes=["udb:read"],
            tenant_id="tenant-1",
            project_id="billing",
        )


def test_exchange_token_for_job_sends_the_request_and_returns_the_token() -> None:
    auth = UdbAuthClient(
        "127.0.0.1:1",
        Metadata(tenant_id="tenant-1", project_id="billing", purpose="test", correlation_id="c-1"),
    )
    fake = _FakeAuthn()
    auth.authn = fake
    try:
        token = auth.exchange_token_for_job("user.jwt", purpose="export", job_id="export-42", ttl_seconds=600)
        assert token == DelegatedJobToken(
            access_token="delegated.jwt",
            jti="dlg_1",
            expires_at_unix=1_900_000_000,
            subject="user-1",
            actor="spiffe://acme/exporter",
            purpose="export",
            job_id="export-42",
            scopes=("udb:read",),
            tenant_id="tenant-1",
            project_id="billing",
        )
        sent = fake.requests[0]
        assert (sent.subject_token, sent.purpose, sent.job_id, sent.ttl_seconds) == (
            "user.jwt",
            "export",
            "export-42",
            600,
        )
        # Refused locally, before any call.
        for kwargs in (
            dict(subject_token=" ", purpose="export", job_id="j"),
            dict(subject_token="t", purpose="", job_id="j"),
            dict(subject_token="t", purpose="export", job_id=""),
            dict(subject_token="t", purpose="export", job_id="j", ttl_seconds=-1),
        ):
            subject = kwargs.pop("subject_token")
            with pytest.raises(UdbConfigurationError):
                auth.exchange_token_for_job(subject, **kwargs)
        assert len(fake.requests) == 1
    finally:
        auth.close()
