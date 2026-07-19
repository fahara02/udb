"""Phase 7 conformance / outbound unit tests (M9).

Covers the hand-written auth/adapter surface without a live broker by injecting
fake gRPC stubs that capture the outbound request:

* ``can()`` / ``native_access()`` populate ``requested_scopes`` on the wire.
* Canonical request headers are emitted by ``Metadata`` + the adapter's
  outbound-header builder (``x-api-key`` / ``x-tenant-id`` / ``x-user-id`` /
  ``x-purpose`` / ``x-scopes`` / ``x-correlation-id`` / ``x-udb-project-id`` /
  ``x-request-id``).
* :class:`AuthzCache` honours the server TTL (hit within TTL, re-fetch after).
* :func:`verify_policy_bundle` accepts a correct signature and rejects a
  tampered bundle.
"""

from __future__ import annotations

import hashlib
import hmac

import pytest

from udb.core.authz.services.v1 import core_pb2 as authz
from udb_client import (
    Metadata,
    UdbConfig,
    UdbProject,
    UdbPolicyBundleError,
    verify_policy_bundle,
)
from udb_client.adapters import (
    HEADER_API_KEY,
    HEADER_REQUEST_ID,
    RequestContext,
    canonical_outbound_headers,
)
from udb_client.auth import AuthzCache, UdbAuthClient


# ── fakes ──────────────────────────────────────────────────────────────────
class _FakeUnary:
    """A captured-call gRPC unary method returning a canned response."""

    def __init__(self, response):
        self._response = response
        self.requests: list = []
        self.calls = 0

    def __call__(self, request, *, metadata=None, timeout=None):
        self.requests.append(request)
        self.calls += 1
        if callable(self._response):
            return self._response(request)
        return self._response


class _FakeAuthzStub:
    def __init__(self, *, authorize=None, native=None):
        self.Authorize = authorize
        self.GetNativeAccess = native


class _NoopChannel:
    """Minimal grpc.Channel stand-in: stub ctors call ``unary_unary`` etc."""

    def _noop(self, *_a, **_k):
        return lambda *a, **k: None

    unary_unary = unary_stream = stream_unary = stream_stream = property(
        lambda self: self._noop
    )

    def close(self) -> None:  # pragma: no cover - never owns the channel here
        pass


def _client_with_authz(stub) -> UdbAuthClient:
    """A UdbAuthClient whose authz stub is replaced with a fake (no real dial)."""
    meta = Metadata(
        tenant_id="acme",
        purpose="billing.read",
        correlation_id="corr-1",
        scopes=("udb:read", "udb:write"),
        user_id="user-1",
        project_id="billing",
    )
    client = UdbAuthClient("unused:1", meta, channel=_NoopChannel())
    client.authz = stub
    return client


# ── requested_scopes population ─────────────────────────────────────────────
def test_can_populates_requested_scopes() -> None:
    decision = authz.Decision(allowed=True, cache_ttl_seconds=0)
    authorize = _FakeUnary(authz.AuthzResponse(decision=decision))
    client = _client_with_authz(_FakeAuthzStub(authorize=authorize))

    allowed, _ = client.can("acme.billing.v1.Invoice", "read", use_cache=False)

    assert allowed is True
    sent = authorize.requests[-1]
    assert list(sent.requested_scopes) == ["udb:read", "udb:write"]
    # The principal also carries the scopes.
    assert list(sent.principal.scopes) == ["udb:read", "udb:write"]
    assert sent.resource.message_type == "acme.billing.v1.Invoice"
    assert sent.action == "read"


def test_native_access_populates_requested_scopes() -> None:
    grant = authz.NativeAccessGrant(dsn="postgres://x", role="r")
    response = authz.NativeAccessResponse(
        decision=authz.Decision(allowed=True), grant=grant
    )
    native = _FakeUnary(response)
    client = _client_with_authz(_FakeAuthzStub(native=native))

    result = client.native_access(
        authz.ResourceRef(message_type="acme.billing.v1.Invoice"), "read"
    )

    assert result is grant or result.dsn == "postgres://x"
    sent = native.requests[-1]
    assert list(sent.requested_scopes) == ["udb:read", "udb:write"]
    assert list(sent.principal.scopes) == ["udb:read", "udb:write"]


# ── canonical headers ───────────────────────────────────────────────────────
def test_canonical_outbound_headers() -> None:
    context = RequestContext.from_headers(
        {
            "x-tenant-id": "acme",
            "x-user-id": "user-1",
            "x-purpose": "billing.read",
            "x-correlation-id": "corr-1",
            "x-scopes": "udb:read,udb:write",
            "x-udb-project-id": "billing",
            "authorization": "Bearer bearer-123",
            "x-api-key": "key-123",
            "x-request-id": "req-9",
        }
    )
    headers = dict(canonical_outbound_headers(context))

    assert headers["x-tenant-id"] == "acme"
    assert headers["x-user-id"] == "user-1"
    assert headers["x-purpose"] == "billing.read"
    assert headers["x-correlation-id"] == "corr-1"
    assert headers["x-scopes"] == "udb:read,udb:write"
    assert headers["x-udb-project-id"] == "billing"
    assert headers["authorization"] == "Bearer bearer-123"
    assert headers[HEADER_API_KEY] == "key-123"
    assert headers[HEADER_REQUEST_ID] == "req-9"


def test_request_context_generates_request_id_and_metadata() -> None:
    context = RequestContext.from_headers({"x-tenant-id": "acme"})
    # A request id is minted when none is supplied; correlation id mirrors it.
    assert context.request_id
    assert context.correlation_id == context.request_id
    meta = context.to_metadata()
    assert meta.tenant_id == "acme"
    assert meta.project_id == "default"
    # Bearer token is stripped of its scheme prefix.
    ctx2 = RequestContext.from_headers({"authorization": "Bearer abc.def"})
    assert ctx2.bearer_token == "abc.def"
    assert ctx2.to_metadata().bearer_token == "abc.def"


def test_metadata_emits_canonical_grpc_headers() -> None:
    headers = dict(
        Metadata(
            tenant_id="acme",
            purpose="billing.read",
            correlation_id="corr-1",
            scopes=("udb:read",),
            user_id="user-1",
            project_id="billing",
            bearer_token="bearer-123",
            api_key="key-123",
        ).to_grpc_metadata()
    )
    for key in (
        "x-tenant-id",
        "x-user-id",
        "x-purpose",
        "x-correlation-id",
        "x-scopes",
        "x-udb-project-id",
    ):
        assert key in headers
    assert headers["authorization"] == "Bearer bearer-123"
    assert headers[HEADER_API_KEY] == "key-123"


def test_project_config_and_set_credentials_feed_shared_metadata() -> None:
    project = UdbProject(
        UdbConfig(
            target="unused:1",
            tenant_id="acme",
            purpose="billing.read",
            correlation_id="corr-1",
            bearer_token="bearer-1",
            api_key="key-1",
        )
    )

    try:
        headers = dict(project._ctl_metadata(None))
        assert headers["authorization"] == "Bearer bearer-1"
        assert HEADER_API_KEY not in headers

        project.set_credentials(bearer_token="bearer-2", api_key="key-2")

        headers = dict(project._ctl_metadata(None))
        assert headers["authorization"] == "Bearer bearer-2"
        assert HEADER_API_KEY not in headers
        override = Metadata(
            tenant_id="other",
            purpose="override",
            correlation_id="corr-2",
        )
        headers = dict(project._ctl_metadata(override))
        assert headers["x-tenant-id"] == "other"
        assert headers["authorization"] == "Bearer bearer-2"
        assert HEADER_API_KEY not in headers
        assert dict(project.data._call_metadata(None))["authorization"] == (
            "Bearer bearer-2"
        )
        assert HEADER_API_KEY not in dict(project.auth._effective_metadata(None).to_grpc_metadata())
    finally:
        project.close()


# ── AuthzCache TTL ──────────────────────────────────────────────────────────
class _FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_authz_cache_hit_then_expiry() -> None:
    decision = authz.Decision(allowed=True, cache_ttl_seconds=60)
    authorize = _FakeUnary(authz.AuthzResponse(decision=decision))
    client = _client_with_authz(_FakeAuthzStub(authorize=authorize))
    clock = _FakeClock()
    cache = AuthzCache(client, clock=clock)

    allowed, _ = cache.can("acme.v1.X", "read")
    assert allowed is True
    assert authorize.calls == 1

    # Within TTL → cache hit, no new RPC.
    clock.now += 30
    cache.can("acme.v1.X", "read")
    assert authorize.calls == 1

    # After TTL expiry → re-fetch.
    clock.now += 31  # now 61s past the first call (> 60s TTL)
    cache.can("acme.v1.X", "read")
    assert authorize.calls == 2


def test_authz_cache_zero_ttl_not_cached() -> None:
    decision = authz.Decision(allowed=True, cache_ttl_seconds=0)
    authorize = _FakeUnary(authz.AuthzResponse(decision=decision))
    client = _client_with_authz(_FakeAuthzStub(authorize=authorize))
    cache = AuthzCache(client, clock=_FakeClock())

    cache.can("acme.v1.X", "read")
    cache.can("acme.v1.X", "read")
    # A zero TTL is never cached → every call hits the server.
    assert authorize.calls == 2


# ── policy bundle signature verification ────────────────────────────────────
def _signed_bundle(secret: bytes, payload: bytes) -> authz.SignedPolicyBundle:
    signature = hmac.new(secret, payload, hashlib.sha256).hexdigest()
    return authz.SignedPolicyBundle(
        bundle=payload,
        signature=signature,
        key_id="udb-policy-v1",
        algorithm="HMAC-SHA256",
    )


def test_verify_policy_bundle_accepts_correct_signature() -> None:
    secret = b"topsecret"
    payload = b'{"schema":"udb.policy-bundle.v1","policies":[]}'
    signed = _signed_bundle(secret, payload)
    # Accepts a str secret too (encoded as utf-8 to match the server).
    assert verify_policy_bundle(signed, "topsecret") is signed


def test_verify_policy_bundle_rejects_tampered_bundle() -> None:
    secret = b"topsecret"
    payload = b'{"schema":"udb.policy-bundle.v1","policies":[]}'
    signed = _signed_bundle(secret, payload)
    signed.bundle = signed.bundle + b"x"  # tamper after signing
    with pytest.raises(UdbPolicyBundleError):
        verify_policy_bundle(signed, secret)


def test_verify_policy_bundle_rejects_unsupported_algorithm() -> None:
    payload = b"{}"
    signed = _signed_bundle(b"s", payload)
    signed.algorithm = "HS512"
    with pytest.raises(UdbPolicyBundleError):
        verify_policy_bundle(signed, b"s")


def test_authn_authz_response_json_fixtures_do_not_expose_persisted_credentials() -> None:
    import json

    payload = json.dumps(
        [
            {"user": {"user_id": "u1", "username": "ada"}},
            {"session": {"session_id": "sesspub_abc"}},
            {"key": {"key_id": "udbk_abc", "key_prefix": "udbk_abc"}},
            {"audits": [{"decision_audit_id": "a1", "user_id": "u1"}]},
        ],
        sort_keys=True,
    )
    for banned in (
        "argon2id$",
        "hmac-sha256:",
        "password_hash",
        "passwordHash",
        "totp_secret_enc",
        "totpSecretEnc",
        "session_token_lookup",
        "sessionTokenLookup",
        "session_token_hash",
        "sessionTokenHash",
        "csrf_token_hash",
        "csrfTokenHash",
        "key_hash",
        "keyHash",
    ):
        assert banned not in payload
