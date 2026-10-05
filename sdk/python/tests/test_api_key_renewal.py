"""API-key bearer renewal on :class:`UdbProject` (capture fakes, no live broker).

An API key is exchanged via ``Authn.Authenticate`` for a short-lived bearer that
carries NO refresh token. The project must retain the key, re-exchange it before
expiry, fall back to the raw ``x-api-key`` header when no bearer can be
obtained, and stop renewing on ``close()``.
"""

from __future__ import annotations

import logging
import time

import pytest

from udb_client import UdbConfig, UdbProject

_authn = pytest.importorskip("udb.core.authn.services.v1.core_pb2")


def _minted(token: str, ttl: int):
    def make(request):
        resp = _authn.AuthnResponse(
            access_token=token, expires_at_unix=int(time.time()) + ttl
        )
        resp.principal.tenant_id = "tenant-uuid"
        return resp

    return make


class _FakeAuthenticate:
    def __init__(self, *responses):
        self._responses = list(responses)
        self.requests: list = []
        self.metadata: list = []

    def __call__(self, request, *, metadata=None, timeout=None):
        self.requests.append(request)
        self.metadata.append(dict(metadata or ()))
        make = self._responses[min(len(self.requests) - 1, len(self._responses) - 1)]
        return make(request)


@pytest.fixture
def project():
    proj = UdbProject(UdbConfig(target="unused:1", tenant_id="acme"))
    try:
        yield proj
    finally:
        proj.close()


def _headers(proj: UdbProject) -> dict:
    return dict(proj._ctl_metadata(None))


def test_bearer_is_reexchanged_with_retained_key_at_80_percent(project) -> None:
    fake = _FakeAuthenticate(_minted("bearer-1", 900), _minted("bearer-2", 900))
    project.auth.authn.Authenticate = fake
    project.authenticate_api_key_and_adopt("svc-key")

    assert project.config.api_key == "", "key scrubbed from the public config"
    assert _headers(project)["authorization"] == "Bearer bearer-1"
    assert "x-api-key" not in _headers(project)
    assert project._api_key_thread is not None and project._api_key_thread.is_alive()

    # 170s left of 900s: past the 80% mark, outside the plain 60s skew.
    now = time.time()
    project._api_key_issued_at = now - 730
    project._api_key_expires_at = now + 170
    project._api_key_renewal_tick()

    assert [r.api_key for r in fake.requests] == ["svc-key", "svc-key"]
    assert "authorization" not in fake.metadata[1], "no stale bearer on re-exchange"
    assert _headers(project)["authorization"] == "Bearer bearer-2"
    assert "x-api-key" not in _headers(project)


def test_not_due_tick_is_a_noop(project) -> None:
    fake = _FakeAuthenticate(_minted("bearer-1", 900))
    project.auth.authn.Authenticate = fake
    project.authenticate_api_key_and_adopt("svc-key")
    project._api_key_renewal_tick()
    assert len(fake.requests) == 1


def test_empty_access_token_falls_back_to_raw_key(project, caplog) -> None:
    def empty(request):
        resp = _authn.AuthnResponse(access_token="")
        resp.principal.tenant_id = "tenant-uuid"
        return resp

    project.auth.authn.Authenticate = _FakeAuthenticate(empty)
    with caplog.at_level(logging.WARNING, logger="udb_client"):
        project.authenticate_api_key_and_adopt("svc-key")

    headers = _headers(project)
    assert headers["x-api-key"] == "svc-key"
    assert "authorization" not in headers
    assert project.metadata.tenant_id == "tenant-uuid"
    assert project._api_key_thread is None, "nothing to renew"
    assert len(caplog.records) == 1
    assert "svc-key" not in caplog.text, "warning must not leak the key"


def test_failing_reexchange_near_expiry_falls_back_then_recovers(project) -> None:
    state = {"fail": True}

    def flaky(request):
        if state["fail"]:
            raise RuntimeError("authn unavailable")
        return _minted("bearer-2", 900)(request)

    project.auth.authn.Authenticate = _FakeAuthenticate(_minted("bearer-1", 900), flaky)
    project.authenticate_api_key_and_adopt("svc-key")

    now = time.time()
    project._api_key_issued_at = now - 890
    project._api_key_expires_at = now + 10
    project._api_key_renewal_tick()

    assert project._api_key_failures == 1
    assert project.api_key_renewal_error() is not None
    assert project._api_key_next_delay() >= 1.0, "backoff, no hot loop"
    headers = _headers(project)
    assert headers["x-api-key"] == "svc-key"
    assert "authorization" not in headers

    state["fail"] = False
    project._api_key_renewal_tick()
    assert project._api_key_failures == 0
    headers = _headers(project)
    assert headers["authorization"] == "Bearer bearer-2"
    assert "x-api-key" not in headers


def test_blip_while_bearer_still_valid_keeps_bearer(project) -> None:
    def boom(request):
        raise RuntimeError("blip")

    project.auth.authn.Authenticate = _FakeAuthenticate(_minted("bearer-1", 900), boom)
    project.authenticate_api_key_and_adopt("svc-key")
    now = time.time()
    project._api_key_issued_at = now - 800
    project._api_key_expires_at = now + 150
    project._api_key_renewal_tick()
    headers = _headers(project)
    assert headers["authorization"] == "Bearer bearer-1"
    assert "x-api-key" not in headers


def test_close_stops_renewal_thread_and_drops_key(project) -> None:
    project.auth.authn.Authenticate = _FakeAuthenticate(_minted("bearer-1", 900))
    project.authenticate_api_key_and_adopt("svc-key")
    thread = project._api_key_thread
    assert thread is not None and thread.is_alive()
    project.close()
    assert not thread.is_alive()
    assert project._api_key_secret == ""
