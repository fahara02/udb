"""TokenSession keeps the broker's rotated refresh token.

The broker rotates the refresh token on every RefreshToken call and each one
works once; presenting a spent token is "invalid credential". A session that
kept the original token failed its second refresh, so a long-running service
lost its session about two access-token lifetimes after login.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from udb_client.auth import InMemoryTokenStore, StoredToken, TokenSession


class RotatingAuth:
    """Fake auth client whose refresh tokens are single-use, like the broker."""

    def __init__(self, first: str) -> None:
        self.live = first
        self.minted = 1
        self.presented: list[str] = []

    def refresh_token(self, refresh_token: str, *, session_id: str = "", metadata=None):
        self.presented.append(refresh_token)
        if refresh_token != self.live:
            raise RuntimeError("16 UNAUTHENTICATED: invalid credential")
        self.minted += 1
        self.live = f"refresh-{self.minted}"
        return SimpleNamespace(
            access_token=f"token-{self.minted}",
            access_token_expires_in=900,
            refresh_token=self.live,
            refresh_token_expires_in=86400,
            session_public_id="public-session",
        )


def test_repeated_refreshes_present_each_rotated_token() -> None:
    auth = RotatingAuth("refresh-1")
    store = InMemoryTokenStore()
    store.save(StoredToken(access_token="token-1", refresh_token="refresh-1",
                           session_id="legacy-session", expires_at=0.0))
    session = TokenSession(auth, store=store)

    for _ in range(4):
        session.refresh_if_needed(force=True)

    assert auth.presented == ["refresh-1", "refresh-2", "refresh-3", "refresh-4"]
    stored = store.load()
    assert stored.refresh_token == "refresh-5"
    assert stored.access_token == "token-5"
    assert stored.session_id == "legacy-session", "the legacy refresh session id is kept"


def test_legacy_session_refresh_without_a_rotated_token_keeps_the_stored_one() -> None:
    class LegacyAuth:
        def refresh_token(self, refresh_token: str, *, session_id: str = "", metadata=None):
            return SimpleNamespace(access_token="token-2", access_token_expires_in=900,
                                   refresh_token="", session_public_id="")

    store = InMemoryTokenStore()
    store.save(StoredToken(access_token="token-1", refresh_token="refresh-1",
                           session_id="legacy-session", expires_at=0.0))
    session = TokenSession(LegacyAuth(), store=store)

    session.refresh_if_needed(force=True)

    assert store.load().refresh_token == "refresh-1"
    assert store.load().access_token == "token-2"


def test_a_spent_refresh_token_is_refused_by_the_fake() -> None:
    # Guards the fake itself: without rotation the second refresh must fail,
    # which is exactly the production failure the first test prevents.
    auth = RotatingAuth("refresh-1")
    auth.refresh_token("refresh-1")
    with pytest.raises(RuntimeError, match="invalid credential"):
        auth.refresh_token("refresh-1")
