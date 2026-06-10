"""Hand-written auth ergonomics over the generated AuthnService / AuthzService
stubs, mirroring :class:`udb_client.client.UdbClient`'s metadata convention.

The generated authn/authz stubs live in the buf ``gen`` tree
(``udb.core.authn.services.v1`` / ``udb.core.authz.services.v1``). They require
``buf generate --include-imports`` so the ``google/api`` annotation modules are
emitted alongside them.
"""

from __future__ import annotations

import hashlib
import hmac
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Protocol, Sequence, runtime_checkable

import grpc

from udb.core.authn.services.v1 import authn_service_pb2_grpc
from udb.core.authn.services.v1 import core_pb2 as authn
from udb.core.authz.services.v1 import authz_service_pb2_grpc
from udb.core.authz.services.v1 import core_pb2 as authz

from .exceptions import (
    UdbAuthzDenied,
    UdbConfigurationError,
    UdbPolicyBundleError,
    UdbRpcError,
)
from .metadata import Metadata

# Algorithm the broker stamps on a SignedPolicyBundle (see
# src/runtime/authz/bundle.rs). Only HMAC-SHA256 is supported by this verifier.
POLICY_BUNDLE_ALGORITHM = "HMAC-SHA256"


def verify_policy_bundle(
    signed: "authz.SignedPolicyBundle", secret: "str | bytes"
) -> "authz.SignedPolicyBundle":
    """Verify a :class:`SignedPolicyBundle`'s HMAC-SHA256 signature.

    The broker signs the ``bundle`` bytes with a keyed HMAC-SHA256 and emits the
    signature as **lowercase hex** (``src/runtime/authz/bundle.rs``). This
    recomputes the MAC over ``signed.bundle`` with ``secret`` and compares it to
    ``signed.signature`` in constant time via :func:`hmac.compare_digest`.

    Returns ``signed`` unchanged on success. Raises
    :class:`UdbPolicyBundleError` when the algorithm is unsupported or the
    signature does not match (e.g. a tampered bundle or the wrong secret).

    NOTE: the proto comment says the signature is Base64, but the server emits
    lowercase hex — this matches the server, which is the source of truth.
    """
    algorithm = signed.algorithm or POLICY_BUNDLE_ALGORITHM
    if algorithm != POLICY_BUNDLE_ALGORITHM:
        raise UdbPolicyBundleError(
            f"unsupported signature algorithm {algorithm!r} "
            f"(expected {POLICY_BUNDLE_ALGORITHM})",
            bundle=signed,
        )
    key = secret.encode("utf-8") if isinstance(secret, str) else secret
    expected = hmac.new(key, signed.bundle, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signed.signature):
        raise UdbPolicyBundleError("signature mismatch", bundle=signed)
    return signed


def as_resource(resource: "authz.ResourceRef | str") -> "authz.ResourceRef":
    """Coerce a resource string into an :class:`authz.ResourceRef`.

    Bare strings are treated as a fully-qualified message/resource name (the
    common case for proto-message authz), matching what the broker's
    ``BatchCheckPermissions`` does server-side (it copies ``object`` into both
    ``resource_name`` and ``message_type`` before enriching). A ``ResourceRef``
    is returned unchanged.
    """
    if isinstance(resource, str):
        return authz.ResourceRef(resource_name=resource, message_type=resource)
    return resource


def _resource_key(resource: "authz.ResourceRef") -> str:
    """The ``object`` string the server keys decisions by (message_type first)."""
    return resource.message_type or resource.resource_name or resource.table


class UdbAuthClient:
    """Convenience wrapper over AuthnService + AuthzService.

    The same :class:`Metadata` used for broker calls is attached to every auth
    RPC so the control plane sees a consistent tenant/identity/scope context.
    """

    def __init__(
        self,
        target: str,
        metadata: Metadata | None = None,
        *,
        secure: bool = False,
        root_certificates: bytes | None = None,
        timeout: float | None = 30.0,
        channel: grpc.Channel | None = None,
        policy_bundle_secret: str | bytes | None = None,
    ):
        if not target and channel is None:
            raise UdbConfigurationError("UDB target is required, e.g. '127.0.0.1:50051'")
        self._metadata = metadata
        self._timeout = timeout
        # HMAC secret used to verify SignedPolicyBundle signatures in
        # ``get_policy_bundle``. ``None`` skips verification (server still signs).
        self._policy_bundle_secret = policy_bundle_secret
        self._owns_channel = channel is None
        if channel is None:
            if secure:
                creds = grpc.ssl_channel_credentials(root_certificates)
                channel = grpc.secure_channel(target, creds)
            else:
                channel = grpc.insecure_channel(target)
        self._channel = channel
        self.authn = authn_service_pb2_grpc.AuthnServiceStub(channel)
        self.authz = authz_service_pb2_grpc.AuthzServiceStub(channel)
        # Lazily-built TTL cache so ``can``/``require``/``explain`` reuse the
        # server-controlled decision TTL without each caller wiring a cache.
        self._cache: "AuthzCache | None" = None

    @property
    def cache(self) -> "AuthzCache":
        """The shared :class:`AuthzCache` backing ``can``/``require``/``explain``."""
        if self._cache is None:
            self._cache = AuthzCache(self)
        return self._cache

    def bind_metadata(self, metadata: Metadata) -> None:
        self._metadata = metadata

    def close(self) -> None:
        if self._owns_channel:
            self._channel.close()

    def __enter__(self) -> "UdbAuthClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ── Authentication ────────────────────────────────────────────────────
    def authenticate(self, request: authn.AuthnRequest, *, metadata: Metadata | None = None):
        return self._call(self.authn.Authenticate, "Authenticate", request, metadata)

    def authenticate_bearer(self, token: str, *, metadata: Metadata | None = None):
        meta = self._effective_metadata(metadata)
        return self.authenticate(
            authn.AuthnRequest(
                bearer_token=token,
                tenant_hint=meta.tenant_id,
                project_hint=meta.project_id,
                requested_scopes=list(meta.scopes),
            ),
            metadata=metadata,
        )

    def authenticate_api_key(self, api_key: str, *, metadata: Metadata | None = None):
        meta = self._effective_metadata(metadata)
        return self.authenticate(
            authn.AuthnRequest(
                api_key=api_key,
                tenant_hint=meta.tenant_id,
                project_hint=meta.project_id,
                requested_scopes=list(meta.scopes),
            ),
            metadata=metadata,
        )

    def authenticate_session(self, session_id: str, *, metadata: Metadata | None = None):
        meta = self._effective_metadata(metadata)
        return self.authenticate(
            authn.AuthnRequest(
                session_id=session_id,
                tenant_hint=meta.tenant_id,
                project_hint=meta.project_id,
            ),
            metadata=metadata,
        )

    # ── Password login + token refresh (token lifecycle) ──────────────────
    def login(
        self,
        username: str,
        password: str,
        *,
        totp_code: str = "",
        mfa_otp_id: str = "",
        device_name: str = "",
        access_surface: str = "",
        metadata: Metadata | None = None,
    ) -> "authn.LoginResponse":
        """Password login (``AuthnService.Login``).

        Returns ``LoginResponse`` with ``access_token`` + ``refresh_token`` +
        ``access_token_expires_in`` (seconds). ``mfa_required`` indicates a
        second factor is needed; resubmit with ``totp_code``/``mfa_otp_id``.
        """
        meta = self._effective_metadata(metadata)
        request = authn.LoginRequest(
            username=username,
            password=password,
            totp_code=totp_code,
            mfa_otp_id=mfa_otp_id,
            device_name=device_name,
            access_surface=access_surface,
            tenant_hint=meta.tenant_id,
            project_hint=meta.project_id,
        )
        return self._call(self.authn.Login, "Login", request, metadata)

    def refresh_token(
        self,
        refresh_token: str = "",
        *,
        session_id: str = "",
        metadata: Metadata | None = None,
    ) -> "authn.RefreshTokenResponse":
        """Exchange a refresh token / session for a new short-lived access token.

        ``AuthnService.RefreshToken`` returns ``RefreshTokenResponse`` with a new
        ``access_token`` + ``access_token_expires_in``. NOTE: the server does not
        return a rotated refresh token here, so the caller keeps reusing the
        original refresh credential until logout.
        """
        request = authn.RefreshTokenRequest(
            refresh_token=refresh_token, session_id=session_id
        )
        return self._call(self.authn.RefreshToken, "RefreshToken", request, metadata)

    # ── Authorization ─────────────────────────────────────────────────────
    def authorize(self, request: authz.AuthzRequest, *, metadata: Metadata | None = None) -> authz.Decision:
        resp = self._call(self.authz.Authorize, "Authorize", request, metadata)
        return resp.decision

    def authorize_decision(
        self,
        resource: "authz.ResourceRef | str",
        action: str,
        *,
        purpose: str = "",
        metadata: Metadata | None = None,
    ) -> authz.Decision:
        """Single uncached ``Authorize`` call returning the full Decision."""
        meta = self._effective_metadata(metadata)
        return self.authorize(
            authz.AuthzRequest(
                principal=authz.Principal(
                    user_id=meta.user_id,
                    service_identity=meta.service_identity,
                    tenant_id=meta.tenant_id,
                    project_id=meta.project_id,
                    scopes=list(meta.scopes),
                ),
                tenant_id=meta.tenant_id,
                project_id=meta.project_id,
                resource=as_resource(resource),
                action=action,
                purpose=purpose or meta.purpose,
                requested_scopes=list(meta.scopes),
            ),
            metadata=metadata,
        )

    def can(
        self,
        resource: "authz.ResourceRef | str",
        action: str,
        *,
        purpose: str = "",
        metadata: Metadata | None = None,
        use_cache: bool = True,
    ) -> tuple[bool, authz.Decision]:
        """Return ``(allowed, decision)``; routed through the TTL cache by default."""
        if use_cache:
            return self.cache.can(
                as_resource(resource), action, purpose=purpose, metadata=metadata
            )
        decision = self.authorize_decision(
            resource, action, purpose=purpose, metadata=metadata
        )
        return decision.allowed, decision

    def require(
        self,
        resource: "authz.ResourceRef | str",
        action: str,
        *,
        purpose: str | None = None,
        metadata: Metadata | None = None,
        use_cache: bool = True,
    ) -> authz.Decision:
        """Authorize and raise :class:`UdbAuthzDenied` on deny.

        Returns the allowing :class:`Decision` so the caller can read the
        granted scopes / cache TTL. ``purpose`` defaults to the bound
        metadata's purpose when ``None``.
        """
        ref = as_resource(resource)
        allowed, decision = self.can(
            ref,
            action,
            purpose=purpose or "",
            metadata=metadata,
            use_cache=use_cache,
        )
        if not allowed:
            raise UdbAuthzDenied(_resource_key(ref), action, decision)
        return decision

    def explain(
        self,
        resource: "authz.ResourceRef | str",
        action: str,
        *,
        purpose: str | None = None,
        metadata: Metadata | None = None,
        use_cache: bool = False,
    ) -> authz.Decision:
        """Return the full :class:`Decision` without raising on deny.

        Defaults to a fresh (uncached) decision so the returned ``deny_reason``
        / ``matched_policy_ids`` / ``required_scopes`` reflect current policy.
        """
        _allowed, decision = self.can(
            resource,
            action,
            purpose=purpose or "",
            metadata=metadata,
            use_cache=use_cache,
        )
        return decision

    def batch_can(
        self,
        checks: Sequence[tuple[str, str]],
        *,
        metadata: Metadata | None = None,
    ) -> dict[tuple[str, str], bool]:
        """Evaluate many ``(object, action)`` permissions in one RPC.

        Uses ``AuthzService.BatchCheckPermissions`` with a
        ``BatchCheckPermissionsRequest`` of ``user_id`` + ``domain`` (tenant) +
        repeated ``PermissionCheck{object, action}``. The server returns a
        ``map<string, bool> results`` keyed by ``"object:action"``; this method
        maps those back onto the input ``(object, action)`` tuples.
        """
        meta = self._effective_metadata(metadata)
        request = authz.BatchCheckPermissionsRequest(
            user_id=meta.user_id or meta.service_identity,
            domain=meta.tenant_id,
            checks=[
                authz.PermissionCheck(object=obj, action=act) for obj, act in checks
            ],
        )
        resp = self._call(
            self.authz.BatchCheckPermissions,
            "BatchCheckPermissions",
            request,
            metadata,
        )
        results: dict[tuple[str, str], bool] = {}
        for obj, act in checks:
            results[(obj, act)] = bool(resp.results.get(f"{obj}:{act}", False))
        return results

    def check_access(self, request: authz.CheckAccessRequest, *, metadata: Metadata | None = None):
        return self._call(self.authz.CheckAccess, "CheckAccess", request, metadata)

    # ── Stage 2: native database fast-path access (item 138) ──────────────
    def get_native_access(
        self, request: authz.NativeAccessRequest, *, metadata: Metadata | None = None
    ) -> authz.NativeAccessResponse:
        return self._call(self.authz.GetNativeAccess, "GetNativeAccess", request, metadata)

    def native_access(
        self,
        resource: authz.ResourceRef,
        action: str,
        *,
        purpose: str = "",
        metadata: Metadata | None = None,
    ) -> authz.NativeAccessGrant | None:
        """Authorize and, when allowed, return the native-access grant.

        Returns ``None`` when access is allowed but the server minted no grant
        (native access not configured). Raises ``PermissionError`` on deny.
        """
        meta = self._effective_metadata(metadata)
        resp = self.get_native_access(
            authz.NativeAccessRequest(
                principal=authz.Principal(
                    user_id=meta.user_id,
                    service_identity=meta.service_identity,
                    tenant_id=meta.tenant_id,
                    project_id=meta.project_id,
                    scopes=list(meta.scopes),
                ),
                tenant_id=meta.tenant_id,
                project_id=meta.project_id,
                resource=resource,
                action=action,
                purpose=purpose or meta.purpose,
                requested_scopes=list(meta.scopes),
            ),
            metadata=metadata,
        )
        if resp.decision and not resp.decision.allowed:
            raise PermissionError(f"udb: native access denied: {resp.decision.deny_reason}")
        return resp.grant if resp.HasField("grant") else None

    # ── Stage 2: signed policy bundle (item 140) ──────────────────────────
    def get_policy_bundle(
        self,
        *,
        metadata: Metadata | None = None,
        verify: bool | None = None,
    ) -> authz.SignedPolicyBundle:
        """Fetch the signed policy bundle for the bound tenant/project.

        When a ``policy_bundle_secret`` was configured on this client, the
        returned :class:`SignedPolicyBundle` is HMAC-SHA256 verified before it is
        handed back; a mismatch raises :class:`UdbPolicyBundleError`. Pass
        ``verify=False`` to skip the check, or ``verify=True`` to require a
        secret be configured (raising :class:`UdbConfigurationError` otherwise).
        """
        meta = self._effective_metadata(metadata)
        resp = self._call(
            self.authz.GetPolicyBundle,
            "GetPolicyBundle",
            authz.PolicyBundleRequest(tenant_id=meta.tenant_id, project_id=meta.project_id),
            metadata,
        )
        bundle = resp.bundle
        should_verify = (
            self._policy_bundle_secret is not None if verify is None else verify
        )
        if should_verify:
            if self._policy_bundle_secret is None:
                raise UdbConfigurationError(
                    "policy bundle verification requested but no "
                    "policy_bundle_secret is configured"
                )
            verify_policy_bundle(bundle, self._policy_bundle_secret)
        return bundle

    # ── internals ─────────────────────────────────────────────────────────
    def _effective_metadata(self, metadata: Metadata | None) -> Metadata:
        effective = metadata or self._metadata
        if effective is None:
            raise UdbConfigurationError(
                "No UDB metadata is bound. Pass metadata=... or call bind_metadata()."
            )
        return effective

    def _call(self, method: Any, name: str, request: Any, metadata: Metadata | None):
        try:
            return method(
                request,
                metadata=self._effective_metadata(metadata).to_grpc_metadata(),
                timeout=self._timeout,
            )
        except grpc.RpcError as error:
            raise UdbRpcError(name, error) from error


@contextmanager
def native_transaction(connection: Any, grant: "authz.NativeAccessGrant") -> Iterator[Any]:
    """Run a transaction on a DB-API connection opened with ``grant.dsn``,
    applying the grant's ``app.current_*`` session variables with
    ``set_config(name, value, true)`` so the broker-generated RLS policies see
    the same request context UDB enforced. Commits on success, rolls back on
    error. Works with any PEP-249 driver (e.g. psycopg) using ``%s`` params.
    """
    cursor = connection.cursor()
    try:
        if grant is not None:
            for key, value in grant.session_variables.items():
                cursor.execute("SELECT set_config(%s, %s, true)", (key, value))
        yield cursor
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


class AuthzCache:
    """Thread-safe local authorization cache over :class:`UdbAuthClient`.

    Wraps :meth:`UdbAuthClient.can` with a TTL cache keyed by the
    (principal, resource, action, purpose) tuple. The TTL is the server's
    ``Decision.cache_ttl_seconds`` so UDB controls reuse; a zero TTL is never
    cached.
    """

    def __init__(self, client: UdbAuthClient, *, clock: Any = time.monotonic):
        self._client = client
        self._clock = clock
        self._lock = threading.RLock()
        self._cache: dict[tuple, tuple[float, authz.Decision]] = {}

    @staticmethod
    def _key(meta: Metadata, resource: authz.ResourceRef, action: str, purpose: str) -> tuple:
        subject = meta.user_id or meta.service_identity
        res = resource.message_type or resource.resource_name or resource.table
        return (meta.tenant_id, meta.project_id, subject, res, action, purpose)

    def can(
        self,
        resource: "authz.ResourceRef | str",
        action: str,
        *,
        purpose: str = "",
        metadata: Metadata | None = None,
    ) -> tuple[bool, authz.Decision]:
        resource = as_resource(resource)
        meta = self._client._effective_metadata(metadata)
        purpose = purpose or meta.purpose
        key = self._key(meta, resource, action, purpose)
        now = self._clock()
        with self._lock:
            hit = self._cache.get(key)
            if hit is not None and now < hit[0]:
                return hit[1].allowed, hit[1]
        # Always hit the uncached Authorize path to avoid recursing back into
        # the cache via ``UdbAuthClient.can``.
        decision = self._client.authorize_decision(
            resource, action, purpose=purpose, metadata=metadata
        )
        allowed = decision.allowed
        ttl = decision.cache_ttl_seconds
        if ttl > 0:
            with self._lock:
                self._cache[key] = (now + ttl, decision)
        return allowed, decision

    def invalidate(self) -> None:
        with self._lock:
            self._cache.clear()


# ── Token / session lifecycle ─────────────────────────────────────────────
@dataclass
class StoredToken:
    """A persisted credential set from login / refresh.

    ``expires_at`` is an absolute monotonic-ish wall clock (``time.time()``)
    deadline in seconds; ``None`` means unknown (never auto-refresh on time).
    """

    access_token: str = ""
    refresh_token: str = ""
    session_id: str = ""
    expires_at: float | None = None

    def is_expired(self, *, skew_seconds: float = 30.0, now: float | None = None) -> bool:
        if self.expires_at is None:
            return False
        clock = now if now is not None else time.time()
        return clock >= (self.expires_at - skew_seconds)


@runtime_checkable
class TokenStore(Protocol):
    """Pluggable persistence for the active :class:`StoredToken`.

    Implement ``load``/``save``/``clear`` to back the session with a file, OS
    keyring, Redis, etc. :class:`InMemoryTokenStore` is the default process-local
    implementation.
    """

    def load(self) -> StoredToken | None: ...

    def save(self, token: StoredToken) -> None: ...

    def clear(self) -> None: ...


class InMemoryTokenStore:
    """Thread-safe, process-local :class:`TokenStore` (the default)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._token: StoredToken | None = None

    def load(self) -> StoredToken | None:
        with self._lock:
            return self._token

    def save(self, token: StoredToken) -> None:
        with self._lock:
            self._token = token

    def clear(self) -> None:
        with self._lock:
            self._token = None


class TokenSession:
    """Login + single-flight refresh on top of :class:`UdbAuthClient`.

    Lifecycle:

    * :meth:`login` performs a password ``Login`` and persists the resulting
      access/refresh tokens + expiry into the :class:`TokenStore`.
    * :meth:`access_token` returns a valid bearer token, transparently calling
      :meth:`refresh_if_needed` first.
    * :meth:`refresh_if_needed` refreshes only when the stored token is near
      expiry, and does so under a single lock so concurrent callers share ONE
      ``RefreshToken`` RPC (the single-flight guarantee) instead of stampeding.

    Refresh shape note: ``RefreshToken`` returns a new ``access_token`` +
    ``access_token_expires_in`` but NOT a rotated refresh token, so the original
    refresh token / session_id is retained across refreshes. If the server later
    rotates refresh tokens, update :meth:`_apply_refresh` to read the new value.
    """

    def __init__(
        self,
        client: UdbAuthClient,
        *,
        store: TokenStore | None = None,
        skew_seconds: float = 30.0,
        clock: Any = time.time,
    ):
        self._client = client
        self._store: TokenStore = store or InMemoryTokenStore()
        self._skew = skew_seconds
        self._clock = clock
        self._lock = threading.Lock()

    @property
    def store(self) -> TokenStore:
        return self._store

    def current(self) -> StoredToken | None:
        return self._store.load()

    def login(
        self,
        username: str,
        password: str,
        *,
        totp_code: str = "",
        mfa_otp_id: str = "",
        metadata: Metadata | None = None,
    ) -> StoredToken:
        """Password login; store and return the resulting tokens."""
        resp = self._client.login(
            username,
            password,
            totp_code=totp_code,
            mfa_otp_id=mfa_otp_id,
            metadata=metadata,
        )
        expires_at = (
            self._clock() + resp.access_token_expires_in
            if resp.access_token_expires_in
            else None
        )
        token = StoredToken(
            access_token=resp.access_token,
            refresh_token=resp.refresh_token,
            session_id=resp.session_id,
            expires_at=expires_at,
        )
        self._store.save(token)
        return token

    def refresh_if_needed(
        self, *, force: bool = False, metadata: Metadata | None = None
    ) -> StoredToken | None:
        """Refresh the access token when near expiry, single-flight.

        Returns the (possibly refreshed) token, or ``None`` if nothing is
        stored. The whole check-and-refresh runs under one lock so concurrent
        callers collapse onto a single ``RefreshToken`` RPC; the second caller
        re-checks expiry inside the lock and reuses the just-refreshed token.
        """
        with self._lock:
            token = self._store.load()
            if token is None:
                return None
            if not force and not token.is_expired(
                skew_seconds=self._skew, now=self._clock()
            ):
                return token
            if not token.refresh_token and not token.session_id:
                # Nothing to refresh with; surface the stale token unchanged.
                return token
            return self._apply_refresh(token, metadata)

    def _apply_refresh(
        self, token: StoredToken, metadata: Metadata | None
    ) -> StoredToken:
        resp = self._client.refresh_token(
            token.refresh_token, session_id=token.session_id, metadata=metadata
        )
        expires_at = (
            self._clock() + resp.access_token_expires_in
            if resp.access_token_expires_in
            else None
        )
        refreshed = StoredToken(
            access_token=resp.access_token,
            # RefreshToken does not rotate the refresh token; retain it.
            refresh_token=token.refresh_token,
            session_id=token.session_id,
            expires_at=expires_at,
        )
        self._store.save(refreshed)
        return refreshed

    def access_token(self, *, metadata: Metadata | None = None) -> str:
        """Return a currently-valid bearer access token (refreshing if needed)."""
        token = self.refresh_if_needed(metadata=metadata)
        if token is None:
            raise UdbConfigurationError(
                "no stored token; call TokenSession.login(...) first"
            )
        return token.access_token

    def logout(self) -> None:
        """Drop the stored credential (server-side Logout is a separate RPC)."""
        self._store.clear()
