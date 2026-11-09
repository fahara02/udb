"""The one-call production-path setup: :func:`connect_enterprise`.

It dials the data and auth targets, logs in with a username and password, verifies
the bearer, adopts the canonical tenant UUID from the verified principal, and keeps
the bearer current for the life of the session:

* a background thread refreshes it shortly before it expires;
* a call that finds it stale joins the same single refresh;
* a refresh token the broker refuses leads to a fresh login with the same
  credentials, so a revoked session does not stay dead until a restart;
* a renewal that comes back as a different principal is terminal, and every later
  call is refused locally rather than sent as someone else.

Use :attr:`EnterpriseSession.canonical_tenant_id`, never the human tenant code, in
tenant-scoped records and filters.
"""

from __future__ import annotations

import asyncio
import re
import threading
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from typing import Any, Sequence

import grpc

from udb.core.authn.services.v1 import core_pb2 as authn

from ._credentials import (
    RenewalFlight,
    UdbCredentialBindingError,
    UdbCredentialError,
    check_renewed_principal,
    credential_refusal,
    identity_from_principal,
    without_credentials,
)
from .exceptions import UdbConfigurationError, UdbError
from .metadata import Metadata
from .project import UdbAsyncProject, UdbConfig, UdbProject

_REFRESH_SKEW_S = 30.0  # refresh this long before expiry, at most
_REFRESH_MIN_S = 1.0  # floor between attempts, so failures never busy-loop
_REFRESH_IDLE_S = 300.0  # cadence when the token carries no expiry

_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


class UdbTenantError(UdbError):
    """A tenant value is not verified, not canonical, or not the session's tenant."""


@dataclass
class TenantState:
    """The tenant-code to canonical-UUID transition, kept explicit.

    The human code (``"acme"``) is only a pre-login hint. After login the broker
    verifies the canonical tenant UUID, and that is the value every tenant-scoped
    record and filter must use. Conflating the two makes reads miss and writes
    stamp an unusable tenant, so this fails fast on a mismatch.
    """

    #: The human code supplied before login. Never written into tenant-scoped data.
    tenant_code: str = ""
    #: The verified canonical tenant UUID. Empty until :meth:`adopt` succeeds.
    canonical_id: str = ""
    is_verified: bool = False

    def adopt(self, canonical_id: str) -> None:
        """Record the canonical tenant from the verified principal.

        Raises :class:`UdbTenantError`, leaving the state unchanged, unless the
        value is a lowercase hyphenated UUID: row security compares it to the
        tenant column, so any other form would miss rows.
        """
        value = canonical_id.strip()
        if not value:
            raise UdbTenantError("udb: cannot adopt an empty canonical tenant_id")
        if not _UUID.fullmatch(value) or value != value.lower():
            raise UdbTenantError(
                f"udb: canonical tenant_id {canonical_id!r} is not a canonical "
                "(lowercase hyphenated) UUID"
            )
        self.canonical_id = value
        self.is_verified = True

    def get_verified(self) -> str:
        """The canonical tenant UUID. Raises :class:`UdbTenantError` before login."""
        if not self.is_verified or not self.canonical_id:
            raise UdbTenantError(
                "udb: tenant not verified; call connect_enterprise before tenant-scoped operations"
            )
        return self.canonical_id

    def validate_tenant_id(self, record_tenant_id: str) -> None:
        """Raise when ``record_tenant_id`` differs from the verified tenant.

        An empty value passes: the caller fills it from :meth:`get_verified`.
        """
        canonical = self.get_verified()
        if record_tenant_id and record_tenant_id != canonical:
            raise UdbTenantError(
                f"udb: tenant_id mismatch: record/filter has {record_tenant_id!r} "
                f"but the authenticated tenant is {canonical!r}"
            )


@dataclass
class EnterpriseConfig:
    """What :func:`connect_enterprise` needs. ``target``, ``username`` and ``password`` are required."""

    target: str = ""
    #: Control-plane target; defaults to ``target``.
    auth_target: str = ""
    username: str = ""
    password: str = field(default="", repr=False)
    #: The human tenant code hint used before login. The verified UUID replaces it.
    tenant_code: str = ""
    project_id: str = ""
    purpose: str = "python.request"
    scopes: Sequence[str] = ()
    user_id: str = ""
    tls: bool = False
    root_certificates: bytes | None = None
    #: PEM client certificate chain + key for a mutual-TLS endpoint; both or neither.
    client_certificate_chain: bytes | None = None
    client_private_key: bytes | None = field(default=None, repr=False)
    deadline: float | None = 30.0
    channel_options: Sequence[tuple[str, Any]] | None = None
    retry: Any = None


@dataclass
class _Token:
    access_token: str = ""
    refresh_token: str = ""
    session_id: str = ""
    expires_at: float | None = None
    issued_at: float | None = None

    def refresh_skew(self, skew: float) -> float:
        # A configured margin must not consume a short token's entire life.
        skew = max(skew, 0.0)
        if self.issued_at is not None and self.expires_at is not None:
            lifetime = self.expires_at - self.issued_at
            if lifetime > 0:
                skew = min(skew, lifetime / 5)
        return skew

    def valid(self, now: float, skew: float) -> bool:
        if not self.access_token:
            return False
        if self.expires_at is None:
            return True
        return now + self.refresh_skew(skew) < self.expires_at


class EnterpriseSession(UdbProject):
    """A :class:`UdbProject` whose verified identity and bearer the SDK maintains.

    Everything on :class:`UdbProject` is here (``.data``, ``.storage``, ``.flags``
    and so on), and calls carry the current bearer. ``tenant_state`` is the
    :class:`TenantState`; ``.tenant`` stays the TenantService stub, as on any
    project. The session owns its credential, so ``set_credentials`` has no effect
    on calls made with ``session.metadata``.
    """

    def __init__(self, config: EnterpriseConfig):
        if not config.target or not config.username or not config.password:
            raise UdbConfigurationError(
                "udb: connect_enterprise requires target, username, and password"
            )
        super().__init__(
            UdbConfig(
                target=config.target,
                auth_target=config.auth_target or config.target,
                # A pre-login hint; the canonical UUID replaces it below.
                tenant_id=config.tenant_code or "default",
                project_id=config.project_id,
                purpose=config.purpose,
                scopes=tuple(config.scopes),
                user_id=config.user_id,
                service_identity="",
                tls=config.tls,
                root_certificates=config.root_certificates,
                client_certificate_chain=config.client_certificate_chain,
                client_private_key=config.client_private_key,
                deadline=config.deadline,
                channel_options=config.channel_options,
                retry=config.retry,
            )
        )
        self._enterprise = config
        self._clock = time.time
        self.refresh_skew = _REFRESH_SKEW_S
        # Guards the bearer, poison state and refresh flight.
        self._state_lock = threading.Lock()
        self._token = _Token()
        self._bearer = ""
        self._last_refresh_error: BaseException | None = None
        self._poisoned = False
        self._flight: RenewalFlight | None = None
        self._needs_recovery = False
        self._terminal_error: BaseException | None = None
        self._stop_refresh = threading.Event()
        self._refresh_thread: threading.Thread | None = None
        try:
            token, principal = self._login_and_verify()
            self.tenant_state = TenantState(tenant_code=config.tenant_code)
            self.tenant_state.adopt(principal.tenant_id)
            self._adopt_principal(principal, token)
        except BaseException:
            self.close()
            raise
        self._start_refresh_loop()

    def _adopt_principal(self, principal: Any, token: _Token) -> None:
        identity = identity_from_principal(self._metadata, principal, token.access_token)
        self.canonical_tenant_id = principal.tenant_id
        self.canonical_project_id = principal.project_id
        self._principal = _copy_principal(principal)
        self._pinned = identity
        self._token = token
        self._bearer = f"Bearer {token.access_token}"
        self.config.tenant_id = principal.tenant_id
        self.config.project_id = principal.project_id
        self.config.user_id = principal.user_id
        self.config.service_identity = principal.service_identity
        self.config.scopes = tuple(principal.scopes)
        self.config.bearer_token = token.access_token
        self.bind_metadata(identity)

    # ── identity and login ────────────────────────────────────────────────
    def _owns_credential(self) -> bool:
        return True

    def _recovery_metadata(self) -> Metadata:
        return without_credentials(self._metadata)

    def _login_and_verify(self) -> tuple[_Token, Any]:
        config = self._enterprise
        meta = self._recovery_metadata()
        received_at = self._clock()
        login = self.auth._call(
            self.auth.authn.Login,
            "Login",
            authn.LoginRequest(
                username=config.username,
                password=config.password,
                # Only the verified principal is ever used for scope; these are hints.
                tenant_hint=config.tenant_code,
                project_hint=config.project_id,
            ),
            meta,
        )
        if not login.access_token:
            raise UdbError(
                f"udb: Login returned no access token (MFA required: {login.mfa_required})"
            )
        verified = self.auth.authenticate_bearer(login.access_token, metadata=meta)
        if not verified.HasField("principal"):
            raise UdbError("udb: AuthenticateBearer returned no principal")
        token = _Token(
            access_token=login.access_token,
            refresh_token=login.refresh_token,
            session_id=login.session_id,
            issued_at=received_at,
        )
        if login.access_token_expires_in > 0:
            token.expires_at = received_at + login.access_token_expires_in
        # Verification takes time; the broker's expiry does not count from its end.
        if verified.expires_at_unix > 0:
            token.expires_at = float(verified.expires_at_unix)
        if not token.valid(self._clock(), 0):
            raise UdbError("udb: verified Login bearer is empty or expired")
        return token, verified.principal

    def _check_renewed_principal(self, principal: Any) -> None:
        check_renewed_principal(principal, self._principal, self._pinned, self._metadata)

    def _relogin(self) -> _Token:
        token, principal = self._login_and_verify()
        self._check_renewed_principal(principal)
        return token

    def _refresh(self, previous: _Token) -> None:
        meta = self._recovery_metadata()
        received_at = self._clock()
        response = self.auth.refresh_token(
            previous.refresh_token, session_id=previous.session_id, metadata=meta
        )
        token = replace(previous, access_token=response.access_token)
        if response.access_token_expires_in > 0:
            token.issued_at = received_at
            token.expires_at = received_at + response.access_token_expires_in
        # The presented refresh token is single-use, so the rotated one must be
        # kept. The reply omits it only for a legacy session-id refresh.
        if response.refresh_token:
            token.refresh_token = response.refresh_token
        verified = self.auth.authenticate_bearer(token.access_token, metadata=meta)
        self._check_renewed_principal(verified.principal)
        if verified.expires_at_unix > 0:
            token.expires_at = float(verified.expires_at_unix)
        if not token.valid(self._clock(), 0):
            raise UdbError("udb: refresh returned an empty or expired bearer")
        with self._state_lock:
            self._token = token

    # ── refresh ───────────────────────────────────────────────────────────
    def refresh_if_needed(self, *, force: bool = False) -> None:
        """Refresh the bearer when it is near expiry, or now with ``force=True``.

        Concurrent callers share one refresh and one outcome. A refused refresh
        falls back to a fresh login; after any failed attempt the next one logs in
        again rather than resubmit a refresh token that may already be spent.
        """
        with self._state_lock:
            if self._terminal_error is not None:
                raise self._terminal_error
            if not force and self._token.valid(self._clock(), self.refresh_skew):
                return
            running = self._flight
            flight = running or RenewalFlight()
            self._flight = flight
            previous = self._token
            needs_recovery = self._needs_recovery
        if running is not None:
            running.done.wait()
            if running.error is not None:
                raise running.error
            return

        error = self._run_refresh(previous, needs_recovery)
        with self._state_lock:
            if isinstance(error, UdbCredentialBindingError):
                self._terminal_error = error
            self._needs_recovery = error is not None
            flight.error = error
            self._flight = None
            flight.done.set()
        if error is not None:
            raise error

    def _run_refresh(self, previous: _Token, needs_recovery: bool) -> BaseException | None:
        error: BaseException | None = None
        if needs_recovery:
            error = UdbError("udb: prior refresh outcome requires re-login")
        else:
            try:
                self._refresh(previous)
            except Exception as exc:  # noqa: BLE001 - recorded and re-raised by the caller
                error = exc
        if error is not None and not isinstance(error, UdbCredentialBindingError):
            try:
                token = self._relogin()
                if not token.valid(self._clock(), 0):
                    raise UdbError("udb: recovery returned an empty or expired bearer")
                with self._state_lock:
                    self._token = token
                error = None
            except UdbCredentialBindingError as exc:
                error = exc
            except Exception as exc:  # noqa: BLE001 - recorded and re-raised by the caller
                error = UdbError(f"udb: renewal and re-login failed: {error}; {exc}")
        if error is None:
            try:
                self._publish_bearer()
            except Exception as exc:  # noqa: BLE001 - recorded and re-raised by the caller
                error = exc
        return error

    def _publish_bearer(self) -> None:
        with self._state_lock:
            token = self._token
            if not token.valid(self._clock(), 0):
                raise UdbError("udb: session bearer is empty or expired")
            bearer = f"Bearer {token.access_token}"
            self._last_refresh_error = None
            self._poisoned = False
            if bearer == self._bearer:
                return
            self._bearer = bearer
            self.config.bearer_token = token.access_token
            self.bind_metadata(replace(self._metadata, bearer_token=token.access_token))

    def _record_credential_error(self, error: BaseException) -> None:
        # A transient failure while the bearer is still valid must not fail closed.
        with self._state_lock:
            self._last_refresh_error = error
            self._poisoned = isinstance(
                error, UdbCredentialBindingError
            ) or not self._token.valid(self._clock(), 0)

    def _next_refresh_wait(self) -> float:
        with self._state_lock:
            token = self._token
        if token.expires_at is None:
            return _REFRESH_IDLE_S
        wait = token.expires_at - token.refresh_skew(self.refresh_skew) - self._clock()
        return wait if wait > 0 else _REFRESH_MIN_S

    def _refresh_loop(self) -> None:
        while not self._stop_refresh.wait(self._next_refresh_wait()):
            try:
                self.refresh_if_needed()
                self._publish_bearer()
            except Exception as exc:  # noqa: BLE001 - the loop must keep running
                self._record_credential_error(exc)

    def _start_refresh_loop(self) -> None:
        thread = threading.Thread(target=self._refresh_loop, name="udb-enterprise-refresh", daemon=True)
        self._refresh_thread = thread
        thread.start()

    # ── what calls use ────────────────────────────────────────────────────
    def _credential_refusal(self) -> BaseException | None:
        closed = super()._credential_refusal()
        if closed is not None:
            return closed
        with self._state_lock:
            if not self._poisoned:
                return None
            cause = self._last_refresh_error
        if cause is None:
            return UdbCredentialError("udb: bearer refresh failed, session not authorized")
        return UdbCredentialError(f"udb: bearer refresh failed, session not authorized: {cause}")

    def _current_bearer(self) -> str:
        refusal = self._credential_refusal()
        if refusal is not None:
            raise refusal
        try:
            self.refresh_if_needed()
            self._publish_bearer()
        except Exception as exc:  # noqa: BLE001 - a failed renewal refuses the call
            self._record_credential_error(exc)
            raise credential_refusal(exc) from exc
        with self._state_lock:
            return self._bearer

    # ── public surface ────────────────────────────────────────────────────
    def data_metadata(self, metadata: Metadata | None = None) -> Metadata:
        """Metadata for a data-plane call. Raises when the session is poisoned.

        Pass ``as_user(...)`` metadata to keep that user's bearer.
        """
        return self._checked(metadata)

    def native_metadata(self, metadata: Metadata | None = None) -> Metadata:
        """Metadata for a native-service call. Raises when the session is poisoned."""
        return self._checked(metadata)

    def _checked(self, metadata: Metadata | None) -> Metadata:
        refusal = self._credential_refusal()
        if refusal is not None:
            raise refusal
        return metadata or self._metadata

    def refresh_error(self) -> BaseException | None:
        """The latest refresh failure, or ``None`` if the last one succeeded."""
        with self._state_lock:
            return self._last_refresh_error

    def credential_error(self) -> BaseException | None:
        """Why the session cannot make calls, or ``None`` while it can."""
        return self._credential_refusal()

    def bearer(self) -> str:
        """The ``Bearer <token>`` credential, for callers building their own metadata."""
        with self._state_lock:
            return self._bearer

    def principal(self) -> Any:
        """A copy of the verified login principal."""
        return _copy_principal(self._principal)

    def bearer_expires_at(self) -> datetime | None:
        """When the current bearer expires, ``None`` when the token carries no expiry."""
        with self._state_lock:
            expires_at = self._token.expires_at
        return None if expires_at is None else datetime.fromtimestamp(expires_at, timezone.utc)

    def validate_tenant(self, record_tenant_id: str) -> None:
        """Raise :class:`UdbTenantError` when the value differs from the verified tenant.

        Call it before a tenant-scoped write.
        """
        self.tenant_state.validate_tenant_id(record_tenant_id)

    @property
    def native_channel(self) -> grpc.Channel:
        """The session-owned control-plane channel, for a generated stub the facades
        do not cover. Do not close it; :meth:`close` does. Pass
        ``metadata=session.native_metadata().to_grpc_metadata()`` on each call."""
        return self._ctl_channel

    def lock_service(self) -> "EnterpriseLockService":
        """A typed distributed-lock facade on the session-owned control-plane channel."""
        from udb.core.lock.services.v1 import lock_service_pb2_grpc

        return EnterpriseLockService(lock_service_pb2_grpc.LockServiceStub(self._ctl_channel), self)

    def __enter__(self) -> "EnterpriseSession":
        return self

    def close(self) -> None:
        """Stop the refresher and close the channels. Safe to call more than once."""
        stop = getattr(self, "_stop_refresh", None)
        if stop is not None:
            stop.set()
        super().close()
        thread = getattr(self, "_refresh_thread", None)
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)
        self._refresh_thread = None


class EnterpriseLockService:
    """Distributed locks on the session's channel, bound to its canonical tenant.

    A request's tenant defaults to the canonical UUID when empty and an explicit
    value is never overridden. ``raw`` is the generated stub.
    """

    _SERVICE_FULL = "udb.core.lock.services.v1.LockService"

    def __init__(self, raw: Any, session: EnterpriseSession):
        self.raw = raw
        self._session = session

    def _call(self, rpc: str, request: Any) -> Any:
        if not request.tenant_id:
            request.tenant_id = self._session.canonical_tenant_id
        return self._session._invoke(self._SERVICE_FULL, rpc, self.raw, request, None)

    def acquire(self, request: Any) -> Any:
        """Acquire a lock. The response carries the fencing token for renew and release."""
        return self._call("AcquireLock", request)

    def renew(self, request: Any) -> Any:
        """Extend a held lock's lease. The presented fencing token must not be stale."""
        return self._call("RenewLock", request)

    def release(self, request: Any) -> Any:
        """Release a held lock. The presented fencing token must not be stale."""
        return self._call("ReleaseLock", request)

    def get(self, request: Any) -> Any:
        """Fetch one lock by name (``found=False`` on a miss, not an error)."""
        return self._call("GetLock", request)

    def list(self, request: Any) -> Any:
        """List the tenant's locks, paginated."""
        return self._call("ListLocks", request)


def _copy_principal(principal: Any) -> Any:
    copied = type(principal)()
    copied.CopyFrom(principal)
    return copied


def connect_enterprise(config: EnterpriseConfig | None = None, /, **kwargs: Any) -> EnterpriseSession:
    """Log in and return a session with a verified tenant and a self-renewing bearer.

    Pass an :class:`EnterpriseConfig` or the keyword args for one. A refused login
    raises the broker's :class:`~udb_client.exceptions.UdbRpcError`.
    """
    if config is None:
        config = EnterpriseConfig(**kwargs)
    elif kwargs:
        raise UdbConfigurationError("pass either an EnterpriseConfig or keyword args, not both")
    return EnterpriseSession(config)


async def connect_enterprise_async(
    config: EnterpriseConfig | None = None, /, **kwargs: Any
) -> UdbAsyncProject:
    """:func:`connect_enterprise` for async code.

    The session keeps owning the bearer and its refresh; the returned project sends
    data and native calls over ``grpc.aio`` with it, and closes it. Its
    ``credential_owner`` is the :class:`EnterpriseSession`, for ``validate_tenant``
    and ``lock_service``. A call that finds the bearer stale waits for the renewal on
    the event loop, bounded by the deadline; the background refresh normally gets
    there first.
    """
    session = await asyncio.to_thread(connect_enterprise, config, **kwargs)
    return UdbAsyncProject.from_project(session)
