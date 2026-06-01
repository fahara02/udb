"""Hand-written auth ergonomics over the generated AuthnService / AuthzService
stubs, mirroring :class:`udb_client.client.UdbClient`'s metadata convention.

The generated authn/authz stubs live in the buf ``gen`` tree
(``udb.core.authn.services.v1`` / ``udb.core.authz.services.v1``). They require
``buf generate --include-imports`` so the ``google/api`` annotation modules are
emitted alongside them.
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator, Sequence

import grpc

from udb.core.authn.services.v1 import authn_service_pb2_grpc
from udb.core.authn.services.v1 import core_pb2 as authn
from udb.core.authz.services.v1 import authz_service_pb2_grpc
from udb.core.authz.services.v1 import core_pb2 as authz

from .exceptions import UdbConfigurationError, UdbRpcError
from .metadata import Metadata


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
    ):
        if not target and channel is None:
            raise UdbConfigurationError("UDB target is required, e.g. '127.0.0.1:50051'")
        self._metadata = metadata
        self._timeout = timeout
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

    # ── Authorization ─────────────────────────────────────────────────────
    def authorize(self, request: authz.AuthzRequest, *, metadata: Metadata | None = None) -> authz.Decision:
        resp = self._call(self.authz.Authorize, "Authorize", request, metadata)
        return resp.decision

    def can(
        self,
        resource: authz.ResourceRef,
        action: str,
        *,
        purpose: str = "",
        metadata: Metadata | None = None,
    ) -> tuple[bool, authz.Decision]:
        meta = self._effective_metadata(metadata)
        decision = self.authorize(
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
                resource=resource,
                action=action,
                purpose=purpose or meta.purpose,
                requested_scopes=list(meta.scopes),
            ),
            metadata=metadata,
        )
        return decision.allowed, decision

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
    def get_policy_bundle(self, *, metadata: Metadata | None = None) -> authz.SignedPolicyBundle:
        meta = self._effective_metadata(metadata)
        resp = self._call(
            self.authz.GetPolicyBundle,
            "GetPolicyBundle",
            authz.PolicyBundleRequest(tenant_id=meta.tenant_id, project_id=meta.project_id),
            metadata,
        )
        return resp.bundle

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
        resource: authz.ResourceRef,
        action: str,
        *,
        purpose: str = "",
        metadata: Metadata | None = None,
    ) -> tuple[bool, authz.Decision]:
        meta = self._client._effective_metadata(metadata)
        purpose = purpose or meta.purpose
        key = self._key(meta, resource, action, purpose)
        now = self._clock()
        with self._lock:
            hit = self._cache.get(key)
            if hit is not None and now < hit[0]:
                return hit[1].allowed, hit[1]
        allowed, decision = self._client.can(
            resource, action, purpose=purpose, metadata=metadata
        )
        ttl = decision.cache_ttl_seconds
        if ttl > 0:
            with self._lock:
                self._cache[key] = (now + ttl, decision)
        return allowed, decision

    def invalidate(self) -> None:
        with self._lock:
            self._cache.clear()
