"""The parts of :class:`~udb_client.project.UdbProject` and its async form that are
built on the channels and metadata they already own: credential introspection,
readiness, per-request users, flags, migrations, live queries, and the graph and
vector builders.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Callable, Sequence

import grpc

from ._credentials import (
    DelegatedMetadata,
    OwnedMetadata,
    UdbCredentialBindingError,
    UdbCredentialError,
    metadata_values,
    normalized_bearer,
)
from .exceptions import UdbConfigurationError, UdbError, UdbRpcError
from .metadata import Metadata

_HEALTH_CHECK = "/grpc.health.v1.Health/Check"
_HEALTH_STATUS = {0: "UNKNOWN", 1: "SERVING", 2: "NOT_SERVING", 3: "SERVICE_UNKNOWN"}


def _health_status(data: bytes) -> str:
    # HealthCheckResponse is one enum field (number 1), so a set value is two bytes.
    # grpcio-health-checking is not a dependency of this package.
    return _HEALTH_STATUS.get(data[1] if data[:1] == b"\x08" and len(data) > 1 else 0, "UNKNOWN")


def _health_method(channel: Any) -> Any:
    return channel.unary_unary(
        _HEALTH_CHECK,
        request_serializer=lambda _request: b"",
        response_deserializer=_health_status,
    )


def _require_serving(status: str) -> None:
    if status != "SERVING":
        raise UdbError(f"udb: broker is {status}")


def _delegated_metadata(
    bearer: str, base: Metadata, connected: Metadata, owner: Any
) -> DelegatedMetadata:
    token = normalized_bearer(bearer)
    if not token:
        raise UdbCredentialError("udb: delegated bearer is required")
    values = metadata_values(base)
    values.update(
        # The connected tenant and project stay; the broker verifies the bearer.
        tenant_id=connected.tenant_id,
        project_id=connected.project_id,
        # The service's own principal hints must not ride along with the user's bearer.
        user_id="",
        service_identity="",
        scopes=(),
        purpose=base.purpose or connected.purpose,
        correlation_id=base.correlation_id or connected.correlation_id,
        client_catalog_version=base.client_catalog_version or connected.client_catalog_version,
        bearer_token=token,
        # A call carries exactly one credential.
        api_key="",
    )
    return DelegatedMetadata(**values, owner=owner)


class _ProjectSurface:
    """Mixed into :class:`~udb_client.project.UdbProject`, which sets these."""

    config: Any
    data: Any
    _metadata: Metadata
    _data_channel: Any
    _ctl_channel: Any
    _webrtc_channel: Any
    _api_key_principal: Any
    _api_key_secret: str
    _api_key_expires_at: float
    _api_key_last_error: BaseException | None
    _api_key_stop: Any

    def _ctl_metadata(self, metadata: Metadata | None) -> tuple[tuple[str, str], ...]:
        raise NotImplementedError

    # ── credential ownership ──────────────────────────────────────────────
    def _owns_credential(self) -> bool:
        """Whether calls resolve their bearer here rather than carry a fixed one."""
        return self._exchanges_bearer()

    def _exchanges_bearer(self) -> bool:
        """Whether this project renews its bearer by exchanging again: an API key,
        or (``for_tenant``) the home bearer for a member-tenant token."""
        return bool(self._api_key_secret or getattr(self, "_bearer_exchange", None))

    def _own_metadata(self, metadata: Metadata) -> Metadata:
        """``metadata`` in the form this project binds: live while it owns the credential."""
        if isinstance(metadata, DelegatedMetadata):
            return metadata
        if self._owns_credential():
            return OwnedMetadata(**metadata_values(metadata), owner=self)
        if isinstance(metadata, OwnedMetadata):
            return Metadata(**metadata_values(metadata))
        return metadata

    # ── credential introspection ──────────────────────────────────────────
    def principal(self) -> Any:
        """A copy of the verified principal the broker returned for the exchanged
        API key; ``None`` when the connection does not exchange one."""
        principal = self._api_key_principal
        if principal is None or not self._exchanges_bearer():
            return None  # set_credentials or a password login superseded the key.
        copied = type(principal)()
        copied.CopyFrom(principal)
        return copied

    def bearer_expires_at(self) -> datetime | None:
        """When the current exchanged bearer expires; ``None`` when the connection
        does not exchange an API key or the bearer carries no expiry."""
        expires_at = self._api_key_expires_at
        if expires_at <= 0 or not self._exchanges_bearer():
            return None
        return datetime.fromtimestamp(expires_at, timezone.utc)

    def _credential_refusal(self) -> BaseException | None:
        """The error a delegated call must refuse with, ``None`` while calls may proceed."""
        if self._api_key_stop.is_set():
            return UdbCredentialError(
                "udb: credential owner is closed", code=grpc.StatusCode.CANCELLED
            )
        error = self._api_key_last_error
        if error is None or not self._exchanges_bearer():
            return None
        # A blip while the bearer is still valid leaves the next call free to renew;
        # a bearer the broker refused is not valid whatever its expiry.
        if (
            isinstance(error, UdbCredentialBindingError)
            or getattr(self, "_api_key_rejected", False)
            or self._api_key_expires_at <= time.time()
        ):
            return UdbCredentialError(f"udb: credential renewal failed: {error}")
        return None

    def credential_error(self) -> BaseException | None:
        """Why the background credential refresh is failing, ``None`` while it is current."""
        if self._api_key_stop.is_set():
            return UdbCredentialError(
                "udb: credential owner is closed", code=grpc.StatusCode.CANCELLED
            )
        return self._api_key_last_error if self._exchanges_bearer() else None

    def verify(self, expect: Any = None) -> None:
        """Check the connected principal against an :class:`~udb_client.connect_env.Expect`.

        Every mismatch is reported at once in a :class:`UdbConfigurationError`. A
        connection with no verified principal (one made with a raw bearer) fails
        here if anything is expected.
        """
        if expect is None or not (
            expect.tenant_id or expect.project_id or expect.service_identity or expect.required_scopes
        ):
            return
        principal = self.principal()
        if principal is None:
            raise UdbConfigurationError(
                "udb: cannot verify the connection: it has no verified principal "
                "(connect with an API key, or use connect_enterprise)"
            )
        problems: list[str] = []
        for label, actual, wanted in (
            ("tenant", principal.tenant_id, expect.tenant_id),
            ("project", principal.project_id, expect.project_id),
            ("service identity", principal.service_identity, expect.service_identity),
        ):
            if wanted and actual != wanted:
                problems.append(f"{label} is {actual!r}, expected {wanted!r}")
        granted = {scope.lower() for scope in principal.scopes}
        missing = [scope for scope in expect.required_scopes if scope.lower() not in granted]
        if missing:
            problems.append(
                f"the grant lacks scopes {', '.join(missing)} (add them to the service account's grant)"
            )
        if problems:
            raise UdbConfigurationError(f"udb: connected principal does not match: {'; '.join(problems)}")

    def ready(self, *, timeout: float | None = None) -> None:
        """Raise unless the broker is serving and this connection's credential is current.

        The standard gRPC health check on the broker connection plus
        :meth:`credential_error`. Use it as the readiness probe; it needs no admin scope.
        """
        error = self.credential_error()
        if error is not None:
            if isinstance(error, UdbCredentialError):
                raise error
            raise UdbCredentialError(f"udb: credential refresh is failing: {error}") from error
        try:
            status = _health_method(self._data_channel)(
                b"", timeout=self.config.deadline if timeout is None else timeout
            )
        except grpc.RpcError as rpc_error:
            raise UdbRpcError("Health/Check", rpc_error) from rpc_error
        _require_serving(status)

    # ── per-request user ──────────────────────────────────────────────────
    def as_user(self, bearer: str, *, metadata: Metadata | None = None) -> Metadata:
        """Metadata whose calls carry the end user's bearer instead of the service's.

        For operations the broker must authorize as that user (their files, their
        sessions); pass it as ``metadata=`` on any call. The token may carry the
        ``Bearer `` prefix. The broker verifies it: claims are not adopted and the
        connected tenant and project do not change, so the user must belong to that
        same scope. ``metadata`` supplies request audit values (purpose, correlation
        id) and other request options to keep. An empty bearer raises rather than
        fall back to the service's own credential.
        """
        return _delegated_metadata(bearer, metadata or self._metadata, self._metadata, self)

    # ── facades ───────────────────────────────────────────────────────────
    @property
    def flags(self) -> Any:
        """Feature flags (ConfigService), served on the control-plane target."""
        from udb.core.config.services.v1 import config_service_pb2_grpc

        from .facades import FlagsFacade

        return FlagsFacade(config_service_pb2_grpc.ConfigServiceStub(self._ctl_channel), self)

    @property
    def admin(self) -> Any:
        """The migration lifecycle helper over the data-plane client."""
        from .facades import AdminFacade

        return AdminFacade(self.data)

    @property
    def generated(self) -> Any:
        """The generated client for every RPC the facades do not cover.

        Shares this project's channels and metadata, so ``bind_metadata`` and credential
        renewal reach it. ``project.close()`` closes the channels; do not close it yourself.
        """
        generated = self.__dict__.get("_generated")
        if generated is None:
            generated = self._build_generated()
            self.__dict__["_generated"] = generated
        return generated

    def _build_generated(self) -> Any:
        from .generated_client import (
            DataBrokerClient,
            GeneratedClient,
            PeerServiceClient,
            RetryPolicy,
            RoomServiceClient,
            SignalingServiceClient,
            TrackServiceClient,
            TurnServiceClient,
        )

        config = self.config
        common: dict[str, Any] = {"timeout": config.deadline}
        if isinstance(config.retry, RetryPolicy):
            common["retry"] = config.retry
        generated = GeneratedClient(
            config.target, self._metadata, channel=self._ctl_channel, **common
        )
        # The data plane and WebRTC can live on other listeners than the native services.
        generated.DataBroker = DataBrokerClient(
            config.target, self._metadata, channel=self._data_channel, **common
        )
        for name, client in (
            ("PeerService", PeerServiceClient),
            ("RoomService", RoomServiceClient),
            ("SignalingService", SignalingServiceClient),
            ("TrackService", TrackServiceClient),
            ("TurnService", TurnServiceClient),
        ):
            setattr(
                generated,
                name,
                client(config.target, self._metadata, channel=self._webrtc_channel, **common),
            )
        return generated

    def _rebind_generated(self, metadata: Metadata) -> None:
        generated = self.__dict__.get("_generated")
        if generated is not None:
            generated.bind_metadata(metadata)

    def live_query(
        self,
        message: Any,
        *,
        where: Sequence[Any] = (),
        any_of: Sequence[Sequence[Any]] = (),
        snapshot_limit: int = 0,
        since_event_id: str = "",
        backoff: float | None = None,
        on_error: Callable[[Exception], None] | None = None,
        metadata: Metadata | None = None,
    ) -> Any:
        """A :class:`~udb_client.livequery.LiveQuery` over the rows of ``message`` in
        the caller's tenant and project.

        ``message`` is an entity message class, whose rows arrive decoded, or a full
        message type name, whose rows arrive as dicts. ``where`` predicates (see
        ``lq_eq`` and friends) are AND-ed; each ``any_of`` group is an OR, AND-ed with
        the rest. ``backoff`` is the first reconnect delay in seconds (default 0.5),
        doubled up to 30. ``on_error`` observes reconnects.
        """
        from udb.core.livequery.services.v1 import livequery_service_pb2_grpc

        from .livequery import LiveQuery, _subscribe_request

        meta = metadata or self._metadata
        return LiveQuery(
            livequery_service_pb2_grpc.LiveQueryServiceStub(self._ctl_channel),
            _subscribe_request(message, meta.tenant_id, meta.project_id, where, any_of, snapshot_limit),
            message,
            lambda: self._ctl_metadata(metadata),
            since_event_id=since_event_id,
            backoff=backoff,
            on_error=on_error,
        )

    def graph_from(self, message: Any, start_id: str) -> Any:
        """Start a traversal at the node of entity ``message`` (a message class) with ``start_id``."""
        from .graph_vector import GraphTraversalBuilder

        return GraphTraversalBuilder(self.data, message, start_id)

    def vectors_hybrid(self, collection: str, message: Any = None) -> Any:
        """Start a hybrid search over ``collection`` (the vector projection's resource
        name). With a message class, each hit's ``row`` is the payload decoded as it."""
        from .graph_vector import VectorSearchBuilder

        return VectorSearchBuilder(self.data, collection, message)


class _AsyncProjectSurface:
    """Mixed into :class:`~udb_client.project.UdbAsyncProject`.

    Native-service calls here use a ``grpc.aio`` channel opened on first use against
    the auth target (the data channel when the targets coincide).
    """

    config: Any
    data: Any
    _metadata: Metadata
    #: The synchronous project that owns the credential, set by :meth:`from_project`.
    credential_owner: Any = None
    _owns_credential_owner: bool = False

    @classmethod
    def from_project(cls, project: Any, *, owns: bool = True) -> Any:
        """An async project sending with ``project``'s credential and identity.

        ``project`` keeps renewing the credential and is reachable as
        ``credential_owner``. With ``owns`` (the default) closing this closes it; pass
        ``owns=False`` for a project a tenant pool holds. Each async project's
        channels belong to the event loop that uses them.
        """
        factory: Any = cls
        async_project = factory(project.config)
        async_project.credential_owner = project
        async_project._owns_credential_owner = owns
        async_project.bind_metadata(project.metadata)
        return async_project

    def principal(self) -> Any:
        """See :meth:`_ProjectSurface.principal`; ``None`` without a ``credential_owner``."""
        return None if self.credential_owner is None else self.credential_owner.principal()

    def bearer_expires_at(self) -> datetime | None:
        """See :meth:`_ProjectSurface.bearer_expires_at`."""
        return None if self.credential_owner is None else self.credential_owner.bearer_expires_at()

    def credential_error(self) -> BaseException | None:
        """See :meth:`_ProjectSurface.credential_error`."""
        return None if self.credential_owner is None else self.credential_owner.credential_error()

    def verify(self, expect: Any = None) -> None:
        """See :meth:`_ProjectSurface.verify`. Needs a ``credential_owner`` when anything is expected."""
        if self.credential_owner is not None:
            self.credential_owner.verify(expect)
        elif expect is not None and (
            expect.tenant_id or expect.project_id or expect.service_identity or expect.required_scopes
        ):
            raise UdbConfigurationError(
                "udb: cannot verify the connection: it has no verified principal "
                "(use connect_from_env_async or connect_enterprise_async)"
            )

    def _async_control_channel(self) -> Any:
        channel = self.__dict__.get("_ctl_channel_async")
        if channel is not None:
            return channel
        auth_target = self.config.effective_auth_target()
        if auth_target == self.config.target:
            channel = self.data._channel
        else:
            from ._channel import merge_channel_options, ssl_credentials

            options = merge_channel_options(self.config.channel_options)
            if self.config.tls:
                channel = grpc.aio.secure_channel(
                    auth_target,
                    ssl_credentials(
                        self.config.root_certificates,
                        self.config.client_private_key,
                        self.config.client_certificate_chain,
                    ),
                    options=options,
                )
            else:
                channel = grpc.aio.insecure_channel(auth_target, options=options)
            self.__dict__["_owned_ctl_channel_async"] = channel
        self.__dict__["_ctl_channel_async"] = channel
        return channel

    async def _close_surface(self) -> None:
        channel = self.__dict__.pop("_owned_ctl_channel_async", None)
        self.__dict__.pop("_ctl_channel_async", None)
        if channel is not None:
            await channel.close()
        if self.credential_owner is not None and self._owns_credential_owner:
            await asyncio.to_thread(self.credential_owner.close)

    async def ready(self, *, timeout: float | None = None) -> None:
        """Raise unless the broker is serving. See :meth:`_ProjectSurface.ready`."""
        error = self.credential_error()
        if error is not None:
            if isinstance(error, UdbCredentialError):
                raise error
            raise UdbCredentialError(f"udb: credential refresh is failing: {error}") from error
        try:
            status = await _health_method(self.data._channel)(
                b"", timeout=self.config.deadline if timeout is None else timeout
            )
        except grpc.RpcError as rpc_error:
            raise UdbRpcError("Health/Check", rpc_error) from rpc_error
        _require_serving(status)

    def as_user(self, bearer: str, *, metadata: Metadata | None = None) -> Metadata:
        """See :meth:`_ProjectSurface.as_user`."""
        return _delegated_metadata(bearer, metadata or self._metadata, self._metadata, self.credential_owner)

    @property
    def flags(self) -> Any:
        """Awaitable feature flags (ConfigService)."""
        from .facades import AsyncFlagsFacade

        return AsyncFlagsFacade(self)

    @property
    def admin(self) -> Any:
        """Awaitable migration lifecycle helper."""
        from .facades import AsyncAdminFacade

        return AsyncAdminFacade(self.data)

    def live_query(
        self,
        message: Any,
        *,
        where: Sequence[Any] = (),
        any_of: Sequence[Sequence[Any]] = (),
        snapshot_limit: int = 0,
        since_event_id: str = "",
        backoff: float | None = None,
        on_error: Callable[[Exception], None] | None = None,
        metadata: Metadata | None = None,
    ) -> Any:
        """An :class:`~udb_client.livequery.AsyncLiveQuery`. See :meth:`_ProjectSurface.live_query`."""
        from udb.core.livequery.services.v1 import livequery_service_pb2_grpc

        from .livequery import AsyncLiveQuery, _subscribe_request

        meta = metadata or self._metadata
        return AsyncLiveQuery(
            lambda: livequery_service_pb2_grpc.LiveQueryServiceStub(self._async_control_channel()),
            _subscribe_request(message, meta.tenant_id, meta.project_id, where, any_of, snapshot_limit),
            message,
            lambda: (metadata or self._metadata).to_grpc_metadata(),
            since_event_id=since_event_id,
            backoff=backoff,
            on_error=on_error,
        )

    def graph_from(self, message: Any, start_id: str) -> Any:
        """See :meth:`_ProjectSurface.graph_from`; ``run`` is awaited."""
        from .graph_vector import AsyncGraphTraversalBuilder

        return AsyncGraphTraversalBuilder(self.data, message, start_id)

    def vectors_hybrid(self, collection: str, message: Any = None) -> Any:
        """See :meth:`_ProjectSurface.vectors_hybrid`; ``run`` is awaited."""
        from .graph_vector import AsyncVectorSearchBuilder

        return AsyncVectorSearchBuilder(self.data, collection, message)
