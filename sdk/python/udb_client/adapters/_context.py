"""Framework-agnostic request-context extraction for the web adapters.

This module owns the mapping between inbound HTTP headers and a UDB
:class:`~udb_client.metadata.Metadata`, independent of any web framework. The
FastAPI and Starlette adapters both build on it so the header contract stays in
one place.

Canonical header names mirror :meth:`Metadata.to_grpc_metadata` plus the
transport request id the generated client adds in ``_call_metadata``, so a
request flowing in through an adapter and back out to the broker carries a
consistent context end to end.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterable, Mapping

from ..metadata import Metadata

if TYPE_CHECKING:  # pragma: no cover - typing only
    from ..project import UdbProject

# Inbound/outbound header names. These are the canonical UDB headers; the first
# group matches ``Metadata.to_grpc_metadata`` and the request id matches the
# generated client's per-call transport header.
HEADER_TENANT_ID = "x-tenant-id"
HEADER_USER_ID = "x-user-id"
HEADER_PURPOSE = "x-purpose"
HEADER_CORRELATION_ID = "x-correlation-id"
HEADER_SCOPES = "x-scopes"
HEADER_SERVICE_IDENTITY = "x-service-identity"
HEADER_PROJECT_ID = "x-udb-project-id"
HEADER_API_KEY = "x-api-key"
HEADER_BEARER = "authorization"
HEADER_REQUEST_ID = "x-request-id"

# The canonical UDB request headers an adapter reads off an inbound request.
CANONICAL_HEADERS = (
    HEADER_TENANT_ID,
    HEADER_USER_ID,
    HEADER_PURPOSE,
    HEADER_CORRELATION_ID,
    HEADER_SCOPES,
    HEADER_SERVICE_IDENTITY,
    HEADER_PROJECT_ID,
    HEADER_BEARER,
    HEADER_API_KEY,
    HEADER_REQUEST_ID,
)


def _lookup(headers: Mapping[str, str], name: str) -> str:
    """Case-insensitive single-header lookup returning ``""`` when absent.

    Accepts any mapping; for plain ``dict``s the lookup is made case-insensitive
    by scanning lowercased keys. Framework header containers (Starlette
    ``Headers``, etc.) are already case-insensitive and hit the fast path.
    """
    value = headers.get(name)
    if value is None and not _is_case_insensitive(headers):
        lowered = name.lower()
        for key, val in headers.items():
            if key.lower() == lowered:
                value = val
                break
    return value or ""


def _is_case_insensitive(headers: Mapping[str, str]) -> bool:
    # Starlette ``Headers`` (and similar) are case-insensitive; a plain dict is
    # not. We only special-case the dict path to keep extraction allocation-free
    # for the framework containers.
    return not isinstance(headers, dict)


@dataclass(frozen=True)
class RequestContext:
    """The UDB request context extracted from inbound HTTP headers.

    ``request_id`` is the inbound ``x-request-id`` (or a freshly minted one when
    absent) so it can be echoed back on the response and reused as the outbound
    transport request id. ``bearer_token`` / ``api_key`` carry the caller's
    credential through to outbound UDB calls.
    """

    tenant_id: str = ""
    user_id: str = ""
    purpose: str = ""
    correlation_id: str = ""
    scopes: tuple[str, ...] = ()
    service_identity: str = ""
    project_id: str = ""
    request_id: str = ""
    api_key: str = ""
    bearer_token: str = ""

    @classmethod
    def from_headers(
        cls,
        headers: Mapping[str, str],
        *,
        default_purpose: str = "python.request",
        default_service_identity: str = "python.app",
        default_project_id: str = "default",
        generate_request_id: bool = True,
    ) -> "RequestContext":
        """Build a context from an inbound header mapping (case-insensitive)."""
        raw_scopes = _lookup(headers, HEADER_SCOPES)
        scopes = tuple(s.strip() for s in raw_scopes.split(",") if s.strip())
        request_id = _lookup(headers, HEADER_REQUEST_ID)
        if not request_id and generate_request_id:
            request_id = uuid.uuid4().hex
        bearer = _lookup(headers, HEADER_BEARER)
        if bearer.lower().startswith("bearer "):
            bearer = bearer[len("bearer "):].strip()
        return cls(
            tenant_id=_lookup(headers, HEADER_TENANT_ID),
            user_id=_lookup(headers, HEADER_USER_ID),
            purpose=_lookup(headers, HEADER_PURPOSE) or default_purpose,
            correlation_id=_lookup(headers, HEADER_CORRELATION_ID) or request_id,
            scopes=scopes,
            service_identity=(
                _lookup(headers, HEADER_SERVICE_IDENTITY) or default_service_identity
            ),
            project_id=_lookup(headers, HEADER_PROJECT_ID) or default_project_id,
            request_id=request_id,
            api_key=_lookup(headers, HEADER_API_KEY),
            bearer_token=bearer,
        )

    def to_metadata(self) -> Metadata:
        """The per-request :class:`Metadata` for downstream UDB calls."""
        return Metadata(
            tenant_id=self.tenant_id,
            purpose=self.purpose,
            correlation_id=self.correlation_id,
            scopes=self.scopes,
            service_identity=self.service_identity,
            user_id=self.user_id,
            project_id=self.project_id or "default",
            bearer_token=self.bearer_token,
            api_key=self.api_key,
        )


def metadata_from_headers(
    headers: Mapping[str, str], **kwargs: object
) -> Metadata:
    """Shortcut: extract a :class:`RequestContext` and return its Metadata."""
    return RequestContext.from_headers(headers, **kwargs).to_metadata()  # type: ignore[arg-type]


def canonical_outbound_headers(
    context: RequestContext,
) -> tuple[tuple[str, str], ...]:
    """Canonical outbound gRPC headers for a request-scoped UDB call.

    Adds the transport ``x-request-id`` on top of
    ``Metadata.to_grpc_metadata()``. Credentials already live on the metadata,
    matching the header set the generated client emits in ``_call_metadata`` so
    an adapter can forward the caller's identity verbatim.
    """
    headers: list[tuple[str, str]] = list(context.to_metadata().to_grpc_metadata())
    headers.append((HEADER_REQUEST_ID, context.request_id or uuid.uuid4().hex))
    return tuple(headers)


def iter_canonical_header_names() -> Iterable[str]:
    """The canonical inbound header names this extractor reads."""
    return CANONICAL_HEADERS


class RequestScopedUdb:
    """A request-scoped view over a shared :class:`UdbProject`.

    The shared project's channels are reused across requests, but the per-request
    :class:`Metadata` (and therefore tenant / user / scope context) differs per
    request. Rebinding the shared project's metadata is not request-safe under
    concurrency, so instead this proxy forwards every call with the request's
    metadata passed explicitly (every SDK surface accepts ``metadata=...``).

    Attributes:

    * ``context``  — the extracted :class:`RequestContext`.
    * ``metadata`` — the per-request :class:`Metadata`.
    * ``project``  — the shared :class:`UdbProject` (escape hatch).
    * ``data`` / ``auth`` — the shared sub-clients (pass ``metadata=`` yourself).
    * ``authz``    — cached authz with this request's metadata bound in.
    """

    def __init__(self, project: "UdbProject", context: RequestContext):
        self.project = project
        self.context = context
        self.metadata = context.to_metadata()

    @property
    def data(self) -> Any:
        return self.project.data

    @property
    def auth(self) -> Any:
        return self.project.auth

    def can(self, resource: Any, action: str, **kwargs: Any) -> Any:
        kwargs.setdefault("metadata", self.metadata)
        return self.project.authz.can(resource, action, **kwargs)

    def require(self, resource: Any, action: str, **kwargs: Any) -> Any:
        kwargs.setdefault("metadata", self.metadata)
        return self.project.authz.require(resource, action, **kwargs)

    def explain(self, resource: Any, action: str, **kwargs: Any) -> Any:
        kwargs.setdefault("metadata", self.metadata)
        return self.project.authz.explain(resource, action, **kwargs)

    def native_access(self, resource: Any, action: str, **kwargs: Any) -> Any:
        kwargs.setdefault("metadata", self.metadata)
        return self.project.authz.native_access(resource, action, **kwargs)
