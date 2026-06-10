"""Starlette adapter for the UDB Python SDK (M5.6).

Provides :class:`UdbContextMiddleware`, a Starlette/ASGI middleware that
extracts the UDB request context from inbound headers, stashes it (and a
per-request :class:`Metadata`, plus an optional request-scoped
:class:`RequestScopedUdb`) on the request scope/``request.state``, and echoes
the resolved ``x-request-id`` back on the response.

``starlette`` is imported lazily inside the constructor so importing this module
(or ``udb_client.adapters``) never hard-requires Starlette to be installed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Optional

from ._context import HEADER_REQUEST_ID, RequestContext, RequestScopedUdb

if TYPE_CHECKING:  # pragma: no cover - typing only
    from starlette.requests import Request

    from ..project import UdbProject

# Key under which the per-request context object is stored on the ASGI scope and
# mirrored on ``request.state``.
SCOPE_KEY = "udb"


def _require_starlette() -> Any:
    try:
        import starlette  # noqa: F401
        from starlette.datastructures import Headers, MutableHeaders  # noqa: F401
    except ImportError as exc:  # pragma: no cover - exercised only without dep
        raise ImportError(
            "udb_client.adapters.starlette requires 'starlette'. "
            "Install it with: pip install starlette"
        ) from exc
    return starlette


class UdbContextMiddleware:
    """Pure-ASGI middleware that attaches a UDB request context per request.

    On each HTTP request it builds a :class:`RequestContext` from the inbound
    headers and stores a small holder object at ``scope['state'][SCOPE_KEY]``
    (and therefore ``request.state.udb``) exposing ``.context`` / ``.metadata``
    and, when a ``project`` (or ``project_factory``) was supplied, ``.udb`` (a
    :class:`RequestScopedUdb`). The resolved ``x-request-id`` is echoed on the
    response headers so clients can correlate.

    Pass either ``project`` (a shared :class:`UdbProject` whose channels are
    reused across requests) or ``project_factory`` (called per request).
    """

    def __init__(
        self,
        app: Any,
        *,
        project: "Optional[UdbProject]" = None,
        project_factory: "Optional[Callable[[RequestContext], UdbProject]]" = None,
        default_purpose: str = "python.request",
        default_service_identity: str = "python.app",
        default_project_id: str = "default",
    ) -> None:
        _require_starlette()
        self.app = app
        self._project = project
        self._project_factory = project_factory
        self._defaults = dict(
            default_purpose=default_purpose,
            default_service_identity=default_service_identity,
            default_project_id=default_project_id,
        )

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        from starlette.datastructures import Headers, MutableHeaders

        headers = Headers(scope=scope)
        context = RequestContext.from_headers(headers, **self._defaults)
        holder = _UdbHolder(context, self._resolve_project(context))

        # Stash on the ASGI scope state (mirrors request.state.udb).
        state = scope.setdefault("state", {})
        state[SCOPE_KEY] = holder

        async def send_with_request_id(message: Any) -> None:
            if message["type"] == "http.response.start":
                response_headers = MutableHeaders(scope=message)
                response_headers.setdefault(HEADER_REQUEST_ID, context.request_id)
            await send(message)

        await self.app(scope, receive, send_with_request_id)

    def _resolve_project(self, context: RequestContext) -> "Optional[UdbProject]":
        if self._project_factory is not None:
            return self._project_factory(context)
        return self._project


class _UdbHolder:
    """Per-request holder exposed at ``request.state.udb``."""

    def __init__(self, context: RequestContext, project: "Optional[UdbProject]"):
        self.context = context
        self.metadata = context.to_metadata()
        self._project = project

    @property
    def udb(self) -> RequestScopedUdb:
        """The request-scoped UDB facade (requires a project be configured)."""
        if self._project is None:
            raise RuntimeError(
                "UdbContextMiddleware has no project configured; pass "
                "project=... or project_factory=... to use request.state.udb.udb"
            )
        return RequestScopedUdb(self._project, self.context)


def udb_context(request: "Request") -> RequestContext:
    """Return the :class:`RequestContext` the middleware attached to ``request``.

    Falls back to extracting from the request headers when the middleware is not
    installed, so handlers can rely on it either way.
    """
    holder = getattr(request.state, SCOPE_KEY, None)
    if holder is not None:
        return holder.context
    return RequestContext.from_headers(request.headers)


def udb_request(request: "Request") -> RequestScopedUdb:
    """Return the request-scoped :class:`RequestScopedUdb` for ``request``.

    Requires :class:`UdbContextMiddleware` be installed with a ``project`` /
    ``project_factory``.
    """
    holder = getattr(request.state, SCOPE_KEY, None)
    if holder is None:
        raise RuntimeError(
            "UdbContextMiddleware is not installed on this app; add it to use "
            "udb_request(request)."
        )
    return holder.udb
