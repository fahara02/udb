"""FastAPI adapter for the UDB Python SDK (M5.6).

Provides:

* :class:`UdbFastAPI` — a small integration bound to a shared
  :class:`UdbProject`, exposing FastAPI dependencies that yield the per-request
  :class:`RequestContext`, :class:`Metadata`, or a request-scoped
  :class:`RequestScopedUdb`.
* :func:`udb_context_dependency` — a dependency callable usable without a
  project (yields just the extracted :class:`RequestContext` / metadata).
* :class:`UdbContextMiddleware` — re-exported from the Starlette adapter
  (FastAPI is built on Starlette), which also echoes ``x-request-id`` on
  responses.

``fastapi`` / ``starlette`` are imported lazily so importing this module (or
``udb_client.adapters``) never hard-requires either framework.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Optional

from ..metadata import Metadata
from ._context import RequestContext, RequestScopedUdb
from .starlette import UdbContextMiddleware, udb_context

if TYPE_CHECKING:  # pragma: no cover - typing only
    from fastapi import Request

    from ..project import UdbProject

__all__ = [
    "UdbContextMiddleware",
    "UdbFastAPI",
    "udb_context_dependency",
]


def _require_fastapi() -> Any:
    try:
        import fastapi  # noqa: F401
    except ImportError as exc:  # pragma: no cover - exercised only without dep
        raise ImportError(
            "udb_client.adapters.fastapi requires 'fastapi'. "
            "Install it with: pip install fastapi"
        ) from exc
    return fastapi


def udb_context_dependency(
    *,
    default_purpose: str = "python.request",
    default_service_identity: str = "python.app",
    default_project_id: str = "default",
) -> "Callable[[Request], RequestContext]":
    """Build a FastAPI dependency that yields the per-request context.

    Use it directly in a path operation::

        from fastapi import Depends
        from udb_client.adapters.fastapi import udb_context_dependency

        ctx_dep = udb_context_dependency()

        @app.get("/items")
        def list_items(ctx = Depends(ctx_dep)):
            ...  # ctx.metadata / ctx.tenant_id / ctx.scopes

    Prefers the context already attached by :class:`UdbContextMiddleware`; when
    the middleware is absent it extracts straight from the request headers.
    """
    _require_fastapi()

    def dependency(request: "Request") -> RequestContext:
        holder = getattr(request.state, "udb", None)
        if holder is not None:
            return holder.context
        return RequestContext.from_headers(
            request.headers,
            default_purpose=default_purpose,
            default_service_identity=default_service_identity,
            default_project_id=default_project_id,
        )

    return dependency


class UdbFastAPI:
    """FastAPI integration bound to a shared :class:`UdbProject`.

    The shared project's gRPC channels are reused across requests; only the
    per-request :class:`Metadata` differs, so the request-scoped facade
    (:class:`RequestScopedUdb`) forwards the request metadata on every call.

    Wire it up once::

        udb = create_udb(target="127.0.0.1:50051", tenant_id="acme")
        integration = UdbFastAPI(udb)
        app.add_middleware(*integration.middleware())

        @app.get("/me")
        def me(scoped = Depends(integration.request)):
            scoped.require("acme.v1.User", "read")
            return {"tenant": scoped.context.tenant_id}
    """

    def __init__(
        self,
        project: "Optional[UdbProject]" = None,
        *,
        project_factory: "Optional[Callable[[RequestContext], UdbProject]]" = None,
        default_purpose: str = "python.request",
        default_service_identity: str = "python.app",
        default_project_id: str = "default",
    ) -> None:
        _require_fastapi()
        self._project = project
        self._project_factory = project_factory
        self._defaults = dict(
            default_purpose=default_purpose,
            default_service_identity=default_service_identity,
            default_project_id=default_project_id,
        )

    # ── middleware wiring ─────────────────────────────────────────────────────
    def middleware(self) -> tuple[Any, dict[str, Any]]:
        """``(middleware_class, kwargs)`` for ``app.add_middleware(*...)``."""
        return (
            UdbContextMiddleware,
            dict(
                project=self._project,
                project_factory=self._project_factory,
                **self._defaults,
            ),
        )

    def install(self, app: Any) -> None:
        """Install the context middleware on ``app``."""
        cls, kwargs = self.middleware()
        app.add_middleware(cls, **kwargs)

    # ── dependencies ──────────────────────────────────────────────────────────
    def context(self, request: "Request") -> RequestContext:
        """Dependency: the per-request :class:`RequestContext`."""
        return udb_context(request)

    def metadata(self, request: "Request") -> Metadata:
        """Dependency: the per-request :class:`Metadata`."""
        return udb_context(request).to_metadata()

    def request(self, request: "Request") -> RequestScopedUdb:
        """Dependency: a request-scoped :class:`RequestScopedUdb`.

        Resolves the project from ``project_factory`` (per request) or the shared
        ``project``; raises if neither was configured.
        """
        context = udb_context(request)
        project = self._resolve_project(context)
        if project is None:
            raise RuntimeError(
                "UdbFastAPI has no project configured; pass project=... or "
                "project_factory=... to use the request-scoped dependency."
            )
        return RequestScopedUdb(project, context)

    def _resolve_project(self, context: RequestContext) -> "Optional[UdbProject]":
        if self._project_factory is not None:
            return self._project_factory(context)
        return self._project
