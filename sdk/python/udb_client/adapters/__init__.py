"""Web-framework adapters for the UDB Python SDK (Phase 7, M5.6).

These adapters extract the UDB request context (tenant / user / correlation /
request-id / scopes / purpose / api-key) from an inbound HTTP request, build a
per-request :class:`udb_client.metadata.Metadata`, and expose a request-scoped
auth/project context so handlers can call UDB with the caller's identity already
plumbed through.

``fastapi`` and ``starlette`` are **optional** — they are lazily imported inside
the adapter modules, so importing :mod:`udb_client.adapters` (or the whole
``udb_client`` package) never requires either framework to be installed.

The framework-agnostic pieces (header names, the
:class:`~udb_client.adapters._context.RequestContext` extractor, and the
canonical outbound-header builder) live in
:mod:`udb_client.adapters._context` and are re-exported here for convenience.
"""

from __future__ import annotations

from ._context import (
    CANONICAL_HEADERS,
    HEADER_API_KEY,
    HEADER_BEARER,
    HEADER_REQUEST_ID,
    RequestContext,
    RequestScopedUdb,
    canonical_outbound_headers,
    metadata_from_headers,
)

__all__ = [
    "CANONICAL_HEADERS",
    "HEADER_API_KEY",
    "HEADER_BEARER",
    "HEADER_REQUEST_ID",
    "RequestContext",
    "RequestScopedUdb",
    "canonical_outbound_headers",
    "metadata_from_headers",
]


def __getattr__(name: str):  # pragma: no cover - thin lazy re-export
    """Lazily surface the framework adapters without importing the frameworks.

    ``udb_client.adapters.fastapi`` / ``.starlette`` import their framework only
    when first accessed, keeping fastapi/starlette optional.
    """
    if name in ("fastapi", "starlette"):
        import importlib

        return importlib.import_module(f"{__name__}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
