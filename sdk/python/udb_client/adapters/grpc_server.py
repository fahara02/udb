"""gRPC server interceptors that lift the UDB request context off inbound metadata.

The context lands in a :class:`contextvars.ContextVar` for the handler's duration,
so handlers and any outbound UDB call they make read the same identity without
re-parsing headers::

    server = grpc.server(pool, interceptors=[UdbServerInterceptor()])
    server = grpc.aio.server(interceptors=[UdbAsyncServerInterceptor()])

    def Handle(self, request, context):
        meta = metadata_from_context()

Credentials are not lifted: a handler authenticates its own outbound calls.
"""

from __future__ import annotations

import contextvars
from typing import Any, Callable, Iterable, Iterator

import grpc
import grpc.aio

from ..metadata import Metadata
from ._context import (
    HEADER_CORRELATION_ID,
    HEADER_PROJECT_ID,
    HEADER_PURPOSE,
    HEADER_REQUEST_ID,
    HEADER_SCOPES,
    HEADER_SERVICE_IDENTITY,
    HEADER_TENANT_ID,
    HEADER_USER_ID,
)

HEADER_CATALOG_VERSION = "x-udb-client-catalog-version"

_CURRENT: contextvars.ContextVar[Metadata | None] = contextvars.ContextVar(
    "udb_client_request_metadata", default=None
)


def with_metadata(metadata: Metadata) -> contextvars.Token:
    """Make ``metadata`` the current request's; returns the token to reset it with."""
    return _CURRENT.set(metadata)


def from_context() -> Metadata | None:
    """The request's metadata, ``None`` outside an intercepted handler."""
    return _CURRENT.get()


def metadata_from_context() -> Metadata:
    """The request's metadata, or empty metadata when none has been set."""
    return _CURRENT.get() or _metadata_from_pairs(())


def correlation_id() -> str:
    """The request's correlation id, ``""`` when unset."""
    return metadata_from_context().correlation_id


def tenant_id() -> str:
    """The request's tenant id, ``""`` when unset."""
    return metadata_from_context().tenant_id


def user_id() -> str:
    """The request's user id, ``""`` when unset."""
    return metadata_from_context().user_id


def _metadata_from_pairs(pairs: Iterable[Any]) -> Metadata:
    first: dict[str, str] = {}
    for key, value in pairs or ():
        if isinstance(value, str):
            first.setdefault(key, value)
    return Metadata(
        tenant_id=first.get(HEADER_TENANT_ID, ""),
        user_id=first.get(HEADER_USER_ID, ""),
        purpose=first.get(HEADER_PURPOSE, ""),
        correlation_id=first.get(HEADER_CORRELATION_ID, "") or first.get(HEADER_REQUEST_ID, ""),
        scopes=tuple(scope for scope in first.get(HEADER_SCOPES, "").split(",") if scope),
        service_identity=first.get(HEADER_SERVICE_IDENTITY, ""),
        project_id=first.get(HEADER_PROJECT_ID, ""),
        client_catalog_version=first.get(HEADER_CATALOG_VERSION, ""),
    )


def _context_for(pairs: Iterable[Any]) -> contextvars.Context:
    context = contextvars.copy_context()
    context.run(_CURRENT.set, _metadata_from_pairs(pairs))
    return context


def _wrap_unary_result(behavior: Callable[..., Any]) -> Callable[..., Any]:
    def wrapped(request: Any, servicer_context: Any) -> Any:
        context = _context_for(servicer_context.invocation_metadata())
        return context.run(behavior, request, servicer_context)

    return wrapped


def _wrap_stream_result(behavior: Callable[..., Iterator[Any]]) -> Callable[..., Iterator[Any]]:
    def wrapped(request: Any, servicer_context: Any) -> Iterator[Any]:
        context = _context_for(servicer_context.invocation_metadata())
        responses = iter(context.run(behavior, request, servicer_context))
        # A generator handler's body runs on each next(), so each step needs the context.
        while True:
            try:
                item = context.run(next, responses)
            except StopIteration:
                return
            yield item

    return wrapped


class UdbServerInterceptor(grpc.ServerInterceptor):
    """Exposes the UDB request context to unary and streaming handlers."""

    def intercept_service(self, continuation: Callable[..., Any], handler_call_details: Any) -> Any:
        handler = continuation(handler_call_details)
        if handler is None:
            return None
        serializers = {
            "request_deserializer": handler.request_deserializer,
            "response_serializer": handler.response_serializer,
        }
        if handler.unary_unary is not None:
            return grpc.unary_unary_rpc_method_handler(_wrap_unary_result(handler.unary_unary), **serializers)
        if handler.unary_stream is not None:
            return grpc.unary_stream_rpc_method_handler(_wrap_stream_result(handler.unary_stream), **serializers)
        if handler.stream_unary is not None:
            return grpc.stream_unary_rpc_method_handler(_wrap_unary_result(handler.stream_unary), **serializers)
        return grpc.stream_stream_rpc_method_handler(_wrap_stream_result(handler.stream_stream), **serializers)


class UdbAsyncServerInterceptor(grpc.aio.ServerInterceptor):
    """The ``grpc.aio`` form: each RPC runs in its own task, so the context stays per-request."""

    async def intercept_service(self, continuation: Callable[..., Any], handler_call_details: Any) -> Any:
        _CURRENT.set(_metadata_from_pairs(handler_call_details.invocation_metadata))
        return await continuation(handler_call_details)
