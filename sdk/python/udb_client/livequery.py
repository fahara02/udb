"""Live queries: a view of the rows matching a filter.

A :class:`LiveQuery` delivers the current rows, then every insert, update and delete
as it happens, and reconnects on its own, resuming after the last change it
delivered. Keepalive frames are absorbed::

    query = project.live_query(Note, where=[lq_eq("owner", me)])
    for event in query:
        if event.is_snapshot:
            view = {row.note_id: row for row in event.snapshot}
        ...

Iteration ends after :meth:`LiveQuery.cancel`. A refusal reconnecting cannot fix
is raised, and a broker without a change feed raises :class:`UdbNoDeltaFeed` after
delivering the snapshot.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import threading
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Generator, Sequence

import grpc

from udb.core.livequery.services.v1 import livequery_service_pb2 as livequery_pb

from ._credentials import UdbCredentialError
from ._rows import decode_row, message_class, message_type_of
from .exceptions import UdbError, UdbRpcError
from .generated_client import _map_error

_RESUME_HEADER = "x-udb-livequery-resume"
_MAX_BACKOFF_S = 30.0
_DEFAULT_BACKOFF_S = 0.5
_FATAL_CODES = (
    grpc.StatusCode.INVALID_ARGUMENT,
    grpc.StatusCode.PERMISSION_DENIED,
    grpc.StatusCode.NOT_FOUND,
    grpc.StatusCode.UNIMPLEMENTED,
)


class UdbNoDeltaFeed(UdbError):
    """The broker has no CDC change feed: the snapshot arrived but no change ever will.

    It needs ``UDB_CDC_ENABLED`` (default on) and ``UDB_KAFKA_BROKERS`` on the broker;
    reconnecting cannot fix it.
    """


@dataclass(frozen=True)
class LiveQueryEvent:
    """One delivery: a snapshot of every matching row, or one change."""

    #: The rows matching now, on a snapshot delivery. ``None`` on a change.
    snapshot: list[Any] | None = None
    #: True for a snapshot following a reconnect. Changes missed while disconnected
    #: are replayed after it, so a view that applies changes by key converges.
    resumed: bool = False
    #: The change kind (``LIVE_QUERY_CHANGE_OP_*``); unspecified on a snapshot.
    op: int = livequery_pb.LIVE_QUERY_CHANGE_OP_UNSPECIFIED
    #: The changed row on a change delivery.
    row: Any = None
    #: Identifies the change; the query resumes after the last one delivered.
    event_id: str = ""

    @property
    def is_snapshot(self) -> bool:
        return not self.event_id and self.op == livequery_pb.LIVE_QUERY_CHANGE_OP_UNSPECIFIED


def _predicate(field: str, op: Any, value: str = "", values: Sequence[str] = ()) -> Any:
    return livequery_pb.LiveQueryPredicate(field=field, op=op, value=value, values=list(values))


def lq_eq(field: str, value: str) -> Any:
    """Rows whose field equals ``value``. Values compare numerically when both sides are numbers."""
    return _predicate(field, livequery_pb.LIVE_QUERY_COMPARISON_EQ, value)


def lq_ne(field: str, value: str) -> Any:
    """Rows whose field differs from ``value``."""
    return _predicate(field, livequery_pb.LIVE_QUERY_COMPARISON_NE, value)


def lq_lt(field: str, value: str) -> Any:
    """Rows whose field is below ``value``."""
    return _predicate(field, livequery_pb.LIVE_QUERY_COMPARISON_LT, value)


def lq_le(field: str, value: str) -> Any:
    """Rows whose field is at most ``value``."""
    return _predicate(field, livequery_pb.LIVE_QUERY_COMPARISON_LE, value)


def lq_gt(field: str, value: str) -> Any:
    """Rows whose field is above ``value``."""
    return _predicate(field, livequery_pb.LIVE_QUERY_COMPARISON_GT, value)


def lq_ge(field: str, value: str) -> Any:
    """Rows whose field is at least ``value``."""
    return _predicate(field, livequery_pb.LIVE_QUERY_COMPARISON_GE, value)


def lq_in(field: str, *values: str) -> Any:
    """Rows whose field equals one of ``values``."""
    return _predicate(field, livequery_pb.LIVE_QUERY_COMPARISON_IN, values=values)


def lq_not_in(field: str, *values: str) -> Any:
    """Rows whose field is set and equals none of ``values``."""
    return _predicate(field, livequery_pb.LIVE_QUERY_COMPARISON_NOT_IN, values=values)


def lq_is_null(field: str) -> Any:
    """Rows whose field is null or absent."""
    return _predicate(field, livequery_pb.LIVE_QUERY_COMPARISON_IS_NULL)


def lq_is_not_null(field: str) -> Any:
    """Rows whose field is set."""
    return _predicate(field, livequery_pb.LIVE_QUERY_COMPARISON_IS_NOT_NULL)


def _subscribe_request(
    message: Any,
    tenant_id: str,
    project_id: str,
    where: Sequence[Any],
    any_of: Sequence[Sequence[Any]],
    snapshot_limit: int,
) -> Any:
    return livequery_pb.SubscribeRequest(
        tenant_id=tenant_id,
        project_id=project_id,
        message_type=message_type_of(message),
        filters=list(where),
        snapshot_limit=snapshot_limit,
        any_of=[livequery_pb.LiveQueryAnyOf(predicates=list(group)) for group in any_of],
    )


def _decode_live_row(raw: str, message: Any) -> Any:
    try:
        record = json.loads(raw)
    except ValueError as error:
        raise UdbError(f"udb: live query row is not a JSON object: {error}") from error
    if record is None:
        record = {}
    if not isinstance(record, dict):
        raise UdbError("udb: live query row is not a JSON object")
    return decode_row(record, message)


def _frame_event(frame: Any, message: Any, resumed: bool) -> LiveQueryEvent | None:
    """The event a frame delivers, ``None`` for a keepalive."""
    payload = frame.WhichOneof("payload")
    if payload == "snapshot":
        rows = [_decode_live_row(raw, message) for raw in frame.snapshot.rows_json]
        return LiveQueryEvent(snapshot=rows, resumed=resumed)
    if payload == "change":
        change = frame.change
        if not change.event_id and change.op == livequery_pb.LIVE_QUERY_CHANGE_OP_UNSPECIFIED:
            return None  # A keepalive from a broker older than explicit heartbeats.
        return LiveQueryEvent(
            op=change.op, row=_decode_live_row(change.row_json, message), event_id=change.event_id
        )
    return None


def _transient_or_raise(error: UdbRpcError) -> None:
    """Raise a refusal reconnecting cannot fix; return for a transient failure."""
    code = error.code
    if isinstance(error, UdbCredentialError) and code == grpc.StatusCode.CANCELLED:
        raise error  # The client was closed; no reconnect can succeed.
    if code == grpc.StatusCode.FAILED_PRECONDITION:
        if getattr(error, "capability_required", "") == "cdc_change_feed":
            raise UdbNoDeltaFeed(
                "udb: live query: the broker has no CDC change feed "
                f"(set UDB_KAFKA_BROKERS and leave UDB_CDC_ENABLED on): {error}"
            ) from error
        raise error
    if code in _FATAL_CODES:
        raise error


class _Reconnect:
    """The reconnect schedule shared by the sync and async queries."""

    def __init__(self, backoff: float | None, message: Any, on_error: Callable[[Exception], None] | None):
        self._backoff = backoff if backoff and backoff > 0 else _DEFAULT_BACKOFF_S
        self._delay = self._backoff
        self._message_type = message_type_of(message)
        self._on_error = on_error

    def next_wait(self, progressed: bool, error: Exception) -> float:
        if progressed:
            self._delay = self._backoff
        if self._on_error is not None:
            self._on_error(UdbError(f"udb: live query {self._message_type} reconnecting: {error}"))
        retry_after = int(getattr(error, "retry_after_ms", 0) or 0) / 1000.0
        wait = max(self._delay, retry_after)
        self._delay = min(self._delay * 2, _MAX_BACKOFF_S)
        return wait


def _headers(metadata: tuple[tuple[str, str], ...], since_event_id: str) -> tuple[tuple[str, str], ...]:
    # Brokers before 0.5.29 read the resume point from this header only.
    return metadata + ((_RESUME_HEADER, since_event_id),) if since_event_id else metadata


class LiveQuery:
    """A view of matching rows. Iterate it, or give :meth:`run` a handler."""

    def __init__(
        self,
        stub: Any,
        request: Any,
        message: Any,
        metadata: Callable[[], tuple[tuple[str, str], ...]],
        *,
        since_event_id: str = "",
        backoff: float | None = None,
        on_error: Callable[[Exception], None] | None = None,
    ):
        self._stub = stub
        self._request = request
        self._message = message_class(message)
        self._metadata = metadata
        self._last_event_id = since_event_id
        self._reconnect = _Reconnect(backoff, message, on_error)
        self._stop = threading.Event()
        self._call: Any = None

    @property
    def last_event_id(self) -> str:
        """The last delivered change, for resuming in a later process via ``since_event_id``."""
        return self._last_event_id

    def cancel(self) -> None:
        """End the view. Safe from another thread; iteration then ends."""
        self._stop.set()
        call = self._call
        if call is not None:
            call.cancel()

    def __enter__(self) -> "LiveQuery":
        return self

    def __exit__(self, *_: object) -> None:
        self.cancel()

    def run(self, handle: Callable[[LiveQueryEvent], None]) -> None:
        """Call ``handle`` for each event until :meth:`cancel`, a raised refusal, or
        ``handle`` raising, which stops the view and propagates."""
        with contextlib.closing(iter(self)) as events:
            for event in events:
                handle(event)

    def __iter__(self) -> Generator[LiveQueryEvent, None, None]:
        connects = 0
        while not self._stop.is_set():
            progressed = False
            call = None
            try:
                self._request.since_event_id = self._last_event_id
                call = self._stub.Subscribe(
                    self._request,
                    metadata=_headers(self._metadata(), self._last_event_id),
                    timeout=None,
                )
                self._call = call
                if self._stop.is_set():
                    return
                for frame in call:
                    event = _frame_event(frame, self._message, connects > 0)
                    if event is None:
                        continue
                    yield event
                    if event.event_id:
                        self._last_event_id = event.event_id
                    progressed = True
                failure: Exception = UdbError("the live query stream ended")
            except grpc.RpcError as error:
                if self._stop.is_set():
                    return
                failure = _map_error("Subscribe", error)
                _transient_or_raise(failure)
            except UdbRpcError as error:  # A local credential refusal.
                failure = error
                _transient_or_raise(failure)
            finally:
                if call is not None:
                    call.cancel()
                self._call = None
            connects += 1
            if self._stop.is_set():
                return
            if self._stop.wait(self._reconnect.next_wait(progressed, failure)):
                return


class AsyncLiveQuery:
    """The async form of :class:`LiveQuery`: ``async for event in query``."""

    def __init__(
        self,
        stub_factory: Callable[[], Any],
        request: Any,
        message: Any,
        metadata: Callable[[], tuple[tuple[str, str], ...]],
        *,
        since_event_id: str = "",
        backoff: float | None = None,
        on_error: Callable[[Exception], None] | None = None,
    ):
        self._stub_factory = stub_factory
        self._request = request
        self._message = message_class(message)
        self._metadata = metadata
        self._last_event_id = since_event_id
        self._reconnect = _Reconnect(backoff, message, on_error)
        self._cancelled = False
        self._call: Any = None

    @property
    def last_event_id(self) -> str:
        """See :attr:`LiveQuery.last_event_id`."""
        return self._last_event_id

    def cancel(self) -> None:
        """End the view; iteration then ends."""
        self._cancelled = True
        call = self._call
        if call is not None:
            call.cancel()

    async def __aiter__(self) -> AsyncIterator[LiveQueryEvent]:
        stub = self._stub_factory()
        connects = 0
        while not self._cancelled:
            progressed = False
            call = None
            try:
                self._request.since_event_id = self._last_event_id
                call = stub.Subscribe(
                    self._request,
                    metadata=_headers(self._metadata(), self._last_event_id),
                    timeout=None,
                )
                self._call = call
                async for frame in call:
                    event = _frame_event(frame, self._message, connects > 0)
                    if event is None:
                        continue
                    yield event
                    if event.event_id:
                        self._last_event_id = event.event_id
                    progressed = True
                failure: Exception = UdbError("the live query stream ended")
            except asyncio.CancelledError:
                if self._cancelled:
                    return
                raise
            except grpc.RpcError as error:
                if self._cancelled:
                    return
                failure = _map_error("Subscribe", error)
                _transient_or_raise(failure)
            except UdbRpcError as error:
                failure = error
                _transient_or_raise(failure)
            finally:
                if call is not None:
                    call.cancel()
                self._call = None
            connects += 1
            if self._cancelled:
                return
            await asyncio.sleep(self._reconnect.next_wait(progressed, failure))
