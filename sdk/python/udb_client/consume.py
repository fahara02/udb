"""Durable consumers.

:func:`consume` reads a topic as a named consumer: the broker stores how far the
consumer has got and resumes it there after a restart, a reconnect or a credential
refresh, so a service keeps no cursor of its own.

Delivery is at-least-once. An event is acknowledged after the handler returns, so a
crash redelivers at most the event in flight (or the unsettled batch, with
``ack_batch_size`` above 1). An event the handler keeps failing on is dead-lettered
and later events keep flowing; a broker that cannot dead-letter stops the consumer
instead of discarding the event.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from inspect import isawaitable
import json
import queue
import threading
import time
from typing import Any, Callable

import grpc
from google.protobuf import json_format

from udb.entity.v1 import types_pb2

from .error_code import inspect, map_rpc_error
from .exceptions import UdbError
from .tx import EVENT_ENVELOPE_VERSION

_MAX_ATTEMPTS = 1000
_MAX_ACK_BATCH = 1024
_MAX_BACKOFF_S = 30.0
_SETTLEMENT_TIMEOUT_S = 5.0
_MAX_FAILURE_REASON_BYTES = 2048
_RECENT_LIMIT = 4096
_STOP_POLL_S = 0.05
_FATAL_CODES = frozenset(
    {
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.NOT_FOUND,
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.OUT_OF_RANGE,
        grpc.StatusCode.UNIMPLEMENTED,
    }
)


@dataclass
class ConsumedEvent:
    """One event delivered to a handler."""

    id: str
    type: str
    topic: str
    partition_key: str
    correlation_id: str
    #: The event message when ``message_type`` was given, else the payload dict.
    payload: Any
    #: The full envelope as delivered, for fields ``payload`` lacks.
    envelope: dict[str, Any]
    published_at: datetime | None = None


@dataclass
class ConsumeOptions:
    """Tune a consumer. The defaults are sensible."""

    #: How often a failing handler is tried for one event before it is dead-lettered
    #: (default 5, at most 1000).
    max_attempts: int = 5
    #: The first retry delay in seconds, doubled per attempt up to 30. Also paces reconnects.
    backoff: float = 0.5
    #: Acknowledge this many successful events together (default 1, at most 1024).
    #: At 1, an event is acknowledged before the next is read and a failed
    #: acknowledgement stops the stream before later events can advance the cursor.
    #: Above 1, the batch settles as one watermark, so a refusal or a crash redelivers
    #: the whole batch. Handlers must be idempotent either way.
    ack_batch_size: int = 1
    #: Flush a partial batch this often, in seconds, even on an idle stream.
    ack_interval: float = 1.0
    #: Bound on each settlement in seconds, including a shutdown flush (at most 30).
    ack_timeout: float = _SETTLEMENT_TIMEOUT_S
    #: A consumer name that has never subscribed replays the retained history. By
    #: default it starts at the journal head. No effect once the name has a position.
    from_earliest: bool = False
    #: Observes handler failures and reconnects.
    on_error: Callable[[Exception], None] | None = None

    def normalized(self) -> "ConsumeOptions":
        return ConsumeOptions(
            max_attempts=min(self.max_attempts if self.max_attempts > 0 else 5, _MAX_ATTEMPTS),
            backoff=min(self.backoff if self.backoff > 0 else 0.5, _MAX_BACKOFF_S),
            ack_batch_size=min(self.ack_batch_size if self.ack_batch_size > 0 else 1, _MAX_ACK_BATCH),
            ack_interval=self.ack_interval if self.ack_interval > 0 else 1.0,
            ack_timeout=min(self.ack_timeout if self.ack_timeout > 0 else _SETTLEMENT_TIMEOUT_S, _MAX_BACKOFF_S),
            from_earliest=self.from_earliest,
            on_error=self.on_error,
        )


class _Fatal(Exception):
    """A failure reconnecting cannot fix."""

    def __init__(self, error: Exception):
        super().__init__(str(error))
        self.error = error


class _SettlementRefused(Exception):
    """A failure from a settlement call rather than the stream, so its batch is not
    reissued on the way out."""

    def __init__(self, error: Exception):
        super().__init__(str(error))
        self.error = error


def _attempt_error(error: Exception) -> Exception:
    if inspect(error).grpc_code in _FATAL_CODES:
        return _Fatal(error)
    return error


def _failure_reason(error: BaseException) -> str:
    raw = str(error).encode("utf-8", "replace")
    if len(raw) <= _MAX_FAILURE_REASON_BYTES:
        return raw.decode("utf-8", "replace")
    return raw[:_MAX_FAILURE_REASON_BYTES].decode("utf-8", "ignore")


def _decode_event(
    event_id: str, topic: str, partition: str, payload_json: str, message_type: Any
) -> ConsumedEvent:
    """Read the envelope a stream delivered. The domain event is under ``payload``; a
    journal row that wraps the envelope once more is unwrapped."""
    try:
        envelope = json.loads(payload_json)
    except ValueError as error:
        raise ValueError(f"envelope is not JSON: {error}") from error
    inner = envelope.get("payload") if isinstance(envelope, dict) else None
    if isinstance(inner, dict) and "event_type" in inner and "payload" in inner:
        envelope = inner
    version = envelope.get("envelope_version") if isinstance(envelope, dict) else None
    if isinstance(version, bool) or not isinstance(version, (int, float)):
        raise ValueError(
            "envelope has no envelope_version; unversioned envelopes are not supported "
            f"(this SDK reads version {EVENT_ENVELOPE_VERSION})"
        )
    if int(version) > EVENT_ENVELOPE_VERSION:
        raise ValueError(
            f"envelope_version {int(version)} is newer than this SDK understands "
            f"({EVENT_ENVELOPE_VERSION}); upgrade the SDK"
        )
    if int(version) != EVENT_ENVELOPE_VERSION:
        raise ValueError(
            f"envelope_version {int(version)} is not supported; this SDK reads version {EVENT_ENVELOPE_VERSION}"
        )
    domain = envelope.get("payload")
    payload: Any = domain
    if message_type is not None:
        payload = message_type()
        if domain is not None:
            try:
                json_format.ParseDict(domain, payload, ignore_unknown_fields=True)
            except json_format.ParseError as error:
                raise ValueError(
                    f"payload does not decode as {message_type.DESCRIPTOR.full_name}: {error}"
                ) from error

    def text(key: str) -> str:
        value = envelope.get(key)
        return value if isinstance(value, str) else ""

    return ConsumedEvent(
        id=event_id,
        type=text("event_type"),
        topic=topic,
        partition_key=partition,
        correlation_id=text("correlation_id"),
        payload=payload,
        envelope=envelope,
    )


class _Progress:
    """Three distinct states of an event id, which must not be conflated: its handler
    completed (a redelivery never runs it again), the broker confirmed its cursor or
    dead-letter (durable), and it sits in the open acknowledgement batch."""

    def __init__(self) -> None:
        self.order: list[str] = []
        self.handled: set[str] = set()
        self.durable: set[str] = set()
        self.pending: list[str] = []
        self.pending_set: set[str] = set()
        self.poison: tuple[str, int, str] | None = None
        # Counts confirmed settlements, to tell a routine reauthentication from a
        # stream that made no progress.
        self.settlements = 0

    def add(self, event_id: str) -> None:
        if event_id in self.handled:
            return
        self.handled.add(event_id)
        self.order.append(event_id)
        if len(self.order) > _RECENT_LIMIT:
            self.handled.discard(self.order.pop(0))

    def enqueue(self, event_id: str) -> None:
        # A redelivery of a handled event is queued on purpose: the broker never saw
        # the lost acknowledgement, so sending it again is what advances the cursor.
        self.pending.append(event_id)
        self.pending_set.add(event_id)

    def needs_settling(self) -> bool:
        return bool(self.pending or self.poison)

    def acked(self) -> None:
        for event_id in self.pending:
            self.add(event_id)
            self.durable.add(event_id)
        self._clear_pending()
        self.settlements += 1

    def ack_failed(self, batched: bool) -> None:
        if not batched:
            # Nothing to carry across the reconnect: the broker redelivers the event
            # and the already-handled path acknowledges it again.
            self._clear_pending()

    def _clear_pending(self) -> None:
        self.pending.clear()
        self.pending_set.clear()

    def dead_lettered(self) -> None:
        assert self.poison is not None
        self.add(self.poison[0])
        self.durable.add(self.poison[0])
        self.poison = None
        self.settlements += 1


def _ack_request(name: str, pattern: str, event_id: str) -> types_pb2.AckCdcEventsRequest:
    return types_pb2.AckCdcEventsRequest(consumer_name=name, topic_pattern=pattern, event_id=event_id)


def _nack_request(name: str, pattern: str, poison: tuple[str, int, str]) -> types_pb2.NackCdcEventsRequest:
    return types_pb2.NackCdcEventsRequest(
        consumer_name=name,
        topic_pattern=pattern,
        event_id=poison[0],
        attempts=poison[1],
        failure_reason=poison[2],
    )


def _subscribe_request(name: str, pattern: str, options: ConsumeOptions) -> types_pb2.CDCSubscriptionRequest:
    start = (
        types_pb2.CDC_CONSUMER_START_EARLIEST
        if options.from_earliest
        else types_pb2.CDC_CONSUMER_START_LATEST
    )
    # Progress markers let a consumer of a quiet topic move its stored position past
    # the rest of the journal; they never reach the handler.
    return types_pb2.CDCSubscriptionRequest(
        topic_pattern=pattern, consumer_name=name, progress_markers=True, start=start
    )


def _report(options: ConsumeOptions, error: Exception) -> None:
    if options.on_error is not None:
        options.on_error(error)


def _settle_ack_failure(
    name: str, last: str, error: grpc.RpcError, options: ConsumeOptions, progress: _Progress
) -> _SettlementRefused:
    mapped = map_rpc_error("AckCdcEvents", error)
    _report(options, UdbError(f"udb: consumer {name} ack {last}: {mapped}"))
    progress.ack_failed(options.ack_batch_size > 1)
    return _SettlementRefused(_attempt_error(mapped))


def _settle_nack_failure(
    name: str, event_id: str, error: grpc.RpcError, options: ConsumeOptions
) -> _SettlementRefused:
    mapped = map_rpc_error("NackCdcEvents", error)
    _report(options, UdbError(f"udb: consumer {name} dead-letter {event_id}: {mapped}"))
    return _SettlementRefused(_attempt_error(mapped))


def _unwrap(error: BaseException) -> BaseException:
    while isinstance(error, (_Fatal, _SettlementRefused)):
        error = error.error
    return error


def _is_fatal(error: BaseException) -> bool:
    while isinstance(error, _SettlementRefused):
        error = error.error
    return isinstance(error, _Fatal)


def _published_at(envelope: Any) -> datetime | None:
    if not envelope.HasField("published_at"):
        return None
    return envelope.published_at.ToDatetime(tzinfo=timezone.utc)


class _Decision:
    """What the event loop does with one delivered envelope, shared by both loops."""

    SKIP = 0
    SETTLE_IF_DUE = 1
    HANDLE = 2

    @staticmethod
    def of(envelope: Any, progress: _Progress) -> int:
        event_id = envelope.event_id
        if event_id in progress.pending_set:
            return _Decision.SKIP
        if envelope.progress_only:
            # A marker says the stream read every journal row up to its position. Its
            # token goes last into the batch, settled with or after earlier events.
            if event_id in progress.durable:
                return _Decision.SKIP
            progress.enqueue(event_id)
            return _Decision.SETTLE_IF_DUE
        if event_id not in progress.handled and event_id not in progress.durable:
            return _Decision.HANDLE
        # A handled but not durable id is acknowledged again, which advances a cursor
        # the broker never saw confirmed.
        progress.enqueue(event_id)
        return _Decision.SETTLE_IF_DUE


def _decode_delivered(name: str, envelope: Any, message_type: Any) -> ConsumedEvent:
    try:
        event = _decode_event(
            envelope.event_id, envelope.topic, envelope.partition_key, envelope.payload_json, message_type
        )
    except ValueError as error:
        raise _Fatal(UdbError(f"udb: consumer {name}: event {envelope.event_id}: {error}")) from error
    event.published_at = _published_at(envelope)
    return event


def _settle(client: Any, name: str, pattern: str, options: ConsumeOptions, progress: _Progress) -> None:
    if not progress.needs_settling():
        return
    if progress.pending:
        last = progress.pending[-1]
        try:
            client.stub.AckCdcEvents(
                client._with_context(_ack_request(name, pattern, last), None),
                metadata=client._call_metadata(None),
                timeout=options.ack_timeout,
            )
        except grpc.RpcError as error:
            raise _settle_ack_failure(name, last, error, options, progress) from error
        progress.acked()
    if progress.poison is not None:
        try:
            client.stub.NackCdcEvents(
                client._with_context(_nack_request(name, pattern, progress.poison), None),
                metadata=client._call_metadata(None),
                timeout=options.ack_timeout,
            )
        except grpc.RpcError as error:
            raise _settle_nack_failure(name, progress.poison[0], error, options) from error
        progress.dead_lettered()


def _handle_with_retry(
    event: ConsumedEvent,
    handle: Callable[[ConsumedEvent], Any],
    options: ConsumeOptions,
    stop: threading.Event,
) -> Exception | None:
    """Run the handler, retrying; the last failure when attempts ran out, else None."""
    delay = options.backoff
    for attempt in range(1, options.max_attempts + 1):
        try:
            handle(event)
            return None
        except Exception as error:
            _report(options, UdbError(f"event {event.id} attempt {attempt}: {error}"))
            if attempt == options.max_attempts:
                return error
        if stop.wait(delay):
            raise _Stopped
        delay = min(delay * 2, _MAX_BACKOFF_S)
    return None


class _Stopped(Exception):
    """The caller asked the consumer to stop."""


_STREAM_ENDED = object()


def _consume_once(
    client: Any,
    name: str,
    pattern: str,
    handle: Callable[[ConsumedEvent], Any],
    message_type: Any,
    options: ConsumeOptions,
    progress: _Progress,
    stop: threading.Event,
) -> None:
    batched = options.ack_batch_size > 1
    # Retries a dead-letter or watermark the broker has not confirmed before reopening.
    _settle(client, name, pattern, options, progress)
    stream = client.stub.PublishCDC(
        client._with_context(_subscribe_request(name, pattern, options), None),
        metadata=client._call_metadata(None),
    )
    received: "queue.Queue[Any]" = queue.Queue()

    def read() -> None:
        try:
            for envelope in stream:
                received.put(envelope)
            received.put(_STREAM_ENDED)
        except grpc.RpcError as error:
            received.put(error)

    reader = threading.Thread(target=read, name=f"udb-consumer-{name}", daemon=True)
    reader.start()
    next_flush = time.monotonic() + options.ack_interval
    try:
        while True:
            if stop.is_set():
                raise _Stopped
            if batched and time.monotonic() >= next_flush:
                next_flush = time.monotonic() + options.ack_interval
                _settle(client, name, pattern, options, progress)
            try:
                item = received.get(timeout=_STOP_POLL_S)
            except queue.Empty:
                continue
            if item is _STREAM_ENDED or isinstance(item, grpc.RpcError):
                if batched:
                    # Settle the retained watermark before reporting the stream
                    # error, so a reconnect does not replay acknowledged events.
                    _settle(client, name, pattern, options, progress)
                if item is _STREAM_ENDED:
                    raise UdbError("the event stream ended")
                raise _attempt_error(map_rpc_error("PublishCDC", item))
            # A stopped consumer never starts another handler, even for an event the
            # stream had already buffered: it is redelivered to the next run.
            if stop.is_set():
                raise _Stopped
            decision = _Decision.of(item, progress)
            if decision == _Decision.SKIP:
                continue
            if decision == _Decision.HANDLE:
                event = _decode_delivered(name, item, message_type)
                failure = _handle_with_retry(event, handle, options, stop)
                if failure is not None:
                    progress.poison = (event.id, options.max_attempts, _failure_reason(failure))
                    _settle(client, name, pattern, options, progress)
                    continue
                # The handler completed even if the acknowledgement is later lost:
                # a redelivery retries only the acknowledgement.
                progress.add(event.id)
                progress.enqueue(event.id)
            if not batched or len(progress.pending) >= options.ack_batch_size:
                _settle(client, name, pattern, options, progress)
    finally:
        stream.cancel()


def consume(
    db: Any,
    name: str,
    topic_pattern: str,
    handle: Callable[[ConsumedEvent], Any],
    *,
    message_type: Any = None,
    options: ConsumeOptions | None = None,
    stop: threading.Event | None = None,
) -> None:
    """Deliver every event on ``topic_pattern`` to ``handle``, as the durable consumer
    ``name``, until ``stop`` is set. ``db`` is a ``UdbProject`` or ``UdbClient``.
    ``message_type`` decodes each payload into that event message class; without it
    the payload is a dict.

    Returns once ``stop`` is set, after flushing the open batch. Raises on what
    reconnecting cannot fix: invalid consumer or cursor, missing authority, or an
    envelope newer than this SDK understands, which stops the consumer rather than
    being misread. A settlement that fails during the shutdown flush raises too.
    """
    client = getattr(db, "data", db)
    options = (options or ConsumeOptions()).normalized()
    stop = stop or threading.Event()
    progress = _Progress()
    delay = options.backoff

    def finish() -> None:
        try:
            _settle(client, name, topic_pattern, options, progress)
        except _SettlementRefused as refused:
            raise _unwrap(refused) from None

    def best_effort_finish() -> None:
        try:
            finish()
        except UdbError:
            pass  # Reported through on_error; the cause matters more.

    while True:
        if stop.is_set():
            finish()
            return
        before = progress.settlements
        try:
            _consume_once(client, name, topic_pattern, handle, message_type, options, progress, stop)
        except _Stopped:
            finish()
            return
        except (_Fatal, _SettlementRefused, UdbError) as error:
            if isinstance(error, _SettlementRefused) and stop.is_set():
                # This batch was refused: reissuing it on the way out cannot succeed.
                raise _unwrap(error) from None
            if _is_fatal(error):
                if not isinstance(error, _SettlementRefused):
                    best_effort_finish()
                raise _unwrap(error) from None
            failure: Exception = error
        # Credential expiry and progress on a healthy stream must not accumulate an
        # ever-longer outage across a routine reauthentication.
        unauthenticated = inspect(_unwrap(failure)).grpc_code == grpc.StatusCode.UNAUTHENTICATED
        if progress.settlements != before or unauthenticated:
            delay = options.backoff
        _report(options, UdbError(f"udb: consumer {name} reconnecting: {_unwrap(failure)}"))
        if stop.wait(delay):
            finish()
            return
        delay = min(delay * 2, _MAX_BACKOFF_S)


async def _settle_async(
    client: Any, name: str, pattern: str, options: ConsumeOptions, progress: _Progress
) -> None:
    if not progress.needs_settling():
        return
    if progress.pending:
        last = progress.pending[-1]
        try:
            await client.stub.AckCdcEvents(
                client._with_context(_ack_request(name, pattern, last), None),
                metadata=client._call_metadata(None),
                timeout=options.ack_timeout,
            )
        except grpc.RpcError as error:
            raise _settle_ack_failure(name, last, error, options, progress) from error
        progress.acked()
    if progress.poison is not None:
        try:
            await client.stub.NackCdcEvents(
                client._with_context(_nack_request(name, pattern, progress.poison), None),
                metadata=client._call_metadata(None),
                timeout=options.ack_timeout,
            )
        except grpc.RpcError as error:
            raise _settle_nack_failure(name, progress.poison[0], error, options) from error
        progress.dead_lettered()


async def _wait_or_stop(stop: asyncio.Event, delay: float) -> bool:
    try:
        await asyncio.wait_for(stop.wait(), delay)
    except asyncio.TimeoutError:
        return False
    return True


async def _handle_with_retry_async(
    event: ConsumedEvent,
    handle: Callable[[ConsumedEvent], Any],
    options: ConsumeOptions,
    stop: asyncio.Event,
) -> Exception | None:
    delay = options.backoff
    for attempt in range(1, options.max_attempts + 1):
        try:
            outcome = handle(event)
            if isawaitable(outcome):
                await outcome
            return None
        except Exception as error:
            _report(options, UdbError(f"event {event.id} attempt {attempt}: {error}"))
            if attempt == options.max_attempts:
                return error
        if await _wait_or_stop(stop, delay):
            raise _Stopped
        delay = min(delay * 2, _MAX_BACKOFF_S)
    return None


async def _consume_once_async(
    client: Any,
    name: str,
    pattern: str,
    handle: Callable[[ConsumedEvent], Any],
    message_type: Any,
    options: ConsumeOptions,
    progress: _Progress,
    stop: asyncio.Event,
) -> None:
    batched = options.ack_batch_size > 1
    await _settle_async(client, name, pattern, options, progress)
    stream = client.stub.PublishCDC(
        client._with_context(_subscribe_request(name, pattern, options), None),
        metadata=client._call_metadata(None),
    )
    received: "asyncio.Queue[Any]" = asyncio.Queue()

    async def read() -> None:
        try:
            async for envelope in stream:
                await received.put(envelope)
            await received.put(_STREAM_ENDED)
        except grpc.RpcError as error:
            await received.put(error)

    reader = asyncio.ensure_future(read())
    # One pending get across polls: cancelling it on a timeout could drop an event.
    getter: "asyncio.Future[Any]" = asyncio.ensure_future(received.get())
    next_flush = time.monotonic() + options.ack_interval
    try:
        while True:
            if stop.is_set():
                raise _Stopped
            if batched and time.monotonic() >= next_flush:
                next_flush = time.monotonic() + options.ack_interval
                await _settle_async(client, name, pattern, options, progress)
            done, _ = await asyncio.wait({getter}, timeout=_STOP_POLL_S)
            if not done:
                continue
            item = getter.result()
            getter = asyncio.ensure_future(received.get())
            if item is _STREAM_ENDED or isinstance(item, grpc.RpcError):
                if batched:
                    await _settle_async(client, name, pattern, options, progress)
                if item is _STREAM_ENDED:
                    raise UdbError("the event stream ended")
                raise _attempt_error(map_rpc_error("PublishCDC", item))
            if stop.is_set():
                raise _Stopped
            decision = _Decision.of(item, progress)
            if decision == _Decision.SKIP:
                continue
            if decision == _Decision.HANDLE:
                event = _decode_delivered(name, item, message_type)
                failure = await _handle_with_retry_async(event, handle, options, stop)
                if failure is not None:
                    progress.poison = (event.id, options.max_attempts, _failure_reason(failure))
                    await _settle_async(client, name, pattern, options, progress)
                    continue
                progress.add(event.id)
                progress.enqueue(event.id)
            if not batched or len(progress.pending) >= options.ack_batch_size:
                await _settle_async(client, name, pattern, options, progress)
    finally:
        stream.cancel()
        reader.cancel()
        getter.cancel()


async def consume_async(
    db: Any,
    name: str,
    topic_pattern: str,
    handle: Callable[[ConsumedEvent], Any],
    *,
    message_type: Any = None,
    options: ConsumeOptions | None = None,
    stop: asyncio.Event | None = None,
) -> None:
    """See :func:`consume`. ``db`` is a ``UdbAsyncProject`` or ``UdbAsyncClient``, and
    ``handle`` may be sync or async. Cancelling the task stops the consumer too, but
    only ``stop`` flushes an open batch first."""
    client = getattr(db, "data", db)
    options = (options or ConsumeOptions()).normalized()
    stop = stop or asyncio.Event()
    progress = _Progress()
    delay = options.backoff

    async def finish() -> None:
        try:
            await _settle_async(client, name, topic_pattern, options, progress)
        except _SettlementRefused as refused:
            raise _unwrap(refused) from None

    while True:
        if stop.is_set():
            await finish()
            return
        before = progress.settlements
        try:
            await _consume_once_async(client, name, topic_pattern, handle, message_type, options, progress, stop)
        except _Stopped:
            await finish()
            return
        except (_Fatal, _SettlementRefused, UdbError) as error:
            if isinstance(error, _SettlementRefused) and stop.is_set():
                raise _unwrap(error) from None
            if _is_fatal(error):
                if not isinstance(error, _SettlementRefused):
                    try:
                        await finish()
                    except UdbError:
                        pass  # Reported through on_error; the cause matters more.
                raise _unwrap(error) from None
            failure: Exception = error
        unauthenticated = inspect(_unwrap(failure)).grpc_code == grpc.StatusCode.UNAUTHENTICATED
        if progress.settlements != before or unauthenticated:
            delay = options.backoff
        _report(options, UdbError(f"udb: consumer {name} reconnecting: {_unwrap(failure)}"))
        if await _wait_or_stop(stop, delay):
            await finish()
            return
        delay = min(delay * 2, _MAX_BACKOFF_S)
