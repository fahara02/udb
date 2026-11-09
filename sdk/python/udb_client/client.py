from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
import json
import threading
import time
from typing import Any, AsyncIterator, Awaitable, Callable, Iterable, Iterator, Mapping, Sequence

import grpc
from google.protobuf.struct_pb2 import Struct

from udb.entity.v1 import types_pb2
from udb.services.v1 import data_broker_pb2_grpc

from ._channel import merge_channel_options, ssl_credentials
from .error_code import map_rpc_error
from .exceptions import UdbConfigurationError
from .generated_client import (
    RPC_OPERATION_KIND,
    RetryPolicy,
    _is_replay_safe,
    _request_has_idempotency_key,
    _retry_deadline,
)
from .metadata import ConsistencyMode, Metadata, ReadFence, WriteReceipt
from .record_codec import format_timestamp

JsonMapping = Mapping[str, Any]

_DATA_BROKER_PATH = "/udb.services.v1.DataBroker/"
_EMPTY_CAS = (
    "udb: guarded write requires a non-empty expected precondition; refusing to run unconditionally"
)
# How long after a write its fence rides on reads, and how long a fenced read may
# wait for its write to land.
_READ_YOUR_WRITES_WINDOW_S = 30.0
_READ_FENCE_WAIT_MS = 2000


def receipt_from_response(response: Any) -> WriteReceipt:
    """Parse the typed :class:`WriteReceipt` from a ``MutationResponse``.

    Reads ``MutationResponse.write_receipt_json`` (field 7); returns an empty
    receipt when the field is absent/blank so callers never hand-parse JSON.
    """
    return WriteReceipt.from_json(getattr(response, "write_receipt_json", "") or "")


def was_duplicate(response: Any) -> bool:
    """Whether a ``MutationResponse`` was a durable-idempotency replay.

    Ergonomic accessor for ``MutationResponse.was_duplicate`` (field 6): the
    broker sets it when a mutation carrying an idempotency key was durably
    deduplicated (a replayed write returning the original receipt) rather than
    applied fresh. Lets callers distinguish a replay from a new write without
    reaching into the raw proto; the raw response stays available. Returns
    ``False`` defensively when the field is absent.
    """
    return bool(getattr(response, "was_duplicate", False))


def optional_struct(value: JsonMapping | Struct | None) -> Struct | None:
    """A request's `payload`, or None when the caller gave none. An empty Struct is
    still a present payload, and the broker reads a present payload as the row, so
    sending one beside `record_json` replaced the caller's record with nothing."""
    return None if value is None else to_struct(value)


def to_struct(value: JsonMapping | Struct | None) -> Struct:
    if isinstance(value, Struct):
        return value
    struct = Struct()
    if value:
        struct.update(_json_safe(value))
    return struct


def _json_safe(value: Any) -> Any:
    """Values a Struct accepts: nested mappings and sequences become dicts and
    lists, and a datetime becomes the text udb stores for a TIMESTAMPTZ."""
    if isinstance(value, Mapping):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return format_timestamp(value)
    # A Struct number is a double: beyond 2^53 an integer travels as its decimal string,
    # which the broker reads back exactly.
    if isinstance(value, int) and not isinstance(value, bool) and abs(value) >= 2**53:
        return str(value)
    return value


def to_record_json(record: JsonMapping | bytes | str) -> bytes:
    if isinstance(record, bytes):
        return record
    if isinstance(record, str):
        return record.encode("utf-8")
    return json.dumps(record, separators=(",", ":"), sort_keys=True).encode("utf-8")


def decode_records(record_set: types_pb2.RecordSet) -> list[dict[str, Any]]:
    """Decode a RecordSet's rows. ``records_json`` is the canonical representation;
    ``rows`` without it means the records are missing, not that nothing matched, and
    an empty body fails rather than leaving the page short."""
    if not record_set.records_json and len(record_set.rows) > 0:
        raise ValueError(
            f"udb: record set carries {len(record_set.rows)} compatibility row(s) but no records_json"
        )
    records: list[dict[str, Any]] = []
    for index, raw in enumerate(record_set.records_json):
        if not raw:
            raise ValueError(f"udb: decode record row {index}: empty body")
        records.append(json.loads(raw.decode("utf-8")))
    return records


@dataclass
class Page:
    """One page of a keyset-paginated read."""

    rows: list[dict[str, Any]] = field(default_factory=list)
    #: Feed back as ``page_token``; empty on the last page. Opaque.
    next_page_token: str = ""
    #: True when the page is full: more rows may match.
    has_more: bool = False
    #: Every matching row, when ``include_total`` asked for it; 0 otherwise.
    exact_total: int = 0


@dataclass
class UpsertResult:
    """An entity upsert's outcome. Other attributes read from ``response``."""

    response: types_pb2.MutationResponse
    #: The returned row, only when ``return_record`` was set.
    record: dict[str, Any] | None = None
    #: True when the broker collapsed this write onto an earlier one with the same
    #: idempotency key instead of applying it again.
    was_duplicate: bool = False

    def __getattr__(self, name: str) -> Any:
        if name == "response":
            raise AttributeError(name)
        return getattr(self.response, name)


class _ClientBase:
    """State and request plumbing shared by the sync and async data clients."""

    _metadata: Metadata | None
    _retry: RetryPolicy

    def _init_fence(self) -> None:
        self._fence_lock = threading.Lock()
        self._fence_receipt: WriteReceipt | None = None
        self._fence_at = 0.0

    def remember_write(self, response: Any) -> None:
        """Keep a write's receipt so the next reads through this client see it.

        Called by entity handles and typed tables after every write; call it after a
        raw :meth:`upsert` to make that write count as well."""
        try:
            receipt = receipt_from_response(response)
        except (ValueError, TypeError):
            return
        self.remember_receipt(receipt)

    def remember_receipt(self, receipt: WriteReceipt) -> None:
        if receipt == WriteReceipt():
            return
        with self._fence_lock:
            self._fence_receipt = receipt
            self._fence_at = time.monotonic()

    def read_fence(self) -> types_pb2.RequestContext | None:
        """The context carrying the latest write's fence, ``None`` when no write
        happened in the last 30 seconds."""
        with self._fence_lock:
            receipt, at = self._fence_receipt, self._fence_at
        if receipt is None or time.monotonic() - at > _READ_YOUR_WRITES_WINDOW_S:
            return None
        fence = ReadFence.from_receipt(receipt, _READ_FENCE_WAIT_MS)
        return types_pb2.RequestContext(read_fence_json=fence.to_json())

    def _retry_facts(self, rpc_name: str, request: Any) -> dict[str, bool]:
        path = _DATA_BROKER_PATH + rpc_name
        return {
            "read_only": RPC_OPERATION_KIND.get(path) == "read_only",
            "replay_safe": _is_replay_safe(path),
            "has_idempotency_key": _request_has_idempotency_key(request),
        }

    def _effective_metadata(self, metadata: Metadata | None) -> Metadata:
        effective = metadata or self._metadata
        if effective is None:
            raise UdbConfigurationError(
                "No UDB metadata is bound. Pass metadata=... or call bind_metadata()."
            )
        return effective

    def _call_metadata(self, metadata: Metadata | None) -> tuple[tuple[str, str], ...]:
        return self._effective_metadata(metadata).to_grpc_metadata()

    def _with_context(self, request: Any, metadata: Metadata | None) -> Any:
        if not hasattr(request, "context"):
            return request
        cloned = type(request)()
        cloned.CopyFrom(request)
        if not cloned.context.tenant_id:
            fence = cloned.context.read_fence_json
            cloned.context.CopyFrom(self._effective_metadata(metadata).to_request_context())
            # A fence the caller put on its metadata wins over the automatic one.
            if fence and not cloned.context.read_fence_json:
                cloned.context.read_fence_json = fence
        return cloned


def _insert_request(
    message_type: str,
    record: JsonMapping | bytes | str | None,
    payload: JsonMapping | Struct | None,
    return_record: bool,
    idempotency_key: str,
) -> types_pb2.UpsertRequest:
    request = types_pb2.UpsertRequest(
        message_type=message_type,
        record_json=to_record_json(record or {}),
        return_record=return_record,
        idempotency_key=idempotency_key,
    )
    if payload:
        request.payload.CopyFrom(to_struct(payload))
    return request


def _update_request(
    message_type: str,
    filter: JsonMapping | Struct | None,
    changes: JsonMapping | Struct | None,
    increments: Mapping[str, float] | None,
    expected: JsonMapping | Struct | None,
    idempotency_key: str,
    return_record: bool,
    expected_revision: str,
    require_affected: int,
) -> types_pb2.UpdateRequest:
    request = types_pb2.UpdateRequest(
        message_type=message_type,
        idempotency_key=idempotency_key,
        return_record=return_record,
        expected_revision=expected_revision,
        require_affected=require_affected,
    )
    if filter:
        request.filter.CopyFrom(to_struct(filter))
    if changes:
        request.changes.CopyFrom(to_struct(changes))
    if expected:
        request.expected.CopyFrom(to_struct(expected))
    # Deterministic wire order for the increments.
    for column in sorted(increments or {}):
        request.increments.add(column=column, delta=float(increments[column]))  # type: ignore[index]
    return request


def _check_idempotency_key(key: str) -> None:
    # A blank key would silently disable replay-safety while looking set.
    if key and not key.strip():
        raise ValueError("udb: idempotency key must not be whitespace-only")


def _sort_keys(sort: Sequence[types_pb2.Sort | str]) -> list[types_pb2.Sort]:
    keys = []
    for item in sort:
        if isinstance(item, str):
            descending = item.startswith("-")
            keys.append(types_pb2.Sort(field=item[1:] if descending else item, descending=descending))
        else:
            keys.append(item)
    return keys


def _page_from(record_set: types_pb2.RecordSet) -> Page:
    return Page(
        rows=decode_records(record_set),
        next_page_token=record_set.next_page_token,
        has_more=record_set.has_more,
        exact_total=record_set.exact_total,
    )


def _upsert_result(response: types_pb2.MutationResponse, return_record: bool) -> UpsertResult:
    result = UpsertResult(response=response, was_duplicate=response.was_duplicate)
    if return_record and response.record_json:
        result.record = json.loads(response.record_json.decode("utf-8"))
    return result


class UdbClient(_ClientBase):
    """Synchronous UDB DataBroker client with metadata injection.

    Unary calls retry per ``retry`` (default :class:`RetryPolicy`): reads on
    transient codes, mutations only when replay-safe and idempotency-keyed. A failed
    call raises :class:`~udb_client.exceptions.UdbRpcError`, carrying the broker's
    ErrorDetail as :class:`~udb_client.generated_client.UdbDetailedRpcError` when
    one was sent.
    """

    def __init__(
        self,
        target: str,
        metadata: Metadata | None = None,
        *,
        secure: bool = False,
        root_certificates: bytes | None = None,
        client_certificate_chain: bytes | None = None,
        client_private_key: bytes | None = None,
        channel_options: Sequence[tuple[str, Any]] | None = None,
        timeout: float | None = 30.0,
        channel: grpc.Channel | None = None,
        retry: RetryPolicy | None = None,
    ):
        if not target and channel is None:
            raise UdbConfigurationError("UDB target is required, e.g. '127.0.0.1:50051'")
        self._metadata = metadata
        self._timeout = timeout
        self._retry = retry or RetryPolicy()
        self._init_fence()
        self._owns_channel = channel is None
        if channel is None:
            options = merge_channel_options(channel_options)
            if secure:
                credentials = ssl_credentials(
                    root_certificates, client_private_key, client_certificate_chain
                )
                channel = grpc.secure_channel(target, credentials, options=options)
            else:
                channel = grpc.insecure_channel(target, options=options)
        self._channel = channel
        self._stub = data_broker_pb2_grpc.DataBrokerStub(channel)

    @property
    def stub(self) -> data_broker_pb2_grpc.DataBrokerStub:
        """The generated raw DataBroker stub for advanced RPCs."""

        return self._stub

    def bind_metadata(self, metadata: Metadata) -> None:
        self._metadata = metadata

    def close(self) -> None:
        if self._owns_channel:
            self._channel.close()

    def __enter__(self) -> "UdbClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def call(
        self,
        rpc_name: str,
        request: Any,
        *,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> Any:
        method = getattr(self._stub, rpc_name)
        request = self._with_context(request, metadata)
        facts = self._retry_facts(rpc_name, request)
        effective_timeout = self._timeout if timeout is None else timeout
        deadline = _retry_deadline(effective_timeout)
        attempt = 0
        while True:
            attempt += 1
            try:
                return method(
                    request,
                    metadata=self._call_metadata(metadata),
                    timeout=effective_timeout,
                )
            except grpc.RpcError as error:
                if self._retry.should_retry(error.code(), attempt, **facts):
                    wait = self._retry.wait_for(attempt, error, deadline)
                    if wait is not None:
                        time.sleep(wait)
                        continue
                raise map_rpc_error(rpc_name, error) from error

    def select(
        self,
        request: types_pb2.SelectRequest | None = None,
        *,
        message_type: str = "",
        filter: JsonMapping | Struct | None = None,
        fields: Sequence[str] = (),
        limit: int = 0,
        page_token: str = "",
        sort: Sequence[types_pb2.Sort] = (),
        include_total: bool = False,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.RecordSet:
        req = request or types_pb2.SelectRequest(
            message_type=message_type,
            filter=to_struct(filter),
            fields=list(fields),
            limit=limit,
            page_token=page_token,
            sort=list(sort),
            include_total=include_total,
        )
        return self.call("Select", req, metadata=metadata, timeout=timeout)

    def upsert(
        self,
        request: types_pb2.UpsertRequest | None = None,
        *,
        message_type: str = "",
        record: JsonMapping | bytes | str | None = None,
        payload: JsonMapping | Struct | None = None,
        conflict_fields: Sequence[str] = (),
        return_record: bool = False,
        idempotency_key: str = "",
        expected: JsonMapping | Struct | None = None,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        # `expected` is the broker's optimistic CAS precondition
        # (UpsertRequest.expected, enforced while holding the target row lock).
        # It was reachable only by hand-building an UpsertRequest, so the
        # convenience method could not express a compare-and-swap at all — the
        # operation acknowledgement and workflow state transitions need. An unset
        # or empty value behaves exactly as before.
        req = request or types_pb2.UpsertRequest(
            message_type=message_type,
            record_json=to_record_json(record or {}),
            payload=optional_struct(payload),
            conflict_fields=list(conflict_fields),
            return_record=return_record,
            idempotency_key=idempotency_key,
            expected=to_struct(expected),
        )
        return self.call("Upsert", req, metadata=metadata, timeout=timeout)

    def delete(
        self,
        request: types_pb2.DeleteRequest | None = None,
        *,
        message_type: str = "",
        filter: JsonMapping | Struct | None = None,
        idempotency_key: str = "",
        expected: JsonMapping | Struct | None = None,
        require_affected: int = 0,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        req = request or types_pb2.DeleteRequest(
            message_type=message_type,
            filter=to_struct(filter),
            idempotency_key=idempotency_key,
            require_affected=require_affected,
        )
        if request is None and expected:
            req.expected.CopyFrom(to_struct(expected))
        return self.call("Delete", req, metadata=metadata, timeout=timeout)

    def insert(
        self,
        request: types_pb2.UpsertRequest | None = None,
        *,
        message_type: str = "",
        record: JsonMapping | bytes | str | None = None,
        payload: JsonMapping | Struct | None = None,
        return_record: bool = False,
        idempotency_key: str = "",
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        """Create a row. A primary or unique-key collision raises and never replaces
        the existing row. Older brokers answer UNIMPLEMENTED."""
        req = request or _insert_request(message_type, record, payload, return_record, idempotency_key)
        return self.call("Insert", req, metadata=metadata, timeout=timeout)

    def update(
        self,
        request: types_pb2.UpdateRequest | None = None,
        *,
        message_type: str = "",
        filter: JsonMapping | Struct | None = None,
        changes: JsonMapping | Struct | None = None,
        increments: Mapping[str, float] | None = None,
        expected: JsonMapping | Struct | None = None,
        idempotency_key: str = "",
        return_record: bool = False,
        expected_revision: str = "",
        require_affected: int = 0,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        """One partial update: set only ``changes`` (``None`` writes SQL NULL) and
        apply ``increments`` atomically on the rows ``filter`` matches."""
        req = request or _update_request(
            message_type, filter, changes, increments, expected,
            idempotency_key, return_record, expected_revision, require_affected,
        )
        return self.call("Update", req, metadata=metadata, timeout=timeout)

    def upsert_with_receipt(
        self, *args: Any, **kwargs: Any
    ) -> tuple[types_pb2.MutationResponse, WriteReceipt]:
        """Like :meth:`upsert` but also returns the parsed :class:`WriteReceipt`.

        Lets callers do ``resp, receipt = udb.upsert_with_receipt(...)`` then
        ``metadata.after_write(receipt)`` for read-your-writes without
        hand-parsing ``write_receipt_json``.
        """
        response = self.upsert(*args, **kwargs)
        return response, receipt_from_response(response)

    def delete_with_receipt(
        self, *args: Any, **kwargs: Any
    ) -> tuple[types_pb2.MutationResponse, WriteReceipt]:
        """Like :meth:`delete` but also returns the parsed :class:`WriteReceipt`."""
        response = self.delete(*args, **kwargs)
        return response, receipt_from_response(response)

    def entity(self, message_type: str, *, key: Sequence[str] = ()) -> "_BoundEntity":
        """Bind an entity once, then ``select/upsert/delete`` with plain dicts.

        ``key`` is the conflict (primary-key) field set used for upserts. The
        returned wrapper reuses :func:`to_record_json`/:func:`to_struct`/
        :func:`decode_records`; it binds the supplied key once and never fetches
        the catalog per request.
        """
        return _BoundEntity(self, message_type, tuple(key))

    def table(self, name: str, *, key: Sequence[str] = ()) -> "_BoundEntity":
        """Table-shaped alias of :meth:`entity` (``udb.data.table("invoice")``)."""
        return self.entity(name, key=key)

    def tx(self, fn: Callable[[Any], Any], *, metadata: Metadata | None = None, timeout: float | None = None) -> None:
        """Run ``fn(tx)`` and commit everything it collected atomically: every write
        and every event commits together or none does. See :class:`udb_client.tx.TxScope`."""
        from .tx import run_tx

        run_tx(self, fn, metadata=metadata, timeout=timeout)

    def batch_select(
        self,
        requests: Iterable[types_pb2.SelectRequest],
        *,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> Iterator[types_pb2.RecordSet]:
        return _mapped_stream(
            "BatchSelect",
            lambda: self._stub.BatchSelect(
                (self._with_context(req, metadata) for req in requests),
                metadata=self._call_metadata(metadata),
                timeout=self._timeout if timeout is None else timeout,
            ),
        )

    def batch_upsert(
        self,
        requests: Iterable[types_pb2.UpsertRequest],
        *,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> Iterator[types_pb2.MutationResponse]:
        return _mapped_stream(
            "BatchUpsert",
            lambda: self._stub.BatchUpsert(
                (self._with_context(req, metadata) for req in requests),
                metadata=self._call_metadata(metadata),
                timeout=self._timeout if timeout is None else timeout,
            ),
        )

    def vector_upsert(
        self,
        collection: str,
        points: Sequence[types_pb2.VectorPointMutation],
        *,
        idempotency_key: str = "",
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        req = types_pb2.VectorUpsertRequest(
            collection=collection,
            points=list(points),
            idempotency_key=idempotency_key,
        )
        return self.call("VectorUpsert", req, metadata=metadata, timeout=timeout)

    def vector_search(
        self,
        collection: str,
        vector: Sequence[float],
        *,
        filter: JsonMapping | Struct | None = None,
        limit: int = 10,
        score_threshold: float = 0.0,
        with_payload: bool = True,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.VectorSet:
        req = types_pb2.VectorSearchRequest(
            collection=collection,
            vector=list(vector),
            filter=to_struct(filter),
            limit=limit,
            score_threshold=score_threshold,
            with_payload=with_payload,
        )
        return self.call("VectorSearch", req, metadata=metadata, timeout=timeout)

    def vector_hybrid_search(
        self,
        collection: str,
        vector: Sequence[float],
        text_query: str,
        *,
        filter: JsonMapping | Struct | None = None,
        limit: int = 10,
        fusion_weights: Sequence[float] = (),
        with_payload: bool = True,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.VectorSet:
        req = types_pb2.VectorHybridSearchRequest(
            collection=collection,
            vector=list(vector),
            text_query=text_query,
            filter=to_struct(filter),
            limit=limit,
            fusion_weights=list(fusion_weights),
            with_payload=with_payload,
        )
        return self.call("VectorHybridSearch", req, metadata=metadata, timeout=timeout)

    def put_object(
        self,
        bucket: str,
        object_key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        chunk_size: int = 1024 * 1024,
        idempotency_key: str = "",
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        context = self._effective_metadata(metadata).to_request_context()

        def chunks() -> Iterator[types_pb2.Chunk]:
            for offset in range(0, len(data), chunk_size):
                end = offset + chunk_size
                yield types_pb2.Chunk(
                    context=context,
                    bucket=bucket,
                    object_key=object_key,
                    data=data[offset:end],
                    final_chunk=end >= len(data),
                    content_type=content_type,
                    idempotency_key=idempotency_key,
                )
            if not data:
                yield types_pb2.Chunk(
                    context=context,
                    bucket=bucket,
                    object_key=object_key,
                    final_chunk=True,
                    content_type=content_type,
                    idempotency_key=idempotency_key,
                )

        try:
            return self._stub.PutObject(
                chunks(),
                metadata=self._call_metadata(metadata),
                timeout=self._timeout if timeout is None else timeout,
            )
        except grpc.RpcError as error:
            raise map_rpc_error("PutObject", error) from error

    def get_object(
        self,
        bucket: str,
        object_key: str,
        *,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> bytes:
        req = types_pb2.ObjectRequest(bucket=bucket, object_key=object_key)
        try:
            chunks = self._stub.GetObject(
                self._with_context(req, metadata),
                metadata=self._call_metadata(metadata),
                timeout=self._timeout if timeout is None else timeout,
            )
            return b"".join(chunk.data for chunk in chunks)
        except grpc.RpcError as error:
            raise map_rpc_error("GetObject", error) from error

    def generate_presigned_url(
        self,
        bucket: str,
        object_key: str,
        *,
        method: str = "GET",
        ttl_seconds: int = 900,
        content_type: str = "",
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.UrlResponse:
        req = types_pb2.UrlRequest(
            bucket=bucket,
            object_key=object_key,
            method=method,
            ttl_seconds=ttl_seconds,
            content_type=content_type,
        )
        return self.call("GeneratePresignedUrl", req, metadata=metadata, timeout=timeout)

    def enqueue_outbox_event(
        self,
        *,
        topic: str,
        partition_key: str,
        payload: JsonMapping | Struct,
        schema_uri: str = "",
        idempotency_key: str = "",
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.EnqueueOutboxEventResponse:
        req = types_pb2.EnqueueOutboxEventRequest(
            topic=topic,
            partition_key=partition_key,
            payload=optional_struct(payload),
            schema_uri=schema_uri,
            idempotency_key=idempotency_key,
        )
        return self.call("EnqueueOutboxEvent", req, metadata=metadata, timeout=timeout)

    def get_capabilities(
        self,
        *,
        project_id: str = "",
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.CapabilitiesResponse:
        req = types_pb2.CapabilitiesRequest(project_id=project_id)
        return self.call("GetCapabilities", req, metadata=metadata, timeout=timeout)

    def warmup(self, *, metadata: Metadata | None = None) -> types_pb2.CapabilitiesResponse:
        return self.get_capabilities(metadata=metadata)


def _mapped_stream(rpc_name: str, open_stream: Callable[[], Iterable[Any]]) -> Iterator[Any]:
    """A response stream whose failures raise the typed error, which they do while
    iterating rather than when the stream opens."""
    try:
        yield from open_stream()
    except grpc.RpcError as error:
        raise map_rpc_error(rpc_name, error) from error


class _EntityBase:
    """Request building shared by the sync and async entity handles.

    A handle binds a message type and key once and takes plain dicts, with no
    per-request catalog lookup. Each verb is exactly one RPC. Reads carry the fence
    of this client's latest handle write, and every write refreshes it, so a read
    after a write sees it even from a replica; a fence on the caller's own metadata
    wins. The broker stays the enforcement point: nothing here sets scopes.
    """

    def __init__(self, client: Any, message_type: str, key: Sequence[str], consistency: Any = None):
        self._client = client
        self._message_type = message_type
        self._key = tuple(key)
        self._consistency = consistency

    def with_consistency(self, mode: "ConsistencyMode | str | None") -> Any:
        """A copy whose calls request ``mode``; ``None`` clears it. The broker stays
        the enforcement point: this only states the caller's preference."""
        return type(self)(self._client, self._message_type, self._key, mode)

    def _metadata_for(self, metadata: Metadata | None) -> Metadata | None:
        if self._consistency is None:
            return metadata
        return self._client._effective_metadata(metadata).with_consistency(self._consistency)

    def _select_request(
        self,
        where: JsonMapping | Struct | None,
        fields: Sequence[str],
        limit: int,
        page_token: str,
        sort: Sequence[types_pb2.Sort | str],
        include_total: bool,
    ) -> types_pb2.SelectRequest:
        request = types_pb2.SelectRequest(
            message_type=self._message_type,
            filter=to_struct(where),
            fields=list(fields),
            limit=limit,
            page_token=page_token,
            sort=_sort_keys(sort),
            include_total=include_total,
        )
        fence = self._client.read_fence()
        if fence is not None:
            request.context.CopyFrom(fence)
        return request

    def _upsert_request(
        self,
        record: JsonMapping,
        return_record: bool,
        idempotency_key: str,
        expected: JsonMapping | Struct | None,
    ) -> types_pb2.UpsertRequest:
        if record is None:
            raise ValueError("udb: Upsert record is None")
        _check_idempotency_key(idempotency_key)
        request = types_pb2.UpsertRequest(
            message_type=self._message_type,
            record_json=to_record_json(record),
            conflict_fields=list(self._key),
            return_record=return_record,
            idempotency_key=idempotency_key,
        )
        if expected:
            request.expected.CopyFrom(to_struct(expected))
        return request

    def _delete_request(
        self,
        where: JsonMapping | Struct | None,
        idempotency_key: str,
        expected: JsonMapping | Struct | None,
    ) -> types_pb2.DeleteRequest:
        # A guarded delete must never degrade into deleting every matched row.
        if expected is not None and not expected:
            raise ValueError(_EMPTY_CAS)
        _check_idempotency_key(idempotency_key)
        request = types_pb2.DeleteRequest(
            message_type=self._message_type,
            filter=to_struct(where),
            idempotency_key=idempotency_key,
        )
        if expected:
            request.expected.CopyFrom(to_struct(expected))
        return request

    def _update_for(
        self,
        where: JsonMapping | Struct | None,
        changes: JsonMapping | Struct | None,
        deltas: Mapping[str, float] | None,
        expected: JsonMapping | Struct | None,
        idempotency_key: str,
        return_record: bool,
    ) -> types_pb2.UpdateRequest:
        if expected is not None and not expected:
            raise ValueError(_EMPTY_CAS)
        return _update_request(
            self._message_type, where, changes, deltas, expected, idempotency_key, return_record, "", 0
        )


class _BoundEntity(_EntityBase):
    """A message-type/table bound to a :class:`UdbClient`, taking plain dicts.

    Returned by :meth:`UdbClient.entity` / :meth:`UdbClient.table`.
    """

    def select(
        self,
        *,
        where: JsonMapping | Struct | None = None,
        fields: Sequence[str] = (),
        limit: int = 0,
        page_token: str = "",
        sort: Sequence[types_pb2.Sort | str] = (),
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> list[dict[str, Any]]:
        """One ``Select``; returns decoded records (no hidden List)."""
        request = self._select_request(where, fields, limit, page_token, sort, False)
        record_set = self._client.select(request, metadata=self._metadata_for(metadata), timeout=timeout)
        return decode_records(record_set)

    def select_page(
        self,
        where: JsonMapping | Struct | None = None,
        *,
        fields: Sequence[str] = (),
        sort: Sequence[types_pb2.Sort | str] = (),
        limit: int = 0,
        page_token: str = "",
        include_total: bool = False,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> Page:
        """One keyset-paginated ``Select``. Pass an empty ``page_token`` for the first
        page, then each returned ``next_page_token`` until it comes back empty. A
        ``sort`` item is a :class:`Sort` or a column name (``"-column"`` descends)."""
        request = self._select_request(where, fields, limit, page_token, sort, include_total)
        record_set = self._client.select(request, metadata=self._metadata_for(metadata), timeout=timeout)
        return _page_from(record_set)

    def upsert(
        self,
        record: JsonMapping,
        *,
        return_record: bool = False,
        idempotency_key: str = "",
        expected: JsonMapping | Struct | None = None,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> UpsertResult:
        """One ``Upsert`` with ``conflict_fields`` from the bound key.

        ``expected`` makes it a compare-and-swap: every field must equal the current
        row or the broker refuses with FAILED_PRECONDITION and writes nothing; an
        empty value leaves it unconditional. ``return_record`` decodes the record
        from the same response, without a second read. The result reads like the
        ``MutationResponse`` and adds ``was_duplicate`` and ``record``.
        """
        request = self._upsert_request(record, return_record, idempotency_key, expected)
        response = self._client.upsert(request, metadata=self._metadata_for(metadata), timeout=timeout)
        self._client.remember_write(response)
        return _upsert_result(response, return_record)

    def update(
        self,
        where: JsonMapping | Struct | None,
        changes: JsonMapping | Struct,
        *,
        expected: JsonMapping | Struct | None = None,
        idempotency_key: str = "",
        return_record: bool = False,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        """One partial update: set only the columns in ``changes`` (``None`` writes
        SQL NULL) on the rows ``where`` matches. ``expected`` makes it a compare-and-
        swap; an empty ``expected`` raises before any RPC, so a guarded update never
        degrades into an unconditional one."""
        request = self._update_for(where, changes, None, expected, idempotency_key, return_record)
        response = self._client.update(request, metadata=self._metadata_for(metadata), timeout=timeout)
        self._client.remember_write(response)
        return response

    def increment(
        self,
        where: JsonMapping | Struct | None,
        deltas: Mapping[str, float],
        *,
        expected: JsonMapping | Struct | None = None,
        idempotency_key: str = "",
        return_record: bool = False,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        """Atomic counter deltas (``column = column + delta``) in one statement, with
        no read-modify-write window."""
        request = self._update_for(where, None, deltas, expected, idempotency_key, return_record)
        response = self._client.update(request, metadata=self._metadata_for(metadata), timeout=timeout)
        self._client.remember_write(response)
        return response

    def delete(
        self,
        *,
        where: JsonMapping | Struct | None = None,
        idempotency_key: str = "",
        expected: JsonMapping | Struct | None = None,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        """One ``Delete`` filtered by ``where``. ``expected`` makes it a compare-and-
        swap; an empty ``expected`` raises before any RPC."""
        request = self._delete_request(where, idempotency_key, expected)
        response = self._client.delete(request, metadata=self._metadata_for(metadata), timeout=timeout)
        self._client.remember_write(response)
        return response


class _AsyncBoundEntity(_EntityBase):
    """The async form of :class:`_BoundEntity`, from :meth:`UdbAsyncClient.entity`."""

    async def select(
        self,
        *,
        where: JsonMapping | Struct | None = None,
        fields: Sequence[str] = (),
        limit: int = 0,
        page_token: str = "",
        sort: Sequence[types_pb2.Sort | str] = (),
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> list[dict[str, Any]]:
        request = self._select_request(where, fields, limit, page_token, sort, False)
        record_set = await self._client.select(request, metadata=self._metadata_for(metadata), timeout=timeout)
        return decode_records(record_set)

    async def select_page(
        self,
        where: JsonMapping | Struct | None = None,
        *,
        fields: Sequence[str] = (),
        sort: Sequence[types_pb2.Sort | str] = (),
        limit: int = 0,
        page_token: str = "",
        include_total: bool = False,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> Page:
        request = self._select_request(where, fields, limit, page_token, sort, include_total)
        record_set = await self._client.select(request, metadata=self._metadata_for(metadata), timeout=timeout)
        return _page_from(record_set)

    async def upsert(
        self,
        record: JsonMapping,
        *,
        return_record: bool = False,
        idempotency_key: str = "",
        expected: JsonMapping | Struct | None = None,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> UpsertResult:
        request = self._upsert_request(record, return_record, idempotency_key, expected)
        response = await self._client.upsert(request, metadata=self._metadata_for(metadata), timeout=timeout)
        self._client.remember_write(response)
        return _upsert_result(response, return_record)

    async def update(
        self,
        where: JsonMapping | Struct | None,
        changes: JsonMapping | Struct,
        *,
        expected: JsonMapping | Struct | None = None,
        idempotency_key: str = "",
        return_record: bool = False,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        request = self._update_for(where, changes, None, expected, idempotency_key, return_record)
        response = await self._client.update(request, metadata=self._metadata_for(metadata), timeout=timeout)
        self._client.remember_write(response)
        return response

    async def increment(
        self,
        where: JsonMapping | Struct | None,
        deltas: Mapping[str, float],
        *,
        expected: JsonMapping | Struct | None = None,
        idempotency_key: str = "",
        return_record: bool = False,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        request = self._update_for(where, None, deltas, expected, idempotency_key, return_record)
        response = await self._client.update(request, metadata=self._metadata_for(metadata), timeout=timeout)
        self._client.remember_write(response)
        return response

    async def delete(
        self,
        *,
        where: JsonMapping | Struct | None = None,
        idempotency_key: str = "",
        expected: JsonMapping | Struct | None = None,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        request = self._delete_request(where, idempotency_key, expected)
        response = await self._client.delete(request, metadata=self._metadata_for(metadata), timeout=timeout)
        self._client.remember_write(response)
        return response


class UdbAsyncClient(_ClientBase):
    """Async UDB DataBroker client with the same convenience surface."""

    def __init__(
        self,
        target: str,
        metadata: Metadata | None = None,
        *,
        secure: bool = False,
        root_certificates: bytes | None = None,
        client_certificate_chain: bytes | None = None,
        client_private_key: bytes | None = None,
        channel_options: Sequence[tuple[str, Any]] | None = None,
        timeout: float | None = 30.0,
        channel: grpc.aio.Channel | None = None,
        retry: RetryPolicy | None = None,
    ):
        if not target and channel is None:
            raise UdbConfigurationError("UDB target is required, e.g. '127.0.0.1:50051'")
        self._metadata = metadata
        self._timeout = timeout
        self._retry = retry or RetryPolicy()
        self._init_fence()
        self._owns_channel = channel is None
        if channel is None:
            options = merge_channel_options(channel_options)
            if secure:
                credentials = ssl_credentials(
                    root_certificates, client_private_key, client_certificate_chain
                )
                channel = grpc.aio.secure_channel(target, credentials, options=options)
            else:
                channel = grpc.aio.insecure_channel(target, options=options)
        self._channel = channel
        self._stub = data_broker_pb2_grpc.DataBrokerStub(channel)

    @property
    def stub(self) -> data_broker_pb2_grpc.DataBrokerStub:
        return self._stub

    def bind_metadata(self, metadata: Metadata) -> None:
        self._metadata = metadata

    async def close(self) -> None:
        if self._owns_channel:
            await self._channel.close()

    async def __aenter__(self) -> "UdbAsyncClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def call(
        self,
        rpc_name: str,
        request: Any,
        *,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> Any:
        method = getattr(self._stub, rpc_name)
        request = self._with_context(request, metadata)
        facts = self._retry_facts(rpc_name, request)
        effective_timeout = self._timeout if timeout is None else timeout
        deadline = _retry_deadline(effective_timeout)
        attempt = 0
        while True:
            attempt += 1
            try:
                return await method(
                    request,
                    metadata=self._call_metadata(metadata),
                    timeout=effective_timeout,
                )
            except grpc.RpcError as error:
                if self._retry.should_retry(error.code(), attempt, **facts):
                    wait = self._retry.wait_for(attempt, error, deadline)
                    if wait is not None:
                        await asyncio.sleep(wait)
                        continue
                raise map_rpc_error(rpc_name, error) from error

    async def select(
        self,
        request: types_pb2.SelectRequest | None = None,
        *,
        message_type: str = "",
        filter: JsonMapping | Struct | None = None,
        fields: Sequence[str] = (),
        limit: int = 0,
        page_token: str = "",
        sort: Sequence[types_pb2.Sort] = (),
        include_total: bool = False,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.RecordSet:
        req = request or types_pb2.SelectRequest(
            message_type=message_type,
            filter=to_struct(filter),
            fields=list(fields),
            limit=limit,
            page_token=page_token,
            sort=list(sort),
            include_total=include_total,
        )
        return await self.call("Select", req, metadata=metadata, timeout=timeout)

    async def upsert(
        self,
        request: types_pb2.UpsertRequest | None = None,
        *,
        message_type: str = "",
        record: JsonMapping | bytes | str | None = None,
        payload: JsonMapping | Struct | None = None,
        conflict_fields: Sequence[str] = (),
        return_record: bool = False,
        idempotency_key: str = "",
        expected: JsonMapping | Struct | None = None,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        # `expected` is the broker's optimistic CAS precondition
        # (UpsertRequest.expected, enforced while holding the target row lock).
        # It was reachable only by hand-building an UpsertRequest, so the
        # convenience method could not express a compare-and-swap at all — the
        # operation acknowledgement and workflow state transitions need. An unset
        # or empty value behaves exactly as before.
        req = request or types_pb2.UpsertRequest(
            message_type=message_type,
            record_json=to_record_json(record or {}),
            payload=optional_struct(payload),
            conflict_fields=list(conflict_fields),
            return_record=return_record,
            idempotency_key=idempotency_key,
            expected=to_struct(expected),
        )
        return await self.call("Upsert", req, metadata=metadata, timeout=timeout)

    async def delete(
        self,
        request: types_pb2.DeleteRequest | None = None,
        *,
        message_type: str = "",
        filter: JsonMapping | Struct | None = None,
        idempotency_key: str = "",
        expected: JsonMapping | Struct | None = None,
        require_affected: int = 0,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        req = request or types_pb2.DeleteRequest(
            message_type=message_type,
            filter=to_struct(filter),
            idempotency_key=idempotency_key,
            require_affected=require_affected,
        )
        if request is None and expected:
            req.expected.CopyFrom(to_struct(expected))
        return await self.call("Delete", req, metadata=metadata, timeout=timeout)

    async def insert(
        self,
        request: types_pb2.UpsertRequest | None = None,
        *,
        message_type: str = "",
        record: JsonMapping | bytes | str | None = None,
        payload: JsonMapping | Struct | None = None,
        return_record: bool = False,
        idempotency_key: str = "",
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        """See :meth:`UdbClient.insert`."""
        req = request or _insert_request(message_type, record, payload, return_record, idempotency_key)
        return await self.call("Insert", req, metadata=metadata, timeout=timeout)

    async def update(
        self,
        request: types_pb2.UpdateRequest | None = None,
        *,
        message_type: str = "",
        filter: JsonMapping | Struct | None = None,
        changes: JsonMapping | Struct | None = None,
        increments: Mapping[str, float] | None = None,
        expected: JsonMapping | Struct | None = None,
        idempotency_key: str = "",
        return_record: bool = False,
        expected_revision: str = "",
        require_affected: int = 0,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        """See :meth:`UdbClient.update`."""
        req = request or _update_request(
            message_type, filter, changes, increments, expected,
            idempotency_key, return_record, expected_revision, require_affected,
        )
        return await self.call("Update", req, metadata=metadata, timeout=timeout)

    def entity(self, message_type: str, *, key: Sequence[str] = ()) -> _AsyncBoundEntity:
        """Bind an entity once, then ``await`` its verbs with plain dicts."""
        return _AsyncBoundEntity(self, message_type, tuple(key))

    def table(self, name: str, *, key: Sequence[str] = ()) -> _AsyncBoundEntity:
        """Table-shaped alias of :meth:`entity`."""
        return self.entity(name, key=key)

    async def tx(
        self,
        fn: Callable[[Any], "Awaitable[Any] | Any"],
        *,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> None:
        """See :meth:`UdbClient.tx`; ``fn`` may be sync or async."""
        from .tx import run_tx_async

        await run_tx_async(self, fn, metadata=metadata, timeout=timeout)

    async def batch_select(
        self,
        requests: Iterable[types_pb2.SelectRequest],
        *,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> AsyncIterator[types_pb2.RecordSet]:
        try:
            stream = self._stub.BatchSelect(
                (self._with_context(req, metadata) for req in requests),
                metadata=self._call_metadata(metadata),
                timeout=self._timeout if timeout is None else timeout,
            )
            async for item in stream:
                yield item
        except grpc.RpcError as error:
            raise map_rpc_error("BatchSelect", error) from error

    async def vector_upsert(
        self,
        collection: str,
        points: Sequence[types_pb2.VectorPointMutation],
        *,
        idempotency_key: str = "",
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        req = types_pb2.VectorUpsertRequest(
            collection=collection,
            points=list(points),
            idempotency_key=idempotency_key,
        )
        return await self.call("VectorUpsert", req, metadata=metadata, timeout=timeout)

    async def vector_search(
        self,
        collection: str,
        vector: Sequence[float],
        *,
        filter: JsonMapping | Struct | None = None,
        limit: int = 10,
        score_threshold: float = 0.0,
        with_payload: bool = True,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.VectorSet:
        req = types_pb2.VectorSearchRequest(
            collection=collection,
            vector=list(vector),
            filter=to_struct(filter),
            limit=limit,
            score_threshold=score_threshold,
            with_payload=with_payload,
        )
        return await self.call("VectorSearch", req, metadata=metadata, timeout=timeout)

    async def vector_hybrid_search(
        self,
        collection: str,
        vector: Sequence[float],
        text_query: str,
        *,
        filter: JsonMapping | Struct | None = None,
        limit: int = 10,
        fusion_weights: Sequence[float] = (),
        with_payload: bool = True,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.VectorSet:
        req = types_pb2.VectorHybridSearchRequest(
            collection=collection,
            vector=list(vector),
            text_query=text_query,
            filter=to_struct(filter),
            limit=limit,
            fusion_weights=list(fusion_weights),
            with_payload=with_payload,
        )
        return await self.call("VectorHybridSearch", req, metadata=metadata, timeout=timeout)

    async def put_object(
        self,
        bucket: str,
        object_key: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        chunk_size: int = 1024 * 1024,
        idempotency_key: str = "",
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        context = self._effective_metadata(metadata).to_request_context()

        async def chunks() -> AsyncIterator[types_pb2.Chunk]:
            for offset in range(0, len(data), chunk_size):
                end = offset + chunk_size
                yield types_pb2.Chunk(
                    context=context,
                    bucket=bucket,
                    object_key=object_key,
                    data=data[offset:end],
                    final_chunk=end >= len(data),
                    content_type=content_type,
                    idempotency_key=idempotency_key,
                )
            if not data:
                yield types_pb2.Chunk(
                    context=context,
                    bucket=bucket,
                    object_key=object_key,
                    final_chunk=True,
                    content_type=content_type,
                    idempotency_key=idempotency_key,
                )

        try:
            return await self._stub.PutObject(
                chunks(),
                metadata=self._call_metadata(metadata),
                timeout=self._timeout if timeout is None else timeout,
            )
        except grpc.RpcError as error:
            raise map_rpc_error("PutObject", error) from error

    async def get_object(
        self,
        bucket: str,
        object_key: str,
        *,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> bytes:
        req = types_pb2.ObjectRequest(bucket=bucket, object_key=object_key)
        try:
            chunks = self._stub.GetObject(
                self._with_context(req, metadata),
                metadata=self._call_metadata(metadata),
                timeout=self._timeout if timeout is None else timeout,
            )
            data = bytearray()
            async for chunk in chunks:
                data.extend(chunk.data)
            return bytes(data)
        except grpc.RpcError as error:
            raise map_rpc_error("GetObject", error) from error

    async def generate_presigned_url(
        self,
        bucket: str,
        object_key: str,
        *,
        method: str = "GET",
        ttl_seconds: int = 900,
        content_type: str = "",
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.UrlResponse:
        req = types_pb2.UrlRequest(
            bucket=bucket,
            object_key=object_key,
            method=method,
            ttl_seconds=ttl_seconds,
            content_type=content_type,
        )
        return await self.call("GeneratePresignedUrl", req, metadata=metadata, timeout=timeout)

    async def enqueue_outbox_event(
        self,
        *,
        topic: str,
        partition_key: str,
        payload: JsonMapping | Struct,
        schema_uri: str = "",
        idempotency_key: str = "",
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.EnqueueOutboxEventResponse:
        req = types_pb2.EnqueueOutboxEventRequest(
            topic=topic,
            partition_key=partition_key,
            payload=optional_struct(payload),
            schema_uri=schema_uri,
            idempotency_key=idempotency_key,
        )
        return await self.call("EnqueueOutboxEvent", req, metadata=metadata, timeout=timeout)

    async def get_capabilities(
        self,
        *,
        project_id: str = "",
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.CapabilitiesResponse:
        req = types_pb2.CapabilitiesRequest(project_id=project_id)
        return await self.call("GetCapabilities", req, metadata=metadata, timeout=timeout)

    async def warmup(
        self, *, metadata: Metadata | None = None
    ) -> types_pb2.CapabilitiesResponse:
        return await self.get_capabilities(metadata=metadata)
