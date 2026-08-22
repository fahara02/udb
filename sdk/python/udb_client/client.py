from __future__ import annotations

import json
from typing import Any, AsyncIterator, Iterable, Iterator, Mapping, Sequence

import grpc
from google.protobuf.struct_pb2 import Struct

from udb.entity.v1 import types_pb2
from udb.services.v1 import data_broker_pb2_grpc

from ._channel import merge_channel_options
from .exceptions import UdbConfigurationError, UdbRpcError
from .metadata import Metadata, WriteReceipt

JsonMapping = Mapping[str, Any]


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


def to_struct(value: JsonMapping | Struct | None) -> Struct:
    if isinstance(value, Struct):
        return value
    struct = Struct()
    if value:
        struct.update(dict(value))
    return struct


def to_record_json(record: JsonMapping | bytes | str) -> bytes:
    if isinstance(record, bytes):
        return record
    if isinstance(record, str):
        return record.encode("utf-8")
    return json.dumps(record, separators=(",", ":"), sort_keys=True).encode("utf-8")


def decode_records(record_set: types_pb2.RecordSet) -> list[dict[str, Any]]:
    return [json.loads(raw.decode("utf-8")) for raw in record_set.records_json]


class UdbClient:
    """Synchronous UDB DataBroker client with metadata injection."""

    def __init__(
        self,
        target: str,
        metadata: Metadata | None = None,
        *,
        secure: bool = False,
        root_certificates: bytes | None = None,
        channel_options: Sequence[tuple[str, Any]] | None = None,
        timeout: float | None = 30.0,
        channel: grpc.Channel | None = None,
    ):
        if not target and channel is None:
            raise UdbConfigurationError("UDB target is required, e.g. '127.0.0.1:50051'")
        self._metadata = metadata
        self._timeout = timeout
        self._owns_channel = channel is None
        if channel is None:
            options = merge_channel_options(channel_options)
            if secure:
                credentials = grpc.ssl_channel_credentials(root_certificates)
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
        try:
            return method(
                request,
                metadata=self._call_metadata(metadata),
                timeout=self._timeout if timeout is None else timeout,
            )
        except grpc.RpcError as error:
            raise UdbRpcError(rpc_name, error) from error

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
            payload=to_struct(payload),
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
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        req = request or types_pb2.DeleteRequest(
            message_type=message_type,
            filter=to_struct(filter),
            idempotency_key=idempotency_key,
        )
        return self.call("Delete", req, metadata=metadata, timeout=timeout)

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

    def batch_select(
        self,
        requests: Iterable[types_pb2.SelectRequest],
        *,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> Iterator[types_pb2.RecordSet]:
        try:
            return self._stub.BatchSelect(
                (self._with_context(req, metadata) for req in requests),
                metadata=self._call_metadata(metadata),
                timeout=self._timeout if timeout is None else timeout,
            )
        except grpc.RpcError as error:
            raise UdbRpcError("BatchSelect", error) from error

    def batch_upsert(
        self,
        requests: Iterable[types_pb2.UpsertRequest],
        *,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> Iterator[types_pb2.MutationResponse]:
        try:
            return self._stub.BatchUpsert(
                (self._with_context(req, metadata) for req in requests),
                metadata=self._call_metadata(metadata),
                timeout=self._timeout if timeout is None else timeout,
            )
        except grpc.RpcError as error:
            raise UdbRpcError("BatchUpsert", error) from error

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
            raise UdbRpcError("PutObject", error) from error

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
            raise UdbRpcError("GetObject", error) from error

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
            payload=to_struct(payload),
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
            cloned.context.CopyFrom(self._effective_metadata(metadata).to_request_context())
        return cloned


class _BoundEntity:
    """A message-type/table bound to a :class:`UdbClient`, taking plain dicts.

    Returned by :meth:`UdbClient.entity` / :meth:`UdbClient.table`. Its
    ``select``/``upsert``/``delete`` build the proto requests from dicts using the
    same :func:`to_record_json`/:func:`to_struct`/:func:`decode_records` helpers
    the raw client uses, and set ``conflict_fields`` from the bound key. The bind
    happens once; no per-request catalog lookup.
    """

    def __init__(self, client: "UdbClient", message_type: str, key: Sequence[str]):
        self._client = client
        self._message_type = message_type
        self._key = tuple(key)

    def select(
        self,
        *,
        where: JsonMapping | Struct | None = None,
        fields: Sequence[str] = (),
        limit: int = 0,
        page_token: str = "",
        sort: Sequence[types_pb2.Sort] = (),
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> list[dict[str, Any]]:
        """One ``Select``; returns decoded records (no hidden List)."""
        record_set = self._client.select(
            message_type=self._message_type,
            filter=where,
            fields=fields,
            limit=limit,
            page_token=page_token,
            sort=sort,
            metadata=metadata,
            timeout=timeout,
        )
        return decode_records(record_set)

    def upsert(
        self,
        record: JsonMapping,
        *,
        return_record: bool = False,
        idempotency_key: str = "",
        expected: JsonMapping | Struct | None = None,
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        """One ``Upsert`` with ``conflict_fields`` from the bound key.

        ``expected`` is passed through as the broker's optimistic CAS
        precondition; a key-bound handle is exactly where a caller reaches for
        compare-and-swap, so dropping it here would leave the capability
        unreachable through the ergonomic surface.
        """
        return self._client.upsert(
            message_type=self._message_type,
            record=record,
            conflict_fields=self._key,
            return_record=return_record,
            idempotency_key=idempotency_key,
            expected=expected,
            metadata=metadata,
            timeout=timeout,
        )

    def delete(
        self,
        *,
        where: JsonMapping | Struct | None = None,
        idempotency_key: str = "",
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        """One ``Delete`` filtered by ``where``."""
        return self._client.delete(
            message_type=self._message_type,
            filter=where,
            idempotency_key=idempotency_key,
            metadata=metadata,
            timeout=timeout,
        )


class UdbAsyncClient:
    """Async UDB DataBroker client with the same convenience surface."""

    def __init__(
        self,
        target: str,
        metadata: Metadata | None = None,
        *,
        secure: bool = False,
        root_certificates: bytes | None = None,
        channel_options: Sequence[tuple[str, Any]] | None = None,
        timeout: float | None = 30.0,
        channel: grpc.aio.Channel | None = None,
    ):
        if not target and channel is None:
            raise UdbConfigurationError("UDB target is required, e.g. '127.0.0.1:50051'")
        self._metadata = metadata
        self._timeout = timeout
        self._owns_channel = channel is None
        if channel is None:
            options = merge_channel_options(channel_options)
            if secure:
                credentials = grpc.ssl_channel_credentials(root_certificates)
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
        try:
            return await method(
                request,
                metadata=self._call_metadata(metadata),
                timeout=self._timeout if timeout is None else timeout,
            )
        except grpc.RpcError as error:
            raise UdbRpcError(rpc_name, error) from error

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
            payload=to_struct(payload),
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
        metadata: Metadata | None = None,
        timeout: float | None = None,
    ) -> types_pb2.MutationResponse:
        req = request or types_pb2.DeleteRequest(
            message_type=message_type,
            filter=to_struct(filter),
            idempotency_key=idempotency_key,
        )
        return await self.call("Delete", req, metadata=metadata, timeout=timeout)

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
            raise UdbRpcError("BatchSelect", error) from error

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
            raise UdbRpcError("PutObject", error) from error

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
            raise UdbRpcError("GetObject", error) from error

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
            payload=to_struct(payload),
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
            cloned.context.CopyFrom(self._effective_metadata(metadata).to_request_context())
        return cloned
