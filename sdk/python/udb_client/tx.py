"""Transactions with typed writes and outbox events.

``project.tx(fn)`` collects writes and events, then commits them in one broker
transaction: every write and every event commits together or none does.
:meth:`TxScope.emit` builds the outbox envelope from the event message's own contract
(topic, partition key), so a producer never assembles envelope fields by hand.
"""

from __future__ import annotations

from inspect import isawaitable
from typing import Any, AsyncIterator, Callable, Iterator, Mapping
import hashlib
import uuid

import grpc
from google.protobuf import json_format
from google.protobuf.message import Message

from udb.entity.v1 import types_pb2

from .client import _json_safe, to_record_json, to_struct
from .error_code import classify, map_rpc_error
from .exceptions import UdbError
from .metadata import Metadata, WriteReceipt
from .record_codec import WRITE_INSERT, WRITE_UPDATE, encode_record
from .schema import event_type, message_type, partition_key_field, primary_keys, topic

#: The outbox envelope version this SDK writes and reads. Consumers refuse a newer one.
EVENT_ENVELOPE_VERSION = 2

_RPC_NAME = "BeginTx"
_STATUS_BY_NUMBER = {status.value[0]: status for status in grpc.StatusCode}


class _FrameError(grpc.RpcError):
    """A refusal delivered in a transaction status frame, shaped like a call error."""

    def __init__(self, frame: Any):
        super().__init__()
        code = _STATUS_BY_NUMBER.get(frame.code, grpc.StatusCode.UNKNOWN)
        self._code = grpc.StatusCode.UNKNOWN if code == grpc.StatusCode.OK else code
        self._details = frame.message
        self._detail = frame.error_detail.SerializeToString()

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details

    def trailing_metadata(self) -> tuple[tuple[str, bytes], ...]:
        return (("udb-error-detail-bin", self._detail),) if self._detail else ()


def _entity_name(entity: Any) -> str:
    if isinstance(entity, str):
        return entity
    table_type = getattr(entity, "message_type", None)
    if isinstance(table_type, str):
        return table_type
    return message_type(entity)


class TxScope:
    """Collects the writes of one transaction. Use it only inside the function passed
    to ``tx``. Nothing reaches the broker until that function returns.

    Writes take an entity message, or a typed table plus a dict row:
    ``tx.upsert(File(...))`` or ``tx.upsert(files_table, {"file_id": ...})``.
    """

    def __init__(self, correlation_id: str, replay_key: str = ""):
        self._replay_key = replay_key
        self._tx_id = str(uuid.uuid4())
        self._correlation_id = correlation_id or self._tx_id
        self._mutations: list[types_pb2.Mutation] = []
        self._error: Exception | None = None

    def _add(self, mutation: types_pb2.Mutation) -> None:
        # A replay key on each write of a keyed transaction, so a retry repeats none.
        if self._replay_key and mutation.operation in ("upsert", "update", "delete"):
            mutation.idempotency_key = f"{self._replay_key}:{len(self._mutations)}"
        self._mutations.append(mutation)

    def _event_id(self) -> str:
        """Random, or stable across retries of a keyed transaction."""
        if not self._replay_key:
            return str(uuid.uuid4())
        digest = hashlib.sha256(f"{self._replay_key}:{len(self._mutations)}".encode()).digest()
        return str(uuid.UUID(bytes=digest[:16], version=5))

    def _fail(self, error: Exception) -> Exception:
        # Remembered, so a function that catches the error still commits nothing.
        if self._error is None:
            self._error = error
        return error

    def upsert(self, entity: Any, row: Mapping[str, Any] | None = None) -> None:
        """Write the whole row in the transaction."""
        self._write(entity, row, "upsert")

    def insert(self, entity: Any, row: Mapping[str, Any] | None = None) -> None:
        """Create a row when its primary and unique keys are new. A duplicate fails
        with a unique error and rolls back every write and event in the transaction."""
        self._write(entity, row, "insert")

    def _write(self, entity: Any, row: Mapping[str, Any] | None, operation: str) -> None:
        try:
            if isinstance(entity, Message):
                record = encode_record(entity, WRITE_INSERT)
            else:
                record = entity._spec.to_record(row)
            raw = to_record_json(record)
        except (ValueError, TypeError, AttributeError) as error:
            raise self._fail(error) from error
        self._add(
            types_pb2.Mutation(operation=operation, message_type=_entity_name(entity), record_json=raw)
        )

    def patch(self, entity: Any, key: Mapping[str, Any], fields: Mapping[str, Any]) -> None:
        """Set the given columns on the row with ``key``. ``entity`` names the entity:
        a message, a message class, a typed table or a message type."""
        self._update(_entity_name(entity), key, fields, None)

    def update_if(self, next_row: Any, expected: Mapping[str, Any], row: Mapping[str, Any] | None = None) -> None:
        """Write the updatable columns of a message if the stored row still holds
        ``expected``; otherwise the whole transaction fails with ``UdbConflict``. To
        use a typed table, pass it first and the dict row as ``row``."""
        try:
            if not expected:
                raise ValueError("udb: guarded write requires a non-empty expected precondition")
            if isinstance(next_row, Message):
                record = encode_record(next_row, WRITE_INSERT)
                key = {column: record.get(column) for column in primary_keys(next_row)}
                if not key:
                    raise ValueError(f"udb: {message_type(next_row)} declares no primary key")
                for column, value in key.items():
                    if value is None or value == "":
                        raise ValueError(
                            f"udb: {message_type(next_row)} has no value for primary-key column {column}"
                        )
                changes = encode_record(next_row, WRITE_UPDATE)
                name = message_type(next_row)
            else:
                key = next_row._fields_of(next_row.key_of(row))
                changes = next_row._updatable(next_row._fields_of(row))
                name = next_row.message_type
        except (ValueError, TypeError, AttributeError) as error:
            raise self._fail(error) from error
        self._update(name, key, changes, expected)

    def delete(self, entity: Any, key: Mapping[str, Any]) -> None:
        """Remove the row with ``key`` in the transaction."""
        try:
            filter_ = to_struct(key)
        except (ValueError, TypeError) as error:
            raise self._fail(error) from error
        self._add(
            types_pb2.Mutation(
                operation="delete",
                message_type=_entity_name(entity),
                filter=filter_,
                require_affected=1,
            )
        )

    def _update(
        self,
        name: str,
        key: Mapping[str, Any],
        changes: Mapping[str, Any],
        expected: Mapping[str, Any] | None,
    ) -> None:
        try:
            mutation = types_pb2.Mutation(
                operation="update", message_type=name, filter=to_struct(key), require_affected=1
            )
            if changes:
                mutation.changes.CopyFrom(to_struct(changes))
            if expected:
                mutation.expected.CopyFrom(to_struct(expected))
        except (ValueError, TypeError) as error:
            raise self._fail(error) from error
        self._add(mutation)

    def emit(self, event: Message) -> None:
        """Queue ``event`` on the outbox, committed with the transaction. The topic is
        the event's declared outbox topic and the partition key its declared field; the
        event's own fields go under the envelope's payload."""
        name = message_type(event)
        declared_topic = topic(event)
        if not declared_topic:
            raise self._fail(
                ValueError(f"udb: {name} declares no outbox topic (message_event_contract.outbox_topic)")
            )
        key_field = partition_key_field(event)
        if not key_field:
            raise self._fail(
                ValueError(f"udb: {name} declares no partition key field (message_event_contract.partition_key_field)")
            )
        payload = json_format.MessageToDict(event, preserving_proto_field_name=True)
        document = payload.get(key_field)
        if not isinstance(document, str) or not document:
            raise self._fail(ValueError(f"udb: event {name} has no value in its partition key field {key_field}"))
        envelope = to_struct(
            _json_safe(
                {
                    "event_id": self._event_id(),
                    "event_type": event_type(event),
                    "correlation_id": self._correlation_id,
                    "document_id": document,
                    "envelope_version": EVENT_ENVELOPE_VERSION,
                    "payload": payload,
                }
            )
        )
        self._mutations.append(
            types_pb2.Mutation(
                operation="enqueue_outbox_event",
                message_type=name,
                collection=declared_topic,
                object_key=document,
                payload=envelope,
            )
        )

    def _frames(self) -> Iterator[types_pb2.Mutation]:
        for mutation in self._mutations:
            mutation.tx_id = self._tx_id
            yield mutation
        yield types_pb2.Mutation(tx_id=self._tx_id, commit=True)


class _Outcome:
    """What a stream of transaction status frames said."""

    def __init__(self, client: Any):
        self._client = client
        self.committed = False
        self.refused: grpc.RpcError | None = None

    def read(self, frame: Any) -> None:
        if frame.state == types_pb2.TxStatus.TX_STATE_COMMITTED:
            self.committed = True
            if frame.HasField("write_receipt"):
                receipt = frame.write_receipt
                self._client.remember_receipt(
                    WriteReceipt(
                        source_lsn=receipt.source_lsn,
                        outbox_seq=receipt.outbox_seq,
                        projection_task_ids=tuple(receipt.projection_task_ids),
                        manifest_checksum=receipt.manifest_checksum,
                        written_at_unix_ms=receipt.written_at_unix_ms,
                    )
                )
        elif frame.state in (types_pb2.TxStatus.TX_STATE_ERROR, types_pb2.TxStatus.TX_STATE_ROLLED_BACK):
            self.refused = _FrameError(frame)

    def finish(self) -> None:
        if self.refused is not None:
            _raise_refusal(self.refused)
        if not self.committed:
            raise UdbError("udb: transaction ended without a COMMITTED status")


def _raise_refusal(error: grpc.RpcError) -> None:
    mapped = map_rpc_error(_RPC_NAME, error)
    classified = classify(mapped)
    if classified is mapped:
        raise mapped from error
    raise classified from mapped


def _scope_for(client: Any, metadata: Metadata | None, replay_key: str = "") -> TxScope:
    return TxScope(client._effective_metadata(metadata).correlation_id, replay_key)


def run_tx(
    client: Any,
    fn: Callable[[TxScope], Any],
    *,
    metadata: Metadata | None = None,
    timeout: float | None = None,
    replay_key: str = "",
) -> None:
    """Run ``fn`` and commit what it collected. An exception from ``fn`` commits
    nothing. A conditional write that lost a race fails the transaction with
    ``UdbConflict``. A transaction is never retried automatically. ``replay_key`` makes
    a retry safe after a lost response: each write gets a key derived from it, and each
    event a stable id. ``fn`` must make the same writes on a retry, and ``insert`` has no
    replay form, so use ``upsert``."""
    scope = _scope_for(client, metadata, replay_key)
    fn(scope)
    if scope._error is not None:
        raise scope._error
    if not scope._mutations:
        return
    outcome = _Outcome(client)
    try:
        stream = client.stub.BeginTx(
            scope._frames(),
            metadata=client._call_metadata(metadata),
            timeout=client._timeout if timeout is None else timeout,
        )
        for frame in stream:
            outcome.read(frame)
    except grpc.RpcError as error:
        # Once the broker said it committed, a broken stream loses nothing.
        if outcome.committed:
            return
        _raise_refusal(error)
    outcome.finish()


async def _async_frames(scope: TxScope) -> AsyncIterator[types_pb2.Mutation]:
    for frame in scope._frames():
        yield frame


async def run_tx_async(
    client: Any,
    fn: Callable[[TxScope], Any],
    *,
    metadata: Metadata | None = None,
    timeout: float | None = None,
    replay_key: str = "",
) -> None:
    """See :func:`run_tx`; ``fn`` may be sync or async."""
    scope = _scope_for(client, metadata, replay_key)
    result = fn(scope)
    if isawaitable(result):
        await result
    if scope._error is not None:
        raise scope._error
    if not scope._mutations:
        return
    outcome = _Outcome(client)
    try:
        stream = client.stub.BeginTx(
            _async_frames(scope),
            metadata=client._call_metadata(metadata),
            timeout=client._timeout if timeout is None else timeout,
        )
        async for frame in stream:
            outcome.read(frame)
    except grpc.RpcError as error:
        if outcome.committed:
            return
        _raise_refusal(error)
    outcome.finish()
