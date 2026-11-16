"""The fake broker's ``DataBroker.BulkUpsert``: one batch of rows in one transaction.

Mirrors the broker: every row is checked and scoped as ``Upsert`` checks it, every row
carries the same columns as the first, no two rows share a conflict key, and the batch
is written whole or not at all. An entity that declares a ``cdc_topic`` gets ONE change
event for the batch, not one per row, and only when a row was written.
"""

from __future__ import annotations

import copy
import hashlib
import json
from typing import TYPE_CHECKING, Any, Mapping, Sequence
import unicodedata
import uuid

import grpc

from udb.entity.v1 import error_pb2, types_pb2

from ..schema import table_options
from ..tx import EVENT_ENVELOPE_VERSION
from .fake import (
    Event,
    _column_refusal,
    _decode_object,
    _Entity,
    _invalid,
    _json_text,
    _refuse,
    _violation,
)

if TYPE_CHECKING:
    from .fake import Fake, Identity, _Call

#: The most rows the broker takes in one batch.
MAX_BULK_ROWS = 10000
_MAX_BATCH_ID = 128
_TOKEN_SEPARATOR = "\x1f"


def _has_blank(text: str) -> bool:
    return any(char.isspace() or unicodedata.category(char) == "Cc" for char in text)


def _batch_id(requested: str) -> str:
    if not requested:
        return str(uuid.uuid4())
    if len(requested) > _MAX_BATCH_ID or _has_blank(requested):
        raise _violation(
            grpc.StatusCode.INVALID_ARGUMENT, "",
            f"batch_id must be at most {_MAX_BATCH_ID} characters with no whitespace or control characters",
            error_pb2.ERROR_KIND_VALIDATION, "batch_id", "at most 128 characters, no whitespace or control characters",
        )
    return requested


def _token(value: Any) -> str:
    """One key column's value as the broker renders it into a key tuple."""
    if value is None:
        return "z:"
    if isinstance(value, bool):
        return "b:true" if value else "b:false"
    if isinstance(value, str):
        return f"s:{value}"
    if isinstance(value, int):
        return f"i:{value}"
    if isinstance(value, float):
        if value.is_integer():
            return f"i:{int(value)}"
        return f"f:{value!r}"
    return "j:" + json.dumps(value, separators=(",", ":"), sort_keys=True)


def _conflict_columns(entity: _Entity, fields: Sequence[str]) -> list[str]:
    entity.conflict_fields(list(fields))
    if fields:
        return [entity.aliases[name.strip().lower()] for name in fields]
    if not entity.primary_key:
        raise _refuse(grpc.StatusCode.FAILED_PRECONDITION, "", f"{entity.message_type} declares no primary key")
    return list(entity.primary_key)


def _resolved(entity: _Entity, raw: Mapping[str, Any]) -> dict[str, str]:
    """Each sent key mapped to the column it names (an unknown name maps to itself,
    and normalize refuses it)."""
    return {key: entity.aliases.get(key.lower(), key) for key in raw}


def _prepared_rows(
    entity: _Entity, identity: "Identity", request: types_pb2.BulkUpsertRequest, columns: Sequence[str]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    """The rows to write (as sent, with any defaulted key column filled in), their
    normalized and scoped form (the idempotency inputs) and their key tuples."""
    raws: list[dict[str, Any]] = []
    for index, raw in enumerate(request.records_json):
        try:
            raws.append(_decode_object(raw))
        except ValueError as error:
            raise _invalid(f"row {index}: {error}") from error
    shape = set(_resolved(entity, raws[0]).values())
    for index, raw in enumerate(raws[1:], start=1):
        if set(_resolved(entity, raw).values()) != shape:
            raise _violation(
                grpc.StatusCode.INVALID_ARGUMENT, "UDB_BULK_ROW_SHAPE_MISMATCH",
                f"row {index} carries a different set of fields than row 0; every row of a batch must carry the same fields",
                error_pb2.ERROR_KIND_VALIDATION, "records_json", f"row {index} differs in shape from row 0",
            )
    writes: list[dict[str, Any]] = []
    inputs: list[dict[str, Any]] = []
    keys: list[str] = []
    seen: dict[str, int] = {}
    for index, raw in enumerate(raws):
        record = entity.normalize(raw)
        entity.bind_record_scope(record, identity)
        inputs.append(copy.deepcopy(record))
        write = dict(raw)
        names = _resolved(entity, raw)
        sent = {column: key for key, column in names.items()}
        defaulted = dict(record)
        entity.defaults(defaulted)
        tokens = []
        for column in columns:
            if column in sent and column in record:
                value = raw[sent[column]]
            elif column in defaulted:
                # The key's column default, generated once so the row is written with
                # the key this response reports.
                value = defaulted[column]
                write[sent.get(column, column)] = value
            else:
                value = None
            if column in entity.primary_key and (value is None or value == ""):
                raise _column_refusal(column)
            tokens.append(_token(value))
        key = _TOKEN_SEPARATOR.join(tokens)
        if key in seen:
            raise _violation(
                grpc.StatusCode.INVALID_ARGUMENT, "UDB_BULK_DUPLICATE_KEY",
                f"rows {seen[key]} and {index} share a conflict key; a batch may write each key once",
                error_pb2.ERROR_KIND_VALIDATION, "records_json", f"row {index} repeats the key of row {seen[key]}",
            )
        seen[key] = index
        writes.append(write)
        keys.append(key)
    return writes, inputs, keys


def bulk_upsert(fake: "Fake", call: "_Call", request: types_pb2.BulkUpsertRequest) -> types_pb2.BulkUpsertResponse:
    identity = call.identity
    key = request.idempotency_key
    if key and _has_blank(key):
        raise _violation(
            grpc.StatusCode.INVALID_ARGUMENT, "", "idempotency_key must contain no whitespace or control characters",
            error_pb2.ERROR_KIND_VALIDATION, "idempotency_key", "must contain no whitespace or control characters",
        )
    batch_id = _batch_id(request.batch_id)
    count = len(request.records_json)
    if count == 0:
        raise _invalid("a bulk upsert needs at least one row")
    if count > MAX_BULK_ROWS:
        raise _violation(
            grpc.StatusCode.INVALID_ARGUMENT, "UDB_BULK_BATCH_TOO_LARGE",
            f"the batch has {count} rows; at most {MAX_BULK_ROWS} are accepted in one batch",
            error_pb2.ERROR_KIND_VALIDATION, "records_json", f"at most {MAX_BULK_ROWS} rows",
        )
    with fake._lock:
        entity = fake._entity(request.message_type)
        columns = _conflict_columns(entity, request.conflict_fields)
        writes, inputs, keys = _prepared_rows(entity, identity, request, columns)
        scope = (identity.tenant_id, identity.project_id, request.message_type, "bulk_upsert", key)
        digest = hashlib.sha256(
            _json_text({"rows": inputs, "conflict_fields": list(request.conflict_fields), "batch_id": request.batch_id}).encode("utf-8")
        ).hexdigest()
        if key:
            receipt = fake._receipts.get(scope)
            if receipt is not None:
                if receipt[0] != digest:
                    raise _violation(
                        grpc.StatusCode.FAILED_PRECONDITION, "UDB_IDEMPOTENCY_REUSE",
                        "idempotency_key was already used for a different batch",
                        error_pb2.ERROR_KIND_CONFLICT, "idempotency_key", "already used with different inputs",
                        "Reuse a key only to retry an identical batch; use a new key for a different batch.",
                    )
                replay = types_pb2.BulkUpsertResponse()
                replay.CopyFrom(receipt[1])
                replay.was_duplicate = True
                return replay
        rows = copy.deepcopy(fake._rows)
        revisions = dict(fake._revisions)
        receipts = dict(fake._receipts)
        events = len(fake._events)
        emitted = fake._emitted
        try:
            response = _write_batch(fake, identity, entity, request, batch_id, writes, keys, columns)
            if key:
                kept = types_pb2.BulkUpsertResponse()
                kept.CopyFrom(response)
                fake._receipts[scope] = (digest, kept)
        except Exception:
            fake._rows, fake._revisions, fake._receipts = rows, revisions, receipts
            del fake._events[events:]
            fake._emitted = emitted
            raise
        return response


def _write_batch(
    fake: "Fake",
    identity: "Identity",
    entity: _Entity,
    request: types_pb2.BulkUpsertRequest,
    batch_id: str,
    writes: Sequence[Mapping[str, Any]],
    keys: Sequence[str],
    columns: Sequence[str],
) -> types_pb2.BulkUpsertResponse:
    affected = 0
    for write in writes:
        upsert = types_pb2.UpsertRequest(
            message_type=request.message_type,
            record_json=json.dumps(write, separators=(",", ":")).encode("utf-8"),
            conflict_fields=list(request.conflict_fields),
        )
        affected += fake._write(identity, upsert, False).affected_rows
    response = types_pb2.BulkUpsertResponse(
        batch_id=batch_id,
        row_count=len(writes),
        affected_rows=affected,
        keys_sha256=hashlib.sha256("\n".join(keys).encode("utf-8")).hexdigest(),
        first_key=keys[0],
        last_key=keys[-1],
        statement_count=1,
    )
    table = table_options(entity.message_class.DESCRIPTOR)
    topic = (table.cdc_topic if table is not None else "").strip()
    if topic and affected > 0:
        fake._emitted += 1
        event_id = f"0190f1b2-0000-4000-8000-{fake._emitted:012x}"
        fake._events.append(
            Event(
                topic,
                batch_id,
                {
                    "event_id": event_id, "event_type": topic, "topic": topic,
                    "tenant_id": identity.tenant_id, "project_id": identity.project_id,
                    "operation": "bulk_upsert", "message_type": request.message_type,
                    "document_id": batch_id, "correlation_id": event_id,
                    "envelope_version": EVENT_ENVELOPE_VERSION,
                    "payload": {
                        "batch_id": batch_id, "message_type": request.message_type,
                        "row_count": len(writes), "affected_rows": affected,
                        "keys_sha256": response.keys_sha256,
                        "first_key": response.first_key, "last_key": response.last_key,
                        "conflict_columns": list(columns),
                    },
                },
            )
        )
        response.event_id = event_id
    return response
