"""``bulk_upsert`` / ``bulk_load`` against the in-memory fake's BulkUpsert: one
transaction and one change event per batch, refusals that write nothing, and a
resumable keyed load."""

from __future__ import annotations

import hashlib
import uuid

import pytest

from udb.core.storage.entity.v1 import file_pb2
from udb_client import BulkLoadError, ErrorCode, UdbRpcError, inspect, to_record_json, udbtest

FILE = "udb.core.storage.entity.v1.File"
TOPIC = "udb.storage.files.cdc"


def _row(file_id: str, name: str = "") -> dict:
    return {"file_id": file_id, "filename": name or f"{file_id}.pdf", "object_key": f"bulk/{file_id}"}


def _bulk_events(fake: udbtest.Fake) -> list[udbtest.Event]:
    return [
        event for event in fake.events()
        if event.topic == TOPIC and event.envelope.get("operation") == "bulk_upsert"
    ]


def test_bulk_load_sends_one_transaction_and_one_event_per_batch() -> None:
    tenant = str(uuid.uuid4())
    ids = [str(uuid.uuid4()) for _ in range(2500)]
    seen: list[int] = []
    with udbtest.new(file_pb2.File) as fake:
        data = fake.client(tenant).data
        result = data.bulk_load(
            FILE, (_row(file_id) for file_id in ids), batch_rows=1000,
            batch_id_prefix="load", idempotency_key_prefix="load-key",
            on_batch=lambda index, _: seen.append(index),
        )
        assert (result.batches, result.rows, result.affected_rows) == (3, 2500, 2500)
        assert seen == [0, 1, 2]
        assert [r.batch_id for r in result.responses] == ["load/000000", "load/000001", "load/000002"]
        assert all(r.statement_count == 1 and not r.was_duplicate for r in result.responses)
        stored = fake.rows(FILE, tenant)
        assert sorted(row["file_id"] for row in stored) == sorted(ids)

        first = result.responses[0]
        tuples = [f"s:{file_id}" for file_id in ids[:1000]]
        assert first.keys_sha256 == hashlib.sha256("\n".join(tuples).encode("utf-8")).hexdigest()
        assert (first.first_key, first.last_key) == (tuples[0], tuples[-1])

        events = _bulk_events(fake)
        assert len(events) == 3, "one event per batch, not per row"
        assert [e.envelope["payload"]["row_count"] for e in events] == [1000, 1000, 500]
        for event, response in zip(events, result.responses):
            envelope = event.envelope
            assert event.partition_key == envelope["document_id"] == response.batch_id
            assert envelope["event_id"] == response.event_id
            assert envelope["tenant_id"] == tenant and envelope["message_type"] == FILE
            assert envelope["payload"]["keys_sha256"] == response.keys_sha256
            assert envelope["payload"]["conflict_columns"] == ["file_id"]

        # The same keyed load again replays every batch: nothing is written twice.
        again = data.bulk_load(
            FILE, (_row(file_id) for file_id in ids), batch_rows=1000,
            batch_id_prefix="load", idempotency_key_prefix="load-key",
        )
        assert again.batches == 3 and all(r.was_duplicate for r in again.responses)
        assert len(_bulk_events(fake)) == 3
        assert len(fake.rows(FILE, tenant)) == 2500


def test_a_duplicate_key_batch_writes_nothing_and_emits_nothing() -> None:
    tenant = str(uuid.uuid4())
    file_id = str(uuid.uuid4())
    with udbtest.new(file_pb2.File) as fake:
        data = fake.client(tenant).data
        with pytest.raises(UdbRpcError) as refused:
            data.bulk_upsert(FILE, [_row(str(uuid.uuid4())), _row(file_id, "a.pdf"), _row(file_id, "b.pdf")])
        info = inspect(refused.value)
        assert info.code is ErrorCode.INVALID and info.reason == "UDB_BULK_DUPLICATE_KEY"
        assert fake.rows(FILE, tenant) == [] and _bulk_events(fake) == []


def test_a_row_of_another_shape_is_refused() -> None:
    tenant = str(uuid.uuid4())
    with udbtest.new(file_pb2.File) as fake:
        data = fake.client(tenant).data
        short = {"file_id": str(uuid.uuid4()), "filename": "short.pdf"}
        with pytest.raises(UdbRpcError) as refused:
            data.bulk_upsert(FILE, [_row(str(uuid.uuid4())), short])
        assert inspect(refused.value).reason == "UDB_BULK_ROW_SHAPE_MISMATCH"
        assert "row 1" in str(refused.value)
        assert fake.rows(FILE, tenant) == [] and _bulk_events(fake) == []


def test_a_failing_row_rolls_the_whole_batch_back() -> None:
    tenant = str(uuid.uuid4())
    taken = str(uuid.uuid4())
    with udbtest.new(file_pb2.File) as fake:
        data = fake.client(tenant).data
        data.upsert(message_type=FILE, record=_row(taken))
        clash = _row(str(uuid.uuid4()))
        clash["object_key"] = f"bulk/{taken}"  # object_key is unique
        with pytest.raises(UdbRpcError):
            data.bulk_upsert(FILE, [_row(str(uuid.uuid4())), clash])
        assert [row["file_id"] for row in fake.rows(FILE, tenant)] == [taken]
        assert _bulk_events(fake) == []


def test_bulk_load_stops_at_the_first_failure_and_says_what_committed() -> None:
    tenant = str(uuid.uuid4())
    repeated = str(uuid.uuid4())
    rows = [_row(str(uuid.uuid4())) for _ in range(3)] + [_row(repeated, "a.pdf"), _row(repeated, "b.pdf")]
    with udbtest.new(file_pb2.File) as fake:
        data = fake.client(tenant).data
        with pytest.raises(BulkLoadError) as stopped:
            data.bulk_load(FILE, rows, batch_rows=3)
        assert stopped.value.batch == 1 and stopped.value.batches == 1 and stopped.value.rows == 3
        assert "1 batch(es)" in str(stopped.value)
        assert inspect(stopped.value).reason == "UDB_BULK_DUPLICATE_KEY"
        assert len(fake.rows(FILE, tenant)) == 3 and len(_bulk_events(fake)) == 1


def test_bulk_load_closes_a_batch_on_bytes_and_refuses_an_oversized_row() -> None:
    tenant = str(uuid.uuid4())
    rows = [_row(str(uuid.uuid4())) for _ in range(4)]
    one = len(to_record_json(rows[0]))  # every row encodes to the same length
    with udbtest.new(file_pb2.File) as fake:
        data = fake.client(tenant).data
        result = data.bulk_load(FILE, rows, max_batch_bytes=one * 2)
        assert [r.row_count for r in result.responses] == [2, 2]
        with pytest.raises(BulkLoadError) as stopped:
            data.bulk_load(FILE, rows, max_batch_bytes=one - 1)
        assert stopped.value.batches == 0 and isinstance(stopped.value.__cause__, ValueError)
        for bad in ({"batch_rows": 0}, {"batch_rows": 10001}, {"max_batch_bytes": 0}):
            with pytest.raises(ValueError):
                data.bulk_load(FILE, rows, **bad)
        assert data.bulk_load(FILE, []).batches == 0, "no rows, no call"
