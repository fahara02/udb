"""Data-plane parity with the Go SDK, served by the real in-memory broker: retry and
typed errors, error classification, entity handles, read-your-writes, transactions,
durable consumers and the async stores. Sync and async run the same assertions."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from typing import Any
import uuid

import grpc
import pytest

from udb.core.apikey.events.v1 import apikey_events_pb2
from udb.core.storage.entity.v1 import file_pb2
from udb.entity.v1 import types_pb2
from udb_client import (
    AsyncRepo,
    AsyncTable,
    ConsumeOptions,
    ErrorCode,
    Repo,
    Table,
    UdbConflict,
    UdbNotFound,
    UdbRpcError,
    code_of,
    consume,
    consume_async,
    error_reason,
    inspect,
    is_cas_conflict,
    is_conflict,
    is_not_found,
    is_unique,
    missing_scope,
    table_spec_of,
)
from udb_client.generated_client import RetryPolicy, UdbDetailedRpcError
from udb_client.tx import EVENT_ENVELOPE_VERSION
from udb_client import udbtest
from udb_client.udbtest import Fault, Identity, Policy
from udb_client.udbtest.fake import Fake
from udb.core.workflow.entity.v1 import workflow_instance_pb2
from udb_client import UdbRpcError, decode_record, encode_record

WORKFLOW_TYPE = "udb.core.workflow.entity.v1.WorkflowInstance"

FILE = "udb.core.storage.entity.v1.File"
SPEC = table_spec_of(file_pb2.File)
TOPIC = "udb.apikey.created.v1"
FAST_RETRY = RetryPolicy(initial_backoff=0.001, max_backoff=0.002)
FAST_CONSUMER = dict(backoff=0.01, ack_timeout=2.0)


def row(name: str = "a.pdf", file_id: str | None = None) -> dict[str, Any]:
    file_id = file_id or str(uuid.uuid4())
    return {"file_id": file_id, "filename": name, "object_key": f"k/{file_id}"}


def test_the_fake_holds_a_json_column_as_the_broker_does() -> None:
    with udbtest.new(workflow_instance_pb2.WorkflowInstance) as served:
        tenant = str(uuid.uuid4())
        client = served.client(tenant)
        entity = client.entity(WORKFLOW_TYPE, key=["workflow_id"])
        for payload in ('"My"', '"5"', '""', "5", "[1]", '{"a":1}'):
            message = workflow_instance_pb2.WorkflowInstance(
                workflow_id=str(uuid.uuid4()),
                tenant_id=tenant,
                workflow_type="json",
                payload=payload,
            )
            for _ in range(3):
                entity.upsert(encode_record(message))
                rows = entity.select(where={"workflow_id": message.workflow_id})
                back = decode_record(rows[0], workflow_instance_pb2.WorkflowInstance())
                assert back.payload == payload
                message.payload = back.payload
        for text in ("My", "null"):
            with pytest.raises(UdbRpcError):
                entity.upsert({"workflow_id": str(uuid.uuid4()), "workflow_type": "json", "payload": text})


@pytest.fixture
def fake():
    with udbtest.new(file_pb2.File) as served:
        yield served


@pytest.fixture
def tenant() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def project(fake: Fake, tenant: str):
    client = fake.client(tenant)
    client.data._retry = FAST_RETRY
    return client


def run(coroutine_factory):
    """Run an async test body with a client bound to its own event loop."""
    return asyncio.run(coroutine_factory())


async def _async_client(fake: Fake, tenant: str):
    client = fake.async_client(tenant)
    client.data._retry = FAST_RETRY
    return client


# ── gap 1: retry and typed errors ────────────────────────────────────────────


def test_read_retries_transient_failures_then_succeeds(fake: Fake, project, tenant: str) -> None:
    project.entity(FILE, key=["file_id"]).upsert(row())
    fake.inject_fault(Fault(code=grpc.StatusCode.UNAVAILABLE, method="Select", times=3))
    assert len(project.entity(FILE).select(where={"filename": "a.pdf"})) == 1


def test_read_gives_up_after_the_policy_attempts_with_the_typed_error(fake: Fake, project) -> None:
    fake.inject_fault(Fault(code=grpc.StatusCode.UNAVAILABLE, method="Select", times=10, retry_after=0.25))
    with pytest.raises(UdbRpcError) as raised:
        project.entity(FILE).select(where={"filename": "a.pdf"})
    assert isinstance(raised.value, UdbDetailedRpcError)
    assert raised.value.retryable is True and raised.value.error_detail.reason == "UDB_BACKEND_UNAVAILABLE"
    info = inspect(raised.value)
    assert info.code is ErrorCode.UNAVAILABLE and info.retry_after == 0.25
    # 4 attempts used up 4 of the 10; the other 6 prove nothing more was sent.
    assert fake._faults[0].times == 6


def test_mutation_without_a_key_is_never_retried(fake: Fake, project) -> None:
    fake.inject_fault(Fault(code=grpc.StatusCode.UNAVAILABLE, method="Upsert", times=2))
    with pytest.raises(UdbRpcError):
        project.entity(FILE).upsert(row())
    assert fake._faults[0].times == 1, "the mutation was sent again"


def test_replay_safe_mutation_with_a_key_retries_and_applies_once(fake: Fake, project, tenant: str) -> None:
    fake.inject_fault(Fault(code=grpc.StatusCode.UNAVAILABLE, method="Upsert", times=2))
    project.entity(FILE).upsert(row(), idempotency_key=str(uuid.uuid4()))
    assert fake._faults[0].times == 0 and len(fake.rows(FILE, tenant)) == 1


def test_config_retry_governs_the_data_plane(fake: Fake, tenant: str) -> None:
    fake.add_api_key(f"udbtest-key-{tenant}", tenant)
    from udb_client import UdbConfig, UdbProject

    with UdbProject(
        UdbConfig(
            target=fake.target, tenant_id=tenant, project_id="", purpose="test",
            api_key=f"udbtest-key-{tenant}", retry=RetryPolicy(max_attempts=1),
        )
    ) as disabled:
        fake.inject_fault(Fault(code=grpc.StatusCode.UNAVAILABLE, method="Select", times=2))
        with pytest.raises(UdbRpcError):
            disabled.entity(FILE).select()
    assert fake._faults[0].times == 1, "max_attempts=1 must disable retry"


def test_async_read_retries_and_raises_the_typed_error(fake: Fake, tenant: str) -> None:
    async def body() -> None:
        client = await _async_client(fake, tenant)
        try:
            await client.entity(FILE, key=["file_id"]).upsert(row())
            fake.inject_fault(Fault(code=grpc.StatusCode.UNAVAILABLE, method="Select", times=3))
            assert len(await client.entity(FILE).select()) == 1
            assert fake._faults[-1].times == 0
            with pytest.raises(UdbRpcError) as raised:
                await client.entity("no.such.Message").select()
            assert inspect(raised.value).reason == "UDB_UNKNOWN_MESSAGE_TYPE"
            assert isinstance(raised.value, UdbDetailedRpcError)
            fake.inject_fault(Fault(code=grpc.StatusCode.UNAVAILABLE, method="Upsert", times=2))
            with pytest.raises(UdbRpcError):
                await client.entity(FILE).upsert(row())
            assert fake._faults[-1].times == 1, "the keyless mutation was sent again"
        finally:
            await client.close()

    run(body)


# ── gap 4: error classification ──────────────────────────────────────────────


def _refusal(fn) -> Exception:
    with pytest.raises(Exception) as raised:
        fn()
    return raised.value


def test_inspect_reads_reason_constraint_and_column(fake: Fake, project) -> None:
    entity = project.entity(FILE, key=["file_id"])
    first = row()
    project.data.insert(message_type=FILE, record=first)
    duplicate = _refusal(lambda: project.data.insert(message_type=FILE, record=first))
    info = inspect(duplicate)
    assert info.code is ErrorCode.UNIQUE and info.reason == "UDB_UNIQUE_VIOLATION" and info.constraint
    assert info.grpc_code == grpc.StatusCode.ALREADY_EXISTS
    assert is_unique(duplicate) and is_unique(duplicate, info.constraint)
    assert not is_unique(duplicate, "some_other_constraint")
    missing = _refusal(lambda: entity.upsert({"file_id": str(uuid.uuid4()), "filename": None, "object_key": "x"}))
    assert inspect(missing).code is ErrorCode.NOT_NULL and inspect(missing).column == "filename"


def test_cas_conflict_is_only_the_typed_reason(project) -> None:
    entity = project.entity(FILE, key=["file_id"])
    stored = row()
    entity.upsert(stored)
    where = {"file_id": stored["file_id"]}
    conflict = _refusal(lambda: entity.update(where, {"filename": "b"}, expected={"filename": "stale"}))
    assert is_cas_conflict(conflict) and is_conflict(conflict) and error_reason(conflict) == "UDB_CAS_CONFLICT"
    # A conditional write not pinned by a key is a usage error a retry cannot fix.
    usage = _refusal(lambda: entity.update({"filename": "a.pdf"}, {"filename": "b"}, expected={"filename": "a.pdf"}))
    assert inspect(usage).reason == "UDB_CAS_KEY_NOT_PK"
    assert not is_cas_conflict(usage) and code_of(usage) is ErrorCode.PRECONDITION


def test_missing_scope_names_the_scope_and_nothing_else(fake: Fake, project) -> None:
    denied = udbtest.fake._missing_scope("udb:pii:read")
    assert denied.detail.missing["scope"] == "udb:pii:read"
    # Over the wire: the DLQ APIs need udb:admin, which this identity lacks.
    fake.add_identity_api_key("reader", Identity(tenant_id="t-reader", service_identity="r", scopes=("udb:read",)))
    reader = fake.client_for_key("reader")
    refused = _refusal(lambda: reader.data.call("ListDlqEvents", types_pb2.DlqListRequest()))
    assert missing_scope(refused) == "udb:admin" and code_of(refused) is ErrorCode.SCOPE_MISSING
    other = _refusal(lambda: project.data.select(message_type="no.such.Message"))
    assert missing_scope(other) == ""


def test_policy_denial_says_what_rule_is_missing(fake: Fake, project) -> None:
    fake.set_policies([Policy(effect="allow", resource="something.else", action="*")])
    refused = _refusal(lambda: project.entity(FILE).select())
    info = inspect(refused)
    assert info.code is ErrorCode.POLICY_DENIED and info.missing["rule"] == f"Select {FILE}"


def test_inspect_of_non_udb_errors() -> None:
    assert code_of(None) is ErrorCode.UNKNOWN and inspect(None).grpc_code is None
    assert code_of(ValueError("nope")) is ErrorCode.UNKNOWN
    assert missing_scope(None) == "" and not is_cas_conflict(None)
    assert is_not_found(UdbNotFound("x")) and is_conflict(UdbConflict("x"))


def test_store_errors_keep_the_detail_through_their_cause(project) -> None:
    table = Table(project, SPEC)
    stored = row()
    table.upsert(stored)
    with pytest.raises(UdbConflict) as raised:
        table.update_if({**stored, "filename": "b"}, {"filename": "stale"})
    assert inspect(raised.value).reason == "UDB_CAS_CONFLICT" and is_cas_conflict(raised.value)
    with pytest.raises(UdbNotFound) as missing:
        table.delete({"file_id": str(uuid.uuid4())})
    assert inspect(missing.value).reason == "UDB_NO_ROWS_AFFECTED"


# ── gap 5: entity handles ────────────────────────────────────────────────────


def test_entity_select_page_walks_every_row(project) -> None:
    entity = project.table(FILE, key=["file_id"])
    prefix = str(uuid.uuid4())
    for index in range(5):
        entity.upsert(row(f"{prefix}-{index}"))
    seen: list[str] = []
    token = ""
    for _ in range(5):
        page = entity.select_page({"filename": {"$like": f"{prefix}%"}}, limit=2, page_token=token, sort=["-filename"], include_total=True)
        assert page.exact_total == 5
        seen += [item["filename"] for item in page.rows]
        token = page.next_page_token
        if not token:
            break
    assert seen == [f"{prefix}-{index}" for index in (4, 3, 2, 1, 0)]
    assert not page.next_page_token
    without_total = entity.select_page({"filename": {"$like": f"{prefix}%"}}, limit=1)
    assert without_total.has_more and without_total.exact_total == 0


def test_entity_update_sets_only_the_named_columns(project) -> None:
    entity = project.entity(FILE, key=["file_id"])
    stored = row()
    entity.upsert(stored)
    where = {"file_id": stored["file_id"]}
    response = entity.update(where, {"filename": "b.pdf", "content_type": None})
    assert response.affected_rows == 1
    got = entity.select(where=where)[0]
    assert got["filename"] == "b.pdf" and got["object_key"] == stored["object_key"] and got["content_type"] is None
    # Repeat with a guard that holds, then one that does not.
    entity.update(where, {"filename": "c.pdf"}, expected={"filename": "b.pdf"})
    with pytest.raises(UdbRpcError):
        entity.update(where, {"filename": "d.pdf"}, expected={"filename": "b.pdf"})
    assert entity.select(where=where)[0]["filename"] == "c.pdf"


@pytest.mark.parametrize("verb", ["update", "increment", "delete"])
def test_empty_guard_is_refused_before_any_rpc(project, verb: str) -> None:
    entity = project.entity(FILE)
    project.data.stub.Update = project.data.stub.Delete = lambda *a, **k: pytest.fail("no RPC expected")
    with pytest.raises(ValueError, match="non-empty expected"):
        if verb == "update":
            entity.update({"file_id": "x"}, {"filename": "b"}, expected={})
        elif verb == "increment":
            entity.increment({"file_id": "x"}, {"size_bytes": 1}, expected={})
        else:
            entity.delete(where={"file_id": "x"}, expected={})


def test_entity_increment_is_atomic_and_exact(project) -> None:
    entity = project.entity(FILE, key=["file_id"])
    stored = {**row(), "size_bytes": 9007199254740993}
    entity.upsert(stored)
    where = {"file_id": stored["file_id"]}
    for _ in range(3):
        assert entity.increment(where, {"size_bytes": 1}).affected_rows == 1
    assert int(entity.select(where=where)[0]["size_bytes"]) == 9007199254740996


def test_entity_upsert_with_expected_and_was_duplicate(project) -> None:
    entity = project.entity(FILE, key=["file_id"])
    stored = row()
    key = str(uuid.uuid4())
    first = entity.upsert(stored, idempotency_key=key, return_record=False)
    assert first.was_duplicate is False and first.affected_rows == 1, "reads like the response"
    replay = entity.upsert(stored, idempotency_key=key)
    assert replay.was_duplicate is True, "the same key replays"
    moved = {**stored, "filename": "moved.pdf"}
    held = entity.upsert(moved, expected={"filename": "a.pdf"})
    assert held.affected_rows == 1
    refused = _refusal(lambda: entity.upsert(moved, expected={"filename": "a.pdf"}))
    assert is_cas_conflict(refused)
    with pytest.raises(ValueError, match="whitespace-only"):
        entity.upsert(stored, idempotency_key="   ")
    with pytest.raises(ValueError, match="whitespace-only"):
        entity.delete(where={"file_id": "x"}, idempotency_key="   ")


def test_upsert_decodes_a_returned_record_from_the_same_response(project) -> None:
    def fake_upsert(request, *, metadata=None, timeout=None):
        return types_pb2.MutationResponse(record_json=b'{"file_id":"1"}', affected_rows=1)

    project.data.stub.Upsert = fake_upsert
    assert project.entity(FILE, key=["file_id"]).upsert(row(), return_record=True).record == {"file_id": "1"}
    assert project.entity(FILE, key=["file_id"]).upsert(row()).record is None


def test_with_consistency_stamps_only_the_copy(project) -> None:
    captured: list[Any] = []

    def fake_select(request, *, metadata=None, timeout=None):
        captured.append(dict(metadata))
        return types_pb2.RecordSet()

    project.data.stub.Select = fake_select
    entity = project.entity(FILE)
    entity.with_consistency("strong").select()
    entity.select()
    assert captured[0]["x-udb-consistency"] == "strong" and captured[0]["x-udb-primary-read"] == "true"
    assert "x-udb-consistency" not in captured[1]


def test_decode_records_refuses_a_missing_representation(project) -> None:
    def rows_only(request, *, metadata=None, timeout=None):
        record_set = types_pb2.RecordSet()
        record_set.rows.add()
        return record_set

    project.data.stub.Select = rows_only
    with pytest.raises(ValueError, match="no records_json"):
        project.entity(FILE).select()


def test_async_entity_verbs(fake: Fake, tenant: str) -> None:
    async def body() -> None:
        client = await _async_client(fake, tenant)
        try:
            entity = client.entity(FILE, key=["file_id"])
            stored = row()
            await entity.upsert(stored)
            where = {"file_id": stored["file_id"]}
            await entity.update(where, {"filename": "b.pdf"})
            await entity.increment(where, {"size_bytes": 5})
            page = await entity.select_page(where, include_total=True)
            assert page.exact_total == 1 and page.rows[0]["filename"] == "b.pdf"
            assert int(page.rows[0]["size_bytes"]) == 5
            with pytest.raises(UdbRpcError) as raised:
                await entity.update(where, {"filename": "c"}, expected={"filename": "stale"})
            assert is_cas_conflict(raised.value)
            await entity.delete(where=where)
            assert await entity.select(where=where) == []
        finally:
            await client.close()

    run(body)


# ── gap 6: automatic read-your-writes ────────────────────────────────────────


class _Capture:
    """Stub verbs that answer with a receipt and record each request's fence."""

    RECEIPT = '{"source_lsn":"0/ABC","outbox_seq":7,"projection_task_ids":["t1"],"manifest_checksum":"","written_at_unix_ms":1}'

    def __init__(self, client: Any):
        self.fences: list[str] = []
        for verb in ("Upsert", "Insert", "Update", "Delete"):
            setattr(client.stub, verb, self.write)
        client.stub.Select = self.select

    def write(self, request, *, metadata=None, timeout=None):
        return types_pb2.MutationResponse(affected_rows=1, write_receipt_json=self.RECEIPT)

    def select(self, request, *, metadata=None, timeout=None):
        self.fences.append(request.context.read_fence_json)
        return types_pb2.RecordSet()


def _assert_fenced(fence: str) -> None:
    assert json.loads(fence) == {"min_outbox_lsn": "0/ABC", "projection_task_ids": ["t1"], "max_wait_ms": 2000}


def test_entity_reads_carry_the_fence_of_the_latest_write(project) -> None:
    capture = _Capture(project.data)
    entity = project.entity(FILE, key=["file_id"])
    entity.select()
    assert capture.fences == [""], "no write yet, so no fence"
    for write in (
        lambda: entity.upsert(row()),
        lambda: entity.update({"file_id": "1"}, {"filename": "b"}),
        lambda: entity.increment({"file_id": "1"}, {"size_bytes": 1}),
        lambda: entity.delete(where={"file_id": "1"}),
    ):
        project.data._fence_receipt = None
        write()
        entity.select_page({"file_id": "1"})
        _assert_fenced(capture.fences[-1])
    # The fence is per client, so another handle on it reads its writes too.
    project.entity("other.Message").select()
    _assert_fenced(capture.fences[-1])


def test_table_reads_carry_the_fence_of_the_latest_write(project) -> None:
    capture = _Capture(project.data)
    table = Table(project, SPEC)
    table.select()
    assert capture.fences == [""]
    stored = row()
    for write in (
        lambda: table.upsert(stored),
        lambda: table.insert(stored),
        lambda: table.patch({"file_id": stored["file_id"]}, {"filename": "b"}),
        lambda: table.update_if(stored, {"filename": "a"}),
        lambda: table.increment({"file_id": stored["file_id"]}, {"size_bytes": 1}),
        lambda: table.delete({"file_id": stored["file_id"]}),
    ):
        project.data._fence_receipt = None
        write()
        table.select()
        _assert_fenced(capture.fences[-1])


def test_fence_expires_after_the_window_and_caller_fence_wins(project) -> None:
    capture = _Capture(project.data)
    entity = project.entity(FILE, key=["file_id"])
    entity.upsert(row())
    project.data._fence_at -= 31
    entity.select()
    assert capture.fences[-1] == "", "a fence older than the window no longer rides on reads"
    entity.upsert(row())
    own = project.metadata.with_read_fence('{"max_wait_ms":1}')
    entity.select(metadata=own)
    assert capture.fences[-1] == '{"max_wait_ms":1}', "the caller's own fence wins"


def test_a_write_with_no_receipt_leaves_no_fence(project) -> None:
    table = Table(project, SPEC)
    table.upsert(row())  # The in-memory broker returns no receipt.
    assert project.data.read_fence() is None


def test_async_stores_carry_the_fence_too(fake: Fake, tenant: str) -> None:
    async def body() -> None:
        client = await _async_client(fake, tenant)
        try:
            fences: list[str] = []

            async def write(request, *, metadata=None, timeout=None):
                return types_pb2.MutationResponse(affected_rows=1, write_receipt_json=_Capture.RECEIPT)

            async def select(request, *, metadata=None, timeout=None):
                fences.append(request.context.read_fence_json)
                return types_pb2.RecordSet()

            client.data.stub.Upsert = write
            client.data.stub.Select = select
            table = AsyncTable(client, SPEC)
            entity = client.entity(FILE, key=["file_id"])
            await table.select()
            assert fences == [""]
            await table.upsert(row())
            await table.select()
            _assert_fenced(fences[-1])
            client.data._fence_receipt = None
            await entity.upsert(row())
            await entity.select()
            _assert_fenced(fences[-1])
        finally:
            await client.close()

    run(body)


# ── gap 2: transactions and emit ─────────────────────────────────────────────


def _message(name: str = "a.pdf", file_id: str | None = None) -> file_pb2.File:
    file_id = file_id or str(uuid.uuid4())
    return file_pb2.File(file_id=file_id, filename=name, object_key=f"k/{file_id}")


def test_tx_commits_writes_and_events_together(fake: Fake, project, tenant: str) -> None:
    table = Table(project, SPEC)
    for _ in range(3):
        kept = _message()
        event = apikey_events_pb2.ApiKeyCreatedEvent(key_id=kept.file_id, tenant_id=tenant)

        def commit(tx: Any) -> None:
            tx.upsert(kept)
            tx.emit(event)

        project.tx(commit)
        assert table.get({"file_id": kept.file_id})["filename"] == "a.pdf"
    events = fake.events()
    assert [item.topic for item in events] == [TOPIC] * 3
    envelope = events[-1].envelope
    assert envelope["document_id"] == tenant and events[-1].partition_key == tenant
    assert envelope["envelope_version"] == EVENT_ENVELOPE_VERSION
    assert envelope["event_type"] == "udb.core.apikey.events.v1.ApiKeyCreatedEvent"
    assert envelope["payload"]["key_id"] == kept.file_id and envelope["payload"]["tenant_id"] == tenant
    assert envelope["correlation_id"], "a transaction without a correlation id uses its own id"


def test_tx_lost_race_rolls_back_writes_and_events(fake: Fake, project, tenant: str) -> None:
    table = Table(project, SPEC)
    existing = _message("v1.pdf")
    table.upsert({"file_id": existing.file_id, "filename": "v1.pdf", "object_key": existing.object_key})
    fresh = _message()

    def lost(tx: Any) -> None:
        tx.upsert(fresh)
        tx.emit(apikey_events_pb2.ApiKeyCreatedEvent(key_id="k", tenant_id=tenant))
        tx.update_if(_message("v2.pdf", existing.file_id), {"filename": "stale.pdf"})

    with pytest.raises(UdbConflict) as raised:
        project.tx(lost)
    assert inspect(raised.value).reason == "UDB_CAS_CONFLICT"
    assert fake.events() == []
    with pytest.raises(UdbNotFound):
        table.get({"file_id": fresh.file_id})


def test_tx_commits_nothing_when_the_function_raises(fake: Fake, project, tenant: str) -> None:
    fresh = _message()

    def failing(tx: Any) -> None:
        tx.upsert(fresh)
        raise RuntimeError("changed my mind")

    with pytest.raises(RuntimeError, match="changed my mind"):
        project.tx(failing)
    assert fake.rows(FILE, tenant) == []


def test_tx_with_nothing_collected_opens_no_stream(project) -> None:
    project.data.stub.BeginTx = lambda *a, **k: pytest.fail("no stream expected")
    project.tx(lambda tx: None)


def test_tx_refuses_events_without_a_contract_even_if_the_error_is_swallowed(fake: Fake, project, tenant: str) -> None:
    def careless(tx: Any) -> None:
        tx.upsert(_message())
        try:
            tx.emit(file_pb2.File(file_id="x"))
        except ValueError:
            pass

    with pytest.raises(ValueError, match="declares no outbox topic"):
        project.tx(careless)
    assert fake.rows(FILE, tenant) == []
    with pytest.raises(ValueError, match="no value in its partition key field tenant_id"):
        project.tx(lambda tx: tx.emit(apikey_events_pb2.ApiKeyCreatedEvent(key_id="k")))


def test_tx_supports_table_rows_and_delete(fake: Fake, project, tenant: str) -> None:
    table = Table(project, SPEC)
    stored = row("v1.pdf")

    def create(tx: Any) -> None:
        tx.insert(table, stored)

    project.tx(create)
    project.tx(lambda tx: tx.update_if(table, {"filename": "v1.pdf"}, {**stored, "filename": "v2.pdf"}))
    assert table.get({"file_id": stored["file_id"]})["filename"] == "v2.pdf"
    project.tx(lambda tx: tx.patch(table, {"file_id": stored["file_id"]}, {"filename": "v3.pdf"}))
    assert table.get({"file_id": stored["file_id"]})["filename"] == "v3.pdf"
    project.tx(lambda tx: tx.delete(table, {"file_id": stored["file_id"]}))
    with pytest.raises(UdbNotFound):
        table.get({"file_id": stored["file_id"]})


def test_tx_commit_receipt_fences_the_next_read(fake: Fake, project) -> None:
    from udb_client.tx import _Outcome

    outcome = _Outcome(project.data)
    frame = types_pb2.TxStatus(state=types_pb2.TxStatus.TX_STATE_COMMITTED)
    frame.write_receipt.source_lsn = "0/F"
    outcome.read(frame)
    assert outcome.committed and json.loads(project.data.read_fence().read_fence_json)["min_outbox_lsn"] == "0/F"


def test_tx_frame_refusal_keeps_code_and_detail(project) -> None:
    from udb_client.tx import _Outcome

    outcome = _Outcome(project.data)
    frame = types_pb2.TxStatus(
        state=types_pb2.TxStatus.TX_STATE_ERROR, code=grpc.StatusCode.ALREADY_EXISTS.value[0], message="dup"
    )
    frame.error_detail.reason = "UDB_UNIQUE_VIOLATION"
    frame.error_detail.constraint = "files_pkey"
    outcome.read(frame)
    with pytest.raises(UdbRpcError) as raised:
        outcome.finish()
    assert is_unique(raised.value, "files_pkey") and raised.value.code == grpc.StatusCode.ALREADY_EXISTS
    with pytest.raises(Exception, match="without a COMMITTED status"):
        _Outcome(project.data).finish()


def test_async_tx(fake: Fake, tenant: str) -> None:
    async def body() -> None:
        client = await _async_client(fake, tenant)
        try:
            table = AsyncTable(client, SPEC)
            kept, fresh = _message(), _message()

            async def commit(tx: Any) -> None:
                tx.upsert(kept)
                tx.emit(apikey_events_pb2.ApiKeyCreatedEvent(key_id="k", tenant_id=tenant))

            await client.tx(commit)
            assert (await table.get({"file_id": kept.file_id}))["filename"] == "a.pdf"
            assert len(fake.events()) == 1

            def lost(tx: Any) -> None:
                tx.upsert(fresh)
                tx.update_if(_message("x", kept.file_id), {"filename": "stale"})

            with pytest.raises(UdbConflict) as raised:
                await client.tx(lost)
            assert inspect(raised.value).reason == "UDB_CAS_CONFLICT"
            with pytest.raises(UdbNotFound):
                await table.get({"file_id": fresh.file_id})
            with pytest.raises(UdbRpcError) as duplicate:
                await client.tx(lambda tx: tx.insert(kept))
            assert is_unique(duplicate.value), "a refused stream keeps its typed detail on asyncio"
        finally:
            await client.close()

    run(body)


# ── gap 3: durable consumer ──────────────────────────────────────────────────


class _Run:
    """Run a consumer on a thread until the handler has seen enough."""

    def __init__(self, project: Any, name: str, handler, **kwargs: Any):
        self.stop = threading.Event()
        self.error: BaseException | None = None
        self.handled: list[Any] = []
        self._handler = handler

        def target() -> None:
            try:
                consume(project, name, kwargs.pop("topic", TOPIC), self._handle, stop=self.stop, **kwargs)
            except BaseException as error:  # noqa: BLE001 - reported to the test.
                self.error = error

        self.thread = threading.Thread(target=target, daemon=True)
        self.thread.start()

    def _handle(self, event: Any) -> None:
        self.handled.append(event)
        if self._handler is not None:
            self._handler(event, self)

    def finish(self, timeout: float = 10.0) -> None:
        self.thread.join(timeout)
        assert not self.thread.is_alive(), "the consumer did not stop"

    def wait_for(self, predicate, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while not predicate():
            assert time.monotonic() < deadline, f"timed out; handled {[e.id for e in self.handled]}, error {self.error}"
            time.sleep(0.005)


def _stop_after(count: int):
    def handler(event: Any, consumer: _Run) -> None:
        if len(consumer.handled) >= count:
            consumer.stop.set()

    return handler


def _emit_keys(fake: Fake, tenant: str, *keys: str) -> list[str]:
    return [fake.emit(tenant, TOPIC, tenant, {"key_id": key, "tenant_id": tenant}) for key in keys]


def test_consume_resumes_after_the_last_acknowledged_event(fake: Fake, project, tenant: str) -> None:
    _emit_keys(fake, tenant, "k1", "k2", "k3")
    first = _Run(project, "billing", _stop_after(2), message_type=apikey_events_pb2.ApiKeyCreatedEvent)
    first.finish()
    assert first.error is None
    assert [event.payload.key_id for event in first.handled] == ["k1", "k2"]
    _emit_keys(fake, tenant, "k4")
    second = _Run(project, "billing", _stop_after(2), message_type=apikey_events_pb2.ApiKeyCreatedEvent)
    second.finish()
    assert [event.payload.key_id for event in second.handled] == ["k3", "k4"], "nothing skipped, nothing repeated"
    event = second.handled[0]
    assert event.topic == TOPIC and event.partition_key == tenant and event.type == TOPIC
    assert event.envelope["payload"]["key_id"] == "k3"


def test_consume_only_sees_the_callers_tenant(fake: Fake, project, tenant: str) -> None:
    other = str(uuid.uuid4())
    fake.emit(other, TOPIC, other, {"key_id": "theirs"})
    fake.emit(tenant, TOPIC, tenant, {"key_id": "mine"})
    consumer = _Run(project, "audit", _stop_after(1))
    consumer.finish()
    assert [event.payload["key_id"] for event in consumer.handled] == ["mine"], "a dict payload without a message_type"


def test_max_attempts_then_dead_letter_and_keep_flowing(fake: Fake, project, tenant: str) -> None:
    poison_id, later_id = _emit_keys(fake, tenant, "poison", "later")
    attempts: list[str] = []

    def handler(event: Any, consumer: _Run) -> None:
        attempts.append(event.payload["key_id"])
        if event.payload["key_id"] == "poison":
            raise RuntimeError("bad event")
        consumer.stop.set()

    errors: list[Exception] = []
    consumer = _Run(project, "dlq", handler, options=ConsumeOptions(max_attempts=3, on_error=errors.append, **FAST_CONSUMER))
    consumer.finish()
    assert consumer.error is None
    assert attempts == ["poison"] * 3 + ["later"], "attempted max_attempts times, then later events flow"
    assert len(errors) == 3
    dead = project.data.call("ListDlqEvents", types_pb2.DlqListRequest()).events
    assert [item.event_id for item in dead] == [poison_id]
    assert dead[0].status == "QUARANTINED" and dead[0].error_message == "bad event"
    assert json.loads(dead[0].payload_json)["consumer_failures"]
    # A restart does not hand either event over again.
    again = _Run(project, "dlq", None, options=ConsumeOptions(**FAST_CONSUMER))
    time.sleep(0.2)
    again.stop.set()
    again.finish()
    assert again.handled == []


def test_broker_that_cannot_dead_letter_stops_the_consumer(fake: Fake, project, tenant: str) -> None:
    poison_id = _emit_keys(fake, tenant, "poison")[0]

    def handler(event: Any, consumer: _Run) -> None:
        raise RuntimeError("bad")

    fake.inject_fault(Fault(code=grpc.StatusCode.UNIMPLEMENTED, method="NackCdcEvents", times=1))
    consumer = _Run(project, "old", handler, options=ConsumeOptions(max_attempts=1, **FAST_CONSUMER))
    consumer.finish()
    assert isinstance(consumer.error, UdbRpcError) and consumer.error.code == grpc.StatusCode.UNIMPLEMENTED
    assert not project.data.call("ListDlqEvents", types_pb2.DlqListRequest()).events, poison_id


def test_lost_ack_is_retried_without_handling_again(fake: Fake, project, tenant: str) -> None:
    _emit_keys(fake, tenant, "k1", "k2")
    fake.inject_fault(Fault(code=grpc.StatusCode.UNAVAILABLE, method="AckCdcEvents", times=1))
    errors: list[Exception] = []
    consumer = _Run(project, "lost-ack", None, options=ConsumeOptions(on_error=errors.append, **FAST_CONSUMER))
    consumer.wait_for(lambda: len(consumer.handled) >= 2)
    consumer.stop.set()
    consumer.finish()
    keys = [event.payload["key_id"] for event in consumer.handled]
    assert keys == ["k1", "k2"], "the redelivered event only re-acknowledges"
    assert any("reconnecting" in str(error) for error in errors)
    cursor = next(iter(fake._cursors.values()))
    assert cursor["index"] == 1, "both events settled"


def test_ack_batch_size_settles_one_watermark(fake: Fake, project, tenant: str) -> None:
    ids = _emit_keys(fake, tenant, "a", "b", "c")
    acks: list[str] = []
    original = project.data.stub.AckCdcEvents

    def counting(request, **kwargs):
        acks.append(request.event_id)
        return original(request, **kwargs)

    project.data.stub.AckCdcEvents = counting
    consumer = _Run(project, "batch", None, options=ConsumeOptions(ack_batch_size=2, ack_interval=60.0, **FAST_CONSUMER))
    consumer.wait_for(lambda: len(consumer.handled) >= 3)
    time.sleep(0.05)
    assert acks == [ids[1]], "one watermark for the full batch; the partial batch waits"
    consumer.stop.set()
    consumer.finish()
    assert acks == [ids[1], ids[2]], "the shutdown flush settles the remainder"


def test_idle_partial_batch_flushes_on_the_interval(fake: Fake, project, tenant: str) -> None:
    ids = _emit_keys(fake, tenant, "a")
    acks: list[str] = []
    original = project.data.stub.AckCdcEvents

    def counting(request, **kwargs):
        acks.append(request.event_id)
        return original(request, **kwargs)

    project.data.stub.AckCdcEvents = counting
    consumer = _Run(project, "idle", None, options=ConsumeOptions(ack_batch_size=100, ack_interval=0.05, **FAST_CONSUMER))
    consumer.wait_for(lambda: acks == ids)
    consumer.stop.set()
    consumer.finish()


def test_ack_timeout_reaches_each_settlement(fake: Fake, project, tenant: str) -> None:
    _emit_keys(fake, tenant, "a")
    timeouts: list[float] = []
    original = project.data.stub.AckCdcEvents

    def recording(request, *, metadata=None, timeout=None):
        timeouts.append(timeout)
        return original(request, metadata=metadata, timeout=timeout)

    project.data.stub.AckCdcEvents = recording
    consumer = _Run(project, "t", _stop_after(1), options=ConsumeOptions(ack_timeout=1.5))
    consumer.finish()
    assert timeouts == [1.5]
    assert ConsumeOptions(ack_timeout=99, ack_batch_size=99999, max_attempts=99999).normalized().ack_timeout == 30.0
    assert ConsumeOptions(ack_batch_size=99999).normalized().ack_batch_size == 1024
    assert ConsumeOptions(max_attempts=99999).normalized().max_attempts == 1000
    assert ConsumeOptions(max_attempts=0).normalized().max_attempts == 5


def test_permanent_ack_refusal_is_raised_with_its_detail(fake: Fake, project, tenant: str) -> None:
    _emit_keys(fake, tenant, "a")
    fake.inject_fault(Fault(code=grpc.StatusCode.PERMISSION_DENIED, method="AckCdcEvents", reason="UDB_POLICY_DENIED", times=1))
    consumer = _Run(project, "refused", None, options=ConsumeOptions(**FAST_CONSUMER))
    consumer.finish()
    assert inspect(consumer.error).reason == "UDB_POLICY_DENIED"


def test_envelope_newer_than_the_sdk_stops_the_consumer(fake: Fake, project, tenant: str) -> None:
    good, newer = _emit_keys(fake, tenant, "good", "newer")
    fake._events[1].envelope["envelope_version"] = EVENT_ENVELOPE_VERSION + 1
    consumer = _Run(project, "future", None, options=ConsumeOptions(**FAST_CONSUMER))
    consumer.finish()
    assert "newer than this SDK understands" in str(consumer.error)
    assert [event.id for event in consumer.handled] == [good]
    assert next(iter(fake._cursors.values()))["id"] == good, "only the earlier event settled"
    assert newer


def test_consumer_name_is_validated_by_the_broker(fake: Fake, project) -> None:
    consumer = _Run(project, "not a valid name!", None, options=ConsumeOptions(**FAST_CONSUMER))
    consumer.finish()
    assert code_of(consumer.error) is ErrorCode.INVALID


def test_consume_async_resumes_and_dead_letters(fake: Fake, tenant: str) -> None:
    poison, *_ = _emit_keys(fake, tenant, "poison", "k2", "k3")

    async def body() -> None:
        client = await _async_client(fake, tenant)
        try:
            stop = asyncio.Event()
            seen: list[str] = []

            async def handler(event: Any) -> None:
                seen.append(event.payload.key_id)
                if event.payload.key_id == "poison":
                    raise RuntimeError("bad")
                if event.payload.key_id == "k2":
                    stop.set()

            await asyncio.wait_for(
                consume_async(
                    client, "async", TOPIC, handler, stop=stop,
                    message_type=apikey_events_pb2.ApiKeyCreatedEvent,
                    options=ConsumeOptions(max_attempts=2, **FAST_CONSUMER),
                ),
                10,
            )
            assert seen == ["poison", "poison", "k2"]
            resumed = asyncio.Event()

            def plain_handler(event: Any) -> None:
                seen.append(event.payload["key_id"])
                resumed.set()

            await asyncio.wait_for(
                consume_async(client, "async", TOPIC, plain_handler, stop=resumed, options=ConsumeOptions(**FAST_CONSUMER)),
                10,
            )
            assert seen[-1] == "k3" and seen.count("k3") == 1
            response = await client.data.call("ListDlqEvents", types_pb2.DlqListRequest())
            assert [item.event_id for item in response.events] == [poison]
        finally:
            await client.close()

    run(body)


# ── gap 8: fake behaviour and fault injection ────────────────────────────────


def test_inject_fault_validates_and_counts(fake: Fake, project) -> None:
    with pytest.raises(ValueError):
        fake.inject_fault(Fault(code=grpc.StatusCode.OK))
    with pytest.raises(ValueError):
        fake.inject_fault(Fault(code=grpc.StatusCode.INTERNAL, times=-1))
    fake.inject_fault(Fault(code=grpc.StatusCode.INTERNAL, method="/udb.services.v1.DataBroker/Select"))
    with pytest.raises(UdbRpcError) as raised:
        project.entity(FILE).select()
    assert raised.value.code == grpc.StatusCode.INTERNAL and code_of(raised.value) is ErrorCode.INTERNAL
    assert project.entity(FILE).select() == [], "the fault was bounded to one call"


def test_fault_scoped_to_another_tenant_passes(fake: Fake, project) -> None:
    fake.inject_fault(Fault(code=grpc.StatusCode.INTERNAL, tenant_id="someone-else", times=5))
    assert project.entity(FILE).select() == []


def test_rate_limit_carries_retry_after(fake: Fake, project) -> None:
    project.data._retry = RetryPolicy(max_attempts=1)
    fake.set_rate_limit(1, 60.0)
    assert project.entity(FILE).select() == []
    with pytest.raises(UdbRpcError) as raised:
        project.entity(FILE).select()
    info = inspect(raised.value)
    assert info.code is ErrorCode.RATE_LIMITED and info.retry_after > 0


def test_expired_bearer_is_refused_by_the_fake(fake: Fake, tenant: str) -> None:
    from udb_client import UdbConfig, UdbProject

    fake.set_token_lifetime(1)
    with UdbProject(UdbConfig(target=fake.target, tenant_id=tenant, project_id="", purpose="t", bearer_token="unknown")) as bad:
        with pytest.raises(UdbRpcError) as raised:
            bad.entity(FILE).select()
    assert code_of(raised.value) is ErrorCode.UNAUTHENTICATED
    with pytest.raises(ValueError):
        fake.set_token_lifetime(0.5)


def test_fake_rows_are_a_copy(fake: Fake, project, tenant: str) -> None:
    project.entity(FILE, key=["file_id"]).upsert(row())
    rows = fake.rows(FILE, tenant)
    assert rows[0]["tenant_id"] == tenant, "the broker fills the verified tenant in"
    rows[0]["filename"] = "tampered"
    assert fake.rows(FILE, tenant)[0]["filename"] == "a.pdf"


def test_start_refuses_without_a_broker_binary(monkeypatch: pytest.MonkeyPatch) -> None:
    from udb_client.udbtest import embedded

    monkeypatch.delenv("UDB_TEST_BINARY", raising=False)
    monkeypatch.setattr(embedded.shutil, "which", lambda name: None)
    with pytest.raises(RuntimeError, match="install an embedded-capable udb or set UDB_TEST_BINARY"):
        udbtest.start()


# ── gap 9: async typed table ─────────────────────────────────────────────────


def test_async_table_has_every_method_of_the_sync_table() -> None:
    public = lambda cls: {name for name in dir(cls) if not name.startswith("_")}  # noqa: E731
    assert public(AsyncTable) == public(Table)
    assert public(AsyncRepo) == public(Repo)


def test_async_table_contract(fake: Fake, tenant: str) -> None:
    async def body() -> None:
        client = await _async_client(fake, tenant)
        try:
            table = AsyncTable(client, SPEC)
            stored = row("v1.pdf")
            key = {"file_id": stored["file_id"]}
            with pytest.raises(UdbNotFound):
                await table.get(key)
            await table.upsert(stored)
            assert (await table.get(key))["filename"] == "v1.pdf"
            with pytest.raises(UdbRpcError) as duplicate:
                await table.insert(stored)
            assert is_unique(duplicate.value)
            await table.patch(key, {"filename": "v2.pdf"})
            await table.update_if({**stored, "filename": "v3.pdf"}, {"filename": "v2.pdf"})
            with pytest.raises(UdbConflict):
                await table.update_if({**stored, "filename": "v4.pdf"}, {"filename": "v2.pdf"})

            async def rename(current: dict[str, Any]) -> dict[str, Any]:
                return {**current, "filename": "async-" + current["filename"]}

            assert (await table.update_with_retry(key, rename))["filename"] == "async-v3.pdf"
            assert (await table.update_with_retry(key, lambda current: current))["filename"] == "async-v3.pdf"
            await table.increment(key, {"size_bytes": 2})
            assert (await table.get(key))["size_bytes"] == 2
            for index in range(3):
                await table.upsert(row(f"p{index}"))
            assert await table.count() == 4
            seen: list[str] = []

            async def collect(item: dict[str, Any]) -> None:
                seen.append(item["filename"])

            await table.select_all(None, collect)
            assert len(seen) == 4
            page = await table.select(limit=2, order_by=["filename"], include_total=True)
            assert len(page.rows) == 2 and page.has_more and page.total == 4
            await table.delete(key)
            with pytest.raises(UdbNotFound):
                await table.delete(key)
            with pytest.raises(ValueError, match="has no column"):
                await table.select({"nope": 1})
        finally:
            await client.close()

    run(body)


def test_async_repo_contract(fake: Fake, tenant: str) -> None:
    async def body() -> None:
        client = await _async_client(fake, tenant)
        try:
            repo = AsyncRepo(client, SPEC)
            table = AsyncTable(client, SPEC)
            stored = row("v1.pdf")
            where = {"file_id": stored["file_id"]}
            assert await repo.get(where) is None
            await table.upsert(stored)
            assert (await repo.get(where))["filename"] == "v1.pdf"
            with pytest.raises(ValueError, match="missing from filter"):
                await repo.get({"filename": "v1.pdf"})
            assert (await repo.find_first({"filename": "v1.pdf"}))["file_id"] == stored["file_id"]
            assert (await repo.list(limit=1)).rows
            await repo.update_guarded(where, {"filename": "v2.pdf"}, {"filename": "v1.pdf"})
            with pytest.raises(UdbConflict):
                await repo.update_guarded(where, {"filename": "v3.pdf"}, {"filename": "v1.pdf"})
            with pytest.raises(ValueError, match="expected precondition is empty"):
                await repo.update_guarded(where, {"filename": "x"}, {})
            with pytest.raises(ValueError, match="expected precondition is empty"):
                await repo.delete_guarded(where, {})
            await repo.delete_guarded(where, {"filename": "v2.pdf"})
            assert await repo.get(where) is None
        finally:
            await client.close()

    run(body)


def test_sync_repo_guards(project) -> None:
    repo = Repo(project, SPEC)
    table = Table(project, SPEC)
    stored = row("v1.pdf")
    table.upsert(stored)
    where = {"file_id": stored["file_id"]}
    repo.update_guarded(where, {"filename": "v2.pdf"}, {"filename": "v1.pdf"})
    with pytest.raises(UdbConflict):
        repo.delete_guarded(where, {"filename": "v1.pdf"})
    with pytest.raises(ValueError, match="unconditional delete"):
        repo.delete_guarded(where, {})
    with pytest.raises(ValueError, match="unconditional update"):
        repo.update_guarded(where, {"filename": "x"}, {})
    repo.delete_guarded(where, {"filename": "v2.pdf"})
    assert repo.get(where) is None


def test_table_writes_send_no_conflict_fields_like_go(project) -> None:
    captured: list[Any] = []

    def capture(request, *, metadata=None, timeout=None):
        captured.append(request)
        return types_pb2.MutationResponse(affected_rows=1)

    project.data.stub.Upsert = project.data.stub.Insert = capture
    table = Table(project, SPEC)
    table.insert(row())
    table.upsert(row())
    assert [list(request.conflict_fields) for request in captured] == [[], []]


def test_update_if_never_writes_the_key_or_scope_columns(project) -> None:
    table = Table(project, SPEC)
    stored = row()
    table.upsert(stored)
    current = table.get({"file_id": stored["file_id"]})
    assert "tenant_id" in current
    # A row read back carries its tenant; resending it through an update must work.
    table.update_if({**current, "filename": "b.pdf"}, {"filename": "a.pdf"})
    assert table.get({"file_id": stored["file_id"]})["filename"] == "b.pdf"
