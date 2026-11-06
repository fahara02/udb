"""The data path against a real broker: the same behaviour the Go and TypeScript live
tests prove, driven through this SDK.

Skipped unless ``UDB_LIVE_SDK_TESTS=1``. Runs on a Postgres-only broker with the
environment of CI's "SDK refresh-token rotation (live)" job. The durable-consumer test
also needs a served CDC lane, so it is skipped unless ``UDB_LIVE_CDC_TESTS=1``.

Each area repeats the action and includes a negative case: a behaviour that works once
or only accepts is not proven.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import threading
from typing import Any
import uuid

import grpc
import pytest

from udb.core.storage.entity.v1 import file_pb2
from udb.core.storage.events.v1 import storage_events_pb2
from udb.core.workflow.entity.v1 import enums_pb2, workflow_instance_pb2
from udb_client import (
    AsyncRepo,
    AsyncTable,
    ConsumeOptions,
    ErrorCode,
    Metadata,
    Repo,
    Table,
    UdbClient,
    UdbConfig,
    UdbAsyncProject,
    UdbConflict,
    UdbNotFound,
    UdbProject,
    UdbRpcError,
    code_of,
    consume,
    encode_record,
    inspect,
    is_cas_conflict,
    is_unique,
    table_spec_of,
    topic,
)
from udb_client.generated_client import RetryPolicy, UdbDetailedRpcError
from udb_client.udbtest import conformance

pytestmark = pytest.mark.skipif(
    os.getenv("UDB_LIVE_SDK_TESTS") != "1",
    reason="requires live UDB broker",
)

REPEATS = 3
WORKFLOW = "udb.core.workflow.entity.v1.WorkflowInstance"
FILE = "udb.core.storage.entity.v1.File"
SPEC = table_spec_of(workflow_instance_pb2.WorkflowInstance)
CASES_FILE = Path(__file__).resolve().parents[2] / "conformance" / "table-cases.json"


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise AssertionError(f"{name} is required when UDB_LIVE_SDK_TESTS=1")
    return value


def _config(**overrides: Any) -> UdbConfig:
    target = required_env("UDB_GRPC_TARGET")
    fields: dict[str, Any] = dict(
        target=target,
        auth_target=os.getenv("UDB_AUTH_GRPC_TARGET", "").strip() or target,
        tenant_id=required_env("UDB_LIVE_TENANT"),
        project_id=os.getenv("UDB_LIVE_PROJECT", "default"),
        purpose="python.live.parity-data",
        correlation_id="python-live-parity-data",
        service_identity="python.sdk.live",
        deadline=20.0,
    )
    fields.update(overrides)
    return UdbConfig(**fields)


@pytest.fixture
def project():
    client = UdbProject(_config())
    client.login_and_adopt_tenant(required_env("UDB_LIVE_USERNAME"), required_env("UDB_LIVE_PASSWORD"))
    assert client.metadata.tenant_id, "the verified principal must carry its canonical tenant"
    yield client
    client.close()


def _async_project(project: UdbProject) -> UdbAsyncProject:
    meta = project.metadata
    return UdbAsyncProject(
        _config(
            tenant_id=meta.tenant_id,
            project_id=meta.project_id,
            user_id=meta.user_id,
            service_identity=meta.service_identity,
            scopes=tuple(meta.scopes),
            bearer_token=meta.bearer_token,
        )
    )


def workflow_row(project: UdbProject, **changes: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "workflow_id": str(uuid.uuid4()),
        "tenant_id": project.metadata.tenant_id,
        "workflow_type": f"py.live.parity.{uuid.uuid4()}",
        "current_step": 0,
        "total_steps": 7,
    }
    row.update(changes)
    return row


def _file(project: UdbProject, name: str = "parity.pdf") -> file_pb2.File:
    file_id = str(uuid.uuid4())
    return file_pb2.File(
        file_id=file_id,
        tenant_id=project.metadata.tenant_id,
        filename=name,
        object_key=f"py-live-parity/{file_id}",
        status="FILE_STATUS_PENDING",
    )


def _file_uploaded(project: UdbProject, marker: str) -> storage_events_pb2.FileUploaded:
    return storage_events_pb2.FileUploaded(
        file_id=marker, tenant_id=project.metadata.tenant_id, object_key=f"py-live-parity/{marker}"
    )


# ── retry and typed errors ───────────────────────────────────────────────────


def test_live_refusals_are_typed_every_time(project: UdbProject) -> None:
    entity = project.entity(WORKFLOW, key=["workflow_id"])
    row = workflow_row(project)
    entity.upsert(row)
    where = {"workflow_id": row["workflow_id"]}
    for attempt in range(REPEATS):
        # `except UdbRpcError` keeps working, and now carries the broker's detail.
        with pytest.raises(UdbRpcError) as refused:
            entity.update(where, {"current_step": 1}, expected={"current_step": 99 + attempt})
        assert isinstance(refused.value, UdbDetailedRpcError)
        assert is_cas_conflict(refused.value), repr(refused.value)
        assert inspect(refused.value).code is ErrorCode.CONFLICT
    # Negative: a refusal that is not a conflict does not read as one.
    with pytest.raises(UdbRpcError) as unknown:
        project.data.select(message_type="udb.no.such.Message")
    assert not is_cas_conflict(unknown.value)
    assert unknown.value.code == grpc.StatusCode.INVALID_ARGUMENT, repr(unknown.value)


def test_live_no_default_catalog_pin_and_a_sent_pin_is_honoured(project: UdbProject) -> None:
    assert project.metadata.client_catalog_version == ""
    for _ in range(REPEATS):
        project.data.select(message_type=WORKFLOW, limit=1)
    pinned = UdbProject(_config(client_catalog_version="999.0.0"))
    try:
        pinned.login_and_adopt_tenant(required_env("UDB_LIVE_USERNAME"), required_env("UDB_LIVE_PASSWORD"))
        for _ in range(REPEATS):
            with pytest.raises(UdbRpcError) as refused:
                pinned.data.select(message_type=WORKFLOW, limit=1)
            assert refused.value.code == grpc.StatusCode.FAILED_PRECONDITION, repr(refused.value)
    finally:
        pinned.close()


def test_live_a_json_string_column_reads_and_writes_as_json_text(project: UdbProject) -> None:
    entity = project.entity(WORKFLOW, key=["workflow_id"])
    for payload in ('"My"', '"5"', '""', "5", "[1]", '{"a":1}'):
        message = workflow_instance_pb2.WorkflowInstance(
            workflow_id=str(uuid.uuid4()),
            tenant_id=project.metadata.tenant_id,
            workflow_type="py.live.json-string",
            status=enums_pb2.WORKFLOW_STATUS_PENDING,
            total_steps=1,
            payload=payload,
        )
        for _ in range(REPEATS):
            entity.upsert(encode_record(message))
            rows = entity.select(where={"workflow_id": message.workflow_id})
            decoded = decode_record(rows[0], workflow_instance_pb2.WorkflowInstance())
            assert decoded.payload == payload, "what was read is what was written"
            message.payload = decoded.payload
    # Negative: text that is not JSON never reaches the column.
    with pytest.raises(ValueError, match="not JSON"):
        encode_record(workflow_instance_pb2.WorkflowInstance(payload="My"))


def test_live_read_retries_a_dead_endpoint_then_gives_up(project: UdbProject) -> None:
    attempts = 0
    with UdbClient(
        "127.0.0.1:1",
        Metadata(tenant_id=project.metadata.tenant_id, purpose="python.live", correlation_id="c"),
        timeout=2.0,
        retry=RetryPolicy(max_attempts=3, initial_backoff=0.001, max_backoff=0.002),
    ) as dead:
        real = dead.stub.Select

        def counted(*args: Any, **kwargs: Any) -> Any:
            nonlocal attempts
            attempts += 1
            return real(*args, **kwargs)

        dead.stub.Select = counted
        with pytest.raises(UdbRpcError) as raised:
            dead.select(message_type=WORKFLOW)
        assert attempts == 3, "a read retries up to the policy"
        assert code_of(raised.value) is ErrorCode.UNAVAILABLE

        attempts = 0
        real_upsert = dead.stub.Upsert

        def counted_upsert(*args: Any, **kwargs: Any) -> Any:
            nonlocal attempts
            attempts += 1
            return real_upsert(*args, **kwargs)

        dead.stub.Upsert = counted_upsert
        with pytest.raises(UdbRpcError):
            dead.upsert(message_type=WORKFLOW, record={"a": 1})
        assert attempts == 1, "a keyless mutation is never retried"


# ── entity handle ────────────────────────────────────────────────────────────


def test_live_entity_verbs(project: UdbProject) -> None:
    entity = project.entity(WORKFLOW, key=["workflow_id"])
    workflow_type = f"py.live.parity.{uuid.uuid4()}"
    rows = [workflow_row(project, workflow_type=workflow_type) for _ in range(5)]
    for row in rows:
        entity.upsert(row)
    seen: list[str] = []
    token = ""
    for _ in range(5):
        page = entity.select_page({"workflow_type": workflow_type}, limit=2, page_token=token, include_total=True)
        assert page.exact_total == 5
        seen += [item["workflow_id"] for item in page.rows]
        token = page.next_page_token
        if not token:
            break
    assert sorted(seen) == sorted(row["workflow_id"] for row in rows), "paging visits every row once"

    target = rows[0]
    where = {"workflow_id": target["workflow_id"]}
    for step in range(1, REPEATS + 1):
        entity.update(where, {"current_step": step}, expected={"current_step": step - 1})
    assert entity.select(where=where)[0]["current_step"] == REPEATS
    for _ in range(REPEATS):
        entity.increment(where, {"total_steps": 2})
    assert entity.select(where=where)[0]["total_steps"] == 7 + 2 * REPEATS

    key = str(uuid.uuid4())
    replayed = workflow_row(project)
    assert entity.upsert(replayed, idempotency_key=key).was_duplicate is False
    for _ in range(REPEATS):
        assert entity.upsert(replayed, idempotency_key=key).was_duplicate is True

    # Negative: a guarded write that lost changes nothing, and an empty guard is refused.
    with pytest.raises(UdbRpcError) as stale:
        entity.upsert({**target, "current_step": 50}, expected={"current_step": 1})
    assert is_cas_conflict(stale.value)
    assert entity.select(where=where)[0]["current_step"] == REPEATS
    with pytest.raises(ValueError, match="non-empty expected"):
        entity.update(where, {"current_step": 0}, expected={})


# ── read-your-writes ─────────────────────────────────────────────────────────


def test_live_reads_after_writes_carry_the_receipt_fence(project: UdbProject) -> None:
    table = Table(project, SPEC)
    fences: list[str] = []
    real = project.data.stub.Select

    def observing(request: Any, **kwargs: Any) -> Any:
        fences.append(request.context.read_fence_json)
        return real(request, **kwargs)

    project.data.stub.Select = observing
    # Negative: no write yet on this client, so no fence.
    missing = {"workflow_id": str(uuid.uuid4())}
    with pytest.raises(UdbNotFound):
        table.get(missing)
    assert fences == [""]
    for step in range(REPEATS):
        row = workflow_row(project, current_step=step)
        table.insert(row)
        assert table.get({"workflow_id": row["workflow_id"]})["current_step"] == step
        assert '"max_wait_ms":2000' in fences[-1], f"the read carried no fence: {fences[-1]!r}"


# ── typed table contract ─────────────────────────────────────────────────────


@pytest.mark.parametrize("case", conformance.load_table_cases(CASES_FILE), ids=lambda case: case["name"])
def test_live_shared_table_cases(project: UdbProject, case: dict[str, Any]) -> None:
    conformance.run_table_case(lambda tenant: project, case)


def test_live_table_and_repo_guards(project: UdbProject) -> None:
    table = Table(project, SPEC)
    repo = Repo(project, SPEC)
    row = workflow_row(project)
    table.insert(row)
    key = {"workflow_id": row["workflow_id"]}
    with pytest.raises(UdbRpcError) as duplicate:
        table.insert(row)
    assert is_unique(duplicate.value)
    current = table.get(key)
    for step in range(1, REPEATS + 1):
        table.update_if({**current, "current_step": step}, {"current_step": step - 1})
    assert table.get(key)["current_step"] == REPEATS
    with pytest.raises(UdbConflict) as lost:
        table.update_if({**current, "current_step": 9}, {"current_step": 0})
    assert inspect(lost.value).reason, "the conflict keeps the broker's reason"
    assert repo.get({"workflow_id": str(uuid.uuid4())}) is None
    with pytest.raises(ValueError):
        repo.update_guarded(key, {"current_step": 1}, {})


# ── transactions and emit ────────────────────────────────────────────────────


def test_live_tx_commits_writes_and_events_together(project: UdbProject) -> None:
    files = Table(project, table_spec_of(file_pb2.File))
    existing = _file(project, "v1.pdf")
    files.insert(encode_record(existing))
    for _ in range(REPEATS):
        fresh = _file(project)
        marker = f"py-live-{uuid.uuid4()}"

        def commit(tx: Any) -> None:
            tx.upsert(fresh)
            tx.emit(_file_uploaded(project, marker))

        project.tx(commit)
        assert files.get({"file_id": fresh.file_id})["filename"] == "parity.pdf"

    # Negative: a lost compare-and-swap rolls back the earlier write and the event.
    rolled_back = _file(project, "must-not-commit.pdf")

    def lost(tx: Any) -> None:
        tx.upsert(rolled_back)
        tx.emit(_file_uploaded(project, f"py-live-{uuid.uuid4()}"))
        tx.update_if(_file_replacing(existing, "v2.pdf"), {"filename": "stale.pdf"})

    with pytest.raises(UdbConflict):
        project.tx(lost)
    with pytest.raises(UdbNotFound):
        files.get({"file_id": rolled_back.file_id})
    assert files.get({"file_id": existing.file_id})["filename"] == "v1.pdf"

    def duplicate(tx: Any) -> None:
        tx.insert(_file(project, "also-not.pdf"))
        tx.insert(existing)

    with pytest.raises(UdbRpcError) as refused:
        project.tx(duplicate)
    assert is_unique(refused.value), repr(refused.value)


def _file_replacing(existing: file_pb2.File, name: str) -> file_pb2.File:
    copy = file_pb2.File()
    copy.CopyFrom(existing)
    copy.filename = name
    return copy


# ── durable consumer ─────────────────────────────────────────────────────────


@pytest.mark.skipif(os.getenv("UDB_LIVE_CDC_TESTS") != "1", reason="requires the CI served CDC lane")
def test_live_consumer_resumes_and_dead_letters(project: UdbProject) -> None:
    name = f"py-live-{uuid.uuid4().hex}"
    prefix = f"{name}/"
    event_topic = topic(storage_events_pb2.FileUploaded)

    def emit(marker: str) -> None:
        project.tx(lambda tx: tx.emit(_file_uploaded(project, prefix + marker)))

    def run(want: str, handler_failures: dict[str, int] | None = None) -> list[str]:
        stop = threading.Event()
        seen: list[str] = []
        failures = handler_failures or {}

        def handler(event: Any) -> None:
            if not event.payload.file_id.startswith(prefix):
                return
            marker = event.payload.file_id[len(prefix):]
            seen.append(marker)
            if failures.get(marker, 0) > 0:
                failures[marker] -= 1
                raise RuntimeError(f"failing {marker}")
            if marker == want:
                stop.set()

        errors: list[BaseException] = []

        def target() -> None:
            try:
                consume(
                    project, name, event_topic, handler, stop=stop,
                    message_type=storage_events_pb2.FileUploaded,
                    options=ConsumeOptions(from_earliest=True, max_attempts=2, backoff=0.01),
                )
            except BaseException as error:  # noqa: BLE001 - reported below.
                errors.append(error)

        thread = threading.Thread(target=target, daemon=True)
        thread.start()
        thread.join(90)
        stop.set()
        thread.join(15)
        assert not thread.is_alive(), f"the consumer did not stop; saw {seen}"
        assert not errors, errors
        assert want in seen, f"{want} never arrived; saw {seen}"
        return seen

    emit("first")
    assert run("first") == ["first"]
    for step in range(REPEATS):
        marker = f"next-{step}"
        emit(marker)
        assert run(marker) == [marker], "a restart resumes after the last acknowledged event"

    # Negative: a poison event is attempted max_attempts times, then later events flow.
    emit("poison")
    emit("after")
    assert run("after", {"poison": 2}) == ["poison", "poison", "after"]


# ── async stores ─────────────────────────────────────────────────────────────


def test_live_async_stores(project: UdbProject) -> None:
    async def body() -> None:
        client = _async_project(project)
        try:
            table = AsyncTable(client, SPEC)
            repo = AsyncRepo(client, SPEC)
            entity = client.entity(WORKFLOW, key=["workflow_id"])
            fences: list[str] = []
            real = client.data.stub.Select

            def observing(request: Any, **kwargs: Any) -> Any:
                fences.append(request.context.read_fence_json)
                return real(request, **kwargs)

            client.data.stub.Select = observing
            assert await repo.get({"workflow_id": str(uuid.uuid4())}) is None
            assert fences == [""], "no write yet, so no fence"
            for step in range(REPEATS):
                row = workflow_row(project, current_step=step)
                await table.insert(row)
                key = {"workflow_id": row["workflow_id"]}
                assert (await table.get(key))["current_step"] == step
                assert '"max_wait_ms":2000' in fences[-1], f"the read carried no fence: {fences[-1]!r}"
                await table.patch(key, {"current_step": step + 1})
                with pytest.raises(UdbConflict):
                    await table.update_if({**row, "current_step": 9}, {"current_step": step})
                await entity.increment(key, {"total_steps": 1})
                page = await entity.select_page(key, include_total=True)
                assert page.exact_total == 1 and page.rows[0]["total_steps"] == 8
                await repo.update_guarded(key, {"current_step": 5}, {"current_step": step + 1})

            # A transaction on asyncio, with its negative case.
            files = AsyncTable(client, table_spec_of(file_pb2.File))
            for _ in range(REPEATS):
                fresh = _file(project)
                await client.tx(lambda tx, fresh=fresh: tx.upsert(fresh))
                assert (await files.get({"file_id": fresh.file_id}))["filename"] == "parity.pdf"

            first, second = _file(project), _file(project)
            first.object_key = second.object_key

            def collide(tx: Any) -> None:
                tx.insert(first)
                tx.insert(second)

            with pytest.raises(UdbRpcError) as refused:
                await client.tx(collide)
            assert is_unique(refused.value), repr(refused.value)
            with pytest.raises(UdbNotFound):
                await files.get({"file_id": first.file_id})
        finally:
            await client.close()

    asyncio.run(body())
