"""``tx(..., replay_key=...)`` on the client and project facades, sync and async, matches
Go ``TxWithKey`` and the TypeScript ``replayKey`` option."""

from __future__ import annotations

import asyncio
from typing import Any
import uuid

import pytest

from udb.core.storage.entity.v1 import file_pb2
from udb_client import ErrorCode, inspect, udbtest
from udb_client import tx as tx_module
from udb_client.client import UdbAsyncClient
from udb_client.project import UdbAsyncProject
from udb_client.udbtest import conformance


def _file(name: str, file_id: str | None = None) -> file_pb2.File:
    file_id = file_id or str(uuid.uuid4())
    return file_pb2.File(file_id=file_id, filename=name, object_key=f"k/{file_id}")


def test_project_tx_replay_key_commits_once_and_skips_require_affected_on_replay() -> None:
    with udbtest.new(file_pb2.File) as fake:
        u = fake.client(str(uuid.uuid4()))
        target = _file("doomed.pdf")
        u.tx(lambda tx: tx.insert(target))
        key = {"file_id": target.file_id}
        added = _file("added.pdf")

        def body(tx: Any) -> None:
            tx.upsert(added)
            # Delete sends require_affected=1: a replay must not recheck it, or the
            # retry of a committed delete would be refused as a missed row.
            tx.delete(file_pb2.File, key)

        replay_key = str(uuid.uuid4())
        u.tx(body, replay_key=replay_key)
        u.tx(body, replay_key=replay_key)
        assert conformance._files(u).get({"file_id": added.file_id})["filename"] == "added.pdf"

        # Without a key the same retry is a fresh delete of a missing row: refused,
        # and the upsert in front of it rolls back.
        fresh = _file("fresh.pdf")

        def unkeyed(tx: Any) -> None:
            tx.upsert(fresh)
            tx.delete(file_pb2.File, key)

        with pytest.raises(Exception) as refused:
            u.tx(unkeyed)
        info = inspect(refused.value)
        assert info.code is ErrorCode.NOT_FOUND and info.reason == "UDB_NO_ROWS_AFFECTED", repr(refused.value)
        with pytest.raises(Exception) as missing:
            conformance._files(u).get({"file_id": fresh.file_id})
        assert inspect(missing.value).code is ErrorCode.NOT_FOUND

        # The same key with different writes is refused, not replayed.
        with pytest.raises(Exception) as reused:
            u.tx(lambda tx: tx.upsert(_file("other.pdf", added.file_id)), replay_key=replay_key)
        assert inspect(reused.value).reason == "UDB_IDEMPOTENCY_REUSE", repr(reused.value)


def test_async_tx_passes_replay_key(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    async def fake_run_tx_async(client: Any, fn: Any, *, metadata: Any = None, timeout: Any = None, replay_key: str = "") -> None:
        seen.append(replay_key)

    monkeypatch.setattr(tx_module, "run_tx_async", fake_run_tx_async)
    client = object.__new__(UdbAsyncClient)
    asyncio.run(client.tx(lambda tx: None, replay_key="k-1"))
    asyncio.run(client.tx(lambda tx: None))

    project = object.__new__(UdbAsyncProject)
    project.data = client
    asyncio.run(project.tx(lambda tx: None, replay_key="k-2"))
    assert seen == ["k-1", "", "k-2"]
