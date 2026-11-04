"""The behaviour contract a typed store must meet, written once and run against the
in-memory fake, an embedded broker and a live broker. A fake that drifts from the
broker fails the same test the broker passes, so a service's unit tests stay honest.

:func:`run_table` runs the contract, and :func:`table_contract` yields it case by case
for ``pytest.mark.parametrize``. :func:`run_table_cases` runs the cross-SDK file
``sdk/conformance/table-cases.json``, the same cases the Go and TypeScript runners read.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence
import uuid

import grpc

from udb.core.storage.entity.v1 import file_pb2
from udb.entity.v1 import types_pb2

from ..client import decode_records, to_record_json, to_struct
from ..error_code import ErrorCode, code_of, inspect, is_unique
from ..exceptions import UdbConflict, UdbNotFound, UdbRpcError
from ..generated_client import UdbDetailedRpcError
from ..record_codec import WRITE_INSERT, encode_record
from ..schema import message_type, primary_keys
from ..table import Table, table_spec_of
from ..tx import EVENT_ENVELOPE_VERSION

#: Returns a client for a tenant. A fresh tenant per call keeps runs isolated.
Connect = Callable[[str], Any]

_FILE_TYPE = message_type(file_pb2.File)
_SPEC = table_spec_of(file_pb2.File)


def new_id() -> str:
    """A random UUID: every tenant and row of a run is distinct, so runs against a
    shared live broker never collide."""
    return str(uuid.uuid4())


def _row(file_id: str, name: str) -> dict[str, Any]:
    # A row without a tenant: the broker fills the caller's verified tenant in.
    return {
        "file_id": file_id,
        "filename": name,
        "object_key": f"conformance/{file_id}",
        "status": "FILE_STATUS_PENDING",
    }


def _message(file_id: str, name: str) -> file_pb2.File:
    return file_pb2.File(
        file_id=file_id, filename=name, object_key=f"conformance/{file_id}",
        status="FILE_STATUS_PENDING",
    )


def _files(project: Any) -> Table:
    return Table(project, _SPEC)


def _expect_error(kind: Any, fn: Callable[[], Any], what: str) -> BaseException:
    try:
        fn()
    except kind as error:
        return error
    raise AssertionError(f"{what}: no error, want {kind}")


def _raises(fn: Callable[[], Any]) -> BaseException | None:
    try:
        fn()
    except Exception as error:  # noqa: BLE001 - the contract inspects what was raised.
        return error
    return None


def _expect_code(code: ErrorCode, fn: Callable[[], Any], what: str) -> BaseException:
    error = _raises(fn)
    assert code_of(error) is code, f"{what}: {error!r}, want {code}"
    assert error is not None
    return error


def _replays(first: Any, replay: Any) -> bool:
    if first.was_duplicate or not replay.was_duplicate:
        return False
    expected = type(first)()
    expected.CopyFrom(first)
    expected.was_duplicate = True
    return expected == replay


def _record_bytes(row: file_pb2.File, **changes: Any) -> bytes:
    record = encode_record(row, WRITE_INSERT)
    record.update(changes)
    return to_record_json(record)


# ── revisions ────────────────────────────────────────────────────────────────


def _case_revisions(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    u = connect(tenant())
    row = _message(new_id(), "revision-original.pdf")
    insert = types_pb2.UpsertRequest(message_type=_FILE_TYPE, record_json=_record_bytes(row))
    written = u.data.insert(insert)
    assert written.revision, "Insert omitted its committed revision"
    initial = written.revision
    filter_ = to_struct({"file_id": row.file_id})

    def read() -> str:
        result = u.data.select(
            types_pb2.SelectRequest(
                message_type=_FILE_TYPE, filter=filter_, fields=["filename"], include_revision=True
            )
        )
        assert len(result.records_json) == 1 and len(result.record_revisions) == 1 and result.record_revisions[0]
        assert decode_records(result)[0]["file_id"] == row.file_id, "revision read lacks the primary key"
        return result.record_revisions[0]

    assert read() == initial, "served revision differs from Insert"

    def update(**overrides: Any) -> types_pb2.UpdateRequest:
        fields = dict(
            message_type=_FILE_TYPE, filter=filter_, changes=to_struct({"filename": "revision-updated.pdf"}),
            expected_revision=initial, require_affected=1,
        )
        fields.update(overrides)
        return types_pb2.UpdateRequest(**fields)

    keyed = update(idempotency_key=new_id())
    first = u.data.update(keyed)
    assert first.revision and first.revision != initial and read() == first.revision
    replay = u.data.update(keyed)
    assert _replays(first, replay) and read() == first.revision, "retry rechecked or bumped its revision"
    for changed in (
        update(idempotency_key=keyed.idempotency_key, expected_revision=first.revision),
        update(idempotency_key=keyed.idempotency_key, require_affected=2),
    ):
        info = inspect(_raises(lambda request=changed: u.data.update(request)))
        assert info.reason == "UDB_IDEMPOTENCY_REUSE" and info.code is ErrorCode.PRECONDITION
        assert read() == first.revision

    def assert_refusal(request: types_pb2.UpdateRequest, reason: str) -> None:
        error = _raises(lambda: u.data.update(request))
        assert inspect(error).reason == reason, f"{error!r}, want {reason}"
        assert isinstance(error, UdbDetailedRpcError) and error.field_violations, "no field violation"
        assert read() == first.revision, "refused Update changed the revision"

    assert_refusal(update(), "UDB_REVISION_CONFLICT")
    assert_refusal(
        update(filter=to_struct({"filename": "revision-updated.pdf"}), expected_revision=first.revision),
        "UDB_CAS_KEY_NOT_PK",
    )
    _expect_code(ErrorCode.INVALID, lambda: u.data.update(update(expected_revision="invalid-revision")), "malformed revision")
    delete = types_pb2.DeleteRequest(message_type=_FILE_TYPE, filter=filter_, expected_revision=initial, require_affected=1)
    assert inspect(_raises(lambda: u.data.delete(delete))).reason == "UDB_REVISION_CONFLICT"
    key = {"file_id": row.file_id}
    u.tx(lambda tx: tx.patch(file_pb2.File, key, {"filename": "revision-transaction.pdf"}))
    transaction_revision = read()
    assert transaction_revision != first.revision, "BeginTx Update left the old revision valid"
    assert inspect(_raises(lambda: u.data.update(update(expected_revision=first.revision)))).reason == "UDB_REVISION_CONFLICT"

    def rolled_back(tx: Any) -> None:
        tx.patch(file_pb2.File, key, {"filename": "must-roll-back.pdf"})
        tx.patch(file_pb2.File, key, {"file_id": row.file_id})

    error = _raises(lambda: u.tx(rolled_back))
    assert code_of(error) is ErrorCode.INVALID and read() == transaction_revision, repr(error)
    assert _files(u).get(key)["filename"] == "revision-transaction.pdf"
    delete_now = types_pb2.DeleteRequest(
        message_type=_FILE_TYPE, filter=filter_, expected_revision=transaction_revision, require_affected=1
    )
    u.data.delete(delete_now)
    # File is soft-deleted. Upsert restores the same primary key; an Insert would
    # correctly refuse the tombstone's unique constraint.
    written = u.data.upsert(insert)
    assert written.revision and written.revision != transaction_revision, "delete/reinsert revived a revision"
    assert inspect(_raises(lambda: u.data.update(update(expected_revision=transaction_revision)))).reason == "UDB_REVISION_CONFLICT"
    plain = u.data.select(types_pb2.SelectRequest(message_type=_FILE_TYPE, filter=filter_))
    assert not plain.record_revisions, "default Select unexpectedly included revisions"


# ── constraints ──────────────────────────────────────────────────────────────


def _case_update_admission(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    u = connect(tenant())
    files = _files(u)
    row = _row(new_id(), "immutable-original.pdf")
    files.insert(row)
    key = {"file_id": row["file_id"]}
    assert files.get(key).get("project_id", "") == u.metadata.project_id, "stored project differs from verified"
    values = {"file_id": row["file_id"], "tenant_id": u.metadata.tenant_id, "project_id": u.metadata.project_id}
    for column, value in values.items():
        # Even resending the current identity is forbidden by Update's admission
        # contract; the setter must never silently ignore it.
        changes = {column: value, "filename": "must-not-change.pdf"}
        _expect_code(ErrorCode.INVALID, lambda: files.patch(key, changes), f"immutable {column} update")
        prefix = _message(new_id(), "must-roll-back.pdf")

        def transaction(tx: Any) -> None:
            tx.insert(prefix)
            tx.patch(file_pb2.File, key, changes)

        _expect_code(ErrorCode.INVALID, lambda: u.tx(transaction), f"immutable {column} transaction")
        _expect_error(UdbNotFound, lambda: files.get({"file_id": prefix.file_id}), "refusal committed its prefix")
    changes = to_struct({"size_bytes": "1"})
    filter_ = to_struct(key)
    with_both = types_pb2.UpdateRequest(message_type=_FILE_TYPE, filter=filter_, changes=changes)
    with_both.increments.add(column="size_bytes", delta=2)
    for request in (
        types_pb2.UpdateRequest(message_type=_FILE_TYPE, filter=filter_),
        types_pb2.UpdateRequest(message_type=_FILE_TYPE, changes=changes),
        with_both,
    ):
        _expect_code(ErrorCode.INVALID, lambda request=request: u.data.update(request), "invalid Update shape")
    got = files.get(key)
    assert got["filename"] == row["filename"] and got.get("size_bytes", 0) == 0, "refused update changed its target"


def _case_keyed_replay(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    u = connect(tenant())
    row = _message(new_id(), "keyed-original.pdf")
    insert = types_pb2.UpsertRequest(message_type=_FILE_TYPE, record_json=_record_bytes(row), idempotency_key=new_id())
    first = u.data.insert(insert)
    assert _replays(first, u.data.insert(insert)), "keyed insert did not replay its original receipt"
    # Insert and Upsert are distinct operations even when their key and target match:
    # an Upsert must replace the row, not replay the Insert.
    upsert = types_pb2.UpsertRequest()
    upsert.CopyFrom(insert)
    upsert.record_json = _record_bytes(row, filename="keyed-upserted.pdf")
    first = u.data.upsert(upsert)
    assert not first.was_duplicate, "Insert and Upsert collided in the key namespace"
    assert _replays(first, u.data.upsert(upsert)), "keyed upsert did not replay its original receipt"
    filter_ = to_struct({"file_id": row.file_id})
    update = types_pb2.UpdateRequest(
        message_type=_FILE_TYPE, filter=filter_, expected=to_struct({"size_bytes": "0"}),
        require_affected=1, idempotency_key=new_id(),
    )
    update.increments.add(column="size_bytes", delta=7)
    first = u.data.update(update)
    assert _replays(first, u.data.update(update)), "keyed update replay checked the changed CAS value"
    changed = types_pb2.UpdateRequest()
    changed.CopyFrom(update)
    changed.increments[0].delta = 11
    info = inspect(_raises(lambda: u.data.update(changed)))
    assert info.reason == "UDB_IDEMPOTENCY_REUSE" and info.code is ErrorCode.PRECONDITION and info.fix_hint
    files = _files(u)
    got = files.get({"file_id": row.file_id})
    assert got["size_bytes"] == 7 and got["filename"] == "keyed-upserted.pdf", "increment ran twice or reuse wrote"
    delete = types_pb2.DeleteRequest(message_type=_FILE_TYPE, filter=filter_, require_affected=1, idempotency_key=new_id())
    first = u.data.delete(delete)
    assert _replays(first, u.data.delete(delete)), "keyed delete replay checked the deleted row"
    _expect_error(UdbNotFound, lambda: files.get({"file_id": row.file_id}), "keyed delete left a visible row")


def _case_explicit_conflict_target(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    u = connect(tenant())
    row = _message(new_id(), "original.pdf")
    files = _files(u)
    files.insert(_row(row.file_id, "original.pdf"))
    u.data.upsert(
        types_pb2.UpsertRequest(
            message_type=_FILE_TYPE,
            record_json=_record_bytes(row, filename="replaced.pdf"),
            conflict_fields=primary_keys(row),
        )
    )
    assert files.get({"file_id": row.file_id})["filename"] == "replaced.pdf"


def _case_tx_idempotency_key(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    u = connect(tenant())
    # Relational upsert/update/delete keys may be honoured inside a transaction;
    # insert and outbox enqueue have no receipt, so a key on them refuses the batch.
    for operation in ("insert", "enqueue_outbox_event"):
        row = _message(new_id(), "must-not-commit.pdf")
        raw = _record_bytes(row)
        keyed = types_pb2.Mutation(operation=operation, message_type=_FILE_TYPE, idempotency_key=new_id())
        if operation == "insert":
            keyed.record_json = raw
        else:
            keyed.collection, keyed.object_key = "udb.conformance.v1", row.file_id
            keyed.payload.CopyFrom(
                to_struct(
                    {
                        "event_id": new_id(), "event_type": "conformance", "document_id": row.file_id,
                        "correlation_id": new_id(), "envelope_version": EVENT_ENVELOPE_VERSION, "payload": {},
                    }
                )
            )
        frames = [
            types_pb2.Mutation(operation="upsert", message_type=_FILE_TYPE, record_json=raw),
            keyed,
            types_pb2.Mutation(commit=True),
        ]
        details: list[Any] = []
        stream = u.data.stub.BeginTx(iter(frames), metadata=u.data._call_metadata(None), timeout=10.0)
        try:
            for frame in stream:
                details.append(frame.error_detail)
        except grpc.RpcError as error:
            mapped = _mapped(error)
            if mapped.error_detail is not None:
                details.append(mapped.error_detail)
        refused = any(
            violation.field == "mutations[1].idempotency_key"
            for detail in details
            for violation in detail.field_violations
        )
        assert refused, f"ignored key was not explicitly refused: {details}"
        _expect_error(UdbNotFound, lambda: _files(u).get({"file_id": row.file_id}), "refused transaction kept its prefix")


def _mapped(error: grpc.RpcError) -> UdbRpcError:
    from ..error_code import map_rpc_error

    return map_rpc_error("BeginTx", error)


def _case_secondary_uniqueness(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    u = connect(tenant())
    files = _files(u)
    first, second = _row(new_id(), "first.pdf"), _row(new_id(), "second.pdf")
    files.insert(first)
    files.insert(second)
    duplicate = _row(new_id(), "duplicate.pdf")
    duplicate["object_key"] = first["object_key"]
    error = _raises(lambda: files.insert(duplicate))
    assert is_unique(error) and inspect(error).constraint, f"secondary duplicate: {error!r}"
    filter_ = {"file_id": {"$in": [first["file_id"], second["file_id"]]}}
    changes = {"object_key": f"conformance/shared/{new_id()}", "filename": "must-rollback.pdf"}
    error = _raises(lambda: u.data.update(message_type=_FILE_TYPE, filter=filter_, changes=changes))
    assert is_unique(error), f"multirow collision: {error!r}"
    for original in (first, second):
        got = files.get({"file_id": original["file_id"]})
        assert got["filename"] == original["filename"] and got["object_key"] == original["object_key"]
    fresh = _message(new_id(), "tx-prefix.pdf")
    duplicate_message = file_pb2.File(file_id=duplicate["file_id"], filename="duplicate.pdf", object_key=first["object_key"], status="FILE_STATUS_PENDING")

    def transaction(tx: Any) -> None:
        tx.insert(fresh)
        tx.insert(duplicate_message)

    error = _raises(lambda: u.tx(transaction))
    assert is_unique(error), f"transaction secondary collision: {error!r}"
    _expect_error(UdbNotFound, lambda: files.get({"file_id": fresh.file_id}), "unique refusal kept its prefix")


def _case_not_null(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    u = connect(tenant())
    raw = json.dumps({"file_id": new_id(), "filename": None, "object_key": f"conformance/{new_id()}"}).encode()
    error = _raises(lambda: u.data.upsert(types_pb2.UpsertRequest(message_type=_FILE_TYPE, record_json=raw)))
    info = inspect(error)
    assert info.code is ErrorCode.NOT_NULL and info.column == "filename", f"NULL refusal: {error!r} ({info})"


def _case_exact_int64(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    files = _files(connect(tenant()))
    row = _row(new_id(), "large.pdf")
    row["size_bytes"] = 9007199254740993
    files.insert(row)
    key = {"file_id": row["file_id"]}
    assert files.get(key)["size_bytes"] == row["size_bytes"], "rounded stored int64"
    files.increment(key, {"size_bytes": 1})
    assert files.get(key)["size_bytes"] == row["size_bytes"] + 1, "rounded increment"


def _case_soft_delete(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    files = _files(connect(tenant()))
    row = _row(new_id(), "deleted.pdf")
    files.insert(row)
    key = {"file_id": row["file_id"]}
    files.delete(key)
    _expect_error(UdbNotFound, lambda: files.get(key), "tombstone remained visible")
    assert is_unique(_raises(lambda: files.insert(row))), "strict insert reused a tombstone key"
    row["filename"] = "revived.pdf"
    files.upsert(row)
    got = files.get(key)
    assert got["filename"] == "revived.pdf" and "deleted_at" not in got, "upsert did not revive tombstone"


def _case_default_page(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    u = connect(tenant())
    files = _files(u)
    rows = [_message(new_id(), f"page-{i:03d}.pdf") for i in range(103)]

    def transaction(tx: Any) -> None:
        for row in rows:
            tx.insert(row)

    u.tx(transaction)
    filter_ = {"file_id": {"$in": [row.file_id for row in rows]}}
    page = files.select(filter_, include_total=True)
    assert len(page.rows) == 100 and page.has_more and page.total == 103, page
    seen: list[str] = []
    files.select_all(filter_, lambda row: seen.append(row["file_id"]))
    assert len(seen) == 103 and len(set(seen)) == 103, f"select_all visited {len(seen)} rows"


# ── queries ──────────────────────────────────────────────────────────────────


def _case_unknown_columns(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    u = connect(tenant())
    filter_ = {"file_id": new_id(), "$or": [{"not_a_declared_column": "value"}]}
    error = _raises(lambda: u.data.select(message_type=_FILE_TYPE, filter=filter_))
    assert isinstance(error, UdbRpcError) and error.code == grpc.StatusCode.INVALID_ARGUMENT, repr(error)


def _case_three_valued_logic(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    u = connect(tenant())
    files = _files(u)
    ids = [new_id(), new_id(), new_id()]
    for file_id, content_type in zip(ids, (None, "blocked/type", "allowed/type")):
        payload = {
            "file_id": file_id, "filename": "nullable.pdf",
            "object_key": f"conformance/{file_id}", "content_type": content_type,
        }
        u.data.upsert(types_pb2.UpsertRequest(message_type=_FILE_TYPE, payload=to_struct(payload)))
    cases: Sequence[tuple[Mapping[str, Any], Sequence[str]]] = (
        ({"$ne": "blocked/type"}, ids[2:]),
        ({"$nin": ["blocked/type"]}, ids[2:]),
        ({"$not": {"$eq": "blocked/type"}}, ids[2:]),
        ({"$not": {"$not": {"$eq": "allowed/type"}}}, ids[2:]),
        ({"$is_null": False}, ids[1:]),
        ({"$not": {"$not_null": True}}, ids[:1]),
    )
    for predicate, want in cases:
        page = files.select({"file_id": {"$in": ids}, "content_type": predicate})
        got = {row["file_id"] for row in page.rows}
        assert got == set(want), f"{predicate}: selected {got}, want {set(want)}"


# ── typed store ──────────────────────────────────────────────────────────────


def _case_get_missing(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    _expect_error(UdbNotFound, lambda: _files(connect(tenant())).get({"file_id": new_id()}), "get of a missing row")


def _case_round_trip(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    files = _files(connect(tenant()))
    file_id = new_id()
    files.upsert(_row(file_id, "a.pdf"))
    got = files.get({"file_id": file_id})
    assert got["filename"] == "a.pdf" and got["status"] == "FILE_STATUS_PENDING" and got.get("tenant_id"), got


def _case_insert_duplicate(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    files = _files(connect(tenant()))
    file_id = new_id()
    files.insert(_row(file_id, "original.pdf"))
    assert is_unique(_raises(lambda: files.insert(_row(file_id, "replacement.pdf"))))
    assert files.get({"file_id": file_id})["filename"] == "original.pdf"


def _case_concurrent_inserts(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    files = _files(connect(tenant()))
    file_id = new_id()
    names = ["a.pdf", "b.pdf"]
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda name: _raises(lambda: files.insert(_row(file_id, name))), names))
    winners = [name for name, error in zip(names, results) if error is None]
    assert len(winners) == 1, f"winners={winners}"
    assert all(is_unique(error) for error in results if error is not None), results
    assert files.get({"file_id": file_id})["filename"] == winners[0], "winner was overwritten"


def _case_tx_duplicate(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    u = connect(tenant())
    files = _files(u)
    existing, fresh = new_id(), new_id()
    files.insert(_row(existing, "original.pdf"))

    def transaction(tx: Any) -> None:
        tx.insert(_message(fresh, "new.pdf"))
        tx.insert(_message(existing, "replacement.pdf"))

    assert is_unique(_raises(lambda: u.tx(transaction)))
    _expect_error(UdbNotFound, lambda: files.get({"file_id": fresh}), "transaction left an earlier insert")
    assert files.get({"file_id": existing})["filename"] == "original.pdf"
    u.tx(lambda tx: tx.insert(_message(fresh, "committed.pdf")))
    assert files.get({"file_id": fresh})["filename"] == "committed.pdf"


def _case_tenant_isolation(connect: Connect, tenant: Callable[[], str], peer: str) -> None:
    a = tenant()
    b = peer or tenant()
    files_a, files_b = _files(connect(a)), _files(connect(b))
    file_id = new_id()
    files_a.upsert(_row(file_id, "secret.pdf"))
    _expect_error(UdbNotFound, lambda: files_b.get({"file_id": file_id}), "another tenant read the row")
    assert _raises(lambda: files_b.select({"tenant_id": a})) is not None, "naming another tenant must be refused"
    assert _raises(lambda: files_b.upsert(_row(file_id, "must-not-overwrite.pdf"))) is not None, (
        "another tenant upserted a stored global primary key"
    )
    assert files_a.get({"file_id": file_id})["filename"] == "secret.pdf", "foreign write changed original"


def _case_patch(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    files = _files(connect(tenant()))
    file_id = new_id()
    files.upsert(_row(file_id, "draft.pdf"))
    files.patch({"file_id": file_id}, {"filename": "final.pdf"})
    got = files.get({"file_id": file_id})
    assert got["filename"] == "final.pdf" and got["object_key"] == f"conformance/{file_id}", got
    _expect_error(UdbNotFound, lambda: files.patch({"file_id": new_id()}, {"filename": "x"}), "patching a missing row")


def _case_conditional_update(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    files = _files(connect(tenant()))
    file_id = new_id()
    files.upsert(_row(file_id, "v1.pdf"))
    files.update_if(_row(file_id, "v2.pdf"), {"filename": "v1.pdf"})
    _expect_error(UdbConflict, lambda: files.update_if(_row(file_id, "v3.pdf"), {"filename": "v1.pdf"}), "a stale expectation")
    assert files.get({"file_id": file_id})["filename"] == "v2.pdf", "a lost update was applied"


def _case_update_with_retry(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    files = _files(connect(tenant()))
    file_id = new_id()
    files.upsert(_row(file_id, "n.pdf"))

    def rename(current: dict[str, Any]) -> dict[str, Any]:
        current["filename"] = "renamed-" + current["filename"]
        return current

    updated = files.update_with_retry({"file_id": file_id}, rename, 3)
    assert updated["filename"] == "renamed-n.pdf", updated


def _case_paging(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    files = _files(connect(tenant()))
    ids: list[str] = []
    for i in range(5):
        ids.append(new_id())
        files.upsert(_row(ids[-1], f"p{i}.pdf"))
    # A live login shares its tenant with the other cases and previous runs, so count
    # and pagination must cover this case's five rows only.
    filter_: dict[str, Any] = {"file_id": {"$in": ids}}
    page = files.select(filter_, limit=2, include_total=True)
    assert len(page.rows) == 2 and page.has_more and page.total == 5, page
    seen: list[Any] = []
    files.select_all(filter_, seen.append)
    assert len(seen) == 5, f"select_all saw {len(seen)} rows"
    filter_["filename"] = {"$in": ["p1.pdf", "p3.pdf"]}
    assert files.count(filter_) == 2


def _case_tx_atomic(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    u = connect(tenant())
    files = _files(u)
    existing, fresh = new_id(), new_id()
    files.upsert(_row(existing, "v1.pdf"))

    def lost(tx: Any) -> None:
        tx.upsert(_message(fresh, "new.pdf"))
        tx.update_if(_message(existing, "v2.pdf"), {"filename": "stale.pdf"})

    _expect_error(UdbConflict, lambda: u.tx(lost), "a transaction with a lost CAS")
    _expect_error(UdbNotFound, lambda: files.get({"file_id": fresh}), "the failed transaction's other write was kept")

    def held(tx: Any) -> None:
        tx.upsert(_message(fresh, "new.pdf"))
        tx.update_if(_message(existing, "v2.pdf"), {"filename": "v1.pdf"})

    u.tx(held)
    assert files.get({"file_id": existing})["filename"] == "v2.pdf"
    files.get({"file_id": fresh})


def _case_delete(connect: Connect, tenant: Callable[[], str], _: Any) -> None:
    files = _files(connect(tenant()))
    file_id = new_id()
    files.upsert(_row(file_id, "gone.pdf"))
    files.delete({"file_id": file_id})
    _expect_error(UdbNotFound, lambda: files.delete({"file_id": file_id}), "deleting it again")


_CONTRACT: tuple[tuple[str, Callable[[Connect, Callable[[], str], Any], None]], ...] = (
    ("opaque revisions reject stale writes and survive transaction rollback", _case_revisions),
    ("update admission preserves identity and rolls back transaction prefixes", _case_update_admission),
    ("unary keys replay before CAS and reject changed input", _case_keyed_replay),
    ("explicit primary conflict target replaces the original row", _case_explicit_conflict_target),
    ("unsupported transaction idempotency key refuses the entire batch", _case_tx_idempotency_key),
    ("secondary uniqueness refuses insert and rolls back a multirow update", _case_secondary_uniqueness),
    ("NOT NULL refusal names the column", _case_not_null),
    ("int64 stays exact through storage and an atomic increment", _case_exact_int64),
    ("soft delete retains unique keys and upsert revives the row", _case_soft_delete),
    ("default page caps at 100 and select_all visits every row", _case_default_page),
    ("unknown columns are refused even without matching rows", _case_unknown_columns),
    ("nullable predicates preserve SQL three-valued logic", _case_three_valued_logic),
    ("get missing is not found", _case_get_missing),
    ("upsert then get round-trips the typed row", _case_round_trip),
    ("insert refuses a duplicate without changing the stored row", _case_insert_duplicate),
    ("two concurrent inserts have exactly one winner", _case_concurrent_inserts),
    ("transaction insert duplicate rolls back every earlier write", _case_tx_duplicate),
    ("tenants never see each other's rows", _case_tenant_isolation),
    ("patch changes only the given columns and needs the row", _case_patch),
    ("conditional update detects a lost race", _case_conditional_update),
    ("update with retry applies the change once", _case_update_with_retry),
    ("select pages, says when capped, and counts", _case_paging),
    ("a transaction commits every write or none", _case_tx_atomic),
    ("delete needs the row", _case_delete),
)


def _tenant_source(tenants: Sequence[str] | None) -> tuple[Callable[[], str], str]:
    if tenants is None:
        return new_id, ""
    if len(tenants) != 2 or not tenants[0] or not tenants[1] or tenants[0] == tenants[1]:
        raise ValueError("conformance requires two distinct provisioned tenants")
    return (lambda: tenants[0]), tenants[1]


def table_contract(
    connect: Connect, tenants: Sequence[str] | None = None
) -> Iterator[tuple[str, Callable[[], None]]]:
    """Each contract case as ``(name, run)``. ``tenants`` selects two provisioned
    canonical tenant UUIDs; without it, fresh tenants are used. Cross-tenant cases
    never skip."""
    tenant, peer = _tenant_source(tenants)
    for name, case in _CONTRACT:
        yield name, (lambda case=case: case(connect, tenant, peer))


def run_table(connect: Connect, tenants: Sequence[str] | None = None) -> None:
    """Run the typed-store contract. The first broken case raises ``AssertionError``
    naming it."""
    for name, run in table_contract(connect, tenants):
        try:
            run()
        except Exception as error:
            raise AssertionError(f"conformance case {name!r}: {error!r}") from error


def _substitute(values: Mapping[str, Any] | None, file_id: str) -> dict[str, Any]:
    return {
        key: value.replace("$id", file_id) if isinstance(value, str) else value
        for key, value in (values or {}).items()
    }


def _error_name(error: BaseException | None) -> str:
    return "" if error is None else str(code_of(error))


def load_table_cases(path: str | Path) -> list[dict[str, Any]]:
    """The cases of the shared ``table-cases.json`` file."""
    return json.loads(Path(path).read_text(encoding="utf-8"))["cases"]


def run_table_case(connect: Connect, case: Mapping[str, Any], tenant: str = "") -> None:
    """Run one case of the shared file against ``connect``."""
    files = _files(connect(tenant or new_id()))
    file_id = new_id()
    for index, step in enumerate(case["steps"]):
        op = step["op"]
        key = _substitute(step.get("key"), file_id)
        got: dict[str, Any] | None = None
        error: BaseException | None = None
        try:
            if op == "upsert":
                files.upsert(_substitute(step.get("row"), file_id))
            elif op == "get":
                got = files.get(key)
            elif op == "patch":
                files.patch(key, _substitute(step.get("changes"), file_id))
            elif op == "update_if":
                current = dict(files.get(key))
                current.update(_substitute(step.get("changes"), file_id))
                files.update_if(current, _substitute(step.get("expected"), file_id))
            elif op == "delete":
                files.delete(key)
            else:
                raise AssertionError(f"step {index}: unknown op {op!r}")
        except AssertionError:
            raise
        except Exception as caught:  # noqa: BLE001 - classified against the step's expectation.
            error = caught
        expect = step.get("expect", {})
        name = _error_name(error)
        want = expect.get("error", "")
        assert name == want, f"{case['name']} step {index} ({op}): error {name!r} ({error!r}), want {want!r}"
        if got is not None and expect.get("row") is not None:
            record = _SPEC.to_fields(got)
            for column, value in _substitute(expect["row"], file_id).items():
                assert record.get(column) == value, f"{case['name']} step {index}: {column} = {record.get(column)!r}, want {value!r}"


def run_table_cases(connect: Connect, path: str | Path, tenants: Sequence[str] | None = None) -> None:
    """Run every case in the shared JSON file against ``connect``."""
    tenant, _ = _tenant_source(tenants)
    for case in load_table_cases(path):
        run_table_case(connect, case, tenant())
