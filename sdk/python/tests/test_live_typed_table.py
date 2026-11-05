"""Generated typed tables against a real broker.

The code ``udb sdk generate --project-proto --lang python`` emits, driven through a
``UdbProject``. The TypeScript and Go live tests prove the same behaviour.

Skipped unless ``UDB_LIVE_SDK_TESTS=1``. ``UDB_PY_ENTITIES`` names the generated
module, so this runs what the generator really produced rather than a hand-written
copy of it.
"""

from __future__ import annotations

import importlib.util
import os
import uuid

import pytest

from udb_client.exceptions import UdbConflict, UdbNotFound, UdbRpcError
from udb_client.project import UdbConfig, UdbProject

pytestmark = pytest.mark.skipif(
    os.getenv("UDB_LIVE_SDK_TESTS") != "1",
    reason="requires a live UDB broker (set UDB_LIVE_SDK_TESTS=1)",
)


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required when UDB_LIVE_SDK_TESTS=1")
    return value


def _generated():
    spec = importlib.util.spec_from_file_location(
        "udb_entities_gen", _required_env("UDB_PY_ENTITIES")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _open(tenant_id: str) -> UdbProject:
    target = _required_env("UDB_GRPC_TARGET")
    return UdbProject(
        UdbConfig(
            target=target,
            auth_target=os.environ.get("UDB_AUTH_GRPC_TARGET", "").strip() or target,
            tenant_id=tenant_id,
            project_id=os.environ.get("UDB_LIVE_PROJECT", "default"),
        )
    )


def test_live_generated_typed_table_has_the_go_table_surface():
    generated = _generated()
    tenant_hint = os.environ.get("UDB_LIVE_TENANT", "sdk-live")

    project = _open(tenant_hint)
    try:
        # Adopts the canonical tenant UUID, which the record body must carry.
        authn = project.login_and_adopt_tenant(
            _required_env("UDB_LIVE_USERNAME"), _required_env("UDB_LIVE_PASSWORD")
        )
        tenant_id = authn.principal.tenant_id
        assert tenant_id, "the authenticated principal must carry a tenant"
        table = generated.WorkflowInstanceTable(project)
        repo = generated.WorkflowInstanceRepo(project)
        where = generated.WorkflowInstanceWhere
        workflow_type = f"py.live.typed-table.{uuid.uuid4()}"
        workflow_id = str(uuid.uuid4())
        key = {"workflow_id": workflow_id}
        payload = {"caseId": "nested_key_stays", "items": [{"a_b": 1}, {"a_b": 2}]}
        row = {
            "workflow_id": workflow_id,
            "tenant_id": tenant_id,
            "workflow_type": workflow_type,
            "status": "WORKFLOW_STATUS_COMPLETED",
            "current_step": 3,
            "total_steps": 7,
            "payload": payload,
            "compensations": [{"step_name": "undo"}],
            "last_error": "boom",
            "next_run_at": "2031-02-03T04:05:06Z",
        }

        assert table.message_type == "udb.core.workflow.entity.v1.WorkflowInstance"
        assert table.key_of(row) == key
        with pytest.raises(ValueError, match="primary-key column workflow_id"):
            table.key_of({"tenant_id": tenant_id})

        # insert creates, and a second insert never replaces the row.
        table.insert(row)
        with pytest.raises(UdbRpcError):
            table.insert({**row, "last_error": "replaced"})

        written = table.get(key)
        assert written["workflow_type"] == workflow_type
        assert written["status"] == "WORKFLOW_STATUS_COMPLETED", "a text enum maps back"
        assert written["current_step"] == 3
        assert written["last_error"] == "boom", "the failed insert replaced nothing"
        assert written["next_run_at"], "a timestamp reads back"
        assert written["payload"] == payload, "a JSONB object keeps its nested keys"
        assert written["compensations"] == [{"step_name": "undo"}]
        assert "pending_signal" not in written, "a NULL column is absent from the row"

        # upsert updates the same row; repeated, it stays one row.
        table.upsert({**row, "last_error": "again"})
        table.upsert({**row, "last_error": "again"})
        assert table.count({"workflow_type": workflow_type}) == 1

        # patch clears: a read cannot tell SQL NULL from the JSON document `null`,
        # because the row leaves both out, so `$is_null` is the proof.
        table.patch(key, {"payload": None, "next_run_at": None, "last_error": None})
        cleared = table.get(key)
        assert "payload" not in cleared, "patch with None must clear a JSONB column"
        assert "next_run_at" not in cleared, "patch with None must clear a TIMESTAMPTZ column"
        assert "last_error" not in cleared, "patch with None must clear a text column"
        assert cleared["compensations"] == [{"step_name": "undo"}]
        is_null = table.select(
            {"workflow_id": workflow_id, **where.payload.is_null()}, fields=["workflow_id"]
        )
        assert len(is_null.rows) == 1, "a cleared JSONB column must be SQL NULL"
        has_value = table.select(
            {"workflow_id": workflow_id, **where.compensations.is_null()}, fields=["workflow_id"]
        )
        assert len(has_value.rows) == 0, "the negative case: a column holding a value is not NULL"

        # increment is atomic and repeats.
        table.increment(key, {"current_step": 2})
        table.increment(key, {"current_step": 2})
        assert table.get(key)["current_step"] == 7

        # update_if writes on a match and is a conflict on a stale expectation.
        table.update_if({"workflow_id": workflow_id, "total_steps": 8}, {"current_step": 7})
        assert table.get(key)["total_steps"] == 8
        with pytest.raises(UdbConflict):
            table.update_if({"workflow_id": workflow_id, "total_steps": 9}, {"current_step": 1})
        assert table.get(key)["total_steps"] == 8, "a lost race writes nothing"

        # update_with_retry retries past a real conflict: the first attempt races.
        attempts = []

        def change(current):
            attempts.append(1)
            if len(attempts) == 1:
                table.increment(key, {"total_steps": 1})
            return {**current, "total_steps": current["total_steps"] + 10}

        updated = table.update_with_retry(key, change)
        assert len(attempts) == 2, "the conflict must cause exactly one retry"
        assert updated["total_steps"] == 19
        assert table.get(key)["total_steps"] == 19

        # select pages, sorts and totals; select_all walks every page.
        siblings = [str(uuid.uuid4()) for _ in range(3)]
        for index, sibling in enumerate(siblings):
            table.insert({**row, "workflow_id": sibling, "current_step": 100 + index})
        by_type = where.workflow_type.eq(workflow_type)
        first = table.select(by_type, limit=2, order_by=["-current_step"], include_total=True)
        assert first.total == 4
        assert first.has_more is True
        assert [r["current_step"] for r in first.rows] == [102, 101]
        second = table.select(
            by_type, limit=2, order_by=["-current_step"], page_token=first.next_page_token
        )
        assert [r["current_step"] for r in second.rows] == [100, 7]
        walked = []
        table.select_all(by_type, lambda r: walked.append(r["workflow_id"]))
        assert len(set(walked)) == 4
        assert table.count(where.current_step.gte(100)) >= 3

        # The predicates reach the broker.
        assert len(table.select(where.workflow_id.in_(*siblings)).rows) == 3
        assert (
            len(table.select({"workflow_type": workflow_type, **where.workflow_id.not_in(*siblings)}).rows)
            == 1
        )
        assert (
            len(table.select({"workflow_type": workflow_type, **where.current_step.between(100, 101)}).rows)
            == 2
        )

        # The repository: None on absence, and guarded writes need a precondition.
        assert repo.get({"workflow_id": workflow_id})["workflow_id"] == workflow_id
        assert repo.get({"workflow_id": str(uuid.uuid4())}) is None
        with pytest.raises(ValueError, match="missing from filter"):
            repo.get({"workflow_type": workflow_type})
        assert repo.find_first({"workflow_type": workflow_type})
        assert repo.find_first({"workflow_type": "absent"}) is None
        with pytest.raises(ValueError, match="expected precondition is empty"):
            repo.update_guarded(key, {"total_steps": 1}, {})
        repo.update_guarded(key, {"total_steps": 30}, {"total_steps": 19})
        with pytest.raises(UdbConflict):
            repo.update_guarded(key, {"total_steps": 40}, {"total_steps": 19})
        assert table.get(key)["total_steps"] == 30
        with pytest.raises(ValueError, match="expected precondition is empty"):
            repo.delete_guarded(key, {})
        with pytest.raises(UdbConflict):
            repo.delete_guarded(key, {"total_steps": 1})
        repo.delete_guarded(key, {"total_steps": 30})

        # A deleted row reads as absent, and an absent row is not found on each verb
        # that addresses one.
        with pytest.raises(UdbNotFound):
            table.get(key)
        absent = {"workflow_id": str(uuid.uuid4())}
        with pytest.raises(UdbNotFound):
            table.get(absent)
        with pytest.raises(UdbNotFound):
            table.patch(absent, {"last_error": "x"})
        with pytest.raises(UdbNotFound):
            table.delete(absent)
        with pytest.raises(UdbNotFound):
            table.increment(absent, {"current_step": 1})

        # An unknown column is refused, never dropped.
        with pytest.raises(ValueError, match="not_a_column"):
            table.patch({"workflow_id": siblings[0]}, {"not_a_column": 1})
        with pytest.raises(ValueError, match="not_a_column"):
            table.upsert({**row, "not_a_column": 1})
        with pytest.raises(ValueError, match="not_a_column"):
            table.select({"not_a_column": 1})

        for sibling in siblings:
            table.delete({"workflow_id": sibling})
        assert table.count({"workflow_type": workflow_type}) == 0
    finally:
        project.close()
