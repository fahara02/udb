"""The typed-store contract against the in-memory fake: the Go contract ported case by
case, plus the shared ``sdk/conformance/table-cases.json`` that every SDK runner reads."""

from __future__ import annotations

import json
from pathlib import Path
import uuid

import pytest

from udb.core.storage.entity.v1 import file_pb2
from udb.core.workflow.entity.v1 import workflow_instance_pb2
from udb.entity.v1 import types_pb2
from udb_client import ErrorCode, UdbRpcError, inspect, udbtest
from udb_client.udbtest import conformance

CASES_FILE = Path(__file__).resolve().parents[2] / "conformance" / "table-cases.json"


@pytest.fixture
def connect():
    with udbtest.new(file_pb2.File) as fake:
        yield fake.client


@pytest.mark.parametrize("index", range(len(conformance._CONTRACT)), ids=[name for name, _ in conformance._CONTRACT])
def test_table_contract(connect, index: int) -> None:
    list(conformance.table_contract(connect))[index][1]()


@pytest.mark.parametrize("case", conformance.load_table_cases(CASES_FILE), ids=lambda case: case["name"])
def test_shared_table_cases(connect, case) -> None:
    conformance.run_table_case(connect, case)


def test_run_table_runs_the_whole_contract(connect) -> None:
    conformance.run_table(connect)
    conformance.run_table_cases(connect, CASES_FILE)


def test_run_table_reports_a_store_that_breaks_the_contract() -> None:
    class Broken:
        """Connects to nothing, so every case must fail rather than pass quietly."""

        def __call__(self, tenant: str):
            raise RuntimeError(f"no broker for {tenant}")

    with pytest.raises(AssertionError, match="conformance case"):
        conformance.run_table(Broken())


def test_conformance_refuses_tenants_that_are_not_two_distinct() -> None:
    with pytest.raises(ValueError):
        list(conformance.table_contract(lambda tenant: None, ("same", "same")))


def test_select_many_reads_several_entities_in_request_order() -> None:
    """``select_many`` against the fake's BatchSelect: several reads across two
    entities in one call, results in request order, and one bad read fails it."""
    file_type = "udb.core.storage.entity.v1.File"
    workflow = "udb.core.workflow.entity.v1.WorkflowInstance"
    file_a, file_b, flow = (str(uuid.uuid4()) for _ in range(3))
    with udbtest.new(file_pb2.File, workflow_instance_pb2.WorkflowInstance) as fake:
        data = fake.client(str(uuid.uuid4())).data
        data.upsert(message_type=file_type, record={"file_id": file_a, "filename": "a.pdf", "object_key": f"many/{file_a}"})
        data.upsert(message_type=file_type, record={"file_id": file_b, "filename": "b.pdf", "object_key": f"many/{file_b}"})
        data.upsert(message_type=workflow, record={"workflow_id": flow, "workflow_type": "many"})

        def read(message_type: str, where: dict, limit: int = 0, sort: tuple = ()) -> types_pb2.SelectRequest:
            request = types_pb2.SelectRequest(message_type=message_type, limit=limit, sort=list(sort))
            request.filter.update(where)
            return request

        def column(result: types_pb2.RecordSet, name: str) -> list:
            return [json.loads(raw)[name] for raw in result.records_json]

        for _ in range(3):
            results = data.select_many([
                read(file_type, {"file_id": file_b}),
                read(workflow, {"workflow_id": flow}),
                read(file_type, {"file_id": file_a}),
                read(file_type, {"file_id": {"$in": [file_a, file_b]}}, 1, (types_pb2.Sort(field="filename", descending=True),)),
                read(workflow, {"workflow_id": str(uuid.uuid4())}),
            ])
            assert len(results) == 5
            assert column(results[0], "filename") == ["b.pdf"], "results keep request order"
            assert column(results[1], "workflow_type") == ["many"]
            assert column(results[2], "filename") == ["a.pdf"]
            assert column(results[3], "filename") == ["b.pdf"] and results[3].has_more
            assert len(results[4].records_json) == 0, "a read matching nothing returns no rows"

        with pytest.raises(UdbRpcError) as refused:
            data.select_many([read(file_type, {}), read("udb.nope.v1.Missing", {})])
        assert inspect(refused.value).code is ErrorCode.INVALID
        assert data.select_many([]) == [], "no reads, no call"


def _ids(data, message_type: str, key: str, where: dict) -> list[str]:
    result = data.select(message_type=message_type, filter=where)
    return sorted(json.loads(raw)[key][-3:] for raw in result.records_json)


def test_array_filters_on_an_array_column() -> None:
    """The broker's ``$contains`` (@>), ``$contained_by`` (<@) and ``$overlaps``/``$overlap``
    (&&) on an array (repeated) column."""
    from udb.core.authz.entity.v1 import access_decision_audit_pb2

    audit = "udb.core.authz.entity.v1.AccessDecisionAudit"
    tenant = str(uuid.uuid4())
    with udbtest.new(access_decision_audit_pb2.AccessDecisionAudit) as fake:
        data = fake.client(tenant).data
        for suffix, matched in (("101", ["p1", "p2"]), ("102", ["p2", "p3"]), ("103", [])):
            data.upsert(message_type=audit, record={
                "decision_audit_id": f"0190f1b2-0000-4000-8000-000000000{suffix}", "user_id": tenant,
                "domain": "d", "object": "o", "action": "read", "effect": "POLICY_EFFECT_ALLOW",
                "decision_source": "DECISION_SOURCE_ROLE_POLICY", "tenant_id": tenant,
                "matched_policy_ids": matched,
            })

        def ids(op: str, arg) -> list[str]:
            return _ids(data, audit, "decision_audit_id", {"matched_policy_ids": {op: arg}})

        assert ids("$contains", ["p2"]) == ["101", "102"]
        assert ids("$contains", ["p1", "p2"]) == ["101"]
        assert ids("$contained_by", ["p1", "p2", "p9"]) == ["101", "103"]
        assert ids("$overlaps", ["p3", "p9"]) == ["102"]
        assert ids("$overlap", ["p1", "p3"]) == ["101", "102"]
        assert ids("$overlaps", []) == []
        assert ids("$not", {"$contains": ["p1"]}) == ["102", "103"]
        for bad in ({"matched_policy_ids": {"$contains": "p1"}}, {"domain": {"$contains": ["d"]}}):
            with pytest.raises(UdbRpcError) as refused:
                data.select(message_type=audit, filter=bad)
            assert inspect(refused.value).code is ErrorCode.INVALID


def test_json_column_takes_json_containment() -> None:
    """On a JSON column ``$contains``/``$contained_by`` are jsonb containment, as on the
    broker; ``$overlaps`` has no jsonb form and is refused."""
    workflow = "udb.core.workflow.entity.v1.WorkflowInstance"
    with udbtest.new(workflow_instance_pb2.WorkflowInstance) as fake:
        data = fake.client(str(uuid.uuid4())).data
        for suffix, payload in (
            ("201", '{"kind":"invoice","tags":["a","b"],"n":1}'),
            ("202", '{"kind":"receipt","tags":["b"]}'),
            ("203", '["x","y"]'),
        ):
            data.upsert(message_type=workflow, record={
                "workflow_id": f"0190f1b2-0000-4000-8000-000000000{suffix}", "workflow_type": "json", "payload": payload,
            })

        def ids(op: str, arg) -> list[str]:
            return _ids(data, workflow, "workflow_id", {"payload": {op: arg}})

        assert ids("$contains", {"kind": "invoice"}) == ["201"]
        assert ids("$contains", {"tags": ["b"]}) == ["201", "202"]
        assert ids("$contains", {"n": "1"}) == [], "a JSON string is not the number 1"
        assert ids("$contains", "x") == ["203"], "a top-level array contains a bare scalar it holds"
        assert ids("$contains", ["y"]) == ["203"]
        assert ids("$contained_by", {"kind": "receipt", "tags": ["b", "c"], "extra": True}) == ["202"]
        with pytest.raises(UdbRpcError) as refused:
            data.select(message_type=workflow, filter={"payload": {"$overlaps": ["x"]}})
        assert inspect(refused.value).code is ErrorCode.INVALID


def test_array_operators_never_match_null() -> None:
    """A NULL column matches no array operator, nor its negation; a NULL array element
    equals nothing, while jsonb null equals null."""
    from udb_client.udbtest import fake as fake_module

    for op in ("$contains", "$contained_by", "$overlaps", fake_module._JSON_CONTAINS, fake_module._JSON_CONTAINED_BY):
        assert not fake_module._matches({"c": None}, {"c": {op: ["p1"]}})
        assert not fake_module._matches({"c": None}, {"c": {"$not": {op: ["p1"]}}})
    assert not fake_module._array_contains(["a", None], [None])
    assert not fake_module._array_overlaps([None], [None])
    assert fake_module._json_contains([None, 1], [None])
    assert not fake_module._json_contains([1, [1, 3]], [1, 3]), "3 is not a top-level element"
    assert fake_module._json_contains([1, [1, 3]], [[3]])
    assert not fake_module._json_contains([True], [1]), "jsonb true is not the number 1"
