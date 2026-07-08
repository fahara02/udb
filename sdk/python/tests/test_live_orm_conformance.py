"""Live ORM conformance (masterplan Phase 10 served proofs).

Runs against a REAL broker over the real JWT login path and exercises the
generated ORM surface end-to-end:

* typed IR query/write/delete builders dispatched through the served
  GenericDispatch chokepoint (10.1),
* descriptor-backed repository CRUD asserting the EMITTED wire conflict clause
  targets the descriptor primary keys — never a hardcoded id (10.2),
* lazy/batch relation queries plus the one-query eager include path, proving
  the N+1-safe secondary fetch against served Postgres (10.3),
* UnitOfWork flush through the served DataBroker.BeginTx bidi stream:
  committed statuses, identity-map clean-up, and atomic rollback of the whole
  batch when one mutation fails server-side (10.4).

Gated on ``UDB_LIVE_SDK_TESTS=1`` like the rest of the live suite.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import replace

import pytest

from udb_client.auth import UdbAuthClient
from udb_client.generated_client import (
    DataBrokerClient,
    UdbError,
    UnitOfWorkUnsupportedBackendError,
    EagerIncludeUnsupportedBackendError,
    query,
    repository,
    unit_of_work,
)
from udb_client.metadata import Metadata

pytestmark = pytest.mark.skipif(
    os.getenv("UDB_LIVE_SDK_TESTS") != "1",
    reason="requires live UDB broker",
)


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise AssertionError(f"{name} is required when UDB_LIVE_SDK_TESTS=1")
    return value


class _CaptureDispatch:
    """Records the last emitted GenericDispatchRequest before forwarding it to
    the live broker, so assertions run on the REAL wire payload."""

    def __init__(self, broker: DataBrokerClient) -> None:
        self._broker = broker
        self.last = None

    def generic_dispatch(self, request, *, metadata=None, timeout=None, retry=True):
        self.last = request
        return self._broker.generic_dispatch(
            request, metadata=metadata, timeout=timeout, retry=retry
        )


def _rows(result_json: str) -> list[dict]:
    body = json.loads(result_json)
    if isinstance(body, list):
        return body
    if isinstance(body, dict) and isinstance(body.get("rows"), list):
        return body["rows"]
    raise AssertionError(f"dispatch result_json is not a row set: {result_json!r}")


def _embedded(row: dict, field: str) -> dict:
    value = row.get(field)
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    raise AssertionError(f"embedded relation {field!r} missing on row: {row!r}")


def _number_equals(value, want: int) -> bool:
    try:
        return int(float(value)) == want
    except (TypeError, ValueError):
        return False


def _assert_conflict_matches_descriptor_pk(request, repo) -> None:
    """10.2 wire contract: conflict kind update, conflict_on == descriptor PKs,
    no PK ever listed as an update field."""
    assert request is not None, "no dispatch request captured"
    spec = json.loads(request.spec_json)
    ir = spec["ir"]
    assert ir["op"] == "write", f"repository upsert must emit ir.op=write, got {ir['op']!r}"
    conflict = ir["conflict"]
    assert conflict["kind"] == "update", f"conflict kind must be update, got {conflict['kind']!r}"
    assert tuple(conflict["conflict_on"]) == tuple(repo.binding.primary_keys), (
        f"emitted conflict_on {conflict['conflict_on']} must equal descriptor "
        f"primary keys {repo.binding.primary_keys}"
    )
    for pk in repo.binding.primary_keys:
        assert pk not in conflict.get("fields", []), (
            f"primary key {pk!r} must never be an on-conflict update field"
        )


def test_live_orm_conformance():
    target = _required_env("UDB_GRPC_TARGET")
    auth_target = os.getenv("UDB_AUTH_GRPC_TARGET", target)

    meta = Metadata(
        tenant_id=os.getenv("UDB_LIVE_TENANT", "sdk-live"),
        project_id=os.getenv("UDB_LIVE_PROJECT", "default"),
        purpose="python.live.orm",
        correlation_id="python-live-orm",
        scopes=(),
        service_identity="python.sdk.live.orm",
    )
    auth = UdbAuthClient(auth_target, meta, timeout=10.0)
    login = auth.login(
        _required_env("UDB_LIVE_USERNAME"),
        _required_env("UDB_LIVE_PASSWORD"),
        device_name="python-sdk-live-orm",
    )
    assert login.access_token
    principal = auth.authenticate_bearer(login.access_token)
    tenant = principal.principal.tenant_id
    assert tenant, "authenticated principal must carry the canonical tenant UUID"
    project = meta.project_id

    authed_meta = replace(
        meta, tenant_id=tenant, bearer_token=login.access_token, client_catalog_version=""
    )
    broker = DataBrokerClient(target, authed_meta, bearer_token=login.access_token)
    dispatch = _CaptureDispatch(broker)

    suffix = uuid.uuid4().hex[:12]

    # ------------------------------------------------------------------
    # 10.2 — descriptor-backed repository CRUD with conflict_on == PK.
    # ------------------------------------------------------------------
    tmpl_repo = repository("udb.core.notification.entity.v1.NotificationTemplate")
    template_id = str(uuid.uuid4())
    event_type = f"orm.live.py.{suffix}"
    template = {
        "template_id": template_id,
        "event_type": event_type,
        "channel": "EMAIL",
        "subject_template": "orm live subject",
        "body_template": "orm live body v1",
        "locale": "en",
        "is_active": True,
        "tenant_id": tenant,
    }
    tmpl_repo.upsert(template, dispatch)
    _assert_conflict_matches_descriptor_pk(dispatch.last, tmpl_repo)

    found = _rows(tmpl_repo.find({"template_id": template_id}, dispatch).result_json)
    assert len(found) == 1 and found[0]["event_type"] == event_type
    assert '"op":"read"' in dispatch.last.spec_json.replace(" ", ""), (
        "repository find must dispatch an IR read envelope"
    )

    template["body_template"] = "orm live body v2"
    tmpl_repo.upsert(template, dispatch)
    _assert_conflict_matches_descriptor_pk(dispatch.last, tmpl_repo)

    by_event = _rows(
        query(tmpl_repo.binding.message_type)
        .where("event_type", "eq", event_type)
        .execute(dispatch)
        .result_json
    )
    assert len(by_event) == 1, (
        f"conflict-on-PK upsert must UPDATE, not duplicate: {len(by_event)} rows"
    )
    assert by_event[0]["body_template"] == "orm live body v2"

    # ------------------------------------------------------------------
    # 10.1 — typed IR query builder through served GenericDispatch.
    # ------------------------------------------------------------------
    resp = (
        query(tmpl_repo.binding.message_type)
        .where("event_type", "eq", event_type)
        .select("template_id", "event_type", "locale")
        .order_by("template_id", "asc")
        .limit(5)
        .execute(dispatch)
    )
    assert resp.backend == "postgres"
    assert '"ir"' in dispatch.last.spec_json, "typed builder must emit the IR envelope"
    q_rows = _rows(resp.result_json)
    assert len(q_rows) == 1 and q_rows[0]["template_id"]

    in_rows = _rows(
        query(tmpl_repo.binding.message_type)
        .where_in("template_id", [template_id])
        .execute(dispatch)
        .result_json
    )
    assert len(in_rows) == 1

    # ------------------------------------------------------------------
    # 10.3 (served side) — lazy relation, batch secondary fetch, eager include.
    # ------------------------------------------------------------------
    log_repo = repository("udb.core.notification.entity.v1.NotificationLog")
    log_id1, log_id2 = str(uuid.uuid4()), str(uuid.uuid4())

    def mk_log(log_id: str) -> dict:
        return {
            "log_id": log_id,
            "template_id": template_id,
            "event_type": event_type,
            "channel": "EMAIL",
            "recipient_address": "orm-live-py@example.com",
            "status": "PENDING",
            "retry_count": 0,
            "tenant_id": tenant,
        }

    log1, log2 = mk_log(log_id1), mk_log(log_id2)
    for record in (log1, log2):
        log_repo.upsert(record, dispatch)

    lazy = _rows(log_repo.relation_query("template", log1).execute(dispatch).result_json)
    assert len(lazy) == 1 and lazy[0]["template_id"] == template_id

    batch = _rows(
        log_repo.relation_batch_query("template", [log1, log2]).execute(dispatch).result_json
    )
    assert len(batch) == 1, "batch belongs_to over one shared parent must dedupe to 1 row"

    children = _rows(
        tmpl_repo.relation_batch_query("notification_logs", [template])
        .execute(dispatch)
        .result_json
    )
    assert len(children) == 2, (
        f"has_many secondary fetch must return both children in ONE query, got {len(children)}"
    )

    inc_rows = _rows(
        query(log_repo.binding.message_type)
        .where_in("log_id", [log_id1, log_id2])
        .include("template")
        .order_by("log_id", "asc")
        .execute(dispatch)
        .result_json
    )
    assert len(inc_rows) == 2
    for row in inc_rows:
        assert _embedded(row, "template")["template_id"] == template_id

    with pytest.raises(EagerIncludeUnsupportedBackendError):
        query(log_repo.binding.message_type).include("template").to_request("redis")

    # ------------------------------------------------------------------
    # 10.4 — UnitOfWork flush via the served DataBroker.BeginTx stream.
    # ------------------------------------------------------------------
    flag_repo = repository("udb.core.config.entity.v1.Flag")
    assert flag_repo.binding.version_field == "revision"
    flag_id = str(uuid.uuid4())
    flag = {
        "flag_id": flag_id,
        "tenant_id": tenant,
        "project_id": project,
        "environment": "live",
        "flag_key": f"orm.live.py.{suffix}",
        "value_type": "bool",
        "value_json": "true",
        "enabled": True,
        "rollout_percentage": 0,
        "rollout_context_key": "",
        "revision": 1,
        "metadata_json": "{}",
    }

    uow = unit_of_work()
    with pytest.raises(UnitOfWorkUnsupportedBackendError):
        uow.require_transactional_backend("qdrant")

    tracked = uow.attach(flag_repo, flag)
    tracked["value_json"] = "false"
    tracked["revision"] = 2

    statuses = uow.flush(broker)
    assert len(statuses) >= 2, "flush must return per-mutation + commit statuses"
    assert statuses[-1].state == statuses[-1].TX_STATE_COMMITTED, (
        f"final status must be committed: {statuses[-1]}"
    )
    assert not uow.dirty_entries(), "identity map must be clean after successful flush"

    persisted = _rows(flag_repo.find({"flag_id": flag_id}, dispatch).result_json)
    assert len(persisted) == 1 and _number_equals(persisted[0]["revision"], 2)

    # Atomic rollback: a poisoned mutation (text bound into the INTEGER
    # rollout_percentage column — no implicit PG cast) must roll back the
    # WHOLE served transaction.
    tracked["revision"] = 3
    tracked["value_json"] = '"v3"'
    poisoned = uow.attach(
        flag_repo,
        {
            "flag_id": str(uuid.uuid4()),
            "tenant_id": tenant,
            "project_id": project,
            "environment": "live",
            "flag_key": f"orm.live.py.poison.{suffix}",
            "value_type": "bool",
            "value_json": "true",
            "enabled": True,
            "rollout_percentage": "boom",
            "rollout_context_key": "",
            "revision": 1,
            "metadata_json": "{}",
        },
    )
    poisoned["enabled"] = False

    with pytest.raises(UdbError):
        uow.flush(broker)
    assert uow.dirty_entries(), "identity map must stay dirty after a failed flush"

    persisted = _rows(flag_repo.find({"flag_id": flag_id}, dispatch).result_json)
    assert len(persisted) == 1 and _number_equals(persisted[0]["revision"], 2), (
        "served BeginTx must roll back the whole batch"
    )

    # ------------------------------------------------------------------
    # Cleanup through the typed delete path (proves DeleteQuery live).
    # ------------------------------------------------------------------
    log_repo.delete({"log_id": log_id1}, dispatch)
    log_repo.delete({"log_id": log_id2}, dispatch)
    tmpl_repo.delete({"template_id": template_id}, dispatch)
    flag_repo.delete({"flag_id": flag_id}, dispatch)
    assert not _rows(tmpl_repo.find({"template_id": template_id}, dispatch).result_json)
