// Live ORM conformance (masterplan Phase 10 served proofs).
//
// Skipped unless UDB_LIVE_SDK_TESTS=1, like live-auth.test.ts. Runs against a
// REAL broker over the real JWT login path and exercises the generated ORM
// surface end-to-end:
//
//   - typed IR query/write/delete builders dispatched through the served
//     GenericDispatch chokepoint (10.1),
//   - descriptor-backed repository CRUD asserting the EMITTED wire conflict
//     clause targets the descriptor primary keys (10.2),
//   - lazy/batch relation queries plus the one-query eager include path,
//     proving the N+1-safe secondary fetch against served Postgres (10.3),
//   - UnitOfWork flush through the served DataBroker.BeginTx bidi stream:
//     committed statuses, identity-map clean-up, and atomic rollback of the
//     whole batch when one mutation fails server-side (10.4).

import { strict as assert } from "node:assert";
import { test } from "node:test";
import { randomUUID } from "node:crypto";

import { UdbProject } from "./project";
import {
  EagerIncludeUnsupportedBackendError,
  Repository,
  UnitOfWorkUnsupportedBackendError,
  query,
  repository,
  unitOfWork,
} from "./generatedClient";

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
  return value;
}

function rows(resp: any): any[] {
  const body = JSON.parse(String(resp.result_json ?? ""));
  if (Array.isArray(body)) return body;
  if (body && Array.isArray(body.rows)) return body.rows;
  throw new Error(`dispatch result_json is not a row set: ${resp.result_json}`);
}

function embedded(row: any, field: string): any {
  const value = row?.[field];
  if (value && typeof value === "object") return value;
  if (typeof value === "string") return JSON.parse(value);
  throw new Error(`embedded relation '${field}' missing on row: ${JSON.stringify(row)}`);
}

// 10.2 wire contract on the ACTUALLY-emitted request: conflict kind update,
// conflict_on == descriptor primary keys, no PK as an update field.
function assertConflictMatchesDescriptorPK(request: any, repo: Repository): void {
  assert.ok(request, "no dispatch request captured");
  const spec = JSON.parse(request.spec_json);
  assert.equal(spec.ir.op, "write", "repository upsert must emit ir.op=write");
  assert.equal(spec.ir.conflict.kind, "update");
  assert.deepEqual(
    spec.ir.conflict.conflict_on,
    [...repo.binding.primaryKeys],
    "emitted conflict_on must equal descriptor primary keys",
  );
  for (const pk of repo.binding.primaryKeys) {
    assert.ok(
      !(spec.ir.conflict.fields ?? []).includes(pk),
      `primary key '${pk}' must never be an on-conflict update field`,
    );
  }
}

test("live ORM conformance: builders, repository, relations, UnitOfWork over the served broker", {
  skip: process.env.UDB_LIVE_SDK_TESTS === "1" ? false : "requires live UDB broker",
}, async () => {
  const target = requiredEnv("UDB_GRPC_TARGET");
  const authTarget = process.env.UDB_AUTH_GRPC_TARGET?.trim() || target;
  const username = requiredEnv("UDB_LIVE_USERNAME");
  const password = requiredEnv("UDB_LIVE_PASSWORD");
  let tenantId = process.env.UDB_LIVE_TENANT?.trim() || "sdk-live";
  const projectId = process.env.UDB_LIVE_PROJECT?.trim() || "default";

  // Resolve the canonical tenant UUID from the authenticated principal, so every
  // record body matches the JWT claim (fail-closed handlers).
  {
    const probe = new UdbProject({
      target,
      authTarget,
      tenantId,
      projectId,
      purpose: "ts.live.orm.tenant-probe",
      deadlineMs: 10_000,
    });
    try {
      const probeLogin = await probe.login({
        username,
        password,
        tenant_hint: tenantId,
        project_hint: projectId,
        device_name: "ts-orm-tenant-probe",
      });
      const who = await probe.auth.authenticateBearer(probeLogin.access_token);
      tenantId = who?.principal?.tenant_id || tenantId;
    } finally {
      probe.close();
    }
  }

  const project = new UdbProject({
    target,
    authTarget,
    tenantId,
    projectId,
    purpose: "ts.live.orm",
    deadlineMs: 15_000,
  });
  try {
    const login = await project.login({
      username,
      password,
      tenant_hint: tenantId,
      project_hint: projectId,
      device_name: "ts-sdk-live-orm",
    });
    assert.ok(login.access_token, "live login must return an access token");

    const broker = project.generated.DataBroker;
    let lastRequest: any = null;
    const dispatch = {
      generic_dispatch: (request: any, call?: any) => {
        lastRequest = request;
        return broker.generic_dispatch(request, call);
      },
    };
    const suffix = randomUUID().replace(/-/g, "").slice(0, 12);

    // ---------------------------------------------------------------
    // 10.2 — descriptor-backed repository CRUD with conflict_on == PK.
    // ---------------------------------------------------------------
    const tmplRepo = repository("udb.core.notification.entity.v1.NotificationTemplate");
    const templateId = randomUUID();
    const eventType = `orm.live.ts.${suffix}`;
    const template: Record<string, unknown> = {
      template_id: templateId,
      event_type: eventType,
      channel: "EMAIL",
      subject_template: "orm live subject",
      body_template: "orm live body v1",
      locale: "en",
      is_active: true,
      tenant_id: tenantId,
    };
    await tmplRepo.upsert(template, dispatch);
    assertConflictMatchesDescriptorPK(lastRequest, tmplRepo);

    let found = rows(await tmplRepo.find({ template_id: templateId }, dispatch));
    assert.equal(found.length, 1, "repository find must return the inserted row");
    assert.equal(found[0].event_type, eventType);
    assert.ok(
      String(lastRequest.spec_json).includes('"op":"read"'),
      "repository find must dispatch an IR read envelope",
    );

    template.body_template = "orm live body v2";
    await tmplRepo.upsert(template, dispatch);
    assertConflictMatchesDescriptorPK(lastRequest, tmplRepo);

    const byEvent = rows(
      await query(tmplRepo.binding.messageType)
        .where("event_type", "eq", eventType)
        .execute(dispatch),
    );
    assert.equal(byEvent.length, 1, "conflict-on-PK upsert must UPDATE, not duplicate");
    assert.equal(byEvent[0].body_template, "orm live body v2");

    // ---------------------------------------------------------------
    // 10.1 — typed IR query builder through served GenericDispatch.
    // ---------------------------------------------------------------
    const resp = await query(tmplRepo.binding.messageType)
      .where("event_type", "eq", eventType)
      .select("template_id", "event_type", "locale")
      .orderBy("template_id", "asc")
      .limit(5)
      .execute(dispatch);
    assert.equal(resp.backend, "postgres");
    assert.ok(String(lastRequest.spec_json).includes('"ir"'), "typed builder must emit the IR envelope");
    const qRows = rows(resp);
    assert.equal(qRows.length, 1);
    assert.ok(qRows[0].template_id);

    const inRows = rows(
      await query(tmplRepo.binding.messageType)
        .whereIn("template_id", [templateId])
        .execute(dispatch),
    );
    assert.equal(inRows.length, 1);

    // ---------------------------------------------------------------
    // 10.3 (served side) — lazy relation, batch secondary fetch, include.
    // ---------------------------------------------------------------
    const logRepo = repository("udb.core.notification.entity.v1.NotificationLog");
    const logId1 = randomUUID();
    const logId2 = randomUUID();
    const mkLog = (logId: string): Record<string, unknown> => ({
      log_id: logId,
      template_id: templateId,
      event_type: eventType,
      channel: "EMAIL",
      recipient_address: "orm-live-ts@example.com",
      status: "PENDING",
      retry_count: 0,
      tenant_id: tenantId,
    });
    const log1 = mkLog(logId1);
    const log2 = mkLog(logId2);
    await logRepo.upsert(log1, dispatch);
    await logRepo.upsert(log2, dispatch);

    const lazy = rows(await logRepo.relationQuery("template", log1).execute(dispatch));
    assert.equal(lazy.length, 1, "lazy belongs_to must load exactly the parent template");
    assert.equal(lazy[0].template_id, templateId);

    const batch = rows(
      await logRepo.relationBatchQuery("template", [log1, log2]).execute(dispatch),
    );
    assert.equal(batch.length, 1, "batch belongs_to over one shared parent must dedupe to 1 row");

    const children = rows(
      await tmplRepo.relationBatchQuery("notification_logs", [template]).execute(dispatch),
    );
    assert.equal(children.length, 2, "has_many secondary fetch must return both children in ONE query");

    const incRows = rows(
      await query(logRepo.binding.messageType)
        .whereIn("log_id", [logId1, logId2])
        .include("template")
        .orderBy("log_id", "asc")
        .execute(dispatch),
    );
    assert.equal(incRows.length, 2, "eager include must return both child rows");
    for (const row of incRows) {
      assert.equal(embedded(row, "template").template_id, templateId, "eager include row must embed the parent");
    }

    assert.throws(
      () => query(logRepo.binding.messageType).include("template").toRequest("redis"),
      EagerIncludeUnsupportedBackendError,
      "eager include on a kv-tier backend must be rejected client-side",
    );

    // ---------------------------------------------------------------
    // 10.4 — UnitOfWork flush via the served DataBroker.BeginTx stream.
    // ---------------------------------------------------------------
    const flagRepo = repository("udb.core.config.entity.v1.Flag");
    assert.equal(flagRepo.binding.versionField, "revision");
    const flagId = randomUUID();
    const flag: Record<string, unknown> = {
      flag_id: flagId,
      tenant_id: tenantId,
      project_id: projectId,
      environment: "live",
      flag_key: `orm.live.ts.${suffix}`,
      value_type: "bool",
      value_json: "true",
      enabled: true,
      rollout_percentage: 0,
      rollout_context_key: "",
      revision: 1,
      metadata_json: "{}",
    };

    const uow = unitOfWork();
    assert.throws(
      () => uow.requireTransactionalBackend("qdrant"),
      UnitOfWorkUnsupportedBackendError,
      "UnitOfWork must reject projection backends before a commit batch",
    );

    const tracked = uow.attach(flagRepo, flag);
    tracked.value_json = "false";
    tracked.revision = 2;

    const statuses = await uow.flush(project.generated);
    assert.ok(statuses.length >= 2, "flush must return per-mutation + commit statuses");
    const lastStatus = statuses[statuses.length - 1];
    assert.ok(
      String(lastStatus.state).includes("COMMITTED") || String(lastStatus.state) === "2",
      `final status must be committed, got ${JSON.stringify(lastStatus)}`,
    );
    assert.equal(uow.dirtyEntries().length, 0, "identity map must be clean after successful flush");

    let persisted = rows(await flagRepo.find({ flag_id: flagId }, dispatch));
    assert.equal(persisted.length, 1);
    assert.equal(Number(persisted[0].revision), 2, "flushed flag must persist with revision 2");

    // Atomic rollback: a poisoned mutation (text bound into the INTEGER
    // rollout_percentage column — no implicit PG cast) must roll back the
    // WHOLE served transaction.
    tracked.revision = 3;
    tracked.value_json = '"v3"';
    const poisoned = uow.attach(flagRepo, {
      flag_id: randomUUID(),
      tenant_id: tenantId,
      project_id: projectId,
      environment: "live",
      flag_key: `orm.live.ts.poison.${suffix}`,
      value_type: "bool",
      value_json: "true",
      enabled: true,
      rollout_percentage: "boom",
      rollout_context_key: "",
      revision: 1,
      metadata_json: "{}",
    });
    poisoned.enabled = false;

    await assert.rejects(uow.flush(project.generated), "flush with a poisoned mutation must fail");
    assert.ok(uow.dirtyEntries().length > 0, "identity map must stay dirty after a failed flush");

    persisted = rows(await flagRepo.find({ flag_id: flagId }, dispatch));
    assert.equal(persisted.length, 1);
    assert.equal(Number(persisted[0].revision), 2, "served BeginTx must roll back the whole batch");

    // ---------------------------------------------------------------
    // Cleanup through the typed delete path (proves DeleteQuery live).
    // ---------------------------------------------------------------
    await logRepo.delete({ log_id: logId1 }, dispatch);
    await logRepo.delete({ log_id: logId2 }, dispatch);
    await tmplRepo.delete({ template_id: templateId }, dispatch);
    await flagRepo.delete({ flag_id: flagId }, dispatch);
    const gone = rows(await tmplRepo.find({ template_id: templateId }, dispatch));
    assert.equal(gone.length, 0, "deleted template must not be visible");
  } finally {
    project.close();
  }
});
