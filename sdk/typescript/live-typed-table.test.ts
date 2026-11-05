// Generated typed tables against a real broker: the code `udb sdk generate
// --project-proto --lang ts` emits, driven through a UdbProject. The Python and Go
// live tests prove the same behaviour.
//
// Skipped unless UDB_LIVE_SDK_TESTS=1. UDB_TS_ENTITIES_JS names the compiled
// generated module, so this runs what the generator really produced rather than
// a hand-written copy of it.

import { strict as assert } from "node:assert";
import { test } from "node:test";
import { randomUUID } from "node:crypto";

import { UdbProject } from "./project";

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
  return value;
}

// The generated module uses the built package, a separate copy of the SDK from this
// test's, so errors are recognised by name and not by class identity.
const isConflict = (err: Error) => err.name === "UdbConflictError";
const isNotFound = (err: Error) => err.name === "UdbNotFoundError";

test("live generated typed table has the Go table surface", {
  skip: process.env.UDB_LIVE_SDK_TESTS === "1" ? false : "requires live UDB broker",
}, async () => {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const generated = require(requiredEnv("UDB_TS_ENTITIES_JS"));
  const target = requiredEnv("UDB_GRPC_TARGET");
  const authTarget = process.env.UDB_AUTH_GRPC_TARGET?.trim() || target;
  const username = requiredEnv("UDB_LIVE_USERNAME");
  const password = requiredEnv("UDB_LIVE_PASSWORD");
  const tenantHint = process.env.UDB_LIVE_TENANT?.trim() || "sdk-live";
  const projectId = process.env.UDB_LIVE_PROJECT?.trim() || "default";

  const open = (tenantId: string) =>
    new UdbProject({
      target,
      authTarget,
      tenantId,
      projectId,
      purpose: "ts.live.typed-table",
      deadlineMs: 15_000,
    });
  const credentials = {
    username,
    password,
    tenant_hint: tenantHint,
    project_hint: projectId,
    device_name: "ts-sdk-live-typed-table",
  };

  // The record body must carry the canonical tenant UUID from the JWT claim.
  const probe = open(tenantHint);
  let tenantId: string;
  try {
    const login = await probe.login(credentials);
    const who = await probe.auth.authenticateBearer(login.access_token);
    tenantId = who?.principal?.tenant_id;
    assert.ok(tenantId, "the authenticated principal must carry a tenant");
  } finally {
    probe.close();
  }

  const project = open(tenantId);
  try {
    await project.login(credentials);
    const table = generated.WorkflowInstanceTable(project);
    const repo = generated.WorkflowInstanceRepo(project);
    const where = generated.WorkflowInstanceWhere;
    const workflowType = `ts.live.typed-table.${randomUUID()}`;
    const workflowId = randomUUID();
    const key = { workflowId };
    const payload = { caseId: "nested_key_stays", items: [{ a_b: 1 }, { a_b: 2 }] };
    const row = {
      workflowId,
      tenantId,
      workflowType,
      status: "WORKFLOW_STATUS_COMPLETED",
      currentStep: 3,
      totalSteps: 7,
      payload,
      compensations: [{ step_name: "undo" }],
      lastError: "boom",
      nextRunAt: "2031-02-03T04:05:06Z",
    };

    assert.equal(table.messageType, "udb.core.workflow.entity.v1.WorkflowInstance");
    assert.deepEqual(table.keyOf(row), key);
    assert.throws(() => table.keyOf({ tenantId }), /primary-key column workflow_id/);

    // insert creates, and a second insert never replaces the row.
    await table.insert(row);
    await assert.rejects(() => table.insert({ ...row, lastError: "replaced" }));

    const written = await table.get(key);
    assert.equal(written.workflowType, workflowType);
    assert.equal(written.status, "WORKFLOW_STATUS_COMPLETED", "a text enum maps back");
    assert.equal(written.currentStep, 3);
    assert.equal(written.lastError, "boom", "the failed insert replaced nothing");
    assert.ok(written.nextRunAt, "a timestamp reads back");
    assert.deepEqual(written.payload, payload, "a JSONB object keeps its nested keys");
    assert.deepEqual(written.compensations, [{ step_name: "undo" }], "a JSONB array stays an array");
    assert.equal("workflow_type" in written, false, "rows use camelCase, not field names");
    assert.equal("pendingSignal" in written, false, "a NULL column is absent from the row");

    // upsert updates the same row; repeated, it stays one row.
    await table.upsert({ ...row, lastError: "again" });
    await table.upsert({ ...row, lastError: "again" });
    assert.equal(await table.count({ workflowType }), 1);

    // patch clears: a read cannot tell SQL NULL from the JSON document `null`,
    // because the row leaves both out, so `$is_null` is the proof.
    await table.patch(key, { payload: null, nextRunAt: null, lastError: null });
    const cleared = await table.get(key);
    assert.equal("payload" in cleared, false, "patch with null must clear a JSONB column");
    assert.equal("nextRunAt" in cleared, false, "patch with null must clear a TIMESTAMPTZ column");
    assert.equal("lastError" in cleared, false, "patch with null must clear a text column");
    assert.deepEqual(cleared.compensations, [{ step_name: "undo" }], "a patch leaves other columns");
    const isNull = await table.select({
      where: { workflowId, ...where.payload.isNull() },
      fields: ["workflowId"],
    });
    assert.equal(isNull.rows.length, 1, "a cleared JSONB column must be SQL NULL");
    const hasValue = await table.select({
      where: { workflowId, ...where.compensations.isNull() },
      fields: ["workflowId"],
    });
    assert.equal(hasValue.rows.length, 0, "the negative case: a column holding a value is not NULL");

    // increment is atomic and repeats.
    await table.increment(key, { currentStep: 2 });
    await table.increment(key, { currentStep: 2 });
    assert.equal((await table.get(key)).currentStep, 7);

    // updateIf writes on a match and is a conflict on a stale expectation.
    await table.updateIf({ workflowId, totalSteps: 8 }, { currentStep: 7 });
    assert.equal((await table.get(key)).totalSteps, 8);
    await assert.rejects(
      () => table.updateIf({ workflowId, totalSteps: 9 }, { currentStep: 1 }),
      isConflict,
    );
    assert.equal((await table.get(key)).totalSteps, 8, "a lost race writes nothing");

    // updateWithRetry retries past a real conflict: the first attempt races.
    let attempts = 0;
    const updated = await table.updateWithRetry(key, async (current: any) => {
      attempts += 1;
      if (attempts === 1) await table.increment(key, { totalSteps: 1 });
      return { ...current, totalSteps: current.totalSteps + 10 };
    });
    assert.equal(attempts, 2, "the conflict must cause exactly one retry");
    assert.equal(updated.totalSteps, 19);
    assert.equal((await table.get(key)).totalSteps, 19);

    // select pages, sorts and totals; selectAll walks every page.
    const siblings = [randomUUID(), randomUUID(), randomUUID()];
    for (const [index, id] of siblings.entries()) {
      await table.insert({ ...row, workflowId: id, currentStep: 100 + index });
    }
    const first = await table.select({
      where: where.workflowType.eq(workflowType),
      limit: 2,
      orderBy: ["-currentStep"],
      includeTotal: true,
    });
    assert.equal(first.total, 4);
    assert.equal(first.rows.length, 2);
    assert.equal(first.hasMore, true);
    assert.deepEqual(first.rows.map((r: any) => r.currentStep), [102, 101]);
    const second = await table.select({
      where: where.workflowType.eq(workflowType),
      limit: 2,
      orderBy: ["-currentStep"],
      pageToken: first.nextPageToken,
    });
    assert.deepEqual(second.rows.map((r: any) => r.currentStep), [100, 7]);
    const walked: string[] = [];
    await table.selectAll(where.workflowType.eq(workflowType), (r: any) => {
      walked.push(r.workflowId);
    });
    assert.equal(new Set(walked).size, 4);
    assert.equal(await table.count(where.currentStep.gte(100)) >= 3, true);

    // The predicates reach the broker.
    const inList = await table.select({ where: where.workflowId.in(...siblings) });
    assert.equal(inList.rows.length, 3);
    const notInList = await table.select({
      where: { workflowType, ...where.workflowId.notIn(...siblings) },
    });
    assert.equal(notInList.rows.length, 1);
    const between = await table.select({
      where: { workflowType, ...where.currentStep.between(100, 101) },
    });
    assert.equal(between.rows.length, 2);

    // The repository: null on absence, and guarded writes need a precondition.
    assert.equal((await repo.get({ workflowId })).workflowId, workflowId);
    assert.equal(await repo.get({ workflowId: randomUUID() }), null);
    await assert.rejects(() => repo.get({ workflowType }), /missing from filter/);
    assert.ok(await repo.findFirst({ workflowType }));
    assert.equal(await repo.findFirst({ workflowType: "absent" }), null);
    await assert.rejects(() => repo.updateGuarded(key, { totalSteps: 1 }, {}), /expected precondition is empty/);
    await repo.updateGuarded(key, { totalSteps: 30 }, { totalSteps: 19 });
    await assert.rejects(() => repo.updateGuarded(key, { totalSteps: 40 }, { totalSteps: 19 }), isConflict);
    assert.equal((await table.get(key)).totalSteps, 30);
    await assert.rejects(() => repo.deleteGuarded(key, {}), /expected precondition is empty/);
    await assert.rejects(() => repo.deleteGuarded(key, { totalSteps: 1 }), isConflict);
    await repo.deleteGuarded(key, { totalSteps: 30 });

    // A deleted row reads as absent, and an absent row is not found on each verb
    // that addresses one.
    await assert.rejects(() => table.get(key), isNotFound);
    const absent = { workflowId: randomUUID() };
    await assert.rejects(() => table.get(absent), isNotFound);
    await assert.rejects(() => table.patch(absent, { lastError: "x" }), isNotFound);
    await assert.rejects(() => table.delete(absent), isNotFound);
    await assert.rejects(() => table.increment(absent, { currentStep: 1 }), isNotFound);

    // An unknown property is refused, never dropped.
    await assert.rejects(() => table.patch({ workflowId: siblings[0] }, { notAColumn: 1 }), /notAColumn/);
    await assert.rejects(() => table.upsert({ ...row, notAColumn: 1 }), /notAColumn/);
    await assert.rejects(() => table.select({ where: { notAColumn: 1 } }), /notAColumn/);

    for (const id of siblings) await table.delete({ workflowId: id });
    assert.equal(await table.count({ workflowType }), 0);
  } finally {
    project.close();
  }
});
