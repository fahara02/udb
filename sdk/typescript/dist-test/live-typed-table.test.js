"use strict";
// Generated typed tables against a real broker: the code `udb sdk generate
// --project-proto --lang ts` emits, driven through a UdbProject. The Python and Go
// live tests prove the same behaviour.
//
// Skipped unless UDB_LIVE_SDK_TESTS=1. UDB_TS_ENTITIES_JS names the compiled
// generated module, so this runs what the generator really produced rather than
// a hand-written copy of it.
Object.defineProperty(exports, "__esModule", { value: true });
const node_assert_1 = require("node:assert");
const node_test_1 = require("node:test");
const node_crypto_1 = require("node:crypto");
const project_1 = require("./project");
function requiredEnv(name) {
    const value = process.env[name]?.trim();
    if (!value)
        throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
    return value;
}
// The generated module uses the built package, a separate copy of the SDK from this
// test's, so errors are recognised by name and not by class identity.
const isConflict = (err) => err.name === "UdbConflictError";
const isNotFound = (err) => err.name === "UdbNotFoundError";
(0, node_test_1.test)("live generated typed table has the Go table surface", {
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
    const open = (tenantId) => new project_1.UdbProject({
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
    let tenantId;
    try {
        const login = await probe.login(credentials);
        const who = await probe.auth.authenticateBearer(login.access_token);
        tenantId = who?.principal?.tenant_id;
        node_assert_1.strict.ok(tenantId, "the authenticated principal must carry a tenant");
    }
    finally {
        probe.close();
    }
    const project = open(tenantId);
    try {
        await project.login(credentials);
        const table = generated.WorkflowInstanceTable(project);
        const repo = generated.WorkflowInstanceRepo(project);
        const where = generated.WorkflowInstanceWhere;
        const workflowType = `ts.live.typed-table.${(0, node_crypto_1.randomUUID)()}`;
        const workflowId = (0, node_crypto_1.randomUUID)();
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
        node_assert_1.strict.equal(table.messageType, "udb.core.workflow.entity.v1.WorkflowInstance");
        node_assert_1.strict.deepEqual(table.keyOf(row), key);
        node_assert_1.strict.throws(() => table.keyOf({ tenantId }), /primary-key column workflow_id/);
        // insert creates, and a second insert never replaces the row.
        await table.insert(row);
        await node_assert_1.strict.rejects(() => table.insert({ ...row, lastError: "replaced" }));
        const written = await table.get(key);
        node_assert_1.strict.equal(written.workflowType, workflowType);
        node_assert_1.strict.equal(written.status, "WORKFLOW_STATUS_COMPLETED", "a text enum maps back");
        node_assert_1.strict.equal(written.currentStep, 3);
        node_assert_1.strict.equal(written.lastError, "boom", "the failed insert replaced nothing");
        node_assert_1.strict.ok(written.nextRunAt, "a timestamp reads back");
        node_assert_1.strict.deepEqual(written.payload, payload, "a JSONB object keeps its nested keys");
        node_assert_1.strict.deepEqual(written.compensations, [{ step_name: "undo" }], "a JSONB array stays an array");
        node_assert_1.strict.equal("workflow_type" in written, false, "rows use camelCase, not field names");
        node_assert_1.strict.equal("pendingSignal" in written, false, "a NULL column is absent from the row");
        // upsert updates the same row; repeated, it stays one row.
        await table.upsert({ ...row, lastError: "again" });
        await table.upsert({ ...row, lastError: "again" });
        node_assert_1.strict.equal(await table.count({ workflowType }), 1);
        // patch clears: a read cannot tell SQL NULL from the JSON document `null`,
        // because the row leaves both out, so `$is_null` is the proof.
        await table.patch(key, { payload: null, nextRunAt: null, lastError: null });
        const cleared = await table.get(key);
        node_assert_1.strict.equal("payload" in cleared, false, "patch with null must clear a JSONB column");
        node_assert_1.strict.equal("nextRunAt" in cleared, false, "patch with null must clear a TIMESTAMPTZ column");
        node_assert_1.strict.equal("lastError" in cleared, false, "patch with null must clear a text column");
        node_assert_1.strict.deepEqual(cleared.compensations, [{ step_name: "undo" }], "a patch leaves other columns");
        const isNull = await table.select({
            where: { workflowId, ...where.payload.isNull() },
            fields: ["workflowId"],
        });
        node_assert_1.strict.equal(isNull.rows.length, 1, "a cleared JSONB column must be SQL NULL");
        const hasValue = await table.select({
            where: { workflowId, ...where.compensations.isNull() },
            fields: ["workflowId"],
        });
        node_assert_1.strict.equal(hasValue.rows.length, 0, "the negative case: a column holding a value is not NULL");
        // increment is atomic and repeats.
        await table.increment(key, { currentStep: 2 });
        await table.increment(key, { currentStep: 2 });
        node_assert_1.strict.equal((await table.get(key)).currentStep, 7);
        // updateIf writes on a match and is a conflict on a stale expectation.
        await table.updateIf({ workflowId, totalSteps: 8 }, { currentStep: 7 });
        node_assert_1.strict.equal((await table.get(key)).totalSteps, 8);
        await node_assert_1.strict.rejects(() => table.updateIf({ workflowId, totalSteps: 9 }, { currentStep: 1 }), isConflict);
        node_assert_1.strict.equal((await table.get(key)).totalSteps, 8, "a lost race writes nothing");
        // updateWithRetry retries past a real conflict: the first attempt races.
        let attempts = 0;
        const updated = await table.updateWithRetry(key, async (current) => {
            attempts += 1;
            if (attempts === 1)
                await table.increment(key, { totalSteps: 1 });
            return { ...current, totalSteps: current.totalSteps + 10 };
        });
        node_assert_1.strict.equal(attempts, 2, "the conflict must cause exactly one retry");
        node_assert_1.strict.equal(updated.totalSteps, 19);
        node_assert_1.strict.equal((await table.get(key)).totalSteps, 19);
        // select pages, sorts and totals; selectAll walks every page.
        const siblings = [(0, node_crypto_1.randomUUID)(), (0, node_crypto_1.randomUUID)(), (0, node_crypto_1.randomUUID)()];
        for (const [index, id] of siblings.entries()) {
            await table.insert({ ...row, workflowId: id, currentStep: 100 + index });
        }
        const first = await table.select({
            where: where.workflowType.eq(workflowType),
            limit: 2,
            orderBy: ["-currentStep"],
            includeTotal: true,
        });
        node_assert_1.strict.equal(first.total, 4);
        node_assert_1.strict.equal(first.rows.length, 2);
        node_assert_1.strict.equal(first.hasMore, true);
        node_assert_1.strict.deepEqual(first.rows.map((r) => r.currentStep), [102, 101]);
        const second = await table.select({
            where: where.workflowType.eq(workflowType),
            limit: 2,
            orderBy: ["-currentStep"],
            pageToken: first.nextPageToken,
        });
        node_assert_1.strict.deepEqual(second.rows.map((r) => r.currentStep), [100, 7]);
        const walked = [];
        await table.selectAll(where.workflowType.eq(workflowType), (r) => {
            walked.push(r.workflowId);
        });
        node_assert_1.strict.equal(new Set(walked).size, 4);
        node_assert_1.strict.equal(await table.count(where.currentStep.gte(100)) >= 3, true);
        // The predicates reach the broker.
        const inList = await table.select({ where: where.workflowId.in(...siblings) });
        node_assert_1.strict.equal(inList.rows.length, 3);
        const notInList = await table.select({
            where: { workflowType, ...where.workflowId.notIn(...siblings) },
        });
        node_assert_1.strict.equal(notInList.rows.length, 1);
        const between = await table.select({
            where: { workflowType, ...where.currentStep.between(100, 101) },
        });
        node_assert_1.strict.equal(between.rows.length, 2);
        // The repository: null on absence, and guarded writes need a precondition.
        node_assert_1.strict.equal((await repo.get({ workflowId })).workflowId, workflowId);
        node_assert_1.strict.equal(await repo.get({ workflowId: (0, node_crypto_1.randomUUID)() }), null);
        await node_assert_1.strict.rejects(() => repo.get({ workflowType }), /missing from filter/);
        node_assert_1.strict.ok(await repo.findFirst({ workflowType }));
        node_assert_1.strict.equal(await repo.findFirst({ workflowType: "absent" }), null);
        await node_assert_1.strict.rejects(() => repo.updateGuarded(key, { totalSteps: 1 }, {}), /expected precondition is empty/);
        await repo.updateGuarded(key, { totalSteps: 30 }, { totalSteps: 19 });
        await node_assert_1.strict.rejects(() => repo.updateGuarded(key, { totalSteps: 40 }, { totalSteps: 19 }), isConflict);
        node_assert_1.strict.equal((await table.get(key)).totalSteps, 30);
        await node_assert_1.strict.rejects(() => repo.deleteGuarded(key, {}), /expected precondition is empty/);
        await node_assert_1.strict.rejects(() => repo.deleteGuarded(key, { totalSteps: 1 }), isConflict);
        await repo.deleteGuarded(key, { totalSteps: 30 });
        // A deleted row reads as absent, and an absent row is not found on each verb
        // that addresses one.
        await node_assert_1.strict.rejects(() => table.get(key), isNotFound);
        await node_assert_1.strict.rejects(() => table.patch(key, { lastError: "x" }), isNotFound);
        await node_assert_1.strict.rejects(() => table.increment(key, { currentStep: 1 }), isNotFound);
        await node_assert_1.strict.rejects(() => table.delete(key), isNotFound);
        const absent = { workflowId: (0, node_crypto_1.randomUUID)() };
        await node_assert_1.strict.rejects(() => table.get(absent), isNotFound);
        await node_assert_1.strict.rejects(() => table.patch(absent, { lastError: "x" }), isNotFound);
        await node_assert_1.strict.rejects(() => table.delete(absent), isNotFound);
        await node_assert_1.strict.rejects(() => table.increment(absent, { currentStep: 1 }), isNotFound);
        // An unknown property is refused, never dropped.
        await node_assert_1.strict.rejects(() => table.patch({ workflowId: siblings[0] }, { notAColumn: 1 }), /notAColumn/);
        await node_assert_1.strict.rejects(() => table.upsert({ ...row, notAColumn: 1 }), /notAColumn/);
        await node_assert_1.strict.rejects(() => table.select({ where: { notAColumn: 1 } }), /notAColumn/);
        for (const id of siblings)
            await table.delete({ workflowId: id });
        node_assert_1.strict.equal(await table.count({ workflowType }), 0);
    }
    finally {
        project.close();
    }
});
