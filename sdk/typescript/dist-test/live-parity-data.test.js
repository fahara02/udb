"use strict";
// Live data-path parity with the Go SDK. Skipped unless UDB_LIVE_SDK_TESTS=1; the
// broker is the Postgres-only one the CI live lanes build and start. Every test repeats
// its action and has a negative case, and uses core tables only, so it needs no
// generated code.
//
// Needs: UDB_GRPC_TARGET, UDB_LIVE_USERNAME, UDB_LIVE_PASSWORD; optionally
// UDB_AUTH_GRPC_TARGET, UDB_LIVE_TENANT, UDB_LIVE_PROJECT. The durable-consumer test
// also needs UDB_LIVE_CDC_TESTS=1: a broker with CDC enabled, as for the Go SDK's
// served consumer test.
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const node_assert_1 = require("node:assert");
const node_crypto_1 = require("node:crypto");
const node_test_1 = require("node:test");
const grpc = __importStar(require("@grpc/grpc-js"));
const project_1 = require("./project");
const recordCodec_1 = require("./recordCodec");
const stores_1 = require("./stores");
const WORKFLOW = "udb.core.workflow.entity.v1.WorkflowInstance";
const EVENT = "udb.core.storage.events.v1.FileUploaded";
const TOPIC = "udb.storage.file_uploaded.v1";
const REPEATS = 3;
const live = { skip: process.env.UDB_LIVE_SDK_TESTS === "1" ? false : "requires live UDB broker" };
function requiredEnv(name) {
    const value = process.env[name]?.trim();
    if (!value)
        throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
    return value;
}
/** A session logged in as the live test user, with the verified tenant adopted. */
async function connect(t, purpose, clientCatalogVersion) {
    const target = requiredEnv("UDB_GRPC_TARGET");
    const session = await project_1.UdbProject.connectEnterprise({
        target,
        authTarget: process.env.UDB_AUTH_GRPC_TARGET?.trim() || target,
        tenantId: process.env.UDB_LIVE_TENANT?.trim() || "sdk-live",
        projectId: process.env.UDB_LIVE_PROJECT?.trim() || "default",
        username: requiredEnv("UDB_LIVE_USERNAME"),
        password: requiredEnv("UDB_LIVE_PASSWORD"),
        purpose,
        deadlineMs: 15_000,
        ...(clientCatalogVersion === undefined ? {} : { clientCatalogVersion }),
    });
    t.after(() => session.close());
    return session;
}
function workflow(id, type) {
    return { workflow_id: id, workflow_type: type, status: "WORKFLOW_STATUS_PENDING", current_step: 0, total_steps: 3 };
}
async function waitFor(check, what, ms = 30_000) {
    const deadline = Date.now() + ms;
    while (!check()) {
        if (Date.now() > deadline)
            node_assert_1.strict.fail(`timed out waiting for ${what}`);
        await new Promise((resolve) => setTimeout(resolve, 50));
    }
}
(0, node_test_1.test)("live tx commits writes and events together, repeatedly, and rolls all back on a conflict", live, async (t) => {
    const u = await connect(t, "ts.live.parity.tx");
    const table = u.tableOf(WORKFLOW);
    const tenant = u.core.opts.meta.tenantId;
    for (let i = 0; i < REPEATS; i += 1) {
        const id = (0, node_crypto_1.randomUUID)();
        await u.tx((scope) => {
            scope.insert(WORKFLOW, workflow(id, `ts.live.tx.${i}`));
            scope.emit(EVENT, { file_id: id, tenant_id: tenant, object_key: `ts.live.tx/${id}` });
        });
        node_assert_1.strict.equal((await table.get({ workflow_id: id })).workflow_type, `ts.live.tx.${i}`);
        await u.tx((scope) => scope.updateIf(WORKFLOW, { workflow_id: id }, { current_step: 0 }, { current_step: 1 }));
        node_assert_1.strict.equal((await table.get({ workflow_id: id })).current_step, 1, "a committed conditional update must read back");
        // Negative: a lost CAS rolls back the earlier insert in the same transaction.
        const fresh = (0, node_crypto_1.randomUUID)();
        await node_assert_1.strict.rejects(u.tx((scope) => {
            scope.insert(WORKFLOW, workflow(fresh, "ts.live.tx.rollback"));
            scope.updateIf(WORKFLOW, { workflow_id: id }, { current_step: 99 }, { current_step: 2 });
        }), (err) => (0, stores_1.codeOf)(err) === stores_1.ErrorCode.Conflict);
        await node_assert_1.strict.rejects(table.get({ workflow_id: fresh }), (err) => (0, stores_1.codeOf)(err) === stores_1.ErrorCode.NotFound, "a rolled-back insert must not exist");
        node_assert_1.strict.equal((await table.get({ workflow_id: id })).current_step, 1, "a rolled-back update must not apply");
        // Negative: a duplicate insert is `unique`, and no half-commit remains.
        await node_assert_1.strict.rejects(u.tx((scope) => {
            scope.insert(WORKFLOW, workflow(fresh, "ts.live.tx.prefix"));
            scope.insert(WORKFLOW, workflow(id, "ts.live.tx.duplicate"));
        }), (err) => (0, stores_1.isUnique)(err));
        await node_assert_1.strict.rejects(table.get({ workflow_id: fresh }), (err) => (0, stores_1.codeOf)(err) === stores_1.ErrorCode.NotFound);
    }
    // Negative: an event without its partition key fails before anything is sent.
    const never = (0, node_crypto_1.randomUUID)();
    await node_assert_1.strict.rejects(u.tx((scope) => {
        scope.insert(WORKFLOW, workflow(never, "ts.live.tx.never"));
        scope.emit(EVENT, { file_id: never });
    }), /partition key/);
    await node_assert_1.strict.rejects(table.get({ workflow_id: never }), (err) => (0, stores_1.codeOf)(err) === stores_1.ErrorCode.NotFound);
});
(0, node_test_1.test)("live consume resumes after its acknowledged events and dead-letters a poison event", {
    skip: process.env.UDB_LIVE_SDK_TESTS === "1" && process.env.UDB_LIVE_CDC_TESTS === "1" ? false : "requires the served CDC lane (UDB_LIVE_CDC_TESTS=1)",
}, async (t) => {
    const u = await connect(t, "ts.live.parity.consume");
    const tenant = u.core.opts.meta.tenantId;
    const name = `ts-live-${(0, node_crypto_1.randomUUID)()}`;
    const run = (0, node_crypto_1.randomUUID)();
    const emit = async (label) => {
        const id = `${label}-${run}`;
        await u.tx((scope) => scope.emit(EVENT, { file_id: id, tenant_id: tenant, object_key: `ts.live.consume/${id}` }));
        return id;
    };
    const consumeUntil = async (want, opts, onEvent = () => { }) => {
        const controller = new AbortController();
        const seen = [];
        const running = u.consume(name, TOPIC, async (event) => {
            if (!event.payload.file_id?.endsWith(run))
                return; // Another run's event on the shared topic.
            onEvent(event.payload.file_id);
            seen.push(event.payload.file_id);
        }, { signal: controller.signal, fromEarliest: true, messageType: EVENT, ...opts });
        const failed = running.then(() => undefined, (err) => err);
        await Promise.race([
            waitFor(() => [...want].every((key) => seen.includes(key)), `events ${[...want]}`),
            failed.then((err) => (err ? node_assert_1.strict.fail(`consumer stopped: ${err}`) : undefined)),
        ]);
        controller.abort();
        node_assert_1.strict.equal(await failed, undefined, "a clean shutdown flushes without an error");
        return seen;
    };
    // Repeat: each batch is delivered once and a restart resumes after it.
    const delivered = [];
    for (let i = 0; i < REPEATS; i += 1) {
        const key = await emit(`round${i}`);
        delivered.push(...(await consumeUntil(new Set([key]), {})));
    }
    node_assert_1.strict.equal(new Set(delivered).size, delivered.length, `no acknowledged event is handed over again: ${delivered}`);
    // Batched acknowledgement settles as one watermark and also resumes.
    const batch = [await emit("batch1"), await emit("batch2"), await emit("batch3")];
    const batched = await consumeUntil(new Set(batch), { ackBatchSize: 2, ackIntervalMs: 200, ackTimeoutMs: 5_000 });
    node_assert_1.strict.deepEqual(batched.filter((key) => batch.includes(key)), batch);
    // Negative: a handler that keeps failing is retried maxAttempts times, dead-lettered,
    // and the event after it still arrives.
    const poison = await emit("poison");
    const after = await emit("after");
    let attempts = 0;
    const seen = await consumeUntil(new Set([after]), { maxAttempts: 2, backoffMs: 10 }, (key) => {
        if (key === poison) {
            attempts += 1;
            throw new Error("ts live poison");
        }
    });
    node_assert_1.strict.equal(attempts, 2, "the handler ran exactly maxAttempts times");
    node_assert_1.strict.ok(seen.includes(after));
    const again = await emit("final");
    let redelivered = false;
    const replayed = await consumeUntil(new Set([again]), {}, (key) => {
        redelivered = redelivered || key === poison;
    });
    node_assert_1.strict.equal(redelivered, false, "a dead-lettered event is not redelivered");
    node_assert_1.strict.ok(replayed.includes(again));
    // Negative: an invalid consumer name is refused without reconnecting.
    await node_assert_1.strict.rejects(u.consume("not a valid name!", TOPIC, async () => { }, { backoffMs: 1 }), (err) => (0, stores_1.inspect)(err).grpc === grpc.status.INVALID_ARGUMENT);
});
(0, node_test_1.test)("live: no default catalog pin, and a sent pin is honoured", live, async (t) => {
    const u = await connect(t, "ts.live.parity.pin");
    const handle = u.entity(WORKFLOW, { key: ["workflow_id"] });
    for (let i = 0; i < REPEATS; i += 1) {
        node_assert_1.strict.equal((await handle.select({ workflow_id: (0, node_crypto_1.randomUUID)() })).length, 0);
    }
    const pinned = await connect(t, "ts.live.parity.pin", "999.0.0");
    const stale = pinned.entity(WORKFLOW, { key: ["workflow_id"] });
    for (let i = 0; i < REPEATS; i += 1) {
        let refused;
        await stale.select({ workflow_id: (0, node_crypto_1.randomUUID)() }).catch((err) => {
            refused = err;
        });
        node_assert_1.strict.match(String(refused?.message), /incompatible catalog version/);
    }
});
(0, node_test_1.test)("live error helpers read the broker's typed refusals", live, async (t) => {
    const u = await connect(t, "ts.live.parity.errors");
    const handle = u.entity(WORKFLOW, { key: ["workflow_id"] });
    const id = (0, node_crypto_1.randomUUID)();
    await handle.upsert({ workflow_id: id, workflow_type: "ts.live.errors", status: "PENDING", current_step: 0, total_steps: 1 });
    for (let i = 0; i < REPEATS; i += 1) {
        let lost;
        await handle.update({ workflow_id: id }, { current_step: 5 }, { expected: { current_step: 1000 + i } }).catch((err) => {
            lost = err;
        });
        node_assert_1.strict.equal((0, stores_1.isCasConflict)(lost), true, `a lost CAS must be recognised: ${lost}`);
        node_assert_1.strict.equal((0, stores_1.missingScope)(lost), "", "a conflict is not a missing scope");
    }
    // Negative: a CAS whose filter does not pin the key is a usage error, not a conflict.
    let usage;
    await handle.update({ workflow_type: "ts.live.errors" }, { current_step: 5 }, { expected: { current_step: 0 } }).catch((err) => {
        usage = err;
    });
    node_assert_1.strict.equal((0, stores_1.inspect)(usage).reason, "UDB_CAS_KEY_NOT_PK");
    node_assert_1.strict.equal((0, stores_1.isCasConflict)(usage), false);
    // A missing row of a guarded write is a CAS conflict too.
    let gone;
    await handle.update({ workflow_id: (0, node_crypto_1.randomUUID)() }, { current_step: 5 }, { expected: { current_step: 0 } }).catch((err) => {
        gone = err;
    });
    node_assert_1.strict.equal((0, stores_1.isCasConflict)(gone), true, `a missing row: ${gone}`);
});
(0, node_test_1.test)("live entity options: selectPage, update, increment, expected, wasDuplicate", live, async (t) => {
    const u = await connect(t, "ts.live.parity.entity");
    const handle = u.entity(WORKFLOW, { key: ["workflow_id"] });
    const type = `ts.live.entity.${(0, node_crypto_1.randomUUID)()}`;
    const ids = [(0, node_crypto_1.randomUUID)(), (0, node_crypto_1.randomUUID)(), (0, node_crypto_1.randomUUID)(), (0, node_crypto_1.randomUUID)(), (0, node_crypto_1.randomUUID)()].sort();
    for (const id of ids) {
        await handle.upsert({ workflow_id: id, workflow_type: type, status: "PENDING", current_step: 0, total_steps: 1 });
    }
    const seen = [];
    let pageToken;
    for (let page = 0; page < ids.length; page += 1) {
        const result = await handle.selectPage({ where: { workflow_type: type }, limit: 2, pageToken, orderBy: ["-workflow_id"], includeTotal: true });
        node_assert_1.strict.equal(result.total, ids.length, "includeTotal counts every matching row");
        seen.push(...result.rows.map((row) => row.workflow_id));
        if (!result.nextPageToken)
            break;
        pageToken = result.nextPageToken;
    }
    node_assert_1.strict.deepEqual(seen, [...ids].reverse(), "a descending keyset walk visits every row once");
    const id = ids[0];
    // Repeated atomic increments add up.
    for (let i = 1; i <= REPEATS; i += 1) {
        await handle.increment({ workflow_id: id }, { current_step: 2 }, { requireAffected: 1 });
        node_assert_1.strict.equal((await handle.select({ workflow_id: id }))[0].current_step, 2 * i);
    }
    // Negative: requireAffected refuses a missing row and an empty precondition never runs.
    await node_assert_1.strict.rejects(handle.increment({ workflow_id: (0, node_crypto_1.randomUUID)() }, { current_step: 1 }, { requireAffected: 1 }), (err) => (0, stores_1.codeOf)(err) === stores_1.ErrorCode.NotFound);
    await node_assert_1.strict.rejects(handle.update({ workflow_id: id }, { current_step: 0 }, { expected: {} }), /non-empty expected/);
    // A keyed upsert replays; the same key with other input is refused.
    const key = (0, node_crypto_1.randomUUID)();
    const row = { workflow_id: id, workflow_type: type, status: "PENDING", current_step: 6, total_steps: 1 };
    const firstWrite = await handle.upsertResult(row, { idempotencyKey: key });
    node_assert_1.strict.equal(firstWrite.wasDuplicate, false, "the first keyed write is fresh");
    for (let i = 0; i < REPEATS; i += 1) {
        const replay = await handle.upsertResult(row, { idempotencyKey: key });
        node_assert_1.strict.equal(replay.wasDuplicate, true, `replay ${i} must report the earlier write`);
    }
    await node_assert_1.strict.rejects(handle.upsertResult({ ...row, current_step: 7 }, { idempotencyKey: key }), (err) => (0, stores_1.inspect)(err).reason === "UDB_IDEMPOTENCY_REUSE");
    // Upsert expected: right value applies, stale value refuses, the row is untouched.
    await handle.upsertResult({ ...row, total_steps: 2 }, { expected: { total_steps: 1 } });
    await node_assert_1.strict.rejects(handle.upsertResult({ ...row, total_steps: 3 }, { expected: { total_steps: 1 } }), (err) => (0, stores_1.isCasConflict)(err));
    node_assert_1.strict.equal((await handle.select({ workflow_id: id }))[0].total_steps, 2);
    const returned = await handle.upsertResult({ ...row, total_steps: 4 }, { returnRecord: true });
    node_assert_1.strict.equal(returned.record?.total_steps, 4, "returnRecord decodes the row from the same response");
});
(0, node_test_1.test)("live read-your-writes: reads after a write carry its fence and see it", live, async (t) => {
    const u = await connect(t, "ts.live.parity.fence");
    const handle = u.entity(WORKFLOW, { key: ["workflow_id"] });
    const table = u.tableOf(WORKFLOW);
    // Observe the requests the unmodified client sends.
    const contexts = [];
    const unary = u.core.unary.bind(u.core);
    u.core.unary = (service, method, request, call) => {
        if (method === "Select")
            contexts.push(request.context);
        return unary(service, method, request, call);
    };
    for (let i = 0; i < REPEATS; i += 1) {
        const id = (0, node_crypto_1.randomUUID)();
        await handle.upsert({ workflow_id: id, workflow_type: "ts.live.fence", status: "PENDING", current_step: i, total_steps: 1 });
        const fenced = (await handle.select({ workflow_id: id }))[0];
        node_assert_1.strict.equal(fenced.current_step, i, "a fenced read sees its own write");
        node_assert_1.strict.ok(contexts.at(-1)?.read_fence_json, "the read after a write carries a read fence");
        node_assert_1.strict.ok(JSON.parse(contexts.at(-1).read_fence_json).max_wait_ms > 0);
        await table.patch({ workflow_id: id }, { current_step: i + 100 });
        node_assert_1.strict.equal((await table.get({ workflow_id: id })).current_step, i + 100);
        node_assert_1.strict.ok(contexts.at(-1)?.read_fence_json, "a Table read after a Table write carries a read fence");
    }
    // Negative: a client that has not written sends no fence, and sees the rows anyway.
    const other = await connect(t, "ts.live.parity.fence.reader");
    const seen = [];
    const otherUnary = other.core.unary.bind(other.core);
    other.core.unary = (service, method, request, call) => {
        seen.push(request.context);
        return otherUnary(service, method, request, call);
    };
    await other.entity(WORKFLOW, { key: ["workflow_id"] }).select({ workflow_type: "ts.live.fence" }, { limit: 1 });
    node_assert_1.strict.equal(seen.at(-1)?.read_fence_json, undefined, "no write, no fence");
});
(0, node_test_1.test)("live: a JSON string column reads and writes as JSON text, repeatedly", live, async (t) => {
    const u = await connect(t, "ts.live.parity.json-string");
    const table = u.tableOf(WORKFLOW);
    for (const payload of ['"My"', '"5"', '""', "5", "[1]", '{"a":1}']) {
        const id = (0, node_crypto_1.randomUUID)();
        const message = { workflow_id: id, workflow_type: "ts.live.json-string", status: "WORKFLOW_STATUS_PENDING", current_step: 0, total_steps: 1, payload };
        for (let i = 0; i < REPEATS; i += 1) {
            await table.upsert(message);
            const stored = await table.get({ workflow_id: id });
            const text = typeof stored.payload === "string" ? stored.payload : JSON.stringify(stored.payload);
            node_assert_1.strict.equal(text, payload, "what was read is what was written");
            node_assert_1.strict.equal((0, recordCodec_1.decodeRecord)(WORKFLOW, { payload: JSON.parse(payload) }).payload, payload);
            message.payload = text;
        }
    }
    // Negative: text that is not JSON never reaches the column.
    node_assert_1.strict.throws(() => (0, recordCodec_1.encodeRecord)(WORKFLOW, { workflow_id: (0, node_crypto_1.randomUUID)(), payload: "My" }), /not JSON/);
});
(0, node_test_1.test)("live record codec: encoded rows store, read back and decode to the same message", live, async (t) => {
    const u = await connect(t, "ts.live.parity.codec");
    const table = u.tableOf(WORKFLOW);
    for (let i = 0; i < REPEATS; i += 1) {
        const id = (0, node_crypto_1.randomUUID)();
        const message = {
            workflow_id: id,
            workflow_type: `ts.live.codec.${i}`,
            status: "WORKFLOW_STATUS_COMPLETED",
            current_step: i,
            total_steps: 3,
            payload: JSON.stringify({ a_b: i, nested: { keep_me: true } }),
            next_run_at: "2031-02-03T04:05:06Z",
        };
        await table.upsert(message);
        const stored = await table.get({ workflow_id: id });
        node_assert_1.strict.equal(stored.status, "COMPLETED", "a text enum is stored as its short token");
        const decoded = (0, recordCodec_1.decodeRecord)(WORKFLOW, stored);
        node_assert_1.strict.equal(decoded.status, "WORKFLOW_STATUS_COMPLETED", "the short token decodes to the full name");
        node_assert_1.strict.equal(decoded.workflow_type, message.workflow_type);
        node_assert_1.strict.equal(decoded.current_step, i);
        node_assert_1.strict.deepEqual(JSON.parse(decoded.payload), { a_b: i, nested: { keep_me: true } }, "a JSONB column decodes to its JSON text");
        node_assert_1.strict.equal(Date.parse(decoded.next_run_at), Date.parse(message.next_run_at));
        const update = (0, recordCodec_1.encodeRecord)(WORKFLOW, decoded, recordCodec_1.WriteMode.Update);
        node_assert_1.strict.equal("workflow_id" in update || "tenant_id" in update, false, "an update record carries no identity");
    }
    // Negative: text that is not JSON never reaches the JSONB column.
    await node_assert_1.strict.rejects(table.upsert({ workflow_id: (0, node_crypto_1.randomUUID)(), workflow_type: "x", status: "WORKFLOW_STATUS_PENDING", payload: "not json" }), /not JSON/);
    node_assert_1.strict.throws(() => (0, recordCodec_1.encodeEvent)(WORKFLOW, {}), /no outbox topic/);
});
