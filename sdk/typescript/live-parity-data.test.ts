// Live data-path parity with the Go SDK. Skipped unless UDB_LIVE_SDK_TESTS=1; the
// broker is the Postgres-only one the CI live lanes build and start. Every test repeats
// its action and has a negative case, and uses core tables only, so it needs no
// generated code.
//
// Needs: UDB_GRPC_TARGET, UDB_LIVE_USERNAME, UDB_LIVE_PASSWORD; optionally
// UDB_AUTH_GRPC_TARGET, UDB_LIVE_TENANT, UDB_LIVE_PROJECT. The durable-consumer test
// also needs UDB_LIVE_CDC_TESTS=1: a broker with CDC enabled, as for the Go SDK's
// served consumer test.

import { strict as assert } from "node:assert";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import * as grpc from "@grpc/grpc-js";

import { UdbProject } from "./project";
import { WriteMode, decodeRecord, encodeEvent, encodeRecord } from "./recordCodec";
import { ErrorCode, codeOf, inspect, isCasConflict, isUnique, missingScope } from "./stores";

const WORKFLOW = "udb.core.workflow.entity.v1.WorkflowInstance";
const EVENT = "udb.core.storage.events.v1.FileUploaded";
const TOPIC = "udb.storage.file_uploaded.v1";
const REPEATS = 3;

const live = { skip: process.env.UDB_LIVE_SDK_TESTS === "1" ? false : "requires live UDB broker" };

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required when UDB_LIVE_SDK_TESTS=1`);
  return value;
}

/** A session logged in as the live test user, with the verified tenant adopted. */
async function connect(t: { after(fn: () => void): void }, purpose: string, clientCatalogVersion?: string): Promise<UdbProject> {
  const target = requiredEnv("UDB_GRPC_TARGET");
  const session = await UdbProject.connectEnterprise({
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

function workflow(id: string, type: string): Record<string, unknown> {
  return { workflow_id: id, workflow_type: type, status: "WORKFLOW_STATUS_PENDING", current_step: 0, total_steps: 3 };
}

async function waitFor(check: () => boolean, what: string, ms = 30_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!check()) {
    if (Date.now() > deadline) assert.fail(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

test("live tx commits writes and events together, repeatedly, and rolls all back on a conflict", live, async (t) => {
  const u = await connect(t, "ts.live.parity.tx");
  const table = u.tableOf(WORKFLOW);
  const tenant = u.core.opts.meta.tenantId;
  for (let i = 0; i < REPEATS; i += 1) {
    const id = randomUUID();
    await u.tx((scope) => {
      scope.insert(WORKFLOW, workflow(id, `ts.live.tx.${i}`));
      scope.emit(EVENT, { file_id: id, tenant_id: tenant, object_key: `ts.live.tx/${id}` });
    });
    assert.equal((await table.get({ workflow_id: id })).workflow_type, `ts.live.tx.${i}`);
    await u.tx((scope) => scope.updateIf(WORKFLOW, { workflow_id: id }, { current_step: 0 }, { current_step: 1 }));
    assert.equal((await table.get({ workflow_id: id })).current_step, 1, "a committed conditional update must read back");

    // Negative: a lost CAS rolls back the earlier insert in the same transaction.
    const fresh = randomUUID();
    await assert.rejects(
      u.tx((scope) => {
        scope.insert(WORKFLOW, workflow(fresh, "ts.live.tx.rollback"));
        scope.updateIf(WORKFLOW, { workflow_id: id }, { current_step: 99 }, { current_step: 2 });
      }),
      (err) => codeOf(err) === ErrorCode.Conflict,
    );
    await assert.rejects(table.get({ workflow_id: fresh }), (err) => codeOf(err) === ErrorCode.NotFound, "a rolled-back insert must not exist");
    assert.equal((await table.get({ workflow_id: id })).current_step, 1, "a rolled-back update must not apply");

    // Negative: a duplicate insert is `unique`, and no half-commit remains.
    await assert.rejects(
      u.tx((scope) => {
        scope.insert(WORKFLOW, workflow(fresh, "ts.live.tx.prefix"));
        scope.insert(WORKFLOW, workflow(id, "ts.live.tx.duplicate"));
      }),
      (err) => isUnique(err),
    );
    await assert.rejects(table.get({ workflow_id: fresh }), (err) => codeOf(err) === ErrorCode.NotFound);
  }
  // Negative: an event without its partition key fails before anything is sent.
  const never = randomUUID();
  await assert.rejects(
    u.tx((scope) => {
      scope.insert(WORKFLOW, workflow(never, "ts.live.tx.never"));
      scope.emit(EVENT, { file_id: never });
    }),
    /partition key/,
  );
  await assert.rejects(table.get({ workflow_id: never }), (err) => codeOf(err) === ErrorCode.NotFound);
});

test("live consume resumes after its acknowledged events and dead-letters a poison event", {
  skip: process.env.UDB_LIVE_SDK_TESTS === "1" && process.env.UDB_LIVE_CDC_TESTS === "1" ? false : "requires the served CDC lane (UDB_LIVE_CDC_TESTS=1)",
}, async (t) => {
  const u = await connect(t, "ts.live.parity.consume");
  const tenant = u.core.opts.meta.tenantId;
  const name = `ts-live-${randomUUID()}`;
  const run = randomUUID();
  const emit = async (label: string) => {
    const id = `${label}-${run}`;
    await u.tx((scope) => scope.emit(EVENT, { file_id: id, tenant_id: tenant, object_key: `ts.live.consume/${id}` }));
    return id;
  };
  const consumeUntil = async (want: Set<string>, opts: Record<string, unknown>, onEvent: (key: string) => void = () => {}) => {
    const controller = new AbortController();
    const seen: string[] = [];
    const running = u.consume<{ file_id: string }>(name, TOPIC, async (event) => {
      if (!event.payload.file_id?.endsWith(run)) return; // Another run's event on the shared topic.
      onEvent(event.payload.file_id);
      seen.push(event.payload.file_id);
    }, { signal: controller.signal, fromEarliest: true, messageType: EVENT, ...opts });
    const failed = running.then(() => undefined, (err) => err);
    await Promise.race([
      waitFor(() => [...want].every((key) => seen.includes(key)), `events ${[...want]}`),
      failed.then((err) => (err ? assert.fail(`consumer stopped: ${err}`) : undefined)),
    ]);
    controller.abort();
    assert.equal(await failed, undefined, "a clean shutdown flushes without an error");
    return seen;
  };
  // Repeat: each batch is delivered once and a restart resumes after it.
  const delivered: string[] = [];
  for (let i = 0; i < REPEATS; i += 1) {
    const key = await emit(`round${i}`);
    delivered.push(...(await consumeUntil(new Set([key]), {})));
  }
  assert.equal(new Set(delivered).size, delivered.length, `no acknowledged event is handed over again: ${delivered}`);
  // Batched acknowledgement settles as one watermark and also resumes.
  const batch = [await emit("batch1"), await emit("batch2"), await emit("batch3")];
  const batched = await consumeUntil(new Set(batch), { ackBatchSize: 2, ackIntervalMs: 200, ackTimeoutMs: 5_000 });
  assert.deepEqual(batched.filter((key) => batch.includes(key)), batch);
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
  assert.equal(attempts, 2, "the handler ran exactly maxAttempts times");
  assert.ok(seen.includes(after));
  const again = await emit("final");
  let redelivered = false;
  const replayed = await consumeUntil(new Set([again]), {}, (key) => {
    redelivered = redelivered || key === poison;
  });
  assert.equal(redelivered, false, "a dead-lettered event is not redelivered");
  assert.ok(replayed.includes(again));
  // Negative: an invalid consumer name is refused without reconnecting.
  await assert.rejects(u.consume("not a valid name!", TOPIC, async () => {}, { backoffMs: 1 }), (err) => inspect(err).grpc === grpc.status.INVALID_ARGUMENT);
});

test("live: no default catalog pin, and a sent pin is honoured", live, async (t) => {
  const u = await connect(t, "ts.live.parity.pin");
  const handle = u.entity(WORKFLOW, { key: ["workflow_id"] });
  for (let i = 0; i < REPEATS; i += 1) {
    assert.equal((await handle.select({ workflow_id: randomUUID() })).length, 0);
  }
  const pinned = await connect(t, "ts.live.parity.pin", "999.0.0");
  const stale = pinned.entity(WORKFLOW, { key: ["workflow_id"] });
  for (let i = 0; i < REPEATS; i += 1) {
    let refused: unknown;
    await stale.select({ workflow_id: randomUUID() }).catch((err: unknown) => {
      refused = err;
    });
    assert.match(String((refused as Error | undefined)?.message), /incompatible catalog version/);
  }
});

test("live error helpers read the broker's typed refusals", live, async (t) => {
  const u = await connect(t, "ts.live.parity.errors");
  const handle = u.entity(WORKFLOW, { key: ["workflow_id"] });
  const id = randomUUID();
  await handle.upsert({ workflow_id: id, workflow_type: "ts.live.errors", status: "PENDING", current_step: 0, total_steps: 1 });
  for (let i = 0; i < REPEATS; i += 1) {
    let lost: unknown;
    await handle.update({ workflow_id: id }, { current_step: 5 }, { expected: { current_step: 1000 + i } }).catch((err) => {
      lost = err;
    });
    assert.equal(isCasConflict(lost), true, `a lost CAS must be recognised: ${lost}`);
    assert.equal(missingScope(lost), "", "a conflict is not a missing scope");
  }
  // Negative: a CAS whose filter does not pin the key is a usage error, not a conflict.
  let usage: unknown;
  await handle.update({ workflow_type: "ts.live.errors" }, { current_step: 5 }, { expected: { current_step: 0 } }).catch((err) => {
    usage = err;
  });
  assert.equal(inspect(usage).reason, "UDB_CAS_KEY_NOT_PK");
  assert.equal(isCasConflict(usage), false);
  // A missing row of a guarded write is a CAS conflict too.
  let gone: unknown;
  await handle.update({ workflow_id: randomUUID() }, { current_step: 5 }, { expected: { current_step: 0 } }).catch((err) => {
    gone = err;
  });
  assert.equal(isCasConflict(gone), true, `a missing row: ${gone}`);
});

test("live entity options: selectPage, update, increment, expected, wasDuplicate", live, async (t) => {
  const u = await connect(t, "ts.live.parity.entity");
  const handle = u.entity<any>(WORKFLOW, { key: ["workflow_id"] });
  const type = `ts.live.entity.${randomUUID()}`;
  const ids = [randomUUID(), randomUUID(), randomUUID(), randomUUID(), randomUUID()].sort();
  for (const id of ids) {
    await handle.upsert({ workflow_id: id, workflow_type: type, status: "PENDING", current_step: 0, total_steps: 1 });
  }
  const seen: string[] = [];
  let pageToken: string | undefined;
  for (let page = 0; page < ids.length; page += 1) {
    const result = await handle.selectPage({ where: { workflow_type: type }, limit: 2, pageToken, orderBy: ["-workflow_id"], includeTotal: true });
    assert.equal(result.total, ids.length, "includeTotal counts every matching row");
    seen.push(...result.rows.map((row) => row.workflow_id));
    if (!result.nextPageToken) break;
    pageToken = result.nextPageToken;
  }
  assert.deepEqual(seen, [...ids].reverse(), "a descending keyset walk visits every row once");
  const id = ids[0];
  // Repeated atomic increments add up.
  for (let i = 1; i <= REPEATS; i += 1) {
    await handle.increment({ workflow_id: id }, { current_step: 2 }, { requireAffected: 1 });
    assert.equal((await handle.select({ workflow_id: id }))[0].current_step, 2 * i);
  }
  // Negative: requireAffected refuses a missing row and an empty precondition never runs.
  await assert.rejects(handle.increment({ workflow_id: randomUUID() }, { current_step: 1 }, { requireAffected: 1 }), (err) => codeOf(err) === ErrorCode.NotFound);
  await assert.rejects(handle.update({ workflow_id: id }, { current_step: 0 }, { expected: {} }), /non-empty expected/);
  // A keyed upsert replays; the same key with other input is refused.
  const key = randomUUID();
  const row = { workflow_id: id, workflow_type: type, status: "PENDING", current_step: 6, total_steps: 1 };
  const firstWrite = await handle.upsertResult(row, { idempotencyKey: key });
  assert.equal(firstWrite.wasDuplicate, false, "the first keyed write is fresh");
  for (let i = 0; i < REPEATS; i += 1) {
    const replay = await handle.upsertResult(row, { idempotencyKey: key });
    assert.equal(replay.wasDuplicate, true, `replay ${i} must report the earlier write`);
  }
  await assert.rejects(handle.upsertResult({ ...row, current_step: 7 }, { idempotencyKey: key }), (err) => inspect(err).reason === "UDB_IDEMPOTENCY_REUSE");
  // Upsert expected: right value applies, stale value refuses, the row is untouched.
  await handle.upsertResult({ ...row, total_steps: 2 }, { expected: { total_steps: 1 } });
  await assert.rejects(handle.upsertResult({ ...row, total_steps: 3 }, { expected: { total_steps: 1 } }), (err) => isCasConflict(err));
  assert.equal((await handle.select({ workflow_id: id }))[0].total_steps, 2);
  const returned = await handle.upsertResult({ ...row, total_steps: 4 }, { returnRecord: true });
  assert.equal(returned.record?.total_steps, 4, "returnRecord decodes the row from the same response");
});

test("live read-your-writes: reads after a write carry its fence and see it", live, async (t) => {
  const u = await connect(t, "ts.live.parity.fence");
  const handle = u.entity<any>(WORKFLOW, { key: ["workflow_id"] });
  const table = u.tableOf(WORKFLOW);
  // Observe the requests the unmodified client sends.
  const contexts: any[] = [];
  const unary = u.core.unary.bind(u.core);
  (u.core as any).unary = (service: string, method: string, request: any, call?: any) => {
    if (method === "Select") contexts.push(request.context);
    return unary(service, method, request, call);
  };
  for (let i = 0; i < REPEATS; i += 1) {
    const id = randomUUID();
    await handle.upsert({ workflow_id: id, workflow_type: "ts.live.fence", status: "PENDING", current_step: i, total_steps: 1 });
    const fenced = (await handle.select({ workflow_id: id }))[0];
    assert.equal(fenced.current_step, i, "a fenced read sees its own write");
    assert.ok(contexts.at(-1)?.read_fence_json, "the read after a write carries a read fence");
    assert.ok(JSON.parse(contexts.at(-1).read_fence_json).max_wait_ms > 0);
    await table.patch({ workflow_id: id }, { current_step: i + 100 });
    assert.equal((await table.get({ workflow_id: id })).current_step, i + 100);
    assert.ok(contexts.at(-1)?.read_fence_json, "a Table read after a Table write carries a read fence");
  }
  // Negative: a client that has not written sends no fence, and sees the rows anyway.
  const other = await connect(t, "ts.live.parity.fence.reader");
  const seen: any[] = [];
  const otherUnary = other.core.unary.bind(other.core);
  (other.core as any).unary = (service: string, method: string, request: any, call?: any) => {
    seen.push(request.context);
    return otherUnary(service, method, request, call);
  };
  await other.entity(WORKFLOW, { key: ["workflow_id"] }).select({ workflow_type: "ts.live.fence" }, { limit: 1 });
  assert.equal(seen.at(-1)?.read_fence_json, undefined, "no write, no fence");
});

test("live: a JSON string column reads and writes as JSON text, repeatedly", live, async (t) => {
  const u = await connect(t, "ts.live.parity.json-string");
  const table = u.tableOf(WORKFLOW);
  for (const payload of ['"My"', '"5"', '""', "5", "[1]", '{"a":1}']) {
    const id = randomUUID();
    const message = { workflow_id: id, workflow_type: "ts.live.json-string", status: "WORKFLOW_STATUS_PENDING", current_step: 0, total_steps: 1, payload };
    for (let i = 0; i < REPEATS; i += 1) {
      await table.upsert(message);
      const decoded = decodeRecord<any>(WORKFLOW, await table.get({ workflow_id: id }));
      assert.equal(decoded.payload, payload, "what was read is what was written");
      message.payload = decoded.payload;
    }
  }
  // Negative: text that is not JSON never reaches the column.
  assert.throws(() => encodeRecord(WORKFLOW, { workflow_id: randomUUID(), payload: "My" }), /not JSON/);
});

test("live record codec: encoded rows store, read back and decode to the same message", live, async (t) => {
  const u = await connect(t, "ts.live.parity.codec");
  const table = u.tableOf(WORKFLOW);
  for (let i = 0; i < REPEATS; i += 1) {
    const id = randomUUID();
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
    assert.equal(stored.status, "COMPLETED", "a text enum is stored as its short token");
    const decoded = decodeRecord<any>(WORKFLOW, stored);
    assert.equal(decoded.status, "WORKFLOW_STATUS_COMPLETED", "the short token decodes to the full name");
    assert.equal(decoded.workflow_type, message.workflow_type);
    assert.equal(decoded.current_step, i);
    assert.deepEqual(JSON.parse(decoded.payload), { a_b: i, nested: { keep_me: true } }, "a JSONB column decodes to its JSON text");
    assert.equal(Date.parse(decoded.next_run_at), Date.parse(message.next_run_at));
    const update = encodeRecord(WORKFLOW, decoded, WriteMode.Update);
    assert.equal("workflow_id" in update || "tenant_id" in update, false, "an update record carries no identity");
  }
  // Negative: text that is not JSON never reaches the JSONB column.
  await assert.rejects(table.upsert({ workflow_id: randomUUID(), workflow_type: "x", status: "WORKFLOW_STATUS_PENDING", payload: "not json" }), /not JSON/);
  assert.throws(() => encodeEvent(WORKFLOW, {}), /no outbox topic/);
});
