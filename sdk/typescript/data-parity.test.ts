// Atomic writes, durable consumers, error helpers, entity options and automatic
// read-your-writes, matching the Go SDK's behaviour.

import { test } from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import * as grpc from "@grpc/grpc-js";

import { READ_FENCE_WAIT_MS, READ_YOUR_WRITES_WINDOW_MS, SessionFence, afterWrite } from "./consistency";
import { EntityHandle, TypedTable, UdbConflictError, UdbNotFoundError } from "./entity";
import { UdbError } from "./generatedClient";
import { UdbProject } from "./project";
import {
  ConflictError,
  ErrorCode,
  NotFoundError,
  Table,
  TxScope,
  codeOf,
  consume,
  inspect,
  isCasConflict,
  missingScope,
  tx,
} from "./stores";

const FILE = "udb.core.storage.entity.v1.File";
const EVENT = "udb.core.apikey.events.v1.ApiKeyCreatedEvent";
const RECEIPT = JSON.stringify({ source_lsn: "0/16B3748", outbox_seq: 7, projection_task_ids: ["p1"], manifest_checksum: "abc", written_at_unix_ms: 1 });
const FENCE = JSON.stringify({ min_outbox_lsn: "0/16B3748", projection_task_ids: ["p1"], max_wait_ms: READ_FENCE_WAIT_MS });

function failure(code: number, detail?: any): UdbError {
  return new UdbError("DataBroker/Update", Object.assign(new Error("refused"), { code, details: "refused", metadata: new grpc.Metadata() }) as any, detail);
}

// ── Error helpers ───────────────────────────────────────────────────────────

test("isCasConflict needs FAILED_PRECONDITION and a CAS reason, never the message", () => {
  for (const reason of ["UDB_CAS_CONFLICT", "UDB_CAS_ROW_MISSING", "UDB_REVISION_CONFLICT"]) {
    assert.equal(isCasConflict(failure(9, { reason })), true, reason);
  }
  assert.equal(isCasConflict(failure(9, { reason: "UDB_CAS_KEY_NOT_PK" })), false, "a usage error a retry cannot fix");
  assert.equal(isCasConflict(failure(10, { reason: "UDB_CAS_CONFLICT" })), false, "the wrong code");
  assert.equal(isCasConflict(new Error("UDB_CAS_CONFLICT")), false, "text is never matched");
  assert.equal(isCasConflict(null), false);
  assert.equal(isCasConflict(new ConflictError("lost", { cause: failure(9, { reason: "UDB_CAS_CONFLICT" }) })), true, "through a store's error");
  assert.equal(isCasConflict(new UdbConflictError("lost", failure(9, { reason: "UDB_CAS_CONFLICT" }))), true, "through a typed table's error");
});

test("missingScope names the scope only for a missing-scope failure", () => {
  assert.equal(missingScope(failure(7, { reason: "UDB_SCOPE_MISSING", missing: { scope: "udb:pii:read" } })), "udb:pii:read");
  assert.equal(missingScope(failure(7, { reason: "UDB_POLICY_DENIED", missing: { rule: "x" } })), "");
  assert.equal(missingScope(new Error("plain")), "");
});

test("inspect follows reason, then kind except validation, then the sentinel, then the code", () => {
  assert.equal(codeOf(failure(9, { kind: 7 })), ErrorCode.Precondition, "a validation kind defers to the gRPC code");
  assert.equal(codeOf(failure(9, { kind: "ERROR_KIND_CONFLICT" })), ErrorCode.Conflict, "a kind name from a transaction frame");
  assert.equal(codeOf(failure(5)), ErrorCode.NotFound);
  assert.equal(inspect(failure(8, { reason: "UDB_RATE_LIMITED", retry_after_ms: 250 })).retryAfterMs, 250);
  assert.equal(inspect(failure(3)).grpc, 3, "the gRPC code is reported");
  assert.equal(codeOf(Object.assign(new Error("raw"), { code: 14 })), ErrorCode.Unavailable, "a raw gRPC error");
  assert.equal(codeOf(new UdbNotFoundError("x")), ErrorCode.NotFound);
  assert.equal(codeOf(new UdbConflictError("x")), ErrorCode.Conflict);
  assert.equal(inspect(new NotFoundError("x", { cause: failure(5, { reason: "UDB_NO_ROWS_AFFECTED" }) })).reason, "UDB_NO_ROWS_AFFECTED", "a wrapped status keeps its reason");
  assert.equal(codeOf(undefined), ErrorCode.Unknown);
});

// ── Transactions ────────────────────────────────────────────────────────────

function fakeTxPlane(frames: any[] = [{ state: "TX_STATE_COMMITTED" }]) {
  const written: any[] = [];
  let opened = 0;
  return {
    written,
    opens: () => opened,
    plane: {
      unary: async () => ({}),
      bidiStream: () => {
        opened += 1;
        const stream = Object.assign(new EventEmitter(), {
          write: (m: any) => {
            written.push(m);
          },
          end: () =>
            queueMicrotask(() => {
              for (const frame of frames) stream.emit("data", frame);
              stream.emit("end");
            }),
        });
        return stream;
      },
    },
  };
}

test("tx carries insert and emits the envelope from the event's own contract", async () => {
  const { plane, written } = fakeTxPlane();
  await tx(plane, (scope) => {
    scope.insert(FILE, { file_id: "f1", filename: "a.pdf", object_key: "k" });
    scope.emit(EVENT, { key_id: "k1", tenant_id: "t1", name: "ci" });
  }, { correlationId: "corr-1" });
  assert.equal(written[0].operation, "insert");
  assert.equal(written[0].message_type, FILE);
  const row = JSON.parse(written[0].record_json.toString());
  assert.equal(row.filename, "a.pdf");
  assert.equal(row.status, "UNSPECIFIED", "the whole row goes through the column rules");
  const event = written[1];
  assert.equal(event.operation, "enqueue_outbox_event");
  assert.equal(event.message_type, EVENT);
  assert.equal(event.collection, "udb.apikey.created.v1", "the topic comes from the contract");
  assert.equal(event.object_key, "t1", "the partition key comes from the contract");
  assert.equal(event.payload.document_id, "t1");
  assert.equal(event.payload.event_type, EVENT);
  assert.equal(event.payload.correlation_id, "corr-1");
  assert.equal(event.payload.envelope_version, 2);
  assert.deepEqual(event.payload.payload, { key_id: "k1", tenant_id: "t1", name: "ci" });
  assert.ok(event.payload.event_id, "every event has an id");
  assert.ok(written.every((m) => m.tx_id === written[0].tx_id), "one transaction id");
  assert.equal(written[written.length - 1].commit, true);
});

test("tx with no writes opens no stream; a throwing body commits nothing", async () => {
  const { plane, opens } = fakeTxPlane();
  await tx(plane, () => {});
  assert.equal(opens(), 0);
  await assert.rejects(tx(plane, (scope) => {
    scope.insert(FILE, { file_id: "f1" });
    throw new Error("changed my mind");
  }), /changed my mind/);
  assert.equal(opens(), 0);
});

test("an event that cannot be emitted fails the whole transaction, even when the body catches it", async () => {
  const { plane, opens } = fakeTxPlane();
  await assert.rejects(tx(plane, (scope) => {
    scope.insert(FILE, { file_id: "f1" });
    try {
      scope.emit(EVENT, { key_id: "no-tenant" });
    } catch {
      // Swallowed on purpose: the scope remembers.
    }
  }), /no value in its partition key field tenant_id/);
  await assert.rejects(tx(plane, (scope) => scope.emit(FILE, { file_id: "f1" })), /declares no outbox topic/);
  const declared = new TxScope("c");
  assert.throws(() => declared.insert(FILE, { nope: 1 }), /no field "nope"/);
  assert.equal(opens(), 0);
});

test("tx correlation falls back to the client's, then the transaction id", async () => {
  const { plane, written } = fakeTxPlane();
  await tx({ ...plane, opts: { meta: { correlationId: "from-client" } } }, (scope) => scope.emit(EVENT, { key_id: "k", tenant_id: "t" }));
  assert.equal(written[0].payload.correlation_id, "from-client");
  written.length = 0;
  await tx(plane, (scope) => scope.emit(EVENT, { key_id: "k", tenant_id: "t" }));
  assert.equal(written[0].payload.correlation_id, written[0].tx_id);
});

test("a rolled-back frame keeps its status code and a kind name", async () => {
  const { plane } = fakeTxPlane([{ state: "TX_STATE_ERROR", message: "x", code: 6, error_detail: { kind: "ERROR_KIND_UNIQUE", constraint: "files_pkey" } }]);
  await assert.rejects(tx(plane, (scope) => scope.delete(FILE, { file_id: "f" })), (err) => {
    assert.equal(codeOf(err), ErrorCode.Unique);
    assert.equal(inspect(err).constraint, "files_pkey");
    return true;
  });
  const bare = fakeTxPlane([{ state: "TX_STATE_ROLLED_BACK", message: "no detail", code: 0 }]);
  await assert.rejects(tx(bare.plane, (scope) => scope.delete(FILE, { file_id: "f" })), (err: any) => err.code === grpc.status.UNKNOWN);
});

// ── Durable consumers ───────────────────────────────────────────────────────

const ID = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;

function envelope(id: string, payload: Record<string, unknown> = { value: id }) {
  return { event_id: id, topic: "fixture", partition_key: "p", payload_json: JSON.stringify({ envelope_version: 2, event_type: "fixture.event", correlation_id: "corr", payload }) };
}

/** Each subscription delivers its script. A script's `refuse` ends it with a status. */
function scriptedPlane(scripts: any[][], settle?: (method: string, request: any) => void) {
  const subscriptions: any[] = [];
  const settled: { method: string; request: any; call: any }[] = [];
  return {
    subscriptions,
    settled,
    plane: {
      unary: async (_service: string, method: string, request: any, call: any) => {
        settled.push({ method, request, call });
        settle?.(method, request);
        return {};
      },
      serverStream: (_service: string, _method: string, request: any) => {
        const script = scripts[subscriptions.length] ?? [];
        subscriptions.push(request);
        const stream = Object.assign(new EventEmitter(), { cancel: () => {} });
        queueMicrotask(() => {
          for (const item of script) {
            if (item.refuse) {
              stream.emit("error", Object.assign(new Error("refused"), { code: item.refuse }));
              return;
            }
            if (item.hold) return;
            stream.emit("data", item);
          }
          stream.emit("end");
        });
        return stream;
      },
    },
  };
}

test("a failing handler retries with backoff and its event is dead-lettered, then later events flow", async () => {
  const controller = new AbortController();
  const { plane, settled } = scriptedPlane([[envelope(ID(1)), envelope(ID(2)), { hold: true }]]);
  const attempts: string[] = [];
  const errors: string[] = [];
  await consume(plane, "c", "fixture", async (event) => {
    attempts.push(event.id);
    if (event.id === ID(1)) throw new Error("poison pill");
    controller.abort();
  }, { signal: controller.signal, maxAttempts: 3, backoffMs: 1, onError: (err) => errors.push(String((err as Error).message)) });
  assert.deepEqual(attempts, [ID(1), ID(1), ID(1), ID(2)]);
  assert.equal(errors.filter((text) => text.includes("attempt")).length, 3);
  const nack = settled.find((s) => s.method === "NackCdcEvents")!;
  assert.equal(nack.request.event_id, ID(1));
  assert.equal(nack.request.attempts, 3);
  assert.equal(nack.request.failure_reason, "poison pill");
  assert.equal(settled.filter((s) => s.method === "AckCdcEvents" && s.request.event_id === ID(1)).length, 0, "a poison event is not acknowledged");
  assert.ok(settled.some((s) => s.method === "AckCdcEvents" && s.request.event_id === ID(2)), "the shutdown flush acknowledges the completed event");
});

test("a dead-letter the broker cannot take stops the consumer instead of discarding the event", async () => {
  const { plane } = scriptedPlane([[envelope(ID(1))]], (method) => {
    if (method === "NackCdcEvents") throw Object.assign(new Error("old broker"), { code: grpc.status.UNIMPLEMENTED });
  });
  await assert.rejects(consume(plane, "c", "fixture", async () => {
    throw new Error("always fails");
  }, { maxAttempts: 1, backoffMs: 1 }), /old broker/);
});

test("ackBatchSize settles one watermark per batch, and the shutdown flush settles the rest", async () => {
  const controller = new AbortController();
  const events = [1, 2, 3, 4, 5].map((n) => envelope(ID(n)));
  const { plane, settled } = scriptedPlane([[...events, { hold: true }]]);
  const handled: string[] = [];
  await consume(plane, "c", "fixture", async (event) => {
    handled.push(event.id);
    if (handled.length === 5) controller.abort();
  }, { signal: controller.signal, ackBatchSize: 2, ackIntervalMs: 60_000 });
  assert.equal(handled.length, 5);
  assert.deepEqual(settled.map((s) => s.request.event_id), [ID(2), ID(4), ID(5)], "the watermark is the newest id of each batch");
});

test("ackIntervalMs flushes a partial batch on an idle stream", async () => {
  const controller = new AbortController();
  const { plane, settled } = scriptedPlane([[envelope(ID(1)), { hold: true }]], () => controller.abort());
  await consume(plane, "c", "fixture", async () => {}, { signal: controller.signal, ackBatchSize: 10, ackIntervalMs: 5 });
  assert.deepEqual(settled.map((s) => s.request.event_id), [ID(1)]);
});

test("ackTimeoutMs bounds each settlement, and every bound is clamped", async () => {
  const controller = new AbortController();
  const { plane, settled } = scriptedPlane([[envelope(ID(1)), { hold: true }]], () => controller.abort());
  await consume(plane, "c", "fixture", async () => {}, { signal: controller.signal, ackTimeoutMs: 120 });
  assert.equal(settled[0].call.deadlineMs, 120);
  const clamped = scriptedPlane([[envelope(ID(1)), { hold: true }]], () => second.abort());
  const second = new AbortController();
  await consume(clamped.plane, "c", "fixture", async () => {}, { signal: second.signal, ackTimeoutMs: 999_000 });
  assert.equal(clamped.settled[0].call.deadlineMs, 30_000, "the maximum is 30s");
  const defaults = scriptedPlane([[envelope(ID(1)), { hold: true }]], () => third.abort());
  const third = new AbortController();
  await consume(defaults.plane, "c", "fixture", async () => {}, { signal: third.signal });
  assert.equal(defaults.settled[0].call.deadlineMs, 5_000, "the default is 5s");
});

test("a lost acknowledgement retries against the redelivery without rerunning the handler", async () => {
  const controller = new AbortController();
  let first = true;
  const { plane, settled, subscriptions } = scriptedPlane([[envelope(ID(1)), envelope(ID(2)), { hold: true }], [envelope(ID(1)), { hold: true }]], (method, request) => {
    if (method === "AckCdcEvents" && first) {
      first = false;
      throw Object.assign(new Error("lost"), { code: grpc.status.UNAVAILABLE });
    }
    if (request.event_id === ID(1)) controller.abort();
  });
  const handled: string[] = [];
  const failedAck: string[] = [];
  await consume(plane, "c", "fixture", async (event) => {
    handled.push(event.id);
  }, { signal: controller.signal, backoffMs: 1, onError: (err) => failedAck.push((err as Error).message) });
  assert.deepEqual(handled, [ID(1)], "the handler ran once and the later event never advanced the cursor");
  assert.equal(subscriptions.length, 2, "the failed acknowledgement stopped the stream and reconnected");
  assert.deepEqual(settled.map((s) => s.request.event_id), [ID(1), ID(1)]);
  assert.ok(failedAck.some((text) => text.includes(`ack ${ID(1)}`)));
});

test("a refusal reconnecting cannot fix stops the consumer; a transient one reconnects", async () => {
  for (const code of [grpc.status.INVALID_ARGUMENT, grpc.status.PERMISSION_DENIED, grpc.status.NOT_FOUND, grpc.status.FAILED_PRECONDITION, grpc.status.OUT_OF_RANGE, grpc.status.UNIMPLEMENTED]) {
    const { plane, subscriptions } = scriptedPlane([[{ refuse: code }]]);
    await assert.rejects(consume(plane, "c", "fixture", async () => {}, { backoffMs: 1 }), (err: any) => err.code === code);
    assert.equal(subscriptions.length, 1, `code ${code} must not reconnect`);
  }
  const controller = new AbortController();
  const { plane, subscriptions } = scriptedPlane([[{ refuse: grpc.status.UNAVAILABLE }], [{ refuse: grpc.status.UNAUTHENTICATED }], [{ hold: true }]]);
  const reconnects: string[] = [];
  await consume(plane, "c", "fixture", async () => {}, {
    signal: controller.signal,
    backoffMs: 1,
    onError: (err) => {
      reconnects.push((err as Error).message);
      if (reconnects.length === 2) setTimeout(() => controller.abort(), 20);
    },
  });
  assert.equal(subscriptions.length, 3);
  assert.ok(reconnects[0].includes("udb: consumer c reconnecting"));
});

test("a shutdown flush that fails is reported, not swallowed", async () => {
  const controller = new AbortController();
  const { plane } = scriptedPlane([[envelope(ID(1)), { hold: true }]], () => {
    throw Object.assign(new Error("broker gone"), { code: grpc.status.UNAVAILABLE });
  });
  await assert.rejects(consume(plane, "c", "fixture", async () => {
    controller.abort();
  }, { signal: controller.signal, ackBatchSize: 5, ackIntervalMs: 60_000 }), /broker gone/);
});

test("a consumed event decodes through its message type and carries the envelope fields", async () => {
  const controller = new AbortController();
  const raw = { ...envelope(ID(1), { key_id: "k1", added_later: 1 }), published_at: { seconds: "1793000000", nanos: 0 } };
  const { plane } = scriptedPlane([[raw, { hold: true }]]);
  let seen: any;
  await consume<{ key_id: string }>(plane, "c", "fixture", async (event) => {
    seen = event;
    controller.abort();
  }, { signal: controller.signal, messageType: EVENT });
  assert.deepEqual(seen.payload, { key_id: "k1" });
  assert.equal(seen.correlationId, "corr");
  assert.equal(seen.eventType, "fixture.event");
  assert.equal(seen.publishedAt.getTime(), 1793000000000);
  const bad = scriptedPlane([[envelope(ID(2), { key_id: 5 })]]);
  await assert.rejects(consume(bad.plane, "c", "fixture", async () => assert.fail("must not run"), { messageType: EVENT }), /does not decode/);
});

// ── Entity options ──────────────────────────────────────────────────────────

function recordingCore(response: any = { affected_rows: "1" }) {
  const calls: { method: string; request: any }[] = [];
  return {
    calls,
    core: {
      unary: async (_service: string, method: string, request: any) => {
        calls.push({ method, request });
        return typeof response === "function" ? response(method) : response;
      },
    } as any,
  };
}

test("EntityHandle.increment is one Update with sorted deltas", async () => {
  const { core, calls } = recordingCore();
  const handle = new EntityHandle(core, FILE, { key: ["file_id"] });
  await handle.increment({ file_id: "f" }, { z_count: 2, a_count: -1 }, { requireAffected: 1, idempotencyKey: "k" });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].method, "Update");
  assert.deepEqual(calls[0].request.increments, [{ column: "a_count", delta: -1 }, { column: "z_count", delta: 2 }]);
  assert.equal("changes" in calls[0].request, false, "no column appears in both");
  assert.equal(calls[0].request.require_affected, 1);
  assert.equal(calls[0].request.idempotency_key, "k");
});

test("upsertResult reports a replay and decodes the row only when asked", async () => {
  const { core, calls } = recordingCore({ was_duplicate: true, record_json: Buffer.from('{"file_id":"f"}') });
  const handle = new EntityHandle<any>(core, FILE, { key: ["file_id"] });
  const plain = await handle.upsertResult({ file_id: "f" }, { idempotencyKey: "k" });
  assert.equal(plain.wasDuplicate, true);
  assert.equal(plain.record, null);
  assert.equal(calls[0].request.return_record, false);
  const returned = await handle.upsertResult({ file_id: "f" }, { returnRecord: true });
  assert.deepEqual(returned.record, { file_id: "f" });
  assert.equal(calls.length, 2, "no proof read");
  const fresh = await new EntityHandle(recordingCore({ was_duplicate: false }).core, FILE, { key: ["file_id"] }).upsertResult({ file_id: "f" });
  assert.equal(fresh.wasDuplicate, false);
});

test("upsert expected is a compare-and-swap; an empty one stays unconditional", async () => {
  const { core, calls } = recordingCore({});
  const handle = new EntityHandle(core, FILE, { key: ["file_id"] });
  await handle.upsert({ file_id: "f" }, { expected: { filename: "a" } });
  assert.deepEqual(calls[0].request.expected, { filename: "a" });
  await handle.upsert({ file_id: "f" }, { expected: {} });
  assert.equal("expected" in calls[1].request, false);
  await handle.upsertResult({ file_id: "f" }, { expected: { filename: "b" } });
  assert.deepEqual(calls[2].request.expected, { filename: "b" });
});

test("a guarded update or delete with an empty expected is refused before any RPC", async () => {
  const { core, calls } = recordingCore();
  const handle = new EntityHandle(core, FILE, { key: ["file_id"] });
  await assert.rejects(handle.update({ file_id: "f" }, { filename: "x" }, { expected: {} }), /non-empty expected/);
  await assert.rejects(handle.increment({ file_id: "f" }, { n: 1 }, { expected: {} }), /non-empty expected/);
  await assert.rejects(handle.delete({ file_id: "f" }, { expected: {} }), /non-empty expected/);
  assert.equal(calls.length, 0);
});

test("a whitespace-only idempotency key is refused; an unset one is fine", async () => {
  const { core, calls } = recordingCore();
  const handle = new EntityHandle(core, FILE, { key: ["file_id"] });
  await assert.rejects(handle.upsert({ file_id: "f" }, { idempotencyKey: "  " }), /whitespace-only/);
  await assert.rejects(handle.delete({ file_id: "f" }, { idempotencyKey: "\t" }), /whitespace-only/);
  assert.equal(calls.length, 0);
  await handle.upsert({ file_id: "f" });
  assert.equal("idempotency_key" in calls[0].request, false);
});

test("update returnRecord asks for the post-update row", async () => {
  const { core, calls } = recordingCore();
  await new EntityHandle(core, FILE, { key: ["file_id"] }).update({ file_id: "f" }, { filename: "x" }, { returnRecord: true });
  assert.equal(calls[0].request.return_record, true);
});

test("a record set that does not decode fails rather than coming back short", async () => {
  const handle = (response: any) => new EntityHandle(recordingCore(response).core, FILE, { key: ["file_id"] });
  await assert.rejects(handle({ records_json: [Buffer.from("not json")] }).select({}), /row 0: not JSON/);
  await assert.rejects(handle({ records_json: [Buffer.alloc(0)] }).selectPage({}), /row 0: empty body/);
  await assert.rejects(handle({ records_json: [], rows: [{}] }).select({}), /no records_json/);
  assert.deepEqual(await handle({ records_json: [] }).select({}), []);
});

// ── Automatic read-your-writes ──────────────────────────────────────────────

test("SessionFence keeps the latest receipt for the window, and ignores an empty one", (t) => {
  const fence = new SessionFence();
  assert.deepEqual(fence.readContext(), {}, "no write, no fence");
  fence.rememberWrite({ write_receipt_json: RECEIPT });
  assert.deepEqual(fence.readContext(), { read_fence_json: FENCE });
  fence.rememberWrite({ write_receipt_json: "" });
  fence.rememberWrite({ write_receipt_json: JSON.stringify({ source_lsn: "", outbox_seq: 0, projection_task_ids: [], manifest_checksum: "", written_at_unix_ms: 0 }) });
  fence.rememberWrite({});
  assert.deepEqual(fence.readContext(), { read_fence_json: FENCE }, "a no-op write keeps the earlier fence");
  t.mock.timers.enable({ apis: ["Date"], now: Date.now() + READ_YOUR_WRITES_WINDOW_MS + 1 });
  assert.deepEqual(fence.readContext(), {}, "past the window a read is not fenced");
});

test("an entity handle fences the reads after a write, on every write verb", async () => {
  for (const write of [
    (h: EntityHandle) => h.upsert({ file_id: "f" }),
    (h: EntityHandle) => h.write("Insert", { file_id: "f" }),
    (h: EntityHandle) => h.update({ file_id: "f" }, { filename: "x" }),
    (h: EntityHandle) => h.increment({ file_id: "f" }, { n: 1 }),
    (h: EntityHandle) => h.delete({ file_id: "f" }),
  ]) {
    const { core, calls } = recordingCore((method: string) => (method === "Select" ? { records_json: [] } : { write_receipt_json: RECEIPT }));
    const handle = new EntityHandle(core, FILE, { key: ["file_id"] }, { tenant_id: "t" }, new SessionFence());
    await handle.select({});
    assert.equal(calls[0].request.context.read_fence_json, undefined, "a read before any write is not fenced");
    await write(handle);
    assert.equal(calls[1].request.context?.read_fence_json, undefined, "a write carries no fence");
    await handle.select({});
    assert.equal(calls[2].request.context.read_fence_json, FENCE);
    assert.equal(calls[2].request.context.tenant_id, "t");
    await handle.selectPage({});
    assert.equal(calls[3].request.context.read_fence_json, FENCE);
  }
});

test("without a fence a handle sends no read fence (the negative case)", async () => {
  const { core, calls } = recordingCore((method: string) => (method === "Select" ? { records_json: [] } : { write_receipt_json: RECEIPT }));
  const handle = new EntityHandle(core, FILE, { key: ["file_id"] }, { tenant_id: "t" });
  await handle.upsert({ file_id: "f" });
  await handle.select({});
  assert.deepEqual(calls[1].request.context, { tenant_id: "t" });
});

test("the fence is per client: another client's reads stay unfenced", async () => {
  const projects = [0, 1].map(() => new UdbProject({ target: "127.0.0.1:1", tenantId: "t", purpose: "test" }));
  try {
    assert.notEqual(projects[0].fence, projects[1].fence);
    projects[0].fence.rememberWrite({ write_receipt_json: RECEIPT });
    assert.ok(projects[0].fence.readContext().read_fence_json);
    assert.equal(projects[1].fence.readContext().read_fence_json, undefined);
    const seen: any[] = [];
    (projects[0].core as any).unary = async (_s: string, _m: string, request: any) => {
      seen.push(request);
      return { records_json: [] };
    };
    await projects[0].entity(FILE).select({});
    await projects[0].tableOf(FILE).select({});
    assert.equal(seen[0].context.read_fence_json, FENCE, "UdbProject.entity shares the fence");
    assert.equal(seen[1].context.read_fence_json, FENCE, "UdbProject.tableOf shares the fence");
  } finally {
    for (const project of projects) project.close();
  }
});

test("a typed table inherits the fence through its host", async () => {
  const { core, calls } = recordingCore((method: string) => (method === "Select" ? { records_json: [] } : { affected_rows: "1", write_receipt_json: RECEIPT }));
  const fence = new SessionFence();
  const host = { entity: (messageType: string, opts?: any) => new EntityHandle<any>(core, messageType, { key: opts?.key ?? [] }, {}, fence) };
  const table = new TypedTable<any, any>(host, {
    messageType: FILE,
    key: ["file_id"],
    fields: { fileId: "file_id", filename: "filename" },
    toRecord: (row: any) => ({ file_id: row.fileId, filename: row.filename }),
    toFields: (row: any) => (row.fileId !== undefined ? { file_id: row.fileId } : { filename: row.filename }),
    fromRow: (row: any) => row,
  });
  await table.upsert({ fileId: "f", filename: "a" });
  await table.select({});
  assert.equal(calls[1].request.context.read_fence_json, FENCE);
  assert.equal(calls[1].method, "Select");
});

test("a Table remembers its writes and a committed transaction's receipt", async () => {
  const fence = new SessionFence();
  const requests: any[] = [];
  const plane = {
    unary: async (_s: string, method: string, request: any) => {
      requests.push(request);
      return method === "Select" ? { records_json: [] } : { affected_rows: "1", write_receipt_json: RECEIPT };
    },
  };
  const table = new Table(plane, "acme.v1.Undeclared", { key: ["id"], fence });
  await table.patch({ id: "1" }, { name: "x" });
  await table.select({ id: "1" });
  assert.equal(requests[1].context.read_fence_json, FENCE);
  const committed = fakeTxPlane([{
    state: "TX_STATE_COMMITTED",
    write_receipt: { source_lsn: "0/AA", outbox_seq: "3", projection_task_ids: [], manifest_checksum: "", written_at_unix_ms: "9" },
  }]);
  const txFence = new SessionFence();
  await tx(committed.plane, (scope) => scope.delete(FILE, { file_id: "f" }), { fence: txFence });
  assert.equal(txFence.readContext().read_fence_json, JSON.stringify({ min_outbox_lsn: "0/AA", max_wait_ms: READ_FENCE_WAIT_MS }));
  const rolledBack = fakeTxPlane([{ state: "TX_STATE_ERROR", write_receipt: { source_lsn: "0/BB" } }]);
  const clean = new SessionFence();
  await assert.rejects(tx(rolledBack.plane, (scope) => scope.delete(FILE, { file_id: "f" }), { fence: clean }));
  assert.deepEqual(clean.readContext(), {}, "a failed transaction leaves no fence");
  assert.ok(afterWrite, "the manual helper stays available");
});
