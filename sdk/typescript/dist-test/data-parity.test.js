"use strict";
// Atomic writes, durable consumers, error helpers, entity options and automatic
// read-your-writes, matching the Go SDK's behaviour.
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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_test_1 = require("node:test");
const strict_1 = __importDefault(require("node:assert/strict"));
const node_events_1 = require("node:events");
const grpc = __importStar(require("@grpc/grpc-js"));
const consistency_1 = require("./consistency");
const entity_1 = require("./entity");
const generatedClient_1 = require("./generatedClient");
const project_1 = require("./project");
const stores_1 = require("./stores");
const FILE = "udb.core.storage.entity.v1.File";
const EVENT = "udb.core.apikey.events.v1.ApiKeyCreatedEvent";
const RECEIPT = JSON.stringify({ source_lsn: "0/16B3748", outbox_seq: 7, projection_task_ids: ["p1"], manifest_checksum: "abc", written_at_unix_ms: 1 });
const FENCE = JSON.stringify({ min_outbox_lsn: "0/16B3748", projection_task_ids: ["p1"], max_wait_ms: consistency_1.READ_FENCE_WAIT_MS });
function failure(code, detail) {
    return new generatedClient_1.UdbError("DataBroker/Update", Object.assign(new Error("refused"), { code, details: "refused", metadata: new grpc.Metadata() }), detail);
}
// ── Error helpers ───────────────────────────────────────────────────────────
(0, node_test_1.test)("a record keeps 64-bit integers exact, on the row and in array columns only", () => {
    const row = (0, entity_1.parseRecordJson)('{"a":9007199254740993,"b":12,"c":[9007199254740993,1],"d":-9007199254740993,' +
        '"s":"9007199254740993 \\" {","f":1.5,"e":1e30,"doc":{"n":9007199254740993,"keep":1}}');
    strict_1.default.equal(row.a, "9007199254740993");
    strict_1.default.equal(row.b, 12, "a safe integer stays a number");
    strict_1.default.deepEqual(row.c, ["9007199254740993", 1]);
    strict_1.default.equal(row.d, "-9007199254740993");
    strict_1.default.equal(row.s, '9007199254740993 " {', "text in a string is untouched");
    strict_1.default.equal(row.f, 1.5);
    strict_1.default.equal(row.e, 1e30, "a float is not an integer token");
    strict_1.default.equal(typeof row.doc.n, "number", "a JSON document keeps its shape");
    strict_1.default.equal(row.doc.keep, 1);
    strict_1.default.throws(() => (0, entity_1.parseRecordJson)("{bad"), "malformed JSON still fails");
});
(0, node_test_1.test)("isCasConflict needs FAILED_PRECONDITION and a CAS reason, never the message", () => {
    for (const reason of ["UDB_CAS_CONFLICT", "UDB_CAS_ROW_MISSING", "UDB_REVISION_CONFLICT"]) {
        strict_1.default.equal((0, stores_1.isCasConflict)(failure(9, { reason })), true, reason);
    }
    strict_1.default.equal((0, stores_1.isCasConflict)(failure(9, { reason: "UDB_CAS_KEY_NOT_PK" })), false, "a usage error a retry cannot fix");
    strict_1.default.equal((0, stores_1.isCasConflict)(failure(10, { reason: "UDB_CAS_CONFLICT" })), false, "the wrong code");
    strict_1.default.equal((0, stores_1.isCasConflict)(new Error("UDB_CAS_CONFLICT")), false, "text is never matched");
    strict_1.default.equal((0, stores_1.isCasConflict)(null), false);
    strict_1.default.equal((0, stores_1.isCasConflict)(new stores_1.ConflictError("lost", { cause: failure(9, { reason: "UDB_CAS_CONFLICT" }) })), true, "through a store's error");
    strict_1.default.equal((0, stores_1.isCasConflict)(new entity_1.UdbConflictError("lost", failure(9, { reason: "UDB_CAS_CONFLICT" }))), true, "through a typed table's error");
});
(0, node_test_1.test)("missingScope names the scope only for a missing-scope failure", () => {
    strict_1.default.equal((0, stores_1.missingScope)(failure(7, { reason: "UDB_SCOPE_MISSING", missing: { scope: "udb:pii:read" } })), "udb:pii:read");
    strict_1.default.equal((0, stores_1.missingScope)(failure(7, { reason: "UDB_POLICY_DENIED", missing: { rule: "x" } })), "");
    strict_1.default.equal((0, stores_1.missingScope)(new Error("plain")), "");
});
(0, node_test_1.test)("inspect follows reason, then kind except validation, then the sentinel, then the code", () => {
    strict_1.default.equal((0, stores_1.codeOf)(failure(9, { kind: 7 })), stores_1.ErrorCode.Precondition, "a validation kind defers to the gRPC code");
    strict_1.default.equal((0, stores_1.codeOf)(failure(9, { kind: "ERROR_KIND_CONFLICT" })), stores_1.ErrorCode.Conflict, "a kind name from a transaction frame");
    strict_1.default.equal((0, stores_1.codeOf)(failure(5)), stores_1.ErrorCode.NotFound);
    strict_1.default.equal((0, stores_1.inspect)(failure(8, { reason: "UDB_RATE_LIMITED", retry_after_ms: 250 })).retryAfterMs, 250);
    strict_1.default.equal((0, stores_1.inspect)(failure(3)).grpc, 3, "the gRPC code is reported");
    strict_1.default.equal((0, stores_1.codeOf)(Object.assign(new Error("raw"), { code: 14 })), stores_1.ErrorCode.Unavailable, "a raw gRPC error");
    strict_1.default.equal((0, stores_1.codeOf)(new entity_1.UdbNotFoundError("x")), stores_1.ErrorCode.NotFound);
    strict_1.default.equal((0, stores_1.codeOf)(new entity_1.UdbConflictError("x")), stores_1.ErrorCode.Conflict);
    strict_1.default.equal((0, stores_1.inspect)(new stores_1.NotFoundError("x", { cause: failure(5, { reason: "UDB_NO_ROWS_AFFECTED" }) })).reason, "UDB_NO_ROWS_AFFECTED", "a wrapped status keeps its reason");
    strict_1.default.equal((0, stores_1.codeOf)(undefined), stores_1.ErrorCode.Unknown);
});
// ── Transactions ────────────────────────────────────────────────────────────
function fakeTxPlane(frames = [{ state: "TX_STATE_COMMITTED" }]) {
    const written = [];
    let opened = 0;
    return {
        written,
        opens: () => opened,
        plane: {
            unary: async () => ({}),
            bidiStream: () => {
                opened += 1;
                const stream = Object.assign(new node_events_1.EventEmitter(), {
                    write: (m) => {
                        written.push(m);
                    },
                    end: () => queueMicrotask(() => {
                        for (const frame of frames)
                            stream.emit("data", frame);
                        stream.emit("end");
                    }),
                });
                return stream;
            },
        },
    };
}
(0, node_test_1.test)("tx carries insert and emits the envelope from the event's own contract", async () => {
    const { plane, written } = fakeTxPlane();
    await (0, stores_1.tx)(plane, (scope) => {
        scope.insert(FILE, { file_id: "f1", filename: "a.pdf", object_key: "k" });
        scope.emit(EVENT, { key_id: "k1", tenant_id: "t1", name: "ci" });
    }, { correlationId: "corr-1" });
    strict_1.default.equal(written[0].operation, "insert");
    strict_1.default.equal(written[0].message_type, FILE);
    const row = JSON.parse(written[0].record_json.toString());
    strict_1.default.equal(row.filename, "a.pdf");
    strict_1.default.equal(row.status, "UNSPECIFIED", "the whole row goes through the column rules");
    const event = written[1];
    strict_1.default.equal(event.operation, "enqueue_outbox_event");
    strict_1.default.equal(event.message_type, EVENT);
    strict_1.default.equal(event.collection, "udb.apikey.created.v1", "the topic comes from the contract");
    strict_1.default.equal(event.object_key, "t1", "the partition key comes from the contract");
    strict_1.default.equal(event.payload.document_id, "t1");
    strict_1.default.equal(event.payload.event_type, EVENT);
    strict_1.default.equal(event.payload.correlation_id, "corr-1");
    strict_1.default.equal(event.payload.envelope_version, 2);
    strict_1.default.deepEqual(event.payload.payload, { key_id: "k1", tenant_id: "t1", name: "ci" });
    strict_1.default.ok(event.payload.event_id, "every event has an id");
    strict_1.default.ok(written.every((m) => m.tx_id === written[0].tx_id), "one transaction id");
    strict_1.default.equal(written[written.length - 1].commit, true);
});
(0, node_test_1.test)("tx with no writes opens no stream; a throwing body commits nothing", async () => {
    const { plane, opens } = fakeTxPlane();
    await (0, stores_1.tx)(plane, () => { });
    strict_1.default.equal(opens(), 0);
    await strict_1.default.rejects((0, stores_1.tx)(plane, (scope) => {
        scope.insert(FILE, { file_id: "f1" });
        throw new Error("changed my mind");
    }), /changed my mind/);
    strict_1.default.equal(opens(), 0);
});
(0, node_test_1.test)("an event that cannot be emitted fails the whole transaction, even when the body catches it", async () => {
    const { plane, opens } = fakeTxPlane();
    await strict_1.default.rejects((0, stores_1.tx)(plane, (scope) => {
        scope.insert(FILE, { file_id: "f1" });
        try {
            scope.emit(EVENT, { key_id: "no-tenant" });
        }
        catch {
            // Swallowed on purpose: the scope remembers.
        }
    }), /no value in its partition key field tenant_id/);
    await strict_1.default.rejects((0, stores_1.tx)(plane, (scope) => scope.emit(FILE, { file_id: "f1" })), /declares no outbox topic/);
    const declared = new stores_1.TxScope("c");
    strict_1.default.throws(() => declared.insert(FILE, { nope: 1 }), /no field "nope"/);
    strict_1.default.equal(opens(), 0);
});
(0, node_test_1.test)("tx correlation falls back to the client's, then the transaction id", async () => {
    const { plane, written } = fakeTxPlane();
    await (0, stores_1.tx)({ ...plane, opts: { meta: { correlationId: "from-client" } } }, (scope) => scope.emit(EVENT, { key_id: "k", tenant_id: "t" }));
    strict_1.default.equal(written[0].payload.correlation_id, "from-client");
    written.length = 0;
    await (0, stores_1.tx)(plane, (scope) => scope.emit(EVENT, { key_id: "k", tenant_id: "t" }));
    strict_1.default.equal(written[0].payload.correlation_id, written[0].tx_id);
});
(0, node_test_1.test)("a rolled-back frame keeps its status code and a kind name", async () => {
    const { plane } = fakeTxPlane([{ state: "TX_STATE_ERROR", message: "x", code: 6, error_detail: { kind: "ERROR_KIND_UNIQUE", constraint: "files_pkey" } }]);
    await strict_1.default.rejects((0, stores_1.tx)(plane, (scope) => scope.delete(FILE, { file_id: "f" })), (err) => {
        strict_1.default.equal((0, stores_1.codeOf)(err), stores_1.ErrorCode.Unique);
        strict_1.default.equal((0, stores_1.inspect)(err).constraint, "files_pkey");
        return true;
    });
    const bare = fakeTxPlane([{ state: "TX_STATE_ROLLED_BACK", message: "no detail", code: 0 }]);
    await strict_1.default.rejects((0, stores_1.tx)(bare.plane, (scope) => scope.delete(FILE, { file_id: "f" })), (err) => err.code === grpc.status.UNKNOWN);
});
// ── Durable consumers ───────────────────────────────────────────────────────
const ID = (n) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
function envelope(id, payload = { value: id }) {
    return { event_id: id, topic: "fixture", partition_key: "p", payload_json: JSON.stringify({ envelope_version: 2, event_type: "fixture.event", correlation_id: "corr", payload }) };
}
/** Each subscription delivers its script. A script's `refuse` ends it with a status. */
function scriptedPlane(scripts, settle) {
    const subscriptions = [];
    const settled = [];
    return {
        subscriptions,
        settled,
        plane: {
            unary: async (_service, method, request, call) => {
                settled.push({ method, request, call });
                settle?.(method, request);
                return {};
            },
            serverStream: (_service, _method, request) => {
                const script = scripts[subscriptions.length] ?? [];
                subscriptions.push(request);
                const stream = Object.assign(new node_events_1.EventEmitter(), { cancel: () => { } });
                queueMicrotask(() => {
                    for (const item of script) {
                        if (item.refuse) {
                            stream.emit("error", Object.assign(new Error("refused"), { code: item.refuse }));
                            return;
                        }
                        if (item.hold)
                            return;
                        stream.emit("data", item);
                    }
                    stream.emit("end");
                });
                return stream;
            },
        },
    };
}
(0, node_test_1.test)("a failing handler retries with backoff and its event is dead-lettered, then later events flow", async () => {
    const controller = new AbortController();
    const { plane, settled } = scriptedPlane([[envelope(ID(1)), envelope(ID(2)), { hold: true }]]);
    const attempts = [];
    const errors = [];
    await (0, stores_1.consume)(plane, "c", "fixture", async (event) => {
        attempts.push(event.id);
        if (event.id === ID(1))
            throw new Error("poison pill");
        controller.abort();
    }, { signal: controller.signal, maxAttempts: 3, backoffMs: 1, onError: (err) => errors.push(String(err.message)) });
    strict_1.default.deepEqual(attempts, [ID(1), ID(1), ID(1), ID(2)]);
    strict_1.default.equal(errors.filter((text) => text.includes("attempt")).length, 3);
    const nack = settled.find((s) => s.method === "NackCdcEvents");
    strict_1.default.equal(nack.request.event_id, ID(1));
    strict_1.default.equal(nack.request.attempts, 3);
    strict_1.default.equal(nack.request.failure_reason, "poison pill");
    strict_1.default.equal(settled.filter((s) => s.method === "AckCdcEvents" && s.request.event_id === ID(1)).length, 0, "a poison event is not acknowledged");
    strict_1.default.ok(settled.some((s) => s.method === "AckCdcEvents" && s.request.event_id === ID(2)), "the shutdown flush acknowledges the completed event");
});
(0, node_test_1.test)("a dead-letter the broker cannot take stops the consumer instead of discarding the event", async () => {
    const { plane } = scriptedPlane([[envelope(ID(1))]], (method) => {
        if (method === "NackCdcEvents")
            throw Object.assign(new Error("old broker"), { code: grpc.status.UNIMPLEMENTED });
    });
    await strict_1.default.rejects((0, stores_1.consume)(plane, "c", "fixture", async () => {
        throw new Error("always fails");
    }, { maxAttempts: 1, backoffMs: 1 }), /old broker/);
});
(0, node_test_1.test)("ackBatchSize settles one watermark per batch, and the shutdown flush settles the rest", async () => {
    const controller = new AbortController();
    const events = [1, 2, 3, 4, 5].map((n) => envelope(ID(n)));
    const { plane, settled } = scriptedPlane([[...events, { hold: true }]]);
    const handled = [];
    await (0, stores_1.consume)(plane, "c", "fixture", async (event) => {
        handled.push(event.id);
        if (handled.length === 5)
            controller.abort();
    }, { signal: controller.signal, ackBatchSize: 2, ackIntervalMs: 60_000 });
    strict_1.default.equal(handled.length, 5);
    strict_1.default.deepEqual(settled.map((s) => s.request.event_id), [ID(2), ID(4), ID(5)], "the watermark is the newest id of each batch");
});
(0, node_test_1.test)("ackIntervalMs flushes a partial batch on an idle stream", async () => {
    const controller = new AbortController();
    const { plane, settled } = scriptedPlane([[envelope(ID(1)), { hold: true }]], () => controller.abort());
    await (0, stores_1.consume)(plane, "c", "fixture", async () => { }, { signal: controller.signal, ackBatchSize: 10, ackIntervalMs: 5 });
    strict_1.default.deepEqual(settled.map((s) => s.request.event_id), [ID(1)]);
});
(0, node_test_1.test)("ackTimeoutMs bounds each settlement, and every bound is clamped", async () => {
    const controller = new AbortController();
    const { plane, settled } = scriptedPlane([[envelope(ID(1)), { hold: true }]], () => controller.abort());
    await (0, stores_1.consume)(plane, "c", "fixture", async () => { }, { signal: controller.signal, ackTimeoutMs: 120 });
    strict_1.default.equal(settled[0].call.deadlineMs, 120);
    const clamped = scriptedPlane([[envelope(ID(1)), { hold: true }]], () => second.abort());
    const second = new AbortController();
    await (0, stores_1.consume)(clamped.plane, "c", "fixture", async () => { }, { signal: second.signal, ackTimeoutMs: 999_000 });
    strict_1.default.equal(clamped.settled[0].call.deadlineMs, 30_000, "the maximum is 30s");
    const defaults = scriptedPlane([[envelope(ID(1)), { hold: true }]], () => third.abort());
    const third = new AbortController();
    await (0, stores_1.consume)(defaults.plane, "c", "fixture", async () => { }, { signal: third.signal });
    strict_1.default.equal(defaults.settled[0].call.deadlineMs, 5_000, "the default is 5s");
});
(0, node_test_1.test)("a lost acknowledgement retries against the redelivery without rerunning the handler", async () => {
    const controller = new AbortController();
    let first = true;
    const { plane, settled, subscriptions } = scriptedPlane([[envelope(ID(1)), envelope(ID(2)), { hold: true }], [envelope(ID(1)), { hold: true }]], (method, request) => {
        if (method === "AckCdcEvents" && first) {
            first = false;
            throw Object.assign(new Error("lost"), { code: grpc.status.UNAVAILABLE });
        }
        if (request.event_id === ID(1))
            controller.abort();
    });
    const handled = [];
    const failedAck = [];
    await (0, stores_1.consume)(plane, "c", "fixture", async (event) => {
        handled.push(event.id);
    }, { signal: controller.signal, backoffMs: 1, onError: (err) => failedAck.push(err.message) });
    strict_1.default.deepEqual(handled, [ID(1)], "the handler ran once and the later event never advanced the cursor");
    strict_1.default.equal(subscriptions.length, 2, "the failed acknowledgement stopped the stream and reconnected");
    strict_1.default.deepEqual(settled.map((s) => s.request.event_id), [ID(1), ID(1)]);
    strict_1.default.ok(failedAck.some((text) => text.includes(`ack ${ID(1)}`)));
});
(0, node_test_1.test)("a refusal reconnecting cannot fix stops the consumer; a transient one reconnects", async () => {
    for (const code of [grpc.status.INVALID_ARGUMENT, grpc.status.PERMISSION_DENIED, grpc.status.NOT_FOUND, grpc.status.FAILED_PRECONDITION, grpc.status.OUT_OF_RANGE, grpc.status.UNIMPLEMENTED]) {
        const { plane, subscriptions } = scriptedPlane([[{ refuse: code }]]);
        await strict_1.default.rejects((0, stores_1.consume)(plane, "c", "fixture", async () => { }, { backoffMs: 1 }), (err) => err.code === code);
        strict_1.default.equal(subscriptions.length, 1, `code ${code} must not reconnect`);
    }
    const controller = new AbortController();
    const { plane, subscriptions } = scriptedPlane([[{ refuse: grpc.status.UNAVAILABLE }], [{ refuse: grpc.status.UNAUTHENTICATED }], [{ hold: true }]]);
    const reconnects = [];
    await (0, stores_1.consume)(plane, "c", "fixture", async () => { }, {
        signal: controller.signal,
        backoffMs: 1,
        onError: (err) => {
            reconnects.push(err.message);
            if (reconnects.length === 2)
                setTimeout(() => controller.abort(), 20);
        },
    });
    strict_1.default.equal(subscriptions.length, 3);
    strict_1.default.ok(reconnects[0].includes("udb: consumer c reconnecting"));
});
(0, node_test_1.test)("a shutdown flush that fails is reported, not swallowed", async () => {
    const controller = new AbortController();
    const { plane } = scriptedPlane([[envelope(ID(1)), { hold: true }]], () => {
        throw Object.assign(new Error("broker gone"), { code: grpc.status.UNAVAILABLE });
    });
    await strict_1.default.rejects((0, stores_1.consume)(plane, "c", "fixture", async () => {
        controller.abort();
    }, { signal: controller.signal, ackBatchSize: 5, ackIntervalMs: 60_000 }), /broker gone/);
});
(0, node_test_1.test)("a consumed event decodes through its message type and carries the envelope fields", async () => {
    const controller = new AbortController();
    const raw = { ...envelope(ID(1), { key_id: "k1", added_later: 1 }), published_at: { seconds: "1793000000", nanos: 0 } };
    const { plane } = scriptedPlane([[raw, { hold: true }]]);
    let seen;
    await (0, stores_1.consume)(plane, "c", "fixture", async (event) => {
        seen = event;
        controller.abort();
    }, { signal: controller.signal, messageType: EVENT });
    strict_1.default.deepEqual(seen.payload, { key_id: "k1" });
    strict_1.default.equal(seen.correlationId, "corr");
    strict_1.default.equal(seen.eventType, "fixture.event");
    strict_1.default.equal(seen.publishedAt.getTime(), 1793000000000);
    const bad = scriptedPlane([[envelope(ID(2), { key_id: 5 })]]);
    await strict_1.default.rejects((0, stores_1.consume)(bad.plane, "c", "fixture", async () => strict_1.default.fail("must not run"), { messageType: EVENT }), /does not decode/);
});
// ── Entity options ──────────────────────────────────────────────────────────
function recordingCore(response = { affected_rows: "1" }) {
    const calls = [];
    return {
        calls,
        core: {
            unary: async (_service, method, request) => {
                calls.push({ method, request });
                return typeof response === "function" ? response(method) : response;
            },
        },
    };
}
(0, node_test_1.test)("EntityHandle.increment is one Update with sorted deltas", async () => {
    const { core, calls } = recordingCore();
    const handle = new entity_1.EntityHandle(core, FILE, { key: ["file_id"] });
    await handle.increment({ file_id: "f" }, { z_count: 2, a_count: -1 }, { requireAffected: 1, idempotencyKey: "k" });
    strict_1.default.equal(calls.length, 1);
    strict_1.default.equal(calls[0].method, "Update");
    strict_1.default.deepEqual(calls[0].request.increments, [{ column: "a_count", delta: -1 }, { column: "z_count", delta: 2 }]);
    strict_1.default.equal("changes" in calls[0].request, false, "no column appears in both");
    strict_1.default.equal(calls[0].request.require_affected, 1);
    strict_1.default.equal(calls[0].request.idempotency_key, "k");
});
(0, node_test_1.test)("upsertResult reports a replay and decodes the row only when asked", async () => {
    const { core, calls } = recordingCore({ was_duplicate: true, record_json: Buffer.from('{"file_id":"f"}') });
    const handle = new entity_1.EntityHandle(core, FILE, { key: ["file_id"] });
    const plain = await handle.upsertResult({ file_id: "f" }, { idempotencyKey: "k" });
    strict_1.default.equal(plain.wasDuplicate, true);
    strict_1.default.equal(plain.record, null);
    strict_1.default.equal(calls[0].request.return_record, false);
    const returned = await handle.upsertResult({ file_id: "f" }, { returnRecord: true });
    strict_1.default.deepEqual(returned.record, { file_id: "f" });
    strict_1.default.equal(calls.length, 2, "no proof read");
    const fresh = await new entity_1.EntityHandle(recordingCore({ was_duplicate: false }).core, FILE, { key: ["file_id"] }).upsertResult({ file_id: "f" });
    strict_1.default.equal(fresh.wasDuplicate, false);
});
(0, node_test_1.test)("upsert expected is a compare-and-swap; an empty one stays unconditional", async () => {
    const { core, calls } = recordingCore({});
    const handle = new entity_1.EntityHandle(core, FILE, { key: ["file_id"] });
    await handle.upsert({ file_id: "f" }, { expected: { filename: "a" } });
    strict_1.default.deepEqual(calls[0].request.expected, { filename: "a" });
    await handle.upsert({ file_id: "f" }, { expected: {} });
    strict_1.default.equal("expected" in calls[1].request, false);
    await handle.upsertResult({ file_id: "f" }, { expected: { filename: "b" } });
    strict_1.default.deepEqual(calls[2].request.expected, { filename: "b" });
});
(0, node_test_1.test)("a guarded update or delete with an empty expected is refused before any RPC", async () => {
    const { core, calls } = recordingCore();
    const handle = new entity_1.EntityHandle(core, FILE, { key: ["file_id"] });
    await strict_1.default.rejects(handle.update({ file_id: "f" }, { filename: "x" }, { expected: {} }), /non-empty expected/);
    await strict_1.default.rejects(handle.increment({ file_id: "f" }, { n: 1 }, { expected: {} }), /non-empty expected/);
    await strict_1.default.rejects(handle.delete({ file_id: "f" }, { expected: {} }), /non-empty expected/);
    strict_1.default.equal(calls.length, 0);
});
(0, node_test_1.test)("a whitespace-only idempotency key is refused; an unset one is fine", async () => {
    const { core, calls } = recordingCore();
    const handle = new entity_1.EntityHandle(core, FILE, { key: ["file_id"] });
    await strict_1.default.rejects(handle.upsert({ file_id: "f" }, { idempotencyKey: "  " }), /whitespace-only/);
    await strict_1.default.rejects(handle.delete({ file_id: "f" }, { idempotencyKey: "\t" }), /whitespace-only/);
    strict_1.default.equal(calls.length, 0);
    await handle.upsert({ file_id: "f" });
    strict_1.default.equal("idempotency_key" in calls[0].request, false);
});
(0, node_test_1.test)("update returnRecord asks for the post-update row", async () => {
    const { core, calls } = recordingCore();
    await new entity_1.EntityHandle(core, FILE, { key: ["file_id"] }).update({ file_id: "f" }, { filename: "x" }, { returnRecord: true });
    strict_1.default.equal(calls[0].request.return_record, true);
});
(0, node_test_1.test)("a record set that does not decode fails rather than coming back short", async () => {
    const handle = (response) => new entity_1.EntityHandle(recordingCore(response).core, FILE, { key: ["file_id"] });
    await strict_1.default.rejects(handle({ records_json: [Buffer.from("not json")] }).select({}), /row 0: not JSON/);
    await strict_1.default.rejects(handle({ records_json: [Buffer.alloc(0)] }).selectPage({}), /row 0: empty body/);
    await strict_1.default.rejects(handle({ records_json: [], rows: [{}] }).select({}), /no records_json/);
    strict_1.default.deepEqual(await handle({ records_json: [] }).select({}), []);
});
// ── Automatic read-your-writes ──────────────────────────────────────────────
(0, node_test_1.test)("SessionFence keeps the latest receipt for the window, and ignores an empty one", (t) => {
    const fence = new consistency_1.SessionFence();
    strict_1.default.deepEqual(fence.readContext(), {}, "no write, no fence");
    fence.rememberWrite({ write_receipt_json: RECEIPT });
    strict_1.default.deepEqual(fence.readContext(), { read_fence_json: FENCE });
    fence.rememberWrite({ write_receipt_json: "" });
    fence.rememberWrite({ write_receipt_json: JSON.stringify({ source_lsn: "", outbox_seq: 0, projection_task_ids: [], manifest_checksum: "", written_at_unix_ms: 0 }) });
    fence.rememberWrite({});
    strict_1.default.deepEqual(fence.readContext(), { read_fence_json: FENCE }, "a no-op write keeps the earlier fence");
    t.mock.timers.enable({ apis: ["Date"], now: Date.now() + consistency_1.READ_YOUR_WRITES_WINDOW_MS + 1 });
    strict_1.default.deepEqual(fence.readContext(), {}, "past the window a read is not fenced");
});
(0, node_test_1.test)("an entity handle fences the reads after a write, on every write verb", async () => {
    for (const write of [
        (h) => h.upsert({ file_id: "f" }),
        (h) => h.write("Insert", { file_id: "f" }),
        (h) => h.update({ file_id: "f" }, { filename: "x" }),
        (h) => h.increment({ file_id: "f" }, { n: 1 }),
        (h) => h.delete({ file_id: "f" }),
    ]) {
        const { core, calls } = recordingCore((method) => (method === "Select" ? { records_json: [] } : { write_receipt_json: RECEIPT }));
        const handle = new entity_1.EntityHandle(core, FILE, { key: ["file_id"] }, { tenant_id: "t" }, new consistency_1.SessionFence());
        await handle.select({});
        strict_1.default.equal(calls[0].request.context.read_fence_json, undefined, "a read before any write is not fenced");
        await write(handle);
        strict_1.default.equal(calls[1].request.context?.read_fence_json, undefined, "a write carries no fence");
        await handle.select({});
        strict_1.default.equal(calls[2].request.context.read_fence_json, FENCE);
        strict_1.default.equal(calls[2].request.context.tenant_id, "t");
        await handle.selectPage({});
        strict_1.default.equal(calls[3].request.context.read_fence_json, FENCE);
    }
});
(0, node_test_1.test)("without a fence a handle sends no read fence (the negative case)", async () => {
    const { core, calls } = recordingCore((method) => (method === "Select" ? { records_json: [] } : { write_receipt_json: RECEIPT }));
    const handle = new entity_1.EntityHandle(core, FILE, { key: ["file_id"] }, { tenant_id: "t" });
    await handle.upsert({ file_id: "f" });
    await handle.select({});
    strict_1.default.deepEqual(calls[1].request.context, { tenant_id: "t" });
});
(0, node_test_1.test)("the fence is per client: another client's reads stay unfenced", async () => {
    const projects = [0, 1].map(() => new project_1.UdbProject({ target: "127.0.0.1:1", tenantId: "t", purpose: "test" }));
    try {
        strict_1.default.notEqual(projects[0].fence, projects[1].fence);
        projects[0].fence.rememberWrite({ write_receipt_json: RECEIPT });
        strict_1.default.ok(projects[0].fence.readContext().read_fence_json);
        strict_1.default.equal(projects[1].fence.readContext().read_fence_json, undefined);
        const seen = [];
        projects[0].core.unary = async (_s, _m, request) => {
            seen.push(request);
            return { records_json: [] };
        };
        await projects[0].entity(FILE).select({});
        await projects[0].tableOf(FILE).select({});
        strict_1.default.equal(seen[0].context.read_fence_json, FENCE, "UdbProject.entity shares the fence");
        strict_1.default.equal(seen[1].context.read_fence_json, FENCE, "UdbProject.tableOf shares the fence");
    }
    finally {
        for (const project of projects)
            project.close();
    }
});
(0, node_test_1.test)("a typed table inherits the fence through its host", async () => {
    const { core, calls } = recordingCore((method) => (method === "Select" ? { records_json: [] } : { affected_rows: "1", write_receipt_json: RECEIPT }));
    const fence = new consistency_1.SessionFence();
    const host = { entity: (messageType, opts) => new entity_1.EntityHandle(core, messageType, { key: opts?.key ?? [] }, {}, fence) };
    const table = new entity_1.TypedTable(host, {
        messageType: FILE,
        key: ["file_id"],
        fields: { fileId: "file_id", filename: "filename" },
        toRecord: (row) => ({ file_id: row.fileId, filename: row.filename }),
        toFields: (row) => (row.fileId !== undefined ? { file_id: row.fileId } : { filename: row.filename }),
        fromRow: (row) => row,
    });
    await table.upsert({ fileId: "f", filename: "a" });
    await table.select({});
    strict_1.default.equal(calls[1].request.context.read_fence_json, FENCE);
    strict_1.default.equal(calls[1].method, "Select");
});
(0, node_test_1.test)("a Table remembers its writes and a committed transaction's receipt", async () => {
    const fence = new consistency_1.SessionFence();
    const requests = [];
    const plane = {
        unary: async (_s, method, request) => {
            requests.push(request);
            return method === "Select" ? { records_json: [] } : { affected_rows: "1", write_receipt_json: RECEIPT };
        },
    };
    const table = new stores_1.Table(plane, "acme.v1.Undeclared", { key: ["id"], fence });
    await table.patch({ id: "1" }, { name: "x" });
    await table.select({ id: "1" });
    strict_1.default.equal(requests[1].context.read_fence_json, FENCE);
    const committed = fakeTxPlane([{
            state: "TX_STATE_COMMITTED",
            write_receipt: { source_lsn: "0/AA", outbox_seq: "3", projection_task_ids: [], manifest_checksum: "", written_at_unix_ms: "9" },
        }]);
    const txFence = new consistency_1.SessionFence();
    await (0, stores_1.tx)(committed.plane, (scope) => scope.delete(FILE, { file_id: "f" }), { fence: txFence });
    strict_1.default.equal(txFence.readContext().read_fence_json, JSON.stringify({ min_outbox_lsn: "0/AA", max_wait_ms: consistency_1.READ_FENCE_WAIT_MS }));
    const rolledBack = fakeTxPlane([{ state: "TX_STATE_ERROR", write_receipt: { source_lsn: "0/BB" } }]);
    const clean = new consistency_1.SessionFence();
    await strict_1.default.rejects((0, stores_1.tx)(rolledBack.plane, (scope) => scope.delete(FILE, { file_id: "f" }), { fence: clean }));
    strict_1.default.deepEqual(clean.readContext(), {}, "a failed transaction leaves no fence");
    strict_1.default.ok(consistency_1.afterWrite, "the manual helper stays available");
});
