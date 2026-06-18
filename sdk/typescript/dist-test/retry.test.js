"use strict";
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
const node_test_1 = require("node:test");
const grpc = __importStar(require("@grpc/grpc-js"));
const generatedClient_1 = require("./generatedClient");
const DATA_BROKER = "udb.services.v1.DataBroker";
function unavailable() {
    return Object.assign(new Error("unavailable"), {
        code: grpc.status.UNAVAILABLE,
        details: "unavailable",
        metadata: new grpc.Metadata(),
    });
}
function fakeCore(stub) {
    const core = Object.create(generatedClient_1.UdbCore.prototype);
    core.opts = {
        target: "unused.invalid:50051",
        meta: { tenantId: "tenant-a", purpose: "test", correlationId: "corr-a" },
    };
    core.retry = {
        maxAttempts: 3,
        initialBackoffMs: 0,
        maxBackoffMs: 0,
        backoffMultiplier: 1,
        retryableCodes: [grpc.status.UNAVAILABLE],
    };
    core.stubs = new Map([[DATA_BROKER, stub]]);
    return core;
}
(0, node_test_1.test)("mutating unary RPC is not retried on retryable status", async () => {
    let calls = 0;
    const core = fakeCore({
        MarkSagaReviewed(_request, _metadata, _options, cb) {
            calls += 1;
            cb(unavailable(), null);
        },
    });
    await node_assert_1.strict.rejects(() => core.unary(DATA_BROKER, "MarkSagaReviewed", {}), (err) => err instanceof generatedClient_1.UdbError && err.code === grpc.status.UNAVAILABLE);
    node_assert_1.strict.equal(calls, 1);
});
(0, node_test_1.test)("read-only unary RPC still retries transient status", async () => {
    let calls = 0;
    const core = fakeCore({
        ListSagas(_request, _metadata, _options, cb) {
            calls += 1;
            if (calls === 1) {
                cb(unavailable(), null);
                return;
            }
            cb(null, { ok: true });
        },
    });
    const result = await core.unary(DATA_BROKER, "ListSagas", {});
    node_assert_1.strict.deepEqual(result, { ok: true });
    node_assert_1.strict.equal(calls, 2);
});
// ── Replay-safe mutation retry (R1.2) ───────────────────────────────────────
//
// The proto marks Upsert/Delete `replay_safe` (R1.1) — the broker collapses a
// retried duplicate carrying the same idempotency key. So a replay-safe mutation
// may be retried on a transient failure ONLY when a non-empty key is present.
(0, node_test_1.test)("catalog: Upsert is replay-safe and a mutation; ListSagas is not", () => {
    // Guards the R1.1 catalog this retry behavior depends on.
    node_assert_1.strict.equal(generatedClient_1.RPC_REPLAY_SAFE[`/${DATA_BROKER}/Upsert`], true);
    node_assert_1.strict.equal(generatedClient_1.RPC_REPLAY_SAFE[`/${DATA_BROKER}/Delete`], true);
    node_assert_1.strict.equal(generatedClient_1.RPC_OPERATION_KIND[`/${DATA_BROKER}/Upsert`], "mutation");
    // A non-replay-safe mutation and a read-only RPC are absent/false.
    node_assert_1.strict.notEqual(generatedClient_1.RPC_REPLAY_SAFE[`/${DATA_BROKER}/MarkSagaReviewed`], true);
    node_assert_1.strict.notEqual(generatedClient_1.RPC_REPLAY_SAFE[`/${DATA_BROKER}/ListSagas`], true);
});
(0, node_test_1.test)("replay-safe mutation WITH idempotency key retries then succeeds", async () => {
    let calls = 0;
    const core = fakeCore({
        Upsert(_request, _metadata, _options, cb) {
            calls += 1;
            if (calls === 1) {
                cb(unavailable(), null);
                return;
            }
            cb(null, { was_duplicate: true });
        },
    });
    const result = await core.unary(DATA_BROKER, "Upsert", { idempotency_key: "key-123" });
    node_assert_1.strict.deepEqual(result, { was_duplicate: true });
    node_assert_1.strict.equal(calls, 2);
});
(0, node_test_1.test)("replay-safe mutation WITHOUT idempotency key is not retried", async () => {
    let calls = 0;
    const core = fakeCore({
        Upsert(_request, _metadata, _options, cb) {
            calls += 1;
            cb(unavailable(), null);
        },
    });
    await node_assert_1.strict.rejects(() => core.unary(DATA_BROKER, "Upsert", {}), (err) => err instanceof generatedClient_1.UdbError && err.code === grpc.status.UNAVAILABLE);
    node_assert_1.strict.equal(calls, 1);
});
(0, node_test_1.test)("replay-safe mutation with BLANK idempotency key is not retried", async () => {
    let calls = 0;
    const core = fakeCore({
        Upsert(_request, _metadata, _options, cb) {
            calls += 1;
            cb(unavailable(), null);
        },
    });
    await node_assert_1.strict.rejects(() => core.unary(DATA_BROKER, "Upsert", { idempotency_key: "   " }));
    node_assert_1.strict.equal(calls, 1);
});
(0, node_test_1.test)("non-replay-safe mutation is not retried even WITH an idempotency key", async () => {
    let calls = 0;
    const core = fakeCore({
        MarkSagaReviewed(_request, _metadata, _options, cb) {
            calls += 1;
            cb(unavailable(), null);
        },
    });
    await node_assert_1.strict.rejects(() => core.unary(DATA_BROKER, "MarkSagaReviewed", { idempotency_key: "key-123" }), (err) => err instanceof generatedClient_1.UdbError && err.code === grpc.status.UNAVAILABLE);
    node_assert_1.strict.equal(calls, 1);
});
