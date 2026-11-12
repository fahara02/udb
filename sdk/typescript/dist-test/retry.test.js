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
(0, node_test_1.test)("replay-safe mutation with CONTEXT-ONLY idempotency key is not retried", async () => {
    let calls = 0;
    const core = fakeCore({
        Upsert(_request, _metadata, _options, cb) {
            calls += 1;
            cb(unavailable(), null);
        },
    });
    await node_assert_1.strict.rejects(() => core.unary(DATA_BROKER, "Upsert", { context: { idempotency_key: "ctx-key" } }), (err) => err instanceof generatedClient_1.UdbError && err.code === grpc.status.UNAVAILABLE);
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
// ── Broker retry_after_ms (rate limit / fair admission / overload) ───────────
//
// A RESOURCE_EXHAUSTED refusal carries `retry_after_ms` in the
// `udb-error-detail-bin` trailer. The retry must wait at least that long,
// must not sleep past the call's deadline, and a delay longer than
// `maxRetryAfterMs` (default 10 s) is returned to the caller at once.
/** Prost-encode an ErrorDetail carrying only field 5 (retry_after_ms) and
 *  field 4 (retryable = true). */
function retryAfterTrailer(retryAfterMs) {
    const varint = (n) => {
        const out = [];
        while (n > 0x7f) {
            out.push((n & 0x7f) | 0x80);
            n = Math.floor(n / 128);
        }
        out.push(n);
        return out;
    };
    return Buffer.from([0x20, 0x01, 0x28, ...varint(retryAfterMs)]);
}
function exhausted(retryAfterMs) {
    const metadata = new grpc.Metadata();
    metadata.set("udb-error-detail-bin", retryAfterTrailer(retryAfterMs));
    return Object.assign(new Error("rate limited"), {
        code: grpc.status.RESOURCE_EXHAUSTED,
        details: "rate limited",
        metadata,
    });
}
function rateLimitedCore(failures, retryAfterMs, sleeps, retry = {}) {
    let calls = 0;
    const core = fakeCore({
        ListSagas(_request, _metadata, _options, cb) {
            calls += 1;
            if (calls <= failures) {
                cb(exhausted(retryAfterMs), null);
                return;
            }
            cb(null, { ok: true });
        },
    });
    core.retry.retryableCodes = [grpc.status.UNAVAILABLE, grpc.status.RESOURCE_EXHAUSTED];
    core.retry.initialBackoffMs = 100;
    core.retry.maxBackoffMs = 800;
    Object.assign(core.retry, retry);
    core.delay = async (ms) => {
        sleeps.push(ms);
    };
    return { core, calls: () => calls };
}
(0, node_test_1.test)("retry waits at least the broker's retry_after_ms (repeated 3x)", async () => {
    for (let run = 0; run < 3; run += 1) {
        const sleeps = [];
        const { core, calls } = rateLimitedCore(1, 1200, sleeps);
        const result = await core.unary(DATA_BROKER, "ListSagas", {});
        node_assert_1.strict.deepEqual(result, { ok: true });
        node_assert_1.strict.equal(calls(), 2);
        node_assert_1.strict.equal(sleeps.length, 1);
        node_assert_1.strict.ok(sleeps[0] >= 1200, `run ${run}: slept ${sleeps[0]}ms, want >= 1200`);
    }
});
(0, node_test_1.test)("retry_after_ms past the call deadline: no sleep, original error raised", async () => {
    const sleeps = [];
    const { core, calls } = rateLimitedCore(1, 5000, sleeps);
    await node_assert_1.strict.rejects(() => core.unary(DATA_BROKER, "ListSagas", {}, { deadlineMs: 1000 }), (err) => err instanceof generatedClient_1.UdbError &&
        err.code === grpc.status.RESOURCE_EXHAUSTED &&
        err.retryAfterMs === 5000 &&
        Number(err.detail?.retry_after_ms) === 5000);
    node_assert_1.strict.equal(calls(), 1);
    node_assert_1.strict.deepEqual(sleeps, []);
});
(0, node_test_1.test)("retry_after_ms above maxRetryAfterMs: no sleep, one call, error returned", async () => {
    const cases = [
        [90_000, { maxRetryAfterMs: 2500 }],
        [60_000, {}], // default cap
    ];
    for (const [retryAfterMs, retry] of cases) {
        const sleeps = [];
        const { core, calls } = rateLimitedCore(1, retryAfterMs, sleeps, retry);
        await node_assert_1.strict.rejects(() => core.unary(DATA_BROKER, "ListSagas", {}), (err) => err instanceof generatedClient_1.UdbError &&
            err.code === grpc.status.RESOURCE_EXHAUSTED &&
            err.retryAfterMs === retryAfterMs &&
            Number(err.detail?.retry_after_ms) === retryAfterMs);
        node_assert_1.strict.equal(calls(), 1);
        node_assert_1.strict.deepEqual(sleeps, []);
    }
});
(0, node_test_1.test)("default maxRetryAfterMs is 10 s and a delay at the cap is waited for", async () => {
    node_assert_1.strict.equal(generatedClient_1.DEFAULT_RETRY_POLICY.maxRetryAfterMs, 10_000);
    const sleeps = [];
    const { core, calls } = rateLimitedCore(1, 10_000, sleeps);
    node_assert_1.strict.deepEqual(await core.unary(DATA_BROKER, "ListSagas", {}), { ok: true });
    node_assert_1.strict.equal(calls(), 2);
    node_assert_1.strict.deepEqual(sleeps, [10_000]);
});
(0, node_test_1.test)("UdbError.retryAfterMs is 0 when the broker sent no retry_after_ms", async () => {
    const sleeps = [];
    const { core } = rateLimitedCore(0, 0, sleeps);
    const err = new generatedClient_1.UdbError("x/y", unavailable(), undefined);
    node_assert_1.strict.equal(err.retryAfterMs, 0);
    node_assert_1.strict.deepEqual(await core.unary(DATA_BROKER, "ListSagas", {}), { ok: true });
});
