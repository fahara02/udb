import { strict as assert } from "node:assert";
import { test } from "node:test";

import * as grpc from "@grpc/grpc-js";

import { DEFAULT_RETRY_POLICY, UdbCore, UdbError, RPC_REPLAY_SAFE, RPC_OPERATION_KIND } from "./generatedClient";

const DATA_BROKER = "udb.services.v1.DataBroker";

function unavailable(): grpc.ServiceError {
  return Object.assign(new Error("unavailable"), {
    code: grpc.status.UNAVAILABLE,
    details: "unavailable",
    metadata: new grpc.Metadata(),
  }) as grpc.ServiceError;
}

function fakeCore(stub: Record<string, unknown>): UdbCore {
  const core: any = Object.create(UdbCore.prototype);
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
  return core as UdbCore;
}

test("mutating unary RPC is not retried on retryable status", async () => {
  let calls = 0;
  const core = fakeCore({
    MarkSagaReviewed(_request: any, _metadata: grpc.Metadata, _options: grpc.CallOptions, cb: Function) {
      calls += 1;
      cb(unavailable(), null);
    },
  });

  await assert.rejects(
    () => core.unary(DATA_BROKER, "MarkSagaReviewed", {}),
    (err: unknown) => err instanceof UdbError && (err as UdbError).code === grpc.status.UNAVAILABLE,
  );
  assert.equal(calls, 1);
});

test("read-only unary RPC still retries transient status", async () => {
  let calls = 0;
  const core = fakeCore({
    ListSagas(_request: any, _metadata: grpc.Metadata, _options: grpc.CallOptions, cb: Function) {
      calls += 1;
      if (calls === 1) {
        cb(unavailable(), null);
        return;
      }
      cb(null, { ok: true });
    },
  });

  const result = await core.unary(DATA_BROKER, "ListSagas", {});
  assert.deepEqual(result, { ok: true });
  assert.equal(calls, 2);
});

// ── Replay-safe mutation retry (R1.2) ───────────────────────────────────────
//
// The proto marks Upsert/Delete `replay_safe` (R1.1) — the broker collapses a
// retried duplicate carrying the same idempotency key. So a replay-safe mutation
// may be retried on a transient failure ONLY when a non-empty key is present.

test("catalog: Upsert is replay-safe and a mutation; ListSagas is not", () => {
  // Guards the R1.1 catalog this retry behavior depends on.
  assert.equal(RPC_REPLAY_SAFE[`/${DATA_BROKER}/Upsert`], true);
  assert.equal(RPC_REPLAY_SAFE[`/${DATA_BROKER}/Delete`], true);
  assert.equal(RPC_OPERATION_KIND[`/${DATA_BROKER}/Upsert`], "mutation");
  // A non-replay-safe mutation and a read-only RPC are absent/false.
  assert.notEqual(RPC_REPLAY_SAFE[`/${DATA_BROKER}/MarkSagaReviewed`], true);
  assert.notEqual(RPC_REPLAY_SAFE[`/${DATA_BROKER}/ListSagas`], true);
});

test("replay-safe mutation WITH idempotency key retries then succeeds", async () => {
  let calls = 0;
  const core = fakeCore({
    Upsert(_request: any, _metadata: grpc.Metadata, _options: grpc.CallOptions, cb: Function) {
      calls += 1;
      if (calls === 1) {
        cb(unavailable(), null);
        return;
      }
      cb(null, { was_duplicate: true });
    },
  });

  const result = await core.unary(DATA_BROKER, "Upsert", { idempotency_key: "key-123" });
  assert.deepEqual(result, { was_duplicate: true });
  assert.equal(calls, 2);
});

test("replay-safe mutation WITHOUT idempotency key is not retried", async () => {
  let calls = 0;
  const core = fakeCore({
    Upsert(_request: any, _metadata: grpc.Metadata, _options: grpc.CallOptions, cb: Function) {
      calls += 1;
      cb(unavailable(), null);
    },
  });

  await assert.rejects(
    () => core.unary(DATA_BROKER, "Upsert", {}),
    (err: unknown) => err instanceof UdbError && (err as UdbError).code === grpc.status.UNAVAILABLE,
  );
  assert.equal(calls, 1);
});

test("replay-safe mutation with BLANK idempotency key is not retried", async () => {
  let calls = 0;
  const core = fakeCore({
    Upsert(_request: any, _metadata: grpc.Metadata, _options: grpc.CallOptions, cb: Function) {
      calls += 1;
      cb(unavailable(), null);
    },
  });

  await assert.rejects(() => core.unary(DATA_BROKER, "Upsert", { idempotency_key: "   " }));
  assert.equal(calls, 1);
});

test("replay-safe mutation with CONTEXT-ONLY idempotency key is not retried", async () => {
  let calls = 0;
  const core = fakeCore({
    Upsert(_request: any, _metadata: grpc.Metadata, _options: grpc.CallOptions, cb: Function) {
      calls += 1;
      cb(unavailable(), null);
    },
  });

  await assert.rejects(
    () => core.unary(DATA_BROKER, "Upsert", { context: { idempotency_key: "ctx-key" } }),
    (err: unknown) => err instanceof UdbError && (err as UdbError).code === grpc.status.UNAVAILABLE,
  );
  assert.equal(calls, 1);
});

test("non-replay-safe mutation is not retried even WITH an idempotency key", async () => {
  let calls = 0;
  const core = fakeCore({
    MarkSagaReviewed(_request: any, _metadata: grpc.Metadata, _options: grpc.CallOptions, cb: Function) {
      calls += 1;
      cb(unavailable(), null);
    },
  });

  await assert.rejects(
    () => core.unary(DATA_BROKER, "MarkSagaReviewed", { idempotency_key: "key-123" }),
    (err: unknown) => err instanceof UdbError && (err as UdbError).code === grpc.status.UNAVAILABLE,
  );
  assert.equal(calls, 1);
});

// ── Broker retry_after_ms (rate limit / fair admission / overload) ───────────
//
// A RESOURCE_EXHAUSTED refusal carries `retry_after_ms` in the
// `udb-error-detail-bin` trailer. The retry must wait at least that long,
// must not sleep past the call's deadline, and a delay longer than
// `maxRetryAfterMs` (default 10 s) is returned to the caller at once.

/** Prost-encode an ErrorDetail carrying only field 5 (retry_after_ms) and
 *  field 4 (retryable = true). */
function retryAfterTrailer(retryAfterMs: number): Buffer {
  const varint = (n: number): number[] => {
    const out: number[] = [];
    while (n > 0x7f) {
      out.push((n & 0x7f) | 0x80);
      n = Math.floor(n / 128);
    }
    out.push(n);
    return out;
  };
  return Buffer.from([0x20, 0x01, 0x28, ...varint(retryAfterMs)]);
}

function exhausted(retryAfterMs: number): grpc.ServiceError {
  const metadata = new grpc.Metadata();
  metadata.set("udb-error-detail-bin", retryAfterTrailer(retryAfterMs));
  return Object.assign(new Error("rate limited"), {
    code: grpc.status.RESOURCE_EXHAUSTED,
    details: "rate limited",
    metadata,
  }) as grpc.ServiceError;
}

function rateLimitedCore(
  failures: number,
  retryAfterMs: number,
  sleeps: number[],
  retry: Record<string, unknown> = {},
): { core: UdbCore; calls: () => number } {
  let calls = 0;
  const core: any = fakeCore({
    ListSagas(_request: any, _metadata: grpc.Metadata, _options: grpc.CallOptions, cb: Function) {
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
  core.delay = async (ms: number) => {
    sleeps.push(ms);
  };
  return { core, calls: () => calls };
}

test("retry waits at least the broker's retry_after_ms (repeated 3x)", async () => {
  for (let run = 0; run < 3; run += 1) {
    const sleeps: number[] = [];
    const { core, calls } = rateLimitedCore(1, 1200, sleeps);
    const result = await core.unary(DATA_BROKER, "ListSagas", {});
    assert.deepEqual(result, { ok: true });
    assert.equal(calls(), 2);
    assert.equal(sleeps.length, 1);
    assert.ok(sleeps[0] >= 1200, `run ${run}: slept ${sleeps[0]}ms, want >= 1200`);
  }
});

test("retry_after_ms past the call deadline: no sleep, original error raised", async () => {
  const sleeps: number[] = [];
  const { core, calls } = rateLimitedCore(1, 5000, sleeps);
  await assert.rejects(
    () => core.unary(DATA_BROKER, "ListSagas", {}, { deadlineMs: 1000 }),
    (err: unknown) =>
      err instanceof UdbError &&
      err.code === grpc.status.RESOURCE_EXHAUSTED &&
      err.retryAfterMs === 5000 &&
      Number(err.detail?.retry_after_ms) === 5000,
  );
  assert.equal(calls(), 1);
  assert.deepEqual(sleeps, []);
});

test("retry_after_ms above maxRetryAfterMs: no sleep, one call, error returned", async () => {
  const cases: Array<[number, Record<string, unknown>]> = [
    [90_000, { maxRetryAfterMs: 2500 }],
    [60_000, {}], // default cap
  ];
  for (const [retryAfterMs, retry] of cases) {
    const sleeps: number[] = [];
    const { core, calls } = rateLimitedCore(1, retryAfterMs, sleeps, retry);
    await assert.rejects(
      () => core.unary(DATA_BROKER, "ListSagas", {}),
      (err: unknown) =>
        err instanceof UdbError &&
        err.code === grpc.status.RESOURCE_EXHAUSTED &&
        err.retryAfterMs === retryAfterMs &&
        Number(err.detail?.retry_after_ms) === retryAfterMs,
    );
    assert.equal(calls(), 1);
    assert.deepEqual(sleeps, []);
  }
});

test("default maxRetryAfterMs is 10 s and a delay at the cap is waited for", async () => {
  assert.equal(DEFAULT_RETRY_POLICY.maxRetryAfterMs, 10_000);
  const sleeps: number[] = [];
  const { core, calls } = rateLimitedCore(1, 10_000, sleeps);
  assert.deepEqual(await core.unary(DATA_BROKER, "ListSagas", {}), { ok: true });
  assert.equal(calls(), 2);
  assert.deepEqual(sleeps, [10_000]);
});

test("UdbError.retryAfterMs is 0 when the broker sent no retry_after_ms", async () => {
  const sleeps: number[] = [];
  const { core } = rateLimitedCore(0, 0, sleeps);
  const err = new UdbError("x/y", unavailable(), undefined);
  assert.equal(err.retryAfterMs, 0);
  assert.deepEqual(await core.unary(DATA_BROKER, "ListSagas", {}), { ok: true });
});
