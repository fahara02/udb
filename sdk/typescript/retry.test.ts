import { strict as assert } from "node:assert";
import { test } from "node:test";

import * as grpc from "@grpc/grpc-js";

import { UdbCore, UdbError, RPC_REPLAY_SAFE, RPC_OPERATION_KIND } from "./generatedClient";

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
