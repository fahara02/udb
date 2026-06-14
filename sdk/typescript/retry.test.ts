import { strict as assert } from "node:assert";
import { test } from "node:test";

import * as grpc from "@grpc/grpc-js";

import { UdbCore, UdbError } from "./generatedClient";

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
