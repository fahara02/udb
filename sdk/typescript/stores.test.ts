import { test } from "node:test";
import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import * as grpc from "@grpc/grpc-js";

import {
  ConflictError,
  ErrorKind,
  ErrorCode,
  InMemoryDataPlane,
  NotFoundError,
  Table,
  TxScope,
  codeOf,
  decodeEnvelope,
  inspect,
  tx,
  consume,
} from "./stores";
import { UdbError } from "./generatedClient";
import { runStoreCases, storeCaseKeys } from "./test-support/store-conformance";

// T2: the cross-SDK typed-store cases (shared with the Go runner).
test("Table meets the shared conformance cases", async (t) =>
  runStoreCases(t, () => new InMemoryDataPlane(storeCaseKeys)));

test("a key reused for changed input is not a retryable CAS conflict", () => {
  const cause = Object.assign(new Error("request refused"), {
    code: grpc.status.FAILED_PRECONDITION, details: "request refused", metadata: new grpc.Metadata(),
  });
  const err = new UdbError("DataBroker/Update", cause, {
    reason: "UDB_IDEMPOTENCY_REUSE", kind: ErrorKind.CONFLICT,
  });
  assert.equal(codeOf(err), ErrorCode.Precondition);
  const typedOnly = new UdbError("DataBroker/Update", cause, {
    reason: "UDB_FUTURE_CONSTRAINT", kind: ErrorKind.UNIQUE,
  });
  assert.equal(codeOf(typedOnly), ErrorCode.Unique);
});

test("a zero-affected scope guard response is not a successful typed upsert", async () => {
  for (const affected_rows of [0, "0", undefined]) {
    const plane = { unary: async () => ({ affected_rows }) };
    const table = new Table<Record<string, unknown>>(plane, "udb.core.storage.entity.v1.File", { key: ["file_id"] });
    await assert.rejects(table.upsert({ file_id: "foreign-row" }), NotFoundError);
  }
});

test("inspect maps reason codes and the new detail fields", () => {
  const cause: any = Object.assign(new Error("dup"), { code: 6 });
  const err = new UdbError("DataBroker/Upsert", cause, {
    reason: "UDB_UNIQUE_VIOLATION",
    constraint: "users_email_key",
    column: "email",
    fix_hint: "use the existing row",
    missing: { scope: "udb:data:write" },
  });
  const info = inspect(err);
  assert.equal(info.code, ErrorCode.Unique);
  assert.equal(info.constraint, "users_email_key");
  assert.equal(info.column, "email");
  assert.deepEqual(info.missing, { scope: "udb:data:write" });
  assert.equal(codeOf(new NotFoundError("x")), ErrorCode.NotFound);
  assert.equal(codeOf(new ConflictError("x")), ErrorCode.Conflict);
  assert.equal(codeOf(new Error("plain")), ErrorCode.Unknown);
});

test("tx scope emits a versioned envelope with the writes", () => {
  const scope = new TxScope("corr-1");
  scope.upsert("acme.v1.Order", { id: "o1" });
  scope.emit({ topic: "acme.order.placed.v1", eventType: "acme.OrderPlaced", documentId: "o1", payload: { id: "o1" } });
  assert.equal(scope.mutations.length, 2);
  const event = scope.mutations[1];
  assert.equal(event.operation, "enqueue_outbox_event");
  assert.equal(event.collection, "acme.order.placed.v1");
  assert.equal(event.payload.envelope_version, 2);
  assert.equal(event.payload.correlation_id, "corr-1");
});

test("decodeEnvelope unwraps a nested journal envelope and refuses unversioned, old and newer versions", () => {
  const nested = JSON.stringify({ payload: { event_type: "x", payload: { a: 1 }, envelope_version: 2 } });
  assert.deepEqual(decodeEnvelope(nested).payload, { a: 1 });
  assert.throws(() => decodeEnvelope(JSON.stringify({ event_type: "x", payload: {}, envelope_version: 3 })), /upgrade the SDK/);
  assert.throws(() => decodeEnvelope(JSON.stringify({ event_type: "x", payload: {} })), /no envelope_version/);
  assert.throws(() => decodeEnvelope(JSON.stringify({ event_type: "x", payload: {}, envelope_version: 1 })), /not supported/);
});

test("tx requires a commit acknowledgement and preserves the typed failure trailer", async () => {
  const conflict = new UdbError("DataBroker/BeginTx", Object.assign(new Error("CAS failed"), { code: 10 }) as any, { reason: "UDB_CAS_CONFLICT" });
  const run = (statuses: any[], error?: unknown) => tx({
    unary: async () => ({}),
    bidiStream: () => {
      const stream = Object.assign(new EventEmitter(), {
        write: () => {},
        end: () => queueMicrotask(() => {
          for (const status of statuses) stream.emit("data", status);
          if (error) stream.emit("error", error);
          stream.emit("end");
        }),
      });
      return stream;
    },
  }, () => {});
  await assert.rejects(run([{ state: "TX_STATE_OPEN" }]), /without a commit acknowledgement/);
  await assert.rejects(run([{ state: "TX_STATE_ERROR", message: "CAS failed" }], { udb: conflict }), (err) => err === conflict && codeOf(err) === ErrorCode.Conflict);
  await assert.rejects(run([{ state: "TX_STATE_ERROR", message: "opaque operator text", code: 9,
    error_detail: { reason: "UDB_CAS_CONFLICT" } }]), (err) => codeOf(err) === ErrorCode.Conflict);
  await run([{ state: "TX_STATE_COMMITTED" }]);
});

test("a failed consumer handler cannot acknowledge a queued later event", async () => {
  const controller = new AbortController();
  const handled: string[] = [];
  const acked: string[] = [];
  await consume({
    unary: async (_service, _method, request) => { acked.push(request.event_id); return {}; },
    serverStream: () => {
      const stream = Object.assign(new EventEmitter(), { cancel: () => stream.emit("error", { code: 1 }) });
      queueMicrotask(() => {
        for (const event_id of ["head", "tail"]) stream.emit("data", { event_id, payload_json: JSON.stringify({ envelope_version: 2, payload: {} }) });
        stream.emit("end");
      });
      return stream;
    },
  }, "consumer", "topic", async (event) => {
    handled.push(event.id);
    throw new Error("head failed");
  }, { signal: controller.signal, onError: () => controller.abort() });
  assert.deepEqual(handled, ["head"]);
  assert.deepEqual(acked, []);
});

test("consumer acknowledges progress markers in order without handling them", async () => {
  const controller = new AbortController();
  const handled: string[] = [];
  const acked: string[] = [];
  let subscribed: any;
  const event = (event_id: string) => ({ event_id, payload_json: JSON.stringify({ envelope_version: 2, payload: {} }) });
  await consume({
    unary: async (_service, _method, request) => { acked.push(request.event_id); return {}; },
    serverStream: (_service, _method, request) => {
      subscribed = request;
      const stream = Object.assign(new EventEmitter(), { cancel: () => stream.emit("error", { code: 1 }) });
      queueMicrotask(() => {
        stream.emit("data", event("a"));
        stream.emit("data", { event_id: "marker", topic: "udb.cdc.progress.v1", progress_only: true,
          payload_json: JSON.stringify({ kind: "progress", journal_position: "7", caught_up: true }) });
        stream.emit("data", event("b"));
        stream.emit("end");
      });
      return stream;
    },
  }, "consumer", "topic", async (consumed) => {
    handled.push(consumed.id);
  }, { signal: controller.signal, onError: () => controller.abort(), fromEarliest: true });
  assert.deepEqual(handled, ["a", "b"]);
  assert.deepEqual(acked, ["a", "marker", "b"]);
  assert.equal(subscribed.progress_markers, true);
  assert.equal(subscribed.start, "CDC_CONSUMER_START_EARLIEST");
});

test("consumer stops on an unsupported envelope version without acknowledging it", async () => {
  let acked = false;
  await assert.rejects(consume({
    unary: async () => { acked = true; return {}; },
    serverStream: () => {
      const stream = Object.assign(new EventEmitter(), { cancel: () => stream.emit("error", { code: 1 }) });
      queueMicrotask(() => stream.emit("data", { event_id: "new", payload_json: JSON.stringify({ envelope_version: 3, payload: {} }) }));
      return stream;
    },
  }, "consumer", "topic", async () => assert.fail("unknown envelope cannot be delivered")), /upgrade the SDK/);
  assert.equal(acked, false);
});
