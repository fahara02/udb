// Unit tests for the SDK helper layer added in chapter 09:
//  - the prost ErrorDetail decoder (09.1.1.x) over `udb-error-detail-bin`,
//  - the typed WriteReceipt / ReadFence helpers against lane 07's committed
//    machine-derived golden fixture (09.3.1.x),
//  - the send-one / await-first stream helpers (09.7.x),
//  - the conformance-proof TOTP/dev-echo helper (13.2.1.2).
//
// Pure unit tests — no live server. Run via:
//   npx tsc -p tsconfig.test.json && node --test dist-test/sdkhelpers.test.js

import { strict as assert } from "node:assert";
import * as fs from "node:fs";
import * as path from "node:path";
import { test } from "node:test";

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import * as protobuf from "protobufjs";

import {
  ERROR_KIND_NAMES,
  UdbCore,
  UdbError,
} from "./generatedClient";
import {
  afterWrite,
  consistencyMetadata,
  parseWriteReceipt,
  readFenceFromReceipt,
  receiptFromResponse,
  withReadFence,
  withReadFenceFromReceipt,
  type WriteReceipt,
} from "./consistency";
import { sendOneBidiAwaitFirst, sendOneClientStream } from "./stream";
import { defaultProtoRoot } from "./protoRoot";
import { structToObject } from "./wkt";

// Exercise the real DataBroker request serializer rather than a capturing stub:
// nested filters must cross protobufjs's Struct -> Value -> Struct converters.
function selectWireMethod(): grpc.MethodDefinition<any, any> {
  const root = defaultProtoRoot();
  const definition = protoLoader.loadSync(path.join(root, "udb/services/v1/data_broker.proto"), {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    includeDirs: [root, path.resolve(root, "../third_party/googleapis")],
  });
  const service = definition["udb.services.v1.DataBroker"] as grpc.ServiceDefinition;
  assert.ok(service.Select, "actual canonical Select serializer must exist");
  return service.Select;
}

test("actual Select serializer preserves nested predicates and literal fields data", () => {
  const method = selectWireMethod();
  const filter = {
    tenant_id: "owned-tenant",
    record_id: { $in: ["own-row", "peer-row"] },
    $or: [{ project_id: "own-project" }, { project_id: "peer-project" }],
    fields: { stringValue: "ordinary user data", nested: { fields: { numberValue: 7 } } },
    values: [null, false, true, 0, 1.5, "", [], {}, ["nested", { accepted: true }]],
  };
  const before = JSON.stringify(filter);
  for (let attempt = 0; attempt < 2; attempt++) {
    const bytes = method.requestSerialize({
      message_type: "udb.sdk.live.v1.SdkLiveRecord", filter, limit: 10,
    });
    const decoded = method.requestDeserialize(bytes);
    assert.deepEqual(structToObject(decoded.filter), filter);
  }
  assert.equal(JSON.stringify(filter), before, "normalization must not mutate caller predicates");
});

test("actual Struct messages serialize unchanged and wrapper converters preserve depth", () => {
  const schema = protobuf.common.get("google/protobuf/struct.proto");
  assert.ok(schema, "protobufjs's canonical Struct descriptor must exist");
  const struct = protobuf.Root.fromJSON(schema).lookupType("google.protobuf.Struct");
  const converter = struct as unknown as {
    fromObject(value: unknown, depth?: number): protobuf.Message;
    toObject(value: protobuf.Message, options?: protobuf.IConversionOptions, depth?: number): object;
  };
  const input = {
    fields: { stringValue: "literal" }, predicate: { $in: ["a", "b"] },
    emptyString: "", zero: 0, falseValue: false, nullValue: null,
    number: 1.5, trueValue: true,
    literalKeys: JSON.parse('{"__proto__":{"stringValue":"own data"},"constructor":false,"prototype":0}'),
    nested: [{ text: "", count: 0, enabled: false, missing: null }, [false, 0, "", null]],
  };
  const message = struct.fromObject(input);
  assert.equal(struct.fromObject(message), message, "a genuine protobuf message remains the same instance");
  assert.deepEqual(structToObject(message), input, "only actual oneof members decode from a genuine message");
  const literalKeys = structToObject(message).literalKeys as Record<string, unknown>;
  assert.equal(Object.getPrototypeOf(literalKeys), Object.prototype);
  assert.ok(Object.prototype.hasOwnProperty.call(literalKeys, "__proto__"));
  assert.deepEqual(literalKeys["__proto__"], { stringValue: "own data" });
  const before = JSON.stringify(struct.toObject(message, { oneofs: true }));
  const method = selectWireMethod();
  const bytes = method.requestSerialize({ filter: message });
  assert.deepEqual(bytes, method.requestSerialize({ filter: input }), "foreign-root and plain JSON Select bytes match");
  const decoded = method.requestDeserialize(bytes);
  assert.deepEqual(structToObject(decoded.filter), input);
  assert.equal(JSON.stringify(struct.toObject(message, { oneofs: true })), before, "foreign-root conversion leaves its message untouched");
  const limit = (protobuf.util as unknown as { recursionLimit: number }).recursionLimit;
  assert.ok(Number.isInteger(limit) && limit > 0);
  assert.throws(() => converter.fromObject({}, limit + 1), /maximum nesting depth exceeded/);
  assert.throws(() => converter.toObject(message, {}, limit + 1), /max depth exceeded/);
  let nested: object = { leaf: "value" };
  for (let depth = 0; depth <= limit; depth++) nested = { nested };
  assert.throws(() => method.requestSerialize({ filter: nested }), /maximum nesting depth exceeded/);
  const cycle: Record<string, unknown> = {};
  cycle.self = cycle;
  assert.throws(() => method.requestSerialize({ filter: cycle }), /maximum nesting depth exceeded/);
  const messageCycle = struct.fromObject({ nested: {} }) as protobuf.Message & {
    fields: Record<string, { structValue: protobuf.Message }>;
  };
  messageCycle.fields.nested.structValue = messageCycle;
  assert.throws(() => method.requestSerialize({ filter: messageCycle }), /maximum nesting depth exceeded/);
});

// ── 09.1: ErrorDetail decode + UdbError.kind/kindName/retryable accessors ─────

test("ERROR_KIND_NAMES maps the ErrorKind enum (0..7)", () => {
  assert.equal(ERROR_KIND_NAMES[3], "QUOTA");
  assert.equal(ERROR_KIND_NAMES[5], "RETRYABLE");
  assert.equal(ERROR_KIND_NAMES[6], "INTERNAL");
  assert.equal(ERROR_KIND_NAMES[7], "VALIDATION");
});

/** Encode a canonical prost ErrorDetail buffer for the unit test. */
function encodeErrorDetail(opts: {
  retryable?: boolean;
  retryAfterMs?: number;
  kind?: number;
  field?: string | null;
  description?: string;
} = {}): Buffer {
  const varint = (n: number): number[] => {
    const out: number[] = [];
    while (n > 0x7f) { out.push((n & 0x7f) | 0x80); n = Math.floor(n / 128); }
    out.push(n & 0x7f);
    return out;
  };
  const lenDelim = (field: number, value: string): number[] => {
    const b = Buffer.from(value, "utf8");
    return [...varint((field << 3) | 2), ...varint(b.length), ...b];
  };
  const v = (field: number, value: number): number[] => [...varint((field << 3) | 0), ...varint(value)];
  const field = opts.field ?? "email";
  const description = opts.description ?? "must be a valid email";
  const fieldViolation = Buffer.from([
    ...lenDelim(1, field),
    ...lenDelim(2, description),
  ]);
  const nested = (field: number, bytes: Buffer): number[] => [
    ...varint((field << 3) | 2),
    ...varint(bytes.length),
    ...bytes,
  ];
  const retryable = opts.retryable === true ? 1 : 0;
  const retryAfterMs = opts.retryAfterMs ?? 0;
  const kind = opts.kind ?? 7;
  const fields = [...v(4, retryable), ...v(5, retryAfterMs), ...v(8, kind)];
  if (opts.field !== null) fields.push(...nested(9, fieldViolation));
  return Buffer.from(fields);
}

test("the real decoder reads validation field violations off udb-error-detail-bin", async () => {
  const md = new grpc.Metadata();
  md.set("udb-error-detail-bin", encodeErrorDetail());
  const erroringStub: any = {
    DoThing: (_req: any, _meta: any, _opts: any, cb: any) =>
      cb({ code: grpc.status.UNKNOWN, details: "boom", message: "boom", metadata: md, name: "Error" }),
  };
  const core: any = Object.create(UdbCore.prototype);
  (core as any).retry = { maxAttempts: 1, retryableCodes: [] };
  (core as any).stub = () => erroringStub;
  (core as any).metadataFor = () => new grpc.Metadata();
  (core as any).callMeta = () => ({});
  (core as any).isRetryable = () => false;
  await assert.rejects(
    () => UdbCore.prototype.unary.call(core, "svc", "DoThing", {}, { noRetry: true }),
    (e: any) => {
      assert.ok(e instanceof UdbError);
      assert.equal(e.retryable, false);
      assert.equal(e.kind, 7);
      assert.equal(e.kindName, "VALIDATION");
      assert.deepEqual(e.fieldViolations, [
        { field: "email", description: "must be a valid email" },
      ]);
      assert.ok(Buffer.isBuffer(e.detail?.rawBytes));
      return true;
    },
  );
});

test("the real decoder preserves retryable quota backoff detail", async () => {
  const md = new grpc.Metadata();
  md.set("udb-error-detail-bin", encodeErrorDetail({
    retryable: true,
    retryAfterMs: 250,
    kind: 3,
    field: null,
  }));
  const erroringStub: any = {
    DoThing: (_req: any, _meta: any, _opts: any, cb: any) =>
      cb({ code: grpc.status.RESOURCE_EXHAUSTED, details: "quota", message: "quota", metadata: md, name: "Error" }),
  };
  const core: any = Object.create(UdbCore.prototype);
  (core as any).retry = { maxAttempts: 1, retryableCodes: [] };
  (core as any).stub = () => erroringStub;
  (core as any).metadataFor = () => new grpc.Metadata();
  (core as any).callMeta = () => ({});
  (core as any).isRetryable = () => false;
  await assert.rejects(
    () => UdbCore.prototype.unary.call(core, "svc", "DoThing", {}, { noRetry: true }),
    (e: any) => {
      assert.ok(e instanceof UdbError);
      assert.equal(e.retryable, true);
      assert.equal(e.kind, 3);
      assert.equal(e.kindName, "QUOTA");
      assert.equal(e.detail?.retry_after_ms, 250);
      assert.deepEqual(e.fieldViolations, []);
      return true;
    },
  );
});

test("trailerless transport errors synthesize the same retryable detail shape", async () => {
  const erroringStub: any = {
    DoThing: (_req: any, _meta: any, _opts: any, cb: any) =>
      cb({ code: grpc.status.DEADLINE_EXCEEDED, details: "deadline", message: "deadline", metadata: new grpc.Metadata(), name: "Error" }),
  };
  const core: any = Object.create(UdbCore.prototype);
  (core as any).retry = { maxAttempts: 1, retryableCodes: [] };
  (core as any).stub = () => erroringStub;
  (core as any).metadataFor = () => new grpc.Metadata();
  (core as any).callMeta = () => ({});
  (core as any).isRetryable = () => false;
  await assert.rejects(
    () => UdbCore.prototype.unary.call(core, "svc", "DoThing", {}, { noRetry: true }),
    (e: any) => {
      assert.ok(e instanceof UdbError);
      assert.equal(e.detail?.backend, "transport");
      assert.equal(e.detail?.operation, "deadline_exceeded");
      assert.equal(e.retryable, true);
      assert.equal(e.kindName, "RETRYABLE");
      assert.equal(e.detail?.retry_after_ms, 0);
      assert.deepEqual(e.fieldViolations, []);
      return true;
    },
  );
});

test("trailerless cancellation synthesizes non-retryable transport detail", async () => {
  const erroringStub: any = {
    DoThing: (_req: any, _meta: any, _opts: any, cb: any) =>
      cb({ code: grpc.status.CANCELLED, details: "cancelled", message: "cancelled", metadata: new grpc.Metadata(), name: "Error" }),
  };
  const core: any = Object.create(UdbCore.prototype);
  (core as any).retry = { maxAttempts: 1, retryableCodes: [] };
  (core as any).stub = () => erroringStub;
  (core as any).metadataFor = () => new grpc.Metadata();
  (core as any).callMeta = () => ({});
  (core as any).isRetryable = () => false;
  await assert.rejects(
    () => UdbCore.prototype.unary.call(core, "svc", "DoThing", {}, { noRetry: true }),
    (e: any) => {
      assert.ok(e instanceof UdbError);
      assert.equal(e.detail?.backend, "transport");
      assert.equal(e.detail?.operation, "cancelled");
      assert.equal(e.retryable, false);
      assert.equal(e.kindName, "RETRYABLE");
      assert.equal(e.detail?.retry_after_ms, 0);
      assert.deepEqual(e.fieldViolations, []);
      return true;
    },
  );
});

// Guard label: UdbError exposes kind / kindName / retryable.
test("the real decoder reads retryable/kind/capability_required", () => {
  const cause = {
    code: grpc.status.UNKNOWN,
    details: "boom",
    message: "boom",
    metadata: new grpc.Metadata(),
    name: "Error",
  } as unknown as grpc.ServiceError;
  const err = new UdbError("svc/M", cause, {
    retryable: true,
    kind: 5,
    kindName: ERROR_KIND_NAMES[5],
    capability_required: "x",
  });
  assert.equal(err.retryable, true);
  assert.equal(err.kind, 5);
  assert.equal(err.kindName, "RETRYABLE");
  assert.equal(err.detail?.capability_required, "x");
  // No detail at all => retryable false, kind undefined (starved getter is safe).
  const bare = new UdbError("svc/M", cause);
  assert.equal(bare.retryable, false);
  assert.equal(bare.kind, undefined);
});

// ── 09.3: WriteReceipt / ReadFence against lane 07's golden fixture ───────────

const golden = JSON.parse(
  fs.readFileSync(
    path.resolve(__dirname, "../../../docs/generated/consistency-golden.json"),
    "utf8",
  ),
);

test("parseWriteReceipt parses the golden write_receipt with no missing keys", () => {
  const receipt = parseWriteReceipt(JSON.stringify(golden.write_receipt))!;
  assert.equal(receipt.source_lsn, golden.write_receipt.source_lsn);
  assert.equal(receipt.outbox_seq, golden.write_receipt.outbox_seq);
  assert.deepEqual(receipt.projection_task_ids, golden.write_receipt.projection_task_ids);
  assert.equal(receipt.manifest_checksum, golden.write_receipt.manifest_checksum);
  assert.equal(receipt.written_at_unix_ms, golden.write_receipt.written_at_unix_ms);
});

test("readFenceFromReceipt maps source_lsn->min_outbox_lsn matching the golden fence", () => {
  const receipt = golden.write_receipt as WriteReceipt;
  const fence = readFenceFromReceipt(receipt, golden.read_fence.max_wait_ms);
  assert.equal(fence.min_outbox_lsn, golden.read_fence.min_outbox_lsn);
  assert.deepEqual(fence.projection_task_ids, golden.read_fence.projection_task_ids);
  assert.equal(fence.max_wait_ms, golden.read_fence.max_wait_ms);
});

test("parseWriteReceipt tolerates empty / {} (no-op receipt)", () => {
  assert.equal(parseWriteReceipt(""), null);
  const empty = parseWriteReceipt("{}");
  assert.ok(empty && empty.source_lsn === "" && empty.outbox_seq === 0);
});

test("receiptFromResponse reads write_receipt_json (field 7)", () => {
  const resp = { write_receipt_json: JSON.stringify(golden.write_receipt) };
  const receipt = receiptFromResponse(resp)!;
  assert.equal(receipt.source_lsn, golden.write_receipt.source_lsn);
  assert.equal(receiptFromResponse({}), null);
});

test("withReadFence omits empty fields and sets the x-udb-read-fence header", () => {
  const fence = readFenceFromReceipt(
    { source_lsn: "", outbox_seq: 0, projection_task_ids: [], manifest_checksum: "", written_at_unix_ms: 0 },
    1000,
  );
  // empty source_lsn + empty task ids are omitted (skip_serializing_if mirror)
  const json = JSON.stringify(fence);
  assert.ok(!json.includes("min_outbox_lsn"));
  assert.ok(!json.includes("projection_task_ids"));
  assert.ok(json.includes("max_wait_ms"));
  const opts = withReadFence(readFenceFromReceipt(golden.write_receipt as WriteReceipt, 2500));
  assert.ok(opts.headers && typeof opts.headers["x-udb-read-fence"] === "string");
});

test("afterWrite / withReadFenceFromReceipt = readFenceFromReceipt + withReadFence", () => {
  const receipt = golden.write_receipt as WriteReceipt;
  // Both helpers produce the same x-udb-read-fence header as the explicit compose.
  const explicit = withReadFence(readFenceFromReceipt(receipt, 2500));
  const oneShot = withReadFenceFromReceipt(receipt, 2500);
  assert.equal(
    oneShot.headers!["x-udb-read-fence"],
    explicit.headers!["x-udb-read-fence"],
  );
  // afterWrite is the naming-contract alias (default maxWaitMs); header is set.
  const aw = afterWrite(receipt);
  assert.ok(typeof aw.headers!["x-udb-read-fence"] === "string");
  // The grouped accessor used as `metadata.afterWrite(...)` in the spec.
  assert.equal(consistencyMetadata.afterWrite, afterWrite);
  assert.equal(consistencyMetadata.withReadFenceFromReceipt, withReadFenceFromReceipt);
});

// ── 09.7: stream send-one / await-first helpers ──────────────────────────────

function fakeStreamCore() {
  const writes: any[] = [];
  let ended = false;
  const core: any = Object.create(UdbCore.prototype);
  core.clientStream = () => ({
    stream: { write: (m: any) => writes.push(m), end: () => (ended = true) },
    response: Promise.resolve({ ok: true }),
  });
  return { core: core as UdbCore, writes, didEnd: () => ended };
}

test("sendOneClientStream writes exactly one message, ends, returns the response", async () => {
  const { core, writes, didEnd } = fakeStreamCore();
  const resp: any = await sendOneClientStream(core, "svc", "M", { a: 1 });
  assert.deepEqual(writes, [{ a: 1 }]);
  assert.ok(didEnd());
  assert.deepEqual(resp, { ok: true });
});

test("sendOneBidiAwaitFirst resolves on the first data; ignores later responses", async () => {
  const handlers: Record<string, (arg?: any) => void> = {};
  const duplex: any = {
    on: (ev: string, cb: any) => (handlers[ev] = cb),
    write: () => {},
  };
  const core: any = Object.create(UdbCore.prototype);
  core.bidiStream = () => duplex;
  const p = sendOneBidiAwaitFirst(core as UdbCore, "svc", "M", { req: 1 });
  handlers["data"]({ first: true });
  handlers["data"]({ second: true }); // ignored
  assert.deepEqual(await p, { first: true });
});

test("sendOneBidiAwaitFirst rejects when the stream ends before any data", async () => {
  const handlers: Record<string, (arg?: any) => void> = {};
  const duplex: any = { on: (ev: string, cb: any) => (handlers[ev] = cb), write: () => {} };
  const core: any = Object.create(UdbCore.prototype);
  core.bidiStream = () => duplex;
  const p = sendOneBidiAwaitFirst(core as UdbCore, "svc", "M", {});
  handlers["end"]();
  await assert.rejects(p, /ended before any response/);
});
