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

// ── 09.1: ErrorDetail decode + UdbError.kind/kindName/retryable accessors ─────

test("ERROR_KIND_NAMES maps the ErrorKind enum (0..6)", () => {
  assert.equal(ERROR_KIND_NAMES[3], "ALREADY_EXISTS");
  assert.equal(ERROR_KIND_NAMES[5], "RETRYABLE");
  assert.equal(ERROR_KIND_NAMES[6], "INTERNAL");
});

/** Encode a minimal prost ErrorDetail buffer for the unit test. */
function encodeErrorDetail(): Buffer {
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
  // capability_required:"x"(3), retryable:true(4), kind:5(8)
  return Buffer.from([...lenDelim(3, "x"), ...v(4, 1), ...v(8, 5)]);
}

test("the real decoder reads retryable/kind/capability_required off udb-error-detail-bin", async () => {
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
      assert.equal(e.retryable, true);
      assert.equal(e.kind, 5);
      assert.equal(e.kindName, "RETRYABLE");
      assert.equal(e.detail?.capability_required, "x");
      assert.ok(Buffer.isBuffer(e.detail?.rawBytes));
      return true;
    },
  );
});

test("UdbError exposes kind / kindName / retryable from a decoded detail", () => {
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
