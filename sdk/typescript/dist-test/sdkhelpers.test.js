"use strict";
// Unit tests for the SDK helper layer added in chapter 09:
//  - the prost ErrorDetail decoder (09.1.1.x) over `udb-error-detail-bin`,
//  - the typed WriteReceipt / ReadFence helpers against lane 07's committed
//    machine-derived golden fixture (09.3.1.x),
//  - the send-one / await-first stream helpers (09.7.x),
//  - the conformance-proof TOTP/dev-echo helper (13.2.1.2).
//
// Pure unit tests — no live server. Run via:
//   npx tsc -p tsconfig.test.json && node --test dist-test/sdkhelpers.test.js
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
const fs = __importStar(require("node:fs"));
const path = __importStar(require("node:path"));
const node_test_1 = require("node:test");
const grpc = __importStar(require("@grpc/grpc-js"));
const generatedClient_1 = require("./generatedClient");
const consistency_1 = require("./consistency");
const stream_1 = require("./stream");
// ── 09.1: ErrorDetail decode + UdbError.kind/kindName/retryable accessors ─────
(0, node_test_1.test)("ERROR_KIND_NAMES maps the ErrorKind enum (0..6)", () => {
    node_assert_1.strict.equal(generatedClient_1.ERROR_KIND_NAMES[3], "ALREADY_EXISTS");
    node_assert_1.strict.equal(generatedClient_1.ERROR_KIND_NAMES[5], "RETRYABLE");
    node_assert_1.strict.equal(generatedClient_1.ERROR_KIND_NAMES[6], "INTERNAL");
});
/** Encode a minimal prost ErrorDetail buffer for the unit test. */
function encodeErrorDetail() {
    const varint = (n) => {
        const out = [];
        while (n > 0x7f) {
            out.push((n & 0x7f) | 0x80);
            n = Math.floor(n / 128);
        }
        out.push(n & 0x7f);
        return out;
    };
    const lenDelim = (field, value) => {
        const b = Buffer.from(value, "utf8");
        return [...varint((field << 3) | 2), ...varint(b.length), ...b];
    };
    const v = (field, value) => [...varint((field << 3) | 0), ...varint(value)];
    // capability_required:"x"(3), retryable:true(4), kind:5(8)
    return Buffer.from([...lenDelim(3, "x"), ...v(4, 1), ...v(8, 5)]);
}
(0, node_test_1.test)("the real decoder reads retryable/kind/capability_required off udb-error-detail-bin", async () => {
    const md = new grpc.Metadata();
    md.set("udb-error-detail-bin", encodeErrorDetail());
    const erroringStub = {
        DoThing: (_req, _meta, _opts, cb) => cb({ code: grpc.status.UNKNOWN, details: "boom", message: "boom", metadata: md, name: "Error" }),
    };
    const core = Object.create(generatedClient_1.UdbCore.prototype);
    core.retry = { maxAttempts: 1, retryableCodes: [] };
    core.stub = () => erroringStub;
    core.metadataFor = () => new grpc.Metadata();
    core.callMeta = () => ({});
    core.isRetryable = () => false;
    await node_assert_1.strict.rejects(() => generatedClient_1.UdbCore.prototype.unary.call(core, "svc", "DoThing", {}, { noRetry: true }), (e) => {
        node_assert_1.strict.ok(e instanceof generatedClient_1.UdbError);
        node_assert_1.strict.equal(e.retryable, true);
        node_assert_1.strict.equal(e.kind, 5);
        node_assert_1.strict.equal(e.kindName, "RETRYABLE");
        node_assert_1.strict.equal(e.detail?.capability_required, "x");
        node_assert_1.strict.ok(Buffer.isBuffer(e.detail?.rawBytes));
        return true;
    });
});
(0, node_test_1.test)("UdbError exposes kind / kindName / retryable from a decoded detail", () => {
    const cause = {
        code: grpc.status.UNKNOWN,
        details: "boom",
        message: "boom",
        metadata: new grpc.Metadata(),
        name: "Error",
    };
    const err = new generatedClient_1.UdbError("svc/M", cause, {
        retryable: true,
        kind: 5,
        kindName: generatedClient_1.ERROR_KIND_NAMES[5],
        capability_required: "x",
    });
    node_assert_1.strict.equal(err.retryable, true);
    node_assert_1.strict.equal(err.kind, 5);
    node_assert_1.strict.equal(err.kindName, "RETRYABLE");
    // No detail at all => retryable false, kind undefined (starved getter is safe).
    const bare = new generatedClient_1.UdbError("svc/M", cause);
    node_assert_1.strict.equal(bare.retryable, false);
    node_assert_1.strict.equal(bare.kind, undefined);
});
// ── 09.3: WriteReceipt / ReadFence against lane 07's golden fixture ───────────
const golden = JSON.parse(fs.readFileSync(path.resolve(__dirname, "../../../docs/generated/consistency-golden.json"), "utf8"));
(0, node_test_1.test)("parseWriteReceipt parses the golden write_receipt with no missing keys", () => {
    const receipt = (0, consistency_1.parseWriteReceipt)(JSON.stringify(golden.write_receipt));
    node_assert_1.strict.equal(receipt.source_lsn, golden.write_receipt.source_lsn);
    node_assert_1.strict.equal(receipt.outbox_seq, golden.write_receipt.outbox_seq);
    node_assert_1.strict.deepEqual(receipt.projection_task_ids, golden.write_receipt.projection_task_ids);
    node_assert_1.strict.equal(receipt.manifest_checksum, golden.write_receipt.manifest_checksum);
    node_assert_1.strict.equal(receipt.written_at_unix_ms, golden.write_receipt.written_at_unix_ms);
});
(0, node_test_1.test)("readFenceFromReceipt maps source_lsn->min_outbox_lsn matching the golden fence", () => {
    const receipt = golden.write_receipt;
    const fence = (0, consistency_1.readFenceFromReceipt)(receipt, golden.read_fence.max_wait_ms);
    node_assert_1.strict.equal(fence.min_outbox_lsn, golden.read_fence.min_outbox_lsn);
    node_assert_1.strict.deepEqual(fence.projection_task_ids, golden.read_fence.projection_task_ids);
    node_assert_1.strict.equal(fence.max_wait_ms, golden.read_fence.max_wait_ms);
});
(0, node_test_1.test)("parseWriteReceipt tolerates empty / {} (no-op receipt)", () => {
    node_assert_1.strict.equal((0, consistency_1.parseWriteReceipt)(""), null);
    const empty = (0, consistency_1.parseWriteReceipt)("{}");
    node_assert_1.strict.ok(empty && empty.source_lsn === "" && empty.outbox_seq === 0);
});
(0, node_test_1.test)("receiptFromResponse reads write_receipt_json (field 7)", () => {
    const resp = { write_receipt_json: JSON.stringify(golden.write_receipt) };
    const receipt = (0, consistency_1.receiptFromResponse)(resp);
    node_assert_1.strict.equal(receipt.source_lsn, golden.write_receipt.source_lsn);
    node_assert_1.strict.equal((0, consistency_1.receiptFromResponse)({}), null);
});
(0, node_test_1.test)("withReadFence omits empty fields and sets the x-udb-read-fence header", () => {
    const fence = (0, consistency_1.readFenceFromReceipt)({ source_lsn: "", outbox_seq: 0, projection_task_ids: [], manifest_checksum: "", written_at_unix_ms: 0 }, 1000);
    // empty source_lsn + empty task ids are omitted (skip_serializing_if mirror)
    const json = JSON.stringify(fence);
    node_assert_1.strict.ok(!json.includes("min_outbox_lsn"));
    node_assert_1.strict.ok(!json.includes("projection_task_ids"));
    node_assert_1.strict.ok(json.includes("max_wait_ms"));
    const opts = (0, consistency_1.withReadFence)((0, consistency_1.readFenceFromReceipt)(golden.write_receipt, 2500));
    node_assert_1.strict.ok(opts.headers && typeof opts.headers["x-udb-read-fence"] === "string");
});
(0, node_test_1.test)("afterWrite / withReadFenceFromReceipt = readFenceFromReceipt + withReadFence", () => {
    const receipt = golden.write_receipt;
    // Both helpers produce the same x-udb-read-fence header as the explicit compose.
    const explicit = (0, consistency_1.withReadFence)((0, consistency_1.readFenceFromReceipt)(receipt, 2500));
    const oneShot = (0, consistency_1.withReadFenceFromReceipt)(receipt, 2500);
    node_assert_1.strict.equal(oneShot.headers["x-udb-read-fence"], explicit.headers["x-udb-read-fence"]);
    // afterWrite is the naming-contract alias (default maxWaitMs); header is set.
    const aw = (0, consistency_1.afterWrite)(receipt);
    node_assert_1.strict.ok(typeof aw.headers["x-udb-read-fence"] === "string");
    // The grouped accessor used as `metadata.afterWrite(...)` in the spec.
    node_assert_1.strict.equal(consistency_1.consistencyMetadata.afterWrite, consistency_1.afterWrite);
    node_assert_1.strict.equal(consistency_1.consistencyMetadata.withReadFenceFromReceipt, consistency_1.withReadFenceFromReceipt);
});
// ── 09.7: stream send-one / await-first helpers ──────────────────────────────
function fakeStreamCore() {
    const writes = [];
    let ended = false;
    const core = Object.create(generatedClient_1.UdbCore.prototype);
    core.clientStream = () => ({
        stream: { write: (m) => writes.push(m), end: () => (ended = true) },
        response: Promise.resolve({ ok: true }),
    });
    return { core: core, writes, didEnd: () => ended };
}
(0, node_test_1.test)("sendOneClientStream writes exactly one message, ends, returns the response", async () => {
    const { core, writes, didEnd } = fakeStreamCore();
    const resp = await (0, stream_1.sendOneClientStream)(core, "svc", "M", { a: 1 });
    node_assert_1.strict.deepEqual(writes, [{ a: 1 }]);
    node_assert_1.strict.ok(didEnd());
    node_assert_1.strict.deepEqual(resp, { ok: true });
});
(0, node_test_1.test)("sendOneBidiAwaitFirst resolves on the first data; ignores later responses", async () => {
    const handlers = {};
    const duplex = {
        on: (ev, cb) => (handlers[ev] = cb),
        write: () => { },
    };
    const core = Object.create(generatedClient_1.UdbCore.prototype);
    core.bidiStream = () => duplex;
    const p = (0, stream_1.sendOneBidiAwaitFirst)(core, "svc", "M", { req: 1 });
    handlers["data"]({ first: true });
    handlers["data"]({ second: true }); // ignored
    node_assert_1.strict.deepEqual(await p, { first: true });
});
(0, node_test_1.test)("sendOneBidiAwaitFirst rejects when the stream ends before any data", async () => {
    const handlers = {};
    const duplex = { on: (ev, cb) => (handlers[ev] = cb), write: () => { } };
    const core = Object.create(generatedClient_1.UdbCore.prototype);
    core.bidiStream = () => duplex;
    const p = (0, stream_1.sendOneBidiAwaitFirst)(core, "svc", "M", {});
    handlers["end"]();
    await node_assert_1.strict.rejects(p, /ended before any response/);
});
