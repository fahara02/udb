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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_test_1 = require("node:test");
const strict_1 = __importDefault(require("node:assert/strict"));
const node_events_1 = require("node:events");
const grpc = __importStar(require("@grpc/grpc-js"));
const stores_1 = require("./stores");
const generatedClient_1 = require("./generatedClient");
const store_conformance_1 = require("./test-support/store-conformance");
// T2: the cross-SDK typed-store cases (shared with the Go runner).
(0, node_test_1.test)("Table meets the shared conformance cases", async (t) => (0, store_conformance_1.runStoreCases)(t, () => new stores_1.InMemoryDataPlane(store_conformance_1.storeCaseKeys)));
(0, node_test_1.test)("a key reused for changed input is not a retryable CAS conflict", () => {
    const cause = Object.assign(new Error("request refused"), {
        code: grpc.status.FAILED_PRECONDITION, details: "request refused", metadata: new grpc.Metadata(),
    });
    const err = new generatedClient_1.UdbError("DataBroker/Update", cause, {
        reason: "UDB_IDEMPOTENCY_REUSE", kind: stores_1.ErrorKind.CONFLICT,
    });
    strict_1.default.equal((0, stores_1.codeOf)(err), stores_1.ErrorCode.Precondition);
    const typedOnly = new generatedClient_1.UdbError("DataBroker/Update", cause, {
        reason: "UDB_FUTURE_CONSTRAINT", kind: stores_1.ErrorKind.UNIQUE,
    });
    strict_1.default.equal((0, stores_1.codeOf)(typedOnly), stores_1.ErrorCode.Unique);
});
(0, node_test_1.test)("a zero-affected scope guard response is not a successful typed upsert", async () => {
    for (const affected_rows of [0, "0", undefined]) {
        const plane = { unary: async () => ({ affected_rows }) };
        const table = new stores_1.Table(plane, "udb.core.storage.entity.v1.File", { key: ["file_id"] });
        await strict_1.default.rejects(table.upsert({ file_id: "foreign-row" }), stores_1.NotFoundError);
    }
});
(0, node_test_1.test)("inspect maps reason codes and the new detail fields", () => {
    const cause = Object.assign(new Error("dup"), { code: 6 });
    const err = new generatedClient_1.UdbError("DataBroker/Upsert", cause, {
        reason: "UDB_UNIQUE_VIOLATION",
        constraint: "users_email_key",
        column: "email",
        fix_hint: "use the existing row",
        missing: { scope: "udb:data:write" },
    });
    const info = (0, stores_1.inspect)(err);
    strict_1.default.equal(info.code, stores_1.ErrorCode.Unique);
    strict_1.default.equal(info.constraint, "users_email_key");
    strict_1.default.equal(info.column, "email");
    strict_1.default.deepEqual(info.missing, { scope: "udb:data:write" });
    strict_1.default.equal((0, stores_1.codeOf)(new stores_1.NotFoundError("x")), stores_1.ErrorCode.NotFound);
    strict_1.default.equal((0, stores_1.codeOf)(new stores_1.ConflictError("x")), stores_1.ErrorCode.Conflict);
    strict_1.default.equal((0, stores_1.codeOf)(new Error("plain")), stores_1.ErrorCode.Unknown);
});
(0, node_test_1.test)("tx scope emits a versioned envelope with the writes", () => {
    const scope = new stores_1.TxScope("corr-1");
    scope.upsert("acme.v1.Order", { id: "o1" });
    scope.emit({ topic: "acme.order.placed.v1", eventType: "acme.OrderPlaced", documentId: "o1", payload: { id: "o1" } });
    strict_1.default.equal(scope.mutations.length, 2);
    const event = scope.mutations[1];
    strict_1.default.equal(event.operation, "enqueue_outbox_event");
    strict_1.default.equal(event.collection, "acme.order.placed.v1");
    strict_1.default.equal(event.payload.envelope_version, 2);
    strict_1.default.equal(event.payload.correlation_id, "corr-1");
});
(0, node_test_1.test)("decodeEnvelope unwraps a nested journal envelope and refuses newer versions", () => {
    const nested = JSON.stringify({ payload: { event_type: "x", payload: { a: 1 }, envelope_version: 2 } });
    strict_1.default.deepEqual((0, stores_1.decodeEnvelope)(nested).payload, { a: 1 });
    strict_1.default.throws(() => (0, stores_1.decodeEnvelope)(JSON.stringify({ event_type: "x", payload: {}, envelope_version: 3 })));
});
(0, node_test_1.test)("tx requires a commit acknowledgement and preserves the typed failure trailer", async () => {
    const conflict = new generatedClient_1.UdbError("DataBroker/BeginTx", Object.assign(new Error("CAS failed"), { code: 10 }), { reason: "UDB_CAS_CONFLICT" });
    const run = (statuses, error) => (0, stores_1.tx)({
        unary: async () => ({}),
        bidiStream: () => {
            const stream = Object.assign(new node_events_1.EventEmitter(), {
                write: () => { },
                end: () => queueMicrotask(() => {
                    for (const status of statuses)
                        stream.emit("data", status);
                    if (error)
                        stream.emit("error", error);
                    stream.emit("end");
                }),
            });
            return stream;
        },
    }, () => { });
    await strict_1.default.rejects(run([{ state: "TX_STATE_OPEN" }]), /without a commit acknowledgement/);
    await strict_1.default.rejects(run([{ state: "TX_STATE_ERROR", message: "CAS failed" }], { udb: conflict }), (err) => err === conflict && (0, stores_1.codeOf)(err) === stores_1.ErrorCode.Conflict);
    await strict_1.default.rejects(run([{ state: "TX_STATE_ERROR", message: "opaque operator text", code: 9,
            error_detail: { reason: "UDB_CAS_CONFLICT" } }]), (err) => (0, stores_1.codeOf)(err) === stores_1.ErrorCode.Conflict);
    await run([{ state: "TX_STATE_COMMITTED" }]);
});
(0, node_test_1.test)("a failed consumer handler cannot acknowledge a queued later event", async () => {
    const controller = new AbortController();
    const handled = [];
    const acked = [];
    await (0, stores_1.consume)({
        unary: async (_service, _method, request) => { acked.push(request.event_id); return {}; },
        serverStream: () => {
            const stream = Object.assign(new node_events_1.EventEmitter(), { cancel: () => stream.emit("error", { code: 1 }) });
            queueMicrotask(() => {
                for (const event_id of ["head", "tail"])
                    stream.emit("data", { event_id, payload_json: JSON.stringify({ envelope_version: 2, payload: {} }) });
                stream.emit("end");
            });
            return stream;
        },
    }, "consumer", "topic", async (event) => {
        handled.push(event.id);
        throw new Error("head failed");
    }, { signal: controller.signal, onError: () => controller.abort() });
    strict_1.default.deepEqual(handled, ["head"]);
    strict_1.default.deepEqual(acked, []);
});
(0, node_test_1.test)("consumer stops on an unsupported envelope version without acknowledging it", async () => {
    let acked = false;
    await strict_1.default.rejects((0, stores_1.consume)({
        unary: async () => { acked = true; return {}; },
        serverStream: () => {
            const stream = Object.assign(new node_events_1.EventEmitter(), { cancel: () => stream.emit("error", { code: 1 }) });
            queueMicrotask(() => stream.emit("data", { event_id: "new", payload_json: JSON.stringify({ envelope_version: 3, payload: {} }) }));
            return stream;
        },
    }, "consumer", "topic", async () => strict_1.default.fail("unknown envelope cannot be delivered")), /upgrade the SDK/);
    strict_1.default.equal(acked, false);
});
