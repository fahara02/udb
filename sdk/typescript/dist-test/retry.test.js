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
