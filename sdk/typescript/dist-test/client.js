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
exports.UDB_DEFAULT_CHANNEL_OPTIONS = exports.UDB_PROTOCOL_VERSION = void 0;
exports.metadata = metadata;
exports.dataBrokerClient = dataBrokerClient;
const grpc = __importStar(require("@grpc/grpc-js"));
const protoLoader = __importStar(require("@grpc/proto-loader"));
const path_1 = __importDefault(require("path"));
const node_crypto_1 = require("node:crypto");
require("./wkt"); // registers the google.protobuf.Struct serializer (must precede any loadSync)
const protoRoot_1 = require("./protoRoot");
exports.UDB_PROTOCOL_VERSION = "1.0.0";
function metadata(meta) {
    const headers = new grpc.Metadata();
    // §13: native RPCs require a request context (x-request-id / x-correlation-id /
    // traceparent) and fail closed without one. Always send a request id, and fall
    // back to it for the correlation id when the caller set none.
    const requestId = (0, node_crypto_1.randomUUID)();
    const correlationId = meta.correlationId || requestId;
    headers.set("x-tenant-id", meta.tenantId);
    headers.set("x-user-id", meta.userId ?? "");
    headers.set("x-purpose", meta.purpose);
    headers.set("x-correlation-id", correlationId);
    headers.set("x-request-id", requestId);
    headers.set("x-scopes", (meta.scopes ?? []).join(","));
    headers.set("x-service-identity", meta.serviceIdentity ?? "example.service");
    headers.set("x-udb-project-id", meta.projectId ?? "default");
    headers.set("x-udb-client-catalog-version", meta.clientCatalogVersion ?? exports.UDB_PROTOCOL_VERSION);
    if (meta.bearerToken)
        headers.set("authorization", `Bearer ${meta.bearerToken}`);
    if (meta.apiKey)
        headers.set("x-api-key", meta.apiKey);
    return headers;
}
// Default channel options for the long-lived UDB channel: keepalive keeps an idle
// HTTP/2 connection warm instead of dropping to IDLE and re-handshaking. Retries
// are handled by the generated wrapper where proto-derived operation_kind is known.
exports.UDB_DEFAULT_CHANNEL_OPTIONS = {
    "grpc.keepalive_time_ms": 30_000,
    "grpc.keepalive_timeout_ms": 10_000,
    "grpc.keepalive_permit_without_calls": 1,
    "grpc.http2.max_pings_without_data": 0,
};
function dataBrokerClient(target, protoRoot = (0, protoRoot_1.defaultProtoRoot)()) {
    const protoPath = path_1.default.join(protoRoot, "udb/services/v1/data_broker.proto");
    const definition = protoLoader.loadSync(protoPath, {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    });
    const loaded = grpc.loadPackageDefinition(definition);
    return new loaded.udb.services.v1.DataBroker(target, grpc.credentials.createInsecure(), exports.UDB_DEFAULT_CHANNEL_OPTIONS);
}
