"use strict";
// gRPC server adapter. It lifts the canonical UDB request context (tenant, user,
// correlation, project, purpose, scopes) off an inbound call and makes it available
// to the handler, so downstream code and any UDB client built from it propagate the
// same identity without re-parsing headers. The header names are the ones the SDK
// sends, so a call that came from a UDB client round-trips its context faithfully.
//
//   const server = new grpc.Server({ interceptors: [udbServerInterceptor] });
//   // in a handler, unary or streaming:
//   const tenant = requestTenantId();
//
// Node has no request context object, so the value lives in AsyncLocalStorage and
// follows the handler across awaits.
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
exports.udbServerInterceptor = void 0;
exports.metadataFromGrpc = metadataFromGrpc;
exports.withRequestMetadata = withRequestMetadata;
exports.requestMetadata = requestMetadata;
exports.requestCorrelationId = requestCorrelationId;
exports.requestTenantId = requestTenantId;
exports.requestUserId = requestUserId;
const node_async_hooks_1 = require("node:async_hooks");
const grpc = __importStar(require("@grpc/grpc-js"));
const REQUEST_METADATA = new node_async_hooks_1.AsyncLocalStorage();
function emptyMetadata() {
    return { tenantId: "", purpose: "", correlationId: "" };
}
function splitScopes(raw) {
    const scopes = raw.split(",").filter(Boolean);
    return scopes.length ? scopes : undefined;
}
/** The canonical UDB identity on inbound gRPC metadata. The request id stands in
 *  for a missing correlation id. */
function metadataFromGrpc(md) {
    const first = (key) => {
        const values = md.get(key);
        return values.length ? String(values[0]) : "";
    };
    return {
        tenantId: first("x-tenant-id"),
        userId: first("x-user-id"),
        purpose: first("x-purpose"),
        correlationId: first("x-correlation-id") || first("x-request-id"),
        scopes: splitScopes(first("x-scopes")),
        serviceIdentity: first("x-service-identity"),
        projectId: first("x-udb-project-id"),
        clientCatalogVersion: first("x-udb-client-catalog-version"),
    };
}
/** Server interceptor for `new grpc.Server({ interceptors: [...] })`, covering unary
 *  and all streaming handlers. Handlers read the identity with `requestMetadata()`. */
const udbServerInterceptor = (_method, call) => {
    let meta = emptyMetadata();
    return new grpc.ServerInterceptingCall(call, {
        start(next) {
            const listener = new grpc.ServerListenerBuilder()
                .withOnReceiveMetadata((md, proceed) => {
                meta = metadataFromGrpc(md);
                REQUEST_METADATA.run(meta, () => proceed(md));
            })
                .withOnReceiveMessage((message, proceed) => {
                REQUEST_METADATA.run(meta, () => proceed(message));
            })
                .withOnReceiveHalfClose((proceed) => {
                REQUEST_METADATA.run(meta, () => proceed());
            })
                .build();
            next(listener);
        },
    });
};
exports.udbServerInterceptor = udbServerInterceptor;
/** Run `fn` with `meta` as the request identity. */
function withRequestMetadata(meta, fn) {
    return REQUEST_METADATA.run(meta, fn);
}
/** The request identity, or undefined outside a request. */
function requestMetadata() {
    return REQUEST_METADATA.getStore();
}
/** The request's correlation id, or "" when unset. */
function requestCorrelationId() {
    return requestMetadata()?.correlationId ?? "";
}
/** The request's tenant id, or "" when unset. */
function requestTenantId() {
    return requestMetadata()?.tenantId ?? "";
}
/** The request's user id, or "" when unset. */
function requestUserId() {
    return requestMetadata()?.userId ?? "";
}
