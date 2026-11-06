"use strict";
// A broker refusal: a gRPC status with the typed ErrorDetail trailer, exactly as the
// real broker sends it, so a service's error handling sees what production sends.
Object.defineProperty(exports, "__esModule", { value: true });
exports.Refusal = void 0;
exports.refuse = refuse;
exports.errorKind = errorKind;
exports.statusOf = statusOf;
const grpc_js_1 = require("@grpc/grpc-js");
const protoSchema_1 = require("../protoSchema");
class Refusal extends Error {
    code;
    reason;
    detail;
    constructor(code, reason, message, detail) {
        super(message);
        this.code = code;
        this.reason = reason;
        this.detail = detail;
    }
}
exports.Refusal = Refusal;
function refuse(code, reason, message, detail) {
    return new Refusal(code, reason, message, detail);
}
function errorKind(code, reason) {
    switch (reason) {
        case "UDB_CAS_CONFLICT":
        case "UDB_CAS_ROW_MISSING":
            return "ERROR_KIND_CONFLICT";
        case "UDB_CAS_KEY_NOT_PK":
        case "UDB_TABLE_NOT_TENANT_SCOPED":
        case "UDB_NULL_COMPARISON":
        case "UDB_UNSUPPORTED_FILTER_OPERATOR":
            return "ERROR_KIND_VALIDATION";
        case "UDB_UNIQUE_VIOLATION":
            return "ERROR_KIND_UNIQUE";
        case "UDB_NOT_NULL_VIOLATION":
            return "ERROR_KIND_NOT_NULL";
        case "UDB_SCOPE_MISSING":
        case "UDB_TENANT_MISMATCH":
        case "UDB_PROJECT_MISMATCH":
            return "ERROR_KIND_PERMISSION";
        case "UDB_POLICY_DENIED":
            return "ERROR_KIND_POLICY";
        case "UDB_UNKNOWN_MESSAGE_TYPE":
            return "ERROR_KIND_SCHEMA";
        case "UDB_RATE_LIMITED":
            return "ERROR_KIND_RATE_LIMITED";
        case "UDB_NO_ROWS_AFFECTED":
            return "ERROR_KIND_NOT_FOUND";
    }
    switch (code) {
        case grpc_js_1.status.UNAVAILABLE:
            return "ERROR_KIND_RETRYABLE";
        case grpc_js_1.status.INTERNAL:
            return "ERROR_KIND_INTERNAL";
        case grpc_js_1.status.NOT_FOUND:
            return "ERROR_KIND_NOT_FOUND";
        case grpc_js_1.status.INVALID_ARGUMENT:
            return "ERROR_KIND_VALIDATION";
        case grpc_js_1.status.PERMISSION_DENIED:
            return "ERROR_KIND_POLICY";
    }
    return "ERROR_KIND_UNSPECIFIED";
}
/** The gRPC status object grpc-js sends for an error from a handler. */
function statusOf(err) {
    const metadata = new grpc_js_1.Metadata();
    if (!(err instanceof Refusal)) {
        return { code: grpc_js_1.status.INTERNAL, details: err instanceof Error ? err.message : String(err), metadata };
    }
    if (err.reason || err.detail) {
        const detail = err.detail ?? { reason: err.reason, kind: errorKind(err.code, err.reason) };
        const type = (0, protoSchema_1.protoSchemas)().protoType("udb.entity.v1.ErrorDetail");
        metadata.set("udb-error-detail-bin", Buffer.from(type.encode(type.fromObject(detail)).finish()));
    }
    return { code: err.code, details: err.message, metadata };
}
