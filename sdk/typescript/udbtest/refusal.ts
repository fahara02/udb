// A broker refusal: a gRPC status with the typed ErrorDetail trailer, exactly as the
// real broker sends it, so a service's error handling sees what production sends.

import { Metadata, status as codes } from "@grpc/grpc-js";

import { protoSchemas } from "../protoSchema";

/** The typed detail a refusal carries, keyed by `udb.entity.v1.ErrorDetail` fields. */
export interface RefusalDetail {
  reason?: string;
  kind?: string;
  column?: string;
  constraint?: string;
  fix_hint?: string;
  retryable?: boolean;
  retry_after_ms?: number;
  operation?: string;
  missing?: Record<string, string>;
  field_violations?: { field: string; description: string }[];
}

export class Refusal extends Error {
  constructor(
    readonly code: codes,
    readonly reason: string,
    message: string,
    readonly detail?: RefusalDetail,
  ) {
    super(message);
  }
}

export function refuse(code: codes, reason: string, message: string, detail?: RefusalDetail): Refusal {
  return new Refusal(code, reason, message, detail);
}

export function errorKind(code: codes, reason: string): string {
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
    case codes.UNAVAILABLE:
      return "ERROR_KIND_RETRYABLE";
    case codes.INTERNAL:
      return "ERROR_KIND_INTERNAL";
    case codes.NOT_FOUND:
      return "ERROR_KIND_NOT_FOUND";
    case codes.INVALID_ARGUMENT:
      return "ERROR_KIND_VALIDATION";
    case codes.PERMISSION_DENIED:
      return "ERROR_KIND_POLICY";
  }
  return "ERROR_KIND_UNSPECIFIED";
}

/** The gRPC status object grpc-js sends for an error from a handler. */
export function statusOf(err: unknown): { code: codes; details: string; metadata: Metadata } {
  const metadata = new Metadata();
  if (!(err instanceof Refusal)) {
    return { code: codes.INTERNAL, details: err instanceof Error ? err.message : String(err), metadata };
  }
  if (err.reason || err.detail) {
    const detail = err.detail ?? { reason: err.reason, kind: errorKind(err.code, err.reason) };
    const type = protoSchemas().protoType("udb.entity.v1.ErrorDetail");
    metadata.set("udb-error-detail-bin", Buffer.from(type.encode(type.fromObject(detail)).finish()));
  }
  return { code: err.code, details: err.message, metadata };
}
