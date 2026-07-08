// Hand-written snake_case request/response interfaces for the common-RPC subset
// the facades use (§8B). These match the WIRE JSON the `@grpc/proto-loader`
// runtime sends with `keepCase: true` (plain snake_case objects) — NOT the
// camelCase `@bufbuild/protobuf` `gen/**` Message classes (which are excluded
// from the build and must not be imported at runtime).
//
// Every interface carries `[k: string]: unknown` so the dynamic escape hatch
// (passing extra/raw fields) is preserved; a MISSPELLED known field is still a
// tsc error when a caller annotates a request with one of these types.
//
// This is a DELIBERATE subset (Select/Upsert/Delete + the storage/auth/migration
// helpers the facades use). The full descriptor-driven per-message interface
// generator is deferred (D10).

/** bytes on the wire → Buffer (keepCase loader) or Uint8Array. */
export type Bytes = Buffer | Uint8Array;

/** google.protobuf.Struct → plain JS object (wkt.ts converts to/from Struct). */
export type StructObject = Record<string, unknown>;

/** A request/response context (tenant/project/idempotency/fence/…). Loose by
 *  design — the broker is the authority for identity. */
export interface RequestContext {
  tenant_id?: string;
  project_id?: string;
  purpose?: string;
  correlation_id?: string;
  read_fence_json?: string;
  /** Requested read consistency (context.proto field 18). A pinned wire token
   *  ("strong" / "read_your_writes" / "bounded_staleness" / "replica_bounded" /
   *  "eventual" / "projection_ok" / "cache_ok"); empty uses the runtime default.
   *  Mirrors the `x-udb-consistency` header. Set it ergonomically with
   *  `consistencyContext()` (body) or `withConsistency()` (header) from
   *  ./consistency rather than hand-typing the token. */
  consistency?: string;
  /** Typed consistency mode (context.proto field 22, ConsistencyMode enum). Under
   *  the keepCase / `enums: String` proto-loader this is the enum NAME string
   *  (e.g. "CONSISTENCY_MODE_STRONG"). The `x-udb-consistency` header still wins
   *  server-side when both are supplied. */
  consistency_mode?: string;
  [k: string]: unknown;
}

// ── DataBroker: Select / Upsert / Delete ─────────────────────────────────────

export interface SelectRequest {
  context?: RequestContext;
  message_type?: string;
  filter?: StructObject;
  limit?: number;
  offset?: number;
  order_by?: string[];
  [k: string]: unknown;
}

export interface SelectResponse {
  records_json?: Bytes[];
  total?: number;
  stale_read_warning?: StructObject;
  [k: string]: unknown;
}

export interface UpsertRequest {
  context?: RequestContext;
  message_type?: string;
  record_json?: Bytes;
  conflict_fields?: string[];
  return_record?: boolean;
  idempotency_key?: string;
  [k: string]: unknown;
}

export interface MutationResponse {
  record_json?: Bytes;
  rows_affected?: number;
  was_duplicate?: boolean;
  write_receipt_json?: string;
  [k: string]: unknown;
}

export interface DeleteRequest {
  context?: RequestContext;
  message_type?: string;
  filter?: StructObject;
  idempotency_key?: string;
  [k: string]: unknown;
}

// ── StorageService: upload lifecycle ─────────────────────────────────────────

export interface RegisterUploadRequest {
  context?: RequestContext;
  file_name?: string;
  content_type?: string;
  file_type?: string;
  size_bytes?: number;
  [k: string]: unknown;
}

export interface RegisterUploadResponse {
  file_id?: string;
  upload_url?: string;
  expires_at?: string;
  error?: StructObject;
  [k: string]: unknown;
}

export interface FinalizeUploadRequest {
  context?: RequestContext;
  file_id?: string;
  size_bytes?: number;
  checksum?: string;
  etag?: string;
  [k: string]: unknown;
}

export interface FinalizeUploadResponse {
  file_id?: string;
  error?: StructObject;
  [k: string]: unknown;
}

// ── AuthnService: login / authenticate ───────────────────────────────────────

export interface LoginRequest {
  username?: string;
  password?: string;
  tenant_hint?: string;
  project_hint?: string;
  [k: string]: unknown;
}

export interface LoginResponse {
  access_token?: string;
  refresh_token?: string;
  session_token?: string;
  session_id?: string;
  access_token_expires_in?: number;
  mfa_required?: boolean;
  principal?: StructObject;
  [k: string]: unknown;
}

export interface AuthenticateRequest {
  bearer_token?: string;
  api_key?: string;
  session_id?: string;
  tenant_hint?: string;
  project_hint?: string;
  requested_scopes?: string[];
  [k: string]: unknown;
}

export interface AuthenticateResponse {
  principal?: {
    user_id?: string;
    tenant_id?: string;
    project_id?: string;
    scopes?: string[];
    [k: string]: unknown;
  };
  [k: string]: unknown;
}

// ── Admin: migration lifecycle ───────────────────────────────────────────────

export interface MigrationStatusResponse {
  plan_id?: string;
  approval_token?: string;
  applyable?: boolean;
  [k: string]: unknown;
}
