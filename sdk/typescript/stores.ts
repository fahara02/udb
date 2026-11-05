// Typed stores, transactions, durable consumers and error codes — the
// TypeScript port of the Go SDK's Table[T], Tx, Consume and ErrorCode (T1).
//
// Everything here goes through a `StorePlane` (anything with the generated
// `UdbCore.unary`/`serverStream`/`bidiStream` shape), so tests can swap in
// `InMemoryDataPlane` instead of a broker.

import { createHash, randomUUID } from "node:crypto";
import * as grpc from "@grpc/grpc-js";
import { SessionFence, receiptFromProto } from "./consistency";
import { UdbConflictError, UdbNotFoundError, parseRecordJson } from "./entity";
import { CallOptions, UdbError } from "./generatedClient";
import { MessageSchema, ProtoSchemas, protoSchemas } from "./protoSchema";
import {
  RawJson,
  WriteMode,
  decodeEventPayload,
  decodeRecord,
  encodeEvent,
  encodeRecord,
  formatTimestamp,
  recordToJson,
} from "./recordCodec";

const DATABROKER = "udb.services.v1.DataBroker";

/** The transport a store needs: the generated `UdbCore` satisfies it. */
export interface StorePlane {
  unary(service: string, method: string, request: any, call?: CallOptions): Promise<any>;
  serverStream?(service: string, method: string, request: any, call?: CallOptions): any;
  bidiStream?(service: string, method: string, call?: CallOptions): any;
  /** The client's settings, when the plane is a real client. */
  opts?: { protoRoot?: string; meta?: { correlationId?: string } };
}

// ── Error codes ─────────────────────────────────────────────────────────────

/** A closed set of outcomes to branch on, instead of matching messages. */
export enum ErrorCode {
  Unknown = "unknown",
  NotFound = "not_found",
  Conflict = "conflict",
  Unique = "unique",
  NotNull = "not_null",
  ForeignKey = "foreign_key",
  Invalid = "invalid",
  PolicyDenied = "policy_denied",
  ScopeMissing = "scope_missing",
  Unauthenticated = "unauthenticated",
  RateLimited = "rate_limited",
  Unavailable = "unavailable",
  Redacted = "redacted",
  Precondition = "precondition",
  Internal = "internal",
}

const REASON_CODES: Record<string, ErrorCode> = {
  UDB_CAS_CONFLICT: ErrorCode.Conflict,
  UDB_CAS_ROW_MISSING: ErrorCode.Conflict,
  UDB_REVISION_CONFLICT: ErrorCode.Conflict,
  UDB_GRANT_OWNED_BY_OTHER: ErrorCode.Conflict,
  UDB_CAS_KEY_NOT_PK: ErrorCode.Precondition,
  UDB_IDEMPOTENCY_REUSE: ErrorCode.Precondition,
  UDB_NO_ROWS_AFFECTED: ErrorCode.NotFound,
  UDB_UNIQUE_VIOLATION: ErrorCode.Unique,
  UDB_NOT_NULL_VIOLATION: ErrorCode.NotNull,
  UDB_UPSERT_INCOMPLETE_ROW: ErrorCode.NotNull,
  UDB_FOREIGN_KEY_VIOLATION: ErrorCode.ForeignKey,
  UDB_TYPE_MISMATCH: ErrorCode.Invalid,
  UDB_UNSUPPORTED_FILTER_OPERATOR: ErrorCode.Invalid,
  UDB_NULL_COMPARISON: ErrorCode.Invalid,
  UDB_TENANT_MISMATCH: ErrorCode.PolicyDenied,
  UDB_TABLE_NOT_TENANT_SCOPED: ErrorCode.Invalid,
  UDB_SCOPE_MISSING: ErrorCode.ScopeMissing,
  UDB_POLICY_DENIED: ErrorCode.PolicyDenied,
  UDB_REDACTED_VALUE_WRITE: ErrorCode.Redacted,
  UDB_RATE_LIMITED: ErrorCode.RateLimited,
  UDB_DECODE_FAILED: ErrorCode.Internal,
  UDB_UNKNOWN_MESSAGE_TYPE: ErrorCode.Invalid,
  UDB_ENVELOPE_VERSION_UNSUPPORTED: ErrorCode.Precondition,
};

/** Wire values of `udb.entity.v1.ErrorKind` (proto/udb/entity/v1/error.proto). */
export const ErrorKind = {
  UNSPECIFIED: 0,
  CAPABILITY: 1,
  POLICY: 2,
  QUOTA: 3,
  SCHEMA: 4,
  RETRYABLE: 5,
  INTERNAL: 6,
  VALIDATION: 7,
  CONFLICT: 8,
  NOT_FOUND: 9,
  UNIQUE: 10,
  NOT_NULL: 11,
  FOREIGN_KEY: 12,
  PERMISSION: 13,
  REDACTED: 14,
  RATE_LIMITED: 15,
} as const;
export type ErrorKind = (typeof ErrorKind)[keyof typeof ErrorKind];

const KIND_CODES: Partial<Record<ErrorKind, ErrorCode>> = {
  [ErrorKind.CONFLICT]: ErrorCode.Conflict,
  [ErrorKind.NOT_FOUND]: ErrorCode.NotFound,
  [ErrorKind.UNIQUE]: ErrorCode.Unique,
  [ErrorKind.NOT_NULL]: ErrorCode.NotNull,
  [ErrorKind.FOREIGN_KEY]: ErrorCode.ForeignKey,
  [ErrorKind.PERMISSION]: ErrorCode.PolicyDenied,
  [ErrorKind.REDACTED]: ErrorCode.Redacted,
  [ErrorKind.RATE_LIMITED]: ErrorCode.RateLimited,
  [ErrorKind.QUOTA]: ErrorCode.RateLimited,
  [ErrorKind.RETRYABLE]: ErrorCode.Unavailable,
  [ErrorKind.VALIDATION]: ErrorCode.Invalid,
  [ErrorKind.INTERNAL]: ErrorCode.Internal,
};

const GRPC_CODES: Record<number, ErrorCode> = {
  5: ErrorCode.NotFound,
  6: ErrorCode.Unique,
  9: ErrorCode.Precondition,
  7: ErrorCode.PolicyDenied,
  8: ErrorCode.RateLimited,
  14: ErrorCode.Unavailable,
  4: ErrorCode.Unavailable,
  3: ErrorCode.Invalid,
  16: ErrorCode.Unauthenticated,
  13: ErrorCode.Internal,
};

/** Everything the broker said about a failure. */
export interface ErrorInfo {
  code: ErrorCode;
  reason?: string;
  constraint?: string;
  column?: string;
  fixHint?: string;
  /** What the caller lacks (scope, rule, purpose, ...). */
  missing?: Record<string, string>;
  retryAfterMs?: number;
  /** The underlying gRPC status code; undefined for an error that is not a call failure. */
  grpc?: number;
}

/** The row a keyed read or conditional write named does not exist. */
export class NotFoundError extends Error {
  readonly code = ErrorCode.NotFound;
}

/** A compare-and-swap precondition did not hold. */
export class ConflictError extends Error {
  readonly code = ErrorCode.Conflict;
}

/** Find the broker failure inside an error: a store's error keeps it as its cause. */
function underlyingFailure(err: unknown): UdbError | undefined {
  let current: any = err;
  for (let depth = 0; current && depth < 8; depth += 1) {
    if (current instanceof UdbError) return current;
    if (current.udb instanceof UdbError) return current.udb;
    current = current.cause;
  }
  return undefined;
}

/** A trailer gives the kind as its number; a transaction frame as its enum name. */
function kindNumber(kind: unknown): number | undefined {
  if (typeof kind === "number") return kind;
  if (typeof kind === "string") {
    const name = kind.replace(/^ERROR_KIND_/, "") as keyof typeof ErrorKind;
    return ErrorKind[name];
  }
  return undefined;
}

/** Classify any error from a store, the generated client or a raw gRPC call: the
 *  broker's stable reason decides when present, then the typed kind, then the gRPC
 *  code. An error that is not a call failure has code `unknown`. */
export function inspect(err: unknown): ErrorInfo {
  const udb = underlyingFailure(err);
  const sentinel =
    err instanceof ConflictError || err instanceof UdbConflictError
      ? ErrorCode.Conflict
      : err instanceof NotFoundError || err instanceof UdbNotFoundError
        ? ErrorCode.NotFound
        : undefined;
  if (!udb) {
    const code = (err as { code?: unknown } | null | undefined)?.code;
    const status = typeof code === "number" ? code : undefined;
    return { code: sentinel ?? (status !== undefined ? GRPC_CODES[status] : undefined) ?? ErrorCode.Unknown, grpc: status };
  }
  const detail = udb.detail ?? {};
  const reason = typeof detail.reason === "string" && detail.reason.startsWith("UDB_") ? detail.reason : undefined;
  const kind = kindNumber(detail.kind);
  // A validation kind says little on its own: the gRPC code decides, so a
  // FAILED_PRECONDITION stays a precondition rather than an invalid request.
  const byKind = kind !== undefined && kind !== ErrorKind.VALIDATION ? KIND_CODES[kind as ErrorKind] : undefined;
  const retryAfter = Number(detail.retry_after_ms ?? 0);
  return {
    code: (reason && REASON_CODES[reason]) || byKind || sentinel || GRPC_CODES[udb.code] || ErrorCode.Unknown,
    reason,
    constraint: detail.constraint as string | undefined,
    column: detail.column as string | undefined,
    fixHint: detail.fix_hint as string | undefined,
    missing: detail.missing as Record<string, string> | undefined,
    retryAfterMs: retryAfter > 0 ? retryAfter : undefined,
    grpc: udb.code,
  };
}

export const codeOf = (err: unknown): ErrorCode => inspect(err).code;
export const isNotFound = (err: unknown): boolean => codeOf(err) === ErrorCode.NotFound;
export const isConflict = (err: unknown): boolean => codeOf(err) === ErrorCode.Conflict;
export const isUnique = (err: unknown, constraint?: string): boolean => {
  const info = inspect(err);
  return info.code === ErrorCode.Unique && (!constraint || info.constraint === constraint);
};

/** The scope the broker said the caller lacks ("" when the failure is not a missing scope). */
export function missingScope(err: unknown): string {
  const info = inspect(err);
  return info.code === ErrorCode.ScopeMissing ? info.missing?.scope ?? "" : "";
}

/** Whether `err` is a compare-and-swap precondition failure: the target row was
 *  absent or a field no longer matched. Check it after a write with `expected` to
 *  decide whether to re-read and retry. Other FAILED_PRECONDITION refusals, such as
 *  a conditional write not keyed by a primary key, are usage errors a retry cannot
 *  fix, and are not conflicts. The message text is never matched. */
export function isCasConflict(err: unknown): boolean {
  const udb = underlyingFailure(err);
  if (!udb || udb.code !== grpc.status.FAILED_PRECONDITION) return false;
  const reason = udb.detail?.reason;
  return reason === "UDB_CAS_CONFLICT" || reason === "UDB_CAS_ROW_MISSING" || reason === "UDB_REVISION_CONFLICT";
}

// ── Table ───────────────────────────────────────────────────────────────────

/** The columns of one row, by field name: every primary-key column, or every
 *  column of one declared unique key. */
export type RowKey = Record<string, unknown>;

export interface TablePage<T> {
  rows: T[];
  /** True when the page is full: more rows may match. */
  hasMore: boolean;
  /** Continues the walk (empty on the last page, or when no limit was set). */
  next: string;
  /** Every matching row, when `includeTotal` was set. */
  total?: number;
  /** Columns that came back as the redaction placeholder; never write those back. */
  redacted: string[];
}

/** Options of {@link Table.select}. */
export interface TableSelectOptions {
  /** The page size (0 = the broker default of 100). */
  limit?: number;
  /** Continues a previous page ({@link TablePage.next}). */
  pageToken?: string;
  /** Sort by these columns (`-column` for descending). */
  orderBy?: readonly string[];
  /** Also count every matching row into `total`. */
  includeTotal?: boolean;
  /** Read only these columns (empty = every non-PII column). */
  fields?: readonly string[];
  primaryRead?: boolean;
}

/** Options of {@link Table}. */
export interface TableOptions {
  /** The key fields; read from the message's annotations when omitted. */
  key?: string[];
  /** The client's read-your-writes state, shared by its stores. */
  fence?: SessionFence;
  /** The proto tree declaring the message; the bundled one by default. */
  schemas?: ProtoSchemas;
}

function jsonBytes(value: unknown): Buffer {
  return Buffer.from(recordToJson(value), "utf8");
}

/** Make a value a google.protobuf.Struct can carry without losing it: an integer
 *  beyond 2^53 travels as its decimal string, which the broker reads exactly. */
function structSafe(value: unknown): unknown {
  if (typeof value === "bigint") return value.toString();
  if (value instanceof RawJson) return value.toJSON();
  if (value instanceof Date) return formatTimestamp(value);
  if (Array.isArray(value)) return value.map(structSafe);
  if (value && typeof value === "object" && !Buffer.isBuffer(value) && !(value instanceof Uint8Array)) {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, structSafe(item)]));
  }
  return value;
}

function structMap(value: Record<string, unknown>): Record<string, unknown> {
  return structSafe(value) as Record<string, unknown>;
}

function decodeRows<T>(response: any): T[] {
  const raw = response?.records_json;
  const items: unknown[] = Array.isArray(raw) ? raw : raw ? [raw] : [];
  const rows: T[] = [];
  for (const item of items) {
    const text = Buffer.isBuffer(item) || item instanceof Uint8Array ? Buffer.from(item).toString("utf8") : typeof item === "string" ? item : "";
    if (!text) continue;
    const parsed = parseRecordJson(text);
    if (Array.isArray(parsed)) rows.push(...(parsed as T[]));
    else rows.push(parsed as T);
  }
  return rows;
}

/** Find the proto declaration of a message; undefined when the tree has none, as
 *  for a transport-only unit test over an undeclared type. */
function schemaOf(schemas: ProtoSchemas, messageType: string): MessageSchema | undefined {
  try {
    return schemas.find(messageType);
  } catch {
    return undefined;
  }
}

function schemasOf(plane: StorePlane, given?: ProtoSchemas): ProtoSchemas {
  return given ?? protoSchemas(plane.opts?.protoRoot);
}

/** The whole row a write stores, through the column rules when the message is
 *  declared. An unknown property is refused rather than dropped. */
function writeRecord(schema: MessageSchema | undefined, schemas: ProtoSchemas, messageType: string, row: Record<string, unknown>): unknown {
  if (!schema) return row;
  for (const key of Object.keys(row)) {
    if (!schema.field(key)) throw new Error(`udb: ${messageType} has no field "${key}"`);
  }
  return encodeRecord(messageType, decodeRecord(messageType, row, schemas), WriteMode.Insert, schemas);
}

/** Shapes one write of a {@link Table}. */
export interface TableWriteOptions {
  /** Makes the write safe to retry after a lost response: the broker replays the first
   *  result for the key. Reusing a key with different inputs is refused. */
  idempotencyKey?: string;
}

const EMPTY_GUARD = "udb: guarded write requires a non-empty expected precondition";

/** A typed store over one entity. Writes go through the broker's data plane, so
 *  policies, tenancy, projections and events apply as for any write. After a
 *  write, the next reads through the same fence carry that write's read fence. */
export class Table<T extends Record<string, unknown> = Record<string, unknown>> {
  private readonly fence?: SessionFence;
  private readonly schemas: ProtoSchemas;
  private readonly explicitKey?: string[];

  constructor(
    private readonly plane: StorePlane,
    readonly messageType: string,
    opts: TableOptions = {},
  ) {
    if (opts.key && !opts.key.length) throw new Error(`udb: Table(${messageType}) needs its key fields`);
    this.explicitKey = opts.key;
    this.fence = opts.fence;
    this.schemas = schemasOf(plane, opts.schemas);
  }

  private get schema(): MessageSchema | undefined {
    return schemaOf(this.schemas, this.messageType);
  }

  private get key(): string[] {
    return this.explicitKey ?? this.schema?.primaryKeys ?? [];
  }

  /** The filter that addresses one row: the key fields of `key`, or `key` itself
   *  when it names another unique key. */
  keyFilter(key: RowKey): Record<string, unknown> {
    const fields = this.key;
    const filter: Record<string, unknown> = {};
    if (fields.length && fields.every((field) => field in key)) {
      for (const field of fields) filter[field] = key[field];
      return structMap(filter);
    }
    if (this.explicitKey) {
      const missing = fields.find((field) => !(field in key));
      throw new Error(`udb: ${this.messageType} key is missing ${missing}`);
    }
    if (Object.keys(key).length === 0) throw new Error(`udb: ${this.messageType} key is empty`);
    return structMap(key);
  }

  /** The primary key of `row`. Throws when a primary-key column has no value. */
  keyOf(row: Partial<T>): RowKey {
    const fields = this.key;
    if (fields.length === 0) throw new Error(`udb: ${this.messageType} declares no primary key`);
    const key: RowKey = {};
    for (const field of fields) {
      const value = (row as Record<string, unknown>)[field];
      if (value === undefined || value === null || value === "") {
        throw new Error(`udb: ${this.messageType} has no value for primary-key column ${field}`);
      }
      key[field] = value;
    }
    return key;
  }

  /** The row with `key`. A missing row throws {@link NotFoundError}. */
  async get(key: RowKey): Promise<T> {
    const page = await this.select(this.keyFilter(key), { limit: 2 });
    if (page.rows.length === 0) throw new NotFoundError(`udb: ${this.messageType} ${JSON.stringify(key)} not found`);
    if (page.rows.length > 1) {
      throw new Error(`udb: ${this.messageType} key ${JSON.stringify(key)} matched more than one row; address a primary or unique key`);
    }
    return page.rows[0];
  }

  /** One page of rows matching `filter`. */
  async select(filter: Record<string, unknown> = {}, opts: TableSelectOptions = {}): Promise<TablePage<T>> {
    const request: Record<string, unknown> = {
      message_type: this.messageType,
      filter: structMap(filter),
      limit: opts.limit ?? 0,
      include_total: opts.includeTotal ?? false,
    };
    if (opts.pageToken) request.page_token = opts.pageToken;
    if (opts.fields?.length) request.fields = [...opts.fields];
    if (opts.orderBy?.length) {
      request.sort = opts.orderBy.map((column) => ({
        field: column.startsWith("-") ? column.slice(1) : column,
        descending: column.startsWith("-"),
      }));
    }
    const context = { ...(this.fence?.readContext() ?? {}), ...(opts.primaryRead ? { primary_read: true } : {}) };
    if (Object.keys(context).length > 0) request.context = context;
    const response = await this.plane.unary(DATABROKER, "Select", request).catch((err) => {
      throw classify(err);
    });
    return {
      rows: decodeRows<T>(response),
      hasMore: Boolean(response?.has_more),
      next: String(response?.next_page_token ?? ""),
      total: opts.includeTotal ? Number(response?.exact_total ?? 0) : undefined,
      redacted: Array.isArray(response?.redacted_fields) ? response.redacted_fields : [],
    };
  }

  /** Walk every row matching `filter`, page by page. A throw from `fn` stops the
   *  walk and propagates. */
  async selectAll(filter: Record<string, unknown>, fn: (row: T) => void | Promise<void>): Promise<void> {
    let pageToken: string | undefined;
    for (;;) {
      const page = await this.select(filter, { limit: 500, pageToken });
      for (const row of page.rows) await fn(row);
      if (!page.next || !page.hasMore) return;
      pageToken = page.next;
    }
  }

  async count(filter: Record<string, unknown> = {}): Promise<number> {
    return (await this.select(filter, { limit: 1, includeTotal: true })).total ?? 0;
  }

  /** Write the whole row: inserted when its key is new, replaced when it exists.
   *  To change some columns use {@link patch} or {@link updateIf}. NotFoundError
   *  means the broker's scope guard left a conflicting row untouched. */
  async upsert(row: T, opts: TableWriteOptions = {}): Promise<void> {
    await this.write("Upsert", row, opts);
  }

  /** Create the row. A primary or unique-key collision throws with code `unique`
   *  and never replaces the existing row. */
  async insert(row: T, opts: TableWriteOptions = {}): Promise<void> {
    await this.write("Insert", row, opts);
  }

  private async write(rpc: "Upsert" | "Insert", row: T, opts: TableWriteOptions): Promise<void> {
    const request: Record<string, unknown> = {
      message_type: this.messageType,
      record_json: jsonBytes(writeRecord(this.schema, this.schemas, this.messageType, row)),
    };
    if (rpc === "Upsert" && this.explicitKey) request.conflict_fields = this.explicitKey;
    if (opts.idempotencyKey) request.idempotency_key = opts.idempotencyKey;
    const response = await this.plane.unary(DATABROKER, rpc, request).catch((err) => {
      throw classify(err);
    });
    if (String(response?.affected_rows ?? "0") === "0" && !response?.was_duplicate) {
      throw new NotFoundError(`${this.messageType}: write target not found in the verified scope`);
    }
    this.fence?.rememberWrite(response);
  }

  /** Set some columns of an existing row; NotFoundError when it does not exist. */
  async patch(key: RowKey, changes: Partial<T>, opts: TableWriteOptions = {}): Promise<void> {
    await this.update(key, changes, undefined, undefined, opts);
  }

  /** Set columns only while the row still holds `expected`; ConflictError when
   *  it changed or is gone. */
  async updateIf(key: RowKey, expected: Partial<T>, changes: Partial<T>, opts: TableWriteOptions = {}): Promise<void> {
    if (Object.keys(expected).length === 0) throw new Error(EMPTY_GUARD);
    await this.update(key, changes, expected, undefined, opts);
  }

  /** Add `deltas` to numeric columns of the row with `key`, atomically on the
   *  broker, with no read-modify-write window. */
  async increment(key: RowKey, deltas: Readonly<Record<string, number>>, opts: TableWriteOptions = {}): Promise<void> {
    await this.update(key, undefined, undefined, deltas, opts);
  }

  /** Read the row, apply `change`, and write back only the columns it modified,
   *  guarded by their previous values. A conflict reads and tries again, up to
   *  `attempts` times. Returns the row as written. */
  async updateWithRetry(key: RowKey, change: (current: T) => T | Promise<T>, attempts = 5): Promise<T> {
    const limit = attempts > 0 ? attempts : 5;
    for (let attempt = 0; attempt < limit; attempt += 1) {
      const current = await this.get(key);
      const before = this.updateColumns(current);
      const next = await change(structuredClone(current));
      const after = this.updateColumns(next);
      const changes: Record<string, unknown> = {};
      const expected: Record<string, unknown> = {};
      for (const [column, value] of Object.entries(after)) {
        if (recordToJson(before[column]) !== recordToJson(value)) {
          changes[column] = value;
          // What the row holds, not how this SDK re-encodes it: NULL, "" and a REAL's digits
          // do not survive that. A column the read did not return cannot be guarded.
          const stored = current as Record<string, unknown>;
          if (column in stored) expected[column] = stored[column];
        }
      }
      if (Object.keys(changes).length === 0) return current;
      try {
        await this.update(key, changes as Partial<T>, expected as Partial<T>);
        return next;
      } catch (err) {
        if (!(err instanceof ConflictError)) throw err;
      }
    }
    throw new ConflictError(`udb: conflict: gave up after ${limit} attempts`);
  }

  /** The columns of `row` an update may set. */
  private updateColumns(row: T): Record<string, unknown> {
    const schema = this.schema;
    if (schema) {
      return encodeRecord(this.messageType, decodeRecord(this.messageType, row, this.schemas), WriteMode.Update, this.schemas);
    }
    const out: Record<string, unknown> = { ...row };
    for (const field of this.key) delete out[field];
    return out;
  }

  async delete(key: RowKey, opts: TableWriteOptions = {}): Promise<void> {
    const response = await this.plane
      .unary(DATABROKER, "Delete", {
        message_type: this.messageType,
        filter: this.keyFilter(key),
        require_affected: 1,
        ...(opts.idempotencyKey ? { idempotency_key: opts.idempotencyKey } : {}),
      })
      .catch((err) => {
        throw classify(err, this.messageType, key);
      });
    this.fence?.rememberWrite(response);
  }

  private async update(
    key: RowKey,
    changes?: Partial<T>,
    expected?: Partial<T>,
    deltas?: Readonly<Record<string, number>>,
    opts: TableWriteOptions = {},
  ): Promise<void> {
    const request: Record<string, unknown> = {
      message_type: this.messageType,
      filter: this.keyFilter(key),
      require_affected: 1,
    };
    if (opts.idempotencyKey) request.idempotency_key = opts.idempotencyKey;
    if (changes && Object.keys(changes).length > 0) request.changes = structMap(changes);
    if (expected && Object.keys(expected).length > 0) request.expected = structMap(expected);
    if (deltas) {
      request.increments = Object.entries(deltas).map(([column, delta]) => ({ column, delta }));
    }
    const response = await this.plane.unary(DATABROKER, "Update", request).catch((err) => {
      throw classify(err, this.messageType, key);
    });
    this.fence?.rememberWrite(response);
  }
}

/** Map a broker status to the errors callers test with `instanceof`, keeping the
 *  typed status as the cause so {@link inspect} still reads its reason. */
function classify(err: unknown, messageType?: string, key?: RowKey): unknown {
  const info = inspect(err);
  const subject = messageType ? ` ${messageType} ${JSON.stringify(key ?? {})}` : "";
  if (info.reason === "UDB_CAS_CONFLICT" || info.reason === "UDB_REVISION_CONFLICT" || info.reason === "UDB_CAS_ROW_MISSING") {
    return new ConflictError(`udb:${subject} changed: ${(err as Error).message}`, { cause: err });
  }
  if (info.reason === "UDB_NO_ROWS_AFFECTED" || (info.grpc === grpc.status.NOT_FOUND && !(err instanceof NotFoundError))) {
    return new NotFoundError(`udb:${subject} not found: ${(err as Error).message}`, { cause: err });
  }
  return err;
}

// ── Transactions ────────────────────────────────────────────────────────────

/** A hand-assembled outbox event. Prefer {@link TxScope.emit} with the event's
 *  message type, which reads the topic and partition key from its contract. */
export interface TxEvent {
  topic: string;
  eventType: string;
  /** The partition key value (e.g. the aggregate id). */
  documentId: string;
  payload: Record<string, unknown>;
  messageType?: string;
}

/** The outbox envelope version this SDK writes and reads. */
export const EVENT_ENVELOPE_VERSION = 2;

/** Writes collected for one atomic commit. Use it only inside the function
 *  passed to {@link tx}. A write this scope refuses throws, and also fails the
 *  whole transaction even when the function catches the throw. */
export class TxScope {
  readonly mutations: any[] = [];
  /** The first write this scope refused. */
  failure?: unknown;

  constructor(
    readonly correlationId: string,
    private readonly schemas: ProtoSchemas = protoSchemas(),
    private readonly replayKey?: string,
  ) {}

  private fail(err: unknown): never {
    if (this.failure === undefined) this.failure = err;
    throw err;
  }

  private write(operation: string, messageType: string, row: Record<string, unknown>): void {
    let record: unknown;
    try {
      record = writeRecord(schemaOf(this.schemas, messageType), this.schemas, messageType, row);
    } catch (err) {
      this.fail(err);
    }
    this.push({ operation, message_type: messageType, record_json: jsonBytes(record) });
  }

  /** Write the whole row in the transaction. */
  upsert(messageType: string, row: Record<string, unknown>): void {
    this.write("upsert", messageType, row);
  }

  /** Create the row. A duplicate fails with code `unique` and rolls back every
   *  write and event in the transaction. */
  insert(messageType: string, row: Record<string, unknown>): void {
    this.write("insert", messageType, row);
  }

  /** Set the given columns on the row with `key`. */
  patch(messageType: string, key: RowKey, changes: Record<string, unknown>): void {
    this.update(messageType, key, changes);
  }

  /** Set columns if the stored row still holds `expected`; otherwise the whole
   *  transaction fails with code `conflict`. */
  updateIf(messageType: string, key: RowKey, expected: Record<string, unknown>, changes: Record<string, unknown>): void {
    if (Object.keys(expected).length === 0) this.fail(new Error(EMPTY_GUARD));
    this.update(messageType, key, changes, expected);
  }

  /** Write the updatable columns of `next` if the stored row still holds
   *  `expected`; the key comes from `next`'s primary-key fields. */
  updateRowIf(messageType: string, next: Record<string, unknown>, expected: Record<string, unknown>): void {
    let key: RowKey = {};
    let changes: Record<string, unknown> = {};
    try {
      const schema = this.schemas.message(messageType);
      const message = decodeRecord(messageType, next, this.schemas);
      const record = encodeRecord(messageType, message, WriteMode.Insert, this.schemas);
      if (Object.keys(expected).length === 0) throw new Error(EMPTY_GUARD);
      if (schema.primaryKeys.length === 0) throw new Error(`udb: ${messageType} declares no primary key`);
      for (const column of schema.primaryKeys) {
        const value = record[column];
        if (value === undefined || value === null || value === "") {
          throw new Error(`udb: ${messageType} has no value for primary-key column ${column}`);
        }
        key[column] = value;
      }
      changes = encodeRecord(messageType, message, WriteMode.Update, this.schemas);
    } catch (err) {
      this.fail(err);
    }
    this.update(messageType, key, changes, expected);
  }

  /** Remove the row with `key` in the transaction. */
  delete(messageType: string, key: RowKey): void {
    this.push({ operation: "delete", message_type: messageType, filter: structMap(key), require_affected: 1 });
  }

  private update(messageType: string, key: RowKey, changes: Record<string, unknown>, expected?: Record<string, unknown>): void {
    const mutation: Record<string, unknown> = {
      operation: "update",
      message_type: messageType,
      filter: structMap(key),
      require_affected: 1,
    };
    if (Object.keys(changes).length > 0) mutation.changes = structMap(changes);
    if (expected && Object.keys(expected).length > 0) mutation.expected = structMap(expected);
    this.push(mutation);
  }

  /** Queue an event on the outbox, committed with the transaction. The topic is the
   *  event's declared outbox topic and the partition key its declared partition-key
   *  field (message_event_contract); its own fields go under the envelope's payload.
   *  The older form taking a whole {@link TxEvent} is kept for undeclared events. */
  emit(messageType: string, event: Record<string, unknown>): void;
  emit(event: TxEvent): void;
  emit(first: string | TxEvent, event?: Record<string, unknown>): void {
    if (typeof first !== "string") {
      this.enqueue(first.messageType ?? first.eventType, first.topic, first.eventType, first.documentId, first.payload);
      return;
    }
    const messageType = first;
    let topic = "";
    let eventType = "";
    let document: unknown;
    let payload: Record<string, unknown> = {};
    try {
      const schema = this.schemas.message(messageType);
      topic = schema.topic;
      if (!topic) throw new Error(`udb: ${messageType} declares no outbox topic (message_event_contract.outbox_topic)`);
      const keyField = schema.partitionKeyField;
      if (!keyField) throw new Error(`udb: ${messageType} declares no partition key field (message_event_contract.partition_key_field)`);
      payload = encodeEvent(messageType, event ?? {}, this.schemas);
      document = payload[keyField];
      if (typeof document !== "string" || document === "") {
        throw new Error(`udb: event ${messageType} has no value in its partition key field ${keyField}`);
      }
      eventType = schema.eventType;
    } catch (err) {
      this.fail(err);
    }
    this.enqueue(messageType, topic, eventType, document as string, payload);
  }

  /** A replay key on each write of a keyed transaction, so a retry repeats none of them. */
  private push(mutation: Record<string, unknown>): void {
    if (this.replayKey && ["upsert", "update", "delete"].includes(String(mutation.operation))) {
      mutation.idempotency_key = `${this.replayKey}:${this.mutations.length}`;
    }
    this.mutations.push(mutation);
  }

  /** Random, or stable across retries of a keyed transaction. */
  private eventId(): string {
    if (!this.replayKey) return randomUUID();
    const sum = createHash("sha256").update(`${this.replayKey}:${this.mutations.length}`).digest();
    sum[6] = (sum[6] & 0x0f) | 0x50;
    sum[8] = (sum[8] & 0x3f) | 0x80;
    const hex = sum.subarray(0, 16).toString("hex");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }

  private enqueue(messageType: string, topic: string, eventType: string, document: string, payload: Record<string, unknown>): void {
    this.mutations.push({
      operation: "enqueue_outbox_event",
      message_type: messageType,
      collection: topic,
      object_key: document,
      payload: {
        event_id: this.eventId(),
        event_type: eventType,
        correlation_id: this.correlationId,
        document_id: document,
        envelope_version: EVENT_ENVELOPE_VERSION,
        payload,
      },
    });
  }
}

/** Options of {@link tx}. */
export interface TxOptions {
  /** Ties the events to a request; defaults to the client's, then the transaction id. */
  correlationId?: string;
  /** Keeps the commit's receipt so the next reads see this transaction's writes. */
  fence?: SessionFence;
  schemas?: ProtoSchemas;
  call?: CallOptions;
  /** Makes the transaction safe to retry after a lost response: each write gets a key
   *  derived from this, so the broker replays it, and each event a stable id for consumers
   *  to deduplicate. `body` must make the same writes on a retry. `insert` has no replay
   *  form; use `upsert` in a retried transaction. */
  replayKey?: string;
}

/** The error a rolled-back status frame describes. The terminal trailer stays
 *  authoritative when the broker sends one. */
function txStatusError(status: any): UdbError {
  const message = String(status?.message ?? "transaction rolled back");
  const code = Number(status?.code) || grpc.status.UNKNOWN;
  return new UdbError(
    "DataBroker/BeginTx",
    Object.assign(new Error(message), { code, details: message, metadata: new grpc.Metadata() }),
    status?.error_detail ?? undefined,
  );
}

/** Run `body` and commit everything it collected atomically over BeginTx: every
 *  write and event commits together or none does. `body` throwing commits nothing,
 *  and with nothing collected no stream opens. A failed mutation rejects with its
 *  own error (conflict, unique, ...). */
export async function tx(
  plane: StorePlane,
  body: (scope: TxScope) => void | Promise<void>,
  opts: TxOptions = {},
): Promise<void> {
  if (!plane.bidiStream) throw new Error("udb: this transport does not support transactions");
  const txId = randomUUID();
  const correlationId = opts.correlationId || plane.opts?.meta?.correlationId || txId;
  const scope = new TxScope(correlationId, schemasOf(plane, opts.schemas), opts.replayKey);
  await body(scope);
  if (scope.failure !== undefined) throw scope.failure;
  if (scope.mutations.length === 0) return;
  const stream = plane.bidiStream(DATABROKER, "BeginTx", opts.call);
  await new Promise<void>((resolve, reject) => {
    let committed = false;
    let refused: unknown;
    stream.on("data", (status: any) => {
      const state = String(status?.state ?? "");
      if (state === "TX_STATE_COMMITTED" || state === "2") {
        committed = true;
        opts.fence?.rememberReceipt(receiptFromProto(status?.write_receipt));
      }
      if (state === "TX_STATE_ERROR" || state === "TX_STATE_ROLLED_BACK" || state === "4" || state === "3") {
        refused = txStatusError(status);
      }
    });
    // The trailer carries the typed mutation error; an ERROR data frame must
    // not reject first and hide the Conflict/Unique classification.
    // Once the broker said it committed, a broken stream loses nothing.
    stream.on("error", (err: unknown) => (committed ? resolve() : reject(classify((err as any)?.udb ?? err))));
    stream.on("end", () => {
      if (refused) reject(classify(refused));
      else if (!committed) reject(new Error(`udb: transaction ${txId} ended without a commit acknowledgement`));
      else resolve();
    });
    for (const mutation of scope.mutations) stream.write({ ...mutation, tx_id: txId });
    stream.write({ tx_id: txId, commit: true });
    stream.end();
  });
}

// ── Durable consumers ───────────────────────────────────────────────────────

export interface ConsumedEvent<P = Record<string, unknown>> {
  id: string;
  topic: string;
  eventType: string;
  partitionKey: string;
  correlationId: string;
  /** When the journal published the event, when the broker reports it. */
  publishedAt?: Date;
  /** The domain event; decoded through the proto when `messageType` is set. */
  payload: P;
  /** The full envelope as delivered, for fields the payload lacks. */
  envelope: Record<string, unknown>;
}

class EnvelopeVersionError extends Error {
  readonly reason = "UDB_ENVELOPE_VERSION_UNSUPPORTED";
}

/** Unwrap the outbox envelope (a journal row may nest it once more). */
export function decodeEnvelope(payloadJson: string): Record<string, unknown> {
  let envelope = JSON.parse(payloadJson || "{}");
  const inner = envelope?.payload;
  if (inner && typeof inner === "object" && "event_type" in inner && "payload" in inner) envelope = inner;
  if (envelope?.envelope_version === undefined || envelope?.envelope_version === null) {
    throw new EnvelopeVersionError(`udb: envelope has no envelope_version; unversioned envelopes are not supported (this SDK reads version ${EVENT_ENVELOPE_VERSION})`);
  }
  const version = Number(envelope.envelope_version);
  if (version > EVENT_ENVELOPE_VERSION) {
    throw new EnvelopeVersionError(`udb: envelope_version ${version} is newer than this SDK understands (${EVENT_ENVELOPE_VERSION}); upgrade the SDK`);
  }
  if (version !== EVENT_ENVELOPE_VERSION) {
    throw new EnvelopeVersionError(`udb: envelope_version ${version} is not supported; this SDK reads version ${EVENT_ENVELOPE_VERSION}`);
  }
  return envelope;
}

/** Options of {@link consume}. Every default suits most consumers. */
export interface ConsumeOptions {
  signal?: AbortSignal;
  /** How often a failing handler is retried for one event before the event is
   *  dead-lettered (default 5, maximum 1000). */
  maxAttempts?: number;
  /** The first retry delay, doubled per attempt up to 30s (default 500ms). It also
   *  paces reconnects. */
  backoffMs?: number;
  /** Acknowledge this many successful events together (default 1, maximum 1024).
   *
   *  The default keeps the strict contract: an event is acknowledged before the
   *  next one is read, and a failed acknowledgement stops the stream before later
   *  events can advance the cursor. Raising it trades that for throughput: the
   *  batch settles as one watermark, so a refusal or crash redelivers the whole
   *  batch. Handlers must be idempotent, which at-least-once delivery requires. */
  ackBatchSize?: number;
  /** Flush a partial batch even on an idle stream (default 1s). Only meaningful
   *  when `ackBatchSize` is greater than 1. */
  ackIntervalMs?: number;
  /** Bound on each acknowledgement, including a shutdown flush (default 5s, maximum 30s). */
  ackTimeoutMs?: number;
  /** Make a name that has never subscribed replay the retained history. A new name
   *  otherwise starts at the journal head. No effect once the name has a position. */
  fromEarliest?: boolean;
  /** Observes handler failures and reconnects. */
  onError?: (err: unknown) => void;
  /** The event's message type: decodes the payload through the proto, and an event
   *  that does not decode stops the consumer. */
  messageType?: string;
  schemas?: ProtoSchemas;
}

const MAX_CONSUMER_ATTEMPTS = 1000;
const MAX_CONSUMER_ACK_BATCH = 1024;
const MAX_BACKOFF_MS = 30_000;
const SETTLEMENT_TIMEOUT_MS = 5_000;
const MAX_FAILURE_REASON_BYTES = 2048;
const RECENT_IDS = 4096;

interface NormalizedConsume {
  maxAttempts: number;
  backoffMs: number;
  ackBatchSize: number;
  ackIntervalMs: number;
  ackTimeoutMs: number;
}

function normalizeConsume(o: ConsumeOptions): NormalizedConsume {
  const positive = (value: number | undefined, fallback: number) => (value && value > 0 ? value : fallback);
  return {
    maxAttempts: Math.min(positive(o.maxAttempts, 5), MAX_CONSUMER_ATTEMPTS),
    backoffMs: Math.min(positive(o.backoffMs, 500), MAX_BACKOFF_MS),
    ackBatchSize: Math.min(positive(o.ackBatchSize, 1), MAX_CONSUMER_ACK_BATCH),
    ackIntervalMs: positive(o.ackIntervalMs, 1000),
    ackTimeoutMs: Math.min(positive(o.ackTimeoutMs, SETTLEMENT_TIMEOUT_MS), MAX_BACKOFF_MS),
  };
}

/** An error reconnecting cannot fix. */
class ConsumeFatal {
  constructor(readonly err: unknown) {}
}

/** An error from a settlement rather than the stream: this batch is not reissued
 *  on the way out. */
class SettlementRefused {
  constructor(readonly err: unknown) {}
}

const STOPPED = Symbol("consumer stopped");

function unwrapConsumeError(err: unknown): unknown {
  if (err instanceof SettlementRefused) return unwrapConsumeError(err.err);
  if (err instanceof ConsumeFatal) return err.err;
  return err;
}

/** Invalid consumer or cursor, missing authority and unavailable contracts need the
 *  caller; reconnecting cannot repair them. */
function consumeAttemptError(err: unknown): unknown {
  const code = inspect(err).grpc;
  return code === grpc.status.INVALID_ARGUMENT ||
    code === grpc.status.PERMISSION_DENIED ||
    code === grpc.status.NOT_FOUND ||
    code === grpc.status.FAILED_PRECONDITION ||
    code === grpc.status.OUT_OF_RANGE ||
    code === grpc.status.UNIMPLEMENTED
    ? new ConsumeFatal(err)
    : err;
}

function errorText(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/** The handler error as valid UTF-8, cut to the bound on a character boundary. */
function consumerFailureReason(err: unknown): string {
  // Encoding replaces a lone surrogate, so the bytes are always valid UTF-8.
  const bytes = Buffer.from(errorText(err), "utf8");
  if (bytes.length <= MAX_FAILURE_REASON_BYTES) return bytes.toString("utf8");
  let end = MAX_FAILURE_REASON_BYTES;
  while (end > 0 && (bytes[end] & 0xc0) === 0x80) end -= 1;
  return bytes.subarray(0, end).toString("utf8");
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const done = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal?.addEventListener("abort", done, { once: true });
  });
}

/** Three distinct states for an event id: its handler completed (a redelivery never
 *  runs it again), the broker confirmed its cursor or dead-letter (it is durable),
 *  and it is inside the open acknowledgement batch. */
class ConsumerProgress {
  private readonly completed = new Set<string>();
  private readonly durable = new Set<string>();
  private readonly pending: string[] = [];
  private readonly pendingSet = new Set<string>();
  poison?: { id: string; attempts: number; reason: string };
  /** Successful settlements: tells a routine reauthentication from no progress. */
  settlements = 0;

  has(id: string): boolean {
    return this.completed.has(id);
  }
  durableHas(id: string): boolean {
    return this.durable.has(id);
  }
  pendingHas(id: string): boolean {
    return this.pendingSet.has(id);
  }
  get pendingCount(): number {
    return this.pending.length;
  }

  /** Queue `id` as the newest watermark. A redelivery of a handled event is queued
   *  on purpose: re-sending its acknowledgement is what advances the cursor. */
  enqueue(id: string): void {
    this.pending.push(id);
    this.pendingSet.add(id);
  }

  add(id: string): void {
    this.remember(this.completed, id);
  }

  private remember(set: Set<string>, id: string): void {
    set.add(id);
    if (set.size > RECENT_IDS) set.delete(set.values().next().value!);
  }

  private clearPending(): void {
    this.pending.length = 0;
    this.pendingSet.clear();
  }

  /** Acknowledge the open batch as one watermark and dead-letter a poison event. It
   *  outlives a cancelled consumer for one bounded call, and never marks an event
   *  durable before the broker confirms, so a lost response leaves it retained and
   *  a retry is safe. Returns the failure instead of throwing. */
  async settle(plane: StorePlane, name: string, pattern: string, o: NormalizedConsume, onError?: (err: unknown) => void): Promise<unknown> {
    if (this.pending.length === 0 && !this.poison) return undefined;
    const call: CallOptions = { deadlineMs: o.ackTimeoutMs, noRetry: true };
    if (this.pending.length > 0) {
      const last = this.pending[this.pending.length - 1];
      try {
        await plane.unary(DATABROKER, "AckCdcEvents", { consumer_name: name, topic_pattern: pattern, event_id: last }, call);
      } catch (err) {
        onError?.(new Error(`udb: consumer ${name} ack ${last}: ${errorText(err)}`, { cause: err }));
        // At batch size 1 nothing carries across the reconnect: the broker redelivers
        // the event and the already-handled path acknowledges it again.
        if (o.ackBatchSize <= 1) this.clearPending();
        return new SettlementRefused(consumeAttemptError(err));
      }
      for (const id of this.pending) {
        this.add(id);
        this.remember(this.durable, id);
      }
      this.clearPending();
      this.settlements += 1;
    }
    if (this.poison) {
      const failed = this.poison;
      try {
        await plane.unary(
          DATABROKER,
          "NackCdcEvents",
          { consumer_name: name, topic_pattern: pattern, event_id: failed.id, attempts: failed.attempts, failure_reason: failed.reason },
          call,
        );
      } catch (err) {
        onError?.(new Error(`udb: consumer ${name} dead-letter ${failed.id}: ${errorText(err)}`, { cause: err }));
        return new SettlementRefused(consumeAttemptError(err));
      }
      this.add(failed.id);
      this.remember(this.durable, failed.id);
      this.poison = undefined;
      this.settlements += 1;
    }
    return undefined;
  }
}

async function handleWithRetry<P>(
  event: ConsumedEvent<P>,
  handle: (event: ConsumedEvent<P>, signal: AbortSignal) => Promise<void> | void,
  o: NormalizedConsume,
  signal: AbortSignal,
  onError?: (err: unknown) => void,
): Promise<unknown> {
  let delay = o.backoffMs;
  let failure: unknown;
  for (let attempt = 1; attempt <= o.maxAttempts; attempt += 1) {
    try {
      await handle(event, signal);
      return undefined;
    } catch (err) {
      failure = err ?? new Error("udb: consumer handler failed");
    }
    onError?.(new Error(`event ${event.id} attempt ${attempt}: ${errorText(failure)}`, { cause: failure }));
    if (attempt === o.maxAttempts) break;
    await sleep(delay, signal);
    if (signal.aborted) return STOPPED;
    delay = Math.min(delay * 2, MAX_BACKOFF_MS);
  }
  return failure;
}

function publishedAtOf(envelope: any): Date | undefined {
  const stamp = envelope?.published_at;
  if (!stamp) return undefined;
  const ms = Number(stamp.seconds ?? 0) * 1000 + Math.floor(Number(stamp.nanos ?? 0) / 1e6);
  return ms > 0 ? new Date(ms) : undefined;
}

function decodeConsumedEvent<P>(id: string, raw: any, messageType: string | undefined, schemas: ProtoSchemas): ConsumedEvent<P> {
  const envelope = decodeEnvelope(String(raw?.payload_json ?? ""));
  const domain = envelope.payload;
  let payload: unknown = domain ?? {};
  if (messageType && domain !== undefined && domain !== null) {
    try {
      payload = decodeEventPayload(messageType, domain as Record<string, unknown>, schemas);
    } catch (err) {
      throw new Error(`payload does not decode as ${messageType}: ${errorText(err)}`);
    }
  }
  return {
    id,
    topic: String(raw?.topic ?? ""),
    eventType: String(envelope.event_type ?? ""),
    partitionKey: String(raw?.partition_key ?? ""),
    correlationId: String(envelope.correlation_id ?? ""),
    publishedAt: publishedAtOf(raw),
    payload: payload as P,
    envelope,
  };
}

/** Hold items a stream delivered until the loop asks; pauses the stream when the
 *  loop falls behind, so a slow handler cannot grow the queue without bound. */
const STREAM_HIGH_WATER = 256;

async function consumeOnce<P>(
  plane: StorePlane,
  name: string,
  topicPattern: string,
  handle: (event: ConsumedEvent<P>, signal: AbortSignal) => Promise<void> | void,
  opts: ConsumeOptions,
  o: NormalizedConsume,
  progress: ConsumerProgress,
  schemas: ProtoSchemas,
): Promise<unknown> {
  const signal = opts.signal;
  const batched = o.ackBatchSize > 1;
  const settle = () => progress.settle(plane, name, topicPattern, o, opts.onError);
  const failed = await settle();
  if (failed) return failed;
  if (signal?.aborted) return STOPPED;
  const attempt = new AbortController();
  const stopAttempt = () => attempt.abort();
  signal?.addEventListener("abort", stopAttempt, { once: true });
  let stream: any;
  try {
    // Progress markers let a consumer of a quiet topic move its stored position
    // past the rest of the journal; they never reach the handler.
    stream = plane.serverStream!(
      DATABROKER,
      "PublishCDC",
      {
        topic_pattern: topicPattern,
        consumer_name: name,
        progress_markers: true,
        start: opts.fromEarliest ? "CDC_CONSUMER_START_EARLIEST" : "CDC_CONSUMER_START_LATEST",
      },
      { signal: attempt.signal },
    );
  } catch (err) {
    signal?.removeEventListener("abort", stopAttempt);
    return consumeAttemptError(err);
  }
  type Item = { envelope: any } | { error: unknown } | { ended: true };
  const queue: Item[] = [];
  let wake: (() => void) | undefined;
  const notify = () => {
    const resume = wake;
    wake = undefined;
    resume?.();
  };
  let paused = false;
  stream.on("data", (envelope: any) => {
    queue.push({ envelope });
    if (queue.length >= STREAM_HIGH_WATER && stream.pause) {
      paused = true;
      stream.pause();
    }
    notify();
  });
  stream.on("error", (err: any) => {
    queue.push({ error: err?.udb ?? err });
    notify();
  });
  stream.on("end", () => {
    queue.push({ ended: true });
    notify();
  });
  signal?.addEventListener("abort", notify, { once: true });
  let tick = false;
  const ticker = batched
    ? setInterval(() => {
        tick = true;
        notify();
      }, o.ackIntervalMs)
    : undefined;
  try {
    for (;;) {
      if (signal?.aborted) return STOPPED;
      if (tick) {
        tick = false;
        const failure = await settle();
        if (failure) return failure;
      }
      const item = queue.shift();
      if (!item) {
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
        continue;
      }
      if (paused && queue.length < STREAM_HIGH_WATER / 2) {
        paused = false;
        stream.resume?.();
      }
      if ("error" in item || "ended" in item) {
        if (batched) {
          // Settle the retained watermark first, so a reconnect does not replay
          // acknowledged events.
          const failure = await settle();
          if (failure) return failure;
        }
        if ("ended" in item) return new Error("udb: the event stream ended");
        return consumeAttemptError(item.error);
      }
      const envelope = item.envelope;
      const id = String(envelope?.event_id ?? "");
      if (progress.pendingHas(id)) continue;
      if (envelope?.progress_only) {
        // Not an event: the stream read the journal up to this position. Every
        // earlier event is settled or queued ahead of it, so its token goes last.
        if (progress.durableHas(id)) continue;
        progress.enqueue(id);
      } else {
        // A durable id needs nothing at all; a handled-but-not-durable id is
        // acknowledged again, which advances a cursor the broker never confirmed.
        if (!progress.has(id) && !progress.durableHas(id)) {
          let event: ConsumedEvent<P>;
          try {
            event = decodeConsumedEvent<P>(id, envelope, opts.messageType, schemas);
          } catch (err) {
            const message = `udb: consumer ${name}: event ${id}: ${errorText(err)}`;
            return new ConsumeFatal(err instanceof EnvelopeVersionError ? new EnvelopeVersionError(message) : new Error(message, { cause: err }));
          }
          const failure = await handleWithRetry(event, handle, o, attempt.signal, opts.onError);
          if (failure !== undefined) {
            if (failure === STOPPED || signal?.aborted) return STOPPED;
            if ((failure as Error)?.name === "AbortError" || (failure as Error)?.name === "TimeoutError") {
              return new ConsumeFatal(new Error(`udb: consumer ${name} gave up on event ${id}: ${errorText(failure)}`, { cause: failure }));
            }
            // A poison event is dead-lettered: the broker stores it and advances this
            // consumer in one transaction, so later events keep flowing. A broker that
            // cannot (UNIMPLEMENTED) stops the consumer instead of discarding the event.
            progress.poison = { id, attempts: o.maxAttempts, reason: consumerFailureReason(failure) };
            const refused = await settle();
            if (refused) return refused;
            continue;
          }
          // The handler completed even if the acknowledgement is lost: a redelivery
          // retries only the acknowledgement and never reruns the handler.
          progress.add(id);
        }
        progress.enqueue(id);
      }
      if (!batched || progress.pendingCount >= o.ackBatchSize) {
        const failure = await settle();
        if (failure) return failure;
      }
    }
  } finally {
    if (ticker) clearInterval(ticker);
    signal?.removeEventListener("abort", stopAttempt);
    signal?.removeEventListener("abort", notify);
    attempt.abort();
    stream.cancel?.();
  }
}

/** Deliver every event on `topicPattern` to `handle` as the durable consumer `name`:
 *  the broker keeps the position (AckCdcEvents) and reconnects resume there. An event
 *  is acknowledged after `handle` resolves, so a crash redelivers at most the event
 *  in flight. A handler that keeps failing has its event dead-lettered after
 *  `maxAttempts`, and later events keep flowing. A failed acknowledgement stops the
 *  stream before later events can advance the cursor. An envelope newer than this
 *  SDK understands stops the consumer rather than being misread.
 *
 *  Resolves when `signal` aborts, after flushing completed events. Rejects on a
 *  refusal reconnecting cannot fix, or when that shutdown flush fails. */
export async function consume<P = Record<string, unknown>>(
  plane: StorePlane,
  name: string,
  topicPattern: string,
  handle: (event: ConsumedEvent<P>, signal: AbortSignal) => Promise<void> | void,
  opts: ConsumeOptions = {},
): Promise<void> {
  if (!plane.serverStream) throw new Error("udb: this transport does not support streams");
  const o = normalizeConsume(opts);
  const progress = new ConsumerProgress();
  const schemas = schemasOf(plane, opts.schemas);
  // Flush a retained batch before returning, so an event whose handler completed
  // just before shutdown is still acknowledged. A settlement the broker definitively
  // refused is not reissued: it could only repeat a call that cannot succeed.
  const finish = async (cause?: unknown): Promise<void> => {
    if (cause instanceof SettlementRefused) throw unwrapConsumeError(cause);
    const failure = await progress.settle(plane, name, topicPattern, o, opts.onError);
    if (cause !== undefined) {
      if (failure) throw new AggregateError([unwrapConsumeError(cause), unwrapConsumeError(failure)], errorText(unwrapConsumeError(cause)));
      throw unwrapConsumeError(cause);
    }
    if (failure) throw unwrapConsumeError(failure);
  };
  let delay = o.backoffMs;
  for (;;) {
    if (opts.signal?.aborted) return finish();
    const before = progress.settlements;
    const failure = await consumeOnce(plane, name, topicPattern, handle, opts, o, progress, schemas);
    if (opts.signal?.aborted) {
      return failure instanceof SettlementRefused ? finish(failure) : finish();
    }
    if (failure === STOPPED) return finish();
    const inner = failure instanceof SettlementRefused ? failure.err : failure;
    if (inner instanceof ConsumeFatal) return finish(failure instanceof SettlementRefused ? failure : inner);
    // Credential expiry and progress on a healthy stream must not accumulate an
    // ever-longer outage across a routine reauthentication.
    if (progress.settlements !== before || inspect(inner).grpc === grpc.status.UNAUTHENTICATED) delay = o.backoffMs;
    opts.onError?.(new Error(`udb: consumer ${name} reconnecting: ${errorText(inner)}`, { cause: inner }));
    await sleep(delay, opts.signal);
    if (opts.signal?.aborted) return finish();
    delay = Math.min(delay * 2, MAX_BACKOFF_MS);
  }
}

// ── In-memory data plane (tests) ────────────────────────────────────────────

/** A small stand-in for a unit test of code that only uses `Table` over plain rows:
 *  Select, Upsert, Insert, Update and Delete, with the broker's NotFound, Conflict
 *  and Unique reasons. For the broker's full semantics (tenancy, transactions,
 *  consumers, faults) use `createFake` from `@udb_plus/sdk/udbtest`. */
export class InMemoryDataPlane implements StorePlane {
  readonly rows = new Map<string, Record<string, unknown>[]>();

  constructor(private readonly keys: Record<string, string[]>) {}

  private matches(row: Record<string, unknown>, filter: Record<string, unknown> = {}): boolean {
    return Object.entries(filter ?? {}).every(([field, value]) => row[field] === value);
  }

  private refuse(code: number, reason: string, message: string): never {
    const err: any = new Error(message);
    err.code = code;
    throw new UdbError("InMemory", err, { reason, kind: code === 9 ? 8 : code === 6 ? 10 : 9 });
  }

  async unary(_service: string, method: string, request: any): Promise<any> {
    const table = this.rows.get(request.message_type) ?? [];
    this.rows.set(request.message_type, table);
    switch (method) {
      case "Select": {
        const found = table.filter((row) => this.matches(row, request.filter));
        const limited = request.limit ? found.slice(0, request.limit) : found;
        return {
          records_json: limited.map((row) => Buffer.from(JSON.stringify(row))),
          has_more: limited.length < found.length,
          exact_total: request.include_total ? found.length : 0,
        };
      }
      case "Insert":
      case "Upsert": {
        const row = JSON.parse(Buffer.from(request.record_json).toString("utf8"));
        const key = this.keys[request.message_type] ?? request.conflict_fields ?? [];
        const index = table.findIndex((existing) => key.every((field: string) => existing[field] === row[field]));
        if (index >= 0 && method === "Insert") this.refuse(6, "UDB_UNIQUE_VIOLATION", "duplicate key");
        if (index >= 0) table[index] = row;
        else table.push(row);
        return { affected_rows: 1 };
      }
      case "Update": {
        const hits = table.filter((row) => this.matches(row, request.filter));
        if (request.require_affected && hits.length !== request.require_affected) {
          if (request.expected && hits.length === 0) this.refuse(9, "UDB_CAS_ROW_MISSING", "row missing");
          this.refuse(5, "UDB_NO_ROWS_AFFECTED", "no rows affected");
        }
        if (request.expected && hits.some((row) => !this.matches(row, request.expected))) {
          this.refuse(9, "UDB_CAS_CONFLICT", "precondition failed");
        }
        for (const row of hits) Object.assign(row, request.changes ?? {});
        return { affected_rows: hits.length };
      }
      case "Delete": {
        const keep = table.filter((row) => !this.matches(row, request.filter));
        const removed = table.length - keep.length;
        if (request.require_affected && removed !== request.require_affected) {
          this.refuse(5, "UDB_NO_ROWS_AFFECTED", "no rows affected");
        }
        this.rows.set(request.message_type, keep);
        return { affected_rows: removed };
      }
      default:
        throw new Error(`udb: InMemoryDataPlane does not implement ${method}`);
    }
  }
}
