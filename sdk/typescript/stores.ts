// Typed stores, transactions, durable consumers and error codes — the
// TypeScript port of the Go SDK's Table[T], Tx, Consume and ErrorCode (T1).
//
// Everything here goes through a `StorePlane` (anything with the generated
// `UdbCore.unary`/`serverStream`/`bidiStream` shape), so tests can swap in
// `InMemoryDataPlane` instead of a broker.

import { randomUUID } from "node:crypto";
import * as grpc from "@grpc/grpc-js";
import { UdbError } from "./generatedClient";

const DATABROKER = "udb.services.v1.DataBroker";

/** The transport a store needs: the generated `UdbCore` satisfies it. */
export interface StorePlane {
  unary(service: string, method: string, request: any): Promise<any>;
  serverStream?(service: string, method: string, request: any): any;
  bidiStream?(service: string, method: string): any;
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
  missing?: Record<string, string>;
  retryAfterMs?: number;
}

/** The row a keyed read or conditional write named does not exist. */
export class NotFoundError extends Error {
  readonly code = ErrorCode.NotFound;
}

/** A compare-and-swap precondition did not hold. */
export class ConflictError extends Error {
  readonly code = ErrorCode.Conflict;
}

/** Classify any error from a store, the generated client or a raw gRPC call. */
export function inspect(err: unknown): ErrorInfo {
  if (err instanceof NotFoundError) return { code: ErrorCode.NotFound };
  if (err instanceof ConflictError) return { code: ErrorCode.Conflict };
  const udb = err instanceof UdbError ? err : (err as any)?.udb instanceof UdbError ? (err as any).udb : undefined;
  if (!udb) return { code: ErrorCode.Unknown };
  const detail = udb.detail ?? {};
  const reason = typeof detail.reason === "string" && detail.reason.startsWith("UDB_") ? detail.reason : undefined;
  return {
    code: (reason && REASON_CODES[reason]) || KIND_CODES[detail.kind as ErrorKind] || GRPC_CODES[udb.code] || ErrorCode.Unknown,
    reason,
    constraint: detail.constraint as string | undefined,
    column: detail.column as string | undefined,
    fixHint: detail.fix_hint as string | undefined,
    missing: detail.missing as Record<string, string> | undefined,
    retryAfterMs: detail.retry_after_ms as number | undefined,
  };
}

export const codeOf = (err: unknown): ErrorCode => inspect(err).code;
export const isNotFound = (err: unknown): boolean => codeOf(err) === ErrorCode.NotFound;
export const isConflict = (err: unknown): boolean => codeOf(err) === ErrorCode.Conflict;
export const isUnique = (err: unknown, constraint?: string): boolean => {
  const info = inspect(err);
  return info.code === ErrorCode.Unique && (!constraint || info.constraint === constraint);
};

// ── Table ───────────────────────────────────────────────────────────────────

/** The primary-key values of one row, by field name. */
export type RowKey = Record<string, unknown>;

export interface TablePage<T> {
  rows: T[];
  hasMore: boolean;
  total?: number;
}

function jsonBytes(value: unknown): Buffer {
  return Buffer.from(JSON.stringify(value), "utf8");
}

function decodeRows<T>(response: any): T[] {
  const raw = response?.records_json;
  const items: unknown[] = Array.isArray(raw) ? raw : raw ? [raw] : [];
  const rows: T[] = [];
  for (const item of items) {
    const text = Buffer.isBuffer(item) ? item.toString("utf8") : typeof item === "string" ? item : "";
    if (!text) continue;
    const parsed = JSON.parse(text);
    if (Array.isArray(parsed)) rows.push(...(parsed as T[]));
    else rows.push(parsed as T);
  }
  return rows;
}

/** A typed store over one entity. Writes go through the broker's data plane, so
 *  policies, tenancy, projections and events apply as for any write. */
export class Table<T extends Record<string, unknown>> {
  constructor(
    private readonly plane: StorePlane,
    readonly messageType: string,
    private readonly opts: { key: string[] },
  ) {
    if (!opts.key.length) throw new Error(`udb: Table(${messageType}) needs its key fields`);
  }

  /** The filter that addresses exactly one row. */
  keyFilter(key: RowKey): Record<string, unknown> {
    const filter: Record<string, unknown> = {};
    for (const field of this.opts.key) {
      if (!(field in key)) throw new Error(`udb: ${this.messageType} key is missing ${field}`);
      filter[field] = key[field];
    }
    return filter;
  }

  async get(key: RowKey): Promise<T> {
    const page = await this.select(this.keyFilter(key), { limit: 1 });
    if (!page.rows.length) throw new NotFoundError(`udb: ${this.messageType} ${JSON.stringify(key)} not found`);
    return page.rows[0];
  }

  async select(
    filter: Record<string, unknown> = {},
    opts: { limit?: number; includeTotal?: boolean; primaryRead?: boolean } = {},
  ): Promise<TablePage<T>> {
    const response = await this.plane.unary(DATABROKER, "Select", {
      message_type: this.messageType,
      filter,
      limit: opts.limit ?? 0,
      include_total: opts.includeTotal ?? false,
      context: opts.primaryRead ? { primary_read: true } : undefined,
    });
    return {
      rows: decodeRows<T>(response),
      hasMore: Boolean(response?.has_more),
      total: opts.includeTotal ? Number(response?.exact_total ?? 0) : undefined,
    };
  }

  async count(filter: Record<string, unknown> = {}): Promise<number> {
    return (await this.select(filter, { limit: 1, includeTotal: true })).total ?? 0;
  }

  /** Insert or replace the whole row; NotFoundError when the broker's scope
   *  guard leaves a conflicting row untouched. */
  async upsert(row: T): Promise<void> {
    const response = await this.plane.unary(DATABROKER, "Upsert", {
      message_type: this.messageType,
      record_json: jsonBytes(row),
      conflict_fields: this.opts.key,
    });
    if (String(response?.affected_rows ?? "0") === "0") {
      throw new NotFoundError(`${this.messageType}: write target not found in the verified scope`);
    }
  }

  /** Set some columns of an existing row; NotFoundError when it does not exist. */
  async patch(key: RowKey, changes: Partial<T>): Promise<void> {
    await this.update(key, changes);
  }

  /** Set columns only while the row still holds `expected`; ConflictError when
   *  it changed, NotFoundError when it is gone. */
  async updateIf(key: RowKey, expected: Partial<T>, changes: Partial<T>): Promise<void> {
    await this.update(key, changes, expected);
  }

  async delete(key: RowKey): Promise<void> {
    try {
      await this.plane.unary(DATABROKER, "Delete", {
        message_type: this.messageType,
        filter: this.keyFilter(key),
        require_affected: 1,
      });
    } catch (err) {
      throw classify(err, this.messageType, key);
    }
  }

  private async update(key: RowKey, changes: Partial<T>, expected?: Partial<T>): Promise<void> {
    try {
      await this.plane.unary(DATABROKER, "Update", {
        message_type: this.messageType,
        filter: this.keyFilter(key),
        changes,
        expected,
        require_affected: 1,
      });
    } catch (err) {
      throw classify(err, this.messageType, key);
    }
  }
}

function classify(err: unknown, messageType: string, key: RowKey): unknown {
  switch (codeOf(err)) {
    case ErrorCode.NotFound:
      return new NotFoundError(`udb: ${messageType} ${JSON.stringify(key)} not found`);
    case ErrorCode.Conflict:
      return new ConflictError(`udb: ${messageType} ${JSON.stringify(key)} changed`);
    default:
      return err;
  }
}

// ── Transactions ────────────────────────────────────────────────────────────

/** An event emitted inside a transaction; it commits with the writes. */
export interface TxEvent {
  topic: string;
  eventType: string;
  /** The partition key value (e.g. the aggregate id). */
  documentId: string;
  payload: Record<string, unknown>;
  messageType?: string;
}

export const EVENT_ENVELOPE_VERSION = 2;

/** Writes collected for one atomic commit. */
export class TxScope {
  readonly mutations: any[] = [];
  constructor(readonly correlationId: string) {}

  upsert(messageType: string, row: Record<string, unknown>): void {
    this.mutations.push({ operation: "upsert", message_type: messageType, record_json: jsonBytes(row) });
  }
  patch(messageType: string, key: RowKey, changes: Record<string, unknown>): void {
    this.mutations.push({ operation: "update", message_type: messageType, filter: key, changes });
  }
  updateIf(messageType: string, key: RowKey, expected: Record<string, unknown>, changes: Record<string, unknown>): void {
    this.mutations.push({ operation: "update", message_type: messageType, filter: key, changes, expected });
  }
  delete(messageType: string, key: RowKey): void {
    this.mutations.push({ operation: "delete", message_type: messageType, filter: key });
  }
  emit(event: TxEvent): void {
    this.mutations.push({
      operation: "enqueue_outbox_event",
      message_type: event.messageType ?? event.eventType,
      collection: event.topic,
      object_key: event.documentId,
      payload: {
        event_id: randomUUID(),
        event_type: event.eventType,
        correlation_id: this.correlationId,
        document_id: event.documentId,
        envelope_version: EVENT_ENVELOPE_VERSION,
        payload: event.payload,
      },
    });
  }
}

/** Run `body`, then commit its writes and events atomically over BeginTx. A
 *  failed mutation rejects with its own error (Conflict, Unique, ...). */
export async function tx(plane: StorePlane, body: (scope: TxScope) => void | Promise<void>): Promise<void> {
  if (!plane.bidiStream) throw new Error("udb: this transport does not support transactions");
  const scope = new TxScope(randomUUID());
  await body(scope);
  const txId = randomUUID();
  const stream = plane.bidiStream(DATABROKER, "BeginTx");
  await new Promise<void>((resolve, reject) => {
    let committed = false;
    let failure: Error | undefined;
    stream.on("data", (status: any) => {
      const state = String(status?.state ?? "");
      if (state === "TX_STATE_COMMITTED" || state === "2") committed = true;
      if (state === "TX_STATE_ERROR" || state === "TX_STATE_ROLLED_BACK" || state === "4" || state === "3") {
        const message = `udb: transaction ${txId} rolled back: ${status?.message ?? ""}`;
        failure = status?.error_detail ? new UdbError("DataBroker/BeginTx",
          Object.assign(new Error(message), { code: Number(status.code) || grpc.status.UNKNOWN,
            details: message, metadata: new grpc.Metadata() }), status.error_detail) : new Error(message);
      }
    });
    // The trailer carries the typed mutation error; an ERROR data frame must
    // not reject first and hide the Conflict/Unique classification.
    stream.on("error", (err: unknown) => reject((err as any)?.udb ?? err));
    stream.on("end", () => {
      if (failure || !committed) reject(failure ?? new Error(`udb: transaction ${txId} ended without a commit acknowledgement`));
      else resolve();
    });
    for (const mutation of scope.mutations) stream.write({ ...mutation, tx_id: txId });
    stream.write({ tx_id: txId, commit: true });
    stream.end();
  });
}

// ── Durable consumers ───────────────────────────────────────────────────────

export interface ConsumedEvent {
  id: string;
  topic: string;
  eventType: string;
  partitionKey: string;
  payload: Record<string, unknown>;
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
  const version = Number(envelope?.envelope_version ?? 0);
  if (version > EVENT_ENVELOPE_VERSION) {
    throw new EnvelopeVersionError(`udb: envelope_version ${version} is newer than this SDK understands (${EVENT_ENVELOPE_VERSION}); upgrade the SDK`);
  }
  return envelope;
}

/** Deliver every event on `topicPattern` to `handle` as the durable consumer
 *  `name`: the broker keeps the position (AckCdcEvents), reconnects resume
 *  there, and an event is acked only after `handle` resolves. Returns when
 *  `signal` aborts or a refusal reconnecting cannot fix arrives. */
export async function consume(
  plane: StorePlane,
  name: string,
  topicPattern: string,
  handle: (event: ConsumedEvent) => Promise<void>,
  opts: { signal?: AbortSignal; backoffMs?: number; onError?: (err: unknown) => void } = {},
): Promise<void> {
  if (!plane.serverStream) throw new Error("udb: this transport does not support streams");
  const seen = new Set<string>();
  let delay = opts.backoffMs ?? 500;
  for (;;) {
    if (opts.signal?.aborted) return;
    const failure = await new Promise<unknown>((resolve) => {
      const stream = plane.serverStream!(DATABROKER, "PublishCDC", { topic_pattern: topicPattern, consumer_name: name });
      let chain = Promise.resolve();
      let closing = false;
      let stopped = false;
      let handlerFailure: unknown;
      const finish = (terminalError: unknown) => {
        if (closing) return;
        closing = true;
        // Drain queued work before reconnecting. A failed head event stops the
        // whole queue so a later ack cannot advance the cursor past it.
        void chain.then(() => {
          opts.signal?.removeEventListener("abort", abort);
          resolve(handlerFailure ?? terminalError);
        });
      };
      const abort = () => {
        stream.cancel?.();
        finish(undefined);
      };
      opts.signal?.addEventListener("abort", abort, { once: true });
      stream.on("data", (envelope: any) => {
        if (closing) return;
        chain = chain.then(async () => {
          if (stopped || opts.signal?.aborted) return;
          const id = String(envelope?.event_id ?? "");
          if (seen.has(id)) return;
          const decoded = decodeEnvelope(String(envelope?.payload_json ?? ""));
          await handle({
            id,
            topic: String(envelope?.topic ?? ""),
            eventType: String(decoded.event_type ?? ""),
            partitionKey: String(envelope?.partition_key ?? ""),
            payload: (decoded.payload as Record<string, unknown>) ?? {},
            envelope: decoded,
          });
          await plane.unary(DATABROKER, "AckCdcEvents", { consumer_name: name, topic_pattern: topicPattern, event_id: id });
          seen.add(id);
          if (seen.size > 4096) seen.delete(seen.values().next().value!);
          delay = opts.backoffMs ?? 500;
        }).catch((err) => {
          stopped = true;
          handlerFailure = err;
          finish(err);
          stream.cancel?.();
        });
      });
      stream.on("error", (err: any) => {
        finish(err?.udb ?? err);
      });
      stream.on("end", () => finish(new Error("udb: the event stream ended")));
      if (opts.signal?.aborted) abort();
    });
    if (opts.signal?.aborted) return;
    const error = (failure as any)?.udb ?? failure;
    if (error instanceof EnvelopeVersionError || [3, 5, 7].includes((error as any)?.code)) throw error;
    opts.onError?.(error);
    if (opts.signal?.aborted) return;
    await new Promise<void>((resolve) => {
      const done = () => {
        clearTimeout(timer);
        opts.signal?.removeEventListener("abort", done);
        resolve();
      };
      const timer = setTimeout(done, delay);
      opts.signal?.addEventListener("abort", done, { once: true });
      if (opts.signal?.aborted) done();
    });
    delay = Math.min(delay * 2, 30_000);
  }
}

// ── In-memory data plane (tests) ────────────────────────────────────────────

/** A broker stand-in for unit tests of code that uses `Table`: Select, Upsert,
 *  Update (with expected/require_affected) and Delete over plain rows, with the
 *  same NotFound/Conflict outcomes and reason codes the broker sends. */
export class InMemoryDataPlane implements StorePlane {
  readonly rows = new Map<string, Record<string, unknown>[]>();

  constructor(private readonly keys: Record<string, string[]>) {}

  private matches(row: Record<string, unknown>, filter: Record<string, unknown> = {}): boolean {
    return Object.entries(filter ?? {}).every(([field, value]) => row[field] === value);
  }

  private refuse(code: number, reason: string, message: string): never {
    const err: any = new Error(message);
    err.code = code;
    throw new UdbError("InMemory", err, { reason, kind: code === 9 ? 8 : 9 });
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
      case "Upsert": {
        const row = JSON.parse(Buffer.from(request.record_json).toString("utf8"));
        const key = this.keys[request.message_type] ?? request.conflict_fields ?? [];
        const index = table.findIndex((existing) => key.every((field: string) => existing[field] === row[field]));
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
