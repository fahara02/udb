// Bound entity / table helper: hides record_json packing, conflict_fields, the
// plain-object filter, and record_json/records_json decode behind a typed handle
// over the DataBroker Upsert/Select/Delete RPCs.
//
// The broker remains the sole RLS / authority enforcement point — this helper
// never sets authoritative principal/actor/scopes; it only shapes the request
// body the caller already controls. The key (+ optional tenant/project field
// names) is supplied by the caller (or, once lane 07 ships {{ENTITY_*}}, read
// from the generated ENTITY_REGISTRY by `UdbProject.entity`).

import type { SessionFence } from "./consistency";
import type { CallOptions, UdbCore } from "./generatedClient";

const DATABROKER = "udb.services.v1.DataBroker";

/** Construction options for an {@link EntityHandle}. */
export interface EntityHandleOptions {
  /** Conflict/primary-key field names used for `conflict_fields` on upsert. */
  key: string[];
  /** Tenant column name (informational; broker still enforces tenancy). */
  tenantField?: string;
  /** Project column name (informational). */
  projectField?: string;
}

/** Encode a JS record as `record_json` bytes the broker expects. */
function jsonBytes(record: unknown): Buffer {
  return Buffer.from(JSON.stringify(record), "utf8");
}

/** A guarded write with an empty precondition would apply to every matched row. */
const EMPTY_CAS =
  "udb: guarded write requires a non-empty expected precondition; refusing to run unconditionally";

function refuseEmptyExpected(expected: Record<string, unknown> | undefined): void {
  if (expected !== undefined && Object.keys(expected).length === 0) throw new Error(EMPTY_CAS);
}

/** A blank key would silently disable replay safety while looking set. */
function refuseBlankKey(key: string | undefined): void {
  if (key && key.trim() === "") throw new Error("udb: idempotency key must not be whitespace-only");
}

/** The outcome of {@link EntityHandle.upsertResult}. */
export interface UpsertResult<T> {
  /** The broker's mutation response. */
  response: any;
  /** The written row, when `returnRecord` asked for it and the broker sent one. */
  record: T | null;
  /** True when the broker collapsed this write onto an earlier one with the same
   *  idempotency key instead of applying it again. */
  wasDuplicate: boolean;
}

/** Decode one row of a record set. A row that does not decode throws: skipping it
 *  would leave the page short while hasMore and the total describe the full result. */
function decodeRow<T>(raw: unknown, index: number): T {
  const row = decodeRecord<T>(raw);
  if (row == null) {
    const bytes = raw instanceof Uint8Array ? raw.length : String(raw ?? "").length;
    throw new Error(`udb: decode record row ${index}: ${bytes === 0 ? "empty body" : "not JSON"}`);
  }
  return row;
}

/** Decode a record set. `rows` is a compatibility field the broker sends empty, one
 *  entry per record, so rows without records_json are missing records, not no match. */
function decodeRecordSet<T>(resp: any): T[] {
  const records: unknown[] = Array.isArray(resp?.records_json) ? resp.records_json : [];
  if (records.length === 0 && Array.isArray(resp?.rows) && resp.rows.length > 0) {
    throw new Error(`udb: record set carries ${resp.rows.length} compatibility row(s) but no records_json`);
  }
  return records.map((raw, index) => decodeRow<T>(raw, index));
}

/** Decode `record_json` (single) or `records_json` (repeated) bytes/strings back
 *  to JS objects. The keepCase loader surfaces `bytes` as Buffer; strings pass
 *  through JSON.parse. */
function decodeRecord<T>(raw: unknown): T | null {
  if (raw == null) return null;
  let text: string;
  if (typeof raw === "string") text = raw;
  else if (Buffer.isBuffer(raw)) text = raw.toString("utf8");
  else if (raw instanceof Uint8Array) text = Buffer.from(raw).toString("utf8");
  else return raw as T; // already an object
  if (!text) return null;
  try {
    return JSON.parse(text) as T;
  } catch {
    return null;
  }
}

/**
 * A typed handle bound to one entity message type. Forwards to the DataBroker
 * Upsert / Select / Delete RPCs over the shared core, packing/decoding
 * record_json so callers work in plain JS records.
 */
export class EntityHandle<T = any> {
  /** `context` is merged into each request body, for example tenant_id/project_id.
   *  `fence` is the client's read-your-writes state: a write's receipt is kept
   *  there, and the reads right after it carry the matching read fence. */
  constructor(
    private readonly core: UdbCore,
    private readonly messageType: string,
    private readonly opts: EntityHandleOptions,
    private readonly context: Record<string, unknown> = {},
    private readonly fence?: SessionFence,
  ) {}

  /** The request context of a read: the bound context plus any read fence. */
  private readContext(): Record<string, unknown> {
    return this.fence ? { ...this.context, ...this.fence.readContext() } : this.context;
  }

  private async mutate(rpc: string, request: Record<string, unknown>, call?: CallOptions): Promise<any> {
    const resp: any = await this.core.unary(DATABROKER, rpc, request, call);
    this.fence?.rememberWrite(resp);
    return resp;
  }

  /** Upsert one record. Emits exactly ONE `Upsert` with `record_json` bytes and
   *  `conflict_fields` from the configured key. Decodes the returned record.
   *  `expected` makes it a compare-and-swap; {@link upsertResult} also reports
   *  whether the broker replayed an earlier write. */
  async upsert(
    record: Partial<T> & Record<string, unknown>,
    opts?: {
      returnRecord?: boolean;
      idempotencyKey?: string;
      expected?: Record<string, unknown>;
      call?: CallOptions;
    },
  ): Promise<T | null> {
    const resp = await this.sendUpsert(record, opts?.returnRecord ?? true, opts);
    return decodeRecord<T>(resp?.record_json);
  }

  /** Upsert one record and return the whole outcome. The row is decoded from this
   *  same response and only when `returnRecord` is set; no second read is issued.
   *  Each `expected` field must equal the current row or the broker refuses with
   *  FAILED_PRECONDITION and writes nothing; an empty map stays unconditional. */
  async upsertResult(
    record: Partial<T> & Record<string, unknown>,
    opts?: {
      returnRecord?: boolean;
      idempotencyKey?: string;
      expected?: Record<string, unknown>;
      call?: CallOptions;
    },
  ): Promise<UpsertResult<T>> {
    const resp = await this.sendUpsert(record, opts?.returnRecord ?? false, opts);
    return {
      response: resp,
      record: opts?.returnRecord ? decodeRecord<T>(resp?.record_json) : null,
      wasDuplicate: resp?.was_duplicate === true,
    };
  }

  private async sendUpsert(
    record: Record<string, unknown>,
    returnRecord: boolean,
    opts?: { idempotencyKey?: string; expected?: Record<string, unknown>; call?: CallOptions },
  ): Promise<any> {
    refuseBlankKey(opts?.idempotencyKey);
    const request: Record<string, unknown> = {
      context: this.context,
      message_type: this.messageType,
      record_json: jsonBytes(record),
      conflict_fields: this.opts.key,
      return_record: returnRecord,
    };
    if (opts?.idempotencyKey) request.idempotency_key = opts.idempotencyKey;
    if (opts?.expected && Object.keys(opts.expected).length > 0) request.expected = opts.expected;
    return this.mutate("Upsert", request, opts?.call);
  }

  /** Select records matching a plain-object filter. Emits exactly ONE `Select`
   *  with a plain-object `filter` (wkt.ts converts it to Struct on the wire).
   *  Decodes `records_json` to `T[]`.
   *
   *  Two accepted call shapes (both emit the SAME single `Select`):
   *   - legacy: `select(where, { limit, call })`
   *   - contract: `select({ where, limit, call })` (the `simple_client_code.md`
   *     headline form `table(name).select({ where })`).
   */
  async select(
    whereOrOpts:
      | Record<string, unknown>
      | { where?: Record<string, unknown>; limit?: number; call?: CallOptions } = {},
    opts?: { limit?: number; call?: CallOptions },
  ): Promise<T[]> {
    // Disambiguate the contract object form `{ where, limit?, call? }` from a
    // plain filter: only when `opts` is omitted AND the object carries `where`.
    const isContractForm =
      opts === undefined &&
      whereOrOpts != null &&
      typeof whereOrOpts === "object" &&
      "where" in whereOrOpts &&
      !Array.isArray(whereOrOpts);
    const where: Record<string, unknown> = isContractForm
      ? ((whereOrOpts as { where?: Record<string, unknown> }).where ?? {})
      : (whereOrOpts as Record<string, unknown>);
    const effectiveOpts = isContractForm
      ? (whereOrOpts as { limit?: number; call?: CallOptions })
      : opts;
    const request: Record<string, unknown> = {
      context: this.readContext(),
      message_type: this.messageType,
      filter: where,
    };
    if (effectiveOpts?.limit != null) request.limit = effectiveOpts.limit;
    const resp: any = await this.core.unary(DATABROKER, "Select", request, effectiveOpts?.call);
    if (Array.isArray(resp?.records) && !Array.isArray(resp?.records_json)) {
      return resp.records.map((r: unknown, index: number) => decodeRow<T>(r, index));
    }
    return decodeRecordSet<T>(resp);
  }

  /** One page of records, with the column selection, the continuation token
   *  and the redacted columns the broker reports. Emits exactly ONE `Select`. */
  async selectPage(
    opts: {
      where?: Record<string, unknown>;
      limit?: number;
      pageToken?: string;
      fields?: readonly string[];
      /** Sort columns; `-column` for descending. */
      orderBy?: readonly string[];
      includeTotal?: boolean;
      call?: CallOptions;
    } = {},
  ): Promise<EntityPage<T>> {
    const request: Record<string, unknown> = {
      context: this.readContext(),
      message_type: this.messageType,
      filter: opts.where ?? {},
    };
    if (opts.limit != null) request.limit = opts.limit;
    if (opts.pageToken) request.page_token = opts.pageToken;
    if (opts.fields?.length) request.fields = [...opts.fields];
    if (opts.includeTotal) request.include_total = true;
    if (opts.orderBy?.length) {
      request.sort = opts.orderBy.map((column) => ({
        field: column.startsWith("-") ? column.slice(1) : column,
        descending: column.startsWith("-"),
      }));
    }
    const resp: any = await this.core.unary(DATABROKER, "Select", request, opts.call);
    return {
      rows: decodeRecordSet<T>(resp),
      nextPageToken: resp?.next_page_token ?? "",
      hasMore: Boolean(resp?.has_more),
      total: Number(resp?.exact_total ?? 0),
      redactedFields: Array.isArray(resp?.redacted_fields) ? resp.redacted_fields : [],
    };
  }

  /** Write one record and return the broker's mutation response. `Insert` never
   *  replaces an existing row; `Upsert` conflicts on the configured key. */
  async write(
    rpc: "Upsert" | "Insert",
    record: Record<string, unknown>,
    opts?: { idempotencyKey?: string; call?: CallOptions },
  ): Promise<any> {
    const request: Record<string, unknown> = {
      context: this.context,
      message_type: this.messageType,
      record_json: jsonBytes(record),
      conflict_fields: this.opts.key,
    };
    if (opts?.idempotencyKey) request.idempotency_key = opts.idempotencyKey;
    return this.mutate(rpc, request, opts?.call);
  }

  /** Set `changes` on the records matching a plain-object filter; a null value
   *  writes SQL NULL. `expected` makes it a compare-and-swap, `increments` adds to
   *  numeric columns atomically, and `requireAffected` fails the call unless at
   *  least that many rows matched. An `expected` that is present but empty is
   *  refused before the RPC. Emits exactly ONE `Update`. */
  async update(
    where: Record<string, unknown>,
    changes: Record<string, unknown>,
    opts?: {
      idempotencyKey?: string;
      expected?: Record<string, unknown>;
      increments?: readonly { column: string; delta: number }[];
      requireAffected?: number;
      /** Also return the post-update row in `record_json`. */
      returnRecord?: boolean;
      call?: CallOptions;
    },
  ): Promise<any> {
    refuseEmptyExpected(opts?.expected);
    const request: Record<string, unknown> = {
      context: this.context,
      message_type: this.messageType,
      filter: where,
    };
    if (Object.keys(changes).length > 0) request.changes = changes;
    if (opts?.expected && Object.keys(opts.expected).length > 0) request.expected = opts.expected;
    if (opts?.increments?.length) request.increments = [...opts.increments];
    if (opts?.requireAffected) request.require_affected = opts.requireAffected;
    if (opts?.idempotencyKey) request.idempotency_key = opts.idempotencyKey;
    if (opts?.returnRecord) request.return_record = true;
    return this.mutate("Update", request, opts?.call);
  }

  /** Apply atomic counter deltas (`column = column + delta`) to the matched rows in
   *  one statement, with no read-modify-write window. Columns go on the wire in
   *  sorted order. Emits exactly ONE `Update`. */
  increment(
    where: Record<string, unknown>,
    deltas: Readonly<Record<string, number>>,
    opts?: {
      idempotencyKey?: string;
      expected?: Record<string, unknown>;
      requireAffected?: number;
      returnRecord?: boolean;
      call?: CallOptions;
    },
  ): Promise<any> {
    const increments = Object.keys(deltas)
      .sort()
      .map((column) => ({ column, delta: deltas[column] }));
    return this.update(where, {}, { ...opts, increments });
  }

  /** Delete records matching a plain-object filter. An `expected` that is present
   *  but empty is refused before the RPC. Emits exactly ONE `Delete`. */
  async delete(
    where: Record<string, unknown>,
    opts?: {
      idempotencyKey?: string;
      expected?: Record<string, unknown>;
      requireAffected?: number;
      call?: CallOptions;
    },
  ): Promise<any> {
    refuseEmptyExpected(opts?.expected);
    refuseBlankKey(opts?.idempotencyKey);
    const request: Record<string, unknown> = {
      context: this.context,
      message_type: this.messageType,
      filter: where,
    };
    if (opts?.expected && Object.keys(opts.expected).length > 0) request.expected = opts.expected;
    if (opts?.requireAffected) request.require_affected = opts.requireAffected;
    if (opts?.idempotencyKey) request.idempotency_key = opts.idempotencyKey;
    return this.mutate("Delete", request, opts?.call);
  }
}

// ── Typed tables (used by the `udb sdk generate --project-proto` output) ──────

/** A udb filter: `{ column: value }` for equality or `{ column: { $op: value } }`
 *  with $eq $ne $gt $gte $lt $lte $in $nin $between $like $ilike $is_null $not_null
 *  $not; combine with $and / $or. Columns are row properties or proto field names. */
export type Filter = Record<string, unknown>;

/** The addressed row does not exist, or the caller cannot see it. */
export class UdbNotFoundError extends Error {
  constructor(message: string, readonly cause?: unknown) {
    super(message);
    this.name = "UdbNotFoundError";
  }
}

/** A conditional write lost: the row changed since it was read. Read it again
 *  and retry, which {@link TypedTable.updateWithRetry} does. */
export class UdbConflictError extends Error {
  constructor(message: string, readonly cause?: unknown) {
    super(message);
    this.name = "UdbConflictError";
  }
}

/** Map a broker status to the errors callers test with `instanceof`. */
function classifyTableError(err: unknown): unknown {
  const failure = err as { code?: number; detail?: { reason?: string }; message?: string };
  const reason = failure?.detail?.reason ?? "";
  if (
    reason === "UDB_CAS_CONFLICT" ||
    reason === "UDB_REVISION_CONFLICT" ||
    reason === "UDB_CAS_ROW_MISSING"
  ) {
    return new UdbConflictError(`udb: conflict: the row changed since it was read: ${failure.message}`, err);
  }
  if (reason === "UDB_NO_ROWS_AFFECTED" || failure?.code === 5 /* NOT_FOUND */) {
    return new UdbNotFoundError(`udb: not found: ${failure.message}`, err);
  }
  return err;
}

/** One page of rows from {@link EntityHandle.selectPage} or {@link TypedTable.select}. */
export interface EntityPage<T> {
  rows: T[];
  /** Pass as `pageToken` to continue; empty on the last page. */
  nextPageToken: string;
  /** True when the page is full: more rows may match. */
  hasMore: boolean;
  /** Every matching row, when `includeTotal` was set. */
  total: number;
  /** Columns that came back as the redaction placeholder because the caller
   *  lacks the PII read scope. Never write those values back. */
  redactedFields: string[];
}

/** What a generated table needs from a client: `UdbProject` provides it. */
export interface EntityHost {
  entity<T = any>(messageType: string, opts?: Partial<EntityHandleOptions>): EntityHandle<T>;
}

/** The column policy the generator emits for each column of an entity. */
export interface UDBColumn {
  sqlType: string;
  notNull: boolean;
  hasPresence: boolean;
  isArray: boolean;
  json: boolean;
  jsonb: boolean;
  excludeFromInsert: boolean;
  pii: boolean;
  encrypted: boolean;
  /** The filterable HMAC sibling column; empty when there is none. */
  blindIndex: string;
  enumTokens: readonly string[];
  /** False for a broker-injected audit column, which the broker fills in. */
  declaredInProto: boolean;
}

/** The per-entity facts the generator emits for a {@link TypedTable}. */
export interface TypedTableSpec<Row> {
  messageType: string;
  /** Primary-key proto field names. */
  key: readonly string[];
  /** Columns a whole-row update never writes, besides the key: the tenant, project,
   *  generated and exclude_from_update columns. */
  updateExcluded?: readonly string[];
  /** Row property to proto field name, for every readable column. */
  fields: Readonly<Record<string, string>>;
  /** The writable columns of `row`, as record keys and stored values. */
  toRecord(row: Partial<Row>): Record<string, unknown>;
  /** Every column of `row`, as used in a filter or a patch. */
  toFields(row: Partial<Row>): Record<string, unknown>;
  fromRow(row: Record<string, unknown>): Row;
}

/** Read options of a {@link TypedTable}. */
export interface TypedReadOptions {
  /** Also read PII columns: needs `udb:pii:read`, or they come back redacted
   *  and are listed in `redactedFields`. Default: every non-PII column. */
  includePii?: boolean;
  call?: CallOptions;
}

/** Options of {@link TypedTable.select}. */
export interface TypedSelectOptions extends TypedReadOptions {
  where?: Filter;
  limit?: number;
  pageToken?: string;
  /** Sort by these columns (`-column` for descending); the primary key always
   *  breaks ties, so the order is total. */
  orderBy?: readonly string[];
  /** Also count every matching row into `total`. */
  includeTotal?: boolean;
  /** Read only these columns, so rows are partial. */
  fields?: readonly string[];
}

/** Options of a write on a {@link TypedTable}. */
export interface TypedWriteOptions {
  idempotencyKey?: string;
  call?: CallOptions;
}

/**
 * A typed table over one entity: rows use the generated interface, and values
 * cross the generated mappers. A NULL column is absent from a returned row.
 * Clear a column with `patch(key, { column: null })`. `upsert` writes the columns
 * present in its row, so an absent property leaves a column unchanged.
 */
export class TypedTable<Row extends object, Key extends object> {
  private readonly handle: EntityHandle<Record<string, unknown>>;

  constructor(
    host: EntityHost,
    private readonly spec: TypedTableSpec<Row>,
  ) {
    this.handle = host.entity<Record<string, unknown>>(spec.messageType, { key: [...spec.key] });
  }

  /** The entity's udb message type (its proto full name). */
  get messageType(): string {
    return this.spec.messageType;
  }

  /** The primary key of `row`. Throws when a primary-key column has no value. */
  keyOf(row: Partial<Row>): Key {
    const properties = Object.entries(this.spec.fields);
    const key: Record<string, unknown> = {};
    for (const field of this.spec.key) {
      const property = properties.find(([, name]) => name === field)?.[0] ?? field;
      const value = (row as Record<string, unknown>)[property];
      if (value === undefined || value === null || value === "") {
        throw new Error(`udb: ${this.spec.messageType} has no value for primary-key column ${field}`);
      }
      key[property] = value;
    }
    if (Object.keys(key).length === 0) {
      throw new Error(`udb: ${this.spec.messageType} declares no primary key`);
    }
    return key as unknown as Key;
  }

  /** The row with `key`: its primary key or a declared unique key. A missing row
   *  throws {@link UdbNotFoundError}. */
  async get(key: Key, opts: TypedReadOptions = {}): Promise<Row> {
    const page = await this.select({ ...opts, where: key as unknown as Filter, limit: 2 });
    if (page.rows.length === 0) {
      throw new UdbNotFoundError(`udb: not found: ${this.spec.messageType} ${JSON.stringify(key)}`);
    }
    if (page.rows.length > 1) {
      throw new Error(
        `udb: ${this.spec.messageType} key ${JSON.stringify(key)} matched more than one row; address a primary or unique key`,
      );
    }
    return page.rows[0];
  }

  /** One page of rows matching `where`. */
  async select(opts: TypedSelectOptions = {}): Promise<EntityPage<Row>> {
    try {
      const page = await this.handle.selectPage({
        where: opts.where ? this.fieldsOf(opts.where) : {},
        limit: opts.limit,
        pageToken: opts.pageToken,
        orderBy: opts.orderBy?.map((column) =>
          column.startsWith("-") ? `-${this.fieldOf(column.slice(1))}` : this.fieldOf(column),
        ),
        includeTotal: opts.includeTotal,
        fields: opts.fields
          ? opts.fields.map((name) => this.fieldOf(name))
          : opts.includePii
            ? Object.values(this.spec.fields)
            : undefined,
        call: opts.call,
      });
      return { ...page, rows: page.rows.map((row) => this.spec.fromRow(row)) };
    } catch (err) {
      throw classifyTableError(err);
    }
  }

  /** Walk every row matching `where`, page by page, calling `fn` for each. A
   *  throw from `fn` stops the walk and propagates. */
  async selectAll(
    where: Filter,
    fn: (row: Row) => void | Promise<void>,
    opts: TypedReadOptions = {},
  ): Promise<void> {
    let pageToken: string | undefined;
    for (;;) {
      const page = await this.select({ ...opts, where, limit: 500, pageToken });
      for (const row of page.rows) await fn(row);
      if (!page.nextPageToken || !page.hasMore) return;
      pageToken = page.nextPageToken;
    }
  }

  /** The number of rows matching `where`. */
  async count(where: Filter = {}, opts: { call?: CallOptions } = {}): Promise<number> {
    const page = await this.select({ where, limit: 1, includeTotal: true, call: opts.call });
    return page.total;
  }

  /** Write `row`: inserted when its key is new, updated when it exists. A property
   *  set to null writes SQL NULL. */
  async upsert(row: Key & Partial<Row>, opts: TypedWriteOptions = {}): Promise<void> {
    await this.write("Upsert", row, opts);
  }

  /** Create `row`. A primary or unique-key collision throws and never replaces
   *  the existing row. */
  async insert(row: Partial<Row>, opts: TypedWriteOptions = {}): Promise<void> {
    await this.write("Insert", row, opts);
  }

  /** Set only the given columns on the row with `key`; null clears a column. A
   *  missing row throws {@link UdbNotFoundError} and nothing is written. */
  async patch(key: Key, changes: Partial<Row>, opts: TypedWriteOptions = {}): Promise<void> {
    const fields = this.fieldsOf(changes as Filter);
    if (Object.keys(fields).length === 0) {
      throw new Error(`udb: patch on ${this.spec.messageType} has no columns to change`);
    }
    await this.update(key, fields, undefined, undefined, opts);
  }

  /** Write the updatable columns of `next` only if the stored row still holds
   *  `expected`. A lost race throws {@link UdbConflictError}. */
  async updateIf(
    next: Partial<Row>,
    expected: Partial<Row>,
    opts: TypedWriteOptions = {},
  ): Promise<void> {
    await this.update(
      this.keyOf(next),
      this.withoutKey(this.fieldsOf(next as Filter)),
      this.fieldsOf(expected as Filter),
      undefined,
      opts,
    );
  }

  /** Read the row, apply `change`, and write back only the columns it modified,
   *  guarded by their previous values. A conflict reads and tries again, up to
   *  `attempts` times. Returns the row as written. */
  async updateWithRetry(
    key: Key,
    change: (current: Row) => Row | Promise<Row>,
    attempts = 5,
    opts: TypedWriteOptions = {},
  ): Promise<Row> {
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const current = await this.get(key, { call: opts.call });
      const before = this.withoutKey(this.spec.toFields(current));
      const next = await change(structuredClone(current));
      const after = this.withoutKey(this.spec.toFields(next));
      const changes: Record<string, unknown> = {};
      const expected: Record<string, unknown> = {};
      for (const column of new Set([...Object.keys(before), ...Object.keys(after)])) {
        if (JSON.stringify(before[column]) !== JSON.stringify(after[column])) {
          changes[column] = after[column] ?? null;
          expected[column] = before[column] ?? null;
        }
      }
      if (Object.keys(changes).length === 0) return current;
      try {
        await this.update(key, changes, expected, undefined, opts);
        return next;
      } catch (err) {
        if (!(err instanceof UdbConflictError)) throw err;
      }
    }
    throw new UdbConflictError(`udb: conflict: gave up after ${attempts} attempts`);
  }

  /** Add `deltas` to numeric columns of the row with `key`, atomically on the
   *  broker, with no read-modify-write window. */
  async increment(
    key: Key,
    deltas: Readonly<Record<string, number>>,
    opts: TypedWriteOptions = {},
  ): Promise<void> {
    await this.update(key, undefined, undefined, deltas, opts);
  }

  /** Remove the row with `key`. A missing row throws {@link UdbNotFoundError}. */
  async delete(key: Key, opts: TypedWriteOptions = {}): Promise<void> {
    try {
      await this.handle.delete(this.fieldsOf(key as unknown as Filter), {
        ...opts,
        requireAffected: 1,
      });
    } catch (err) {
      throw classifyTableError(err);
    }
  }

  /** @internal The filter-first compare-and-swap behind {@link TypedRepo}. */
  async guardedUpdate(
    where: Filter,
    changes: Partial<Row>,
    expected: Partial<Row>,
    opts: TypedWriteOptions,
  ): Promise<void> {
    try {
      await this.handle.update(this.fieldsOf(where), this.fieldsOf(changes as Filter), {
        ...opts,
        expected: this.fieldsOf(expected as Filter),
      });
    } catch (err) {
      throw classifyTableError(err);
    }
  }

  /** @internal The filter-first compare-and-swap behind {@link TypedRepo}. */
  async guardedDelete(
    where: Filter,
    expected: Partial<Row>,
    opts: TypedWriteOptions,
  ): Promise<void> {
    try {
      await this.handle.delete(this.fieldsOf(where), {
        ...opts,
        expected: this.fieldsOf(expected as Filter),
      });
    } catch (err) {
      throw classifyTableError(err);
    }
  }

  private async write(
    rpc: "Upsert" | "Insert",
    row: Partial<Row>,
    opts: TypedWriteOptions,
  ): Promise<void> {
    this.fieldsOf(row as Filter); // An unknown property is refused, not dropped.
    let response: any;
    try {
      response = await this.handle.write(rpc, this.spec.toRecord(row), opts);
    } catch (err) {
      throw classifyTableError(err);
    }
    if (Number(response?.affected_rows ?? 0) === 0) {
      throw new UdbNotFoundError(`udb: not found: ${this.spec.messageType} was left untouched`);
    }
  }

  private async update(
    key: Key,
    changes: Record<string, unknown> | undefined,
    expected: Record<string, unknown> | undefined,
    deltas: Readonly<Record<string, number>> | undefined,
    opts: TypedWriteOptions,
  ): Promise<void> {
    const increments = Object.entries(deltas ?? {}).map(([name, delta]) => ({
      column: this.fieldOf(name),
      delta,
    }));
    try {
      await this.handle.update(this.fieldsOf(key as unknown as Filter), changes ?? {}, {
        ...opts,
        expected,
        increments,
        requireAffected: 1,
      });
    } catch (err) {
      throw classifyTableError(err);
    }
  }

  private withoutKey(fields: Record<string, unknown>): Record<string, unknown> {
    const rest = { ...fields };
    for (const column of this.spec.key) delete rest[column];
    for (const column of this.spec.updateExcluded ?? ["tenant_id", "project_id"]) {
      delete rest[column];
    }
    return rest;
  }

  private fieldOf(name: string): string {
    const field = this.spec.fields[name];
    if (field !== undefined) return field;
    if (Object.values(this.spec.fields).includes(name)) return name;
    throw new Error(`udb: ${this.spec.messageType} has no column '${name}'`);
  }

  /** Map row properties to field names. A property the entity does not have is
   *  refused: dropping it would widen a filter to rows the caller did not ask for. */
  private fieldsOf(values: Filter): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    for (const [name, value] of Object.entries(values)) {
      if (value === undefined) continue;
      if (name === "$and" || name === "$or") {
        out[name] = (value as Filter[]).map((part) => this.fieldsOf(part));
      } else if (name === "$not") {
        out[name] = this.fieldsOf(value as Filter);
      } else {
        const field = this.fieldOf(name);
        const mapped = this.spec.toFields({ [name]: value } as Partial<Row>);
        out[field] = field in mapped ? mapped[field] : value;
      }
    }
    return out;
  }
}

/**
 * Filter-first access to one entity: paged lists, a point read that returns null
 * when absent, and guarded writes that refuse to run without a precondition.
 */
export class TypedRepo<Row extends object> {
  private readonly table: TypedTable<Row, object>;

  constructor(
    host: EntityHost,
    private readonly spec: TypedTableSpec<Row>,
  ) {
    this.table = new TypedTable<Row, object>(host, spec);
  }

  /** One page of rows matching `where`. A row that fails to decode fails the page. */
  list(where: Filter = {}, opts: Omit<TypedSelectOptions, "where"> = {}): Promise<EntityPage<Row>> {
    return this.table.select({ ...opts, where });
  }

  /** The row identified by its FULL primary key, or null when absent. A partial
   *  key is refused before the RPC: use {@link findFirst} for other lookups. */
  async get(where: Filter, opts: TypedReadOptions = {}): Promise<Row | null> {
    for (const field of this.spec.key) {
      const property = Object.entries(this.spec.fields).find(([, name]) => name === field)?.[0];
      if (!(field in where) && !(property !== undefined && property in where)) {
        throw new Error(
          `${this.spec.messageType}: primary key "${field}" missing from filter; use findFirst for a partial-key lookup`,
        );
      }
    }
    const page = await this.table.select({ ...opts, where, limit: 2 });
    if (page.rows.length > 1) {
      throw new Error(`${this.spec.messageType}: primary key matched ${page.rows.length} rows, expected at most 1`);
    }
    return page.rows[0] ?? null;
  }

  /** The first row matching an arbitrary filter, or null when none match. */
  async findFirst(where: Filter, opts: TypedReadOptions = {}): Promise<Row | null> {
    const page = await this.table.select({ ...opts, where, limit: 1 });
    return page.rows[0] ?? null;
  }

  /** A compare-and-swap update: set `changes` on the rows matching `where`, only
   *  if `expected` still equals the current row. An empty `expected` is refused,
   *  so a guarded update never degrades into an unconditional one. */
  async updateGuarded(
    where: Filter,
    changes: Partial<Row>,
    expected: Partial<Row>,
    opts: TypedWriteOptions = {},
  ): Promise<void> {
    if (Object.keys(expected).length === 0) {
      throw new Error(
        `${this.spec.messageType}.updateGuarded: expected precondition is empty; refusing to run an unconditional update`,
      );
    }
    await this.table.guardedUpdate(where, changes, expected, opts);
  }

  /** Delete the matched row only if `expected` still equals it. An empty
   *  `expected` is refused before the RPC. */
  async deleteGuarded(
    where: Filter,
    expected: Partial<Row>,
    opts: TypedWriteOptions = {},
  ): Promise<void> {
    if (Object.keys(expected).length === 0) {
      throw new Error(
        `${this.spec.messageType}.deleteGuarded: expected precondition is empty; refusing to run an unconditional delete`,
      );
    }
    await this.table.guardedDelete(where, expected, opts);
  }
}

/** A typed predicate on one column. Each method returns a {@link Filter}. */
export class UdbColumn<T> {
  constructor(
    readonly field: string,
    private readonly enumPrefix: string = "",
  ) {}

  private value(v: T): unknown {
    if (v === null || v === undefined) {
      throw new Error(`udb: predicate ${this.field}: value is NULL or omitted; use isNull`);
    }
    if (typeof v === "number" && Number.isInteger(v) && !Number.isSafeInteger(v)) {
      throw new Error(
        `udb: predicate ${this.field}: integer exceeds the exact filter number range; pass it as a string`,
      );
    }
    return this.enumPrefix ? udbEnumToText(v, this.enumPrefix) : v;
  }

  private compare(op: string, v: T): Filter {
    return { [this.field]: { [op]: this.value(v) } };
  }

  eq(v: T): Filter { return this.compare("$eq", v); }
  gt(v: T): Filter { return this.compare("$gt", v); }
  gte(v: T): Filter { return this.compare("$gte", v); }
  lt(v: T): Filter { return this.compare("$lt", v); }
  lte(v: T): Filter { return this.compare("$lte", v); }
  in(...values: T[]): Filter { return { [this.field]: { $in: values.map((v) => this.value(v)) } }; }
  notIn(...values: T[]): Filter { return { [this.field]: { $nin: values.map((v) => this.value(v)) } }; }
  between(low: T, high: T): Filter {
    return { [this.field]: { $between: [this.value(low), this.value(high)] } };
  }
  isNull(): Filter { return { [this.field]: { $is_null: true } }; }
  notNull(): Filter { return { [this.field]: { $not_null: true } }; }
}

/** Stored text of an enum value: the value name without the common prefix. */
export function udbEnumToText(value: unknown, prefix: string): unknown {
  return typeof value === "string" && value.startsWith(prefix) ? value.slice(prefix.length) : value;
}

/** The enum value name for stored text. */
export function udbEnumFromText(value: unknown, prefix: string): unknown {
  return typeof value === "string" && !value.startsWith(prefix) ? prefix + value : value;
}

/** A 64-bit integer as read: a number when exact, else its decimal string. */
export function udbBigInt(value: unknown): number | string {
  if (typeof value === "number") return value;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) ? parsed : String(value);
}
