"use strict";
// Bound entity / table helper: hides record_json packing, conflict_fields, the
// plain-object filter, and record_json/records_json decode behind a typed handle
// over the DataBroker Upsert/Select/Delete RPCs.
//
// The broker remains the sole RLS / authority enforcement point — this helper
// never sets authoritative principal/actor/scopes; it only shapes the request
// body the caller already controls. The key (+ optional tenant/project field
// names) is supplied by the caller (or, once lane 07 ships {{ENTITY_*}}, read
// from the generated ENTITY_REGISTRY by `UdbProject.entity`).
Object.defineProperty(exports, "__esModule", { value: true });
exports.UdbColumn = exports.TypedRepo = exports.TypedTable = exports.UdbConflictError = exports.UdbNotFoundError = exports.EntityHandle = void 0;
exports.parseRecordJson = parseRecordJson;
exports.udbEnumToText = udbEnumToText;
exports.udbEnumFromText = udbEnumFromText;
exports.udbBigInt = udbBigInt;
const DATABROKER = "udb.services.v1.DataBroker";
/** Encode a JS record as `record_json` bytes the broker expects. */
function jsonBytes(record) {
    return Buffer.from(JSON.stringify(record), "utf8");
}
/** A guarded write with an empty precondition would apply to every matched row. */
const EMPTY_CAS = "udb: guarded write requires a non-empty expected precondition; refusing to run unconditionally";
function refuseEmptyExpected(expected) {
    if (expected !== undefined && Object.keys(expected).length === 0)
        throw new Error(EMPTY_CAS);
}
/** A blank key would silently disable replay safety while looking set. */
function refuseBlankKey(key) {
    if (key && key.trim() === "")
        throw new Error("udb: idempotency key must not be whitespace-only");
}
/** Decode one row of a record set. A row that does not decode throws: skipping it
 *  would leave the page short while hasMore and the total describe the full result. */
function decodeRow(raw, index) {
    const row = decodeRecord(raw);
    if (row == null) {
        const bytes = raw instanceof Uint8Array ? raw.length : String(raw ?? "").length;
        throw new Error(`udb: decode record row ${index}: ${bytes === 0 ? "empty body" : "not JSON"}`);
    }
    return row;
}
/** Decode a record set. `rows` is a compatibility field the broker sends empty, one
 *  entry per record, so rows without records_json are missing records, not no match. */
function decodeRecordSet(resp) {
    const records = Array.isArray(resp?.records_json) ? resp.records_json : [];
    if (records.length === 0 && Array.isArray(resp?.rows) && resp.rows.length > 0) {
        throw new Error(`udb: record set carries ${resp.rows.length} compatibility row(s) but no records_json`);
    }
    return records.map((raw, index) => decodeRow(raw, index));
}
/**
 * Parse a broker record without losing 64-bit integers. `JSON.parse` rounds an
 * integer past `Number.MAX_SAFE_INTEGER`, so a column value like that (directly on
 * the record, or in an array column) becomes its decimal string. Numbers inside a
 * JSON document column keep `JSON.parse` behaviour, so the document's shape is kept.
 */
function parseRecordJson(text) {
    let out = "";
    let copied = 0;
    let objects = 0;
    for (let i = 0; i < text.length; i += 1) {
        const ch = text[i];
        if (ch === '"') {
            for (i += 1; i < text.length && text[i] !== '"'; i += 1) {
                if (text[i] === "\\")
                    i += 1;
            }
        }
        else if (ch === "{") {
            objects += 1;
        }
        else if (ch === "}") {
            objects -= 1;
        }
        else if (ch === "-" || (ch >= "0" && ch <= "9")) {
            let end = i + 1;
            while (end < text.length && /[0-9.eE+-]/.test(text[end]))
                end += 1;
            const token = text.slice(i, end);
            if (objects <= 1 && /^-?\d+$/.test(token) && !Number.isSafeInteger(Number(token))) {
                out += `${text.slice(copied, i)}"${token}"`;
                copied = end;
            }
            i = end - 1;
        }
    }
    return JSON.parse(copied === 0 ? text : out + text.slice(copied));
}
/** Decode `record_json` (single) or `records_json` (repeated) bytes/strings back
 *  to JS objects. The keepCase loader surfaces `bytes` as Buffer; strings pass
 *  through JSON.parse. */
function decodeRecord(raw) {
    if (raw == null)
        return null;
    let text;
    if (typeof raw === "string")
        text = raw;
    else if (Buffer.isBuffer(raw))
        text = raw.toString("utf8");
    else if (raw instanceof Uint8Array)
        text = Buffer.from(raw).toString("utf8");
    else
        return raw; // already an object
    if (!text)
        return null;
    try {
        return parseRecordJson(text);
    }
    catch {
        return null;
    }
}
/**
 * A typed handle bound to one entity message type. Forwards to the DataBroker
 * Upsert / Select / Delete RPCs over the shared core, packing/decoding
 * record_json so callers work in plain JS records.
 */
class EntityHandle {
    core;
    messageType;
    opts;
    context;
    fence;
    /** `context` is merged into each request body, for example tenant_id/project_id.
     *  `fence` is the client's read-your-writes state: a write's receipt is kept
     *  there, and the reads right after it carry the matching read fence. */
    constructor(core, messageType, opts, context = {}, fence) {
        this.core = core;
        this.messageType = messageType;
        this.opts = opts;
        this.context = context;
        this.fence = fence;
    }
    /** The request context of a read: the bound context plus any read fence. */
    readContext() {
        return this.fence ? { ...this.context, ...this.fence.readContext() } : this.context;
    }
    async mutate(rpc, request, call) {
        const resp = await this.core.unary(DATABROKER, rpc, request, call);
        this.fence?.rememberWrite(resp);
        return resp;
    }
    /** Upsert one record. Emits exactly ONE `Upsert` with `record_json` bytes and
     *  `conflict_fields` from the configured key. Decodes the returned record.
     *  `expected` makes it a compare-and-swap; {@link upsertResult} also reports
     *  whether the broker replayed an earlier write. */
    async upsert(record, opts) {
        const resp = await this.sendUpsert(record, opts?.returnRecord ?? true, opts);
        return decodeRecord(resp?.record_json);
    }
    /** Upsert one record and return the whole outcome. The row is decoded from this
     *  same response and only when `returnRecord` is set; no second read is issued.
     *  Each `expected` field must equal the current row or the broker refuses with
     *  FAILED_PRECONDITION and writes nothing; an empty map stays unconditional. */
    async upsertResult(record, opts) {
        const resp = await this.sendUpsert(record, opts?.returnRecord ?? false, opts);
        return {
            response: resp,
            record: opts?.returnRecord ? decodeRecord(resp?.record_json) : null,
            wasDuplicate: resp?.was_duplicate === true,
        };
    }
    async sendUpsert(record, returnRecord, opts) {
        refuseBlankKey(opts?.idempotencyKey);
        const request = {
            context: this.context,
            message_type: this.messageType,
            record_json: jsonBytes(record),
            conflict_fields: this.opts.key,
            return_record: returnRecord,
        };
        if (opts?.idempotencyKey)
            request.idempotency_key = opts.idempotencyKey;
        if (opts?.expected && Object.keys(opts.expected).length > 0)
            request.expected = opts.expected;
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
    async select(whereOrOpts = {}, opts) {
        // Disambiguate the contract object form `{ where, limit?, call? }` from a
        // plain filter: only when `opts` is omitted AND the object carries `where`.
        const isContractForm = opts === undefined &&
            whereOrOpts != null &&
            typeof whereOrOpts === "object" &&
            "where" in whereOrOpts &&
            !Array.isArray(whereOrOpts);
        const where = isContractForm
            ? (whereOrOpts.where ?? {})
            : whereOrOpts;
        const effectiveOpts = isContractForm
            ? whereOrOpts
            : opts;
        const request = {
            context: this.readContext(),
            message_type: this.messageType,
            filter: where,
        };
        if (effectiveOpts?.limit != null)
            request.limit = effectiveOpts.limit;
        const resp = await this.core.unary(DATABROKER, "Select", request, effectiveOpts?.call);
        if (Array.isArray(resp?.records) && !Array.isArray(resp?.records_json)) {
            return resp.records.map((r, index) => decodeRow(r, index));
        }
        // A capped read must not pass for the whole answer. A caller who set a limit asked
        // for a capped read.
        if (resp?.has_more && effectiveOpts?.limit == null) {
            throw new Error(`udb: more ${this.messageType} rows match than one page; walk them with selectPage`);
        }
        return decodeRecordSet(resp);
    }
    /** One page of records, with the column selection, the continuation token
     *  and the redacted columns the broker reports. Emits exactly ONE `Select`. */
    async selectPage(opts = {}) {
        const request = {
            context: this.readContext(),
            message_type: this.messageType,
            filter: opts.where ?? {},
        };
        if (opts.limit != null)
            request.limit = opts.limit;
        if (opts.pageToken)
            request.page_token = opts.pageToken;
        if (opts.fields?.length)
            request.fields = [...opts.fields];
        if (opts.includeTotal)
            request.include_total = true;
        if (opts.orderBy?.length) {
            request.sort = opts.orderBy.map((column) => ({
                field: column.startsWith("-") ? column.slice(1) : column,
                descending: column.startsWith("-"),
            }));
        }
        const resp = await this.core.unary(DATABROKER, "Select", request, opts.call);
        return {
            rows: decodeRecordSet(resp),
            nextPageToken: resp?.next_page_token ?? "",
            hasMore: Boolean(resp?.has_more),
            total: Number(resp?.exact_total ?? 0),
            redactedFields: Array.isArray(resp?.redacted_fields) ? resp.redacted_fields : [],
        };
    }
    /** Write one record and return the broker's mutation response. `Insert` never
     *  replaces an existing row; `Upsert` conflicts on the configured key. */
    async write(rpc, record, opts) {
        const request = {
            context: this.context,
            message_type: this.messageType,
            record_json: jsonBytes(record),
        };
        if (opts?.idempotencyKey)
            request.idempotency_key = opts.idempotencyKey;
        return this.mutate(rpc, request, opts?.call);
    }
    /** Set `changes` on the records matching a plain-object filter; a null value
     *  writes SQL NULL. `expected` makes it a compare-and-swap, `increments` adds to
     *  numeric columns atomically, and `requireAffected` fails the call unless at
     *  least that many rows matched. An `expected` that is present but empty is
     *  refused before the RPC. Emits exactly ONE `Update`. */
    async update(where, changes, opts) {
        refuseEmptyExpected(opts?.expected);
        const request = {
            context: this.context,
            message_type: this.messageType,
            filter: where,
        };
        if (Object.keys(changes).length > 0)
            request.changes = changes;
        if (opts?.expected && Object.keys(opts.expected).length > 0)
            request.expected = opts.expected;
        if (opts?.increments?.length)
            request.increments = [...opts.increments];
        if (opts?.requireAffected)
            request.require_affected = opts.requireAffected;
        if (opts?.idempotencyKey)
            request.idempotency_key = opts.idempotencyKey;
        if (opts?.returnRecord)
            request.return_record = true;
        return this.mutate("Update", request, opts?.call);
    }
    /** Apply atomic counter deltas (`column = column + delta`) to the matched rows in
     *  one statement, with no read-modify-write window. Columns go on the wire in
     *  sorted order. Emits exactly ONE `Update`. */
    increment(where, deltas, opts) {
        const increments = Object.keys(deltas)
            .sort()
            .map((column) => ({ column, delta: deltas[column] }));
        return this.update(where, {}, { ...opts, increments });
    }
    /** Delete records matching a plain-object filter. An `expected` that is present
     *  but empty is refused before the RPC. Emits exactly ONE `Delete`. */
    async delete(where, opts) {
        refuseEmptyExpected(opts?.expected);
        refuseBlankKey(opts?.idempotencyKey);
        const request = {
            context: this.context,
            message_type: this.messageType,
            filter: where,
        };
        if (opts?.expected && Object.keys(opts.expected).length > 0)
            request.expected = opts.expected;
        if (opts?.requireAffected)
            request.require_affected = opts.requireAffected;
        if (opts?.idempotencyKey)
            request.idempotency_key = opts.idempotencyKey;
        return this.mutate("Delete", request, opts?.call);
    }
}
exports.EntityHandle = EntityHandle;
/** The addressed row does not exist, or the caller cannot see it. */
class UdbNotFoundError extends Error {
    cause;
    constructor(message, cause) {
        super(message);
        this.cause = cause;
        this.name = "UdbNotFoundError";
    }
}
exports.UdbNotFoundError = UdbNotFoundError;
/** A conditional write lost: the row changed since it was read. Read it again
 *  and retry, which {@link TypedTable.updateWithRetry} does. */
class UdbConflictError extends Error {
    cause;
    constructor(message, cause) {
        super(message);
        this.cause = cause;
        this.name = "UdbConflictError";
    }
}
exports.UdbConflictError = UdbConflictError;
/** Map a broker status to the errors callers test with `instanceof`. */
function classifyTableError(err) {
    const failure = err;
    const reason = failure?.detail?.reason ?? "";
    if (reason === "UDB_CAS_CONFLICT" ||
        reason === "UDB_REVISION_CONFLICT" ||
        reason === "UDB_CAS_ROW_MISSING") {
        return new UdbConflictError(`udb: conflict: the row changed since it was read: ${failure.message}`, err);
    }
    if (reason === "UDB_NO_ROWS_AFFECTED" || failure?.code === 5 /* NOT_FOUND */) {
        return new UdbNotFoundError(`udb: not found: ${failure.message}`, err);
    }
    return err;
}
/**
 * A typed table over one entity: rows use the generated interface, and values
 * cross the generated mappers. A NULL column is absent from a returned row.
 * Clear a column with `patch(key, { column: null })`. `upsert` writes the columns
 * present in its row, so an absent property leaves a column unchanged.
 */
class TypedTable {
    spec;
    handle;
    constructor(host, spec) {
        this.spec = spec;
        this.handle = host.entity(spec.messageType, { key: [...spec.key] });
    }
    /** The entity's udb message type (its proto full name). */
    get messageType() {
        return this.spec.messageType;
    }
    /** The primary key of `row`. Throws when a primary-key column has no value. */
    keyOf(row) {
        const properties = Object.entries(this.spec.fields);
        const key = {};
        for (const field of this.spec.key) {
            const property = properties.find(([, name]) => name === field)?.[0] ?? field;
            const value = row[property];
            if (value === undefined || value === null || value === "") {
                throw new Error(`udb: ${this.spec.messageType} has no value for primary-key column ${field}`);
            }
            key[property] = value;
        }
        if (Object.keys(key).length === 0) {
            throw new Error(`udb: ${this.spec.messageType} declares no primary key`);
        }
        return key;
    }
    /** The row with `key`: its primary key or a declared unique key. A missing row
     *  throws {@link UdbNotFoundError}. */
    async get(key, opts = {}) {
        const page = await this.select({ ...opts, where: key, limit: 2 });
        if (page.rows.length === 0) {
            throw new UdbNotFoundError(`udb: not found: ${this.spec.messageType} ${JSON.stringify(key)}`);
        }
        if (page.rows.length > 1) {
            throw new Error(`udb: ${this.spec.messageType} key ${JSON.stringify(key)} matched more than one row; address a primary or unique key`);
        }
        return page.rows[0];
    }
    /** One page of rows matching `where`. */
    async select(opts = {}) {
        try {
            const page = await this.handle.selectPage({
                where: opts.where ? this.fieldsOf(opts.where) : {},
                limit: opts.limit,
                pageToken: opts.pageToken,
                orderBy: opts.orderBy?.map((column) => column.startsWith("-") ? `-${this.fieldOf(column.slice(1))}` : this.fieldOf(column)),
                includeTotal: opts.includeTotal,
                fields: opts.fields
                    ? opts.fields.map((name) => this.fieldOf(name))
                    : opts.includePii
                        ? Object.values(this.spec.fields)
                        : undefined,
                call: opts.call,
            });
            return { ...page, rows: page.rows.map((row) => this.spec.fromRow(row)) };
        }
        catch (err) {
            throw classifyTableError(err);
        }
    }
    /** Walk every row matching `where`, page by page, calling `fn` for each. A
     *  throw from `fn` stops the walk and propagates. */
    async selectAll(where, fn, opts = {}) {
        let pageToken;
        for (;;) {
            const page = await this.select({ ...opts, where, limit: 500, pageToken });
            for (const row of page.rows)
                await fn(row);
            if (!page.nextPageToken || !page.hasMore)
                return;
            pageToken = page.nextPageToken;
        }
    }
    /** The number of rows matching `where`. */
    async count(where = {}, opts = {}) {
        const page = await this.select({ where, limit: 1, includeTotal: true, call: opts.call });
        return page.total;
    }
    /** Write `row`: inserted when its key is new, updated when it exists. A property
     *  set to null writes SQL NULL. */
    async upsert(row, opts = {}) {
        await this.write("Upsert", row, opts);
    }
    /** Create `row`. A primary or unique-key collision throws and never replaces
     *  the existing row. */
    async insert(row, opts = {}) {
        await this.write("Insert", row, opts);
    }
    /** Set only the given columns on the row with `key`; null clears a column. A
     *  missing row throws {@link UdbNotFoundError} and nothing is written. */
    async patch(key, changes, opts = {}) {
        const fields = this.fieldsOf(changes);
        if (Object.keys(fields).length === 0) {
            throw new Error(`udb: patch on ${this.spec.messageType} has no columns to change`);
        }
        await this.update(key, fields, undefined, undefined, opts);
    }
    /** Write the updatable columns of `next` only if the stored row still holds
     *  `expected`. A lost race throws {@link UdbConflictError}. */
    async updateIf(next, expected, opts = {}) {
        await this.update(this.keyOf(next), this.withoutKey(this.fieldsOf(next)), this.fieldsOf(expected), undefined, opts);
    }
    /** Read the row, apply `change`, and write back only the columns it modified,
     *  guarded by their previous values. A conflict reads and tries again, up to
     *  `attempts` times. Returns the row as written. */
    async updateWithRetry(key, change, attempts = 5, opts = {}) {
        const limit = attempts > 0 ? attempts : 5;
        for (let attempt = 0; attempt < limit; attempt += 1) {
            const current = await this.get(key, { call: opts.call });
            const before = this.withoutKey(this.spec.toFields(current));
            const next = await change(structuredClone(current));
            const after = this.withoutKey(this.spec.toFields(next));
            const changes = {};
            const expected = {};
            for (const column of new Set([...Object.keys(before), ...Object.keys(after)])) {
                if (JSON.stringify(before[column]) !== JSON.stringify(after[column])) {
                    changes[column] = after[column] ?? null;
                    expected[column] = before[column] ?? null;
                }
            }
            if (Object.keys(changes).length === 0)
                return current;
            try {
                await this.update(key, changes, expected, undefined, opts);
                return next;
            }
            catch (err) {
                if (!(err instanceof UdbConflictError))
                    throw err;
            }
        }
        throw new UdbConflictError(`udb: conflict: gave up after ${limit} attempts`);
    }
    /** Add `deltas` to numeric columns of the row with `key`, atomically on the
     *  broker, with no read-modify-write window. */
    async increment(key, deltas, opts = {}) {
        await this.update(key, undefined, undefined, deltas, opts);
    }
    /** Remove the row with `key`. A missing row throws {@link UdbNotFoundError}. */
    async delete(key, opts = {}) {
        try {
            await this.handle.delete(this.fieldsOf(key), {
                ...opts,
                requireAffected: 1,
            });
        }
        catch (err) {
            throw classifyTableError(err);
        }
    }
    /** @internal The filter-first compare-and-swap behind {@link TypedRepo}. */
    async guardedUpdate(where, changes, expected, opts) {
        try {
            await this.handle.update(this.fieldsOf(where), this.fieldsOf(changes), {
                ...opts,
                expected: this.fieldsOf(expected),
            });
        }
        catch (err) {
            throw classifyTableError(err);
        }
    }
    /** @internal The filter-first compare-and-swap behind {@link TypedRepo}. */
    async guardedDelete(where, expected, opts) {
        try {
            await this.handle.delete(this.fieldsOf(where), {
                ...opts,
                expected: this.fieldsOf(expected),
            });
        }
        catch (err) {
            throw classifyTableError(err);
        }
    }
    async write(rpc, row, opts) {
        this.fieldsOf(row); // An unknown property is refused, not dropped.
        let response;
        try {
            response = await this.handle.write(rpc, this.spec.toRecord(row), opts);
        }
        catch (err) {
            throw classifyTableError(err);
        }
        if (Number(response?.affected_rows ?? 0) === 0) {
            throw new UdbNotFoundError(`udb: not found: ${this.spec.messageType} was left untouched`);
        }
    }
    async update(key, changes, expected, deltas, opts) {
        const increments = Object.entries(deltas ?? {}).map(([name, delta]) => ({
            column: this.fieldOf(name),
            delta,
        }));
        try {
            await this.handle.update(this.fieldsOf(key), changes ?? {}, {
                ...opts,
                expected,
                increments,
                requireAffected: 1,
            });
        }
        catch (err) {
            throw classifyTableError(err);
        }
    }
    withoutKey(fields) {
        const rest = { ...fields };
        for (const column of this.spec.key)
            delete rest[column];
        for (const column of this.spec.updateExcluded ?? ["tenant_id", "project_id"]) {
            delete rest[column];
        }
        return rest;
    }
    fieldOf(name) {
        const field = this.spec.fields[name];
        if (field !== undefined)
            return field;
        if (Object.values(this.spec.fields).includes(name))
            return name;
        throw new Error(`udb: ${this.spec.messageType} has no column '${name}'`);
    }
    /** Map row properties to field names. A property the entity does not have is
     *  refused: dropping it would widen a filter to rows the caller did not ask for. */
    fieldsOf(values) {
        const out = {};
        for (const [name, value] of Object.entries(values)) {
            if (value === undefined)
                continue;
            if (name === "$and" || name === "$or") {
                out[name] = value.map((part) => this.fieldsOf(part));
            }
            else if (name === "$not") {
                out[name] = this.fieldsOf(value);
            }
            else {
                const field = this.fieldOf(name);
                const mapped = this.spec.toFields({ [name]: value });
                out[field] = field in mapped ? mapped[field] : value;
            }
        }
        return out;
    }
}
exports.TypedTable = TypedTable;
/**
 * Filter-first access to one entity: paged lists, a point read that returns null
 * when absent, and guarded writes that refuse to run without a precondition.
 */
class TypedRepo {
    spec;
    table;
    constructor(host, spec) {
        this.spec = spec;
        this.table = new TypedTable(host, spec);
    }
    /** One page of rows matching `where`. A row that fails to decode fails the page. */
    list(where = {}, opts = {}) {
        return this.table.select({ ...opts, where });
    }
    /** The row identified by its FULL primary key, or null when absent. A partial
     *  key is refused before the RPC: use {@link findFirst} for other lookups. */
    async get(where, opts = {}) {
        for (const field of this.spec.key) {
            const property = Object.entries(this.spec.fields).find(([, name]) => name === field)?.[0];
            if (!(field in where) && !(property !== undefined && property in where)) {
                throw new Error(`${this.spec.messageType}: primary key "${field}" missing from filter; use findFirst for a partial-key lookup`);
            }
        }
        const page = await this.table.select({ ...opts, where, limit: 2 });
        if (page.rows.length > 1) {
            throw new Error(`${this.spec.messageType}: primary key matched ${page.rows.length} rows, expected at most 1`);
        }
        return page.rows[0] ?? null;
    }
    /** The first row matching an arbitrary filter, or null when none match. */
    async findFirst(where, opts = {}) {
        const page = await this.table.select({ ...opts, where, limit: 1 });
        return page.rows[0] ?? null;
    }
    /** A compare-and-swap update: set `changes` on the rows matching `where`, only
     *  if `expected` still equals the current row. An empty `expected` is refused,
     *  so a guarded update never degrades into an unconditional one. */
    async updateGuarded(where, changes, expected, opts = {}) {
        if (Object.keys(expected).length === 0) {
            throw new Error(`${this.spec.messageType}.updateGuarded: expected precondition is empty; refusing to run an unconditional update`);
        }
        await this.table.guardedUpdate(where, changes, expected, opts);
    }
    /** Delete the matched row only if `expected` still equals it. An empty
     *  `expected` is refused before the RPC. */
    async deleteGuarded(where, expected, opts = {}) {
        if (Object.keys(expected).length === 0) {
            throw new Error(`${this.spec.messageType}.deleteGuarded: expected precondition is empty; refusing to run an unconditional delete`);
        }
        await this.table.guardedDelete(where, expected, opts);
    }
}
exports.TypedRepo = TypedRepo;
/** A typed predicate on one column. Each method returns a {@link Filter}. */
class UdbColumn {
    field;
    enumPrefix;
    constructor(field, enumPrefix = "") {
        this.field = field;
        this.enumPrefix = enumPrefix;
    }
    value(v) {
        if (v === null || v === undefined) {
            throw new Error(`udb: predicate ${this.field}: value is NULL or omitted; use isNull`);
        }
        if (typeof v === "number" && Number.isInteger(v) && !Number.isSafeInteger(v)) {
            throw new Error(`udb: predicate ${this.field}: integer exceeds the exact filter number range; pass it as a string`);
        }
        return this.enumPrefix ? udbEnumToText(v, this.enumPrefix) : v;
    }
    compare(op, v) {
        return { [this.field]: { [op]: this.value(v) } };
    }
    eq(v) { return this.compare("$eq", v); }
    gt(v) { return this.compare("$gt", v); }
    gte(v) { return this.compare("$gte", v); }
    lt(v) { return this.compare("$lt", v); }
    lte(v) { return this.compare("$lte", v); }
    in(...values) { return { [this.field]: { $in: values.map((v) => this.value(v)) } }; }
    notIn(...values) { return { [this.field]: { $nin: values.map((v) => this.value(v)) } }; }
    between(low, high) {
        return { [this.field]: { $between: [this.value(low), this.value(high)] } };
    }
    isNull() { return { [this.field]: { $is_null: true } }; }
    notNull() { return { [this.field]: { $not_null: true } }; }
}
exports.UdbColumn = UdbColumn;
/** Stored text of an enum value: the value name without the common prefix. */
function udbEnumToText(value, prefix) {
    return typeof value === "string" && value.startsWith(prefix) ? value.slice(prefix.length) : value;
}
/** The enum value name for stored text. */
function udbEnumFromText(value, prefix) {
    return typeof value === "string" && !value.startsWith(prefix) ? prefix + value : value;
}
/** A 64-bit integer as read: a number when exact, else its decimal string. */
function udbBigInt(value) {
    if (typeof value === "number")
        return value;
    const parsed = Number(value);
    return Number.isSafeInteger(parsed) ? parsed : String(value);
}
