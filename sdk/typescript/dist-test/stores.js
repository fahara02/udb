"use strict";
// Typed stores, transactions, durable consumers and error codes — the
// TypeScript port of the Go SDK's Table[T], Tx, Consume and ErrorCode (T1).
//
// Everything here goes through a `StorePlane` (anything with the generated
// `UdbCore.unary`/`serverStream`/`bidiStream` shape), so tests can swap in
// `InMemoryDataPlane` instead of a broker.
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
exports.InMemoryDataPlane = exports.TxScope = exports.EVENT_ENVELOPE_VERSION = exports.Table = exports.isUnique = exports.isConflict = exports.isNotFound = exports.codeOf = exports.ConflictError = exports.NotFoundError = exports.ErrorKind = exports.ErrorCode = void 0;
exports.inspect = inspect;
exports.missingScope = missingScope;
exports.isCasConflict = isCasConflict;
exports.tx = tx;
exports.decodeEnvelope = decodeEnvelope;
exports.consume = consume;
exports.crossTenantRead = crossTenantRead;
const node_crypto_1 = require("node:crypto");
const grpc = __importStar(require("@grpc/grpc-js"));
const consistency_1 = require("./consistency");
const entity_1 = require("./entity");
const generatedClient_1 = require("./generatedClient");
const protoSchema_1 = require("./protoSchema");
const recordCodec_1 = require("./recordCodec");
const DATABROKER = "udb.services.v1.DataBroker";
// ── Error codes ─────────────────────────────────────────────────────────────
/** A closed set of outcomes to branch on, instead of matching messages. */
var ErrorCode;
(function (ErrorCode) {
    ErrorCode["Unknown"] = "unknown";
    ErrorCode["NotFound"] = "not_found";
    ErrorCode["Conflict"] = "conflict";
    ErrorCode["Unique"] = "unique";
    ErrorCode["NotNull"] = "not_null";
    ErrorCode["ForeignKey"] = "foreign_key";
    ErrorCode["Invalid"] = "invalid";
    ErrorCode["PolicyDenied"] = "policy_denied";
    ErrorCode["ScopeMissing"] = "scope_missing";
    ErrorCode["Unauthenticated"] = "unauthenticated";
    ErrorCode["RateLimited"] = "rate_limited";
    ErrorCode["Unavailable"] = "unavailable";
    ErrorCode["Redacted"] = "redacted";
    ErrorCode["Precondition"] = "precondition";
    ErrorCode["Internal"] = "internal";
})(ErrorCode || (exports.ErrorCode = ErrorCode = {}));
const REASON_CODES = {
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
exports.ErrorKind = {
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
};
const KIND_CODES = {
    [exports.ErrorKind.CONFLICT]: ErrorCode.Conflict,
    [exports.ErrorKind.NOT_FOUND]: ErrorCode.NotFound,
    [exports.ErrorKind.UNIQUE]: ErrorCode.Unique,
    [exports.ErrorKind.NOT_NULL]: ErrorCode.NotNull,
    [exports.ErrorKind.FOREIGN_KEY]: ErrorCode.ForeignKey,
    [exports.ErrorKind.PERMISSION]: ErrorCode.PolicyDenied,
    [exports.ErrorKind.REDACTED]: ErrorCode.Redacted,
    [exports.ErrorKind.RATE_LIMITED]: ErrorCode.RateLimited,
    [exports.ErrorKind.QUOTA]: ErrorCode.RateLimited,
    [exports.ErrorKind.RETRYABLE]: ErrorCode.Unavailable,
    [exports.ErrorKind.VALIDATION]: ErrorCode.Invalid,
    [exports.ErrorKind.INTERNAL]: ErrorCode.Internal,
};
const GRPC_CODES = {
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
/** The row a keyed read or conditional write named does not exist. */
class NotFoundError extends Error {
    code = ErrorCode.NotFound;
}
exports.NotFoundError = NotFoundError;
/** A compare-and-swap precondition did not hold. */
class ConflictError extends Error {
    code = ErrorCode.Conflict;
}
exports.ConflictError = ConflictError;
/** Find the broker failure inside an error: a store's error keeps it as its cause. */
function underlyingFailure(err) {
    let current = err;
    for (let depth = 0; current && depth < 8; depth += 1) {
        if (current instanceof generatedClient_1.UdbError)
            return current;
        if (current.udb instanceof generatedClient_1.UdbError)
            return current.udb;
        current = current.cause;
    }
    return undefined;
}
/** A trailer gives the kind as its number; a transaction frame as its enum name. */
function kindNumber(kind) {
    if (typeof kind === "number")
        return kind;
    if (typeof kind === "string") {
        const name = kind.replace(/^ERROR_KIND_/, "");
        return exports.ErrorKind[name];
    }
    return undefined;
}
/** Classify any error from a store, the generated client or a raw gRPC call: the
 *  broker's stable reason decides when present, then the typed kind, then the gRPC
 *  code. An error that is not a call failure has code `unknown`. */
function inspect(err) {
    const udb = underlyingFailure(err);
    const sentinel = err instanceof ConflictError || err instanceof entity_1.UdbConflictError
        ? ErrorCode.Conflict
        : err instanceof NotFoundError || err instanceof entity_1.UdbNotFoundError
            ? ErrorCode.NotFound
            : undefined;
    if (!udb) {
        const code = err?.code;
        const status = typeof code === "number" ? code : undefined;
        return { code: sentinel ?? (status !== undefined ? GRPC_CODES[status] : undefined) ?? ErrorCode.Unknown, grpc: status };
    }
    const detail = udb.detail ?? {};
    const reason = typeof detail.reason === "string" && detail.reason.startsWith("UDB_") ? detail.reason : undefined;
    const kind = kindNumber(detail.kind);
    // A validation kind says little on its own: the gRPC code decides, so a
    // FAILED_PRECONDITION stays a precondition rather than an invalid request.
    const byKind = kind !== undefined && kind !== exports.ErrorKind.VALIDATION ? KIND_CODES[kind] : undefined;
    const retryAfter = Number(detail.retry_after_ms ?? 0);
    return {
        code: (reason && REASON_CODES[reason]) || byKind || sentinel || GRPC_CODES[udb.code] || ErrorCode.Unknown,
        reason,
        constraint: detail.constraint,
        column: detail.column,
        fixHint: detail.fix_hint,
        missing: detail.missing,
        retryAfterMs: retryAfter > 0 ? retryAfter : undefined,
        grpc: udb.code,
    };
}
const codeOf = (err) => inspect(err).code;
exports.codeOf = codeOf;
const isNotFound = (err) => (0, exports.codeOf)(err) === ErrorCode.NotFound;
exports.isNotFound = isNotFound;
const isConflict = (err) => (0, exports.codeOf)(err) === ErrorCode.Conflict;
exports.isConflict = isConflict;
const isUnique = (err, constraint) => {
    const info = inspect(err);
    return info.code === ErrorCode.Unique && (!constraint || info.constraint === constraint);
};
exports.isUnique = isUnique;
/** The scope the broker said the caller lacks ("" when the failure is not a missing scope). */
function missingScope(err) {
    const info = inspect(err);
    return info.code === ErrorCode.ScopeMissing ? info.missing?.scope ?? "" : "";
}
/** Whether `err` is a compare-and-swap precondition failure: the target row was
 *  absent or a field no longer matched. Check it after a write with `expected` to
 *  decide whether to re-read and retry. Other FAILED_PRECONDITION refusals, such as
 *  a conditional write not keyed by a primary key, are usage errors a retry cannot
 *  fix, and are not conflicts. The message text is never matched. */
function isCasConflict(err) {
    const udb = underlyingFailure(err);
    if (!udb || udb.code !== grpc.status.FAILED_PRECONDITION)
        return false;
    const reason = udb.detail?.reason;
    return reason === "UDB_CAS_CONFLICT" || reason === "UDB_CAS_ROW_MISSING" || reason === "UDB_REVISION_CONFLICT";
}
function jsonBytes(value) {
    return Buffer.from((0, recordCodec_1.recordToJson)(value), "utf8");
}
/** Make a value a google.protobuf.Struct can carry without losing it: an integer
 *  beyond 2^53 travels as its decimal string, which the broker reads exactly. */
function structSafe(value) {
    if (typeof value === "bigint")
        return value.toString();
    if (value instanceof recordCodec_1.RawJson)
        return value.toJSON();
    if (value instanceof Date)
        return (0, recordCodec_1.formatTimestamp)(value);
    if (Array.isArray(value))
        return value.map(structSafe);
    if (value && typeof value === "object" && !Buffer.isBuffer(value) && !(value instanceof Uint8Array)) {
        return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, structSafe(item)]));
    }
    return value;
}
function structMap(value) {
    return structSafe(value);
}
function decodeRows(response) {
    const raw = response?.records_json;
    const items = Array.isArray(raw) ? raw : raw ? [raw] : [];
    if (items.length === 0 && Array.isArray(response?.rows) && response.rows.length > 0) {
        throw new Error(`udb: the broker sent ${response.rows.length} row(s) without records_json`);
    }
    const rows = [];
    for (const item of items) {
        const text = Buffer.isBuffer(item) || item instanceof Uint8Array ? Buffer.from(item).toString("utf8") : typeof item === "string" ? item : "";
        const parsed = text ? (0, entity_1.parseRecordJson)(text) : undefined;
        if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
            throw new Error("udb: a row in records_json is not a JSON object");
        }
        rows.push(parsed);
    }
    return rows;
}
/** Find the proto declaration of a message; undefined when the tree has none, as
 *  for a transport-only unit test over an undeclared type. */
function schemaOf(schemas, messageType) {
    try {
        return schemas.find(messageType);
    }
    catch {
        return undefined;
    }
}
function schemasOf(plane, given) {
    return given ?? (0, protoSchema_1.protoSchemas)(plane.opts?.protoRoot);
}
/** The whole row a write stores, through the column rules when the message is
 *  declared. An unknown property is refused rather than dropped. */
function writeRecord(schema, schemas, messageType, row) {
    if (!schema)
        return row;
    for (const key of Object.keys(row)) {
        if (!schema.field(key))
            throw new Error(`udb: ${messageType} has no field "${key}"`);
    }
    return (0, recordCodec_1.encodeRecord)(messageType, (0, recordCodec_1.messageFromRow)(messageType, row, schemas), recordCodec_1.WriteMode.Insert, schemas);
}
const EMPTY_GUARD = "udb: guarded write requires a non-empty expected precondition";
/** A typed store over one entity. Writes go through the broker's data plane, so
 *  policies, tenancy, projections and events apply as for any write. After a
 *  write, the next reads through the same fence carry that write's read fence. */
class Table {
    plane;
    messageType;
    fence;
    schemas;
    explicitKey;
    constructor(plane, messageType, opts = {}) {
        this.plane = plane;
        this.messageType = messageType;
        if (opts.key && !opts.key.length)
            throw new Error(`udb: Table(${messageType}) needs its key fields`);
        this.explicitKey = opts.key;
        this.fence = opts.fence;
        this.schemas = schemasOf(plane, opts.schemas);
    }
    get schema() {
        return schemaOf(this.schemas, this.messageType);
    }
    get key() {
        return this.explicitKey ?? this.schema?.primaryKeys ?? [];
    }
    /** The filter that addresses one row: the key fields of `key`, or `key` itself
     *  when it names another unique key. */
    keyFilter(key) {
        const fields = this.key;
        const filter = {};
        if (fields.length && fields.every((field) => field in key)) {
            for (const field of fields)
                filter[field] = key[field];
            return structMap(filter);
        }
        if (this.explicitKey) {
            const missing = fields.find((field) => !(field in key));
            throw new Error(`udb: ${this.messageType} key is missing ${missing}`);
        }
        if (Object.keys(key).length === 0)
            throw new Error(`udb: ${this.messageType} key is empty`);
        return structMap(key);
    }
    /** The primary key of `row`. Throws when a primary-key column has no value. */
    keyOf(row) {
        const fields = this.key;
        if (fields.length === 0)
            throw new Error(`udb: ${this.messageType} declares no primary key`);
        const key = {};
        for (const field of fields) {
            const value = row[field];
            if (value === undefined || value === null || value === "") {
                throw new Error(`udb: ${this.messageType} has no value for primary-key column ${field}`);
            }
            key[field] = value;
        }
        return key;
    }
    /** The row with `key`. A missing row throws {@link NotFoundError}. */
    async get(key) {
        const page = await this.select(this.keyFilter(key), { limit: 2 });
        if (page.rows.length === 0)
            throw new NotFoundError(`udb: ${this.messageType} ${JSON.stringify(key)} not found`);
        if (page.rows.length > 1) {
            throw new Error(`udb: ${this.messageType} key ${JSON.stringify(key)} matched more than one row; address a primary or unique key`);
        }
        return page.rows[0];
    }
    /** One page of rows matching `filter`. */
    async select(filter = {}, opts = {}) {
        const request = {
            message_type: this.messageType,
            filter: structMap(filter),
            limit: opts.limit ?? 0,
            include_total: opts.includeTotal ?? false,
        };
        if (opts.pageToken)
            request.page_token = opts.pageToken;
        if (opts.fields?.length)
            request.fields = [...opts.fields];
        if (opts.orderBy?.length) {
            request.sort = opts.orderBy.map((column) => ({
                field: column.startsWith("-") ? column.slice(1) : column,
                descending: column.startsWith("-"),
            }));
        }
        const context = { ...(this.fence?.readContext() ?? {}), ...(opts.primaryRead ? { primary_read: true } : {}) };
        if (Object.keys(context).length > 0)
            request.context = context;
        const response = await this.plane.unary(DATABROKER, "Select", request).catch((err) => {
            throw classify(err);
        });
        return {
            rows: this.schema
                ? decodeRows(response).map((row) => (0, recordCodec_1.rowFromRecord)(this.messageType, row, this.schemas))
                : decodeRows(response),
            hasMore: Boolean(response?.has_more),
            next: String(response?.next_page_token ?? ""),
            total: opts.includeTotal ? Number(response?.exact_total ?? 0) : undefined,
            redacted: Array.isArray(response?.redacted_fields) ? response.redacted_fields : [],
        };
    }
    /** Walk every row matching `filter`, page by page. A throw from `fn` stops the
     *  walk and propagates. */
    async selectAll(filter, fn) {
        let pageToken;
        for (;;) {
            const page = await this.select(filter, { limit: 500, pageToken });
            for (const row of page.rows)
                await fn(row);
            if (!page.next || !page.hasMore)
                return;
            pageToken = page.next;
        }
    }
    async count(filter = {}) {
        return (await this.select(filter, { limit: 1, includeTotal: true })).total ?? 0;
    }
    /** Write the whole row: inserted when its key is new, replaced when it exists.
     *  To change some columns use {@link patch} or {@link updateIf}. NotFoundError
     *  means the broker's scope guard left a conflicting row untouched. */
    async upsert(row, opts = {}) {
        await this.write("Upsert", row, opts);
    }
    /** Create the row. A primary or unique-key collision throws with code `unique`
     *  and never replaces the existing row. */
    async insert(row, opts = {}) {
        await this.write("Insert", row, opts);
    }
    async write(rpc, row, opts) {
        const request = {
            message_type: this.messageType,
            record_json: jsonBytes(writeRecord(this.schema, this.schemas, this.messageType, row)),
        };
        if (rpc === "Upsert" && this.explicitKey)
            request.conflict_fields = this.explicitKey;
        if (opts.idempotencyKey)
            request.idempotency_key = opts.idempotencyKey;
        const response = await this.plane.unary(DATABROKER, rpc, request).catch((err) => {
            throw classify(err);
        });
        if (String(response?.affected_rows ?? "0") === "0" && !response?.was_duplicate) {
            throw new NotFoundError(`${this.messageType}: write target not found in the verified scope`);
        }
        this.fence?.rememberWrite(response);
    }
    /** Set some columns of an existing row; NotFoundError when it does not exist. */
    async patch(key, changes, opts = {}) {
        await this.update(key, changes, undefined, undefined, opts);
    }
    /** Set columns only while the row still holds `expected`; ConflictError when
     *  it changed or is gone. */
    async updateIf(key, expected, changes, opts = {}) {
        if (Object.keys(expected).length === 0)
            throw new Error(EMPTY_GUARD);
        await this.update(key, changes, expected, undefined, opts);
    }
    /** Add `deltas` to numeric columns of the row with `key`, atomically on the
     *  broker, with no read-modify-write window. */
    async increment(key, deltas, opts = {}) {
        await this.update(key, undefined, undefined, deltas, opts);
    }
    /** Read the row, apply `change`, and write back only the columns it modified,
     *  guarded by their previous values. A conflict reads and tries again, up to
     *  `attempts` times. Returns the row as written. */
    async updateWithRetry(key, change, attempts = 5) {
        const limit = attempts > 0 ? attempts : 5;
        for (let attempt = 0; attempt < limit; attempt += 1) {
            const current = await this.get(key);
            const before = this.updateColumns(current);
            const next = await change(structuredClone(current));
            const after = this.updateColumns(next);
            const changes = {};
            const expected = {};
            for (const [column, value] of Object.entries(after)) {
                if ((0, recordCodec_1.recordToJson)(before[column]) !== (0, recordCodec_1.recordToJson)(value)) {
                    changes[column] = value;
                    // What the row holds, not how this SDK re-encodes it: NULL, "" and a REAL's digits
                    // do not survive that. A column the read did not return cannot be guarded.
                    const stored = current;
                    if (column in stored)
                        expected[column] = stored[column];
                }
            }
            if (Object.keys(changes).length === 0)
                return current;
            try {
                await this.update(key, changes, expected);
                return next;
            }
            catch (err) {
                if (!(err instanceof ConflictError))
                    throw err;
            }
        }
        throw new ConflictError(`udb: conflict: gave up after ${limit} attempts`);
    }
    /** The columns of `row` an update may set. */
    updateColumns(row) {
        const schema = this.schema;
        if (schema) {
            return (0, recordCodec_1.encodeRecord)(this.messageType, (0, recordCodec_1.messageFromRow)(this.messageType, row, this.schemas), recordCodec_1.WriteMode.Update, this.schemas);
        }
        const out = { ...row };
        for (const field of this.key)
            delete out[field];
        return out;
    }
    async delete(key, opts = {}) {
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
    async update(key, changes, expected, deltas, opts = {}) {
        const request = {
            message_type: this.messageType,
            filter: this.keyFilter(key),
            require_affected: 1,
        };
        if (opts.idempotencyKey)
            request.idempotency_key = opts.idempotencyKey;
        if (changes && Object.keys(changes).length > 0)
            request.changes = structMap(changes);
        if (expected && Object.keys(expected).length > 0)
            request.expected = structMap(expected);
        if (deltas) {
            request.increments = Object.entries(deltas).map(([column, delta]) => ({ column, delta }));
        }
        const response = await this.plane.unary(DATABROKER, "Update", request).catch((err) => {
            throw classify(err, this.messageType, key);
        });
        this.fence?.rememberWrite(response);
    }
}
exports.Table = Table;
/** Map a broker status to the errors callers test with `instanceof`, keeping the
 *  typed status as the cause so {@link inspect} still reads its reason. */
function classify(err, messageType, key) {
    const info = inspect(err);
    const subject = messageType ? ` ${messageType} ${JSON.stringify(key ?? {})}` : "";
    if (info.reason === "UDB_CAS_CONFLICT" || info.reason === "UDB_REVISION_CONFLICT" || info.reason === "UDB_CAS_ROW_MISSING") {
        return new ConflictError(`udb:${subject} changed: ${err.message}`, { cause: err });
    }
    if (info.reason === "UDB_NO_ROWS_AFFECTED" || (info.grpc === grpc.status.NOT_FOUND && !(err instanceof NotFoundError))) {
        return new NotFoundError(`udb:${subject} not found: ${err.message}`, { cause: err });
    }
    return err;
}
/** The outbox envelope version this SDK writes and reads. */
exports.EVENT_ENVELOPE_VERSION = 2;
/** Writes collected for one atomic commit. Use it only inside the function
 *  passed to {@link tx}. A write this scope refuses throws, and also fails the
 *  whole transaction even when the function catches the throw. */
class TxScope {
    correlationId;
    schemas;
    replayKey;
    mutations = [];
    /** The first write this scope refused. */
    failure;
    constructor(correlationId, schemas = (0, protoSchema_1.protoSchemas)(), replayKey) {
        this.correlationId = correlationId;
        this.schemas = schemas;
        this.replayKey = replayKey;
    }
    fail(err) {
        if (this.failure === undefined)
            this.failure = err;
        throw err;
    }
    write(operation, messageType, row) {
        let record;
        try {
            record = writeRecord(schemaOf(this.schemas, messageType), this.schemas, messageType, row);
        }
        catch (err) {
            this.fail(err);
        }
        this.push({ operation, message_type: messageType, record_json: jsonBytes(record) });
    }
    /** Write the whole row in the transaction. */
    upsert(messageType, row) {
        this.write("upsert", messageType, row);
    }
    /** Create the row. A duplicate fails with code `unique` and rolls back every
     *  write and event in the transaction. */
    insert(messageType, row) {
        this.write("insert", messageType, row);
    }
    /** Set the given columns on the row with `key`. */
    patch(messageType, key, changes) {
        this.update(messageType, key, changes);
    }
    /** Set columns if the stored row still holds `expected`; otherwise the whole
     *  transaction fails with code `conflict`. */
    updateIf(messageType, key, expected, changes) {
        if (Object.keys(expected).length === 0)
            this.fail(new Error(EMPTY_GUARD));
        this.update(messageType, key, changes, expected);
    }
    /** Write the updatable columns of `next` if the stored row still holds
     *  `expected`; the key comes from `next`'s primary-key fields. */
    updateRowIf(messageType, next, expected) {
        let key = {};
        let changes = {};
        try {
            const schema = this.schemas.message(messageType);
            const message = (0, recordCodec_1.messageFromRow)(messageType, next, this.schemas);
            const record = (0, recordCodec_1.encodeRecord)(messageType, message, recordCodec_1.WriteMode.Insert, this.schemas);
            if (Object.keys(expected).length === 0)
                throw new Error(EMPTY_GUARD);
            if (schema.primaryKeys.length === 0)
                throw new Error(`udb: ${messageType} declares no primary key`);
            for (const column of schema.primaryKeys) {
                const value = record[column];
                if (value === undefined || value === null || value === "") {
                    throw new Error(`udb: ${messageType} has no value for primary-key column ${column}`);
                }
                key[column] = value;
            }
            changes = (0, recordCodec_1.encodeRecord)(messageType, message, recordCodec_1.WriteMode.Update, this.schemas);
        }
        catch (err) {
            this.fail(err);
        }
        this.update(messageType, key, changes, expected);
    }
    /** Remove the row with `key` in the transaction. */
    delete(messageType, key) {
        this.push({ operation: "delete", message_type: messageType, filter: structMap(key), require_affected: 1 });
    }
    update(messageType, key, changes, expected) {
        const mutation = {
            operation: "update",
            message_type: messageType,
            filter: structMap(key),
            require_affected: 1,
        };
        if (Object.keys(changes).length > 0)
            mutation.changes = structMap(changes);
        if (expected && Object.keys(expected).length > 0)
            mutation.expected = structMap(expected);
        this.push(mutation);
    }
    emit(first, event) {
        if (typeof first !== "string") {
            this.enqueue(first.messageType ?? first.eventType, first.topic, first.eventType, first.documentId, first.payload);
            return;
        }
        const messageType = first;
        let topic = "";
        let eventType = "";
        let document;
        let payload = {};
        try {
            const schema = this.schemas.message(messageType);
            topic = schema.topic;
            if (!topic)
                throw new Error(`udb: ${messageType} declares no outbox topic (message_event_contract.outbox_topic)`);
            const keyField = schema.partitionKeyField;
            if (!keyField)
                throw new Error(`udb: ${messageType} declares no partition key field (message_event_contract.partition_key_field)`);
            payload = (0, recordCodec_1.encodeEvent)(messageType, event ?? {}, this.schemas);
            document = payload[keyField];
            if (typeof document !== "string" || document === "") {
                throw new Error(`udb: event ${messageType} has no value in its partition key field ${keyField}`);
            }
            eventType = schema.eventType;
        }
        catch (err) {
            this.fail(err);
        }
        this.enqueue(messageType, topic, eventType, document, payload);
    }
    /** A replay key on each write of a keyed transaction, so a retry repeats none of them. */
    push(mutation) {
        if (this.replayKey && ["upsert", "update", "delete"].includes(String(mutation.operation))) {
            mutation.idempotency_key = `${this.replayKey}:${this.mutations.length}`;
        }
        this.mutations.push(mutation);
    }
    /** Random, or stable across retries of a keyed transaction. */
    eventId() {
        if (!this.replayKey)
            return (0, node_crypto_1.randomUUID)();
        const sum = (0, node_crypto_1.createHash)("sha256").update(`${this.replayKey}:${this.mutations.length}`).digest();
        sum[6] = (sum[6] & 0x0f) | 0x50;
        sum[8] = (sum[8] & 0x3f) | 0x80;
        const hex = sum.subarray(0, 16).toString("hex");
        return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
    }
    enqueue(messageType, topic, eventType, document, payload) {
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
                envelope_version: exports.EVENT_ENVELOPE_VERSION,
                payload,
            },
        });
    }
}
exports.TxScope = TxScope;
/** The error a rolled-back status frame describes. The terminal trailer stays
 *  authoritative when the broker sends one. */
function txStatusError(status) {
    const message = String(status?.message ?? "transaction rolled back");
    const code = Number(status?.code) || grpc.status.UNKNOWN;
    return new generatedClient_1.UdbError("DataBroker/BeginTx", Object.assign(new Error(message), { code, details: message, metadata: new grpc.Metadata() }), status?.error_detail ?? undefined);
}
/** Run `body` and commit everything it collected atomically over BeginTx: every
 *  write and event commits together or none does. `body` throwing commits nothing,
 *  and with nothing collected no stream opens. A failed mutation rejects with its
 *  own error (conflict, unique, ...). */
async function tx(plane, body, opts = {}) {
    if (!plane.bidiStream)
        throw new Error("udb: this transport does not support transactions");
    const txId = (0, node_crypto_1.randomUUID)();
    const correlationId = opts.correlationId || plane.opts?.meta?.correlationId || txId;
    const scope = new TxScope(correlationId, schemasOf(plane, opts.schemas), opts.replayKey);
    await body(scope);
    if (scope.failure !== undefined)
        throw scope.failure;
    if (scope.mutations.length === 0)
        return;
    // A transaction ends, unlike a subscription, so the call deadline bounds it.
    const deadlineMs = opts.call?.deadlineMs ?? plane.opts?.deadlineMs;
    const stream = plane.bidiStream(DATABROKER, "BeginTx", deadlineMs ? { ...opts.call, deadlineMs } : opts.call);
    await new Promise((resolve, reject) => {
        let committed = false;
        let refused;
        stream.on("data", (status) => {
            const state = String(status?.state ?? "");
            if (state === "TX_STATE_COMMITTED" || state === "2") {
                committed = true;
                opts.fence?.rememberReceipt((0, consistency_1.receiptFromProto)(status?.write_receipt));
            }
            if (state === "TX_STATE_ERROR" || state === "TX_STATE_ROLLED_BACK" || state === "4" || state === "3") {
                refused = txStatusError(status);
            }
        });
        // The trailer carries the typed mutation error; an ERROR data frame must
        // not reject first and hide the Conflict/Unique classification.
        // Once the broker said it committed, a broken stream loses nothing.
        stream.on("error", (err) => (committed ? resolve() : reject(classify(err?.udb ?? err))));
        stream.on("end", () => {
            if (refused)
                reject(classify(refused));
            else if (!committed)
                reject(new Error(`udb: transaction ${txId} ended without a commit acknowledgement`));
            else
                resolve();
        });
        for (const mutation of scope.mutations)
            stream.write({ ...mutation, tx_id: txId });
        stream.write({ tx_id: txId, commit: true });
        stream.end();
    });
}
class EnvelopeVersionError extends Error {
    reason = "UDB_ENVELOPE_VERSION_UNSUPPORTED";
}
/** Unwrap the outbox envelope (a journal row may nest it once more). */
function decodeEnvelope(payloadJson) {
    let envelope = JSON.parse(payloadJson || "{}");
    const inner = envelope?.payload;
    if (inner && typeof inner === "object" && "event_type" in inner && "payload" in inner)
        envelope = inner;
    if (envelope?.envelope_version === undefined || envelope?.envelope_version === null) {
        throw new EnvelopeVersionError(`udb: envelope has no envelope_version; unversioned envelopes are not supported (this SDK reads version ${exports.EVENT_ENVELOPE_VERSION})`);
    }
    const version = Number(envelope.envelope_version);
    if (version > exports.EVENT_ENVELOPE_VERSION) {
        throw new EnvelopeVersionError(`udb: envelope_version ${version} is newer than this SDK understands (${exports.EVENT_ENVELOPE_VERSION}); upgrade the SDK`);
    }
    if (version !== exports.EVENT_ENVELOPE_VERSION) {
        throw new EnvelopeVersionError(`udb: envelope_version ${version} is not supported; this SDK reads version ${exports.EVENT_ENVELOPE_VERSION}`);
    }
    return envelope;
}
const MAX_CONSUMER_ATTEMPTS = 1000;
const MAX_CONSUMER_ACK_BATCH = 1024;
const MAX_BACKOFF_MS = 30_000;
const SETTLEMENT_TIMEOUT_MS = 5_000;
const MAX_FAILURE_REASON_BYTES = 2048;
const RECENT_IDS = 4096;
function normalizeConsume(o) {
    const positive = (value, fallback) => (value && value > 0 ? value : fallback);
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
    err;
    constructor(err) {
        this.err = err;
    }
}
/** An error from a settlement rather than the stream: this batch is not reissued
 *  on the way out. */
class SettlementRefused {
    err;
    constructor(err) {
        this.err = err;
    }
}
const STOPPED = Symbol("consumer stopped");
function unwrapConsumeError(err) {
    if (err instanceof SettlementRefused)
        return unwrapConsumeError(err.err);
    if (err instanceof ConsumeFatal)
        return err.err;
    return err;
}
/** Invalid consumer or cursor, missing authority and unavailable contracts need the
 *  caller; reconnecting cannot repair them. */
function consumeAttemptError(err) {
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
function errorText(err) {
    return err instanceof Error ? err.message : String(err);
}
/** The handler error as valid UTF-8, cut to the bound on a character boundary. */
function consumerFailureReason(err) {
    // Encoding replaces a lone surrogate, so the bytes are always valid UTF-8.
    const bytes = Buffer.from(errorText(err), "utf8");
    if (bytes.length <= MAX_FAILURE_REASON_BYTES)
        return bytes.toString("utf8");
    let end = MAX_FAILURE_REASON_BYTES;
    while (end > 0 && (bytes[end] & 0xc0) === 0x80)
        end -= 1;
    return bytes.subarray(0, end).toString("utf8");
}
function sleep(ms, signal) {
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
    completed = new Set();
    durable = new Set();
    pending = [];
    pendingSet = new Set();
    poison;
    /** Successful settlements: tells a routine reauthentication from no progress. */
    settlements = 0;
    has(id) {
        return this.completed.has(id);
    }
    durableHas(id) {
        return this.durable.has(id);
    }
    pendingHas(id) {
        return this.pendingSet.has(id);
    }
    get pendingCount() {
        return this.pending.length;
    }
    /** Queue `id` as the newest watermark. A redelivery of a handled event is queued
     *  on purpose: re-sending its acknowledgement is what advances the cursor. */
    enqueue(id) {
        this.pending.push(id);
        this.pendingSet.add(id);
    }
    add(id) {
        this.remember(this.completed, id);
    }
    remember(set, id) {
        set.add(id);
        if (set.size > RECENT_IDS)
            set.delete(set.values().next().value);
    }
    clearPending() {
        this.pending.length = 0;
        this.pendingSet.clear();
    }
    /** Acknowledge the open batch as one watermark and dead-letter a poison event. It
     *  outlives a cancelled consumer for one bounded call, and never marks an event
     *  durable before the broker confirms, so a lost response leaves it retained and
     *  a retry is safe. Returns the failure instead of throwing. */
    async settle(plane, name, pattern, o, onError) {
        if (this.pending.length === 0 && !this.poison)
            return undefined;
        const call = { deadlineMs: o.ackTimeoutMs, noRetry: true };
        if (this.pending.length > 0) {
            const last = this.pending[this.pending.length - 1];
            try {
                await plane.unary(DATABROKER, "AckCdcEvents", { consumer_name: name, topic_pattern: pattern, event_id: last }, call);
            }
            catch (err) {
                onError?.(new Error(`udb: consumer ${name} ack ${last}: ${errorText(err)}`, { cause: err }));
                // At batch size 1 nothing carries across the reconnect: the broker redelivers
                // the event and the already-handled path acknowledges it again.
                if (o.ackBatchSize <= 1)
                    this.clearPending();
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
                await plane.unary(DATABROKER, "NackCdcEvents", { consumer_name: name, topic_pattern: pattern, event_id: failed.id, attempts: failed.attempts, failure_reason: failed.reason }, call);
            }
            catch (err) {
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
async function handleWithRetry(event, handle, o, signal, onError) {
    let delay = o.backoffMs;
    let failure;
    for (let attempt = 1; attempt <= o.maxAttempts; attempt += 1) {
        try {
            await handle(event, signal);
            return undefined;
        }
        catch (err) {
            failure = err ?? new Error("udb: consumer handler failed");
        }
        onError?.(new Error(`event ${event.id} attempt ${attempt}: ${errorText(failure)}`, { cause: failure }));
        if (attempt === o.maxAttempts)
            break;
        await sleep(delay, signal);
        if (signal.aborted)
            return STOPPED;
        delay = Math.min(delay * 2, MAX_BACKOFF_MS);
    }
    return failure;
}
function publishedAtOf(envelope) {
    const stamp = envelope?.published_at;
    if (!stamp)
        return undefined;
    const ms = Number(stamp.seconds ?? 0) * 1000 + Math.floor(Number(stamp.nanos ?? 0) / 1e6);
    return ms > 0 ? new Date(ms) : undefined;
}
function decodeConsumedEvent(id, raw, messageType, schemas) {
    const envelope = decodeEnvelope(String(raw?.payload_json ?? ""));
    const domain = envelope.payload;
    let payload = domain ?? {};
    if (messageType && domain !== undefined && domain !== null) {
        try {
            payload = (0, recordCodec_1.decodeEventPayload)(messageType, domain, schemas);
        }
        catch (err) {
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
        payload: payload,
        envelope,
    };
}
/** Hold items a stream delivered until the loop asks; pauses the stream when the
 *  loop falls behind, so a slow handler cannot grow the queue without bound. */
const STREAM_HIGH_WATER = 256;
async function consumeOnce(plane, name, topicPattern, handle, opts, o, progress, schemas) {
    const signal = opts.signal;
    const batched = o.ackBatchSize > 1;
    const settle = () => progress.settle(plane, name, topicPattern, o, opts.onError);
    const failed = await settle();
    if (failed)
        return failed;
    if (signal?.aborted)
        return STOPPED;
    const attempt = new AbortController();
    const stopAttempt = () => attempt.abort();
    signal?.addEventListener("abort", stopAttempt, { once: true });
    let stream;
    try {
        // Progress markers let a consumer of a quiet topic move its stored position
        // past the rest of the journal; they never reach the handler.
        stream = plane.serverStream(DATABROKER, "PublishCDC", {
            topic_pattern: topicPattern,
            consumer_name: name,
            progress_markers: true,
            start: opts.fromEarliest ? "CDC_CONSUMER_START_EARLIEST" : "CDC_CONSUMER_START_LATEST",
        }, { signal: attempt.signal });
    }
    catch (err) {
        signal?.removeEventListener("abort", stopAttempt);
        return consumeAttemptError(err);
    }
    const queue = [];
    let wake;
    const notify = () => {
        const resume = wake;
        wake = undefined;
        resume?.();
    };
    let paused = false;
    stream.on("data", (envelope) => {
        queue.push({ envelope });
        if (queue.length >= STREAM_HIGH_WATER && stream.pause) {
            paused = true;
            stream.pause();
        }
        notify();
    });
    stream.on("error", (err) => {
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
            if (signal?.aborted)
                return STOPPED;
            if (tick) {
                tick = false;
                const failure = await settle();
                if (failure)
                    return failure;
            }
            const item = queue.shift();
            if (!item) {
                await new Promise((resolve) => {
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
                    if (failure)
                        return failure;
                }
                if ("ended" in item)
                    return new Error("udb: the event stream ended");
                return consumeAttemptError(item.error);
            }
            const envelope = item.envelope;
            const id = String(envelope?.event_id ?? "");
            if (progress.pendingHas(id))
                continue;
            if (envelope?.progress_only) {
                // Not an event: the stream read the journal up to this position. Every
                // earlier event is settled or queued ahead of it, so its token goes last.
                if (progress.durableHas(id))
                    continue;
                progress.enqueue(id);
            }
            else {
                // A durable id needs nothing at all; a handled-but-not-durable id is
                // acknowledged again, which advances a cursor the broker never confirmed.
                if (!progress.has(id) && !progress.durableHas(id)) {
                    let event;
                    try {
                        event = decodeConsumedEvent(id, envelope, opts.messageType, schemas);
                    }
                    catch (err) {
                        const message = `udb: consumer ${name}: event ${id}: ${errorText(err)}`;
                        return new ConsumeFatal(err instanceof EnvelopeVersionError ? new EnvelopeVersionError(message) : new Error(message, { cause: err }));
                    }
                    const failure = await handleWithRetry(event, handle, o, attempt.signal, opts.onError);
                    if (failure !== undefined) {
                        if (failure === STOPPED || signal?.aborted)
                            return STOPPED;
                        if (failure?.name === "AbortError" || failure?.name === "TimeoutError") {
                            return new ConsumeFatal(new Error(`udb: consumer ${name} gave up on event ${id}: ${errorText(failure)}`, { cause: failure }));
                        }
                        // A poison event is dead-lettered: the broker stores it and advances this
                        // consumer in one transaction, so later events keep flowing. A broker that
                        // cannot (UNIMPLEMENTED) stops the consumer instead of discarding the event.
                        progress.poison = { id, attempts: o.maxAttempts, reason: consumerFailureReason(failure) };
                        const refused = await settle();
                        if (refused)
                            return refused;
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
                if (failure)
                    return failure;
            }
        }
    }
    finally {
        if (ticker)
            clearInterval(ticker);
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
async function consume(plane, name, topicPattern, handle, opts = {}) {
    if (!plane.serverStream)
        throw new Error("udb: this transport does not support streams");
    const o = normalizeConsume(opts);
    const progress = new ConsumerProgress();
    const schemas = schemasOf(plane, opts.schemas);
    // Flush a retained batch before returning, so an event whose handler completed
    // just before shutdown is still acknowledged. A settlement the broker definitively
    // refused is not reissued: it could only repeat a call that cannot succeed.
    const finish = async (cause) => {
        if (cause instanceof SettlementRefused)
            throw unwrapConsumeError(cause);
        const failure = await progress.settle(plane, name, topicPattern, o, opts.onError);
        if (cause !== undefined) {
            if (failure)
                throw new AggregateError([unwrapConsumeError(cause), unwrapConsumeError(failure)], errorText(unwrapConsumeError(cause)));
            throw unwrapConsumeError(cause);
        }
        if (failure)
            throw unwrapConsumeError(failure);
    };
    let delay = o.backoffMs;
    for (;;) {
        if (opts.signal?.aborted)
            return finish();
        const before = progress.settlements;
        const failure = await consumeOnce(plane, name, topicPattern, handle, opts, o, progress, schemas);
        if (opts.signal?.aborted) {
            return failure instanceof SettlementRefused ? finish(failure) : finish();
        }
        if (failure === STOPPED)
            return finish();
        const inner = failure instanceof SettlementRefused ? failure.err : failure;
        if (inner instanceof ConsumeFatal)
            return finish(failure instanceof SettlementRefused ? failure : inner);
        // Credential expiry and progress on a healthy stream must not accumulate an
        // ever-longer outage across a routine reauthentication.
        if (progress.settlements !== before || inspect(inner).grpc === grpc.status.UNAUTHENTICATED)
            delay = o.backoffMs;
        opts.onError?.(new Error(`udb: consumer ${name} reconnecting: ${errorText(inner)}`, { cause: inner }));
        await sleep(delay, opts.signal);
        if (opts.signal?.aborted)
            return finish();
        delay = Math.min(delay * 2, MAX_BACKOFF_MS);
    }
}
// ── In-memory data plane (tests) ────────────────────────────────────────────
/** A small stand-in for a unit test of code that only uses `Table` over plain rows:
 *  Select, Upsert, Insert, Update and Delete, with the broker's NotFound, Conflict
 *  and Unique reasons. For the broker's full semantics (tenancy, transactions,
 *  consumers, faults) use `createFake` from `@udb_plus/sdk/udbtest`. */
class InMemoryDataPlane {
    keys;
    rows = new Map();
    constructor(keys) {
        this.keys = keys;
    }
    matches(row, filter = {}) {
        return Object.entries(filter ?? {}).every(([field, value]) => row[field] === value);
    }
    refuse(code, reason, message) {
        const err = new Error(message);
        err.code = code;
        throw new generatedClient_1.UdbError("InMemory", err, { reason, kind: code === 9 ? 8 : code === 6 ? 10 : 9 });
    }
    async unary(_service, method, request) {
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
                const index = table.findIndex((existing) => key.every((field) => existing[field] === row[field]));
                if (index >= 0 && method === "Insert")
                    this.refuse(6, "UDB_UNIQUE_VIOLATION", "duplicate key");
                if (index >= 0)
                    table[index] = row;
                else
                    table.push(row);
                return { affected_rows: 1 };
            }
            case "Update": {
                const hits = table.filter((row) => this.matches(row, request.filter));
                if (request.require_affected && hits.length !== request.require_affected) {
                    if (request.expected && hits.length === 0)
                        this.refuse(9, "UDB_CAS_ROW_MISSING", "row missing");
                    this.refuse(5, "UDB_NO_ROWS_AFFECTED", "no rows affected");
                }
                if (request.expected && hits.some((row) => !this.matches(row, request.expected))) {
                    this.refuse(9, "UDB_CAS_CONFLICT", "precondition failed");
                }
                for (const row of hits)
                    Object.assign(row, request.changes ?? {});
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
exports.InMemoryDataPlane = InMemoryDataPlane;
/** Call options that make a Select read `tenant`'s rows instead of the caller's. The credential
 *  needs platform authority and the `udb:cross_tenant:read` scope, and the broker records
 *  `reason` in the durable audit log before it reads. */
function crossTenantRead(tenant, reason, call = {}) {
    return {
        ...call,
        headers: {
            ...call.headers,
            "x-udb-cross-tenant-target": tenant,
            "x-udb-cross-tenant-reason": reason,
        },
    };
}
