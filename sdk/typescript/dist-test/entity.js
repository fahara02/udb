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
exports.EntityHandle = void 0;
const DATABROKER = "udb.services.v1.DataBroker";
/** Encode a JS record as `record_json` bytes the broker expects. */
function jsonBytes(record) {
    return Buffer.from(JSON.stringify(record), "utf8");
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
        return JSON.parse(text);
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
    constructor(core, messageType, opts,
    /** Shared request context (tenant_id/project_id/…) merged into each call. */
    context = {}) {
        this.core = core;
        this.messageType = messageType;
        this.opts = opts;
        this.context = context;
    }
    /** Upsert one record. Emits exactly ONE `Upsert` with `record_json` bytes and
     *  `conflict_fields` from the configured key. Decodes the returned record. */
    async upsert(record, opts) {
        const request = {
            context: this.context,
            message_type: this.messageType,
            record_json: jsonBytes(record),
            conflict_fields: this.opts.key,
            return_record: opts?.returnRecord ?? true,
        };
        if (opts?.idempotencyKey)
            request.idempotency_key = opts.idempotencyKey;
        const resp = await this.core.unary(DATABROKER, "Upsert", request, opts?.call);
        return decodeRecord(resp?.record_json);
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
            context: this.context,
            message_type: this.messageType,
            filter: where,
        };
        if (effectiveOpts?.limit != null)
            request.limit = effectiveOpts.limit;
        const resp = await this.core.unary(DATABROKER, "Select", request, effectiveOpts?.call);
        const rows = Array.isArray(resp?.records_json)
            ? resp.records_json
            : Array.isArray(resp?.records)
                ? resp.records
                : [];
        return rows.map((r) => decodeRecord(r)).filter((r) => r != null);
    }
    /** Delete records matching a plain-object filter. Emits exactly ONE `Delete`. */
    async delete(where, opts) {
        const request = {
            context: this.context,
            message_type: this.messageType,
            filter: where,
        };
        if (opts?.idempotencyKey)
            request.idempotency_key = opts.idempotencyKey;
        return this.core.unary(DATABROKER, "Delete", request, opts?.call);
    }
}
exports.EntityHandle = EntityHandle;
