// Bound entity / table helper: hides record_json packing, conflict_fields, the
// plain-object filter, and record_json/records_json decode behind a typed handle
// over the DataBroker Upsert/Select/Delete RPCs.
//
// The broker remains the sole RLS / authority enforcement point — this helper
// never sets authoritative principal/actor/scopes; it only shapes the request
// body the caller already controls. The key (+ optional tenant/project field
// names) is supplied by the caller (or, once lane 07 ships {{ENTITY_*}}, read
// from the generated ENTITY_REGISTRY by `UdbProject.entity`).

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
  constructor(
    private readonly core: UdbCore,
    private readonly messageType: string,
    private readonly opts: EntityHandleOptions,
    /** Shared request context (tenant_id/project_id/…) merged into each call. */
    private readonly context: Record<string, unknown> = {},
  ) {}

  /** Upsert one record. Emits exactly ONE `Upsert` with `record_json` bytes and
   *  `conflict_fields` from the configured key. Decodes the returned record. */
  async upsert(
    record: Partial<T> & Record<string, unknown>,
    opts?: { returnRecord?: boolean; idempotencyKey?: string; call?: CallOptions },
  ): Promise<T | null> {
    const request: Record<string, unknown> = {
      context: this.context,
      message_type: this.messageType,
      record_json: jsonBytes(record),
      conflict_fields: this.opts.key,
      return_record: opts?.returnRecord ?? true,
    };
    if (opts?.idempotencyKey) request.idempotency_key = opts.idempotencyKey;
    const resp: any = await this.core.unary(DATABROKER, "Upsert", request, opts?.call);
    return decodeRecord<T>(resp?.record_json);
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
      context: this.context,
      message_type: this.messageType,
      filter: where,
    };
    if (effectiveOpts?.limit != null) request.limit = effectiveOpts.limit;
    const resp: any = await this.core.unary(DATABROKER, "Select", request, effectiveOpts?.call);
    const rows: unknown[] = Array.isArray(resp?.records_json)
      ? resp.records_json
      : Array.isArray(resp?.records)
        ? resp.records
        : [];
    return rows.map((r) => decodeRecord<T>(r)).filter((r): r is T => r != null);
  }

  /** Delete records matching a plain-object filter. Emits exactly ONE `Delete`. */
  async delete(
    where: Record<string, unknown>,
    opts?: { idempotencyKey?: string; call?: CallOptions },
  ): Promise<any> {
    const request: Record<string, unknown> = {
      context: this.context,
      message_type: this.messageType,
      filter: where,
    };
    if (opts?.idempotencyKey) request.idempotency_key = opts.idempotencyKey;
    return this.core.unary(DATABROKER, "Delete", request, opts?.call);
  }
}
