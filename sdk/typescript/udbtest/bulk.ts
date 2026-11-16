// The broker's BulkUpsert rules the fake applies before it writes a batch: the batch
// size and id, one row shape per batch, one row per key, and the key tuples whose
// digest the response and the batch's change event carry.

import { createHash, randomUUID } from "node:crypto";
import * as grpc from "@grpc/grpc-js";

import { RawJson, recordToJson } from "../recordCodec";
import { EntityRules } from "./entityRules";
import { Row } from "./filter";
import { refuse } from "./refusal";

const codes = grpc.status;

/** The most rows one BulkUpsert batch may carry. */
export const BULK_MAX_ROWS = 10_000;
const BATCH_ID_MAX = 128;
const KEY_SEPARATOR = "\u001f";

function invalid(reason: string, message: string, field: string) {
  return refuse(codes.INVALID_ARGUMENT, reason, message, {
    ...(reason ? { reason } : {}),
    kind: "ERROR_KIND_VALIDATION",
    field_violations: [{ field, description: message }],
  });
}

/** Refuse an empty batch or one over {@link BULK_MAX_ROWS} rows. */
export function checkBatchSize(rows: number): void {
  if (rows === 0) throw invalid("", "a bulk upsert needs at least one row in records_json", "records_json");
  if (rows > BULK_MAX_ROWS) {
    throw invalid("UDB_BULK_BATCH_TOO_LARGE", `a bulk upsert batch carries at most ${BULK_MAX_ROWS} rows, got ${rows}`, "records_json");
  }
}

/** The batch id to use: the caller's, checked, or a fresh UUID when empty. */
export function batchIdOf(given: unknown): string {
  const id = String(given ?? "");
  if (id === "") return randomUUID();
  if ([...id].length > BATCH_ID_MAX || /[\s\p{Cc}]/u.test(id)) {
    throw invalid("", `batch_id must be at most ${BATCH_ID_MAX} characters with no whitespace or control characters`, "batch_id");
  }
  return id;
}

/** The conflict-key columns: the request's conflict_fields resolved to field names,
 *  else the primary key. */
export function conflictColumns(entity: EntityRules, fields: readonly string[]): string[] {
  if (fields.length === 0) return [...entity.primaryKey];
  return fields.map((field) => entity.aliases.get(field.trim().toLowerCase()) ?? field);
}

/** A row's properties under their field names (an unknown property keeps its name). */
function canonical(entity: EntityRules, row: Row): Map<string, unknown> {
  const out = new Map<string, unknown>();
  for (const [name, value] of Object.entries(row)) out.set(entity.aliases.get(name.toLowerCase()) ?? name, value);
  return out;
}

/** Refuse a batch whose rows do not all carry the fields of the first row. */
export function checkRowShapes(entity: EntityRules, rows: readonly Row[]): void {
  const first = [...canonical(entity, rows[0]).keys()].sort();
  for (let index = 1; index < rows.length; index += 1) {
    const fields = [...canonical(entity, rows[index]).keys()].sort();
    if (fields.length !== first.length || fields.some((field, at) => field !== first[at])) {
      throw invalid(
        "UDB_BULK_ROW_SHAPE_MISMATCH",
        `row ${index} carries fields [${fields.join(", ")}] but row 0 carries [${first.join(", ")}]; every row of a batch must carry the same fields`,
        `records_json[${index}]`,
      );
    }
  }
}

/** One key value as a token: `s:` string, `i:` integer, `f:` other number,
 *  `b:` bool, `z:` null, `j:` any other JSON (canonical). */
export function keyToken(value: unknown): string {
  if (value === null || value === undefined) return "z:";
  if (typeof value === "string") return `s:${value}`;
  if (typeof value === "boolean") return `b:${value ? "true" : "false"}`;
  if (typeof value === "bigint") return `i:${value.toString()}`;
  if (typeof value === "number") return Number.isInteger(value) ? `i:${value}` : `f:${value}`;
  if (value instanceof RawJson) {
    const text = value.text.trim();
    if (/^-?\d+$/.test(text)) return `i:${text}`;
    if (/^-?\d/.test(text)) return `f:${text}`;
    return `j:${recordToJson(value)}`;
  }
  return `j:${recordToJson(value)}`;
}

/** A row's key tuple: its conflict-key values as tokens joined by U+001F. */
export function keyTuple(entity: EntityRules, columns: readonly string[], row: Row): string {
  const values = canonical(entity, row);
  return columns.map((column) => keyToken(values.get(column))).join(KEY_SEPARATOR);
}

/** The key tuples of a batch, refusing two rows with the same key. */
export function batchKeys(entity: EntityRules, columns: readonly string[], rows: readonly Row[]): string[] {
  const seen = new Map<string, number>();
  return rows.map((row, index) => {
    const tuple = keyTuple(entity, columns, row);
    const earlier = seen.get(tuple);
    if (earlier !== undefined) {
      throw invalid(
        "UDB_BULK_DUPLICATE_KEY",
        `row ${index} has the same key (${columns.join(", ")}) as row ${earlier}; a batch writes each key once`,
        `records_json[${index}]`,
      );
    }
    seen.set(tuple, index);
    return tuple;
  });
}

/** Lowercase hex SHA-256 of the key tuples joined by "\n", in request order. */
export function keysSha256(tuples: readonly string[]): string {
  return createHash("sha256").update(tuples.join("\n"), "utf8").digest("hex");
}
