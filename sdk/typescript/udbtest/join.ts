// A Select whose message_type names two or more entities ("A,B" or "A+B") is the
// broker's join-fusion read: one inner join of the tables along their foreign keys,
// each output column aliased `{Message}__{column}` (the short message name), or the
// requested field with its dots turned into `__`. It takes simple equality filters
// only, has no sort, page token or total, and leaves PII and encrypted columns out
// of the default projection. These are the fake's planning helpers for it.

import * as grpc from "@grpc/grpc-js";

import type { EntityRules } from "./entityRules";
import { refuse } from "./refusal";

const codes = grpc.status;

/** A foreign key in manifest form: lower-case SQL names, schema defaulting to public. */
export interface JoinForeignKey {
  name: string;
  columns: string[];
  refSchema: string;
  refTable: string;
  refColumns: string[];
}

/** One ON clause: the `left` table's columns equal the `right` table's. */
export interface JoinEdge {
  left: number;
  right: number;
  leftColumns: string[];
  rightColumns: string[];
}

/** Split a message_type as the broker does: on ',' or '+', trimmed, empties dropped. */
export function joinMessageTypes(messageType: string): string[] {
  return String(messageType ?? "")
    .split(/[,+]/)
    .map((part) => part.trim())
    .filter((part) => part !== "");
}

export function joinRefusal(message: string) {
  return refuse(codes.INVALID_ARGUMENT, "", message);
}

function identOr(value: string | undefined, fallback: string): string {
  const out = (value ?? "").trim().toLowerCase();
  return out || fallback;
}

/** The table's schema as the manifest names it. */
export function schemaOf(entity: EntityRules): string {
  return identOr(entity.schema.table.schema_name, "public");
}

/** Every foreign key, table-level and per column (a `foreign_key` block, else a
 *  `references: "schema.table(col)"`), in the manifest's order, so the first edge
 *  found is the broker's. */
export function foreignKeys(entity: EntityRules): JoinForeignKey[] {
  const lower = (values: readonly string[]) => values.map((v) => v.trim().toLowerCase()).filter((v) => v !== "");
  const out: JoinForeignKey[] = [];
  const add = (name: string | undefined, columns: string[], schema: string | undefined, table: string | undefined, refColumns: string[]) => {
    const local = lower(columns);
    const remote = lower(refColumns);
    out.push({
      name: (name ?? "").trim(),
      columns: local,
      refSchema: identOr(schema, "public"),
      refTable: (table ?? "").trim().toLowerCase(),
      refColumns: remote.length ? remote : local,
    });
  };
  for (const fk of entity.schema.table.foreign_keys ?? []) {
    add(fk.constraint_name, fk.columns, fk.references_schema, fk.references_table, fk.references_column);
  }
  for (const [name, field] of entity.columns) {
    const column = field.column;
    if (!column) continue;
    const local = column.column_name || name;
    if (column.foreign_key) {
      const fk = column.foreign_key;
      add(fk.constraint_name, [local], fk.references_schema, fk.references_table, fk.references_column ? [fk.references_column] : []);
      continue;
    }
    const reference = (column.references ?? "").trim();
    if (!reference) continue;
    let target = reference;
    let refColumn = "";
    const start = reference.indexOf("(");
    if (start >= 0) {
      target = reference.slice(0, start);
      const rest = reference.slice(start + 1);
      const end = rest.indexOf(")");
      refColumn = (end >= 0 ? rest.slice(0, end) : rest).trim();
    }
    const parts = target
      .split(".")
      .map((part) => part.trim())
      .filter((part) => part !== "");
    if (!refColumn) refColumn = local;
    if (parts.length === 1) add("", [local], "", parts[0], [refColumn]);
    else if (parts.length === 2) add("", [local], parts[0], parts[1], [refColumn]);
  }
  const key = (fk: JoinForeignKey) =>
    `${fk.name.toLowerCase()}|${fk.columns.join(",")}|${fk.refSchema}.${fk.refTable}(${fk.refColumns.join(",")})`;
  return out.sort((a, b) => (key(a) < key(b) ? -1 : key(a) > key(b) ? 1 : 0));
}

function references(from: EntityRules, to: EntityRules): JoinForeignKey | undefined {
  return foreignKeys(from).find((fk) => fk.refSchema === schemaOf(to) && fk.refTable === to.tableName.toLowerCase());
}

/** The ON clause joining each table after the first to an earlier one. */
export function joinEdges(entities: readonly EntityRules[], names: readonly string[]): JoinEdge[] {
  const edges: JoinEdge[] = [];
  for (let next = 1; next < entities.length; next++) {
    let edge: JoinEdge | undefined;
    for (let prior = 0; prior < next && !edge; prior++) {
      const forward = references(entities[prior], entities[next]);
      if (forward) {
        edge = { left: prior, right: next, leftColumns: forward.columns, rightColumns: forward.refColumns };
        continue;
      }
      const backward = references(entities[next], entities[prior]);
      if (backward) edge = { left: next, right: prior, leftColumns: backward.columns, rightColumns: backward.refColumns };
    }
    if (!edge) throw joinRefusal(`no foreign key path found for join fusion target ${names[next]}`);
    edges.push(edge);
  }
  return edges;
}

/** A fused field's table and column, as the broker's planner reads it: `Prefix.column`
 *  where the prefix is one of `names` (or its short form), the longest match winning;
 *  an unqualified field belongs to the first table. */
export function joinField(field: string, names: readonly string[]): [number, string] {
  if (!field.includes(".")) return [0, field.toLowerCase()];
  let best = -1;
  let bestLength = 0;
  let column = "";
  names.forEach((candidate, index) => {
    const dot = candidate.lastIndexOf(".");
    const prefixes = dot >= 0 ? [candidate, candidate.slice(dot + 1)] : [candidate];
    for (const prefix of prefixes) {
      if (!prefix || field.length <= prefix.length || field[prefix.length] !== ".") continue;
      if (field.slice(0, prefix.length).toLowerCase() !== prefix.toLowerCase()) continue;
      if (prefix.length > bestLength) {
        best = index;
        bestLength = prefix.length;
        column = field.slice(prefix.length + 1);
      }
    }
  });
  if (best < 0) throw joinRefusal(`unknown join field prefix ${field.split(".")[0]}`);
  return [best, column.toLowerCase()];
}

/** The SQL column name a field is stored under. */
export function columnName(entity: EntityRules, name: string): string {
  return entity.columns.get(name)?.column?.column_name || name;
}
