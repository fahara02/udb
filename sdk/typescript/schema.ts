// Schema reflection. Everything a service needs to know about a table (its keys,
// owner columns, column bounds, security class, event contract) is read here from
// the udb annotations in the proto tree, so no service restates the schema.
//
// Go reads these from compiled descriptors. TypeScript has no compiled descriptors
// at runtime, so each function takes the message's full name and reads the proto
// files the SDK ships. For your own messages pass `protoRoot`, the directory holding
// your protos.

import * as protobuf from "protobufjs";

import { protoSchemas } from "./protoSchema";

export interface SchemaOptions {
  /** Directory containing your protos; the SDK's bundled tree is always available. */
  protoRoot?: string;
}

function schemaOf(messageType: string, options?: SchemaOptions) {
  return protoSchemas(options?.protoRoot).message(messageType);
}

function lookupMessage(messageType: string, options?: SchemaOptions): protobuf.Type {
  return schemaOf(messageType, options).type;
}

/** The value of one udb option on a message or field, found by the extension's
 *  name; undefined when the annotation is absent. */
function optionOf(holder: protobuf.ReflectionObject, name: string): any {
  const parsed: Array<Record<string, unknown>> = (holder as any).parsedOptions ?? [];
  const suffix = `.${name})`;
  for (const option of parsed) {
    for (const [key, value] of Object.entries(option)) {
      if (key.endsWith(suffix) || key === `(${name})`) return value;
    }
  }
  return undefined;
}

function asList<T>(value: T | T[] | undefined): T[] {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}

const columnOf = (field: protobuf.Field): any => optionOf(field, "column") ?? optionOf(field, "pg_column");
const tableOf = (type: protobuf.Type): any => optionOf(type, "table") ?? optionOf(type, "pg_table");
const tableSecurityOf = (type: protobuf.Type): any => optionOf(type, "db_table_security");

function fieldsWhere(type: protobuf.Type, keep: (field: protobuf.Field) => boolean): string[] {
  return type.fieldsArray.filter(keep).map((field) => field.name);
}

const foldEq = (a: string, b: string) => a.toLowerCase() === b.toLowerCase();

/** The outbox topic an event message declares in its contract ("" when none).
 *  Producers and consumers name topics through this, never as string literals. */
export function topic(messageType: string, options?: SchemaOptions): string {
  return schemaOf(messageType, options).topic;
}

/** The event type an event message's contract declares, else its outbox topic. */
export function eventType(messageType: string, options?: SchemaOptions): string {
  return schemaOf(messageType, options).eventType;
}

/** The payload field an event message's contract names as its partition key ("" when
 *  none). Its value is the outbox envelope's document id. */
export function partitionKeyField(messageType: string, options?: SchemaOptions): string {
  return schemaOf(messageType, options).partitionKeyField;
}

function primaryKeysOf(type: protobuf.Type): string[] {
  return fieldsWhere(type, (field) => Boolean(columnOf(field)?.primary_key));
}

/** The primary-key columns declared on an entity message, in field order. */
export function primaryKeys(messageType: string, options?: SchemaOptions): string[] {
  return primaryKeysOf(lookupMessage(messageType, options));
}

/** The primary-key column of a single-key table: "" when the message declares none
 *  or a composite one, so a caller assuming one key never keys on half of it. */
export function primaryKey(messageType: string, options?: SchemaOptions): string {
  const keys = primaryKeys(messageType, options);
  return keys.length === 1 ? keys[0] : "";
}

// Follows the parser's naming rule: acronym boundaries, separators and repeated
// underscores are handled before comparing descriptor aliases.
function schemaSqlName(raw: string): string {
  const chars = Array.from(raw.trim());
  const upper = (ch: string | undefined) => ch !== undefined && /\p{Uppercase}/u.test(ch);
  const lower = (ch: string | undefined) => ch !== undefined && /\p{Lowercase}/u.test(ch);
  let out = "";
  chars.forEach((ch, i) => {
    if (upper(ch)) {
      if (i > 0 && (!upper(chars[i - 1]) || (i + 1 < chars.length && lower(chars[i + 1])))) out += "_";
      out += ch.toLowerCase();
    } else if (ch === "-" || ch === " ") {
      out += "_";
    } else {
      out += ch;
    }
  });
  return out.replace(/_{2,}/g, "_").replace(/^_+|_+$/g, "");
}

function columnSqlName(field: protobuf.Field): string {
  return schemaSqlName(columnOf(field)?.column_name || field.name);
}

function uniqueKeySetId(key: string[]): string {
  return [...key].sort().join("\u0000");
}

function plainIndexColumn(name: string): boolean {
  return /^[_a-zA-Z][_a-zA-Z0-9]*$/.test(name);
}

// Commas inside expressions or SQL quoted strings do not split a column entry.
function splitIndexColumns(raw: string): string[] {
  const out: string[] = [];
  let depth = 0;
  let start = 0;
  let quoted = false;
  for (let i = 0; i < raw.length; i += 1) {
    const ch = raw[i];
    if (ch === "'") {
      if (quoted && raw[i + 1] === "'") {
        i += 1;
        continue;
      }
      quoted = !quoted;
    } else if (ch === "(" && !quoted) {
      depth += 1;
    } else if (ch === ")" && !quoted) {
      depth -= 1;
    } else if (ch === "," && !quoted && depth === 0) {
      const name = raw.slice(start, i).trim();
      if (name) out.push(name);
      start = i + 1;
    }
  }
  const name = raw.slice(start).trim();
  if (name) out.push(name);
  return out;
}

/** Unconditional unique lookup keys as proto field names. Composite order is kept.
 *  Primary keys, duplicates, partial indexes and expression indexes are excluded.
 *  Partition columns are included because the generated SQL enforces uniqueness
 *  within a partition. Ambiguous or unknown columns throw instead of advertising an
 *  unsafe lookup. Lookup components must be non-null; SQL unique constraints permit
 *  null rows. */
export function uniqueKeys(messageType: string, options?: SchemaOptions): string[][] {
  const type = lookupMessage(messageType, options);
  const resolve = (raw: string): string => {
    const name = raw.trim();
    const matches = type.fieldsArray
      .filter((field) => columnOf(field) && (foldEq(field.name, name) || foldEq(columnSqlName(field), name)))
      .map((field) => field.name);
    if (matches.length !== 1) {
      throw new Error(`udb: ${messageType} unique key column "${name}" resolves to ${matches.length} fields`);
    }
    return matches[0];
  };
  const table = tableOf(type) ?? {};
  let partition = "";
  const strategy = table.partition_strategy;
  if (strategy && strategy !== "PARTITION_STRATEGY_UNSPECIFIED" && strategy !== "PARTITION_STRATEGY_NONE" && table.partition_column) {
    partition = resolve(table.partition_column);
  }
  const effective = (names: string[]): string[] => {
    const key: string[] = [];
    const repeated = new Set<string>();
    for (const name of names) {
      const field = resolve(name);
      if (repeated.has(field)) throw new Error(`udb: ${messageType} unique key repeats field "${field}"`);
      repeated.add(field);
      key.push(field);
    }
    if (partition && !repeated.has(partition)) key.push(partition);
    return key;
  };
  const primaryNames = primaryKeysOf(type);
  const primary = primaryNames.length ? effective(primaryNames) : [];
  const seen = new Set<string>([uniqueKeySetId(primary)]);
  const out: string[][] = [];
  const add = (names: string[]) => {
    if (!names.length) throw new Error(`udb: ${messageType} unique index has no columns`);
    const key = effective(names);
    if (!seen.has(uniqueKeySetId(key))) out.push(key);
  };
  const indexes: Array<{ options: any; local: string }> = asList<any>(table.indexes).map((index) => ({
    options: index,
    local: "",
  }));
  for (const field of type.fieldsArray) {
    const column = columnOf(field);
    if (column?.unique && !column.primary_key) add([field.name]);
    // A field index starts with its owning SQL column and omits explicit
    // repetitions of that same SQL name.
    if (column?.index) indexes.push({ options: column.index, local: columnSqlName(field) });
  }
  const indexName = (index: { options: any }) => String(index.options.index_name ?? "");
  indexes.sort((a, b) => (indexName(a) < indexName(b) ? -1 : indexName(a) > indexName(b) ? 1 : 0));
  for (const { options: index, local } of indexes) {
    if (!index.unique || String(index.where_clause ?? "").trim()) continue;
    const names: string[] = local ? [local] : [];
    let expression = Boolean(local) && !plainIndexColumn(local);
    const entries = [...asList<string>(index.columns), ...asList<string>(index.composite_fields)];
    for (const raw of entries) {
      for (const entry of splitIndexColumns(String(raw))) {
        if (/[(']/.test(entry)) {
          expression = true;
          continue;
        }
        const name = schemaSqlName(entry);
        if (!plainIndexColumn(name)) expression = true;
        if (!local || !foldEq(local, name)) names.push(name);
      }
    }
    if (!expression) add(names);
  }
  const joined = (key: string[]) => key.join("\u0000");
  out.sort((a, b) => (joined(a) < joined(b) ? -1 : joined(a) > joined(b) ? 1 : 0));
  const unique: string[][] = [];
  for (const key of out) {
    const id = uniqueKeySetId(key);
    if (!seen.has(id)) {
      seen.add(id);
      unique.push(key);
    }
  }
  return unique;
}

/** The proto field holding the row's tenant, resolved from table security, its
 *  tenant_column flag, then the broker's conventional tenant names. */
export function tenantColumn(messageType: string, options?: SchemaOptions): string {
  return schemaOf(messageType, options).tenantColumn;
}

/** The proto field holding the row's project, resolved the way the broker does:
 *  table security, the project_column flag, then conventional project names. */
export function projectColumn(messageType: string, options?: SchemaOptions): string {
  return schemaOf(messageType, options).projectColumn;
}

/** The columns annotated owner_field (they drive privacy export and erasure). */
export function ownerColumns(messageType: string, options?: SchemaOptions): string[] {
  return fieldsWhere(lookupMessage(messageType, options), (field) =>
    Boolean(optionOf(field, "db_column_security")?.owner_field),
  );
}

/** The first owner_field column ("" for a tenant-wide table). */
export function ownerColumn(messageType: string, options?: SchemaOptions): string {
  return ownerColumns(messageType, options)[0] ?? "";
}

/** Whether the table's rows belong in a privacy export. */
export function exportEligible(messageType: string, options?: SchemaOptions): boolean {
  return Boolean(tableSecurityOf(lookupMessage(messageType, options))?.export_eligible);
}

/** The table's retention class ("" when it declares none). */
export function retentionClass(messageType: string, options?: SchemaOptions): string {
  return tableSecurityOf(lookupMessage(messageType, options))?.retention_class ?? "";
}

/** Whether the table is declared soft_delete (a delete tombstones its rows). */
export function softDelete(messageType: string, options?: SchemaOptions): boolean {
  return Boolean(tableOf(lookupMessage(messageType, options))?.soft_delete);
}

/** n for a column declared VARCHAR(n); 0 when the field is unknown or has no such bound. */
export function maxLen(messageType: string, field: string, options?: SchemaOptions): number {
  const declared = lookupMessage(messageType, options).fields[field];
  if (!declared) return 0;
  const match = /^VARCHAR\(([+-]?\d+)\)$/.exec(String(columnOf(declared)?.sql_type ?? ""));
  return match ? Number(match[1]) : 0;
}

const INT32_KINDS = new Set(["int32", "sint32", "sfixed32"]);
const INT64_KINDS = new Set(["int64", "sint64", "sfixed64"]);
const BOOL_VALUES: Record<string, boolean> = {
  "1": true,
  t: true,
  T: true,
  TRUE: true,
  true: true,
  True: true,
  "0": false,
  f: false,
  F: false,
  FALSE: false,
  false: false,
  False: false,
};

/** Set every scalar bool and integer field of `row` whose column declares a
 *  default_value to that default, so a row a service creates starts from the table's
 *  own defaults instead of a restated value. A default that does not parse for the
 *  field's kind throws. Keys are proto field names; an int64 outside the safe integer
 *  range is set as a decimal string, the form the SDK reads them in. Returns `row`. */
export function applyColumnDefaults<R extends Record<string, unknown>>(
  messageType: string,
  row: R,
  options?: SchemaOptions,
): R {
  const type = lookupMessage(messageType, options);
  const target = row as Record<string, unknown>;
  for (const field of type.fieldsArray) {
    const def = String(columnOf(field)?.default_value ?? "").trim();
    if (!def || field.repeated || field.map) continue;
    const refuse = () => new Error(`udb: ${messageType}.${field.name} default "${def}" does not parse as ${field.type}`);
    if (field.type === "bool") {
      if (!(def in BOOL_VALUES)) throw refuse();
      target[field.name] = BOOL_VALUES[def];
    } else if (INT32_KINDS.has(field.type) || INT64_KINDS.has(field.type)) {
      if (!/^[+-]?\d+$/.test(def)) throw refuse();
      const value = BigInt(def);
      const bits = INT32_KINDS.has(field.type) ? 31n : 63n;
      if (value < -(1n << bits) || value >= 1n << bits) throw refuse();
      const safe = value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER);
      target[field.name] = safe ? Number(value) : value.toString();
    }
  }
  return row;
}

/** The node label UDB projects an entity under: the graph data_store's node_label (or
 *  udb.neo4j_label), else its resource name, else the entity's table name. */
export function graphLabel(messageType: string, options?: SchemaOptions): string {
  const type = lookupMessage(messageType, options);
  const store = optionOf(type, "data_store");
  if (store && (store.backend === "neo4j" || store.store_kind === "graph")) {
    const declared = asList<any>(store.options);
    const lookup = (key: string): string => {
      for (const entry of declared) {
        if (entry?.key === key && entry.value) return String(entry.value);
        if (entry && !("key" in entry) && entry[key]) return String(entry[key]);
      }
      return "";
    };
    const label = lookup("udb.neo4j_label") || lookup("node_label");
    if (label) return label;
    if (store.resource_name) return store.resource_name;
  }
  return tableOf(type)?.table_name ?? "";
}
