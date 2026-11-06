// What the broker enforces per entity, read from the message's annotations.

import { randomUUID } from "node:crypto";
import { status as codes } from "@grpc/grpc-js";

import { FieldSchema, IndexOptions, MessageSchema, ProtoSchemas } from "../protoSchema";
import { RawJson, decodeField, encodeField, parseExactJson, recordToJson } from "../recordCodec";
import { Row, equal, validateFilter } from "./filter";
import { refuse } from "./refusal";

/** A broker-verified identity, provisioned with an API key. Request headers cannot
 *  add scopes or roles, or change its tenant, project or owner. */
export interface Identity {
  tenantId: string;
  projectId?: string;
  subject?: string;
  userId?: string;
  serviceIdentity?: string;
  scopes?: string[];
  roles?: string[];
}

interface UniqueKey {
  name: string;
  columns: string[];
  where: string;
}

export function columnRefusal(column: string) {
  return refuse(codes.INVALID_ARGUMENT, "UDB_NOT_NULL_VIOLATION", `required column is null: ${column}`, {
    reason: "UDB_NOT_NULL_VIOLATION",
    kind: "ERROR_KIND_NOT_NULL",
    column,
  });
}

export function uniqueRefusal(name: string) {
  return refuse(codes.ALREADY_EXISTS, "UDB_UNIQUE_VIOLATION", "write conflicts with a declared unique key", {
    reason: "UDB_UNIQUE_VIOLATION",
    kind: "ERROR_KIND_UNIQUE",
    constraint: name,
  });
}

export function integerColumn(field: FieldSchema | undefined): boolean {
  const sql = (field?.column?.sql_type ?? "").trim().toUpperCase();
  return ["SMALLINT", "INTEGER", "INT", "INT2", "INT4", "INT8", "BIGINT", "SERIAL", "BIGSERIAL"].includes(sql);
}

function jsonColumn(field: FieldSchema): boolean {
  const column = field.column;
  return Boolean(column?.is_json || column?.is_jsonb || (column?.sql_type ?? "").trim().toUpperCase().startsWith("JSON"));
}

/** A string written to a JSON column is JSON text, as on the broker, and the column holds the
 *  document it encodes. */
function jsonColumnDocument(name: string, text: string): unknown {
  let document: unknown;
  try {
    document = parseExactJson(text);
  } catch {
    throw refuse(codes.INVALID_ARGUMENT, "", `value for JSON column ${name} is not valid JSON: send a JSON document, or null to clear it`);
  }
  if (document === null) {
    throw refuse(codes.INVALID_ARGUMENT, "", `the text null is not accepted for JSON column ${name}: send a real null to clear it`);
  }
  return document;
}

function defaultValue(expression: string): unknown {
  const [rawBase, cast = ""] = expression.split("::");
  const base = rawBase.trim();
  const lower = base.toLowerCase();
  if (lower === "gen_random_uuid()" || lower === "uuid_generate_v4()") return randomUUID();
  if (lower === "now()" || lower === "current_timestamp") return new Date().toISOString();
  if (lower === "null") return null;
  if (lower === "true" || lower === "false") return lower === "true";
  if (base.length >= 2 && base.startsWith("'") && base.endsWith("'")) {
    const value = base.slice(1, -1).replace(/''/g, "'");
    return cast.trim().toLowerCase().startsWith("json") ? parseExactJson(value) : value;
  }
  if (/^[+-]?\d+$/.test(base)) {
    const number = Number(base);
    return Number.isSafeInteger(number) ? number : BigInt(base);
  }
  if (/^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(base)) return Number(base);
  throw new Error(`unsupported SQL default "${expression}"; use start() for database expressions`);
}

/** Copy a stored value so a caller or later write cannot alias it. */
export function copyValue<T>(value: T): T {
  if (Array.isArray(value)) return value.map(copyValue) as unknown as T;
  if (Buffer.isBuffer(value)) return Buffer.from(value) as unknown as T;
  if (value && typeof value === "object" && !(value instanceof RawJson)) {
    const out: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value)) {
      Object.defineProperty(out, key, { value: copyValue(item), enumerable: true, configurable: true, writable: true });
    }
    return out as T;
  }
  return value;
}

/** The rules of one entity: columns, aliases, unique keys, soft delete and masking. */
export class EntityRules {
  readonly messageType: string;
  readonly primaryKey: string[];
  readonly tenantColumn: string;
  readonly projectColumn: string;
  readonly columns = new Map<string, FieldSchema>();
  readonly aliases = new Map<string, string>();
  readonly unique: UniqueKey[] = [];
  readonly softDelete: string = "";
  readonly tableName: string;
  schemaError = "";

  constructor(
    readonly schema: MessageSchema,
    private readonly schemas: ProtoSchemas,
  ) {
    this.messageType = schema.fullName;
    this.primaryKey = schema.primaryKeys;
    this.tenantColumn = schema.tenantColumn;
    this.projectColumn = schema.projectColumn;
    this.tableName = schema.table.table_name || schema.type.name;
    for (const field of schema.fields) {
      if (!field.column) continue;
      this.columns.set(field.name, field);
      this.aliases.set(field.name.toLowerCase(), field.name);
      if (field.column.column_name) this.aliases.set(field.column.column_name.toLowerCase(), field.name);
    }
    for (const field of this.columns.values()) {
      if (field.column?.unique) {
        this.unique.push({ name: `${this.tableName}_${field.name}_key`, columns: [field.name], where: "" });
      }
      if (field.column?.index?.unique) this.addUnique(field.column.index, field.name);
    }
    for (const index of schema.table.indexes ?? []) {
      if (index.unique) this.addUnique(index, "");
    }
    if (schema.table.soft_delete) {
      const declared = schema.table.soft_delete_column || "deleted_at";
      const name = this.aliases.get(declared.toLowerCase());
      if (name) this.softDelete = name;
      else this.schemaError = "soft_delete_column is not a declared column";
    }
  }

  private addUnique(index: IndexOptions, fallback: string): void {
    let declared = index.columns?.length ? index.columns : index.composite_fields ?? [];
    if (declared.length === 0 && fallback) declared = [fallback];
    const columns: string[] = [];
    for (const column of declared) {
      const name = this.aliases.get(column.toLowerCase());
      if (!name) {
        this.schemaError = `unique index refers to an unknown column: ${column}`;
        return;
      }
      columns.push(name);
    }
    if (columns.length === 0) {
      this.schemaError = "unique index has no columns";
      return;
    }
    const key: UniqueKey = {
      name: index.index_name || `${this.tableName}_${columns.join("_")}_key`,
      columns,
      where: (index.where_clause ?? "").trim(),
    };
    // A fake must never silently drop a partial uniqueness predicate. The common
    // soft-delete predicates are supported; other SQL needs start().
    if (key.where) {
      try {
        this.inUnique(key, {});
      } catch (err) {
        this.schemaError = (err as Error).message;
        return;
      }
    }
    this.unique.push(key);
  }

  inUnique(key: UniqueKey, row: Row): boolean {
    if (!key.where) return true;
    const parts = key.where.split(/\s+/);
    const isNot = parts.length === 4 && parts[2].toUpperCase() === "NOT";
    if ((parts.length === 3 || isNot) && parts[1].toUpperCase() === "IS" && parts[parts.length - 1].toUpperCase() === "NULL") {
      const name = this.aliases.get(parts[0].replace(/"/g, ""));
      if (name) return (row[name] === null || row[name] === undefined) === !isNot;
    }
    throw new Error(`udbtest: unsupported unique-index predicate "${key.where}"; use start() for SQL expressions`);
  }

  /** Coerce a record through the SDK's own codec, so a value the codec refuses is
   *  refused here and the stored shape is what the broker would store. */
  normalize(record: Row): Row {
    if (this.schemaError) throw refuse(codes.FAILED_PRECONDITION, "", this.schemaError);
    const out: Row = {};
    for (const [column, value] of Object.entries(record)) {
      const name = this.aliases.get(column.toLowerCase());
      if (!name) throw refuse(codes.INVALID_ARGUMENT, "", `unknown column ${column}`);
      if (name in out) throw refuse(codes.INVALID_ARGUMENT, "", `column supplied through two aliases: ${name}`);
      const field = this.columns.get(name)!;
      if (field.masked && (value === "***MASKED***" || value === "***REDACTED***")) {
        throw refuse(codes.INVALID_ARGUMENT, "UDB_REDACTED_VALUE_WRITE", "cannot write a redacted placeholder", {
          reason: "UDB_REDACTED_VALUE_WRITE",
          kind: "ERROR_KIND_REDACTED",
          column: name,
        });
      }
      if (typeof value === "number" && integerColumn(field) && (Math.abs(value) > 2 ** 53 || Math.trunc(value) !== value)) {
        throw refuse(codes.INVALID_ARGUMENT, "", `integer column requires an exact number or decimal string: ${name}`);
      }
      if (value === null || value === undefined) {
        out[name] = null;
        continue;
      }
      if (typeof value === "string" && jsonColumn(field)) {
        out[name] = jsonColumnDocument(name, value);
        continue;
      }
      const message: Row = {};
      let encoded: { value: unknown; present: boolean };
      try {
        decodeField(this.messageType, message, name, value, this.schemas);
        encoded = encodeField(this.messageType, message, name, this.schemas);
      } catch (err) {
        throw refuse(codes.INVALID_ARGUMENT, "", (err as Error).message);
      }
      if (encoded.present) {
        out[name] = encoded.value instanceof RawJson ? parseExactJson(encoded.value.text) : copyValue(encoded.value);
      }
    }
    return out;
  }

  defaults(record: Row): void {
    for (const [name, field] of this.columns) {
      if (name in record) continue;
      const expression = (field.column?.default_value ?? "").trim();
      if (!expression) continue;
      try {
        record[name] = defaultValue(expression);
      } catch (err) {
        throw refuse(codes.FAILED_PRECONDITION, "", `udbtest: default for ${name}: ${(err as Error).message}`);
      }
    }
  }

  keyOf(record: Row): string {
    if (this.primaryKey.length === 0) {
      throw refuse(codes.FAILED_PRECONDITION, "", `${this.messageType} declares no primary key`);
    }
    const parts = this.primaryKey.map((column) => {
      const value = record[column];
      if (value === undefined || value === null || value === "") throw columnRefusal(column);
      return value;
    });
    return recordToJson(parts);
  }

  live(row: Row): boolean {
    return !this.softDelete || row[this.softDelete] === null || row[this.softDelete] === undefined;
  }

  visibleTo(row: Row, identity: Identity): boolean {
    return !this.projectColumn || !identity.projectId || row[this.projectColumn] === identity.projectId;
  }

  /** Resolve every column at plan time, including nested boolean branches: an unknown
   *  column must never look like a successful zero-row query. */
  queryFilter(filter: Row): Row {
    validateFilter(filter);
    const out: Row = {};
    for (const [field, condition] of Object.entries(filter)) {
      const name = field.trim().toLowerCase();
      if (name === "$and" || name === "$or") {
        if (name in out) throw refuse(codes.INVALID_ARGUMENT, "", `boolean filter supplied through two aliases: ${name}`);
        out[name] = (condition as Row[]).map((child) => this.queryFilter(child));
        continue;
      }
      if (name === "tenant_id" && !this.tenantColumn) {
        throw refuse(codes.INVALID_ARGUMENT, "UDB_TABLE_NOT_TENANT_SCOPED", "the entity has no tenant column");
      }
      const column = this.aliases.get(name);
      if (!column) throw refuse(codes.INVALID_ARGUMENT, "", `unknown filter column ${field}`);
      if (column in out) throw refuse(codes.INVALID_ARGUMENT, "", `filter column supplied through two aliases: ${column}`);
      out[column] = this.canonicalOperators(condition);
    }
    return out;
  }

  private canonicalOperators(value: unknown): unknown {
    if (typeof value !== "object" || value === null || Array.isArray(value)) return copyValue(value);
    const out: Row = {};
    for (const [op, arg] of Object.entries(value)) {
      const name = op.toLowerCase();
      if (name in out) throw refuse(codes.INVALID_ARGUMENT, "", `filter operator supplied through two aliases: ${name}`);
      out[name] = name === "$not" ? this.canonicalOperators(arg) : copyValue(arg);
    }
    return out;
  }

  expectedColumns(values: Row): Row {
    const out: Row = {};
    for (const [field, value] of Object.entries(values)) {
      const column = this.aliases.get(field.trim().toLowerCase());
      if (!column) throw refuse(codes.INVALID_ARGUMENT, "", `unknown expected column ${field}`);
      if (column in out) throw refuse(codes.INVALID_ARGUMENT, "", `expected column supplied through two aliases: ${column}`);
      out[column] = copyValue(value);
    }
    return out;
  }

  conflictFields(fields: readonly string[]): void {
    if (fields.length === 0) return;
    const seen = new Set<string>();
    for (const field of fields) {
      const column = this.aliases.get(field.trim().toLowerCase());
      if (!column || seen.has(column)) throw refuse(codes.INVALID_ARGUMENT, "", `unknown or duplicate conflict column ${field}`);
      seen.add(column);
    }
    if (seen.size !== this.primaryKey.length || !this.primaryKey.every((column) => seen.has(column))) {
      throw refuse(codes.FAILED_PRECONDITION, "", "udbtest: alternate Upsert conflict_fields require start(); they are never ignored");
    }
  }

  /** A primary key, tenant or project is immutable through Update, even when a
   *  caller supplies its current value. */
  updateColumns(changes: Row, increments: readonly { column: string }[]): void {
    if (Object.keys(changes).length === 0 && increments.length === 0) {
      throw refuse(codes.INVALID_ARGUMENT, "", "update requires at least one change or increment");
    }
    const resolve = (field: string) => {
      const name = this.aliases.get(field.trim().toLowerCase());
      if (!name) throw refuse(codes.INVALID_ARGUMENT, "", `unknown update column ${field}`);
      if (name === this.tenantColumn || name === this.projectColumn || this.primaryKey.includes(name)) {
        throw refuse(codes.INVALID_ARGUMENT, "", `column ${name} is immutable through Update (primary key / tenant / project); use Upsert for identity changes`);
      }
      return name;
    };
    const changed = new Set(Object.keys(changes).map(resolve));
    for (const increment of increments) {
      const name = resolve(increment.column);
      if (changed.has(name)) throw refuse(codes.INVALID_ARGUMENT, "", `column ${name} appears in both changes and increments`);
    }
  }

  /** CAS must pin an entire declared key, even when another filter matches one row today. */
  validateCasKey(filter: Row): void {
    const pins = new Set<string>();
    const collect = (part: Row) => {
      for (const [name, raw] of Object.entries(part)) {
        if (name.toLowerCase() === "$and") {
          (raw as Row[]).forEach(collect);
          continue;
        }
        let value = raw;
        if (typeof raw === "object" && raw !== null && !Array.isArray(raw)) {
          value = null;
          for (const [operator, assertion] of Object.entries(raw)) {
            if (operator.toLowerCase() === "$eq") value = assertion;
          }
        }
        if (value !== null && value !== undefined && !name.startsWith("$")) pins.add(name);
      }
    };
    collect(filter);
    const keys = [this.primaryKey, ...this.unique.filter((unique) => !unique.where).map((unique) => unique.columns)];
    if (keys.some((key) => key.length > 0 && key.every((name) => pins.has(name)))) return;
    throw refuse(codes.FAILED_PRECONDITION, "UDB_CAS_KEY_NOT_PK", "CAS requires equality on every column of a primary or unique key");
  }

  /** A filter that pins a different tenant is refused; one without is scoped to the caller. */
  checkTenant(filter: Row, tenant: string): void {
    if (!this.tenantColumn) {
      if ("tenant_id" in filter) throw refuse(codes.INVALID_ARGUMENT, "UDB_TABLE_NOT_TENANT_SCOPED", "the entity has no tenant column");
      return;
    }
    let value = filter[this.tenantColumn];
    if (typeof value === "object" && value !== null && Object.keys(value).length === 1) value = value.$eq;
    if (typeof value === "string" && value !== tenant) {
      throw refuse(codes.PERMISSION_DENIED, "UDB_TENANT_MISMATCH", "the request names a different tenant than the caller's verified tenant");
    }
  }

  bindRecordScope(record: Row, identity: Identity): void {
    if (this.tenantColumn) {
      const given = record[this.tenantColumn];
      if (typeof given === "string" && given !== "" && given !== identity.tenantId) {
        throw refuse(codes.PERMISSION_DENIED, "UDB_TENANT_MISMATCH", "the record names a different tenant than the caller's");
      }
      record[this.tenantColumn] = identity.tenantId;
    }
    if (this.projectColumn && identity.projectId) {
      const given = record[this.projectColumn];
      if (given !== null && given !== undefined && given !== identity.projectId) {
        throw refuse(codes.INVALID_ARGUMENT, "", "record project_id must match RequestContext.project_id");
      }
      record[this.projectColumn] = identity.projectId;
    }
  }

  /** The conflict guard compares every supplied isolation column with the existing
   *  row; it never reassigns ownership. */
  sameRecordScope(existing: Row, incoming: Row): boolean {
    for (const column of [this.tenantColumn, this.projectColumn]) {
      if (column && column in incoming && !equal(existing[column] ?? null, incoming[column])) return false;
    }
    return true;
  }

  /** The write filter with the verified scope filled in, for hashing a keyed write.
   *  A filter holding only scope is not expanded. */
  scopedWriteFilter(filter: Row, identity: Identity): Row {
    const out = copyValue(filter);
    const scope: Record<string, string> = {};
    if (this.tenantColumn) scope[this.tenantColumn] = identity.tenantId;
    if (this.projectColumn && identity.projectId) scope[this.projectColumn] = identity.projectId;
    const referenced = new Set<string>();
    const visit = (part: Row) => {
      for (const [field, value] of Object.entries(part)) {
        if (!field.startsWith("$")) {
          referenced.add(field);
        } else if (Array.isArray(value)) {
          for (const child of value) if (child && typeof child === "object") visit(child);
        } else if (value && typeof value === "object") {
          visit(value);
        }
      }
    };
    visit(out);
    if ([...referenced].some((column) => !(column in scope))) {
      for (const [column, value] of Object.entries(scope)) {
        if (!referenced.has(column)) out[column] = value;
      }
    }
    return out;
  }

  /** The broker's integer wire shape: an exact JSON number through 2^53, then a
   *  decimal string. Storage keeps the exact value on either side. */
  wireValue(name: string, value: unknown): unknown {
    if (value === null || value === undefined) return null;
    const field = this.columns.get(name)!;
    if (!["int64", "uint64", "sint64", "fixed64", "sfixed64"].includes(field.kind)) return value;
    const convert = (item: unknown) => {
      if (typeof item !== "number" && typeof item !== "bigint" && !(typeof item === "string" && /^-?\d+$/.test(item))) return item;
      const exact = BigInt(item as number | bigint | string);
      const absolute = exact < 0n ? -exact : exact;
      return absolute > 1n << 53n ? exact.toString() : Number(exact);
    };
    return Array.isArray(value) && field.repeated ? value.map(convert) : convert(value);
  }

  /** The stored value after `delta`. SQL NULL + delta is NULL. */
  incrementValue(name: string, value: unknown, delta: number): unknown {
    if (value === null || value === undefined) return null;
    const field = this.columns.get(name);
    if (!Number.isFinite(delta)) throw refuse(codes.INVALID_ARGUMENT, "", "increment requires a finite numeric column and delta");
    const integer = integerColumn(field);
    const textual = typeof value === "string" ? value.trim() : value;
    if (integer) {
      if (!Number.isInteger(delta)) throw refuse(codes.INVALID_ARGUMENT, "", "integer increment is fractional or out of range");
      let current: bigint;
      try {
        current = BigInt(textual as number | bigint | string);
      } catch {
        throw refuse(codes.INVALID_ARGUMENT, "", "increment requires a finite numeric column and delta");
      }
      const result = current + BigInt(delta);
      if (result > (1n << 63n) - 1n || result < -(1n << 63n)) {
        throw refuse(codes.INVALID_ARGUMENT, "", "integer increment is fractional or out of range");
      }
      return Number.isSafeInteger(Number(result)) ? Number(result) : result;
    }
    const current = Number(textual);
    if (!Number.isFinite(current)) throw refuse(codes.INVALID_ARGUMENT, "", "increment requires a finite numeric column and delta");
    return current + delta;
  }
}
