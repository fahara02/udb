// Reads the udb annotations from the bundled .proto tree: what a table's keys,
// owner columns and column rules are, and what an event's topic and partition
// key are. It is the TypeScript counterpart of the Go SDK's schema_options.go,
// so no service restates the schema.

import fs from "node:fs";
import path from "node:path";
// proto-loader registers the well-known files protobufjs does not expose on its
// own (google/protobuf/descriptor.proto, which every annotation extends).
import "@grpc/proto-loader";
import * as protobuf from "protobufjs";

import { defaultProtoRoot } from "./protoRoot";

/** A column annotation, keyed by its proto option names (`udb.core.common.v1.ColumnOptions`). */
export interface ColumnOptions {
  column_name?: string;
  sql_type?: string;
  primary_key?: boolean;
  not_null?: boolean;
  unique?: boolean;
  generated?: boolean;
  exclude_from_insert?: boolean;
  exclude_from_update?: boolean;
  tenant_column?: boolean;
  project_column?: boolean;
  is_json?: boolean;
  is_jsonb?: boolean;
  encrypted?: boolean;
  default_value?: string;
  index?: IndexOptions;
}

/** An index annotation (`udb.core.common.v1.IndexOptions`). */
export interface IndexOptions {
  index_name?: string;
  unique?: boolean;
  where_clause?: string;
  columns?: string[];
  composite_fields?: string[];
}

/** A table annotation (`udb.core.common.v1.TableOptions`). */
export interface TableOptions {
  table_name?: string;
  is_table?: boolean;
  soft_delete?: boolean;
  soft_delete_column?: string;
  indexes?: IndexOptions[];
}

/** An event annotation (`udb.core.common.v1.EventContractOptions`). */
export interface EventContract {
  event_type?: string;
  outbox_topic?: string;
  partition_key_field?: string;
}

const LONG_KINDS = new Set(["int64", "uint64", "sint64", "fixed64", "sfixed64"]);
const INT32_KINDS = new Set(["int32", "uint32", "sint32", "fixed32", "sfixed32"]);

/** One field of a message. `kind` is the scalar type name, or `enum`/`message`. */
export interface FieldSchema {
  name: string;
  jsonName: string;
  kind: string;
  repeated: boolean;
  map: boolean;
  keyKind?: string;
  /** proto3 presence: a message field, a oneof member or an `optional` scalar. */
  hasPresence: boolean;
  /** The name of the real oneof holding this field; absent for `optional`. */
  oneof?: string;
  enumType?: protobuf.Enum;
  messageType?: protobuf.Type;
  /** The `column` annotation, else `pg_column`; undefined when not a column. */
  column?: ColumnOptions;
  masked: boolean;
  pii: boolean;
  encrypted: boolean;
}

function asArray<T>(value: T | T[] | undefined): T[] {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}

/** The annotations on one proto element, by their short extension name. */
function annotations(element: protobuf.ReflectionObject): Record<string, any> {
  const out: Record<string, any> = {};
  const parsed = (element as unknown as { parsedOptions?: Record<string, unknown>[] }).parsedOptions;
  for (const option of parsed ?? []) {
    for (const [key, value] of Object.entries(option)) {
      const match = /^\(([^)]+)\)$/.exec(key);
      if (match) out[match[1].split(".").pop()!] = value;
    }
  }
  return out;
}

function normalizeIndex(raw: any): IndexOptions {
  return { ...raw, columns: asArray<string>(raw.columns), composite_fields: asArray<string>(raw.composite_fields) };
}

function fieldSchema(field: protobuf.Field): FieldSchema {
  field.resolve();
  const notes = annotations(field);
  const raw = notes.column ?? notes.pg_column;
  const column: ColumnOptions | undefined = raw
    ? { ...raw, index: raw.index ? normalizeIndex(raw.index) : undefined }
    : undefined;
  const resolved = field.resolvedType;
  const isMap = field.map === true;
  const repeated = field.repeated === true;
  const enumType = resolved instanceof protobuf.Enum ? resolved : undefined;
  const messageType = resolved instanceof protobuf.Type ? resolved : undefined;
  const group = field.partOf;
  const synthetic = (field.options as Record<string, unknown> | undefined)?.proto3_optional === true;
  const security = notes.db_column_security ?? {};
  const legacy = notes.column_security ?? {};
  const view = security.output_view;
  const redaction = security.redaction_strategy;
  const pii = Boolean(legacy.is_pii || notes.pii);
  return {
    name: field.name,
    jsonName: protobuf.util.camelCase(field.name),
    kind: enumType ? "enum" : messageType ? "message" : field.type,
    repeated,
    map: isMap,
    keyKind: isMap ? (field as unknown as protobuf.MapField).keyType : undefined,
    hasPresence: !repeated && !isMap && (Boolean(messageType) || Boolean(group)),
    oneof: group && !synthetic ? group.name : undefined,
    enumType,
    messageType,
    column,
    pii,
    encrypted: Boolean(
      legacy.is_encrypted || notes.encrypted_security || security.encryption_key_class || security.searchable_encrypted,
    ),
    masked:
      pii ||
      Boolean(legacy.mask_in_logs || notes.log_masked || notes.log_redacted || notes.sensitive) ||
      view === "OUTPUT_VIEW_STORAGE_ONLY" ||
      view === "OUTPUT_VIEW_NEVER" ||
      (redaction !== undefined && redaction !== "REDACTION_STRATEGY_UNSPECIFIED" && redaction !== "REDACTION_STRATEGY_NONE"),
  };
}

/** Whether a field stores a 64-bit integer. */
export function is64BitField(field: FieldSchema): boolean {
  return LONG_KINDS.has(field.kind);
}

/** Whether a field stores a 32-bit integer. */
export function is32BitField(field: FieldSchema): boolean {
  return INT32_KINDS.has(field.kind);
}

const TENANT_CANDIDATES = ["tenant_id", "_tenant_id", "org_id", "institution_id"];
const PROJECT_CANDIDATES = ["project_id", "_project_id"];

/** One message type with its annotations. */
export class MessageSchema {
  readonly fullName: string;
  readonly fields: FieldSchema[];
  readonly table: TableOptions;
  readonly event: EventContract;
  private readonly security: { tenant_column?: string; project_column?: string };
  private readonly byName = new Map<string, FieldSchema>();

  constructor(readonly type: protobuf.Type) {
    this.fullName = type.fullName.replace(/^\./, "");
    this.fields = type.fieldsArray.map(fieldSchema);
    const notes = annotations(type);
    const table = notes.table ?? notes.pg_table ?? {};
    this.table = { ...table, indexes: asArray<any>(table.indexes).map(normalizeIndex) };
    this.event = notes.message_event_contract ?? {};
    this.security = notes.db_table_security ?? {};
    for (const field of this.fields) {
      this.byName.set(field.name, field);
      this.byName.set(field.jsonName, field);
    }
  }

  /** The field with this proto name or JSON (camelCase) name. */
  field(name: string): FieldSchema | undefined {
    return this.byName.get(name);
  }

  /** The primary-key columns, in field order. */
  get primaryKeys(): string[] {
    return this.fields.filter((field) => field.column?.primary_key).map((field) => field.name);
  }

  /** The outbox topic an event declares ("" when none). */
  get topic(): string {
    return this.event.outbox_topic ?? "";
  }

  /** The event type an event declares, else its outbox topic. */
  get eventType(): string {
    return this.event.event_type || this.topic;
  }

  /** The payload field an event names as its partition key ("" when none). */
  get partitionKeyField(): string {
    return this.event.partition_key_field ?? "";
  }

  /** The proto field holding the row's tenant ("" when the table has none). */
  get tenantColumn(): string {
    return this.scopeColumn(this.security.tenant_column, "tenant_column", TENANT_CANDIDATES);
  }

  /** The proto field holding the row's project ("" when the table has none). */
  get projectColumn(): string {
    return this.scopeColumn(this.security.project_column, "project_column", PROJECT_CANDIDATES);
  }

  private scopeColumn(declaredRaw: string | undefined, flag: "tenant_column" | "project_column", candidates: string[]): string {
    const declared = (declaredRaw ?? "").trim();
    if (declared) {
      const found = this.fields.find(
        (field) => field.column && (field.name === declared || (field.column.column_name ?? "").toLowerCase() === declared),
      );
      if (found) return found.name;
    }
    const flagged = this.fields.find((field) => field.column?.[flag]);
    if (flagged) return flagged.name;
    const named = this.fields.find(
      (field) =>
        field.column &&
        candidates.some(
          (name) =>
            field.name.toLowerCase() === name.toLowerCase() ||
            (field.column?.column_name ?? "").toLowerCase() === name.toLowerCase(),
        ),
    );
    return named?.name ?? "";
  }
}

function protoFiles(dir: string): string[] {
  const out: string[] = [];
  const walk = (current: string) => {
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) walk(full);
      else if (entry.name.endsWith(".proto")) out.push(full);
    }
  };
  walk(dir);
  return out;
}

const SCALAR_TYPES: Record<string, string> = {
  TYPE_DOUBLE: "double",
  TYPE_FLOAT: "float",
  TYPE_INT64: "int64",
  TYPE_UINT64: "uint64",
  TYPE_INT32: "int32",
  TYPE_FIXED64: "fixed64",
  TYPE_FIXED32: "fixed32",
  TYPE_BOOL: "bool",
  TYPE_STRING: "string",
  TYPE_BYTES: "bytes",
  TYPE_UINT32: "uint32",
  TYPE_SFIXED32: "sfixed32",
  TYPE_SFIXED64: "sfixed64",
  TYPE_SINT32: "sint32",
  TYPE_SINT64: "sint64",
};

/** A decoded options message's udb annotations, in the form the .proto parser gives
 *  them. The bundled tree declares the extensions, so they decode by field number. */
function setAnnotations(element: protobuf.ReflectionObject, options: Record<string, unknown> | undefined): void {
  const parsed = Object.entries(options ?? {})
    .filter(([key]) => key.startsWith("."))
    .map(([key, value]) => ({ [`(${key.slice(1)})`]: value }));
  if (parsed.length > 0) (element as unknown as { parsedOptions: unknown }).parsedOptions = parsed;
}

function fieldType(field: any): string {
  return SCALAR_TYPES[field.type] ?? field.typeName;
}

function addEnum(scope: protobuf.NamespaceBase, descriptor: any): void {
  const values: Record<string, number> = {};
  for (const value of descriptor.value ?? []) values[value.name] = value.number ?? 0;
  scope.add(new protobuf.Enum(descriptor.name, values));
}

function addMessage(scope: protobuf.NamespaceBase, descriptor: any): void {
  const type = new protobuf.Type(descriptor.name);
  setAnnotations(type, descriptor.options);
  const nested: any[] = descriptor.nestedType ?? [];
  const entries = new Map(nested.filter((item) => item.options?.mapEntry).map((item) => [item.name, item]));
  for (const item of nested) if (!item.options?.mapEntry) addMessage(type, item);
  for (const item of descriptor.enumType ?? []) addEnum(type, item);
  const groups = (descriptor.oneofDecl ?? []).map((oneof: any) => new protobuf.OneOf(oneof.name));
  const used = new Set<protobuf.OneOf>();
  for (const raw of descriptor.field ?? []) {
    const entry = raw.type === "TYPE_MESSAGE" ? entries.get(String(raw.typeName).split(".").pop()) : undefined;
    let field: protobuf.Field;
    if (entry) {
      const [key, value] = entry.field;
      field = new protobuf.MapField(raw.name, raw.number, fieldType(key), fieldType(value)) as unknown as protobuf.Field;
    } else {
      field = new protobuf.Field(raw.name, raw.number, fieldType(raw), raw.label === "LABEL_REPEATED" ? "repeated" : undefined);
    }
    setAnnotations(field, raw.options);
    if (raw.proto3Optional) field.setOption("proto3_optional", true);
    type.add(field);
    if (raw.oneofIndex !== undefined && raw.oneofIndex !== null) {
      groups[raw.oneofIndex].add(field);
      used.add(groups[raw.oneofIndex]);
    }
  }
  for (const group of used) type.add(group);
  scope.add(type);
}

function rootFromDescriptorSet(bytes: Uint8Array): protobuf.Root {
  const setType = protoSchemas().protoType("google.protobuf.FileDescriptorSet");
  const root = new protobuf.Root();
  try {
    const set: any = setType.toObject(setType.decode(bytes), { enums: String, longs: String });
    for (const file of set.file ?? []) {
      const scope = file.package ? root.define(file.package) : root;
      for (const item of file.enumType ?? []) addEnum(scope, item);
      for (const item of file.messageType ?? []) addMessage(scope, item);
    }
    root.resolveAll();
  } catch (err) {
    throw new Error(`udbtest: descriptor set: ${(err as Error).message}`);
  }
  return root;
}

/** The message types of one proto tree: the SDK's own `udb/**` files plus, when
 *  `protoRoot` names an application's directory, that application's messages. */
export class ProtoSchemas {
  private root?: protobuf.Root;
  private readonly cache = new Map<string, MessageSchema>();

  constructor(readonly protoRoot?: string) {}

  /** The message types of a compiled `google.protobuf.FileDescriptorSet`, with their
   *  udb annotations. Every import must be in the set; a missing one throws. */
  static fromDescriptorSet(bytes: Uint8Array): ProtoSchemas {
    const schemas = new ProtoSchemas();
    schemas.root = rootFromDescriptorSet(bytes);
    return schemas;
  }

  private load(): protobuf.Root {
    if (this.root) return this.root;
    const bundled = defaultProtoRoot();
    const appRoot = this.protoRoot ? path.resolve(this.protoRoot) : undefined;
    const includeDirs = [
      ...(appRoot ? [appRoot] : []),
      bundled,
      path.resolve(appRoot ?? bundled, "../third_party/googleapis"),
      path.resolve(bundled, "../third_party/googleapis"),
    ];
    const root = new protobuf.Root();
    const fallback = root.resolvePath.bind(root);
    root.resolvePath = (origin, target) => {
      if (path.isAbsolute(target)) return target;
      for (const dir of includeDirs) {
        const full = path.join(dir, target);
        if (fs.existsSync(full)) return full;
      }
      return fallback(origin, target);
    };
    // An application tree that carries its own udb/ copy replaces the bundled one.
    const appHasUdb = appRoot !== undefined && fs.existsSync(path.join(appRoot, "udb"));
    const files = [
      ...(appRoot ? protoFiles(appRoot) : []),
      ...(appHasUdb ? [] : protoFiles(path.join(bundled, "udb"))),
    ];
    root.loadSync(files, { keepCase: true });
    root.resolveAll();
    this.root = root;
    return root;
  }

  /** The named message, or undefined when this tree does not declare it. */
  find(messageType: string): MessageSchema | undefined {
    const cached = this.cache.get(messageType);
    if (cached) return cached;
    const found = this.load().lookup(messageType);
    if (!(found instanceof protobuf.Type)) return undefined;
    const schema = new MessageSchema(found);
    this.cache.set(messageType, schema);
    return schema;
  }

  /** The named message; an unknown name throws. */
  message(messageType: string): MessageSchema {
    const schema = this.find(messageType);
    if (!schema) throw new Error(`udb: ${messageType} is not declared in the loaded protos`);
    return schema;
  }

  /** The named protobuf type itself, for encoding a message on the wire. */
  protoType(typeName: string): protobuf.Type {
    return this.load().lookupType(typeName);
  }

  /** Every message annotated as a table. */
  tables(): MessageSchema[] {
    const out: MessageSchema[] = [];
    const visit = (scope: protobuf.NamespaceBase) => {
      for (const nested of scope.nestedArray) {
        if (nested instanceof protobuf.Type) {
          const schema = this.find(nested.fullName.replace(/^\./, ""));
          if (schema?.table.is_table) out.push(schema);
        }
        if (nested instanceof protobuf.Namespace) visit(nested);
      }
    };
    visit(this.load());
    return out;
  }
}

const BY_ROOT = new Map<string, ProtoSchemas>();

/** The shared schema set of a proto root (the bundled tree when omitted). */
export function protoSchemas(protoRoot?: string): ProtoSchemas {
  const key = protoRoot ? path.resolve(protoRoot) : "";
  let schemas = BY_ROOT.get(key);
  if (!schemas) {
    schemas = new ProtoSchemas(protoRoot);
    BY_ROOT.set(key, schemas);
  }
  return schemas;
}
