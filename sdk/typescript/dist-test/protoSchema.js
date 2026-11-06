"use strict";
// Reads the udb annotations from the bundled .proto tree: what a table's keys,
// owner columns and column rules are, and what an event's topic and partition
// key are. It is the TypeScript counterpart of the Go SDK's schema_options.go,
// so no service restates the schema.
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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ProtoSchemas = exports.MessageSchema = void 0;
exports.is64BitField = is64BitField;
exports.is32BitField = is32BitField;
exports.protoSchemas = protoSchemas;
const node_fs_1 = __importDefault(require("node:fs"));
const node_path_1 = __importDefault(require("node:path"));
// proto-loader registers the well-known files protobufjs does not expose on its
// own (google/protobuf/descriptor.proto, which every annotation extends).
require("@grpc/proto-loader");
const protobuf = __importStar(require("protobufjs"));
const protoRoot_1 = require("./protoRoot");
const LONG_KINDS = new Set(["int64", "uint64", "sint64", "fixed64", "sfixed64"]);
const INT32_KINDS = new Set(["int32", "uint32", "sint32", "fixed32", "sfixed32"]);
function asArray(value) {
    if (value === undefined || value === null)
        return [];
    return Array.isArray(value) ? value : [value];
}
/** The annotations on one proto element, by their short extension name. */
function annotations(element) {
    const out = {};
    const parsed = element.parsedOptions;
    for (const option of parsed ?? []) {
        for (const [key, value] of Object.entries(option)) {
            const match = /^\(([^)]+)\)$/.exec(key);
            if (match)
                out[match[1].split(".").pop()] = value;
        }
    }
    return out;
}
function normalizeIndex(raw) {
    return { ...raw, columns: asArray(raw.columns), composite_fields: asArray(raw.composite_fields) };
}
function fieldSchema(field) {
    field.resolve();
    const notes = annotations(field);
    const raw = notes.column ?? notes.pg_column;
    const column = raw
        ? { ...raw, index: raw.index ? normalizeIndex(raw.index) : undefined }
        : undefined;
    const resolved = field.resolvedType;
    const isMap = field.map === true;
    const repeated = field.repeated === true;
    const enumType = resolved instanceof protobuf.Enum ? resolved : undefined;
    const messageType = resolved instanceof protobuf.Type ? resolved : undefined;
    const group = field.partOf;
    const synthetic = field.options?.proto3_optional === true;
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
        keyKind: isMap ? field.keyType : undefined,
        hasPresence: !repeated && !isMap && (Boolean(messageType) || Boolean(group)),
        oneof: group && !synthetic ? group.name : undefined,
        enumType,
        messageType,
        column,
        pii,
        encrypted: Boolean(legacy.is_encrypted || notes.encrypted_security || security.encryption_key_class || security.searchable_encrypted),
        masked: pii ||
            (view !== "OUTPUT_VIEW_PUBLIC" && SENSITIVE_CLASSIFICATIONS.has(security.secret_classification)) ||
            Boolean(legacy.mask_in_logs || notes.log_masked || notes.log_redacted || notes.sensitive) ||
            view === "OUTPUT_VIEW_STORAGE_ONLY" ||
            view === "OUTPUT_VIEW_NEVER" ||
            (redaction !== undefined && redaction !== "REDACTION_STRATEGY_UNSPECIFIED" && redaction !== "REDACTION_STRATEGY_NONE"),
    };
}
/** Classifications that keep a column from a client in clear text unless its view is public. */
const SENSITIVE_CLASSIFICATIONS = new Set([
    "SECRET_CLASSIFICATION_CREDENTIAL",
    "SECRET_CLASSIFICATION_TOKEN",
    "SECRET_CLASSIFICATION_KEY",
    "SECRET_CLASSIFICATION_PRIVATE_KEY",
    "SECRET_CLASSIFICATION_BIOMETRIC",
    "SECRET_CLASSIFICATION_IDENTITY",
    "SECRET_CLASSIFICATION_PII",
]);
/** Whether a field stores a 64-bit integer. */
function is64BitField(field) {
    return LONG_KINDS.has(field.kind);
}
/** Whether a field stores a 32-bit integer. */
function is32BitField(field) {
    return INT32_KINDS.has(field.kind);
}
const TENANT_CANDIDATES = ["tenant_id", "_tenant_id", "org_id", "institution_id"];
const PROJECT_CANDIDATES = ["project_id", "_project_id"];
/** One message type with its annotations. */
class MessageSchema {
    type;
    fullName;
    fields;
    table;
    event;
    security;
    byName = new Map();
    constructor(type) {
        this.type = type;
        this.fullName = type.fullName.replace(/^\./, "");
        this.fields = type.fieldsArray.map(fieldSchema);
        const notes = annotations(type);
        const table = notes.table ?? notes.pg_table ?? {};
        this.table = { ...table, indexes: asArray(table.indexes).map(normalizeIndex) };
        this.event = notes.message_event_contract ?? {};
        this.security = notes.db_table_security ?? {};
        for (const field of this.fields) {
            this.byName.set(field.name, field);
            this.byName.set(field.jsonName, field);
        }
    }
    /** The field with this proto name or JSON (camelCase) name. */
    field(name) {
        return this.byName.get(name);
    }
    /** The primary-key columns, in field order. */
    get primaryKeys() {
        return this.fields.filter((field) => field.column?.primary_key).map((field) => field.name);
    }
    /** The outbox topic an event declares ("" when none). */
    get topic() {
        return this.event.outbox_topic ?? "";
    }
    /** The event type an event declares, else its outbox topic. */
    get eventType() {
        return this.event.event_type || this.topic;
    }
    /** The payload field an event names as its partition key ("" when none). */
    get partitionKeyField() {
        return this.event.partition_key_field ?? "";
    }
    /** The proto field holding the row's tenant ("" when the table has none). */
    get tenantColumn() {
        return this.scopeColumn(this.security.tenant_column, "tenant_column", TENANT_CANDIDATES);
    }
    /** The proto field holding the row's project ("" when the table has none). */
    get projectColumn() {
        return this.scopeColumn(this.security.project_column, "project_column", PROJECT_CANDIDATES);
    }
    scopeColumn(declaredRaw, flag, candidates) {
        const declared = (declaredRaw ?? "").trim();
        if (declared) {
            const found = this.fields.find((field) => field.column && (field.name === declared || (field.column.column_name ?? "").toLowerCase() === declared));
            if (found)
                return found.name;
        }
        const flagged = this.fields.find((field) => field.column?.[flag]);
        if (flagged)
            return flagged.name;
        const named = this.fields.find((field) => field.column &&
            candidates.some((name) => field.name.toLowerCase() === name.toLowerCase() ||
                (field.column?.column_name ?? "").toLowerCase() === name.toLowerCase()));
        return named?.name ?? "";
    }
}
exports.MessageSchema = MessageSchema;
function protoFiles(dir) {
    const out = [];
    const walk = (current) => {
        for (const entry of node_fs_1.default.readdirSync(current, { withFileTypes: true })) {
            const full = node_path_1.default.join(current, entry.name);
            if (entry.isDirectory())
                walk(full);
            else if (entry.name.endsWith(".proto"))
                out.push(full);
        }
    };
    walk(dir);
    return out;
}
const SCALAR_TYPES = {
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
function setAnnotations(element, options) {
    const parsed = Object.entries(options ?? {})
        .filter(([key]) => key.startsWith("."))
        .map(([key, value]) => ({ [`(${key.slice(1)})`]: value }));
    if (parsed.length > 0)
        element.parsedOptions = parsed;
}
function fieldType(field) {
    return SCALAR_TYPES[field.type] ?? field.typeName;
}
function addEnum(scope, descriptor) {
    const values = {};
    for (const value of descriptor.value ?? [])
        values[value.name] = value.number ?? 0;
    scope.add(new protobuf.Enum(descriptor.name, values));
}
function addMessage(scope, descriptor) {
    const type = new protobuf.Type(descriptor.name);
    setAnnotations(type, descriptor.options);
    const nested = descriptor.nestedType ?? [];
    const entries = new Map(nested.filter((item) => item.options?.mapEntry).map((item) => [item.name, item]));
    for (const item of nested)
        if (!item.options?.mapEntry)
            addMessage(type, item);
    for (const item of descriptor.enumType ?? [])
        addEnum(type, item);
    const groups = (descriptor.oneofDecl ?? []).map((oneof) => new protobuf.OneOf(oneof.name));
    const used = new Set();
    for (const raw of descriptor.field ?? []) {
        const entry = raw.type === "TYPE_MESSAGE" ? entries.get(String(raw.typeName).split(".").pop()) : undefined;
        let field;
        if (entry) {
            const [key, value] = entry.field;
            field = new protobuf.MapField(raw.name, raw.number, fieldType(key), fieldType(value));
        }
        else {
            field = new protobuf.Field(raw.name, raw.number, fieldType(raw), raw.label === "LABEL_REPEATED" ? "repeated" : undefined);
        }
        setAnnotations(field, raw.options);
        if (raw.proto3Optional)
            field.setOption("proto3_optional", true);
        type.add(field);
        if (raw.oneofIndex !== undefined && raw.oneofIndex !== null) {
            groups[raw.oneofIndex].add(field);
            used.add(groups[raw.oneofIndex]);
        }
    }
    for (const group of used)
        type.add(group);
    scope.add(type);
}
function rootFromDescriptorSet(bytes) {
    const setType = protoSchemas().protoType("google.protobuf.FileDescriptorSet");
    const root = new protobuf.Root();
    try {
        const set = setType.toObject(setType.decode(bytes), { enums: String, longs: String });
        for (const file of set.file ?? []) {
            const scope = file.package ? root.define(file.package) : root;
            for (const item of file.enumType ?? [])
                addEnum(scope, item);
            for (const item of file.messageType ?? [])
                addMessage(scope, item);
        }
        root.resolveAll();
    }
    catch (err) {
        throw new Error(`udbtest: descriptor set: ${err.message}`);
    }
    return root;
}
/** The message types of one proto tree: the SDK's own `udb/**` files plus, when
 *  `protoRoot` names an application's directory, that application's messages. */
class ProtoSchemas {
    protoRoot;
    root;
    cache = new Map();
    constructor(protoRoot) {
        this.protoRoot = protoRoot;
    }
    /** The message types of a compiled `google.protobuf.FileDescriptorSet`, with their
     *  udb annotations. Every import must be in the set; a missing one throws. */
    static fromDescriptorSet(bytes) {
        const schemas = new ProtoSchemas();
        schemas.root = rootFromDescriptorSet(bytes);
        return schemas;
    }
    load() {
        if (this.root)
            return this.root;
        const bundled = (0, protoRoot_1.defaultProtoRoot)();
        const appRoot = this.protoRoot ? node_path_1.default.resolve(this.protoRoot) : undefined;
        const includeDirs = [
            ...(appRoot ? [appRoot] : []),
            bundled,
            node_path_1.default.resolve(appRoot ?? bundled, "../third_party/googleapis"),
            node_path_1.default.resolve(bundled, "../third_party/googleapis"),
        ];
        const root = new protobuf.Root();
        const fallback = root.resolvePath.bind(root);
        root.resolvePath = (origin, target) => {
            if (node_path_1.default.isAbsolute(target))
                return target;
            for (const dir of includeDirs) {
                const full = node_path_1.default.join(dir, target);
                if (node_fs_1.default.existsSync(full))
                    return full;
            }
            return fallback(origin, target);
        };
        // An application tree that carries its own udb/ copy replaces the bundled one.
        const appHasUdb = appRoot !== undefined && node_fs_1.default.existsSync(node_path_1.default.join(appRoot, "udb"));
        const files = [
            ...(appRoot ? protoFiles(appRoot) : []),
            ...(appHasUdb ? [] : protoFiles(node_path_1.default.join(bundled, "udb"))),
        ];
        root.loadSync(files, { keepCase: true });
        root.resolveAll();
        this.root = root;
        return root;
    }
    /** The named message, or undefined when this tree does not declare it. */
    find(messageType) {
        const cached = this.cache.get(messageType);
        if (cached)
            return cached;
        const found = this.load().lookup(messageType);
        if (!(found instanceof protobuf.Type))
            return undefined;
        const schema = new MessageSchema(found);
        this.cache.set(messageType, schema);
        return schema;
    }
    /** The named message; an unknown name throws. */
    message(messageType) {
        const schema = this.find(messageType);
        if (!schema)
            throw new Error(`udb: ${messageType} is not declared in the loaded protos`);
        return schema;
    }
    /** The named protobuf type itself, for encoding a message on the wire. */
    protoType(typeName) {
        return this.load().lookupType(typeName);
    }
    /** Every message annotated as a table. */
    tables() {
        const out = [];
        const visit = (scope) => {
            for (const nested of scope.nestedArray) {
                if (nested instanceof protobuf.Type) {
                    const schema = this.find(nested.fullName.replace(/^\./, ""));
                    if (schema?.table.is_table)
                        out.push(schema);
                }
                if (nested instanceof protobuf.Namespace)
                    visit(nested);
            }
        };
        visit(this.load());
        return out;
    }
}
exports.ProtoSchemas = ProtoSchemas;
const BY_ROOT = new Map();
/** The shared schema set of a proto root (the bundled tree when omitted). */
function protoSchemas(protoRoot) {
    const key = protoRoot ? node_path_1.default.resolve(protoRoot) : "";
    let schemas = BY_ROOT.get(key);
    if (!schemas) {
        schemas = new ProtoSchemas(protoRoot);
        BY_ROOT.set(key, schemas);
    }
    return schemas;
}
