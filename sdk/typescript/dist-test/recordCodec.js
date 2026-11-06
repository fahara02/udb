"use strict";
// The one translation between an entity message and a udb record, driven by the
// message's column annotations. It follows the Go SDK's record_codec.go rule for
// rule; test-support/record-codec-cases.json pins the outputs.
//
// A message here is a plain object keyed by proto field names (or their
// camelCase JSON names). A field holding null or undefined is unset.
Object.defineProperty(exports, "__esModule", { value: true });
exports.RawJson = exports.WriteMode = void 0;
exports.parseExactJson = parseExactJson;
exports.recordToJson = recordToJson;
exports.parseRfc3339 = parseRfc3339;
exports.formatTimestamp = formatTimestamp;
exports.parseTimestamp = parseTimestamp;
exports.encodeRecord = encodeRecord;
exports.encodeField = encodeField;
exports.encodeEvent = encodeEvent;
exports.decodeRecord = decodeRecord;
exports.messageFromRow = messageFromRow;
exports.rowFromRecord = rowFromRecord;
exports.decodeField = decodeField;
exports.decodeEventPayload = decodeEventPayload;
const protoSchema_1 = require("./protoSchema");
/** Which columns a record carries. */
var WriteMode;
(function (WriteMode) {
    /** Drops generated and exclude_from_insert columns. */
    WriteMode["Insert"] = "insert";
    /** Drops generated, exclude_from_update, primary-key, tenant and project columns. */
    WriteMode["Update"] = "update";
})(WriteMode || (exports.WriteMode = WriteMode = {}));
/** A JSON column's document, kept as its text so key order and number digits survive. */
class RawJson {
    text;
    constructor(text) {
        this.text = text;
    }
    /** The parsed document. */
    toJSON() {
        return JSON.parse(this.text);
    }
}
exports.RawJson = RawJson;
// ── Exact JSON ──────────────────────────────────────────────────────────────
/** Parse JSON keeping an integer beyond 2^53 exact, as a bigint. */
function parseExactJson(text) {
    let at = 0;
    const fail = () => {
        throw new SyntaxError(`invalid JSON at position ${at}`);
    };
    const space = () => {
        while (at < text.length && " \t\r\n".includes(text[at]))
            at += 1;
    };
    const value = () => {
        space();
        const c = text[at];
        if (c === "{") {
            at += 1;
            const out = {};
            space();
            if (text[at] === "}") {
                at += 1;
                return out;
            }
            for (;;) {
                space();
                if (text[at] !== '"')
                    fail();
                const key = string();
                space();
                if (text[at] !== ":")
                    fail();
                at += 1;
                Object.defineProperty(out, key, { value: value(), enumerable: true, configurable: true, writable: true });
                space();
                if (text[at] === ",") {
                    at += 1;
                    continue;
                }
                if (text[at] === "}") {
                    at += 1;
                    return out;
                }
                fail();
            }
        }
        if (c === "[") {
            at += 1;
            const out = [];
            space();
            if (text[at] === "]") {
                at += 1;
                return out;
            }
            for (;;) {
                out.push(value());
                space();
                if (text[at] === ",") {
                    at += 1;
                    continue;
                }
                if (text[at] === "]") {
                    at += 1;
                    return out;
                }
                fail();
            }
        }
        if (c === '"')
            return string();
        for (const [word, result] of [["true", true], ["false", false], ["null", null]]) {
            if (text.startsWith(word, at)) {
                at += word.length;
                return result;
            }
        }
        const match = /^-?(0|[1-9]\d*)(\.\d+)?([eE][+-]?\d+)?/.exec(text.slice(at, at + 400));
        if (!match)
            return fail();
        at += match[0].length;
        if (match[2] || match[3])
            return Number(match[0]);
        const number = Number(match[0]);
        return Number.isSafeInteger(number) ? number : BigInt(match[0]);
    };
    const string = () => {
        const start = at;
        at += 1;
        while (at < text.length && text[at] !== '"')
            at += text[at] === "\\" ? 2 : 1;
        if (at >= text.length)
            fail();
        at += 1;
        return JSON.parse(text.slice(start, at));
    };
    const result = value();
    space();
    if (at !== text.length)
        fail();
    return result;
}
function escapeHtml(json) {
    return json.replace(/[<>&\u2028\u2029]/g, (c) => `\\u${c.charCodeAt(0).toString(16).padStart(4, "0")}`);
}
function compactJson(text) {
    // Compact outside strings without reordering keys or touching number digits.
    let out = "";
    let inString = false;
    for (let i = 0; i < text.length; i += 1) {
        const c = text[i];
        if (inString) {
            out += c;
            if (c === "\\") {
                i += 1;
                out += text[i];
            }
            else if (c === '"')
                inString = false;
        }
        else if (c === '"') {
            inString = true;
            out += c;
        }
        else if (!" \t\r\n".includes(c))
            out += c;
    }
    return out;
}
/** The record as JSON text: sorted keys, exact integers, raw JSON documents. */
function recordToJson(value) {
    if (value === null || value === undefined)
        return "null";
    if (value instanceof RawJson)
        return escapeHtml(compactJson(value.text));
    switch (typeof value) {
        case "bigint":
            return value.toString();
        case "string":
            return escapeHtml(JSON.stringify(value));
        case "number":
            if (!Number.isFinite(value))
                throw new Error(`udb: ${value} is not a JSON number`);
            return JSON.stringify(value);
        case "boolean":
            return value ? "true" : "false";
    }
    if (Array.isArray(value))
        return `[${value.map(recordToJson).join(",")}]`;
    if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
        return JSON.stringify(Buffer.from(value).toString("base64"));
    }
    if (value instanceof Date)
        return JSON.stringify(formatTimestamp(value));
    const entries = Object.entries(value)
        .filter(([, item]) => item !== undefined)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${entries.map(([key, item]) => `${escapeHtml(JSON.stringify(key))}:${recordToJson(item)}`).join(",")}}`;
}
/** Whether `text` is one valid JSON document. */
function isValidJson(text) {
    try {
        JSON.parse(text);
        return true;
    }
    catch {
        return false;
    }
}
// ── Timestamps ──────────────────────────────────────────────────────────────
const RFC3339 = /^(\d{4})-(\d{2})-(\d{2})[Tt](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?([Zz]|[+-]\d{2}:\d{2})$/;
/** Parse RFC 3339 into whole seconds and nanoseconds; null when it is not RFC 3339. */
function parseRfc3339(text) {
    const match = RFC3339.exec(text);
    if (!match)
        return null;
    const [, y, mo, d, h, mi, s, fraction, zone] = match;
    const year = Number(y);
    const month = Number(mo);
    const day = Number(d);
    const date = new Date(0);
    date.setUTCFullYear(year, month - 1, day);
    if (date.getUTCMonth() !== month - 1 || date.getUTCDate() !== day)
        return null;
    if (Number(h) > 23 || Number(mi) > 59 || Number(s) > 59)
        return null;
    date.setUTCHours(Number(h), Number(mi), Number(s), 0);
    let seconds = Math.floor(date.getTime() / 1000);
    if (zone !== "Z" && zone !== "z") {
        const sign = zone[0] === "-" ? 1 : -1;
        seconds += sign * (Number(zone.slice(1, 3)) * 3600 + Number(zone.slice(4, 6)) * 60);
    }
    return { seconds, nanos: fraction ? Number(fraction.padEnd(9, "0")) : 0 };
}
function secondsText(seconds) {
    return new Date(seconds * 1000).toISOString().slice(0, 19);
}
/** The proto JSON form of a Timestamp: UTC, Z, with 0, 3, 6 or 9 fractional digits. */
function protoTimestamp(seconds, nanos) {
    if (nanos === 0)
        return `${secondsText(seconds)}Z`;
    let digits = String(nanos).padStart(9, "0");
    if (digits.endsWith("000000"))
        digits = digits.slice(0, 3);
    else if (digits.endsWith("000"))
        digits = digits.slice(0, 6);
    return `${secondsText(seconds)}.${digits}Z`;
}
/** The one text form udb uses for a TIMESTAMPTZ value: RFC 3339 in UTC with exactly
 *  six fractional digits, so a value compares equal to the broker's echo as text. */
function formatTimestamp(value) {
    const parts = typeof value === "string" ? parseRfc3339(value) : { seconds: Math.floor(value.getTime() / 1000), nanos: ((value.getTime() % 1000) + 1000) % 1000 * 1e6 };
    if (!parts)
        throw new Error(`udb: "${value}" is not an RFC 3339 timestamp`);
    return `${secondsText(parts.seconds)}.${String(Math.floor(parts.nanos / 1000)).padStart(6, "0")}Z`;
}
/** Parse a timestamp column as {@link decodeRecord} leaves it; "" (NULL) is null. */
function parseTimestamp(text) {
    if (text === "")
        return null;
    const parts = parseRfc3339(text);
    if (!parts)
        throw new Error(`udb: "${text}" is not an RFC 3339 timestamp`);
    return new Date(parts.seconds * 1000 + Math.floor(parts.nanos / 1e6));
}
const WKT = "google.protobuf.";
const NESTED = new WeakMap();
function nestedSchema(type) {
    let schema = NESTED.get(type);
    if (!schema) {
        schema = new protoSchema_1.MessageSchema(type);
        NESTED.set(type, schema);
    }
    return schema;
}
function typeName(field) {
    return field.messageType.fullName.replace(/^\./, "");
}
function enumNumber(field, value, path) {
    const values = field.enumType.values;
    if (typeof value === "number" && Number.isInteger(value))
        return value;
    if (typeof value === "string") {
        if (value in values)
            return values[value];
        const full = enumCommonPrefix(field) + value;
        if (full in values)
            return values[full];
    }
    throw new Error(`udb: ${path}: ${JSON.stringify(value)} is not a value of ${field.enumType.name}`);
}
/** The longest common UPPER_SNAKE_ prefix of an enum's value names, cut at a `_`. */
function enumCommonPrefix(field) {
    const names = Object.keys(field.enumType.values);
    if (names.length === 0)
        return "";
    const first = names[0];
    let end = first.length;
    for (const name of names.slice(1)) {
        let n = 0;
        while (n < first.length && n < name.length && first[n] === name[n])
            n += 1;
        end = Math.min(end, n);
    }
    const cut = first.slice(0, end).lastIndexOf("_");
    return cut >= 0 ? first.slice(0, cut + 1) : "";
}
function enumName(field, number) {
    return field.enumType.valuesById[number];
}
function integerText(value, path) {
    if (typeof value === "bigint")
        return value.toString();
    if (typeof value === "number") {
        if (!Number.isInteger(value))
            throw new Error(`udb: ${path}: ${value} is not an integer`);
        if (!Number.isSafeInteger(value))
            throw new Error(`udb: ${path}: ${value} exceeds the exact integer range; pass a string or bigint`);
        return String(value);
    }
    if (typeof value === "string" && /^-?\d+$/.test(value.trim()))
        return value.trim();
    if (value && typeof value === "object" && typeof value.toString === "function") {
        const text = String(value);
        if (/^-?\d+$/.test(text))
            return text; // A Long.
    }
    throw new Error(`udb: ${path}: ${JSON.stringify(value)} is not an integer`);
}
function checkIntegerRange(field, text, path) {
    const value = BigInt(text);
    const unsigned = field.kind.startsWith("uint") || field.kind.startsWith("fixed");
    const bits = (0, protoSchema_1.is64BitField)(field) ? 64n : 32n;
    const low = unsigned ? 0n : -(1n << (bits - 1n));
    const high = unsigned ? (1n << bits) - 1n : (1n << (bits - 1n)) - 1n;
    if (value < low || value > high)
        throw new Error(`udb: ${path}: ${text} is out of range for ${field.kind}`);
}
function float32Number(value) {
    const single = Math.fround(value);
    for (let precision = 1; precision <= 9; precision += 1) {
        const shortest = Number(single.toPrecision(precision));
        if (Math.fround(shortest) === single)
            return shortest;
    }
    return single;
}
function floatJson(field, value, path) {
    const number = typeof value === "string" ? Number(value) : value;
    if (typeof number !== "number" || (typeof value === "string" && !["NaN", "Infinity", "-Infinity"].includes(value) && Number.isNaN(number))) {
        throw new Error(`udb: ${path}: ${JSON.stringify(value)} is not a number`);
    }
    if (Number.isNaN(number))
        return "NaN";
    if (number === Infinity)
        return "Infinity";
    if (number === -Infinity)
        return "-Infinity";
    return field.kind === "float" ? float32Number(number) : number;
}
function bytesText(value, path) {
    if (typeof value === "string")
        return value;
    if (Buffer.isBuffer(value) || value instanceof Uint8Array)
        return Buffer.from(value).toString("base64");
    throw new Error(`udb: ${path}: expected bytes`);
}
function scalarJson(field, value, options, path) {
    if (field.kind === "enum") {
        const number = enumNumber(field, value, path);
        return options.enumNumbers ? number : enumName(field, number) ?? number;
    }
    if (field.kind === "message")
        return messageJson(field.messageType, value, options, path);
    if ((0, protoSchema_1.is64BitField)(field)) {
        const text = integerText(value, path);
        checkIntegerRange(field, text, path);
        return text;
    }
    if ((0, protoSchema_1.is32BitField)(field)) {
        const text = integerText(value, path);
        checkIntegerRange(field, text, path);
        return Number(text);
    }
    switch (field.kind) {
        case "string":
            if (typeof value !== "string")
                throw new Error(`udb: ${path}: expected a string, got ${JSON.stringify(value)}`);
            return value;
        case "bool":
            if (typeof value !== "boolean")
                throw new Error(`udb: ${path}: expected a boolean, got ${JSON.stringify(value)}`);
            return value;
        case "bytes":
            return bytesText(value, path);
        case "float":
        case "double":
            return floatJson(field, value, path);
    }
    throw new Error(`udb: ${path}: unsupported field type ${field.kind}`);
}
function isPopulated(field, value) {
    if (value === undefined || value === null)
        return false;
    if (field.repeated)
        return value.length > 0;
    if (field.map)
        return Object.keys(value).length > 0;
    if (field.hasPresence)
        return true;
    switch (field.kind) {
        case "string":
            return value !== "";
        case "bool":
            return value !== false;
        case "bytes":
            return bytesText(value, field.name) !== "";
        case "enum":
            return enumNumberOrZero(field, value) !== 0;
        case "float":
        case "double":
            return value !== 0 && value !== "0";
        default:
            return String(value) !== "0";
    }
}
function enumNumberOrZero(field, value) {
    return enumNumber(field, value, field.name);
}
/** RFC 3339 from the text a TIMESTAMP or DATE column reads as, taking no zone as UTC. */
function columnTimeText(text) {
    const trimmed = text.trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed))
        return `${trimmed}T00:00:00Z`;
    const spaced = /^\d{4}-\d{2}-\d{2} /.test(trimmed) ? `${trimmed.slice(0, 10)}T${trimmed.slice(11)}` : trimmed;
    return /[Zz+-]/.test(spaced.slice(10)) ? spaced : `${spaced}Z`;
}
function timestampJson(value, path) {
    if (value instanceof Date) {
        const ms = value.getTime();
        if (Number.isNaN(ms))
            throw new Error(`udb: ${path}: invalid Date`);
        return protoTimestamp(Math.floor(ms / 1000), ((ms % 1000) + 1000) % 1000 * 1e6);
    }
    if (typeof value === "string") {
        const parts = parseRfc3339(columnTimeText(value));
        if (!parts)
            throw new Error(`udb: ${path}: "${value}" is not an RFC 3339 timestamp`);
        return protoTimestamp(parts.seconds, parts.nanos);
    }
    const object = value;
    if (object && typeof object === "object") {
        return protoTimestamp(Number(object.seconds ?? 0), Number(object.nanos ?? 0));
    }
    throw new Error(`udb: ${path}: expected a timestamp`);
}
function durationJson(value, path) {
    if (typeof value === "string" && /^-?\d+(\.\d{1,9})?s$/.test(value)) {
        const negative = value.startsWith("-");
        const [whole, fraction = ""] = value.replace(/^-|s$/g, "").split(".");
        return durationText(negative, whole, Number(fraction.padEnd(9, "0") || 0));
    }
    const object = value;
    if (object && typeof object === "object") {
        const seconds = BigInt(String(object.seconds ?? 0));
        const nanos = Number(object.nanos ?? 0);
        const negative = seconds < 0n || nanos < 0;
        return durationText(negative, (negative ? -seconds : seconds).toString(), Math.abs(nanos));
    }
    throw new Error(`udb: ${path}: expected a duration`);
}
function durationText(negative, seconds, nanos) {
    let digits = "";
    if (nanos !== 0) {
        digits = String(nanos).padStart(9, "0");
        if (digits.endsWith("000000"))
            digits = digits.slice(0, 3);
        else if (digits.endsWith("000"))
            digits = digits.slice(0, 6);
        digits = `.${digits}`;
    }
    return `${negative ? "-" : ""}${seconds}${digits}s`;
}
/** The wrapper types carry their scalar's kind on their `value` field. */
const WRAPPER_KINDS = {
    DoubleValue: "double",
    FloatValue: "float",
    Int64Value: "int64",
    UInt64Value: "uint64",
    Int32Value: "int32",
    UInt32Value: "uint32",
    BoolValue: "bool",
    StringValue: "string",
    BytesValue: "bytes",
};
function wrapperField(kind) {
    return { name: "value", jsonName: "value", kind, repeated: false, map: false, hasPresence: false, masked: false, pii: false, encrypted: false };
}
function messageJson(type, value, options, path) {
    const name = type.fullName.replace(/^\./, "");
    if (name.startsWith(WKT)) {
        const short = name.slice(WKT.length);
        if (short === "Timestamp")
            return timestampJson(value, path);
        if (short === "Duration")
            return durationJson(value, path);
        if (short === "Struct" || short === "Value" || short === "ListValue" || short === "Any")
            return value;
        if (short === "Empty")
            return {};
        if (short === "FieldMask")
            return Array.isArray(value?.paths) ? value.paths.join(",") : value;
        const wrapped = WRAPPER_KINDS[short];
        if (wrapped) {
            const inner = typeof value === "object" && value !== null && !Buffer.isBuffer(value) && !(value instanceof Uint8Array) && "value" in value
                ? value.value
                : value;
            return scalarJson(wrapperField(wrapped), inner, options, path);
        }
    }
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
        throw new Error(`udb: ${path}: expected a ${name} object`);
    }
    return objectJson(nestedSchema(type), value, options, path);
}
function fieldJson(field, value, options, path) {
    if (field.repeated) {
        if (!Array.isArray(value))
            throw new Error(`udb: ${path}: expected an array`);
        return value.map((item, index) => scalarJson(field, item, options, `${path}[${index}]`));
    }
    if (field.map) {
        const out = {};
        for (const [key, item] of Object.entries(value)) {
            out[key] = scalarJson(field, item, options, `${path}.${key}`);
        }
        return out;
    }
    return scalarJson(field, value, options, path);
}
function fieldDefaultJson(field, options) {
    if (field.repeated)
        return [];
    if (field.map)
        return {};
    if (field.kind === "message")
        return null;
    if (field.kind === "enum")
        return options.enumNumbers ? 0 : enumName(field, 0) ?? 0;
    if ((0, protoSchema_1.is64BitField)(field))
        return "0";
    switch (field.kind) {
        case "string":
        case "bytes":
            return "";
        case "bool":
            return false;
        default:
            return 0;
    }
}
/** Read a message's value per field, refusing a field the message does not have
 *  and two members of one oneof. */
function populatedValues(schema, message, path) {
    const values = new Map();
    const groups = new Map();
    for (const [key, value] of Object.entries(message)) {
        if (value === undefined)
            continue;
        const field = schema.field(key);
        if (!field)
            throw new Error(`udb: ${path} has no field "${key}"`);
        if (value === null)
            continue;
        if (values.has(field))
            throw new Error(`udb: ${path} sets ${field.name} twice`);
        if (field.oneof) {
            const previous = groups.get(field.oneof);
            if (previous)
                throw new Error(`udb: ${path}: oneof ${field.oneof} has both ${previous} and ${field.name}`);
            groups.set(field.oneof, field.name);
        }
        values.set(field, value);
    }
    return values;
}
function objectJson(schema, message, options, path) {
    const values = populatedValues(schema, message, path);
    const out = {};
    for (const field of schema.fields) {
        const value = values.get(field);
        if (value !== undefined && isPopulated(field, value)) {
            out[field.name] = fieldJson(field, value, options, `${path}.${field.name}`);
        }
        else if (options.emitUnpopulated && (!field.hasPresence || (field.kind === "message" && !field.oneof))) {
            out[field.name] = fieldDefaultJson(field, options);
        }
    }
    return out;
}
// ── encodeRecord ────────────────────────────────────────────────────────────
function textColumn(field) {
    const sql = (field.column?.sql_type ?? "").trim().toUpperCase();
    return ["VARCHAR", "CHARACTER VARYING", "TEXT", "CHAR", "CITEXT"].some((prefix) => sql.startsWith(prefix));
}
function isJsonColumn(field) {
    const column = field.column;
    return Boolean(column?.is_json || column?.is_jsonb || (column?.sql_type ?? "").toUpperCase().startsWith("JSON"));
}
function noEmptyValue(field) {
    const sql = (field.column?.sql_type ?? "").toUpperCase();
    return (isJsonColumn(field) ||
        sql.startsWith("UUID") ||
        sql.startsWith("CHAR(") ||
        sql.startsWith("CHARACTER(") ||
        sql.startsWith("TIMESTAMP") ||
        sql === "DATE");
}
function carries(field, mode) {
    const column = field.column;
    if (!column || column.generated)
        return false;
    if (mode === WriteMode.Insert)
        return !column.exclude_from_insert;
    return !column.exclude_from_update && !column.primary_key && !column.tenant_column;
}
function exactInts(value) {
    if (typeof value === "string") {
        const number = Number(value);
        return Number.isSafeInteger(number) ? number : BigInt(value);
    }
    if (Array.isArray(value))
        return value.map(exactInts);
    return value;
}
function enumTokens(field, value) {
    const prefix = enumCommonPrefix(field);
    const token = (number) => {
        const name = typeof number === "number" ? enumName(field, number) : undefined;
        return name === undefined ? number : name.startsWith(prefix) ? name.slice(prefix.length) : name;
    };
    return Array.isArray(value) ? value.map(token) : token(value);
}
/** Apply the column rules to `record[field.name]`, which holds proto JSON. */
function encodeColumn(schema, field, isSet, record) {
    const name = field.name;
    // A default read leaves PII and encrypted columns out or masks them, so an unset one here
    // may only be unread. A full-row write must not erase it: clear it with patch and null.
    if (!isSet && !field.repeated && !field.map && (field.pii || field.encrypted || field.masked)) {
        delete record[name];
        return;
    }
    if (field.hasPresence && !isSet) {
        if (field.column?.not_null)
            delete record[name];
        else
            record[name] = null;
        return;
    }
    // A strict DATE bind takes the date alone.
    if (field.kind === "message" &&
        field.messageType?.fullName === ".google.protobuf.Timestamp" &&
        field.column?.sql_type?.trim().toUpperCase() === "DATE" &&
        typeof record[name] === "string") {
        record[name] = record[name].slice(0, 10);
        return;
    }
    if ((0, protoSchema_1.is64BitField)(field)) {
        record[name] = exactInts(record[name]);
        return;
    }
    if (field.kind === "enum" && textColumn(field)) {
        record[name] = enumTokens(field, record[name]);
        return;
    }
    const text = record[name];
    if (typeof text !== "string" || field.repeated || field.kind !== "string")
        return;
    if (text === "" && !field.column?.not_null && !field.hasPresence) {
        record[name] = null;
    }
    else if (text === "" && noEmptyValue(field) && !field.hasPresence) {
        delete record[name];
    }
    else if (isJsonColumn(field) && (text !== "" || field.hasPresence)) {
        if (!isValidJson(text)) {
            throw new Error(`udb: encode ${schema.fullName}.${name}: JSON column holds text that is not JSON`);
        }
        // The broker reads a string as JSON text, so a JSON string goes as its text.
        record[name] = text.trimStart().startsWith('"') ? text : new RawJson(text);
    }
}
/** Turn an entity message into a udb record keyed by proto field names, following
 *  its column annotations:
 *   - fields without a column annotation, generated columns and the columns `mode`
 *     excludes are dropped;
 *   - enums are numbers, or short tokens in a text column; 64-bit integers are exact;
 *   - an unset PII or encrypted column is dropped, because a default read does not return it;
 *   - an unset field with presence is NULL, or dropped when its column is NOT NULL;
 *   - an empty string without presence in a nullable column is NULL; in a NOT NULL
 *     UUID, CHAR(n), DATE, TIMESTAMP or JSON column it is dropped, and a NOT NULL
 *     VARCHAR/TEXT column keeps it;
 *   - a JSON column carries its text as a JSON value; text that is not JSON throws. */
function encodeRecord(messageType, message, mode = WriteMode.Insert, schemas = (0, protoSchema_1.protoSchemas)()) {
    const schema = schemas.message(messageType);
    const options = { emitUnpopulated: true, enumNumbers: true };
    const record = objectJson(schema, message, options, messageType);
    const tenant = mode === WriteMode.Update ? schema.tenantColumn : "";
    const project = mode === WriteMode.Update ? schema.projectColumn : "";
    const values = populatedValues(schema, message, messageType);
    for (const field of schema.fields) {
        if (!carries(field, mode) || (mode === WriteMode.Update && (field.name === tenant || field.name === project))) {
            delete record[field.name];
            continue;
        }
        encodeColumn(schema, field, values.has(field), record);
    }
    return record;
}
/** Encode one field exactly as {@link encodeRecord} encodes its column.
 *  `present` is false when the column is left out of the record. */
function encodeField(messageType, message, fieldName, schemas = (0, protoSchema_1.protoSchemas)()) {
    const schema = schemas.message(messageType);
    const field = schema.field(fieldName);
    if (!field)
        throw new Error(`udb: ${messageType} has no field "${fieldName}"`);
    const value = populatedValues(schema, message, messageType).get(field);
    const options = { emitUnpopulated: true, enumNumbers: true };
    const record = {};
    if (value !== undefined && isPopulated(field, value)) {
        record[field.name] = fieldJson(field, value, options, `${messageType}.${field.name}`);
    }
    else if (!field.hasPresence) {
        record[field.name] = fieldDefaultJson(field, options);
    }
    encodeColumn(schema, field, value !== undefined, record);
    return { value: record[field.name], present: field.name in record };
}
/** Turn an event message into an outbox payload (proto JSON keyed by proto field
 *  names). An event that declares no outbox topic throws: it has nowhere to go. */
function encodeEvent(messageType, event, schemas = (0, protoSchema_1.protoSchemas)()) {
    const schema = schemas.message(messageType);
    if (!schema.topic)
        throw new Error(`udb: ${messageType} declares no outbox topic`);
    return objectJson(schema, event, { emitUnpopulated: false, enumNumbers: false }, messageType);
}
// ── decodeRecord ────────────────────────────────────────────────────────────
function parseBool(text) {
    const trimmed = text.trim();
    if (["1", "t", "T", "TRUE", "true", "True"].includes(trimmed))
        return true;
    if (["0", "f", "F", "FALSE", "false", "False"].includes(trimmed))
        return false;
    throw new Error(`"${text}" is not a boolean`);
}
function enumFullName(field, token) {
    const values = field.enumType.values;
    if (token in values)
        return token;
    const full = enumCommonPrefix(field) + token;
    return full in values ? full : token;
}
function splitArrayLiteral(body) {
    const out = [];
    if (body.trim() === "")
        return out;
    let current = "";
    let quoted = false;
    let inQuotes = false;
    let escaped = false;
    const flush = () => {
        out.push({ text: quoted ? current : current.trim(), quoted });
        current = "";
        quoted = false;
    };
    for (const c of body) {
        if (escaped) {
            current += c;
            escaped = false;
        }
        else if (c === "\\") {
            escaped = true;
        }
        else if (c === '"') {
            inQuotes = !inQuotes;
            quoted = true;
        }
        else if (c === "," && !inQuotes) {
            flush();
        }
        else if (c === "{" && !inQuotes) {
            throw new Error("nested arrays are not supported");
        }
        else {
            current += c;
        }
    }
    if (inQuotes || escaped)
        throw new Error("unterminated element");
    flush();
    return out;
}
function arrayLiteral(literal, field) {
    const trimmed = literal.trim();
    if (!trimmed.startsWith("{") || !trimmed.endsWith("}"))
        throw new Error(`not an array literal: "${trimmed}"`);
    const items = splitArrayLiteral(trimmed.slice(1, -1));
    return items.map(({ text, quoted }) => {
        if (!quoted && text.toUpperCase() === "NULL")
            throw new Error(`array literal "${trimmed}" holds NULL`);
        switch (field.kind) {
            case "string":
            case "bytes":
                return text;
            case "bool":
                return parseBool(text);
            case "float":
            case "double": {
                const number = Number(text);
                if (Number.isNaN(number) && text !== "NaN")
                    throw new Error(`"${text}" is not a number`);
                return number;
            }
            case "enum":
                return /^-?\d+$/.test(text) ? Number(text) : text;
            default:
                if (!/^-?\d+$/.test(text))
                    throw new Error(`"${text}" is not an integer`);
                return exactInts(text);
        }
    });
}
/** A column value's JSON text. */
function jsonText(value) {
    return value instanceof RawJson ? compactJson(value.text) : recordToJson(value);
}
/** The JSON text of a JSON column read as a string. The broker returns the value, so a JSON
 *  string arrives without its quotes. A store that reads JSON as text returns a document unchanged. */
function jsonColumnText(value) {
    const text = value.trim();
    if ((text.startsWith("{") || text.startsWith("[")) && isValidJson(text))
        return value;
    return JSON.stringify(value);
}
/** Rewrite one column value into the proto JSON form of its field. */
function normalise(field, column, jsonIsText = false) {
    const raw = column instanceof RawJson && field.kind !== "string" ? column.toJSON() : column;
    if (field.map || field.kind === "message")
        return raw;
    if (field.repeated) {
        let list = raw;
        if (typeof raw === "string") {
            const trimmed = raw.trim();
            list = trimmed.startsWith("[") ? parseExactJson(trimmed) : arrayLiteral(raw, field);
        }
        if (field.kind === "enum" && Array.isArray(list)) {
            return list.map((item) => (typeof item === "string" ? enumFullName(field, item) : item));
        }
        return list;
    }
    if (typeof raw !== "string")
        return field.kind === "string" ? jsonText(raw) : raw;
    if (field.kind === "string") {
        if (isJsonColumn(field))
            return jsonIsText ? raw : jsonColumnText(raw);
        const sql = (field.column?.sql_type ?? "").toUpperCase();
        if (sql.startsWith("CHAR(") || sql.startsWith("CHARACTER("))
            return raw.replace(/ +$/, "");
        if (sql === "DATE" && raw.length > 10)
            return raw.slice(0, 10);
        return raw;
    }
    if (field.kind === "bool")
        return parseBool(raw);
    if (field.kind === "enum") {
        const trimmed = raw.trim();
        return /^-?\d+$/.test(trimmed) ? Number(trimmed) : enumFullName(field, trimmed);
    }
    return raw; // Numbers as text: proto JSON accepts quoted numbers.
}
function integerFromJson(field, value, path) {
    let text;
    if (typeof value === "bigint")
        text = value.toString();
    else if (typeof value === "number") {
        if (!Number.isInteger(value))
            throw new Error(`udb: decode ${path}: ${value} is not an integer`);
        text = BigInt(value).toString();
    }
    else if (typeof value === "string") {
        const trimmed = value.trim();
        const match = /^(-?\d+)(?:\.0*)?$/.exec(trimmed);
        if (match)
            text = match[1];
        else if (/^-?\d+(\.\d+)?[eE][+-]?\d+$/.test(trimmed) && Number.isInteger(Number(trimmed)))
            text = BigInt(Number(trimmed)).toString();
        else
            throw new Error(`udb: decode ${path}: "${value}" is not an integer`);
    }
    else {
        throw new Error(`udb: decode ${path}: ${JSON.stringify(value)} is not an integer`);
    }
    try {
        checkIntegerRange(field, text, path);
    }
    catch {
        throw new Error(`udb: decode ${path}: ${text} is out of range for ${field.kind}`);
    }
    const number = Number(text);
    return Number.isSafeInteger(number) ? number : text;
}
function scalarFromJson(field, value, options, path) {
    if (field.kind === "message")
        return messageFromJson(field.messageType, value, options, path);
    if (field.kind === "enum") {
        if (typeof value === "number" && Number.isInteger(value))
            return enumName(field, value) ?? value;
        if (typeof value === "string" && value in field.enumType.values)
            return value;
        throw new Error(`udb: decode ${path}: ${JSON.stringify(value)} is not a value of ${field.enumType.name}`);
    }
    if ((0, protoSchema_1.is64BitField)(field) || (0, protoSchema_1.is32BitField)(field))
        return integerFromJson(field, value, path);
    switch (field.kind) {
        case "string":
            if (typeof value !== "string")
                throw new Error(`udb: decode ${path}: expected a string, got ${JSON.stringify(value)}`);
            return value;
        case "bool":
            if (typeof value !== "boolean")
                throw new Error(`udb: decode ${path}: expected a boolean, got ${JSON.stringify(value)}`);
            return value;
        case "bytes":
            if (typeof value !== "string" || !/^[A-Za-z0-9+/_-]*={0,2}$/.test(value))
                throw new Error(`udb: decode ${path}: expected base64`);
            return Buffer.from(value.replace(/-/g, "+").replace(/_/g, "/"), "base64");
        case "float":
        case "double": {
            const number = typeof value === "string" ? (value === "NaN" ? NaN : value.trim() === "" ? undefined : Number(value)) : value;
            if (typeof number !== "number" || (Number.isNaN(number) && value !== "NaN")) {
                throw new Error(`udb: decode ${path}: ${JSON.stringify(value)} is not a number`);
            }
            if (field.kind === "float" && Number.isFinite(number) && !Number.isFinite(Math.fround(number))) {
                throw new Error(`udb: decode ${path}: ${number} is out of range for float`);
            }
            return number;
        }
    }
    throw new Error(`udb: decode ${path}: unsupported field type ${field.kind}`);
}
function fieldFromJson(field, value, options, path) {
    if (field.repeated) {
        if (!Array.isArray(value))
            throw new Error(`udb: decode ${path}: expected an array`);
        return value.map((item, index) => {
            if (item === null)
                throw new Error(`udb: decode ${path}[${index}]: a repeated field cannot hold null`);
            return scalarFromJson(field, item, options, `${path}[${index}]`);
        });
    }
    if (field.map) {
        if (typeof value !== "object" || value === null)
            throw new Error(`udb: decode ${path}: expected an object`);
        const out = {};
        for (const [key, item] of Object.entries(value)) {
            out[key] = scalarFromJson(field, item, options, `${path}.${key}`);
        }
        return out;
    }
    return scalarFromJson(field, value, options, path);
}
function messageFromJson(type, value, options, path) {
    const name = type.fullName.replace(/^\./, "");
    if (name.startsWith(WKT)) {
        const short = name.slice(WKT.length);
        if (short === "Timestamp") {
            if (typeof value !== "string" || !parseRfc3339(value))
                throw new Error(`udb: decode ${path}: ${JSON.stringify(value)} is not an RFC 3339 timestamp`);
            return timestampJson(value, path);
        }
        if (short === "Duration") {
            if (typeof value !== "string")
                throw new Error(`udb: decode ${path}: expected a duration`);
            return durationJson(value, path);
        }
        if (short === "Struct" || short === "Value" || short === "ListValue" || short === "Any")
            return value;
        if (short === "Empty")
            return {};
        if (short === "FieldMask")
            return value;
        const wrapped = WRAPPER_KINDS[short];
        if (wrapped)
            return scalarFromJson(wrapperField(wrapped), value, options, path);
    }
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
        throw new Error(`udb: decode ${path}: expected a ${name} object`);
    }
    return objectFromJson(nestedSchema(type), value, options, path);
}
function objectFromJson(schema, value, options, path) {
    const out = {};
    const groups = new Map();
    for (const [key, item] of Object.entries(value)) {
        const field = schema.field(key);
        if (!field) {
            if (options.discardUnknown)
                continue;
            throw new Error(`udb: decode ${path}: unknown field "${key}"`);
        }
        if (item === null || item === undefined)
            continue;
        if (field.oneof) {
            const previous = groups.get(field.oneof);
            if (previous && previous !== field.name)
                throw new Error(`udb: decode ${path}: oneof ${field.oneof} has both ${previous} and ${field.name}`);
            groups.set(field.oneof, field.name);
        }
        out[field.name] = fieldFromJson(field, item, options, `${path}.${field.name}`);
    }
    return out;
}
/** Fill a message from a udb record. It accepts every shape udb (or Postgres
 *  behind it) hands back for a column:
 *   - NULL and unknown columns are skipped;
 *   - a string field whose column came back as a JSON value gets that value's JSON
 *     text; CHAR(n) padding is trimmed; a DATE with a time part keeps its date;
 *   - booleans and enum or integer numbers that came back as text are parsed;
 *   - a repeated field that came back as text is read as a Postgres array literal
 *     or as a JSON array.
 *  The result is keyed by proto field names: enums are their full value names, a
 *  64-bit integer is a number when exact and otherwise a decimal string, a
 *  timestamp is RFC 3339 text, and bytes are a Buffer. An unknown enum name throws. */
function decodeRecord(messageType, record, schemas = (0, protoSchema_1.protoSchemas)()) {
    return decodeColumns(messageType, record, schemas, false);
}
/** A message from a row a caller wrote. A string in a JSON column is already its JSON text, so
 *  text that is not JSON is refused when the message is encoded. */
function messageFromRow(messageType, row, schemas = (0, protoSchema_1.protoSchemas)()) {
    return decodeColumns(messageType, row, schemas, true);
}
/** The row a table returns: a JSON string column holds its JSON text, as a string field does, so
 *  a row read and written back is unchanged. Other columns keep their stored value. */
function rowFromRecord(messageType, record, schemas = (0, protoSchema_1.protoSchemas)()) {
    const row = { ...record };
    for (const field of schemas.message(messageType).fields) {
        const value = row[field.name];
        if (typeof value === "string" && field.kind === "string" && !field.repeated && isJsonColumn(field)) {
            row[field.name] = jsonColumnText(value);
        }
    }
    return row;
}
function decodeColumns(messageType, record, schemas, jsonIsText) {
    const schema = schemas.message(messageType);
    const out = {};
    const groups = new Map();
    for (const field of schema.fields) {
        const value = record[field.name];
        // A protected column reads as this placeholder. An unprotected text column may really
        // hold the text, so only a protected or non-text column can be sure it is no value.
        if (value === undefined || value === null)
            continue;
        if (value === "***MASKED***" && (field.masked || field.pii || field.encrypted || field.kind !== "string"))
            continue;
        let normal;
        try {
            normal = normalise(field, value, jsonIsText);
        }
        catch (err) {
            throw new Error(`udb: decode ${messageType}.${field.name}: ${err.message}`);
        }
        if (field.oneof) {
            const previous = groups.get(field.oneof);
            if (previous)
                throw new Error(`udb: decode ${messageType}: oneof ${field.oneof} has both ${previous} and ${field.name}`);
            groups.set(field.oneof, field.name);
        }
        out[field.name] = fieldFromJson(field, normal, { discardUnknown: false }, `${messageType}.${field.name}`);
    }
    return out;
}
/** Set one field of `message` from the value a row holds for its column,
 *  accepting every shape {@link decodeRecord} accepts. A NULL or absent value
 *  leaves the field unchanged. */
function decodeField(messageType, message, fieldName, raw, schemas = (0, protoSchema_1.protoSchemas)()) {
    const schema = schemas.message(messageType);
    const field = schema.field(fieldName);
    if (!field)
        throw new Error(`udb: ${messageType} has no field "${fieldName}"`);
    const decoded = decodeRecord(messageType, { [field.name]: raw }, schemas);
    if (!(field.name in decoded))
        return;
    if (field.oneof) {
        for (const other of schema.fields) {
            if (other !== field && other.oneof === field.oneof && message[other.name] !== undefined && message[other.name] !== null) {
                throw new Error(`udb: decode ${messageType}: oneof ${field.oneof} has both ${other.name} and ${field.name}`);
            }
        }
    }
    message[field.name] = decoded[field.name];
}
/** Decode an event payload (proto JSON) into a message, ignoring unknown fields so
 *  a newer producer can add one; a value of the wrong type throws. */
function decodeEventPayload(messageType, payload, schemas = (0, protoSchema_1.protoSchemas)()) {
    return objectFromJson(schemas.message(messageType), payload, { discardUnknown: true }, messageType);
}
