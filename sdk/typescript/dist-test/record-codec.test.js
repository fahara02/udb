"use strict";
// The record codec against the Go SDK's rules. test-support/record-codec-cases.json
// holds inputs and the exact JSON text Go's json.Marshal(EncodeRecord(...)) produces.
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_test_1 = require("node:test");
const strict_1 = __importDefault(require("node:assert/strict"));
const node_fs_1 = __importDefault(require("node:fs"));
const node_os_1 = __importDefault(require("node:os"));
const node_path_1 = __importDefault(require("node:path"));
const protoSchema_1 = require("./protoSchema");
const recordCodec_1 = require("./recordCodec");
const FILE = "udb.core.storage.entity.v1.File";
const TENANT = "0190f1b2-0000-7000-8000-0000000000aa";
function casesFile() {
    for (const relative of ["../test-support", "test-support", "../../test-support"]) {
        const candidate = node_path_1.default.join(__dirname, relative, "record-codec-cases.json");
        if (node_fs_1.default.existsSync(candidate))
            return candidate;
    }
    throw new Error("record-codec-cases.json not found");
}
const cases = JSON.parse(node_fs_1.default.readFileSync(casesFile(), "utf8"));
for (const tc of cases.encode_record) {
    (0, node_test_1.test)(`encodeRecord matches Go: ${tc.name}`, () => {
        const mode = tc.mode === "update" ? recordCodec_1.WriteMode.Update : recordCodec_1.WriteMode.Insert;
        strict_1.default.equal((0, recordCodec_1.recordToJson)((0, recordCodec_1.encodeRecord)(tc.message_type, tc.message, mode)), tc.json);
    });
}
for (const tc of cases.encode_event) {
    (0, node_test_1.test)(`encodeEvent matches Go: ${tc.name}`, () => {
        strict_1.default.equal((0, recordCodec_1.recordToJson)((0, recordCodec_1.encodeEvent)(tc.message_type, tc.message)), tc.json);
    });
}
for (const tc of cases.decode_record) {
    (0, node_test_1.test)(`decodeRecord accepts the stored shape: ${tc.name}`, () => {
        strict_1.default.deepEqual((0, recordCodec_1.decodeRecord)(tc.message_type, tc.record), tc.message);
    });
}
(0, node_test_1.test)("encodeRecord follows the column annotations", () => {
    const file = { tenant_id: TENANT, filename: "notes.pdf", status: "FILE_STATUS_PENDING", file_type: "FILE_TYPE_PDF", expires_at: "2026-10-07T09:30:00Z" };
    const rec = (0, recordCodec_1.encodeRecord)(FILE, file);
    strict_1.default.equal("file_id" in rec, false, "an empty NOT NULL UUID key is dropped so its default applies");
    strict_1.default.equal(rec.project_id, null, "an empty nullable column is NULL");
    strict_1.default.equal(rec.status, "PENDING", "an enum in a VARCHAR column is its short token");
    strict_1.default.equal(rec.file_type, "PDF");
    strict_1.default.equal(rec.expires_at, "2026-10-07T09:30:00Z");
    strict_1.default.equal(rec.tenant_id, TENANT);
    const update = (0, recordCodec_1.encodeRecord)(FILE, { ...file, project_id: "owned-project", file_id: "x" }, recordCodec_1.WriteMode.Update);
    for (const owned of ["file_id", "tenant_id", "project_id"])
        strict_1.default.equal(owned in update, false, `an update must not carry ${owned}`);
});
(0, node_test_1.test)("a 64-bit integer beyond 2^53 stays exact", () => {
    const rec = (0, recordCodec_1.encodeRecord)(FILE, { size_bytes: "9007199254740993" });
    strict_1.default.equal(rec.size_bytes, 9007199254740993n);
    strict_1.default.ok((0, recordCodec_1.recordToJson)(rec).includes('"size_bytes":9007199254740993'));
    strict_1.default.equal((0, recordCodec_1.encodeRecord)(FILE, { size_bytes: 7 }).size_bytes, 7);
    strict_1.default.throws(() => (0, recordCodec_1.encodeRecord)(FILE, { size_bytes: 2 ** 60 }), /exact integer range/, "a rounded number is refused");
    strict_1.default.throws(() => (0, recordCodec_1.encodeRecord)(FILE, { size_bytes: "1".repeat(30) }), /out of range/);
});
(0, node_test_1.test)("a JSON column holds its text as a raw document, and refuses text that is not JSON", () => {
    const workflow = "udb.core.workflow.entity.v1.WorkflowInstance";
    const rec = (0, recordCodec_1.encodeRecord)(workflow, { payload: '{"b":1, "a":12345678901234567890}' });
    strict_1.default.ok(rec.payload instanceof recordCodec_1.RawJson);
    strict_1.default.ok((0, recordCodec_1.recordToJson)(rec).includes('"payload":{"b":1,"a":12345678901234567890}'), "key order and digits survive");
    strict_1.default.equal((0, recordCodec_1.encodeRecord)(workflow, {}).payload, null, "an empty nullable JSON column is NULL");
    strict_1.default.throws(() => (0, recordCodec_1.encodeRecord)(workflow, { payload: "not json" }), /not JSON/);
});
(0, node_test_1.test)("a JSON column's text is what a string field reads and writes, for every kind of value", () => {
    const workflow = "udb.core.workflow.entity.v1.WorkflowInstance";
    // The broker reads a string as JSON text, so a JSON string goes as its text.
    strict_1.default.equal((0, recordCodec_1.encodeRecord)(workflow, { payload: '"My"' }).payload, '"My"');
    strict_1.default.ok((0, recordCodec_1.encodeRecord)(workflow, { payload: "5" }).payload instanceof recordCodec_1.RawJson);
    for (const [stored, text] of [["My", '"My"'], ["5", '"5"'], ["", '""'], ['a"b', '"a\\"b"'], [5, "5"], [[1], "[1]"], [{ a: 1 }, '{"a":1}']]) {
        strict_1.default.equal((0, recordCodec_1.decodeRecord)(workflow, { payload: stored }).payload, text);
    }
    // A store that reads JSON as text returns a document unchanged.
    strict_1.default.equal((0, recordCodec_1.decodeRecord)(workflow, { payload: '{"a":1}' }).payload, '{"a":1}');
    strict_1.default.equal((0, recordCodec_1.decodeRecord)(workflow, { payload: "[1]" }).payload, "[1]");
});
(0, node_test_1.test)("a table row holds a JSON string column as its text, and writes it back unchanged", () => {
    const workflow = "udb.core.workflow.entity.v1.WorkflowInstance";
    const row = (0, recordCodec_1.rowFromRecord)(workflow, { payload: "My", workflow_type: "My" });
    strict_1.default.equal(row.payload, '"My"');
    strict_1.default.equal(row.workflow_type, "My", "a text column is left alone");
    strict_1.default.deepEqual((0, recordCodec_1.rowFromRecord)(workflow, { payload: { a: 1 } }).payload, { a: 1 });
    strict_1.default.equal((0, recordCodec_1.encodeRecord)(workflow, (0, recordCodec_1.messageFromRow)(workflow, { payload: row.payload })).payload, '"My"');
    // Text a caller wrote is not re-read as a stored value: it must be JSON.
    strict_1.default.throws(() => (0, recordCodec_1.encodeRecord)(workflow, (0, recordCodec_1.messageFromRow)(workflow, { payload: "not json" })), /not JSON/);
});
(0, node_test_1.test)("an unknown field and a wrong value type are refused", () => {
    strict_1.default.throws(() => (0, recordCodec_1.encodeRecord)(FILE, { nope: 1 }), /no field "nope"/);
    strict_1.default.throws(() => (0, recordCodec_1.encodeRecord)(FILE, { filename: 5 }), /expected a string/);
    strict_1.default.throws(() => (0, recordCodec_1.encodeRecord)(FILE, { status: "NOT_A_STATUS" }), /not a value/);
    strict_1.default.throws(() => (0, recordCodec_1.encodeRecord)(FILE, { expires_at: "yesterday" }), /RFC 3339/);
    strict_1.default.throws(() => (0, recordCodec_1.encodeRecord)("udb.no.such.Message", {}), /not declared/);
});
(0, node_test_1.test)("decodeRecord fills a message and encode then decode is the identity", () => {
    const stored = JSON.parse(`{"file_id":"f1","tenant_id":"${TENANT}","filename":"notes.pdf","status":"PENDING","file_type":"FILE_TYPE_PDF","scan_verdict":"1","is_public":"true","expires_at":"2026-10-07T09:30:00Z","not_a_column":"ignored"}`);
    const message = (0, recordCodec_1.decodeRecord)(FILE, stored);
    strict_1.default.equal(message.status, "FILE_STATUS_PENDING", "a short token");
    strict_1.default.equal(message.file_type, "FILE_TYPE_PDF", "a full name");
    strict_1.default.equal(typeof message.scan_verdict, "string", "an enum number as text");
    strict_1.default.equal(message.is_public, true, "boolean text");
    strict_1.default.equal("not_a_column" in message, false, "an unknown column is skipped");
    const again = (0, recordCodec_1.decodeRecord)(FILE, (0, recordCodec_1.encodeRecord)(FILE, message));
    for (const field of ["file_id", "filename", "status", "file_type", "is_public", "expires_at"])
        strict_1.default.deepEqual(again[field], message[field], field);
    strict_1.default.throws(() => (0, recordCodec_1.decodeRecord)(FILE, { status: "NOT_A_STATUS" }), /not a value/, "an unknown enum name fails, it does not decode as zero");
    strict_1.default.throws(() => (0, recordCodec_1.decodeRecord)(FILE, { is_public: "maybe" }), /not a boolean/);
    strict_1.default.throws(() => (0, recordCodec_1.decodeRecord)(FILE, { size_bytes: "1.5" }), /not an integer/);
});
(0, node_test_1.test)("field encode and decode follow the same column rules", () => {
    const file = { tenant_id: TENANT, status: "FILE_STATUS_PENDING" };
    strict_1.default.deepEqual((0, recordCodec_1.encodeField)(FILE, file, "status"), { value: "PENDING", present: true });
    strict_1.default.deepEqual((0, recordCodec_1.encodeField)(FILE, file, "project_id"), { value: null, present: true });
    strict_1.default.throws(() => (0, recordCodec_1.encodeField)(FILE, file, "nope"), /no field/);
    const out = {};
    (0, recordCodec_1.decodeField)(FILE, out, "status", "FILE_STATUS_ACTIVE");
    (0, recordCodec_1.decodeField)(FILE, out, "file_type", "PDF");
    strict_1.default.equal(out.status, "FILE_STATUS_ACTIVE");
    strict_1.default.equal(out.file_type, "FILE_TYPE_PDF");
    (0, recordCodec_1.decodeField)(FILE, out, "status", null);
    strict_1.default.equal(out.status, "FILE_STATUS_ACTIVE", "NULL leaves the field unchanged");
});
(0, node_test_1.test)("timestamps format as the broker's text", () => {
    strict_1.default.equal((0, recordCodec_1.formatTimestamp)(new Date("2026-10-15T08:20:00Z")), "2026-10-15T08:20:00.000000Z");
    strict_1.default.equal((0, recordCodec_1.formatTimestamp)("2026-10-15T08:20:00.8Z"), "2026-10-15T08:20:00.800000Z");
    strict_1.default.equal((0, recordCodec_1.formatTimestamp)("2026-10-15T08:20:00.807240999Z"), "2026-10-15T08:20:00.807240Z");
    strict_1.default.equal((0, recordCodec_1.formatTimestamp)("2026-10-15T14:20:00+06:00"), "2026-10-15T08:20:00.000000Z", "a non-UTC time renders in UTC");
    strict_1.default.equal((0, recordCodec_1.parseTimestamp)(""), null);
    strict_1.default.equal((0, recordCodec_1.parseTimestamp)("2026-10-15T08:20:00.800000Z").getTime(), Date.parse("2026-10-15T08:20:00.800Z"));
    strict_1.default.throws(() => (0, recordCodec_1.parseTimestamp)("not a time"), /RFC 3339/);
    strict_1.default.equal((0, recordCodec_1.encodeRecord)(FILE, { expires_at: "2026-10-15T14:20:00.100+06:00" }).expires_at, "2026-10-15T08:20:00.100Z", "proto JSON trims to 0, 3, 6 or 9 digits");
});
(0, node_test_1.test)("parseExactJson keeps large integers and rejects trailing data", () => {
    strict_1.default.deepEqual((0, recordCodec_1.parseExactJson)('{"a":9007199254740993,"b":[1,1.5,"x",null,true]}'), { a: 9007199254740993n, b: [1, 1.5, "x", null, true] });
    strict_1.default.throws(() => (0, recordCodec_1.parseExactJson)('{"a":1} extra'));
    strict_1.default.throws(() => (0, recordCodec_1.parseExactJson)("{bad"));
});
(0, node_test_1.test)("encodeEvent requires a declared topic and carries only populated fields", () => {
    strict_1.default.throws(() => (0, recordCodec_1.encodeEvent)(FILE, {}), /no outbox topic/);
    strict_1.default.deepEqual((0, recordCodec_1.encodeEvent)("udb.core.apikey.events.v1.ApiKeyCreatedEvent", { key_id: "k", tenant_id: TENANT, name: "" }), { key_id: "k", tenant_id: TENANT });
    strict_1.default.deepEqual((0, recordCodec_1.decodeEventPayload)("udb.core.apikey.events.v1.ApiKeyCreatedEvent", { key_id: "k", added_later: true }), { key_id: "k" }, "a newer producer's field is ignored");
    strict_1.default.throws(() => (0, recordCodec_1.decodeEventPayload)("udb.core.apikey.events.v1.ApiKeyCreatedEvent", { key_id: 5 }), /expected a string/);
});
// The presence rules need optional and oneof fields, so they use an application proto.
const PRESENCE = new Map();
function presenceSchemas(sqlType) {
    let schemas = PRESENCE.get(sqlType);
    if (!schemas) {
        schemas = writePresenceProto(sqlType);
        PRESENCE.set(sqlType, schemas);
    }
    return schemas;
}
function writePresenceProto(sqlType) {
    const dir = node_fs_1.default.mkdtempSync(node_path_1.default.join(node_os_1.default.tmpdir(), "udb-codec-"));
    node_fs_1.default.writeFileSync(node_path_1.default.join(dir, "row.proto"), `syntax = "proto3";
package udb.test.codec;
import "udb/core/common/v1/db.proto";
message Row {
  optional string note = 1 [(udb.core.common.v1.pg_column) = { sql_type: "${sqlType}" }];
  oneof choice {
    string text_choice = 2 [(udb.core.common.v1.pg_column) = { sql_type: "${sqlType}" }];
    int64 count_choice = 3 [(udb.core.common.v1.pg_column) = { sql_type: "BIGINT" }];
  }
  string plain = 4 [(udb.core.common.v1.pg_column) = { sql_type: "TEXT" }];
}
`);
    return new protoSchema_1.ProtoSchemas(dir);
}
const ROW = "udb.test.codec.Row";
(0, node_test_1.test)("a present empty string stays empty; an unset one is NULL", () => {
    for (const mode of [recordCodec_1.WriteMode.Insert, recordCodec_1.WriteMode.Update]) {
        for (const sqlType of ["TEXT", "VARCHAR(64)", "CHAR(8)", "UUID", "DATE"]) {
            for (const name of ["note", "text_choice"]) {
                const schemas = presenceSchemas(sqlType);
                const absent = (0, recordCodec_1.encodeRecord)(ROW, {}, mode, schemas);
                strict_1.default.ok(name in absent && absent[name] === null, `unset ${name} encodes as NULL`);
                const record = (0, recordCodec_1.encodeRecord)(ROW, { [name]: "" }, mode, schemas);
                strict_1.default.equal(record[name], "", `present empty ${name} stays empty`);
                strict_1.default.deepEqual((0, recordCodec_1.encodeField)(ROW, { [name]: "" }, name, schemas), { value: "", present: true });
                strict_1.default.equal(record.plain, null, "ordinary empty nullable text keeps its NULL shape");
                strict_1.default.equal((0, recordCodec_1.decodeRecord)(ROW, record, schemas)[name], "", "a round trip keeps explicit presence");
            }
        }
    }
});
(0, node_test_1.test)("a present empty JSON string is refused instead of becoming NULL or dropped", () => {
    const schemas = presenceSchemas("JSONB");
    for (const mode of [recordCodec_1.WriteMode.Insert, recordCodec_1.WriteMode.Update]) {
        strict_1.default.throws(() => (0, recordCodec_1.encodeRecord)(ROW, { note: "" }, mode, schemas), /not JSON/);
    }
    strict_1.default.throws(() => (0, recordCodec_1.encodeField)(ROW, { note: "" }, "note", schemas), /not JSON/);
});
(0, node_test_1.test)("two members of a oneof are refused without changing the message", () => {
    const schemas = presenceSchemas("TEXT");
    const values = { text_choice: "", count_choice: 7 };
    for (const [first, second] of [["text_choice", "count_choice"], ["count_choice", "text_choice"]]) {
        const message = {};
        (0, recordCodec_1.decodeField)(ROW, message, first, values[first], schemas);
        const before = { ...message };
        strict_1.default.throws(() => (0, recordCodec_1.decodeField)(ROW, message, second, values[second], schemas), /oneof choice/);
        strict_1.default.deepEqual(message, before, "a refused column does not replace the existing member");
        (0, recordCodec_1.decodeField)(ROW, message, second, null, schemas);
        strict_1.default.deepEqual(message, before, "a NULL alternative keeps the existing member");
        (0, recordCodec_1.decodeField)(ROW, message, first, values[first], schemas);
        strict_1.default.deepEqual(message, before, "the same member stays writable");
    }
    strict_1.default.throws(() => (0, recordCodec_1.encodeRecord)(ROW, values, recordCodec_1.WriteMode.Insert, schemas), /oneof choice/);
});
(0, node_test_1.test)("the schema reads keys, scope columns and event contracts from annotations", () => {
    const schemas = new protoSchema_1.ProtoSchemas();
    const file = schemas.message(FILE);
    strict_1.default.deepEqual(file.primaryKeys, ["file_id"]);
    strict_1.default.equal(file.tenantColumn, "tenant_id");
    strict_1.default.equal(file.projectColumn, "project_id");
    const event = schemas.message("udb.core.apikey.events.v1.ApiKeyCreatedEvent");
    strict_1.default.equal(event.topic, "udb.apikey.created.v1");
    strict_1.default.equal(event.partitionKeyField, "tenant_id");
    strict_1.default.equal(event.eventType, "udb.core.apikey.events.v1.ApiKeyCreatedEvent");
    strict_1.default.equal(schemas.find("udb.no.such.Message"), undefined);
});
(0, node_test_1.test)("a masked column is no value: it stays unset on decode", () => {
    const file = (0, recordCodec_1.decodeRecord)(FILE, {
        file_id: "f",
        size_bytes: "***MASKED***",
        filename: "***MASKED***",
    });
    strict_1.default.equal(file.file_id, "f");
    strict_1.default.equal(file.size_bytes, undefined);
    // An unprotected text column may really hold the text.
    strict_1.default.equal(file.filename, "***MASKED***");
    const device = (0, recordCodec_1.decodeRecord)("udb.core.authn.entity.v1.Device", { last_ip_masked: "***MASKED***" });
    strict_1.default.equal(device.last_ip_masked, undefined);
});
