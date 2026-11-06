// The record codec against the Go SDK's rules. test-support/record-codec-cases.json
// holds inputs and the exact JSON text Go's json.Marshal(EncodeRecord(...)) produces.

import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { ProtoSchemas } from "./protoSchema";
import {
  RawJson,
  WriteMode,
  decodeEventPayload,
  decodeField,
  decodeRecord,
  encodeEvent,
  encodeField,
  encodeRecord,
  formatTimestamp,
  parseExactJson,
  parseTimestamp,
  recordToJson,
} from "./recordCodec";

const FILE = "udb.core.storage.entity.v1.File";
const TENANT = "0190f1b2-0000-7000-8000-0000000000aa";

function casesFile(): string {
  for (const relative of ["../test-support", "test-support", "../../test-support"]) {
    const candidate = path.join(__dirname, relative, "record-codec-cases.json");
    if (fs.existsSync(candidate)) return candidate;
  }
  throw new Error("record-codec-cases.json not found");
}

const cases = JSON.parse(fs.readFileSync(casesFile(), "utf8"));

for (const tc of cases.encode_record as { name: string; message_type: string; message: any; mode: string; json: string }[]) {
  test(`encodeRecord matches Go: ${tc.name}`, () => {
    const mode = tc.mode === "update" ? WriteMode.Update : WriteMode.Insert;
    assert.equal(recordToJson(encodeRecord(tc.message_type, tc.message, mode)), tc.json);
  });
}

for (const tc of cases.encode_event as { name: string; message_type: string; message: any; json: string }[]) {
  test(`encodeEvent matches Go: ${tc.name}`, () => {
    assert.equal(recordToJson(encodeEvent(tc.message_type, tc.message)), tc.json);
  });
}

for (const tc of cases.decode_record as { name: string; message_type: string; record: any; message: any }[]) {
  test(`decodeRecord accepts the stored shape: ${tc.name}`, () => {
    assert.deepEqual(decodeRecord(tc.message_type, tc.record), tc.message);
  });
}

test("encodeRecord follows the column annotations", () => {
  const file = { tenant_id: TENANT, filename: "notes.pdf", status: "FILE_STATUS_PENDING", file_type: "FILE_TYPE_PDF", expires_at: "2026-10-07T09:30:00Z" };
  const rec = encodeRecord(FILE, file);
  assert.equal("file_id" in rec, false, "an empty NOT NULL UUID key is dropped so its default applies");
  assert.equal(rec.project_id, null, "an empty nullable column is NULL");
  assert.equal(rec.status, "PENDING", "an enum in a VARCHAR column is its short token");
  assert.equal(rec.file_type, "PDF");
  assert.equal(rec.expires_at, "2026-10-07T09:30:00Z");
  assert.equal(rec.tenant_id, TENANT);
  const update = encodeRecord(FILE, { ...file, project_id: "owned-project", file_id: "x" }, WriteMode.Update);
  for (const owned of ["file_id", "tenant_id", "project_id"]) assert.equal(owned in update, false, `an update must not carry ${owned}`);
});

test("a 64-bit integer beyond 2^53 stays exact", () => {
  const rec = encodeRecord(FILE, { size_bytes: "9007199254740993" });
  assert.equal(rec.size_bytes, 9007199254740993n);
  assert.ok(recordToJson(rec).includes('"size_bytes":9007199254740993'));
  assert.equal(encodeRecord(FILE, { size_bytes: 7 }).size_bytes, 7);
  assert.throws(() => encodeRecord(FILE, { size_bytes: 2 ** 60 }), /exact integer range/, "a rounded number is refused");
  assert.throws(() => encodeRecord(FILE, { size_bytes: "1".repeat(30) }), /out of range/);
});

test("a JSON column holds its text as a raw document, and refuses text that is not JSON", () => {
  const workflow = "udb.core.workflow.entity.v1.WorkflowInstance";
  const rec = encodeRecord(workflow, { payload: '{"b":1, "a":12345678901234567890}' });
  assert.ok(rec.payload instanceof RawJson);
  assert.ok(recordToJson(rec).includes('"payload":{"b":1,"a":12345678901234567890}'), "key order and digits survive");
  assert.equal(encodeRecord(workflow, {}).payload, null, "an empty nullable JSON column is NULL");
  assert.throws(() => encodeRecord(workflow, { payload: "not json" }), /not JSON/);
});

test("an unknown field and a wrong value type are refused", () => {
  assert.throws(() => encodeRecord(FILE, { nope: 1 }), /no field "nope"/);
  assert.throws(() => encodeRecord(FILE, { filename: 5 }), /expected a string/);
  assert.throws(() => encodeRecord(FILE, { status: "NOT_A_STATUS" }), /not a value/);
  assert.throws(() => encodeRecord(FILE, { expires_at: "yesterday" }), /RFC 3339/);
  assert.throws(() => encodeRecord("udb.no.such.Message", {}), /not declared/);
});

test("decodeRecord fills a message and encode then decode is the identity", () => {
  const stored = JSON.parse(`{"file_id":"f1","tenant_id":"${TENANT}","filename":"notes.pdf","status":"PENDING","file_type":"FILE_TYPE_PDF","scan_verdict":"1","is_public":"true","expires_at":"2026-10-07T09:30:00Z","not_a_column":"ignored"}`);
  const message = decodeRecord<any>(FILE, stored);
  assert.equal(message.status, "FILE_STATUS_PENDING", "a short token");
  assert.equal(message.file_type, "FILE_TYPE_PDF", "a full name");
  assert.equal(typeof message.scan_verdict, "string", "an enum number as text");
  assert.equal(message.is_public, true, "boolean text");
  assert.equal("not_a_column" in message, false, "an unknown column is skipped");
  const again = decodeRecord<any>(FILE, encodeRecord(FILE, message));
  for (const field of ["file_id", "filename", "status", "file_type", "is_public", "expires_at"]) assert.deepEqual(again[field], message[field], field);
  assert.throws(() => decodeRecord(FILE, { status: "NOT_A_STATUS" }), /not a value/, "an unknown enum name fails, it does not decode as zero");
  assert.throws(() => decodeRecord(FILE, { is_public: "maybe" }), /not a boolean/);
  assert.throws(() => decodeRecord(FILE, { size_bytes: "1.5" }), /not an integer/);
});

test("field encode and decode follow the same column rules", () => {
  const file = { tenant_id: TENANT, status: "FILE_STATUS_PENDING" };
  assert.deepEqual(encodeField(FILE, file, "status"), { value: "PENDING", present: true });
  assert.deepEqual(encodeField(FILE, file, "project_id"), { value: null, present: true });
  assert.throws(() => encodeField(FILE, file, "nope"), /no field/);
  const out: Record<string, unknown> = {};
  decodeField(FILE, out, "status", "FILE_STATUS_ACTIVE");
  decodeField(FILE, out, "file_type", "PDF");
  assert.equal(out.status, "FILE_STATUS_ACTIVE");
  assert.equal(out.file_type, "FILE_TYPE_PDF");
  decodeField(FILE, out, "status", null);
  assert.equal(out.status, "FILE_STATUS_ACTIVE", "NULL leaves the field unchanged");
});

test("timestamps format as the broker's text", () => {
  assert.equal(formatTimestamp(new Date("2026-10-15T08:20:00Z")), "2026-10-15T08:20:00.000000Z");
  assert.equal(formatTimestamp("2026-10-15T08:20:00.8Z"), "2026-10-15T08:20:00.800000Z");
  assert.equal(formatTimestamp("2026-10-15T08:20:00.807240999Z"), "2026-10-15T08:20:00.807240Z");
  assert.equal(formatTimestamp("2026-10-15T14:20:00+06:00"), "2026-10-15T08:20:00.000000Z", "a non-UTC time renders in UTC");
  assert.equal(parseTimestamp(""), null);
  assert.equal(parseTimestamp("2026-10-15T08:20:00.800000Z")!.getTime(), Date.parse("2026-10-15T08:20:00.800Z"));
  assert.throws(() => parseTimestamp("not a time"), /RFC 3339/);
  assert.equal(encodeRecord(FILE, { expires_at: "2026-10-15T14:20:00.100+06:00" }).expires_at, "2026-10-15T08:20:00.100Z", "proto JSON trims to 0, 3, 6 or 9 digits");
});

test("parseExactJson keeps large integers and rejects trailing data", () => {
  assert.deepEqual(parseExactJson('{"a":9007199254740993,"b":[1,1.5,"x",null,true]}'), { a: 9007199254740993n, b: [1, 1.5, "x", null, true] });
  assert.throws(() => parseExactJson('{"a":1} extra'));
  assert.throws(() => parseExactJson("{bad"));
});

test("encodeEvent requires a declared topic and carries only populated fields", () => {
  assert.throws(() => encodeEvent(FILE, {}), /no outbox topic/);
  assert.deepEqual(encodeEvent("udb.core.apikey.events.v1.ApiKeyCreatedEvent", { key_id: "k", tenant_id: TENANT, name: "" }), { key_id: "k", tenant_id: TENANT });
  assert.deepEqual(decodeEventPayload("udb.core.apikey.events.v1.ApiKeyCreatedEvent", { key_id: "k", added_later: true }), { key_id: "k" }, "a newer producer's field is ignored");
  assert.throws(() => decodeEventPayload("udb.core.apikey.events.v1.ApiKeyCreatedEvent", { key_id: 5 }), /expected a string/);
});

// The presence rules need optional and oneof fields, so they use an application proto.
const PRESENCE = new Map<string, ProtoSchemas>();

function presenceSchemas(sqlType: string): ProtoSchemas {
  let schemas = PRESENCE.get(sqlType);
  if (!schemas) {
    schemas = writePresenceProto(sqlType);
    PRESENCE.set(sqlType, schemas);
  }
  return schemas;
}

function writePresenceProto(sqlType: string): ProtoSchemas {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "udb-codec-"));
  fs.writeFileSync(
    path.join(dir, "row.proto"),
    `syntax = "proto3";
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
`,
  );
  return new ProtoSchemas(dir);
}

const ROW = "udb.test.codec.Row";

test("a present empty string stays empty; an unset one is NULL", () => {
  for (const mode of [WriteMode.Insert, WriteMode.Update]) {
    for (const sqlType of ["TEXT", "VARCHAR(64)", "CHAR(8)", "UUID", "DATE"]) {
      for (const name of ["note", "text_choice"]) {
        const schemas = presenceSchemas(sqlType);
        const absent = encodeRecord(ROW, {}, mode, schemas);
        assert.ok(name in absent && absent[name] === null, `unset ${name} encodes as NULL`);
        const record = encodeRecord(ROW, { [name]: "" }, mode, schemas);
        assert.equal(record[name], "", `present empty ${name} stays empty`);
        assert.deepEqual(encodeField(ROW, { [name]: "" }, name, schemas), { value: "", present: true });
        assert.equal(record.plain, null, "ordinary empty nullable text keeps its NULL shape");
        assert.equal(decodeRecord<any>(ROW, record, schemas)[name], "", "a round trip keeps explicit presence");
      }
    }
  }
});

test("a present empty JSON string is refused instead of becoming NULL or dropped", () => {
  const schemas = presenceSchemas("JSONB");
  for (const mode of [WriteMode.Insert, WriteMode.Update]) {
    assert.throws(() => encodeRecord(ROW, { note: "" }, mode, schemas), /not JSON/);
  }
  assert.throws(() => encodeField(ROW, { note: "" }, "note", schemas), /not JSON/);
});

test("two members of a oneof are refused without changing the message", () => {
  const schemas = presenceSchemas("TEXT");
  const values: Record<string, unknown> = { text_choice: "", count_choice: 7 };
  for (const [first, second] of [["text_choice", "count_choice"], ["count_choice", "text_choice"]]) {
    const message: Record<string, unknown> = {};
    decodeField(ROW, message, first, values[first], schemas);
    const before = { ...message };
    assert.throws(() => decodeField(ROW, message, second, values[second], schemas), /oneof choice/);
    assert.deepEqual(message, before, "a refused column does not replace the existing member");
    decodeField(ROW, message, second, null, schemas);
    assert.deepEqual(message, before, "a NULL alternative keeps the existing member");
    decodeField(ROW, message, first, values[first], schemas);
    assert.deepEqual(message, before, "the same member stays writable");
  }
  assert.throws(() => encodeRecord(ROW, values, WriteMode.Insert, schemas), /oneof choice/);
});

test("the schema reads keys, scope columns and event contracts from annotations", () => {
  const schemas = new ProtoSchemas();
  const file = schemas.message(FILE);
  assert.deepEqual(file.primaryKeys, ["file_id"]);
  assert.equal(file.tenantColumn, "tenant_id");
  assert.equal(file.projectColumn, "project_id");
  const event = schemas.message("udb.core.apikey.events.v1.ApiKeyCreatedEvent");
  assert.equal(event.topic, "udb.apikey.created.v1");
  assert.equal(event.partitionKeyField, "tenant_id");
  assert.equal(event.eventType, "udb.core.apikey.events.v1.ApiKeyCreatedEvent");
  assert.equal(schemas.find("udb.no.such.Message"), undefined);
});

test("a masked column is no value: it stays unset on decode", () => {
  const file = decodeRecord<Record<string, unknown>>(FILE, {
    file_id: "f",
    size_bytes: "***MASKED***",
    filename: "***MASKED***",
  });
  assert.equal(file.file_id, "f");
  assert.equal(file.size_bytes, undefined);
  // An unprotected text column may really hold the text.
  assert.equal(file.filename, "***MASKED***");
  const device = decodeRecord<Record<string, unknown>>("udb.core.authn.entity.v1.Device", { last_ip_masked: "***MASKED***" });
  assert.equal(device.last_ip_masked, undefined);
});
