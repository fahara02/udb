// The in-memory broker: it meets the same typed-store contract a real broker does,
// over a real loopback listener with an unmodified UdbProject.

import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import * as grpc from "@grpc/grpc-js";

import { UdbProject } from "./project";
import { ProtoSchemas, protoSchemas } from "./protoSchema";
import { ErrorCode, Table, codeOf, inspect, isNotFound, isUnique, missingScope } from "./stores";
import { createDescriptorFake, createFake, parsePolicies, runTable, runTableCases, start, startPair } from "./udbtest";

const FILE = "udb.core.storage.entity.v1.File";
const EVENT = "udb.core.apikey.events.v1.ApiKeyCreatedEvent";
const TOPIC = "udb.apikey.created.v1";
const TENANT = "0190f1b2-0000-4000-8000-0000000000aa";
const DATABROKER = "udb.services.v1.DataBroker";

test("the fake meets the table contract", async (t) => {
  const fake = await createFake(t, [FILE]);
  await runTable(t, (tenant) => fake.client(tenant));
});

test("the fake holds a JSON column as the broker does, for every kind of value", async (t) => {
  const workflow = "udb.core.workflow.entity.v1.WorkflowInstance";
  const fake = await createFake(t, [workflow]);
  const table = (await fake.client(TENANT)).tableOf<any>(workflow);
  let n = 0;
  for (const payload of ['"My"', '"5"', '""', "5", "[1]", '{"a":1}']) {
    n += 1;
    const id = `0190f1b2-0000-4000-8000-${String(n).padStart(12, "0")}`;
    const row: any = { workflow_id: id, workflow_type: "json", status: "WORKFLOW_STATUS_PENDING", payload };
    for (let i = 0; i < 3; i += 1) {
      await table.upsert(row);
      const stored = await table.get({ workflow_id: id });
      const text = typeof stored.payload === "string" ? stored.payload : JSON.stringify(stored.payload);
      assert.equal(text, payload);
      row.payload = text;
    }
  }
  await assert.rejects(table.upsert({ workflow_id: "0190f1b2-0000-4000-8000-000000000099", workflow_type: "json", payload: "My" }), /not JSON/);
});

test("the fake meets the shared cross-SDK cases", async (t) => {
  const fake = await createFake(t, [FILE]);
  await runTableCases(t, (tenant) => fake.client(tenant));
});

test("emit builds the envelope from the event's contract and commits with the writes", async (t) => {
  const fake = await createFake(t, [FILE]);
  const u = await fake.client(TENANT);
  await u.tx((scope) => {
    scope.upsert(FILE, { file_id: "0190f1b2-0000-4000-8000-000000000001", filename: "a.pdf", object_key: "k" });
    scope.emit(EVENT, { key_id: "key-1", tenant_id: TENANT, name: "ci" });
  });
  const events = fake.events();
  assert.equal(events.length, 1);
  assert.equal(events[0].topic, TOPIC);
  assert.equal(events[0].partitionKey, TENANT);
  assert.equal(events[0].envelope.event_type, EVENT);
  assert.equal(events[0].envelope.envelope_version, 2);
  assert.equal(events[0].envelope.payload.key_id, "key-1", "the event's fields ride under payload");
  assert.equal(fake.rows(FILE, TENANT).length, 1);
  await assert.rejects(u.tx((scope) => scope.emit(EVENT, { key_id: "key-2" })), /partition key/);
  await assert.rejects(u.tx((scope) => {
    scope.emit(EVENT, { key_id: "key-3", tenant_id: TENANT });
    scope.insert(FILE, { file_id: "0190f1b2-0000-4000-8000-000000000001", filename: "dup.pdf", object_key: "k2" });
  }), (err) => isUnique(err));
  assert.equal(fake.events().length, 1, "a refused transaction records no event");
});

test("inspect reads the reason the broker sends in the trailer", async (t) => {
  const fake = await createFake(t, [FILE]);
  const u = await fake.client(TENANT);
  const files = u.tableOf(FILE);
  let refused: unknown;
  await files.delete({ file_id: "0190f1b2-0000-4000-8000-000000000009" }).catch((err) => {
    refused = err;
  });
  const info = inspect(refused);
  assert.equal(info.code, ErrorCode.NotFound);
  assert.equal(info.reason, "UDB_NO_ROWS_AFFECTED");
  assert.equal(isNotFound(refused), true);
  await assert.rejects(files.select({ filename: { $regex: "x" } }), (err) => {
    const got = inspect(err);
    return got.code === ErrorCode.Invalid && got.reason === "UDB_UNSUPPORTED_FILTER_OPERATOR";
  });
});

test("a named consumer resumes after the last event it acknowledged", async (t) => {
  const fake = await createFake(t, [FILE]);
  const u = await fake.client(TENANT);
  for (const key of ["k1", "k2", "k3"]) fake.emit(TENANT, TOPIC, TENANT, { key_id: key, tenant_id: TENANT });
  const handled: string[] = [];
  const run = async (stopAfter: number) => {
    const controller = new AbortController();
    let seen = 0;
    await u.consume<{ key_id: string }>("billing-projector", TOPIC, async (event) => {
      handled.push(event.payload.key_id);
      seen += 1;
      // Completed handlers must flush before the cancelled consumer returns.
      if (seen === stopAfter) controller.abort();
    }, { signal: controller.signal, messageType: EVENT });
  };
  await run(2);
  assert.deepEqual(handled.slice(0, 2), ["k1", "k2"]);
  fake.emit(TENANT, TOPIC, TENANT, { key_id: "k4", tenant_id: TENANT });
  await run(2);
  assert.deepEqual([...handled].sort(), ["k1", "k2", "k3", "k4"], "no event is skipped or handed over twice across a restart");
});

test("a consumer only sees its own tenant, and another principal cannot take its name", async (t) => {
  const fake = await createFake(t, [FILE]);
  const other = "0190f1b2-0000-4000-8000-0000000000ee";
  const u = await fake.client(TENANT);
  fake.emit(other, TOPIC, other, { key_id: "theirs" });
  fake.emit(TENANT, TOPIC, TENANT, { key_id: "mine" });
  const controller = new AbortController();
  const seen: string[] = [];
  await u.consume<{ key_id: string }>("audit", TOPIC, async (event) => {
    seen.push(event.payload.key_id);
    controller.abort();
  }, { signal: controller.signal, messageType: EVENT });
  assert.deepEqual(seen, ["mine"]);
  fake.addIdentityApiKey("other-key", { tenantId: TENANT, serviceIdentity: "another-service", scopes: ["udb:read"] });
  const thief = await fake.clientForKey("other-key");
  await assert.rejects(thief.consume("audit", TOPIC, async () => {}, { backoffMs: 1 }), (err) => inspect(err).reason === "UDB_POLICY_DENIED");
});

test("a poison event reaches the DLQ with one atomic cursor advance, and replays", async (t) => {
  const fake = await createFake(t, [FILE]);
  const u = await fake.client(TENANT);
  fake.emit(TENANT, TOPIC, TENANT, { key_id: "poison" });
  fake.emit(TENANT, TOPIC, TENANT, { key_id: "good" });
  const controller = new AbortController();
  const seen: string[] = [];
  await u.consume<{ key_id: string }>("dlq-test", TOPIC, async (event) => {
    seen.push(event.payload.key_id);
    if (event.payload.key_id === "poison") throw new Error("cannot handle");
    controller.abort();
  }, { signal: controller.signal, maxAttempts: 2, backoffMs: 1, messageType: EVENT });
  assert.deepEqual(seen, ["poison", "poison", "good"]);
  const listed = await u.core.unary<any>(DATABROKER, "ListDlqEvents", {});
  assert.equal(listed.total_count, 1);
  const record = listed.events[0];
  assert.equal(record.status, "QUARANTINED");
  assert.equal(record.error_message, "cannot handle");
  await u.core.unary(DATABROKER, "ReplayDlqEvent", { dlq_id: record.dlq_id });
  assert.equal((await u.core.unary<any>(DATABROKER, "GetDlqEvent", { dlq_id: record.dlq_id })).event.status, "REPLAYED");
  assert.equal(fake.events().length, 3, "a replay appends a new event");
  await assert.rejects(u.core.unary(DATABROKER, "ReplayDlqEvent", { dlq_id: record.dlq_id }), (err) => isNotFound(err));
  fake.addIdentityApiKey("no-admin", { tenantId: TENANT, serviceIdentity: "reader", scopes: ["udb:read"] });
  const reader = await fake.clientForKey("no-admin");
  await assert.rejects(reader.core.unary(DATABROKER, "ListDlqEvents", {}), (err) => missingScope(err) === "udb:admin");
});

test("injected faults are bounded, typed and applied before any write", async (t) => {
  const fake = await createFake(t, [FILE]);
  const u = await fake.client(TENANT);
  const table = new Table(u.core, FILE);
  assert.throws(() => fake.injectFault({ code: grpc.status.OK }), /failure code/);
  assert.throws(() => fake.injectFault({ code: grpc.status.UNAVAILABLE, times: -1 }), /non-negative/);
  const id = "0190f1b2-0000-4000-8000-000000000002";
  const row = { file_id: id, filename: "a.pdf", object_key: "k" };
  fake.injectFault({ method: "/udb.services.v1.DataBroker/Insert", code: grpc.status.UNAVAILABLE, times: 2, retryAfterMs: 50 });
  for (let attempt = 0; attempt < 2; attempt += 1) {
    await assert.rejects(table.insert(row), (err) => {
      const info = inspect(err);
      return info.code === ErrorCode.Unavailable && info.reason === "UDB_BACKEND_UNAVAILABLE" && info.retryAfterMs === 50;
    });
  }
  assert.equal(fake.rows(FILE, TENANT).length, 0, "a fault happens before the mutation");
  await table.insert(row);
  assert.equal(fake.rows(FILE, TENANT).length, 1, "the fault was used up");
  fake.injectFault({ code: grpc.status.INTERNAL, tenantId: "someone-else" });
  await table.get({ file_id: id });
  fake.injectFault({ method: "Authenticate", code: grpc.status.UNAVAILABLE });
  await assert.rejects(fake.client("0190f1b2-0000-4000-8000-0000000000bb"), (err) => codeOf(err) === ErrorCode.Unavailable);
});

test("the rate limit refuses with the broker's reason and a retry delay", async (t) => {
  const fake = await createFake(t, [FILE]);
  const u = await fake.client(TENANT);
  assert.throws(() => fake.setRateLimit(1, 0), /invalid rate limit/);
  fake.setRateLimit(1, 60_000);
  const table = u.tableOf(FILE);
  await table.select({});
  let limited: unknown;
  await table.select({}).catch((err) => {
    limited = err;
  });
  const info = inspect(limited);
  assert.equal(info.code, ErrorCode.RateLimited);
  assert.ok(info.retryAfterMs! > 0);
  fake.setRateLimit(0, 0);
  await table.select({});
});

test("an expired bearer and a missing purpose are refused", async (t) => {
  const fake = await createFake(t, [FILE]);
  const u = await fake.client(TENANT);
  assert.throws(() => fake.setTokenLifetime(10), /at least one second/);
  const bare = new UdbProject({ target: fake.target, tenantId: TENANT, purpose: "test", projectId: "" });
  t.after(() => bare.close());
  await assert.rejects(bare.tableOf(FILE).select({}), (err) => codeOf(err) === ErrorCode.Unauthenticated);
  assert.equal((await u.tableOf(FILE).select({})).rows.length, 0);
  const wrong = new UdbProject({ target: fake.target, tenantId: "another-tenant", purpose: "test", projectId: "", credentials: { bearerToken: (u.core.opts as any).bearerToken } });
  t.after(() => wrong.close());
  await assert.rejects(wrong.tableOf(FILE).select({}), (err) => inspect(err).reason === "UDB_TENANT_MISMATCH");
});

test("a descriptor fake denies by default, and a policy opens exactly what it names", async (t) => {
  const denied = await createDescriptorFake();
  t.after(() => denied.close());
  const client = await denied.client(TENANT);
  await assert.rejects(client.tableOf(FILE).select({}), (err) => {
    const info = inspect(err);
    return info.code === ErrorCode.PolicyDenied && info.missing?.rule === `Select ${FILE}`;
  });
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "udbtest-policy-"));
  const file = path.join(dir, "policies.json");
  fs.writeFileSync(file, JSON.stringify({ policies: [{ id: "read", action: "Select", resource: FILE, required_scopes: ["udb:read"] }, { id: "no", effect: "deny", action: "Delete", resource: FILE }] }));
  const allowed = await createDescriptorFake({ policyFile: file });
  t.after(() => allowed.close());
  const reader = await allowed.client(TENANT);
  assert.equal((await reader.tableOf(FILE).select({})).rows.length, 0);
  await assert.rejects(reader.tableOf(FILE).delete({ file_id: "0190f1b2-0000-4000-8000-000000000003" }), (err) => inspect(err).reason === "UDB_POLICY_DENIED");
  await assert.rejects(createDescriptorFake({ policyFile: path.join(dir, "missing.yaml") }), /policy file/);
});

test("an unknown entity is refused, and rows and events are copies", async (t) => {
  const fake = await createFake(t, [FILE]);
  const u = await fake.client(TENANT);
  await assert.rejects(u.tableOf("udb.core.workflow.entity.v1.WorkflowInstance").select({}), (err) => inspect(err).reason === "UDB_UNKNOWN_MESSAGE_TYPE");
  fake.emit(TENANT, TOPIC, TENANT, { key_id: "k" });
  fake.events()[0].envelope.payload.key_id = "changed";
  assert.equal(fake.events()[0].envelope.payload.key_id, "k");
  const id = "0190f1b2-0000-4000-8000-000000000004";
  await u.tableOf(FILE).insert({ file_id: id, filename: "a.pdf", object_key: "k" });
  fake.rows(FILE, TENANT)[0].filename = "changed";
  assert.equal(fake.rows(FILE, TENANT)[0].filename, "a.pdf");
  assert.equal(fake.rows(FILE, "someone-else").length, 0);
});

test("a YAML policy file opens what it names, like the same file in JSON", async (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "udbtest-yaml-"));
  const file = path.join(dir, "policies.yaml");
  fs.writeFileSync(file, `tenant: ${TENANT}
policies:
  - id: read
    action: Select
    resource: ${FILE}
    required_scopes: [udb:read]
  - id: off
    enabled: no
    action: Delete
    resource: ${FILE}
  - id: other-tenant
    tenant: someone-else
    action: Delete
    resource: ${FILE}
`);
  const fake = await createDescriptorFake({ policyFile: file });
  t.after(() => fake.close());
  const client = await fake.client(TENANT);
  assert.equal((await client.tableOf(FILE).select({})).rows.length, 0);
  await assert.rejects(client.tableOf(FILE).delete({ file_id: TENANT }), (err) => inspect(err).reason === "UDB_POLICY_DENIED", "a disabled policy and another tenant's open nothing");
  const parsed = parsePolicies(fs.readFileSync(file, "utf8"));
  assert.equal(parsed[0].tenant, TENANT, "a policy inherits the document's tenant");
  assert.equal(parsed[1].enabled, false);
  assert.equal(parsed[2].tenant, "someone-else", "its own tenant wins");
  assert.deepEqual(parsePolicies(JSON.stringify(parsed)), parsed, "the JSON form reads the same");
  assert.deepEqual(parsePolicies("- action: Select\n"), [{ action: "Select" }], "a bare list is a document");
});

test("a malformed policy file is refused before the fake serves", async () => {
  const refused: [string, RegExp][] = [
    ["", /empty policy document/],
    ["# only a comment\n", /empty policy document/],
    ["policies: [\n", /parse policy file/],
    ["- a: 1\n---\n- b: 2\n", /one document/],
    ["just text\n", /policy list or policies document/],
    ["tenant: t\n", /requires policies/],
    ["policies: nope\n", /policies must be a list/],
    ["- not-a-mapping\n", /must be a mapping/],
    ["- effect: maybe\n", /invalid effect/],
    ["- enabled: sometimes\n", /enabled must be a boolean/],
    ["- priority: high\n", /priority must be an integer/],
    ["- required_scopes: udb:read\n", /required_scopes must be a list/],
    ["- conditions: [a]\n", /conditions must be a mapping/],
    ["- action: {a: 1}\n", /action must be a scalar/],
    ["- action: a\n  action: b\n", /parse policy file/],
  ];
  for (const [source, want] of refused) assert.throws(() => parsePolicies(source), want, JSON.stringify(source));
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "udbtest-bad-"));
  const file = path.join(dir, "policies.yml");
  fs.writeFileSync(file, "- effect: maybe\n");
  await assert.rejects(createDescriptorFake({ policyFile: file }), /invalid effect/);
});

/** A compiled descriptor set, on the wire, with the udb annotations as extensions. */
function descriptorSet(file: Record<string, unknown>): Uint8Array {
  const type = protoSchemas().protoType("google.protobuf.FileDescriptorSet");
  return type.encode(type.fromObject({ file: [file] })).finish();
}

const PG_COLUMN = ".udb.core.common.v1.pg_column";
const PG_TABLE = ".udb.core.common.v1.pg_table";

function noteFile(): Record<string, unknown> {
  const column = (name: string, number: number, type: string, options: Record<string, unknown>, extra: Record<string, unknown> = {}) => ({
    name,
    number,
    type,
    label: "LABEL_OPTIONAL",
    options: { [PG_COLUMN]: options },
    ...extra,
  });
  return {
    name: "acme/notes.proto",
    package: "acme.notes.v1",
    syntax: "proto3",
    enumType: [{ name: "NoteKind", value: [{ name: "NOTE_KIND_UNSPECIFIED", number: 0 }, { name: "NOTE_KIND_MEMO", number: 1 }] }],
    messageType: [
      {
        name: "Note",
        options: { [PG_TABLE]: { table_name: "notes", is_table: true } },
        oneofDecl: [{ name: "_title" }],
        field: [
          column("note_id", 1, "TYPE_STRING", { sql_type: "UUID", primary_key: true, not_null: true }),
          column("tenant_id", 2, "TYPE_STRING", { sql_type: "UUID", not_null: true, tenant_column: true }),
          column("title", 3, "TYPE_STRING", { sql_type: "TEXT" }, { oneofIndex: 0, proto3Optional: true }),
          column("kind", 4, "TYPE_ENUM", { sql_type: "VARCHAR(16)", not_null: true }, { typeName: ".acme.notes.v1.NoteKind" }),
          column("slug", 5, "TYPE_STRING", { sql_type: "TEXT", unique: true }),
          column("hits", 6, "TYPE_INT64", { sql_type: "BIGINT", not_null: true }),
        ],
      },
      { name: "NotATable", field: [{ name: "x", number: 1, type: "TYPE_STRING", label: "LABEL_OPTIONAL" }] },
    ],
  };
}

test("a descriptor-set fake serves its tables with their annotations, under a policy", async (t) => {
  const bytes = descriptorSet(noteFile());
  const schemas = ProtoSchemas.fromDescriptorSet(bytes);
  const NOTE = "acme.notes.v1.Note";
  const note = schemas.message(NOTE);
  assert.deepEqual(note.primaryKeys, ["note_id"]);
  assert.equal(note.tenantColumn, "tenant_id");
  assert.equal(note.field("title")?.hasPresence, true);
  assert.equal(note.field("title")?.oneof, undefined, "proto3 optional is not a real oneof");
  assert.deepEqual(schemas.tables().map((table) => table.fullName), [NOTE], "only annotated tables register");
  const fake = await createDescriptorFake({
    descriptorSet: bytes,
    policies: [{ id: "all", action: "*", resource: "*" }],
  });
  t.after(() => fake.close());
  const client = await fake.client(TENANT);
  const notes = client.tableOf(NOTE, { schemas });
  const id = "0190f1b2-0000-4000-8000-000000000010";
  await notes.insert({ note_id: id, kind: "NOTE_KIND_MEMO", slug: "a" });
  const stored = await notes.get({ note_id: id });
  assert.equal(stored.kind, "MEMO", "the enum column follows the descriptor");
  assert.equal(stored.title, null, "an unset optional is NULL");
  assert.equal(stored.tenant_id, TENANT, "the tenant column follows the descriptor");
  await notes.increment({ note_id: id }, { hits: 2 });
  assert.equal((await notes.get({ note_id: id })).hits, 2);
  await assert.rejects(notes.insert({ note_id: "0190f1b2-0000-4000-8000-000000000011", slug: "a" }), (err) => isUnique(err) && inspect(err).constraint === "notes_slug_key", "a declared unique key is enforced");
  await assert.rejects(client.tableOf(FILE).select({}), (err) => inspect(err).reason === "UDB_UNKNOWN_MESSAGE_TYPE", "a table outside the set is not served");
  // Negative: a denying fake over the same set.
  const closed = await createDescriptorFake({ descriptorSet: bytes });
  t.after(() => closed.close());
  await assert.rejects((await closed.client(TENANT)).tableOf(NOTE, { schemas }).select({}), (err) => inspect(err).reason === "UDB_POLICY_DENIED");
});

test("a bad descriptor set is refused", async () => {
  await assert.rejects(createDescriptorFake({ descriptorSet: Buffer.from([0xff, 0xff, 0xff]) }), /descriptor set:/, "not a descriptor set");
  const missingImport = noteFile();
  (missingImport.messageType as any[])[0].field.push({ name: "at", number: 7, type: "TYPE_MESSAGE", label: "LABEL_OPTIONAL", typeName: ".google.protobuf.Timestamp" });
  await assert.rejects(createDescriptorFake({ descriptorSet: descriptorSet(missingImport) }), /descriptor set:/, "an import missing from the set");
  const noTables = noteFile();
  noTables.messageType = (noTables.messageType as any[]).slice(1);
  await assert.rejects(createDescriptorFake({ descriptorSet: descriptorSet(noTables) }), /declares no tables/);
  await assert.rejects(createDescriptorFake({ descriptorSet: descriptorSet(noteFile()), protoRoot: "x" }), /not both/);
});

// Runs in an embedded CI lane with that lane's compiled broker: the same contract used
// by the fake and an external live broker.
test("an embedded broker meets the table contract", {
  skip: process.env.UDB_EMBEDDED_SDK_TESTS === "1" ? false : "requires an embedded broker (UDB_EMBEDDED_SDK_TESTS=1)",
}, async (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "udbtest-embedded-"));
  const policy = path.join(dir, "policies.yaml");
  fs.writeFileSync(policy, `policies:
  - id: embedded-table-contract
    action: '*'
    resource: ${FILE}
    purpose: udbtest.embedded
    required_scopes: [udb:write, udb:read]
`);
  const repoProto = path.resolve(__dirname, "..", "..", "..", "proto");
  const [u, peer] = await startPair(t, repoProto, policy);
  const tenantOf = (client: UdbProject) => client.core.opts.meta.tenantId;
  await runTable(t, (tenant) => (tenant === tenantOf(peer) ? peer : u), { tenants: [tenantOf(u), tenantOf(peer)] });
  await runTableCases(t, () => u);
  assert.ok(start, "start is exported alongside startPair");
});
