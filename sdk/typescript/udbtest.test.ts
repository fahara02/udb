// The in-memory broker: it meets the same typed-store contract a real broker does,
// over a real loopback listener with an unmodified UdbProject.

import { randomUUID } from "node:crypto";
import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import * as grpc from "@grpc/grpc-js";

import { UdbProject } from "./project";
import { ProtoSchemas, protoSchemas } from "./protoSchema";
import { ErrorCode, Table, codeOf, inspect, isNotFound, isUnique, missingScope, selectMany } from "./stores";
import { DELETED_READ_SCOPE, createDescriptorFake, createFake, parsePolicies, runJoin, runTable, runTableCases, start, startPair } from "./udbtest";
import { uniquePredicate } from "./udbtest/entityRules";
import { matches } from "./udbtest/filter";
import { decodeRecord, encodeRecord } from "./recordCodec";
import { UdbColumn, udbEnumFromText, udbEnumToText } from "./entity";
import { approvalPlanArgs, embeddedEnvironment } from "./udbtest/embedded";

const FILE = "udb.core.storage.entity.v1.File";
const EVENT = "udb.core.apikey.events.v1.ApiKeyCreatedEvent";
const TOPIC = "udb.apikey.created.v1";
const TENANT = "0190f1b2-0000-4000-8000-0000000000aa";
const DATABROKER = "udb.services.v1.DataBroker";

test("the fake meets the table contract", async (t) => {
  const fake = await createFake(t, [FILE]);
  await runTable(t, (tenant) => fake.client(tenant));
});

test("the fake answers a join-fusion read as the broker does", async (t) => {
  const fake = await createFake(t, [FILE, "udb.core.authz.entity.v1.PolicySet", "udb.core.authz.entity.v1.PolicyVersion"]);
  await runJoin(t, (tenant) => fake.client(tenant));
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

test("a JSON column read back is accepted as the expected value of the next write", async (t) => {
  // A row read back and sent as `expected` must match the row it came from, for an
  // empty JSON list as for any other document (MedDesk: medications "[]").
  const workflow = "udb.core.workflow.entity.v1.WorkflowInstance";
  const fake = await createFake(t, [workflow]);
  const u = await fake.client(TENANT);
  const table = u.tableOf<any>(workflow);
  let n = 100;
  for (const payload of ["[]", "{}", "[1]", '{"a":[]}', [], {}, [1], { a: [] }] as unknown[]) {
    n += 1;
    const id = `0190f1b2-0000-4000-8000-${String(n).padStart(12, "0")}`;
    await table.upsert({ workflow_id: id, workflow_type: "json", status: "WORKFLOW_STATUS_PENDING", payload });
    const stored = await table.get({ workflow_id: id });
    await table.updateIf({ workflow_id: id }, { payload: stored.payload }, { workflow_type: "json-2" });
    // The atomic-commit path: the whole row read back is the expectation.
    const before = await table.get({ workflow_id: id });
    await u.tx((scope) => scope.updateRowIf(workflow, { ...before, workflow_type: "json-3" }, before));
    assert.equal((await table.get({ workflow_id: id })).workflow_type, "json-3", JSON.stringify(payload));
  }
});

test("a raw record with an empty JSON list commits against itself as expected", async (t) => {
  // MedDesk stages raw mapped records: insert, then updateIf with that record as
  // the expectation. An empty list must compare equal to what was stored.
  const workflow = "udb.core.workflow.entity.v1.WorkflowInstance";
  const fake = await createFake(t, [workflow]);
  const u = await fake.client(TENANT);
  let n = 200;
  for (const payload of [[], {}, [1], "[]"] as unknown[]) {
    n += 1;
    const id = `0190f1b2-0000-4000-8000-${String(n).padStart(12, "0")}`;
    const record = { workflow_id: id, workflow_type: "json", payload };
    await u.tx((scope) => scope.insert(workflow, record));
    await u.tx((scope) => scope.updateIf(workflow, { workflow_id: id }, record, { workflow_type: "json-2" }));
    assert.equal(fake.rows(workflow, TENANT).find((row: any) => row.workflow_id === id)?.workflow_type, "json-2", JSON.stringify(payload));
  }
});

test("the fake meets the shared cross-SDK cases", async (t) => {
  const fake = await createFake(t, [FILE]);
  await runTableCases(t, (tenant) => fake.client(tenant));
});

test("selectMany reads several entities in one BatchSelect, in request order, and one bad read rejects", async (t) => {
  const workflow = "udb.core.workflow.entity.v1.WorkflowInstance";
  const fake = await createFake(t, [FILE, workflow]);
  const u = await fake.client(TENANT);
  const fileA = "0190f1b2-0000-4000-8000-000000000301";
  const fileB = "0190f1b2-0000-4000-8000-000000000302";
  const flow = "0190f1b2-0000-4000-8000-000000000303";
  await u.tableOf<any>(FILE).upsert({ file_id: fileA, filename: "a.pdf", object_key: `many/${fileA}` });
  await u.tableOf<any>(FILE).upsert({ file_id: fileB, filename: "b.pdf", object_key: `many/${fileB}` });
  await u.tableOf<any>(workflow).upsert({ workflow_id: flow, workflow_type: "many", status: "WORKFLOW_STATUS_PENDING" });
  for (let i = 0; i < 3; i += 1) {
    const results = await selectMany<any>(u.core, [
      { messageType: FILE, where: { file_id: fileB } },
      { messageType: workflow, where: { workflow_id: flow } },
      { messageType: FILE, where: { file_id: fileA } },
      { messageType: FILE, where: { file_id: { $in: [fileA, fileB] } }, orderBy: ["-filename"], limit: 1 },
      { messageType: workflow, where: { workflow_id: "0190f1b2-0000-4000-8000-000000000399" } },
    ]);
    assert.equal(results.length, 5);
    assert.equal(results[0].rows[0]?.filename, "b.pdf", "results keep request order");
    assert.equal(results[1].rows[0]?.workflow_type, "many");
    assert.equal(results[2].rows[0]?.filename, "a.pdf");
    assert.deepEqual(results[3].rows.map((row) => row.filename), ["b.pdf"]);
    assert.equal(results[3].hasMore, true);
    assert.equal(results[4].rows.length, 0, "a read matching nothing returns no rows");
  }
  // One failing read rejects the whole call with that read's error.
  await assert.rejects(
    selectMany(u.core, [
      { messageType: FILE, where: {} },
      { messageType: "udb.nope.v1.Missing", where: {} },
    ]),
    (err: unknown) => codeOf(err) === ErrorCode.Invalid,
  );
  assert.deepEqual(await selectMany(u.core, []), [], "no reads, no call");
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

test("array filters: $contains, $contained_by, $overlaps/$overlap on an array column", async (t) => {
  const audit = "udb.core.authz.entity.v1.AccessDecisionAudit";
  const fake = await createFake(t, [audit]);
  const table = (await fake.client(TENANT)).tableOf<any>(audit);
  const rows: [string, string[]][] = [
    ["0190f1b2-0000-4000-8000-000000000101", ["p1", "p2"]],
    ["0190f1b2-0000-4000-8000-000000000102", ["p2", "p3"]],
    ["0190f1b2-0000-4000-8000-000000000103", []],
  ];
  for (const [id, matched] of rows) {
    await table.upsert({
      decision_audit_id: id, user_id: TENANT, domain: "d", object: "o", action: "read",
      effect: "POLICY_EFFECT_ALLOW", decision_source: "DECISION_SOURCE_ROLE_POLICY", tenant_id: TENANT, matched_policy_ids: matched,
    });
  }
  const ids = async (filter: any) =>
    (await table.select(filter)).rows.map((row: any) => row.decision_audit_id.slice(-3)).sort();
  assert.deepEqual(await ids({ matched_policy_ids: { $contains: ["p2"] } }), ["101", "102"]);
  assert.deepEqual(await ids({ matched_policy_ids: { $contains: ["p1", "p2"] } }), ["101"]);
  assert.deepEqual(await ids({ matched_policy_ids: { $contained_by: ["p1", "p2", "p9"] } }), ["101", "103"]);
  assert.deepEqual(await ids({ matched_policy_ids: { $overlaps: ["p3", "p9"] } }), ["102"]);
  assert.deepEqual(await ids({ matched_policy_ids: { $overlap: ["p1", "p3"] } }), ["101", "102"]);
  assert.deepEqual(await ids({ matched_policy_ids: { $overlaps: [] } }), []);
  assert.deepEqual(await ids({ matched_policy_ids: { $not: { $contains: ["p1"] } } }), ["102", "103"]);
  // A NULL column matches no array operator, nor its negation.
  for (const op of ["$contains", "$contained_by", "$overlaps"]) {
    assert.equal(matches({ c: null }, { c: { [op]: ["p1"] } }), false, op);
    assert.equal(matches({ c: null }, { c: { $not: { [op]: ["p1"] } } }), false, op);
  }
  await assert.rejects(table.select({ matched_policy_ids: { $contains: "p1" } }), /requires an array value/);
  await assert.rejects(table.select({ domain: { $contains: ["d"] } }), /array \(repeated\) or JSON column/);
});

test("a JSON column takes JSON containment for $contains and $contained_by", async (t) => {
  const workflow = "udb.core.workflow.entity.v1.WorkflowInstance";
  const fake = await createFake(t, [workflow]);
  const table = (await fake.client(TENANT)).tableOf<any>(workflow);
  const docs: [string, string][] = [
    ["201", '{"kind":"invoice","tags":["a","b"],"n":1}'],
    ["202", '{"kind":"receipt","tags":["b"]}'],
    ["203", '["x","y"]'],
  ];
  for (const [n, payload] of docs) {
    await table.upsert({ workflow_id: `0190f1b2-0000-4000-8000-000000000${n}`, workflow_type: "json", status: "WORKFLOW_STATUS_PENDING", payload });
  }
  await table.upsert({ workflow_id: "0190f1b2-0000-4000-8000-000000000204", workflow_type: "json", status: "WORKFLOW_STATUS_PENDING", payload: null });
  const ids = async (filter: any) => (await table.select(filter)).rows.map((row: any) => row.workflow_id.slice(-3)).sort();
  assert.deepEqual(await ids({ payload: { $contains: { kind: "invoice" } } }), ["201"]);
  assert.deepEqual(await ids({ payload: { $contains: { tags: ["b"] } } }), ["201", "202"]);
  assert.deepEqual(await ids({ payload: { $contains: { n: "1" } } }), [], "a JSON string is not the number 1");
  assert.deepEqual(await ids({ payload: { $contains: "x" } }), ["203"], "a top-level array contains a bare scalar it holds");
  assert.deepEqual(await ids({ payload: { $contains: ["y"] } }), ["203"]);
  assert.deepEqual(await ids({ payload: { $contained_by: { kind: "receipt", tags: ["b", "c"], extra: true } } }), ["202"]);
  await assert.rejects(table.select({ payload: { $overlaps: ["x"] } }), /needs an array column/);
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
  // The window is aligned to the clock, so a boundary can fall between two calls and let
  // both through. It can fall only once among a few quick calls.
  let limited: unknown;
  for (let i = 0; i < 3 && limited === undefined; i += 1) {
    await table.select({}).catch((err) => {
      limited = err;
    });
  }
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

/** The note file with a `code` column unique among rows matching `where`. */
function partialUniqueFile(where: string): Record<string, unknown> {
  const file = noteFile();
  const note = (file.messageType as any[])[0];
  const text = (name: string, number: number) => ({ name, number, type: "TYPE_STRING", label: "LABEL_OPTIONAL", options: { [PG_COLUMN]: { sql_type: "TEXT" } } });
  note.field.push(text("code", 7), text("retired_at", 8), text("deleted_at", 9), text("role", 10));
  note.options[PG_TABLE].indexes = [{ index_name: "notes_code_live", unique: true, columns: ["code"], where_clause: where }];
  return file;
}

test("a partial unique index with a conjunction holds only among the rows it names", async (t) => {
  const NOTE = "acme.notes.v1.Note";
  let n = 20;
  const id = () => `0190f1b2-0000-4000-8000-0000000000${n++}`;
  const open = async (where: string) => {
    const bytes = descriptorSet(partialUniqueFile(where));
    const fake = await createDescriptorFake({ descriptorSet: bytes, policies: [{ id: "all", action: "*", resource: "*" }] });
    t.after(() => fake.close());
    return (await fake.client(TENANT)).tableOf(NOTE, { schemas: ProtoSchemas.fromDescriptorSet(bytes) });
  };
  const unique = (err: unknown) => isUnique(err) && inspect(err).constraint === "notes_code_live";

  const live = await open("retired_at IS NULL AND deleted_at IS NULL");
  await live.insert({ note_id: id(), kind: "NOTE_KIND_MEMO", code: "a" });
  await live.insert({ note_id: id(), kind: "NOTE_KIND_MEMO", code: "a", retired_at: "2026-01-01" });
  await live.insert({ note_id: id(), kind: "NOTE_KIND_MEMO", code: "a", deleted_at: "2026-01-01" });
  await assert.rejects(live.insert({ note_id: id(), kind: "NOTE_KIND_MEMO", code: "a" }), unique, "two live rows collide");

  const primary = await open("role = 'primary' AND deleted_at IS NULL");
  await primary.insert({ note_id: id(), kind: "NOTE_KIND_MEMO", code: "b", role: "primary" });
  await primary.insert({ note_id: id(), kind: "NOTE_KIND_MEMO", code: "b", role: "backup" });
  await primary.insert({ note_id: id(), kind: "NOTE_KIND_MEMO", code: "b" });
  await primary.insert({ note_id: id(), kind: "NOTE_KIND_MEMO", code: "b", role: "primary", deleted_at: "2026-01-01" });
  await assert.rejects(primary.insert({ note_id: id(), kind: "NOTE_KIND_MEMO", code: "b", role: "primary" }), unique, "two live primaries collide");

  // Negative: a predicate the fake cannot read is refused, never dropped.
  const or = await open("retired_at IS NULL OR deleted_at IS NULL");
  await assert.rejects(or.insert({ note_id: id(), kind: "NOTE_KIND_MEMO", code: "c" }), /unsupported unique-index predicate/);
});

/** The note file whose `kind` column declares `enumValues`, so its CHECK lists them. */
function checkedKindFile(enumValues: string[]): Record<string, unknown> {
  const file = noteFile();
  (file.enumType as any[])[0].value.push({ name: "NOTE_KIND_TASK", number: 2 });
  const kind = (file.messageType as any[])[0].field.find((field: any) => field.name === "kind");
  kind.options[PG_COLUMN].enum_values = enumValues;
  return file;
}

test("a text enum column stores the form its enum_values declare, and the fake enforces its CHECK", async (t) => {
  const NOTE = "acme.notes.v1.Note";
  let n = 40;
  const id = () => `0190f1b2-0000-4000-8000-0000000000${n++}`;
  const check = (err: unknown) =>
    (err as grpc.ServiceError).code === grpc.status.FAILED_PRECONDITION && /breaks constraint chk_notes_kind_enum/.test((err as Error).message);
  for (const [declared, stored] of [
    [["NOTE_KIND_UNSPECIFIED", "NOTE_KIND_MEMO"], "NOTE_KIND_MEMO"],
    [["UNSPECIFIED", "MEMO"], "MEMO"],
  ] as const) {
    const bytes = descriptorSet(checkedKindFile([...declared]));
    const schemas = ProtoSchemas.fromDescriptorSet(bytes);
    // The writer: the declared form, whichever form the caller used; a value outside throws.
    assert.equal(encodeRecord(NOTE, { kind: "NOTE_KIND_MEMO" }, undefined, schemas).kind, stored);
    assert.throws(() => encodeRecord(NOTE, { kind: "NOTE_KIND_TASK" }, undefined, schemas), /breaks constraint chk_notes_kind_enum/);
    const fake = await createDescriptorFake({ descriptorSet: bytes, policies: [{ id: "all", action: "*", resource: "*" }] });
    t.after(() => fake.close());
    const client = await fake.client(TENANT);
    const notes = client.tableOf(NOTE, { schemas });
    const full = id();
    await notes.insert({ note_id: full, kind: "NOTE_KIND_MEMO" });
    assert.equal((await notes.get({ note_id: full })).kind, stored, "a full name is accepted and stored as declared");
    const short = id();
    await notes.insert({ note_id: short, kind: "MEMO" });
    assert.equal((await notes.get({ note_id: short })).kind, stored, "a short token is accepted and stored as declared");
    // The reader returns the full name from either stored form.
    assert.equal(decodeRecord<any>(NOTE, { kind: stored }, schemas).kind, "NOTE_KIND_MEMO");
    // Negative: the typed table refuses a value outside the list before sending it, and the
    // fake, sent the raw record, refuses it and junk with the broker's check violation.
    await assert.rejects(notes.insert({ note_id: id(), kind: "NOTE_KIND_TASK" }), /breaks constraint chk_notes_kind_enum/);
    for (const kind of ["NOTE_KIND_TASK", "TASK", "NOT_A_KIND"]) {
      await assert.rejects(client.tx((scope) => scope.insert(NOTE, { note_id: id(), kind })), check, kind);
    }
  }
  // A column that declares no enum_values keeps the short token and has no CHECK.
  const plain = ProtoSchemas.fromDescriptorSet(descriptorSet(noteFile()));
  assert.equal(encodeRecord(NOTE, { kind: "NOTE_KIND_MEMO" }, undefined, plain).kind, "MEMO");
});

test("generated mappers store the declared enum form, or the short token without one", () => {
  assert.equal(udbEnumToText("ORDER_STATUS_OPEN", "ORDER_STATUS_"), "OPEN", "no declared values: the short token");
  assert.equal(udbEnumToText("OPEN", "ORDER_STATUS_", ["ORDER_STATUS_OPEN"]), "ORDER_STATUS_OPEN");
  assert.equal(udbEnumToText("ORDER_STATUS_OPEN", "ORDER_STATUS_", ["OPEN", "CLOSED"]), "OPEN");
  assert.throws(() => udbEnumToText("ORDER_STATUS_CLOSED", "ORDER_STATUS_", ["ORDER_STATUS_OPEN"]), /breaks its CHECK constraint/);
  assert.equal(udbEnumFromText("OPEN", "ORDER_STATUS_"), "ORDER_STATUS_OPEN", "a short token reads as the full name");
  assert.equal(udbEnumFromText("ORDER_STATUS_OPEN", "ORDER_STATUS_"), "ORDER_STATUS_OPEN");
  assert.deepEqual(new UdbColumn<string>("status", "ORDER_STATUS_", ["ORDER_STATUS_OPEN"]).eq("OPEN"), { status: { $eq: "ORDER_STATUS_OPEN" } });
});

test("the unique-index predicate reader takes conjunctions and refuses everything else", () => {
  const columns = (column: string) => (["retired_at", "deleted_at", "kind", "label"].includes(column) ? column : undefined);
  const both = uniquePredicate(`retired_at IS NULL and "Deleted_At" is null`, columns);
  assert.equal(both({ retired_at: null, deleted_at: null }), true);
  assert.equal(both({ retired_at: "x", deleted_at: null }), false);
  assert.equal(both({ retired_at: null, deleted_at: "x" }), false);
  const notEqual = uniquePredicate(`label <> 'it''s and more'`, columns);
  assert.equal(notEqual({ label: "it's and more" }), false);
  assert.equal(notEqual({ label: "x" }), true);
  assert.equal(notEqual({ label: null }), false, "NULL makes <> false");
  for (const where of ["retired_at IS NULL OR deleted_at IS NULL", "(deleted_at IS NULL)", "lower(kind) = 'primary'", "unknown_column IS NULL", "kind > 'a'"]) {
    assert.throws(() => uniquePredicate(where, columns), /unsupported unique-index predicate/, where);
  }
});

test("the embedded broker's environment drops UDB_* settings and carries only the approval plan it is given", () => {
  const source = { PATH: "/bin", UDB_PG_DSN: "postgres://other", udb_grpc_addr: "x", DATABASE_URL: "postgres://x", UDB_HOME: "/h", UDB_MIGRATION_APPROVAL_PLAN: "/stale.json" };
  const plain = embeddedEnvironment(source, "/proto");
  assert.deepEqual(plain, { PATH: "/bin", UDB_HOME: "/h", UDB_PROTO_ROOT: "/proto" }, "a caller's approval plan is never inherited");
  const approved = embeddedEnvironment(source, "/proto", "/project/approval.json");
  assert.equal(approved.UDB_MIGRATION_APPROVAL_PLAN, "/project/approval.json");
  assert.equal(approved.UDB_PG_DSN, undefined);
  assert.deepEqual(approvalPlanArgs("/project/approval.json"), ["plan", "--emit-approval-plan", "/project/approval.json"]);
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

test("the fake meets the include_deleted contract for a caller holding udb:deleted:read", async (t) => {
  const fake = await createFake(t, [FILE]);
  await runTable(t, (tenant) => {
    const key = `deleted-reader-${tenant}`;
    fake.addIdentityApiKey(key, {
      tenantId: tenant,
      userId: "auditor",
      scopes: ["udb:read", "udb:write", "udb:admin", DELETED_READ_SCOPE],
    });
    return fake.clientForKey(key);
  });
});

test("an include_deleted read stays in the tenant, is recorded, and is refused without soft delete", async (t) => {
  const audit = "udb.core.authz.entity.v1.AccessDecisionAudit";
  const fake = await createFake(t, [FILE, audit]);
  const tenant = randomUUID();
  fake.addIdentityApiKey("reader", { tenantId: tenant, userId: "auditor", scopes: ["udb:read", "udb:write", DELETED_READ_SCOPE] });
  const reader = await fake.clientForKey("reader");
  const foreign = (await fake.client(randomUUID())).tableOf<Record<string, any>>(FILE);
  const theirs = { file_id: randomUUID(), filename: "theirs.pdf", object_key: `t/${randomUUID()}`, status: "FILE_STATUS_PENDING" };
  await foreign.insert(theirs);
  await foreign.delete({ file_id: theirs.file_id });
  const files = reader.tableOf<Record<string, any>>(FILE);
  const mine = { file_id: randomUUID(), filename: "mine.pdf", object_key: `m/${randomUUID()}`, status: "FILE_STATUS_PENDING" };
  await files.insert(mine);
  await files.delete({ file_id: mine.file_id });

  const page = await files.select({}, { includeDeleted: true, fields: ["file_id"] });
  assert.deepEqual(page.rows.map((row) => row.file_id), [mine.file_id], "only the caller's deleted row");
  assert.ok(page.rows[0].deleted_at, "the deleted row is marked even with a field list");
  const reads = fake.deletedReads();
  assert.equal(reads.length, 1);
  assert.deepEqual(
    { ...reads[0], table: "" },
    { tenantId: tenant, projectId: "", actor: "auditor", messageType: FILE, table: "", rows: 1 },
  );
  assert.ok(reads[0].table, "the record names the table");

  await assert.rejects(
    reader.tableOf<Record<string, any>>(audit).select({}, { includeDeleted: true }),
    (err: unknown) => codeOf(err) === ErrorCode.Invalid,
  );
  assert.equal(fake.deletedReads().length, 1, "a refused read is not recorded");

  const plain = await fake.client(tenant);
  await assert.rejects(
    plain.tableOf<Record<string, any>>(FILE).select({}, { includeDeleted: true }),
    (err: unknown) => inspect(err).reason === "UDB_DELETED_READ_SCOPE_MISSING" && codeOf(err) === ErrorCode.ScopeMissing,
  );
});

test("the fake loads rows in BulkUpsert batches with one change event per batch", async (t) => {
  const fake = await createFake(t, [FILE]);
  const u = await fake.client(TENANT);
  const ids: string[] = [];
  function* rows() {
    for (let i = 0; i < 2500; i += 1) {
      const id = randomUUID();
      ids.push(id);
      yield { file_id: id, filename: `bulk-${i}.pdf`, object_key: `bulk/${id}` };
    }
  }
  const seen: number[] = [];
  const result = await u.bulkLoad(FILE, rows(), { batchRows: 1000, batchIdPrefix: "load", onBatch: (index) => void seen.push(index) });
  assert.equal(result.batches, 3);
  assert.equal(result.rows, 2500);
  assert.equal(result.affectedRows, 2500);
  assert.deepEqual(seen, [0, 1, 2]);
  assert.deepEqual(result.responses.map((r) => r.batch_id), ["load/000000", "load/000001", "load/000002"]);
  const stored = new Set(fake.rows(FILE, TENANT).map((row: any) => row.file_id));
  assert.equal(stored.size, 2500);
  for (const id of ids) assert.ok(stored.has(id), `row ${id} was loaded`);

  const cdc = () => fake.events().filter((event) => event.topic === "udb.storage.files.cdc");
  const events = cdc();
  assert.equal(events.length, 3, "one event per batch, not per row");
  assert.deepEqual(events.map((event) => (event.envelope as any).payload.row_count), [1000, 1000, 500]);
  for (const [index, event] of events.entries()) {
    const envelope = event.envelope as any;
    assert.equal(envelope.operation, "bulk_upsert");
    assert.equal(envelope.document_id, result.responses[index].batch_id);
    assert.equal(event.partitionKey, result.responses[index].batch_id);
    assert.equal(envelope.event_id, result.responses[index].event_id);
    assert.equal(envelope.payload.keys_sha256, result.responses[index].keys_sha256);
    assert.match(envelope.payload.keys_sha256, /^[0-9a-f]{64}$/);
  }
  assert.equal(result.responses[0].first_key, `s:${ids[0]}`);
  assert.equal(result.responses[2].last_key, `s:${ids[2499]}`);

  const dup = randomUUID();
  const fresh = randomUUID();
  await assert.rejects(
    u.bulkUpsert(FILE, [
      { file_id: fresh, filename: "fresh.pdf", object_key: `bulk/${fresh}` },
      { file_id: dup, filename: "a.pdf", object_key: `bulk/${dup}-a` },
      { file_id: dup, filename: "b.pdf", object_key: `bulk/${dup}-b` },
    ]),
    (err) => inspect(err).reason === "UDB_BULK_DUPLICATE_KEY" && codeOf(err) === ErrorCode.Invalid,
  );
  assert.equal(fake.rows(FILE, TENANT).length, 2500, "a refused batch writes nothing");
  assert.equal(cdc().length, 3, "a refused batch emits no event");

  const other = randomUUID();
  await assert.rejects(
    u.bulkUpsert(FILE, [
      { file_id: randomUUID(), filename: "a.pdf", object_key: `bulk/${randomUUID()}` },
      { file_id: other, filename: "b.pdf" },
    ]),
    (err) => inspect(err).reason === "UDB_BULK_ROW_SHAPE_MISMATCH" && /row 1/.test((err as Error).message),
  );
  assert.equal(fake.rows(FILE, TENANT).length, 2500);

  const keyed = randomUUID();
  const batch = [{ file_id: keyed, filename: "keyed.pdf", object_key: `bulk/${keyed}` }];
  const first = await u.bulkUpsert(FILE, batch, { idempotencyKey: "bulk-key-1", batchId: "keyed" });
  const replay = await u.bulkUpsert(FILE, batch, { idempotencyKey: "bulk-key-1", batchId: "keyed" });
  assert.equal(replay.was_duplicate, true);
  assert.equal(replay.event_id, first.event_id);
  assert.equal(cdc().length, 4, "a replayed batch emits nothing");
  await assert.rejects(
    u.bulkUpsert(FILE, [{ ...batch[0], filename: "changed.pdf" }], { idempotencyKey: "bulk-key-1", batchId: "keyed" }),
    (err) => inspect(err).reason === "UDB_IDEMPOTENCY_REUSE",
  );
});
