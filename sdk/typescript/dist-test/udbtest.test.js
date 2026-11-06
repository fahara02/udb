"use strict";
// The in-memory broker: it meets the same typed-store contract a real broker does,
// over a real loopback listener with an unmodified UdbProject.
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
const node_test_1 = require("node:test");
const strict_1 = __importDefault(require("node:assert/strict"));
const node_fs_1 = __importDefault(require("node:fs"));
const node_os_1 = __importDefault(require("node:os"));
const node_path_1 = __importDefault(require("node:path"));
const grpc = __importStar(require("@grpc/grpc-js"));
const project_1 = require("./project");
const protoSchema_1 = require("./protoSchema");
const stores_1 = require("./stores");
const udbtest_1 = require("./udbtest");
const FILE = "udb.core.storage.entity.v1.File";
const EVENT = "udb.core.apikey.events.v1.ApiKeyCreatedEvent";
const TOPIC = "udb.apikey.created.v1";
const TENANT = "0190f1b2-0000-4000-8000-0000000000aa";
const DATABROKER = "udb.services.v1.DataBroker";
(0, node_test_1.test)("the fake meets the table contract", async (t) => {
    const fake = await (0, udbtest_1.createFake)(t, [FILE]);
    await (0, udbtest_1.runTable)(t, (tenant) => fake.client(tenant));
});
(0, node_test_1.test)("the fake meets the shared cross-SDK cases", async (t) => {
    const fake = await (0, udbtest_1.createFake)(t, [FILE]);
    await (0, udbtest_1.runTableCases)(t, (tenant) => fake.client(tenant));
});
(0, node_test_1.test)("emit builds the envelope from the event's contract and commits with the writes", async (t) => {
    const fake = await (0, udbtest_1.createFake)(t, [FILE]);
    const u = await fake.client(TENANT);
    await u.tx((scope) => {
        scope.upsert(FILE, { file_id: "0190f1b2-0000-4000-8000-000000000001", filename: "a.pdf", object_key: "k" });
        scope.emit(EVENT, { key_id: "key-1", tenant_id: TENANT, name: "ci" });
    });
    const events = fake.events();
    strict_1.default.equal(events.length, 1);
    strict_1.default.equal(events[0].topic, TOPIC);
    strict_1.default.equal(events[0].partitionKey, TENANT);
    strict_1.default.equal(events[0].envelope.event_type, EVENT);
    strict_1.default.equal(events[0].envelope.envelope_version, 2);
    strict_1.default.equal(events[0].envelope.payload.key_id, "key-1", "the event's fields ride under payload");
    strict_1.default.equal(fake.rows(FILE, TENANT).length, 1);
    await strict_1.default.rejects(u.tx((scope) => scope.emit(EVENT, { key_id: "key-2" })), /partition key/);
    await strict_1.default.rejects(u.tx((scope) => {
        scope.emit(EVENT, { key_id: "key-3", tenant_id: TENANT });
        scope.insert(FILE, { file_id: "0190f1b2-0000-4000-8000-000000000001", filename: "dup.pdf", object_key: "k2" });
    }), (err) => (0, stores_1.isUnique)(err));
    strict_1.default.equal(fake.events().length, 1, "a refused transaction records no event");
});
(0, node_test_1.test)("inspect reads the reason the broker sends in the trailer", async (t) => {
    const fake = await (0, udbtest_1.createFake)(t, [FILE]);
    const u = await fake.client(TENANT);
    const files = u.tableOf(FILE);
    let refused;
    await files.delete({ file_id: "0190f1b2-0000-4000-8000-000000000009" }).catch((err) => {
        refused = err;
    });
    const info = (0, stores_1.inspect)(refused);
    strict_1.default.equal(info.code, stores_1.ErrorCode.NotFound);
    strict_1.default.equal(info.reason, "UDB_NO_ROWS_AFFECTED");
    strict_1.default.equal((0, stores_1.isNotFound)(refused), true);
    await strict_1.default.rejects(files.select({ filename: { $regex: "x" } }), (err) => {
        const got = (0, stores_1.inspect)(err);
        return got.code === stores_1.ErrorCode.Invalid && got.reason === "UDB_UNSUPPORTED_FILTER_OPERATOR";
    });
});
(0, node_test_1.test)("a named consumer resumes after the last event it acknowledged", async (t) => {
    const fake = await (0, udbtest_1.createFake)(t, [FILE]);
    const u = await fake.client(TENANT);
    for (const key of ["k1", "k2", "k3"])
        fake.emit(TENANT, TOPIC, TENANT, { key_id: key, tenant_id: TENANT });
    const handled = [];
    const run = async (stopAfter) => {
        const controller = new AbortController();
        let seen = 0;
        await u.consume("billing-projector", TOPIC, async (event) => {
            handled.push(event.payload.key_id);
            seen += 1;
            // Completed handlers must flush before the cancelled consumer returns.
            if (seen === stopAfter)
                controller.abort();
        }, { signal: controller.signal, messageType: EVENT });
    };
    await run(2);
    strict_1.default.deepEqual(handled.slice(0, 2), ["k1", "k2"]);
    fake.emit(TENANT, TOPIC, TENANT, { key_id: "k4", tenant_id: TENANT });
    await run(2);
    strict_1.default.deepEqual([...handled].sort(), ["k1", "k2", "k3", "k4"], "no event is skipped or handed over twice across a restart");
});
(0, node_test_1.test)("a consumer only sees its own tenant, and another principal cannot take its name", async (t) => {
    const fake = await (0, udbtest_1.createFake)(t, [FILE]);
    const other = "0190f1b2-0000-4000-8000-0000000000ee";
    const u = await fake.client(TENANT);
    fake.emit(other, TOPIC, other, { key_id: "theirs" });
    fake.emit(TENANT, TOPIC, TENANT, { key_id: "mine" });
    const controller = new AbortController();
    const seen = [];
    await u.consume("audit", TOPIC, async (event) => {
        seen.push(event.payload.key_id);
        controller.abort();
    }, { signal: controller.signal, messageType: EVENT });
    strict_1.default.deepEqual(seen, ["mine"]);
    fake.addIdentityApiKey("other-key", { tenantId: TENANT, serviceIdentity: "another-service", scopes: ["udb:read"] });
    const thief = await fake.clientForKey("other-key");
    await strict_1.default.rejects(thief.consume("audit", TOPIC, async () => { }, { backoffMs: 1 }), (err) => (0, stores_1.inspect)(err).reason === "UDB_POLICY_DENIED");
});
(0, node_test_1.test)("a poison event reaches the DLQ with one atomic cursor advance, and replays", async (t) => {
    const fake = await (0, udbtest_1.createFake)(t, [FILE]);
    const u = await fake.client(TENANT);
    fake.emit(TENANT, TOPIC, TENANT, { key_id: "poison" });
    fake.emit(TENANT, TOPIC, TENANT, { key_id: "good" });
    const controller = new AbortController();
    const seen = [];
    await u.consume("dlq-test", TOPIC, async (event) => {
        seen.push(event.payload.key_id);
        if (event.payload.key_id === "poison")
            throw new Error("cannot handle");
        controller.abort();
    }, { signal: controller.signal, maxAttempts: 2, backoffMs: 1, messageType: EVENT });
    strict_1.default.deepEqual(seen, ["poison", "poison", "good"]);
    const listed = await u.core.unary(DATABROKER, "ListDlqEvents", {});
    strict_1.default.equal(listed.total_count, 1);
    const record = listed.events[0];
    strict_1.default.equal(record.status, "QUARANTINED");
    strict_1.default.equal(record.error_message, "cannot handle");
    await u.core.unary(DATABROKER, "ReplayDlqEvent", { dlq_id: record.dlq_id });
    strict_1.default.equal((await u.core.unary(DATABROKER, "GetDlqEvent", { dlq_id: record.dlq_id })).event.status, "REPLAYED");
    strict_1.default.equal(fake.events().length, 3, "a replay appends a new event");
    await strict_1.default.rejects(u.core.unary(DATABROKER, "ReplayDlqEvent", { dlq_id: record.dlq_id }), (err) => (0, stores_1.isNotFound)(err));
    fake.addIdentityApiKey("no-admin", { tenantId: TENANT, serviceIdentity: "reader", scopes: ["udb:read"] });
    const reader = await fake.clientForKey("no-admin");
    await strict_1.default.rejects(reader.core.unary(DATABROKER, "ListDlqEvents", {}), (err) => (0, stores_1.missingScope)(err) === "udb:admin");
});
(0, node_test_1.test)("injected faults are bounded, typed and applied before any write", async (t) => {
    const fake = await (0, udbtest_1.createFake)(t, [FILE]);
    const u = await fake.client(TENANT);
    const table = new stores_1.Table(u.core, FILE);
    strict_1.default.throws(() => fake.injectFault({ code: grpc.status.OK }), /failure code/);
    strict_1.default.throws(() => fake.injectFault({ code: grpc.status.UNAVAILABLE, times: -1 }), /non-negative/);
    const id = "0190f1b2-0000-4000-8000-000000000002";
    const row = { file_id: id, filename: "a.pdf", object_key: "k" };
    fake.injectFault({ method: "/udb.services.v1.DataBroker/Insert", code: grpc.status.UNAVAILABLE, times: 2, retryAfterMs: 50 });
    for (let attempt = 0; attempt < 2; attempt += 1) {
        await strict_1.default.rejects(table.insert(row), (err) => {
            const info = (0, stores_1.inspect)(err);
            return info.code === stores_1.ErrorCode.Unavailable && info.reason === "UDB_BACKEND_UNAVAILABLE" && info.retryAfterMs === 50;
        });
    }
    strict_1.default.equal(fake.rows(FILE, TENANT).length, 0, "a fault happens before the mutation");
    await table.insert(row);
    strict_1.default.equal(fake.rows(FILE, TENANT).length, 1, "the fault was used up");
    fake.injectFault({ code: grpc.status.INTERNAL, tenantId: "someone-else" });
    await table.get({ file_id: id });
    fake.injectFault({ method: "Authenticate", code: grpc.status.UNAVAILABLE });
    await strict_1.default.rejects(fake.client("0190f1b2-0000-4000-8000-0000000000bb"), (err) => (0, stores_1.codeOf)(err) === stores_1.ErrorCode.Unavailable);
});
(0, node_test_1.test)("the rate limit refuses with the broker's reason and a retry delay", async (t) => {
    const fake = await (0, udbtest_1.createFake)(t, [FILE]);
    const u = await fake.client(TENANT);
    strict_1.default.throws(() => fake.setRateLimit(1, 0), /invalid rate limit/);
    fake.setRateLimit(1, 60_000);
    const table = u.tableOf(FILE);
    await table.select({});
    let limited;
    await table.select({}).catch((err) => {
        limited = err;
    });
    const info = (0, stores_1.inspect)(limited);
    strict_1.default.equal(info.code, stores_1.ErrorCode.RateLimited);
    strict_1.default.ok(info.retryAfterMs > 0);
    fake.setRateLimit(0, 0);
    await table.select({});
});
(0, node_test_1.test)("an expired bearer and a missing purpose are refused", async (t) => {
    const fake = await (0, udbtest_1.createFake)(t, [FILE]);
    const u = await fake.client(TENANT);
    strict_1.default.throws(() => fake.setTokenLifetime(10), /at least one second/);
    const bare = new project_1.UdbProject({ target: fake.target, tenantId: TENANT, purpose: "test", projectId: "" });
    t.after(() => bare.close());
    await strict_1.default.rejects(bare.tableOf(FILE).select({}), (err) => (0, stores_1.codeOf)(err) === stores_1.ErrorCode.Unauthenticated);
    strict_1.default.equal((await u.tableOf(FILE).select({})).rows.length, 0);
    const wrong = new project_1.UdbProject({ target: fake.target, tenantId: "another-tenant", purpose: "test", projectId: "", credentials: { bearerToken: u.core.opts.bearerToken } });
    t.after(() => wrong.close());
    await strict_1.default.rejects(wrong.tableOf(FILE).select({}), (err) => (0, stores_1.inspect)(err).reason === "UDB_TENANT_MISMATCH");
});
(0, node_test_1.test)("a descriptor fake denies by default, and a policy opens exactly what it names", async (t) => {
    const denied = await (0, udbtest_1.createDescriptorFake)();
    t.after(() => denied.close());
    const client = await denied.client(TENANT);
    await strict_1.default.rejects(client.tableOf(FILE).select({}), (err) => {
        const info = (0, stores_1.inspect)(err);
        return info.code === stores_1.ErrorCode.PolicyDenied && info.missing?.rule === `Select ${FILE}`;
    });
    const dir = node_fs_1.default.mkdtempSync(node_path_1.default.join(node_os_1.default.tmpdir(), "udbtest-policy-"));
    const file = node_path_1.default.join(dir, "policies.json");
    node_fs_1.default.writeFileSync(file, JSON.stringify({ policies: [{ id: "read", action: "Select", resource: FILE, required_scopes: ["udb:read"] }, { id: "no", effect: "deny", action: "Delete", resource: FILE }] }));
    const allowed = await (0, udbtest_1.createDescriptorFake)({ policyFile: file });
    t.after(() => allowed.close());
    const reader = await allowed.client(TENANT);
    strict_1.default.equal((await reader.tableOf(FILE).select({})).rows.length, 0);
    await strict_1.default.rejects(reader.tableOf(FILE).delete({ file_id: "0190f1b2-0000-4000-8000-000000000003" }), (err) => (0, stores_1.inspect)(err).reason === "UDB_POLICY_DENIED");
    await strict_1.default.rejects((0, udbtest_1.createDescriptorFake)({ policyFile: node_path_1.default.join(dir, "missing.yaml") }), /policy file/);
});
(0, node_test_1.test)("an unknown entity is refused, and rows and events are copies", async (t) => {
    const fake = await (0, udbtest_1.createFake)(t, [FILE]);
    const u = await fake.client(TENANT);
    await strict_1.default.rejects(u.tableOf("udb.core.workflow.entity.v1.WorkflowInstance").select({}), (err) => (0, stores_1.inspect)(err).reason === "UDB_UNKNOWN_MESSAGE_TYPE");
    fake.emit(TENANT, TOPIC, TENANT, { key_id: "k" });
    fake.events()[0].envelope.payload.key_id = "changed";
    strict_1.default.equal(fake.events()[0].envelope.payload.key_id, "k");
    const id = "0190f1b2-0000-4000-8000-000000000004";
    await u.tableOf(FILE).insert({ file_id: id, filename: "a.pdf", object_key: "k" });
    fake.rows(FILE, TENANT)[0].filename = "changed";
    strict_1.default.equal(fake.rows(FILE, TENANT)[0].filename, "a.pdf");
    strict_1.default.equal(fake.rows(FILE, "someone-else").length, 0);
});
(0, node_test_1.test)("a YAML policy file opens what it names, like the same file in JSON", async (t) => {
    const dir = node_fs_1.default.mkdtempSync(node_path_1.default.join(node_os_1.default.tmpdir(), "udbtest-yaml-"));
    const file = node_path_1.default.join(dir, "policies.yaml");
    node_fs_1.default.writeFileSync(file, `tenant: ${TENANT}
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
    const fake = await (0, udbtest_1.createDescriptorFake)({ policyFile: file });
    t.after(() => fake.close());
    const client = await fake.client(TENANT);
    strict_1.default.equal((await client.tableOf(FILE).select({})).rows.length, 0);
    await strict_1.default.rejects(client.tableOf(FILE).delete({ file_id: TENANT }), (err) => (0, stores_1.inspect)(err).reason === "UDB_POLICY_DENIED", "a disabled policy and another tenant's open nothing");
    const parsed = (0, udbtest_1.parsePolicies)(node_fs_1.default.readFileSync(file, "utf8"));
    strict_1.default.equal(parsed[0].tenant, TENANT, "a policy inherits the document's tenant");
    strict_1.default.equal(parsed[1].enabled, false);
    strict_1.default.equal(parsed[2].tenant, "someone-else", "its own tenant wins");
    strict_1.default.deepEqual((0, udbtest_1.parsePolicies)(JSON.stringify(parsed)), parsed, "the JSON form reads the same");
    strict_1.default.deepEqual((0, udbtest_1.parsePolicies)("- action: Select\n"), [{ action: "Select" }], "a bare list is a document");
});
(0, node_test_1.test)("a malformed policy file is refused before the fake serves", async () => {
    const refused = [
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
    for (const [source, want] of refused)
        strict_1.default.throws(() => (0, udbtest_1.parsePolicies)(source), want, JSON.stringify(source));
    const dir = node_fs_1.default.mkdtempSync(node_path_1.default.join(node_os_1.default.tmpdir(), "udbtest-bad-"));
    const file = node_path_1.default.join(dir, "policies.yml");
    node_fs_1.default.writeFileSync(file, "- effect: maybe\n");
    await strict_1.default.rejects((0, udbtest_1.createDescriptorFake)({ policyFile: file }), /invalid effect/);
});
/** A compiled descriptor set, on the wire, with the udb annotations as extensions. */
function descriptorSet(file) {
    const type = (0, protoSchema_1.protoSchemas)().protoType("google.protobuf.FileDescriptorSet");
    return type.encode(type.fromObject({ file: [file] })).finish();
}
const PG_COLUMN = ".udb.core.common.v1.pg_column";
const PG_TABLE = ".udb.core.common.v1.pg_table";
function noteFile() {
    const column = (name, number, type, options, extra = {}) => ({
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
(0, node_test_1.test)("a descriptor-set fake serves its tables with their annotations, under a policy", async (t) => {
    const bytes = descriptorSet(noteFile());
    const schemas = protoSchema_1.ProtoSchemas.fromDescriptorSet(bytes);
    const NOTE = "acme.notes.v1.Note";
    const note = schemas.message(NOTE);
    strict_1.default.deepEqual(note.primaryKeys, ["note_id"]);
    strict_1.default.equal(note.tenantColumn, "tenant_id");
    strict_1.default.equal(note.field("title")?.hasPresence, true);
    strict_1.default.equal(note.field("title")?.oneof, undefined, "proto3 optional is not a real oneof");
    strict_1.default.deepEqual(schemas.tables().map((table) => table.fullName), [NOTE], "only annotated tables register");
    const fake = await (0, udbtest_1.createDescriptorFake)({
        descriptorSet: bytes,
        policies: [{ id: "all", action: "*", resource: "*" }],
    });
    t.after(() => fake.close());
    const client = await fake.client(TENANT);
    const notes = client.tableOf(NOTE, { schemas });
    const id = "0190f1b2-0000-4000-8000-000000000010";
    await notes.insert({ note_id: id, kind: "NOTE_KIND_MEMO", slug: "a" });
    const stored = await notes.get({ note_id: id });
    strict_1.default.equal(stored.kind, "MEMO", "the enum column follows the descriptor");
    strict_1.default.equal(stored.title, null, "an unset optional is NULL");
    strict_1.default.equal(stored.tenant_id, TENANT, "the tenant column follows the descriptor");
    await notes.increment({ note_id: id }, { hits: 2 });
    strict_1.default.equal((await notes.get({ note_id: id })).hits, 2);
    await strict_1.default.rejects(notes.insert({ note_id: "0190f1b2-0000-4000-8000-000000000011", slug: "a" }), (err) => (0, stores_1.isUnique)(err) && (0, stores_1.inspect)(err).constraint === "notes_slug_key", "a declared unique key is enforced");
    await strict_1.default.rejects(client.tableOf(FILE).select({}), (err) => (0, stores_1.inspect)(err).reason === "UDB_UNKNOWN_MESSAGE_TYPE", "a table outside the set is not served");
    // Negative: a denying fake over the same set.
    const closed = await (0, udbtest_1.createDescriptorFake)({ descriptorSet: bytes });
    t.after(() => closed.close());
    await strict_1.default.rejects((await closed.client(TENANT)).tableOf(NOTE, { schemas }).select({}), (err) => (0, stores_1.inspect)(err).reason === "UDB_POLICY_DENIED");
});
(0, node_test_1.test)("a bad descriptor set is refused", async () => {
    await strict_1.default.rejects((0, udbtest_1.createDescriptorFake)({ descriptorSet: Buffer.from([0xff, 0xff, 0xff]) }), /descriptor set:/, "not a descriptor set");
    const missingImport = noteFile();
    missingImport.messageType[0].field.push({ name: "at", number: 7, type: "TYPE_MESSAGE", label: "LABEL_OPTIONAL", typeName: ".google.protobuf.Timestamp" });
    await strict_1.default.rejects((0, udbtest_1.createDescriptorFake)({ descriptorSet: descriptorSet(missingImport) }), /descriptor set:/, "an import missing from the set");
    const noTables = noteFile();
    noTables.messageType = noTables.messageType.slice(1);
    await strict_1.default.rejects((0, udbtest_1.createDescriptorFake)({ descriptorSet: descriptorSet(noTables) }), /declares no tables/);
    await strict_1.default.rejects((0, udbtest_1.createDescriptorFake)({ descriptorSet: descriptorSet(noteFile()), protoRoot: "x" }), /not both/);
});
// Runs in an embedded CI lane with that lane's compiled broker: the same contract used
// by the fake and an external live broker.
(0, node_test_1.test)("an embedded broker meets the table contract", {
    skip: process.env.UDB_EMBEDDED_SDK_TESTS === "1" ? false : "requires an embedded broker (UDB_EMBEDDED_SDK_TESTS=1)",
}, async (t) => {
    const dir = node_fs_1.default.mkdtempSync(node_path_1.default.join(node_os_1.default.tmpdir(), "udbtest-embedded-"));
    const policy = node_path_1.default.join(dir, "policies.yaml");
    node_fs_1.default.writeFileSync(policy, `policies:
  - id: embedded-table-contract
    action: '*'
    resource: ${FILE}
    purpose: udbtest.embedded
    required_scopes: [udb:write, udb:read]
`);
    const repoProto = node_path_1.default.resolve(__dirname, "..", "..", "..", "proto");
    const [u, peer] = await (0, udbtest_1.startPair)(t, repoProto, policy);
    const tenantOf = (client) => client.core.opts.meta.tenantId;
    await (0, udbtest_1.runTable)(t, (tenant) => (tenant === tenantOf(peer) ? peer : u), { tenants: [tenantOf(u), tenantOf(peer)] });
    await (0, udbtest_1.runTableCases)(t, () => u);
    strict_1.default.ok(udbtest_1.start, "start is exported alongside startPair");
});
