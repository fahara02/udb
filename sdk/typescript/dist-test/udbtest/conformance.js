"use strict";
// The behaviour contract a typed store must meet, written once and run against the
// in-memory fake, an embedded broker and a live broker. A fake that drifts from the
// broker fails the same test the broker passes, so a service's unit tests stay honest.
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.runTable = runTable;
exports.runTableCases = runTableCases;
exports.tableCasesPath = tableCasesPath;
const strict_1 = __importDefault(require("node:assert/strict"));
const node_crypto_1 = require("node:crypto");
const node_fs_1 = __importDefault(require("node:fs"));
const node_path_1 = __importDefault(require("node:path"));
const recordCodec_1 = require("../recordCodec");
const stores_1 = require("../stores");
const DATABROKER = "udb.services.v1.DataBroker";
const FILE = "udb.core.storage.entity.v1.File";
function file(id, name) {
    return { file_id: id, filename: name, object_key: `conformance/${id}`, status: "FILE_STATUS_PENDING" };
}
function files(u) {
    return u.tableOf(FILE);
}
function rawRecord(row) {
    return Buffer.from((0, recordCodec_1.recordToJson)((0, recordCodec_1.encodeRecord)(FILE, row, recordCodec_1.WriteMode.Insert)), "utf8");
}
async function rejectsWith(promise, check, message) {
    try {
        await promise;
    }
    catch (err) {
        strict_1.default.ok(check(err), `${message}: ${err}`);
        return;
    }
    strict_1.default.fail(`${message}: the call succeeded`);
}
const isMissing = (err) => err instanceof stores_1.NotFoundError;
const isLost = (err) => err instanceof stores_1.ConflictError;
const code = (want) => (err) => (0, stores_1.codeOf)(err) === want;
const reason = (want) => (err) => (0, stores_1.inspect)(err).reason === want;
function matchesKeyedReplay(first, replay) {
    if (!first || !replay || first.was_duplicate || !replay.was_duplicate)
        return false;
    return JSON.stringify({ ...first, was_duplicate: true }) === JSON.stringify(replay);
}
/** Run the typed-store contract. */
async function runTable(t, connect, opts = {}) {
    if (opts.tenants && (!opts.tenants[0] || !opts.tenants[1] || opts.tenants[0] === opts.tenants[1])) {
        strict_1.default.fail("conformance requires two distinct provisioned tenants");
    }
    const newTenant = () => opts.tenants?.[0] ?? (0, node_crypto_1.randomUUID)();
    await runRevisions(t, connect, newTenant);
    await runConstraints(t, connect, newTenant);
    await runQueries(t, connect, newTenant);
    await t.test("get missing is not found", async () => {
        const table = files(await connect(newTenant()));
        await rejectsWith(table.get({ file_id: (0, node_crypto_1.randomUUID)() }), isMissing, "get of a missing row");
    });
    await t.test("upsert then get round-trips the row", async () => {
        const table = files(await connect(newTenant()));
        const id = (0, node_crypto_1.randomUUID)();
        await table.upsert(file(id, "a.pdf"));
        const got = await table.get({ file_id: id });
        strict_1.default.equal(got.filename, "a.pdf");
        strict_1.default.equal(got.status, "PENDING");
        strict_1.default.ok(got.tenant_id, "the broker fills the verified tenant in");
    });
    await t.test("insert refuses a duplicate without changing the stored row", async () => {
        const table = files(await connect(newTenant()));
        const id = (0, node_crypto_1.randomUUID)();
        await table.insert(file(id, "original.pdf"));
        await rejectsWith(table.insert(file(id, "replacement.pdf")), (err) => (0, stores_1.isUnique)(err), "duplicate insert");
        strict_1.default.equal((await table.get({ file_id: id })).filename, "original.pdf");
    });
    await t.test("two concurrent inserts have exactly one winner", async () => {
        const table = files(await connect(newTenant()));
        const id = (0, node_crypto_1.randomUUID)();
        const names = ["a.pdf", "b.pdf"];
        const results = await Promise.allSettled(names.map((name) => table.insert(file(id, name))));
        const winners = names.filter((_, index) => results[index].status === "fulfilled");
        strict_1.default.equal(winners.length, 1, "exactly one insert wins");
        const lost = results.find((result) => result.status === "rejected");
        strict_1.default.ok((0, stores_1.isUnique)(lost.reason), `the loser fails with unique: ${lost.reason}`);
        strict_1.default.equal((await table.get({ file_id: id })).filename, winners[0]);
    });
    await t.test("transaction insert duplicate rolls back every earlier write", async () => {
        const u = await connect(newTenant());
        const table = files(u);
        const existing = (0, node_crypto_1.randomUUID)();
        const fresh = (0, node_crypto_1.randomUUID)();
        await table.insert(file(existing, "original.pdf"));
        await rejectsWith(u.tx((scope) => {
            scope.insert(FILE, file(fresh, "new.pdf"));
            scope.insert(FILE, file(existing, "replacement.pdf"));
        }), (err) => (0, stores_1.isUnique)(err), "transaction duplicate");
        await rejectsWith(table.get({ file_id: fresh }), isMissing, "the transaction left an earlier insert behind");
        strict_1.default.equal((await table.get({ file_id: existing })).filename, "original.pdf");
        await u.tx((scope) => scope.insert(FILE, file(fresh, "committed.pdf")));
        strict_1.default.equal((await table.get({ file_id: fresh })).filename, "committed.pdf");
    });
    await t.test("tenants never see each other's rows", async () => {
        const a = newTenant();
        const b = opts.tenants?.[1] ?? (0, node_crypto_1.randomUUID)();
        const first = files(await connect(a));
        const second = files(await connect(b));
        const id = (0, node_crypto_1.randomUUID)();
        await first.upsert(file(id, "secret.pdf"));
        await rejectsWith(second.get({ file_id: id }), isMissing, "another tenant read the row");
        await rejectsWith(second.select({ tenant_id: a }), () => true, "naming another tenant in a filter must be refused");
        await rejectsWith(second.upsert(file(id, "must-not-overwrite.pdf")), () => true, "another tenant upserted a stored global primary key");
        strict_1.default.equal((await first.get({ file_id: id })).filename, "secret.pdf");
    });
    await t.test("patch changes only the given columns and needs the row", async () => {
        const table = files(await connect(newTenant()));
        const id = (0, node_crypto_1.randomUUID)();
        await table.upsert(file(id, "draft.pdf"));
        await table.patch({ file_id: id }, { filename: "final.pdf" });
        const got = await table.get({ file_id: id });
        strict_1.default.equal(got.filename, "final.pdf");
        strict_1.default.equal(got.object_key, `conformance/${id}`);
        await rejectsWith(table.patch({ file_id: (0, node_crypto_1.randomUUID)() }, { filename: "x" }), isMissing, "patching a missing row");
    });
    await t.test("conditional update detects a lost race", async () => {
        const table = files(await connect(newTenant()));
        const id = (0, node_crypto_1.randomUUID)();
        await table.upsert(file(id, "v1.pdf"));
        await table.updateIf({ file_id: id }, { filename: "v1.pdf" }, { filename: "v2.pdf" });
        await rejectsWith(table.updateIf({ file_id: id }, { filename: "v1.pdf" }, { filename: "v3.pdf" }), isLost, "a stale expectation");
        strict_1.default.equal((await table.get({ file_id: id })).filename, "v2.pdf");
    });
    await t.test("update with retry applies the change once", async () => {
        const table = files(await connect(newTenant()));
        const id = (0, node_crypto_1.randomUUID)();
        await table.upsert(file(id, "n.pdf"));
        const updated = await table.updateWithRetry({ file_id: id }, (current) => ({ ...current, filename: `renamed-${current.filename}` }), 3);
        strict_1.default.equal(updated.filename, "renamed-n.pdf");
        strict_1.default.equal((await table.get({ file_id: id })).filename, "renamed-n.pdf");
    });
    await t.test("select pages, says when capped, and counts", async () => {
        const table = files(await connect(newTenant()));
        const ids = [];
        for (let i = 0; i < 5; i += 1) {
            const id = (0, node_crypto_1.randomUUID)();
            ids.push(id);
            await table.upsert(file(id, `p${i}.pdf`));
        }
        // A live login shares its tenant with the other cases and previous runs, so count
        // and pagination cover this case's five rows only.
        const filter = { file_id: { $in: ids } };
        const page = await table.select(filter, { limit: 2, includeTotal: true });
        strict_1.default.equal(page.rows.length, 2);
        strict_1.default.equal(page.hasMore, true);
        strict_1.default.equal(page.total, 5);
        let seen = 0;
        await table.selectAll(filter, () => {
            seen += 1;
        });
        strict_1.default.equal(seen, 5);
        strict_1.default.equal(await table.count({ ...filter, filename: { $in: ["p1.pdf", "p3.pdf"] } }), 2);
    });
    await t.test("a transaction commits every write or none", async () => {
        const u = await connect(newTenant());
        const table = files(u);
        const existing = (0, node_crypto_1.randomUUID)();
        const fresh = (0, node_crypto_1.randomUUID)();
        await table.upsert(file(existing, "v1.pdf"));
        const attempt = (expected) => (scope) => {
            scope.upsert(FILE, file(fresh, "new.pdf"));
            scope.updateRowIf(FILE, file(existing, "v2.pdf"), { filename: expected });
        };
        await rejectsWith(u.tx(attempt("stale.pdf")), code(stores_1.ErrorCode.Conflict), "a transaction with a lost CAS");
        await rejectsWith(table.get({ file_id: fresh }), isMissing, "the failed transaction's other write was kept");
        await u.tx(attempt("v1.pdf"));
        strict_1.default.equal((await table.get({ file_id: existing })).filename, "v2.pdf");
        await table.get({ file_id: fresh });
    });
    await t.test("delete needs the row", async () => {
        const table = files(await connect(newTenant()));
        const id = (0, node_crypto_1.randomUUID)();
        await table.upsert(file(id, "gone.pdf"));
        await table.delete({ file_id: id });
        await rejectsWith(table.delete({ file_id: id }), isMissing, "deleting it again");
    });
}
/** Run the cross-SDK cases in the shared JSON file. */
async function runTableCases(t, connect, casesPath = tableCasesPath()) {
    const contract = JSON.parse(node_fs_1.default.readFileSync(casesPath, "utf8"));
    const substitute = (values, id) => Object.fromEntries(Object.entries(values ?? {}).map(([key, value]) => [key, typeof value === "string" ? value.replaceAll("$id", id) : value]));
    for (const tc of contract.cases) {
        await t.test(tc.name, async () => {
            const table = (await connect((0, node_crypto_1.randomUUID)())).tableOf(contract.message_type);
            const id = (0, node_crypto_1.randomUUID)();
            for (const [index, step] of tc.steps.entries()) {
                let got;
                let error = "";
                try {
                    const key = substitute(step.key, id);
                    switch (step.op) {
                        case "upsert":
                            await table.upsert(substitute(step.row, id));
                            break;
                        case "get":
                            got = await table.get(key);
                            break;
                        case "patch":
                            await table.patch(key, substitute(step.changes, id));
                            break;
                        case "update_if":
                            await table.updateIf(key, substitute(step.expected, id), substitute(step.changes, id));
                            break;
                        case "delete":
                            await table.delete(key);
                            break;
                        default:
                            strict_1.default.fail(`unknown op ${step.op}`);
                    }
                }
                catch (err) {
                    error = (0, stores_1.codeOf)(err);
                }
                strict_1.default.equal(error, step.expect?.error ?? "", `step ${index} (${step.op})`);
                if (step.expect?.row) {
                    strict_1.default.ok(got, `step ${index} returned a row`);
                    for (const [field, want] of Object.entries(substitute(step.expect.row, id))) {
                        strict_1.default.equal(got[field], want, `step ${index}: ${field}`);
                    }
                }
            }
        });
    }
}
/** The shared case file: bundled with the package, or sdk/conformance in the repo. */
function tableCasesPath() {
    for (const relative of ["../conformance", "../../conformance", "../../../conformance"]) {
        const candidate = node_path_1.default.join(__dirname, relative, "table-cases.json");
        if (node_fs_1.default.existsSync(candidate))
            return candidate;
    }
    throw new Error("udbtest: table-cases.json is not bundled with this package");
}
async function runRevisions(t, connect, tenant) {
    await t.test("opaque revisions reject stale writes and survive transaction rollback", async () => {
        const u = await connect(tenant());
        const row = file((0, node_crypto_1.randomUUID)(), "revision-original.pdf");
        const insert = { message_type: FILE, record_json: rawRecord(row) };
        const unary = (method, request) => u.core.unary(DATABROKER, method, request);
        const written = await unary("Insert", insert);
        strict_1.default.ok(written.revision, "Insert omitted its committed revision");
        const initial = written.revision;
        const filter = { file_id: row.file_id };
        const read = async () => {
            const result = await unary("Select", { message_type: FILE, filter, fields: ["filename"], include_revision: true });
            strict_1.default.equal(result.records_json.length, 1);
            strict_1.default.equal(result.record_revisions.length, 1, "Select omitted or misaligned the row revision");
            strict_1.default.equal(JSON.parse(result.records_json[0].toString()).file_id, row.file_id, "a revision read includes the primary key");
            return result.record_revisions[0];
        };
        strict_1.default.equal(await read(), initial, "served revision differs from Insert");
        const update = {
            message_type: FILE,
            filter,
            changes: { filename: "revision-updated.pdf" },
            expected_revision: initial,
            require_affected: 1,
            idempotency_key: (0, node_crypto_1.randomUUID)(),
        };
        const first = await unary("Update", update);
        strict_1.default.ok(first.revision && first.revision !== initial && (await read()) === first.revision, "Update did not publish a new revision atomically");
        const replay = await unary("Update", update);
        strict_1.default.ok(matchesKeyedReplay(first, replay) && (await read()) === first.revision, "a lost-response retry rechecked or bumped its revision");
        for (const changed of [{ ...update, expected_revision: first.revision }, { ...update, require_affected: 2 }]) {
            await rejectsWith(unary("Update", changed), reason("UDB_IDEMPOTENCY_REUSE"), "a changed concurrency guard replayed the old receipt");
            strict_1.default.equal(await read(), first.revision);
        }
        const stale = { ...update, idempotency_key: "" };
        const refusal = async (request, want) => {
            await rejectsWith(unary("Update", request), (err) => (0, stores_1.inspect)(err).reason === want && (err.detail?.field_violations ?? []).length > 0, `revision refusal ${want}`);
            strict_1.default.equal(await read(), first.revision, "a refused Update changed the revision");
        };
        await refusal(stale, "UDB_REVISION_CONFLICT");
        await refusal({ ...stale, filter: { filename: "revision-updated.pdf" }, expected_revision: first.revision }, "UDB_CAS_KEY_NOT_PK");
        await rejectsWith(unary("Update", { ...stale, expected_revision: "invalid-revision" }), code(stores_1.ErrorCode.Invalid), "a malformed revision");
        const deleteRequest = { message_type: FILE, filter, expected_revision: initial, require_affected: 1 };
        await rejectsWith(unary("Delete", deleteRequest), reason("UDB_REVISION_CONFLICT"), "a stale Delete");
        const key = { file_id: row.file_id };
        await u.tx((scope) => scope.patch(FILE, key, { filename: "revision-transaction.pdf" }));
        const transactionRevision = await read();
        strict_1.default.notEqual(transactionRevision, first.revision, "BeginTx Update left the pre-transaction revision valid");
        await rejectsWith(unary("Update", { ...stale, expected_revision: first.revision }), reason("UDB_REVISION_CONFLICT"), "a pre-transaction revision stayed writable");
        await rejectsWith(u.tx((scope) => {
            scope.patch(FILE, key, { filename: "must-roll-back.pdf" });
            scope.patch(FILE, key, { file_id: row.file_id });
        }), code(stores_1.ErrorCode.Invalid), "an invalid transaction");
        strict_1.default.equal(await read(), transactionRevision, "transaction rollback retained a revision change");
        strict_1.default.equal((await files(u).get(key)).filename, "revision-transaction.pdf");
        await unary("Delete", { ...deleteRequest, expected_revision: transactionRevision });
        // File is soft-deleted. Upsert restores the same primary key; an Insert would
        // correctly refuse the tombstone's unique constraint.
        const revived = await unary("Upsert", insert);
        strict_1.default.ok(revived.revision && revived.revision !== transactionRevision, "delete and reinsert revived the previous revision");
        await rejectsWith(unary("Update", { ...stale, expected_revision: transactionRevision }), reason("UDB_REVISION_CONFLICT"), "a pre-delete revision was accepted after reinsert");
        const plain = await unary("Select", { message_type: FILE, filter });
        strict_1.default.equal(plain.record_revisions.length, 0, "default Select unexpectedly included revisions");
    });
}
async function runConstraints(t, connect, tenant) {
    await t.test("update admission preserves identity and rolls back transaction prefixes", async () => {
        const u = await connect(tenant());
        const table = files(u);
        const row = file((0, node_crypto_1.randomUUID)(), "immutable-original.pdf");
        await table.insert(row);
        const key = { file_id: row.file_id };
        const meta = u.core.opts.meta;
        const stored = await table.get(key);
        strict_1.default.equal(stored.project_id ?? "", meta.projectId ?? "", "stored project differs from the verified project");
        const values = { file_id: row.file_id, tenant_id: meta.tenantId, project_id: meta.projectId ?? "" };
        for (const column of Object.keys(values)) {
            // Even resending the current identity is forbidden; the setter never ignores it.
            const changes = { [column]: values[column], filename: "must-not-change.pdf" };
            await rejectsWith(table.patch(key, changes), code(stores_1.ErrorCode.Invalid), `immutable ${column} update`);
            const prefix = file((0, node_crypto_1.randomUUID)(), "must-roll-back.pdf");
            await rejectsWith(u.tx((scope) => {
                scope.insert(FILE, prefix);
                scope.patch(FILE, key, changes);
            }), code(stores_1.ErrorCode.Invalid), `immutable ${column} transaction`);
            await rejectsWith(table.get({ file_id: prefix.file_id }), isMissing, `immutable ${column} refusal committed its transaction prefix`);
        }
        for (const request of [
            { message_type: FILE, filter: key },
            { message_type: FILE, changes: { size_bytes: "1" } },
            { message_type: FILE, filter: key, changes: { size_bytes: "1" }, increments: [{ column: "size_bytes", delta: 2 }] },
        ]) {
            await rejectsWith(u.core.unary(DATABROKER, "Update", request), code(stores_1.ErrorCode.Invalid), "an invalid Update shape");
        }
        const got = await table.get(key);
        strict_1.default.equal(got.filename, row.filename, "a refused update changed its target");
        strict_1.default.equal(Number(got.size_bytes ?? 0), 0, "a refused update changed its target");
    });
    await t.test("unary keys replay before CAS and reject changed input", async () => {
        const u = await connect(tenant());
        const unary = (method, request) => u.core.unary(DATABROKER, method, request);
        const row = file((0, node_crypto_1.randomUUID)(), "keyed-original.pdf");
        const insert = { message_type: FILE, record_json: rawRecord(row), idempotency_key: (0, node_crypto_1.randomUUID)() };
        let first = await unary("Insert", insert);
        let replay = await unary("Insert", insert);
        strict_1.default.ok(matchesKeyedReplay(first, replay), "an identical keyed insert did not replay its original receipt");
        // Insert and Upsert are distinct operations even when their key and target match.
        const upsert = { ...insert, record_json: rawRecord({ ...row, filename: "keyed-upserted.pdf" }) };
        first = await unary("Upsert", upsert);
        strict_1.default.equal(first.was_duplicate, false, "Insert and Upsert collided in the key namespace");
        replay = await unary("Upsert", upsert);
        strict_1.default.ok(matchesKeyedReplay(first, replay), "an identical keyed upsert did not replay its original receipt");
        const filter = { file_id: row.file_id };
        const update = {
            message_type: FILE,
            filter,
            expected: { size_bytes: "0" },
            require_affected: 1,
            idempotency_key: (0, node_crypto_1.randomUUID)(),
            increments: [{ column: "size_bytes", delta: 7 }],
        };
        first = await unary("Update", update);
        replay = await unary("Update", update);
        strict_1.default.ok(matchesKeyedReplay(first, replay), "a keyed update replay checked the already-changed CAS value");
        let reuse;
        await unary("Update", { ...update, increments: [{ column: "size_bytes", delta: 11 }] }).catch((err) => {
            reuse = err;
        });
        const info = (0, stores_1.inspect)(reuse);
        strict_1.default.equal(info.reason, "UDB_IDEMPOTENCY_REUSE");
        strict_1.default.equal(info.code, stores_1.ErrorCode.Precondition);
        strict_1.default.ok(info.fixHint, "changed input must have an actionable key-reuse refusal");
        const table = files(u);
        const got = await table.get(filter);
        strict_1.default.equal(Number(got.size_bytes), 7, "a keyed increment ran more than once or changed input wrote");
        strict_1.default.equal(got.filename, "keyed-upserted.pdf");
        const remove = { message_type: FILE, filter, require_affected: 1, idempotency_key: (0, node_crypto_1.randomUUID)() };
        first = await unary("Delete", remove);
        replay = await unary("Delete", remove);
        strict_1.default.ok(matchesKeyedReplay(first, replay), "a keyed delete replay checked the already-deleted row");
        await rejectsWith(table.get(filter), isMissing, "a keyed delete left a visible row");
    });
    await t.test("explicit primary conflict target replaces the original row", async () => {
        const u = await connect(tenant());
        const row = file((0, node_crypto_1.randomUUID)(), "original.pdf");
        const table = files(u);
        await table.insert(row);
        await u.core.unary(DATABROKER, "Upsert", {
            message_type: FILE,
            record_json: rawRecord({ ...row, filename: "replaced.pdf" }),
            conflict_fields: ["file_id"],
        });
        strict_1.default.equal((await table.get({ file_id: row.file_id })).filename, "replaced.pdf");
    });
    await t.test("unsupported transaction idempotency key refuses the entire batch", async () => {
        const u = await connect(tenant());
        // Insert and outbox enqueue have no receipt, so a key on them refuses the batch.
        for (const operation of ["insert", "enqueue_outbox_event"]) {
            const row = file((0, node_crypto_1.randomUUID)(), "must-not-commit.pdf");
            const record = rawRecord(row);
            const keyed = { operation, message_type: FILE, record_json: record, idempotency_key: (0, node_crypto_1.randomUUID)() };
            if (operation === "enqueue_outbox_event") {
                delete keyed.record_json;
                keyed.collection = "udb.conformance.v1";
                keyed.object_key = row.file_id;
                keyed.payload = {
                    event_id: (0, node_crypto_1.randomUUID)(),
                    event_type: "conformance",
                    document_id: row.file_id,
                    correlation_id: (0, node_crypto_1.randomUUID)(),
                    envelope_version: 2,
                    payload: {},
                };
            }
            const violations = await new Promise((resolve) => {
                const stream = u.core.bidiStream(DATABROKER, "BeginTx", { deadlineMs: 10_000 });
                const collected = [];
                stream.on("data", (frame) => collected.push(...(frame?.error_detail?.field_violations ?? [])));
                stream.on("error", (err) => {
                    collected.push(...(err?.udb?.detail?.field_violations ?? []));
                    resolve(collected);
                });
                stream.on("end", () => resolve(collected));
                stream.write({ operation: "upsert", message_type: FILE, record_json: record });
                stream.write(keyed);
                stream.write({ commit: true });
                stream.end();
            });
            strict_1.default.ok(violations.some((violation) => violation.field === "mutations[1].idempotency_key"), `${operation}: an ignored key was not explicitly refused`);
            await rejectsWith(files(u).get({ file_id: row.file_id }), isMissing, `${operation}: the refused transaction kept its prefix`);
        }
    });
    await t.test("secondary uniqueness refuses insert and rolls back a multirow update", async () => {
        const u = await connect(tenant());
        const table = files(u);
        const first = file((0, node_crypto_1.randomUUID)(), "first.pdf");
        const second = file((0, node_crypto_1.randomUUID)(), "second.pdf");
        await table.insert(first);
        await table.insert(second);
        const duplicate = { ...file((0, node_crypto_1.randomUUID)(), "duplicate.pdf"), object_key: first.object_key };
        await rejectsWith(table.insert(duplicate), (err) => (0, stores_1.isUnique)(err) && Boolean((0, stores_1.inspect)(err).constraint), "a secondary duplicate");
        await rejectsWith(u.core.unary(DATABROKER, "Update", {
            message_type: FILE,
            filter: { file_id: { $in: [first.file_id, second.file_id] } },
            changes: { object_key: `conformance/shared/${(0, node_crypto_1.randomUUID)()}`, filename: "must-rollback.pdf" },
        }), (err) => (0, stores_1.isUnique)(err), "a multirow collision");
        for (const original of [first, second]) {
            const got = await table.get({ file_id: original.file_id });
            strict_1.default.equal(got.filename, original.filename, "a failed statement changed a row");
            strict_1.default.equal(got.object_key, original.object_key, "a failed statement changed a row");
        }
        const fresh = file((0, node_crypto_1.randomUUID)(), "tx-prefix.pdf");
        await rejectsWith(u.tx((scope) => {
            scope.insert(FILE, fresh);
            scope.insert(FILE, duplicate);
        }), (err) => (0, stores_1.isUnique)(err), "a transaction secondary collision");
        await rejectsWith(table.get({ file_id: fresh.file_id }), isMissing, "a unique refusal kept the transaction prefix");
    });
    await t.test("NOT NULL refusal names the column", async () => {
        const u = await connect(tenant());
        let refused;
        await u.core
            .unary(DATABROKER, "Upsert", {
            message_type: FILE,
            record_json: Buffer.from(JSON.stringify({ file_id: (0, node_crypto_1.randomUUID)(), filename: null, object_key: `conformance/${(0, node_crypto_1.randomUUID)()}` })),
        })
            .catch((err) => {
            refused = err;
        });
        const info = (0, stores_1.inspect)(refused);
        strict_1.default.equal(info.code, stores_1.ErrorCode.NotNull, `NULL refusal: ${refused}`);
        strict_1.default.equal(info.column, "filename");
    });
    await t.test("int64 stays exact through storage and an atomic increment", async () => {
        const table = files(await connect(tenant()));
        const row = { ...file((0, node_crypto_1.randomUUID)(), "large.pdf"), size_bytes: "9007199254740993" };
        await table.insert(row);
        const key = { file_id: row.file_id };
        strict_1.default.equal(String((await table.get(key)).size_bytes), "9007199254740993", "a stored int64 was rounded");
        await table.increment(key, { size_bytes: 1 });
        strict_1.default.equal(String((await table.get(key)).size_bytes), "9007199254740994", "an increment was rounded");
    });
    await t.test("soft delete retains unique keys and upsert revives the row", async () => {
        const table = files(await connect(tenant()));
        const row = file((0, node_crypto_1.randomUUID)(), "deleted.pdf");
        await table.insert(row);
        const key = { file_id: row.file_id };
        await table.delete(key);
        await rejectsWith(table.get(key), isMissing, "a tombstone remained visible");
        await rejectsWith(table.insert(row), (err) => (0, stores_1.isUnique)(err), "a strict insert reused a tombstone key");
        await table.upsert({ ...row, filename: "revived.pdf" });
        const got = await table.get(key);
        strict_1.default.equal(got.filename, "revived.pdf");
        strict_1.default.equal(got.deleted_at ?? null, null, "upsert did not revive the tombstone");
    });
    await t.test("default page caps at 100 and selectAll visits every row", async () => {
        const u = await connect(tenant());
        const table = files(u);
        const ids = [];
        await u.tx((scope) => {
            for (let i = 0; i < 103; i += 1) {
                const row = file((0, node_crypto_1.randomUUID)(), `page-${String(i).padStart(3, "0")}.pdf`);
                ids.push(row.file_id);
                scope.insert(FILE, row);
            }
        });
        const filter = { file_id: { $in: ids } };
        const page = await table.select(filter, { includeTotal: true });
        strict_1.default.equal(page.rows.length, 100);
        strict_1.default.equal(page.hasMore, true);
        strict_1.default.equal(page.total, 103);
        const seen = new Set();
        await table.selectAll(filter, (row) => {
            strict_1.default.ok(!seen.has(row.file_id), "selectAll repeated a row");
            seen.add(row.file_id);
        });
        strict_1.default.equal(seen.size, 103);
    });
}
// Negating a predicate must not turn a SQL NULL into a matching row.
async function runQueries(t, connect, tenant) {
    await t.test("unknown columns are refused even without matching rows", async () => {
        const u = await connect(tenant());
        await rejectsWith(u.core.unary(DATABROKER, "Select", {
            message_type: FILE,
            filter: { file_id: (0, node_crypto_1.randomUUID)(), $or: [{ not_a_declared_column: "value" }] },
        }), (err) => err?.code === 3, "an unknown column");
    });
    await t.test("nullable predicates preserve SQL three-valued logic", async (sub) => {
        const u = await connect(tenant());
        const table = files(u);
        const ids = [(0, node_crypto_1.randomUUID)(), (0, node_crypto_1.randomUUID)(), (0, node_crypto_1.randomUUID)()];
        for (const [index, contentType] of [null, "blocked/type", "allowed/type"].entries()) {
            await u.core.unary(DATABROKER, "Upsert", {
                message_type: FILE,
                payload: { file_id: ids[index], filename: "nullable.pdf", object_key: `conformance/${ids[index]}`, content_type: contentType },
            });
        }
        const cases = [
            ["not equal", { $ne: "blocked/type" }, ids.slice(2)],
            ["not in", { $nin: ["blocked/type"] }, ids.slice(2)],
            ["not equality", { $not: { $eq: "blocked/type" } }, ids.slice(2)],
            ["nested not", { $not: { $not: { $eq: "allowed/type" } } }, ids.slice(2)],
            ["is not null", { $is_null: false }, ids.slice(1)],
            ["not is not null", { $not: { $not_null: true } }, ids.slice(0, 1)],
        ];
        for (const [name, predicate, want] of cases) {
            await sub.test(name, async () => {
                const page = await table.select({ file_id: { $in: ids }, content_type: predicate });
                strict_1.default.deepEqual(page.rows.map((row) => row.file_id).sort(), [...want].sort());
            });
        }
    });
}
