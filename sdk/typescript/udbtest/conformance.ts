// The behaviour contract a typed store must meet, written once and run against the
// in-memory fake, an embedded broker and a live broker. A fake that drifts from the
// broker fails the same test the broker passes, so a service's unit tests stay honest.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import type { TestContext } from "node:test";

import type { UdbProject } from "../project";
import { WriteMode, encodeRecord, recordToJson } from "../recordCodec";
import { Table, codeOf, ErrorCode, inspect, isUnique, NotFoundError, ConflictError, TxScope } from "../stores";

const DATABROKER = "udb.services.v1.DataBroker";
const FILE = "udb.core.storage.entity.v1.File";

/** A client for `tenant`. A fresh tenant UUID per call keeps runs isolated. */
export type Connect = (tenant: string) => UdbProject | Promise<UdbProject>;

/** Options of {@link runTable}. */
export interface RunTableOptions {
  /** Two provisioned canonical tenant UUIDs. Without them the fake provisions fresh
   *  tenants. The cross-tenant cases never skip. */
  tenants?: [string, string];
}

type FileRow = Record<string, any>;

function file(id: string, name: string): FileRow & { file_id: string; filename: string; object_key: string } {
  return { file_id: id, filename: name, object_key: `conformance/${id}`, status: "FILE_STATUS_PENDING" };
}

function files(u: UdbProject): Table<FileRow> {
  return u.tableOf<FileRow>(FILE);
}

function rawRecord(row: FileRow): Buffer {
  return Buffer.from(recordToJson(encodeRecord(FILE, row, WriteMode.Insert)), "utf8");
}

async function rejectsWith(promise: Promise<unknown>, check: (err: unknown) => boolean, message: string): Promise<void> {
  try {
    await promise;
  } catch (err) {
    assert.ok(check(err), `${message}: ${err}`);
    return;
  }
  assert.fail(`${message}: the call succeeded`);
}

const isMissing = (err: unknown) => err instanceof NotFoundError;
const isLost = (err: unknown) => err instanceof ConflictError;
const code = (want: ErrorCode) => (err: unknown) => codeOf(err) === want;
const reason = (want: string) => (err: unknown) => inspect(err).reason === want;

function matchesKeyedReplay(first: any, replay: any): boolean {
  if (!first || !replay || first.was_duplicate || !replay.was_duplicate) return false;
  return JSON.stringify({ ...first, was_duplicate: true }) === JSON.stringify(replay);
}

/** Run the typed-store contract. */
export async function runTable(t: TestContext, connect: Connect, opts: RunTableOptions = {}): Promise<void> {
  if (opts.tenants && (!opts.tenants[0] || !opts.tenants[1] || opts.tenants[0] === opts.tenants[1])) {
    assert.fail("conformance requires two distinct provisioned tenants");
  }
  const newTenant = () => opts.tenants?.[0] ?? randomUUID();
  await runRevisions(t, connect, newTenant);
  await runConstraints(t, connect, newTenant);
  await runQueries(t, connect, newTenant);

  await t.test("get missing is not found", async () => {
    const table = files(await connect(newTenant()));
    await rejectsWith(table.get({ file_id: randomUUID() }), isMissing, "get of a missing row");
  });

  await t.test("upsert then get round-trips the row", async () => {
    const table = files(await connect(newTenant()));
    const id = randomUUID();
    await table.upsert(file(id, "a.pdf"));
    const got = await table.get({ file_id: id });
    assert.equal(got.filename, "a.pdf");
    assert.equal(got.status, "PENDING");
    assert.ok(got.tenant_id, "the broker fills the verified tenant in");
  });

  await t.test("insert refuses a duplicate without changing the stored row", async () => {
    const table = files(await connect(newTenant()));
    const id = randomUUID();
    await table.insert(file(id, "original.pdf"));
    await rejectsWith(table.insert(file(id, "replacement.pdf")), (err) => isUnique(err), "duplicate insert");
    assert.equal((await table.get({ file_id: id })).filename, "original.pdf");
  });

  await t.test("two concurrent inserts have exactly one winner", async () => {
    const table = files(await connect(newTenant()));
    const id = randomUUID();
    const names = ["a.pdf", "b.pdf"];
    const results = await Promise.allSettled(names.map((name) => table.insert(file(id, name))));
    const winners = names.filter((_, index) => results[index].status === "fulfilled");
    assert.equal(winners.length, 1, "exactly one insert wins");
    const lost = results.find((result) => result.status === "rejected") as PromiseRejectedResult;
    assert.ok(isUnique(lost.reason), `the loser fails with unique: ${lost.reason}`);
    assert.equal((await table.get({ file_id: id })).filename, winners[0]);
  });

  await t.test("transaction insert duplicate rolls back every earlier write", async () => {
    const u = await connect(newTenant());
    const table = files(u);
    const existing = randomUUID();
    const fresh = randomUUID();
    await table.insert(file(existing, "original.pdf"));
    await rejectsWith(
      u.tx((scope) => {
        scope.insert(FILE, file(fresh, "new.pdf"));
        scope.insert(FILE, file(existing, "replacement.pdf"));
      }),
      (err) => isUnique(err),
      "transaction duplicate",
    );
    await rejectsWith(table.get({ file_id: fresh }), isMissing, "the transaction left an earlier insert behind");
    assert.equal((await table.get({ file_id: existing })).filename, "original.pdf");
    await u.tx((scope) => scope.insert(FILE, file(fresh, "committed.pdf")));
    assert.equal((await table.get({ file_id: fresh })).filename, "committed.pdf");
  });

  await t.test("transaction require_affected refuses a missed row and rolls back the prefix", async () => {
    const u = await connect(newTenant());
    const table = files(u);
    const missing = { file_id: randomUUID() };
    const writes: Record<string, (scope: TxScope) => void> = {
      patch: (scope) => scope.patch(FILE, missing, { filename: "never.pdf" }),
      delete: (scope) => scope.delete(FILE, missing),
    };
    for (const [name, write] of Object.entries(writes)) {
      const prefix = file(randomUUID(), "must-roll-back.pdf");
      await rejectsWith(
        u.tx((scope) => {
          scope.insert(FILE, prefix);
          write(scope);
        }),
        code(ErrorCode.NotFound),
        `transaction ${name} of a missing row (UDB_NO_ROWS_AFFECTED)`,
      );
      await rejectsWith(table.get({ file_id: prefix.file_id }), isMissing, `transaction ${name} refusal committed its prefix`);
    }
  });

  await t.test("tenants never see each other's rows", async () => {
    const a = newTenant();
    const b = opts.tenants?.[1] ?? randomUUID();
    const first = files(await connect(a));
    const second = files(await connect(b));
    const id = randomUUID();
    await first.upsert(file(id, "secret.pdf"));
    await rejectsWith(second.get({ file_id: id }), isMissing, "another tenant read the row");
    await rejectsWith(second.select({ tenant_id: a }), () => true, "naming another tenant in a filter must be refused");
    await rejectsWith(second.upsert(file(id, "must-not-overwrite.pdf")), () => true, "another tenant upserted a stored global primary key");
    assert.equal((await first.get({ file_id: id })).filename, "secret.pdf");
  });

  await t.test("patch changes only the given columns and needs the row", async () => {
    const table = files(await connect(newTenant()));
    const id = randomUUID();
    await table.upsert(file(id, "draft.pdf"));
    await table.patch({ file_id: id }, { filename: "final.pdf" });
    const got = await table.get({ file_id: id });
    assert.equal(got.filename, "final.pdf");
    assert.equal(got.object_key, `conformance/${id}`);
    await rejectsWith(table.patch({ file_id: randomUUID() }, { filename: "x" }), isMissing, "patching a missing row");
  });

  await t.test("conditional update detects a lost race", async () => {
    const table = files(await connect(newTenant()));
    const id = randomUUID();
    await table.upsert(file(id, "v1.pdf"));
    await table.updateIf({ file_id: id }, { filename: "v1.pdf" }, { filename: "v2.pdf" });
    await rejectsWith(table.updateIf({ file_id: id }, { filename: "v1.pdf" }, { filename: "v3.pdf" }), isLost, "a stale expectation");
    assert.equal((await table.get({ file_id: id })).filename, "v2.pdf");
  });

  await t.test("update with retry applies the change once", async () => {
    const table = files(await connect(newTenant()));
    const id = randomUUID();
    await table.upsert(file(id, "n.pdf"));
    const updated = await table.updateWithRetry({ file_id: id }, (current) => ({ ...current, filename: `renamed-${current.filename}` }), 3);
    assert.equal(updated.filename, "renamed-n.pdf");
    assert.equal((await table.get({ file_id: id })).filename, "renamed-n.pdf");
  });

  await t.test("select pages, says when capped, and counts", async () => {
    const table = files(await connect(newTenant()));
    const ids: string[] = [];
    for (let i = 0; i < 5; i += 1) {
      const id = randomUUID();
      ids.push(id);
      await table.upsert(file(id, `p${i}.pdf`));
    }
    // A live login shares its tenant with the other cases and previous runs, so count
    // and pagination cover this case's five rows only.
    const filter: Record<string, unknown> = { file_id: { $in: ids } };
    const page = await table.select(filter, { limit: 2, includeTotal: true });
    assert.equal(page.rows.length, 2);
    assert.equal(page.hasMore, true);
    assert.equal(page.total, 5);
    let seen = 0;
    await table.selectAll(filter, () => {
      seen += 1;
    });
    assert.equal(seen, 5);
    assert.equal(await table.count({ ...filter, filename: { $in: ["p1.pdf", "p3.pdf"] } }), 2);
  });

  await t.test("a transaction commits every write or none", async () => {
    const u = await connect(newTenant());
    const table = files(u);
    const existing = randomUUID();
    const fresh = randomUUID();
    await table.upsert(file(existing, "v1.pdf"));
    const attempt = (expected: string) => (scope: TxScope) => {
      scope.upsert(FILE, file(fresh, "new.pdf"));
      scope.updateRowIf(FILE, file(existing, "v2.pdf"), { filename: expected });
    };
    await rejectsWith(u.tx(attempt("stale.pdf")), code(ErrorCode.Conflict), "a transaction with a lost CAS");
    await rejectsWith(table.get({ file_id: fresh }), isMissing, "the failed transaction's other write was kept");
    await u.tx(attempt("v1.pdf"));
    assert.equal((await table.get({ file_id: existing })).filename, "v2.pdf");
    await table.get({ file_id: fresh });
  });

  await t.test("delete needs the row", async () => {
    const table = files(await connect(newTenant()));
    const id = randomUUID();
    await table.upsert(file(id, "gone.pdf"));
    await table.delete({ file_id: id });
    await rejectsWith(table.delete({ file_id: id }), isMissing, "deleting it again");
  });
}

/** Run the cross-SDK cases in the shared JSON file. */
export async function runTableCases(t: TestContext, connect: Connect, casesPath: string = tableCasesPath()): Promise<void> {
  const contract = JSON.parse(fs.readFileSync(casesPath, "utf8"));
  for (const tc of contract.cases as { name: string; steps: any[] }[]) {
    await t.test(tc.name, async () => {
      const table = (await connect(randomUUID())).tableOf<Record<string, unknown>>(contract.message_type);
      const id = randomUUID();
      // "$id<digits>" ($id2, $id3, ...) is a fresh UUID of its own per case, replaced
      // before the bare "$id".
      const numbered = new Map<string, string>();
      const text = (value: string): string =>
        value
          .replace(/\$id(\d+)/g, (_, n: string) => {
            if (!numbered.has(n)) numbered.set(n, randomUUID());
            return numbered.get(n)!;
          })
          .replaceAll("$id", id);
      const substitute = (values: Record<string, unknown> | undefined): Record<string, unknown> =>
        Object.fromEntries(Object.entries(values ?? {}).map(([key, value]) => [key, typeof value === "string" ? text(value) : value]));
      for (const [index, step] of tc.steps.entries()) {
        let got: Record<string, unknown> | undefined;
        let affected: number | undefined;
        let error = "";
        try {
          const key = substitute(step.key);
          switch (step.op) {
            case "upsert":
              await table.upsert(substitute(step.row));
              break;
            case "get":
              got = await table.get(key);
              break;
            case "patch":
              await table.patch(key, substitute(step.changes));
              break;
            case "update_if":
              await table.updateIf(key, substitute(step.expected), substitute(step.changes));
              break;
            case "delete":
              await table.delete(key);
              break;
            case "bulk_upsert":
              affected = (await table.bulkUpsert(((step.rows ?? []) as Record<string, unknown>[]).map((row) => substitute(row)))).affected_rows;
              break;
            default:
              assert.fail(`unknown op ${step.op}`);
          }
        } catch (err) {
          error = codeOf(err);
        }
        assert.equal(error, step.expect?.error ?? "", `step ${index} (${step.op})`);
        if (step.expect?.affected_rows !== undefined) {
          assert.equal(affected, step.expect.affected_rows, `step ${index}: affected_rows`);
        }
        if (step.expect?.row) {
          assert.ok(got, `step ${index} returned a row`);
          for (const [field, want] of Object.entries(substitute(step.expect.row))) {
            assert.equal(got![field], want, `step ${index}: ${field}`);
          }
        }
      }
    });
  }
}

/** The shared case file: bundled with the package, or sdk/conformance in the repo. */
export function tableCasesPath(): string {
  for (const relative of ["../conformance", "../../conformance", "../../../conformance"]) {
    const candidate = path.join(__dirname, relative, "table-cases.json");
    if (fs.existsSync(candidate)) return candidate;
  }
  throw new Error("udbtest: table-cases.json is not bundled with this package");
}

async function runRevisions(t: TestContext, connect: Connect, tenant: () => string): Promise<void> {
  await t.test("opaque revisions reject stale writes and survive transaction rollback", async () => {
    const u = await connect(tenant());
    const row = file(randomUUID(), "revision-original.pdf");
    const insert = { message_type: FILE, record_json: rawRecord(row) };
    const unary = (method: string, request: any) => u.core.unary<any>(DATABROKER, method, request);
    const written = await unary("Insert", insert);
    assert.ok(written.revision, "Insert omitted its committed revision");
    const initial: string = written.revision;
    const filter = { file_id: row.file_id };
    const read = async () => {
      const result = await unary("Select", { message_type: FILE, filter, fields: ["filename"], include_revision: true });
      assert.equal(result.records_json.length, 1);
      assert.equal(result.record_revisions.length, 1, "Select omitted or misaligned the row revision");
      assert.equal(JSON.parse(result.records_json[0].toString()).file_id, row.file_id, "a revision read includes the primary key");
      return result.record_revisions[0] as string;
    };
    assert.equal(await read(), initial, "served revision differs from Insert");
    const update = {
      message_type: FILE,
      filter,
      changes: { filename: "revision-updated.pdf" },
      expected_revision: initial,
      require_affected: 1,
      idempotency_key: randomUUID(),
    };
    const first = await unary("Update", update);
    assert.ok(first.revision && first.revision !== initial && (await read()) === first.revision, "Update did not publish a new revision atomically");
    const replay = await unary("Update", update);
    assert.ok(matchesKeyedReplay(first, replay) && (await read()) === first.revision, "a lost-response retry rechecked or bumped its revision");
    for (const changed of [{ ...update, expected_revision: first.revision }, { ...update, require_affected: 2 }]) {
      await rejectsWith(unary("Update", changed), reason("UDB_IDEMPOTENCY_REUSE"), "a changed concurrency guard replayed the old receipt");
      assert.equal(await read(), first.revision);
    }
    const stale: any = { ...update, idempotency_key: "" };
    const refusal = async (request: any, want: string) => {
      await rejectsWith(unary("Update", request), (err: any) => inspect(err).reason === want && (err.detail?.field_violations ?? []).length > 0, `revision refusal ${want}`);
      assert.equal(await read(), first.revision, "a refused Update changed the revision");
    };
    await refusal(stale, "UDB_REVISION_CONFLICT");
    await refusal({ ...stale, filter: { filename: "revision-updated.pdf" }, expected_revision: first.revision }, "UDB_CAS_KEY_NOT_PK");
    await rejectsWith(unary("Update", { ...stale, expected_revision: "invalid-revision" }), code(ErrorCode.Invalid), "a malformed revision");
    const deleteRequest = { message_type: FILE, filter, expected_revision: initial, require_affected: 1 };
    await rejectsWith(unary("Delete", deleteRequest), reason("UDB_REVISION_CONFLICT"), "a stale Delete");
    const key = { file_id: row.file_id };
    await u.tx((scope) => scope.patch(FILE, key, { filename: "revision-transaction.pdf" }));
    const transactionRevision = await read();
    assert.notEqual(transactionRevision, first.revision, "BeginTx Update left the pre-transaction revision valid");
    await rejectsWith(unary("Update", { ...stale, expected_revision: first.revision }), reason("UDB_REVISION_CONFLICT"), "a pre-transaction revision stayed writable");
    await rejectsWith(
      u.tx((scope) => {
        scope.patch(FILE, key, { filename: "must-roll-back.pdf" });
        scope.patch(FILE, key, { file_id: row.file_id });
      }),
      code(ErrorCode.Invalid),
      "an invalid transaction",
    );
    assert.equal(await read(), transactionRevision, "transaction rollback retained a revision change");
    assert.equal((await files(u).get(key)).filename, "revision-transaction.pdf");
    await unary("Delete", { ...deleteRequest, expected_revision: transactionRevision });
    // File is soft-deleted. Upsert restores the same primary key; an Insert would
    // correctly refuse the tombstone's unique constraint.
    const revived = await unary("Upsert", insert);
    assert.ok(revived.revision && revived.revision !== transactionRevision, "delete and reinsert revived the previous revision");
    await rejectsWith(unary("Update", { ...stale, expected_revision: transactionRevision }), reason("UDB_REVISION_CONFLICT"), "a pre-delete revision was accepted after reinsert");
    const plain = await unary("Select", { message_type: FILE, filter });
    assert.equal(plain.record_revisions.length, 0, "default Select unexpectedly included revisions");
  });
}

async function runConstraints(t: TestContext, connect: Connect, tenant: () => string): Promise<void> {
  await t.test("update admission preserves identity and rolls back transaction prefixes", async () => {
    const u = await connect(tenant());
    const table = files(u);
    const row = file(randomUUID(), "immutable-original.pdf");
    await table.insert(row);
    const key = { file_id: row.file_id };
    const meta = u.core.opts.meta;
    const stored = await table.get(key);
    assert.equal(stored.project_id ?? "", meta.projectId ?? "", "stored project differs from the verified project");
    const values: Record<string, string> = { file_id: row.file_id, tenant_id: meta.tenantId, project_id: meta.projectId ?? "" };
    for (const column of Object.keys(values)) {
      // Even resending the current identity is forbidden; the setter never ignores it.
      const changes = { [column]: values[column], filename: "must-not-change.pdf" };
      await rejectsWith(table.patch(key, changes), code(ErrorCode.Invalid), `immutable ${column} update`);
      const prefix = file(randomUUID(), "must-roll-back.pdf");
      await rejectsWith(
        u.tx((scope) => {
          scope.insert(FILE, prefix);
          scope.patch(FILE, key, changes);
        }),
        code(ErrorCode.Invalid),
        `immutable ${column} transaction`,
      );
      await rejectsWith(table.get({ file_id: prefix.file_id }), isMissing, `immutable ${column} refusal committed its transaction prefix`);
    }
    for (const request of [
      { message_type: FILE, filter: key },
      { message_type: FILE, changes: { size_bytes: "1" } },
      { message_type: FILE, filter: key, changes: { size_bytes: "1" }, increments: [{ column: "size_bytes", delta: 2 }] },
    ]) {
      await rejectsWith(u.core.unary(DATABROKER, "Update", request), code(ErrorCode.Invalid), "an invalid Update shape");
    }
    const got = await table.get(key);
    assert.equal(got.filename, row.filename, "a refused update changed its target");
    assert.equal(Number(got.size_bytes ?? 0), 0, "a refused update changed its target");
  });

  await t.test("unary keys replay before CAS and reject changed input", async () => {
    const u = await connect(tenant());
    const unary = (method: string, request: any) => u.core.unary<any>(DATABROKER, method, request);
    const row = file(randomUUID(), "keyed-original.pdf");
    const insert = { message_type: FILE, record_json: rawRecord(row), idempotency_key: randomUUID() };
    let first = await unary("Insert", insert);
    let replay = await unary("Insert", insert);
    assert.ok(matchesKeyedReplay(first, replay), "an identical keyed insert did not replay its original receipt");
    // Insert and Upsert are distinct operations even when their key and target match.
    const upsert = { ...insert, record_json: rawRecord({ ...row, filename: "keyed-upserted.pdf" }) };
    first = await unary("Upsert", upsert);
    assert.equal(first.was_duplicate, false, "Insert and Upsert collided in the key namespace");
    replay = await unary("Upsert", upsert);
    assert.ok(matchesKeyedReplay(first, replay), "an identical keyed upsert did not replay its original receipt");
    const filter = { file_id: row.file_id };
    const update = {
      message_type: FILE,
      filter,
      expected: { size_bytes: "0" },
      require_affected: 1,
      idempotency_key: randomUUID(),
      increments: [{ column: "size_bytes", delta: 7 }],
    };
    first = await unary("Update", update);
    replay = await unary("Update", update);
    assert.ok(matchesKeyedReplay(first, replay), "a keyed update replay checked the already-changed CAS value");
    let reuse: unknown;
    await unary("Update", { ...update, increments: [{ column: "size_bytes", delta: 11 }] }).catch((err) => {
      reuse = err;
    });
    const info = inspect(reuse);
    assert.equal(info.reason, "UDB_IDEMPOTENCY_REUSE");
    assert.equal(info.code, ErrorCode.Precondition);
    assert.ok(info.fixHint, "changed input must have an actionable key-reuse refusal");
    const table = files(u);
    const got = await table.get(filter);
    assert.equal(Number(got.size_bytes), 7, "a keyed increment ran more than once or changed input wrote");
    assert.equal(got.filename, "keyed-upserted.pdf");
    const remove = { message_type: FILE, filter, require_affected: 1, idempotency_key: randomUUID() };
    first = await unary("Delete", remove);
    replay = await unary("Delete", remove);
    assert.ok(matchesKeyedReplay(first, replay), "a keyed delete replay checked the already-deleted row");
    await rejectsWith(table.get(filter), isMissing, "a keyed delete left a visible row");
  });

  await t.test("explicit primary conflict target replaces the original row", async () => {
    const u = await connect(tenant());
    const row = file(randomUUID(), "original.pdf");
    const table = files(u);
    await table.insert(row);
    await u.core.unary(DATABROKER, "Upsert", {
      message_type: FILE,
      record_json: rawRecord({ ...row, filename: "replaced.pdf" }),
      conflict_fields: ["file_id"],
    });
    assert.equal((await table.get({ file_id: row.file_id })).filename, "replaced.pdf");
  });

  await t.test("unsupported transaction idempotency key refuses the entire batch", async () => {
    const u = await connect(tenant());
    // Insert and outbox enqueue have no receipt, so a key on them refuses the batch.
    for (const operation of ["insert", "enqueue_outbox_event"]) {
      const row = file(randomUUID(), "must-not-commit.pdf");
      const record = rawRecord(row);
      const keyed: any = { operation, message_type: FILE, record_json: record, idempotency_key: randomUUID() };
      if (operation === "enqueue_outbox_event") {
        delete keyed.record_json;
        keyed.collection = "udb.conformance.v1";
        keyed.object_key = row.file_id;
        keyed.payload = {
          event_id: randomUUID(),
          event_type: "conformance",
          document_id: row.file_id,
          correlation_id: randomUUID(),
          envelope_version: 2,
          payload: {},
        };
      }
      const violations = await new Promise<any[]>((resolve) => {
        const stream = u.core.bidiStream(DATABROKER, "BeginTx", { deadlineMs: 10_000 });
        const collected: any[] = [];
        stream.on("data", (frame: any) => collected.push(...(frame?.error_detail?.field_violations ?? [])));
        stream.on("error", (err: any) => {
          collected.push(...(err?.udb?.detail?.field_violations ?? []));
          resolve(collected);
        });
        stream.on("end", () => resolve(collected));
        stream.write({ operation: "upsert", message_type: FILE, record_json: record });
        stream.write(keyed);
        stream.write({ commit: true });
        stream.end();
      });
      assert.ok(violations.some((violation) => violation.field === "mutations[1].idempotency_key"), `${operation}: an ignored key was not explicitly refused`);
      await rejectsWith(files(u).get({ file_id: row.file_id }), isMissing, `${operation}: the refused transaction kept its prefix`);
    }
  });

  await t.test("transaction keys replay before CAS and reject changed input", async () => {
    const u = await connect(tenant());
    const table = files(u);
    const row = file(randomUUID(), "tx-keyed-original.pdf");
    const other = file(randomUUID(), "tx-keyed-upsert.pdf");
    await table.insert(row);
    const key = { file_id: row.file_id };
    const read = async () => {
      const result = await u.core.unary<any>(DATABROKER, "Select", { message_type: FILE, filter: key, fields: ["filename"], include_revision: true });
      assert.equal(result.record_revisions.length, 1, "Select omitted the row revision");
      return result.record_revisions[0] as string;
    };
    const before = await read();
    const replayKey = randomUUID();
    const attempt = (filename: string) => (scope: TxScope) => {
      scope.upsert(FILE, other);
      scope.updateIf(FILE, key, { filename: row.filename }, { filename });
    };
    await u.tx(attempt("tx-keyed-v2.pdf"), { replayKey });
    const committed = await read();
    assert.notEqual(committed, before, "the keyed transaction did not write");
    // The stored row no longer holds `expected`: only a replay that skips CAS succeeds.
    await u.tx(attempt("tx-keyed-v2.pdf"), { replayKey });
    assert.equal(await read(), committed, "a replayed transaction rechecked CAS or wrote again");
    assert.equal((await table.get(key)).filename, "tx-keyed-v2.pdf");
    let reuse: unknown;
    await u.tx(attempt("tx-keyed-v3.pdf"), { replayKey }).catch((err) => {
      reuse = err;
    });
    const info = inspect(reuse);
    assert.equal(info.reason, "UDB_IDEMPOTENCY_REUSE", `changed input under a reused key: ${reuse}`);
    assert.equal(info.code, ErrorCode.Precondition);
    assert.equal(await read(), committed, "a refused key reuse changed the row");
    assert.equal((await table.get(key)).filename, "tx-keyed-v2.pdf");
    assert.equal((await table.get({ file_id: other.file_id })).filename, other.filename);
  });

  await t.test("secondary uniqueness refuses insert and rolls back a multirow update", async () => {
    const u = await connect(tenant());
    const table = files(u);
    const first = file(randomUUID(), "first.pdf");
    const second = file(randomUUID(), "second.pdf");
    await table.insert(first);
    await table.insert(second);
    const duplicate = { ...file(randomUUID(), "duplicate.pdf"), object_key: first.object_key };
    await rejectsWith(table.insert(duplicate), (err) => isUnique(err) && Boolean(inspect(err).constraint), "a secondary duplicate");
    await rejectsWith(
      u.core.unary(DATABROKER, "Update", {
        message_type: FILE,
        filter: { file_id: { $in: [first.file_id, second.file_id] } },
        changes: { object_key: `conformance/shared/${randomUUID()}`, filename: "must-rollback.pdf" },
      }),
      (err) => isUnique(err),
      "a multirow collision",
    );
    for (const original of [first, second]) {
      const got = await table.get({ file_id: original.file_id });
      assert.equal(got.filename, original.filename, "a failed statement changed a row");
      assert.equal(got.object_key, original.object_key, "a failed statement changed a row");
    }
    const fresh = file(randomUUID(), "tx-prefix.pdf");
    await rejectsWith(
      u.tx((scope) => {
        scope.insert(FILE, fresh);
        scope.insert(FILE, duplicate);
      }),
      (err) => isUnique(err),
      "a transaction secondary collision",
    );
    await rejectsWith(table.get({ file_id: fresh.file_id }), isMissing, "a unique refusal kept the transaction prefix");
  });

  await t.test("NOT NULL refusal names the column", async () => {
    const u = await connect(tenant());
    let refused: unknown;
    await u.core
      .unary(DATABROKER, "Upsert", {
        message_type: FILE,
        record_json: Buffer.from(JSON.stringify({ file_id: randomUUID(), filename: null, object_key: `conformance/${randomUUID()}` })),
      })
      .catch((err) => {
        refused = err;
      });
    const info = inspect(refused);
    assert.equal(info.code, ErrorCode.NotNull, `NULL refusal: ${refused}`);
    assert.equal(info.column, "filename");
  });

  await t.test("int64 stays exact through storage and an atomic increment", async () => {
    const table = files(await connect(tenant()));
    const row = { ...file(randomUUID(), "large.pdf"), size_bytes: "9007199254740993" };
    await table.insert(row);
    const key = { file_id: row.file_id };
    assert.equal(String((await table.get(key)).size_bytes), "9007199254740993", "a stored int64 was rounded");
    await table.increment(key, { size_bytes: 1 });
    assert.equal(String((await table.get(key)).size_bytes), "9007199254740994", "an increment was rounded");
  });

  await t.test("soft delete retains unique keys and upsert revives the row", async () => {
    const table = files(await connect(tenant()));
    const row = file(randomUUID(), "deleted.pdf");
    await table.insert(row);
    const key = { file_id: row.file_id };
    await table.delete(key);
    await rejectsWith(table.get(key), isMissing, "a tombstone remained visible");
    await rejectsWith(table.insert(row), (err) => isUnique(err), "a strict insert reused a tombstone key");
    await table.upsert({ ...row, filename: "revived.pdf" });
    const got = await table.get(key);
    assert.equal(got.filename, "revived.pdf");
    assert.equal(got.deleted_at ?? null, null, "upsert did not revive the tombstone");
  });

  // Which branch runs depends on the connected identity's scopes: a caller
  // holding udb:deleted:read sees its deleted row marked, any other caller is
  // refused by name. A plain read keeps hiding the deleted row either way.
  await t.test("include_deleted needs udb:deleted:read and marks deleted rows", async () => {
    const table = files(await connect(tenant()));
    const live = file(randomUUID(), "kept.pdf");
    const gone = file(randomUUID(), "gone.pdf");
    await table.insert(live);
    await table.insert(gone);
    await table.delete({ file_id: gone.file_id });
    const where = { file_id: { $in: [live.file_id, gone.file_id] } };
    const plainReadHidesTheDeletedRow = async () => {
      const page = await table.select(where);
      assert.deepEqual(
        page.rows.map((row) => row.file_id),
        [live.file_id],
        "a plain read must return only the live row",
      );
    };
    await plainReadHidesTheDeletedRow();
    let page;
    try {
      page = await table.select(where, { includeDeleted: true, includeTotal: true });
    } catch (err) {
      const info = inspect(err);
      assert.equal(info.code, ErrorCode.ScopeMissing, `include_deleted without the scope: ${err}`);
      assert.equal(info.reason, "UDB_DELETED_READ_SCOPE_MISSING");
      assert.equal(info.missing?.scope, "udb:deleted:read");
      await plainReadHidesTheDeletedRow();
      return;
    }
    assert.equal(page.rows.length, 2, "both rows come back");
    assert.equal(page.total, 2, "the total counts the deleted row");
    for (const row of page.rows) {
      const deleted = row.deleted_at !== undefined && row.deleted_at !== null;
      assert.equal(deleted, row.file_id === gone.file_id, `only the deleted row carries deleted_at (${row.file_id})`);
    }
    await plainReadHidesTheDeletedRow();
  });

  await t.test("default page caps at 100 and selectAll visits every row", async () => {
    const u = await connect(tenant());
    const table = files(u);
    const ids: string[] = [];
    await u.tx((scope) => {
      for (let i = 0; i < 103; i += 1) {
        const row = file(randomUUID(), `page-${String(i).padStart(3, "0")}.pdf`);
        ids.push(row.file_id);
        scope.insert(FILE, row);
      }
    });
    const filter = { file_id: { $in: ids } };
    const page = await table.select(filter, { includeTotal: true });
    assert.equal(page.rows.length, 100);
    assert.equal(page.hasMore, true);
    assert.equal(page.total, 103);
    const seen = new Set<string>();
    await table.selectAll(filter, (row) => {
      assert.ok(!seen.has(row.file_id), "selectAll repeated a row");
      seen.add(row.file_id);
    });
    assert.equal(seen.size, 103);
  });
}

// Negating a predicate must not turn a SQL NULL into a matching row.
/** Run the join-fusion read contract: a Select whose message_type names two entities
 *  joined by a foreign key (here the authz PolicySet and its PolicyVersions) returns
 *  one record per joined pair, its columns aliased `{Message}__{column}`. The target
 *  must serve both entities and the storage File entity, and grant the caller
 *  udb:read on the fused message type. */
export async function runJoin(t: TestContext, connect: Connect, opts: RunTableOptions = {}): Promise<void> {
  const [tenant, peer] = opts.tenants ?? [randomUUID(), randomUUID()];
  await t.test("join fusion reads related rows of both tables as one aliased record", async () => {
    const u = await connect(tenant);
    const unary = (c: UdbProject, method: string, request: any) => c.core.unary<any>(DATABROKER, method, request);
    const SET = "udb.core.authz.entity.v1.PolicySet";
    const VERSION = "udb.core.authz.entity.v1.PolicyVersion";
    const joined = `${SET},${VERSION}`;
    const set = randomUUID();
    const lone = randomUUID();
    const second = randomUUID();
    await unary(u, "Upsert", { message_type: SET, payload: { policy_set_id: set, name: `join-${set}` } });
    await unary(u, "Upsert", { message_type: SET, payload: { policy_set_id: lone, name: `join-${lone}` } });
    await unary(u, "Upsert", { message_type: VERSION, payload: { policy_version_id: randomUUID(), policy_set_id: set, version_number: 1 } });
    await unary(u, "Upsert", { message_type: VERSION, payload: { policy_version_id: second, policy_set_id: set, version_number: 2 } });
    const read = async (c: UdbProject, request: any) => {
      const result = await unary(c, "Select", { message_type: joined, ...request });
      return { result, records: (result.records_json ?? []).map((raw: Buffer) => JSON.parse(raw.toString())) as Record<string, unknown>[] };
    };

    let { result, records } = await read(u, { filter: { policy_set_id: set }, fields: ["PolicySet.name", "PolicyVersion.version_number"] });
    for (const record of records) {
      assert.deepEqual(Object.keys(record).sort(), ["PolicySet__name", "PolicyVersion__version_number"]);
      assert.equal(record.PolicySet__name, `join-${set}`);
    }
    assert.deepEqual(records.map((record) => String(record.PolicyVersion__version_number)).sort(), ["1", "2"]);

    ({ result, records } = await read(u, { filter: { policy_set_id: set }, limit: 1 }));
    assert.equal(records.length, 1, "a join honours the limit");
    assert.ok(result.has_more, "a capped join says there is more");

    ({ records } = await read(u, { filter: { policy_set_id: set, "PolicyVersion.version_number": 2 } }));
    assert.equal(records.length, 1, "the default projection filtered to one version");
    assert.equal(records[0].PolicySet__policy_set_id, set);
    assert.equal(records[0].PolicyVersion__policy_version_id, second);
    for (const alias of Object.keys(records[0])) {
      assert.ok(alias.startsWith("PolicySet__") || alias.startsWith("PolicyVersion__"), `unaliased column ${alias}`);
    }

    ({ records } = await read(u, { filter: { policy_set_id: lone } }));
    assert.equal(records.length, 0, "an inner join drops a set without versions");
    ({ records } = await read(await connect(peer), { filter: { policy_set_id: set } }));
    assert.equal(records.length, 0, "another tenant joins nothing");

    const invalid = (err: any) => err?.code === 3;
    await rejectsWith(read(u, { sort: [{ field: "name" }] }), invalid, "a sorted join");
    await rejectsWith(read(u, { filter: { policy_set_id: { $eq: set } } }), invalid, "an operator filter");
    await rejectsWith(unary(u, "Select", { message_type: `${FILE},${SET}` }), invalid, "tables without a foreign key");

    await unary(u, "Delete", { message_type: SET, filter: { policy_set_id: set } });
    ({ records } = await read(u, { filter: { policy_set_id: set } }));
    assert.equal(records.length, 0, "a soft-deleted set no longer joins");
  });
}

async function runQueries(t: TestContext, connect: Connect, tenant: () => string): Promise<void> {
  await t.test("unknown columns are refused even without matching rows", async () => {
    const u = await connect(tenant());
    await rejectsWith(
      u.core.unary(DATABROKER, "Select", {
        message_type: FILE,
        filter: { file_id: randomUUID(), $or: [{ not_a_declared_column: "value" }] },
      }),
      (err: any) => err?.code === 3,
      "an unknown column",
    );
  });

  await t.test("nullable predicates preserve SQL three-valued logic", async (sub) => {
    const u = await connect(tenant());
    const table = files(u);
    const ids = [randomUUID(), randomUUID(), randomUUID()];
    for (const [index, contentType] of [null, "blocked/type", "allowed/type"].entries()) {
      await u.core.unary(DATABROKER, "Upsert", {
        message_type: FILE,
        payload: { file_id: ids[index], filename: "nullable.pdf", object_key: `conformance/${ids[index]}`, content_type: contentType },
      });
    }
    const cases: [string, Record<string, unknown>, string[]][] = [
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
        assert.deepEqual(page.rows.map((row) => row.file_id).sort(), [...want].sort());
      });
    }
  });
}
