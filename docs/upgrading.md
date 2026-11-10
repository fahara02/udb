# Upgrading a live UDB deployment

This page is about upgrading **UDB itself** against a database that already has
data in it. A fresh deployment needs none of this — start the broker and it
builds the schema.

Changing **your own protos** on a live database is a related but different job,
and it has its own worked walkthrough:
[examples/schema_change](../examples/schema_change). The commands overlap; the
reasons you run them differ.

The short version:

```bash
udb verify --live --dsn "$UDB_PG_DSN"          # 1. does the live schema match the protos?
udb drift --prior prior-manifest.json          # 2. what will the migration change?
#    (produce that file with: UDB_MANIFEST_EXPORT_PATH=prior-manifest.json udb manifest-export proto
#     — NOT `udb catalog`, which is a different file and diffs as phantom DropTable ops)
udb plan --prior prior-manifest.json \
         --emit-approval-plan plan.json        # 3. produce the file `serve` will accept
# 4. rehearse the whole thing against a pg_dump clone before touching production
```

Every step is read-only except the last, and each one answers a question that is
otherwise answered by a failed startup.

---

## 1. Check the live database against your protos first

`udb verify --live` runs **the same comparison the broker runs during startup
verification**, read-only, before any DDL is applied:

```bash
udb verify --live --dsn "postgresql://user:pass@host:5432/app"
```

It exits `0` when the live schema matches the manifest and `1` with a list of
findings when it does not, so it can gate a deploy directly. It also checks
delivery health — outbox backlog, durable-consumer lag against the journal,
projection queue age — and exits `1` when a check fails, for example an outbox
that no CDC tailer drains.

This matters because of a distinction that is easy to miss:

| Command | Compares | Catches |
|---|---|---|
| `udb drift --prior <manifest>` | protos vs a **prior manifest** | what your schema change will do |
| `udb verify --live --dsn <dsn>` | protos vs the **live database** | what your database already disagrees about |

A long-lived deployment usually has divergence in the second column that nobody
put there deliberately — columns an older migration tool made `NOT NULL`, an
index created by hand during an incident. Those are invisible to every offline
tool. Before `verify --live` existed, they surfaced only at startup, *after* the
migration had been applied.

### Reading the findings

Each finding names its schema, table and column:

```
[missing_index] missing index uq_orders_customer on sales.orders
[nullability_mismatch] PostgreSQL column fleet.driver_profiles.user_id nullability mismatch: manifest not_null=false, live not_null=true
```

Not every finding blocks a startup. A column that is `NOT NULL` in the database
while the manifest allows NULL is *stricter* than UDB requires — it is reported
as a warning, because no read can break and no migration will ever relax it. The
reverse (manifest requires `NOT NULL`, database allows NULL) does block, because
the table may already hold values the contract says cannot exist.

---

## 2. See what the migration will change

```bash
udb drift --prior prior-manifest.json
```

Operations are classified `SafeAuto`, `RequiresReview`, or `Blocked`. A blocked
operation cannot be applied even with an approved plan — it means the change is
not expressible as a safe migration and the proto needs adjusting.

### Gate on `data_destructive`, never on the operation name

Each planned operation carries an explicit flag:

```json
{ "kind": "DropIndex", "safety": "SafeAuto", "data_destructive": false }
{ "kind": "DropColumn", "safety": "RequiresReview", "data_destructive": true }
```

If your deploy pipeline blocks destructive changes, gate on `data_destructive`.
A rule written as "reject anything starting with `Drop`" also rejects
`DropIndex` and `DropPolicy` — which UDB reissues on ordinary version upgrades,
each paired with a matching `Create` — and `DropNotNull`, a widening that cannot
reject any write which succeeds today. Such a rule makes every UDB upgrade
impossible.

`data_destructive` is true only where committed rows or their values can be
lost: dropping a table, column, partition, collection, bucket or store; an
in-place column type rewrite; a storage-engine change.

---

## 3. Produce the approval plan

```bash
udb plan --prior prior-manifest.json --emit-approval-plan plan.json
```

This writes the exact file `serve` accepts, including the `operations_hash` that
binds it to one specific change set. Point the broker at it:

```bash
UDB_MIGRATE_ENABLED=true
UDB_MIGRATION_APPROVAL_PLAN=/path/to/plan.json   # or config: migration.require_approval_plan
```

The environment variable wins over the config file, so a deploy names the plan it
just approved. With `UDB_MIGRATE_ENABLED=false` (the `udb env --profile enterprise`
default) a start applies only the project-schema changes this plan covers, and
refuses to start while any other change is pending.

With an approval signing key configured, `serve` refuses this file and requires a
signed plan. Each approver signs, with their key in `UDB_APPROVAL_APPROVER_KEYS`, or
`UDB_APPROVAL_SIGNING_KEY` for a single approver. Then seal the signatures:

```bash
udb plan sign --plan plan.json --approver dba --reason "reviewed" --out dba.sig.json
udb plan seal --plan plan.json --signature dba.sig.json --out approved.json
# config: migration.require_approval_plan=/path/to/approved.json
```

A migration stops, before it changes anything, on a declared change UDB has no way to
apply: partition settings and retention, a primary key, how the database fills a column
(identity, generated, serial), an index UDB derives from a column, a column renamed and
changed at once, a removed or unrunnable `sql_artifact`, and a column change on a
schema-bound store's table. Apply it by hand, then start once with
`UDB_ACK_MANUAL_BACKEND_RECONCILIATION=true`, or revert the proto.

`udb verify --live` compares each table's `CHECK` rules with the protos. A rule the
database lacks fails the run and prints the statement that applies it. A rule the
protos never declared is reported and does not fail the run.

If the plan and the computed change set disagree, startup fails **before**
mutating anything, naming the mismatch. Regenerate the plan and review again —
that disagreement is the gate working.

---

## 4. Rehearse against a clone

Restore a `pg_dump` into a scratch database, give it its own runtime volume and
its own Redis index, and run the whole upgrade there first. UDB's isolation
primitives make this cheap, and it is the only way to see the real interaction
between your data and the migration.

```bash
createdb upgrade_rehearsal
pg_restore -d upgrade_rehearsal production.dump
udb verify --live --dsn "postgresql://localhost/upgrade_rehearsal"
```

---

## Projects need a catalog before they can serve

From 0.5.9 the data plane refuses any authenticated principal whose project has
no ACTIVE catalog, rather than quietly falling back to the default project's
schema. A deployment created before projects had catalogs has no catalog rows at
all, so after upgrading, every service authenticating under a named project
fails its first call:

```
project 'acme' has no ACTIVE catalog, and falling back to the default project is refused
```

Give each project a catalog once:

```bash
udb up --project acme --tenant "$UDB_TENANT_ID" --dsn "$UDB_PG_DSN"
```

`udb up` builds the manifest from your protos, checks the database against it,
and bootstraps the project's catalog when it has none. Re-running is safe: when
the ACTIVE catalog already matches the protos the catalog step prints
`[skipped]`. When it differs, `udb up` plans the reviewed transition through the
broker and stops at `[next]` with the exact `udb up … --approve <run-id>`
command to run after you have reviewed the plan. The same command then applies
the project's declared policies, service accounts and seed data from
`udb.yaml`.

`udb catalog bootstrap --project acme` remains for the catalog alone. It never
replaces an ACTIVE catalog: it prints "unchanged" when the ACTIVE catalog
matches the protos, and exits 3 naming `udb catalog transition status` when it
does not. Before 0.6.1 it reported success in that case too.

Run it for every project your services authenticate under, not just the default.

---

## When startup fails after the migration applied

The DDL is applied before verification runs, so it is possible to end up with a
changed schema and a broker that will not serve. When that happens the broker
does **not** record the new manifest, so restarting repeats the same
verification rather than silently converging.

The failure lists every finding as its own `ERROR` line, each naming its schema,
table and column. Read those first — they say exactly what disagrees.

Then either reconcile the schema (adjust the proto, or fix the database), or use
the repair path:

```bash
UDB_MIGRATION_EMERGENCY_AUTO_ALTER=true
```

This feeds the live-vs-manifest findings to the repair planner instead of
fail-closing, and applies the safe repairs. Turn it on deliberately, for a
verified set of findings you have already read — not as a general "make startup
work" switch.

---

## Version-specific notes

**Upgrading from before 0.5.9** — 0.5.9 added live nullability verification.
Deployments created earlier never had it enforced, so tables that predate proto
ownership commonly carry constraints the protos never declared. Run
`udb verify --live` first; from 0.5.15 the stricter-than-required direction is a
warning rather than a startup blocker.

**Upgrading from before 0.5.14** — releases 0.5.8 through 0.5.12 could refuse to
start on a database created earlier, reporting field-number reuse on
`created_at`/`updated_at`/`created_by`. Those are auto-generated audit columns;
the numbering was fixed in 0.5.14 and existing deployments migrate cleanly from
that release onward. Upgrade to 0.5.14 or later directly.

**Upgrading to 0.5.18 — generated Go entity code is source-breaking.**
`<Entity>ToUDBRecord` now returns `(map[string]any, error)` instead of
`map[string]any`. `protojson.Marshal` can fail for a message stored in a JSON
column, and the old signature had nowhere to put that failure — the alternatives
were dropping the column silently or panicking. `FromUDBRow` was already
`(*T, error)`, so the two are now symmetric. Regenerate, then take the error at
each call site:

```go
rec, err := UserToUDBRecord(u)
if err != nil {
    return fmt.Errorf("encode user: %w", err)
}
```

**Skip 0.5.18 if you generate Go entity code — use 0.5.19.** 0.5.18 could not
generate it at all for a proto set containing a message-valued JSON column: the
emitter wrote doubled braces into the file and its own `gofmt` check rejected the
result, so generation failed rather than producing bad code. Fixed in 0.5.19.
Nothing else in 0.5.18 is affected — the broker and every SDK client are
unchanged — so a deployment that does not regenerate Go entities can run 0.5.18
safely.

**Upgrading to 0.5.19 — proto `map` columns in JSON columns now round-trip.** A
`map<K, V>` field annotated onto a JSON/JSONB column was emitted as if it were a
message (0.5.18) or as raw JSON text (0.5.17 and earlier), and neither form
compiled. It now marshals with `json.Marshal` and decodes into a typed
`map[K]V`. If you hand-wrote that column as a workaround, drop the workaround
after regenerating.

**Upgrading to 0.5.18 — the durable audit sink now verifies its table.** With
`UDB_AUDIT_SINK=postgres`, boot now checks that `UDB_AUDIT_PG_TABLE` names a
relation UDB can actually *write to*, not merely one it can create. Pointing it
at a pre-existing table of the wrong shape used to boot healthy and then fail
every insert. Under `UDB_FAIL_CLOSED` a bad table now refuses startup and names
the missing columns; otherwise it logs the same detail, the sink degrades to
stdout, and `GetHealthReport` reports that degradation as a failure instead of
staying silent.

---

**Upgrading to 0.6.8 — security options now take effect.** `(column_security)` was
ignored, so a column declared with `is_pii` was returned in clear, and one declared
with `is_encrypted` was stored in clear. Both now apply, and a
`secret_classification` of personal data or a secret masks its column unless
`output_view` is public. Before upgrading:

- run `udb check` on your protos: a key an option does not have is now refused when
  the proto is parsed;
- find `column_security` and `secret_classification` in your protos, and give readers
  of those columns `udb:pii:read`;
- a column newly marked `is_encrypted` still holds clear text in its existing rows. They
  read as before, but stay in clear until rewritten, and a lookup by value does not find them
  until then: rewrite them through the broker.

**Upgrading to 0.6.9 — JSON string columns carry JSON text (breaking for callers).**
In Go, TypeScript and Python, a string field on an `is_json`, `is_jsonb` or `JSON`
column reads and writes the column's JSON text: a JSON string reads as `"My"` with
its quotes, and writing `"My"` stores that JSON string. Remove any code that added
or stripped a layer of encoding around such fields; it would now store a string
containing the quotes. The broker also shares one Redis connection per endpoint,
which ends the minute-long `Unavailable` bursts seen under load.

**Upgrading to 0.6.10 and 0.6.11** — no caller changes. 0.6.10 makes the SDK test
fakes hold JSON columns as the broker does. 0.6.11 makes `udb verify --live` and
`udb up` accept `CHECK` rules re-created from a `pg_dump`.

**Upgrading to 0.6.12 — reviewed-transition receipts survive a migrating start.**
Earlier, a migrating start after a reviewed catalog transition cleared the
transition's receipts and every later start failed with "native
target/application receipt is incomplete or mismatched". After upgrading, rebuild
them with `udb catalog restore-receipts --run RUN_ID` (see
[operations.md](operations.md#restoring-cleared-transition-receipts)).
`udb verify --live` now also names the cause when no CDC tailer drains the outbox.

**Upgrading to 0.6.14 — reviewed transitions follow the database, not its address.**

- The first 0.6.14 start moves each reviewed catalog transition bound by an earlier
  release (to the server's address) onto the database's UDB identity, a row in
  `udb_system.udb_database_identity`, as long as the address still matches. After that,
  a recreated Postgres container, a restore of the same data and a rotated login role
  keep the binding. Start 0.6.14 once against the database at its usual address before
  you move or restore it.
- A copy taken before that first start (a rehearsal clone) still refuses with "routed
  native target authority changed". Run `udb catalog rebind-target --run <id> --actor
  <who> --reason <why>` on the copy, once per applied transition. It is proved by the
  transition's receipts, so it works after later changes moved the schema on (see
  [operations.md](operations.md#moving-a-transition-to-a-restored-or-relocated-database)).
- Breaking for callers: API-key `Authenticate` refuses a tenant hint naming another
  tenant (INVALID_ARGUMENT on `tenant_hint`; send no hint or the key's own tenant), and
  SDK retries return at once when the broker's `retry_after_ms` exceeds the client's
  limit (default 10 s). A write that carries two different rows, one in `payload` and
  one in `record_json`, is refused. `UDB_ABAC_DEFAULT_ALLOW=true` now takes effect on a
  development broker (it never applied before): unset it if your tests expect
  default-deny before their policies exist.

**Upgrading to 0.6.13 — three settings now do what they say (breaking for callers).**

- `UDB_MIGRATE_ENABLED=false` holds the project's schema on start. The broker never
  read it before. Now a start with it off applies no change to the project's own
  schemas unless the approved plan in `UDB_MIGRATION_APPROVAL_PLAN` covers it, and
  refuses to start while one is pending ("UDB_MIGRATE_ENABLED=false and the protos
  change the project's schema"). UDB's own system and native-service tables still
  migrate. Fix: review with `udb plan`, approve it and set
  `UDB_MIGRATION_APPROVAL_PLAN`, or set `UDB_MIGRATE_ENABLED=true` for the start that
  should apply it.
- `UDB_CDC_ENABLED=false` skips UDB's own side-effect events (native-service and
  auth events as well as data mutations), so nothing piles up in an undrained
  outbox. An event a caller enqueues explicitly (`EnqueueOutboxEvent`, a
  transaction's enqueue step, DLQ replay) is still stored and delivered once CDC
  runs.
- `udb catalog rebind-target --run RUN_ID --actor WHO --reason WHY` moves a reviewed
  transition to a restored or relocated database, which previously refused to start
  with "routed native target authority changed" (see
  [operations.md](operations.md#moving-a-transition-to-a-restored-or-relocated-database)).

## Related

- [operations.md](operations.md) — production readiness, runbooks, SLOs
- [enterprise-deployment.md](enterprise-deployment.md) — hardened bring-up from scratch
- `udb env --profile enterprise` — generate a working environment file
- `udb requirements` — the backend contract this manifest declares
- `udb doctor --enterprise` — configuration and connectivity preflight
