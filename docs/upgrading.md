# Upgrading a live UDB deployment

This page is about upgrading a database that **already has data in it**. A fresh
deployment needs none of this — start the broker and it builds the schema.

The short version:

```bash
udb verify --live --dsn "$UDB_PG_DSN"          # 1. does the live schema match the protos?
udb drift --prior prior-manifest.json          # 2. what will the migration change?
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
findings when it does not, so it can gate a deploy directly.

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
# config: migration.require_approval_plan=/path/to/plan.json
```

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

---

## Related

- [operations.md](operations.md) — production readiness, runbooks, SLOs
- [enterprise-deployment.md](enterprise-deployment.md) — hardened bring-up from scratch
- `udb env --profile enterprise` — generate a working environment file
- `udb requirements` — the backend contract this manifest declares
- `udb doctor --enterprise` — configuration and connectivity preflight
