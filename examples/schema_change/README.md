# Changing a model that already has data

Every other example in this folder starts from an empty database. This one
starts from a table with rows in it, because that is where the care is needed
and where the commands stop being optional.

Nothing here is hypothetical — it is the loop you repeat for the life of a
project.

---

## The rule

**Never point a broker at a changed model and hope.** The broker applies DDL
before it verifies the result, so "start it and see" can leave you with a
changed schema *and* a broker that will not serve.

Three commands remove the guesswork, and all three are read-only:

| Command | Answers |
|---|---|
| `udb verify --live` | Does the database still match what the protos claim? |
| `udb drift --prior` | What will this change actually do? |
| `udb plan --emit-approval-plan` | Produce the file the broker will accept |

---

## Setup: a table with data in it

```proto
// proto/shop/v1/orders.proto
message Order {
  option (udb.core.common.v1.pg_table) = {
    table_name: "orders"
    schema_name: "shop"
  };

  string order_id = 1 [(udb.core.common.v1.pg_column) = {
    column_name: "order_id" sql_type: "UUID" primary_key: true
  }];
  string tenant_id = 2 [(udb.core.common.v1.pg_column) = {
    column_name: "tenant_id" sql_type: "UUID" tenant_column: true not_null: true
  }];
  string status = 3 [(udb.core.common.v1.pg_column) = {
    column_name: "status" sql_type: "VARCHAR(32)" not_null: true
  }];
}
```

The field is `primary_key`, not `is_primary` — the annotation names are defined
in `proto/udb/core/common/v1/db.proto`, which is the file `udb proto export`
copies into your project.

Start it once and insert some rows, so the rest of this is real:

```bash
udb serve proto "" 0.0.0.0:50051
```

**Save the manifest before you change anything.** This is the single step people
skip, and without it `drift` has nothing to compare against:

```bash
UDB_MANIFEST_EXPORT_PATH=prior-manifest.json udb manifest-export proto
```

Use **`manifest-export`**, not `udb catalog`. They are different files and the
mistake is expensive:

- `udb catalog` prints the parsed proto catalog (`{"schemas": [...]}`).
  `--prior` expects a `CatalogManifest` (`checksum_sha256`, `tables`, ...), so
  it cannot use that file. Measured on this repository with **no changes at
  all**, the wrong file produced:

  ```text
  warning: could not parse prior manifest '...': missing field `checksum_sha256` — running without diff
  drift: 100 auto-safe, 0 requires-review, 0 blocked      "has_drift": true
  ```

  versus the correct file:

  ```text
  loaded prior manifest from prior-manifest.json
  drift (with prior manifest): 0 auto-safe, 0 requires-review, 0 blocked   "has_drift": false
  ```

  With no prior, every table diffs as new. From 0.5.16 this is a hard error
  instead of a warning, because the warning went to stderr while the report said
  100 changes — a pipeline reading the exit code or the JSON never saw it.
- `manifest-export` writes the same ledger-shaped manifest the broker stores,
  including the embedded `udb_*` native schemas. An app-only manifest is missing
  those tables, so the next diff reports them as **removed** and proposes
  `DropTable` on tables you never touched.

Note it writes to a *path* (`UDB_MANIFEST_EXPORT_PATH`, default
`udb_catalog_manifest.json`), not to stdout.

Do this in CI on every deploy and keep the artifact. It is small, and it is the
difference between a reviewable diff and a guess.

---

## 1. Make the change

Add a nullable column — the safest possible schema change:

```proto
  string customer_note = 4 [(udb.core.common.v1.pg_column) = { column_name: "customer_note" sql_type: "TEXT" }];
```

## 2. Check the database still matches what you think

```bash
udb verify --live --dsn "$UDB_PG_DSN"
```

This compares your protos against the **live database**, not against a previous
manifest — so it catches divergence nobody recorded: a column someone made
`NOT NULL` by hand during an incident, an index added directly in psql. That
class is invisible to every offline tool and, before this command existed,
surfaced only when a startup failed halfway through a migration.

Exit code `0` means clean, `1` means findings. Run it before every change.

## 3. See what the change will do

```bash
udb drift --prior prior-manifest.json
```

```text
drift (with prior manifest): 1 auto-safe, 0 requires-review, 0 blocked
```

Every operation is classified, and each carries an explicit flag:

```json
{ "kind": "AddColumn", "safety": "SafeAuto", "data_destructive": false }
```

**Gate your pipeline on `data_destructive`, never on the operation name.** A rule
like "reject anything starting with `Drop`" looks careful and is not: UDB
reissues `DropIndex` and `DropPolicy` on ordinary version upgrades, each paired
with a matching `Create`, and `DropNotNull` only ever widens what a column
accepts. A team that wrote that rule found every UDB upgrade impossible until
they rewrote it.

`data_destructive` is true only where rows or their values can be lost.

## 4. Produce the approval plan

```bash
udb plan --prior prior-manifest.json --emit-approval-plan plan.json
```

This writes the exact file `serve` accepts, including an `operations_hash` that
binds it to this one change set. If the plan and the computed change disagree at
startup, the broker refuses **before** touching a row and tells you why. That
disagreement is the gate doing its job — regenerate and review again.

## 5. Rehearse on a clone

```bash
createdb orders_rehearsal
pg_restore -d orders_rehearsal production.dump
udb verify --live --dsn "postgresql://localhost/orders_rehearsal"
```

The rehearsal is where you find the interaction between *your data* and the
change. A clone costs minutes; a failed production migration costs the evening.

## 6. Apply

```bash
UDB_MIGRATE_ENABLED=true
# config: migration.require_approval_plan=/path/to/plan.json
udb serve proto "" 0.0.0.0:50051
```

Then save the new manifest as the prior for next time:

```bash
UDB_MANIFEST_EXPORT_PATH=prior-manifest.json udb manifest-export proto
```

---

## Changes that need more thought

| Change | Classified | Why |
|---|---|---|
| Add a nullable column | `SafeAuto` | Nothing existing breaks |
| Add a foreign key, `CHECK`, or `UNIQUE` | `SafeAuto` | Adding a constraint is not destructive |
| Add a `NOT NULL` column with `default_value` or `backfill_sql` | `SafeAuto` | Existing rows get a value |
| Add a `NOT NULL` column **without** either | `RequiresReview` | Existing rows would have no value |
| Drop a column/index/FK **without** `allow_drop` | `RequiresReview` | Deliberate friction — say so in the proto |
| Drop a column | `data_destructive: true` | The values are gone |
| Change a column's type | `data_destructive: true` | An in-place rewrite can be lossy |
| Reuse a field number marked `reserved` | `Blocked` | Breaks protobuf compatibility outright |

### The two escape hatches people look for

**"How do I add a `NOT NULL` column to a table that already has rows?"** Give it
a value for the existing rows — either is enough to make it `SafeAuto`:

```proto
  string region = 5 [(udb.core.common.v1.pg_column) = {
    column_name: "region" sql_type: "VARCHAR(16)" not_null: true default_value: "'unknown'"
  }];
```

Use `backfill_sql` instead when the value has to be computed from other columns.

**"How do I drop something without it being flagged?"** Mark the intent in the
proto with `allow_drop: true` on the column (or the table). Dropping without it
stays `RequiresReview` on purpose — the friction is the point, because the
alternative is a plan that quietly removes data.

`Blocked` is different from both: it cannot be applied even with an approved
plan. It means the change is not expressible as a safe migration and the proto
is what needs adjusting — pick a fresh field number rather than a `reserved` one.

---

## When a startup fails after the DDL applied

It can happen: DDL applies, verification then fails. The broker deliberately does
**not** record the new manifest in that case, so restarting repeats the same
verification instead of quietly converging on a schema nobody approved.

Every finding is logged with its schema, table and column. Read those first —
they say exactly what disagrees. Then either reconcile, or use the documented
repair path for findings you have already reviewed:

```bash
UDB_MIGRATION_EMERGENCY_AUTO_ALTER=true
```

---

## Related

- [docs/upgrading.md](../../docs/upgrading.md) — upgrading UDB itself, as opposed to changing your own model
- [`examples/go_enterprise`](../go_enterprise) — connecting an application
- `udb env --profile enterprise` — generate a working environment file
