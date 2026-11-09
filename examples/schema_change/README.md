# Adding, removing and renaming fields on a table that has data

Every other example starts from an empty database. This one starts from a table with
rows in it, because that is where a schema change can lose data, and where the steps
stop being optional.

Everything below is run for real by [`walkthrough.sh`](walkthrough.sh): it starts a
broker, writes rows, and checks the database after every step. The outputs quoted here
are the ones it prints. Run it yourself against an empty PostgreSQL database:

```bash
UDB_CLI=/path/to/udb UDB_PG_DSN=postgres://user:pass@localhost:5432/scratch ./walkthrough.sh
```

The proto versions it steps through are in [`proto/`](proto): `v1` → `v2` (add a
column) → `v3-blocked` (delete a field, refused) → `v3` (delete it on purpose) →
`v4` (add a NOT NULL column) → `v5` (rename a column).

---

## The short version

| You want to | Do this in the proto | What the broker does |
|---|---|---|
| Add a nullable column | add the field | `AddColumn`, auto-safe |
| Add a NOT NULL column | add the field with `default_value` (or `backfill_sql`) | auto-safe; existing rows get the value |
| Remove a column | delete the field, `reserved` its number and name, **and** set `allow_drop: true` on the table | `DropColumn`, marked `data_destructive`; applied only with an approved plan if migration is off |
| Remove a column, but forget `allow_drop` | delete the field | **Blocked**: refused even with an approved plan; the data stays |
| Rename a column, keeping its values | rename the field and set `previous_column_name: "<old name>"` on it | `RenameColumn`, auto-safe; values kept |

Afterwards, remove one-off hints (`allow_drop`, `previous_column_name`) in your next
change. The diff warns about a hint left behind.

## The three commands

| Command | Answers | Exit code |
|---|---|---|
| `udb verify --live --dsn <dsn>` | Does the live database match the protos, and is delivery healthy? | `0` clean, `1` any finding |
| `udb drift --prior prior.json` | What will this change do, by count? | `0` no drift, `1` drift found |
| `udb plan --prior prior.json --emit-approval-plan plan.json` | The exact plan the broker will accept | `0` |

`udb drift`, `udb plan` and `udb verify` read the protos under `./proto` in the current directory,
so run them from your project root. `drift` exits `1` whenever it finds a change, so a
script that runs it with `set -e` must allow that.

**Gate your pipeline on the plan file, not on the drift report.** The drift report
holds counts (`auto_safe_count`, `requires_review_count`, `blocked_count`) and one item
per change. The plan file lists each operation with its `kind`, `safety` and
`data_destructive`:

```json
{ "kind": "AddColumn", "schema": "shop", "table": "orders", "column": "customer_note",
  "safety": "SafeAuto", "data_destructive": false }
```

Reject on `"data_destructive": true`, never on the operation name. UDB reissues
`DropIndex` and `DropPolicy` on ordinary upgrades, so a "no `Drop*`" rule would block
every upgrade.

---

## Step by step

### 1. First start: the protos create the table

`proto/v1/orders.proto` declares `shop.orders` with `order_id`, `tenant_id` and
`status`. The first start creates it:

```bash
UDB_MIGRATE_ENABLED=true udb serve proto "" 0.0.0.0:50051
```

### 2. Your application writes rows

The walkthrough inserts two rows directly. Your service would write them through the SDK.

### 3. Save the manifest of what is deployed, before you change anything

```bash
UDB_MANIFEST_EXPORT_PATH=prior.json udb manifest-export proto
```

This is the step people skip, and without it `drift` and `plan` have nothing to compare
against. Use `manifest-export`, not `udb catalog`: `--prior` needs a `CatalogManifest`
(with `checksum_sha256` and `tables`), and it must include UDB's own `udb_*` schemas,
which `manifest-export` writes. Keep this file as a build artifact on every deploy.

### 4. Add a nullable column and see what it will do

Add `customer_note` (`proto/v2`). Then:

```text
$ udb verify --live --dsn "$UDB_PG_DSN"      # exit 1: the database lacks the new column
$ udb drift --prior prior.json
drift (with prior manifest): 1 auto-safe, 0 requires-review, 0 blocked
```

### 5. With `UDB_MIGRATE_ENABLED=false`, a start refuses to apply it

```text
UDB_MIGRATE_ENABLED=false and the protos change the project's schema (1 change(s): AddColumn shop.orders(customer_note)). ...
```

The broker does not start, and the column is not added. This is the production setting
(`udb env --profile enterprise` writes it): a restart never changes your schema on its
own.

### 6. Approve it: emit a plan and start with it

```bash
udb plan --prior prior.json --emit-approval-plan plan.json
UDB_MIGRATE_ENABLED=false UDB_MIGRATION_APPROVAL_PLAN=plan.json udb serve proto "" 0.0.0.0:50051
```

The column is added, the rows are untouched, and `udb verify --live` is clean:

```text
verify --live: OK — 84 manifest table(s) match the live schema.
```

If the plan and the computed change disagree, the broker refuses before touching a row.
Regenerate the plan and review it again. Then save the new manifest as the prior for
next time (step 3).

### 7. Delete a field without saying the data may go: refused

Delete `customer_note` and reserve its number and name (`proto/v3-blocked`):

```text
drift (with prior manifest): 0 auto-safe, 0 requires-review, 1 blocked
DropColumn shop.orders(customer_note) [Blocked: drop_column is destructive; add allow_drop only after explicit review]
```

A Blocked change is refused at start, even with an approved plan. The column and its
values stay.

### 8. Delete it on purpose

Keep the `reserved` lines and add `allow_drop: true` to the table (`proto/v3`). The
change becomes auto-safe, and the plan marks it destructive:

```text
drift (with prior manifest): 1 auto-safe, 0 requires-review, 0 blocked
"kind": "DropColumn", ..., "data_destructive": true
```

With `UDB_MIGRATE_ENABLED=false` a plain start still refuses it. Emit a plan and start
with it, as in step 6, and the column is dropped; the rows are kept.

`allow_drop` can also go on the column itself, but then it must be in the deployed
manifest before you delete the field (two releases). The table-level flag does it in
one.

### 9. Remove the hint, and add a NOT NULL column with a default

Remove `allow_drop` and add `region` with `not_null: true` and
`default_value: "'unknown'"` (`proto/v4`). It is auto-safe, and every existing row gets
`'unknown'`. Without the default it would need review, because existing rows would have
no value. Use `backfill_sql` instead when the value has to be computed from other
columns.

### 10. Rename a column and keep its values

Rename the field and say what it was called (`proto/v5`):

```proto
  string state = 3 [(udb.core.common.v1.pg_column) = {
    column_name: "state" sql_type: "VARCHAR(32)" not_null: true previous_column_name: "status"
  }];
```

Keep the field number. The broker renames `status` to `state` in place and the values
stay. (Before 0.6.14 this failed on a table with rows: the bootstrap SQL added `state`
as a new empty column first.)

Without `previous_column_name`, the same edit reads as "drop `status`, add `state`".
The drop is Blocked, and the error suggests the hint.

---

## Rehearse on a copy first

```bash
createdb orders_rehearsal
pg_restore -d orders_rehearsal production.dump
udb verify --live --dsn "postgresql://localhost/orders_rehearsal"
```

The rehearsal shows how *your data* interacts with the change. A restore of the same
data keeps its reviewed catalog transitions bound. Only a copy taken before its source
ran a release with database identities refuses with "routed native target authority
changed"; see `udb catalog rebind-target --help`.

## Adding a new entity

The broker serves only the entities in the protos it was started with. Add the message,
**restart the broker**, then deploy the services that use it. A service that writes an
entity the broker does not know gets `INVALID_ARGUMENT: unknown message_type ...` on
its first write. Check at boot with `LookupMessageSchema` (one entity) or
`GetCatalogManifest` (all of them), and refuse to start if one is missing.

## Classification reference

| Change | Classified | Why |
|---|---|---|
| Add a nullable column | auto-safe | nothing existing breaks |
| Add a foreign key, `CHECK` or `UNIQUE` | auto-safe | adding a constraint loses no data |
| Add a `NOT NULL` column with `default_value` or `backfill_sql` | auto-safe | existing rows get a value |
| Add a `NOT NULL` column without either | requires review | existing rows would have no value |
| Rename with `previous_column_name` | auto-safe | the column is renamed in place |
| Drop a column/table with `allow_drop` | auto-safe, `data_destructive: true` | the values are gone; approve it |
| Drop a column/table without `allow_drop` | **Blocked** | refused even with a plan |
| Change a column's type | `data_destructive: true` | an in-place rewrite can be lossy |
| Reuse a `reserved` field number | **Blocked** | breaks protobuf compatibility |
