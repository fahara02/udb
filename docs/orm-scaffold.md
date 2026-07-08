# `udb orm scaffold` — migration → ORM model generation

`udb orm scaffold` generates typed **entity / repository models** for a target
language directly from UDB's embedded proto descriptor set. It closes the loop
between the migration pipeline (which evolves your schema) and the SDK (which
talks to the broker): once your proto is the schema of record, the same
descriptors drive both the applied DDL and the generated models, so they cannot
drift.

> **One generator, not two.** `udb orm scaffold` does **not** ship a second code
> generator. It calls the exact same FSM/template machinery as
> [`udb sdk generate`](#relationship-to-udb-sdk-generate) (`src/cli/sdk_gen.rs`),
> optionally scoped to a single entity. The typed entity/repository wrappers come
> from the `@@UDB_ENTITY_BEGIN … @@UDB_ENTITY_END` blocks in
> `sdk-templates/<lang>/`, rendered from the embedded `FileDescriptorSet`.

## The workflow: plan → apply → scaffold

Your annotated proto is the single source of truth. A typical project goes:

```bash
# 1. Preview the migration plan derived from your proto.
udb plan

# 2. Write migration artifacts (db_ops/migrations) — proto is source of truth.
udb sync-migrations

# 3. Apply the schema to your database.
udb system-ddl | psql "$DATABASE_URL"

# 4. Generate typed ORM models for your app's language.
udb orm scaffold --lang typescript
```

Steps 1–3 are the **existing migration pipeline** (`udb plan` /
`udb sync-migrations` / `udb system-ddl`). Step 4 regenerates the models that
read and write the schema those steps just produced. `udb init-project` prints
these same four steps after scaffolding a new project.

## Usage

```text
udb orm scaffold [--lang <name>|all] [--entity <pkg.Message>]
                 [--templates <dir>] [--out <dir>]
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--lang` | `all` | Target language, or `all` for every template directory. |
| `--entity` | _(all)_ | Scope generation to one entity. Accepts the bare message name (`User`) or its fully-qualified `pkg.Message` name (`myapp.v1.User`). |
| `--templates` | `sdk-templates` | Template root (one subdirectory per language). |
| `--out` | `sdk` | Output root; files are written under `<out>/<lang>/`. |

Examples:

```bash
# Every entity, every language.
udb orm scaffold

# Just the TypeScript models.
udb orm scaffold --lang typescript

# A single entity, by fully-qualified name.
udb orm scaffold --lang go --entity myapp.v1.Invoice
```

If `--entity` matches no annotated entity the command **stops** with an error
rather than emitting an empty/duplicate model set — run `udb sdk manifest` to
see the available message names.

## What gets generated

For each in-scope entity the template's entity block has access to the
descriptor-derived metadata, including:

- `{{ENTITY_MESSAGE_TYPE}}` / `{{ENTITY_TABLE}}` — message name and backing table.
- `{{ENTITY_PRIMARY_KEYS}}` — the descriptor's primary keys (drive Upsert
  `conflict_fields`; **never** hardcoded to `id`).
- `{{ENTITY_TENANT_FIELD}}` / `{{ENTITY_PROJECT_FIELD}}` /
  `{{ENTITY_SOFT_DELETE_FIELD}}` — isolation and lifecycle columns.
- `{{ENTITY_TS_TYPE}}` / `{{ENTITY_GO_TYPE}}` / `{{ENTITY_PY_IMPORT}}` — the
  per-language generated message class.

Because these come from the same descriptors the broker enforces, the generated
models stay aligned with the wire contract and the applied schema.

## Relationship to `udb sdk generate`

`udb orm scaffold` and `udb sdk generate` share one implementation. The ORM
command is `udb sdk generate` with an all-default RPC selector and an optional
single-entity filter. Anything you can express in `sdk-templates/<lang>/` for
SDK generation is therefore equally available to ORM model generation — there is
no parallel template tree or parallel generator to keep in sync.
