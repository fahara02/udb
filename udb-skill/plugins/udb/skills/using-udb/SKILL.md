---
name: using-udb
description: Help a developer USE a running UDB broker — connect a language SDK, authenticate with scopes/credentials, and CRUD proto-defined entities over the gRPC DataBroker API. Use when the user is building an app against UDB, asks about the UDB SDK (TypeScript/Python/Go/Java/C#/PHP), UDB metadata/tenant/scopes/auth, Select/Upsert/Delete, defining UDB entity protos (table/column annotations), or the `udb` CLI (serve, sdk generate, proto export, auth bootstrap).
allowed-tools: Read, Grep, Bash, WebFetch
---

# Using UDB

UDB is a **proto-driven multi-database broker**. Developers declare their data
model as annotated Protocol Buffers; UDB generates the DB schema and serves a
uniform gRPC **DataBroker** API (`Select`/`Upsert`/`Delete`) plus a native
control plane (auth/authz/api-keys/storage). Every request carries **metadata**
(tenant, project, scopes, identity) that UDB enforces. Default broker address in
examples: `localhost:50051`.

**Full reference (read on demand): [references/using-udb.md](references/using-udb.md)** —
per-language SDK install+connect snippets, the metadata header table, CRUD
shapes, the auth/bootstrap flow, proto annotations, and the `udb` CLI.

## Mental model (hold this)
1. **Entities are protos.** A table = a `message` with `(udb.core.common.v1.table)`;
   a column = a field with `(udb.core.common.v1.column)`. The message's
   fully-qualified name (e.g. `shop.v1.Customer`) is the `message_type` for every
   data RPC.
2. **No SQL.** Call `Select`/`Upsert`/`Delete` with a `message_type` + filter/record.
3. **Every call carries metadata** (tenant/project/scopes/identity); wrong/missing
   scopes → broker denies (gRPC error).
4. **Auth is explicit** — a bearer JWT, API key, or session attached as metadata;
   `udb:read`/`udb:write` scopes gate ops.

## Before giving code, establish
- **Language** (TS / Python / Go / Java / C# / PHP) → use that SDK's install + client snippet from the reference.
- **Broker address** (default `localhost:50051`, plaintext in dev).
- **Credential** — do they have a token/API key, or need to bootstrap one?

## Quick reference

**SDK packages:** TS `@udb_plus/sdk` · Python `udb-client` · Go
`github.com/fahara02/udb/sdk/go` · Java `dev.udb:udb-java-client` · C#
`Udb.Client` · PHP `fahara02/udb-laravel`. (Confirm the latest version tag.)

**Construct a client** with metadata: `tenantId`, `projectId`, `userId`,
`scopes` (`["udb:read","udb:write"]`), `serviceIdentity`, and a credential.

**CRUD** (all by `message_type` = proto FQN):
- `Select { message_type, filter, limit }` → RecordSet
- `Upsert { message_type, record, conflict_fields, return_record }`
- `Delete { message_type, filter }`

**Auth / bootstrap:** clients attach `authorization: Bearer <jwt>` or
`x-api-key`. A fresh broker has no users — mint the first offline:
```bash
UDB_PG_DSN="postgres://udb:udb@host:5432/udb?sslmode=disable" \
UDB_PASSWORD_HASH_SECRET="<secret>" \
udb auth bootstrap user --username admin --email admin@x.com \
    --password '<strong>' --tenant acme --project default
```

**CLI:** `udb proto export --out proto` (vendor annotations) · `udb serve proto "" 0.0.0.0:50051` ·
`udb sdk generate --lang <lang>` · `udb doctor` · `udb auth api-key-create`.

## Common failures
- **`PERMISSION_DENIED` / `INVALID_ARGUMENT` on write** → `udb:write` missing from
  scopes, or tenant ≠ credential's tenant.
- **No users / can't log in on a fresh broker** → run `udb auth bootstrap user`.
- **Unknown `message_type`** → use the proto's fully-qualified name; list the
  contract with `udb sdk manifest`.

## Guardrails
- Always include metadata (tenant/project/scopes) in examples — the #1 cause of
  denials. Use the user's `message_type` (proto FQN), never raw table names.
- Don't invent RPCs/fields. When unsure, point to `udb sdk manifest`,
  `udb native list`, the per-language SDK README, and the reference file above.
</content>
