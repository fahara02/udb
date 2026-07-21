# UDB v0.4.17 AmbuLife integration report — 2026-07-21

Reporter: AmbuLife backend integration (E:\Projects\ambulife)
Broker artifact: official GitHub release `v0.4.17` (releaseId 357268442,
published 2026-07-21T10:39:17Z), `udb-windows-amd64.exe`
sha256 `a68635ce07ab82f7059adec8d24a9a1827a29172d85c06bc9fe2600b90e241b6`,
manifest checksum verified. Installed via the AmbuLife official-release
installer (no source build).

## UDB-CAT-003 — v0.4.17 broker cannot start: its own embedded authn entities fail manifest validation

Severity: **Release-blocking regression.** `udb serve` is dead on arrival for
every deployment; none of the v0.4.17 auth-plane fixes can be exercised live.

### Reproduction

```
udb.exe serve "E:\Projects\ambulife\proto" "" 127.0.0.1:51051
```

(Any proto root; the failure is in the broker's embedded native catalog, not
the application manifest. AmbuLife env: `UDB_ENV=development`, Postgres 16 +
PostGIS reachable, Redis and MinIO reachable.)

Startup reaches `PROTO_CHECKSUM_LINT` and stops:

```
ERROR udb startup lifecycle error: [ERROR] manifest_validation_error:
  udb_authn.service_account_grants db_table_security tenant_column 'tenant_id'
  does not match a pg_column tenant_column=true field
ERROR udb startup lifecycle error: [ERROR] manifest_validation_error:
  udb_authn.certificate_bindings db_table_security tenant_column 'tenant_id'
  does not match a pg_column tenant_column=true field
udb DataBroker stopped with error: {"run_id":..., "state":"PROTO_CHECKSUM_LINT",
  "completed":false, ...}
```

### Root cause (verified in tag `v0.4.17` source)

The two entities introduced by the 0.4.17 verified-principal work declare
tenant isolation in `db_table_security` but never mark the tenant column on the
`pg_column` annotation, which the broker's own strict manifest validation
requires:

- `proto/udb/core/authn/entity/v1/service_account_grant.proto`
  - line ~59: `db_table_security { tenant_column: "tenant_id" ... }`
  - line ~82: `string tenant_id = 4 [(pg_column) = { column_name: "tenant_id"
    sql_type: "VARCHAR(120)" not_null: true ... }]` — **no
    `tenant_column: true`**
- `proto/udb/core/authn/entity/v1/certificate_binding.proto`
  - line ~53: `db_table_security { tenant_column: "tenant_id" ... }`
  - line ~78: `string tenant_id = 5 [...]` — **no `tenant_column: true`**

Compare `proto/udb/core/authn/entity/v1/user.proto` line ~127, which carries
`tenant_column: true` on its `tenant_id` pg_column and passes the same check.

### Required fix

Add `tenant_column: true` to the `tenant_id` pg_column of both new entities
(and regenerate SDK/native artifacts). While in those files, the JSONB columns
`service_account_grants.approved_scopes_json` and
`certificate_bindings.scope_subset_json` also emit `jsonb_missing_is_jsonb`
lint warnings — same one-line annotation fix class.

A release gate that boots `udb serve` against an empty PostGIS database would
have caught this before publication; 0.4.17's CI validated `cargo check` and
SDK tests but never started the broker.

### Impact on AmbuLife

- AmbuCore and Beacon adopted Go SDK `v0.4.17` successfully: builds green, all
  115 AmbuCore packages and the full Beacon suite pass. The SDK-side fixes are
  real (UDB-GO-006 request-scoped metadata verified with the shipped tests;
  additive grant/binding RPC surface compiles and is now used by
  `udbservicebootstrap` to create typed grants).
- The 0.4.17 **broker** cannot be deployed, so every live retest that 0.4.17
  was supposed to unblock (UDB-AUTH-003/004/005/007 verified-principal
  behavior, UDB-AUTH-008 Storage API keys, UDB-AUTH-009 key listing,
  CAT-001/002 catalog identity) remains blocked at startup.
- AmbuLife rolled the local runtime back to the official v0.4.15 broker (the
  last release whose `serve` starts) and keeps all fail-closed gates; no
  bypass, header-scope override, or source patch was applied to conceal this
  regression.

## Environment note (not a UDB defect)

The 2026-07-20 local runtime's Postgres/Redis/MinIO containers were ephemeral
and vanished with their data; AmbuLife now provisions them via
`infra/udb/docker-compose.local.yml` with named volumes and a PostGIS image
(`dispatch.service_zones` requires the `geography` type).
