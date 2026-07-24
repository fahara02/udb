
---

# V22-1 (2026-07-24, found on v0.4.22): JSONB Select→Upsert round-trip asymmetry

The broker returns JSONB columns from Select as **decoded structures**
(`map[string]interface{}` / `[]interface{}`), but its Upsert path binds a
structured value as a non-JSON parameter and fails with
`PostgreSQL upsert failed [SQLSTATE 42846]` (cannot cast) surfaced as opaque
`Internal`. Consequence: any consumer doing the complete-record merge pattern
the data plane itself forces (Select → merge changes → resend full record —
required because partial records 23502 on NOT-NULL columns) breaks on every
JSONB-bearing table.

Repro (live, v0.4.22, entity `ambulife.partner.entity.v1
.PlatformMobileExperienceRelease`): Select row → resend the same row unchanged
via Upsert → 42846. Re-encoding structured values to JSON text before the
Upsert fixes it (our consumer-side mitigation:
`ambucore/microservices/partner/repository/udb_operations.go
normalizeJSONBValues`, applied in the shared upsert helpers).

Asks, in preference order: (1) accept structured JSON values on the write path
(bind as jsonb — the broker emitted them itself); or (2) return JSONB columns
as text so Select output is always re-upsertable; and in all cases (3) map
42846 to a typed InvalidArgument naming the column instead of opaque
Internal. Note this also bit the READ side of our code (a `manifest_json`
decoded via a plain string helper silently became "" and tripped a signed-
release integrity check) — the structured/text ambiguity taxes every consumer
twice.
