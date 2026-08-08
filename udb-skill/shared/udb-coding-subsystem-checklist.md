# UDB 0.5.0 — subsystem search & uncover checklist

Historical coverage tracker for the edge-to-edge audit behind the 0.5.0 "first usable UDB"
release. One row per subsystem. The point is COVERAGE accountability: no area ships
un-swept, and every sweep is first-principles (derive the invariant, attack the code),
NOT known-pattern recurrence — that was the gap the maintainer caught.

Legend: `[x]` swept + findings recorded · `[~]` sweep in flight · `[ ]` not yet scanned
Findings detail lives in `stable_udb_438_todos.md`; fix status in its IMPLEMENTATION LOG.

---

## 0. EXHAUSTIVE SUBSYSTEM INVENTORY (every subsystem, with status)
Status: ✅ swept · ✅✅ swept+adversarially-verified · 🔄 in-flight (gap wave 4) · ⚠️ partial
(touched, needs dedicated) · ⬜ deferred-with-reason (never silently omitted).

### A. Native / control-plane services
| # | Service | Status | Where |
|---|---------|--------|-------|
| A1 | DataBroker (read/write/batch/tx/stream) | ✅✅ | F1-F10, wire-codec, batch/stream S17, B-TX1 verified |
| A2 | AuthN (login/MFA/session/recovery) | ✅✅ | cluster A, S24, verify af4b454b (VLT1/NF-1) |
| A3 | AuthZ (Casbin/ABAC) | ✅✅ | cluster A, config CFG-D2, verify a1ac6d88 |
| A4 | Tenant lifecycle | ✅✅ | S25, verify a1ac6d88 (TEN1) |
| A5 | API key | ✅ | cluster A (A4), niche N2, apikey verify af4b454b |
| A6 | IdP / SCIM / SAML / OIDC federation | 🔄 | S38 (a723963b) — was ⚠️ (ingress SCIM I1 + prior SAML XSW) |
| A7 | Storage (finalize/immutable-CAS) | ✅ | 0.4.37 finalize |
| A8 | Asset pipeline (ingest/EMBED/GC/presign) | 🔄 | S31 (a79c3d59) |
| A9 | Backup / Restore | ✅✅ | S16, verify a67e360a |
| A10 | Vault (secret/transit/dynamic/KEK) | ✅✅ | S24, verify af4b454b |
| A11 | Metering | ✅ | S18 |
| A12 | Scheduler | ✅ | S18 |
| A13 | Rate-limit / admission (channels) | ✅ | S18 (RL1-4) |
| A14 | Analytics | 🔄 | S34 (ae314e6c) |
| A15 | Search / reindex / freshness | 🔄 | S34 |
| A16 | Notification | 🔄 | S35 (a27373c6) — was ⚠️ tracing-only |
| A17 | Webhook delivery | 🔄 | S35 |
| A18 | Workflow engine | 🔄 | S36 (a0feec5e) |
| A19 | Embedding | 🔄 | S36 (+ D16 queue fix done) |
| A20 | Distributed lock | 🔄 | S36 |
| A21 | Cache | 🔄 | S36 |
| A22 | WebRTC / media / SFU / TURN / signalling | ✅ | S20 |
| A23 | LiveQuery | ✅✅ | S12 + S17, CDC1 verified |
| A24 | Evidence / compliance export | ✅ | closer ae70dacc — 2 HIGH (empty/overstated-tamper-evident); DSAR/GDPR absent |
| A25 | Protocol Gateway (HL7/FHIR/OPC-UA) | ⬜✓ | CONFIRMED non-existent (addb048f): no proto/src/git — planned TODO only, not a gap, not even a capability lie |

### B. Cross-cutting subsystems
| # | Subsystem | Status | Where |
|---|-----------|--------|-------|
| B1 | IR compiler (compile_read/write/aggregate per backend) | ✅✅ | F-series, wire-codec deep, projection verify |
| B2 | Executors (18 backends) | ✅ | wire-codec deep (a896c10e) |
| B3 | Canonical store (per backend) | ✅✅ | projection S15+deep+verify, wire-codec |
| B4 | CDC / outbox / events | ✅✅ | S12 + deep + verify (a51fecb9) |
| B5 | XA / 2PC / saga recovery | ✅✅ | S13 + deep + verify (ae546ff9) |
| B6 | Migration / DDL diff / apply | ✅✅ | S14 + verify (a67e360a) |
| B7 | Projection / read-fence / drift | ✅✅ | S15 + deep + verify (a2554958) |
| B8 | Method-security tower layer | ✅✅ | cluster A, B-TX1 verified |
| B9 | Config / env parsing | ✅✅ | S05 + deep + verify (a1ac6d88); config-CRLF REFUTED |
| B10 | TLS / mTLS transport + cert rotation | ✅ | S23 |
| B11 | Descriptor / manifest / build.rs / GOLDEN | 🔄 | S32 (aeace801) |
| B12 | Public listener / gRPC reflection / health | 🔄 | S32 + S37 |
| B13 | SDK generation pipeline (buf/postproc/provenance) | 🔄 | S33 (accfca04) |
| B14 | SDK emitters (Go + Py/TS/PHP/C#/Java) | ✅ | S07, S19 |
| B15 | CLI | ✅ | S08 |
| B16 | Metrics / observability | ✅ | S18 |
| B17 | Tracing / log-PII / error posture | ✅ | S26 |
| B18 | Audit sink (durable PG) | ✅✅ | CFG-D3 verified, tracing |
| B19 | Kafka producer / consumer / txn | ✅✅ | CDC deep + verify |
| B20 | Object stores (S3/GCS/Azure) isolation | 🔄 | S37 (a92c071) + wire-codec value-encoding |
| B21 | Idempotency keystone | ✅ | closer a637201f — core SOLID; E1+E2 confirmed open (reorder+request-hash) |
| B22 | Preflight / doctor | ✅ | S41 — 0 CRIT; doctor/serve divergence (reporting fail-open) |
| B23 | Health / SLO / readiness | ✅ | S41 — 0 CRIT; serving fail-closed, reporting fail-open |
| B24 | Parser / AST / escaping / injection | 🔄 | S39 (a84628cf) — escaping/injection; broader parser historically covered |
| B25 | WASM / portable | ✅ | closer a60561ec — NO cfg-drift (confirmed), parity intact; 1 MED (parser recursion DoS) |
| B26 | Deploy / provision CLI | ⬜ | NOT BUILT — 0.5.0 build TARGET, not an audit target |
| B27 | gRPC↔REST transcoding gateway / OpenAPI | 🔄 | S40 (a807efa0) — REST auth parity |
| B28 | CI/CD workflows + release pipeline | ✅ | S42 — 0 CRIT; REL-001 core SOUND; F1 HIGH unpinned actions |
| B29 | Connection pool / DSN / instance routing / creds | 🔄 | S43 (aa2b73a2) — confused-deputy, cred isolation |
| B30 | Kafka consumer-group / offset / rebalance | ✅ | S44 — 0 CRIT; A1 HIGH commit-drops-failed (at-most-once) |
| B31 | Native-service registry / method routing | ✅ | S44 — routing VERIFIED fail-closed; B2 LOW drift |
| B32 | RLS policy generation + tenant-GUC install | 🔄 | S45 (aa83ffaa) — the isolation FOUNDATION (FORCE RLS, GUC everywhere) |

**Coverage math (updated):** 57 subsystems now enumerated (added B27-B32 + the closers). ~44 ✅
swept (waves 1-4 + closers), 8 🔄 in the final search wave (S40-S45 = B22/B23/B27-B32), 1 ⬜✓
confirmed non-existent (Protocol Gateway), 1 ⬜ deferred-with-reason (Deploy CLI = build target),
plus a few ⚠️ fix-pending (idempotency E1/E2, preflight, health — issues FOUND, on the fix list, not
discovery gaps). After S40-S45 land, every subsystem that exists is swept. ADVERSARIAL VERIFICATION
of wave-4 CRIT/HIGH is running now (5 clusters, §2e below).

## 1. COMPLETED SWEEPS

| # | Subsystem | Scope (files/dirs) | Audit id | Headline findings |
|---|-----------|--------------------|----------|-------------------|
| S01 | Auth binding (authn/authz) | authn/mfa.rs, lifecycle.rs, apikey.rs, authz/governance_* | cluster A | ~14 target-user / caller-tenant / body-tenant binding holes — FIXED |
| S02 | Read-path tenant scoping (IR read) | ir/compile/{pg,mysql,sqlite,mssql,es,qdrant}.rs, setup_data vector | F1–F10 | vector hybrid leg leak (F1/F2), PG read pred (F6), ES _id (F3), belongs_to non-PG (F5), F10 enforce-scope flag |
| S03 | Storage finalize integrity | storage_service/{config,errors,handlers}.rs | (report) | immutable-CAS mismatch + trusted HEAD size — FIXED (0.4.37) |
| S04 | Idempotency keystone | setup_data.rs claim/persist/idempotency_claim_sql | (report) | input-binding + replay-before-CAS — E1–E5 PENDING |
| S05 | Config / env parsing | config/mod.rs parse_bool_env_value, security.rs, is_production | ad7d947 | **CRIT** UDB_ENV CRLF disables prod posture (Win) + 4 HIGH fail-opens |
| S06 | Wire codec (read serializers + binders) | core/mod.rs, executor_utils.rs, mssql.rs, cassandra.rs, redis.rs, postgres_helpers.rs, sql_ddl.rs | a29305 | **5 CRIT** float→NULL, arrays→[], MySQL/MSSQL temporal+uuid→NULL, Cassandra typed→NULL (W1–W17) |
| S07 | Go SDK emitter type-matrix | cli/sdk_gen.rs, bind_one, row_value_to_json, naming.rs | (type-matrix) | T1–T16: scalar/bytes/repeated/enum/decode-fail-open |
| S08 | CLI surface | src/cli/** | (cli audit) | 12: broken-pipe panic, output fidelity, exit codes |
| S09 | Meta / audit-coverage / non-PG DDL | cross-cutting | (meta audit) | 15: M5 non-PG UNIQUE drop, M9 bytes DDL, audit-coverage matrix |
| S10 | Ingress edge | SCIM, /metrics, WS signalling | (ingress) | 3 MED: SCIM token binding, metrics unauth+tenant-leak, WS fail-open |
| S11 | Niche / crypto / DLQ | tail_source, apikey limiter, vault | (niche) | HIGH tail_source PII→DLQ; apikey limiter TOCTOU; vault orphan role |

## 1b. COMPLETED SWEEPS — wave 2 (returned 2026-08-05); findings in stable_udb_438_todos.md

| # | Subsystem | Agent | Headline findings |
|---|-----------|-------|-------------------|
| S12 | CDC / outbox / event streaming | a3ea8fab | 2 HIGH: LiveQuery deltas dead on non-leader replicas (HA lie); AtLeastOnce journal never acked → unbounded growth. +CDC3 rnd-partition delete, CDC4 outbox tenant-spoof, CDC5 anchor oracle |
| S13 | XA / 2PC / saga recovery | a05c8390 | 1 CRIT: MySQL prepared-txn leak (no presumed-abort sweep). 1 HIGH: 2PC orphans Qdrant/S3 side-effects. +XA3-5. SQL 2PC core sound |
| S14 | Migration / DDL diff / apply | af25608 | 1 HIGH: audit-field-number shift wedges startup. +DropUnique no-op, non-PG no ALTER path, PG defaults verbatim, apply no-lock, tenant-col backfill leak |
| S15 | Projection / canonical-store | af454dee | 3 HIGH: retry-reorder stale-revert; orphaned IN_PROGRESS unrecovered (default); read-fence clears on FAILED. +PROJ4-9 (tenant PK collision, LSN pre-commit) |
| S16 | Backup / restore / export | a606ccf4 | 1 HIGH: restore NULLs cols on schema drift (no fingerprint). 1 MED-HIGH: non-canonical UUID defeats fresh-target guard. +BK3-9 |
| S17 | Batch / streaming RPC authz | aa202ad0 | 1 HIGH: BeginTx skips per-table ABAC (wildcard "*" gate). +PublishCDC wildcard, BeginTx unbounded buffer. Side-effect parity CLOSED |
| S18 | Metering / scheduler / rate-limit / admission | ad2dbd3 | 0 HIGH — 3 named classes VERIFIED CLOSED. RL1 MED fairness-bucket bypass; RL2 metering-rollup RLS sibling; RL3/RL4 LOW |

## 1c. COMPLETED SWEEPS — wave 3 (returned 2026-08-05)

| # | Subsystem | Agent | Headline findings |
|---|-----------|-------|-------------------|
| S23 | TLS / mTLS transport | a14fcca9 | 0 HIGH — core mTLS CORRECT. TLS1 silent plaintext downgrade, TLS2 no hot cert/CA rotation, TLS3 inert mTLS knobs, TLS4 shared CA |
| S24 | Secret / vault / key mgmt | a1a7545b | **1 CRIT: TOTP MFA seed under SHA256("") → MFA bypass.** 1 HIGH: startup silently disables encryption. +AAD/rotation/zeroize |
| S26 | Tracing/log PII + error posture | a7392db9 | 0 CRIT/HIGH — posture strong. LOG1 MED raw DB msg bypasses verbose gate on auth store; LOG2 LOW |
| S20 | Media / WebRTC / transcode | a4991e5c | 2 HIGH: ffmpeg no protocol-whitelist (SSRF/LFI); peer-listener scope forces admin creds to browsers. +peer impersonation, ws TURN |
| S25 | Tenant service lifecycle | a5c3ab30 | 1 HIGH: SUSPEND non-durable (node-local, no login block). +reactivation trap, code not normalized, default self-purge |

## 1d. DEEP SECOND PASSES — returned (2026-08-05). The revisit PAID OFF: 2 NEW CRITs pass-1 missed.
| Subsystem (pass-1 yield) | Agent | New headline |
|--------------------------|-------|--------------|
| Config/security (1 CRIT) | a5e2fc6b | **NEW CRIT** is_production()=tls&&svc-id decoupled from UDB_ENV → all fail-closed paths OPEN on clean env. +2 HIGH (ABAC init allow-all, audit-sink trim) |
| Projection (3 HIGH) | aeb5dd76 | **NEW CRIT** read-fence VACUOUS (task_id vs idempotency_key) → RYW dead on all projections. +2 HIGH (drift false-pos, A→B→A dedup) |
| XA/2PC (1 CRIT) | a71225f8 | **REFUTES pass-1 "atomicity holds"**: HIGH torn-commit (grace floor 0 + cross-broker xid). +in_doubt status overload |
| CDC (2 HIGH) | aed1d22e | HIGH KafkaTransactional DLQ outside txn → poison-loop. +unredacted DLQ PII, replay decrypt-to-plaintext |
| Wire-codec (5 CRIT) | a896c10e | **NEW CRIT** ClickHouse `\`-escape SQL-injection. +7 HIGH (Redis/Qdrant key collision, Neo4j map-500, Mongo BSON, CH array/uint DDL, ES no-mapping tenant-empty). 37 new cells, 2 pass-1 non-findings refuted |

## 2. IN-FLIGHT SWEEPS

### 2c. ADVERSARIAL VERIFICATION ROUND (launched 2026-08-05) — skeptic-prior refute-or-confirm
Each agent verifies a cluster of prior CRIT/HIGH claims: CONFIRMED (failure trace) / REFUTED
(the guard) / PARTIAL (true scope), + flags NEW issues (3rd revisit). Verdicts → todos.
| Cluster | Claims under test | Agent |
|---------|-------------------|-------|
| Config/security + tenant | CFG-D1, config-CRLF, CFG-D2, CFG-D3, TEN1 | a1ac6d88 |
| Projection | P2-1, PROJ1, PROJ2, P2-3 | a2554958 |
| XA/2PC | XA1, XA-D1, XA2, XA-D2 | ae546ff9 |
| Auth/vault/MFA | VLT1, VLT2, B-TX1 | af4b454b |
| CDC + media | CDC1, CDC2, CDC-D1, MED-A1, MED-A2 | a51fecb9 |
| Backup + migration | BK1, BK2, MIG1, MIG2, MIG8 | a67e360a |

**Verification round COMPLETE (all 6 clusters back).** Verdicts in todos. Net: config-CRLF CRIT
REFUTED (false positive); VLT2/MIG8/CFG-D2 → PARTIAL; MED-A1 SSRF re-scoped latent; everything else
CONFIRMED; ~19 NEW findings surfaced by the skeptics. No CRIT other than config-CRLF fell.

### 2d. GAP-CLOSING WAVE 4 (launched 2026-08-05) — closes every un-swept subsystem in §0
| # | Subsystem | Agent |
|---|-----------|-------|
| S31 | Asset pipeline | a79c3d59 |
| S32 | Descriptor / build.rs / GOLDEN / reflection | aeace801 |
| S33 | SDK generation pipeline | accfca04 |
| S34 | Analytics + search/reindex | ae314e6c |
| S35 | Notification + webhook | a27373c6 |
| S36 | Workflow / embedding / lock / cache | a0feec5e |
| S37 | Object stores + public listener | a92c071 |
| S38 | IdP / SCIM / SAML / OIDC federation | a723963b |
| S39 | Parser / AST / escaping / injection | a84628cf |

**NEXT (committed):** after wave 4 lands, run an adversarial VERIFICATION turn over its CRIT/HIGH
findings (skeptic-prior, same as the round just completed), then the discovery phase is closed and
implementation proceeds on the verified punch-list.

Wave 3 (S19–S26) complete. All 5 deep passes back (added 3 CRIT: config is_production, projection
fence, ClickHouse injection — of which config-is_production stands, projection+CH confirmed).

Backlog remaining (optional): S21 asset pipeline, S22 Kafka consumer-group, S27 descriptor/GOLDEN,
S28 object-stores/Mongo, S29 SDK pipeline internals, S30 public listener.

Backlog remaining (optional next waves): S21 asset pipeline, S22 Kafka consumer-group,
S27 descriptor/GOLDEN, S28 object-stores/Mongo, S29 SDK pipeline internals, S30 public listener.

## 3. NOT-YET-SCANNED BACKLOG (next waves, prioritized)

High value / high blast radius first. Pick the next fan-out from the top.

- [ ] **S19 — Non-Go SDK emitters (Python/TS/PHP/C#/Java)** — sdk-templates/<lang>/**. Parallel
      type-matrix + facade-parity risk to the Go emitter (T1–T16 class). Prior note: TS camelCase
      Struct bug, Python dual RequestContext, PHP grpc-OOM already seen → emitters likely share defects.
- [ ] **S20 — Media services** — webrtc/SFU, vendored ffmpeg transcode, TURN, ws-signalling.
      3-listener topology (50051/50061/50071). Tenant scope on peer sessions, resource/GC on streams.
- [ ] **S21 — Asset pipeline (beyond D4)** — asset_service ingestion → EMBED → Qdrant, quota, GC,
      presigned URLs, orphan bytes vs metadata.
- [ ] **S22 — Kafka subscriber / consumer-group / offset mgmt** — offset commit correctness, at-least-once
      vs at-most-once, rebalance, poison-message handling (may overlap S12 — dedupe after S12 returns).
- [ ] **S23 — TLS / mTLS transport + cert rotation** — listener TLS config, cert reload, CA validation,
      the mandatory-mTLS production force-set path, fail-closed on bad cert.
- [ ] **S24 — Secret / vault management (beyond N3)** — key lifecycle, rotation, envelope encryption,
      plaintext-at-rest paths, DEV_MODE plaintext vault (corroborate S05 finding).
- [ ] **S25 — Tenant service lifecycle** — ensure_tenant resolve-or-create races, default-tenant fixed
      UUID handling, tenant delete/purge, cross-tenant admin capability (report gates #12/#41).
- [ ] **S26 — Observability: tracing / structured logs PII** — beyond metrics (S10/S18): span attributes,
      log fields, error detail posture leaking row data / secrets / tenant ids.
- [ ] **S27 — Descriptor manifest / build.rs / GOLDEN** — fail-closed decode, embedded FDS integrity,
      GOLDEN snapshot drift, reflection surface on public listener.
- [ ] **S28 — Object stores (S3/GCS/Azure) + Mongo Extended-JSON** — opacity (flagged INFO), Mongo
      Extended-JSON wrapper round-trip (W17), cloud-store auth/tenant prefixing.
- [ ] **S29 — SDK pipeline internals** — buf stub gen, openapi-postprocess, sdk-codegen-postprocess
      (CRLF→LF), plugin pinning, provenance manifest (report gate #4).
- [ ] **S30 — Health / reflection public listener** — what the public 50051 listener exposes; capability
      matrix vs served reality on the public surface (may fold into S18).

## 4. HOW TO USE
- Before cutting 0.5.0, every `[ ]` above is either swept (`[x]`) or explicitly deferred with a
  written reason here. Silent omission = the exact failure mode the maintainer flagged.
- When an in-flight sweep returns: move its row S12–S18 into §1, record findings in
  `stable_udb_438_todos.md`, and add any newly-revealed sub-area to §3.
- Next-wave picker: launch 5–7 agents from the top of §3, same first-principles + read-only mandate.
