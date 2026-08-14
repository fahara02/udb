# UDB v0.5.7 Vault database-credential lifecycle correction

Date: 2026-08-15
Status: implemented; generated protocol artifacts refreshed; GitHub CI pending

## Changed

- Added replay-safe issuance keyed by verified tenant/project plus a required
  caller idempotency key.
- Stored only master-KEK-wrapped recovery material, marked STORAGE_ONLY and
  redacted by the descriptor contract.
- Made STARTING claim, tenant/project-bound physical authority, ACTIVE state,
  provenance, and issued event one PostgreSQL transaction.
- Added public single-lease revoke and project-scoped emergency revoke-all RPCs.
- Added durable STARTING/ACTIVE/REVOKING/REVOKED/FAILED reconciliation,
  immutable target routing, session termination, generated policy/grant cleanup,
  role-absence proof, and strict transactional revocation evidence.
- Terminal REVOKED commits now shred the KEK-wrapped password recovery envelope
  in the same transaction as the durable state and outbox evidence.
- Emergency revoke-all durably marks every matching non-terminal lease before
  processing a bounded synchronous batch; the leader worker drains all remaining
  REVOKING/FAILED intents over subsequent bounded passes.
- Expanded the live Vault test with response-loss replay, active-session revoke,
  role/session absence, recovery-envelope shredding, durable outbox verification,
  and outbox-failure atomic rollback assertions.

## Compatibility

- The service additions and new request/response fields are wire-additive.
- `GenerateDatabaseCredentials.idempotency_key` is operationally required; old
  callers receive a typed validation error until upgraded.
- Historical rows retain migration defaults, but cannot recover passwords that
  were never persisted as protected envelopes.
- The idempotency uniqueness constraint excludes legacy empty-key rows, allowing
  in-place migration while enforcing uniqueness for every new required key.

## Verification

- No local Cargo, build, or test command was run because the operator required
  CI-only verification.
- `buf generate --include-imports`, `openapi-postprocess.mjs`, and
  `sdk-codegen-postprocess.mjs` refreshed the additive Vault SDK and OpenAPI
  artifacts with the repository-pinned generators. The codebase map was also
  regenerated. These are deterministic source-generation steps, not local
  compilation or test evidence.
- Required live filter:
  `vault_db_credentials_live_enforce_fixed_tenant_and_project_after_guc_change`
  with `UDB_LIVE_AUTH_TESTS=1`, the CI Vault authority JSON, Postgres, and a real
  master KEK.
- Required standard gates: workspace library tests, native integration,
  descriptor compatibility, codebase-map freshness, Buf/OpenAPI/SDK drift, and
  formatting/clippy in `.github/workflows/ci.yml`.
- Combined-head CI `31837471675` correctly blocked the first integration push:
  Rust formatting drift, stale descriptor-derived native/SDK metadata for the
  two additive RPCs, and a non-canonical kebab-case emergency REST action. The
  REST action is now lower-camel-case; formatting is normalized without local
  compilation, and descriptor-derived artifacts remain delegated to the
  replacement CI-built broker before merge.
- The first three combined live lanes (`31837515968`, `31837519876`, and
  `31837522725`) all stopped at the shared lib-test compile preflight because
  the expanded lifecycle assertion lacked its explicit
  `postgres_role_exists` import. The import is restored; none of those failed
  runs reached a live assertion, so all three filters must be rerun.
- Combined CI run `31837950928` also identified generated benchmark-body
  skeleton drift after the new Vault lifecycle surfaces were added. The
  repository generator is rerun in this change so the quick gate can build the
  broker artifact used for descriptor, native-contract, and SDK regeneration.
- Combined CI run `31838223377` confirmed that skeleton generation is fresh,
  then found the benchmark posture guard still pinned to the former
  tenant/role/TTL request. Its expected machine JSON now includes the required
  project and idempotency binding used by the implemented issuance contract.
- Combined CI run `31838390984` reached the generated API-inventory guard and
  exposed the expected bootstrap cycle: the two new HTTP RPCs must exist in the
  native contract before the quick gate will build the broker that emits that
  contract. Descriptor-derived records are seeded from the proto annotations
  solely to unlock that CI build; they must be replaced by the artifact broker's
  authoritative manifest before final validation and merge.
- Bootstrap CI run `31838739099` then confirmed all 293 HTTP operations are
  recognized and requested the corresponding generated Vault benchmark rows.
  The destructive rows use explicit project-bound seeded leases and the
  tenant/project confirmation token; they remain opt-in benchmark fixtures.
- Bootstrap CI run `31838911067` advanced to retry-contract parity and found
  the pre-generation Go metadata still marked issuance non-replay-safe and did
  not contain either revoke RPC. Those three descriptor-derived rows are seeded
  to match the proto contract and will be replaced by full CI-binary SDK output.
- Bootstrap CI run `31839123140` passed retry parity and found the generated
  benchmark manifest still at 379 RPCs. The manifest now carries both revoke
  operations plus the current project/idempotency-bound issuance request, so
  benchmark coverage documentation can be regenerated at 381 RPCs.
