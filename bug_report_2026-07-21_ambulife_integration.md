# UDB bugs found by AmbuLife integration

Report date / last updated: **2026-07-21 (Asia/Dhaka)**

Retested release: **official UDB v0.4.15**
(`69a5e8d9d55bd872c778ee0ba8fa72704742ab70`)  
Client: official Go SDK `github.com/fahara02/udb/sdk/go@v0.4.15`
(`sdk/go/v0.4.15`, sum `h1:8Ab6zO3L/rgy+E7/ecufx/CiLrnqpF3p6+wesNJ+lng=`)  
Binary: published `udb-windows-amd64.exe`, freshly downloaded and verified
against both the published binary `.sha256` and checksum-verified release
manifest before execution (99,000,832 bytes; SHA-256
`5cc3c43e266ab9f2f8fdb31738faf6d6a953120cf80cec94082cf13651725287`)

## 2026-07-21 v0.4.15 retest summary

The root `v0.4.15` tag, module-qualified `sdk/go/v0.4.15` tag and repository
`main` all resolve to `69a5e8d9d55bd872c778ee0ba8fa72704742ab70`.
`udb.exe --version` returns `udb 0.4.15`. Read-only official lint against the
current AmbuLife proto root passes **137 tables, 0 errors, 46 warnings and 949
info findings** (descriptor checksum
`55ad0883dbe2541f10792efff9d9b912fb0a161f1f966397056f9c965f4a7e06`).
Official `policy-lint` passes all **196** exact AmbuLife
policies with **0 findings**.

After deleting AmbuLife's compatibility client package and migrating every
AmbuCore/Beacon consumer to raw official v0.4.15 APIs, the complete AmbuCore
`go test ./...` suite passes (115 packages), Beacon `go test ./...` passes, the
official descriptor/policy/consumer contract gate passes, and AmbuPortal passes
lint, TypeScript, 159 tests and its 27-route production build. A subsequent live
broker retest on 2026-07-21 proved tenant-scoped catalogue CRUD through the
official binary and SDK, but exposed UDB-STORAGE-001 and
UDB-DB-READINESS-001 below.

The official v0.4.15 Go SDK no longer contains the private
`gen/google/api` package. After regenerating AmbuLife-owned Go protobufs against
the canonical Google annotations module, the complete AmbuLife contract suite
and the focused partner, marketplace, dispatch, payment and gateway consumer
tests pass with the protobuf registry conflict override unset. This directly
proves **UDB-GEN-001 fixed**. The semantic SDK tag resolves through Go with the
expected origin, which directly proves **UDB-REL-001 fixed**. The new immutable
patch release, manifest and matching digests address **UDB-REL-002 for
v0.4.15**; the v0.4.14 in-place replacement remains a historical provenance
incident.

The SDK now exposes `udbclient.WithExpected(map[string]any)` for atomic generic
Upsert compare-and-swap, so the previously missing UDB-GO-005 client API surface
is present. Broker-side compare-and-swap behavior still needs an independently
deployed official endpoint before AmbuLife can claim the runtime acceptance test
passed. The v0.4.15 release also states that UDB-AUTH-006 is fixed, but AmbuLife
has not started a broker and therefore records that fix as **release-claimed,
runtime retest required**.

The published v0.4.15 Go Entity facade also does not consume the SDK's
request-scoped `udbclient.WithMetadata` values; it binds purpose/correlation to
the long-lived client's immutable `Meta` instead. This new, source-verifiable
finding is recorded as **UDB-GO-006 OPEN** below. AmbuLife removed its outgoing
header workaround and does not mutate shared client metadata.

**Not all historical findings are proven fixed.** UDB-CAT-001/002 and
UDB-AUTH-003/004/005/006/007 require live startup/authentication/negative probes
against an independently deployed official v0.4.15 broker. They remain **RETEST
REQUIRED**. No UDB source was compiled or modified and no broker was started for
this retest.

### Historical v0.4.14 retest record

Latest release/SDK retest: **2026-07-19 (Asia/Dhaka)**. The GitHub v0.4.14
Windows executable and checksum assets were replaced in place after the earlier
retest. AmbuLife discarded the partial first transfer, resumed the same official
asset, and executed it only after the complete file matched both the published
98,946,560-byte size and SHA-256
`4dd8230635f6ae5c7e1fbb986df13fdaeb44e4b9b9209c8f1073e1d0a659b9a0`.
`udb.exe --version` returned
`udb 0.4.14` with exit code 0. Read-only `udb lint --root <AmbuLife proto>` then
passed **133 tables, 0 errors, 53 warnings**, descriptor checksum
`ea720aaa07ab73e7f6f4f0237ed26121d97b95ac3ccebf960152889640eceba7`.
The broker was not started, and UDB source was not compiled or modified.

The root `v0.4.14` tag, `main`, and `HEAD` still all resolve to
`dca600a673ee095cbefdc80aeebb0c1febd49a65`; the release has no body explaining
the artifact replacement. The nested SDK still has no `sdk/go/v0.4.14` tag and
the same immutable release-commit pseudo-version remains selected. A focused
AmbuLife consumer test on that official SDK still panics before test execution
with UDB-GEN-001. Therefore **not all reported defects are fixed**: UDB-REL-001
and UDB-GEN-001 are currently reproducible. Findings that require a running
broker are retained as historical proof but changed to **RETEST REQUIRED** for
the replaced binary rather than being falsely claimed against the new asset.

This file is an integration bug report only. No UDB source file was modified by
AmbuLife.

Current AmbuLife boundary (2026-07-20): no local broker is started and no UDB
source is compiled. Consumer verification uses only the checksum-installed
official executable for read-only descriptor/version checks and the published
Go SDK module. UDB-GEN-001 and UDB-REL-001 are fixed in v0.4.15; the runtime
findings await an independently deployed official endpoint before they can be
revalidated. No application-side registry, RLS, policy or credential workaround
is accepted.

## UDB-REL-002 - v0.4.14 release assets were replaced without a new immutable version or provenance

Status: **RESOLVED FOR v0.4.15 — new version/tag/manifest published; the v0.4.14 incident remains historical**

Severity: **High / breaks reproducible release verification**

The Windows executable and checksum published under the existing v0.4.14
release changed in place while the Git tag and repository `main`/`HEAD` remained
at the same commit. The earlier checksum-verified asset was:

```text
SHA-256 bc2f09b110af5ff4ded0ef1ce81675586bd8a1b51458b4e526bf312ac1868562
```

The current checksum-verified asset is:

```text
size    98946560 bytes
SHA-256 4dd8230635f6ae5c7e1fbb986df13fdaeb44e4b9b9209c8f1073e1d0a659b9a0
asset updated_at 2026-07-19T12:15:32Z
release updated_at 2026-07-19T12:25:41Z
```

Both executables identify themselves only as `udb 0.4.14`. The release body is
empty, and the tag remains
`dca600a673ee095cbefdc80aeebb0c1febd49a65`, so a consumer cannot establish from
the version or source tag which binary was deployed or which reported fixes it
contains. Existing evidence must now distinguish “superseded v0.4.14 asset”
from “current v0.4.14 asset.”

Acceptance: publish fixes as a new immutable patch release (preferred), or at
minimum publish signed build provenance tying each asset digest to its exact
source/workflow inputs and retain a replacement audit log. Do not replace a
released checksum and executable silently under the same semantic version.

## UDB-REL-001 - v0.4.14 release does not publish the Go SDK submodule tag

Status: **FIXED IN v0.4.15 — `sdk/go/v0.4.15` resolves to the release commit**

Severity: **High / prevents a semantic official SDK pin**

The final GitHub release and root repository tag `v0.4.14` both exist at commit
`dca600a673ee095cbefdc80aeebb0c1febd49a65`, and the release workflow reports
success. The Go SDK is a nested module at `github.com/fahara02/udb/sdk/go`, so Go
resolves a semantic release only from a module-qualified tag such as
`sdk/go/v0.4.14`. That tag was not published.

Reproduction on 2026-07-19 (Asia/Dhaka):

```text
> go list -m -json github.com/fahara02/udb/sdk/go@v0.4.14
go: github.com/fahara02/udb/sdk/go@v0.4.14: invalid version: unknown revision sdk/go/v0.4.14
```

`go list -m -versions github.com/fahara02/udb/sdk/go` and the Go module proxy
list only `v0.2.0`, `v0.2.1` and `v0.4.0`. AmbuLife can currently consume the
release commit only as the pseudo-version
`v0.4.1-0.20260719110151-dca600a673ee`. It will not relabel that pseudo-version
as a semantic v0.4.14 SDK release.

Acceptance: publish `sdk/go/v0.4.14` on the same reviewed commit, confirm
`go list -m github.com/fahara02/udb/sdk/go@v0.4.14` succeeds through the public
module proxy, and include the SDK version/tag mapping in the release metadata.

## UDB-CAT-001 - Embedded native schemas collide with legal consumer protobuf names

Status: **RETEST REQUIRED — isolated on the superseded v0.4.14 Windows asset; not yet reproduced on the replaced asset**

Severity: **Critical / prevents broker startup for a valid composed catalog**

AmbuLife defines the canonical `ambulife.authn.entity.v1.User`,
`ambulife.authn.entity.v1.Session` and `ambulife.authn.entity.v1.OTP`, backed by
`authn.users`, `authn.sessions` and `authn.otps`. UDB's
embedded native catalog defines the distinct fully qualified message
`udb.core.authn.entity.v1.OTP`, backed by `udb_authn.otps`. The protobuf package,
fully qualified message name, PostgreSQL schema and physical table are all
different, so these descriptors can legally coexist.

The consumer-only lint command passes the 44-table AmbuLife catalog with zero
errors and zero warnings. `udb serve`, however, composes those 44 schemas with
46 embedded native schemas and exits during `PROTO_CHECKSUM_LINT`:

```text
44 custom schema(s) + 46 UDB-native schema(s)
catalog lint failed with 2 error(s), 3 warning(s)
```

On 2026-07-19, AmbuLife copied the exact same consumer proto root into a
temporary AmbuLife-scoped probe and changed only:

```proto
message OTP      // ambulife.authn.entity.v1.OTP
```

to:

```proto
message AuthnOTP // ambulife.authn.entity.v1.AuthnOTP
```

The package, table annotation, fields, RLS definition and all other 43 schemas
were unchanged. The same checksum-verified official binary then completed
migration and reported:

```text
UDB DataBroker is ready: data=127.0.0.1:51051 auth=127.0.0.1:51061 schemas=44
```

This A/B result demonstrates that combined startup is keying at least one
catalog compatibility path by the unqualified `OTP` name instead of the
protobuf FQN. The probe used official DDL defaults and no UDB build, SDK
replacement, SQL bypass, permissive authorization or source modification.

Acceptance: compose embedded and consumer descriptors by fully qualified
protobuf identity (and physical schema/table identity where applicable). Add a
startup test containing both FQNs above and require broker readiness without
renaming the consumer's valid public type.

## UDB-CAT-002 - Startup catalog lint hides the actual errors and warnings

Status: **RETEST REQUIRED — reproduced on the superseded v0.4.14 Windows asset; not yet reproduced on the replaced asset**

Severity: **High / makes a startup-blocking catalog failure non-actionable**

For UDB-CAT-001, both normal and `RUST_LOG=udb=debug` startup output report only
the aggregate `catalog lint failed with 2 error(s), 3 warning(s)`. The stdout,
stderr and startup FSM JSON contain no issue kind, descriptor identity, source
file, table, field, description or remediation. `udb lint --human` cannot reveal
the problem because it lints only the consumer catalog and passes it.

Acceptance: before exiting, `udb serve` must emit every combined-catalog error
and warning with the same structured identity and remediation fields available
from `udb lint --human`; startup FSM output should retain the structured issue
list as well as the summary count.

## UDB-AUTH-003 - Password service accounts cannot obtain restricted data scopes

Status: **RETEST REQUIRED — reproduced on the superseded v0.4.14 Windows asset; not yet reproduced on the replaced asset**  
Severity: **Critical / blocks least-privilege DataBroker use**

### Expected

An active `ACCOUNT_KIND_SERVICE_ACCOUNT` should be able to authenticate through
the documented Authn password flow and receive an operator-approved/requested
subset such as `udb:read,udb:write` (and service-specific storage scopes), without
receiving `organization_owner`, `udb:admin`, `udb:*` or `*`.

### Actual

Eight distinct AmbuLife service accounts were created through native Authn. They
have zero active role bindings. A password login performed by the official Go SDK
with exact requested scopes returns no matching data scopes. The first fail-closed
verification (`ambucore.authn`, requested
`udb:read,udb:write,udb:pii:read`) failed with:

```text
official UDB service login did not return the exact reviewed non-admin scopes
```

The tagged v0.4.14 implementation still projects token scopes only from
`ROLE_SCOPE_PROJECTIONS`, whose only entry is `organization_owner ->
[udb:admin, udb:*]`. This leaves no native restricted service-account grant path.

### Security impact

The only password-login role known to provide DataBroker scopes is
`organization_owner`, which makes the service a full broker administrator and
bypasses exact entity ABAC matrices. AmbuLife rejected and removed that
workaround. It also removed a local JWT-signing experiment and will not mint UDB
tokens itself.

### Minimal acceptance test

1. Create an active `SERVICE_ACCOUNT` using native Authn.
2. Grant it only `udb:read` and `udb:write` through the supported native API/CLI.
3. Login with the official SDK requesting exactly those scopes.
4. Assert the issued principal has exactly those scopes, no role/admin/wildcard,
   and its canonical tenant/project/service identity.
5. Assert allowed DataBroker CRUD succeeds and a non-allowed entity returns
   `PermissionDenied`.

## UDB-AUTH-004 - Scoped native API keys are explicitly rejected by DataBroker

Status: **RETEST REQUIRED — live reproduction used the superseded v0.4.14 Windows asset; not yet reproduced on the replaced asset**  
Severity: **Critical / blocks the documented scoped service credential path**

### Expected

An API key created by the native `ApiKeyService` with owner type
`SERVICE_ACCOUNT`, an exact tenant/project, and reviewed scopes should work with
the official Go SDK's `udbclient.Credentials{APIKey: ...}` DataBroker path, or
the official Authn API should exchange it for a scope-attenuated short-lived
bearer.

### Actual live proof

On the checksum-verified official v0.4.14 Windows binary, AmbuLife:

1. created eight active service accounts through native Authn;
2. confirmed they have zero role bindings;
3. created eight keys through native `ApiKeyService.CreateApiKey`, each owned by
   the exact service-account principal and containing the exact reviewed scope
   set;
4. authenticated every key through the official SDK's `AuthenticateAPIKey` and
   verified exact principal id, tenant and scopes; and
5. connected DataBroker using the official SDK's `Credentials.APIKey` field.

The first real `Select` failed before ABAC/RLS evaluation with:

```text
API key is not accepted on the DataBroker data plane (it authenticates a JWT
bearer or mTLS only). Log in (username/password -> access_token) and send it as
'authorization: Bearer <jwt>'. (Unauthenticated)
```

`AuthenticateAPIKey` returns no access token, so the SDK exposes a credential
mode that the official DataBroker rejects. The suggested password route remains
blocked by UDB-AUTH-003 because restricted service accounts receive no reviewed
data scopes. AmbuLife did not add an owner role, mint a local bearer, weaken
default-deny, or bypass RLS.

Acceptance: provide either direct, fully supported API-key authentication on
DataBroker or a native API-key-to-short-lived-bearer exchange with tenant,
project, service identity and scope attenuation.

## UDB-AUTH-006 - Service-account API keys lose service identity and requested metadata

Status: **RETEST REQUIRED — v0.4.15 claims the fix; live service-account API-key lineage is not yet independently proven**  
Severity: **High / weakens immutable service-principal lineage**

`CreateApiKeyRequest` accepts `name`, `description`, `owner_type` and `owner_id`,
but the v0.4.14 native implementation stores an `ApiKeyRecord` with an empty
`service_identity`. The returned/read `ApiKey` replaces the requested name with
the generated public key prefix and returns an empty description. Consequently,
`AuthenticateAPIKey` returns the correct owner principal id, tenant and scopes,
but an empty service identity even when the owner is an Authn
`SERVICE_ACCOUNT` whose managed profile contains the service identity.

Acceptance: resolve and persist immutable service identity from the validated
service-account owner (or add a validated request field), preserve requested
non-secret key metadata, and return that lineage consistently from
`AuthenticateAPIKey`. Data-plane authorization must not rely on a freely
client-supplied `x-service-identity` to fill this gap.

## UDB-AUTH-005 - `x-user-id` can replace the verified bearer subject

Status: **RETEST REQUIRED — unsafe branch remains in the tagged source; current replaced-binary behavior is not yet proven**  
Severity: **Critical identity-confusion risk**

During the earlier integration run, DataBroker authorization selected a non-empty
client-supplied `x-user-id` instead of the verified JWT `sub`. A portal end-user
audit UUID therefore replaced the authenticated service account for ABAC
evaluation. AmbuLife stopped sending end-user identity in that metadata field and
keeps it in proto-owned audit fields.

The tagged v0.4.14 `security_from_request` implementation still resolves the
authorization user as the client header whenever it is non-empty, and uses the
verified JWT `sub` only as a fallback. A new live negative probe cannot proceed
without first accepting one of UDB-AUTH-003/UDB-AUTH-004's rejected credential
paths, but the vulnerable precedence is unchanged in the official release.

Acceptance: authorization subject must always come from verified credential
lineage. Untrusted request metadata may add audit/delegation context only through
an explicit, validated impersonation/delegation contract and must never silently
replace `sub`.

## UDB-AUTH-007 - mTLS is not a scoped alternative when JWT verification is configured

Status: **RETEST REQUIRED — conflict remains in tagged source/docs; current replaced-binary behavior is not yet proven**  
Severity: **Critical / advertised fallback cannot carry service scopes**

The DataBroker rejection in UDB-AUTH-004 says it authenticates “a JWT bearer or
mTLS.” However, v0.4.14 `security_from_request` enters the JWT branch whenever a
JWT public key or JWKS URL is configured and immediately requires a bearer. It
does not inspect the verified peer certificate in that branch, so an mTLS-only
service request is rejected in a deployment that also validates JWTs.

If JWT verification is removed, the mTLS branch can derive a service identity
from certificate SAN/CN, but it has no server-side service scope binding. It
returns no scopes unless `UDB_ALLOW_HEADER_SCOPES=true`; production validation
explicitly forbids header scopes. This cannot replace the exact scoped
service-account credential needed by AuthN/Partner, including `udb:pii:read`.
The enterprise guide also says production mode requires mTLS, making the branch
precedence especially confusing in a normal hardened JWT+mTLS deployment.

Acceptance: compose bearer and verified certificate identity safely, or allow an
explicit mTLS-only service path even when JWT validation exists. Bind tenant,
project and scopes to a server-controlled certificate/service registration; do
not require client-asserted scope headers.

## UDB-GEN-001 - Go SDK still ships private copies of `google/api` well-known protos

Status: **FIXED IN v0.4.15 — canonical Google annotations link and AmbuLife consumer tests pass**  
Severity: **Critical / init-time panic in any consumer binary that also links a
Google SDK**

### Evidence (retested against the published module, not a local build)

- The published SDK module still contains
  `gen/google/api/{annotations,field_behavior,http}.pb.go` (package `api`),
  each registering the canonical descriptor paths
  `google/api/annotations.proto`, `google/api/field_behavior.proto` and
  `google/api/http.proto` in the global protobuf registry.
- Every generated UDB service file (`gen/udb/core/*/services/v1/*_service.pb.go`)
  imports that private package, so the private copy is linked into every
  consumer of the SDK unconditionally — no consumer-side `M` mapping can avoid
  it.
- Concrete reproduction in AmbuLife: `ambucore/cmd/notification` links the UDB
  SDK **and** Firebase Admin / Google Cloud SDKs (FCM push), which depend on
  canonical `google.golang.org/genproto/googleapis/api/annotations`. Both
  packages register `google/api/http.proto`.
- On 2026-07-19, with every environment/linker conflict override explicitly
  removed, `go run ./cmd/notification` against the official v0.4.14 SDK panicked
  before `main` with:

```text
panic: proto: file "google/api/http.proto" is already registered
        previously from: "github.com/fahara02/udb/sdk/go/gen/google/api"
        currently from:  "google.golang.org/genproto/googleapis/api/annotations"
```

- Revalidated again on 2026-07-19 after regenerating AmbuLife's Marketplace
  contracts: `go test ./microservices/marketplace/...` passes, but adding the
  consumer binaries `./cmd/marketplace ./cmd/gateway` makes both test binaries
  panic before `main` with the same duplicate `google/api/http.proto`
  registration. The dependency is the public immutable release-commit module
  `github.com/fahara02/udb/sdk/go@v0.4.1-0.20260718174354-0b5cb4ce10a9`; no local
  checkout, `replace`, registry
  suppression, or locally built UDB artifact was present.

- Fresh verified-rating writer reproduction on 2026-07-19: AmbuLife's new
  customer-rating packages (`go test ./microservices/marketplace/rating/...`)
  all pass independently, but `go test ./cmd/marketplaceratingsource -count=1`
  against the official release-commit SDK
  `github.com/fahara02/udb/sdk/go@v0.4.1-0.20260719110151-dca600a673ee`
  panics before `main` with the same duplicate `google/api/http.proto`
  registration. The command combines the official UDB client with an ordinary
  HTTP-annotated AmbuLife gRPC contract. No broker was started, no UDB source or
  SDK was modified or locally built, and no module replacement or protobuf
  conflict override was used.

- The blast radius is broader than command binaries. A 2026-07-19 retest of
  AmbuLife's ordinary repository packages with the same public module
  (`go test ./pkg/udbx ./microservices/authn/core/repository
  ./microservices/authz/core/repository`) lets the SDK-only `pkg/udbx` package
  pass, then panics both AuthN and AuthZ repository test binaries before their
  tests run. Each package combines UDB's official client with AmbuLife service
  protos that use canonical Google API annotations, so the collision blocks
  otherwise normal DataBroker repository validation as well as Firebase/FCM
  consumers. No broker was started and no registry override or local SDK
  replacement was used.

- The same defect reproduced in AmbuLife's full Partner contract gate on
  2026-07-19. The generated contract packages compiled and the SDK-only Partner
  service/repository tests passed, but `microservices/partner/projection`,
  `cmd/partner`, and `cmd/partnerqualityprojector` each panicked before `main`
  while registering `google/api/http.proto`. This run used the public release
  commit module, did not start a broker, and did not use a local SDK replacement
  or registry-conflict suppression.

- A fresh focused reproduction after adding the partner report-response CRUD
  contract on 2026-07-19 behaved identically: `go test ./cmd/gateway -run
  'Test(Submit|List)PartnerReportResponse' -count=1` panicked before executing
  either test. `npm run verify:contracts` then passed Buf lint/build, generated
  Go compilation and the Partner domain/service/repository suites before
  `cmd/partner` hit the same registration panic. Zero `udb.exe` processes were
  running; only the public release-commit SDK module was linked, and no registry
  override, local SDK `replace`, locally compiled UDB, or broker was used.

- The final AmbuLife partner-activation retest on 2026-07-19 used only the
  checksum-verified official `udb 0.4.14` executable for descriptor lint and
  the public release-commit SDK module above. Official lint accepted the full
  current consumer descriptor (**125 tables, 0 errors**), all generated Partner/
  Marketplace/Dispatch/Trip/Payment Go contract packages compiled, and the
  Partner domain/service/repository suites passed. The next package,
  `cmd/partner`, panicked during protobuf initialization with the same duplicate
  `google/api/http.proto` registration before any command test could run.

- A separate focused Gateway reproduction (`go test -count=1 -run
  TestPlatformApplicationGatewayForwardsExactTenantAndRejectsBodyMismatch
  ./cmd/gateway`) failed at the same init-time registration point. Compile-only
  `go test -c` checks for both `cmd/partner` and `cmd/gateway` succeeded, which
  separates ordinary AmbuLife compile/type errors from the runtime descriptor
  registry failure. No UDB process was started, no UDB source was compiled or
  modified, and no local module replacement or registry-suppression workaround
  was present.

- After AmbuLife added its append-only compliance-review entity on 2026-07-19,
  the checksum-verified official executable accepted the expanded consumer
  descriptor (**126 tables, 0 errors**). The complete consumer gate then
  compiled every generated Partner/Marketplace/Dispatch/Trip/Payment package
  and passed Partner repository/service/projector plus catalog/compliance/
  geography seed packages. `cmd/partner` still panicked before its tests while
  the public SDK copy and canonical genproto copy both registered
  `google/api/http.proto`. No broker was started and no UDB source, SDK
  replacement or registry suppression was used.

- Reconfirmed after the public root tag became directly visible on 2026-07-19:
  `git ls-remote --tags https://github.com/fahara02/udb.git
  refs/tags/v0.4.14` resolves to
  `0b5cb4ce10a929635baea18ce7ed95188a8d44ae`, exactly the commit used by the
  published Go pseudo-version above. From `ambulife/ambucore`, `go test
  ./cmd/partner -count=1` still panics before test execution with the private
  SDK registration as the previous source and canonical genproto as the current
  source. `Get-Process -Name udb` returned no process; this reproduction compiled
  only the consumer binary and did not compile or start UDB.

- Reconfirmed again on 2026-07-19 after AmbuLife implemented the official
  Google Play Integrity standard-request server decode. The checksum-verified
  official `udb 0.4.14` executable accepted the expanded AmbuLife descriptor
  (**128 tables, 0 errors, 53 pre-existing repository-wide warnings**), and the
  generated Partner/Marketplace/Dispatch/Trip/Payment Go contract packages plus
  Partner domain, service, repository and activity-projector tests passed. The
  same contract gate then linked `cmd/partner` with the published UDB SDK and
  Google's official `google.golang.org/api/playintegrity/v1` client. The process
  panicked before tests or `main` because the SDK private copy and canonical
  genproto both registered `google/api/http.proto`. No UDB process was started;
  no UDB source was built or edited; and no module replacement, descriptor
  rename or protobuf-registry suppression was used. This proves the defect
  directly blocks a normal UDB consumer from adding Google Play Integrity, not
  only Firebase/FCM.

- AmbuLife has removed `GOLANG_PROTOBUF_REGISTRATION_CONFLICT=warn` and the
  equivalent protobuf registry linker override from development, E2E, local
  launcher and deployment build paths. The affected Notification runtime now
  remains fail-closed instead of suppressing the upstream descriptor conflict.

### Expanded current-asset blast-radius retest — 2026-07-19 (Asia/Dhaka)

- AmbuLife's complete contract gate used only the current checksum-verified
  official `udb 0.4.14` Windows asset and the public SDK pseudo-version
  `v0.4.1-0.20260719110151-dca600a673ee`. Official descriptor lint passed
  **133 tables, 0 errors, 53 warnings**. Official `policy-lint` separately
  passed **179 policies, 0 findings**. Buf lint/build and every generated
  Partner/Marketplace/Dispatch/Trip/Payment Go contract package compiled.
- Partner domain/service/repository/projection and catalog/compliance/geography
  seed packages passed. `cmd/partner` then panicked before any test or `main`
  at the same duplicate `google/api/http.proto` registration.
- The same fresh process-level failure now reproduces in all of these ordinary
  consumer surfaces: `cmd/marketplaceratingsource`,
  `microservices/dispatch/repository`,
  `microservices/payment/core/repository`,
  `microservices/payment/core/service`,
  `microservices/payment/core/settlement`, and
  `beacon/internal/modules/tracking`. Their UDB-independent domain packages
  pass before the affected processes reach protobuf initialization.
- Compile-only `go test -c` checks still succeed for AmbuCore Partner/Gateway
  and Beacon tracking/API packages. This separates Go type/compile failures
  from the SDK's init-time global descriptor collision.
- No broker was started, no UDB source or SDK file was built or edited, no
  local module `replace` was used, and no protobuf registry suppression was
  enabled. This retest expands the demonstrated blast radius; it does not add
  an AmbuLife workaround.

### Required fix

Stop generating private copies of `google/api/*`. In the SDK generator config,
map the well-known Google API protos to the canonical module
(`google.golang.org/genproto/googleapis/api/annotations`) and add it as a
direct dependency, so exactly one registration of each descriptor path exists
in any consumer binary.

### Acceptance

A Go binary importing both the UDB Go SDK and any Google Cloud SDK starts with
the default protobuf registry policy and without a registry panic. No consumer
environment/linker suppression or private `Mgoogle/api/*` mapping is required.

### Fresh AmbuLife distribution-lifecycle reproduction — 2026-07-19 (Asia/Dhaka)

- AmbuLife added canonical forced-RLS `AmbuDriveDistributionRelease` and
  `AmbuDriveDistributionReview` protos plus typed distribution/enrollment RPCs.
  The checksum-verified official Windows asset reports `udb 0.4.14`, SHA-256
  `bc2f09b110af5ff4ded0ef1ce81675586bd8a1b51458b4e526bf312ac1868562`.
  Its read-only descriptor lint passes the new complete consumer schema with
  **130 tables, 0 errors, 53 warnings**, descriptor checksum
  `db5690c0478b09d8f5c3fb308c9f6da300be6de6cb72daa94c3eb7ac16f9db28`.
- `go test ./microservices/partner/core/service
  ./microservices/partner/repository` and the partner projection package pass,
  including Play URL/package validation, signed landing-token tamper/expiry,
  release lineage and activity-source tests.
- `go test ./cmd/gateway ./cmd/partner` then panics before test execution with
  the exact `google/api/http.proto` collision: previous registration from
  `github.com/fahara02/udb/sdk/go/gen/google/api`, current registration from
  `google.golang.org/genproto/googleapis/api/annotations`. The full AmbuLife
  contract gate reaches the same point after Buf/descriptor/generated-code/core
  checks pass.
- The consumer uses public module
  `github.com/fahara02/udb/sdk/go@v0.4.1-0.20260718174354-0b5cb4ce10a9`, the
  exact root v0.4.14 release commit. A fresh
  `go list -m -json github.com/fahara02/udb/sdk/go@v0.4.14` still returns
  `unknown revision sdk/go/v0.4.14`.
- No UDB process was started, no UDB source was built or edited, and no local
  module replacement, registry suppression, descriptor rename or RLS/ABAC
  bypass was used.

### Final release-commit reproduction — 2026-07-19 (Asia/Dhaka)

- GitHub's final `v0.4.14` root tag resolves to
  `dca600a673ee095cbefdc80aeebb0c1febd49a65`; the official Go SDK at that exact
  commit resolves publicly as
  `github.com/fahara02/udb/sdk/go@v0.4.1-0.20260719110151-dca600a673ee` with sum
  `h1:wEflgf+v7kYM4jl1Di5YnN2lWhWo/Z8UA+JzcfoMoco=`.
- With both AmbuCore and Beacon pinned to that module and no local `replace`, a
  fresh `go test ./cmd/gateway -count=1` panics before executing tests:

```text
panic: proto: file "google/api/http.proto" is already registered
        previously from: "github.com/fahara02/udb/sdk/go/gen/google/api"
        currently from:  "google.golang.org/genproto/googleapis/api/annotations"
```

- The checksum-verified final Windows asset reports `udb 0.4.14` and has SHA-256
  `bc2f09b110af5ff4ded0ef1ce81675586bd8a1b51458b4e526bf312ac1868562`.
  Only `--version` was executed. No UDB process was started, no UDB source was
  compiled or modified, and no registry suppression or generated-proto rename
  was introduced in AmbuLife.

## UDB-GO-006 - Entity facade ignores request-scoped SDK metadata

Status: **OPEN IN v0.4.15 — source-verifiable in the published SDK module**  
Severity: **High / request audit and correlation metadata cannot be expressed safely**

### Expected

An application that calls `udbclient.WithMetadata(ctx, metadata)` before an
official `Udb.Entity(...).Select/Upsert/Delete` operation should have the
request-scoped correlation ID, bounded purpose and client catalog version sent
for that operation, while the authenticated tenant/principal remain fixed by
the connected client.

### Actual in official `sdk/go/v0.4.15`

`udbclient.WithMetadata` stores request metadata on `context.Context`, and the
generated adapter layer reads it through `MetadataFromContext`. The hand-written
Entity facade does not. `Entity.requestContext()` constructs the protobuf
request context only from the Entity's copied `e.meta` tenant/project fields.
The underlying `Client.Context(ctx)` then appends purpose, correlation ID,
service identity and catalog version from the long-lived `Client.Meta` fields,
not from `MetadataFromContext(ctx)`.

Consequently, two concurrent portal operations cannot safely carry distinct
correlation IDs or bounded purposes through the official Entity API. Mutating
the shared client's exported `Meta` per call is race-prone and can misattribute
requests. Appending competing raw gRPC headers is an application workaround,
not an acceptable SDK contract, and AmbuLife removed that workaround during its
v0.4.15 direct-SDK refactor. End-user identity is deliberately not sent as
`x-user-id` because UDB-AUTH-005 remains a separate authorization-subject risk.

### Acceptance

Make the Entity facade merge request-scoped SDK metadata from the context for
correlation ID, purpose and client catalog version on every operation. Keep
credential-derived tenant, project, scopes and principal authoritative and
non-overridable. Add concurrent Entity tests proving two contexts preserve
their own audit metadata without mutating `Udb.Meta`, and ensure each metadata
field is emitted exactly once.

## UDB-STORAGE-001 - v0.4.15 blocks service API keys from every Storage RPC

Status: **OPEN IN THE PUBLISHED v0.4.15 BINARY — live reproduced 2026-07-21**  
Severity: **Release blocker / backend services cannot use UDB-managed media**

### Environment

- Published Windows binary: `udb 0.4.15`, 99,000,832 bytes, SHA-256
  `5cc3c43e266ab9f2f8fdb31738faf6d6a953120cf80cec94082cf13651725287`.
- Published Go SDK: `github.com/fahara02/udb/sdk/go@v0.4.15`.
- Client path: `udbclient.Connect` with a tenant-bound
  `udbclient.Credentials{APIKey: ...}`, then the official
  `StorageService/RegisterUpload` client on the SDK-managed auth connection.
- The key is active, tenant/project-bound and carries the exact reviewed
  `udb:storage:register-upload` scope. The same client performs tenant-scoped
  DataBroker catalogue CRUD successfully.

### Expected

A backend service with a valid, tenant-bound, scoped UDB API key can execute
the documented storage flow:

1. `RegisterUpload`;
2. upload to the returned presigned URL;
3. `FinalizeUpload`;
4. use the resulting UDB file identity in application records.

No human password, browser session, direct object-store credential or database
bypass should be required by an application service.

### Actual live result

The first RPC is rejected before storage logic runs:

```text
rpc error: code = PermissionDenied desc = API key credential is not allowed for this method
```

The failing path is:

```text
/udb.core.storage.services.v1.StorageService/RegisterUpload
```

This is not caused by a missing scope. The v0.4.15 release descriptor declares
the exact `udb:storage:register-upload` scope but allows only
`CREDENTIAL_TYPE_BEARER_JWT` and `CREDENTIAL_TYPE_SESSION`. It omits
`CREDENTIAL_TYPE_API_KEY` on `RegisterUpload`, `FinalizeUpload`, `GetFile`,
`GetDownloadUrl`, `ReissueUploadUrl`, `DownloadFile`, `UpdateFile`,
`DeleteFile`, and `ListFiles`.

Exchanging the key through the public `AuthnService/Authenticate` RPC is not a
valid escape hatch in v0.4.15. The issued JWT deliberately retains
`auth_method=api_key`, and the method-security gate maps that claim back to
`CREDENTIAL_TYPE_API_KEY`; the same Storage descriptor omission therefore
rejects the exchanged JWT as well. This behavior is covered by the release
source's `api_key_origin_jwt_fails_closed_when_descriptor_omits_api_key` test.

### Release/source mismatch

The current post-v0.4.15 UDB working tree adds
`CREDENTIAL_TYPE_API_KEY` to all nine Storage RPC descriptors with the comment
`UDB-AUTH-008: scoped service keys`. That correction is not present at the
`v0.4.15` / `sdk/go/v0.4.15` release commit
`69a5e8d9d55bd872c778ee0ba8fa72704742ab70` and is not embedded in the
published v0.4.15 Windows binary tested above.

### Impact on AmbuLife

AmbuLife's real ambulance catalogue entities, presentations, requirements and
add-on memberships work through UDB 0.4.15. Catalogue image upload cannot be
enabled without violating the approved architecture: the portal cannot seed or
publish card images/icons through UDB Storage, and AmbuLife will not introduce a
direct object-store path, human-admin credential, wrapper or authorization
bypass.

### Acceptance

- Publish a new immutable UDB release whose embedded Storage descriptors allow
  scoped API keys on every intended service-to-service Storage RPC.
- Prove both direct scoped API-key calls and API-key-origin JWT calls reach the
  Storage handler with tenant/project/scope enforcement intact.
- Add a live SDK test covering `RegisterUpload -> presigned PUT ->
  FinalizeUpload -> GetFile` with a least-privilege service key.
- Keep negative tests for missing scope, cross-tenant request metadata, revoked
  key, oversized upload and mismatched finalization metadata.

## UDB-DB-READINESS-001 - v0.4.15 keeps serving after database loss and reports API keys as unauthenticated

Status: **OPEN IN THE PUBLISHED v0.4.15 BINARY — live reproduced 2026-07-21**  
Severity: **High / dependency loss is exposed as an authentication failure while the data-plane socket remains live**

### Environment and load

- The same checksum-verified official Windows v0.4.15 binary and AmbuLife
  descriptor root documented above.
- One broker process, started with the published `udb.exe serve` command; no
  local UDB build, source edit, SDK replacement, direct database access or
  application-side authentication fallback.
- Low-volume local integration traffic: AuthN session validation and the
  system-tenant ambulance catalogue. Before the failure, the catalogue read
  returned all 75 seeded records through official SDK/DataBroker CRUD.
- The backing PostgreSQL endpoint later stopped accepting connections. A clean
  v0.4.15 restart correctly identified that root cause with its startup health
  gate. This correction means there is no evidence here of a UDB pool leak or
  worker-caused exhaustion.

### Actual live result

While PostgreSQL was unavailable, the still-running v0.4.15 process and
data-plane port remained live. The broker logged the same indirect pool symptom
from nearly every built-in singleton worker:

```text
pool timed out while waiting for an open connection
```

Affected workers included notification delivery, embeddings, search reindex,
search freshness, scheduler, workflow, webhook delivery, lock expiry, vault
lease reaping, cache invalidation, WebRTC reaping, projection materialization,
XA recovery, saga recovery, the authz snapshot reload and the control-plane
reload subscriber. The error continued across repeated worker ticks from at
least 05:41 through 05:46 Asia/Dhaka on 2026-07-21.

Foreground authentication then failed closed because UDB could not validate a
previously working tenant-bound API key:

```text
udb /udb.services.v1.DataBroker/Select:
x-api-key could not be validated (key store unavailable); request denied
(Unauthenticated)
```

That `Unauthenticated` response propagated through AmbuCore as `authentication
service temporarily unavailable`, breaking both fresh admin login and existing
portal session validation. Redis and NATS were independently healthy at the
time of the final reproduction. Restarting the exact official binary then
produced the accurate startup diagnosis:

```text
PostgreSQL startup health gate failed: a connection string was resolved ...
but the database could not be reached.
```

The startup behavior is correct. The remaining release issue is runtime
dependency/readiness signaling and error classification after a previously
healthy PostgreSQL endpoint disappears.

### Expected

When the backing store becomes unreachable, UDB must immediately report
`NOT_SERVING`/not-ready and return `Unavailable` (or another dependency-specific
server error) rather than claiming that a valid API key is unauthenticated. The
broker should recover automatically when PostgreSQL returns, or exit so its
supervisor can restart it; it must not retain a live-looking data-plane socket
that misclassifies the outage as invalid credentials.

### Acceptance

- Start a healthy broker, stop PostgreSQL, and assert gRPC health/readiness
  changes to `NOT_SERVING` within a bounded interval.
- Assert DataBroker API-key validation returns a dependency/unavailable error,
  never `Unauthenticated`, while the key store cannot be queried.
- Restart PostgreSQL and prove the same broker either recovers automatically or
  has exited for supervised restart; then prove the original key and catalogue
  read succeed again.
- Emit direct database-connectivity state and pool metrics so an operator does
  not have to infer dependency loss from dozens of worker timeout messages.

## AmbuLife v0.4.15 release gate

- [x] Official Windows binary and checksum used; no local UDB build.
- [x] Official semantic Go SDK `sdk/go/v0.4.15` used; no absolute local SDK replacement.
- [x] Official semantic Go SDK `sdk/go/v0.4.15` tag is published and proxy-resolvable.
- [x] AmbuLife protobuf conflict-suppression workarounds are absent and
      UDB-GEN-001 passes with canonical Google annotations.
- [x] AmbuLife short-name compatibility renames removed; canonical
      `User`/`Session`/`OTP` identities are the startup input.
- [ ] Embedded native and consumer descriptors compose by protobuf FQN without
      short-name collisions (UDB-CAT-001).
- [ ] Startup logs expose the concrete combined-catalog lint issues
      (UDB-CAT-002).
- [x] Service accounts have no owner/admin role workaround.
- [x] Native service-account API keys authenticate with exact owner, tenant and reviewed scopes on Authn.
- [ ] Entity CRUD honors request-scoped SDK correlation/purpose metadata without shared-client mutation (UDB-GO-006).
- [ ] Tenant-scoped service API keys can complete the native UDB Storage upload lifecycle (UDB-STORAGE-001).
- [ ] Runtime PostgreSQL loss flips readiness and returns `Unavailable`, not false API-key rejection (UDB-DB-READINESS-001).
- [ ] Restricted password service login returns the exact reviewed scopes.
- [ ] Scoped native API keys are accepted by DataBroker or exchanged for an attenuated bearer.
- [ ] Positive per-service CRUD proofs pass.
- [ ] Negative cross-service entity probes return `PermissionDenied`.
- [ ] `x-user-id` cannot replace bearer subject.
- [ ] mTLS service identity works alongside JWT validation with server-bound scopes.

AmbuLife now runs the portions proven against the official release and keeps
the UDB-backed portal catalogue online. Features blocked by unchecked release
items remain fail-closed; AmbuLife does not weaken RLS/ABAC or add alternate
database/object-storage paths.
