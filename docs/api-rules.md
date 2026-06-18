# UDB API Rules

UDB `0.3.6` is beta and pre-1.0. The purpose of this guide is to settle the
HTTP/JSON API, OpenAPI, and SDK naming rules before the product reaches `1.0.0`.
Until `1.0.0`, UDB may make breaking API and SDK changes when they simplify the
contract and are documented with migration notes. After `1.0.0`, these rules
become part of the public compatibility contract.

This guide is normative for new public API work. When existing code disagrees
with this guide, treat the mismatch as migration work, not as precedent.

## Sources Of Truth

- Proto service definitions and `google.api.http` annotations own HTTP routing.
- Descriptor metadata owns SDK aliases, OpenAPI operation IDs, security posture,
  and generated documentation.
- `api/udb-broker.swagger.json` is generated output. Do not hand-edit it.
- Generated SDK clients and generated docs are output. Change templates or
  descriptor metadata, then regenerate.
- Root docs should describe behavior that is actually served. If docs and code
  disagree, fix the descriptor/runtime path first.

External design basis:

- Google API Improvement Proposals: resource-oriented design
  (`https://google.aip.dev/121`), resource names
  (`https://google.aip.dev/122`), standard methods
  (`https://google.aip.dev/130` through `https://google.aip.dev/135`), custom
  methods (`https://google.aip.dev/136`), request IDs
  (`https://google.aip.dev/155`), pagination (`https://google.aip.dev/158`),
  filtering (`https://google.aip.dev/160`), field masks
  (`https://google.aip.dev/161`), errors (`https://google.aip.dev/193`),
  compatibility (`https://google.aip.dev/180`), and stability levels
  (`https://google.aip.dev/181`).
- Microsoft REST API guidance on noun-based resources and plural collection
  URIs (`https://learn.microsoft.com/en-us/azure/architecture/best-practices/api-design`).
- OpenAPI operation ID and extension rules from the OpenAPI Specification
  (`https://spec.openapis.org/oas/v3.1.0.html`).
- gRPC and Connect error guidance for canonical status codes, typed error
  details, and HTTP mapping (`https://grpc.io/docs/guides/error/`,
  `https://connectrpc.com/docs/protocol/`).
- Stripe's public API versioning writeups as a warning that long-lived
  compatibility layers are expensive and should not be promised before the v1
  surface is settled (`https://stripe.com/blog/api-versioning`).

## Version And Compatibility

- UDB product version `0.x` means beta. Do not claim stable backward
  compatibility for HTTP routes, OpenAPI operation IDs, or SDK method names.
- Breaking `0.x` changes must have migration notes: old route or SDK name, new
  route or SDK name, reason, affected SDK languages, and example migration.
- Breaking changes should be batched before `1.0.0` instead of adding permanent
  shims for every beta route.
- After `1.0.0`, removing a field, changing field meaning, renaming routes,
  changing operation IDs, changing SDK public method names, or changing error
  reasons is a breaking change unless a versioned compatibility path exists.
- The wire protocol version and product version are related but not identical.
  Do not use the protocol version to imply product API stability.

## URL Shape

- All public HTTP routes start with `/v1`.
- Literal path segments use lowercase kebab-case: `/v1/api-keys`, not
  `/v1/api_keys` or `/v1/apiKeys`.
- Collections use plural nouns: `/v1/users`, `/v1/api-keys`,
  `/v1/storage/uploads`.
- Item routes are collection plus identifier:
  `/v1/api-keys/{key_id}`.
- Nested resources are allowed only when the parent is part of the resource
  identity or access boundary:
  `/v1/webrtc/rooms/{room_id}/peers/{peer_id}`.
- Avoid unnecessary domain wrappers. Prefer `/v1/assets` over
  `/v1/asset/assets`.
- Do not use trailing slashes.
- Do not use verbs as path segments for actions:
  `/v1/storage/uploads/{file_id}:finalize`, not
  `/v1/storage/uploads/{file_id}/finalize`.
- Proto path variables use proto field names, normally lower_snake_case. If
  generated OpenAPI renders them as lowerCamelCase, document that as generated
  JSON-name behavior rather than changing proto field names only for cosmetics.
- Avoid paths deeper than collection/item/collection unless the deeper parentage
  is part of identity or authorization. Deep navigation paths are harder to
  evolve and should fail the style review unless justified.

Allowed exceptions:

- SCIM keeps protocol-required resource names such as `Users` and `Groups`.
- Well-known endpoints may keep protocol-defined names such as
  `/.well-known/jwks.json`.
- Health, metrics, reflection, and internal gRPC-only surfaces are outside this
  public HTTP/JSON style guide unless explicitly exposed under `/v1`.

## Resources Before Actions

Model the thing users work with as a resource first.

Good resource examples:

- API keys: `/v1/api-keys`
- OTP attempts or challenges: `/v1/auth/otps`
- CSRF tokens: `/v1/auth/csrf-tokens`
- Storage uploads: `/v1/storage/uploads`
- Storage files: `/v1/storage/files`
- WebRTC rooms: `/v1/webrtc/rooms`
- Governance versions: `/v1/authz/governance/versions`

Durable workflows should usually be resources:

- Creating an upload starts an upload resource.
- Creating a session creates a session resource.
- Creating a persisted challenge creates a challenge resource.
- Creating a long-running job creates an operation or job resource.

Use a custom action only when the operation does not cleanly fit create, get,
list, update, or delete. Custom actions must attach to the resource or
collection they operate on.

## Resource Identity

- Public resources should have one canonical identity form. Prefer stable string
  IDs in path variables and resource bodies.
- Resource names should be path-shaped and stable across product versions when
  they are exposed, for example `api-keys/{key_id}` or
  `storage/files/{file_id}`. Do not include `/v1` in a stored resource name.
- If a resource exposes both `name` and `{resource}_id`, `name` is the canonical
  resource reference and the separate ID is output-only unless the create method
  explicitly supports caller-chosen IDs.
- User-chosen IDs must document their allowed format and must not permit `/`,
  `?`, `#`, unescaped control characters, or ambiguous case-sensitive aliases.
- Aliases such as `users/me` are allowed only when all returned resources still
  contain the canonical identity.
- Do not expose self-links, database tuple keys, or backend physical identifiers
  as the canonical public identity.

## Standard Methods

Use standard methods unless a custom action is clearly simpler.

| Intent | HTTP | Path | Body |
| --- | --- | --- | --- |
| List resources | `GET` | `/v1/resources` | none |
| Get one resource | `GET` | `/v1/resources/{resource_id}` | none |
| Create resource | `POST` | `/v1/resources` | resource or request object |
| Partial update | `PATCH` | `/v1/resources/{resource_id}` | resource plus update mask |
| Full replace/upsert | `PUT` | `/v1/resources/{resource_id}` | full resource |
| Delete resource | `DELETE` | `/v1/resources/{resource_id}` | usually none |

Rules:

- `GET` must be safe and side-effect-free except normal audit/log telemetry.
- `DELETE` should be idempotent from the client's point of view.
- `PATCH` should use an update mask when partial update semantics would
  otherwise be ambiguous.
- `PATCH` request messages use `google.protobuf.FieldMask update_mask` when the
  operation is a protobuf update. The mask is relative to the resource, not to a
  wrapper field, and invalid mask paths return `INVALID_ARGUMENT`.
- `PUT` is for full replacement or an explicitly documented singleton/upsert.
  Do not use `PUT` for arbitrary partial actions.
- Empty list results return an empty list, not `NOT_FOUND`. `NOT_FOUND` is for a
  missing requested resource or missing parent resource.

## Custom Actions

Custom actions use a colon suffix:

- `POST /v1/auth/otps:send`
- `POST /v1/auth/otps:verify`
- `POST /v1/auth/otps:resend`
- `POST /v1/storage/uploads/{file_id}:finalize`
- `POST /v1/webrtc/rooms/{room_id}:close`
- `POST /v1/api-keys/{key_id}:rotate`

Rules:

- Use `:action`, not `?action=...`, and not slash verbs.
- Use lowerCamelCase for multi-word action names in the route suffix:
  `:refreshJwks`, `:testDiscovery`, `:previewClaimMapping`.
- Use `POST` for actions with side effects, security decisions, validation with
  sensitive input, or non-trivial request bodies.
- A custom `GET` action is allowed only for a side-effect-free read that cannot
  be expressed as a resource read; it must be justified in the proto comment.
- Do not create pseudo-read actions such as `:list`, `:get`, or `:status` when a
  normal resource route is clearer.
- Do not hide materially different operations behind request fields. Separate
  routes/actions produce clearer OpenAPI and SDK methods.

## Query Parameters

Query parameters are selectors and read modifiers, not command dispatch.

Allowed query use:

- Pagination: `page_size`, `page_token`.
- Filtering: `filter`.
- Ordering: `order_by`.
- Projection or partial response: `fields` when supported.
- Read consistency or revision selectors when explicitly documented.
- Optional booleans that modify representation, not behavior.

Disallowed query use:

- `?action=verify`, `?action=resend`, `?op=delete`.
- Security-sensitive decisions hidden in query strings.
- Large structured payloads.
- Any selector that changes a safe read into a mutation.

List and search rules:

- Every unbounded list RPC has `page_size` and `page_token` on the request and
  `next_page_token` on the response, unless a bounded singleton/list exception
  is documented.
- `page_size` comments must document default and maximum behavior. Negative
  values return `INVALID_ARGUMENT`; oversized values are either clamped or
  rejected consistently and documented.
- Page tokens are opaque. Clients must not parse them and servers must validate
  tenant/project/caller binding before accepting them.
- `filter` and `order_by` are validated against a documented allowlist. Unknown
  fields, type mismatches, unsupported operators, and raw SQL fragments return
  `INVALID_ARGUMENT`.
- `fields` projections, when supported, are validated against resource field
  paths. Unknown fields return `INVALID_ARGUMENT`; output-only and sensitive
  fields follow the descriptor security policy.
- SCIM filter syntax is a protocol exception and must stay scoped to SCIM
  routes.
- Offset/limit pagination is not the default UDB style for descriptor-owned
  APIs. Use token pagination unless an endpoint documents why offset is
  required.

## Request And Response Shape

- Request and response JSON names follow protobuf JSON mapping.
- Resource IDs should be stable strings. Do not expose database primary key
  details unless the key is already the public resource ID.
- Timestamps use protobuf timestamp JSON form.
- Money, byte counts, durations, and quantities must state units in field names
  or comments.
- Repeated list responses include `items` or a resource-specific repeated field
  and `next_page_token` when paginated.
- Mutations return the changed resource or a typed response with the changed
  resource plus operation metadata.
- Long-running work should return an operation/job resource or a typed accepted
  response that can be polled.
- Avoid `map<string, string>` for structured data that the SDKs need to type.

## Errors

- The canonical server error model is gRPC `google.rpc.Status`: a numeric status
  code, a developer-facing message, and a `details` list of typed `Any`
  payloads. HTTP/JSON and SDK errors are MAPPED from this single model — never a
  second, bespoke error contract, and never a body-level envelope on the gRPC
  wire.
- Use canonical status codes consistently:
  - `INVALID_ARGUMENT` for malformed input.
  - `FAILED_PRECONDITION` for valid input blocked by current state/config.
  - `UNAUTHENTICATED` for missing/invalid credentials.
  - `PERMISSION_DENIED` for authenticated callers without access.
  - `NOT_FOUND` for missing resources.
  - `ALREADY_EXISTS` for create conflicts.
  - `ABORTED` for transaction conflicts that can be retried by policy.
  - `RESOURCE_EXHAUSTED` for quota/rate limits.
  - `UNAVAILABLE` for transient service dependency failures.
- Do not return success with an embedded error for normal request failures.
  Success is signalled by the gRPC `OK` status (a 2xx HTTP status at the REST
  boundary), not by a body-level `success` flag.
- Error reasons are public API. Use stable, documented reason strings.
- SDKs must decode the same typed error detail across languages.

### Rich Error Details (v1 target)

The baseline gRPC error — code plus a human message — is the floor, and the
current default. The v1 target is the RICH model: machine-actionable
information rides in `google.rpc.Status.details` as typed `Any` payloads, NOT
parsed out of the message string. Standard payloads and their public mapping:

| `google.rpc` detail | Carries | Public field |
| --- | --- | --- |
| `BadRequest.FieldViolation` | per-field validation failures | `field_violations[]` `{field, description}` |
| `ErrorInfo` | stable machine `reason` + `domain` + metadata | `code` (the reason), `error_id` (from metadata) |
| `RetryInfo` | server-advised retry delay | `retryable` (+ retry-after) |
| `QuotaFailure` | quota/rate-limit subject | `code` + violation subject |
| `PreconditionFailure` | the unmet state/config precondition | `code` + precondition type |
| `ResourceInfo` | the missing/conflicting resource | resource type/name |

Adopting rich details is additive and non-breaking: clients that read only
code+message keep working, and a site that does not yet emit typed details
falls back to the message string. `retryable` must agree with the descriptor
idempotency/retry-safe contract; do not advertise a mutation as retryable until
its dedup/transaction/outbox behavior is proven through the served path.

### Response And Error Shape At Boundaries

The ONLY uniform contract at the boundaries is the **error** model plus
**inline** metadata. There is NO success-wrapping envelope — not on the gRPC
wire, not at REST, and not as a prescribed SDK return type. A `{success, data}`
/ `ApiResponse<T>` wrapper duplicates the status channel (gRPC `OK` / HTTP 2xx
already signal success), creates a second error path, and breaks streaming;
mature gRPC-first stacks return bare bodies (AIP-193, Connect, Stripe).

- Success is a **bare typed body** with the gRPC `OK` status (a 2xx at REST). Do
  not add a body-level `success` flag or a `data` wrapper.
- Errors are the only structured boundary object: map the gRPC status to an HTTP
  status (`NOT_FOUND`→404, `PERMISSION_DENIED`→403, `INVALID_ARGUMENT`→400,
  `RESOURCE_EXHAUSTED`→429, `UNAVAILABLE`→503, `DEADLINE_EXCEEDED`→504, …), but
  PRESERVE the canonical gRPC code/reason in the body — HTTP codes alone are
  lossy. The error body is `ApiError` mapped from `google.rpc.Status` + its
  typed details (above).
- Metadata is **inline**, not a wrapper: `request_id` rides response headers,
  pagination rides the list body (`next_page_token`/page fields per AIP-158).
- A typed SDK maps the gRPC status + details into a language-native error
  (the `ApiError` equivalent) and synthesizes transport-level errors that never
  reach a server handler (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `CANCELLED`) into
  the same error shape — so callers see one error model. Success stays the
  decoded typed message; idiomatic per-language returns (`(T, error)`, throw)
  are preferred over a uniform result wrapper.

Design basis: Google AIP-193 / AIP-158, the gRPC error guide
(`https://grpc.io/docs/guides/error/`), and the Connect protocol's gRPC→HTTP
status mapping (`https://connectrpc.com/docs/protocol/`).

## Idempotency And Retries

- Mutating create/action routes that clients may retry should accept an
  idempotency key or request ID at the request message level.
- Idempotency keys are scoped to the operation type, tenant/project, and caller
  boundary. They must not be global unscoped keys.
- Replayed idempotent requests return the prior successful response when
  available.
- SDK automatic retries are allowed only for operations marked retry-safe in the
  descriptor contract.
- Do not make a mutation retry-safe until its deduplication, transaction, and
  outbox behavior are proven through the served path.

## Identity, Tenant, And Authorization

- The authenticated principal, tenant, project, scopes, and service identity
  come from verified metadata/claims, not from caller-controlled body fields.
- Body fields such as `tenant_id`, `project_id`, `actor`, `created_by`, or
  `reviewer` must be validated against the claim context or removed from the
  public request shape.
- Reads and writes both enforce the same tenant/project binding rule.
- Admin and native-service routes must fail closed when identity, tenant,
  project, scope, or policy context is missing.
- Audit fields are server-assigned unless the API explicitly models a delegated
  actor and verifies it.

## OpenAPI Rules

- Every public HTTP RPC has a stable `operationId` from descriptor metadata.
- `operationId` uses lowerCamelCase and describes user intent:
  `sendOtp`, `verifyOtp`, `rotateApiKey`, `finalizeStorageUpload`.
- `operationId` values are unique across the whole OpenAPI document. Treat
  uniqueness as case-sensitive per OpenAPI, and also lint case-insensitive and
  language-normalized collisions because SDK generators often normalize names.
- Do not expose generated `Service_RpcName` operation IDs in public Swagger.
- Tags should group by public resource/domain, not by internal Rust module.
- Descriptions must not promise unimplemented behavior or stable compatibility
  during `0.x`.
- Swagger is regenerated from proto/descriptor data and then post-processed by
  the checked-in script. Do not hand-edit generated JSON.
- Public Swagger should carry descriptor-derived vendor extensions for generated
  clients and docs:
  - `x-udb-sdk-alias`
  - `x-udb-scope`
  - `x-udb-retry-safe`
  - `x-udb-idempotency`
  - `x-udb-resource`

## SDK Rules

- Public SDK method names come from descriptor aliases, not raw proto RPC names.
- Wire RPC names remain available only for transport dispatch and diagnostics.
- Descriptor aliases are snake_case at the source:
  `send_otp`, `verify_otp`, `rotate_api_key`.
- Language casing is generated mechanically:
  - Python: `send_otp`
  - TypeScript/PHP: `sendOtp`
  - Java/C#: `SendOtp` or the language's existing public style if documented
  - Go metadata: preserve wire names while exposing alias metadata for docs and
    future helpers
- Alias casing must be acronym-safe. `send_otp` must not become `send_o_t_p`.
- SDKs should expose the same operation set, same retry classification, same
  typed errors, and same tenant/project metadata behavior.
- SDK examples in docs must use the canonical alias names from generated output.

## Naming Rules

- Resource path literals: lowercase kebab-case.
- Proto field names: lower_snake_case.
- JSON field names: protobuf JSON mapping.
- SDK descriptor aliases: lower_snake_case.
- OpenAPI operation IDs: lowerCamelCase.
- Custom action suffixes: lowerCamelCase.
- Error reasons: UPPER_SNAKE_CASE or the existing typed-error convention, but
  one convention must be used consistently per error detail family.
- Avoid overloaded words such as `data`, `object`, `manager`, `process`, and
  `handle` unless they are the actual user-facing concept.

## Review Checklist

Before adding or changing a public API route:

- Is this a real user-facing resource, not a database table leak?
- Can the operation use a standard method?
- If it is a custom action, is `:action` attached to the correct resource?
- Are path literals plural lowercase kebab-case?
- Are query parameters only filters/selectors/modifiers?
- Are list/search methods paginated and bounded?
- Are filters/order_by/fields/update_mask values validated against an allowlist?
- Is the resource identity canonical and free of backend physical IDs?
- Is tenant/project/actor context claim-bound?
- Is the route represented in proto first?
- Does the RPC have a descriptor SDK alias and REST operation ID?
- Does OpenAPI show a stable operation ID?
- Is the operation ID unique after OpenAPI case-sensitive, case-insensitive, and
  language-normalized checks?
- Do all SDK languages receive the same public operation with correct casing?
- Are errors typed and retryability explicit?
- Does the change require a beta migration note?
- Did the route-style checker and native lint cover the rule being relied on?

## Adoption Plan

The implementation plan is Chapter 14 of the private masterplan:
`private/masterplan/todos/14-api-sdk-standardization.md`.

Execution order:

1. Document the beta compatibility posture in public versioning docs.
2. Make descriptor aliases and REST operation IDs load-bearing before renaming
   SDK methods or OpenAPI operations.
3. Add a route-style checker that encodes this guide and reports current
   violations with source proto locations.
4. Migrate routes by domain in proto annotations: API keys, analytics, assets,
   storage, WebRTC, authn, authz governance, and IdP.
5. Regenerate OpenAPI, SDK manifests, generated SDKs, docs, and examples through
   the normal toolchain.
6. Promote the checker from advisory to CI-gating once the known violations are
   gone.

No implementation phase should introduce a separate hand-maintained route list,
SDK alias table, or generated-file edit.
