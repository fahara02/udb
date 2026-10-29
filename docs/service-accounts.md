# Service accounts acting for users

This guide is for the backend service that sits between your users and UDB:
an API server, a worker, a clinic app. It answers five questions.

1. What does my service's token actually contain?
2. How do I ask "may this user do that?" when the caller is my service, not the
   user?
3. How does one service act in more than one tenant?
4. Can I create users who have a phone number and no email?
5. What does it mean when `AcquireLock` says `ALREADY_EXISTS`?

Everything here is available from UDB 0.6.4. Request shapes are written as
protobuf JSON field names; every SDK exposes the same fields on its generated
clients. All of these RPCs live on the native control-plane listener
(`UDB_AUTH_GRPC_ADDR`, default `:50061`).

## A service account in one minute

A service account is two records, and it needs both:

- a **user** created with `CreateUser` and
  `account_kind: ACCOUNT_KIND_SERVICE_ACCOUNT`;
- one **typed grant**, created with `CreateServiceAccountGrant`. It names the
  account's tenant, project, `service_identity` and `approved_scopes`.

Without an ACTIVE grant the account cannot authenticate at all. The grant is the
ceiling: a token never carries a scope the grant does not approve, and a grant
refuses admin, owner and wildcard scopes. The account gets a bearer by password
`Login` or by exchanging an API key with `Authenticate{api_key}`.

## What a service-account token contains

| Claim / field | Where it comes from |
|---|---|
| subject (`user_id`) | the service account's user id |
| tenant and project | the grant's tenant and project |
| scopes | the grant's `approved_scopes`, narrowed to what the login asked for |
| `service_identity` | the grant's service identity |
| roles | the roles bound to the account in that tenant and project, minus the ones a workload may never hold (below) |
| `account_kind` | `ACCOUNT_KIND_SERVICE_ACCOUNT` |

**Scopes come only from the grant.** Roles never add scopes to a service token.

**Roles are filtered.** Before 0.6.4 a service bearer carried no roles. It now
carries the roles you bind to it with `AssignRole`, except roles a workload
credential may never hold:

- platform roles, `platform_admin`, `organization_owner` and every other role
  that projects scopes into a token;
- `break_glass_admin`;
- any role code with a wildcard, `admin`, `owner`, `superuser` or `root`
  segment (for example `ops_admin` or `tenant_owner`).

A withheld role does not fail the login. The account still signs in, just
without that role. The broker logs the withheld role on the `udb.audit.authn`
target and publishes `udb.authn.service.role.withheld.v1`. Authorization
decisions read roles from the authz snapshot, not from the token, so filtering
the token changes what clients see, not what the account may do.

**To tell a service account from a person**, check
`account_kind == ACCOUNT_KIND_SERVICE_ACCOUNT` or a non-empty
`service_identity`. Do not test for an empty `roles` list; that stopped working
in 0.6.4.

**To show who is calling**, call `Authenticate{bearer_token}` or
`ValidateToken`. `Principal.username` (field 15) is the account's username and
`Principal.attributes` holds its redacted profile attributes. Both are read from
the live user row at the moment of the call, never from a token claim, so a
rename or a suspension shows up at once. A credential with no UDB user row (an
external IdP or OIDC token) has an empty `username`.

## Asking "may this user do that?"

`CheckAccess` answers for the **caller**. If an ordinary caller puts someone
else's id in `user_id`, the broker still evaluates the caller. The response
always tells you whom it evaluated, in `evaluated_subject` (field 6). Check it
whenever the answer matters.

A backend that gates its own endpoints on UDB policy needs the user's answer,
not its own. A service account gets it when all of these hold:

1. The caller is a **service account** (a service-account credential carrying
   its grant's service identity). A person, or a raw API key, holding the
   scope is refused with `PERMISSION_DENIED`
   (`delegated_check_requires_service_account`).
2. Its token carries the scope **`udb:authz:check-access-for`**, granted
   literally in the typed grant. `udb:admin` and `udb:*` do not imply it.
3. `user_id` names an **ACTIVE person in the token's tenant**. An unknown id,
   a person in another tenant and a non-person all get the same
   `PERMISSION_DENIED` (`delegated_subject_not_in_tenant`), so the call cannot
   be used to probe which ids exist elsewhere.

The grant for such a service usually approves both
`udb:authz:check-access` (the method's own scope) and
`udb:authz:check-access-for`.

```json
// AuthzService.CheckAccess, with the service account's bearer
{ "user_id": "<the person's user id>", "object": "chart", "action": "read" }

// response
{ "allowed": true, "evaluated_subject": "<the person's user id>", "reason": "..." }
```

What the broker evaluates:

- the person, in the **token's** tenant and project, with the person's own role
  bindings;
- none of the caller's identities, roles, scopes or service identity. A policy
  written for your service never answers for the user;
- no token scopes for the person, so a policy that declares `required_scopes`
  never allows a delegated check.

Every delegated decision, allow and deny alike, is written to the
access-decision audit with the caller in `evaluated_by` and
`evaluated_by_service_identity`, and published as
`udb.authz.check.access.delegated.v1`.

`BatchCheckPermissions` delegates in the same way. `Authorize` does not: it
always decides for the caller.

## Acting in another tenant

A service account belongs to one home tenant and has one grant there. To act in
another tenant, that tenant's administrator approves it with a
**ServiceAccountTenantMembership**, and the account exchanges its home bearer
for a short-lived token in that tenant.

### The target tenant approves

The target tenant's administrator calls
`CreateServiceAccountTenantMembership` with a token **of the target tenant**.
A home-tenant administrator alone cannot approve its own account into someone
else's tenant.

```json
// AuthnService.CreateServiceAccountTenantMembership (target-tenant admin)
{
  "target_tenant_id": "<target tenant UUID: must be the caller's own tenant>",
  "user_id": "<the service account's user id>",
  "home_tenant_id": "<the account's home tenant UUID>",
  "target_project_id": "<project in the target tenant>",
  "approved_scopes": ["udb:authz:check-access"],
  "reason": "billing sync for clinic B",
  "expires_at": "2027-01-31T00:00:00Z"
}
```

- `approved_scopes` are validated like grant scopes (no admin, owner or
  wildcard scopes) and can never include `udb:authn:exchange-tenant-token`.
- `expires_at` is required and at most one year away.
- The account must be an ACTIVE service account with an ACTIVE grant in its
  home tenant.

The same administrator lists memberships with
`ListServiceAccountTenantMemberships` and ends one with
`RevokeServiceAccountTenantMembership`. Approvals and revocations are audited
in both tenants (`udb.authn.service.membership.changed.v1`).

### The account exchanges its home bearer

The account's own grant must approve `udb:authn:exchange-tenant-token`. With its
**home** bearer it calls:

```json
// AuthnService.ExchangeServiceTokenForTenant (service account, home bearer)
{
  "target_tenant_id": "<target tenant UUID>",
  "target_project_id": "<the membership's project>",
  "requested_scopes": ["udb:authz:check-access"]
}
```

The reply carries `access_token`, `expires_at_unix`, `tenant_id`, `project_id`,
`scopes` and `membership_id`. The token:

- names the account as its subject and the target tenant and project as its
  tenant and project;
- carries the requested scopes, which must be a subset of the approved ones
  (send none to get all of them);
- keeps the home grant's service identity, and adds the claims
  `home_tenant_id` and `mbr` (`<membership_id>:<revision>`);
- lives at most **15 minutes**, and never past the membership's expiry;
- has no refresh token and no session. Exchange again when it runs out;
- cannot be exchanged again (no chained exchanges).

Send it with `x-tenant-id` set to the target tenant, like any bearer of that
tenant. Exchanges are audited in both tenants
(`udb.authn.service.token.exchanged.v1`).

### Revocation is immediate

Every use of an exchanged token re-checks the membership (still ACTIVE, same
revision), the home grant and the account. Revoking the membership, revoking
the home grant or deactivating the account stops tokens already issued, on
their next call. Approving the account again creates a new revision; tokens of
the old one stay dead.

An exchange with no membership, or an expired or revoked one, gets a single
`PERMISSION_DENIED` (`service_tenant_membership_required`). A human, an API key
or an exchanged token gets `PERMISSION_DENIED` too: only a service account's
home bearer can exchange.

### Login hints never grant anything

`tenant_hint` and `project_hint` on `Login` or `Authenticate` only pick where to
look up a credential that has no tenant of its own. They never give a token
access to a tenant. When `Authenticate` had to fall back to a hint because the
bearer has no `tenant_id` claim, it says so in `AuthnResponse.warnings` and logs
it on `udb.audit.authn`. A token proves a tenant only through its claims, and a
service account reaches another tenant only through a membership.

A client certificate composes with an exchanged token when the certificate is
bound to the account's home tenant.

## Users with a phone number and no email

`CreateUser` takes `phone` (field 13), an E.164 number: `+` and then 7 to 15
digits, not starting with 0. A user needs a username plus an email or a phone,
or both.

- **Phone only.** The verification code goes out by SMS as a
  `OTP_TYPE_PHONE_VERIFICATION` OTP (`CreateUserResponse.otp_id`).
  `VerifyOTP{otp_id, code}` activates the account and marks the phone verified.
- **Phone-only invite** (`password_setup_required: true`). The password-reset
  code goes by SMS. `ResetPassword` with it sets the first password, activates
  the account and marks the phone verified.
- **Email and phone.** The email is verified as before. The phone is stored
  unverified.

A phone-only `CreateUser` is refused with `FAILED_PRECONDITION`
(`sms_delivery_unavailable`) before anything is written when the broker cannot
send SMS: `UDB_OTP_DELIVERY_WEBHOOK_URL` is not set and
`UDB_NOTIFICATION_DELIVERY_PROVIDERS_JSON` configures no `SMS` provider. Signing in with a phone number is
not supported yet; the user signs in with the username.

A request with neither contact fails `INVALID_ARGUMENT` with field violations
on `email` and `phone`. Match on those fields, not on the message text.

## LockService: "the lock is busy" is not an error

`AcquireLock{tenant_id, lock_name, owner_id, lease_ttl_seconds, metadata_json}`
either grants the lease or tells you someone else holds it.

**Success** returns:

| Field | Meaning |
|---|---|
| `acquired` | `true` |
| `fencing_token` | a per-lock number, strictly greater than any token this lock granted before. Pass it to fenced writes. |
| `lock_name` | the lock you asked for |
| `expires_at_unix` | when the lease lapses unless you renew it |
| `message` | `lock acquired` |

The lease lasts `lease_ttl_seconds`: 30 seconds when you send 0 or less, at
most 3600. The same `owner_id` acquiring again refreshes its own lease and gets
a new, higher fencing token. A lease that has expired can be taken over by
anyone.

**Held by another owner.** When another `owner_id` holds a live lease, the call
fails with gRPC **`ALREADY_EXISTS`**. The message is
`lock is already held by another owner`, and the error detail (trailer
`udb-error-detail-bin`) carries `capability_required: "lock_already_held"`.
That means **busy**, not broken:

- retry with exponential backoff and jitter, bounded by your own deadline or
  attempt budget;
- or give up and report "busy" to your caller;
- never treat it as a failure of the lock service.

The detail is marked not retryable, so the SDKs do **not** retry it for you.
Your code decides whether waiting is worth it.

Other refusals mean something else:

| Code | Reason | What to do |
|---|---|---|
| `RESOURCE_EXHAUSTED` | the tenant already holds 256 active locks | release locks you no longer need |
| `INVALID_ARGUMENT` | empty `lock_name` or `owner_id` | fix the request |
| `FAILED_PRECONDITION` (`stale_fencing_token`) | a fenced write presented a token older than the lock's current one | you lost the lease; stop writing and acquire again |

Renew with `RenewLock` before `expires_at_unix`, and release with
`ReleaseLock` when you are done.

## Where to go next

This page is also bundled with the UDB agent skill, so these are repository
paths:

- `docs/security.md`: request context, the authorization surfaces and identity
  settings.
- `docs/native-services.md`: every native service, including AuthnService,
  AuthzService and LockService.
- `docs/generated/authn-authz-rpc-inventory.md`: the scope each RPC requires.
- `docs/error-reasons.md`: every refusal reason with its fix.
