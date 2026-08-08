# Call UDB's native services — from any SDK

Most UDB examples show the **DataBroker**: the data plane where you Select and
Upsert your proto-defined rows. This folder shows the *other* half — UDB's
built-in **control-plane services**: native **authentication** (users, API keys),
**authorization** (RBAC roles, policies, access checks), and the Stage-2 **native
database fast-path** grant. These are the RPCs you use to *provision* who's
allowed to do what, and to *ask* "is this caller allowed?" on every request.

The same flow is written six ways — Go, Python, PHP, TypeScript, C#, Java — so
you can copy the one in your language. They all talk to the same broker on the
same port; only the syntax differs.

## The 30-second version

Bring up a broker, run the admin flow once to mint an API key, then run the
consumer flow with that key:

```bash
# 1. Broker with native auth (the repo's integration stack is the easy path):
docker compose -f docker-compose.integration.yml up -d --wait postgres kafka redis
docker compose -f docker-compose.integration.yml --profile broker up -d --wait udb
#    → broker gRPC on localhost:50051

# 2. Admin flow — provision a user/role/policy and mint a key.
#    (Go runs admin + consumer in one program, so this is the whole tour.)
cd examples/native-services/go && go run .
#    → prints:  export UDB_API_KEY=udbk_...

# 3. Consumer flow — authenticate that key and make authz decisions.
#    Copy the printed key, then run any language's consumer:
cd ../python && UDB_API_KEY=udbk_... python main.py
```

## The two flows

Each language has two halves. Run the **admin** half once, copy the
`UDB_API_KEY` it prints, then run the **consumer** half as many times as you
like.

- **Admin flow** — provisioning. It drives the raw `Authn` / `Authz` / `ApiKey`
  service stubs directly (creating users and policies is an admin concern):
  register a user → create a role → assign it → put an allow policy → check
  access → mint an API key. Ends by printing `export UDB_API_KEY=…`.
- **Consumer flow** — what your app does on every request. It uses the
  `UdbAuthClient` wrapper's first-class surface: `authenticate` an API key →
  `can` / `check_access` for authz decisions → `native_access` for the Stage-2
  DB grant. Feed it the key the admin flow minted.

| Folder | Admin flow | Consumer flow | Verified with |
| --- | --- | --- | --- |
| [`go/`](go) | `go run .` — one program does register → policy → check → mint key → authenticate → native-access | (same program) | `go build` / `go vet` |
| [`python/`](python) | `python admin.py` | `python main.py` | `py_compile`, imports resolve |
| [`php/`](php) | `php admin.php` | `php main.php` | `php -l` (runtime needs `ext-grpc`) |
| [`typescript/`](typescript) | `npx tsx admin.ts` | `npx tsx main.ts` | `npm install` |
| [`csharp/`](csharp) | `dotnet run -- admin` | `dotnet run -- consumer` | `dotnet build` (net8.0) |
| [`java/`](java) | `Main.java` — one program, full tour | (same program) | `javac` against SDK + gen stubs |

Go and Java each run the whole tour (admin + consumer) in a single program, so
there's no separate consumer step for those two.

## The five steps you'll see printed

Whichever language you run, the tour walks the same arc, simplest to advanced:

1. **Register a user** — `CreateUser` on the Authn service.
2. **Define authorization** — RBAC: `CreateRole` → `AssignRole` →
   `PutAuthzPolicy` (an allow policy for `data.select` on `invoice`).
3. **Check access** — `CheckAccess` returns `allowed=true` for `data.select`,
   `false` for `data.delete` (nothing grants it).
4. **Machine credentials** — `CreateApiKey`, then authenticate the plain key back
   to a resolved principal. This is the key you export for the consumer flow.
5. **Native DB fast-path** — `GetNativeAccess` (Stage 2). When the server has it
   configured, this runs the same authz decision and returns a short-lived
   restricted DSN plus `SET LOCAL` session variables, so you can open a *direct*
   database connection and still have the broker's row-level security apply. If
   the server hasn't enabled it, the example says so and moves on — it's optional.

## Per-language setup

Go and Java build against the in-repo SDK via a local path (`replace` /
relative import) with no published release. The others need the SDK stubs and
transport for their language:

```bash
# Python — generate the udb.* stubs, install grpc:
cd sdk/python && buf generate --include-imports && pip install grpcio
cd ../../examples/native-services/python && UDB_API_KEY=udbk_... python main.py

# TypeScript:
cd examples/native-services/typescript && npm install
UDB_API_KEY=udbk_... npx tsx main.ts

# PHP — needs the grpc PHP extension; composer path-repo points at ../../../sdk/php:
cd examples/native-services/php && composer install
UDB_API_KEY=udbk_... php main.php
```

## Configuration

Every language reads the same two environment variables:

| Env var | What it is | Default |
| --- | --- | --- |
| `UDB_TARGET` | broker gRPC address | `127.0.0.1:50051` (Go uses `localhost:50051`) |
| `UDB_API_KEY` | the key the admin flow minted | (consumer flows only) |

The examples send a fixed identity in gRPC metadata: tenant code `acme`, project
`billing`, purpose `control-plane`, scopes `udb:*`. That broad scope is what lets
the example call admin RPCs. In a real deployment you would instead give the
caller the specific per-RPC scopes (`udb:<service>:<method>`) each native call
requires — native authorization is by **token scope** (`endpoint_security`), not
by a Casbin data policy, so `UDB_ABAC_DEFAULT_ALLOW` (a *data-plane* dev switch)
does not affect these calls.

> **Listener note (important).** Native services listen on a **separate,
> loopback-by-default** listener — `UDB_AUTH_GRPC_ADDR`, default `127.0.0.1:50061`
> (the public data port **+10**). This example uses `127.0.0.1:50051` because the
> bundled integration stack **co-locates** the auth plane on the public port. On a
> default deployment, dial native services at `:50061`; calling them at `:50051`
> returns `UNIMPLEMENTED`, and calling a loopback `:50061` from another host
> returns `ECONNREFUSED` (open it with `UDB_AUTH_GRPC_ADDR=0.0.0.0:50061`).

## Common mistakes this example prevents

- **Confusing the control plane with the data plane.** These RPCs manage
  *identity and access* — they aren't how you read or write your rows. That's the
  DataBroker (`Entity(...).Select/Upsert`); see the `go_enterprise` example next
  door for the data path.
- **Skipping the admin flow.** The consumer flow needs a real `UDB_API_KEY`.
  Mint one with the admin flow (or Go's single program) first, then export it.
- **Expecting login to work on a brand-new user.** A freshly-created user is
  `PENDING_VERIFICATION` until its email OTP is verified out-of-band, so the
  examples authenticate with an **API key** instead — the simplest
  machine-to-machine path, no verification needed.
- **Assuming step 5 always mints a grant.** The Stage-2 native-access grant is
  optional server config. "No native grant minted" means the server hasn't
  enabled it, not that anything failed.
- **Two authorization surfaces.** A `PERMISSION_DENIED` on a native RPC means your
  token is missing the scope `udb:<service>:<method>` — a different fix than a
  data-CRUD deny (which needs a `udb_authz.policy_rules` row; see
  `docs/security.md` and `udb authz seed`).
- **A service account needs a grant.** To use a `SERVICE_ACCOUNT` identity you
  must `CreateServiceAccountGrant` for it (approved scopes) *before* it can
  authenticate or mint an API key — without an active grant it fails closed. Then
  bind it to your data role (`udb auth role bind`) so it can also do CRUD.
