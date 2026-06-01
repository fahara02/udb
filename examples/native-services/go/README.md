# Native services — Go example

A single progressive program (`main.go`) that tours UDB's native control-plane
services from simplest to advanced, over the `udbclient` Go SDK:

1. **Register a user** — `AuthnService.CreateUser` (simplest).
2. **Define authorization** — RBAC: `CreateRole` → `AssignRole` → `PutAuthzPolicy`.
3. **Check access** — `CheckAccess` (allowed vs. denied).
4. **Machine credentials** — `ApiKeyService.CreateApiKey`, then `Authenticate` it.
5. **Native DB fast-path** — `GetNativeAccess` (Stage 2): a short-lived restricted
   DSN + `SET LOCAL` session variables so broker RLS still applies on a direct
   connection (`udbclient.WithNativeTx`).

## Run

Bring up a broker with native auth (the repo integration stack is easiest):

```bash
docker compose -f docker-compose.integration.yml up -d --wait postgres kafka redis
docker compose -f docker-compose.integration.yml --profile broker up -d --wait udb
```

Then:

```bash
cd examples/native-services/go
go run .
```

Expected output (ids vary):

```
1) registered user 7f3e… (alice_169…)
2) role "reader_169…" assigned to user; allow policy on invoice/data.select added
3) check data.select on invoice → allowed=true
   check data.delete on invoice → allowed=false (no policy grants it)
4) api key authenticated → principal user_id=7f3e… scopes=[data:read]
5) native grant: role=… (or "not configured" if Stage-2 native access is off)
```

## Notes

- The module uses a local `replace github.com/fahara02/udb/sdk/go => ../../../sdk/go`,
  so it builds against the in-repo SDK with no published release.
- The broker authorizes the admin RPCs; the integration `udb` service sets
  `UDB_ABAC_DEFAULT_ALLOW=true` so the example's own identity may call them. In a
  real deployment, grant the caller an explicit control-plane policy instead.
- Login (`AuthnService.Login`) is intentionally omitted from the happy path because
  a freshly-created user is `PENDING_VERIFICATION` until its email OTP is verified
  (the OTP is delivered out-of-band). The API-key path needs no verification and is
  the simplest machine-to-machine flow.
