# UDB native services — examples (Go · PHP · TypeScript · Python · C# · Java)

Simplest→advanced examples of using UDB's **native control-plane services** —
authentication (users, sessions, API keys), authorization (RBAC/ABAC/ReBAC roles,
policies, access checks), and the Stage-2 native database fast-path — from each
SDK. One dedicated folder per language.

Each language folder now has **two** examples — an **admin flow** (provision
users/roles/policies/keys) and a **consumer flow** (authenticate + authorize),
symmetric with Go:

| Folder | Admin flow | Consumer flow | Verified |
| --- | --- | --- | --- |
| [`go/`](go) | `main.go` — register → role/assign/policy → check → mint key → authenticate → native-access | (same program) | ✅ `go build`/`vet` |
| [`python/`](python) | `admin.py` | `main.py` | ✅ `py_compile`, imports resolve |
| [`php/`](php) | `admin.php` | `main.php` | ✅ `php -l` (runtime needs `ext-grpc`) |
| [`typescript/`](typescript) | `admin.ts` | `main.ts` | ✅ `npm install` (39 pkgs) |
| [`csharp/`](csharp) | `Program.cs admin` | `Program.cs consumer` | ✅ `dotnet build` (net8.0) |
| [`java/`](java) | `Main.java` — register → role/assign/policy → check → mint key → authenticate → native-access | (same program) | ✅ `javac` against SDK + gen stubs (mvn N/A in env) |

- **Admin flow** drives the raw Authn/Authz/ApiKey service stubs (provisioning is
  an admin concern) and prints `export UDB_API_KEY=…` at the end.
- **Consumer flow** uses the `UdbAuthClient` wrapper's first-class surface
  (`authenticate` → `can`/`check_access` → `native_access`) — what an app does on
  every request. Feed it the key the admin flow minted.

Run the admin flow once, copy the printed `UDB_API_KEY`, then run the consumer
flow (any language).

## 1. Start a broker with native auth

The repo integration stack is the easy path:

```bash
docker compose -f docker-compose.integration.yml up -d --wait postgres kafka redis
docker compose -f docker-compose.integration.yml --profile broker up -d --wait udb
# broker gRPC: localhost:50051  (UDB_ABAC_DEFAULT_ALLOW=true so admin RPCs authorize)
```

## 2. Run an example

### Go (full flow — no extra setup)
```bash
cd go && go run .
```
Prints the minted API key path in step 4; copy it to `UDB_API_KEY` for the other
languages. (Local `replace` points the module at `../../../sdk/go`.)

### Python
```bash
cd ../../sdk/python && buf generate --include-imports   # emit the udb.* stubs
pip install grpcio
cd ../../examples/native-services/python
UDB_API_KEY=udbk_... python main.py
```

### TypeScript
```bash
cd typescript
npm install
UDB_API_KEY=udbk_... npx tsx main.ts
```

### PHP
```bash
cd php
composer install            # path-repo points at ../../../sdk/php
UDB_API_KEY=udbk_... php main.php   # requires the grpc PHP extension
```

## The wire/identity contract

All four SDKs attach the same eight metadata headers (tenant, user, purpose,
correlation, scopes, service-identity, project, catalog-version) to every call,
so the broker sees one consistent identity/scope context regardless of language.
The native auth events these calls produce (`udb.authn.*`, `udb.authz.*`,
`udb.apikey.*`) flow to Kafka via the broker's outbox→CDC relay — see
[`analytics/spark`](../../analytics/spark) for the consumer side.
