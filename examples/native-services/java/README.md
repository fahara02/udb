# UDB native services — Java example

A progressive, simplest→advanced tour of UDB's **native control-plane services**
from Java, symmetric with the [Go](../go) / [Python](../python) /
[TypeScript](../typescript) examples:

1. register a native **user**
2. define authorization — RBAC **role → assignment → allow policy**
3. the everyday **access check** (`CheckAccess`)
4. mint an **API key**, then **authenticate** it
5. (advanced) request a **Stage-2 native-access grant**

It drives the hand-written SDK surface: the [`UdbProject`](../../../sdk/java)
facade (shared identity over data + auth + apikey + tenant + notification +
analytics) and the `UdbAuthClient` ergonomics (`authenticate*` / `can` / `require`
/ `explain` / `batchCan` / `checkAccess` / `nativeAccess`). Provisioning RPCs
(`createUser` / `createRole` / `assignRole` / `putAuthzPolicy`) are reached
through the facade's raw stubs (`auth.authn()`, `udb.authz().authz()`).

The single program is both the **admin flow** (provision) and the **consumer
flow** (authenticate + authorize) — it prints `export UDB_API_KEY=…` at the end
so the other-language consumer examples can authenticate the same key.

## 1. Start a broker with native auth

```bash
docker compose -f docker-compose.integration.yml up -d --wait postgres kafka redis
docker compose -f docker-compose.integration.yml --profile broker up -d --wait udb
# broker gRPC: localhost:50051 (UDB_ABAC_DEFAULT_ALLOW=true so admin RPCs authorize)
```

## 2. Build the SDK, then run the example

The example depends on the local SDK artifact `dev.udb:udb-java-client`, which
add-sources the generated stubs under `sdk/java/gen`. Install it once:

```bash
cd ../../../sdk/java
mvn install -DskipTests            # publishes dev.udb:udb-java-client to ~/.m2
```

Then run the example (Maven 3.9+, JDK 17):

```bash
cd ../../examples/native-services/java
UDB_TARGET=127.0.0.1:50051 mvn -q compile exec:java
```

`UDB_TARGET` defaults to `localhost:50051` if unset.

Expected output (ids vary):

```
1) registered user usr_… (alice_…)
2) role reader_… assigned to user; allow policy on invoice/data.select added
3) check data.select on invoice → allowed=true
   check data.delete on invoice → allowed=false (no policy grants it)
4) api key authenticated → principal user_id=usr_… scopes=[data:read]
   minted dev API key → export UDB_API_KEY=udbk_…
5) native grant: role=… session_vars=… (open a JDBC conn on grant.getDsn())
```

## The wire/identity contract

Like the other SDKs, every call carries the same metadata headers (tenant, user,
purpose, correlation, scopes, service-identity, project, catalog-version) so the
broker sees one consistent identity/scope context. The `can()` / `nativeAccess()`
helpers additionally forward the caller's scopes as the request's
`requested_scopes`, so scope-narrowing matches the Go/Python/TS SDKs exactly.

## What's exercised in the SDK

- `UdbProject` facade + `UdbProjectConfig` builder (shared metadata, `close()`).
- `UdbAuthClient.authn()` / `.authz()` raw stubs for provisioning.
- `UdbAuthClient.checkAccess()`, `.authenticateApiKey()`, `.nativeAccess()`.
- `udb.createApiKey(...)` convenience wrapper.

`AuthzCache` (TTL decision cache), `require`/`explain`/`batchCan`, and the
`tenant()` / `notification()` / `analytics()` facets are also available on the
SDK; see the SDK sources for usage.
