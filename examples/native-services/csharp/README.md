# UDB native services — C# example

Symmetric with the [`go`](../go), [`python`](../python), [`php`](../php) and
[`typescript`](../typescript) examples: an **admin flow** that provisions
users/roles/policies/keys and a **consumer flow** that authenticates + authorizes
on every request. Both live in one project (`NativeServices.csproj`) and are
selected by the first argument.

The project takes a local `ProjectReference` to the in-repo SDK
(`../../../sdk/csharp/Udb.Client`), which compiles the committed buf-generated
stubs under `sdk/csharp/gen`. No `protoc`/`buf` step is needed.

## 1. Start a broker with native auth

```bash
docker compose -f docker-compose.integration.yml up -d --wait postgres kafka redis
docker compose -f docker-compose.integration.yml --profile broker up -d --wait udb
# broker gRPC: localhost:50051  (UDB_ABAC_DEFAULT_ALLOW=true so admin RPCs authorize)
```

## 2. Run the admin flow

```bash
dotnet run -- admin
```

It registers a user, defines an RBAC role + allow policy, checks access, mints an
API key, authenticates it, and tries a Stage-2 native-access grant. At the end it
prints:

```
   minted dev API key → export UDB_API_KEY=udbk_...
```

## 3. Run the consumer flow

Copy that key, then:

```bash
UDB_API_KEY=udbk_... dotnet run -- consumer
```

The consumer flow uses the `UdbProject` facade and `UdbAuthClient` wrapper —
`AuthenticateApiKeyAsync` → `CanAsync` (TTL-cached) / `ExplainAsync` /
`RequireAsync` / `BatchCanAsync` / `NativeAccessAsync` — i.e. the everyday
per-request authz surface, with `requested_scopes` populated from the shared
metadata scopes.

## The wire/identity contract

The facade attaches the same eight metadata headers (tenant, user, purpose,
correlation, scopes, service-identity, project, catalog-version) to every call as
the other SDKs, so the broker sees one consistent identity/scope context. The
native auth events these calls produce (`udb.authn.*`, `udb.authz.*`,
`udb.apikey.*`) flow to Kafka via the broker's outbox→CDC relay.
