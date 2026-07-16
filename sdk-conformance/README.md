# UDB SDK Conformance Suite

<p align="center">
  <img src="../docs/assets/udb_logo.svg" alt="UDB logo" width="96">
</p>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.4.8 | protocol v1.0.0</sub>
</p>

The SDK conformance suite keeps Go, Python, TypeScript, Java, C#, and PHP aligned
on the same public client contract.

## Run

```bash
node sdk-conformance/run.mjs
node sdk-conformance/run.mjs go python
node sdk-conformance/run.mjs metadata typescript python go csharp java php
```

When languages or focused contract gates such as `error-details` are explicitly
named, missing tooling or failing tests are treated as failures. CI uses
explicit language names.

## What It Checks

| Area | Contract |
|---|---|
| Metadata | All SDKs emit the same UDB metadata header names and generated alias/operationId identity maps |
| Credentials | Bearer tokens use `authorization`; API keys use `x-api-key` |
| Authz | SDK helpers populate requested scopes consistently |
| Cache | Authz decision TTL behavior is consistent |
| Policy bundles | Signed bundle verification behaves the same |
| Refresh | Refresh and credential update paths preserve outbound metadata |
| Facades | Native-service helpers route to the expected service clients |

## Live Checks

Broker-backed live checks are a required CI gate in `sdk-live-conformance`.
The job starts Postgres + UDB, seeds the first user with:

```bash
udb auth bootstrap user --username sdk-live --email sdk-live@example.com \
  --password CorrectHorse1! --tenant sdk-live --project default
```

Then it runs the env-gated TypeScript SDK test with `UDB_LIVE_SDK_TESTS=1`,
covering real broker login, refresh-token rotation, refresh single-flight, and
credential hot-swap against the live gRPC listener.
