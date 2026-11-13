# UDB Documentation


```text
┌────────────────────────────────────────────────────────────────────────────┐
│                                                                            │
│    ██    ██  ██████   ██████                                               │
│    ██    ██  ██   ██  ██   ██                                              │
│    ██    ██  ██   ██  ██████                                               │
│    ██    ██  ██   ██  ██   ██                                              │
│     ██████   ██████   ██████                                               │
│                                                                            │
│    UNIVERSAL DATA BROKER                                                   │
│    gRPC data plane | native control plane | tenant/project scope guard     │
│                                                                            │
│    crate v0.6.16 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
These pages document UDB, the Universal Data Broker — one typed API in
front of many databases and the backend services (auth, secrets, search, jobs,
storage, and more) you'd otherwise stitch together yourself. This page is the
map: find what you're trying to do in the left column and follow the link.

**Start here:** new to UDB? Read the [project overview](../README.md) and follow
its [Quick Start](../README.md#quick-start), then
[integration.md](integration.md) to connect an application. When you're ready
for more, jump straight to the guide that matches your task below.

This is the public documentation for UDB 0.6.16. The guides are grouped by what you're doing: understanding the architecture,
annotating your protos, integrating an app, using the native services, running
UDB in production, securing it, testing it, and picking an SDK.

## Guides

Each row is a task. Read across to the guide that covers it.

| Need | Read |
|---|---|
| Project overview | [../README.md](../README.md) |
| Architecture, request flow, routing, pooling, events, and backend capability | [architecture.md](architecture.md) |
| API rules, route naming, OpenAPI operation IDs, SDK alias policy | [api-rules.md](api-rules.md) |
| Proto annotations | [annotations.md](annotations.md) |
| Application integration | [integration.md](integration.md) |
| Migrating from a hand-written udb wrapper: what the SDK and broker now provide | [migrating-from-a-wrapper.md](migrating-from-a-wrapper.md) |
| Error reasons: the stable `UDB_*` codes every refusal carries, with fixes | [error-reasons.md](error-reasons.md) |
| Wire types: the JSON shape of every PostgreSQL column type in a record | [wire-types.md](wire-types.md) |
| Native auth, authz, IdP, storage, assets, WebRTC, and SDK facades | [native-services.md](native-services.md) |
| Production readiness, config, runbooks, SLOs, and validation | [operations.md](operations.md) |
| Upgrading a database that already has data: pre-flight checks, approval plans, rehearsing on a clone, and what to do when startup fails after a migration applied | [upgrading.md](upgrading.md) |
| Changing YOUR OWN model once it holds rows: the day-2 loop, worked end to end | [examples/schema_change](../examples/schema_change) |
| From-scratch hardened/enterprise bring-up: minimal env set, TLS/mTLS, auth-plane exposure, ABAC vs policy_rules, pooler-safe DSN, SDK mTLS | [enterprise-deployment.md](enterprise-deployment.md) |
| Request context, identity, authorization, sensitive data, and compliance profiles | [security.md](security.md) |
| Service accounts acting for users: what a service token carries, CheckAccess for a user, acting in another tenant, phone-only users, lock contention | [service-accounts.md](service-accounts.md) |
| Auth operator runbooks: readiness failures, login and MFA failures, key rotation, denial spikes, policy drift | [auth-runbooks.md](auth-runbooks.md) |
| High-availability deployment: the 3-replica reference architecture | [deploy-ha.md](deploy-ha.md) |
| Service-level objectives: per-RPC latency budgets and the metrics that measure them | [slo.md](slo.md) |
| Generating typed ORM entity/repository models with `udb orm scaffold` | [orm-scaffold.md](orm-scaffold.md) |
| PHP performance: warm gRPC channels, persistent workers, a local UDB sidecar | [php-perf.md](php-perf.md) |
| CI architecture: what CI runs, on which event, and which checks gate merges | [ci-architecture.md](ci-architecture.md) |
| Testing | [testing.md](testing.md) |
| SDKs | [../sdk/README.md](../sdk/README.md) |

## Consolidated Topics

We used to keep a separate page for each narrow topic. Those pages now live
inside the main guides above, so there's one place to look per subject. If you
came here searching for one of these, the table points you to its new home.

| Topic | Consolidated in |
|---|---|
| Architecture doctrine, backend matrix, routing, pooling, event contracts, canonical stores | [architecture.md](architecture.md) |
| Production readiness, runbooks, SLOs, auth production config, HA validation, load/soak, performance baseline | [operations.md](operations.md) |
| Auth compliance profiles, enterprise identity, sensitive-field handling, request security | [security.md](security.md) |
| SCIM, WebSocket signalling, SDK facades, native access, storage/assets/WebRTC | [native-services.md](native-services.md) |
| SDK generation, conformance, language packages, PHP/Laravel publishing | [../sdk/README.md](../sdk/README.md) |

## Diagrams

Prefer a picture? These diagrams show how requests move through UDB. They live
in [assets](assets/):

- [architecture-pipeline.svg](assets/architecture-pipeline.svg)
- [request-flow.svg](assets/request-flow.svg)
- [control-plane.svg](assets/control-plane.svg)

## Generated Docs

These files are produced automatically from UDB's service descriptors, so they
always match the running build. Don't hand-edit them — regenerate them with the
commands below.

- [generated/native-services.md](generated/native-services.md) - native service and RPC table: scopes, accepted credentials, tenant rules
- [generated/udb-native-contract.json](generated/udb-native-contract.json) - descriptor-derived service contract
- [generated/authn-authz-rpc-inventory.md](generated/authn-authz-rpc-inventory.md) - auth RPC inventory
- [generated/authn-authz-sensitive-fields.md](generated/authn-authz-sensitive-fields.md) - sensitive-field inventory

Regenerate descriptor-backed docs with:

```bash
udb native docs
udb native manifest
```
