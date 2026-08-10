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
│    crate v0.5.4 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
These pages document UDB, the Universal Data Broker — one typed API in
front of many databases and the backend services (auth, secrets, search, jobs,
storage, and more) you'd otherwise stitch together yourself. This page is the
map: find what you're trying to do in the left column and follow the link.

If you're brand new, start with the [project overview](../README.md) to see what
UDB is and why it exists. When you're ready to build, jump straight to the guide
that matches your task below.

This is the public documentation for UDB 0.5.4. The guides are grouped by what you're doing: understanding the architecture,
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
| Native auth, authz, IdP, storage, assets, WebRTC, and SDK facades | [native-services.md](native-services.md) |
| Production readiness, config, runbooks, SLOs, and validation | [operations.md](operations.md) |
| From-scratch hardened/enterprise bring-up: minimal env set, TLS/mTLS, auth-plane exposure, ABAC vs policy_rules, pooler-safe DSN, SDK mTLS | [enterprise-deployment.md](enterprise-deployment.md) |
| Request context, identity, authorization, sensitive data, and compliance profiles | [security.md](security.md) |
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

- [generated/native-services.md](generated/native-services.md) - native service table
- [generated/udb-native-contract.json](generated/udb-native-contract.json) - descriptor-derived service contract
- [generated/authn-authz-rpc-inventory.md](generated/authn-authz-rpc-inventory.md) - auth RPC inventory
- [generated/authn-authz-sensitive-fields.md](generated/authn-authz-sensitive-fields.md) - sensitive-field inventory

Regenerate descriptor-backed docs with:

```bash
udb native docs
udb native manifest
```
