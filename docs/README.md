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
│    crate v0.3.6 | protocol v1.0.0                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
This directory contains the public documentation for UDB 0.3.6. The guides are
organized around the product surface: architecture, annotations, integration,
native services, operations, security, testing, and SDKs.

## Guides

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

Older narrow pages have been folded into the main guides:

| Topic | Consolidated in |
|---|---|
| Architecture doctrine, backend matrix, routing, pooling, event contracts, canonical stores | [architecture.md](architecture.md) |
| Production readiness, runbooks, SLOs, auth production config, HA validation, load/soak, performance baseline | [operations.md](operations.md) |
| Auth compliance profiles, enterprise identity, sensitive-field handling, request security | [security.md](security.md) |
| SCIM, WebSocket signalling, SDK facades, native access, storage/assets/WebRTC | [native-services.md](native-services.md) |
| SDK generation, conformance, language packages, PHP/Laravel publishing | [../sdk/README.md](../sdk/README.md) |

## Diagrams

The maintained diagrams live in [assets](assets/):

- [architecture-pipeline.svg](assets/architecture-pipeline.svg)
- [request-flow.svg](assets/request-flow.svg)
- [control-plane.svg](assets/control-plane.svg)

## Generated Docs

- [generated/native-services.md](generated/native-services.md) - native service table
- [generated/udb-native-contract.json](generated/udb-native-contract.json) - descriptor-derived service contract
- [generated/authn-authz-rpc-inventory.md](generated/authn-authz-rpc-inventory.md) - auth RPC inventory
- [generated/authn-authz-sensitive-fields.md](generated/authn-authz-sensitive-fields.md) - sensitive-field inventory

Regenerate descriptor-backed docs with:

```bash
udb native docs
udb native manifest
```
