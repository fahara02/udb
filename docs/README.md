# UDB Documentation

The maintained UDB documentation set. Every page describes the code that exists
today; the source-of-truth files below are authoritative, and the docs are kept
in step with them.

**Architecture at a glance** — the root README renders three custom diagrams you
can reuse:

- [`assets/architecture-pipeline.svg`](assets/architecture-pipeline.svg) — proto to manifest to generation to runtime to 18 backends
- [`assets/request-flow.svg`](assets/request-flow.svg) — the per-request sequence: authz, admission, IR, execute
- [`assets/control-plane.svg`](assets/control-plane.svg) — public vs. isolated internal listener topology

| Need | Read |
|---|---|
| Current architecture and backend inventory | [architecture.md](architecture.md) |
| Native control plane (Authn/Authz/ApiKey/Tenant/Notification/Analytics) | [native-services.md](native-services.md) |
| Proto annotation contract | [annotations.md](annotations.md) |
| Service and SDK integration | [integration.md](integration.md) |
| Deployment, operations, reload, backup, and incident drills | [operations.md](operations.md) |
| Security, audit, encryption, and supply-chain gates | [security.md](security.md) |
| Test commands and live validation matrix | [testing.md](testing.md) |

The root [README.md](../README.md) explains the project at a high level.
The complete environment template is [../.env.example](../.env.example).

## Source Of Truth

- Backend identity: [../src/backend/mod.rs](../src/backend/mod.rs)
- Compiled plugin inventory: [../src/backend/plugins/mod.rs](../src/backend/plugins/mod.rs)
- Runtime config/env overlay: [../src/runtime/config/mod.rs](../src/runtime/config/mod.rs)
- gRPC service surface: [../src/runtime/service](../src/runtime/service)
- CLI commands: [../src/cli](../src/cli)
- Feature graph: [../Cargo.toml](../Cargo.toml)

Docs should describe the code that exists now. Internal audit notes and private
todo files should not be linked from the public documentation set.
