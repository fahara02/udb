# UDB Documentation

These are the maintained project docs as of 2026-05-31. Older audit logs,
older implementation notes, duplicate security guides, and one-off runbooks were consolidated
because they contradicted the current codebase.

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
