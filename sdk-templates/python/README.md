# UDB Python SDK generation templates

These templates are consumed by the UDB Rust generator
(`udb sdk generate --lang python`, implemented in `src/cli/sdk_gen.rs`). Every
file here is materialized at the mirror path under `sdk/python/`:

- a `.tmpl` file is **rendered** (placeholders + per-RPC / per-service blocks
  expanded) and the `.tmpl` suffix is stripped;
- any other file is **copied verbatim**;
- `sdkgen.yaml` / `sdkgen.toml` and dotfiles are skipped.

The list of RPCs/services the blocks iterate comes from the embedded proto
`FileDescriptorSet` — proto is the single source of truth, so the generated
surface can never drift from the wire contract. Nothing is hand-listed.

## What renders where

| Template                                        | Renders to                                   |
| ----------------------------------------------- | -------------------------------------------- |
| `udb_client/generated_client.py.tmpl`           | `sdk/python/udb_client/generated_client.py`  |
| `udb_client/_cli.py.tmpl`                        | `sdk/python/udb_client/_cli.py`              |

The package manifest entry that exposes the `udb` console script lives in the
committed `sdk/python/pyproject.toml` (it is **not** templated, because it is a
hand-maintained file with other content). The generator does not touch it; it
was wired once, minimally and additively:

```toml
[project.scripts]
udb = "udb_client._cli:main"
```

`sdk/python/udb_client/__init__.py` was likewise edited in place to re-export
`GeneratedClient`, `RetryPolicy`, and `UdbDetailedRpcError` behind a defensive
`try/except ImportError`, so a checkout without the generated file still imports.

## The generated client (`generated_client.py`)

A **forwarding / robustness layer** over the buf-generated gRPC stubs. For each
service it emits a `<ServiceName>Client` (e.g. `DataBrokerClient`,
`AuthnServiceClient`); for each RPC it emits a wrapper method (snake_case) that
forwards to the underlying stub method of the same `PascalCase` name and adds:

- **per-call deadline / timeout** (constructor default, per-call override);
- **retry** with exponential backoff + jitter on `UNAVAILABLE`,
  `RESOURCE_EXHAUSTED`, and `DEADLINE_EXCEEDED` only for read-only RPCs
  (`RetryPolicy`). Unary and the pre-first-message phase of server-streaming
  retry; client-streaming and bidi never auto-retry (request stream is not
  replayable);
- **TLS / credentials** wiring (`secure`, `root_certificates`,
  `channel_credentials`, `call_credentials`) or a caller-supplied `channel`;
- **metadata**: the full `udb_client.metadata.Metadata` header set, plus
  `authorization: Bearer …` / `x-udb-api-key` / a per-call `x-request-id`;
- **typed error mapping**: unpacks the `udb-error-detail-bin` trailer into
  `UdbDetailedRpcError`, else raises the existing `udb_client.exceptions.UdbRpcError`.

`GeneratedClient` is an aggregate facade that builds one sub-client per service
over a shared channel/config — it covers every RPC across every service.

### Stub discovery (why per-RPC blocks need no message symbols)

The gRPC stub *module filename* is the proto file basename (e.g.
`authn_service_pb2_grpc`), which is **not** derivable from the service name. So
the base client imports the service package (`{{SERVICE_PKG}}`) and scans its
`*_pb2_grpc` submodules for the `<ServiceName>Stub` class at runtime. This reuses
the buf serializers, so per-RPC blocks only need `{{RPC_NAME}}`/`{{RPC_KIND}}` —
they never resolve request/response message symbols.

This **composes with** and never overwrites the hand-written
`client.py`, `auth.py`, `metadata.py`, `negotiation.py`, `exceptions.py` — it
imports `Metadata`, `UdbRpcError`, `UdbConfigurationError` from them.

## The CLI launcher (`_cli.py`)

Installing the wheel makes a version-matched `udb` binary available via the
`udb = "udb_client._cli:main"` console script. The launcher resolves, in order:

1. `UDB_BIN` if set;
2. an `udb` on `PATH` whose `--version` matches `{{UDB_VERSION}}` (and is not the
   launcher itself);
3. a previously cached binary under the per-user cache dir
   (`%LOCALAPPDATA%\udb\bin\<version>` / `$XDG_CACHE_HOME/udb/bin/<version>`);
4. the matching GitHub release asset
   (`https://github.com/fahara02/udb/releases/tag/v{{UDB_VERSION}}`,
   `udb-v{{UDB_VERSION}}-<os>-<arch>[.exe]`), downloaded + cached.

Set `UDB_SKIP_DOWNLOAD=1` to forbid network fetches (PATH/cache only). The
`{{UDB_VERSION}}` placeholder is baked in at generation time so the CLI always
matches the SDK release.

## Placeholders used here

Scalars: `{{UDB_VERSION}}`, `{{PROTOCOL_VERSION}}`, `{{GENERATED_NOTE}}`,
`{{RPC_COUNT}}`, `{{SERVICE_COUNT}}`.

Per-service block (`@@UDB_SERVICE_BEGIN … @@UDB_SERVICE_END`):
`{{SERVICE_NAME}}`, `{{SERVICE_PKG}}`, `{{SERVICE_FULL}}`, `{{SERVICE_RPC_COUNT}}`.

Per-RPC block (`@@UDB_RPC_BEGIN [service=… kind=…] … @@UDB_RPC_END`):
`{{RPC_NAME}}`, `{{RPC_SNAKE}}`, `{{RPC_PATH}}`, `{{RPC_INPUT}}`,
`{{RPC_INPUT_PKG}}`, `{{RPC_OUTPUT}}`, `{{RPC_OUTPUT_PKG}}`, `{{RPC_KIND}}`.
The `kind=` filter (`unary`, `server_streaming`, `client_streaming`, `bidi`)
selects the correct method shape per RPC.

## Customizing

- Tune retry defaults in `RetryPolicy` (in the `generated_client.py.tmpl`).
- Add headers in `_ServiceClientBase._call_metadata`.
- Change the release asset naming convention in `_cli.py.tmpl` `_asset_name()`
  if the build's release assets use a different scheme.
- Re-run `udb sdk generate --lang python` after editing a template; do not edit
  the rendered `sdk/python/udb_client/generated_client.py` / `_cli.py` directly.
