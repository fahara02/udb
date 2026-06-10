# UDB PHP (Laravel) SDK — generation templates

These templates are consumed by UDB's Rust-driven generator
(`udb sdk generate --lang php`). Every file here is materialized at the mirror
path under `sdk/php/`. A `.tmpl` suffix means "render then strip the suffix";
any other file is copied verbatim. The RPC/service list is read from the
embedded proto descriptor set, so the generated surface can never drift from
the wire contract — nothing is hand-listed.

## What renders where

| Template (`sdk-templates/php/`)            | Renders to (`sdk/php/`)              |
|--------------------------------------------|--------------------------------------|
| `src/Generated/GeneratedClient.php.tmpl`   | `src/Generated/GeneratedClient.php`  |
| `bin/udb.tmpl`                             | `bin/udb` (PHP CLI launcher)         |
| `README.md`                                | `README.md` (copied verbatim)        |

The `composer.json` `bin` entry (`"bin": ["bin/udb"]`) is committed directly in
`sdk/php/composer.json` — it is NOT templated, because composer.json is a
hand-maintained manifest carrying the existing Laravel SDK metadata. The
generator never rewrites it.

## How it composes with the existing SDK (never clobbers)

The hand-written Laravel layer is untouched:

- `src/UdbClient.php`, `src/UdbAuthClient.php` — ergonomic, curated entry points.
- `src/UdbMetadata.php` — the eight broker headers (`x-tenant-id`, …).
- `src/Exceptions/*` — `UdbException`, `UdbRpcException`, `UdbConfigurationException`.
- `src/Support/ScopeJoiner.php`, `src/Http/*` middleware, `src/Facades/*`.

The generated `GeneratedClient` lives in the **new** namespace
`Fahara02\UdbLaravel\Generated` (autoloaded by the existing
`Fahara02\UdbLaravel\` → `src/` PSR-4 rule, since it renders into
`src/Generated/`). It **imports and reuses** `UdbMetadata`, `UdbRpcException`,
and `UdbConfigurationException`; it does not redefine them.

`GeneratedClient` is the exhaustive, every-RPC surface across **all** services
(`DataBroker`, `AuthnService`, `AuthzService`, `ApiKeyService`,
`TenantService`, `NotificationService`, `AnalyticsService`). Each unary RPC is a
wrapper method (`select()`, `upsert()`, …) that forwards to the buf-generated
stub method (`Select`, `Upsert`, …) and adds:

- per-call deadline (`deadline_ms`, microseconds),
- retry with exponential backoff + full jitter on transient gRPC codes
  (`UNAVAILABLE`, `RESOURCE_EXHAUSTED`, and `DEADLINE_EXCEEDED` only for
  read-only RPCs),
- TLS / credentials wiring and a shared channel across every service stub,
- metadata injection from the bound `UdbMetadata`,
- typed error mapping — it unpacks the `udb-error-detail-bin` trailer into a
  `\Udb\Entity\V1\ErrorDetail` (when that generated class is present) and
  throws the existing `UdbRpcException`.

Streaming RPCs (server/client/bidi) are exposed as accessors returning the live
gRPC call object with metadata + deadline applied; retry is intentionally not
applied to streams.

Per-service stub accessors (`{ServiceName}Stub()`) expose the raw
buf-generated stub for callers that want the bare surface or methods a future
regen adds. The stub FQN is derived at runtime from the proto package +
service name (`ucfirst` per dotted segment + `<Service>Client`), so no class
list is hand-maintained.

## CLI bundling — installing the SDK gives you `udb`

`composer require fahara02/udb-laravel` installs `bin/udb` onto Composer's bin
path (`vendor/bin/udb`, or the global bin for global installs) via the
`"bin"` manifest entry. The launcher bakes the pinned version
(`{{UDB_VERSION}}` at render time) and, on each invocation:

1. uses `$UDB_BIN` if it points at an executable (operator override);
2. reuses an `udb` already on `PATH` **iff** its `--version` matches the pinned
   version (a mismatched binary is ignored so SDK and broker stay in lockstep);
3. otherwise downloads the matching release asset for this OS/arch from
   `github.com/fahara02/udb` (tag `v{{UDB_VERSION}}`) into a per-user cache dir
   (`$XDG_CACHE_HOME/udb/bin/<version>/` or `%LOCALAPPDATA%\udb\bin\<version>\`),
   then execs it.

All arguments are forwarded verbatim, and the real binary's exit code is
propagated.

## Customizing

Edit the templates here, **not** the rendered files under `sdk/php/`
(rendered output is regenerated and overwritten). Useful knobs:

- Retry tuning is read from `config/udb.php` via the optional `retry` key
  (`max_attempts`, `base_delay_ms`, `max_delay_ms`) the generated client reads
  from the same config array the hand-written `UdbClient` consumes.
- To change the released asset naming convention, edit `releaseAssetName()` in
  `bin/udb.tmpl`.

## Placeholders used by these templates

Scalars: `{{UDB_VERSION}}`, `{{PROTOCOL_VERSION}}`, `{{LANG}}`, `{{RPC_COUNT}}`,
`{{SERVICE_COUNT}}`, `{{GENERATED_NOTE}}`.

Per-RPC (inside `// @@UDB_RPC_BEGIN … // @@UDB_RPC_END`, optionally filtered by
`kind=unary|server_streaming|client_streaming|bidi` and/or `service=`):
`{{RPC_NAME}}`, `{{RPC_SNAKE}}`, `{{RPC_KIND}}`, `{{RPC_PATH}}`,
`{{RPC_INPUT}}`, `{{RPC_OUTPUT}}`, `{{SERVICE_NAME}}`, `{{SERVICE_PKG}}`,
`{{SERVICE_FULL}}`.

Per-service (inside `// @@UDB_SERVICE_BEGIN … // @@UDB_SERVICE_END`):
`{{SERVICE_NAME}}`, `{{SERVICE_PKG}}`, `{{SERVICE_FULL}}`,
`{{SERVICE_RPC_COUNT}}`.
