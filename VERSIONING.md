# Versioning

<p align="center">
  <img src="docs/assets/udb_logo.svg" alt="UDB logo" width="96">
</p>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.4.14 | protocol v1.0.0</sub>
</p>

UDB uses one product version for the crate, release binaries, Docker image, and
SDK packages, plus one separate wire-protocol version.

## Current Versions

| Item | Version |
|---|---:|
| UDB crate and CLI | `0.4.14` |
| SDK packages | `0.4.14` |
| Wire protocol | `1.0.0` |
| Release tag | `v0.4.14` |

The source of truth is [versions.json](versions.json). Package manifests and SDK
protocol constants are checked against it in CI.

## 0.4.14 Release Gate Assumptions

Before tagging 0.4.14, the staged tree should satisfy the gates that failed the
last two release attempts:

- `node scripts/check-versions.mjs` must agree with [versions.json](versions.json).
- `cargo fmt --all -- --check` must be clean.
- `udb native manifest` must match
  [docs/generated/udb-native-contract.json](docs/generated/udb-native-contract.json).
- CI must provide GitHub credentials to `bufbuild/buf-setup-action` and create
  the MinIO buckets required by live SDK/native-service startup.

## Version Model

- Product releases use SemVer: `MAJOR.MINOR.PATCH`.
- The Rust crate, binaries, Docker image, and all SDKs share the same product
  version.
- The protocol version tracks client-visible gRPC/catalog compatibility and can
  change independently when the wire contract changes.
- Go uses the module tag shape `sdk/go/v<x.y.z>` because the Go module lives in a
  subdirectory. It still follows the same UDB product version.

## Pre-1.0 Beta Compatibility

UDB `0.x` releases are beta. Before `1.0.0`, HTTP routes, OpenAPI
`operationId`s, SDK public method names, generated examples, and benchmark/API
labels may change when the change simplifies the long-term public contract. The
wire protocol version remains factual metadata; it is not a promise that the
product API and SDK surface are stable during `0.x`.

Breaking `0.x` changes must be documented with migration notes, but those notes
are not a backward-compatibility guarantee. Permanent compatibility shims should
not be added for every beta route or SDK method name unless a specific release
requires a temporary bridge.

The normative API/SDK rules are in [docs/api-rules.md](docs/api-rules.md).
The current beta route and SDK alias migration fixture is
[docs/api-sdk-beta-migration.md](docs/api-sdk-beta-migration.md).

### Beta Breaking-Change Note Template

Use this template for any breaking API or SDK change before `1.0.0`:

```markdown
### Beta breaking change: <short title>

- Product version: `0.x.y`
- Old HTTP route or SDK method:
- New HTTP route or SDK method:
- Reason:
- Affected SDK languages:
- Migration snippet:
- Removal/deprecation posture:
- Related API rule:
```

## Check And Fix Versions

```bash
node scripts/check-versions.mjs
node scripts/check-versions.mjs --fix
node scripts/check-versions.mjs --json
```

`--fix` propagates [versions.json](versions.json) into the crate manifest, SDK
manifests, `sdk/UDB_PROTOCOL_VERSION`, and SDK protocol constants.

## Release Tags

| Component | Tag |
|---|---|
| Crate, binaries, Docker, Python, TypeScript, C#, Java, PHP | `v<x.y.z>` |
| Go SDK module | `sdk/go/v<x.y.z>` |

Release workflows assert that the tag, manifests, and [versions.json](versions.json)
agree before publishing.

## Publishing Targets

| Component | Target |
|---|---|
| Rust crate | crates.io |
| Binaries | GitHub Releases |
| Docker image | GitHub Container Registry |
| Python SDK | PyPI package `udb-client` |
| TypeScript SDK | npm package `@udb_plus/sdk` |
| C# SDK | NuGet package `Udb.Client` |
| Java SDK | Maven coordinates `dev.udb:udb-java-client` |
| PHP/Laravel SDK | Packagist package `fahara02/udb-laravel` |
| Go SDK | Go module proxy |

## CLI Name

The public CLI command is always:

```bash
udb
```

Docs, SDK launchers, examples, and release notes should not use older
proto-parser-specific binary names.
