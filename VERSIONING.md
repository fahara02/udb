# Versioning

UDB ships one release version across the Rust crate, binaries/image, and every
SDK, plus one separate **wire protocol** version. This document is the contract
for how those versions are kept consistent and released.

## Model

- **One UDB SemVer for all packages.** The crate and every SDK use the same
  `MAJOR.MINOR.PATCH` from `versions.json`. A `v<x.y.z>` release means the Rust
  crate, binaries, Docker image, PHP package, Python package, TypeScript package,
  and SDK manifests are all at `x.y.z`.
- **One protocol version for the wire contract.** `protocol` in `versions.json`
  is the gRPC wire / catalog-header version (`x-udb-client-catalog-version`). It
  must be identical in every SDK and in `sdk/UDB_PROTOCOL_VERSION`. It changes
  only when the `proto/udb/**` contract changes in a client-visible way.
- **Single source of truth: [`versions.json`](versions.json).** Every manifest
  and every hardcoded protocol constant is derived from / checked against it.

## Source of Truth & Tooling

`versions.json` lists each component's version, manifest, tag pattern, and
publish target. All component versions must match the main UDB release version.

```bash
node scripts/check-versions.mjs          # verify all manifests + protocol consts
node scripts/check-versions.mjs --fix    # rewrite manifests from versions.json
node scripts/check-versions.mjs --json   # machine-readable report
```

`--fix` rewrites: `Cargo.toml`, `sdk/python/pyproject.toml`,
`sdk/typescript/package.json`, `sdk/csharp/Udb.Client/Udb.Client.csproj`,
`sdk/java/pom.xml` (keeps a `-SNAPSHOT` suffix if present),
`sdk/UDB_PROTOCOL_VERSION`, and the protocol constant in each SDK client
(`client.go`, `metadata.py`, `client.ts`, `UdbClient.cs`, `UdbClient.java`,
`UdbMetadata.php`, `config/udb.php`). Go and PHP carry no manifest version field,
so their intended version is asserted from `versions.json` at release time.

## Enforcement

- **CI** runs `node scripts/check-versions.mjs` in the `versions` job on every
  push and PR (`.github/workflows/ci.yml`). Drift fails the build.
- **Release workflows** re-run the same check and assert `v<x.y.z>` matches the
  package manifest or `versions.json`. A publish can only happen when
  **tag == manifest == versions.json**.

## Tag Conventions

| Component | Tag | Manifest | Publishes to |
|---|---|---|---|
| `udb` crate + binaries + Docker image | `v<x.y.z>` | `Cargo.toml` | crates.io, GitHub Release assets, `ghcr.io/fahara02/udb` |
| Python SDK | `v<x.y.z>` | `sdk/python/pyproject.toml` | PyPI (`udb-client`) |
| TypeScript SDK | `v<x.y.z>` | `sdk/typescript/package.json` | npm (`@udb_plus/sdk`) |
| C# SDK | `v<x.y.z>` | `sdk/csharp/Udb.Client/Udb.Client.csproj` | NuGet (`Udb.Client`) |
| Java SDK | `v<x.y.z>` | `sdk/java/pom.xml` | Maven Central (`dev.udb:udb-java-client`) |
| PHP/Laravel SDK | `v<x.y.z>` | git tag only | Packagist (`fahara02/udb-laravel`) |
| Go SDK | `sdk/go/v<x.y.z>` | git tag only | Go module proxy |

The single `v<x.y.z>` tag drives every release workflow that can publish from
the monorepo. Go is the only exception in tag shape because `sdk/go` is a Go
module in a subdirectory; its version still follows the same UDB release number.

## How to Cut a Release

1. Edit the version in **`versions.json`** for `udb` and every SDK component.
   Keep them identical. Edit `protocol` only when the wire contract changes.
2. Propagate the package version into manifests + constants:
   ```bash
   node scripts/check-versions.mjs --fix
   ```
3. Commit, open a PR, and let the `versions` CI job confirm consistency.
4. After merge, push the shared release tag:
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```
   This triggers crate, binary, Docker, PHP, Python, and TypeScript releases.
5. For the Go SDK module, also push the matching submodule tag when publishing it:
   ```bash
   git tag sdk/go/v0.2.0
   git push origin sdk/go/v0.2.0
   ```

## Release Pipelines & Required Secrets

| Workflow | Trigger | Secret(s) (in the `production` environment) |
|---|---|---|
| `release-crates.yml` | `v*.*.*` | `CARGO_REGISTRY_TOKEN` |
| `release-binaries.yml` | `v*.*.*` / dispatch | - (GitHub Release; uses `GITHUB_TOKEN`) |
| `release-docker.yml` | `v*.*.*` / dispatch | - (GHCR; uses `GITHUB_TOKEN`) |
| `release-packagist.yml` | `v*.*.*` | `UDB_LARAVEL_DEPLOY_KEY`, `PACKAGIST_USERNAME`, `PACKAGIST_API_TOKEN` |
| `release-python-sdk.yml` | `v*.*.*` / dispatch | `PYPI_API_TOKEN` |
| `release-typescript-sdk.yml` | `v*.*.*` / dispatch | `NPM_TOKEN` |

C# (NuGet) and Java (Maven Central) are version-checked but their publish
workflows are not yet wired. Add them with the same shared `v*.*.*` trigger when
`NUGET_API_KEY` and Sonatype / `MAVEN_*` + GPG signing secrets are ready.

## Binary Auth Feature Contract

GitHub Release binaries are built from the portable backend feature set plus
`oidc,webauthn` on Linux x86_64, Windows x86_64/MSVC, macOS Apple Silicon, and
macOS x86_64. The CI `auth-release-binary` matrix checks the same targets before
a release tag can ship.

WebAuthn uses `webauthn-rs` and vendored OpenSSL, so Windows release builds
install Strawberry Perl, NASM, and initialize the MSVC toolchain before Cargo
runs. Linux and macOS runners install their Perl/NASM/CMake/Ninja prerequisites
explicitly. This keeps the published CLI/broker binaries independent of the
developer workstation's local Perl/OpenSSL state.
