# Versioning

UDB ships seven independently-released artifacts (the Rust crate + binaries/image,
and six SDKs) plus one cross-cutting **wire protocol** version. This document is
the contract for how they are versioned, kept consistent, and released.

## Model

- **Per-package SemVer, independent.** Each artifact has its own `MAJOR.MINOR.PATCH`.
  They do **not** have to share a number — e.g. the Python SDK may be at `0.1.4`
  while the crate is at `0.1.3`. Bump a package only when *that* package changes.
- **One protocol version for all.** `protocol` in `versions.json` is the gRPC wire /
  catalog-header version (`x-udb-client-catalog-version`). It **must be identical**
  in every SDK and in `sdk/UDB_PROTOCOL_VERSION`. It changes only when the
  `proto/udb/**` contract changes in a client-visible way — independently of any
  package's release number.
- **Single source of truth: [`versions.json`](versions.json).** Every manifest and
  every hardcoded protocol constant is *derived from / checked against* it.

## Source of truth & tooling

`versions.json` lists each component's version, its manifest, its tag pattern, and
where it publishes. The checker propagates and verifies it:

```bash
node scripts/check-versions.mjs          # verify all manifests + protocol consts
node scripts/check-versions.mjs --fix    # rewrite manifests from versions.json
node scripts/check-versions.mjs --json   # machine-readable report
```

`--fix` rewrites: `Cargo.toml`, `sdk/python/pyproject.toml`,
`sdk/typescript/package.json`, `sdk/csharp/Udb.Client/Udb.Client.csproj`,
`sdk/java/pom.xml` (keeps a `-SNAPSHOT` suffix if present), `sdk/UDB_PROTOCOL_VERSION`,
and the protocol constant in each SDK client (`client.go`, `metadata.py`,
`client.ts`, `UdbClient.cs`, `UdbClient.java`, `UdbMetadata.php`, `config/udb.php`).
Go and PHP carry no manifest version field (they are git-tag/VCS-driven), so only
their protocol constants are checked; their package version lives only in the tag.

## Enforcement

- **CI** runs `node scripts/check-versions.mjs` in the `versions` job on every push
  and PR (`.github/workflows/ci.yml`). Drift fails the build.
- **Every release workflow** re-runs the same check *and* asserts the tag matches
  the manifest (and, for tag-driven Go/PHP, `versions.json`). So a publish can only
  happen when **tag == manifest == versions.json**.

## Tag conventions

| Component | Tag | Manifest | Publishes to |
|---|---|---|---|
| `udb` crate + binaries + Docker image | `v<x.y.z>` | `Cargo.toml` | crates.io, GitHub Release assets, `ghcr.io/fahara02/udb` |
| Go SDK | `sdk/go/v<x.y.z>` | git tag only | Go module proxy |
| Python SDK | `python-v<x.y.z>` | `sdk/python/pyproject.toml` | PyPI (`udb-client`) |
| TypeScript SDK | `typescript-v<x.y.z>` | `sdk/typescript/package.json` | npm (`@udb_plus/sdk`) |
| C# SDK | `csharp-v<x.y.z>` | `sdk/csharp/Udb.Client/Udb.Client.csproj` | NuGet (`Udb.Client`) |
| Java SDK | `java-v<x.y.z>` | `sdk/java/pom.xml` | Maven Central (`dev.udb:udb-java-client`) |
| PHP/Laravel SDK | `v<x.y.z>` | git tag only | Packagist (`fahara02/udb-laravel`) |

> The single `v<x.y.z>` tag drives the crate, binaries, Docker, **and** the
> PHP/Packagist split simultaneously (they share the same release cadence). The
> SDK-prefixed tags (`python-v…`, `typescript-v…`, `csharp-v…`, `java-v…`,
> `sdk/go/v…`) let the other SDKs release on their own cadence.

## How to cut a release

1. Edit the version in **`versions.json`** (and the `protocol` field too, if the
   wire contract changed).
2. Propagate it into the manifests + constants:
   ```bash
   node scripts/check-versions.mjs --fix
   ```
3. Commit, open a PR — the `versions` CI job confirms consistency.
4. After merge, push the matching tag for the artifact you're releasing, e.g.
   ```bash
   git tag typescript-v0.1.1 && git push origin typescript-v0.1.1   # → npm
   git tag v0.1.4            && git push origin v0.1.4              # → crate + binaries + docker + packagist
   git tag sdk/go/v0.1.1     && git push origin sdk/go/v0.1.1       # Go: tag IS the release
   ```
   The release workflow re-verifies tag == manifest == `versions.json` before publishing.

## Release pipelines & required secrets

| Workflow | Trigger | Secret(s) (in the `production` environment) |
|---|---|---|
| `release-crates.yml` | `v*.*.*` | `CARGO_REGISTRY_TOKEN` |
| `release-binaries.yml` | `v*.*.*` / dispatch | — (GitHub Release; uses `GITHUB_TOKEN`) |
| `release-docker.yml` | `v*.*.*` / dispatch | — (GHCR; uses `GITHUB_TOKEN`) |
| `release-packagist.yml` | `v*.*.*` | `UDB_LARAVEL_DEPLOY_KEY`, `PACKAGIST_USERNAME`, `PACKAGIST_API_TOKEN` |
| `release-python-sdk.yml` | `python-v*` / dispatch | `PYPI_API_TOKEN` |
| `release-typescript-sdk.yml` | `typescript-v*` / dispatch | `NPM_TOKEN` |

**Go** needs no publish workflow — pushing `sdk/go/v<x.y.z>` is the release; the
Go module proxy fetches it from the tag. **C#** (NuGet) and **Java** (Maven Central)
have tag conventions reserved above and are version-checked, but their publish
workflows are not yet wired (they need `NUGET_API_KEY` and Sonatype/`MAVEN_*`
+ GPG signing secrets respectively); add them mirroring `release-typescript-sdk.yml`
when ready.
