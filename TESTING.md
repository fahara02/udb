# Testing UDB

<p align="center">
  <img src="docs/assets/udb_logo.svg" alt="UDB logo" width="96">
</p>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.4.10 | protocol v1.0.0</sub>
</p>

This is the short test guide for the repository. Most day-to-day checks run
without external services; live backend and load checks are opt-in.

## Rust

```bash
cargo test --lib
cargo test --all-features --lib
cargo test --no-default-features --features postgres --lib
```

The default suite is intended to run without a `.env` file or live databases.
Feature sweeps compile and exercise optional backend code.

### Windows build (rdkafka / CMake)

`rdkafka-sys` compiles native code with CMake at build time. A stray PATH CMake
(e.g. 3.29) cannot drive the Visual Studio 2026 generator and fails with
`Could not create named generator Visual Studio 18 2026`, causing random build
failures. Fix once, user-wide, by pointing `CMAKE` at the VS-bundled cmake:

```powershell
[Environment]::SetEnvironmentVariable("CMAKE",
  "C:\Program Files\Microsoft Visual Studio\18\Community\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe",
  "User")
```

(Adjust the edition path — `Community`/`Professional`/`Enterprise` — and VS major
version to match your install.) CI is unaffected; it installs its own cmake/ninja.

## Protobuf Contract

```bash
buf lint
buf build
buf generate
```

`buf generate` refreshes generated SDK stubs and OpenAPI artifacts from
`proto/`.

## Version And Descriptor Gates

```bash
node scripts/check-versions.mjs
udb native lint
udb native docs --check
```

Use `udb native manifest` when you need to inspect the descriptor-derived native
service contract.

## SDK Conformance

```bash
node sdk-conformance/run.mjs
```

The conformance suite checks cross-language metadata, bearer/API-key headers,
requested scopes, authz cache behavior, policy-bundle signatures, refresh
single-flight behavior, and credential hot-swap behavior.

Run individual language SDK checks from the SDK directories when editing a
specific package:

```bash
cd sdk/typescript && npm test
cd sdk/python && pytest
cd sdk/go && go test ./...
dotnet test sdk/csharp/Udb.Client.Tests/Udb.Client.Tests.csproj
mvn -f sdk/java/pom.xml test
cd sdk/php && composer test
```

## Live Integration

Live checks require real services and are disabled by default. Start the
integration stack first, then enable the relevant environment variables for the
test family you want to run.

```bash
docker compose -f docker-compose.integration.yml up -d --wait
UDB_INTEGRATION_TESTS=1 cargo test --test integration_tests -- --nocapture
docker compose -f docker-compose.integration.yml down -v --remove-orphans
```

Windows PowerShell:

```powershell
docker compose -f docker-compose.integration.yml up -d --wait
$env:UDB_INTEGRATION_TESTS = "1"
cargo test --test integration_tests -- --nocapture
docker compose -f docker-compose.integration.yml down -v --remove-orphans
```

## Load And Smoke

Use the local dev helper for a quick playground:

```bash
udb dev up
udb dev smoke
udb dev down
```

Native-service load scripts use `ghz` and require a running broker plus the
configured backing services:

```bash
scripts/native-load-test.sh
```

PowerShell:

```powershell
.\scripts\native-load-test.ps1
```
