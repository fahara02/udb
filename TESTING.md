# Testing UDB

<p align="center">
  <img src="docs/assets/udb_logo.svg" alt="UDB logo" width="96">
</p>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.4.28 | protocol v1.0.0</sub>
</p>

This is the test guide for people working on the UDB repository. It walks you
through what to run and in roughly what order. The good news: most day-to-day
checks run with nothing but the repo checked out. The heavier live-backend and
load checks are opt-in, so you only reach for them when you need them.

## Rust

Start here. These are the core Rust unit tests. The first line is the everyday
run; the other two also compile the optional backend drivers behind Cargo
feature flags:

```bash
cargo test --lib
cargo test --all-features --lib
cargo test --no-default-features --features postgres --lib
```

The default suite is designed to run without a `.env` file or live databases.
The feature sweeps compile and exercise optional backend code so a change to one
store can't silently break another.

### Windows build (rdkafka / CMake)

On Windows there's one gotcha worth knowing up front. `rdkafka-sys` compiles
native code with CMake at build time. A stray PATH CMake
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

UDB is proto-driven: the `.proto` files under `proto/` are the source of truth.
These commands check that contract and regenerate the code derived from it:

```bash
buf lint
buf build
buf generate
```

`buf generate` refreshes generated SDK stubs and OpenAPI artifacts from
`proto/`.

## Version And Descriptor Gates

Run these before you push. They confirm every version string still agrees and
that the native-service contract (the protobuf-derived description of UDB's
native services) is in sync with its docs:

```bash
node scripts/check-versions.mjs
udb native lint
udb native docs --check
```

Use `udb native manifest` when you need to inspect the descriptor-derived native
service contract.

## SDK Conformance

This suite makes sure every language SDK behaves the same way on the wire:

```bash
node sdk-conformance/run.mjs
```

It checks cross-language metadata, bearer/API-key headers, requested scopes,
authz cache behavior, policy-bundle signatures, refresh single-flight behavior,
and credential hot-swap behavior.

When you're editing one specific package, run just that language's own tests
from its SDK directory:

```bash
cd sdk/typescript && npm test
cd sdk/python && pytest
cd sdk/go && go test ./...
dotnet test sdk/csharp/Udb.Client.Tests/Udb.Client.Tests.csproj
mvn -f sdk/java/pom.xml test
cd sdk/php && composer test
```

## Live Integration

These checks talk to real services, so they're off by default. Start the
integration stack first, then set the environment variables for the test family
you want to run. The final command shuts the stack back down.

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

Want a quick end-to-end playground? The local dev helper spins one up, runs a
smoke test, and tears it down:

```bash
udb dev up
udb dev smoke
udb dev down
```

The native-service load scripts drive traffic with `ghz` (a gRPC load-testing
tool) and need a running broker plus its configured backing services:

```bash
scripts/native-load-test.sh
```

PowerShell:

```powershell
.\scripts\native-load-test.ps1
```
