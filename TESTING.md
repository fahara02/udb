# Testing UDB

The standalone repo has three layers; you can run each in isolation
without the others installed.

## 1. Rust — the broker runtime

The 832-test default suite needs **no external services and no `.env`**.
Build.rs auto-detects the proto root, and tests use in-memory fixtures
or feature-gated mocks.

```bash
cargo test --lib                 # default features, fastest
cargo test --all-features --lib  # ~10 min, exercises every backend
cargo test --features clickhouse,mssql,cassandra --lib   # pick a subset
```

To run the broker against a real Postgres, copy `.env.example` to
`.env.local` and fill in `UDB_PG_DSN`. The build.rs auto-detect
walks both `./` and `../` so the same `.env` works whether this repo
is standalone or embedded inside a parent monorepo.

## 2. Proto contract — `buf` workspace

```bash
buf lint        # style + structure checks
buf build       # compile every proto, surface circular imports
buf generate    # regenerate SDK stubs (PHP, TypeScript, OpenAPI)
```

`buf generate` writes into `sdk/php/gen/`, `sdk/typescript/gen/`, and
`api/`. The generated paths are referenced by each SDK's package
manifest (e.g. composer.json's PSR-4 mapping), so a fresh `buf generate`
is a no-op when nothing changed in `proto/`.

## 3. PHP / Laravel SDK

The SDK's own Pest suite needs PHP 8.1+ and the `grpc` extension.
The fastest way is Docker:

```bash
cd sdk/php
docker run --rm -v "$PWD:/app" -w /app php:8.2-cli bash -c "
  apt-get update -qq && apt-get install -y -qq git unzip libz-dev &&
  pecl install grpc < /dev/null &&
  docker-php-ext-enable grpc &&
  curl -sSL https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer &&
  composer install --no-interaction &&
  vendor/bin/pest
"
```

On a host with PHP installed natively:

```bash
cd sdk/php
composer install
composer test          # vendor/bin/pest
composer analyse       # vendor/bin/phpstan
```

The 23 tests cover the metadata builder, scope joiner, service
provider bindings, and context middleware. No broker is required —
the gRPC stub is doubled.
