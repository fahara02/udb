# UDB Laravel SDK

[![Packagist](https://img.shields.io/packagist/v/fahara02/udb-laravel.svg)](https://packagist.org/packages/fahara02/udb-laravel)
[![PHP Version](https://img.shields.io/badge/php-%5E8.1-blue.svg)](https://www.php.net/)
[![Laravel](https://img.shields.io/badge/laravel-10%20%7C%2011%20%7C%2012-red.svg)](https://laravel.com/)

The official Laravel SDK for [**UDB** (Universal Data Broker)](https://github.com/fahara02/udb) — a real gRPC client, a Laravel `ServiceProvider` + `Facade`, automatic request-context propagation, typed exceptions, and a tested middleware that resolves tenant / user / correlation per request.

This package gives Laravel apps the same first-class UDB experience the Go, Python, TypeScript, C# and Java SDKs already have, with Laravel-idiomatic ergonomics on top.

---

## Why this exists

Before this SDK, Laravel apps that wanted to talk to UDB had two unappealing options:

1. **Bypass UDB entirely and write SQL** — defeats the whole point of having a broker (no RLS, no audit, no cross-backend routing).
2. **Hand-roll a gRPC wrapper per project** — every app re-implements metadata injection, error mapping, and request-context plumbing.

The SDK fixes both: you `composer require` it, set `UDB_ENDPOINT` in `.env`, and `Udb::select($req)` returns a typed proto response with all 8 broker-required headers attached automatically.

---

## Requirements

- **PHP 8.1+**
- **`grpc` PECL extension** — `pecl install grpc` then add `extension=grpc.so` to `php.ini`. The generated stubs extend `\Grpc\BaseStub`; there is no fallback transport in v0.1.
- **Laravel 10, 11, or 12** (Illuminate Container + HTTP + Routing)
- A running UDB broker reachable from the app

---

## Installation

```bash
composer require fahara02/udb-laravel
```

That's it. The `UdbServiceProvider` is auto-discovered. The `Udb` facade is registered. The middleware is mounted on `web` and `api` route groups (opt-out via config).

Publish the config file when you need to override defaults:

```bash
php artisan vendor:publish --tag=udb-config
```

This drops a documented `config/udb.php` into your app.

---

## Configuration

All values are overridable via `.env`:

```env
UDB_ENDPOINT=udb.internal:50051

# TLS
UDB_TLS_ENABLED=true
UDB_TLS_ROOT_CERTS=/etc/ssl/certs/udb-ca.pem
UDB_TLS_TARGET=udb.prod.svc.cluster.local

# Static metadata (per-request values come from middleware)
UDB_SERVICE_IDENTITY=lifeplus.api
UDB_PROJECT_ID=lifeplus
UDB_DEFAULT_PURPOSE=web.request
UDB_DEFAULT_SCOPES=udb:read,udb:write
UDB_CLIENT_CATALOG_VERSION=1.0.0

# Timeouts / channel tuning
UDB_DEADLINE_MS=30000
UDB_GRPC_KEEPALIVE_MS=30000
UDB_GRPC_MAX_RECV_BYTES=16777216

# Middleware
UDB_MIDDLEWARE_AUTO=true
```

The full reference lives in [`config/udb.php`](config/udb.php).

---

## Usage

### Basic — Select

```php
use Udb\Entity\V1\SelectRequest;
use Fahara02\UdbLaravel\Facades\Udb;

$req = (new SelectRequest())
    ->setMessageType('lifeplus.healthcare.v1.Patient')
    ->setLimit(50);

$records = Udb::select($req);

foreach ($records->getRecords() as $record) {
    // $record is a \Udb\Entity\V1\Record proto
}
```

The middleware has already filled in `x-tenant-id`, `x-user-id`, `x-correlation-id` from the request — you didn't have to think about them.

### Upsert + Delete

```php
use Udb\Entity\V1\UpsertRequest;
use Udb\Entity\V1\DeleteRequest;

$response = Udb::upsert($upsertRequest);
$response = Udb::delete($deleteRequest);
```

Both return `\Udb\Entity\V1\MutationResponse`.

### One-off metadata override (queue jobs, scheduler)

When you're outside the HTTP request lifecycle:

```php
use Fahara02\UdbLaravel\UdbMetadata;

$meta = UdbMetadata::fromContext(
    tenantId: 'acme',
    userId: 'system',
    correlationId: 'nightly-billing-' . now()->timestamp,
    purpose: 'scheduled.billing',
    scopes: ['udb:read', 'udb:billing'],
);

$response = Udb::upsert($req, $meta);
```

### Accessing the raw stub

For RPCs the convenience methods don't wrap (`BeginTx`, `PublishCDC`, vector ops, blob ops):

```php
$stub = Udb::stub();
$call = $stub->BeginTx($request, Udb::context()->toGrpcMetadata());
[$response, $status] = $call->wait();
```

### Error handling

```php
use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Fahara02\UdbLaravel\Exceptions\UdbConfigurationException;
use Fahara02\UdbLaravel\Exceptions\UdbException;

try {
    Udb::select($req);
} catch (UdbRpcException $e) {
    // Broker said no. $e->status holds the \Grpc\STATUS_* code.
    if ($e->status === \Grpc\STATUS_NOT_FOUND) { /* ... */ }
} catch (UdbConfigurationException $e) {
    // Misconfigured deployment — alert the on-call.
} catch (UdbException $e) {
    // Catch-all for anything UDB-shaped.
}
```

---

## Per-request context

The shipped `UdbContextMiddleware` resolves three values from each incoming HTTP request and binds them to the singleton client so subsequent RPC calls inherit them:

| Value | Default resolver | Header fallback |
|---|---|---|
| `tenant_id` | `$request->user()->tenant_id` | `X-Tenant-Id` |
| `user_id` | `$request->user()->getAuthIdentifier()` | `X-User-Id` |
| `correlation_id` | `X-Correlation-Id` header | freshly generated v4 UUID |

Override the resolvers by binding the same container keys in your `AppServiceProvider`:

```php
$this->app->bind('udb.tenant_resolver', function () {
    return function (Request $request): ?string {
        return $request->attributes->get('resolved_tenant_id');
    };
});
```

---

## Testing

The SDK ships with Pest tests + an Orchestra Testbench base class. For your own app's tests, swap the `UdbClient` binding for a fake:

```php
use Fahara02\UdbLaravel\UdbClient;

$this->app->instance(UdbClient::class, $fakeClient = Mockery::mock(UdbClient::class));
$fakeClient->shouldReceive('select')->andReturn(new \Udb\Entity\V1\RecordSet());
```

Run the SDK's own tests:

```bash
composer install
composer test          # vendor/bin/pest
composer analyse       # vendor/bin/phpstan
```

---

## Versioning & compatibility

This SDK follows the UDB wire-protocol version — see `UDB_PROTOCOL_VERSION` in the parent `sdk/` directory. Major SDK versions track major broker versions; minor SDK versions add convenience methods and Laravel-idiomatic helpers.

The current wire protocol is **1.0.0**.

---

## License

MIT © fahara02
