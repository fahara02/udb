# UDB Laravel SDK

<!-- UDB_BRAND_HEADER_START -->
<p align="center">
  <img src="../../docs/assets/udb_logo.svg" alt="UDB logo" width="96">
</p>

<p align="center">
  <strong>UDB :: Universal Data Broker</strong><br>
  <sub>gRPC data plane | native control plane | tenant/project scope guard<br>crate v0.3.2 | protocol v1.0.0</sub>
</p>
<!-- UDB_BRAND_HEADER_END -->

[![Packagist](https://img.shields.io/packagist/v/fahara02/udb-laravel.svg)](https://packagist.org/packages/fahara02/udb-laravel)
[![PHP Version](https://img.shields.io/badge/php-%5E8.1-blue.svg)](https://www.php.net/)
[![Laravel](https://img.shields.io/badge/laravel-10%20%7C%2011%20%7C%2012-red.svg)](https://laravel.com/)

Laravel client for [UDB](https://github.com/fahara02/udb), a service that lets your app read and write data through one broker instead of connecting directly to every database or storage backend.

Install this package when a Laravel app needs to call a running UDB broker. It gives you a Laravel `ServiceProvider`, a `Udb` facade, generated PHP protobuf classes, request middleware, and clear exceptions for gRPC errors.

---

## What is UDB?

UDB stands for Universal Data Broker. Think of it as a data API that sits between your Laravel app and the systems where the data actually lives.

A Laravel app sends a request such as "select patients", "upsert this invoice", or "delete this object". UDB decides where that data lives, applies the broker rules, talks to the right backend, and returns a typed response.

That means the Laravel app does not need to know whether the data is in Postgres, MySQL, SQLite, MongoDB, S3, Redis, Qdrant, or another supported backend. The app only needs the UDB endpoint and the request it wants to send.

## What This Package Does

This package makes UDB feel natural inside Laravel:

- `composer require fahara02/udb-laravel:^0.3.2`
- set `UDB_ENDPOINT` in `.env`
- call `Udb::select()`, `Udb::upsert()`, or `Udb::delete()`
- use typed generated PHP classes for requests and responses
- let middleware attach tenant, user, and correlation context automatically
- catch Laravel-friendly exceptions instead of handling raw gRPC status arrays everywhere

---

## Requirements

- **PHP 8.1+**
- **`grpc` PECL extension** — `pecl install grpc` then add `extension=grpc.so` to `php.ini`. The generated stubs extend `\Grpc\BaseStub`; there is no fallback transport.
- **Laravel 10, 11, or 12** (Illuminate Container + HTTP + Routing)
- A running UDB broker reachable from the app

---

## Installation

```bash
composer require fahara02/udb-laravel:^0.3.2
```

The service provider is auto-discovered by Laravel. The `Udb` facade is registered automatically. By default, the package also adds middleware to the `web` and `api` route groups so each request can carry tenant and user context into UDB.

The package also installs a version-matched CLI launcher at `vendor/bin/udb`.
Use it from your Laravel project when you need UDB's shared annotation protos:

```bash
vendor/bin/udb proto export --fmt
```

After that, your app-owned `.proto` files can import:

```proto
import "udb/core/common/v1/db.proto";
```

Run `vendor/bin/udb proto fmt` after export or schema edits to keep long UDB
field annotations on one line for easier review.

Publish the config file when you need to override defaults:

```bash
php artisan vendor:publish --tag=udb-config
```

This drops a documented `config/udb.php` into your app.

---

## Configuration

For local development, the only required value is usually:

```env
UDB_ENDPOINT=127.0.0.1:50051
```

For production, you will normally set TLS, service identity, project, and timeout values too:

```env
UDB_ENDPOINT=udb.prod.svc.cluster.local:50051

# TLS
UDB_TLS_ENABLED=true
UDB_TLS_ROOT_CERTS=/etc/ssl/certs/udb-ca.pem
UDB_TLS_TARGET=udb.prod.svc.cluster.local

# Static metadata (per-request values come from middleware)
UDB_SERVICE_IDENTITY=acme.api
UDB_PROJECT_ID=acme
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

### Read Data

```php
use Udb\Entity\V1\SelectRequest;
use Fahara02\UdbLaravel\Facades\Udb;

$req = (new SelectRequest())
    ->setMessageType('acme.healthcare.v1.Patient')
    ->setLimit(50);

$records = Udb::select($req);

foreach ($records->getRecords() as $record) {
    // $record is a typed protobuf object.
}
```

If the call happens during an HTTP request, the middleware will try to attach the current tenant, user, and correlation ID automatically.

### Write Data

```php
use Udb\Entity\V1\UpsertRequest;
use Fahara02\UdbLaravel\Facades\Udb;

$response = Udb::upsert($upsertRequest);
```

`upsert()` returns a `MutationResponse`.

### Delete Data

```php
use Udb\Entity\V1\DeleteRequest;
use Fahara02\UdbLaravel\Facades\Udb;

$response = Udb::delete($deleteRequest);
```

`delete()` also returns a `MutationResponse`.

### Queue Jobs And Scheduled Commands

Jobs, commands, and schedulers do not have an HTTP request, so pass metadata explicitly:

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

### Raw gRPC Stub

For broker RPCs that do not have a convenience method yet, use the generated gRPC stub:

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
    // UDB returned a gRPC error. $e->status holds the \Grpc\STATUS_* code.
    if ($e->status === \Grpc\STATUS_NOT_FOUND) { /* ... */ }
} catch (UdbConfigurationException $e) {
    // Missing endpoint, invalid TLS config, or similar app setup problem.
} catch (UdbException $e) {
    // Base exception for this package.
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
