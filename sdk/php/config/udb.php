<?php

declare(strict_types=1);

/*
 * UDB (Universal Data Broker) Laravel SDK configuration.
 *
 * Published via:  php artisan vendor:publish --tag=udb-config
 *
 * Every value here can be overridden from .env so deployments stay
 * 12-factor. Defaults match the broker's default development setup.
 */

return [

    /*
    |--------------------------------------------------------------------------
    | Broker endpoint
    |--------------------------------------------------------------------------
    |
    | Host:port the gRPC DataBroker is listening on. The SDK opens one
    | gRPC channel per process and re-uses it; channels are safe to
    | share across requests.
    |
    */

    'endpoint' => env('UDB_ENDPOINT', '127.0.0.1:50051'),

    /*
    |--------------------------------------------------------------------------
    | TLS
    |--------------------------------------------------------------------------
    |
    | `tls.enabled` switches the channel from `createInsecure()` to
    | `createSsl()`. When enabled, supply `tls.root_certs` as the
    | absolute path to a PEM bundle. Server name override (`tls.target`)
    | is useful when the broker presents a cert for a logical name
    | different from `endpoint`'s host.
    |
    */

    'tls' => [
        'enabled'   => env('UDB_TLS_ENABLED', false),
        'root_certs' => env('UDB_TLS_ROOT_CERTS', null),
        'target'    => env('UDB_TLS_TARGET', null),
    ],

    /*
    |--------------------------------------------------------------------------
    | Request metadata headers
    |--------------------------------------------------------------------------
    |
    | The broker requires these headers on every call. `tenant_id`,
    | `user_id`, and `correlation_id` are usually resolved
    | per-request via the `UdbContextMiddleware`; the values here are
    | the fallback when the middleware can't extract them (e.g.
    | scheduler jobs, queue workers without an HTTP request).
    |
    | `service_identity` and `default_scopes` are usually static for
    | a deployment.
    |
    */

    'metadata' => [
        'service_identity'        => env('UDB_SERVICE_IDENTITY', config('app.name', 'laravel.app')),
        'default_project_id'      => env('UDB_PROJECT_ID', 'default'),
        'default_purpose'         => env('UDB_DEFAULT_PURPOSE', 'web.request'),
        'default_scopes'          => array_filter(explode(',', (string) env('UDB_DEFAULT_SCOPES', ''))),
        'client_catalog_version'  => env('UDB_CLIENT_CATALOG_VERSION', '1.0.0'),
        'bearer_token'            => env('UDB_BEARER_TOKEN', ''),
        'api_key'                 => env('UDB_API_KEY', ''),
    ],

    /*
    |--------------------------------------------------------------------------
    | Middleware
    |--------------------------------------------------------------------------
    |
    | `auto_register` mounts `UdbContextMiddleware` on the `web` and
    | `api` middleware groups automatically. Turn off if you want to
    | wire the middleware yourself (e.g. only on specific route
    | groups).
    |
    | `tenant_resolver` and `correlation_resolver` are dotted paths
    | to closure-binding service-container entries. The defaults pull
    | from Laravel's authenticated user (`tenant_id` claim) and the
    | request's `X-Correlation-ID` header.
    |
    */

    'middleware' => [
        'auto_register'          => env('UDB_MIDDLEWARE_AUTO', true),
        'tenant_resolver'        => 'udb.tenant_resolver',
        'correlation_resolver'   => 'udb.correlation_resolver',
        'user_resolver'          => 'udb.user_resolver',
    ],

    /*
    |--------------------------------------------------------------------------
    | gRPC channel options
    |--------------------------------------------------------------------------
    |
    | Passed verbatim to `\Grpc\Channel`. Useful keys:
    |   - 'grpc.keepalive_time_ms'
    |   - 'grpc.keepalive_timeout_ms'
    |   - 'grpc.max_receive_message_length'
    |   - 'grpc.default_authority'
    |
    | See https://grpc.github.io/grpc/core/group__grpc__arg__keys.html
    |
    */

    'channel_options' => [
        'grpc.keepalive_time_ms'         => (int) env('UDB_GRPC_KEEPALIVE_MS', 30_000),
        'grpc.keepalive_timeout_ms'      => (int) env('UDB_GRPC_KEEPALIVE_TIMEOUT_MS', 10_000),
        'grpc.max_receive_message_length' => (int) env('UDB_GRPC_MAX_RECV_BYTES', 16 * 1024 * 1024),
    ],

    /*
    |--------------------------------------------------------------------------
    | Call deadline (per RPC, milliseconds)
    |--------------------------------------------------------------------------
    |
    | Translated into a `\Grpc\Timeval` deadline on every unary call.
    | 0 disables the deadline (not recommended for production —
    | request paths will hang on a stuck broker).
    |
    */

    'deadline_ms' => (int) env('UDB_DEADLINE_MS', 30_000),

];
