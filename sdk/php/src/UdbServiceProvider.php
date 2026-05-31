<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel;

use Fahara02\UdbLaravel\Http\UdbContextMiddleware;
use Illuminate\Contracts\Http\Kernel as HttpKernelContract;
use Illuminate\Foundation\Http\Kernel as FoundationHttpKernel;
use Illuminate\Http\Request;
use Illuminate\Routing\Router;
use Illuminate\Support\ServiceProvider;

/**
 * The SDK's Laravel ServiceProvider. Auto-discovered via the
 * `extra.laravel.providers` entry in composer.json, so apps that
 * `composer require fahara02/udb-laravel` get the bindings + the
 * middleware wiring without editing config/app.php.
 *
 * Responsibilities:
 *  1. Merge the package config so `config('udb.*')` works even
 *     before the consumer publishes the file.
 *  2. Register `UdbClient` as a singleton — gRPC channels are
 *     long-lived and shared.
 *  3. Register the default tenant / correlation / user resolvers
 *     so `UdbContextMiddleware` has sane defaults out of the box.
 *  4. Mount the middleware on `web` and `api` groups when
 *     `udb.middleware.auto_register=true` (opt-out via config).
 *  5. Expose the config file for `vendor:publish` so operators
 *     can override.
 */
final class UdbServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(__DIR__ . '/../config/udb.php', 'udb');

        $this->app->singleton(UdbClient::class, function (): UdbClient {
            /** @var array{
             *   endpoint:string,
             *   tls?: array{enabled?:bool, root_certs?:?string, target?:?string},
             *   channel_options?: array<string,mixed>,
             *   deadline_ms?: int,
             * } $config */
            $config = (array) config('udb', []);
            return new UdbClient($config);
        });

        // Default resolvers — closures the middleware looks up by
        // container key. Operators override by binding the same
        // keys (or pointing the config at different keys) in
        // their own AppServiceProvider.

        $this->app->bind('udb.tenant_resolver', function () {
            return function (Request $request): ?string {
                $user = $request->user();
                if ($user === null) {
                    return null;
                }
                // Try the most common multi-tenant model conventions:
                //   - $user->tenant_id (column)
                //   - $user->getAttribute('tenant_id') (Eloquent)
                //   - $user->tenant?->id (relation)
                if (isset($user->tenant_id) && is_string($user->tenant_id)) {
                    return $user->tenant_id;
                }
                if (method_exists($user, 'getAttribute')) {
                    /** @var mixed $attr */
                    $attr = $user->getAttribute('tenant_id');
                    if (is_string($attr) && $attr !== '') {
                        return $attr;
                    }
                }
                return null;
            };
        });

        $this->app->bind('udb.user_resolver', function () {
            return function (Request $request): ?string {
                $user = $request->user();
                if ($user === null) {
                    return null;
                }
                // Laravel's `Authenticatable::getAuthIdentifier()`
                // returns mixed; coerce to string.
                if (method_exists($user, 'getAuthIdentifier')) {
                    $id = $user->getAuthIdentifier();
                    return $id !== null ? (string) $id : null;
                }
                return null;
            };
        });

        $this->app->bind('udb.correlation_resolver', function () {
            return function (Request $request): ?string {
                $value = $request->header('X-Correlation-Id');
                return is_string($value) && $value !== '' ? $value : null;
            };
        });
    }

    public function boot(): void
    {
        // Publishable config — `php artisan vendor:publish --tag=udb-config`.
        $this->publishes([
            __DIR__ . '/../config/udb.php' => config_path('udb.php'),
        ], 'udb-config');

        if (! (bool) config('udb.middleware.auto_register', true)) {
            return;
        }

        // Mount the middleware on the standard web + api groups.
        // We use the Router's `pushMiddlewareToGroup` rather than
        // the HTTP kernel's prepend so we run AFTER auth (which
        // populates `$request->user()` so the tenant resolver
        // can read it).
        $kernel = $this->app->make(HttpKernelContract::class);
        if ($kernel instanceof FoundationHttpKernel) {
            // No-op for non-HTTP kernels (Octane workers, queue
            // workers, scheduled commands) — those callers bind
            // context manually via `Udb::bindContext(...)` or
            // pass explicit metadata per RPC.
        }
        /** @var Router $router */
        $router = $this->app->make(Router::class);
        foreach (['web', 'api'] as $group) {
            $router->pushMiddlewareToGroup($group, UdbContextMiddleware::class);
        }
    }

    /**
     * @return list<string>
     */
    public function provides(): array
    {
        return [UdbClient::class];
    }
}
