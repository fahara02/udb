<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Http;

use Closure;
use Fahara02\UdbLaravel\UdbAuthClient;
use Illuminate\Http\Request;

/**
 * Example Laravel middleware that forwards the app-authenticated identity to
 * UDB (item 109). After your app's own `auth` middleware has resolved a user,
 * this maps that user into a UDB external principal — UDB then owns the
 * authorization decision. The app's roles travel only as claims; UDB policy
 * decides whether they are sufficient.
 *
 * Register after `auth`:
 *   Route::middleware(['auth', \Fahara02\UdbLaravel\Http\UdbForwardAuthMiddleware::class])->group(...);
 *
 * The resolved UDB principal is stashed on the request as `udb_principal` for
 * downstream controllers; failures are non-fatal here (the example logs and
 * continues) — tighten to `abort(401)` if UDB identity is mandatory.
 */
class UdbForwardAuthMiddleware
{
    public function __construct(private readonly UdbAuthClient $udbAuth)
    {
    }

    public function handle(Request $request, Closure $next, string $providerId = 'laravel')
    {
        $user = $request->user();
        if ($user !== null) {
            // Forward the app identity as verified external claims. In a real
            // deployment sign these (or pass your IdP's JWT) so UDB can verify
            // them; here we forward the app's own claims JSON.
            $claims = json_encode([
                'sub' => (string) $user->getAuthIdentifier(),
                'email' => $user->email ?? '',
                'roles' => method_exists($user, 'getRoleNames') ? $user->getRoleNames()->all() : [],
            ], JSON_THROW_ON_ERROR);

            try {
                $resp = $this->udbAuth->authenticateExternal($claims, $providerId);
                $request->attributes->set('udb_principal', $resp->getPrincipal());
            } catch (\Throwable $e) {
                report($e);
            }
        }

        return $next($request);
    }
}
