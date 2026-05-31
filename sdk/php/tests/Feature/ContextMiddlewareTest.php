<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\Http\UdbContextMiddleware;
use Fahara02\UdbLaravel\UdbClient;
use Illuminate\Http\Request;
use Symfony\Component\HttpFoundation\Response;

it('binds tenant + correlation + user from the request onto the client', function () {
    $client = app(UdbClient::class);
    $middleware = app(UdbContextMiddleware::class);

    $request = Request::create('/anything', 'GET');
    $request->headers->set('X-Tenant-Id', 'acme');
    $request->headers->set('X-User-Id', 'u-42');
    $request->headers->set('X-Correlation-Id', 'corr-xyz');

    $response = $middleware->handle($request, function (Request $req): Response {
        return new Response('ok', 200);
    });

    $meta = $client->context();
    expect($meta->tenantId)->toBe('acme')
        ->and($meta->userId)->toBe('u-42')
        ->and($meta->correlationId)->toBe('corr-xyz');
    expect($response->headers->get('X-Correlation-Id'))->toBe('corr-xyz');
});

it('generates a fresh correlation id when none is supplied', function () {
    $client = app(UdbClient::class);
    $middleware = app(UdbContextMiddleware::class);

    $request = Request::create('/anything', 'GET');
    $request->headers->set('X-Tenant-Id', 'acme');

    $response = $middleware->handle($request, fn (Request $r) => new Response('ok'));

    $cid = $client->context()->correlationId;
    expect($cid)->not->toBe('')
        // RFC 4122 v4 shape: 36 chars with dashes at positions 8,13,18,23.
        ->and(strlen($cid))->toBe(36)
        ->and($cid[14])->toBe('4')
        ->and($response->headers->get('X-Correlation-Id'))->toBe($cid);
});

it('does not overwrite an existing X-Correlation-Id on the response', function () {
    $middleware = app(UdbContextMiddleware::class);
    $request = Request::create('/anything', 'GET');
    $request->headers->set('X-Tenant-Id', 'acme');
    $request->headers->set('X-Correlation-Id', 'corr-in');

    $response = $middleware->handle($request, function (Request $req) {
        $r = new Response('ok');
        $r->headers->set('X-Correlation-Id', 'corr-out-from-handler');
        return $r;
    });

    expect($response->headers->get('X-Correlation-Id'))->toBe('corr-out-from-handler');
});

it('falls back to empty strings when no resolver / header supplies a tenant', function () {
    $client = app(UdbClient::class);
    $middleware = app(UdbContextMiddleware::class);
    $request = Request::create('/anything', 'GET');
    // Deliberately no tenant header, no auth.

    $middleware->handle($request, fn (Request $r) => new Response('ok'));

    // tenantId stays empty so the broker can reject the request
    // with a clear "missing tenant" error rather than the SDK
    // silently substituting a default. This matches the
    // Go/Python/TS SDK behaviour.
    expect($client->context()->tenantId)->toBe('');
});
