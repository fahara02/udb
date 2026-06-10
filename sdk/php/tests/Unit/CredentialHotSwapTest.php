<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;

/*
 * Credential hot-swap (urgent_fix #16).
 *
 * UdbProject::setCredentials() rebuilds the bound metadata via
 * UdbMetadata::withCredentials() and re-binds it, so the refreshed bearer/API
 * key reaches outbound metadata on the next call. These assert the metadata
 * mechanics the swap relies on and that the project exposes the method — pure
 * reflection + value-object behaviour, so they run in the lint-only image where
 * ext-grpc is absent (constructing UdbProject requires the grpc extension).
 */

it('withCredentials swaps the bearer token in the outbound metadata', function () {
    $meta = new UdbMetadata(
        tenantId: 't-acme',
        userId: 'u-1',
        purpose: 'web.request',
        correlationId: 'corr-1',
        scopes: ['read'],
        serviceIdentity: 'svc',
        projectId: 'default',
        clientCatalogVersion: '1.0.0',
        bearerToken: 'token-1',
        apiKey: 'key-1',
    );

    expect($meta->toGrpcMetadata()['authorization'][0])->toBe('Bearer token-1');

    $refreshed = $meta->withCredentials('token-2');
    $wire = $refreshed->toGrpcMetadata();

    expect($wire['authorization'][0])->toBe('Bearer token-2');
    // API key is preserved when only the bearer is swapped.
    expect($wire['x-api-key'][0])->toBe('key-1');
    // The original object is untouched (value-object immutability).
    expect($meta->toGrpcMetadata()['authorization'][0])->toBe('Bearer token-1');
});

it('UdbProject exposes a setCredentials() hot-swap that returns the project', function () {
    expect(method_exists(UdbProject::class, 'setCredentials'))->toBeTrue('UdbProject::setCredentials() missing');

    $method = new ReflectionMethod(UdbProject::class, 'setCredentials');
    expect($method->getReturnType()?->getName())->toBe(UdbProject::class);

    $params = $method->getParameters();
    expect($params[0]->getName())->toBe('bearerToken');
    expect($params[0]->allowsNull())->toBeTrue();
    expect($params[1]->getName())->toBe('apiKey');
    expect($params[1]->allowsNull())->toBeTrue();
});
