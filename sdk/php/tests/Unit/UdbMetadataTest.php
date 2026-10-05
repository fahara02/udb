<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\UdbMetadata;

it('builds metadata from the request context with config fallbacks', function () {
    $meta = UdbMetadata::fromContext(
        tenantId: 'acme',
        userId: 'u-123',
        correlationId: 'corr-abc',
    );

    expect($meta->tenantId)->toBe('acme')
        ->and($meta->userId)->toBe('u-123')
        ->and($meta->correlationId)->toBe('corr-abc')
        // Falls back to TestCase's defineEnvironment values:
        ->and($meta->serviceIdentity)->toBe('test.service')
        ->and($meta->projectId)->toBe('test-project')
        ->and($meta->purpose)->toBe('test.purpose')
        ->and($meta->scopes)->toBe(['udb:read'])
        ->and($meta->clientCatalogVersion)->toBe('1.0.0');
});

it('renders all 8 canonical headers in toGrpcMetadata()', function () {
    $meta = UdbMetadata::fromContext('acme', 'u-1', 'corr-1');
    $headers = $meta->toGrpcMetadata();

    expect(array_keys($headers))->toBe([
        'x-tenant-id',
        'x-user-id',
        'x-purpose',
        'x-correlation-id',
        'x-scopes',
        'x-service-identity',
        'x-udb-project-id',
        'x-udb-client-catalog-version',
    ]);

    // Every value is wrapped in a list — gRPC PHP requires this.
    foreach ($headers as $key => $value) {
        expect($value)->toBeArray()->and($value)->toHaveCount(1, "header {$key} must be a single-element list");
    }
});

it('joins scopes with commas in the wire header', function () {
    $meta = UdbMetadata::fromContext('acme', 'u-1', 'corr-1', scopes: ['udb:read', 'udb:write']);
    $headers = $meta->toGrpcMetadata();
    expect($headers['x-scopes'])->toBe(['udb:read,udb:write']);
});

it('withPurpose returns a new instance and preserves the rest', function () {
    $original = UdbMetadata::fromContext('acme', 'u-1', 'corr-1');
    $modified = $original->withPurpose('billing.daily');

    expect($modified)->not->toBe($original)
        ->and($modified->purpose)->toBe('billing.daily')
        ->and($modified->tenantId)->toBe($original->tenantId)
        ->and($modified->correlationId)->toBe($original->correlationId)
        ->and($modified->scopes)->toBe($original->scopes);
});

it('withScopes replaces the scope list cleanly', function () {
    $meta = UdbMetadata::fromContext('acme', 'u-1', 'corr-1')->withScopes(['admin']);
    expect($meta->scopes)->toBe(['admin'])
        ->and($meta->toGrpcMetadata()['x-scopes'])->toBe(['admin']);
});

it('withProjectId overrides the project header', function () {
    $meta = UdbMetadata::fromContext('acme', 'u-1', 'corr-1')->withProjectId('proj-x');
    expect($meta->projectId)->toBe('proj-x')
        ->and($meta->toGrpcMetadata()['x-udb-project-id'])->toBe(['proj-x']);
});

it('sends a fresh x-request-id only when no correlation id is set', function () {
    $bare = new UdbMetadata('t', 'u', 'p', '', [], 'svc', 'proj', '1.0.0');
    $first = $bare->toGrpcMetadata()['x-request-id'][0];
    expect($first)->toMatch('/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/')
        ->and($bare->toGrpcMetadata()['x-request-id'][0])->not->toBe($first);

    $correlated = new UdbMetadata('t', 'u', 'p', 'corr-1', [], 'svc', 'proj', '1.0.0');
    expect($correlated->toGrpcMetadata())->not->toHaveKey('x-request-id');
});
