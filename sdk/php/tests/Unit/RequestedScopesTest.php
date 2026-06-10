<?php

declare(strict_types=1);

use Udb\Core\Authz\Services\V1\AuthzRequest;
use Udb\Core\Authz\Services\V1\Principal;
use Udb\Core\Authz\Services\V1\ResourceRef;

/*
 * requested_scopes population (M9 conformance).
 *
 * UdbAuthClient::decide() builds the AuthzRequest with requested_scopes from
 * the explicit $scopes argument when given, otherwise the bound metadata
 * scopes — parity with the Go/Python/TS can() implementations. The wiring goes
 * through generated protobuf messages, so this test is CI-ready: it skips
 * gracefully when ext-protobuf is not installed (e.g. the lint-only image).
 */

beforeEach(function () {
    if (! extension_loaded('protobuf')) {
        test()->markTestSkipped('ext-protobuf not installed; cannot construct AuthzRequest.');
    }
});

it('populates requested_scopes on the AuthzRequest', function () {
    $scopes = ['udb:read', 'udb:write'];

    $request = (new AuthzRequest())
        ->setPrincipal((new Principal())->setUserId('u1')->setScopes($scopes))
        ->setTenantId('t1')
        ->setProjectId('p1')
        ->setResource((new ResourceRef())->setResourceType('table'))
        ->setAction('read')
        ->setRequestedScopes($scopes);

    $actual = iterator_to_array($request->getRequestedScopes());
    expect($actual)->toBe($scopes);
})->skip(! extension_loaded('protobuf'), 'requires ext-protobuf');

it('carries an empty requested_scopes when none are supplied', function () {
    $request = (new AuthzRequest())
        ->setResource((new ResourceRef())->setResourceType('table'))
        ->setAction('read')
        ->setRequestedScopes([]);

    expect(iterator_to_array($request->getRequestedScopes()))->toBe([]);
})->skip(! extension_loaded('protobuf'), 'requires ext-protobuf');
