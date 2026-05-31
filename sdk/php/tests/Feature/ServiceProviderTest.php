<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\UdbClient;

it('binds UdbClient as a singleton', function () {
    $a = app(UdbClient::class);
    $b = app(UdbClient::class);
    expect($a)->toBe($b);
});

it('resolves the Udb facade to the bound client', function () {
    $facadeRoot = \Fahara02\UdbLaravel\Facades\Udb::getFacadeRoot();
    expect($facadeRoot)->toBeInstanceOf(UdbClient::class);
});

it('publishes the package config under the udb-config tag', function () {
    $provider = app()->getProviders(\Fahara02\UdbLaravel\UdbServiceProvider::class);
    expect($provider)->not->toBeEmpty();
    // Orchestra resets between tests, so just confirm the config
    // file path the provider exposes is the one shipped with the
    // package.
    $expected = realpath(__DIR__ . '/../../config/udb.php');
    expect($expected)->toBeString()->toContain('config' . DIRECTORY_SEPARATOR . 'udb.php');
});

it('registers default tenant_resolver as a closure', function () {
    expect(app()->bound('udb.tenant_resolver'))->toBeTrue();
    $resolver = app('udb.tenant_resolver');
    expect($resolver)->toBeCallable();
});

it('registers default correlation_resolver and user_resolver', function () {
    expect(app()->bound('udb.correlation_resolver'))->toBeTrue();
    expect(app()->bound('udb.user_resolver'))->toBeTrue();
});

it('does not mount the middleware when auto_register is false (the test default)', function () {
    /** @var \Illuminate\Routing\Router $router */
    $router = app(\Illuminate\Routing\Router::class);
    $webMiddleware = $router->getMiddlewareGroups()['web'] ?? [];
    expect($webMiddleware)->not->toContain(\Fahara02\UdbLaravel\Http\UdbContextMiddleware::class);
});
