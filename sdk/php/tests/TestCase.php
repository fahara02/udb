<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Tests;

use Fahara02\UdbLaravel\UdbServiceProvider;
use Orchestra\Testbench\TestCase as Orchestra;

/**
 * Base TestCase for SDK tests. Boots a minimal Laravel app via
 * Orchestra Testbench and registers the SDK's ServiceProvider so
 * unit + feature tests can resolve `UdbClient` from the
 * container without standing up a real broker.
 *
 * Tests that exercise the gRPC stub use a transport double
 * (`FakeStub`) so no network is touched and the suite stays
 * fast + deterministic.
 */
abstract class TestCase extends Orchestra
{
    /**
     * @param  \Illuminate\Foundation\Application  $app
     * @return list<class-string>
     */
    protected function getPackageProviders($app): array
    {
        return [UdbServiceProvider::class];
    }

    /**
     * @param  \Illuminate\Foundation\Application  $app
     * @return array<string,class-string>
     */
    protected function getPackageAliases($app): array
    {
        return [
            'Udb' => \Fahara02\UdbLaravel\Facades\Udb::class,
        ];
    }

    /**
     * @param  \Illuminate\Foundation\Application  $app
     */
    protected function defineEnvironment($app): void
    {
        // Static, deterministic config so test expectations are
        // independent of the environment running them.
        $app['config']->set('udb.endpoint', '127.0.0.1:50051');
        $app['config']->set('udb.metadata.service_identity', 'test.service');
        $app['config']->set('udb.metadata.default_project_id', 'test-project');
        $app['config']->set('udb.metadata.default_purpose', 'test.purpose');
        $app['config']->set('udb.metadata.default_scopes', ['udb:read']);
        $app['config']->set('udb.metadata.client_catalog_version', '1.0.0');
        $app['config']->set('udb.middleware.auto_register', false);
    }
}
