<?php

declare(strict_types=1);

// Live broker-restart recovery: a PHP client outlives a broker restart without
// being recreated. The same UdbProject reconnects and keeps working on the data
// plane and the native control plane. Same contract as the Go SDK's
// TestLiveBrokerRestartRecovery.
//
// Runs when UDB_LIVE_SDK_TESTS=1 and UDB_LIVE_BROKER_RESTART_CMD names a command
// (run with sh -c) that restarts the broker on the same addresses and KEEPS its
// database (sessions, users and tenant ids survive). While the broker is down a
// call may fail UNAVAILABLE; any other failure, or no recovery within 90s, fails
// the test.

use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Fahara02\UdbLaravel\Generated\GeneratedClient;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Authn\Entity\V1\TokenType;
use Udb\Core\Authn\Services\V1\ValidateTokenRequest;

function brokerRestartEnv(string $name): string
{
    $value = trim((string) getenv($name));
    if ($value === '') {
        throw new RuntimeException("{$name} is required when UDB_LIVE_SDK_TESTS=1");
    }
    return $value;
}

it('live broker restart recovery', function () {
    $restart = trim((string) getenv('UDB_LIVE_BROKER_RESTART_CMD'));
    $authTarget = brokerRestartEnv('UDB_AUTH_GRPC_TARGET');
    $tenant = brokerRestartEnv('UDB_LIVE_TENANT');
    $project = UdbProject::connect([
        'target' => brokerRestartEnv('UDB_GRPC_TARGET'),
        'authTarget' => $authTarget,
        'tenantId' => $tenant,
        'projectId' => brokerRestartEnv('UDB_LIVE_PROJECT'),
        'purpose' => 'php.live.broker-restart',
        'correlationId' => 'php-live-broker-restart',
        'serviceIdentity' => 'php.sdk.live',
        'deadline_ms' => 5_000,
    ]);
    $project->loginAndAdoptTenant(brokerRestartEnv('UDB_LIVE_USERNAME'), brokerRestartEnv('UDB_LIVE_PASSWORD'));
    // One attempt per probe call: the loop below is the only retry.
    $authn = new GeneratedClient(['endpoint' => $authTarget, 'deadline_ms' => 5_000, 'retry' => ['max_attempts' => 1]]);

    $probes = [
        'data GetCapabilities' => fn () => $project->data()->warmup(),
        'native ValidateSession' => function () use ($project, $authn) {
            $meta = $project->metadata();
            $validated = $authn->validateToken(
                (new ValidateTokenRequest())
                    ->setToken($meta->bearerToken)
                    ->setTokenType(TokenType::TOKEN_TYPE_JWT_ACCESS),
                $meta,
            );
            expect($validated->getValid())->toBeTrue('the session token is not valid');
        },
    ];
    foreach ($probes as $name => $call) {
        try {
            $call();
        } catch (UdbRpcException $e) {
            throw new RuntimeException("before restart, {$name}: code={$e->status}", 0, $e);
        }
    }

    // The command's output goes to stderr so the test itself prints nothing.
    $cmd = proc_open(['sh', '-c', $restart], [0 => ['file', 'php://stdin', 'r'], 1 => STDERR, 2 => STDERR], $pipes);
    expect($cmd)->not->toBeFalse('UDB_LIVE_BROKER_RESTART_CMD could not be started');
    $exit = proc_close($cmd);
    expect($exit)->toBe(0, 'UDB_LIVE_BROKER_RESTART_CMD failed');

    foreach ($probes as $name => $call) {
        $deadline = microtime(true) + 90;
        while (true) {
            try {
                $call();
                break;
            } catch (UdbRpcException $e) {
                if ($e->status !== 14) {
                    throw new RuntimeException("after restart, {$name} on the same project: code={$e->status} {$e->details}", 0, $e);
                }
                if (microtime(true) > $deadline) {
                    throw new RuntimeException("after restart, {$name} stayed UNAVAILABLE for 90s: {$e->details}", 0, $e);
                }
                usleep(250_000);
            }
        }
    }
})->skip(
    getenv('UDB_LIVE_SDK_TESTS') !== '1' || trim((string) getenv('UDB_LIVE_BROKER_RESTART_CMD')) === '',
    'requires a live broker and UDB_LIVE_BROKER_RESTART_CMD',
);
