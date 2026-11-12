<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Fahara02\UdbLaravel\Support\ReconnectBackoff;
use Fahara02\UdbLaravel\Support\SignInRetry;

/*
 * Recovery after a broker restart, mirroring the Go SDK's reconnect tests as far
 * as PHP allows. The PHP SDK owns no renewable credential (no refresh token or
 * password is kept; an API key rides every call), so there is no bearer for it
 * to renew after a refusal. The grpc extension hosts no server, so these run
 * offline: the sign-in retry policy over a scripted sign-in, and the reconnect
 * cap every SDK-built channel carries. The end-to-end proof is the live
 * BrokerRestartTest (tests/Live).
 */

function scriptedSignIn(array $codes, int &$calls): Closure
{
    return function () use ($codes, &$calls) {
        $code = $codes[$calls] ?? 0;
        $calls++;
        if ($code !== 0) {
            throw new UdbRpcException(status: $code, details: "attempt {$calls}", rpcName: 'Login');
        }
        return "signed-in-{$calls}";
    };
}

it('retries a transient sign-in failure and then succeeds', function () {
    $calls = 0;
    $waits = [];
    $result = SignInRetry::run(scriptedSignIn([14, 14], $calls), function (int $ms) use (&$waits) {
        $waits[] = $ms;
    });
    expect($result)->toBe('signed-in-3')
        ->and($calls)->toBe(3)
        ->and($waits)->toHaveCount(2)
        // jittered, doubling: [500,1000] then [1000,2000]
        ->and($waits[0])->toBeGreaterThanOrEqual(500)->toBeLessThanOrEqual(1000)
        ->and($waits[1])->toBeGreaterThanOrEqual(1000)->toBeLessThanOrEqual(2000);
});

it('gives up after three transient failures with the last one', function () {
    $calls = 0;
    $err = null;
    try {
        SignInRetry::run(scriptedSignIn([14, 4, 14, 14], $calls), fn (int $ms) => null);
    } catch (UdbRpcException $e) {
        $err = $e;
    }
    expect($err)->not->toBeNull()
        ->and($err->status)->toBe(14)
        ->and($calls)->toBe(SignInRetry::ATTEMPTS);
});

// Negative: a refused credential is final; it is never repeated.
it('never repeats a refused credential', function () {
    foreach ([16, 7, 3] as $code) {
        $calls = 0;
        $waits = 0;
        $err = null;
        try {
            SignInRetry::run(scriptedSignIn([$code, 0], $calls), function (int $ms) use (&$waits) {
                $waits++;
            });
        } catch (UdbRpcException $e) {
            $err = $e;
        }
        expect($err?->status)->toBe($code)
            ->and($calls)->toBe(1)
            ->and($waits)->toBe(0);
    }
});

it('treats only unreachable, overloaded or lost-reply codes as transient', function () {
    expect(SignInRetry::transient(14))->toBeTrue()   // UNAVAILABLE
        ->and(SignInRetry::transient(4))->toBeTrue() // DEADLINE_EXCEEDED
        ->and(SignInRetry::transient(8))->toBeTrue() // RESOURCE_EXHAUSTED
        ->and(SignInRetry::transient(10))->toBeTrue() // ABORTED
        ->and(SignInRetry::transient(16))->toBeFalse() // UNAUTHENTICATED
        ->and(SignInRetry::transient(7))->toBeFalse() // PERMISSION_DENIED
        ->and(SignInRetry::transient(13))->toBeFalse();
});

it('caps the reconnect backoff of every SDK channel at 5s, caller options winning', function () {
    expect(ReconnectBackoff::apply([]))->toBe([
        'grpc.initial_reconnect_backoff_ms' => 500,
        'grpc.max_reconnect_backoff_ms' => 5000,
    ]);
    $own = ReconnectBackoff::apply(['grpc.max_reconnect_backoff_ms' => 2000, 'credentials' => 'x']);
    expect($own['grpc.max_reconnect_backoff_ms'])->toBe(2000)
        ->and($own['grpc.initial_reconnect_backoff_ms'])->toBe(500)
        ->and($own['credentials'])->toBe('x');
});

it('builds every SDK channel through the reconnect cap', function () {
    $root = dirname(__DIR__, 2).'/src/';
    foreach (['UdbClient.php', 'UdbAuthClient.php', 'UdbProject.php', 'Generated/GeneratedClient.php'] as $file) {
        $source = (string) file_get_contents($root.$file);
        expect(str_contains($source, 'ReconnectBackoff::apply('))
            ->toBeTrue("{$file} builds a channel without the reconnect cap");
    }
});
