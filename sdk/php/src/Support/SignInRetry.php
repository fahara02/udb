<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Support;

use Fahara02\UdbLaravel\Exceptions\UdbRpcException;

/**
 * Retry policy for signing in ({@see \Fahara02\UdbLaravel\UdbProject::loginAndAdoptTenant()}).
 *
 * A service that starts while the broker is restarting must still sign in. Up to
 * {@see self::ATTEMPTS} attempts are made, waiting a jittered, doubling interval
 * between them, and only a transient failure is repeated: the broker was
 * unreachable or restarting, overloaded, or the reply was lost. A refused
 * credential is final and thrown at once. Same policy as the Go SDK's
 * ConnectEnterprise.
 *
 * @internal
 */
final class SignInRetry
{
    public const ATTEMPTS = 3;

    private const BACKOFF_BASE_MS = 1_000;

    private const BACKOFF_CAP_MS = 60_000;

    /** gRPC codes after which repeating a sign-in can help. */
    private const TRANSIENT = [
        4,  // DEADLINE_EXCEEDED
        8,  // RESOURCE_EXHAUSTED
        10, // ABORTED
        14, // UNAVAILABLE
    ];

    /**
     * Run `$signIn` under the retry policy.
     *
     * @template T
     * @param  callable():T  $signIn
     * @param  (\Closure(int):void)|null  $sleep  waits the given milliseconds; null means usleep
     * @return T
     */
    public static function run(callable $signIn, ?\Closure $sleep = null): mixed
    {
        $last = null;
        for ($attempt = 0; $attempt < self::ATTEMPTS; $attempt++) {
            if ($attempt > 0) {
                $wait = self::backoffMs($attempt);
                $sleep !== null ? $sleep($wait) : usleep($wait * 1000);
            }
            try {
                return $signIn();
            } catch (UdbRpcException $e) {
                if (! self::transient($e->status)) {
                    throw $e;
                }
                $last = $e;
            }
        }
        /** @var UdbRpcException $last at least one attempt ran and failed transiently */
        throw $last;
    }

    public static function transient(int $code): bool
    {
        return in_array($code, self::TRANSIENT, true);
    }

    /** Wait after the n-th consecutive failure: random in [ceiling/2, ceiling], ceiling = base * 2^(n-1), capped. */
    public static function backoffMs(int $failures): int
    {
        $ceiling = self::BACKOFF_BASE_MS;
        for ($i = 1; $i < $failures && $ceiling < self::BACKOFF_CAP_MS; $i++) {
            $ceiling *= 2;
        }
        $ceiling = min($ceiling, self::BACKOFF_CAP_MS);
        $half = intdiv($ceiling, 2);
        return $half + random_int(0, $ceiling - $half);
    }
}
