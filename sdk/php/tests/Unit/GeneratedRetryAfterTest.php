<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Fahara02\UdbLaravel\Generated\GeneratedClient;
use Fahara02\UdbLaravel\UdbMetadata;

/*
 * The generated client's unary retry honours the broker's ErrorDetail
 * `retry_after_ms` (rate limit / fair admission / overload refusals):
 *   (a) wait = max(own jittered backoff, broker delay)            -> real wait
 *   (b) a broker delay past the call's timeout is not slept      -> error now
 *   (c) a delay above retry.max_retry_after_ms (default 10s)      -> error now
 *
 * Drives the real private invokeUnary() offline: the client is built without
 * its grpc-extension-gated constructor and the invoker returns a scripted call.
 */

$needsProto = function (): void {
    if (! class_exists(\Udb\Entity\V1\ErrorDetail::class) || ! class_exists(\Grpc\BaseStub::class)) {
        test()->markTestSkipped('generated protobuf messages / grpc stubs not available.');
    }
};

/** A GeneratedClient with $config and a recording (or real, when $record = false) sleep. */
function retryAfterClient(array $config, ?array &$sleeps = null, bool $record = true): GeneratedClient
{
    $c = (new ReflectionClass(GeneratedClient::class))->newInstanceWithoutConstructor();
    $stub = (new ReflectionClass(\Grpc\BaseStub::class))->newInstanceWithoutConstructor();
    $sleeps = [];
    $sleepsRef = &$sleeps;
    (function () use ($config, $stub, $record, &$sleepsRef): void {
        $this->config = $config;
        $this->stubs['\\Udb\\Services\\V1\\DataBrokerClient'] = $stub;
        if ($record) {
            $this->retrySleep = function (int $ms) use (&$sleepsRef): void {
                $sleepsRef[] = $ms;
            };
        }
    })->call($c);

    return $c;
}

function quotaStatus(int $code, int $retryAfterMs): object
{
    $ed = new \Udb\Entity\V1\ErrorDetail();
    $ed->setBackend('admission');
    $ed->setOperation('tenant budget');
    $ed->setRetryable(true);
    $ed->setRetryAfterMs($retryAfterMs);
    $ed->setKind(\Udb\Entity\V1\ErrorKind::ERROR_KIND_QUOTA);

    return (object) [
        'code' => $code,
        'details' => 'rate limited',
        'metadata' => ['udb-error-detail-bin' => [$ed->serializeToString()]],
    ];
}

/**
 * Run invokeUnary for read-only Select against scripted statuses (past the end:
 * OK). Returns [result-or-exception, attempt timestamps in ns].
 */
function runScripted(GeneratedClient $c, array $statuses, string $rpc = 'Select', bool $readOnly = true): array
{
    $times = [];
    $invoker = function ($stub, $md, $opts) use (&$times, $statuses) {
        $i = count($times);
        $times[] = hrtime(true);
        $status = $statuses[$i] ?? (object) ['code' => 0, 'details' => '', 'metadata' => []];
        $response = $status->code === 0 ? new stdClass() : null;

        return new class ($response, $status) {
            public function __construct(private mixed $response, private object $status)
            {
            }

            public function wait(): array
            {
                return [$this->response, $this->status];
            }

            public function getMetadata(): array
            {
                return [];
            }
        };
    };
    $meta = new UdbMetadata('t', 'u', 'p', 'c', [], 'svc', 'proj', '1.0.0');
    $m = new ReflectionMethod($c, 'invokeUnary');
    $m->setAccessible(true);
    try {
        $out = $m->invokeArgs($c, [$rpc, 'DataBroker', 'udb.services.v1', $invoker, $meta, $readOnly, null]);
    } catch (UdbRpcException $e) {
        $out = $e;
    }

    return [$out, $times];
}

$fast = ['endpoint' => 'localhost:0', 'deadline_ms' => 30_000, 'retry' => ['base_delay_ms' => 1, 'max_delay_ms' => 1]];

it('(a) honours the broker retry_after_ms with a real wait (3 runs)', function () use ($needsProto, $fast) {
    $needsProto();
    for ($run = 0; $run < 3; $run++) {
        $c = retryAfterClient($fast, $sleeps, record: false);
        [$out, $times] = runScripted($c, [quotaStatus(8, 1200)]);
        expect($out)->toBeInstanceOf(stdClass::class)
            ->and(count($times))->toBe(2)
            ->and(intdiv($times[1] - $times[0], 1_000_000))->toBeGreaterThanOrEqual(1200);
    }
});

it('(b) does not sleep past the call timeout and keeps the detail', function () use ($needsProto, $fast) {
    $needsProto();
    $c = retryAfterClient(['deadline_ms' => 500] + $fast, $sleeps);
    [$out, $times] = runScripted($c, [quotaStatus(8, 1200)]);
    expect($out)->toBeInstanceOf(UdbRpcException::class)
        ->and($out->status)->toBe(8)
        ->and($out->retryAfterMs())->toBe(1200)
        ->and(count($times))->toBe(1)
        ->and($sleeps)->toBe([]);
});

it('(c) returns a delay above max_retry_after_ms at once (default 10s)', function () use ($needsProto, $fast) {
    $needsProto();
    $cfg = $fast;
    $cfg['retry']['max_retry_after_ms'] = 50;
    $c = retryAfterClient($cfg, $sleeps);
    [$out, $times] = runScripted($c, [quotaStatus(8, 5000)]);
    expect($out)->toBeInstanceOf(UdbRpcException::class)
        ->and($out->status)->toBe(8)
        ->and($out->retryAfterMs())->toBe(5000)
        ->and(count($times))->toBe(1)
        ->and($sleeps)->toBe([]);

    $cfg = ['deadline_ms' => 0] + $fast; // no timeout: only the default cap applies
    $c = retryAfterClient($cfg, $sleeps);
    [$out, $times] = runScripted($c, [quotaStatus(8, 60_000)]);
    expect($out)->toBeInstanceOf(UdbRpcException::class)
        ->and($out->status)->toBe(8)
        ->and($out->retryAfterMs())->toBe(60_000)
        ->and(count($times))->toBe(1)
        ->and($sleeps)->toBe([]);

    // A delay at the default cap is still waited for.
    $c = retryAfterClient($cfg, $sleeps);
    [$out] = runScripted($c, [quotaStatus(8, 10_000)]);
    expect($out)->toBeInstanceOf(stdClass::class)->and($sleeps)->toBe([10_000]);
});

it('keeps retry safety: a non-replay-safe mutation is not retried despite a delay', function () use ($needsProto, $fast) {
    $needsProto();
    $c = retryAfterClient($fast, $sleeps);
    [$out, $times] = runScripted($c, [quotaStatus(8, 1200)], rpc: 'DeletePolicy', readOnly: false);
    expect($out)->toBeInstanceOf(UdbRpcException::class)
        ->and($out->retryAfterMs())->toBe(1200)
        ->and(count($times))->toBe(1)
        ->and($sleeps)->toBe([]);
});

it('exposes retryAfterMs() as 0 when no detail was decoded', function () {
    expect((new UdbRpcException(8, 'x'))->retryAfterMs())->toBe(0);
});
