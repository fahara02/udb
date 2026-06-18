<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\Generated\GeneratedClient;

/*
 * R1.2 (PHP): replay-safe mutation retry gate for the generated client.
 *
 * Proves the proto-driven retry predicate offline (no broker / gRPC channel):
 *   (1) replay-safe mutation + non-empty idempotency key  → retries-then-succeeds
 *   (2) replay-safe mutation WITHOUT a key                 → no retry (fail-safe)
 *   (3) non-replay-safe mutation (even with a key)         → no retry
 *   (4) read-only RPC                                       → unchanged
 *
 * The decision lives in GeneratedClient::isRetryable() (transient-code gate keyed
 * on read-only / replay-safe / idempotency-key) and hasIdempotencyKey(). Both are
 * private; we drive them by reflection on an instance built WITHOUT the
 * constructor, so no `grpc` extension or live channel is required. The replay-safe
 * map is asserted against the proto-derived GeneratedClient::REPLAY_SAFE so the
 * proof is grounded in the regenerated per-RPC data, not a name guess.
 */

$needsProto = function (): void {
    if (! class_exists(\Udb\Entity\V1\UpsertRequest::class)) {
        test()->markTestSkipped('generated protobuf messages not available.');
    }
};

/** Build a GeneratedClient without running its grpc-extension-gated constructor. */
function retryClient(): GeneratedClient
{
    return (new ReflectionClass(GeneratedClient::class))->newInstanceWithoutConstructor();
}

/** Invoke a private method on the client via reflection. */
function callPrivate(GeneratedClient $c, string $method, array $args): mixed
{
    $m = new ReflectionMethod($c, $method);
    $m->setAccessible(true);

    return $m->invokeArgs($c, $args);
}

/**
 * Drive the SAME retry decision the production invokeUnary() loop uses
 * (src/Generated/GeneratedClient.php), counting attempts. $codes is the gRPC
 * status code returned per attempt; 0 = OK (success). Returns the attempt count.
 */
function simulateRetry(
    GeneratedClient $c,
    array $codes,
    bool $readOnly,
    bool $replaySafe,
    bool $hasKey,
    int $maxAttempts = 4,
): array {
    $attempts = 0;
    $succeeded = false;
    for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
        $attempts++;
        $code = $codes[$attempt - 1] ?? end($codes);
        if ($code === 0) {
            $succeeded = true;
            break;
        }
        if ($attempt < $maxAttempts
            && callPrivate($c, 'isRetryable', [$code, $readOnly, $replaySafe, $hasKey])) {
            continue;
        }
        break;
    }

    return ['attempts' => $attempts, 'succeeded' => $succeeded];
}

// ── replay-safe map is proto-derived (Upsert/Delete = true; reads absent) ────
it('exposes the regenerated per-RPC replay-safe map', function () {
    expect(GeneratedClient::REPLAY_SAFE['Upsert'] ?? false)->toBeTrue()
        ->and(GeneratedClient::REPLAY_SAFE['Delete'] ?? false)->toBeTrue()
        // A read-only RPC is never marked replay-safe.
        ->and(GeneratedClient::REPLAY_SAFE['Select'] ?? false)->toBeFalse()
        // A non-replay-safe mutation is absent (fail-closed default).
        ->and(GeneratedClient::REPLAY_SAFE['DeletePolicy'] ?? false)->toBeFalse()
        // operation_kind classification still drives read-only.
        ->and(GeneratedClient::OPERATION_KIND['Upsert'])->toBe('mutation')
        ->and(GeneratedClient::OPERATION_KIND['Select'])->toBe('read_only');
});

// ── hasIdempotencyKey reads the proto accessor (no per-message imports) ──────
it('detects a non-empty idempotency key on the request proto', function () use ($needsProto) {
    $needsProto();
    $c = retryClient();

    $keyed = new \Udb\Entity\V1\UpsertRequest();
    $keyed->setIdempotencyKey('idem-1');
    expect(callPrivate($c, 'hasIdempotencyKey', [$keyed]))->toBeTrue();

    // Same message type, empty key → gate not satisfied.
    expect(callPrivate($c, 'hasIdempotencyKey', [new \Udb\Entity\V1\UpsertRequest()]))->toBeFalse();

    // A request without the field never satisfies the gate.
    expect(callPrivate($c, 'hasIdempotencyKey', [new \Udb\Entity\V1\SelectRequest()]))->toBeFalse();

    // Non-object / null inputs are safe.
    expect(callPrivate($c, 'hasIdempotencyKey', [null]))->toBeFalse();
});

// ── predicate: the four required scenarios ──────────────────────────────────
it('gates mutation retry on replay-safe AND idempotency key', function () {
    $c = retryClient();

    // Replay-safe mutation WITH key retries on the configured transient codes.
    foreach ([14 /*UNAVAILABLE*/, 8 /*RESOURCE_EXHAUSTED*/] as $code) {
        expect(callPrivate($c, 'isRetryable', [$code, false, true, true]))->toBeTrue();
    }
    // Same mutation WITHOUT a key → no retry.
    expect(callPrivate($c, 'isRetryable', [14, false, true, false]))->toBeFalse();
    // Non-replay-safe mutation with a key → no retry.
    expect(callPrivate($c, 'isRetryable', [14, false, false, true]))->toBeFalse();
    // Replay-safe + keyed mutation does NOT retry on DEADLINE_EXCEEDED (write may
    // have landed server-side; fail-closed terminal).
    expect(callPrivate($c, 'isRetryable', [4, false, true, true]))->toBeFalse();
});

it('leaves read-only retry behaviour unchanged', function () {
    $c = retryClient();

    // Reads retry on the transient codes plus DEADLINE_EXCEEDED.
    foreach ([14, 8, 4] as $code) {
        expect(callPrivate($c, 'isRetryable', [$code, true, false, false]))->toBeTrue();
    }
    // Non-transient codes are never retried, even for reads.
    expect(callPrivate($c, 'isRetryable', [3 /*INVALID_ARGUMENT*/, true, false, false]))->toBeFalse();
});

// ── full-loop attempt-count proof (mirrors invokeUnary's loop) ───────────────
it('(1) replay-safe mutation + key retries then succeeds', function () {
    $c = retryClient();
    // Transient UNAVAILABLE on the first attempt, OK on the second.
    $r = simulateRetry($c, [14, 0], readOnly: false, replaySafe: true, hasKey: true);
    expect($r['attempts'])->toBe(2)->and($r['succeeded'])->toBeTrue();
});

it('(2) replay-safe mutation without key does not retry', function () {
    $c = retryClient();
    // Would succeed on attempt 2 if it retried — it must not.
    $r = simulateRetry($c, [14, 0], readOnly: false, replaySafe: true, hasKey: false);
    expect($r['attempts'])->toBe(1)->and($r['succeeded'])->toBeFalse();
});

it('(3) non-replay-safe mutation does not retry even with a key', function () {
    $c = retryClient();
    $r = simulateRetry($c, [14, 0], readOnly: false, replaySafe: false, hasKey: true);
    expect($r['attempts'])->toBe(1)->and($r['succeeded'])->toBeFalse();
});

it('(4) read-only retries then succeeds (unchanged)', function () {
    $c = retryClient();
    // UNAVAILABLE, then DEADLINE_EXCEEDED, then OK.
    $r = simulateRetry($c, [14, 4, 0], readOnly: true, replaySafe: false, hasKey: false);
    expect($r['attempts'])->toBe(3)->and($r['succeeded'])->toBeTrue();
});
