<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\UdbAuthClient;
use Udb\Core\Authz\Services\V1\SignedPolicyBundle;

/*
 * Policy-bundle signature verification (M5.3).
 *
 * The signing contract is pure (lowercase-hex HMAC-SHA256 over the raw bundle
 * bytes), so the algorithm is asserted directly with no extensions required —
 * this matches what UdbAuthClient::verifyPolicyBundle() recomputes and what the
 * Rust broker emits (runtime::authz::bundle: hex_lower(hmac_sha256(secret,
 * bundle))).
 *
 * The end-to-end UdbAuthClient::verifyPolicyBundle() cases construct a real
 * SignedPolicyBundle message and an UdbAuthClient (ext-protobuf + ext-grpc) and
 * skip gracefully when those extensions are absent.
 */

it('recomputes the broker signing contract: lowercase-hex HMAC-SHA256', function () {
    $bundle = '{"policies":[],"version":"v1"}';
    $secret = 'topsecret';

    // The exact expression UdbAuthClient::verifyPolicyBundle uses internally.
    $sig = hash_hmac('sha256', $bundle, $secret);

    expect($sig)
        ->toBe(strtolower($sig))                 // lowercase
        ->and(strlen($sig))->toBe(64)            // 32 bytes => 64 hex chars
        ->and($sig)->toMatch('/^[0-9a-f]{64}$/');

    // Tampering the payload changes the signature (no second-preimage shortcut).
    $tampered = hash_hmac('sha256', $bundle . 'x', $secret);
    expect($tampered)->not->toBe($sig);

    // A timing-safe compare accepts the correct sig and rejects a wrong one.
    expect(hash_equals($sig, hash_hmac('sha256', $bundle, $secret)))->toBeTrue();
    expect(hash_equals($sig, $tampered))->toBeFalse();
});

it('accepts a correctly-signed bundle and rejects a tampered one', function () {
    if (! extension_loaded('protobuf') || ! extension_loaded('grpc')) {
        test()->markTestSkipped('verifyPolicyBundle integration needs ext-protobuf + ext-grpc.');
    }

    $secret = 'topsecret';
    $payload = '{"policies":[],"version":"v1"}';
    $goodSig = hash_hmac('sha256', $payload, $secret);

    $client = new UdbAuthClient(['endpoint' => '127.0.0.1:50051']);

    $good = (new SignedPolicyBundle())->setBundle($payload)->setSignature($goodSig);
    expect($client->verifyPolicyBundle($good, $secret))->toBeTrue();

    // Tampered payload (signature now over the wrong bytes).
    $tamperedBundle = (new SignedPolicyBundle())
        ->setBundle($payload . 'TAMPER')
        ->setSignature($goodSig);
    expect($client->verifyPolicyBundle($tamperedBundle, $secret))->toBeFalse();

    // Wrong secret.
    expect($client->verifyPolicyBundle($good, 'wrong-secret'))->toBeFalse();

    // Empty secret never verifies (fail-closed).
    expect($client->verifyPolicyBundle($good, ''))->toBeFalse();
})->skip(
    ! extension_loaded('protobuf') || ! extension_loaded('grpc'),
    'requires ext-protobuf + ext-grpc',
);
