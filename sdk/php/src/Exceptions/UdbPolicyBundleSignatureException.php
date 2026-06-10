<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Exceptions;

/**
 * Thrown when a {@see \Udb\Core\Authz\Services\V1\SignedPolicyBundle}'s HMAC
 * signature does not match the locally recomputed one — i.e. the bundle was
 * tampered with in transit, or the configured bundle secret does not match the
 * key the broker signed with.
 *
 * This is a security-relevant failure: a mismatched bundle MUST NOT be trusted
 * for local `can()` evaluation. Callers should fall back to live Authorize RPCs
 * (fail-closed) rather than proceeding with an unverified bundle.
 */
final class UdbPolicyBundleSignatureException extends UdbException
{
    public function __construct(
        public readonly string $keyId = '',
        public readonly string $algorithm = '',
        string $message = '',
    ) {
        parent::__construct(
            $message !== ''
                ? $message
                : sprintf(
                    'udb: policy bundle signature verification failed (key_id=%s, algorithm=%s)',
                    $keyId !== '' ? $keyId : '<none>',
                    $algorithm !== '' ? $algorithm : '<none>',
                ),
        );
    }
}
