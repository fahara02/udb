<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Exceptions;

use Udb\Core\Authz\Services\V1\Decision;

/**
 * Thrown by {@see \Fahara02\UdbLaravel\UdbAuthClient::require()} (and the
 * facade's `authz()->require(...)`) when the broker denies an authorization
 * check. Carries the full {@see Decision} so callers can inspect the
 * deny reason, matched policy ids, and required scopes without re-issuing
 * the RPC.
 *
 * gRPC status is fixed to 7 (PERMISSION_DENIED) for parity with the Go,
 * Python, and TypeScript SDKs.
 */
final class UdbAuthzDeniedException extends UdbException
{
    public function __construct(
        public readonly string $resource,
        public readonly string $action,
        public readonly ?Decision $decision = null,
        public readonly string $purpose = '',
    ) {
        $reason = $decision !== null ? $decision->getDenyReason() : '';
        parent::__construct(
            sprintf(
                'UDB authz denied: action=%s resource=%s%s%s',
                $action,
                $resource,
                $purpose !== '' ? " purpose={$purpose}" : '',
                $reason !== '' ? " reason={$reason}" : '',
            ),
            7, // PERMISSION_DENIED
        );
    }

    /** The broker's free-form deny reason, or '' when none was supplied. */
    public function denyReason(): string
    {
        return $this->decision !== null ? $this->decision->getDenyReason() : '';
    }
}
