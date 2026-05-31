<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Exceptions;

/**
 * Thrown when a unary RPC returns a non-OK gRPC status. The
 * `status` int is the `\Grpc\STATUS_*` constant the broker
 * returned (so callers can `if ($e->status === STATUS_NOT_FOUND)`);
 * `details` is the broker's free-form message.
 *
 * The original gRPC status object is preserved on `$raw` for
 * callers that need trailing metadata or the binary status
 * details (e.g. for distributed tracing).
 */
final class UdbRpcException extends UdbException
{
    public function __construct(
        public readonly int $status,
        public readonly string $details,
        public readonly mixed $raw = null,
        string $rpcName = 'unknown',
    ) {
        parent::__construct(
            sprintf('UDB %s failed: gRPC status=%d details=%s', $rpcName, $status, $details),
            $status,
        );
    }

    /**
     * Convenience constructor from a gRPC status array as returned
     * by `\Grpc\UnaryCall::wait()`'s second return value.
     *
     * @param  array{code:int,details:string,metadata?:array<string,mixed>}  $status
     */
    public static function fromGrpcStatus(array $status, string $rpcName): self
    {
        return new self(
            status: $status['code'],
            details: $status['details'],
            raw: $status,
            rpcName: $rpcName,
        );
    }
}
