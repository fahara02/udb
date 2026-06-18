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
    /**
     * The decoded `udb-error-detail-bin` trailer, when the broker attached one
     * and {@see \Fahara02\UdbLaravel\Generated\GeneratedClient::mapError} set it.
     * Mutable (not promoted/readonly) so `mapError`'s `property_exists(...)`
     * branch can populate it after construction — previously the decode was
     * dropped because no exception class carried this slot.
     */
    public ?object $errorDetail = null;

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
     * Whether the decoded {@see $errorDetail} flags this error retryable.
     * Lets callers branch on the typed field instead of string-matching
     * {@see $details}. False when no detail was decoded.
     */
    public function isRetryable(): bool
    {
        $ed = $this->errorDetail;
        if ($ed === null || ! method_exists($ed, 'getRetryable')) {
            return false;
        }

        return (bool) $ed->getRetryable();
    }

    /**
     * The decoded {@see \Udb\Entity\V1\ErrorKind} enum name (e.g.
     * "ERROR_KIND_CAPABILITY"), or '' when no detail was decoded.
     */
    public function kind(): string
    {
        $ed = $this->errorDetail;
        if ($ed === null || ! method_exists($ed, 'getKind')) {
            return '';
        }
        $kind = (int) $ed->getKind();
        $fqn = '\\Udb\\Entity\\V1\\ErrorKind';
        if (class_exists($fqn) && method_exists($fqn, 'name')) {
            try {
                return (string) $fqn::name($kind);
            } catch (\Throwable) {
                return '';
            }
        }

        return (string) $kind;
    }

    /**
     * Convenience constructor from a gRPC status value as returned
     * by `\Grpc\UnaryCall::wait()`'s second return value.
     *
     * PHP gRPC may return either an array-like status or a stdClass,
     * depending on extension/runtime version.
     */
    public static function fromGrpcStatus(mixed $status, string $rpcName): self
    {
        $code = is_object($status) ? ($status->code ?? -1) : ($status['code'] ?? -1);
        $details = is_object($status) ? ($status->details ?? '') : ($status['details'] ?? '');

        $exception = new self(
            status: (int) $code,
            details: (string) $details,
            raw: $status,
            rpcName: $rpcName,
        );
        // Decode the binary `udb-error-detail-bin` trailer into the typed
        // ErrorDetail so EVERY non-OK path (hand-written facade AND the generated
        // client's mapError) surfaces the same typed diagnostics — no more
        // dropped decode.
        $detail = self::decodeErrorDetail($status);
        if ($detail !== null) {
            $exception->errorDetail = $detail;
        }

        return $exception;
    }

    /**
     * Extract + decode the `udb-error-detail-bin` trailer from a gRPC status.
     *
     * @return object|null  a \Udb\Entity\V1\ErrorDetail or null
     */
    public static function decodeErrorDetail(mixed $status): ?object
    {
        $metadata = is_object($status) ? ($status->metadata ?? null) : ($status['metadata'] ?? null);
        if (! is_array($metadata)) {
            return null;
        }
        $values = $metadata['udb-error-detail-bin'] ?? null;
        if (! is_array($values) || $values === []) {
            return null;
        }
        $fqn = '\\Udb\\Entity\\V1\\ErrorDetail';
        if (! class_exists($fqn)) {
            return null;
        }
        try {
            /** @var \Google\Protobuf\Internal\Message $detail */
            $detail = new $fqn();
            $detail->mergeFromString((string) $values[0]);

            return $detail;
        } catch (\Throwable) {
            return null;
        }
    }
}
