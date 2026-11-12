<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Support;

/**
 * Recovery after a broker restart.
 *
 * gRPC's reconnect backoff grows to 120s by default: a channel that failed a few
 * dials while the broker was down kept failing calls with UNAVAILABLE for up to
 * two minutes after the broker was back. Every channel this SDK builds caps it at
 * 5s, so it dials again within seconds. A caller's own `channel_options` entry for
 * either key wins.
 *
 * The PHP extension has no call to wake a channel early (grpc-go's
 * `ResetConnectBackoff`), so the cap is the whole of the recovery here.
 */
final class ReconnectBackoff
{
    public const INITIAL_MS = 500;

    public const MAX_MS = 5_000;

    /** @var array<string,int> gRPC core channel arguments. */
    public const CHANNEL_ARGS = [
        'grpc.initial_reconnect_backoff_ms' => self::INITIAL_MS,
        'grpc.max_reconnect_backoff_ms' => self::MAX_MS,
    ];

    /**
     * `$options` with the reconnect cap added where the caller set none.
     *
     * @param  array<string,mixed>  $options
     * @return array<string,mixed>
     */
    public static function apply(array $options): array
    {
        return $options + self::CHANNEL_ARGS;
    }
}
