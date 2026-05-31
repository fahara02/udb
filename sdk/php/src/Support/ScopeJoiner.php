<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Support;

/**
 * Pure helper to join a scope list into the canonical
 * `x-scopes` header value. Matches the Go/Python/TS SDK joiner
 * exactly — comma-separated, no spaces, no quoting.
 *
 * Split out so unit tests can verify edge cases (empty input,
 * single scope, embedded commas) without standing up the whole
 * gRPC client.
 */
final class ScopeJoiner
{
    /**
     * @param  list<string>  $scopes
     */
    public static function join(array $scopes): string
    {
        if ($scopes === []) {
            return '';
        }
        // Filter out empties that creep in from
        // `explode(',', '')`-style env parsing; trim each entry
        // because operators often paste with leading whitespace.
        $cleaned = array_values(array_filter(
            array_map(static fn (string $s): string => trim($s), $scopes),
            static fn (string $s): bool => $s !== '',
        ));
        return implode(',', $cleaned);
    }

    /**
     * Inverse: split an `x-scopes` header value back into a list.
     * Useful inside middleware that forwards scopes between
     * services. Empty input yields an empty list (not `['']`).
     *
     * @return list<string>
     */
    public static function split(string $value): array
    {
        if ($value === '') {
            return [];
        }
        return array_values(array_filter(
            array_map(static fn (string $s): string => trim($s), explode(',', $value)),
            static fn (string $s): bool => $s !== '',
        ));
    }
}
