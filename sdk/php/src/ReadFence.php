<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel;

/**
 * Typed read-your-writes fence carried on a subsequent read (`x-udb-read-fence`).
 * Mirrors the Rust `ReadFence` serde shape: `maxWaitMs` is ALWAYS serialized;
 * `minOutboxLsn`/`projectionTaskIds` are skipped when empty (`skip_serializing_if`).
 *
 * Pairs with {@see WriteReceipt} as the consistency value objects, distinct from
 * the {@see UdbMetadata} header bag.
 */
final class ReadFence
{
    /** Default fence wait, matching the golden fixture + the other SDKs. */
    public const DEFAULT_MAX_WAIT_MS = 2500;

    /**
     * @param  list<string>  $projectionTaskIds
     */
    public function __construct(
        public readonly string $minOutboxLsn = '',
        public readonly array $projectionTaskIds = [],
        public readonly int $maxWaitMs = self::DEFAULT_MAX_WAIT_MS,
    ) {
    }

    /**
     * Build a fence from a receipt, mapping `source_lsn → min_outbox_lsn` (the
     * load-bearing cross-type mapping, not a field rename) and copying the
     * projection task ids.
     */
    public static function fromReceipt(WriteReceipt $receipt, int $maxWaitMs = self::DEFAULT_MAX_WAIT_MS): self
    {
        return new self(
            minOutboxLsn: $receipt->sourceLsn,
            projectionTaskIds: $receipt->projectionTaskIds,
            maxWaitMs: $maxWaitMs,
        );
    }

    /**
     * Serde-shaped array: `max_wait_ms` always; the other two omitted when empty.
     *
     * @return array<string,mixed>
     */
    public function toArray(): array
    {
        $out = ['max_wait_ms' => $this->maxWaitMs];
        if ($this->minOutboxLsn !== '') {
            $out['min_outbox_lsn'] = $this->minOutboxLsn;
        }
        if ($this->projectionTaskIds !== []) {
            $out['projection_task_ids'] = $this->projectionTaskIds;
        }

        return $out;
    }

    public function toJson(): string
    {
        return (string) json_encode($this->toArray());
    }
}
