<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel;

/**
 * Typed durability receipt returned by a mutation
 * (`MutationResponse.write_receipt_json`). Mirrors the Rust `WriteReceipt` serde
 * shape: all five fields are always present. The load-bearing `sourceLsn` is
 * mapped into a {@see ReadFence}'s `minOutboxLsn` by {@see ReadFence::fromReceipt}.
 *
 * Distinct from {@see UdbMetadata}: this and {@see ReadFence} are the consistency
 * value objects (one code path), separate from the request-metadata header bag.
 */
final class WriteReceipt
{
    /**
     * @param  list<string>  $projectionTaskIds
     */
    public function __construct(
        public readonly string $sourceLsn = '',
        public readonly int $outboxSeq = 0,
        public readonly array $projectionTaskIds = [],
        public readonly string $manifestChecksum = '',
        public readonly int $writtenAtUnixMs = 0,
    ) {
    }

    /** Parse a `write_receipt_json` string into a typed receipt. */
    public static function fromJson(string $json): self
    {
        if ($json === '') {
            return new self();
        }
        /** @var array<string,mixed> $data */
        $data = (array) (json_decode($json, true) ?? []);

        return self::fromArray($data);
    }

    /** @param array<string,mixed> $data */
    public static function fromArray(array $data): self
    {
        /** @var list<string> $ids */
        $ids = array_values((array) ($data['projection_task_ids'] ?? []));

        return new self(
            sourceLsn: (string) ($data['source_lsn'] ?? ''),
            outboxSeq: (int) ($data['outbox_seq'] ?? 0),
            projectionTaskIds: $ids,
            manifestChecksum: (string) ($data['manifest_checksum'] ?? ''),
            writtenAtUnixMs: (int) ($data['written_at_unix_ms'] ?? 0),
        );
    }

    /**
     * All five fields, always present (matches the Rust serde shape).
     *
     * @return array{source_lsn:string, outbox_seq:int, projection_task_ids:list<string>, manifest_checksum:string, written_at_unix_ms:int}
     */
    public function toArray(): array
    {
        return [
            'source_lsn' => $this->sourceLsn,
            'outbox_seq' => $this->outboxSeq,
            'projection_task_ids' => $this->projectionTaskIds,
            'manifest_checksum' => $this->manifestChecksum,
            'written_at_unix_ms' => $this->writtenAtUnixMs,
        ];
    }

    public function toJson(): string
    {
        return (string) json_encode($this->toArray());
    }
}
