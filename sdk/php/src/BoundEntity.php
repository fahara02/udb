<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel;

use Udb\Entity\V1\DeleteRequest;
use Udb\Entity\V1\MutationResponse;
use Udb\Entity\V1\RecordSet;
use Udb\Entity\V1\SelectRequest;
use Udb\Entity\V1\UpsertRequest;

/**
 * A message type/table bound to a {@see UdbClient}, taking plain PHP arrays.
 *
 * Returned by {@see UdbClient::entity()} / {@see UdbClient::table()}. Its
 * `select`/`upsert`/`delete` build the proto requests from arrays (json-encoding
 * the record into `record_json`, building the `google.protobuf.Struct` filter,
 * setting `conflict_fields` from the bound key) so callers no longer construct
 * proto requests by hand. Binds once; no per-request catalog lookup.
 */
final class BoundEntity
{
    /**
     * @param  list<string>  $key
     */
    public function __construct(
        private readonly UdbClient $client,
        private readonly string $messageType,
        private readonly array $key = [],
    ) {
    }

    /**
     * One `Select`, returning the broker's `RecordSet`.
     *
     * @param  array<string,mixed>  $where
     * @param  list<string>  $fields
     */
    public function select(array $where = [], array $fields = [], int $limit = 0, ?UdbMetadata $metadata = null): RecordSet
    {
        $request = (new SelectRequest())
            ->setMessageType($this->messageType)
            ->setFilter(UdbClient::toStruct($where))
            ->setFields($fields)
            ->setLimit($limit);

        return $this->client->select($request, $metadata);
    }

    /**
     * One `Upsert` with `conflict_fields` from the bound key.
     *
     * @param  array<string,mixed>  $record
     */
    public function upsert(array $record, bool $returnRecord = false, string $idempotencyKey = '', ?UdbMetadata $metadata = null): MutationResponse
    {
        $request = (new UpsertRequest())
            ->setMessageType($this->messageType)
            ->setRecordJson(UdbClient::encodeRecordJson($record))
            ->setConflictFields($this->key)
            ->setReturnRecord($returnRecord)
            ->setIdempotencyKey($idempotencyKey);

        return $this->client->upsert($request, $metadata);
    }

    /**
     * One `Delete` filtered by `$where`.
     *
     * @param  array<string,mixed>  $where
     */
    public function delete(array $where = [], string $idempotencyKey = '', ?UdbMetadata $metadata = null): MutationResponse
    {
        $request = (new DeleteRequest())
            ->setMessageType($this->messageType)
            ->setFilter(UdbClient::toStruct($where))
            ->setIdempotencyKey($idempotencyKey);

        return $this->client->delete($request, $metadata);
    }
}
