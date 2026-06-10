<?php

/**
 * UDB PHP quickstart — Example 1: basic CRUD.
 *
 * One table ("shop"."customers", from proto/shop/v1/customer.proto), four
 * operations: create, read, update, delete. This is the smallest thing that
 * talks to the broker and back.
 *
 *   docker compose up -d                         # Postgres + Redis
 *   ./scripts/serve-broker.ps1                   # broker (separate terminal)
 *   $env:UDB_TARGET = "127.0.0.1:50051"; php 01_crud.php
 */

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use Fahara02\UdbLaravel\UdbClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Google\Protobuf\Struct;
use PhpQuickstart\Shop\V1\Customer;
use Udb\Entity\V1\DeleteRequest;
use Udb\Entity\V1\SelectRequest;
use Udb\Entity\V1\UpsertRequest;

// The fully-qualified proto name is the broker's catalog key for the table.
const MESSAGE_TYPE = 'shop.v1.Customer';

$target = getenv('UDB_TARGET') ?: '127.0.0.1:50051';

// One long-lived client per process; the gRPC channel multiplexes every call.
$client = new UdbClient([
    'endpoint'    => $target,
    'tls'         => ['enabled' => false],
    'deadline_ms' => 30_000,
]);

// Per-request context the broker requires on every call. `udb:read` + `udb:write`
// are the operation scopes — Example 2 shows what happens without them.
$metadata = new UdbMetadata(
    tenantId: 'quickstart',
    userId: 'php-quickstart',
    purpose: 'quickstart.crud',
    correlationId: 'php-quickstart-' . getmypid(),
    scopes: ['udb:read', 'udb:write'],
    serviceIdentity: 'examples.php.quickstart',
    projectId: 'default',
    clientCatalogVersion: '1.0.0',
);

// We own the primary key so every operation targets the same row and re-runs
// are idempotent. (Omit customer_id and the DB fills it with gen_random_uuid(),
// but then you'd need a different stable key to read/update/delete by.)
$customerId = uuidV4();

// ── CREATE ───────────────────────────────────────────────────────────────────
$customer = (new Customer())
    ->setCustomerId($customerId)
    ->setEmail('ada@example.com')
    ->setFullName('Ada Lovelace')
    ->setLoyaltyPoints(100);

$created = $client->upsert(new UpsertRequest([
    'message_type'    => MESSAGE_TYPE,
    'record_json'     => customerToJson($customer),
    'conflict_fields' => ['customer_id'],   // ON CONFLICT (customer_id) DO UPDATE …
    'return_record'   => true,
    'idempotency_key' => "create-{$customerId}",
]), $metadata);
printf("created   affected_rows=%d\n", $created->getAffectedRows());

// ── READ ─────────────────────────────────────────────────────────────────────
$rows = $client->select(new SelectRequest([
    'message_type' => MESSAGE_TYPE,
    'filter'       => filterById($customerId),
    'limit'        => 10,
]), $metadata);
printf("read      rows=%d  %s\n", recordCount($rows), firstRecord($rows));

// ── UPDATE ───────────────────────────────────────────────────────────────────
// Same PK, new values — the conflict on customer_id turns it into an UPDATE.
$customer->setFullName('Augusta Ada King')->setLoyaltyPoints(250);
$client->upsert(new UpsertRequest([
    'message_type'    => MESSAGE_TYPE,
    'record_json'     => customerToJson($customer),
    'conflict_fields' => ['customer_id'],
    'return_record'   => true,
    'idempotency_key' => "update-{$customerId}",
]), $metadata);

$rows = $client->select(new SelectRequest([
    'message_type' => MESSAGE_TYPE,
    'filter'       => filterById($customerId),
    'limit'        => 10,
]), $metadata);
if (! recordsContain($rows, 'Augusta Ada King')) {
    throw new RuntimeException('update was not visible in the follow-up read');
}
printf("updated   rows=%d  %s\n", recordCount($rows), firstRecord($rows));

// ── DELETE ───────────────────────────────────────────────────────────────────
$client->delete(new DeleteRequest([
    'message_type'    => MESSAGE_TYPE,
    'filter'          => filterById($customerId),
    'idempotency_key' => "delete-{$customerId}",
]), $metadata);

$rows = $client->select(new SelectRequest([
    'message_type' => MESSAGE_TYPE,
    'filter'       => filterById($customerId),
    'limit'        => 10,
]), $metadata);
if (recordCount($rows) !== 0) {
    throw new RuntimeException('row still present after delete');
}
printf("deleted   rows=%d\n", recordCount($rows));

echo "\nCRUD OK\n";

// ── helpers ──────────────────────────────────────────────────────────────────

/** Map the typed model to the column-shaped JSON the broker stores. */
function customerToJson(Customer $c): string
{
    return json_encode([
        'customer_id'    => $c->getCustomerId(),
        'email'          => $c->getEmail(),
        'full_name'      => $c->getFullName(),
        'loyalty_points' => $c->getLoyaltyPoints(),
    ], JSON_THROW_ON_ERROR);
}

function filterById(string $customerId): Struct
{
    $filter = new Struct();
    $filter->mergeFromJsonString(json_encode(['customer_id' => $customerId], JSON_THROW_ON_ERROR));
    return $filter;
}

function recordCount(object $rows): int
{
    $json = count($rows->getRecordsJson());
    return $json > 0 ? $json : count($rows->getRows());
}

function recordsContain(object $rows, string $needle): bool
{
    foreach ($rows->getRecordsJson() as $record) {
        if (str_contains((string) $record, $needle)) {
            return true;
        }
    }
    return false;
}

function firstRecord(object $rows): string
{
    foreach ($rows->getRecordsJson() as $record) {
        return (string) $record;
    }
    return '(none)';
}

/** RFC 4122 v4 UUID — the broker stores customer_id as a UUID column. */
function uuidV4(): string
{
    $bytes = random_bytes(16);
    $bytes[6] = chr((ord($bytes[6]) & 0x0f) | 0x40);
    $bytes[8] = chr((ord($bytes[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bytes), 4));
}
