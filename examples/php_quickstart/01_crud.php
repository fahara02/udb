<?php

/**
 * UDB PHP quickstart — example 1: basic CRUD.
 *
 * One table ("shop"."customers", from proto/shop/v1/customer.proto), four
 * operations: create, read, update, delete. No auth, no vectors, no objects —
 * just the smallest thing that talks to the broker and back.
 *
 *   docker compose up -d                       # Postgres + Redis
 *   ./scripts/serve-broker.ps1                 # broker (separate terminal)
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

// The fully-qualified proto name == the broker's catalog key for the table.
const MESSAGE_TYPE = 'shop.v1.Customer';

$target = getenv('UDB_TARGET') ?: '127.0.0.1:50051';

// One long-lived client per process. The gRPC channel multiplexes every call.
$client = new UdbClient([
    'endpoint'   => $target,
    'tls'        => ['enabled' => false],
    'deadline_ms' => 30_000,
]);

// Per-request context the broker requires on every call (tenant, who, why,
// scopes). For the quickstart these are static; in a web app they come from the
// authenticated request.
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

// A natural key so each run is idempotent (email is UNIQUE in the schema).
$email = 'ada@example.com';

// ── CREATE ───────────────────────────────────────────────────────────────────
// Build the row with the generated, type-safe Customer model (from buf generate).
// customer_id (UUID) and created_at are filled by the database defaults, so we
// only set what we own.
$customer = (new Customer())
    ->setEmail($email)
    ->setFullName('Ada Lovelace')
    ->setLoyaltyPoints(100);

$created = $client->upsert(new UpsertRequest([
    'message_type'    => MESSAGE_TYPE,
    'record_json'     => customerToJson($customer),
    'conflict_fields' => ['email'],   // ON CONFLICT (email) DO UPDATE …
    'return_record'   => true,
    'idempotency_key' => "create-{$email}",
]), $metadata);
printf("created   affected_rows=%d\n", $created->getAffectedRows());

// ── READ ─────────────────────────────────────────────────────────────────────
$rows = $client->select(new SelectRequest([
    'message_type' => MESSAGE_TYPE,
    'filter'       => filterByEmail($email),
    'limit'        => 10,
]), $metadata);
printf("read      rows=%d  %s\n", recordCount($rows), firstRecord($rows));

// ── UPDATE ───────────────────────────────────────────────────────────────────
// Same upsert, new values — the conflict on email turns it into an UPDATE.
$customer->setFullName('Augusta Ada King')->setLoyaltyPoints(250);
$client->upsert(new UpsertRequest([
    'message_type'    => MESSAGE_TYPE,
    'record_json'     => customerToJson($customer),
    'conflict_fields' => ['email'],
    'return_record'   => true,
    'idempotency_key' => "update-{$email}",
]), $metadata);

$rows = $client->select(new SelectRequest([
    'message_type' => MESSAGE_TYPE,
    'filter'       => filterByEmail($email),
    'limit'        => 10,
]), $metadata);
if (! recordsContain($rows, 'Augusta Ada King')) {
    throw new RuntimeException('update was not visible in the follow-up read');
}
printf("updated   rows=%d  %s\n", recordCount($rows), firstRecord($rows));

// ── DELETE ───────────────────────────────────────────────────────────────────
$client->delete(new DeleteRequest([
    'message_type'    => MESSAGE_TYPE,
    'filter'          => filterByEmail($email),
    'idempotency_key' => "delete-{$email}",
]), $metadata);

$rows = $client->select(new SelectRequest([
    'message_type' => MESSAGE_TYPE,
    'filter'       => filterByEmail($email),
    'limit'        => 10,
]), $metadata);
if (recordCount($rows) !== 0) {
    throw new RuntimeException('row still present after delete');
}
printf("deleted   rows=%d\n", recordCount($rows));

echo "\nCRUD OK\n";

// ── helpers ──────────────────────────────────────────────────────────────────

/**
 * Map the typed model to the column-shaped JSON the broker stores. We send only
 * the fields we own; server-defaulted columns (customer_id, created_at) are
 * omitted so the database fills them.
 */
function customerToJson(Customer $c): string
{
    return json_encode([
        'email'          => $c->getEmail(),
        'full_name'      => $c->getFullName(),
        'loyalty_points' => $c->getLoyaltyPoints(),
    ], JSON_THROW_ON_ERROR);
}

function filterByEmail(string $email): Struct
{
    $filter = new Struct();
    $filter->mergeFromJsonString(json_encode(['email' => $email], JSON_THROW_ON_ERROR));
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
