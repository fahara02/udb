<?php

/**
 * UDB PHP quickstart — Example 2: authorization (operation scopes).
 *
 * Builds directly on Example 1. The CRUD calls are identical — what changes is
 * the *scopes* in the request metadata. The broker enforces, on every call:
 *
 *   • a write (Upsert / Delete) requires the `udb:write` scope
 *   • a read  (Select)          requires the `udb:read`  scope
 *
 * Nothing in this script checks permissions — the broker does, and returns
 * gRPC INVALID_ARGUMENT ("scope udb:write is required") when the caller's
 * metadata lacks the scope. We show one identity that is allowed and one that
 * is not.
 *
 *   $env:UDB_TARGET = "127.0.0.1:50051"; php 02_authz.php
 */

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Fahara02\UdbLaravel\UdbClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Google\Protobuf\Struct;
use PhpQuickstart\Shop\V1\Customer;
use Udb\Entity\V1\SelectRequest;
use Udb\Entity\V1\UpsertRequest;

const MESSAGE_TYPE = 'shop.v1.Customer';

$target = getenv('UDB_TARGET') ?: '127.0.0.1:50051';
$client = new UdbClient(['endpoint' => $target, 'tls' => ['enabled' => false], 'deadline_ms' => 30_000]);

// Same identity, two scope sets. In a real app these come from the
// authenticated principal; here we vary them to see the broker's enforcement.
$readOnly  = identity(['udb:read']);
$readWrite = identity(['udb:read', 'udb:write']);

$customerId = uuidV4();
$record = customerToJson((new Customer())
    ->setCustomerId($customerId)
    ->setEmail('grace@example.com')
    ->setFullName('Grace Hopper')
    ->setLoyaltyPoints(10));

$upsert = fn(UdbMetadata $who) => $client->upsert(new UpsertRequest([
    'message_type'    => MESSAGE_TYPE,
    'record_json'     => $record,
    'conflict_fields' => ['customer_id'],
    'idempotency_key' => "authz-{$customerId}",
]), $who);

$select = fn(UdbMetadata $who) => $client->select(new SelectRequest([
    'message_type' => MESSAGE_TYPE,
    'filter'       => filterById($customerId),
    'limit'        => 10,
]), $who);

// ── 1. read-only identity tries to WRITE → broker denies ─────────────────────
echo "read-only identity, Upsert:\n";
try {
    $upsert($readOnly);
    echo "  UNEXPECTED: write succeeded without udb:write\n";
} catch (UdbRpcException $e) {
    printf("  denied as expected — gRPC %d: %s\n", $e->status, $e->details);
}

// ── 2. read-write identity WRITES → allowed ──────────────────────────────────
echo "read-write identity, Upsert:\n";
$res = $upsert($readWrite);
printf("  allowed — affected_rows=%d\n", $res->getAffectedRows());

// ── 3. read-only identity READS → allowed ────────────────────────────────────
echo "read-only identity, Select:\n";
$rows = $select($readOnly);
printf("  allowed — rows=%d\n", recordCount($rows));

// ── 4. an identity with NO read scope tries to READ → broker denies ──────────
echo "write-only identity, Select:\n";
try {
    $select(identity(['udb:write']));
    echo "  UNEXPECTED: read succeeded without udb:read\n";
} catch (UdbRpcException $e) {
    printf("  denied as expected — gRPC %d: %s\n", $e->status, $e->details);
}

// cleanup (needs write)
$client->delete(new \Udb\Entity\V1\DeleteRequest([
    'message_type'    => MESSAGE_TYPE,
    'filter'          => filterById($customerId),
    'idempotency_key' => "authz-del-{$customerId}",
]), $readWrite);

echo "\nAUTHZ OK\n";

// ── helpers ──────────────────────────────────────────────────────────────────

/** @param list<string> $scopes */
function identity(array $scopes): UdbMetadata
{
    return new UdbMetadata(
        tenantId: 'quickstart',
        userId: 'php-quickstart',
        purpose: 'quickstart.authz',
        correlationId: 'php-quickstart-authz-' . getmypid(),
        scopes: $scopes,
        serviceIdentity: 'examples.php.quickstart',
        projectId: 'default',
        clientCatalogVersion: '1.0.0',
    );
}

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

function uuidV4(): string
{
    $bytes = random_bytes(16);
    $bytes[6] = chr((ord($bytes[6]) & 0x0f) | 0x40);
    $bytes[8] = chr((ord($bytes[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bytes), 4));
}
