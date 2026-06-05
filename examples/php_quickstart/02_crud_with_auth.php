<?php

/**
 * UDB PHP quickstart — example 2: the same CRUD, now gated by native authn + authz.
 *
 * What changes vs. example 1:
 *   1. AUTHN — instead of asserting an identity in headers, we exchange a UDB
 *      API key for a verified Principal via the native AuthnService.
 *   2. AUTHZ — before each operation we ask the native AuthzService whether this
 *      principal may perform it (`$auth->can(...)`). The broker's policy engine
 *      decides; the app just honours the decision.
 *
 * The CRUD calls themselves are identical to 01_crud.php — only the metadata is
 * now derived from the authenticated principal.
 *
 * Seed an API key (and, if you turned default-allow off, a policy) first — see
 * the "Example 2" section of the README. Then:
 *
 *   $env:UDB_TARGET  = "127.0.0.1:50051"
 *   $env:UDB_API_KEY = "<the key printed by `udb auth api-key create`>"
 *   php 02_crud_with_auth.php
 */

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use Fahara02\UdbLaravel\UdbAuthClient;
use Fahara02\UdbLaravel\UdbClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Google\Protobuf\Struct;
use PhpQuickstart\Shop\V1\Customer;
use Udb\Core\Authz\Services\V1\ResourceRef;
use Udb\Entity\V1\DeleteRequest;
use Udb\Entity\V1\SelectRequest;
use Udb\Entity\V1\UpsertRequest;

const MESSAGE_TYPE = 'shop.v1.Customer';

$target = getenv('UDB_TARGET') ?: '127.0.0.1:50051';
$apiKey = getenv('UDB_API_KEY') ?: '';
if ($apiKey === '') {
    fwrite(STDERR, "Set UDB_API_KEY (see the README \"Example 2\" section).\n");
    exit(2);
}

$config = ['endpoint' => $target, 'tls' => ['enabled' => false], 'deadline_ms' => 30_000];
$client = new UdbClient($config);
$auth   = new UdbAuthClient($config);

// A bootstrap context: who we *claim* to be while we exchange the API key. The
// tenant/project here scope the lookup; the verified identity comes back in the
// AuthnResponse.
$bootstrap = new UdbMetadata(
    tenantId: 'quickstart',
    userId: 'pending',
    purpose: 'quickstart.auth',
    correlationId: 'php-quickstart-auth-' . getmypid(),
    scopes: [],
    serviceIdentity: 'examples.php.quickstart',
    projectId: 'default',
    clientCatalogVersion: '1.0.0',
);

// ── 1. AUTHN: exchange the API key for a verified principal ───────────────────
$authn     = $auth->authenticateApiKey($apiKey, $bootstrap);
$principal = $authn->getPrincipal();
if ($principal === null) {
    throw new RuntimeException('authentication returned no principal');
}
printf(
    "authenticated  user=%s  method=%s  scopes=[%s]\n",
    $principal->getUserId() ?: $principal->getSubject(),
    $principal->getAuthMethod() ?: 'api_key',
    implode(',', iterator_to_array($principal->getScopes()))
);

// Build the request context from the *verified* principal — the scopes the
// broker granted, not scopes we invented.
$metadata = new UdbMetadata(
    tenantId: $principal->getTenantId() ?: 'quickstart',
    userId: $principal->getUserId() ?: $principal->getSubject(),
    purpose: 'quickstart.crud',
    correlationId: $bootstrap->correlationId,
    scopes: iterator_to_array($principal->getScopes()),
    serviceIdentity: 'examples.php.quickstart',
    projectId: $principal->getProjectId() ?: 'default',
    clientCatalogVersion: '1.0.0',
);
$auth->bindContext($metadata);

// The thing we're authorizing access to: the customers table.
$resource = (new ResourceRef())
    ->setResourceType('message')
    ->setMessageType(MESSAGE_TYPE)
    ->setSchema('shop')
    ->setTable('customers')
    ->setTenantId($metadata->tenantId)
    ->setProjectId($metadata->projectId);

$email = 'ada@example.com';

// ── 2. AUTHZ + CREATE ─────────────────────────────────────────────────────────
authorize($auth, $resource, 'write');
$customer = (new Customer())
    ->setEmail($email)
    ->setFullName('Ada Lovelace')
    ->setLoyaltyPoints(100);
$created = $client->upsert(new UpsertRequest([
    'message_type'    => MESSAGE_TYPE,
    'record_json'     => customerToJson($customer),
    'conflict_fields' => ['email'],
    'return_record'   => true,
    'idempotency_key' => "create-{$email}",
]), $metadata);
printf("created    affected_rows=%d\n", $created->getAffectedRows());

// ── AUTHZ + READ ──────────────────────────────────────────────────────────────
authorize($auth, $resource, 'read');
$rows = $client->select(new SelectRequest([
    'message_type' => MESSAGE_TYPE,
    'filter'       => filterByEmail($email),
    'limit'        => 10,
]), $metadata);
printf("read       rows=%d  %s\n", recordCount($rows), firstRecord($rows));

// ── AUTHZ + DELETE ──────────────────────────────────────────────────────────────
authorize($auth, $resource, 'write');
$client->delete(new DeleteRequest([
    'message_type'    => MESSAGE_TYPE,
    'filter'          => filterByEmail($email),
    'idempotency_key' => "delete-{$email}",
]), $metadata);
printf("deleted    email=%s\n", $email);

echo "\nAUTH + CRUD OK\n";

// ── helpers ──────────────────────────────────────────────────────────────────

/**
 * Ask the native AuthzService for a decision and stop on denial. This is the
 * explicit-check pattern; the broker *also* enforces on the CRUD call itself, so
 * this is defence in depth + a clear, early, human-readable failure.
 */
function authorize(UdbAuthClient $auth, ResourceRef $resource, string $action): void
{
    [$allowed, $decision] = $auth->can($resource, $action);
    if (! $allowed) {
        $reason = $decision->getDenyReason() ?: 'no matching allow policy';
        fwrite(STDERR, "DENIED  action={$action}  reason={$reason}\n");
        exit(1);
    }
    printf("authorized action=%-5s effect=%s\n", $action, $decision->getEffect() ?: 'ALLOW');
}

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

function firstRecord(object $rows): string
{
    foreach ($rows->getRecordsJson() as $record) {
        return (string) $record;
    }
    return '(none)';
}
