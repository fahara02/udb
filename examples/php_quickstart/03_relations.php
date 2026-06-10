<?php

/**
 * UDB PHP quickstart — Example 3: relationships & queries.
 *
 * Adds a second table ("shop"."orders", from proto/shop/v1/order.proto) that
 * references a customer by id. We create one customer, place several orders for
 * them, then run a real query: filter by customer, sort by amount, limit the
 * result — i.e. reading many related rows, not just one.
 *
 *   $env:UDB_TARGET = "127.0.0.1:50051"; php 03_relations.php
 */

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use Fahara02\UdbLaravel\UdbClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Google\Protobuf\Struct;
use PhpQuickstart\Shop\V1\Customer;
use PhpQuickstart\Shop\V1\Order;
use Udb\Entity\V1\SelectRequest;
use Udb\Entity\V1\Sort;
use Udb\Entity\V1\UpsertRequest;

$target = getenv('UDB_TARGET') ?: '127.0.0.1:50051';
$client = new UdbClient(['endpoint' => $target, 'tls' => ['enabled' => false], 'deadline_ms' => 30_000]);
$md = new UdbMetadata(
    tenantId: 'quickstart',
    userId: 'php-quickstart',
    purpose: 'quickstart.relations',
    correlationId: 'php-quickstart-rel-' . getmypid(),
    scopes: ['udb:read', 'udb:write'],
    serviceIdentity: 'examples.php.quickstart',
    projectId: 'default',
    clientCatalogVersion: '1.0.0',
);

// Deterministic ids so re-running this script updates the same rows in place
// (idempotent) rather than inserting duplicates. In a real app these are your
// natural keys.
$customerId = '0a0a0a0a-0000-4000-8000-000000000001';
$orderIds = [
    '0b0b0b0b-0000-4000-8000-000000000001',
    '0b0b0b0b-0000-4000-8000-000000000002',
    '0b0b0b0b-0000-4000-8000-000000000003',
    '0b0b0b0b-0000-4000-8000-000000000004',
];

// ── 1. the parent row: one customer ──────────────────────────────────────────
$client->upsert(new UpsertRequest([
    'message_type'    => 'shop.v1.Customer',
    'record_json'     => json_encode([
        'customer_id'    => $customerId,
        'email'          => 'katherine@example.com',
        'full_name'      => 'Katherine Johnson',
        'loyalty_points' => 0,
    ], JSON_THROW_ON_ERROR),
    'conflict_fields' => ['customer_id'],
    'idempotency_key' => "cust-{$customerId}",
]), $md);
printf("customer upserted: %s\n", $customerId);

// ── 2. several child rows: orders that reference the customer ─────────────────
$amounts = [1299, 4999, 2500, 999];
foreach ($amounts as $i => $cents) {
    $order = (new Order())
        ->setOrderId($orderIds[$i])
        ->setCustomerId($customerId)
        ->setAmountCents($cents)
        ->setStatus('paid');
    $client->upsert(new UpsertRequest([
        'message_type'    => 'shop.v1.Order',
        'record_json'     => json_encode([
            'order_id'     => $order->getOrderId(),
            'customer_id'  => $order->getCustomerId(),
            'amount_cents' => $order->getAmountCents(),
            'status'       => $order->getStatus(),
        ], JSON_THROW_ON_ERROR),
        'conflict_fields' => ['order_id'],
        'idempotency_key' => "order-{$customerId}-{$i}",
    ]), $md);
}
printf("orders upserted: %d\n", count($amounts));

// ── 3. query: this customer's top 3 orders by amount, descending ─────────────
$top = $client->select(new SelectRequest([
    'message_type' => 'shop.v1.Order',
    'filter'       => filterBy(['customer_id' => $customerId]),
    'sort'         => [new Sort(['field' => 'amount_cents', 'descending' => true])],
    'limit'        => 3,
]), $md);

printf("top %d orders for customer (amount desc):\n", $top->getTotalCount());
foreach ($top->getRows() as $row) {
    $f = $row->getFields();
    printf("  order=…%s  amount_cents=%s  status=%s\n",
        substr(scalar($f['order_id'] ?? null), -4),
        scalar($f['amount_cents'] ?? null),
        scalar($f['status'] ?? null),
    );
}

echo "\nRELATIONS OK\n";

// ── helpers ──────────────────────────────────────────────────────────────────

/** @param array<string,mixed> $kv */
function filterBy(array $kv): Struct
{
    $filter = new Struct();
    $filter->mergeFromJsonString(json_encode($kv, JSON_THROW_ON_ERROR));
    return $filter;
}

/** Read a scalar out of a row's protobuf Value field. */
function scalar(?\Google\Protobuf\Value $v): string
{
    if ($v === null) {
        return '';
    }
    return match ($v->getKind()) {
        'string_value' => $v->getStringValue(),
        'number_value' => formatNumber($v->getNumberValue()),
        'bool_value'   => $v->getBoolValue() ? 'true' : 'false',
        default        => $v->serializeToJsonString(),
    };
}

/** Whole numbers print without a decimal point; fractions print as-is. */
function formatNumber(float $n): string
{
    return $n == floor($n) && is_finite($n) ? sprintf('%.0f', $n) : (string) $n;
}
