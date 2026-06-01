<?php

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use Fahara02\UdbLaravel\UdbClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Google\Protobuf\Struct;
use PhpArbitaryProject\Acme\Billing\V1\Product;
use Udb\Entity\V1\Chunk;
use Udb\Entity\V1\DeleteRequest;
use Udb\Entity\V1\ObjectRequest;
use Udb\Entity\V1\SelectRequest;
use Udb\Entity\V1\UpsertRequest;
use Udb\Entity\V1\UrlRequest;
use Udb\Entity\V1\VectorPointMutation;
use Udb\Entity\V1\VectorSearchRequest;
use Udb\Entity\V1\VectorUpsertRequest;

$target = getenv('UDB_TARGET') ?: '127.0.0.1:50051';
$projectId = 'default';
$tenantId = 'acme-org-1';
$messageType = 'acme.billing.v1.Product';
$collection = 'acme_products';
$bucket = 'acme-billing-documents';
$showTimings = filter_var(getenv('UDB_SHOW_TIMINGS') ?: 'false', FILTER_VALIDATE_BOOLEAN);
$warmup = filter_var(getenv('UDB_WARMUP') ?: 'true', FILTER_VALIDATE_BOOLEAN);

$client = new UdbClient([
    'endpoint' => $target,
    'tls' => ['enabled' => false],
    'deadline_ms' => 30_000,
]);

$metadata = new UdbMetadata(
    tenantId: $tenantId,
    userId: 'sdk-php-example',
    purpose: 'billing.example',
    correlationId: 'php-acme-billing-example',
    scopes: [
        'udb:read',
        'udb:write',
        'udb:admin',
        'udb:vector:read',
        'udb:vector:write',
        'udb:object:presign',
        'udb:stream',
    ],
    serviceIdentity: 'examples.php',
    projectId: $projectId,
    clientCatalogVersion: '1.0.0',
);

if ($warmup) {
    timed('client warmup', fn() => $client->warmup($metadata));
}

$runId = (string) time() . random_int(1000, 9999);
$productId = "prod-sdk-php-crud-{$runId}";

timed('relational cleanup delete', function () use ($client, $metadata, $messageType, $productId, $runId): void {
    $client->delete(new DeleteRequest([
    'message_type' => $messageType,
    'filter' => productFilter($productId),
    'idempotency_key' => "php-product-cleanup-{$runId}",
    ]), $metadata);
});

$product = new Product([
    'product_id' => $productId,
    'name' => 'SDK smoke test product',
    'description' => 'Inserted by the PHP UDB SDK example',
    'price_cents' => 22900,
    'sku' => 'SDK-PHP-001',
]);

$upsert = new UpsertRequest([
    'message_type' => $messageType,
    'record_json' => productRecordJson($product),
    'conflict_fields' => ['product_id'],
    'return_record' => true,
    'idempotency_key' => "php-product-create-{$runId}",
]);

timed('relational create upsert', fn() => $client->upsert($upsert, $metadata));

$rows = timed('relational create select', fn() => $client->select(new SelectRequest([
    'message_type' => $messageType,
    'filter' => productFilter($productId),
    'limit' => 10,
]), $metadata));

$rowCount = recordCount($rows);
if ($rowCount !== 1) {
    throw new RuntimeException("relational create/read expected 1 row, got {$rowCount}");
}
printf("relational create/read rows=%d\n", $rowCount);

$product->setName('SDK smoke test product updated');
timed('relational update upsert', fn() => $client->upsert(new UpsertRequest([
    'message_type' => $messageType,
    'record_json' => productRecordJson($product),
    'conflict_fields' => ['product_id'],
    'return_record' => true,
    'idempotency_key' => "php-product-update-{$runId}",
]), $metadata));

$rows = timed('relational update select', fn() => $client->select(new SelectRequest([
    'message_type' => $messageType,
    'filter' => productFilter($productId),
    'limit' => 10,
]), $metadata));
if (! recordsContain($rows, 'SDK smoke test product updated')) {
    throw new RuntimeException('relational update was not visible in select response');
}
printf("relational update verified rows=%d\n", recordCount($rows));

timed('relational delete', fn() => $client->delete(new DeleteRequest([
    'message_type' => $messageType,
    'filter' => productFilter($productId),
    'idempotency_key' => "php-product-delete-{$runId}",
]), $metadata));

$rows = timed('relational delete select', fn() => $client->select(new SelectRequest([
    'message_type' => $messageType,
    'filter' => productFilter($productId),
    'limit' => 10,
]), $metadata));
$rowCount = recordCount($rows);
if ($rowCount !== 0) {
    throw new RuntimeException("relational delete expected 0 rows, got {$rowCount}");
}
printf("relational delete verified rows=%d\n", $rowCount);

$vector = makeVector(768);
timed('vector upsert', fn() => waitUnary($client->stub()->VectorUpsert(new VectorUpsertRequest([
    'collection' => $collection,
    'points' => [
        new VectorPointMutation([
            'id' => '22222222-2222-4222-8222-222222222222',
            'vector' => $vector,
        ]),
    ],
    'idempotency_key' => 'php-vector-sdk-001',
]), $metadata->toGrpcMetadata()), 'VectorUpsert'));

$vectorRows = timed('vector search', fn() => waitUnary($client->stub()->VectorSearch(new VectorSearchRequest([
    'collection' => $collection,
    'vector' => $vector,
    'limit' => 3,
    'with_payload' => true,
]), $metadata->toGrpcMetadata()), 'VectorSearch'));

printf("vector search points=%d\n", count($vectorRows->getPoints()));

$objectCall = $client->stub()->PutObject($metadata->toGrpcMetadata());
$objectCall->write(new Chunk([
    'bucket' => $bucket,
    'object_key' => 'invoices/sdk/php/smoke.txt',
    'data' => "hello from the PHP UDB SDK example\n",
    'final_chunk' => true,
    'content_type' => 'text/plain',
    'idempotency_key' => 'php-object-sdk-001',
]));
timed('object put', fn() => waitUnary($objectCall, 'PutObject'));

$objectBytes = timed('object get', function () use ($client, $metadata, $bucket): string {
    $objectStream = $client->stub()->GetObject(new ObjectRequest([
        'bucket' => $bucket,
        'object_key' => 'invoices/sdk/php/smoke.txt',
    ]), $metadata->toGrpcMetadata());
    $bytes = '';
    foreach ($objectStream->responses() as $chunk) {
        $bytes .= $chunk->getData();
    }
    assertOkStatus($objectStream->getStatus(), 'GetObject');
    return $bytes;
});
if (! str_contains($objectBytes, 'hello from the PHP UDB SDK example')) {
    throw new RuntimeException('object readback did not match uploaded content');
}
printf("object readback bytes=%d\n", strlen($objectBytes));

$url = timed('object presign', fn() => waitUnary($client->stub()->GeneratePresignedUrl(new UrlRequest([
    'bucket' => $bucket,
    'object_key' => 'invoices/sdk/php/smoke.txt',
    'method' => 'GET',
    'ttl_seconds' => 300,
    'content_type' => 'text/plain',
]), $metadata->toGrpcMetadata()), 'GeneratePresignedUrl'));

printf("object presigned url expires_at=%d\n", $url->getExpiresAtUnix());

/**
 * @return list<float>
 */
function makeVector(int $dimension): array
{
    $vector = [];
    for ($i = 0; $i < $dimension; $i++) {
        $vector[] = (($i % 17) + 1) / 17.0;
    }
    return $vector;
}

function productRecordJson(Product $product): string
{
    return json_encode([
        'product_id' => $product->getProductId(),
        'name' => $product->getName(),
        'description' => $product->getDescription(),
        'price_cents' => $product->getPriceCents(),
        'sku' => $product->getSku(),
    ], JSON_THROW_ON_ERROR);
}

function productFilter(string $productId): Struct
{
    $filter = new Struct();
    $filter->mergeFromJsonString(json_encode([
        'product_id' => $productId,
    ], JSON_THROW_ON_ERROR));
    return $filter;
}

function recordCount(object $rows): int
{
    $jsonRows = count($rows->getRecordsJson());
    if ($jsonRows > 0) {
        return $jsonRows;
    }
    return count($rows->getRows());
}

function recordsContain(object $rows, string $needle): bool
{
    foreach ($rows->getRecordsJson() as $record) {
        if (str_contains((string) $record, $needle)) {
            return true;
        }
    }
    foreach ($rows->getRows() as $row) {
        if (str_contains($row->serializeToJsonString(), $needle)) {
            return true;
        }
    }
    return false;
}

function waitUnary($call, string $rpcName): object
{
    [$response, $status] = $call->wait();
    assertOkStatus($status, $rpcName);
    if (! is_object($response)) {
        throw new RuntimeException("{$rpcName} returned no response");
    }
    return $response;
}

function assertOkStatus(object|array $status, string $rpcName): void
{
    $code = is_object($status) ? ($status->code ?? -1) : ($status['code'] ?? -1);
    if ($code === 0) {
        return;
    }
    $details = is_object($status) ? ($status->details ?? 'unknown gRPC error') : ($status['details'] ?? 'unknown gRPC error');
    throw new RuntimeException("{$rpcName} failed: {$details}");
}

/**
 * @template T
 * @param callable(): T $operation
 * @return T
 */
function timed(string $label, callable $operation): mixed
{
    global $showTimings;

    if (! $showTimings) {
        return $operation();
    }

    $start = hrtime(true);
    try {
        return $operation();
    } finally {
        $elapsedMs = (hrtime(true) - $start) / 1_000_000;
        printf("timing %-28s %.2f ms\n", $label, $elapsedMs);
    }
}
