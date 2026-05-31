<?php

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use Fahara02\UdbLaravel\UdbClient;
use Fahara02\UdbLaravel\UdbMetadata;
use PhpArbitaryProject\Acme\Billing\V1\Product;
use Udb\Entity\V1\Chunk;
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

$product = new Product([
    'product_id' => 'prod-sdk-php-001',
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
    'idempotency_key' => 'php-product-sdk-001',
]);

$client->upsert($upsert, $metadata);

$rows = $client->select(new SelectRequest([
    'message_type' => $messageType,
    'limit' => 10,
]), $metadata);

printf("selected rows=%d\n", count($rows->getRecordsJson()) + count($rows->getRows()));

$vector = makeVector(768);
waitUnary($client->stub()->VectorUpsert(new VectorUpsertRequest([
    'collection' => $collection,
    'points' => [
        new VectorPointMutation([
            'id' => '22222222-2222-4222-8222-222222222222',
            'vector' => $vector,
        ]),
    ],
    'idempotency_key' => 'php-vector-sdk-001',
]), $metadata->toGrpcMetadata()), 'VectorUpsert');

$vectorRows = waitUnary($client->stub()->VectorSearch(new VectorSearchRequest([
    'collection' => $collection,
    'vector' => $vector,
    'limit' => 3,
    'with_payload' => true,
]), $metadata->toGrpcMetadata()), 'VectorSearch');

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
$objectCall->writesDone();
waitUnary($objectCall, 'PutObject');

$url = waitUnary($client->stub()->GeneratePresignedUrl(new UrlRequest([
    'bucket' => $bucket,
    'object_key' => 'invoices/sdk/php/smoke.txt',
    'method' => 'GET',
    'ttl_seconds' => 300,
    'content_type' => 'text/plain',
]), $metadata->toGrpcMetadata()), 'GeneratePresignedUrl');

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

function waitUnary($call, string $rpcName): object
{
    [$response, $status] = $call->wait();
    if (($status->code ?? $status['code'] ?? -1) !== 0) {
        $details = $status->details ?? $status['details'] ?? 'unknown gRPC error';
        throw new RuntimeException("{$rpcName} failed: {$details}");
    }
    if (! is_object($response)) {
        throw new RuntimeException("{$rpcName} returned no response");
    }
    return $response;
}
