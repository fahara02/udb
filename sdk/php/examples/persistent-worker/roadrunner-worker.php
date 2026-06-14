<?php

declare(strict_types=1);

/*
 * =============================================================================
 * UDB warm-channel worker for RoadRunner.
 *
 * RoadRunner keeps a pool of long-lived PHP worker processes. Each worker runs
 * this script ONCE: it builds the UDB client (warm gRPC channel) before the
 * accept loop, then pulls requests off RoadRunner in a `while` loop, reusing
 * the SAME channel for every request. Nothing tears the channel down between
 * requests, so only the first request pays channel setup.
 *
 * Requires:  composer require spiral/roadrunner-http nyholm/psr7
 * .rr.yaml:
 *   server:
 *     command: "php examples/persistent-worker/roadrunner-worker.php"
 *   http:
 *     address: 0.0.0.0:8080
 *     pool:
 *       num_workers: 4
 * =============================================================================
 */

require __DIR__ . '/../../vendor/autoload.php';

// This worker is self-contained (it does not `require` worker.php, whose CLI
// driver would run). The warm-client construction is inlined below.

use Fahara02\UdbLaravel\UdbClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Nyholm\Psr7\Factory\Psr17Factory;
use Spiral\RoadRunner\Http\PSR7Worker;
use Spiral\RoadRunner\Worker;
use Udb\Entity\V1\RequestContext;
use Udb\Entity\V1\SelectRequest;

function rr_env(string $key, string $default): string
{
    $v = getenv($key);
    return ($v === false || $v === '') ? $default : $v;
}

// ---- build the warm UDB client ONCE, before the accept loop ----
$client = new UdbClient([
    'endpoint'    => rr_env('UDB_ENDPOINT', '127.0.0.1:50051'), // or unix:///var/run/udb.sock
    'deadline_ms' => (int) rr_env('UDB_DEADLINE_MS', '15000'),
    'channel_options' => [
        'grpc.keepalive_time_ms'              => (int) rr_env('UDB_GRPC_KEEPALIVE_MS', '30000'),
        'grpc.keepalive_timeout_ms'           => (int) rr_env('UDB_GRPC_KEEPALIVE_TIMEOUT_MS', '10000'),
        'grpc.keepalive_permit_without_calls' => 1,
        'grpc.max_connection_idle_ms'         => (int) rr_env('UDB_GRPC_MAX_IDLE_MS', '600000'),
    ],
]);
$client->warmup(); // move first-call channel setup out of the first user request

// ---- RoadRunner PSR-7 accept loop ----
$factory = new Psr17Factory();
$psr7 = new PSR7Worker(Worker::create(), $factory, $factory, $factory);

$counter = 0;
while ($req = $psr7->waitRequest()) {
    try {
        $tenant  = $req->getHeaderLine('X-Tenant-Id') ?: rr_env('UDB_TENANT', 'default');
        $project = $req->getHeaderLine('X-Project-Id') ?: rr_env('UDB_PROJECT', 'default');

        $meta = new UdbMetadata(
            tenantId: $tenant,
            userId: '',
            purpose: 'php.roadrunner.example',
            correlationId: $req->getHeaderLine('X-Correlation-Id') ?: ('rr-' . (++$counter)),
            scopes: [],
            serviceIdentity: 'php.roadrunner.example',
            projectId: $project,
            clientCatalogVersion: '1.0.0',
            bearerToken: rr_env('UDB_BEARER_TOKEN', ''),
        );

        $ctx = (new RequestContext())->setTenantId($tenant)->setProjectId($project)
            ->setPurpose('php.roadrunner.example');

        // <-- reuses the warm channel built before the loop
        $rs = $client->select(
            (new SelectRequest())->setContext($ctx)
                ->setMessageType(rr_env('UDB_MESSAGE_TYPE', 'udb.sdk.live.v1.SdkLiveRecord'))
                ->setLimit(1),
            $meta,
        );

        $body = $factory->createStream(
            json_encode(['records' => count($rs->getRecordsJson())]) . "\n"
        );
        $psr7->respond($factory->createResponse(200)->withBody($body));
    } catch (\Throwable $e) {
        $psr7->respond(
            $factory->createResponse(500)->withBody($factory->createStream($e->getMessage()))
        );
    }
}
