<?php

declare(strict_types=1);

/*
 * =============================================================================
 * UDB persistent-worker example.
 *
 * THE PROBLEM (PHP-FPM):
 *   PHP-FPM is shared-nothing — the worker process is reset after every
 *   request, so a gRPC channel opened in one request cannot survive into the
 *   next (grpc#15426). Every request therefore pays a fresh TCP + (TLS) +
 *   HTTP/2 handshake before the first byte of useful work. On a warm channel
 *   that handshake is gone; on PHP-FPM it is the dominant per-request latency.
 *
 * THE FIX (this file):
 *   Run PHP as a long-lived worker (OpenSwoole / RoadRunner / FrankenPHP) that
 *   builds the UDB client ONCE at boot and reuses the same warm channel for
 *   every request it serves. The gRPC channel multiplexes concurrent requests
 *   over one HTTP/2 connection and is safe to share across the worker lifetime.
 *
 * This example is deliberately runtime-agnostic: `bootClient()` builds the
 * client once, `handleRequest()` runs one logical request on the warm channel,
 * and the bottom of the file shows how to drive both from an OpenSwoole HTTP
 * server (a RoadRunner / FrankenPHP loop is structurally identical — see
 * docs/php-perf.md). Run the plain-CLI fallback with:
 *
 *     php examples/persistent-worker/worker.php
 *
 * It boots the client once and issues N warm RPCs so you can see that only the
 * FIRST call pays channel setup.
 * =============================================================================
 */

require __DIR__ . '/../../vendor/autoload.php';

use Fahara02\UdbLaravel\UdbClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Udb\Entity\V1\RequestContext;
use Udb\Entity\V1\SelectRequest;

/**
 * Tenant-prefixed cache key helper for the example payload.
 */
function env_or(string $key, string $default): string
{
    $v = getenv($key);
    return ($v === false || $v === '') ? $default : $v;
}

/**
 * Build the UDB client ONCE per worker. Call this in the worker's
 * boot/start hook — NOT per request. The returned client holds the warm
 * gRPC channel for the worker's whole lifetime.
 *
 * The `channel_options` here are the keepalive args that stop an idle
 * channel from being torn down to IDLE (gRPC default 300s) and forced to
 * re-handshake on the next request — exactly the cost we are removing.
 */
function bootClient(): UdbClient
{
    // Endpoint can be either:
    //   host:port              e.g. 127.0.0.1:50051         (TCP / loopback)
    //   unix:///path/to.sock   e.g. unix:///var/run/udb.sock (local sidecar)
    // A UDS endpoint removes the TCP/loopback (and, in containers, NAT) hop
    // for a co-located broker. See docs/php-perf.md "Local UDB sidecar".
    $endpoint = env_or('UDB_ENDPOINT', '127.0.0.1:50051');

    return new UdbClient([
        'endpoint'    => $endpoint,
        'deadline_ms' => (int) env_or('UDB_DEADLINE_MS', '15000'),
        'channel_options' => [
            // Keepalive: ping the broker periodically so the warm channel
            // is NOT dropped to IDLE between requests and re-handshaked.
            'grpc.keepalive_time_ms'           => (int) env_or('UDB_GRPC_KEEPALIVE_MS', '30000'),
            'grpc.keepalive_timeout_ms'        => (int) env_or('UDB_GRPC_KEEPALIVE_TIMEOUT_MS', '10000'),
            // Allow keepalive pings even when there are no active RPCs, so an
            // idle worker keeps its connection warm for the next request.
            'grpc.keepalive_permit_without_calls' => 1,
            // Don't let gRPC park the connection into IDLE on its own.
            'grpc.max_connection_idle_ms'      => (int) env_or('UDB_GRPC_MAX_IDLE_MS', '600000'),
        ],
    ]);
}

/**
 * Handle ONE request on the already-warm channel. In a real worker you would
 * derive tenant/correlation from the inbound HTTP request; here we read env.
 *
 * Note: the client is request-stateless — per-request identity flows through
 * the `UdbMetadata` we pass to each RPC, so one shared client safely serves
 * many tenants/requests.
 */
function handleRequest(UdbClient $client, int $requestNo): string
{
    $tenant  = env_or('UDB_TENANT', 'default');
    $project = env_or('UDB_PROJECT', 'default');

    $meta = new UdbMetadata(
        tenantId: $tenant,
        userId: '',
        purpose: 'php.persistent-worker.example',
        correlationId: 'persistent-worker-' . $requestNo,
        scopes: [],
        serviceIdentity: 'php.persistent-worker.example',
        projectId: $project,
        clientCatalogVersion: '1.0.0',
        bearerToken: env_or('UDB_BEARER_TOKEN', ''),
    );

    $ctx = (new RequestContext())
        ->setTenantId($tenant)
        ->setProjectId($project)
        ->setPurpose('php.persistent-worker.example');

    $rs = $client->select(
        (new SelectRequest())
            ->setContext($ctx)
            ->setMessageType(env_or('UDB_MESSAGE_TYPE', 'udb.sdk.live.v1.SdkLiveRecord'))
            ->setLimit(1),
        $meta,
    );

    return sprintf('request #%d -> %d record(s)', $requestNo, count($rs->getRecordsJson()));
}

// -----------------------------------------------------------------------------
// Driver A: OpenSwoole HTTP server (persistent worker).
//
// Each worker process runs `WorkerStart` ONCE and keeps `$client` for its whole
// life; every HTTP request reuses that warm channel. Enable by installing
// ext-openswoole and running this file directly with the extension loaded.
// -----------------------------------------------------------------------------
if (extension_loaded('openswoole') && env_or('UDB_RUN_SWOOLE', '0') === '1') {
    $host = env_or('UDB_WORKER_HOST', '0.0.0.0');
    $port = (int) env_or('UDB_WORKER_PORT', '9501');

    $server = new OpenSwoole\HTTP\Server($host, $port);
    $server->set([
        'worker_num' => (int) env_or('UDB_WORKER_NUM', '4'),
        'enable_coroutine' => false, // the gRPC PECL stub is blocking, not coroutine-aware
    ]);

    // Build the warm UDB client ONCE per worker process.
    $clients = [];
    $server->on('WorkerStart', function (OpenSwoole\HTTP\Server $srv, int $workerId) use (&$clients): void {
        $clients[$workerId] = bootClient();
        // Optional: move first-call channel setup out of the first user request.
        $clients[$workerId]->warmup();
        fwrite(STDERR, "[worker $workerId] UDB channel warmed\n");
    });

    $counter = 0;
    $server->on('request', function ($request, $response) use (&$clients, &$counter): void {
        $workerId = $request->server['worker_id'] ?? 0;
        $client = $clients[$workerId];           // <-- reused warm channel
        $out = handleRequest($client, ++$counter);
        $response->header('Content-Type', 'text/plain');
        $response->end($out . "\n");
    });

    fwrite(STDERR, "OpenSwoole UDB worker listening on $host:$port (warm channel per worker)\n");
    $server->start();
    return;
}

// -----------------------------------------------------------------------------
// Driver B: plain-CLI fallback so the example is runnable without any runtime.
//
// Boots the client ONCE, then issues N warm RPCs. Only the first call pays
// channel setup; the rest reuse the warm channel — exactly what a persistent
// worker buys you over PHP-FPM.
// -----------------------------------------------------------------------------
$client = bootClient();                          // built ONCE
$client->warmup();                               // pay channel setup up front
$n = (int) env_or('UDB_EXAMPLE_REQUESTS', '5');
for ($i = 1; $i <= $n; $i++) {
    $t = hrtime(true);
    $line = handleRequest($client, $i);          // reuses the warm channel
    $ms = (hrtime(true) - $t) / 1e6;
    printf("%s  (%.2f ms on warm channel)\n", $line, $ms);
}
