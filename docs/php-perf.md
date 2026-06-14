# PHP performance: warm gRPC channels, persistent workers, and a local UDB sidecar

This guide is the **#1 PHP-user latency win** for UDB. It explains why the
default PHP-FPM deployment is slow against a gRPC broker, and gives correct,
copy-pasteable recipes to fix it with a persistent-worker runtime
(OpenSwoole / RoadRunner / FrankenPHP) and/or a co-located UDB sidecar reached
over a Unix domain socket.

Runnable example: [`sdk/php/examples/persistent-worker/`](../sdk/php/examples/persistent-worker/).

---

## Why PHP-FPM can't pool a gRPC channel (the per-request handshake tax)

PHP-FPM is **shared-nothing**: the worker process state is reset at the end of
every request. A gRPC channel opened during one request does **not** survive
into the next one — there is no process-global place to keep it alive
(this is [grpc#15426](https://github.com/grpc/grpc/issues/15426)).

So on PHP-FPM, every request does this before any useful work:

1. open a TCP connection to the broker,
2. (if TLS) complete the TLS handshake,
3. complete the HTTP/2 connection preface / settings exchange,
4. *then* send the first RPC.

That setup — **a fresh TCP + TLS + HTTP/2 handshake on every single request** —
is the dominant per-request cost for a PHP UDB client. A long-lived client (Go,
Python, Node, a PHP **worker**) pays it once and amortizes it over thousands of
requests; PHP-FPM pays it every time.

> The UDB PHP client already holds **one** channel for its lifetime and
> multiplexes requests over it (`UdbClient` is a request-stateless singleton).
> The problem is purely that **PHP-FPM throws that lifetime away each request.**
> The fix is to give the client a longer-lived host process.

### Concrete cost

The handshake adds a full extra network round-trip (TCP) plus, with TLS, the
TLS handshake round-trip(s) — on a loopback/co-located broker this is on the
order of a **few hundred microseconds to low single-digit milliseconds per
request**, and it is **pure overhead** repeated on every request. Across a
busy endpoint that is the difference between "UDB is a thin hop" and "UDB adds
a visible tax". The fixes below remove it.

---

## The fix: hold a warm channel in a persistent worker

Run PHP as a **long-lived worker** that builds the UDB client **once at boot**
and reuses the same warm channel for every request it serves. The gRPC channel
is safe to share across the worker's lifetime — it multiplexes concurrent
requests over one HTTP/2 connection.

The common shape, regardless of runtime:

```php
// ONCE, at worker boot:
$client = new UdbClient([
    'endpoint' => getenv('UDB_ENDPOINT') ?: '127.0.0.1:50051',
    'channel_options' => [
        'grpc.keepalive_time_ms'              => 30000,
        'grpc.keepalive_timeout_ms'           => 10000,
        'grpc.keepalive_permit_without_calls' => 1,
        'grpc.max_connection_idle_ms'         => 600000,
    ],
]);
$client->warmup(); // optional: move first-call setup out of the first user request

// PER REQUEST (channel is already warm):
$rs = $client->select($selectRequest, $perRequestMetadata);
```

The key rule: **construct the client in the worker boot hook, not per request.**
Below, each runtime's correct lifecycle.

### OpenSwoole

OpenSwoole runs a pool of worker processes. The `WorkerStart` event fires
**once per worker process** — build the client there and keep it in a
process-scoped variable; every `request` event on that worker reuses it.

```php
$server = new OpenSwoole\HTTP\Server('0.0.0.0', 9501);
$server->set([
    'worker_num'       => 4,
    'enable_coroutine' => false, // the gRPC PECL stub is blocking, not coroutine-aware
]);

$clients = [];
$server->on('WorkerStart', function ($srv, int $workerId) use (&$clients) {
    $clients[$workerId] = bootClient(); // <-- ONCE per worker
    $clients[$workerId]->warmup();
});

$server->on('request', function ($req, $resp) use (&$clients) {
    $client = $clients[$req->server['worker_id']]; // <-- warm channel reused
    // ... build per-request UdbMetadata from $req, call $client->select(...) ...
    $resp->end($out);
});

$server->start();
```

> Keep `enable_coroutine => false` for the blocking `grpc` PECL stub. If you
> enable coroutines, the synchronous `BaseStub::wait()` call will block the
> whole worker's coroutine scheduler. A blocking worker pool (the default here)
> is correct for the gRPC PECL extension.

Full version: [`examples/persistent-worker/worker.php`](../sdk/php/examples/persistent-worker/worker.php)
(run with `UDB_RUN_SWOOLE=1` and ext-openswoole loaded).

### RoadRunner

RoadRunner keeps a pool of long-lived PHP worker processes. Each worker runs
your script **once**: build the client **before** the accept loop, then pull
requests in a `while ($req = $psr7->waitRequest())` loop, reusing the same
channel for every request.

```php
$client = new UdbClient([ /* endpoint + keepalive */ ]);
$client->warmup();                       // <-- ONCE, before the loop

$psr7 = new Spiral\RoadRunner\Http\PSR7Worker(/* ... */);
while ($req = $psr7->waitRequest()) {     // long-lived loop
    // ... build per-request UdbMetadata from $req headers ...
    $rs = $client->select($selectRequest, $meta); // <-- warm channel reused
    $psr7->respond(/* ... */);
}
```

`.rr.yaml`:

```yaml
server:
  command: "php examples/persistent-worker/roadrunner-worker.php"
http:
  address: 0.0.0.0:8080
  pool:
    num_workers: 4
```

Full version: [`examples/persistent-worker/roadrunner-worker.php`](../sdk/php/examples/persistent-worker/roadrunner-worker.php).

### FrankenPHP (worker mode)

FrankenPHP's **worker mode** boots your worker script once and then loops,
handing it requests via `frankenphp_handle_request()`. Everything **before**
the loop runs once per worker; the closure passed to `frankenphp_handle_request`
runs per request. Build the UDB client before the loop.

```php
<?php
// worker bootstrap — runs ONCE per FrankenPHP worker
require __DIR__ . '/../../vendor/autoload.php';

$client = new \Fahara02\UdbLaravel\UdbClient([
    'endpoint' => getenv('UDB_ENDPOINT') ?: '127.0.0.1:50051',
    'channel_options' => [
        'grpc.keepalive_time_ms'              => 30000,
        'grpc.keepalive_timeout_ms'           => 10000,
        'grpc.keepalive_permit_without_calls' => 1,
        'grpc.max_connection_idle_ms'         => 600000,
    ],
]);
$client->warmup();

// per-request loop — $client is captured warm
$handler = static function () use ($client) {
    // ... derive UdbMetadata from $_SERVER / superglobals, call $client->select(...) ...
    echo $out;
};
for ($running = true; $running;) {
    $running = \frankenphp_handle_request($handler);
}
```

Run it (Caddyfile snippet):

```caddyfile
frankenphp {
    worker ./public/worker.php 4   # 4 workers, each holding a warm UDB channel
}
```

> Caveat: state in the bootstrap (the `$client`) persists across requests by
> design — that is exactly what keeps the channel warm. Do **not** put
> per-request, mutable state into the bootstrap; keep request data in the
> per-request `UdbMetadata` you pass to each RPC. The UDB client itself is
> request-stateless, so sharing it is safe.

---

## Keepalive for the warm-worker channel

A warm channel is only a win if it stays open between requests. By default an
idle HTTP/2 connection can be parked to `IDLE` and torn down, forcing a
re-handshake on the next request — reintroducing the exact cost you removed.

Set these gRPC channel args on the worker's client (the PHP SDK config already
defaults the first two; the example sets all four):

| Channel arg | Example value | Effect |
|---|---|---|
| `grpc.keepalive_time_ms` | `30000` | send a keepalive PING every 30s so the connection stays live |
| `grpc.keepalive_timeout_ms` | `10000` | wait 10s for the PING ack before considering the connection dead |
| `grpc.keepalive_permit_without_calls` | `1` | keep pinging even with no in-flight RPCs (an idle worker stays warm) |
| `grpc.max_connection_idle_ms` | `600000` | don't let gRPC park the connection to IDLE for 10 min |

> Tune `keepalive_time_ms` against the broker's server-side keepalive policy —
> pinging more often than the server permits can get the connection closed with
> `GOAWAY too_many_pings`. 30s is a safe default for a co-located broker.

The PHP SDK reads `grpc.keepalive_time_ms` / `grpc.keepalive_timeout_ms` from
`config/udb.php#channel_options` (env `UDB_GRPC_KEEPALIVE_MS` /
`UDB_GRPC_KEEPALIVE_TIMEOUT_MS`); add the other two there for a worker
deployment.

---

## Local UDB sidecar over a Unix domain socket

When the PHP app and the UDB broker run on the **same host** (a sidecar
container in the same pod, or the broker as a local daemon), connect over a
**Unix domain socket** instead of TCP loopback. This removes the TCP/loopback
hop and, inside containers, the NAT cost — the biggest real win for co-located
deployments.

gRPC accepts a `unix:` target string, and the UDB PHP client passes the
endpoint **verbatim** to the gRPC stub, so you connect over a UDS simply by
setting the endpoint to a `unix:` URI:

```php
$client = new UdbClient([
    'endpoint' => 'unix:///var/run/udb.sock',   // <-- Unix domain socket
    'channel_options' => [ /* keepalive as above */ ],
]);
```

or via env:

```bash
UDB_ENDPOINT=unix:///var/run/udb.sock
```

Accepted UDS target forms (all begin with the `unix:` scheme):

| Form | Meaning |
|---|---|
| `unix:///var/run/udb.sock` | URI form, absolute path (recommended) |
| `unix:/var/run/udb.sock`   | absolute path |
| `unix:run/udb.sock`        | path relative to the worker's CWD |

The PHP client recognizes a `unix:` endpoint and defaults the HTTP/2
`:authority` to `localhost` (a UDS has no host to derive it from, and some
servers reject an empty authority). You can override it with `tls.target` if
your deployment needs a specific authority. The existing **host:port** API is
unchanged — a `127.0.0.1:50051` endpoint behaves exactly as before.

Combine the sidecar with a persistent worker for the best result: a warm
channel **and** a UDS transport.

### Broker-side UDS listener (implemented — `UDB_DATA_UDS_PATH`)

UDS is a two-sided contract: the broker must also bind a Unix-domain-socket
listener at the path the PHP client targets. **This is now built in.** Set
`UDB_DATA_UDS_PATH` on the broker to an absolute socket path and it binds an
additional DataBroker listener on that socket alongside its TCP listener:

```bash
# broker (same host/pod as the PHP worker), Unix only:
UDB_DATA_UDS_PATH=/var/run/udb.sock udb serve …
```

```bash
# PHP worker:
UDB_ENDPOINT=unix:///var/run/udb.sock
```

Details of the broker behaviour:

- **Unix-only** (the listener is `#[cfg(unix)]`; on Windows the variable is
  ignored and only the TCP listener runs).
- The UDS listener wraps the **same** security/timeout/concurrency tower layer
  as the TCP listener — a UDS connection is a transport swap, **not** an auth
  bypass; `MethodSecurityLayer` still enforces every RPC.
- A stale socket file from a prior run is removed on startup before bind; a bind
  failure is **logged and non-fatal** (the TCP listener keeps serving).
- Make sure the socket file's filesystem permissions allow the co-located PHP
  worker's user to connect (set the directory/umask accordingly).

The persistent-worker + keepalive path in this guide is independently shippable
and does not depend on the UDS bind; UDS additionally removes the
TCP/loopback+NAT hop when broker and worker share a host.

---

## Which fix do I need?

| Situation | Fix |
|---|---|
| PHP-FPM, broker is remote | Persistent worker (OpenSwoole / RoadRunner / FrankenPHP) + keepalive |
| Broker co-located on the same host/pod | Persistent worker **+** Unix-domain-socket endpoint (`UDB_DATA_UDS_PATH` on the broker, `unix://` on the client) |
| Can't change runtime yet | At minimum call `$client->warmup()` early; but PHP-FPM still re-handshakes per request — a worker runtime is the real fix |

Benchmark before/after with [`sdk/php/bench/bench.php`](../sdk/php/bench/) on a
**release** broker and record the warm-channel numbers.
