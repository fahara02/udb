# UDB persistent-worker example (warm gRPC channel for PHP)

PHP-FPM is shared-nothing, so it opens a **new gRPC channel per request** and
re-pays the TCP + TLS + HTTP/2 handshake every time ([grpc#15426]). The fix is a
**long-lived worker** that builds the UDB client **once** and reuses the warm
channel across requests.

Full rationale and per-runtime setup: [`docs/php-perf.md`](../../../../docs/php-perf.md).

## Files

| File | What it shows |
|---|---|
| `worker.php` | Runtime-agnostic worker. Builds the client once (`bootClient()`), serves a request on the warm channel (`handleRequest()`). Includes an **OpenSwoole** HTTP-server driver and a **plain-CLI** fallback you can run with no extra runtime. |
| `roadrunner-worker.php` | A **RoadRunner** PSR-7 worker: builds the client before the accept loop, reuses the warm channel for every request. |

## Run the CLI fallback (no runtime required)

```bash
cd sdk/php
UDB_ENDPOINT=127.0.0.1:50051 \
UDB_TENANT=default \
php examples/persistent-worker/worker.php
```

It boots the client once, warms the channel, then issues 5 RPCs — only the
first pays channel setup; the rest run on the warm channel.

## Run the OpenSwoole server

```bash
# requires ext-openswoole
UDB_RUN_SWOOLE=1 UDB_WORKER_PORT=9501 php examples/persistent-worker/worker.php
# then: curl http://127.0.0.1:9501/
```

## Use a local sidecar over a Unix domain socket

Point the endpoint at a `unix:` target instead of host:port:

```bash
UDB_ENDPOINT=unix:///var/run/udb.sock php examples/persistent-worker/worker.php
```

> Requires the **broker** to bind a UDS listener at that path — a Rust change
> owned by the broker maintainer (see the "Required broker-side follow-up"
> section in `docs/php-perf.md`). Until then, use a `host:port` endpoint; the
> warm-channel fix works regardless of transport.

## Keepalive

The examples set `grpc.keepalive_time_ms`, `grpc.keepalive_timeout_ms`,
`grpc.keepalive_permit_without_calls`, and `grpc.max_connection_idle_ms` so the
warm channel is not dropped to IDLE and re-handshaked between requests.

[grpc#15426]: https://github.com/grpc/grpc/issues/15426
