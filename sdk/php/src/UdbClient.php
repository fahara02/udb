<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel;

use Fahara02\UdbLaravel\Exceptions\UdbConfigurationException;
use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Grpc\BaseStub;
use Grpc\ChannelCredentials;
use Grpc\Timeval;
use Udb\Entity\V1\CapabilitiesRequest;
use Udb\Entity\V1\CapabilitiesResponse;
use Udb\Entity\V1\DeleteRequest;
use Udb\Entity\V1\MutationResponse;
use Udb\Entity\V1\RecordSet;
use Udb\Entity\V1\SelectRequest;
use Udb\Entity\V1\UpsertRequest;
use Udb\Services\V1\DataBrokerClient;

/**
 * The main UDB client for Laravel applications.
 *
 * Holds a single, long-lived gRPC channel + a `DataBrokerClient`
 * stub. Designed to be a singleton in the container — gRPC
 * channels multiplex requests over HTTP/2 and are safe to share
 * across the entire application lifetime. Per-request data
 * (tenant_id, correlation_id) flows in via the bound
 * `UdbMetadata`; the client itself is request-stateless.
 *
 * Typical wiring (handled automatically by the ServiceProvider):
 *
 *   $client = new UdbClient($config);
 *   $client->bindContext($metadata);          // per-request
 *   $rs = $client->select($selectRequest);    // RPC + metadata injection + error mapping
 *
 * For one-off calls outside a request lifecycle (queue workers,
 * scheduler), pass an explicit `UdbMetadata` as the second arg
 * to any RPC method — that overrides the bound context for that
 * single call.
 */
class UdbClient
{
    /**
     * Default HTTP/2 `:authority` for a Unix-domain-socket endpoint.
     *
     * A `unix:` target has no host component, but gRPC still sends an
     * `:authority` pseudo-header and some servers reject an empty one.
     * `localhost` is the conventional value for a local sidecar reached
     * over a UDS. Overridable per-deployment via `tls.target`.
     */
    private const UDS_DEFAULT_AUTHORITY = 'localhost';

    private ?UdbMetadata $boundContext = null;
    private ?DataBrokerClient $stub = null;

    /**
     * @param  array{
     *   endpoint:string,
     *   tls?: array{enabled?:bool, root_certs?:?string, target?:?string},
     *   channel_options?: array<string,mixed>,
     *   deadline_ms?: int,
     * }  $config
     */
    public function __construct(
        private readonly array $config,
    ) {
        if (! \extension_loaded('grpc')) {
            throw new UdbConfigurationException(
                'The "grpc" PHP extension is not loaded. Install via PECL: '
                . '`pecl install grpc` and add "extension=grpc.so" to php.ini. '
                . 'The UDB SDK speaks the canonical gRPC wire protocol — there '
                . 'is no fallback transport in v0.1.'
            );
        }
        $endpoint = trim((string) ($config['endpoint'] ?? ''));
        if ($endpoint === '') {
            throw new UdbConfigurationException(
                'UDB endpoint is not configured. Set UDB_ENDPOINT or '
                . 'config/udb.php#endpoint to host:port (e.g. "127.0.0.1:50051") '
                . 'or a Unix-domain-socket target (e.g. "unix:///var/run/udb.sock") '
                . 'for a co-located sidecar.'
            );
        }
    }

    /**
     * Is `$endpoint` a gRPC Unix-domain-socket target? gRPC accepts
     * `unix:relative/path`, `unix:/absolute/path` and the URI form
     * `unix:///absolute/path` — all begin with the `unix:` scheme.
     */
    private static function isUnixTarget(string $endpoint): bool
    {
        return str_starts_with($endpoint, 'unix:');
    }

    /**
     * Bind a request-scoped Metadata to this client instance. The
     * container wires this from `UdbContextMiddleware` early in
     * the request lifecycle so every subsequent RPC call inherits
     * the request's tenant + correlation without the caller
     * passing them explicitly.
     */
    public function bindContext(UdbMetadata $metadata): void
    {
        $this->boundContext = $metadata;
    }

    /**
     * Return the bound Metadata (or throw if none bound — common
     * when called from a queue worker without first calling
     * `bindContext` or supplying an explicit metadata override).
     */
    public function context(): UdbMetadata
    {
        if ($this->boundContext === null) {
            throw new UdbConfigurationException(
                'No UDB request context is bound. Either run inside an HTTP '
                . 'request with UdbContextMiddleware enabled, or call '
                . '`$client->bindContext($metadata)` before issuing RPCs. '
                . 'For one-off calls, pass an explicit UdbMetadata as the '
                . 'second argument to the RPC method.'
            );
        }
        return $this->boundContext;
    }

    // ── RPC methods ─────────────────────────────────────────────

    /**
     * Issue a `Select` RPC. Returns the broker's `RecordSet`
     * proto. Throws `UdbRpcException` on non-OK status.
     */
    public function select(SelectRequest $request, ?UdbMetadata $metadata = null): RecordSet
    {
        return $this->invokeUnary(
            'Select',
            fn (array $md, array $opts) => $this->stub()->Select($request, $md, $opts),
            $metadata,
            RecordSet::class,
        );
    }

    /**
     * Issue an `Upsert` RPC. Returns the broker's
     * `MutationResponse` proto.
     */
    public function upsert(UpsertRequest $request, ?UdbMetadata $metadata = null): MutationResponse
    {
        return $this->invokeUnary(
            'Upsert',
            fn (array $md, array $opts) => $this->stub()->Upsert($request, $md, $opts),
            $metadata,
            MutationResponse::class,
        );
    }

    /**
     * Issue a `Delete` RPC. Returns the broker's
     * `MutationResponse` proto.
     */
    public function delete(DeleteRequest $request, ?UdbMetadata $metadata = null): MutationResponse
    {
        return $this->invokeUnary(
            'Delete',
            fn (array $md, array $opts) => $this->stub()->Delete($request, $md, $opts),
            $metadata,
            MutationResponse::class,
        );
    }

    /**
     * Bind a message type/table once, then `select/upsert/delete` with plain PHP
     * arrays instead of hand-built proto requests. `$key` is the conflict
     * (primary-key) field set used for upserts. Binds once; never fetches the
     * catalog per request.
     *
     * @param  list<string>  $key
     */
    public function entity(string $messageType, array $key = []): BoundEntity
    {
        return new BoundEntity($this, $messageType, $key);
    }

    /** Table-shaped alias of {@see entity()} (`$udb->data()->table('invoice')`). */
    public function table(string $name, array $key = []): BoundEntity
    {
        return $this->entity($name, $key);
    }

    /**
     * Encode a PHP record array into the `record_json` bytes the broker expects
     * (canonical JSON). Shared by {@see BoundEntity}.
     *
     * @param  array<string,mixed>  $record
     */
    public static function encodeRecordJson(array $record): string
    {
        return (string) json_encode($record);
    }

    /**
     * Build a `google.protobuf.Struct` filter from a PHP array via its JSON form.
     *
     * @param  array<string,mixed>  $filter
     */
    public static function toStruct(array $filter): \Google\Protobuf\Struct
    {
        $struct = new \Google\Protobuf\Struct();
        if ($filter !== []) {
            $struct->mergeFromJsonString((string) json_encode($filter));
        }

        return $struct;
    }

    /**
     * Warm the underlying gRPC channel with a cheap metadata RPC.
     *
     * Call this once when a long-lived PHP worker starts (Laravel
     * Octane, queues, or PHP-FPM worker bootstrapping) to move the
     * first-call channel setup cost out of the first user-facing CRUD
     * request.
     */
    public function warmup(?UdbMetadata $metadata = null): CapabilitiesResponse
    {
        return $this->invokeUnary(
            'GetCapabilities',
            fn (array $md, array $opts) => $this->stub()->GetCapabilities(new CapabilitiesRequest(), $md, $opts),
            $metadata,
            CapabilitiesResponse::class,
        );
    }

    /**
     * Direct access to the underlying generated stub for RPCs the
     * convenience methods above don't wrap (BatchSelect, Vector*,
     * BeginTx, PublishCDC, etc.). Callers are responsible for
     * supplying metadata + handling status codes.
     *
     * Intentionally returns the bare `DataBrokerClient` — the
     * generated class is the most authoritative API surface and
     * we don't want to lock callers out of new methods that
     * regen adds.
     */
    public function stub(): DataBrokerClient
    {
        if ($this->stub === null) {
            $this->stub = $this->buildStub();
        }
        return $this->stub;
    }

    // ── Internals ───────────────────────────────────────────────

    private function buildStub(): DataBrokerClient
    {
        $tls = (array) ($this->config['tls'] ?? []);
        $tlsEnabled = (bool) ($tls['enabled'] ?? false);

        if ($tlsEnabled) {
            $rootCerts = $tls['root_certs'] ?? null;
            if ($rootCerts !== null && ! is_readable((string) $rootCerts)) {
                throw new UdbConfigurationException(
                    "UDB TLS root cert bundle not readable at: {$rootCerts}"
                );
            }
            $pem = $rootCerts !== null
                ? (string) file_get_contents((string) $rootCerts)
                : null;
            $credentials = ChannelCredentials::createSsl($pem);
        } else {
            $credentials = ChannelCredentials::createInsecure();
        }

        $options = (array) ($this->config['channel_options'] ?? []);
        $options['credentials'] = $credentials;

        $endpoint = (string) $this->config['endpoint'];
        $target = $tls['target'] ?? $endpoint;
        if ($target !== $endpoint) {
            // gRPC's PHP extension honours `grpc.default_authority`
            // for SNI / authority override — match Go's
            // `WithAuthority(...)` semantics.
            $options['grpc.default_authority'] = (string) $target;
        } elseif (self::isUnixTarget($endpoint) && ! isset($options['grpc.default_authority'])) {
            // A `unix:` target carries no host, so gRPC has no authority
            // to derive. Default it so servers that require a non-empty
            // `:authority` accept the call. host:port endpoints are
            // unaffected — they keep gRPC's host-derived authority.
            $options['grpc.default_authority'] = self::UDS_DEFAULT_AUTHORITY;
        }

        // The endpoint is passed verbatim as the gRPC target, so a
        // `unix:///var/run/udb.sock` value connects over a Unix domain
        // socket with no code change beyond the authority default above.
        return new DataBrokerClient($endpoint, $options);
    }

    /**
     * Shared unary RPC invoker: builds metadata + deadline,
     * executes the bound closure, maps non-OK status to a typed
     * exception. Every convenience method funnels through here so
     * error handling is consistent.
     *
     * @template T of object
     * @param  callable(array<string,list<string>>, array<string,mixed>):mixed  $invoker
     * @param  class-string<T>  $responseClass
     * @return T
     */
    private function invokeUnary(
        string $rpcName,
        callable $invoker,
        ?UdbMetadata $metadata,
        string $responseClass,
    ): object {
        $meta = $metadata ?? $this->context();
        $deadlineMs = (int) ($this->config['deadline_ms'] ?? 30_000);
        $opts = [];
        if ($deadlineMs > 0) {
            // gRPC PHP wants seconds + microseconds.
            $opts['timeout'] = $deadlineMs * 1000;
        }

        /** @var \Grpc\UnaryCall $call */
        $call = $invoker($meta->toGrpcMetadata(), $opts);
        /**
         * `wait()` returns [response, status]. Status is
         * `['metadata' => ..., 'code' => int, 'details' => string]`.
         * Code 0 (OK) means success.
         *
         * @var array{0: ?object, 1: array{code:int,details:string,metadata?:array<string,mixed>}} $result
         */
        $result = $call->wait();
        [$response, $status] = $result;

        $statusCode = is_object($status) ? ($status->code ?? -1) : ($status['code'] ?? -1);
        if ($statusCode !== 0) {
            throw UdbRpcException::fromGrpcStatus($status, $rpcName);
        }

        if ($response === null) {
            // OK status with a null body is a server bug; surface
            // explicitly rather than handing back null and
            // tripping a downstream NPE.
            throw new UdbRpcException(
                status: 13, // INTERNAL
                details: "UDB {$rpcName} returned OK status with empty body",
                raw: $status,
                rpcName: $rpcName,
            );
        }

        if (! $response instanceof $responseClass) {
            throw new UdbRpcException(
                status: 13, // INTERNAL
                details: "UDB {$rpcName} returned an unexpected response type",
                raw: $status,
                rpcName: $rpcName,
            );
        }

        /** @var T $response */
        return $response;
    }
}
