<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Cache\Services\V1;

/**
 * CacheService (master-plan 9.6) — a cache that invalidates itself.
 *
 * This PROMOTES the four typed DataBroker cache RPCs (CacheGet/CacheSet/
 * CacheDelete/CacheScan, which remain as additive aliases) into a first-class
 * native service with bounded, namespaced, claim-scoped keys. Every entry lives
 * under `udb:cache:<tenant>:<ns>:<key>` where `<tenant>` is derived from the
 * VERIFIED bearer/claim tenant — never a body-supplied value — so two tenants
 * can use the same namespace and key without colliding and a caller can never
 * read or sweep another tenant's namespace. Each namespace carries a per-tenant
 * memory budget (`max_bytes`); a Set that would exceed it fails closed with
 * `resource_exhausted`. Prefix sweeps use Redis `SCAN`, never `KEYS`. A
 * leader-elected CDC invalidation worker maps source-table changes to a
 * namespace sweep and emits `udb.cache.invalidated.v1`.
 */
class CacheServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Read a value from a namespaced cache key. Tenant-scoped: the key is derived
     * from the verified claim tenant, so a caller can never read another tenant's
     * entry by spoofing the body tenant_id.
     * @param \Udb\Core\Cache\Services\V1\GetRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Cache\Services\V1\GetResponse>
     */
    public function Get(\Udb\Core\Cache\Services\V1\GetRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.cache.services.v1.CacheService/Get',
        $argument,
        ['\Udb\Core\Cache\Services\V1\GetResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Write a value with an optional TTL. Bounded: a write that would push the
     * namespace over its per-tenant `max_bytes` budget fails closed with
     * `resource_exhausted`.
     * @param \Udb\Core\Cache\Services\V1\SetRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Cache\Services\V1\SetResponse>
     */
    public function Set(\Udb\Core\Cache\Services\V1\SetRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.cache.services.v1.CacheService/Set',
        $argument,
        ['\Udb\Core\Cache\Services\V1\SetResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Delete a single namespaced key. Idempotent.
     * @param \Udb\Core\Cache\Services\V1\DeleteRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Cache\Services\V1\DeleteResponse>
     */
    public function Delete(\Udb\Core\Cache\Services\V1\DeleteRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.cache.services.v1.CacheService/Delete',
        $argument,
        ['\Udb\Core\Cache\Services\V1\DeleteResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Cursor-paged scan over a namespace key prefix. Implemented with Redis SCAN
     * (never KEYS), so it never blocks the server on a large keyspace.
     * @param \Udb\Core\Cache\Services\V1\ScanRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Cache\Services\V1\ScanResponse>
     */
    public function Scan(\Udb\Core\Cache\Services\V1\ScanRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.cache.services.v1.CacheService/Scan',
        $argument,
        ['\Udb\Core\Cache\Services\V1\ScanResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Declare (or update) a namespace and its per-tenant byte budget + default TTL.
     * @param \Udb\Core\Cache\Services\V1\CreateNamespaceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Cache\Services\V1\CreateNamespaceResponse>
     */
    public function CreateNamespace(\Udb\Core\Cache\Services\V1\CreateNamespaceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.cache.services.v1.CacheService/CreateNamespace',
        $argument,
        ['\Udb\Core\Cache\Services\V1\CreateNamespaceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Flush an entire namespace for the caller's tenant (SCAN+DEL sweep) and emit
     * an invalidation event. DESTRUCTIVE — gated by a confirmation token.
     * @param \Udb\Core\Cache\Services\V1\DeleteNamespaceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Cache\Services\V1\DeleteNamespaceResponse>
     */
    public function DeleteNamespace(\Udb\Core\Cache\Services\V1\DeleteNamespaceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.cache.services.v1.CacheService/DeleteNamespace',
        $argument,
        ['\Udb\Core\Cache\Services\V1\DeleteNamespaceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Report a namespace's current used-bytes counter, configured budget, and item
     * count for the caller's tenant.
     * @param \Udb\Core\Cache\Services\V1\GetNamespaceStatsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Cache\Services\V1\GetNamespaceStatsResponse>
     */
    public function GetNamespaceStats(\Udb\Core\Cache\Services\V1\GetNamespaceStatsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.cache.services.v1.CacheService/GetNamespaceStats',
        $argument,
        ['\Udb\Core\Cache\Services\V1\GetNamespaceStatsResponse', 'decode'],
        $metadata, $options);
    }

}
