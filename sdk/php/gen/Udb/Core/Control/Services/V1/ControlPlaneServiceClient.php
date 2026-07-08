<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Control\Services\V1;

/**
 * ---------------------------------------------------------------------------
 * ControlPlaneService — Versioned, ACK/NACK, nonce-paired, ordered control-plane
 * policy/config distribution (xDS-style). Nodes (data-plane PEPs) open an
 * aggregated state-of-the-world stream (StreamResources) or an incremental delta
 * stream (DeltaResources) and receive versioned resources in dependency order
 * (backend-target definitions before referencing routing/RLS policies), each
 * response carrying a fresh nonce the node echoes to ACK (apply) or NACK (reject
 * without applying). A node that NACKs keeps its last-good version. Unary helpers
 * fetch resources on demand (incl. by tenant) and expose per-node ack visibility.
 *
 * Server-only control plane: runs on the isolated native auth listener with an
 * admin/service-account credential; never exposed on the public DataBroker port.
 * ---------------------------------------------------------------------------
 */
class ControlPlaneServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * ── Aggregated state-of-the-world (ADS) ───────────────────────────────────
     * Node↔broker push channel only: a data-plane PEP node opens this bidirectional
     * stream to receive versioned resources and echo ACK/NACK nonces. It carries no
     * human/session credential, no REST surface, and is never part of an application
     * CRUD facade — so it is gated to internal callers (a loopback node or a node
     * presenting a verified mTLS identity); an untrusted remote caller is rejected.
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\BidiStreamingCall
     */
    public function StreamResources($metadata = [], $options = []) {
        return $this->_bidiRequest('/udb.core.control.services.v1.ControlPlaneService/StreamResources',
        ['\Udb\Core\Control\Services\V1\DiscoveryResponse','decode'],
        $metadata, $options);
    }

    /**
     * ── Incremental / delta discovery ─────────────────────────────────────────
     * Same node↔broker push semantics as StreamResources (incremental form). Only a
     * data-plane node should open it; restricted to internal callers for the same
     * reasons (no session credential, no REST surface, not an application facade RPC).
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\BidiStreamingCall
     */
    public function DeltaResources($metadata = [], $options = []) {
        return $this->_bidiRequest('/udb.core.control.services.v1.ControlPlaneService/DeltaResources',
        ['\Udb\Core\Control\Services\V1\DeltaDiscoveryResponse','decode'],
        $metadata, $options);
    }

    /**
     * ── On-demand fetch (incl. by tenant) ─────────────────────────────────────
     * @param \Udb\Core\Control\Services\V1\GetResourcesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Control\Services\V1\GetResourcesResponse>
     */
    public function GetResources(\Udb\Core\Control\Services\V1\GetResourcesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.control.services.v1.ControlPlaneService/GetResources',
        $argument,
        ['\Udb\Core\Control\Services\V1\GetResourcesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Admin visibility ──────────────────────────────────────────────────────
     * @param \Udb\Core\Control\Services\V1\ListNodeStatesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Control\Services\V1\ListNodeStatesResponse>
     */
    public function ListNodeStates(\Udb\Core\Control\Services\V1\ListNodeStatesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.control.services.v1.ControlPlaneService/ListNodeStates',
        $argument,
        ['\Udb\Core\Control\Services\V1\ListNodeStatesResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Control\Services\V1\AckStatusRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Control\Services\V1\AckStatusResponse>
     */
    public function AckStatus(\Udb\Core\Control\Services\V1\AckStatusRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.control.services.v1.ControlPlaneService/AckStatus',
        $argument,
        ['\Udb\Core\Control\Services\V1\AckStatusResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Rollback a node/resource-type to a retained served snapshot ────────────
     * @param \Udb\Core\Control\Services\V1\RollbackResourcesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Control\Services\V1\RollbackResourcesResponse>
     */
    public function RollbackResources(\Udb\Core\Control\Services\V1\RollbackResourcesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.control.services.v1.ControlPlaneService/RollbackResources',
        $argument,
        ['\Udb\Core\Control\Services\V1\RollbackResourcesResponse', 'decode'],
        $metadata, $options);
    }

}
