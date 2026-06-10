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

}
