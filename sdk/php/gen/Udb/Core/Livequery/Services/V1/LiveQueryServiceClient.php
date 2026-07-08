<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Livequery\Services\V1;

/**
 * LiveQueryService (master-plan 9.7) — query results that update themselves. A
 * client subscribes to a tenant-scoped query over a source entity and receives
 * an initial Snapshot (the current matching rows) followed by an open stream of
 * Change deltas (insert / update / delete) as the underlying data mutates.
 *
 * Tenant isolation is the whole point: the snapshot is produced ONLY through the
 * mediated IR read path with the tenant predicate injected server-side from the
 * verified claim (never a raw query), and EVERY delta event is re-checked, fail
 * closed, against the subscriber's tenant scope before it is yielded — a CDC
 * event with a missing or foreign tenant_id is dropped, never streamed. The
 * subscription filter is an IR-expressible predicate set, not a raw query
 * string, so no caller-supplied SQL ever reaches a backend.
 */
class LiveQueryServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Subscribe to a tenant-scoped live query. SERVER-STREAMING: the first message
     * carries the initial Snapshot (the current rows matching the IR filter, read
     * through the mediated path with the tenant predicate injected server-side);
     * every subsequent message carries a single Change delta. Fails closed
     * (failed_precondition) when the source entity has no resolvable tenant column.
     * @param \Udb\Core\Livequery\Services\V1\SubscribeRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Subscribe(\Udb\Core\Livequery\Services\V1\SubscribeRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/udb.core.livequery.services.v1.LiveQueryService/Subscribe',
        $argument,
        ['\Udb\Core\Livequery\Services\V1\SubscribeResponse', 'decode'],
        $metadata, $options);
    }

}
