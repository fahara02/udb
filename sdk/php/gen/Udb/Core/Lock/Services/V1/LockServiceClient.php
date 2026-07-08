<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Lock\Services\V1;

/**
 * LockService (master-plan 9.2) — distributed locks for applications. Backed by
 * the portable `udb_advisory_leases` mutual-exclusion primitive, with a durable
 * tenant-scoped bookkeeping row and a monotone fencing token per grant so a
 * slow/partitioned holder can be safely fenced off.
 */
class LockServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Acquire a distributed lock. Quota-aware: a tenant cannot exceed its active
     * lock budget. Returns the monotone fencing token the holder must present on
     * Renew/Release.
     * @param \Udb\Core\Lock\Services\V1\AcquireLockRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Lock\Services\V1\AcquireLockResponse>
     */
    public function AcquireLock(\Udb\Core\Lock\Services\V1\AcquireLockRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.lock.services.v1.LockService/AcquireLock',
        $argument,
        ['\Udb\Core\Lock\Services\V1\AcquireLockResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Renew (extend the lease of) a lock the caller currently holds. The presented
     * fencing token must not be stale; a lower token is rejected.
     * @param \Udb\Core\Lock\Services\V1\RenewLockRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Lock\Services\V1\RenewLockResponse>
     */
    public function RenewLock(\Udb\Core\Lock\Services\V1\RenewLockRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.lock.services.v1.LockService/RenewLock',
        $argument,
        ['\Udb\Core\Lock\Services\V1\RenewLockResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Release a lock the caller currently holds. The presented fencing token must
     * not be stale; a lower token is rejected.
     * @param \Udb\Core\Lock\Services\V1\ReleaseLockRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Lock\Services\V1\ReleaseLockResponse>
     */
    public function ReleaseLock(\Udb\Core\Lock\Services\V1\ReleaseLockRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.lock.services.v1.LockService/ReleaseLock',
        $argument,
        ['\Udb\Core\Lock\Services\V1\ReleaseLockResponse', 'decode'],
        $metadata, $options);
    }

}
