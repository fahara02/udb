<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Metering\Services\V1;

/**
 * MeteringService (master-plan 9.9) — usage metering and quotas. Usage is an
 * append-only, durable stream of `UsageEvent` rows (written by a cheap admission
 * hook and by explicit RecordUsage ingest); quotas (`QuotaRule`) cap a metric
 * over a rolling window. CheckQuota is PURE aggregation — it sums the durable
 * rows in the window and compares against the limit (never an in-memory counter,
 * which would lie across restarts and replicas). Metering must NEVER fail the
 * metered request: the ingest hook log-and-swallows on error. Quota mutations are
 * durable, tenant-scoped by the verified claim, bump a monotone per-row revision,
 * and emit `udb.metering.quota.changed.v1`.
 */
class MeteringServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Explicitly ingest a usage event. Durable append (single INSERT, no read);
     * attribution-only — it never blocks the caller's real operation.
     * @param \Udb\Core\Metering\Services\V1\RecordUsageRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Metering\Services\V1\RecordUsageResponse>
     */
    public function RecordUsage(\Udb\Core\Metering\Services\V1\RecordUsageRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.metering.services.v1.MeteringService/RecordUsage',
        $argument,
        ['\Udb\Core\Metering\Services\V1\RecordUsageResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Aggregate a tenant's usage for a metric over a rolling window (durable SUM).
     * @param \Udb\Core\Metering\Services\V1\QueryUsageRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Metering\Services\V1\QueryUsageResponse>
     */
    public function QueryUsage(\Udb\Core\Metering\Services\V1\QueryUsageRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.metering.services.v1.MeteringService/QueryUsage',
        $argument,
        ['\Udb\Core\Metering\Services\V1\QueryUsageResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Create or update a quota rule at a (tenant, project, metric) scope. Bumps the
     * rule's monotone revision and emits `udb.metering.quota.changed.v1`.
     * @param \Udb\Core\Metering\Services\V1\PutQuotaRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Metering\Services\V1\PutQuotaResponse>
     */
    public function PutQuota(\Udb\Core\Metering\Services\V1\PutQuotaRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.metering.services.v1.MeteringService/PutQuota',
        $argument,
        ['\Udb\Core\Metering\Services\V1\PutQuotaResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Fetch a single quota rule at an exact (tenant, project, metric) scope.
     * @param \Udb\Core\Metering\Services\V1\GetQuotaRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Metering\Services\V1\GetQuotaResponse>
     */
    public function GetQuota(\Udb\Core\Metering\Services\V1\GetQuotaRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.metering.services.v1.MeteringService/GetQuota',
        $argument,
        ['\Udb\Core\Metering\Services\V1\GetQuotaResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List a tenant's quota rules, optionally narrowed to a project.
     * @param \Udb\Core\Metering\Services\V1\ListQuotasRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Metering\Services\V1\ListQuotasResponse>
     */
    public function ListQuotas(\Udb\Core\Metering\Services\V1\ListQuotasRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.metering.services.v1.MeteringService/ListQuotas',
        $argument,
        ['\Udb\Core\Metering\Services\V1\ListQuotasResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Check a quota: sum durable usage in the rule's window and compare against the
     * limit. Returns {allowed, used, limit, remaining}. The ingest hook remains
     * best-effort, but explicit quota checks fail closed when the durable aggregate
     * is unavailable, so an outage cannot silently bypass an enabled quota.
     * @param \Udb\Core\Metering\Services\V1\CheckQuotaRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Metering\Services\V1\CheckQuotaResponse>
     */
    public function CheckQuota(\Udb\Core\Metering\Services\V1\CheckQuotaRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.metering.services.v1.MeteringService/CheckQuota',
        $argument,
        ['\Udb\Core\Metering\Services\V1\CheckQuotaResponse', 'decode'],
        $metadata, $options);
    }

}
