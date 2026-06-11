<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Analytics\Services\V1;

/**
 * ---------------------------------------------------------------------------
 * AnalyticsService - pipeline statistics, performance dashboards, Prometheus
 * metric backing, SLA compliance reporting, and executor/reconciliation analytics.
 * 
 * HTTP prefix: /v1/analytics
 * ---------------------------------------------------------------------------
 *
 */
class AnalyticsServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Record a single pipeline stage request observation (called per-request).
     * @param \Udb\Core\Analytics\Services\V1\RecordPipelineMetricRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Analytics\Services\V1\RecordPipelineMetricResponse>
     */
    public function RecordPipelineMetric(\Udb\Core\Analytics\Services\V1\RecordPipelineMetricRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.analytics.services.v1.AnalyticsService/RecordPipelineMetric',
        $argument,
        ['\Udb\Core\Analytics\Services\V1\RecordPipelineMetricResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Query aggregated pipeline stage performance snapshots.
     * @param \Udb\Core\Analytics\Services\V1\GetPipelineSummaryRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Analytics\Services\V1\GetPipelineSummaryResponse>
     */
    public function GetPipelineSummary(\Udb\Core\Analytics\Services\V1\GetPipelineSummaryRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.analytics.services.v1.AnalyticsService/GetPipelineSummary',
        $argument,
        ['\Udb\Core\Analytics\Services\V1\GetPipelineSummaryResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Query daily executor performance roll-ups.
     * @param \Udb\Core\Analytics\Services\V1\GetExecutorPerformanceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Analytics\Services\V1\GetExecutorPerformanceResponse>
     */
    public function GetExecutorPerformance(\Udb\Core\Analytics\Services\V1\GetExecutorPerformanceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.analytics.services.v1.AnalyticsService/GetExecutorPerformance',
        $argument,
        ['\Udb\Core\Analytics\Services\V1\GetExecutorPerformanceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Query daily reconciliation and conflict analytics.
     * @param \Udb\Core\Analytics\Services\V1\GetReconciliationAnalyticsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Analytics\Services\V1\GetReconciliationAnalyticsResponse>
     */
    public function GetReconciliationAnalytics(\Udb\Core\Analytics\Services\V1\GetReconciliationAnalyticsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.analytics.services.v1.AnalyticsService/GetReconciliationAnalytics',
        $argument,
        ['\Udb\Core\Analytics\Services\V1\GetReconciliationAnalyticsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get throughput statistics over a time window.
     * @param \Udb\Core\Analytics\Services\V1\GetThroughputRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Analytics\Services\V1\GetThroughputResponse>
     */
    public function GetThroughput(\Udb\Core\Analytics\Services\V1\GetThroughputRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.analytics.services.v1.AnalyticsService/GetThroughput',
        $argument,
        ['\Udb\Core\Analytics\Services\V1\GetThroughputResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get SLA compliance report for a stage and time period.
     * @param \Udb\Core\Analytics\Services\V1\GetSlaComplianceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Analytics\Services\V1\GetSlaComplianceResponse>
     */
    public function GetSlaCompliance(\Udb\Core\Analytics\Services\V1\GetSlaComplianceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.analytics.services.v1.AnalyticsService/GetSlaCompliance',
        $argument,
        ['\Udb\Core\Analytics\Services\V1\GetSlaComplianceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Manually trigger hourly snapshot aggregation (normally a cron job).
     * @param \Udb\Core\Analytics\Services\V1\TriggerSnapshotRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Analytics\Services\V1\TriggerSnapshotResponse>
     */
    public function TriggerSnapshot(\Udb\Core\Analytics\Services\V1\TriggerSnapshotRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.analytics.services.v1.AnalyticsService/TriggerSnapshot',
        $argument,
        ['\Udb\Core\Analytics\Services\V1\TriggerSnapshotResponse', 'decode'],
        $metadata, $options);
    }

}
