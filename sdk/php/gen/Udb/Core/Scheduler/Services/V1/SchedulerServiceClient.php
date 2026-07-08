<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Scheduler\Services\V1;

/**
 * SchedulerService — durable cron and one-shot jobs as a native service.
 * Mutations persist to the canonical store, tenant-scoped by the verified claim,
 * and emit one outbox event each. The leader-elected scheduler tick fires DUE
 * jobs as outbox events only (consumers do the work).
 */
class SchedulerServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Create a cron or one-shot job.
     * @param \Udb\Core\Scheduler\Services\V1\CreateJobRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Scheduler\Services\V1\CreateJobResponse>
     */
    public function CreateJob(\Udb\Core\Scheduler\Services\V1\CreateJobRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.scheduler.services.v1.SchedulerService/CreateJob',
        $argument,
        ['\Udb\Core\Scheduler\Services\V1\CreateJobResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get a job by id.
     * @param \Udb\Core\Scheduler\Services\V1\GetJobRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Scheduler\Services\V1\GetJobResponse>
     */
    public function GetJob(\Udb\Core\Scheduler\Services\V1\GetJobRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.scheduler.services.v1.SchedulerService/GetJob',
        $argument,
        ['\Udb\Core\Scheduler\Services\V1\GetJobResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List jobs for the caller's tenant.
     * @param \Udb\Core\Scheduler\Services\V1\ListJobsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Scheduler\Services\V1\ListJobsResponse>
     */
    public function ListJobs(\Udb\Core\Scheduler\Services\V1\ListJobsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.scheduler.services.v1.SchedulerService/ListJobs',
        $argument,
        ['\Udb\Core\Scheduler\Services\V1\ListJobsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Delete (soft-delete) a job.
     * @param \Udb\Core\Scheduler\Services\V1\DeleteJobRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Scheduler\Services\V1\DeleteJobResponse>
     */
    public function DeleteJob(\Udb\Core\Scheduler\Services\V1\DeleteJobRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.scheduler.services.v1.SchedulerService/DeleteJob',
        $argument,
        ['\Udb\Core\Scheduler\Services\V1\DeleteJobResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Pause a job so the tick stops claiming it.
     * @param \Udb\Core\Scheduler\Services\V1\PauseJobRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Scheduler\Services\V1\PauseJobResponse>
     */
    public function PauseJob(\Udb\Core\Scheduler\Services\V1\PauseJobRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.scheduler.services.v1.SchedulerService/PauseJob',
        $argument,
        ['\Udb\Core\Scheduler\Services\V1\PauseJobResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Resume a paused job.
     * @param \Udb\Core\Scheduler\Services\V1\ResumeJobRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Scheduler\Services\V1\ResumeJobResponse>
     */
    public function ResumeJob(\Udb\Core\Scheduler\Services\V1\ResumeJobRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.scheduler.services.v1.SchedulerService/ResumeJob',
        $argument,
        ['\Udb\Core\Scheduler\Services\V1\ResumeJobResponse', 'decode'],
        $metadata, $options);
    }

}
