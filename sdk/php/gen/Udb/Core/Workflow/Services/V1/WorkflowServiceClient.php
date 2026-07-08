<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Workflow\Services\V1;

/**
 * WorkflowService (master-plan 9.12) — durable multi-step operations with
 * compensation, exposed as a first-class native service. A workflow is a durable,
 * tenant-scoped instance handed to the EXISTING saga engine (`runtime::saga`):
 * forward progress is driven by the leader-elected workflow tick
 * (`FOR UPDATE SKIP LOCKED`, one advancer cluster-wide, fires transition events
 * only), and cancellation reuses the established saga compensation (reverse-order)
 * machinery rather than reimplementing it. Every state transition emits one
 * versioned `udb.workflow.<state>.v1` outbox event.
 */
class WorkflowServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Start a durable workflow instance and hand it to the saga engine. The instance
     * is persisted before any forward step runs, so it survives a restart.
     * @param \Udb\Core\Workflow\Services\V1\StartWorkflowRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Workflow\Services\V1\StartWorkflowResponse>
     */
    public function StartWorkflow(\Udb\Core\Workflow\Services\V1\StartWorkflowRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.workflow.services.v1.WorkflowService/StartWorkflow',
        $argument,
        ['\Udb\Core\Workflow\Services\V1\StartWorkflowResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Fetch a single workflow instance by id (tenant-scoped).
     * @param \Udb\Core\Workflow\Services\V1\GetWorkflowRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Workflow\Services\V1\GetWorkflowResponse>
     */
    public function GetWorkflow(\Udb\Core\Workflow\Services\V1\GetWorkflowRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.workflow.services.v1.WorkflowService/GetWorkflow',
        $argument,
        ['\Udb\Core\Workflow\Services\V1\GetWorkflowResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List workflow instances for the verified tenant, optionally filtered by status.
     * @param \Udb\Core\Workflow\Services\V1\ListWorkflowsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Workflow\Services\V1\ListWorkflowsResponse>
     */
    public function ListWorkflows(\Udb\Core\Workflow\Services\V1\ListWorkflowsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.workflow.services.v1.WorkflowService/ListWorkflows',
        $argument,
        ['\Udb\Core\Workflow\Services\V1\ListWorkflowsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Cancel a workflow and trigger the saga compensation path (reverse-order). The
     * instance moves to COMPENSATING and the EXISTING recovery worker undoes the
     * recorded side effects — this RPC never reimplements compensation.
     * @param \Udb\Core\Workflow\Services\V1\CancelWorkflowRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Workflow\Services\V1\CancelWorkflowResponse>
     */
    public function CancelWorkflow(\Udb\Core\Workflow\Services\V1\CancelWorkflowRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.workflow.services.v1.WorkflowService/CancelWorkflow',
        $argument,
        ['\Udb\Core\Workflow\Services\V1\CancelWorkflowResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Deliver an external signal to a waiting workflow step, resuming forward
     * progress (the durable equivalent of completing a blocked step).
     * @param \Udb\Core\Workflow\Services\V1\SignalWorkflowRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Workflow\Services\V1\SignalWorkflowResponse>
     */
    public function SignalWorkflow(\Udb\Core\Workflow\Services\V1\SignalWorkflowRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.workflow.services.v1.WorkflowService/SignalWorkflow',
        $argument,
        ['\Udb\Core\Workflow\Services\V1\SignalWorkflowResponse', 'decode'],
        $metadata, $options);
    }

}
