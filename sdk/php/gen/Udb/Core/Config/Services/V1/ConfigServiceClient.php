<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Config\Services\V1;

/**
 * ConfigService (master-plan 9.8) — feature flags and runtime configuration.
 * Flags are scoped to (tenant, project, environment); evaluation precedence is
 * environment > project > tenant-default. EvaluateFlags is a PURE function of
 * (flags, context) — the same algorithm ships in the SDK so client and server
 * agree bit-for-bit (the unit-test fixtures are the SDK<->server contract).
 * Every mutation is durable, tenant-scoped by the verified claim, bumps a
 * monotone per-row revision, and emits `udb.config.flag.changed.v1`.
 */
class ConfigServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Create or update a flag at a (tenant, project, environment) scope. Bumps the
     * flag's monotone revision and emits `udb.config.flag.changed.v1`.
     * @param \Udb\Core\Config\Services\V1\PutFlagRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Config\Services\V1\PutFlagResponse>
     */
    public function PutFlag(\Udb\Core\Config\Services\V1\PutFlagRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.config.services.v1.ConfigService/PutFlag',
        $argument,
        ['\Udb\Core\Config\Services\V1\PutFlagResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Fetch a single flag's stored definition at an exact (tenant, project,
     * environment, key) scope. Read-only; performs no rollout evaluation.
     * @param \Udb\Core\Config\Services\V1\GetFlagRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Config\Services\V1\GetFlagResponse>
     */
    public function GetFlag(\Udb\Core\Config\Services\V1\GetFlagRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.config.services.v1.ConfigService/GetFlag',
        $argument,
        ['\Udb\Core\Config\Services\V1\GetFlagResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List a tenant's flags, optionally narrowed to a project and/or environment.
     * @param \Udb\Core\Config\Services\V1\ListFlagsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Config\Services\V1\ListFlagsResponse>
     */
    public function ListFlags(\Udb\Core\Config\Services\V1\ListFlagsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.config.services.v1.ConfigService/ListFlags',
        $argument,
        ['\Udb\Core\Config\Services\V1\ListFlagsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Delete a flag at an exact scope. Destructive; bumps the revision in the
     * emitted change event.
     * @param \Udb\Core\Config\Services\V1\DeleteFlagRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Config\Services\V1\DeleteFlagResponse>
     */
    public function DeleteFlag(\Udb\Core\Config\Services\V1\DeleteFlagRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.config.services.v1.ConfigService/DeleteFlag',
        $argument,
        ['\Udb\Core\Config\Services\V1\DeleteFlagResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Evaluate a set of flag keys for an evaluation context. The server applies the
     * SAME pure algorithm the SDK uses (scope precedence + stable-hash percentage
     * rollout) and returns the resolved typed values plus a server-authoritative
     * cache TTL and the observed config revision. Read-only.
     * @param \Udb\Core\Config\Services\V1\EvaluateFlagsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Config\Services\V1\EvaluateFlagsResponse>
     */
    public function EvaluateFlags(\Udb\Core\Config\Services\V1\EvaluateFlagsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.config.services.v1.ConfigService/EvaluateFlags',
        $argument,
        ['\Udb\Core\Config\Services\V1\EvaluateFlagsResponse', 'decode'],
        $metadata, $options);
    }

}
