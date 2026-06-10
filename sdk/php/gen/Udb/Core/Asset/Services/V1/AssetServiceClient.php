<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Asset\Services\V1;

/**
 */
class AssetServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Create a reusable pipeline definition
     * @param \Udb\Core\Asset\Services\V1\CreatePipelineDefinitionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Asset\Services\V1\CreatePipelineDefinitionResponse>
     */
    public function CreatePipelineDefinition(\Udb\Core\Asset\Services\V1\CreatePipelineDefinitionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.asset.services.v1.AssetService/CreatePipelineDefinition',
        $argument,
        ['\Udb\Core\Asset\Services\V1\CreatePipelineDefinitionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get a pipeline definition
     * @param \Udb\Core\Asset\Services\V1\GetPipelineDefinitionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Asset\Services\V1\GetPipelineDefinitionResponse>
     */
    public function GetPipelineDefinition(\Udb\Core\Asset\Services\V1\GetPipelineDefinitionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.asset.services.v1.AssetService/GetPipelineDefinition',
        $argument,
        ['\Udb\Core\Asset\Services\V1\GetPipelineDefinitionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Register a managed asset wrapping a storage file
     * @param \Udb\Core\Asset\Services\V1\RegisterAssetRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Asset\Services\V1\RegisterAssetResponse>
     */
    public function RegisterAsset(\Udb\Core\Asset\Services\V1\RegisterAssetRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.asset.services.v1.AssetService/RegisterAsset',
        $argument,
        ['\Udb\Core\Asset\Services\V1\RegisterAssetResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Start a pipeline instance for an asset
     * @param \Udb\Core\Asset\Services\V1\StartPipelineRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Asset\Services\V1\StartPipelineResponse>
     */
    public function StartPipeline(\Udb\Core\Asset\Services\V1\StartPipelineRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.asset.services.v1.AssetService/StartPipeline',
        $argument,
        ['\Udb\Core\Asset\Services\V1\StartPipelineResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get a pipeline instance with its steps
     * @param \Udb\Core\Asset\Services\V1\GetPipelineRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Asset\Services\V1\GetPipelineResponse>
     */
    public function GetPipeline(\Udb\Core\Asset\Services\V1\GetPipelineRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.asset.services.v1.AssetService/GetPipeline',
        $argument,
        ['\Udb\Core\Asset\Services\V1\GetPipelineResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Complete (or skip/fail) a pipeline step
     * @param \Udb\Core\Asset\Services\V1\CompleteStepRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Asset\Services\V1\CompleteStepResponse>
     */
    public function CompleteStep(\Udb\Core\Asset\Services\V1\CompleteStepRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.asset.services.v1.AssetService/CompleteStep',
        $argument,
        ['\Udb\Core\Asset\Services\V1\CompleteStepResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List assets
     * @param \Udb\Core\Asset\Services\V1\ListAssetsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Asset\Services\V1\ListAssetsResponse>
     */
    public function ListAssets(\Udb\Core\Asset\Services\V1\ListAssetsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.asset.services.v1.AssetService/ListAssets',
        $argument,
        ['\Udb\Core\Asset\Services\V1\ListAssetsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get an asset
     * @param \Udb\Core\Asset\Services\V1\GetAssetRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Asset\Services\V1\GetAssetResponse>
     */
    public function GetAsset(\Udb\Core\Asset\Services\V1\GetAssetRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.asset.services.v1.AssetService/GetAsset',
        $argument,
        ['\Udb\Core\Asset\Services\V1\GetAssetResponse', 'decode'],
        $metadata, $options);
    }

}
