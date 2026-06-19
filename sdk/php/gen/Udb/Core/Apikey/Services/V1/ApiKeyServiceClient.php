<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Apikey\Services\V1;

/**
 * ---------------------------------------------------------------------------
 * ApiKeyService — Machine-to-machine key lifecycle and validation.
 *
 * HTTP prefix: /v1/api-keys
 * URL conventions: kebab-case paths, :lowerCamel custom method suffix, kebab-case query params.
 *
 * The gateway calls ValidateApiKey on every inbound API request to:
 *   1. Verify key hash
 *   2. Check scope grants
 *   3. Enforce IP allowlist
 *   4. Enforce rate limits (increment usage counter)
 * ---------------------------------------------------------------------------
 */
class ApiKeyServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * ── Key lifecycle (admin-only) ────────────────────────────────────────────
     * Returns the plain key ONCE in CreateApiKeyResponse — never again.
     * @param \Udb\Core\Apikey\Services\V1\CreateApiKeyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Apikey\Services\V1\CreateApiKeyResponse>
     */
    public function CreateApiKey(\Udb\Core\Apikey\Services\V1\CreateApiKeyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.apikey.services.v1.ApiKeyService/CreateApiKey',
        $argument,
        ['\Udb\Core\Apikey\Services\V1\CreateApiKeyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Apikey\Services\V1\GetApiKeyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Apikey\Services\V1\GetApiKeyResponse>
     */
    public function GetApiKey(\Udb\Core\Apikey\Services\V1\GetApiKeyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.apikey.services.v1.ApiKeyService/GetApiKey',
        $argument,
        ['\Udb\Core\Apikey\Services\V1\GetApiKeyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Apikey\Services\V1\ListApiKeysRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Apikey\Services\V1\ListApiKeysResponse>
     */
    public function ListApiKeys(\Udb\Core\Apikey\Services\V1\ListApiKeysRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.apikey.services.v1.ApiKeyService/ListApiKeys',
        $argument,
        ['\Udb\Core\Apikey\Services\V1\ListApiKeysResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Apikey\Services\V1\UpdateApiKeyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Apikey\Services\V1\UpdateApiKeyResponse>
     */
    public function UpdateApiKey(\Udb\Core\Apikey\Services\V1\UpdateApiKeyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.apikey.services.v1.ApiKeyService/UpdateApiKey',
        $argument,
        ['\Udb\Core\Apikey\Services\V1\UpdateApiKeyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Udb\Core\Apikey\Services\V1\RevokeApiKeyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Apikey\Services\V1\RevokeApiKeyResponse>
     */
    public function RevokeApiKey(\Udb\Core\Apikey\Services\V1\RevokeApiKeyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.apikey.services.v1.ApiKeyService/RevokeApiKey',
        $argument,
        ['\Udb\Core\Apikey\Services\V1\RevokeApiKeyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Rotate a key's secret in place (same key_id + lineage). Returns the new
     * plain key ONCE; the old secret is invalidated immediately.
     * @param \Udb\Core\Apikey\Services\V1\RotateApiKeyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Apikey\Services\V1\RotateApiKeyResponse>
     */
    public function RotateApiKey(\Udb\Core\Apikey\Services\V1\RotateApiKeyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.apikey.services.v1.ApiKeyService/RotateApiKey',
        $argument,
        ['\Udb\Core\Apikey\Services\V1\RotateApiKeyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Emergency bulk revoke by selector (prefix/owner/tenant/project/scope/before).
     * @param \Udb\Core\Apikey\Services\V1\EmergencyRevokeApiKeysRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Apikey\Services\V1\EmergencyRevokeApiKeysResponse>
     */
    public function EmergencyRevokeApiKeys(\Udb\Core\Apikey\Services\V1\EmergencyRevokeApiKeysRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.apikey.services.v1.ApiKeyService/EmergencyRevokeApiKeys',
        $argument,
        ['\Udb\Core\Apikey\Services\V1\EmergencyRevokeApiKeysResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Validation (called by API gateway — internal, not public HTTP) ────────
     * @param \Udb\Core\Apikey\Services\V1\ValidateApiKeyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Apikey\Services\V1\ValidateApiKeyResponse>
     */
    public function ValidateApiKey(\Udb\Core\Apikey\Services\V1\ValidateApiKeyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.apikey.services.v1.ApiKeyService/ValidateApiKey',
        $argument,
        ['\Udb\Core\Apikey\Services\V1\ValidateApiKeyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * ── Usage stats ───────────────────────────────────────────────────────────
     * @param \Udb\Core\Apikey\Services\V1\GetApiKeyUsageStatsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Apikey\Services\V1\GetApiKeyUsageStatsResponse>
     */
    public function GetApiKeyUsageStats(\Udb\Core\Apikey\Services\V1\GetApiKeyUsageStatsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.apikey.services.v1.ApiKeyService/GetApiKeyUsageStats',
        $argument,
        ['\Udb\Core\Apikey\Services\V1\GetApiKeyUsageStatsResponse', 'decode'],
        $metadata, $options);
    }

}
