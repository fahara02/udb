<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Webhook\Services\V1;

/**
 * WebhookService (master-plan 9.4) — delivers tenant-scoped domain events to the
 * outside world. A tenant registers an external HTTPS endpoint with a topic
 * subscription; the leader-elected delivery worker consumes the tenant-bound CDC
 * stream, signs each event body with the per-endpoint secret (HMAC-SHA256 →
 * `X-Udb-Signature`), POSTs it with retries/backoff, dead-letters after
 * `max_attempts`, and journals every delivery. Every external target is run
 * through an SSRF guard at registration AND again at delivery (DNS rebinding):
 * https-only, never a private/loopback/link-local/CGNAT host.
 */
class WebhookServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Register an external webhook endpoint. The target URL is SSRF-validated
     * (https-only, no private/loopback/link-local/CGNAT host). The per-endpoint
     * signing secret is returned exactly once in the response and never again.
     * @param \Udb\Core\Webhook\Services\V1\CreateEndpointRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webhook\Services\V1\CreateEndpointResponse>
     */
    public function CreateEndpoint(\Udb\Core\Webhook\Services\V1\CreateEndpointRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webhook.services.v1.WebhookService/CreateEndpoint',
        $argument,
        ['\Udb\Core\Webhook\Services\V1\CreateEndpointResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Fetch one webhook endpoint (the signing secret is NEVER returned on read).
     * @param \Udb\Core\Webhook\Services\V1\GetEndpointRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webhook\Services\V1\GetEndpointResponse>
     */
    public function GetEndpoint(\Udb\Core\Webhook\Services\V1\GetEndpointRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webhook.services.v1.WebhookService/GetEndpoint',
        $argument,
        ['\Udb\Core\Webhook\Services\V1\GetEndpointResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List a tenant's webhook endpoints (signing secrets are NEVER returned).
     * @param \Udb\Core\Webhook\Services\V1\ListEndpointsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webhook\Services\V1\ListEndpointsResponse>
     */
    public function ListEndpoints(\Udb\Core\Webhook\Services\V1\ListEndpointsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webhook.services.v1.WebhookService/ListEndpoints',
        $argument,
        ['\Udb\Core\Webhook\Services\V1\ListEndpointsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Update an endpoint. A changed URL is SSRF-revalidated before it is stored.
     * @param \Udb\Core\Webhook\Services\V1\UpdateEndpointRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webhook\Services\V1\UpdateEndpointResponse>
     */
    public function UpdateEndpoint(\Udb\Core\Webhook\Services\V1\UpdateEndpointRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webhook.services.v1.WebhookService/UpdateEndpoint',
        $argument,
        ['\Udb\Core\Webhook\Services\V1\UpdateEndpointResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Delete (soft) a webhook endpoint; no further events are delivered to it.
     * @param \Udb\Core\Webhook\Services\V1\DeleteEndpointRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webhook\Services\V1\DeleteEndpointResponse>
     */
    public function DeleteEndpoint(\Udb\Core\Webhook\Services\V1\DeleteEndpointRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webhook.services.v1.WebhookService/DeleteEndpoint',
        $argument,
        ['\Udb\Core\Webhook\Services\V1\DeleteEndpointResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List the delivery journal for a tenant, optionally narrowed to one endpoint
     * or one delivery status.
     * @param \Udb\Core\Webhook\Services\V1\ListDeliveriesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webhook\Services\V1\ListDeliveriesResponse>
     */
    public function ListDeliveries(\Udb\Core\Webhook\Services\V1\ListDeliveriesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webhook.services.v1.WebhookService/ListDeliveries',
        $argument,
        ['\Udb\Core\Webhook\Services\V1\ListDeliveriesResponse', 'decode'],
        $metadata, $options);
    }

}
