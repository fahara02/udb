<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Webrtc\Services\V1;

/**
 */
class TurnServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Issue ephemeral TURN/STUN credentials
     * @param \Udb\Core\Webrtc\Services\V1\IssueCredentialsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\IssueCredentialsResponse>
     */
    public function IssueCredentials(\Udb\Core\Webrtc\Services\V1\IssueCredentialsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.TurnService/IssueCredentials',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\IssueCredentialsResponse', 'decode'],
        $metadata, $options);
    }

}
