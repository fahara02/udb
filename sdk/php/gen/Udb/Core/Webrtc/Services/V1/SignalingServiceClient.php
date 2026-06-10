<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Webrtc\Services\V1;

/**
 */
class SignalingServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Bidirectional signaling channel for SDP offer/answer and ICE exchange.
     * Streaming RPC: no google.api.http (REST mapping is not supported for
     * streaming methods) and no rest_contract.
     * Named `Signal` (not `Connect`) because tonic generates a client `connect`
     * associated constructor; an RPC named `Connect` collides with it.
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\BidiStreamingCall
     */
    public function Signal($metadata = [], $options = []) {
        return $this->_bidiRequest('/udb.core.webrtc.services.v1.SignalingService/Signal',
        ['\Udb\Core\Webrtc\Services\V1\SignalResponse','decode'],
        $metadata, $options);
    }

}
