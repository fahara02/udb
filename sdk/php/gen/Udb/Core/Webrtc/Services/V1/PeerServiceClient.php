<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Webrtc\Services\V1;

/**
 */
class PeerServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Join a room
     * @param \Udb\Core\Webrtc\Services\V1\JoinRoomRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\JoinRoomResponse>
     */
    public function JoinRoom(\Udb\Core\Webrtc\Services\V1\JoinRoomRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.PeerService/JoinRoom',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\JoinRoomResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Join a room and atomically mint TURN credentials for the freshly-inserted peer
     * @param \Udb\Core\Webrtc\Services\V1\JoinSessionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\JoinSessionResponse>
     */
    public function JoinSession(\Udb\Core\Webrtc\Services\V1\JoinSessionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.PeerService/JoinSession',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\JoinSessionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Leave a room
     * @param \Udb\Core\Webrtc\Services\V1\LeaveRoomRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\LeaveRoomResponse>
     */
    public function LeaveRoom(\Udb\Core\Webrtc\Services\V1\LeaveRoomRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.PeerService/LeaveRoom',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\LeaveRoomResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get a peer
     * @param \Udb\Core\Webrtc\Services\V1\GetPeerRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\GetPeerResponse>
     */
    public function GetPeer(\Udb\Core\Webrtc\Services\V1\GetPeerRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.PeerService/GetPeer',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\GetPeerResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List peers
     * @param \Udb\Core\Webrtc\Services\V1\ListPeersRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\ListPeersResponse>
     */
    public function ListPeers(\Udb\Core\Webrtc\Services\V1\ListPeersRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.PeerService/ListPeers',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\ListPeersResponse', 'decode'],
        $metadata, $options);
    }

}
