<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Webrtc\Services\V1;

/**
 */
class RoomServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Create a room
     * @param \Udb\Core\Webrtc\Services\V1\CreateRoomRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\CreateRoomResponse>
     */
    public function CreateRoom(\Udb\Core\Webrtc\Services\V1\CreateRoomRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.RoomService/CreateRoom',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\CreateRoomResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Get a room
     * @param \Udb\Core\Webrtc\Services\V1\GetRoomRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\GetRoomResponse>
     */
    public function GetRoom(\Udb\Core\Webrtc\Services\V1\GetRoomRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.RoomService/GetRoom',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\GetRoomResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Update a room
     * @param \Udb\Core\Webrtc\Services\V1\UpdateRoomRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\UpdateRoomResponse>
     */
    public function UpdateRoom(\Udb\Core\Webrtc\Services\V1\UpdateRoomRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.RoomService/UpdateRoom',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\UpdateRoomResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Close a room
     * @param \Udb\Core\Webrtc\Services\V1\CloseRoomRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\CloseRoomResponse>
     */
    public function CloseRoom(\Udb\Core\Webrtc\Services\V1\CloseRoomRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.RoomService/CloseRoom',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\CloseRoomResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List rooms
     * @param \Udb\Core\Webrtc\Services\V1\ListRoomsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\ListRoomsResponse>
     */
    public function ListRooms(\Udb\Core\Webrtc\Services\V1\ListRoomsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.RoomService/ListRooms',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\ListRoomsResponse', 'decode'],
        $metadata, $options);
    }

}
