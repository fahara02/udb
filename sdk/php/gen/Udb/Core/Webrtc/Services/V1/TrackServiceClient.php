<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Udb\Core\Webrtc\Services\V1;

/**
 */
class TrackServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Publish a track
     * @param \Udb\Core\Webrtc\Services\V1\PublishTrackRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\PublishTrackResponse>
     */
    public function PublishTrack(\Udb\Core\Webrtc\Services\V1\PublishTrackRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.TrackService/PublishTrack',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\PublishTrackResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Unpublish a track
     * @param \Udb\Core\Webrtc\Services\V1\UnpublishTrackRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\UnpublishTrackResponse>
     */
    public function UnpublishTrack(\Udb\Core\Webrtc\Services\V1\UnpublishTrackRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.TrackService/UnpublishTrack',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\UnpublishTrackResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Mute or unmute a track
     * @param \Udb\Core\Webrtc\Services\V1\MuteTrackRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\MuteTrackResponse>
     */
    public function MuteTrack(\Udb\Core\Webrtc\Services\V1\MuteTrackRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.TrackService/MuteTrack',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\MuteTrackResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * List tracks
     * @param \Udb\Core\Webrtc\Services\V1\ListTracksRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Udb\Core\Webrtc\Services\V1\ListTracksResponse>
     */
    public function ListTracks(\Udb\Core\Webrtc\Services\V1\ListTracksRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/udb.core.webrtc.services.v1.TrackService/ListTracks',
        $argument,
        ['\Udb\Core\Webrtc\Services\V1\ListTracksResponse', 'decode'],
        $metadata, $options);
    }

}
