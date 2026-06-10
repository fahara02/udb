<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services;

use Fahara02\UdbLaravel\Services\WebRtc\PeerApi;
use Fahara02\UdbLaravel\Services\WebRtc\RoomApi;
use Fahara02\UdbLaravel\Services\WebRtc\SignalingApi;
use Fahara02\UdbLaravel\Services\WebRtc\TrackApi;
use Fahara02\UdbLaravel\Services\WebRtc\TurnApi;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Webrtc\Services\V1\PeerServiceClient;
use Udb\Core\Webrtc\Services\V1\RoomServiceClient;
use Udb\Core\Webrtc\Services\V1\SignalingServiceClient;
use Udb\Core\Webrtc\Services\V1\TrackServiceClient;
use Udb\Core\Webrtc\Services\V1\TurnServiceClient;

/**
 * Grouping facade over the five generated WebRTC service clients. The WebRTC
 * surface is split proto-side into Room / Peer / Track / Turn / Signaling
 * services, so this exposes one accessor per group (each its own convenience
 * wrapper). All share the project's metadata + deadline.
 *
 *   $udb->webRtc()->room()->createRoom('standup');
 *   $udb->webRtc()->peer()->joinRoom($roomId, 'Alice');
 *   $udb->webRtc()->track()->publishTrack($roomId, $peerId, 'video');
 *   $udb->webRtc()->turn()->issueCredentials($roomId, $peerId);
 *   $stream = $udb->webRtc()->signaling()->signal(); // raw bidi stream
 */
final class WebRtcService
{
    private ?RoomApi $room = null;
    private ?PeerApi $peer = null;
    private ?TrackApi $track = null;
    private ?TurnApi $turn = null;
    private ?SignalingApi $signaling = null;

    public function __construct(
        private readonly UdbProject $project,
        private readonly RoomServiceClient $roomStub,
        private readonly PeerServiceClient $peerStub,
        private readonly TrackServiceClient $trackStub,
        private readonly TurnServiceClient $turnStub,
        private readonly SignalingServiceClient $signalingStub,
    ) {
    }

    /** Room lifecycle: create / get / update / close / list. */
    public function room(): RoomApi
    {
        return $this->room ??= new RoomApi($this->project, $this->roomStub);
    }

    /** Peer presence: join / leave / get / list. */
    public function peer(): PeerApi
    {
        return $this->peer ??= new PeerApi($this->project, $this->peerStub);
    }

    /** Media tracks: publish / unpublish / mute / list. */
    public function track(): TrackApi
    {
        return $this->track ??= new TrackApi($this->project, $this->trackStub);
    }

    /** TURN/STUN: issue ephemeral credentials. */
    public function turn(): TurnApi
    {
        return $this->turn ??= new TurnApi($this->project, $this->turnStub);
    }

    /** Bidirectional SDP/ICE signaling stream (raw bidi accessor). */
    public function signaling(): SignalingApi
    {
        return $this->signaling ??= new SignalingApi($this->project, $this->signalingStub);
    }
}
