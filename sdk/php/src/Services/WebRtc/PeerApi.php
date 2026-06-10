<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services\WebRtc;

use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Webrtc\Services\V1\GetPeerRequest;
use Udb\Core\Webrtc\Services\V1\GetPeerResponse;
use Udb\Core\Webrtc\Services\V1\JoinRoomRequest;
use Udb\Core\Webrtc\Services\V1\JoinRoomResponse;
use Udb\Core\Webrtc\Services\V1\LeaveRoomRequest;
use Udb\Core\Webrtc\Services\V1\LeaveRoomResponse;
use Udb\Core\Webrtc\Services\V1\ListPeersRequest;
use Udb\Core\Webrtc\Services\V1\ListPeersResponse;
use Udb\Core\Webrtc\Services\V1\PeerServiceClient;

/**
 * Convenience wrapper over the generated {@see PeerServiceClient}
 * (WebRTC peer join/leave/query). Reach the raw stub via {@see client()}.
 */
final class PeerApi
{
    public function __construct(
        private readonly UdbProject $project,
        private readonly PeerServiceClient $stub,
    ) {
    }

    /** The raw generated stub, for RPCs without a convenience wrapper. */
    public function client(): PeerServiceClient
    {
        return $this->stub;
    }

    /** Join a room. `$metadataJson` is JSON-encoded per-peer metadata. */
    public function joinRoom(
        string $roomId,
        string $displayName = '',
        string $metadataJson = '',
        string $userAgent = '',
        ?UdbMetadata $metadata = null,
    ): JoinRoomResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new JoinRoomRequest())
            ->setTenantId($meta->tenantId)
            ->setRoomId($roomId)
            ->setDisplayName($displayName)
            ->setMetadata($metadataJson)
            ->setUserAgent($userAgent);

        return $this->project->invoke(
            'JoinRoom',
            fn (array $md, array $o) => $this->stub->JoinRoom($request, $md, $o),
            $metadata,
            JoinRoomResponse::class,
        );
    }

    /** Leave a room. */
    public function leaveRoom(string $roomId, string $peerId, ?UdbMetadata $metadata = null): LeaveRoomResponse
    {
        $meta = $this->project->metadata($metadata);
        $request = (new LeaveRoomRequest())
            ->setTenantId($meta->tenantId)
            ->setRoomId($roomId)
            ->setPeerId($peerId);

        return $this->project->invoke(
            'LeaveRoom',
            fn (array $md, array $o) => $this->stub->LeaveRoom($request, $md, $o),
            $metadata,
            LeaveRoomResponse::class,
        );
    }

    /** Get a peer. */
    public function getPeer(string $peerId, ?UdbMetadata $metadata = null): GetPeerResponse
    {
        $meta = $this->project->metadata($metadata);
        $request = (new GetPeerRequest())
            ->setTenantId($meta->tenantId)
            ->setPeerId($peerId);

        return $this->project->invoke(
            'GetPeer',
            fn (array $md, array $o) => $this->stub->GetPeer($request, $md, $o),
            $metadata,
            GetPeerResponse::class,
        );
    }

    /** List peers in a room, optionally filtered by state. */
    public function listPeers(
        string $roomId,
        string $state = '',
        ?UdbMetadata $metadata = null,
    ): ListPeersResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new ListPeersRequest())
            ->setTenantId($meta->tenantId)
            ->setRoomId($roomId)
            ->setState($state);

        return $this->project->invoke(
            'ListPeers',
            fn (array $md, array $o) => $this->stub->ListPeers($request, $md, $o),
            $metadata,
            ListPeersResponse::class,
        );
    }
}
