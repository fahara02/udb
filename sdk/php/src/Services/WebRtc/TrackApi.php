<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services\WebRtc;

use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Webrtc\Services\V1\ListTracksRequest;
use Udb\Core\Webrtc\Services\V1\ListTracksResponse;
use Udb\Core\Webrtc\Services\V1\MuteTrackRequest;
use Udb\Core\Webrtc\Services\V1\MuteTrackResponse;
use Udb\Core\Webrtc\Services\V1\PublishTrackRequest;
use Udb\Core\Webrtc\Services\V1\PublishTrackResponse;
use Udb\Core\Webrtc\Services\V1\TrackServiceClient;
use Udb\Core\Webrtc\Services\V1\UnpublishTrackRequest;
use Udb\Core\Webrtc\Services\V1\UnpublishTrackResponse;

/**
 * Convenience wrapper over the generated {@see TrackServiceClient}
 * (WebRTC track publish/mute/list). Reach the raw stub via {@see client()}.
 */
final class TrackApi
{
    public function __construct(
        private readonly UdbProject $project,
        private readonly TrackServiceClient $stub,
    ) {
    }

    /** The raw generated stub, for RPCs without a convenience wrapper. */
    public function client(): TrackServiceClient
    {
        return $this->stub;
    }

    /**
     * Publish a track. `$kind` is "audio"/"video"; `$settings` and
     * `$metadataJson` are JSON-encoded track settings / metadata.
     */
    public function publishTrack(
        string $roomId,
        string $peerId,
        string $kind,
        string $label = '',
        string $settings = '',
        string $metadataJson = '',
        ?UdbMetadata $metadata = null,
    ): PublishTrackResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new PublishTrackRequest())
            ->setTenantId($meta->tenantId)
            ->setRoomId($roomId)
            ->setPeerId($peerId)
            ->setKind($kind)
            ->setLabel($label)
            ->setSettings($settings)
            ->setMetadata($metadataJson);

        return $this->project->invoke(
            'PublishTrack',
            fn (array $md, array $o) => $this->stub->PublishTrack($request, $md, $o),
            $metadata,
            PublishTrackResponse::class,
        );
    }

    /** Unpublish a track. */
    public function unpublishTrack(string $trackId, ?UdbMetadata $metadata = null): UnpublishTrackResponse
    {
        $meta = $this->project->metadata($metadata);
        $request = (new UnpublishTrackRequest())
            ->setTenantId($meta->tenantId)
            ->setTrackId($trackId);

        return $this->project->invoke(
            'UnpublishTrack',
            fn (array $md, array $o) => $this->stub->UnpublishTrack($request, $md, $o),
            $metadata,
            UnpublishTrackResponse::class,
        );
    }

    /** Mute or unmute a track. */
    public function muteTrack(string $trackId, bool $muted, ?UdbMetadata $metadata = null): MuteTrackResponse
    {
        $meta = $this->project->metadata($metadata);
        $request = (new MuteTrackRequest())
            ->setTenantId($meta->tenantId)
            ->setTrackId($trackId)
            ->setMuted($muted);

        return $this->project->invoke(
            'MuteTrack',
            fn (array $md, array $o) => $this->stub->MuteTrack($request, $md, $o),
            $metadata,
            MuteTrackResponse::class,
        );
    }

    /** List tracks, optionally filtered by peer / kind within a room. */
    public function listTracks(
        string $roomId,
        string $peerId = '',
        string $kind = '',
        ?UdbMetadata $metadata = null,
    ): ListTracksResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new ListTracksRequest())
            ->setTenantId($meta->tenantId)
            ->setRoomId($roomId)
            ->setPeerId($peerId)
            ->setKind($kind);

        return $this->project->invoke(
            'ListTracks',
            fn (array $md, array $o) => $this->stub->ListTracks($request, $md, $o),
            $metadata,
            ListTracksResponse::class,
        );
    }
}
