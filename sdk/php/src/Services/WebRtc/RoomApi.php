<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services\WebRtc;

use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Webrtc\Services\V1\CloseRoomRequest;
use Udb\Core\Webrtc\Services\V1\CloseRoomResponse;
use Udb\Core\Webrtc\Services\V1\CreateRoomRequest;
use Udb\Core\Webrtc\Services\V1\CreateRoomResponse;
use Udb\Core\Webrtc\Services\V1\GetRoomRequest;
use Udb\Core\Webrtc\Services\V1\GetRoomResponse;
use Udb\Core\Webrtc\Services\V1\ListRoomsRequest;
use Udb\Core\Webrtc\Services\V1\ListRoomsResponse;
use Udb\Core\Webrtc\Services\V1\RoomServiceClient;
use Udb\Core\Webrtc\Services\V1\UpdateRoomRequest;
use Udb\Core\Webrtc\Services\V1\UpdateRoomResponse;

/**
 * Convenience wrapper over the generated {@see RoomServiceClient}
 * (WebRTC room lifecycle). Reach the raw stub via {@see client()}.
 */
final class RoomApi
{
    public function __construct(
        private readonly UdbProject $project,
        private readonly RoomServiceClient $stub,
    ) {
    }

    /** The raw generated stub, for RPCs without a convenience wrapper. */
    public function client(): RoomServiceClient
    {
        return $this->stub;
    }

    /** Create a room. `$config` is JSON-encoded room config. */
    public function createRoom(
        string $name,
        int $maxParticipants = 0,
        string $config = '',
        string $createdBy = '',
        ?UdbMetadata $metadata = null,
    ): CreateRoomResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new CreateRoomRequest())
            ->setTenantId($meta->tenantId)
            ->setName($name)
            ->setMaxParticipants($maxParticipants)
            ->setConfig($config)
            ->setCreatedBy($createdBy !== '' ? $createdBy : $meta->userId);

        return $this->project->invoke(
            'CreateRoom',
            fn (array $md, array $o) => $this->stub->CreateRoom($request, $md, $o),
            $metadata,
            CreateRoomResponse::class,
        );
    }

    /** Get a room. */
    public function getRoom(string $roomId, ?UdbMetadata $metadata = null): GetRoomResponse
    {
        $meta = $this->project->metadata($metadata);
        $request = (new GetRoomRequest())
            ->setTenantId($meta->tenantId)
            ->setRoomId($roomId);

        return $this->project->invoke(
            'GetRoom',
            fn (array $md, array $o) => $this->stub->GetRoom($request, $md, $o),
            $metadata,
            GetRoomResponse::class,
        );
    }

    /** Update a room's mutable fields. `$state`/`$config` are broker strings. */
    public function updateRoom(
        string $roomId,
        string $name = '',
        string $state = '',
        string $config = '',
        ?UdbMetadata $metadata = null,
    ): UpdateRoomResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new UpdateRoomRequest())
            ->setTenantId($meta->tenantId)
            ->setRoomId($roomId)
            ->setName($name)
            ->setState($state)
            ->setConfig($config);

        return $this->project->invoke(
            'UpdateRoom',
            fn (array $md, array $o) => $this->stub->UpdateRoom($request, $md, $o),
            $metadata,
            UpdateRoomResponse::class,
        );
    }

    /** Close a room. */
    public function closeRoom(string $roomId, ?UdbMetadata $metadata = null): CloseRoomResponse
    {
        $meta = $this->project->metadata($metadata);
        $request = (new CloseRoomRequest())
            ->setTenantId($meta->tenantId)
            ->setRoomId($roomId);

        return $this->project->invoke(
            'CloseRoom',
            fn (array $md, array $o) => $this->stub->CloseRoom($request, $md, $o),
            $metadata,
            CloseRoomResponse::class,
        );
    }

    /** List rooms, optionally filtered by state, with pagination. */
    public function listRooms(
        string $state = '',
        int $page = 0,
        int $pageSize = 0,
        ?UdbMetadata $metadata = null,
    ): ListRoomsResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new ListRoomsRequest())
            ->setTenantId($meta->tenantId)
            ->setState($state)
            ->setPage($page)
            ->setPageSize($pageSize);

        return $this->project->invoke(
            'ListRooms',
            fn (array $md, array $o) => $this->stub->ListRooms($request, $md, $o),
            $metadata,
            ListRoomsResponse::class,
        );
    }
}
