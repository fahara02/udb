<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services\WebRtc;

use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Webrtc\Services\V1\IssueCredentialsRequest;
use Udb\Core\Webrtc\Services\V1\IssueCredentialsResponse;
use Udb\Core\Webrtc\Services\V1\TurnServiceClient;

/**
 * Convenience wrapper over the generated {@see TurnServiceClient}
 * (ephemeral TURN/STUN credential issuance). Reach the raw stub via
 * {@see client()}.
 */
final class TurnApi
{
    public function __construct(
        private readonly UdbProject $project,
        private readonly TurnServiceClient $stub,
    ) {
    }

    /** The raw generated stub, for RPCs without a convenience wrapper. */
    public function client(): TurnServiceClient
    {
        return $this->stub;
    }

    /** Issue ephemeral TURN/STUN credentials for a peer in a room. */
    public function issueCredentials(
        string $roomId,
        string $peerId,
        int $ttlSeconds = 0,
        ?UdbMetadata $metadata = null,
    ): IssueCredentialsResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new IssueCredentialsRequest())
            ->setTenantId($meta->tenantId)
            ->setRoomId($roomId)
            ->setPeerId($peerId)
            ->setTtlSeconds($ttlSeconds);

        return $this->project->invoke(
            'IssueCredentials',
            fn (array $md, array $o) => $this->stub->IssueCredentials($request, $md, $o),
            $metadata,
            IssueCredentialsResponse::class,
        );
    }
}
