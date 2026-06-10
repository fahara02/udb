<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services;

use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Notification\Services\V1\NotificationServiceClient;
use Udb\Core\Notification\Services\V1\SendNotificationRequest;
use Udb\Core\Notification\Services\V1\SendNotificationResponse;

/**
 * Convenience wrapper over the generated {@see NotificationServiceClient}.
 * Shares the project's metadata + deadline; the raw stub remains reachable
 * via {@see client()} for RPCs not wrapped here.
 */
final class NotificationService
{
    public function __construct(
        private readonly UdbProject $project,
        private readonly NotificationServiceClient $stub,
    ) {
    }

    /** The raw generated stub, for RPCs without a convenience wrapper. */
    public function client(): NotificationServiceClient
    {
        return $this->stub;
    }

    /**
     * Send a notification. `channels` are NotificationChannel enum ints
     * (see {@see \Udb\Core\Notification\Entity\V1\NotificationChannel}); an
     * empty list lets the broker resolve channels from preferences/templates.
     *
     * @param  list<int>  $channels
     * @param  array<string,string>  $variables
     */
    public function send(
        string $eventType,
        string $recipientId,
        string $recipientAddress = '',
        array $channels = [],
        array $variables = [],
        ?UdbMetadata $metadata = null,
    ): SendNotificationResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new SendNotificationRequest())
            ->setEventType($eventType)
            ->setRecipientId($recipientId)
            ->setRecipientAddress($recipientAddress)
            ->setTenantId($meta->tenantId)
            ->setProjectId($meta->projectId ?? '')
            ->setCorrelationId($meta->correlationId)
            ->setChannels($channels)
            ->setVariables($variables);

        return $this->project->invoke(
            'SendNotification',
            fn (array $md, array $o) => $this->stub->SendNotification($request, $md, $o),
            $metadata,
            SendNotificationResponse::class,
        );
    }

    public function sendRequest(SendNotificationRequest $request, ?UdbMetadata $metadata = null): SendNotificationResponse
    {
        return $this->project->invoke(
            'SendNotification',
            fn (array $md, array $o) => $this->stub->SendNotification($request, $md, $o),
            $metadata,
            SendNotificationResponse::class,
        );
    }
}
