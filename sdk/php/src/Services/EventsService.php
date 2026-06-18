<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services;

use Fahara02\UdbLaravel\UdbClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Entity\V1\CDCSubscriptionRequest;
use Udb\Entity\V1\EnqueueOutboxEventRequest;
use Udb\Entity\V1\EnqueueOutboxEventResponse;

/**
 * `project->events()` — CDC subscribe + publish-and-wait over the DataBroker
 * `PublishCDC` (server-stream) and `EnqueueOutboxEvent` RPCs. The PHP analogue
 * of the TS `events.subscribe(topic).ready()` / `events.publishAndWait(...)`
 * and the Python `_EventsFacade`.
 *
 * Naming-contract surface:
 *   $events->subscribe($topic)->ready();          // open stream + block to readiness
 *   $env = $events->publishAndWait($topic, $payload);  // enqueue then await match
 *
 * Sync-model note: PHP's ext-grpc is fully synchronous — a `ServerStreamingCall`
 * has no async/event-loop readiness signal. {@see EventSubscription::ready()}
 * therefore uses the server's INITIAL METADATA as the readiness boundary
 * (`ServerStreamingCall::getMetadata()`, which blocks until the server has
 * accepted the stream) — server-driven, never a `sleep()`. This matches
 * Python's `initial_metadata()` boundary. The full `subscribe(topic)->ready()`
 * shape IS supported; it is simply blocking rather than promise-based.
 *
 * Stream opening is injectable ({@see withStreamOpener()}) so the facade can be
 * unit-tested without ext-grpc / a live broker.
 */
final class EventsService
{
    /** @var null|callable(CDCSubscriptionRequest, array<string,list<string>>):object */
    private $streamOpener = null;

    public function __construct(
        private readonly UdbProject $project,
        private readonly UdbClient $data,
    ) {
    }

    /**
     * Override how the `PublishCDC` server-stream is opened. The opener receives
     * the built {@see CDCSubscriptionRequest} + the gRPC metadata headers and
     * must return a stream object exposing `responses()` / `getMetadata()`
     * (the `\Grpc\ServerStreamingCall` contract). Test-only seam — production
     * defaults to the generated DataBroker stub.
     *
     * @param  callable(CDCSubscriptionRequest, array<string,list<string>>):object  $opener
     */
    public function withStreamOpener(callable $opener): self
    {
        $this->streamOpener = $opener;

        return $this;
    }

    /**
     * Open a tenant-scoped `PublishCDC` stream for `$topicPattern` and return a
     * subscription handle. Call `->ready()` to block until the server has
     * accepted the stream; iterate it for `CDCEnvelope`s.
     */
    public function subscribe(
        string $topicPattern,
        string $sinceEventId = '',
        ?UdbMetadata $metadata = null,
    ): EventSubscription {
        $meta = $this->project->metadata($metadata);
        $request = (new CDCSubscriptionRequest())
            ->setContext($meta->toRequestContext())
            ->setTopicPattern($topicPattern)
            ->setSinceEventId($sinceEventId);
        $headers = $meta->toGrpcMetadata();

        $opener = $this->streamOpener ?? function (CDCSubscriptionRequest $req, array $md): object {
            return $this->data->stub()->PublishCDC($req, $md);
        };

        return new EventSubscription($opener($request, $headers));
    }

    /**
     * Enqueue one event, then read the subscription until `$match` matches —
     * the PHP analogue of TS `publishAndWait` / Python `publish_and_wait`.
     *
     * Issues EXACTLY one `EnqueueOutboxEvent` (a unary mutation, routed through
     * the shared {@see UdbProject::invoke()} so metadata + deadline are
     * consistent); then iterates `$subscription` (or a freshly opened one) for
     * the matching `CDCEnvelope`. `$match` is `callable(object):bool`; the
     * default matches the enqueued `event_id`. No `sleep()` — purely
     * stream-driven. Returns the matching envelope, or null if the stream ends
     * first.
     *
     * @param  null|callable(object):bool  $match
     */
    public function publishAndWait(
        string $topic,
        string $payload,
        ?callable $match = null,
        string $partitionKey = '',
        string $schemaUri = '',
        string $idempotencyKey = '',
        ?EventSubscription $subscription = null,
        ?UdbMetadata $metadata = null,
    ): ?object {
        $sub = $subscription ?? $this->subscribe($topic, '', $metadata);
        $enqueued = $this->enqueue(
            $topic,
            $payload,
            $partitionKey,
            $schemaUri,
            $idempotencyKey,
            $metadata,
        );

        if ($match === null) {
            $target = $enqueued->getEventId();
            $match = static function (object $envelope) use ($target): bool {
                return method_exists($envelope, 'getEventId')
                    && $envelope->getEventId() === $target;
            };
        }

        foreach ($sub as $envelope) {
            if ($match($envelope)) {
                return $envelope;
            }
        }

        return null;
    }

    /**
     * Enqueue a single outbox event (one `EnqueueOutboxEvent` RPC). Exposed
     * directly for fire-and-forget publishes that don't need to await delivery.
     */
    public function enqueue(
        string $topic,
        string $payload,
        string $partitionKey = '',
        string $schemaUri = '',
        string $idempotencyKey = '',
        ?UdbMetadata $metadata = null,
    ): EnqueueOutboxEventResponse {
        $meta = $this->project->metadata($metadata);
        $request = (new EnqueueOutboxEventRequest())
            ->setContext($meta->toRequestContext())
            ->setTopic($topic)
            ->setPartitionKey($partitionKey)
            ->setPayload($payload)
            ->setSchemaUri($schemaUri)
            ->setIdempotencyKey($idempotencyKey);

        return $this->project->invoke(
            'EnqueueOutboxEvent',
            fn (array $md, array $o) => $this->data->stub()->EnqueueOutboxEvent($request, $md, $o),
            $metadata,
            EnqueueOutboxEventResponse::class,
        );
    }
}
