<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Symfony;

use Fahara02\UdbLaravel\UdbMetadata;
use Symfony\Component\EventDispatcher\EventSubscriberInterface;
use Symfony\Component\HttpKernel\Event\RequestEvent;
use Symfony\Component\HttpKernel\Event\ResponseEvent;
use Symfony\Component\HttpKernel\KernelEvents;

/**
 * Symfony adapter (M5.6) — the framework-native counterpart to the Laravel
 * {@see \Fahara02\UdbLaravel\Http\UdbContextMiddleware}.
 *
 * Subscribes to the HttpKernel `kernel.request` event to extract the request
 * context (tenant / user / correlation id / request id) from headers, build a
 * request-scoped {@see UdbMetadata}, and bind it onto the shared
 * {@see UdbContextHolder} (and, through it, the request-scoped UdbProject) so
 * every UDB RPC issued during the request inherits it automatically. On
 * `kernel.response` it echoes the correlation id back as a response header for
 * cross-service tracing — exactly like the Laravel middleware.
 *
 * symfony/http-kernel is an OPTIONAL (dev) dependency: this file references
 * Symfony types only by name, so it `php -l`s cleanly with the package absent.
 * It is only autoloaded/instantiated by an app that actually runs Symfony.
 *
 * Wire it as a tagged `kernel.event_subscriber` service, e.g. in services.yaml:
 *
 *   Fahara02\UdbLaravel\Symfony\UdbContextHolder:
 *       arguments: ['@Fahara02\UdbLaravel\UdbProject']
 *   Fahara02\UdbLaravel\Symfony\UdbContextSubscriber:
 *       arguments: ['@Fahara02\UdbLaravel\Symfony\UdbContextHolder']
 *       tags: ['kernel.event_subscriber']
 *
 * Resolution strategy per field (first non-empty wins):
 *   tenant   : X-Tenant-Id header
 *   user     : X-User-Id header
 *   request  : X-Request-Id header  (carried on the metadata correlation chain)
 *   correlation: X-Correlation-Id header, else X-Request-Id, else a fresh UUIDv4
 */
final class UdbContextSubscriber implements EventSubscriberInterface
{
    private string $correlationId = '';

    public function __construct(
        private readonly UdbContextHolder $holder,
        private readonly string $defaultPurpose = 'web.request',
        private readonly string $serviceIdentity = 'symfony.app',
        private readonly string $defaultProjectId = 'default',
        private readonly string $clientCatalogVersion = '1.0.0',
    ) {
    }

    /**
     * @return array<string, array{0: string, 1: int}>
     */
    public static function getSubscribedEvents(): array
    {
        // Bind context as early as possible (high priority) on the request, and
        // stamp the response late (low priority) so a downstream-set header wins.
        return [
            KernelEvents::REQUEST => ['onKernelRequest', 4096],
            KernelEvents::RESPONSE => ['onKernelResponse', -4096],
        ];
    }

    public function onKernelRequest(RequestEvent $event): void
    {
        if (! $event->isMainRequest()) {
            return; // sub-requests inherit the main request's bound context
        }

        $request = $event->getRequest();
        $headers = $request->headers;

        $tenantId = (string) ($headers->get('X-Tenant-Id') ?? '');
        $userId = (string) ($headers->get('X-User-Id') ?? '');
        $requestId = (string) ($headers->get('X-Request-Id') ?? '');
        $correlationId = (string) ($headers->get('X-Correlation-Id') ?? '');
        if ($correlationId === '') {
            $correlationId = $requestId !== '' ? $requestId : self::generateCorrelationId();
        }
        $this->correlationId = $correlationId;

        $metadata = new UdbMetadata(
            tenantId: $tenantId,
            userId: $userId,
            purpose: $this->defaultPurpose,
            correlationId: $correlationId,
            scopes: [],
            serviceIdentity: $this->serviceIdentity,
            projectId: $this->defaultProjectId,
            clientCatalogVersion: $this->clientCatalogVersion,
            bearerToken: $this->bearerToken((string) ($headers->get('Authorization') ?? '')),
            apiKey: (string) ($headers->get('X-Api-Key') ?? ''),
        );
        $this->holder->bind($metadata);
    }

    public function onKernelResponse(ResponseEvent $event): void
    {
        if (! $event->isMainRequest() || $this->correlationId === '') {
            return;
        }
        $response = $event->getResponse();
        if (! $response->headers->has('X-Correlation-Id')) {
            $response->headers->set('X-Correlation-Id', $this->correlationId);
        }
    }

    /** RFC 4122 v4 — identical shape to the other UDB SDKs' correlation ids. */
    private static function generateCorrelationId(): string
    {
        $bytes = random_bytes(16);
        $bytes[6] = chr((ord($bytes[6]) & 0x0f) | 0x40);
        $bytes[8] = chr((ord($bytes[8]) & 0x3f) | 0x80);

        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bytes), 4));
    }

    private function bearerToken(string $authorization): string
    {
        $authorization = trim($authorization);
        return strncasecmp($authorization, 'Bearer ', 7) === 0
            ? trim(substr($authorization, 7))
            : '';
    }
}
