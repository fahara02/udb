<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Symfony;

use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;

/**
 * Request-scoped holder for the UDB request context under Symfony.
 *
 * Register this as a service (it is stateless across requests until the
 * subscriber populates it). The {@see UdbContextSubscriber} sets the
 * per-request {@see UdbMetadata} on kernel.request and binds it onto the
 * (optional) {@see UdbProject}; controllers / services then read the
 * request-scoped metadata from here.
 *
 * This mirrors the Laravel middleware's "bind once, inherit everywhere"
 * pattern but stays framework-light: it holds no Symfony types, so it
 * `php -l`s and runs even when symfony/http-kernel is absent.
 */
final class UdbContextHolder
{
    private ?UdbMetadata $metadata = null;

    public function __construct(private readonly ?UdbProject $project = null)
    {
    }

    /** Bind the request-scoped metadata, propagating it onto the UdbProject. */
    public function bind(UdbMetadata $metadata): void
    {
        $this->metadata = $metadata;
        $this->project?->bindContext($metadata);
    }

    /** The metadata bound for the current request, or null before kernel.request. */
    public function metadata(): ?UdbMetadata
    {
        return $this->metadata;
    }

    /** The request-scoped {@see UdbProject}, when one was wired in. */
    public function project(): ?UdbProject
    {
        return $this->project;
    }

    /** Clear the bound context (e.g. between pooled worker requests). */
    public function reset(): void
    {
        $this->metadata = null;
    }
}
