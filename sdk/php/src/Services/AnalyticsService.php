<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Services;

use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Analytics\Services\V1\AnalyticsServiceClient;

/**
 * Thin handle over the generated {@see AnalyticsServiceClient}. The analytics
 * RPCs (RecordPipelineMetric, GetThroughput, GetSlaCompliance, …) are read/
 * report-shaped with no obvious single convenience verb, so this wrapper just
 * exposes the raw stub plus the shared project metadata for callers that want
 * to drive it directly. Add convenience methods here as usage patterns settle.
 */
final class AnalyticsService
{
    public function __construct(
        private readonly UdbProject $project,
        private readonly AnalyticsServiceClient $stub,
    ) {
    }

    /** The raw generated stub. */
    public function client(): AnalyticsServiceClient
    {
        return $this->stub;
    }

    /** The owning project, for shared metadata / invoke helpers. */
    public function project(): UdbProject
    {
        return $this->project;
    }
}
