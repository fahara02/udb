<?php

declare(strict_types=1);

namespace Fahara02\UdbLaravel\Facades;

use Fahara02\UdbLaravel\UdbClient;
use Illuminate\Support\Facades\Facade;
use Udb\Entity\V1\DeleteRequest;
use Udb\Entity\V1\MutationResponse;
use Udb\Entity\V1\RecordSet;
use Udb\Entity\V1\SelectRequest;
use Udb\Entity\V1\UpsertRequest;
use Udb\Services\V1\DataBrokerClient;

/**
 * Static-style facade for `UdbClient`. Lets callers write
 * `Udb::select(...)` instead of resolving the client from the
 * container explicitly.
 *
 * The method docblock below is the source of IDE autocomplete
 * for Larastan / Psalm / PhpStorm. Keep it synchronised with
 * `UdbClient`'s public surface.
 *
 * @method static RecordSet           select(SelectRequest $request, ?\Fahara02\UdbLaravel\UdbMetadata $metadata = null)
 * @method static MutationResponse    upsert(UpsertRequest $request, ?\Fahara02\UdbLaravel\UdbMetadata $metadata = null)
 * @method static MutationResponse    delete(DeleteRequest $request, ?\Fahara02\UdbLaravel\UdbMetadata $metadata = null)
 * @method static DataBrokerClient    stub()
 * @method static \Fahara02\UdbLaravel\UdbMetadata context()
 * @method static void                bindContext(\Fahara02\UdbLaravel\UdbMetadata $metadata)
 *
 * @see UdbClient
 */
final class Udb extends Facade
{
    protected static function getFacadeAccessor(): string
    {
        // Matches the binding name in `UdbServiceProvider::register`.
        return UdbClient::class;
    }
}
