<?php

declare(strict_types=1);

// The data path a caller depends on, through this SDK: a value beyond 2^53 stays exact, a value the column cannot hold is refused, and a delete removes the row.
// Skipped unless UDB_LIVE_SDK_TESTS=1; CI's "SDK refresh-token rotation (live)" job runs it.

use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Fahara02\UdbLaravel\UdbProject;
use Google\Protobuf\Struct;
use Google\Protobuf\Value;
use Udb\Entity\V1\DeleteRequest;
use Udb\Entity\V1\SelectRequest;
use Udb\Entity\V1\UpsertRequest;

function dataContractEnv(string $name, ?string $fallback = null): string
{
    $value = trim((string) getenv($name));
    if ($value !== '') {
        return $value;
    }
    if ($fallback !== null) {
        return $fallback;
    }
    throw new RuntimeException("{$name} is required when UDB_LIVE_SDK_TESTS=1");
}

function dataContractKey(string $id): Struct
{
    return (new Struct())->setFields(['file_id' => (new Value())->setStringValue($id)]);
}

it('live data contract holds', function () {
    $file = 'udb.core.storage.entity.v1.File';
    $beyondDouble = '9007199254740993';
    $target = dataContractEnv('UDB_GRPC_TARGET');
    $project = UdbProject::connect([
        'target' => $target,
        'authTarget' => dataContractEnv('UDB_AUTH_GRPC_TARGET', $target),
        'tenantId' => dataContractEnv('UDB_LIVE_TENANT', 'sdk-live'),
        'projectId' => dataContractEnv('UDB_LIVE_PROJECT', 'default'),
        'purpose' => 'php.live.data-contract',
        'correlationId' => 'php-live-data-contract',
        'serviceIdentity' => 'php.sdk.live',
        'deadline_ms' => 10_000,
    ]);
    $project->loginAndAdoptTenant(dataContractEnv('UDB_LIVE_USERNAME'), dataContractEnv('UDB_LIVE_PASSWORD'));

    $id = sprintf('%04x%04x-%04x-4%03x-a%03x-%012x', random_int(0, 0xffff), random_int(0, 0xffff), random_int(0, 0xffff), random_int(0, 0xfff), random_int(0, 0xfff), random_int(0, 0xffffffffffff));
    $row = fn (string $size): string => "{\"file_id\":\"{$id}\",\"filename\":\"a.pdf\",\"object_key\":\"contract/{$id}\",\"status\":\"PENDING\",\"size_bytes\":{$size}}";
    $upsert = fn (string $size) => $project->data()->upsert(
        (new UpsertRequest())->setMessageType($file)->setRecordJson($row($size)),
    );
    $read = fn () => $project->data()->select(
        (new SelectRequest())->setMessageType($file)->setFilter(dataContractKey($id))->setLimit(1),
    );

    $upsert($beyondDouble);
    $found = $read();
    expect(count($found->getRecordsJson()))->toBe(1);
    expect($found->getRecordsJson()[0])->toContain($beyondDouble);

    expect(fn () => $upsert('1.5'))->toThrow(UdbRpcException::class);

    $project->data()->delete(
        (new DeleteRequest())->setMessageType($file)->setFilter(dataContractKey($id)),
    );
    expect(count($read()->getRecordsJson()))->toBe(0);

    // No catalog pin unless the caller asks for one, and a pin is honoured when it is sent.
    for ($i = 0; $i < 3; $i++) {
        expect(count($read()->getRecordsJson()))->toBe(0);
    }
    $pinned = UdbProject::connect([
        'target' => $target,
        'authTarget' => dataContractEnv('UDB_AUTH_GRPC_TARGET', $target),
        'tenantId' => dataContractEnv('UDB_LIVE_TENANT', 'sdk-live'),
        'projectId' => dataContractEnv('UDB_LIVE_PROJECT', 'default'),
        'purpose' => 'php.live.data-contract',
        'correlationId' => 'php-live-data-contract',
        'serviceIdentity' => 'php.sdk.live',
        'deadline_ms' => 10_000,
        'clientCatalogVersion' => '999.0.0',
    ]);
    $pinned->loginAndAdoptTenant(dataContractEnv('UDB_LIVE_USERNAME'), dataContractEnv('UDB_LIVE_PASSWORD'));
    for ($i = 0; $i < 3; $i++) {
        expect(fn () => $pinned->data()->select(
            (new SelectRequest())->setMessageType($file)->setFilter(dataContractKey($id))->setLimit(1),
        ))->toThrow(UdbRpcException::class, 'incompatible catalog version');
    }
})->skip(fn () => getenv('UDB_LIVE_SDK_TESTS') !== '1', 'requires a live broker');
