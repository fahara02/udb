<?php

declare(strict_types=1);

// Live ORM conformance (masterplan Phase 10 served proofs).
//
// Runs against a REAL broker over the real JWT login path and exercises the
// generated ORM surface end-to-end:
//  - typed IR query/write/delete builders dispatched through the served
//    GenericDispatch chokepoint (10.1),
//  - descriptor-backed repository CRUD asserting the emitted wire conflict
//    clause targets the descriptor primary keys (10.2),
//  - lazy/batch relation queries plus the one-query eager include path,
//    proving the N+1-safe secondary fetch against served Postgres (10.3),
//  - UnitOfWork flush through the served DataBroker.BeginTx bidi stream:
//    committed statuses, identity-map clean-up, and atomic rollback of the
//    whole batch when one mutation fails server-side (10.4).
//
// Gated on UDB_LIVE_SDK_TESTS=1 like the rest of the live suite.

use Fahara02\UdbLaravel\Generated\EagerIncludeUnsupportedBackendException;
use Fahara02\UdbLaravel\Generated\EntityRepository;
use Fahara02\UdbLaravel\Generated\GeneratedClient;
use Fahara02\UdbLaravel\Generated\UnitOfWorkUnsupportedBackendException;
use Fahara02\UdbLaravel\UdbAuthClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Udb\Core\Authn\Services\V1\LoginRequest;

if (getenv('UDB_LIVE_SDK_TESTS') !== '1') {
    test('live ORM conformance')->skip('requires live UDB broker');
    return;
}

function ormLiveEnv(string $name, ?string $fallback = null): string
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

function ormLiveMeta(string $bearerToken = '', string $tenantId = ''): UdbMetadata
{
    return new UdbMetadata(
        tenantId: $tenantId !== '' ? $tenantId : ormLiveEnv('UDB_LIVE_TENANT', 'sdk-live'),
        userId: '',
        purpose: 'php.live.orm',
        correlationId: 'php-live-orm',
        scopes: [],
        serviceIdentity: 'php.sdk.live.orm',
        projectId: ormLiveEnv('UDB_LIVE_PROJECT', 'default'),
        clientCatalogVersion: '1.0.0',
        bearerToken: $bearerToken,
    );
}

/** @return list<array<string,mixed>> */
function ormRows(object $resp): array
{
    $body = json_decode((string) $resp->getResultJson(), true, 512, JSON_THROW_ON_ERROR);
    if (is_array($body) && array_is_list($body)) {
        return $body;
    }
    if (is_array($body) && isset($body['rows']) && is_array($body['rows'])) {
        return array_values($body['rows']);
    }
    throw new RuntimeException('dispatch result_json is not a row set: ' . $resp->getResultJson());
}

/** @return array<string,mixed> */
function ormEmbedded(array $row, string $field): array
{
    $value = $row[$field] ?? null;
    if (is_array($value)) {
        return $value;
    }
    if (is_string($value)) {
        return json_decode($value, true, 512, JSON_THROW_ON_ERROR);
    }
    throw new RuntimeException("embedded relation '{$field}' missing on row: " . json_encode($row));
}

function ormUuid(): string
{
    $bytes = random_bytes(16);
    $bytes[6] = chr((ord($bytes[6]) & 0x0F) | 0x40);
    $bytes[8] = chr((ord($bytes[8]) & 0x3F) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bytes), 4));
}

// 10.2 wire contract: the repository upsert path is exactly this builder chain
// (EntityRepository::upsert), so pin the EMITTED spec: conflict kind update,
// conflict_on == descriptor primary keys, no PK as an update field.
function ormAssertConflictMatchesDescriptorPK(EntityRepository $repo, array $record): void
{
    $updateFields = array_values(array_diff(array_keys($record), $repo->binding()['primary_keys']));
    $request = GeneratedClient::writeTo($repo->messageType())
        ->record($record)
        ->updateOnConflict($updateFields, $repo->binding()['primary_keys'])
        ->toRequest();
    $spec = json_decode((string) $request->getSpecJson(), true, 512, JSON_THROW_ON_ERROR);
    expect($spec['ir']['op'])->toBe('write');
    expect($spec['ir']['conflict']['kind'])->toBe('update');
    expect(array_values($spec['ir']['conflict']['conflict_on']))
        ->toBe(array_values($repo->binding()['primary_keys']));
    foreach ($repo->binding()['primary_keys'] as $pk) {
        expect($spec['ir']['conflict']['fields'] ?? [])->not->toContain($pk);
    }
}

test('live ORM conformance: builders, repository, relations, UnitOfWork over the served broker', function (): void {
    $target = ormLiveEnv('UDB_GRPC_TARGET');
    $authTarget = ormLiveEnv('UDB_AUTH_GRPC_TARGET', $target);
    $meta = ormLiveMeta();

    $openAuth = new GeneratedClient(['endpoint' => $authTarget, 'deadline_ms' => 10_000, 'retry' => ['max_attempts' => 1]]);
    $openAuth->bindContext($meta);
    $login = $openAuth->login(
        (new LoginRequest())
            ->setUsername(ormLiveEnv('UDB_LIVE_USERNAME'))
            ->setPassword(ormLiveEnv('UDB_LIVE_PASSWORD'))
            ->setTenantHint($meta->tenantId)
            ->setProjectHint($meta->projectId)
            ->setDeviceName('php-sdk-live-orm'),
        $meta,
    );
    expect($login?->getAccessToken())->not->toBe('');

    $auth = new UdbAuthClient(['endpoint' => $authTarget, 'deadline_ms' => 10_000]);
    $auth->bindContext($meta);
    $authResp = $auth->authenticateBearer($login->getAccessToken(), $meta);
    $tenant = $authResp?->getPrincipal()?->getTenantId() ?: $meta->tenantId;
    expect($tenant)->not->toBe('');
    $project = $meta->projectId;

    $authedMeta = ormLiveMeta($login->getAccessToken(), $tenant);
    $data = new GeneratedClient(['endpoint' => $target, 'deadline_ms' => 15_000, 'retry' => ['max_attempts' => 1]]);
    $data->bindContext($authedMeta);

    $suffix = substr(str_replace('-', '', ormUuid()), 0, 12);

    // ------------------------------------------------------------------
    // 10.2 — descriptor-backed repository CRUD with conflict_on == PK.
    // ------------------------------------------------------------------
    $tmplRepo = GeneratedClient::repository('udb.core.notification.entity.v1.NotificationTemplate');
    $templateId = ormUuid();
    $eventType = "orm.live.php.{$suffix}";
    $template = [
        'template_id' => $templateId,
        'event_type' => $eventType,
        'channel' => 'EMAIL',
        'subject_template' => 'orm live subject',
        'body_template' => 'orm live body v1',
        'locale' => 'en',
        'is_active' => true,
        'tenant_id' => $tenant,
    ];
    ormAssertConflictMatchesDescriptorPK($tmplRepo, $template);
    $tmplRepo->upsert($template, $data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta);

    $found = ormRows($tmplRepo->find(['template_id' => $templateId], $data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta));
    expect($found)->toHaveCount(1);
    expect($found[0]['event_type'])->toBe($eventType);

    $template['body_template'] = 'orm live body v2';
    $tmplRepo->upsert($template, $data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta);

    $byEvent = ormRows(
        GeneratedClient::query($tmplRepo->messageType())
            ->where('event_type', 'eq', $eventType)
            ->execute($data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta)
    );
    expect($byEvent)->toHaveCount(1); // conflict-on-PK upsert must UPDATE, not duplicate
    expect($byEvent[0]['body_template'])->toBe('orm live body v2');

    // ------------------------------------------------------------------
    // 10.1 — typed IR query builder through served GenericDispatch.
    // ------------------------------------------------------------------
    $query = GeneratedClient::query($tmplRepo->messageType())
        ->where('event_type', 'eq', $eventType)
        ->select('template_id', 'event_type', 'locale')
        ->orderBy('template_id', 'asc')
        ->limit(5);
    expect((string) $query->toRequest()->getSpecJson())->toContain('"ir"');
    $resp = $query->execute($data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta);
    expect($resp->getBackend())->toBe('postgres');
    $qRows = ormRows($resp);
    expect($qRows)->toHaveCount(1);
    expect((string) $qRows[0]['template_id'])->not->toBe('');

    $inRows = ormRows(
        GeneratedClient::query($tmplRepo->messageType())
            ->whereIn('template_id', [$templateId])
            ->execute($data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta)
    );
    expect($inRows)->toHaveCount(1);

    // ------------------------------------------------------------------
    // 10.3 (served side) — lazy relation, batch secondary fetch, include.
    // ------------------------------------------------------------------
    $logRepo = GeneratedClient::repository('udb.core.notification.entity.v1.NotificationLog');
    $logId1 = ormUuid();
    $logId2 = ormUuid();
    $mkLog = static fn (string $logId): array => [
        'log_id' => $logId,
        'template_id' => $templateId,
        'event_type' => $eventType,
        'channel' => 'EMAIL',
        'recipient_address' => 'orm-live-php@example.com',
        'status' => 'PENDING',
        'retry_count' => 0,
        'tenant_id' => $tenant,
    ];
    $log1 = $mkLog($logId1);
    $log2 = $mkLog($logId2);
    $logRepo->upsert($log1, $data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta);
    $logRepo->upsert($log2, $data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta);

    $lazy = ormRows($logRepo->relationQuery('template', $log1)->execute($data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta));
    expect($lazy)->toHaveCount(1);
    expect($lazy[0]['template_id'])->toBe($templateId);

    $batch = ormRows($logRepo->relationBatchQuery('template', [$log1, $log2])->execute($data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta));
    expect($batch)->toHaveCount(1); // deduped whereIn over one shared parent

    $children = ormRows($tmplRepo->relationBatchQuery('notification_logs', [$template])->execute($data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta));
    expect($children)->toHaveCount(2); // ONE query loads all children — N+1-safe

    $incRows = ormRows(
        GeneratedClient::query($logRepo->messageType())
            ->whereIn('log_id', [$logId1, $logId2])
            ->include('template')
            ->orderBy('log_id', 'asc')
            ->execute($data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta)
    );
    expect($incRows)->toHaveCount(2);
    foreach ($incRows as $row) {
        expect(ormEmbedded($row, 'template')['template_id'])->toBe($templateId);
    }

    expect(static fn () => GeneratedClient::query($logRepo->messageType())->include('template')->toRequest('redis'))
        ->toThrow(EagerIncludeUnsupportedBackendException::class);

    // ------------------------------------------------------------------
    // 10.4 — UnitOfWork flush via the served DataBroker.BeginTx stream.
    // ------------------------------------------------------------------
    $flagRepo = GeneratedClient::repository('udb.core.config.entity.v1.Flag');
    expect($flagRepo->binding()['version_field'])->toBe('revision');
    $flagId = ormUuid();
    $flag = [
        'flag_id' => $flagId,
        'tenant_id' => $tenant,
        'project_id' => $project,
        'environment' => 'live',
        'flag_key' => "orm.live.php.{$suffix}",
        'value_type' => 'bool',
        'value_json' => 'true',
        'enabled' => true,
        'rollout_percentage' => 0,
        'rollout_context_key' => '',
        'revision' => 1,
        'metadata_json' => '{}',
    ];

    $uow = GeneratedClient::unitOfWork();
    expect(static fn () => $uow->requireTransactionalBackend('qdrant'))
        ->toThrow(UnitOfWorkUnsupportedBackendException::class);

    $uow->attach($flagRepo, $flag);
    $flag['value_json'] = 'false';
    $flag['revision'] = 2;
    $uow->update($flagRepo, $flag);

    $statuses = $uow->flush($data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta);
    expect(count($statuses))->toBeGreaterThanOrEqual(2);
    $last = $statuses[count($statuses) - 1];
    // TX_STATE_COMMITTED = 2 in the generated enum.
    expect((int) $last->getState())->toBe(2);
    expect($uow->dirtyEntries())->toBe([]);

    $persisted = ormRows($flagRepo->find(['flag_id' => $flagId], $data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta));
    expect($persisted)->toHaveCount(1);
    expect((int) $persisted[0]['revision'])->toBe(2);

    // Atomic rollback: a poisoned mutation (text bound into the INTEGER
    // rollout_percentage column — no implicit PG cast) must roll back the
    // WHOLE served transaction.
    $flag['revision'] = 3;
    $flag['value_json'] = '"v3"';
    $uow->update($flagRepo, $flag);
    $poisoned = [
        'flag_id' => ormUuid(),
        'tenant_id' => $tenant,
        'project_id' => $project,
        'environment' => 'live',
        'flag_key' => "orm.live.php.poison.{$suffix}",
        'value_type' => 'bool',
        'value_json' => 'true',
        'enabled' => true,
        'rollout_percentage' => 'boom',
        'rollout_context_key' => '',
        'revision' => 1,
        'metadata_json' => '{}',
    ];
    $uow->attach($flagRepo, $poisoned);
    $poisoned['enabled'] = false;
    $uow->update($flagRepo, $poisoned);

    $failed = false;
    try {
        $uow->flush($data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta);
    } catch (\Throwable $err) {
        $failed = true;
    }
    expect($failed)->toBeTrue('flush with a poisoned mutation must fail');
    expect($uow->dirtyEntries())->not->toBe([]);

    $persisted = ormRows($flagRepo->find(['flag_id' => $flagId], $data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta));
    expect($persisted)->toHaveCount(1);
    expect((int) $persisted[0]['revision'])->toBe(2); // whole batch rolled back

    // ------------------------------------------------------------------
    // Cleanup through the typed delete path (proves IrDeleteQuery live).
    // ------------------------------------------------------------------
    $logRepo->delete(['log_id' => $logId1], $data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta);
    $logRepo->delete(['log_id' => $logId2], $data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta);
    $tmplRepo->delete(['template_id' => $templateId], $data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta);
    $flagRepo->delete(['flag_id' => $flagId], $data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta);
    expect(ormRows($tmplRepo->find(['template_id' => $templateId], $data, GeneratedClient::DEFAULT_IR_BACKEND, $authedMeta)))->toBe([]);
})->skip(! extension_loaded('grpc'), 'requires grpc PHP extension');
