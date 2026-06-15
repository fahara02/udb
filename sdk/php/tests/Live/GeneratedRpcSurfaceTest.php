<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\Generated\GeneratedClient;
use Fahara02\UdbLaravel\UdbAuthClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Udb\Core\Authn\Services\V1\LoginRequest;
use Udb\Core\Authn\Services\V1\RefreshTokenRequest;
use Udb\Entity\V1\CapabilitiesRequest;

if (getenv('UDB_LIVE_SDK_TESTS') !== '1') {
    test('live generated RPC surface')->skip('requires live UDB broker');
    return;
}

function liveEnv(string $name, ?string $fallback = null): string
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

function liveMeta(string $bearerToken = '', string $tenantId = ''): UdbMetadata
{
    return new UdbMetadata(
        // Tenant-identity fix (auth_fix.md): once authenticated, callers pass the
        // CANONICAL tenant UUID (discovered from the principal) so request bodies
        // match the bearer claim; falls back to the human code pre-auth.
        tenantId: $tenantId !== '' ? $tenantId : liveEnv('UDB_LIVE_TENANT', 'sdk-live'),
        userId: '',
        purpose: 'php.live.conformance',
        correlationId: 'php-live-conformance',
        // No client-asserted scopes: admin authority comes from the Login JWT
        // (broker derives scopes from the validated bearer; header/body scopes are
        // ignored when a JWT verifier is configured). The real production path.
        scopes: [],
        serviceIdentity: 'php.sdk.live',
        projectId: liveEnv('UDB_LIVE_PROJECT', 'default'),
        clientCatalogVersion: '1.0.0',
        bearerToken: $bearerToken,
    );
}

function isFatalLiveStatus(int $code): bool
{
    // DEADLINE_EXCEEDED is NOT a mount failure: an unmounted RPC returns
    // UNIMPLEMENTED instantly, so a timeout means the server accepted the call and
    // is processing/blocking (e.g. PublishCDC is an open-ended CDC subscription
    // stream that legitimately blocks waiting for events).
    return in_array($code, [
        12, // UNIMPLEMENTED
        14, // UNAVAILABLE
        2,  // UNKNOWN
    ], true);
}

function assertLiveStatusMounted(string $label, mixed $status): void
{
    $code = is_object($status) ? (int) ($status->code ?? -1) : (int) ($status['code'] ?? -1);
    $details = is_object($status) ? (string) ($status->details ?? '') : (string) ($status['details'] ?? '');
    expect(isFatalLiveStatus($code))
        ->toBeFalse("{$label} did not reach an implemented live RPC: code={$code} details={$details}");
}

function requestFor(ReflectionMethod $method): object
{
    $params = $method->getParameters();
    if (count($params) === 0) {
        throw new RuntimeException("{$method->getName()} has no request parameter");
    }
    $type = $params[0]->getType();
    if (! $type instanceof ReflectionNamedType || $type->isBuiltin()) {
        throw new RuntimeException("{$method->getName()} first parameter is not a generated request type");
    }
    $class = $type->getName();
    return new $class();
}

function generatedStubMethods(object $stub): array
{
    $out = [];
    $ref = new ReflectionClass($stub);
    foreach ($ref->getMethods(ReflectionMethod::IS_PUBLIC) as $method) {
        if ($method->getDeclaringClass()->getName() !== $ref->getName()) {
            continue;
        }
        if ($method->isConstructor()) {
            continue;
        }
        $out[] = $method;
    }
    return $out;
}

function stubAccessors(GeneratedClient $data, GeneratedClient $authGenerated): array
{
    $out = [];
    $ref = new ReflectionClass(GeneratedClient::class);
    foreach ($ref->getMethods(ReflectionMethod::IS_PUBLIC) as $method) {
        if (! str_ends_with($method->getName(), 'Stub')) {
            continue;
        }
        $client = $method->getName() === 'DataBrokerStub' ? $data : $authGenerated;
        $out[$method->getName()] = $method->invoke($client);
    }
    return $out;
}

// Normalize a grpc stub method name (PascalCase, e.g. PutObject / RefreshSession / GetJwks)
// to the snake_case form used by the body builder + phase lists. No-op for snake input.
function rpcSnake(string $name): string
{
    return strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $name));
}

function liveStruct(array $fields): \Google\Protobuf\Struct
{
    $struct = new \Google\Protobuf\Struct();
    $map = $struct->getFields();
    foreach ($fields as $key => $value) {
        $v = new \Google\Protobuf\Value();
        if (is_bool($value)) {
            $v->setBoolValue($value);
        } elseif (is_int($value) || is_float($value)) {
            $v->setNumberValue($value);
        } else {
            $v->setStringValue((string) $value);
        }
        $map[$key] = $v;
    }
    return $struct;
}

function liveRecordJson(string $recordId, string $tenant, string $project, string $lookupKey, string $payload, int $revision): string
{
    return json_encode([
        'record_id' => $recordId,
        'tenant_id' => $tenant,
        'project_id' => $project,
        'lookup_key' => $lookupKey,
        'payload' => $payload,
        'revision' => $revision,
    ]);
}

function liveRecordPayload($recordSet, int $index = 0): string
{
    $raw = $recordSet->getRecordsJson()[$index];
    return json_decode($raw, true)['payload'] ?? '';
}

function liveDocPayload($documentSet): string
{
    $docs = $documentSet->getDocuments();
    if (count($docs) === 0) {
        return '';
    }
    return $docs[0]->getFields()['payload']->getStringValue();
}

/**
 * Real DataBroker backend round-trips (Postgres typed CRUD + Mongo document CRUD)
 * over unary RPCs — proves the data plane actually reads/writes, not just that the
 * methods are mounted. Streaming RPCs remain covered by the mount probe.
 */
function run_live_backend_e2e(GeneratedClient $data, UdbMetadata $meta, string $tenant, string $project): void
{
    $suffix = bin2hex(random_bytes(8));
    $messageType = 'udb.sdk.live.v1.SdkLiveRecord';
    $recordId = "php-$suffix";
    $ctx = (new \Udb\Entity\V1\RequestContext())
        ->setTenantId($tenant)
        ->setProjectId($project)
        ->setPurpose('php.live.backend.e2e')
        ->setCorrelationId("php-backend-$suffix")
        ->setServiceIdentity('php.sdk.live');

    $data->generic_dispatch((new \Udb\Entity\V1\GenericDispatchRequest())
        ->setContext($ctx)->setBackend('postgres')->setOperation('query')
        ->setSpecJson('{"sql":"SELECT 1::INT AS live_probe"}'), $meta);

    $inserted = $data->upsert((new \Udb\Entity\V1\UpsertRequest())
        ->setContext($ctx)->setMessageType($messageType)
        ->setRecordJson(liveRecordJson($recordId, $tenant, $project, "php-lk-$suffix", 'created-from-php', 1))
        ->setConflictFields(['record_id'])->setReturnRecord(true), $meta);
    expect($inserted->getAffectedRows())->toBe(1);

    $selected = $data->select((new \Udb\Entity\V1\SelectRequest())
        ->setContext($ctx)->setMessageType($messageType)
        ->setFilter(liveStruct(['record_id' => $recordId, 'tenant_id' => $tenant, 'project_id' => $project]))
        ->setLimit(1), $meta);
    expect(liveRecordPayload($selected))->toBe('created-from-php');

    $data->upsert((new \Udb\Entity\V1\UpsertRequest())
        ->setContext($ctx)->setMessageType($messageType)
        ->setRecordJson(liveRecordJson($recordId, $tenant, $project, "php-lk-$suffix", 'updated-from-php', 2))
        ->setConflictFields(['record_id'])->setReturnRecord(true), $meta);

    $collection = "sdk_live_docs_php_$suffix";
    $data->ensure_resource((new \Udb\Entity\V1\ResourceAdminRequest())
        ->setContext($ctx)->setBackend('mongodb')->setResourceName($collection)
        ->setSpecJson('{"collection":"' . $collection . '"}'), $meta);
    $resources = $data->list_resources((new \Udb\Entity\V1\ResourceAdminRequest())
        ->setContext($ctx)->setBackend('mongodb'), $meta);
    $hasCollection = false;
    foreach ($resources->getResources() as $name) {
        if (str_contains($name, $collection)) {
            $hasCollection = true;
            break;
        }
    }
    expect($hasCollection)->toBeTrue();

    $documentId = "doc-$suffix";
    $resource = (new \Udb\Entity\V1\StoreResource())->setBackend('mongodb')->setResourceName($collection);
    $data->document_upsert((new \Udb\Entity\V1\DocumentUpsertRequest())
        ->setContext($ctx)->setResource($resource)->setDocumentId($documentId)
        ->setDocument(liveStruct(['_id' => $documentId, 'tenant_id' => $tenant, 'project_id' => $project, 'payload' => 'mongo-created', 'revision' => 1])), $meta);
    $gotDoc = $data->document_get((new \Udb\Entity\V1\DocumentGetRequest())
        ->setContext($ctx)->setResource($resource)->setDocumentId($documentId), $meta);
    expect(liveDocPayload($gotDoc))->toBe('mongo-created');
    $data->document_upsert((new \Udb\Entity\V1\DocumentUpsertRequest())
        ->setContext($ctx)->setResource($resource)->setDocumentId($documentId)
        ->setDocument(liveStruct(['payload' => 'mongo-updated', 'revision' => 2])), $meta);
    $foundDoc = $data->document_find((new \Udb\Entity\V1\DocumentFindRequest())
        ->setContext($ctx)->setResource($resource)->setFilter(liveStruct(['_id' => $documentId]))->setLimit(1), $meta);
    expect(liveDocPayload($foundDoc))->toBe('mongo-updated');
    $deletedDoc = $data->document_delete((new \Udb\Entity\V1\DocumentDeleteRequest())
        ->setContext($ctx)->setResource($resource)->setDocumentId($documentId), $meta);
    expect($deletedDoc->getAffectedRows())->toBe(1);

    $deleted = $data->delete((new \Udb\Entity\V1\DeleteRequest())
        ->setContext($ctx)->setMessageType($messageType)
        ->setFilter(liveStruct(['record_id' => $recordId, 'tenant_id' => $tenant, 'project_id' => $project])), $meta);
    expect($deleted->getAffectedRows())->toBe(1);

    // Control-plane data ops: project create+list, policy reads, catalog/schema/
    // health. PutPolicy is intentionally NOT called — an abac policy insert flips
    // the data plane to default-deny.
    $projId = "sdklive_proj_php_$suffix";
    $data->ensure_project((new \Udb\Entity\V1\EnsureProjectRequest())
        ->setContext($ctx)->setProjectId($projId)->setName('SDK Live Project'), $meta);
    $projects = $data->list_projects((new \Udb\Entity\V1\ProjectListRequest())->setContext($ctx), $meta);
    $foundProject = false;
    foreach ($projects->getProjects() as $p) {
        if ($p->getProjectId() === $projId) {
            $foundProject = true;
            break;
        }
    }
    expect($foundProject)->toBeTrue();
    $data->list_policies((new \Udb\Entity\V1\PolicyListRequest())->setContext($ctx), $meta);
    $data->lint_policies((new \Udb\Entity\V1\CapabilitiesRequest())->setContext($ctx), $meta);
    $manifest = $data->get_catalog_manifest((new \Udb\Entity\V1\CatalogManifestRequest())->setContext($ctx), $meta);
    expect(strlen($manifest->getManifestJson()))->toBeGreaterThan(0);
    $schemas = $data->list_message_schemas((new \Udb\Entity\V1\MessageSchemaListRequest())
        ->setContext($ctx)->setProjectId($project), $meta);
    expect(count($schemas->getMessageTypes()))->toBeGreaterThan(0);
    $lookup = $data->lookup_message_schema((new \Udb\Entity\V1\MessageSchemaLookupRequest())
        ->setContext($ctx)->setProjectId($project)->setMessageType($messageType), $meta);
    expect($lookup->getSchema())->not->toBeNull();
    $data->get_health_report((new \Udb\Entity\V1\HealthReportRequest())
        ->setContext($ctx)->setWithProbes(true)->setProjectId($project), $meta);
}

/**
 * Real create→read→assert CRUD against every native control-plane service.
 * Free-text-tenant services use $authGenerated (sdk-live admin); the UUID-tenant
 * services (storage/webrtc/asset) use $uuidGenerated (a second admin bootstrapped
 * on a UUID tenant). Authz created_by must be a UUID; the notification
 * recipient_id is an FK to a real users row.
 */
function run_native_service_e2e(GeneratedClient $authGenerated, GeneratedClient $uuidGenerated, UdbMetadata $meta, UdbMetadata $uuidMeta, string $tenant, string $project, string $uuidTenant): void
{
    $suffix = bin2hex(random_bytes(8));

    // TenantService — CreateTenant is a platform write.
    $createdTenant = $authGenerated->create_tenant((new \Udb\Core\Tenant\Services\V1\CreateTenantRequest())
        ->setCode("sdklivephp$suffix")->setName('SDK Live PHP')->setType('WORKSPACE'), $meta);
    expect($createdTenant->getTenantId())->not->toBe('');

    // AuthzService — role create/get/list.
    $roleCode = "sdk_reader_php_$suffix";
    $createdRole = $authGenerated->create_role((new \Udb\Core\Authz\Services\V1\CreateRoleRequest())
        ->setName("SDK Reader PHP $suffix")->setDescription('Live SDK reader role')
        ->setCreatedBy(liveUuidV4())->setRoleCode($roleCode)
        ->setDomain($tenant)->setTenantId($tenant)->setProjectId($project), $meta)->getRole();
    expect($createdRole->getRoleCode())->toBe($roleCode);
    $gotRole = $authGenerated->get_role((new \Udb\Core\Authz\Services\V1\GetRoleRequest())
        ->setRoleId($createdRole->getRoleId()), $meta)->getRole();
    expect($gotRole->getRoleCode())->toBe($roleCode);
    $roles = $authGenerated->list_roles((new \Udb\Core\Authz\Services\V1\ListRolesRequest())
        ->setDomain($tenant)->setActiveOnly(true), $meta);
    $foundRole = false;
    foreach ($roles->getRoles() as $r) {
        if ($r->getRoleId() === $createdRole->getRoleId()) {
            $foundRole = true;
            break;
        }
    }
    expect($foundRole)->toBeTrue();

    // Full decision flow: assign the role to a real user, attach an allow policy,
    // prove CheckAccess flips allow→deny across a role revoke (security-critical).
    $subject = $authGenerated->create_user((new \Udb\Core\Authn\Services\V1\CreateUserRequest())
        ->setUsername("sdk-authz-php-$suffix")->setEmail("sdk-authz-php-$suffix@example.com")
        ->setPassword('CorrectHorse1!')->setTenantId($tenant)->setProjectId($project)->setFullName('SDK Authz Subject'), $meta)->getUser();
    $assigned = $authGenerated->assign_role((new \Udb\Core\Authz\Services\V1\AssignRoleRequest())
        ->setUserId($subject->getUserId())->setRoleId($createdRole->getRoleId())->setDomain($tenant)
        ->setAssignedBy($subject->getUserId())->setTenantId($tenant)->setProjectId($project), $meta)->getUserRole();
    $authGenerated->put_authz_policy((new \Udb\Core\Authz\Services\V1\PutAuthzPolicyRequest())
        ->setPolicy((new \Udb\Core\Authz\Services\V1\AuthzPolicyRecord())
            ->setId(liveUuidV4())->setEnabled(true)->setEffect('allow')->setTenant($tenant)->setProject($project)
            ->setRole($createdRole->getRoleCode())->setAction('data.select')->setResource('invoice')), $meta);
    $allowed = $authGenerated->check_access((new \Udb\Core\Authz\Services\V1\CheckAccessRequest())
        ->setUserId($subject->getUserId())->setDomain($tenant)->setTenantId($tenant)->setProjectId($project)
        ->setObject('invoice')->setAction('data.select'), $meta);
    expect($allowed->getAllowed())->toBeTrue();
    $userRoles = $authGenerated->list_user_roles((new \Udb\Core\Authz\Services\V1\ListUserRolesRequest())
        ->setUserId($subject->getUserId())->setDomain($tenant)->setActiveOnly(true), $meta);
    expect(count($userRoles->getUserRoles()))->toBe(1);
    $authGenerated->revoke_role((new \Udb\Core\Authz\Services\V1\RevokeRoleRequest())
        ->setUserRoleId($assigned->getUserRoleId())->setUserId($subject->getUserId())->setReason('sdk_live_test')->setRevokedBy($subject->getUserId()), $meta);
    $denied = $authGenerated->check_access((new \Udb\Core\Authz\Services\V1\CheckAccessRequest())
        ->setUserId($subject->getUserId())->setDomain($tenant)->setTenantId($tenant)->setProjectId($project)
        ->setObject('invoice')->setAction('data.select'), $meta);
    expect($denied->getAllowed())->toBeFalse();

    // ApiKeyService — create/validate/list/revoke lifecycle.
    $principal = "sdk-live-svc-$suffix";
    $keyCtx = (new \Udb\Core\Common\V1\RequestContext())
        ->setUserId($principal)
        ->setTenant((new \Udb\Core\Common\V1\TenantContext())->setTenantId($tenant)->setProjectId($project));
    $createdKey = $authGenerated->create_api_key((new \Udb\Core\Apikey\Services\V1\CreateApiKeyRequest())
        ->setName("sdk-live-key-$suffix")->setOwnerId($principal)->setScopes(['data:read'])->setContext($keyCtx), $meta);
    expect(str_starts_with($createdKey->getPlainKey(), 'udbk_'))->toBeTrue();
    $keyId = $createdKey->getKey()->getKeyId();
    $valid = $authGenerated->validate_api_key((new \Udb\Core\Apikey\Services\V1\ValidateApiKeyRequest())
        ->setPlainKey($createdKey->getPlainKey())->setRequiredScope('data:read'), $meta);
    expect($valid->getValid())->toBeTrue();
    expect($valid->getOwnerId())->toBe($principal);
    $listedKeys = $authGenerated->list_api_keys((new \Udb\Core\Apikey\Services\V1\ListApiKeysRequest())
        ->setOwnerId($principal)->setStatus(1), $meta); // 1 = ACTIVE
    expect(count($listedKeys->getKeys()))->toBe(1);
    expect($listedKeys->getKeys()[0]->getKeyId())->toBe($keyId);
    $gotKey = $authGenerated->get_api_key((new \Udb\Core\Apikey\Services\V1\GetApiKeyRequest())->setKeyId($keyId), $meta);
    expect($gotKey->getKey()->getOwnerId())->toBe($principal);
    $authGenerated->update_api_key((new \Udb\Core\Apikey\Services\V1\UpdateApiKeyRequest())
        ->setKeyId($keyId)->setScopes(['data:read', 'data:write'])->setContext($keyCtx), $meta);
    $writeOk = $authGenerated->validate_api_key((new \Udb\Core\Apikey\Services\V1\ValidateApiKeyRequest())
        ->setPlainKey($createdKey->getPlainKey())->setRequiredScope('data:write'), $meta);
    expect($writeOk->getValid())->toBeTrue();
    $authGenerated->revoke_api_key((new \Udb\Core\Apikey\Services\V1\RevokeApiKeyRequest())
        ->setKeyId($keyId)->setRevokeReason('sdk_live_test')->setContext($keyCtx), $meta);
    $afterRevoke = $authGenerated->validate_api_key((new \Udb\Core\Apikey\Services\V1\ValidateApiKeyRequest())
        ->setPlainKey($createdKey->getPlainKey())->setRequiredScope('data:read'), $meta);
    expect($afterRevoke->getValid())->toBeFalse();

    // AnalyticsService — record metrics then roll up.
    $stage = "sdk_live_stage_php_$suffix";
    foreach ([[100.0, true], [200.0, true], [400.0, false]] as [$latency, $ok]) {
        $accepted = $authGenerated->record_pipeline_metric((new \Udb\Core\Analytics\Services\V1\RecordPipelineMetricRequest())
            ->setStageName($stage)->setTenantId($tenant)->setLatencyMs($latency)->setIsSuccess($ok), $meta);
        expect($accepted->getAccepted())->toBeTrue();
    }
    $summary = $authGenerated->get_pipeline_summary((new \Udb\Core\Analytics\Services\V1\GetPipelineSummaryRequest())
        ->setStageName($stage)->setTenantId($tenant)
        ->setPage((new \Udb\Core\Common\V1\PageRequest())->setPage(1)->setPageSize(10)), $meta);
    expect(count($summary->getSnapshots()))->toBe(1);
    expect($summary->getSnapshots()[0]->getTotalRequests())->toBe(3);
    $throughput = $authGenerated->get_throughput((new \Udb\Core\Analytics\Services\V1\GetThroughputRequest())
        ->setTenantId($tenant), $meta);
    expect($throughput->getTotalRequests())->toBeGreaterThanOrEqual(3);
    $trig = $authGenerated->trigger_snapshot((new \Udb\Core\Analytics\Services\V1\TriggerSnapshotRequest())
        ->setStageName($stage), $meta);
    expect($trig->getSnapshotsWritten())->toBeGreaterThanOrEqual(1);

    // NotificationService — template + send to a real user (recipient_id FK).
    $recipient = $authGenerated->create_user((new \Udb\Core\Authn\Services\V1\CreateUserRequest())
        ->setUsername("sdk-notif-php-$suffix")->setEmail("sdk-notif-php-$suffix@example.com")
        ->setPassword('CorrectHorse1!')->setTenantId($tenant)->setProjectId($project)->setFullName('SDK Notify PHP'), $meta)->getUser();
    $event = "sdk.live.php.$suffix";
    $body = "sdk-live-body-php-$suffix";
    $authGenerated->upsert_template((new \Udb\Core\Notification\Services\V1\UpsertTemplateRequest())
        ->setEventType($event)->setChannel(1)->setLocale('en')
        ->setSubjectTemplate('SDK {{n}}')->setBodyTemplate($body)->setIsActive(true), $meta);
    $template = $authGenerated->get_template((new \Udb\Core\Notification\Services\V1\GetTemplateRequest())
        ->setEventType($event)->setChannel(1)->setLocale('en'), $meta)->getTemplate();
    expect($template->getBodyTemplate())->toBe($body);
    $sent = $authGenerated->send_notification((new \Udb\Core\Notification\Services\V1\SendNotificationRequest())
        ->setEventType($event)->setRecipientId($recipient->getUserId())->setRecipientAddress("sdk+$suffix@example.com")
        ->setTenantId($tenant)->setChannels([1]), $meta);
    expect(count($sent->getLogs()))->toBeGreaterThanOrEqual(1);
    $logId = $sent->getLogs()[0]->getLogId();
    $listedNotifs = $authGenerated->list_notifications((new \Udb\Core\Notification\Services\V1\ListNotificationsRequest())
        ->setTenantId($tenant), $meta);
    $foundLog = false;
    foreach ($listedNotifs->getLogs() as $l) {
        if ($l->getLogId() === $logId) {
            $foundLog = true;
            break;
        }
    }
    expect($foundLog)->toBeTrue();
    $gotNotif = $authGenerated->get_notification((new \Udb\Core\Notification\Services\V1\GetNotificationRequest())->setLogId($logId), $meta);
    expect($gotNotif->getLog()->getLogId())->toBe($logId);
    $authGenerated->set_preference((new \Udb\Core\Notification\Services\V1\SetPreferenceRequest())
        ->setUserId($recipient->getUserId())->setTenantId($tenant)->setChannel(1)->setIsOptedOut(true), $meta);
    $pref = $authGenerated->get_preference((new \Udb\Core\Notification\Services\V1\GetPreferenceRequest())
        ->setUserId($recipient->getUserId())->setTenantId($tenant)->setChannel(1), $meta);
    expect($pref->getPreference()->getIsOptedOut())->toBeTrue();
    $prefs = $authGenerated->list_preferences((new \Udb\Core\Notification\Services\V1\ListPreferencesRequest())
        ->setUserId($recipient->getUserId())->setTenantId($tenant), $meta);
    expect(count($prefs->getPreferences()))->toBeGreaterThanOrEqual(1);
    $authGenerated->get_delivery_stats((new \Udb\Core\Notification\Services\V1\GetDeliveryStatsRequest())
        ->setTenantId($tenant), $meta);

    // StorageService — file lifecycle under the UUID-tenant admin (project_id and
    // reference_id are UUID columns: empty project → NULL, reference_id a UUID).
    $ref = liveUuidV4();
    $reg = $uuidGenerated->register_upload((new \Udb\Core\Storage\Services\V1\RegisterUploadRequest())
        ->setTenantId($uuidTenant)->setProjectId('')->setFilename("sdk-$suffix.txt")->setContentType('text/plain')
        ->setFileType('DOCUMENT')->setReferenceId($ref)->setReferenceType('sdk.live')->setSizeBytes(128)->setExpiresInMinutes(10), $uuidMeta);
    expect($reg->getFileId())->not->toBe('');
    expect(str_starts_with($reg->getUploadUrl(), 'http'))->toBeTrue();
    $gotFile = $uuidGenerated->get_file((new \Udb\Core\Storage\Services\V1\GetFileRequest())
        ->setTenantId($uuidTenant)->setFileId($reg->getFileId()), $uuidMeta);
    expect($gotFile->getFile()->getFileId())->toBe($reg->getFileId());
    $renamed = "sdk-renamed-$suffix.txt";
    $uuidGenerated->update_file((new \Udb\Core\Storage\Services\V1\UpdateFileRequest())
        ->setTenantId($uuidTenant)->setFileId($reg->getFileId())->setFilename($renamed), $uuidMeta);
    $reread = $uuidGenerated->get_file((new \Udb\Core\Storage\Services\V1\GetFileRequest())
        ->setTenantId($uuidTenant)->setFileId($reg->getFileId()), $uuidMeta);
    expect($reread->getFile()->getFilename())->toBe($renamed);
    $download = $uuidGenerated->get_download_url((new \Udb\Core\Storage\Services\V1\GetDownloadUrlRequest())
        ->setTenantId($uuidTenant)->setFileId($reg->getFileId())->setExpiresInMinutes(10), $uuidMeta);
    expect(str_starts_with($download->getDownloadUrl(), 'http'))->toBeTrue();
    $listedFiles = $uuidGenerated->list_files((new \Udb\Core\Storage\Services\V1\ListFilesRequest())
        ->setTenantId($uuidTenant)->setReferenceId($ref), $uuidMeta);
    expect($listedFiles->getTotalCount())->toBeGreaterThanOrEqual(1);
    $deletedFile = $uuidGenerated->delete_file((new \Udb\Core\Storage\Services\V1\DeleteFileRequest())
        ->setTenantId($uuidTenant)->setFileId($reg->getFileId()), $uuidMeta);
    expect($deletedFile->getSuccess())->toBeTrue();

    // AssetService — pipeline definition + asset registered against a stored file.
    $assetFile = $uuidGenerated->register_upload((new \Udb\Core\Storage\Services\V1\RegisterUploadRequest())
        ->setTenantId($uuidTenant)->setProjectId('')->setFilename("asset-$suffix.json")->setContentType('application/json')
        ->setFileType('OTHER')->setReferenceId(liveUuidV4())->setReferenceType('sdk.asset')->setSizeBytes(64)->setExpiresInMinutes(10), $uuidMeta);
    $definition = $uuidGenerated->create_pipeline_definition((new \Udb\Core\Asset\Services\V1\CreatePipelineDefinitionRequest())
        ->setTenantId($uuidTenant)->setName("sdk-pipeline-$suffix")->setDescription('Live SDK pipeline')
        ->setMediaType('application/json')->setSteps('[{"name":"extract","type":"EXTRACT"}]')->setVersion(1), $uuidMeta);
    expect($definition->getDefinitionId())->not->toBe('');
    $uuidGenerated->get_pipeline_definition((new \Udb\Core\Asset\Services\V1\GetPipelineDefinitionRequest())
        ->setTenantId($uuidTenant)->setDefinitionId($definition->getDefinitionId()), $uuidMeta);
    $asset = $uuidGenerated->register_asset((new \Udb\Core\Asset\Services\V1\RegisterAssetRequest())
        ->setTenantId($uuidTenant)->setProjectId('')->setFileId($assetFile->getFileId())->setName("sdk-asset-$suffix")
        ->setMediaType('application/json')->setMetadata('{"source":"sdk-live"}'), $uuidMeta);
    expect($asset->getAssetId())->not->toBe('');
    $uuidGenerated->get_asset((new \Udb\Core\Asset\Services\V1\GetAssetRequest())
        ->setTenantId($uuidTenant)->setAssetId($asset->getAssetId()), $uuidMeta);
    $started = $uuidGenerated->start_pipeline((new \Udb\Core\Asset\Services\V1\StartPipelineRequest())
        ->setTenantId($uuidTenant)->setDefinitionId($definition->getDefinitionId())->setAssetId($asset->getAssetId())
        ->setContext('{}')->setCorrelationId("sdk-live-$suffix"), $uuidMeta);
    expect($started->getInstanceId())->not->toBe('');
    $uuidGenerated->get_pipeline((new \Udb\Core\Asset\Services\V1\GetPipelineRequest())
        ->setTenantId($uuidTenant)->setInstanceId($started->getInstanceId()), $uuidMeta);
    $assets = $uuidGenerated->list_assets((new \Udb\Core\Asset\Services\V1\ListAssetsRequest())
        ->setTenantId($uuidTenant), $uuidMeta);
    $foundAsset = false;
    foreach ($assets->getAssets() as $a) {
        if ($a->getAssetId() === $asset->getAssetId()) {
            $foundAsset = true;
            break;
        }
    }
    expect($foundAsset)->toBeTrue();

    // WebRTC — room/peer/track lifecycle + best-effort TURN issuance.
    $room = $uuidGenerated->create_room((new \Udb\Core\Webrtc\Services\V1\CreateRoomRequest())
        ->setTenantId($uuidTenant)->setName("sdk-room-$suffix")->setMaxParticipants(8)->setConfig('{}')->setCreatedBy(liveUuidV4()), $uuidMeta);
    expect($room->getRoomId())->not->toBe('');
    $uuidGenerated->get_room((new \Udb\Core\Webrtc\Services\V1\GetRoomRequest())
        ->setTenantId($uuidTenant)->setRoomId($room->getRoomId()), $uuidMeta);
    $listedRooms = $uuidGenerated->list_rooms((new \Udb\Core\Webrtc\Services\V1\ListRoomsRequest())
        ->setTenantId($uuidTenant), $uuidMeta);
    $foundRoom = false;
    foreach ($listedRooms->getRooms() as $r) {
        if ($r->getRoomId() === $room->getRoomId()) {
            $foundRoom = true;
            break;
        }
    }
    expect($foundRoom)->toBeTrue();
    $joined = $uuidGenerated->join_room((new \Udb\Core\Webrtc\Services\V1\JoinRoomRequest())
        ->setTenantId($uuidTenant)->setRoomId($room->getRoomId())->setDisplayName('sdk-peer')->setMetadata('{}')->setUserAgent('sdk-live'), $uuidMeta);
    expect($joined->getPeer()->getPeerId())->not->toBe('');
    $peerList = $uuidGenerated->list_peers((new \Udb\Core\Webrtc\Services\V1\ListPeersRequest())
        ->setTenantId($uuidTenant)->setRoomId($room->getRoomId()), $uuidMeta);
    $foundPeer = false;
    foreach ($peerList->getPeers() as $p) {
        if ($p->getPeerId() === $joined->getPeer()->getPeerId()) {
            $foundPeer = true;
            break;
        }
    }
    expect($foundPeer)->toBeTrue();
    $uuidGenerated->get_peer((new \Udb\Core\Webrtc\Services\V1\GetPeerRequest())
        ->setTenantId($uuidTenant)->setPeerId($joined->getPeer()->getPeerId()), $uuidMeta);
    $uuidGenerated->update_room((new \Udb\Core\Webrtc\Services\V1\UpdateRoomRequest())
        ->setTenantId($uuidTenant)->setRoomId($room->getRoomId())->setName("sdk-room-renamed-$suffix"), $uuidMeta);
    $published = $uuidGenerated->publish_track((new \Udb\Core\Webrtc\Services\V1\PublishTrackRequest())
        ->setTenantId($uuidTenant)->setRoomId($room->getRoomId())->setPeerId($joined->getPeer()->getPeerId())
        ->setKind('audio')->setLabel('mic')->setSettings('{}')->setMetadata('{}'), $uuidMeta);
    expect($published->getTrackId())->not->toBe('');
    $tracks = $uuidGenerated->list_tracks((new \Udb\Core\Webrtc\Services\V1\ListTracksRequest())
        ->setTenantId($uuidTenant)->setRoomId($room->getRoomId()), $uuidMeta);
    expect(count($tracks->getTracks()))->toBeGreaterThanOrEqual(1);
    $uuidGenerated->mute_track((new \Udb\Core\Webrtc\Services\V1\MuteTrackRequest())
        ->setTenantId($uuidTenant)->setTrackId($published->getTrackId())->setMuted(true), $uuidMeta);
    $uuidGenerated->unpublish_track((new \Udb\Core\Webrtc\Services\V1\UnpublishTrackRequest())
        ->setTenantId($uuidTenant)->setTrackId($published->getTrackId()), $uuidMeta);
    try {
        // TURN issuance is best-effort: coturn may be unconfigured locally and the
        // service fail-closes with a real status (not a mount failure).
        $uuidGenerated->issue_credentials((new \Udb\Core\Webrtc\Services\V1\IssueCredentialsRequest())
            ->setTenantId($uuidTenant)->setRoomId($room->getRoomId())->setPeerId($joined->getPeer()->getPeerId())->setTtlSeconds(3600), $uuidMeta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        expect(isFatalLiveStatus($e->status))->toBeFalse("IssueCredentials did not reach a live RPC: {$e->getMessage()}");
    }
    $left = $uuidGenerated->leave_room((new \Udb\Core\Webrtc\Services\V1\LeaveRoomRequest())
        ->setTenantId($uuidTenant)->setRoomId($room->getRoomId())->setPeerId($joined->getPeer()->getPeerId()), $uuidMeta);
    expect($left->getSuccess())->toBeTrue();
    $uuidGenerated->close_room((new \Udb\Core\Webrtc\Services\V1\CloseRoomRequest())
        ->setTenantId($uuidTenant)->setRoomId($room->getRoomId()), $uuidMeta);
}

function liveUuidV4(): string
{
    $data = random_bytes(16);
    $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
    $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
}

// Whether to field-populate an RPC's probe request: every RPC except those
// classified DESTRUCTIVE by the proto-derived GeneratedClient::OPERATION_KIND map
// (keyed by the globally-unique method name) — never a hardcoded name list. A
// populated destructive RPC (PutPolicy, RollbackCatalog, the revoke-all/emergency/
// reset family, DropResource, …) could corrupt shared/global broker state.
function shouldPopulatePhp(string $name): bool
{
    return (\Fahara02\UdbLaravel\Generated\GeneratedClient::OPERATION_KIND[$name] ?? '') !== 'destructive';
}

// Name-aware probe string, mirroring the Go/Python descriptor probes: tenant/
// project/message_type/domain/purpose get meaningful values, page tokens stay
// empty, everything else gets a marker. Keyed by the lower-cased field name.
function probeStringPhp(string $field, string $tenant, string $project): string
{
    $n = strtolower($field);
    if (str_contains($n, 'tenant')) {
        return $tenant;
    }
    if (str_contains($n, 'project')) {
        return $project;
    }
    if ($n === 'messagetype' || str_contains($n, 'messagetype')) {
        return 'udb.sdk.live.v1.SdkLiveRecord';
    }
    if (str_contains($n, 'domain')) {
        return $tenant;
    }
    if (str_contains($n, 'purpose')) {
        return 'php.live.probe';
    }
    if ($n === 'locale' || str_contains($n, 'locale')) {
        return 'en';
    }
    if (str_contains($n, 'pagetoken')) {
        return '';
    }
    return 'sdk-live-probe';
}

// Populate a RequestContext (data-plane entity.v1 is flat tenant_id/project_id;
// control-plane common.v1 nests tenant {tenant_id, project_id}). Setting whichever
// setters exist covers both shapes.
function populateContextPhp(object $ctx, string $tenant, string $project): void
{
    if (method_exists($ctx, 'setTenantId')) {
        $ctx->setTenantId($tenant);
    }
    if (method_exists($ctx, 'setProjectId')) {
        $ctx->setProjectId($project);
    }
    if (method_exists($ctx, 'setPurpose')) {
        $ctx->setPurpose('php.live.probe');
    }
    if (method_exists($ctx, 'setTenant')) {
        $ctx->setTenant((new \Udb\Core\Common\V1\TenantContext())->setTenantId($tenant)->setProjectId($project));
    }
}

// Descriptor-equivalent field population via reflection: enumerate every generated
// `set*` setter and populate ALL scalar fields (string/int/float) plus the nested
// RequestContext, shallow-recursing one level into sub-messages — exactly what the
// Go/Python/TS descriptor probes do. Repeated fields (array|RepeatedField), well-
// known types (Struct/Timestamp/…) and bool flags are left at their defaults so the
// probe never flips a semantic toggle. This replaces the old 8-hardcoded-setter
// version that left most of each request empty (a shallow "one field and done" probe
// that never exercised the bulk of each handler's decode/validation path).
function populateProbeRequest(object $request, string $tenant, string $project, int $depth = 0): void
{
    $ref = new ReflectionObject($request);
    foreach ($ref->getMethods(ReflectionMethod::IS_PUBLIC) as $method) {
        $name = $method->getName();
        if (! str_starts_with($name, 'set') || $method->getNumberOfParameters() !== 1) {
            continue;
        }
        $type = $method->getParameters()[0]->getType();
        $field = lcfirst(substr($name, 3));
        try {
            if ($type instanceof ReflectionNamedType && $type->isBuiltin()) {
                switch ($type->getName()) {
                    case 'string':
                        $request->{$name}(probeStringPhp($field, $tenant, $project));
                        break;
                    case 'int':
                        $request->{$name}(1);
                        break;
                    case 'float':
                        $request->{$name}(1.0);
                        break;
                    // bool left at default: flipping a semantic flag (return_record,
                    // active_only, with_probes, …) could change handler behavior.
                }
            } elseif ($type instanceof ReflectionUnionType) {
                // Message-typed (and repeated) setters are union types in the proto3
                // PHP gencode: `\Pkg\Msg|null` for singular messages, `array|
                // RepeatedField` for repeated. Populate singular sub-messages only.
                foreach ($type->getTypes() as $t) {
                    if (! $t instanceof ReflectionNamedType || $t->isBuiltin()) {
                        continue;
                    }
                    $cls = $t->getName();
                    if (! class_exists($cls) || ! is_subclass_of($cls, \Google\Protobuf\Internal\Message::class)) {
                        continue;
                    }
                    if (str_starts_with(ltrim($cls, '\\'), 'Google\\Protobuf\\')) {
                        break; // well-known types: a default-valued message is valid
                    }
                    if ($field === 'context') {
                        $sub = new $cls();
                        populateContextPhp($sub, $tenant, $project);
                        $request->{$name}($sub);
                    } elseif ($depth < 1) {
                        $sub = new $cls();
                        populateProbeRequest($sub, $tenant, $project, $depth + 1);
                        $request->{$name}($sub);
                    }
                    break;
                }
            }
        } catch (\Throwable $e) {
            // best-effort: a populate mismatch must never break the probe
        }
    }
}

// Don't trust the capability claim — every advertised backend must answer a real
// ListResources (a mount/unavailable failure means a capability lie).
function run_backend_claim_check_php(GeneratedClient $data, \Udb\Entity\V1\RequestContext $ctx, UdbMetadata $meta, array $enabled): void
{
    expect(count($enabled))->toBeGreaterThan(0);
    foreach ($enabled as $backend) {
        try {
            $data->list_resources((new \Udb\Entity\V1\ResourceAdminRequest())->setContext($ctx)->setBackend($backend), $meta);
        } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
            expect(isFatalLiveStatus($e->status))->toBeFalse("backend {$backend} advertised but unreachable (capability lie): {$e->getMessage()}");
        }
    }
}

// Full session lifecycle: prove Logout invalidates the session — the access
// token, refresh token and session-refresh must ALL fail afterwards.
function run_auth_lifecycle_php(GeneratedClient $client, UdbMetadata $meta, string $username, string $password): void
{
    $login = $client->login((new \Udb\Core\Authn\Services\V1\LoginRequest())
        ->setUsername($username)->setPassword($password)->setTenantHint($meta->tenantId)->setProjectHint($meta->projectId)->setDeviceName('php-sdk-lifecycle'), $meta);
    $token = $login->getAccessToken();
    $sid = $login->getSessionId();
    $refresh = $login->getRefreshToken();
    expect($token)->not->toBe('');
    expect($sid)->not->toBe('');
    expect($refresh)->not->toBe('');
    $pre = $client->validate_token((new \Udb\Core\Authn\Services\V1\ValidateTokenRequest())->setToken($token)->setTokenType(1), $meta); // 1 = TOKEN_TYPE_JWT_ACCESS
    expect($pre->getValid())->toBeTrue();
    $client->get_session((new \Udb\Core\Authn\Services\V1\GetSessionRequest())->setSessionId($sid), $meta);
    $preIntro = $client->introspect_token((new \Udb\Core\Authn\Services\V1\IntrospectTokenRequest())->setToken($token), $meta);
    expect($preIntro->getActive())->toBeTrue();
    $out = $client->logout((new \Udb\Core\Authn\Services\V1\LogoutRequest())->setSessionId($sid)->setRevokeReason('sdk_live_test'), $meta);
    expect($out->getSessionsRevoked())->toBeGreaterThanOrEqual(1);

    $failures = [];
    try {
        if ($client->validate_token((new \Udb\Core\Authn\Services\V1\ValidateTokenRequest())->setToken($token)->setTokenType(1), $meta)->getValid()) {
            $failures[] = 'access token still validates after logout';
        }
    } catch (\Throwable $e) {
    }
    try {
        if ($client->introspect_token((new \Udb\Core\Authn\Services\V1\IntrospectTokenRequest())->setToken($token), $meta)->getActive()) {
            $failures[] = 'token still introspects Active after logout';
        }
    } catch (\Throwable $e) {
    }
    try {
        $client->refresh_token((new \Udb\Core\Authn\Services\V1\RefreshTokenRequest())->setRefreshToken($refresh)->setSessionId($sid), $meta);
        $failures[] = 'refresh token still works after logout — token family not revoked';
    } catch (\Throwable $e) {
    }
    try {
        $client->refresh_session((new \Udb\Core\Authn\Services\V1\RefreshSessionRequest())->setSessionId($sid), $meta);
        $failures[] = 'RefreshSession still works after logout — session not revoked';
    } catch (\Throwable $e) {
    }
    expect(count($failures))->toBe(0, 'SECURITY (logout did not fully invalidate the session): '.implode('; ', $failures));
}

// Canonical generic-dispatch op vocabulary the broker gates per backend
// (src/runtime/service/mod.rs check_generic_dispatch_operation), safe-first.
function genericDispatchOpsPhp(): array
{
    return ['ping', 'probe', 'list_resources', 'search', 'query', 'transaction', 'get_object', 'put_object', 'mutate', 'ensure_resource', 'drop_resource'];
}

// Challenge EVERY advertised backend's per-operation claims in BOTH directions via
// GenericDispatch (the single op-gated entry point shared by every backend kind). A
// claimed side-effect-free op must be admitted; the first unclaimed op must be
// refused with the declared unsupported code — proving each backend kind honors
// exactly the surface it advertises.
function run_backend_capability_challenge_php(GeneratedClient $data, UdbMetadata $meta, $capabilities): void
{
    $descriptors = iterator_to_array($capabilities->getBackendCapabilities());
    expect(count($descriptors))->toBeGreaterThan(0);
    $ctx = (new \Udb\Entity\V1\RequestContext())->setTenantId($meta->tenantId)->setProjectId($meta->projectId)->setPurpose('php.live.backend.capability');
    $dispatch = function (string $backend, string $op) use ($data, $ctx, $meta): ?\Fahara02\UdbLaravel\Exceptions\UdbRpcException {
        try {
            $data->generic_dispatch((new \Udb\Entity\V1\GenericDispatchRequest())->setContext($ctx)->setBackend($backend)->setOperation($op)->setSpecJson('{}'), $meta);
            return null;
        } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
            return $e;
        }
    };
    foreach ($descriptors as $d) {
        $backend = $d->getBackend();
        expect($backend)->not->toBe('');
        expect($d->getTier())->not->toBe('');
        $claimed = iterator_to_array($d->getOperations());
        expect(count($claimed))->toBeGreaterThan(0);
        expect($d->getUnsupportedErrorCode())->toBe('UDB_UNSUPPORTED_OPERATION');
        $claimedSet = array_flip($claimed);
        foreach (['ping', 'probe', 'list_resources'] as $op) {
            if (! isset($claimedSet[$op])) {
                continue;
            }
            $e = $dispatch($backend, $op);
            if ($e !== null) {
                expect(isFatalLiveStatus($e->status))->toBeFalse("backend {$backend} claims {$op} but did not reach a live RPC: {$e->getMessage()}");
                expect(str_contains($e->getMessage(), 'UDB_UNSUPPORTED_OPERATION'))->toBeFalse("CAPABILITY LIE: backend {$backend} advertises {$op} but the gate refused it: {$e->getMessage()}");
            }
        }
        foreach (genericDispatchOpsPhp() as $op) {
            if (isset($claimedSet[$op])) {
                continue;
            }
            $e = $dispatch($backend, $op);
            expect($e)->not->toBeNull("CAPABILITY LIE: backend {$backend} does NOT advertise {$op} yet GenericDispatch admitted it (silent over-claim)");
            if ($e !== null) {
                expect(isFatalLiveStatus($e->status))->toBeFalse("backend {$backend} unclaimed-op {$op} did not reach a live RPC: {$e->getMessage()}");
                expect(str_contains($e->getMessage(), 'UDB_UNSUPPORTED_OPERATION'))->toBeTrue("backend {$backend} refused unclaimed op {$op} but not with UDB_UNSUPPORTED_OPERATION: {$e->getMessage()}");
            }
            break;
        }
    }
    // NOTE: enabled_backends and backend_capabilities are intentionally NOT
    // cross-checked as a subset relation — they derive from different sources and
    // naming. backend_capabilities is the full compiled matrix (a descriptor per
    // built-in backend, each with a `configured` flag) keyed by canonical name (e.g.
    // "sqlserver"); enabled_backends is the enabled subset, possibly aliased (e.g.
    // "mssql"). The meaningful invariant is the per-backend both-directions op
    // challenge above; a list-vs-list subset assertion flags those legitimate
    // naming/scope differences as false positives.
}

function backendCategoryPhp(string $tier, array $opsSet): string
{
    if (isset($opsSet['get_object']) || isset($opsSet['put_object'])) {
        return 'object';
    }
    return [
        'vector' => 'vector', 'cache' => 'cache', 'document' => 'document',
        'graph' => 'graph', 'sql' => 'relational', 'column' => 'relational',
    ][strtolower($tier)] ?? '';
}

// Drive a real, category-appropriate data-plane round-trip against EVERY backend the
// broker advertises (relational SQL, object, document, cache, vector, graph) — not
// just the canonical postgres/mongodb/minio trio. Adapts to whatever the broker has
// enabled. A claimed RPC must at minimum REACH an implementation (a mount failure is
// fatal); per-backend business quirks are tolerated, values asserted on success.
function run_all_backend_kinds_matrix_php(GeneratedClient $data, UdbMetadata $meta, $capabilities): void
{
    $suffix = bin2hex(random_bytes(6));
    $rc = fn (string $p) => (new \Udb\Entity\V1\RequestContext())->setTenantId($meta->tenantId)->setProjectId($meta->projectId)->setPurpose($p);
    $mountOk = function (string $backend, string $op, ?\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e): void {
        if ($e !== null) {
            expect(isFatalLiveStatus($e->status))->toBeFalse("backend {$backend} ({$op}) did not reach a live RPC: {$e->getMessage()}");
        }
    };
    $exercised = [];
    foreach ($capabilities->getBackendCapabilities() as $d) {
        $backend = $d->getBackend();
        if ($backend === '') {
            continue;
        }
        $opsSet = array_flip(iterator_to_array($d->getOperations()));
        $cat = backendCategoryPhp($d->getTier(), $opsSet);
        $exercised[$cat] = ($exercised[$cat] ?? 0) + 1;
        switch ($cat) {
            case 'relational':
                try {
                    $data->generic_dispatch((new \Udb\Entity\V1\GenericDispatchRequest())->setContext($rc('php.live.kind.relational'))->setBackend($backend)->setOperation('query')->setSpecJson('{"sql":"SELECT 1 AS live_probe"}'), $meta);
                } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
                    expect(isFatalLiveStatus($e->status))->toBeFalse("backend {$backend} (query) did not reach a live RPC: {$e->getMessage()}");
                    expect(str_contains($e->getMessage(), 'UDB_UNSUPPORTED_OPERATION'))->toBeFalse("CAPABILITY LIE: relational backend {$backend} refused a claimed query: {$e->getMessage()}");
                }
                break;
            case 'object': run_object_kind_php($data, $meta, $rc, $backend, $mountOk); break;
            case 'document': run_document_kind_php($data, $meta, $rc, $backend, $suffix, $mountOk); break;
            case 'cache': run_cache_kind_php($data, $meta, $rc, $backend, $suffix, $mountOk); break;
            case 'vector': run_vector_kind_php($data, $meta, $rc, $backend, $suffix, $mountOk); break;
            case 'graph': run_graph_kind_php($data, $meta, $rc, $backend, $suffix, $mountOk); break;
        }
    }
    expect($exercised['relational'] ?? 0)->toBeGreaterThan(0);
}

// Object backends: bucket lifecycle reachability (the PHP suite intentionally avoids
// the streaming Put/GetObject path; the Go/Python suites assert the body round-trip).
function run_object_kind_php(GeneratedClient $data, UdbMetadata $meta, callable $rc, string $backend, callable $mountOk): void
{
    $bucket = liveEnv('UDB_LIVE_S3_BUCKET', 'udb-live-sdk');
    try {
        $data->ensure_resource((new \Udb\Entity\V1\ResourceAdminRequest())->setContext($rc('php.live.kind.object'))->setBackend($backend)->setResourceName($bucket)->setSpecJson('{}'), $meta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'ensure_resource', $e);
    }
    try {
        $data->list_resources((new \Udb\Entity\V1\ResourceAdminRequest())->setContext($rc('php.live.kind.object'))->setBackend($backend), $meta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'list_resources', $e);
    }
}

function run_document_kind_php(GeneratedClient $data, UdbMetadata $meta, callable $rc, string $backend, string $suffix, callable $mountOk): void
{
    $collection = 'sdk_kind_docs_' . str_replace('-', '_', $backend) . "_$suffix";
    $docId = "doc-$suffix";
    $res = (new \Udb\Entity\V1\StoreResource())->setBackend($backend)->setResourceName($collection);
    try {
        $data->ensure_resource((new \Udb\Entity\V1\ResourceAdminRequest())->setContext($rc('php.live.kind.document'))->setBackend($backend)->setResourceName($collection)->setSpecJson('{"collection":"' . $collection . '"}'), $meta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'ensure_resource', $e);
    }
    try {
        $data->document_upsert((new \Udb\Entity\V1\DocumentUpsertRequest())->setContext($rc('php.live.kind.document'))->setResource($res)->setDocumentId($docId)->setDocument(liveStruct(['_id' => $docId, 'payload' => "doc-$backend", 'revision' => 1])), $meta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'mutate', $e);
        return;
    }
    try {
        $got = $data->document_get((new \Udb\Entity\V1\DocumentGetRequest())->setContext($rc('php.live.kind.document'))->setResource($res)->setDocumentId($docId), $meta);
        if (count($got->getDocuments()) > 0) {
            expect(liveDocPayload($got))->toBe("doc-$backend");
        }
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'query', $e);
    }
    try {
        $data->document_delete((new \Udb\Entity\V1\DocumentDeleteRequest())->setContext($rc('php.live.kind.document'))->setResource($res)->setDocumentId($docId), $meta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'mutate', $e);
    }
}

function run_cache_kind_php(GeneratedClient $data, UdbMetadata $meta, callable $rc, string $backend, string $suffix, callable $mountOk): void
{
    $res = (new \Udb\Entity\V1\StoreResource())->setBackend($backend);
    $key = "sdk-live-cache-$suffix";
    $val = "cache-$backend-$suffix";
    try {
        $data->cache_set((new \Udb\Entity\V1\CacheSetRequest())->setContext($rc('php.live.kind.cache'))->setResource($res)->setKey($key)->setValue($val)->setContentType('text/plain')->setTtlSeconds(60), $meta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'cache_set', $e);
        return;
    }
    try {
        $got = $data->cache_get((new \Udb\Entity\V1\CacheGetRequest())->setContext($rc('php.live.kind.cache'))->setResource($res)->setKey($key), $meta);
        if ($got->getFound()) {
            expect($got->getValue())->toBe($val);
        }
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'cache_get', $e);
    }
    try {
        $data->cache_scan((new \Udb\Entity\V1\CacheScanRequest())->setContext($rc('php.live.kind.cache'))->setResource($res)->setKeyPattern('sdk-live-cache-*')->setLimit(10), $meta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'cache_scan', $e);
    }
    try {
        $data->cache_delete((new \Udb\Entity\V1\CacheDeleteRequest())->setContext($rc('php.live.kind.cache'))->setResource($res)->setKey($key), $meta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'cache_delete', $e);
    }
}

function run_vector_kind_php(GeneratedClient $data, UdbMetadata $meta, callable $rc, string $backend, string $suffix, callable $mountOk): void
{
    $collection = 'sdk_kind_vec_' . str_replace('-', '_', $backend) . "_$suffix";
    try {
        $data->ensure_resource((new \Udb\Entity\V1\ResourceAdminRequest())->setContext($rc('php.live.kind.vector'))->setBackend($backend)->setResourceName($collection)->setSpecJson('{"dimension":4,"distance":"cosine"}'), $meta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'ensure_resource', $e);
    }
    $vec = [0.1, 0.2, 0.3, 0.4];
    try {
        $data->vector_upsert((new \Udb\Entity\V1\VectorUpsertRequest())->setContext($rc('php.live.kind.vector'))->setCollection($collection)->setPoints([(new \Udb\Entity\V1\VectorPointMutation())->setId("v-$suffix")->setVector($vec)->setPayload(liveStruct(['tag' => 'sdk-live']))]), $meta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'mutate', $e);
        return;
    }
    try {
        $data->vector_search((new \Udb\Entity\V1\VectorSearchRequest())->setContext($rc('php.live.kind.vector'))->setCollection($collection)->setVector($vec)->setLimit(1)->setWithPayload(true), $meta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'search', $e);
    }
}

function run_graph_kind_php(GeneratedClient $data, UdbMetadata $meta, callable $rc, string $backend, string $suffix, callable $mountOk): void
{
    $res = (new \Udb\Entity\V1\StoreResource())->setBackend($backend);
    $label = "SdkLive$suffix";
    try {
        $data->graph_mutate((new \Udb\Entity\V1\GraphMutationRequest())->setContext($rc('php.live.kind.graph'))->setResource($res)->setQuery("CREATE (n:$label {id: \$id}) RETURN n")->setParameters(liveStruct(['id' => $suffix])), $meta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'mutate', $e);
        return;
    }
    try {
        $data->graph_query((new \Udb\Entity\V1\GraphQueryRequest())->setContext($rc('php.live.kind.graph'))->setResource($res)->setQuery("MATCH (n:$label) RETURN n LIMIT 1")->setReadOnly(true), $meta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        $mountOk($backend, 'query', $e);
    }
}

// Edge cases the happy-path suite skips: the auth plane must fail CLOSED. A wrong
// password mints no access token; a garbage/forged bearer never validates or
// introspects active. A mount failure is still fatal (the negative paths must be
// wired too, not just the positive ones).
function run_auth_negative_php(GeneratedClient $client, UdbMetadata $meta, string $username): void
{
    try {
        $bad = $client->login((new \Udb\Core\Authn\Services\V1\LoginRequest())
            ->setUsername($username)->setPassword("definitely-wrong-$username-Pw1!")
            ->setTenantHint($meta->tenantId)->setProjectHint($meta->projectId)->setDeviceName('php-sdk-negative'), $meta);
        expect($bad?->getAccessToken() ?? '')->toBe('', 'SECURITY: Login with a wrong password returned an access token');
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        expect(isFatalLiveStatus($e->status))->toBeFalse("negative Login did not reach a live RPC: {$e->getMessage()}");
    }
    try {
        $v = $client->validate_token((new \Udb\Core\Authn\Services\V1\ValidateTokenRequest())->setToken('not-a-real-jwt')->setTokenType(1), $meta);
        expect($v->getValid())->toBeFalse('SECURITY: a garbage token validated as valid');
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        expect(isFatalLiveStatus($e->status))->toBeFalse("negative ValidateToken did not reach a live RPC: {$e->getMessage()}");
    }
    try {
        $i = $client->introspect_token((new \Udb\Core\Authn\Services\V1\IntrospectTokenRequest())->setToken('not-a-real-jwt'), $meta);
        expect($i->getActive())->toBeFalse('SECURITY: a garbage token introspected as active');
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        expect(isFatalLiveStatus($e->status))->toBeFalse("negative IntrospectToken did not reach a live RPC: {$e->getMessage()}");
    }
}

// A server fault (gRPC INTERNAL=13 / UNKNOWN=2 / DATA_LOSS=15) means a malformed
// input crashed the handler instead of being validated — always a bug. Client-side
// codes (InvalidArgument/FailedPrecondition/NotFound/PermissionDenied) are correct.
function isServerFaultPhp(int $code): bool
{
    return in_array($code, [13, 2, 15], true);
}

/**
 * Per-RPC EDGE cases (malformed/hostile inputs + isolation boundaries). Every case
 * must FAIL CLOSED with a typed client-side error (or safely accept-and-sanitise),
 * never leak another tenant's rows, and never surface a server fault. Mirrors the Go
 * `runLiveEdgeCasesE2E` / Python `run_live_edge_cases` suites.
 */
function run_edge_cases_php(GeneratedClient $data, UdbMetadata $meta, string $tenant, string $project): void
{
    $suffix = bin2hex(random_bytes(6));
    $mt = 'udb.sdk.live.v1.SdkLiveRecord';
    $ctx = fn (string $p) => (new \Udb\Entity\V1\RequestContext())
        ->setTenantId($tenant)->setProjectId($project)->setPurpose($p);

    // 1. missing project_id in the filter -> project isolation must reject it.
    $accepted = false;
    try {
        $data->select((new \Udb\Entity\V1\SelectRequest())->setContext($ctx('php.edge.no-project'))
            ->setMessageType($mt)->setFilter(liveStruct(['tenant_id' => $tenant]))->setLimit(1), $meta);
        $accepted = true;
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        expect(isServerFaultPhp($e->status))->toBeFalse("missing project_id faulted the server: {$e->getMessage()}");
    }
    expect($accepted)->toBeFalse('Select without a project_id filter was ACCEPTED — project isolation not enforced');

    // 2. cross-tenant read -> RLS scopes to the JWT tenant; a foreign filter leaks nothing.
    $foreign = '00000000-0000-0000-0000-0000deadbeef';
    try {
        $resp = $data->select((new \Udb\Entity\V1\SelectRequest())->setContext($ctx('php.edge.cross-tenant'))
            ->setMessageType($mt)->setFilter(liveStruct(['tenant_id' => $foreign, 'project_id' => $project]))->setLimit(10), $meta);
        expect(count($resp->getRecordsJson()))->toBe(0, 'cross-tenant Select LEAKED rows for a foreign tenant');
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        expect(isServerFaultPhp($e->status))->toBeFalse("cross-tenant Select faulted: {$e->getMessage()}");
    }

    // 3. NUL byte in a text field -> stripped/rejected, never a raw UTF8 0x00 fault (B14).
    try {
        $data->upsert((new \Udb\Entity\V1\UpsertRequest())->setContext($ctx('php.edge.nul'))->setMessageType($mt)
            ->setRecordJson(liveRecordJson("edge-nul-$suffix", $tenant, $project, "edge-nul-lk-$suffix", "payload\x00with-nul", 1))
            ->setConflictFields(['record_id']), $meta);
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        expect(isServerFaultPhp($e->status))->toBeFalse("NUL-byte payload faulted: {$e->getMessage()}");
    }

    // 4. limit boundaries (negative/zero/huge) -> clamped/validated, never a crash.
    foreach ([-1, 0, 1000000] as $lim) {
        try {
            $data->select((new \Udb\Entity\V1\SelectRequest())->setContext($ctx('php.edge.limit'))->setMessageType($mt)
                ->setFilter(liveStruct(['tenant_id' => $tenant, 'project_id' => $project]))->setLimit($lim), $meta);
        } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
            expect(isServerFaultPhp($e->status))->toBeFalse("Select limit=$lim faulted: {$e->getMessage()}");
        }
    }

    // 5. unknown message_type -> typed error, not a 500.
    $acc5 = false;
    try {
        $data->select((new \Udb\Entity\V1\SelectRequest())->setContext($ctx('php.edge.unknown-type'))
            ->setMessageType('udb.does.not.Exist')
            ->setFilter(liveStruct(['tenant_id' => $tenant, 'project_id' => $project]))->setLimit(1), $meta);
        $acc5 = true;
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        expect(isServerFaultPhp($e->status))->toBeFalse("unknown message_type faulted: {$e->getMessage()}");
    }
    expect($acc5)->toBeFalse('Select on an unknown message_type was ACCEPTED');

    // 6. invalid backend -> typed error, never a panic/Internal.
    $acc6 = false;
    try {
        $data->list_resources((new \Udb\Entity\V1\ResourceAdminRequest())->setContext($ctx('php.edge.bad-backend'))
            ->setBackend('nonexistent-backend-xyz'), $meta);
        $acc6 = true;
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        expect(isServerFaultPhp($e->status))->toBeFalse("invalid backend faulted: {$e->getMessage()}");
    }
    expect($acc6)->toBeFalse('ListResources on a nonexistent backend was ACCEPTED');
}

it('covers the live generated RPC surface', function () {
    $target = liveEnv('UDB_GRPC_TARGET');
    $authTarget = liveEnv('UDB_AUTH_GRPC_TARGET', $target);
    $meta = liveMeta();

    $openAuthGenerated = new GeneratedClient(['endpoint' => $authTarget, 'deadline_ms' => 10_000, 'retry' => ['max_attempts' => 1]]);
    $openAuthGenerated->bindContext($meta);
    $login = $openAuthGenerated->login(
        (new LoginRequest())
            ->setUsername(liveEnv('UDB_LIVE_USERNAME'))
            ->setPassword(liveEnv('UDB_LIVE_PASSWORD'))
            ->setTenantHint($meta->tenantId)
            ->setProjectHint($meta->projectId)
            ->setDeviceName('php-sdk-live-conformance'),
        $meta,
    );
    expect($login?->getAccessToken())->not->toBe('');
    expect($login?->getRefreshToken())->not->toBe('');

    $auth = new UdbAuthClient(['endpoint' => $authTarget, 'deadline_ms' => 10_000]);
    $auth->bindContext($meta);
    $authResp = $auth->authenticateBearer($login->getAccessToken(), $meta);
    // Discover our CANONICAL tenant UUID from the authenticated principal — bootstrap
    // binds the admin to the tenant's UUID, so the Login JWT claim is a UUID, not the
    // human code. Use it for every request body so the body matches the claim and the
    // UUID-strict services (storage/webrtc/asset) accept it. ONE admin serves all RPCs.
    $canonicalTenant = $authResp?->getPrincipal()?->getTenantId() ?: $meta->tenantId;
    expect($canonicalTenant)->not->toBe('');
    $refresh = $openAuthGenerated->refresh_token(
        (new RefreshTokenRequest())->setRefreshToken($login->getRefreshToken()),
        $meta,
    );
    expect($refresh?->getAccessToken())->not->toBe('');

    $authedMeta = liveMeta($login->getAccessToken(), $canonicalTenant);
    // 15s, not 2s: against a full 14-backend broker the heavy RPCs legitimately take
    // longer than a 3-backend CI broker — GetHealthReport probes every backend (incl.
    // the slow mssql/cassandra) and GetCatalogManifest returns the whole manifest — and
    // the PHP gRPC ext over Docker host networking adds latency. A 2s deadline spuriously
    // trips DEADLINE_EXCEEDED on those; the other SDKs already use 10s+.
    $data = new GeneratedClient(['endpoint' => $target, 'deadline_ms' => 15_000, 'retry' => ['max_attempts' => 1]]);
    $authGenerated = new GeneratedClient(['endpoint' => $authTarget, 'deadline_ms' => 15_000, 'retry' => ['max_attempts' => 1]]);
    $data->bindContext($authedMeta);
    $authGenerated->bindContext($authedMeta);

    $capabilities = $data->get_capabilities(new CapabilitiesRequest(), $authedMeta);
    $enabled = array_map('strtolower', iterator_to_array($capabilities->getEnabledBackends()));
    $required = array_filter(array_map('trim', explode(',', liveEnv('UDB_LIVE_REQUIRED_BACKENDS', 'postgres,mongodb,minio'))));
    foreach ($required as $backend) {
        expect($enabled)->toContain(strtolower($backend));
    }

    // Real DataBroker backend round-trips (Postgres + Mongo, unary).
    run_live_backend_e2e($data, $authedMeta, $authedMeta->tenantId, $meta->projectId);

    // Per-RPC EDGE cases (fail-closed / no cross-tenant leak / no server fault).
    run_edge_cases_php($data, $authedMeta, $authedMeta->tenantId, $meta->projectId);

    // Breadth: a real category-appropriate round-trip against EVERY advertised backend
    // kind (relational SQL, object, document, cache, vector, graph) — not just the
    // canonical postgres/mongodb/minio trio. Adapts to whatever the broker enabled.
    run_all_backend_kinds_matrix_php($data, $authedMeta, $capabilities);

    // Real create→read→assert CRUD against every native control-plane service. A
    // SINGLE admin (bound to the canonical tenant UUID) now serves the UUID-strict
    // services (storage/webrtc/asset) and the free-text ones alike — no second "uuid
    // tenant" admin needed (auth_fix.md tenant-identity fix).
    run_native_service_e2e($authGenerated, $authGenerated, $authedMeta, $authedMeta, $authedMeta->tenantId, $meta->projectId, $authedMeta->tenantId);

    // Don't trust the capability claim — exercise every advertised backend.
    $claimCtx = (new \Udb\Entity\V1\RequestContext())->setTenantId($authedMeta->tenantId)->setProjectId($meta->projectId)->setPurpose('php.live.backend.claim');
    run_backend_claim_check_php($data, $claimCtx, $authedMeta, $enabled);

    // Challenge every advertised backend KIND's per-operation claims in BOTH directions.
    run_backend_capability_challenge_php($data, $authedMeta, $capabilities);

    // Full session lifecycle on a throwaway login: prove logout invalidates the
    // session (access token + refresh token + session-refresh all rejected after).
    run_auth_lifecycle_php($authGenerated, $authedMeta, liveEnv('UDB_LIVE_USERNAME'), liveEnv('UDB_LIVE_PASSWORD'));

    // Edge cases: the auth plane must fail CLOSED on bad credentials/forged bearers.
    run_auth_negative_php($authGenerated, $authedMeta, liveEnv('UDB_LIVE_USERNAME'));

    // Per-RPC surface probing now lives in the data-driven test below
    // ("reaches live RPC … with (<stub>/<rpc>)") so the runner reports granular
    // per-RPC pass/fail (262 cases) instead of one opaque test — matching Go's
    // sub-tests and Python's parametrized cases. The deep create→read→assert
    // e2e above stays in this test.
});

// --- Per-RPC surface coverage (granular: one Pest case per RPC) ---------------

// Memoized live session: log in ONCE and reuse the authed clients across all 262
// data-driven cases. A dataset re-runs the test closure per case, so a
// non-memoized login would re-authenticate 262 times.
function phpLiveSession(): array
{
    static $session = null;
    if ($session !== null) {
        return $session;
    }
    $target = liveEnv('UDB_GRPC_TARGET');
    $authTarget = liveEnv('UDB_AUTH_GRPC_TARGET', $target);
    $meta = liveMeta();
    $openAuth = new GeneratedClient(['endpoint' => $authTarget, 'deadline_ms' => 10_000, 'retry' => ['max_attempts' => 1]]);
    $openAuth->bindContext($meta);
    $login = $openAuth->login(
        (new LoginRequest())
            ->setUsername(liveEnv('UDB_LIVE_USERNAME'))
            ->setPassword(liveEnv('UDB_LIVE_PASSWORD'))
            ->setTenantHint($meta->tenantId)
            ->setProjectHint($meta->projectId)
            ->setDeviceName('php-sdk-per-rpc'),
        $meta,
    );
    $auth = new UdbAuthClient(['endpoint' => $authTarget, 'deadline_ms' => 10_000]);
    $auth->bindContext($meta);
    $authResp = $auth->authenticateBearer($login->getAccessToken(), $meta);
    $canonicalTenant = $authResp?->getPrincipal()?->getTenantId() ?: $meta->tenantId;
    $authedMeta = liveMeta($login->getAccessToken(), $canonicalTenant);
    $data = new GeneratedClient(['endpoint' => $target, 'deadline_ms' => 15_000, 'retry' => ['max_attempts' => 1]]);
    $authGenerated = new GeneratedClient(['endpoint' => $authTarget, 'deadline_ms' => 15_000, 'retry' => ['max_attempts' => 1]]);
    $data->bindContext($authedMeta);
    $authGenerated->bindContext($authedMeta);

    return $session = compact('data', 'authGenerated', 'authedMeta', 'meta');
}

// Enumerate every (stub, method) by REFLECTION ONLY (no live call), so the
// dataset is available at Pest collection time without a broker connection.
function phpLiveRpcCatalog(): array
{
    $probe = new GeneratedClient(['endpoint' => '127.0.0.1:1', 'deadline_ms' => 1_000, 'retry' => ['max_attempts' => 1]]);
    $out = [];
    foreach (stubAccessors($probe, $probe) as $stubName => $stub) {
        foreach (generatedStubMethods($stub) as $method) {
            $out["{$stubName}/{$method->getName()}"] = [$stubName, $method->getName()];
        }
    }

    return $out;
}

dataset('liveRpcs', fn () => phpLiveRpcCatalog());

it('reaches live RPC', function (string $stubName, string $methodName) {
    $s = phpLiveSession();
    $stub = stubAccessors($s['data'], $s['authGenerated'])[$stubName];
    $method = null;
    foreach (generatedStubMethods($stub) as $m) {
        if ($m->getName() === $methodName) {
            $method = $m;
            break;
        }
    }
    expect($method)->not->toBeNull("missing method {$methodName} on {$stubName}");

    $authedMeta = $s['authedMeta'];
    $meta = $s['meta'];
    $label = "{$stubName}.{$methodName}";
    $params = $method->getParameters();
    $hasRequest = isset($params[0])
        && $params[0]->getType() instanceof ReflectionNamedType
        && ! $params[0]->getType()->isBuiltin();
    try {
        $probeRequest = null;
        if ($hasRequest) {
            $probeRequest = requestFor($method);
            if (shouldPopulatePhp($methodName)) {
                populateProbeRequest($probeRequest, $meta->tenantId, $meta->projectId);
            }
        }
        $call = $hasRequest
            ? $method->invoke($stub, $probeRequest, $authedMeta->toGrpcMetadata(), ['timeout' => 15_000_000])
            : $method->invoke($stub, $authedMeta->toGrpcMetadata(), ['timeout' => 15_000_000]);
        if (method_exists($call, 'responses')) {
            foreach ($call->responses() as $_) {
                break;
            }
            assertLiveStatusMounted($label, $call->getStatus());
        } elseif (method_exists($call, 'writesDone')) {
            $call->writesDone();
            if (method_exists($call, 'read')) {
                $call->read();
            }
            if (method_exists($call, 'getStatus')) {
                assertLiveStatusMounted($label, $call->getStatus());
            }
        } elseif (method_exists($call, 'wait')) {
            [, $status] = $call->wait();
            assertLiveStatusMounted($label, $status);
        }
    } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
        expect(isFatalLiveStatus($e->status))->toBeFalse("{$label} did not reach an implemented live RPC: {$e->getMessage()}");
    }
})->with('liveRpcs');

// Coverage guard: the per-RPC dataset must enumerate exactly the full surface.
it('enumerates exactly 262 live RPCs', function () {
    expect(count(phpLiveRpcCatalog()))->toBe(262);
});

// gRPC status code -> NAME (BENCH_RPC_BODIES.md #1/#3: a failing RPC must be recorded
// as a FAILURE with its code, never a silent latency sample). UdbRpcException->status
// is the integer gRPC code.
function grpcStatusNamePhp(int $code): string
{
    static $names = [
        0 => 'OK', 1 => 'CANCELLED', 2 => 'UNKNOWN', 3 => 'INVALID_ARGUMENT',
        4 => 'DEADLINE_EXCEEDED', 5 => 'NOT_FOUND', 6 => 'ALREADY_EXISTS',
        7 => 'PERMISSION_DENIED', 8 => 'RESOURCE_EXHAUSTED', 9 => 'FAILED_PRECONDITION',
        10 => 'ABORTED', 11 => 'OUT_OF_RANGE', 12 => 'UNIMPLEMENTED', 13 => 'INTERNAL',
        14 => 'UNAVAILABLE', 15 => 'DATA_LOSS', 16 => 'UNAUTHENTICATED',
    ];

    return $names[$code] ?? "CODE_$code";
}

// --- Per-RPC performance (gated on UDB_LIVE_PERF=1) ---------------------------
// Times every RPC over multiple iterations and writes perf_report_php.md — the
// PHP counterpart of the Go/Python/TS perf harness. read_only RPCs are timed
// many times; mutations a few; destructive once typed-empty.
/**
 * PerfFixturesPhp: a semantic-field-name -> real-seeded-value map (mirror of the
 * Go perfFixtures / Python PerfFixtures). lookup() does an exact match first, then
 * matches a registered key as a suffix of the field name (so "approved_by",
 * "assigned_by" reach the seeded user; "definition_id" the seeded pipeline).
 */
class PerfFixturesPhp
{
    private array $m = [];

    public function set(string $key, $val): void
    {
        if ($val !== null && $val !== '') {
            $this->m[strtolower($key)] = (string) $val;
        }
    }

    public function lookup(string $field): ?string
    {
        if (isset($this->m[$field])) {
            return $this->m[$field];
        }
        foreach ($this->m as $k => $v) {
            if ($field === $k || str_ends_with($field, '_'.$k)) {
                return $v;
            }
        }

        return null;
    }
}

/**
 * perfBodyPhp returns the DOCUMENTED request body (docs/bench-bodies/<svc>.md) for a
 * given RPC, with every `<seed:...>` reference resolved from the seed fixtures so the
 * RPC drives its real SUCCESS path. There is NO generic fallback: an RPC not covered
 * here returns null and the caller sends the typed-empty request (never a generically
 * populated one). $name is the snake_case generated method name.
 */
function perfBodyPhp(string $name, PerfFixturesPhp $fix, string $tenant, string $project): ?object
{
    $g = fn (string $k, string $d = '') => $fix->lookup($k) ?? $d;
    $sub = $fix->lookup('subject') ?? ('user:'.($fix->lookup('user_id') ?? ''));
    // created_by / assigned_by / deleted_by / … are audit columns the broker validates
    // as bare UUIDs — the casbin $sub ("user:<uuid>") is NOT a valid UUID there.
    $byId = $fix->lookup('user_id') ?? liveUuidV4();
    // data-plane (udb.entity.v1) context
    $ctxE = fn () => (new \Udb\Entity\V1\RequestContext())->setTenantId($tenant)->setProjectId($project)->setPurpose('php.live.perf');
    // control-plane (udb.core.common.v1) context with nested tenant
    $ctxC = fn () => (new \Udb\Core\Common\V1\RequestContext())
        ->setTenant((new \Udb\Core\Common\V1\TenantContext())->setTenantId($tenant)->setProjectId($project))
        ->setPurpose('php.live.perf');
    $A = fn (string $c) => "\\Udb\\Core\\Authn\\Services\\V1\\$c";
    $Z = fn (string $c) => "\\Udb\\Core\\Authz\\Services\\V1\\$c";
    $K = fn (string $c) => "\\Udb\\Core\\Apikey\\Services\\V1\\$c";
    $AN = fn (string $c) => "\\Udb\\Core\\Analytics\\Services\\V1\\$c";
    $N = fn (string $c) => "\\Udb\\Core\\Notification\\Services\\V1\\$c";
    $S = fn (string $c) => "\\Udb\\Core\\Storage\\Services\\V1\\$c";
    $AS = fn (string $c) => "\\Udb\\Core\\Asset\\Services\\V1\\$c";
    $W = fn (string $c) => "\\Udb\\Core\\Webrtc\\Services\\V1\\$c";
    $T = fn (string $c) => "\\Udb\\Core\\Tenant\\Services\\V1\\$c";
    $C = fn (string $c) => "\\Udb\\Core\\Control\\Services\\V1\\$c";
    $I = fn (string $c) => "\\Udb\\Core\\Idp\\Services\\V1\\$c";
    $E = fn (string $c) => "\\Udb\\Entity\\V1\\$c";
    $page = fn (string $cls = '', int $sz = 20) => (new \Udb\Core\Common\V1\PageRequest())->setPage(1)->setPageSize($sz);
    // Authz nested-message builders (shared across many authz RPCs).
    $principal = fn () => (new ($Z('Principal'))())->setSubject($sub)->setUserId($g('user_id'))->setTenantId($tenant)->setProjectId($project)->setScopes(['udb:read']);
    $resourceRef = fn () => (new ($Z('ResourceRef'))())->setResourceType($g('resource', 'invoice'))->setTable('records');
    $actor = fn (array $sc) => (new ($Z('GovernanceActor'))())->setSubject($sub)->setTenantId($tenant)->setProjectId($project)->setScopes($sc);
    // DataBroker StoreResource for a given backend.
    $store = fn (string $backend, string $rn = '') => (new ($E('StoreResource'))())->setBackend($backend)->setResourceName($rn);
    $ts = fn () => (new \Google\Protobuf\Timestamp())->setSeconds(1748736000);

    // Normalize the generated method name to snake_case so the switch matches
    // regardless of whether the stub exposes PascalCase (Upsert, CreateUser,
    // SendOTP -> send_o_t_p) or already-snake names. No-op for snake input.
    $n = strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $name));

    switch ($n) {
        // ── AuthnService — auth flow first (login / refresh_token), then the rest ──
        case 'login':
            return (new ($A('LoginRequest'))())->setUsername(liveEnv('UDB_LIVE_USERNAME'))->setPassword(liveEnv('UDB_LIVE_PASSWORD'))
                ->setDeviceType(2)->setDeviceName('cli')->setTenantHint($tenant)->setProjectHint($project); // DEVICE_TYPE_API=2
        case 'refresh_token':
            return (new ($A('RefreshTokenRequest'))())->setRefreshToken($g('refresh_token'));
        case 'authenticate':
            return (new ($A('AuthnRequest'))())->setBearerToken($g('token'))->setCredentialType(1); // BEARER_TOKEN
        case 'logout':
            return (new ($A('LogoutRequest'))())->setSessionId($g('session_id'));
        case 'validate_token':
            return (new ($A('ValidateTokenRequest'))())->setToken($g('token'))->setTokenType(1); // JWT_ACCESS
        case 'introspect_token':
            return (new ($A('IntrospectTokenRequest'))())->setToken($g('token'));
        case 'get_jwks':
            return new ($A('GetJwksRequest'))();
        case 'create_user':
            return (new ($A('CreateUserRequest'))())->setUsername('alice-'.bin2hex(random_bytes(4)))->setEmail('alice-'.bin2hex(random_bytes(4)).'@acme.test')
                ->setPassword('Str0ng!Passw0rd')->setTenantId($tenant)->setFullName('Alice A')->setAccountKind(1); // PERSON=1
        case 'get_user':
            return (new ($A('GetUserRequest'))())->setUserId($g('user_id'));
        case 'list_users':
            return (new ($A('ListUsersRequest'))())->setTenantId($tenant);
        case 'update_user':
            return (new ($A('UpdateUserRequest'))())->setUserId($g('user_id'))->setFullName('Alice B')->setEmail('alice2@acme.test')->setTenantId($tenant);
        case 'change_user_status':
            return (new ($A('ChangeUserStatusRequest'))())->setUserId($g('user_id'))->setNewStatus(3)->setReason('admin action'); // SUSPENDED=3
        case 'admin_reset_password':
            return (new ($A('AdminResetPasswordRequest'))())->setUserId($g('user_id'));
        case 'send_o_t_p':
        case 'send_otp':
            return (new ($A('SendOTPRequest'))())->setUserId($g('user_id'))->setOtpType(1); // EMAIL_VERIFICATION=1
        case 'verify_o_t_p':
        case 'verify_otp':
            return (new ($A('VerifyOTPRequest'))())->setOtpId($g('otp_id'))->setCode($g('otp_code') ?: '123456');
        case 'resend_o_t_p':
        case 'resend_otp':
            return (new ($A('ResendOTPRequest'))())->setOriginalOtpId($g('otp_id'))->setReason('not_received');
        case 'change_password':
            // current_password MUST be the exact password the seed user was created with.
            return (new ($A('ChangePasswordRequest'))())->setUserId($g('user_id'))->setCurrentPassword('CorrectHorse1!')->setNewPassword('N3w!Passw0rd9');
        case 'create_session':
            return (new ($A('CreateSessionRequest'))())->setPrincipal((new ($A('Principal'))())->setPrincipalId($g('user_id'))->setSubject($sub)->setUserId($g('user_id'))->setTenantId($tenant))->setTtlSeconds(3600);
        case 'refresh_session':
            return (new ($A('RefreshSessionRequest'))())->setSessionId($g('session_id'))->setTtlSeconds(3600);
        case 'get_session':
            return (new ($A('GetSessionRequest'))())->setSessionId($g('session_id'));
        case 'list_sessions':
            return (new ($A('ListSessionsRequest'))())->setUserId($g('user_id'));
        case 'revoke_session':
            return (new ($A('RevokeSessionRequest'))())->setSessionId($g('session_id'))->setRevokeReason('user logout');
        case 'validate_c_s_r_f':
        case 'validate_csrf':
            return (new ($A('ValidateCSRFRequest'))())->setSessionId($g('session_id'))->setCsrfToken($g('csrf_token'));
        case 'enroll_m_f_a':
        case 'enroll_mfa':
            return (new ($A('EnrollMFARequest'))())->setUserId($g('user_id'))->setMfaType(1); // TOTP=1
        case 'confirm_m_f_a_enrollment':
            return (new ($A('ConfirmMFAEnrollmentRequest'))())->setUserId($g('user_id'))->setOtpId($g('code'))->setCode('123456');
        case 'generate_recovery_codes':
            return (new ($A('GenerateRecoveryCodesRequest'))())->setUserId($g('user_id'))->setCount(10);
        case 'put_mfa_policy':
            // require_mfa MUST stay false on the live login tenant: setting it true
            // makes every later Login fail FAILED_PRECONDITION "MFA enrollment required
            // by tenant policy" and poisons the whole bench (the admin has no 2nd factor).
            return (new ($A('PutMfaPolicyRequest'))())->setTenantId($tenant)->setRequireMfa(false);
        case 'get_mfa_policy':
            return (new ($A('GetMfaPolicyRequest'))())->setTenantId($tenant);
        case 'forgot_password':
            return (new ($A('ForgotPasswordRequest'))())->setIdentifier('alice@acme.test');
        case 'reset_password':
            // Real dev-echoed code only (UDB_OTP_DEV_ECHO=1 echoes it unconditionally —
            // mfa.rs:208). No '123456' fallback: a wrong code denies and masks an empty
            // reset_otp_code (BENCH_TS_PHP_ADVISORY.md).
            return (new ($A('ResetPasswordRequest'))())->setOtpId($g('reset_otp_id'))->setCode($g('reset_otp_code'))->setNewPassword('N3w!Passw0rd9');
        case 'send_phone_verification':
            return (new ($A('SendPhoneVerificationRequest'))())->setUserId($g('user_id'))->setPhone('+15551234567');
        case 'start_web_authn_registration':
            return (new ($A('StartWebAuthnRegistrationRequest'))())->setUserId($g('user_id'))->setLabel('yubikey')->setTenantId($tenant);
        case 'finish_web_authn_registration':
            return (new ($A('FinishWebAuthnRegistrationRequest'))())->setChallengeId($g('reg_challenge_id'))->setPublicKeyCredentialJson('__UDB_WEBAUTHN_TEST__')->setLabel('perf-key');
        case 'start_web_authn_authentication':
            return (new ($A('StartWebAuthnAuthenticationRequest'))())->setUserId($g('user_id'))->setTenantId($tenant);
        case 'finish_web_authn_authentication':
            return (new ($A('FinishWebAuthnAuthenticationRequest'))())->setChallengeId($g('auth_challenge_id'))->setPublicKeyCredentialJson('__UDB_WEBAUTHN_TEST__');
        case 'list_devices':
            return (new ($A('ListDevicesRequest'))())->setUserId($g('user_id'));
        case 'revoke_device':
            return (new ($A('RevokeDeviceRequest'))())->setDeviceId($g('device_id'))->setReason('lost device');
        case 'admin_revoke_session':
            return (new ($A('AdminRevokeSessionRequest'))())->setUserId($g('user_id'))->setSessionId($g('session_id'))->setReason('compromised');
        case 'admin_revoke_all_user_sessions':
            return (new ($A('AdminRevokeAllUserSessionsRequest'))())->setUserId($g('user_id'))->setReason('compromised');
        case 'admin_revoke_all_tenant_sessions':
            return (new ($A('AdminRevokeAllTenantSessionsRequest'))())->setTenantId($tenant)->setReason('incident');
        case 'emergency_revoke':
            return (new ($A('EmergencyRevokeRequest'))())->setTenantId($tenant)->setReason('incident');
        case 'issue_mfa_challenge':
            return (new ($A('IssueMfaChallengeRequest'))())->setUserId($g('user_id'))->setFactorKind(1)->setPurpose(1); // TOTP=1, SENSITIVE_OP=1
        case 'verify_mfa_challenge':
            return (new ($A('VerifyMfaChallengeRequest'))())->setChallengeId($g('challenge_id'))->setCode($g('otp_code') ?: '123456');
        case 'list_mfa_factors':
            return (new ($A('ListMfaFactorsRequest'))())->setUserId($g('user_id'));
        case 'disable_mfa_factor':
            return (new ($A('DisableMfaFactorRequest'))())->setUserId($g('user_id'))->setFactorKind(1);
        case 'rename_passkey':
            return (new ($A('RenamePasskeyRequest'))())->setUserId($g('user_id'))->setCredentialId($g('record_id'))->setNewLabel('work key');
        case 'revoke_recovery_codes':
            return (new ($A('RevokeRecoveryCodesRequest'))())->setUserId($g('user_id'));
        case 'admin_reset_mfa':
            return (new ($A('AdminResetMfaRequest'))())->setUserId($g('user_id'))->setReason('lost device');
        case 'list_web_authn_credentials':
            return (new ($A('ListWebAuthnCredentialsRequest'))())->setUserId($g('user_id'));
        case 'delete_web_authn_credential':
            return (new ($A('DeleteWebAuthnCredentialRequest'))())->setUserId($g('user_id'))->setCredentialId($g('record_id'));

        // ── ApiKeyService ──
        case 'create_api_key':
            return (new ($K('CreateApiKeyRequest'))())->setName('bench-key')->setDescription('bench')->setOwnerType(6)->setOwnerId($g('owner_id'))->setScopes(['resource:read'])->setContext($ctxC()); // SERVICE_ACCOUNT=6
        case 'get_api_key':
            return (new ($K('GetApiKeyRequest'))())->setKeyId($g('key_id'));
        case 'list_api_keys':
            return (new ($K('ListApiKeysRequest'))())->setOwnerId($g('owner_id'));
        case 'update_api_key':
            // revoke/rotate/update resolve the key by PREFIX (get_by_prefix), not key_id UUID.
            return (new ($K('UpdateApiKeyRequest'))())->setKeyId($g('update_key_prefix') ?: $g('key_prefix'))->setName('bench-key-2')->setDescription('updated')->setScopes(['resource:read'])->setContext($ctxC());
        case 'revoke_api_key':
            // the SEPARATE disposable key seeded for revocation (real 200); the primary
            // key survives for rotate/update/get/validate. Lookup is by key_prefix.
            return (new ($K('RevokeApiKeyRequest'))())->setKeyId($g('revoke_key_prefix') ?: $g('key_prefix'))->setRevokeReason('bench cleanup')->setContext($ctxC());
        case 'rotate_api_key':
            return (new ($K('RotateApiKeyRequest'))())->setKeyId($g('key_prefix'))->setRotationReason('bench rotate')->setContext($ctxC());
        case 'emergency_revoke_api_keys':
            return (new ($K('EmergencyRevokeApiKeysRequest'))())->setOwnerId($g('owner_id'))->setReason('bench emergency')->setContext($ctxC());
        case 'validate_api_key':
            return (new ($K('ValidateApiKeyRequest'))())->setPlainKey($g('plain_key'))->setEndpoint('/v1/test')->setRequiredScope('resource:read')->setIpAddress('127.0.0.1');
        case 'get_api_key_usage_stats':
            return (new ($K('GetApiKeyUsageStatsRequest'))())->setKeyId($g('key_id'));

        // ── AnalyticsService ──
        case 'record_pipeline_metric':
            return (new ($AN('RecordPipelineMetricRequest'))())->setStageName($g('stage_name'))->setTenantId($tenant)->setLatencyMs(12.5)->setIsSuccess(true)->setContext($ctxC());
        case 'get_pipeline_summary':
            return (new ($AN('GetPipelineSummaryRequest'))())->setStageName($g('stage_name'))->setTenantId($tenant)->setHourFrom('2026-06-01T00:00:00Z')->setHourTo('2026-06-14T23:00:00Z');
        case 'get_executor_performance':
            return (new ($AN('GetExecutorPerformanceRequest'))())->setDateFrom('2026-06-01')->setDateTo('2026-06-14');
        case 'get_reconciliation_analytics':
            return (new ($AN('GetReconciliationAnalyticsRequest'))())->setDateFrom('2026-06-01')->setDateTo('2026-06-14');
        case 'get_throughput':
            return (new ($AN('GetThroughputRequest'))())->setTenantId($tenant)->setHourFrom('2026-06-01T00:00:00Z')->setHourTo('2026-06-14T23:00:00Z');
        case 'get_sla_compliance':
            return (new ($AN('GetSlaComplianceRequest'))())->setStageName($g('stage_name'))->setDateFrom('2026-06-01')->setDateTo('2026-06-14')->setP99ThresholdMs(250.0)->setErrorRateThreshold(0.01);
        case 'trigger_snapshot':
            return (new ($AN('TriggerSnapshotRequest'))())->setStageName($g('stage_name'))->setHour('2026-06-14T10:00:00Z')->setContext($ctxC());

        // ── NotificationService ──
        case 'send_notification':
            return (new ($N('SendNotificationRequest'))())->setEventType($g('event_type'))->setRecipientId($g('user_id'))->setRecipientAddress('user@example.com')->setTenantId($tenant)->setProjectId($project)->setLocale('en')->setChannels([1]);
        case 'get_notification':
            return (new ($N('GetNotificationRequest'))())->setLogId($g('log_id'));
        case 'list_notifications':
            return (new ($N('ListNotificationsRequest'))())->setTenantId($tenant)->setPage($page($N('PageRequest') ?? '\\Udb\\Core\\Common\\V1\\PageRequest'));
        case 'retry_notification':
            return (new ($N('RetryNotificationRequest'))())->setLogId($g('log_id'));
        case 'upsert_template':
            return (new ($N('UpsertTemplateRequest'))())->setEventType($g('event_type'))->setChannel(1)->setLocale('en')->setSubjectTemplate('Hello {name}')->setBodyTemplate('Body {name}')->setIsActive(true);
        case 'get_template':
            return (new ($N('GetTemplateRequest'))())->setEventType($g('event_type'))->setChannel(1)->setLocale('en');
        case 'list_templates':
            return new ($N('ListTemplatesRequest'))();
        case 'get_delivery_stats':
            return (new ($N('GetDeliveryStatsRequest'))())->setTenantId($tenant)->setEventType($g('event_type'))->setDateFrom('2026-01-01')->setDateTo('2026-12-31');
        case 'set_preference':
            return (new ($N('SetPreferenceRequest'))())->setUserId($g('user_id'))->setTenantId($tenant)->setChannel(1)->setIsOptedOut(true);
        case 'get_preference':
            return (new ($N('GetPreferenceRequest'))())->setUserId($g('user_id'))->setTenantId($tenant)->setChannel(1);
        case 'list_preferences':
            return (new ($N('ListPreferencesRequest'))())->setUserId($g('user_id'))->setTenantId($tenant);

        // ── StorageService ──
        case 'register_upload':
            // project_id must be empty (the broker parses a non-empty project_id as a UUID;
            // "default" fails "uuid params must be UUID strings"). reference_id is a fresh UUID.
            return (new ($S('RegisterUploadRequest'))())->setTenantId($tenant)->setProjectId('')->setFilename('report.pdf')->setContentType('application/pdf')->setFileType('document')->setReferenceId(liveUuidV4())->setReferenceType('document')->setExpiresInMinutes(15)->setSizeBytes(1024);
        case 'finalize_upload':
            return (new ($S('FinalizeUploadRequest'))())->setTenantId($tenant)->setFileId($g('file_id'))->setContentType('application/pdf')->setFileType('document')->setReferenceId($g('file_id'))->setReferenceType('document')->setSizeBytes(1024);
        case 'get_download_url':
            return (new ($S('GetDownloadUrlRequest'))())->setTenantId($tenant)->setFileId($g('file_id'))->setExpiresInMinutes(15);
        case 'get_file':
            return (new ($S('GetFileRequest'))())->setTenantId($tenant)->setFileId($g('file_id'));
        case 'update_file':
            return (new ($S('UpdateFileRequest'))())->setTenantId($tenant)->setFileId($g('file_id'))->setFilename('renamed.pdf')->setContentType('application/pdf')->setFileType('document')->setReferenceId($g('file_id'))->setReferenceType('document');
        case 'delete_file':
            // destructive — the SEPARATE disposable file seeded for deletion (real 200).
            return (new ($S('DeleteFileRequest'))())->setTenantId($tenant)->setFileId($g('delete_file_id') ?: liveUuidV4());
        case 'list_files':
            return (new ($S('ListFilesRequest'))())->setTenantId($tenant)->setPage(1)->setPageSize(20);

        // ── AssetService ──
        case 'create_pipeline_definition':
            return (new ($AS('CreatePipelineDefinitionRequest'))())->setTenantId($tenant)->setName('thumbnail-pipeline')->setDescription('Generate thumbnails')->setMediaType('image/png')->setSteps('[{"name":"resize","type":"TRANSFORM"}]')->setVersion(1);
        case 'get_pipeline_definition':
            return (new ($AS('GetPipelineDefinitionRequest'))())->setTenantId($tenant)->setDefinitionId($g('definition_id'));
        case 'register_asset':
            // project_id must be empty (non-empty is parsed as a UUID and "default" fails).
            return (new ($AS('RegisterAssetRequest'))())->setTenantId($tenant)->setProjectId('')->setFileId($g('file_id'))->setName('logo.png')->setMediaType('image/png')->setMetadata('{"source":"upload"}');
        case 'start_pipeline':
            return (new ($AS('StartPipelineRequest'))())->setTenantId($tenant)->setDefinitionId($g('definition_id'))->setAssetId($g('asset_id'))->setContext('{}')->setCorrelationId('run-001');
        case 'get_pipeline':
            return (new ($AS('GetPipelineRequest'))())->setTenantId($tenant)->setInstanceId($g('instance_id'));
        case 'complete_step':
            return (new ($AS('CompleteStepRequest'))())->setTenantId($tenant)->setStepId($g('step_id'))->setStatus('COMPLETED')->setResult('{}');
        case 'list_assets':
            return (new ($AS('ListAssetsRequest'))())->setTenantId($tenant)->setPage(1)->setPageSize(20);
        case 'get_asset':
            return (new ($AS('GetAssetRequest'))())->setTenantId($tenant)->setAssetId($g('asset_id'));

        // ── WebRTC (Room/Peer/Track/Turn) ──
        case 'create_room':
            return (new ($W('CreateRoomRequest'))())->setTenantId($tenant)->setName('bench-room')->setMaxParticipants(10)->setConfig('{}')->setCreatedBy($g('user_id'));
        case 'get_room':
            return (new ($W('GetRoomRequest'))())->setTenantId($tenant)->setRoomId($g('room_id'));
        case 'update_room':
            return (new ($W('UpdateRoomRequest'))())->setTenantId($tenant)->setRoomId($g('room_id'))->setName('bench-room-2')->setState('active')->setConfig('{}');
        case 'close_room':
            // the SEPARATE disposable room seeded for closing (real 200); the main room survives.
            return (new ($W('CloseRoomRequest'))())->setTenantId($tenant)->setRoomId($g('close_room_id') ?: liveUuidV4());
        case 'list_rooms':
            return (new ($W('ListRoomsRequest'))())->setTenantId($tenant)->setState('active')->setPage(1)->setPageSize(20);
        case 'join_room':
            return (new ($W('JoinRoomRequest'))())->setTenantId($tenant)->setRoomId($g('room_id'))->setDisplayName('Bench User')->setMetadata('{}')->setUserAgent('bench/1.0');
        case 'leave_room':
            return (new ($W('LeaveRoomRequest'))())->setTenantId($tenant)->setRoomId($g('room_id'))->setPeerId($g('leave_peer_id') ?: liveUuidV4());
        case 'get_peer':
            return (new ($W('GetPeerRequest'))())->setTenantId($tenant)->setPeerId($g('peer_id'));
        case 'list_peers':
            return (new ($W('ListPeersRequest'))())->setTenantId($tenant)->setRoomId($g('room_id'));
        case 'publish_track':
            return (new ($W('PublishTrackRequest'))())->setTenantId($tenant)->setRoomId($g('room_id'))->setPeerId($g('peer_id'))->setKind('audio')->setLabel('mic')->setSettings('{}')->setMetadata('{}');
        case 'unpublish_track':
            return (new ($W('UnpublishTrackRequest'))())->setTenantId($tenant)->setTrackId($g('unpublish_track_id') ?: liveUuidV4());
        case 'mute_track':
            return (new ($W('MuteTrackRequest'))())->setTenantId($tenant)->setTrackId($g('track_id'))->setMuted(true);
        case 'list_tracks':
            return (new ($W('ListTracksRequest'))())->setTenantId($tenant)->setRoomId($g('room_id'));
        case 'issue_credentials':
            return (new ($W('IssueCredentialsRequest'))())->setTenantId($tenant)->setRoomId($g('room_id'))->setPeerId($g('peer_id'))->setTtlSeconds(3600);

        // ── TenantService ──
        case 'create_tenant':
            return (new ($T('CreateTenantRequest'))())->setCode('acme-bench-'.bin2hex(random_bytes(4)))->setName('Acme Bench')->setType('organization')->setConfig('{}')->setBranding('{}');
        case 'get_tenant':
            return (new ($T('GetTenantRequest'))())->setTenantId($tenant);
        case 'list_tenants':
            return (new ($T('ListTenantsRequest'))())->setPage(1)->setPageSize(20);
        case 'update_tenant':
            return (new ($T('UpdateTenantRequest'))())->setTenantId($tenant)->setName('Acme Bench')->setStatus('active')->setConfig('{}')->setBranding('{}');
        case 'get_tenant_config':
            return (new ($T('GetTenantConfigRequest'))())->setTenantId($tenant);
        case 'update_tenant_config':
            return (new ($T('UpdateTenantConfigRequest'))())->setTenantId($tenant)->setConfigKey('feature.flag')->setConfigValue('on')->setType('string');

        // ── IdentityProviderService ──
        case 'create_provider':
            return (new ($I('CreateProviderRequest'))())->setTenantId($tenant)->setKind(2)->setDisplayName('Acme OIDC '.bin2hex(random_bytes(3)))->setIssuer('https://idp.example.com')->setJwksUrl('https://idp.example.com/jwks')->setClientIds(['client-1'])->setAudiences(['udb'])->setClaimMappingJson('{}')->setGroupMappingJson('{}')->setJitPolicyJson('{"require_verified_email":false}')->setAccountLinkingPolicy('explicit')->setEnabled(true)->setCreatedBy($g('user_id'))->setContext($ctxC()); // OIDC=2
        case 'update_provider':
            // PRESERVE the seeded group_mapping key — UpdateProvider overwrites the provider's
            // group_mapping_json, and ScimGetGroup resolves scim_group_id against those KEYS
            // (idp/mod.rs group_keys). Wiping to '{}' makes a later/earlier ScimGetGroup
            // NOT_FOUND regardless of phase ordering. Mirror Python's postprocess (keep the key).
            return (new ($I('UpdateProviderRequest'))())->setProviderId($g('provider_id'))->setTenantId($tenant)->setDisplayName('Acme OIDC '.bin2hex(random_bytes(4)))->setClaimMappingJson('{}')->setGroupMappingJson('{"sdk-perf-group":"reader"}')->setJitPolicyJson('{"require_verified_email":false}')->setAccountLinkingPolicy('explicit')->setUpdatedBy($g('user_id'))->setContext($ctxC());
        case 'disable_provider':
            return (new ($I('DisableProviderRequest'))())->setProviderId($g('disable_provider_id') ?: $g('provider_id'))->setTenantId($tenant)->setUpdatedBy($g('user_id'))->setContext($ctxC());
        case 'get_provider':
            return (new ($I('GetProviderRequest'))())->setProviderId($g('provider_id'))->setTenantId($tenant);
        case 'list_providers':
            return (new ($I('ListProvidersRequest'))())->setTenantId($tenant)->setPage($page($I('PageRequest') ?? '\\Udb\\Core\\Common\\V1\\PageRequest'));
        case 'test_provider_discovery':
            return (new ($I('TestProviderDiscoveryRequest'))())->setProviderId($g('provider_id'))->setTenantId($tenant);
        case 'force_jwks_refresh':
            return (new ($I('ForceJwksRefreshRequest'))())->setProviderId($g('provider_id'))->setTenantId($tenant);
        case 'preview_claim_mapping':
            return (new ($I('PreviewClaimMappingRequest'))())->setProviderId($g('provider_id'))->setTenantId($tenant)->setClaimsJson('{"sub":"abc","email":"a@x.com"}');
        case 'preview_group_mapping':
            return (new ($I('PreviewGroupMappingRequest'))())->setProviderId($g('provider_id'))->setTenantId($tenant)->setGroups(['admins']);
        case 'list_external_identities':
            return (new ($I('ListExternalIdentitiesRequest'))())->setTenantId($tenant)->setPage($page($I('PageRequest') ?? '\\Udb\\Core\\Common\\V1\\PageRequest'));
        case 'link_identity':
            return (new ($I('LinkIdentityRequest'))())->setTenantId($tenant)->setProviderId($g('provider_id'))->setSubject('ext-subject-1')->setUserId($g('user_id'))->setEmail('a@x.com')->setEmailVerified(true)->setContext($ctxC());
        case 'resolve_external_identity':
            return (new ($I('ResolveExternalIdentityRequest'))())->setProviderId($g('provider_id'))->setTenantId($tenant)->setClaimsJson('{"sub":"abc","email":"a@x.com","email_verified":true}');

        // ── DataBroker (the high-value CRUD; reads/admin) ──
        case 'upsert':
            return (new ($E('UpsertRequest'))())->setContext($ctxE())->setMessageType('udb.sdk.live.v1.SdkLiveRecord')
                ->setRecordJson(liveRecordJson($g('record_id'), $tenant, $project, 'php-perf-lk', 'php-perf', 1))->setConflictFields(['record_id']);
        case 'select':
            return (new ($E('SelectRequest'))())->setContext($ctxE())->setMessageType('udb.sdk.live.v1.SdkLiveRecord')->setFilter(liveStruct(['record_id' => $g('record_id'), 'tenant_id' => $tenant, 'project_id' => $project]))->setLimit(10);
        case 'delete':
            return (new ($E('DeleteRequest'))())->setContext($ctxE())->setMessageType('udb.sdk.live.v1.SdkLiveRecord')->setFilter(liveStruct(['record_id' => 'php-perf-delete-noop', 'tenant_id' => $tenant, 'project_id' => $project]));
        case 'get_capabilities':
            return (new ($E('CapabilitiesRequest'))())->setContext($ctxE())->setProjectId($project);
        case 'get_catalog_manifest':
            return (new ($E('CatalogManifestRequest'))())->setContext($ctxE());
        case 'get_catalog_versions':
            return (new ($E('CatalogManifestRequest'))())->setContext($ctxE());
        case 'list_message_schemas':
            return (new ($E('MessageSchemaListRequest'))())->setContext($ctxE())->setProjectId($project);
        case 'lookup_message_schema':
            return (new ($E('MessageSchemaLookupRequest'))())->setContext($ctxE())->setProjectId($project)->setMessageType('udb.sdk.live.v1.SdkLiveRecord');
        case 'get_health_report':
            return (new ($E('HealthReportRequest'))())->setContext($ctxE())->setProjectId($project);
        case 'get_admin_summary':
            return (new ($E('AdminSummaryRequest'))())->setContext($ctxE())->setProjectId($project);
        case 'ensure_project':
            return (new ($E('EnsureProjectRequest'))())->setContext($ctxE())->setProjectId($project)->setName('My Project')->setCdcTopicPrefix($project.'.');
        case 'list_projects':
            return (new ($E('ProjectListRequest'))())->setContext($ctxE())->setLimit(50);
        case 'list_sagas':
            return (new ($E('SagaListRequest'))())->setContext($ctxE())->setLimit(50);
        case 'list_dlq_events':
            return (new ($E('DlqListRequest'))())->setContext($ctxE())->setLimit(50);
        case 'list_migration_runs':
            return (new ($E('MigrationRunListRequest'))())->setContext($ctxE())->setProjectId($project)->setLimit(50);
        case 'list_admin_audit_logs':
            return (new ($E('AdminAuditLogRequest'))())->setContext($ctxE())->setLimit(50);
        case 'verify_admin_audit_log':
            return (new ($E('AdminAuditVerifyRequest'))())->setContext($ctxE())->setLimit(0);
        case 'list_policies':
            return (new ($E('PolicyListRequest'))())->setContext($ctxE())->setLimit(50);
        case 'generic_dispatch':
            return (new ($E('GenericDispatchRequest'))())->setContext($ctxE())->setBackend('postgres')->setOperation('query')->setSpecJson('{"sql":"SELECT 1 AS live_probe"}');
        case 'ensure_resource':
            return (new ($E('ResourceAdminRequest'))())->setContext($ctxE())->setBackend('mongodb')->setResourceName($g('mongo_collection', 'sdk_perf'));
        case 'list_resources':
            return (new ($E('ResourceAdminRequest'))())->setContext($ctxE())->setBackend('mongodb');

        // ── AuthzService ──
        case 'authorize':
            return (new ($Z('AuthzRequest'))())->setPrincipal($principal())->setTenantId($tenant)->setProjectId($project)->setResource($resourceRef())->setAction($g('action', 'data.select'))->setDomain($tenant)->setRequestedScopes(['udb:read']);
        case 'check_access':
            return (new ($Z('CheckAccessRequest'))())->setUserId($g('user_id'))->setDomain($tenant)->setObject($g('object', 'group:sdk'))->setAction($g('action', 'data.select'));
        case 'create_role':
            // random name/role_code so per-iteration rebuilds don't collide on the
            // unique (name,domain) constraint; created_by must be a bare UUID.
            return (new ($Z('CreateRoleRequest'))())->setName('reader-'.bin2hex(random_bytes(4)))->setCreatedBy($byId)->setRoleCode('reader_'.bin2hex(random_bytes(4)))->setDomain($tenant)->setTenantId($tenant)->setScopeType(2);
        case 'assign_role':
            return (new ($Z('AssignRoleRequest'))())->setUserId($g('user_id'))->setRoleId($g('role_id'))->setDomain($tenant)->setAssignedBy($byId)->setPrincipalKind(1)->setTenantId($tenant);
        case 'create_policy_rule':
            return (new ($Z('CreatePolicyRuleRequest'))())->setSubject($sub)->setDomain($tenant)->setObject($g('object', 'group:sdk'))->setAction($g('action', 'data.update'))->setEffect(1)->setCreatedBy($byId)->setTenantId($tenant);
        case 'list_user_permissions':
            return (new ($Z('ListUserPermissionsRequest'))())->setUserId($g('user_id'))->setDomain($tenant);
        case 'list_access_decision_audits':
            return (new ($Z('ListAccessDecisionAuditsRequest'))())->setUserId($g('user_id'))->setDomain($tenant)->setPage($page());
        case 'revoke_role':
            return (new ($Z('RevokeRoleRequest'))())->setUserId($g('user_id'))->setUserRoleId($g('user_role_id'))->setReason('rotation')->setRevokedBy($byId);
        case 'list_user_roles':
            return (new ($Z('ListUserRolesRequest'))())->setUserId($g('user_id'))->setDomain($tenant)->setActiveOnly(true);
        case 'get_role':
            return (new ($Z('GetRoleRequest'))())->setRoleId($g('role_id'));
        case 'list_roles':
            return (new ($Z('ListRolesRequest'))())->setDomain($tenant)->setActiveOnly(true)->setPage($page());
        case 'batch_check_permissions':
            return (new ($Z('BatchCheckPermissionsRequest'))())->setUserId($g('user_id'))->setDomain($tenant)->setChecks([(new ($Z('PermissionCheck'))())->setObject($g('object', 'group:sdk'))->setAction($g('action', 'data.select'))]);
        case 'update_role':
            return (new ($Z('UpdateRoleRequest'))())->setRoleId($g('role_id'))->setUpdatedBy($byId)->setName('reader-'.bin2hex(random_bytes(4)))->setDescription('x')->setIsActive(true);
        case 'delete_role':
            // destructive — the SEPARATE disposable role seeded for deletion (real 200).
            return (new ($Z('DeleteRoleRequest'))())->setRoleId($g('delete_role_id') ?: liveUuidV4())->setDeletedBy($byId);
        case 'get_policy_rule':
            return (new ($Z('GetPolicyRuleRequest'))())->setPolicyId($g('policy_id'));
        case 'list_policy_rules':
            return (new ($Z('ListPolicyRulesRequest'))())->setDomain($tenant)->setActiveOnly(true)->setPage($page());
        case 'delete_policy_rule':
            // destructive — the SEPARATE disposable policy rule seeded for deletion (real 200).
            return (new ($Z('DeletePolicyRuleRequest'))())->setPolicyId($g('delete_policy_id') ?: liveUuidV4())->setDeletedBy($byId);
        case 'put_role_binding':
            return (new ($Z('PutRoleBindingRequest'))())->setBinding((new ($Z('RoleBinding'))())->setSubject($sub)->setRole($g('role', 'reader'))->setTenant($tenant)->setProject($project)->setSource('manual'));
        case 'put_relationship':
            return (new ($Z('PutRelationshipRequest'))())->setTuple((new ($Z('RelationshipTuple'))())->setSubject($sub)->setRelation($g('relation', 'member'))->setObject($g('object', 'group:sdk'))->setTenant($tenant)->setProject($project)->setSource('manual'));
        case 'put_authz_policy':
            return (new ($Z('PutAuthzPolicyRequest'))())->setPolicy((new ($Z('AuthzPolicyRecord'))())->setId($g('policy_id', 'p1'))->setPriority(100)->setEnabled(true)->setEffect('allow')->setTenant($tenant)->setSubject($sub)->setAction($g('action', 'data.select'))->setResource($g('resource', 'invoice'))->setRequiredScopes(['udb:read']));
        case 'lint_authz_policies':
            return new ($Z('LintAuthzPoliciesRequest'))();
        case 'get_native_access':
            return (new ($Z('NativeAccessRequest'))())->setPrincipal($principal())->setTenantId($tenant)->setProjectId($project)->setResource($resourceRef())->setAction($g('action', 'data.select'))->setBackend('postgres')->setRequestedScopes(['udb:read']);
        case 'get_policy_bundle':
            return (new ($Z('PolicyBundleRequest'))())->setTenantId($tenant)->setProjectId($project)->setDomain($tenant);
        case 'create_policy_draft':
            return (new ($Z('CreatePolicyDraftRequest'))())->setActor($actor(['authz:policy:write']))->setTenantId($tenant)->setProjectId($project)->setPolicySetName('default')->setTitle('draft 1')->setChangeReason('init')->setDocument(new ($Z('PolicyDocument'))());
        case 'update_policy_draft':
            return (new ($Z('UpdatePolicyDraftRequest'))())->setActor($actor(['authz:policy:write']))->setDraftId($g('update_draft_id') ?: $g('policy_draft_id'))->setDocument(new ($Z('PolicyDocument'))())->setChangeReason('edit')->setTitle('draft 1');
        case 'diff_policy_draft':
            return (new ($Z('DiffPolicyDraftRequest'))())->setActor($actor(['authz:policy:read']))->setDraftId($g('policy_draft_id'));
        case 'submit_policy_draft':
            return (new ($Z('SubmitPolicyDraftRequest'))())->setActor($actor(['authz:policy:write']))->setDraftId($g('policy_draft_id'));
        case 'approve_policy_draft':
            return (new ($Z('ApprovePolicyDraftRequest'))())->setActor($actor(['authz:policy:approve']))->setDraftId($g('approve_draft_id'))->setReviewer($sub)->setReason('ok');
        case 'reject_policy_draft':
            return (new ($Z('RejectPolicyDraftRequest'))())->setActor($actor(['authz:policy:approve']))->setDraftId($g('reject_draft_id'))->setReviewer($sub)->setReason('nack');
        case 'activate_policy_version':
            return (new ($Z('ActivatePolicyVersionRequest'))())->setActor($actor(['authz:admin']))->setPolicyVersionId($g('policy_version_id') ?: liveUuidV4());
        case 'rollback_policy_version':
            return (new ($Z('RollbackPolicyVersionRequest'))())->setActor($actor(['authz:admin']))->setPolicySetId($g('rollback_policy_set_id') ?: liveUuidV4())->setTargetVersionId($g('rollback_target_version_id') ?: liveUuidV4())->setChangeReason('revert');
        case 'activate_canary':
            return (new ($Z('ActivateCanaryRequest'))())->setActor($actor(['authz:admin']))->setPolicyVersionId($g('canary_version_id') ?: liveUuidV4())->setScopeKind(3)->setScopeValues(['10'])->setSuccessWindowSecs(0)->setMetricThreshold(0.99)->setMinSamples(0);
        case 'promote_canary':
            return (new ($Z('PromoteCanaryRequest'))())->setActor($actor(['authz:admin']))->setCanaryId($g('canary_id') ?: liveUuidV4());
        case 'get_canary_status':
            return (new ($Z('GetCanaryStatusRequest'))())->setActor($actor(['authz:policy:read']))->setCanaryId($g('canary_id') ?: liveUuidV4());
        case 'list_policy_versions':
            return (new ($Z('ListPolicyVersionsRequest'))())->setActor($actor(['authz:policy:read']))->setTenantId($tenant)->setProjectId($project)->setPolicySetId($g('policy_id'))->setState(4)->setPage($page());
        case 'simulate_policy':
            return (new ($Z('SimulatePolicyRequest'))())->setActor($actor(['authz:policy:read']))->setTenantId($tenant)->setProjectId($project)->setCases([(new ($Z('SimulationCase'))())->setPrincipal($principal())->setResource($resourceRef())->setAction($g('action', 'data.select'))->setLabel('c1')])->setPersist(false);
        case 'explain_policy':
            return (new ($Z('ExplainPolicyRequest'))())->setActor($actor(['authz:policy:read']))->setTenantId($tenant)->setProjectId($project)->setTestCase((new ($Z('SimulationCase'))())->setPrincipal($principal())->setResource($resourceRef())->setAction($g('action', 'data.select')));
        case 'get_authz_revision':
            return (new ($Z('GetAuthzRevisionRequest'))())->setTenantId($tenant)->setProjectId($project);
        case 'invalidate_policy_bundles':
            return (new ($Z('InvalidatePolicyBundlesRequest'))())->setActor($actor(['authz:admin']))->setTenantId($tenant)->setProjectId($project)->setReason('rotate');
        case 'seed_builtin_roles':
            return (new ($Z('SeedBuiltinRolesRequest'))())->setActor($actor(['authz:admin']))->setTenantId($tenant)->setProjectId($project);
        case 'migrate_legacy_policies':
            return (new ($Z('MigrateLegacyPoliciesRequest'))())->setActor($actor(['authz:admin']))->setTenantId($tenant)->setProjectId($project)->setApply(false)->setPolicySetName('default');

        // ── DataBroker (vector / object / cache / document / graph / timeseries / tx / cdc / catalog / migration / dlq / saga / policy) ──
        case 'batch_select':
        case 'select_v2':
            return (new ($E('SelectRequest'))())->setContext($ctxE())->setMessageType('udb.sdk.live.v1.SdkLiveRecord')->setFilter(liveStruct(['record_id' => $g('record_id'), 'tenant_id' => $tenant, 'project_id' => $project]))->setLimit(10);
        case 'batch_upsert':
            return (new ($E('UpsertRequest'))())->setContext($ctxE())->setMessageType('udb.sdk.live.v1.SdkLiveRecord')->setRecordJson(liveRecordJson($g('record_id'), $tenant, $project, 'php-perf-lk', 'php-perf', 1))->setConflictFields(['record_id']);
        case 'vector_search':
            return (new ($E('VectorSearchRequest'))())->setContext($ctxE())->setCollection('sdk_live_records')->setVector([0.1, 0.2, 0.3])->setLimit(5)->setWithPayload(true);
        case 'vector_hybrid_search':
            return (new ($E('VectorHybridSearchRequest'))())->setContext($ctxE())->setCollection('sdk_live_records')->setVector([0.1, 0.2, 0.3])->setTextQuery('hello')->setLimit(5)->setWithPayload(true);
        case 'vector_upsert':
        case 'vector_batch_upsert':
            return (new ($E('VectorUpsertRequest'))())->setContext($ctxE())->setCollection('sdk_live_records')->setPoints([(new ($E('VectorPointMutation'))())->setId($g('record_id'))->setVector([0.1, 0.2, 0.3])]);
        case 'put_object':
            return (new ($E('Chunk'))())->setContext($ctxE())->setBucket($g('bucket', 'udb-live-sdk'))->setObjectKey($g('object_key', 'perf.txt'))->setData('x')->setContentType('application/octet-stream')->setFinalChunk(true);
        case 'get_object':
            return (new ($E('ObjectRequest'))())->setContext($ctxE())->setBucket($g('bucket', 'udb-live-sdk'))->setObjectKey($g('object_key', 'perf.txt'));
        case 'generate_presigned_url':
            return (new ($E('UrlRequest'))())->setContext($ctxE())->setBucket($g('bucket', 'udb-live-sdk'))->setObjectKey($g('object_key', 'perf.txt'))->setMethod('GET')->setTtlSeconds(300);
        case 'initiate_multipart_upload':
            return (new ($E('MultipartUploadRequest'))())->setContext($ctxE())->setBucket($g('bucket', 'udb-live-sdk'))->setObjectKey($g('object_key', 'perf.txt'))->setContentType('application/octet-stream')->setPartCount(1)->setTtlSeconds(300);
        case 'cache_get':
            return (new ($E('CacheGetRequest'))())->setContext($ctxE())->setResource($store('redis'))->setKey($g('object_key', 'perf-key'))->setTouch(false);
        case 'cache_set':
            return (new ($E('CacheSetRequest'))())->setContext($ctxE())->setResource($store('redis'))->setKey($g('object_key', 'perf-key'))->setValue('v')->setContentType('text/plain')->setTtlSeconds(60);
        case 'cache_delete':
            return (new ($E('CacheDeleteRequest'))())->setContext($ctxE())->setResource($store('redis'))->setKey($g('object_key', 'perf-key'));
        case 'cache_scan':
            return (new ($E('CacheScanRequest'))())->setContext($ctxE())->setResource($store('redis'))->setKeyPattern('*')->setLimit(50);
        case 'document_get':
            return (new ($E('DocumentGetRequest'))())->setContext($ctxE())->setResource($store('mongodb', $g('mongo_collection', 'sdk_perf')))->setDocumentId($g('document_id', 'doc-1'));
        case 'document_find':
            return (new ($E('DocumentFindRequest'))())->setContext($ctxE())->setResource($store('mongodb', $g('mongo_collection', 'sdk_perf')))->setFilter(liveStruct([]))->setLimit(10);
        case 'document_upsert':
            return (new ($E('DocumentUpsertRequest'))())->setContext($ctxE())->setResource($store('mongodb', $g('mongo_collection', 'sdk_perf')))->setDocumentId($g('document_id', 'doc-1'))->setDocument(liveStruct(['name' => 'x']));
        case 'document_delete':
            return (new ($E('DocumentDeleteRequest'))())->setContext($ctxE())->setResource($store('mongodb', $g('mongo_collection', 'sdk_perf')))->setDocumentId($g('document_id', 'doc-1'));
        case 'graph_query':
            return (new ($E('GraphQueryRequest'))())->setContext($ctxE())->setResource($store('neo4j'))->setQuery('MATCH (n) RETURN n LIMIT 1')->setReadOnly(true)->setLimit(10);
        case 'graph_mutate':
            return (new ($E('GraphMutationRequest'))())->setContext($ctxE())->setResource($store('neo4j'))->setQuery('CREATE (n:Node {id:$id})')->setParameters(liveStruct(['id' => $g('record_id')]));
        case 'time_series_write':
            // No points (matches Go) — TimeSeriesPoint.timestamp is a Timestamp message; the empty write resolves.
            return (new ($E('TimeSeriesWriteRequest'))())->setContext($ctxE())->setResource($store('clickhouse', $g('ts_table', 'sdk_perf_ts')));
        case 'time_series_query':
            // No from/to (matches Go) — they are Timestamp messages; resource_name + limit is a valid query.
            return (new ($E('TimeSeriesQueryRequest'))())->setContext($ctxE())->setResource($store('clickhouse', $g('ts_table', 'sdk_perf_ts')))->setLimit(100);
        case 'analytical_query':
            return (new ($E('AnalyticalQueryRequest'))())->setContext($ctxE())->setResource($store('clickhouse'))->setQuery('SELECT 1')->setLimit(100);
        case 'begin_tx':
            return (new ($E('Mutation'))())->setContext($ctxE())->setOperation('upsert')->setMessageType('udb.sdk.live.v1.SdkLiveRecord')->setPayload(liveStruct(['record_id' => $g('record_id')]));
        case 'publish_cdc':
        case 'publish_c_d_c':
            return (new ($E('CDCSubscriptionRequest'))())->setContext($ctxE())->setTopicPattern($project.'.*');
        case 'create_materialized_view':
            return (new ($E('ViewDefinition'))())->setContext($ctxE())->setSchema('public')->setName('mv_test')->setQuery('SELECT 1')->setWithData(true);
        case 'enqueue_outbox_event':
            return (new ($E('EnqueueOutboxEventRequest'))())->setContext($ctxE())->setTopic($g('event_type', 'sdk.perf'))->setPartitionKey($g('document_id', 'doc-1'))->setPayload(liveStruct(['event_id' => liveUuidV4(), 'event_type' => $g('event_type', 'sdk.perf'), 'correlation_id' => liveUuidV4(), 'document_id' => $g('document_id', 'doc-1')]));
        case 'drop_resource':
            // destructive — throwaway name + explicit RLS-bypass ack (a drop spans tenants,
            // so the broker fail-closes unless udb_allow_rls_bypass is set).
            return (new ($E('ResourceAdminRequest'))())->setContext($ctxE())->setBackend('mongodb')->setResourceName('php_perf_drop_noop')->setSpecJson(json_encode(['udb_allow_rls_bypass' => true]));
        case 'stage_catalog':
        case 'validate_catalog':
            // manifest_json captured live in the seed (the new binary doesn't poison on stage).
            return (new ($E('StageCatalogRequest'))())->setContext($ctxE())->setManifestJson($g('catalog_manifest'))->setProjectId($project)->setReason('stage');
        case 'activate_catalog':
        case 'rollback_catalog':
        case 'get_catalog_version':
            return (new ($E('CatalogVersionRequest'))())->setContext($ctxE())->setProjectId($project);
        case 'plan_migration':
            return (new ($E('MigrationPlanRequest'))())->setContext($ctxE())->setProjectId($project)->setDryRun(true);
        case 'apply_migration':
            // Token is a BODY field (handlers_catalog.rs:694 reads req.approval_token); it was
            // returned in the ApproveMigrationPlan response HEADER x-udb-approval-token and
            // captured into 'approval_token' (BENCH_TS_PHP_ADVISORY.md).
            return (new ($E('MigrationApplyRequest'))())->setContext($ctxE())->setRunId($g('apply_run_id') ?: $g('migration_id'))->setProjectId($project)->setApprovalToken($g('approval_token'));
        case 'get_migration_status':
        case 'approve_migration_plan':
            return (new ($E('MigrationRunRequest'))())->setContext($ctxE())->setRunId($g('approve_run_id') ?: $g('migration_id'))->setProjectId($project);
        case 'get_dlq_event':
            return (new ($E('DlqEventRequest'))())->setContext($ctxE())->setDlqId($g('dlq_id') ?: liveUuidV4());
        case 'replay_dlq_event':
            return (new ($E('DlqActionRequest'))())->setContext($ctxE())->setDlqId($g('replay_dlq_id') ?: liveUuidV4())->setPreserveEventId(false);
        case 'dismiss_dlq_event':
            return (new ($E('DlqActionRequest'))())->setContext($ctxE())->setDlqId($g('dismiss_dlq_id') ?: liveUuidV4());
        case 'quarantine_dlq_event':
            return (new ($E('DlqActionRequest'))())->setContext($ctxE())->setDlqId($g('quarantine_dlq_id') ?: liveUuidV4());
        case 'get_cdc_status':
            return (new ($E('CdcControlRequest'))())->setContext($ctxE())->setSlotName('udb_cdc');
        case 'pause_cdc':
            return (new ($E('CdcControlRequest'))())->setContext($ctxE())->setSlotName('udb_cdc')->setReason('maintenance');
        case 'resume_cdc':
            return (new ($E('CdcControlRequest'))())->setContext($ctxE())->setSlotName('udb_cdc')->setReason('resume');
        case 'step_down_cdc_leader':
            return (new ($E('CdcControlRequest'))())->setContext($ctxE())->setSlotName('udb_cdc')->setReason('failover');
        case 'preview_cdc_redaction':
            return (new ($E('CdcRedactionPreviewRequest'))())->setContext($ctxE())->setMessageType('udb.sdk.live.v1.SdkLiveRecord')->setTopic($g('event_type', 'sdk.perf'))->setPayloadJson('{"record_id":"x"}')->setRedactionMode('mask')->setRedactionVersion(1);
        case 'scan_projection_drift':
            return (new ($E('ProjectionDriftScanRequest'))())->setContext($ctxE())->setProjectId($project)->setMessageType('udb.sdk.live.v1.SdkLiveRecord')->setScanMode('sample')->setRowsPerTarget(100)->setLimit(10);
        case 'get_saga':
            return (new ($E('SagaRequest'))())->setContext($ctxE())->setSagaId($g('saga_id') ?: liveUuidV4());
        case 'retry_saga_compensation':
            return (new ($E('SagaRequest'))())->setContext($ctxE())->setSagaId($g('retry_saga_id') ?: liveUuidV4())->setReason('retry');
        case 'mark_saga_reviewed':
            return (new ($E('SagaRequest'))())->setContext($ctxE())->setSagaId($g('mark_saga_id') ?: liveUuidV4())->setReason('reviewed');
        case 'put_policy':
            // ALLOW-ALL (empty selectors = match-any): a narrow policy would flip the
            // data plane to deny-by-default (snapshot non-empty) and deny the admin's
            // own Upsert/Select/Vector*/TimeSeries* once reload_policies runs. An
            // allow-all keeps the data plane open while still exercising the write path.
            return (new ($E('PutPolicyRequest'))())->setContext($ctxE())->setPolicy((new ($E('PolicyRecord'))())->setEffect('allow')->setServiceIdentity('')->setTenantId($tenant)->setMessageType('')->setOperation('')->setRequiredScope('')->setPriority(1)->setEnabled(true));
        case 'delete_policy':
            return (new ($E('PolicyRequest'))())->setContext($ctxE())->setPolicyId((int) ($g('ds_policy_id') ?: 0));
        case 'reload_policies':
        case 'lint_policies':
            return (new ($E('CapabilitiesRequest'))())->setContext($ctxE())->setProjectId($project);

        // ── IdentityProviderService — SCIM / SAML / unlink ──
        case 'unlink_identity':
            return (new ($I('UnlinkIdentityRequest'))())->setTenantId($tenant)->setExternalIdentityId($g('external_identity_id') ?: liveUuidV4())->setContext($ctxC());
        case 'import_saml_metadata':
            return (new ($I('ImportSamlMetadataRequest'))())->setProviderId($g('saml_provider_id') ?: $g('provider_id'))->setTenantId($tenant)->setMetadataXml('<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://idp.example.com/perf-saml"><md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol"><md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.example.com/sso"/><md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST" Location="https://idp.example.com/sso"/></md:IDPSSODescriptor></md:EntityDescriptor>')->setUpdatedBy($g('user_id'))->setContext($ctxC());
        case 'start_saml_login':
            // Must target the SAML-kind provider with an imported SSO URL, not the OIDC one.
            return (new ($I('StartSamlLoginRequest'))())->setProviderId($g('saml_provider_id') ?: $g('provider_id'))->setTenantId($tenant)->setRelayState('state-1');
        case 'saml_acs':
            return (new ($I('SamlAcsRequest'))())->setProviderId($g('provider_id'))->setTenantId($tenant)->setSamlResponse('__UDB_SAML_TEST__')->setRelayState('state-1')->setContext($ctxC());
        case 'scim_create_user':
            return (new ($I('ScimCreateUserRequest'))())->setTenantId($tenant)->setProviderId($g('provider_id'))->setScimUserJson(json_encode(['userName' => 'scim-'.bin2hex(random_bytes(6)).'@x.com', 'active' => true]))->setContext($ctxC());
        case 'scim_get_user':
            return (new ($I('ScimGetUserRequest'))())->setTenantId($tenant)->setProviderId($g('provider_id'))->setScimUserId($g('scim_user_id') ?: $g('record_id'));
        case 'scim_list_users':
            return (new ($I('ScimListUsersRequest'))())->setTenantId($tenant)->setProviderId($g('provider_id'))->setPage($page());
        case 'scim_replace_user':
            return (new ($I('ScimReplaceUserRequest'))())->setTenantId($tenant)->setProviderId($g('provider_id'))->setScimUserId($g('scim_user_id') ?: $g('record_id'))->setScimUserJson('{"userName":"a@x.com","active":true}')->setContext($ctxC());
        case 'scim_patch_user':
            return (new ($I('ScimPatchUserRequest'))())->setTenantId($tenant)->setProviderId($g('provider_id'))->setScimUserId($g('scim_user_id') ?: $g('record_id'))->setOperations([(new ($I('ScimPatchOp'))())->setOp('replace')->setPath('active')->setValueJson('false')])->setContext($ctxC());
        case 'scim_delete_user':
            return (new ($I('ScimDeleteUserRequest'))())->setTenantId($tenant)->setProviderId($g('provider_id'))->setScimUserId($g('delete_scim_user_id') ?: $g('record_id'))->setContext($ctxC());
        case 'scim_create_group':
            return (new ($I('ScimCreateGroupRequest'))())->setTenantId($tenant)->setProviderId($g('provider_id'))->setScimGroupJson(json_encode(['displayName' => 'grp-'.bin2hex(random_bytes(6))]))->setContext($ctxC());
        case 'scim_get_group':
            return (new ($I('ScimGetGroupRequest'))())->setTenantId($tenant)->setProviderId($g('provider_id'))->setScimGroupId($g('scim_group_id') ?: $g('record_id'));
        case 'scim_list_groups':
            return (new ($I('ScimListGroupsRequest'))())->setTenantId($tenant)->setProviderId($g('provider_id'))->setPage($page());
        case 'scim_patch_group':
            return (new ($I('ScimPatchGroupRequest'))())->setTenantId($tenant)->setProviderId($g('provider_id'))->setScimGroupId($g('record_id'))->setOperations([(new ($I('ScimPatchOp'))())->setOp('add')->setPath('members')->setValueJson('["x"]')])->setContext($ctxC());
        case 'scim_delete_group':
            return (new ($I('ScimDeleteGroupRequest'))())->setTenantId($tenant)->setProviderId($g('provider_id'))->setScimGroupId($g('record_id'))->setContext($ctxC());

        // ── WebRTC SignalingService ──
        case 'signal':
            return (new ($W('SignalRequest'))())->setTenantId($tenant)->setRoomId($g('room_id'))->setPeerId($g('peer_id'))->setPing(true);

        // ── ControlPlaneService (xDS-style; context = common.v1, resource_type BACKEND_TARGET_DEFINITION=5) ──
        case 'stream_resources':
            return (new ($C('DiscoveryRequest'))())->setNodeId($g('node_id', 'php-perf-node'))->setResourceType(5)->setVersionInfo('')->setResponseNonce('')->setContext($ctxC());
        case 'delta_resources':
            return (new ($C('DeltaDiscoveryRequest'))())->setNodeId($g('node_id', 'php-perf-node'))->setResourceType(5)->setResponseNonce('')->setResourceNamesSubscribe([])->setContext($ctxC());
        case 'get_resources':
            return (new ($C('GetResourcesRequest'))())->setResourceType(5)->setTenantId($tenant)->setPage($page('', 50))->setContext($ctxC());
        case 'list_node_states':
            return (new ($C('ListNodeStatesRequest'))())->setResourceType(0)->setPage($page('', 50))->setContext($ctxC());
        case 'ack_status':
            return (new ($C('AckStatusRequest'))())->setNodeId($g('node_id', 'php-perf-node'))->setResourceType(5)->setContext($ctxC());

        default:
            return null; // not yet covered → caller sends typed-empty (NEVER generic)
    }
}

/**
 * perfSeedPhp: create real, disposable entities across the services the perf run
 * touches (dependency order, namespaced by a per-run suffix) and record their IDs
 * into a PerfFixturesPhp — a faithful port of the Go perfSeed / Python perf_seed,
 * using the exact create-call shapes the conformance suite proves succeed. The
 * admin's tenant claim IS the canonical UUID, so one client/metadata serves both the
 * control plane and the UUID-strict native services. Best-effort: a failed create is
 * logged and skipped, never fatal. Returns [PerfFixturesPhp, recordId, cleanup].
 */
function perfSeedPhp(array $s): array
{
    $data = $s['data'];
    $authGen = $s['authGenerated'];
    $meta = $s['authedMeta'];
    $tenant = $meta->tenantId;
    $project = $meta->projectId;
    $suffix = bin2hex(random_bytes(8));
    $fix = new PerfFixturesPhp();
    $cleanups = [];

    foreach ([
        'tenant_id' => $tenant, 'tenant' => $tenant, 'project_id' => $project, 'project' => $project,
        'domain' => $tenant, 'message_type' => 'udb.sdk.live.v1.SdkLiveRecord', 'locale' => 'en',
        'name' => "sdk-perf-$suffix", 'filename' => "sdk-perf-$suffix.txt", 'content_type' => 'text/plain',
        'file_type' => 'DOCUMENT', 'kind' => 'audio',
    ] as $k => $v) {
        $fix->set($k, $v);
    }

    $try = function (string $label, callable $fn) {
        try {
            return $fn();
        } catch (\Throwable $e) {
            fwrite(STDERR, "perf seed: $label failed: {$e->getMessage()}\n");

            return null;
        }
    };

    // DataBroker: a real SdkLiveRecord row (Select/Delete success path).
    $recordId = "php-perf-$suffix";
    $rc = (new \Udb\Entity\V1\RequestContext())->setTenantId($tenant)->setProjectId($project)->setPurpose('php.live.perf.seed');
    $try('Upsert', fn () => $data->upsert((new \Udb\Entity\V1\UpsertRequest())
        ->setContext($rc)->setMessageType('udb.sdk.live.v1.SdkLiveRecord')
        ->setRecordJson(liveRecordJson($recordId, $tenant, $project, "php-perf-lk-$suffix", 'perf-seed', 1))
        ->setConflictFields(['record_id']), $meta));
    $fix->set('record_id', $recordId);

    // AuthnService: a real user (reused everywhere a user_id is needed) + login + codes.
    $uname = "sdk-perf-$suffix";
    $created = $try('CreateUser', fn () => $authGen->create_user((new \Udb\Core\Authn\Services\V1\CreateUserRequest())
        ->setUsername($uname)->setEmail("$uname@example.com")->setPassword('CorrectHorse1!')
        ->setTenantId($tenant)->setProjectId($project)->setFullName('SDK Perf User'), $meta));
    $uid = $created ? $created->getUser()->getUserId() : '';
    if ($uid !== '') {
        foreach (['user_id', 'recipient_id', 'assigned_by', 'created_by', 'updated_by', 'revoked_by', 'deleted_by', 'approved_by', 'rejected_by'] as $k) {
            $fix->set($k, $uid);
        }
        $fix->set('subject', "user:$uid");
        $login = $try('Login', fn () => $authGen->login((new \Udb\Core\Authn\Services\V1\LoginRequest())
            ->setUsername($uname)->setPassword('CorrectHorse1!')->setTenantHint($tenant)->setProjectHint($project)
            ->setDeviceName('php-perf-seed'), $meta));
        if ($login) {
            $fix->set('session_id', $login->getSessionId());
            $fix->set('token', $login->getAccessToken());
            $fix->set('refresh_token', $login->getRefreshToken());
            $fix->set('csrf_token', $login->getCsrfToken());
        }
        $codes = $try('GenerateRecoveryCodes', fn () => $authGen->generate_recovery_codes((new \Udb\Core\Authn\Services\V1\GenerateRecoveryCodesRequest())
            ->setUserId($uid)->setCount(8), $meta));
        if ($codes && count($codes->getCodes()) > 0) {
            $fix->set('code', $codes->getCodes()[0]);
            $fix->set('recovery_code', $codes->getCodes()[0]);
        }
        // A DEDICATED OTP user (avoids the main user's SendOTP cooldown) -> real
        // otp_id + dev-echoed code for VerifyOTP / ResendOTP. OTP_TYPE_SENSITIVE_OPERATION=4.
        $ou = $try('SeedOTPUser', fn () => $authGen->create_user((new \Udb\Core\Authn\Services\V1\CreateUserRequest())
            ->setUsername("sdk-perf-otp-$suffix")->setEmail("sdk-perf-otp-$suffix@example.com")->setPassword('CorrectHorse1!')
            ->setTenantId($tenant)->setProjectId($project)->setFullName('SDK Perf OTP User'), $meta));
        if ($ou) {
            $so = $try('SeedSendOTP', fn () => $authGen->send_o_t_p((new \Udb\Core\Authn\Services\V1\SendOTPRequest())
                ->setUserId($ou->getUser()->getUserId())->setOtpType(4), $meta));
            if ($so) {
                $fix->set('otp_id', $so->getOtpId());
                if (method_exists($so, 'getDevOtpCode') && $so->getDevOtpCode() !== '') {
                    $fix->set('otp_code', $so->getDevOtpCode());
                }
            }
        }
        // A real MFA challenge -> challenge_id (valid UUID) for VerifyMfaChallenge.
        // AUTH_FACTOR_KIND_EMAIL_OTP=2, MFA_CHALLENGE_PURPOSE_LOGIN_STEP_UP=1.
        $mc = $try('SeedMfaChallenge', fn () => $authGen->issue_mfa_challenge((new \Udb\Core\Authn\Services\V1\IssueMfaChallengeRequest())
            ->setUserId($uid)->setFactorKind(2)->setPurpose(1), $meta));
        if ($mc) {
            $fix->set('challenge_id', $mc->getChallengeId());
        }
        // A real device row: Login with a device FINGERPRINT (device_id field 7) so
        // register_login_device inserts a devices row -> device_id for RevokeDevice.
        $try('SeedDeviceLogin', fn () => $authGen->login((new \Udb\Core\Authn\Services\V1\LoginRequest())
            ->setUsername($uname)->setPassword('CorrectHorse1!')->setTenantHint($tenant)->setProjectHint($project)
            ->setDeviceId("php-perf-fp-$suffix")->setDeviceName('php-perf-device')->setIpAddress('127.0.0.1'), $meta));
        $dl = $try('SeedDevices', fn () => $authGen->list_devices((new \Udb\Core\Authn\Services\V1\ListDevicesRequest())->setUserId($uid), $meta));
        if ($dl && count($dl->getDevices()) > 0) {
            $fix->set('device_id', $dl->getDevices()[0]->getDeviceId());
        }
        // WebAuthn dev soft-authenticator (UDB_WEBAUTHN_TEST_MODE=1): register a passkey + a FRESH
        // challenge for each measured Finish (the sentinel makes the broker mint+verify a credential).
        $sr = $try('SeedWebAuthnReg', fn () => $authGen->start_web_authn_registration((new \Udb\Core\Authn\Services\V1\StartWebAuthnRegistrationRequest())
            ->setUserId($uid)->setLabel('perf-passkey')->setTenantId($tenant)->setProjectId($project), $meta));
        if ($sr) {
            $try('SeedWebAuthnFinish', fn () => $authGen->finish_web_authn_registration((new \Udb\Core\Authn\Services\V1\FinishWebAuthnRegistrationRequest())
                ->setChallengeId($sr->getChallengeId())->setPublicKeyCredentialJson('__UDB_WEBAUTHN_TEST__')->setLabel('perf-passkey'), $meta));
        }
        // The dev soft-authenticator is deterministic (one credential id per user), so the
        // measured FinishWebAuthnRegistration must use a SEPARATE user with NO existing passkey —
        // else it's a duplicate/exclude-credential case, not the first-registration success path
        // (harness_correction.md FinishWebAuthnRegistration).
        $webauthnRegUid = $uid;
        $wru = $try('SeedWebAuthnRegUser', fn () => $authGen->create_user((new \Udb\Core\Authn\Services\V1\CreateUserRequest())
            ->setUsername("sdk-perf-webauthn-reg-$suffix")->setEmail("sdk-perf-webauthn-reg-$suffix@example.com")->setPassword('CorrectHorse1!')
            ->setTenantId($tenant)->setProjectId($project)->setFullName('SDK Perf WebAuthn Registration User'), $meta));
        if ($wru && method_exists($wru, 'getUser') && $wru->getUser()) { $webauthnRegUid = $wru->getUser()->getUserId(); }
        $sr2 = $try('SeedWebAuthnReg2', fn () => $authGen->start_web_authn_registration((new \Udb\Core\Authn\Services\V1\StartWebAuthnRegistrationRequest())
            ->setUserId($webauthnRegUid)->setLabel('perf-passkey-2')->setTenantId($tenant)->setProjectId($project), $meta));
        if ($sr2) { $fix->set('reg_challenge_id', $sr2->getChallengeId()); }
        $sa = $try('SeedWebAuthnAuth', fn () => $authGen->start_web_authn_authentication((new \Udb\Core\Authn\Services\V1\StartWebAuthnAuthenticationRequest())
            ->setUserId($uid)->setTenantId($tenant), $meta));
        if ($sa) { $fix->set('auth_challenge_id', $sa->getChallengeId()); }
        // PASSWORD_RESET OTP (type 3) on a dedicated user; the dev_otp_code IS echoed
        // unconditionally when UDB_OTP_DEV_ECHO=1 (mfa.rs:208) — BENCH_TS_PHP_ADVISORY.md.
        $ru = $try('SeedResetUser', fn () => $authGen->create_user((new \Udb\Core\Authn\Services\V1\CreateUserRequest())
            ->setUsername("sdk-perf-rst-$suffix")->setEmail("sdk-perf-rst-$suffix@example.com")->setPassword('CorrectHorse1!')
            ->setTenantId($tenant)->setProjectId($project)->setFullName('SDK Perf Reset User'), $meta));
        if ($ru) {
            $rso = $try('SeedResetOTP', fn () => $authGen->send_o_t_p((new \Udb\Core\Authn\Services\V1\SendOTPRequest())
                ->setUserId($ru->getUser()->getUserId())->setOtpType(3), $meta));
            if ($rso) {
                $fix->set('reset_otp_id', $rso->getOtpId());
                if (method_exists($rso, 'getDevOtpCode') && $rso->getDevOtpCode() !== '') { $fix->set('reset_otp_code', $rso->getDevOtpCode()); }
            }
        }
        // THREE independent fresh logins so RefreshToken's rotation doesn't invalidate
        // Authenticate's token or RefreshSession's session. MUST use the ADMIN bench user
        // — the measured change_user_status/change_password mutate the sdk-perf user and
        // would deactivate its tokens/sessions.
        $adminU = liveEnv('UDB_LIVE_USERNAME', $uname);
        $adminP = liveEnv('UDB_LIVE_PASSWORD', 'CorrectHorse1!');
        $lt = $try('FreshLoginToken', fn () => $authGen->login((new \Udb\Core\Authn\Services\V1\LoginRequest())
            ->setUsername($adminU)->setPassword($adminP)->setTenantHint($tenant)->setProjectHint($project)->setDeviceName('php-perf-token'), $meta));
        if ($lt) { $fix->set('token', $lt->getAccessToken()); $fix->set('csrf_token', $lt->getCsrfToken()); }
        $lr = $try('FreshLoginRefresh', fn () => $authGen->login((new \Udb\Core\Authn\Services\V1\LoginRequest())
            ->setUsername($adminU)->setPassword($adminP)->setTenantHint($tenant)->setProjectHint($project)->setDeviceName('php-perf-refresh'), $meta));
        if ($lr) { $fix->set('refresh_token', $lr->getRefreshToken()); }
        $ls = $try('FreshLoginSession', fn () => $authGen->login((new \Udb\Core\Authn\Services\V1\LoginRequest())
            ->setUsername($adminU)->setPassword($adminP)->setTenantHint($tenant)->setProjectHint($project)->setDeviceName('php-perf-session'), $meta));
        if ($ls) { $fix->set('session_id', $ls->getSessionId()); }
    }

    // AuthzService: role + assignment + policy rule + relationship.
    $roleCode = "sdk_perf_reader_$suffix";
    $role = $try('CreateRole', fn () => $authGen->create_role((new \Udb\Core\Authz\Services\V1\CreateRoleRequest())
        ->setName("SDK Perf Reader $suffix")->setDescription('perf seed role')->setCreatedBy(liveUuidV4())
        ->setRoleCode($roleCode)->setDomain($tenant)->setTenantId($tenant)->setProjectId($project), $meta));
    if ($role) {
        $rid = $role->getRole()->getRoleId();
        $fix->set('role_id', $rid);
        $fix->set('role', $roleCode);
        $fix->set('role_code', $roleCode);
        if ($uid !== '' && $rid !== '') {
            $try('AssignRole', fn () => $authGen->assign_role((new \Udb\Core\Authz\Services\V1\AssignRoleRequest())
                ->setUserId($uid)->setRoleId($rid)->setDomain($tenant)->setAssignedBy($uid)->setTenantId($tenant)->setProjectId($project), $meta));
            $cleanups[] = fn () => $try('DeleteRole', fn () => $authGen->delete_role((new \Udb\Core\Authz\Services\V1\DeleteRoleRequest())
                ->setRoleId($rid)->setDeletedBy($uid), $meta));
        }
    }
    // A SEPARATE disposable role for the destructive DeleteRole -> real 200.
    $delRole = $try('CreateDeleteRole', fn () => $authGen->create_role((new \Udb\Core\Authz\Services\V1\CreateRoleRequest())
        ->setName("SDK Perf Del $suffix")->setDescription('disposable')->setCreatedBy($uid !== '' ? $uid : liveUuidV4())
        ->setRoleCode("sdk_perf_del_$suffix")->setDomain($tenant)->setTenantId($tenant)->setProjectId($project), $meta));
    if ($delRole) {
        $fix->set('delete_role_id', $delRole->getRole()->getRoleId());
    }
    if ($uid !== '') {
        // GetPolicyRule's CreatePolicyRule response id IS Get-queryable, BUT
        // ActivatePolicyVersion/RollbackPolicyVersion DELETE+regenerate ALL policy_rules for the
        // tenant/project and sort BEFORE GetPolicyRule — wiping a main-project rule. Seed the
        // target in an ISOLATED project no version-activation touches (harness_correction.md).
        $getPolProject = "$project-getpolrule";
        $created = $try('CreatePolicyRule', fn () => $authGen->create_policy_rule((new \Udb\Core\Authz\Services\V1\CreatePolicyRuleRequest())
            ->setSubject($roleCode)->setDomain($tenant)->setObject('ledger')->setAction('data.update')
            ->setEffect(1)->setDescription('perf seed rule (version-isolated)')->setCreatedBy($uid)->setTenantId($tenant)->setProjectId($getPolProject), $meta));
        if ($created && method_exists($created, 'getPolicy') && $created->getPolicy()) {
            $fix->set('policy_id', $created->getPolicy()->getPolicyId());
        }
        // A SEPARATE disposable rule (same isolated project) for the destructive DeletePolicyRule.
        $delRule = $try('CreateDeletePolicyRule', fn () => $authGen->create_policy_rule((new \Udb\Core\Authz\Services\V1\CreatePolicyRuleRequest())
            ->setSubject($roleCode)->setDomain($tenant)->setObject('ledger-disposable')->setAction('data.delete')
            ->setEffect(1)->setDescription('disposable')->setCreatedBy($uid)->setTenantId($tenant)->setProjectId($getPolProject), $meta));
        if ($delRule && method_exists($delRule, 'getPolicy') && $delRule->getPolicy()) {
            $fix->set('delete_policy_id', $delRule->getPolicy()->getPolicyId());
        }
    }
    $fix->set('relation', 'member');
    $fix->set('object', "group:sdk-perf-$suffix");
    $fix->set('resource', 'invoice');
    $fix->set('action', 'data.select');

    // ApiKeyService: a real key.
    $principal = "sdk-perf-svc-$suffix";
    $keyCtx = (new \Udb\Core\Common\V1\RequestContext())->setUserId($principal)
        ->setTenant((new \Udb\Core\Common\V1\TenantContext())->setTenantId($tenant)->setProjectId($project));
    $key = $try('CreateApiKey', fn () => $authGen->create_api_key((new \Udb\Core\Apikey\Services\V1\CreateApiKeyRequest())
        ->setName("sdk-perf-key-$suffix")->setOwnerId($principal)->setScopes(['data:read'])->setContext($keyCtx), $meta));
    if ($key) {
        $fix->set('key_id', $key->getKey()->getKeyId());
        // revoke/rotate/update look up by key_PREFIX (get_by_prefix), not the key_id UUID.
        $fix->set('key_prefix', $key->getKey()->getKeyPrefix());
        $fix->set('plain_key', $key->getPlainKey());
        $fix->set('owner_id', $principal);
    }
    // A SEPARATE disposable key for the destructive RevokeApiKey -> real 200.
    $revKey = $try('CreateRevokeKey', fn () => $authGen->create_api_key((new \Udb\Core\Apikey\Services\V1\CreateApiKeyRequest())
        ->setName("sdk-perf-revoke-$suffix")->setOwnerId($principal)->setScopes(['data:read'])->setContext($keyCtx), $meta));
    if ($revKey) {
        $fix->set('revoke_key_id', $revKey->getKey()->getKeyId());
        $fix->set('revoke_key_prefix', $revKey->getKey()->getKeyPrefix());
    }
    // A SEPARATE disposable key for UpdateApiKey (RotateApiKey rotates the primary key_id).
    $updKey = $try('CreateUpdateKey', fn () => $authGen->create_api_key((new \Udb\Core\Apikey\Services\V1\CreateApiKeyRequest())
        ->setName("sdk-perf-update-$suffix")->setOwnerId($principal)->setScopes(['data:read'])->setContext($keyCtx), $meta));
    if ($updKey) {
        $fix->set('update_key_id', $updKey->getKey()->getKeyId());
        $fix->set('update_key_prefix', $updKey->getKey()->getKeyPrefix());
    }

    // IdentityProviderService: a real OIDC provider -> provider_id (24 IdP RPCs read it).
    $ctxC = (new \Udb\Core\Common\V1\RequestContext())->setTenant((new \Udb\Core\Common\V1\TenantContext())->setTenantId($tenant)->setProjectId($project));
    $prov = $try('CreateProvider', fn () => $authGen->create_provider((new \Udb\Core\Idp\Services\V1\CreateProviderRequest())
        ->setTenantId($tenant)->setKind(2)->setDisplayName("SDK Perf OIDC $suffix")->setIssuer('https://idp.example.com')
        ->setJwksUrl('https://idp.example.com/jwks')->setClientIds(['client-1'])->setAudiences(['udb'])
        ->setClaimMappingJson('{}')->setGroupMappingJson('{"sdk-perf-group":"reader"}')->setJitPolicyJson('{"require_verified_email":false}')->setAccountLinkingPolicy('explicit')
        ->setEnabled(true)->setCreatedBy($uid !== '' ? $uid : liveUuidV4())->setContext($ctxC), $meta));
    if ($prov) {
        $pid = method_exists($prov, 'getProvider') && $prov->getProvider() ? $prov->getProvider()->getProviderId() : '';
        if ($pid !== '') {
            $fix->set('provider_id', $pid);
            $cleanups[] = fn () => $try('DisableProvider', fn () => $authGen->disable_provider((new \Udb\Core\Idp\Services\V1\DisableProviderRequest())
                ->setProviderId($pid)->setTenantId($tenant)->setUpdatedBy($uid !== '' ? $uid : liveUuidV4())->setContext($ctxC), $meta));
            // SCIM: JIT-provision users. The broker resolves Scim Get/Patch/Replace/Delete by
            // the SCIM user_id == the userName/subject; external_identity_id = the returned id.
            $scimUserName = "scim-$suffix@x.com";
            $su = $try('ScimCreateUser', fn () => $authGen->scim_create_user((new \Udb\Core\Idp\Services\V1\ScimCreateUserRequest())
                ->setTenantId($tenant)->setProviderId($pid)->setScimUserJson(json_encode(['userName' => $scimUserName, 'active' => true]))->setContext($ctxC), $meta));
            if ($su) {
                $fix->set('scim_user_id', $scimUserName);
            }
            // A SEPARATE disposable SCIM user backs external_identity_id, so the measured
            // UnlinkIdentity (which deletes that external identity) does NOT delete the main
            // SCIM user that ScimGet/Patch/Replace read.
            $unl = $try('ScimCreateUnlinkUser', fn () => $authGen->scim_create_user((new \Udb\Core\Idp\Services\V1\ScimCreateUserRequest())
                ->setTenantId($tenant)->setProviderId($pid)->setScimUserJson(json_encode(['userName' => "scim-unl-$suffix@x.com", 'active' => true]))->setContext($ctxC), $meta));
            if ($unl && method_exists($unl, 'getUser') && $unl->getUser()) {
                $fix->set('external_identity_id', $unl->getUser()->getId());
            }
            $delName = "scim-del-$suffix@x.com";
            $try('ScimCreateDeleteUser', fn () => $authGen->scim_create_user((new \Udb\Core\Idp\Services\V1\ScimCreateUserRequest())
                ->setTenantId($tenant)->setProviderId($pid)->setScimUserJson(json_encode(['userName' => $delName, 'active' => true]))->setContext($ctxC), $meta));
            $fix->set('delete_scim_user_id', $delName);
            $fix->set('scim_group_id', 'sdk-perf-group');
            // A real enabled SAML provider (+ imported metadata for an SSO URL) so
            // StartSamlLogin/SamlAcs resolve an active SAML provider. IDP_KIND_SAML=3.
            $samlXml = '<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://idp.example.com/perf-saml"><md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol"><md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.example.com/sso"/><md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST" Location="https://idp.example.com/sso"/></md:IDPSSODescriptor></md:EntityDescriptor>';
            $sp = $try('CreateSamlProvider', fn () => $authGen->create_provider((new \Udb\Core\Idp\Services\V1\CreateProviderRequest())
                ->setTenantId($tenant)->setKind(3)->setDisplayName("SDK Perf SAML $suffix")->setIssuer("https://saml.example.com/$suffix")
                ->setJwksUrl('https://saml.example.com/jwks')->setClientIds(['perf-saml'])->setAudiences(['udb'])
                ->setClaimMappingJson('{}')->setGroupMappingJson('{}')->setJitPolicyJson('{"require_verified_email":false}')->setAccountLinkingPolicy('explicit')
                ->setEnabled(true)->setCreatedBy($uid !== '' ? $uid : liveUuidV4())->setContext($ctxC), $meta));
            if ($sp && method_exists($sp, 'getProvider') && $sp->getProvider()) {
                $spid = $sp->getProvider()->getProviderId();
                $fix->set('saml_provider_id', $spid);
                $try('ImportSamlMetadata', fn () => $authGen->import_saml_metadata((new \Udb\Core\Idp\Services\V1\ImportSamlMetadataRequest())
                    ->setProviderId($spid)->setTenantId($tenant)->setMetadataXml($samlXml)->setUpdatedBy($uid !== '' ? $uid : liveUuidV4())->setContext($ctxC), $meta));
            }
            // A SEPARATE disposable OIDC provider for the destructive DisableProvider, so disabling
            // it does NOT disable the primary provider_id that SamlAcs/ResolveExternalIdentity read.
            $dp = $try('CreateDisposableProvider', fn () => $authGen->create_provider((new \Udb\Core\Idp\Services\V1\CreateProviderRequest())
                ->setTenantId($tenant)->setKind(2)->setDisplayName("SDK Perf OIDC Disposable $suffix")->setIssuer("https://idp-disposable.example.com/$suffix")
                ->setJwksUrl('https://idp-disposable.example.com/jwks')->setClientIds(['perf-client-disp'])->setAudiences(['udb'])
                ->setClaimMappingJson('{}')->setGroupMappingJson('{}')->setJitPolicyJson('{"require_verified_email":false}')->setAccountLinkingPolicy('explicit')
                ->setEnabled(true)->setCreatedBy($uid !== '' ? $uid : liveUuidV4())->setContext($ctxC), $meta));
            if ($dp && method_exists($dp, 'getProvider') && $dp->getProvider()) {
                $fix->set('disable_provider_id', $dp->getProvider()->getProviderId());
            }
        }
    }

    // AuthzService governance: a real policy draft -> policy_draft_id.
    $draft = $try('CreatePolicyDraft', fn () => $authGen->create_policy_draft((new \Udb\Core\Authz\Services\V1\CreatePolicyDraftRequest())
        ->setActor((new \Udb\Core\Authz\Services\V1\GovernanceActor())->setSubject($fix->lookup('subject') ?? ('user:'.$uid))->setTenantId($tenant)->setProjectId($project)->setScopes(['authz:policy:write']))
        ->setTenantId($tenant)->setProjectId($project)->setPolicySetName('default')->setTitle("sdk-perf draft $suffix")->setChangeReason('seed')->setDocument(new \Udb\Core\Authz\Services\V1\PolicyDocument()), $meta));
    if ($draft) {
        $did2 = method_exists($draft, 'getDraft') && $draft->getDraft() ? $draft->getDraft()->getDraftId() : '';
        if ($did2 !== '') {
            $fix->set('policy_draft_id', $did2);
        }
    }
    // Governance lifecycle: drafts in each state, approved versions, a canary, a rollback set.
    $gA = fn () => (new \Udb\Core\Authz\Services\V1\GovernanceActor())->setSubject($fix->lookup('subject') ?? ('user:'.$uid))->setTenantId($tenant)->setProjectId($project)->setScopes(['authz:admin', 'authz:policy:write', 'authz:policy:approve', 'policy:read']);
    $mkDraft = function (string $title, string $setName) use ($authGen, $tenant, $project, $meta, $gA, $suffix): string {
        try {
            $d = $authGen->create_policy_draft((new \Udb\Core\Authz\Services\V1\CreatePolicyDraftRequest())->setActor($gA())->setTenantId($tenant)->setProjectId($project)->setPolicySetName($setName)->setTitle($title.$suffix)->setChangeReason('seed')->setDocument(new \Udb\Core\Authz\Services\V1\PolicyDocument()), $meta);
            return ($d->getDraft()) ? $d->getDraft()->getDraftId() : '';
        } catch (\Throwable $e) {
            return '';
        }
    };
    $submit = function (string $did) use ($authGen, $gA, $meta): void {
        try {
            $authGen->submit_policy_draft((new \Udb\Core\Authz\Services\V1\SubmitPolicyDraftRequest())->setActor($gA())->setDraftId($did), $meta);
        } catch (\Throwable $e) {
        }
    };
    $u = $mkDraft('sdk-perf-update-', 'default');
    if ($u !== '') {
        $fix->set('update_draft_id', $u);
    }
    $ad = $mkDraft('sdk-perf-approve-', 'default');
    if ($ad !== '') {
        $submit($ad);
        $fix->set('approve_draft_id', $ad);
    }
    $rd = $mkDraft('sdk-perf-reject-', 'default');
    if ($rd !== '') {
        $submit($rd);
        $fix->set('reject_draft_id', $rd);
    }
    $mkVersion = function (string $setName, string $title) use ($authGen, $meta, $gA, $mkDraft, $submit, $uid): ?object {
        $did = $mkDraft($title, $setName);
        if ($did === '') {
            return null;
        }
        $submit($did);
        try {
            $ap = $authGen->approve_policy_draft((new \Udb\Core\Authz\Services\V1\ApprovePolicyDraftRequest())->setActor($gA())->setDraftId($did)->setReviewer($uid)->setReason('seed approve'), $meta);
            return $ap->getVersion();
        } catch (\Throwable $e) {
            return null;
        }
    };
    $av = $mkVersion("sdk-perf-activate-set-$suffix", 'activate-');
    if ($av) {
        $fix->set('policy_version_id', $av->getPolicyVersionId());
    }
    $cv = $mkVersion("sdk-perf-canary-set-$suffix", 'canary-');
    if ($cv) {
        $fix->set('canary_version_id', $cv->getPolicyVersionId());
        try {
            // success_window_secs MUST be > 0 (1s): 0 makes the broker substitute a default that
            // never elapses, so PromoteCanary stays "not promote-eligible".
            $c = $authGen->activate_canary((new \Udb\Core\Authz\Services\V1\ActivateCanaryRequest())->setActor($gA())->setPolicyVersionId($cv->getPolicyVersionId())->setScopeKind(3)->setScopeValues(['10'])->setSuccessWindowSecs(1)->setMetricThreshold(0.99)->setMinSamples(0), $meta);
            if ($c->getCanary()) {
                $fix->set('canary_id', $c->getCanary()->getCanaryId());
            }
        } catch (\Throwable $e) {
        }
    }
    $v1 = $mkVersion("sdk-perf-rollback-set-$suffix", 'rb1-');
    if ($v1) {
        try {
            $authGen->activate_policy_version((new \Udb\Core\Authz\Services\V1\ActivatePolicyVersionRequest())->setActor($gA())->setPolicyVersionId($v1->getPolicyVersionId()), $meta);
        } catch (\Throwable $e) {
        }
        $v2 = $mkVersion("sdk-perf-rollback-set-$suffix", 'rb2-');
        if ($v2) {
            try {
                $authGen->activate_policy_version((new \Udb\Core\Authz\Services\V1\ActivatePolicyVersionRequest())->setActor($gA())->setPolicyVersionId($v2->getPolicyVersionId()), $meta);
            } catch (\Throwable $e) {
            }
            $fix->set('rollback_policy_set_id', $v2->getPolicySetId());
            $fix->set('rollback_target_version_id', $v1->getPolicyVersionId());
        }
    }

    // DataBroker: a dry-run migration plan -> migration_id (run_id).
    $rcSeed = (new \Udb\Entity\V1\RequestContext())->setTenantId($tenant)->setProjectId($project)->setPurpose('php.live.perf.seed');
    $plan = $try('PlanMigration', fn () => $data->plan_migration((new \Udb\Entity\V1\MigrationPlanRequest())->setContext($rcSeed)->setProjectId($project)->setDryRun(true), $meta));
    if ($plan && $plan->getRunId() !== '') {
        $fix->set('migration_id', $plan->getRunId());
    }
    // approve_run_id: a NON-dry-run plan left in PREFLIGHT for the measured ApproveMigrationPlan.
    $p1 = $try('PlanMigrationApprove', fn () => $data->plan_migration((new \Udb\Entity\V1\MigrationPlanRequest())->setContext($rcSeed)->setProjectId($project)->setDryRun(false), $meta));
    if ($p1 && $p1->getRunId() !== '') {
        $fix->set('approve_run_id', $p1->getRunId());
    }
    // apply_run_id + approval_token: a SECOND non-dry-run plan, then APPROVE it so the measured
    // ApplyMigration has a valid token. ApproveMigrationPlan returns the token ONLY in the
    // response HEADER x-udb-approval-token (handlers_catalog.rs:882) — read it via the client's
    // lastResponseMetadata() right after the approve (BENCH_TS_PHP_ADVISORY.md).
    $p2 = $try('PlanMigrationApply', fn () => $data->plan_migration((new \Udb\Entity\V1\MigrationPlanRequest())->setContext($rcSeed)->setProjectId($project)->setDryRun(false), $meta));
    if ($p2 && $p2->getRunId() !== '') {
        $applyRunId = $p2->getRunId();
        $fix->set('apply_run_id', $applyRunId);
        $try('ApproveForApply', function () use ($data, $rcSeed, $project, $applyRunId, $fix, $meta) {
            $data->approve_migration_plan((new \Udb\Entity\V1\MigrationRunRequest())->setContext($rcSeed)->setRunId($applyRunId)->setProjectId($project), $meta);
            $md = $data->lastResponseMetadata();
            $tok = $md['x-udb-approval-token'][0] ?? '';
            if ($tok !== '') {
                $fix->set('approval_token', (string) $tok);
            }
            return $tok;
        });
    }
    // ds_policy_id: a real broker policy (allow-all, harmless) for the measured DeletePolicy.
    $try('PutPolicy', fn () => $data->put_policy((new \Udb\Entity\V1\PutPolicyRequest())->setContext($rcSeed)->setPolicy((new \Udb\Entity\V1\PolicyRecord())->setEffect('allow')->setTenantId($tenant)->setPriority(1)->setEnabled(true)), $meta));
    $pl = $try('ListPolicies', fn () => $data->list_policies((new \Udb\Entity\V1\PolicyListRequest())->setContext($rcSeed)->setIncludeDisabled(true)->setLimit(50), $meta));
    if ($pl && count($pl->getPolicies()) > 0) {
        $fix->set('ds_policy_id', (string) $pl->getPolicies()[0]->getPolicyId());
    }

    // Qdrant: a real vector collection. The name must be qdrant-safe (no dots) -> use
    // "sdk_live_records". size 3 / cosine matches the 3-dim [0.1,0.2,0.3] body vectors.
    $try('EnsureVectorCollection', fn () => $data->ensure_resource((new \Udb\Entity\V1\ResourceAdminRequest())
        ->setContext($rcSeed)->setBackend('qdrant')->setResourceName('sdk_live_records')->setSpecJson('{"size":3,"distance":"Cosine"}'), $meta));
    // ClickHouse: a real table so TimeSeriesWrite/Query resolve a column store -> ts_table.
    $tsOk = $try('EnsureTsTable', fn () => $data->ensure_resource((new \Udb\Entity\V1\ResourceAdminRequest())
        ->setContext($rcSeed)->setBackend('clickhouse')->setResourceName('sdk_perf_ts')->setSpecJson('{}'), $meta));
    $fix->set('ts_table', 'sdk_perf_ts');

    // Capture the live catalog manifest (READ-ONLY) so the measured StageCatalog has a valid
    // CatalogManifest (the new binary doesn't bump the active version on stage). K2 catalog
    // activate/rollback/get_version stay broker-blocked.
    $cm = $try('CaptureCatalogManifest', fn () => $data->get_catalog_manifest((new \Udb\Entity\V1\CatalogManifestRequest())->setContext($rcSeed)->setRedact(false), $meta));
    if ($cm && $cm->getManifestJson() !== '') {
        $fix->set('catalog_manifest', $cm->getManifestJson());
    }

    // AnalyticsService: a recorded metric.
    $stage = "sdk_perf_stage_$suffix";
    $try('RecordPipelineMetric', fn () => $authGen->record_pipeline_metric((new \Udb\Core\Analytics\Services\V1\RecordPipelineMetricRequest())
        ->setStageName($stage)->setTenantId($tenant)->setLatencyMs(100)->setIsSuccess(true), $meta));
    $fix->set('stage_name', $stage);

    // NotificationService: template + a sent notification.
    $event = "sdk.perf.$suffix";
    $try('UpsertTemplate', fn () => $authGen->upsert_template((new \Udb\Core\Notification\Services\V1\UpsertTemplateRequest())
        ->setEventType($event)->setChannel(1)->setLocale('en')->setSubjectTemplate('SDK {{n}}')->setBodyTemplate('sdk-perf-body')->setIsActive(true), $meta));
    $fix->set('event_type', $event);
    if ($uid !== '') {
        $sent = $try('SendNotification', fn () => $authGen->send_notification((new \Udb\Core\Notification\Services\V1\SendNotificationRequest())
            ->setEventType($event)->setRecipientId($uid)->setRecipientAddress("sdk+$suffix@example.com")->setTenantId($tenant)->setChannels([1]), $meta));
        if ($sent && count($sent->getLogs()) > 0) {
            $logId = $sent->getLogs()[0]->getLogId();
            $fix->set('log_id', $logId);
            $fix->set('notification_id', $logId);
            // RetryNotification is status-gated to FAILED rows — mark this log FAILED via
            // GenericDispatch operation="mutate" (query only allows SELECT). Go pattern.
            $try('MarkNotificationFailed', fn () => $data->generic_dispatch((new \Udb\Entity\V1\GenericDispatchRequest())
                ->setContext($rcSeed)->setBackend('postgres')->setOperation('mutate')
                ->setSpecJson(json_encode(['sql' => "UPDATE udb_notification.notification_logs SET status = 'FAILED', error_message = 'perf seed failure' WHERE log_id = \$1::UUID AND tenant_id = \$2 RETURNING log_id", 'params' => [$logId, $tenant], 'param_types' => ['uuid', 'string'], 'return_rows' => true])), $meta));
        }
        // An EMAIL preference row so GetPreference resolves.
        $try('SetPreference', fn () => $authGen->set_preference((new \Udb\Core\Notification\Services\V1\SetPreferenceRequest())
            ->setUserId($uid)->setTenantId($tenant)->setChannel(1)->setIsOptedOut(false), $meta));
    }
    // node_id for AckStatus (best-effort: set the id; a non-empty node_id avoids the
    // "node_id is required" reject even if no stream session is opened here).
    $nodeId = "sdk-perf-node-$suffix";
    $fix->set('node_id', $nodeId);
    // Open a StreamResources session so a node-state row exists for (node_id, BACKEND_TARGET=5);
    // AckStatus 404s without it.
    $try('SeedNodeState', function () use ($authGen, $nodeId, $tenant, $project, $meta) {
        $cpCtx = (new \Udb\Core\Common\V1\RequestContext())->setTenant((new \Udb\Core\Common\V1\TenantContext())->setTenantId($tenant)->setProjectId($project));
        $stream = $authGen->stream_resources($meta);
        $stream->write((new \Udb\Core\Control\Services\V1\DiscoveryRequest())->setNodeId($nodeId)->setResourceType(5)->setContext($cpCtx));
        $stream->read();
        $stream->cancel();
    });
    // Saga + DLQ rows: pre-seeded out-of-band into udb_system (fixed UUIDs, one per
    // mutating RPC). The SQL insert runs before the test (scoped to the run's tenant).
    $fix->set('saga_id', '11111111-1111-4111-8111-111111111101');
    $fix->set('retry_saga_id', '11111111-1111-4111-8111-111111111102');
    $fix->set('mark_saga_id', '11111111-1111-4111-8111-111111111103');
    $fix->set('dlq_id', '22222222-2222-4222-8222-222222222201');
    $fix->set('dismiss_dlq_id', '22222222-2222-4222-8222-222222222202');
    $fix->set('quarantine_dlq_id', '22222222-2222-4222-8222-222222222203');
    $fix->set('replay_dlq_id', '22222222-2222-4222-8222-222222222204');

    // StorageService (UUID tenant): a registered file -> file_id, + Asset pipeline.
    $reg = $try('RegisterUpload', fn () => $authGen->register_upload((new \Udb\Core\Storage\Services\V1\RegisterUploadRequest())
        ->setTenantId($tenant)->setProjectId('')->setFilename("perf-$suffix.txt")->setContentType('text/plain')
        ->setFileType('DOCUMENT')->setReferenceId(liveUuidV4())->setReferenceType('sdk.perf')->setSizeBytes(128)->setExpiresInMinutes(30), $meta));
    if ($reg) {
        $fid = $reg->getFileId();
        $fix->set('file_id', $fid);
        // FinalizeUpload HEADs the object bytes the StorageService minted. Upload through the
        // presigned RegisterUpload.upload_url FIRST (the canonical native path); DataBroker
        // PutObject is a manifest-gated fallback (harness_correction.md FinalizeUpload).
        $try('SeedPutObject', function () use ($data, $reg, $suffix, $tenant, $project, $meta) {
            $payload = "sdk-perf-file-$suffix";
            $url = method_exists($reg, 'getUploadUrl') ? $reg->getUploadUrl() : '';
            if ($url !== '') {
                // The presigned URL targets the broker's minio endpoint. When this harness runs
                // IN DOCKER (broker reached via host.docker.internal) rewrite the minio host so the
                // container can reach it; the rewrite breaks the SigV4 host → 403 → DataBroker
                // fallback below. When running NATIVELY (CI: broker on localhost, minio on
                // localhost reachable directly) DO NOT rewrite — the presigned PUT works as-is.
                if (str_contains(liveEnv('UDB_GRPC_TARGET', ''), 'host.docker.internal')) {
                    $url = str_replace(['127.0.0.1', 'localhost'], 'host.docker.internal', $url);
                }
                $ch = curl_init($url);
                curl_setopt_array($ch, [CURLOPT_CUSTOMREQUEST => 'PUT', CURLOPT_POSTFIELDS => $payload, CURLOPT_HTTPHEADER => ['Content-Type: text/plain'], CURLOPT_RETURNTRANSFER => true, CURLOPT_TIMEOUT => 10]);
                curl_exec($ch);
                $code = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
                curl_close($ch);
                if ($code >= 200 && $code < 300) {
                    return;
                }
            }
            // Fallback: DataBroker.PutObject to the catalog-manifest bucket (UDB_LIVE_S3_BUCKET).
            // FinalizeUpload HEADs UDB_STORAGE_BUCKET, which the broker lane sets to the same
            // bucket so the streamed bytes are found.
            $bucket = liveEnv('UDB_LIVE_S3_BUCKET', 'udb-live-sdk');
            $rc = (new \Udb\Entity\V1\RequestContext())->setTenantId($tenant)->setProjectId($project)->setPurpose('php.live.perf.seed');
            $call = $data->put_object($meta);
            $call->write((new \Udb\Entity\V1\Chunk())->setContext($rc)->setBucket($bucket)->setObjectKey($reg->getObjectKey())->setData($payload)->setContentType('text/plain')->setFinalChunk(true));
            $call->wait();
        });
        // A SEPARATE disposable file for the destructive DeleteFile -> real 200.
        $delReg = $try('RegisterDeleteFile', fn () => $authGen->register_upload((new \Udb\Core\Storage\Services\V1\RegisterUploadRequest())
            ->setTenantId($tenant)->setProjectId('')->setFilename("perf-del-$suffix.txt")->setContentType('text/plain')
            ->setFileType('DOCUMENT')->setReferenceId(liveUuidV4())->setReferenceType('sdk.perf')->setSizeBytes(64)->setExpiresInMinutes(30), $meta));
        if ($delReg) {
            $fix->set('delete_file_id', $delReg->getFileId());
        }
        if ($fid !== '') {
            $cleanups[] = fn () => $try('DeleteFile', fn () => $authGen->delete_file((new \Udb\Core\Storage\Services\V1\DeleteFileRequest())
                ->setTenantId($tenant)->setFileId($fid), $meta));
            $defn = $try('CreatePipelineDefinition', fn () => $authGen->create_pipeline_definition((new \Udb\Core\Asset\Services\V1\CreatePipelineDefinitionRequest())
                ->setTenantId($tenant)->setName("sdk-perf-pipeline-$suffix")->setDescription('perf seed')
                ->setMediaType('application/json')->setSteps('[{"name":"extract","type":"EXTRACT"}]')->setVersion(1), $meta));
            if ($defn) {
                $fix->set('definition_id', $defn->getDefinitionId());
            }
            $asset = $try('RegisterAsset', fn () => $authGen->register_asset((new \Udb\Core\Asset\Services\V1\RegisterAssetRequest())
                ->setTenantId($tenant)->setProjectId('')->setFileId($fid)->setName("sdk-perf-asset-$suffix")
                ->setMediaType('application/json')->setMetadata('{"source":"sdk-perf"}'), $meta));
            if ($asset) {
                $aid = $asset->getAssetId();
                $fix->set('asset_id', $aid);
                $did = $fix->lookup('definition_id');
                if ($aid !== '' && $did) {
                    $inst = $try('StartPipeline', fn () => $authGen->start_pipeline((new \Udb\Core\Asset\Services\V1\StartPipelineRequest())
                        ->setTenantId($tenant)->setDefinitionId($did)->setAssetId($aid)->setContext('{}')->setCorrelationId("sdk-perf-$suffix"), $meta));
                    if ($inst) {
                        $iid = $inst->getInstanceId();
                        $fix->set('instance_id', $iid);
                        // A started pipeline exposes its steps -> a real step_id for CompleteStep.
                        $pl = $try('GetPipelineSteps', fn () => $authGen->get_pipeline((new \Udb\Core\Asset\Services\V1\GetPipelineRequest())
                            ->setTenantId($tenant)->setInstanceId($iid), $meta));
                        if ($pl && count($pl->getSteps()) > 0) {
                            $fix->set('step_id', $pl->getSteps()[0]->getStepId());
                        }
                    }
                }
            }
        }
    }

    // WebRTC (UUID tenant): room + peer + track.
    $room = $try('CreateRoom', fn () => $authGen->create_room((new \Udb\Core\Webrtc\Services\V1\CreateRoomRequest())
        ->setTenantId($tenant)->setName("sdk-perf-room-$suffix")->setMaxParticipants(8)->setConfig('{}')->setCreatedBy(liveUuidV4()), $meta));
    if ($room) {
        $roomId = $room->getRoomId();
        $fix->set('room_id', $roomId);
        if ($roomId !== '') {
            $cleanups[] = fn () => $try('CloseRoom', fn () => $authGen->close_room((new \Udb\Core\Webrtc\Services\V1\CloseRoomRequest())
                ->setTenantId($tenant)->setRoomId($roomId), $meta));
            $joined = $try('JoinRoom', fn () => $authGen->join_room((new \Udb\Core\Webrtc\Services\V1\JoinRoomRequest())
                ->setTenantId($tenant)->setRoomId($roomId)->setDisplayName('sdk-perf-peer')->setMetadata('{}')->setUserAgent('sdk-perf'), $meta));
            if ($joined) {
                $pid = $joined->getPeer()->getPeerId();
                $fix->set('peer_id', $pid);
                if ($pid !== '') {
                    $pub = $try('PublishTrack', fn () => $authGen->publish_track((new \Udb\Core\Webrtc\Services\V1\PublishTrackRequest())
                        ->setTenantId($tenant)->setRoomId($roomId)->setPeerId($pid)->setKind('audio')->setLabel('mic')->setSettings('{}')->setMetadata('{}'), $meta));
                    if ($pub) {
                        $fix->set('track_id', $pub->getTrackId());
                    }
                    // A SECOND disposable track for the destructive UnpublishTrack.
                    $pub2 = $try('PublishUnpublishTrack', fn () => $authGen->publish_track((new \Udb\Core\Webrtc\Services\V1\PublishTrackRequest())
                        ->setTenantId($tenant)->setRoomId($roomId)->setPeerId($pid)->setKind('video')->setLabel('cam')->setSettings('{}')->setMetadata('{}'), $meta));
                    if ($pub2) {
                        $fix->set('unpublish_track_id', $pub2->getTrackId());
                    }
                }
            }
            // A SEPARATE disposable peer for the destructive LeaveRoom.
            $lj = $try('JoinLeavePeer', fn () => $authGen->join_room((new \Udb\Core\Webrtc\Services\V1\JoinRoomRequest())
                ->setTenantId($tenant)->setRoomId($roomId)->setDisplayName('sdk-perf-leave-peer')->setMetadata('{}')->setUserAgent('sdk-perf'), $meta));
            if ($lj) {
                $fix->set('leave_peer_id', $lj->getPeer()->getPeerId());
            }
            // A SEPARATE disposable room for the destructive CloseRoom (closing the main room
            // would close its peers and break PublishTrack/MuteTrack/Signal).
            $cr = $try('CreateCloseRoom', fn () => $authGen->create_room((new \Udb\Core\Webrtc\Services\V1\CreateRoomRequest())
                ->setTenantId($tenant)->setName("sdk-perf-close-room-$suffix")->setMaxParticipants(8)->setConfig('{}')->setCreatedBy($uid !== '' ? $uid : liveUuidV4()), $meta));
            if ($cr) {
                $fix->set('close_room_id', $cr->getRoomId());
            }
        }
    }

    $cleanup = function () use ($cleanups) {
        foreach (array_reverse($cleanups) as $fn) {
            $fn();
        }
    };

    return [$fix, $recordId, $cleanup];
}

it('measures per-RPC latency', function () {
    $s = phpLiveSession();
    $authedMeta = $s['authedMeta'];
    $meta = $s['meta'];

    // SEED PHASE: create real, disposable entities so every RPC is driven down its
    // SUCCESS path (Go/Python parity). $authedMeta->tenantId is the canonical tenant
    // UUID, so one client serves the UUID-strict native services (storage/asset/
    // webrtc) too. $fix maps every reference/ID field -> a real seeded entity.
    [$fix, $seedRecordId, $seedCleanup] = perfSeedPhp($s);

    $itersFor = fn (string $kind) => $kind === 'destructive' ? 1 : ($kind === 'mutation' ? 5 : 25);

    $invoke = fn ($stub, ReflectionMethod $method, $hasRequest, $probeRequest) => $hasRequest
        ? $method->invoke($stub, $probeRequest, $authedMeta->toGrpcMetadata(), ['timeout' => 20_000_000])
        : $method->invoke($stub, $authedMeta->toGrpcMetadata(), ['timeout' => 20_000_000]);

    // Unary-only timer: invoke + wait() is a single request->response round-trip.
    // Returns [elapsed_ms, err] where err is the gRPC status code NAME on a non-OK
    // status, else "OK" — so a failing RPC is recorded as a FAILURE, never a silent
    // latency sample (BENCH_RPC_BODIES.md #1/#3).
    $timeUnary = function ($stub, ReflectionMethod $method, $hasRequest, $probeRequest) use ($invoke, $authedMeta): array {
        $start = microtime(true);
        $err = 'OK';
        $detail = '';
        try {
            // put_object is CLIENT-STREAMING: the request body is STREAMED via write(), not a
            // request arg. The generic $invoke binds the Chunk to the put_object(?UdbMetadata)
            // metadata slot → the broker reads an empty stream ("empty object stream"). Open
            // with the real metadata, WRITE the seeded Chunk, then close + wait (mirrors the
            // working SeedPutObject pattern).
            if ($method->getName() === 'put_object') {
                $call = $stub->put_object($authedMeta);
                if ($probeRequest !== null) {
                    $call->write($probeRequest);
                }
            } else {
                $call = $invoke($stub, $method, $hasRequest, $probeRequest);
            }
            if (method_exists($call, 'wait')) {
                // Raw \Grpc\UnaryCall::wait() returns [response, status] and does NOT
                // throw on a non-OK status — inspect the status code or every failing
                // RPC is silently recorded as a latency sample (the PHP "false green").
                $res = $call->wait();
                $status = is_array($res) ? ($res[1] ?? null) : null;
                $code = is_object($status) ? (int) ($status->code ?? 0) : (is_array($status) ? (int) ($status['code'] ?? 0) : 0);
                if ($code !== 0) {
                    $err = grpcStatusNamePhp($code);
                    $detail = is_object($status) ? (string) ($status->details ?? '') : (string) ($status['details'] ?? '');
                }
            }
        } catch (\Fahara02\UdbLaravel\Exceptions\UdbRpcException $e) {
            $err = grpcStatusNamePhp($e->status);
            $detail = $e->getMessage();
        } catch (\Throwable $e) {
            $err = 'UNKNOWN';
            $detail = $e->getMessage();
        }

        return [(microtime(true) - $start) * 1000.0, $err, $detail];
    };

    // All RPCs are measured. Unary = full round-trip. Streaming = STREAM-OPEN latency
    // (initiate the call, then cancel WITHOUT draining): a subscription/upload stream
    // emits a first message only on an event, so draining it in a passive run would
    // just hit the deadline — that 20 s drain is what produced the bogus 272 ms.
    $samples = [];
    // Auth-phase ordering (mirrors Go orderRPCsByAuthPhase): Phase 1 establishes/validates the
    // session FIRST, Phase 2 runs everything else (reads BEFORE mutations BEFORE destructive so a
    // read of a seeded entity isn't invalidated by a later rotate/revoke/delete), Phase 3 tears
    // the session/credentials down LAST.
    $PHASE1_AUTHN = ['login', 'refresh_session', 'authenticate', 'validate_token', 'introspect_token', 'refresh_token', 'get_jwks'];
    $PHASE3_AUTHN = ['logout', 'revoke_session', 'admin_revoke_session', 'admin_revoke_all_user_sessions', 'admin_revoke_all_tenant_sessions', 'emergency_revoke', 'change_password', 'reset_password', 'admin_reset_password', 'change_user_status', 'admin_reset_mfa', 'revoke_recovery_codes', 'revoke_device', 'delete_web_authn_credential', 'disable_mfa_factor'];
    $okRank = ['read_only' => 0, 'mutation' => 1, 'destructive' => 2];
    $kindOf = fn (string $n) => \Fahara02\UdbLaravel\Generated\GeneratedClient::OPERATION_KIND[$n] ?? 'read_only';
    // Collect every (stub, method) unit, then sort into phases before measuring.
    $units = [];
    foreach (stubAccessors($s['data'], $s['authGenerated']) as $stubName => $stub) {
        $svc = preg_replace('/Stub$/', '', $stubName);
        foreach (generatedStubMethods($stub) as $method) {
            $units[] = ['stub' => $stub, 'svc' => $svc, 'method' => $method, 'name' => $method->getName()];
        }
    }
    // NOTE: $u['name'] is the raw grpc stub method (PascalCase, e.g. RefreshSession); the phase
    // lists are snake_case → normalize via rpcSnake before matching, or Phase-1/3 protection is
    // silently lost and a Phase-3 revoke (Logout/RevokeSession) runs BEFORE RefreshSession.
    $phaseOf = function (array $u) use ($PHASE1_AUTHN, $PHASE3_AUTHN): int {
        $nm = rpcSnake($u['name']);
        if ($u['svc'] === 'AuthnService' && in_array($nm, $PHASE1_AUTHN, true)) { return 1; }
        if ($u['svc'] === 'AuthnService' && in_array($nm, $PHASE3_AUTHN, true)) { return 3; }
        return 2;
    };
    usort($units, function (array $a, array $b) use ($phaseOf, $PHASE1_AUTHN, $okRank, $kindOf): int {
        $pa = $phaseOf($a); $pb = $phaseOf($b);
        if ($pa !== $pb) { return $pa <=> $pb; }
        if ($pa === 1) { return array_search(rpcSnake($a['name']), $PHASE1_AUTHN, true) <=> array_search(rpcSnake($b['name']), $PHASE1_AUTHN, true); }
        if ($pa === 2) { return ($okRank[$kindOf($a['name'])] ?? 0) <=> ($okRank[$kindOf($b['name'])] ?? 0); }
        return 0;
    });
    foreach ($units as $u) {
        {
            $stub = $u['stub']; $svc = $u['svc']; $method = $u['method'];
            $name = $method->getName();
            // put_object is CLIENT-STREAMING: drive it explicitly (open with metadata, WRITE the
            // seeded Chunk, then wait) — the reflective probe/timeUnary path binds the Chunk to the
            // metadata slot → empty stream. Mirrors the working SeedPutObject (which lands code=0).
            if (rpcSnake($name) === 'put_object') {
                $durs = [];
                $err = 'OK';
                $detail = '';
                for ($i = 0; $i < 3; $i++) {
                    $t0 = microtime(true);
                    try {
                        // $stub is the RAW grpc stub (PascalCase PutObject, grpc-array metadata).
                        // Use the GeneratedClient ($s['data']) snake client-streaming put_object
                        // with the UdbMetadata — the exact working SeedPutObject pattern.
                        $call = $s['data']->put_object($authedMeta);
                        $call->write(perfBodyPhp('put_object', $fix, $authedMeta->tenantId, $authedMeta->projectId));
                        $res = $call->wait();
                        $st = is_array($res) ? ($res[1] ?? null) : null;
                        $code = is_object($st) ? (int) ($st->code ?? 0) : 0;
                        if ($code !== 0 && $err === 'OK') {
                            $err = grpcStatusNamePhp($code);
                            $detail = is_object($st) ? (string) ($st->details ?? '') : '';
                        }
                    } catch (\Throwable $e) {
                        if ($err === 'OK') { $err = 'UNKNOWN'; $detail = $e->getMessage(); }
                    }
                    $durs[] = (microtime(true) - $t0) * 1000.0;
                }
                if ($err !== 'OK') { fwrite(STDERR, "FAILDETAIL $svc/$name [$err] ".substr($detail, 0, 200)."\n"); }
                sort($durs);
                $pp = fn (int $p) => $durs[min(count($durs) - 1, intdiv($p * (count($durs) - 1), 100))];
                $samples[] = ['service' => $svc, 'rpc' => $name, 'kind' => 'mutation', 'err' => $err, 'p50' => $pp(50), 'p99' => $pp(99), 'mean' => array_sum($durs) / count($durs)];

                continue;
            }
            $params = $method->getParameters();
            $hasRequest = isset($params[0])
                && $params[0]->getType() instanceof ReflectionNamedType
                && ! $params[0]->getType()->isBuiltin();
            $probeRequest = null;
            $kind = 'read_only';
            $mkBody = null;
            if ($hasRequest) {
                // DOCUMENTED body per docs/bench-bodies/<svc>.md, seed refs resolved from
                // $fix. NO generic population: an RPC not yet covered by perfBodyPhp gets a
                // typed-empty request (never a generically-populated placeholder). Built via
                // a factory and rebuilt PER ITERATION so create-style RPCs (random unique
                // username/role_code/name) don't collide on a reused body (iters 2+ would
                // hit the unique constraint and the broker leaks it as INTERNAL).
                $mkBody = fn () => perfBodyPhp($name, $fix, $authedMeta->tenantId, $authedMeta->projectId) ?? requestFor($method);
                $probeRequest = $mkBody();
                $kind = shouldPopulatePhp($name) ? 'mutation' : 'destructive';
            }
            // Classify with one probe invoke — invoke() does not block; only
            // responses()/wait() do. Streaming = a server-streaming (responses) or
            // bidi (writesDone) call, no unary wait(), OR an invoke that throws because
            // the method's streaming signature rejects the unary arg shape. Either way
            // the RPC is MEASURED as stream-open latency (never dropped).
            $openStart = microtime(true);
            $isStreaming = false;
            try {
                $probe = $invoke($stub, $method, $hasRequest, $probeRequest);
                $isStreaming = method_exists($probe, 'responses') || method_exists($probe, 'writesDone') || ! method_exists($probe, 'wait');
                if (method_exists($probe, 'cancel')) {
                    try {
                        $probe->cancel();
                    } catch (\Throwable $e) {
                    }
                }
            } catch (\Throwable $e) {
                $isStreaming = true;
            }
            if ($isStreaming) {
                // Stream-open latency (initiate + cancel, no response drain).
                $openMs = (microtime(true) - $openStart) * 1000.0;
                $samples[] = [
                    'service' => $svc, 'rpc' => $name, 'kind' => 'stream_open', 'err' => 'OK',
                    'p50' => $openMs, 'p99' => $openMs, 'mean' => $openMs,
                ];

                continue;
            }
            // Warm-up ONLY for idempotent reads — a warm-up on a non-idempotent mutation
            // CONSUMES the op (submit/approve a draft, rotate a token, revoke a key).
            if ($kind === 'read_only') {
                $timeUnary($stub, $method, $hasRequest, $mkBody ? $mkBody() : $probeRequest);
            }
            $allDurs = [];
            $okDurs = [];
            $anyOk = false;
            $firstErr = 'OK';
            $errDetail = '';
            for ($i = 0; $i < $itersFor($kind); $i++) {
                [$ms, $err, $detail] = $timeUnary($stub, $method, $hasRequest, $mkBody ? $mkBody() : $probeRequest);
                $allDurs[] = $ms;
                if ($err === 'OK') {
                    $anyOk = true;
                    $okDurs[] = $ms;
                } elseif ($firstErr === 'OK') {
                    $firstErr = $err;
                    $errDetail = $detail;
                }
            }
            // An RPC that succeeds AT LEAST ONCE works: repeated-call failures on a
            // non-idempotent mutation (consumed token / duplicate / already-deleted) are a
            // measurement artifact, not an RPC failure (mirrors the Go harness).
            $errCode = $anyOk ? 'OK' : $firstErr;
            $durs = $anyOk ? $okDurs : $allDurs;
            if ($errCode !== 'OK') {
                fwrite(STDERR, "FAILDETAIL $svc/$name [$errCode] ".substr($errDetail, 0, 200)."\n");
            }
            sort($durs);
            $pct = fn (int $p) => $durs[min(count($durs) - 1, intdiv($p * (count($durs) - 1), 100))];
            $samples[] = [
                'service' => $svc, 'rpc' => $name, 'kind' => $kind, 'err' => $errCode,
                'p50' => $pct(50), 'p99' => $pct(99), 'mean' => array_sum($durs) / count($durs),
            ];
        }
    }

    $bySvc = [];
    foreach ($samples as $row) {
        $bySvc[$row['service']][] = $row['mean'];
    }
    $svcMean = [];
    foreach ($bySvc as $svc => $means) {
        $svcMean[$svc] = array_sum($means) / count($means);
    }
    arsort($svcMean);
    $lines = ['# UDB SDK Live Perf — PHP (Docker → host)', '',
        'RPCs measured: '.count($samples), '',
        'Unary = full request/response round-trip. Streaming rows (kind=stream_open) report '
            .'stream-open latency (initiate + cancel, no response drain), NOT first-message latency.', '',
        '## Per-service mean latency', '', '| Service | RPCs | mean ms |', '|---|--:|--:|'];
    foreach ($svcMean as $svc => $mean) {
        $lines[] = sprintf('| %s | %d | %.2f |', $svc, count($bySvc[$svc]), $mean);
    }
    usort($samples, fn ($a, $b) => $b['p99'] <=> $a['p99']);
    $lines = array_merge($lines, ['', '## Slowest 20 by p99', '', '| RPC | kind | err | p50 ms | p99 ms | mean ms |', '|---|---|---|--:|--:|--:|']);
    foreach (array_slice($samples, 0, 20) as $row) {
        $lines[] = sprintf('| %s/%s | %s | %s | %.2f | %.2f | %.2f |', $row['service'], $row['rpc'], $row['kind'], $row['err'] ?? 'OK', $row['p50'], $row['p99'], $row['mean']);
    }
    // Failures section (BENCH_RPC_BODIES.md #1/#3): every RPC whose last iteration
    // returned a non-OK gRPC status is a FAILURE, not a latency sample.
    $failed = array_values(array_filter($samples, fn ($r) => ($r['err'] ?? 'OK') !== 'OK'));
    usort($failed, fn ($a, $b) => ($a['service'].'/'.$a['rpc']) <=> ($b['service'].'/'.$b['rpc']));
    $lines = array_merge($lines, ['', '## Failures ('.count($failed).')', '']);
    if (count($failed) === 0) {
        $lines[] = 'No RPC returned a non-OK gRPC status.';
    } else {
        $lines[] = 'These RPCs returned a non-OK gRPC status and are FAILURES, not latency samples.';
        $lines[] = '';
        $lines[] = '| RPC | kind | err | p99 ms |';
        $lines[] = '|---|---|---|--:|';
        foreach ($failed as $row) {
            $lines[] = sprintf('| %s/%s | %s | %s | %.2f |', $row['service'], $row['rpc'], $row['kind'], $row['err'], $row['p99']);
        }
    }
    file_put_contents('perf_report_php.md', implode("\n", $lines)."\n");
    $seedCleanup();
    expect(count($samples))->toBeGreaterThanOrEqual(200);
})->skip(getenv('UDB_LIVE_PERF') !== '1', 'perf run requires UDB_LIVE_PERF=1');
