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

// --- Per-RPC performance (gated on UDB_LIVE_PERF=1) ---------------------------
// Times every RPC over multiple iterations and writes perf_report_php.md — the
// PHP counterpart of the Go/Python/TS perf harness. read_only RPCs are timed
// many times; mutations a few; destructive once typed-empty.
it('measures per-RPC latency', function () {
    $s = phpLiveSession();
    $authedMeta = $s['authedMeta'];
    $meta = $s['meta'];

    $itersFor = fn (string $kind) => $kind === 'destructive' ? 1 : ($kind === 'mutation' ? 5 : 25);

    $invoke = fn ($stub, ReflectionMethod $method, $hasRequest, $probeRequest) => $hasRequest
        ? $method->invoke($stub, $probeRequest, $authedMeta->toGrpcMetadata(), ['timeout' => 20_000_000])
        : $method->invoke($stub, $authedMeta->toGrpcMetadata(), ['timeout' => 20_000_000]);

    // Unary-only timer: invoke + wait() is a single request->response round-trip.
    $timeUnary = function ($stub, ReflectionMethod $method, $hasRequest, $probeRequest) use ($invoke): float {
        $start = microtime(true);
        try {
            $call = $invoke($stub, $method, $hasRequest, $probeRequest);
            if (method_exists($call, 'wait')) {
                $call->wait();
            }
        } catch (\Throwable $e) {
            // latency still counts
        }

        return (microtime(true) - $start) * 1000.0;
    };

    // All RPCs are measured. Unary = full round-trip. Streaming = STREAM-OPEN latency
    // (initiate the call, then cancel WITHOUT draining): a subscription/upload stream
    // emits a first message only on an event, so draining it in a passive run would
    // just hit the deadline — that 20 s drain is what produced the bogus 272 ms.
    $samples = [];
    foreach (stubAccessors($s['data'], $s['authGenerated']) as $stubName => $stub) {
        $svc = preg_replace('/Stub$/', '', $stubName);
        foreach (generatedStubMethods($stub) as $method) {
            $name = $method->getName();
            $params = $method->getParameters();
            $hasRequest = isset($params[0])
                && $params[0]->getType() instanceof ReflectionNamedType
                && ! $params[0]->getType()->isBuiltin();
            $probeRequest = null;
            $kind = 'read_only';
            if ($hasRequest) {
                $probeRequest = requestFor($method);
                if (shouldPopulatePhp($name)) {
                    populateProbeRequest($probeRequest, $meta->tenantId, $meta->projectId);
                    $kind = 'mutation';
                } else {
                    $kind = 'destructive';
                }
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
                    'service' => $svc, 'rpc' => $name, 'kind' => 'stream_open',
                    'p50' => $openMs, 'p99' => $openMs, 'mean' => $openMs,
                ];

                continue;
            }
            $timeUnary($stub, $method, $hasRequest, $probeRequest); // warm-up
            $durs = [];
            for ($i = 0; $i < $itersFor($kind); $i++) {
                $durs[] = $timeUnary($stub, $method, $hasRequest, $probeRequest);
            }
            sort($durs);
            $pct = fn (int $p) => $durs[min(count($durs) - 1, intdiv($p * (count($durs) - 1), 100))];
            $samples[] = [
                'service' => $svc, 'rpc' => $name, 'kind' => $kind,
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
    $lines = array_merge($lines, ['', '## Slowest 20 by p99', '', '| RPC | kind | p50 ms | p99 ms | mean ms |', '|---|---|--:|--:|--:|']);
    foreach (array_slice($samples, 0, 20) as $row) {
        $lines[] = sprintf('| %s/%s | %s | %.2f | %.2f | %.2f |', $row['service'], $row['rpc'], $row['kind'], $row['p50'], $row['p99'], $row['mean']);
    }
    file_put_contents('perf_report_php.md', implode("\n", $lines)."\n");
    expect(count($samples))->toBeGreaterThanOrEqual(200);
})->skip(getenv('UDB_LIVE_PERF') !== '1', 'perf run requires UDB_LIVE_PERF=1');
