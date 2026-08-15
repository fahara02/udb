<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\Generated\GeneratedClient;
use Fahara02\UdbLaravel\UdbAuthClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Udb\Core\Authn\Services\V1\LoginRequest;
use Udb\Core\Authn\Services\V1\RefreshTokenRequest;
use Udb\Entity\V1\CapabilitiesRequest;

if (! function_exists('Google\\Protobuf\\Internal\\bccomp')) {
    eval(<<<'PHP'
namespace Google\Protobuf\Internal {
    function bccomp(string|int|float $left, string|int|float $right, int $scale = 0): int
    {
        $normalize = static function (string|int|float $value): array {
            $s = trim((string) $value);
            $negative = str_starts_with($s, '-');
            if ($negative) {
                $s = substr($s, 1);
            }
            $s = preg_replace('/\..*$/', '', $s) ?? '0';
            $s = ltrim($s, '0');
            return [$negative, $s === '' ? '0' : $s];
        };
        [$ln, $lv] = $normalize($left);
        [$rn, $rv] = $normalize($right);
        if ($ln !== $rn) {
            return $ln ? -1 : 1;
        }
        if (strlen($lv) !== strlen($rv)) {
            $cmp = strlen($lv) <=> strlen($rv);
            return $ln ? -$cmp : $cmp;
        }
        $cmp = $lv <=> $rv;
        return $ln ? -$cmp : $cmp;
    }
}
PHP);
}

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
    $known = [
        'SendOTP' => 'send_otp',
        'VerifyOTP' => 'verify_otp',
        'ResendOTP' => 'resend_otp',
        'ValidateCSRF' => 'validate_csrf',
        'EnrollMFA' => 'enroll_mfa',
        'ConfirmMFAEnrollment' => 'confirm_mfaenrollment',
        'GetJWKS' => 'get_jwks',
        'PublishCDC' => 'publish_cdc',
        'SelectV2' => 'select_v_2',
    ];
    if (isset($known[$name])) {
        return $known[$name];
    }
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
    $keyCtx = (new \Udb\Core\Common\V1\RequestContext())->setUserId($principal)
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
        ->setSubjectTemplate('SDK notify')->setBodyTemplate($body)->setIsActive(true), $meta);
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
// (keyed by Service/Method) — never a hardcoded name list. A populated
// destructive RPC (PutPolicy, RollbackCatalog, the revoke-all/emergency/reset
// family, DropResource, ...) could corrupt shared/global broker state.
function shouldPopulatePhp(string $name, string $service = ''): bool
{
    return operationKindPhp($name, $service) !== 'destructive';
}

function operationKindPhp(string $name, string $service = ''): string
{
    $key = $service !== '' ? "{$service}/{$name}" : $name;
    $byRpc = \Fahara02\UdbLaravel\Generated\GeneratedClient::OPERATION_KIND_BY_RPC ?? [];
    if (array_key_exists($key, $byRpc)) {
        return $byRpc[$key];
    }
    $kinds = \Fahara02\UdbLaravel\Generated\GeneratedClient::OPERATION_KIND;
    if (array_key_exists($key, $kinds)) {
        return $kinds[$key];
    }
    $matches = [];
    foreach ($kinds as $candidate => $kind) {
        if (str_ends_with($candidate, "/{$name}")) {
            $matches[] = $kind;
        }
    }
    return count($matches) === 1 ? $matches[0] : 'read_only';
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
    // per-RPC pass/fail for the generated surface instead of one opaque test — matching Go's
    // sub-tests and Python's parametrized cases. The deep create→read→assert
    // e2e above stays in this test.
})->skip(! extension_loaded('grpc'), 'requires grpc PHP extension');

// --- Per-RPC surface coverage (granular: one Pest case per RPC) ---------------

// Memoized live session: log in ONCE and reuse the authed clients across all generated
// data-driven cases. A dataset re-runs the test closure per case, so a
// non-memoized login would re-authenticate once per RPC.
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
    if (! extension_loaded('grpc')) {
        return [];
    }
    $probe = new GeneratedClient(['endpoint' => '127.0.0.1:1', 'deadline_ms' => 1_000, 'retry' => ['max_attempts' => 1]]);
    $out = [];
    foreach (stubAccessors($probe, $probe) as $stubName => $stub) {
        foreach (generatedStubMethods($stub) as $method) {
            $out["{$stubName}/{$method->getName()}"] = [$stubName, $method->getName()];
        }
    }

    return $out;
}

dataset('liveRpcs', fn () => extension_loaded('grpc') ? phpLiveRpcCatalog() : [['__grpc_missing__', '__grpc_missing__']]);

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
            if (shouldPopulatePhp($methodName, preg_replace('/Stub$/', '', $stubName))) {
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
})->with('liveRpcs')->skip(! extension_loaded('grpc'), 'requires grpc PHP extension');

// Coverage guard: the per-RPC dataset must enumerate exactly the generated surface.
it('enumerates the full generated live RPC surface', function () {
    expect(count(phpLiveRpcCatalog()))->toBe(count(phpBenchBodyRows()));
})->skip(! extension_loaded('grpc'), 'requires grpc PHP extension for generated stub reflection');

/**
 * Read the shared bench-body manifest (docs/bench-bodies/<svc>.md) the way the
 * Python consumer does (test_live_conformance.py::bench_body_rows): every row is
 * `| [ ] | <RpcName> | <KIND> | <ReqType> | <body> | <notes> |`. Returns a map
 * of RPC name → documented JSON body (column 4), with `<seed:KEY>` references
 * intact (resolved at send time from the seed fixtures). Cross-SDK parity: Go/
 * Python already consume this; PHP now reads the same source of truth so adding
 * an RPC needs only a new manifest row.
 *
 * @return array<string,string>
 */
function phpBenchBodyRows(): array
{
    static $rows = null;
    if ($rows !== null) {
        return $rows;
    }
    $rows = [];
    // Consume the GENERATED machine-readable manifest
    // (scripts/gen-bench-bodies-json.mjs -> docs/generated/bench-bodies.json), the
    // new cross-SDK consumer source of truth. The markdown corpus stays the
    // human-editable source; phpBenchBodyMarkdownRows() + the drift test prove the
    // JSON equals a fresh markdown parse. tests/Live -> php -> sdk -> udb (repo
    // root) is dirname(__DIR__, 4); the JSON lives at <repo>/docs/generated.
    $json = dirname(__DIR__, 4).'/docs/generated/bench-bodies.json';
    $entries = json_decode((string) file_get_contents($json), true) ?: [];
    foreach ($entries as $e) {
        // Key on the full RPC name (col2), body on col5.
        $rows[$e['rpc']] = $e['body'];
    }

    return $rows;
}

/**
 * @return list<array{rpc:string,service:string,request_msg:string,body:string,api_alias:string}>
 */
function phpBenchBodyEntries(): array
{
    static $entries = null;
    if ($entries !== null) {
        return $entries;
    }
    $json = dirname(__DIR__, 4).'/docs/generated/bench-bodies.json';
    $entries = json_decode((string) file_get_contents($json), true) ?: [];

    return $entries;
}

/**
 * @return array<string,array{rpc:string,service:string,request_msg:string,body:string,api_alias:string}>
 */
function phpBenchBodyEntriesByAlias(): array
{
    static $byAlias = null;
    if ($byAlias !== null) {
        return $byAlias;
    }
    $byAlias = [];
    $put = function (string $key, array $entry) use (&$byAlias): void {
        $key = strtolower(trim($key));
        if ($key !== '') {
            $byAlias[$key] = $entry;
        }
    };
    foreach (phpBenchBodyEntries() as $e) {
        $alias = (string) ($e['api_alias'] ?? '');
        $entry = [
            'rpc' => (string) ($e['rpc'] ?? ''),
            'service' => (string) ($e['service'] ?? ''),
            'request_msg' => (string) ($e['request_msg'] ?? ''),
            'body' => (string) ($e['body'] ?? ''),
            'api_alias' => $alias,
        ];
        $service = strtolower((string) ($e['service'] ?? ''));
        $rpc = (string) ($e['rpc'] ?? '');
        $wireRpc = (string) ($e['wire_rpc'] ?? '');
        foreach (array_filter([$alias, $rpc, rpcSnake($rpc)]) as $key) {
            $put((string) $key, $entry);
            if ($service !== '') {
                $put($service.'.'.(string) $key, $entry);
            }
        }
        if ($wireRpc !== '') {
            $put(str_replace('/', '.', $wireRpc), $entry);
        }
    }

    return $byAlias;
}

function phpStrictManifestJsonCell(string $body): ?string
{
    $body = trim($body);
    if (! preg_match('/^`(\{.*\})`$/s', $body, $m)) {
        return null;
    }
    $probe = preg_replace('/"<seed:[^>]+>"/', '"__seed__"', $m[1]);
    $probe = preg_replace('/<seed:[^>]+>/', '0', (string) $probe);
    json_decode((string) $probe, true);
    if (json_last_error() !== JSON_ERROR_NONE) {
        return null;
    }

    return $m[1];
}

function phpResolveManifestSeeds(string $json, PerfFixturesPhp $fix, string $tenant, string $project): string
{
    return preg_replace_callback('/<seed:([^>]+)>/', function (array $m) use ($fix, $tenant, $project): string {
        $key = strtolower((string) $m[1]);
        $value = match ($key) {
            'tenant_id' => $tenant,
            'project', 'project_id' => $project,
            default => $fix->lookup($key),
        };
        if ($value === null || $value === '') {
            throw new RuntimeException("missing PHP bench manifest seed '{$key}'");
        }

        return addcslashes((string) $value, "\\\"");
    }, $json);
}

/**
 * @return array<string,list<string>>
 */
function phpGeneratedRequestClassIndex(): array
{
    static $index = null;
    if ($index !== null) {
        return $index;
    }
    $index = [];
    $root = dirname(__DIR__, 2).'/gen';
    $it = new RecursiveIteratorIterator(new RecursiveDirectoryIterator($root, FilesystemIterator::SKIP_DOTS));
    foreach ($it as $file) {
        if (! $file->isFile() || $file->getExtension() !== 'php') {
            continue;
        }
        $path = $file->getPathname();
        if (str_contains($path, DIRECTORY_SEPARATOR.'GPBMetadata'.DIRECTORY_SEPARATOR)) {
            continue;
        }
        $base = $file->getBasename('.php');
        if (! str_ends_with($base, 'Request') && ! in_array($base, ['ViewDefinition', 'Mutation', 'Chunk'], true)) {
            continue;
        }
        $rel = substr($path, strlen($root) + 1, -4);
        $class = '\\'.str_replace(DIRECTORY_SEPARATOR, '\\', $rel);
        if (class_exists($class)) {
            $index[$base][] = $class;
        }
    }

    return $index;
}

function phpRequestClassForManifestEntry(array $entry): ?string
{
    $requestMsg = trim((string) ($entry['request_msg'] ?? ''));
    $requestMsg = preg_replace('/^stream\s+/i', '', $requestMsg) ?? $requestMsg;
    if ($requestMsg === '') {
        return null;
    }
    $candidates = phpGeneratedRequestClassIndex()[$requestMsg] ?? [];
    if (count($candidates) === 1) {
        return $candidates[0];
    }
    if (($entry['service'] ?? '') === 'DataBroker') {
        foreach ($candidates as $class) {
            if (str_starts_with($class, '\\Udb\\Entity\\V1\\') || str_starts_with($class, '\\Udb\\Services\\V1\\')) {
                return $class;
            }
        }
    }
    if (($entry['service'] ?? '') === 'CacheService' && $requestMsg === 'DeleteRequest') {
        $class = '\\Udb\\Core\\Cache\\Services\\V1\\DeleteRequest';
        if (class_exists($class)) {
            return $class;
        }
    }
    $serviceHint = strtolower(preg_replace('/service$/i', '', (string) ($entry['service'] ?? '')));
    $serviceHint = preg_replace('/[^a-z0-9]/', '', $serviceHint);
    foreach ($candidates as $class) {
        $normalized = strtolower(preg_replace('/[^a-z0-9]/', '', $class));
        if ($serviceHint !== '' && str_contains($normalized, $serviceHint)) {
            return $class;
        }
    }

    return null;
}

function phpManifestJsonBody(string $name, PerfFixturesPhp $fix, string $tenant, string $project, ?string $serviceName = null): ?object
{
    $n = strtolower(rpcSnake($name));
    $rows = phpBenchBodyEntriesByAlias();
    $entry = null;
    if ($serviceName !== null && $serviceName !== '') {
        $entry = $rows[strtolower($serviceName).'.'.$n] ?? null;
    }
    $entry ??= $rows[$n] ?? null;
    if ($entry === null) {
        return null;
    }
    $json = phpStrictManifestJsonCell($entry['body']);
    if ($json === null) {
        return null;
    }
    $class = phpRequestClassForManifestEntry($entry);
    if ($class === null) {
        throw new RuntimeException("PHP bench manifest request class missing: {$entry['request_msg']}");
    }
    $request = new $class();
    $json = preg_replace(
        '/"timestamp"\s*:\s*\{\s*"seconds"\s*:\s*1767225600\s*,\s*"nanos"\s*:\s*0\s*\}/',
        '"timestamp": "2026-01-01T00:00:00Z"',
        phpResolveManifestSeeds($json, $fix, $tenant, $project),
    ) ?? phpResolveManifestSeeds($json, $fix, $tenant, $project);
    $request->mergeFromJsonString($json);

    return $request;
}

/**
 * LEGACY markdown parse, retained ONLY to power the drift test that proves the
 * generated JSON still equals a fresh parse of the human-editable markdown.
 *
 * @return array<string,string>
 */
function phpBenchBodyMarkdownRows(): array
{
    $rows = [];
    $dir = dirname(__DIR__, 4).'/docs/bench-bodies';
    foreach (glob($dir.'/*.md') ?: [] as $path) {
        if (basename($path) === 'workflow-sequences.md') {
            continue;
        }
        foreach (file($path, FILE_IGNORE_NEW_LINES) ?: [] as $line) {
            if (! str_starts_with($line, '| [')) {
                continue;
            }
            $parts = array_map('trim', explode('|', trim($line, " \t|")));
            if (count($parts) >= 5 && $parts[1] !== 'RPC') {
                $rows[$parts[1]] = $parts[4];
            }
        }
    }

    return $rows;
}

// The bench-body manifest is the shared source of truth. PHP reads it like the
// other SDKs; adding an RPC needs a manifest row and a generated catalog entry.
// The full generic JSON-merge hydrator (BENCH §11.1.4.2/.3,
// the descriptor-driven consumer Go/Python use) stays DEFERRED for PHP: the protobuf
// PHP extension exposes no usable descriptor reflection in this env
// (DescriptorPool::getDescriptorByClassName -> null), so a like-for-like generic
// hydrator is not portable. The typed switch + this row-count contract are the
// honest PHP equivalent.
it('reads one shared bench-body row per generated live RPC', function () {
    expect(count(phpBenchBodyRows()))->toBe(count(phpLiveRpcCatalog()));
});

// R6.1 DRIFT gate: docs/generated/bench-bodies.json must equal a fresh parse of
// the human-editable docs/bench-bodies/*.md. Editing markdown without regenerating
// (`node scripts/gen-bench-bodies-json.mjs`) trips this.
it('bench-bodies.json matches a fresh markdown parse', function () {
    $fromJson = phpBenchBodyRows();
    $fromMd = phpBenchBodyMarkdownRows();
    $expected = count(phpLiveRpcCatalog());
    expect(count($fromJson))->toBe($expected);
    expect(count($fromMd))->toBe($expected);
    ksort($fromJson);
    ksort($fromMd);
    expect($fromJson)->toBe($fromMd);
});

// Manifest↔catalog parity is enforced by COUNT: the manifest assertion above and
// the generated live-RPC reflection assertion both pin the same number, so a
// drift on either side trips a test. A name-by-name cross-
// check is intentionally NOT done here because the two key shapes don't map 1:1:
// the catalog keys are "<stub>/<Method>" (e.g. "data/EnsureBaseline") while the
// manifest mixes bare method names ("EnsureBaseline") with "Service.Method" forms
// ("PeerService.JoinSession", which disambiguates the webrtc Join* overloads) —
// reconciling them cleanly needs the descriptor reflection PHP doesn't expose
// (see the deferral note above). What we CAN assert offline (no ext-grpc, no
// broker) is that the manifest carries representative service-qualified keys.
it('manifest carries representative service-qualified RPC keys', function () {
    $rows = phpBenchBodyRows();
    expect($rows)->toHaveKey('DataBroker.Delete')
        ->and($rows)->toHaveKey('CacheService.Delete')
        ->and($rows)->toHaveKey('PeerService.JoinSession');
});

function phpManifestFixtureValue(string $key): string
{
    return match ($key) {
        'catalog_manifest_b64' => base64_encode('{"resources":[]}'),
        'ds_policy_id' => '42',
        'fencing_token', 'renew_fencing_token', 'release_fencing_token' => '17',
        'gov_exp' => (string) (time() + 900),
        'plain_key' => 'udb_live_key_test',
        'tenant_id', 'tenant' => 'tenant-php',
        'project', 'project_id' => 'project-php',
        'token', 'refresh_token', 'csrf_token' => 'token-php',
        'vault_ciphertext', 'vault_signature' => base64_encode('perf'),
        default => str_ends_with($key, '_id') || $key === 'id'
            ? '11111111-1111-4111-8111-'.str_pad((string) (crc32($key) % 1000000000000), 12, '0', STR_PAD_LEFT)
            : 'seed-'.$key,
    };
}

function phpFullSurfaceManifestFixtures(): PerfFixturesPhp
{
    $fix = new PerfFixturesPhp();
    foreach (phpBenchBodyEntries() as $entry) {
        foreach (preg_match_all('/<seed:([^>]+)>/', (string) ($entry['body'] ?? ''), $m) ? $m[1] : [] as $key) {
            $key = strtolower((string) $key);
            $fix->set($key, phpManifestFixtureValue($key));
        }
    }

    return $fix;
}

it('manifest-only perf body hydrates every generated RPC request', function () {
    $fix = phpFullSurfaceManifestFixtures();
    $failures = [];
    foreach (phpBenchBodyEntries() as $entry) {
        try {
            $body = phpManifestJsonBody(
                (string) $entry['api_alias'],
                $fix,
                'tenant-php',
                'project-php',
                (string) $entry['service'],
            );
            if (! is_object($body)) {
                $failures[] = "{$entry['service']}.{$entry['rpc']} did not hydrate";
            }
        } catch (Throwable $e) {
            $failures[] = "{$entry['service']}.{$entry['rpc']}: {$e->getMessage()}";
        }
    }

    expect($failures)->toBe([], implode("\n", $failures));
    expect(count(phpBenchBodyEntries()))->toBe(count(phpBenchBodyRows()));
});

it('manifest JSON body hydrates AnalyticsService requests with seed refs', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('stage_name', 'ingest');
    $body = phpManifestJsonBody('get_pipeline_summary', $fix, 'tenant-php', 'project-php');
    expect($body)->toBeInstanceOf(\Udb\Core\Analytics\Services\V1\GetPipelineSummaryRequest::class)
        ->and($body->getStageName())->toBe('ingest')
        ->and($body->getTenantId())->toBe('tenant-php')
        ->and($body->getPage()->getPageSize())->toBe(50);
});

it('manifest JSON body hydrates TenantService requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('tenant_code', 'tenant-code-php');
    $fix->set('purge_tenant_id', 'tenant-purge-php');
    $created = phpManifestJsonBody('create_tenant', $fix, 'tenant-php', 'project-php', 'TenantService');
    $tenant = phpManifestJsonBody('get_tenant', $fix, 'tenant-php', 'project-php', 'TenantService');
    $config = phpManifestJsonBody('get_tenant_config', $fix, 'tenant-php', 'project-php', 'TenantService');
    $list = phpManifestJsonBody('list_tenants', $fix, 'tenant-php', 'project-php', 'TenantService');
    $purged = phpManifestJsonBody('purge_tenant', $fix, 'tenant-php', 'project-php', 'TenantService');
    $updated = phpManifestJsonBody('update_tenant', $fix, 'tenant-php', 'project-php', 'TenantService');
    $updatedConfig = phpManifestJsonBody('update_tenant_config', $fix, 'tenant-php', 'project-php', 'TenantService');
    expect($created)->toBeInstanceOf(\Udb\Core\Tenant\Services\V1\CreateTenantRequest::class)
        ->and($created->getCode())->toBe('tenant-code-php')
        ->and($created->getConfig())->toBe('{}')
        ->and($created->getBranding())->toBe('{}')
        ->and($tenant)->toBeInstanceOf(\Udb\Core\Tenant\Services\V1\GetTenantRequest::class)
        ->and($tenant->getTenantId())->toBe('tenant-php')
        ->and($config)->toBeInstanceOf(\Udb\Core\Tenant\Services\V1\GetTenantConfigRequest::class)
        ->and($config->getTenantId())->toBe('tenant-php')
        ->and($list)->toBeInstanceOf(\Udb\Core\Tenant\Services\V1\ListTenantsRequest::class)
        ->and($list->getPage())->toBe(1)
        ->and($list->getPageSize())->toBe(20)
        ->and($purged)->toBeInstanceOf(\Udb\Core\Tenant\Services\V1\PurgeTenantRequest::class)
        ->and($purged->getTenantId())->toBe('tenant-purge-php')
        ->and($purged->getConfirmationToken())->toBe('sdk-perf-confirm-purge')
        ->and($updated)->toBeInstanceOf(\Udb\Core\Tenant\Services\V1\UpdateTenantRequest::class)
        ->and($updated->getStatus())->toBe('active')
        ->and($updated->getConfig())->toBe('{}')
        ->and($updatedConfig)->toBeInstanceOf(\Udb\Core\Tenant\Services\V1\UpdateTenantConfigRequest::class)
        ->and($updatedConfig->getConfigKey())->toBe('feature.flag')
        ->and($updatedConfig->getConfigValue())->toBe('on');
});

it('manifest JSON body hydrates DataBroker scalar read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('project', 'project-php');
    $fix->set('message_type', 'myapp.v1.Invoice');
    $fix->set('dlq_id', 'dlq-php');
    $fix->set('saga_id', 'saga-php');
    $fix->set('migration_id', 'migration-php');
    $fix->set('object_key', 'cache-key-php');
    $fix->set('mongo_collection', 'invoices');
    $fix->set('document_id', 'document-php');
    $fix->set('record_id', 'record-php');
    $fix->set('bucket', 'bucket-php');
    $fix->set('ts_table', 'metrics_php');
    $fix->set('event_type', 'invoice.updated');
    $capabilities = phpManifestJsonBody('get_capabilities', $fix, 'tenant-php', 'project-php');
    $catalog = phpManifestJsonBody('get_catalog_manifest', $fix, 'tenant-php', 'project-php');
    $health = phpManifestJsonBody('get_health_report', $fix, 'tenant-php', 'project-php');
    $schemas = phpManifestJsonBody('lookup_message_schema', $fix, 'tenant-php', 'project-php');
    $dlq = phpManifestJsonBody('get_dlq_event', $fix, 'tenant-php', 'project-php');
    $dlqs = phpManifestJsonBody('list_dlq_events', $fix, 'tenant-php', 'project-php');
    $saga = phpManifestJsonBody('get_saga', $fix, 'tenant-php', 'project-php');
    $sagas = phpManifestJsonBody('list_sagas', $fix, 'tenant-php', 'project-php');
    $policies = phpManifestJsonBody('list_policies', $fix, 'tenant-php', 'project-php');
    $lint = phpManifestJsonBody('lint_policies', $fix, 'tenant-php', 'project-php');
    $admin = phpManifestJsonBody('get_admin_summary', $fix, 'tenant-php', 'project-php');
    $catalogVersion = phpManifestJsonBody('get_catalog_version', $fix, 'tenant-php', 'project-php');
    $catalogVersions = phpManifestJsonBody('get_catalog_versions', $fix, 'tenant-php', 'project-php');
    $cdc = phpManifestJsonBody('get_cdc_status', $fix, 'tenant-php', 'project-php');
    $migration = phpManifestJsonBody('get_migration_status', $fix, 'tenant-php', 'project-php');
    $migrationRuns = phpManifestJsonBody('list_migration_runs', $fix, 'tenant-php', 'project-php');
    $projects = phpManifestJsonBody('list_projects', $fix, 'tenant-php', 'project-php');
    $resources = phpManifestJsonBody('list_resources', $fix, 'tenant-php', 'project-php');
    $audit = phpManifestJsonBody('list_admin_audit_logs', $fix, 'tenant-php', 'project-php');
    $verify = phpManifestJsonBody('verify_admin_audit_log', $fix, 'tenant-php', 'project-php');
    $vector = phpManifestJsonBody('vector_search', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $hybrid = phpManifestJsonBody('vector_hybrid_search', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $cacheGet = phpManifestJsonBody('cache_get', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $cacheScan = phpManifestJsonBody('cache_scan', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $documentGet = phpManifestJsonBody('document_get', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $documentFind = phpManifestJsonBody('document_find', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $graph = phpManifestJsonBody('graph_query', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $analytical = phpManifestJsonBody('analytical_query', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $select = phpManifestJsonBody('select', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $selectV2 = phpManifestJsonBody('select_v_2', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $object = phpManifestJsonBody('get_object', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $timeSeries = phpManifestJsonBody('time_series_query', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $preview = phpManifestJsonBody('preview_cdc_redaction', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $drift = phpManifestJsonBody('scan_projection_drift', $fix, 'tenant-php', 'project-php', 'DataBroker');
    expect($capabilities)->toBeInstanceOf(\Udb\Entity\V1\CapabilitiesRequest::class)
        ->and($capabilities->getContext()->getTenantId())->toBe('tenant-php')
        ->and($capabilities->getProjectId())->toBe('project-php')
        ->and($catalog)->toBeInstanceOf(\Udb\Entity\V1\CatalogManifestRequest::class)
        ->and($catalog->getRedact())->toBeFalse()
        ->and($health)->toBeInstanceOf(\Udb\Entity\V1\HealthReportRequest::class)
        ->and($health->getWithProbes())->toBeFalse()
        ->and($schemas)->toBeInstanceOf(\Udb\Entity\V1\MessageSchemaLookupRequest::class)
        ->and($schemas->getMessageType())->toBe('myapp.v1.Invoice')
        ->and($dlq)->toBeInstanceOf(\Udb\Entity\V1\DlqEventRequest::class)
        ->and($dlq->getDlqId())->toBe('dlq-php')
        ->and($dlqs)->toBeInstanceOf(\Udb\Entity\V1\DlqListRequest::class)
        ->and($dlqs->getLimit())->toBe(50)
        ->and($saga)->toBeInstanceOf(\Udb\Entity\V1\SagaRequest::class)
        ->and($saga->getSagaId())->toBe('saga-php')
        ->and($sagas)->toBeInstanceOf(\Udb\Entity\V1\SagaListRequest::class)
        ->and($sagas->getLimit())->toBe(50)
        ->and($policies)->toBeInstanceOf(\Udb\Entity\V1\PolicyListRequest::class)
        ->and($policies->getIncludeDisabled())->toBeFalse()
        ->and($lint)->toBeInstanceOf(\Udb\Entity\V1\CapabilitiesRequest::class)
        ->and($lint->getProjectId())->toBe('project-php')
        ->and($admin)->toBeInstanceOf(\Udb\Entity\V1\AdminSummaryRequest::class)
        ->and(iterator_to_array($admin->getContext()->getScopes()))->toBe(['udb:admin'])
        ->and($admin->getWithProbes())->toBeFalse()
        ->and($catalogVersion)->toBeInstanceOf(\Udb\Entity\V1\CatalogVersionRequest::class)
        ->and($catalogVersion->getVersion())->toBe('')
        ->and($catalogVersions)->toBeInstanceOf(\Udb\Entity\V1\CatalogManifestRequest::class)
        ->and($catalogVersions->getRedact())->toBeFalse()
        ->and($cdc)->toBeInstanceOf(\Udb\Entity\V1\CdcControlRequest::class)
        ->and($cdc->getSlotName())->toBe('udb_cdc')
        ->and($migration)->toBeInstanceOf(\Udb\Entity\V1\MigrationRunRequest::class)
        ->and($migration->getRunId())->toBe('migration-php')
        ->and($migrationRuns)->toBeInstanceOf(\Udb\Entity\V1\MigrationRunListRequest::class)
        ->and($migrationRuns->getLimit())->toBe(50)
        ->and($projects)->toBeInstanceOf(\Udb\Entity\V1\ProjectListRequest::class)
        ->and($projects->getLimit())->toBe(50)
        ->and($resources)->toBeInstanceOf(\Udb\Entity\V1\ResourceAdminRequest::class)
        ->and($resources->getBackend())->toBe('mongodb')
        ->and($audit)->toBeInstanceOf(\Udb\Entity\V1\AdminAuditLogRequest::class)
        ->and($audit->getRedact())->toBeFalse()
        ->and($verify)->toBeInstanceOf(\Udb\Entity\V1\AdminAuditVerifyRequest::class)
        ->and($verify->getLimit())->toBe(0)
        ->and($vector)->toBeInstanceOf(\Udb\Entity\V1\VectorSearchRequest::class)
        ->and($vector->getCollection())->toBe('sdk_live_records')
        ->and(count($vector->getVector()))->toBe(3)
        ->and($hybrid)->toBeInstanceOf(\Udb\Entity\V1\VectorHybridSearchRequest::class)
        ->and($hybrid->getTextQuery())->toBe('hello')
        ->and($cacheGet)->toBeInstanceOf(\Udb\Entity\V1\CacheGetRequest::class)
        ->and($cacheGet->getResource()->getBackend())->toBe('redis')
        ->and($cacheGet->getKey())->toBe('cache-key-php')
        ->and($cacheScan)->toBeInstanceOf(\Udb\Entity\V1\CacheScanRequest::class)
        ->and($cacheScan->getLimit())->toBe(50)
        ->and($documentGet)->toBeInstanceOf(\Udb\Entity\V1\DocumentGetRequest::class)
        ->and($documentGet->getResource()->getResourceName())->toBe('invoices')
        ->and($documentGet->getDocumentId())->toBe('document-php')
        ->and($documentFind)->toBeInstanceOf(\Udb\Entity\V1\DocumentFindRequest::class)
        ->and($documentFind->getLimit())->toBe(10)
        ->and($graph)->toBeInstanceOf(\Udb\Entity\V1\GraphQueryRequest::class)
        ->and($graph->getReadOnly())->toBeTrue()
        ->and($analytical)->toBeInstanceOf(\Udb\Entity\V1\AnalyticalQueryRequest::class)
        ->and($analytical->getQuery())->toBe('SELECT 1')
        ->and($select)->toBeInstanceOf(\Udb\Entity\V1\SelectRequest::class)
        ->and($select->getMessageType())->toBe('myapp.v1.Invoice')
        ->and($select->getLimit())->toBe(10)
        ->and($selectV2)->toBeInstanceOf(\Udb\Entity\V1\SelectRequest::class)
        ->and($selectV2->getLimit())->toBe(10)
        ->and($object)->toBeInstanceOf(\Udb\Entity\V1\ObjectRequest::class)
        ->and($object->getBucket())->toBe('bucket-php')
        ->and($timeSeries)->toBeInstanceOf(\Udb\Entity\V1\TimeSeriesQueryRequest::class)
        ->and($timeSeries->getResource()->getResourceName())->toBe('metrics_php')
        ->and($preview)->toBeInstanceOf(\Udb\Entity\V1\CdcRedactionPreviewRequest::class)
        ->and($preview->getPayloadJson())->toBe('{}')
        ->and($drift)->toBeInstanceOf(\Udb\Entity\V1\ProjectionDriftScanRequest::class)
        ->and($drift->getRowsPerTarget())->toBe(100);
});

it('manifest JSON body hydrates DataBroker CDC control mutation requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $pause = phpManifestJsonBody('pause_cdc', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $resume = phpManifestJsonBody('resume_cdc', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $stepDown = phpManifestJsonBody('step_down_cdc_leader', $fix, 'tenant-php', 'project-php', 'DataBroker');
    expect($pause)->toBeInstanceOf(\Udb\Entity\V1\CdcControlRequest::class)
        ->and($pause->getSlotName())->toBe('udb_cdc')
        ->and($pause->getReason())->toBe('maintenance')
        ->and($resume)->toBeInstanceOf(\Udb\Entity\V1\CdcControlRequest::class)
        ->and($resume->getReason())->toBe('resume')
        ->and($stepDown)->toBeInstanceOf(\Udb\Entity\V1\CdcControlRequest::class)
        ->and($stepDown->getReason())->toBe('failover');
});

it('manifest JSON body hydrates DataBroker unary mutation requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('record_id', 'record-php');
    $fix->set('bucket', 'bucket-php');
    $fix->set('object_key', 'object-php');
    $fix->set('mongo_collection', 'invoices');
    $fix->set('document_id', 'document-php');
    $url = phpManifestJsonBody('generate_presigned_url', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $multipart = phpManifestJsonBody('initiate_multipart_upload', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $doc = phpManifestJsonBody('document_upsert', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $graph = phpManifestJsonBody('graph_mutate', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $vector = phpManifestJsonBody('vector_upsert', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $view = phpManifestJsonBody('create_materialized_view', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $plan = phpManifestJsonBody('plan_migration', $fix, 'tenant-php', 'project-php', 'DataBroker');
    expect($url)->toBeInstanceOf(\Udb\Entity\V1\UrlRequest::class)
        ->and($url->getMethod())->toBe('GET')
        ->and($url->getTtlSeconds())->toBe(300)
        ->and($multipart)->toBeInstanceOf(\Udb\Entity\V1\MultipartUploadRequest::class)
        ->and($multipart->getPartCount())->toBe(1)
        ->and($doc)->toBeInstanceOf(\Udb\Entity\V1\DocumentUpsertRequest::class)
        ->and($doc->getDocumentId())->toBe('document-php')
        ->and($graph)->toBeInstanceOf(\Udb\Entity\V1\GraphMutationRequest::class)
        ->and($graph->getQuery())->toBe('CREATE (n:Node {id:$id})')
        ->and($vector)->toBeInstanceOf(\Udb\Entity\V1\VectorUpsertRequest::class)
        ->and(count($vector->getPoints()))->toBe(1)
        ->and($view)->toBeInstanceOf(\Udb\Entity\V1\ViewDefinition::class)
        ->and($view->getWithData())->toBeTrue()
        ->and($plan)->toBeInstanceOf(\Udb\Entity\V1\MigrationPlanRequest::class)
        ->and(iterator_to_array($plan->getContext()->getScopes()))->toBe(['udb:admin'])
        ->and($plan->getDryRun())->toBeTrue();
});

it('manifest JSON body hydrates DataBroker scalar action requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('object_key', 'cache-key-php');
    $fix->set('replay_dlq_id', 'replay-dlq-php');
    $fix->set('dismiss_dlq_id', 'dismiss-dlq-php');
    $fix->set('quarantine_dlq_id', 'quarantine-dlq-php');
    $fix->set('retry_saga_id', 'retry-saga-php');
    $fix->set('mark_saga_id', 'mark-saga-php');
    $fix->set('ds_policy_id', '42');
    $cacheDelete = phpManifestJsonBody('cache_delete', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $replay = phpManifestJsonBody('replay_dlq_event', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $dismiss = phpManifestJsonBody('dismiss_dlq_event', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $quarantine = phpManifestJsonBody('quarantine_dlq_event', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $retry = phpManifestJsonBody('retry_saga_compensation', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $reviewed = phpManifestJsonBody('mark_saga_reviewed', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $deletePolicy = phpManifestJsonBody('delete_policy', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $reload = phpManifestJsonBody('reload_policies', $fix, 'tenant-php', 'project-php', 'DataBroker');
    expect($cacheDelete)->toBeInstanceOf(\Udb\Entity\V1\CacheDeleteRequest::class)
        ->and($cacheDelete->getKey())->toBe('cache-key-php')
        ->and($replay)->toBeInstanceOf(\Udb\Entity\V1\DlqActionRequest::class)
        ->and($replay->getDlqId())->toBe('replay-dlq-php')
        ->and($replay->getPreserveEventId())->toBeFalse()
        ->and($dismiss)->toBeInstanceOf(\Udb\Entity\V1\DlqActionRequest::class)
        ->and($dismiss->getDlqId())->toBe('dismiss-dlq-php')
        ->and($quarantine)->toBeInstanceOf(\Udb\Entity\V1\DlqActionRequest::class)
        ->and($quarantine->getDlqId())->toBe('quarantine-dlq-php')
        ->and($retry)->toBeInstanceOf(\Udb\Entity\V1\SagaRequest::class)
        ->and($retry->getReason())->toBe('retry')
        ->and($reviewed)->toBeInstanceOf(\Udb\Entity\V1\SagaRequest::class)
        ->and($reviewed->getReason())->toBe('reviewed')
        ->and($deletePolicy)->toBeInstanceOf(\Udb\Entity\V1\PolicyRequest::class)
        ->and($deletePolicy->getPolicyId())->toBe(42)
        ->and($reload)->toBeInstanceOf(\Udb\Entity\V1\CapabilitiesRequest::class)
        ->and($reload->getProjectId())->toBe('project-php');
});

it('manifest JSON body hydrates DataBroker mutation and admin requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('message_type', 'myapp.v1.Invoice');
    $fix->set('record_id', 'record-php');
    $fix->set('object_key', 'cache-key-php');
    $fix->set('mongo_collection', 'invoices');
    $fix->set('document_id', 'document-php');
    $fix->set('apply_run_id', 'apply-run-php');
    $fix->set('approve_run_id', 'approve-run-php');
    $fix->set('approval_token', 'approval-token-php');
    $apply = phpManifestJsonBody('apply_migration', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $approve = phpManifestJsonBody('approve_migration_plan', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $batchSelect = phpManifestJsonBody('batch_select', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $batchUpsert = phpManifestJsonBody('batch_upsert', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $cacheSet = phpManifestJsonBody('cache_set', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $delete = phpManifestJsonBody('delete', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $documentDelete = phpManifestJsonBody('document_delete', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $baseline = phpManifestJsonBody('ensure_baseline', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $project = phpManifestJsonBody('ensure_project', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $ensureResource = phpManifestJsonBody('ensure_resource', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $dropResource = phpManifestJsonBody('drop_resource', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $generic = phpManifestJsonBody('generic_dispatch', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $publish = phpManifestJsonBody('publish_cdc', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $upsert = phpManifestJsonBody('upsert', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $vectorBatch = phpManifestJsonBody('vector_batch_upsert', $fix, 'tenant-php', 'project-php', 'DataBroker');
    expect($apply)->toBeInstanceOf(\Udb\Entity\V1\MigrationApplyRequest::class)
        ->and($apply->getApprovalToken())->toBe('approval-token-php')
        ->and($approve)->toBeInstanceOf(\Udb\Entity\V1\MigrationRunRequest::class)
        ->and($approve->getRunId())->toBe('approve-run-php')
        ->and($batchSelect)->toBeInstanceOf(\Udb\Entity\V1\SelectRequest::class)
        ->and($batchSelect->getLimit())->toBe(10)
        ->and($batchUpsert)->toBeInstanceOf(\Udb\Entity\V1\UpsertRequest::class)
        ->and($batchUpsert->getReturnRecord())->toBeTrue()
        ->and($cacheSet)->toBeInstanceOf(\Udb\Entity\V1\CacheSetRequest::class)
        ->and($cacheSet->getValue())->toBe('perf')
        ->and($delete)->toBeInstanceOf(\Udb\Entity\V1\DeleteRequest::class)
        ->and($delete->getMessageType())->toBe('myapp.v1.Invoice')
        ->and($documentDelete)->toBeInstanceOf(\Udb\Entity\V1\DocumentDeleteRequest::class)
        ->and($documentDelete->getDocumentId())->toBe('document-php')
        ->and($baseline)->toBeInstanceOf(\Udb\Services\V1\EnsureBaselineRequest::class)
        ->and(iterator_to_array($baseline->getContext()->getScopes()))->toBe(['udb:admin'])
        ->and($project)->toBeInstanceOf(\Udb\Entity\V1\EnsureProjectRequest::class)
        ->and($project->getCdcTopicPrefix())->toBe('project-php.')
        ->and($ensureResource)->toBeInstanceOf(\Udb\Entity\V1\ResourceAdminRequest::class)
        ->and($ensureResource->getResourceName())->toBe('invoices')
        ->and($dropResource)->toBeInstanceOf(\Udb\Entity\V1\ResourceAdminRequest::class)
        ->and($dropResource->getSpecJson())->toBe('{"udb_allow_rls_bypass":true}')
        ->and($generic)->toBeInstanceOf(\Udb\Entity\V1\GenericDispatchRequest::class)
        ->and($generic->getOperation())->toBe('ping')
        ->and($publish)->toBeInstanceOf(\Udb\Entity\V1\CDCSubscriptionRequest::class)
        ->and($publish->getTopicPattern())->toBe('*')
        ->and($upsert)->toBeInstanceOf(\Udb\Entity\V1\UpsertRequest::class)
        ->and($upsert->getReturnRecord())->toBeTrue()
        ->and($vectorBatch)->toBeInstanceOf(\Udb\Entity\V1\VectorUpsertRequest::class)
        ->and(count($vectorBatch->getPoints()))->toBe(1);
});

it('manifest JSON body hydrates every remaining DataBroker request', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('message_type', 'myapp.v1.Invoice');
    $fix->set('document_id', 'document-php');
    $fix->set('bucket', 'bucket-php');
    $fix->set('object_key', 'object-php');
    $fix->set('event_type', 'invoice.updated');
    $fix->set('ts_table', 'metrics_php');
    $fix->set('catalog_manifest_b64', 'e30=');
    $activate = phpManifestJsonBody('activate_catalog', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $begin = phpManifestJsonBody('begin_tx', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $enqueue = phpManifestJsonBody('enqueue_outbox_event', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $putObject = phpManifestJsonBody('put_object', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $putPolicy = phpManifestJsonBody('put_policy', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $rollback = phpManifestJsonBody('rollback_catalog', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $stage = phpManifestJsonBody('stage_catalog', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $timeSeries = phpManifestJsonBody('time_series_write', $fix, 'tenant-php', 'project-php', 'DataBroker');
    $validate = phpManifestJsonBody('validate_catalog', $fix, 'tenant-php', 'project-php', 'DataBroker');
    expect($activate)->toBeInstanceOf(\Udb\Entity\V1\CatalogVersionRequest::class)
        ->and($activate->getProjectId())->toBe('project-php')
        ->and($begin)->toBeInstanceOf(\Udb\Entity\V1\Mutation::class)
        ->and($begin->getOperation())->toBe('upsert')
        ->and($enqueue)->toBeInstanceOf(\Udb\Entity\V1\EnqueueOutboxEventRequest::class)
        ->and($enqueue->getTopic())->toBe('invoice.updated')
        ->and($putObject)->toBeInstanceOf(\Udb\Entity\V1\Chunk::class)
        ->and($putObject->getData())->toBe('perf')
        ->and($putObject->getFinalChunk())->toBeTrue()
        ->and($putPolicy)->toBeInstanceOf(\Udb\Entity\V1\PutPolicyRequest::class)
        ->and($putPolicy->getPolicy()->getEffect())->toBe('allow')
        ->and($putPolicy->getPolicy()->getEnabled())->toBeTrue()
        ->and($rollback)->toBeInstanceOf(\Udb\Entity\V1\CatalogVersionRequest::class)
        ->and($rollback->getProjectId())->toBe('project-php')
        ->and($stage)->toBeInstanceOf(\Udb\Entity\V1\StageCatalogRequest::class)
        ->and($stage->getManifestJson())->toBe('{}')
        ->and($stage->getReason())->toBe('stage')
        ->and($timeSeries)->toBeInstanceOf(\Udb\Entity\V1\TimeSeriesWriteRequest::class)
        ->and(count($timeSeries->getPoints()))->toBe(1)
        ->and($validate)->toBeInstanceOf(\Udb\Entity\V1\StageCatalogRequest::class)
        ->and($validate->getManifestJson())->toBe('{}')
        ->and($validate->getReason())->toBe('validate');
});

it('manifest JSON body hydrates StorageService read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('file_id', 'file-php');
    $fix->set('user_id', 'user-php');
    $get = phpManifestJsonBody('get_file', $fix, 'tenant-php', 'project-php');
    $download = phpManifestJsonBody('download_file', $fix, 'tenant-php', 'project-php');
    $list = phpManifestJsonBody('list_files', $fix, 'tenant-php', 'project-php');
    expect($get)->toBeInstanceOf(\Udb\Core\Storage\Services\V1\GetFileRequest::class)
        ->and($get->getTenantId())->toBe('tenant-php')
        ->and($get->getFileId())->toBe('file-php')
        ->and($download)->toBeInstanceOf(\Udb\Core\Storage\Services\V1\DownloadFileRequest::class)
        ->and($download->getChunkSizeBytes())->toBe(65536)
        ->and($list)->toBeInstanceOf(\Udb\Core\Storage\Services\V1\ListFilesRequest::class)
        ->and($list->getUploadedBy())->toBe('user-php')
        ->and($list->getPageSize())->toBe(20);
});

it('manifest JSON body hydrates ApiKeyService requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('key_id', 'key-php');
    $fix->set('plain_key', 'plain-php');
    $fix->set('owner_id', 'owner-php');
    $fix->set('project', 'project-php');
    $fix->set('update_key_id', 'update-key-php');
    $fix->set('revoke_key_id', 'revoke-key-php');
    $created = phpManifestJsonBody('create_api_key', $fix, 'tenant-php', 'project-php', 'ApiKeyService');
    $get = phpManifestJsonBody('get_api_key', $fix, 'tenant-php', 'project-php');
    $usage = phpManifestJsonBody('get_api_key_usage_stats', $fix, 'tenant-php', 'project-php');
    $list = phpManifestJsonBody('list_api_keys', $fix, 'tenant-php', 'project-php');
    $updated = phpManifestJsonBody('update_api_key', $fix, 'tenant-php', 'project-php', 'ApiKeyService');
    $revoked = phpManifestJsonBody('revoke_api_key', $fix, 'tenant-php', 'project-php', 'ApiKeyService');
    $rotated = phpManifestJsonBody('rotate_api_key', $fix, 'tenant-php', 'project-php', 'ApiKeyService');
    $emergency = phpManifestJsonBody('emergency_revoke_api_keys', $fix, 'tenant-php', 'project-php', 'ApiKeyService');
    $validate = phpManifestJsonBody('validate_api_key', $fix, 'tenant-php', 'project-php');
    expect($created)->toBeInstanceOf(\Udb\Core\Apikey\Services\V1\CreateApiKeyRequest::class)
        ->and($created->getOwnerId())->toBe('owner-php')
        ->and(count($created->getScopes()))->toBe(1)
        ->and($created->getContext()->getTenant()->getProjectId())->toBe('project-php')
        ->and($created->getContext()->getUserId())->toBe('owner-php')
        ->and($get)->toBeInstanceOf(\Udb\Core\Apikey\Services\V1\GetApiKeyRequest::class)
        ->and($get->getKeyId())->toBe('key-php')
        ->and($usage)->toBeInstanceOf(\Udb\Core\Apikey\Services\V1\GetApiKeyUsageStatsRequest::class)
        ->and($usage->getKeyId())->toBe('key-php')
        ->and($list)->toBeInstanceOf(\Udb\Core\Apikey\Services\V1\ListApiKeysRequest::class)
        ->and($list->getOwnerId())->toBe('owner-php')
        ->and($list->getPage()->getPageSize())->toBe(50)
        ->and($updated)->toBeInstanceOf(\Udb\Core\Apikey\Services\V1\UpdateApiKeyRequest::class)
        ->and($updated->getKeyId())->toBe('update-key-php')
        ->and($updated->getName())->toBe('bench-key-2')
        ->and(count($updated->getIpAllowlist()))->toBe(0)
        ->and($revoked)->toBeInstanceOf(\Udb\Core\Apikey\Services\V1\RevokeApiKeyRequest::class)
        ->and($revoked->getKeyId())->toBe('revoke-key-php')
        ->and($revoked->getRevokeReason())->toBe('bench cleanup')
        ->and($rotated)->toBeInstanceOf(\Udb\Core\Apikey\Services\V1\RotateApiKeyRequest::class)
        ->and($rotated->getKeyId())->toBe('key-php')
        ->and($rotated->getRotationReason())->toBe('bench rotate')
        ->and($emergency)->toBeInstanceOf(\Udb\Core\Apikey\Services\V1\EmergencyRevokeApiKeysRequest::class)
        ->and($emergency->getTenantId())->toBe('tenant-php')
        ->and($emergency->getProjectId())->toBe('project-php')
        ->and($emergency->getScope())->toBe('resource:read')
        ->and($validate)->toBeInstanceOf(\Udb\Core\Apikey\Services\V1\ValidateApiKeyRequest::class)
        ->and($validate->getPlainKey())->toBe('plain-php')
        ->and($validate->getRequiredScope())->toBe('resource:read');
});

it('manifest JSON body hydrates AuthnService read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('user_id', 'user-php');
    $fix->set('session_id', 'session-php');
    $fix->set('token', 'token-php');
    $fix->set('csrf_token', 'csrf-php');
    $fix->set('otp_id', 'otp-php');
    $fix->set('otp_code', '654321');
    $fix->set('challenge_id', 'challenge-php');
    $get = phpManifestJsonBody('get_user', $fix, 'tenant-php', 'project-php');
    $sessions = phpManifestJsonBody('list_sessions', $fix, 'tenant-php', 'project-php');
    $validate = phpManifestJsonBody('validate_token', $fix, 'tenant-php', 'project-php');
    $authenticate = phpManifestJsonBody('authenticate', $fix, 'tenant-php', 'project-php');
    $csrf = phpManifestJsonBody('validate_csrf', $fix, 'tenant-php', 'project-php');
    $otp = phpManifestJsonBody('verify_otp', $fix, 'tenant-php', 'project-php');
    $mfa = phpManifestJsonBody('verify_mfa_challenge', $fix, 'tenant-php', 'project-php');
    expect($get)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\GetUserRequest::class)
        ->and($get->getUserId())->toBe('user-php')
        ->and($sessions)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\ListSessionsRequest::class)
        ->and($sessions->getUserId())->toBe('user-php')
        ->and($sessions->getActiveOnly())->toBeTrue()
        ->and($sessions->getPage()->getPageSize())->toBe(20)
        ->and($validate)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\ValidateTokenRequest::class)
        ->and($validate->getToken())->toBe('token-php')
        ->and($authenticate)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\AuthnRequest::class)
        ->and($authenticate->getBearerToken())->toBe('token-php')
        ->and($csrf)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\ValidateCSRFRequest::class)
        ->and($csrf->getCsrfToken())->toBe('csrf-php')
        ->and($otp)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\VerifyOTPRequest::class)
        ->and($otp->getOtpId())->toBe('otp-php')
        ->and($otp->getCode())->toBe('654321')
        ->and($mfa)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\VerifyMfaChallengeRequest::class)
        ->and($mfa->getChallengeId())->toBe('challenge-php');
});

it('manifest JSON body hydrates AuthnService session and MFA setup requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('project', 'project-php');
    $fix->set('username', 'bench-user');
    $fix->set('user_id', 'user-php');
    $fix->set('subject', 'subject-php');
    $fix->set('refresh_token', 'refresh-php');
    $fix->set('refresh_session_id', 'refresh-session-php');
    $fix->set('otp_id', 'otp-php');
    $login = phpManifestJsonBody('login', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $refreshToken = phpManifestJsonBody('refresh_token', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $refreshSession = phpManifestJsonBody('refresh_session', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $session = phpManifestJsonBody('create_session', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $created = phpManifestJsonBody('create_user', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $updated = phpManifestJsonBody('update_user', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $sentOtp = phpManifestJsonBody('send_otp', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $resentOtp = phpManifestJsonBody('resend_otp', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $enrolled = phpManifestJsonBody('enroll_mfa', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $recovery = phpManifestJsonBody('generate_recovery_codes', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $policy = phpManifestJsonBody('put_mfa_policy', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $forgot = phpManifestJsonBody('forgot_password', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $phone = phpManifestJsonBody('send_phone_verification', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $challenge = phpManifestJsonBody('issue_mfa_challenge', $fix, 'tenant-php', 'project-php', 'AuthnService');
    expect($login)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\LoginRequest::class)
        ->and($login->getUsername())->toBe('bench-user')
        ->and($login->getPassword())->toBe('CorrectHorse1!')
        ->and($login->getDeviceType())->not->toBe(0)
        ->and($login->getProjectHint())->toBe('project-php')
        ->and($refreshToken)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\RefreshTokenRequest::class)
        ->and($refreshToken->getRefreshToken())->toBe('refresh-php')
        ->and($refreshSession)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\RefreshSessionRequest::class)
        ->and($refreshSession->getSessionId())->toBe('refresh-session-php')
        ->and($refreshSession->getTtlSeconds())->toBe(3600)
        ->and($session)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\CreateSessionRequest::class)
        ->and($session->getPrincipal()->getSubject())->toBe('subject-php')
        ->and($session->getTtlSeconds())->toBe(3600)
        ->and($created)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\CreateUserRequest::class)
        ->and($created->getUsername())->toBe('perf-u')
        ->and($created->getAccountKind())->not->toBe(0)
        ->and($updated)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\UpdateUserRequest::class)
        ->and($updated->getFullName())->toBe('Perf U2')
        ->and($updated->getEmail())->toBe('perf-u2@acme.test')
        ->and($sentOtp)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\SendOTPRequest::class)
        ->and($sentOtp->getOtpType())->not->toBe(0)
        ->and($resentOtp)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\ResendOTPRequest::class)
        ->and($resentOtp->getOriginalOtpId())->toBe('otp-php')
        ->and($enrolled)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\EnrollMFARequest::class)
        ->and($enrolled->getMfaType())->not->toBe(0)
        ->and($recovery)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\GenerateRecoveryCodesRequest::class)
        ->and($recovery->getCount())->toBe(10)
        ->and($policy)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\PutMfaPolicyRequest::class)
        ->and($policy->getRequireMfa())->toBeFalse()
        ->and($forgot)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\ForgotPasswordRequest::class)
        ->and($forgot->getIdentifier())->toBe('perf-u@acme.test')
        ->and($phone)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\SendPhoneVerificationRequest::class)
        ->and($phone->getPhone())->toBe('+15551234567')
        ->and($challenge)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\IssueMfaChallengeRequest::class)
        ->and($challenge->getPurpose())->not->toBe(0);
});

it('manifest JSON body hydrates AuthnService terminal and WebAuthn requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('user_id', 'user-php');
    $fix->set('session_id', 'session-php');
    $fix->set('subject', 'subject-php');
    $fix->set('code', 'code-php');
    $fix->set('reset_otp_id', 'reset-otp-php');
    $fix->set('reset_otp_code', '135790');
    $fix->set('device_id', 'device-php');
    $fix->set('record_id', 'credential-php');
    $fix->set('reg_challenge_id', 'reg-challenge-php');
    $fix->set('auth_challenge_id', 'auth-challenge-php');
    $logout = phpManifestJsonBody('logout', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $revoked = phpManifestJsonBody('revoke_session', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $adminRevoked = phpManifestJsonBody('admin_revoke_session', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $adminAllUsers = phpManifestJsonBody('admin_revoke_all_user_sessions', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $adminAllTenant = phpManifestJsonBody('admin_revoke_all_tenant_sessions', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $emergency = phpManifestJsonBody('emergency_revoke', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $changedPassword = phpManifestJsonBody('change_password', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $resetPassword = phpManifestJsonBody('reset_password', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $changedStatus = phpManifestJsonBody('change_user_status', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $adminResetPassword = phpManifestJsonBody('admin_reset_password', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $confirmedMfa = phpManifestJsonBody('confirm_mfaenrollment', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $disabledMfa = phpManifestJsonBody('disable_mfa_factor', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $renamed = phpManifestJsonBody('rename_passkey', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $revokedRecovery = phpManifestJsonBody('revoke_recovery_codes', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $adminResetMfa = phpManifestJsonBody('admin_reset_mfa', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $revokedDevice = phpManifestJsonBody('revoke_device', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $deletedWebAuthn = phpManifestJsonBody('delete_web_authn_credential', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $startedReg = phpManifestJsonBody('start_web_authn_registration', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $finishedReg = phpManifestJsonBody('finish_web_authn_registration', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $startedAuth = phpManifestJsonBody('start_web_authn_authentication', $fix, 'tenant-php', 'project-php', 'AuthnService');
    $finishedAuth = phpManifestJsonBody('finish_web_authn_authentication', $fix, 'tenant-php', 'project-php', 'AuthnService');
    expect($logout)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\LogoutRequest::class)
        ->and($logout->getSessionId())->toBe('session-php')
        ->and($revoked)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\RevokeSessionRequest::class)
        ->and($revoked->getRevokeReason())->toBe('perf')
        ->and($adminRevoked)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\AdminRevokeSessionRequest::class)
        ->and($adminRevoked->getReason())->toBe('perf')
        ->and($adminAllUsers)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\AdminRevokeAllUserSessionsRequest::class)
        ->and($adminAllUsers->getUserId())->toBe('user-php')
        ->and($adminAllTenant)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\AdminRevokeAllTenantSessionsRequest::class)
        ->and($adminAllTenant->getTenantId())->toBe('tenant-php')
        ->and($emergency)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\EmergencyRevokeRequest::class)
        ->and($emergency->getPrincipalId())->toBe('subject-php')
        ->and($changedPassword)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\ChangePasswordRequest::class)
        ->and($changedPassword->getCurrentPassword())->toBe('CorrectHorse1!')
        ->and($changedPassword->getOtpId())->toBe('')
        ->and($resetPassword)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\ResetPasswordRequest::class)
        ->and($resetPassword->getCode())->toBe('135790')
        ->and($changedStatus)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\ChangeUserStatusRequest::class)
        ->and($changedStatus->getNewStatus())->not->toBe(0)
        ->and($adminResetPassword)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\AdminResetPasswordRequest::class)
        ->and($adminResetPassword->getUserId())->toBe('user-php')
        ->and($confirmedMfa)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\ConfirmMFAEnrollmentRequest::class)
        ->and($confirmedMfa->getOtpId())->toBe('code-php')
        ->and($disabledMfa)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\DisableMfaFactorRequest::class)
        ->and($disabledMfa->getFactorKind())->not->toBe(0)
        ->and($renamed)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\RenamePasskeyRequest::class)
        ->and($renamed->getNewLabel())->toBe('perf-key2')
        ->and($revokedRecovery)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\RevokeRecoveryCodesRequest::class)
        ->and($revokedRecovery->getUserId())->toBe('user-php')
        ->and($adminResetMfa)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\AdminResetMfaRequest::class)
        ->and($adminResetMfa->getReason())->toBe('perf')
        ->and($revokedDevice)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\RevokeDeviceRequest::class)
        ->and($revokedDevice->getDeviceId())->toBe('device-php')
        ->and($deletedWebAuthn)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\DeleteWebAuthnCredentialRequest::class)
        ->and($deletedWebAuthn->getCredentialId())->toBe('credential-php')
        ->and($startedReg)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\StartWebAuthnRegistrationRequest::class)
        ->and($startedReg->getLabel())->toBe('perf-key')
        ->and($finishedReg)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\FinishWebAuthnRegistrationRequest::class)
        ->and($finishedReg->getChallengeId())->toBe('reg-challenge-php')
        ->and($finishedReg->getPublicKeyCredentialJson())->toBe('__UDB_WEBAUTHN_TEST__')
        ->and($startedAuth)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\StartWebAuthnAuthenticationRequest::class)
        ->and($startedAuth->getTenantId())->toBe('tenant-php')
        ->and($finishedAuth)->toBeInstanceOf(\Udb\Core\Authn\Services\V1\FinishWebAuthnAuthenticationRequest::class)
        ->and($finishedAuth->getChallengeId())->toBe('auth-challenge-php')
        ->and($finishedAuth->getPublicKeyCredentialJson())->toBe('__UDB_WEBAUTHN_TEST__');
});

it('manifest JSON body hydrates IdentityProviderService read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('provider_id', 'provider-php');
    $get = phpManifestJsonBody('get_provider', $fix, 'tenant-php', 'project-php');
    $list = phpManifestJsonBody('list_providers', $fix, 'tenant-php', 'project-php');
    $claims = phpManifestJsonBody('preview_claim_mapping', $fix, 'tenant-php', 'project-php');
    $groups = phpManifestJsonBody('preview_group_mapping', $fix, 'tenant-php', 'project-php');
    expect($get)->toBeInstanceOf(\Udb\Core\Idp\Services\V1\GetProviderRequest::class)
        ->and($get->getProviderId())->toBe('provider-php')
        ->and($get->getTenantId())->toBe('tenant-php')
        ->and($list)->toBeInstanceOf(\Udb\Core\Idp\Services\V1\ListProvidersRequest::class)
        ->and($list->getPage()->getPageSize())->toBe(20)
        ->and($claims)->toBeInstanceOf(\Udb\Core\Idp\Services\V1\PreviewClaimMappingRequest::class)
        ->and($claims->getClaimsJson())->toBe('{"sub":"abc","email":"a@x.com"}')
        ->and($groups)->toBeInstanceOf(\Udb\Core\Idp\Services\V1\PreviewGroupMappingRequest::class)
        ->and(iterator_to_array($groups->getGroups()))->toBe(['admins']);
});

it('manifest JSON body hydrates AssetService requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('asset_id', 'asset-php');
    $fix->set('definition_id', 'definition-php');
    $fix->set('file_id', 'file-php');
    $fix->set('instance_id', 'instance-php');
    $fix->set('project', 'project-php');
    $fix->set('step_id', 'step-php');
    $complete = phpManifestJsonBody('complete_step', $fix, 'tenant-php', 'project-php', 'AssetService');
    $created = phpManifestJsonBody('create_pipeline_definition', $fix, 'tenant-php', 'project-php', 'AssetService');
    $asset = phpManifestJsonBody('get_asset', $fix, 'tenant-php', 'project-php', 'AssetService');
    $definition = phpManifestJsonBody('get_pipeline_definition', $fix, 'tenant-php', 'project-php', 'AssetService');
    $pipeline = phpManifestJsonBody('get_pipeline', $fix, 'tenant-php', 'project-php', 'AssetService');
    $list = phpManifestJsonBody('list_assets', $fix, 'tenant-php', 'project-php', 'AssetService');
    $registered = phpManifestJsonBody('register_asset', $fix, 'tenant-php', 'project-php', 'AssetService');
    $started = phpManifestJsonBody('start_pipeline', $fix, 'tenant-php', 'project-php', 'AssetService');
    expect($complete)->toBeInstanceOf(\Udb\Core\Asset\Services\V1\CompleteStepRequest::class)
        ->and($complete->getStepId())->toBe('step-php')
        ->and($complete->getStatus())->toBe('COMPLETED')
        ->and($complete->getResult())->toBe('{}')
        ->and($created)->toBeInstanceOf(\Udb\Core\Asset\Services\V1\CreatePipelineDefinitionRequest::class)
        ->and($created->getSteps())->toBe('[{"name":"resize","type":"TRANSFORM"}]')
        ->and($created->getVersion())->toBe(1)
        ->and($asset)->toBeInstanceOf(\Udb\Core\Asset\Services\V1\GetAssetRequest::class)
        ->and($asset->getAssetId())->toBe('asset-php')
        ->and($definition)->toBeInstanceOf(\Udb\Core\Asset\Services\V1\GetPipelineDefinitionRequest::class)
        ->and($definition->getDefinitionId())->toBe('definition-php')
        ->and($pipeline)->toBeInstanceOf(\Udb\Core\Asset\Services\V1\GetPipelineRequest::class)
        ->and($pipeline->getInstanceId())->toBe('instance-php')
        ->and($list)->toBeInstanceOf(\Udb\Core\Asset\Services\V1\ListAssetsRequest::class)
        ->and($list->getMediaType())->toBe('image/png')
        ->and($list->getPageSize())->toBe(20)
        ->and($registered)->toBeInstanceOf(\Udb\Core\Asset\Services\V1\RegisterAssetRequest::class)
        ->and($registered->getProjectId())->toBe('project-php')
        ->and($registered->getFileId())->toBe('file-php')
        ->and($registered->getMetadata())->toBe('{"source":"upload"}')
        ->and($started)->toBeInstanceOf(\Udb\Core\Asset\Services\V1\StartPipelineRequest::class)
        ->and($started->getDefinitionId())->toBe('definition-php')
        ->and($started->getAssetId())->toBe('asset-php')
        ->and($started->getCorrelationId())->toBe('run-001');
});

it('manifest JSON body hydrates WebRTC read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('room_id', 'room-php');
    $fix->set('peer_id', 'peer-php');
    $fix->set('track_id', 'track-php');
    $room = phpManifestJsonBody('get_room', $fix, 'tenant-php', 'project-php');
    $rooms = phpManifestJsonBody('list_rooms', $fix, 'tenant-php', 'project-php');
    $egress = phpManifestJsonBody('list_egress', $fix, 'tenant-php', 'project-php');
    $peer = phpManifestJsonBody('get_peer', $fix, 'tenant-php', 'project-php');
    $peers = phpManifestJsonBody('list_peers', $fix, 'tenant-php', 'project-php');
    $tracks = phpManifestJsonBody('list_tracks', $fix, 'tenant-php', 'project-php');
    expect($room)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\GetRoomRequest::class)
        ->and($room->getRoomId())->toBe('room-php')
        ->and($rooms)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\ListRoomsRequest::class)
        ->and($rooms->getState())->toBe('active')
        ->and($rooms->getPageSize())->toBe(20)
        ->and($egress)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\ListEgressRequest::class)
        ->and($egress->getRoomId())->toBe('room-php')
        ->and($peer)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\GetPeerRequest::class)
        ->and($peer->getPeerId())->toBe('peer-php')
        ->and($peers)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\ListPeersRequest::class)
        ->and($peers->getState())->toBe('connected')
        ->and($tracks)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\ListTracksRequest::class)
        ->and($tracks->getKind())->toBe('audio')
        ->and($tracks->getPageSize())->toBe(20);
});

it('manifest JSON body hydrates RoomService mutation requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('room_id', 'room-php');
    $fix->set('close_room_id', 'close-room-php');
    $fix->set('track_id', 'track-php');
    $fix->set('object_key', 'object-php');
    $fix->set('egress_id', 'egress-php');
    $fix->set('user_id', 'user-php');
    $created = phpManifestJsonBody('create_room', $fix, 'tenant-php', 'project-php', 'RoomService');
    $updated = phpManifestJsonBody('update_room', $fix, 'tenant-php', 'project-php', 'RoomService');
    $closed = phpManifestJsonBody('close_room', $fix, 'tenant-php', 'project-php', 'RoomService');
    $composite = phpManifestJsonBody('start_room_composite', $fix, 'tenant-php', 'project-php', 'RoomService');
    $trackEgress = phpManifestJsonBody('start_track_egress', $fix, 'tenant-php', 'project-php', 'RoomService');
    $stopped = phpManifestJsonBody('stop_egress', $fix, 'tenant-php', 'project-php', 'RoomService');
    expect($created)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\CreateRoomRequest::class)
        ->and($created->getCreatedBy())->toBe('user-php')
        ->and($created->getMaxParticipants())->toBe(10)
        ->and($created->getConfig())->toBe('{}')
        ->and($updated)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\UpdateRoomRequest::class)
        ->and($updated->getName())->toBe('bench-room-2')
        ->and($updated->getState())->toBe('active')
        ->and($closed)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\CloseRoomRequest::class)
        ->and($closed->getRoomId())->toBe('close-room-php')
        ->and($composite)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\StartRoomCompositeRequest::class)
        ->and($composite->getDestination())->toBe('object-php')
        ->and($composite->getOptions())->toBe('{}')
        ->and($trackEgress)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\StartTrackEgressRequest::class)
        ->and($trackEgress->getTrackId())->toBe('track-php')
        ->and($trackEgress->getFormat())->toBe('mp4')
        ->and($stopped)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\StopEgressRequest::class)
        ->and($stopped->getEgressId())->toBe('egress-php');
});

it('manifest JSON body hydrates PeerService mutation requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('room_id', 'room-php');
    $fix->set('join_session_room_id', 'join-session-room-php');
    $fix->set('leave_peer_id', 'leave-peer-php');
    $joined = phpManifestJsonBody('join_room', $fix, 'tenant-php', 'project-php', 'PeerService');
    $session = phpManifestJsonBody('join_session', $fix, 'tenant-php', 'project-php', 'PeerService');
    $left = phpManifestJsonBody('leave_room', $fix, 'tenant-php', 'project-php', 'PeerService');
    expect($joined)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\JoinRoomRequest::class)
        ->and($joined->getDisplayName())->toBe('Bench User')
        ->and($joined->getMetadata())->toBe('{}')
        ->and($joined->getUserAgent())->toBe('bench/1.0')
        ->and($session)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\JoinSessionRequest::class)
        ->and($session->getRoomId())->toBe('join-session-room-php')
        ->and($session->getTtlSeconds())->toBe(3600)
        ->and($left)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\LeaveRoomRequest::class)
        ->and($left->getPeerId())->toBe('leave-peer-php');
});

it('manifest JSON body hydrates TrackService mutation requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('room_id', 'room-php');
    $fix->set('peer_id', 'peer-php');
    $fix->set('track_id', 'track-php');
    $fix->set('unpublish_track_id', 'track-disposable-php');
    $published = phpManifestJsonBody('publish_track', $fix, 'tenant-php', 'project-php', 'TrackService');
    $muted = phpManifestJsonBody('mute_track', $fix, 'tenant-php', 'project-php', 'TrackService');
    $unpublished = phpManifestJsonBody('unpublish_track', $fix, 'tenant-php', 'project-php', 'TrackService');
    expect($published)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\PublishTrackRequest::class)
        ->and($published->getKind())->toBe('audio')
        ->and($published->getLabel())->toBe('mic')
        ->and($published->getSettings())->toBe('{}')
        ->and($published->getMetadata())->toBe('{}')
        ->and($muted)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\MuteTrackRequest::class)
        ->and($muted->getTrackId())->toBe('track-php')
        ->and($muted->getMuted())->toBeTrue()
        ->and($unpublished)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\UnpublishTrackRequest::class)
        ->and($unpublished->getTrackId())->toBe('track-disposable-php');
});

it('manifest JSON body hydrates NotificationService read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('user_id', 'user-php');
    $fix->set('event_type', 'event-php');
    $fix->set('log_id', 'log-php');
    $stats = phpManifestJsonBody('get_delivery_stats', $fix, 'tenant-php', 'project-php');
    $notification = phpManifestJsonBody('get_notification', $fix, 'tenant-php', 'project-php');
    $preference = phpManifestJsonBody('get_preference', $fix, 'tenant-php', 'project-php');
    $template = phpManifestJsonBody('get_template', $fix, 'tenant-php', 'project-php');
    $notifications = phpManifestJsonBody('list_notifications', $fix, 'tenant-php', 'project-php');
    $preferences = phpManifestJsonBody('list_preferences', $fix, 'tenant-php', 'project-php');
    $templates = phpManifestJsonBody('list_templates', $fix, 'tenant-php', 'project-php');
    expect($stats)->toBeInstanceOf(\Udb\Core\Notification\Services\V1\GetDeliveryStatsRequest::class)
        ->and($stats->getEventType())->toBe('event-php')
        ->and($stats->getDateTo())->toBe('2026-12-31')
        ->and($notification)->toBeInstanceOf(\Udb\Core\Notification\Services\V1\GetNotificationRequest::class)
        ->and($notification->getLogId())->toBe('log-php')
        ->and($preference)->toBeInstanceOf(\Udb\Core\Notification\Services\V1\GetPreferenceRequest::class)
        ->and($preference->getUserId())->toBe('user-php')
        ->and($template)->toBeInstanceOf(\Udb\Core\Notification\Services\V1\GetTemplateRequest::class)
        ->and($template->getLocale())->toBe('en')
        ->and($notifications)->toBeInstanceOf(\Udb\Core\Notification\Services\V1\ListNotificationsRequest::class)
        ->and($notifications->getPage()->getPageSize())->toBe(20)
        ->and($preferences)->toBeInstanceOf(\Udb\Core\Notification\Services\V1\ListPreferencesRequest::class)
        ->and($preferences->getUserId())->toBe('user-php')
        ->and($templates)->toBeInstanceOf(\Udb\Core\Notification\Services\V1\ListTemplatesRequest::class)
        ->and($templates->getPage()->getPageSize())->toBe(20);
});

it('manifest JSON body hydrates NotificationService mutation requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('project', 'project-php');
    $fix->set('user_id', 'user-php');
    $fix->set('event_type', 'event-php');
    $fix->set('log_id', 'log-php');
    $sent = phpManifestJsonBody('send_notification', $fix, 'tenant-php', 'project-php', 'NotificationService');
    $reported = phpManifestJsonBody('report_delivery', $fix, 'tenant-php', 'project-php', 'NotificationService');
    $retried = phpManifestJsonBody('retry_notification', $fix, 'tenant-php', 'project-php', 'NotificationService');
    $preference = phpManifestJsonBody('set_preference', $fix, 'tenant-php', 'project-php', 'NotificationService');
    $template = phpManifestJsonBody('upsert_template', $fix, 'tenant-php', 'project-php', 'NotificationService');
    $sendVariables = $sent ? iterator_to_array($sent->getVariables()) : [];
    expect($sent)->toBeInstanceOf(\Udb\Core\Notification\Services\V1\SendNotificationRequest::class)
        ->and($sent->getProjectId())->toBe('project-php')
        ->and($sendVariables['name'] ?? null)->toBe('SDK')
        ->and(count($sent->getChannels()))->toBe(1)
        ->and($sent->getContext()->getPurpose())->toBe('go.live.perf')
        ->and($reported)->toBeInstanceOf(\Udb\Core\Notification\Services\V1\ReportDeliveryRequest::class)
        ->and($reported->getProvider())->toBe('sdk-perf')
        ->and($reported->getProviderMessageId())->toBe('sdk-perf-delivery')
        ->and($reported->getContext()->getTenant()->getProjectId())->toBe('project-php')
        ->and($retried)->toBeInstanceOf(\Udb\Core\Notification\Services\V1\RetryNotificationRequest::class)
        ->and($retried->getLogId())->toBe('log-php')
        ->and($retried->getContext()->getPurpose())->toBe('go.live.perf')
        ->and($preference)->toBeInstanceOf(\Udb\Core\Notification\Services\V1\SetPreferenceRequest::class)
        ->and($preference->getIsOptedOut())->toBeTrue()
        ->and($preference->getEventType())->toBe('')
        ->and($template)->toBeInstanceOf(\Udb\Core\Notification\Services\V1\UpsertTemplateRequest::class)
        ->and($template->getSubjectTemplate())->toBe('Hello {name}')
        ->and($template->getBodyTemplate())->toBe('Body {name}')
        ->and($template->getIsActive())->toBeTrue();
});

it('manifest JSON body hydrates CacheService read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('object_key', 'cache-key-php');
    $get = phpManifestJsonBody('cache_get', $fix, 'tenant-php', 'project-php', 'CacheService');
    $stats = phpManifestJsonBody('get_cache_namespace_stats', $fix, 'tenant-php', 'project-php', 'CacheService');
    $scan = phpManifestJsonBody('cache_scan', $fix, 'tenant-php', 'project-php', 'CacheService');
    expect($get)->toBeInstanceOf(\Udb\Core\Cache\Services\V1\GetRequest::class)
        ->and($get->getTenantId())->toBe('tenant-php')
        ->and($get->getKey())->toBe('cache-key-php')
        ->and($stats)->toBeInstanceOf(\Udb\Core\Cache\Services\V1\GetNamespaceStatsRequest::class)
        ->and($stats->getNamespace())->toBe('sdk-perf-cache')
        ->and($scan)->toBeInstanceOf(\Udb\Core\Cache\Services\V1\ScanRequest::class)
        ->and($scan->getLimit())->toBe(50)
        ->and($scan->getPageToken())->toBe('0');
});

it('manifest JSON body hydrates CacheService mutation requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('object_key', 'cache-key-php');
    $created = phpManifestJsonBody('create_cache_namespace', $fix, 'tenant-php', 'project-php', 'CacheService');
    $set = phpManifestJsonBody('cache_set', $fix, 'tenant-php', 'project-php', 'CacheService');
    $deleted = phpManifestJsonBody('cache_delete', $fix, 'tenant-php', 'project-php', 'CacheService');
    $dropped = phpManifestJsonBody('delete_cache_namespace', $fix, 'tenant-php', 'project-php', 'CacheService');
    expect($created)->toBeInstanceOf(\Udb\Core\Cache\Services\V1\CreateNamespaceRequest::class)
        ->and($created->getNamespace())->toBe('sdk-perf-cache')
        ->and($created->getMaxBytes())->toBe(1048576)
        ->and($created->getDefaultTtlSeconds())->toBe(300)
        ->and($set)->toBeInstanceOf(\Udb\Core\Cache\Services\V1\SetRequest::class)
        ->and($set->getKey())->toBe('cache-key-php')
        ->and($set->getValue())->toBe('perf')
        ->and($set->getTtlSeconds())->toBe(300)
        ->and($deleted)->toBeInstanceOf(\Udb\Core\Cache\Services\V1\DeleteRequest::class)
        ->and($deleted->getKey())->toBe('cache-key-php')
        ->and($dropped)->toBeInstanceOf(\Udb\Core\Cache\Services\V1\DeleteNamespaceRequest::class)
        ->and($dropped->getConfirmationToken())->toBe('sdk-perf-cache');
});

it('manifest JSON body hydrates MeteringService read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $check = phpManifestJsonBody('check_quota', $fix, 'tenant-php', 'project-php');
    $quota = phpManifestJsonBody('get_quota', $fix, 'tenant-php', 'project-php');
    $list = phpManifestJsonBody('list_quotas', $fix, 'tenant-php', 'project-php');
    $usage = phpManifestJsonBody('query_usage', $fix, 'tenant-php', 'project-php');
    expect($check)->toBeInstanceOf(\Udb\Core\Metering\Services\V1\CheckQuotaRequest::class)
        ->and($check->getMetric())->toBe('sdk.perf.request')
        ->and($quota)->toBeInstanceOf(\Udb\Core\Metering\Services\V1\GetQuotaRequest::class)
        ->and($quota->getProjectId())->toBe('project-php')
        ->and($list)->toBeInstanceOf(\Udb\Core\Metering\Services\V1\ListQuotasRequest::class)
        ->and($list->getLimit())->toBe(50)
        ->and($list->getPageSize())->toBe(50)
           ->and($usage)->toBeInstanceOf(\Udb\Core\Metering\Services\V1\QueryUsageRequest::class)
           ->and($usage->getWindowSeconds())->toBe(86400);
});

it('manifest JSON body hydrates MeteringService mutation requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('project', 'project-php');
    $fix->set('user_id', 'user-php');
    $quota = phpManifestJsonBody('put_quota', $fix, 'tenant-php', 'project-php');
    $usage = phpManifestJsonBody('record_usage', $fix, 'tenant-php', 'project-php');
    expect($quota)->toBeInstanceOf(\Udb\Core\Metering\Services\V1\PutQuotaRequest::class)
        ->and($quota->getLimitValue())->toBe(1000000)
        ->and($quota->getEnabled())->toBeTrue()
        ->and($quota->getMetadataJson())->toBe('{}')
        ->and($usage)->toBeInstanceOf(\Udb\Core\Metering\Services\V1\RecordUsageRequest::class)
        ->and($usage->getPrincipalId())->toBe('user-php')
        ->and($usage->getQuantity())->toBe(1)
        ->and($usage->getUnit())->toBe('request');
});

it('manifest JSON body hydrates LockService requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('user_id', 'user-php');
    $fix->set('fencing_token', '77');
    $acquired = phpManifestJsonBody('acquire_lock', $fix, 'tenant-php', 'project-php', 'LockService');
    $renewed = phpManifestJsonBody('renew_lock', $fix, 'tenant-php', 'project-php', 'LockService');
    $released = phpManifestJsonBody('release_lock', $fix, 'tenant-php', 'project-php', 'LockService');
    expect($acquired)->toBeInstanceOf(\Udb\Core\Lock\Services\V1\AcquireLockRequest::class)
        ->and($acquired->getLeaseTtlSeconds())->toBe(60)
        ->and($acquired->getMetadataJson())->toBe('{}')
        ->and($renewed)->toBeInstanceOf(\Udb\Core\Lock\Services\V1\RenewLockRequest::class)
        ->and($renewed->getFencingToken())->toBe(77)
        ->and($released)->toBeInstanceOf(\Udb\Core\Lock\Services\V1\ReleaseLockRequest::class)
        ->and($released->getOwnerId())->toBe('user-php')
        ->and($released->getFencingToken())->toBe(77);
});

it('manifest JSON body hydrates SchedulerService read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('job_id', 'job-php');
    $job = phpManifestJsonBody('get_job', $fix, 'tenant-php', 'project-php');
    $jobs = phpManifestJsonBody('list_jobs', $fix, 'tenant-php', 'project-php');
    expect($job)->toBeInstanceOf(\Udb\Core\Scheduler\Services\V1\GetJobRequest::class)
        ->and($job->getJobId())->toBe('job-php')
        ->and($jobs)->toBeInstanceOf(\Udb\Core\Scheduler\Services\V1\ListJobsRequest::class)
        ->and($jobs->getTenantId())->toBe('tenant-php')
        ->and($jobs->getPageSize())->toBe(20);
});

it('manifest JSON body hydrates SchedulerService mutation requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('project', 'project-php');
    $fix->set('job_id', 'job-php');
    $created = phpManifestJsonBody('create_job', $fix, 'tenant-php', 'project-php');
    $paused = phpManifestJsonBody('pause_job', $fix, 'tenant-php', 'project-php');
    $resumed = phpManifestJsonBody('resume_job', $fix, 'tenant-php', 'project-php');
    $deleted = phpManifestJsonBody('delete_job', $fix, 'tenant-php', 'project-php');
    expect($created)->toBeInstanceOf(\Udb\Core\Scheduler\Services\V1\CreateJobRequest::class)
        ->and($created->getProjectId())->toBe('project-php')
        ->and($created->getName())->toBe('sdk-perf-job')
        ->and($created->getScheduleType())->toBe('CRON')
        ->and($created->getCronExpression())->toBe('*/5 * * * *')
        ->and($created->getPayload())->toBe('{}')
        ->and($created->getTargetTopic())->toBe('sdk.perf.scheduler')
        ->and($created->getMaxAttempts())->toBe(3)
        ->and($created->getBackoffSeconds())->toBe(30)
        ->and($paused)->toBeInstanceOf(\Udb\Core\Scheduler\Services\V1\PauseJobRequest::class)
        ->and($paused->getJobId())->toBe('job-php')
        ->and($resumed)->toBeInstanceOf(\Udb\Core\Scheduler\Services\V1\ResumeJobRequest::class)
        ->and($resumed->getJobId())->toBe('job-php')
        ->and($deleted)->toBeInstanceOf(\Udb\Core\Scheduler\Services\V1\DeleteJobRequest::class)
        ->and($deleted->getJobId())->toBe('job-php');
});

it('manifest JSON body hydrates WebhookService read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('endpoint_id', 'endpoint-php');
    $endpoint = phpManifestJsonBody('get_endpoint', $fix, 'tenant-php', 'project-php');
    $deliveries = phpManifestJsonBody('list_deliveries', $fix, 'tenant-php', 'project-php');
    $endpoints = phpManifestJsonBody('list_endpoints', $fix, 'tenant-php', 'project-php');
    expect($endpoint)->toBeInstanceOf(\Udb\Core\Webhook\Services\V1\GetEndpointRequest::class)
        ->and($endpoint->getEndpointId())->toBe('endpoint-php')
        ->and($deliveries)->toBeInstanceOf(\Udb\Core\Webhook\Services\V1\ListDeliveriesRequest::class)
        ->and($deliveries->getPageSize())->toBe(20)
        ->and($endpoints)->toBeInstanceOf(\Udb\Core\Webhook\Services\V1\ListEndpointsRequest::class)
        ->and($endpoints->getActiveOnly())->toBeTrue();
});

it('manifest JSON body hydrates WebhookService mutation requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('endpoint_id', 'endpoint-php');
    $fix->set('delete_endpoint_id', 'endpoint-delete-php');
    $fix->set('topic_pattern', 'tenant-php.*');
    $created = phpManifestJsonBody('create_endpoint', $fix, 'tenant-php', 'project-php');
    $updated = phpManifestJsonBody('update_endpoint', $fix, 'tenant-php', 'project-php');
    $deleted = phpManifestJsonBody('delete_endpoint', $fix, 'tenant-php', 'project-php');
    expect($created)->toBeInstanceOf(\Udb\Core\Webhook\Services\V1\CreateEndpointRequest::class)
        ->and($created->getUrl())->toBe('https://example.com/udb-webhook')
        ->and($created->getTopicPattern())->toBe('tenant-php.*')
        ->and($created->getMetadataJson())->toBe('{}')
        ->and($created->getMaxAttempts())->toBe(3)
        ->and($updated)->toBeInstanceOf(\Udb\Core\Webhook\Services\V1\UpdateEndpointRequest::class)
        ->and($updated->getEndpointId())->toBe('endpoint-php')
        ->and($updated->getDescription())->toBe('sdk perf webhook updated')
        ->and($updated->getActive())->toBeTrue()
        ->and($deleted)->toBeInstanceOf(\Udb\Core\Webhook\Services\V1\DeleteEndpointRequest::class)
        ->and($deleted->getEndpointId())->toBe('endpoint-delete-php');
});

it('manifest JSON body hydrates BackupService requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('backup_id', 'backup-php');
    $fix->set('restore_tenant_id', 'restore-tenant-php');
    $backup = phpManifestJsonBody('get_backup', $fix, 'tenant-php', 'project-php');
    $policy = phpManifestJsonBody('get_backup_policy', $fix, 'tenant-php', 'project-php');
    $policies = phpManifestJsonBody('list_backup_policies', $fix, 'tenant-php', 'project-php');
    $backups = phpManifestJsonBody('list_backups', $fix, 'tenant-php', 'project-php');
    $putPolicy = phpManifestJsonBody('put_backup_policy', $fix, 'tenant-php', 'project-php');
    $started = phpManifestJsonBody('start_tenant_backup', $fix, 'tenant-php', 'project-php');
    $restored = phpManifestJsonBody('restore_tenant', $fix, 'tenant-php', 'project-php');
    $deleted = phpManifestJsonBody('delete_backup_policy', $fix, 'tenant-php', 'project-php');
    expect($backup)->toBeInstanceOf(\Udb\Core\Backup\Services\V1\GetBackupRequest::class)
        ->and($backup->getBackupId())->toBe('backup-php')
        ->and($policy)->toBeInstanceOf(\Udb\Core\Backup\Services\V1\GetBackupPolicyRequest::class)
        ->and($policy->getPolicyName())->toBe('sdk-perf-default')
        ->and($policies)->toBeInstanceOf(\Udb\Core\Backup\Services\V1\ListBackupPoliciesRequest::class)
        ->and($policies->getPageSize())->toBe(20)
        ->and($backups)->toBeInstanceOf(\Udb\Core\Backup\Services\V1\ListBackupsRequest::class)
        ->and($backups->getTenantId())->toBe('tenant-php')
        ->and($putPolicy)->toBeInstanceOf(\Udb\Core\Backup\Services\V1\PutBackupPolicyRequest::class)
        ->and($putPolicy->getScheduleCron())->toBe('0 3 * * *')
        ->and($putPolicy->getRetentionDays())->toBe(7)
        ->and($putPolicy->getMaxRetainedBackups())->toBe(3)
        ->and($putPolicy->getEnabled())->toBeTrue()
        ->and($started)->toBeInstanceOf(\Udb\Core\Backup\Services\V1\StartTenantBackupRequest::class)
        ->and($started->getTenantId())->toBe('tenant-php')
        ->and($restored)->toBeInstanceOf(\Udb\Core\Backup\Services\V1\RestoreTenantRequest::class)
        ->and($restored->getTargetTenantId())->toBe('restore-tenant-php')
        ->and($restored->getConfirmationToken())->toBe('yes')
        ->and($restored->getAllowCrossTenant())->toBeFalse()
        ->and($deleted)->toBeInstanceOf(\Udb\Core\Backup\Services\V1\DeleteBackupPolicyRequest::class)
        ->and($deleted->getPolicyName())->toBe('sdk-perf-default');
});

it('manifest JSON body hydrates ConfigService read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('project', 'project-php');
    $evaluated = phpManifestJsonBody('evaluate_flags', $fix, 'tenant-php', 'project-php');
    $flag = phpManifestJsonBody('get_flag', $fix, 'tenant-php', 'project-php');
    $flags = phpManifestJsonBody('list_flags', $fix, 'tenant-php', 'project-php');
    expect($evaluated)->toBeInstanceOf(\Udb\Core\Config\Services\V1\EvaluateFlagsRequest::class)
        ->and(iterator_to_array($evaluated->getKeys()))->toBe(['sdk.perf.enabled'])
        ->and($evaluated->getContext()->getProjectId())->toBe('project-php')
        ->and($flag)->toBeInstanceOf(\Udb\Core\Config\Services\V1\GetFlagRequest::class)
        ->and($flag->getFlagKey())->toBe('sdk.perf.enabled')
        ->and($flags)->toBeInstanceOf(\Udb\Core\Config\Services\V1\ListFlagsRequest::class)
        ->and($flags->getLimit())->toBe(50);
});

it('manifest JSON body hydrates ConfigService mutation requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('project', 'project-php');
    $put = phpManifestJsonBody('put_flag', $fix, 'tenant-php', 'project-php');
    $delete = phpManifestJsonBody('delete_flag', $fix, 'tenant-php', 'project-php');
    expect($put)->toBeInstanceOf(\Udb\Core\Config\Services\V1\PutFlagRequest::class)
        ->and($put->getValue()->getBoolValue())->toBeTrue()
        ->and($put->getRolloutPercentage())->toBe(100)
        ->and($put->getMetadataJson())->toBe('{}')
        ->and($delete)->toBeInstanceOf(\Udb\Core\Config\Services\V1\DeleteFlagRequest::class)
        ->and($delete->getFlagKey())->toBe('sdk.perf.enabled')
        ->and($delete->getProjectId())->toBe('project-php');
});

it('manifest JSON body hydrates WorkflowService read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('workflow_id', 'workflow-php');
    $workflow = phpManifestJsonBody('get_workflow', $fix, 'tenant-php', 'project-php');
    $workflows = phpManifestJsonBody('list_workflows', $fix, 'tenant-php', 'project-php');
    expect($workflow)->toBeInstanceOf(\Udb\Core\Workflow\Services\V1\GetWorkflowRequest::class)
        ->and($workflow->getWorkflowId())->toBe('workflow-php')
        ->and($workflows)->toBeInstanceOf(\Udb\Core\Workflow\Services\V1\ListWorkflowsRequest::class)
        ->and($workflows->getStatus())->toBe('RUNNING')
        ->and($workflows->getPageSize())->toBe(20);
});

it('manifest JSON body hydrates WorkflowService mutation requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('project', 'project-php');
    $fix->set('record_id', 'record-php');
    $fix->set('workflow_id', 'workflow-php');
    $fix->set('cancel_workflow_id', 'workflow-cancel-php');
    $started = phpManifestJsonBody('start_workflow', $fix, 'tenant-php', 'project-php');
    $cancelled = phpManifestJsonBody('cancel_workflow', $fix, 'tenant-php', 'project-php');
    $signalled = phpManifestJsonBody('signal_workflow', $fix, 'tenant-php', 'project-php');
    expect($started)->toBeInstanceOf(\Udb\Core\Workflow\Services\V1\StartWorkflowRequest::class)
        ->and($started->getProjectId())->toBe('project-php')
        ->and($started->getWorkflowType())->toBe('sdk.perf.workflow')
        ->and($started->getTotalSteps())->toBe(1)
        ->and($started->getPayload())->toBe('{}')
        ->and($started->getCompensations())->toBe('[]')
        ->and($started->getCorrelationId())->toBe('record-php')
        ->and($cancelled)->toBeInstanceOf(\Udb\Core\Workflow\Services\V1\CancelWorkflowRequest::class)
        ->and($cancelled->getWorkflowId())->toBe('workflow-cancel-php')
        ->and($cancelled->getReason())->toBe('sdk perf cancel')
        ->and($signalled)->toBeInstanceOf(\Udb\Core\Workflow\Services\V1\SignalWorkflowRequest::class)
        ->and($signalled->getWorkflowId())->toBe('workflow-php')
        ->and($signalled->getSignalName())->toBe('continue')
        ->and($signalled->getSignalPayload())->toBe('{"ok":true}');
});

it('manifest JSON body hydrates SearchService read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $indexes = phpManifestJsonBody('list_indexes', $fix, 'tenant-php', 'project-php');
    $search = phpManifestJsonBody('search', $fix, 'tenant-php', 'project-php');
    expect($indexes)->toBeInstanceOf(\Udb\Core\Search\Services\V1\ListIndexesRequest::class)
        ->and($indexes->getPageSize())->toBe(50)
        ->and($search)->toBeInstanceOf(\Udb\Core\Search\Services\V1\SearchRequest::class)
        ->and(array_map(fn ($v) => round((float) $v, 1), iterator_to_array($search->getQueryVector())))->toBe([0.1, 0.2, 0.3])
        ->and($search->getTopK())->toBe(5)
        ->and($search->getMode())->toBe(\Udb\Core\Search\Services\V1\SearchMode::SEARCH_MODE_HYBRID);
});

it('manifest JSON body hydrates SearchService mutation requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('message_type', 'myapp.v1.Invoice');
    $created = phpManifestJsonBody('create_index', $fix, 'tenant-php', 'project-php');
    $reindex = phpManifestJsonBody('reindex', $fix, 'tenant-php', 'project-php');
    $deleted = phpManifestJsonBody('delete_index', $fix, 'tenant-php', 'project-php');
    expect($created)->toBeInstanceOf(\Udb\Core\Search\Services\V1\CreateIndexRequest::class)
        ->and($created->getSourceMessageType())->toBe('myapp.v1.Invoice')
        ->and($created->getBackend())->toBe('qdrant')
        ->and($created->getVectorDims())->toBe(3)
        ->and($created->getMetadataJson())->toBe('{}')
        ->and($reindex)->toBeInstanceOf(\Udb\Core\Search\Services\V1\ReindexRequest::class)
        ->and($reindex->getIndexName())->toBe('sdk_live_records')
        ->and($deleted)->toBeInstanceOf(\Udb\Core\Search\Services\V1\DeleteIndexRequest::class)
        ->and($deleted->getIndexName())->toBe('sdk_live_records');
});

it('manifest JSON body hydrates EmbeddingService read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $sources = phpManifestJsonBody('list_sources', $fix, 'tenant-php', 'project-php');
    $retrieve = phpManifestJsonBody('retrieve', $fix, 'tenant-php', 'project-php');
    expect($sources)->toBeInstanceOf(\Udb\Core\Embedding\Services\V1\ListSourcesRequest::class)
        ->and($sources->getPageSize())->toBe(50)
        ->and($retrieve)->toBeInstanceOf(\Udb\Core\Embedding\Services\V1\RetrieveRequest::class)
        ->and(array_map(fn ($v) => round((float) $v, 1), iterator_to_array($retrieve->getQueryVector())))->toBe([0.1, 0.2, 0.3])
        ->and($retrieve->getSourceName())->toBe('sdk_live_records')
        ->and($retrieve->getTopK())->toBe(5);
});

it('manifest JSON body hydrates EmbeddingService mutation requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('message_type', 'myapp.v1.Invoice');
    $fix->set('record_id', 'record-php');
    $registered = phpManifestJsonBody('register_source', $fix, 'tenant-php', 'project-php');
    $reported = phpManifestJsonBody('report_embedding', $fix, 'tenant-php', 'project-php');
    $backfill = phpManifestJsonBody('backfill', $fix, 'tenant-php', 'project-php');
    $deleted = phpManifestJsonBody('delete_source', $fix, 'tenant-php', 'project-php');
    expect($registered)->toBeInstanceOf(\Udb\Core\Embedding\Services\V1\RegisterSourceRequest::class)
        ->and($registered->getSourceMessageType())->toBe('myapp.v1.Invoice')
        ->and(iterator_to_array($registered->getTextFields()))->toBe(['payload'])
        ->and($registered->getMetadataJson())->toBe('{}')
        ->and($reported)->toBeInstanceOf(\Udb\Core\Embedding\Services\V1\ReportEmbeddingRequest::class)
        ->and($reported->getRowPk())->toBe('record-php')
        ->and(array_map(fn ($v) => round((float) $v, 1), iterator_to_array($reported->getVector())))->toBe([0.1, 0.2, 0.3])
        ->and($reported->getDims())->toBe(3)
        ->and($backfill)->toBeInstanceOf(\Udb\Core\Embedding\Services\V1\BackfillRequest::class)
        ->and($backfill->getSourceName())->toBe('sdk_live_records')
        ->and($deleted)->toBeInstanceOf(\Udb\Core\Embedding\Services\V1\DeleteSourceRequest::class)
        ->and($deleted->getSourceName())->toBe('sdk_live_records');
});

it('manifest JSON body hydrates LiveQueryService subscribe request', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('project', 'project-php');
    $fix->set('message_type', 'myapp.v1.Invoice');
    $fix->set('record_id', 'record-php');
    $subscribe = phpManifestJsonBody('subscribe', $fix, 'tenant-php', 'project-php', 'LiveQueryService');
    $filters = iterator_to_array($subscribe->getFilters());
    expect($subscribe)->toBeInstanceOf(\Udb\Core\Livequery\Services\V1\SubscribeRequest::class)
        ->and($subscribe->getMessageType())->toBe('myapp.v1.Invoice')
        ->and($subscribe->getProjectId())->toBe('project-php')
        ->and($subscribe->getSnapshotLimit())->toBe(10)
        ->and(count($filters))->toBe(1)
        ->and($filters[0]->getField())->toBe('record_id')
        ->and($filters[0]->getOp())->toBe(\Udb\Core\Livequery\Services\V1\LiveQueryComparison::LIVE_QUERY_COMPARISON_EQ)
        ->and($filters[0]->getValue())->toBe('record-php');
});

it('manifest JSON body hydrates WebRTC turn and signaling requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('room_id', 'room-php');
    $fix->set('peer_id', 'peer-php');
    $fix->set('signal_peer_id', 'signal-peer-php');
    $turn = phpManifestJsonBody('issue_credentials', $fix, 'tenant-php', 'project-php', 'TurnService');
    $signal = phpManifestJsonBody('signal', $fix, 'tenant-php', 'project-php', 'SignalingService');
    expect($turn)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\IssueCredentialsRequest::class)
        ->and($turn->getTtlSeconds())->toBe(3600)
        ->and($turn->getPeerId())->toBe('peer-php')
        ->and($signal)->toBeInstanceOf(\Udb\Core\Webrtc\Services\V1\SignalRequest::class)
        ->and($signal->getPeerId())->toBe('signal-peer-php')
        ->and($signal->getPing())->toBeTrue();
});

it('manifest JSON body hydrates VaultService requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('vault_key_name', 'vault-key-php');
    $fix->set('vault_signing_key_name', 'vault-signing-key-php');
    $fix->set('vault_hmac_key_name', 'vault-hmac-key-php');
    $fix->set('vault_ciphertext', 'udb-vault:v1:php');
    $fix->set('vault_secret_path', 'app/config');
    $fix->set('vault_signature', 'udb-vault-sig:v1:php');
    $fix->set('vault_delete_secret_path', 'app/delete');
    $fix->set('vault_destroy_secret_path', 'app/destroy');
    $fix->set('vault_db_role', 'sdk-readonly');
    $created = phpManifestJsonBody('create_transit_key', $fix, 'tenant-php', 'project-php', 'VaultService');
    $decrypt = phpManifestJsonBody('decrypt', $fix, 'tenant-php', 'project-php', 'VaultService');
    $deleted = phpManifestJsonBody('delete_secret', $fix, 'tenant-php', 'project-php', 'VaultService');
    $destroyed = phpManifestJsonBody('destroy_secret', $fix, 'tenant-php', 'project-php', 'VaultService');
    $encrypted = phpManifestJsonBody('encrypt', $fix, 'tenant-php', 'project-php', 'VaultService');
    $dbCreds = phpManifestJsonBody('generate_database_credentials', $fix, 'tenant-php', 'project-php', 'VaultService');
    $secret = phpManifestJsonBody('get_secret', $fix, 'tenant-php', 'project-php', 'VaultService');
    $hmac = phpManifestJsonBody('hmac', $fix, 'tenant-php', 'project-php', 'VaultService');
    $secrets = phpManifestJsonBody('list_secrets', $fix, 'tenant-php', 'project-php', 'VaultService');
    $put = phpManifestJsonBody('put_secret', $fix, 'tenant-php', 'project-php', 'VaultService');
    $rotated = phpManifestJsonBody('rotate_transit_key', $fix, 'tenant-php', 'project-php', 'VaultService');
    $seal = phpManifestJsonBody('seal_status', $fix, 'tenant-php', 'project-php', 'VaultService');
    $signed = phpManifestJsonBody('sign', $fix, 'tenant-php', 'project-php', 'VaultService');
    $verify = phpManifestJsonBody('verify', $fix, 'tenant-php', 'project-php', 'VaultService');
    expect($created)->toBeInstanceOf(\Udb\Core\Vault\Services\V1\CreateTransitKeyRequest::class)
        ->and($created->getAlgorithm())->toBe('aes256-gcm-siv')
        ->and($decrypt)->toBeInstanceOf(\Udb\Core\Vault\Services\V1\DecryptRequest::class)
        ->and($decrypt->getCiphertext())->toBe('udb-vault:v1:php')
        ->and($deleted)->toBeInstanceOf(\Udb\Core\Vault\Services\V1\DeleteSecretRequest::class)
        ->and($deleted->getSecretPath())->toBe('app/delete')
        ->and($destroyed)->toBeInstanceOf(\Udb\Core\Vault\Services\V1\DestroySecretRequest::class)
        ->and($destroyed->getConfirmationToken())->toBe('destroy')
        ->and($encrypted)->toBeInstanceOf(\Udb\Core\Vault\Services\V1\EncryptRequest::class)
        ->and($encrypted->getPlaintext())->toBe('perf')
        ->and($dbCreds)->toBeInstanceOf(\Udb\Core\Vault\Services\V1\GenerateDatabaseCredentialsRequest::class)
        ->and($dbCreds->getRoleName())->toBe('sdk-readonly')
        ->and($dbCreds->getTtlSeconds())->toBe(900)
        ->and($secret)->toBeInstanceOf(\Udb\Core\Vault\Services\V1\GetSecretRequest::class)
        ->and($secret->getSecretPath())->toBe('app/config')
        ->and($hmac)->toBeInstanceOf(\Udb\Core\Vault\Services\V1\HmacRequest::class)
        ->and($hmac->getInput())->toBe('perf')
        ->and($hmac->getKeyName())->toBe('vault-hmac-key-php')
        ->and($secrets)->toBeInstanceOf(\Udb\Core\Vault\Services\V1\ListSecretsRequest::class)
        ->and($secrets->getPageSize())->toBe(50)
        ->and($put)->toBeInstanceOf(\Udb\Core\Vault\Services\V1\PutSecretRequest::class)
        ->and($put->getSecretValue())->toBe('perf-secret')
        ->and($put->getExpectedVersion())->toBe(0)
        ->and($rotated)->toBeInstanceOf(\Udb\Core\Vault\Services\V1\RotateTransitKeyRequest::class)
        ->and($rotated->getKeyName())->toBe('vault-key-php')
        ->and($seal)->toBeInstanceOf(\Udb\Core\Vault\Services\V1\SealStatusRequest::class)
        ->and($seal->getTenantId())->toBe('tenant-php')
        ->and($signed)->toBeInstanceOf(\Udb\Core\Vault\Services\V1\SignRequest::class)
        ->and($signed->getInput())->toBe('perf')
        ->and($verify)->toBeInstanceOf(\Udb\Core\Vault\Services\V1\VerifyRequest::class)
        ->and($verify->getSignature())->toBe('udb-vault-sig:v1:php');
});

it('manifest JSON body hydrates ControlPlaneService requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('project', 'project-php');
    $fix->set('node_id', 'node-php');
    $fix->set('resource_name', 'backend-target-php');
    $ack = phpManifestJsonBody('ack_status', $fix, 'tenant-php', 'project-php', 'ControlPlaneService');
    $delta = phpManifestJsonBody('delta_resources', $fix, 'tenant-php', 'project-php', 'ControlPlaneService');
    $resources = phpManifestJsonBody('get_resources', $fix, 'tenant-php', 'project-php', 'ControlPlaneService');
    $nodes = phpManifestJsonBody('list_node_states', $fix, 'tenant-php', 'project-php', 'ControlPlaneService');
    $rollback = phpManifestJsonBody('rollback_resources', $fix, 'tenant-php', 'project-php', 'ControlPlaneService');
    $stream = phpManifestJsonBody('stream_resources', $fix, 'tenant-php', 'project-php', 'ControlPlaneService');
    expect($ack)->toBeInstanceOf(\Udb\Core\Control\Services\V1\AckStatusRequest::class)
        ->and($ack->getNodeId())->toBe('node-php')
        ->and($ack->getResourceType())->toBe(5)
        ->and($ack->getContext()->getTenant()->getTenantId())->toBe('tenant-php')
        ->and($delta)->toBeInstanceOf(\Udb\Core\Control\Services\V1\DeltaDiscoveryRequest::class)
        ->and($delta->getNodeId())->toBe('node-php')
        ->and(iterator_to_array($delta->getResourceNamesSubscribe()))->toBe(['backend-target-php'])
        ->and(iterator_to_array($delta->getInitialResourceVersions()))->toBe([])
        ->and($resources)->toBeInstanceOf(\Udb\Core\Control\Services\V1\GetResourcesRequest::class)
        ->and($resources->getTenantId())->toBe('tenant-php')
        ->and($resources->getPage()->getPageSize())->toBe(50)
        ->and($resources->getContext()->getTenant()->getTenantId())->toBe('tenant-php')
        ->and($nodes)->toBeInstanceOf(\Udb\Core\Control\Services\V1\ListNodeStatesRequest::class)
        ->and($nodes->getPage()->getPage())->toBe(1)
        ->and($nodes->getPage()->getPageSize())->toBe(50)
        ->and($rollback)->toBeInstanceOf(\Udb\Core\Control\Services\V1\RollbackResourcesRequest::class)
        ->and($rollback->getTargetVersion())->toBe('')
        ->and($rollback->getContext()->getTenant()->getProjectId())->toBe('project-php')
        ->and($stream)->toBeInstanceOf(\Udb\Core\Control\Services\V1\DiscoveryRequest::class)
        ->and($stream->getNodeId())->toBe('node-php')
        ->and(iterator_to_array($stream->getResourceNames()))->toBe([])
        ->and($stream->getVersionInfo())->toBe('');
});

it('manifest JSON body hydrates AuthzService core read-only requests', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('project', 'project-php');
    $fix->set('user_id', 'user-php');
    $fix->set('object', 'ledger');
    $fix->set('action', 'data.select');
    $fix->set('subject', 'subject-php');
    $fix->set('policy_id', 'policy-php');
    $fix->set('resource', 'invoice');
    $fix->set('role_id', 'role-php');
    $fix->set('policy_draft_id', 'draft-php');
    $fix->set('canary_id', 'canary-php');
    $fix->set('gov_exp', '1893456000');
    $authorize = phpManifestJsonBody('authorize', $fix, 'tenant-php', 'project-php');
    $batch = phpManifestJsonBody('batch_check_permissions', $fix, 'tenant-php', 'project-php');
    $check = phpManifestJsonBody('check_access', $fix, 'tenant-php', 'project-php');
    $revision = phpManifestJsonBody('get_authz_revision', $fix, 'tenant-php', 'project-php');
    $native = phpManifestJsonBody('get_native_access', $fix, 'tenant-php', 'project-php');
    $bundle = phpManifestJsonBody('get_policy_bundle', $fix, 'tenant-php', 'project-php');
    $policy = phpManifestJsonBody('get_policy_rule', $fix, 'tenant-php', 'project-php');
    $role = phpManifestJsonBody('get_role', $fix, 'tenant-php', 'project-php');
    $audits = phpManifestJsonBody('list_access_decision_audits', $fix, 'tenant-php', 'project-php');
    $lint = phpManifestJsonBody('lint_authz_policies', $fix, 'tenant-php', 'project-php');
    $roles = phpManifestJsonBody('list_roles', $fix, 'tenant-php', 'project-php');
    $rules = phpManifestJsonBody('list_policy_rules', $fix, 'tenant-php', 'project-php');
    $permissions = phpManifestJsonBody('list_user_permissions', $fix, 'tenant-php', 'project-php');
    $userRoles = phpManifestJsonBody('list_user_roles', $fix, 'tenant-php', 'project-php');
    $diff = phpManifestJsonBody('diff_policy_draft', $fix, 'tenant-php', 'project-php');
    $explain = phpManifestJsonBody('explain_policy', $fix, 'tenant-php', 'project-php');
    $canary = phpManifestJsonBody('get_canary_status', $fix, 'tenant-php', 'project-php');
    $versions = phpManifestJsonBody('list_policy_versions', $fix, 'tenant-php', 'project-php');
    expect($authorize)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\AuthzRequest::class)
        ->and($authorize->getPrincipal()->getUserId())->toBe('user-php')
        ->and($authorize->getResource()->getTable())->toBe('sdk_live_records')
        ->and(iterator_to_array($authorize->getRequestedScopes()))->toBe(['udb:read'])
        ->and($batch)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\BatchCheckPermissionsRequest::class)
        ->and($batch->getChecks()[0]->getAction())->toBe('data.select')
        ->and($check)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\CheckAccessRequest::class)
        ->and($check->getObject())->toBe('ledger')
        ->and($revision)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\GetAuthzRevisionRequest::class)
        ->and($revision->getProjectId())->toBe('project-php')
        ->and($native)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\NativeAccessRequest::class)
        ->and($native->getBackend())->toBe('postgres')
        ->and($bundle)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\PolicyBundleRequest::class)
        ->and($bundle->getDomain())->toBe('tenant-php')
        ->and($policy)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\GetPolicyRuleRequest::class)
        ->and($policy->getPolicyId())->toBe('policy-php')
        ->and($role)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\GetRoleRequest::class)
        ->and($role->getRoleId())->toBe('role-php')
        ->and($audits)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\ListAccessDecisionAuditsRequest::class)
        ->and($audits->getPage()->getPageSize())->toBe(50)
        ->and($lint)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\LintAuthzPoliciesRequest::class)
        ->and($roles)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\ListRolesRequest::class)
        ->and($roles->getPage()->getPageSize())->toBe(50)
        ->and($rules)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\ListPolicyRulesRequest::class)
        ->and($rules->getActiveOnly())->toBeTrue()
        ->and($permissions)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\ListUserPermissionsRequest::class)
        ->and($permissions->getUserId())->toBe('user-php')
        ->and($userRoles)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\ListUserRolesRequest::class)
        ->and($userRoles->getActiveOnly())->toBeTrue()
        ->and($diff)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\DiffPolicyDraftRequest::class)
        ->and($diff->getActor()->getBreakGlassExpiresAtUnix())->toBe(1893456000)
        ->and($explain)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\ExplainPolicyRequest::class)
        ->and($explain->getTestCase()->getResource()->getResourceType())->toBe('invoice')
        ->and($canary)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\GetCanaryStatusRequest::class)
        ->and($canary->getCanaryId())->toBe('canary-php')
        ->and($versions)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\ListPolicyVersionsRequest::class)
        ->and($versions->getPolicySetId())->toBe('policy-php');
});

it('manifest JSON body hydrates AuthzService create-policy-draft request', function () {
    $fix = new PerfFixturesPhp();
    $fix->set('tenant_id', 'tenant-php');
    $fix->set('project', 'project-php');
    $fix->set('subject', 'subject-php');
    $fix->set('gov_exp', '1893456000');
    $draft = phpManifestJsonBody('create_policy_draft', $fix, 'tenant-php', 'project-php');
    expect($draft)->toBeInstanceOf(\Udb\Core\Authz\Services\V1\CreatePolicyDraftRequest::class)
        ->and($draft->getTenantId())->toBe('tenant-php')
        ->and($draft->getProjectId())->toBe('project-php')
        ->and($draft->getPolicySetName())->toBe('default')
        ->and($draft->getTitle())->toBe('draft 1')
        ->and($draft->getChangeReason())->toBe('init')
        ->and($draft->getActor()->getSubject())->toBe('subject-php')
        ->and(iterator_to_array($draft->getActor()->getScopes()))->toBe(['authz:policy:write'])
        ->and($draft->getActor()->getBreakGlass())->toBeTrue()
        ->and($draft->getDocument())->toBeInstanceOf(\Udb\Core\Authz\Services\V1\PolicyDocument::class);
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

function isCapabilitySkipPhp(string $service, string $name, string $err, string $detail): bool
{
    $rpc = "{$service}/{$name}";
    $detail = strtolower($detail);
    if (in_array($rpc, [
        'RoomService/StartRoomComposite',
        'RoomService/StartTrackEgress',
        'RoomService/StopEgress',
        'PeerService/CreateDataChannel',
        'RoomService/ListEgress',
    ], true)) {
        return in_array($err, ['FAILED_PRECONDITION', 'RESOURCE_EXHAUSTED', 'UNAVAILABLE', 'UNIMPLEMENTED'], true)
            || str_contains($detail, 'egress')
            || str_contains($detail, 'capability')
            || str_contains($detail, 'not configured')
            || str_contains($detail, 'unavailable');
    }

    return false;
}

function sdkBenchMethodPhp(string $service, string $name, string $apiAlias): string
{
    if ($service === 'CacheService') {
        return match ($name) {
            'Delete' => 'cacheServiceDelete',
            'Get' => 'cacheServiceGet',
            'Scan' => 'cacheServiceScan',
            'Set' => 'cacheServiceSet',
            default => \Fahara02\UdbLaravel\Generated\GeneratedClient::METHOD_ALIASES[$apiAlias] ?? $apiAlias,
        };
    }

    return \Fahara02\UdbLaravel\Generated\GeneratedClient::METHOD_ALIASES[$apiAlias] ?? $apiAlias;
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

    public function keys(): array
    {
        $keys = array_keys($this->m);
        sort($keys);

        return $keys;
    }
}

/**
 * perfBodyPhp returns the manifest-documented typed request body for a generated
 * RPC. The benchmark harness is manifest-only: missing rows, non-JSON cells, or
 * unresolved request classes fail the bench instead of falling back to a hand-
 * coded body or an empty placeholder.
 */
function perfBodyPhp(string $name, PerfFixturesPhp $fix, string $tenant, string $project, ?string $serviceName = null): object
{
    $manifestBody = phpManifestJsonBody($name, $fix, $tenant, $project, $serviceName);
    if ($manifestBody !== null) {
        phpUniquifyPerfBody($manifestBody, $name, $serviceName);
        return $manifestBody;
    }

    $label = ($serviceName !== null && $serviceName !== '') ? "{$serviceName}.{$name}" : $name;
    throw new RuntimeException("PHP bench manifest body missing or non-hydratable for {$label}");
}

function phpUniquifyPerfBody(object $request, string $name, ?string $serviceName): void
{
    $rpc = strtolower(($serviceName !== null && $serviceName !== '' ? $serviceName.'.' : '').rpcSnake($name));
    $suffix = bin2hex(random_bytes(6));
    if ($rpc === 'authnservice.create_user') {
        if (method_exists($request, 'setUsername')) {
            $request->setUsername("perf-u-$suffix");
        }
        if (method_exists($request, 'setEmail')) {
            $request->setEmail("perf-u-$suffix@acme.test");
        }
    } elseif ($rpc === 'assetservice.create_pipeline_definition' && method_exists($request, 'setName')) {
        $request->setName("thumbnail-pipeline-$suffix");
    } elseif ($rpc === 'authzservice.create_role') {
        if (method_exists($request, 'setName')) {
            $request->setName("SDK Perf Role $suffix");
        }
        if (method_exists($request, 'setRoleCode')) {
            $request->setRoleCode("php_perf_role_$suffix");
        }
    } elseif ($rpc === 'authzservice.create_policy_rule') {
        if (method_exists($request, 'setObject')) {
            $request->setObject("ledger-$suffix");
        }
        if (method_exists($request, 'setAction')) {
            $request->setAction("data.perf.$suffix");
        }
    } elseif ($rpc === 'authzservice.create_policy_draft') {
        if (method_exists($request, 'setTitle')) {
            $request->setTitle("sdk perf draft $suffix");
        }
        if (method_exists($request, 'setPolicySetName')) {
            $request->setPolicySetName("sdk-perf-set-$suffix");
        }
    } elseif ($rpc === 'storageservice.register_upload') {
        if (method_exists($request, 'setFilename')) {
            $request->setFilename("perf-$suffix.txt");
        }
        if (method_exists($request, 'setReferenceId')) {
            $request->setReferenceId(liveUuidV4());
        }
    } elseif ($rpc === 'identityproviderservice.scim_create_group') {
        if (method_exists($request, 'setScimGroupJson')) {
            $request->setScimGroupJson(json_encode(['displayName' => 'sdk-perf-group', 'members' => []]));
        }
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
        'tenant_code' => "sdk-perf-tenant-$suffix",
        'name' => "sdk-perf-$suffix", 'filename' => "sdk-perf-$suffix.txt", 'content_type' => 'text/plain',
        'file_type' => 'DOCUMENT', 'kind' => 'audio',
        'bucket' => liveEnv('UDB_LIVE_S3_BUCKET', 'udb-live-sdk'),
        'object_key' => "php-perf/$suffix.txt",
        'egress_id' => "eg-$tenant-$suffix",
        'mongo_collection' => "sdk_perf_docs_$suffix",
        'document_id' => "php-doc-$suffix",
        'resource_name' => 'postgres:default',
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

    // AdminPurgeTenant is a PRIVILEGED cross-tenant purge; the tenant-status gate
    // (live since 0.4.32) suspends the PURGED tenant, so pointing it at the caller's
    // own tenant self-suspends the benchmark tenant mid-run and denies every later
    // RPC. Target a SEPARATE disposable tenant so only the terminal self-PurgeTenant
    // suspends the caller, at the very end. The explicit set is REQUIRED: without it
    // the fixture suffix-match resolves admin_purge_tenant_id back to tenant_id (the
    // caller). Fall back to a non-existent UUID (isolated NotFound, never a cascade).
    $fix->set('admin_purge_tenant_id', liveUuidV4());
    $dispTenant = $try('disposable admin-purge tenant', fn () => $authGen->create_tenant(
        (new \Udb\Core\Tenant\Services\V1\CreateTenantRequest())
            ->setCode("sdkperfadminpurge$suffix")->setName('SDK Perf Admin-Purge Disposable')
            ->setType('organization')->setConfig('{}')->setBranding('{}'), $meta));
    if ($dispTenant !== null && $dispTenant->getTenantId() !== '') {
        $fix->set('admin_purge_tenant_id', $dispTenant->getTenantId());
    }

    // DataBroker: a real SdkLiveRecord row (Select/Delete success path).
    $recordId = "php-perf-$suffix";
    $rc = (new \Udb\Entity\V1\RequestContext())->setTenantId($tenant)->setProjectId($project)->setPurpose('php.live.perf.seed');
    $try('Upsert', fn () => $data->upsert((new \Udb\Entity\V1\UpsertRequest())
        ->setContext($rc)->setMessageType('udb.sdk.live.v1.SdkLiveRecord')
        ->setRecordJson(liveRecordJson($recordId, $tenant, $project, "php-perf-lk-$suffix", 'perf-seed', 1))
        ->setConflictFields(['record_id']), $meta));
    $fix->set('record_id', $recordId);
    $try('SeedBrokerCache', fn () => $data->cache_set((new \Udb\Entity\V1\CacheSetRequest())
        ->setContext($rc)->setResource((new \Udb\Entity\V1\StoreResource())->setBackend('redis'))
        ->setKey((string) $fix->lookup('object_key'))->setValue('perf')->setContentType('text/plain')->setTtlSeconds(300), $meta));

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
        $fix->set('username', $uname);
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
        if ($lr) { $fix->set('refresh_token', $lr->getRefreshToken()); $fix->set('refresh_token_session_id', $lr->getSessionId()); }
        $ls = $try('FreshLoginSession', fn () => $authGen->login((new \Udb\Core\Authn\Services\V1\LoginRequest())
            ->setUsername($adminU)->setPassword($adminP)->setTenantHint($tenant)->setProjectHint($project)->setDeviceName('php-perf-session'), $meta));
        if ($ls) { $fix->set('session_id', $ls->getSessionId()); $fix->set('refresh_session_id', $ls->getSessionId()); }
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

    // ApiKeyService: a real key. Canonical-identity model: the owner must be
    // an EXISTING ACTIVE SERVICE_ACCOUNT with an active typed grant, addressed
    // by its UUID — a bare service NAME is not a user_id.
    $svcName = "sdk-perf-svc-$suffix";
    $principal = $svcName;
    $svcUser = $try('SeedServiceAccount', fn () => $authGen->create_user((new \Udb\Core\Authn\Services\V1\CreateUserRequest())
        ->setUsername($svcName)->setEmail("$svcName@example.com")->setPassword('CorrectHorse1!')
        ->setTenantId($tenant)->setProjectId($project)->setFullName('SDK Perf Service Account')
        ->setAccountKind(\Udb\Core\Authn\Entity\V1\AccountKind::ACCOUNT_KIND_SERVICE_ACCOUNT), $meta));
    if ($svcUser) {
        $principal = $svcUser->getUser()->getUserId();
        // CreateUser persists PENDING_VERIFICATION; the typed grant and
        // CreateApiKey both require an ACTIVE service account.
        $try('SeedServiceAccountActivate', fn () => $authGen->change_user_status((new \Udb\Core\Authn\Services\V1\ChangeUserStatusRequest())
            ->setUserId($principal)->setNewStatus(\Udb\Core\Authn\Entity\V1\UserStatus::USER_STATUS_ACTIVE)->setReason('perf seed activate')
            ->setContext((new \Udb\Core\Common\V1\RequestContext())->setTenant((new \Udb\Core\Common\V1\TenantContext())->setTenantId($tenant)->setProjectId($project))), $meta));
        $try('SeedServiceAccountGrant', fn () => $authGen->create_service_account_grant((new \Udb\Core\Authn\Services\V1\CreateServiceAccountGrantRequest())
            ->setTenantId($tenant)->setUserId($principal)->setServiceIdentity($svcName)
            ->setProjectId($project)->setApprovedScopes(['data:read', 'resource:read'])->setReason('sdk perf seed'), $meta));
        // The measured RevokeCertificateBinding revokes THIS seeded binding.
        $seedBinding = $try('SeedCertificateBinding', fn () => $authGen->create_certificate_binding((new \Udb\Core\Authn\Services\V1\CreateCertificateBindingRequest())
            ->setTenantId($tenant)->setUserId($principal)->setSelectorKind('SPIFFE_URI')
            ->setSelectorValue("spiffe://bench/seed-binding-$suffix")->setReason('perf seed binding'), $meta));
        if ($seedBinding) {
            $fix->set('grant_binding_id', $seedBinding->getBinding()->getBindingId());
        }
    }
    $keyCtx = (new \Udb\Core\Common\V1\RequestContext())->setUserId($principal)
        ->setTenant((new \Udb\Core\Common\V1\TenantContext())->setTenantId($tenant)->setProjectId($project));
    // A SECOND ACTIVE service account WITHOUT a grant: the measured
    // CreateServiceAccountGrant makes its revision-1 grant here, and the
    // destructive-phase RotateServiceAccountIdentity rotates that same grant.
    $svcBName = "sdk-perf-svc-b-$suffix";
    $svcB = $try('SeedServiceAccountB', fn () => $authGen->create_user((new \Udb\Core\Authn\Services\V1\CreateUserRequest())
        ->setUsername($svcBName)->setEmail("$svcBName@example.com")->setPassword('CorrectHorse1!')
        ->setTenantId($tenant)->setProjectId($project)->setFullName('SDK Perf Service Account B')
        ->setAccountKind(\Udb\Core\Authn\Entity\V1\AccountKind::ACCOUNT_KIND_SERVICE_ACCOUNT), $meta));
    if ($svcB) {
        $bid = $svcB->getUser()->getUserId();
        $try('SeedServiceAccountBActivate', fn () => $authGen->change_user_status((new \Udb\Core\Authn\Services\V1\ChangeUserStatusRequest())
            ->setUserId($bid)->setNewStatus(\Udb\Core\Authn\Entity\V1\UserStatus::USER_STATUS_ACTIVE)->setReason('perf seed activate')
            ->setContext((new \Udb\Core\Common\V1\RequestContext())->setTenant((new \Udb\Core\Common\V1\TenantContext())->setTenantId($tenant)->setProjectId($project))), $meta));
        $fix->set('grant_create_user_id', $bid);
    }
    // A THIRD ACTIVE service account, also grantless, reserved for the measured
    // TransferServiceAccountGrant: the transfer moves the owner's ACTIVE grant onto a
    // grantless ACTIVE SERVICE ACCOUNT. Service-account-B cannot serve here — the
    // measured CreateServiceAccountGrant gives B a grant, and the handler refuses a
    // target that already holds one. Without its own fixture the key suffix-matches a
    // HUMAN user_id and the transfer is rejected "grants may only target service accounts".
    $svcCName = "sdk-perf-svc-c-$suffix";
    $svcC = $try('SeedServiceAccountC', fn () => $authGen->create_user((new \Udb\Core\Authn\Services\V1\CreateUserRequest())
        ->setUsername($svcCName)->setEmail("$svcCName@example.com")->setPassword('CorrectHorse1!')
        ->setTenantId($tenant)->setProjectId($project)->setFullName('SDK Perf Service Account C')
        ->setAccountKind(\Udb\Core\Authn\Entity\V1\AccountKind::ACCOUNT_KIND_SERVICE_ACCOUNT), $meta));
    if ($svcC) {
        $cid = $svcC->getUser()->getUserId();
        $try('SeedServiceAccountCActivate', fn () => $authGen->change_user_status((new \Udb\Core\Authn\Services\V1\ChangeUserStatusRequest())
            ->setUserId($cid)->setNewStatus(\Udb\Core\Authn\Entity\V1\UserStatus::USER_STATUS_ACTIVE)->setReason('perf seed activate')
            ->setContext((new \Udb\Core\Common\V1\RequestContext())->setTenant((new \Udb\Core\Common\V1\TenantContext())->setTenantId($tenant)->setProjectId($project))), $meta));
        $fix->set('grant_transfer_to_user_id', $cid);
    }
    // A FOURTH service account that OWNS a fresh grant, used only as the transfer's
    // SOURCE. The api-key owner cannot serve: its grant backs the measured api-key RPCs
    // and its revision moves, so the transfer's `expected_revision: 1` CAS fails
    // "source grant is inactive, missing, or its revision changed".
    $svcDName = "sdk-perf-svc-d-$suffix";
    $svcD = $try('SeedServiceAccountD', fn () => $authGen->create_user((new \Udb\Core\Authn\Services\V1\CreateUserRequest())
        ->setUsername($svcDName)->setEmail("$svcDName@example.com")->setPassword('CorrectHorse1!')
        ->setTenantId($tenant)->setProjectId($project)->setFullName('SDK Perf Service Account D')
        ->setAccountKind(\Udb\Core\Authn\Entity\V1\AccountKind::ACCOUNT_KIND_SERVICE_ACCOUNT), $meta));
    if ($svcD) {
        $did = $svcD->getUser()->getUserId();
        $try('SeedServiceAccountDActivate', fn () => $authGen->change_user_status((new \Udb\Core\Authn\Services\V1\ChangeUserStatusRequest())
            ->setUserId($did)->setNewStatus(\Udb\Core\Authn\Entity\V1\UserStatus::USER_STATUS_ACTIVE)->setReason('perf seed activate')
            ->setContext((new \Udb\Core\Common\V1\RequestContext())->setTenant((new \Udb\Core\Common\V1\TenantContext())->setTenantId($tenant)->setProjectId($project))), $meta));
        $grantD = $try('SeedTransferSourceGrant', fn () => $authGen->create_service_account_grant((new \Udb\Core\Authn\Services\V1\CreateServiceAccountGrantRequest())
            ->setTenantId($tenant)->setUserId($did)->setServiceIdentity($svcDName)
            ->setProjectId($project)->setApprovedScopes(['data:read'])->setReason('sdk perf transfer source'), $meta));
        if ($grantD) {
            $fix->set('grant_transfer_from_user_id', $did);
        }
    }
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
        ->setTenantId($tenant)->setKind(2)->setDisplayName("SDK Perf OIDC $suffix")->setIssuer("https://idp.example.com/$suffix")
        ->setJwksUrl("https://idp.example.com/$suffix/jwks")->setClientIds(['client-1'])->setAudiences(['udb'])
        ->setClaimMappingJson('{}')->setGroupMappingJson('{"sdk-perf-group":"admin"}')->setJitPolicyJson('{"require_verified_email":false}')->setAccountLinkingPolicy('explicit')
        ->setEnabled(true)->setCreatedBy($uid !== '' ? $uid : liveUuidV4())->setContext($ctxC), $meta));
    if ($prov) {
        $pid = method_exists($prov, 'getProvider') && $prov->getProvider() ? $prov->getProvider()->getProviderId() : '';
        if ($pid !== '') {
            $groupMappingJson = '{"sdk-perf-group":"admin"}';
            $storedGroupMapping = '';
            $gp = $try('GetProviderGroupMapping', fn () => $authGen->get_provider((new \Udb\Core\Idp\Services\V1\GetProviderRequest())
                ->setProviderId($pid)->setTenantId($tenant), $meta));
            if ($gp && method_exists($gp, 'getProvider') && $gp->getProvider()) {
                $storedGroupMapping = $gp->getProvider()->getGroupMappingJson();
            }
            if (!str_contains($storedGroupMapping, 'sdk-perf-group')) {
                $try('UpdateProviderGroupMapping', fn () => $authGen->update_provider((new \Udb\Core\Idp\Services\V1\UpdateProviderRequest())
                    ->setProviderId($pid)->setTenantId($tenant)->setGroupMappingJson($groupMappingJson)
                    ->setUpdatedBy($uid !== '' ? $uid : liveUuidV4())->setContext($ctxC), $meta));
            }
            $fix->set('provider_id', $pid);
            $cleanups[] = fn () => $try('DisableProvider', fn () => $authGen->disable_provider((new \Udb\Core\Idp\Services\V1\DisableProviderRequest())
                ->setProviderId($pid)->setTenantId($tenant)->setUpdatedBy($uid !== '' ? $uid : liveUuidV4())->setContext($ctxC), $meta));
            $try('ScimCreateGroupSeed', fn () => $authGen->scim_create_group((new \Udb\Core\Idp\Services\V1\ScimCreateGroupRequest())
                ->setTenantId($tenant)->setProviderId($pid)->setScimGroupJson(json_encode(['displayName' => 'sdk-perf-group', 'members' => []]))->setContext($ctxC), $meta));
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
        ->setActor((new \Udb\Core\Authz\Services\V1\GovernanceActor())->setSubject($fix->lookup('subject') ?? ('user:'.$uid))->setTenantId($tenant)->setProjectId($project)
            ->setBreakGlass(true)->setBreakGlassReason('sdk perf seed')->setBreakGlassExpiresAtUnix(time() + 900))
        ->setTenantId($tenant)->setProjectId($project)->setPolicySetName('default')->setTitle("sdk-perf draft $suffix")->setChangeReason('seed')->setDocument(new \Udb\Core\Authz\Services\V1\PolicyDocument()), $meta));
    if ($draft) {
        $did2 = method_exists($draft, 'getDraft') && $draft->getDraft() ? $draft->getDraft()->getDraftId() : '';
        if ($did2 !== '') {
            $fix->set('policy_draft_id', $did2);
        }
    }
    // Governance lifecycle: drafts in each state, approved versions, a canary, a rollback set.
    // Body actor.scopes are ignored by the live D1/D2 gate (it reads claim scopes,
    // and no role projects to authz:*), so the seed's own governance writes must use
    // the body-authoritative break-glass bypass — otherwise the drafts/versions/
    // canary are never created and the governance RPCs that read them fail
    // "<id> is required".
    $gA = fn () => (new \Udb\Core\Authz\Services\V1\GovernanceActor())->setSubject($fix->lookup('subject') ?? ('user:'.$uid))->setTenantId($tenant)->setProjectId($project)->setBreakGlass(true)->setBreakGlassReason('sdk perf seed')->setBreakGlassExpiresAtUnix(time() + 900);
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
    // Governance break-glass expiry: the D1/D2 governance gate reads scopes from the
    // VERIFIED claim, not request-body actor.scopes, and no role projects to authz:*
    // — so the measured governance RPCs are reached via the body-authoritative
    // break-glass bypass (<=900s, reason-bearing, audited). Set at seed time; the
    // governance RPCs measure shortly after.
    $fix->set('gov_exp', (string) (time() + 900));

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
    // ApplyMigration has a valid token. Per 06.1.1.1 ApproveMigrationPlan now returns the token
    // in the response BODY (MigrationStatusResponse.approval_token); the legacy response HEADER
    // x-udb-approval-token is kept for one compat release and used only as a fallback.
    $p2 = $try('PlanMigrationApply', fn () => $data->plan_migration((new \Udb\Entity\V1\MigrationPlanRequest())->setContext($rcSeed)->setProjectId($project)->setDryRun(false), $meta));
    if ($p2 && $p2->getRunId() !== '') {
        $applyRunId = $p2->getRunId();
        $fix->set('apply_run_id', $applyRunId);
        $try('ApproveForApply', function () use ($data, $rcSeed, $project, $applyRunId, $fix, $meta) {
            $resp = $data->approve_migration_plan((new \Udb\Entity\V1\MigrationRunRequest())->setContext($rcSeed)->setRunId($applyRunId)->setProjectId($project), $meta);
            // Prefer the body field (06.1.1.1); fall back to the legacy header.
            $tok = ($resp !== null && method_exists($resp, 'getApprovalToken')) ? (string) $resp->getApprovalToken() : '';
            if ($tok === '') {
                $md = $data->lastResponseMetadata();
                $tok = (string) ($md['x-udb-approval-token'][0] ?? '');
            }
            if ($tok !== '') {
                $fix->set('approval_token', $tok);
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
    // MongoDB: a real collection + document for the document RPC family.
    $mongoCollection = (string) $fix->lookup('mongo_collection');
    $documentId = (string) $fix->lookup('document_id');
    $try('EnsureMongoCollection', fn () => $data->ensure_resource((new \Udb\Entity\V1\ResourceAdminRequest())
        ->setContext($rcSeed)->setBackend('mongodb')->setResourceName($mongoCollection)->setSpecJson(json_encode(['collection' => $mongoCollection])), $meta));
    $doc = new \Google\Protobuf\Struct();
    $doc->mergeFromJsonString('{"name":"x"}');
    $try('SeedMongoDocument', fn () => $data->document_upsert((new \Udb\Entity\V1\DocumentUpsertRequest())
        ->setContext($rcSeed)->setResource((new \Udb\Entity\V1\StoreResource())->setBackend('mongodb')->setResourceName($mongoCollection))
        ->setDocumentId($documentId)->setDocument($doc), $meta));

    // Capture the live catalog manifest (READ-ONLY) so the measured StageCatalog has a valid
    // CatalogManifest (the new binary doesn't bump the active version on stage). K2 catalog
    // activate/rollback/get_version stay broker-blocked.
    $cm = $try('CaptureCatalogManifest', fn () => $data->get_catalog_manifest((new \Udb\Entity\V1\CatalogManifestRequest())->setContext($rcSeed)->setRedact(false), $meta));
    if ($cm && $cm->getManifestJson() !== '') {
        $fix->set('catalog_manifest', $cm->getManifestJson());
        $fix->set('catalog_manifest_b64', base64_encode($cm->getManifestJson()));
    }

    // AnalyticsService: a recorded metric.
    $stage = "sdk_perf_stage_$suffix";
    $try('RecordPipelineMetric', fn () => $authGen->record_pipeline_metric((new \Udb\Core\Analytics\Services\V1\RecordPipelineMetricRequest())
        ->setStageName($stage)->setTenantId($tenant)->setLatencyMs(100)->setIsSuccess(true), $meta));
    $fix->set('stage_name', $stage);

    // NotificationService: template + a sent notification.
    $event = "sdk.perf.$suffix";
    // subject_template MUST NOT carry a placeholder: "SDK {{n}}" requires var `n`,
    // which SendNotification below does not pass -> render fails -> no log_id ->
    // Get/Retry/Send notification RPCs fail. Use a literal subject.
    $try('UpsertTemplate', fn () => $authGen->upsert_template((new \Udb\Core\Notification\Services\V1\UpsertTemplateRequest())
        ->setEventType($event)->setChannel(1)->setLocale('en')->setSubjectTemplate('SDK perf')->setBodyTemplate('sdk-perf-body')->setIsActive(true), $meta));
    $fix->set('event_type', $event);
    if ($uid !== '') {
        $sent = $try('SendNotification', fn () => $authGen->send_notification((new \Udb\Core\Notification\Services\V1\SendNotificationRequest())
            ->setEventType($event)->setRecipientId($uid)->setRecipientAddress("sdk+$suffix@example.com")->setTenantId($tenant)
            ->setResourceType('__perf_force_failed__')->setChannels([1]), $meta));
        if ($sent && count($sent->getLogs()) > 0) {
            $logId = $sent->getLogs()[0]->getLogId();
            $fix->set('log_id', $logId);
            $fix->set('notification_id', $logId);
            // UDB_NOTIFICATION_TEST_MODE + ResourceType sentinel makes this served
            // send produce a real FAILED row for RetryNotification.
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
    $try('SeedNodeState', function () use ($authGen, $nodeId, $tenant, $project, $meta, $fix) {
        $cpCtx = (new \Udb\Core\Common\V1\RequestContext())->setTenant((new \Udb\Core\Common\V1\TenantContext())->setTenantId($tenant)->setProjectId($project));
        $stream = $authGen->stream_resources($meta);
        $stream->write((new \Udb\Core\Control\Services\V1\DiscoveryRequest())->setNodeId($nodeId)->setResourceType(5)->setContext($cpCtx));
        $resp = $stream->read();
        if ($resp && method_exists($resp, 'getVersionInfo') && $resp->getVersionInfo() !== '') {
            $fix->set('rollback_resource_version', $resp->getVersionInfo());
        }
        if ($resp && method_exists($resp, 'getResources') && count($resp->getResources()) > 0 && $resp->getResources()[0]->getName() !== '') {
            $fix->set('resource_name', $resp->getResources()[0]->getName());
        }
        $stream->cancel();
    });
    if (($fix->lookup('rollback_resource_version') ?? '') === '') {
        $gr = $try('SeedControlGetResources', fn () => $authGen->get_resources((new \Udb\Core\Control\Services\V1\GetResourcesRequest())
            ->setResourceType(5)->setTenantId($tenant)->setResourceNames([])->setContext((new \Udb\Core\Common\V1\RequestContext())->setTenant((new \Udb\Core\Common\V1\TenantContext())->setTenantId($tenant)->setProjectId($project))), $meta));
        if ($gr && method_exists($gr, 'getVersionInfo') && $gr->getVersionInfo() !== '') {
            $fix->set('rollback_resource_version', $gr->getVersionInfo());
        }
        if ($gr && method_exists($gr, 'getResources') && count($gr->getResources()) > 0 && $gr->getResources()[0]->getName() !== '') {
            $fix->set('resource_name', $gr->getResources()[0]->getName());
        }
    }
    // Saga + DLQ rows: create recovery state through the served, admin-gated
    // EnsureBaseline RPC instead of raw udb_system inserts. Each mutating RPC gets
    // its own disposable row because the op transitions status.
    foreach ([['saga_id', 'dlq_id'], ['retry_saga_id', 'dismiss_dlq_id'], ['mark_saga_id', 'quarantine_dlq_id'], ['', 'replay_dlq_id']] as [$sagaKey, $dlqKey]) {
        $baseline = $try("EnsureBaseline:$dlqKey", fn () => $data->ensure_baseline((new \Udb\Services\V1\EnsureBaselineRequest())->setContext($rcSeed), $meta));
        if ($baseline) {
            if ($sagaKey !== '' && count($baseline->getSagaIds()) > 0) {
                $fix->set($sagaKey, $baseline->getSagaIds()[0]);
            }
            if (count($baseline->getDlqIds()) > 0) {
                $fix->set($dlqKey, $baseline->getDlqIds()[0]);
            }
        }
    }

    // StorageService (UUID tenant): a registered file -> file_id, + Asset pipeline.
    $reg = $try('RegisterUpload', fn () => $authGen->register_upload((new \Udb\Core\Storage\Services\V1\RegisterUploadRequest())
        ->setTenantId($tenant)->setProjectId('')->setFilename("perf-$suffix.txt")->setContentType('text/plain')
        ->setFileType('DOCUMENT')->setReferenceId(liveUuidV4())->setReferenceType('sdk.perf')->setSizeBytes(128)->setExpiresInMinutes(30), $meta));
    if ($reg) {
        $fid = $reg->getFileId();
        $fix->set('file_id', $fid);
        // Ensure the OBJECT bucket FinalizeUpload HEADs (UDB_OBJECT_BUCKET=udb-storage on the
        // bench lane) exists, so the PutObject fallback below lands bytes where Finalize looks
        // (mirrors the Go seed's EnsureResource(storageBucket)).
        $try('SeedStorageBucket', fn () => $data->ensure_resource((new \Udb\Entity\V1\ResourceAdminRequest())
            ->setContext((new \Udb\Entity\V1\RequestContext())->setTenantId($tenant)->setProjectId($project)->setPurpose('php.live.perf.seed'))
            ->setBackend('minio')->setResourceName(liveEnv('UDB_LIVE_S3_BUCKET', 'udb-live-sdk'))->setSpecJson('{}'), $meta));
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
        // FINALIZE the primary file so its object bytes are present: the measured
        // DownloadFile streams chunks from this finalized file_id, and GetFile/Get-
        // DownloadUrl read it. The measured FinalizeUpload uses a SEPARATE un-finalized
        // file (finalize_file_id below) so it does not double-finalize this one.
        $try('SeedFinalizePrimary', fn () => $authGen->finalize_upload((new \Udb\Core\Storage\Services\V1\FinalizeUploadRequest())
            ->setTenantId($tenant)->setFileId($fid)->setContentType('text/plain')->setFileType('DOCUMENT')->setSizeBytes(strlen("sdk-perf-file-$suffix")), $meta));
        // A SEPARATE registered+uploaded but NOT finalized file for the measured
        // FinalizeUpload — finalizing the primary file_id again fails "already
        // finalized", so the measured Finalize needs its own un-finalized target.
        // FinalizeUpload verifies the stored object's byte length against the size
        // DECLARED at RegisterUpload, so declare exactly what we upload — a fixed
        // literal fails "uploaded object size N does not match declared M".
        // The shared bench body declares size_bytes: 1024 and FinalizeUpload verifies
        // the stored object against THAT, so the seeded object must be exactly 1024 B.
        $finPayloadLen = 1024;
        // FinalizeUpload refuses to CHANGE reference_id from the value established at
        // RegisterUpload, so the measured body must resend that exact value — seed it.
        $finRefId = liveUuidV4();
        $fix->set('finalize_reference_id', $finRefId);
        $finReg = $try('RegisterFinalizeFile', fn () => $authGen->register_upload((new \Udb\Core\Storage\Services\V1\RegisterUploadRequest())
            ->setTenantId($tenant)->setProjectId('')->setFilename("perf-fin-$suffix.txt")->setContentType('text/plain')
            ->setFileType('DOCUMENT')->setReferenceId($finRefId)->setReferenceType('sdk.perf')->setSizeBytes($finPayloadLen)->setExpiresInMinutes(30), $meta));
        if ($finReg) {
            $ffid = $finReg->getFileId();
            $fix->set('finalize_file_id', $ffid);
            $fix->set('file_size_bytes', (string) $finPayloadLen);
            if ($ffid !== '') {
                $cleanups[] = fn () => $try('DeleteFinalizeFile', fn () => $authGen->delete_file((new \Udb\Core\Storage\Services\V1\DeleteFileRequest())
                    ->setTenantId($tenant)->setFileId($ffid), $meta));
            }
            // Upload bytes (presigned first, DataBroker PutObject fallback) but DO NOT
            // finalize — the measured FinalizeUpload finalizes it.
            $try('SeedFinalizeFilePut', function () use ($data, $finReg, $suffix, $tenant, $project, $meta) {
                $payloadBase = "sdk-perf-finalize-$suffix";
                $payload = $payloadBase.str_repeat('x', 1024 - strlen($payloadBase));
                $url = method_exists($finReg, 'getUploadUrl') ? $finReg->getUploadUrl() : '';
                if ($url !== '') {
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
                $bucket = liveEnv('UDB_LIVE_S3_BUCKET', 'udb-live-sdk');
                $rc = (new \Udb\Entity\V1\RequestContext())->setTenantId($tenant)->setProjectId($project)->setPurpose('php.live.perf.seed');
                $call = $data->put_object($meta);
                $call->write((new \Udb\Entity\V1\Chunk())->setContext($rc)->setBucket($bucket)->setObjectKey($finReg->getObjectKey())->setData($payload)->setContentType('text/plain')->setFinalChunk(true));
                $call->wait();
            });
        }
        // A SEPARATE disposable file for the destructive DeleteFile -> real 200.
        $delReg = $try('RegisterDeleteFile', fn () => $authGen->register_upload((new \Udb\Core\Storage\Services\V1\RegisterUploadRequest())
            ->setTenantId($tenant)->setProjectId('')->setFilename("perf-del-$suffix.txt")->setContentType('text/plain')
            ->setFileType('DOCUMENT')->setReferenceId(liveUuidV4())->setReferenceType('sdk.perf')->setSizeBytes(64)->setExpiresInMinutes(30), $meta));
        if ($delReg) {
            $fix->set('delete_file_id', $delReg->getFileId());
        }
        // A registered-but-PENDING upload (never uploaded, never finalized) for the
        // measured ReissueUploadUrl — it resumes a PENDING upload and rejects any
        // non-PENDING (finalized/ACTIVE) file, so it needs its own PENDING target.
        $reissueReg = $try('RegisterReissueFile', fn () => $authGen->register_upload((new \Udb\Core\Storage\Services\V1\RegisterUploadRequest())
            ->setTenantId($tenant)->setProjectId('')->setFilename("perf-reissue-$suffix.txt")->setContentType('text/plain')
            ->setFileType('DOCUMENT')->setReferenceId(liveUuidV4())->setReferenceType('sdk.perf')->setSizeBytes(64)->setExpiresInMinutes(30), $meta));
        if ($reissueReg) {
            $rfid = $reissueReg->getFileId();
            $fix->set('reissue_file_id', $rfid);
            if ($rfid !== '') {
                $cleanups[] = fn () => $try('DeleteReissueFile', fn () => $authGen->delete_file((new \Udb\Core\Storage\Services\V1\DeleteFileRequest())
                    ->setTenantId($tenant)->setFileId($rfid), $meta));
            }
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
            // A SEPARATE high-capacity room for the measured JoinSession. The main
            // room_id is filled to capacity (cap 8) by JoinRoom's mutation iters, so
            // JoinSession against it would hit "room ... at capacity". This room's
            // maxParticipants=64 absorbs the 5 JoinSession iters.
            $jsr = $try('CreateJoinSessionRoom', fn () => $authGen->create_room((new \Udb\Core\Webrtc\Services\V1\CreateRoomRequest())
                ->setTenantId($tenant)->setName("sdk-perf-joinsession-room-$suffix")->setMaxParticipants(64)->setConfig('{}')->setCreatedBy($uid !== '' ? $uid : liveUuidV4()), $meta));
            if ($jsr) {
                $jsrId = $jsr->getRoomId();
                $fix->set('join_session_room_id', $jsrId);
                if ($jsrId !== '') {
                    $cleanups[] = fn () => $try('CloseJoinSessionRoom', fn () => $authGen->close_room((new \Udb\Core\Webrtc\Services\V1\CloseRoomRequest())
                        ->setTenantId($tenant)->setRoomId($jsrId), $meta));
                }
            }
        }
    }

    // ── New-service fixtures (Vault / Lock / Workflow / Scheduler / Webhook /
    //    Backup / Embedding / Search / Metering / Config) ──────────────────────────
    // These services have NO generated method wrappers in the PHP client, so the
    // reflective full-surface sweep drives them through their RAW gRPC stubs and
    // hydrates each probe body from the shared bench-body manifest. Those bodies
    // read <seed:...> refs (phpResolveManifestSeeds THROWS on a missing ref), and
    // the read/mutation RPCs need a pre-existing row/id/token — neither of which
    // any earlier seed produced. Populate the exact ref names the Go seeds use
    // (sdk/go/udbclient/live_perf_seed_test.go) and create the backing rows.
    $lockOwner = $uid !== '' ? $uid : liveUuidV4();
    $seedStub = function (string $label, object $stub, string $rpc, string $reqClass, array $body) use ($meta, $try): ?object {
        return $try($label, function () use ($stub, $rpc, $reqClass, $body, $meta) {
            /** @var \Google\Protobuf\Internal\Message $req */
            $req = new $reqClass();
            $req->mergeFromJsonString((string) json_encode($body));
            /** @var \Grpc\UnaryCall $call */
            $call = $stub->{$rpc}($req, $meta->toGrpcMetadata(), ['timeout' => 20_000_000]);
            [$resp, $status] = $call->wait();
            $code = is_object($status) ? (int) ($status->code ?? 0) : (is_array($status) ? (int) ($status['code'] ?? 0) : 0);

            return $code === 0 ? $resp : null;
        });
    };

    // VaultService: a preseeded transit key + real ciphertext/signature (for the
    // measured Decrypt/Verify), plus preseeded secret paths for Get/Delete/Destroy.
    // The measured CreateTransitKey/PutSecret write DISTINCT names/paths so they do
    // not collide with these read fixtures.
    $vault = $authGen->VaultServiceStub();
    $fix->set('vault_key_name', "sdk-perf-key-$suffix");
    $fix->set('vault_create_key_name', "sdk-perf-create-key-$suffix");
    $fix->set('vault_secret_path', "app/config-$suffix");
    $fix->set('vault_put_secret_path', "app/put-$suffix");
    $fix->set('vault_delete_secret_path', "app/delete-$suffix");
    $fix->set('vault_destroy_secret_path', "app/destroy-$suffix");
    $fix->set('vault_db_role', 'readonly');
    // Fallbacks so the Decrypt/Verify bodies always resolve even if Encrypt/Sign fail.
    $fix->set('vault_ciphertext', base64_encode('perf'));
    $fix->set('vault_signature', base64_encode('perf'));
    $seedStub('CreateTransitKey', $vault, 'CreateTransitKey', \Udb\Core\Vault\Services\V1\CreateTransitKeyRequest::class,
        ['tenant_id' => $tenant, 'key_name' => $fix->lookup('vault_key_name'), 'algorithm' => 'aes256-gcm-siv']);
    // A dedicated ed25519 SIGNING key so GetTransitPublicKey exports a real public
    // key — the aes256-gcm-siv key above has no exportable public half.
    $fix->set('vault_signing_key_name', "sdk-perf-signing-key-$suffix");
    $seedStub('CreateSigningKey', $vault, 'CreateTransitKey', \Udb\Core\Vault\Services\V1\CreateTransitKeyRequest::class,
        ['tenant_id' => $tenant, 'key_name' => $fix->lookup('vault_signing_key_name'), 'algorithm' => 'ed25519']);
    // A dedicated hmac-sha256 key — the transit Hmac verb now requires a
    // purpose-built hmac-sha256 key and rejects the symmetric aes256-gcm-siv key.
    $fix->set('vault_hmac_key_name', "sdk-perf-hmac-key-$suffix");
    $seedStub('CreateHmacKey', $vault, 'CreateTransitKey', \Udb\Core\Vault\Services\V1\CreateTransitKeyRequest::class,
        ['tenant_id' => $tenant, 'key_name' => $fix->lookup('vault_hmac_key_name'), 'algorithm' => 'hmac-sha256']);
    $enc = $seedStub('VaultEncrypt', $vault, 'Encrypt', \Udb\Core\Vault\Services\V1\EncryptRequest::class,
        ['tenant_id' => $tenant, 'key_name' => $fix->lookup('vault_key_name'), 'plaintext' => 'perf']);
    if ($enc && $enc->getCiphertext() !== '') {
        $fix->set('vault_ciphertext', $enc->getCiphertext());
    }
    $sig = $seedStub('VaultSign', $vault, 'Sign', \Udb\Core\Vault\Services\V1\SignRequest::class,
        ['tenant_id' => $tenant, 'key_name' => $fix->lookup('vault_signing_key_name'), 'input' => 'perf']);
    if ($sig && $sig->getSignature() !== '') {
        $fix->set('vault_signature', $sig->getSignature());
    }
    foreach (['vault_secret_path' => 'perf-secret', 'vault_delete_secret_path' => 'delete-secret', 'vault_destroy_secret_path' => 'destroy-secret'] as $ref => $val) {
        $seedStub("VaultPutSecret:$ref", $vault, 'PutSecret', \Udb\Core\Vault\Services\V1\PutSecretRequest::class,
            ['tenant_id' => $tenant, 'secret_path' => $fix->lookup($ref), 'secret_value' => $val, 'expected_version' => 0, 'metadata_json' => '{}']);
    }

    // LockService: two independent locks whose captured fencing tokens back the
    // measured RenewLock / ReleaseLock (each keyed by its exact seeded lock name).
    $locks = $authGen->LockServiceStub();
    // Lease long enough to outlive the whole measured run — the perf surface takes
    // well over a minute (PHP runs in Docker) to reach the measured RenewLock/
    // ReleaseLock, and a short lease would expire first → "lock_not_held".
    $renew = $seedStub('AcquireLock:renew', $locks, 'AcquireLock', \Udb\Core\Lock\Services\V1\AcquireLockRequest::class,
        ['tenant_id' => $tenant, 'lock_name' => 'sdk-perf-renew-lock', 'owner_id' => $lockOwner, 'lease_ttl_seconds' => 3600, 'metadata_json' => '{}']);
    if ($renew) {
        $fix->set('renew_fencing_token', (string) $renew->getFencingToken());
    }
    $release = $seedStub('AcquireLock:release', $locks, 'AcquireLock', \Udb\Core\Lock\Services\V1\AcquireLockRequest::class,
        ['tenant_id' => $tenant, 'lock_name' => 'sdk-perf-release-lock', 'owner_id' => $lockOwner, 'lease_ttl_seconds' => 3600, 'metadata_json' => '{}']);
    if ($release) {
        $fix->set('release_fencing_token', (string) $release->getFencingToken());
    }

    // WorkflowService: a primary + a disposable (cancel) workflow instance.
    $workflow = $authGen->WorkflowServiceStub();
    $wf = $seedStub('StartWorkflow', $workflow, 'StartWorkflow', \Udb\Core\Workflow\Services\V1\StartWorkflowRequest::class,
        ['tenant_id' => $tenant, 'project_id' => '', 'workflow_type' => 'sdk.perf.workflow', 'total_steps' => 20, 'payload' => '{}', 'compensations' => '[]', 'correlation_id' => $recordId]);
    if ($wf) {
        $fix->set('workflow_id', $wf->getWorkflowId());
    }
    $cwf = $seedStub('StartWorkflow:cancel', $workflow, 'StartWorkflow', \Udb\Core\Workflow\Services\V1\StartWorkflowRequest::class,
        ['tenant_id' => $tenant, 'project_id' => '', 'workflow_type' => 'sdk.perf.cancel', 'total_steps' => 20, 'payload' => '{}', 'compensations' => '[]', 'correlation_id' => "cancel-$recordId"]);
    if ($cwf) {
        $fix->set('cancel_workflow_id', $cwf->getWorkflowId());
    }

    // SchedulerService: a stable job for the read/pause/resume/delete fixtures.
    $scheduler = $authGen->SchedulerServiceStub();
    $job = $seedStub('CreateJob', $scheduler, 'CreateJob', \Udb\Core\Scheduler\Services\V1\CreateJobRequest::class,
        ['tenant_id' => $tenant, 'project_id' => '', 'name' => "sdk-perf-seed-job-$suffix", 'schedule_type' => 'CRON', 'cron_expression' => '*/5 * * * *', 'payload' => '{}', 'target_topic' => 'sdk.perf.scheduler', 'max_attempts' => 3, 'backoff_seconds' => 30]);
    if ($job) {
        $fix->set('job_id', $job->getJobId());
    }

    // WebhookService: a primary + a disposable endpoint (topic_pattern = "*" mirrors Go).
    $webhooks = $authGen->WebhookServiceStub();
    $fix->set('topic_pattern', '*');
    $ep = $seedStub('CreateEndpoint', $webhooks, 'CreateEndpoint', \Udb\Core\Webhook\Services\V1\CreateEndpointRequest::class,
        ['tenant_id' => $tenant, 'url' => 'https://example.com/udb-webhook-seed', 'topic_pattern' => '*', 'description' => 'sdk perf seed webhook', 'max_attempts' => 3, 'metadata_json' => '{}']);
    if ($ep) {
        $fix->set('endpoint_id', $ep->getEndpointId());
    }
    $dep = $seedStub('CreateEndpoint:delete', $webhooks, 'CreateEndpoint', \Udb\Core\Webhook\Services\V1\CreateEndpointRequest::class,
        ['tenant_id' => $tenant, 'url' => 'https://example.com/udb-webhook-delete', 'topic_pattern' => '*', 'description' => 'sdk perf delete webhook', 'max_attempts' => 3, 'metadata_json' => '{}']);
    if ($dep) {
        $fix->set('delete_endpoint_id', $dep->getEndpointId());
    }

    // BackupService: a policy row + a started backup id (+ a restore target tenant).
    $backup = $authGen->BackupServiceStub();
    $fix->set('restore_tenant_id', liveUuidV4());
    $seedStub('PutBackupPolicy', $backup, 'PutBackupPolicy', \Udb\Core\Backup\Services\V1\PutBackupPolicyRequest::class,
        ['tenant_id' => $tenant, 'policy_name' => 'sdk-perf-default', 'schedule_cron' => '0 3 * * *', 'retention_days' => 7, 'max_retained_backups' => 3, 'enabled' => true, 'metadata_json' => '{}']);
    $bk = $seedStub('StartTenantBackup', $backup, 'StartTenantBackup', \Udb\Core\Backup\Services\V1\StartTenantBackupRequest::class,
        ['tenant_id' => $tenant]);
    if ($bk) {
        $fix->set('backup_id', $bk->getBackupId());
    }

    // EmbeddingService: model registry, durable jobs, and one searchable vector.
    $embedding = $authGen->EmbeddingServiceStub();
    $registerEmbeddingModel = function (string $label, string $modelId, string $collection, string $alias) use ($seedStub, $embedding, $tenant) {
        return $seedStub($label, $embedding, 'RegisterModel', \Udb\Core\Embedding\Services\V1\RegisterModelRequest::class,
            ['tenant_id' => $tenant, 'model_id' => $modelId, 'provider' => 'deterministic',
                'model_name' => 'text-embedding-3-small', 'version' => '1', 'dimensions' => 3,
                'matryoshka_dims' => [3], 'distance_metric' => 'COSINE', 'normalize' => true,
                'output_dtype' => 'FLOAT32', 'max_input_tokens' => 8192, 'tokenizer' => 'cl100k_base',
                'task_type' => 'DOCUMENT', 'provider_endpoint_ref' => 'vault://embedding/sdk-live',
                'vector_backend' => 'qdrant', 'vector_instance' => 'default', 'collection_alias' => $alias,
                'active_collection' => $collection, 'chunking_strategy' => 'TOKEN_RECURSIVE',
                'chunk_tokens' => 256, 'chunk_overlap_tokens' => 32,
                'metadata_json' => '{"suite":"sdk-live"}']);
    };
    $registerEmbeddingModel('RegisterModel', 'text-embedding-3-small', 'sdk_live_records', 'sdk_live_records_alias');
    $embeddingDeleteModelId = "sdk-live-delete-model-$suffix";
    $fix->set('embedding_delete_model_id', $embeddingDeleteModelId);
    $registerEmbeddingModel('RegisterModel:delete', $embeddingDeleteModelId, "sdk_live_delete_records_$suffix", "sdk_live_delete_records_alias_$suffix");
    $seedStub('RegisterSource', $embedding, 'RegisterSource', \Udb\Core\Embedding\Services\V1\RegisterSourceRequest::class,
        ['tenant_id' => $tenant, 'source_name' => 'sdk_live_records', 'source_message_type' => 'udb.sdk.live.v1.SdkLiveRecord', 'text_fields' => ['payload'], 'target_collection' => 'sdk_live_records', 'model_id' => 'text-embedding-3-small', 'metadata_json' => '{}']);
    $seedStub('ReportEmbedding', $embedding, 'ReportEmbedding', \Udb\Core\Embedding\Services\V1\ReportEmbeddingRequest::class,
        ['tenant_id' => $tenant, 'source_name' => 'sdk_live_records', 'row_pk' => $recordId, 'vector' => [0.1, 0.2, 0.3], 'model' => 'text-embedding-3-small', 'dims' => 3]);
    $embeddingDocument = $seedStub('IngestDocument:work', $embedding, 'IngestDocument', \Udb\Core\Embedding\Services\V1\IngestDocumentRequest::class,
        ['tenant_id' => $tenant, 'external_id' => "sdk-live-work-$suffix", 'title' => 'SDK benchmark work fixture',
            'raw_text' => 'Durable embedding work is seeded from real document text for the SDK benchmark.',
            'content_type' => 'text/plain', 'doc_version' => '1', 'model_id' => 'text-embedding-3-small',
            'metadata_json' => '{"suite":"sdk-live","fixture":"work"}']);
    if ($embeddingDocument) {
        $fix->set('embedding_job_id', $embeddingDocument->getJobId());
        $work = $seedStub('ListEmbeddingWorkItems:seed', $embedding, 'ListEmbeddingWorkItems', \Udb\Core\Embedding\Services\V1\ListEmbeddingWorkItemsRequest::class,
            ['tenant_id' => $tenant, 'job_id' => $embeddingDocument->getJobId(), 'page_size' => 50]);
        if ($work && count($work->getWorkItems()) > 0) {
            $fix->set('embedding_work_item_id', $work->getWorkItems()[0]->getWorkItemId());
        }
    }
    $parserDocument = $seedStub('IngestDocument:parser', $embedding, 'IngestDocument', \Udb\Core\Embedding\Services\V1\IngestDocumentRequest::class,
        ['tenant_id' => $tenant, 'external_id' => "sdk-live-parser-$suffix", 'title' => 'SDK benchmark parser fixture',
            'storage_object_ref' => "udb://sdk-live/embedding-$suffix.txt", 'content_type' => 'text/plain',
            'doc_version' => '1', 'model_id' => 'text-embedding-3-small',
            'metadata_json' => '{"suite":"sdk-live","fixture":"parser"}']);
    if ($parserDocument) {
        $fix->set('embedding_document_id', $parserDocument->getDocumentId());
        $fix->set('embedding_document_job_id', $parserDocument->getJobId());
    }

    // SearchService: create the seeded index so the search read RPCs resolve.
    $search = $authGen->SearchServiceStub();
    $seedStub('CreateIndex', $search, 'CreateIndex', \Udb\Core\Search\Services\V1\CreateIndexRequest::class,
        ['tenant_id' => $tenant, 'index_name' => 'sdk_live_records', 'source_message_type' => 'udb.sdk.live.v1.SdkLiveRecord', 'backend' => 'qdrant', 'resource_name' => 'sdk_live_records', 'vector_dims' => 3, 'metadata_json' => '{}']);

    // MeteringService: upsert a quota rule so GetQuota/QueryUsage resolve a row.
    $metering = $authGen->MeteringServiceStub();
    $seedStub('PutQuota', $metering, 'PutQuota', \Udb\Core\Metering\Services\V1\PutQuotaRequest::class,
        ['tenant_id' => $tenant, 'project_id' => $project, 'metric' => 'sdk.perf.request', 'limit_value' => 1000000, 'window_seconds' => 86400, 'enabled' => true, 'metadata_json' => '{}']);

    // ConfigService: upsert a project flag so GetFlag/ListFlags resolve a row.
    $config = $authGen->ConfigServiceStub();
    $seedStub('PutFlag', $config, 'PutFlag', \Udb\Core\Config\Services\V1\PutFlagRequest::class,
        ['tenant_id' => $tenant, 'project_id' => $project, 'environment' => 'prod', 'flag_key' => 'sdk.perf.enabled', 'value' => ['bool_value' => true], 'enabled' => true, 'rollout_percentage' => 100, 'rollout_context_key' => 'user_id', 'metadata_json' => '{}']);

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
    $timeUnary = function (GeneratedClient $sdkClient, string $sdkMethodName, $stub, ReflectionMethod $method, $hasRequest, $probeRequest) use ($invoke, $authedMeta): array {
        $start = microtime(true);
        $err = 'OK';
        $detail = '';
        try {
            // Measure the generated SDK wrapper for unary RPCs, not the raw grpc stub.
            // The wrapper is where metadata binding, deadline, retry gating, and typed
            // error mapping live. Raw stubs are retained only as a fallback for unusual
            // signatures and for the streaming probe below.
            if ($hasRequest && method_exists($sdkClient, $sdkMethodName)) {
                $sdkClient->{$sdkMethodName}($probeRequest, $authedMeta);
            } else {
                $call = $invoke($stub, $method, $hasRequest, $probeRequest);
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
    $apiKeyOf = fn (string $svc, string $name) => "$svc/$name";
    $kindOf = fn (array $u) => operationKindPhp($u['name'], $u['svc']);
    $aliasOf = fn (string $svc, string $name) => \Fahara02\UdbLaravel\Generated\GeneratedClient::API_ALIAS[$apiKeyOf($svc, $name)] ?? rpcSnake($name);
    $operationIdOf = fn (string $svc, string $name) => \Fahara02\UdbLaravel\Generated\GeneratedClient::OPERATION_ID[$apiKeyOf($svc, $name)] ?? lcfirst($name);
    // Collect every (stub, method) unit, then sort into phases before measuring.
    $units = [];
    foreach (stubAccessors($s['data'], $s['authGenerated']) as $stubName => $stub) {
        $svc = preg_replace('/Stub$/', '', $stubName);
        $sdkClient = $stubName === 'DataBrokerStub' ? $s['data'] : $s['authGenerated'];
        foreach (generatedStubMethods($stub) as $method) {
            $units[] = ['stub' => $stub, 'client' => $sdkClient, 'svc' => $svc, 'method' => $method, 'name' => $method->getName()];
        }
    }
    // NOTE: $u['name'] is the raw grpc stub method (PascalCase, e.g. RefreshSession); the phase
    // lists are snake_case → normalize via rpcSnake before matching, or Phase-1/3 protection is
    // silently lost and a Phase-3 revoke (Logout/RevokeSession) runs BEFORE RefreshSession.
    $phaseOf = function (array $u) use ($PHASE1_AUTHN, $PHASE3_AUTHN): int {
        $nm = rpcSnake($u['name']);
        if ($u['svc'] === 'AuthnService' && in_array($nm, $PHASE1_AUTHN, true)) { return 1; }
        if ($u['svc'] === 'AuthnService' && in_array($nm, $PHASE3_AUTHN, true)) { return 3; }
        if ($u['svc'] === 'TenantService' && $nm === 'purge_tenant') { return 4; }
        return 2;
    };
    usort($units, function (array $a, array $b) use ($phaseOf, $PHASE1_AUTHN, $okRank, $kindOf): int {
        $pa = $phaseOf($a); $pb = $phaseOf($b);
        if ($pa !== $pb) { return $pa <=> $pb; }
        if ($pa === 1) { return array_search(rpcSnake($a['name']), $PHASE1_AUTHN, true) <=> array_search(rpcSnake($b['name']), $PHASE1_AUTHN, true); }
        if ($pa === 2) { return ($okRank[$kindOf($a)] ?? 0) <=> ($okRank[$kindOf($b)] ?? 0); }
        return 0;
    });
    foreach ($units as $u) {
        {
            $stub = $u['stub']; $sdkClient = $u['client']; $svc = $u['svc']; $method = $u['method'];
            $name = $method->getName();
            $rpcName = rpcSnake($name);
            $apiAlias = $aliasOf($svc, $name);
            $sdkMethodName = sdkBenchMethodPhp($svc, $name, $apiAlias);
            if ($svc === 'IdentityProviderService' && in_array($name, ['ScimCreateGroup', 'ScimGetGroup', 'ScimPatchGroup', 'ScimDeleteGroup'], true)) {
                $pid = $fix->lookup('provider_id');
                if ($pid !== null && $pid !== '') {
                    $ctxC = (new \Udb\Core\Common\V1\RequestContext())->setTenant(
                        (new \Udb\Core\Common\V1\TenantContext())->setTenantId($authedMeta->tenantId)->setProjectId($authedMeta->projectId),
                    );
                    try {
                        $s['authGenerated']->update_provider((new \Udb\Core\Idp\Services\V1\UpdateProviderRequest())
                            ->setProviderId($pid)->setTenantId($authedMeta->tenantId)
                            ->setGroupMappingJson('{"sdk-perf-group":"admin"}')
                            ->setUpdatedBy($fix->lookup('user_id') ?? liveUuidV4())->setContext($ctxC), $authedMeta);
                        $fix->set('scim_group_id', 'sdk-perf-group');
                    } catch (\Throwable $e) {
                    }
                }
            }
            // put_object is CLIENT-STREAMING: drive it explicitly (open with metadata, WRITE the
            // seeded Chunk, then wait) — the reflective probe/timeUnary path binds the Chunk to the
            // metadata slot → empty stream. Mirrors the working SeedPutObject (which lands code=0).
            if ($rpcName === 'put_object') {
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
                $samples[] = [
                    'service' => $svc, 'rpc' => $name, 'api_alias' => $aliasOf($svc, $name),
                    'operation_id' => $operationIdOf($svc, $name), 'kind' => 'mutation', 'err' => $err,
                    'p50' => $pp(50), 'p99' => $pp(99), 'mean' => array_sum($durs) / count($durs),
                    'iters' => count($durs),
                ];

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
                $mkBody = fn () => perfBodyPhp($name, $fix, $authedMeta->tenantId, $authedMeta->projectId, $svc);
                $probeRequest = $mkBody();
                $kind = operationKindPhp($name, $svc);
            }
            // Refresh-token rotation is single-use. A second call with the same
            // fixture token is a replay signal in v0.5.7 and correctly revokes the
            // principal's sessions, including the bearer for every later RPC.
            $iters = $svc === 'AuthnService' && $rpcName === 'refresh_token'
                ? 1
                : $itersFor($kind);
            if ($rpcName === 'approve_migration_plan') {
                $iters = 1;
            }
            // Classify streaming by generated stub signature, not by invoking a
            // probe call. A probe on a unary mutation is a real side effect
            // (CreateUser/CreatePipelineDefinition/etc.) and can consume the only
            // unique success path before the timed loop starts.
            $openStart = microtime(true);
            $doc = (string) ($method->getDocComment() ?: '');
            $isStreaming = str_contains($doc, 'ServerStreamingCall')
                || str_contains($doc, 'ClientStreamingCall')
                || str_contains($doc, 'BidiStreamingCall');
            if ($isStreaming) {
                try {
                    $probe = $invoke($stub, $method, $hasRequest, $probeRequest);
                    if (method_exists($probe, 'cancel')) {
                        try {
                            $probe->cancel();
                        } catch (\Throwable $e) {
                        }
                    }
                } catch (\Throwable $e) {
                    // Signature-level streaming coverage is still reported as a
                    // stream-open sample; unary failures are handled below by
                    // timeUnary(), never hidden here.
                }
                // Stream-open latency (initiate + cancel, no response drain).
                $openMs = (microtime(true) - $openStart) * 1000.0;
                $samples[] = [
                    'service' => $svc, 'rpc' => $name, 'api_alias' => $aliasOf($svc, $name),
                    'operation_id' => $operationIdOf($svc, $name), 'kind' => 'stream_open', 'err' => 'OK',
                    'p50' => $openMs, 'p99' => $openMs, 'mean' => $openMs, 'iters' => 1,
                ];

                continue;
            }
            // Warm-up ONLY for idempotent reads — a warm-up on a non-idempotent mutation
            // CONSUMES the op (submit/approve a draft, rotate a token, revoke a key).
            if ($kind === 'read_only') {
                $timeUnary($sdkClient, $sdkMethodName, $stub, $method, $hasRequest, $mkBody ? $mkBody() : $probeRequest);
            }
            $allDurs = [];
            $okDurs = [];
            $anyOk = false;
            $firstErr = 'OK';
            $errDetail = '';
            for ($i = 0; $i < $iters; $i++) {
                [$ms, $err, $detail] = $timeUnary($sdkClient, $sdkMethodName, $stub, $method, $hasRequest, $mkBody ? $mkBody() : $probeRequest);
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
            $capabilitySkipped = ! $anyOk && isCapabilitySkipPhp($svc, $name, $firstErr, $errDetail);
            $errCode = $anyOk ? 'OK' : ($capabilitySkipped ? 'CAPABILITY_SKIPPED' : $firstErr);
            $durs = $anyOk ? $okDurs : $allDurs;
            if ($errCode !== 'OK' && $errCode !== 'CAPABILITY_SKIPPED') {
                fwrite(STDERR, "FAILDETAIL $svc/$name [$errCode] ".substr($errDetail, 0, 200)."\n");
            }
            sort($durs);
            $pct = fn (int $p) => $durs[min(count($durs) - 1, intdiv($p * (count($durs) - 1), 100))];
            $samples[] = [
                'service' => $svc, 'rpc' => $name, 'api_alias' => $aliasOf($svc, $name),
                'operation_id' => $operationIdOf($svc, $name), 'kind' => $kind, 'err' => $errCode,
                'p50' => $pct(50), 'p99' => $pct(99), 'mean' => array_sum($durs) / count($durs),
                'iters' => count($durs),
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
    // Failures section (BENCH_RPC_BODIES.md #1/#3): every RPC whose measured
    // status is non-OK is a FAILURE, not just a latency sample.
    $capabilitySkipped = array_values(array_filter($samples, fn ($r) => ($r['err'] ?? 'OK') === 'CAPABILITY_SKIPPED'));
    $failed = array_values(array_filter($samples, fn ($r) => ($r['err'] ?? 'OK') !== 'OK' && ($r['err'] ?? 'OK') !== 'CAPABILITY_SKIPPED'));
    usort($failed, fn ($a, $b) => ($a['service'].'/'.$a['rpc']) <=> ($b['service'].'/'.$b['rpc']));
    $fixtureKeys = $fix->keys();
    $lines = ['# UDB SDK Live Perf — PHP (Docker → host)', '',
        'RPCs measured: '.count($samples).'   tenant='.$authedMeta->tenantId, '',
        'Every RPC is driven down its SUCCESS path: a SEED phase first creates real, disposable entities '
            .'(a user, role + assignment + policies, an API key, a notification, a stored file, an asset + pipeline, '
            .'a WebRTC room/peer/track, an SdkLiveRecord row) and the harness resolves each request\'s reference/ID '
            .'fields to those real identifiers. So the numbers reflect real handler work, not validation-rejection '
            .'latency. The TARGET is zero failures; any residual non-OK RPC is listed under Failures for the maintainer '
            .'to finish.', '',
        'Unary = full request/response round-trip. Streaming rows (kind=stream_open) report '
            .'stream-open latency (initiate + cancel, no response drain), NOT first-message latency.', '',
        '## Seeded fixtures', '',
        'Captured semantic field -> seeded value keys used to resolve request fields: '
            .(count($fixtureKeys) > 0 ? implode(', ', $fixtureKeys) : '(none)'), '',
        '## Per-service mean latency', '', '| Service | RPCs | mean ms |', '|---|--:|--:|'];
    foreach ($svcMean as $svc => $mean) {
        $lines[] = sprintf('| %s | %d | %.2f |', $svc, count($bySvc[$svc]), $mean);
    }
    $lines = array_merge($lines, ['', '## Capability skips ('.count($capabilitySkipped).')', '']);
    if (count($capabilitySkipped) === 0) {
        $lines[] = 'No RPC was skipped for an optional server capability.';
    } else {
        $lines[] = '| RPC | api_alias | operation_id | kind | p99 ms |';
        $lines[] = '|---|---|---|---|--:|';
        foreach ($capabilitySkipped as $row) {
            $lines[] = sprintf('| %s/%s | %s | %s | %s | %.2f |', $row['service'], $row['rpc'], $row['api_alias'], $row['operation_id'], $row['kind'], $row['p99']);
        }
    }
    $lines = array_merge($lines, ['', '## Failures ('.count($failed).')', '']);
    if (count($failed) === 0) {
        $lines[] = 'No RPC returned a non-OK gRPC status.';
    } else {
        $lines[] = 'These RPCs returned a non-OK gRPC status and are FAILURES, not latency samples.';
        $lines[] = '';
        $lines[] = '| RPC | api_alias | operation_id | kind | err | p99 ms |';
        $lines[] = '|---|---|---|---|---|--:|';
        foreach ($failed as $row) {
            $lines[] = sprintf('| %s/%s | %s | %s | %s | %s | %.2f |', $row['service'], $row['rpc'], $row['api_alias'], $row['operation_id'], $row['kind'], $row['err'], $row['p99']);
        }
    }
    usort($samples, fn ($a, $b) => $b['p99'] <=> $a['p99']);
    $lines = array_merge($lines, ['', '## Slowest 20 by p99', '', '| RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms |', '|---|---|---|---|---|--:|--:|--:|']);
    foreach (array_slice($samples, 0, 20) as $row) {
        $lines[] = sprintf('| %s/%s | %s | %s | %s | %s | %.2f | %.2f | %.2f |', $row['service'], $row['rpc'], $row['api_alias'], $row['operation_id'], $row['kind'], $row['err'] ?? 'OK', $row['p50'], $row['p99'], $row['mean']);
    }
    usort($samples, fn ($a, $b) => ($a['service'] === $b['service']) ? ($a['rpc'] <=> $b['rpc']) : ($a['service'] <=> $b['service']));
    $lines = array_merge($lines, ['', '## Full per-RPC table (sorted by service, then RPC)', '', '| Service | RPC | api_alias | operation_id | kind | err | p50 ms | p99 ms | mean ms | iters |', '|---|---|---|---|---|---|--:|--:|--:|--:|']);
    foreach ($samples as $row) {
        $lines[] = sprintf('| %s | %s | %s | %s | %s | %s | %.2f | %.2f | %.2f | %d |', $row['service'], $row['rpc'], $row['api_alias'], $row['operation_id'], $row['kind'], $row['err'] ?? 'OK', $row['p50'], $row['p99'], $row['mean'], $row['iters'] ?? 0);
    }
    file_put_contents('perf_report_php.md', implode("\n", $lines)."\n");
    $seedCleanup();
    expect(count($samples))->toBeGreaterThanOrEqual(200);
})->skip(getenv('UDB_LIVE_PERF') !== '1', 'perf run requires UDB_LIVE_PERF=1');
