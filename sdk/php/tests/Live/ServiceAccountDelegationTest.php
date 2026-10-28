<?php

declare(strict_types=1);

// Live service-account delegation conformance. Skipped unless
// UDB_LIVE_SDK_TESTS=1; CI's "SDK service accounts (live)" job runs it against
// the broker it just built, on every push to main, after
// scripts/provision-service-account-live.py has created the identities it
// reads from the environment.
//
// It proves, through this SDK, the served behavior UDB 0.6.4 added:
//  1. Authenticate names the account (Principal.username).
//  2. A service account's bearer carries its ordinary bound roles and its
//     udb:authz:check-access-for scope, but never an admin-segment role.
//  3. A service account holding udb:authz:check-access-for gets a CheckAccess
//     evaluated for an active person of its tenant (evaluated_subject), and is
//     refused for an unknown user id.
//  4. ExchangeServiceTokenForTenant turns the home bearer into a short-lived
//     token of another tenant that approved a membership, and that token
//     works for an authenticated call in the target tenant; a tenant with no
//     membership is refused.
//  5. A phone-only CreateUser creates a PENDING_VERIFICATION account that a
//     PHONE_VERIFICATION OTP activates.
//
// Every authenticated call asserts the CANONICAL tenant UUID the bearer was
// issued for, never the human tenant code.

use Fahara02\UdbLaravel\Exceptions\UdbRpcException;
use Fahara02\UdbLaravel\Generated\GeneratedClient;
use Fahara02\UdbLaravel\UdbMetadata;
use Fahara02\UdbLaravel\UdbProject;
use Udb\Core\Authn\Entity\V1\AccountKind;
use Udb\Core\Authn\Entity\V1\OTPType;
use Udb\Core\Authn\Entity\V1\UserStatus;
use Udb\Core\Authn\Services\V1\CreateUserRequest;
use Udb\Core\Authn\Services\V1\ExchangeServiceTokenForTenantRequest;
use Udb\Core\Authn\Services\V1\GetUserRequest;
use Udb\Core\Authn\Services\V1\SendOTPRequest;
use Udb\Core\Authn\Services\V1\VerifyOTPRequest;
use Udb\Core\Authz\Services\V1\CheckAccessRequest;

const SA_LIVE_CHECK_OBJECT = 'sdk-live-chart';
const SA_LIVE_CHECK_ACTION = 'read';
const SA_LIVE_CHECK_ACCESS_SCOPE = 'udb:authz:check-access';
const SA_LIVE_CHECK_ACCESS_FOR_SCOPE = 'udb:authz:check-access-for';
// Ceiling of an exchanged token's lifetime (15 minutes) plus clock skew.
const SA_LIVE_EXCHANGED_TOKEN_MAX_SECONDS = 15 * 60 + 60;
const SA_LIVE_PHONE_PASSWORD = 'SdkLivePhone#2026';
// gRPC status codes.
const SA_LIVE_PERMISSION_DENIED = 7;

function serviceAccountEnv(string $name, ?string $fallback = null): string
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

/** Copy of $base asserting $tenantId/$projectId and carrying $bearer. */
function serviceAccountMetadata(UdbMetadata $base, string $tenantId, string $projectId, string $bearer): UdbMetadata
{
    return new UdbMetadata(
        tenantId: $tenantId,
        userId: $base->userId,
        purpose: $base->purpose,
        correlationId: $base->correlationId,
        scopes: $base->scopes,
        serviceIdentity: $base->serviceIdentity,
        projectId: $projectId,
        clientCatalogVersion: $base->clientCatalogVersion,
        bearerToken: $bearer,
        apiKey: '',
    );
}

/** @return list<string> */
function serviceAccountList(iterable $repeated): array
{
    $out = [];
    foreach ($repeated as $value) {
        $out[] = (string) $value;
    }
    return $out;
}

/** The gRPC status code $call fails with, or null when it succeeds. */
function serviceAccountFailureCode(callable $call): ?int
{
    try {
        $call();
    } catch (UdbRpcException $e) {
        return $e->status;
    }
    return null;
}

function serviceAccountUuidV4(): string
{
    $bytes = random_bytes(16);
    $bytes[6] = chr((ord($bytes[6]) & 0x0f) | 0x40);
    $bytes[8] = chr((ord($bytes[8]) & 0x3f) | 0x80);

    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($bytes), 4));
}

it('live service account delegation conformance', function () {
    $target = serviceAccountEnv('UDB_GRPC_TARGET');
    $authTarget = serviceAccountEnv('UDB_AUTH_GRPC_TARGET', $target);
    $username = serviceAccountEnv('UDB_LIVE_USERNAME');
    $password = serviceAccountEnv('UDB_LIVE_PASSWORD');
    $tenantHint = serviceAccountEnv('UDB_LIVE_TENANT');
    $projectHint = serviceAccountEnv('UDB_LIVE_PROJECT');
    $serviceUsername = serviceAccountEnv('UDB_LIVE_SERVICE_USERNAME');
    $servicePassword = serviceAccountEnv('UDB_LIVE_SERVICE_PASSWORD');
    $serviceUserId = serviceAccountEnv('UDB_LIVE_SERVICE_USER_ID');
    $serviceRole = serviceAccountEnv('UDB_LIVE_SERVICE_ROLE');
    $withheldRole = serviceAccountEnv('UDB_LIVE_SERVICE_WITHHELD_ROLE');
    $delegatedUserId = serviceAccountEnv('UDB_LIVE_DELEGATED_USER_ID');
    $targetTenant = serviceAccountEnv('UDB_LIVE_TARGET_TENANT_ID');
    $targetProject = serviceAccountEnv('UDB_LIVE_TARGET_PROJECT');
    $unapprovedTenant = serviceAccountEnv('UDB_LIVE_UNAPPROVED_TENANT_ID');

    $project = UdbProject::connect([
        'target' => $target,
        'authTarget' => $authTarget,
        'tenantId' => $tenantHint,
        'projectId' => $projectHint,
        'purpose' => 'php.live.service-accounts',
        'correlationId' => 'php-live-service-accounts-'.bin2hex(random_bytes(8)),
        'serviceIdentity' => 'php.sdk.live',
        'deadline_ms' => 15_000,
    ]);
    // Login and Authenticate are public: the tenant/project here are hints.
    $hints = $project->metadata();
    $auth = $project->auth();
    $rpc = new GeneratedClient(['endpoint' => $authTarget, 'deadline_ms' => 15_000, 'retry' => ['max_attempts' => 1]]);

    // ── 1. Principal.username ──────────────────────────────────────────────
    $humanLogin = $auth->login($username, $password, '', '', $hints);
    expect($humanLogin->getAccessToken())->not->toBe('', 'step 1: the human login returned no access token');
    $human = $auth->authenticateBearer($humanLogin->getAccessToken(), $hints)->getPrincipal();
    expect($human)->not->toBeNull('step 1: Authenticate returned no principal for the human');
    expect(strtolower($human->getUsername()))->toBe(
        strtolower($username),
        "step 1: Authenticate principal.username is '{$human->getUsername()}', expected '{$username}'",
    );
    $homeTenant = $human->getTenantId();
    expect($homeTenant)->not->toBe('', 'step 1: the human principal carries no canonical tenant_id');
    $humanProject = $human->getProjectId() !== '' ? $human->getProjectId() : $projectHint;
    $humanMeta = serviceAccountMetadata($hints, $homeTenant, $humanProject, $humanLogin->getAccessToken());

    // ── 2. The service bearer's roles, scopes and username ─────────────────
    $serviceLogin = $auth->login($serviceUsername, $servicePassword, '', '', $hints);
    expect($serviceLogin->getAccessToken())->not->toBe('', 'step 2: the service-account login returned no access token');
    $service = $auth->authenticateBearer($serviceLogin->getAccessToken(), $hints)->getPrincipal();
    expect($service)->not->toBeNull('step 2: Authenticate returned no principal for the service account');
    expect(strtolower($service->getUsername()))->toBe(
        strtolower($serviceUsername),
        "step 2: service principal.username is '{$service->getUsername()}', expected '{$serviceUsername}'",
    );
    expect($service->getUserId())->toBe(
        $serviceUserId,
        "step 2: service principal.user_id is '{$service->getUserId()}', expected '{$serviceUserId}'",
    );
    expect($service->getAccountKind())->toBe(
        AccountKind::ACCOUNT_KIND_SERVICE_ACCOUNT,
        'step 2: service principal.account_kind is '.AccountKind::name($service->getAccountKind()).', expected ACCOUNT_KIND_SERVICE_ACCOUNT',
    );
    expect($service->getTenantId())->toBe(
        $homeTenant,
        "step 2: service principal.tenant_id is '{$service->getTenantId()}', expected home tenant '{$homeTenant}'",
    );
    $roles = serviceAccountList($service->getRoles());
    $rolesText = implode(',', $roles);
    expect(in_array($serviceRole, $roles, true))->toBeTrue(
        "step 2: the service bearer lacks bound role '{$serviceRole}'; roles=[{$rolesText}]",
    );
    foreach ([$withheldRole, 'platform_admin', 'organization_owner'] as $forbidden) {
        expect(in_array($forbidden, $roles, true))->toBeFalse(
            "step 2: the service bearer carries admin-segment role '{$forbidden}'; roles=[{$rolesText}]",
        );
    }
    $scopes = serviceAccountList($service->getScopes());
    expect(in_array(SA_LIVE_CHECK_ACCESS_FOR_SCOPE, $scopes, true))->toBeTrue(
        'step 2: the service bearer lacks scope '.SA_LIVE_CHECK_ACCESS_FOR_SCOPE.'; scopes=['.implode(',', $scopes).']',
    );
    $serviceProject = $service->getProjectId() !== '' ? $service->getProjectId() : $projectHint;
    $serviceMeta = serviceAccountMetadata($hints, $homeTenant, $serviceProject, $serviceLogin->getAccessToken());

    // ── 3. Delegated CheckAccess ───────────────────────────────────────────
    $delegated = $rpc->checkAccess(
        (new CheckAccessRequest())
            ->setUserId($delegatedUserId)
            ->setObject(SA_LIVE_CHECK_OBJECT)
            ->setAction(SA_LIVE_CHECK_ACTION),
        $serviceMeta,
    );
    expect($delegated->getEvaluatedSubject())->toBe(
        $delegatedUserId,
        "step 3: delegated CheckAccess evaluated '{$delegated->getEvaluatedSubject()}', expected the person '{$delegatedUserId}'",
    );
    $unknownUser = serviceAccountUuidV4();
    $unknownCode = serviceAccountFailureCode(fn () => $rpc->checkAccess(
        (new CheckAccessRequest())
            ->setUserId($unknownUser)
            ->setObject(SA_LIVE_CHECK_OBJECT)
            ->setAction(SA_LIVE_CHECK_ACTION),
        $serviceMeta,
    ));
    expect($unknownCode)->toBe(
        SA_LIVE_PERMISSION_DENIED,
        "step 3: delegated CheckAccess for unknown user {$unknownUser} must fail PERMISSION_DENIED (7); got ".var_export($unknownCode, true),
    );

    // ── 4. Tenant token exchange ───────────────────────────────────────────
    $exchanged = $rpc->exchangeServiceTokenForTenant(
        (new ExchangeServiceTokenForTenantRequest())
            ->setTargetTenantId($targetTenant)
            ->setTargetProjectId($targetProject)
            ->setRequestedScopes([SA_LIVE_CHECK_ACCESS_SCOPE]),
        $serviceMeta,
    );
    $now = time();
    $exchangedToken = $exchanged->getAccessToken();
    expect($exchangedToken)->not->toBe('', 'step 4: ExchangeServiceTokenForTenant returned no access token');
    expect($exchanged->getTenantId())->toBe(
        $targetTenant,
        "step 4: exchanged tenant_id is '{$exchanged->getTenantId()}', expected '{$targetTenant}'",
    );
    expect($exchanged->getProjectId())->toBe(
        $targetProject,
        "step 4: exchanged project_id is '{$exchanged->getProjectId()}', expected '{$targetProject}'",
    );
    $exchangedScopes = serviceAccountList($exchanged->getScopes());
    expect($exchangedScopes)->toBe(
        [SA_LIVE_CHECK_ACCESS_SCOPE],
        'step 4: exchanged scopes are ['.implode(',', $exchangedScopes).'], expected ['.SA_LIVE_CHECK_ACCESS_SCOPE.']',
    );
    expect($exchanged->getMembershipId())->not->toBe('', 'step 4: the exchanged token names no membership_id');
    $expiresAt = (int) $exchanged->getExpiresAtUnix();
    $ceiling = $now + SA_LIVE_EXCHANGED_TOKEN_MAX_SECONDS;
    expect($expiresAt > $now && $expiresAt <= $ceiling)->toBeTrue(
        "step 4: exchanged expires_at_unix {$expiresAt} is not within ({$now}, {$ceiling}]",
    );

    // The tenant/project hints of this Authenticate are the target's.
    $targetHints = serviceAccountMetadata($hints, $targetTenant, $targetProject, '');
    $exchangedPrincipal = $auth->authenticateBearer($exchangedToken, $targetHints)->getPrincipal();
    expect($exchangedPrincipal)->not->toBeNull('step 4: Authenticate returned no principal for the exchanged token');
    expect($exchangedPrincipal->getTenantId())->toBe(
        $targetTenant,
        "step 4: the exchanged principal's tenant_id is '{$exchangedPrincipal->getTenantId()}', expected '{$targetTenant}'",
    );
    expect($exchangedPrincipal->getUserId())->toBe(
        $serviceUserId,
        "step 4: the exchanged principal's user_id is '{$exchangedPrincipal->getUserId()}', expected the service account '{$serviceUserId}'",
    );

    // An authenticated call IN the target tenant. CheckAccess requires a
    // user_id; naming the caller itself keeps it claim-bound (the exchanged
    // token carries no delegation scope).
    $targetMeta = serviceAccountMetadata($hints, $targetTenant, $targetProject, $exchangedToken);
    $inTarget = $rpc->checkAccess(
        (new CheckAccessRequest())
            ->setUserId($serviceUserId)
            ->setObject(SA_LIVE_CHECK_OBJECT)
            ->setAction(SA_LIVE_CHECK_ACTION),
        $targetMeta,
    );
    expect($inTarget->getEvaluatedSubject())->toBe(
        $serviceUserId,
        "step 4: CheckAccess in the target tenant evaluated '{$inTarget->getEvaluatedSubject()}', expected the service account '{$serviceUserId}'",
    );

    $unapprovedCode = serviceAccountFailureCode(fn () => $rpc->exchangeServiceTokenForTenant(
        (new ExchangeServiceTokenForTenantRequest())
            ->setTargetTenantId($unapprovedTenant)
            ->setTargetProjectId($targetProject),
        $serviceMeta,
    ));
    expect($unapprovedCode)->toBe(
        SA_LIVE_PERMISSION_DENIED,
        "step 4: exchange into unapproved tenant {$unapprovedTenant} must fail PERMISSION_DENIED (7); got ".var_export($unapprovedCode, true),
    );

    // ── 5. Phone-only CreateUser activated by a phone OTP ──────────────────
    $phoneUsername = 'sdk-live-phone-php-'.bin2hex(random_bytes(6));
    $phone = '+1555'.sprintf('%07d', random_int(0, 9_999_999));
    $created = $rpc->createUser(
        (new CreateUserRequest())
            ->setUsername($phoneUsername)
            ->setPhone($phone)
            ->setPassword(SA_LIVE_PHONE_PASSWORD)
            ->setTenantId($homeTenant)
            ->setProjectId($humanProject),
        $humanMeta,
    );
    $user = $created->getUser();
    expect($user)->not->toBeNull('step 5: phone-only CreateUser returned no user');
    $userId = $user->getUserId();
    expect($userId)->not->toBe('', 'step 5: phone-only CreateUser returned no user_id');
    expect($user->getEmail())->toBe('', "step 5: phone-only user has email '{$user->getEmail()}', expected none");
    expect($user->getPhone())->toBe($phone, "step 5: created user.phone is '{$user->getPhone()}', expected '{$phone}'");
    expect($user->getStatus())->toBe(
        UserStatus::USER_STATUS_PENDING_VERIFICATION,
        'step 5: created user.status is '.UserStatus::name($user->getStatus()).', expected USER_STATUS_PENDING_VERIFICATION',
    );
    expect($created->getOtpId())->not->toBe('', 'step 5: phone-only CreateUser returned no otp_id');

    $sent = $rpc->sendOtp(
        (new SendOTPRequest())
            ->setUserId($userId)
            ->setOtpType(OTPType::OTP_TYPE_PHONE_VERIFICATION),
        $humanMeta,
    );
    expect($sent->getDevOtpCode())->not->toBe(
        '',
        'step 5: SendOTP(PHONE_VERIFICATION) echoed no dev_otp_code (is UDB_OTP_DEV_ECHO=1 set?)',
    );
    expect($sent->getOtpId())->not->toBe('', 'step 5: SendOTP(PHONE_VERIFICATION) returned no otp_id');
    $verified = $rpc->verifyOtp(
        (new VerifyOTPRequest())
            ->setOtpId($sent->getOtpId())
            ->setCode($sent->getDevOtpCode()),
        $humanMeta,
    );
    expect($verified->getVerified())->toBeTrue('step 5: VerifyOTP on the phone code did not verify');
    expect($verified->getOtpType())->toBe(
        OTPType::OTP_TYPE_PHONE_VERIFICATION,
        'step 5: VerifyOTP otp_type is '.OTPType::name($verified->getOtpType()).', expected OTP_TYPE_PHONE_VERIFICATION',
    );
    $fetched = $rpc->getUser((new GetUserRequest())->setUserId($userId), $humanMeta)->getUser();
    expect($fetched)->not->toBeNull('step 5: GetUser returned no user');
    expect($fetched->getStatus())->toBe(
        UserStatus::USER_STATUS_ACTIVE,
        'step 5: after VerifyOTP user.status is '.UserStatus::name($fetched->getStatus()).', expected USER_STATUS_ACTIVE',
    );
    expect($fetched->getPhone())->toBe($phone, "step 5: GetUser phone is '{$fetched->getPhone()}', expected '{$phone}'");
})->skip(getenv('UDB_LIVE_SDK_TESTS') !== '1', 'requires live UDB broker');
