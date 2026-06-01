<?php

declare(strict_types=1);

/**
 * Admin-flow variant — the full native-services provisioning sequence from PHP,
 * symmetric with the Go example: register a user, define RBAC
 * (role → assignment → policy), verify the access check, and mint an API key the
 * consumer example (main.php) can authenticate.
 *
 * Uses the generated service clients directly (the UdbAuthClient wrapper covers
 * the consumer surface; provisioning drives the raw Authn/Authz/ApiKey RPCs).
 *
 * Prereqs: the `grpc` PHP extension, a running broker, and the SDK on the
 * autoloader (composer path repo → ../../../sdk/php). Run:
 *     UDB_TARGET=127.0.0.1:50051 php admin.php
 * It prints `export UDB_API_KEY=...` for the consumer example.
 */

require __DIR__ . '/vendor/autoload.php';

use Fahara02\UdbLaravel\UdbMetadata;
use Grpc\ChannelCredentials;
use Udb\Core\Apikey\Services\V1\ApiKeyServiceClient;
use Udb\Core\Apikey\Services\V1\CreateApiKeyRequest;
use Udb\Core\Authn\Services\V1\AuthnServiceClient;
use Udb\Core\Authn\Services\V1\CreateUserRequest;
use Udb\Core\Authz\Services\V1\AssignRoleRequest;
use Udb\Core\Authz\Services\V1\AuthzPolicyRecord;
use Udb\Core\Authz\Services\V1\AuthzServiceClient;
use Udb\Core\Authz\Services\V1\CheckAccessRequest;
use Udb\Core\Authz\Services\V1\CreateRoleRequest;
use Udb\Core\Authz\Services\V1\PutAuthzPolicyRequest;

$endpoint = getenv('UDB_TARGET') ?: '127.0.0.1:50051';
$opts = ['credentials' => ChannelCredentials::createInsecure()];
$md = (new UdbMetadata(
    tenantId: 'acme',
    userId: '',
    purpose: 'control-plane',
    correlationId: 'native-php-admin',
    scopes: ['udb:*'],
    serviceIdentity: 'examples.native-php-admin',
    projectId: 'billing',
    clientCatalogVersion: '1.0.0',
))->toGrpcMetadata();

$authn = new AuthnServiceClient($endpoint, $opts);
$authz = new AuthzServiceClient($endpoint, $opts);
$apikey = new ApiKeyServiceClient($endpoint, $opts);
$suffix = (string) round(microtime(true) * 1000);

/** Wait on a unary call; exit on non-OK status. */
$call = static function (\Grpc\UnaryCall $c): object {
    [$resp, $status] = $c->wait();
    if (($status->code ?? -1) !== 0) {
        fwrite(STDERR, 'RPC failed: ' . ($status->details ?? '') . "\n");
        exit(1);
    }
    return $resp;
};

// ── Step 1: register a user ──────────────────────────────────────────────────
$user = $call($authn->CreateUser(
    (new CreateUserRequest())
        ->setUsername("alice_$suffix")->setEmail("alice_$suffix@example.com")
        ->setPassword('CorrectHorse1!')->setTenantId('acme')
        ->setFullName('Alice Example')->setProjectId('billing'),
    $md,
))->getUser();
printf("1) registered user %s (%s)\n", $user->getUserId(), $user->getUsername());

// ── Step 2: RBAC — role → assignment → allow policy ──────────────────────────
$role = $call($authz->CreateRole(
    (new CreateRoleRequest())
        ->setName("Reader $suffix")->setRoleCode("reader_$suffix")
        ->setCreatedBy($user->getUserId())->setDomain('acme')
        ->setTenantId('acme')->setProjectId('billing'),
    $md,
))->getRole();
$call($authz->AssignRole(
    (new AssignRoleRequest())
        ->setUserId($user->getUserId())->setRoleId($role->getRoleId())
        ->setDomain('acme')->setAssignedBy($user->getUserId())
        ->setTenantId('acme')->setProjectId('billing'),
    $md,
));
$call($authz->PutAuthzPolicy(
    (new PutAuthzPolicyRequest())->setPolicy(
        (new AuthzPolicyRecord())
            ->setId('policy-' . $role->getRoleCode())->setEnabled(true)->setEffect('allow')
            ->setTenant('acme')->setProject('billing')->setRole($role->getRoleCode())
            ->setAction('data.select')->setResource('invoice'),
    ),
    $md,
));
printf("2) role %s assigned to user; allow policy on invoice/data.select added\n", $role->getRoleCode());

// ── Step 3: verify the access check ──────────────────────────────────────────
$decision = $call($authz->CheckAccess(
    (new CheckAccessRequest())
        ->setUserId($user->getUserId())->setDomain('acme')->setTenantId('acme')
        ->setProjectId('billing')->setObject('invoice')->setAction('data.select'),
    $md,
));
printf("3) check data.select on invoice → %s\n", $decision->getAllowed() ? 'true' : 'false');

// ── Step 4: mint an API key for the consumer example ─────────────────────────
$created = $call($apikey->CreateApiKey(
    (new CreateApiKeyRequest())
        ->setName('native-php-admin-key')->setOwnerId($user->getUserId())->setScopes(['data:read']),
    $md,
));
printf("4) minted dev API key → export UDB_API_KEY=%s\n", $created->getPlainKey());
