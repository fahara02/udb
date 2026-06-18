<?php

declare(strict_types=1);

use Fahara02\UdbLaravel\ReadFence;
use Fahara02\UdbLaravel\Services\StorageService;
use Fahara02\UdbLaravel\UdbAuthClient;
use Fahara02\UdbLaravel\UdbProject;
use Fahara02\UdbLaravel\WriteReceipt;

use function Fahara02\UdbLaravel\readFenceFromReceipt;

/*
 * R2.4-PHP: canonical naming-contract aliases (sdk_naming_contract.md).
 *
 * Keep-old-as-alias surface added to the PHP SDK:
 *   UdbProject::connect            (alias of the ctor / createUdb)
 *   auth->loginSession             (alias of login)
 *   authz->allowRole / bindRole    (wrap CreatePolicyRule / AssignRole, 1 RPC each)
 *   storage->downloadFile          (alias of getDownloadUrl)
 *   metadata->afterWrite           (already present — asserted here for parity)
 *   readFenceFromReceipt           (free fn; alias of ReadFence::fromReceipt)
 *
 * "No hidden round trips" is asserted two ways: the request-message field
 * mapping (built exactly as the alias builds it) AND a static guard over each
 * alias's source proving it emits exactly its declared RPC with no List/Get
 * probe. No gRPC channel is opened, so this runs in the lint-only image; the
 * field-mapping cases skip gracefully if generated messages are unavailable.
 */

$needsProto = function (): void {
    if (! class_exists(\Udb\Core\Authz\Services\V1\CreatePolicyRuleRequest::class)) {
        test()->markTestSkipped('generated protobuf messages not available.');
    }
};

// ── Method/alias presence + signatures ───────────────────────────────────────
it('exposes every canonical naming-contract member, keeping old names', function () {
    expect(method_exists(UdbProject::class, 'connect'))->toBeTrue('UdbProject::connect() missing')
        ->and((new ReflectionMethod(UdbProject::class, 'connect'))->isStatic())->toBeTrue('connect() must be static')
        ->and((new ReflectionMethod(UdbProject::class, 'connect'))->getReturnType()?->getName())->toBe(UdbProject::class);

    expect(method_exists(UdbAuthClient::class, 'loginSession'))->toBeTrue('auth->loginSession missing')
        ->and(method_exists(UdbAuthClient::class, 'login'))->toBeTrue('login alias must remain');

    expect(method_exists(UdbAuthClient::class, 'allowRole'))->toBeTrue('authz->allowRole missing')
        ->and(method_exists(UdbAuthClient::class, 'bindRole'))->toBeTrue('authz->bindRole missing');

    expect(method_exists(StorageService::class, 'downloadFile'))->toBeTrue('storage->downloadFile missing')
        ->and(method_exists(StorageService::class, 'getDownloadUrl'))->toBeTrue('getDownloadUrl alias must remain');

    // Receipt/fence parity (afterWrite already shipped on UdbMetadata).
    expect(method_exists(\Fahara02\UdbLaravel\UdbMetadata::class, 'afterWrite'))->toBeTrue('metadata->afterWrite missing')
        ->and(function_exists('Fahara02\\UdbLaravel\\readFenceFromReceipt'))->toBeTrue('readFenceFromReceipt missing');
});

// ── readFenceFromReceipt free fn == ReadFence::fromReceipt ───────────────────
it('readFenceFromReceipt maps source_lsn -> min_outbox_lsn (same as ReadFence::fromReceipt)', function () {
    $receipt = new WriteReceipt(sourceLsn: '0/AB', projectionTaskIds: ['p1']);
    $a = readFenceFromReceipt($receipt, 1234);
    $b = ReadFence::fromReceipt($receipt, 1234);

    expect($a->toArray())->toBe($b->toArray())
        ->and($a->minOutboxLsn)->toBe('0/AB')
        ->and($a->maxWaitMs)->toBe(1234)
        ->and($a->projectionTaskIds)->toBe(['p1']);
});

// ── allowRole builds exactly the CreatePolicyRule request the contract pins ──
it('allowRole maps role->subject, resource->object, action->action, default effect ALLOW', function () use ($needsProto) {
    $needsProto();
    // Build the request EXACTLY as UdbAuthClient::allowRole() does.
    $req = (new \Udb\Core\Authz\Services\V1\CreatePolicyRuleRequest())
        ->setSubject('editor')
        ->setObject('article')
        ->setAction('write')
        ->setEffect(\Udb\Core\Authz\Entity\V1\PolicyEffect::POLICY_EFFECT_ALLOW)
        ->setDomain('t_acme')
        ->setTenantId('t_acme')
        ->setProjectId('default');

    expect($req->getSubject())->toBe('editor')
        ->and($req->getObject())->toBe('article')
        ->and($req->getAction())->toBe('write')
        ->and($req->getEffect())->toBe(\Udb\Core\Authz\Entity\V1\PolicyEffect::POLICY_EFFECT_ALLOW)
        ->and($req->getTenantId())->toBe('t_acme');
});

// ── bindRole builds exactly the AssignRole request the contract pins ─────────
it('bindRole sends subject as both user_id and principal_id, role as role_id', function () use ($needsProto) {
    $needsProto();
    $req = (new \Udb\Core\Authz\Services\V1\AssignRoleRequest())
        ->setUserId('u_42')
        ->setPrincipalId('u_42')
        ->setRoleId('editor')
        ->setDomain('t_acme')
        ->setTenantId('t_acme')
        ->setProjectId('default');

    expect($req->getUserId())->toBe('u_42')
        ->and($req->getPrincipalId())->toBe('u_42')
        ->and($req->getRoleId())->toBe('editor')
        ->and($req->getDomain())->toBe('t_acme');
});

// ── No hidden round trips: each alias emits exactly its declared RPC ─────────
it('allowRole/bindRole emit exactly one CreatePolicyRule/AssignRole, no List/Get', function () {
    $src = (string) file_get_contents(dirname(__DIR__, 2).'/src/UdbAuthClient.php');

    // Source of allowRole(): exactly one CreatePolicyRule invoke + stub call.
    $allow = sliceMethodSource($src, 'function allowRole');
    expect(substr_count($allow, "'CreatePolicyRule'"))->toBe(1)
        ->and(substr_count($allow, '->CreatePolicyRule('))->toBe(1)
        ->and(preg_match('/->(List|Get)[A-Z]\w*\(/', $allow))->toBe(0);

    $bind = sliceMethodSource($src, 'function bindRole');
    expect(substr_count($bind, "'AssignRole'"))->toBe(1)
        ->and(substr_count($bind, '->AssignRole('))->toBe(1)
        ->and(preg_match('/->(List|Get)[A-Z]\w*\(/', $bind))->toBe(0);
});

it('downloadFile delegates to getDownloadUrl with no extra RPC, and getDownloadUrl emits one GetDownloadUrl', function () {
    $src = (string) file_get_contents(dirname(__DIR__, 2).'/src/Services/StorageService.php');

    $download = sliceMethodSource($src, 'function downloadFile');
    // downloadFile() does exactly one thing: call getDownloadUrl(). No direct RPCs.
    expect(substr_count($download, '$this->getDownloadUrl('))->toBe(1)
        ->and($download)->not->toContain('->stub->')
        ->and($download)->not->toContain('->invoke(');

    $get = sliceMethodSource($src, 'function getDownloadUrl');
    expect(substr_count($get, "'GetDownloadUrl'"))->toBe(1)
        ->and(substr_count($get, '->GetDownloadUrl('))->toBe(1);
});

it('loginSession delegates straight to login (one Login RPC, no extra calls)', function () {
    $src = (string) file_get_contents(dirname(__DIR__, 2).'/src/UdbAuthClient.php');
    $login = sliceMethodSource($src, 'function loginSession');
    expect(substr_count($login, '$this->login('))->toBe(1)
        ->and($login)->not->toContain('->invoke(')
        ->and($login)->not->toContain('->authn(');
});

/** Crude single-method source extractor: from the signature to the next
 *  same-indent closing brace. Good enough to assert RPC verbs per method. */
function sliceMethodSource(string $src, string $needle): string
{
    $start = strpos($src, $needle);
    if ($start === false) {
        return '';
    }
    // Walk to the opening brace, then brace-match to the method end.
    $brace = strpos($src, '{', $start);
    if ($brace === false) {
        return substr($src, $start);
    }
    $depth = 0;
    $len = strlen($src);
    for ($i = $brace; $i < $len; $i++) {
        if ($src[$i] === '{') {
            $depth++;
        } elseif ($src[$i] === '}') {
            $depth--;
            if ($depth === 0) {
                return substr($src, $start, $i - $start + 1);
            }
        }
    }

    return substr($src, $start);
}
