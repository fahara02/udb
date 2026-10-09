#!/usr/bin/env python3
"""Fail CI if the Go simple-client SDK posture drifts."""

from __future__ import annotations

import argparse
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class SourceCheck:
    label: str
    path: str
    required: tuple[str, ...]
    forbidden: tuple[str, ...] = ()


CHECKS: tuple[SourceCheck, ...] = (
    SourceCheck(
        "go consistency helpers",
        "sdk/go/udbclient/consistency.go",
        (
            "type WriteReceipt struct",
            "SourceLsn",
            '`json:"source_lsn"`',
            'ProjectionTaskIds []string `json:"projection_task_ids"`',
            "func ParseWriteReceipt(b []byte) (WriteReceipt, error)",
            "type ReadFence struct",
            "MinOutboxLsn",
            '`json:"min_outbox_lsn,omitempty"`',
            'ProjectionTaskIds []string `json:"projection_task_ids,omitempty"`',
            "func ReadFenceFromReceipt(r WriteReceipt, maxWaitMs uint64) ReadFence",
            "MinOutboxLsn:      r.SourceLsn",
            "func ReceiptFromMutation(m *entityv1.MutationResponse) (WriteReceipt, error)",
            "func AfterWrite(rc *entityv1.RequestContext, r WriteReceipt, maxWaitMs uint64)",
            "rc.ReadFenceJson = string(b)",
        ),
    ),
    SourceCheck(
        "go consistency golden tests",
        "sdk/go/udbclient/consistency_test.go",
        (
            "TestReceiptFenceGoldenJSON",
            "docs/generated/consistency-golden.json",
            "ReadFenceFromReceipt(receipt, 2500)",
            "TestWriteReceiptAllFieldsUnconditional",
            "TestReadFenceEmptySkips",
            "TestReadFenceFromReceiptMapping",
            "TestAfterWriteStampsFence",
            "TestReceiptFromMutation",
        ),
    ),
    SourceCheck(
        "go typed error detail helpers",
        "sdk/go/udbclient/errordetail.go",
        (
            "func (e *Error) Detail() (*entityv1.ErrorDetail, bool)",
            "proto.Unmarshal(e.DetailBin, &d)",
            "func (e *Error) Retryable() bool",
            "func (e *Error) Kind() entityv1.ErrorKind",
        ),
    ),
    SourceCheck(
        "go typed error detail tests",
        "sdk/go/udbclient/errordetail_test.go",
        (
            "ErrorDetail{",
            "Retryable:    true",
            "Retryable()",
            "Kind()",
            "DetailBin",
        ),
    ),
    SourceCheck(
        "go storage upload helper",
        "sdk/go/udbclient/media.go",
        (
            "type httpDoer interface",
            "var uploadHTTPDoer httpDoer = http.DefaultClient",
            "MaxUploadBytes int64",
            "type UploadOptions struct",
            "func (f *StorageFacade) UploadFile(ctx context.Context, filename string, data []byte, opts ...UploadOption)",
            "if f.MaxUploadBytes > 0 && int64(len(data)) > f.MaxUploadBytes",
            "http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))",
            "f.Raw.FinalizeUpload(ctx, fin)",
        ),
    ),
    SourceCheck(
        "go storage upload sequence tests",
        "sdk/go/udbclient/upload_test.go",
        (
            "TestUploadFileExactSequence",
            "loadWorkflowSequence(t, \"StorageFacade.uploadFile\")",
            "TestUploadFileNoURLSkipsPut",
            "StorageFacade.uploadFile.noUrl",
            "TestUploadFileSizeCap",
            "withUploadDoer",
        ),
    ),
    SourceCheck(
        "go bound entity helper",
        "sdk/go/udbclient/entity.go",
        (
            "type EntityKey []string",
            "func Key(fields ...string) EntityKey",
            "func (c *Client) Entity(fqn string, key EntityKey) *Entity",
            "func (u *Udb) Entity(fqn string, key EntityKey) *Entity",
            "func (e *Entity) requestContext() *entityv1.RequestContext",
            "func ReturnRecord() UpsertOption",
            "func (e *Entity) Upsert(ctx context.Context, record any, opts ...UpsertOption)",
            "ConflictFields: []string(e.key)",
            "func (e *Entity) Select(ctx context.Context, where map[string]any)",
            "structpb.NewStruct(where)",
            "func (e *Entity) Delete(ctx context.Context, where map[string]any, opts ...DeleteOption)",
            "type EntityDescriptor struct",
        ),
    ),
    SourceCheck(
        "go bound entity tests",
        "sdk/go/udbclient/entity_test.go",
        (
            "TestEntityUpsertSingleRPC",
            "entity requestContext must NOT set body scopes",
            "entity requestContext must NOT force primary_read",
            "TestEntityUpsertReturnRecordNoSecondRPC",
            "TestEntitySelectSingleRPC",
            "TestEntityDeleteSingleRPC",
            "TestEntityRequestContextDistinct",
        ),
    ),
    SourceCheck(
        "go generated entity registry template",
        "sdk-templates/go/udbclient/generated_client.go.tmpl",
        (
            "var Entities = map[string]EntityDescriptor{",
            "// @@UDB_ENTITY_BEGIN",
            '"{{ENTITY_MESSAGE_TYPE}}": {Table: "{{ENTITY_TABLE}}"',
            "PrimaryKeys: []string{{{ENTITY_PRIMARY_KEYS}}}",
            "Fields: []string{{{ENTITY_JSON_FIELDS}}}",
            "Relations: mustEntityRelations(`{{ENTITY_RELATIONS_JSON}}`)",
            "VersionField: \"{{ENTITY_VERSION_FIELD}}\"",
            "TenantField: \"{{ENTITY_TENANT_FIELD}}\"",
            "ProjectField: \"{{ENTITY_PROJECT_FIELD}}\"",
            "GoType: \"{{ENTITY_GO_TYPE}}\"",
            "// @@UDB_ENTITY_END",
        ),
    ),
    SourceCheck(
        "go generated replay-safe and atomic metadata template",
        "sdk-templates/go/udbclient/generated_client.go.tmpl",
        (
            "func (rc RetryConfig) retryableForRPC(code codes.Code, readOnly, replaySafe, hasIdempotencyKey bool) bool",
            "if !replaySafe || !hasIdempotencyKey",
            "ReplaySafe    bool",
            "ReplaySafe: {{RPC_REPLAY_SAFE}}",
            "func (g *GeneratedClient) SetMeta(meta Metadata)",
            "func (g *GeneratedClient) SetAuthorization(authorization string)",
            "func isReplaySafeRPC(method string) bool",
        ),
    ),
    SourceCheck(
        "go emitted replay-safe and atomic metadata",
        "sdk/go/udbclient/generated_client.go",
        (
            "func (rc RetryConfig) retryableForRPC(code codes.Code, readOnly, replaySafe, hasIdempotencyKey bool) bool",
            "if !replaySafe || !hasIdempotencyKey",
            "ReplaySafe    bool",
            "ReplaySafe: true",
            "ReplaySafe: false",
            "func (g *GeneratedClient) SetMeta(meta Metadata)",
            "func (g *GeneratedClient) SetAuthorization(authorization string)",
            "func isReplaySafeRPC(method string) bool",
        ),
    ),
    SourceCheck(
        "go replay-safe retry tests",
        "sdk/go/udbclient/generated_retry_test.go",
        (
            "TestRetryConfigReplaySafeMutationRequiresKey",
            "replay-safe mutation with key on Unavailable",
            "replay-safe mutation without key",
            "non-replay-safe mutation with key",
            "DeadlineExceeded",
            "TestRetryReplaySafeMutationWithKeyRetriesThenSucceeds",
            "TestRetryReplaySafeMutationWithoutKeyDoesNotRetry",
            "TestRetryNonReplaySafeMutationDoesNotRetry",
        ),
    ),
    SourceCheck(
        "go login adopt and metadata swap",
        "sdk/go/udbclient/project.go",
        (
            "adoptMu sync.Mutex",
            "func (u *Udb) adoptMetadata(meta Metadata, bearer string)",
            "u.installGeneratedIdentity(meta, bearer)",
            "opt.Meta = meta",
            "u.Generated.opt.Store(&opt)",
            "type AdoptedLogin struct",
            "func (u *Udb) LoginAndAdoptTenant(ctx context.Context, req *authnv1.LoginRequest)",
            "u.Auth.Authn.Login(u.Auth.Context(u.recoveryContext(ctx, credentialLogin)), req)",
            "u.Auth.AuthenticateBearer(u.recoveryContext(ctx, credentialAuthenticate), token)",
            "u.adoptMetadata(meta, bearer)",
            "opt.Authorization = \"Bearer \" + bearer",
            "func (u *Udb) loginAndVerify(ctx context.Context, req *authnv1.LoginRequest)",
            "u.adoptPrincipal(adopted.Principal, adopted.Token.AccessToken)",
        ),
    ),
    SourceCheck(
        "go login adopt tests",
        "sdk/go/udbclient/adopt_test.go",
        (
            "TestLoginAndAdoptTenantTwoRPCs",
            "must be EXACTLY [Login, AuthenticateBearer]",
            "TestSetMetaAtomicSwap",
            "SetMeta",
            "SetAuthorization",
        ),
    ),
    SourceCheck(
        "go login with device helper",
        "sdk/go/udbclient/token.go",
        (
            "func (m *TokenManager) LoginWithDevice(ctx context.Context, req *authnv1.LoginRequest) (Token, error)",
            "m.auth.Authn.Login(m.auth.Context(ctx), req)",
            "tokenFromLogin(resp, m.now())",
        ),
    ),
    SourceCheck(
        "go login with device test",
        "sdk/go/udbclient/auth_helpers_test.go",
        (
            "TestLoginWithDeviceSendsDeviceID",
            "LoginWithDevice must issue only Login",
            "DeviceId: \"dev-stable\"",
        ),
    ),
    SourceCheck(
        "go admin approval-token body helper",
        "sdk/go/udbclient/admin.go",
        (
            "func (a *AdminFacade) ApplyCurrent(ctx context.Context, projectID string)",
            "approve.GetApprovalToken()",
            "ApprovalToken: token",
        ),
    ),
    SourceCheck(
        "go shared tenant isolation contract",
        "sdk/go/udbtest/conformance/table.go",
        (
            "func Tenants(first, second string) Option",
            "conformance requires two distinct provisioned tenants",
            "b = o.tenants[1]",
            't.Run("tenants never see each other\'s rows"',
            "another tenant read the row",
            "naming another tenant in a filter must be refused",
        ),
        ("SingleTenant", "singleTenant", "t.Skip("),
    ),
    SourceCheck(
        "go live provisioned tenant identities",
        "sdk/go/udbtest/live_test.go",
        (
            'required("UDB_LIVE_PEER_USERNAME")',
            'required("UDB_LIVE_PEER_PASSWORD")',
            'required("UDB_LIVE_PEER_TENANT")',
            "peer := connectLiveIdentity(t, ctx, true)",
            "tenant == peer.CanonicalTenantID",
            "return peer.Udb",
            "live conformance requires two distinct verified tenant IDs",
            "conformance.Tenants(sess.CanonicalTenantID, peer.CanonicalTenantID)",
        ),
        ("conformance.SingleTenant()",),
    ),
    SourceCheck(
        "go live second tenant provisioning",
        ".github/workflows/go-sdk-live.yml",
        (
            "UDB_LIVE_PEER_USERNAME: sdk-live-peer",
            "UDB_LIVE_PEER_TENANT: sdk-live-peer",
            "name: Provision the second conformance tenant",
            '--username "${UDB_LIVE_PEER_USERNAME}"',
            '--password "${UDB_LIVE_PEER_PASSWORD}"',
            '--tenant "${UDB_LIVE_PEER_TENANT}"',
            "go test ./udbtest -run '^TestLiveBrokerMeetsTheTableContract$' -count=1 -v -timeout 10m",
        ),
        ("--platform-admin",),
    ),
)



CHECKS += (
    SourceCheck(
        "go private owned credential preflight",
        "sdk/go/udbclient/credential_lifecycle.go",
        (
            "type credentialOwner struct",
            "type credentialPrincipalBinding struct",
            "func (b credentialPrincipalBinding) validate(p *authnv1.Principal) error",
            "func (g *GeneratedClient) prepareCredential(ctx context.Context, method string)",
            "prepared.used.CompareAndSwap(false, true)",
            "capability.owner == o && capability.method == method",
            "method == credentialAuthenticate || method == credentialLogin || method == credentialRefresh",
            "func (o *credentialOwner) closedError() error",
            "if err := o.closedError(); err != nil",
            "bearer, err := provider.resolve(resolveCtx)",
            "context.AfterFunc(o.ctx, cancel)",
            'md.Delete("x-api-key")',
        ),
    ),
    SourceCheck(
        "go generated owned credential transport",
        "sdk/go/udbclient/generated_client.go",
        (
            "credential atomic.Pointer[credentialResolver]",
            "callCtx, err := g.prepareCredential(callCtx, fullMethod)",
            "callCtx, retireHandoff := g.credentialHandoff(callCtx, fullMethod)",
            "attemptCtx = context.WithValue(ctx, credentialPreparedKey{}, nil)",
            'hasNonempty := func(key string) bool',
        ),
    ),
    SourceCheck(
        "go tenant pool generation fence",
        "sdk/go/udbclient/tenant_pool.go",
        (
            "func (p *TenantSessionPool) completeDialLocked",
            "p.pending[tenantID] == dial",
            "dial.stopCancellation()",
            "if !installed && u != nil",
            "_ = u.Close()",
        ),
    ),
    SourceCheck(
        "go real login and API-key renewal proofs",
        ".github/workflows/_live-sdk-suite.yml",
        (
            "^TestLiveG1(SessionLifecycle|APIKeyCredentialLifecycle)$",
            "-count=1 -v -timeout 5m",
        ),
    ),
    SourceCheck(
        "go credential and pool race proofs",
        ".github/workflows/ci.yml",
        (
            "TokenManager|EnterpriseSession|APIKey|ConsumeG6|AsUser|CredentialLifecycle|TenantSessionPool",
        ),
    ),
)


def check_source(root: Path = ROOT) -> list[str]:
    failures: list[str] = []
    for check in CHECKS:
        path = root / check.path
        if not path.is_file():
            failures.append(f"{check.label}: missing file {check.path}")
            continue
        text = path.read_text(encoding="utf-8")
        for token in check.required:
            if token not in text:
                failures.append(f"{check.label}: missing token {token!r} in {check.path}")
        for token in check.forbidden:
            if token in text:
                failures.append(f"{check.label}: forbidden stale token {token!r} in {check.path}")
    return failures


def run_selftest() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        contents_by_path: dict[str, list[str]] = {}
        for check in CHECKS:
            contents_by_path.setdefault(check.path, []).extend(check.required)

        for rel_path, tokens in contents_by_path.items():
            path = root / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("\n".join(dict.fromkeys(tokens)) + "\n", encoding="utf-8")

        failures = check_source(root)
        if failures:
            raise AssertionError(f"expected clean fixture, got {failures}")

        media = root / "sdk/go/udbclient/media.go"
        media.write_text(
            media.read_text(encoding="utf-8").replace("func (f *StorageFacade) UploadFile", "func (f *StorageFacade) PutFile"),
            encoding="utf-8",
        )
        failures = check_source(root)
        if not any("UploadFile" in failure for failure in failures):
            raise AssertionError(f"expected UploadFile drift failure, got {failures}")

        admin = root / "sdk/go/udbclient/admin.go"
        admin.write_text(admin.read_text(encoding="utf-8").replace("approve.GetApprovalToken()", "metadataToken()"), encoding="utf-8")
        failures = check_source(root)
        if not any("approve.GetApprovalToken()" in failure for failure in failures):
            raise AssertionError(f"expected approval-token body-field failure, got {failures}")

        for rel_path, missing_token, expected in (
            ("sdk/go/udbclient/credential_lifecycle.go", "capability.owner == o && capability.method == method", "go private owned credential preflight"),
            ("sdk/go/udbclient/generated_client.go", "callCtx, err := g.prepareCredential(callCtx, fullMethod)", "go generated owned credential transport"),
            ("sdk/go/udbclient/tenant_pool.go", "p.pending[tenantID] == dial", "go tenant pool generation fence"),
            (".github/workflows/_live-sdk-suite.yml", "^TestLiveG1(SessionLifecycle|APIKeyCredentialLifecycle)$", "go real login and API-key renewal proofs"),
            ("sdk/go/udbtest/live_test.go", 'required("UDB_LIVE_PEER_USERNAME")', "go live provisioned tenant identities"),
            (".github/workflows/go-sdk-live.yml", '--tenant "${UDB_LIVE_PEER_TENANT}"', "go live second tenant provisioning"),
        ):
            path = root / rel_path
            original = path.read_text(encoding="utf-8")
            path.write_text(original.replace(missing_token, "removed"), encoding="utf-8")
            failures = check_source(root)
            if not any(expected in failure for failure in failures):
                raise AssertionError(f"expected second-tenant drift failure, got {failures}")
            path.write_text(original, encoding="utf-8")

        table = root / "sdk/go/udbtest/conformance/table.go"
        table.write_text(table.read_text(encoding="utf-8") + '\nt.Skip("one tenant")\n', encoding="utf-8")
        failures = check_source(root)
        if not any("go shared tenant isolation contract: forbidden" in failure for failure in failures):
            raise AssertionError(f"expected skipped tenant proof failure, got {failures}")

    print("Go SDK posture selftest passed")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selftest", action="store_true", help="run no-repo assertions")
    args = parser.parse_args(argv)
    if args.selftest:
        return run_selftest()

    failures = check_source()
    if failures:
        for failure in failures:
            print(f"::error::{failure}", file=sys.stderr)
        return 1
    print("Go SDK posture guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
