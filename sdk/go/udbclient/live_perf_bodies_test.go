package udbclient

// Explicit per-RPC request bodies for the perf harness, sourced from the shared
// strict-JSON bench-body manifest. There is NO generic fill: every measured RPC
// hydrates a dynamicpb request with real fields, valid enum values, and seeded
// reference IDs, so a request never carries placeholder garbage that the broker
// rejects with INVALID_ARGUMENT. An RPC with no manifest body is reported as a
// NO-BODY failure for the maintainer to add — it is NEVER generic-probed.
//
// Filled service-by-service following the auth route (Phase 1 auth → all RPCs →
// Phase 3 terminal auth). Each batch turns its RPCs from NO-BODY into a measured
// success path.

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/backup/services/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/cache/services/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/config/services/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/control/services/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/embedding/services/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/livequery/services/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/lock/services/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/metering/services/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/scheduler/services/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/search/services/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/vault/services/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/webhook/services/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/workflow/services/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// errNoExplicitBody is returned for an RPC that has no strict-JSON manifest body.
// The generic fill is NUKED: rather than send a placeholder
// request the broker rejects with INVALID_ARGUMENT, the harness surfaces this as a
// clear NO-BODY failure so the missing body gets added to BENCH_RPC_BODIES.md.
var errNoExplicitBody = errors.New("NO-BODY")

// perfSeedUserPassword is the password the perf seed creates its disposable user
// with; the ChangePassword manifest body presents it as current_password so the change
// succeeds against the real account.
const perfSeedUserPassword = "CorrectHorse1!"

func TestLivePerfExplicitBodyCoverage(t *testing.T) {
	fix := newPerfFixtures()
	for k, v := range map[string]string{
		"tenant_id": "tenant-1", "tenant": "tenant-1", "project": "project-1", "project_id": "project-1",
		"tenant_code": "tenant-code-1", "purge_tenant_id": "tenant-purge-1",
		// Separate disposable targets: the privileged AdminPurgeTenant must not point at
		// the caller's tenant, the grant transfer needs its own source/target service
		// accounts, and FinalizeUpload must resend the reference_id set at register.
		"admin_purge_tenant_id":       "tenant-admin-purge-1",
		"grant_transfer_from_user_id": "user-grant-from-1",
		"finalize_reference_id":       "finalize-ref-1",
		"message_type":                liveMessageType, "record_id": "record-1", "bucket": "bucket-1", "object_key": "object-1",
		"document_id": "document-1", "mongo_collection": "collection_1", "node_id": "node-1",
		"user_id": "user-1", "subject": "user:user-1", "session_id": "session-1", "token": "token-1",
		"refresh_token": "refresh-1", "csrf_token": "csrf-1", "code": "123456", "role_id": "role-1",
		"role": "reader", "role_code": "reader", "user_role_id": "user-role-1", "policy_id": "1",
		"policy_draft_id": "draft-1", "relation": "member", "object": "group:bench", "resource": "invoice",
		"action": "data.select", "key_id": "key-1", "plain_key": "udbk_key", "stage_name": "stage-1",
		"event_type": "event-1", "log_id": "log-1", "file_id": "file-1", "definition_id": "definition-1",
		"admin_reset_mfa_user_id": "admin-reset-mfa-user-1", "admin_reset_password_user_id": "admin-reset-password-user-1",
		"asset_id": "asset-1", "instance_id": "instance-1", "room_id": "room-1", "peer_id": "peer-1",
		"track_id": "track-1", "provider_id": "provider-1", "migration_id": "migration-1", "saga_id": "saga-1",
		"apply_run_id": "apply-run-1", "approval_token": "approval-token-1", "approve_run_id": "approve-run-1",
		"auth_challenge_id": "auth-challenge-1", "backup_id": "backup-1", "canary_id": "canary-1", "canary_version_id": "canary-version-1",
		"cancel_workflow_id": "cancel-workflow-1", "catalog_manifest_b64": "e30=", "challenge_id": "challenge-1",
		"change_password_user_id": "change-password-user-1", "change_status_user_id": "change-status-user-1",
		"close_room_id": "close-room-1", "delete_endpoint_id": "delete-endpoint-1", "delete_file_id": "delete-file-1",
		"device_id": "device-1", "disable_mfa_user_id": "disable-mfa-user-1", "disable_provider_id": "disable-provider-1", "dismiss_dlq_id": "dismiss-dlq-1", "dlq_id": "dlq-1",
		"delete_policy_id": "delete-policy-1", "delete_role_id": "delete-role-1",
		"ds_policy_id": "2", "egress_id": "egress-1", "endpoint_id": "endpoint-1", "external_identity_id": "external-1",
		"finalize_file_id": "finalize-file-1", "gov_exp": "1900000000", "job_id": "job-1",
		"embedding_job_id": "11111111-1111-4111-8111-000000000101", "embedding_work_item_id": "11111111-1111-4111-8111-000000000102",
		"embedding_document_id": "11111111-1111-4111-8111-000000000103", "embedding_document_job_id": "11111111-1111-4111-8111-000000000104",
		"embedding_delete_model_id": "embedding-delete-model-1",
		"grant_binding_id":          "11111111-1111-4111-8111-000000000201",
		"grant_create_user_id":      "11111111-1111-4111-8111-000000000202",
		"grant_transfer_to_user_id": "11111111-1111-4111-8111-000000000203",
		"join_session_room_id":      "join-room-1", "leave_peer_id": "leave-peer-1", "mark_saga_id": "mark-saga-1",
		"otp_code": "123456", "otp_id": "otp-1", "owner_id": "owner-1", "quarantine_dlq_id": "quarantine-dlq-1",
		"policy_version_id": "policy-version-1", "approve_draft_id": "approve-draft-1", "reject_draft_id": "reject-draft-1",
		"refresh_session_id": "refresh-session-1", "reg_challenge_id": "reg-challenge-1", "replay_dlq_id": "replay-dlq-1",
		"reset_otp_code": "654321", "reset_otp_id": "reset-otp-1", "resource_name": "resource-1",
		"restore_tenant_id": "restore-tenant-1", "retry_saga_id": "retry-saga-1", "revoke_key_id": "revoke-key-1",
		"rollback_policy_set_id": "rollback-set-1", "rollback_resource_version": "control-version-1", "rollback_target_version_id": "rollback-version-1",
		"release_fencing_token": "2", "renew_fencing_token": "1", "revoke_device_id": "revoke-device-1", "revoke_recovery_user_id": "revoke-recovery-user-1",
		"saml_provider_id": "saml-provider-1", "scim_group_id": "sdk-perf-group", "scim_user_id": "scim-user-1", "delete_scim_user_id": "delete-scim-user-1",
		"signal_peer_id": "signal-peer-1", "step_id": "step-1", "topic_pattern": "topic.*", "ts_table": "sdk_timeseries",
		"unpublish_track_id": "unpublish-track-1", "update_draft_id": "update-draft-1", "update_key_id": "update-key-1", "username": "perf-u",
		"vault_ciphertext": "vault-ciphertext-1", "vault_db_role": "readonly", "vault_db_idempotency_key": "vault-db-idempotency-1", "vault_db_lease_id": "vault-db-lease-1", "vault_delete_secret_path": "secret/delete",
		"vault_create_key_name": "transit-create-key", "vault_destroy_secret_path": "secret/destroy", "vault_key_name": "transit-key", "vault_put_secret_path": "secret/put", "vault_secret_path": "secret/path",
		"vault_signature": "vault-signature-1", "vault_signing_key_name": "transit-signing-key", "vault_hmac_key_name": "transit-hmac-key", "reissue_file_id": "reissue-file-1", "workflow_id": "workflow-1",
	} {
		fix.set(k, v)
	}
	var missing []string
	for _, rpc := range AllRPCs {
		if _, _, ok := buildSpecBody(rpc.FullMethod, fix); ok {
			continue
		}
		missing = append(missing, rpc.FullMethod)
	}
	if len(missing) > 0 {
		t.Fatalf("perf body coverage has %d NO-BODY gaps: %s", len(missing), strings.Join(missing, ", "))
	}
}

// Auth route (BENCH_RPC_BODIES.md): Phase 1 establishes the session FIRST, Phase 3
// tears it down LAST; everything else is Phase 2. orderRPCsByAuthPhase returns the
// measurement order Phase1→Phase2→Phase3 so terminal auth never kills the run.
var phase1AuthnOrder = []string{
	// RefreshSession + Authenticate consume the dedicated refresh_session_id/auth_token
	// and run BEFORE RefreshToken (which rotates the shared family) for belt-and-suspenders.
	"Login", "RefreshSession", "Authenticate", "ValidateToken", "IntrospectToken", "RefreshToken", "GetJwks",
}

var phase3Authn = map[string]bool{
	"Logout": true, "RevokeSession": true, "AdminRevokeSession": true,
	"AdminRevokeAllUserSessions": true, "AdminRevokeAllTenantSessions": true, "EmergencyRevoke": true,
	"ChangePassword": true, "ResetPassword": true, "AdminResetPassword": true, "ChangeUserStatus": true,
	"AdminResetMfa": true, "RevokeRecoveryCodes": true, "RevokeDevice": true,
	"DeleteWebAuthnCredential": true, "DisableMfaFactor": true,
}

var finalEphemeralCleanupRPCs = map[string]bool{
	"/udb.core.tenant.services.v1.TenantService/PurgeTenant": true,
}

func orderRPCsByAuthPhase(all []RPCInfo) []RPCInfo {
	p1idx := map[string]int{}
	for i, n := range phase1AuthnOrder {
		p1idx[n] = i
	}
	var p1, p2, p3, final []RPCInfo
	for _, r := range all {
		if finalEphemeralCleanupRPCs[r.FullMethod] {
			final = append(final, r)
			continue
		}
		if r.Service == "AuthnService" {
			if _, ok := p1idx[r.Name]; ok {
				p1 = append(p1, r)
				continue
			}
			if phase3Authn[r.Name] {
				p3 = append(p3, r)
				continue
			}
		}
		p2 = append(p2, r)
	}
	sort.Slice(p1, func(i, j int) bool { return p1idx[p1[i].Name] < p1idx[p1[j].Name] })
	// Within Phase 2, run reads BEFORE mutations BEFORE destructive ops so a read of a
	// seeded entity (GetRole/GetApiKey/GetPolicyRule) is never invalidated by a
	// delete/revoke of that same entity later in the run.
	okRank := map[string]int{"read_only": 0, "mutation": 1, "destructive": 2}
	catalogLifecycleRank := map[string]int{"StageCatalog": 0, "ActivateCatalog": 1, "RollbackCatalog": 2}
	sort.SliceStable(p2, func(i, j int) bool {
		leftKind, rightKind := okRank[p2[i].OperationKind], okRank[p2[j].OperationKind]
		if leftKind != rightKind {
			return leftKind < rightKind
		}
		leftLifecycle, leftCatalog := catalogLifecycleRank[p2[i].Name]
		rightLifecycle, rightCatalog := catalogLifecycleRank[p2[j].Name]
		if p2[i].Service == "DataBroker" && leftCatalog {
			if p2[j].Service == "DataBroker" && rightCatalog {
				return leftLifecycle < rightLifecycle
			}
			return true
		}
		if p2[j].Service == "DataBroker" && rightCatalog {
			return false
		}
		return false
	})
	// The tenant-wide revoke intentionally kills the benchmark administrator's
	// current session. Keep it after every disposable-principal teardown; the
	// harness re-authenticates once before the final self-PurgeTenant.
	sort.SliceStable(p3, func(i, j int) bool {
		return p3[i].Name != "AdminRevokeAllTenantSessions" && p3[j].Name == "AdminRevokeAllTenantSessions"
	})
	out := make([]RPCInfo, 0, len(all))
	out = append(out, p1...)
	out = append(out, p2...)
	out = append(out, p3...)
	out = append(out, final...)
	return out
}

func TestOrderRPCsReauthBoundaryFollowsTenantWideRevoke(t *testing.T) {
	ordered := orderRPCsByAuthPhase([]RPCInfo{
		{
			Service:    "TenantService",
			FullMethod: "/udb.core.tenant.services.v1.TenantService/PurgeTenant",
			Name:       "PurgeTenant",
		},
		{Service: "AuthnService", Name: "AdminRevokeAllTenantSessions"},
		{Service: "AuthnService", Name: "Logout"},
		{Service: "DataBroker", Name: "Select", OperationKind: "read_only"},
	})

	want := []string{"Select", "Logout", "AdminRevokeAllTenantSessions", "PurgeTenant"}
	if len(ordered) != len(want) {
		t.Fatalf("ordered RPC count = %d, want %d", len(ordered), len(want))
	}
	for i, name := range want {
		if ordered[i].Name != name {
			t.Fatalf("ordered RPC %d = %s, want %s", i, ordered[i].Name, name)
		}
	}
}

func TestOrderRPCsPinsCatalogLifecycle(t *testing.T) {
	ordered := orderRPCsByAuthPhase([]RPCInfo{
		{Service: "DataBroker", Name: "ActivateCatalog", OperationKind: "destructive"},
		{Service: "DataBroker", Name: "RollbackCatalog", OperationKind: "destructive"},
		{Service: "DataBroker", Name: "StageCatalog", OperationKind: "destructive"},
	})
	want := []string{"StageCatalog", "ActivateCatalog", "RollbackCatalog"}
	for i, name := range want {
		if ordered[i].Name != name {
			t.Fatalf("ordered catalog RPC %d = %s, want %s", i, ordered[i].Name, name)
		}
	}
}

// Dev test-mode sentinel: the broker (UDB_WEBAUTHN_TEST_MODE) mints and verifies
// a REAL credential/assertion when the harness sends this value.
const webauthnTestCredential = "__UDB_WEBAUTHN_TEST__"

// buildSpecBody builds the dynamicpb request for fullMethod from the shared
// strict-JSON bench-body manifest. Returns ok=false when the manifest cannot be
// hydrated; caller must NOT fall back to a generic request.
func buildSpecBody(fullMethod string, fix *perfFixtures) (proto.Message, proto.Message, bool) {
	if in, out, ok := buildManifestJSONBody(fullMethod, fix); ok {
		return in, out, true
	}
	return nil, nil, false
}

var benchBodyRowsForPerf = struct {
	once sync.Once
	rows map[string]string
	err  error
}{}

func loadBenchBodyRowsForPerf() (map[string]string, error) {
	benchBodyRowsForPerf.once.Do(func() {
		raw, err := os.ReadFile(filepath.FromSlash(benchBodiesJSONPath))
		if err != nil {
			benchBodyRowsForPerf.err = err
			return
		}
		var entries []benchBodyEntry
		if err := json.Unmarshal(raw, &entries); err != nil {
			benchBodyRowsForPerf.err = err
			return
		}
		rows := make(map[string]string, len(entries)*2)
		for _, entry := range entries {
			key := manifestRPCKey(entry.RPC)
			rows[key] = entry.Body
			if entry.Service != "" && entry.RPC != "" {
				rows[entry.Service+"."+entry.RPC] = entry.Body
			}
		}
		benchBodyRowsForPerf.rows = rows
	})
	return benchBodyRowsForPerf.rows, benchBodyRowsForPerf.err
}

func manifestBodyForFullMethod(fullMethod string) (string, bool) {
	rows, err := loadBenchBodyRowsForPerf()
	if err != nil {
		return "", false
	}
	rpc, ok := LookupRPC(fullMethod)
	if !ok {
		return "", false
	}
	dups := duplicateRPCNames()
	for _, key := range []string{
		rpcBenchKey(rpc.Service, rpc.Name, dups),
		rpc.Service + "." + rpc.Name,
		rpc.Name,
	} {
		if body, ok := rows[key]; ok {
			return body, true
		}
	}
	return "", false
}

func manifestJSONBodyCell(body string) (string, bool) {
	body = strings.TrimSpace(body)
	if strings.HasPrefix(body, "`") && strings.HasSuffix(body, "`") && len(body) >= 2 {
		body = strings.TrimSpace(body[1 : len(body)-1])
	}
	if !strings.HasPrefix(body, "{") || !strings.HasSuffix(body, "}") {
		return "", false
	}
	return body, true
}

func resolveManifestSeeds(body string, fix *perfFixtures) (string, bool) {
	const prefix = "<seed:"
	for {
		start := strings.Index(body, prefix)
		if start < 0 {
			return body, true
		}
		endRel := strings.Index(body[start:], ">")
		if endRel < 0 {
			return "", false
		}
		end := start + endRel
		key := strings.ToLower(strings.TrimSpace(body[start+len(prefix) : end]))
		value, ok := fix.lookupSeed(key)
		if !ok || value == "" {
			return "", false
		}
		body = body[:start] + value + body[end+1:]
	}
}

func buildManifestJSONBody(fullMethod string, fix *perfFixtures) (proto.Message, proto.Message, bool) {
	body, ok := manifestBodyForFullMethod(fullMethod)
	if !ok {
		return nil, nil, false
	}
	jsonBody, ok := manifestJSONBodyCell(body)
	if !ok {
		return nil, nil, false
	}
	jsonBody, ok = resolveManifestSeeds(jsonBody, fix)
	if !ok {
		return nil, nil, false
	}
	jsonBody = strings.ReplaceAll(jsonBody, `"timestamp": { "seconds": 1767225600, "nanos": 0 }`, `"timestamp": "2026-01-01T00:00:00Z"`)
	md := resolveMethodDesc(fullMethod)
	if md == nil {
		return nil, nil, false
	}
	in := dynamicpb.NewMessage(md.Input())
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal([]byte(jsonBody), in); err != nil {
		return nil, nil, false
	}
	return in, dynamicpb.NewMessage(md.Output()), true
}

func TestBuildManifestJSONBodyUsesSharedManifest(t *testing.T) {
	fix := newPerfFixtures()
	fix.set("tenant_id", "tenant-1")
	fix.set("stage_name", "stage-1")
	in, _, ok := buildManifestJSONBody("/udb.core.analytics.services.v1.AnalyticsService/GetPipelineSummary", fix)
	if !ok {
		t.Fatalf("manifest JSON body was not hydrated")
	}
	msg := in.ProtoReflect()
	fields := msg.Descriptor().Fields()
	if got := msg.Get(fields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("tenant_id = %q, want tenant-1", got)
	}
	if got := msg.Get(fields.ByName("stage_name")).String(); got != "stage-1" {
		t.Fatalf("stage_name = %q, want stage-1", got)
	}
	fix.set("file_id", "file-1")
	storageIn, _, ok := buildManifestJSONBody("/udb.core.storage.services.v1.StorageService/GetFile", fix)
	if !ok {
		t.Fatalf("StorageService manifest JSON body was not hydrated")
	}
	storageMsg := storageIn.ProtoReflect()
	storageFields := storageMsg.Descriptor().Fields()
	if got := storageMsg.Get(storageFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("storage tenant_id = %q, want tenant-1", got)
	}
	if got := storageMsg.Get(storageFields.ByName("file_id")).String(); got != "file-1" {
		t.Fatalf("storage file_id = %q, want file-1", got)
	}
	fix.set("tenant_code", "tenant-code-1")
	fix.set("purge_tenant_id", "tenant-purge-1")
	fix.set("admin_purge_tenant_id", "tenant-admin-purge-1")
	createTenantIn, _, ok := buildManifestJSONBody("/udb.core.tenant.services.v1.TenantService/CreateTenant", fix)
	if !ok {
		t.Fatalf("TenantService CreateTenant manifest JSON body was not hydrated")
	}
	createTenantMsg := createTenantIn.ProtoReflect()
	createTenantFields := createTenantMsg.Descriptor().Fields()
	if got := createTenantMsg.Get(createTenantFields.ByName("code")).String(); got != "tenant-code-1" {
		t.Fatalf("tenant create code = %q, want tenant-code-1", got)
	}
	if got := createTenantMsg.Get(createTenantFields.ByName("config")).String(); got != "{}" {
		t.Fatalf("tenant create config = %q, want {}", got)
	}
	tenantIn, _, ok := buildManifestJSONBody("/udb.core.tenant.services.v1.TenantService/GetTenant", fix)
	if !ok {
		t.Fatalf("TenantService GetTenant manifest JSON body was not hydrated")
	}
	tenantMsg := tenantIn.ProtoReflect()
	tenantFields := tenantMsg.Descriptor().Fields()
	if got := tenantMsg.Get(tenantFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("tenant get tenant_id = %q, want tenant-1", got)
	}
	updateTenantIn, _, ok := buildManifestJSONBody("/udb.core.tenant.services.v1.TenantService/UpdateTenant", fix)
	if !ok {
		t.Fatalf("TenantService UpdateTenant manifest JSON body was not hydrated")
	}
	updateTenantMsg := updateTenantIn.ProtoReflect()
	updateTenantFields := updateTenantMsg.Descriptor().Fields()
	if got := updateTenantMsg.Get(updateTenantFields.ByName("status")).String(); got != "active" {
		t.Fatalf("tenant update status = %q, want active", got)
	}
	updateTenantConfigIn, _, ok := buildManifestJSONBody("/udb.core.tenant.services.v1.TenantService/UpdateTenantConfig", fix)
	if !ok {
		t.Fatalf("TenantService UpdateTenantConfig manifest JSON body was not hydrated")
	}
	updateTenantConfigMsg := updateTenantConfigIn.ProtoReflect()
	updateTenantConfigFields := updateTenantConfigMsg.Descriptor().Fields()
	if got := updateTenantConfigMsg.Get(updateTenantConfigFields.ByName("config_key")).String(); got != "feature.flag" {
		t.Fatalf("tenant update config key = %q, want feature.flag", got)
	}
	purgeTenantIn, _, ok := buildManifestJSONBody("/udb.core.tenant.services.v1.TenantService/PurgeTenant", fix)
	if !ok {
		t.Fatalf("TenantService PurgeTenant manifest JSON body was not hydrated")
	}
	purgeTenantMsg := purgeTenantIn.ProtoReflect()
	purgeTenantFields := purgeTenantMsg.Descriptor().Fields()
	if got := purgeTenantMsg.Get(purgeTenantFields.ByName("tenant_id")).String(); got != "tenant-purge-1" {
		t.Fatalf("tenant purge tenant_id = %q, want tenant-purge-1", got)
	}
	if got := purgeTenantMsg.Get(purgeTenantFields.ByName("confirmation_token")).String(); got != "sdk-perf-confirm-purge" {
		t.Fatalf("tenant purge confirmation_token = %q, want sdk-perf-confirm-purge", got)
	}
	adminPurgeTenantIn, _, ok := buildManifestJSONBody("/udb.core.tenant.services.v1.TenantService/AdminPurgeTenant", fix)
	if !ok {
		t.Fatalf("TenantService AdminPurgeTenant manifest JSON body was not hydrated")
	}
	adminPurgeTenantMsg := adminPurgeTenantIn.ProtoReflect()
	adminPurgeTenantFields := adminPurgeTenantMsg.Descriptor().Fields()
	// The PRIVILEGED cross-tenant purge must target its OWN disposable tenant, never
	// `purge_tenant_id` (the caller's own tenant, which the terminal self-PurgeTenant
	// uses). Since 0.4.32 the tenant-status gate suspends the purged tenant, so
	// pointing this at the caller kills every later RPC in the run.
	if got := adminPurgeTenantMsg.Get(adminPurgeTenantFields.ByName("target_tenant_id")).String(); got != "tenant-admin-purge-1" {
		t.Fatalf("tenant admin purge target_tenant_id = %q, want tenant-admin-purge-1", got)
	}
	if got := adminPurgeTenantMsg.Get(adminPurgeTenantFields.ByName("target_tenant_id")).String(); got == "tenant-purge-1" {
		t.Fatalf("admin purge must NOT target the caller's own purge tenant")
	}
	// confirmation_token MUST equal target_tenant_id (fail-closed cross-tenant guard).
	if got := adminPurgeTenantMsg.Get(adminPurgeTenantFields.ByName("confirmation_token")).String(); got != "tenant-admin-purge-1" {
		t.Fatalf("tenant admin purge confirmation_token = %q, want tenant-admin-purge-1", got)
	}
	// mode must be an explicit non-UNSPECIFIED enum (ADMIN_PURGE_MODE_UNSPECIFIED is rejected).
	if got := adminPurgeTenantMsg.Get(adminPurgeTenantFields.ByName("mode")).Enum(); got == 0 {
		t.Fatalf("tenant admin purge mode was not set from manifest enum")
	}
	fix.set("key_id", "key-1")
	fix.set("plain_key", "plain-1")
	fix.set("owner_id", "owner-1")
	fix.set("project", "project-1")
	fix.set("update_key_id", "update-key-1")
	fix.set("revoke_key_id", "revoke-key-1")
	createApiKeyIn, _, ok := buildManifestJSONBody("/udb.core.apikey.services.v1.ApiKeyService/CreateApiKey", fix)
	if !ok {
		t.Fatalf("ApiKeyService CreateApiKey manifest JSON body was not hydrated")
	}
	createApiKeyMsg := createApiKeyIn.ProtoReflect()
	createApiKeyFields := createApiKeyMsg.Descriptor().Fields()
	if got := createApiKeyMsg.Get(createApiKeyFields.ByName("owner_id")).String(); got != "owner-1" {
		t.Fatalf("apikey create owner_id = %q, want owner-1", got)
	}
	if got := createApiKeyMsg.Get(createApiKeyFields.ByName("scopes")).List().Len(); got != 1 {
		t.Fatalf("apikey create scopes len = %d, want 1", got)
	}
	createApiKeyCtx := createApiKeyMsg.Get(createApiKeyFields.ByName("context")).Message()
	createApiKeyCtxFields := createApiKeyCtx.Descriptor().Fields()
	createApiKeyTenant := createApiKeyCtx.Get(createApiKeyCtxFields.ByName("tenant")).Message()
	createApiKeyTenantFields := createApiKeyTenant.Descriptor().Fields()
	if got := createApiKeyTenant.Get(createApiKeyTenantFields.ByName("project_id")).String(); got != "project-1" {
		t.Fatalf("apikey create context project_id = %q, want project-1", got)
	}
	apiKeyIn, _, ok := buildManifestJSONBody("/udb.core.apikey.services.v1.ApiKeyService/ListApiKeys", fix)
	if !ok {
		t.Fatalf("ApiKeyService ListApiKeys manifest JSON body was not hydrated")
	}
	apiKeyMsg := apiKeyIn.ProtoReflect()
	apiKeyFields := apiKeyMsg.Descriptor().Fields()
	if got := apiKeyMsg.Get(apiKeyFields.ByName("owner_id")).String(); got != "owner-1" {
		t.Fatalf("apikey owner_id = %q, want owner-1", got)
	}
	if got := apiKeyMsg.Get(apiKeyFields.ByName("owner_type")).Enum(); got == 0 {
		t.Fatalf("apikey owner_type was not set from manifest enum")
	}
	if got := apiKeyMsg.Get(apiKeyFields.ByName("page")).Message().Get(apiKeyFields.ByName("page").Message().Fields().ByName("page_size")).Int(); got != 50 {
		t.Fatalf("apikey page_size = %d, want 50", got)
	}
	updateApiKeyIn, _, ok := buildManifestJSONBody("/udb.core.apikey.services.v1.ApiKeyService/UpdateApiKey", fix)
	if !ok {
		t.Fatalf("ApiKeyService UpdateApiKey manifest JSON body was not hydrated")
	}
	updateApiKeyMsg := updateApiKeyIn.ProtoReflect()
	updateApiKeyFields := updateApiKeyMsg.Descriptor().Fields()
	if got := updateApiKeyMsg.Get(updateApiKeyFields.ByName("key_id")).String(); got != "update-key-1" {
		t.Fatalf("apikey update key_id = %q, want update-key-1", got)
	}
	if got := updateApiKeyMsg.Get(updateApiKeyFields.ByName("scopes")).List().Len(); got != 1 {
		t.Fatalf("apikey update scopes len = %d, want 1", got)
	}
	revokeApiKeyIn, _, ok := buildManifestJSONBody("/udb.core.apikey.services.v1.ApiKeyService/RevokeApiKey", fix)
	if !ok {
		t.Fatalf("ApiKeyService RevokeApiKey manifest JSON body was not hydrated")
	}
	revokeApiKeyMsg := revokeApiKeyIn.ProtoReflect()
	revokeApiKeyFields := revokeApiKeyMsg.Descriptor().Fields()
	if got := revokeApiKeyMsg.Get(revokeApiKeyFields.ByName("key_id")).String(); got != "revoke-key-1" {
		t.Fatalf("apikey revoke key_id = %q, want revoke-key-1", got)
	}
	rotateApiKeyIn, _, ok := buildManifestJSONBody("/udb.core.apikey.services.v1.ApiKeyService/RotateApiKey", fix)
	if !ok {
		t.Fatalf("ApiKeyService RotateApiKey manifest JSON body was not hydrated")
	}
	rotateApiKeyMsg := rotateApiKeyIn.ProtoReflect()
	rotateApiKeyFields := rotateApiKeyMsg.Descriptor().Fields()
	if got := rotateApiKeyMsg.Get(rotateApiKeyFields.ByName("rotation_reason")).String(); got != "bench rotate" {
		t.Fatalf("apikey rotate reason = %q, want bench rotate", got)
	}
	emergencyApiKeyIn, _, ok := buildManifestJSONBody("/udb.core.apikey.services.v1.ApiKeyService/EmergencyRevokeApiKeys", fix)
	if !ok {
		t.Fatalf("ApiKeyService EmergencyRevokeApiKeys manifest JSON body was not hydrated")
	}
	emergencyApiKeyMsg := emergencyApiKeyIn.ProtoReflect()
	emergencyApiKeyFields := emergencyApiKeyMsg.Descriptor().Fields()
	if got := emergencyApiKeyMsg.Get(emergencyApiKeyFields.ByName("scope")).String(); got != "resource:read" {
		t.Fatalf("apikey emergency scope = %q, want resource:read", got)
	}
	fix.set("user_id", "user-1")
	fix.set("session_id", "session-1")
	fix.set("token", "token-1")
	fix.set("csrf_token", "csrf-1")
	fix.set("otp_id", "otp-1")
	fix.set("otp_code", "654321")
	fix.set("challenge_id", "challenge-1")
	fix.set("username", "bench-user")
	fix.set("refresh_token", "refresh-1")
	fix.set("refresh_session_id", "refresh-session-1")
	fix.set("subject", "subject-1")
	authnIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/ListSessions", fix)
	if !ok {
		t.Fatalf("AuthnService manifest JSON body was not hydrated")
	}
	authnMsg := authnIn.ProtoReflect()
	authnFields := authnMsg.Descriptor().Fields()
	if got := authnMsg.Get(authnFields.ByName("user_id")).String(); got != "user-1" {
		t.Fatalf("authn user_id = %q, want user-1", got)
	}
	if got := authnMsg.Get(authnFields.ByName("active_only")).Bool(); !got {
		t.Fatalf("authn active_only was not set from manifest")
	}
	if got := authnMsg.Get(authnFields.ByName("page")).Message().Get(authnFields.ByName("page").Message().Fields().ByName("page_size")).Int(); got != 20 {
		t.Fatalf("authn page_size = %d, want 20", got)
	}
	csrfIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/ValidateCSRF", fix)
	if !ok {
		t.Fatalf("AuthnService ValidateCSRF manifest JSON body was not hydrated")
	}
	csrfMsg := csrfIn.ProtoReflect()
	csrfFields := csrfMsg.Descriptor().Fields()
	if got := csrfMsg.Get(csrfFields.ByName("csrf_token")).String(); got != "csrf-1" {
		t.Fatalf("authn csrf_token = %q, want csrf-1", got)
	}
	otpIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/VerifyOTP", fix)
	if !ok {
		t.Fatalf("AuthnService VerifyOTP manifest JSON body was not hydrated")
	}
	otpMsg := otpIn.ProtoReflect()
	otpFields := otpMsg.Descriptor().Fields()
	if got := otpMsg.Get(otpFields.ByName("otp_id")).String(); got != "otp-1" {
		t.Fatalf("authn otp_id = %q, want otp-1", got)
	}
	if got := otpMsg.Get(otpFields.ByName("code")).String(); got != "654321" {
		t.Fatalf("authn otp code = %q, want 654321", got)
	}
	mfaIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/VerifyMfaChallenge", fix)
	if !ok {
		t.Fatalf("AuthnService VerifyMfaChallenge manifest JSON body was not hydrated")
	}
	mfaMsg := mfaIn.ProtoReflect()
	mfaFields := mfaMsg.Descriptor().Fields()
	if got := mfaMsg.Get(mfaFields.ByName("challenge_id")).String(); got != "challenge-1" {
		t.Fatalf("authn challenge_id = %q, want challenge-1", got)
	}
	loginIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/Login", fix)
	if !ok {
		t.Fatalf("AuthnService Login manifest JSON body was not hydrated")
	}
	loginMsg := loginIn.ProtoReflect()
	loginFields := loginMsg.Descriptor().Fields()
	if got := loginMsg.Get(loginFields.ByName("username")).String(); got != "bench-user" {
		t.Fatalf("authn login username = %q, want bench-user", got)
	}
	if got := loginMsg.Get(loginFields.ByName("password")).String(); got != "CorrectHorse1!" {
		t.Fatalf("authn login password = %q, want CorrectHorse1!", got)
	}
	if got := loginMsg.Get(loginFields.ByName("device_type")).Enum(); got == 0 {
		t.Fatalf("authn login device_type was not set from manifest enum")
	}
	if got := loginMsg.Get(loginFields.ByName("project_hint")).String(); got != "project-1" {
		t.Fatalf("authn login project_hint = %q, want project-1", got)
	}
	// fix_plan Phases 2+3: typed service-account grant + mTLS certificate
	// binding management RPCs — every one carries an explicit manifest body
	// (no generic fill). Seeds for the grant id family are set above.
	fix.set("grant_binding_id", "11111111-1111-4111-8111-000000000201")
	fix.set("grant_create_user_id", "11111111-1111-4111-8111-000000000202")
	fix.set("grant_transfer_to_user_id", "11111111-1111-4111-8111-000000000203")
	// The transfer SOURCE is its own service account: the api-key owner's grant
	// revision moves under the measured api-key RPCs and breaks the expected_revision CAS.
	fix.set("grant_transfer_from_user_id", "11111111-1111-4111-8111-000000000204")
	for _, rpc := range []string{
		"AuthnService/CreateServiceAccountGrant",
		"AuthnService/GetServiceAccountGrant",
		"AuthnService/ListServiceAccountGrants",
		"AuthnService/ReplaceServiceAccountGrant",
		"AuthnService/RotateServiceAccountIdentity",
		"AuthnService/TransferServiceAccountGrant",
		"AuthnService/RevokeServiceAccountGrant",
		"AuthnService/CreateCertificateBinding",
		"AuthnService/ListCertificateBindings",
		"AuthnService/RevokeCertificateBinding",
	} {
		if _, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1."+rpc, fix); !ok {
			t.Fatalf("%s manifest JSON body was not hydrated", rpc)
		}
	}
	refreshTokenIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/RefreshToken", fix)
	if !ok {
		t.Fatalf("AuthnService RefreshToken manifest JSON body was not hydrated")
	}
	refreshTokenMsg := refreshTokenIn.ProtoReflect()
	refreshTokenFields := refreshTokenMsg.Descriptor().Fields()
	if got := refreshTokenMsg.Get(refreshTokenFields.ByName("refresh_token")).String(); got != "refresh-1" {
		t.Fatalf("authn refresh_token = %q, want refresh-1", got)
	}
	refreshSessionIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/RefreshSession", fix)
	if !ok {
		t.Fatalf("AuthnService RefreshSession manifest JSON body was not hydrated")
	}
	refreshSessionMsg := refreshSessionIn.ProtoReflect()
	refreshSessionFields := refreshSessionMsg.Descriptor().Fields()
	if got := refreshSessionMsg.Get(refreshSessionFields.ByName("session_id")).String(); got != "refresh-session-1" {
		t.Fatalf("authn refresh session_id = %q, want refresh-session-1", got)
	}
	if got := refreshSessionMsg.Get(refreshSessionFields.ByName("ttl_seconds")).Int(); got != 3600 {
		t.Fatalf("authn refresh ttl_seconds = %d, want 3600", got)
	}
	createSessionIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/CreateSession", fix)
	if !ok {
		t.Fatalf("AuthnService CreateSession manifest JSON body was not hydrated")
	}
	createSessionMsg := createSessionIn.ProtoReflect()
	createSessionFields := createSessionMsg.Descriptor().Fields()
	createSessionPrincipal := createSessionMsg.Get(createSessionFields.ByName("principal")).Message()
	createSessionPrincipalFields := createSessionPrincipal.Descriptor().Fields()
	if got := createSessionPrincipal.Get(createSessionPrincipalFields.ByName("subject")).String(); got != "subject-1" {
		t.Fatalf("authn create session subject = %q, want subject-1", got)
	}
	if got := createSessionMsg.Get(createSessionFields.ByName("ttl_seconds")).Int(); got != 3600 {
		t.Fatalf("authn create session ttl_seconds = %d, want 3600", got)
	}
	createUserIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/CreateUser", fix)
	if !ok {
		t.Fatalf("AuthnService CreateUser manifest JSON body was not hydrated")
	}
	createUserMsg := createUserIn.ProtoReflect()
	createUserFields := createUserMsg.Descriptor().Fields()
	if got := createUserMsg.Get(createUserFields.ByName("username")).String(); got != "perf-u" {
		t.Fatalf("authn create user username = %q, want perf-u", got)
	}
	if got := createUserMsg.Get(createUserFields.ByName("account_kind")).Enum(); got == 0 {
		t.Fatalf("authn create user account_kind was not set from manifest enum")
	}
	updateUserIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/UpdateUser", fix)
	if !ok {
		t.Fatalf("AuthnService UpdateUser manifest JSON body was not hydrated")
	}
	updateUserMsg := updateUserIn.ProtoReflect()
	updateUserFields := updateUserMsg.Descriptor().Fields()
	if got := updateUserMsg.Get(updateUserFields.ByName("full_name")).String(); got != "Perf U2" {
		t.Fatalf("authn update user full_name = %q, want Perf U2", got)
	}
	sendOTPIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/SendOTP", fix)
	if !ok {
		t.Fatalf("AuthnService SendOTP manifest JSON body was not hydrated")
	}
	sendOTPMsg := sendOTPIn.ProtoReflect()
	sendOTPFields := sendOTPMsg.Descriptor().Fields()
	if got := sendOTPMsg.Get(sendOTPFields.ByName("otp_type")).Enum(); got == 0 {
		t.Fatalf("authn send otp_type was not set from manifest enum")
	}
	resendOTPIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/ResendOTP", fix)
	if !ok {
		t.Fatalf("AuthnService ResendOTP manifest JSON body was not hydrated")
	}
	resendOTPMsg := resendOTPIn.ProtoReflect()
	resendOTPFields := resendOTPMsg.Descriptor().Fields()
	if got := resendOTPMsg.Get(resendOTPFields.ByName("original_otp_id")).String(); got != "otp-1" {
		t.Fatalf("authn resend original_otp_id = %q, want otp-1", got)
	}
	enrollIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/EnrollMFA", fix)
	if !ok {
		t.Fatalf("AuthnService EnrollMFA manifest JSON body was not hydrated")
	}
	enrollMsg := enrollIn.ProtoReflect()
	enrollFields := enrollMsg.Descriptor().Fields()
	if got := enrollMsg.Get(enrollFields.ByName("mfa_type")).Enum(); got == 0 {
		t.Fatalf("authn enroll mfa_type was not set from manifest enum")
	}
	recoveryIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/GenerateRecoveryCodes", fix)
	if !ok {
		t.Fatalf("AuthnService GenerateRecoveryCodes manifest JSON body was not hydrated")
	}
	recoveryMsg := recoveryIn.ProtoReflect()
	recoveryFields := recoveryMsg.Descriptor().Fields()
	if got := recoveryMsg.Get(recoveryFields.ByName("count")).Int(); got != 10 {
		t.Fatalf("authn recovery count = %d, want 10", got)
	}
	authnPolicyIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/PutMfaPolicy", fix)
	if !ok {
		t.Fatalf("AuthnService PutMfaPolicy manifest JSON body was not hydrated")
	}
	authnPolicyMsg := authnPolicyIn.ProtoReflect()
	authnPolicyFields := authnPolicyMsg.Descriptor().Fields()
	if got := authnPolicyMsg.Get(authnPolicyFields.ByName("require_mfa")).Bool(); got {
		t.Fatalf("authn require_mfa = true, want false")
	}
	forgotIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/ForgotPassword", fix)
	if !ok {
		t.Fatalf("AuthnService ForgotPassword manifest JSON body was not hydrated")
	}
	forgotMsg := forgotIn.ProtoReflect()
	forgotFields := forgotMsg.Descriptor().Fields()
	if got := forgotMsg.Get(forgotFields.ByName("identifier")).String(); got != "perf-u@acme.test" {
		t.Fatalf("authn forgot identifier = %q, want perf-u@acme.test", got)
	}
	phoneIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/SendPhoneVerification", fix)
	if !ok {
		t.Fatalf("AuthnService SendPhoneVerification manifest JSON body was not hydrated")
	}
	phoneMsg := phoneIn.ProtoReflect()
	phoneFields := phoneMsg.Descriptor().Fields()
	if got := phoneMsg.Get(phoneFields.ByName("phone")).String(); got != "+15551234567" {
		t.Fatalf("authn phone = %q, want +15551234567", got)
	}
	issueMFAIn, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/IssueMfaChallenge", fix)
	if !ok {
		t.Fatalf("AuthnService IssueMfaChallenge manifest JSON body was not hydrated")
	}
	issueMFAMsg := issueMFAIn.ProtoReflect()
	issueMFAFields := issueMFAMsg.Descriptor().Fields()
	if got := issueMFAMsg.Get(issueMFAFields.ByName("purpose")).Enum(); got == 0 {
		t.Fatalf("authn issue mfa purpose was not set from manifest enum")
	}
	fix.set("code", "code-1")
	fix.set("reset_otp_id", "reset-otp-1")
	fix.set("reset_otp_code", "135790")
	fix.set("device_id", "device-1")
	fix.set("reg_challenge_id", "reg-challenge-1")
	fix.set("auth_challenge_id", "auth-challenge-1")
	fix.set("record_id", "credential-1")
	fix.set("admin_reset_mfa_user_id", "admin-reset-mfa-user-1")
	fix.set("admin_reset_password_user_id", "admin-reset-password-user-1")
	fix.set("change_password_user_id", "change-password-user-1")
	fix.set("change_status_user_id", "change-status-user-1")
	fix.set("disable_mfa_user_id", "disable-mfa-user-1")
	fix.set("revoke_device_id", "revoke-device-1")
	fix.set("revoke_recovery_user_id", "revoke-recovery-user-1")
	authnManifest := func(method string) (protoreflect.Message, protoreflect.FieldDescriptors) {
		t.Helper()
		in, _, ok := buildManifestJSONBody("/udb.core.authn.services.v1.AuthnService/"+method, fix)
		if !ok {
			t.Fatalf("AuthnService %s manifest JSON body was not hydrated", method)
		}
		msg := in.ProtoReflect()
		return msg, msg.Descriptor().Fields()
	}
	logoutMsg, logoutFields := authnManifest("Logout")
	if got := logoutMsg.Get(logoutFields.ByName("session_id")).String(); got != "session-1" {
		t.Fatalf("authn logout session_id = %q, want session-1", got)
	}
	revokeSessionMsg, revokeSessionFields := authnManifest("RevokeSession")
	if got := revokeSessionMsg.Get(revokeSessionFields.ByName("revoke_reason")).String(); got != "perf" {
		t.Fatalf("authn revoke session reason = %q, want perf", got)
	}
	adminRevokeMsg, adminRevokeFields := authnManifest("AdminRevokeSession")
	if got := adminRevokeMsg.Get(adminRevokeFields.ByName("reason")).String(); got != "perf" {
		t.Fatalf("authn admin revoke reason = %q, want perf", got)
	}
	adminAllUserMsg, adminAllUserFields := authnManifest("AdminRevokeAllUserSessions")
	if got := adminAllUserMsg.Get(adminAllUserFields.ByName("user_id")).String(); got != "user-1" {
		t.Fatalf("authn admin all user_id = %q, want user-1", got)
	}
	adminAllTenantMsg, adminAllTenantFields := authnManifest("AdminRevokeAllTenantSessions")
	if got := adminAllTenantMsg.Get(adminAllTenantFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("authn admin all tenant_id = %q, want tenant-1", got)
	}
	emergencyMsg, emergencyFields := authnManifest("EmergencyRevoke")
	if got := emergencyMsg.Get(emergencyFields.ByName("principal_id")).String(); got != "subject-1" {
		t.Fatalf("authn emergency principal_id = %q, want subject-1", got)
	}
	changePasswordMsg, changePasswordFields := authnManifest("ChangePassword")
	if got := changePasswordMsg.Get(changePasswordFields.ByName("user_id")).String(); got != "change-password-user-1" {
		t.Fatalf("authn change password user_id = %q, want change-password-user-1", got)
	}
	if got := changePasswordMsg.Get(changePasswordFields.ByName("current_password")).String(); got != "CorrectHorse1!" {
		t.Fatalf("authn change password current_password = %q, want CorrectHorse1!", got)
	}
	if got := changePasswordMsg.Get(changePasswordFields.ByName("otp_id")).String(); got != "" {
		t.Fatalf("authn change password otp_id = %q, want empty", got)
	}
	resetPasswordMsg, resetPasswordFields := authnManifest("ResetPassword")
	if got := resetPasswordMsg.Get(resetPasswordFields.ByName("code")).String(); got != "135790" {
		t.Fatalf("authn reset code = %q, want 135790", got)
	}
	statusMsg, statusFields := authnManifest("ChangeUserStatus")
	if got := statusMsg.Get(statusFields.ByName("user_id")).String(); got != "change-status-user-1" {
		t.Fatalf("authn change status user_id = %q, want change-status-user-1", got)
	}
	if got := statusMsg.Get(statusFields.ByName("new_status")).Enum(); got == 0 {
		t.Fatalf("authn change status enum was not set from manifest")
	}
	adminResetPasswordMsg, adminResetPasswordFields := authnManifest("AdminResetPassword")
	if got := adminResetPasswordMsg.Get(adminResetPasswordFields.ByName("user_id")).String(); got != "admin-reset-password-user-1" {
		t.Fatalf("authn admin reset password user_id = %q, want admin-reset-password-user-1", got)
	}
	confirmMFAMsg, confirmMFAFields := authnManifest("ConfirmMFAEnrollment")
	if got := confirmMFAMsg.Get(confirmMFAFields.ByName("otp_id")).String(); got != "code-1" {
		t.Fatalf("authn confirm mfa otp_id = %q, want code-1", got)
	}
	disableMFAMsg, disableMFAFields := authnManifest("DisableMfaFactor")
	if got := disableMFAMsg.Get(disableMFAFields.ByName("user_id")).String(); got != "disable-mfa-user-1" {
		t.Fatalf("authn disable mfa user_id = %q, want disable-mfa-user-1", got)
	}
	if got := disableMFAMsg.Get(disableMFAFields.ByName("factor_kind")).Enum(); got == 0 {
		t.Fatalf("authn disable mfa factor_kind was not set from manifest")
	}
	renameMsg, renameFields := authnManifest("RenamePasskey")
	if got := renameMsg.Get(renameFields.ByName("new_label")).String(); got != "perf-key2" {
		t.Fatalf("authn rename passkey label = %q, want perf-key2", got)
	}
	revokeRecoveryMsg, revokeRecoveryFields := authnManifest("RevokeRecoveryCodes")
	if got := revokeRecoveryMsg.Get(revokeRecoveryFields.ByName("user_id")).String(); got != "revoke-recovery-user-1" {
		t.Fatalf("authn revoke recovery user_id = %q, want revoke-recovery-user-1", got)
	}
	adminResetMFAMsg, adminResetMFAFields := authnManifest("AdminResetMfa")
	if got := adminResetMFAMsg.Get(adminResetMFAFields.ByName("user_id")).String(); got != "admin-reset-mfa-user-1" {
		t.Fatalf("authn admin reset mfa user_id = %q, want admin-reset-mfa-user-1", got)
	}
	if got := adminResetMFAMsg.Get(adminResetMFAFields.ByName("reason")).String(); got != "perf" {
		t.Fatalf("authn admin reset mfa reason = %q, want perf", got)
	}
	revokeDeviceMsg, revokeDeviceFields := authnManifest("RevokeDevice")
	if got := revokeDeviceMsg.Get(revokeDeviceFields.ByName("device_id")).String(); got != "revoke-device-1" {
		t.Fatalf("authn revoke device_id = %q, want revoke-device-1", got)
	}
	deleteWebAuthnMsg, deleteWebAuthnFields := authnManifest("DeleteWebAuthnCredential")
	if got := deleteWebAuthnMsg.Get(deleteWebAuthnFields.ByName("credential_id")).String(); got != "credential-1" {
		t.Fatalf("authn delete webauthn credential_id = %q, want credential-1", got)
	}
	startRegMsg, startRegFields := authnManifest("StartWebAuthnRegistration")
	if got := startRegMsg.Get(startRegFields.ByName("label")).String(); got != "perf-key" {
		t.Fatalf("authn start webauthn registration label = %q, want perf-key", got)
	}
	finishRegMsg, finishRegFields := authnManifest("FinishWebAuthnRegistration")
	if got := finishRegMsg.Get(finishRegFields.ByName("challenge_id")).String(); got != "reg-challenge-1" {
		t.Fatalf("authn finish webauthn registration challenge_id = %q, want reg-challenge-1", got)
	}
	if got := finishRegMsg.Get(finishRegFields.ByName("public_key_credential_json")).String(); got != webauthnTestCredential {
		t.Fatalf("authn finish webauthn registration credential = %q, want test sentinel", got)
	}
	startAuthMsg, startAuthFields := authnManifest("StartWebAuthnAuthentication")
	if got := startAuthMsg.Get(startAuthFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("authn start webauthn auth tenant_id = %q, want tenant-1", got)
	}
	finishAuthMsg, finishAuthFields := authnManifest("FinishWebAuthnAuthentication")
	if got := finishAuthMsg.Get(finishAuthFields.ByName("challenge_id")).String(); got != "auth-challenge-1" {
		t.Fatalf("authn finish webauthn auth challenge_id = %q, want auth-challenge-1", got)
	}
	fix.set("provider_id", "provider-1")
	idpIn, _, ok := buildManifestJSONBody("/udb.core.idp.services.v1.IdentityProviderService/ListProviders", fix)
	if !ok {
		t.Fatalf("IdentityProviderService manifest JSON body was not hydrated")
	}
	idpMsg := idpIn.ProtoReflect()
	idpFields := idpMsg.Descriptor().Fields()
	if got := idpMsg.Get(idpFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("idp tenant_id = %q, want tenant-1", got)
	}
	if got := idpMsg.Get(idpFields.ByName("page")).Message().Get(idpFields.ByName("page").Message().Fields().ByName("page_size")).Int(); got != 20 {
		t.Fatalf("idp page_size = %d, want 20", got)
	}
	fix.set("asset_id", "asset-1")
	fix.set("definition_id", "definition-1")
	fix.set("file_id", "file-1")
	fix.set("instance_id", "instance-1")
	fix.set("project", "project-1")
	fix.set("step_id", "step-1")
	assetIn, _, ok := buildManifestJSONBody("/udb.core.asset.services.v1.AssetService/ListAssets", fix)
	if !ok {
		t.Fatalf("AssetService manifest JSON body was not hydrated")
	}
	assetMsg := assetIn.ProtoReflect()
	assetFields := assetMsg.Descriptor().Fields()
	if got := assetMsg.Get(assetFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("asset tenant_id = %q, want tenant-1", got)
	}
	if got := assetMsg.Get(assetFields.ByName("media_type")).String(); got != "image/png" {
		t.Fatalf("asset media_type = %q, want image/png", got)
	}
	if got := assetMsg.Get(assetFields.ByName("page_size")).Int(); got != 20 {
		t.Fatalf("asset page_size = %d, want 20", got)
	}
	createAssetPipelineIn, _, ok := buildManifestJSONBody("/udb.core.asset.services.v1.AssetService/CreatePipelineDefinition", fix)
	if !ok {
		t.Fatalf("AssetService CreatePipelineDefinition manifest JSON body was not hydrated")
	}
	createAssetPipelineMsg := createAssetPipelineIn.ProtoReflect()
	createAssetPipelineFields := createAssetPipelineMsg.Descriptor().Fields()
	if got := createAssetPipelineMsg.Get(createAssetPipelineFields.ByName("steps")).String(); got != `[{"name":"resize","type":"TRANSFORM"}]` {
		t.Fatalf("asset create_pipeline_definition steps = %q, want resize transform JSON", got)
	}
	if got := createAssetPipelineMsg.Get(createAssetPipelineFields.ByName("version")).Int(); got != 1 {
		t.Fatalf("asset create_pipeline_definition version = %d, want 1", got)
	}
	registerAssetIn, _, ok := buildManifestJSONBody("/udb.core.asset.services.v1.AssetService/RegisterAsset", fix)
	if !ok {
		t.Fatalf("AssetService RegisterAsset manifest JSON body was not hydrated")
	}
	registerAssetMsg := registerAssetIn.ProtoReflect()
	registerAssetFields := registerAssetMsg.Descriptor().Fields()
	if got := registerAssetMsg.Get(registerAssetFields.ByName("project_id")).String(); got != "" {
		t.Fatalf("asset register project_id = %q, want empty project_id", got)
	}
	if got := registerAssetMsg.Get(registerAssetFields.ByName("file_id")).String(); got != "file-1" {
		t.Fatalf("asset register file_id = %q, want file-1", got)
	}
	if got := registerAssetMsg.Get(registerAssetFields.ByName("metadata")).String(); got != `{"source":"upload"}` {
		t.Fatalf("asset register metadata = %q, want source upload JSON", got)
	}
	startAssetPipelineIn, _, ok := buildManifestJSONBody("/udb.core.asset.services.v1.AssetService/StartPipeline", fix)
	if !ok {
		t.Fatalf("AssetService StartPipeline manifest JSON body was not hydrated")
	}
	startAssetPipelineMsg := startAssetPipelineIn.ProtoReflect()
	startAssetPipelineFields := startAssetPipelineMsg.Descriptor().Fields()
	if got := startAssetPipelineMsg.Get(startAssetPipelineFields.ByName("definition_id")).String(); got != "definition-1" {
		t.Fatalf("asset start definition_id = %q, want definition-1", got)
	}
	if got := startAssetPipelineMsg.Get(startAssetPipelineFields.ByName("asset_id")).String(); got != "asset-1" {
		t.Fatalf("asset start asset_id = %q, want asset-1", got)
	}
	if got := startAssetPipelineMsg.Get(startAssetPipelineFields.ByName("correlation_id")).String(); got != "run-001" {
		t.Fatalf("asset start correlation_id = %q, want run-001", got)
	}
	completeAssetStepIn, _, ok := buildManifestJSONBody("/udb.core.asset.services.v1.AssetService/CompleteStep", fix)
	if !ok {
		t.Fatalf("AssetService CompleteStep manifest JSON body was not hydrated")
	}
	completeAssetStepMsg := completeAssetStepIn.ProtoReflect()
	completeAssetStepFields := completeAssetStepMsg.Descriptor().Fields()
	if got := completeAssetStepMsg.Get(completeAssetStepFields.ByName("step_id")).String(); got != "step-1" {
		t.Fatalf("asset complete step_id = %q, want step-1", got)
	}
	if got := completeAssetStepMsg.Get(completeAssetStepFields.ByName("status")).String(); got != "COMPLETED" {
		t.Fatalf("asset complete status = %q, want COMPLETED", got)
	}
	fix.set("room_id", "room-1")
	fix.set("close_room_id", "close-room-1")
	fix.set("peer_id", "peer-1")
	fix.set("join_session_room_id", "join-session-room-1")
	fix.set("leave_peer_id", "leave-peer-1")
	fix.set("signal_peer_id", "signal-peer-1")
	fix.set("track_id", "track-1")
	fix.set("unpublish_track_id", "track-disposable-1")
	fix.set("egress_id", "egress-1")
	fix.set("object_key", "object-1")
	webrtcIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.TrackService/ListTracks", fix)
	if !ok {
		t.Fatalf("WebRTC manifest JSON body was not hydrated")
	}
	webrtcMsg := webrtcIn.ProtoReflect()
	webrtcFields := webrtcMsg.Descriptor().Fields()
	if got := webrtcMsg.Get(webrtcFields.ByName("room_id")).String(); got != "room-1" {
		t.Fatalf("webrtc room_id = %q, want room-1", got)
	}
	if got := webrtcMsg.Get(webrtcFields.ByName("peer_id")).String(); got != "peer-1" {
		t.Fatalf("webrtc peer_id = %q, want peer-1", got)
	}
	if got := webrtcMsg.Get(webrtcFields.ByName("page_size")).Int(); got != 20 {
		t.Fatalf("webrtc page_size = %d, want 20", got)
	}
	turnIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.TurnService/IssueCredentials", fix)
	if !ok {
		t.Fatalf("TurnService manifest JSON body was not hydrated")
	}
	turnMsg := turnIn.ProtoReflect()
	turnFields := turnMsg.Descriptor().Fields()
	if got := turnMsg.Get(turnFields.ByName("ttl_seconds")).Int(); got != 3600 {
		t.Fatalf("turn ttl_seconds = %d, want 3600", got)
	}
	signalIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.SignalingService/Signal", fix)
	if !ok {
		t.Fatalf("SignalingService manifest JSON body was not hydrated")
	}
	signalMsg := signalIn.ProtoReflect()
	signalFields := signalMsg.Descriptor().Fields()
	if got := signalMsg.Get(signalFields.ByName("peer_id")).String(); got != "signal-peer-1" {
		t.Fatalf("signal peer_id = %q, want signal-peer-1", got)
	}
	if got := signalMsg.Get(signalFields.ByName("ping")).Bool(); !got {
		t.Fatalf("signal ping was not set from manifest")
	}
	createRoomIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.RoomService/CreateRoom", fix)
	if !ok {
		t.Fatalf("RoomService CreateRoom manifest JSON body was not hydrated")
	}
	createRoomMsg := createRoomIn.ProtoReflect()
	createRoomFields := createRoomMsg.Descriptor().Fields()
	if got := createRoomMsg.Get(createRoomFields.ByName("created_by")).String(); got != "user-1" {
		t.Fatalf("room create created_by = %q, want user-1", got)
	}
	if got := createRoomMsg.Get(createRoomFields.ByName("max_participants")).Int(); got != 10 {
		t.Fatalf("room create max_participants = %d, want 10", got)
	}
	updateRoomIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.RoomService/UpdateRoom", fix)
	if !ok {
		t.Fatalf("RoomService UpdateRoom manifest JSON body was not hydrated")
	}
	updateRoomMsg := updateRoomIn.ProtoReflect()
	updateRoomFields := updateRoomMsg.Descriptor().Fields()
	if got := updateRoomMsg.Get(updateRoomFields.ByName("name")).String(); got != "bench-room-2" {
		t.Fatalf("room update name = %q, want bench-room-2", got)
	}
	if got := updateRoomMsg.Get(updateRoomFields.ByName("state")).String(); got != "active" {
		t.Fatalf("room update state = %q, want active", got)
	}
	closeRoomIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.RoomService/CloseRoom", fix)
	if !ok {
		t.Fatalf("RoomService CloseRoom manifest JSON body was not hydrated")
	}
	closeRoomMsg := closeRoomIn.ProtoReflect()
	closeRoomFields := closeRoomMsg.Descriptor().Fields()
	if got := closeRoomMsg.Get(closeRoomFields.ByName("room_id")).String(); got != "close-room-1" {
		t.Fatalf("room close room_id = %q, want close-room-1", got)
	}
	startCompositeIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.RoomService/StartRoomComposite", fix)
	if !ok {
		t.Fatalf("RoomService StartRoomComposite manifest JSON body was not hydrated")
	}
	startCompositeMsg := startCompositeIn.ProtoReflect()
	startCompositeFields := startCompositeMsg.Descriptor().Fields()
	if got := startCompositeMsg.Get(startCompositeFields.ByName("destination")).String(); got != "object-1" {
		t.Fatalf("room composite destination = %q, want object-1", got)
	}
	startTrackEgressIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.RoomService/StartTrackEgress", fix)
	if !ok {
		t.Fatalf("RoomService StartTrackEgress manifest JSON body was not hydrated")
	}
	startTrackEgressMsg := startTrackEgressIn.ProtoReflect()
	startTrackEgressFields := startTrackEgressMsg.Descriptor().Fields()
	if got := startTrackEgressMsg.Get(startTrackEgressFields.ByName("track_id")).String(); got != "track-1" {
		t.Fatalf("room track egress track_id = %q, want track-1", got)
	}
	stopEgressIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.RoomService/StopEgress", fix)
	if !ok {
		t.Fatalf("RoomService StopEgress manifest JSON body was not hydrated")
	}
	stopEgressMsg := stopEgressIn.ProtoReflect()
	stopEgressFields := stopEgressMsg.Descriptor().Fields()
	if got := stopEgressMsg.Get(stopEgressFields.ByName("egress_id")).String(); got != "egress-1" {
		t.Fatalf("room stop egress_id = %q, want egress-1", got)
	}
	joinRoomIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.PeerService/JoinRoom", fix)
	if !ok {
		t.Fatalf("PeerService JoinRoom manifest JSON body was not hydrated")
	}
	joinRoomMsg := joinRoomIn.ProtoReflect()
	joinRoomFields := joinRoomMsg.Descriptor().Fields()
	if got := joinRoomMsg.Get(joinRoomFields.ByName("display_name")).String(); got != "Bench User" {
		t.Fatalf("peer join_room display_name = %q, want Bench User", got)
	}
	if got := joinRoomMsg.Get(joinRoomFields.ByName("metadata")).String(); got != "{}" {
		t.Fatalf("peer join_room metadata = %q, want {}", got)
	}
	joinSessionIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.PeerService/JoinSession", fix)
	if !ok {
		t.Fatalf("PeerService JoinSession manifest JSON body was not hydrated")
	}
	joinSessionMsg := joinSessionIn.ProtoReflect()
	joinSessionFields := joinSessionMsg.Descriptor().Fields()
	if got := joinSessionMsg.Get(joinSessionFields.ByName("room_id")).String(); got != "join-session-room-1" {
		t.Fatalf("peer join_session room_id = %q, want join-session-room-1", got)
	}
	if got := joinSessionMsg.Get(joinSessionFields.ByName("ttl_seconds")).Int(); got != 3600 {
		t.Fatalf("peer join_session ttl_seconds = %d, want 3600", got)
	}
	leaveRoomIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.PeerService/LeaveRoom", fix)
	if !ok {
		t.Fatalf("PeerService LeaveRoom manifest JSON body was not hydrated")
	}
	leaveRoomMsg := leaveRoomIn.ProtoReflect()
	leaveRoomFields := leaveRoomMsg.Descriptor().Fields()
	if got := leaveRoomMsg.Get(leaveRoomFields.ByName("peer_id")).String(); got != "leave-peer-1" {
		t.Fatalf("peer leave_room peer_id = %q, want leave-peer-1", got)
	}
	publishTrackIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.TrackService/PublishTrack", fix)
	if !ok {
		t.Fatalf("TrackService PublishTrack manifest JSON body was not hydrated")
	}
	publishTrackMsg := publishTrackIn.ProtoReflect()
	publishTrackFields := publishTrackMsg.Descriptor().Fields()
	if got := publishTrackMsg.Get(publishTrackFields.ByName("label")).String(); got != "mic" {
		t.Fatalf("track publish label = %q, want mic", got)
	}
	if got := publishTrackMsg.Get(publishTrackFields.ByName("metadata")).String(); got != "{}" {
		t.Fatalf("track publish metadata = %q, want {}", got)
	}
	muteTrackIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.TrackService/MuteTrack", fix)
	if !ok {
		t.Fatalf("TrackService MuteTrack manifest JSON body was not hydrated")
	}
	muteTrackMsg := muteTrackIn.ProtoReflect()
	muteTrackFields := muteTrackMsg.Descriptor().Fields()
	if got := muteTrackMsg.Get(muteTrackFields.ByName("track_id")).String(); got != "track-1" {
		t.Fatalf("track mute track_id = %q, want track-1", got)
	}
	if got := muteTrackMsg.Get(muteTrackFields.ByName("muted")).Bool(); !got {
		t.Fatalf("track mute muted was not set from manifest")
	}
	unpublishTrackIn, _, ok := buildManifestJSONBody("/udb.core.webrtc.services.v1.TrackService/UnpublishTrack", fix)
	if !ok {
		t.Fatalf("TrackService UnpublishTrack manifest JSON body was not hydrated")
	}
	unpublishTrackMsg := unpublishTrackIn.ProtoReflect()
	unpublishTrackFields := unpublishTrackMsg.Descriptor().Fields()
	if got := unpublishTrackMsg.Get(unpublishTrackFields.ByName("track_id")).String(); got != "track-disposable-1" {
		t.Fatalf("track unpublish track_id = %q, want track-disposable-1", got)
	}
	fix.set("event_type", "event-1")
	fix.set("log_id", "log-1")
	notificationIn, _, ok := buildManifestJSONBody("/udb.core.notification.services.v1.NotificationService/GetPreference", fix)
	if !ok {
		t.Fatalf("NotificationService manifest JSON body was not hydrated")
	}
	notificationMsg := notificationIn.ProtoReflect()
	notificationFields := notificationMsg.Descriptor().Fields()
	if got := notificationMsg.Get(notificationFields.ByName("user_id")).String(); got != "user-1" {
		t.Fatalf("notification user_id = %q, want user-1", got)
	}
	if got := notificationMsg.Get(notificationFields.ByName("channel")).Enum(); got == 0 {
		t.Fatalf("notification channel was not set from manifest enum")
	}
	sendNotificationIn, _, ok := buildManifestJSONBody("/udb.core.notification.services.v1.NotificationService/SendNotification", fix)
	if !ok {
		t.Fatalf("NotificationService SendNotification manifest JSON body was not hydrated")
	}
	sendNotificationMsg := sendNotificationIn.ProtoReflect()
	sendNotificationFields := sendNotificationMsg.Descriptor().Fields()
	if got := sendNotificationMsg.Get(sendNotificationFields.ByName("project_id")).String(); got != "project-1" {
		t.Fatalf("notification send project_id = %q, want project-1", got)
	}
	if got := sendNotificationMsg.Get(sendNotificationFields.ByName("channels")).List().Len(); got != 1 {
		t.Fatalf("notification send channel count = %d, want 1", got)
	}
	sendVars := sendNotificationMsg.Get(sendNotificationFields.ByName("variables")).Map()
	if got := sendVars.Get(protoreflect.ValueOfString("name").MapKey()).String(); got != "SDK" {
		t.Fatalf("notification send variables.name = %q, want SDK", got)
	}
	sendCtx := sendNotificationMsg.Get(sendNotificationFields.ByName("context")).Message()
	sendCtxFields := sendCtx.Descriptor().Fields()
	if got := sendCtx.Get(sendCtxFields.ByName("purpose")).String(); got != "go.live.perf" {
		t.Fatalf("notification send context.purpose = %q, want go.live.perf", got)
	}
	reportDeliveryIn, _, ok := buildManifestJSONBody("/udb.core.notification.services.v1.NotificationService/ReportDelivery", fix)
	if !ok {
		t.Fatalf("NotificationService ReportDelivery manifest JSON body was not hydrated")
	}
	reportDeliveryMsg := reportDeliveryIn.ProtoReflect()
	reportDeliveryFields := reportDeliveryMsg.Descriptor().Fields()
	if got := reportDeliveryMsg.Get(reportDeliveryFields.ByName("provider")).String(); got != "sdk-perf" {
		t.Fatalf("notification report provider = %q, want sdk-perf", got)
	}
	if got := reportDeliveryMsg.Get(reportDeliveryFields.ByName("status")).Enum(); got == 0 {
		t.Fatalf("notification report status was not set from manifest enum")
	}
	retryNotificationIn, _, ok := buildManifestJSONBody("/udb.core.notification.services.v1.NotificationService/RetryNotification", fix)
	if !ok {
		t.Fatalf("NotificationService RetryNotification manifest JSON body was not hydrated")
	}
	retryNotificationMsg := retryNotificationIn.ProtoReflect()
	retryNotificationFields := retryNotificationMsg.Descriptor().Fields()
	if got := retryNotificationMsg.Get(retryNotificationFields.ByName("log_id")).String(); got != "log-1" {
		t.Fatalf("notification retry log_id = %q, want log-1", got)
	}
	upsertTemplateIn, _, ok := buildManifestJSONBody("/udb.core.notification.services.v1.NotificationService/UpsertTemplate", fix)
	if !ok {
		t.Fatalf("NotificationService UpsertTemplate manifest JSON body was not hydrated")
	}
	upsertTemplateMsg := upsertTemplateIn.ProtoReflect()
	upsertTemplateFields := upsertTemplateMsg.Descriptor().Fields()
	if got := upsertTemplateMsg.Get(upsertTemplateFields.ByName("subject_template")).String(); got != "Hello {name}" {
		t.Fatalf("notification template subject = %q, want Hello {name}", got)
	}
	setPreferenceIn, _, ok := buildManifestJSONBody("/udb.core.notification.services.v1.NotificationService/SetPreference", fix)
	if !ok {
		t.Fatalf("NotificationService SetPreference manifest JSON body was not hydrated")
	}
	setPreferenceMsg := setPreferenceIn.ProtoReflect()
	setPreferenceFields := setPreferenceMsg.Descriptor().Fields()
	if got := setPreferenceMsg.Get(setPreferenceFields.ByName("is_opted_out")).Bool(); !got {
		t.Fatalf("notification set preference is_opted_out was not set from manifest")
	}
	fix.set("object_key", "cache-key-1")
	cacheIn, _, ok := buildManifestJSONBody("/udb.core.cache.services.v1.CacheService/Scan", fix)
	if !ok {
		t.Fatalf("CacheService manifest JSON body was not hydrated")
	}
	cacheMsg := cacheIn.ProtoReflect()
	cacheFields := cacheMsg.Descriptor().Fields()
	if got := cacheMsg.Get(cacheFields.ByName("namespace")).String(); got != "sdk-perf-cache" {
		t.Fatalf("cache namespace = %q, want sdk-perf-cache", got)
	}
	if got := cacheMsg.Get(cacheFields.ByName("limit")).Int(); got != 50 {
		t.Fatalf("cache limit = %d, want 50", got)
	}
	cacheNamespaceCreateIn, _, ok := buildManifestJSONBody("/udb.core.cache.services.v1.CacheService/CreateNamespace", fix)
	if !ok {
		t.Fatalf("CacheService CreateNamespace manifest JSON body was not hydrated")
	}
	cacheNamespaceCreateMsg := cacheNamespaceCreateIn.ProtoReflect()
	cacheNamespaceCreateFields := cacheNamespaceCreateMsg.Descriptor().Fields()
	if got := cacheNamespaceCreateMsg.Get(cacheNamespaceCreateFields.ByName("max_bytes")).Int(); got != 1048576 {
		t.Fatalf("cache create max_bytes = %d, want 1048576", got)
	}
	if got := cacheNamespaceCreateMsg.Get(cacheNamespaceCreateFields.ByName("default_ttl_seconds")).Int(); got != 300 {
		t.Fatalf("cache create default_ttl_seconds = %d, want 300", got)
	}
	cacheServiceSetIn, _, ok := buildManifestJSONBody("/udb.core.cache.services.v1.CacheService/Set", fix)
	if !ok {
		t.Fatalf("CacheService Set manifest JSON body was not hydrated")
	}
	cacheServiceSetMsg := cacheServiceSetIn.ProtoReflect()
	cacheServiceSetFields := cacheServiceSetMsg.Descriptor().Fields()
	if got := string(cacheServiceSetMsg.Get(cacheServiceSetFields.ByName("value")).Bytes()); got != "perf" {
		t.Fatalf("cache set value = %q, want perf", got)
	}
	if got := cacheServiceSetMsg.Get(cacheServiceSetFields.ByName("ttl_seconds")).Int(); got != 300 {
		t.Fatalf("cache set ttl_seconds = %d, want 300", got)
	}
	cacheServiceDeleteIn, _, ok := buildManifestJSONBody("/udb.core.cache.services.v1.CacheService/Delete", fix)
	if !ok {
		t.Fatalf("CacheService Delete manifest JSON body was not hydrated")
	}
	cacheServiceDeleteMsg := cacheServiceDeleteIn.ProtoReflect()
	cacheServiceDeleteFields := cacheServiceDeleteMsg.Descriptor().Fields()
	if got := cacheServiceDeleteMsg.Get(cacheServiceDeleteFields.ByName("key")).String(); got != "cache-key-1" {
		t.Fatalf("cache delete key = %q, want cache-key-1", got)
	}
	cacheNamespaceDeleteIn, _, ok := buildManifestJSONBody("/udb.core.cache.services.v1.CacheService/DeleteNamespace", fix)
	if !ok {
		t.Fatalf("CacheService DeleteNamespace manifest JSON body was not hydrated")
	}
	cacheNamespaceDeleteMsg := cacheNamespaceDeleteIn.ProtoReflect()
	cacheNamespaceDeleteFields := cacheNamespaceDeleteMsg.Descriptor().Fields()
	if got := cacheNamespaceDeleteMsg.Get(cacheNamespaceDeleteFields.ByName("confirmation_token")).String(); got != "sdk-perf-cache" {
		t.Fatalf("cache delete namespace confirmation_token = %q, want sdk-perf-cache", got)
	}
	fix.set("project", "project-1")
	meteringIn, _, ok := buildManifestJSONBody("/udb.core.metering.services.v1.MeteringService/ListQuotas", fix)
	if !ok {
		t.Fatalf("MeteringService manifest JSON body was not hydrated")
	}
	meteringMsg := meteringIn.ProtoReflect()
	meteringFields := meteringMsg.Descriptor().Fields()
	if got := meteringMsg.Get(meteringFields.ByName("project_id")).String(); got != "project-1" {
		t.Fatalf("metering project_id = %q, want project-1", got)
	}
	if got := meteringMsg.Get(meteringFields.ByName("limit")).Uint(); got != 50 {
		t.Fatalf("metering limit = %d, want 50", got)
	}
	putQuotaIn, _, ok := buildManifestJSONBody("/udb.core.metering.services.v1.MeteringService/PutQuota", fix)
	if !ok {
		t.Fatalf("MeteringService PutQuota manifest JSON body was not hydrated")
	}
	putQuotaMsg := putQuotaIn.ProtoReflect()
	putQuotaFields := putQuotaMsg.Descriptor().Fields()
	if got := putQuotaMsg.Get(putQuotaFields.ByName("limit_value")).Int(); got != 1000000 {
		t.Fatalf("metering PutQuota limit_value = %d, want 1000000", got)
	}
	if got := putQuotaMsg.Get(putQuotaFields.ByName("enabled")).Bool(); !got {
		t.Fatalf("metering PutQuota enabled was not set")
	}
	recordUsageIn, _, ok := buildManifestJSONBody("/udb.core.metering.services.v1.MeteringService/RecordUsage", fix)
	if !ok {
		t.Fatalf("MeteringService RecordUsage manifest JSON body was not hydrated")
	}
	recordUsageMsg := recordUsageIn.ProtoReflect()
	recordUsageFields := recordUsageMsg.Descriptor().Fields()
	if got := recordUsageMsg.Get(recordUsageFields.ByName("principal_id")).String(); got != "user-1" {
		t.Fatalf("metering RecordUsage principal_id = %q, want user-1", got)
	}
	if got := recordUsageMsg.Get(recordUsageFields.ByName("quantity")).Int(); got != 1 {
		t.Fatalf("metering RecordUsage quantity = %d, want 1", got)
	}
	fix.set("renew_fencing_token", "77")
	fix.set("release_fencing_token", "88")
	lockIn, _, ok := buildManifestJSONBody("/udb.core.lock.services.v1.LockService/AcquireLock", fix)
	if !ok {
		t.Fatalf("LockService AcquireLock manifest JSON body was not hydrated")
	}
	lockMsg := lockIn.ProtoReflect()
	lockFields := lockMsg.Descriptor().Fields()
	if got := lockMsg.Get(lockFields.ByName("lock_name")).String(); got != "sdk-perf-acquire-lock" {
		t.Fatalf("lock AcquireLock lock_name = %q, want sdk-perf-acquire-lock", got)
	}
	if got := lockMsg.Get(lockFields.ByName("lease_ttl_seconds")).Int(); got != 60 {
		t.Fatalf("lock AcquireLock lease_ttl_seconds = %d, want 60", got)
	}
	if got := lockMsg.Get(lockFields.ByName("metadata_json")).String(); got != "{}" {
		t.Fatalf("lock AcquireLock metadata_json = %q, want {}", got)
	}
	renewLockIn, _, ok := buildManifestJSONBody("/udb.core.lock.services.v1.LockService/RenewLock", fix)
	if !ok {
		t.Fatalf("LockService RenewLock manifest JSON body was not hydrated")
	}
	renewLockMsg := renewLockIn.ProtoReflect()
	renewLockFields := renewLockMsg.Descriptor().Fields()
	if got := renewLockMsg.Get(renewLockFields.ByName("lock_name")).String(); got != "sdk-perf-renew-lock" {
		t.Fatalf("lock RenewLock lock_name = %q, want sdk-perf-renew-lock", got)
	}
	if got := renewLockMsg.Get(renewLockFields.ByName("fencing_token")).Int(); got != 77 {
		t.Fatalf("lock RenewLock fencing_token = %d, want 77", got)
	}
	releaseLockIn, _, ok := buildManifestJSONBody("/udb.core.lock.services.v1.LockService/ReleaseLock", fix)
	if !ok {
		t.Fatalf("LockService ReleaseLock manifest JSON body was not hydrated")
	}
	releaseLockMsg := releaseLockIn.ProtoReflect()
	releaseLockFields := releaseLockMsg.Descriptor().Fields()
	if got := releaseLockMsg.Get(releaseLockFields.ByName("lock_name")).String(); got != "sdk-perf-release-lock" {
		t.Fatalf("lock ReleaseLock lock_name = %q, want sdk-perf-release-lock", got)
	}
	if got := releaseLockMsg.Get(releaseLockFields.ByName("owner_id")).String(); got != "user-1" {
		t.Fatalf("lock ReleaseLock owner_id = %q, want user-1", got)
	}
	if got := releaseLockMsg.Get(releaseLockFields.ByName("fencing_token")).Int(); got != 88 {
		t.Fatalf("lock ReleaseLock fencing_token = %d, want 88", got)
	}
	fix.set("job_id", "job-1")
	schedulerIn, _, ok := buildManifestJSONBody("/udb.core.scheduler.services.v1.SchedulerService/ListJobs", fix)
	if !ok {
		t.Fatalf("SchedulerService manifest JSON body was not hydrated")
	}
	schedulerMsg := schedulerIn.ProtoReflect()
	schedulerFields := schedulerMsg.Descriptor().Fields()
	if got := schedulerMsg.Get(schedulerFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("scheduler tenant_id = %q, want tenant-1", got)
	}
	if got := schedulerMsg.Get(schedulerFields.ByName("page_size")).Int(); got != 20 {
		t.Fatalf("scheduler page_size = %d, want 20", got)
	}
	createJobIn, _, ok := buildManifestJSONBody("/udb.core.scheduler.services.v1.SchedulerService/CreateJob", fix)
	if !ok {
		t.Fatalf("SchedulerService CreateJob manifest JSON body was not hydrated")
	}
	createJobMsg := createJobIn.ProtoReflect()
	createJobFields := createJobMsg.Descriptor().Fields()
	if got := createJobMsg.Get(createJobFields.ByName("project_id")).String(); got != "" {
		t.Fatalf("scheduler create project_id = %q, want empty project_id", got)
	}
	if got := createJobMsg.Get(createJobFields.ByName("cron_expression")).String(); got != "*/5 * * * *" {
		t.Fatalf("scheduler create cron_expression = %q, want */5 * * * *", got)
	}
	if got := createJobMsg.Get(createJobFields.ByName("max_attempts")).Int(); got != 3 {
		t.Fatalf("scheduler create max_attempts = %d, want 3", got)
	}
	if got := createJobMsg.Get(createJobFields.ByName("backoff_seconds")).Int(); got != 30 {
		t.Fatalf("scheduler create backoff_seconds = %d, want 30", got)
	}
	pauseJobIn, _, ok := buildManifestJSONBody("/udb.core.scheduler.services.v1.SchedulerService/PauseJob", fix)
	if !ok {
		t.Fatalf("SchedulerService PauseJob manifest JSON body was not hydrated")
	}
	pauseJobMsg := pauseJobIn.ProtoReflect()
	pauseJobFields := pauseJobMsg.Descriptor().Fields()
	if got := pauseJobMsg.Get(pauseJobFields.ByName("job_id")).String(); got != "job-1" {
		t.Fatalf("scheduler pause job_id = %q, want job-1", got)
	}
	resumeJobIn, _, ok := buildManifestJSONBody("/udb.core.scheduler.services.v1.SchedulerService/ResumeJob", fix)
	if !ok {
		t.Fatalf("SchedulerService ResumeJob manifest JSON body was not hydrated")
	}
	resumeJobMsg := resumeJobIn.ProtoReflect()
	resumeJobFields := resumeJobMsg.Descriptor().Fields()
	if got := resumeJobMsg.Get(resumeJobFields.ByName("job_id")).String(); got != "job-1" {
		t.Fatalf("scheduler resume job_id = %q, want job-1", got)
	}
	deleteJobIn, _, ok := buildManifestJSONBody("/udb.core.scheduler.services.v1.SchedulerService/DeleteJob", fix)
	if !ok {
		t.Fatalf("SchedulerService DeleteJob manifest JSON body was not hydrated")
	}
	deleteJobMsg := deleteJobIn.ProtoReflect()
	deleteJobFields := deleteJobMsg.Descriptor().Fields()
	if got := deleteJobMsg.Get(deleteJobFields.ByName("job_id")).String(); got != "job-1" {
		t.Fatalf("scheduler delete job_id = %q, want job-1", got)
	}
	fix.set("endpoint_id", "endpoint-1")
	fix.set("delete_endpoint_id", "endpoint-delete-1")
	fix.set("topic_pattern", "tenant-1.*")
	webhookIn, _, ok := buildManifestJSONBody("/udb.core.webhook.services.v1.WebhookService/ListEndpoints", fix)
	if !ok {
		t.Fatalf("WebhookService manifest JSON body was not hydrated")
	}
	webhookMsg := webhookIn.ProtoReflect()
	webhookFields := webhookMsg.Descriptor().Fields()
	if got := webhookMsg.Get(webhookFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("webhook tenant_id = %q, want tenant-1", got)
	}
	if got := webhookMsg.Get(webhookFields.ByName("active_only")).Bool(); !got {
		t.Fatalf("webhook active_only was not set from manifest")
	}
	if got := webhookMsg.Get(webhookFields.ByName("page_size")).Int(); got != 20 {
		t.Fatalf("webhook page_size = %d, want 20", got)
	}
	createEndpointIn, _, ok := buildManifestJSONBody("/udb.core.webhook.services.v1.WebhookService/CreateEndpoint", fix)
	if !ok {
		t.Fatalf("WebhookService CreateEndpoint manifest JSON body was not hydrated")
	}
	createEndpointMsg := createEndpointIn.ProtoReflect()
	createEndpointFields := createEndpointMsg.Descriptor().Fields()
	if got := createEndpointMsg.Get(createEndpointFields.ByName("topic_pattern")).String(); got != "tenant-1.*" {
		t.Fatalf("webhook create topic_pattern = %q, want tenant-1.*", got)
	}
	if got := createEndpointMsg.Get(createEndpointFields.ByName("metadata_json")).String(); got != "{}" {
		t.Fatalf("webhook create metadata_json = %q, want {}", got)
	}
	if got := createEndpointMsg.Get(createEndpointFields.ByName("max_attempts")).Int(); got != 3 {
		t.Fatalf("webhook create max_attempts = %d, want 3", got)
	}
	updateEndpointIn, _, ok := buildManifestJSONBody("/udb.core.webhook.services.v1.WebhookService/UpdateEndpoint", fix)
	if !ok {
		t.Fatalf("WebhookService UpdateEndpoint manifest JSON body was not hydrated")
	}
	updateEndpointMsg := updateEndpointIn.ProtoReflect()
	updateEndpointFields := updateEndpointMsg.Descriptor().Fields()
	if got := updateEndpointMsg.Get(updateEndpointFields.ByName("endpoint_id")).String(); got != "endpoint-1" {
		t.Fatalf("webhook update endpoint_id = %q, want endpoint-1", got)
	}
	if got := updateEndpointMsg.Get(updateEndpointFields.ByName("active")).Bool(); !got {
		t.Fatalf("webhook update active was not set from manifest")
	}
	deleteEndpointIn, _, ok := buildManifestJSONBody("/udb.core.webhook.services.v1.WebhookService/DeleteEndpoint", fix)
	if !ok {
		t.Fatalf("WebhookService DeleteEndpoint manifest JSON body was not hydrated")
	}
	deleteEndpointMsg := deleteEndpointIn.ProtoReflect()
	deleteEndpointFields := deleteEndpointMsg.Descriptor().Fields()
	if got := deleteEndpointMsg.Get(deleteEndpointFields.ByName("endpoint_id")).String(); got != "endpoint-delete-1" {
		t.Fatalf("webhook delete endpoint_id = %q, want endpoint-delete-1", got)
	}
	fix.set("backup_id", "backup-1")
	backupIn, _, ok := buildManifestJSONBody("/udb.core.backup.services.v1.BackupService/ListBackups", fix)
	if !ok {
		t.Fatalf("BackupService manifest JSON body was not hydrated")
	}
	backupMsg := backupIn.ProtoReflect()
	backupFields := backupMsg.Descriptor().Fields()
	if got := backupMsg.Get(backupFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("backup tenant_id = %q, want tenant-1", got)
	}
	if got := backupMsg.Get(backupFields.ByName("page_size")).Int(); got != 20 {
		t.Fatalf("backup page_size = %d, want 20", got)
	}
	backupPolicyIn, _, ok := buildManifestJSONBody("/udb.core.backup.services.v1.BackupService/PutBackupPolicy", fix)
	if !ok {
		t.Fatalf("BackupService PutBackupPolicy manifest JSON body was not hydrated")
	}
	backupPolicyMsg := backupPolicyIn.ProtoReflect()
	backupPolicyFields := backupPolicyMsg.Descriptor().Fields()
	if got := backupPolicyMsg.Get(backupPolicyFields.ByName("policy_name")).String(); got != "sdk-perf-default" {
		t.Fatalf("backup policy_name = %q, want sdk-perf-default", got)
	}
	if got := backupPolicyMsg.Get(backupPolicyFields.ByName("retention_days")).Int(); got != 7 {
		t.Fatalf("backup retention_days = %d, want 7", got)
	}
	if got := backupPolicyMsg.Get(backupPolicyFields.ByName("max_retained_backups")).Int(); got != 3 {
		t.Fatalf("backup max_retained_backups = %d, want 3", got)
	}
	if got := backupPolicyMsg.Get(backupPolicyFields.ByName("enabled")).Bool(); !got {
		t.Fatalf("backup policy enabled was not set")
	}
	startBackupIn, _, ok := buildManifestJSONBody("/udb.core.backup.services.v1.BackupService/StartTenantBackup", fix)
	if !ok {
		t.Fatalf("BackupService StartTenantBackup manifest JSON body was not hydrated")
	}
	startBackupMsg := startBackupIn.ProtoReflect()
	startBackupFields := startBackupMsg.Descriptor().Fields()
	if got := startBackupMsg.Get(startBackupFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("backup start tenant_id = %q, want tenant-1", got)
	}
	fix.set("restore_tenant_id", "restore-tenant-1")
	restoreBackupIn, _, ok := buildManifestJSONBody("/udb.core.backup.services.v1.BackupService/RestoreTenant", fix)
	if !ok {
		t.Fatalf("BackupService RestoreTenant manifest JSON body was not hydrated")
	}
	restoreBackupMsg := restoreBackupIn.ProtoReflect()
	restoreBackupFields := restoreBackupMsg.Descriptor().Fields()
	if got := restoreBackupMsg.Get(restoreBackupFields.ByName("target_tenant_id")).String(); got != "restore-tenant-1" {
		t.Fatalf("backup restore target_tenant_id = %q, want restore-tenant-1", got)
	}
	if got := restoreBackupMsg.Get(restoreBackupFields.ByName("confirmation_token")).String(); got != "yes" {
		t.Fatalf("backup restore confirmation_token = %q, want yes", got)
	}
	if got := restoreBackupMsg.Get(restoreBackupFields.ByName("allow_cross_tenant")).Bool(); !got {
		t.Fatalf("backup restore allow_cross_tenant = false, want true")
	}
	deleteBackupPolicyIn, _, ok := buildManifestJSONBody("/udb.core.backup.services.v1.BackupService/DeleteBackupPolicy", fix)
	if !ok {
		t.Fatalf("BackupService DeleteBackupPolicy manifest JSON body was not hydrated")
	}
	deleteBackupPolicyMsg := deleteBackupPolicyIn.ProtoReflect()
	deleteBackupPolicyFields := deleteBackupPolicyMsg.Descriptor().Fields()
	if got := deleteBackupPolicyMsg.Get(deleteBackupPolicyFields.ByName("policy_name")).String(); got != "sdk-perf-default" {
		t.Fatalf("backup delete policy_name = %q, want sdk-perf-default", got)
	}
	configIn, _, ok := buildManifestJSONBody("/udb.core.config.services.v1.ConfigService/EvaluateFlags", fix)
	if !ok {
		t.Fatalf("ConfigService manifest JSON body was not hydrated")
	}
	configMsg := configIn.ProtoReflect()
	configFields := configMsg.Descriptor().Fields()
	if got := configMsg.Get(configFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("config tenant_id = %q, want tenant-1", got)
	}
	if got := configMsg.Get(configFields.ByName("keys")).List().Len(); got != 1 {
		t.Fatalf("config keys len = %d, want 1", got)
	}
	contextMsg := configMsg.Get(configFields.ByName("context")).Message()
	contextFields := contextMsg.Descriptor().Fields()
	if got := contextMsg.Get(contextFields.ByName("project_id")).String(); got != "project-1" {
		t.Fatalf("config context project_id = %q, want project-1", got)
	}
	configPutIn, _, ok := buildManifestJSONBody("/udb.core.config.services.v1.ConfigService/PutFlag", fix)
	if !ok {
		t.Fatalf("ConfigService PutFlag manifest JSON body was not hydrated")
	}
	configPutMsg := configPutIn.ProtoReflect()
	configPutFields := configPutMsg.Descriptor().Fields()
	if got := configPutMsg.Get(configPutFields.ByName("flag_key")).String(); got != "sdk.perf.enabled" {
		t.Fatalf("config PutFlag flag_key = %q, want sdk.perf.enabled", got)
	}
	configValueMsg := configPutMsg.Get(configPutFields.ByName("value")).Message()
	configValueFields := configValueMsg.Descriptor().Fields()
	if got := configValueMsg.Get(configValueFields.ByName("bool_value")).Bool(); !got {
		t.Fatalf("config PutFlag value.bool_value was not set")
	}
	if got := configPutMsg.Get(configPutFields.ByName("rollout_percentage")).Int(); got != 100 {
		t.Fatalf("config PutFlag rollout_percentage = %d, want 100", got)
	}
	configDeleteIn, _, ok := buildManifestJSONBody("/udb.core.config.services.v1.ConfigService/DeleteFlag", fix)
	if !ok {
		t.Fatalf("ConfigService DeleteFlag manifest JSON body was not hydrated")
	}
	configDeleteMsg := configDeleteIn.ProtoReflect()
	configDeleteFields := configDeleteMsg.Descriptor().Fields()
	if got := configDeleteMsg.Get(configDeleteFields.ByName("project_id")).String(); got != "project-1" {
		t.Fatalf("config DeleteFlag project_id = %q, want project-1", got)
	}
	if got := configDeleteMsg.Get(configDeleteFields.ByName("flag_key")).String(); got != "sdk.perf.enabled" {
		t.Fatalf("config DeleteFlag flag_key = %q, want sdk.perf.enabled", got)
	}
	fix.set("workflow_id", "workflow-1")
	fix.set("record_id", "record-1")
	workflowIn, _, ok := buildManifestJSONBody("/udb.core.workflow.services.v1.WorkflowService/ListWorkflows", fix)
	if !ok {
		t.Fatalf("WorkflowService manifest JSON body was not hydrated")
	}
	workflowMsg := workflowIn.ProtoReflect()
	workflowFields := workflowMsg.Descriptor().Fields()
	if got := workflowMsg.Get(workflowFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("workflow tenant_id = %q, want tenant-1", got)
	}
	if got := workflowMsg.Get(workflowFields.ByName("status")).String(); got != "RUNNING" {
		t.Fatalf("workflow status = %q, want RUNNING", got)
	}
	if got := workflowMsg.Get(workflowFields.ByName("page_size")).Int(); got != 20 {
		t.Fatalf("workflow page_size = %d, want 20", got)
	}
	fix.set("cancel_workflow_id", "workflow-cancel-1")
	startWorkflowIn, _, ok := buildManifestJSONBody("/udb.core.workflow.services.v1.WorkflowService/StartWorkflow", fix)
	if !ok {
		t.Fatalf("WorkflowService StartWorkflow manifest JSON body was not hydrated")
	}
	startWorkflowMsg := startWorkflowIn.ProtoReflect()
	startWorkflowFields := startWorkflowMsg.Descriptor().Fields()
	if got := startWorkflowMsg.Get(startWorkflowFields.ByName("project_id")).String(); got != "" {
		t.Fatalf("workflow start project_id = %q, want empty project_id", got)
	}
	if got := startWorkflowMsg.Get(startWorkflowFields.ByName("workflow_type")).String(); got != "sdk.perf.workflow" {
		t.Fatalf("workflow start workflow_type = %q, want sdk.perf.workflow", got)
	}
	if got := startWorkflowMsg.Get(startWorkflowFields.ByName("total_steps")).Int(); got != 20 {
		t.Fatalf("workflow start total_steps = %d, want 20", got)
	}
	if got := startWorkflowMsg.Get(startWorkflowFields.ByName("correlation_id")).String(); got != "record-1" {
		t.Fatalf("workflow start correlation_id = %q, want record-1", got)
	}
	cancelWorkflowIn, _, ok := buildManifestJSONBody("/udb.core.workflow.services.v1.WorkflowService/CancelWorkflow", fix)
	if !ok {
		t.Fatalf("WorkflowService CancelWorkflow manifest JSON body was not hydrated")
	}
	cancelWorkflowMsg := cancelWorkflowIn.ProtoReflect()
	cancelWorkflowFields := cancelWorkflowMsg.Descriptor().Fields()
	if got := cancelWorkflowMsg.Get(cancelWorkflowFields.ByName("workflow_id")).String(); got != "workflow-cancel-1" {
		t.Fatalf("workflow cancel workflow_id = %q, want workflow-cancel-1", got)
	}
	if got := cancelWorkflowMsg.Get(cancelWorkflowFields.ByName("reason")).String(); got != "sdk perf cancel" {
		t.Fatalf("workflow cancel reason = %q, want sdk perf cancel", got)
	}
	signalWorkflowIn, _, ok := buildManifestJSONBody("/udb.core.workflow.services.v1.WorkflowService/SignalWorkflow", fix)
	if !ok {
		t.Fatalf("WorkflowService SignalWorkflow manifest JSON body was not hydrated")
	}
	signalWorkflowMsg := signalWorkflowIn.ProtoReflect()
	signalWorkflowFields := signalWorkflowMsg.Descriptor().Fields()
	if got := signalWorkflowMsg.Get(signalWorkflowFields.ByName("signal_name")).String(); got != "continue" {
		t.Fatalf("workflow signal signal_name = %q, want continue", got)
	}
	if got := signalWorkflowMsg.Get(signalWorkflowFields.ByName("signal_payload")).String(); got != `{"ok":true}` {
		t.Fatalf("workflow signal signal_payload = %q, want {\"ok\":true}", got)
	}
	searchIn, _, ok := buildManifestJSONBody("/udb.core.search.services.v1.SearchService/Search", fix)
	if !ok {
		t.Fatalf("SearchService manifest JSON body was not hydrated")
	}
	searchMsg := searchIn.ProtoReflect()
	searchFields := searchMsg.Descriptor().Fields()
	if got := searchMsg.Get(searchFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("search tenant_id = %q, want tenant-1", got)
	}
	if got := searchMsg.Get(searchFields.ByName("query_vector")).List().Len(); got != 3 {
		t.Fatalf("search query_vector len = %d, want 3", got)
	}
	if got := searchMsg.Get(searchFields.ByName("top_k")).Int(); got != 5 {
		t.Fatalf("search top_k = %d, want 5", got)
	}
	if got := searchMsg.Get(searchFields.ByName("mode")).Enum(); got == 0 {
		t.Fatalf("search mode was not set from manifest")
	}
	fix.set("message_type", "myapp.v1.Invoice")
	createIndexIn, _, ok := buildManifestJSONBody("/udb.core.search.services.v1.SearchService/CreateIndex", fix)
	if !ok {
		t.Fatalf("SearchService CreateIndex manifest JSON body was not hydrated")
	}
	createIndexMsg := createIndexIn.ProtoReflect()
	createIndexFields := createIndexMsg.Descriptor().Fields()
	if got := createIndexMsg.Get(createIndexFields.ByName("source_message_type")).String(); got != "myapp.v1.Invoice" {
		t.Fatalf("search create_index source_message_type = %q, want myapp.v1.Invoice", got)
	}
	if got := createIndexMsg.Get(createIndexFields.ByName("vector_dims")).Int(); got != 3 {
		t.Fatalf("search create_index vector_dims = %d, want 3", got)
	}
	if got := createIndexMsg.Get(createIndexFields.ByName("metadata_json")).String(); got != "{}" {
		t.Fatalf("search create_index metadata_json = %q, want {}", got)
	}
	reindexIn, _, ok := buildManifestJSONBody("/udb.core.search.services.v1.SearchService/Reindex", fix)
	if !ok {
		t.Fatalf("SearchService Reindex manifest JSON body was not hydrated")
	}
	reindexMsg := reindexIn.ProtoReflect()
	reindexFields := reindexMsg.Descriptor().Fields()
	if got := reindexMsg.Get(reindexFields.ByName("index_name")).String(); got != "sdk_live_records" {
		t.Fatalf("search reindex index_name = %q, want sdk_live_records", got)
	}
	deleteIndexIn, _, ok := buildManifestJSONBody("/udb.core.search.services.v1.SearchService/DeleteIndex", fix)
	if !ok {
		t.Fatalf("SearchService DeleteIndex manifest JSON body was not hydrated")
	}
	deleteIndexMsg := deleteIndexIn.ProtoReflect()
	deleteIndexFields := deleteIndexMsg.Descriptor().Fields()
	if got := deleteIndexMsg.Get(deleteIndexFields.ByName("index_name")).String(); got != "sdk_live_records" {
		t.Fatalf("search delete_index index_name = %q, want sdk_live_records", got)
	}
	// The embedding manifest bodies reference their own seed family (job /
	// work-item / document / delete-model ids) — mirror the values the live
	// coverage fixture uses so every body hydrates in this offline check too.
	fix.set("embedding_job_id", "11111111-1111-4111-8111-000000000101")
	fix.set("embedding_work_item_id", "11111111-1111-4111-8111-000000000102")
	fix.set("embedding_document_id", "11111111-1111-4111-8111-000000000103")
	fix.set("embedding_document_job_id", "11111111-1111-4111-8111-000000000104")
	fix.set("embedding_delete_model_id", "embedding-delete-model-1")
	embeddingRPCs := []string{
		"EmbeddingService/Backfill",
		"EmbeddingService/CutoverModelAlias",
		"EmbeddingService/DeleteModel",
		"EmbeddingService/DeleteSource",
		"EmbeddingService/GetEmbeddingJobStatus",
		"EmbeddingService/IngestDocument",
		"EmbeddingService/IngestDocumentBatch",
		"EmbeddingService/ListEmbeddingWorkItems",
		"EmbeddingService/ListModels",
		"EmbeddingService/ListSources",
		"EmbeddingService/RegisterModel",
		"EmbeddingService/RegisterSource",
		"EmbeddingService/ReportEmbedding",
		"EmbeddingService/ReportEmbeddingBatch",
		"EmbeddingService/ReportEmbeddingFailure",
		"EmbeddingService/ReportParsedDocument",
		"EmbeddingService/ReportRetrievalEvaluation",
		"EmbeddingService/Retrieve",
		"EmbeddingService/SetModelStatus",
	}
	for _, rpc := range embeddingRPCs {
		path := "/udb.core.embedding.services.v1." + rpc
		if _, _, ok := buildManifestJSONBody(path, fix); !ok {
			t.Fatalf("%s manifest JSON body was not hydrated", rpc)
		}
	}
	embeddingIn, _, ok := buildManifestJSONBody("/udb.core.embedding.services.v1.EmbeddingService/Retrieve", fix)
	if !ok {
		t.Fatalf("EmbeddingService manifest JSON body was not hydrated")
	}
	embeddingMsg := embeddingIn.ProtoReflect()
	embeddingFields := embeddingMsg.Descriptor().Fields()
	if got := embeddingMsg.Get(embeddingFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("embedding tenant_id = %q, want tenant-1", got)
	}
	if got := embeddingMsg.Get(embeddingFields.ByName("query_vector")).List().Len(); got != 3 {
		t.Fatalf("embedding query_vector len = %d, want 3", got)
	}
	if got := embeddingMsg.Get(embeddingFields.ByName("top_k")).Int(); got != 5 {
		t.Fatalf("embedding top_k = %d, want 5", got)
	}
	registerSourceIn, _, ok := buildManifestJSONBody("/udb.core.embedding.services.v1.EmbeddingService/RegisterSource", fix)
	if !ok {
		t.Fatalf("EmbeddingService RegisterSource manifest JSON body was not hydrated")
	}
	registerSourceMsg := registerSourceIn.ProtoReflect()
	registerSourceFields := registerSourceMsg.Descriptor().Fields()
	if got := registerSourceMsg.Get(registerSourceFields.ByName("source_message_type")).String(); got != "myapp.v1.Invoice" {
		t.Fatalf("embedding register_source source_message_type = %q, want myapp.v1.Invoice", got)
	}
	textFields := registerSourceMsg.Get(registerSourceFields.ByName("text_fields")).List()
	if got := textFields.Len(); got != 1 {
		t.Fatalf("embedding register_source text_fields len = %d, want 1", got)
	}
	if got := textFields.Get(0).String(); got != "payload" {
		t.Fatalf("embedding register_source text_fields[0] = %q, want payload", got)
	}
	if got := registerSourceMsg.Get(registerSourceFields.ByName("metadata_json")).String(); got != "{}" {
		t.Fatalf("embedding register_source metadata_json = %q, want {}", got)
	}
	reportEmbeddingIn, _, ok := buildManifestJSONBody("/udb.core.embedding.services.v1.EmbeddingService/ReportEmbedding", fix)
	if !ok {
		t.Fatalf("EmbeddingService ReportEmbedding manifest JSON body was not hydrated")
	}
	reportEmbeddingMsg := reportEmbeddingIn.ProtoReflect()
	reportEmbeddingFields := reportEmbeddingMsg.Descriptor().Fields()
	if got := reportEmbeddingMsg.Get(reportEmbeddingFields.ByName("row_pk")).String(); got != "record-1" {
		t.Fatalf("embedding report_embedding row_pk = %q, want record-1", got)
	}
	reportVector := reportEmbeddingMsg.Get(reportEmbeddingFields.ByName("vector")).List()
	if got := reportVector.Len(); got != 3 {
		t.Fatalf("embedding report_embedding vector len = %d, want 3", got)
	}
	if got := reportVector.Get(0).Float(); got < 0.09 || got > 0.11 {
		t.Fatalf("embedding report_embedding vector[0] = %f, want 0.1", got)
	}
	if got := reportEmbeddingMsg.Get(reportEmbeddingFields.ByName("dims")).Int(); got != 3 {
		t.Fatalf("embedding report_embedding dims = %d, want 3", got)
	}
	backfillIn, _, ok := buildManifestJSONBody("/udb.core.embedding.services.v1.EmbeddingService/Backfill", fix)
	if !ok {
		t.Fatalf("EmbeddingService Backfill manifest JSON body was not hydrated")
	}
	backfillMsg := backfillIn.ProtoReflect()
	backfillFields := backfillMsg.Descriptor().Fields()
	if got := backfillMsg.Get(backfillFields.ByName("source_name")).String(); got != "sdk_live_records" {
		t.Fatalf("embedding backfill source_name = %q, want sdk_live_records", got)
	}
	deleteSourceIn, _, ok := buildManifestJSONBody("/udb.core.embedding.services.v1.EmbeddingService/DeleteSource", fix)
	if !ok {
		t.Fatalf("EmbeddingService DeleteSource manifest JSON body was not hydrated")
	}
	deleteSourceMsg := deleteSourceIn.ProtoReflect()
	deleteSourceFields := deleteSourceMsg.Descriptor().Fields()
	if got := deleteSourceMsg.Get(deleteSourceFields.ByName("source_name")).String(); got != "sdk_live_records" {
		t.Fatalf("embedding delete_source source_name = %q, want sdk_live_records", got)
	}
	fix.set("vault_key_name", "sdk-perf-key")
	fix.set("vault_signing_key_name", "sdk-perf-signing-key")
	fix.set("vault_hmac_key_name", "sdk-perf-hmac-key")
	fix.set("vault_create_key_name", "sdk-perf-create-key")
	fix.set("vault_ciphertext", "udb-vault:v1:seed")
	fix.set("vault_secret_path", "app/config")
	fix.set("vault_put_secret_path", "app/put")
	fix.set("vault_signature", "udb-vault-sig:v1:seed")
	fix.set("vault_delete_secret_path", "app/delete")
	fix.set("vault_destroy_secret_path", "app/destroy")
	fix.set("vault_db_role", "sdk-readonly")
	fix.set("vault_db_idempotency_key", "sdk-vault-db-idempotency")
	fix.set("vault_db_lease_id", "sdk-vault-db-lease")
	vaultIn, _, ok := buildManifestJSONBody("/udb.core.vault.services.v1.VaultService/Verify", fix)
	if !ok {
		t.Fatalf("VaultService manifest JSON body was not hydrated")
	}
	vaultMsg := vaultIn.ProtoReflect()
	vaultFields := vaultMsg.Descriptor().Fields()
	if got := vaultMsg.Get(vaultFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("vault tenant_id = %q, want tenant-1", got)
	}
	if got := vaultMsg.Get(vaultFields.ByName("key_name")).String(); got != "sdk-perf-signing-key" {
		t.Fatalf("vault key_name = %q, want sdk-perf-signing-key", got)
	}
	if got := vaultMsg.Get(vaultFields.ByName("signature")).String(); got != "udb-vault-sig:v1:seed" {
		t.Fatalf("vault signature = %q, want seeded signature", got)
	}
	dbCredentialsIn, _, ok := buildManifestJSONBody("/udb.core.vault.services.v1.VaultService/GenerateDatabaseCredentials", fix)
	if !ok {
		t.Fatalf("VaultService GenerateDatabaseCredentials manifest JSON body was not hydrated")
	}
	dbCredentialsMsg := dbCredentialsIn.ProtoReflect()
	dbCredentialsFields := dbCredentialsMsg.Descriptor().Fields()
	if got := dbCredentialsMsg.Get(dbCredentialsFields.ByName("idempotency_key")).String(); got != "sdk-vault-db-idempotency" {
		t.Fatalf("vault GenerateDatabaseCredentials idempotency_key = %q, want sdk-vault-db-idempotency", got)
	}
	revokeCredentialsIn, _, ok := buildManifestJSONBody("/udb.core.vault.services.v1.VaultService/RevokeDatabaseCredentials", fix)
	if !ok {
		t.Fatalf("VaultService RevokeDatabaseCredentials manifest JSON body was not hydrated")
	}
	revokeCredentialsMsg := revokeCredentialsIn.ProtoReflect()
	revokeCredentialsFields := revokeCredentialsMsg.Descriptor().Fields()
	if got := revokeCredentialsMsg.Get(revokeCredentialsFields.ByName("lease_id")).String(); got != "sdk-vault-db-lease" {
		t.Fatalf("vault RevokeDatabaseCredentials lease_id = %q, want sdk-vault-db-lease", got)
	}
	createKeyIn, _, ok := buildManifestJSONBody("/udb.core.vault.services.v1.VaultService/CreateTransitKey", fix)
	if !ok {
		t.Fatalf("VaultService CreateTransitKey manifest JSON body was not hydrated")
	}
	createKeyMsg := createKeyIn.ProtoReflect()
	createKeyFields := createKeyMsg.Descriptor().Fields()
	if got := createKeyMsg.Get(createKeyFields.ByName("key_name")).String(); got != "sdk-perf-create-key" {
		t.Fatalf("vault CreateTransitKey key_name = %q, want sdk-perf-create-key", got)
	}
	putSecretIn, _, ok := buildManifestJSONBody("/udb.core.vault.services.v1.VaultService/PutSecret", fix)
	if !ok {
		t.Fatalf("VaultService PutSecret manifest JSON body was not hydrated")
	}
	putSecretMsg := putSecretIn.ProtoReflect()
	putSecretFields := putSecretMsg.Descriptor().Fields()
	if got := putSecretMsg.Get(putSecretFields.ByName("secret_path")).String(); got != "app/put" {
		t.Fatalf("vault PutSecret secret_path = %q, want app/put", got)
	}
	if got := putSecretMsg.Get(putSecretFields.ByName("secret_value")).String(); got != "perf-secret" {
		t.Fatalf("vault PutSecret secret_value = %q, want perf-secret", got)
	}
	if got := putSecretMsg.Get(putSecretFields.ByName("expected_version")).Int(); got != 0 {
		t.Fatalf("vault PutSecret expected_version = %d, want 0", got)
	}
	deleteSecretIn, _, ok := buildManifestJSONBody("/udb.core.vault.services.v1.VaultService/DeleteSecret", fix)
	if !ok {
		t.Fatalf("VaultService DeleteSecret manifest JSON body was not hydrated")
	}
	deleteSecretMsg := deleteSecretIn.ProtoReflect()
	deleteSecretFields := deleteSecretMsg.Descriptor().Fields()
	if got := deleteSecretMsg.Get(deleteSecretFields.ByName("secret_path")).String(); got != "app/delete" {
		t.Fatalf("vault DeleteSecret secret_path = %q, want app/delete", got)
	}
	destroySecretIn, _, ok := buildManifestJSONBody("/udb.core.vault.services.v1.VaultService/DestroySecret", fix)
	if !ok {
		t.Fatalf("VaultService DestroySecret manifest JSON body was not hydrated")
	}
	destroySecretMsg := destroySecretIn.ProtoReflect()
	destroySecretFields := destroySecretMsg.Descriptor().Fields()
	// The irreversible crypto-shred is authorized only when confirmation_token EQUALS
	// secret_path; a fixed "destroy" literal is rejected INVALID_ARGUMENT.
	destroySecretPath := destroySecretMsg.Get(destroySecretFields.ByName("secret_path")).String()
	if got := destroySecretMsg.Get(destroySecretFields.ByName("confirmation_token")).String(); got != destroySecretPath {
		t.Fatalf("vault DestroySecret confirmation_token = %q, want it to equal secret_path %q", got, destroySecretPath)
	}
	createTransitIn, _, ok := buildManifestJSONBody("/udb.core.vault.services.v1.VaultService/CreateTransitKey", fix)
	if !ok {
		t.Fatalf("VaultService CreateTransitKey manifest JSON body was not hydrated")
	}
	createTransitMsg := createTransitIn.ProtoReflect()
	createTransitFields := createTransitMsg.Descriptor().Fields()
	if got := createTransitMsg.Get(createTransitFields.ByName("algorithm")).String(); got != "aes256-gcm-siv" {
		t.Fatalf("vault CreateTransitKey algorithm = %q, want aes256-gcm-siv", got)
	}
	rotateTransitIn, _, ok := buildManifestJSONBody("/udb.core.vault.services.v1.VaultService/RotateTransitKey", fix)
	if !ok {
		t.Fatalf("VaultService RotateTransitKey manifest JSON body was not hydrated")
	}
	rotateTransitMsg := rotateTransitIn.ProtoReflect()
	rotateTransitFields := rotateTransitMsg.Descriptor().Fields()
	if got := rotateTransitMsg.Get(rotateTransitFields.ByName("key_name")).String(); got != "sdk-perf-key" {
		t.Fatalf("vault RotateTransitKey key_name = %q, want sdk-perf-key", got)
	}
	encryptIn, _, ok := buildManifestJSONBody("/udb.core.vault.services.v1.VaultService/Encrypt", fix)
	if !ok {
		t.Fatalf("VaultService Encrypt manifest JSON body was not hydrated")
	}
	encryptMsg := encryptIn.ProtoReflect()
	encryptFields := encryptMsg.Descriptor().Fields()
	if got := encryptMsg.Get(encryptFields.ByName("plaintext")).String(); got != "perf" {
		t.Fatalf("vault Encrypt plaintext = %q, want perf", got)
	}
	signIn, _, ok := buildManifestJSONBody("/udb.core.vault.services.v1.VaultService/Sign", fix)
	if !ok {
		t.Fatalf("VaultService Sign manifest JSON body was not hydrated")
	}
	signMsg := signIn.ProtoReflect()
	signFields := signMsg.Descriptor().Fields()
	if got := signMsg.Get(signFields.ByName("input")).String(); got != "perf" {
		t.Fatalf("vault Sign input = %q, want perf", got)
	}
	hmacIn, _, ok := buildManifestJSONBody("/udb.core.vault.services.v1.VaultService/Hmac", fix)
	if !ok {
		t.Fatalf("VaultService Hmac manifest JSON body was not hydrated")
	}
	hmacMsg := hmacIn.ProtoReflect()
	hmacFields := hmacMsg.Descriptor().Fields()
	if got := hmacMsg.Get(hmacFields.ByName("input")).String(); got != "perf" {
		t.Fatalf("vault Hmac input = %q, want perf", got)
	}
	if got := hmacMsg.Get(hmacFields.ByName("key_name")).String(); got != "sdk-perf-hmac-key" {
		t.Fatalf("vault Hmac key_name = %q, want sdk-perf-hmac-key", got)
	}
	dbCredsIn, _, ok := buildManifestJSONBody("/udb.core.vault.services.v1.VaultService/GenerateDatabaseCredentials", fix)
	if !ok {
		t.Fatalf("VaultService GenerateDatabaseCredentials manifest JSON body was not hydrated")
	}
	dbCredsMsg := dbCredsIn.ProtoReflect()
	dbCredsFields := dbCredsMsg.Descriptor().Fields()
	if got := dbCredsMsg.Get(dbCredsFields.ByName("role_name")).String(); got != "sdk-readonly" {
		t.Fatalf("vault GenerateDatabaseCredentials role_name = %q, want sdk-readonly", got)
	}
	if got := dbCredsMsg.Get(dbCredsFields.ByName("ttl_seconds")).Int(); got != 900 {
		t.Fatalf("vault GenerateDatabaseCredentials ttl_seconds = %d, want 900", got)
	}
	fix.set("node_id", "node-1")
	fix.set("resource_name", "backend-target-1")
	controlIn, _, ok := buildManifestJSONBody("/udb.core.control.services.v1.ControlPlaneService/GetResources", fix)
	if !ok {
		t.Fatalf("ControlPlaneService GetResources manifest JSON body was not hydrated")
	}
	controlMsg := controlIn.ProtoReflect()
	controlFields := controlMsg.Descriptor().Fields()
	if got := controlMsg.Get(controlFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("control tenant_id = %q, want tenant-1", got)
	}
	if got := controlMsg.Get(controlFields.ByName("resource_type")).Enum(); got == 0 {
		t.Fatalf("control resource_type was not set from manifest")
	}
	pageMsg := controlMsg.Get(controlFields.ByName("page")).Message()
	pageFields := pageMsg.Descriptor().Fields()
	if got := pageMsg.Get(pageFields.ByName("page_size")).Int(); got != 50 {
		t.Fatalf("control page_size = %d, want 50", got)
	}
	ackIn, _, ok := buildManifestJSONBody("/udb.core.control.services.v1.ControlPlaneService/AckStatus", fix)
	if !ok {
		t.Fatalf("ControlPlaneService AckStatus manifest JSON body was not hydrated")
	}
	ackMsg := ackIn.ProtoReflect()
	ackFields := ackMsg.Descriptor().Fields()
	if got := ackMsg.Get(ackFields.ByName("node_id")).String(); got != "node-1" {
		t.Fatalf("control ack node_id = %q, want node-1", got)
	}
	if got := ackMsg.Get(ackFields.ByName("resource_type")).Enum(); got == 0 {
		t.Fatalf("control ack resource_type was not set from manifest")
	}
	ackContext := ackMsg.Get(ackFields.ByName("context")).Message()
	ackTenant := ackContext.Get(ackContext.Descriptor().Fields().ByName("tenant")).Message()
	if got := ackTenant.Get(ackTenant.Descriptor().Fields().ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("control ack context tenant_id = %q, want tenant-1", got)
	}
	deltaIn, _, ok := buildManifestJSONBody("/udb.core.control.services.v1.ControlPlaneService/DeltaResources", fix)
	if !ok {
		t.Fatalf("ControlPlaneService DeltaResources manifest JSON body was not hydrated")
	}
	deltaMsg := deltaIn.ProtoReflect()
	deltaFields := deltaMsg.Descriptor().Fields()
	if got := deltaMsg.Get(deltaFields.ByName("node_id")).String(); got != "node-1" {
		t.Fatalf("control delta node_id = %q, want node-1", got)
	}
	deltaSub := deltaMsg.Get(deltaFields.ByName("resource_names_subscribe")).List()
	if got := deltaSub.Len(); got != 1 {
		t.Fatalf("control delta resource_names_subscribe len = %d, want 1", got)
	}
	if got := deltaSub.Get(0).String(); got != "backend-target-1" {
		t.Fatalf("control delta resource_names_subscribe[0] = %q, want backend-target-1", got)
	}
	if got := deltaMsg.Get(deltaFields.ByName("initial_resource_versions")).Map().Len(); got != 0 {
		t.Fatalf("control delta initial_resource_versions len = %d, want 0", got)
	}
	fix.set("rollback_resource_version", "control-version-1")
	controlRollbackIn, _, ok := buildManifestJSONBody("/udb.core.control.services.v1.ControlPlaneService/RollbackResources", fix)
	if !ok {
		t.Fatalf("ControlPlaneService RollbackResources manifest JSON body was not hydrated")
	}
	controlRollbackMsg := controlRollbackIn.ProtoReflect()
	controlRollbackFields := controlRollbackMsg.Descriptor().Fields()
	if got := controlRollbackMsg.Get(controlRollbackFields.ByName("target_version")).String(); got != "control-version-1" {
		t.Fatalf("control rollback target_version = %q, want control-version-1", got)
	}
	controlRollbackContext := controlRollbackMsg.Get(controlRollbackFields.ByName("context")).Message()
	controlRollbackTenant := controlRollbackContext.Get(controlRollbackContext.Descriptor().Fields().ByName("tenant")).Message()
	if got := controlRollbackTenant.Get(controlRollbackTenant.Descriptor().Fields().ByName("project_id")).String(); got != "project-1" {
		t.Fatalf("control rollback context project_id = %q, want project-1", got)
	}
	streamIn, _, ok := buildManifestJSONBody("/udb.core.control.services.v1.ControlPlaneService/StreamResources", fix)
	if !ok {
		t.Fatalf("ControlPlaneService StreamResources manifest JSON body was not hydrated")
	}
	streamMsg := streamIn.ProtoReflect()
	streamFields := streamMsg.Descriptor().Fields()
	if got := streamMsg.Get(streamFields.ByName("node_id")).String(); got != "node-1" {
		t.Fatalf("control stream node_id = %q, want node-1", got)
	}
	if got := streamMsg.Get(streamFields.ByName("resource_names")).List().Len(); got != 0 {
		t.Fatalf("control stream resource_names len = %d, want 0", got)
	}
	if got := streamMsg.Get(streamFields.ByName("version_info")).String(); got != "" {
		t.Fatalf("control stream version_info = %q, want empty", got)
	}
	fix.set("user_id", "user-1")
	fix.set("object", "ledger")
	fix.set("action", "data.select")
	fix.set("subject", "subject-1")
	fix.set("policy_id", "policy-1")
	fix.set("resource", "invoice")
	fix.set("role_id", "role-1")
	fix.set("policy_draft_id", "draft-1")
	fix.set("canary_id", "canary-1")
	fix.set("gov_exp", "1893456000")
	authzIn, _, ok := buildManifestJSONBody("/udb.core.authz.services.v1.AuthzService/BatchCheckPermissions", fix)
	if !ok {
		t.Fatalf("AuthzService manifest JSON body was not hydrated")
	}
	authzMsg := authzIn.ProtoReflect()
	authzFields := authzMsg.Descriptor().Fields()
	if got := authzMsg.Get(authzFields.ByName("user_id")).String(); got != "user-1" {
		t.Fatalf("authz user_id = %q, want user-1", got)
	}
	if got := authzMsg.Get(authzFields.ByName("checks")).List().Len(); got != 1 {
		t.Fatalf("authz checks len = %d, want 1", got)
	}
	checkMsg := authzMsg.Get(authzFields.ByName("checks")).List().Get(0).Message()
	checkFields := checkMsg.Descriptor().Fields()
	if got := checkMsg.Get(checkFields.ByName("action")).String(); got != "data.select" {
		t.Fatalf("authz check action = %q, want data.select", got)
	}
	authorizeIn, _, ok := buildManifestJSONBody("/udb.core.authz.services.v1.AuthzService/Authorize", fix)
	if !ok {
		t.Fatalf("AuthzService Authorize manifest JSON body was not hydrated")
	}
	authorizeMsg := authorizeIn.ProtoReflect()
	authorizeFields := authorizeMsg.Descriptor().Fields()
	if got := authorizeMsg.Get(authorizeFields.ByName("principal")).Message().Get(authorizeFields.ByName("principal").Message().Fields().ByName("user_id")).String(); got != "user-1" {
		t.Fatalf("authz authorize principal user_id = %q, want user-1", got)
	}
	if got := authorizeMsg.Get(authorizeFields.ByName("resource")).Message().Get(authorizeFields.ByName("resource").Message().Fields().ByName("table")).String(); got != "sdk_live_records" {
		t.Fatalf("authz authorize resource table = %q, want sdk_live_records", got)
	}
	roleIn, _, ok := buildManifestJSONBody("/udb.core.authz.services.v1.AuthzService/GetRole", fix)
	if !ok {
		t.Fatalf("AuthzService GetRole manifest JSON body was not hydrated")
	}
	roleMsg := roleIn.ProtoReflect()
	roleFields := roleMsg.Descriptor().Fields()
	if got := roleMsg.Get(roleFields.ByName("role_id")).String(); got != "role-1" {
		t.Fatalf("authz role_id = %q, want role-1", got)
	}
	auditsIn, _, ok := buildManifestJSONBody("/udb.core.authz.services.v1.AuthzService/ListAccessDecisionAudits", fix)
	if !ok {
		t.Fatalf("AuthzService ListAccessDecisionAudits manifest JSON body was not hydrated")
	}
	auditsMsg := auditsIn.ProtoReflect()
	auditsFields := auditsMsg.Descriptor().Fields()
	if got := auditsMsg.Get(auditsFields.ByName("page")).Message().Get(auditsFields.ByName("page").Message().Fields().ByName("page_size")).Int(); got != 50 {
		t.Fatalf("authz audits page_size = %d, want 50", got)
	}
	diffIn, _, ok := buildManifestJSONBody("/udb.core.authz.services.v1.AuthzService/DiffPolicyDraft", fix)
	if !ok {
		t.Fatalf("AuthzService DiffPolicyDraft manifest JSON body was not hydrated")
	}
	diffMsg := diffIn.ProtoReflect()
	diffFields := diffMsg.Descriptor().Fields()
	actorMsg := diffMsg.Get(diffFields.ByName("actor")).Message()
	actorFields := actorMsg.Descriptor().Fields()
	if got := actorMsg.Get(actorFields.ByName("break_glass_expires_at_unix")).Int(); got != 1893456000 {
		t.Fatalf("authz gov_exp = %d, want 1893456000", got)
	}
	versionsIn, _, ok := buildManifestJSONBody("/udb.core.authz.services.v1.AuthzService/ListPolicyVersions", fix)
	if !ok {
		t.Fatalf("AuthzService ListPolicyVersions manifest JSON body was not hydrated")
	}
	versionsMsg := versionsIn.ProtoReflect()
	versionsFields := versionsMsg.Descriptor().Fields()
	if got := versionsMsg.Get(versionsFields.ByName("state")).Enum(); got == 0 {
		t.Fatalf("authz policy version state was not set from manifest enum")
	}
	createDraftIn, _, ok := buildManifestJSONBody("/udb.core.authz.services.v1.AuthzService/CreatePolicyDraft", fix)
	if !ok {
		t.Fatalf("AuthzService CreatePolicyDraft manifest JSON body was not hydrated")
	}
	createDraftMsg := createDraftIn.ProtoReflect()
	createDraftFields := createDraftMsg.Descriptor().Fields()
	if got := createDraftMsg.Get(createDraftFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("authz create policy draft tenant_id = %q, want tenant-1", got)
	}
	if got := createDraftMsg.Get(createDraftFields.ByName("project_id")).String(); got != "project-1" {
		t.Fatalf("authz create policy draft project_id = %q, want project-1", got)
	}
	if got := createDraftMsg.Get(createDraftFields.ByName("policy_set_name")).String(); got != "default" {
		t.Fatalf("authz create policy draft policy_set_name = %q, want default", got)
	}
	if got := createDraftMsg.Get(createDraftFields.ByName("title")).String(); got != "draft 1" {
		t.Fatalf("authz create policy draft title = %q, want draft 1", got)
	}
	if got := createDraftMsg.Get(createDraftFields.ByName("change_reason")).String(); got != "init" {
		t.Fatalf("authz create policy draft change_reason = %q, want init", got)
	}
	createDraftActor := createDraftMsg.Get(createDraftFields.ByName("actor")).Message()
	createDraftActorFields := createDraftActor.Descriptor().Fields()
	if got := createDraftActor.Get(createDraftActorFields.ByName("subject")).String(); got != "subject-1" {
		t.Fatalf("authz create policy draft actor.subject = %q, want subject-1", got)
	}
	if got := createDraftActor.Get(createDraftActorFields.ByName("break_glass")).Bool(); !got {
		t.Fatalf("authz create policy draft actor.break_glass = false, want true")
	}
	createDraftScopes := createDraftActor.Get(createDraftActorFields.ByName("scopes")).List()
	if createDraftScopes.Len() != 1 || createDraftScopes.Get(0).String() != "authz:policy:write" {
		t.Fatalf("authz create policy draft actor.scopes = %v, want [authz:policy:write]", createDraftScopes)
	}
	if !createDraftMsg.Has(createDraftFields.ByName("document")) {
		t.Fatalf("authz create policy draft document was not set")
	}
	fix.set("message_type", "myapp.v1.Invoice")
	fix.set("dlq_id", "dlq-1")
	fix.set("saga_id", "saga-1")
	fix.set("migration_id", "migration-1")
	fix.set("object_key", "cache-key-1")
	fix.set("mongo_collection", "invoices")
	fix.set("document_id", "document-1")
	fix.set("record_id", "record-1")
	fix.set("bucket", "bucket-1")
	fix.set("ts_table", "metrics_1")
	fix.set("event_type", "invoice.updated")
	fix.set("replay_dlq_id", "replay-dlq-1")
	fix.set("dismiss_dlq_id", "dismiss-dlq-1")
	fix.set("quarantine_dlq_id", "quarantine-dlq-1")
	fix.set("retry_saga_id", "retry-saga-1")
	fix.set("mark_saga_id", "mark-saga-1")
	fix.set("ds_policy_id", "42")
	fix.set("apply_run_id", "apply-run-1")
	fix.set("approve_run_id", "approve-run-1")
	fix.set("approval_token", "approval-token-1")
	fix.set("catalog_manifest_b64", "e30=")
	liveQueryIn, _, ok := buildManifestJSONBody("/udb.core.livequery.services.v1.LiveQueryService/Subscribe", fix)
	if !ok {
		t.Fatalf("LiveQueryService Subscribe manifest JSON body was not hydrated")
	}
	liveQueryMsg := liveQueryIn.ProtoReflect()
	liveQueryFields := liveQueryMsg.Descriptor().Fields()
	if got := liveQueryMsg.Get(liveQueryFields.ByName("message_type")).String(); got != "udb.core.lock.entity.v1.Lock" {
		t.Fatalf("livequery message_type = %q, want udb.core.lock.entity.v1.Lock", got)
	}
	if got := liveQueryMsg.Get(liveQueryFields.ByName("snapshot_limit")).Int(); got != 10 {
		t.Fatalf("livequery snapshot_limit = %d, want 10", got)
	}
	liveQueryFilters := liveQueryMsg.Get(liveQueryFields.ByName("filters")).List()
	if got := liveQueryFilters.Len(); got != 1 {
		t.Fatalf("livequery filters len = %d, want 1", got)
	}
	liveQueryFilterMsg := liveQueryFilters.Get(0).Message()
	liveQueryFilterFields := liveQueryFilterMsg.Descriptor().Fields()
	if got := liveQueryFilterMsg.Get(liveQueryFilterFields.ByName("field")).String(); got != "lock_name" {
		t.Fatalf("livequery filter field = %q, want lock_name", got)
	}
	if got := liveQueryFilterMsg.Get(liveQueryFilterFields.ByName("op")).Enum(); got == 0 {
		t.Fatalf("livequery filter op was not set from manifest enum")
	}
	if got := liveQueryFilterMsg.Get(liveQueryFilterFields.ByName("value")).String(); got != "sdk-perf-renew-lock" {
		t.Fatalf("livequery filter value = %q, want sdk-perf-renew-lock", got)
	}
	brokerIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/GetCapabilities", fix)
	if !ok {
		t.Fatalf("DataBroker GetCapabilities manifest JSON body was not hydrated")
	}
	brokerMsg := brokerIn.ProtoReflect()
	brokerFields := brokerMsg.Descriptor().Fields()
	if got := brokerMsg.Get(brokerFields.ByName("project_id")).String(); got != "project-1" {
		t.Fatalf("databroker project_id = %q, want project-1", got)
	}
	ctxMsg := brokerMsg.Get(brokerFields.ByName("context")).Message()
	ctxFields := ctxMsg.Descriptor().Fields()
	if got := ctxMsg.Get(ctxFields.ByName("tenant_id")).String(); got != "tenant-1" {
		t.Fatalf("databroker context tenant_id = %q, want tenant-1", got)
	}
	schemasIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/LookupMessageSchema", fix)
	if !ok {
		t.Fatalf("DataBroker LookupMessageSchema manifest JSON body was not hydrated")
	}
	schemasMsg := schemasIn.ProtoReflect()
	schemasFields := schemasMsg.Descriptor().Fields()
	if got := schemasMsg.Get(schemasFields.ByName("message_type")).String(); got != "myapp.v1.Invoice" {
		t.Fatalf("databroker message_type = %q, want myapp.v1.Invoice", got)
	}
	dlqIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/GetDlqEvent", fix)
	if !ok {
		t.Fatalf("DataBroker GetDlqEvent manifest JSON body was not hydrated")
	}
	dlqMsg := dlqIn.ProtoReflect()
	dlqFields := dlqMsg.Descriptor().Fields()
	if got := dlqMsg.Get(dlqFields.ByName("dlq_id")).String(); got != "dlq-1" {
		t.Fatalf("databroker dlq_id = %q, want dlq-1", got)
	}
	sagasIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/ListSagas", fix)
	if !ok {
		t.Fatalf("DataBroker ListSagas manifest JSON body was not hydrated")
	}
	sagasMsg := sagasIn.ProtoReflect()
	sagasFields := sagasMsg.Descriptor().Fields()
	if got := sagasMsg.Get(sagasFields.ByName("limit")).Int(); got != 50 {
		t.Fatalf("databroker saga list limit = %d, want 50", got)
	}
	adminIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/GetAdminSummary", fix)
	if !ok {
		t.Fatalf("DataBroker GetAdminSummary manifest JSON body was not hydrated")
	}
	adminMsg := adminIn.ProtoReflect()
	adminFields := adminMsg.Descriptor().Fields()
	if got := adminMsg.Get(adminFields.ByName("with_probes")).Bool(); got {
		t.Fatalf("databroker admin with_probes = true, want false")
	}
	adminCtx := adminMsg.Get(adminFields.ByName("context")).Message()
	adminCtxFields := adminCtx.Descriptor().Fields()
	adminScopes := adminCtx.Get(adminCtxFields.ByName("scopes")).List()
	if got := adminScopes.Len(); got != 1 || adminScopes.Get(0).String() != "udb:admin" {
		t.Fatalf("databroker admin scopes = %v, want [udb:admin]", adminScopes)
	}
	catalogVersionIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/GetCatalogVersion", fix)
	if !ok {
		t.Fatalf("DataBroker GetCatalogVersion manifest JSON body was not hydrated")
	}
	catalogVersionMsg := catalogVersionIn.ProtoReflect()
	catalogVersionFields := catalogVersionMsg.Descriptor().Fields()
	if got := catalogVersionMsg.Get(catalogVersionFields.ByName("version")).String(); got != "" {
		t.Fatalf("databroker catalog version = %q, want empty", got)
	}
	catalogVersionsIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/GetCatalogVersions", fix)
	if !ok {
		t.Fatalf("DataBroker GetCatalogVersions manifest JSON body was not hydrated")
	}
	catalogVersionsMsg := catalogVersionsIn.ProtoReflect()
	catalogVersionsFields := catalogVersionsMsg.Descriptor().Fields()
	if got := catalogVersionsMsg.Get(catalogVersionsFields.ByName("redact")).Bool(); got {
		t.Fatalf("databroker catalog versions redact = true, want false")
	}
	cdcIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/GetCdcStatus", fix)
	if !ok {
		t.Fatalf("DataBroker GetCdcStatus manifest JSON body was not hydrated")
	}
	cdcMsg := cdcIn.ProtoReflect()
	cdcFields := cdcMsg.Descriptor().Fields()
	if got := cdcMsg.Get(cdcFields.ByName("slot_name")).String(); got != "udb_cdc" {
		t.Fatalf("databroker cdc slot_name = %q, want udb_cdc", got)
	}
	pauseCdcIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/PauseCdc", fix)
	if !ok {
		t.Fatalf("DataBroker PauseCdc manifest JSON body was not hydrated")
	}
	pauseCdcMsg := pauseCdcIn.ProtoReflect()
	pauseCdcFields := pauseCdcMsg.Descriptor().Fields()
	if got := pauseCdcMsg.Get(pauseCdcFields.ByName("reason")).String(); got != "maintenance" {
		t.Fatalf("databroker pause cdc reason = %q, want maintenance", got)
	}
	resumeCdcIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/ResumeCdc", fix)
	if !ok {
		t.Fatalf("DataBroker ResumeCdc manifest JSON body was not hydrated")
	}
	resumeCdcMsg := resumeCdcIn.ProtoReflect()
	resumeCdcFields := resumeCdcMsg.Descriptor().Fields()
	if got := resumeCdcMsg.Get(resumeCdcFields.ByName("reason")).String(); got != "resume" {
		t.Fatalf("databroker resume cdc reason = %q, want resume", got)
	}
	stepDownCdcIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/StepDownCdcLeader", fix)
	if !ok {
		t.Fatalf("DataBroker StepDownCdcLeader manifest JSON body was not hydrated")
	}
	stepDownCdcMsg := stepDownCdcIn.ProtoReflect()
	stepDownCdcFields := stepDownCdcMsg.Descriptor().Fields()
	if got := stepDownCdcMsg.Get(stepDownCdcFields.ByName("reason")).String(); got != "failover" {
		t.Fatalf("databroker step-down cdc reason = %q, want failover", got)
	}
	migrationIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/GetMigrationStatus", fix)
	if !ok {
		t.Fatalf("DataBroker GetMigrationStatus manifest JSON body was not hydrated")
	}
	migrationMsg := migrationIn.ProtoReflect()
	migrationFields := migrationMsg.Descriptor().Fields()
	if got := migrationMsg.Get(migrationFields.ByName("run_id")).String(); got != "migration-1" {
		t.Fatalf("databroker migration run_id = %q, want migration-1", got)
	}
	migrationRunsIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/ListMigrationRuns", fix)
	if !ok {
		t.Fatalf("DataBroker ListMigrationRuns manifest JSON body was not hydrated")
	}
	migrationRunsMsg := migrationRunsIn.ProtoReflect()
	migrationRunsFields := migrationRunsMsg.Descriptor().Fields()
	if got := migrationRunsMsg.Get(migrationRunsFields.ByName("limit")).Int(); got != 50 {
		t.Fatalf("databroker migration list limit = %d, want 50", got)
	}
	projectsIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/ListProjects", fix)
	if !ok {
		t.Fatalf("DataBroker ListProjects manifest JSON body was not hydrated")
	}
	projectsMsg := projectsIn.ProtoReflect()
	projectsFields := projectsMsg.Descriptor().Fields()
	if got := projectsMsg.Get(projectsFields.ByName("limit")).Int(); got != 50 {
		t.Fatalf("databroker project list limit = %d, want 50", got)
	}
	resourcesIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/ListResources", fix)
	if !ok {
		t.Fatalf("DataBroker ListResources manifest JSON body was not hydrated")
	}
	resourcesMsg := resourcesIn.ProtoReflect()
	resourcesFields := resourcesMsg.Descriptor().Fields()
	if got := resourcesMsg.Get(resourcesFields.ByName("backend")).String(); got != "mongodb" {
		t.Fatalf("databroker resources backend = %q, want mongodb", got)
	}
	auditIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/ListAdminAuditLogs", fix)
	if !ok {
		t.Fatalf("DataBroker ListAdminAuditLogs manifest JSON body was not hydrated")
	}
	auditMsg := auditIn.ProtoReflect()
	auditFields := auditMsg.Descriptor().Fields()
	if got := auditMsg.Get(auditFields.ByName("limit")).Int(); got != 50 {
		t.Fatalf("databroker audit list limit = %d, want 50", got)
	}
	verifyIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/VerifyAdminAuditLog", fix)
	if !ok {
		t.Fatalf("DataBroker VerifyAdminAuditLog manifest JSON body was not hydrated")
	}
	verifyMsg := verifyIn.ProtoReflect()
	verifyFields := verifyMsg.Descriptor().Fields()
	if got := verifyMsg.Get(verifyFields.ByName("limit")).Int(); got != 0 {
		t.Fatalf("databroker audit verify limit = %d, want 0", got)
	}
	vectorIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/VectorSearch", fix)
	if !ok {
		t.Fatalf("DataBroker VectorSearch manifest JSON body was not hydrated")
	}
	vectorMsg := vectorIn.ProtoReflect()
	vectorFields := vectorMsg.Descriptor().Fields()
	if got := vectorMsg.Get(vectorFields.ByName("collection")).String(); got != "sdk_live_records" {
		t.Fatalf("databroker vector collection = %q, want sdk_live_records", got)
	}
	if got := vectorMsg.Get(vectorFields.ByName("vector")).List().Len(); got != 3 {
		t.Fatalf("databroker vector len = %d, want 3", got)
	}
	hybridIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/VectorHybridSearch", fix)
	if !ok {
		t.Fatalf("DataBroker VectorHybridSearch manifest JSON body was not hydrated")
	}
	hybridMsg := hybridIn.ProtoReflect()
	hybridFields := hybridMsg.Descriptor().Fields()
	if got := hybridMsg.Get(hybridFields.ByName("text_query")).String(); got != "hello" {
		t.Fatalf("databroker hybrid text_query = %q, want hello", got)
	}
	brokerCacheIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/CacheGet", fix)
	if !ok {
		t.Fatalf("DataBroker CacheGet manifest JSON body was not hydrated")
	}
	brokerCacheMsg := brokerCacheIn.ProtoReflect()
	brokerCacheFields := brokerCacheMsg.Descriptor().Fields()
	if got := brokerCacheMsg.Get(brokerCacheFields.ByName("key")).String(); got != "cache-key-1" {
		t.Fatalf("databroker cache key = %q, want cache-key-1", got)
	}
	cacheResource := brokerCacheMsg.Get(brokerCacheFields.ByName("resource")).Message()
	cacheResourceFields := cacheResource.Descriptor().Fields()
	if got := cacheResource.Get(cacheResourceFields.ByName("backend")).String(); got != "redis" {
		t.Fatalf("databroker cache backend = %q, want redis", got)
	}
	cacheScanIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/CacheScan", fix)
	if !ok {
		t.Fatalf("DataBroker CacheScan manifest JSON body was not hydrated")
	}
	cacheScanMsg := cacheScanIn.ProtoReflect()
	cacheScanFields := cacheScanMsg.Descriptor().Fields()
	if got := cacheScanMsg.Get(cacheScanFields.ByName("limit")).Int(); got != 50 {
		t.Fatalf("databroker cache scan limit = %d, want 50", got)
	}
	documentIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/DocumentGet", fix)
	if !ok {
		t.Fatalf("DataBroker DocumentGet manifest JSON body was not hydrated")
	}
	documentMsg := documentIn.ProtoReflect()
	documentFields := documentMsg.Descriptor().Fields()
	if got := documentMsg.Get(documentFields.ByName("document_id")).String(); got != "document-1" {
		t.Fatalf("databroker document_id = %q, want document-1", got)
	}
	documentResource := documentMsg.Get(documentFields.ByName("resource")).Message()
	documentResourceFields := documentResource.Descriptor().Fields()
	if got := documentResource.Get(documentResourceFields.ByName("resource_name")).String(); got != "invoices" {
		t.Fatalf("databroker document resource_name = %q, want invoices", got)
	}
	documentFindIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/DocumentFind", fix)
	if !ok {
		t.Fatalf("DataBroker DocumentFind manifest JSON body was not hydrated")
	}
	documentFindMsg := documentFindIn.ProtoReflect()
	documentFindFields := documentFindMsg.Descriptor().Fields()
	if got := documentFindMsg.Get(documentFindFields.ByName("limit")).Int(); got != 10 {
		t.Fatalf("databroker document find limit = %d, want 10", got)
	}
	graphIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/GraphQuery", fix)
	if !ok {
		t.Fatalf("DataBroker GraphQuery manifest JSON body was not hydrated")
	}
	graphMsg := graphIn.ProtoReflect()
	graphFields := graphMsg.Descriptor().Fields()
	if got := graphMsg.Get(graphFields.ByName("read_only")).Bool(); !got {
		t.Fatalf("databroker graph read_only = false, want true")
	}
	analyticalIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/AnalyticalQuery", fix)
	if !ok {
		t.Fatalf("DataBroker AnalyticalQuery manifest JSON body was not hydrated")
	}
	analyticalMsg := analyticalIn.ProtoReflect()
	analyticalFields := analyticalMsg.Descriptor().Fields()
	if got := analyticalMsg.Get(analyticalFields.ByName("query")).String(); got != "SELECT 1" {
		t.Fatalf("databroker analytical query = %q, want SELECT 1", got)
	}
	selectIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/Select", fix)
	if !ok {
		t.Fatalf("DataBroker Select manifest JSON body was not hydrated")
	}
	selectMsg := selectIn.ProtoReflect()
	selectFields := selectMsg.Descriptor().Fields()
	if got := selectMsg.Get(selectFields.ByName("message_type")).String(); got != "myapp.v1.Invoice" {
		t.Fatalf("databroker select message_type = %q, want myapp.v1.Invoice", got)
	}
	if got := selectMsg.Get(selectFields.ByName("limit")).Int(); got != 10 {
		t.Fatalf("databroker select limit = %d, want 10", got)
	}
	selectV2In, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/SelectV2", fix)
	if !ok {
		t.Fatalf("DataBroker SelectV2 manifest JSON body was not hydrated")
	}
	selectV2Msg := selectV2In.ProtoReflect()
	selectV2Fields := selectV2Msg.Descriptor().Fields()
	if got := selectV2Msg.Get(selectV2Fields.ByName("limit")).Int(); got != 10 {
		t.Fatalf("databroker select_v2 limit = %d, want 10", got)
	}
	objectIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/GetObject", fix)
	if !ok {
		t.Fatalf("DataBroker GetObject manifest JSON body was not hydrated")
	}
	objectMsg := objectIn.ProtoReflect()
	objectFields := objectMsg.Descriptor().Fields()
	if got := objectMsg.Get(objectFields.ByName("bucket")).String(); got != "bucket-1" {
		t.Fatalf("databroker object bucket = %q, want bucket-1", got)
	}
	tsIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/TimeSeriesQuery", fix)
	if !ok {
		t.Fatalf("DataBroker TimeSeriesQuery manifest JSON body was not hydrated")
	}
	tsMsg := tsIn.ProtoReflect()
	tsFields := tsMsg.Descriptor().Fields()
	tsResource := tsMsg.Get(tsFields.ByName("resource")).Message()
	tsResourceFields := tsResource.Descriptor().Fields()
	if got := tsResource.Get(tsResourceFields.ByName("resource_name")).String(); got != "metrics_1" {
		t.Fatalf("databroker timeseries resource_name = %q, want metrics_1", got)
	}
	previewIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/PreviewCdcRedaction", fix)
	if !ok {
		t.Fatalf("DataBroker PreviewCdcRedaction manifest JSON body was not hydrated")
	}
	previewMsg := previewIn.ProtoReflect()
	previewFields := previewMsg.Descriptor().Fields()
	if got := string(previewMsg.Get(previewFields.ByName("payload_json")).Bytes()); got != "{}" {
		t.Fatalf("databroker preview payload_json = %q, want {}", got)
	}
	driftIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/ScanProjectionDrift", fix)
	if !ok {
		t.Fatalf("DataBroker ScanProjectionDrift manifest JSON body was not hydrated")
	}
	driftMsg := driftIn.ProtoReflect()
	driftFields := driftMsg.Descriptor().Fields()
	if got := driftMsg.Get(driftFields.ByName("rows_per_target")).Int(); got != 100 {
		t.Fatalf("databroker drift rows_per_target = %d, want 100", got)
	}
	urlIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/GeneratePresignedUrl", fix)
	if !ok {
		t.Fatalf("DataBroker GeneratePresignedUrl manifest JSON body was not hydrated")
	}
	urlMsg := urlIn.ProtoReflect()
	urlFields := urlMsg.Descriptor().Fields()
	if got := urlMsg.Get(urlFields.ByName("method")).String(); got != "GET" {
		t.Fatalf("databroker presigned method = %q, want GET", got)
	}
	if got := urlMsg.Get(urlFields.ByName("ttl_seconds")).Int(); got != 300 {
		t.Fatalf("databroker presigned ttl_seconds = %d, want 300", got)
	}
	multipartIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/InitiateMultipartUpload", fix)
	if !ok {
		t.Fatalf("DataBroker InitiateMultipartUpload manifest JSON body was not hydrated")
	}
	multipartMsg := multipartIn.ProtoReflect()
	multipartFields := multipartMsg.Descriptor().Fields()
	if got := multipartMsg.Get(multipartFields.ByName("part_count")).Int(); got != 1 {
		t.Fatalf("databroker multipart part_count = %d, want 1", got)
	}
	docUpsertIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/DocumentUpsert", fix)
	if !ok {
		t.Fatalf("DataBroker DocumentUpsert manifest JSON body was not hydrated")
	}
	docUpsertMsg := docUpsertIn.ProtoReflect()
	docUpsertFields := docUpsertMsg.Descriptor().Fields()
	if got := docUpsertMsg.Get(docUpsertFields.ByName("document_id")).String(); got != "document-1" {
		t.Fatalf("databroker document upsert id = %q, want document-1", got)
	}
	graphMutateIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/GraphMutate", fix)
	if !ok {
		t.Fatalf("DataBroker GraphMutate manifest JSON body was not hydrated")
	}
	graphMutateMsg := graphMutateIn.ProtoReflect()
	graphMutateFields := graphMutateMsg.Descriptor().Fields()
	if got := graphMutateMsg.Get(graphMutateFields.ByName("query")).String(); got != `CREATE (n:Node {id:$id})` {
		t.Fatalf("databroker graph mutate query = %q, want CREATE", got)
	}
	vectorUpsertIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/VectorUpsert", fix)
	if !ok {
		t.Fatalf("DataBroker VectorUpsert manifest JSON body was not hydrated")
	}
	vectorUpsertMsg := vectorUpsertIn.ProtoReflect()
	vectorUpsertFields := vectorUpsertMsg.Descriptor().Fields()
	if got := vectorUpsertMsg.Get(vectorUpsertFields.ByName("points")).List().Len(); got != 1 {
		t.Fatalf("databroker vector upsert points len = %d, want 1", got)
	}
	viewIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/CreateMaterializedView", fix)
	if !ok {
		t.Fatalf("DataBroker CreateMaterializedView manifest JSON body was not hydrated")
	}
	viewMsg := viewIn.ProtoReflect()
	viewFields := viewMsg.Descriptor().Fields()
	if got := viewMsg.Get(viewFields.ByName("with_data")).Bool(); !got {
		t.Fatalf("databroker materialized view with_data = false, want true")
	}
	planIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/PlanMigration", fix)
	if !ok {
		t.Fatalf("DataBroker PlanMigration manifest JSON body was not hydrated")
	}
	planMsg := planIn.ProtoReflect()
	planFields := planMsg.Descriptor().Fields()
	if got := planMsg.Get(planFields.ByName("dry_run")).Bool(); !got {
		t.Fatalf("databroker plan migration dry_run = false, want true")
	}
	cacheDeleteIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/CacheDelete", fix)
	if !ok {
		t.Fatalf("DataBroker CacheDelete manifest JSON body was not hydrated")
	}
	cacheDeleteMsg := cacheDeleteIn.ProtoReflect()
	cacheDeleteFields := cacheDeleteMsg.Descriptor().Fields()
	if got := cacheDeleteMsg.Get(cacheDeleteFields.ByName("key")).String(); got != "cache-key-1" {
		t.Fatalf("databroker cache delete key = %q, want cache-key-1", got)
	}
	replayIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/ReplayDlqEvent", fix)
	if !ok {
		t.Fatalf("DataBroker ReplayDlqEvent manifest JSON body was not hydrated")
	}
	replayMsg := replayIn.ProtoReflect()
	replayFields := replayMsg.Descriptor().Fields()
	if got := replayMsg.Get(replayFields.ByName("dlq_id")).String(); got != "replay-dlq-1" {
		t.Fatalf("databroker replay dlq_id = %q, want replay-dlq-1", got)
	}
	dismissIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/DismissDlqEvent", fix)
	if !ok {
		t.Fatalf("DataBroker DismissDlqEvent manifest JSON body was not hydrated")
	}
	dismissMsg := dismissIn.ProtoReflect()
	dismissFields := dismissMsg.Descriptor().Fields()
	if got := dismissMsg.Get(dismissFields.ByName("dlq_id")).String(); got != "dismiss-dlq-1" {
		t.Fatalf("databroker dismiss dlq_id = %q, want dismiss-dlq-1", got)
	}
	quarantineIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/QuarantineDlqEvent", fix)
	if !ok {
		t.Fatalf("DataBroker QuarantineDlqEvent manifest JSON body was not hydrated")
	}
	quarantineMsg := quarantineIn.ProtoReflect()
	quarantineFields := quarantineMsg.Descriptor().Fields()
	if got := quarantineMsg.Get(quarantineFields.ByName("dlq_id")).String(); got != "quarantine-dlq-1" {
		t.Fatalf("databroker quarantine dlq_id = %q, want quarantine-dlq-1", got)
	}
	retrySagaIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/RetrySagaCompensation", fix)
	if !ok {
		t.Fatalf("DataBroker RetrySagaCompensation manifest JSON body was not hydrated")
	}
	retrySagaMsg := retrySagaIn.ProtoReflect()
	retrySagaFields := retrySagaMsg.Descriptor().Fields()
	if got := retrySagaMsg.Get(retrySagaFields.ByName("reason")).String(); got != "retry" {
		t.Fatalf("databroker retry saga reason = %q, want retry", got)
	}
	markSagaIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/MarkSagaReviewed", fix)
	if !ok {
		t.Fatalf("DataBroker MarkSagaReviewed manifest JSON body was not hydrated")
	}
	markSagaMsg := markSagaIn.ProtoReflect()
	markSagaFields := markSagaMsg.Descriptor().Fields()
	if got := markSagaMsg.Get(markSagaFields.ByName("reason")).String(); got != "reviewed" {
		t.Fatalf("databroker mark saga reason = %q, want reviewed", got)
	}
	deletePolicyIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/DeletePolicy", fix)
	if !ok {
		t.Fatalf("DataBroker DeletePolicy manifest JSON body was not hydrated")
	}
	deletePolicyMsg := deletePolicyIn.ProtoReflect()
	deletePolicyFields := deletePolicyMsg.Descriptor().Fields()
	if got := deletePolicyMsg.Get(deletePolicyFields.ByName("policy_id")).Int(); got != 42 {
		t.Fatalf("databroker delete policy_id = %d, want 42", got)
	}
	reloadIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/ReloadPolicies", fix)
	if !ok {
		t.Fatalf("DataBroker ReloadPolicies manifest JSON body was not hydrated")
	}
	reloadMsg := reloadIn.ProtoReflect()
	reloadFields := reloadMsg.Descriptor().Fields()
	if got := reloadMsg.Get(reloadFields.ByName("project_id")).String(); got != "project-1" {
		t.Fatalf("databroker reload project_id = %q, want project-1", got)
	}
	applyIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/ApplyMigration", fix)
	if !ok {
		t.Fatalf("DataBroker ApplyMigration manifest JSON body was not hydrated")
	}
	applyMsg := applyIn.ProtoReflect()
	applyFields := applyMsg.Descriptor().Fields()
	if got := applyMsg.Get(applyFields.ByName("approval_token")).String(); got != "approval-token-1" {
		t.Fatalf("databroker apply approval_token = %q, want approval-token-1", got)
	}
	approveIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/ApproveMigrationPlan", fix)
	if !ok {
		t.Fatalf("DataBroker ApproveMigrationPlan manifest JSON body was not hydrated")
	}
	approveMsg := approveIn.ProtoReflect()
	approveFields := approveMsg.Descriptor().Fields()
	if got := approveMsg.Get(approveFields.ByName("run_id")).String(); got != "approve-run-1" {
		t.Fatalf("databroker approve run_id = %q, want approve-run-1", got)
	}
	batchSelectIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/BatchSelect", fix)
	if !ok {
		t.Fatalf("DataBroker BatchSelect manifest JSON body was not hydrated")
	}
	batchSelectMsg := batchSelectIn.ProtoReflect()
	batchSelectFields := batchSelectMsg.Descriptor().Fields()
	if got := batchSelectMsg.Get(batchSelectFields.ByName("limit")).Int(); got != 10 {
		t.Fatalf("databroker batch select limit = %d, want 10", got)
	}
	batchUpsertIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/BatchUpsert", fix)
	if !ok {
		t.Fatalf("DataBroker BatchUpsert manifest JSON body was not hydrated")
	}
	batchUpsertMsg := batchUpsertIn.ProtoReflect()
	batchUpsertFields := batchUpsertMsg.Descriptor().Fields()
	if got := batchUpsertMsg.Get(batchUpsertFields.ByName("return_record")).Bool(); !got {
		t.Fatalf("databroker batch upsert return_record = false, want true")
	}
	cacheSetIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/CacheSet", fix)
	if !ok {
		t.Fatalf("DataBroker CacheSet manifest JSON body was not hydrated")
	}
	cacheSetMsg := cacheSetIn.ProtoReflect()
	cacheSetFields := cacheSetMsg.Descriptor().Fields()
	if got := string(cacheSetMsg.Get(cacheSetFields.ByName("value")).Bytes()); got != "perf" {
		t.Fatalf("databroker cache set value = %q, want perf", got)
	}
	deleteIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/Delete", fix)
	if !ok {
		t.Fatalf("DataBroker Delete manifest JSON body was not hydrated")
	}
	deleteMsg := deleteIn.ProtoReflect()
	deleteFields := deleteMsg.Descriptor().Fields()
	if got := deleteMsg.Get(deleteFields.ByName("message_type")).String(); got != "myapp.v1.Invoice" {
		t.Fatalf("databroker delete message_type = %q, want myapp.v1.Invoice", got)
	}
	deleteFilterField := deleteFields.ByName("filter")
	deleteFilterMapField := deleteFilterField.Message().Fields().ByName("fields")
	deleteFilterStringField := deleteFilterMapField.MapValue().Message().Fields().ByName("string_value")
	deleteFilterFields := deleteMsg.Get(deleteFilterField).Message().Get(deleteFilterMapField).Map()
	if got := deleteFilterFields.Get(protoreflect.ValueOfString("record_id").MapKey()).Message().
		Get(deleteFilterStringField).String(); got != "record-1" {
		t.Fatalf("databroker delete filter record_id = %q, want record-1", got)
	}
	if got := deleteFilterFields.Get(protoreflect.ValueOfString("tenant_id").MapKey()).Message().
		Get(deleteFilterStringField).String(); got != "tenant-1" {
		t.Fatalf("databroker delete filter tenant_id = %q, want tenant-1", got)
	}
	if got := deleteFilterFields.Get(protoreflect.ValueOfString("project_id").MapKey()).Message().
		Get(deleteFilterStringField).String(); got != "project-1" {
		t.Fatalf("databroker delete filter project_id = %q, want project-1", got)
	}
	documentDeleteIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/DocumentDelete", fix)
	if !ok {
		t.Fatalf("DataBroker DocumentDelete manifest JSON body was not hydrated")
	}
	documentDeleteMsg := documentDeleteIn.ProtoReflect()
	documentDeleteFields := documentDeleteMsg.Descriptor().Fields()
	if got := documentDeleteMsg.Get(documentDeleteFields.ByName("document_id")).String(); got != "document-1" {
		t.Fatalf("databroker document delete id = %q, want document-1", got)
	}
	ensureBaselineIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/EnsureBaseline", fix)
	if !ok {
		t.Fatalf("DataBroker EnsureBaseline manifest JSON body was not hydrated")
	}
	ensureBaselineMsg := ensureBaselineIn.ProtoReflect()
	ensureBaselineFields := ensureBaselineMsg.Descriptor().Fields()
	ensureBaselineCtx := ensureBaselineMsg.Get(ensureBaselineFields.ByName("context")).Message()
	ensureBaselineCtxFields := ensureBaselineCtx.Descriptor().Fields()
	if got := ensureBaselineCtx.Get(ensureBaselineCtxFields.ByName("scopes")).List().Get(0).String(); got != "udb:admin" {
		t.Fatalf("databroker ensure baseline scope = %q, want udb:admin", got)
	}
	ensureProjectIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/EnsureProject", fix)
	if !ok {
		t.Fatalf("DataBroker EnsureProject manifest JSON body was not hydrated")
	}
	ensureProjectMsg := ensureProjectIn.ProtoReflect()
	ensureProjectFields := ensureProjectMsg.Descriptor().Fields()
	if got := ensureProjectMsg.Get(ensureProjectFields.ByName("cdc_topic_prefix")).String(); got != "project-1." {
		t.Fatalf("databroker ensure project prefix = %q, want project-1.", got)
	}
	ensureResourceIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/EnsureResource", fix)
	if !ok {
		t.Fatalf("DataBroker EnsureResource manifest JSON body was not hydrated")
	}
	ensureResourceMsg := ensureResourceIn.ProtoReflect()
	ensureResourceFields := ensureResourceMsg.Descriptor().Fields()
	if got := ensureResourceMsg.Get(ensureResourceFields.ByName("resource_name")).String(); got != "invoices" {
		t.Fatalf("databroker ensure resource_name = %q, want invoices", got)
	}
	dropResourceIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/DropResource", fix)
	if !ok {
		t.Fatalf("DataBroker DropResource manifest JSON body was not hydrated")
	}
	dropResourceMsg := dropResourceIn.ProtoReflect()
	dropResourceFields := dropResourceMsg.Descriptor().Fields()
	if got := dropResourceMsg.Get(dropResourceFields.ByName("spec_json")).String(); got != `{"udb_allow_rls_bypass":true}` {
		t.Fatalf("databroker drop spec_json = %q, want allow rls bypass", got)
	}
	genericIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/GenericDispatch", fix)
	if !ok {
		t.Fatalf("DataBroker GenericDispatch manifest JSON body was not hydrated")
	}
	genericMsg := genericIn.ProtoReflect()
	genericFields := genericMsg.Descriptor().Fields()
	if got := genericMsg.Get(genericFields.ByName("operation")).String(); got != "ping" {
		t.Fatalf("databroker generic operation = %q, want ping", got)
	}
	if got := genericMsg.Get(genericFields.ByName("spec_json")).String(); got != "{}" {
		t.Fatalf("databroker generic spec_json = %q, want {}", got)
	}
	publishIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/PublishCDC", fix)
	if !ok {
		t.Fatalf("DataBroker PublishCDC manifest JSON body was not hydrated")
	}
	publishMsg := publishIn.ProtoReflect()
	publishFields := publishMsg.Descriptor().Fields()
	if got := publishMsg.Get(publishFields.ByName("topic_pattern")).String(); got != "*" {
		t.Fatalf("databroker publish topic_pattern = %q, want *", got)
	}
	upsertIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/Upsert", fix)
	if !ok {
		t.Fatalf("DataBroker Upsert manifest JSON body was not hydrated")
	}
	upsertMsg := upsertIn.ProtoReflect()
	upsertFields := upsertMsg.Descriptor().Fields()
	if got := upsertMsg.Get(upsertFields.ByName("return_record")).Bool(); !got {
		t.Fatalf("databroker upsert return_record = false, want true")
	}
	bulkCasIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/BulkCas", fix)
	if !ok {
		t.Fatalf("DataBroker BulkCas manifest JSON body was not hydrated")
	}
	bulkCasMsg := bulkCasIn.ProtoReflect()
	bulkCasFields := bulkCasMsg.Descriptor().Fields()
	if got := bulkCasMsg.Get(bulkCasFields.ByName("items")).List().Len(); got != 1 {
		t.Fatalf("databroker bulk_cas items len = %d, want 1", got)
	}
	if got := bulkCasMsg.Get(bulkCasFields.ByName("max_rows")).Int(); got != 10 {
		t.Fatalf("databroker bulk_cas max_rows = %d, want 10", got)
	}
	vectorBatchIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/VectorBatchUpsert", fix)
	if !ok {
		t.Fatalf("DataBroker VectorBatchUpsert manifest JSON body was not hydrated")
	}
	vectorBatchMsg := vectorBatchIn.ProtoReflect()
	vectorBatchFields := vectorBatchMsg.Descriptor().Fields()
	if got := vectorBatchMsg.Get(vectorBatchFields.ByName("points")).List().Len(); got != 1 {
		t.Fatalf("databroker vector batch points len = %d, want 1", got)
	}
	activateIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/ActivateCatalog", fix)
	if !ok {
		t.Fatalf("DataBroker ActivateCatalog manifest JSON body was not hydrated")
	}
	activateMsg := activateIn.ProtoReflect()
	activateFields := activateMsg.Descriptor().Fields()
	if got := activateMsg.Get(activateFields.ByName("project_id")).String(); got != "project-1" {
		t.Fatalf("databroker activate project_id = %q, want project-1", got)
	}
	beginTxIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/BeginTx", fix)
	if !ok {
		t.Fatalf("DataBroker BeginTx manifest JSON body was not hydrated")
	}
	beginTxMsg := beginTxIn.ProtoReflect()
	beginTxFields := beginTxMsg.Descriptor().Fields()
	if got := beginTxMsg.Get(beginTxFields.ByName("operation")).String(); got != "upsert" {
		t.Fatalf("databroker begin tx operation = %q, want upsert", got)
	}
	enqueueIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/EnqueueOutboxEvent", fix)
	if !ok {
		t.Fatalf("DataBroker EnqueueOutboxEvent manifest JSON body was not hydrated")
	}
	enqueueMsg := enqueueIn.ProtoReflect()
	enqueueFields := enqueueMsg.Descriptor().Fields()
	if got := enqueueMsg.Get(enqueueFields.ByName("topic")).String(); got != "invoice.updated" {
		t.Fatalf("databroker enqueue topic = %q, want invoice.updated", got)
	}
	putObjectIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/PutObject", fix)
	if !ok {
		t.Fatalf("DataBroker PutObject manifest JSON body was not hydrated")
	}
	putObjectMsg := putObjectIn.ProtoReflect()
	putObjectFields := putObjectMsg.Descriptor().Fields()
	if got := string(putObjectMsg.Get(putObjectFields.ByName("data")).Bytes()); got != "perf" {
		t.Fatalf("databroker put object data = %q, want perf", got)
	}
	putPolicyIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/PutPolicy", fix)
	if !ok {
		t.Fatalf("DataBroker PutPolicy manifest JSON body was not hydrated")
	}
	putPolicyMsg := putPolicyIn.ProtoReflect()
	putPolicyFields := putPolicyMsg.Descriptor().Fields()
	policyMsg := putPolicyMsg.Get(putPolicyFields.ByName("policy")).Message()
	policyFields := policyMsg.Descriptor().Fields()
	if got := policyMsg.Get(policyFields.ByName("effect")).String(); got != "allow" {
		t.Fatalf("databroker put policy effect = %q, want allow", got)
	}
	rollbackIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/RollbackCatalog", fix)
	if !ok {
		t.Fatalf("DataBroker RollbackCatalog manifest JSON body was not hydrated")
	}
	rollbackMsg := rollbackIn.ProtoReflect()
	rollbackFields := rollbackMsg.Descriptor().Fields()
	if got := rollbackMsg.Get(rollbackFields.ByName("project_id")).String(); got != "project-1" {
		t.Fatalf("databroker rollback project_id = %q, want project-1", got)
	}
	stageIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/StageCatalog", fix)
	if !ok {
		t.Fatalf("DataBroker StageCatalog manifest JSON body was not hydrated")
	}
	stageMsg := stageIn.ProtoReflect()
	stageFields := stageMsg.Descriptor().Fields()
	if got := string(stageMsg.Get(stageFields.ByName("manifest_json")).Bytes()); got != "{}" {
		t.Fatalf("databroker stage manifest_json = %q, want {}", got)
	}
	tsWriteIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/TimeSeriesWrite", fix)
	if !ok {
		t.Fatalf("DataBroker TimeSeriesWrite manifest JSON body was not hydrated")
	}
	tsWriteMsg := tsWriteIn.ProtoReflect()
	tsWriteFields := tsWriteMsg.Descriptor().Fields()
	if got := tsWriteMsg.Get(tsWriteFields.ByName("points")).List().Len(); got != 1 {
		t.Fatalf("databroker time series write points len = %d, want 1", got)
	}
	validateIn, _, ok := buildManifestJSONBody("/udb.services.v1.DataBroker/ValidateCatalog", fix)
	if !ok {
		t.Fatalf("DataBroker ValidateCatalog manifest JSON body was not hydrated")
	}
	validateMsg := validateIn.ProtoReflect()
	validateFields := validateMsg.Descriptor().Fields()
	if got := validateMsg.Get(validateFields.ByName("reason")).String(); got != "validate" {
		t.Fatalf("databroker validate reason = %q, want validate", got)
	}
}
