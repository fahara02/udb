package udbclient

// Perf-harness SEED phase + fixture map.
//
// The perf run (live_perf_test.go) measures REAL successful-call latency for the
// whole RPC surface. To do that, every reference/ID field in a request must point
// at an entity that actually exists. This file builds those entities up front —
// REUSING the same create/setup calls the conformance suite (live_conformance_test.go,
// live_native_services_test.go) already proves succeed — and records their real
// identifiers into a perfFixtures map keyed by SEMANTIC field name (user_id,
// tenant_id, role, policy_id, file_id, asset_id, room_id, subject, object, …).
//
// populateProbeMessage (live_surface_probe_test.go) consults this map first, so a
// reflectively-built request for, say, AuthzService/GetRole gets the seeded
// role_id and drives the GetRole SUCCESS path instead of a NotFound.
//
// Seeding runs in DEPENDENCY ORDER (a user before a role assignment before a
// notification; a file before an asset; a room before a peer before a track).
// Everything is namespaced by a per-run suffix so reruns are idempotent, and
// perfSeed returns a cleanup closure that removes the disposable entities.

import (
	"context"
	"strings"
	"testing"
	"time"

	analyticspb "github.com/fahara02/udb/sdk/go/gen/udb/core/analytics/services/v1"
	apikeypb "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/services/v1"
	assetpb "github.com/fahara02/udb/sdk/go/gen/udb/core/asset/services/v1"
	authnpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	authzentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/entity/v1"
	authzpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
	commonpb "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	idpentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/idp/entity/v1"
	idppb "github.com/fahara02/udb/sdk/go/gen/udb/core/idp/services/v1"
	notifentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/notification/entity/v1"
	notifpb "github.com/fahara02/udb/sdk/go/gen/udb/core/notification/services/v1"
	storagepb "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/services/v1"
	webrtcpb "github.com/fahara02/udb/sdk/go/gen/udb/core/webrtc/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
)

// perfFixtures maps a semantic field name → a real seeded value. lookup resolves
// a proto field name (already lower-cased) against it, preferring an exact match
// then a suffix match (so "user_id", "assigned_by", "created_by" all reach the
// seeded user UUID when registered under those keys).
type perfFixtures struct {
	m map[string]string
}

func newPerfFixtures() *perfFixtures { return &perfFixtures{m: map[string]string{}} }

func (f *perfFixtures) set(key, val string) {
	if val != "" {
		f.m[strings.ToLower(key)] = val
	}
}

// lookup returns the seeded value for a proto field name. It tries an exact match
// first, then matches a registered key as a suffix of the field name (e.g. field
// "definition_id" matches key "definition_id"; field "approved_by" matches key
// "approved_by"). This keeps resolution explicit — only names we deliberately
// seeded resolve, everything else falls through to the generic scalar.
func (f *perfFixtures) lookup(field string) (string, bool) {
	if v, ok := f.m[field]; ok {
		return v, true
	}
	for k, v := range f.m {
		if field == k || strings.HasSuffix(field, "_"+k) {
			return v, true
		}
	}
	return "", false
}

// perfSeedResult carries the fixture map plus the seeded record's primary key (so
// the CDC event-driver and the data-plane RPCs target the same real row) and a
// cleanup func.
type perfSeedResult struct {
	fix      *perfFixtures
	recordID string // seeded SdkLiveRecord primary key (for Upsert/Select/CDC)
	cleanup  func()
}

// perfSeed creates real, disposable entities across the services the perf run
// touches and records their identifiers. broker is the data-plane connection
// (DataBroker); authConn is the control-plane connection (everything else).
// uuidTenant is the canonical tenant UUID used by the UUID-strict services
// (storage/asset/webrtc); base/native are the matching outgoing contexts.
func perfSeed(t *testing.T, ctx context.Context, broker servicesv1.DataBrokerClient, brokerCtx context.Context,
	authConn grpc.ClientConnInterface, base context.Context, nativeCtxFn func() context.Context,
	tenant, project, uuidTenant string) perfSeedResult {
	t.Helper()
	fix := newPerfFixtures()
	suffix := strings.NewReplacer(".", "", ":", "", "+", "").Replace(time.Now().UTC().Format("20060102150405.000000000"))
	var cleanups []func()
	addCleanup := func(fn func()) { cleanups = append(cleanups, fn) }

	// Always-known scalars.
	fix.set("tenant_id", tenant)
	fix.set("tenant", tenant)
	fix.set("project_id", project)
	fix.set("project", project)
	fix.set("domain", tenant)
	fix.set("message_type", liveMessageType)

	rc := liveRequestContext(tenant, project, "go.live.perf.seed")

	// ── DataBroker: a real SdkLiveRecord row (drives Upsert/Select/Delete + CDC) ──
	recordID := "go-perf-" + suffix
	if _, err := broker.Upsert(brokerCtx, &entityv1.UpsertRequest{
		Context:        rc,
		MessageType:    liveMessageType,
		RecordJson:     liveRecordJSON(t, recordID, tenant, project, "go-perf-lk-"+suffix, "perf-seed", 1),
		ConflictFields: []string{"record_id"},
	}); err != nil {
		t.Logf("perf seed: SdkLiveRecord upsert failed (Select/CDC may be empty): %v", err)
	}
	fix.set("record_id", recordID)

	// A real project for ListProjects / project-scoped reads.
	projID := "sdklive_perf_" + strings.NewReplacer("-", "", ".", "").Replace(suffix)
	if _, err := broker.EnsureProject(brokerCtx, &entityv1.EnsureProjectRequest{Context: rc, ProjectId: projID, Name: "SDK Perf Project"}); err == nil {
		// project_id intentionally NOT overridden to projID — the rest of the
		// surface expects the caller's project; this just makes EnsureProject real.
	}

	// A real MinIO bucket + object so GetObject (server-streaming) and the object
	// RPCs run their success path.
	bucket := liveEnv("UDB_LIVE_S3_BUCKET", "udb-live-sdk")
	objectKey := "go-perf/" + suffix + ".txt"
	_, _ = broker.EnsureResource(brokerCtx, &entityv1.ResourceAdminRequest{Context: rc, Backend: "minio", ResourceName: bucket, SpecJson: `{}`})
	if put, err := broker.PutObject(brokerCtx); err == nil {
		_ = put.Send(&entityv1.Chunk{Context: rc, Bucket: bucket, ObjectKey: objectKey, Data: []byte("go-perf-object-" + suffix), ContentType: "text/plain", FinalChunk: true})
		_, _ = put.CloseAndRecv()
	}
	fix.set("bucket", bucket)
	fix.set("object_key", objectKey)

	// A real Mongo collection + document so the document RPCs resolve a resource.
	collection := "sdk_perf_docs_" + strings.NewReplacer("-", "_").Replace(suffix)
	_, _ = broker.EnsureResource(brokerCtx, &entityv1.ResourceAdminRequest{Context: rc, Backend: "mongodb", ResourceName: collection, SpecJson: `{"collection":"` + collection + `"}`})
	docID := "doc-perf-" + suffix
	_, _ = broker.DocumentUpsert(brokerCtx, &entityv1.DocumentUpsertRequest{
		Context: rc, Resource: &entityv1.StoreResource{Backend: "mongodb", ResourceName: collection}, DocumentId: docID,
		Document: liveStruct(t, map[string]any{"_id": docID, "payload": "perf", "revision": 1}),
	})
	fix.set("document_id", docID)
	// NOTE: a single "backend"/"resource_name" fixture cannot serve both the SQL
	// and the document/cache/vector/graph RPCs (each needs its own backend +
	// resource). Those backend-specific DataBroker RPCs are driven by typed bodies
	// in perfRealBody (live_perf_test.go) instead of the generic reflective probe,
	// so we deliberately do NOT register a global backend/resource_name fixture.
	fix.set("collection", collection)
	fix.set("mongo_collection", collection)

	// ── AuthnService: a real user (id reused everywhere a user_id is needed) ──────
	authn := authnpb.NewAuthnServiceClient(authConn)
	pw := "CorrectHorse1!"
	uname := "sdk-perf-" + suffix
	if created, err := authn.CreateUser(base, &authnpb.CreateUserRequest{
		Username: uname, Email: uname + "@example.com", Password: pw,
		TenantId: tenant, ProjectId: project, FullName: "SDK Perf User",
	}); err != nil {
		t.Logf("perf seed: CreateUser failed (user-scoped RPCs will fall back): %v", err)
	} else {
		uid := created.GetUser().GetUserId()
		fix.set("user_id", uid)
		// The measured Phase-1 Login drives this username + the same password, so the
		// seeded user IS the account the perf Login authenticates against.
		fix.set("username", uname)
		fix.set("recipient_id", uid)
		fix.set("assigned_by", uid)
		fix.set("created_by", uid)
		fix.set("updated_by", uid)
		fix.set("revoked_by", uid)
		fix.set("deleted_by", uid)
		fix.set("approved_by", uid)
		fix.set("rejected_by", uid)
		fix.set("subject", "user:"+uid)
		// A real login → session id + tokens for session/token RPCs.
		if login, err := authn.Login(ctx, &authnpb.LoginRequest{Username: uname, Password: pw, TenantHint: tenant, ProjectHint: project, DeviceName: "go-sdk-perf-seed"}); err == nil {
			fix.set("session_id", login.GetSessionId())
			fix.set("token", login.GetAccessToken())
			fix.set("refresh_token", login.GetRefreshToken())
			fix.set("csrf_token", login.GetCsrfToken())
		}
		// Recovery codes (so VerifyOTP/recovery-style reads have a real code).
		if codes, err := authn.GenerateRecoveryCodes(base, &authnpb.GenerateRecoveryCodesRequest{UserId: uid, Count: 8}); err == nil && len(codes.GetCodes()) > 0 {
			fix.set("code", codes.GetCodes()[0])
			fix.set("recovery_code", codes.GetCodes()[0])
		}
	}

	// ── AuthzService: role + assignment + policies + relationship ─────────────────
	authz := authzpb.NewAuthzServiceClient(authConn)
	roleCode := "sdk_perf_reader_" + suffix
	if role, err := authz.CreateRole(base, &authzpb.CreateRoleRequest{
		Name: "SDK Perf Reader " + suffix, Description: "perf seed role", CreatedBy: uuid4(),
		RoleCode: roleCode, Domain: tenant, TenantId: tenant, ProjectId: project,
	}); err != nil {
		t.Logf("perf seed: CreateRole failed: %v", err)
	} else {
		rid := role.GetRole().GetRoleId()
		fix.set("role_id", rid)
		fix.set("role", roleCode)
		fix.set("role_code", roleCode)
		if uid := fix.m["user_id"]; uid != "" {
			if assigned, err := authz.AssignRole(base, &authzpb.AssignRoleRequest{
				UserId: uid, RoleId: rid, Domain: tenant, AssignedBy: uid, TenantId: tenant, ProjectId: project,
			}); err == nil {
				fix.set("user_role_id", assigned.GetUserRole().GetUserRoleId())
			}
		}
		addCleanup(func() {
			_, _ = authz.DeleteRole(base, &authzpb.DeleteRoleRequest{RoleId: rid, DeletedBy: fix.m["user_id"]})
		})
	}
	// ABAC policy + an RBAC policy rule → policy_id for GetPolicyRule/DeletePolicyRule.
	abacPolicyID := uuid4()
	_, _ = authz.PutAuthzPolicy(base, &authzpb.PutAuthzPolicyRequest{Policy: &authzpb.AuthzPolicyRecord{
		Id: abacPolicyID, Enabled: true, Effect: "allow", Tenant: tenant, Project: project,
		Role: roleCode, Action: "data.select", Resource: "invoice",
	}})
	if uid := fix.m["user_id"]; uid != "" {
		if rule, err := authz.CreatePolicyRule(base, &authzpb.CreatePolicyRuleRequest{
			Subject: roleCode, Domain: tenant, Object: "ledger", Action: "data.update",
			Effect: authzentpb.PolicyEffect_POLICY_EFFECT_ALLOW, Description: "perf seed rule", CreatedBy: uid, TenantId: tenant, ProjectId: project,
		}); err == nil {
			fix.set("policy_id", rule.GetPolicy().GetPolicyId())
		}
		_, _ = authz.PutRoleBinding(base, &authzpb.PutRoleBindingRequest{Binding: &authzpb.RoleBinding{Subject: "user:" + uid, Role: roleCode, Tenant: tenant, Project: project, Source: "sdk-perf"}})
		_, _ = authz.PutRelationship(base, &authzpb.PutRelationshipRequest{Tuple: &authzpb.RelationshipTuple{Subject: "user:" + uid, Relation: "member", Object: "group:sdk-perf-" + suffix, Tenant: tenant, Project: project, Source: "sdk-perf"}})
	}
	fix.set("relation", "member")
	fix.set("object", "group:sdk-perf-"+suffix)
	fix.set("resource", "invoice")
	fix.set("action", "data.select")
	// A governed policy draft → policy_draft_id for the draft lifecycle RPCs
	// (Update/Diff/Submit/Approve/Reject/Simulate).
	if draft, err := authz.CreatePolicyDraft(base, &authzpb.CreatePolicyDraftRequest{
		Actor:    &authzpb.GovernanceActor{Subject: fix.m["subject"], TenantId: tenant, ProjectId: project, Scopes: []string{"udb:authz:policy:write"}},
		TenantId: tenant, ProjectId: project, PolicySetName: "default", Title: "sdk perf draft " + suffix, ChangeReason: "seed", Document: &authzpb.PolicyDocument{},
	}); err == nil {
		fix.set("policy_draft_id", draft.GetDraft().GetDraftId())
	}

	// ── IdentityProviderService: a real OIDC provider → provider_id ───────────────
	idp := idppb.NewIdentityProviderServiceClient(authConn)
	if prov, err := idp.CreateProvider(base, &idppb.CreateProviderRequest{
		TenantId: tenant, Kind: idpentpb.IdpKind_IDP_KIND_OIDC, DisplayName: "SDK Perf OIDC " + suffix,
		Issuer: "https://idp.example.com/" + suffix, JwksUrl: "https://idp.example.com/jwks",
		ClientIds: []string{"perf-client"}, Audiences: []string{"udb"},
		ClaimMappingJson: "{}", GroupMappingJson: "{}", JitPolicyJson: "{}", AccountLinkingPolicy: "explicit",
		Enabled: true, CreatedBy: fix.m["user_id"],
		Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}},
	}); err != nil {
		t.Logf("perf seed: CreateProvider failed (idp RPCs will fall back): %v", err)
	} else {
		fix.set("provider_id", prov.GetProvider().GetProviderId())
	}

	// ── ApiKeyService: a real key → key_id + plain_key ────────────────────────────
	apikey := apikeypb.NewApiKeyServiceClient(authConn)
	keyCtx := &commonpb.RequestContext{UserId: "sdk-perf-svc-" + suffix, Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}}
	if key, err := apikey.CreateApiKey(base, &apikeypb.CreateApiKeyRequest{
		Name: "sdk-perf-key-" + suffix, OwnerId: "sdk-perf-svc-" + suffix, Scopes: []string{"data:read"}, Context: keyCtx,
	}); err != nil {
		t.Logf("perf seed: CreateApiKey failed: %v", err)
	} else {
		fix.set("key_id", key.GetKey().GetKeyId())
		fix.set("plain_key", key.GetPlainKey())
		fix.set("owner_id", "sdk-perf-svc-"+suffix)
	}

	// ── AnalyticsService: a recorded metric → a stage_name with data ──────────────
	analytics := analyticspb.NewAnalyticsServiceClient(authConn)
	stage := "sdk_perf_stage_" + suffix
	_, _ = analytics.RecordPipelineMetric(base, &analyticspb.RecordPipelineMetricRequest{StageName: stage, TenantId: tenant, LatencyMs: 100, IsSuccess: true})
	fix.set("stage_name", stage)

	// ── NotificationService: template + a sent notification → log_id, event_type ──
	notif := notifpb.NewNotificationServiceClient(authConn)
	event := "sdk.perf." + suffix
	_, _ = notif.UpsertTemplate(base, &notifpb.UpsertTemplateRequest{
		EventType: event, Channel: notifentpb.NotificationChannel_NOTIFICATION_CHANNEL_EMAIL, Locale: "en",
		SubjectTemplate: "SDK {{n}}", BodyTemplate: "sdk-perf-body", IsActive: true,
	})
	fix.set("event_type", event)
	fix.set("locale", "en")
	if rid := fix.m["recipient_id"]; rid != "" {
		if sent, err := notif.SendNotification(base, &notifpb.SendNotificationRequest{
			EventType: event, RecipientId: rid, RecipientAddress: "sdk+" + suffix + "@example.com",
			TenantId: tenant, Channels: []notifentpb.NotificationChannel{notifentpb.NotificationChannel_NOTIFICATION_CHANNEL_EMAIL},
		}); err == nil && len(sent.GetLogs()) > 0 {
			fix.set("log_id", sent.GetLogs()[0].GetLogId())
			fix.set("notification_id", sent.GetLogs()[0].GetLogId())
		}
	}

	// ── StorageService (UUID tenant): a registered file → file_id ─────────────────
	storage := storagepb.NewStorageServiceClient(authConn)
	nctx := nativeCtxFn()
	if reg, err := storage.RegisterUpload(nctx, &storagepb.RegisterUploadRequest{
		TenantId: uuidTenant, ProjectId: "", Filename: "perf-" + suffix + ".txt", ContentType: "text/plain",
		FileType: "DOCUMENT", ReferenceId: uuid4(), ReferenceType: "sdk.perf", SizeBytes: 128, ExpiresInMinutes: 30,
	}); err != nil {
		t.Logf("perf seed: RegisterUpload failed (storage/asset RPCs limited): %v", err)
	} else {
		fid := reg.GetFileId()
		fix.set("file_id", fid)
		addCleanup(func() {
			_, _ = storage.DeleteFile(nativeCtxFn(), &storagepb.DeleteFileRequest{TenantId: uuidTenant, FileId: fid})
		})

		// ── AssetService: pipeline definition + asset + a started instance ────────
		asset := assetpb.NewAssetServiceClient(authConn)
		if def, err := asset.CreatePipelineDefinition(nativeCtxFn(), &assetpb.CreatePipelineDefinitionRequest{
			TenantId: uuidTenant, Name: "sdk-perf-pipeline-" + suffix, Description: "perf seed",
			MediaType: "application/json", Steps: `[{"name":"extract","type":"EXTRACT"}]`, Version: 1,
		}); err == nil {
			fix.set("definition_id", def.GetDefinitionId())
		}
		if a, err := asset.RegisterAsset(nativeCtxFn(), &assetpb.RegisterAssetRequest{
			TenantId: uuidTenant, ProjectId: "", FileId: fid, Name: "sdk-perf-asset-" + suffix,
			MediaType: "application/json", Metadata: `{"source":"sdk-perf"}`,
		}); err == nil {
			fix.set("asset_id", a.GetAssetId())
			if did := fix.m["definition_id"]; did != "" {
				if inst, err := asset.StartPipeline(nativeCtxFn(), &assetpb.StartPipelineRequest{
					TenantId: uuidTenant, DefinitionId: did, AssetId: a.GetAssetId(), Context: `{}`, CorrelationId: "sdk-perf-" + suffix,
				}); err == nil {
					fix.set("instance_id", inst.GetInstanceId())
					// A started pipeline exposes its steps → a real step_id for CompleteStep.
					if pl, err := asset.GetPipeline(nativeCtxFn(), &assetpb.GetPipelineRequest{TenantId: uuidTenant, InstanceId: inst.GetInstanceId()}); err == nil && len(pl.GetSteps()) > 0 {
						fix.set("step_id", pl.GetSteps()[0].GetStepId())
					}
				}
			}
		}
	}

	// ── WebRTC (UUID tenant): room + peer + track ─────────────────────────────────
	rooms := webrtcpb.NewRoomServiceClient(authConn)
	peers := webrtcpb.NewPeerServiceClient(authConn)
	tracks := webrtcpb.NewTrackServiceClient(authConn)
	if room, err := rooms.CreateRoom(nativeCtxFn(), &webrtcpb.CreateRoomRequest{
		TenantId: uuidTenant, Name: "sdk-perf-room-" + suffix, MaxParticipants: 8, Config: `{}`, CreatedBy: uuid4(),
	}); err != nil {
		t.Logf("perf seed: CreateRoom failed (webrtc RPCs limited): %v", err)
	} else {
		roomID := room.GetRoomId()
		fix.set("room_id", roomID)
		addCleanup(func() {
			_, _ = rooms.CloseRoom(nativeCtxFn(), &webrtcpb.CloseRoomRequest{TenantId: uuidTenant, RoomId: roomID})
		})
		if joined, err := peers.JoinRoom(nativeCtxFn(), &webrtcpb.JoinRoomRequest{
			TenantId: uuidTenant, RoomId: roomID, DisplayName: "sdk-perf-peer", Metadata: `{}`, UserAgent: "sdk-perf",
		}); err == nil {
			pid := joined.GetPeer().GetPeerId()
			fix.set("peer_id", pid)
			if pub, err := tracks.PublishTrack(nativeCtxFn(), &webrtcpb.PublishTrackRequest{
				TenantId: uuidTenant, RoomId: roomID, PeerId: pid, Kind: "audio", Label: "mic", Settings: `{}`, Metadata: `{}`,
			}); err == nil {
				fix.set("track_id", pub.GetTrackId())
			}
		}
	}

	// Convenience scalars consumed by reflective populate for non-ID constrained
	// fields the heuristics already handle (email/url) are set in probeString; the
	// remaining commonly-required free-text fields default to a stable value.
	fix.set("name", "sdk-perf-"+suffix)
	fix.set("filename", "sdk-perf-"+suffix+".txt")
	fix.set("content_type", "text/plain")
	fix.set("file_type", "DOCUMENT")
	fix.set("kind", "audio")
	fix.set("topic_pattern", perfCdcTopicPattern(tenant))

	return perfSeedResult{
		fix:      fix,
		recordID: recordID,
		cleanup: func() {
			for i := len(cleanups) - 1; i >= 0; i-- {
				cleanups[i]()
			}
		},
	}
}

// perfCdcTopicPattern returns a permissive subscription pattern so the seeded
// Upsert's outbox→CDC→Kafka event is delivered to the subscriber regardless of
// the broker's exact topic naming. The CDC handler treats "*"/"" as match-all.
func perfCdcTopicPattern(tenant string) string { return "*" }
