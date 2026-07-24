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
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	analyticspb "github.com/fahara02/udb/sdk/go/gen/udb/core/analytics/services/v1"
	apikeypb "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/services/v1"
	assetpb "github.com/fahara02/udb/sdk/go/gen/udb/core/asset/services/v1"
	authnentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/entity/v1"
	authnpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	authzentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/entity/v1"
	authzpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
	backuppb "github.com/fahara02/udb/sdk/go/gen/udb/core/backup/services/v1"
	commonpb "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	controlentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/control/entity/v1"
	controlpb "github.com/fahara02/udb/sdk/go/gen/udb/core/control/services/v1"
	embeddingpb "github.com/fahara02/udb/sdk/go/gen/udb/core/embedding/services/v1"
	idpentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/idp/entity/v1"
	idppb "github.com/fahara02/udb/sdk/go/gen/udb/core/idp/services/v1"
	lockpb "github.com/fahara02/udb/sdk/go/gen/udb/core/lock/services/v1"
	notifentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/notification/entity/v1"
	notifpb "github.com/fahara02/udb/sdk/go/gen/udb/core/notification/services/v1"
	schedulerpb "github.com/fahara02/udb/sdk/go/gen/udb/core/scheduler/services/v1"
	storagepb "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/services/v1"
	vaultpb "github.com/fahara02/udb/sdk/go/gen/udb/core/vault/services/v1"
	webhookpb "github.com/fahara02/udb/sdk/go/gen/udb/core/webhook/services/v1"
	webrtcpb "github.com/fahara02/udb/sdk/go/gen/udb/core/webrtc/services/v1"
	workflowpb "github.com/fahara02/udb/sdk/go/gen/udb/core/workflow/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// samlIdpMetadataXML is minimal-but-valid SAML 2.0 IdP metadata (entityID +
// IDPSSODescriptor + a SingleSignOnService) so ImportSamlMetadata parses instead
// of failing "metadata missing entityID".
const samlIdpMetadataXML = `<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://idp.example.com/perf-saml">` +
	`<md:IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">` +
	`<md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.example.com/sso"/>` +
	`<md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST" Location="https://idp.example.com/sso"/>` +
	`</md:IDPSSODescriptor></md:EntityDescriptor>`

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
// seeded resolve; body builders must explicitly handle everything else.
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

func (f *perfFixtures) lookupSeed(key string) (string, bool) {
	v, ok := f.m[strings.ToLower(key)]
	return v, ok
}

func putPresignedStorageObject(ctx context.Context, uploadURL string, data []byte, contentType string) error {
	if strings.TrimSpace(uploadURL) == "" {
		return fmt.Errorf("empty storage upload_url")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("presigned storage PUT failed: %s", resp.Status)
	}
	return nil
}

// perfDeviceFingerprint returns a stable per-run device fingerprint for the seed
// Login's `device_id`. Passing it makes the broker's register_login_device mint a
// single listable udb_authn.devices row for THIS user+tenant (login.rs:501,
// lifecycle.rs:365) — replacing the old out-of-band GenericDispatch INSERT into
// udb_authn.devices. The broker HMACs the fingerprint, so it need not be a real
// UUID; this formats the all-digit run suffix into a UUID-shaped, deterministic
// string so reruns are idempotent (same user/tenant → same row, no duplicates).
func perfDeviceFingerprint(suffix string) string {
	hex := suffix
	for len(hex) < 32 {
		hex += "0"
	}
	hex = hex[:32]
	return hex[0:8] + "-" + hex[8:12] + "-" + hex[12:16] + "-" + hex[16:20] + "-" + hex[20:32]
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
	fix.set("tenant_code", "sdk-perf-tenant-"+suffix)
	// PurgeTenant is measured last against the ephemeral benchmark tenant. It
	// cannot target an arbitrary fresh tenant without matching tenant credentials.
	fix.set("purge_tenant_id", tenant)
	fix.set("resource_name", "postgres:default")
	fix.set("vault_db_role", "readonly")
	fix.set("egress_id", "eg-"+tenant+"-"+uuid4())

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
	// resource). Those backend-specific DataBroker RPCs are driven by the shared
	// bench-body manifest, so we deliberately do NOT register a global
	// backend/resource_name fixture.
	fix.set("collection", collection)
	fix.set("mongo_collection", collection)

	// ── Qdrant: a real vector collection named after the message type so the ──────
	// Vector* RPCs (which address the collection by message_type) resolve. The
	// seed vectors are 3-dim ([0.1,0.2,0.3]), so declare size 3 / cosine distance.
	_, _ = broker.EnsureResource(brokerCtx, &entityv1.ResourceAdminRequest{
		Context: rc, Backend: "qdrant", ResourceName: liveMessageType,
		SpecJson: `{"size":3,"distance":"Cosine"}`,
	})

	// ── ClickHouse: a real table so TimeSeriesQuery/Write resolve a column store. The
	// TimeSeries bodies send resource.resource_name = "sdk_perf_ts"; default columns
	// (_id String, _data String) match the seeded write point.
	_, _ = broker.EnsureResource(brokerCtx, &entityv1.ResourceAdminRequest{
		Context: rc, Backend: "clickhouse", ResourceName: "sdk_perf_ts", SpecJson: `{}`,
	})
	fix.set("ts_table", "sdk_perf_ts")

	// ── AuthnService: a real user (id reused everywhere a user_id is needed) ──────
	authn := authnpb.NewAuthnServiceClient(authConn)
	pw := perfSeedUserPassword
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
		// CreateUser persists the account as PENDING_VERIFICATION (core.rs:708). Login is
		// special-cased to allow it, but Authenticate's jwt_persisted_state_valid gates on
		// AccountStatus::Active (user_is_active, mod.rs:563) → "token subject is no longer
		// active". Activate the seeded user so the Phase-1 Authenticate (bearer_token of
		// THIS user) resolves an ACTIVE subject. The measured Phase-3 ChangeUserStatus
		// later suspends it — fine, Authenticate runs first.
		_, _ = authn.ChangeUserStatus(base, &authnpb.ChangeUserStatusRequest{
			UserId: uid, NewStatus: authnentpb.UserStatus_USER_STATUS_ACTIVE, Reason: "perf seed activate",
			Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}},
		})
		// A real login → session id + tokens for session/token RPCs. Passing a stable
		// DeviceId makes the broker's register_login_device mint a listable udb_authn.devices
		// row (login.rs:501, lifecycle.rs:365) for THIS user+tenant, so ListDevices below
		// returns a real device_id for RevokeDevice (no out-of-band INSERT needed). The id is
		// a deterministic UUID derived from the per-run suffix so reruns are idempotent.
		seedDeviceID := perfDeviceFingerprint(suffix)
		if login, err := authn.Login(ctx, &authnpb.LoginRequest{Username: uname, Password: pw, TenantHint: tenant, ProjectHint: project, DeviceName: "go-sdk-perf-seed", DeviceId: seedDeviceID}); err == nil {
			fix.set("session_id", login.GetSessionId())
			fix.set("token", login.GetAccessToken())
			fix.set("refresh_token", login.GetRefreshToken())
			fix.set("csrf_token", login.GetCsrfToken())
		}
		// A SEPARATE dedicated login → refresh_session_id + auth_token for the Phase-1
		// RefreshSession + Authenticate. The shared session_id/token above are consumed
		// by the measured RefreshToken (rotates the family → supersedes its session) and
		// the Phase-3 Logout/RevokeSession, so reusing them makes RefreshSession ("session
		// is not active") and Authenticate ("token subject is no longer active", which
		// also validates the issuing session) fail. This login is a distinct token family
		// + session that no measured RPC touches before its Phase-1 read.
		if dlogin, err := authn.Login(ctx, &authnpb.LoginRequest{Username: uname, Password: pw, TenantHint: tenant, ProjectHint: project, DeviceName: "go-sdk-perf-refresh", DeviceId: seedDeviceID}); err == nil {
			fix.set("refresh_session_id", dlogin.GetSessionId())
			fix.set("auth_token", dlogin.GetAccessToken())
		}
		// Recovery codes (so VerifyOTP/recovery-style reads have a real code).
		if codes, err := authn.GenerateRecoveryCodes(base, &authnpb.GenerateRecoveryCodesRequest{UserId: uid, Count: 8}); err == nil && len(codes.GetCodes()) > 0 {
			fix.set("code", codes.GetCodes()[0])
			fix.set("recovery_code", codes.GetCodes()[0])
		}
		// device_id for RevokeDevice. The seed Login above passed a stable DeviceId, so the
		// broker's register_login_device minted a real udb_authn.devices row for THIS
		// user+tenant; ListDevices now returns it. Fall back to the same deterministic
		// fingerprint if the list read is empty (revoke_device matches on device_id + claim
		// tenant only, so the minted id is sufficient).
		if dl, err := authn.ListDevices(base, &authnpb.ListDevicesRequest{UserId: uid}); err == nil && len(dl.GetDevices()) > 0 {
			fix.set("device_id", dl.GetDevices()[0].GetDeviceId())
		} else {
			fix.set("device_id", seedDeviceID)
		}
		// ── WebAuthn dev soft-authenticator (broker UDB_WEBAUTHN_TEST_MODE=1). The harness
		// sends the sentinel as public_key_credential_json and the broker mints+verifies a
		// REAL credential. The dev authenticator is deterministic: one credential id per
		// user. So bootstrap authentication on the main user, but measure registration
		// against a separate throwaway user with no existing passkey; otherwise the measured
		// FinishWebAuthnRegistration is a duplicate/exclude case, not the success path.
		if sr, err := authn.StartWebAuthnRegistration(base, &authnpb.StartWebAuthnRegistrationRequest{UserId: uid, Label: "perf-passkey", TenantId: tenant, ProjectId: project}); err == nil {
			if _, err := authn.FinishWebAuthnRegistration(base, &authnpb.FinishWebAuthnRegistrationRequest{
				ChallengeId: sr.GetChallengeId(), PublicKeyCredentialJson: webauthnTestCredential, Label: "perf-passkey",
			}); err != nil {
				t.Logf("perf seed: bootstrap FinishWebAuthnRegistration failed: %v", err)
			}
		} else {
			t.Logf("perf seed: bootstrap StartWebAuthnRegistration failed: %v", err)
		}
		regUserID := ""
		if regUser, err := authn.CreateUser(base, &authnpb.CreateUserRequest{
			Username: "sdk-perf-webauthn-reg-" + suffix, Email: "sdk-perf-webauthn-reg-" + suffix + "@example.com",
			Password: pw, TenantId: tenant, ProjectId: project, FullName: "SDK Perf WebAuthn Registration User",
		}); err == nil && regUser.GetUser() != nil {
			regUserID = regUser.GetUser().GetUserId()
		} else if err != nil {
			t.Logf("perf seed: WebAuthn registration user CreateUser failed: %v", err)
		}
		if regUserID == "" {
			regUserID = uid
		}
		if sr2, err := authn.StartWebAuthnRegistration(base, &authnpb.StartWebAuthnRegistrationRequest{UserId: regUserID, Label: "perf-passkey-2", TenantId: tenant, ProjectId: project}); err == nil {
			fix.set("reg_challenge_id", sr2.GetChallengeId())
		} else {
			t.Logf("perf seed: measured StartWebAuthnRegistration failed: %v", err)
		}
		if sa, err := authn.StartWebAuthnAuthentication(base, &authnpb.StartWebAuthnAuthenticationRequest{UserId: uid, TenantId: tenant, ProjectId: project}); err == nil {
			fix.set("auth_challenge_id", sa.GetChallengeId())
		} else {
			t.Logf("perf seed: StartWebAuthnAuthentication failed: %v", err)
		}
		// A real MFA challenge → challenge_id for VerifyMfaChallenge (the answer code
		// still needs a dev echo — see bug_report.md §G MFA note; this at least makes
		// challenge_id a valid UUID instead of the "must be UUID" reject).
		if mc, err := authn.IssueMfaChallenge(base, &authnpb.IssueMfaChallengeRequest{
			UserId: uid, FactorKind: authnentpb.AuthFactorKind_AUTH_FACTOR_KIND_EMAIL_OTP,
			Purpose: authnentpb.MfaChallengePurpose_MFA_CHALLENGE_PURPOSE_LOGIN_STEP_UP,
		}); err == nil {
			fix.set("challenge_id", mc.GetChallengeId())
		}
		// A DEDICATED OTP user (so the seeded OTP doesn't trip the measured SendOTP's
		// per-user cooldown on the main user) → real otp_id + dev-echoed code for
		// ResendOTP / VerifyOTP.
		if ou, err := authn.CreateUser(base, &authnpb.CreateUserRequest{
			Username: "sdk-perf-otp-" + suffix, Email: "sdk-perf-otp-" + suffix + "@example.com",
			Password: pw, TenantId: tenant, ProjectId: project, FullName: "SDK Perf OTP User",
		}); err == nil {
			_, _ = authn.ChangeUserStatus(base, &authnpb.ChangeUserStatusRequest{
				UserId: ou.GetUser().GetUserId(), NewStatus: authnentpb.UserStatus_USER_STATUS_ACTIVE, Reason: "perf seed activate otp",
				Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}},
			})
			// SENSITIVE_OPERATION (not the EMAIL_VERIFICATION that CreateUser auto-sends,
			// which would leave us in cooldown) so this SendOTP actually issues an otp_id.
			if so, err := authn.SendOTP(base, &authnpb.SendOTPRequest{
				UserId: ou.GetUser().GetUserId(), OtpType: authnentpb.OTPType_OTP_TYPE_SENSITIVE_OPERATION,
				Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}},
			}); err == nil {
				fix.set("otp_id", so.GetOtpId())
				if so.DevOtpCode != "" {
					fix.set("otp_code", so.DevOtpCode)
				}
			} else {
				t.Logf("perf seed: SendOTP failed: %v", err)
			}
			// A PASSWORD_RESET OTP on the SAME disposable user → reset_otp_id/code for
			// ResetPassword (reset_password_impl requires OtpType::PasswordReset, login.rs:689).
			// Cooldown is disabled (UDB_OTP_COOLDOWN_SECONDS=0) so this second SendOTP works.
			if rso, err := authn.SendOTP(base, &authnpb.SendOTPRequest{
				UserId: ou.GetUser().GetUserId(), OtpType: authnentpb.OTPType_OTP_TYPE_PASSWORD_RESET,
				Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}},
			}); err == nil {
				fix.set("reset_otp_id", rso.GetOtpId())
				if rso.DevOtpCode != "" {
					fix.set("reset_otp_code", rso.DevOtpCode)
				}
			} else {
				t.Logf("perf seed: reset SendOTP failed: %v", err)
			}
		} else {
			t.Logf("perf seed: OTP-user CreateUser failed: %v", err)
		}
	}
	seedAuthnUser := func(key, label string) string {
		username := "sdk-perf-" + label + "-" + suffix
		created, err := authn.CreateUser(base, &authnpb.CreateUserRequest{
			Username: username, Email: username + "@example.com", Password: pw,
			TenantId: tenant, ProjectId: project, FullName: "SDK Perf " + label,
		})
		if err != nil || created.GetUser() == nil {
			if err != nil {
				t.Logf("perf seed: CreateUser(%s) failed: %v", key, err)
			}
			return ""
		}
		userID := created.GetUser().GetUserId()
		fix.set(key, userID)
		_, _ = authn.ChangeUserStatus(base, &authnpb.ChangeUserStatusRequest{
			UserId: userID, NewStatus: authnentpb.UserStatus_USER_STATUS_ACTIVE, Reason: "perf seed activate " + label,
			Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}},
		})
		return userID
	}
	seedAuthnUser("admin_reset_mfa_user_id", "admin-reset-mfa")
	seedAuthnUser("admin_reset_password_user_id", "admin-reset-password")
	seedAuthnUser("change_password_user_id", "change-password")
	seedAuthnUser("change_status_user_id", "change-status")
	seedAuthnUser("disable_mfa_user_id", "disable-mfa")
	seedAuthnUser("revoke_recovery_user_id", "revoke-recovery")
	if deviceUserID := seedAuthnUser("revoke_device_user_id", "revoke-device"); deviceUserID != "" {
		deviceID := perfDeviceFingerprint("revoke-" + suffix)
		deviceUsername := "sdk-perf-revoke-device-" + suffix
		if _, err := authn.Login(ctx, &authnpb.LoginRequest{
			Username: deviceUsername, Password: pw, TenantHint: tenant, ProjectHint: project,
			DeviceName: "go-sdk-perf-revoke-device", DeviceId: deviceID,
		}); err == nil {
			if dl, err := authn.ListDevices(base, &authnpb.ListDevicesRequest{UserId: deviceUserID}); err == nil && len(dl.GetDevices()) > 0 {
				fix.set("revoke_device_id", dl.GetDevices()[0].GetDeviceId())
			} else {
				fix.set("revoke_device_id", deviceID)
			}
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
	// A SEPARATE disposable role for the destructive DeleteRole, so deleting it does
	// not invalidate the primary role_id that GetRole/UpdateRole/ListUserRoles read.
	if delRole, err := authz.CreateRole(base, &authzpb.CreateRoleRequest{
		Name: "SDK Perf Disposable " + suffix, Description: "perf seed disposable role", CreatedBy: uuid4(),
		RoleCode: "sdk_perf_disposable_" + suffix, Domain: tenant, TenantId: tenant, ProjectId: project,
	}); err == nil {
		fix.set("delete_role_id", delRole.GetRole().GetRoleId())
	}
	// ABAC policy + an RBAC policy rule. Capture policy_id only after the exact
	// GetPolicyRule path accepts it; a tenant-wide ListPolicyRules row can be an
	// older unrelated policy in dirty live benches.
	abacPolicyID := uuid4()
	_, _ = authz.PutAuthzPolicy(base, &authzpb.PutAuthzPolicyRequest{Policy: &authzpb.AuthzPolicyRecord{
		Id: abacPolicyID, Enabled: true, Effect: "allow", Tenant: tenant, Project: project,
		Role: roleCode, Action: "data.select", Resource: "invoice",
	}})
	if uid := fix.m["user_id"]; uid != "" {
		// ActivatePolicyVersion/RollbackPolicyVersion DELETE policy_rules WHERE
		// tenant=$1 AND project=$2 for the activated (main) project and re-insert the
		// version's rules with FRESH gen_random_uuid() ids (governance_activate.rs:236,274).
		// Those measured RPCs sort BEFORE GetPolicyRule, so a main-project rule (and any
		// captured/seed-verified id) is wiped before the read. GetPolicyRule reads by
		// policy_id ALONE (no project filter; owner bypasses RLS), so seed its target in
		// an ISOLATED project no version-activation touches → the row survives the whole
		// run and its CreatePolicyRule response id stays Get-queryable.
		getPolProject := project + "-getpolrule"
		if created, err := authz.CreatePolicyRule(base, &authzpb.CreatePolicyRuleRequest{
			Subject: roleCode, Domain: tenant, Object: "ledger", Action: "data.update",
			Effect: authzentpb.PolicyEffect_POLICY_EFFECT_ALLOW, Description: "perf seed rule (version-isolated)", CreatedBy: uid, TenantId: tenant, ProjectId: getPolProject,
		}); err != nil {
			t.Logf("perf seed: CreatePolicyRule (getpolrule) failed: %v", err)
		} else {
			fix.set("policy_id", created.GetPolicy().GetPolicyId())
		}
		// A SEPARATE disposable rule (same isolated project) for the destructive
		// DeletePolicyRule, so deleting it never touches the GetPolicyRule target.
		if delRule, err := authz.CreatePolicyRule(base, &authzpb.CreatePolicyRuleRequest{
			Subject: roleCode, Domain: tenant, Object: "ledger-disposable", Action: "data.delete",
			Effect: authzentpb.PolicyEffect_POLICY_EFFECT_ALLOW, Description: "perf seed disposable rule", CreatedBy: uid, TenantId: tenant, ProjectId: getPolProject,
		}); err == nil {
			fix.set("delete_policy_id", delRule.GetPolicy().GetPolicyId())
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
		Actor:    &authzpb.GovernanceActor{Subject: fix.m["subject"], TenantId: tenant, ProjectId: project, BreakGlass: true, BreakGlassReason: "sdk perf seed", BreakGlassExpiresAtUnix: time.Now().Unix() + 900},
		TenantId: tenant, ProjectId: project, PolicySetName: "default", Title: "sdk perf draft " + suffix, ChangeReason: "seed", Document: &authzpb.PolicyDocument{},
	}); err == nil {
		fix.set("policy_draft_id", draft.GetDraft().GetDraftId())
	}
	// Draft lifecycle fixtures: UpdatePolicyDraft needs an OPEN draft; Approve/Reject
	// need IN_REVIEW (draft_reviewable, governance_drafts.rs). Seed one OPEN draft and
	// two submitted-to-IN_REVIEW drafts (one each for Approve/Reject, which consume them).
	gActor := func() *authzpb.GovernanceActor {
		// Body actor.scopes are ignored by the live D1/D2 gate (it reads claim
		// scopes, and no role projects to authz:*), so the seed's own governance
		// writes must use the body-authoritative break-glass bypass — otherwise the
		// drafts/versions/canary are never created and the governance RPCs that read
		// them fail "<id> is required".
		return &authzpb.GovernanceActor{Subject: fix.m["subject"], TenantId: tenant, ProjectId: project, BreakGlass: true, BreakGlassReason: "sdk perf seed", BreakGlassExpiresAtUnix: time.Now().Unix() + 900}
	}
	mkDraft := func(title string) string {
		d, err := authz.CreatePolicyDraft(base, &authzpb.CreatePolicyDraftRequest{
			Actor: gActor(), TenantId: tenant, ProjectId: project, PolicySetName: "default",
			Title: title + suffix, ChangeReason: "seed", Document: &authzpb.PolicyDocument{},
		})
		if err != nil {
			return ""
		}
		return d.GetDraft().GetDraftId()
	}
	fix.set("update_draft_id", mkDraft("sdk-perf-update-")) // left OPEN
	if id := mkDraft("sdk-perf-approve-"); id != "" {
		if _, err := authz.SubmitPolicyDraft(base, &authzpb.SubmitPolicyDraftRequest{Actor: gActor(), DraftId: id}); err == nil {
			fix.set("approve_draft_id", id) // now IN_REVIEW
		}
	}
	if id := mkDraft("sdk-perf-reject-"); id != "" {
		if _, err := authz.SubmitPolicyDraft(base, &authzpb.SubmitPolicyDraftRequest{Actor: gActor(), DraftId: id}); err == nil {
			fix.set("reject_draft_id", id) // now IN_REVIEW
		}
	}
	// Policy VERSIONS + CANARY: approving a draft promotes a PolicyVersion (APPROVED).
	// mkVersion(set) → CreateDraft→Submit→Approve→the promoted version. ActivatePolicyVersion/
	// ActivateCanary need a version in Approved/Superseded/RolledBack; GetCanaryStatus/
	// PromoteCanary need a real canary_id; RollbackPolicyVersion needs a set with a prior version.
	mkVersion := func(setName, title string) *authzentpb.PolicyVersion {
		d, err := authz.CreatePolicyDraft(base, &authzpb.CreatePolicyDraftRequest{
			Actor: gActor(), TenantId: tenant, ProjectId: project, PolicySetName: setName,
			Title: title + suffix, ChangeReason: "seed", Document: &authzpb.PolicyDocument{},
		})
		if err != nil {
			return nil
		}
		did := d.GetDraft().GetDraftId()
		if _, err := authz.SubmitPolicyDraft(base, &authzpb.SubmitPolicyDraftRequest{Actor: gActor(), DraftId: did}); err != nil {
			return nil
		}
		ap, err := authz.ApprovePolicyDraft(base, &authzpb.ApprovePolicyDraftRequest{Actor: gActor(), DraftId: did, Reviewer: fix.m["user_id"], Reason: "seed approve"})
		if err != nil {
			t.Logf("perf seed: ApprovePolicyDraft(version) failed: %v", err)
			return nil
		}
		return ap.GetVersion()
	}
	if v := mkVersion("sdk-perf-activate-set-"+suffix, "activate-"); v != nil {
		fix.set("policy_version_id", v.GetPolicyVersionId()) // measured ActivatePolicyVersion
	}
	if v := mkVersion("sdk-perf-canary-set-"+suffix, "canary-"); v != nil {
		fix.set("canary_version_id", v.GetPolicyVersionId()) // measured ActivateCanary
		// Seed a canary with a 1s success window → promote-eligible ~1s after activation
		// (promote_eligible: now-started >= success_window_secs; canary.rs:248). NOTE: passing
		// 0 makes ActivateCanary substitute DEFAULT_CANARY_WINDOW_SECS (governance_activate.rs:631),
		// which never elapses during the run; the measured PromoteCanary runs well after 1s.
		if c, err := authz.ActivateCanary(base, &authzpb.ActivateCanaryRequest{
			Actor: gActor(), PolicyVersionId: v.GetPolicyVersionId(),
			ScopeKind: authzentpb.CanaryScopeKind_CANARY_SCOPE_KIND_PERCENT, ScopeValues: []string{"10"},
			SuccessWindowSecs: 1, MetricThreshold: 0.99, MinSamples: 0,
		}); err == nil {
			fix.set("canary_id", c.GetCanary().GetCanaryId())
		} else {
			t.Logf("perf seed: ActivateCanary failed: %v", err)
		}
	}
	// Rollback set: activate TWO versions in one set so RollbackPolicyVersion has a prior target.
	if v1 := mkVersion("sdk-perf-rollback-set-"+suffix, "rb1-"); v1 != nil {
		_, _ = authz.ActivatePolicyVersion(base, &authzpb.ActivatePolicyVersionRequest{Actor: gActor(), PolicyVersionId: v1.GetPolicyVersionId()})
		if v2 := mkVersion("sdk-perf-rollback-set-"+suffix, "rb2-"); v2 != nil {
			_, _ = authz.ActivatePolicyVersion(base, &authzpb.ActivatePolicyVersionRequest{Actor: gActor(), PolicyVersionId: v2.GetPolicyVersionId()})
			fix.set("rollback_policy_set_id", v2.GetPolicySetId())
			fix.set("rollback_target_version_id", v1.GetPolicyVersionId())
		}
	}

	// ── IdentityProviderService: a real OIDC provider → provider_id ───────────────
	idp := idppb.NewIdentityProviderServiceClient(authConn)
	if prov, err := idp.CreateProvider(base, &idppb.CreateProviderRequest{
		TenantId: tenant, Kind: idpentpb.IdpKind_IDP_KIND_OIDC, DisplayName: "SDK Perf OIDC " + suffix,
		Issuer: "https://idp.example.com/" + suffix, JwksUrl: "https://idp.example.com/jwks",
		ClientIds: []string{"perf-client"}, Audiences: []string{"udb"},
		// A non-empty group→role mapping so ScimGetGroup (which resolves scim_group_id
		// against the provider's group_mapping_json KEYS — groups are not persisted,
		// mod.rs:1401) finds a group. scim_group_id below = this key.
		// require_verified_email:false — the SamlAcs dev self-asserted assertion carries an
		// `email` attribute (= NameID) but no `email_verified`, and JIT defaults
		// require_verified_email=true (mapping.rs:252), which rejects "email is not verified".
		ClaimMappingJson: "{}", GroupMappingJson: `{"sdk-perf-group":"admin"}`, JitPolicyJson: `{"require_verified_email":false}`, AccountLinkingPolicy: "explicit",
		Enabled: true, CreatedBy: fix.m["user_id"],
		Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}},
	}); err != nil {
		t.Logf("perf seed: CreateProvider failed (idp RPCs will fall back): %v", err)
	} else {
		pid := prov.GetProvider().GetProviderId()
		fix.set("provider_id", pid)
		fix.set("scim_group_id", "sdk-perf-group") // a key in GroupMappingJson above
		// A SEPARATE disposable provider for the destructive DisableProvider, so disabling it
		// does NOT disable the primary provider_id that SamlAcs + ResolveExternalIdentity read
		// (both reject a disabled provider before their dev-mode path runs).
		if dprov, err := idp.CreateProvider(base, &idppb.CreateProviderRequest{
			TenantId: tenant, Kind: idpentpb.IdpKind_IDP_KIND_OIDC, DisplayName: "SDK Perf OIDC Disposable " + suffix,
			Issuer: "https://idp-disposable.example.com/" + suffix, JwksUrl: "https://idp-disposable.example.com/jwks",
			ClientIds: []string{"perf-client-disp"}, Audiences: []string{"udb"},
			ClaimMappingJson: "{}", GroupMappingJson: "{}", JitPolicyJson: "{}", AccountLinkingPolicy: "explicit",
			Enabled: true, CreatedBy: fix.m["user_id"],
			Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}},
		}); err == nil {
			fix.set("disable_provider_id", dprov.GetProvider().GetProviderId())
		}
		// SCIM users JIT-provisioned via the seeded provider. NOTE: scim_user_id is set
		// to the SUBJECT (userName), NOT the external_identity_id that ScimCreateUser
		// returns — find_scim_identity only resolves by subject (broker bug SCIM-1 in
		// bug_report.md §G). Each user gets a UNIQUE email: an empty email collides on
		// the unique idx_users_email (empty string != NULL — broker bug SCIM-2). A
		// SEPARATE disposable user backs the destructive ScimDeleteUser.
		scimCtx := &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}}
		scimUser := "scim-perf-user-" + suffix
		scimDel := "scim-perf-del-" + suffix
		if su, err := idp.ScimCreateUser(base, &idppb.ScimCreateUserRequest{
			TenantId: tenant, ProviderId: pid, Context: scimCtx,
			ScimUserJson: `{"userName":"` + scimUser + `","emails":[{"value":"` + scimUser + `@example.com","primary":true}],"active":true}`,
		}); err == nil {
			fix.set("scim_user_id", scimUser)
			// The returned SCIM user id IS the external_identity_id (UUID) → UnlinkIdentity.
			fix.set("external_identity_id", su.GetUser().GetId())
		} else {
			t.Logf("perf seed: ScimCreateUser failed: %v", err)
		}
		// A real enabled SAML provider so StartSamlLogin resolves an active provider
		// (ensure_provider_active, idp/mod.rs:321). OIDC above is used for SCIM; SAML
		// login needs a SAML-kind provider.
		if sp, err := idp.CreateProvider(base, &idppb.CreateProviderRequest{
			TenantId: tenant, Kind: idpentpb.IdpKind_IDP_KIND_SAML, DisplayName: "SDK Perf SAML " + suffix,
			Issuer: "https://saml.example.com/" + suffix, JwksUrl: "https://saml.example.com/jwks",
			ClientIds: []string{"perf-saml"}, Audiences: []string{"udb"},
			ClaimMappingJson: "{}", GroupMappingJson: "{}", JitPolicyJson: `{"require_verified_email":false}`, AccountLinkingPolicy: "explicit",
			Enabled: true, CreatedBy: fix.m["user_id"],
			Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}},
		}); err == nil {
			spid := sp.GetProvider().GetProviderId()
			fix.set("saml_provider_id", spid)
			// Import metadata so the SAML provider gets an SSO URL (StartSamlLogin needs it:
			// "provider has no SAML SSO URL; import metadata first").
			_, _ = idp.ImportSamlMetadata(base, &idppb.ImportSamlMetadataRequest{
				ProviderId: spid, TenantId: tenant, MetadataXml: samlIdpMetadataXML, UpdatedBy: fix.m["user_id"],
				Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}},
			})
		}
		if _, err := idp.ScimCreateUser(base, &idppb.ScimCreateUserRequest{
			TenantId: tenant, ProviderId: pid, Context: scimCtx,
			ScimUserJson: `{"userName":"` + scimDel + `","emails":[{"value":"` + scimDel + `@example.com","primary":true}],"active":true}`,
		}); err == nil {
			fix.set("delete_scim_user_id", scimDel)
		}
	}

	// ── ApiKeyService: a real key → key_id + plain_key ────────────────────────────
	// Canonical-identity model: the key owner must be an EXISTING ACTIVE
	// SERVICE_ACCOUNT with an active typed grant, addressed by its UUID — a
	// bare service NAME is not a user_id and never was one.
	apikey := apikeypb.NewApiKeyServiceClient(authConn)
	svcName := "sdk-perf-svc-" + suffix
	svcOwner := ""
	if svcUser, err := authn.CreateUser(base, &authnpb.CreateUserRequest{
		Username: svcName, Email: svcName + "@example.com", Password: pw,
		TenantId: tenant, ProjectId: project, FullName: "SDK Perf Service Account",
		AccountKind: authnentpb.AccountKind_ACCOUNT_KIND_SERVICE_ACCOUNT,
	}); err != nil {
		t.Logf("perf seed: service-account CreateUser failed (apikey RPCs fall back): %v", err)
	} else {
		svcOwner = svcUser.GetUser().GetUserId()
		// CreateUser persists PENDING_VERIFICATION; the typed grant and
		// CreateApiKey both require an ACTIVE service account.
		_, _ = authn.ChangeUserStatus(base, &authnpb.ChangeUserStatusRequest{
			UserId: svcOwner, NewStatus: authnentpb.UserStatus_USER_STATUS_ACTIVE, Reason: "perf seed activate",
			Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}},
		})
		if _, err := authn.CreateServiceAccountGrant(base, &authnpb.CreateServiceAccountGrantRequest{
			TenantId: tenant, UserId: svcOwner, ServiceIdentity: svcName,
			ProjectId: project, ApprovedScopes: []string{"data:read"},
			Reason: "sdk perf seed",
		}); err != nil {
			t.Logf("perf seed: CreateServiceAccountGrant failed (apikey RPCs fall back): %v", err)
		}
	}
	if svcOwner == "" {
		svcOwner = svcName // fall back; CreateApiKey will fail typed, not INTERNAL
	}
	keyCtx := &commonpb.RequestContext{UserId: svcOwner, Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}}
	if key, err := apikey.CreateApiKey(base, &apikeypb.CreateApiKeyRequest{
		Name: "sdk-perf-key-" + suffix, OwnerId: svcOwner, Scopes: []string{"data:read"}, Context: keyCtx,
	}); err != nil {
		t.Logf("perf seed: CreateApiKey failed: %v", err)
	} else {
		fix.set("key_id", key.GetKey().GetKeyId())
		fix.set("plain_key", key.GetPlainKey())
		fix.set("owner_id", svcOwner)
	}
	// A SEPARATE disposable key for the destructive RevokeApiKey, so revoking it does
	// not invalidate the primary key_id that RotateApiKey/UpdateApiKey/GetApiKey read.
	if rk, err := apikey.CreateApiKey(base, &apikeypb.CreateApiKeyRequest{
		Name: "sdk-perf-revoke-" + suffix, OwnerId: svcOwner, Scopes: []string{"data:read"}, Context: keyCtx,
	}); err == nil {
		fix.set("revoke_key_id", rk.GetKey().GetKeyId())
	}
	// A SEPARATE disposable key for UpdateApiKey, so the measured RotateApiKey (which
	// rotates the primary key_id) can't invalidate the key UpdateApiKey targets.
	if uk, err := apikey.CreateApiKey(base, &apikeypb.CreateApiKeyRequest{
		Name: "sdk-perf-update-" + suffix, OwnerId: svcOwner, Scopes: []string{"data:read"}, Context: keyCtx,
	}); err == nil {
		fix.set("update_key_id", uk.GetKey().GetKeyId())
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
		SubjectTemplate: "SDK perf", BodyTemplate: "sdk-perf-body", IsActive: true,
	})
	fix.set("event_type", event)
	fix.set("locale", "en")
	// Governance break-glass expiry: the D1/D2 governance gate reads scopes from
	// the VERIFIED claim, not request-body actor.scopes, and no role projects to
	// authz:* (tokens.rs ROLE_SCOPE_PROJECTIONS) — so the governance RPCs are
	// reached via the body-authoritative break-glass bypass (≤900s, reason-bearing,
	// audited). Set at seed time; the governance RPCs measure shortly after.
	fix.set("gov_exp", fmt.Sprintf("%d", time.Now().Unix()+900))
	if rid := fix.m["recipient_id"]; rid != "" {
		if sent, err := notif.SendNotification(base, &notifpb.SendNotificationRequest{
			EventType: event, RecipientId: rid, RecipientAddress: "sdk+" + suffix + "@example.com",
			TenantId: tenant, ResourceType: "__perf_force_failed__", Channels: []notifentpb.NotificationChannel{notifentpb.NotificationChannel_NOTIFICATION_CHANNEL_EMAIL},
		}); err == nil && len(sent.GetLogs()) > 0 {
			logID := sent.GetLogs()[0].GetLogId()
			fix.set("log_id", logID)
			fix.set("notification_id", logID)
			// UDB_NOTIFICATION_TEST_MODE + ResourceType="__perf_force_failed__"
			// makes SendNotification write a real FAILED row, so RetryNotification
			// measures a served path without touching internal notification tables.
		}
		// Seed an EMAIL-channel preference for the user so GetPreference (which the
		// perf spec calls with user_id + tenant_id + EMAIL) finds a real row.
		_, _ = notif.SetPreference(base, &notifpb.SetPreferenceRequest{
			UserId: rid, TenantId: tenant,
			Channel: notifentpb.NotificationChannel_NOTIFICATION_CHANNEL_EMAIL, IsOptedOut: false,
		})
	}

	nctx := nativeCtxFn()

	// ── BackupService: policy + backup id for read/restore fixtures ─────────────
	backup := backuppb.NewBackupServiceClient(authConn)
	// StartTenantBackup exports to the broker's object_bucket (UDB_OBJECT_BUCKET,
	// default "udb-storage"). That bucket is otherwise first created by the storage
	// section further below, so ensure it here — before the backup runs — or the
	// export fails "S3 put_object failed" and backup_id/restore fixtures go empty.
	backupBucket := liveEnv("UDB_OBJECT_BUCKET", "udb-storage")
	if _, err := broker.EnsureResource(brokerCtx, &entityv1.ResourceAdminRequest{Context: rc, Backend: "minio", ResourceName: backupBucket, SpecJson: `{}`}); err != nil {
		t.Logf("perf seed: EnsureResource for backup bucket %q failed: %v", backupBucket, err)
	}
	if _, err := backup.PutBackupPolicy(nctx, &backuppb.PutBackupPolicyRequest{
		TenantId: tenant, PolicyName: "sdk-perf-default", ScheduleCron: "0 3 * * *",
		RetentionDays: 7, MaxRetainedBackups: 3, Enabled: true, MetadataJson: "{}",
	}); err != nil {
		t.Logf("perf seed: PutBackupPolicy failed: %v", err)
	}
	if b, err := backup.StartTenantBackup(nctx, &backuppb.StartTenantBackupRequest{
		TenantId: tenant, MetadataJson: `{"source":"sdk-perf-seed"}`,
	}); err == nil {
		fix.set("backup_id", b.GetBackupId())
		fix.set("restore_tenant_id", uuid4())
	} else {
		t.Logf("perf seed: StartTenantBackup failed: %v", err)
	}

	// ── SchedulerService: stable job for reads/pause/resume/delete ──────────────
	scheduler := schedulerpb.NewSchedulerServiceClient(authConn)
	if job, err := scheduler.CreateJob(nctx, &schedulerpb.CreateJobRequest{
		TenantId: tenant, ProjectId: "", Name: "sdk-perf-seed-job-" + suffix,
		ScheduleType: "CRON", CronExpression: "*/5 * * * *", Payload: "{}",
		TargetTopic: "sdk.perf.scheduler", MaxAttempts: 3, BackoffSeconds: 30,
	}); err == nil {
		fix.set("job_id", job.GetJobId())
	} else {
		t.Logf("perf seed: CreateJob failed: %v", err)
	}

	// ── WebhookService: primary + disposable endpoint fixtures ──────────────────
	webhooks := webhookpb.NewWebhookServiceClient(authConn)
	if ep, err := webhooks.CreateEndpoint(nctx, &webhookpb.CreateEndpointRequest{
		TenantId: tenant, Url: "https://example.com/udb-webhook-seed",
		TopicPattern: perfCdcTopicPattern(tenant), Description: "sdk perf seed webhook",
		MaxAttempts: 3, MetadataJson: "{}",
	}); err == nil {
		fix.set("endpoint_id", ep.GetEndpointId())
	} else {
		t.Logf("perf seed: CreateEndpoint failed: %v", err)
	}
	if dep, err := webhooks.CreateEndpoint(nctx, &webhookpb.CreateEndpointRequest{
		TenantId: tenant, Url: "https://example.com/udb-webhook-delete",
		TopicPattern: perfCdcTopicPattern(tenant), Description: "sdk perf delete webhook",
		MaxAttempts: 3, MetadataJson: "{}",
	}); err == nil {
		fix.set("delete_endpoint_id", dep.GetEndpointId())
	}

	// ── VaultService: read fixtures that run before measured vault mutations ─────
	vault := vaultpb.NewVaultServiceClient(authConn)
	vaultKey := "sdk-perf-key-" + suffix
	fix.set("vault_key_name", vaultKey)
	fix.set("vault_create_key_name", "sdk-perf-create-key-"+suffix)
	if _, err := vault.CreateTransitKey(nctx, &vaultpb.CreateTransitKeyRequest{
		TenantId: tenant, KeyName: vaultKey, Algorithm: "aes256-gcm-siv",
	}); err != nil {
		t.Logf("perf seed: CreateTransitKey failed: %v", err)
	}
	// A dedicated ed25519 SIGNING key so GetTransitPublicKey exports a real public
	// key — the aes256-gcm-siv key above has no exportable public half, so the
	// GetTransitPublicKey read fixture needs its own asymmetric key.
	signingKey := "sdk-perf-signing-key-" + suffix
	fix.set("vault_signing_key_name", signingKey)
	if _, err := vault.CreateTransitKey(nctx, &vaultpb.CreateTransitKeyRequest{
		TenantId: tenant, KeyName: signingKey, Algorithm: "ed25519",
	}); err != nil {
		t.Logf("perf seed: CreateTransitKey(ed25519 signing) failed: %v", err)
	}
	if enc, err := vault.Encrypt(nctx, &vaultpb.EncryptRequest{
		TenantId: tenant, KeyName: vaultKey, Plaintext: "perf",
	}); err == nil {
		fix.set("vault_ciphertext", enc.GetCiphertext())
	} else {
		t.Logf("perf seed: Encrypt failed: %v", err)
	}
	if sig, err := vault.Sign(nctx, &vaultpb.SignRequest{TenantId: tenant, KeyName: vaultKey, Input: "perf"}); err == nil {
		fix.set("vault_signature", sig.GetSignature())
	} else {
		t.Logf("perf seed: Sign failed: %v", err)
	}
	fix.set("vault_secret_path", "app/config-"+suffix)
	fix.set("vault_put_secret_path", "app/put-"+suffix)
	fix.set("vault_delete_secret_path", "app/delete-"+suffix)
	fix.set("vault_destroy_secret_path", "app/destroy-"+suffix)
	_, _ = vault.PutSecret(nctx, &vaultpb.PutSecretRequest{TenantId: tenant, SecretPath: fix.m["vault_secret_path"], SecretValue: "perf-secret", ExpectedVersion: 0, MetadataJson: "{}"})
	_, _ = vault.PutSecret(nctx, &vaultpb.PutSecretRequest{TenantId: tenant, SecretPath: fix.m["vault_delete_secret_path"], SecretValue: "delete-secret", ExpectedVersion: 0, MetadataJson: "{}"})
	_, _ = vault.PutSecret(nctx, &vaultpb.PutSecretRequest{TenantId: tenant, SecretPath: fix.m["vault_destroy_secret_path"], SecretValue: "destroy-secret", ExpectedVersion: 0, MetadataJson: "{}"})

	// ── WorkflowService: primary + disposable workflow fixtures ─────────────────
	workflow := workflowpb.NewWorkflowServiceClient(authConn)
	if wf, err := workflow.StartWorkflow(nctx, &workflowpb.StartWorkflowRequest{
		TenantId: tenant, ProjectId: "", WorkflowType: "sdk.perf.workflow",
		TotalSteps: 20, Payload: "{}", Compensations: "[]", CorrelationId: recordID,
	}); err == nil {
		fix.set("workflow_id", wf.GetWorkflowId())
	} else {
		t.Logf("perf seed: StartWorkflow failed: %v", err)
	}
	if cwf, err := workflow.StartWorkflow(nctx, &workflowpb.StartWorkflowRequest{
		TenantId: tenant, ProjectId: "", WorkflowType: "sdk.perf.cancel",
		TotalSteps: 20, Payload: "{}", Compensations: "[]", CorrelationId: "cancel-" + recordID,
	}); err == nil {
		fix.set("cancel_workflow_id", cwf.GetWorkflowId())
	}

	// ── EmbeddingService: model registry, durable jobs, and one searchable vector ─
	embedding := embeddingpb.NewEmbeddingServiceClient(authConn)
	registerModel := func(modelID, collection, alias string) error {
		_, err := embedding.RegisterModel(nctx, &embeddingpb.RegisterModelRequest{
			TenantId: tenant, ModelId: modelID, Provider: "deterministic",
			ModelName: "text-embedding-3-small", Version: "1", Dimensions: 3,
			MatryoshkaDims: []int32{3}, DistanceMetric: "COSINE", Normalize: true,
			OutputDtype: "FLOAT32", MaxInputTokens: 8192, Tokenizer: "cl100k_base",
			TaskType: "DOCUMENT", ProviderEndpointRef: "vault://embedding/sdk-live",
			VectorBackend: "qdrant", VectorInstance: "default", CollectionAlias: alias,
			ActiveCollection: collection, ChunkingStrategy: "TOKEN_RECURSIVE",
			ChunkTokens: 256, ChunkOverlapTokens: 32, MetadataJson: `{"suite":"sdk-live"}`,
		})
		return err
	}
	if err := registerModel("text-embedding-3-small", "sdk_live_records", "sdk_live_records_alias"); err != nil {
		t.Logf("perf seed: RegisterModel failed: %v", err)
	}
	deleteModelID := "sdk-live-delete-model-" + suffix
	fix.set("embedding_delete_model_id", deleteModelID)
	if err := registerModel(deleteModelID, "sdk_live_delete_records_"+suffix, "sdk_live_delete_records_alias_"+suffix); err != nil {
		t.Logf("perf seed: RegisterModel(delete fixture) failed: %v", err)
	}
	if _, err := embedding.RegisterSource(nctx, &embeddingpb.RegisterSourceRequest{
		TenantId: tenant, SourceName: "sdk_live_records", SourceMessageType: fix.m["message_type"],
		TextFields: []string{"payload"}, TargetCollection: "sdk_live_records",
		ModelId: "text-embedding-3-small", MetadataJson: "{}",
	}); err != nil {
		t.Logf("perf seed: RegisterSource failed: %v", err)
	}
	if _, err := embedding.ReportEmbedding(nctx, &embeddingpb.ReportEmbeddingRequest{
		TenantId: tenant, SourceName: "sdk_live_records", RowPk: recordID,
		Vector: []float32{0.1, 0.2, 0.3}, Model: "text-embedding-3-small", Dims: 3,
	}); err != nil {
		t.Logf("perf seed: ReportEmbedding failed: %v", err)
	}
	if document, err := embedding.IngestDocument(nctx, &embeddingpb.IngestDocumentRequest{
		TenantId: tenant, ExternalId: "sdk-live-work-" + suffix,
		Title:       "SDK benchmark work fixture",
		RawText:     "Durable embedding work is seeded from real document text for the SDK benchmark.",
		ContentType: "text/plain", DocVersion: "1", ModelId: "text-embedding-3-small",
		MetadataJson: `{"suite":"sdk-live","fixture":"work"}`,
	}); err == nil {
		fix.set("embedding_job_id", document.GetJobId())
		if work, listErr := embedding.ListEmbeddingWorkItems(nctx, &embeddingpb.ListEmbeddingWorkItemsRequest{
			TenantId: tenant, JobId: document.GetJobId(), PageSize: 50,
		}); listErr == nil && len(work.GetWorkItems()) > 0 {
			fix.set("embedding_work_item_id", work.GetWorkItems()[0].GetWorkItemId())
		} else if listErr != nil {
			t.Logf("perf seed: ListEmbeddingWorkItems failed: %v", listErr)
		}
	} else {
		t.Logf("perf seed: IngestDocument(work fixture) failed: %v", err)
	}
	if parser, err := embedding.IngestDocument(nctx, &embeddingpb.IngestDocumentRequest{
		TenantId: tenant, ExternalId: "sdk-live-parser-" + suffix,
		Title: "SDK benchmark parser fixture", StorageObjectRef: "udb://sdk-live/embedding-" + suffix + ".txt",
		ContentType: "text/plain", DocVersion: "1", ModelId: "text-embedding-3-small",
		MetadataJson: `{"suite":"sdk-live","fixture":"parser"}`,
	}); err == nil {
		fix.set("embedding_document_id", parser.GetDocumentId())
		fix.set("embedding_document_job_id", parser.GetJobId())
	} else {
		t.Logf("perf seed: IngestDocument(parser fixture) failed: %v", err)
	}

	// ── LockService: independent locks for renew/release mutation rows ──────────
	locks := lockpb.NewLockServiceClient(authConn)
	if renew, err := locks.AcquireLock(nctx, &lockpb.AcquireLockRequest{
		TenantId: tenant, LockName: "sdk-perf-renew-lock", OwnerId: fix.m["user_id"],
		LeaseTtlSeconds: 60, MetadataJson: "{}",
	}); err == nil {
		fix.set("renew_fencing_token", strconv.FormatInt(renew.GetFencingToken(), 10))
	} else {
		t.Logf("perf seed: AcquireLock(renew) failed: %v", err)
	}
	if release, err := locks.AcquireLock(nctx, &lockpb.AcquireLockRequest{
		TenantId: tenant, LockName: "sdk-perf-release-lock", OwnerId: fix.m["user_id"],
		LeaseTtlSeconds: 60, MetadataJson: "{}",
	}); err == nil {
		fix.set("release_fencing_token", strconv.FormatInt(release.GetFencingToken(), 10))
	} else {
		t.Logf("perf seed: AcquireLock(release) failed: %v", err)
	}

	// ── StorageService (UUID tenant): a registered file → file_id ─────────────────
	storage := storagepb.NewStorageServiceClient(authConn)
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
		// FinalizeUpload verifies the bytes at the exact native storage target
		// minted by RegisterUpload. Upload through that presigned URL first; broker
		// PutObject is manifest-gated and may reject the storage service's private
		// bucket, so it is only a fallback when presign is unavailable.
		storageBucket := liveEnv("UDB_STORAGE_BUCKET", "udb-storage")
		if _, err := broker.EnsureResource(brokerCtx, &entityv1.ResourceAdminRequest{Context: rc, Backend: "minio", ResourceName: storageBucket, SpecJson: `{}`}); err != nil {
			t.Logf("perf seed: EnsureResource for storage bucket %q failed: %v", storageBucket, err)
		}
		payload := []byte("sdk-perf-file-" + suffix)
		uploaded := false
		if reg.GetUploadUrl() != "" {
			putCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err := putPresignedStorageObject(putCtx, reg.GetUploadUrl(), payload, "text/plain")
			cancel()
			if err != nil {
				t.Logf("perf seed: presigned storage PUT failed: %v", err)
			} else {
				uploaded = true
			}
		}
		if !uploaded {
			if put, err := broker.PutObject(brokerCtx); err == nil {
				if err := put.Send(&entityv1.Chunk{Context: rc, Bucket: storageBucket, ObjectKey: reg.GetObjectKey(), Data: payload, ContentType: "text/plain", FinalChunk: true}); err != nil {
					t.Logf("perf seed: fallback storage PutObject send failed: %v", err)
				}
				if _, err := put.CloseAndRecv(); err != nil {
					t.Logf("perf seed: fallback storage PutObject close failed: %v", err)
				}
			} else {
				t.Logf("perf seed: fallback storage PutObject open failed: %v", err)
			}
		}
		if _, err := storage.FinalizeUpload(nctx, &storagepb.FinalizeUploadRequest{
			TenantId: uuidTenant, FileId: fid, ContentType: "text/plain", FileType: "DOCUMENT", SizeBytes: 17,
		}); err != nil {
			t.Logf("perf seed: FinalizeUpload seed failed for file_id=%s object_key=%s bucket=%s uploaded=%v: %v", fid, reg.GetObjectKey(), storageBucket, uploaded, err)
		}
		// A SEPARATE disposable file for the destructive DeleteFile, so deleting it
		// does not invalidate the primary file_id read by Get/Update/Finalize.
		if dreg, err := storage.RegisterUpload(nctx, &storagepb.RegisterUploadRequest{
			TenantId: uuidTenant, ProjectId: "", Filename: "perf-del-" + suffix + ".txt", ContentType: "text/plain",
			FileType: "DOCUMENT", ReferenceId: uuid4(), ReferenceType: "sdk.perf", SizeBytes: 64, ExpiresInMinutes: 30,
		}); err == nil {
			fix.set("delete_file_id", dreg.GetFileId())
		}
		// A SEPARATE registered+uploaded but NOT finalized file for the measured
		// FinalizeUpload — finalizing the primary file_id again fails "already
		// finalized", so the measured Finalize needs its own un-finalized target.
		if freg, err := storage.RegisterUpload(nctx, &storagepb.RegisterUploadRequest{
			TenantId: uuidTenant, ProjectId: "", Filename: "perf-fin-" + suffix + ".txt", ContentType: "text/plain",
			FileType: "DOCUMENT", ReferenceId: uuid4(), ReferenceType: "sdk.perf", SizeBytes: 64, ExpiresInMinutes: 30,
		}); err == nil {
			ffid := freg.GetFileId()
			fix.set("finalize_file_id", ffid)
			addCleanup(func() {
				_, _ = storage.DeleteFile(nativeCtxFn(), &storagepb.DeleteFileRequest{TenantId: uuidTenant, FileId: ffid})
			})
			fpayload := []byte("sdk-perf-finalize-" + suffix)
			fuploaded := false
			if freg.GetUploadUrl() != "" {
				putCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				if err := putPresignedStorageObject(putCtx, freg.GetUploadUrl(), fpayload, "text/plain"); err == nil {
					fuploaded = true
				}
				cancel()
			}
			if !fuploaded {
				if put, err := broker.PutObject(brokerCtx); err == nil {
					_ = put.Send(&entityv1.Chunk{Context: rc, Bucket: storageBucket, ObjectKey: freg.GetObjectKey(), Data: fpayload, ContentType: "text/plain", FinalChunk: true})
					_, _ = put.CloseAndRecv()
				}
			}
			// NB: intentionally NOT finalized — the measured FinalizeUpload finalizes it.
		}

		// A registered-but-PENDING upload (never uploaded, never finalized) for the
		// measured ReissueUploadUrl: it resumes a PENDING upload by minting a fresh
		// presigned URL, and rejects any non-PENDING (finalized/ACTIVE) file — so it
		// needs its own upload that stays in the PENDING state for the whole run.
		if rreg, err := storage.RegisterUpload(nctx, &storagepb.RegisterUploadRequest{
			TenantId: uuidTenant, ProjectId: "", Filename: "perf-reissue-" + suffix + ".txt", ContentType: "text/plain",
			FileType: "DOCUMENT", ReferenceId: uuid4(), ReferenceType: "sdk.perf", SizeBytes: 64, ExpiresInMinutes: 30,
		}); err == nil {
			rfid := rreg.GetFileId()
			fix.set("reissue_file_id", rfid)
			addCleanup(func() {
				_, _ = storage.DeleteFile(nativeCtxFn(), &storagepb.DeleteFileRequest{TenantId: uuidTenant, FileId: rfid})
			})
		} else {
			t.Logf("perf seed: RegisterUpload(reissue PENDING) failed: %v", err)
		}

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
			// A SECOND disposable track on the same peer for the destructive
			// UnpublishTrack, so it doesn't end the track MuteTrack/ListTracks read.
			if pub2, err := tracks.PublishTrack(nativeCtxFn(), &webrtcpb.PublishTrackRequest{
				TenantId: uuidTenant, RoomId: roomID, PeerId: pid, Kind: "video", Label: "cam", Settings: `{}`, Metadata: `{}`,
			}); err == nil {
				fix.set("unpublish_track_id", pub2.GetTrackId())
			}
		}
		// A SEPARATE disposable peer for the destructive LeaveRoom, so leaving it does
		// NOT remove the primary peer that PublishTrack/MuteTrack/Signal/IssueCredentials
		// require to be an ACTIVE member (these run in arbitrary mutation order, so a
		// shared peer would be gone before they measure).
		if lj, err := peers.JoinRoom(nativeCtxFn(), &webrtcpb.JoinRoomRequest{
			TenantId: uuidTenant, RoomId: roomID, DisplayName: "sdk-perf-leave-peer", Metadata: `{}`, UserAgent: "sdk-perf",
		}); err == nil {
			fix.set("leave_peer_id", lj.GetPeer().GetPeerId())
		}
		// A SEPARATE disposable peer for the measured Signal stream. The harness opens
		// then CLOSES the bidi Signal stream; the broker's stream-death cleanup
		// (disconnect_peer_sql) transitions the streamed peer CONNECTED→DISCONNECTED.
		// Pointing Signal at the MAIN peer would disconnect it before PublishTrack/
		// IssueCredentials (arbitrary order) → "peer is not an active member".
		if sj, err := peers.JoinRoom(nativeCtxFn(), &webrtcpb.JoinRoomRequest{
			TenantId: uuidTenant, RoomId: roomID, DisplayName: "sdk-perf-signal-peer", Metadata: `{}`, UserAgent: "sdk-perf",
		}); err == nil {
			fix.set("signal_peer_id", sj.GetPeer().GetPeerId())
		}
		// A SEPARATE disposable room for the destructive CloseRoom — closing the MAIN room
		// CLOSES all its peers, breaking PublishTrack/MuteTrack/Signal/IssueCredentials
		// (which run in arbitrary order vs CloseRoom and are all keyed to the main room/peer).
		if cr, err := rooms.CreateRoom(nativeCtxFn(), &webrtcpb.CreateRoomRequest{
			TenantId: uuidTenant, Name: "sdk-perf-close-room-" + suffix, MaxParticipants: 8, Config: `{}`, CreatedBy: uuid4(),
		}); err == nil {
			fix.set("close_room_id", cr.GetRoomId())
		}
		// A SEPARATE high-capacity room for the measured JoinSession. The main
		// room_id is filled to capacity by JoinRoom's mutation iters (3 seeded
		// peers + 5 joins = the cap of 8), so JoinSession against it would hit
		// "room ... at capacity". Its 5 iters each join a fresh peer here.
		if jsr, err := rooms.CreateRoom(nativeCtxFn(), &webrtcpb.CreateRoomRequest{
			TenantId: uuidTenant, Name: "sdk-perf-joinsession-room-" + suffix, MaxParticipants: 64, Config: `{}`, CreatedBy: uuid4(),
		}); err == nil {
			jsrID := jsr.GetRoomId()
			fix.set("join_session_room_id", jsrID)
			addCleanup(func() {
				_, _ = rooms.CloseRoom(nativeCtxFn(), &webrtcpb.CloseRoomRequest{TenantId: uuidTenant, RoomId: jsrID})
			})
		}
	}

	// ── DataBroker migration lifecycle ──
	// migration_id: a dry-run run for GetMigrationStatus/ListMigrationRuns.
	if plan, err := broker.PlanMigration(brokerCtx, &entityv1.MigrationPlanRequest{
		Context: liveRequestContext(tenant, project, "go.live.perf.seed"), ProjectId: project, DryRun: true,
	}); err == nil {
		fix.set("migration_id", plan.GetRunId())
	}
	// approve_run_id: a NON-dry run left in PREFLIGHT for the measured ApproveMigrationPlan.
	if p1, err := broker.PlanMigration(brokerCtx, &entityv1.MigrationPlanRequest{
		Context: liveRequestContext(tenant, project, "go.live.perf.seed.approve"), ProjectId: project, DryRun: false,
	}); err == nil {
		fix.set("approve_run_id", p1.GetRunId())
	} else {
		t.Logf("perf seed: PlanMigration(approve) failed: %v", err)
	}
	// apply_run_id + approval_token: a SECOND non-dry run, pre-approved in the seed so the
	// measured ApplyMigration has a valid token (returned in the x-udb-approval-token header).
	if p2, err := broker.PlanMigration(brokerCtx, &entityv1.MigrationPlanRequest{
		Context: liveRequestContext(tenant, project, "go.live.perf.seed.apply"), ProjectId: project, DryRun: false,
	}); err == nil {
		var md metadata.MD
		if _, err := broker.ApproveMigrationPlan(brokerCtx, &entityv1.MigrationRunRequest{
			Context: liveRequestContext(tenant, project, "go.live.perf.seed.apply"), RunId: p2.GetRunId(), ProjectId: project,
		}, grpc.Header(&md)); err == nil {
			fix.set("apply_run_id", p2.GetRunId())
			if tok := md.Get("x-udb-approval-token"); len(tok) > 0 {
				fix.set("approval_token", tok[0])
			}
		} else {
			t.Logf("perf seed: ApproveMigrationPlan(apply) failed: %v", err)
		}
	}

	// ── DataBroker catalog: capture the live manifest ONLY (for the measured StageCatalog
	// body, which needs a valid CatalogManifest with checksum_sha256). Do NOT Stage+Activate
	// a catalog here: activating the live manifest relabels the active catalog version to
	// "generator-3" (the manifest carries no client-style "version"; catalog_payload_version
	// derives it from generator_version, and the in-memory stage path keeps that label), which
	// the client's pinned "1.0.0" then fails as incompatible — breaking ~80 DataBroker RPCs.
	// This is a broker round-trip limitation (bug_report.md K2), not a harness gap:
	// GetCatalogManifest → StageCatalog → ActivateCatalog cannot preserve the "1.0.0" contract.
	if cm, err := broker.GetCatalogManifest(brokerCtx, &entityv1.CatalogManifestRequest{Context: rc, Redact: false}); err == nil && len(cm.GetManifestJson()) > 0 {
		fix.set("catalog_manifest", string(cm.GetManifestJson()))
		fix.set("catalog_manifest_b64", base64.StdEncoding.EncodeToString(cm.GetManifestJson()))
	}

	// ── DataBroker method-security policy → policy_id (int64) for the destructive DeletePolicy.
	_, _ = broker.PutPolicy(brokerCtx, &entityv1.PutPolicyRequest{Context: rc, Policy: &entityv1.PolicyRecord{
		Effect: "allow", TenantId: tenant, Priority: 1, Enabled: true,
	}})
	if pl, err := broker.ListPolicies(brokerCtx, &entityv1.PolicyListRequest{Context: rc, IncludeDisabled: true, Limit: 50}); err == nil && len(pl.GetPolicies()) > 0 {
		fix.set("ds_policy_id", strconv.FormatInt(pl.GetPolicies()[0].GetPolicyId(), 10))
	}

	// ── Saga + DLQ rows: create recovery state through the served, admin-scoped
	// EnsureBaseline RPC instead of raw udb_system inserts. Each mutating RPC gets
	// its own disposable row because the op transitions status.
	for _, keys := range [][2]string{
		{"saga_id", "dlq_id"},
		{"retry_saga_id", "dismiss_dlq_id"},
		{"mark_saga_id", "quarantine_dlq_id"},
		{"", "replay_dlq_id"},
	} {
		resp, err := broker.EnsureBaseline(brokerCtx, &servicesv1.EnsureBaselineRequest{Context: rc})
		if err != nil {
			t.Logf("perf seed: EnsureBaseline failed: %v", err)
			break
		}
		if keys[0] != "" && len(resp.GetSagaIds()) > 0 {
			fix.set(keys[0], resp.GetSagaIds()[0])
		}
		if len(resp.GetDlqIds()) > 0 {
			fix.set(keys[1], resp.GetDlqIds()[0])
		}
	}
	// ── ControlPlaneService: open a StreamResources session under node_id so a node ──
	// state row exists for (node_id, BACKEND_TARGET_DEFINITION). AckStatus reads that
	// row and 404s without it; only Stream/DeltaResources call ensure_node_state, and
	// in the measured run AckStatus sorts BEFORE StreamResources, so register it here.
	nodeID := "sdk-perf-node-" + suffix
	fix.set("node_id", nodeID)
	cpCtx := &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: project}, Purpose: "go.live.perf.seed"}
	if cs, err := controlpb.NewControlPlaneServiceClient(authConn).StreamResources(base); err == nil {
		_ = cs.Send(&controlpb.DiscoveryRequest{
			NodeId: nodeID, ResourceType: controlentpb.ResourceType_RESOURCE_TYPE_BACKEND_TARGET_DEFINITION, Context: cpCtx,
		})
		// First server response confirms the node-state row was created; then close.
		if resp, err := cs.Recv(); err == nil && resp.GetVersionInfo() != "" {
			fix.set("rollback_resource_version", resp.GetVersionInfo())
			if len(resp.GetResources()) > 0 {
				fix.set("resource_name", resp.GetResources()[0].GetName())
			}
		}
		_ = cs.CloseSend()
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
	// node_id is seeded above (a StreamResources session is opened under it so AckStatus
	// finds a node-state row).

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
