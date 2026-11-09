// Command native-services is the Go tour of UDB's native control plane: it logs
// in as the bootstrapped admin on the real enterprise path and makes at least one
// real call to each of the 28 native services, printing one numbered line per
// service.
//
// It is the reference for the TypeScript and Python tours next to it: same steps,
// same requests, same output lines.
//
// Run (after examples/ts_enterprise/scripts/bootstrap.sh + serve.sh):
//
//	set -a; source ../../ts_enterprise/secrets/enterprise.env; set +a
//	go run .
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	udb "github.com/fahara02/udb/sdk/go/udbclient"

	analyticspb "github.com/fahara02/udb/sdk/go/gen/udb/core/analytics/services/v1"
	apikeypb "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/services/v1"
	assetpb "github.com/fahara02/udb/sdk/go/gen/udb/core/asset/services/v1"
	authnentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/entity/v1"
	authnpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	authzpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
	backuppb "github.com/fahara02/udb/sdk/go/gen/udb/core/backup/services/v1"
	cachepb "github.com/fahara02/udb/sdk/go/gen/udb/core/cache/services/v1"
	commonpb "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	configpb "github.com/fahara02/udb/sdk/go/gen/udb/core/config/services/v1"
	controlentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/control/entity/v1"
	controlpb "github.com/fahara02/udb/sdk/go/gen/udb/core/control/services/v1"
	embeddingpb "github.com/fahara02/udb/sdk/go/gen/udb/core/embedding/services/v1"
	idpentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/idp/entity/v1"
	idppb "github.com/fahara02/udb/sdk/go/gen/udb/core/idp/services/v1"
	livequerypb "github.com/fahara02/udb/sdk/go/gen/udb/core/livequery/services/v1"
	lockpb "github.com/fahara02/udb/sdk/go/gen/udb/core/lock/services/v1"
	messagingpb "github.com/fahara02/udb/sdk/go/gen/udb/core/messaging/services/v1"
	meteringpb "github.com/fahara02/udb/sdk/go/gen/udb/core/metering/services/v1"
	notifentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/notification/entity/v1"
	notifpb "github.com/fahara02/udb/sdk/go/gen/udb/core/notification/services/v1"
	schedulerpb "github.com/fahara02/udb/sdk/go/gen/udb/core/scheduler/services/v1"
	searchpb "github.com/fahara02/udb/sdk/go/gen/udb/core/search/services/v1"
	storagepb "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/services/v1"
	tenantpb "github.com/fahara02/udb/sdk/go/gen/udb/core/tenant/services/v1"
	vaultpb "github.com/fahara02/udb/sdk/go/gen/udb/core/vault/services/v1"
	webhookpb "github.com/fahara02/udb/sdk/go/gen/udb/core/webhook/services/v1"
	webrtcpb "github.com/fahara02/udb/sdk/go/gen/udb/core/webrtc/services/v1"
	workflowpb "github.com/fahara02/udb/sdk/go/gen/udb/core/workflow/services/v1"
	errorpb "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const totalServices = 28

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ── errors ──────────────────────────────────────────────────────────────────

// reasonConn wraps the session's control-plane connection so every unary error
// also carries the service's own "error-reason" trailer (e.g. TURN_NOT_CONFIGURED).
// The typed ErrorDetail (udb-error-detail-bin) is already decoded by the SDK.
type reasonConn struct{ grpc.ClientConnInterface }

type reasonErr struct {
	err    error
	reason string
}

func (e *reasonErr) Error() string { return e.err.Error() }
func (e *reasonErr) Unwrap() error { return e.err }

func (c reasonConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	var trailer metadata.MD
	err := c.ClientConnInterface.Invoke(ctx, method, args, reply, append(opts, grpc.Trailer(&trailer))...)
	if err != nil {
		reason := ""
		if v := trailer.Get("error-reason"); len(v) > 0 {
			reason = v[len(v)-1]
		}
		return &reasonErr{err: err, reason: reason}
	}
	return nil
}

// bodyErr is a refusal the service returned inside an OK response (ApiError).
type bodyErr struct{ rpc, code, message string }

func (e *bodyErr) Error() string { return fmt.Sprintf("%s returned %s: %s", e.rpc, e.code, e.message) }

func apiErr(rpc string, e *commonpb.ApiError) error {
	if e != nil && e.GetCode() != "" {
		return &bodyErr{rpc: rpc, code: e.GetCode(), message: e.GetMessage()}
	}
	return nil
}

// notConfigured matches the domain reasons a service uses when its backend or
// secret is not set up on this broker (UPLOAD_URL_UNAVAILABLE, TURN_NOT_CONFIGURED, ...).
var notConfigured = regexp.MustCompile(`(NOT_CONFIGURED|NOT_AVAILABLE|UNAVAILABLE)$`)

type rpcInfo struct {
	rpc        bool // false: a local check failed, not a broker call
	code       codes.Code
	message    string
	reason     string // the broker's typed reason (UDB_*), "" when none
	domain     string // the service's own error-reason trailer, "" when none
	capability string // ErrorDetail.capability_required
	kind       errorpb.ErrorKind
}

func inspect(err error) rpcInfo {
	info := rpcInfo{code: codes.Unknown, message: err.Error()}
	if st, ok := status.FromError(err); ok {
		info.rpc, info.code, info.message = true, st.Code(), st.Message()
	}
	var re *reasonErr
	if errors.As(err, &re) {
		info.domain = re.reason
	}
	var ue *udb.Error
	if errors.As(err, &ue) {
		info.rpc, info.code, info.message = true, ue.Code, ue.Message
		if d, ok := ue.Detail(); ok {
			info.reason, info.capability, info.kind = d.GetReason(), d.GetCapabilityRequired(), d.GetKind()
		}
	}
	return info
}

// skipReason reports whether err is the broker saying "this service's backend is
// not configured here", and if so the reason line to print. Only a typed
// capability refusal (FAILED_PRECONDITION + ERROR_KIND_CAPABILITY or a
// *_NOT_CONFIGURED / *_UNAVAILABLE reason) qualifies; anything else is a failure.
func skipReason(err error) (string, bool) {
	var be *bodyErr
	if errors.As(err, &be) {
		if notConfigured.MatchString(be.code) {
			return be.code + ": " + be.message, true
		}
		return "", false
	}
	info := inspect(err)
	if info.code != codes.FailedPrecondition {
		return "", false
	}
	capability := info.kind == errorpb.ErrorKind_ERROR_KIND_CAPABILITY || info.reason == "UDB_CAPABILITY_UNSUPPORTED"
	if !capability && !notConfigured.MatchString(info.domain) {
		return "", false
	}
	reason := info.domain
	if reason == "" {
		reason = info.reason
	}
	if reason == "" {
		reason = "UDB_CAPABILITY_UNSUPPORTED"
	}
	if info.capability != "" {
		reason += " (" + info.capability + ")"
	}
	return reason + ": " + info.message, true
}

var camel = regexp.MustCompile(`([a-z])([A-Z])`)

func describe(err error) string {
	var be *bodyErr
	if errors.As(err, &be) {
		return be.Error()
	}
	info := inspect(err)
	if !info.rpc {
		return err.Error()
	}
	code := strings.ToUpper(camel.ReplaceAllString(info.code.String(), "${1}_${2}"))
	reason := info.domain
	if reason == "" {
		reason = info.reason
	}
	if reason != "" {
		return fmt.Sprintf("%s %s: %s", code, reason, info.message)
	}
	return fmt.Sprintf("%s: %s", code, info.message)
}

// errSkipped / errNeeds mark a step that could not run because an earlier step it
// depends on was skipped or failed.
type depErr struct {
	msg     string
	skipped bool
}

func (e *depErr) Error() string { return e.msg }

// ── tour ────────────────────────────────────────────────────────────────────

type tour struct {
	s       *udb.EnterpriseSession
	conn    grpc.ClientConnInterface
	tenant  string // canonical tenant UUID
	project string // verified project
	actor   string // admin user id
	suffix  string

	n, called, skipped int
	failures           []string
	status             map[string]string // service -> "ok" | "skipped" | "failed"

	// shared WebRTC state
	roomID, peerID string
}

func (t *tour) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(t.s.NativeContext(context.Background()), 15*time.Second)
}

func (t *tour) step(service string, optional bool, fn func() (string, error)) {
	t.n++
	prefix := fmt.Sprintf("%02d %s:", t.n, service)
	msg, err := fn()
	var dep *depErr
	switch {
	case err == nil:
		t.called++
		t.status[service] = "ok"
		fmt.Println(prefix, msg)
	case errors.As(err, &dep) && dep.skipped:
		t.skipped++
		t.status[service] = "skipped"
		fmt.Println(prefix, "skipped —", dep.msg)
	case optional:
		if reason, ok := skipReason(err); ok {
			t.skipped++
			t.status[service] = "skipped"
			fmt.Println(prefix, "skipped —", reason)
			return
		}
		fallthrough
	default:
		t.status[service] = "failed"
		line := describe(err)
		t.failures = append(t.failures, fmt.Sprintf("%02d %s: %s", t.n, service, line))
		fmt.Println(prefix, "FAILED —", line)
	}
}

// needs returns an error when a service this step depends on did not succeed.
func (t *tour) needs(service string) error {
	switch t.status[service] {
	case "ok":
		return nil
	case "skipped":
		return &depErr{msg: "needs " + service + ", which was skipped above", skipped: true}
	default:
		return &depErr{msg: "needs " + service + ", which failed above"}
	}
}

func randomSuffix() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func uuid4() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func main() {
	password := os.Getenv("UDB_PASS")
	if password == "" {
		fmt.Fprintln(os.Stderr, "set UDB_PASS to the admin password bootstrap.sh wrote to secrets/enterprise.env")
		os.Exit(2)
	}
	tenantCode := env("UDB_TENANT", "acme")
	username := env("UDB_USER", "admin")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	session, err := udb.ConnectEnterprise(ctx, udb.EnterpriseConfig{
		Target:     env("UDB_TARGET", "127.0.0.1:50051"),
		AuthTarget: env("UDB_AUTH_TARGET", "127.0.0.1:50061"),
		Username:   username,
		Password:   password,
		TenantCode: tenantCode,
		ProjectID:  env("UDB_PROJECT", "default"),
		Purpose:    "native-services-tour",
	})
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "login failed: %v\n", err)
		os.Exit(1)
	}
	defer session.Close()

	t := &tour{
		s:       session,
		conn:    reasonConn{session.NativeConn()},
		tenant:  session.CanonicalTenantID,
		project: session.CanonicalProjectID,
		actor:   session.Principal.GetUserId(),
		suffix:  randomSuffix(),
		status:  map[string]string{},
	}
	fmt.Printf("logged in as %s (tenant code %q -> %s)\n", username, tenantCode, t.tenant)

	t.step("AuthnService", false, t.authn)
	t.step("TenantService", false, t.tenantSvc)
	t.step("AuthzService", false, t.authz)
	t.step("ApiKeyService", false, t.apiKey)
	t.step("IdentityProviderService", false, t.idp)
	t.step("ConfigService", false, t.config)
	t.step("LockService", false, t.lock)
	t.step("LiveQueryService", false, t.liveQuery)
	t.step("CacheService", true, t.cache)
	t.step("SchedulerService", false, t.scheduler)
	t.step("WorkflowService", false, t.workflow)
	t.step("WebhookService", false, t.webhook)
	t.step("VaultService", true, t.vault)
	t.step("NotificationService", true, t.notification)
	t.step("MeteringService", true, t.metering)
	t.step("AnalyticsService", false, t.analytics)
	t.step("BackupService", false, t.backup)
	t.step("ControlPlaneService", false, t.controlPlane)
	t.step("StorageService", true, t.storage)
	t.step("AssetService", true, t.asset)
	t.step("SearchService", true, t.search)
	t.step("EmbeddingService", true, t.embedding)
	t.step("MessagingService", true, t.messaging)
	t.step("RoomService", true, t.room)
	t.step("PeerService", true, t.peer)
	t.step("TrackService", true, t.track)
	t.step("TurnService", true, t.turn)
	t.step("SignalingService", true, t.signaling)

	fmt.Println()
	if len(t.failures) > 0 {
		fmt.Printf("FAILED %d/%d services (%d called, %d skipped):\n", len(t.failures), totalServices, t.called, t.skipped)
		for _, f := range t.failures {
			fmt.Println("  " + f)
		}
		os.Exit(1)
	}
	fmt.Printf("OK %d/%d services called, %d skipped (backend not configured)\n", t.called, totalServices, t.skipped)
}

// ── steps (one per native service) ──────────────────────────────────────────

func (t *tour) authn() (string, error) {
	c := authnpb.NewAuthnServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	got, err := c.GetUser(ctx, &authnpb.GetUserRequest{UserId: t.actor})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("GetUser -> %q (%s)", got.GetUser().GetUsername(), got.GetUser().GetStatus()), nil
}

func (t *tour) tenantSvc() (string, error) {
	c := tenantpb.NewTenantServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	if _, err := c.UpdateTenantConfig(ctx, &tenantpb.UpdateTenantConfigRequest{
		TenantId: t.tenant, ConfigKey: "native_tour_max_users", ConfigValue: "200", Type: "NUMBER",
	}); err != nil {
		return "", err
	}
	cfg, err := c.GetTenantConfig(ctx, &tenantpb.GetTenantConfigRequest{TenantId: t.tenant})
	if err != nil {
		return "", err
	}
	if err := apiErr("GetTenantConfig", cfg.GetError()); err != nil {
		return "", err
	}
	for _, kv := range cfg.GetConfigs() {
		if kv.GetConfigKey() == "native_tour_max_users" {
			return "UpdateTenantConfig + GetTenantConfig -> native_tour_max_users=" + kv.GetConfigValue(), nil
		}
	}
	return "", errors.New("GetTenantConfig did not return native_tour_max_users")
}

func (t *tour) authz() (string, error) {
	c := authzpb.NewAuthzServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	code := "native_tour_" + t.suffix
	created, err := c.CreateRole(ctx, &authzpb.CreateRoleRequest{
		Name: "Native tour " + t.suffix, Description: "native-services tour role", CreatedBy: t.actor,
		RoleCode: code, Domain: t.tenant, TenantId: t.tenant, ProjectId: t.project,
	})
	if err != nil {
		return "", err
	}
	roleID := created.GetRole().GetRoleId()
	got, err := c.GetRole(ctx, &authzpb.GetRoleRequest{RoleId: roleID})
	if err == nil && got.GetRole().GetRoleCode() != code {
		err = fmt.Errorf("GetRole returned role_code %q, want %q", got.GetRole().GetRoleCode(), code)
	}
	var check *authzpb.CheckAccessResponse
	if err == nil {
		check, err = c.CheckAccess(ctx, &authzpb.CheckAccessRequest{
			UserId: t.actor, Domain: t.tenant, TenantId: t.tenant, ProjectId: t.project,
			Object: "billing.v1.Invoice", Action: "Select",
		})
	}
	_, delErr := c.DeleteRole(ctx, &authzpb.DeleteRoleRequest{RoleId: roleID, DeletedBy: t.actor})
	if err != nil {
		return "", err
	}
	if delErr != nil {
		return "", delErr
	}
	return fmt.Sprintf("CreateRole + GetRole + DeleteRole ok; CheckAccess(admin, Select billing.v1.Invoice) -> allowed=%t", check.GetAllowed()), nil
}

func (t *tour) apiKey() (string, error) {
	authn := authnpb.NewAuthnServiceClient(t.conn)
	keys := apikeypb.NewApiKeyServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	tenantCtx := &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: t.tenant, ProjectId: t.project}}

	// An API key belongs to a service account, and a service account needs an
	// active grant (its approved scopes) before it can hold a key.
	name := "native-tour-svc-" + t.suffix
	sa, err := authn.CreateUser(ctx, &authnpb.CreateUserRequest{
		Username: name, Email: name + "@example.com", Password: "CorrectHorse1!",
		TenantId: t.tenant, ProjectId: t.project, FullName: "Native tour service account",
		AccountKind: authnentpb.AccountKind_ACCOUNT_KIND_SERVICE_ACCOUNT,
	})
	if err != nil {
		return "", err
	}
	owner := sa.GetUser().GetUserId()
	deactivate := func() error {
		_, err := authn.ChangeUserStatus(ctx, &authnpb.ChangeUserStatusRequest{
			UserId: owner, NewStatus: authnentpb.UserStatus_USER_STATUS_DEACTIVATED,
			Reason: "native tour cleanup", Context: tenantCtx,
		})
		return err
	}
	if _, err := authn.ChangeUserStatus(ctx, &authnpb.ChangeUserStatusRequest{
		UserId: owner, NewStatus: authnentpb.UserStatus_USER_STATUS_ACTIVE,
		Reason: "native tour service account", Context: tenantCtx,
	}); err != nil {
		_ = deactivate()
		return "", err
	}
	if _, err := authn.CreateServiceAccountGrant(ctx, &authnpb.CreateServiceAccountGrantRequest{
		TenantId: t.tenant, ProjectId: t.project, UserId: owner, ServiceIdentity: name,
		ApprovedScopes: []string{"data:read"}, Reason: "native tour API key",
	}); err != nil {
		_ = deactivate()
		return "", err
	}
	keyCtx := &commonpb.RequestContext{UserId: owner, Tenant: tenantCtx.Tenant}
	created, err := keys.CreateApiKey(ctx, &apikeypb.CreateApiKeyRequest{
		Name: "native-tour-key-" + t.suffix, OwnerId: owner, Scopes: []string{"data:read"}, Context: keyCtx,
	})
	var valid *apikeypb.ValidateApiKeyResponse
	if err == nil {
		valid, err = keys.ValidateApiKey(ctx, &apikeypb.ValidateApiKeyRequest{
			PlainKey: created.GetPlainKey(), RequiredScope: "data:read",
		})
		if err == nil && (!valid.GetValid() || valid.GetOwnerId() != owner) {
			err = fmt.Errorf("ValidateApiKey valid=%t owner=%q, want true/%q", valid.GetValid(), valid.GetOwnerId(), owner)
		}
		if _, revokeErr := keys.RevokeApiKey(ctx, &apikeypb.RevokeApiKeyRequest{
			KeyId: created.GetKey().GetKeyId(), RevokeReason: "native tour cleanup", Context: keyCtx,
		}); err == nil {
			err = revokeErr
		}
	}
	_, grantErr := authn.RevokeServiceAccountGrant(ctx, &authnpb.RevokeServiceAccountGrantRequest{
		TenantId: t.tenant, UserId: owner, Reason: "native tour cleanup",
	})
	userErr := deactivate()
	for _, e := range []error{err, grantErr, userErr} {
		if e != nil {
			return "", e
		}
	}
	return fmt.Sprintf("CreateApiKey(service account) + ValidateApiKey(data:read) -> valid=%t; RevokeApiKey ok", valid.GetValid()), nil
}

func (t *tour) idp() (string, error) {
	c := idppb.NewIdentityProviderServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	list, err := c.ListProviders(ctx, &idppb.ListProvidersRequest{
		TenantId: t.tenant, Kind: idpentpb.IdpKind_IDP_KIND_UNSPECIFIED,
		Page: &commonpb.PageRequest{Page: 1, PageSize: 20},
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ListProviders -> %d provider(s)", len(list.GetProviders())), nil
}

func (t *tour) config() (string, error) {
	c := configpb.NewConfigServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	key := "native_tour." + t.suffix
	put, err := c.PutFlag(ctx, &configpb.PutFlagRequest{
		TenantId: t.tenant, ProjectId: t.project, Environment: "prod", FlagKey: key,
		Value:   &configpb.FlagValue{Value: &configpb.FlagValue_BoolValue{BoolValue: true}},
		Enabled: true, RolloutPercentage: 100, RolloutContextKey: "user_id", MetadataJson: "{}",
	})
	if err == nil {
		err = apiErr("PutFlag", put.GetError())
	}
	if err != nil {
		return "", err
	}
	got, err := c.GetFlag(ctx, &configpb.GetFlagRequest{TenantId: t.tenant, ProjectId: t.project, Environment: "prod", FlagKey: key})
	if err == nil {
		err = apiErr("GetFlag", got.GetError())
	}
	del, delErr := c.DeleteFlag(ctx, &configpb.DeleteFlagRequest{TenantId: t.tenant, ProjectId: t.project, Environment: "prod", FlagKey: key})
	if delErr == nil {
		delErr = apiErr("DeleteFlag", del.GetError())
	}
	if err != nil {
		return "", err
	}
	if delErr != nil {
		return "", delErr
	}
	if !got.GetFound() || !got.GetFlag().GetValue().GetBoolValue() {
		return "", fmt.Errorf("GetFlag found=%t value=%v, want the flag just written", got.GetFound(), got.GetFlag().GetValue())
	}
	return "PutFlag + GetFlag -> bool_value=true; DeleteFlag ok", nil
}

func (t *tour) lock() (string, error) {
	c := lockpb.NewLockServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	name := "native-tour-lock-" + t.suffix
	acq, err := c.AcquireLock(ctx, &lockpb.AcquireLockRequest{
		TenantId: t.tenant, LockName: name, OwnerId: t.actor, LeaseTtlSeconds: 60, MetadataJson: "{}",
	})
	if err == nil {
		err = apiErr("AcquireLock", acq.GetError())
	}
	if err == nil && !acq.GetAcquired() {
		err = fmt.Errorf("AcquireLock acquired=false: %s", acq.GetMessage())
	}
	if err != nil {
		return "", err
	}
	got, err := c.GetLock(ctx, &lockpb.GetLockRequest{TenantId: t.tenant, LockName: name})
	if err == nil {
		err = apiErr("GetLock", got.GetError())
	}
	rel, relErr := c.ReleaseLock(ctx, &lockpb.ReleaseLockRequest{
		TenantId: t.tenant, LockName: name, OwnerId: t.actor, FencingToken: acq.GetFencingToken(),
	})
	if relErr == nil {
		relErr = apiErr("ReleaseLock", rel.GetError())
	}
	if err != nil {
		return "", err
	}
	if relErr != nil {
		return "", relErr
	}
	if !got.GetFound() {
		return "", errors.New("GetLock did not find the lock just acquired")
	}
	return fmt.Sprintf("AcquireLock -> fencing token issued; GetLock -> found=%t; ReleaseLock -> released=%t", got.GetFound(), rel.GetReleased()), nil
}

func (t *tour) liveQuery() (string, error) {
	locks := lockpb.NewLockServiceClient(t.conn)
	c := livequerypb.NewLiveQueryServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	// Hold a lock so the subscription's snapshot has a row to show.
	name := "native-tour-live-" + t.suffix
	acq, err := locks.AcquireLock(ctx, &lockpb.AcquireLockRequest{
		TenantId: t.tenant, LockName: name, OwnerId: t.actor, LeaseTtlSeconds: 60, MetadataJson: "{}",
	})
	if err == nil {
		err = apiErr("AcquireLock", acq.GetError())
	}
	if err != nil {
		return "", err
	}
	release := func() error {
		rel, err := locks.ReleaseLock(ctx, &lockpb.ReleaseLockRequest{
			TenantId: t.tenant, LockName: name, OwnerId: t.actor, FencingToken: acq.GetFencingToken(),
		})
		if err == nil {
			err = apiErr("ReleaseLock", rel.GetError())
		}
		return err
	}

	subCtx, stop := context.WithCancel(ctx)
	stream, err := c.Subscribe(subCtx, &livequerypb.SubscribeRequest{
		TenantId: t.tenant, MessageType: "udb.core.lock.entity.v1.Lock", SnapshotLimit: 10,
		Filters: []*livequerypb.LiveQueryPredicate{{
			Field: "lock_name", Op: livequerypb.LiveQueryComparison_LIVE_QUERY_COMPARISON_EQ, Value: name,
		}},
	})
	var rows int64 = -1
	for err == nil && rows < 0 {
		var frame *livequerypb.SubscribeResponse
		frame, err = stream.Recv()
		if err != nil {
			break
		}
		if err = apiErr("Subscribe", frame.GetError()); err != nil {
			break
		}
		if snap := frame.GetSnapshot(); snap != nil {
			rows = snap.GetRowCount()
		}
	}
	stop()
	relErr := release()
	if err != nil {
		return "", err
	}
	if relErr != nil {
		return "", relErr
	}
	return fmt.Sprintf("Subscribe(udb.core.lock.entity.v1.Lock, lock_name=...) -> snapshot with %d row(s)", rows), nil
}

func (t *tour) cache() (string, error) {
	c := cachepb.NewCacheServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	ns := "native-tour-" + t.suffix
	created, err := c.CreateNamespace(ctx, &cachepb.CreateNamespaceRequest{
		TenantId: t.tenant, Namespace: ns, MaxBytes: 1 << 20, DefaultTtlSeconds: 300,
	})
	if err == nil {
		err = apiErr("CreateNamespace", created.GetError())
	}
	if err != nil {
		return "", err
	}
	set, err := c.Set(ctx, &cachepb.SetRequest{TenantId: t.tenant, Namespace: ns, Key: "greeting", Value: []byte("hello"), TtlSeconds: 300})
	if err == nil {
		err = apiErr("Set", set.GetError())
	}
	var got *cachepb.GetResponse
	if err == nil {
		got, err = c.Get(ctx, &cachepb.GetRequest{TenantId: t.tenant, Namespace: ns, Key: "greeting"})
		if err == nil {
			err = apiErr("Get", got.GetError())
		}
	}
	del, delErr := c.DeleteNamespace(ctx, &cachepb.DeleteNamespaceRequest{TenantId: t.tenant, Namespace: ns, ConfirmationToken: ns})
	if delErr == nil {
		delErr = apiErr("DeleteNamespace", del.GetError())
	}
	if err != nil {
		return "", err
	}
	if delErr != nil {
		return "", delErr
	}
	if !got.GetFound() || string(got.GetValue()) != "hello" {
		return "", fmt.Errorf("Get found=%t value=%q, want \"hello\"", got.GetFound(), got.GetValue())
	}
	return `CreateNamespace + Set + Get -> "hello"; DeleteNamespace ok`, nil
}

func (t *tour) scheduler() (string, error) {
	c := schedulerpb.NewSchedulerServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	created, err := c.CreateJob(ctx, &schedulerpb.CreateJobRequest{
		TenantId: t.tenant, ProjectId: t.project, Name: "native-tour-" + t.suffix,
		ScheduleType: "CRON", CronExpression: "*/5 * * * *", Payload: "{}",
		TargetTopic: "native_tour.scheduler", MaxAttempts: 3, BackoffSeconds: 30,
	})
	if err == nil {
		err = apiErr("CreateJob", created.GetError())
	}
	if err != nil {
		return "", err
	}
	got, err := c.GetJob(ctx, &schedulerpb.GetJobRequest{TenantId: t.tenant, JobId: created.GetJobId()})
	if err == nil {
		err = apiErr("GetJob", got.GetError())
	}
	del, delErr := c.DeleteJob(ctx, &schedulerpb.DeleteJobRequest{TenantId: t.tenant, JobId: created.GetJobId()})
	if delErr == nil {
		delErr = apiErr("DeleteJob", del.GetError())
	}
	if err != nil {
		return "", err
	}
	if delErr != nil {
		return "", delErr
	}
	return fmt.Sprintf("CreateJob(*/5 * * * *) + GetJob -> %s; DeleteJob ok", got.GetJob().GetStatus()), nil
}

func (t *tour) workflow() (string, error) {
	c := workflowpb.NewWorkflowServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	started, err := c.StartWorkflow(ctx, &workflowpb.StartWorkflowRequest{
		TenantId: t.tenant, ProjectId: t.project, WorkflowType: "native_tour.demo", TotalSteps: 1,
		Payload: "{}", Compensations: "[]", CorrelationId: "native-tour-" + t.suffix,
	})
	if err == nil {
		err = apiErr("StartWorkflow", started.GetError())
	}
	if err != nil {
		return "", err
	}
	got, err := c.GetWorkflow(ctx, &workflowpb.GetWorkflowRequest{TenantId: t.tenant, WorkflowId: started.GetWorkflowId()})
	if err == nil {
		err = apiErr("GetWorkflow", got.GetError())
	}
	cancelled, cancelErr := c.CancelWorkflow(ctx, &workflowpb.CancelWorkflowRequest{
		TenantId: t.tenant, WorkflowId: started.GetWorkflowId(), Reason: "native tour cleanup",
	})
	if cancelErr == nil {
		cancelErr = apiErr("CancelWorkflow", cancelled.GetError())
	}
	if err != nil {
		return "", err
	}
	if cancelErr != nil {
		return "", cancelErr
	}
	return fmt.Sprintf("StartWorkflow + GetWorkflow -> %s; CancelWorkflow ok", got.GetWorkflow().GetStatus()), nil
}

func (t *tour) webhook() (string, error) {
	c := webhookpb.NewWebhookServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	created, err := c.CreateEndpoint(ctx, &webhookpb.CreateEndpointRequest{
		TenantId: t.tenant, Url: "https://example.com/udb-native-tour", TopicPattern: "native_tour.*",
		Description: "native-services tour", MaxAttempts: 3, MetadataJson: "{}",
	})
	if err == nil {
		err = apiErr("CreateEndpoint", created.GetError())
	}
	if err != nil {
		return "", err
	}
	got, err := c.GetEndpoint(ctx, &webhookpb.GetEndpointRequest{TenantId: t.tenant, EndpointId: created.GetEndpointId()})
	if err == nil {
		err = apiErr("GetEndpoint", got.GetError())
	}
	del, delErr := c.DeleteEndpoint(ctx, &webhookpb.DeleteEndpointRequest{TenantId: t.tenant, EndpointId: created.GetEndpointId()})
	if delErr == nil {
		delErr = apiErr("DeleteEndpoint", del.GetError())
	}
	if err != nil {
		return "", err
	}
	if delErr != nil {
		return "", delErr
	}
	return fmt.Sprintf("CreateEndpoint + GetEndpoint -> %s; DeleteEndpoint ok", got.GetEndpoint().GetUrl()), nil
}

func (t *tour) vault() (string, error) {
	c := vaultpb.NewVaultServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	path := "native-tour/" + t.suffix + "/db-password"
	put, err := c.PutSecret(ctx, &vaultpb.PutSecretRequest{
		TenantId: t.tenant, SecretPath: path, SecretValue: "s3cr3t", ExpectedVersion: 0, MetadataJson: "{}",
	})
	if err == nil {
		err = apiErr("PutSecret", put.GetError())
	}
	if err != nil {
		return "", err
	}
	got, err := c.GetSecret(ctx, &vaultpb.GetSecretRequest{TenantId: t.tenant, SecretPath: path})
	if err == nil {
		err = apiErr("GetSecret", got.GetError())
	}
	del, delErr := c.DeleteSecret(ctx, &vaultpb.DeleteSecretRequest{TenantId: t.tenant, SecretPath: path})
	if delErr == nil {
		delErr = apiErr("DeleteSecret", del.GetError())
	}
	if err != nil {
		return "", err
	}
	if delErr != nil {
		return "", delErr
	}
	if got.GetSecretValue() != "s3cr3t" {
		return "", errors.New("GetSecret did not return the value just written")
	}
	return fmt.Sprintf("PutSecret + GetSecret -> version %d round-trips; DeleteSecret ok", got.GetVersion()), nil
}

func (t *tour) notification() (string, error) {
	c := notifpb.NewNotificationServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	event := "native_tour." + t.suffix
	email := notifentpb.NotificationChannel_NOTIFICATION_CHANNEL_EMAIL
	if _, err := c.UpsertTemplate(ctx, &notifpb.UpsertTemplateRequest{
		EventType: event, Channel: email, Locale: "en",
		SubjectTemplate: "Native tour", BodyTemplate: "Hello {{name}}", IsActive: true,
	}); err != nil {
		return "", err
	}
	sent, err := c.SendNotification(ctx, &notifpb.SendNotificationRequest{
		EventType: event, RecipientId: t.actor, RecipientAddress: "native-tour@example.com",
		TenantId: t.tenant, ProjectId: t.project, Locale: "en",
		Variables: map[string]string{"name": "UDB"}, Channels: []notifentpb.NotificationChannel{email},
	})
	if err != nil {
		return "", err
	}
	if len(sent.GetLogs()) == 0 {
		return "", errors.New("SendNotification returned no delivery log")
	}
	got, err := c.GetNotification(ctx, &notifpb.GetNotificationRequest{LogId: sent.GetLogs()[0].GetLogId()})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("UpsertTemplate + SendNotification(EMAIL) + GetNotification -> %s", got.GetLog().GetStatus()), nil
}

func (t *tour) metering() (string, error) {
	c := meteringpb.NewMeteringServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	metric := "native_tour." + t.suffix
	rec, err := c.RecordUsage(ctx, &meteringpb.RecordUsageRequest{
		TenantId: t.tenant, PrincipalId: t.actor, Method: metric, Unit: "request", Quantity: 3, MetadataJson: "{}",
	})
	if err == nil {
		err = apiErr("RecordUsage", rec.GetError())
	}
	if err != nil {
		return "", err
	}
	used, err := c.QueryUsage(ctx, &meteringpb.QueryUsageRequest{TenantId: t.tenant, Metric: metric, WindowSeconds: 3600})
	if err == nil {
		err = apiErr("QueryUsage", used.GetError())
	}
	if err != nil {
		return "", err
	}
	if used.GetUsed() != 3 {
		return "", fmt.Errorf("QueryUsage used=%d, want 3", used.GetUsed())
	}
	return fmt.Sprintf("RecordUsage(3) + QueryUsage(1h) -> used=%d", used.GetUsed()), nil
}

func (t *tour) analytics() (string, error) {
	c := analyticspb.NewAnalyticsServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	acc, err := c.RecordPipelineMetric(ctx, &analyticspb.RecordPipelineMetricRequest{
		StageName: "native_tour_" + t.suffix, TenantId: t.tenant, LatencyMs: 12.5, IsSuccess: true,
	})
	if err != nil {
		return "", err
	}
	if !acc.GetAccepted() {
		return "", errors.New("RecordPipelineMetric accepted=false")
	}
	through, err := c.GetThroughput(ctx, &analyticspb.GetThroughputRequest{TenantId: t.tenant})
	if err != nil {
		return "", err
	}
	if through.GetTotalRequests() < 1 {
		return "", fmt.Errorf("GetThroughput total_requests=%d, want >= 1", through.GetTotalRequests())
	}
	return "RecordPipelineMetric + GetThroughput -> total_requests >= 1", nil
}

func (t *tour) backup() (string, error) {
	c := backuppb.NewBackupServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	name := "native-tour-" + t.suffix
	put, err := c.PutBackupPolicy(ctx, &backuppb.PutBackupPolicyRequest{
		TenantId: t.tenant, PolicyName: name, ScheduleCron: "0 3 * * *",
		RetentionDays: 7, MaxRetainedBackups: 3, Enabled: true, MetadataJson: "{}",
	})
	if err == nil {
		err = apiErr("PutBackupPolicy", put.GetError())
	}
	if err != nil {
		return "", err
	}
	got, err := c.GetBackupPolicy(ctx, &backuppb.GetBackupPolicyRequest{TenantId: t.tenant, PolicyName: name})
	if err == nil {
		err = apiErr("GetBackupPolicy", got.GetError())
	}
	del, delErr := c.DeleteBackupPolicy(ctx, &backuppb.DeleteBackupPolicyRequest{TenantId: t.tenant, PolicyName: name})
	if delErr == nil {
		delErr = apiErr("DeleteBackupPolicy", del.GetError())
	}
	if err != nil {
		return "", err
	}
	if delErr != nil {
		return "", delErr
	}
	return fmt.Sprintf("PutBackupPolicy + GetBackupPolicy -> %q retention %d days; DeleteBackupPolicy ok",
		got.GetPolicy().GetScheduleCron(), got.GetPolicy().GetRetentionDays()), nil
}

func (t *tour) controlPlane() (string, error) {
	c := controlpb.NewControlPlaneServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	list, err := c.ListNodeStates(ctx, &controlpb.ListNodeStatesRequest{
		ResourceType: controlentpb.ResourceType_RESOURCE_TYPE_UNSPECIFIED,
		Page:         &commonpb.PageRequest{Page: 1, PageSize: 50},
		Context:      &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: t.tenant}},
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ListNodeStates -> %d node state(s)", len(list.GetNodeStates())), nil
}

func (t *tour) storage() (string, error) {
	c := storagepb.NewStorageServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	reg, err := c.RegisterUpload(ctx, &storagepb.RegisterUploadRequest{
		TenantId: t.tenant, Filename: "native-tour-" + t.suffix + ".txt", ContentType: "text/plain",
		FileType: "DOCUMENT", ReferenceId: uuid4(), ReferenceType: "native_tour", SizeBytes: 5, ExpiresInMinutes: 10,
	})
	if err != nil {
		return "", err
	}
	fileID := reg.GetFileId()
	// Without an object store the file row is still registered, but no upload URL
	// can be minted: the response carries UPLOAD_URL_UNAVAILABLE. Remove the row
	// either way, then report.
	regErr := apiErr("RegisterUpload", reg.GetError())
	var got *storagepb.GetFileResponse
	if regErr == nil && fileID != "" {
		got, err = c.GetFile(ctx, &storagepb.GetFileRequest{TenantId: t.tenant, FileId: fileID})
		if err == nil {
			err = apiErr("GetFile", got.GetError())
		}
	}
	var delErr error
	if fileID != "" {
		del, e := c.DeleteFile(ctx, &storagepb.DeleteFileRequest{TenantId: t.tenant, FileId: fileID, Reason: "native tour cleanup"})
		delErr = e
		if delErr == nil {
			delErr = apiErr("DeleteFile", del.GetError())
		}
	}
	for _, e := range []error{regErr, err, delErr} {
		if e != nil {
			return "", e
		}
	}
	return fmt.Sprintf("RegisterUpload -> presigned upload URL issued; GetFile -> %s; DeleteFile ok", got.GetFile().GetStatus()), nil
}

func (t *tour) asset() (string, error) {
	c := assetpb.NewAssetServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	def, err := c.CreatePipelineDefinition(ctx, &assetpb.CreatePipelineDefinitionRequest{
		TenantId: t.tenant, Name: "native-tour-" + t.suffix, Description: "native-services tour pipeline",
		MediaType: "application/json", Steps: `[{"name":"extract","type":"EXTRACT"}]`, Version: 1,
	})
	if err == nil {
		err = apiErr("CreatePipelineDefinition", def.GetError())
	}
	if err != nil {
		return "", err
	}
	got, err := c.GetPipelineDefinition(ctx, &assetpb.GetPipelineDefinitionRequest{TenantId: t.tenant, DefinitionId: def.GetDefinitionId()})
	if err == nil {
		err = apiErr("GetPipelineDefinition", got.GetError())
	}
	if err != nil {
		return "", err
	}
	assets, err := c.ListAssets(ctx, &assetpb.ListAssetsRequest{TenantId: t.tenant, Page: 1, PageSize: 20})
	if err == nil {
		err = apiErr("ListAssets", assets.GetError())
	}
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("CreatePipelineDefinition + GetPipelineDefinition -> version %d; ListAssets -> %d asset(s)",
		got.GetDefinition().GetVersion(), len(assets.GetAssets())), nil
}

func (t *tour) search() (string, error) {
	c := searchpb.NewSearchServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	list, err := c.ListIndexes(ctx, &searchpb.ListIndexesRequest{TenantId: t.tenant, PageSize: 50})
	if err == nil {
		err = apiErr("ListIndexes", list.GetError())
	}
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ListIndexes -> %d index(es)", len(list.GetIndexes())), nil
}

func (t *tour) embedding() (string, error) {
	c := embeddingpb.NewEmbeddingServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	list, err := c.ListModels(ctx, &embeddingpb.ListModelsRequest{TenantId: t.tenant, PageSize: 50})
	if err == nil {
		err = apiErr("ListModels", list.GetError())
	}
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ListModels -> %d model(s)", len(list.GetModels())), nil
}

func (t *tour) messaging() (string, error) {
	c := messagingpb.NewMessagingServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	list, err := c.ListMessagingDevices(ctx, &messagingpb.ListMessagingDevicesRequest{
		TenantId: t.tenant, SubjectId: t.actor, PageSize: 20,
	})
	if err == nil {
		err = apiErr("ListMessagingDevices", list.GetError())
	}
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ListMessagingDevices(admin) -> %d device(s)", len(list.GetDevices())), nil
}

// The five WebRTC services share one room: RoomService creates it, PeerService
// joins it, TrackService publishes into it, TurnService issues TURN credentials
// for the peer, and SignalingService pings over the signalling stream and then
// closes the room.

func (t *tour) room() (string, error) {
	c := webrtcpb.NewRoomServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	created, err := c.CreateRoom(ctx, &webrtcpb.CreateRoomRequest{
		TenantId: t.tenant, Name: "native-tour-" + t.suffix, MaxParticipants: 8, Config: "{}", CreatedBy: t.actor,
	})
	if err == nil {
		err = apiErr("CreateRoom", created.GetError())
	}
	if err != nil {
		return "", err
	}
	got, err := c.GetRoom(ctx, &webrtcpb.GetRoomRequest{TenantId: t.tenant, RoomId: created.GetRoomId()})
	if err == nil {
		err = apiErr("GetRoom", got.GetError())
	}
	if err != nil {
		_, _ = c.CloseRoom(ctx, &webrtcpb.CloseRoomRequest{TenantId: t.tenant, RoomId: created.GetRoomId()})
		return "", err
	}
	t.roomID = created.GetRoomId()
	return fmt.Sprintf("CreateRoom + GetRoom -> %s", got.GetRoom().GetState()), nil
}

func (t *tour) peer() (string, error) {
	if err := t.needs("RoomService"); err != nil {
		return "", err
	}
	c := webrtcpb.NewPeerServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	joined, err := c.JoinRoom(ctx, &webrtcpb.JoinRoomRequest{
		TenantId: t.tenant, RoomId: t.roomID, DisplayName: "tour-peer", Metadata: "{}", UserAgent: "native-tour",
	})
	if err == nil {
		err = apiErr("JoinRoom", joined.GetError())
	}
	if err != nil {
		return "", err
	}
	t.peerID = joined.GetPeer().GetPeerId()
	list, err := c.ListPeers(ctx, &webrtcpb.ListPeersRequest{TenantId: t.tenant, RoomId: t.roomID, PageSize: 20})
	if err == nil {
		err = apiErr("ListPeers", list.GetError())
	}
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("JoinRoom + ListPeers -> %d peer(s) in the room", len(list.GetPeers())), nil
}

func (t *tour) track() (string, error) {
	if err := t.needs("PeerService"); err != nil {
		return "", err
	}
	c := webrtcpb.NewTrackServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	pub, err := c.PublishTrack(ctx, &webrtcpb.PublishTrackRequest{
		TenantId: t.tenant, RoomId: t.roomID, PeerId: t.peerID, Kind: "audio", Label: "mic", Settings: "{}", Metadata: "{}",
	})
	if err == nil {
		err = apiErr("PublishTrack", pub.GetError())
	}
	if err != nil {
		return "", err
	}
	list, err := c.ListTracks(ctx, &webrtcpb.ListTracksRequest{TenantId: t.tenant, RoomId: t.roomID, PageSize: 20})
	if err == nil {
		err = apiErr("ListTracks", list.GetError())
	}
	unpub, unpubErr := c.UnpublishTrack(ctx, &webrtcpb.UnpublishTrackRequest{TenantId: t.tenant, TrackId: pub.GetTrackId()})
	if unpubErr == nil {
		unpubErr = apiErr("UnpublishTrack", unpub.GetError())
	}
	if err != nil {
		return "", err
	}
	if unpubErr != nil {
		return "", unpubErr
	}
	return fmt.Sprintf("PublishTrack(audio) + ListTracks -> %d track(s); UnpublishTrack ok", len(list.GetTracks())), nil
}

func (t *tour) turn() (string, error) {
	if err := t.needs("PeerService"); err != nil {
		return "", err
	}
	c := webrtcpb.NewTurnServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()
	creds, err := c.IssueCredentials(ctx, &webrtcpb.IssueCredentialsRequest{
		TenantId: t.tenant, RoomId: t.roomID, PeerId: t.peerID, TtlSeconds: 3600,
	})
	if err == nil {
		err = apiErr("IssueCredentials", creds.GetError())
	}
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("IssueCredentials -> %d ICE server(s), ttl %ds", len(creds.GetIceServers()), creds.GetTtlSeconds()), nil
}

func (t *tour) signaling() (string, error) {
	if err := t.needs("RoomService"); err != nil {
		return "", err
	}
	peers := webrtcpb.NewPeerServiceClient(t.conn)
	rooms := webrtcpb.NewRoomServiceClient(t.conn)
	c := webrtcpb.NewSignalingServiceClient(t.conn)
	ctx, cancel := t.ctx()
	defer cancel()

	// Closing a signalling stream disconnects its peer, so ping with a second,
	// disposable peer.
	joined, err := peers.JoinRoom(ctx, &webrtcpb.JoinRoomRequest{
		TenantId: t.tenant, RoomId: t.roomID, DisplayName: "tour-signal", Metadata: "{}", UserAgent: "native-tour",
	})
	if err == nil {
		err = apiErr("JoinRoom", joined.GetError())
	}
	pong := false
	if err == nil {
		streamCtx, stop := context.WithCancel(ctx)
		var stream webrtcpb.SignalingService_SignalClient
		stream, err = c.Signal(streamCtx)
		if err == nil {
			err = stream.Send(&webrtcpb.SignalRequest{
				TenantId: t.tenant, RoomId: t.roomID, PeerId: joined.GetPeer().GetPeerId(),
				Payload: &webrtcpb.SignalRequest_Ping{Ping: true},
			})
		}
		for err == nil && !pong {
			var frame *webrtcpb.SignalResponse
			frame, err = stream.Recv()
			if err == nil {
				pong = frame.GetPong()
			}
		}
		if stream != nil {
			_ = stream.CloseSend()
		}
		stop()
	}

	// Leave + close the shared room (this step runs last).
	var cleanupErr error
	if t.peerID != "" {
		left, e := peers.LeaveRoom(ctx, &webrtcpb.LeaveRoomRequest{TenantId: t.tenant, RoomId: t.roomID, PeerId: t.peerID})
		if e == nil {
			e = apiErr("LeaveRoom", left.GetError())
		}
		cleanupErr = e
	}
	closed, e := rooms.CloseRoom(ctx, &webrtcpb.CloseRoomRequest{TenantId: t.tenant, RoomId: t.roomID})
	if e == nil {
		e = apiErr("CloseRoom", closed.GetError())
	}
	if cleanupErr == nil {
		cleanupErr = e
	}
	if err != nil {
		return "", err
	}
	if cleanupErr != nil {
		return "", cleanupErr
	}
	return "Signal(ping) -> pong; LeaveRoom + CloseRoom ok", nil
}
