package udbclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"slices"
	"sync"
	"time"

	analyticsv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/analytics/services/v1"
	apikeyv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/services/v1"
	assetv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/asset/services/v1"
	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	authzv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
	notificationv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/notification/services/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/services/v1"
	tenantv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/tenant/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var newGrpcClient = grpc.NewClient

// ── Phase 7: UdbProject facade ───────────────────────────────────────────────
//
// Udb is a single entry point that dials the broker once, applies the generated
// robustness layer (retry/backoff/metadata/typed errors) to every call, and
// hangs the per-domain convenience clients off one struct. It owns the
// connections it dials and closes them in Close(). Raw generated clients remain
// reachable on each sub-client (e.g. u.ApiKey.Raw, u.Notification.Raw) so any
// RPC the convenience wrappers don't cover is still one field away.
//
// Service stubs that exist in ./gen are wired: Data (DataBroker), Auth (Authn),
// Authz, ApiKey, Tenant, Notification, Analytics, and the media plane — Storage
// (StorageService), Asset (AssetService), and WebRTC (Room/Peer/Track/Turn +
// Signal). The media services are served on the native control-plane listener,
// so they reuse the auth connection rather than opening their own.

// Config configures NewUdb. Only Target is required; everything else has a sane
// default. AuthTarget defaults to Target when empty (auth lives on the same
// broker endpoint in the default deployment). The media services (Storage/
// Asset/WebRTC) share the auth/control-plane listener, so they follow
// AuthTarget.
type Config struct {
	Target     string // broker gRPC target, e.g. "localhost:50051" (required)
	AuthTarget string // authn/authz/media control-plane target; defaults to Target
	// WebRTCTarget, when set, is the signalling/peer endpoint the WebRTC facade
	// dials on a dedicated connection; defaults to AuthTarget (the control-plane
	// listener that also serves Room/Peer/Track/Turn).
	WebRTCTarget string

	TenantID  string
	ProjectID string
	Purpose   string
	Scopes    []string

	// Credentials, when set, are sent as auth headers on every call:
	// Bearer (an access token) and/or APIKey.
	Credentials Credentials

	// TLS, when non-nil, dials with transport security. When nil the client
	// dials insecure (plaintext) — appropriate for localhost / a mesh sidecar.
	TLS *tls.Config

	// Retry overrides the default backoff policy. Zero value uses
	// DefaultRetryConfig().
	Retry RetryConfig

	// Deadline, when > 0, is applied as a per-call timeout when the caller's
	// context carries none.
	Deadline time.Duration

	// StrictServerVersion refuses responses from brokers outside this SDK's
	// major.minor release, including missing or malformed release headers.
	// By default the client warns. See Options for response-check semantics.
	StrictServerVersion bool
	OnVersionWarning    func(error)

	// UserID / ServiceIdentity / CorrelationID seed the caller Metadata.
	UserID          string
	ServiceIdentity string
	CorrelationID   string
}

// Credentials carries the per-call auth material the facade attaches as headers.
type Credentials struct {
	Bearer string // sent as "authorization: Bearer <token>"
	// APIKey is a service-account key. Without a Bearer, NewUdb exchanges it for
	// a service bearer at connect and keeps that bearer fresh in the background;
	// the raw key is never sent on a call.
	APIKey string
	// RawAPIKey restores the pre-0.5.31 behaviour of sending APIKey as an
	// `x-api-key` header on every call instead of exchanging it. Deprecated:
	// removed in 0.6.0 (the native control plane refuses raw keys).
	RawAPIKey bool
}

func (c Config) metadata() Metadata {
	return Metadata{
		TenantID:        c.TenantID,
		UserID:          c.UserID,
		Purpose:         c.Purpose,
		CorrelationID:   c.CorrelationID,
		Scopes:          c.Scopes,
		ServiceIdentity: c.ServiceIdentity,
		ProjectID:       c.ProjectID,
	}
}

func (c Config) options() Options {
	o := Options{
		Meta:                c.metadata(),
		CallTimeout:         c.Deadline,
		Retry:               c.Retry,
		StrictServerVersion: c.StrictServerVersion,
		OnVersionWarning:    c.OnVersionWarning,
	}
	if c.Credentials.RawAPIKey || c.Credentials.Bearer != "" {
		o.APIKey = c.Credentials.APIKey
	}
	if c.Credentials.Bearer != "" {
		o.Authorization = "Bearer " + c.Credentials.Bearer
	}
	return o
}

// Udb is the unified project facade returned by NewUdb.
type Udb struct {
	Meta Metadata

	// Generated is the robustness layer governing the broker connection.
	Generated *GeneratedClient

	Data         *Client                            // DataBroker (Select/Upsert/Delete + raw Broker)
	Auth         *AuthClient                        // Authn + Authz raw clients and helpers
	Authz        *AuthzFacade                       // Can/Require/BatchCan/Explain/NativeAccess (cached)
	ApiKey       *ApiKeyFacade                      // CreateApiKey/RevokeApiKey + raw
	Tenant       *TenantFacade                      // CreateTenant/onboarding + raw
	Notification *NotificationFacade                // SendNotification + raw
	Analytics    analyticsv1.AnalyticsServiceClient // raw analytics client

	Storage *StorageFacade // StorageService (upload/download/file CRUD) + raw
	Asset   *AssetFacade   // AssetService (pipeline + asset CRUD) + raw
	WebRTC  *WebRTCFacade  // WebRTC Room/Peer/Track/Turn sub-facades + Signal stream
	Events  *EventsFacade  // DataBroker PublishCDC/EnqueueOutboxEvent ready/publish-and-wait

	// adoptMu serializes explicit identity adoption and credential renewal.
	// Initial adoption precedes Connect returning; explicit later adoption must
	// be coordinated with callers. Renewal never writes public facade fields.
	adoptMu sync.Mutex
	// Connections retained so adoptMetadata can rebuild facades over them.
	brokerConn grpc.ClientConnInterface
	authConn   grpc.ClientConnInterface
	webrtcConn grpc.ClientConnInterface

	// owned connections to close.
	conns []*grpc.ClientConn

	// apiKey is the exchanged service-account key state (nil when the
	// connection does not exchange one); see apikey_session.go.
	apiKey *apiKeySession
	// fence is the latest Table write's receipt; see session_fence.go.
	fence writeFence
}

// Connect is the canonical naming-contract constructor: it dials the broker and
// wires the full project facade. It is a thin alias of NewUdb (no behavior
// difference) so the simple-client surface reads `udbclient.Connect(ctx, opts)`
// across languages. NewUdb stays as the original name.
func Connect(ctx context.Context, cfg Config) (*Udb, error) {
	return NewUdb(ctx, cfg)
}

// NewUdb dials the broker (and the auth endpoint, if different), builds the
// generated robustness layer, and wires every available per-domain client.
func NewUdb(ctx context.Context, cfg Config) (*Udb, error) {
	if cfg.Target == "" {
		return nil, fmt.Errorf("udb: Config.Target is required")
	}
	if cfg.AuthTarget == "" {
		cfg.AuthTarget = cfg.Target
	}

	meta := cfg.metadata()
	opt := cfg.options()

	transport := transportCreds(cfg.TLS)

	// Generated layer is built from the broker options; its DialOptions apply
	// retry/metadata/error-mapping to the typed wrappers on the same conn.
	gen := NewGenerated(nil, opt)

	dial := func(target string) (*grpc.ClientConn, error) {
		dialOpts := append([]grpc.DialOption{grpc.WithTransportCredentials(transport)}, gen.DialOptions()...)
		return newGrpcClient(target, dialOpts...)
	}

	brokerConn, err := dial(cfg.Target)
	if err != nil {
		return nil, fmt.Errorf("udb: dial broker %q: %w", cfg.Target, err)
	}

	u := &Udb{Meta: meta, conns: []*grpc.ClientConn{brokerConn}, brokerConn: brokerConn}

	// V23-2: expose the SAME generated client whose interceptors were installed on
	// every dialed connection (gen.DialOptions() above), rebound onto the live
	// broker connection for its escape-hatch Invoke/NewStream. Because the
	// connection interceptors read this exact object's options live, adoptMetadata
	// (SetMeta) and setBearerLocked (SetAuthorization) now update the metadata the
	// connections actually emit — so after tenant adoption or a bearer refresh no
	// connection carries the stale pre-login identity/bearer. (Previously this was
	// a SECOND NewGenerated(brokerConn, opt); the interceptors kept reading the
	// original gen, which nothing updated, duplicating a stale x-tenant-id.)
	gen.rebindConn(brokerConn)
	u.Generated = gen

	authConn := brokerConn
	if cfg.AuthTarget != cfg.Target {
		authConn, err = dial(cfg.AuthTarget)
		if err != nil {
			_ = brokerConn.Close()
			return nil, fmt.Errorf("udb: dial auth %q: %w", cfg.AuthTarget, err)
		}
		u.conns = append(u.conns, authConn)
	}

	u.authConn = authConn

	// Data plane.
	u.Data = New(brokerConn, meta)
	u.Events = newEventsFacade(brokerConn, meta)

	// Auth plane + cached authz facade.
	u.Auth = NewAuthClient(authConn, meta)
	cache := NewAuthzCache(u.Auth)
	u.Auth.AttachAuthzCache(cache)
	u.Authz = &AuthzFacade{client: u.Auth, cache: cache}

	// Control-plane convenience facades over their raw generated clients.
	u.ApiKey = &ApiKeyFacade{Raw: apikeyv1.NewApiKeyServiceClient(authConn), meta: meta}
	u.Tenant = &TenantFacade{Raw: tenantv1.NewTenantServiceClient(authConn), meta: meta}
	u.Notification = &NotificationFacade{Raw: notificationv1.NewNotificationServiceClient(authConn), meta: meta}
	u.Analytics = analyticsv1.NewAnalyticsServiceClient(authConn)

	// Media plane. Storage/Asset are served on the native control-plane listener
	// (same target as auth/tenant), so they reuse authConn. WebRTC dials its own
	// connection when WebRTCTarget is set and differs from the auth target;
	// otherwise it reuses authConn (no new connection is opened).
	u.Storage = &StorageFacade{Raw: storagev1.NewStorageServiceClient(authConn), meta: meta}
	u.Asset = &AssetFacade{Raw: assetv1.NewAssetServiceClient(authConn), meta: meta}

	webrtcConn := authConn
	if cfg.WebRTCTarget != "" && cfg.WebRTCTarget != cfg.AuthTarget {
		webrtcConn, err = dial(cfg.WebRTCTarget)
		if err != nil {
			_ = u.Close()
			return nil, fmt.Errorf("udb: dial webrtc %q: %w", cfg.WebRTCTarget, err)
		}
		u.conns = append(u.conns, webrtcConn)
	}
	u.webrtcConn = webrtcConn
	u.WebRTC = newWebRTCFacade(webrtcConn, meta)

	if cfg.Credentials.APIKey != "" && cfg.Credentials.Bearer == "" && !cfg.Credentials.RawAPIKey {
		if err := u.startAPIKeyExchange(ctx, cfg.Credentials.APIKey, cfg.Deadline); err != nil {
			_ = u.Close()
			return nil, err
		}
	}

	return u, nil
}

// ── Mutable metadata adoption (chapter 08.5) ─────────────────────────────────

// adoptMetadata re-seeds Udb.Meta and rebuilds facades over existing connections
// when the verified metadata changes. adoptMu serializes installations; the
// generated interceptors receive identity and bearer as one atomic snapshot.
// Explicit adoption must be coordinated with facade readers. Credential renewal
// uses renewPrincipal instead and leaves public metadata/facades alone. Caller
// holds adoptMu.
func (u *Udb) adoptMetadata(meta Metadata, bearer string) {
	unchanged := u.Data != nil && u.Auth != nil && u.Meta.TenantID == meta.TenantID && u.Meta.ProjectID == meta.ProjectID &&
		u.Meta.UserID == meta.UserID && u.Meta.ServiceIdentity == meta.ServiceIdentity &&
		u.Meta.Purpose == meta.Purpose && u.Meta.CorrelationID == meta.CorrelationID &&
		u.Meta.ClientCatalogVersion == meta.ClientCatalogVersion && slices.Equal(u.Meta.Scopes, meta.Scopes)
	if unchanged {
		u.installGeneratedIdentity(meta, bearer)
		return
	}

	u.Meta = meta

	// Data + events plane.
	u.Data = New(u.brokerConn, meta)
	u.Events = newEventsFacade(u.brokerConn, meta)

	// Auth plane + cached authz facade (preserve any policy-bundle secret).
	prevSecret := u.Auth.policyBundleSecret
	u.Auth = NewAuthClient(u.authConn, meta)
	u.Auth.policyBundleSecret = prevSecret
	cache := NewAuthzCache(u.Auth)
	u.Auth.AttachAuthzCache(cache)
	u.Authz = &AuthzFacade{client: u.Auth, cache: cache}

	// Control-plane convenience facades.
	u.ApiKey = &ApiKeyFacade{Raw: apikeyv1.NewApiKeyServiceClient(u.authConn), meta: meta}
	u.Tenant = &TenantFacade{Raw: tenantv1.NewTenantServiceClient(u.authConn), meta: meta}
	u.Notification = &NotificationFacade{Raw: notificationv1.NewNotificationServiceClient(u.authConn), meta: meta}
	u.Analytics = analyticsv1.NewAnalyticsServiceClient(u.authConn)

	// Media plane.
	u.Storage = &StorageFacade{Raw: storagev1.NewStorageServiceClient(u.authConn), meta: meta}
	u.Asset = &AssetFacade{Raw: assetv1.NewAssetServiceClient(u.authConn), meta: meta}
	u.WebRTC = newWebRTCFacade(u.webrtcConn, meta)

	u.installGeneratedIdentity(meta, bearer)
}

func (u *Udb) installGeneratedIdentity(meta Metadata, bearer string) {
	if u.Generated != nil {
		opt := u.Generated.options()
		opt.Meta = meta
		if bearer != "" {
			opt.Authorization = "Bearer " + bearer
		}
		u.Generated.opt.Store(&opt)
	}
}

// adoptPrincipal derives identity exclusively from the verified principal,
// retaining only request/audit configuration from the connected client.
func (u *Udb) adoptPrincipal(principal *authnv1.Principal, bearer string) {
	u.adoptMu.Lock()
	defer u.adoptMu.Unlock()
	meta := u.Meta
	meta.TenantID = principal.GetTenantId()
	meta.ProjectID = principal.GetProjectId()
	meta.UserID = principal.GetUserId()
	meta.ServiceIdentity = principal.GetServiceIdentity()
	meta.Scopes = append([]string(nil), principal.GetScopes()...)
	u.adoptMetadata(meta, bearer)
}

// renewPrincipal installs only a new bearer for the connected identity. Both
// the original identity and the current facade identity must still match the
// verified response. Preserve existing scope ordering and all facade handles;
// changing identity requires an explicit adoption coordinated with callers.
func (u *Udb) renewPrincipal(principal *authnv1.Principal, bearer string, pinned Metadata) error {
	u.adoptMu.Lock()
	defer u.adoptMu.Unlock()
	if err := u.checkRenewalPrincipalLocked(principal, pinned); err != nil {
		return err
	}
	if u.Generated != nil {
		u.Generated.SetAuthorization("Bearer " + bearer)
	}
	return nil
}

// Enterprise re-login validates before storing the token; its session publishes
// the current stored bearer separately under the bearer installation lock.
func (u *Udb) validateRenewalPrincipal(principal *authnv1.Principal, pinned Metadata) error {
	u.adoptMu.Lock()
	defer u.adoptMu.Unlock()
	return u.checkRenewalPrincipalLocked(principal, pinned)
}

func (u *Udb) checkRenewalPrincipalLocked(principal *authnv1.Principal, pinned Metadata) error {
	if err := checkPrincipalIdentity(principal, pinned); err != nil {
		return err
	}
	if err := checkPrincipalIdentity(principal, u.Meta); err != nil {
		return err
	}
	return nil
}

// Renewal must not switch the connected principal or its scope set. Scope
// ordering and duplicates carry no additional permissions.
func checkPrincipalIdentity(principal *authnv1.Principal, expected Metadata) error {
	if principal == nil {
		return fmt.Errorf("udb: authentication returned no verified principal")
	}
	for _, field := range []struct{ name, actual, expected string }{
		{"tenant", principal.GetTenantId(), expected.TenantID},
		{"project", principal.GetProjectId(), expected.ProjectID},
		{"user", principal.GetUserId(), expected.UserID},
		{"service identity", principal.GetServiceIdentity(), expected.ServiceIdentity},
	} {
		if field.actual != field.expected {
			return fmt.Errorf("udb: renewed principal changed %s", field.name)
		}
	}
	actualScopes, expectedScopes := slices.Clone(principal.GetScopes()), slices.Clone(expected.Scopes)
	slices.Sort(actualScopes)
	slices.Sort(expectedScopes)
	if !slices.Equal(slices.Compact(actualScopes), slices.Compact(expectedScopes)) {
		return fmt.Errorf("udb: renewed principal changed scopes")
	}
	return nil
}

// AdoptedLogin is the result of LoginAndAdoptTenant: the bearer token set as the
// authorization credential and the verified principal whose canonical tenant/
// project were adopted.
type AdoptedLogin struct {
	Token     Token
	Principal *authnv1.Principal
}

// LoginAndAdoptTenant performs the CANONICAL 2-RPC login-and-adopt sequence:
//
//  1. Login (native AuthnService.Login) to obtain the bearer access token.
//  2. AuthenticateBearer to resolve + VERIFY the canonical principal.
//
// It then derives the FULL identity {tenant_id, project_id, user_id,
// service_identity, scopes} FROM THE VERIFIED PRINCIPAL (never a body hint),
// adopts that metadata across the facades (adoptMetadata), and installs the
// generated identity/bearer snapshot atomically. Both RPCs ALWAYS run —
// there is no "skip authenticate if a principal is already present" branch. No
// body tenant copying afterward (the broker derives identity from the verified
// claim).
func (u *Udb) LoginAndAdoptTenant(ctx context.Context, req *authnv1.LoginRequest) (*AdoptedLogin, error) {
	adopted, err := u.loginAndVerify(ctx, req)
	if err != nil {
		return nil, err
	}
	u.adoptPrincipal(adopted.Principal, adopted.Token.AccessToken)
	return adopted, nil
}

// loginAndVerify performs both authentication RPCs without mutating the live
// client. A session can validate a renewal's identity before installing it.
func (u *Udb) loginAndVerify(ctx context.Context, req *authnv1.LoginRequest) (*AdoptedLogin, error) {
	// RPC 1: native login.
	loginResp, err := u.Auth.Authn.Login(u.Auth.Context(ctx), req)
	if err != nil {
		return nil, err
	}
	receivedAt := time.Now()
	token := loginResp.GetAccessToken()
	if token == "" {
		return nil, fmt.Errorf("udb: Login returned no access token (MFA required: %v)", loginResp.GetMfaRequired())
	}

	// RPC 2: verify the bearer and resolve the canonical principal.
	authResp, err := u.Auth.AuthenticateBearer(ctx, token)
	if err != nil {
		return nil, err
	}
	principal := authResp.GetPrincipal()
	if principal == nil {
		return nil, fmt.Errorf("udb: AuthenticateBearer returned no principal")
	}

	// Verification may take time; do not extend the login bearer lifetime by
	// counting it again from the end of that second RPC.
	tok := tokenFromLogin(loginResp, receivedAt)
	return &AdoptedLogin{Token: tok, Principal: principal}, nil
}

// Close closes every connection NewUdb owns. Safe to call once.
func (u *Udb) Close() error {
	u.stopAPIKeyRefresh()
	var firstErr error
	for _, c := range u.conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	u.conns = nil
	return firstErr
}

func transportCreds(cfg *tls.Config) credentials.TransportCredentials {
	if cfg == nil {
		return insecure.NewCredentials()
	}
	return credentials.NewTLS(cfg)
}

// ── Authz facade ─────────────────────────────────────────────────────────────

// AuthzFacade exposes the authz ergonomics surface (Can/Require/BatchCan/Explain
// /NativeAccess) over a cached AuthClient.
type AuthzFacade struct {
	client *AuthClient
	cache  *AuthzCache
}

// Can answers allow/deny (cached) and returns the Decision for inspection.
func (f *AuthzFacade) Can(ctx context.Context, resource *authzv1.ResourceRef, action, purpose string) (bool, *authzv1.Decision, error) {
	return f.cache.Can(ctx, resource, action, purpose)
}

// Require returns nil on allow or an *AuthzDeniedError on deny (cached).
func (f *AuthzFacade) Require(ctx context.Context, resource *authzv1.ResourceRef, action, purpose string) error {
	return f.cache.Require(ctx, resource, action, purpose)
}

// Explain returns the full Decision without erroring on a clean deny (cached).
func (f *AuthzFacade) Explain(ctx context.Context, resource *authzv1.ResourceRef, action, purpose string) (*authzv1.Decision, error) {
	return f.cache.Explain(ctx, resource, action, purpose)
}

// BatchCan evaluates many (object, action) checks in one RPC.
func (f *AuthzFacade) BatchCan(ctx context.Context, checks []BatchCheck) ([]BatchResult, map[string]bool, error) {
	return f.client.BatchCan(ctx, checks)
}

// NativeAccess returns a short-lived native DB grant when allowed.
func (f *AuthzFacade) NativeAccess(ctx context.Context, resource *authzv1.ResourceRef, action, purpose string) (*authzv1.NativeAccessGrant, error) {
	return f.client.NativeAccess(ctx, resource, action, purpose)
}

// Invalidate drops cached decisions (e.g. after a known policy change).
func (f *AuthzFacade) Invalidate() { f.cache.Invalidate() }
