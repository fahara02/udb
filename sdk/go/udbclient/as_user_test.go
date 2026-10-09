package udbclient

import (
	"context"
	"net"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	authnentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/entity/v1"
	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	eventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/events/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	asUserFixtureTenant  = "11111111-1111-4111-8111-111111111111"
	asUserFixtureProject = "22222222-2222-4222-8222-222222222222"
	asUserFixtureService = "33333333-3333-4333-8333-333333333333"
	asUserFixtureUser    = "44444444-4444-4444-8444-444444444444"
	asUserFixtureBearer  = "Bearer fixture-user-bearer"
)

// This fixture validates the actual incoming wire metadata independently of
// SDK context helpers. Production Connect exchanges its key through generated
// TCP auth; data/native/generated unary and stream calls use the owned channel.
// Actual bearer verification, tenant/PDP and revocation are covered separately
// by TestLiveAsUserPreservesVerifiedAuthority against the real broker.
type asUserWireFixture struct {
	authnv1.UnimplementedAuthnServiceServer
	servicesv1.UnimplementedDataBrokerServer
	mu      sync.Mutex
	service int
	user    int
}

func (f *asUserWireFixture) Authenticate(_ context.Context, request *authnv1.AuthnRequest) (*authnv1.AuthnResponse, error) {
	if request.GetApiKey() != "fixture-service-key" {
		return nil, status.Error(codes.Unauthenticated, "fixture API key required")
	}
	return &authnv1.AuthnResponse{
		AccessToken: "fixture-service-bearer", ExpiresAtUnix: time.Now().Add(10 * time.Minute).Unix(),
		Principal: &authnv1.Principal{
			TenantId: asUserFixtureTenant, ProjectId: asUserFixtureProject,
			UserId: asUserFixtureService, ServiceIdentity: "spiffe://fixture/service",
			Scopes:      []string{"data:read", "service:scope"},
			AccountKind: authnentpb.AccountKind_ACCOUNT_KIND_SERVICE_ACCOUNT,
		},
	}, nil
}

func (f *asUserWireFixture) admit(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	credential := md.Get("authorization")
	if len(credential) != 1 {
		return status.Error(codes.PermissionDenied, "authorization must have one value")
	}
	user := credential[0] == asUserFixtureBearer
	if !user && credential[0] != "Bearer fixture-service-bearer" {
		return status.Error(codes.Unauthenticated, "fixture bearer not valid")
	}
	for key, want := range map[string]string{
		"x-tenant-id": asUserFixtureTenant, "x-udb-project-id": asUserFixtureProject,
	} {
		values := md.Get(key)
		if len(values) != 1 || values[0] != want {
			return status.Error(codes.PermissionDenied, "connected tenant/project binding changed")
		}
	}
	for _, key := range []string{"x-api-key", "x-udb-api-key"} {
		for _, value := range md.Get(key) {
			if value != "" {
				return status.Error(codes.PermissionDenied, "delegated call carried a raw key")
			}
		}
	}
	if user {
		for _, key := range []string{"x-user-id", "x-service-identity", "x-scopes"} {
			values := md.Get(key)
			if len(values) > 1 || (len(values) == 1 && values[0] != "") {
				return status.Error(codes.PermissionDenied, "delegated call retained a service principal hint")
			}
		}
		for key, want := range map[string]string{
			"x-purpose": "delegated-purpose", "x-correlation-id": "delegated-correlation",
			"x-udb-client-catalog-version": "delegated-catalog",
		} {
			values := md.Get(key)
			if len(values) != 1 || values[0] != want {
				return status.Error(codes.PermissionDenied, "delegation changed request audit metadata")
			}
		}
	} else {
		for key, want := range map[string]string{
			"x-user-id": asUserFixtureService, "x-service-identity": "spiffe://fixture/service",
			"x-scopes": "data:read,service:scope", "x-purpose": "service-purpose",
		} {
			values := md.Get(key)
			if len(values) != 1 || values[0] != want {
				return status.Error(codes.PermissionDenied, "ordinary service metadata changed")
			}
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if user {
		f.user++
	} else {
		f.service++
	}
	return nil
}

func (f *asUserWireFixture) Select(ctx context.Context, _ *entityv1.SelectRequest) (*entityv1.RecordSet, error) {
	if err := f.admit(ctx); err != nil {
		return nil, err
	}
	return &entityv1.RecordSet{RecordsJson: [][]byte{[]byte(`{"record_id":"owned"}`)}}, nil
}

func (f *asUserWireFixture) GetCapabilities(ctx context.Context, _ *entityv1.CapabilitiesRequest) (*entityv1.CapabilitiesResponse, error) {
	if err := f.admit(ctx); err != nil {
		return nil, err
	}
	return &entityv1.CapabilitiesResponse{EnabledBackends: []string{"postgres"}}, nil
}

func (f *asUserWireFixture) ValidateToken(ctx context.Context, request *authnv1.ValidateTokenRequest) (*authnv1.ValidateTokenResponse, error) {
	if request.GetTokenType() != authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS {
		return nil, status.Error(codes.InvalidArgument, "token_type must identify a JWT access token")
	}
	if err := f.admit(ctx); err != nil {
		return nil, err
	}
	if request.GetToken() != "fixture-user-bearer" {
		return nil, status.Error(codes.Unauthenticated, "fixture user token required")
	}
	return &authnv1.ValidateTokenResponse{Valid: true, UserId: asUserFixtureUser}, nil
}

func (f *asUserWireFixture) PublishCDC(_ *entityv1.CDCSubscriptionRequest, stream grpc.ServerStreamingServer[eventsv1.CDCEnvelope]) error {
	if err := f.admit(stream.Context()); err != nil {
		return err
	}
	if err := stream.SendHeader(metadata.Pairs("x-udb-version", SDKVersion)); err != nil {
		return err
	}
	return stream.Send(&eventsv1.CDCEnvelope{EventId: "owned-event", Topic: "owned-topic"})
}

func TestAsUserUsesOneBearerWithoutServiceHintsOnOwnedConnections(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("could not open the generated TCP fixture")
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := grpc.SendHeader(ctx, metadata.Pairs("x-udb-version", SDKVersion)); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}))
	fixture := &asUserWireFixture{}
	servicesv1.RegisterDataBrokerServer(server, fixture)
	authnv1.RegisterAuthnServiceServer(server, fixture)
	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		select {
		case <-stopped:
		case <-time.After(3 * time.Second):
			t.Error("generated TCP fixture did not stop")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	u, err := Connect(ctx, Config{
		Target: listener.Addr().String(), Credentials: Credentials{APIKey: "fixture-service-key"},
		Purpose: "service-purpose", Retry: RetryConfig{MaxAttempts: 1}, Deadline: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("production service Connect failed: code=%s", status.Code(err))
	}
	t.Cleanup(func() { _ = u.Close() })
	beforeMeta := u.Meta
	beforeMeta.Scopes = slices.Clone(u.Meta.Scopes)
	beforeOptions, beforeData, beforeAuth := u.Generated.options(), u.Data, u.Auth
	delegatedAudit := Metadata{Purpose: "delegated-purpose", CorrelationID: "delegated-correlation", ClientCatalogVersion: "delegated-catalog"}
	for _, inherited := range []bool{false, true} {
		base := WithMetadata(ctx, delegatedAudit)
		if inherited {
			base = u.Generated.outgoingContext(u.Data.Context(base))
			base = metadata.AppendToOutgoingContext(base, "x-api-key", "old-key", "x-udb-api-key", "old-alias")
		}
		original, _ := metadata.FromOutgoingContext(base)
		original = original.Copy()
		asUser := u.AsUser(base, "  Bearer fixture-user-bearer  ")
		rows, err := u.Data.Select(asUser, &entityv1.SelectRequest{})
		if err != nil || len(rows.GetRecordsJson()) != 1 {
			t.Fatalf("delegated Data.Context call failed: code=%s", status.Code(err))
		}
		verified, err := u.Auth.ValidateSession(asUser, "fixture-user-bearer")
		if err != nil || verified.UserID != asUserFixtureUser {
			t.Fatalf("delegated Auth.Context call failed: code=%s", status.Code(err))
		}
		var caps entityv1.CapabilitiesResponse
		if err := u.Generated.InvokeUnary(asUser, "/udb.services.v1.DataBroker/GetCapabilities", &entityv1.CapabilitiesRequest{}, &caps); err != nil {
			t.Fatalf("delegated generated unary failed: code=%s", status.Code(err))
		}
		streamCtx, streamCancel := context.WithTimeout(asUser, 3*time.Second)
		stream, err := u.Data.Broker.PublishCDC(streamCtx, &entityv1.CDCSubscriptionRequest{TopicPattern: "owned-topic"})
		if err == nil {
			_, err = stream.Recv()
		}
		streamCancel()
		if err != nil {
			t.Fatalf("delegated generated stream failed: code=%s", status.Code(err))
		}
		unchanged, _ := metadata.FromOutgoingContext(base)
		if !reflect.DeepEqual(original, unchanged.Copy()) {
			t.Fatal("AsUser changed the caller's existing metadata")
		}
	}
	// Audit headers from a manually built context survive nested delegation too.
	inherited := metadata.NewOutgoingContext(ctx, metadata.Pairs("x-purpose", "delegated-purpose", "x-correlation-id", "delegated-correlation", "x-udb-client-catalog-version", "delegated-catalog", "x-extra-audit", "preserved"))
	nested := u.AsUser(u.AsUser(inherited, "other-user-token"), "fixture-user-bearer")
	if _, err := u.Data.Select(nested, &entityv1.SelectRequest{}); err != nil {
		t.Fatalf("nested delegation lost inherited audit: code=%s", status.Code(err))
	}
	md, _ := metadata.FromOutgoingContext(nested)
	if values := md.Get("x-extra-audit"); len(values) != 1 || values[0] != "preserved" {
		t.Fatal("AsUser discarded unrelated outgoing metadata")
	}
	// Ordinary calls race with explicit delegation without mutating shared
	// options, facade handles or the connected principal's identity/scopes.
	var wg sync.WaitGroup
	results := make(chan error, 16)
	for index := 0; index < 16; index++ {
		wg.Add(1)
		go func(delegated bool) {
			defer wg.Done()
			callCtx, callCancel := context.WithTimeout(ctx, 3*time.Second)
			defer callCancel()
			if delegated {
				callCtx = u.AsUser(WithMetadata(callCtx, delegatedAudit), "fixture-user-bearer")
			}
			_, err := u.Data.Broker.GetCapabilities(callCtx, &entityv1.CapabilitiesRequest{})
			results <- err
		}(index%2 == 0)
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Errorf("concurrent service/delegated call failed: code=%s", status.Code(err))
		}
	}
	// Legacy raw-key mode has a nonempty interceptor default even after AsUser
	// removes an inherited header. It must not restore that service credential.
	legacy, err := Connect(ctx, Config{
		Target: listener.Addr().String(), TenantID: asUserFixtureTenant, ProjectID: asUserFixtureProject,
		UserID: asUserFixtureService, ServiceIdentity: "spiffe://fixture/service", Scopes: []string{"data:read"},
		Credentials: Credentials{APIKey: "fixture-legacy-key", RawAPIKey: true},
		Purpose:     "service-purpose", Retry: RetryConfig{MaxAttempts: 1}, Deadline: 3 * time.Second,
	})
	if err == nil || legacy != nil {
		if legacy != nil {
			_ = legacy.Close()
		}
		t.Fatal("Connect must refuse legacy raw API-key mode")
	}
	// The low-level explicit static option remains caller-owned. Delegation
	// must suppress it without changing the default or adding another credential.
	legacyGenerated := NewGenerated(u.brokerConn, Options{APIKey: "fixture-legacy-key", Meta: u.Meta, Retry: RetryConfig{MaxAttempts: 1}})
	if err := legacyGenerated.InvokeUnary(u.AsUser(WithMetadata(ctx, delegatedAudit), "fixture-user-bearer"), "/udb.services.v1.DataBroker/GetCapabilities", &entityv1.CapabilitiesRequest{}, &entityv1.CapabilitiesResponse{}); err != nil {
		t.Fatalf("legacy raw-key default overrode delegation: code=%s", status.Code(err))
	}
	if legacyGenerated.options().APIKey != "fixture-legacy-key" {
		t.Fatal("request delegation changed the ordinary legacy credential default")
	}
	for _, invalid := range []string{"", "not-a-token"} {
		_, err := u.Data.Broker.GetCapabilities(u.AsUser(ctx, invalid), &entityv1.CapabilitiesRequest{})
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("invalid delegation must not fall back to service authority: code=%s", status.Code(err))
		}
	}
	if !reflect.DeepEqual(beforeMeta, u.Meta) || !reflect.DeepEqual(beforeOptions, u.Generated.options()) || u.Data != beforeData || u.Auth != beforeAuth {
		t.Fatal("AsUser mutated the connected service identity or facades")
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.service != 8 || fixture.user != 18 {
		t.Fatalf("served wire coverage incomplete: service=%d delegated=%d", fixture.service, fixture.user)
	}
}
