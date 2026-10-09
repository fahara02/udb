package udbclient

import (
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	authnentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/entity/v1"
	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Actual TCP servers independently count business admission and inspect wire
// metadata. Credential failures must stop before Invoke/NewStream reaches them.
type lifecycleWireFixture struct {
	authnv1.UnimplementedAuthnServiceServer
	servicesv1.UnimplementedDataBrokerServer
	mu                                   sync.Mutex
	principal                            *authnv1.Principal
	business, refresh, login, exchange   int
	failRefresh, failLogin, failExchange bool
	entered                              chan struct{}
	release                              chan struct{}
	refreshEntered, refreshRelease       chan struct{}
	wire                                 []metadata.MD
}

func (f *lifecycleWireFixture) Login(context.Context, *authnv1.LoginRequest) (*authnv1.LoginResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.login++
	if f.failLogin {
		return nil, status.Error(codes.Unauthenticated, "owned login refused")
	}
	return &authnv1.LoginResponse{AccessToken: fmt.Sprintf("owned-login-%d", f.login), RefreshToken: fmt.Sprintf("owned-refresh-%d", f.login), SessionId: "owned-session", AccessTokenExpiresIn: 3600}, nil
}

func (f *lifecycleWireFixture) Authenticate(ctx context.Context, req *authnv1.AuthnRequest) (*authnv1.AuthnResponse, error) {
	f.mu.Lock()
	if req.GetApiKey() != "" {
		f.exchange++
		if f.failExchange {
			f.mu.Unlock()
			return nil, status.Error(codes.Unauthenticated, "owned API key refused")
		}
		if f.entered != nil {
			close(f.entered)
			f.entered = nil
		}
		release := f.release
		f.mu.Unlock()
		if release != nil {
			select {
			case <-release:
			case <-ctx.Done():
				return nil, status.FromContextError(ctx.Err()).Err()
			}
		}
		f.mu.Lock()
	}
	defer f.mu.Unlock()
	md, _ := metadata.FromIncomingContext(ctx)
	if values := md.Get("authorization"); len(values) > 0 && values[0] != "" {
		return nil, status.Error(codes.InvalidArgument, "internal recovery sent cached authorization")
	}
	if len(md.Get("x-api-key")) > 0 {
		return nil, status.Error(codes.InvalidArgument, "internal recovery sent raw API key")
	}
	return &authnv1.AuthnResponse{AccessToken: fmt.Sprintf("owned-key-%d", f.exchange), ExpiresAtUnix: time.Now().Add(time.Hour).Unix(), Principal: proto.Clone(f.principal).(*authnv1.Principal)}, nil
}

func (f *lifecycleWireFixture) RefreshToken(ctx context.Context, _ *authnv1.RefreshTokenRequest) (*authnv1.RefreshTokenResponse, error) {
	f.mu.Lock()
	f.refresh++
	count := f.refresh
	if f.failRefresh {
		f.mu.Unlock()
		return nil, status.Error(codes.Unauthenticated, "owned refresh refused")
	}
	if f.refreshEntered != nil {
		close(f.refreshEntered)
		f.refreshEntered = nil
	}
	release := f.refreshRelease
	f.mu.Unlock()
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	return &authnv1.RefreshTokenResponse{AccessToken: fmt.Sprintf("owned-rotated-%d", count), RefreshToken: fmt.Sprintf("owned-next-%d", count), AccessTokenExpiresIn: 3600}, nil
}

func (f *lifecycleWireFixture) admit(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	if values := md.Get("authorization"); len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") || len(strings.TrimPrefix(values[0], "Bearer ")) == 0 {
		return status.Error(codes.Unauthenticated, "singleton bearer required")
	}
	f.mu.Lock()
	f.business++
	f.wire = append(f.wire, md.Copy())
	f.mu.Unlock()
	return grpc.SetHeader(ctx, metadata.Pairs("x-udb-version", SDKVersion))
}

func (f *lifecycleWireFixture) GetCapabilities(ctx context.Context, _ *entityv1.CapabilitiesRequest) (*entityv1.CapabilitiesResponse, error) {
	if err := f.admit(ctx); err != nil {
		return nil, err
	}
	return &entityv1.CapabilitiesResponse{}, nil
}
func (f *lifecycleWireFixture) ValidateToken(ctx context.Context, _ *authnv1.ValidateTokenRequest) (*authnv1.ValidateTokenResponse, error) {
	if err := f.admit(ctx); err != nil {
		return nil, err
	}
	return &authnv1.ValidateTokenResponse{Valid: true}, nil
}

func serveLifecycleWire(t *testing.T, f *lifecycleWireFixture) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	authnv1.RegisterAuthnServiceServer(srv, f)
	servicesv1.RegisterDataBrokerServer(srv, f)
	handler := func(_ any, s grpc.ServerStream) error {
		if err := s.RecvMsg(&emptypb.Empty{}); err != nil {
			return err
		}
		if err := f.admit(s.Context()); err != nil {
			return err
		}
		if err := s.SendMsg(&emptypb.Empty{}); err != nil {
			return err
		}
		if method, _ := grpc.MethodFromServerStream(s); strings.HasSuffix(method, "/Bidi") {
			if err := s.RecvMsg(&emptypb.Empty{}); err != io.EOF {
				return err
			}
		}
		return nil
	}
	srv.RegisterService(&grpc.ServiceDesc{ServiceName: "credential.fixture.Streams", HandlerType: (*interface{})(nil), Streams: []grpc.StreamDesc{
		{StreamName: "Server", Handler: handler, ServerStreams: true},
		{StreamName: "Client", Handler: handler, ClientStreams: true},
		{StreamName: "Bidi", Handler: handler, ClientStreams: true, ServerStreams: true},
	}}, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func lifecyclePrincipal() *authnv1.Principal {
	return &authnv1.Principal{TenantId: "fixture-tenant", ProjectId: "fixture-project", UserId: "fixture-user", ServiceIdentity: "fixture-service", Scopes: []string{"data:read"}}
}

func lifecycleAssertWireBearer(t *testing.T, f *lifecycleWireFixture, start int, want string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.wire) <= start {
		t.Fatal("actual business calls did not reach the fixture")
	}
	for _, md := range f.wire[start:] {
		values := md.Get("authorization")
		if len(values) != 1 || values[0] != want {
			t.Fatal("actual transport emitted an earlier or different owned bearer")
		}
		for _, key := range []string{"x-api-key", "x-udb-api-key"} {
			for _, value := range md.Get(key) {
				if value != "" {
					t.Fatal("actual owned transport emitted a raw API key")
				}
			}
		}
	}
}

func lifecycleBusinessCalls(u *Udb) []struct {
	name string
	call func(context.Context) error
} {
	calls := []struct {
		name string
		call func(context.Context) error
	}{
		{"data", func(ctx context.Context) error {
			_, e := u.Data.Broker.GetCapabilities(u.Data.Context(ctx), &entityv1.CapabilitiesRequest{})
			return e
		}},
		{"auth", func(ctx context.Context) error {
			_, e := u.Auth.Authn.ValidateToken(u.Auth.Context(ctx), &authnv1.ValidateTokenRequest{Token: "fixture-access", TokenType: authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS})
			return e
		}},
		{"generated", func(ctx context.Context) error {
			return u.Generated.InvokeUnary(ctx, "/udb.services.v1.DataBroker/GetCapabilities", &entityv1.CapabilitiesRequest{}, &entityv1.CapabilitiesResponse{})
		}},
		{"media", func(ctx context.Context) error {
			return u.webrtcConn.Invoke(ctx, "/udb.services.v1.DataBroker/GetCapabilities", &entityv1.CapabilitiesRequest{}, &entityv1.CapabilitiesResponse{})
		}},
	}
	for _, channel := range []struct {
		name string
		conn *grpc.ClientConn
	}{{"broker", u.brokerConn}, {"native", u.authConn}, {"media", u.webrtcConn}} {
		for _, desc := range []grpc.StreamDesc{{StreamName: "Server", ServerStreams: true}, {StreamName: "Client", ClientStreams: true}, {StreamName: "Bidi", ClientStreams: true, ServerStreams: true}} {
			channel, desc := channel, desc
			calls = append(calls, struct {
				name string
				call func(context.Context) error
			}{channel.name + "/" + desc.StreamName, func(ctx context.Context) error {
				stream, e := channel.conn.NewStream(ctx, &desc, "/credential.fixture.Streams/"+desc.StreamName)
				if e != nil {
					return e
				}
				if e = stream.SendMsg(&emptypb.Empty{}); e != nil {
					return e
				}
				if e = stream.CloseSend(); e != nil {
					return e
				}
				if e = stream.RecvMsg(&emptypb.Empty{}); e != nil {
					return e
				}
				e = stream.RecvMsg(&emptypb.Empty{})
				if e == io.EOF {
					return nil
				}
				return e
			}})
		}
	}
	for _, desc := range []grpc.StreamDesc{{StreamName: "Server", ServerStreams: true}, {StreamName: "Client", ClientStreams: true}, {StreamName: "Bidi", ClientStreams: true, ServerStreams: true}} {
		desc := desc
		calls = append(calls, struct {
			name string
			call func(context.Context) error
		}{"generated/" + desc.StreamName, func(ctx context.Context) error {
			var stream grpc.ClientStream
			var e error
			if desc.StreamName == "Server" {
				stream, e = u.Generated.NewServerStream(ctx, "/credential.fixture.Streams/Server", &desc, &emptypb.Empty{})
			} else {
				stream, e = u.Generated.NewClientStream(ctx, "/credential.fixture.Streams/"+desc.StreamName, &desc)
				if e == nil {
					e = stream.SendMsg(&emptypb.Empty{})
				}
				if e == nil {
					e = stream.CloseSend()
				}
			}
			if e != nil {
				return e
			}
			if e = stream.RecvMsg(&emptypb.Empty{}); e != nil {
				return e
			}
			e = stream.RecvMsg(&emptypb.Empty{})
			if e == io.EOF {
				return nil
			}
			return e
		}})
	}
	return calls
}

func TestCredentialLifecycleTransportRefusesEveryPathAndRecoversOneFlight(t *testing.T) {
	f := &lifecycleWireFixture{principal: lifecyclePrincipal()}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{Target: serveLifecycleWire(t, f), AuthTarget: serveLifecycleWire(t, f), Username: "fixture", Password: "fixture", Deadline: 2 * time.Second, Retry: RetryConfig{MaxAttempts: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	// Ensure the third independently dialed transport shares the same owner.
	media, err := newGrpcClient(serveLifecycleWire(t, f), append([]grpc.DialOption{grpc.WithTransportCredentials(transportCreds(nil))}, sess.Generated.DialOptions()...)...)
	if err != nil {
		t.Fatal(err)
	}
	defer media.Close()
	sess.webrtcConn = media
	sess.stopOnce.Do(func() { close(sess.stopRefresh) })
	<-sess.refreshDone
	for _, path := range lifecycleBusinessCalls(sess.Udb) {
		if err := path.call(ctx); err != nil {
			t.Fatalf("healthy %s: %v", path.name, err)
		}
	}
	f.mu.Lock()
	baseline := f.business
	f.failRefresh, f.failLogin = true, true
	f.mu.Unlock()
	tok, _ := sess.tm.store.Load(ctx)
	tok.ExpiresAt = time.Now().Add(-time.Second)
	_ = sess.tm.store.Save(ctx, tok)
	for _, path := range lifecycleBusinessCalls(sess.Udb) {
		if code := status.Code(path.call(ctx)); code != codes.Unauthenticated {
			t.Fatalf("failed %s emitted old auth or lost refusal: %s", path.name, code)
		}
	}
	f.mu.Lock()
	if f.business != baseline {
		t.Fatal("failed credential reached business transport")
	}
	f.failLogin = false
	f.mu.Unlock()
	// The single background leader includes real RefreshToken refusal, real
	// re-login, principal verification, store Save, and bearer publication.
	sess.backgroundRefresh()
	if sess.RefreshErr() != nil {
		t.Fatalf("recovery: %v", sess.RefreshErr())
	}
	for _, path := range lifecycleBusinessCalls(sess.Udb) {
		if err := path.call(ctx); err != nil {
			t.Fatalf("recovered %s: %v", path.name, err)
		}
	}
	lifecycleAssertWireBearer(t, f, baseline, sess.Bearer())
	canceled, stop := context.WithCancel(ctx)
	stop()
	f.mu.Lock()
	beforeCancel := f.business
	f.mu.Unlock()
	for _, path := range lifecycleBusinessCalls(sess.Udb) {
		if code := status.Code(path.call(canceled)); code != codes.Canceled {
			t.Fatalf("canceled %s: %s", path.name, code)
		}
	}
	sess.mu.Lock()
	sess.poisoned = true
	sess.lastRefreshErr = fmt.Errorf("owned refusal")
	sess.mu.Unlock()
	for _, path := range lifecycleBusinessCalls(sess.Udb) {
		if code := status.Code(path.call(sess.AsUser(ctx, "person-bearer"))); code != codes.Unauthenticated {
			t.Fatalf("poisoned delegation %s: %s", path.name, code)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.business != beforeCancel {
		t.Fatal("cancelled or poisoned request reached business transport")
	}
}

func TestCredentialLifecycleAPIKeyDemandAndBackgroundShareActualExchange(t *testing.T) {
	f := &lifecycleWireFixture{principal: lifecyclePrincipal()}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	u, err := Connect(ctx, Config{Target: serveLifecycleWire(t, f), AuthTarget: serveLifecycleWire(t, f), WebRTCTarget: serveLifecycleWire(t, f), Credentials: Credentials{APIKey: "owned-key"}, Retry: RetryConfig{MaxAttempts: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	f.mu.Lock()
	f.entered = make(chan struct{})
	f.release = make(chan struct{})
	entered, release := f.entered, f.release
	f.mu.Unlock()
	u.apiKey.mu.Lock()
	u.apiKey.renewAt = time.Now().Add(-time.Second)
	u.apiKey.mu.Unlock()
	var workers sync.WaitGroup
	errs := make(chan error, 24)
	workers.Add(1)
	go func() { defer workers.Done(); _, e := u.renewAPIKey(ctx, u.apiKey, false); errs <- e }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("actual background leader did not enter exchange")
	}
	for range 20 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, e := u.Data.Broker.GetCapabilities(ctx, &entityv1.CapabilitiesRequest{})
			errs <- e
		}()
	}
	close(release)
	workers.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	lifecycleAssertWireBearer(t, f, 0, u.Generated.options().Authorization)
	f.mu.Lock()
	if f.exchange != 2 {
		t.Fatalf("background+demand repeated actual exchange: %d", f.exchange)
	}
	f.failExchange = true
	before := f.business
	f.mu.Unlock()
	u.apiKey.mu.Lock()
	u.apiKey.renewAt = time.Now().Add(-time.Second)
	u.apiKey.mu.Unlock()
	for _, path := range lifecycleBusinessCalls(u) {
		if code := status.Code(path.call(ctx)); code != codes.Unauthenticated {
			t.Fatalf("failed API key %s: %s", path.name, code)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.business != before {
		t.Fatal("failed API exchange emitted prior still-live bearer")
	}
}

func TestCredentialLifecycleRejectsRawAndMultipleCredentialConfigurations(t *testing.T) {
	for _, credentials := range []Credentials{{APIKey: "key", RawAPIKey: true}, {APIKey: "key", Bearer: "bearer"}} {
		u, err := Connect(context.Background(), Config{Target: "127.0.0.1:1", Credentials: credentials})
		if err == nil || u != nil {
			if u != nil {
				_ = u.Close()
			}
			t.Fatal("invalid credential mode must refuse before dial")
		}
		if !strings.Contains(err.Error(), "Credentials.") {
			t.Fatal("credential mode refusal must name its configuration field")
		}
	}
}

func TestCredentialLifecycleReusedPublicStreamContextCannotBypassOwner(t *testing.T) {
	for _, opening := range []string{"generated", "direct"} {
		t.Run(opening, func(t *testing.T) {
			f := &lifecycleWireFixture{principal: lifecyclePrincipal()}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			sess, err := ConnectEnterprise(ctx, EnterpriseConfig{Target: serveLifecycleWire(t, f), Username: "fixture", Password: "fixture", Retry: RetryConfig{MaxAttempts: 1}})
			if err != nil {
				t.Fatal(err)
			}
			defer sess.Close()
			sess.stopOnce.Do(func() { close(sess.stopRefresh) })
			<-sess.refreshDone
			desc := &grpc.StreamDesc{StreamName: "Bidi", ClientStreams: true, ServerStreams: true}
			const method = "/credential.fixture.Streams/Bidi"
			var stream grpc.ClientStream
			if opening == "generated" {
				stream, err = sess.Generated.NewClientStream(ctx, method, desc)
			} else {
				stream, err = sess.brokerConn.NewStream(ctx, desc, method)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
				t.Fatal(err)
			}
			if err := stream.RecvMsg(&emptypb.Empty{}); err != nil {
				t.Fatal(err)
			}
			if stream.Context().Err() != nil {
				t.Fatal("bidi fixture must remain live until client closes sending")
			}
			sess.mu.Lock()
			sess.poisoned = true
			sess.lastRefreshErr = fmt.Errorf("owned refusal")
			sess.mu.Unlock()
			if _, err := sess.Generated.NewClientStream(stream.Context(), method, desc); status.Code(err) != codes.Unauthenticated {
				t.Fatalf("public context bypassed generated owner: %s", status.Code(err))
			}
			if _, err := sess.brokerConn.NewStream(stream.Context(), desc, method); status.Code(err) != codes.Unauthenticated {
				t.Fatalf("public context bypassed transport owner: %s", status.Code(err))
			}
			_ = stream.CloseSend()
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.business != 1 {
				t.Fatalf("refused new stream reached server: %d", f.business)
			}
		})
	}
}

func TestCredentialLifecycleRefreshedScopeMismatchNeverInstallsOrReplays(t *testing.T) {
	f := &lifecycleWireFixture{principal: lifecyclePrincipal()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{Target: serveLifecycleWire(t, f), Username: "fixture", Password: "fixture", Retry: RetryConfig{MaxAttempts: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	sess.stopOnce.Do(func() { close(sess.stopRefresh) })
	<-sess.refreshDone
	before := sess.Bearer()
	tok, _ := sess.tm.store.Load(ctx)
	tok.ExpiresAt = time.Now().Add(-time.Second)
	_ = sess.tm.store.Save(ctx, tok)
	f.mu.Lock()
	f.principal.Scopes = []string{"data:write"}
	f.mu.Unlock()
	for range 2 {
		if _, err := sess.Data.Broker.GetCapabilities(ctx, &entityv1.CapabilitiesRequest{}); status.Code(err) != codes.Unauthenticated || !strings.Contains(err.Error(), "scopes") {
			t.Fatalf("scope mismatch did not refuse: %v", err)
		}
	}
	sess.backgroundRefresh()
	if sess.Bearer() != before || sess.Generated.options().Authorization != before {
		t.Fatal("unverified changed authority was installed")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refresh != 1 || f.login != 1 || f.business != 0 {
		t.Fatalf("refused rotation was replayed or admitted: refresh=%d login=%d business=%d", f.refresh, f.login, f.business)
	}
}

func TestCredentialLifecycleCanceledRefreshUsesReloginWithoutTokenReplay(t *testing.T) {
	f := &lifecycleWireFixture{principal: lifecyclePrincipal()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{Target: serveLifecycleWire(t, f), Username: "fixture", Password: "fixture", Retry: RetryConfig{MaxAttempts: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	sess.stopOnce.Do(func() { close(sess.stopRefresh) })
	<-sess.refreshDone
	tok, _ := sess.tm.store.Load(ctx)
	tok.ExpiresAt = time.Now().Add(-time.Second)
	_ = sess.tm.store.Save(ctx, tok)
	f.mu.Lock()
	f.refreshEntered = make(chan struct{})
	f.refreshRelease = make(chan struct{})
	entered, release := f.refreshEntered, f.refreshRelease
	f.mu.Unlock()
	callCtx, callCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := sess.Data.Broker.GetCapabilities(callCtx, &entityv1.CapabilitiesRequest{})
		done <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("actual refresh did not enter")
	}
	callCancel()
	close(release)
	select {
	case err := <-done:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("canceled refresh: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("canceled caller did not finish")
	}
	sess.backgroundRefresh()
	if sess.RefreshErr() != nil {
		t.Fatalf("owned re-login did not recover: %v", sess.RefreshErr())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refresh != 1 || f.login != 2 || f.business != 0 {
		t.Fatalf("ambiguous refresh was replayed: refresh=%d login=%d business=%d", f.refresh, f.login, f.business)
	}
}

func TestCredentialLifecycleAPIKeyChangedScopesRefusesDelegation(t *testing.T) {
	f := &lifecycleWireFixture{principal: lifecyclePrincipal()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u, err := Connect(ctx, Config{Target: serveLifecycleWire(t, f), AuthTarget: serveLifecycleWire(t, f), WebRTCTarget: serveLifecycleWire(t, f), Credentials: Credentials{APIKey: "owned-key"}, Retry: RetryConfig{MaxAttempts: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	before := u.Generated.options().Authorization
	f.mu.Lock()
	f.principal.Scopes = []string{"data:write"}
	f.mu.Unlock()
	u.apiKey.mu.Lock()
	u.apiKey.renewAt = time.Now().Add(-time.Second)
	u.apiKey.mu.Unlock()
	if _, err := u.Data.Broker.GetCapabilities(ctx, &entityv1.CapabilitiesRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("changed service scopes: %v", err)
	}
	for _, path := range lifecycleBusinessCalls(u) {
		if code := status.Code(path.call(u.AsUser(ctx, "person-bearer"))); code != codes.Unauthenticated {
			t.Fatalf("changed-scope delegation %s: %s", path.name, code)
		}
	}
	if u.Generated.options().Authorization != before {
		t.Fatal("changed API-key authority was installed")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.exchange != 2 || f.business != 0 {
		t.Fatalf("changed-scope authority reached transport: exchange=%d business=%d", f.exchange, f.business)
	}
}

func TestCredentialLifecycleChangedPrincipalBindingNeverInstalls(t *testing.T) {
	for _, field := range []string{"principal_id", "subject", "account_kind"} {
		for _, mode := range []string{"password", "api-key"} {
			t.Run(mode+"/"+field, func(t *testing.T) {
				f := &lifecycleWireFixture{principal: lifecyclePrincipal()}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				target := serveLifecycleWire(t, f)
				var u *Udb
				if mode == "password" {
					sess, err := ConnectEnterprise(ctx, EnterpriseConfig{Target: target, Username: "fixture", Password: "fixture", Retry: RetryConfig{MaxAttempts: 1}})
					if err != nil {
						t.Fatal(err)
					}
					defer sess.Close()
					sess.stopOnce.Do(func() { close(sess.stopRefresh) })
					<-sess.refreshDone
					tok, _ := sess.tm.store.Load(ctx)
					tok.ExpiresAt = time.Now().Add(-time.Second)
					_ = sess.tm.store.Save(ctx, tok)
					u = sess.Udb
				} else {
					var err error
					u, err = Connect(ctx, Config{Target: target, Credentials: Credentials{APIKey: "owned-key"}, Retry: RetryConfig{MaxAttempts: 1}})
					if err != nil {
						t.Fatal(err)
					}
					defer u.Close()
					u.apiKey.mu.Lock()
					u.apiKey.renewAt = time.Now().Add(-time.Second)
					u.apiKey.mu.Unlock()
				}
				before := u.Generated.options().Authorization
				f.mu.Lock()
				switch field {
				case "principal_id":
					f.principal.PrincipalId = "foreign-owner"
				case "subject":
					f.principal.Subject = "foreign-subject"
				case "account_kind":
					f.principal.AccountKind++
				}
				f.mu.Unlock()
				if _, err := u.Data.Broker.GetCapabilities(ctx, &entityv1.CapabilitiesRequest{}); status.Code(err) != codes.Unauthenticated || !strings.Contains(err.Error(), field) {
					t.Fatalf("changed binding %s: %v", field, err)
				}
				if _, err := u.Data.Broker.GetCapabilities(u.AsUser(ctx, "person-bearer"), &entityv1.CapabilitiesRequest{}); status.Code(err) != codes.Unauthenticated {
					t.Fatalf("changed binding delegated: %v", err)
				}
				if u.Generated.options().Authorization != before {
					t.Fatal("foreign principal binding was installed")
				}
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.business != 0 {
					t.Fatal("foreign principal binding reached business transport")
				}
			})
		}
	}
}

func TestCredentialLifecycleClosedOwnerReadinessRefusesActualTransport(t *testing.T) {
	f := &lifecycleWireFixture{principal: lifecyclePrincipal()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	u, err := Connect(ctx, Config{Target: serveLifecycleWire(t, f), Credentials: Credentials{Bearer: "owned-static"}, Retry: RetryConfig{MaxAttempts: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	if u.CredentialErr() != nil {
		t.Fatal("open static client is not ready")
	}
	if _, err := u.Data.Broker.GetCapabilities(ctx, &entityv1.CapabilitiesRequest{}); err != nil {
		t.Fatal(err)
	}
	if err := u.Close(); err != nil {
		t.Fatal(err)
	}
	if err := u.CredentialErr(); status.Code(err) != codes.Canceled || !strings.Contains(err.Error(), "credential owner is closed") {
		t.Fatalf("closed client readiness: %v", err)
	}
	if _, err := u.Data.Broker.GetCapabilities(ctx, &entityv1.CapabilitiesRequest{}); status.Code(err) != codes.Canceled {
		t.Fatalf("closed client transport: %v", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.business != 1 {
		t.Fatal("closed client reached actual business transport")
	}
}

func TestCredentialLifecycleAuthContextEmptyAuditHeadersGetsRequestID(t *testing.T) {
	f := &lifecycleWireFixture{principal: lifecyclePrincipal()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{Target: serveLifecycleWire(t, f), Username: "fixture", Password: "fixture", Retry: RetryConfig{MaxAttempts: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	sess.stopOnce.Do(func() { close(sess.stopRefresh) })
	<-sess.refreshDone
	callerMD := metadata.Pairs("x-request-id", "", "x-correlation-id", "   ", "traceparent", "")
	original := callerMD.Copy()
	caller := metadata.NewOutgoingContext(ctx, callerMD)
	request := &authnv1.ValidateTokenRequest{Token: "fixture-access", TokenType: authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS}
	for _, path := range []string{"native", "generated"} {
		callCtx := sess.Auth.Context(caller)
		if path == "native" {
			_, err = sess.Auth.Authn.ValidateToken(callCtx, request)
		} else {
			err = sess.Generated.InvokeUnary(callCtx, "/udb.core.authn.services.v1.AuthnService/ValidateToken", request, &authnv1.ValidateTokenResponse{})
		}
		if err != nil {
			t.Fatalf("empty audit %s: %v", path, err)
		}
	}
	if !reflect.DeepEqual(callerMD, original) {
		t.Fatal("automatic request ID changed caller metadata")
	}
	lifecycleAssertWireBearer(t, f, 0, sess.Bearer())
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.wire) != 2 {
		t.Fatal("actual audit fixture did not receive both paths")
	}
	ids := []string{}
	for _, md := range f.wire {
		values := md.Get("x-request-id")
		if len(values) != 1 || strings.TrimSpace(values[0]) == "" {
			t.Fatal("typed empty audit metadata suppressed automatic request ID")
		}
		ids = append(ids, values[0])
	}
	if ids[0] == ids[1] {
		t.Fatal("independent calls must generate independent request IDs")
	}
}
