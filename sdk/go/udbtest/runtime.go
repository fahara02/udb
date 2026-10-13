package udbtest

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"github.com/fahara02/udb/sdk/go/internal/protoschema"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// NewFake serves descriptor-defined tables with the same JSON/YAML policy file
// as `udb policy apply`. With no policy file it denies data requests by default.
// Call Close when finished. New(t, entities...) remains a convenience fixture
// with an explicit allow-all test policy; use NewFake for policy conformance.
func NewFake(descriptorSet *descriptorpb.FileDescriptorSet, policyFile string) (*Fake, error) {
	if descriptorSet == nil {
		return nil, fmt.Errorf("udbtest: descriptorSet is required")
	}
	files, err := protodesc.NewFiles(descriptorSet)
	if err != nil {
		return nil, fmt.Errorf("udbtest: descriptor set: %w", err)
	}
	f := newFake()
	if policyFile != "" {
		f.policy.policies, err = loadPolicies(policyFile)
		if err != nil {
			return nil, err
		}
	}
	var register func(protoreflect.MessageDescriptors)
	register = func(messages protoreflect.MessageDescriptors) {
		for i := 0; i < messages.Len(); i++ {
			message := messages.Get(i)
			if protoschema.Table(message).GetIsTable() {
				f.Register(dynamicpb.NewMessage(message))
			}
			register(message.Messages())
		}
	}
	files.RangeFiles(func(file protoreflect.FileDescriptor) bool { register(file.Messages()); return true })
	if len(f.entities) == 0 {
		return nil, fmt.Errorf("udbtest: descriptor set declares no tables")
	}
	if err := f.serve(); err != nil {
		return nil, err
	}
	return f, nil
}

func newFake() *Fake {
	f := &Fake{
		entities: map[string]*entity{}, rows: map[string]map[string]map[string]map[string]any{},
		identities: map[string]Identity{}, tokens: map[string]fakeToken{},
		cursors: map[string]consumerCursor{}, dlq: map[string]*fakeDlq{}, tokenLifetime: time.Hour,
	}
	f.authn.fake = f
	return f
}
func (f *Fake) serve() error {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("udbtest: listen: %w", err)
	}
	server := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			ctx, err := f.beforeRPC(ctx, info.FullMethod, request)
			var response any
			if err == nil {
				response, err = handler(ctx, request)
			}
			if err != nil {
				detailTrailer(ctx, err)
			}
			return response, err
		}),
		grpc.StreamInterceptor(func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			ctx, err := f.beforeRPC(stream.Context(), info.FullMethod, nil)
			if err == nil {
				err = handler(server, &fakeServerStream{ServerStream: stream, ctx: ctx})
			}
			if err != nil {
				detailTrailer(ctx, err)
			}
			return err
		}),
	)
	servicesv1.RegisterDataBrokerServer(server, f)
	authnv1.RegisterAuthnServiceServer(server, &f.authn)
	f.server, f.addr = server, listener.Addr().String()
	go func() { _ = server.Serve(listener) }()
	return nil
}
func (f *Fake) Close() error {
	f.mu.Lock()
	server := f.server
	f.server = nil
	f.mu.Unlock()
	if server != nil {
		server.Stop()
	}
	return nil
}

type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *fakeServerStream) Context() context.Context { return s.ctx }

type fakeIdentityKey struct{}
type fakeToken struct {
	identity Identity
	expires  time.Time
}

func callerIdentity(ctx context.Context) Identity {
	value, _ := ctx.Value(fakeIdentityKey{}).(fakeToken)
	return value.identity
}

// AddIdentityAPIKey provisions immutable verified identity attributes.
func (f *Fake) AddIdentityAPIKey(key string, identity Identity) {
	f.mu.Lock()
	defer f.mu.Unlock()
	identity.Scopes = append([]string(nil), identity.Scopes...)
	identity.Roles = append([]string(nil), identity.Roles...)
	if identity.Subject == "" {
		identity.Subject = identity.UserID
		if identity.Subject == "" {
			identity.Subject = identity.ServiceIdentity
		}
	}
	f.identities[key] = identity
}

// ClientForKey connects with a provisioned key without replacing its identity.
func (f *Fake) ClientForKey(t testing.TB, key string) *udbclient.Udb {
	t.Helper()
	f.mu.Lock()
	identity, ok := f.identities[key]
	f.mu.Unlock()
	if !ok {
		t.Fatalf("udbtest: key is not provisioned")
	}
	client, err := udbclient.Connect(context.Background(), udbclient.Config{
		Target: f.addr, TenantID: identity.TenantID, ProjectID: identity.ProjectID, Purpose: "test",
		Credentials: udbclient.Credentials{APIKey: key},
	})
	if err != nil {
		t.Fatalf("udbtest: connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func (f *Fake) beforeRPC(ctx context.Context, fullMethod string, request any) (context.Context, error) {
	method := fullMethod[strings.LastIndex(fullMethod, "/")+1:]
	if method != "Authenticate" {
		md, _ := metadata.FromIncomingContext(ctx)
		authorization := md.Get("authorization")
		if len(authorization) != 1 || !strings.HasPrefix(authorization[0], "Bearer ") {
			return ctx, refuse(codes.Unauthenticated, "", "a verified bearer is required")
		}
		f.mu.Lock()
		token, ok := f.tokens[strings.TrimPrefix(authorization[0], "Bearer ")]
		f.mu.Unlock()
		if !ok || !time.Now().Before(token.expires) {
			return ctx, refuse(codes.Unauthenticated, "", "bearer is unknown or expired")
		}
		for _, key := range []string{"x-tenant-id", "x-udb-tenant-id"} {
			if supplied := md.Get(key); len(supplied) > 1 || (len(supplied) == 1 && supplied[0] != token.identity.TenantID) {
				return ctx, refuse(codes.PermissionDenied, "UDB_TENANT_MISMATCH", "tenant header differs from verified tenant")
			}
		}
		for _, key := range []string{"x-project-id", "x-udb-project-id"} {
			if supplied := md.Get(key); len(supplied) > 1 || (len(supplied) == 1 && supplied[0] != token.identity.ProjectID) {
				return ctx, refuse(codes.PermissionDenied, "UDB_POLICY_DENIED", "project header differs from verified project")
			}
		}
		ctx = context.WithValue(ctx, fakeIdentityKey{}, token)
		if contextual, ok := request.(interface {
			GetContext() *entityv1.RequestContext
		}); ok {
			if err := bindBody(ctx, contextual.GetContext()); err != nil {
				return ctx, err
			}
		}
		switch method {
		case "ListDlqEvents", "GetDlqEvent", "ReplayDlqEvent", "DismissDlqEvent", "QuarantineDlqEvent":
			if !hasScope(token.identity, "udb:admin") {
				return ctx, missingScope("udb:admin")
			}
		}
		if typed, ok := request.(interface{ GetMessageType() string }); ok {
			action := method
			if action == "Insert" {
				action = "Upsert"
			}
			if err := f.authorize(ctx, typed.GetMessageType(), action); err != nil {
				return ctx, err
			}
		}
	}
	return ctx, f.admit(ctx, method, request)
}

func (f *Fake) authorize(ctx context.Context, resource, action string) error {
	md, _ := metadata.FromIncomingContext(ctx)
	purpose := ""
	if values := md.Get("x-purpose"); len(values) > 0 {
		purpose = values[0]
	}
	if strings.TrimSpace(purpose) == "" {
		return refuse(codes.PermissionDenied, "UDB_POLICY_DENIED", "purpose is required")
	}
	identity := callerIdentity(ctx)
	f.mu.Lock()
	allowed := f.policy.allows(identity, resource, action, purpose)
	f.mu.Unlock()
	if !allowed {
		r := refuse(codes.PermissionDenied, "UDB_POLICY_DENIED", "no matching allow or an explicit deny policy").(*refusal)
		r.detail = &entityv1.ErrorDetail{Reason: r.reason, Kind: entityv1.ErrorKind_ERROR_KIND_POLICY,
			Missing: map[string]string{"rule": action + " " + resource, "purpose": purpose}}
		return r
	}
	return nil
}

func bindBody(ctx context.Context, body *entityv1.RequestContext) error {
	identity := callerIdentity(ctx)
	if tenant := body.GetTenantId(); tenant != "" && tenant != identity.TenantID {
		return refuse(codes.PermissionDenied, "UDB_TENANT_MISMATCH", "body tenant differs from verified tenant")
	}
	if project := body.GetProjectId(); project != "" && project != identity.ProjectID {
		return refuse(codes.PermissionDenied, "UDB_POLICY_DENIED", "body project differs from verified project")
	}
	return nil
}

func missingScope(scope string) error {
	r := refuse(codes.PermissionDenied, "UDB_SCOPE_MISSING", "verified identity lacks "+scope).(*refusal)
	r.detail = &entityv1.ErrorDetail{Reason: r.reason, Kind: entityv1.ErrorKind_ERROR_KIND_PERMISSION,
		Missing: map[string]string{"scope": scope}}
	return r
}
