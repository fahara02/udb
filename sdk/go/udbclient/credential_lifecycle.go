package udbclient

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	credentialAuthenticate = "/udb.core.authn.services.v1.AuthnService/Authenticate"
	credentialLogin        = "/udb.core.authn.services.v1.AuthnService/Login"
	credentialRefresh      = "/udb.core.authn.services.v1.AuthnService/RefreshToken"
)

// A resolver belongs to the same GeneratedClient installed on every owned
// channel. Public static NewGenerated clients retain their explicit options.
type credentialResolver func(context.Context, string) (context.Context, error)

type credentialProvider struct {
	resolve func(context.Context) (string, error)
	refusal func() error
}

// A connected identity cannot change implicitly. In particular, a successfully
// rotated but refused credential must never retry its consumed refresh token.
type credentialBindingError struct{ cause error }

func (e *credentialBindingError) Error() string { return e.cause.Error() }
func (e *credentialBindingError) Unwrap() error { return e.cause }

// Metadata deliberately leaves an exchanged service's UserID empty. Pin the
// remaining verified identity fields too, without retaining a mutable pointer.
type credentialPrincipalBinding struct {
	principalID, subject string
	accountKind          int32
}

func principalCredentialBinding(p *authnv1.Principal) credentialPrincipalBinding {
	return credentialPrincipalBinding{principalID: p.GetPrincipalId(), subject: p.GetSubject(), accountKind: int32(p.GetAccountKind())}
}

func (b credentialPrincipalBinding) validate(p *authnv1.Principal) error {
	if p == nil {
		return fmt.Errorf("udb: renewal principal is missing")
	}
	for _, field := range []struct {
		name    string
		matches bool
	}{
		{"principal_id", p.GetPrincipalId() == b.principalID},
		{"subject", p.GetSubject() == b.subject},
		{"account_kind", int32(p.GetAccountKind()) == b.accountKind},
	} {
		if !field.matches {
			return fmt.Errorf("udb: renewal changed %s", field.name)
		}
	}
	return nil
}

type credentialOwner struct {
	u        *Udb
	ctx      context.Context
	cancel   context.CancelFunc
	provider atomic.Pointer[credentialProvider]
}

type credentialRecoveryKey struct{}
type credentialRecovery struct {
	owner  *credentialOwner
	method string
}
type credentialPreparedKey struct{}
type credentialPrepared struct {
	client *GeneratedClient
	method string
	used   atomic.Bool
}

func newCredentialOwner(u *Udb) *credentialOwner {
	ctx, cancel := context.WithCancel(context.Background())
	o := &credentialOwner{u: u, ctx: ctx, cancel: cancel}
	u.Generated.setCredentialResolver(o.prepare)
	return o
}

func (g *GeneratedClient) setCredentialResolver(resolve credentialResolver) {
	g.credential.Store(&resolve)
}

func credentialContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}

func (g *GeneratedClient) prepareCredential(ctx context.Context, method string) (context.Context, error) {
	if err := credentialContextError(ctx); err != nil {
		return nil, err
	}
	if prepared, ok := ctx.Value(credentialPreparedKey{}).(*credentialPrepared); ok && prepared.client == g && prepared.method == method && prepared.used.CompareAndSwap(false, true) {
		return ctx, nil
	}
	if resolve := g.credential.Load(); resolve != nil {
		var err error
		ctx, err = (*resolve)(ctx, method)
		if err != nil {
			return nil, err
		}
	}
	return g.outgoingContext(ctx), nil
}

// Deduplicate only the immediate wrapper-to-interceptor handoff. An exported
// stream.Context can be reused, so a lasting context marker cannot be trusted.
// A caller-owned connection might have no interceptor; retire on return too.
func (g *GeneratedClient) credentialHandoff(ctx context.Context, method string) (context.Context, func()) {
	prepared := &credentialPrepared{client: g, method: method}
	return context.WithValue(ctx, credentialPreparedKey{}, prepared), func() { prepared.used.Store(true) }
}

// Only the owning lifecycle can mint this capability, and only the exact
// authentication RPC it is about to issue can consume it. Request headers,
// request IDs, PUBLIC methods, and arbitrary Authn calls never bypass renewal.
func (o *credentialOwner) recoveryContext(ctx context.Context, method string) context.Context {
	return context.WithValue(ctx, credentialRecoveryKey{}, credentialRecovery{owner: o, method: method})
}

func (u *Udb) recoveryContext(ctx context.Context, method string) context.Context {
	if u.credential == nil {
		return ctx
	}
	return u.credential.recoveryContext(ctx, method)
}

func (o *credentialOwner) closedError() error {
	if o.ctx.Err() != nil {
		return status.Error(codes.Canceled, "udb: credential owner is closed")
	}
	return nil
}

func (o *credentialOwner) prepare(ctx context.Context, method string) (context.Context, error) {
	if err := credentialContextError(ctx); err != nil {
		return nil, err
	}
	if err := o.closedError(); err != nil {
		return nil, err
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	if md == nil {
		md = metadata.MD{}
	}
	if capability, ok := ctx.Value(credentialRecoveryKey{}).(credentialRecovery); ok && capability.owner == o && capability.method == method &&
		(method == credentialAuthenticate || method == credentialLogin || method == credentialRefresh) {
		// These operations carry their credential in the typed request. Suppress
		// the expired owned bearer and raw-key defaults during recovery.
		md.Set("authorization", "")
		md.Delete("x-api-key")
		md.Delete("x-udb-api-key")
		return metadata.NewOutgoingContext(ctx, md), nil
	}
	provider := o.provider.Load()
	if provider == nil {
		return ctx, nil
	}
	if provider.refusal != nil {
		if err := provider.refusal(); err != nil {
			return nil, err
		}
	}
	if _, delegated := ctx.Value(asUserContextKey{}).(asUserContext); delegated {
		values := md.Get("authorization")
		if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") || strings.TrimSpace(strings.TrimPrefix(values[0], "Bearer ")) == "" {
			return nil, status.Error(codes.Unauthenticated, "udb: delegated bearer is required")
		}
		md.Set("x-api-key", "")
		md.Delete("x-udb-api-key")
		return metadata.NewOutgoingContext(ctx, md), nil
	}
	// Closing cancels an in-flight owned renewal as well as the channels. A
	// follower still waits with its own context and cannot cancel another flight.
	resolveCtx, cancel := context.WithTimeout(ctx, bgRefreshTimeout)
	stop := context.AfterFunc(o.ctx, cancel)
	bearer, err := provider.resolve(resolveCtx)
	stop()
	cancel()
	if err != nil {
		return nil, err
	}
	if err := credentialContextError(ctx); err != nil {
		return nil, err
	}
	if err := o.closedError(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(bearer) == "" {
		return nil, status.Error(codes.Unauthenticated, "udb: credential renewal returned no bearer")
	}
	md.Set("authorization", bearer)
	md.Delete("x-api-key")
	md.Delete("x-udb-api-key")
	return metadata.NewOutgoingContext(ctx, md), nil
}

func credentialRefusal(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.Canceled || status.Code(err) == codes.DeadlineExceeded {
		return err
	}
	if errors.Is(err, context.Canceled) {
		return status.FromContextError(context.Canceled).Err()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(context.DeadlineExceeded).Err()
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	return status.Error(codes.Unauthenticated, fmt.Sprintf("udb: credential renewal failed: %v", err))
}
