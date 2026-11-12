package udbclient

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Recovery after a broker restart.
//
// A client must outlive the broker it talks to. Three things used to keep a Go
// client broken after the broker restarted, until its process was restarted:
//
//   - The channel used grpc's default reconnect backoff, which grows to 120s.
//     A channel that failed a few dials while the broker was down kept failing
//     calls with UNAVAILABLE for up to two minutes after the broker was back.
//     Now the backoff is capped at reconnectMaxDelay, and a call that fails with
//     UNAVAILABLE wakes the channel to dial at once (at most once per
//     reconnectKickInterval, so an outage is not hammered).
//   - The owned bearer was renewed only when its local expiry came near. A
//     broker that refused it (sessions lost in the restart, a revoked session)
//     got the same refused bearer on every call until it expired. Now a refusal
//     of the owned bearer renews it (refresh, else sign in again; a service
//     account re-exchanges its key) and repeats the call once. The broker
//     refuses authentication before any handler runs, so the repeat cannot apply
//     a write twice. AuthnService calls are never repeated (their refusals are
//     about the credential in the request), and a bearer renewed moments ago
//     that is refused again is not renewed again (renewing cannot help).
//   - ConnectEnterprise gave up on the first UNAVAILABLE login, so a service
//     that started while the broker was restarting failed to start. It now
//     retries a transient failure like the service-account exchange does.

const (
	reconnectBaseDelay    = 500 * time.Millisecond
	reconnectMaxDelay     = 5 * time.Second
	reconnectKickInterval = time.Second
	// rejectedRenewInterval is how soon after renewing a refused bearer another
	// refusal is taken as final rather than as a reason to renew again.
	rejectedRenewInterval = 5 * time.Second
	authnServicePrefix    = "/udb.core.authn.services.v1.AuthnService/"
)

func reconnectParams() grpc.ConnectParams {
	return grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  reconnectBaseDelay,
			Multiplier: 1.6,
			Jitter:     0.2,
			MaxDelay:   reconnectMaxDelay,
		},
		MinConnectTimeout: 20 * time.Second,
	}
}

// kickReconnect makes a channel that failed a call with UNAVAILABLE dial again
// now instead of when its backoff ends.
func (g *GeneratedClient) kickReconnect(cc *grpc.ClientConn, err error) {
	if cc == nil || status.Code(err) != codes.Unavailable {
		return
	}
	now := time.Now().UnixNano()
	last := g.reconnectKickedAt.Load()
	if now-last < int64(reconnectKickInterval) || !g.reconnectKickedAt.CompareAndSwap(last, now) {
		return
	}
	cc.ResetConnectBackoff()
}

// sentAuthorization is the authorization header a prepared call carries.
func sentAuthorization(ctx context.Context) string {
	md, _ := metadata.FromOutgoingContext(ctx)
	values := md.Get("authorization")
	if len(values) != 1 {
		return ""
	}
	return values[0]
}

// renewRejectedBearer reports whether the call should be made once more: the
// broker refused, as UNAUTHENTICATED, the bearer the credential owner attached,
// and the owner has marked it for renewal.
func (g *GeneratedClient) renewRejectedBearer(method string, err error, sent string) bool {
	if sent == "" || status.Code(err) != codes.Unauthenticated || strings.HasPrefix(method, authnServicePrefix) {
		return false
	}
	reject := g.bearerRejected.Load()
	if reject == nil {
		return false
	}
	return (*reject)(sent)
}

// bearerRejected handles the broker's refusal of bearer sent. It returns true
// when a repeat can carry a different, renewed bearer.
func (o *credentialOwner) bearerRejected(sent string) bool {
	if o.closedError() != nil {
		return false
	}
	p := o.provider.Load()
	if p == nil || p.current == nil || p.invalidate == nil {
		return false
	}
	o.rejectMu.Lock()
	defer o.rejectMu.Unlock()
	switch {
	case sent == o.lastRejected:
		// Already marked by a concurrent call; the next resolve renews it.
		return true
	case sent != p.current():
		// Not the owner's bearer (a delegated AsUser bearer), or one already
		// replaced by an ordinary refresh: nothing to renew.
		return false
	case !o.lastRejectedAt.IsZero() && time.Since(o.lastRejectedAt) < rejectedRenewInterval:
		// The bearer renewed moments ago is refused too.
		return false
	}
	o.lastRejected, o.lastRejectedAt = sent, time.Now()
	p.invalidate(sent)
	return true
}
