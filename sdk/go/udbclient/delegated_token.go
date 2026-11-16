package udbclient

import (
	"context"
	"errors"
	"fmt"
	"strings"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
)

// A durable async job (an export, a report, a nightly sync) often has to act
// for the user who started it, long after that user's own short-lived access
// token expired. AuthnService.ExchangeTokenForJob mints a delegated job token
// for that: the service presents the user's current access token, and the
// broker returns a token whose subject is the user, whose RFC 8693 actor is
// the service, which carries the job's purpose and id, and whose scopes are
// those both the user and the service's grant allow. It has no refresh token,
// cannot be exchanged again, lives at most the broker's
// UDB_DELEGATED_TOKEN_MAX_TTL (24 hours by default), and stops working as soon
// as the user, the token or the service's udb:token:delegate grant is revoked.

// ExchangeTokenForJobRequest is what ExchangeTokenForJob asks for.
type ExchangeTokenForJobRequest struct {
	// SubjectToken is the user's current access token ("Bearer " prefix optional).
	SubjectToken string
	// Purpose says why the job acts for the user, e.g. "export"; it is signed
	// into the token.
	Purpose string
	// JobID names the job; at most 200 bytes.
	JobID string
	// TTLSeconds is the requested lifetime; 0 asks for the broker's maximum.
	TTLSeconds int64
}

// DelegatedJobToken is a token a job uses to act for Subject as Actor.
type DelegatedJobToken struct {
	AccessToken string
	// JTI is the token's id; revoking it stops the token.
	JTI           string
	ExpiresAtUnix int64
	// Subject is the user the token acts for.
	Subject string
	// Actor is the acting service's identity (the RFC 8693 act claim).
	Actor   string
	Purpose string
	JobID   string
	// Scopes are those both the user's token and the service's grant allow.
	Scopes    []string
	TenantID  string
	ProjectID string
}

// ExchangeTokenForJob exchanges a user's access token for a delegated job
// token. ctx must carry the SERVICE's own bearer (a service account whose
// grant holds udb:token:delegate, in the user's tenant); the broker refuses
// any other caller, and any subject token that is not a valid user access
// token of that tenant, with PERMISSION_DENIED.
func (c *AuthClient) ExchangeTokenForJob(ctx context.Context, req ExchangeTokenForJobRequest) (*DelegatedJobToken, error) {
	if strings.TrimSpace(req.SubjectToken) == "" {
		return nil, errors.New("udb: ExchangeTokenForJob: SubjectToken is required")
	}
	if strings.TrimSpace(req.JobID) == "" {
		return nil, errors.New("udb: ExchangeTokenForJob: JobID is required")
	}
	if strings.TrimSpace(req.Purpose) == "" {
		return nil, errors.New("udb: ExchangeTokenForJob: Purpose is required")
	}
	if req.TTLSeconds < 0 {
		return nil, errors.New("udb: ExchangeTokenForJob: TTLSeconds must not be negative")
	}
	res, err := c.Authn.ExchangeTokenForJob(c.Context(ctx), &authnv1.ExchangeTokenForJobRequest{
		SubjectToken: req.SubjectToken,
		Purpose:      req.Purpose,
		JobId:        req.JobID,
		TtlSeconds:   req.TTLSeconds,
	})
	if err != nil {
		return nil, fmt.Errorf("udb: exchange the user's token for job %s: %w", req.JobID, err)
	}
	if res.GetAccessToken() == "" {
		return nil, fmt.Errorf("udb: exchange the user's token for job %s: the broker returned no token", req.JobID)
	}
	return &DelegatedJobToken{
		AccessToken:   res.GetAccessToken(),
		JTI:           res.GetJti(),
		ExpiresAtUnix: res.GetExpiresAtUnix(),
		Subject:       res.GetSubject(),
		Actor:         res.GetActor(),
		Purpose:       res.GetPurpose(),
		JobID:         res.GetJobId(),
		Scopes:        res.GetScopes(),
		TenantID:      res.GetTenantId(),
		ProjectID:     res.GetProjectId(),
	}, nil
}
