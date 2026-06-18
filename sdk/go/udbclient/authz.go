package udbclient

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	authzentv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/entity/v1"
	authzv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/services/v1"
)

// ── Phase 7: authorization ergonomics ────────────────────────────────────────
//
// These helpers sit on top of the existing AuthClient.Can / Authorize and the
// AuthzCache. They add the three shapes callers reach for most:
//
//   - Require: a one-line guard that returns a typed error on deny
//   - Explain: the full Decision without ever erroring on a clean deny
//   - BatchCan: many (object, action) checks in a single RPC
//
// Require/Explain route through an optional AuthzCache so a hot allow/deny path
// reuses the server-blessed Decision.cache_ttl_seconds window. BatchCan uses the
// dedicated BatchCheckPermissions RPC (Casbin object/action shape) and is not
// cached — it is the bulk path.

// ErrAuthzDenied is the sentinel a denied Require unwraps to. Callers can match
// it with errors.Is(err, ErrAuthzDenied) without depending on the concrete
// *AuthzDeniedError type.
var ErrAuthzDenied = errors.New("udb: authorization denied")

// AuthzDeniedError is the typed error Require returns when access is denied. It
// carries the resource/action/purpose that was checked and the server Decision
// so callers can inspect the deny reason, matched policies, and required scopes.
type AuthzDeniedError struct {
	Resource *authzv1.ResourceRef
	Action   string
	Purpose  string
	Decision *authzv1.Decision
}

func (e *AuthzDeniedError) Error() string {
	reason := ""
	if e.Decision != nil {
		reason = e.Decision.GetDenyReason()
	}
	res := resourceLabel(e.Resource)
	if reason == "" {
		return fmt.Sprintf("udb: authorization denied: action %q on %q", e.Action, res)
	}
	return fmt.Sprintf("udb: authorization denied: action %q on %q: %s", e.Action, res, reason)
}

// Is lets errors.Is(err, ErrAuthzDenied) succeed for any AuthzDeniedError.
func (e *AuthzDeniedError) Is(target error) bool { return target == ErrAuthzDenied }

// DeniedRequiredScopes surfaces the scopes the decision said were missing, when
// the broker populated them, so a caller can prompt for step-up.
func (e *AuthzDeniedError) DeniedRequiredScopes() []string {
	if e.Decision == nil {
		return nil
	}
	return e.Decision.GetRequiredScopes()
}

// resourceLabel renders a ResourceRef as a short human label for error text,
// mirroring the precedence cacheKey uses.
func resourceLabel(r *authzv1.ResourceRef) string {
	if r == nil {
		return ""
	}
	for _, v := range []string{r.GetMessageType(), r.GetResourceName(), r.GetTable(), r.GetResourceType()} {
		if v != "" {
			return v
		}
	}
	return ""
}

// canChecker is the minimal surface Require/Explain need; both *AuthClient and
// *AuthzCache satisfy it, so the helpers transparently use the cache when one is
// supplied and fall back to a direct call otherwise.
type canChecker interface {
	Can(ctx context.Context, resource *authzv1.ResourceRef, action, purpose string) (bool, *authzv1.Decision, error)
}

// Require checks (resource, action, purpose) and returns nil when allowed or an
// *AuthzDeniedError (which errors.Is(err, ErrAuthzDenied)) when denied. A
// transport/server error is returned as-is. It routes through the AuthClient's
// cache when one has been attached via AttachAuthzCache, otherwise it calls the
// server directly.
func (c *AuthClient) Require(ctx context.Context, resource *authzv1.ResourceRef, action, purpose string) error {
	return requireVia(ctx, c.checker(), resource, action, purpose)
}

// Explain returns the full Decision for (resource, action, purpose) without ever
// turning a clean deny into an error. Only transport/server failures error.
func (c *AuthClient) Explain(ctx context.Context, resource *authzv1.ResourceRef, action, purpose string) (*authzv1.Decision, error) {
	_, decision, err := c.checker().Can(ctx, resource, action, purpose)
	if err != nil {
		return nil, err
	}
	return decision, nil
}

// Require on the cache routes the underlying Can through the TTL cache.
func (a *AuthzCache) Require(ctx context.Context, resource *authzv1.ResourceRef, action, purpose string) error {
	return requireVia(ctx, a, resource, action, purpose)
}

// Explain on the cache returns the cached/fresh Decision without erroring on deny.
func (a *AuthzCache) Explain(ctx context.Context, resource *authzv1.ResourceRef, action, purpose string) (*authzv1.Decision, error) {
	_, decision, err := a.Can(ctx, resource, action, purpose)
	if err != nil {
		return nil, err
	}
	return decision, nil
}

func requireVia(ctx context.Context, chk canChecker, resource *authzv1.ResourceRef, action, purpose string) error {
	allowed, decision, err := chk.Can(ctx, resource, action, purpose)
	if err != nil {
		return err
	}
	if !allowed {
		return &AuthzDeniedError{Resource: resource, Action: action, Purpose: purpose, Decision: decision}
	}
	return nil
}

// AttachAuthzCache binds a decision cache so Can/Require/Explain reuse server
// TTLs. Pass nil to detach. The cache is created over this same AuthClient.
func (c *AuthClient) AttachAuthzCache(cache *AuthzCache) { c.authzCache = cache }

// checker returns the cache when attached, else the AuthClient itself.
func (c *AuthClient) checker() canChecker {
	if c.authzCache != nil {
		return c.authzCache
	}
	return c
}

// ── Batch authorization ──────────────────────────────────────────────────────

// BatchCheck is one (object, action) pair for BatchCan. Object is the Casbin
// object string (e.g. a table or resource name); Action is the verb.
type BatchCheck struct {
	Object string
	Action string
}

// BatchResult pairs a BatchCheck with its allow/deny outcome.
type BatchResult struct {
	BatchCheck
	Allowed bool
}

// BatchCan evaluates many permission checks in a single BatchCheckPermissions
// RPC. It returns the results in the same order as the input checks plus a
// keyed map ("object:action" → allowed) matching the server's response shape.
// The subject is the caller's UserID (falling back to ServiceIdentity) and the
// domain is the caller's TenantID, taken from Metadata.
func (c *AuthClient) BatchCan(ctx context.Context, checks []BatchCheck) ([]BatchResult, map[string]bool, error) {
	subject := c.Meta.UserID
	if subject == "" {
		subject = c.Meta.ServiceIdentity
	}
	pbChecks := make([]*authzv1.PermissionCheck, 0, len(checks))
	for _, ck := range checks {
		pbChecks = append(pbChecks, &authzv1.PermissionCheck{Object: ck.Object, Action: ck.Action})
	}
	resp, err := c.Authz.BatchCheckPermissions(c.Context(ctx), &authzv1.BatchCheckPermissionsRequest{
		UserId: subject,
		Domain: c.Meta.TenantID,
		Checks: pbChecks,
	})
	if err != nil {
		return nil, nil, err
	}
	results := resp.GetResults() // map["object:action"]bool
	out := make([]BatchResult, 0, len(checks))
	for _, ck := range checks {
		out = append(out, BatchResult{BatchCheck: ck, Allowed: results[batchKey(ck.Object, ck.Action)]})
	}
	return out, results, nil
}

// batchKey builds the "object:action" key the broker uses for BatchCheck
// results.
func batchKey(object, action string) string {
	return strings.Join([]string{object, action}, ":")
}

// ── Role / policy ergonomics (naming contract R2.0) ──────────────────────────
//
// AllowRole and BindRole are the canonical simple-client accessors that wrap the
// raw AuthzService.CreatePolicyRule / AssignRole RPCs. Each emits EXACTLY ONE
// RPC — no hidden List/Get probe — and seeds tenant/project/domain + the
// created_by/assigned_by actor from the caller Metadata. The raw RPCs stay
// reachable on AuthClient.Authz for anything these don't cover.

// AllowRole grants role permission to perform action on resource. It emits
// exactly one CreatePolicyRule RPC with effect = ALLOW: role becomes the policy
// subject, the resource's object key (message_type/resource_name/table, in that
// precedence) becomes the object, and tenant/project/domain + created_by are
// filled from the caller Metadata.
func (c *AuthClient) AllowRole(ctx context.Context, role string, resource *authzv1.ResourceRef, action string) (*authzv1.CreatePolicyRuleResponse, error) {
	meta := c.Meta
	actor := meta.UserID
	if actor == "" {
		actor = meta.ServiceIdentity
	}
	return c.Authz.CreatePolicyRule(c.Context(ctx), &authzv1.CreatePolicyRuleRequest{
		Subject:   role,
		Domain:    meta.TenantID,
		Object:    resourceLabel(resource),
		Action:    action,
		Effect:    authzentv1.PolicyEffect_POLICY_EFFECT_ALLOW,
		CreatedBy: actor,
		TenantId:  meta.TenantID,
		ProjectId: meta.ProjectID,
	})
}

// BindRole binds subject (a user/principal id) to role. It emits exactly one
// AssignRole RPC: subject is sent as both user_id and principal_id, role as
// role_id, and domain/tenant/project + assigned_by are filled from the caller
// Metadata.
func (c *AuthClient) BindRole(ctx context.Context, subject, role string) (*authzv1.AssignRoleResponse, error) {
	meta := c.Meta
	actor := meta.UserID
	if actor == "" {
		actor = meta.ServiceIdentity
	}
	return c.Authz.AssignRole(c.Context(ctx), &authzv1.AssignRoleRequest{
		UserId:      subject,
		PrincipalId: subject,
		RoleId:      role,
		Domain:      meta.TenantID,
		AssignedBy:  actor,
		TenantId:    meta.TenantID,
		ProjectId:   meta.ProjectID,
	})
}

// AllowRole on the AuthzFacade forwards to the underlying AuthClient so the
// canonical `u.Authz.AllowRole(...)` surface exists (one CreatePolicyRule RPC).
func (f *AuthzFacade) AllowRole(ctx context.Context, role string, resource *authzv1.ResourceRef, action string) (*authzv1.CreatePolicyRuleResponse, error) {
	return f.client.AllowRole(ctx, role, resource, action)
}

// BindRole on the AuthzFacade forwards to the underlying AuthClient so the
// canonical `u.Authz.BindRole(...)` surface exists (one AssignRole RPC).
func (f *AuthzFacade) BindRole(ctx context.Context, subject, role string) (*authzv1.AssignRoleResponse, error) {
	return f.client.BindRole(ctx, subject, role)
}

// ── Phase 7 (M5.3): policy-bundle signature verification ──────────────────────
//
// The broker signs a SignedPolicyBundle by computing an HMAC-SHA256 over the raw
// Bundle bytes with a shared secret and emitting the result as lowercase hex in
// Signature (despite the proto comment saying Base64 — the server emits hex, and
// we match the server). VerifyPolicyBundle recomputes that MAC and compares it in
// constant time, so a caller can trust a cached/offline bundle between fetches.

// ErrPolicyBundleSignature is the sentinel a failed VerifyPolicyBundle unwraps
// to. Match it with errors.Is(err, ErrPolicyBundleSignature).
var ErrPolicyBundleSignature = errors.New("udb: policy bundle signature mismatch")

// PolicyBundleSignatureError is the typed error returned when a bundle's HMAC
// signature does not match. It carries the bundle's key id / algorithm / version
// for diagnostics.
type PolicyBundleSignatureError struct {
	KeyID         string
	Algorithm     string
	PolicyVersion string
}

func (e *PolicyBundleSignatureError) Error() string {
	return fmt.Sprintf(
		"udb: policy bundle signature mismatch (key_id=%q algorithm=%q policy_version=%q)",
		e.KeyID, e.Algorithm, e.PolicyVersion)
}

// Is lets errors.Is(err, ErrPolicyBundleSignature) succeed.
func (e *PolicyBundleSignatureError) Is(target error) bool {
	return target == ErrPolicyBundleSignature
}

// VerifyPolicyBundle recomputes the HMAC-SHA256 (lowercase hex) of signed.Bundle
// with secret and constant-time compares it to signed.Signature. It returns nil
// on a match and a *PolicyBundleSignatureError (which errors.Is matches
// ErrPolicyBundleSignature) on mismatch. A nil bundle or empty secret is a usage
// error returned as-is.
func VerifyPolicyBundle(signed *authzv1.SignedPolicyBundle, secret []byte) error {
	if signed == nil {
		return errors.New("udb: VerifyPolicyBundle: nil bundle")
	}
	if len(secret) == 0 {
		return errors.New("udb: VerifyPolicyBundle: empty secret")
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(signed.GetBundle())
	want := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(want), []byte(signed.GetSignature())) {
		return &PolicyBundleSignatureError{
			KeyID:         signed.GetKeyId(),
			Algorithm:     signed.GetAlgorithm(),
			PolicyVersion: signed.GetPolicyVersion(),
		}
	}
	return nil
}
