package udbclient

import "fmt"

// TenantState makes the tenant-code → canonical-UUID transition explicit
// (critic.md §15). The human tenant code (e.g. "acme", "medpac") is only a
// pre-login hint; after login the broker resolves and verifies the canonical
// tenant UUID, and THAT is the value every tenant-scoped record/filter must use.
// Conflating the two is a subtle, high-risk bug (reads miss, writes stamp an
// unusable tenant, operators blame RLS) — this type keeps them separate and
// fails fast on mismatch.
type TenantState struct {
	// TenantCode is the human code supplied before login. Used only as a hint;
	// never written into tenant-scoped data.
	TenantCode string
	// CanonicalID is the verified canonical tenant UUID from the authenticated
	// principal. Empty until Adopt succeeds.
	CanonicalID string
	// IsVerified reports whether CanonicalID has been adopted post-login.
	IsVerified bool
}

// NewTenantState creates an unverified state from a human tenant code/hint.
func NewTenantState(code string) TenantState {
	return TenantState{TenantCode: code}
}

// Adopt records the canonical tenant UUID returned by the verified principal.
func (t *TenantState) Adopt(canonicalID string) error {
	if canonicalID == "" {
		return fmt.Errorf("udb: cannot adopt an empty canonical tenant_id")
	}
	t.CanonicalID = canonicalID
	t.IsVerified = true
	return nil
}

// GetVerified returns the canonical tenant UUID, or an error if login/adoption
// has not happened yet — so tenant-scoped requests fail fast instead of silently
// using the human code.
func (t TenantState) GetVerified() (string, error) {
	if !t.IsVerified || t.CanonicalID == "" {
		return "", fmt.Errorf(
			"udb: tenant not verified; call LoginAndAdoptTenant or ConnectEnterprise before tenant-scoped operations")
	}
	return t.CanonicalID, nil
}

// ValidateTenantID rejects a tenant-scoped record/filter whose tenant_id differs
// from the verified canonical tenant, naming both values (critic.md §15). An
// empty recordTenantID passes (the caller will fill it from GetVerified()).
func (t TenantState) ValidateTenantID(recordTenantID string) error {
	canonical, err := t.GetVerified()
	if err != nil {
		return err
	}
	if recordTenantID != "" && recordTenantID != canonical {
		return fmt.Errorf(
			"udb: tenant_id mismatch: record/filter has %q but the authenticated tenant is %q",
			recordTenantID, canonical)
	}
	return nil
}
