package udbclient

import (
	"encoding/json"
	"os"
	"testing"
)

// Only actual live seeds load this file; manifest hydration unit fixtures remain
// explicit. Refuse a file from another reset, tenant, or project before copying
// any reference that will become a measured request's authority.
func loadPreparedBenchmarkFixtures(t testing.TB, fix *perfFixtures, tenant, project string) {
	t.Helper()
	path := os.Getenv("UDB_BENCH_FIXTURES")
	if path == "" {
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read prepared benchmark fixtures: %v", err)
	}
	var prepared struct {
		SchemaVersion int               `json:"schema_version"`
		TenantID      string            `json:"tenant_id"`
		ProjectID     string            `json:"project_id"`
		Fixtures      map[string]string `json:"fixtures"`
	}
	if err := json.Unmarshal(raw, &prepared); err != nil {
		t.Fatalf("decode prepared benchmark fixtures: %v", err)
	}
	if prepared.SchemaVersion != 1 || prepared.TenantID != tenant || prepared.ProjectID != project {
		t.Fatal("prepared benchmark fixtures do not match the verified tenant/project")
	}
	for _, key := range []string{"multipart_bucket", "multipart_object_key", "multipart_upload_id", "multipart_etag", "ack_workflow_id", "cdc_ack_event_id", "cdc_ack_topic"} {
		value := prepared.Fixtures[key]
		if value == "" {
			t.Fatalf("prepared benchmark fixtures missing %s", key)
		}
		fix.set(key, value)
	}
	// MessagingService MLS objects minted by `udb messaging bench-fixture`;
	// absent when the benchmark broker predates the messaging service.
	for _, key := range messagingFixtureKeys {
		if value := prepared.Fixtures[key]; value != "" {
			fix.set(key, value)
		}
	}
	// Per-tenant service-account membership fixtures; absent when the
	// benchmark broker predates tenant memberships (0.6.4).
	for _, key := range membershipFixtureKeys {
		if value := prepared.Fixtures[key]; value != "" {
			fix.set(key, value)
		}
	}
	// 0.6.6 identity-flow fixtures (tenant auth policy, passwordless sign-in,
	// invitations, step-up, OIDC); absent when the benchmark broker predates
	// them or (OIDC only) runs without UDB_OIDC_TEST_MODE.
	for _, key := range identityFixtureKeys {
		if value := prepared.Fixtures[key]; value != "" {
			fix.set(key, value)
		}
	}
}

// identityFixtureKeys are the 0.6.6 identity-flow bench-body fixtures
// scripts/prepare-benchmark-fixtures.py provisions (the same list in every
// harness): single-use codes, invitation accept tokens and a step-up token for
// disposable targets, never a password or an access/refresh token.
var identityFixtureKeys = []string{
	"login_otp_send_phone",
	"login_otp_id",
	"login_otp_code",
	"invitation_get_id",
	"invitation_get_token",
	"invitation_start_id",
	"invitation_start_token",
	"invitation_accept_id",
	"invitation_accept_token",
	"invitation_accept_otp_id",
	"invitation_accept_otp_code",
	"invitation_revoke_id",
	"invitation_resend_id",
	"step_up_token",
	"oidc_provider_id",
	"oidc_state",
}

// membershipFixtureKeys are the `membership_*` bench-body fixtures
// scripts/prepare-benchmark-fixtures.py provisions for the AuthnService
// tenant-membership RPCs (the same list in every harness). No credential is
// ever part of the prepared file: the exchange account's password is
// UDB_LIVE_PASSWORD.
var membershipFixtureKeys = []string{
	"membership_user_id",
	"membership_home_tenant_id",
	"membership_expires_at",
	"membership_id",
	"membership_exchange_username",
	"membership_exchange_project_id",
}

// messagingFixtureKeys are the `mls_*` bench-body fixtures
// scripts/prepare-benchmark-fixtures.py copies from `udb messaging
// bench-fixture` (the same list in every harness).
var messagingFixtureKeys = []string{
	"mls_subject_id",
	"mls_device_id",
	"mls_peer_device_id",
	"mls_revoke_device_id",
	"mls_register_device_id",
	"mls_register_signature_key",
	"mls_register_credential",
	"mls_key_package",
	"mls_group_id",
	"mls_message_ciphertext",
	"mls_commit_group_id",
	"mls_commit",
	"mls_welcome",
	"mls_new_group_id",
	"mls_new_group_info",
	"mls_new_ratchet_tree",
}
