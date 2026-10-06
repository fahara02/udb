package udbclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/health/grpc_health_v1"
)

// The one environment contract a service connects with. Every service used to
// read its own variable names, validate them by hand, and re-check the
// principal after connecting; ConnectFromEnv does all of it.
//
// With prefix "UDB_" (the default when prefix is empty):
//
//	UDB_TARGET            broker gRPC target, host:port               (required)
//	UDB_AUTH_TARGET       control-plane target; defaults to UDB_TARGET
//	UDB_WEBRTC_TARGET     WebRTC signalling target; defaults to the auth target
//	UDB_TENANT_ID         the service's tenant UUID                    (required)
//	UDB_PROJECT_ID        project (UDB_PROJECT is accepted too)
//	UDB_PURPOSE           purpose sent on every call                   (required)
//	UDB_API_KEY           service-account key, exchanged for a bearer  (required unless UDB_BEARER)
//	UDB_BEARER            a ready bearer token instead of an API key
//	UDB_SCOPES            requested scopes, comma or space separated
//	UDB_SERVICE_IDENTITY  the service identity the grant is bound to
//	UDB_DEADLINE          per-call deadline when the caller sets none (Go duration, e.g. 10s)
//	UDB_TLS               "true" to dial with TLS (system roots)
//	UDB_TLS_SERVER_NAME   TLS server name override

// ConfigFromEnv reads the environment contract above. Every missing or invalid
// variable is reported in one error, so a misconfigured deployment is fixed in
// one pass instead of one restart per variable.
func ConfigFromEnv(prefix string) (Config, error) {
	if prefix == "" {
		prefix = "UDB_"
	}
	get := func(name string) string { return strings.TrimSpace(os.Getenv(prefix + name)) }
	var problems []string
	require := func(name string) string {
		value := get(name)
		if value == "" {
			problems = append(problems, prefix+name+" is not set")
		}
		return value
	}

	cfg := Config{
		Target:          require("TARGET"),
		AuthTarget:      get("AUTH_TARGET"),
		WebRTCTarget:    get("WEBRTC_TARGET"),
		TenantID:        require("TENANT_ID"),
		ProjectID:       get("PROJECT_ID"),
		Purpose:         require("PURPOSE"),
		ServiceIdentity: get("SERVICE_IDENTITY"),
		Scopes:          splitEnvScopes(get("SCOPES")),
		Credentials: Credentials{
			APIKey: get("API_KEY"),
			Bearer: get("BEARER"),
		},
	}
	if cfg.ProjectID == "" {
		cfg.ProjectID = get("PROJECT")
	}
	if cfg.Credentials.APIKey == "" && cfg.Credentials.Bearer == "" {
		problems = append(problems, prefix+"API_KEY (or "+prefix+"BEARER) is not set")
	}
	for name, target := range map[string]string{
		"TARGET": cfg.Target, "AUTH_TARGET": cfg.AuthTarget, "WEBRTC_TARGET": cfg.WebRTCTarget,
	} {
		if target != "" && !strings.Contains(target, ":") {
			problems = append(problems, fmt.Sprintf("%s%s must be host:port, got %q", prefix, name, target))
		}
	}
	if raw := get("DEADLINE"); raw != "" {
		deadline, err := time.ParseDuration(raw)
		if err != nil || deadline <= 0 {
			problems = append(problems, fmt.Sprintf("%sDEADLINE must be a positive duration such as 10s, got %q", prefix, raw))
		} else {
			cfg.Deadline = deadline
		}
	}
	if strings.EqualFold(get("TLS"), "true") || get("TLS") == "1" {
		cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: get("TLS_SERVER_NAME")}
	}
	if len(problems) > 0 {
		return Config{}, fmt.Errorf("udb: environment is incomplete: %s", strings.Join(problems, "; "))
	}
	return cfg, nil
}

func splitEnvScopes(raw string) []string {
	var out []string
	seen := map[string]bool{}
	for _, scope := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ' ' || r == '\t' }) {
		if !seen[scope] {
			seen[scope] = true
			out = append(out, scope)
		}
	}
	return out
}

// Expect is what a service asserts about the principal it connected as. Every
// field is optional; set the ones the service relies on.
type Expect struct {
	TenantID        string
	ProjectID       string
	ServiceIdentity string
	// RequiredScopes must all be granted; a scope the service needs but the
	// grant lacks fails here instead of on the first call that needs it.
	RequiredScopes []string
}

// ConnectFromEnv reads ConfigFromEnv(prefix), connects, and checks the
// verified principal against expect. The returned client carries an exchanged,
// self-refreshing bearer when an API key was configured.
func ConnectFromEnv(ctx context.Context, prefix string, expect Expect) (*Udb, error) {
	cfg, err := ConfigFromEnv(prefix)
	if err != nil {
		return nil, err
	}
	u, err := Connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := u.Verify(expect); err != nil {
		_ = u.Close()
		return nil, err
	}
	return u, nil
}

// Verify checks the connected principal against expect and reports every
// mismatch at once. It needs a verified principal, which a connection that
// exchanged an API key has; a connection made with a raw bearer has none and
// fails here if anything is expected.
func (u *Udb) Verify(expect Expect) error {
	if expect.TenantID == "" && expect.ProjectID == "" && expect.ServiceIdentity == "" && len(expect.RequiredScopes) == 0 {
		return nil
	}
	principal := u.Principal()
	if principal == nil {
		return fmt.Errorf("udb: cannot verify the connection: it has no verified principal (connect with an API key, or use ConnectEnterprise)")
	}
	var problems []string
	if expect.TenantID != "" && principal.GetTenantId() != expect.TenantID {
		problems = append(problems, fmt.Sprintf("tenant is %q, expected %q", principal.GetTenantId(), expect.TenantID))
	}
	if expect.ProjectID != "" && principal.GetProjectId() != expect.ProjectID {
		problems = append(problems, fmt.Sprintf("project is %q, expected %q", principal.GetProjectId(), expect.ProjectID))
	}
	if expect.ServiceIdentity != "" && principal.GetServiceIdentity() != expect.ServiceIdentity {
		problems = append(problems, fmt.Sprintf("service identity is %q, expected %q", principal.GetServiceIdentity(), expect.ServiceIdentity))
	}
	granted := map[string]bool{}
	for _, scope := range principal.GetScopes() {
		granted[strings.ToLower(scope)] = true
	}
	var missing []string
	for _, scope := range expect.RequiredScopes {
		if !granted[strings.ToLower(scope)] {
			missing = append(missing, scope)
		}
	}
	if len(missing) > 0 {
		problems = append(problems, "the grant lacks scopes "+strings.Join(missing, ", ")+" (add them to the service account's grant)")
	}
	if len(problems) > 0 {
		return fmt.Errorf("udb: connected principal does not match: %s", strings.Join(problems, "; "))
	}
	return nil
}

// Ready reports whether the broker is serving and this connection's credential
// is current: the standard gRPC health check on the broker connection plus
// CredentialErr. Use it as the service's readiness probe; it needs no admin scope.
func (u *Udb) Ready(ctx context.Context) error {
	if err := u.CredentialErr(); err != nil {
		return fmt.Errorf("udb: credential refresh is failing: %w", err)
	}
	res, err := grpc_health_v1.NewHealthClient(u.brokerConn).Check(ctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("udb: broker health check failed: %w", err)
	}
	if res.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("udb: broker is %s", res.GetStatus())
	}
	return nil
}
