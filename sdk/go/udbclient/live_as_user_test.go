package udbclient

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	apikeyv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/services/v1"
	authnentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/entity/v1"
	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	commonpb "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Ordinary candidate TestLive discovery runs this proof on the same actual
// broker/catalog fixture as the existing data/native conformance suite. It
// changes no policy or broker configuration and creates only owned fixtures.
func TestLiveAsUserPreservesVerifiedAuthority(t *testing.T) {
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" {
		t.Skip("requires the actual live SDK broker fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	target := requiredLiveEnv(t, "UDB_GRPC_TARGET")
	authTarget := liveEnv("UDB_AUTH_GRPC_TARGET", target)
	operator, err := Connect(ctx, Config{
		Target: target, AuthTarget: authTarget,
		TenantID: requiredLiveEnv(t, "UDB_LIVE_TENANT"), ProjectID: requiredLiveEnv(t, "UDB_LIVE_PROJECT"),
		Purpose: "go.live.as-user", Deadline: 5 * time.Second, Retry: RetryConfig{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatalf("could not construct the ordinary tenant operator: code=%s", status.Code(err))
	}
	defer operator.Close()
	call := func(perform func(context.Context) error) error {
		callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
		defer callCancel()
		return perform(callCtx)
	}
	var adopted *AdoptedLogin
	err = call(func(callCtx context.Context) error {
		var err error
		adopted, err = operator.LoginAndAdoptTenant(callCtx, &authnv1.LoginRequest{
			Username: requiredLiveEnv(t, "UDB_LIVE_USERNAME"), Password: requiredLiveEnv(t, "UDB_LIVE_PASSWORD"),
			TenantHint: requiredLiveEnv(t, "UDB_LIVE_TENANT"), ProjectHint: requiredLiveEnv(t, "UDB_LIVE_PROJECT"),
			DeviceName: "go-live-as-user-operator",
		})
		return err
	})
	if err == nil && adopted != nil && adopted.Token.SessionID != "" {
		// Register immediately after login. This cleanup also runs if identity
		// assertions below fail, after every later owned-fixture cleanup.
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			_, err := operator.Auth.Authn.RevokeSession(cleanupCtx, &authnv1.RevokeSessionRequest{
				SessionId: adopted.Token.SessionID, PrincipalId: operator.Meta.UserID, RevokeReason: "go live AsUser operator cleanup",
				Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: operator.Meta.TenantID, ProjectId: operator.Meta.ProjectID}, UserId: operator.Meta.UserID, PrincipalId: operator.Meta.UserID, Purpose: operator.Meta.Purpose},
			})
			if err != nil {
				t.Errorf("AsUser fixture cleanup owned operator session failed: code=%s", status.Code(err))
			}
		}()
	}
	if err != nil || adopted == nil || adopted.Principal.GetAccountKind() != authnentpb.AccountKind_ACCOUNT_KIND_PERSON || adopted.Token.SessionID == "" {
		t.Fatalf("actual tenant operator login failed: code=%s", status.Code(err))
	}
	identity := operator.Meta
	if identity.TenantID == "" || identity.ProjectID == "" || identity.UserID == "" {
		t.Fatal("ordinary operator must adopt a verified tenant/project/user")
	}
	commonContext := func(user string, tenant string) *commonpb.RequestContext {
		return &commonpb.RequestContext{
			Tenant: &commonpb.TenantContext{TenantId: tenant, ProjectId: identity.ProjectID},
			UserId: user, PrincipalId: user, Purpose: identity.Purpose,
		}
	}
	serviceID, keyID, recordID, enterpriseSessionID := "", "", "", ""
	grantCreated := false
	var delegated, peer *authnv1.LoginResponse
	var peerPrincipal *authnv1.Principal
	// Cleanup remains on the genuine operator/peer authority and uses existing
	// channels. Revoke the operator's session LAST so owned fixture cleanup can
	// still authenticate on assertion failure. No token/error bodies are logged.
	defer func() {
		cleanup := func(label string, perform func(context.Context) error) {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanupCancel()
			if err := perform(cleanupCtx); err != nil {
				t.Errorf("AsUser fixture cleanup %s failed: code=%s", label, status.Code(err))
			}
		}
		if recordID != "" {
			cleanup("owned row", func(cleanupCtx context.Context) error {
				_, err := operator.Data.Delete(cleanupCtx, &entityv1.DeleteRequest{
					MessageType: liveMessageType, Filter: liveStruct(t, map[string]any{"record_id": recordID}),
				})
				return err
			})
		}
		if enterpriseSessionID != "" {
			cleanup("owned enterprise service session", func(cleanupCtx context.Context) error {
				_, err := operator.Auth.Authn.RevokeSession(cleanupCtx, &authnv1.RevokeSessionRequest{
					SessionId: enterpriseSessionID, PrincipalId: serviceID, RevokeReason: "go live AsUser cleanup",
					Context: commonContext(identity.UserID, identity.TenantID),
				})
				return err
			})
		}
		if keyID != "" {
			cleanup("owned key", func(cleanupCtx context.Context) error {
				_, err := operator.ApiKey.Raw.RevokeApiKey(cleanupCtx, &apikeyv1.RevokeApiKeyRequest{
					KeyId: keyID, RevokeReason: "go live AsUser cleanup", Context: commonContext(serviceID, identity.TenantID),
				})
				return err
			})
		}
		if grantCreated {
			cleanup("owned grant", func(cleanupCtx context.Context) error {
				_, err := operator.Auth.Authn.RevokeServiceAccountGrant(cleanupCtx, &authnv1.RevokeServiceAccountGrantRequest{
					TenantId: identity.TenantID, UserId: serviceID, Reason: "go live AsUser cleanup",
				})
				return err
			})
		}
		if serviceID != "" {
			cleanup("owned service account", func(cleanupCtx context.Context) error {
				_, err := operator.Auth.Authn.ChangeUserStatus(cleanupCtx, &authnv1.ChangeUserStatusRequest{
					UserId: serviceID, NewStatus: authnentpb.UserStatus_USER_STATUS_DEACTIVATED,
					Reason: "go live AsUser cleanup", Context: commonContext(identity.UserID, identity.TenantID),
				})
				return err
			})
		}
		if peer != nil && peerPrincipal != nil {
			cleanup("owned peer session", func(cleanupCtx context.Context) error {
				peerMeta := Metadata{TenantID: peerPrincipal.GetTenantId(), ProjectID: peerPrincipal.GetProjectId(), UserID: peerPrincipal.GetUserId(), Purpose: identity.Purpose}
				peerGen := NewGenerated(operator.authConn, Options{Meta: peerMeta, Authorization: "Bearer " + peer.GetAccessToken(), Retry: RetryConfig{MaxAttempts: 1}})
				_, err := operator.Auth.Authn.RevokeSession(peerGen.outgoingContext(cleanupCtx), &authnv1.RevokeSessionRequest{
					SessionId: peer.GetSessionId(), PrincipalId: peerPrincipal.GetUserId(), RevokeReason: "go live AsUser cleanup",
					Context: &commonpb.RequestContext{Tenant: &commonpb.TenantContext{TenantId: peerPrincipal.GetTenantId(), ProjectId: peerPrincipal.GetProjectId()}, UserId: peerPrincipal.GetUserId(), PrincipalId: peerPrincipal.GetUserId(), Purpose: identity.Purpose},
				})
				return err
			})
		}
		if session := delegated.GetSessionId(); session != "" {
			cleanup("owned person session", func(cleanupCtx context.Context) error {
				_, err := operator.Auth.Authn.RevokeSession(cleanupCtx, &authnv1.RevokeSessionRequest{
					SessionId: session, PrincipalId: identity.UserID, RevokeReason: "go live AsUser cleanup",
					Context: commonContext(identity.UserID, identity.TenantID),
				})
				return err
			})
		}
	}()
	name := "go-as-user-" + strings.ReplaceAll(uuid4(), "-", "")
	err = call(func(callCtx context.Context) error {
		created, err := operator.Auth.Authn.CreateUser(callCtx, &authnv1.CreateUserRequest{
			Username: name, Email: name + "@example.invalid", Password: "CorrectHorse1!",
			TenantId: identity.TenantID, ProjectId: identity.ProjectID, FullName: "Go AsUser owned service account",
			AccountKind: authnentpb.AccountKind_ACCOUNT_KIND_SERVICE_ACCOUNT,
		})
		if err == nil {
			serviceID = created.GetUser().GetUserId()
		}
		return err
	})
	if err != nil || serviceID == "" || serviceID == identity.UserID {
		t.Fatalf("owned service account must be distinct from the person: code=%s", status.Code(err))
	}
	err = call(func(callCtx context.Context) error {
		_, err := operator.Auth.Authn.ChangeUserStatus(callCtx, &authnv1.ChangeUserStatusRequest{
			UserId: serviceID, NewStatus: authnentpb.UserStatus_USER_STATUS_ACTIVE,
			Reason: "go live AsUser activation", Context: commonContext(identity.UserID, identity.TenantID),
		})
		return err
	})
	if err != nil {
		t.Fatalf("owned service activation failed: code=%s", status.Code(err))
	}
	err = call(func(callCtx context.Context) error {
		_, err := operator.Auth.Authn.CreateServiceAccountGrant(callCtx, &authnv1.CreateServiceAccountGrantRequest{
			TenantId: identity.TenantID, ProjectId: identity.ProjectID, UserId: serviceID,
			ServiceIdentity: name, ApprovedScopes: []string{"data:read"}, Reason: "go live AsUser narrow read fixture",
		})
		grantCreated = err == nil
		return err
	})
	if err != nil {
		t.Fatalf("owned narrow service grant failed: code=%s", status.Code(err))
	}
	var key *apikeyv1.CreateApiKeyResponse
	err = call(func(callCtx context.Context) error {
		var err error
		key, err = operator.ApiKey.Raw.CreateApiKey(callCtx, &apikeyv1.CreateApiKeyRequest{
			Name: name, OwnerId: serviceID, Scopes: []string{"data:read"}, Context: commonContext(serviceID, identity.TenantID),
		})
		if err == nil {
			keyID = key.GetKey().GetKeyId()
		}
		return err
	})
	if err != nil || keyID == "" || key.GetPlainKey() == "" {
		t.Fatalf("owned key fixture failed: code=%s", status.Code(err))
	}
	var service *Udb
	err = call(func(callCtx context.Context) error {
		var err error
		service, err = Connect(callCtx, Config{
			Target: target, AuthTarget: authTarget, TenantID: identity.TenantID, ProjectID: identity.ProjectID,
			Purpose: identity.Purpose, Credentials: Credentials{APIKey: key.GetPlainKey()},
			Deadline: 5 * time.Second, Retry: RetryConfig{MaxAttempts: 1},
		})
		return err
	})
	if err != nil {
		t.Fatalf("actual service-key Connect failed: code=%s", status.Code(err))
	}
	defer service.Close()
	// API-key authority is the approved service principal, not a person user.
	// The broker deliberately leaves UserId empty on this exchange path while
	// PrincipalId retains the durable account owner and Subject names the grant.
	principal := service.Principal()
	if principal == nil || principal.GetAccountKind() != authnentpb.AccountKind_ACCOUNT_KIND_SERVICE_ACCOUNT ||
		principal.GetPrincipalId() != serviceID || principal.GetServiceIdentity() != name || principal.GetSubject() != name || principal.GetUserId() != "" ||
		principal.GetTenantId() != identity.TenantID || principal.GetProjectId() != identity.ProjectID || !slices.Equal(principal.GetScopes(), []string{"data:read"}) {
		t.Fatal("service exchange must return exactly the approved canonical service principal")
	}
	if service.Meta.UserID != principal.GetUserId() || service.Meta.ServiceIdentity != principal.GetServiceIdentity() ||
		service.Meta.TenantID != principal.GetTenantId() || service.Meta.ProjectID != principal.GetProjectId() || !slices.Equal(service.Meta.Scopes, principal.GetScopes()) {
		t.Fatal("service Connect did not adopt exactly the approved verified principal")
	}
	beforeMeta, beforeData, beforeAuth := service.Meta, service.Data, service.Auth
	beforeMeta.Scopes = slices.Clone(service.Meta.Scopes)
	err = call(func(callCtx context.Context) error {
		var err error
		delegated, err = operator.Auth.Authn.Login(callCtx, &authnv1.LoginRequest{
			Username: requiredLiveEnv(t, "UDB_LIVE_USERNAME"), Password: requiredLiveEnv(t, "UDB_LIVE_PASSWORD"),
			TenantHint: identity.TenantID, ProjectHint: identity.ProjectID, DeviceName: "go-live-as-user-person",
		})
		return err
	})
	if err != nil || delegated.GetUserId() != identity.UserID || delegated.GetAccessToken() == "" || delegated.GetSessionId() == "" || delegated.GetSessionId() == adopted.Token.SessionID {
		t.Fatalf("separate actual person session failed: code=%s", status.Code(err))
	}
	// This native call requires a scope absent from the service's narrow grant.
	// Its success under delegation therefore cannot be explained by falling
	// back to the original service bearer or copying service header scopes.
	err = call(func(callCtx context.Context) error {
		_, err := service.Auth.ValidateSession(callCtx, delegated.GetAccessToken())
		return err
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ordinary service must lack native token-validation authority: code=%s", status.Code(err))
	}
	recordID = "as-user-" + uuid4()
	err = call(func(callCtx context.Context) error {
		written, err := operator.Data.Upsert(callCtx, &entityv1.UpsertRequest{
			MessageType: liveMessageType, RecordJson: liveRecordJSON(t, recordID, identity.TenantID, identity.ProjectID, recordID, "as-user-live", 1),
			ConflictFields: []string{"record_id"},
		})
		if err == nil && written.GetAffectedRows() != 1 {
			t.Fatal("owned AsUser record was not written")
		}
		return err
	})
	if err != nil {
		t.Fatalf("owned row fixture failed: code=%s", status.Code(err))
	}
	selectRequest := func() *entityv1.SelectRequest {
		return &entityv1.SelectRequest{
			Context:     &entityv1.RequestContext{TenantId: identity.TenantID, ProjectId: identity.ProjectID, Purpose: identity.Purpose},
			MessageType: liveMessageType, Limit: 1, Filter: liveStruct(t, map[string]any{"record_id": recordID}),
		}
	}
	assertRows := func(rows *entityv1.RecordSet) {
		if len(rows.GetRecordsJson()) != 1 {
			t.Fatal("delegated Select must return exactly its owned fixture row")
		}
		var row map[string]any
		if json.Unmarshal(rows.GetRecordsJson()[0], &row) != nil || row["record_id"] != recordID || row["tenant_id"] != identity.TenantID || row["project_id"] != identity.ProjectID {
			t.Fatal("delegated Select returned another row or scope")
		}
	}
	for _, inherited := range []bool{false, true} {
		err = call(func(callCtx context.Context) error {
			if inherited {
				callCtx = service.Generated.outgoingContext(service.Data.Context(callCtx))
			}
			rows, err := service.Data.Select(service.AsUser(callCtx, delegated.GetAccessToken()), selectRequest())
			if err == nil {
				assertRows(rows)
			}
			return err
		})
		if err != nil {
			t.Fatalf("actual delegated Data.Context read failed: code=%s", status.Code(err))
		}
		err = call(func(callCtx context.Context) error {
			if inherited {
				callCtx = service.Generated.outgoingContext(service.Auth.Context(callCtx))
			}
			verified, err := service.Auth.ValidateSession(service.AsUser(callCtx, delegated.GetAccessToken()), delegated.GetAccessToken())
			if err == nil && (verified.UserID != identity.UserID || verified.TenantID != identity.TenantID || verified.ProjectID != identity.ProjectID || verified.Principal.GetAccountKind() != authnentpb.AccountKind_ACCOUNT_KIND_PERSON) {
				t.Fatal("delegated native validation returned another principal")
			}
			return err
		})
		if err != nil {
			t.Fatalf("actual delegated Auth.Context call failed: code=%s", status.Code(err))
		}
		err = call(func(callCtx context.Context) error {
			if inherited {
				callCtx = service.Generated.outgoingContext(callCtx)
			}
			var rows entityv1.RecordSet
			err := service.Generated.InvokeUnary(service.AsUser(callCtx, "Bearer "+delegated.GetAccessToken()), "/udb.services.v1.DataBroker/Select", selectRequest(), &rows)
			if err == nil {
				assertRows(&rows)
			}
			return err
		})
		if err != nil {
			t.Fatalf("actual delegated generated call failed: code=%s", status.Code(err))
		}
	}
	// Verify the same explicit user bearer through the real password-connected
	// enterprise service session. Its own approved authority is still data:read;
	// delegation must survive the additional DataContext/NativeContext wrappers.
	var enterprise *EnterpriseSession
	err = call(func(callCtx context.Context) error {
		var err error
		enterprise, err = ConnectEnterprise(callCtx, EnterpriseConfig{
			Target: target, AuthTarget: authTarget, Username: name, Password: "CorrectHorse1!",
			TenantCode: identity.TenantID, ProjectID: identity.ProjectID, Purpose: identity.Purpose,
			Deadline: 5 * time.Second, Retry: RetryConfig{MaxAttempts: 1},
		})
		return err
	})
	if err != nil {
		t.Fatalf("actual enterprise service Connect failed: code=%s", status.Code(err))
	}
	defer enterprise.Close()
	enterpriseToken, err := enterprise.tm.store.Load(ctx)
	if err != nil || enterpriseToken.SessionID == "" {
		t.Fatal("actual enterprise service login must own a session")
	}
	enterpriseSessionID = enterpriseToken.SessionID
	if enterprise.Principal.GetAccountKind() != authnentpb.AccountKind_ACCOUNT_KIND_SERVICE_ACCOUNT || enterprise.Meta.UserID != serviceID || !slices.Equal(enterprise.Meta.Scopes, []string{"data:read"}) {
		t.Fatal("enterprise service must retain the actual narrow grant")
	}
	beforeEnterpriseMeta, beforeEnterpriseData, beforeEnterpriseAuth := enterprise.Meta, enterprise.Data, enterprise.Auth
	beforeEnterpriseMeta.Scopes = slices.Clone(enterprise.Meta.Scopes)
	err = call(func(callCtx context.Context) error {
		rows, err := enterprise.Data.Broker.Select(enterprise.DataContext(enterprise.AsUser(callCtx, delegated.GetAccessToken())), selectRequest())
		if err == nil {
			assertRows(rows)
		}
		return err
	})
	if err != nil {
		t.Fatalf("actual enterprise DataContext delegation failed: code=%s", status.Code(err))
	}
	err = call(func(callCtx context.Context) error {
		verified, err := enterprise.Auth.Authn.ValidateToken(enterprise.NativeContext(enterprise.AsUser(callCtx, delegated.GetAccessToken())), &authnv1.ValidateTokenRequest{Token: delegated.GetAccessToken(), TokenType: authnentpb.TokenType_TOKEN_TYPE_JWT_ACCESS})
		if err == nil && (!verified.GetValid() || verified.GetUserId() != identity.UserID || verified.GetTenantId() != identity.TenantID || verified.GetProjectId() != identity.ProjectID) {
			t.Fatal("enterprise native delegation returned another verified principal")
		}
		return err
	})
	if err != nil {
		t.Fatalf("actual enterprise NativeContext delegation failed: code=%s", status.Code(err))
	}
	err = call(func(callCtx context.Context) error {
		var rows entityv1.RecordSet
		err := enterprise.Generated.InvokeUnary(enterprise.DataContext(enterprise.AsUser(callCtx, delegated.GetAccessToken())), "/udb.services.v1.DataBroker/Select", selectRequest(), &rows)
		if err == nil {
			assertRows(&rows)
		}
		return err
	})
	if err != nil || !reflect.DeepEqual(beforeEnterpriseMeta, enterprise.Meta) || beforeEnterpriseData != enterprise.Data || beforeEnterpriseAuth != enterprise.Auth {
		t.Fatalf("actual enterprise delegation changed service state or generated call failed: code=%s", status.Code(err))
	}
	err = call(func(callCtx context.Context) error {
		var err error
		peer, err = operator.Auth.Authn.Login(callCtx, &authnv1.LoginRequest{
			Username: requiredLiveEnv(t, "UDB_LIVE_PEER_USERNAME"), Password: requiredLiveEnv(t, "UDB_LIVE_PEER_PASSWORD"),
			TenantHint: requiredLiveEnv(t, "UDB_LIVE_PEER_TENANT"), ProjectHint: requiredLiveEnv(t, "UDB_LIVE_PROJECT"), DeviceName: "go-live-as-user-peer",
		})
		return err
	})
	if err != nil || peer.GetAccessToken() == "" || peer.GetSessionId() == "" {
		t.Fatalf("actual peer login failed: code=%s", status.Code(err))
	}
	err = call(func(callCtx context.Context) error {
		verified, err := operator.Auth.Authn.Authenticate(callCtx, &authnv1.AuthnRequest{BearerToken: peer.GetAccessToken()})
		if err == nil {
			peerPrincipal = verified.GetPrincipal()
		}
		return err
	})
	if err != nil || peerPrincipal.GetTenantId() == "" || peerPrincipal.GetTenantId() == identity.TenantID || peerPrincipal.GetUserId() != peer.GetUserId() {
		t.Fatalf("actual peer must verify as a different tenant: code=%s", status.Code(err))
	}
	err = call(func(callCtx context.Context) error {
		_, err := service.Data.Select(service.AsUser(callCtx, peer.GetAccessToken()), selectRequest())
		return err
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("AsUser must retain connected tenant/project guard: code=%s", status.Code(err))
	}
	err = call(func(callCtx context.Context) error {
		_, err := service.Auth.ValidateSession(service.AsUser(callCtx, peer.GetAccessToken()), peer.GetAccessToken())
		return err
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("native delegation must retain the tenant guard: code=%s", status.Code(err))
	}
	for _, invalid := range []string{"", "not-a-valid-udb-token"} {
		err = call(func(callCtx context.Context) error {
			_, err := service.Data.Select(service.AsUser(callCtx, invalid), selectRequest())
			return err
		})
		if code := status.Code(err); code != codes.Unauthenticated && code != codes.PermissionDenied {
			t.Fatalf("invalid user bearer must fail closed: code=%s", code)
		}
	}
	err = call(func(callCtx context.Context) error {
		revoked, err := operator.Auth.Authn.RevokeSession(callCtx, &authnv1.RevokeSessionRequest{
			SessionId: delegated.GetSessionId(), PrincipalId: identity.UserID, RevokeReason: "go live AsUser revocation proof",
			Context: commonContext(identity.UserID, identity.TenantID),
		})
		if err == nil && revoked.GetRevokedCount() != 1 {
			t.Fatal("actual delegation session must be revoked exactly once")
		}
		return err
	})
	if err != nil {
		t.Fatalf("actual person session revoke failed: code=%s", status.Code(err))
	}
	err = call(func(callCtx context.Context) error {
		_, err := service.Data.Select(service.AsUser(callCtx, delegated.GetAccessToken()), selectRequest())
		return err
	})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("revoked actual user session must not regain service authority: code=%s", status.Code(err))
	}
	err = call(func(callCtx context.Context) error {
		caps, err := service.Data.Broker.GetCapabilities(callCtx, &entityv1.CapabilitiesRequest{})
		if err == nil && len(caps.GetEnabledBackends()) == 0 {
			t.Fatal("ordinary service capability call returned no configured backend")
		}
		return err
	})
	if err != nil || !reflect.DeepEqual(beforeMeta, service.Meta) || service.Data != beforeData || service.Auth != beforeAuth {
		t.Fatalf("delegation damaged ordinary service identity/calls: code=%s", status.Code(err))
	}
	t.Log("verified service-to-person delegation on data/native/generated paths; tenant, invalid and revoked-token refusals retained")
}
