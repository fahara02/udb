package udbclient

// Explicit per-RPC request bodies for the perf harness, transcribed from
// BENCH_RPC_BODIES.md. There is NO generic fill: every measured unary RPC is
// driven by an explicit field-spec built here (real fields, valid enum values,
// seeded reference IDs), so a request never carries placeholder garbage that the
// broker rejects with INVALID_ARGUMENT. An RPC with no spec (and no perfRealBody
// override) is reported as a NO-BODY failure for the maintainer to add — it is
// NEVER generic-probed.
//
// Filled service-by-service following the auth route (Phase 1 auth → all RPCs →
// Phase 3 terminal auth). Each batch turns its RPCs from NO-BODY into a measured
// success path.

import (
	"errors"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// errNoExplicitBody is returned for a unary RPC that has neither a spec entry nor a
// perfRealBody override. The generic fill is NUKED: rather than send a placeholder
// request the broker rejects with INVALID_ARGUMENT, the harness surfaces this as a
// clear NO-BODY failure so the missing body gets added to BENCH_RPC_BODIES.md.
var errNoExplicitBody = errors.New("NO-BODY")

// Auth route (BENCH_RPC_BODIES.md): Phase 1 establishes the session FIRST, Phase 3
// tears it down LAST; everything else is Phase 2. orderRPCsByAuthPhase returns the
// measurement order Phase1→Phase2→Phase3 so terminal auth never kills the run.
var phase1AuthnOrder = []string{
	"Login", "RefreshToken", "RefreshSession", "Authenticate", "ValidateToken", "IntrospectToken", "GetJwks",
}

var phase3Authn = map[string]bool{
	"Logout": true, "RevokeSession": true, "AdminRevokeSession": true,
	"AdminRevokeAllUserSessions": true, "AdminRevokeAllTenantSessions": true, "EmergencyRevoke": true,
	"ChangePassword": true, "ResetPassword": true, "AdminResetPassword": true, "ChangeUserStatus": true,
	"AdminResetMfa": true, "RevokeRecoveryCodes": true, "RevokeDevice": true,
	"DeleteWebAuthnCredential": true, "DisableMfaFactor": true,
}

func orderRPCsByAuthPhase(all []RPCInfo) []RPCInfo {
	p1idx := map[string]int{}
	for i, n := range phase1AuthnOrder {
		p1idx[n] = i
	}
	var p1, p2, p3 []RPCInfo
	for _, r := range all {
		if r.Service == "AuthnService" {
			if _, ok := p1idx[r.Name]; ok {
				p1 = append(p1, r)
				continue
			}
			if phase3Authn[r.Name] {
				p3 = append(p3, r)
				continue
			}
		}
		p2 = append(p2, r)
	}
	sort.Slice(p1, func(i, j int) bool { return p1idx[p1[i].Name] < p1idx[p1[j].Name] })
	out := make([]RPCInfo, 0, len(all))
	out = append(out, p1...)
	out = append(out, p2...)
	out = append(out, p3...)
	return out
}

// bodyField is one explicit field assignment; bodyVal carries its value form.
type bodyField struct {
	name string // proto field name (snake_case)
	val  bodyVal
}

type bvKind int

const (
	bvStr    bvKind = iota // literal string
	bvInt                  // literal int (int32/int64/uint…)
	bvBool                 // literal bool
	bvF64                  // literal double/float
	bvEnum                 // enum value by NAME
	bvSeed                 // seeded fixture value (by an explicit key, or the field name)
	bvMsg                  // nested message
	bvList                 // repeated field (scalar / enum / message elements)
	bvStruct               // google.protobuf.Struct built from scalar/seed sub-fields
)

type bodyVal struct {
	kind bvKind
	s    string      // string literal / enum name / seed key
	i    int64       // int literal
	b    bool        // bool literal
	f    float64     // double literal
	sub  []bodyField // nested message fields
	list []bodyVal   // repeated elements
}

func litS(v string) bodyVal                { return bodyVal{kind: bvStr, s: v} }
func litI(v int64) bodyVal                 { return bodyVal{kind: bvInt, i: v} }
func litB(v bool) bodyVal                  { return bodyVal{kind: bvBool, b: v} }
func litF(v float64) bodyVal               { return bodyVal{kind: bvF64, f: v} }
func enumV(name string) bodyVal            { return bodyVal{kind: bvEnum, s: name} }
func seed(key string) bodyVal              { return bodyVal{kind: bvSeed, s: key} }
func sub(fs ...bodyField) bodyVal          { return bodyVal{kind: bvMsg, sub: fs} }
func list(vs ...bodyVal) bodyVal           { return bodyVal{kind: bvList, list: vs} }
func structV(fs ...bodyField) bodyVal      { return bodyVal{kind: bvStruct, sub: fs} }
func fld(name string, v bodyVal) bodyField { return bodyField{name: name, val: v} }

// dctx builds the DataBroker `context` (udb.entity.v1.RequestContext — FLAT
// tenant_id/project_id/scopes, not the nested TenantContext the control-plane
// services use). scopes carry the method's required scope.
func dctx(scopes ...string) bodyField {
	sc := make([]bodyVal, 0, len(scopes))
	for _, s := range scopes {
		sc = append(sc, litS(s))
	}
	return fld("context", sub(
		fld("tenant_id", seed("tenant_id")),
		fld("project_id", seed("project")),
		fld("scopes", list(sc...)),
		fld("purpose", litS("go.live.perf")),
	))
}

// buildSpecBody builds the dynamicpb request for fullMethod from its explicit spec.
// Returns ok=false when there is no spec (caller must NOT fall back to a generic
// request — it records a NO-BODY failure instead).
func buildSpecBody(fullMethod string, fix *perfFixtures) (proto.Message, proto.Message, bool) {
	// Keys are stored without the leading "/" that RPCInfo.FullMethod carries.
	spec, ok := perfBodySpecs[strings.TrimPrefix(fullMethod, "/")]
	if !ok {
		return nil, nil, false
	}
	md := resolveMethodDesc(fullMethod)
	if md == nil {
		return nil, nil, false
	}
	in := dynamicpb.NewMessage(md.Input())
	applyBodyFields(in, spec, fix)
	return in, dynamicpb.NewMessage(md.Output()), true
}

// applyBodyFields sets each spec'd field on the message, matching the field's proto
// kind. Unknown field names are skipped (defensive — the spec is grounded in the
// proto, so this should not happen). Seed values resolve via the fixture map: an
// explicit key if given, else the field name.
func applyBodyFields(m protoreflect.Message, fields []bodyField, fix *perfFixtures) {
	fds := m.Descriptor().Fields()
	for _, bf := range fields {
		fd := fds.ByName(protoreflect.Name(bf.name))
		if fd == nil {
			continue
		}
		setBodyField(m, fd, bf.val, fix)
	}
}

func setBodyField(m protoreflect.Message, fd protoreflect.FieldDescriptor, bv bodyVal, fix *perfFixtures) {
	switch bv.kind {
	case bvSeed:
		key := bv.s
		if key == "" {
			key = string(fd.Name())
		}
		v, ok := fix.lookup(key)
		if !ok {
			v, _ = fix.lookup(string(fd.Name()))
		}
		if v != "" && fd.Kind() == protoreflect.StringKind {
			m.Set(fd, protoreflect.ValueOfString(v))
		}
	case bvStr:
		if fd.Kind() == protoreflect.StringKind {
			m.Set(fd, protoreflect.ValueOfString(bv.s))
		} else if fd.Kind() == protoreflect.BytesKind {
			m.Set(fd, protoreflect.ValueOfBytes([]byte(bv.s)))
		}
	case bvBool:
		m.Set(fd, protoreflect.ValueOfBool(bv.b))
	case bvF64:
		switch fd.Kind() {
		case protoreflect.DoubleKind:
			m.Set(fd, protoreflect.ValueOfFloat64(bv.f))
		case protoreflect.FloatKind:
			m.Set(fd, protoreflect.ValueOfFloat32(float32(bv.f)))
		}
	case bvInt:
		switch fd.Kind() {
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
			m.Set(fd, protoreflect.ValueOfInt32(int32(bv.i)))
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
			m.Set(fd, protoreflect.ValueOfUint32(uint32(bv.i)))
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			m.Set(fd, protoreflect.ValueOfInt64(bv.i))
		case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			m.Set(fd, protoreflect.ValueOfUint64(uint64(bv.i)))
		}
	case bvEnum:
		if fd.Kind() == protoreflect.EnumKind {
			if ev := fd.Enum().Values().ByName(protoreflect.Name(bv.s)); ev != nil {
				m.Set(fd, protoreflect.ValueOfEnum(ev.Number()))
			}
		}
	case bvMsg:
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			applyBodyFields(m.Mutable(fd).Message(), bv.sub, fix)
		}
	case bvList:
		if !fd.IsList() {
			return
		}
		l := m.Mutable(fd).List()
		for _, ev := range bv.list {
			switch fd.Kind() {
			case protoreflect.StringKind:
				s := ev.s
				if ev.kind == bvSeed {
					if v, ok := fix.lookup(ev.s); ok {
						s = v
					}
				}
				l.Append(protoreflect.ValueOfString(s))
			case protoreflect.EnumKind:
				if e := fd.Enum().Values().ByName(protoreflect.Name(ev.s)); e != nil {
					l.Append(protoreflect.ValueOfEnum(e.Number()))
				}
			case protoreflect.FloatKind:
				l.Append(protoreflect.ValueOfFloat32(float32(ev.f)))
			case protoreflect.DoubleKind:
				l.Append(protoreflect.ValueOfFloat64(ev.f))
			case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
				l.Append(protoreflect.ValueOfInt32(int32(ev.i)))
			case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
				l.Append(protoreflect.ValueOfInt64(ev.i))
			case protoreflect.BoolKind:
				l.Append(protoreflect.ValueOfBool(ev.b))
			case protoreflect.MessageKind, protoreflect.GroupKind:
				elem := l.NewElement()
				applyBodyFields(elem.Message(), ev.sub, fix)
				l.Append(elem)
			}
		}
	case bvStruct:
		if fd.Kind() != protoreflect.MessageKind {
			return
		}
		mp := map[string]any{}
		for _, f := range bv.sub {
			switch f.val.kind {
			case bvSeed:
				if v, ok := fix.lookup(f.val.s); ok {
					mp[f.name] = v
				}
			case bvStr:
				mp[f.name] = f.val.s
			case bvInt:
				mp[f.name] = float64(f.val.i)
			case bvF64:
				mp[f.name] = f.val.f
			case bvBool:
				mp[f.name] = f.val.b
			}
		}
		if st, err := structpb.NewStruct(mp); err == nil {
			m.Set(fd, protoreflect.ValueOfMessage(st.ProtoReflect()))
		}
	}
}

// ctxField builds a common request `context` sub-message with the seeded tenant.
// Many control-plane requests carry `context { tenant { tenant_id, project_id } }`.
func ctxField() bodyField {
	return fld("context", sub(
		fld("tenant", sub(
			fld("tenant_id", seed("tenant_id")),
			fld("project_id", seed("project")),
		)),
		fld("purpose", litS("go.live.perf")),
	))
}

// actorF builds an authz governance `actor` (GovernanceActor) carrying the scope
// the RPC re-checks under native.authz.governance — without it the call is
// PERMISSION_DENIED even with an admin bearer.
func actorF(scope string) bodyField {
	return fld("actor", sub(
		fld("subject", seed("subject")),
		fld("tenant_id", seed("tenant_id")),
		fld("project_id", seed("project")),
		fld("scopes", list(litS(scope))),
	))
}

// perfBodySpecs maps "Service/Method" (RPCInfo.FullMethod form) → explicit body.
// Transcribed row-for-row from BENCH_RPC_BODIES.md. DataBroker stays in
// perfRealBody (typed, backend-specific); this covers the control-plane services.
var perfBodySpecs = map[string][]bodyField{
	// ── AuthnService — Phase 1 (session setup) ──────────────────────────────
	"udb.core.authn.services.v1.AuthnService/Login": {
		fld("username", seed("username")), fld("password", litS("CorrectHorse1!")),
		fld("device_type", enumV("DEVICE_TYPE_API")), fld("device_name", litS("go-perf")),
		fld("tenant_hint", seed("tenant_id")), fld("project_hint", seed("project")),
	},
	"udb.core.authn.services.v1.AuthnService/RefreshToken": {
		fld("refresh_token", seed("refresh_token")),
	},
	"udb.core.authn.services.v1.AuthnService/RefreshSession": {
		fld("session_id", seed("session_id")), fld("ttl_seconds", litI(3600)),
	},
	"udb.core.authn.services.v1.AuthnService/Authenticate": {
		fld("bearer_token", seed("token")),
		fld("credential_type", enumV("AUTH_CREDENTIAL_TYPE_BEARER_TOKEN")),
	},
	"udb.core.authn.services.v1.AuthnService/ValidateToken": {
		fld("token", seed("token")), fld("token_type", enumV("TOKEN_TYPE_JWT_ACCESS")),
	},
	"udb.core.authn.services.v1.AuthnService/IntrospectToken": {
		fld("token", seed("token")),
	},
	"udb.core.authn.services.v1.AuthnService/GetJwks": {},

	// ── AuthnService — Phase 2 (neutral: user/session/mfa/device reads+writes on the seeded user) ──
	"udb.core.authn.services.v1.AuthnService/CreateUser": {
		fld("username", litS("perf-u")), fld("email", litS("perf-u@acme.test")),
		fld("password", litS("Str0ng!Passw0rd")), fld("tenant_id", seed("tenant_id")),
		fld("full_name", litS("Perf U")), fld("account_kind", enumV("ACCOUNT_KIND_PERSON")),
	},
	"udb.core.authn.services.v1.AuthnService/GetUser":   {fld("user_id", seed("user_id"))},
	"udb.core.authn.services.v1.AuthnService/ListUsers": {fld("tenant_id", seed("tenant_id"))},
	"udb.core.authn.services.v1.AuthnService/UpdateUser": {
		fld("user_id", seed("user_id")), fld("full_name", litS("Perf U2")),
		fld("email", litS("perf-u2@acme.test")), fld("tenant_id", seed("tenant_id")),
	},
	"udb.core.authn.services.v1.AuthnService/SendOTP": {
		fld("user_id", seed("user_id")), fld("otp_type", enumV("OTP_TYPE_EMAIL_VERIFICATION")),
	},
	"udb.core.authn.services.v1.AuthnService/VerifyOTP": {
		fld("otp_id", seed("code")), fld("code", litS("123456")),
	},
	"udb.core.authn.services.v1.AuthnService/ResendOTP": {
		fld("original_otp_id", seed("code")), fld("reason", litS("not_received")),
	},
	"udb.core.authn.services.v1.AuthnService/CreateSession": {
		fld("principal", sub(
			fld("principal_id", seed("user_id")), fld("subject", seed("subject")),
			fld("user_id", seed("user_id")), fld("tenant_id", seed("tenant_id")),
		)), fld("ttl_seconds", litI(3600)),
	},
	"udb.core.authn.services.v1.AuthnService/GetSession":   {fld("session_id", seed("session_id"))},
	"udb.core.authn.services.v1.AuthnService/ListSessions": {fld("user_id", seed("user_id"))},
	"udb.core.authn.services.v1.AuthnService/ValidateCSRF": {
		fld("session_id", seed("session_id")), fld("csrf_token", seed("csrf_token")),
	},
	"udb.core.authn.services.v1.AuthnService/EnrollMFA": {
		fld("user_id", seed("user_id")), fld("mfa_type", enumV("AUTH_FACTOR_KIND_TOTP")),
	},
	"udb.core.authn.services.v1.AuthnService/ConfirmMFAEnrollment": {
		fld("user_id", seed("user_id")), fld("otp_id", seed("code")), fld("code", litS("123456")),
	},
	"udb.core.authn.services.v1.AuthnService/GenerateRecoveryCodes": {
		fld("user_id", seed("user_id")), fld("count", litI(10)),
	},
	"udb.core.authn.services.v1.AuthnService/PutMfaPolicy": {
		fld("tenant_id", seed("tenant_id")), fld("require_mfa", litB(true)),
	},
	"udb.core.authn.services.v1.AuthnService/GetMfaPolicy":   {fld("tenant_id", seed("tenant_id"))},
	"udb.core.authn.services.v1.AuthnService/ForgotPassword": {fld("identifier", litS("perf-u@acme.test"))},
	"udb.core.authn.services.v1.AuthnService/SendPhoneVerification": {
		fld("user_id", seed("user_id")), fld("phone", litS("+15551234567")),
	},
	"udb.core.authn.services.v1.AuthnService/StartWebAuthnRegistration": {
		fld("user_id", seed("user_id")), fld("label", litS("perf-key")), fld("tenant_id", seed("tenant_id")),
	},
	"udb.core.authn.services.v1.AuthnService/StartWebAuthnAuthentication": {
		fld("user_id", seed("user_id")), fld("tenant_id", seed("tenant_id")),
	},
	"udb.core.authn.services.v1.AuthnService/ListDevices": {fld("user_id", seed("user_id"))},
	"udb.core.authn.services.v1.AuthnService/IssueMfaChallenge": {
		fld("user_id", seed("user_id")), fld("factor_kind", enumV("AUTH_FACTOR_KIND_TOTP")),
		fld("purpose", enumV("MFA_CHALLENGE_PURPOSE_SENSITIVE_OPERATION")),
	},
	"udb.core.authn.services.v1.AuthnService/VerifyMfaChallenge": {
		fld("challenge_id", seed("code")), fld("code", litS("123456")),
	},
	"udb.core.authn.services.v1.AuthnService/ListMfaFactors":          {fld("user_id", seed("user_id"))},
	"udb.core.authn.services.v1.AuthnService/ListWebAuthnCredentials": {fld("user_id", seed("user_id"))},

	// ── AuthnService — Phase 3 (terminal: target the SEEDED disposable user/session) ──
	"udb.core.authn.services.v1.AuthnService/Logout":        {fld("session_id", seed("session_id"))},
	"udb.core.authn.services.v1.AuthnService/RevokeSession": {fld("session_id", seed("session_id")), fld("revoke_reason", litS("perf"))},
	"udb.core.authn.services.v1.AuthnService/ChangeUserStatus": {
		fld("user_id", seed("user_id")), fld("new_status", enumV("USER_STATUS_SUSPENDED")), fld("reason", litS("perf")),
	},
	"udb.core.authn.services.v1.AuthnService/AdminResetPassword": {fld("user_id", seed("user_id"))},
	"udb.core.authn.services.v1.AuthnService/ChangePassword": {
		fld("user_id", seed("user_id")), fld("current_password", litS("Str0ng!Passw0rd")),
		fld("new_password", litS("N3w!Passw0rd9")), fld("otp_id", seed("code")),
	},
	"udb.core.authn.services.v1.AuthnService/ResetPassword": {
		fld("otp_id", seed("code")), fld("code", litS("123456")), fld("new_password", litS("N3w!Passw0rd9")),
	},
	"udb.core.authn.services.v1.AuthnService/EnableMfa": {fld("user_id", seed("user_id"))},
	"udb.core.authn.services.v1.AuthnService/DisableMfaFactor": {
		fld("user_id", seed("user_id")), fld("factor_kind", enumV("AUTH_FACTOR_KIND_TOTP")),
	},
	"udb.core.authn.services.v1.AuthnService/RenamePasskey": {
		fld("user_id", seed("user_id")), fld("credential_id", seed("record_id")), fld("new_label", litS("perf-key2")),
	},
	"udb.core.authn.services.v1.AuthnService/RevokeRecoveryCodes": {fld("user_id", seed("user_id"))},
	"udb.core.authn.services.v1.AuthnService/AdminResetMfa":       {fld("user_id", seed("user_id")), fld("reason", litS("perf"))},
	"udb.core.authn.services.v1.AuthnService/RevokeDevice": {
		fld("device_id", seed("record_id")), fld("reason", litS("perf")),
	},
	"udb.core.authn.services.v1.AuthnService/DeleteWebAuthnCredential": {
		fld("user_id", seed("user_id")), fld("credential_id", seed("record_id")),
	},
	"udb.core.authn.services.v1.AuthnService/AdminRevokeSession": {
		fld("user_id", seed("user_id")), fld("session_id", seed("session_id")), fld("reason", litS("perf")),
	},
	"udb.core.authn.services.v1.AuthnService/AdminRevokeAllUserSessions": {
		fld("user_id", seed("user_id")), fld("reason", litS("perf")),
	},
	"udb.core.authn.services.v1.AuthnService/AdminRevokeAllTenantSessions": {
		fld("tenant_id", seed("tenant_id")), fld("reason", litS("perf")),
	},
	"udb.core.authn.services.v1.AuthnService/EmergencyRevoke": {
		fld("principal_id", seed("subject")), fld("reason", litS("perf")),
	},
	"udb.core.authn.services.v1.AuthnService/FinishWebAuthnRegistration": {
		fld("challenge_id", seed("code")), fld("public_key_credential_json", litS("{}")), fld("label", litS("perf-key")),
	},
	"udb.core.authn.services.v1.AuthnService/FinishWebAuthnAuthentication": {
		fld("challenge_id", seed("code")), fld("public_key_credential_json", litS("{}")),
	},

	// ── ApiKeyService ───────────────────────────────────────────────────────
	"udb.core.apikey.services.v1.ApiKeyService/CreateApiKey": {
		fld("name", litS("bench-key")), fld("description", litS("bench")),
		fld("owner_type", enumV("API_KEY_OWNER_TYPE_SERVICE_ACCOUNT")), fld("owner_id", seed("owner_id")),
		fld("scopes", list(litS("resource:read"))), ctxField(),
	},
	"udb.core.apikey.services.v1.ApiKeyService/GetApiKey": {fld("key_id", seed("key_id"))},
	"udb.core.apikey.services.v1.ApiKeyService/ListApiKeys": {
		fld("owner_id", seed("owner_id")), fld("owner_type", enumV("API_KEY_OWNER_TYPE_SERVICE_ACCOUNT")),
		fld("status", enumV("API_KEY_STATUS_ACTIVE")), fld("page", sub(fld("page", litI(1)), fld("page_size", litI(50)))),
	},
	"udb.core.apikey.services.v1.ApiKeyService/UpdateApiKey": {
		fld("key_id", seed("key_id")), fld("name", litS("bench-key-2")), fld("description", litS("updated")),
		fld("scopes", list(litS("resource:read"))), ctxField(),
	},
	"udb.core.apikey.services.v1.ApiKeyService/RevokeApiKey": {
		fld("key_id", seed("key_id")), fld("revoke_reason", litS("bench cleanup")), ctxField(),
	},
	"udb.core.apikey.services.v1.ApiKeyService/RotateApiKey": {
		fld("key_id", seed("key_id")), fld("rotation_reason", litS("bench rotate")), ctxField(),
	},
	"udb.core.apikey.services.v1.ApiKeyService/EmergencyRevokeApiKeys": {
		fld("owner_id", seed("owner_id")), fld("tenant_id", seed("tenant_id")),
		fld("reason", litS("bench emergency")), ctxField(),
	},
	"udb.core.apikey.services.v1.ApiKeyService/ValidateApiKey": {
		fld("plain_key", seed("plain_key")), fld("endpoint", litS("/v1/test")),
		fld("required_scope", litS("resource:read")), fld("ip_address", litS("127.0.0.1")),
	},
	"udb.core.apikey.services.v1.ApiKeyService/GetApiKeyUsageStats": {fld("key_id", seed("key_id"))},

	// ── NotificationService ─────────────────────────────────────────────────
	"udb.core.notification.services.v1.NotificationService/SendNotification": {
		fld("event_type", seed("event_type")), fld("recipient_id", seed("user_id")),
		fld("recipient_address", litS("user@example.com")), fld("tenant_id", seed("tenant_id")),
		fld("project_id", seed("project")), fld("locale", litS("en")),
		fld("channels", list(enumV("NOTIFICATION_CHANNEL_EMAIL"))),
	},
	"udb.core.notification.services.v1.NotificationService/GetNotification": {fld("log_id", seed("log_id"))},
	"udb.core.notification.services.v1.NotificationService/ListNotifications": {
		fld("tenant_id", seed("tenant_id")), fld("page", sub(fld("page", litI(1)), fld("page_size", litI(20)))),
	},
	"udb.core.notification.services.v1.NotificationService/RetryNotification": {fld("log_id", seed("log_id"))},
	"udb.core.notification.services.v1.NotificationService/UpsertTemplate": {
		fld("event_type", seed("event_type")), fld("channel", enumV("NOTIFICATION_CHANNEL_EMAIL")),
		fld("locale", litS("en")), fld("subject_template", litS("Hello {name}")),
		fld("body_template", litS("Body {name}")), fld("is_active", litB(true)),
	},
	"udb.core.notification.services.v1.NotificationService/GetTemplate": {
		fld("event_type", seed("event_type")), fld("channel", enumV("NOTIFICATION_CHANNEL_EMAIL")), fld("locale", litS("en")),
	},
	"udb.core.notification.services.v1.NotificationService/ListTemplates": {
		fld("page", sub(fld("page", litI(1)), fld("page_size", litI(20)))),
	},
	"udb.core.notification.services.v1.NotificationService/GetDeliveryStats": {
		fld("tenant_id", seed("tenant_id")), fld("event_type", seed("event_type")),
		fld("date_from", litS("2026-01-01")), fld("date_to", litS("2026-12-31")),
	},
	"udb.core.notification.services.v1.NotificationService/SetPreference": {
		fld("user_id", seed("user_id")), fld("tenant_id", seed("tenant_id")),
		fld("channel", enumV("NOTIFICATION_CHANNEL_EMAIL")), fld("is_opted_out", litB(true)),
	},
	"udb.core.notification.services.v1.NotificationService/GetPreference": {
		fld("user_id", seed("user_id")), fld("tenant_id", seed("tenant_id")), fld("channel", enumV("NOTIFICATION_CHANNEL_EMAIL")),
	},
	"udb.core.notification.services.v1.NotificationService/ListPreferences": {
		fld("user_id", seed("user_id")), fld("tenant_id", seed("tenant_id")), fld("page", sub(fld("page", litI(1)), fld("page_size", litI(20)))),
	},

	// ── AuthzService ────────────────────────────────────────────────────────
	"udb.core.authz.services.v1.AuthzService/Authorize": {
		fld("principal", sub(fld("subject", seed("subject")), fld("user_id", seed("user_id")), fld("tenant_id", seed("tenant_id")))),
		fld("tenant_id", seed("tenant_id")), fld("project_id", seed("project")),
		fld("resource", sub(fld("resource_type", seed("resource")), fld("table", litS("sdk_live_records")))),
		fld("action", seed("action")), fld("domain", seed("tenant_id")),
	},
	"udb.core.authz.services.v1.AuthzService/CheckAccess": {
		fld("user_id", seed("user_id")), fld("domain", seed("tenant_id")), fld("object", seed("object")), fld("action", seed("action")),
	},
	"udb.core.authz.services.v1.AuthzService/CreateRole": {
		fld("name", litS("reader")), fld("created_by", seed("subject")), fld("role_code", seed("role_code")),
		fld("domain", seed("tenant_id")), fld("tenant_id", seed("tenant_id")), fld("scope_type", enumV("ROLE_SCOPE_TYPE_TENANT")),
	},
	"udb.core.authz.services.v1.AuthzService/AssignRole": {
		fld("user_id", seed("user_id")), fld("role_id", seed("role_id")), fld("domain", seed("tenant_id")),
		fld("assigned_by", seed("subject")), fld("principal_kind", enumV("PRINCIPAL_KIND_USER")), fld("tenant_id", seed("tenant_id")),
	},
	"udb.core.authz.services.v1.AuthzService/CreatePolicyRule": {
		fld("subject", seed("subject")), fld("domain", seed("tenant_id")), fld("object", seed("object")),
		fld("action", seed("action")), fld("effect", enumV("POLICY_EFFECT_ALLOW")), fld("created_by", seed("subject")), fld("tenant_id", seed("tenant_id")),
	},
	"udb.core.authz.services.v1.AuthzService/ListUserPermissions": {fld("user_id", seed("user_id")), fld("domain", seed("tenant_id"))},
	"udb.core.authz.services.v1.AuthzService/ListAccessDecisionAudits": {
		fld("user_id", seed("user_id")), fld("domain", seed("tenant_id")), fld("page", sub(fld("page_size", litI(50)))),
	},
	"udb.core.authz.services.v1.AuthzService/RevokeRole": {
		fld("user_id", seed("user_id")), fld("user_role_id", seed("user_role_id")), fld("reason", litS("rotation")), fld("revoked_by", seed("subject")),
	},
	"udb.core.authz.services.v1.AuthzService/ListUserRoles": {fld("user_id", seed("user_id")), fld("domain", seed("tenant_id")), fld("active_only", litB(true))},
	"udb.core.authz.services.v1.AuthzService/GetRole":       {fld("role_id", seed("role_id"))},
	"udb.core.authz.services.v1.AuthzService/ListRoles":     {fld("domain", seed("tenant_id")), fld("active_only", litB(true)), fld("page", sub(fld("page_size", litI(50))))},
	"udb.core.authz.services.v1.AuthzService/BatchCheckPermissions": {
		fld("user_id", seed("user_id")), fld("domain", seed("tenant_id")),
		fld("checks", list(sub(fld("object", seed("object")), fld("action", seed("action"))))),
	},
	"udb.core.authz.services.v1.AuthzService/UpdateRole": {
		fld("role_id", seed("role_id")), fld("updated_by", seed("subject")), fld("name", litS("reader-2")), fld("description", litS("bench")), fld("is_active", litB(true)),
	},
	"udb.core.authz.services.v1.AuthzService/DeleteRole":       {fld("role_id", seed("role_id")), fld("deleted_by", seed("subject"))},
	"udb.core.authz.services.v1.AuthzService/GetPolicyRule":    {fld("policy_id", seed("policy_id"))},
	"udb.core.authz.services.v1.AuthzService/ListPolicyRules":  {fld("domain", seed("tenant_id")), fld("active_only", litB(true)), fld("page", sub(fld("page_size", litI(50))))},
	"udb.core.authz.services.v1.AuthzService/DeletePolicyRule": {fld("policy_id", seed("policy_id")), fld("deleted_by", seed("subject"))},
	"udb.core.authz.services.v1.AuthzService/PutRoleBinding": {
		fld("binding", sub(fld("subject", seed("subject")), fld("role", seed("role")), fld("tenant", seed("tenant_id")), fld("project", seed("project")), fld("source", litS("manual")))),
	},
	"udb.core.authz.services.v1.AuthzService/PutRelationship": {
		fld("tuple", sub(fld("subject", seed("subject")), fld("relation", seed("relation")), fld("object", seed("object")), fld("tenant", seed("tenant_id")), fld("project", seed("project")), fld("source", litS("manual")))),
	},
	"udb.core.authz.services.v1.AuthzService/PutAuthzPolicy": {
		fld("policy", sub(fld("id", seed("policy_id")), fld("priority", litI(100)), fld("enabled", litB(true)), fld("effect", litS("allow")),
			fld("tenant", seed("tenant_id")), fld("subject", seed("subject")), fld("action", seed("action")), fld("resource", seed("resource")), fld("required_scopes", list(litS("udb:read"))))),
	},
	"udb.core.authz.services.v1.AuthzService/LintAuthzPolicies": {},
	"udb.core.authz.services.v1.AuthzService/GetNativeAccess": {
		fld("principal", sub(fld("subject", seed("subject")), fld("user_id", seed("user_id")), fld("tenant_id", seed("tenant_id")))),
		fld("tenant_id", seed("tenant_id")), fld("project_id", seed("project")),
		fld("resource", sub(fld("resource_type", seed("resource")), fld("table", litS("sdk_live_records")))),
		fld("action", seed("action")), fld("backend", litS("postgres")), fld("requested_scopes", list(litS("udb:read"))),
	},
	"udb.core.authz.services.v1.AuthzService/GetPolicyBundle": {fld("tenant_id", seed("tenant_id")), fld("project_id", seed("project")), fld("domain", seed("tenant_id"))},
	"udb.core.authz.services.v1.AuthzService/CreatePolicyDraft": {
		actorF("udb:authz:policy:write"), fld("tenant_id", seed("tenant_id")), fld("project_id", seed("project")),
		fld("policy_set_name", litS("default")), fld("title", litS("draft 1")), fld("change_reason", litS("init")), fld("document", sub()),
	},
	"udb.core.authz.services.v1.AuthzService/UpdatePolicyDraft": {
		actorF("udb:authz:policy:write"), fld("draft_id", seed("policy_draft_id")), fld("document", sub()), fld("change_reason", litS("edit")), fld("title", litS("draft 1")),
	},
	"udb.core.authz.services.v1.AuthzService/DiffPolicyDraft":   {actorF("udb:authz:policy:read"), fld("draft_id", seed("policy_draft_id"))},
	"udb.core.authz.services.v1.AuthzService/SubmitPolicyDraft": {actorF("udb:authz:policy:write"), fld("draft_id", seed("policy_draft_id"))},
	"udb.core.authz.services.v1.AuthzService/ApprovePolicyDraft": {
		actorF("udb:authz:policy:approve"), fld("draft_id", seed("policy_draft_id")), fld("reviewer", seed("subject")), fld("reason", litS("ok")),
	},
	"udb.core.authz.services.v1.AuthzService/RejectPolicyDraft": {
		actorF("udb:authz:policy:approve"), fld("draft_id", seed("policy_draft_id")), fld("reviewer", seed("subject")), fld("reason", litS("nack")),
	},
	"udb.core.authz.services.v1.AuthzService/ActivatePolicyVersion": {actorF("udb:authz:admin"), fld("policy_version_id", seed("policy_id"))},
	"udb.core.authz.services.v1.AuthzService/RollbackPolicyVersion": {
		actorF("udb:authz:admin"), fld("policy_set_id", seed("policy_id")), fld("target_version_id", seed("policy_id")), fld("change_reason", litS("revert")),
	},
	"udb.core.authz.services.v1.AuthzService/ActivateCanary": {
		actorF("udb:authz:admin"), fld("policy_version_id", seed("policy_id")), fld("scope_kind", enumV("CANARY_SCOPE_KIND_PERCENT")),
		fld("scope_values", list(litS("10"))), fld("success_window_secs", litI(300)), fld("metric_threshold", litF(0.99)), fld("min_samples", litI(100)),
	},
	"udb.core.authz.services.v1.AuthzService/PromoteCanary":   {actorF("udb:authz:admin"), fld("canary_id", seed("policy_id"))},
	"udb.core.authz.services.v1.AuthzService/GetCanaryStatus": {actorF("udb:authz:policy:read"), fld("canary_id", seed("policy_id"))},
	"udb.core.authz.services.v1.AuthzService/ListPolicyVersions": {
		actorF("udb:authz:policy:read"), fld("tenant_id", seed("tenant_id")), fld("project_id", seed("project")),
		fld("policy_set_id", seed("policy_id")), fld("state", enumV("POLICY_VERSION_STATE_ACTIVE")), fld("page", sub(fld("page_size", litI(50)))),
	},
	"udb.core.authz.services.v1.AuthzService/SimulatePolicy": {
		actorF("udb:authz:policy:read"), fld("tenant_id", seed("tenant_id")), fld("project_id", seed("project")), fld("draft_id", seed("policy_draft_id")),
		fld("cases", list(sub(fld("principal", sub(fld("subject", seed("subject")))), fld("resource", sub(fld("resource_type", seed("resource")))), fld("action", seed("action")), fld("label", litS("c1"))))), fld("persist", litB(false)),
	},
	"udb.core.authz.services.v1.AuthzService/ExplainPolicy": {
		actorF("udb:authz:policy:read"), fld("tenant_id", seed("tenant_id")), fld("project_id", seed("project")),
		fld("test_case", sub(fld("principal", sub(fld("subject", seed("subject")))), fld("resource", sub(fld("resource_type", seed("resource")))), fld("action", seed("action")))),
	},
	"udb.core.authz.services.v1.AuthzService/GetAuthzRevision":        {fld("tenant_id", seed("tenant_id")), fld("project_id", seed("project"))},
	"udb.core.authz.services.v1.AuthzService/InvalidatePolicyBundles": {actorF("udb:authz:admin"), fld("tenant_id", seed("tenant_id")), fld("project_id", seed("project")), fld("reason", litS("rotate"))},
	"udb.core.authz.services.v1.AuthzService/SeedBuiltinRoles":        {actorF("udb:authz:admin"), fld("tenant_id", seed("tenant_id")), fld("project_id", seed("project"))},
	"udb.core.authz.services.v1.AuthzService/MigrateLegacyPolicies":   {actorF("udb:authz:admin"), fld("tenant_id", seed("tenant_id")), fld("project_id", seed("project")), fld("apply", litB(false)), fld("policy_set_name", litS("default"))},

	// ── IdentityProviderService ─────────────────────────────────────────────
	"udb.core.idp.services.v1.IdentityProviderService/CreateProvider": {
		fld("tenant_id", seed("tenant_id")), fld("kind", enumV("IDP_KIND_OIDC")), fld("display_name", litS("Acme OIDC")),
		fld("issuer", litS("https://idp.example.com")), fld("jwks_url", litS("https://idp.example.com/jwks")),
		fld("client_ids", list(litS("client-1"))), fld("audiences", list(litS("udb"))),
		fld("claim_mapping_json", litS("{}")), fld("group_mapping_json", litS("{}")), fld("jit_policy_json", litS("{}")),
		fld("account_linking_policy", litS("explicit")), fld("enabled", litB(true)), fld("created_by", seed("user_id")), ctxField(),
	},
	"udb.core.idp.services.v1.IdentityProviderService/UpdateProvider": {
		fld("provider_id", seed("provider_id")), fld("tenant_id", seed("tenant_id")), fld("display_name", litS("Acme OIDC v2")),
		fld("claim_mapping_json", litS("{}")), fld("group_mapping_json", litS("{}")), fld("jit_policy_json", litS("{}")),
		fld("account_linking_policy", litS("explicit")), fld("updated_by", seed("user_id")), ctxField(),
	},
	"udb.core.idp.services.v1.IdentityProviderService/DisableProvider":       {fld("provider_id", seed("provider_id")), fld("tenant_id", seed("tenant_id")), fld("updated_by", seed("user_id")), ctxField()},
	"udb.core.idp.services.v1.IdentityProviderService/GetProvider":           {fld("provider_id", seed("provider_id")), fld("tenant_id", seed("tenant_id"))},
	"udb.core.idp.services.v1.IdentityProviderService/ListProviders":         {fld("tenant_id", seed("tenant_id")), fld("kind", enumV("IDP_KIND_UNSPECIFIED")), fld("enabled_only", litB(false)), fld("page", sub(fld("page", litI(1)), fld("page_size", litI(20))))},
	"udb.core.idp.services.v1.IdentityProviderService/TestProviderDiscovery": {fld("provider_id", seed("provider_id")), fld("tenant_id", seed("tenant_id"))},
	"udb.core.idp.services.v1.IdentityProviderService/ForceJwksRefresh":      {fld("provider_id", seed("provider_id")), fld("tenant_id", seed("tenant_id"))},
	"udb.core.idp.services.v1.IdentityProviderService/PreviewClaimMapping": {
		fld("provider_id", seed("provider_id")), fld("tenant_id", seed("tenant_id")), fld("claims_json", litS(`{"sub":"abc","email":"a@x.com"}`)), fld("claim_mapping_json", litS("")),
	},
	"udb.core.idp.services.v1.IdentityProviderService/PreviewGroupMapping": {
		fld("provider_id", seed("provider_id")), fld("tenant_id", seed("tenant_id")), fld("groups", list(litS("admins"))), fld("group_mapping_json", litS("")),
	},
	"udb.core.idp.services.v1.IdentityProviderService/ListExternalIdentities": {fld("tenant_id", seed("tenant_id")), fld("page", sub(fld("page", litI(1)), fld("page_size", litI(20))))},
	"udb.core.idp.services.v1.IdentityProviderService/LinkIdentity": {
		fld("tenant_id", seed("tenant_id")), fld("provider_id", seed("provider_id")), fld("subject", litS("ext-subject-1")), fld("user_id", seed("user_id")), fld("email", litS("a@x.com")), fld("email_verified", litB(true)), ctxField(),
	},
	"udb.core.idp.services.v1.IdentityProviderService/UnlinkIdentity": {fld("tenant_id", seed("tenant_id")), fld("external_identity_id", seed("record_id")), ctxField()},
	"udb.core.idp.services.v1.IdentityProviderService/ImportSamlMetadata": {
		fld("provider_id", seed("provider_id")), fld("tenant_id", seed("tenant_id")), fld("metadata_xml", litS("<EntityDescriptor></EntityDescriptor>")), fld("updated_by", seed("user_id")), ctxField(),
	},
	"udb.core.idp.services.v1.IdentityProviderService/StartSamlLogin": {fld("provider_id", seed("provider_id")), fld("tenant_id", seed("tenant_id")), fld("relay_state", litS("state-1"))},
	"udb.core.idp.services.v1.IdentityProviderService/SamlAcs":        {fld("provider_id", seed("provider_id")), fld("tenant_id", seed("tenant_id")), fld("saml_response", litS("")), fld("relay_state", litS("state-1")), ctxField()},
	"udb.core.idp.services.v1.IdentityProviderService/ResolveExternalIdentity": {
		fld("provider_id", seed("provider_id")), fld("tenant_id", seed("tenant_id")), fld("claims_json", litS(`{"sub":"abc","email":"a@x.com","email_verified":true}`)),
	},
	"udb.core.idp.services.v1.IdentityProviderService/ScimCreateUser":  {fld("tenant_id", seed("tenant_id")), fld("provider_id", seed("provider_id")), fld("scim_user_json", litS(`{"userName":"a@x.com","active":true}`)), ctxField()},
	"udb.core.idp.services.v1.IdentityProviderService/ScimGetUser":     {fld("tenant_id", seed("tenant_id")), fld("provider_id", seed("provider_id")), fld("scim_user_id", seed("record_id"))},
	"udb.core.idp.services.v1.IdentityProviderService/ScimListUsers":   {fld("tenant_id", seed("tenant_id")), fld("provider_id", seed("provider_id")), fld("filter", litS("")), fld("page", sub(fld("page", litI(1)), fld("page_size", litI(20))))},
	"udb.core.idp.services.v1.IdentityProviderService/ScimReplaceUser": {fld("tenant_id", seed("tenant_id")), fld("provider_id", seed("provider_id")), fld("scim_user_id", seed("record_id")), fld("scim_user_json", litS(`{"userName":"a@x.com","active":true}`)), ctxField()},
	"udb.core.idp.services.v1.IdentityProviderService/ScimPatchUser": {
		fld("tenant_id", seed("tenant_id")), fld("provider_id", seed("provider_id")), fld("scim_user_id", seed("record_id")),
		fld("operations", list(sub(fld("op", litS("replace")), fld("path", litS("active")), fld("value_json", litS("false"))))), ctxField(),
	},
	"udb.core.idp.services.v1.IdentityProviderService/ScimDeleteUser":  {fld("tenant_id", seed("tenant_id")), fld("provider_id", seed("provider_id")), fld("scim_user_id", seed("record_id")), ctxField()},
	"udb.core.idp.services.v1.IdentityProviderService/ScimCreateGroup": {fld("tenant_id", seed("tenant_id")), fld("provider_id", seed("provider_id")), fld("scim_group_json", litS(`{"displayName":"admins"}`)), ctxField()},
	"udb.core.idp.services.v1.IdentityProviderService/ScimGetGroup":    {fld("tenant_id", seed("tenant_id")), fld("provider_id", seed("provider_id")), fld("scim_group_id", seed("record_id"))},
	"udb.core.idp.services.v1.IdentityProviderService/ScimListGroups":  {fld("tenant_id", seed("tenant_id")), fld("provider_id", seed("provider_id")), fld("filter", litS("")), fld("page", sub(fld("page", litI(1)), fld("page_size", litI(20))))},
	"udb.core.idp.services.v1.IdentityProviderService/ScimPatchGroup": {
		fld("tenant_id", seed("tenant_id")), fld("provider_id", seed("provider_id")), fld("scim_group_id", seed("record_id")),
		fld("operations", list(sub(fld("op", litS("add")), fld("path", litS("members")), fld("value_json", litS(`["x"]`))))), ctxField(),
	},
	"udb.core.idp.services.v1.IdentityProviderService/ScimDeleteGroup": {fld("tenant_id", seed("tenant_id")), fld("provider_id", seed("provider_id")), fld("scim_group_id", seed("record_id")), ctxField()},

	// ── AssetService ────────────────────────────────────────────────────────
	"udb.core.asset.services.v1.AssetService/CreatePipelineDefinition": {
		fld("tenant_id", seed("tenant_id")), fld("name", litS("thumbnail-pipeline")), fld("description", litS("Generate thumbnails")),
		fld("media_type", litS("image/png")), fld("steps", litS(`[{"name":"resize","type":"TRANSFORM"}]`)), fld("version", litI(1)),
	},
	"udb.core.asset.services.v1.AssetService/GetPipelineDefinition": {fld("tenant_id", seed("tenant_id")), fld("definition_id", seed("definition_id"))},
	"udb.core.asset.services.v1.AssetService/RegisterAsset": {
		fld("tenant_id", seed("tenant_id")), fld("project_id", seed("project")), fld("file_id", seed("file_id")),
		fld("name", litS("logo.png")), fld("media_type", litS("image/png")), fld("metadata", litS(`{"source":"upload"}`)),
	},
	"udb.core.asset.services.v1.AssetService/StartPipeline": {
		fld("tenant_id", seed("tenant_id")), fld("definition_id", seed("definition_id")), fld("asset_id", seed("asset_id")), fld("context", litS("{}")), fld("correlation_id", litS("run-001")),
	},
	"udb.core.asset.services.v1.AssetService/GetPipeline": {fld("tenant_id", seed("tenant_id")), fld("instance_id", seed("instance_id"))},
	"udb.core.asset.services.v1.AssetService/CompleteStep": {
		fld("tenant_id", seed("tenant_id")), fld("step_id", seed("step_id")), fld("status", litS("COMPLETED")), fld("result", litS("{}")), fld("error_message", litS("")),
	},
	"udb.core.asset.services.v1.AssetService/ListAssets": {fld("tenant_id", seed("tenant_id")), fld("media_type", litS("image/png")), fld("page", litI(1)), fld("page_size", litI(20))},
	"udb.core.asset.services.v1.AssetService/GetAsset":   {fld("tenant_id", seed("tenant_id")), fld("asset_id", seed("asset_id"))},

	// ── StorageService ──────────────────────────────────────────────────────
	"udb.core.storage.services.v1.StorageService/RegisterUpload": {
		fld("tenant_id", seed("tenant_id")), fld("project_id", seed("project")), fld("filename", litS("report.pdf")), fld("content_type", litS("application/pdf")),
		fld("file_type", litS("document")), fld("reference_id", seed("file_id")), fld("reference_type", litS("document")), fld("expires_in_minutes", litI(15)), fld("size_bytes", litI(1024)),
	},
	"udb.core.storage.services.v1.StorageService/FinalizeUpload": {
		fld("tenant_id", seed("tenant_id")), fld("file_id", seed("file_id")), fld("content_type", litS("application/pdf")), fld("file_type", litS("document")),
		fld("reference_id", seed("file_id")), fld("reference_type", litS("document")), fld("size_bytes", litI(1024)),
	},
	"udb.core.storage.services.v1.StorageService/GetDownloadUrl": {fld("tenant_id", seed("tenant_id")), fld("file_id", seed("file_id")), fld("expires_in_minutes", litI(15))},
	"udb.core.storage.services.v1.StorageService/GetFile":        {fld("tenant_id", seed("tenant_id")), fld("file_id", seed("file_id"))},
	"udb.core.storage.services.v1.StorageService/UpdateFile": {
		fld("tenant_id", seed("tenant_id")), fld("file_id", seed("file_id")), fld("filename", litS("renamed.pdf")), fld("content_type", litS("application/pdf")),
		fld("file_type", litS("document")), fld("reference_id", seed("file_id")), fld("reference_type", litS("document")), fld("is_public", litB(true)),
	},
	"udb.core.storage.services.v1.StorageService/DeleteFile": {fld("tenant_id", seed("tenant_id")), fld("file_id", seed("file_id"))},
	"udb.core.storage.services.v1.StorageService/ListFiles": {
		fld("tenant_id", seed("tenant_id")), fld("file_type", litS("document")), fld("reference_id", seed("file_id")), fld("reference_type", litS("document")), fld("uploaded_by", seed("user_id")), fld("page", litI(1)), fld("page_size", litI(20)),
	},

	// ── AnalyticsService ────────────────────────────────────────────────────
	"udb.core.analytics.services.v1.AnalyticsService/RecordPipelineMetric": {
		fld("stage_name", seed("stage_name")), fld("tenant_id", seed("tenant_id")), fld("latency_ms", litF(12.5)), fld("is_success", litB(true)), ctxField(),
	},
	"udb.core.analytics.services.v1.AnalyticsService/GetPipelineSummary": {
		fld("stage_name", seed("stage_name")), fld("tenant_id", seed("tenant_id")), fld("hour_from", litS("2026-06-01T00")), fld("hour_to", litS("2026-06-14T23")), fld("page", sub(fld("page", litI(1)), fld("page_size", litI(50)))),
	},
	"udb.core.analytics.services.v1.AnalyticsService/GetExecutorPerformance":     {fld("date_from", litS("2026-06-01")), fld("date_to", litS("2026-06-14"))},
	"udb.core.analytics.services.v1.AnalyticsService/GetReconciliationAnalytics": {fld("date_from", litS("2026-06-01")), fld("date_to", litS("2026-06-14"))},
	"udb.core.analytics.services.v1.AnalyticsService/GetThroughput":              {fld("tenant_id", seed("tenant_id")), fld("hour_from", litS("2026-06-01T00")), fld("hour_to", litS("2026-06-14T23"))},
	"udb.core.analytics.services.v1.AnalyticsService/GetSlaCompliance": {
		fld("stage_name", seed("stage_name")), fld("date_from", litS("2026-06-01")), fld("date_to", litS("2026-06-14")), fld("p99_threshold_ms", litF(250.0)), fld("error_rate_threshold", litF(0.01)),
	},
	"udb.core.analytics.services.v1.AnalyticsService/TriggerSnapshot": {fld("stage_name", seed("stage_name")), fld("hour", litS("2026-06-14T10")), ctxField()},

	// ── TenantService ───────────────────────────────────────────────────────
	"udb.core.tenant.services.v1.TenantService/CreateTenant": {
		fld("code", litS("acme-bench")), fld("name", litS("Acme Bench")), fld("type", litS("organization")), fld("config", litS("{}")), fld("branding", litS("{}")),
	},
	"udb.core.tenant.services.v1.TenantService/GetTenant":          {fld("tenant_id", seed("tenant_id"))},
	"udb.core.tenant.services.v1.TenantService/ListTenants":        {fld("page", litI(1)), fld("page_size", litI(20))},
	"udb.core.tenant.services.v1.TenantService/UpdateTenant":       {fld("tenant_id", seed("tenant_id")), fld("name", litS("Acme Bench")), fld("status", litS("active")), fld("config", litS("{}")), fld("branding", litS("{}"))},
	"udb.core.tenant.services.v1.TenantService/GetTenantConfig":    {fld("tenant_id", seed("tenant_id"))},
	"udb.core.tenant.services.v1.TenantService/UpdateTenantConfig": {fld("tenant_id", seed("tenant_id")), fld("config_key", litS("feature.flag")), fld("config_value", litS("on")), fld("type", litS("string"))},

	// ── WebRTC (Room/Peer/Track/Turn/Signaling) ─────────────────────────────
	"udb.core.webrtc.services.v1.RoomService/CreateRoom":       {fld("tenant_id", seed("tenant_id")), fld("name", litS("bench-room")), fld("max_participants", litI(10)), fld("config", litS("{}")), fld("created_by", seed("user_id"))},
	"udb.core.webrtc.services.v1.RoomService/GetRoom":          {fld("tenant_id", seed("tenant_id")), fld("room_id", seed("room_id"))},
	"udb.core.webrtc.services.v1.RoomService/UpdateRoom":       {fld("tenant_id", seed("tenant_id")), fld("room_id", seed("room_id")), fld("name", litS("bench-room-2")), fld("state", litS("active")), fld("config", litS("{}"))},
	"udb.core.webrtc.services.v1.RoomService/CloseRoom":        {fld("tenant_id", seed("tenant_id")), fld("room_id", seed("room_id"))},
	"udb.core.webrtc.services.v1.RoomService/ListRooms":        {fld("tenant_id", seed("tenant_id")), fld("state", litS("active")), fld("page", litI(1)), fld("page_size", litI(20))},
	"udb.core.webrtc.services.v1.PeerService/JoinRoom":         {fld("tenant_id", seed("tenant_id")), fld("room_id", seed("room_id")), fld("display_name", litS("Bench User")), fld("metadata", litS("{}")), fld("user_agent", litS("bench/1.0"))},
	"udb.core.webrtc.services.v1.PeerService/LeaveRoom":        {fld("tenant_id", seed("tenant_id")), fld("room_id", seed("room_id")), fld("peer_id", seed("peer_id"))},
	"udb.core.webrtc.services.v1.PeerService/GetPeer":          {fld("tenant_id", seed("tenant_id")), fld("peer_id", seed("peer_id"))},
	"udb.core.webrtc.services.v1.PeerService/ListPeers":        {fld("tenant_id", seed("tenant_id")), fld("room_id", seed("room_id")), fld("state", litS("connected"))},
	"udb.core.webrtc.services.v1.TrackService/PublishTrack":    {fld("tenant_id", seed("tenant_id")), fld("room_id", seed("room_id")), fld("peer_id", seed("peer_id")), fld("kind", litS("audio")), fld("label", litS("mic")), fld("settings", litS("{}")), fld("metadata", litS("{}"))},
	"udb.core.webrtc.services.v1.TrackService/UnpublishTrack":  {fld("tenant_id", seed("tenant_id")), fld("track_id", seed("track_id"))},
	"udb.core.webrtc.services.v1.TrackService/MuteTrack":       {fld("tenant_id", seed("tenant_id")), fld("track_id", seed("track_id")), fld("muted", litB(true))},
	"udb.core.webrtc.services.v1.TrackService/ListTracks":      {fld("tenant_id", seed("tenant_id")), fld("room_id", seed("room_id")), fld("peer_id", seed("peer_id")), fld("kind", litS("audio"))},
	"udb.core.webrtc.services.v1.TurnService/IssueCredentials": {fld("tenant_id", seed("tenant_id")), fld("room_id", seed("room_id")), fld("peer_id", seed("peer_id")), fld("ttl_seconds", litI(3600))},
	"udb.core.webrtc.services.v1.SignalingService/Signal":      {fld("tenant_id", seed("tenant_id")), fld("room_id", seed("room_id")), fld("peer_id", seed("peer_id")), fld("ping", litB(true))},

	// ── ControlPlaneService (xDS) ───────────────────────────────────────────
	"udb.core.control.services.v1.ControlPlaneService/GetResources":    {fld("resource_type", enumV("RESOURCE_TYPE_BACKEND_TARGET_DEFINITION")), fld("tenant_id", seed("tenant_id")), fld("page", sub(fld("page", litI(1)), fld("page_size", litI(50)))), ctxField()},
	"udb.core.control.services.v1.ControlPlaneService/ListNodeStates":  {fld("resource_type", enumV("RESOURCE_TYPE_UNSPECIFIED")), fld("page", sub(fld("page", litI(1)), fld("page_size", litI(50)))), ctxField()},
	"udb.core.control.services.v1.ControlPlaneService/AckStatus":       {fld("node_id", seed("node_id")), fld("resource_type", enumV("RESOURCE_TYPE_BACKEND_TARGET_DEFINITION")), ctxField()},
	"udb.core.control.services.v1.ControlPlaneService/StreamResources": {fld("node_id", seed("node_id")), fld("resource_type", enumV("RESOURCE_TYPE_BACKEND_TARGET_DEFINITION")), ctxField()},
	"udb.core.control.services.v1.ControlPlaneService/DeltaResources":  {fld("node_id", seed("node_id")), fld("resource_type", enumV("RESOURCE_TYPE_BACKEND_TARGET_DEFINITION")), ctxField()},

	// ── DataBroker (the 64 not covered by the typed perfRealBody) ────────────
	"udb.services.v1.DataBroker/BatchSelect":             {dctx(), fld("message_type", seed("message_type")), fld("limit", litI(10))},
	"udb.services.v1.DataBroker/SelectV2":                {dctx(), fld("message_type", seed("message_type")), fld("filter", structV(fld("record_id", seed("record_id")))), fld("limit", litI(10))},
	"udb.services.v1.DataBroker/BatchUpsert":             {dctx(), fld("message_type", seed("message_type")), fld("payload", structV(fld("record_id", seed("record_id")), fld("payload", litS("perf")))), fld("return_record", litB(true))},
	"udb.services.v1.DataBroker/VectorSearch":            {dctx(), fld("collection", seed("message_type")), fld("vector", list(litF(0.1), litF(0.2), litF(0.3))), fld("limit", litI(5)), fld("with_payload", litB(true))},
	"udb.services.v1.DataBroker/VectorHybridSearch":      {dctx(), fld("collection", seed("message_type")), fld("vector", list(litF(0.1), litF(0.2), litF(0.3))), fld("text_query", litS("hello")), fld("limit", litI(5)), fld("with_payload", litB(true))},
	"udb.services.v1.DataBroker/VectorUpsert":            {dctx(), fld("collection", seed("message_type")), fld("points", list(sub(fld("id", seed("record_id")), fld("vector", list(litF(0.1), litF(0.2), litF(0.3))))))},
	"udb.services.v1.DataBroker/VectorBatchUpsert":       {dctx(), fld("collection", seed("message_type")), fld("points", list(sub(fld("id", seed("record_id")), fld("vector", list(litF(0.1), litF(0.2), litF(0.3))))))},
	"udb.services.v1.DataBroker/PutObject":               {dctx(), fld("bucket", seed("bucket")), fld("object_key", seed("object_key")), fld("data", litS("perf")), fld("content_type", litS("application/octet-stream")), fld("final_chunk", litB(true))},
	"udb.services.v1.DataBroker/GetObject":               {dctx(), fld("bucket", seed("bucket")), fld("object_key", seed("object_key"))},
	"udb.services.v1.DataBroker/InitiateMultipartUpload": {dctx(), fld("bucket", seed("bucket")), fld("object_key", seed("object_key")), fld("content_type", litS("application/octet-stream")), fld("part_count", litI(1)), fld("ttl_seconds", litI(300))},
	"udb.services.v1.DataBroker/CacheDelete":             {dctx(), fld("resource", sub(fld("backend", litS("redis")))), fld("key", seed("object_key"))},
	"udb.services.v1.DataBroker/CacheScan":               {dctx(), fld("resource", sub(fld("backend", litS("redis")))), fld("key_pattern", litS("*")), fld("limit", litI(50))},
	"udb.services.v1.DataBroker/DocumentDelete":          {dctx(), fld("resource", sub(fld("backend", litS("mongodb")), fld("resource_name", seed("mongo_collection")))), fld("document_id", seed("document_id"))},
	"udb.services.v1.DataBroker/GraphQuery":              {dctx(), fld("resource", sub(fld("backend", litS("neo4j")))), fld("query", litS("MATCH (n) RETURN n LIMIT 1")), fld("read_only", litB(true)), fld("limit", litI(10))},
	"udb.services.v1.DataBroker/GraphMutate":             {dctx(), fld("resource", sub(fld("backend", litS("neo4j")))), fld("query", litS("CREATE (n:Node {id:$id})")), fld("parameters", structV(fld("id", seed("record_id"))))},
	"udb.services.v1.DataBroker/TimeSeriesWrite":         {dctx(), fld("resource", sub(fld("backend", litS("clickhouse"))))},
	"udb.services.v1.DataBroker/TimeSeriesQuery":         {dctx(), fld("resource", sub(fld("backend", litS("clickhouse")))), fld("limit", litI(100))},
	"udb.services.v1.DataBroker/AnalyticalQuery":         {dctx(), fld("resource", sub(fld("backend", litS("clickhouse")))), fld("query", litS("SELECT 1")), fld("limit", litI(100))},
	"udb.services.v1.DataBroker/BeginTx":                 {dctx(), fld("operation", litS("upsert")), fld("message_type", seed("message_type")), fld("payload", structV(fld("record_id", seed("record_id"))))},
	"udb.services.v1.DataBroker/PublishCDC":              {dctx(), fld("topic_pattern", litS("*"))},
	"udb.services.v1.DataBroker/CreateMaterializedView":  {dctx(), fld("schema", litS("public")), fld("name", litS("mv_test")), fld("query", litS("SELECT 1")), fld("with_data", litB(true))},
	"udb.services.v1.DataBroker/EnqueueOutboxEvent":      {dctx(), fld("topic", seed("event_type")), fld("partition_key", seed("document_id")), fld("payload", structV(fld("event_id", litS("perf-evt-1")), fld("event_type", seed("event_type")), fld("correlation_id", litS("perf-corr-1")), fld("document_id", seed("document_id"))))},
	"udb.services.v1.DataBroker/DropResource":            {dctx("udb:admin"), fld("backend", litS("mongodb")), fld("resource_name", seed("mongo_collection"))},
	"udb.services.v1.DataBroker/StageCatalog":            {dctx("udb:admin"), fld("manifest_json", litS("{}")), fld("project_id", seed("project")), fld("reason", litS("stage"))},
	"udb.services.v1.DataBroker/ActivateCatalog":         {dctx("udb:admin"), fld("project_id", seed("project"))},
	"udb.services.v1.DataBroker/RollbackCatalog":         {dctx("udb:admin"), fld("project_id", seed("project"))},
	"udb.services.v1.DataBroker/ValidateCatalog":         {dctx("udb:admin"), fld("manifest_json", litS("{}")), fld("project_id", seed("project"))},
	"udb.services.v1.DataBroker/GetCatalogVersions":      {dctx("udb:admin"), fld("redact", litB(false))},
	"udb.services.v1.DataBroker/GetCatalogVersion":       {dctx("udb:admin"), fld("project_id", seed("project"))},
	"udb.services.v1.DataBroker/PlanMigration":           {dctx("udb:admin"), fld("project_id", seed("project")), fld("dry_run", litB(true))},
	"udb.services.v1.DataBroker/ApplyMigration":          {dctx("udb:admin"), fld("run_id", seed("migration_id")), fld("project_id", seed("project"))},
	"udb.services.v1.DataBroker/GetMigrationStatus":      {dctx("udb:admin"), fld("run_id", seed("migration_id")), fld("project_id", seed("project"))},
	"udb.services.v1.DataBroker/ListMigrationRuns":       {dctx("udb:admin"), fld("project_id", seed("project")), fld("limit", litI(50))},
	"udb.services.v1.DataBroker/ApproveMigrationPlan":    {dctx("udb:admin"), fld("run_id", seed("migration_id")), fld("project_id", seed("project"))},
	"udb.services.v1.DataBroker/ListDlqEvents":           {dctx(), fld("limit", litI(50))},
	"udb.services.v1.DataBroker/GetDlqEvent":             {dctx(), fld("dlq_id", seed("record_id"))},
	"udb.services.v1.DataBroker/ReplayDlqEvent":          {dctx(), fld("dlq_id", seed("record_id")), fld("preserve_event_id", litB(false))},
	"udb.services.v1.DataBroker/DismissDlqEvent":         {dctx(), fld("dlq_id", seed("record_id"))},
	"udb.services.v1.DataBroker/QuarantineDlqEvent":      {dctx(), fld("dlq_id", seed("record_id"))},
	"udb.services.v1.DataBroker/GetCdcStatus":            {dctx(), fld("slot_name", litS("udb_cdc"))},
	"udb.services.v1.DataBroker/PauseCdc":                {dctx(), fld("slot_name", litS("udb_cdc")), fld("reason", litS("maintenance"))},
	"udb.services.v1.DataBroker/ResumeCdc":               {dctx(), fld("slot_name", litS("udb_cdc")), fld("reason", litS("resume"))},
	"udb.services.v1.DataBroker/StepDownCdcLeader":       {dctx(), fld("slot_name", litS("udb_cdc")), fld("reason", litS("failover"))},
	"udb.services.v1.DataBroker/PreviewCdcRedaction":     {dctx(), fld("message_type", seed("message_type")), fld("topic", seed("event_type")), fld("payload_json", litS("{}")), fld("redaction_mode", litS("mask")), fld("redaction_version", litI(1))},
	"udb.services.v1.DataBroker/ScanProjectionDrift":     {dctx(), fld("project_id", seed("project")), fld("message_type", seed("message_type")), fld("scan_mode", litS("sample")), fld("rows_per_target", litI(100)), fld("limit", litI(10))},
	"udb.services.v1.DataBroker/ListSagas":               {dctx(), fld("limit", litI(50))},
	"udb.services.v1.DataBroker/GetSaga":                 {dctx(), fld("saga_id", seed("saga_id"))},
	"udb.services.v1.DataBroker/RetrySagaCompensation":   {dctx(), fld("saga_id", seed("saga_id")), fld("reason", litS("retry"))},
	"udb.services.v1.DataBroker/MarkSagaReviewed":        {dctx(), fld("saga_id", seed("saga_id")), fld("reason", litS("reviewed"))},
	"udb.services.v1.DataBroker/ListPolicies":            {dctx(), fld("include_disabled", litB(false)), fld("limit", litI(50))},
	"udb.services.v1.DataBroker/PutPolicy":               {dctx(), fld("policy", sub(fld("effect", litS("allow")), fld("service_identity", seed("user_id")), fld("tenant_id", seed("tenant_id")), fld("message_type", seed("message_type")), fld("operation", litS("read")), fld("required_scope", litS("udb:read")), fld("priority", litI(100)), fld("enabled", litB(true))))},
	"udb.services.v1.DataBroker/DeletePolicy":            {dctx(), fld("policy_id", litI(1))},
	"udb.services.v1.DataBroker/ReloadPolicies":          {dctx(), fld("project_id", seed("project"))},
	"udb.services.v1.DataBroker/LintPolicies":            {dctx(), fld("project_id", seed("project"))},
	"udb.services.v1.DataBroker/GetCapabilities":         {dctx(), fld("project_id", seed("project"))},
	"udb.services.v1.DataBroker/GetCatalogManifest":      {dctx(), fld("redact", litB(false))},
	"udb.services.v1.DataBroker/LookupMessageSchema":     {dctx(), fld("project_id", seed("project")), fld("message_type", seed("message_type"))},
	"udb.services.v1.DataBroker/ListMessageSchemas":      {dctx(), fld("project_id", seed("project"))},
	"udb.services.v1.DataBroker/GetHealthReport":         {dctx(), fld("with_probes", litB(false)), fld("project_id", seed("project"))},
	"udb.services.v1.DataBroker/EnsureProject":           {dctx("udb:admin"), fld("project_id", seed("project")), fld("name", litS("My Project")), fld("cdc_topic_prefix", litS("perf."))},
	"udb.services.v1.DataBroker/ListProjects":            {dctx("udb:admin"), fld("limit", litI(50))},
	"udb.services.v1.DataBroker/GetAdminSummary":         {dctx("udb:admin"), fld("project_id", seed("project")), fld("with_probes", litB(false)), fld("redact", litB(false))},
	"udb.services.v1.DataBroker/ListAdminAuditLogs":      {dctx("udb:admin"), fld("limit", litI(50)), fld("redact", litB(false))},
	"udb.services.v1.DataBroker/VerifyAdminAuditLog":     {dctx("udb:admin"), fld("limit", litI(0))},
}
