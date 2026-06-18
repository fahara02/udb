package udbclient

// Descriptor-driven deepening of the full-surface RPC probe. The base probe
// proves every one of the ~265 RPCs is mounted; this upgrades it so that every
// side-effect-free READ RPC (Get/List/Lookup/Validate/…) is sent a correctly
// typed, field-POPULATED request resolved from its proto descriptor — exercising
// real request decode + tenant-scope/validation + handler-entry logic across the
// whole surface, not just an empty mount ping. Mutations keep a default-valued
// request so the probe never triggers a destructive side effect (a populated
// PutPolicy/DropResource/RollbackCatalog could corrupt broker state).

import (
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

// resolveMethodDesc maps "/pkg.Service/Method" → its MethodDescriptor via the
// global proto registry (populated by importing the generated packages).
func resolveMethodDesc(fullMethod string) protoreflect.MethodDescriptor {
	s := strings.TrimPrefix(fullMethod, "/")
	i := strings.LastIndex(s, "/")
	if i < 1 {
		return nil
	}
	svc, meth := s[:i], s[i+1:]
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(svc))
	if err != nil {
		return nil
	}
	sd, ok := d.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil
	}
	m := sd.Methods().ByName(protoreflect.Name(meth))
	if m == nil {
		return nil
	}
	return m
}

// probeRPCDestructive reports whether an RPC is classified DESTRUCTIVE by the
// proto-derived operation_kind on the generated client (RPCInfo.OperationKind) —
// the SINGLE authoritative source, never a hardcoded name list. A destructive RPC
// (abac PutPolicy flips the data plane to default-deny; catalog/policy-version
// mutations rewrite the active config; the revoke-all/emergency/reset family can
// lock out the admin) would corrupt shared state if field-populated, so it is
// sent a typed-empty request: the handler still decodes + validates, but the
// destructive action never executes.
func probeRPCDestructive(fullMethod string) bool {
	r, ok := LookupRPC(fullMethod)
	return ok && r.OperationKind == "destructive"
}

// probeRequestIsPopulated reports whether buildProbeMessages field-populates this
// RPC's request (every RPC except the DESTRUCTIVE ones, so the deepening covers
// the WHOLE surface — used by the test's coverage assertion).
func probeRequestIsPopulated(fullMethod string) bool {
	md := resolveMethodDesc(fullMethod)
	return md != nil && !probeRPCDestructive(fullMethod)
}

// buildProbeMessages returns a typed (input, output) pair for a probe. Every RPC
// gets a correctly-typed request; all but the DESTRUCTIVE ones are also
// field-populated so the probe exercises real decode + validation + handler
// logic across the FULL surface. Falls back to Empty when the descriptor can't
// be resolved.
func buildProbeMessages(fullMethod, tenant, project string) (proto.Message, proto.Message) {
	return buildSeededProbeMessages(fullMethod, tenant, project, nil)
}

// buildSeededProbeMessages is buildProbeMessages with an optional fixture map: a
// non-nil fix lets populateProbeMessage resolve reference/ID fields (user_id,
// role, policy_id, file_id, subject, object, …) to REAL seeded identifiers so the
// request drives the RPC's SUCCESS path instead of failing on a placeholder.
// fix==nil reproduces the original behavior (generic placeholders) for the
// reachability-probe callers.
func buildSeededProbeMessages(fullMethod, tenant, project string, fix *perfFixtures) (proto.Message, proto.Message) {
	md := resolveMethodDesc(fullMethod)
	if md == nil {
		return &emptypb.Empty{}, &emptypb.Empty{}
	}
	in := dynamicpb.NewMessage(md.Input())
	out := dynamicpb.NewMessage(md.Output())
	// In the seeded (perf) path even destructive RPCs are driven with valid
	// inputs — the perf harness seeds throwaway entities and targets them, so the
	// destructive action runs against a real, disposable target rather than being
	// suppressed with a typed-empty body. fix==nil keeps the conformance-probe
	// rule of never populating a destructive request.
	if fix != nil || !probeRPCDestructive(fullMethod) {
		populateProbeMessage(in, tenant, project, 0, fix)
	}
	return in, out
}

func populateProbeMessage(m protoreflect.Message, tenant, project string, depth int, fix *perfFixtures) {
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		if f.IsList() || f.IsMap() {
			continue
		}
		switch f.Kind() {
		case protoreflect.StringKind:
			m.Set(f, protoreflect.ValueOfString(probeString(string(f.Name()), tenant, project, fix)))
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
			m.Set(f, protoreflect.ValueOfInt32(1))
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
			m.Set(f, protoreflect.ValueOfUint32(1))
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			m.Set(f, protoreflect.ValueOfInt64(1))
		case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			m.Set(f, protoreflect.ValueOfUint64(1))
		case protoreflect.DoubleKind:
			m.Set(f, protoreflect.ValueOfFloat64(1))
		case protoreflect.FloatKind:
			m.Set(f, protoreflect.ValueOfFloat32(1))
		case protoreflect.EnumKind:
			// Pick the first non-zero enum value when one exists (a zero enum is
			// often UNSPECIFIED and rejected by validators); else leave default.
			if vals := f.Enum().Values(); vals.Len() > 1 {
				m.Set(f, protoreflect.ValueOfEnum(vals.Get(1).Number()))
			}
		case protoreflect.MessageKind, protoreflect.GroupKind:
			full := string(f.Message().FullName())
			switch {
			case full == "udb.entity.v1.RequestContext" || full == "udb.core.common.v1.RequestContext":
				populateProbeContext(m.Mutable(f).Message(), full, tenant, project)
			case strings.HasPrefix(full, "google.protobuf."):
				// well-known types (Struct/Timestamp/Any/…): default is valid
			default:
				if depth < 1 {
					populateProbeMessage(m.Mutable(f).Message(), tenant, project, depth+1, fix)
				}
			}
		}
	}
}

func populateProbeContext(m protoreflect.Message, full, tenant, project string) {
	setStr := func(msg protoreflect.Message, name, val string) {
		f := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
		if f != nil && f.Kind() == protoreflect.StringKind {
			msg.Set(f, protoreflect.ValueOfString(val))
		}
	}
	if full == "udb.core.common.v1.RequestContext" {
		// control-plane context carries a nested tenant {tenant_id, project_id}.
		if tf := m.Descriptor().Fields().ByName("tenant"); tf != nil && tf.Kind() == protoreflect.MessageKind {
			tc := m.Mutable(tf).Message()
			setStr(tc, "tenant_id", tenant)
			setStr(tc, "project_id", project)
		}
		setStr(m, "purpose", "go.live.probe")
		return
	}
	// data-plane entity.v1.RequestContext: flat tenant_id/project_id.
	setStr(m, "tenant_id", tenant)
	setStr(m, "project_id", project)
	setStr(m, "purpose", "go.live.probe")
}

func probeString(name, tenant, project string, fix *perfFixtures) string {
	n := strings.ToLower(name)
	// Seeded reference/ID/constrained fields resolve to REAL values so the request
	// drives the RPC's success path. The fixture map is consulted FIRST (by exact
	// then suffix match), then the constrained-format heuristics, then the generic
	// scalar fallback.
	if fix != nil {
		if v, ok := fix.lookup(n); ok {
			return v
		}
	}
	switch {
	case strings.Contains(n, "tenant"):
		return tenant
	case strings.Contains(n, "project"):
		return project
	case n == "message_type" || strings.Contains(n, "messagetype"):
		return liveMessageType
	case strings.Contains(n, "domain"):
		return tenant
	case strings.Contains(n, "purpose"):
		return "go.live.probe"
	case strings.Contains(n, "page_token") || strings.Contains(n, "pagetoken"):
		return ""
	case strings.Contains(n, "email"):
		return "sdk-live-probe@example.com"
	case n == "url" || strings.HasSuffix(n, "_url") || strings.Contains(n, "issuer") || strings.Contains(n, "jwks"):
		return "https://idp.example/sdk-live"
	default:
		return "sdk-live-probe"
	}
}
