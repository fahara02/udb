package udbclient

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	commonv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
)

type methodDocsContract struct {
	ServiceCount int `json:"service_count"`
	Services     []struct {
		Service string `json:"service"`
		Native  *struct {
			Data    bool `json:"public_listener_allowed"`
			Control bool `json:"control_plane_listener_allowed"`
			Peer    bool `json:"peer_listener_allowed"`
		} `json:"native_service"`
		RPCCount int `json:"rpc_count"`
		RPCs     []struct {
			Path      string   `json:"path"`
			Kind      string   `json:"kind"`
			Operation string   `json:"operation_kind"`
			AuthMode  string   `json:"auth_mode"`
			Scopes    []string `json:"scopes"`
			Endpoint  *struct {
				Internal bool    `json:"internal_grpc_only"`
				Allowed  []int32 `json:"allowed_credential_types"`
				Key      bool    `json:"idempotency_required"`
			} `json:"endpoint_security"`
			Idempotency *struct {
				RequestKey string `json:"request_key_field"`
				ServerKey  bool   `json:"server_generated_key"`
				Duplicate  string `json:"duplicate_response_field"`
				ReplaySafe bool   `json:"replay_safe"`
			} `json:"idempotency_contract"`
		} `json:"rpcs"`
	} `json:"services"`
}

func methodDocsRepo(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate repository source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../../.."))
	if _, err := os.Stat(filepath.Join(root, "docs/generated/udb-native-contract.json")); err != nil {
		if os.IsNotExist(err) && os.Getenv("CI") == "" {
			t.Skip("repository generation proof requires the canonical contract outside the SDK module")
		}
		t.Fatal(err)
	}
	return root
}

// Parse the actual typed clients, rather than the generic metadata facade or a
// template that does not produce these files. Every contract block must belong
// to the corresponding interface method's godoc.
func literalMethodDocs(t *testing.T, root string) map[string]string {
	t.Helper()
	docs := make(map[string]string)
	err := filepath.WalkDir(filepath.Join(root, "sdk/go/gen/udb"), func(file string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(file, "_grpc.pb.go") {
			return err
		}
		source, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ParseComments)
		if err != nil {
			return err
		}
		services := make(map[string]string)
		ast.Inspect(source, func(node ast.Node) bool {
			value, ok := node.(*ast.ValueSpec)
			if !ok {
				return true
			}
			for i, name := range value.Names {
				if !strings.HasSuffix(name.Name, "_FullMethodName") || i >= len(value.Values) {
					continue
				}
				literal, ok := value.Values[i].(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				wire, err := strconv.Unquote(literal.Value)
				if err != nil || !strings.HasPrefix(wire, "/udb.") {
					continue
				}
				slash := strings.LastIndex(wire, "/")
				method := wire[slash+1:]
				prefix := strings.TrimSuffix(name.Name, "_"+method+"_FullMethodName")
				client, service := prefix+"Client", wire[1:slash]
				if old := services[client]; old != "" && old != service {
					t.Errorf("ambiguous service for %s in %s", client, file)
				}
				services[client] = service
			}
			return true
		})
		ast.Inspect(source, func(node ast.Node) bool {
			typ, ok := node.(*ast.TypeSpec)
			if !ok || !strings.HasSuffix(typ.Name.Name, "Client") {
				return true
			}
			iface, ok := typ.Type.(*ast.InterfaceType)
			if !ok {
				return true
			}
			service := services[typ.Name.Name]
			if service == "" {
				t.Errorf("no wire identity for %s", typ.Name.Name)
				return true
			}
			for _, method := range iface.Methods.List {
				if len(method.Names) != 1 {
					t.Errorf("unexpected embedded client method in %s", file)
					continue
				}
				wire := "/" + service + "/" + method.Names[0].Name
				comment := method.Doc.Text()
				start := strings.Index(comment, "UDB contract: ")
				end := strings.Index(comment, "End UDB contract.")
				if start < 0 || end < start || strings.Count(comment, "UDB contract: ") != 1 {
					t.Errorf("%s lacks exactly one method godoc contract block", wire)
					continue
				}
				if _, duplicate := docs[wire]; duplicate {
					t.Errorf("duplicate typed client method %s", wire)
				}
				docs[wire] = comment[start : end+len("End UDB contract.")]
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return docs
}

func TestGeneratedRPCMethodDocsMatchCanonicalContract(t *testing.T) {
	root := methodDocsRepo(t)
	docs := literalMethodDocs(t, root)
	data, err := os.ReadFile(filepath.Join(root, "docs/generated/udb-native-contract.json"))
	if err != nil {
		t.Fatal(err)
	}
	var contract methodDocsContract
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatal(err)
	}
	if contract.ServiceCount != len(contract.Services) {
		t.Fatal("canonical service count drift")
	}
	seen := make(map[string]bool)
	for _, service := range contract.Services {
		if service.RPCCount != len(service.RPCs) {
			t.Errorf("canonical RPC count drift for %s", service.Service)
		}
		for _, rpc := range service.RPCs {
			if seen[rpc.Path] {
				t.Errorf("duplicate canonical method %s", rpc.Path)
			}
			seen[rpc.Path] = true
			doc, exists := docs[rpc.Path]
			if !exists {
				t.Errorf("missing method godoc for %s", rpc.Path)
				continue
			}
			want := []string{"UDB contract: " + rpc.Path, "Operation kind: " + rpc.Operation + "."}
			var listeners []string
			if rpc.Endpoint != nil && rpc.Endpoint.Internal {
				listeners = []string{"internal loopback"}
			} else if service.Native == nil && service.Service == "udb.services.v1.DataBroker" {
				listeners = []string{"data plane"}
			} else if service.Native != nil {
				if service.Native.Data {
					listeners = append(listeners, "data plane")
				}
				if service.Native.Control {
					listeners = append(listeners, "control plane")
				}
				if service.Native.Peer {
					listeners = append(listeners, "peer")
				}
			}
			if len(listeners) == 0 {
				t.Errorf("canonical listener missing for %s", rpc.Path)
			}
			want = append(want, "Listener: "+strings.Join(listeners, ", ")+".")
			scopes := append([]string(nil), rpc.Scopes...)
			sort.Strings(scopes)
			scopeText := "listener and resource policy defaults (no explicit endpoint scopes)"
			if len(scopes) > 0 {
				scopeText = strings.Join(scopes, ", ")
			} else if rpc.AuthMode == "public" {
				scopeText = "no endpoint scopes (PUBLIC); method-specific authorization still applies"
			}
			want = append(want, "Scopes: "+scopeText+".")
			var credentials []string
			if rpc.Endpoint != nil {
				allowed := append([]int32(nil), rpc.Endpoint.Allowed...)
				sort.Slice(allowed, func(i, j int) bool { return allowed[i] < allowed[j] })
				for _, code := range allowed {
					name, ok := commonv1.CredentialType_name[code]
					if !ok {
						t.Fatalf("unknown credential type %d in %s", code, rpc.Path)
					}
					credentials = append(credentials, fmt.Sprintf("%s (%d)", strings.TrimPrefix(name, "CREDENTIAL_TYPE_"), code))
				}
			}
			credentialText := "listener credential policy defaults (no explicit allowlist)"
			if len(credentials) > 0 {
				credentialText = strings.Join(credentials, ", ")
			} else if rpc.AuthMode == "public" {
				credentialText = "PUBLIC endpoint; method-specific request credentials"
			}
			want = append(want, "Credential types: "+credentialText+".")
			retry := "no automatic mutation replay; a server-generated key alone does not permit retry."
			if rpc.Kind != "unary" {
				retry = "streams are not automatically replayed; use the RPC resume contract."
			} else if rpc.Operation == "read_only" {
				retry = "read-only unary calls may retry transient failures."
			} else if rpc.Idempotency != nil && rpc.Idempotency.ReplaySafe && rpc.Idempotency.RequestKey != "" {
				retry = "automatic transient retry requires the declared request key; reuse it only for unchanged request semantics and the same tenant/project."
			}
			want = append(want, "Idempotency: "+retry)
			fields := "Idempotency fields: no declared method replay contract."
			if key := rpc.Idempotency; key != nil {
				request, duplicate := key.RequestKey, key.Duplicate
				if request == "" {
					request = "none"
				}
				if duplicate == "" {
					duplicate = "none"
				}
				fields = fmt.Sprintf("Idempotency fields: request key=%s; server-generated key=%t; duplicate response=%s; replay-safe=%t.", request, key.ServerKey, duplicate, key.ReplaySafe)
			}
			want = append(want, fields)
			if rpc.Endpoint != nil && rpc.Endpoint.Key {
				want = append(want, "The endpoint requires an idempotency key.")
			}
			for _, line := range want {
				if !strings.Contains("\n"+doc+"\n", "\n"+line+"\n") {
					t.Errorf("%s godoc missing canonical line %q", rpc.Path, line)
				}
			}
		}
	}
	for wire := range docs {
		if !seen[wire] {
			t.Errorf("generated method has no canonical contract: %s", wire)
		}
	}
	if len(docs) != len(seen) {
		t.Errorf("documented methods=%d, canonical methods=%d", len(docs), len(seen))
	}
}

func TestGeneratedRPCMethodDocsSnapshots(t *testing.T) {
	docs := literalMethodDocs(t, methodDocsRepo(t))
	cases := map[string]string{
		"/udb.services.v1.DataBroker/Select": `UDB contract: /udb.services.v1.DataBroker/Select
Listener: data plane.
Scopes: listener and resource policy defaults (no explicit endpoint scopes).
Credential types: listener credential policy defaults (no explicit allowlist).
Operation kind: read_only.
Idempotency: read-only unary calls may retry transient failures.
Idempotency fields: no declared method replay contract.
End UDB contract.`,
		"/udb.services.v1.DataBroker/Upsert": `UDB contract: /udb.services.v1.DataBroker/Upsert
Listener: data plane.
Scopes: listener and resource policy defaults (no explicit endpoint scopes).
Credential types: listener credential policy defaults (no explicit allowlist).
Operation kind: mutation.
Idempotency: automatic transient retry requires the declared request key; reuse it only for unchanged request semantics and the same tenant/project.
Idempotency fields: request key=idempotency_key; server-generated key=false; duplicate response=was_duplicate; replay-safe=true.
End UDB contract.`,
		"/udb.services.v1.DataBroker/BeginTx": `UDB contract: /udb.services.v1.DataBroker/BeginTx
Listener: data plane.
Scopes: listener and resource policy defaults (no explicit endpoint scopes).
Credential types: listener credential policy defaults (no explicit allowlist).
Operation kind: mutation.
Idempotency: streams are not automatically replayed; use the RPC resume contract.
Idempotency fields: no declared method replay contract.
BeginTx validates transaction guards and per-mutation keys for upsert, update, delete and vector_upsert; unsupported keyed operations are refused.
Relational mutation keys use durable replay receipts; changed replay inputs refuse the whole transaction.
End UDB contract.`,
		"/udb.core.authn.services.v1.AuthnService/Authenticate": `UDB contract: /udb.core.authn.services.v1.AuthnService/Authenticate
Listener: control plane.
Scopes: no endpoint scopes (PUBLIC); method-specific authorization still applies.
Credential types: PUBLIC endpoint; method-specific request credentials.
Operation kind: read_only.
Idempotency: read-only unary calls may retry transient failures.
Idempotency fields: no declared method replay contract.
End UDB contract.`,
		"/udb.core.authz.services.v1.AuthzService/Authorize": `UDB contract: /udb.core.authz.services.v1.AuthzService/Authorize
Listener: control plane.
Scopes: udb:authz:authorize.
Credential types: BEARER_JWT (1), SESSION (2), SERVICE_ACCOUNT (4).
Operation kind: read_only.
Idempotency: read-only unary calls may retry transient failures.
Idempotency fields: no declared method replay contract.
End UDB contract.`,
		"/udb.core.tenant.services.v1.TenantService/AdminPurgeTenant": `UDB contract: /udb.core.tenant.services.v1.TenantService/AdminPurgeTenant
Listener: control plane.
Scopes: udb:tenant:admin-purge.
Credential types: BEARER_JWT (1), SESSION (2).
Operation kind: destructive.
Idempotency: no automatic mutation replay; a server-generated key alone does not permit retry.
Idempotency fields: no declared method replay contract.
The endpoint requires an idempotency key.
End UDB contract.`,
		"/udb.core.control.services.v1.ControlPlaneService/DeltaResources": `UDB contract: /udb.core.control.services.v1.ControlPlaneService/DeltaResources
Listener: internal loopback.
Scopes: udb:control:delta-resources.
Credential types: BEARER_JWT (1), SERVICE_ACCOUNT (4).
Operation kind: mutation.
Idempotency: streams are not automatically replayed; use the RPC resume contract.
Idempotency fields: no declared method replay contract.
End UDB contract.`,
		"/udb.core.webrtc.services.v1.PeerService/GetPeer": `UDB contract: /udb.core.webrtc.services.v1.PeerService/GetPeer
Listener: control plane, peer.
Scopes: udb:webrtc:peer:get-peer.
Credential types: BEARER_JWT (1), SESSION (2).
Operation kind: read_only.
Idempotency: read-only unary calls may retry transient failures.
Idempotency fields: no declared method replay contract.
End UDB contract.`,
	}
	for wire, want := range cases {
		if got := docs[wire]; got != want {
			t.Errorf("%s method godoc:\n%s\nwant:\n%s", wire, got, want)
		}
	}
}

// Exercise the same source producer used by CI, including its fail-closed
// metadata/coverage checks. SDK consumers do not need Node to use the library.
func TestGeneratedRPCMethodDocsProducerRefusesInvalidContracts(t *testing.T) {
	root := methodDocsRepo(t)
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv("CI") == "" {
			t.Skip("repository source-producer proof requires Node")
		}
		t.Fatal(err)
	}
	const script = `
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
const root = process.argv[1];
const { GoRPCContractDocs, credentialTypeNames } = await import(pathToFileURL(path.join(root, "scripts/go-rpc-contract-docs.mjs")));
const read = (file) => fs.readFileSync(path.join(root, file), "utf8");
const contract = JSON.parse(read("docs/generated/udb-native-contract.json"));
const names = credentialTypeNames(read("sdk/go/gen/udb/core/common/v1/security.pb.go"));
const source = read("sdk/go/gen/udb/services/v1/data_broker_grpc.pb.go");
const refuse = (name, run) => { let refused = false; try { run(); } catch { refused = true; } if (!refused) throw new Error(name + " did not refuse"); };
refuse("missing enum", () => credentialTypeNames(""));
refuse("service count", () => new GoRPCContractDocs({ ...contract, service_count: -1 }, names));
const duplicate = structuredClone(contract);
duplicate.services[0].rpcs.push(duplicate.services[0].rpcs[0]); duplicate.services[0].rpc_count++;
refuse("duplicate canonical method", () => new GoRPCContractDocs(duplicate, names));
const unknown = structuredClone(contract);
unknown.services[0].rpcs[0].endpoint_security = { allowed_credential_types: [9999] };
refuse("unknown credential", () => new GoRPCContractDocs(unknown, names));
const unsafeText = structuredClone(contract); unsafeText.services[0].rpcs[0].scopes = ["scope\nforged comment"];
refuse("unsafe comment text", () => new GoRPCContractDocs(unsafeText, names));
refuse("missing generated methods", () => new GoRPCContractDocs(contract, names).finish());
refuse("unknown generated method", () => new GoRPCContractDocs(contract, names).annotate(source.replace(/\tSelect\(/u, "\tUnlisted("), "fixture"));
refuse("duplicate generated method", () => { const producer = new GoRPCContractDocs(contract, names); producer.annotate(source, "first"); producer.annotate(source, "second"); });
`
	if output, err := exec.Command(node, "--input-type=module", "-e", script, root).CombinedOutput(); err != nil {
		t.Fatalf("canonical source producer refusal proof: %v\n%s", err, output)
	}
}
