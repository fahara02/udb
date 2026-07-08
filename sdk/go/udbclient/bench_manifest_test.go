package udbclient

// Bench-body manifest consumer + cross-consistency contract (chapter 11.1.2 /
// 11.1.5 Go items).
//
// The shared manifest docs/bench-bodies/*.md is the SINGLE SOURCE OF TRUTH for
// which RPCs carry a documented bench request body. The column contract is
// documented in data_broker.md: col1=done, col2=RPC, col3=op_kind,
// col4=request msg, col5=valid body, col6=notes. The four language bench-body
// consumers (Go loadBenchBodyRows, TS loadBenchBodyRows, PHP loadBenchBodyRows,
// Python bench_body_rows) all key on col2/col5 and must match AllRPCs.
//
// The manifest col5 body cell is now strict JSON for every generated RPC. Go
// hydrates it directly through protojson/dynamicpb, so there is no parallel
// perfBodySpecs map to drift away from the shared docs/generated manifest.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// benchBodyManifestDir is the shared cross-SDK manifest directory, OUTSIDE this
// Go module tree (repo root is three dirs up from sdk/go/udbclient), so it
// cannot be go:embed'd — it is read at test time like the consistency golden.
const benchBodyManifestDir = "../../../docs/bench-bodies"

// benchBodiesJSONPath is the GENERATED machine-readable bench manifest
// (scripts/gen-bench-bodies-json.mjs), the new consumer source of truth. The
// markdown corpus under docs/bench-bodies/ stays the human-editable source; the
// drift test (TestBenchBodyManifestMatchesMarkdown) proves the JSON equals a
// fresh markdown parse, so the two can never diverge.
const benchBodiesJSONPath = "../../../docs/generated/bench-bodies.json"

// benchBodyEntry mirrors one object in docs/generated/bench-bodies.json.
type benchBodyEntry struct {
	RPC        string `json:"rpc"`
	Service    string `json:"service"`
	OpKind     string `json:"op_kind"`
	RequestMsg string `json:"request_msg"`
	Body       string `json:"body"`
	Notes      string `json:"notes"`
}

// loadBenchBodyEntries reads the generated JSON manifest verbatim.
func loadBenchBodyEntries(t *testing.T) []benchBodyEntry {
	t.Helper()
	raw, err := os.ReadFile(filepath.FromSlash(benchBodiesJSONPath))
	if err != nil {
		t.Fatalf("read bench-bodies.json (run `node scripts/gen-bench-bodies-json.mjs`): %v", err)
	}
	var entries []benchBodyEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatalf("parse bench-bodies.json: %v", err)
	}
	return entries
}

func duplicateRPCNames() map[string]bool {
	counts := map[string]int{}
	for _, rpc := range AllRPCs {
		counts[rpc.Name]++
	}
	out := map[string]bool{}
	for name, count := range counts {
		if count > 1 {
			out[name] = true
		}
	}
	return out
}

func rpcBenchKey(service, name string, duplicateNames map[string]bool) string {
	if duplicateNames[name] {
		return service + "." + name
	}
	return name
}

func manifestRPCKey(rpc string) string {
	if i := strings.LastIndex(rpc, "."); i >= 0 {
		return rpc
	}
	return rpc
}

func surfaceBenchKeys() map[string]bool {
	dups := duplicateRPCNames()
	keys := map[string]bool{}
	for _, rpc := range AllRPCs {
		keys[rpcBenchKey(rpc.Service, rpc.Name, dups)] = true
		keys[rpc.Service+"."+rpc.Name] = true
	}
	return keys
}

func rowHasRPC(rows map[string]string, rpc RPCInfo) bool {
	dups := duplicateRPCNames()
	if _, ok := rows[rpcBenchKey(rpc.Service, rpc.Name, dups)]; ok {
		return true
	}
	_, ok := rows[rpc.Service+"."+rpc.Name]
	return ok
}

// loadBenchBodyRows reads the generated docs/generated/bench-bodies.json and maps
// the RPC manifest key to the body cell. Unique RPC names use the bare method
// name; duplicated names use `Service.Method`.
func loadBenchBodyRows(t *testing.T) map[string]string {
	t.Helper()
	entries := loadBenchBodyEntries(t)
	rows := map[string]string{}
	for _, e := range entries {
		if e.RPC == "" {
			t.Fatalf("manifest entry with empty RPC name: %+v", e)
		}
		name := manifestRPCKey(e.RPC)
		if prev, dup := rows[name]; dup {
			t.Fatalf("duplicate bench-body RPC key %q (%q vs %q)", name, prev, e.Body)
		}
		rows[name] = e.Body
	}
	if len(rows) != len(AllRPCs) {
		t.Fatalf("bench-body manifest has %d rows, want current AllRPCs count %d (chapter 14 API surface contract)", len(rows), len(AllRPCs))
	}
	return rows
}

// parseBenchBodyMarkdown walks docs/bench-bodies/*.md and maps the manifest RPC
// key to the col5 body cell — the LEGACY markdown parse, retained ONLY to power
// the drift test that proves the generated JSON still equals a fresh parse of the
// human-editable markdown source.
func parseBenchBodyMarkdown(t *testing.T) map[string]string {
	t.Helper()
	files, err := filepath.Glob(filepath.FromSlash(benchBodyManifestDir + "/*.md"))
	if err != nil {
		t.Fatalf("glob bench-bodies: %v", err)
	}
	rows := map[string]string{}
	total := 0
	for _, f := range files {
		if strings.HasSuffix(filepath.ToSlash(f), "workflow-sequences.md") {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			line = strings.TrimRight(line, "\r")
			if !strings.HasPrefix(line, "| [") {
				continue
			}
			cols := strings.Split(line, "|")
			if len(cols) < 6 {
				t.Fatalf("malformed manifest row in %s: %q", filepath.Base(f), line)
			}
			rpc := strings.TrimSpace(cols[2])
			body := strings.TrimSpace(cols[5])
			if rpc == "" {
				t.Fatalf("manifest row with empty RPC name in %s: %q", filepath.Base(f), line)
			}
			rows[manifestRPCKey(rpc)] = body
			total++
		}
	}
	if total != len(AllRPCs) {
		t.Fatalf("bench-body markdown has %d rows, want current AllRPCs count %d", total, len(AllRPCs))
	}
	return rows
}

// workflowSequencesPath is the shared cross-SDK workflow RPC-sequence contract
// (| helper | sequence |), read at test time like the bench-body manifest.
const workflowSequencesPath = "../../../docs/bench-bodies/workflow-sequences.md"

// loadWorkflowSequence reads docs/bench-bodies/workflow-sequences.md and returns
// the exact ordered RPC sequence for one workflow helper key (col1, e.g.
// "StorageFacade.uploadFile"), splitting col2 on "," (chapter 11.2.x Go mirror).
// It t.Fatalf's if the helper is absent so a contract rename fails the test
// rather than silently asserting an empty sequence.
func loadWorkflowSequence(t *testing.T, helper string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.FromSlash(workflowSequencesPath))
	if err != nil {
		t.Fatalf("read workflow-sequences.md: %v", err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimRight(line, "\r")
		if !strings.HasPrefix(line, "|") {
			continue
		}
		cols := strings.Split(line, "|")
		if len(cols) < 4 {
			continue
		}
		key := strings.TrimSpace(cols[1])
		if key != helper {
			continue
		}
		var seq []string
		for _, part := range strings.Split(cols[2], ",") {
			if p := strings.TrimSpace(part); p != "" {
				seq = append(seq, p)
			}
		}
		if len(seq) == 0 {
			t.Fatalf("workflow helper %q has an empty sequence", helper)
		}
		return seq
	}
	t.Fatalf("workflow helper %q not found in workflow-sequences.md", helper)
	return nil
}

// TestBenchBodyManifestMatchesMarkdown is the R6.1 DRIFT gate: the generated
// docs/generated/bench-bodies.json (the consumer source of truth) must equal a
// fresh parse of the human-editable docs/bench-bodies/*.md markdown. If markdown
// is edited without regenerating the JSON (`node scripts/gen-bench-bodies-json.mjs`),
// this fails — so the two views can never silently diverge. It also re-asserts
// the AllRPCs count on both sides.
func TestBenchBodyManifestMatchesMarkdown(t *testing.T) {
	fromJSON := loadBenchBodyRows(t)
	fromMD := parseBenchBodyMarkdown(t)
	if len(fromJSON) != len(AllRPCs) {
		t.Fatalf("JSON manifest has %d rows, want current AllRPCs count %d", len(fromJSON), len(AllRPCs))
	}
	if len(fromMD) != len(AllRPCs) {
		t.Fatalf("markdown manifest has %d rows, want current AllRPCs count %d", len(fromMD), len(AllRPCs))
	}
	var diffs []string
	for name, body := range fromMD {
		if jb, ok := fromJSON[name]; !ok {
			diffs = append(diffs, "missing in JSON: "+name)
		} else if jb != body {
			diffs = append(diffs, "body mismatch for "+name)
		}
	}
	for name := range fromJSON {
		if _, ok := fromMD[name]; !ok {
			diffs = append(diffs, "stale in JSON (not in markdown): "+name)
		}
	}
	sort.Strings(diffs)
	if len(diffs) > 0 {
		t.Fatalf("bench-bodies.json drifted from markdown (run `node scripts/gen-bench-bodies-json.mjs`):\n%s", strings.Join(diffs, "\n"))
	}
}

// TestBenchBodyManifestCount is the standalone 11.1.5.1 count gate. loadBenchBodyRows
// already enforces it, but this names the assertion explicitly.
func TestBenchBodyManifestCount(t *testing.T) {
	rows := loadBenchBodyRows(t)
	if len(rows) != len(AllRPCs) {
		t.Fatalf("loaded %d unique bench-body rows, want current AllRPCs count %d", len(rows), len(AllRPCs))
	}
}

// TestBenchBodyManifestMatchesSurface is the cross-consistency contract
// (11.1.2.x): the manifest and the live RPC surface must be in exact bijection by
// manifest RPC key. A manifest row whose RPC is not in AllRPCs (stale/renamed),
// or an AllRPCs RPC with no manifest row (a body added in code but not documented
// as the contract), fails `go test` — so the manifest cannot drift from the surface.
func TestBenchBodyManifestMatchesSurface(t *testing.T) {
	rows := loadBenchBodyRows(t)
	surface := surfaceBenchKeys()
	var manifestNotInSurface, surfaceNotInManifest []string
	for name := range rows {
		if !surface[name] {
			manifestNotInSurface = append(manifestNotInSurface, name)
		}
	}
	for _, rpc := range AllRPCs {
		if !rowHasRPC(rows, rpc) {
			surfaceNotInManifest = append(surfaceNotInManifest, rpc.Service+"/"+rpc.Name)
		}
	}
	sort.Strings(manifestNotInSurface)
	sort.Strings(surfaceNotInManifest)
	if len(manifestNotInSurface) > 0 {
		t.Errorf("manifest rows with no live RPC (stale/renamed): %s", strings.Join(manifestNotInSurface, ", "))
	}
	if len(surfaceNotInManifest) > 0 {
		t.Errorf("live RPCs with no bench-body manifest row: %s", strings.Join(surfaceNotInManifest, ", "))
	}
}

// TestBenchBodyManifestRowsAreStrictJSON asserts every manifest body can be
// consumed by the shared protojson/dynamicpb path. This is the post-perfBodySpecs
// 11.1.2.3 invariant: adding a prose row reintroduces per-language drift.
func TestBenchBodyManifestRowsAreStrictJSON(t *testing.T) {
	rows := loadBenchBodyRows(t)
	var nonJSON []string
	for key, body := range rows {
		jsonBody, ok := manifestJSONBodyCell(body)
		if !ok || !json.Valid([]byte(benchBodyJSONSample(jsonBody))) {
			nonJSON = append(nonJSON, key)
		}
	}
	sort.Strings(nonJSON)
	if len(nonJSON) > 0 {
		t.Fatalf("bench-body manifest rows with non-JSON body cells: %s", strings.Join(nonJSON, ", "))
	}
}

func benchBodyJSONSample(body string) string {
	for {
		start := strings.Index(body, "<seed:")
		if start < 0 {
			return body
		}
		endRel := strings.Index(body[start:], ">")
		if endRel < 0 {
			return body
		}
		end := start + endRel
		key := strings.ToLower(strings.TrimSpace(body[start+len("<seed:") : end]))
		replacement := "x"
		if strings.HasSuffix(key, "fencing_token") || key == "gov_exp" {
			replacement = "1"
		}
		body = body[:start] + replacement + body[end+1:]
	}
}
