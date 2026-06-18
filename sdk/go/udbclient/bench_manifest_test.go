package udbclient

// Bench-body manifest consumer + cross-consistency contract (chapter 11.1.2 /
// 11.1.5 Go items).
//
// The shared manifest docs/bench-bodies/*.md is the SINGLE SOURCE OF TRUTH for
// which RPCs carry a documented bench request body. The column contract is
// documented in data_broker.md: col1=done, col2=RPC, col3=op_kind,
// col4=request msg, col5=valid body, col6=notes. The four language bench-body
// consumers (Go loadBenchBodyRows, TS loadBenchBodyRows, PHP loadBenchBodyRows,
// Python bench_body_rows) all key on col2/col5 and must total exactly 265 rows.
//
// HONEST RESIDUAL (11.1.2.2/11.1.2.3 decision): the manifest col5 body cell is
// free-form markdown prose (e.g. `username`=`<seed:username>`, `password`="X"),
// NOT machine-readable JSON. A faithful generic markdown -> dynamicpb hydration
// would lose fidelity for the control-plane services — the typed perfBodySpecs
// encode real enum values (e.g. DEVICE_TYPE_API), nested actor/context scope
// sub-messages, and seeded reference IDs that the prose cannot reliably yield,
// and which the broker requires (it rejects placeholder bodies with
// INVALID_ARGUMENT / PERMISSION_DENIED). So perfBodySpecs is KEPT as the typed
// realization, and the manifest is enforced as the CONTRACT via the
// cross-consistency tests below: a manifest row with no Go body, or a Go body
// with no manifest row, fails `go test`. The two copies cannot silently drift.

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

// expectedBenchBodyRows is the contract total (chapter 11.1.5.1). It must equal
// the live RPC surface (len(AllRPCs)); both are asserted to match below.
const expectedBenchBodyRows = 265

// benchBodyEntry mirrors one object in docs/generated/bench-bodies.json.
type benchBodyEntry struct {
	RPC        string `json:"rpc"`
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

// loadBenchBodyRows reads the generated docs/generated/bench-bodies.json and maps
// the bare RPC method name (rpc's last dotted segment — webrtc uses
// `Service.Method`, others use a bare `Method`) to the body cell. It t.Fatalf's
// unless exactly expectedBenchBodyRows rows load (11.1.5.1, the count==265
// assertion). The bare-name keys are unique across the whole corpus (verified),
// so the returned map has exactly that many entries.
func loadBenchBodyRows(t *testing.T) map[string]string {
	t.Helper()
	entries := loadBenchBodyEntries(t)
	rows := map[string]string{}
	for _, e := range entries {
		if e.RPC == "" {
			t.Fatalf("manifest entry with empty RPC name: %+v", e)
		}
		// Key on the bare method name (last dotted segment).
		name := e.RPC
		if i := strings.LastIndex(e.RPC, "."); i >= 0 {
			name = e.RPC[i+1:]
		}
		if prev, dup := rows[name]; dup {
			t.Fatalf("duplicate bench-body RPC name %q (%q vs %q) — bare names must be unique across the corpus", name, prev, e.Body)
		}
		rows[name] = e.Body
	}
	if len(rows) != expectedBenchBodyRows {
		t.Fatalf("bench-body manifest has %d rows, want exactly %d (chapter 11.1.5.1)", len(rows), expectedBenchBodyRows)
	}
	return rows
}

// parseBenchBodyMarkdown walks docs/bench-bodies/*.md and maps the bare RPC method
// name to the col5 body cell — the LEGACY markdown parse, retained ONLY to power
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
			name := rpc
			if i := strings.LastIndex(rpc, "."); i >= 0 {
				name = rpc[i+1:]
			}
			rows[name] = body
			total++
		}
	}
	if total != expectedBenchBodyRows {
		t.Fatalf("bench-body markdown has %d rows, want exactly %d", total, expectedBenchBodyRows)
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
// the 265 count on both sides.
func TestBenchBodyManifestMatchesMarkdown(t *testing.T) {
	fromJSON := loadBenchBodyRows(t)
	fromMD := parseBenchBodyMarkdown(t)
	if len(fromJSON) != expectedBenchBodyRows {
		t.Fatalf("JSON manifest has %d rows, want %d", len(fromJSON), expectedBenchBodyRows)
	}
	if len(fromMD) != expectedBenchBodyRows {
		t.Fatalf("markdown manifest has %d rows, want %d", len(fromMD), expectedBenchBodyRows)
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
	if len(rows) != expectedBenchBodyRows {
		t.Fatalf("loaded %d unique bench-body rows, want %d", len(rows), expectedBenchBodyRows)
	}
	if len(AllRPCs) != expectedBenchBodyRows {
		t.Fatalf("AllRPCs surface is %d but the bench-body manifest expects %d — they must match", len(AllRPCs), expectedBenchBodyRows)
	}
}

// TestBenchBodyManifestMatchesSurface is the cross-consistency contract
// (11.1.2.x): the manifest and the live RPC surface must be in exact bijection by
// bare method name. A manifest row whose RPC is not in AllRPCs (stale/renamed),
// or an AllRPCs RPC with no manifest row (a body added in code but not documented
// as the contract), fails `go test` — so the manifest cannot drift from the surface.
func TestBenchBodyManifestMatchesSurface(t *testing.T) {
	rows := loadBenchBodyRows(t)
	surface := map[string]bool{}
	for _, rpc := range AllRPCs {
		surface[rpc.Name] = true
	}
	var manifestNotInSurface, surfaceNotInManifest []string
	for name := range rows {
		if !surface[name] {
			manifestNotInSurface = append(manifestNotInSurface, name)
		}
	}
	for name := range surface {
		if _, ok := rows[name]; !ok {
			surfaceNotInManifest = append(surfaceNotInManifest, name)
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

// TestBenchBodySpecsHaveManifestRow asserts every typed control-plane perfBodySpecs
// key (the realization) has a corresponding manifest row (the contract). This is
// the spec->manifest direction of 11.1.2.2: the typed specs may not encode a body
// the manifest does not document. (The reverse direction — every manifest row has
// a Go body — is covered by TestLivePerfExplicitBodyCoverage, which iterates the
// full 265-RPC surface and fails on any NO-BODY gap.)
func TestBenchBodySpecsHaveManifestRow(t *testing.T) {
	rows := loadBenchBodyRows(t)
	var missing []string
	for key := range perfBodySpecs {
		// key form: "Pkg.Service/Method" — take the bare method after "/".
		name := key
		if i := strings.LastIndex(key, "/"); i >= 0 {
			name = key[i+1:]
		}
		if _, ok := rows[name]; !ok {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("perfBodySpecs keys with no bench-body manifest row: %s", strings.Join(missing, ", "))
	}
}
