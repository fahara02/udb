package conformance

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"strings"
	"testing"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/protobuf/proto"
)

// The cross-SDK typed-store cases (sdk/conformance/table-cases.json): the same
// file drives the TypeScript runner, so the SDKs cannot drift apart.

type caseFile struct {
	Cases []tableCase `json:"cases"`
}

type tableCase struct {
	Name  string     `json:"name"`
	Steps []caseStep `json:"steps"`
}

type caseStep struct {
	Op       string           `json:"op"`
	Key      map[string]any   `json:"key"`
	Row      map[string]any   `json:"row"`
	Rows     []map[string]any `json:"rows"`
	Changes  map[string]any   `json:"changes"`
	Expected map[string]any   `json:"expected"`
	Expect   struct {
		Error        string         `json:"error"`
		Row          map[string]any `json:"row"`
		AffectedRows *int64         `json:"affected_rows"`
	} `json:"expect"`
}

// numberedID matches the "$id<digits>" placeholders ("$id2", "$id3", ...).
var numberedID = regexp.MustCompile(`\$id[0-9]+`)

// placeholders resolves a case's ids: "$id" is one fresh UUID per case and
// each "$id<digits>" its own fresh UUID per case (the numbered ones are
// replaced first, so "$id2" never becomes "<uuid>2").
type placeholders struct {
	id       string
	numbered map[string]string
}

func newPlaceholders() *placeholders {
	return &placeholders{id: NewID(), numbered: map[string]string{}}
}

func (p *placeholders) replace(s string) string {
	s = numberedID.ReplaceAllStringFunc(s, func(name string) string {
		id, ok := p.numbered[name]
		if !ok {
			id = NewID()
			p.numbered[name] = id
		}
		return id
	})
	return strings.ReplaceAll(s, "$id", p.id)
}

// substitute replaces the id placeholders in every string value.
func substitute(values map[string]any, ids *placeholders) map[string]any {
	out := make(map[string]any, len(values))
	for k, v := range values {
		if s, ok := v.(string); ok {
			v = ids.replace(s)
		}
		out[k] = v
	}
	return out
}

// errorName maps a store error onto the cases' closed error vocabulary.
func errorName(err error) string {
	if err == nil {
		return ""
	}
	return udbclient.CodeOf(err).String()
}

// RunTableCases runs every case in the shared JSON file against connect.
func RunTableCases(t *testing.T, connect Connect, path string) {
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var file caseFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, tc := range file.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := context.Background()
			u := connect(t, newTenant())
			files := udbclient.TableOf[*storagev1.File](u)
			id := newPlaceholders()
			for index, step := range tc.Steps {
				key := udbclient.RowKey(substitute(step.Key, id))
				var got *storagev1.File
				switch step.Op {
				case "upsert":
					row := &storagev1.File{}
					if err = udbclient.DecodeRecord(udbclient.Record(substitute(step.Row, id)), row); err == nil {
						err = files.Upsert(ctx, row)
					}
				case "get":
					got, err = files.Get(ctx, key)
				case "patch":
					err = files.Patch(ctx, key, udbclient.Record(substitute(step.Changes, id)))
				case "update_if":
					var current *storagev1.File
					if current, err = files.Get(ctx, key); err == nil {
						next := proto.Clone(current).(*storagev1.File)
						for field, value := range substitute(step.Changes, id) {
							if err = udbclient.DecodeField(next, field, value); err != nil {
								break
							}
						}
						if err == nil {
							err = files.UpdateIf(ctx, next, udbclient.Record(substitute(step.Expected, id)))
						}
					}
				case "delete":
					err = files.Delete(ctx, key)
				case "bulk_upsert":
					rows := make([]*storagev1.File, 0, len(step.Rows))
					for _, values := range step.Rows {
						row := &storagev1.File{}
						if err = udbclient.DecodeRecord(udbclient.Record(substitute(values, id)), row); err != nil {
							break
						}
						rows = append(rows, row)
					}
					if err == nil {
						var res *entityv1.BulkUpsertResponse
						if res, err = files.BulkUpsert(ctx, rows, udbclient.BulkUpsertBatchOptions{}); err == nil &&
							step.Expect.AffectedRows != nil && res.GetAffectedRows() != *step.Expect.AffectedRows {
							t.Fatalf("step %d (%s): affected_rows %d, want %d", index, step.Op, res.GetAffectedRows(), *step.Expect.AffectedRows)
						}
					}
				default:
					t.Fatalf("step %d: unknown op %q", index, step.Op)
				}
				if name := errorName(err); name != step.Expect.Error {
					t.Fatalf("step %d (%s): error %q (%v), want %q", index, step.Op, name, err, step.Expect.Error)
				}
				if got != nil && step.Expect.Row != nil {
					rec, encErr := udbclient.EncodeRecord(got, udbclient.WriteInsert)
					if encErr != nil {
						t.Fatalf("step %d: encode: %v", index, encErr)
					}
					for field, want := range substitute(step.Expect.Row, id) {
						if gotValue := rec[field]; gotValue != want {
							t.Fatalf("step %d: %s = %v, want %v", index, field, gotValue, want)
						}
					}
				}
			}
		})
	}
}
