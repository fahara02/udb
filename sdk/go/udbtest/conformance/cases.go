package conformance

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
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
	Op       string         `json:"op"`
	Key      map[string]any `json:"key"`
	Row      map[string]any `json:"row"`
	Changes  map[string]any `json:"changes"`
	Expected map[string]any `json:"expected"`
	Expect   struct {
		Error string         `json:"error"`
		Row   map[string]any `json:"row"`
	} `json:"expect"`
}

// substitute replaces "$id" in every string value.
func substitute(values map[string]any, id string) map[string]any {
	out := make(map[string]any, len(values))
	for k, v := range values {
		if s, ok := v.(string); ok {
			v = strings.ReplaceAll(s, "$id", id)
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
			id := NewID()
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
