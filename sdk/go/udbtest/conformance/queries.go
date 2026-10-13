package conformance

import (
	"context"
	"testing"

	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// Query semantics are shared by fake, embedded and live runs. In particular,
// negating a predicate must not turn a SQL NULL into a matching row.
func runQueries(t *testing.T, connect Connect, tenant func() string) {
	t.Run("unknown columns are refused even without matching rows", func(t *testing.T) {
		u := connect(t, tenant())
		filter, _ := structpb.NewStruct(map[string]any{
			"file_id": NewID(),
			"$or":     []any{map[string]any{"not_a_declared_column": "value"}},
		})
		_, err := u.Data.Broker.Select(context.Background(), &entityv1.SelectRequest{
			MessageType: udbclient.MessageType(&storagev1.File{}), Filter: filter,
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("unknown column returned %v, want InvalidArgument", err)
		}
	})
	t.Run("nullable predicates preserve SQL three-valued logic", func(t *testing.T) {
		u := connect(t, tenant())
		files := udbclient.TableOf[*storagev1.File](u)
		ctx := context.Background()
		ids := []any{NewID(), NewID(), NewID()}
		for i, contentType := range []any{nil, "blocked/type", "allowed/type"} {
			payload, err := structpb.NewStruct(map[string]any{
				"file_id": ids[i], "filename": "nullable.pdf",
				"object_key": "conformance/" + ids[i].(string), "content_type": contentType,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := u.Data.Broker.Upsert(ctx, &entityv1.UpsertRequest{
				MessageType: udbclient.MessageType(&storagev1.File{}), Payload: payload,
			}); err != nil {
				t.Fatal(err)
			}
		}
		cases := []struct {
			name      string
			predicate udbclient.Filter
			want      []any
		}{
			{"not equal", udbclient.Filter{"$ne": "blocked/type"}, ids[2:]},
			{"not in", udbclient.Filter{"$nin": []any{"blocked/type"}}, ids[2:]},
			{"not equality", udbclient.Filter{"$not": udbclient.Filter{"$eq": "blocked/type"}}, ids[2:]},
			{"nested not", udbclient.Filter{"$not": udbclient.Filter{"$not": udbclient.Filter{"$eq": "allowed/type"}}}, ids[2:]},
			{"is not null", udbclient.Filter{"$is_null": false}, ids[1:]},
			{"not is not null", udbclient.Filter{"$not": udbclient.Filter{"$not_null": true}}, ids[:1]},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				page, err := files.Select(ctx, udbclient.Filter{
					"file_id": udbclient.Filter{"$in": ids}, "content_type": tc.predicate,
				}, udbclient.SelectOptions{})
				if err != nil {
					t.Fatal(err)
				}
				want := map[string]bool{}
				for _, id := range tc.want {
					want[id.(string)] = true
				}
				if len(page.Rows) != len(want) {
					t.Fatalf("predicate returned %d rows, want %d", len(page.Rows), len(want))
				}
				for _, row := range page.Rows {
					if !want[row.GetFileId()] {
						t.Fatalf("predicate incorrectly selected %s", row.GetFileId())
					}
					delete(want, row.GetFileId())
				}
			})
		}
	})
}
