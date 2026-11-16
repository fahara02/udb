package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	authzv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/entity/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// RunJoin runs the join-fusion read contract: a Select whose message_type
// names two entities joined by a foreign key (here the authz PolicySet and its
// PolicyVersions) returns one record per joined pair, its columns aliased
// `{Message}__{column}`. The target must serve both entities and the storage
// File entity, and grant the caller udb:read on the fused message type.
func RunJoin(t *testing.T, connect Connect, opts ...Option) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	tenant, peer := NewID(), NewID()
	if o.tenants[0] != "" {
		tenant, peer = o.tenants[0], o.tenants[1]
	}
	t.Run("join fusion reads related rows of both tables as one aliased record", func(t *testing.T) {
		ctx := context.Background()
		u := connect(t, tenant)
		setType := udbclient.MessageType(&authzv1.PolicySet{})
		versionType := udbclient.MessageType(&authzv1.PolicyVersion{})
		joined := setType + "," + versionType
		write := func(messageType string, record map[string]any) {
			t.Helper()
			payload, err := structpb.NewStruct(record)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := u.Data.Broker.Upsert(ctx, &entityv1.UpsertRequest{MessageType: messageType, Payload: payload}); err != nil {
				t.Fatalf("upsert %s: %v", messageType, err)
			}
		}
		set, lone, second := NewID(), NewID(), NewID()
		write(setType, map[string]any{"policy_set_id": set, "name": "join-" + set})
		write(setType, map[string]any{"policy_set_id": lone, "name": "join-" + lone})
		write(versionType, map[string]any{"policy_version_id": NewID(), "policy_set_id": set, "version_number": 1})
		write(versionType, map[string]any{"policy_version_id": second, "policy_set_id": set, "version_number": 2})

		read := func(c *udbclient.Udb, req *entityv1.SelectRequest) (*entityv1.RecordSet, []map[string]any, error) {
			t.Helper()
			req.MessageType = joined
			result, err := c.Data.Broker.Select(ctx, req)
			if err != nil {
				return nil, nil, err
			}
			var records []map[string]any
			for _, raw := range result.GetRecordsJson() {
				decoder := json.NewDecoder(bytes.NewReader(raw))
				decoder.UseNumber()
				var record map[string]any
				if err := decoder.Decode(&record); err != nil {
					t.Fatalf("decode %s: %v", raw, err)
				}
				records = append(records, record)
			}
			return result, records, nil
		}
		filter := func(values map[string]any) *structpb.Struct {
			t.Helper()
			s, err := structpb.NewStruct(values)
			if err != nil {
				t.Fatal(err)
			}
			return s
		}

		_, records, err := read(u, &entityv1.SelectRequest{
			Filter: filter(map[string]any{"policy_set_id": set}),
			Fields: []string{"PolicySet.name", "PolicyVersion.version_number"},
		})
		if err != nil {
			t.Fatalf("join select: %v", err)
		}
		var versions []string
		for _, record := range records {
			if len(record) != 2 || record["PolicySet__name"] != "join-"+set {
				t.Fatalf("joined record = %v, want only PolicySet__name and PolicyVersion__version_number", record)
			}
			versions = append(versions, fmt.Sprint(record["PolicyVersion__version_number"]))
		}
		sort.Strings(versions)
		if strings.Join(versions, ",") != "1,2" {
			t.Fatalf("joined versions = %v, want 1 and 2", versions)
		}

		page, records, err := read(u, &entityv1.SelectRequest{Filter: filter(map[string]any{"policy_set_id": set}), Limit: 1})
		if err != nil || len(records) != 1 || !page.GetHasMore() {
			t.Fatalf("limit 1 = %d records, has_more %v, %v; want 1 and has_more", len(records), page.GetHasMore(), err)
		}

		_, records, err = read(u, &entityv1.SelectRequest{
			Filter: filter(map[string]any{"policy_set_id": set, "PolicyVersion.version_number": 2}),
		})
		if err != nil || len(records) != 1 {
			t.Fatalf("default projection = %v, %v; want one record", records, err)
		}
		if records[0]["PolicySet__policy_set_id"] != set || records[0]["PolicyVersion__policy_version_id"] != second {
			t.Fatalf("default projection = %v, want the set and its second version", records[0])
		}
		for alias := range records[0] {
			if !strings.HasPrefix(alias, "PolicySet__") && !strings.HasPrefix(alias, "PolicyVersion__") {
				t.Fatalf("default projection has an unaliased column %q", alias)
			}
		}

		if _, records, err = read(u, &entityv1.SelectRequest{Filter: filter(map[string]any{"policy_set_id": lone})}); err != nil || len(records) != 0 {
			t.Fatalf("a set without versions joined %v, %v; an inner join returns nothing", records, err)
		}
		if _, records, err = read(connect(t, peer), &entityv1.SelectRequest{Filter: filter(map[string]any{"policy_set_id": set})}); err != nil || len(records) != 0 {
			t.Fatalf("another tenant joined %v, %v; want nothing", records, err)
		}

		refused := func(what string, req *entityv1.SelectRequest) {
			t.Helper()
			if req.MessageType == "" {
				_, _, err = read(u, req)
			} else {
				_, err = u.Data.Broker.Select(ctx, req)
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("%s returned %v, want InvalidArgument", what, err)
			}
		}
		refused("a sorted join", &entityv1.SelectRequest{Sort: []*entityv1.Sort{{Field: "name"}}})
		refused("an operator filter", &entityv1.SelectRequest{Filter: filter(map[string]any{"policy_set_id": map[string]any{"$eq": set}})})
		refused("tables without a foreign key", &entityv1.SelectRequest{MessageType: udbclient.MessageType(&storagev1.File{}) + "," + setType})

		del, err := structpb.NewStruct(map[string]any{"policy_set_id": set})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := u.Data.Broker.Delete(ctx, &entityv1.DeleteRequest{MessageType: setType, Filter: del}); err != nil {
			t.Fatalf("delete set: %v", err)
		}
		if _, records, err = read(u, &entityv1.SelectRequest{Filter: filter(map[string]any{"policy_set_id": set})}); err != nil || len(records) != 0 {
			t.Fatalf("a soft-deleted set still joined %v, %v", records, err)
		}
	})
}
