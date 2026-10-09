package udbclient

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	apikeyentpb "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/entity/v1"
	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	livev1 "github.com/fahara02/udb/sdk/go/gen/udb/sdk/live/v1"
	"google.golang.org/grpc/status"
)

// This uses the already served catalog and ordinary verified operator login.
// It neither adds a schema nor changes tenant/PDP configuration.
func TestLiveG2MetadataAndTypedPredicates(t *testing.T) {
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" {
		t.Skip("requires a live UDB broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	sess, err := ConnectEnterprise(ctx, EnterpriseConfig{
		Target: requiredLiveEnv(t, "UDB_GRPC_TARGET"), AuthTarget: os.Getenv("UDB_AUTH_GRPC_TARGET"),
		Username: requiredLiveEnv(t, "UDB_LIVE_USERNAME"), Password: requiredLiveEnv(t, "UDB_LIVE_PASSWORD"),
		TenantCode: liveEnv("UDB_LIVE_TENANT", "sdk-live"), ProjectID: liveEnv("UDB_LIVE_PROJECT", "default"),
		Purpose: "go.live.g2.metadata", Deadline: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("G2 fixture login failed: code=%s", status.Code(err))
	}
	defer sess.Close()
	if sess.CanonicalTenantID == "" || sess.CanonicalProjectID == "" || sess.Meta.UserID == "" {
		t.Fatal("G2 fixture requires verified tenant/project/user identity")
	}
	token, err := sess.tm.store.Load(ctx)
	if err != nil || token.SessionID == "" {
		t.Fatal("G2 fixture must own a real login session")
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		callCtx := sess.NativeContext(cleanupCtx)
		if _, cleanupErr := sess.Auth.Authn.Logout(callCtx, &authnv1.LogoutRequest{SessionId: token.SessionID, RevokeReason: "go live G2 cleanup"}); cleanupErr != nil {
			t.Errorf("G2 session cleanup failed: code=%s", status.Code(cleanupErr))
		}
	}()
	keys, err := UniqueKeys((*livev1.SdkLiveRecord)(nil))
	if err != nil || !reflect.DeepEqual(keys, [][]string{{"lookup_key"}}) {
		t.Fatal("actual served fixture descriptor must deduplicate its declared unique key")
	}
	// The default producer registry covers native entities. Project-local
	// descriptors remain usable through TableOf without a fabricated registry entry.
	nativeKeys, err := UniqueKeys((*apikeyentpb.ApiKey)(nil))
	generated, ok := Entities["udb.core.apikey.entity.v1.ApiKey"]
	if err != nil || !reflect.DeepEqual(nativeKeys, [][]string{{"key_hash"}}) || !ok || !reflect.DeepEqual(generated.UniqueKeys, nativeKeys) {
		t.Fatal("actual native SDK producer registry must retain its descriptor's effective UniqueKeys")
	}
	table := TableOf[*livev1.SdkLiveRecord](sess.Udb)
	id := "g2-" + uuid4()
	lookup := "g2-lookup-" + uuid4()
	const exact int64 = 9007199254740993
	record := &livev1.SdkLiveRecord{
		RecordId: id, TenantId: sess.CanonicalTenantID, ProjectId: sess.CanonicalProjectID,
		LookupKey: lookup, Payload: "G2 owned predicate fixture", Revision: exact,
	}
	written := false
	defer func() {
		if !written {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if cleanupErr := table.Delete(cleanupCtx, RowKey{"record_id": id}); cleanupErr != nil {
			t.Errorf("G2 owned row cleanup failed: code=%s", status.Code(cleanupErr))
		}
	}()
	writeCtx, writeCancel := context.WithTimeout(ctx, 5*time.Second)
	err = table.Upsert(writeCtx, record)
	writeCancel()
	if err != nil {
		t.Fatalf("G2 typed fixture Upsert failed: code=%s", status.Code(err))
	}
	written = true
	getCtx, getCancel := context.WithTimeout(ctx, 5*time.Second)
	got, err := table.Get(getCtx, RowKey{keys[0][0]: lookup})
	getCancel()
	if err != nil || got.GetRecordId() != id || got.GetRevision() != exact || got.GetTenantId() != sess.CanonicalTenantID || got.GetProjectId() != sess.CanonicalProjectID {
		t.Fatalf("G2 real unique lookup must return the owned exact row: code=%s", status.Code(err))
	}
	revision := ColumnOf[int64]((*livev1.SdkLiveRecord)(nil), "revision")
	lookupColumn := ColumnOf[string]((*livev1.SdkLiveRecord)(nil), "lookup_key")
	tests := []struct {
		name  string
		build func() (Filter, error)
		count int
	}{
		{"exact64_eq", func() (Filter, error) { return revision.Eq(exact) }, 1},
		{"exact64_between", func() (Filter, error) { return revision.Between(exact, exact+2) }, 1},
		{"exact64_gt_refusal", func() (Filter, error) { return revision.Gt(exact) }, 0},
		{"unique_in", func() (Filter, error) { return lookupColumn.In(lookup) }, 1},
		{"unique_not_in", func() (Filter, error) { return lookupColumn.NotIn(lookup) }, 0},
		{"unique_empty_in", func() (Filter, error) { return lookupColumn.In() }, 0},
		{"null", func() (Filter, error) { return lookupColumn.IsNull() }, 0},
		{"not_null", func() (Filter, error) { return lookupColumn.NotNull() }, 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			where, err := test.build()
			if err != nil || where == nil {
				t.Fatal("G2 typed predicate must build a valid filter")
			}
			callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
			defer callCancel()
			page, err := table.Select(callCtx, Filter{"$and": []any{Filter{"record_id": id}, where}}, SelectOptions{Limit: 2})
			if err != nil || len(page.Rows) != test.count {
				t.Fatalf("G2 served predicate %s returned %d rows, want %d: code=%s", test.name, len(page.Rows), test.count, status.Code(err))
			}
			for _, row := range page.Rows {
				if row.GetRecordId() != id || row.GetRevision() != exact || row.GetTenantId() != sess.CanonicalTenantID || row.GetProjectId() != sess.CanonicalProjectID {
					t.Fatal("G2 served predicate changed the exact value or verified row scope")
				}
			}
		})
	}
}
