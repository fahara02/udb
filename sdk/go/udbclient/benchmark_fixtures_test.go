package udbclient

import (
	"encoding/json"
	"os"
	"testing"
)

// Only actual live seeds load this file; manifest hydration unit fixtures remain
// explicit. Refuse a file from another reset, tenant, or project before copying
// any reference that will become a measured request's authority.
func loadPreparedBenchmarkFixtures(t testing.TB, fix *perfFixtures, tenant, project string) {
	t.Helper()
	path := os.Getenv("UDB_BENCH_FIXTURES")
	if path == "" {
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read prepared benchmark fixtures: %v", err)
	}
	var prepared struct {
		SchemaVersion int               `json:"schema_version"`
		TenantID      string            `json:"tenant_id"`
		ProjectID     string            `json:"project_id"`
		Fixtures      map[string]string `json:"fixtures"`
	}
	if err := json.Unmarshal(raw, &prepared); err != nil {
		t.Fatalf("decode prepared benchmark fixtures: %v", err)
	}
	if prepared.SchemaVersion != 1 || prepared.TenantID != tenant || prepared.ProjectID != project {
		t.Fatal("prepared benchmark fixtures do not match the verified tenant/project")
	}
	for _, key := range []string{"multipart_bucket", "multipart_object_key", "multipart_upload_id", "multipart_etag", "ack_workflow_id"} {
		value := prepared.Fixtures[key]
		if value == "" {
			t.Fatalf("prepared benchmark fixtures missing %s", key)
		}
		fix.set(key, value)
	}
}
