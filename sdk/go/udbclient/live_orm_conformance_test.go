package udbclient

// Live ORM conformance (masterplan Phase 10 served proofs).
//
// Everything here runs against a REAL broker over the real JWT login path and
// exercises the generated ORM surface end-to-end:
//
//   - typed IR query/write/delete builders dispatched through the served
//     GenericDispatch chokepoint (10.1),
//   - descriptor-backed repository CRUD asserting the EMITTED wire conflict
//     clause targets the descriptor primary keys — never a hardcoded id (10.2),
//   - lazy/batch relation queries plus the one-query eager include path,
//     proving the N+1-safe secondary fetch against served Postgres (10.3),
//   - UnitOfWork flush through the served DataBroker.BeginTx bidi stream:
//     committed statuses, identity-map clean-up, and atomic rollback of the
//     whole batch when one mutation fails server-side (10.4).
//
// Gated on UDB_LIVE_SDK_TESTS=1 like the rest of the live suite.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// captureDispatcher records the last GenericDispatchRequest the ORM surface
// emitted before forwarding it to the live broker, so tests can assert the
// REAL wire payload (not a re-derived copy) while still proving the served
// round-trip.
type captureDispatcher struct {
	inner IRDispatcher
	last  *entityv1.GenericDispatchRequest
}

func (c *captureDispatcher) GenericDispatch(ctx context.Context, in *entityv1.GenericDispatchRequest, opts ...grpc.CallOption) (*entityv1.GenericDispatchResponse, error) {
	c.last = in
	return c.inner.GenericDispatch(ctx, in, opts...)
}

func TestLiveOrmConformance(t *testing.T) {
	if os.Getenv("UDB_LIVE_SDK_TESTS") != "1" {
		t.Skip("requires live UDB broker")
	}

	target := requiredLiveEnv(t, "UDB_GRPC_TARGET")
	authTarget := os.Getenv("UDB_AUTH_GRPC_TARGET")
	if authTarget == "" {
		authTarget = target
	}
	tenant := liveEnv("UDB_LIVE_TENANT", "sdk-live")
	project := liveEnv("UDB_LIVE_PROJECT", "default")
	meta := Metadata{
		TenantID:             tenant,
		ProjectID:            project,
		Purpose:              "go.live.orm",
		CorrelationID:        "go-live-orm",
		ServiceIdentity:      "go.sdk.live.orm",
		ClientCatalogVersion: ProtocolVersion,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	brokerConn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer brokerConn.Close()
	authConn := brokerConn
	if authTarget != target {
		authConn, err = grpc.NewClient(authTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("dial auth: %v", err)
		}
		defer authConn.Close()
	}

	login, err := authnv1.NewAuthnServiceClient(authConn).Login(ctx, &authnv1.LoginRequest{
		Username:    requiredLiveEnv(t, "UDB_LIVE_USERNAME"),
		Password:    requiredLiveEnv(t, "UDB_LIVE_PASSWORD"),
		TenantHint:  tenant,
		ProjectHint: project,
		DeviceName:  "go-sdk-live-orm",
	})
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}
	auth := NewAuthClient(authConn, meta)
	authResp, err := auth.AuthenticateBearer(ctx, login.GetAccessToken())
	if err != nil {
		t.Fatalf("AuthenticateBearer rejected Login access token: %v", err)
	}
	// Bind every record body to the canonical tenant UUID from the validated
	// principal, so body tenant == claim tenant (fail-closed handlers).
	if pt := authResp.GetPrincipal().GetTenantId(); pt != "" {
		tenant = pt
		meta.TenantID = tenant
	}

	authz := "Bearer " + login.GetAccessToken()
	brokerGen := NewGenerated(brokerConn, liveGeneratedOptions(meta, authz))
	callCtx := brokerGen.outgoingContext(ctx)
	dispatch := &captureDispatcher{inner: servicesv1.NewDataBrokerClient(brokerConn)}

	suffix := strings.ReplaceAll(uuid4(), "-", "")[:12]

	// ------------------------------------------------------------------
	// 10.2 — descriptor-backed repository CRUD with conflict_on == PK.
	// ------------------------------------------------------------------
	tmplRepo, err := NotificationTemplateRepository()
	if err != nil {
		t.Fatalf("NotificationTemplateRepository: %v", err)
	}
	templateID := uuid4()
	eventType := "orm.live." + suffix
	template := map[string]any{
		"template_id":      templateID,
		"event_type":       eventType,
		"channel":          "EMAIL",
		"subject_template": "orm live subject",
		"body_template":    "orm live body v1",
		"locale":           "en",
		"is_active":        true,
		"tenant_id":        tenant,
	}
	if _, err := tmplRepo.Upsert(callCtx, dispatch, template); err != nil {
		t.Fatalf("repository upsert (insert) failed on served GenericDispatch: %v", err)
	}
	assertEmittedConflictMatchesDescriptorPK(t, dispatch.last, tmplRepo)

	rows := liveOrmQueryRows(t, dispatch.last, mustFind(t, tmplRepo, callCtx, dispatch, map[string]any{"template_id": templateID}))
	if len(rows) != 1 {
		t.Fatalf("repository Find after insert: want exactly 1 row, got %d", len(rows))
	}
	if got := stringField(rows[0], "event_type"); got != eventType {
		t.Fatalf("repository Find returned wrong row: event_type=%q want %q", got, eventType)
	}

	// Update through the SAME upsert path: conflict on the descriptor PK must
	// turn this into an UPDATE, not a duplicate insert.
	template["body_template"] = "orm live body v2"
	if _, err := tmplRepo.Upsert(callCtx, dispatch, template); err != nil {
		t.Fatalf("repository upsert (update) failed: %v", err)
	}
	assertEmittedConflictMatchesDescriptorPK(t, dispatch.last, tmplRepo)

	byEvent, err := Query(tmplRepo.MessageType).
		Where("event_type", "eq", eventType).
		Execute(callCtx, dispatch)
	if err != nil {
		t.Fatalf("query by event_type failed: %v", err)
	}
	eventRows := liveOrmRows(t, byEvent.GetResultJson())
	if len(eventRows) != 1 {
		t.Fatalf("conflict-on-PK upsert must UPDATE, not duplicate: got %d rows for event_type %q", len(eventRows), eventType)
	}
	if got := stringField(eventRows[0], "body_template"); got != "orm live body v2" {
		t.Fatalf("second upsert did not update body_template: got %q", got)
	}

	// ------------------------------------------------------------------
	// 10.1 — typed IR query builder through served GenericDispatch.
	// ------------------------------------------------------------------
	q := Query(tmplRepo.MessageType).
		Where("event_type", "eq", eventType).
		Select("template_id", "event_type", "locale").
		OrderBy("template_id", "asc").
		Limit(5)
	resp, err := q.Execute(callCtx, dispatch)
	if err != nil {
		t.Fatalf("typed query builder Execute failed on served GenericDispatch: %v", err)
	}
	if resp.GetBackend() != DefaultIRBackend {
		t.Fatalf("dispatch echoed backend %q, want %q", resp.GetBackend(), DefaultIRBackend)
	}
	if !strings.Contains(dispatch.last.GetSpecJson(), `"ir"`) {
		t.Fatalf("typed builder must emit the canonical IR envelope; got spec_json %q", dispatch.last.GetSpecJson())
	}
	qRows := liveOrmRows(t, resp.GetResultJson())
	if len(qRows) != 1 || stringField(qRows[0], "template_id") == "" {
		t.Fatalf("typed query builder must return the projected row, got %v", qRows)
	}

	inRows, err := Query(tmplRepo.MessageType).WhereIn("template_id", templateID).Execute(callCtx, dispatch)
	if err != nil {
		t.Fatalf("WhereIn query failed: %v", err)
	}
	if got := len(liveOrmRows(t, inRows.GetResultJson())); got != 1 {
		t.Fatalf("WhereIn(template_id) want 1 row, got %d", got)
	}

	// ------------------------------------------------------------------
	// 10.3 (served side) — lazy relation query, batch secondary fetch,
	// and the one-query eager include path.
	// ------------------------------------------------------------------
	logRepo, err := NotificationLogRepository()
	if err != nil {
		t.Fatalf("NotificationLogRepository: %v", err)
	}
	logID1, logID2 := uuid4(), uuid4()
	mkLog := func(id string) map[string]any {
		return map[string]any{
			"log_id":            id,
			"template_id":       templateID,
			"event_type":        eventType,
			"channel":           "EMAIL",
			"recipient_address": "orm-live@example.com",
			"status":            "PENDING",
			"retry_count":       0,
			"tenant_id":         tenant,
		}
	}
	log1, log2 := mkLog(logID1), mkLog(logID2)
	for _, record := range []map[string]any{log1, log2} {
		if _, err := logRepo.Upsert(callCtx, dispatch, record); err != nil {
			t.Fatalf("seed notification_log failed: %v", err)
		}
	}

	// Lazy belongs_to: one child -> parent template, one served query.
	lazyQ, err := logRepo.RelationQuery("template", log1)
	if err != nil {
		t.Fatalf("RelationQuery(template): %v", err)
	}
	lazyResp, err := lazyQ.Execute(callCtx, dispatch)
	if err != nil {
		t.Fatalf("lazy relation query failed on served path: %v", err)
	}
	lazyRows := liveOrmRows(t, lazyResp.GetResultJson())
	if len(lazyRows) != 1 || stringField(lazyRows[0], "template_id") != templateID {
		t.Fatalf("lazy belongs_to must load exactly the parent template, got %v", lazyRows)
	}

	// Batch secondary fetch (N+1-safe): BOTH parents resolved by ONE deduped
	// WhereIn child query — a single served dispatch.
	batchQ, err := logRepo.RelationBatchQuery("template", []map[string]any{log1, log2})
	if err != nil {
		t.Fatalf("RelationBatchQuery(template): %v", err)
	}
	batchResp, err := batchQ.Execute(callCtx, dispatch)
	if err != nil {
		t.Fatalf("batch relation query failed on served path: %v", err)
	}
	if got := len(liveOrmRows(t, batchResp.GetResultJson())); got != 1 {
		t.Fatalf("batch belongs_to over 2 children sharing one parent: want 1 deduped row, got %d", got)
	}

	// Inverse has_many batch: one query loads ALL children of the parent set.
	hasManyQ, err := tmplRepo.RelationBatchQuery("notification_logs", []map[string]any{template})
	if err != nil {
		t.Fatalf("RelationBatchQuery(notification_logs): %v", err)
	}
	hasManyResp, err := hasManyQ.Execute(callCtx, dispatch)
	if err != nil {
		t.Fatalf("has_many batch query failed on served path: %v", err)
	}
	childRows := liveOrmRows(t, hasManyResp.GetResultJson())
	if len(childRows) != 2 {
		t.Fatalf("has_many secondary fetch must return both children in ONE query, got %d rows", len(childRows))
	}

	// Eager include: one compiled SQL query returns child rows with the parent
	// embedded under the relation name.
	incResp, err := Query(logRepo.MessageType).
		WhereIn("log_id", logID1, logID2).
		Include("template").
		OrderBy("log_id", "asc").
		Execute(callCtx, dispatch)
	if err != nil {
		t.Fatalf("eager include query failed on served path: %v", err)
	}
	incRows := liveOrmRows(t, incResp.GetResultJson())
	if len(incRows) != 2 {
		t.Fatalf("eager include: want 2 child rows, got %d", len(incRows))
	}
	for _, row := range incRows {
		embedded := embeddedObject(t, row, "template")
		if stringField(embedded, "template_id") != templateID {
			t.Fatalf("eager include row missing embedded parent template: %v", row)
		}
	}

	// Non-relational backends must be refused BEFORE dispatch (fail-closed tier gate).
	if _, err := Query(logRepo.MessageType).Include("template").ToRequest("redis"); err == nil {
		t.Fatalf("eager include on a kv-tier backend must be rejected client-side")
	}

	// ------------------------------------------------------------------
	// 10.4 — UnitOfWork flush via the served DataBroker.BeginTx stream.
	// ------------------------------------------------------------------
	flagRepo, err := FlagRepository()
	if err != nil {
		t.Fatalf("FlagRepository: %v", err)
	}
	if flagRepo.Descriptor.VersionField == "" {
		t.Fatalf("Flag entity must carry descriptor version metadata for the UnitOfWork version check")
	}
	flagID := uuid4()
	flag := map[string]any{
		"flag_id":             flagID,
		"tenant_id":           tenant,
		"project_id":          project,
		"environment":         "live",
		"flag_key":            "orm.live." + suffix,
		"value_type":          "bool",
		"value_json":          "true",
		"enabled":             true,
		"rollout_percentage":  0,
		"rollout_context_key": "",
		"revision":            1,
		"metadata_json":       "{}",
	}

	// Transaction honesty: a projection backend must be refused before any stream opens.
	uowHonesty := NewUnitOfWork()
	if err := uowHonesty.RequireTransactionalBackend("qdrant"); err == nil {
		t.Fatalf("UnitOfWork must reject projection backends before a commit batch")
	}

	uow := NewUnitOfWork()
	tracked, err := uow.Attach(flagRepo, flag)
	if err != nil {
		t.Fatalf("UnitOfWork attach: %v", err)
	}
	tracked["value_json"] = "false"
	tracked["revision"] = 2

	statuses, err := uow.Flush(ctx, brokerGen)
	if err != nil {
		t.Fatalf("UnitOfWork flush over served BeginTx failed: %v (statuses=%v)", err, statuses)
	}
	if len(statuses) < 2 {
		t.Fatalf("flush must return per-mutation + commit statuses, got %d", len(statuses))
	}
	if statuses[0].GetState() != entityv1.TxStatus_TX_STATE_OPEN {
		t.Fatalf("first mutation status: got %v want TX_STATE_OPEN", statuses[0].GetState())
	}
	last := statuses[len(statuses)-1]
	if last.GetState() != entityv1.TxStatus_TX_STATE_COMMITTED {
		t.Fatalf("final status: got %v (%s) want TX_STATE_COMMITTED", last.GetState(), last.GetMessage())
	}
	dirty, err := uow.DirtyEntries()
	if err != nil {
		t.Fatalf("DirtyEntries after flush: %v", err)
	}
	if len(dirty) != 0 {
		t.Fatalf("identity map must be clean after successful flush, still %d dirty", len(dirty))
	}
	flagRows := liveOrmRows(t, mustFind(t, flagRepo, callCtx, dispatch, map[string]any{"flag_id": flagID}))
	if len(flagRows) != 1 || !jsonNumberEquals(flagRows[0]["revision"], 2) {
		t.Fatalf("flushed flag not persisted with revision 2: %v", flagRows)
	}

	// Atomic rollback: a batch with one poisoned mutation (text bound into the
	// INTEGER rollout_percentage column — no implicit PG cast) must roll back
	// the WHOLE served transaction — the valid mutation in the same batch must
	// not persist, the identity map stays dirty, and the failure surfaces as a
	// typed SDK error.
	tracked["revision"] = 3
	tracked["value_json"] = "\"v3\""
	poisoned := map[string]any{
		"flag_id":             uuid4(),
		"tenant_id":           tenant,
		"project_id":          project,
		"environment":         "live",
		"flag_key":            "orm.live.poison." + suffix,
		"value_type":          "bool",
		"value_json":          "true",
		"enabled":             true,
		"rollout_percentage":  "boom",
		"rollout_context_key": "",
		"revision":            1,
		"metadata_json":       "{}",
	}
	poisonedTracked, err := uow.Attach(flagRepo, poisoned)
	if err != nil {
		t.Fatalf("attach poisoned record: %v", err)
	}
	poisonedTracked["enabled"] = false

	_, err = uow.Flush(ctx, brokerGen)
	if err == nil {
		t.Fatalf("flush with a poisoned mutation must fail")
	}
	if _, ok := AsError(err); !ok {
		t.Fatalf("flush failure must surface as a typed SDK error, got %T: %v", err, err)
	}
	dirty, err = uow.DirtyEntries()
	if err != nil {
		t.Fatalf("DirtyEntries after failed flush: %v", err)
	}
	if len(dirty) == 0 {
		t.Fatalf("identity map must stay dirty after a failed flush")
	}
	flagRows = liveOrmRows(t, mustFind(t, flagRepo, callCtx, dispatch, map[string]any{"flag_id": flagID}))
	if len(flagRows) != 1 || !jsonNumberEquals(flagRows[0]["revision"], 2) {
		t.Fatalf("served BeginTx must roll back the whole batch: flag revision drifted to %v", flagRows)
	}

	// ------------------------------------------------------------------
	// Cleanup through the typed delete path (also proves DeleteBuilder live).
	// ------------------------------------------------------------------
	for _, cleanup := range []struct {
		repo *Repository
		key  map[string]any
	}{
		{logRepo, map[string]any{"log_id": logID1}},
		{logRepo, map[string]any{"log_id": logID2}},
		{tmplRepo, map[string]any{"template_id": templateID}},
		{flagRepo, map[string]any{"flag_id": flagID}},
	} {
		if _, err := cleanup.repo.Delete(callCtx, dispatch, cleanup.key); err != nil {
			t.Fatalf("repository delete failed for %s: %v", cleanup.repo.MessageType, err)
		}
	}
	goneRows := liveOrmRows(t, mustFind(t, tmplRepo, callCtx, dispatch, map[string]any{"template_id": templateID}))
	if len(goneRows) != 0 {
		t.Fatalf("deleted template still visible: %v", goneRows)
	}

	t.Logf("live ORM conformance green: builders+repository+relations+include+UnitOfWork over served GenericDispatch/BeginTx (tenant=%s)", tenant)
}

// assertEmittedConflictMatchesDescriptorPK decodes the actually-emitted wire
// spec and pins the 10.2 contract: conflict kind "update", conflict_on ==
// descriptor primary keys, and no PK ever listed as an update field.
func assertEmittedConflictMatchesDescriptorPK(t *testing.T, req *entityv1.GenericDispatchRequest, repo *Repository) {
	t.Helper()
	if req == nil {
		t.Fatalf("no dispatch request captured")
	}
	var spec struct {
		IR struct {
			Op       string `json:"op"`
			Conflict struct {
				Kind       string   `json:"kind"`
				Fields     []string `json:"fields"`
				ConflictOn []string `json:"conflict_on"`
			} `json:"conflict"`
		} `json:"ir"`
	}
	if err := json.Unmarshal([]byte(req.GetSpecJson()), &spec); err != nil {
		t.Fatalf("emitted spec_json is not the IR envelope: %v (%s)", err, req.GetSpecJson())
	}
	if spec.IR.Op != "write" {
		t.Fatalf("repository upsert must emit ir.op=write, got %q", spec.IR.Op)
	}
	if spec.IR.Conflict.Kind != "update" {
		t.Fatalf("repository upsert must emit conflict kind update, got %q", spec.IR.Conflict.Kind)
	}
	if fmt.Sprintf("%v", spec.IR.Conflict.ConflictOn) != fmt.Sprintf("%v", repo.Descriptor.PrimaryKeys) {
		t.Fatalf("emitted conflict_on %v must equal descriptor primary keys %v", spec.IR.Conflict.ConflictOn, repo.Descriptor.PrimaryKeys)
	}
	for _, pk := range repo.Descriptor.PrimaryKeys {
		for _, field := range spec.IR.Conflict.Fields {
			if field == pk {
				t.Fatalf("primary key %q must never be an on-conflict update field", pk)
			}
		}
	}
}

func mustFind(t *testing.T, repo *Repository, ctx context.Context, dispatch IRDispatcher, key map[string]any) string {
	t.Helper()
	resp, err := repo.Find(ctx, dispatch, key)
	if err != nil {
		t.Fatalf("repository Find on %s failed: %v", repo.MessageType, err)
	}
	return resp.GetResultJson()
}

// liveOrmQueryRows keeps the captured-request plumbing honest: the Find above
// must have gone through GenericDispatch with an IR read envelope.
func liveOrmQueryRows(t *testing.T, req *entityv1.GenericDispatchRequest, resultJSON string) []map[string]any {
	t.Helper()
	if req == nil || !strings.Contains(req.GetSpecJson(), `"op":"read"`) {
		t.Fatalf("repository Find must dispatch an IR read envelope, captured: %v", req.GetSpecJson())
	}
	return liveOrmRows(t, resultJSON)
}

func liveOrmRows(t *testing.T, resultJSON string) []map[string]any {
	t.Helper()
	trimmed := strings.TrimSpace(resultJSON)
	var rows []map[string]any
	if err := json.Unmarshal([]byte(trimmed), &rows); err == nil {
		return rows
	}
	var wrapper map[string]any
	if err := json.Unmarshal([]byte(trimmed), &wrapper); err == nil {
		if raw, ok := wrapper["rows"]; ok {
			body, err := json.Marshal(raw)
			if err == nil && json.Unmarshal(body, &rows) == nil {
				return rows
			}
		}
	}
	t.Fatalf("dispatch result_json is not a row set: %s", trimmed)
	return nil
}

func stringField(row map[string]any, field string) string {
	value, ok := row[field]
	if !ok || value == nil {
		return ""
	}
	if s, ok := value.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", value)
}

// embeddedObject accepts both nested-object and JSON-string renderings of an
// embedded eager-include payload.
func embeddedObject(t *testing.T, row map[string]any, field string) map[string]any {
	t.Helper()
	value, ok := row[field]
	if !ok || value == nil {
		t.Fatalf("row missing embedded relation %q: %v", field, row)
	}
	if obj, ok := value.(map[string]any); ok {
		return obj
	}
	if s, ok := value.(string); ok {
		var obj map[string]any
		if err := json.Unmarshal([]byte(s), &obj); err == nil {
			return obj
		}
	}
	t.Fatalf("embedded relation %q is neither object nor JSON string: %v", field, value)
	return nil
}

func jsonNumberEquals(value any, want int64) bool {
	switch v := value.(type) {
	case float64:
		return int64(v) == want
	case int64:
		return v == want
	case int:
		return int64(v) == want
	case json.Number:
		n, err := v.Int64()
		return err == nil && n == want
	case string:
		var f float64
		if err := json.Unmarshal([]byte(v), &f); err == nil {
			return int64(f) == want
		}
	}
	return false
}
