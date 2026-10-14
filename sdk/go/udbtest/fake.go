// Package udbtest is an in-memory udb broker for unit tests.
//
// It serves the DataBroker data verbs (Select, Insert, Upsert, Update, Delete),
// atomic transactions, durable CDC consumers and operator DLQ APIs, plus the
// API-key exchange over a real loopback gRPC listener, so the code under test
// uses an unmodified *udbclient.Udb. Its semantics follow the broker's:
// the caller's tenant is filled into filters and records (a different tenant is
// refused), primary keys are unique, conditional writes compare by value,
// require_affected is enforced, reads page with has_more and exact totals, and
// every refusal carries the broker's stable `UDB_*` reason in the typed error
// detail — so a service's tests catch what production would.
//
// The same conformance suite runs against this fake and against a live broker
// (sdk/go/udbclient conformance tests), which keeps the two from drifting.
package udbtest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	commonv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"
)

// Fake is the in-memory broker.
type Fake struct {
	servicesv1.UnimplementedDataBrokerServer
	authn         fakeAuthn
	server        *grpc.Server
	policy        fakePolicySet
	identities    map[string]Identity
	tokens        map[string]fakeToken
	tokenLifetime time.Duration
	faults        []Fault
	rate          fakeRateLimit

	addr string

	mu       sync.Mutex
	entities map[string]*entity
	// rows: message type → tenant → primary-key string → row.
	rows   map[string]map[string]map[string]map[string]any
	events []Event
	// cursors: tenant + project + consumer + topic pattern → verified owner and position.
	cursors   map[string]consumerCursor
	dlq       map[string]*fakeDlq
	receipts  map[mutationKey]mutationReceipt
	revisions map[revisionKey]int64
	emitted   int
}

type entity struct {
	messageType   string
	primaryKey    []string
	tenantColumn  string
	projectColumn string
	columns       map[string]protoreflect.FieldDescriptor
	descriptor    protoreflect.MessageDescriptor
	options       map[string]*commonv1.ColumnOptions
	aliases       map[string]string
	masked        map[string]bool
	omitImplicit  map[string]bool
	piiScope      map[string]bool
	unique        []uniqueKey
	softDelete    string
	tableName     string
	schemaError   string
}

// New starts a fake serving the given entity messages (their udb column
// annotations define keys and the tenant column). It stops when t ends.
func New(t testing.TB, entities ...proto.Message) *Fake {
	t.Helper()
	f := newFake()
	f.policy.defaultAllow = true
	for _, message := range entities {
		f.Register(message)
	}
	if err := f.serve(); err != nil {
		t.Fatalf("udbtest: start: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// Register adds an entity message type to the fake.
func (f *Fake) Register(m proto.Message) {
	desc := m.ProtoReflect().Descriptor()
	e := &entity{
		descriptor:  desc,
		messageType: udbclient.MessageType(m),
		primaryKey:  udbclient.PrimaryKeys(m),
		columns:     map[string]protoreflect.FieldDescriptor{},
	}
	for i := 0; i < desc.Fields().Len(); i++ {
		fd := desc.Fields().Get(i)
		e.columns[string(fd.Name())] = fd
	}
	e.tenantColumn = udbclient.TenantColumn(m)
	e.projectColumn = udbclient.ProjectColumn(m)
	e.descriptorRules(desc)
	f.mu.Lock()
	f.entities[e.messageType] = e
	f.mu.Unlock()
}

// Target is the fake's gRPC address.
func (f *Fake) Target() string { return f.addr }

// AddAPIKey makes key exchangeable for a bearer of tenant.
func (f *Fake) AddAPIKey(key, tenant string) {
	f.AddIdentityAPIKey(key, Identity{TenantID: tenant, ServiceIdentity: "udbtest-service-" + tenant,
		Scopes: []string{"udb:read", "udb:write", "udb:admin", "udb:cdc:read", "udb:cdc:subscribe"}})
}

// Client connects a client for tenant (with an exchanged API key, as a
// service would) and closes it when t ends.
func (f *Fake) Client(t testing.TB, tenant string) *udbclient.Udb {
	t.Helper()
	key := "udbtest-key-" + tenant
	f.AddAPIKey(key, tenant)
	u, err := udbclient.Connect(context.Background(), udbclient.Config{
		Target:      f.addr,
		TenantID:    tenant,
		Purpose:     "test",
		Credentials: udbclient.Credentials{APIKey: key},
	})
	if err != nil {
		t.Fatalf("udbtest: connect: %v", err)
	}
	t.Cleanup(func() { _ = u.Close() })
	return u
}

// Rows returns a copy of every stored row of messageType for tenant, for
// assertions.
func (f *Fake) Rows(messageType, tenant string) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []map[string]any
	for _, row := range f.rows[messageType][tenant] {
		out = append(out, copyRow(row))
	}
	sort.Slice(out, func(i, j int) bool { return fmt.Sprint(out[i]) < fmt.Sprint(out[j]) })
	return out
}

// ── authn ─────────────────────────────────────────────────────────────────────

type fakeAuthn struct {
	authnv1.UnimplementedAuthnServiceServer
	fake *Fake
}

func (a *fakeAuthn) Authenticate(_ context.Context, req *authnv1.AuthnRequest) (*authnv1.AuthnResponse, error) {
	return a.fake.authenticate(req.GetApiKey())
}

// ── data verbs ────────────────────────────────────────────────────────────────

func callerTenant(ctx context.Context) string {
	return callerIdentity(ctx).TenantID
}

func (f *Fake) entity(messageType string) (*entity, error) {
	e, ok := f.entities[messageType]
	if !ok {
		return nil, refuse(codes.InvalidArgument, "UDB_UNKNOWN_MESSAGE_TYPE",
			fmt.Sprintf("unknown message_type %s: the fake serves no such entity", messageType))
	}
	if e.schemaError != "" {
		return nil, refuse(codes.FailedPrecondition, "", e.schemaError)
	}
	return e, nil
}

// Select reads rows like the broker: scoped to the caller's tenant, ordered by
// the sort then the primary key, paged with has_more and an optional total.
func (f *Fake) Select(ctx context.Context, req *entityv1.SelectRequest) (*entityv1.RecordSet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, err := f.entity(req.GetMessageType())
	if err != nil {
		return nil, err
	}
	identity := callerIdentity(ctx)
	tenant := identity.TenantID
	filter, err := e.queryFilter(req.GetFilter().AsMap())
	if err != nil {
		return nil, err
	}
	if err := e.checkTenant(filter, tenant); err != nil {
		return nil, err
	}
	selected, err := e.readColumns(ctx, req.GetFields())
	if err != nil {
		return nil, err
	}
	if req.GetIncludeRevision() {
		for _, column := range e.primaryKey {
			selected[column] = true
		}
	}
	var matched []map[string]any
	for _, row := range f.rows[e.messageType][tenant] {
		if !e.live(row) || !e.visibleTo(row, identity) {
			continue
		}
		ok, err := matches(row, filter)
		if err != nil {
			return nil, err
		}
		if ok {
			matched = append(matched, row)
		}
	}
	var order []*entityv1.Sort
	for _, requested := range req.GetSort() {
		column, ok := e.aliases[strings.ToLower(strings.TrimSpace(requested.GetField()))]
		if !ok {
			return nil, refuse(codes.InvalidArgument, "", "unknown sort column "+requested.GetField())
		}
		order = append(order, &entityv1.Sort{Field: column, Descending: requested.GetDescending()})
	}
	for _, pk := range e.primaryKey {
		order = append(order, &entityv1.Sort{Field: pk})
	}
	sort.SliceStable(matched, func(i, j int) bool {
		for _, s := range order {
			c := compare(matched[i][s.GetField()], matched[j][s.GetField()])
			if c != 0 {
				return (c < 0) != s.GetDescending()
			}
		}
		return false
	})
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 100
	}
	offset := 0
	if tok := req.GetPageToken(); tok != "" {
		raw, err := base64.RawURLEncoding.DecodeString(tok)
		if err != nil {
			return nil, refuse(codes.InvalidArgument, "", "invalid page_token")
		}
		offset, err = strconv.Atoi(string(raw))
		if err != nil || offset < 0 || offset > len(matched) {
			return nil, refuse(codes.InvalidArgument, "", "invalid page_token offset")
		}
	}
	out := &entityv1.RecordSet{}
	if req.GetIncludeTotal() {
		out.ExactTotal = int64(len(matched))
	}
	end := min(offset+limit, len(matched))
	for _, row := range matched[min(offset, len(matched)):end] {
		view := make(map[string]any, len(selected))
		for column := range selected {
			if value, exists := row[column]; exists {
				view[column] = e.wireValue(column, copyValue(value))
			}
		}
		if !hasScope(callerIdentity(ctx), "udb:pii:read") {
			for column, masked := range e.masked {
				if _, exists := view[column]; masked && exists {
					view[column] = "***MASKED***"
				}
			}
		}
		raw, err := json.Marshal(view)
		if err != nil {
			return nil, refuse(codes.Internal, "", "stored row is not JSON")
		}
		out.RecordsJson = append(out.RecordsJson, raw)
		if req.GetIncludeRevision() {
			key, err := e.keyOf(row)
			if err != nil {
				return nil, err
			}
			out.RecordRevisions = append(out.RecordRevisions, f.revision(identity, e, key))
		}
		fields, err := structpb.NewStruct(protobufJSON(view).(map[string]any))
		if err != nil {
			return nil, refuse(codes.Internal, "", "stored row cannot be encoded as protobuf Values")
		}
		out.Rows = append(out.Rows, &entityv1.Row{Fields: fields.GetFields()})
	}
	redacted := map[string]bool{}
	for _, raw := range out.RecordsJson {
		var view map[string]any
		_ = json.Unmarshal(raw, &view)
		for name, value := range view {
			if value == "***MASKED***" {
				redacted[name] = true
			}
		}
	}
	for name := range redacted {
		out.RedactedFields = append(out.RedactedFields, name)
	}
	sort.Strings(out.RedactedFields)
	out.HasMore = len(out.RecordsJson) >= limit
	if out.HasMore && end < len(matched) {
		out.NextPageToken = base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}
	return out, nil
}

// Upsert inserts or replaces a whole row by primary key.
func (f *Fake) Upsert(ctx context.Context, req *entityv1.UpsertRequest) (*entityv1.MutationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.keyedWriteLocked(ctx, "upsert", req, func() (*entityv1.MutationResponse, error) {
		return f.upsertLocked(callerIdentity(ctx), req)
	})
}

func (f *Fake) upsertLocked(identity Identity, req *entityv1.UpsertRequest) (*entityv1.MutationResponse, error) {
	return f.writeLocked(identity, req, false)
}

// Insert creates a new row atomically; a duplicate key never overwrites it.
func (f *Fake) Insert(ctx context.Context, req *entityv1.UpsertRequest) (*entityv1.MutationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.keyedWriteLocked(ctx, "insert", req, func() (*entityv1.MutationResponse, error) {
		return f.writeLocked(callerIdentity(ctx), req, true)
	})
}

func (f *Fake) writeLocked(identity Identity, req *entityv1.UpsertRequest, insertOnly bool) (*entityv1.MutationResponse, error) {
	tenant := identity.TenantID
	if insertOnly && (len(req.GetConflictFields()) > 0 || len(req.GetExpected().GetFields()) > 0) {
		return nil, refuse(codes.InvalidArgument, "", "Insert does not accept conflict_fields or expected")
	}
	e, err := f.entity(req.GetMessageType())
	if err != nil {
		return nil, err
	}
	var record map[string]any
	if req.GetPayload() != nil {
		record = req.GetPayload().AsMap()
	} else {
		record, err = decodeObject(req.GetRecordJson())
		if err != nil {
			return nil, refuse(codes.InvalidArgument, "", err.Error())
		}
	}
	record, err = e.normalize(record)
	if err != nil {
		return nil, err
	}
	if err := e.bindRecordScope(record, identity); err != nil {
		return nil, err
	}
	if err := e.conflictFields(req.GetConflictFields()); err != nil {
		return nil, err
	}
	if err := e.defaults(record); err != nil {
		return nil, err
	}
	key, err := e.keyOf(record)
	if err != nil {
		return nil, err
	}
	if insertOnly {
		for _, rows := range f.rows[e.messageType] {
			if _, exists := rows[key]; exists {
				return nil, uniqueRefusal(e.tableName + "_pkey")
			}
		}
	}
	if !insertOnly {
		for owner, rows := range f.rows[e.messageType] {
			if existing, found := rows[key]; found && (owner != tenant || !e.sameRecordScope(existing, record)) {
				if len(req.GetExpected().GetFields()) > 0 {
					return nil, refuse(codes.FailedPrecondition, "UDB_CAS_ROW_MISSING", "compare-and-swap target does not exist in the verified scope")
				}
				// Native ON CONFLICT scope guard leaves a foreign row untouched.
				return &entityv1.MutationResponse{MutationId: key, AffectedRows: 0}, nil
			}
		}
	}
	table := f.table(e.messageType, tenant)
	expected, err := e.expectedColumns(req.GetExpected().AsMap())
	if err != nil {
		return nil, err
	}
	if len(expected) > 0 {
		current, ok := table[key]
		if !ok {
			return nil, refuse(codes.FailedPrecondition, "UDB_CAS_ROW_MISSING", "compare-and-swap precondition failed: the target row does not exist")
		}
		if !holds(current, expected) {
			return nil, refuse(codes.FailedPrecondition, "UDB_CAS_CONFLICT", "compare-and-swap precondition failed: a field did not match the current row")
		}
	}
	if e.softDelete != "" && !insertOnly {
		if _, supplied := record[e.softDelete]; !supplied {
			record[e.softDelete] = nil
		}
	}
	if err := f.validateRow(e, tenant, key, record); err != nil {
		return nil, err
	}
	if current, exists := table[key]; exists && !insertOnly {
		// Isolation columns are excluded from native conflict UPDATE SET.
		for _, column := range []string{e.tenantColumn, e.projectColumn} {
			if column != "" {
				if value, present := current[column]; present {
					record[column] = copyValue(value)
				}
			}
		}
	}
	table[key] = record
	return &entityv1.MutationResponse{MutationId: key, AffectedRows: 1, Revision: f.bumpRevision(identity, e, key)}, nil
}

// Update sets the given columns (and applies increments) on the matched rows.
func (f *Fake) Update(ctx context.Context, req *entityv1.UpdateRequest) (*entityv1.MutationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.keyedWriteLocked(ctx, "update", req, func() (*entityv1.MutationResponse, error) {
		return f.updateLocked(callerIdentity(ctx), req)
	})
}

func (f *Fake) updateLocked(identity Identity, req *entityv1.UpdateRequest) (*entityv1.MutationResponse, error) {
	tenant := identity.TenantID
	e, err := f.entity(req.GetMessageType())
	if err != nil {
		return nil, err
	}
	filter, err := e.queryFilter(req.GetFilter().AsMap())
	if err != nil {
		return nil, err
	}
	if len(filter) == 0 {
		return nil, refuse(codes.InvalidArgument, "", "an update needs a filter")
	}
	if err := e.checkTenant(filter, tenant); err != nil {
		return nil, err
	}
	if err := e.updateColumns(req.GetChanges().AsMap(), req.GetIncrements()); err != nil {
		return nil, err
	}
	expected, err := e.expectedColumns(req.GetExpected().AsMap())
	if err != nil {
		return nil, err
	}
	if len(expected) > 0 {
		if err := e.validateCASKey(filter); err != nil {
			return nil, err
		}
	}
	changes, err := e.normalize(req.GetChanges().AsMap())
	if err != nil {
		return nil, err
	}
	table := f.table(e.messageType, tenant)
	var keys []string
	for key, row := range table {
		if !e.live(row) || !e.visibleTo(row, identity) {
			continue
		}
		matched, err := matches(row, filter)
		if err != nil {
			return nil, err
		}
		if matched {
			keys = append(keys, key)
		}
	}
	if len(expected) > 0 {
		if len(keys) == 0 {
			return nil, refuse(codes.FailedPrecondition, "UDB_CAS_ROW_MISSING", "CAS row is missing")
		}
		if len(keys) != 1 || !holds(table[keys[0]], expected) {
			return nil, refuse(codes.FailedPrecondition, "UDB_CAS_CONFLICT", "compare-and-swap precondition failed")
		}
	}
	if err := f.checkRevision(identity, e, filter, req.GetExpectedRevision()); err != nil {
		return nil, err
	}
	if want := req.GetRequireAffected(); want > 0 && uint32(len(keys)) != want {
		return nil, refuse(codes.NotFound, "UDB_NO_ROWS_AFFECTED", "write matched a different number of rows than require_affected")
	}
	// Build the entire statement result before publishing it. A late unique or
	// NOT NULL violation rolls back every earlier matched row, like PostgreSQL.
	candidate := make(map[string]map[string]any, len(table))
	for key, row := range table {
		candidate[key] = row
	}
	for _, key := range keys {
		delete(candidate, key)
	}
	var changedKeys []string
	for _, key := range keys {
		row := copyRow(table[key])
		for name, value := range changes {
			row[name] = copyValue(value)
		}
		for _, inc := range req.GetIncrements() {
			name, ok := e.aliases[strings.ToLower(strings.TrimSpace(inc.GetColumn()))]
			if !ok {
				return nil, refuse(codes.InvalidArgument, "", "unknown increment column")
			}
			row[name], err = incrementValue(row[name], inc.GetDelta(), integerColumn(e.options[name]))
			if err != nil {
				return nil, err
			}
		}
		newKey, err := e.keyOf(row)
		if err != nil {
			return nil, err
		}
		if _, exists := candidate[newKey]; exists {
			return nil, uniqueRefusal(e.tableName + "_pkey")
		}
		candidate[newKey] = row
		changedKeys = append(changedKeys, newKey)
	}
	f.rows[e.messageType][tenant] = candidate
	for _, key := range changedKeys {
		if err := f.validateRow(e, tenant, key, candidate[key]); err != nil {
			f.rows[e.messageType][tenant] = table
			return nil, err
		}
	}
	response := &entityv1.MutationResponse{AffectedRows: int64(len(keys))}
	if len(keys) > 0 {
		if key, err := e.revisionFilterKey(identity, filter); err == nil {
			response.Revision = f.bumpRevision(identity, e, key)
		}
	}
	return response, nil
}

// Delete removes the matched rows.
func (f *Fake) Delete(ctx context.Context, req *entityv1.DeleteRequest) (*entityv1.MutationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.keyedWriteLocked(ctx, "delete", req, func() (*entityv1.MutationResponse, error) {
		return f.deleteLocked(callerIdentity(ctx), req)
	})
}

func (f *Fake) deleteLocked(identity Identity, req *entityv1.DeleteRequest) (*entityv1.MutationResponse, error) {
	tenant := identity.TenantID
	e, err := f.entity(req.GetMessageType())
	if err != nil {
		return nil, err
	}
	filter, err := e.queryFilter(req.GetFilter().AsMap())
	if err != nil {
		return nil, err
	}
	if len(filter) == 0 {
		return nil, refuse(codes.InvalidArgument, "", "a delete needs a filter")
	}
	if err := e.checkTenant(filter, tenant); err != nil {
		return nil, err
	}
	expected, err := e.expectedColumns(req.GetExpected().AsMap())
	if err != nil {
		return nil, err
	}
	if len(expected) > 0 {
		if err := e.validateCASKey(filter); err != nil {
			return nil, err
		}
	}
	table := f.table(e.messageType, tenant)
	var keys []string
	for key, row := range table {
		if !e.live(row) || !e.visibleTo(row, identity) {
			continue
		}
		ok, err := matches(row, filter)
		if err != nil {
			return nil, err
		}
		if ok {
			keys = append(keys, key)
		}
	}
	if len(expected) > 0 {
		if len(keys) == 0 {
			return nil, refuse(codes.FailedPrecondition, "UDB_CAS_ROW_MISSING", "CAS row is missing")
		}
		if len(keys) != 1 || !holds(table[keys[0]], expected) {
			return nil, refuse(codes.FailedPrecondition, "UDB_CAS_CONFLICT", "compare-and-swap precondition failed")
		}
	}
	if err := f.checkRevision(identity, e, filter, req.GetExpectedRevision()); err != nil {
		return nil, err
	}
	if want := req.GetRequireAffected(); want > 0 && uint32(len(keys)) != want {
		return nil, refuse(codes.NotFound, "UDB_NO_ROWS_AFFECTED",
			fmt.Sprintf("the write matched %d row(s) but require_affected is %d; nothing was changed", len(keys), want))
	}
	for _, key := range keys {
		if e.softDelete == "" {
			delete(table, key)
		} else {
			row := copyRow(table[key])
			row[e.softDelete] = time.Now().UTC().Format(time.RFC3339Nano)
			table[key] = row
		}
	}
	return &entityv1.MutationResponse{AffectedRows: int64(len(keys))}, nil
}

// BeginTx applies a transaction's mutations all-or-nothing: any refusal rolls
// every earlier mutation back, as the broker's transaction does. Outbox events
// it carries are recorded (see Events) only when the transaction commits.
func (f *Fake) BeginTx(stream servicesv1.DataBroker_BeginTxServer) error {
	ctx := stream.Context()
	var muts []*entityv1.Mutation
	var terminal *entityv1.Mutation
	for {
		m, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if terminal != nil {
			return refuse(codes.InvalidArgument, "", "mutation after transaction terminal marker")
		}
		if err := bindBody(ctx, m.GetContext()); err != nil {
			return err
		}
		if m.GetCommit() && m.GetRollback() {
			return refuse(codes.InvalidArgument, "", "commit and rollback cannot both be set")
		}
		if m.GetOperation() != "" {
			operation := strings.ToLower(m.GetOperation())
			m.Operation = operation
			action, resource := "", m.GetMessageType()
			switch operation {
			case "upsert", "insert":
				action = "Upsert"
			case "update":
				action = "Update"
			case "delete":
				action = "Delete"
			case "enqueue_outbox_event":
				action = "EnqueueOutboxEvent"
				resource = m.GetCollection()
				if resource == "" {
					resource = m.GetMessageType()
				}
			default:
				return refuse(codes.InvalidArgument, "", "unsupported transaction operation "+operation)
			}
			if strings.TrimSpace(m.GetIdempotencyKey()) != "" {
				field := fmt.Sprintf("mutations[%d].idempotency_key", len(muts))
				return &refusal{code: codes.InvalidArgument,
					message: "this transaction path cannot honour idempotency_key; nothing was written",
					detail: &entityv1.ErrorDetail{Kind: entityv1.ErrorKind_ERROR_KIND_VALIDATION,
						FieldViolations: []*entityv1.ErrorFieldViolation{{Field: field, Description: "not supported on this operation inside BeginTx; leave it empty"}}}}
			}
			if err := f.authorize(ctx, resource, action); err != nil {
				return err
			}
			muts = append(muts, m)
		}
		if m.GetCommit() || m.GetRollback() {
			terminal = m
		}
	}
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if terminal == nil {
		return refuse(codes.FailedPrecondition, "", "transaction closed without commit or rollback")
	}
	if terminal.GetRollback() {
		return stream.Send(&entityv1.TxStatus{TxId: terminal.GetTxId(), State: entityv1.TxStatus_TX_STATE_ROLLED_BACK})
	}
	f.mu.Lock()
	snapshot := f.snapshot()
	revisions := make(map[revisionKey]int64, len(f.revisions))
	for key, value := range f.revisions {
		revisions[key] = value
	}
	events := len(f.events)
	err := f.applyTx(callerIdentity(ctx), muts)
	if err != nil {
		f.rows = snapshot
		f.revisions = revisions
		f.events = f.events[:events]
	}
	f.mu.Unlock()
	if err != nil {
		return err
	}
	return stream.Send(&entityv1.TxStatus{TxId: terminal.GetTxId(), State: entityv1.TxStatus_TX_STATE_COMMITTED, Message: fmt.Sprintf("%d mutation(s) committed", len(muts))})
}

func (f *Fake) applyTx(identity Identity, muts []*entityv1.Mutation) error {
	for _, m := range muts {
		var err error
		switch m.GetOperation() {
		case "upsert", "insert":
			_, err = f.writeLocked(identity, &entityv1.UpsertRequest{MessageType: m.GetMessageType(), RecordJson: m.GetRecordJson(), Payload: m.GetPayload(), Expected: m.GetExpected()}, m.GetOperation() == "insert")
		case "update":
			_, err = f.updateLocked(identity, &entityv1.UpdateRequest{MessageType: m.GetMessageType(), Filter: m.GetFilter(), Changes: m.GetChanges(), Expected: m.GetExpected(), Increments: m.GetIncrements()})
		case "delete":
			_, err = f.deleteLocked(identity, &entityv1.DeleteRequest{MessageType: m.GetMessageType(), Filter: m.GetFilter(), Expected: m.GetExpected()})
		case "enqueue_outbox_event":
			envelope := m.GetPayload().AsMap()
			if envelope["document_id"] != m.GetObjectKey() || m.GetObjectKey() == "" {
				return refuse(codes.InvalidArgument, "", "outbox partition_key must equal payload.document_id")
			}
			if given, ok := envelope["tenant_id"].(string); ok && given != "" && given != identity.TenantID {
				return refuse(codes.PermissionDenied, "UDB_TENANT_MISMATCH", "outbox tenant differs from verified tenant")
			}
			if given, ok := envelope["project_id"].(string); ok && given != "" && given != identity.ProjectID {
				return refuse(codes.PermissionDenied, "UDB_POLICY_DENIED", "outbox project differs from verified project")
			}
			if envelope["event_id"] == nil || envelope["event_type"] == nil || envelope["envelope_version"] != float64(udbclient.EventEnvelopeVersion) {
				return refuse(codes.InvalidArgument, "", "outbox needs a versioned event envelope")
			}
			envelope["tenant_id"], envelope["project_id"] = identity.TenantID, identity.ProjectID
			topic := m.GetCollection()
			if topic == "" {
				topic = m.GetMessageType()
			}
			f.events = append(f.events, Event{Topic: topic, PartitionKey: m.GetObjectKey(), Envelope: copyRow(envelope)})
		default:
			err = refuse(codes.InvalidArgument, "", "unsupported transaction operation "+m.GetOperation())
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Emit appends an event as if a transaction of tenant had committed it, for
// tests that only exercise a consumer.
func (f *Fake) Emit(tenant, topic, partitionKey string, payload map[string]any) string {
	f.mu.Lock()
	f.emitted++
	id := fmt.Sprintf("0190f1b2-0000-4000-8000-%012x", f.emitted)
	f.events = append(f.events, Event{Topic: topic, PartitionKey: partitionKey, Envelope: map[string]any{
		"event_id": id, "event_type": topic, "tenant_id": tenant, "document_id": partitionKey,
		"correlation_id": id, "envelope_version": float64(udbclient.EventEnvelopeVersion), "payload": copyRow(payload), "project_id": "",
	}})
	f.mu.Unlock()
	return id
}

func topicMatches(pattern, topic string) bool {
	if pattern == "*" || pattern == topic {
		return true
	}
	if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
		return strings.HasPrefix(topic, prefix)
	}
	return false
}

// Event is one outbox event a committed transaction emitted.
type Event struct {
	Topic        string
	PartitionKey string
	Envelope     map[string]any
}

// Events returns the outbox events committed so far, in commit order.
func (f *Fake) Events() []Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Event, len(f.events))
	for i, e := range f.events {
		out[i] = Event{Topic: e.Topic, PartitionKey: e.PartitionKey, Envelope: copyRow(e.Envelope)}
	}
	return out
}

func (f *Fake) snapshot() map[string]map[string]map[string]map[string]any {
	out := make(map[string]map[string]map[string]map[string]any, len(f.rows))
	for messageType, byTenant := range f.rows {
		copied := make(map[string]map[string]map[string]any, len(byTenant))
		for tenant, table := range byTenant {
			rows := make(map[string]map[string]any, len(table))
			for key, row := range table {
				rows[key] = copyRow(row)
			}
			copied[tenant] = rows
		}
		out[messageType] = copied
	}
	return out
}

func (f *Fake) table(messageType, tenant string) map[string]map[string]any {
	byTenant, ok := f.rows[messageType]
	if !ok {
		byTenant = map[string]map[string]map[string]any{}
		f.rows[messageType] = byTenant
	}
	table, ok := byTenant[tenant]
	if !ok {
		table = map[string]map[string]any{}
		byTenant[tenant] = table
	}
	return table
}

// checkTenant refuses a filter that pins a different tenant (the broker's
// UDB_TENANT_MISMATCH); a filter without one is scoped to the caller anyway.
func (e *entity) checkTenant(filter map[string]any, tenant string) error {
	if e.tenantColumn == "" {
		if _, ok := filter["tenant_id"]; ok {
			return refuse(codes.InvalidArgument, "UDB_TABLE_NOT_TENANT_SCOPED", "the entity has no tenant column")
		}
		return nil
	}
	value := filter[e.tenantColumn]
	if options, ok := value.(map[string]any); ok && len(options) == 1 {
		value = options["$eq"]
	}
	if pinned, ok := value.(string); ok && pinned != tenant {
		return refuse(codes.PermissionDenied, "UDB_TENANT_MISMATCH", "the request names a different tenant than the caller's verified tenant")
	}
	return nil
}

func (e *entity) keyOf(record map[string]any) (string, error) {
	if len(e.primaryKey) == 0 {
		return "", refuse(codes.FailedPrecondition, "", e.messageType+" declares no primary key")
	}
	parts := make([]any, 0, len(e.primaryKey))
	for _, column := range e.primaryKey {
		value, ok := record[column]
		if !ok || value == nil || value == "" {
			return "", columnRefusal(column)
		}
		parts = append(parts, value)
	}
	raw, err := json.Marshal(parts)
	if err != nil {
		return "", refuse(codes.InvalidArgument, "", "primary key is not a JSON value")
	}
	return string(raw), nil
}

// ── filter evaluation (the broker's grammar) ──────────────────────────────────

var supportedOperators = map[string]bool{
	"$eq": true, "$ne": true, "$gt": true, "$gte": true, "$lt": true, "$lte": true,
	"$in": true, "$nin": true, "$between": true, "$is_null": true, "$not_null": true,
	"$not": true, "$like": true, "$ilike": true,
}

// validateFilter refuses the shapes the broker refuses at plan time, before
// any row is read: an unknown operator and an equality with NULL.
func validateFilter(filter map[string]any) error {
	for key, cond := range filter {
		name := strings.TrimSpace(key)
		if strings.EqualFold(name, "$and") || strings.EqualFold(name, "$or") {
			items, ok := cond.([]any)
			if !ok || len(items) == 0 {
				return refuse(codes.InvalidArgument, "", "boolean filter requires a nonempty array of objects")
			}
			for _, item := range items {
				sub, ok := item.(map[string]any)
				if !ok || len(sub) == 0 {
					return refuse(codes.InvalidArgument, "", "boolean filter child must be a nonempty object")
				}
				if err := validateFilter(sub); err != nil {
					return err
				}
			}
			continue
		}
		if cond == nil {
			return refuse(codes.InvalidArgument, "UDB_NULL_COMPARISON", "comparison with NULL matches no rows; use $is_null")
		}
		if err := validateOperators(cond); err != nil {
			return err
		}
	}
	return nil
}

func validateOperators(cond any) error {
	ops, ok := cond.(map[string]any)
	if !ok {
		return nil
	}
	for op, arg := range ops {
		if !supportedOperators[strings.ToLower(op)] {
			return refuse(codes.InvalidArgument, "UDB_UNSUPPORTED_FILTER_OPERATOR", "unsupported filter operator "+op)
		}
		if (strings.EqualFold(op, "$eq") || strings.EqualFold(op, "$ne")) && arg == nil {
			return refuse(codes.InvalidArgument, "UDB_NULL_COMPARISON", "use $is_null or $not_null instead of comparing NULL")
		}
		if strings.EqualFold(op, "$in") || strings.EqualFold(op, "$nin") || strings.EqualFold(op, "$between") {
			items, ok := arg.([]any)
			if !ok || (strings.EqualFold(op, "$between") && len(items) != 2) {
				return refuse(codes.InvalidArgument, "", "filter operator requires an array of values")
			}
		}
		if strings.EqualFold(op, "$not") {
			if err := validateOperators(arg); err != nil {
				return err
			}
		}
	}
	return nil
}

func matches(row map[string]any, filter map[string]any) (bool, error) {
	for key, cond := range filter {
		switch strings.ToLower(key) {
		case "$and", "$or":
			items, _ := cond.([]any)
			anyMatched := false
			for _, item := range items {
				sub, _ := item.(map[string]any)
				ok, err := matches(row, sub)
				if err != nil {
					return false, err
				}
				if strings.EqualFold(key, "$and") && !ok {
					return false, nil
				}
				anyMatched = anyMatched || ok
			}
			if strings.EqualFold(key, "$or") && !anyMatched {
				return false, nil
			}
			continue
		}
		ok, err := matchColumn(row[key], cond)
		if err != nil || !ok {
			return false, err
		}
	}
	return true, nil
}

func matchColumn(value, cond any) (bool, error) {
	result, err := columnTruth(value, cond)
	return result == filterTrue, err
}

type filterTruth uint8

const (
	filterFalse filterTruth = iota
	filterTrue
	filterUnknown
)

func boolTruth(value bool) filterTruth {
	if value {
		return filterTrue
	}
	return filterFalse
}

// SQL uses three-valued logic: NULL <> x and NOT(NULL = x) stay unknown,
// and cannot select a row. Preserve that state through nested NOT predicates.
func columnTruth(value, cond any) (filterTruth, error) {
	ops, isOps := cond.(map[string]any)
	if !isOps {
		if cond == nil {
			return filterFalse, refuse(codes.InvalidArgument, "UDB_NULL_COMPARISON", "comparison with NULL matches no rows; use $is_null")
		}
		if value == nil {
			return filterUnknown, nil
		}
		return boolTruth(equal(value, cond)), nil
	}
	unknown := false
	for op, arg := range ops {
		var ok bool
		switch strings.ToLower(op) {
		case "$is_null", "$not_null", "$not", "$in", "$nin":
		default:
			if value == nil || arg == nil {
				unknown = true
				continue
			}
		}
		switch strings.ToLower(op) {
		case "$eq":
			ok = equal(value, arg)
		case "$ne":
			ok = !equal(value, arg)
		case "$gt":
			ok = value != nil && compare(value, arg) > 0
		case "$gte":
			ok = value != nil && compare(value, arg) >= 0
		case "$lt":
			ok = value != nil && compare(value, arg) < 0
		case "$lte":
			ok = value != nil && compare(value, arg) <= 0
		case "$in", "$nin":
			items, _ := arg.([]any)
			if len(items) == 0 {
				ok = strings.EqualFold(op, "$nin")
				break
			}
			if value == nil {
				unknown = true
				continue
			}
			found := false
			hasNull := false
			for _, item := range items {
				hasNull = hasNull || item == nil
				if equal(value, item) {
					found = true
				}
			}
			if !found && hasNull {
				unknown = true
				continue
			}
			ok = found == strings.EqualFold(op, "$in")
		case "$between":
			bounds, _ := arg.([]any)
			if len(bounds) == 2 && (bounds[0] == nil || bounds[1] == nil) {
				unknown = true
				continue
			}
			ok = len(bounds) == 2 && value != nil && compare(value, bounds[0]) >= 0 && compare(value, bounds[1]) <= 0
		case "$is_null":
			want, _ := arg.(bool)
			if _, isBool := arg.(bool); !isBool {
				want = true
			}
			ok = (value == nil) == want
		case "$not_null":
			want, _ := arg.(bool)
			if _, isBool := arg.(bool); !isBool {
				want = true
			}
			ok = (value != nil) == want
		case "$not":
			inner, err := columnTruth(value, arg)
			if err != nil {
				return filterFalse, err
			}
			if inner == filterUnknown {
				unknown = true
				continue
			}
			ok = inner == filterFalse
		case "$like", "$ilike":
			pattern, _ := arg.(string)
			text, _ := value.(string)
			if strings.EqualFold(op, "$ilike") {
				pattern, text = strings.ToLower(pattern), strings.ToLower(text)
			}
			ok = likeMatch(text, pattern)
		default:
			return filterFalse, refuse(codes.InvalidArgument, "UDB_UNSUPPORTED_FILTER_OPERATOR", "unsupported filter operator "+op)
		}
		if !ok {
			return filterFalse, nil
		}
	}
	if unknown {
		return filterUnknown, nil
	}
	return filterTrue, nil
}

// holds reports whether every expected column equals the row's value.
func holds(row, expected map[string]any) bool {
	for column, want := range expected {
		if !equal(row[column], want) {
			return false
		}
	}
	return true
}

func equal(a, b any) bool {
	if left, right, ok := numberPair(a, b); ok {
		return left.Cmp(right) == 0
	}
	if left, ok := a.(string); ok {
		if right, ok := b.(string); ok {
			if l, err := time.Parse(time.RFC3339Nano, left); err == nil {
				if r, err := time.Parse(time.RFC3339Nano, right); err == nil {
					return l.Equal(r)
				}
			}
			return left == right
		}
	}
	left, err := json.Marshal(a)
	if err != nil {
		return false
	}
	right, err := json.Marshal(b)
	return err == nil && bytes.Equal(left, right)
}

func compare(a, b any) int {
	if left, right, ok := numberPair(a, b); ok {
		return left.Cmp(right)
	}
	if left, ok := a.(string); ok {
		if right, ok := b.(string); ok {
			if l, err := time.Parse(time.RFC3339Nano, left); err == nil {
				if r, err := time.Parse(time.RFC3339Nano, right); err == nil {
					return l.Compare(r)
				}
			}
		}
	}
	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
}

func likeMatch(text, pattern string) bool {
	if pattern == "" {
		return text == ""
	}
	switch pattern[0] {
	case '%':
		for i := 0; i <= len(text); i++ {
			if likeMatch(text[i:], pattern[1:]) {
				return true
			}
		}
		return false
	case '_':
		return text != "" && likeMatch(text[1:], pattern[1:])
	}
	return text != "" && text[0] == pattern[0] && likeMatch(text[1:], pattern[1:])
}

// ── helpers ───────────────────────────────────────────────────────────────────

func copyRow(row map[string]any) map[string]any {
	out := make(map[string]any, len(row))
	for k, v := range row {
		out[k] = copyValue(v)
	}
	return out
}

// refuse builds a status carrying the broker's typed ErrorDetail trailer with
// reason, exactly as the real broker sends it.
func refuse(code codes.Code, reason, message string) error {
	return &refusal{code: code, reason: reason, message: message}
}

type refusal struct {
	code    codes.Code
	reason  string
	message string
	detail  *entityv1.ErrorDetail
}

func (r *refusal) Error() string { return r.message }

func (r *refusal) GRPCStatus() *status.Status { return status.New(r.code, r.message) }

// detailTrailer attaches the ErrorDetail trailer for refusals (installed as the
// server's unary interceptor in New).
func detailTrailer(ctx context.Context, err error) {
	r, ok := err.(*refusal)
	if !ok || (r.reason == "" && r.detail == nil) {
		return
	}
	_ = grpc.SetTrailer(ctx, detailMetadata(r))
}

func detailMetadata(r *refusal) metadata.MD {
	detail := r.detail
	if detail == nil {
		detail = &entityv1.ErrorDetail{Reason: r.reason, Kind: errorKind(r.code, r.reason)}
	}
	raw, _ := proto.Marshal(detail)
	return metadata.Pairs("udb-error-detail-bin", string(raw))
}
