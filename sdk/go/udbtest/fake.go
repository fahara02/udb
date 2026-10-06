// Package udbtest is an in-memory udb broker for unit tests.
//
// It serves the DataBroker data verbs (Select, Upsert, Update, Delete) and the
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
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	authnv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/authn/services/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	eventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/events/v1"
	servicesv1 "github.com/fahara02/udb/sdk/go/gen/udb/services/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Fake is the in-memory broker.
type Fake struct {
	servicesv1.UnimplementedDataBrokerServer
	authn fakeAuthn

	addr string

	mu       sync.Mutex
	entities map[string]*entity
	// rows: message type → tenant → primary-key string → row.
	rows   map[string]map[string]map[string]map[string]any
	keys   map[string]string // API key → tenant
	events []Event
	// cursors: tenant + consumer + topic pattern → last acknowledged event id.
	cursors map[string]string
	emitted int
}

type entity struct {
	messageType  string
	primaryKey   []string
	tenantColumn string
	columns      map[string]protoreflect.FieldDescriptor
}

// New starts a fake serving the given entity messages (their udb column
// annotations define keys and the tenant column). It stops when t ends.
func New(t testing.TB, entities ...proto.Message) *Fake {
	t.Helper()
	f := &Fake{
		entities: map[string]*entity{},
		rows:     map[string]map[string]map[string]map[string]any{},
		keys:     map[string]string{},
		cursors:  map[string]string{},
	}
	f.authn.fake = f
	for _, m := range entities {
		f.Register(m)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("udbtest: listen: %v", err)
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			detailTrailer(ctx, err)
		}
		return resp, err
	}))
	servicesv1.RegisterDataBrokerServer(srv, f)
	authnv1.RegisterAuthnServiceServer(srv, &f.authn)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	f.addr = lis.Addr().String()
	return f
}

// Register adds an entity message type to the fake.
func (f *Fake) Register(m proto.Message) {
	desc := m.ProtoReflect().Descriptor()
	e := &entity{
		messageType: udbclient.MessageType(m),
		primaryKey:  udbclient.PrimaryKeys(m),
		columns:     map[string]protoreflect.FieldDescriptor{},
	}
	for i := 0; i < desc.Fields().Len(); i++ {
		fd := desc.Fields().Get(i)
		e.columns[string(fd.Name())] = fd
	}
	e.tenantColumn = udbclient.TenantColumn(m)
	f.mu.Lock()
	f.entities[e.messageType] = e
	f.mu.Unlock()
}

// Target is the fake's gRPC address.
func (f *Fake) Target() string { return f.addr }

// AddAPIKey makes key exchangeable for a bearer of tenant.
func (f *Fake) AddAPIKey(key, tenant string) {
	f.mu.Lock()
	f.keys[key] = tenant
	f.mu.Unlock()
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
	a.fake.mu.Lock()
	tenant, ok := a.fake.keys[req.GetApiKey()]
	a.fake.mu.Unlock()
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "unknown API key")
	}
	return &authnv1.AuthnResponse{
		AccessToken:   "udbtest-bearer:" + tenant,
		ExpiresAtUnix: time.Now().Add(time.Hour).Unix(),
		Principal: &authnv1.Principal{
			TenantId: tenant,
			Scopes:   []string{"udb:read", "udb:write"},
		},
	}, nil
}

// ── data verbs ────────────────────────────────────────────────────────────────

func callerTenant(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	if auth := md.Get("authorization"); len(auth) > 0 {
		if tenant, ok := strings.CutPrefix(auth[0], "Bearer udbtest-bearer:"); ok {
			return tenant
		}
	}
	if tenant := md.Get("x-tenant-id"); len(tenant) > 0 {
		return tenant[0]
	}
	return ""
}

func (f *Fake) entity(messageType string) (*entity, error) {
	e, ok := f.entities[messageType]
	if !ok {
		return nil, refuse(codes.InvalidArgument, "UDB_UNKNOWN_MESSAGE_TYPE",
			fmt.Sprintf("unknown message_type %s: the fake serves no such entity", messageType))
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
	tenant := callerTenant(ctx)
	filter := req.GetFilter().AsMap()
	if err := validateFilter(filter); err != nil {
		return nil, err
	}
	if err := e.checkTenant(filter, tenant); err != nil {
		return nil, err
	}
	var matched []map[string]any
	for _, row := range f.rows[e.messageType][tenant] {
		ok, err := matches(row, filter)
		if err != nil {
			return nil, err
		}
		if ok {
			matched = append(matched, row)
		}
	}
	order := append([]*entityv1.Sort(nil), req.GetSort()...)
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
		offset, _ = strconv.Atoi(string(raw))
	}
	out := &entityv1.RecordSet{}
	if req.GetIncludeTotal() {
		out.ExactTotal = int64(len(matched))
	}
	end := min(offset+limit, len(matched))
	for _, row := range matched[min(offset, len(matched)):end] {
		raw, _ := json.Marshal(row)
		out.RecordsJson = append(out.RecordsJson, raw)
		out.Rows = append(out.Rows, &entityv1.Row{})
	}
	out.TotalCount = int32(len(out.RecordsJson))
	out.HasMore = len(out.RecordsJson) >= limit
	if out.HasMore && req.GetLimit() > 0 && end < len(matched) {
		out.NextPageToken = base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}
	return out, nil
}

// Upsert inserts or replaces a whole row by primary key.
func (f *Fake) Upsert(ctx context.Context, req *entityv1.UpsertRequest) (*entityv1.MutationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.upsertLocked(callerTenant(ctx), req)
}

func (f *Fake) upsertLocked(tenant string, req *entityv1.UpsertRequest) (*entityv1.MutationResponse, error) {
	e, err := f.entity(req.GetMessageType())
	if err != nil {
		return nil, err
	}
	var record map[string]any
	if req.GetPayload() != nil {
		record = req.GetPayload().AsMap()
	} else if err := decodeJSON(req.GetRecordJson(), &record); err != nil {
		return nil, refuse(codes.InvalidArgument, "", "record_json must be valid JSON")
	}
	if e.tenantColumn != "" {
		if given, ok := record[e.tenantColumn].(string); ok && given != "" && given != tenant {
			return nil, refuse(codes.PermissionDenied, "UDB_TENANT_MISMATCH", "the record names a different tenant than the caller's")
		}
		record[e.tenantColumn] = tenant
	}
	key, err := e.keyOf(record)
	if err != nil {
		return nil, err
	}
	table := f.table(e.messageType, tenant)
	if expected := req.GetExpected().AsMap(); len(expected) > 0 {
		current, ok := table[key]
		if !ok {
			return nil, refuse(codes.FailedPrecondition, "UDB_CAS_ROW_MISSING", "compare-and-swap precondition failed: the target row does not exist")
		}
		if !holds(current, expected) {
			return nil, refuse(codes.FailedPrecondition, "UDB_CAS_CONFLICT", "compare-and-swap precondition failed: a field did not match the current row")
		}
	}
	table[key] = record
	return &entityv1.MutationResponse{MutationId: key, AffectedRows: 1}, nil
}

// Update sets the given columns (and applies increments) on the matched rows.
func (f *Fake) Update(ctx context.Context, req *entityv1.UpdateRequest) (*entityv1.MutationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.updateLocked(callerTenant(ctx), req)
}

func (f *Fake) updateLocked(tenant string, req *entityv1.UpdateRequest) (*entityv1.MutationResponse, error) {
	e, err := f.entity(req.GetMessageType())
	if err != nil {
		return nil, err
	}
	filter := req.GetFilter().AsMap()
	if err := validateFilter(filter); err != nil {
		return nil, err
	}
	if err := e.checkTenant(filter, tenant); err != nil {
		return nil, err
	}
	table := f.table(e.messageType, tenant)
	var keys []string
	for key, row := range table {
		ok, err := matches(row, filter)
		if err != nil {
			return nil, err
		}
		if ok {
			keys = append(keys, key)
		}
	}
	if expected := req.GetExpected().AsMap(); len(expected) > 0 {
		if len(keys) != 1 {
			return nil, refuse(codes.FailedPrecondition, "UDB_CAS_KEY_NOT_PK", "conditional mutation requires an equality filter on every primary-key column or on every column of a declared unique key")
		}
		if !holds(table[keys[0]], expected) {
			return nil, refuse(codes.FailedPrecondition, "UDB_CAS_CONFLICT", "compare-and-swap precondition failed: a field did not match the current row")
		}
	}
	if want := req.GetRequireAffected(); want > 0 && uint32(len(keys)) != want {
		return nil, refuse(codes.NotFound, "UDB_NO_ROWS_AFFECTED",
			fmt.Sprintf("the write matched %d row(s) but require_affected is %d; nothing was changed", len(keys), want))
	}
	changes := req.GetChanges().AsMap()
	for _, key := range keys {
		row := copyRow(table[key])
		for column, value := range changes {
			row[column] = value
		}
		for _, inc := range req.GetIncrements() {
			current, _ := toFloat(row[inc.GetColumn()])
			row[inc.GetColumn()] = current + inc.GetDelta()
		}
		table[key] = row
	}
	return &entityv1.MutationResponse{AffectedRows: int64(len(keys))}, nil
}

// Delete removes the matched rows.
func (f *Fake) Delete(ctx context.Context, req *entityv1.DeleteRequest) (*entityv1.MutationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deleteLocked(callerTenant(ctx), req)
}

func (f *Fake) deleteLocked(tenant string, req *entityv1.DeleteRequest) (*entityv1.MutationResponse, error) {
	e, err := f.entity(req.GetMessageType())
	if err != nil {
		return nil, err
	}
	filter := req.GetFilter().AsMap()
	if len(filter) == 0 {
		return nil, refuse(codes.InvalidArgument, "", "a delete needs a filter")
	}
	if err := validateFilter(filter); err != nil {
		return nil, err
	}
	if err := e.checkTenant(filter, tenant); err != nil {
		return nil, err
	}
	table := f.table(e.messageType, tenant)
	var keys []string
	for key, row := range table {
		ok, err := matches(row, filter)
		if err != nil {
			return nil, err
		}
		if ok {
			keys = append(keys, key)
		}
	}
	if expected := req.GetExpected().AsMap(); len(expected) > 0 {
		if len(keys) != 1 || !holds(table[keys[0]], expected) {
			return nil, refuse(codes.FailedPrecondition, "UDB_CAS_CONFLICT", "compare-and-swap precondition failed")
		}
	}
	if want := req.GetRequireAffected(); want > 0 && uint32(len(keys)) != want {
		return nil, refuse(codes.NotFound, "UDB_NO_ROWS_AFFECTED",
			fmt.Sprintf("the write matched %d row(s) but require_affected is %d; nothing was changed", len(keys), want))
	}
	for _, key := range keys {
		delete(table, key)
	}
	return &entityv1.MutationResponse{AffectedRows: int64(len(keys))}, nil
}

// BeginTx applies a transaction's mutations all-or-nothing: any refusal rolls
// every earlier mutation back, as the broker's transaction does. Outbox events
// it carries are recorded (see Events) only when the transaction commits.
func (f *Fake) BeginTx(stream servicesv1.DataBroker_BeginTxServer) error {
	tenant := callerTenant(stream.Context())
	var muts []*entityv1.Mutation
	for {
		m, err := stream.Recv()
		if err != nil {
			break
		}
		if m.GetOperation() != "" {
			muts = append(muts, m)
		}
		if m.GetCommit() || m.GetRollback() {
			break
		}
	}
	f.mu.Lock()
	snapshot := f.snapshot()
	events := len(f.events)
	err := f.applyTx(tenant, muts)
	if err != nil {
		f.rows = snapshot
		f.events = f.events[:events]
	}
	f.mu.Unlock()
	if err != nil {
		if r, ok := err.(*refusal); ok && r.reason != "" {
			stream.SetTrailer(detailMetadata(r))
		}
		return err
	}
	return stream.Send(&entityv1.TxStatus{State: entityv1.TxStatus_TX_STATE_COMMITTED, Message: fmt.Sprintf("%d mutation(s) committed", len(muts))})
}

func (f *Fake) applyTx(tenant string, muts []*entityv1.Mutation) error {
	for _, m := range muts {
		var err error
		switch m.GetOperation() {
		case "upsert":
			_, err = f.upsertLocked(tenant, &entityv1.UpsertRequest{MessageType: m.GetMessageType(), RecordJson: m.GetRecordJson(), Payload: m.GetPayload(), Expected: m.GetExpected()})
		case "update":
			_, err = f.updateLocked(tenant, &entityv1.UpdateRequest{MessageType: m.GetMessageType(), Filter: m.GetFilter(), Changes: m.GetChanges(), Expected: m.GetExpected(), Increments: m.GetIncrements()})
		case "delete":
			_, err = f.deleteLocked(tenant, &entityv1.DeleteRequest{MessageType: m.GetMessageType(), Filter: m.GetFilter(), Expected: m.GetExpected()})
		case "enqueue_outbox_event":
			envelope := m.GetPayload().AsMap()
			if envelope["document_id"] != m.GetObjectKey() {
				err = refuse(codes.InvalidArgument, "", "outbox partition_key must equal payload.document_id")
				break
			}
			envelope["tenant_id"] = tenant
			f.events = append(f.events, Event{Topic: m.GetCollection(), PartitionKey: m.GetObjectKey(), Envelope: envelope})
		default:
			err = refuse(codes.InvalidArgument, "", "unsupported transaction operation "+m.GetOperation())
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// PublishCDC streams committed events of the caller's tenant on the topic
// pattern, resuming a named consumer after its last acknowledged event, then
// follows new commits until the client goes away.
func (f *Fake) PublishCDC(req *entityv1.CDCSubscriptionRequest, stream grpc.ServerStreamingServer[eventsv1.CDCEnvelope]) error {
	tenant := callerTenant(stream.Context())
	pattern := req.GetTopicPattern()
	if pattern == "" {
		pattern = "*"
	}
	since := req.GetSinceEventId()
	f.mu.Lock()
	if since == "" && req.GetConsumerName() != "" {
		since = f.cursors[cursorKey(tenant, req.GetConsumerName(), pattern)]
	}
	next := 0
	if since != "" {
		next = -1
		for i, e := range f.events {
			if e.Envelope["event_id"] == since {
				next = i + 1
			}
		}
	}
	f.mu.Unlock()
	if next < 0 {
		return refuse(codes.NotFound, "", "CDC resume cursor is unknown or no longer retained")
	}
	for {
		f.mu.Lock()
		pending := append([]Event(nil), f.events[next:]...)
		next = len(f.events)
		f.mu.Unlock()
		for _, e := range pending {
			if e.Envelope["tenant_id"] != tenant || !topicMatches(pattern, e.Topic) {
				continue
			}
			raw, _ := json.Marshal(e.Envelope)
			id, _ := e.Envelope["event_id"].(string)
			if err := stream.Send(&eventsv1.CDCEnvelope{EventId: id, Topic: e.Topic, PartitionKey: e.PartitionKey, PayloadJson: string(raw)}); err != nil {
				return err
			}
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// AckCdcEvents stores a durable consumer's position.
func (f *Fake) AckCdcEvents(ctx context.Context, req *entityv1.AckCdcEventsRequest) (*entityv1.AckCdcEventsResponse, error) {
	if req.GetConsumerName() == "" || req.GetEventId() == "" {
		return nil, refuse(codes.InvalidArgument, "", "consumer_name and event_id are required")
	}
	pattern := req.GetTopicPattern()
	if pattern == "" {
		pattern = "*"
	}
	f.mu.Lock()
	f.cursors[cursorKey(callerTenant(ctx), req.GetConsumerName(), pattern)] = req.GetEventId()
	f.mu.Unlock()
	return &entityv1.AckCdcEventsResponse{ConsumerName: req.GetConsumerName(), TopicPattern: pattern, EventId: req.GetEventId(), AckedAtUnix: time.Now().Unix()}, nil
}

// Emit appends an event as if a transaction of tenant had committed it, for
// tests that only exercise a consumer.
func (f *Fake) Emit(tenant, topic, partitionKey string, payload map[string]any) string {
	f.mu.Lock()
	f.emitted++
	id := fmt.Sprintf("0190f1b2-0000-4000-8000-%012x", f.emitted)
	f.events = append(f.events, Event{Topic: topic, PartitionKey: partitionKey, Envelope: map[string]any{
		"event_id": id, "event_type": topic, "tenant_id": tenant, "document_id": partitionKey,
		"correlation_id": id, "envelope_version": float64(2), "payload": payload,
	}})
	f.mu.Unlock()
	return id
}

func cursorKey(tenant, consumer, pattern string) string {
	return tenant + "\x00" + consumer + "\x00" + pattern
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
	return append([]Event(nil), f.events...)
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
	if pinned, ok := filter[e.tenantColumn].(string); ok && pinned != tenant {
		return refuse(codes.PermissionDenied, "UDB_TENANT_MISMATCH", "the request names a different tenant than the caller's verified tenant")
	}
	return nil
}

func (e *entity) keyOf(record map[string]any) (string, error) {
	if len(e.primaryKey) == 0 {
		return "", refuse(codes.FailedPrecondition, "", e.messageType+" declares no primary key")
	}
	parts := make([]string, 0, len(e.primaryKey))
	for _, column := range e.primaryKey {
		value, ok := record[column]
		if !ok || value == nil || value == "" {
			return "", refuse(codes.InvalidArgument, "UDB_NOT_NULL_VIOLATION", "required field '"+column+"' is missing")
		}
		parts = append(parts, fmt.Sprint(value))
	}
	return strings.Join(parts, "\x00"), nil
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
		if strings.EqualFold(key, "$and") || strings.EqualFold(key, "$or") {
			items, _ := cond.([]any)
			for _, item := range items {
				sub, _ := item.(map[string]any)
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
	ops, isOps := cond.(map[string]any)
	if !isOps {
		if cond == nil {
			return false, refuse(codes.InvalidArgument, "UDB_NULL_COMPARISON", "comparison with NULL matches no rows; use $is_null")
		}
		return equal(value, cond), nil
	}
	for op, arg := range ops {
		var ok bool
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
			found := false
			for _, item := range items {
				if equal(value, item) {
					found = true
				}
			}
			ok = found == strings.EqualFold(op, "$in")
		case "$between":
			bounds, _ := arg.([]any)
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
			inner, err := matchColumn(value, arg)
			if err != nil {
				return false, err
			}
			ok = !inner
		case "$like", "$ilike":
			pattern, _ := arg.(string)
			text, _ := value.(string)
			if strings.EqualFold(op, "$ilike") {
				pattern, text = strings.ToLower(pattern), strings.ToLower(text)
			}
			ok = likeMatch(text, pattern)
		default:
			return false, refuse(codes.InvalidArgument, "UDB_UNSUPPORTED_FILTER_OPERATOR", "unsupported filter operator "+op)
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
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
	if af, ok := toFloat(a); ok {
		if bf, ok := toFloat(b); ok {
			return af == bf
		}
	}
	if as, ok := a.(string); ok {
		if bs, ok := b.(string); ok {
			if at, err := time.Parse(time.RFC3339Nano, as); err == nil {
				if bt, err := time.Parse(time.RFC3339Nano, bs); err == nil {
					return at.Equal(bt)
				}
			}
			return as == bs
		}
	}
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return bytes.Equal(ab, bb)
}

func compare(a, b any) int {
	if af, ok := toFloat(a); ok {
		if bf, ok := toFloat(b); ok {
			switch {
			case af < bf:
				return -1
			case af > bf:
				return 1
			}
			return 0
		}
	}
	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int64:
		return float64(x), true
	case int:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	}
	return 0, false
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

func decodeJSON(raw []byte, out *map[string]any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return err
	}
	for k, v := range *out {
		if n, ok := v.(json.Number); ok {
			if f, err := n.Float64(); err == nil {
				(*out)[k] = f
			}
		}
	}
	return nil
}

func copyRow(row map[string]any) map[string]any {
	out := make(map[string]any, len(row))
	for k, v := range row {
		out[k] = v
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
}

func (r *refusal) Error() string { return r.message }

func (r *refusal) GRPCStatus() *status.Status { return status.New(r.code, r.message) }

// detailTrailer attaches the ErrorDetail trailer for refusals (installed as the
// server's unary interceptor in New).
func detailTrailer(ctx context.Context, err error) {
	r, ok := err.(*refusal)
	if !ok || r.reason == "" {
		return
	}
	_ = grpc.SetTrailer(ctx, detailMetadata(r))
}

func detailMetadata(r *refusal) metadata.MD {
	detail := &entityv1.ErrorDetail{Reason: r.reason, Kind: entityv1.ErrorKind_ERROR_KIND_VALIDATION}
	raw, _ := proto.Marshal(detail)
	return metadata.Pairs("udb-error-detail-bin", string(raw))
}
