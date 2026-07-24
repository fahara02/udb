package udbclient

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// ── Bound Entity API (chapter 08.4) ──────────────────────────────────────────
//
// A thin, ergonomic layer over Client.Upsert/Select/Delete that hides
// record_json, google.protobuf.Struct filters, conflict_fields, and tenant/
// project defaults behind one call per write. The broker stays the RLS
// enforcement point — this never sets body scopes or forces primary reads. The
// hot-path guardrail: entity.Upsert is exactly one Upsert RPC unless a readback
// is explicitly requested via ReturnRecord().

// EntityKey is the ordered set of primary-key field names that become an Upsert's
// conflict_fields.
type EntityKey []string

// Key builds an EntityKey from primary-key field names, e.g. Key("record_id").
func Key(fields ...string) EntityKey { return EntityKey(fields) }

type EntityRelationDescriptor struct {
	Name              string   `json:"name"`
	Kind              string   `json:"kind"`
	LocalFields       []string `json:"local_fields"`
	TargetMessageType string   `json:"target_message_type"`
	TargetTable       string   `json:"target_table"`
	TargetFields      []string `json:"target_fields"`
	OnDelete          string   `json:"on_delete,omitempty"`
	OnUpdate          string   `json:"on_update,omitempty"`
}

// Entity binds a message FQN + primary key once so CRUD calls stay terse.
type Entity struct {
	client      *Client
	fqn         string
	key         EntityKey
	meta        Metadata
	consistency ConsistencyMode
}

// Entity binds a message FQN + key over this Client.
func (c *Client) Entity(fqn string, key EntityKey) *Entity {
	return &Entity{client: c, fqn: fqn, key: key, meta: c.Meta}
}

// Entity forwards to the data-plane Client's binder so both u.Entity(...) and
// u.Data.Entity(...) surfaces from the masterplan exist.
func (u *Udb) Entity(fqn string, key EntityKey) *Entity { return u.Data.Entity(fqn, key) }

// WithConsistency returns a shallow copy of the Entity whose reads/writes stamp
// the requested consistency mode on their per-call RequestContext
// (RequestContext.consistency_mode). The broker stays the enforcement point —
// this only expresses the caller's preference (strong / read-your-writes /
// bounded-staleness / eventual, etc.). Passing an empty mode clears it.
func (e *Entity) WithConsistency(mode ConsistencyMode) *Entity {
	cp := *e
	cp.consistency = mode
	return &cp
}

// requestContext returns a FRESH per-call RequestContext seeded with tenant/
// project from meta only. It never sets body scopes (the broker derives effective
// scopes from the verified claim — body scopes are requested-only) and never
// forces primary_read (reads stay replica-eligible unless an explicit fence /
// consistency choice is made via AfterWrite on the returned context or a
// consistency mode was selected with WithConsistency).
func (e *Entity) requestContext() *entityv1.RequestContext {
	rc := &entityv1.RequestContext{
		TenantId:  e.meta.TenantID,
		ProjectId: e.meta.ProjectID,
	}
	// Stamp the caller's consistency preference (if any) onto this one request.
	e.consistency.Apply(rc)
	return rc
}

// UpsertOption configures Upsert.
type UpsertOption func(*upsertOptions)

type upsertOptions struct {
	returnRecord   bool
	idempotencyKey string
	expected       map[string]any
}

// ReturnRecord requests that Upsert decode the MutationResponse.record_json the
// broker already returns on the SAME response — it does NOT issue a second Get.
func ReturnRecord() UpsertOption { return func(o *upsertOptions) { o.returnRecord = true } }

// WithIdempotencyKey attaches a caller-supplied durable idempotency key to the
// Upsert. The broker deduplicates replays of the SAME key (surfacing
// WasDuplicate) so an ambiguous client/network retry cannot create a second row
// or repeat a side effect, and it re-enables the generated mutation retry policy
// for this bound-entity path. A key that is present but only whitespace is
// rejected; an unset key leaves the request's idempotency_key empty (unchanged).
func WithIdempotencyKey(key string) UpsertOption {
	return func(o *upsertOptions) { o.idempotencyKey = key }
}

// WithExpected makes the Upsert a compare-and-swap: each field -> value
// assertion must equal the CURRENT row (located by the bound entity key and
// row-locked) inside the same transaction and tenant/RLS context as the write.
// If the row is absent or any assertion fails, the broker returns
// FAILED_PRECONDITION and writes nothing — so an optimistic
// "update WHERE version = N" is atomic without raw SQL or an external lock.
// Combine with WithIdempotencyKey for replay-safety of the winning command.
// A nil/empty map leaves the request unconditional (unchanged behavior).
func WithExpected(expected map[string]any) UpsertOption {
	return func(o *upsertOptions) { o.expected = expected }
}

// UpsertResult carries an Upsert outcome. Record is populated only when
// ReturnRecord() was passed and the broker returned a record body.
type UpsertResult struct {
	Response *entityv1.MutationResponse
	Record   map[string]any
	// WasDuplicate is true when the broker collapsed this write onto a prior one
	// via durable idempotency (a replay of the same idempotency key) instead of
	// applying a fresh mutation. Mirrors MutationResponse.was_duplicate so a
	// caller can distinguish an idempotency replay from a fresh write.
	WasDuplicate bool
}

// Upsert marshals record (a map[string]any or json.RawMessage) to record_json,
// builds the UpsertRequest with conflict_fields from the bound key, and issues
// exactly ONE Upsert RPC. No proof Get afterward unless ReturnRecord() is given.
func (e *Entity) Upsert(ctx context.Context, record any, opts ...UpsertOption) (*UpsertResult, error) {
	var o upsertOptions
	for _, opt := range opts {
		opt(&o)
	}
	// A supplied idempotency key must not be whitespace-only — a blank key would
	// silently disable replay-safety while looking set. Empty (unset) is fine.
	if o.idempotencyKey != "" && strings.TrimSpace(o.idempotencyKey) == "" {
		return nil, fmt.Errorf("udb: idempotency key must not be whitespace-only")
	}
	b, err := toRecordJSON(record)
	if err != nil {
		return nil, err
	}
	// UDB-GO-005: an expected map becomes the request's compare-and-swap
	// precondition (google.protobuf.Struct); nil stays unconditional.
	var expected *structpb.Struct
	if len(o.expected) > 0 {
		expected, err = structFilter(o.expected)
		if err != nil {
			return nil, fmt.Errorf("udb: encode expected precondition: %w", err)
		}
	}
	resp, err := e.client.Upsert(ctx, &entityv1.UpsertRequest{
		Context:        e.requestContext(),
		MessageType:    e.fqn,
		RecordJson:     b,
		ConflictFields: []string(e.key),
		ReturnRecord:   o.returnRecord,
		IdempotencyKey: o.idempotencyKey,
		Expected:       expected,
	})
	if err != nil {
		return nil, err
	}
	out := &UpsertResult{Response: resp, WasDuplicate: resp.GetWasDuplicate()}
	if o.returnRecord {
		if rb := resp.GetRecordJson(); len(rb) > 0 {
			rec := map[string]any{}
			if err := json.Unmarshal(rb, &rec); err != nil {
				return nil, fmt.Errorf("udb: decode returned record_json: %w", err)
			}
			out.Record = rec
		}
	}
	return out, nil
}

// Select issues exactly ONE Select RPC for the bound FQN with a Struct filter
// built from where, and decodes the RecordSet rows into []map[string]any.
func (e *Entity) Select(ctx context.Context, where map[string]any) ([]map[string]any, error) {
	filter, err := structFilter(where)
	if err != nil {
		return nil, err
	}
	rs, err := e.client.Select(ctx, &entityv1.SelectRequest{
		Context:     e.requestContext(),
		MessageType: e.fqn,
		Filter:      filter,
	})
	if err != nil {
		return nil, err
	}
	return decodeRecordSet(rs)
}

// SortKey is one ordering key for a paged Select.
type SortKey struct {
	Field      string
	Descending bool
}

// PageOptions configures a keyset-paginated Select.
type PageOptions struct {
	// Fields restricts the projection; empty selects every column.
	Fields []string
	// Sort defines the order; the primary key is appended as a tiebreaker
	// server-side so the cursor is stable.
	Sort []SortKey
	// Limit is the page size. A non-zero limit engages keyset pagination.
	Limit int32
	// PageToken continues a walk; empty requests the first page.
	PageToken string
}

// Page is one page of a keyset-paginated read.
type Page struct {
	Rows []map[string]any
	// NextPageToken is the cursor for the following page — empty on the last
	// page. Feed it back verbatim as PageOptions.PageToken; it is opaque.
	NextPageToken string
	// TotalCount is the number of rows in THIS page (see decodeRecordSet).
	TotalCount int32
}

// SelectPage issues a single keyset-paginated Select for the bound FQN. Pass an
// empty PageToken for the first page, then feed each returned NextPageToken back
// until it comes back empty (P-1). This replaces the hand-built structpb filter +
// manual RecordSet decode a consumer otherwise writes for every listing endpoint.
func (e *Entity) SelectPage(ctx context.Context, where map[string]any, opts PageOptions) (*Page, error) {
	filter, err := structFilter(where)
	if err != nil {
		return nil, err
	}
	sort := make([]*entityv1.Sort, 0, len(opts.Sort))
	for _, s := range opts.Sort {
		sort = append(sort, &entityv1.Sort{Field: s.Field, Descending: s.Descending})
	}
	rs, err := e.client.Select(ctx, &entityv1.SelectRequest{
		Context:     e.requestContext(),
		MessageType: e.fqn,
		Filter:      filter,
		Fields:      opts.Fields,
		Limit:       opts.Limit,
		PageToken:   opts.PageToken,
		Sort:        sort,
	})
	if err != nil {
		return nil, err
	}
	rows, err := decodeRecordSet(rs)
	if err != nil {
		return nil, err
	}
	return &Page{
		Rows:          rows,
		NextPageToken: rs.GetNextPageToken(),
		TotalCount:    rs.GetTotalCount(),
	}, nil
}

// DeleteOption configures Delete.
type DeleteOption func(*deleteOptions)

type deleteOptions struct {
	expected map[string]any
}

// WithDeleteExpected makes the Delete a compare-and-swap (G-2): the row the filter
// targets is removed only if each field -> value still equals the CURRENT row
// (row-locked, in the same tenant/RLS transaction). If the row is absent or any
// assertion fails, the broker returns FAILED_PRECONDITION and deletes nothing.
// Requires the filter to pin every primary-key column by equality. A nil/empty
// map leaves the delete unconditional (unchanged behavior).
func WithDeleteExpected(expected map[string]any) DeleteOption {
	return func(o *deleteOptions) { o.expected = expected }
}

// Delete issues exactly ONE Delete RPC for the bound FQN with a Struct filter
// built from where. Delete is a mutation/destructive RPC and is never
// auto-retried (see retryableForRPC). Pass WithDeleteExpected for compare-and-swap.
func (e *Entity) Delete(ctx context.Context, where map[string]any, opts ...DeleteOption) (*entityv1.MutationResponse, error) {
	var o deleteOptions
	for _, opt := range opts {
		opt(&o)
	}
	filter, err := structFilter(where)
	if err != nil {
		return nil, err
	}
	var expected *structpb.Struct
	if len(o.expected) > 0 {
		expected, err = structFilter(o.expected)
		if err != nil {
			return nil, fmt.Errorf("udb: encode delete expected precondition: %w", err)
		}
	}
	return e.client.Delete(ctx, &entityv1.DeleteRequest{
		Context:     e.requestContext(),
		MessageType: e.fqn,
		Filter:      filter,
		Expected:    expected,
	})
}

// UpdateOption configures Update / Increment.
type UpdateOption func(*updateOptions)

type updateOptions struct {
	expected       map[string]any
	idempotencyKey string
	returnRecord   bool
}

// WithUpdateExpected makes the Update a compare-and-swap: every field -> value
// must still equal the CURRENT row (row-locked, same tenant/RLS transaction) or
// the broker returns FAILED_PRECONDITION and writes nothing. Requires the
// filter to pin every primary-key column by equality.
func WithUpdateExpected(expected map[string]any) UpdateOption {
	return func(o *updateOptions) { o.expected = expected }
}

// WithUpdateIdempotencyKey enables durable keyed replay: a retried Update with
// the same key returns the original response with was_duplicate=true.
func WithUpdateIdempotencyKey(key string) UpdateOption {
	return func(o *updateOptions) { o.idempotencyKey = key }
}

// WithUpdateReturnRecord asks the broker to return the post-update row.
func WithUpdateReturnRecord() UpdateOption {
	return func(o *updateOptions) { o.returnRecord = true }
}

// Update issues exactly ONE partial-update RPC: SET only the columns named in
// changes (a JSON null value writes SQL NULL; columns not named are untouched)
// on the rows matched by where. This replaces the Select -> merge -> full-record
// Upsert pattern — no merge helper, no NOT-NULL resend hazard, and combined
// with WithUpdateExpected it is an atomic guarded write.
func (e *Entity) Update(ctx context.Context, where map[string]any, changes map[string]any, opts ...UpdateOption) (*entityv1.MutationResponse, error) {
	return e.update(ctx, where, changes, nil, opts...)
}

// Increment applies atomic counter deltas (`column = column + delta`) on the
// matched rows in ONE statement — no read-modify-write lost-update window.
// Combine with Update by calling update via separate RPCs only when the two
// column sets are disjoint; a single call never names a column in both.
func (e *Entity) Increment(ctx context.Context, where map[string]any, deltas map[string]float64, opts ...UpdateOption) (*entityv1.MutationResponse, error) {
	return e.update(ctx, where, nil, deltas, opts...)
}

func (e *Entity) update(ctx context.Context, where map[string]any, changes map[string]any, deltas map[string]float64, opts ...UpdateOption) (*entityv1.MutationResponse, error) {
	var o updateOptions
	for _, opt := range opts {
		opt(&o)
	}
	filter, err := structFilter(where)
	if err != nil {
		return nil, err
	}
	var changesStruct *structpb.Struct
	if len(changes) > 0 {
		changesStruct, err = structFilter(changes)
		if err != nil {
			return nil, fmt.Errorf("udb: encode update changes: %w", err)
		}
	}
	var expected *structpb.Struct
	if len(o.expected) > 0 {
		expected, err = structFilter(o.expected)
		if err != nil {
			return nil, fmt.Errorf("udb: encode update expected precondition: %w", err)
		}
	}
	// Deterministic wire order for the increments (sorted by column).
	columns := make([]string, 0, len(deltas))
	for column := range deltas {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	increments := make([]*entityv1.UpdateRequest_Increment, 0, len(columns))
	for _, column := range columns {
		increments = append(increments, &entityv1.UpdateRequest_Increment{
			Column: column,
			Delta:  deltas[column],
		})
	}
	return e.client.Update(ctx, &entityv1.UpdateRequest{
		Context:        e.requestContext(),
		MessageType:    e.fqn,
		Filter:         filter,
		Changes:        changesStruct,
		Expected:       expected,
		Increments:     increments,
		IdempotencyKey: o.idempotencyKey,
		ReturnRecord:   o.returnRecord,
	})
}

func toRecordJSON(record any) ([]byte, error) {
	switch v := record.(type) {
	case nil:
		return nil, fmt.Errorf("udb: Upsert record is nil")
	case json.RawMessage:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		b, err := json.Marshal(record)
		if err != nil {
			return nil, fmt.Errorf("udb: marshal record: %w", err)
		}
		return b, nil
	}
}

func structFilter(where map[string]any) (*structpb.Struct, error) {
	if len(where) == 0 {
		return nil, nil
	}
	s, err := structpb.NewStruct(where)
	if err != nil {
		return nil, fmt.Errorf("udb: build filter struct: %w", err)
	}
	return s, nil
}

func decodeRecordSet(rs *entityv1.RecordSet) ([]map[string]any, error) {
	if rs == nil {
		return nil, nil
	}
	rows := rs.GetRecordsJson()
	out := make([]map[string]any, 0, len(rows))
	for _, raw := range rows {
		if len(raw) == 0 {
			continue
		}
		m := map[string]any{}
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("udb: decode record row: %w", err)
		}
		out = append(out, m)
	}
	return out, nil
}

// EntityDescriptor is a catalog-derived entity registry entry. It mirrors lane
// 07's canonical EntityDescriptor field-set; the generated @@UDB_ENTITY block in
// generated_client.go populates the Entities map with one per annotated entity
// message so (*Client).Entity can default conflict_fields/PK from the manifest
// instead of the caller passing Key(...).
type EntityDescriptor struct {
	Table        string
	PrimaryKeys  []string
	Fields       []string
	Relations    []EntityRelationDescriptor
	VersionField string
	TenantField  string
	ProjectField string
	GoType       string
}
