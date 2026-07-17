package udbclient

import (
	"context"
	"encoding/json"
	"fmt"
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
	resp, err := e.client.Upsert(ctx, &entityv1.UpsertRequest{
		Context:        e.requestContext(),
		MessageType:    e.fqn,
		RecordJson:     b,
		ConflictFields: []string(e.key),
		ReturnRecord:   o.returnRecord,
		IdempotencyKey: o.idempotencyKey,
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

// Delete issues exactly ONE Delete RPC for the bound FQN with a Struct filter
// built from where. Delete is a mutation/destructive RPC and is never
// auto-retried (see retryableForRPC).
func (e *Entity) Delete(ctx context.Context, where map[string]any) (*entityv1.MutationResponse, error) {
	filter, err := structFilter(where)
	if err != nil {
		return nil, err
	}
	return e.client.Delete(ctx, &entityv1.DeleteRequest{
		Context:     e.requestContext(),
		MessageType: e.fqn,
		Filter:      filter,
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
