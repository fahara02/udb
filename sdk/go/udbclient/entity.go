package udbclient

import (
	"context"
	"encoding/json"
	"fmt"

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

// Entity binds a message FQN + primary key once so CRUD calls stay terse.
type Entity struct {
	client *Client
	fqn    string
	key    EntityKey
	meta   Metadata
}

// Entity binds a message FQN + key over this Client.
func (c *Client) Entity(fqn string, key EntityKey) *Entity {
	return &Entity{client: c, fqn: fqn, key: key, meta: c.Meta}
}

// Entity forwards to the data-plane Client's binder so both u.Entity(...) and
// u.Data.Entity(...) surfaces from the masterplan exist.
func (u *Udb) Entity(fqn string, key EntityKey) *Entity { return u.Data.Entity(fqn, key) }

// requestContext returns a FRESH per-call RequestContext seeded with tenant/
// project from meta only. It never sets body scopes (the broker derives effective
// scopes from the verified claim — body scopes are requested-only) and never
// forces primary_read (reads stay replica-eligible unless an explicit fence /
// consistency choice is made via AfterWrite on the returned context).
func (e *Entity) requestContext() *entityv1.RequestContext {
	return &entityv1.RequestContext{
		TenantId:  e.meta.TenantID,
		ProjectId: e.meta.ProjectID,
	}
}

// UpsertOption configures Upsert.
type UpsertOption func(*upsertOptions)

type upsertOptions struct {
	returnRecord bool
}

// ReturnRecord requests that Upsert decode the MutationResponse.record_json the
// broker already returns on the SAME response — it does NOT issue a second Get.
func ReturnRecord() UpsertOption { return func(o *upsertOptions) { o.returnRecord = true } }

// UpsertResult carries an Upsert outcome. Record is populated only when
// ReturnRecord() was passed and the broker returned a record body.
type UpsertResult struct {
	Response *entityv1.MutationResponse
	Record   map[string]any
}

// Upsert marshals record (a map[string]any or json.RawMessage) to record_json,
// builds the UpsertRequest with conflict_fields from the bound key, and issues
// exactly ONE Upsert RPC. No proof Get afterward unless ReturnRecord() is given.
func (e *Entity) Upsert(ctx context.Context, record any, opts ...UpsertOption) (*UpsertResult, error) {
	var o upsertOptions
	for _, opt := range opts {
		opt(&o)
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
	})
	if err != nil {
		return nil, err
	}
	out := &UpsertResult{Response: resp}
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
	Table       string
	PrimaryKeys []string
	GoType      string
}
