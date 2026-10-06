package udbclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// Typed entity stores.
//
// Table[T] reads and writes one entity message type through the DataBroker
// with typed messages in and out: no record maps, no JSON, no tenant column in
// filters (the broker fills the verified tenant in), and conflicts as errors
// you can test for. It is what every consumer used to write as its own store
// layer on top of map[string]any records.

// RowKey addresses one row: every primary-key column, or every column of one
// declared unique key, by equality.
type RowKey map[string]any

// Filter is a udb filter document: {"column": value} for equality, or
// {"column": {"$op": value}} with $eq $ne $gt $gte $lt $lte $in $nin $between
// $like $ilike $is_null $not_null $not; combine with $and / $or.
type Filter map[string]any

// ErrNotFound is returned when the addressed row does not exist (or is not
// visible to the caller).
var ErrNotFound = errors.New("udb: not found")

// ErrConflict is returned when a conditional write lost: the row changed since
// it was read. Read it again and retry (UpdateWithRetry does).
var ErrConflict = errors.New("udb: conflict: the row changed since it was read")

// Table is the typed store for entity message T.
type Table[T proto.Message] struct {
	u           *Udb
	messageType string
	newT        func() T
}

// TableOf returns the typed store for T, e.g.
//
//	notes := udbclient.TableOf[*notesv1.Note](u)
//	n, err := notes.Get(ctx, udbclient.RowKey{"note_id": id})
func TableOf[T proto.Message](u *Udb) *Table[T] {
	var zero T
	prototype := zero.ProtoReflect().Type()
	return &Table[T]{
		u:           u,
		messageType: MessageType(zero),
		newT:        func() T { return prototype.New().Interface().(T) },
	}
}

// MessageType is the entity's udb message type (its proto full name).
func (t *Table[T]) MessageType() string { return t.messageType }

// KeyOf builds the primary key of m from its primary-key fields.
func (t *Table[T]) KeyOf(m T) (RowKey, error) {
	rec, err := EncodeRecord(m, WriteInsert)
	if err != nil {
		return nil, err
	}
	key := RowKey{}
	for _, column := range PrimaryKeys(m) {
		value, ok := rec[column]
		if !ok || value == nil || value == "" {
			return nil, fmt.Errorf("udb: %s has no value for primary-key column %s", t.messageType, column)
		}
		key[column] = value
	}
	if len(key) == 0 {
		return nil, fmt.Errorf("udb: %s declares no primary key", t.messageType)
	}
	return key, nil
}

// SelectOptions shape a read.
type SelectOptions struct {
	// Limit is the page size (0 = the broker default of 100).
	Limit int
	// PageToken continues a previous page (TablePage.Next).
	PageToken string
	// OrderBy sorts by these columns ("-column" for descending); the primary
	// key always breaks ties, so the order is total.
	OrderBy []string
	// IncludeTotal also counts every matching row into TablePage.Total.
	IncludeTotal bool
	// Fields restricts the columns read (empty = every non-PII column).
	Fields []string
}

// TablePage is one page of typed rows.
type TablePage[T proto.Message] struct {
	Rows []T
	// HasMore is true when the page is full: more rows may match.
	HasMore bool
	// Next continues the walk (empty on the last page, or when no Limit was set).
	Next string
	// Total is every matching row when IncludeTotal was set.
	Total int64
	// Redacted lists the columns that came back as the redaction placeholder
	// because the caller lacks the PII read scope; never write those back.
	Redacted []string
}

// Get reads the row with key. A missing row is ErrNotFound.
func (t *Table[T]) Get(ctx context.Context, key RowKey) (T, error) {
	var zero T
	page, err := t.Select(ctx, Filter(key), SelectOptions{Limit: 2})
	if err != nil {
		return zero, err
	}
	switch len(page.Rows) {
	case 0:
		return zero, fmt.Errorf("%w: %s %v", ErrNotFound, t.messageType, map[string]any(key))
	case 1:
		return page.Rows[0], nil
	default:
		return zero, fmt.Errorf("udb: %s key %v matched more than one row; address a primary or unique key", t.messageType, map[string]any(key))
	}
}

// Select reads one page of rows matching where.
func (t *Table[T]) Select(ctx context.Context, where Filter, opts SelectOptions) (TablePage[T], error) {
	filter, err := structpb.NewStruct(jsonSafe(where))
	if err != nil {
		return TablePage[T]{}, fmt.Errorf("udb: %s filter: %w", t.messageType, err)
	}
	req := &entityv1.SelectRequest{
		MessageType:  t.messageType,
		Filter:       filter,
		Limit:        int32(opts.Limit),
		PageToken:    opts.PageToken,
		IncludeTotal: opts.IncludeTotal,
		Fields:       opts.Fields,
	}
	for _, column := range opts.OrderBy {
		desc := len(column) > 0 && column[0] == '-'
		if desc {
			column = column[1:]
		}
		req.Sort = append(req.Sort, &entityv1.Sort{Field: column, Descending: desc})
	}
	req.Context = t.u.readFence()
	res, err := t.u.Data.Broker.Select(ctx, req)
	if err != nil {
		return TablePage[T]{}, classify(err)
	}
	page := TablePage[T]{
		HasMore:  res.GetHasMore(),
		Next:     res.GetNextPageToken(),
		Total:    res.GetExactTotal(),
		Redacted: res.GetRedactedFields(),
	}
	for _, raw := range res.GetRecordsJson() {
		row, err := t.decode(raw)
		if err != nil {
			return TablePage[T]{}, err
		}
		page.Rows = append(page.Rows, row)
	}
	return page, nil
}

// SelectAll walks every row matching where, page by page, calling fn for each.
// fn returning an error stops the walk and returns it.
func (t *Table[T]) SelectAll(ctx context.Context, where Filter, fn func(T) error) error {
	opts := SelectOptions{Limit: 500}
	for {
		page, err := t.Select(ctx, where, opts)
		if err != nil {
			return err
		}
		for _, row := range page.Rows {
			if err := fn(row); err != nil {
				return err
			}
		}
		if page.Next == "" || !page.HasMore {
			return nil
		}
		opts.PageToken = page.Next
	}
}

// Count counts the rows matching where.
func (t *Table[T]) Count(ctx context.Context, where Filter) (int64, error) {
	page, err := t.Select(ctx, where, SelectOptions{Limit: 1, IncludeTotal: true})
	if err != nil {
		return 0, err
	}
	return page.Total, nil
}

// Upsert writes the whole row m: inserted when its key is new, replaced when it
// exists. To change some columns of an existing row use Patch or UpdateIf.
func (t *Table[T]) Upsert(ctx context.Context, m T) error {
	rec, err := EncodeRecord(m, WriteInsert)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("udb: encode %s: %w", t.messageType, err)
	}
	res, err := t.u.Data.Broker.Upsert(ctx, &entityv1.UpsertRequest{MessageType: t.messageType, RecordJson: raw})
	if err != nil {
		return classify(err)
	}
	t.u.rememberWrite(res)
	return nil
}

// Patch sets only the given columns on the row with key. The row must exist
// (a missing row is ErrNotFound and nothing is written).
func (t *Table[T]) Patch(ctx context.Context, key RowKey, fields Record) error {
	return t.update(ctx, key, fields, nil, nil)
}

// UpdateIf writes next's updatable columns only if the stored row still holds
// expected (column → value, compared by type on the broker). A lost race is
// ErrConflict and nothing is written.
func (t *Table[T]) UpdateIf(ctx context.Context, next T, expected Record) error {
	key, err := t.KeyOf(next)
	if err != nil {
		return err
	}
	changes, err := EncodeRecord(next, WriteUpdate)
	if err != nil {
		return err
	}
	return t.update(ctx, key, changes, expected, nil)
}

// UpdateWithRetry reads the row, applies change, and writes back only the
// columns change modified, guarded by their previous values; on a conflict it
// re-reads and tries again, up to attempts times (default 5).
func (t *Table[T]) UpdateWithRetry(ctx context.Context, key RowKey, attempts int, change func(current T) (T, error)) (T, error) {
	var zero T
	if attempts <= 0 {
		attempts = 5
	}
	for attempt := 0; attempt < attempts; attempt++ {
		current, err := t.Get(ctx, key)
		if err != nil {
			return zero, err
		}
		before, err := EncodeRecord(current, WriteUpdate)
		if err != nil {
			return zero, err
		}
		next, err := change(proto.Clone(current).(T))
		if err != nil {
			return zero, err
		}
		after, err := EncodeRecord(next, WriteUpdate)
		if err != nil {
			return zero, err
		}
		changes, expected := Record{}, Record{}
		for column, value := range after {
			if !reflect.DeepEqual(before[column], value) {
				changes[column] = value
				expected[column] = before[column]
			}
		}
		if len(changes) == 0 {
			return current, nil
		}
		err = t.update(ctx, key, changes, expected, nil)
		if errors.Is(err, ErrConflict) {
			continue
		}
		if err != nil {
			return zero, err
		}
		return next, nil
	}
	return zero, fmt.Errorf("%w: gave up after %d attempts", ErrConflict, attempts)
}

// Increment adds deltas to numeric columns of the row with key, atomically on
// the broker (col = col + delta), with no read-modify-write window.
func (t *Table[T]) Increment(ctx context.Context, key RowKey, deltas map[string]float64) error {
	return t.update(ctx, key, nil, nil, deltas)
}

// Delete removes the row with key. A missing row is ErrNotFound.
func (t *Table[T]) Delete(ctx context.Context, key RowKey) error {
	filter, err := structpb.NewStruct(jsonSafe(Filter(key)))
	if err != nil {
		return fmt.Errorf("udb: %s key: %w", t.messageType, err)
	}
	res, err := t.u.Data.Broker.Delete(ctx, &entityv1.DeleteRequest{
		MessageType:     t.messageType,
		Filter:          filter,
		RequireAffected: 1,
	})
	if err != nil {
		return classify(err)
	}
	t.u.rememberWrite(res)
	return nil
}

func (t *Table[T]) update(ctx context.Context, key RowKey, changes, expected Record, deltas map[string]float64) error {
	filter, err := structpb.NewStruct(jsonSafe(Filter(key)))
	if err != nil {
		return fmt.Errorf("udb: %s key: %w", t.messageType, err)
	}
	req := &entityv1.UpdateRequest{
		MessageType:     t.messageType,
		Filter:          filter,
		RequireAffected: 1,
	}
	if len(changes) > 0 {
		if req.Changes, err = structpb.NewStruct(jsonSafe(changes)); err != nil {
			return fmt.Errorf("udb: %s changes: %w", t.messageType, err)
		}
	}
	if len(expected) > 0 {
		if req.Expected, err = structpb.NewStruct(jsonSafe(expected)); err != nil {
			return fmt.Errorf("udb: %s expected: %w", t.messageType, err)
		}
	}
	for column, delta := range deltas {
		req.Increments = append(req.Increments, &entityv1.UpdateRequest_Increment{Column: column, Delta: delta})
	}
	res, err := t.u.Data.Broker.Update(ctx, req)
	if err != nil {
		return classify(err)
	}
	t.u.rememberWrite(res)
	return nil
}

func (t *Table[T]) decode(raw []byte) (T, error) {
	var zero T
	var rec Record
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&rec); err != nil {
		return zero, fmt.Errorf("udb: decode %s row: %w", t.messageType, err)
	}
	row := t.newT()
	if err := DecodeRecord(rec, row); err != nil {
		return zero, err
	}
	return row, nil
}

// classify maps a broker status to the sentinel errors callers test with
// errors.Is, keeping the status (and its typed detail) wrapped.
func classify(err error) error {
	code := status.Code(err)
	reason := ""
	if e, ok := AsError(err); ok {
		code = e.Code
		reason = e.Reason()
	}
	switch {
	case reason == "UDB_CAS_CONFLICT" || reason == "UDB_REVISION_CONFLICT" || reason == "UDB_CAS_ROW_MISSING" || IsCASConflict(err):
		return fmt.Errorf("%w: %w", ErrConflict, err)
	case reason == "UDB_NO_ROWS_AFFECTED" || code == codes.NotFound:
		return fmt.Errorf("%w: %w", ErrNotFound, err)
	}
	return err
}

// jsonSafe turns json.Number values (from records decoded with UseNumber) into
// numbers structpb accepts.
func jsonSafe(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = jsonSafeValue(v)
	}
	return out
}

func jsonSafeValue(v any) any {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		f, _ := x.Float64()
		return f
	case json.RawMessage:
		var decoded any
		if err := json.Unmarshal(x, &decoded); err == nil {
			return decoded
		}
		return string(x)
	case map[string]any:
		return jsonSafe(x)
	case Record:
		return jsonSafe(x)
	case Filter:
		return jsonSafe(x)
	case RowKey:
		return jsonSafe(x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = jsonSafeValue(e)
		}
		return out
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	}
	return v
}
