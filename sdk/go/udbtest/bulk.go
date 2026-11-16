package udbtest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/internal/protoschema"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// maxBulkRows is the fake's BulkUpsert batch cap (the broker's default).
const maxBulkRows = 10000

// maxBulkBatchIDChars is the longest batch_id the broker accepts.
const maxBulkBatchIDChars = 128

// bulkKeySeparator joins one row's key tokens (U+001F, the unit separator).
const bulkKeySeparator = "\x1f"

type bulkReceipt struct {
	hash     [32]byte
	response *entityv1.BulkUpsertResponse
}

// BulkUpsert writes a batch of rows of one entity in one transaction: every
// row is validated and scoped as Upsert does, and a refused row leaves nothing
// written. A committed batch with affected rows emits ONE change event on the
// entity's cdc_topic (none when the table declares no topic).
func (f *Fake) BulkUpsert(ctx context.Context, req *entityv1.BulkUpsertRequest) (*entityv1.BulkUpsertResponse, error) {
	batchID := req.GetBatchId()
	if utf8.RuneCountInString(batchID) > maxBulkBatchIDChars {
		return nil, refuse(codes.InvalidArgument, "", fmt.Sprintf("batch_id must be at most %d characters", maxBulkBatchIDChars))
	}
	if strings.IndexFunc(batchID, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return nil, refuse(codes.InvalidArgument, "", "batch_id must contain no whitespace or control characters")
	}
	key := req.GetIdempotencyKey()
	if strings.IndexFunc(key, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return nil, &refusal{code: codes.InvalidArgument, message: "idempotency_key must contain no whitespace or control characters",
			detail: &entityv1.ErrorDetail{Kind: entityv1.ErrorKind_ERROR_KIND_VALIDATION,
				FieldViolations: []*entityv1.ErrorFieldViolation{{Field: "idempotency_key", Description: "must contain no whitespace or control characters"}}}}
	}
	raws := req.GetRecordsJson()
	if len(raws) == 0 {
		return nil, refuse(codes.InvalidArgument, "", "BulkUpsert needs at least one row in records_json")
	}
	if len(raws) > maxBulkRows {
		return nil, refuse(codes.InvalidArgument, "UDB_BULK_BATCH_TOO_LARGE",
			fmt.Sprintf("the batch has %d rows, more than the limit of %d; split it into smaller batches", len(raws), maxBulkRows))
	}

	identity := callerIdentity(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	e, err := f.entity(req.GetMessageType())
	if err != nil {
		return nil, err
	}
	if err := e.conflictFields(req.GetConflictFields()); err != nil {
		return nil, err
	}
	conflictColumns := e.primaryKey
	if fields := req.GetConflictFields(); len(fields) > 0 {
		conflictColumns = make([]string, 0, len(fields))
		for _, field := range fields {
			conflictColumns = append(conflictColumns, e.aliases[strings.ToLower(strings.TrimSpace(field))])
		}
	}

	// Decode, shape-check and scope every row before anything is written.
	objects := make([]map[string]any, len(raws))
	records := make([]map[string]any, len(raws))
	var shape []string
	for index, raw := range raws {
		object, err := decodeObject(raw)
		if err != nil {
			return nil, refuse(codes.InvalidArgument, "", fmt.Sprintf("records_json[%d]: %v", index, err))
		}
		fields := make([]string, 0, len(object))
		for name := range object {
			fields = append(fields, name)
		}
		sort.Strings(fields)
		if index == 0 {
			shape = fields
		} else if strings.Join(fields, ",") != strings.Join(shape, ",") {
			return nil, refuse(codes.InvalidArgument, "UDB_BULK_ROW_SHAPE_MISMATCH",
				fmt.Sprintf("records_json[%d] carries a different set of fields than records_json[0]; every row of a batch must carry the same fields", index))
		}
		record, err := e.normalize(copyRow(object))
		if err != nil {
			return nil, rowRefusal(index, err)
		}
		if err := e.bindRecordScope(record, identity); err != nil {
			return nil, rowRefusal(index, err)
		}
		objects[index], records[index] = object, record
	}

	// Idempotency: the inputs are the scoped rows, the conflict target and the
	// requested batch id; replay precedes key checks and writes.
	var receiptScope mutationKey
	var hash [32]byte
	if key != "" {
		inputs := map[string]any{"records": records, "conflict_fields": req.GetConflictFields(), "batch_id": batchID}
		raw, err := json.Marshal(inputs)
		if err != nil {
			return nil, refuse(codes.InvalidArgument, "", "idempotent bulk inputs must be JSON values")
		}
		hash = sha256.Sum256(raw)
		receiptScope = mutationKey{identity.TenantID, identity.ProjectID, e.messageType, "bulk_upsert", key}
		if receipt, found := f.bulkReceipts[receiptScope]; found {
			if receipt.hash != hash {
				return nil, &refusal{code: codes.FailedPrecondition, reason: "UDB_IDEMPOTENCY_REUSE",
					message: "idempotency_key was already used for a different request",
					detail: &entityv1.ErrorDetail{Reason: "UDB_IDEMPOTENCY_REUSE", Kind: entityv1.ErrorKind_ERROR_KIND_CONFLICT,
						FixHint:         "Reuse a key only to retry identical inputs; use a new key for a different write.",
						FieldViolations: []*entityv1.ErrorFieldViolation{{Field: "idempotency_key", Description: "already used with different inputs"}}}}
			}
			replay := proto.Clone(receipt.response).(*entityv1.BulkUpsertResponse)
			replay.WasDuplicate = true
			return replay, nil
		}
	}

	// Fill defaults once (a generated key must be the one written), then key
	// every row and refuse a key the batch repeats.
	tuples := make([]string, len(records))
	seen := make(map[string]int, len(records))
	for index, record := range records {
		supplied := make(map[string]bool, len(record))
		for column := range record {
			supplied[column] = true
		}
		if err := e.defaults(record); err != nil {
			return nil, rowRefusal(index, err)
		}
		for column, value := range record {
			if !supplied[column] {
				objects[index][column] = value
			}
		}
		tokens := make([]string, 0, len(conflictColumns))
		for _, column := range conflictColumns {
			value, present := record[column]
			if !present || value == nil || value == "" {
				return nil, rowRefusal(index, columnRefusal(column))
			}
			tokens = append(tokens, bulkKeyToken(value))
		}
		tuple := strings.Join(tokens, bulkKeySeparator)
		if first, duplicate := seen[tuple]; duplicate {
			return nil, refuse(codes.InvalidArgument, "UDB_BULK_DUPLICATE_KEY",
				fmt.Sprintf("records_json[%d] repeats the conflict key of records_json[%d]; a batch must address each row once", index, first))
		}
		seen[tuple] = index
		tuples[index] = tuple
	}

	// One transaction: a refused row restores everything written before it.
	snapshot := f.snapshot()
	revisions := make(map[revisionKey]int64, len(f.revisions))
	for k, v := range f.revisions {
		revisions[k] = v
	}
	events := len(f.events)
	rollback := func() {
		f.rows = snapshot
		f.revisions = revisions
		f.events = f.events[:events]
	}
	var affected int64
	for index, object := range objects {
		raw, err := json.Marshal(object)
		if err != nil {
			rollback()
			return nil, rowRefusal(index, refuse(codes.InvalidArgument, "", "row is not a JSON object"))
		}
		res, err := f.writeLocked(identity, &entityv1.UpsertRequest{
			MessageType: e.messageType, RecordJson: raw, ConflictFields: req.GetConflictFields(),
		}, false)
		if err != nil {
			rollback()
			return nil, rowRefusal(index, err)
		}
		affected += res.GetAffectedRows()
	}

	if batchID == "" {
		id, err := defaultValue("gen_random_uuid()")
		if err != nil {
			rollback()
			return nil, refuse(codes.Internal, "", "batch id generation failed")
		}
		batchID = fmt.Sprint(id)
	}
	digest := sha256.Sum256([]byte(strings.Join(tuples, "\n")))
	response := &entityv1.BulkUpsertResponse{
		BatchId:        batchID,
		RowCount:       uint32(len(objects)),
		AffectedRows:   affected,
		KeysSha256:     hex.EncodeToString(digest[:]),
		FirstKey:       tuples[0],
		LastKey:        tuples[len(tuples)-1],
		StatementCount: 1,
	}
	if topic := protoschema.Table(e.descriptor).GetCdcTopic(); topic != "" && affected > 0 {
		f.emitted++
		id := fmt.Sprintf("0190f1b2-0000-4000-8000-%012x", f.emitted)
		correlation := id
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if values := md.Get("x-correlation-id"); len(values) > 0 && strings.TrimSpace(values[0]) != "" {
				correlation = values[0]
			}
		}
		columns := make([]any, len(conflictColumns))
		for i, column := range conflictColumns {
			columns[i] = column
		}
		f.events = append(f.events, Event{Topic: topic, PartitionKey: batchID, Envelope: map[string]any{
			"event_id": id, "event_type": topic, "topic": topic, "tenant_id": identity.TenantID, "project_id": identity.ProjectID,
			"operation": "bulk_upsert", "message_type": e.messageType, "document_id": batchID, "correlation_id": correlation,
			"envelope_version": float64(udbclient.EventEnvelopeVersion),
			"payload": map[string]any{
				"batch_id": batchID, "message_type": e.messageType, "row_count": float64(len(objects)),
				"affected_rows": float64(affected), "keys_sha256": response.KeysSha256,
				"first_key": response.FirstKey, "last_key": response.LastKey, "conflict_columns": columns,
			},
		}})
		response.EventId = id
	}
	if key != "" {
		if f.bulkReceipts == nil {
			f.bulkReceipts = make(map[mutationKey]bulkReceipt)
		}
		f.bulkReceipts[receiptScope] = bulkReceipt{hash: hash, response: proto.Clone(response).(*entityv1.BulkUpsertResponse)}
	}
	return response, nil
}

// rowRefusal names the refused row while keeping the refusal's code, reason
// and typed detail.
func rowRefusal(index int, err error) error {
	var r *refusal
	if errors.As(err, &r) {
		return &refusal{code: r.code, reason: r.reason, detail: r.detail, message: fmt.Sprintf("records_json[%d]: %s", index, r.message)}
	}
	return refuse(codes.InvalidArgument, "", fmt.Sprintf("records_json[%d]: %v", index, err))
}

// bulkKeyToken renders one key column's value as the broker's key-tuple token.
func bulkKeyToken(value any) string {
	switch v := value.(type) {
	case nil:
		return "z:"
	case string:
		return "s:" + v
	case bool:
		if v {
			return "b:true"
		}
		return "b:false"
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return "i:" + strconv.FormatInt(i, 10)
		}
		if number, err := v.Float64(); err == nil {
			return numberKeyToken(number, v.String())
		}
		return "f:" + v.String()
	case float64:
		return numberKeyToken(v, strconv.FormatFloat(v, 'g', -1, 64))
	case float32:
		return numberKeyToken(float64(v), strconv.FormatFloat(float64(v), 'g', -1, 32))
	case int:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int32:
		return "i:" + strconv.FormatInt(int64(v), 10)
	case int64:
		return "i:" + strconv.FormatInt(v, 10)
	case uint32:
		return "i:" + strconv.FormatUint(uint64(v), 10)
	case uint64:
		return "i:" + strconv.FormatUint(v, 10)
	}
	// encoding/json sorts object keys: the canonical form of any other value.
	raw, err := json.Marshal(value)
	if err != nil {
		return "j:" + fmt.Sprint(value)
	}
	return "j:" + string(raw)
}

func numberKeyToken(number float64, text string) string {
	if number == math.Trunc(number) && math.Abs(number) < 1<<63 {
		return "i:" + strconv.FormatInt(int64(number), 10)
	}
	return "f:" + text
}
