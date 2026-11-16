package udbclient

import (
	"context"
	"fmt"
	"strings"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/protobuf/proto"
)

// Bulk loading.
//
// DataBroker.BulkUpsert writes one batch of rows of one entity in ONE broker
// transaction: every row is written or none is, and the broker emits one change
// event for the batch (not one per row). The broker never splits a batch;
// BulkLoad is the client-side helper that splits a large load into batches and
// sends them one after another. Neither retries on its own: a re-run with the
// same IdempotencyKeyPrefix replays the batches that already committed instead
// of writing them twice.

// DefaultBulkBatchRows is BulkLoadOptions.BatchRows when it is left at zero.
const DefaultBulkBatchRows = 1000

// DefaultBulkMaxBatchBytes is BulkLoadOptions.MaxBatchBytes when it is left at zero (4 MiB).
const DefaultBulkMaxBatchBytes = 4 << 20

// MaxBulkBatchRows is the largest BulkLoadOptions.BatchRows (the broker's default batch cap).
const MaxBulkBatchRows = 10000

// BulkUpsertBatchOptions shape one BulkUpsert batch.
type BulkUpsertBatchOptions struct {
	// ConflictFields is the conflict target (empty = the primary key).
	ConflictFields []string
	// IdempotencyKey makes the batch safe to resend after a lost response: the
	// broker returns the first result (WasDuplicate) for an identical batch and
	// refuses the same key with a different batch.
	IdempotencyKey string
	// BatchID names the batch in its change event and response (at most 128
	// characters, no whitespace or control characters). Empty = the broker
	// assigns a UUID.
	BatchID string
}

// BulkUpsert sends rows as ONE BulkUpsert batch: one transaction, all rows or
// none. Each row is encoded exactly as Entity.Upsert encodes its record.
func (c *Client) BulkUpsert(ctx context.Context, messageType string, rows []map[string]any, opts BulkUpsertBatchOptions) (*entityv1.BulkUpsertResponse, error) {
	raws := make([][]byte, 0, len(rows))
	for index, row := range rows {
		raw, err := toRecordJSON(row)
		if err != nil {
			return nil, fmt.Errorf("udb: bulk upsert %s row %d: %w", messageType, index, err)
		}
		raws = append(raws, raw)
	}
	return c.bulkUpsertRaw(ctx, messageType, raws, opts)
}

func (c *Client) bulkUpsertRaw(ctx context.Context, messageType string, raws [][]byte, opts BulkUpsertBatchOptions) (*entityv1.BulkUpsertResponse, error) {
	if len(raws) == 0 {
		return nil, fmt.Errorf("udb: bulk upsert %s: the batch has no rows", messageType)
	}
	if opts.IdempotencyKey != "" && strings.TrimSpace(opts.IdempotencyKey) == "" {
		return nil, fmt.Errorf("udb: idempotency key must not be whitespace-only")
	}
	return c.Broker.BulkUpsert(c.Context(ctx), &entityv1.BulkUpsertRequest{
		Context:        &entityv1.RequestContext{TenantId: c.Meta.TenantID, ProjectId: c.Meta.ProjectID},
		MessageType:    messageType,
		RecordsJson:    raws,
		ConflictFields: opts.ConflictFields,
		IdempotencyKey: opts.IdempotencyKey,
		BatchId:        opts.BatchID,
	})
}

// BulkUpsert sends rows as ONE batch through the data plane and keeps the
// batch's write receipt so the following reads see it.
func (u *Udb) BulkUpsert(ctx context.Context, messageType string, rows []map[string]any, opts BulkUpsertBatchOptions) (*entityv1.BulkUpsertResponse, error) {
	res, err := u.Data.BulkUpsert(ctx, messageType, rows, opts)
	if err != nil {
		return nil, err
	}
	u.rememberBulkWrite(res)
	return res, nil
}

func (u *Udb) rememberBulkWrite(res *entityv1.BulkUpsertResponse) {
	receipt, err := ParseWriteReceipt([]byte(res.GetWriteReceiptJson()))
	if err != nil {
		return
	}
	u.rememberReceiptValue(receipt)
}

// BulkLoadOptions shape a chunked load.
type BulkLoadOptions struct {
	// BatchRows is the most rows per batch (0 = DefaultBulkBatchRows).
	BatchRows int
	// MaxBatchBytes caps the encoded rows of one batch (0 = DefaultBulkMaxBatchBytes).
	// A batch is closed before a row that would take it past the cap; a single
	// row larger than the cap is an error before that row's batch is sent.
	MaxBatchBytes int
	// ConflictFields is every batch's conflict target (empty = the primary key).
	ConflictFields []string
	// BatchIDPrefix, when set, names batch i "<prefix>/<i as 6 digits>" (i from
	// 0); otherwise the broker assigns each batch a UUID.
	BatchIDPrefix string
	// IdempotencyKeyPrefix, when set, keys batch i "<prefix>-<i>" (i from 0), so
	// re-running the same load with the same prefix replays the committed
	// batches instead of writing them again and continues with the rest.
	IdempotencyKeyPrefix string
	// OnBatch runs after batch index committed; an error stops the load.
	OnBatch func(index int, resp *entityv1.BulkUpsertResponse) error
}

// BulkLoadResult sums the batches a load committed.
type BulkLoadResult struct {
	// Batches is how many batches committed.
	Batches int
	// Rows is how many rows those batches carried.
	Rows int
	// AffectedRows is the broker's affected-row total over those batches.
	AffectedRows int64
	// Responses holds each committed batch's response, in order.
	Responses []*entityv1.BulkUpsertResponse
}

// BulkLoadError is the error of a load that stopped: Result holds the batches
// that committed before it (they stay committed, each batch being its own
// transaction) and Err the cause (errors.Is / errors.As see through it).
type BulkLoadError struct {
	MessageType string
	Result      BulkLoadResult
	Err         error
}

func (e *BulkLoadError) Error() string {
	return fmt.Sprintf("udb: bulk load %s stopped at batch %d after %d committed batch(es) (%d row(s), %d affected): %v",
		e.MessageType, e.Result.Batches, e.Result.Batches, e.Result.Rows, e.Result.AffectedRows, e.Err)
}

func (e *BulkLoadError) Unwrap() error { return e.Err }

// BulkLoad splits rows into batches and sends them one after another,
// stopping at the first error, which is a *BulkLoadError saying how many
// batches committed. No rows means no call and an empty result.
func (c *Client) BulkLoad(ctx context.Context, messageType string, rows []map[string]any, opts BulkLoadOptions) (BulkLoadResult, error) {
	index := 0
	return c.BulkLoadFunc(ctx, messageType, func() (map[string]any, bool, error) {
		if index >= len(rows) {
			return nil, false, nil
		}
		index++
		return rows[index-1], true, nil
	}, opts)
}

// BulkLoadFunc is BulkLoad over a pull function: next returns the following
// row and true, or false when there are no more rows; an error stops the load.
func (c *Client) BulkLoadFunc(ctx context.Context, messageType string, next func() (map[string]any, bool, error), opts BulkLoadOptions) (BulkLoadResult, error) {
	return bulkLoad(ctx, messageType, func() ([]byte, bool, error) {
		row, ok, err := next()
		if err != nil || !ok {
			return nil, ok, err
		}
		raw, err := toRecordJSON(row)
		return raw, true, err
	}, opts, c.bulkUpsertRaw, nil)
}

// BulkLoad is Client.BulkLoad that also keeps the last batch's write receipt
// for the following reads.
func (u *Udb) BulkLoad(ctx context.Context, messageType string, rows []map[string]any, opts BulkLoadOptions) (BulkLoadResult, error) {
	index := 0
	return u.BulkLoadFunc(ctx, messageType, func() (map[string]any, bool, error) {
		if index >= len(rows) {
			return nil, false, nil
		}
		index++
		return rows[index-1], true, nil
	}, opts)
}

// BulkLoadFunc is Client.BulkLoadFunc that also keeps each batch's write receipt.
func (u *Udb) BulkLoadFunc(ctx context.Context, messageType string, next func() (map[string]any, bool, error), opts BulkLoadOptions) (BulkLoadResult, error) {
	return bulkLoad(ctx, messageType, func() ([]byte, bool, error) {
		row, ok, err := next()
		if err != nil || !ok {
			return nil, ok, err
		}
		raw, err := toRecordJSON(row)
		return raw, true, err
	}, opts, u.Data.bulkUpsertRaw, u.rememberBulkWrite)
}

// BulkLoad writes rows in batches, each encoded as Upsert encodes its row.
// See Client.BulkLoad for the batching and stop-on-error rules.
func (t *Table[T]) BulkLoad(ctx context.Context, rows []T, opts BulkLoadOptions) (BulkLoadResult, error) {
	index := 0
	return bulkLoad(ctx, t.messageType, func() ([]byte, bool, error) {
		if index >= len(rows) {
			return nil, false, nil
		}
		row := rows[index]
		index++
		raw, err := t.encodeRow(row)
		return raw, true, err
	}, opts, func(ctx context.Context, messageType string, raws [][]byte, batch BulkUpsertBatchOptions) (*entityv1.BulkUpsertResponse, error) {
		res, err := t.u.Data.bulkUpsertRaw(ctx, messageType, raws, batch)
		if err != nil {
			return nil, classify(err)
		}
		return res, nil
	}, t.u.rememberBulkWrite)
}

// BulkUpsert writes rows as ONE batch: one transaction, all rows or none.
func (t *Table[T]) BulkUpsert(ctx context.Context, rows []T, opts BulkUpsertBatchOptions) (*entityv1.BulkUpsertResponse, error) {
	raws := make([][]byte, 0, len(rows))
	for index, row := range rows {
		raw, err := t.encodeRow(row)
		if err != nil {
			return nil, fmt.Errorf("udb: bulk upsert %s row %d: %w", t.messageType, index, err)
		}
		raws = append(raws, raw)
	}
	res, err := t.u.Data.bulkUpsertRaw(ctx, t.messageType, raws, opts)
	if err != nil {
		return nil, classify(err)
	}
	t.u.rememberBulkWrite(res)
	return res, nil
}

// encodeRow encodes m exactly as Table.Upsert does.
func (t *Table[T]) encodeRow(m T) ([]byte, error) {
	if !m.ProtoReflect().IsValid() {
		return nil, fmt.Errorf("udb: %s row is nil", t.messageType)
	}
	rec, err := EncodeRecord(proto.Message(m), WriteInsert)
	if err != nil {
		return nil, err
	}
	return toRecordJSON(map[string]any(rec))
}

type bulkSender func(ctx context.Context, messageType string, raws [][]byte, opts BulkUpsertBatchOptions) (*entityv1.BulkUpsertResponse, error)

func bulkLoad(ctx context.Context, messageType string, next func() ([]byte, bool, error), opts BulkLoadOptions,
	send bulkSender, remember func(*entityv1.BulkUpsertResponse),
) (BulkLoadResult, error) {
	var result BulkLoadResult
	batchRows, maxBytes := opts.BatchRows, opts.MaxBatchBytes
	if batchRows == 0 {
		batchRows = DefaultBulkBatchRows
	}
	if maxBytes == 0 {
		maxBytes = DefaultBulkMaxBatchBytes
	}
	if batchRows < 1 || batchRows > MaxBulkBatchRows {
		return result, fmt.Errorf("udb: bulk load %s: BatchRows must be 1..%d (0 = %d), got %d", messageType, MaxBulkBatchRows, DefaultBulkBatchRows, opts.BatchRows)
	}
	if maxBytes < 1 {
		return result, fmt.Errorf("udb: bulk load %s: MaxBatchBytes must be positive (0 = %d), got %d", messageType, DefaultBulkMaxBatchBytes, opts.MaxBatchBytes)
	}
	stopped := func(err error) (BulkLoadResult, error) {
		return result, &BulkLoadError{MessageType: messageType, Result: result, Err: err}
	}
	var pending [][]byte
	pendingBytes := 0
	flush := func() error {
		index := result.Batches
		batch := BulkUpsertBatchOptions{ConflictFields: opts.ConflictFields}
		if opts.BatchIDPrefix != "" {
			batch.BatchID = fmt.Sprintf("%s/%06d", opts.BatchIDPrefix, index)
		}
		if opts.IdempotencyKeyPrefix != "" {
			batch.IdempotencyKey = fmt.Sprintf("%s-%d", opts.IdempotencyKeyPrefix, index)
		}
		res, err := send(ctx, messageType, pending, batch)
		if err != nil {
			return err
		}
		if remember != nil {
			remember(res)
		}
		result.Batches++
		result.Rows += len(pending)
		result.AffectedRows += res.GetAffectedRows()
		result.Responses = append(result.Responses, res)
		pending, pendingBytes = nil, 0
		if opts.OnBatch != nil {
			if err := opts.OnBatch(index, res); err != nil {
				return fmt.Errorf("OnBatch(%d): %w", index, err)
			}
		}
		return nil
	}
	for rowIndex := 0; ; rowIndex++ {
		if err := ctx.Err(); err != nil {
			return stopped(err)
		}
		raw, ok, err := next()
		if err != nil {
			return stopped(fmt.Errorf("row %d: %w", rowIndex, err))
		}
		if !ok {
			break
		}
		if len(raw) > maxBytes {
			return stopped(fmt.Errorf("row %d encodes to %d bytes, more than MaxBatchBytes %d", rowIndex, len(raw), maxBytes))
		}
		if len(pending) > 0 && (len(pending) >= batchRows || pendingBytes+len(raw) > maxBytes) {
			if err := flush(); err != nil {
				return stopped(err)
			}
		}
		pending = append(pending, raw)
		pendingBytes += len(raw)
	}
	if len(pending) > 0 {
		if err := flush(); err != nil {
			return stopped(err)
		}
	}
	return result, nil
}
