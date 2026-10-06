package udbclient

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// Transactions with typed writes and outbox events.
//
// u.Tx collects writes and events, then commits them in one broker transaction:
// every write and every event commits together or none does. Emit builds the
// outbox envelope from the event message's own contract (topic, partition key),
// so a producer never assembles envelope fields by hand — the mistake that once
// delivered every event of a service with an empty payload.

// TxScope collects the writes of one transaction. Use it only inside the
// function passed to Udb.Tx.
type TxScope struct {
	u             *Udb
	txID          string
	correlationID string
	muts          []*entityv1.Mutation
	err           error
}

// Tx runs fn and commits everything it collected atomically. fn returning an
// error commits nothing. A conditional write that lost a race fails the whole
// transaction with ErrConflict.
func (u *Udb) Tx(ctx context.Context, fn func(tx *TxScope) error) error {
	tx := &TxScope{u: u, txID: newUUID(), correlationID: MetadataFromContext(ctx).CorrelationID}
	if tx.correlationID == "" {
		tx.correlationID = u.Meta.CorrelationID
	}
	if tx.correlationID == "" {
		tx.correlationID = tx.txID
	}
	if err := fn(tx); err != nil {
		return err
	}
	if tx.err != nil {
		return tx.err
	}
	if len(tx.muts) == 0 {
		return nil
	}
	stream, err := u.Data.Broker.BeginTx(ctx)
	if err != nil {
		return classify(err)
	}
	for _, mutation := range tx.muts {
		mutation.TxId = tx.txID
		if err := stream.Send(mutation); err != nil {
			return classify(err)
		}
	}
	if err := stream.Send(&entityv1.Mutation{TxId: tx.txID, Commit: true}); err != nil {
		return classify(err)
	}
	if err := stream.CloseSend(); err != nil {
		return classify(err)
	}
	committed := false
	for {
		st, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// A stream's refusal carries its typed detail in the trailer; map it
			// the way unary calls are mapped so the reason survives.
			return classify(mapError("/udb.services.v1.DataBroker/BeginTx", err, stream.Trailer()))
		}
		switch st.GetState() {
		case entityv1.TxStatus_TX_STATE_COMMITTED:
			committed = true
			u.rememberReceipt(st.GetWriteReceipt())
		case entityv1.TxStatus_TX_STATE_ERROR, entityv1.TxStatus_TX_STATE_ROLLED_BACK:
			return fmt.Errorf("udb: transaction %s: %s", st.GetState(), st.GetMessage())
		}
	}
	if !committed {
		return errors.New("udb: transaction ended without a COMMITTED status")
	}
	return nil
}

func (tx *TxScope) add(m *entityv1.Mutation) {
	tx.muts = append(tx.muts, m)
}

func (tx *TxScope) fail(err error) error {
	if tx.err == nil {
		tx.err = err
	}
	return err
}

// Upsert writes the whole row m in the transaction.
func (tx *TxScope) Upsert(m proto.Message) error {
	rec, err := EncodeRecord(m, WriteInsert)
	if err != nil {
		return tx.fail(err)
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return tx.fail(err)
	}
	tx.add(&entityv1.Mutation{Operation: "upsert", MessageType: MessageType(m), RecordJson: raw})
	return nil
}

// Patch sets the given columns on the row of entity (any message of the
// entity's type names it, e.g. &notesv1.Note{}) with key.
func (tx *TxScope) Patch(entity proto.Message, key RowKey, fields Record) error {
	return tx.update(MessageType(entity), key, fields, nil)
}

// UpdateIf writes next's updatable columns if the stored row still holds
// expected; otherwise the whole transaction fails with ErrConflict.
func (tx *TxScope) UpdateIf(next proto.Message, expected Record) error {
	key := RowKey{}
	rec, err := EncodeRecord(next, WriteInsert)
	if err != nil {
		return tx.fail(err)
	}
	for _, column := range PrimaryKeys(next) {
		key[column] = rec[column]
	}
	changes, err := EncodeRecord(next, WriteUpdate)
	if err != nil {
		return tx.fail(err)
	}
	return tx.update(MessageType(next), key, changes, expected)
}

// Delete removes the row of entity with key in the transaction.
func (tx *TxScope) Delete(entity proto.Message, key RowKey) error {
	filter, err := structpb.NewStruct(jsonSafe(Filter(key)))
	if err != nil {
		return tx.fail(err)
	}
	tx.add(&entityv1.Mutation{Operation: "delete", MessageType: MessageType(entity), Filter: filter})
	return nil
}

func (tx *TxScope) update(messageType string, key RowKey, changes, expected Record) error {
	filter, err := structpb.NewStruct(jsonSafe(Filter(key)))
	if err != nil {
		return tx.fail(err)
	}
	m := &entityv1.Mutation{Operation: "update", MessageType: messageType, Filter: filter}
	if len(changes) > 0 {
		if m.Changes, err = structpb.NewStruct(jsonSafe(changes)); err != nil {
			return tx.fail(err)
		}
	}
	if len(expected) > 0 {
		if m.Expected, err = structpb.NewStruct(jsonSafe(expected)); err != nil {
			return tx.fail(err)
		}
	}
	tx.add(m)
	return nil
}

// Emit queues event on the outbox, committed with the transaction. The topic
// is the event's declared outbox topic and the partition key its declared
// partition-key field (message_event_contract); the event's own fields go under
// the envelope's payload.
func (tx *TxScope) Emit(event proto.Message) error {
	topic := Topic(event)
	if topic == "" {
		return tx.fail(fmt.Errorf("udb: %s declares no outbox topic (message_event_contract.outbox_topic)", MessageType(event)))
	}
	keyField := PartitionKeyField(event)
	if keyField == "" {
		return tx.fail(fmt.Errorf("udb: %s declares no partition key field (message_event_contract.partition_key_field)", MessageType(event)))
	}
	raw, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(event)
	if err != nil {
		return tx.fail(fmt.Errorf("udb: encode event %s: %w", MessageType(event), err))
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return tx.fail(fmt.Errorf("udb: encode event %s: %w", MessageType(event), err))
	}
	document, _ := payload[keyField].(string)
	if document == "" {
		return tx.fail(fmt.Errorf("udb: event %s has no value in its partition key field %s", MessageType(event), keyField))
	}
	envelope, err := structpb.NewStruct(map[string]any{
		"event_id":         newUUID(),
		"event_type":       EventType(event),
		"correlation_id":   tx.correlationID,
		"document_id":      document,
		"envelope_version": EventEnvelopeVersion,
		"payload":          payload,
	})
	if err != nil {
		return tx.fail(fmt.Errorf("udb: event %s envelope: %w", MessageType(event), err))
	}
	tx.add(&entityv1.Mutation{
		Operation:   "enqueue_outbox_event",
		MessageType: MessageType(event),
		Collection:  topic,
		ObjectKey:   document,
		Payload:     envelope,
	})
	return nil
}

// EventEnvelopeVersion is the outbox envelope version this SDK writes and
// reads; consumers refuse a newer major (see Consume).
const EventEnvelopeVersion = 2

// rememberReceipt keeps a committed transaction's receipt for read-your-writes.
func (u *Udb) rememberReceipt(receipt *entityv1.WriteReceipt) {
	if receipt == nil {
		return
	}
	u.rememberReceiptValue(WriteReceipt{
		SourceLsn:         receipt.GetSourceLsn(),
		OutboxSeq:         receipt.GetOutboxSeq(),
		ProjectionTaskIds: receipt.GetProjectionTaskIds(),
		ManifestChecksum:  receipt.GetManifestChecksum(),
		WrittenAtUnixMs:   receipt.GetWrittenAtUnixMs(),
	})
}

// newUUID is a random version-4 UUID.
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
