package udbtest

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

type mutationKey struct {
	tenant, project, messageType, operation, key string
}

type mutationReceipt struct {
	hash     [32]byte
	response *entityv1.MutationResponse
}

// Called with f.mu held. Only successful writes retain a receipt; the receipt
// and row change become visible together. Replay precedes CAS and row matching,
// so a lost-response retry does not fail because the first write changed a row.
func (f *Fake) keyedWriteLocked(ctx context.Context, operation string, request proto.Message,
	write func() (*entityv1.MutationResponse, error),
) (*entityv1.MutationResponse, error) {
	var messageType, key string
	switch req := request.(type) {
	case *entityv1.UpsertRequest:
		messageType, key = req.GetMessageType(), req.GetIdempotencyKey()
	case *entityv1.UpdateRequest:
		messageType, key = req.GetMessageType(), req.GetIdempotencyKey()
	case *entityv1.DeleteRequest:
		messageType, key = req.GetMessageType(), req.GetIdempotencyKey()
	}
	if key == "" {
		return write()
	}
	if strings.IndexFunc(key, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return nil, &refusal{code: codes.InvalidArgument, message: "idempotency_key must contain no whitespace or control characters",
			detail: &entityv1.ErrorDetail{Kind: entityv1.ErrorKind_ERROR_KIND_VALIDATION,
				FieldViolations: []*entityv1.ErrorFieldViolation{{Field: "idempotency_key", Description: "must contain no whitespace or control characters"}}}}
	}
	e, err := f.entity(messageType)
	if err != nil {
		return nil, err
	}
	identity := callerIdentity(ctx)
	inputs, err := e.mutationInputs(identity, request)
	if err != nil {
		return nil, err
	}
	// encoding/json sorts map keys recursively; array order stays authoritative.
	raw, err := json.Marshal(inputs)
	if err != nil {
		return nil, refuse(codes.InvalidArgument, "", "idempotent mutation inputs must be JSON values")
	}
	hash := sha256.Sum256(raw)
	scope := mutationKey{identity.TenantID, identity.ProjectID, messageType, operation, key}
	if receipt, found := f.receipts[scope]; found {
		if receipt.hash != hash {
			return nil, &refusal{code: codes.FailedPrecondition, reason: "UDB_IDEMPOTENCY_REUSE",
				message: "idempotency_key was already used for a different request",
				detail: &entityv1.ErrorDetail{Reason: "UDB_IDEMPOTENCY_REUSE", Kind: entityv1.ErrorKind_ERROR_KIND_CONFLICT,
					FixHint:         "Reuse a key only to retry identical inputs; use a new key for a different write.",
					FieldViolations: []*entityv1.ErrorFieldViolation{{Field: "idempotency_key", Description: "already used with different inputs"}}}}
		}
		replay := proto.Clone(receipt.response).(*entityv1.MutationResponse)
		replay.WasDuplicate = true
		return replay, nil
	}
	response, err := write()
	if err != nil {
		return nil, err
	}
	if f.receipts == nil {
		f.receipts = make(map[mutationKey]mutationReceipt)
	}
	f.receipts[scope] = mutationReceipt{hash: hash, response: proto.Clone(response).(*entityv1.MutationResponse)}
	return response, nil
}

// validateTxGuards refuses, before anything is written, a key on an operation
// with no replay receipt and a malformed key on one that has.
func validateTxGuards(received []*entityv1.Mutation) error {
	for index, m := range received {
		operation := strings.ToLower(strings.TrimSpace(m.GetOperation()))
		key := m.GetIdempotencyKey()
		switch operation {
		case "upsert", "update", "delete", "vector_upsert":
		default:
			if key != "" {
				return &refusal{code: codes.InvalidArgument,
					message: fmt.Sprintf("BeginTx mutation %d (%s) cannot honour an idempotency_key; nothing was written", index, operation),
					detail: &entityv1.ErrorDetail{Kind: entityv1.ErrorKind_ERROR_KIND_VALIDATION,
						FieldViolations: []*entityv1.ErrorFieldViolation{{Field: fmt.Sprintf("mutations[%d].idempotency_key", index),
							Description: "this operation has no replay receipt; leave the key empty"}}}}
			}
		}
		if strings.IndexFunc(key, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
			return &refusal{code: codes.InvalidArgument, message: "idempotency_key must be empty or contain no whitespace or control characters",
				detail: &entityv1.ErrorDetail{Kind: entityv1.ErrorKind_ERROR_KIND_VALIDATION,
					FieldViolations: []*entityv1.ErrorFieldViolation{{Field: "idempotency_key", Description: "must be empty or contain no whitespace or control characters"}}}}
		}
		switch operation {
		case "upsert", "update", "delete":
		default:
			if m.GetRequireAffected() != 0 {
				return &refusal{code: codes.InvalidArgument, message: "require_affected requires an upsert, update or delete mutation",
					detail: &entityv1.ErrorDetail{Kind: entityv1.ErrorKind_ERROR_KIND_VALIDATION,
						FieldViolations: []*entityv1.ErrorFieldViolation{{Field: fmt.Sprintf("mutations[%d].require_affected", index),
							Description: "only supported on relational mutations"}}}}
			}
		}
	}
	return nil
}

// keyedTxMutationLocked keeps a receipt per keyed relational mutation, in a
// begin_tx/<op> namespace a unary write never shares. Called with f.mu held. The
// claim precedes CAS, so an identical retry neither rechecks expected nor writes
// nor bumps a revision; the same key with different inputs refuses the whole
// transaction.
func (f *Fake) keyedTxMutationLocked(identity Identity, m *entityv1.Mutation) error {
	e, err := f.entity(m.GetMessageType())
	if err != nil {
		return err
	}
	var request proto.Message
	switch m.GetOperation() {
	case "upsert":
		request = &entityv1.UpsertRequest{MessageType: m.GetMessageType(), RecordJson: m.GetRecordJson(), Payload: m.GetPayload(),
			ConflictFields: m.GetConflictFields(), Expected: m.GetExpected()}
	case "update":
		request = &entityv1.UpdateRequest{MessageType: m.GetMessageType(), Filter: m.GetFilter(), Changes: m.GetChanges(),
			Increments: m.GetIncrements(), Expected: m.GetExpected()}
	default:
		request = &entityv1.DeleteRequest{MessageType: m.GetMessageType(), Filter: m.GetFilter(), Expected: m.GetExpected()}
	}
	inputs, err := e.mutationInputs(identity, request)
	if err != nil {
		return err
	}
	inputs["require_affected"], inputs["cdc_required"] = m.GetRequireAffected(), m.GetCdcRequired()
	raw, err := json.Marshal(inputs)
	if err != nil {
		return refuse(codes.InvalidArgument, "", "idempotent mutation inputs must be JSON values")
	}
	hash := sha256.Sum256(raw)
	scope := mutationKey{identity.TenantID, identity.ProjectID, m.GetMessageType(), "begin_tx/" + m.GetOperation(), m.GetIdempotencyKey()}
	if receipt, found := f.receipts[scope]; found {
		if receipt.hash != hash {
			return &refusal{code: codes.FailedPrecondition, reason: "UDB_IDEMPOTENCY_REUSE",
				message: "idempotency_key was already used for a different request; reuse a key only to retry an identical request",
				detail: &entityv1.ErrorDetail{Reason: "UDB_IDEMPOTENCY_REUSE", Kind: entityv1.ErrorKind_ERROR_KIND_CONFLICT,
					FixHint: "Reuse a key only to retry identical inputs; use a new key for a different write.",
					FieldViolations: []*entityv1.ErrorFieldViolation{{Field: "idempotency_key",
						Description: "the same idempotency_key was already claimed by a request with different inputs"}}}}
		}
		return nil
	}
	response, err := f.applyTxMutation(identity, m)
	if err != nil {
		return err
	}
	if f.receipts == nil {
		f.receipts = make(map[mutationKey]mutationReceipt)
	}
	f.receipts[scope] = mutationReceipt{hash: hash, response: &entityv1.MutationResponse{
		MutationId: response.GetMutationId(), AffectedRows: response.GetAffectedRows()}}
	return nil
}

func (e *entity) mutationInputs(identity Identity, request proto.Message) (map[string]any, error) {
	inputs := map[string]any{"expected": nil}
	var filter map[string]any
	switch req := request.(type) {
	case *entityv1.UpsertRequest:
		var record map[string]any
		var err error
		if req.GetPayload() != nil {
			record = req.GetPayload().AsMap()
		} else {
			record, err = decodeObject(req.GetRecordJson())
		}
		if err != nil {
			return nil, refuse(codes.InvalidArgument, "", err.Error())
		}
		record, err = e.normalize(record)
		if err != nil {
			return nil, err
		}
		if err := e.bindRecordScope(record, identity); err != nil {
			return nil, err
		}
		inputs["record"], inputs["conflict_fields"] = record, req.GetConflictFields()
		if len(req.GetExpected().GetFields()) > 0 {
			inputs["expected"] = req.GetExpected().AsMap()
		}
	case *entityv1.UpdateRequest:
		if revision := strings.TrimSpace(req.GetExpectedRevision()); revision != "" {
			inputs["expected_revision"] = revision
		}
		if req.GetRequireAffected() != 0 {
			inputs["require_affected"] = req.GetRequireAffected()
		}
		changes, err := e.normalize(req.GetChanges().AsMap())
		if err != nil {
			return nil, err
		}
		var increments []any
		for _, inc := range req.GetIncrements() {
			increments = append(increments, []any{inc.GetColumn(), inc.GetDelta()})
		}
		inputs["changes"], inputs["increments"] = changes, increments
		filter = req.GetFilter().AsMap()
		if len(req.GetExpected().GetFields()) > 0 {
			inputs["expected"] = req.GetExpected().AsMap()
		}
	case *entityv1.DeleteRequest:
		if revision := strings.TrimSpace(req.GetExpectedRevision()); revision != "" {
			inputs["expected_revision"] = revision
		}
		if req.GetRequireAffected() != 0 {
			inputs["require_affected"] = req.GetRequireAffected()
		}
		filter = req.GetFilter().AsMap()
		if len(req.GetExpected().GetFields()) > 0 {
			inputs["expected"] = req.GetExpected().AsMap()
		}
	}
	if filter != nil {
		resolved, err := e.queryFilter(filter)
		if err != nil {
			return nil, err
		}
		inputs["filter"] = e.scopedWriteFilter(resolved, identity)
	}
	return inputs, nil
}
