package udbtest

import (
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
)

// DeletedReadScope is the scope a Select needs to set include_deleted.
const DeletedReadScope = "udb:deleted:read"

// DeletedRead is the audit record of one Select that included soft-deleted
// rows, as the broker writes it to its audit log.
type DeletedRead struct {
	TenantID    string
	ProjectID   string
	Actor       string
	MessageType string
	Table       string
	Rows        int
}

// DeletedReads returns the audit record of every include_deleted read served
// so far, in order.
func (f *Fake) DeletedReads() []DeletedRead {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]DeletedRead(nil), f.deletedReads...)
}

// admitIncludeDeleted refuses an include_deleted read the broker refuses: the
// caller lacks udb:deleted:read, or the entity has no soft delete.
func (e *entity) admitIncludeDeleted(identity Identity) error {
	if !hasScope(identity, DeletedReadScope) {
		r := refuse(codes.PermissionDenied, "UDB_DELETED_READ_SCOPE_MISSING", "include_deleted needs scope "+DeletedReadScope).(*refusal)
		r.detail = &entityv1.ErrorDetail{Reason: r.reason, Kind: entityv1.ErrorKind_ERROR_KIND_PERMISSION,
			Missing: map[string]string{"scope": DeletedReadScope}}
		return r
	}
	if e.softDelete == "" {
		return &refusal{code: codes.InvalidArgument,
			message: "include_deleted: " + e.messageType + " has no soft delete, so it has no deleted rows to include",
			detail: &entityv1.ErrorDetail{Kind: entityv1.ErrorKind_ERROR_KIND_VALIDATION,
				FieldViolations: []*entityv1.ErrorFieldViolation{{Field: "include_deleted",
					Description: "the entity has no usable soft-delete column, so there are no deleted rows to include"}}}}
	}
	return nil
}

// recordDeletedRead keeps the audit record of one include_deleted read. Called
// with f.mu held.
func (f *Fake) recordDeletedRead(identity Identity, e *entity, rows int) {
	actor := identity.UserID
	if actor == "" {
		actor = identity.ServiceIdentity
	}
	f.deletedReads = append(f.deletedReads, DeletedRead{TenantID: identity.TenantID, ProjectID: identity.ProjectID,
		Actor: actor, MessageType: e.messageType, Table: e.tableName, Rows: rows})
}

// includeDeletedJoinRefusal is the broker's refusal of include_deleted on a
// join read: deleted rows are read one table at a time.
func includeDeletedJoinRefusal() error {
	return &refusal{code: codes.InvalidArgument,
		message: "a join read cannot include deleted rows; read one table to include them",
		detail: &entityv1.ErrorDetail{Kind: entityv1.ErrorKind_ERROR_KIND_VALIDATION,
			FieldViolations: []*entityv1.ErrorFieldViolation{{Field: "include_deleted",
				Description: "a join read cannot include deleted rows"}}}}
}
