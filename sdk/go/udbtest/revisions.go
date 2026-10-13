package udbtest

import (
	"strconv"
	"strings"

	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"google.golang.org/grpc/codes"
)

type revisionKey struct {
	tenant, project, messageType, key string
}

func rowRevisionKey(identity Identity, e *entity, key string) revisionKey {
	return revisionKey{identity.TenantID, identity.ProjectID, e.messageType, key}
}

// Called under f.mu, together with the row write. Keep entries after deletion:
// re-inserting the same key must never revive an earlier revision (ABA).
func (f *Fake) bumpRevision(identity Identity, e *entity, key string) string {
	if f.revisions == nil {
		f.revisions = make(map[revisionKey]int64)
	}
	scope := rowRevisionKey(identity, e, key)
	f.revisions[scope]++
	return strconv.FormatInt(f.revisions[scope], 10)
}

func (f *Fake) revision(identity Identity, e *entity, key string) string {
	if revision := f.revisions[rowRevisionKey(identity, e, key)]; revision != 0 {
		return strconv.FormatInt(revision, 10)
	}
	return ""
}

// Opaque revisions address only primary keys, unlike field-map CAS which also
// accepts complete unique keys. Match the native conservative equality rule.
func (e *entity) revisionFilterKey(identity Identity, filter map[string]any) (string, error) {
	filter = e.scopedWriteFilter(filter, identity)
	key := make(map[string]any, len(e.primaryKey))
	for _, column := range e.primaryKey {
		value, present := filter[column]
		if operators, ok := value.(map[string]any); ok {
			value, present = operators["$eq"]
			present = present && len(operators) == 1
		}
		if !present || value == nil {
			return "", &refusal{code: codes.FailedPrecondition, reason: "UDB_CAS_KEY_NOT_PK",
				message: "conditional mutation requires an equality filter on every primary-key column",
				detail: &entityv1.ErrorDetail{Reason: "UDB_CAS_KEY_NOT_PK", Kind: entityv1.ErrorKind_ERROR_KIND_VALIDATION,
					FieldViolations: []*entityv1.ErrorFieldViolation{{Field: column, Description: "primary-key column must be pinned by equality"}}}}
		}
		key[column] = value
	}
	resolved, err := e.normalize(key)
	if err != nil {
		return "", err
	}
	return e.keyOf(resolved)
}

func (f *Fake) checkRevision(identity Identity, e *entity, filter map[string]any, expected string) error {
	if strings.TrimSpace(expected) == "" {
		return nil
	}
	key, err := e.revisionFilterKey(identity, filter)
	if err != nil {
		return err
	}
	revision, err := strconv.ParseInt(strings.TrimSpace(expected), 10, 64)
	if err != nil {
		return &refusal{code: codes.InvalidArgument, message: "expected_revision is not a valid revision token",
			detail: &entityv1.ErrorDetail{Kind: entityv1.ErrorKind_ERROR_KIND_VALIDATION,
				FieldViolations: []*entityv1.ErrorFieldViolation{{Field: "expected_revision", Description: "must be an opaque revision token previously returned by the broker"}}}}
	}
	current, tracked := f.revisions[rowRevisionKey(identity, e, key)]
	if !tracked || current != revision {
		return &refusal{code: codes.FailedPrecondition, reason: "UDB_REVISION_CONFLICT",
			message: "revision precondition failed: the row's current revision differs from expected_revision, or the row is not revision-tracked",
			detail: &entityv1.ErrorDetail{Reason: "UDB_REVISION_CONFLICT", Kind: entityv1.ErrorKind_ERROR_KIND_CONFLICT,
				FieldViolations: []*entityv1.ErrorFieldViolation{{Field: "expected_revision", Description: "the current revision differs from the expected revision"}}}}
	}
	return nil
}
