package udbtest

import (
	"strings"

	"google.golang.org/grpc/codes"
)

// Match scope_autofill::autofill_filter for write inputs. Scope referenced in a
// boolean branch stays authoritative for hashing; the independent scope guard
// still fences matching rows. A filter containing only scope is not expanded.
func (e *entity) scopedWriteFilter(filter map[string]any, identity Identity) map[string]any {
	filter = copyRow(filter)
	scope := map[string]string{}
	if e.tenantColumn != "" {
		scope[e.tenantColumn] = identity.TenantID
	}
	if e.projectColumn != "" && identity.ProjectID != "" {
		scope[e.projectColumn] = identity.ProjectID
	}
	referenced := map[string]bool{}
	var visit func(map[string]any)
	visit = func(part map[string]any) {
		for field, value := range part {
			if strings.HasPrefix(field, "$") {
				if children, ok := value.([]any); ok {
					for _, child := range children {
						if object, ok := child.(map[string]any); ok {
							visit(object)
						}
					}
				} else if object, ok := value.(map[string]any); ok {
					visit(object)
				}
			} else {
				referenced[field] = true
			}
		}
	}
	visit(filter)
	onlyScope := true
	for column := range referenced {
		if _, scoped := scope[column]; !scoped {
			onlyScope = false
		}
	}
	if !onlyScope {
		for column, value := range scope {
			if !referenced[column] {
				filter[column] = value
			}
		}
	}
	return filter
}

// These follow scope_autofill and the SQL compiler's verified scope guards.
// An empty project context is single-project mode and scopes reads to tenant.
func (e *entity) bindRecordScope(record map[string]any, identity Identity) error {
	if e.tenantColumn != "" {
		if given, ok := record[e.tenantColumn].(string); ok && given != "" && given != identity.TenantID {
			return refuse(codes.PermissionDenied, "UDB_TENANT_MISMATCH", "the record names a different tenant than the caller's")
		}
		record[e.tenantColumn] = identity.TenantID
	}
	if e.projectColumn != "" && identity.ProjectID != "" {
		if given := record[e.projectColumn]; given != nil && given != identity.ProjectID {
			return refuse(codes.InvalidArgument, "", "record project_id must match RequestContext.project_id")
		}
		record[e.projectColumn] = identity.ProjectID
	}
	return nil
}

func (e *entity) visibleTo(row map[string]any, identity Identity) bool {
	return e.projectColumn == "" || identity.ProjectID == "" || row[e.projectColumn] == identity.ProjectID
}

// The native conflict guard compares every supplied isolation column with
// the existing row using null-safe equality. It never reassigns ownership.
func (e *entity) sameRecordScope(existing, incoming map[string]any) bool {
	for _, column := range []string{e.tenantColumn, e.projectColumn} {
		if column == "" {
			continue
		}
		if value, supplied := incoming[column]; supplied && !equal(existing[column], value) {
			return false
		}
	}
	return true
}
