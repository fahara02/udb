package udbtest

import (
	"strings"

	"google.golang.org/grpc/codes"
)

// Resolve every column at plan time, including empty tables and nested boolean
// branches. Unknown columns must never look like a successful zero-row query.
func (e *entity) queryFilter(filter map[string]any) (map[string]any, error) {
	if err := validateFilter(filter); err != nil {
		return nil, err
	}
	out := make(map[string]any, len(filter))
	for field, condition := range filter {
		name := strings.ToLower(strings.TrimSpace(field))
		if name == "$and" || name == "$or" {
			if _, duplicate := out[name]; duplicate {
				return nil, refuse(codes.InvalidArgument, "", "boolean filter supplied through two aliases: "+name)
			}
			var children []any
			for _, child := range condition.([]any) {
				resolved, err := e.queryFilter(child.(map[string]any))
				if err != nil {
					return nil, err
				}
				children = append(children, resolved)
			}
			out[name] = children
			continue
		}
		if name == "tenant_id" && e.tenantColumn == "" {
			return nil, refuse(codes.InvalidArgument, "UDB_TABLE_NOT_TENANT_SCOPED", "the entity has no tenant column")
		}
		column, ok := e.aliases[name]
		if !ok {
			return nil, refuse(codes.InvalidArgument, "", "unknown filter column "+field)
		}
		if _, duplicate := out[column]; duplicate {
			return nil, refuse(codes.InvalidArgument, "", "filter column supplied through two aliases: "+column)
		}
		resolved, err := canonicalOperators(condition)
		if err != nil {
			return nil, err
		}
		out[column] = resolved
	}
	return out, nil
}

func canonicalOperators(value any) (any, error) {
	ops, ok := value.(map[string]any)
	if !ok {
		return copyValue(value), nil
	}
	out := make(map[string]any, len(ops))
	for op, arg := range ops {
		name := strings.ToLower(op)
		if _, duplicate := out[name]; duplicate {
			return nil, refuse(codes.InvalidArgument, "", "filter operator supplied through two aliases: "+name)
		}
		if strings.EqualFold(op, "$not") {
			var err error
			arg, err = canonicalOperators(arg)
			if err != nil {
				return nil, err
			}
		}
		out[name] = copyValue(arg)
	}
	return out, nil
}

func (e *entity) expectedColumns(values map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(values))
	for field, value := range values {
		column, ok := e.aliases[strings.ToLower(strings.TrimSpace(field))]
		if !ok {
			return nil, refuse(codes.InvalidArgument, "", "unknown expected column "+field)
		}
		if _, duplicate := out[column]; duplicate {
			return nil, refuse(codes.InvalidArgument, "", "expected column supplied through two aliases: "+column)
		}
		out[column] = copyValue(value)
	}
	return out, nil
}

func (e *entity) conflictFields(fields []string) error {
	if len(fields) == 0 {
		return nil
	}
	seen := map[string]bool{}
	for _, field := range fields {
		column, ok := e.aliases[strings.ToLower(strings.TrimSpace(field))]
		if !ok || seen[column] {
			return refuse(codes.InvalidArgument, "", "unknown or duplicate conflict column "+field)
		}
		seen[column] = true
	}
	primary := len(seen) == len(e.primaryKey)
	for _, column := range e.primaryKey {
		primary = primary && seen[column]
	}
	if !primary {
		return refuse(codes.FailedPrecondition, "", "udbtest: alternate Upsert conflict_fields require Start; they are never ignored")
	}
	return nil
}
