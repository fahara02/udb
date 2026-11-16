package udbtest

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	commonv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/internal/protoschema"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/structpb"
)

// A Select whose message_type names two or more entities ("A,B" or "A+B") is
// the broker's join-fusion read: one inner join of the tables along their
// foreign keys, each output column aliased `{Message}__{column}` (the short
// message name), or the requested field with its dots turned into `__`. It
// takes simple equality filters only, has no sort, page token or total, and
// leaves PII and encrypted columns out of the default projection. The fake
// answers it the same way.

type foreignKey struct {
	name       string
	columns    []string
	refSchema  string
	refTable   string
	refColumns []string
}

// joinMessageTypes splits a message_type as the broker does: on ',' or '+',
// trimmed, empty parts dropped. More than one part is a join.
func joinMessageTypes(messageType string) []string {
	var out []string
	for _, part := range strings.FieldsFunc(messageType, func(r rune) bool { return r == ',' || r == '+' }) {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func identOr(value, fallback string) string {
	if value = strings.ToLower(strings.TrimSpace(value)); value == "" {
		return fallback
	}
	return value
}

// joinRules records what a join needs from the table annotation: the schema
// (default public), the required scope and every foreign key, table-level and
// per column (a `foreign_key` block, else a `references: "schema.table(col)"`).
// They are ordered as the manifest orders them, so the first edge found is the
// broker's.
func (e *entity) joinRules(table *commonv1.TableOptions) {
	e.schemaName = identOr(table.GetSchemaName(), "public")
	e.requiredScope = strings.TrimSpace(table.GetRequiredScope())
	lower := func(values []string) []string {
		var out []string
		for _, value := range values {
			if value = strings.ToLower(strings.TrimSpace(value)); value != "" {
				out = append(out, value)
			}
		}
		return out
	}
	add := func(name string, columns []string, schema, table string, refColumns []string) {
		columns = lower(columns)
		refColumns = lower(refColumns)
		if len(refColumns) == 0 {
			refColumns = columns
		}
		e.foreignKeys = append(e.foreignKeys, foreignKey{name: strings.TrimSpace(name), columns: columns,
			refSchema: identOr(schema, "public"), refTable: strings.ToLower(strings.TrimSpace(table)), refColumns: refColumns})
	}
	for _, fk := range table.GetForeignKeys() {
		add(fk.GetConstraintName(), fk.GetColumns(), fk.GetReferencesSchema(), fk.GetReferencesTable(), fk.GetReferencesColumn())
	}
	for name, column := range e.options {
		local := column.GetColumnName()
		if local == "" {
			local = name
		}
		if fk := column.GetForeignKey(); fk != nil {
			var refColumns []string
			if fk.GetReferencesColumn() != "" {
				refColumns = []string{fk.GetReferencesColumn()}
			}
			add(fk.GetConstraintName(), []string{local}, fk.GetReferencesSchema(), fk.GetReferencesTable(), refColumns)
			continue
		}
		reference := strings.TrimSpace(column.GetReferences())
		if reference == "" {
			continue
		}
		target, refColumn := reference, ""
		if start := strings.Index(reference, "("); start >= 0 {
			target = reference[:start]
			refColumn = reference[start+1:]
			if end := strings.Index(refColumn, ")"); end >= 0 {
				refColumn = refColumn[:end]
			}
			refColumn = strings.TrimSpace(refColumn)
		}
		var parts []string
		for _, part := range strings.Split(target, ".") {
			if part = strings.TrimSpace(part); part != "" {
				parts = append(parts, part)
			}
		}
		if refColumn == "" {
			refColumn = local
		}
		switch len(parts) {
		case 1:
			add("", []string{local}, "", parts[0], []string{refColumn})
		case 2:
			add("", []string{local}, parts[0], parts[1], []string{refColumn})
		}
	}
	sort.SliceStable(e.foreignKeys, func(i, j int) bool {
		return e.foreignKeys[i].key() < e.foreignKeys[j].key()
	})
}

func (fk foreignKey) key() string {
	return fmt.Sprintf("%s|%s|%s.%s(%s)", strings.ToLower(fk.name), strings.Join(fk.columns, ","),
		fk.refSchema, fk.refTable, strings.Join(fk.refColumns, ","))
}

func (e *entity) references(other *entity) (foreignKey, bool) {
	for _, fk := range e.foreignKeys {
		if fk.refSchema == other.schemaName && fk.refTable == strings.ToLower(other.tableName) {
			return fk, true
		}
	}
	return foreignKey{}, false
}

// joinEdge is one ON clause: the left table's columns equal the right's.
type joinEdge struct {
	left, right               int
	leftColumns, rightColumns []string
}

func joinRefusal(message string) error { return refuse(codes.InvalidArgument, "", message) }

// joinField resolves a fused field name to its table and column as the
// broker's planner does: `Prefix.column`, where the prefix is one of names (or
// its short form), the longest matching prefix winning; an unqualified field
// belongs to the first table.
func joinField(field string, names []string) (int, string, error) {
	if !strings.Contains(field, ".") {
		return 0, strings.ToLower(field), nil
	}
	best, bestLen, column := -1, 0, ""
	for index, candidate := range names {
		prefixes := []string{candidate}
		if dot := strings.LastIndex(candidate, "."); dot >= 0 {
			prefixes = append(prefixes, candidate[dot+1:])
		}
		for _, prefix := range prefixes {
			if prefix == "" || len(field) <= len(prefix) || field[len(prefix)] != '.' || !strings.EqualFold(field[:len(prefix)], prefix) {
				continue
			}
			if len(prefix) > bestLen {
				best, bestLen, column = index, len(prefix), field[len(prefix)+1:]
			}
		}
	}
	if best < 0 {
		head, _, _ := strings.Cut(field, ".")
		return 0, "", joinRefusal("unknown join field prefix " + head)
	}
	return best, strings.ToLower(column), nil
}

func (e *entity) columnName(name string) string {
	if column := e.options[name].GetColumnName(); column != "" {
		return column
	}
	return name
}

func (e *entity) encryptedColumn(name string) bool {
	return protoschema.Encrypted(e.columns[name]) || e.options[name].GetEncrypted()
}

type joinOutput struct {
	table  int
	column string
	alias  string
}

// selectJoin is Select for a fused message_type. f.mu is held.
func (f *Fake) selectJoin(ctx context.Context, req *entityv1.SelectRequest) (*entityv1.RecordSet, error) {
	if len(req.GetSort()) > 0 || strings.TrimSpace(req.GetPageToken()) != "" || req.GetIncludeTotal() {
		return nil, joinRefusal("a join read has no sort, page token or total; read one table to use them")
	}
	identity := callerIdentity(ctx)
	names := joinMessageTypes(req.GetMessageType())
	entities := make([]*entity, len(names))
	shortNames := make([]string, len(names))
	for i, name := range names {
		e, err := f.entity(name)
		if err != nil {
			return nil, err
		}
		entities[i], shortNames[i] = e, e.shortName
	}
	// The served export-control gate: a named PII or encrypted column needs
	// udb:pii:read (or an export purpose), as on a single-table read.
	if len(req.GetFields()) > 0 {
		for i, e := range entities {
			var nested []string
			for _, field := range req.GetFields() {
				index, column, err := joinField(field, shortNames)
				if err == nil && index == i {
					if name, ok := e.aliases[column]; ok {
						nested = append(nested, name)
					}
				}
			}
			if len(nested) > 0 {
				if _, err := e.readColumns(ctx, nested); err != nil {
					return nil, err
				}
			}
		}
	}
	if !hasScope(identity, "udb:read") {
		return nil, joinRefusal("scope udb:read is required for join fusion")
	}
	var outputs []joinOutput
	if len(req.GetFields()) == 0 {
		for i, e := range entities {
			var columns []string
			for name := range e.columns {
				if !e.piiScope[name] {
					columns = append(columns, name)
				}
			}
			sort.Strings(columns)
			for _, name := range columns {
				outputs = append(outputs, joinOutput{table: i, column: name, alias: e.shortName + "__" + e.columnName(name)})
			}
		}
	} else {
		for _, field := range req.GetFields() {
			index, column, err := joinField(field, shortNames)
			if err != nil {
				return nil, err
			}
			name, ok := entities[index].aliases[column]
			if !ok {
				return nil, joinRefusal("unknown join selected field " + field)
			}
			if entities[index].encryptedColumn(name) {
				return nil, joinRefusal("join selected encrypted field " + field + ": a join cannot read an encrypted column; read it from its own table")
			}
			outputs = append(outputs, joinOutput{table: index, column: name, alias: strings.ReplaceAll(field, ".", "__")})
		}
	}
	var edges []joinEdge
	for next := 1; next < len(entities); next++ {
		found := false
		for prior := 0; prior < next && !found; prior++ {
			if fk, ok := entities[prior].references(entities[next]); ok {
				edges = append(edges, joinEdge{left: prior, right: next, leftColumns: fk.columns, rightColumns: fk.refColumns})
				found = true
			} else if fk, ok := entities[next].references(entities[prior]); ok {
				edges = append(edges, joinEdge{left: next, right: prior, leftColumns: fk.columns, rightColumns: fk.refColumns})
				found = true
			}
		}
		if !found {
			return nil, joinRefusal("no foreign key path found for join fusion target " + names[next])
		}
	}
	for _, e := range entities {
		if e.requiredScope != "" && !hasScope(identity, e.requiredScope) {
			return nil, joinRefusal("scope " + e.requiredScope + " is required for this table")
		}
	}
	// Simple equality only; a NULL never equals anything, as in SQL.
	filters := make([]map[string]any, len(entities))
	never := false
	for field, value := range req.GetFilter().AsMap() {
		switch value.(type) {
		case map[string]any, []any:
			return nil, joinRefusal("join fusion supports only simple equality filters")
		}
		if strings.HasPrefix(field, "$") {
			return nil, joinRefusal("join fusion supports only simple equality filters")
		}
		index, column, err := joinField(field, names)
		if err != nil {
			return nil, err
		}
		name, ok := entities[index].aliases[column]
		if !ok {
			return nil, joinRefusal("unknown join filter field " + field)
		}
		if value == nil {
			never = true
			continue
		}
		if filters[index] == nil {
			filters[index] = map[string]any{}
		}
		filters[index][name] = value
	}
	candidates := make([][]map[string]any, len(entities))
	for i, e := range entities {
		filter, err := e.queryFilter(filters[i])
		if err != nil {
			return nil, err
		}
		for _, row := range f.rows[e.messageType][identity.TenantID] {
			if !e.live(row) || !e.visibleTo(row, identity) {
				continue
			}
			ok, err := matches(row, filter)
			if err != nil {
				return nil, err
			}
			if ok {
				candidates[i] = append(candidates[i], row)
			}
		}
		// The broker's join has no ORDER BY; the fake answers in primary-key
		// order so its own runs repeat.
		sort.SliceStable(candidates[i], func(a, b int) bool {
			for _, pk := range e.primaryKey {
				if c := compare(candidates[i][a][pk], candidates[i][b][pk]); c != 0 {
					return c < 0
				}
			}
			return false
		})
	}
	resolve := func(e *entity, column string) string {
		if name, ok := e.aliases[column]; ok {
			return name
		}
		return column
	}
	joins := func(combo []map[string]any, edge joinEdge) bool {
		for k := range edge.leftColumns {
			if k >= len(edge.rightColumns) {
				break
			}
			left := combo[edge.left][resolve(entities[edge.left], edge.leftColumns[k])]
			right := combo[edge.right][resolve(entities[edge.right], edge.rightColumns[k])]
			if left == nil || right == nil || !equal(left, right) {
				return false
			}
		}
		return true
	}
	var matched [][]map[string]any
	if !never {
		var walk func(combo []map[string]any)
		walk = func(combo []map[string]any) {
			if len(combo) == len(entities) {
				matched = append(matched, append([]map[string]any(nil), combo...))
				return
			}
			for _, row := range candidates[len(combo)] {
				next := append(combo, row)
				if len(next) > 1 && !joins(next, edges[len(next)-2]) {
					continue
				}
				walk(next)
			}
		}
		walk(make([]map[string]any, 0, len(entities)))
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 100
	}
	out := &entityv1.RecordSet{HasMore: len(matched) > limit}
	if out.HasMore {
		matched = matched[:limit]
	}
	canRead := hasScope(identity, "udb:pii:read")
	redacted := map[string]bool{}
	for _, combo := range matched {
		view := make(map[string]any, len(outputs))
		for _, output := range outputs {
			e := entities[output.table]
			value, exists := combo[output.table][output.column]
			if !exists {
				continue
			}
			if e.masked[output.column] && !canRead {
				view[output.alias] = "***MASKED***"
				redacted[output.alias] = true
				continue
			}
			view[output.alias] = e.wireValue(output.column, copyValue(value))
		}
		raw, err := json.Marshal(view)
		if err != nil {
			return nil, refuse(codes.Internal, "", "stored row is not JSON")
		}
		out.RecordsJson = append(out.RecordsJson, raw)
		fields, err := structpb.NewStruct(protobufJSON(view).(map[string]any))
		if err != nil {
			return nil, refuse(codes.Internal, "", "stored row cannot be encoded as protobuf Values")
		}
		out.Rows = append(out.Rows, &entityv1.Row{Fields: fields.GetFields()})
	}
	for name := range redacted {
		out.RedactedFields = append(out.RedactedFields, name)
	}
	sort.Strings(out.RedactedFields)
	return out, nil
}
