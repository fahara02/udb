package udbtest

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	commonv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	entityv1 "github.com/fahara02/udb/sdk/go/gen/udb/entity/v1"
	"github.com/fahara02/udb/sdk/go/internal/protoschema"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

type uniqueKey struct {
	name    string
	columns []string
	where   string
}

func (e *entity) descriptorRules(desc protoreflect.MessageDescriptor) {
	e.options = map[string]*commonv1.ColumnOptions{}
	e.aliases = map[string]string{}
	e.masked = map[string]bool{}
	e.omitImplicit = map[string]bool{}
	e.piiScope = map[string]bool{}
	table := protoschema.Table(desc)
	e.tableName = table.GetTableName()
	if e.tableName == "" {
		e.tableName = string(desc.Name())
	}
	for name, fd := range e.columns {
		column := protoschema.Column(fd)
		if column == nil {
			delete(e.columns, name)
			continue
		}
		e.options[name] = column
		e.aliases[strings.ToLower(name)] = name
		if alias := column.GetColumnName(); alias != "" {
			e.aliases[strings.ToLower(alias)] = name
		}
		e.masked[name] = protoschema.Masked(fd)
		e.omitImplicit[name] = protoschema.PII(fd) || protoschema.Encrypted(fd)
		e.piiScope[name] = e.omitImplicit[name] || column.GetEncrypted()
	}
	for name, column := range e.options {
		if column.GetUnique() {
			e.unique = append(e.unique, uniqueKey{name: e.tableName + "_" + name + "_key", columns: []string{name}})
		}
		if index := column.GetIndex(); index.GetUnique() {
			e.addUnique(index, name)
		}
	}
	for _, index := range table.GetIndexes() {
		if index.GetUnique() {
			e.addUnique(index, "")
		}
	}
	if table.GetSoftDelete() {
		e.softDelete = table.GetSoftDeleteColumn()
		if e.softDelete == "" {
			e.softDelete = "deleted_at"
		}
		if name, ok := e.aliases[strings.ToLower(e.softDelete)]; ok {
			e.softDelete = name
		} else {
			e.schemaError = "soft_delete_column is not a declared column"
		}
	}
}

func (e *entity) addUnique(index *commonv1.IndexOptions, fallback string) {
	columns := index.GetColumns()
	if len(columns) == 0 {
		columns = index.GetCompositeFields()
	}
	if len(columns) == 0 && fallback != "" {
		columns = []string{fallback}
	}
	key := uniqueKey{name: index.GetIndexName(), where: strings.TrimSpace(index.GetWhereClause())}
	for _, column := range columns {
		name, ok := e.aliases[strings.ToLower(column)]
		if !ok {
			e.schemaError = "unique index refers to an unknown column: " + column
			return
		}
		key.columns = append(key.columns, name)
	}
	if len(key.columns) == 0 {
		e.schemaError = "unique index has no columns"
		return
	}
	if key.name == "" {
		key.name = e.tableName + "_" + strings.Join(key.columns, "_") + "_key"
	}
	// A fake must never silently drop a partial uniqueness predicate. The
	// common soft-delete predicates are supported; other SQL needs Start.
	if key.where != "" {
		if _, err := e.inUnique(key, map[string]any{}); err != nil {
			e.schemaError = err.Error()
			return
		}
	}
	e.unique = append(e.unique, key)
}

func (e *entity) inUnique(key uniqueKey, row map[string]any) (bool, error) {
	if key.where == "" {
		return true, nil
	}
	parts := strings.Fields(key.where)
	if (len(parts) == 3 || len(parts) == 4) && strings.EqualFold(parts[1], "IS") &&
		strings.EqualFold(parts[len(parts)-1], "NULL") && (len(parts) == 3 || strings.EqualFold(parts[2], "NOT")) {
		name, ok := e.aliases[strings.Trim(parts[0], "\"")]
		if ok {
			return (row[name] == nil) == (len(parts) == 3), nil
		}
	}
	return false, fmt.Errorf("udbtest: unsupported unique-index predicate %q; use Start for SQL expressions", key.where)
}

func (e *entity) normalize(record map[string]any) (map[string]any, error) {
	if e.schemaError != "" {
		return nil, refuse(codes.FailedPrecondition, "", e.schemaError)
	}
	out := make(map[string]any, len(record))
	for column, value := range record {
		name, ok := e.aliases[strings.ToLower(column)]
		if !ok {
			return nil, refuse(codes.InvalidArgument, "", "unknown column "+column)
		}
		if _, duplicate := out[name]; duplicate {
			return nil, refuse(codes.InvalidArgument, "", "column supplied through two aliases: "+name)
		}
		if e.masked[name] && (value == "***MASKED***" || value == "***REDACTED***") {
			refused := refuse(codes.InvalidArgument, "UDB_REDACTED_VALUE_WRITE", "cannot write a redacted placeholder").(*refusal)
			refused.detail = &entityv1.ErrorDetail{Reason: refused.reason, Kind: entityv1.ErrorKind_ERROR_KIND_REDACTED, Column: name}
			return nil, refused
		}
		if number, ok := value.(float64); ok && integerColumn(e.options[name]) && (math.Abs(number) > 1<<53 || math.Trunc(number) != number) {
			return nil, refuse(codes.InvalidArgument, "", "integer column requires an exact number or decimal string: "+name)
		}
		if value == nil {
			out[name] = nil
			continue
		}
		// Reuse the SDK codec for coercion, enums, timestamps, nested JSON and
		// exact integer bounds. No second lossy JSON-to-protobuf implementation.
		message := dynamicpb.NewMessage(e.descriptor)
		if err := udbclient.DecodeField(message, name, value); err != nil {
			return nil, refuse(codes.InvalidArgument, "", err.Error())
		}
		encoded, present, err := udbclient.EncodeField(message, name)
		if err != nil {
			return nil, refuse(codes.InvalidArgument, "", err.Error())
		}
		if present {
			if raw, ok := encoded.(json.RawMessage); ok {
				decoder := json.NewDecoder(bytes.NewReader(raw))
				decoder.UseNumber()
				if err := decoder.Decode(&encoded); err != nil {
					return nil, refuse(codes.InvalidArgument, "", err.Error())
				}
			}
			out[name] = copyValue(encoded)
		}
	}
	return out, nil
}

func integerColumn(column *commonv1.ColumnOptions) bool {
	switch strings.ToUpper(strings.TrimSpace(column.GetSqlType())) {
	case "SMALLINT", "INTEGER", "INT", "INT2", "INT4", "INT8", "BIGINT", "SERIAL", "BIGSERIAL":
		return true
	}
	return false
}

func (e *entity) defaults(record map[string]any) error {
	for name, column := range e.options {
		if _, present := record[name]; present {
			continue
		}
		if expression := strings.TrimSpace(column.GetDefaultValue()); expression != "" {
			value, err := defaultValue(expression)
			if err != nil {
				return refuse(codes.FailedPrecondition, "", "udbtest: default for "+name+": "+err.Error())
			}
			record[name] = value
		}
	}
	return nil
}

func defaultValue(expression string) (any, error) {
	base, cast, _ := strings.Cut(expression, "::")
	base = strings.TrimSpace(base)
	switch strings.ToLower(base) {
	case "gen_random_uuid()", "uuid_generate_v4()":
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, err
		}
		b[6], b[8] = b[6]&0x0f|0x40, b[8]&0x3f|0x80
		return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
	case "now()", "current_timestamp":
		return time.Now().UTC().Format(time.RFC3339Nano), nil
	case "null":
		return nil, nil
	case "true", "false":
		return strings.EqualFold(base, "true"), nil
	}
	if len(base) >= 2 && base[0] == '\'' && base[len(base)-1] == '\'' {
		value := strings.ReplaceAll(base[1:len(base)-1], "''", "'")
		if strings.EqualFold(cast, "jsonb") || strings.EqualFold(cast, "json") {
			var decoded any
			decoder := json.NewDecoder(strings.NewReader(value))
			decoder.UseNumber()
			if err := decoder.Decode(&decoded); err != nil {
				return nil, err
			}
			return decoded, nil
		}
		return value, nil
	}
	if _, ok := new(big.Rat).SetString(base); ok {
		return json.Number(base), nil
	}
	return nil, fmt.Errorf("unsupported SQL default %q; use Start for database expressions", expression)
}

func columnRefusal(column string) error {
	r := refuse(codes.InvalidArgument, "UDB_NOT_NULL_VIOLATION", "required column is null: "+column).(*refusal)
	r.detail = &entityv1.ErrorDetail{Reason: r.reason, Kind: entityv1.ErrorKind_ERROR_KIND_NOT_NULL, Column: column}
	return r
}

func uniqueRefusal(name string) error {
	r := refuse(codes.AlreadyExists, "UDB_UNIQUE_VIOLATION", "write conflicts with a declared unique key").(*refusal)
	r.detail = &entityv1.ErrorDetail{Reason: r.reason, Kind: entityv1.ErrorKind_ERROR_KIND_UNIQUE, Constraint: name}
	return r
}

func (f *Fake) validateRow(e *entity, tenant, key string, row map[string]any) error {
	for name, column := range e.options {
		if (column.GetNotNull() || column.GetPrimaryKey()) && row[name] == nil {
			return columnRefusal(name)
		}
	}
	for otherTenant, rows := range f.rows[e.messageType] {
		for otherKey, stored := range rows {
			if otherTenant == tenant && otherKey == key {
				continue
			}
			if otherKey == key {
				return uniqueRefusal(e.tableName + "_pkey")
			}
			for _, unique := range e.unique {
				included, err := e.inUnique(unique, row)
				if err != nil {
					return refuse(codes.FailedPrecondition, "", err.Error())
				}
				otherIncluded, _ := e.inUnique(unique, stored)
				if !included || !otherIncluded {
					continue
				}
				collision := true
				for _, name := range unique.columns {
					// SQL UNIQUE treats NULLs as distinct.
					if row[name] == nil || stored[name] == nil || !equal(row[name], stored[name]) {
						collision = false
						break
					}
				}
				if collision {
					return uniqueRefusal(unique.name)
				}
			}
		}
	}
	return nil
}

func (e *entity) live(row map[string]any) bool { return e.softDelete == "" || row[e.softDelete] == nil }

// Follow build_update_plan admission before row matching. A primary key,
// tenant or project is immutable even when a caller supplies its current value.
func (e *entity) updateColumns(changes map[string]any, increments []*entityv1.UpdateRequest_Increment) error {
	if len(changes) == 0 && len(increments) == 0 {
		return refuse(codes.InvalidArgument, "", "update requires at least one change or increment")
	}
	resolve := func(field string) (string, error) {
		name, ok := e.aliases[strings.ToLower(strings.TrimSpace(field))]
		if !ok {
			return "", refuse(codes.InvalidArgument, "", "unknown update column "+field)
		}
		immutable := name == e.tenantColumn || name == e.projectColumn
		for _, key := range e.primaryKey {
			immutable = immutable || name == key
		}
		if immutable {
			return "", refuse(codes.InvalidArgument, "", "column "+name+" is immutable through Update (primary key / tenant / project); use Upsert for identity changes")
		}
		return name, nil
	}
	changed := map[string]bool{}
	for field := range changes {
		name, err := resolve(field)
		if err != nil {
			return err
		}
		changed[name] = true
	}
	for _, increment := range increments {
		name, err := resolve(increment.GetColumn())
		if err != nil {
			return err
		}
		if changed[name] {
			return refuse(codes.InvalidArgument, "", "column "+name+" appears in both changes and increments")
		}
	}
	return nil
}

// CAS must be pinned to an entire declared key, even when another filter
// happens to match exactly one row today. Nested AND and operator EQ work too.
func (e *entity) validateCASKey(filter map[string]any) error {
	pins := map[string]bool{}
	var collect func(map[string]any)
	collect = func(part map[string]any) {
		for name, value := range part {
			if strings.EqualFold(name, "$and") {
				for _, item := range value.([]any) {
					collect(item.(map[string]any))
				}
				continue
			}
			if options, ok := value.(map[string]any); ok {
				value = nil
				for operator, assertion := range options {
					if strings.EqualFold(operator, "$eq") {
						value = assertion
					}
				}
			}
			if value != nil && !strings.HasPrefix(name, "$") {
				pins[name] = true
			}
		}
	}
	collect(filter)
	keys := [][]string{e.primaryKey}
	for _, unique := range e.unique {
		if unique.where == "" {
			keys = append(keys, unique.columns)
		}
	}
	for _, key := range keys {
		complete := len(key) > 0
		for _, name := range key {
			complete = complete && pins[name]
		}
		if complete {
			return nil
		}
	}
	return refuse(codes.FailedPrecondition, "UDB_CAS_KEY_NOT_PK", "CAS requires equality on every column of a primary or unique key")
}

func numeric(value any) (*big.Rat, bool) {
	var text string
	switch v := value.(type) {
	case json.Number:
		text = string(v)
	case float64:
		text = strconv.FormatFloat(v, 'g', -1, 64)
	case int64:
		text = strconv.FormatInt(v, 10)
	case uint64:
		text = strconv.FormatUint(v, 10)
	case int:
		text = strconv.Itoa(v)
	default:
		return nil, false
	}
	return new(big.Rat).SetString(text)
}

func numberPair(a, b any) (*big.Rat, *big.Rat, bool) {
	left, aNumber := numeric(a)
	right, bNumber := numeric(b)
	if aNumber && !bNumber {
		if text, ok := b.(string); ok {
			right, bNumber = new(big.Rat).SetString(text)
		}
	}
	if bNumber && !aNumber {
		if text, ok := a.(string); ok {
			left, aNumber = new(big.Rat).SetString(text)
		}
	}
	return left, right, aNumber && bNumber
}

func incrementValue(value any, delta float64, integer bool) (any, error) {
	if value == nil {
		return nil, nil // SQL NULL + delta is NULL.
	}
	current, ok := numeric(value)
	if !ok {
		if text, isString := value.(string); isString {
			current, ok = new(big.Rat).SetString(text)
		}
	}
	change, finite := numeric(delta)
	if !ok || !finite {
		return nil, refuse(codes.InvalidArgument, "", "increment requires a finite numeric column and delta")
	}
	result := new(big.Rat).Add(current, change)
	if integer {
		if !result.IsInt() || !result.Num().IsInt64() {
			return nil, refuse(codes.InvalidArgument, "", "integer increment is fractional or out of range")
		}
		return json.Number(result.Num().String()), nil
	}
	approx, _ := result.Float64()
	return approx, nil
}

func decodeObject(raw []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil || value == nil {
		return nil, fmt.Errorf("record_json must be a JSON object")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("record_json contains trailing data")
	}
	return value, nil
}

func copyValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return copyRow(v)
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = copyValue(item)
		}
		return out
	case json.RawMessage:
		return append(json.RawMessage(nil), v...)
	case []byte:
		return append([]byte(nil), v...)
	}
	return value
}
