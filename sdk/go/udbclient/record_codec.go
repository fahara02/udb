// Ported from a consumer service's wrapper so no consumer has to write it:
// the one translation between an entity message and a udb record, driven by
// the message's udb column annotations.

package udbclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	udbcommonv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// The record codec: THE one translation between an entity message and a udb record . Every
// column rule comes from the message's udb annotations; nothing here names a table or a column.

// Record is one entity row as udb records carry it: column (proto field) name
// to JSON value.
type Record map[string]any

// WriteMode says which columns a record carries: an insert sends every insertable column, an update
// (the Set of a compare-and-swap) leaves out the key and the columns udb owns after creation.
type WriteMode int

const (
	// WriteInsert drops generated and exclude_from_insert columns.
	WriteInsert WriteMode = iota
	// WriteUpdate drops generated, exclude_from_update, primary-key, tenant and project columns.
	WriteUpdate
)

// EncodeRecord turns an entity message into a udb record keyed by proto field names, following its
// column annotations (udb "Defining an entity"):
//   - fields without a column annotation, generated columns and the columns mode excludes are dropped;
//   - enums are their numbers (SMALLINT columns), 64-bit integers are exact JSON numbers, booleans are
//     booleans and repeated fields are JSON arrays;
//   - an unset optional field (proto3 presence) is NULL, or is dropped when its column is NOT NULL so
//     the stored value (update) or the column default (insert) stays;
//   - an empty string without proto presence in a nullable column is NULL; an explicitly present
//     optional or oneof text string keeps its empty value. In a NOT NULL UUID, CHAR(n), DATE, TIMESTAMP(TZ)
//     or JSON column it is dropped (it is no value of those types: the default applies, or the write
//     fails closed); a NOT NULL VARCHAR/TEXT column keeps it, an empty string being a value there;
//   - a JSON column (is_json, JSON/JSONB) carries its text as a JSON value; text that is not JSON is
//     an error, never stored as a string.
func EncodeRecord(m proto.Message, mode WriteMode) (Record, error) {
	raw, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true, UseEnumNumbers: true}.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("udb: encode %s: %w", MessageType(m), err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var rec Record
	if err := dec.Decode(&rec); err != nil {
		return nil, fmt.Errorf("udb: encode %s: %w", MessageType(m), err)
	}
	pr := m.ProtoReflect()
	fields := pr.Descriptor().Fields()
	tenant, project := "", ""
	if mode == WriteUpdate {
		tenant = TenantColumn(m)
		project = ProjectColumn(m)
	}
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		c := columnOptions(fd)
		if !carries(c, mode) || (mode == WriteUpdate && (string(fd.Name()) == tenant || string(fd.Name()) == project)) {
			delete(rec, string(fd.Name()))
			continue
		}
		if err := encodeColumn(pr, fd, c, rec); err != nil {
			return nil, err
		}
	}
	return rec, nil
}

// encodeColumn applies the column rules to rec[fd.Name()], which holds the
// field's protojson form.
func encodeColumn(pr protoreflect.Message, fd protoreflect.FieldDescriptor, c *udbcommonv1.ColumnOptions, rec Record) error {
	name := string(fd.Name())
	if fd.HasPresence() && !fd.IsList() && !pr.Has(fd) {
		if c.GetNotNull() {
			delete(rec, name)
		} else {
			rec[name] = nil
		}
		return nil
	}
	if is64Bit(fd.Kind()) {
		rec[name] = exactInts(rec[name])
		return nil
	}
	if fd.Kind() == protoreflect.EnumKind && textColumn(c) {
		rec[name] = enumTokens(fd.Enum(), rec[name])
		return nil
	}
	s, ok := rec[name].(string)
	if !ok || fd.IsList() || fd.Kind() != protoreflect.StringKind {
		return nil
	}
	switch {
	case s == "" && !c.GetNotNull() && !fd.HasPresence():
		rec[name] = nil
	case s == "" && noEmptyValue(c) && !fd.HasPresence():
		delete(rec, name)
	case isJSON(c) && (s != "" || fd.HasPresence()):
		if !json.Valid([]byte(s)) {
			return fmt.Errorf("udb: encode %s.%s: JSON column holds text that is not JSON", MessageType(pr.Interface()), name)
		}
		rec[name] = json.RawMessage(s)
	}
	return nil
}

// EncodeField encodes one field of m exactly as EncodeRecord encodes its
// column (arrays, enum tokens in text columns, exact 64-bit integers, JSON
// columns, NULL for an unset optional). ok is false when the column is left
// out of the record. Generated entity code uses it for the field shapes it
// does not spell out inline (repeated fields, messages, foreign enums).
func EncodeField(m proto.Message, field string) (value any, ok bool, err error) {
	pr := m.ProtoReflect()
	fd := pr.Descriptor().Fields().ByName(protoreflect.Name(field))
	if fd == nil {
		return nil, false, fmt.Errorf("udb: %s has no field %q", MessageType(m), field)
	}
	one := pr.New()
	if pr.Has(fd) {
		one.Set(fd, pr.Get(fd))
	}
	raw, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true, UseEnumNumbers: true}.Marshal(one.Interface())
	if err != nil {
		return nil, false, fmt.Errorf("udb: encode %s.%s: %w", MessageType(m), field, err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var rec Record
	if err := dec.Decode(&rec); err != nil {
		return nil, false, fmt.Errorf("udb: encode %s.%s: %w", MessageType(m), field, err)
	}
	if err := encodeColumn(one, fd, columnOptions(fd), rec); err != nil {
		return nil, false, err
	}
	value, ok = rec[field]
	return value, ok, nil
}

// DecodeField sets one field of m from the value a row holds for its column,
// accepting every shape DecodeRecord accepts. A NULL or absent value leaves
// the field unchanged.
func DecodeField(m proto.Message, field string, raw any) error {
	pr := m.ProtoReflect()
	fd := pr.Descriptor().Fields().ByName(protoreflect.Name(field))
	if fd == nil {
		return fmt.Errorf("udb: %s has no field %q", MessageType(m), field)
	}
	tmp := pr.New()
	if err := DecodeRecord(Record{field: raw}, tmp.Interface()); err != nil {
		return err
	}
	if tmp.Has(fd) {
		if group := fd.ContainingOneof(); group != nil {
			if previous := pr.WhichOneof(group); previous != nil && previous != fd {
				return fmt.Errorf("udb: decode %s: oneof %s has both %s and %s", MessageType(m), group.Name(), previous.Name(), fd.Name())
			}
		}
		pr.Set(fd, tmp.Get(fd))
	}
	return nil
}

// carries reports whether a record written in mode includes the column.
func carries(c *udbcommonv1.ColumnOptions, mode WriteMode) bool {
	switch {
	case c == nil, c.GetGenerated():
		return false
	case mode == WriteInsert:
		return !c.GetExcludeFromInsert()
	default:
		return !c.GetExcludeFromUpdate() && !c.GetPrimaryKey() && !c.GetTenantColumn()
	}
}

func is64Bit(k protoreflect.Kind) bool {
	switch k {
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return true
	}
	return false
}

// exactInts turns protojson's quoted 64-bit integers (singular or repeated) into JSON numbers.
func exactInts(v any) any {
	switch x := v.(type) {
	case string:
		return json.Number(x)
	case []any:
		for i, e := range x {
			x[i] = exactInts(e)
		}
	}
	return v
}

// textColumn reports whether an enum column stores text (udb's convention for
// enums in VARCHAR/TEXT/CHAR columns: the value's short token, e.g. ACTIVE for
// USER_STATUS_ACTIVE) rather than the enum number.
func textColumn(c *udbcommonv1.ColumnOptions) bool {
	t := strings.ToUpper(strings.TrimSpace(c.GetSqlType()))
	for _, prefix := range []string{"VARCHAR", "CHARACTER VARYING", "TEXT", "CHAR", "CITEXT"} {
		if strings.HasPrefix(t, prefix) {
			return true
		}
	}
	return false
}

// enumCommonPrefix is the longest common UPPER_SNAKE_ prefix of an enum's value
// names, cut at a `_` (USER_STATUS_ for USER_STATUS_ACTIVE/USER_STATUS_SUSPENDED),
// the part the short token leaves out. Same rule as udb's generator.
func enumCommonPrefix(ed protoreflect.EnumDescriptor) string {
	values := ed.Values()
	if values.Len() == 0 {
		return ""
	}
	first := string(values.Get(0).Name())
	end := len(first)
	for i := 1; i < values.Len(); i++ {
		name := string(values.Get(i).Name())
		n := 0
		for n < len(first) && n < len(name) && first[n] == name[n] {
			n++
		}
		if n < end {
			end = n
		}
	}
	if idx := strings.LastIndex(first[:end], "_"); idx >= 0 {
		return first[:idx+1]
	}
	return ""
}

// enumTokens turns protojson enum numbers (singular or repeated) into short tokens.
func enumTokens(ed protoreflect.EnumDescriptor, v any) any {
	prefix := enumCommonPrefix(ed)
	token := func(n any) any {
		num, ok := n.(json.Number)
		if !ok {
			return n
		}
		i, err := num.Int64()
		if err != nil {
			return n
		}
		value := ed.Values().ByNumber(protoreflect.EnumNumber(i))
		if value == nil {
			return n
		}
		return strings.TrimPrefix(string(value.Name()), prefix)
	}
	if list, ok := v.([]any); ok {
		for i, e := range list {
			list[i] = token(e)
		}
		return list
	}
	return token(v)
}

// enumListFullNames resolves every text element of a JSON enum array.
func enumListFullNames(ed protoreflect.EnumDescriptor, list json.RawMessage) (json.RawMessage, error) {
	var items []any
	if err := json.Unmarshal(list, &items); err != nil {
		return list, nil
	}
	for i, item := range items {
		if token, ok := item.(string); ok {
			items[i] = enumFullName(ed, token)
		}
	}
	return json.Marshal(items)
}

// enumFullName resolves a stored enum token to the value name proto JSON
// expects: a full name passes through, a short token gets its prefix back.
func enumFullName(ed protoreflect.EnumDescriptor, token string) string {
	if ed.Values().ByName(protoreflect.Name(token)) != nil {
		return token
	}
	if full := enumCommonPrefix(ed) + token; ed.Values().ByName(protoreflect.Name(full)) != nil {
		return full
	}
	return token
}

func isJSON(c *udbcommonv1.ColumnOptions) bool {
	return c.GetIsJson() || c.GetIsJsonb() || strings.HasPrefix(strings.ToUpper(c.GetSqlType()), "JSON")
}

// noEmptyValue reports whether "" is no value of the column's type.
func noEmptyValue(c *udbcommonv1.ColumnOptions) bool {
	t := strings.ToUpper(c.GetSqlType())
	return isJSON(c) || strings.HasPrefix(t, "UUID") || strings.HasPrefix(t, "CHAR(") || strings.HasPrefix(t, "CHARACTER(") ||
		strings.HasPrefix(t, "TIMESTAMP") || t == "DATE"
}

// DecodeRecord fills an entity message from a udb record. It accepts every shape udb (or Postgres
// behind it) hands back for a column:
//   - NULL and unknown columns are skipped (the field keeps its zero value / stays unset);
//   - a string field whose column came back as a JSON value (JSONB object, number, boolean) gets
//     that value's JSON text; CHAR(n) padding is trimmed; a DATE with a time part keeps its date;
//   - booleans and enum or integer numbers that came back as text are parsed;
//   - a repeated field that came back as text is read as a Postgres array literal ({a,"b c"}) or as
//     a JSON array.
func DecodeRecord(r Record, m proto.Message) error {
	raw, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("udb: decode %s: %w", MessageType(m), err)
	}
	var cols map[string]json.RawMessage
	if err := json.Unmarshal(raw, &cols); err != nil {
		return fmt.Errorf("udb: decode %s: %w", MessageType(m), err)
	}
	fields := m.ProtoReflect().Descriptor().Fields()
	clean := make(map[string]json.RawMessage, len(cols))
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		name := string(fd.Name())
		v, ok := cols[name]
		if !ok || string(v) == "null" {
			continue
		}
		if v, err = normalise(fd, v); err != nil {
			return fmt.Errorf("udb: decode %s.%s: %w", MessageType(m), name, err)
		}
		clean[name] = v
	}
	body, err := json.Marshal(clean)
	if err != nil {
		return fmt.Errorf("udb: decode %s: %w", MessageType(m), err)
	}
	// Unknown columns are already gone; an unknown enum name must fail, not decode as zero.
	if err := (protojson.UnmarshalOptions{}).Unmarshal(body, m); err != nil {
		return fmt.Errorf("udb: decode %s: %w", MessageType(m), err)
	}
	return nil
}

// normalise rewrites one column value into the proto JSON form of its field.
func normalise(fd protoreflect.FieldDescriptor, v json.RawMessage) (json.RawMessage, error) {
	var s string
	isString := v[0] == '"'
	if isString {
		if err := json.Unmarshal(v, &s); err != nil {
			return nil, err
		}
	}
	switch {
	case fd.IsMap() || fd.Message() != nil:
		return v, nil
	case fd.IsList():
		var list json.RawMessage
		switch {
		case !isString:
			list = v
		case strings.HasPrefix(strings.TrimSpace(s), "["):
			list = json.RawMessage(strings.TrimSpace(s))
		default:
			lit, err := arrayLiteral(s, fd.Kind())
			if err != nil {
				return nil, err
			}
			list = lit
		}
		if fd.Kind() == protoreflect.EnumKind {
			return enumListFullNames(fd.Enum(), list)
		}
		return list, nil
	case fd.Kind() == protoreflect.StringKind && !isString:
		return json.Marshal(string(v))
	case fd.Kind() == protoreflect.StringKind:
		t := strings.ToUpper(columnOptions(fd).GetSqlType())
		switch {
		case strings.HasPrefix(t, "CHAR(") || strings.HasPrefix(t, "CHARACTER("):
			return json.Marshal(strings.TrimRight(s, " "))
		case t == "DATE" && len(s) > len(time.DateOnly):
			return json.Marshal(s[:len(time.DateOnly)])
		}
		return v, nil
	case !isString:
		return v, nil
	case fd.Kind() == protoreflect.BoolKind:
		b, err := strconv.ParseBool(strings.TrimSpace(s))
		if err != nil {
			return nil, err
		}
		return json.Marshal(b)
	case fd.Kind() == protoreflect.EnumKind:
		if n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 32); err == nil {
			return json.Marshal(n)
		}
		return json.Marshal(enumFullName(fd.Enum(), strings.TrimSpace(s))) // a value name or short token
	}
	return v, nil // numbers as text: proto JSON accepts quoted numbers
}

// arrayLiteral converts a Postgres array literal such as {1,2} or {a,"b, c",NULL} into the JSON array
// of a repeated field of kind k. A NULL element is an error: a repeated field cannot hold one.
func arrayLiteral(lit string, k protoreflect.Kind) (json.RawMessage, error) {
	lit = strings.TrimSpace(lit)
	if !strings.HasPrefix(lit, "{") || !strings.HasSuffix(lit, "}") {
		return nil, fmt.Errorf("not an array literal: %q", lit)
	}
	elems, err := splitArrayLiteral(lit[1 : len(lit)-1])
	if err != nil {
		return nil, fmt.Errorf("array literal %q: %w", lit, err)
	}
	out := make([]any, 0, len(elems))
	for _, e := range elems {
		if !e.quoted && strings.EqualFold(e.text, "NULL") {
			return nil, fmt.Errorf("array literal %q holds NULL", lit)
		}
		switch k {
		case protoreflect.StringKind, protoreflect.BytesKind:
			out = append(out, e.text)
		case protoreflect.BoolKind:
			b, err := strconv.ParseBool(e.text)
			if err != nil {
				return nil, err
			}
			out = append(out, b)
		case protoreflect.FloatKind, protoreflect.DoubleKind:
			f, err := strconv.ParseFloat(e.text, 64)
			if err != nil {
				return nil, err
			}
			out = append(out, f)
		case protoreflect.EnumKind:
			if n, err := strconv.ParseInt(e.text, 10, 32); err == nil {
				out = append(out, n)
			} else {
				out = append(out, e.text) // an enum value name
			}
		default:
			if _, err := strconv.ParseInt(e.text, 10, 64); err != nil {
				if _, uerr := strconv.ParseUint(e.text, 10, 64); uerr != nil {
					return nil, err
				}
			}
			out = append(out, json.Number(e.text))
		}
	}
	return json.Marshal(out)
}

type arrayElem struct {
	text   string
	quoted bool
}

// splitArrayLiteral splits the body of a one-dimensional Postgres array literal into its elements,
// honouring double quotes and backslash escapes.
func splitArrayLiteral(body string) ([]arrayElem, error) {
	var out []arrayElem
	if strings.TrimSpace(body) == "" {
		return out, nil
	}
	var cur strings.Builder
	quoted, inQuotes, escaped := false, false, false
	flush := func() {
		text := cur.String()
		if !quoted {
			text = strings.TrimSpace(text)
		}
		out = append(out, arrayElem{text: text, quoted: quoted})
		cur.Reset()
		quoted = false
	}
	for _, r := range body {
		switch {
		case escaped:
			cur.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
		case r == '"':
			inQuotes = !inQuotes
			quoted = true
		case r == ',' && !inQuotes:
			flush()
		case r == '{' && !inQuotes:
			return nil, fmt.Errorf("nested arrays are not supported")
		default:
			cur.WriteRune(r)
		}
	}
	if inQuotes || escaped {
		return nil, fmt.Errorf("unterminated element")
	}
	flush()
	return out, nil
}

// EncodeEvent turns an event message into an outbox payload (proto JSON keyed by proto field names).
// The caller adds the udb envelope keys (document_id, correlation_id). An event whose proto declares
// no outbox topic is an error: it has nowhere to go.
func EncodeEvent(event proto.Message) (Record, error) {
	if Topic(event) == "" {
		return nil, fmt.Errorf("udb: %s declares no outbox topic", MessageType(event))
	}
	raw, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("udb: encode event %s: %w", MessageType(event), err)
	}
	var payload Record
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("udb: encode event %s: %w", MessageType(event), err)
	}
	return payload, nil
}

// ParseTimestamp parses a TIMESTAMPTZ column as DecodeRecord leaves it in a string field (RFC 3339);
// "" (NULL) is the zero time.
func ParseTimestamp(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}
