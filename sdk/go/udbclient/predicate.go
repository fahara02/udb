package udbclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Column builds typed predicates for a declared protobuf field. Values pass
// through the record codec; an invalid value returns an error and no filter.
// Generated <Entity>Where values provide these columns with the field's Go type.
type Column[T any] struct {
	message protoreflect.MessageType
	field   string
	err     error
}

// ColumnOf binds a typed predicate column to an entity descriptor. A typed nil
// protobuf pointer is valid. The descriptor is retained, not the mutable message.
func ColumnOf[T any](m proto.Message, field string) Column[T] {
	c := Column[T]{field: field}
	if m == nil {
		c.err = fmt.Errorf("udb: predicate %s: nil message", field)
		return c
	}
	c.message = m.ProtoReflect().Type()
	fd := c.message.Descriptor().Fields().ByName(protoreflect.Name(field))
	if fd == nil || columnOptions(fd) == nil {
		c.err = fmt.Errorf("udb: predicate %s: field is not a declared column", field)
	}
	return c
}

func (c Column[T]) value(v T) (any, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	var raw []byte
	var err error
	if message, ok := any(v).(proto.Message); ok {
		if value := reflect.ValueOf(message); !value.IsValid() || value.Kind() == reflect.Pointer && value.IsNil() {
			return nil, fmt.Errorf("udb: predicate %s: nil message; use IsNull", c.field)
		}
		raw, err = protojson.Marshal(message)
	} else {
		raw, err = json.Marshal(v)
	}
	if err != nil {
		return nil, fmt.Errorf("udb: predicate %s: %w", c.field, err)
	}
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("udb: predicate %s: %w", c.field, err)
	}
	m := c.message.New().Interface()
	if err := DecodeField(m, c.field, decoded); err != nil {
		return nil, err
	}
	encoded, ok, err := EncodeField(m, c.field)
	if err != nil {
		return nil, err
	}
	if !ok || encoded == nil {
		return nil, fmt.Errorf("udb: predicate %s: value is NULL or omitted; use IsNull", c.field)
	}
	fd := c.message.Descriptor().Fields().ByName(protoreflect.Name(c.field))
	if is64Bit(fd.Kind()) {
		return exactPredicateInts(encoded), nil
	}
	// Struct protobuf numbers are float64. Reject precision loss in JSON
	// predicates instead of silently querying for a different value.
	if err := predicateNumbersExact(encoded); err != nil {
		return nil, fmt.Errorf("udb: predicate %s: %w", c.field, err)
	}
	return encoded, nil
}

func (c Column[T]) check() error {
	if c.err != nil {
		return c.err
	}
	if c.message == nil || c.field == "" {
		return fmt.Errorf("udb: predicate column has no descriptor or field")
	}
	return nil
}

func exactPredicateInts(value any) any {
	switch v := value.(type) {
	case json.Number:
		return v.String()
	case []any:
		out := make([]any, len(v))
		for i := range v {
			out[i] = exactPredicateInts(v[i])
		}
		return out
	default:
		return value
	}
}

func predicateNumbersExact(value any) error {
	switch v := value.(type) {
	case json.Number:
		n, ok := new(big.Rat).SetString(v.String())
		if ok && n.IsInt() && new(big.Int).Abs(n.Num()).Cmp(big.NewInt(1<<53)) > 0 {
			return fmt.Errorf("JSON integer exceeds the exact filter number range")
		}
	case json.RawMessage:
		var decoded any
		decoder := json.NewDecoder(bytes.NewReader(v))
		decoder.UseNumber()
		if err := decoder.Decode(&decoded); err != nil {
			return err
		}
		return predicateNumbersExact(decoded)
	case map[string]any:
		for _, item := range v {
			if err := predicateNumbersExact(item); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range v {
			if err := predicateNumbersExact(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c Column[T]) compare(op string, v T) (Filter, error) {
	value, err := c.value(v)
	if err != nil {
		return nil, err
	}
	return Filter{c.field: map[string]any{op: value}}, nil
}

func (c Column[T]) Eq(v T) (Filter, error)  { return c.compare("$eq", v) }
func (c Column[T]) Gt(v T) (Filter, error)  { return c.compare("$gt", v) }
func (c Column[T]) Gte(v T) (Filter, error) { return c.compare("$gte", v) }
func (c Column[T]) Lt(v T) (Filter, error)  { return c.compare("$lt", v) }
func (c Column[T]) Lte(v T) (Filter, error) { return c.compare("$lte", v) }

func (c Column[T]) values(op string, values []T) (Filter, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	encoded := make([]any, len(values))
	for i, value := range values {
		var err error
		encoded[i], err = c.value(value)
		if err != nil {
			return nil, err
		}
	}
	return Filter{c.field: map[string]any{op: encoded}}, nil
}

func (c Column[T]) In(values ...T) (Filter, error)    { return c.values("$in", values) }
func (c Column[T]) NotIn(values ...T) (Filter, error) { return c.values("$nin", values) }
func (c Column[T]) Between(low, high T) (Filter, error) {
	return c.values("$between", []T{low, high})
}

// IsNull and NotNull reject an invalid column, just like value predicates.
func (c Column[T]) IsNull() (Filter, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	return Filter{c.field: map[string]any{"$is_null": true}}, nil
}

func (c Column[T]) NotNull() (Filter, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	return Filter{c.field: map[string]any{"$not_null": true}}, nil
}
