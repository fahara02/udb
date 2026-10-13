package udbtest

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Follow the planner's implicit column list and served export-control gate.
// Even a PII reader must name a PII column explicitly; implicit * omits it.
func (e *entity) readColumns(ctx context.Context, fields []string) (map[string]bool, error) {
	selected := map[string]bool{}
	identity := callerIdentity(ctx)
	canRead := hasScope(identity, "udb:pii:read")
	md, _ := metadata.FromIncomingContext(ctx)
	if purpose := md.Get("x-purpose"); len(purpose) == 1 {
		switch purpose[0] {
		case "export", "verification", "audit":
			canRead = true
		}
	}
	if len(fields) == 0 {
		for name := range e.columns {
			if e.omitImplicit[name] {
				continue
			}
			if e.piiScope[name] && !canRead {
				return nil, missingScope("udb:pii:read")
			}
			selected[name] = true
		}
		return selected, nil
	}
	for _, field := range fields {
		name, ok := e.aliases[strings.ToLower(strings.TrimSpace(field))]
		if !ok {
			return nil, refuse(codes.InvalidArgument, "", "unknown selected column "+field)
		}
		if e.piiScope[name] && !canRead {
			return nil, missingScope("udb:pii:read")
		}
		selected[name] = true
	}
	return selected, nil
}

// Match the broker's integer wire shape: an exact JSON number through 2^53,
// then a decimal string. Storage keeps the exact value on either side.
func (e *entity) wireValue(name string, value any) any {
	if value == nil {
		return nil
	}
	kind := e.columns[name].Kind()
	isInteger := kind == protoreflect.Int64Kind || kind == protoreflect.Sint64Kind || kind == protoreflect.Sfixed64Kind || kind == protoreflect.Uint64Kind || kind == protoreflect.Fixed64Kind
	convert := func(value any) any {
		if !isInteger {
			return value
		}
		text := fmt.Sprint(value)
		integer, ok := new(big.Int).SetString(text, 10)
		if !ok {
			return value
		}
		if new(big.Int).Abs(integer).Cmp(new(big.Int).Lsh(big.NewInt(1), 53)) > 0 {
			return integer.String()
		}
		return json.Number(integer.String())
	}
	if values, ok := value.([]any); ok && e.columns[name].IsList() {
		out := make([]any, len(values))
		for i, value := range values {
			out[i] = convert(value)
		}
		return out
	}
	return convert(value)
}

func protobufJSON(value any) any {
	switch v := value.(type) {
	case json.Number:
		if number, err := strconv.ParseFloat(string(v), 64); err == nil {
			return number
		}
		return string(v)
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, value := range v {
			out[key] = protobufJSON(value)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, value := range v {
			out[i] = protobufJSON(value)
		}
		return out
	}
	return value
}
