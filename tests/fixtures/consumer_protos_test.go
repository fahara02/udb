package udbentities

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	fixturepb "example.com/consumer/gen"
	foreignpb "example.com/consumer/gen/foreign"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Exercise the real generated adapters after a JSON wire round trip, including
// values that encoding/json alone cannot represent as protobuf message maps.
func TestGeneratedConsumerShapesRoundTrip(t *testing.T) {
	empty := ""
	zeroState := foreignpb.State_STATE_UNSPECIFIED
	details := &fixturepb.Details{Label: "nested", Exact: 9007199254740993, State: foreignpb.State_STATE_READY}
	cases := []*fixturepb.Shape{
		{},
		{Choice: &fixturepb.Shape_ChoiceText{ChoiceText: ""}, Nickname: &empty},
		{Choice: &fixturepb.Shape_ChoiceNumber{ChoiceNumber: 0}},
		{Choice: &fixturepb.Shape_ChoiceDetails{ChoiceDetails: details}},
		{OptionalState: &zeroState, Tags: []string{}, Counters: []int64{}, States: []foreignpb.State{}},
		{
			Exact: 9007199254740993, Port: 4294967295,
			Tags: []string{"a", "", "quote\""}, Counters: []int64{-9007199254740993, 0, 9007199254740993},
			States:        []foreignpb.State{foreignpb.State_STATE_READY, foreignpb.State_STATE_CLOSED},
			Labels:        map[string]string{"a": "unicode-ü"},
			DetailsByName: map[string]*fixturepb.Details{"a": details},
			StatesByName:  map[string]foreignpb.State{"a": foreignpb.State_STATE_READY},
			State:         foreignpb.State_STATE_CLOSED, Details: details, JsonDetails: details, Payload: []byte{0, 255, 1},
			ObservedAt: timestamppb.New(time.Date(2026, 10, 7, 12, 0, 0, 123456000, time.UTC)),
		},
	}
	for i, input := range cases {
		input.Id, input.TenantId = "shape", "tenant"
		record, err := ShapeToUDBRecord(input)
		if err != nil {
			t.Fatalf("case %d encode: %v", i, err)
		}
		// The broker's row decoder retains exact numbers via json.Number.
		wire, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		var row map[string]any
		decoder := json.NewDecoder(bytes.NewReader(wire))
		decoder.UseNumber()
		if err := decoder.Decode(&row); err != nil {
			t.Fatal(err)
		}
		output, err := ShapeFromUDBRow(row)
		if err != nil || !proto.Equal(input, output) {
			t.Fatalf("case %d round trip: input=%v output=%v error=%v", i, input, output, err)
		}
	}
}

func TestGeneratedConsumerOneofRejectsMultipleValues(t *testing.T) {
	if _, err := ShapeFromUDBRow(map[string]any{"choice_text": "", "choice_number": 0}); err == nil || !strings.Contains(err.Error(), "oneof choice") {
		t.Fatalf("two present zero-valued alternatives must be refused with oneof context: %v", err)
	}
}

func TestGeneratedConsumerEnumRefusesUnknownTokens(t *testing.T) {
	for _, field := range []string{"state", "optional_state"} {
		for _, token := range []string{"FUTURE_STATE", ""} {
			if _, err := ShapeFromUDBRow(map[string]any{field: token}); err == nil || !strings.Contains(err.Error(), field) || !strings.Contains(err.Error(), "unknown enum token") {
				t.Fatalf("unknown %s token %q must fail with column context: %v", field, token, err)
			}
		}
	}
}

func TestGeneratedConsumerNullsAndEmptyArrays(t *testing.T) {
	// Protobuf repeated fields cannot distinguish whole-column SQL NULL from an
	// empty array. Both decode to an empty list; optional and oneof NULL stay unset.
	for _, array := range []any{nil, []any{}, "{}", "[]"} {
		output, err := ShapeFromUDBRow(map[string]any{
			"tags": array, "counters": array, "states": array,
			"nickname": nil, "optional_state": nil, "choice_text": nil,
			"choice_number": nil, "choice_details": nil, "details_by_name": nil,
			"states_by_name": nil, "details": nil, "json_details": nil,
		})
		if err != nil || !proto.Equal(output, &fixturepb.Shape{}) {
			t.Fatalf("NULL/empty-array row %v: output=%v error=%v", array, output, err)
		}
	}
}

func TestGeneratedConsumerPostgresArrayRows(t *testing.T) {
	output, err := ShapeFromUDBRow(map[string]any{
		"tags":     `{a,"NULL","quote\""}`,
		"counters": `{-9007199254740993,0,9007199254740993}`,
		"states":   `{READY,STATE_CLOSED}`,
	})
	want := &fixturepb.Shape{
		Tags: []string{"a", "NULL", "quote\""}, Counters: []int64{-9007199254740993, 0, 9007199254740993},
		States: []foreignpb.State{foreignpb.State_STATE_READY, foreignpb.State_STATE_CLOSED},
	}
	if err != nil || !proto.Equal(output, want) {
		t.Fatalf("Postgres array values: output=%v error=%v", output, err)
	}
}

func TestGeneratedConsumerRefusesUnrepresentableArrayValues(t *testing.T) {
	for _, row := range []map[string]any{
		{"tags": "{NULL}"}, {"tags": []any{nil}},
		{"states": []any{"FUTURE_STATE"}}, {"states": "{FUTURE_STATE}"},
		{"details_by_name": map[string]any{"a": map[string]any{"state": "FUTURE_STATE"}}},
	} {
		if _, err := ShapeFromUDBRow(row); err == nil {
			t.Fatalf("unrepresentable row silently accepted: %v", row)
		}
	}
}
