package udbclient

// Proves the TypeScript record codec against this one: the same inputs must give the same JSON text.

import (
	"encoding/json"
	"os"
	"testing"

	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/events/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/authz/entity/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/events/v1"
	_ "github.com/fahara02/udb/sdk/go/gen/udb/core/workflow/entity/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func TestTypeScriptRecordCodecCases(t *testing.T) {
	raw, err := os.ReadFile("../../typescript/test-support/record-codec-cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases struct {
		EncodeRecord []struct {
			Name        string          `json:"name"`
			MessageType string          `json:"message_type"`
			Message     json.RawMessage `json:"message"`
			Mode        string          `json:"mode"`
			JSON        string          `json:"json"`
		} `json:"encode_record"`
		EncodeEvent []struct {
			Name        string          `json:"name"`
			MessageType string          `json:"message_type"`
			Message     json.RawMessage `json:"message"`
			JSON        string          `json:"json"`
		} `json:"encode_event"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	message := func(t *testing.T, name string, body []byte) proto.Message {
		t.Helper()
		mt, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(name))
		if err != nil {
			t.Fatal(err)
		}
		m := mt.New().Interface()
		if err := protojson.Unmarshal(body, m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	if len(cases.EncodeRecord) == 0 || len(cases.EncodeEvent) == 0 {
		t.Fatal("the case file is empty")
	}
	for _, tc := range cases.EncodeRecord {
		t.Run(tc.Name, func(t *testing.T) {
			mode := WriteInsert
			if tc.Mode == "update" {
				mode = WriteUpdate
			}
			rec, err := EncodeRecord(message(t, tc.MessageType, tc.Message), mode)
			if err != nil {
				t.Fatal(err)
			}
			got, err := json.Marshal(rec)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.JSON {
				t.Fatalf("Go produced\n%s\nthe case says\n%s", got, tc.JSON)
			}
		})
	}
	for _, tc := range cases.EncodeEvent {
		t.Run(tc.Name, func(t *testing.T) {
			rec, err := EncodeEvent(message(t, tc.MessageType, tc.Message))
			if err != nil {
				t.Fatal(err)
			}
			got, err := json.Marshal(rec)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.JSON {
				t.Fatalf("Go produced\n%s\nthe case says\n%s", got, tc.JSON)
			}
		})
	}
}
