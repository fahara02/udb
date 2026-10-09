package udbclient

import (
	"reflect"
	"strings"
	"testing"

	commonv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	livev1 "github.com/fahara02/udb/sdk/go/gen/udb/sdk/live/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func uniqueFixture(t *testing.T, configure func(*descriptorpb.DescriptorProto, *commonv1.TableOptions)) proto.Message {
	t.Helper()
	model := protodesc.ToFileDescriptorProto((&livev1.SdkLiveRecord{}).ProtoReflect().Descriptor().ParentFile())
	entity := model.GetMessageType()[0]
	table := proto.GetExtension(entity.GetOptions(), commonv1.E_PgTable).(*commonv1.TableOptions)
	table.Indexes = nil
	configure(entity, table)
	descriptor, err := protodesc.NewFile(model, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	return dynamicpb.NewMessage(descriptor.Messages().Get(0))
}

func fixtureColumn(t *testing.T, entity *descriptorpb.DescriptorProto, field string) *commonv1.ColumnOptions {
	t.Helper()
	for _, fd := range entity.GetField() {
		if fd.GetName() == field {
			return proto.GetExtension(fd.GetOptions(), commonv1.E_PgColumn).(*commonv1.ColumnOptions)
		}
	}
	t.Fatalf("fixture field %s missing", field)
	return nil
}

func TestUniqueKeysFollowEffectiveDeclaredConstraints(t *testing.T) {
	m := uniqueFixture(t, func(entity *descriptorpb.DescriptorProto, table *commonv1.TableOptions) {
		fixtureColumn(t, entity, "lookup_key").ColumnName = "lookup_sql"
		fixtureColumn(t, entity, "record_id").ColumnName = "stored_id"
		fixtureColumn(t, entity, "payload").ColumnName = "payload_sql"
		fixtureColumn(t, entity, "payload").Index = &commonv1.IndexOptions{IndexName: "field_composite", Unique: true, Columns: []string{"payload_sql", "revision"}}
		table.Indexes = []*commonv1.IndexOptions{
			{IndexName: "duplicate", Unique: true, Columns: []string{"lookup_sql"}},
			{IndexName: "reversed_duplicate", Unique: true, Columns: []string{"revision", "payload"}},
			{IndexName: "composite", Unique: true, CompositeFields: []string{"revision, lookup_sql"}},
			{IndexName: "pk", Unique: true, Columns: []string{"stored_id"}},
			{IndexName: "partial", Unique: true, Columns: []string{"payload"}, WhereClause: "revision > 0"},
			{IndexName: "expression", Unique: true, Columns: []string{"date_trunc('day', revision)", "lower(payload)"}},
		}
	})
	got, err := UniqueKeys(m)
	want := [][]string{{"lookup_key"}, {"payload", "revision"}, {"revision", "lookup_key"}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("effective unique keys = %v, error %v; want %v", got, err, want)
	}
	got[0][0] = "mutated"
	again, err := UniqueKeys(m)
	if err != nil || !reflect.DeepEqual(again, want) {
		t.Fatal("caller mutation changed descriptor-derived metadata")
	}
}

func TestUniqueKeysIncludePartitionColumnAndExcludePartialOnly(t *testing.T) {
	m := uniqueFixture(t, func(entity *descriptorpb.DescriptorProto, table *commonv1.TableOptions) {
		table.PartitionStrategy = commonv1.PartitionStrategy_PARTITION_STRATEGY_HASH
		table.PartitionColumn = "project_id"
		table.Indexes = []*commonv1.IndexOptions{{Unique: true, Columns: []string{"payload"}, WhereClause: "revision > 0"}}
	})
	got, err := UniqueKeys(m)
	if err != nil || !reflect.DeepEqual(got, [][]string{{"lookup_key", "project_id"}}) {
		t.Fatalf("partitioned keys = %v, error %v", got, err)
	}
}

func TestUniqueKeysRefuseUnknownAndAmbiguousOrdinaryColumns(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		m := uniqueFixture(t, func(entity *descriptorpb.DescriptorProto, table *commonv1.TableOptions) {
			name := "missing_column"
			if ambiguous {
				name = "lookup_key"
				fixtureColumn(t, entity, "payload").ColumnName = name
			}
			table.Indexes = []*commonv1.IndexOptions{{Unique: true, Columns: []string{name}}}
		})
		keys, err := UniqueKeys(m)
		if err == nil || keys != nil || !strings.Contains(err.Error(), "unique key column") {
			t.Fatalf("invalid metadata must refuse all lookup keys: keys=%v err=%v", keys, err)
		}
	}
}

func TestUniqueKeysRefuseRepeatedTableIndexComponents(t *testing.T) {
	m := uniqueFixture(t, func(_ *descriptorpb.DescriptorProto, table *commonv1.TableOptions) {
		table.Indexes = []*commonv1.IndexOptions{{Unique: true, Columns: []string{"revision", "revision"}}}
	})
	keys, err := UniqueKeys(m)
	if err == nil || keys != nil || !strings.Contains(err.Error(), "repeats field") {
		t.Fatalf("repeated table index components must be refused: keys=%v err=%v", keys, err)
	}
}

func TestUniqueKeysMatchCanonicalParserAliasNormalization(t *testing.T) {
	for _, names := range [][2]string{
		{"ExternalCode", "external_code"},
		{"_External__Code_", "external_code"},
		{"TOTPEnabled", "totp_enabled"},
		{"URLValue", "url_value"},
		{"some-field", "some_field"},
		{" __Mixed UpperCase__ ", "mixed_upper_case"},
	} {
		t.Run(names[0], func(t *testing.T) {
			m := uniqueFixture(t, func(entity *descriptorpb.DescriptorProto, table *commonv1.TableOptions) {
				column := fixtureColumn(t, entity, "lookup_key")
				column.Unique = false
				column.ColumnName = names[0]
				column.Index = &commonv1.IndexOptions{Unique: true, Columns: []string{names[0]}}
				table.Indexes = []*commonv1.IndexOptions{{Unique: true, Columns: []string{names[1]}}}
			})
			keys, err := UniqueKeys(m)
			if err != nil || !reflect.DeepEqual(keys, [][]string{{"lookup_key"}}) {
				t.Fatalf("actual descriptor alias normalization must match owning and table SQL columns: keys=%v err=%v", keys, err)
			}
		})
	}
}
