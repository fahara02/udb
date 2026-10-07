package udbclient

import (
	"fmt"
	"strconv"
	"strings"

	udbcommonv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Schema helpers. Everything a service needs to know about a table (its message type, keys, owner
// columns, column bounds, security class, event contract) is read here from the udb annotations on
// the generated proto descriptors, so no service restates the schema .

// MessageType is the udb message type of an entity message: its proto full name.
func MessageType(m proto.Message) string { return string(m.ProtoReflect().Descriptor().FullName()) }

func eventContract(m proto.Message) *udbcommonv1.EventContractOptions {
	c, _ := proto.GetExtension(m.ProtoReflect().Descriptor().Options(), udbcommonv1.E_MessageEventContract).(*udbcommonv1.EventContractOptions)
	return c
}

// Topic is the outbox topic an event message declares in its message_event_contract ("" when it
// declares none). Producers and consumers name topics through this, never as string literals, so
// the proto stays the single source of truth .
func Topic(m proto.Message) string { return eventContract(m).GetOutboxTopic() }

// EventType is the event type an event message's contract declares, else its
// outbox topic.
func EventType(m proto.Message) string {
	if t := eventContract(m).GetEventType(); t != "" {
		return t
	}
	return Topic(m)
}

// PartitionKeyField is the payload field an event message's contract names as its partition key
// ("" when it declares none). Its value is the outbox envelope's document_id.
func PartitionKeyField(m proto.Message) string { return eventContract(m).GetPartitionKeyField() }

// columnOptions reads a field's column annotation: the backend-neutral
// `column` option, else the Postgres `pg_column` one (udb's own entities use
// pg_column).
func columnOptions(fd protoreflect.FieldDescriptor) *udbcommonv1.ColumnOptions {
	if proto.HasExtension(fd.Options(), udbcommonv1.E_Column) {
		c, _ := proto.GetExtension(fd.Options(), udbcommonv1.E_Column).(*udbcommonv1.ColumnOptions)
		return c
	}
	c, _ := proto.GetExtension(fd.Options(), udbcommonv1.E_PgColumn).(*udbcommonv1.ColumnOptions)
	return c
}

func columnSecurityOptions(fd protoreflect.FieldDescriptor) *udbcommonv1.DbColumnSecurityOptions {
	s, _ := proto.GetExtension(fd.Options(), udbcommonv1.E_DbColumnSecurity).(*udbcommonv1.DbColumnSecurityOptions)
	return s
}

func tableSecurityOptions(m proto.Message) *udbcommonv1.DbTableSecurityOptions {
	s, _ := proto.GetExtension(m.ProtoReflect().Descriptor().Options(), udbcommonv1.E_DbTableSecurity).(*udbcommonv1.DbTableSecurityOptions)
	return s
}

func tableOptions(m proto.Message) *udbcommonv1.TableOptions {
	opts := m.ProtoReflect().Descriptor().Options()
	if proto.HasExtension(opts, udbcommonv1.E_Table) {
		t, _ := proto.GetExtension(opts, udbcommonv1.E_Table).(*udbcommonv1.TableOptions)
		return t
	}
	t, _ := proto.GetExtension(opts, udbcommonv1.E_PgTable).(*udbcommonv1.TableOptions)
	return t
}

// fieldsWhere lists, in field order, the names of m's fields that keep holds for.
func fieldsWhere(m proto.Message, keep func(protoreflect.FieldDescriptor) bool) []string {
	var out []string
	fields := m.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		if keep(fields.Get(i)) {
			out = append(out, string(fields.Get(i).Name()))
		}
	}
	return out
}

// PrimaryKeys lists the primary-key columns declared on an entity message, in field order.
func PrimaryKeys(m proto.Message) []string {
	return fieldsWhere(m, func(fd protoreflect.FieldDescriptor) bool { return columnOptions(fd).GetPrimaryKey() })
}

// PrimaryKey is the primary-key column of a single-key table. It is "" when the message declares no
// primary key or a composite one, so a caller that assumes one key column never keys on half of it.
func PrimaryKey(m proto.Message) string {
	if pk := PrimaryKeys(m); len(pk) == 1 {
		return pk[0]
	}
	return ""
}

var tenantColumnCandidates = []string{"tenant_id", "_tenant_id", "org_id", "institution_id"}
var projectColumnCandidates = []string{"project_id", "_project_id"}

// TenantColumn is the proto field holding the row's tenant, resolved from table
// security, its tenant_column flag, then the broker's conventional tenant names.
func TenantColumn(m proto.Message) string {
	return scopeColumn(m, tableSecurityOptions(m).GetTenantColumn(), func(column *udbcommonv1.ColumnOptions) bool {
		return column.GetTenantColumn()
	}, tenantColumnCandidates)
}

// ProjectColumn is the proto field holding the row's project. Resolution matches
// the broker: table security, the column's project_column flag, then conventional
// project names. A column_name alias still returns the proto field used by records.
func ProjectColumn(m proto.Message) string {
	return scopeColumn(m, tableSecurityOptions(m).GetProjectColumn(), func(column *udbcommonv1.ColumnOptions) bool {
		return column.GetProjectColumn()
	}, projectColumnCandidates)
}

func scopeColumn(m proto.Message, declared string, flagged func(*udbcommonv1.ColumnOptions) bool, candidates []string) string {
	declared = strings.TrimSpace(declared)
	matches := func(fd protoreflect.FieldDescriptor, name string, fold bool) bool {
		column := columnOptions(fd)
		if column == nil {
			return false
		}
		if fold {
			return strings.EqualFold(string(fd.Name()), name) || strings.EqualFold(column.GetColumnName(), name)
		}
		return string(fd.Name()) == name || strings.ToLower(column.GetColumnName()) == name
	}
	if declared != "" {
		if fields := fieldsWhere(m, func(fd protoreflect.FieldDescriptor) bool { return matches(fd, declared, false) }); len(fields) > 0 {
			return fields[0]
		}
	}
	if fields := fieldsWhere(m, func(fd protoreflect.FieldDescriptor) bool { return flagged(columnOptions(fd)) }); len(fields) > 0 {
		return fields[0]
	}
	if fields := fieldsWhere(m, func(fd protoreflect.FieldDescriptor) bool {
		for _, name := range candidates {
			if matches(fd, name, true) {
				return true
			}
		}
		return false
	}); len(fields) > 0 {
		return fields[0]
	}
	return ""
}

// OwnerColumns are the columns annotated owner_field (they drive privacy export and erasure).
func OwnerColumns(m proto.Message) []string {
	return fieldsWhere(m, func(fd protoreflect.FieldDescriptor) bool { return columnSecurityOptions(fd).GetOwnerField() })
}

// OwnerColumn is the first owner_field column ("" for a tenant-wide table).
func OwnerColumn(m proto.Message) string {
	if cols := OwnerColumns(m); len(cols) > 0 {
		return cols[0]
	}
	return ""
}

// ExportEligible reports whether the table's rows belong in a privacy export (db_table_security).
func ExportEligible(m proto.Message) bool { return tableSecurityOptions(m).GetExportEligible() }

// RetentionClass is the table's db_table_security retention class ("" when it declares none).
func RetentionClass(m proto.Message) string { return tableSecurityOptions(m).GetRetentionClass() }

// SoftDelete reports whether the table is declared soft_delete (Delete tombstones its rows).
func SoftDelete(m proto.Message) bool { return tableOptions(m).GetSoftDelete() }

// MaxLen returns n for a column declared VARCHAR(n) (0 when the field is unknown or has no such bound).
func MaxLen(m proto.Message, field string) int {
	fd := m.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(field))
	if fd == nil {
		return 0
	}
	t := columnOptions(fd).GetSqlType()
	if !strings.HasPrefix(t, "VARCHAR(") || !strings.HasSuffix(t, ")") {
		return 0
	}
	n, err := strconv.Atoi(t[len("VARCHAR(") : len(t)-1])
	if err != nil {
		return 0
	}
	return n
}

// ApplyColumnDefaults sets every scalar bool and integer field of m whose column declares a
// default_value to that default, so a row a service creates starts from the table's own defaults
// instead of a restated value. A default that does not parse for the field's kind is an error.
func ApplyColumnDefaults(m proto.Message) error {
	pr := m.ProtoReflect()
	fields := pr.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		def := strings.TrimSpace(columnOptions(fd).GetDefaultValue())
		if def == "" || fd.IsList() || fd.IsMap() {
			continue
		}
		switch fd.Kind() {
		case protoreflect.BoolKind:
			b, err := strconv.ParseBool(def)
			if err != nil {
				return fmt.Errorf("udb: %s.%s default %q: %w", MessageType(m), fd.Name(), def, err)
			}
			pr.Set(fd, protoreflect.ValueOfBool(b))
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
			n, err := strconv.ParseInt(def, 10, 32)
			if err != nil {
				return fmt.Errorf("udb: %s.%s default %q: %w", MessageType(m), fd.Name(), def, err)
			}
			pr.Set(fd, protoreflect.ValueOfInt32(int32(n)))
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			n, err := strconv.ParseInt(def, 10, 64)
			if err != nil {
				return fmt.Errorf("udb: %s.%s default %q: %w", MessageType(m), fd.Name(), def, err)
			}
			pr.Set(fd, protoreflect.ValueOfInt64(n))
		}
	}
	return nil
}
