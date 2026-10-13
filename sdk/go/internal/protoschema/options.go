// Package protoschema reads UDB's canonical descriptor annotations for SDK
// codecs and the test broker. Backend-neutral options take precedence over PG.
package protoschema

import (
	commonv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/common/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func Column(fd protoreflect.FieldDescriptor) *commonv1.ColumnOptions {
	if proto.HasExtension(fd.Options(), commonv1.E_Column) {
		value, _ := proto.GetExtension(fd.Options(), commonv1.E_Column).(*commonv1.ColumnOptions)
		return value
	}
	value, _ := proto.GetExtension(fd.Options(), commonv1.E_PgColumn).(*commonv1.ColumnOptions)
	return value
}
func Table(md protoreflect.MessageDescriptor) *commonv1.TableOptions {
	if proto.HasExtension(md.Options(), commonv1.E_Table) {
		value, _ := proto.GetExtension(md.Options(), commonv1.E_Table).(*commonv1.TableOptions)
		return value
	}
	value, _ := proto.GetExtension(md.Options(), commonv1.E_PgTable).(*commonv1.TableOptions)
	return value
}
func ColumnSecurity(fd protoreflect.FieldDescriptor) *commonv1.DbColumnSecurityOptions {
	value, _ := proto.GetExtension(fd.Options(), commonv1.E_DbColumnSecurity).(*commonv1.DbColumnSecurityOptions)
	return value
}
func TableSecurity(md protoreflect.MessageDescriptor) *commonv1.DbTableSecurityOptions {
	value, _ := proto.GetExtension(md.Options(), commonv1.E_DbTableSecurity).(*commonv1.DbTableSecurityOptions)
	return value
}
func LegacyColumnSecurity(fd protoreflect.FieldDescriptor) *commonv1.ColumnSecurityOptions {
	value, _ := proto.GetExtension(fd.Options(), commonv1.E_ColumnSecurity).(*commonv1.ColumnSecurityOptions)
	return value
}
func Bool(fd protoreflect.FieldDescriptor, extension protoreflect.ExtensionType) bool {
	value, _ := proto.GetExtension(fd.Options(), extension).(bool)
	return value
}

// Masked follows the runtime's merged column-security annotations. A secret
// classification alone does not mask; output-view and redaction rules do.
func Masked(fd protoreflect.FieldDescriptor) bool {
	legacy := LegacyColumnSecurity(fd)
	security := ColumnSecurity(fd)
	view := security.GetOutputView()
	redaction := security.GetRedactionStrategy()
	return legacy.GetIsPii() || legacy.GetMaskInLogs() ||
		Bool(fd, commonv1.E_Pii) || Bool(fd, commonv1.E_LogMasked) ||
		Bool(fd, commonv1.E_LogRedacted) || Bool(fd, commonv1.E_Sensitive) ||
		view == commonv1.OutputView_OUTPUT_VIEW_STORAGE_ONLY ||
		view == commonv1.OutputView_OUTPUT_VIEW_NEVER ||
		(redaction != commonv1.RedactionStrategy_REDACTION_STRATEGY_UNSPECIFIED &&
			redaction != commonv1.RedactionStrategy_REDACTION_STRATEGY_NONE)
}

func PII(fd protoreflect.FieldDescriptor) bool {
	return LegacyColumnSecurity(fd).GetIsPii() || Bool(fd, commonv1.E_Pii)
}

func Encrypted(fd protoreflect.FieldDescriptor) bool {
	security := ColumnSecurity(fd)
	return LegacyColumnSecurity(fd).GetIsEncrypted() || Bool(fd, commonv1.E_EncryptedSecurity) ||
		security.GetEncryptionKeyClass() != "" || security.GetSearchableEncrypted()
}
