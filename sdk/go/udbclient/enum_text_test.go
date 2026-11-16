package udbclient

import (
	"strings"
	"testing"
)

func TestEnumStoredTextFollowsTheDeclaredValues(t *testing.T) {
	cases := []struct {
		value, want string
		allowed     []string
		ok          bool
	}{
		{"ORDER_STATUS_OPEN", "OPEN", nil, true},
		{"OPEN", "OPEN", nil, true},
		{"OPEN", "ORDER_STATUS_OPEN", []string{"ORDER_STATUS_OPEN", "ORDER_STATUS_CLOSED"}, true},
		{"ORDER_STATUS_OPEN", "ORDER_STATUS_OPEN", []string{"ORDER_STATUS_OPEN"}, true},
		{"ORDER_STATUS_OPEN", "OPEN", []string{"OPEN", "CLOSED"}, true},
		{"ORDER_STATUS_CLOSED", "", []string{"ORDER_STATUS_OPEN"}, false},
	}
	for _, c := range cases {
		got, ok := EnumStoredText(c.value, "ORDER_STATUS_", c.allowed)
		if got != c.want || ok != c.ok {
			t.Errorf("EnumStoredText(%q, %v) = %q, %v; want %q, %v", c.value, c.allowed, got, ok, c.want, c.ok)
		}
	}
	if _, err := EnumText("ORDER_STATUS_CLOSED", "ORDER_STATUS_", []string{"ORDER_STATUS_OPEN"}); err == nil || !strings.Contains(err.Error(), "CHECK") {
		t.Errorf("a value outside enum_values must be refused, got %v", err)
	}
	if got := EnumKeyText("ORDER_STATUS_CLOSED", "ORDER_STATUS_", []string{"ORDER_STATUS_OPEN"}); got != "ORDER_STATUS_CLOSED" {
		t.Errorf("a key outside enum_values is left unchanged, got %q", got)
	}
}
