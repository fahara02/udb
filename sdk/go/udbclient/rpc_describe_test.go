package udbclient

import (
	"strings"
	"testing"
)

// Every RPC's generated metadata states where it is served and how retries
// behave, so a caller can read the contract at the point of use.
func TestEveryRPCDescribesItsListenerAndRetryRules(t *testing.T) {
	for _, rpc := range AllRPCs {
		if rpc.Listener == "" {
			t.Errorf("%s has no listener", rpc.FullMethod)
		}
		doc := rpc.Describe()
		if !strings.Contains(doc, "listener: ") || !strings.Contains(doc, "retries: ") {
			t.Errorf("%s Describe() lacks listener/retries:\n%s", rpc.FullMethod, doc)
		}
	}
	if doc := DescribeRPC("/udb.services.v1.DataBroker/Select"); !strings.Contains(doc, "data plane") || !strings.Contains(doc, "safe to retry") {
		t.Fatalf("Select doc = %q", doc)
	}
	if DescribeRPC("NoSuchRPC") != "" {
		t.Fatal("unknown RPC must describe as empty")
	}
}
