package udbtest_test

import (
	"os"
	"testing"

	"github.com/fahara02/udb/sdk/go/udbclient"
	"github.com/fahara02/udb/sdk/go/udbtest"
	"github.com/fahara02/udb/sdk/go/udbtest/conformance"
)

// Runs in the Linux and Windows embedded CI lane with that lane's compiled
// broker. This is the same contract used by the fake and external live broker.
func TestEmbeddedBrokerMeetsTheTableContract(t *testing.T) {
	if os.Getenv("UDB_EMBEDDED_SDK_TESTS") != "1" {
		t.Skip("requires an embedded broker (UDB_EMBEDDED_SDK_TESTS=1)")
	}
	// The shared contract uses the storage File entity as its table, so the
	// embedded broker serves the repository protos (an empty project serves
	// no entities).
	u, peer := udbtest.StartPair(t, "../../../proto", "")
	connect := func(t testing.TB, tenant string) *udbclient.Udb {
		t.Helper()
		if tenant == peer.Meta.TenantID {
			return peer
		}
		return u
	}
	conformance.RunTable(t, connect, conformance.Tenants(u.Meta.TenantID, peer.Meta.TenantID))
	conformance.RunTableCases(t, func(testing.TB, string) *udbclient.Udb { return u }, "../../conformance/table-cases.json")
}
