package udbtest_test

import (
	"context"
	"sync"
	"testing"
	"time"

	apikeyeventsv1 "github.com/fahara02/udb/sdk/go/gen/udb/core/apikey/events/v1"
	storagev1 "github.com/fahara02/udb/sdk/go/gen/udb/core/storage/entity/v1"
	"github.com/fahara02/udb/sdk/go/udbclient"
	"github.com/fahara02/udb/sdk/go/udbtest"
)

// A named consumer resumes after the last event it acknowledged: a restart
// neither skips an event nor hands an acknowledged one over again.
func TestConsumeResumesAfterTheLastAcknowledgedEvent(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	tenant := "0190f1b2-0000-4000-8000-0000000000cc"
	u := fake.Client(t, tenant)
	const topic = "udb.apikey.created.v1"
	for _, key := range []string{"k1", "k2", "k3"} {
		fake.Emit(tenant, topic, tenant, map[string]any{"key_id": key, "tenant_id": tenant})
	}

	var mu sync.Mutex
	var handled []string
	run := func(stopAfter int) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = udbclient.Consume(ctx, u, "billing-projector", topic,
			func(_ context.Context, e udbclient.ConsumedEvent[*apikeyeventsv1.ApiKeyCreatedEvent]) error {
				mu.Lock()
				handled = append(handled, e.Payload.GetKeyId())
				n := len(handled)
				mu.Unlock()
				if n == stopAfter {
					// Let the ack land, then stop the consumer as a crash would.
					go func() { time.Sleep(100 * time.Millisecond); cancel() }()
				}
				return nil
			})
	}

	run(2)
	mu.Lock()
	first := append([]string(nil), handled...)
	mu.Unlock()
	if len(first) < 2 || first[0] != "k1" || first[1] != "k2" {
		t.Fatalf("first run handled %v, want k1 k2 first", first)
	}

	mu.Lock()
	handled = nil
	mu.Unlock()
	fake.Emit(tenant, topic, tenant, map[string]any{"key_id": "k4", "tenant_id": tenant})
	run(len(first) - 2 + 2)
	mu.Lock()
	second := append([]string(nil), handled...)
	mu.Unlock()
	all := append(append([]string(nil), first...), second...)
	seen := map[string]int{}
	for _, key := range all {
		seen[key]++
	}
	for _, key := range []string{"k1", "k2", "k3", "k4"} {
		if seen[key] != 1 {
			t.Fatalf("event %s handled %d times across the restart (first %v, second %v)", key, seen[key], first, second)
		}
	}
}

// Another tenant's events never reach the consumer.
func TestConsumeOnlySeesTheCallersTenant(t *testing.T) {
	fake := udbtest.New(t, &storagev1.File{})
	mine, other := "0190f1b2-0000-4000-8000-0000000000dd", "0190f1b2-0000-4000-8000-0000000000ee"
	u := fake.Client(t, mine)
	const topic = "udb.apikey.created.v1"
	fake.Emit(other, topic, other, map[string]any{"key_id": "theirs"})
	fake.Emit(mine, topic, mine, map[string]any{"key_id": "mine"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var got []string
	_ = udbclient.Consume(ctx, u, "audit", topic,
		func(_ context.Context, e udbclient.ConsumedEvent[*apikeyeventsv1.ApiKeyCreatedEvent]) error {
			got = append(got, e.Payload.GetKeyId())
			cancel()
			return nil
		})
	if len(got) != 1 || got[0] != "mine" {
		t.Fatalf("consumer saw %v, want only its own tenant's event", got)
	}
}
